#include <erl_nif.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>

#include "../libpg_query/pg_query.h"
#include "../libpg_query/protobuf/pg_query.pb-c.h"
#include "../libpg_query/vendor/protobuf-c/protobuf-c.h"

#ifndef MAX_SQL_LENGTH
#define MAX_SQL_LENGTH (16 * 1024 * 1024)
#endif

#ifndef MAX_PROTOBUF_LENGTH
#define MAX_PROTOBUF_LENGTH (32 * 1024 * 1024)
#endif

// Debug logging macro - can be enabled/disabled via compilation flag
#ifdef DEBUG_LOGGING
#define DEBUG_LOG(fmt, ...) fprintf(stderr, "DEBUG: " fmt "\n", ##__VA_ARGS__)
#else
#define DEBUG_LOG(fmt, ...)
#endif

/**
 * Creates an error tuple of the form {:error, message}
 *
 * @param env The NIF environment
 * @param message The error message string
 * @return ERL_NIF_TERM {:error, message} where message is a binary
 */
static ERL_NIF_TERM make_error(ErlNifEnv *env, const char *message) {
  ERL_NIF_TERM binary;
  size_t message_len = strlen(message);
  unsigned char *binary_data = enif_make_new_binary(env, message_len, &binary);
  memcpy(binary_data, message, message_len);

  return enif_make_tuple2(env, enif_make_atom(env, "error"), binary);
}

/**
 * Creates a success tuple of the form {:ok, data}
 *
 * @param env The NIF environment
 * @param data The binary data to wrap
 * @param len The length of the binary data
 * @return ERL_NIF_TERM {:ok, binary}
 */
static ERL_NIF_TERM make_success(ErlNifEnv *env, const unsigned char *data,
                                 size_t len) {
  ERL_NIF_TERM binary;
  unsigned char *binary_data = enif_make_new_binary(env, len, &binary);
  memcpy(binary_data, data, len);

  return enif_make_tuple2(env, enif_make_atom(env, "ok"), binary);
}

/**
 * Allocates a null-terminated string from a binary input using enif_alloc.
 * Caller must use enif_free to deallocate the returned string.
 *
 * @param binary The input binary to convert
 * @param error_term Output parameter for error term if allocation fails
 * @param env The NIF environment
 * @return char* Null-terminated string or NULL if allocation fails
 */
static char *mk_cstr(const ErlNifBinary *binary, ERL_NIF_TERM *error_term,
                     ErlNifEnv *env) {
  // Check for overflow in size calculation
  if (binary->size >= SIZE_MAX - 1) {
    *error_term = make_error(env, "input size too large");
    return NULL;
  }

  char *str = enif_alloc(binary->size + 1);
  if (str == NULL) {
    DEBUG_LOG("Memory allocation failed for string of size %zu",
              binary->size + 1);
    *error_term = make_error(env, "memory allocation failed");
    return NULL;
  }

  memcpy(str, binary->data, binary->size);
  str[binary->size] = '\0';

  return str;
}

/**
 * Validates NIF arguments ensuring proper arity and binary input
 *
 * @param env The NIF environment
 * @param argc Number of arguments passed to the NIF
 * @param argv Array of NIF arguments
 * @param input_binary Output parameter to store the validated binary
 * @param error_term Output parameter to store error term if validation fails
 * @return bool true if validation succeeds, false otherwise
 */
static bool validate_args(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[],
                          ErlNifBinary *input_binary, ERL_NIF_TERM *error_term,
                          size_t max_length) {
  if (argc != 1) {
    *error_term = make_error(env, "invalid number of arguments");
    return false;
  }

  // Check if the argument is binary
  if (!enif_is_binary(env, argv[0])) {
    *error_term = make_error(env, "argument must be a binary");
    return false;
  }

  if (!enif_inspect_binary(env, argv[0], input_binary)) {
    *error_term = make_error(env, "failed to inspect binary input");
    return false;
  }

  if (input_binary->size > max_length) {
    *error_term = make_error(env, "input too large");
    return false;
  }

  return true;
}

/**
 * Creates an error map for parse errors with message and cursor position
 *
 * @param env The NIF environment
 * @param error The PostgreSQL query error
 * @return ERL_NIF_TERM {:error, %{message: string, cursorpos: integer}}
 */
static ERL_NIF_TERM create_parse_error_map(ErlNifEnv *env,
                                           const PgQueryError *error) {
  ERL_NIF_TERM error_map = enif_make_new_map(env);
  ERL_NIF_TERM message_binary;

  // Create binary for error message
  size_t message_len = strlen(error->message);
  unsigned char *message_data =
      enif_make_new_binary(env, message_len, &message_binary);
  memcpy(message_data, error->message, message_len);

  // Add message binary to map
  if (!enif_make_map_put(env, error_map, enif_make_atom(env, "message"),
                         message_binary, &error_map)) {
    DEBUG_LOG("Failed to add message to error map");
    return make_error(env, "failed to create error map");
  }

  // Add cursor position to map (zero-indexed)
  if (!enif_make_map_put(env, error_map, enif_make_atom(env, "cursorpos"),
                         enif_make_int(env, error->cursorpos - 1),
                         &error_map)) {
    DEBUG_LOG("Failed to add cursorpos to error map");
    return make_error(env, "failed to create error map");
  }

  return enif_make_tuple2(env, enif_make_atom(env, "error"), error_map);
}

/**
 * Deparses a PostgreSQL query from its protobuf representation back to SQL
 *
 * Takes a binary containing a protobuf-encoded parse tree and converts it
 * back to SQL text.
 *
 * @param env The NIF environment
 * @param argc Number of arguments
 * @param argv Array of arguments - expects one binary argument
 * @return ERL_NIF_TERM {:ok, sql_binary} | {:error, reason}
 */
static ERL_NIF_TERM deparse_protobuf(ErlNifEnv *env, int argc,
                                     const ERL_NIF_TERM argv[]) {
  ErlNifBinary input_binary;
  ERL_NIF_TERM error_term;

  DEBUG_LOG("Starting deparse_protobuf");

  if (!validate_args(env, argc, argv, &input_binary, &error_term,
                     MAX_PROTOBUF_LENGTH)) {
    return error_term;
  }

  // Try to unpack the protobuf message first to validate it
  PgQuery__ParseResult *msg =
      pg_query__parse_result__unpack(NULL, // Use default allocator
                                     input_binary.size, input_binary.data);

  if (msg == NULL || !protobuf_c_message_check(&msg->base)) {
    DEBUG_LOG("Failed to unpack or validate protobuf message");
    if (msg != NULL) {
      pg_query__parse_result__free_unpacked(msg, NULL);
    }
    return make_error(env, "invalid protobuf message format");
  }

  // Free the unpacked message since we just needed it for validation
  pg_query__parse_result__free_unpacked(msg, NULL);

  // Now proceed with the actual deparse using validated protobuf data
  PgQueryProtobuf protobuf = {.len = input_binary.size,
                              .data = (char *)input_binary.data};

  DEBUG_LOG("Departing protobuf of size %zu", protobuf.len);
  PgQueryDeparseResult result = pg_query_deparse_protobuf(protobuf);

  if (result.error != NULL) {
    DEBUG_LOG("Deparse error: %s", result.error->message);
    ERL_NIF_TERM error_term = make_error(env, result.error->message);
    pg_query_free_deparse_result(result);
    return error_term;
  }

  DEBUG_LOG("Deparse successful");
  ERL_NIF_TERM ok_term =
      make_success(env, (unsigned char *)result.query, strlen(result.query));

  pg_query_free_deparse_result(result);
  return ok_term;
}

/**
 * Parses a SQL query into its protobuf representation
 *
 * Takes a SQL query string and returns its protobuf-encoded parse tree.
 *
 * @param env The NIF environment
 * @param argc Number of arguments
 * @param argv Array of arguments - expects one binary argument containing SQL
 * @return ERL_NIF_TERM {:ok, protobuf_binary} | {:error, reason}
 */
static ERL_NIF_TERM parse_protobuf(ErlNifEnv *env, int argc,
                                   const ERL_NIF_TERM argv[]) {
  ErlNifBinary query_binary;
  ERL_NIF_TERM error_term;

  DEBUG_LOG("Starting parse_protobuf");

  if (!validate_args(env, argc, argv, &query_binary, &error_term,
                     MAX_SQL_LENGTH)) {
    return error_term;
  }

  char *query_str = mk_cstr(&query_binary, &error_term, env);

  if (query_str == NULL) {
    return error_term;
  }

  // Parse the query
  DEBUG_LOG("Parsing query of size %zu", query_binary.size);
  PgQueryProtobufParseResult result = pg_query_parse_protobuf(query_str);
  enif_free(query_str);

  if (result.error != NULL) {
    DEBUG_LOG("Parse error: %s at position %d", result.error->message,
              result.error->cursorpos);

    ERL_NIF_TERM error_term = create_parse_error_map(env, result.error);
    pg_query_free_protobuf_parse_result(result);
    return error_term;
  }

  DEBUG_LOG("Parse successful");
  ERL_NIF_TERM ok_term = make_success(
      env, (unsigned char *)result.parse_tree.data, result.parse_tree.len);

  pg_query_free_protobuf_parse_result(result);
  return ok_term;
}

/**
 * Generates a unique fingerprint for a SQL query
 *
 * The fingerprint can be used to identify similar queries that differ only
 * in their literal values.
 *
 * @param env The NIF environment
 * @param argc Number of arguments
 * @param argv Array of arguments - expects one binary argument containing SQL
 * @return ERL_NIF_TERM {:ok, %{fingerprint: integer, fingerprint_str: binary}}
 * | {:error, reason}
 */
static ERL_NIF_TERM fingerprint(ErlNifEnv *env, int argc,
                                const ERL_NIF_TERM argv[]) {
  ErlNifBinary query_binary;
  ERL_NIF_TERM error_term;

  DEBUG_LOG("Starting fingerprint calculation");

  if (!validate_args(env, argc, argv, &query_binary, &error_term,
                     MAX_SQL_LENGTH)) {
    return error_term;
  }

  char *query_str = mk_cstr(&query_binary, &error_term, env);

  if (query_str == NULL) {
    return error_term;
  }

  // Calculate fingerprint
  DEBUG_LOG("Calculating fingerprint for query of size %zu", query_binary.size);
  PgQueryFingerprintResult result = pg_query_fingerprint(query_str);
  enif_free(query_str); // Free the query string as we don't need it anymore

  if (result.error != NULL) {
    DEBUG_LOG("Fingerprint error: %s", result.error->message);
    ERL_NIF_TERM error_term = make_error(env, result.error->message);
    pg_query_free_fingerprint_result(result);
    return error_term;
  }

  // Create result map
  ERL_NIF_TERM map = enif_make_new_map(env);
  ERL_NIF_TERM fingerprint_int, fingerprint_str;

  // Convert uint64_t to ERL_NIF_TERM
  // Note: using unsigned long long to ensure 64-bit compatibility
  fingerprint_int =
      enif_make_uint64(env, (unsigned long long)result.fingerprint);

  // Convert fingerprint string
  unsigned char *str_binary = enif_make_new_binary(
      env, strlen(result.fingerprint_str), &fingerprint_str);
  memcpy(str_binary, result.fingerprint_str, strlen(result.fingerprint_str));

  // Build the return map with both values
  if (!enif_make_map_put(env, map, enif_make_atom(env, "fingerprint"),
                         fingerprint_int, &map) ||
      !enif_make_map_put(env, map, enif_make_atom(env, "fingerprint_str"),
                         fingerprint_str, &map)) {
    DEBUG_LOG("Failed to create result map");
    pg_query_free_fingerprint_result(result);
    return make_error(env, "failed to create result map");
  }

  DEBUG_LOG("Fingerprint calculation successful");

  // Create the final success tuple with the map
  ERL_NIF_TERM ok_term = enif_make_tuple2(env, enif_make_atom(env, "ok"), map);

  // Free the libpg_query result
  pg_query_free_fingerprint_result(result);

  return ok_term;
}

/**
 * Scans a SQL query into tokens
 *
 * Performs lexical analysis of a SQL query, returning the tokens in
 * protobuf format.
 *
 * @param env The NIF environment
 * @param argc Number of arguments
 * @param argv Array of arguments - expects one binary argument containing SQL
 * @return ERL_NIF_TERM {:ok, protobuf_binary} | {:error, reason}
 */
static ERL_NIF_TERM scan(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
  ErlNifBinary query_binary;
  ERL_NIF_TERM error_term;

  DEBUG_LOG("Starting scan");

  if (!validate_args(env, argc, argv, &query_binary, &error_term,
                     MAX_SQL_LENGTH)) {
    return error_term;
  }

  char *query_str = mk_cstr(&query_binary, &error_term, env);

  if (query_str == NULL) {
    return error_term;
  }

  // Scan the query
  DEBUG_LOG("Scanning query of size %zu", query_binary.size);
  PgQueryScanResult result = pg_query_scan(query_str);
  enif_free(query_str); // Free the query string as we don't need it anymore

  if (result.error != NULL) {
    DEBUG_LOG("Scan error: %s", result.error->message);
    ERL_NIF_TERM error_term = create_parse_error_map(env, result.error);
    pg_query_free_scan_result(result);
    return error_term;
  }

  // Create success term with the protobuf data
  DEBUG_LOG("Scan successful");
  ERL_NIF_TERM ok_term =
      make_success(env, (unsigned char *)result.pbuf.data, result.pbuf.len);

  // Free the scan result
  pg_query_free_scan_result(result);
  return ok_term;
}

/**
 * Normalizes a SQL query by replacing literals with placeholders
 *
 * Takes a SQL query string and returns a normalized version where literals
 * are replaced with parameters (e.g., $1, $2, etc.)
 *
 * @param env The NIF environment
 * @param argc Number of arguments
 * @param argv Array of arguments - expects one binary argument containing SQL
 * @return ERL_NIF_TERM {:ok, normalized_sql_binary} | {:error, reason}
 */
static ERL_NIF_TERM normalize(ErlNifEnv *env, int argc,
                              const ERL_NIF_TERM argv[]) {
  ErlNifBinary query_binary;
  ERL_NIF_TERM error_term;

  DEBUG_LOG("Starting normalize");

  if (!validate_args(env, argc, argv, &query_binary, &error_term,
                     MAX_SQL_LENGTH)) {
    return error_term;
  }

  char *query_str = mk_cstr(&query_binary, &error_term, env);

  if (query_str == NULL) {
    return error_term;
  }

  // Normalize the query
  DEBUG_LOG("Normalizing query of size %zu", query_binary.size);
  PgQueryNormalizeResult result = pg_query_normalize(query_str);
  enif_free(query_str);

  if (result.error != NULL) {
    DEBUG_LOG("Normalize error: %s", result.error->message);
    ERL_NIF_TERM error_term = make_error(env, result.error->message);
    pg_query_free_normalize_result(result);
    return error_term;
  }

  // Create success term with the normalized query
  DEBUG_LOG("Normalize successful");
  ERL_NIF_TERM ok_term =
      make_success(env, (unsigned char *)result.normalized_query,
                   strlen(result.normalized_query));

  // Free the normalize result
  pg_query_free_normalize_result(result);
  return ok_term;
}

/**
 * ExPgQuery NIF Implementation
 *
 * This module provides NIFs for PostgreSQL query parsing, deparsing,
 * scanning and fingerprinting functionality. It wraps the libpg_query
 * library to provide these capabilities to Elixir applications.
 *
 * The module exposes four main functions:
 * - parse_protobuf/1: Parses SQL to protobuf format
 * - deparse_protobuf/1: Converts protobuf back to SQL
 * - scan/1: Performs lexical analysis of SQL
 * - fingerprint/1: Generates query fingerprints
 *
 * All functions expect binary input and return tagged tuples:
 * {:ok, result} | {:error, reason}
 */
static ErlNifFunc funcs[] = {{"parse_protobuf", 1, parse_protobuf},
                             {"deparse_protobuf", 1, deparse_protobuf},
                             {"scan", 1, scan},
                             {"fingerprint", 1, fingerprint},
                             {"normalize", 1, normalize}};

ERL_NIF_INIT(Elixir.ExPgQuery.Native, funcs, NULL, NULL, NULL, NULL)
