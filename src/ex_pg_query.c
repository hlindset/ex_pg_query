#include <erl_nif.h>
#include <stdio.h>
#include <string.h>
#include <stdbool.h>

#include "../libpg_query/pg_query.h"
#include "../libpg_query/protobuf/pg_query.pb-c.h"
#include "../libpg_query/vendor/protobuf-c/protobuf-c.h"

// Debug logging macro - can be enabled/disabled via compilation flag
#ifdef DEBUG_LOGGING
#define DEBUG_LOG(fmt, ...) fprintf(stderr, "DEBUG: " fmt "\n", ##__VA_ARGS__)
#else
#define DEBUG_LOG(fmt, ...)
#endif

// Helper function to create error tuples
static ERL_NIF_TERM make_error(ErlNifEnv *env, const char *message)
{
  return enif_make_tuple2(env,
                          enif_make_atom(env, "error"),
                          enif_make_string(env, message, ERL_NIF_UTF8));
}

// Helper function to create success tuples
static ERL_NIF_TERM make_success(ErlNifEnv *env, const unsigned char *data, size_t len)
{
  ERL_NIF_TERM binary;
  unsigned char *binary_data = enif_make_new_binary(env, len, &binary);
  memcpy(binary_data, data, len);

  return enif_make_tuple2(env,
                          enif_make_atom(env, "ok"),
                          binary);
}

// Helper function to validate NIF arguments
static bool validate_args(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[], ErlNifBinary *input_binary)
{
  if (argc != 1)
  {
    DEBUG_LOG("Invalid number of arguments: expected 1, got %d", argc);
    return false;
  }

  if (!enif_inspect_binary(env, argv[0], input_binary))
  {
    DEBUG_LOG("Failed to inspect binary input");
    return false;
  }

  return true;
}

// Helper function to create error map for parse errors
static ERL_NIF_TERM create_parse_error_map(ErlNifEnv *env,
                                           const PgQueryError *error)
{
  ERL_NIF_TERM error_map = enif_make_new_map(env);

  // Add message to map
  if (!enif_make_map_put(env,
                         error_map,
                         enif_make_atom(env, "message"),
                         enif_make_string(env, error->message, ERL_NIF_UTF8),
                         &error_map))
  {
    DEBUG_LOG("Failed to add message to error map");
    return make_error(env, "Failed to create error map");
  }

  // Add cursor position to map (zero-indexed)
  if (!enif_make_map_put(env,
                         error_map,
                         enif_make_atom(env, "cursorpos"),
                         enif_make_int(env, error->cursorpos - 1),
                         &error_map))
  {
    DEBUG_LOG("Failed to add cursorpos to error map");
    return make_error(env, "Failed to create error map");
  }

  return enif_make_tuple2(env,
                          enif_make_atom(env, "error"),
                          error_map);
}

static ERL_NIF_TERM deparse_protobuf(ErlNifEnv *env,
                                     int argc,
                                     const ERL_NIF_TERM argv[])
{
  ErlNifBinary input_binary;

  DEBUG_LOG("Starting deparse_protobuf");

  if (!validate_args(env, argc, argv, &input_binary))
  {
    return enif_make_badarg(env);
  }

  // Try to unpack the protobuf message first to validate it
  PgQuery__ParseResult *msg = pg_query__parse_result__unpack(
      NULL, // Use default allocator
      input_binary.size,
      input_binary.data);

  if (msg == NULL || !protobuf_c_message_check(&msg->base))
  {
    DEBUG_LOG("Failed to unpack or validate protobuf message");
    if (msg != NULL)
    {
      pg_query__parse_result__free_unpacked(msg, NULL);
    }
    return enif_make_badarg(env);
  }

  // Free the unpacked message since we just needed it for validation
  pg_query__parse_result__free_unpacked(msg, NULL);

  // Now proceed with the actual deparse using validated protobuf data
  PgQueryProtobuf protobuf = {
      .len = input_binary.size,
      .data = (char *)input_binary.data};

  DEBUG_LOG("Departing protobuf of size %zu", protobuf.len);
  PgQueryDeparseResult result = pg_query_deparse_protobuf(protobuf);

  if (result.error != NULL)
  {
    DEBUG_LOG("Deparse error: %s", result.error->message);
    ERL_NIF_TERM error_term = make_error(env, result.error->message);
    pg_query_free_deparse_result(result);
    return error_term;
  }

  DEBUG_LOG("Deparse successful");
  ERL_NIF_TERM ok_term = make_success(env, (unsigned char *)result.query, strlen(result.query));

  pg_query_free_deparse_result(result);
  return ok_term;
}

static ERL_NIF_TERM parse_protobuf(ErlNifEnv *env,
                                   int argc,
                                   const ERL_NIF_TERM argv[])
{
  ErlNifBinary input_binary;

  DEBUG_LOG("Starting parse_protobuf");

  if (!validate_args(env, argc, argv, &input_binary))
  {
    return enif_make_badarg(env);
  }

  // Create null-terminated string from input
  char *query_str = (char *)malloc(input_binary.size + 1);
  if (query_str == NULL)
  {
    DEBUG_LOG("Memory allocation failed for query string");
    return make_error(env, "Memory allocation failed");
  }

  memcpy(query_str, input_binary.data, input_binary.size);
  query_str[input_binary.size] = '\0';

  // Parse the query
  DEBUG_LOG("Parsing query of size %zu", input_binary.size);
  PgQueryProtobufParseResult result = pg_query_parse_protobuf(query_str);
  free(query_str);

  if (result.error != NULL)
  {
    DEBUG_LOG("Parse error: %s at position %d",
              result.error->message,
              result.error->cursorpos);

    ERL_NIF_TERM error_term = create_parse_error_map(env, result.error);
    pg_query_free_protobuf_parse_result(result);
    return error_term;
  }

  DEBUG_LOG("Parse successful");
  ERL_NIF_TERM ok_term = make_success(env,
                                      (unsigned char *)result.parse_tree.data,
                                      result.parse_tree.len);

  pg_query_free_protobuf_parse_result(result);
  return ok_term;
}

static ErlNifFunc funcs[] = {
    {"parse_protobuf", 1, parse_protobuf},
    {"deparse_protobuf", 1, deparse_protobuf}};

ERL_NIF_INIT(Elixir.ExPgQuery.Native, funcs, NULL, NULL, NULL, NULL)