#include <erl_nif.h>
#include <stdio.h>
#include <string.h>

#include "../libpg_query/pg_query.h"
#include "../libpg_query/protobuf/pg_query.pb-c.h"
#include "../libpg_query/vendor/protobuf-c/protobuf-c.h"

static ERL_NIF_TERM result_tuple(ErlNifEnv *env, const char *status,
                                 const char *result, size_t len)
{
  ERL_NIF_TERM binary;
  ERL_NIF_TERM atom = enif_make_atom(env, status);

  unsigned char *data = enif_make_new_binary(env, len, &binary);
  memcpy(data, result, len);

  return enif_make_tuple2(env, atom, binary);
}

ERL_NIF_TERM make_binary(ErlNifEnv *env, char *source)
{
  ERL_NIF_TERM binary;
  size_t len = strlen(source);
  unsigned char *data = enif_make_new_binary(env, len, &binary);
  memcpy(data, source, len);
  return binary;
}

static ERL_NIF_TERM deparse_protobuf(ErlNifEnv *env, 
                                    int argc, 
                                    const ERL_NIF_TERM argv[]) 
{
    ErlNifBinary input_binary;
    
    // Check we have exactly one argument
    if (argc != 1) {
        return enif_make_badarg(env);
    }

    // Check term type and safely extract binary content
    if (!enif_inspect_binary(env, argv[0], &input_binary)) {
        return enif_make_badarg(env);
    }

    // Try to unpack the protobuf message first to validate it
    PgQuery__ParseResult *msg = pg_query__parse_result__unpack(
        NULL,                           // Use default allocator
        input_binary.size,
        input_binary.data
    );

    if (msg == NULL || !protobuf_c_message_check(&msg->base)) {
        if (msg != NULL) {
            pg_query__parse_result__free_unpacked(msg, NULL);
        }
        return enif_make_badarg(env);
    }

    // Free the unpacked message since we just needed it for validation
    pg_query__parse_result__free_unpacked(msg, NULL);

    // Now proceed with the actual deparse using validated protobuf data
    PgQueryProtobuf protobuf = {
        .len = input_binary.size,
        .data = (char*)input_binary.data
    };

    PgQueryDeparseResult result = pg_query_deparse_protobuf(protobuf);

    if (result.error != NULL) {
        ERL_NIF_TERM error_term = enif_make_tuple2(env,
            enif_make_atom(env, "error"),
            enif_make_string(env, result.error->message, ERL_NIF_UTF8)
        );
        pg_query_free_deparse_result(result);
        return error_term;
    }

    ERL_NIF_TERM ok_term = enif_make_tuple2(env,
        enif_make_atom(env, "ok"),
        enif_make_string(env, result.query, ERL_NIF_UTF8)
    );
    
    pg_query_free_deparse_result(result);
    return ok_term;
}

static ERL_NIF_TERM parse_protobuf(ErlNifEnv *env,
                                   int argc,
                                   const ERL_NIF_TERM argv[])
{
  ErlNifBinary query;
  ERL_NIF_TERM term;

  if (argc == 1 && enif_inspect_binary(env, argv[0], &query))
  {
    // add one more byte for the null termination
    char statement[query.size + 1];

    strncpy(statement, (char *)query.data, query.size);

    // terminate the string
    statement[query.size] = 0;

    PgQueryProtobufParseResult result = pg_query_parse_protobuf(statement);

    if (result.error)
    {
      ERL_NIF_TERM error_map = enif_make_new_map(env);

      if (!enif_make_map_put(
              env,
              error_map,
              enif_make_atom(env, "message"),
              make_binary(env, result.error->message),
              &error_map))
      {
        return enif_raise_exception(env, make_binary(env, "failed to update map"));
      }

      if (!enif_make_map_put(
              env,
              error_map,
              enif_make_atom(env, "cursorpos"),
              // drop the cursorpos by one, so it's zero-indexed
              enif_make_int(env, result.error->cursorpos - 1),
              &error_map))
      {
        return enif_raise_exception(env, make_binary(env, "failed to update map"));
      }

      term = enif_make_tuple2(env, enif_make_atom(env, "error"), error_map);
    }
    else
    {
      term = result_tuple(env, "ok", result.parse_tree.data,
                          result.parse_tree.len);
    }
    pg_query_free_protobuf_parse_result(result);

    return term;
  }
  else
  {
    return enif_make_badarg(env);
  }
}

static ErlNifFunc funcs[] = {
    {"parse_protobuf", 1, parse_protobuf},
    {"deparse_protobuf", 1, deparse_protobuf}};

ERL_NIF_INIT(Elixir.ExPgQuery.Native, funcs, NULL, NULL, NULL, NULL)
