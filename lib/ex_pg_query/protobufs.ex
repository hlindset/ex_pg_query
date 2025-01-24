defmodule ExPgQueryProtobufs do
  use Protox, files: [
    "./libpg_query/protobuf/pg_query.proto"
  ]
end
