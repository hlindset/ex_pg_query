defmodule ExPgQuery.Protobuf do
  @moduledoc """
  Handles conversion between SQL queries and Protocol Buffer AST representations.

  ## Examples

      iex> {:ok, protobuf} = ExPgQuery.Protobuf.from_sql("SELECT * FROM users")
      iex> ExPgQuery.Protobuf.to_sql(protobuf)
      {:ok, "SELECT * FROM users"}
  """

  use Protox,
    files: ["./libpg_query/protobuf/pg_query.proto"],
    keep_unknown_fields: false

  @postgres_query_version 170_000

  @doc """
  Parses a SQL query into a Protocol Buffer AST.

  ## Parameters

    * `query` - SQL query string to parse

  ## Returns

    * `{:ok, protobuf}` - Successfully parsed `PgQuery.ParseResult`
    * `{:error, error}` - Error with reason

  ## Examples

      iex> parsed = ExPgQuery.Protobuf.from_sql("SELECT * FROM users")
      {:ok, %PgQuery.ParseResult{}} = parsed

  """
  def from_sql(query) do
    with {:ok, binary} <- ExPgQuery.Native.parse_protobuf(query),
         {:ok, protobuf} <- Protox.decode(binary, PgQuery.ParseResult) do
      {:ok, protobuf}
    else
      {:error, error} -> {:error, error}
    end
  end

  @doc """
  Identical to `from_sql/1` but raises on error.

  ## Parameters

    * `query` - SQL query string to parse

  ## Returns

    * `PgQuery.ParseResult` struct

  ## Raises

    * Runtime error if parsing fails

  """
  def from_sql!(query) do
    case from_sql(query) do
      {:ok, protobuf} -> protobuf
      {:error, error} -> raise "Parse error: #{inspect(error)}"
    end
  end

  @doc """
  Converts a Protocol Buffer AST back into a SQL query string.

  ## Parameters

    * `protobuf` - `PgQuery.ParseResult` struct containing query AST

  ## Returns

    * `{:ok, string}` - Successfully deparsed query
    * `{:error, error}` - Error with reason

  ## Examples

      iex> parsed = ExPgQuery.Protobuf.from_sql!("SELECT * FROM users")
      iex> ExPgQuery.Protobuf.to_sql(parsed)
      {:ok, "SELECT * FROM users"}

  """
  def to_sql(%PgQuery.ParseResult{} = protobuf) do
    binary_protobuf = Protox.encode!(protobuf) |> IO.iodata_to_binary()
    ExPgQuery.Native.deparse_protobuf(binary_protobuf)
  end

  @doc """
  Identical to `to_sql/1` but raises on error.

  ## Parameters

    * `protobuf` - `PgQuery.ParseResult` struct containing query AST

  ## Returns

    * SQL query string

  ## Raises

    * Runtime error if departing fails

  """
  def to_sql!(protobuf) do
    case to_sql(protobuf) do
      {:ok, query} -> query
      {:error, error} -> raise "Deparse error: #{inspect(error)}"
    end
  end

  @doc """
  Deparses a single statement node into SQL.

  Takes a statement struct (like `%SelectStmt{}`, `%InsertStmt{}`, etc) and
  converts it to its SQL representation.

  ## Parameters

    * `stmt` - `PgQuery` statement struct

  ## Returns

    * `{:ok, string}` - Successfully deparsed statement
    * `{:error, error}` - Error with reason

  ## Examples

      iex> %PgQuery.ParseResult{
      ...>   version: 170000,
      ...>   stmts: [
      ...>     %PgQuery.RawStmt{
      ...>       stmt: %PgQuery.Node{
      ...>         node: {:select_stmt, select_stmt}
      ...>       }
      ...>     }
      ...>   ]
      ...> } = ExPgQuery.Protobuf.from_sql!("SELECT * FROM users")
      iex> ExPgQuery.Protobuf.stmt_to_sql(select_stmt)
      {:ok, "SELECT * FROM users"}

  """
  def stmt_to_sql(stmt) do
    %{name: oneof_name} =
      PgQuery.Node.fields_defs() |> Enum.find(&(&1.type == {:message, stmt.__struct__}))

    protobuf =
      %PgQuery.ParseResult{
        version: @postgres_query_version,
        stmts: [%PgQuery.RawStmt{stmt: %PgQuery.Node{node: {oneof_name, stmt}}}]
      }

    to_sql(protobuf)
  end

  @doc """
  Similar to `stmt_to_sql/1` but raises on error.

  ## Parameters

    * `stmt` - `PgQuery` statement struct

  ## Returns

    * SQL statement string

  ## Raises

    * Runtime error if departing fails

  """
  def stmt_to_sql!(stmt) do
    case stmt_to_sql(stmt) do
      {:ok, query} -> query
      {:error, error} -> raise "Deparse error: #{inspect(error)}"
    end
  end

  @doc """
  Deparses a single expression node into SQL.

  Takes an expression node and converts it to its SQL representation by wrapping
  it in a SELECT statement and extracting the WHERE clause.

  ## Parameters

    * `expr` - `PgQuery` expression struct

  ## Returns

    * `{:ok, string}` - Successfully deparsed expression
    * `{:error, error}` - Error with reason

  """
  def expr_to_sql(expr) do
    case stmt_to_sql(%PgQuery.SelectStmt{where_clause: expr, op: :SETOP_NONE}) do
      {:ok, query} ->
        {:ok, String.replace_leading(query, "SELECT WHERE ", "")}

      {:error, error} ->
        {:error, error}
    end
  end

  @doc """
  Similar to `expr_to_sql/1` but raises on error.

  ## Parameters

    * `expr` - `PgQuery` expression struct

  ## Returns

    * SQL expression string

  ## Raises

    * Runtime error if departing fails

  """
  def expr_to_sql!(expr) do
    case expr_to_sql(expr) do
      {:ok, query} -> query
      {:error, error} -> raise "Deparse error: #{inspect(error)}"
    end
  end
end
