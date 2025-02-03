defmodule ExPgQuery do
  @moduledoc """
  Provides high-level functions for parsing, departing, and analyzing PostgreSQL queries.

  This module wraps the low-level NIFs provided by ExPgQuery.Native and adds
  convenient interfaces for working with the PostgreSQL query AST.
  """

  @doc """
  Parses a SQL query into a Protocol Buffer AST.

  ## Parameters
    - query: String containing the SQL query to parse

  ## Returns
    - `{:ok, protobuf}` - Successfully parsed query as PgQuery.ParseResult
    - `{:error, error}` - Error with reason

  ## Examples
      iex> parsed = ExPgQuery.parse_protobuf("SELECT * FROM users")
      {:ok, %PgQuery.ParseResult{}} = parsed
  """
  def parse_protobuf(query) do
    with {:ok, binary} <- ExPgQuery.Native.parse_protobuf(query),
         {:ok, protobuf} <- Protox.decode(binary, PgQuery.ParseResult) do
      {:ok, protobuf}
    else
      {:error, error} -> {:error, error}
    end
  end

  @doc """
  Identical to `parse_protobuf/1` but raises on error.

  ## Parameters
    - query: String containing the SQL query to parse

  ## Returns
    - protobuf: The parsed PgQuery.ParseResult

  ## Raises
    - Runtime error if parsing fails
  """
  def parse_protobuf!(query) do
    case parse_protobuf(query) do
      {:ok, protobuf} -> protobuf
      {:error, error} -> raise "Parse error: #{inspect(error)}"
    end
  end

  @doc """
  Converts a Protocol Buffer AST back into a SQL query string.

  ## Parameters
    - protobuf: A PgQuery.ParseResult struct containing the query AST

  ## Returns
    - `{:ok, string}` - Successfully deparsed query
    - `{:error, error}` - Error with reason

  ## Examples
      iex> parsed = ExPgQuery.parse_protobuf!("SELECT * FROM users")
      iex> ExPgQuery.deparse(parsed)
      {:ok, "SELECT * FROM users"}
  """
  def deparse(%PgQuery.ParseResult{} = protobuf) do
    binary_protobuf = Protox.encode!(protobuf) |> IO.iodata_to_binary()
    ExPgQuery.Native.deparse_protobuf(binary_protobuf)
  end

  @doc """
  Identical to `deparse/1` but raises on error.

  ## Parameters
    - protobuf: A PgQuery.ParseResult struct containing the query AST

  ## Returns
    - string: The deparsed SQL query

  ## Raises
    - Runtime error if departing fails
  """
  def deparse!(protobuf) do
    case deparse(protobuf) do
      {:ok, query} -> query
      {:error, error} -> raise "Deparse error: #{inspect(error)}"
    end
  end

  @doc """
  Deparses a single statement node into SQL.

  Takes a statement struct (like %SelectStmt{}, %InsertStmt{}, etc) and converts
  it to its SQL representation.

  ## Parameters
    - stmt: A PgQuery statement struct

  ## Returns
    - `{:ok, string}` - Successfully deparsed statement
    - `{:error, error}` - Error with reason

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
  ...> } = ExPgQuery.parse_protobuf!("SELECT * FROM users")
  iex> ExPgQuery.deparse_stmt(select_stmt)
  {:ok, "SELECT * FROM users"}
  """
  def deparse_stmt(stmt) do
    # todo: don't hardcode version
    %{name: oneof_name} =
      PgQuery.Node.fields_defs() |> Enum.find(&(&1.type == {:message, stmt.__struct__}))

    protobuf =
      %PgQuery.ParseResult{
        version: 170_000,
        stmts: [%PgQuery.RawStmt{stmt: %PgQuery.Node{node: {oneof_name, stmt}}}]
      }

    deparse(protobuf)
  end

  @doc """
  Similar to `deparse_stmt/1` but raises on error.

  ## Parameters
    - stmt: A statement struct from the PgQuery namespace

  ## Returns
    - string: The deparsed SQL statement

  ## Raises
    - Runtime error if departing fails
  """
  def deparse_stmt!(stmt) do
    case deparse_stmt(stmt) do
      {:ok, query} -> query
      {:error, error} -> raise "Deparse error: #{inspect(error)}"
    end
  end

  @doc """
  Deparses a single expression node into SQL.

  Takes an expression node and converts it to its SQL representation by wrapping
  it in a SELECT statement and extracting the WHERE clause.

  ## Parameters
    - expr: An expression struct from the PgQuery namespace

  ## Returns
    - `{:ok, string}` - Successfully deparsed expression
    - `{:error, error}` - Error with reason
  """
  def deparse_expr(expr) do
    case deparse_stmt(%PgQuery.SelectStmt{where_clause: expr, op: :SETOP_NONE}) do
      {:ok, query} ->
        {:ok, String.replace_leading(query, "SELECT WHERE ", "")}

      {:error, error} ->
        {:error, error}
    end
  end

  @doc """
  Similar to `deparse_expr/1` but raises on error.

  ## Parameters
    - expr: An expression struct from the PgQuery namespace

  ## Returns
    - string: The deparsed SQL expression

  ## Raises
    - Runtime error if departing fails
  """
  def deparse_expr!(expr) do
    case deparse_expr(expr) do
      {:ok, query} -> query
      {:error, error} -> raise "Deparse error: #{inspect(error)}"
    end
  end

  @doc """
  Generates a fingerprint string that identifies structurally similar queries.

  Creates a hash that can be used to group similar queries that differ only in
  their literal values. Useful for query analysis and caching.

  ## Parameters
    - sql: String containing the SQL query to fingerprint

  ## Returns
    - `{:ok, string}` - Successfully generated fingerprint
    - `{:error, reason}` - Error with reason

  ## Examples
      iex> ExPgQuery.fingerprint("SELECT * FROM users WHERE id = 1")
      {:ok, "a0ead580058af585"}
      iex> ExPgQuery.fingerprint("SELECT * FROM users WHERE id = 2")
      {:ok, "a0ead580058af585"}
  """
  def fingerprint(sql) do
    case ExPgQuery.Native.fingerprint(sql) do
      {:ok, %{fingerprint_str: fingerprint}} -> {:ok, fingerprint}
      {:error, _reason} = err -> err
    end
  end

  @doc """
  Normalizes a SQL query by replacing literal values with placeholders.

  This function converts literal values in the query to positional parameters ($1, $2, etc.)
  while preserving the query structure. This is particularly useful for identifying similar
  queries that differ only in their literal values

  ## Parameters
    - sql: String containing the SQL query to normalize

  ## Returns
    - `{:ok, string}` - Successfully normalized query with literals replaced by $N parameters
    - `{:error, reason}` - Error with provided reason

  ## Examples

      iex> ExPgQuery.normalize("SELECT * FROM users WHERE id = 123")
      {:ok, "SELECT * FROM users WHERE id = $1"}

      iex> ExPgQuery.normalize("SELECT a, SUM(b) FROM tbl WHERE c = 'foo' GROUP BY 1, 'bar'")
      {:ok, "SELECT a, SUM(b) FROM tbl WHERE c = $1 GROUP BY 1, $2"}

      # Handles multiple literals of different types
      iex> ExPgQuery.normalize("SELECT * FROM users WHERE name = 'John' AND age > 25")
      {:ok, "SELECT * FROM users WHERE name = $1 AND age > $2"}

      # Also normalizes special cases like passwords in DDL
      iex> ExPgQuery.normalize("CREATE ROLE postgres PASSWORD 'xyz'")
      {:ok, "CREATE ROLE postgres PASSWORD $1"}
  """
  def normalize(sql) do
    ExPgQuery.Native.normalize(sql)
  end
end
