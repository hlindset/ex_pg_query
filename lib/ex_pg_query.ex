defmodule ExPgQuery do
  def parse_protobuf(query) do
    with {:ok, binary} <- ExPgQuery.Native.parse_protobuf(query),
         {:ok, protobuf} <- Protox.decode(binary, PgQuery.ParseResult) do
      {:ok, protobuf}
    else
      {:error, error} -> {:error, error}
    end
  end

  def parse_protobuf!(query) do
    case parse_protobuf(query) do
      {:ok, protobuf} -> protobuf
      {:error, error} -> raise "Parse error: #{inspect(error)}"
    end
  end

  def deparse(%PgQuery.ParseResult{} = protobuf) do
    binary_protobuf = Protox.encode!(protobuf) |> IO.iodata_to_binary()
    ExPgQuery.Native.deparse_protobuf(binary_protobuf)
  end

  def deparse!(protobuf) do
    case deparse(protobuf) do
      {:ok, query} -> query
      {:error, error} -> raise "Deparse error: #{inspect(error)}"
    end
  end

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

  def deparse_stmt!(stmt) do
    case deparse_stmt(stmt) do
      {:ok, query} -> query
      {:error, error} -> raise "Deparse error: #{inspect(error)}"
    end
  end

  def deparse_expr(expr) do
    case deparse_stmt(%PgQuery.SelectStmt{where_clause: expr, op: :SETOP_NONE}) do
      {:ok, query} ->
        {:ok,
         query
         |> String.replace_leading("SELECT WHERE ", "")}

      {:error, error} ->
        {:error, error}
    end
  end

  def deparse_expr!(expr) do
    case deparse_expr(expr) do
      {:ok, query} -> query
      {:error, error} -> raise "Deparse error: #{inspect(error)}"
    end
  end

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
