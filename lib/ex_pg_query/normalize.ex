defmodule ExPgQuery.Normalize do
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

      iex> ExPgQuery.Normalize.normalize("SELECT * FROM users WHERE id = 123")
      {:ok, "SELECT * FROM users WHERE id = $1"}

      iex> ExPgQuery.Normalize.normalize("SELECT a, SUM(b) FROM tbl WHERE c = 'foo' GROUP BY 1, 'bar'")
      {:ok, "SELECT a, SUM(b) FROM tbl WHERE c = $1 GROUP BY 1, $2"}

      # Handles multiple literals of different types
      iex> ExPgQuery.Normalize.normalize("SELECT * FROM users WHERE name = 'John' AND age > 25")
      {:ok, "SELECT * FROM users WHERE name = $1 AND age > $2"}

      # Also normalizes special cases like passwords in DDL
      iex> ExPgQuery.Normalize.normalize("CREATE ROLE postgres PASSWORD 'xyz'")
      {:ok, "CREATE ROLE postgres PASSWORD $1"}
  """
  def normalize(sql) do
    ExPgQuery.Native.normalize(sql)
  end
end
