defmodule ExPgQuery.Normalize do
  @moduledoc """
  Provides SQL query normalization by replacing literal values with parameter placeholders.

  Useful for query analysis, caching, and grouping similar queries together.

  ## Examples

      iex> ExPgQuery.Normalize.normalize("SELECT * FROM users WHERE id = 123")
      {:ok, "SELECT * FROM users WHERE id = $1"}

      iex> ExPgQuery.Normalize.normalize("SELECT * FROM users WHERE name = 'John' AND age > 25")
      {:ok, "SELECT * FROM users WHERE name = $1 AND age > $2"}

      iex> ExPgQuery.Normalize.normalize("CREATE ROLE postgres PASSWORD 'xyz'")
      {:ok, "CREATE ROLE postgres PASSWORD $1"}

  """

  @doc """
  Normalizes a SQL query by replacing literal values with placeholders.

  Converts literal values to positional parameters (`$1`, `$2`, etc.) while
  preserving the query structure.

  ## Parameters

    * `sql` - SQL query string to normalize

  ## Returns

    * `{:ok, string}` - Successfully normalized query
    * `{:error, reason}` - Error with reason

  ## Examples

      iex> ExPgQuery.Normalize.normalize("SELECT * FROM users WHERE id = 123")
      {:ok, "SELECT * FROM users WHERE id = $1"}

      iex> ExPgQuery.Normalize.normalize("SELECT a, SUM(b) FROM tbl WHERE c = 'foo' GROUP BY 1, 'bar'")
      {:ok, "SELECT a, SUM(b) FROM tbl WHERE c = $1 GROUP BY 1, $2"}

      iex> ExPgQuery.Normalize.normalize("SELECT * FROM users WHERE name = 'John' AND age > 25")
      {:ok, "SELECT * FROM users WHERE name = $1 AND age > $2"}

      iex> ExPgQuery.Normalize.normalize("CREATE ROLE postgres PASSWORD 'xyz'")
      {:ok, "CREATE ROLE postgres PASSWORD $1"}
  """
  def normalize(sql) do
    ExPgQuery.Native.normalize(sql)
  end
end
