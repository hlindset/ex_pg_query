defmodule ExPgQuery.Fingerprint do
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
      iex> ExPgQuery.Fingerprint.fingerprint("SELECT * FROM users WHERE id = 1")
      {:ok, "a0ead580058af585"}
      iex> ExPgQuery.Fingerprint.fingerprint("SELECT * FROM users WHERE id = 2")
      {:ok, "a0ead580058af585"}
  """
  def fingerprint(sql) do
    case ExPgQuery.Native.fingerprint(sql) do
      {:ok, %{fingerprint_str: fingerprint}} -> {:ok, fingerprint}
      {:error, _reason} = err -> err
    end
  end
end
