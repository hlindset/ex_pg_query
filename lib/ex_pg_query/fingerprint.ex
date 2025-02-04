defmodule ExPgQuery.Fingerprint do
  @moduledoc """
  Generates fingerprints to identify structurally equivalent SQL queries.

  Useful for e.g. query normalization and grouping similar queries together,
  even when they contain different constants, strings, or parameter values.

  ## Examples

      iex> ExPgQuery.Fingerprint.fingerprint("SELECT * FROM users WHERE id = 1")
      {:ok, "a0ead580058af585"}
      iex> ExPgQuery.Fingerprint.fingerprint("SELECT * FROM users WHERE id = 2")
      {:ok, "a0ead580058af585"}

  The above queries generate the same fingerprint since they are structurally
  identical, differing only in their literal values.
  """

  @doc """
  Generates a fingerprint string that identifies structurally similar queries.

  Can be used to group similar queries that differ only in their literal values.

  ## Parameters

    * `sql` - String containing the SQL query to fingerprint

  ## Returns

    * `{:ok, string}` - Successfully generated fingerprint
    * `{:error, reason}` - Error with reason

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
