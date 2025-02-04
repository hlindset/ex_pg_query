defmodule ExPgQuery.Native do
  @moduledoc """
  Provides native bindings to `libpg_query` C library functionality via NIFs.

  This module contains the core SQL parsing and manipulation functions implemented
  in native code for performance. The functions are loaded as NIFs (Native
  Implemented Functions) when the module is initialized.
  """

  @on_load :init

  def init do
    :ok = load_nif()
  end

  defp load_nif do
    :ex_pg_query
    |> Application.app_dir("priv/ex_pg_query")
    |> String.to_charlist()
    |> :erlang.load_nif(0)
  end

  @doc """
  Parses a SQL query into a Protocol Buffer representation.

  Returns a serialized protobuf that can be decoded as a `PgQuery.ParseResult`.

  ## Parameters

    * `query` - SQL query string to parse

  ## Returns

    * `{:ok, binary}` - Successfully parsed query as serialized protobuf
    * `{:error, reason}` - Error with reason

  ## Examples

      iex> {:ok, bytes} = ExPgQuery.Native.parse_protobuf("SELECT * FROM users")
      iex> is_binary(bytes)
      true
      iex> {:ok, result} = Protox.decode(bytes, PgQuery.ParseResult)

  """
  def parse_protobuf(_), do: exit(:nif_library_not_loaded)

  @doc """
  Converts a Protocol Buffer AST back into a SQL query string.

  ## Parameters

    * `protobuf` - Serialized Protocol Buffer AST binary

  ## Returns

    * `{:ok, string}` - Successfully deparsed query
    * `{:error, reason}` - Error with reason

  ## Examples

      iex> {:ok, bytes} = ExPgQuery.Native.parse_protobuf("SELECT * FROM users")
      iex> ExPgQuery.Native.deparse_protobuf(bytes)
      {:ok, "SELECT * FROM users"}

  """
  def deparse_protobuf(_), do: exit(:nif_library_not_loaded)

  @doc """
  Generates a fingerprint string that identifies structurally similar queries.

  Creates a hash that can be used to group similar queries that differ only in
  their literal values.

  ## Parameters

    * `query` - SQL query string to fingerprint

  ## Returns

    * `{:ok, map}` - Successfully generated fingerprint containing:
      * `:fingerprint` - Integer fingerprint value
      * `:fingerprint_str` - String representation of fingerprint
    * `{:error, reason}` - Error with reason

  ## Examples

      iex> ExPgQuery.Native.fingerprint("SELECT * FROM users WHERE id = 1")
      {:ok, %{fingerprint: 11595314936444286341, fingerprint_str: "a0ead580058af585"}}
      iex> ExPgQuery.Native.fingerprint("SELECT * FROM users WHERE id = 2")
      {:ok, %{fingerprint: 11595314936444286341, fingerprint_str: "a0ead580058af585"}}

  """
  def fingerprint(_), do: exit(:nif_library_not_loaded)

  @doc """
  Performs lexical scanning of a SQL query into tokens.

  Returns a serialized protobuf that can be decoded as a `PgQuery.ScanResult`.

  ## Parameters

    * `query` - SQL query string to scan

  ## Returns

    * `{:ok, binary}` - Successfully scanned query as serialized protobuf
    * `{:error, reason}` - Error with reason

  ## Examples

      iex> {:ok, bytes} = ExPgQuery.Native.scan("SELECT * FROM users")
      iex> is_binary(bytes)
      true
      iex> {:ok, result} = Protox.decode(bytes, PgQuery.ScanResult)

  """
  def scan(_), do: exit(:nif_library_not_loaded)

  @doc """
  Normalizes a SQL query by replacing literal values with placeholders.

  Converts literal values in the query to positional parameters (`$1`, `$2`, etc.)
  while preserving the query structure.

  ## Parameters

    * `sql` - SQL query string to normalize

  ## Returns

    * `{:ok, string}` - Successfully normalized query
    * `{:error, reason}` - Error with reason

  ## Examples

      iex> ExPgQuery.Native.normalize("SELECT * FROM users WHERE id = 123")
      {:ok, "SELECT * FROM users WHERE id = $1"}

      iex> ExPgQuery.Native.normalize("SELECT a, SUM(b) FROM tbl WHERE c = 'foo' GROUP BY 1, 'bar'")
      {:ok, "SELECT a, SUM(b) FROM tbl WHERE c = $1 GROUP BY 1, $2"}

      iex> ExPgQuery.Native.normalize("SELECT * FROM users WHERE name = 'John' AND age > 25")
      {:ok, "SELECT * FROM users WHERE name = $1 AND age > $2"}

      iex> ExPgQuery.Native.normalize("CREATE ROLE postgres PASSWORD 'xyz'")
      {:ok, "CREATE ROLE postgres PASSWORD $1"}

  """
  def normalize(_), do: exit(:nif_library_not_loaded)
end
