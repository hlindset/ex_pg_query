defmodule ExPgQuery.Native do
  @moduledoc """
  Provides native bindings to libpg_query C library functionality via NIFs.

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

  Takes a SQL query string and returns the parsed Abstract Syntax Tree (AST)
  as a serialized Protocol Buffer binary.

  ## Parameters
    - query: String containing the SQL query to parse

  ## Returns
    - `{:ok, binary}` - Successfully parsed query, returns serialized protobuf
                        which can be decoded as a PgQuery.ParseResult
    - `{:error, reason}` - Error with reason

  ## Examples
      iex> ExPgQuery.Native.parse_protobuf("SELECT * FROM users")
      {:ok, <<...>>}
  """
  def parse_protobuf(_), do: exit(:nif_library_not_loaded)

  @doc """
  Converts a Protocol Buffer AST back into a SQL query string.

  Takes a serialized Protocol Buffer binary containing a parsed query AST
  and reconstructs the original SQL query string.

  ## Parameters
    - protobuf: Binary containing the serialized Protocol Buffer AST

  ## Returns
    - `{:ok, string}` - Successfully deparsed query
    - `{:error, reason}` - Error with reason

  ## Examples
      iex> proto = ExPgQuery.Native.parse_protobuf!("SELECT * FROM users")
      iex> ExPgQuery.Native.deparse_protobuf(proto)
      {:ok, "SELECT * FROM users"}
  """
  def deparse_protobuf(_), do: exit(:nif_library_not_loaded)

  @doc """
  Generates a fingerprint string that identifies structurally similar queries.

  Creates a hash that can be used to group similar queries that differ only in
  their literal values. Useful for query analysis and caching.

  ## Parameters
    - query: String containing the SQL query to fingerprint

  ## Returns
    - `{:ok, string}` - Successfully generated fingerprint
    - `{:error, reason}` - Error with reason

  ## Examples
      iex> ExPgQuery.Native.fingerprint("SELECT * FROM users WHERE id = 1")
      {:ok, "418c5509e2202b89"}
      iex> ExPgQuery.Native.fingerprint("SELECT * FROM users WHERE id = 2")
      {:ok, "418c5509e2202b89"}
  """
  def fingerprint(_), do: exit(:nif_library_not_loaded)

  @doc """
  Performs lexical scanning of a SQL query into tokens.

  Breaks down a SQL query string into its constituent lexical tokens
  for analysis or further processing.


  ## Parameters
    - query: String containing the SQL query to scan

  ## Returns
    - `{:ok, binary}` - Successfully parsed query, returns serialized protobuf that
                        can be decoded as a PgQuery.ScanResult
    - `{:error, reason}` - Error with reason

  ## Examples
      iex> ExPgQuery.Native.scan("SELECT * FROM users")
      {:ok, <<...>>}
  """
  def scan(_), do: exit(:nif_library_not_loaded)
end
