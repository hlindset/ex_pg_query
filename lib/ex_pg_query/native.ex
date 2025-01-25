defmodule ExPgQuery.Native do
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

  def parse_protobuf(_), do: exit(:nif_library_not_loaded)
  def deparse_protobuf(_), do: exit(:nif_library_not_loaded)
  def fingerprint(_), do: exit(:nif_library_not_loaded)
  def scan(_), do: exit(:nif_library_not_loaded)
end
