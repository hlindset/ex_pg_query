defmodule ExPgQuery.Fingerprint do
  def fingerprint(sql) do
    case ExPgQuery.Native.fingerprint(sql) do
      {:ok, %{fingerprint_str: fingerprint}} -> {:ok, fingerprint}
      {:error, _reason} = err -> err
    end
  end
end
