defmodule ExPgQuery do
  use Zig,
    otp_app: :ex_pg_query,
    c: [
      include_dirs: "../libpg_query",
      src: "../libpg_query/src/*"
    ]

  ~Z"""
  const pg_query = @cImport(@cInclude("pg_query.h"));

  pub const normalize = pg_query.pg_query_normalize;
  """

  def hello do
    :world
  end
end
