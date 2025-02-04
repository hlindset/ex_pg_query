defmodule ExPgQuery.ParamRefsTest do
  use ExUnit.Case

  alias ExPgQuery.ParamRefs
  alias ExPgQuery.Protobuf

  doctest ExPgQuery.ParamRefs

  describe "param_refs" do
    test "simple query" do
      query = "SELECT * FROM x WHERE y = $1 AND z = $2"
      {:ok, tree} = Protobuf.from_sql(query)
      param_refs = ParamRefs.param_refs(tree)
      assert param_refs == [%{location: 26, length: 2}, %{location: 37, length: 2}]
    end

    test "simple query with :: typecast" do
      query = "SELECT * FROM x WHERE y = $1 AND z = $2::timestamptz"
      {:ok, tree} = Protobuf.from_sql(query)
      param_refs = ParamRefs.param_refs(tree)

      assert param_refs == [
               %{location: 26, length: 2},
               %{location: 37, length: 2, typename: ["timestamptz"]}
             ]
    end

    test "queries with typecasts" do
      query = "SELECT * FROM x WHERE y = $1::text AND z < now() - INTERVAL $2"
      {:ok, tree} = Protobuf.from_sql(query)
      param_refs = ParamRefs.param_refs(tree)

      assert param_refs == [
               %{
                 location: 26,
                 length: 2,
                 typename: ["text"]
               },
               %{
                 location: 51,
                 length: 11,
                 typename: ["pg_catalog", "interval"]
               }
             ]
    end

    test "param refs with different lengths" do
      query = "SELECT * FROM a WHERE x = $1 AND y = $12 AND z = $255"
      {:ok, tree} = Protobuf.from_sql(query)
      param_refs = ParamRefs.param_refs(tree)

      assert param_refs == [
               %{location: 26, length: 2},
               %{location: 37, length: 3},
               %{location: 49, length: 4}
             ]
    end
  end
end
