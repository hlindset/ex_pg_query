defmodule ExPgQuery.NodeTraversalTest do
  use ExUnit.Case

  alias ExPgQuery.{Parser, NodeTraversal}

  describe "table alias tracking" do
    test "tracks table aliases in simple joins" do
      {:ok, parse_result} =
        Parser.parse("""
        SELECT t1.col1, t2.col2, t3.col3
        FROM public.table_1 AS t1
        JOIN schema2.table_2 t2 ON t2.id = t1.id
        LEFT JOIN table_3 AS t3 ON t3.id = t2.id
        WHERE t1.active = true
        """)

      nodes = NodeTraversal.nodes(parse_result.protobuf)

      # Find the SelectStmt context
      select_ctx =
        nodes
        |> Enum.find_value(fn
          {%PgQuery.SelectStmt{}, ctx} -> ctx
          _ -> nil
        end)

      expected_aliases = %{
        "t1" => %{alias: "t1", schema: "public", relation: "table_1", location: 38},
        "t2" => %{alias: "t2", schema: "schema2", relation: "table_2", location: 64},
        "t3" => %{alias: "t3", schema: nil, relation: "table_3", location: 110}
      }

      assert select_ctx.table_aliases == expected_aliases
    end

    test "keeps aliases scoped to their SelectStmt" do
      {:ok, parse_result} =
        Parser.parse("""
        SELECT *
        FROM (
          SELECT * FROM table_1 AS t1
        ) AS sub1,
        (
          SELECT * FROM table_2 AS t2
        ) AS sub2
        WHERE sub1.id = sub2.id
        """)

      nodes = NodeTraversal.nodes(parse_result.protobuf)

      # Find the subquery contexts
      subquery_contexts =
        nodes
        |> Enum.filter(fn
          {%PgQuery.SelectStmt{}, ctx} -> ctx.table_aliases != %{}
          _ -> nil
        end)
        |> Enum.map(fn {_node, ctx} -> ctx.table_aliases end)

      # First subquery should only see t1
      assert Enum.at(subquery_contexts, 0) == %{
               "t1" => %{alias: "t1", schema: nil, relation: "table_1", location: 32}
             }

      # Second subquery should only see t2
      assert Enum.at(subquery_contexts, 1) == %{
               "t2" => %{alias: "t2", schema: nil, relation: "table_2", location: 75}
             }

      # Outer query should only see sub1 and sub2
      outer_ctx =
        nodes
        |> Enum.find_value(fn
          {%PgQuery.SelectStmt{from_clause: [_, _]}, ctx} -> ctx
          _ -> nil
        end)

      assert outer_ctx.table_aliases == %{}
    end
  end
end
