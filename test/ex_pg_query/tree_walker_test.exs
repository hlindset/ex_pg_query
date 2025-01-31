defmodule ExPgQuery.TreeWalkerTest do
  use ExUnit.Case, async: true

  alias ExPgQuery.TreeWalker

  describe "walk/3" do
    test "traverses a simple node" do
      # Create a simple node with a message type
      simple_node = %PgQuery.Node{
        node: {:string, %PgQuery.String{sval: "test"}}
      }

      # Create a callback that collects visited nodes
      visited =
        TreeWalker.walk(simple_node, [], fn node, field_name, {child_node, location}, acc ->
          [{node, field_name, child_node, location} | acc]
        end)

      # The walker visits the string node once
      assert length(visited) == 1
      [{node, field_name, child_node, location}] = visited
      assert node == simple_node
      assert match?(%PgQuery.String{sval: "test"}, child_node)
      assert location == [:string]
    end

    test "traverses nested message nodes" do
      # Create a more complex nested structure
      nested_stmt = %PgQuery.SelectStmt{
        target_list: [
          %PgQuery.Node{
            node:
              {:res_target,
               %PgQuery.ResTarget{
                 name: "col1",
                 val: %PgQuery.Node{
                   node: {:string, %PgQuery.String{sval: "value1"}}
                 }
               }}
          }
        ]
      }

      root_node = %PgQuery.Node{node: {:select_stmt, nested_stmt}}

      # Collect all visited nodes and their paths
      visited =
        TreeWalker.walk(root_node, [], fn node, field_name, {child_node, location}, acc ->
          [{node, field_name, child_node, location} | acc]
        end)

      locations =
        visited
        |> Enum.map(fn {_, _, _, loc} -> loc end)
        |> Enum.reverse()

      assert locations == [
               [:select_stmt],
               [:select_stmt, :distinct_clause],
               [:select_stmt, :target_list],
               [:select_stmt, :from_clause],
               [:select_stmt, :group_clause],
               [:select_stmt, :window_clause],
               [:select_stmt, :values_lists],
               [:select_stmt, :sort_clause],
               [:select_stmt, :locking_clause],
               [:select_stmt, :target_list, 0],
               [:select_stmt, :target_list, 0, :res_target],
               [:select_stmt, :target_list, 0, :res_target, :indirection],
               [:select_stmt, :target_list, 0, :res_target, :val],
               [:select_stmt, :target_list, 0, :res_target, :val, :string]
             ]
    end

    test "traverses list nodes" do
      # Create a node with a list
      list_node = %PgQuery.List{
        items: [
          %PgQuery.Node{node: {:string, %PgQuery.String{sval: "item1"}}},
          %PgQuery.Node{node: {:string, %PgQuery.String{sval: "item2"}}}
        ]
      }

      root_node = %PgQuery.Node{node: {:list, list_node}}

      # Collect visited nodes
      visited =
        TreeWalker.walk(root_node, [], fn node, field_name, {child_node, location}, acc ->
          [{node, field_name, child_node, location} | acc]
        end)

      locations =
        visited
        |> Enum.map(fn {_, _, _, loc} -> loc end)
        |> Enum.reverse()

      assert locations == [
               [:list],
               [:list, :items],
               [:list, :items, 0],
               [:list, :items, 1],
               [:list, :items, 0, :string],
               [:list, :items, 1, :string]
             ]
    end

    test "handles empty or nil nodes" do
      # Test with nil node
      assert TreeWalker.walk(nil, [], fn _, _, _, acc -> acc end) == []

      # Test with empty message
      empty_node = %PgQuery.Node{node: nil}
      assert TreeWalker.walk(empty_node, [], fn _, _, _, acc -> acc end) == []
    end

    test "accumulates values correctly" do
      # Create a node structure
      node =
        %PgQuery.Node{
          node:
            {:select_stmt,
             %PgQuery.SelectStmt{
               distinct_clause: [],
               target_list: [
                 %PgQuery.Node{
                   node:
                     {:res_target,
                      %PgQuery.ResTarget{
                        name: "col1",
                        indirection: [],
                        location: 0
                      }}
                 }
               ],
               from_clause: [],
               group_clause: [],
               window_clause: [],
               values_lists: [],
               sort_clause: [],
               locking_clause: []
             }}
        }

      # accumulated value == the number of nodes visited
      count = TreeWalker.walk(node, 0, fn _, _, _, acc -> acc + 1 end)

      assert count == 12
    end
  end
end
