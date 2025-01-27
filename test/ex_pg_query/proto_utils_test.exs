defmodule ExPgQuery.ProtoUtilsTest do
  use ExUnit.Case
  alias ExPgQuery.Parser
  alias ExPgQuery.ProtoUtils

  describe "update_in_proto/3" do
    test "updates a simple field in a select statement" do
      {:ok, result} = ExPgQuery.parse_protobuf("SELECT * FROM users")
      path = [:stmts, 0, :stmt, :select_stmt, :limit_count]

      updated = ProtoUtils.update_in_proto(result, path, fn _ ->
        %PgQuery.Node{
          node: {:a_const, %PgQuery.A_Const{
            val: {:integer, %PgQuery.Integer{ival: 10}}
          }}
        }
      end)

      assert get_in_path(updated, path).node == {
        :a_const,
        %PgQuery.A_Const{val: {:integer, %PgQuery.Integer{ival: 10}}}
      }
    end

    # test "updates a list by adding a new element" do
    #   {:ok, result} = ExPgQuery.parse_protobuf("SELECT id, name FROM users")
    #   path = [:stmts, 0, :stmt, :select_stmt, :target_list]

    #   updated = ProtoUtils.update_in_proto(result, path, fn target_list ->
    #     target_list ++ [
    #       %PgQuery.Node{
    #         node: {:res_target, %PgQuery.ResTarget{
    #           name: "email",
    #           val: %PgQuery.Node{
    #             node: {:column_ref, %PgQuery.ColumnRef{
    #               fields: [
    #                 %PgQuery.Node{node: {:string, %PgQuery.String{sval: "email"}}}
    #               ]
    #             }}
    #           }
    #         }}
    #       }
    #     ]
    #   end)

    #   new_target_list = get_in_path(updated, path)
    #   assert length(new_target_list) == 3
    #   assert get_in_node(List.last(new_target_list), [:res_target, :name]) == "email"
    # end

    # test "updates a deeply nested node value" do
    #   {:ok, result} = ExPgQuery.parse_protobuf("SELECT * FROM users WHERE id = 1")
    #   path = [:stmts, 0, :stmt, :select_stmt, :where_clause, :node, :a_expr, :rexpr]

    #   updated = ProtoUtils.update_in_proto(result, path, fn _ ->
    #     %PgQuery.Node{
    #       node: {:a_const, %PgQuery.A_Const{
    #         val: {:integer, %PgQuery.Integer{ival: 42}}
    #       }}
    #     }
    #   end)

    #   where_val = get_in_path(updated, path)
    #   assert get_in_node(where_val, [:a_const, :val, :integer, :ival]) == 42
    # end

    # test "updates multiple nodes at once" do
    #   {:ok, result} = ExPgQuery.parse_protobuf("SELECT id, name FROM users LIMIT 5")
    #   paths = [
    #     [:stmts, 0, :stmt, :select_stmt, :limit_count],
    #     [:stmts, 0, :stmt, :select_stmt, :limit_offset]
    #   ]

    #   updated = Enum.reduce(paths, result, fn path, acc ->
    #     ProtoUtils.update_in_proto(acc, path, fn _ ->
    #       %PgQuery.Node{
    #         node: {:a_const, %PgQuery.A_Const{
    #           val: {:integer, %PgQuery.Integer{ival: 10}}
    #         }}
    #       }
    #     end)
    #   end)

    #   assert get_in_node(get_in_path(updated, Enum.at(paths, 0)), [:a_const, :val, :integer, :ival]) == 10
    #   assert get_in_node(get_in_path(updated, Enum.at(paths, 1)), [:a_const, :val, :integer, :ival]) == 10
    # end

    # test "handles non-existent paths gracefully" do
    #   {:ok, result} = ExPgQuery.parse_protobuf("SELECT * FROM users")
    #   path = [:stmts, 0, :stmt, :nonexistent, :field]

    #   assert_raise ArgumentError, fn ->
    #     ProtoUtils.update_in_proto(result, path, fn _ -> "new value" end)
    #   end
    # end
  end

  # Helper functions for traversing nodes in tests
  defp get_in_path(proto, path) do
    Enum.reduce(path, proto, fn
      key, %{node: {_type, node}} when is_atom(key) -> Map.get(node, key)
      key, map when is_map(map) and is_atom(key) -> Map.get(map, key)
      key, list when is_list(list) and is_integer(key) -> Enum.at(list, key)
    end)
  end

  defp get_in_node(node, []), do: node
  defp get_in_node(%{node: {type, node}}, [type | rest]), do: get_in_node(node, rest)
  defp get_in_node(map, [key, :integer, :ival | rest]) when is_map(map) do
    case map do
      %{val: {:integer, %PgQuery.Integer{ival: val}}} -> get_in_node(val, rest)
      _ -> nil
    end
  end
  defp get_in_node(map, [key | rest]) when is_map(map), do: get_in_node(Map.get(map, key), rest)
  defp get_in_node(value, []), do: value
end
