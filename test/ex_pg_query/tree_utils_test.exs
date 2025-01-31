defmodule ExPgQuery.TreeUtilsTest do
  use ExUnit.Case, async: true

  alias ExPgQuery.TreeUtils

  describe "update_in_tree/3" do
    test "updates a simple value" do
      tree = %{a: 1}
      assert {:ok, %{a: 2}} = TreeUtils.update_in_tree(tree, [:a], fn _ -> 2 end)
    end

    test "updates a nested value" do
      tree = %{a: %{b: 1}}
      assert {:ok, %{a: %{b: 2}}} = TreeUtils.update_in_tree(tree, [:a, :b], fn _ -> 2 end)
    end

    test "updates a value in a list" do
      tree = %{items: [1, 2, 3]}

      assert {:ok, %{items: [1, 99, 3]}} =
               TreeUtils.update_in_tree(tree, [:items, 1], fn _ -> 99 end)
    end

    test "handles nil item in list" do
      tree = %{items: [nil, 2, 3]}

      assert {:error, "index 0 out of bounds"} =
               TreeUtils.update_in_tree(tree, [:items, 0], fn _ -> 99 end)
    end

    test "handles index out of bounds" do
      tree = %{items: [1, 2, 3]}

      assert {:error, "index 5 out of bounds"} =
               TreeUtils.update_in_tree(tree, [:items, 5], fn _ -> 99 end)
    end

    test "handles missing keys" do
      tree = %{a: 1}

      assert {:error, "key b not found"} =
               TreeUtils.update_in_tree(tree, [:b], fn _ -> 2 end)
    end

    test "updates a PgQuery.Node" do
      tree = %PgQuery.Node{
        node: {:select_stmt, %PgQuery.SelectStmt{where_clause: nil}}
      }

      new_where = %PgQuery.Node{node: {:column_ref, %PgQuery.ColumnRef{fields: []}}}

      {:ok, updated} =
        TreeUtils.update_in_tree(tree, [:select_stmt, :where_clause], fn _ -> new_where end)

      assert updated == %PgQuery.Node{
               node: {:select_stmt, %PgQuery.SelectStmt{where_clause: new_where}}
             }
    end

    test "handles error propagation in PgQuery.Node update" do
      tree = %PgQuery.Node{
        node: {:select_stmt, %PgQuery.SelectStmt{}}
      }

      assert {:error, "key invalid_key not found"} =
               TreeUtils.update_in_tree(tree, [:select_stmt, :invalid_key], fn _ -> nil end)
    end

    test "handles mismatched node types" do
      tree = %PgQuery.Node{
        node: {:select_stmt, %PgQuery.SelectStmt{}}
      }

      assert {:error, "expected node type update_stmt but found select_stmt"} =
               TreeUtils.update_in_tree(tree, [:update_stmt], fn _ -> nil end)
    end

    test "handles error in oneof field update" do
      tree = %{key: {:type1, %{invalid_key: 1}}}

      assert {:error, "key value not found"} =
               TreeUtils.update_in_tree(tree, [:key, :value], fn _ -> 2 end)
    end

    test "handles error in regular field update" do
      tree = %{key: %{invalid_key: 1}}

      assert {:error, "key value not found"} =
               TreeUtils.update_in_tree(tree, [:key, :value], fn _ -> 2 end)
    end

    test "handles oneof fields" do
      tree = %{key: {:type1, %{value: 1}}}

      {:ok, updated} = TreeUtils.update_in_tree(tree, [:key, :value], fn _ -> 2 end)

      assert updated == %{key: {:type1, %{value: 2}}}
    end
  end

  describe "update_in_tree!/3" do
    test "successfully updates value" do
      tree = %{a: 1}
      assert %{a: 2} = TreeUtils.update_in_tree!(tree, [:a], fn _ -> 2 end)
    end

    test "raises error on failure" do
      tree = %{a: 1}

      assert_raise RuntimeError, "Update error: \"key b not found\"", fn ->
        TreeUtils.update_in_tree!(tree, [:b], fn _ -> 2 end)
      end
    end
  end

  describe "put_in_tree/3" do
    test "sets a value directly" do
      tree = %{a: 1}
      assert {:ok, %{a: 2}} = TreeUtils.put_in_tree(tree, [:a], 2)
    end

    test "handles nested paths" do
      tree = %{a: %{b: 1}}
      assert {:ok, %{a: %{b: 2}}} = TreeUtils.put_in_tree(tree, [:a, :b], 2)
    end

    test "handles error cases" do
      tree = %{a: 1}
      assert {:error, "key b not found"} = TreeUtils.put_in_tree(tree, [:b], 2)
    end
  end
end
