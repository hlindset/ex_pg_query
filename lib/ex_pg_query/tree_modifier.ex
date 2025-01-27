defmodule ExPgQuery.TreeModifier do
  @moduledoc """
  Functions for modifying protobuf trees immutably.
  Provides utilities to update nodes based on predicates or locations.
  """

  @doc """
  Updates all nodes in the tree that match a predicate function.
  The update_fn receives the matching node and should return the modified node.

  ## Example:
      iex> TreeModifier.update_nodes(tree, &match?(%SelectStmt{}, &1), fn node ->
      ...>   %{node | where_clause: new_where}
      ...> end)
  """
  def update_nodes(tree, predicate, update_fn) when is_function(predicate, 1) and is_function(update_fn, 1) do
    do_update(tree, predicate, update_fn)
  end

  @doc """
  Updates a node at a specific location in the tree.
  Location should be a list of keys/indices representing the path to the node.

  ## Example:
      iex> TreeModifier.update_at_location(tree, [:stmts, 0, :stmt, :select_stmt], fn node ->
      ...>   %{node | where_clause: new_where}
      ...> end)
  """
  def update_at_location(tree, location, update_fn) when is_list(location) and is_function(update_fn, 1) do
    do_update_at_location(tree, location)
  end

  # Private implementation for updating nodes based on predicate
  defp do_update(node, predicate, update_fn) do
    cond do
      is_struct(node) ->
        node = if predicate.(node), do: update_fn.(node), else: node
        
        node
        |> Map.from_struct()
        |> Enum.map(fn {k, v} -> {k, do_update(v, predicate, update_fn)} end)
        |> then(&struct(node.__struct__, &1))

      is_list(node) ->
        Enum.map(node, &do_update(&1, predicate, update_fn))

      true ->
        node
    end
  end

  # Private implementation for updating node at specific location
  defp do_update_at_location(node, []), do: node

  defp do_update_at_location(node, [key | rest]) when is_struct(node) do
    current_value = Map.get(node, key)
    updated_value = do_update_at_location(current_value, rest)
    %{node | key => updated_value}
  end

  defp do_update_at_location(list, [index | rest]) when is_list(list) and is_integer(index) do
    List.update_at(list, index, &do_update_at_location(&1, rest))
  end

  @doc """
  Updates all nodes of a specific type in the tree.
  Shorthand for update_nodes/3 with a type-checking predicate.

  ## Example:
      iex> TreeModifier.update_type(tree, PgQuery.SelectStmt, fn node ->
      ...>   %{node | where_clause: new_where}
      ...> end)
  """
  def update_type(tree, type, update_fn) do
    update_nodes(tree, &match?(^type, &1.__struct__), update_fn)
  end

  @doc """
  Returns paths to all nodes that match the given predicate.
  Useful for finding where in the tree certain nodes exist.

  ## Example:
      iex> TreeModifier.find_paths(tree, &match?(%SelectStmt{}, &1))
      [[:stmts, 0, :stmt, :select_stmt], ...]
  """
  def find_paths(tree, predicate) when is_function(predicate, 1) do
    {_, paths} = do_find_paths(tree, predicate, [])
    paths
  end

  defp do_find_paths(node, predicate, current_path) do
    cond do
      is_struct(node) ->
        matches = if predicate.(node), do: [current_path], else: []
        
        node
        |> Map.from_struct()
        |> Enum.reduce({node, matches}, fn {k, v}, {node, paths} ->
          {_, new_paths} = do_find_paths(v, predicate, current_path ++ [k])
          {node, paths ++ new_paths}
        end)

      is_list(node) ->
        Enum.reduce(Enum.with_index(node), {node, []}, fn {item, idx}, {node, paths} ->
          {_, new_paths} = do_find_paths(item, predicate, current_path ++ [idx])
          {node, paths ++ new_paths}
        end)

      true ->
        {node, []}
    end
  end
end