defmodule ExPgQuery.TreeUtils do
  @moduledoc """
  Utilities for working with PgQuery Protobuf structures.

  This module provides functions for safely traversing and updating nested Protobuf
  structures generated from PostgreSQL queries. It handles:

  - Nested map structures
  - List elements
  - PgQuery.Node types
  - Oneof fields
  - Error propagation

  All operations are safe and return either {:ok, result} or {:error, reason}.
  """

  @doc """
  Updates a value deep within a nested structure following the given path.

  ## Parameters

    * `tree` - The root structure to update
    * `path` - A list of keys/indices to traverse
    * `update_fn` - Function that receives the old value and returns the new value

  ## Returns

    * `{:ok, updated_tree}` on success
    * `{:error, reason}` on failure

  ## Examples

      iex> tree = %{a: %{b: 1}}
      iex> TreeUtils.update_in_tree(tree, [:a, :b], fn _ -> 2 end)
      {:ok, %{a: %{b: 2}}}

      iex> tree = %{items: [1, 2, 3]}
      iex> TreeUtils.update_in_tree(tree, [:items, 1], fn _ -> 99 end)
      {:ok, %{items: [1, 99, 3]}}
  """
  def update_in_tree(tree, [], update_fn) when is_function(update_fn) do
    {:ok, update_fn.(tree)}
  end

  def update_in_tree(tree, [key, index | rest], update_fn) when is_integer(index) do
    with {:ok, list} <- Map.fetch(tree, key),
         item when not is_nil(item) <- Enum.at(list, index),
         {:ok, updated} <- update_in_tree(item, rest, update_fn) do
      {:ok, Map.put(tree, key, List.replace_at(list, index, updated))}
    else
      nil -> {:error, "index #{index} out of bounds"}
      :error -> {:error, "key #{key} not found"}
      {:error, reason} -> {:error, reason}
    end
  end

  def update_in_tree(%PgQuery.Node{node: {node_type, struct}}, [type | rest], update_fn)
      when node_type == type do
    case update_in_tree(struct, rest, update_fn) do
      {:ok, updated} -> {:ok, %PgQuery.Node{node: {type, updated}}}
      {:error, reason} -> {:error, reason}
    end
  end

  def update_in_tree(%PgQuery.Node{node: {other_type, _}}, [type | _rest], _update_fn) do
    {:error, "expected node type #{type} but found #{other_type}"}
  end

  def update_in_tree(tree, [key | rest], update_fn) do
    with {:ok, value} <- Map.fetch(tree, key) do
      case value do
        {oneof_type, struct} when is_atom(oneof_type) ->
          case update_in_tree(struct, rest, update_fn) do
            {:ok, updated} -> {:ok, Map.put(tree, key, {oneof_type, updated})}
            {:error, reason} -> {:error, reason}
          end

        value ->
          case update_in_tree(value, rest, update_fn) do
            {:ok, updated} -> {:ok, Map.put(tree, key, updated)}
            {:error, reason} -> {:error, reason}
          end
      end
    else
      :error -> {:error, "key #{key} not found"}
    end
  end

  @doc """
  Bang version of update_in_tree/3 that raises an error on failure.

  ## Parameters

    * `tree` - The root structure to update
    * `path` - A list of keys/indices to traverse
    * `update_fn` - Function that receives the old value and returns the new value

  ## Returns

    * The updated tree on success

  ## Raises

    * RuntimeError with the error message on failure

  ## Examples

      iex> tree = %{a: 1}
      iex> TreeUtils.update_in_tree!(tree, [:a], fn _ -> 2 end)
      %{a: 2}

      iex> tree = %{a: 1}
      iex> TreeUtils.update_in_tree!(tree, [:b], fn _ -> 2 end)
      ** (RuntimeError) Update error: "key b not found"
  """
  def update_in_tree!(tree, path, update_fn) do
    case update_in_tree(tree, path, update_fn) do
      {:ok, updated} -> updated
      {:error, error} -> raise "Update error: #{inspect(error)}"
    end
  end

  @doc """
  Convenience function for setting a value directly in a nested structure.

  This is a wrapper around update_in_tree/3 that simplifies the common case of
  simply wanting to set a value rather than transform it with a function.

  ## Parameters

    * `tree` - The root structure to update
    * `path` - A list of keys/indices to traverse
    * `value` - The value to set at the specified path

  ## Returns

    * `{:ok, updated_tree}` on success
    * `{:error, reason}` on failure

  ## Examples

      iex> tree = %{a: 1}
      iex> TreeUtils.put_in_tree(tree, [:a], 2)
      {:ok, %{a: 2}}

      iex> tree = %{a: %{b: 1}}
      iex> TreeUtils.put_in_tree(tree, [:a, :b], 2)
      {:ok, %{a: %{b: 2}}}
  """
  def put_in_tree(tree, path, value) do
    update_in_tree(tree, path, fn _old -> value end)
  end
end
