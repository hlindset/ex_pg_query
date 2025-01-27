defmodule ExPgQuery.ProtoUtils do
  @moduledoc """
  Utilities for working with libpg_query protobufs.
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

  def update_in_tree!(tree, path, update_fn) do
    case update_in_tree(tree, path, update_fn) do
      {:ok, updated} -> updated
      {:error, error} -> raise "Update error: #{inspect(error)}"
    end
  end

  @doc """
  Convenience function for setting a value directly instead of providing an update function.
  """
  def put_in_tree(tree, path, value) do
    update_in_tree(tree, path, fn _old -> value end)
  end
end
