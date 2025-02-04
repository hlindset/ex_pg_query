defmodule ExPgQuery.TreeWalker do
  @moduledoc """
  Traverses a PgQuery AST, yielding each node to a callback function.

  The walker maintains a path (location) to each node as it traverses the tree,
  allowing for precise tracking of node positions in the overall structure.
  """

  @doc """
  Walks through a Protobuf message tree, applying a callback function to each node.

  ## Parameters

    * `tree` - The root node of the Protobuf message tree to traverse
    * `acc` - The initial accumulator value
    * `callback` - A function that receives four arguments:
      * The parent node
      * The field name or index
      * A tuple of `{current_node, location}`
      * The current accumulator value

  ## Returns

  Returns the final accumulator value after traversing all nodes.

  ## Example

      iex> {:ok, tree} = ExPgQuery.Protobuf.from_sql("SELECT id, name FROM users WHERE age > 21")
      iex> columns = ExPgQuery.TreeWalker.walk(tree, [], fn
      ...>   _parent, _field, {%PgQuery.ColumnRef{} = col, _loc}, acc ->
      ...>     [col | acc]
      ...>   _parent, _field, _node, acc ->
      ...>     acc
      ...> end)
      [
        %PgQuery.ColumnRef{fields: [%PgQuery.Node{node: {:string, %PgQuery.String{sval: "age"}}}], location: 33},
        %PgQuery.ColumnRef{fields: [%PgQuery.Node{node: {:string, %PgQuery.String{sval: "name"}}}], location: 11},
        %PgQuery.ColumnRef{fields: [%PgQuery.Node{node: {:string, %PgQuery.String{sval: "id"}}}], location: 7}
      ]

  """
  def walk(tree, acc, callback) do
    {_, result} = do_walk([{tree, []}], acc, callback)
    result
  end

  # Handles the base case when there are no more nodes to process
  defp do_walk([] = nodes, acc, _callback), do: {nodes, acc}

  # Main traversal function that processes each node and its children
  # Handles three types of nodes:
  # 1. Protobuf messages (structs)
  # 2. Lists of nodes
  # 3. Terminal values (neither messages nor lists)
  defp do_walk([{parent_node, parent_location} | rest], acc, callback) do
    {nodes, new_acc} =
      case parent_node do
        %{__struct__: _} = msg ->
          fields = msg_to_field_nodes(msg)

          Enum.reduce(fields, {[], acc}, fn {field_name, node}, {nodes_acc, new_acc} ->
            location = parent_location ++ [field_name]

            {nodes_acc ++ [{node, location}],
             callback.(parent_node, field_name, {node, location}, new_acc)}
          end)

        nodes when is_list(nodes) ->
          Enum.reduce(Enum.with_index(nodes), {[], acc}, fn {node, idx}, {nodes_acc, new_acc} ->
            location = parent_location ++ [idx]

            {nodes_acc ++ [{node, location}],
             callback.(parent_node, idx, {node, location}, new_acc)}
          end)

        _ ->
          {[], acc}
      end

    do_walk(nodes ++ rest, new_acc, callback)
  end

  # Converts a Protobuf message into a list of field nodes that should be traversed
  # Only includes message-type fields and handles special case of oneof fields in PgQuery.Node
  defp msg_to_field_nodes(msg) do
    apply(msg.__struct__, :fields_defs, [])
    |> Enum.filter(fn
      %Protox.Field{type: {:message, _}} -> true
      _ -> false
    end)
    |> Enum.map(fn field_def ->
      {field_def.name,
       case field_def do
         # traverse oneof fields in %PgQuery.Node{}
         %Protox.Field{kind: {:oneof, :node}, name: name} ->
           case msg.node do
             {^name, value} -> value
             _ -> nil
           end

         _ ->
           Map.get(msg, field_def.name)
       end}
    end)
    |> Enum.reject(fn
      {_, nil} -> true
      _ -> false
    end)
  end
end
