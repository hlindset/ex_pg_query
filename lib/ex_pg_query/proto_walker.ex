defmodule ExPgQuery.ProtoWalker do
  @moduledoc """
  Traverses a Protobuf message tree, yielding each node to a callback function.

  Can traverse with either a simple node callback or with full location tracking.
  """
  def walk(tree, acc, callback) do
    {_, result} = do_walk([{tree, []}], acc, callback)
    result
  end

  # # Walking with location tracking
  defp do_walk([] = nodes, acc, _callback), do: {nodes, acc}

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
