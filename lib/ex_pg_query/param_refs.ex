defmodule ExPgQuery.ParamRefs do
  alias ExPgQuery.TreeWalker

  def param_refs(tree) do
    TreeWalker.walk(tree, [], fn parent_node, field_name, {node, path}, acc ->
      case node do
        %PgQuery.ParamRef{} ->
          case Enum.take(path, -3) do
            [:type_cast, :arg, :param_ref] ->
              # skip - already handled in %PgQuery.TypeCast{} match
              acc

            _ ->
              [
                %{
                  location: node.location,
                  length: param_ref_length(node)
                }
                | acc
              ]
          end

        %PgQuery.TypeCast{
          arg: %PgQuery.Node{
            node: {:param_ref, param_ref_node}
          },
          type_name: %PgQuery.TypeName{} = type_name_node
        } ->
          length = param_ref_length(param_ref_node)
          param_loc = param_ref_node.location
          type_loc = type_name_node.location

          {length, location} =
            cond do
              # xxx: not sure when the following happens or even if it can happen.
              #      the behaviour is copied from the ruby library, but the test
              #      suite didn't cause this branch to be executed
              param_loc == -1 ->
                {length, type_name_node.location}

              type_loc < param_loc ->
                {length + param_loc - type_loc, type_name_node.location}

              true ->
                {length, param_loc}
            end

          [
            %{
              location: location,
              length: length,
              typename:
                Enum.map(type_name_node.names, fn %PgQuery.Node{node: {:string, string}} ->
                  string.sval
                end)
            }
            | acc
          ]

        _ ->
          acc
      end
    end)
    |> Enum.sort_by(& &1.location)
  end

  defp param_ref_length(node) do
    if node.number == 0 do
      # Actually a `?` replacement character
      1
    else
      String.length("$#{node.number}")
    end
  end
end
