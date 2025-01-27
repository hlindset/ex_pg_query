defmodule ExPgQuery.NodeTraversal do
  @moduledoc """
  Recursively traverses a PostgreSQL AST to extract nodes and their context.
  """

  defmodule Ctx do
    @moduledoc "Represents the context in which a node appears"
    @type stmt_type :: :none | :select | :dml | :ddl | :call

    defstruct type: nil,
              depth: 0,
              has_filter: false,
              current_cte: nil,
              is_recursive_cte: false
  end

  def nodes(%PgQuery.ParseResult{} = tree) do
    traverse_node(tree, ctx_for_node(nil, tree, %Ctx{}))
    |> List.flatten()
  end

  defp traverse_node(nil, _ctx), do: []

  defp traverse_node(node_list, ctx) when is_list(node_list) do
    Enum.map(node_list, &traverse_node(&1, ctx))
  end

  defp traverse_node(node, ctx) when is_struct(node) do
    children =
      msg_to_field_nodes(node)
      |> Enum.map(&traverse_node(&1.value, ctx_for_node(node, &1, ctx)))

    [{node, ctx} | children]
  end

  defp ctx_for_node(%PgQuery.SelectStmt{}, %{key: :where_clause}, ctx), do: %Ctx{ctx | type: :select, has_filter: true}
  defp ctx_for_node(%PgQuery.SelectStmt{}, _node, ctx), do: %Ctx{ctx | type: :select}
  defp ctx_for_node(%PgQuery.WithClause{recursive: recursive}, _node, ctx), do: %Ctx{ctx | is_recursive_cte: recursive}
  defp ctx_for_node(%PgQuery.CommonTableExpr{ctename: ctename}, _node, ctx), do: %Ctx{ctx | current_cte: ctename}
  defp ctx_for_node(_parent, _node, ctx), do: ctx

  defp msg_to_field_nodes(msg) do
    apply(msg.__struct__, :fields_defs, [])
    |> Enum.filter(fn
      %Protox.Field{type: {:message, _}} -> true
      _ -> false
    end)
    |> Enum.map(fn field_def ->
      case field_def do
        # traverse oneof fields in %PgQuery.Node{}
        %Protox.Field{kind: {:oneof, oneof_field}, name: name} ->
          case Map.get(msg, oneof_field) do
            {^name, value} -> %{key: field_def.name, value: value}
            _ -> %{key: field_def.name, value: nil}
          end

        _ ->
          %{key: field_def.name, value: Map.get(msg, field_def.name)}
      end
    end)
  end
end
