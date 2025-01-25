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
              current_cte: nil
  end

  def nodes(%PgQuery.ParseResult{stmts: stmts}) do
    Enum.flat_map(stmts, fn
      %{stmt: %{node: {_type, node}}} ->
        traverse_node(node, %Ctx{})
        |> List.flatten()
        |> Enum.reverse()

      _ ->
        []
    end)
  end

  defp traverse_node(%PgQuery.Node{node: {_type, node}}, ctx) do
    traverse_node(node, ctx)
  end

  defp traverse_node(%PgQuery.SelectStmt{} = node, ctx) do
    updated_ctx = %Ctx{ctx | depth: ctx.depth + 1, type: :select}

    [
      traverse_nodes(node.target_list, updated_ctx),
      traverse_maybe_node(node.where_clause, %Ctx{updated_ctx | has_filter: true}),
      traverse_nodes(node.sort_clause, updated_ctx),
      traverse_nodes(node.group_clause, updated_ctx),
      traverse_maybe_node(node.having_clause, updated_ctx),
      traverse_maybe_node(node.with_clause, updated_ctx),
      case node.op do
        :SETOP_NONE ->
          traverse_nodes(node.from_clause, updated_ctx)

        op when op in ~w(SETOP_UNION SETOP_INTERSECT SETOP_EXCEPT)a ->
          [
            traverse_maybe_node(node.larg, updated_ctx),
            traverse_maybe_node(node.rarg, updated_ctx)
          ]

        _ ->
          []
      end,
      {node, ctx}
    ]
  end

  defp traverse_node(%PgQuery.ResTarget{} = node, ctx) do
    [
      traverse_maybe_node(node.val, %Ctx{depth: ctx.depth + 1}),
      {node, %Ctx{ctx | depth: ctx.depth + 1}}
    ]
  end

  defp traverse_node(%PgQuery.FuncCall{} = node, ctx) do
    [
      traverse_nodes(node.args, %Ctx{ctx | depth: ctx.depth + 1}),
      {node, %Ctx{ctx | depth: ctx.depth + 1}}
    ]
  end

  defp traverse_node(%PgQuery.ColumnRef{} = node, ctx) do
    [{node, ctx}]
  end

  defp traverse_node(%PgQuery.WithClause{} = node, ctx) do
    traverse_nodes(node.ctes, ctx)
  end

  defp traverse_node(%PgQuery.CommonTableExpr{} = node, ctx) do
    [
      traverse_node(
        node.ctequery,
        %Ctx{
          ctx
          | depth: ctx.depth + 1,
            current_cte: {node.ctename, node.cterecursive}
        }
      ),
      {node, ctx}
    ]
  end

  defp traverse_node(%PgQuery.RangeVar{} = node, ctx) do
    [{node, ctx}]
  end


  defp traverse_maybe_node(nil, _ctx), do: []

  defp traverse_maybe_node(node, ctx) do
    traverse_node(node, ctx)
  end

  defp traverse_nodes(nodes, ctx) when is_list(nodes) do
    Enum.flat_map(nodes, &traverse_maybe_node(&1, ctx))
  end
end
