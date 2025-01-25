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

  # defmodule Context do
  #   @moduledoc "Represents the context in which a node appears"
  #   @type t :: :none | :select | :dml | :ddl | :call
  # end

  # @type node_info ::
  #         {node :: struct(), depth :: non_neg_integer(), context :: Context.t(),
  #          has_filter :: boolean()}

  @doc """
  Recursively traverses a parse result to extract all relevant nodes with their context.
  """

  # @spec traverse(PgQuery.ParseResult.t()) :: [node_info()]
  # def traverse(%PgQuery.ParseResult{stmts: stmts}) do
  #   stmts
  #   |> Enum.flat_map(fn
  #     %{stmt: %{node: {_type, node}}} -> traverse_node(node, 0, :none, false)
  #     _ -> []
  #   end)
  # end

  # # Main recursive traversal function that handles different node types
  # @spec traverse_node(struct(), non_neg_integer(), Context.t(), boolean()) :: [node_info()]
  # def traverse_node(node, depth, context, has_filter)

  # # SelectStmt nodes - demonstrate handling a complex statement type
  # def traverse_node(%PgQuery.SelectStmt{} = stmt, depth, _context, has_filter) do
  #   next_depth = depth + 1

  #   # Collect nodes from different parts of the SELECT
  #   [
  #     # Handle target list (SELECT clause)
  #     traverse_nodes(stmt.target_list, next_depth, :select, false),

  #     # Handle WHERE clause - note has_filter set to true
  #     traverse_maybe_node(stmt.where_clause, next_depth, :select, true),

  #     # Handle FROM clause
  #     traverse_nodes(stmt.from_clause, next_depth, :select, false),

  #     # Handle other clauses
  #     traverse_nodes(stmt.sort_clause, next_depth, :select, false),
  #     traverse_nodes(stmt.group_clause, next_depth, :select, false),
  #     traverse_maybe_node(stmt.having_clause, next_depth, :select, false),

  #     # Handle WITH clause if present
  #     case stmt.with_clause do
  #       %PgQuery.WithClause{ctes: ctes} -> traverse_nodes(ctes, next_depth, :select, false)
  #       nil -> []
  #     end,

  #     # Handle set operations (UNION, EXCEPT, etc)
  #     case stmt.op do
  #       :SETOP_NONE ->
  #         []

  #       _ ->
  #         [
  #           traverse_maybe_node(stmt.larg, next_depth, :select, false),
  #           traverse_maybe_node(stmt.rarg, next_depth, :select, false)
  #         ]
  #     end
  #   ]
  #   |> List.flatten()
  # end

  # def traverse_node(%PgQuery.CommonTableExpr{} = cte, depth, context, has_filter) do
  #   traverse_maybe_node(cte.ctequery, depth + 1, context, has_filter)
  # end

  # def traverse_node(%PgQuery.A_Expr{} = expr, depth, context, has_filter) do
  #   [
  #     traverse_maybe_node(expr.lexpr, depth + 1, context, has_filter),
  #     traverse_maybe_node(expr.rexpr, depth + 1, context, has_filter)
  #   ]
  #   |> List.flatten()
  # end

  # def traverse_node(%PgQuery.BoolExpr{args: args}, depth, context, has_filter) do
  #   traverse_nodes(args, depth + 1, context, has_filter)
  # end

  # def traverse_node(%PgQuery.BooleanTest{arg: arg}, depth, context, has_filter) do
  #   traverse_maybe_node(arg, depth + 1, context, has_filter)
  # end

  # def traverse_node(%PgQuery.CoalesceExpr{args: args}, depth, context, has_filter) do
  #   traverse_nodes(args, depth + 1, context, has_filter)
  # end

  # def traverse_node(%PgQuery.MinMaxExpr{args: args}, depth, context, has_filter) do
  #   traverse_nodes(args, depth + 1, context, has_filter)
  # end

  # def traverse_node(%PgQuery.NullTest{arg: arg}, depth, context, has_filter) do
  #   traverse_maybe_node(arg, depth + 1, context, has_filter)
  # end

  # def traverse_node(%PgQuery.ResTarget{val: val}, depth, context, has_filter) do
  #   traverse_maybe_node(val, depth + 1, context, has_filter)
  # end

  # def traverse_node(%PgQuery.SubLink{subselect: subselect}, depth, context, has_filter) do
  #   traverse_maybe_node(subselect, depth + 1, context, has_filter)
  # end

  # def traverse_node(%PgQuery.FuncCall{} = call, depth, context, has_filter) do
  #   [

  #     traverse_nodes(call.args, depth + 1, context, has_filter)
  #   ]
  # end

  # def traverse_node(%PgQuery.CaseExpr{} = expr, depth, context, has_filter) do
  #   [
  #     traverse_nodes(expr.args, depth + 1, context, has_filter),
  #     traverse_maybe_node(expr.defresult, depth + 1, context, has_filter)
  #   ]
  #   |> List.flatten()
  # end

  # def traverse_node(%PgQuery.CaseWhen{} = when_expr, depth, context, has_filter) do
  #   [
  #     traverse_maybe_node(when_expr.expr, depth + 1, context, has_filter),
  #     traverse_maybe_node(when_expr.result, depth + 1, context, has_filter)
  #   ]
  #   |> List.flatten()
  # end

  # def traverse_node(%PgQuery.SortBy{node: node}, depth, context, has_filter) do
  #   traverse_maybe_node(node, depth + 1, context, has_filter)
  # end

  # def traverse_node(%PgQuery.TypeCast{arg: arg}, depth, context, has_filter) do
  #   traverse_maybe_node(arg, depth + 1, context, has_filter)
  # end

  # # FROM clause related nodes

  # def traverse_node(%PgQuery.JoinExpr{} = join, depth, context, has_filter) do
  #   [
  #     traverse_maybe_node(join.larg, depth + 1, context, has_filter),
  #     traverse_maybe_node(join.rarg, depth + 1, context, has_filter),
  #     traverse_maybe_node(join.quals, depth + 1, context, has_filter)
  #   ]
  #   |> List.flatten()
  # end

  # def traverse_node(%PgQuery.RangeSubselect{subquery: subquery}, depth, context, has_filter) do
  #   traverse_maybe_node(subquery, depth + 1, context, has_filter)
  # end

  # def traverse_node(%PgQuery.RangeFunction{functions: functions}, depth, context, has_filter) do
  #   traverse_nodes(functions, depth + 1, context, has_filter)
  # end

  # def traverse_node(%PgQuery.RowExpr{args: args}, depth, context, has_filter) do
  #   traverse_nodes(args, depth + 1, context, has_filter)
  # end

  # # Terminal nodes (nodes that actually appear in the final result)

  # def traverse_node(%PgQuery.RangeVar{} = node, depth, context, has_filter) do
  #   [{node, depth, context, has_filter}]
  # end

  # def traverse_node(%PgQuery.ColumnRef{} = node, depth, context, has_filter) do
  #   [{node, depth, context, has_filter}]
  # end

  # def traverse_node(%PgQuery.A_Const{} = node, depth, context, has_filter) do
  #   [{node, depth, context, has_filter}]
  # end

  # def traverse_node(%PgQuery.Param{} = node, depth, context, has_filter) do
  #   [{node, depth, context, has_filter}]
  # end

  # def traverse_node(%PgQuery.RangeVar{} = node, depth, context, has_filter) do
  #   [{node, depth, context, has_filter}]
  # end

  # def traverse_node(%PgQuery.ColumnRef{} = node, depth, context, has_filter) do
  #   [{node, depth, context, has_filter}]
  # end

  # # List node - special case since it's a container
  # def traverse_node(%PgQuery.List{items: items}, depth, context, has_filter) do
  #   traverse_nodes(items, depth + 1, context, has_filter)
  # end

  # # Fallback for unhandled nodes
  # # def traverse_node(_, _depth, _context, _has_filter), do: []

  # # Helper functions for common traversal patterns

  defp traverse_maybe_node(nil, _ctx), do: []

  defp traverse_maybe_node(node, ctx) do
    traverse_node(node, ctx)
  end

  # # defp traverse_maybe_node(_, _depth, _context, _has_filter), do: []

  defp traverse_nodes(nodes, ctx) when is_list(nodes) do
    Enum.flat_map(nodes, &traverse_maybe_node(&1, ctx))
  end

  # # defp traverse_nodes(_, _depth, _context, _has_filter), do: []

  # # Example terminal nodes that should be included in results
end
