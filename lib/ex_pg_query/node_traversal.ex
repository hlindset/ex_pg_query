defmodule ExPgQuery.NodeTraversal do
  @moduledoc """
  Recursively traverses a PostgreSQL AST to extract nodes and their context.

  This module provides functionality to walk through a parsed PostgreSQL query tree,
  collecting each node along with contextual information about where it appears in
  the query structure.

  The traversal maintains context about:
  - The type of statement being processed (select, DML, etc.)
  - Whether we're in a filtering context (WHERE clause)
  - CTE (Common Table Expression) information
  - Position in the query tree
  """

  defmodule Ctx do
    @moduledoc """
    Represents the context in which a node appears in the query tree.

    Fields:
    - type: The type of statement (:none, :select, :dml, :ddl, :call)
    - has_filter: Whether we're in a filtering context (e.g., WHERE clause)
    - current_cte: Name of the CTE currently being processed, if any
    - is_recursive_cte: Whether the current CTE is recursive
    """
    @type stmt_type :: :none | :select | :dml | :ddl | :call

    defstruct type: nil,
              has_filter: false,
              current_cte: nil,
              is_recursive_cte: false,
              subselect_item: false,
              from_clause_item: false,
              join_clause_condition: false,
              where_condition: false,
              table_aliases: %{},
              cte_names: []
  end

  @doc """
  Traverses a ParseResult tree and returns a flattened list of nodes with their context.

  ## Parameters
    - parse_result: A PgQuery.ParseResult struct containing the parsed query

  ## Returns
    A list of tuples containing {node, context} pairs for each node in the tree.

  ## Example
      iex> ExPgQuery.NodeTraversal.nodes(parse_result)
      [{%PgQuery.SelectStmt{...}, %Ctx{type: :select}}, ...]
  """
  def nodes(%PgQuery.ParseResult{stmts: stmts}) do
    Enum.flat_map(stmts, fn
      %{stmt: %{node: {_type, node}}} ->
        traverse_node(node, %Ctx{})
        |> List.flatten()

      _ ->
        []
    end)
  end

  # Handles nil nodes by returning an empty list
  defp traverse_node(nil, _ctx), do: []

  # Handles lists of nodes by mapping traverse_node over each element
  defp traverse_node(node_list, ctx) when is_list(node_list) do
    Enum.map(node_list, &traverse_node(&1, ctx))
  end

  # Unwraps PgQuery.Node wrapper and continues traversal
  defp traverse_node(%PgQuery.Node{node: {_type, node}}, ctx) do
    traverse_node(node, ctx)
  end

  # Main traversal function for struct nodes
  defp traverse_node(node, ctx) when is_struct(node) do
    updated_ctx = ctx_for_node(node, ctx)

    children =
      msg_to_field_nodes(node)
      |> Enum.reject(fn {_key, value} -> is_nil(value) end)
      |> Enum.map(fn {key, value} ->
        traverse_node(value, ctx_for_field(node, key, updated_ctx))
      end)

    [{node, updated_ctx} | children]
  end

  # Updates context based on the type of node being processed
  defp ctx_for_node(%PgQuery.SelectStmt{} = select_stmt, ctx) do
    table_aliases = collect_aliases(select_stmt.from_clause)
    cte_names = collect_cte_names(select_stmt.with_clause)
    %Ctx{ctx | type: :select, table_aliases: table_aliases, cte_names: ctx.cte_names ++ cte_names}
  end

  defp ctx_for_node(node, ctx)
       when is_struct(node, PgQuery.CreateStmt) or
              is_struct(node, PgQuery.AlterTableStmt) or
              is_struct(node, PgQuery.CreateTableAsStmt) or
              is_struct(node, PgQuery.TruncateStmt) or
              is_struct(node, PgQuery.CreateTrigStmt) or
              is_struct(node, PgQuery.VacuumStmt) or
              is_struct(node, PgQuery.RefreshMatViewStmt) or
              is_struct(node, PgQuery.DropStmt) or
              is_struct(node, PgQuery.GrantStmt) or
              is_struct(node, PgQuery.LockStmt) or
              is_struct(node, PgQuery.CreateFunctionStmt) or
              is_struct(node, PgQuery.RenameStmt) or
              is_struct(node, PgQuery.RuleStmt) or
              is_struct(node, PgQuery.IndexStmt) or
              is_struct(node, PgQuery.ViewStmt),
       do: %Ctx{ctx | type: :ddl}

  defp ctx_for_node(%PgQuery.WithClause{recursive: recursive}, ctx),
    do: %Ctx{ctx | is_recursive_cte: recursive}

  defp ctx_for_node(%PgQuery.CommonTableExpr{ctename: ctename}, ctx),
    do: %Ctx{ctx | current_cte: ctename}

  defp ctx_for_node(_node, ctx), do: ctx

  # Updates context based on specific fields within nodes
  defp ctx_for_field(%PgQuery.SelectStmt{}, field, ctx)
       when field in [:target_list, :where_clause, :sort_clause, :group_clause, :having_clause],
       do: %Ctx{ctx | subselect_item: true}

  defp ctx_for_field(%PgQuery.SelectStmt{}, field, ctx)
       when field in [:into_clause],
       do: %Ctx{ctx | type: :ddl}

  defp ctx_for_field(%PgQuery.JoinExpr{}, field, ctx) when field in [:quals],
    do: %Ctx{ctx | join_clause_condition: true}

  defp ctx_for_field(_node, _field, ctx),
    do: ctx

  # Extracts message fields that contain nested nodes
  defp msg_to_field_nodes(msg) do
    apply(msg.__struct__, :fields_defs, [])
    |> Enum.filter(fn
      %Protox.Field{type: {:message, _}} -> true
      _ -> false
    end)
    |> Enum.map(fn field_def ->
      value =
        case field_def do
          # Handle oneof fields (special case for PgQuery.Node)
          %Protox.Field{kind: {:oneof, oneof_field}, name: name} ->
            case Map.get(msg, oneof_field) do
              {^name, value} -> value
              _ -> nil
            end

          # Handle regular message fields
          _ ->
            Map.get(msg, field_def.name)
        end

      {field_def.name, value}
    end)
  end

  defp collect_cte_names(%PgQuery.WithClause{ctes: ctes}) when is_list(ctes) do
    Enum.reduce(ctes, [], fn
      %PgQuery.Node{node: {:common_table_expr, %PgQuery.CommonTableExpr{ctename: ctename}}},
      cte_names ->
        [ctename | cte_names]

      _, cte_names ->
        cte_names
    end)
  end

  defp collect_cte_names(_with_clause), do: []

  defp rvar_to_alias_value(%PgQuery.RangeVar{
         schemaname: schemaname,
         relname: relname,
         alias: %PgQuery.Alias{
           aliasname: aliasname
         },
         location: location
       }),
       do: %{
         schema: if(schemaname == "", do: nil, else: schemaname),
         relation: relname,
         alias: aliasname,
         location: location
       }

  defp rvar_to_alias_value(_rvar), do: nil

  defp collect_aliases(nil), do: %{}

  defp collect_aliases(from_items) when is_list(from_items) do
    Enum.reduce(from_items, %{}, fn
      %PgQuery.Node{
        node: {:range_var, %PgQuery.RangeVar{alias: %PgQuery.Alias{aliasname: name}} = rvar}
      },
      table_aliases ->
        Map.put(table_aliases, name, rvar_to_alias_value(rvar))

      %PgQuery.Node{node: {:join_expr, join}}, table_aliases ->
        table_aliases
        |> collect_join_aliases(join)

      _other, table_aliases ->
        table_aliases
    end)
  end

  defp collect_join_aliases(table_aliases, %PgQuery.JoinExpr{} = join) do
    table_aliases
    |> collect_from_node(join.larg)
    |> collect_from_node(join.rarg)
  end

  defp collect_from_node(table_aliases, %PgQuery.Node{node: {:range_var, rvar}}) do
    case rvar.alias do
      %PgQuery.Alias{aliasname: name} -> Map.put(table_aliases, name, rvar_to_alias_value(rvar))
      nil -> table_aliases
    end
  end

  defp collect_from_node(table_aliases, %PgQuery.Node{node: {:join_expr, join}}) do
    collect_join_aliases(table_aliases, join)
  end

  defp collect_from_node(table_aliases, _), do: table_aliases
end
