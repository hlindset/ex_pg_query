defmodule ExPgQuery.NodeTraversal do
  @moduledoc """
  Recursively traverses a PostgreSQL AST to extract nodes and their context.

  This module provides functionality to walk through a parsed PostgreSQL query tree,
  collecting each node along with contextual information about where it appears in
  the query structure.
  """

  defmodule Ctx do
    @moduledoc """
    Represents the context in which a node appears in the query tree.

    Fields:
    - type: The type of statement (:none, :select, :dml, :ddl, :call)
    - current_cte: Name of the CTE currently being processed, if any
    - is_recursive_cte: Whether the current CTE is recursive
    """
    @type stmt_type :: :none | :select | :dml | :ddl | :call

    defstruct type: :none,
              current_cte: nil,
              is_recursive_cte: false,
              subselect_item: false,
              from_clause_item: false,
              condition_item: false,
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

  #
  # Updates context based on the type of node being processed
  #

  defp ctx_for_node(%PgQuery.SelectStmt{} = select_stmt, ctx) do
    table_aliases =
      case ctx do
        # in case of subqueries - keep previous aliases
        %Ctx{subselect_item: true} -> collect_select_aliases(ctx.table_aliases, select_stmt)
        _ -> collect_select_aliases(%{}, select_stmt)
      end

    cte_names = collect_cte_names(select_stmt.with_clause)
    %Ctx{ctx | type: :select, table_aliases: table_aliases, cte_names: ctx.cte_names ++ cte_names}
  end

  # SubLink (subqueries)
  defp ctx_for_node(%PgQuery.SubLink{}, ctx),
    do: %Ctx{ctx | subselect_item: true}

  defp ctx_for_node(%PgQuery.InsertStmt{} = insert_stmt, ctx) do
    cte_names = collect_cte_names(insert_stmt.with_clause)
    %Ctx{ctx | type: :dml, cte_names: ctx.cte_names ++ cte_names}
  end

  defp ctx_for_node(%PgQuery.UpdateStmt{} = update_stmt, ctx) do
    table_aliases = collect_update_aliases(update_stmt)
    cte_names = collect_cte_names(update_stmt.with_clause)
    %Ctx{ctx | type: :dml, table_aliases: table_aliases, cte_names: ctx.cte_names ++ cte_names}
  end

  defp ctx_for_node(%PgQuery.DeleteStmt{} = delete_stmt, ctx) do
    cte_names = collect_cte_names(delete_stmt.with_clause)
    %Ctx{ctx | type: :dml, cte_names: ctx.cte_names ++ cte_names}
  end

  defp ctx_for_node(%PgQuery.FuncCall{}, ctx) do
    %Ctx{ctx | type: :call}
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

  #
  # Updates context based on specific fields within nodes
  #

  defp ctx_for_field(%PgQuery.SelectStmt{}, :where_clause, ctx),
    do: %Ctx{ctx | condition_item: true}

  defp ctx_for_field(%PgQuery.SelectStmt{}, :into_clause, ctx),
    do: %Ctx{ctx | type: :ddl, from_clause_item: true}

  defp ctx_for_field(%PgQuery.SelectStmt{}, :from_clause, ctx),
    do: %Ctx{ctx | type: :select, from_clause_item: true}

  # DELETE
  defp ctx_for_field(%PgQuery.DeleteStmt{}, :where_clause, ctx),
    do: %Ctx{ctx | type: :select, condition_item: true}

  defp ctx_for_field(%PgQuery.DeleteStmt{}, :relation, ctx),
    do: %Ctx{ctx | type: :dml, from_clause_item: true}

  defp ctx_for_field(%PgQuery.DeleteStmt{}, :using_clause, ctx),
    do: %Ctx{ctx | type: :select, from_clause_item: true}

  # UPDATE
  defp ctx_for_field(%PgQuery.UpdateStmt{}, :relation, ctx),
    do: %Ctx{ctx | type: :dml, from_clause_item: true}

  defp ctx_for_field(%PgQuery.UpdateStmt{}, :where_clause, ctx),
    do: %Ctx{ctx | type: :select, condition_item: true}

  defp ctx_for_field(%PgQuery.UpdateStmt{}, :from_clause, ctx),
    do: %Ctx{ctx | type: :select, from_clause_item: true}

  defp ctx_for_field(%PgQuery.UpdateStmt{}, :relation, ctx),
    do: %Ctx{ctx | type: :dml, from_clause_item: true}

  # INSERT
  defp ctx_for_field(%PgQuery.InsertStmt{}, :from_clause, ctx),
    do: %Ctx{ctx | type: :select, from_clause_item: true}

  defp ctx_for_field(%PgQuery.InsertStmt{}, :relation, ctx),
    do: %Ctx{ctx | type: :dml, from_clause_item: true}

  # CREATE TABLE
  defp ctx_for_field(%PgQuery.CreateStmt{}, :relation, ctx),
    do: %Ctx{ctx | type: :ddl, from_clause_item: true}

  # CREATE TABLE ... AS
  defp ctx_for_field(%PgQuery.CreateTableAsStmt{}, :into, ctx),
    do: %Ctx{ctx | type: :ddl, from_clause_item: true}

  # ALTER TABLE
  defp ctx_for_field(%PgQuery.AlterTableStmt{}, :relation, ctx),
    do: %Ctx{ctx | type: :ddl, from_clause_item: true}

  # COPY
  defp ctx_for_field(%PgQuery.CopyStmt{}, :relation, ctx),
    do: %Ctx{ctx | type: :select, from_clause_item: true}

  # CREATE RULE
  defp ctx_for_field(%PgQuery.RuleStmt{}, :relation, ctx),
    do: %Ctx{ctx | type: :ddl, from_clause_item: true}

  # GRANT
  defp ctx_for_field(%PgQuery.GrantStmt{objtype: :OBJECT_TABLE}, :objects, ctx),
    do: %Ctx{ctx | type: :ddl, from_clause_item: true}

  # TRUNCATE
  defp ctx_for_field(%PgQuery.TruncateStmt{}, :relations, ctx),
    do: %Ctx{ctx | type: :ddl, from_clause_item: true}

  # VACUUM
  defp ctx_for_field(%PgQuery.VacuumStmt{}, :rels, ctx),
    do: %Ctx{ctx | type: :ddl, from_clause_item: true}

  # CREATE VIEW
  defp ctx_for_field(%PgQuery.ViewStmt{}, :view, ctx),
    do: %Ctx{ctx | type: :ddl, from_clause_item: true}

  # REFRESH MATERIALIZED VIEW
  defp ctx_for_field(%PgQuery.RefreshMatViewStmt{}, :relation, ctx),
    do: %Ctx{ctx | type: :ddl, from_clause_item: true}

  # CREATE TRIGGER
  defp ctx_for_field(%PgQuery.CreateTrigStmt{}, :relation, ctx),
    do: %Ctx{ctx | type: :ddl, from_clause_item: true}

  # CREATE INDEX
  defp ctx_for_field(%PgQuery.IndexStmt{}, :where_clause, ctx),
    do: %Ctx{ctx | type: :select, condition_item: true}

  defp ctx_for_field(%PgQuery.IndexStmt{}, :relation, ctx),
    do: %Ctx{ctx | type: :ddl, from_clause_item: true}

  # LOCK TABLE
  defp ctx_for_field(%PgQuery.LockStmt{}, :relations, ctx),
    do: %Ctx{ctx | type: :select, from_clause_item: true}

  # JOIN
  defp ctx_for_field(%PgQuery.JoinExpr{}, :quals, ctx),
    do: %Ctx{ctx | condition_item: true}

  # noop fallback
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

  defp collect_select_aliases(aliases, %PgQuery.SelectStmt{from_clause: from_clause}) do
    collect_from_clause_aliases(aliases, from_clause)
  end

  defp collect_update_aliases(%PgQuery.UpdateStmt{relation: relation, from_clause: from_clause}) do
    %{}
    |> collect_rvar_aliases(relation)
    |> collect_from_clause_aliases(from_clause)
  end

  defp collect_update_aliases(_update_stmt), do: %{}

  defp collect_from_clause_aliases(aliases, from_clause) when is_list(from_clause) do
    Enum.reduce(from_clause, aliases, fn
      %PgQuery.Node{node: {:range_var, rvar}}, aliases_acc ->
        collect_rvar_aliases(aliases_acc, rvar)

      %PgQuery.Node{node: {:join_expr, join}}, aliases_acc ->
        collect_join_aliases(aliases_acc, join)

      _other, aliases_acc ->
        aliases_acc
    end)
  end

  defp collect_from_clause_aliases(aliases, _from_clause), do: aliases

  defp collect_rvar_aliases(aliases, %PgQuery.RangeVar{
         schemaname: schemaname,
         relname: relname,
         alias: %PgQuery.Alias{
           aliasname: aliasname
         },
         location: location
       }) do
    Map.put(aliases, aliasname, %{
      schema: if(schemaname == "", do: nil, else: schemaname),
      relation: relname,
      alias: aliasname,
      location: location
    })
  end

  defp collect_rvar_aliases(aliases, _rvar), do: aliases

  defp collect_join_aliases(aliases, %PgQuery.JoinExpr{} = join) do
    aliases
    |> collect_from_node(join.larg)
    |> collect_from_node(join.rarg)
  end

  defp collect_from_node(aliases, %PgQuery.Node{node: {:range_var, rvar}}) do
    collect_rvar_aliases(aliases, rvar)
  end

  defp collect_from_node(aliases, %PgQuery.Node{node: {:join_expr, join}}) do
    collect_join_aliases(aliases, join)
  end

  defp collect_from_node(aliases, _), do: aliases
end
