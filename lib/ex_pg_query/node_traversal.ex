defmodule ExPgQuery.NodeTraversal do
  @moduledoc """
  Recursively traverses a PostgreSQL AST to extract nodes and their context.

  This module provides functionality to walk through a parsed PostgreSQL query tree,
  collecting each node along with contextual information about where it appears in
  the query structure. It handles:

  - Statement type classification (SELECT, DML, DDL, CALL)
  - Table alias tracking
  - CTE (Common Table Expression) tracking
  - Subquery context management
  - Clause-specific context (FROM, WHERE, etc.)
  """

  @doc """
  Maps (root) PostgreSQL statement types to their general categories.

  Categories are:
  - :select - SELECT statements
  - :dml - Data Manipulation (INSERT, UPDATE, DELETE, etc.)
  - :ddl - Data Definition (CREATE, ALTER, DROP, etc.)
  - :call - Procedure calls and similar execution statements
  - :none - Utility and other statements
  """
  @default_node_type %{
    # SELECT statements
    PgQuery.SelectStmt => :select,

    # DML statements
    PgQuery.CopyStmt => :dml,
    PgQuery.DeleteStmt => :dml,
    PgQuery.InsertStmt => :dml,
    PgQuery.MergeStmt => :dml,
    PgQuery.UpdateStmt => :dml,

    # DDL statements
    PgQuery.AlterDatabaseRefreshCollStmt => :ddl,
    PgQuery.AlterDatabaseSetStmt => :ddl,
    PgQuery.AlterDatabaseStmt => :ddl,
    PgQuery.AlterEnumStmt => :ddl,
    PgQuery.AlterEventTrigStmt => :ddl,
    PgQuery.AlterExtensionContentsStmt => :ddl,
    PgQuery.AlterExtensionStmt => :ddl,
    PgQuery.AlterFdwStmt => :ddl,
    PgQuery.AlterForeignServerStmt => :ddl,
    PgQuery.AlterFunctionStmt => :ddl,
    PgQuery.AlterObjectDependsStmt => :ddl,
    PgQuery.AlterObjectSchemaStmt => :ddl,
    PgQuery.AlterOperatorStmt => :ddl,
    PgQuery.AlterOpFamilyStmt => :ddl,
    PgQuery.AlterOwnerStmt => :ddl,
    PgQuery.AlterPolicyStmt => :ddl,
    PgQuery.AlterPublicationStmt => :ddl,
    PgQuery.AlterRoleSetStmt => :ddl,
    PgQuery.AlterRoleStmt => :ddl,
    PgQuery.AlterSeqStmt => :ddl,
    PgQuery.AlterStatsStmt => :ddl,
    PgQuery.AlterSubscriptionStmt => :ddl,
    PgQuery.AlterSystemStmt => :ddl,
    PgQuery.AlterTableMoveAllStmt => :ddl,
    PgQuery.AlterTableSpaceOptionsStmt => :ddl,
    PgQuery.AlterTableStmt => :ddl,
    PgQuery.AlterTSConfigurationStmt => :ddl,
    PgQuery.AlterTSDictionaryStmt => :ddl,
    PgQuery.AlterTypeStmt => :ddl,
    PgQuery.AlterUserMappingStmt => :ddl,
    PgQuery.ClusterStmt => :ddl,
    PgQuery.CompositeTypeStmt => :ddl,
    PgQuery.CreateAmStmt => :ddl,
    PgQuery.CreateCastStmt => :ddl,
    PgQuery.CreateConversionStmt => :ddl,
    PgQuery.CreatedbStmt => :ddl,
    PgQuery.CreateDomainStmt => :ddl,
    PgQuery.CreateEnumStmt => :ddl,
    PgQuery.CreateEventTrigStmt => :ddl,
    PgQuery.CreateExtensionStmt => :ddl,
    PgQuery.CreateFdwStmt => :ddl,
    PgQuery.CreateForeignServerStmt => :ddl,
    PgQuery.CreateForeignTableStmt => :ddl,
    PgQuery.CreateFunctionStmt => :ddl,
    PgQuery.CreateOpClassStmt => :ddl,
    PgQuery.CreateOpFamilyStmt => :ddl,
    PgQuery.CreatePLangStmt => :ddl,
    PgQuery.CreatePolicyStmt => :ddl,
    PgQuery.CreatePublicationStmt => :ddl,
    PgQuery.CreateRangeStmt => :ddl,
    PgQuery.CreateRoleStmt => :ddl,
    PgQuery.CreateSchemaStmt => :ddl,
    PgQuery.CreateSeqStmt => :ddl,
    PgQuery.CreateStatsStmt => :ddl,
    PgQuery.CreateStmt => :ddl,
    PgQuery.CreateSubscriptionStmt => :ddl,
    PgQuery.CreateTableAsStmt => :ddl,
    PgQuery.CreateTableSpaceStmt => :ddl,
    PgQuery.CreateTransformStmt => :ddl,
    PgQuery.CreateTrigStmt => :ddl,
    PgQuery.CreateUserMappingStmt => :ddl,
    PgQuery.DefineStmt => :ddl,
    PgQuery.DropdbStmt => :ddl,
    PgQuery.DropRoleStmt => :ddl,
    PgQuery.DropStmt => :ddl,
    PgQuery.DropSubscriptionStmt => :ddl,
    PgQuery.DropTableSpaceStmt => :ddl,
    PgQuery.DropUserMappingStmt => :ddl,
    PgQuery.GrantStmt => :ddl,
    PgQuery.ImportForeignSchemaStmt => :ddl,
    PgQuery.IndexStmt => :ddl,
    PgQuery.RefreshMatViewStmt => :ddl,
    PgQuery.ReindexStmt => :ddl,
    PgQuery.RenameStmt => :ddl,
    PgQuery.RuleStmt => :ddl,
    PgQuery.TruncateStmt => :ddl,
    PgQuery.VacuumStmt => :ddl,
    PgQuery.ViewStmt => :ddl,

    # CALL statements
    PgQuery.CallStmt => :call,
    PgQuery.DoStmt => :call,
    PgQuery.ExecuteStmt => :call,

    # Utility statements
    PgQuery.AlterDefaultPrivilegesStmt => :none,
    PgQuery.CheckPointStmt => :none,
    PgQuery.ClosePortalStmt => :none,
    PgQuery.CommentStmt => :none,
    PgQuery.ConstraintsSetStmt => :none,
    PgQuery.DeallocateStmt => :none,
    PgQuery.DeclareCursorStmt => :none,
    PgQuery.DiscardStmt => :none,
    PgQuery.DropOwnedStmt => :none,
    PgQuery.ExplainStmt => :none,
    PgQuery.FetchStmt => :none,
    PgQuery.GrantRoleStmt => :none,
    PgQuery.ListenStmt => :none,
    PgQuery.LoadStmt => :none,
    PgQuery.LockStmt => :none,
    PgQuery.NotifyStmt => :none,
    PgQuery.PrepareStmt => :none,
    PgQuery.ReassignOwnedStmt => :none,
    PgQuery.SecLabelStmt => :none,
    PgQuery.TransactionStmt => :none,
    PgQuery.UnlistenStmt => :none,
    PgQuery.VariableSetStmt => :none,
    PgQuery.VariableShowStmt => :none
  }

  defmodule Ctx do
    @moduledoc """
    Represents the context in which a node appears in the query tree.

    ## Fields

    * `type` - The type of statement (:none, :select, :dml, :ddl, :call)
    * `current_cte` - Name of the CTE currently being processed
    * `is_recursive_cte` - Boolean indicating if current CTE is recursive
    * `subselect_item` - Boolean indicating if node is part of a subselect
    * `from_clause_item` - Boolean indicating if node is in a FROM clause
    * `condition_item` - Boolean indicating if node is in a condition (WHERE, JOIN ON, etc.)
    * `table_aliases` - Map of table aliases to their details in the current scope
    * `cte_names` - List of CTE names defined
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

  # Gets the default statement type for a node
  defp default_node_type(node) when is_struct(node) do
    Map.get(@default_node_type, node.__struct__, :none)
  end

  defp default_node_type(_node), do: :none

  @doc """
  Traverses a ParseResult tree and returns a list of nodes with their context.

  Processes each statement in the parse result, building up context information as it
  traverses the tree. This includes tracking table aliases, CTEs, and statement types.

  ## Parameters

    * `parse_result` - A PgQuery.ParseResult struct containing the parsed query

  ## Returns

    List of {node, context} tuples where:
    * `node` is the AST node
    * `context` is a Ctx struct containing contextual information

  ## Example

      iex> {:ok, result} = Parser.parse("SELECT * FROM users")
      iex> NodeTraversal.nodes(result.protobuf)
      [{%PgQuery.SelectStmt{...}, %Ctx{type: :select, ...}}, ...]
  """
  def nodes(%PgQuery.ParseResult{stmts: stmts}) do
    Enum.flat_map(stmts, fn
      %PgQuery.RawStmt{stmt: %PgQuery.Node{node: {_type, node}}} ->
        traverse_node(node, %Ctx{type: default_node_type(node)})
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

  # Context Update Functions

  # Updates context for SELECT statements, handling aliases and CTEs
  defp ctx_for_node(%PgQuery.SelectStmt{} = select_stmt, ctx) do
    table_aliases =
      case ctx do
        # when select statement is a subquery, keep previous aliases
        %Ctx{subselect_item: true} -> collect_select_aliases(ctx.table_aliases, select_stmt)
        # otherwise: empty them
        _ -> collect_select_aliases(%{}, select_stmt)
      end

    cte_names = collect_cte_names(select_stmt.with_clause)
    %Ctx{ctx | table_aliases: table_aliases, cte_names: ctx.cte_names ++ cte_names}
  end

  # SubLink (subqueries)
  defp ctx_for_node(%PgQuery.SubLink{}, ctx),
    do: %Ctx{ctx | subselect_item: true}

  # RangeSubselect (+ lateral)
  defp ctx_for_node(%PgQuery.RangeSubselect{lateral: true}, ctx),
    do: %Ctx{ctx | subselect_item: true}

  defp ctx_for_node(%PgQuery.InsertStmt{} = insert_stmt, ctx) do
    cte_names = collect_cte_names(insert_stmt.with_clause)
    %Ctx{ctx | cte_names: ctx.cte_names ++ cte_names}
  end

  defp ctx_for_node(%PgQuery.UpdateStmt{} = update_stmt, ctx) do
    table_aliases = collect_update_aliases(update_stmt)
    cte_names = collect_cte_names(update_stmt.with_clause)
    %Ctx{ctx | table_aliases: table_aliases, cte_names: ctx.cte_names ++ cte_names}
  end

  defp ctx_for_node(%PgQuery.DeleteStmt{} = delete_stmt, ctx) do
    cte_names = collect_cte_names(delete_stmt.with_clause)
    %Ctx{ctx | cte_names: ctx.cte_names ++ cte_names}
  end

  defp ctx_for_node(%PgQuery.FuncCall{}, ctx) do
    %Ctx{ctx | type: :call}
  end

  defp ctx_for_node(%PgQuery.WithClause{recursive: recursive}, ctx),
    do: %Ctx{ctx | is_recursive_cte: recursive}

  defp ctx_for_node(%PgQuery.CommonTableExpr{ctename: ctename}, ctx),
    do: %Ctx{ctx | current_cte: ctename}

  defp ctx_for_node(_node, ctx), do: ctx

  # ctx_for_field
  #
  # Sets the context for a single field (and its descendants) in a node.
  # E.g. The `into_clause` in a `SELECT INTO` should be a `:ddl`
  # statement as well as  be considered a `from_clause_item`. This is
  # useful, because a node type on its own is often not enough to figure
  # out the context it is used in.

  # SELECT
  defp ctx_for_field(%PgQuery.SelectStmt{}, field, ctx) do
    case field do
      :where_clause -> %Ctx{ctx | condition_item: true}
      :into_clause -> %Ctx{ctx | type: :ddl, from_clause_item: true}
      :from_clause -> %Ctx{ctx | type: :select, from_clause_item: true}
      _ -> ctx
    end
  end

  # DELETE
  defp ctx_for_field(%PgQuery.DeleteStmt{}, field, ctx) do
    case field do
      :where_clause -> %Ctx{ctx | type: :select, condition_item: true}
      :relation -> %Ctx{ctx | type: :dml, from_clause_item: true}
      :using_clause -> %Ctx{ctx | type: :select, from_clause_item: true}
      _ -> ctx
    end
  end

  # UPDATE
  defp ctx_for_field(%PgQuery.UpdateStmt{}, field, ctx) do
    case field do
      :relation -> %Ctx{ctx | type: :dml, from_clause_item: true}
      :where_clause -> %Ctx{ctx | type: :select, condition_item: true}
      :from_clause -> %Ctx{ctx | type: :select, from_clause_item: true}
      _ -> ctx
    end
  end

  # INSERT
  defp ctx_for_field(%PgQuery.InsertStmt{}, field, ctx) do
    case field do
      :from_clause -> %Ctx{ctx | type: :select, from_clause_item: true}
      :relation -> %Ctx{ctx | type: :dml, from_clause_item: true}
      _ -> ctx
    end
  end

  # CREATE INDEX
  defp ctx_for_field(%PgQuery.IndexStmt{}, field, ctx) do
    case field do
      :where_clause -> %Ctx{ctx | type: :select, condition_item: true}
      :relation -> %Ctx{ctx | type: :ddl, from_clause_item: true}
      _ -> ctx
    end
  end

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
    do: %Ctx{ctx | from_clause_item: true}

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

  # LOCK TABLE
  defp ctx_for_field(%PgQuery.LockStmt{}, :relations, ctx),
    do: %Ctx{ctx | type: :select, from_clause_item: true}

  # JOIN
  defp ctx_for_field(%PgQuery.JoinExpr{}, :quals, ctx),
    do: %Ctx{ctx | condition_item: true}

  # noop fallback
  defp ctx_for_field(_node, _field, ctx), do: ctx

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

  # Collects CTE names from a WITH clause
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

  # Collects aliases from a SELECT statement's FROM clause
  defp collect_select_aliases(aliases, %PgQuery.SelectStmt{from_clause: from_clause}) do
    collect_from_clause_aliases(aliases, from_clause)
  end

  # Collects aliases from an UPDATE statement, combining the target relation
  # and any additional FROM clause aliases
  defp collect_update_aliases(%PgQuery.UpdateStmt{relation: relation, from_clause: from_clause}) do
    %{}
    |> collect_rvar_aliases(relation)
    |> collect_from_clause_aliases(from_clause)
  end

  defp collect_update_aliases(_update_stmt), do: %{}

  # Processes a FROM clause to collect all table aliases, handling both
  # direct table references (range_var) and JOINs
  defp collect_from_clause_aliases(aliases, from_clause) when is_list(from_clause) do
    Enum.reduce(from_clause, aliases, fn
      # Handle direct table references
      %PgQuery.Node{node: {:range_var, rvar}}, aliases_acc ->
        collect_rvar_aliases(aliases_acc, rvar)

      # Handle JOIN expressions
      %PgQuery.Node{node: {:join_expr, join}}, aliases_acc ->
        collect_join_aliases(aliases_acc, join)

      # Skip other node types
      _other, aliases_acc ->
        aliases_acc
    end)
  end

  defp collect_from_clause_aliases(aliases, _from_clause), do: aliases

  # Extracts alias information from a RangeVar node, including schema,
  # relation name, alias name, and source location
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

  # Processes JOIN expressions, collecting aliases from both sides of the join
  defp collect_join_aliases(aliases, %PgQuery.JoinExpr{} = join) do
    aliases
    |> collect_from_node(join.larg)
    |> collect_from_node(join.rarg)
  end

  # Handle direct table references in joins
  defp collect_from_node(aliases, %PgQuery.Node{node: {:range_var, rvar}}) do
    collect_rvar_aliases(aliases, rvar)
  end

  # Handle nested joins
  defp collect_from_node(aliases, %PgQuery.Node{node: {:join_expr, join}}) do
    collect_join_aliases(aliases, join)
  end

  # Skip other node types
  defp collect_from_node(aliases, _), do: aliases
end
