defmodule ExPgQuery.Parser do
  @moduledoc """
  Represents the result of parsing a SQL query, providing analysis of tables,
  functions, CTEs, and aliases found in the query.
  """

  alias ExPgQuery.NodeTraversal
  alias ExPgQuery.NodeTraversal.Ctx

  defmodule Result do
    defstruct protobuf: nil,
              tables: [],
              table_aliases: %{},
              cte_names: [],
              functions: [],
              filter_columns: []
  end

  def parse(query) do
    with {:ok, binary} <- ExPgQuery.Native.parse_protobuf(query),
         {:ok, protobuf} <- Protox.decode(binary, PgQuery.ParseResult) do
      # |> dbg()
      nodes = NodeTraversal.nodes(protobuf) |> dbg()
      initial_result = %Result{protobuf: protobuf}

      # find all CTEs first, so we can compare table names to CTE names
      # without having to worry about the order in which they appear
      # in the nodes list
      cte_results =
        Enum.reduce(nodes, initial_result, fn node_with_ctx, acc ->
          case node_with_ctx do
            {%PgQuery.CommonTableExpr{} = node, _} ->
              %Result{acc | cte_names: [node.ctename | acc.cte_names]}

            _ ->
              acc
          end
        end)

      Enum.each(nodes, fn
        {%PgQuery.ColumnRef{} = node, %Ctx{join_clause_condition: true}} -> IO.inspect(node, limit: :infinity, printable_limit: :infinity, label: :nnn)
        _ -> nil
      end)

      result =
        Enum.reduce(nodes, cte_results, fn node, acc ->
          case node do
            {%PgQuery.RangeVar{} = node, %Ctx{type: type} = ctx} ->
              table_name =
                case node do
                  %PgQuery.RangeVar{schemaname: "", relname: relname} ->
                    relname

                  %PgQuery.RangeVar{schemaname: schemaname, relname: relname} ->
                    "#{schemaname}.#{relname}"
                end

              is_cte_name =
                Enum.member?(acc.cte_names, table_name) || ctx.current_cte == table_name

              cond do
                # we're outside a cte, so it's a cte reference
                is_cte_name && ctx.current_cte == nil ->
                  acc

                # we're inside a recursive cte, so it's a cte's reference to itself
                is_cte_name && ctx.current_cte == table_name && ctx.is_recursive_cte ->
                  acc

                # otherwise, it's a cte's reference to a table with the same name
                # or just a table reference
                true ->
                  table = %{
                    name: table_name,
                    type: type,
                    location: node.location,
                    schemaname: node.schemaname,
                    relname: node.relname,
                    inh: node.inh,
                    relpersistence: node.relpersistence
                  }

                  result = %Result{acc | tables: [table | acc.tables]}

                  # if there's an alias, store it
                  case node.alias do
                    %PgQuery.Alias{aliasname: aliasname} ->
                      %Result{
                        result
                        | table_aliases:
                            Map.merge(result.table_aliases, %{
                              aliasname => %{name: table_name, type: type}
                            })
                      }

                    nil ->
                      result
                  end
              end

            {%PgQuery.DropStmt{remove_type: remove_type} = node, %Ctx{type: type} = ctx} ->
              objects =
                Enum.map(node.objects, fn
                  %PgQuery.Node{node: {:list, list}} ->
                    Enum.map(list.items, fn
                      %PgQuery.Node{node: {:string, %PgQuery.String{sval: sval}}} -> sval
                      _ -> nil
                    end)

                  %PgQuery.Node{node: {:string, %PgQuery.String{sval: sval}}} ->
                    sval
                end)

              # |> dbg()

              # acc

              case remove_type do
                rt when rt in [:OBJECT_TABLE, :OBJECT_VIEW] ->
                  Enum.reduce(objects, acc, fn rel, acc ->
                    table = %{name: Enum.join(rel, "."), type: type}
                    %Result{acc | tables: [table | acc.tables]}
                  end)

                _ ->
                  acc
              end

            #
            # subselect items
            #

            {%PgQuery.ColumnRef{} = node, %Ctx{subselect_item: true}} ->
              field =
                node.fields
                |> Enum.filter(fn %PgQuery.Node{node: {type, _}} -> type == :string end)
                |> Enum.map(fn %PgQuery.Node{node: {:string, %PgQuery.String{sval: sval}}} ->
                  sval
                end)

              field =
                case field do
                  [tbl, fld] -> {tbl, fld}
                  [fld] -> {nil, fld}
                  _ -> nil
                end

              if field do
                %Result{acc | filter_columns: [field | acc.filter_columns]}
              else
                acc
              end

            #
            # from clause items
            #

            #
            # both from clause items and subselect items
            #

            {%PgQuery.FuncCall{} = node, %Ctx{} = ctx} ->
              function =
                node.funcname
                |> Enum.map(fn %PgQuery.Node{node: {:string, %PgQuery.String{sval: sval}}} ->
                  sval
                end)
                |> Enum.join(".")

              %Result{acc | functions: [%{name: function, type: :call} | acc.functions]}

            _ ->
              acc
          end
        end)

      {:ok,
       %Result{
         result
         | tables: Enum.uniq(result.tables),
           cte_names: Enum.uniq(result.cte_names),
           functions: Enum.uniq(result.functions),
           table_aliases: result.table_aliases,
           filter_columns: Enum.uniq(result.filter_columns)
       }}
    end
  end

  def tables(%Result{tables: tables}),
    do: Enum.map(tables, & &1.name) |> Enum.uniq()

  def select_tables(%Result{tables: tables}),
    do:
      tables
      |> Enum.filter(&(&1.type == :select))
      |> Enum.map(& &1.name)
      |> Enum.uniq()

  def ddl_tables(%Result{tables: tables}),
    do:
      tables
      |> Enum.filter(&(&1.type == :ddl))
      |> Enum.map(& &1.name)
      |> Enum.uniq()

  def dml_tables(%Result{tables: tables}),
    do:
      tables
      |> Enum.filter(&(&1.type == :dml))
      |> Enum.map(& &1.name)
      |> Enum.uniq()

  def functions(%Result{functions: functions}),
    do: Enum.map(functions, & &1.name) |> Enum.uniq()

  def call_functions(%Result{functions: functions}),
    do:
      functions
      |> Enum.filter(&(&1.type == :call))
      |> Enum.map(& &1.name)
      |> Enum.uniq()

  def ddl_functions(%Result{functions: functions}),
    do:
      functions
      |> Enum.filter(&(&1.type == :ddl))
      |> Enum.map(& &1.name)
      |> Enum.uniq()

  def statement_types(%Result{protobuf: %PgQuery.ParseResult{stmts: stmts}}) do
    Enum.map(stmts, fn %PgQuery.RawStmt{stmt: %PgQuery.Node{node: {stmt_type, _}}} ->
      stmt_type
    end)
  end
end
