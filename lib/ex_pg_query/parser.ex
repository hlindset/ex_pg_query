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
              table_aliases: [],
              cte_names: [],
              functions: [],
              filter_columns: []
  end

  def parse(query) do
    with {:ok, binary} <- ExPgQuery.Native.parse_protobuf(query),
         {:ok, protobuf} <- Protox.decode(binary, PgQuery.ParseResult) do
      # |> dbg(limit: :infinity, printable_limit: :infinity)
      nodes = NodeTraversal.nodes(protobuf)
      initial_result = %Result{protobuf: protobuf}

      result =
        Enum.reduce(nodes, initial_result, fn node, acc ->
          case node do
            {node, %Ctx{} = ctx}
            when is_struct(node, PgQuery.SelectStmt) or is_struct(node, PgQuery.UpdateStmt) ->
              new_table_aliases =
                ctx.table_aliases
                # we don't want to collect aliases for CTEs
                |> Map.reject(fn {_k, %{relation: relation}} ->
                  Enum.member?(ctx.cte_names, relation)
                end)
                |> Map.values()

              %Result{
                acc
                | table_aliases: acc.table_aliases ++ new_table_aliases,
                  cte_names: acc.cte_names ++ ctx.cte_names
              }

            {%PgQuery.DropStmt{remove_type: remove_type} = node, %Ctx{type: type}}
            when remove_type in [
                   :OBJECT_TABLE,
                   :OBJECT_VIEW,
                   :OBJECT_FUNCTION,
                   :OBJECT_RULE,
                   :OBJECT_TRIGGER
                 ] ->
              objects =
                Enum.map(node.objects, fn
                  %PgQuery.Node{node: {:list, list}} ->
                    Enum.map(list.items, fn
                      %PgQuery.Node{node: {:string, %PgQuery.String{sval: sval}}} -> sval
                      _ -> nil
                    end)

                  %PgQuery.Node{
                    node: {:object_with_args, %PgQuery.ObjectWithArgs{objname: objname}}
                  } ->
                    Enum.map(objname, fn
                      %PgQuery.Node{node: {:string, %PgQuery.String{sval: sval}}} -> sval
                      _ -> nil
                    end)

                  %PgQuery.Node{node: {:string, %PgQuery.String{sval: sval}}} ->
                    sval
                end)

              case remove_type do
                rt when rt in [:OBJECT_TABLE, :OBJECT_VIEW] ->
                  Enum.reduce(objects, acc, fn rel, acc ->
                    table = %{name: Enum.join(rel, "."), type: type}
                    %Result{acc | tables: [table | acc.tables]}
                  end)

                rt when rt in [:OBJECT_RULE, :OBJECT_TRIGGER] ->
                  Enum.reduce(objects, acc, fn obj, acc ->
                    name = Enum.slice(obj, 0..-2//1) |> Enum.join(".")
                    table = %{name: name, type: type}
                    %Result{acc | tables: [table | acc.tables]}
                  end)

                :OBJECT_FUNCTION ->
                  Enum.reduce(objects, acc, fn rel, acc ->
                    function = %{name: Enum.join(rel, "."), type: type}
                    %Result{acc | functions: [function | acc.functions]}
                  end)

                _ ->
                  acc
              end

            #
            # subselect items
            #

            {%PgQuery.ColumnRef{} = node, %Ctx{table_aliases: aliases, condition_item: true}} ->
              field =
                node.fields
                |> Enum.filter(fn %PgQuery.Node{node: {type, _}} -> type == :string end)
                |> Enum.map(fn %PgQuery.Node{node: {:string, %PgQuery.String{sval: sval}}} ->
                  sval
                end)

              field =
                case field do
                  [tbl, fld] ->
                    case Map.get(aliases, tbl) do
                      nil -> {tbl, fld}
                      alias -> {alias_to_name(alias), fld}
                    end

                  [fld] ->
                    {nil, fld}

                  _ ->
                    nil
                end

              if field do
                %Result{acc | filter_columns: [field | acc.filter_columns]}
              else
                acc
              end

            #
            # from clause items
            #

            {%PgQuery.RangeVar{} = node, %Ctx{type: type, from_clause_item: true} = ctx} ->
              table_name =
                case node do
                  %PgQuery.RangeVar{schemaname: "", relname: relname} ->
                    relname

                  %PgQuery.RangeVar{schemaname: schemaname, relname: relname} ->
                    "#{schemaname}.#{relname}"
                end

              is_cte_name =
                Enum.member?(ctx.cte_names, table_name) || ctx.current_cte == table_name

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
                    schemaname: if(node.schemaname == "", do: nil, else: node.schemaname),
                    relname: node.relname,
                    inh: node.inh,
                    relpersistence: node.relpersistence
                  }

                  %Result{acc | tables: [table | acc.tables]}
              end

            #
            # both from clause items and subselect items
            #

            {node, %Ctx{type: type}}
            when is_struct(node, PgQuery.FuncCall) or is_struct(node, PgQuery.CreateFunctionStmt) ->
              function =
                node.funcname
                |> Enum.map(fn %PgQuery.Node{node: {:string, %PgQuery.String{sval: sval}}} ->
                  sval
                end)
                |> Enum.join(".")

              %Result{acc | functions: [%{name: function, type: type} | acc.functions]}

            {%PgQuery.RenameStmt{
               rename_type: :OBJECT_FUNCTION,
               newname: newname,
               object: %PgQuery.Node{
                 node:
                   {:object_with_args,
                    %PgQuery.ObjectWithArgs{
                      objname: objname
                    }}
               }
             }, %Ctx{type: type}} ->
              original_name =
                objname
                |> Enum.map(fn %PgQuery.Node{node: {:string, %PgQuery.String{sval: sval}}} ->
                  sval
                end)
                |> Enum.join(".")

              funcs = [
                %{name: original_name, type: type},
                %{name: newname, type: type}
              ]

              %Result{acc | functions: funcs ++ acc.functions}

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
           table_aliases: Enum.uniq(result.table_aliases),
           filter_columns: Enum.uniq(result.filter_columns)
       }}
    end
  end

  defp alias_to_name(%{relation: relation, schema: nil}), do: relation
  defp alias_to_name(%{relation: relation, schema: schema}), do: "#{schema}.#{relation}"

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
