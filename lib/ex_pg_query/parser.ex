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
              aliases: [],
              cte_names: [],
              functions: [],
              filter_columns: []
  end

  def parse(query) do
    with {:ok, binary} <- ExPgQuery.Native.parse_protobuf(query),
         {:ok, protobuf} <- Protox.decode(binary, PgQuery.ParseResult) do
      nodes = NodeTraversal.nodes(protobuf)
      initial_result = %Result{protobuf: protobuf}

      result =
        Enum.reduce(nodes, initial_result, fn node, acc ->
          case node do
            {%PgQuery.RangeVar{} = node, %Ctx{} = ctx} ->
              table =
                case node do
                  %PgQuery.RangeVar{schemaname: "", relname: relname} ->
                    relname

                  %PgQuery.RangeVar{schemaname: schemaname, relname: relname} ->
                    "#{schemaname}.#{relname}"
                end

              {current_cte, is_recursive} =
                case ctx.current_cte do
                  nil -> {nil, false}
                  {cte, is_recursive} -> {cte, is_recursive}
                end

              # if `current_cte != table` then it's an actual table, unless it's a recursive CTE
              if Enum.member?(acc.cte_names, table) && (current_cte != table || is_recursive) do
                acc
              else
                %Result{acc | tables: [table | acc.tables]}
              end

            {%PgQuery.FuncCall{} = node, %Ctx{}} ->
              function =
                node.funcname
                |> Enum.map(fn %PgQuery.Node{node: {:string, %PgQuery.String{sval: sval}}} ->
                  sval
                end)
                |> Enum.join(".")

              %Result{acc | functions: [function | acc.functions]}

            {%PgQuery.CommonTableExpr{} = node, _} ->
              %Result{acc | cte_names: [node.ctename | acc.cte_names]}

            {%PgQuery.Alias{} = node, _} ->
              %Result{acc | aliases: [node.aliasname | acc.aliases]}

            {%PgQuery.ColumnRef{} = node, %Ctx{has_filter: true}} ->
              field =
                node.fields
                |> Enum.map(fn %PgQuery.Node{node: {:string, %PgQuery.String{sval: sval}}} ->
                  sval
                end)

              field =
                case field do
                  [f1, f2] -> {f2, f1}
                  [f1] -> {nil, f1}
                  _ -> nil
                end

              if field do
                %Result{acc | filter_columns: [field | acc.filter_columns]}
              else
                acc
              end

            _ ->
              acc
          end
        end)

      {:ok, result}
    else
      {:error, reason} -> {:error, reason}
    end
  end

  def statement_types(%Result{protobuf: %PgQuery.ParseResult{stmts: stmts}}) do
    Enum.map(stmts, fn %PgQuery.RawStmt{stmt: %PgQuery.Node{node: {stmt_type, _}}} ->
      stmt_type
    end)
  end
end
