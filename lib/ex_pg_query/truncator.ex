defmodule ExPgQuery.Truncator do
  @moduledoc """
  Provides functionality to intelligently truncate SQL queries while maintaining valid syntax.

  This module implements a smart truncation algorithm that:
  1. Identifies parts of the query that can be safely truncated
  2. Prioritizes truncating less important parts (WHERE clauses, value lists, etc.)
  3. Falls back to simple string truncation if smart truncation isn't sufficient

  Example:
      iex> long_query = "SELECT very, many, columns, that, make, query, too, long FROM a_table"
      iex> {:ok, protobuf} = ExPgQuery.parse_protobuf(long_query)
      iex> ExPgQuery.Truncator.truncate(protobuf, 30)
      {:ok, "SELECT ... FROM a_table"}
  """

  alias ExPgQuery.TreeWalker
  alias ExPgQuery.TreeUtils

  defmodule PossibleTruncation do
    @moduledoc """
    Represents a location in the query that could be truncated.

    Fields:
      * `:location` - Path to the truncatable element in the AST
      * `:node_type` - Type of the node (`:target_list`, `:where_clause`, etc.)
      * `:length` - Length of the text representation of this element
    """
    defstruct [:location, :node_type, :length]
  end

  # Predefined dummy nodes used for truncation
  @short_ellipsis "…"
  @final_ellipsis "..."
  @final_ellipsis_length String.length(@final_ellipsis)
  @dummy_column_ref %PgQuery.Node{
    node:
      {:column_ref,
       %PgQuery.ColumnRef{
         fields: [%PgQuery.Node{node: {:string, %PgQuery.String{sval: @short_ellipsis}}}]
       }}
  }
  @dummy_ctequery_node %PgQuery.SelectStmt{where_clause: @dummy_column_ref, op: :SETOP_NONE}
  @dummy_cols_list [%PgQuery.Node{node: {:res_target, %PgQuery.ResTarget{name: @short_ellipsis}}}]
  @dummy_values_list [%PgQuery.Node{node: {:list, %PgQuery.List{items: [@dummy_column_ref]}}}]
  @dummy_named_target_list [
    %PgQuery.Node{
      node: {:res_target, %PgQuery.ResTarget{name: @short_ellipsis, val: @dummy_column_ref}}
    }
  ]
  @dummy_unnamed_target_list [
    %PgQuery.Node{
      node: {:res_target, %PgQuery.ResTarget{name: "", val: @dummy_column_ref}}
    }
  ]

  # Fixes the output string by replacing placeholders with the final format.
  # E.g. `SELECT WHERE "…"` is replaced by `...`
  defp fix_output(output) do
    output
    |> String.replace(~s|SELECT WHERE "#{@short_ellipsis}"|, @final_ellipsis)
    |> String.replace(~s|"#{@short_ellipsis}"|, @final_ellipsis)
  end

  # Calculates the length of a query represented by a protobuf AST.
  #
  # Returns `{:ok, {length, output}}` where length is the string length and
  # output is the deparsed query string.
  defp query_length(protobuf) do
    with {:ok, encoded} <- Protox.encode(protobuf),
         binary = IO.iodata_to_binary(encoded),
         {:ok, output} <- ExPgQuery.Native.deparse_protobuf(binary) do
      fixed_output = fix_output(output)
      {:ok, {String.length(fixed_output), fixed_output}}
    end
  end

  @doc """
  Truncates a SQL query to be below the specified length.

  Attempts smart truncation of specific query parts before falling back to hard truncation.

  ## Parameters
    * `tree` - A `PgQuery.ParseResult` struct containing the parsed query
    * `max_length` - Maximum allowed length of the output string

  ## Returns
    * `{:ok, string}` - Successfully truncated query
    * `{:error, reason}` - Error during truncation

  ## Examples
      iex> query = "SELECT * FROM users WHERE name = 'very long name'"
      iex> {:ok, tree} = ExPgQuery.parse_protobuf(query)
      iex> ExPgQuery.Truncator.truncate(tree, 30)
      {:ok, "SELECT * FROM users WHERE ..."}
  """
  def truncate(%PgQuery.ParseResult{} = tree, traversed_nodes, max_length) do
    with {:ok, {length, output}} <- query_length(tree) do
      if length <= max_length do
        {:ok, output}
      else
        do_truncate(tree, traversed_nodes, max_length)
      end
    end
  end

  @doc """
  Same as `truncate/2` but raises on error.

  ## Parameters
    * `tree` - A `PgQuery.ParseResult` struct containing the parsed query
    * `max_length` - Maximum allowed length of the output string

  ## Returns
    * `string` - Successfully truncated query

  ## Raises
    * RuntimeError if truncation fails
  """
  def truncate!(tree, traversed_nodes, max_length) do
    case truncate(tree, traversed_nodes, max_length) do
      {:ok, output} -> output
      {:error, error} -> raise "Truncation error: #{inspect(error)}"
    end
  end

  # Performs the actual truncation by trying smart truncation first,
  # then falling back to hard truncation if needed.
  defp do_truncate(tree, traversed_nodes, max_length) do
    # Find possible truncation points
    truncations =
      find_possible_truncations(traversed_nodes)
      |> Enum.reject(&(&1.length < @final_ellipsis_length))
      |> Enum.sort_by(&{length(&1.location) * -1, &1.length * -1})

    # Try smart truncation first
    case try_smart_truncation(tree, truncations, max_length) do
      {:ok, {length, output}} ->
        if length <= max_length do
          {:ok, output}
        else
          # If smart truncation wasn't enough, do hard truncation
          slice_end = max_length - @final_ellipsis_length - 1
          {:ok, String.slice(output, 0..max(slice_end, 0)) <> @final_ellipsis}
        end

      {:error, _} = error ->
        error
    end
  end

  # Attempts to smartly truncate the query by trying each possible truncation point
  # until the query is short enough.
  defp try_smart_truncation(tree, truncations, max_length) do
    final_tree =
      Enum.reduce_while(truncations, {:ok, tree}, fn truncation, {:ok, tree_acc} ->
        with {:ok, updated_tree} <- update_tree(tree_acc, truncation),
             {:ok, {length, _output}} <- query_length(updated_tree) do
          if length > max_length do
            {:cont, {:ok, updated_tree}}
          else
            {:halt, {:ok, updated_tree}}
          end
        else
          {:error, _} = error -> {:halt, error}
        end
      end)

    with {:ok, final_tree} <- final_tree do
      query_length(final_tree)
    end
  end

  # Updates the AST by replacing a node with a truncated version.
  # The type of truncation depends on the node_type in the PossibleTruncation struct.
  defp update_tree(tree, truncation) do
    case truncation do
      %PossibleTruncation{node_type: :target_list_named, location: location} ->
        TreeUtils.put_in_tree(tree, location, @dummy_named_target_list)

      %PossibleTruncation{node_type: :target_list_unnamed, location: location} ->
        TreeUtils.put_in_tree(tree, location, @dummy_unnamed_target_list)

      %PossibleTruncation{node_type: :where_clause, location: location} ->
        TreeUtils.put_in_tree(tree, location, @dummy_column_ref)

      %PossibleTruncation{node_type: :values_lists, location: location} ->
        TreeUtils.put_in_tree(tree, location, @dummy_values_list)

      %PossibleTruncation{node_type: :ctequery, location: location} ->
        TreeUtils.put_in_tree(tree, location, @dummy_ctequery_node)

      %PossibleTruncation{node_type: :cols, location: location} ->
        TreeUtils.put_in_tree(tree, location, @dummy_cols_list)

      possible_truncation ->
        {:error, {:unhandled_truncation, possible_truncation}}
    end
  end

  # The following functions calculate the length of different query components
  # when rendered as strings. They're used to determine which parts to truncate first.

  # Calculates the length of a SELECT target list when rendered.
  defp select_target_list_length(node) do
    with {:ok, query} <-
           ExPgQuery.deparse_stmt(%PgQuery.SelectStmt{
             target_list: node,
             op: :SETOP_NONE
           }) do
      {:ok,
       query
       |> String.replace_leading("SELECT", "")
       |> String.trim_leading()
       |> String.length()}
    end
  end

  # Calculates the length of an UPDATE target list when rendered.
  defp update_target_list_length(node) do
    with {:ok, query} <-
           ExPgQuery.deparse_stmt(%PgQuery.UpdateStmt{
             target_list: node,
             relation: %PgQuery.RangeVar{relname: "x", inh: true}
           }) do
      {:ok,
       query
       |> String.replace_leading("UPDATE x SET", "")
       |> String.trim_leading()
       |> String.length()}
    end
  end

  # Calculates the length of a WHERE clause when rendered.
  defp where_clause_length(node) do
    case ExPgQuery.deparse_expr(node) do
      {:ok, expr} -> {:ok, String.length(expr)}
      {:error, _} = error -> error
    end
  end

  # Calculates the length of VALUES lists when rendered.
  defp select_values_lists_length(node) do
    with {:ok, query} <-
           ExPgQuery.deparse_stmt(%PgQuery.SelectStmt{
             values_lists: node,
             op: :SETOP_NONE
           }) do
      {:ok,
       query
       |> String.replace_leading("VALUES (", "")
       |> String.replace_trailing(")", "")
       |> String.length()}
    end
  end

  # Calculates the length of a CTE query when rendered.
  defp cte_query_length(node) do
    case ExPgQuery.deparse_stmt(node) do
      {:ok, expr} -> {:ok, String.length(expr)}
      {:error, _} = error -> error
    end
  end

  # Calculates the length of column definitions when rendered.
  defp cols_length(cols_list) do
    with {:ok, query} <-
           ExPgQuery.deparse_stmt(%PgQuery.InsertStmt{
             relation: %PgQuery.RangeVar{relname: "x", inh: true},
             cols: cols_list
           }) do
      {:ok,
       query
       |> String.replace_leading("INSERT INTO x (", "")
       |> String.replace_trailing(") DEFAULT VALUES", "")
       |> String.length()}
    end
  end

  # Walks the AST looking for nodes that could potentially be truncated.
  # For each candidate, calculates how long that part of the query is when rendered
  # as a string, to prioritize which parts to truncate first.
  defp find_possible_truncations(traversed_nodes) do
    Enum.reduce(traversed_nodes, [], fn {node, ctx}, acc ->
      acc
      |> find_target_list_truncations(node, ctx)
      |> find_where_clause_truncations(node, ctx)
      |> find_values_lists_truncations(node, ctx)
      |> find_cte_query_truncations(node, ctx)
      |> find_insert_cols_truncations(node, ctx)
    end)
  end

  defp find_target_list_truncations(acc, %{target_list: target_list} = node, ctx)
       when is_struct(node, PgQuery.SelectStmt) or
              is_struct(node, PgQuery.UpdateStmt) or
              is_struct(node, PgQuery.OnConflictClause) do
    length_result =
      case node do
        %PgQuery.SelectStmt{} -> select_target_list_length(target_list)
        _ -> update_target_list_length(target_list)
      end

    case length_result do
      {:ok, length} ->
        [
          %PossibleTruncation{
            location: ctx.location ++ [:target_list],
            node_type:
              case node do
                %PgQuery.UpdateStmt{} -> :target_list_named
                %PgQuery.OnConflictClause{} -> :target_list_named
                _ -> :target_list_unnamed
              end,
            length: length
          }
          | acc
        ]

      {:error, _} ->
        acc
    end
  end

  defp find_target_list_truncations(acc, _node, _ctx), do: acc

  defp find_where_clause_truncations(acc, %{where_clause: where_clause} = node, ctx)
       when not is_nil(where_clause) and
              (is_struct(node, PgQuery.SelectStmt) or
                 is_struct(node, PgQuery.UpdateStmt) or
                 is_struct(node, PgQuery.DeleteStmt) or
                 is_struct(node, PgQuery.CopyStmt) or
                 is_struct(node, PgQuery.IndexStmt) or
                 is_struct(node, PgQuery.RuleStmt) or
                 is_struct(node, PgQuery.InferClause) or
                 is_struct(node, PgQuery.OnConflictClause)) do
    case where_clause_length(where_clause) do
      {:ok, length} ->
        [
          %PossibleTruncation{
            location: ctx.location ++ [:where_clause],
            node_type: :where_clause,
            length: length
          }
          | acc
        ]

      {:error, _} ->
        acc
    end
  end

  defp find_where_clause_truncations(acc, _node, _ctx), do: acc

  defp find_values_lists_truncations(acc, %{values_lists: [_ | _] = values_lists} = node, ctx) do
    case select_values_lists_length(values_lists) do
      {:ok, length} ->
        [
          %PossibleTruncation{
            location: ctx.location ++ [:values_lists],
            node_type: :values_lists,
            length: length
          }
          | acc
        ]

      {:error, _} ->
        acc
    end
  end

  defp find_values_lists_truncations(acc, _node, _ctx), do: acc

  defp find_cte_query_truncations(
         acc,
         %{ctequery: %PgQuery.Node{node: {:select_stmt, cte_select}}} = node,
         ctx
       )
       when is_struct(node, PgQuery.CommonTableExpr) do
    case cte_query_length(cte_select) do
      {:ok, length} ->
        [
          %PossibleTruncation{
            location: ctx.location ++ [:ctequery, :select_stmt],
            node_type: :ctequery,
            length: length
          }
          | acc
        ]

      {:error, _} ->
        acc
    end
  end

  defp find_cte_query_truncations(acc, _node, _ctx), do: acc

  defp find_insert_cols_truncations(acc, %PgQuery.InsertStmt{cols: cols}, ctx) do
    case cols_length(cols) do
      {:ok, length} ->
        [
          %PossibleTruncation{
            location: ctx.location ++ [:cols],
            node_type: :cols,
            length: length
          }
          | acc
        ]

      {:error, _} ->
        acc
    end
  end

  defp find_insert_cols_truncations(acc, _node, _ctx), do: acc
end
