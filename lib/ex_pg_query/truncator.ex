defmodule ExPgQuery.Truncator do
  @moduledoc """
  Provides functionality to intelligently truncate SQL queries while maintaining valid syntax.
  Attempts to truncate less important parts of the query before resorting to simple truncation.
  """

  alias ExPgQuery.ProtoWalker
  alias ExPgQuery.ProtoUtils

  defmodule PossibleTruncation do
    @moduledoc "Represents a location in the query that could be truncated"
    defstruct [:location, :node_type, :length]
  end

  @ellipsis "..."
  @ellipsis_length String.length(@ellipsis)
  @dummy_column_ref %PgQuery.Node{
    node:
      {:column_ref,
       %PgQuery.ColumnRef{
         fields: [%PgQuery.Node{node: {:string, %PgQuery.String{sval: @ellipsis}}}]
       }}
  }
  @dummy_ctequery_node %PgQuery.Node{
    node:
      {:select_stmt,
       %PgQuery.SelectStmt{
         where_clause: @dummy_column_ref,
         op: :SETOP_NONE
       }}
  }
  @dummy_cols_list [
    %PgQuery.Node{
      node: {:res_target, %PgQuery.ResTarget{name: @ellipsis}}
    }
  ]
  @dummy_values_list [
    %PgQuery.Node{
      node: {:list, %PgQuery.List{items: [@dummy_column_ref]}}
    }
  ]

  defp query_length(protobuf) do
    with {:ok, encoded} <- Protox.encode(protobuf),
         binary = IO.iodata_to_binary(encoded),
         {:ok, output} <- ExPgQuery.Native.deparse_protobuf(binary) do
      {:ok, {String.length(output), output}}
    end
  end

  @doc """
  Truncates a SQL query to be below the specified length.
  Attempts smart truncation of specific query parts before falling back to hard truncation.
  """
  def truncate(%PgQuery.ParseResult{} = tree, max_length) do
    with {:ok, {length, output}} <- query_length(tree) do
      if length <= max_length do
        {:ok, output}
      else
        do_truncate(tree, max_length)
      end
    end
  end

  def truncate!(tree, max_length) do
    case truncate(tree, max_length) do
      {:ok, output} -> output
      {:error, error} -> raise "Truncation error: #{inspect(error)}"
    end
  end

  defp do_truncate(tree, max_length) do
    # Find possible truncation points
    truncations =
      find_possible_truncations(tree)
      |> Enum.reject(&(&1.length <= @ellipsis_length))
      |> Enum.sort_by(&{length(&1.location) * -1, &1.length * -1})

    # Try smart truncation first
    case try_smart_truncation(tree, truncations, max_length) |> dbg() do
      {:ok, {length, output}} ->
        if length <= max_length do
          {:ok, output}
        else
          # If smart truncation wasn't enough, do hard truncation
          slice_end = max_length - @ellipsis_length - 1
          {:ok, String.slice(output, 0..max(slice_end, 0)) <> @ellipsis}
        end

      {:error, _} = error ->
        error
    end
  end

  defp try_smart_truncation(tree, truncations, max_length) do
    final_tree =
      Enum.reduce_while(truncations, {:ok, tree}, fn truncation, {:ok, tree_acc} ->
        with {:ok, updated_tree} <- update_tree(tree_acc, truncation),
             {:ok, {length, output}} <- query_length(updated_tree) do
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

  defp update_tree(tree, truncation) do
    try do
      updated_tree =
        case truncation do
          %PossibleTruncation{node_type: :target_list} ->
            ProtoUtils.update_in_tree!(
              tree,
              remove_last_element(truncation.location),
              fn parent_node ->
                res_target_name =
                  case parent_node do
                    %PgQuery.SelectStmt{} -> nil
                    _ -> @ellipsis
                  end

                %{
                  parent_node
                  | target_list: [
                      %PgQuery.Node{
                        node:
                          {:res_target,
                           %PgQuery.ResTarget{name: res_target_name, val: @dummy_column_ref}}
                      }
                    ]
                }
              end
            )

          %PossibleTruncation{node_type: :where_clause} ->
            ProtoUtils.update_in_tree!(
              tree,
              remove_last_element(truncation.location),
              &%{&1 | where_clause: @dummy_column_ref}
            )

          %PossibleTruncation{node_type: :values_lists} ->
            ProtoUtils.update_in_tree!(
              tree,
              remove_last_element(truncation.location),
              &%{&1 | values_lists: @dummy_values_list}
            )

          %PossibleTruncation{node_type: :ctequery} ->
            ProtoUtils.update_in_tree!(
              tree,
              remove_last_element(truncation.location),
              &%{&1 | ctequery: @dummy_ctequery_node}
            )

          %PossibleTruncation{node_type: :cols} ->
            ProtoUtils.update_in_tree!(
              tree,
              remove_last_element(truncation.location),
              &%{&1 | cols: @dummy_cols_list}
            )

          _other ->
            tree
        end

      {:ok, updated_tree}
    rescue
      e in RuntimeError -> {:error, e.message}
      _ -> {:error, "Unknown error during tree update"}
    end
  end

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

  defp where_clause_length(node) do
    case ExPgQuery.deparse_expr(node) do
      {:ok, expr} -> {:ok, String.length(expr)}
      {:error, _} = error -> error
    end
  end

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

  defp cte_query_length(node) do
    case ExPgQuery.deparse_expr(node) do
      {:ok, expr} -> {:ok, String.length(expr)}
      {:error, _} = error -> error
    end
  end

  defp cols_length(node) do
    with {:ok, query} <-
           ExPgQuery.deparse_stmt(%PgQuery.InsertStmt{
             relation: %PgQuery.RangeVar{relname: "x", inh: true},
             cols: node
           }) do
      {:ok,
       query
       |> String.replace_leading("INSERT INTO x (", "")
       |> String.replace_trailing(") DEFAULT VALUES", "")
       |> String.length()}
    end
  end

  defp remove_last_element(list) do
    list |> Enum.reverse() |> tl() |> Enum.reverse()
  end

  def find_possible_truncations(tree) do
    ProtoWalker.walk(tree, [], fn parent_node, field_name, {node, location}, acc ->
      case field_name do
        :target_list
        when is_struct(parent_node, PgQuery.SelectStmt) or
               is_struct(parent_node, PgQuery.UpdateStmt) or
               is_struct(parent_node, PgQuery.OnConflictClause) ->
          length_result =
            case parent_node do
              %PgQuery.SelectStmt{} -> select_target_list_length(node)
              _ -> update_target_list_length(node)
            end

          case length_result do
            {:ok, length} ->
              [
                %PossibleTruncation{
                  location: location,
                  node_type: :target_list,
                  length: length
                }
                | acc
              ]

            {:error, _} ->
              acc
          end

        :where_clause
        when is_struct(parent_node, PgQuery.SelectStmt) or
               is_struct(parent_node, PgQuery.UpdateStmt) or
               is_struct(parent_node, PgQuery.DeleteStmt) or
               is_struct(parent_node, PgQuery.CopyStmt) or
               is_struct(parent_node, PgQuery.IndexStmt) or
               is_struct(parent_node, PgQuery.RuleStmt) or
               is_struct(parent_node, PgQuery.InferClause) or
               is_struct(parent_node, PgQuery.OnConflictClause) ->
          case where_clause_length(node) do
            {:ok, length} ->
              [
                %PossibleTruncation{
                  location: location,
                  node_type: :where_clause,
                  length: length
                }
                | acc
              ]

            {:error, _} ->
              acc
          end

        :values_lists ->
          case select_values_lists_length(node) do
            {:ok, length} ->
              [
                %PossibleTruncation{
                  location: location,
                  node_type: :values_lists,
                  length: length
                }
                | acc
              ]

            {:error, _} ->
              acc
          end

        :ctequery when is_struct(parent_node, PgQuery.CommonTableExpr) ->
          case cte_query_length(node) do
            {:ok, length} ->
              [
                %PossibleTruncation{
                  location: location,
                  node_type: :ctequery,
                  length: length
                }
                | acc
              ]

            {:error, _} ->
              acc
          end

        :cols when is_struct(parent_node, PgQuery.InsertStmt) ->
          case cols_length(node) do
            {:ok, length} ->
              [
                %PossibleTruncation{
                  location: location,
                  node_type: :cols,
                  length: length
                }
                | acc
              ]

            {:error, _} ->
              acc
          end

        _field_name ->
          acc
      end
    end)
  end
end
