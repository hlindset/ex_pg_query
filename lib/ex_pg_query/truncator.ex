defmodule ExPgQuery.Truncator do
  @moduledoc """
  Provides functionality to intelligently truncate SQL queries while maintaining valid syntax.
  Attempts to truncate less important parts of the query before resorting to simple truncation.
  """

  alias ExPgQuery.ProtoWalker
  alias ExPgQuery.ProtoUtils
  alias ExPgQuery.Parser

  defmodule PossibleTruncation do
    @moduledoc "Represents a location in the query that could be truncated"
    defstruct [:location, :node_type, :length, :is_list]
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
    else
      {:error, _reason} = err -> err
    end
  end

  @doc """
  Truncates a SQL query to be below the specified length.
  Attempts smart truncation of specific query parts before falling back to hard truncation.
  """
  def truncate(%PgQuery.ParseResult{} = tree, max_length) do
    {:ok, encoded} = Protox.encode(tree)
    binary = IO.iodata_to_binary(encoded)

    case ExPgQuery.Native.deparse_protobuf(binary) do
      # todo: use String.length to count graphemes
      {:ok, output} when byte_size(output) <= max_length ->
        {:ok, output}

      {:ok, _output} ->
        # Need to truncate
        truncations =
          find_possible_truncations(tree)
          |> Enum.reject(&(&1.length <= @ellipsis_length))
          # Sort by deepest and longest first
          |> Enum.sort_by(&{length(&1.location) * -1, &1.length * -1})

        truncated_tree =
          Enum.reduce_while(truncations, tree, fn truncation, tree_acc ->
            updated_tree =
              case truncation do
                %PossibleTruncation{node_type: :target_list} ->
                  ProtoUtils.update_in_tree!(
                    tree_acc,
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
                    tree_acc,
                    remove_last_element(truncation.location),
                    &%{&1 | where_clause: @dummy_column_ref}
                  )

                %PossibleTruncation{node_type: :values_lists} ->
                  ProtoUtils.update_in_tree!(
                    tree_acc,
                    remove_last_element(truncation.location),
                    &%{&1 | values_lists: @dummy_values_list}
                  )

                %PossibleTruncation{node_type: :ctequery} ->
                  ProtoUtils.update_in_tree!(
                    tree_acc,
                    remove_last_element(truncation.location),
                    &%{&1 | ctequery: @dummy_ctequery_node}
                  )

                %PossibleTruncation{node_type: :cols} ->
                  ProtoUtils.update_in_tree!(
                    tree_acc,
                    remove_last_element(truncation.location),
                    &%{&1 | cols: @dummy_cols_list}
                  )

                other ->
                  tree_acc
              end

            case query_length(updated_tree) do
              {:ok, {length, _}} when length > max_length ->
                {:cont, updated_tree}

              {:ok, _} ->
                {:halt, updated_tree}
                # {:error, _} -> {:halt, updated_tree}
            end
          end)

        case query_length(truncated_tree) do
          {:ok, {length, output}} when length > max_length ->
            # we didn't get there, do a hard cut off
            {:ok, String.slice(output, 0..(max_length - @ellipsis_length - 1)) <> @ellipsis}

          {:ok, {_length, output}} ->
            {:ok, output}
            # {:error, _} -> {:error, "Truncation error"}
        end

      {:error, _} = error ->
        error
    end
  end

  def truncate!(tree, max_length) do
    case truncate(tree, max_length) do
      {:ok, output} -> output
      {:error, error} -> raise "Truncation error: #{inspect(error)}"
    end
  end

  defp remove_last_element(list) do
    list |> Enum.reverse() |> tl() |> Enum.reverse()
  end

  defp select_target_list_length(node) do
    %PgQuery.SelectStmt{target_list: node, op: :SETOP_NONE}
    |> ExPgQuery.deparse_stmt!()
    |> String.replace_leading("SELECT", "")
    |> String.trim_leading()
    |> String.length()
  end

  defp update_target_list_length(node) do
    %PgQuery.UpdateStmt{target_list: node, relation: %PgQuery.RangeVar{relname: "x", inh: true}}
    |> ExPgQuery.deparse_stmt!()
    |> String.replace_leading("UPDATE x SET", "")
    |> String.trim_leading()
    |> String.length()
  end

  defp where_clause_length(node) do
    ExPgQuery.deparse_expr!(node)
    |> String.length()
  end

  defp select_values_lists_length(node) do
    %PgQuery.SelectStmt{values_lists: node, op: :SETOP_NONE}
    |> ExPgQuery.deparse_stmt!()
    |> String.replace_leading("VALUES (", "")
    |> String.replace_trailing(")", "")
    |> String.length()
  end

  defp cte_query_length(node) do
    ExPgQuery.deparse_expr!(node)
    |> String.length()
  end

  defp cols_length(node) do
    %PgQuery.InsertStmt{relation: %PgQuery.RangeVar{relname: "x", inh: true}, cols: node}
    |> ExPgQuery.deparse_stmt!()
    |> String.replace_leading("INSERT INTO x (", "")
    |> String.replace_trailing(") DEFAULT VALUES", "")
    |> String.length()
  end

  def find_possible_truncations(tree) do
    ProtoWalker.walk(tree, [], fn parent_node, field_name, {node, location}, acc ->
      case field_name do
        :target_list
        when is_struct(parent_node, PgQuery.SelectStmt) or
               is_struct(parent_node, PgQuery.UpdateStmt) or
               is_struct(parent_node, PgQuery.OnConflictClause) ->
          target_list_length =
            case parent_node do
              %PgQuery.SelectStmt{} -> select_target_list_length(node)
              _ -> update_target_list_length(node)
            end

          [
            %PossibleTruncation{
              location: location,
              node_type: :target_list,
              length: target_list_length,
              is_list: true
            }
            | acc
          ]

        :where_clause
        when is_struct(parent_node, PgQuery.SelectStmt) or
               is_struct(parent_node, PgQuery.UpdateStmt) or
               is_struct(parent_node, PgQuery.DeleteStmt) or
               is_struct(parent_node, PgQuery.CopyStmt) or
               is_struct(parent_node, PgQuery.IndexStmt) or
               is_struct(parent_node, PgQuery.RuleStmt) or
               is_struct(parent_node, PgQuery.InferClause) or
               is_struct(parent_node, PgQuery.OnConflictClause) ->
          [
            %PossibleTruncation{
              location: location,
              node_type: :where_clause,
              length: where_clause_length(node),
              is_list: false
            }
            | acc
          ]

        :values_lists ->
          [
            %PossibleTruncation{
              location: location,
              node_type: :values_lists,
              length: select_values_lists_length(node),
              is_list: true
            }
            | acc
          ]

        :ctequery when is_struct(parent_node, PgQuery.CommonTableExpr) ->
          [
            %PossibleTruncation{
              location: location,
              node_type: :ctequery,
              length: cte_query_length(node),
              is_list: false
            }
            | acc
          ]

        :cols when is_struct(parent_node, PgQuery.InsertStmt) ->
          [
            %PossibleTruncation{
              location: location,
              node_type: :cols,
              length: cols_length(node),
              is_list: true
            }
            | acc
          ]

        _field_name ->
          acc
      end
    end)
  end
end
