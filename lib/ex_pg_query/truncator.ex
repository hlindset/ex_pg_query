defmodule ExPgQuery.Truncator do
  @moduledoc """
  Provides functionality to intelligently truncate SQL queries while maintaining valid syntax.
  Attempts to truncate less important parts of the query before resorting to simple truncation.
  """

  alias ExPgQuery.TreeModifier
  alias ExPgQuery.Parser

  defmodule PossibleTruncation do
    @moduledoc "Represents a location in the query that could be truncated"
    defstruct [:location, :node_type, :length, :is_array]
  end

  @doc """
  Truncates a SQL query to be below the specified length.
  Attempts smart truncation of specific query parts before falling back to hard truncation.
  """
  def truncate(%PgQuery.ParseResult{} = tree, max_length) do
    {:ok, encoded} = Protox.encode(tree)
    binary = IO.iodata_to_binary(encoded)

    case ExPgQuery.Native.deparse_protobuf(binary) do
      {:ok, output} when byte_size(output) <= max_length ->
        {:ok, output}

      {:ok, _output} ->
        # Need to truncate
        truncations = find_possible_truncations(tree)

        # Sort by deepest and longest first
        truncations = Enum.sort_by(truncations,
          fn t -> {-length(t.location), -t.length} end)

        case try_truncations(tree, truncations, max_length) do
          {:ok, output} -> {:ok, output}
          :error -> hard_truncate(tree, max_length)
        end

      {:error, _} = error ->
        error
    end
  end

  defp try_truncations(_tree, [], _max_length), do: :error
  defp try_truncations(_tree, [%{length: length} | _rest], _max_length) when length < 3, do: :error

  defp try_truncations(tree, [truncation | rest], max_length) do
    case truncate_at_location(tree, truncation) do
      {:ok, modified_tree} ->
        case ExPgQuery.Native.deparse_protobuf(modified_tree) do
          {:ok, output} ->
            output = cleanup_output(output)
            if byte_size(output) <= max_length do
              {:ok, output}
            else
              try_truncations(tree, rest, max_length)
            end
          {:error, _} ->
            try_truncations(tree, rest, max_length)
        end
      :error ->
        try_truncations(tree, rest, max_length)
    end
  end

  defp truncate_at_location(tree, truncation) do
    try do
      modified_tree = TreeModifier.update_at_location(tree, truncation.location, fn node ->
        dummy_column_ref = create_dummy_column_ref()

        case truncation.node_type do
          :target_list ->
            res_target_name = if match?(%PgQuery.UpdateStmt{}, node) or
                                match?(%PgQuery.OnConflictClause{}, node), do: "…", else: nil
            target = %PgQuery.ResTarget{name: res_target_name, val: dummy_column_ref}
            %{node | target_list: [%PgQuery.Node{node: {:res_target, target}}]}

          :where_clause ->
            %{node | where_clause: dummy_column_ref}

          :values_lists ->
            list = %PgQuery.List{items: [dummy_column_ref]}
            %{node | values_lists: [%PgQuery.Node{node: {:list, list}}]}

          :ctequery ->
            select = %PgQuery.SelectStmt{where_clause: dummy_column_ref, op: :SETOP_NONE}
            %{node | ctequery: %PgQuery.Node{node: {:select_stmt, select}}}

          :cols when is_struct(node, PgQuery.InsertStmt) ->
            res_target = %PgQuery.ResTarget{name: "…"}
            %{node | cols: [%PgQuery.Node{node: {:res_target, res_target}}]}

          _ ->
            raise ArgumentError, "Unexpected truncation node type: #{truncation.node_type}"
        end
      end)
      {:ok, modified_tree}
    rescue
      _ -> :error
    end
  end

  defp create_dummy_column_ref do
    string = %PgQuery.String{sval: "…"}
    column_ref = %PgQuery.ColumnRef{
      fields: [%PgQuery.Node{node: {:string, string}}]
    }
    %PgQuery.Node{node: {:column_ref, column_ref}}
  end

  defp cleanup_output(output) do
    output
    |> String.replace(~s(SELECT WHERE "…"), "...")
    |> String.replace(~s("…"), "...")
  end

  defp hard_truncate(tree, max_length) do
    case ExPgQuery.Native.deparse_protobuf(tree) do
      {:ok, output} ->
        truncated = String.slice(output, 0..max_length-4) <> "..."
        {:ok, truncated}
      {:error, _} = error ->
        error
    end
  end

  @doc """
  Finds all possible locations in the tree where truncation could occur.
  Returns a list of PossibleTruncation structs.
  """
  def find_possible_truncations(tree) do
    ExPgQuery.ProtoWalker.walk!(tree, fn parent_node, key, node, location ->
      case {key, parent_node} do
        {:target_list, %{__struct__: struct}} when struct in [PgQuery.SelectStmt, PgQuery.UpdateStmt, PgQuery.OnConflictClause] ->
          length = calculate_target_list_length(parent_node, node)
          [%PossibleTruncation{location: location, node_type: :target_list, length: length, is_array: true}]

        {:where_clause, %{__struct__: struct}} when struct in [PgQuery.SelectStmt, PgQuery.UpdateStmt,
                                                             PgQuery.DeleteStmt, PgQuery.CopyStmt,
                                                             PgQuery.IndexStmt, PgQuery.RuleStmt,
                                                             PgQuery.InferClause, PgQuery.OnConflictClause] ->
          case ExPgQuery.Native.deparse_protobuf(wrap_node(node)) do
            {:ok, output} ->
              [%PossibleTruncation{location: location, node_type: :where_clause, length: String.length(output), is_array: false}]
            {:error, _} ->
              []
          end

        {:values_lists, _} ->
          length = calculate_values_lists_length(node)
          [%PossibleTruncation{location: location, node_type: :values_lists, length: length, is_array: false}]

        {:ctequery, %PgQuery.CommonTableExpr{}} ->
          case ExPgQuery.Native.deparse_protobuf(wrap_node(node)) do
            {:ok, output} ->
              [%PossibleTruncation{location: location, node_type: :ctequery, length: String.length(output), is_array: false}]
            {:error, _} ->
              []
          end

        {:cols, %PgQuery.InsertStmt{}} ->
          length = calculate_cols_length(node)
          [%PossibleTruncation{location: location, node_type: :cols, length: length, is_array: true}]

        _ ->
          []
      end
    end)
    |> List.flatten()
  end

  # Helper functions for length calculations

  defp calculate_target_list_length(%PgQuery.SelectStmt{} = _parent, target_list) do
    select = %PgQuery.SelectStmt{target_list: target_list, op: :SETOP_NONE}
    case ExPgQuery.Native.deparse_protobuf(wrap_stmt(select)) do
      {:ok, output} -> String.length(output) - 7 # "SELECT ".length
      {:error, _} -> 0
    end
  end

  defp calculate_target_list_length(%PgQuery.UpdateStmt{} = _parent, target_list) do
    update = %PgQuery.UpdateStmt{
      target_list: target_list,
      relation: %PgQuery.RangeVar{relname: "x", inh: true}
    }
    case ExPgQuery.Native.deparse_protobuf(wrap_stmt(update)) do
      {:ok, output} -> String.length(output) - 13 # "UPDATE x SET ".length
      {:error, _} -> 0
    end
  end

  defp calculate_target_list_length(_, _), do: 0

  defp calculate_values_lists_length(values_lists) do
    select = %PgQuery.SelectStmt{values_lists: values_lists, op: :SETOP_NONE}
    case ExPgQuery.Native.deparse_protobuf(wrap_stmt(select)) do
      {:ok, output} -> String.length(output) - 7 # "SELECT ".length
      {:error, _} -> 0
    end
  end

  defp calculate_cols_length(cols) do
    insert = %PgQuery.InsertStmt{
      relation: %PgQuery.RangeVar{relname: "x", inh: true},
      cols: cols
    }
    case ExPgQuery.Native.deparse_protobuf(wrap_stmt(insert)) do
      {:ok, output} -> String.length(output) - 31 # "INSERT INTO x () DEFAULT VALUES".length
      {:error, _} -> 0
    end
  end

  # Helper functions to wrap nodes/statements for deparse

  defp wrap_node(node) do
    %PgQuery.ParseResult{
      version: 0,
      stmts: [%PgQuery.RawStmt{stmt: node}]
    }
  end

  defp wrap_stmt(stmt) do
    %PgQuery.ParseResult{
      version: 0,
      stmts: [%PgQuery.RawStmt{stmt: %PgQuery.Node{node: {stmt.__struct__ |> to_string() |> String.downcase() |> String.to_atom(), stmt}}}]
    }
  end
end
