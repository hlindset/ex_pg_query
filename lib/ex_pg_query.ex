defmodule ExPgQuery do
  @moduledoc """
  Provides functionality for parsing and analyzing PostgreSQL SQL queries.

  Parses SQL queries and extracts information about tables, functions, CTEs,
  aliases, and filter columns. Supports all SQL statement types including SELECT,
  DDL, and DML operations.

  ## Examples

      iex> query = "SELECT id, name FROM users WHERE age > 21 AND users.id in (10, 20, 30)"
      iex> {:ok, result} = ExPgQuery.parse(query)
      iex> ExPgQuery.tables(result)
      ["users"]
      iex> ExPgQuery.filter_columns(result)
      [{"users", "id"}, {nil, "age"}]

      # DDL operations
      iex> {:ok, result} = ExPgQuery.parse("CREATE TABLE posts (id integer, title text)")
      iex> ExPgQuery.ddl_tables(result)
      ["posts"]

      iex> {:ok, result} = ExPgQuery.parse("SELECT * INTO films_recent FROM films")
      iex> ExPgQuery.ddl_tables(result)
      ["films_recent"]
      iex> ExPgQuery.select_tables(result)
      ["films"]

      # Function analysis
      iex> {:ok, result} = ExPgQuery.parse("SELECT count(*) FROM users")
      iex> ExPgQuery.functions(result)
      ["count"]

  """

  alias ExPgQuery.NodeTraversal
  alias ExPgQuery.NodeTraversal.Ctx

  defmodule ParseResult do
    @moduledoc """
    Represents the result of parsing a SQL query.

    ## Fields

      * `tree` - Raw parse tree
      * `tables` - Referenced tables
      * `table_aliases` - Table aliases
      * `cte_names` - Common Table Expression names
      * `functions` - Referenced functions
      * `filter_columns` - Columns used to filter rows, e.g. (`WHERE`, `JOIN ... ON`)

    """

    defstruct tree: nil,
              tables: [],
              table_aliases: [],
              cte_names: [],
              functions: [],
              filter_columns: []
  end

  @doc """
  Parses a SQL query and returns detailed information about its structure.

  ## Parameters

    * `query` - SQL query string to parse

  ## Returns

    * `{:ok, ExPgQuery.ParseResult}` - Successfully parsed query with analysis
    * `{:error, reason}` - Error with reason

  ## Examples

      iex> query = "SELECT name FROM users u JOIN posts p ON u.id = p.user_id"
      iex> {:ok, result} = ExPgQuery.parse(query)
      iex> ExPgQuery.tables(result)
      ["posts", "users"]
      iex> ExPgQuery.table_aliases(result)
      [%{alias: "p", relation: "posts", location: 30, schema: nil}, %{alias: "u", relation: "users", location: 17, schema: nil}]

  """
  def parse(query) do
    with {:ok, tree} <- ExPgQuery.Protobuf.from_sql(query) do
      nodes = NodeTraversal.nodes(tree)

      result =
        Enum.reduce(nodes, %ParseResult{tree: tree}, fn node, acc ->
          case node do
            {node, %Ctx{} = ctx}
            when is_struct(node, PgQuery.SelectStmt) or is_struct(node, PgQuery.UpdateStmt) or
                   is_struct(node, PgQuery.MergeStmt) ->
              new_table_aliases =
                ctx.table_aliases
                # we don't want to collect aliases for CTEs
                |> Map.reject(fn {_k, %{relation: relation}} ->
                  Enum.member?(ctx.cte_names, relation)
                end)
                |> Map.values()

              %ParseResult{
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
                    %ParseResult{acc | tables: [table | acc.tables]}
                  end)

                rt when rt in [:OBJECT_RULE, :OBJECT_TRIGGER] ->
                  Enum.reduce(objects, acc, fn obj, acc ->
                    name = Enum.slice(obj, 0..-2//1) |> Enum.join(".")
                    table = %{name: name, type: type}
                    %ParseResult{acc | tables: [table | acc.tables]}
                  end)

                :OBJECT_FUNCTION ->
                  Enum.reduce(objects, acc, fn rel, acc ->
                    function = %{name: Enum.join(rel, "."), type: type}
                    %ParseResult{acc | functions: [function | acc.functions]}
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
                %ParseResult{acc | filter_columns: [field | acc.filter_columns]}
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

                  %ParseResult{acc | tables: [table | acc.tables]}
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

              %ParseResult{acc | functions: [%{name: function, type: type} | acc.functions]}

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

              %ParseResult{acc | functions: funcs ++ acc.functions}

            _ ->
              acc
          end
        end)

      {:ok,
       %ParseResult{
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

  @doc """
  Returns all table names referenced in the query.

  ## Parameters

    * `result` - `ExPgQuery.ParseResult` struct

  ## Returns

    * List of table names

  ## Examples

      iex> {:ok, result} = ExPgQuery.parse("SELECT * FROM users JOIN posts ON users.id = posts.user_id")
      iex> ExPgQuery.tables(result)
      ["posts", "users"]

  """
  def tables(%ParseResult{tables: tables}),
    do: Enum.map(tables, & &1.name) |> Enum.uniq()

  @doc """
  Returns table names from SELECT operations.

  ## Parameters

    * `result` - `ExPgQuery.ParseResult` struct

  ## Returns

    * List of table names used in SELECT statements

  ## Examples

      iex> {:ok, result} = ExPgQuery.parse("SELECT * FROM users; CREATE TABLE posts (id int)")
      iex> ExPgQuery.select_tables(result)
      ["users"]

  """
  def select_tables(%ParseResult{tables: tables}),
    do:
      tables
      |> Enum.filter(&(&1.type == :select))
      |> Enum.map(& &1.name)
      |> Enum.uniq()

  @doc """
  Returns table names from DDL operations.

  ## Parameters

    * `result` - `ExPgQuery.ParseResult` struct

  ## Returns

    * List of table names in DDL statements

  ## Examples

      iex> {:ok, result} = ExPgQuery.parse("CREATE TABLE users (id int); SELECT * FROM posts")
      iex> ExPgQuery.ddl_tables(result)
      ["users"]

  """
  def ddl_tables(%ParseResult{tables: tables}),
    do:
      tables
      |> Enum.filter(&(&1.type == :ddl))
      |> Enum.map(& &1.name)
      |> Enum.uniq()

  @doc """
  Returns table names from DML operations.

  ## Parameters

    * `result` - `ExPgQuery.ParseResult` struct

  ## Returns

    * List of table names in DML statements

  ## Examples

      iex> {:ok, result} = ExPgQuery.parse("INSERT INTO users (name) VALUES ('John')")
      iex> ExPgQuery.dml_tables(result)
      ["users"]

  """
  def dml_tables(%ParseResult{tables: tables}),
    do:
      tables
      |> Enum.filter(&(&1.type == :dml))
      |> Enum.map(& &1.name)
      |> Enum.uniq()

  @doc """
  Returns all function names referenced in the query.

  ## Parameters

    * `result` - `ExPgQuery.ParseResult` struct

  ## Returns

    * List of function names

  ## Examples

      iex> {:ok, result} = ExPgQuery.parse("SELECT count(*), max(age) FROM users")
      iex> ExPgQuery.functions(result)
      ["max", "count"]

  """
  def functions(%ParseResult{functions: functions}),
    do: Enum.map(functions, & &1.name) |> Enum.uniq()

  @doc """
  Returns function names that are called in the query.

  ## Parameters

    * `result` - `ExPgQuery.ParseResult` struct

  ## Returns

    * List of called function names

  ## Examples

      iex> {:ok, result} = ExPgQuery.parse("SELECT * FROM users WHERE age > my_func()")
      iex> ExPgQuery.call_functions(result)
      ["my_func"]

  """
  def call_functions(%ParseResult{functions: functions}),
    do:
      functions
      |> Enum.filter(&(&1.type == :call))
      |> Enum.map(& &1.name)
      |> Enum.uniq()

  @doc """
  Returns function names from DDL operations.

  ## Parameters

    * `result` - `ExPgQuery.ParseResult` struct

  ## Returns

    * List of function names in DDL statements

  ## Examples

      iex> {:ok, result} = ExPgQuery.parse("CREATE FUNCTION add(a int, b int) RETURNS int")
      iex> ExPgQuery.ddl_functions(result)
      ["add"]

  """
  def ddl_functions(%ParseResult{functions: functions}),
    do:
      functions
      |> Enum.filter(&(&1.type == :ddl))
      |> Enum.map(& &1.name)
      |> Enum.uniq()

  @doc """
  Returns column references used in filter conditions.

  ## Parameters

    * `result` - `ExPgQuery.ParseResult` struct

  ## Returns

    * List of `{table_name, column_name}` tuples
      * `table_name` can be nil if table isn't specified in query

  ## Examples

      iex> {:ok, result} = ExPgQuery.parse("SELECT * FROM users WHERE age > 21 AND users.active = true")
      iex> ExPgQuery.filter_columns(result)
      [{"users", "active"}, {nil, "age"}]

  """
  def filter_columns(%ParseResult{filter_columns: filter_columns}),
    do: filter_columns

  @doc """
  Returns table aliases defined in the query.

  ## Parameters

    * `result` - `ExPgQuery.ParseResult` struct

  ## Returns

    * List of alias maps containing:
      * `alias` - Alias name
      * `relation` - Original table name
      * `location` - Position in query
      * `schema` - Schema name (or nil)

  ## Examples

      iex> {:ok, result} = ExPgQuery.parse("SELECT u.name FROM users u JOIN posts p ON u.id = p.user_id")
      iex> ExPgQuery.table_aliases(result)
      [
        %{alias: "p", relation: "posts", location: 32, schema: nil},
        %{alias: "u", relation: "users", location: 19, schema: nil}
      ]

      iex> {:ok, result} = ExPgQuery.parse("SELECT * FROM public.users usr")
      iex> ExPgQuery.table_aliases(result)
      [%{alias: "usr", relation: "users", location: 14, schema: "public"}]

  """
  def table_aliases(%ParseResult{table_aliases: table_aliases}),
    do: table_aliases

  @doc """
  Returns types of statements in the query.

  ## Parameters

    * `result` - `ExPgQuery.ParseResult` struct

  ## Returns

    * List of statement type atoms

  ## Examples

      iex> {:ok, result} = ExPgQuery.parse("SELECT 1; INSERT INTO users (id) VALUES (1)")
      iex> ExPgQuery.statement_types(result)
      [:select_stmt, :insert_stmt]

  """
  def statement_types(%ParseResult{tree: %PgQuery.ParseResult{stmts: stmts}}) do
    Enum.map(stmts, fn %PgQuery.RawStmt{stmt: %PgQuery.Node{node: {stmt_type, _}}} ->
      stmt_type
    end)
  end
end
