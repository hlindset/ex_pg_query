defmodule ExPgQueryTest do
  use ExUnit.Case

  doctest ExPgQuery

  describe "parse_protobuf/1" do
    test "successfully parses valid SQL" do
      query = "SELECT * FROM users WHERE id = 1"
      assert {:ok, result} = ExPgQuery.parse_protobuf(query)
      assert %PgQuery.ParseResult{} = result
    end

    test "returns error for invalid SQL" do
      query = "SELECT * FREM users"
      assert {:error, _} = ExPgQuery.parse_protobuf(query)
    end
  end

  describe "parse_protobuf!/1" do
    test "returns ParseResult for valid SQL" do
      query = "SELECT * FROM users WHERE id = 1"
      assert %PgQuery.ParseResult{} = ExPgQuery.parse_protobuf!(query)
    end

    test "raises error for invalid SQL" do
      query = "SELECT * FREM users"

      assert_raise RuntimeError, ~r/Parse error:/, fn ->
        ExPgQuery.parse_protobuf!(query)
      end
    end
  end

  describe "deparse/1" do
    test "deparses ParseResult back to SQL" do
      original_query = "SELECT * FROM users WHERE id = 1"
      {:ok, parse_result} = ExPgQuery.parse_protobuf(original_query)
      assert {:ok, deparsed} = ExPgQuery.deparse(parse_result)
      assert deparsed =~ "SELECT"
      assert deparsed =~ "FROM users"
      assert deparsed =~ "WHERE id = 1"
    end
  end

  describe "deparse!/1" do
    test "deparses ParseResult back to SQL" do
      original_query = "SELECT * FROM users WHERE id = 1"
      parse_result = ExPgQuery.parse_protobuf!(original_query)
      assert deparsed = ExPgQuery.deparse!(parse_result)
      assert deparsed =~ "SELECT"
      assert deparsed =~ "FROM users"
      assert deparsed =~ "WHERE id = 1"
    end

    test "raises error for invalid protobuf" do
      assert_raise RuntimeError, ~r/Deparse error:/, fn ->
        ExPgQuery.deparse!(%PgQuery.ParseResult{stmts: [%PgQuery.ColumnRef{}]})
      end
    end
  end

  describe "deparse_stmt/1" do
    test "deparses a single statement" do
      stmt = %PgQuery.SelectStmt{
        target_list: [
          %PgQuery.Node{
            node:
              {:res_target,
               %PgQuery.ResTarget{
                 val: %PgQuery.Node{
                   node:
                     {:column_ref,
                      %PgQuery.ColumnRef{
                        fields: [
                          %PgQuery.Node{
                            node: {:a_star, %PgQuery.A_Star{}}
                          }
                        ]
                      }}
                 }
               }}
          }
        ],
        from_clause: [
          %PgQuery.Node{
            node:
              {:range_var,
               %PgQuery.RangeVar{
                 relname: "users",
                 inh: true
               }}
          }
        ]
      }

      assert {:ok, query} = ExPgQuery.deparse_stmt(stmt)
      assert query =~ "SELECT * FROM users"
    end
  end

  describe "deparse_expr/1" do
    test "deparses an expression" do
      expr = %PgQuery.Node{
        node:
          {:a_expr,
           %PgQuery.A_Expr{
             name: [%PgQuery.Node{node: {:string, %PgQuery.String{sval: "="}}}],
             lexpr: %PgQuery.Node{
               node:
                 {:column_ref,
                  %PgQuery.ColumnRef{
                    fields: [%PgQuery.Node{node: {:string, %PgQuery.String{sval: "id"}}}]
                  }}
             },
             rexpr: %PgQuery.Node{
               node:
                 {:a_const,
                  %PgQuery.A_Const{
                    val: {:ival, %PgQuery.Integer{ival: 1}}
                  }}
             }
           }}
      }

      assert {:ok, result} = ExPgQuery.deparse_expr(expr)
      assert result == "id = 1"
    end
  end

  describe "roundtrip parsing and deparsing" do
    test "with simple SELECT query" do
      query = "SELECT * FROM users WHERE active = true"
      {:ok, parsed} = ExPgQuery.parse_protobuf(query)
      {:ok, deparsed} = ExPgQuery.deparse(parsed)
      assert deparsed =~ "SELECT"
      assert deparsed =~ "FROM users"
      assert deparsed =~ "WHERE active = true"
    end

    test "with complex query including JOIN and GROUP BY" do
      query = """
      SELECT users.name, count(orders.id) AS order_count
      FROM users
      LEFT JOIN orders ON users.id = orders.user_id
      WHERE users.active = true
      GROUP BY users.name
      HAVING count(orders.id) > 5
      ORDER BY order_count DESC
      LIMIT 10
      """

      {:ok, parsed} = ExPgQuery.parse_protobuf(query)
      {:ok, deparsed} = ExPgQuery.deparse(parsed)

      assert deparsed == query |> String.replace("\n", " ") |> String.trim()
    end
  end
end
