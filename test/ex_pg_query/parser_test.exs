defmodule ExPgQuery.ParserTest do
  use ExUnit.Case

  alias ExPgQuery.Parser

  describe "parse/1 for SELECT statements" do
    test "parses simple SELECT query" do
      {:ok, result} = Parser.parse("SELECT 1")

      assert result.tables == []
      assert result.cte_names == []
      assert_statement_types(result, [:select_stmt])
    end

    test "handles parse errors" do
      {:error, %{message: message}} =
        Parser.parse("CREATE RANDOM ix_test ON contacts.person;")

      assert message =~ "syntax error at or near \"RANDOM\""

      {:error, %{message: message}} = Parser.parse("SELECT 'ERR")
      assert message =~ "unterminated quoted string"
    end

    test "parses query with multiple tables" do
      {:ok, result} =
        Parser.parse("""
          SELECT memory_total_bytes, memory_free_bytes
          FROM snapshots s
          JOIN system_snapshots ON (snapshot_id = s.id)
          WHERE s.database_id = $1
        """)

      assert Enum.sort(result.tables) == ["snapshots", "system_snapshots"]
      assert result.cte_names == []
      assert_statement_types(result, [:select_stmt])
    end

    test "parses empty queries" do
      {:ok, result} = Parser.parse("-- nothing")

      assert result.tables == []
      assert result.cte_names == []
      assert_statement_types(result, [])
    end

    test "parses nested SELECT in FROM clause" do
      {:ok, result} = Parser.parse("SELECT u.* FROM (SELECT * FROM users) u")

      assert result.tables == ["users"]
      assert result.cte_names == []
      assert_statement_types(result, [:select_stmt])
    end

    test "parses nested SELECT in WHERE clause" do
      {:ok, result} =
        Parser.parse("""
          SELECT users.id
          FROM users
          WHERE 1 = (SELECT COUNT(*) FROM user_roles)
        """)

      assert Enum.sort(result.tables) == ["user_roles", "users"]
      assert result.cte_names == []
      assert_statement_types(result, [:select_stmt])
    end

    test "parses WITH queries (CTEs)" do
      {:ok, result} =
        Parser.parse("""
          WITH cte AS (SELECT * FROM x WHERE x.y = $1)
          SELECT * FROM cte
        """)

      assert result.tables == ["x"]
      assert result.cte_names == ["cte"]
      assert_statement_types(result, [:select_stmt])
    end
  end

  describe "parse/1 for set operations" do
    test "parses UNION query" do
      {:ok, result} =
        Parser.parse("""
          SELECT id FROM table_a
          UNION
          SELECT id FROM table_b
        """)

      assert Enum.sort(result.tables) == ["table_a", "table_b"]
      assert result.cte_names == []
      assert_statement_types(result, [:select_stmt])
    end

    test "parses INTERSECT query" do
      {:ok, result} =
        Parser.parse("""
          SELECT id FROM table_a
          INTERSECT
          SELECT id FROM table_b
        """)

      assert Enum.sort(result.tables) == ["table_a", "table_b"]
      assert result.cte_names == []
      assert_statement_types(result, [:select_stmt])
    end

    test "parses EXCEPT query" do
      {:ok, result} =
        Parser.parse("""
          SELECT id FROM table_a
          EXCEPT
          SELECT id FROM table_b
        """)

      assert Enum.sort(result.tables) == ["table_a", "table_b"]
      assert result.cte_names == []
      assert_statement_types(result, [:select_stmt])
    end

    test "parses complex set operations with CTEs" do
      {:ok, result} =
        Parser.parse("""
          WITH
            cte_a AS (SELECT * FROM table_a),
            cte_b AS (SELECT * FROM table_b)
          SELECT id FROM table_c
          LEFT JOIN cte_b ON table_c.id = cte_b.c_id
          UNION
          SELECT * FROM cte_a
          INTERSECT
          SELECT id FROM table_d
        """)

      assert Enum.sort(result.tables) == ["table_a", "table_b", "table_c", "table_d"]
      assert Enum.sort(result.cte_names) == ["cte_a", "cte_b"]
      assert_statement_types(result, [:select_stmt])
    end

    test "parses set operations with subqueries" do
      {:ok, result} =
        Parser.parse("""
          SELECT * FROM (
            SELECT id FROM table_a
            UNION
            SELECT id FROM table_b
          ) union_result
          WHERE id IN (
            SELECT id FROM table_c
            INTERSECT
            SELECT id FROM table_d
          )
        """)

      assert Enum.sort(result.tables) == ["table_a", "table_b", "table_c", "table_d"]
      assert result.cte_names == []
      assert_statement_types(result, [:select_stmt])
    end

    test "parses UNION ALL vs UNION DISTINCT" do
      {:ok, result} =
        Parser.parse("""
          SELECT id FROM table_a
          UNION ALL
          SELECT id FROM table_b
          UNION DISTINCT
          SELECT id FROM table_c
        """)

      assert Enum.sort(result.tables) == ["table_a", "table_b", "table_c"]
      assert result.cte_names == []
      assert_statement_types(result, [:select_stmt])
    end

    test "parses INTERSECT with ALL option" do
      {:ok, result} =
        Parser.parse("""
          SELECT id FROM table_a
          INTERSECT ALL
          SELECT id FROM table_b
        """)

      assert Enum.sort(result.tables) == ["table_a", "table_b"]
      assert result.cte_names == []
      assert_statement_types(result, [:select_stmt])
    end

    test "parses EXCEPT with ALL option" do
      {:ok, result} =
        Parser.parse("""
          SELECT id FROM table_a
          EXCEPT ALL
          SELECT id FROM table_b
        """)

      assert Enum.sort(result.tables) == ["table_a", "table_b"]
      assert result.cte_names == []
      assert_statement_types(result, [:select_stmt])
    end

    test "parses complex set operations with CTEs and ordering" do
      {:ok, result} =
        Parser.parse("""
          WITH
            cte_a AS (SELECT * FROM table_a ORDER BY id),
            cte_b AS (
              SELECT * FROM table_b
              UNION ALL
              SELECT * FROM table_c
              ORDER BY id DESC
            )
          SELECT id FROM table_d
          LEFT JOIN cte_b ON table_d.id = cte_b.d_id
          UNION ALL
          SELECT * FROM cte_a
          ORDER BY id
        """)

      assert Enum.sort(result.tables) == ["table_a", "table_b", "table_c", "table_d"]
      assert Enum.sort(result.cte_names) == ["cte_a", "cte_b"]
      assert_statement_types(result, [:select_stmt])
    end

    test "parses recursive CTEs with set operations" do
      {:ok, result} =
        Parser.parse("""
          WITH RECURSIVE tree AS (
            -- Base case
            SELECT id, parent_id, name, 1 AS level
            FROM org_tree
            WHERE parent_id IS NULL

            UNION ALL

            -- Recursive case
            SELECT child.id, child.parent_id, child.name, tree.level + 1
            FROM org_tree child
            JOIN tree ON tree.id = child.parent_id
          )
          SELECT * FROM tree ORDER BY level, id
        """)

      assert Enum.sort(result.tables) == ["org_tree"]
      assert result.cte_names == ["tree"]
      assert_statement_types(result, [:select_stmt])
    end

    test "parses complex nested set operations" do
      {:ok, result} =
        Parser.parse("""
          SELECT * FROM (
            SELECT id FROM table_a
            UNION ALL
            SELECT id FROM (
              SELECT id FROM table_b
              INTERSECT ALL
              SELECT id FROM table_c
            )
          ) AS combined
          WHERE id IN (
            SELECT id FROM table_d
            EXCEPT
            SELECT id FROM table_e
            UNION
            SELECT id FROM table_f
          )
          ORDER BY id DESC
        """)

      assert Enum.sort(result.tables) == [
               "table_a",
               "table_b",
               "table_c",
               "table_d",
               "table_e",
               "table_f"
             ]

      assert result.cte_names == []
      assert_statement_types(result, [:select_stmt])
    end

    test "parses set operations with complex ORDER BY and LIMIT" do
      {:ok, result} =
        Parser.parse("""
          SELECT id, name FROM table_a
          UNION ALL
          (SELECT id, name FROM table_b ORDER BY name LIMIT 10)
          UNION
          SELECT id, name FROM table_c
          ORDER BY id
          LIMIT 5 OFFSET 2
        """)

      assert Enum.sort(result.tables) == ["table_a", "table_b", "table_c"]
      assert result.cte_names == []
      assert_statement_types(result, [:select_stmt])
    end
  end

  # Helper to assert statement types
  defp assert_statement_types(result, expected) do
    assert Parser.statement_types(result) == expected
  end
end
