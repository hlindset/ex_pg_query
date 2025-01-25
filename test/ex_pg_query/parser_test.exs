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

  # Helper to assert statement types
  defp assert_statement_types(result, expected) do
    assert Parser.statement_types(result) == expected
  end
end
