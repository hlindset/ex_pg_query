defmodule ExPgQuery.Parser2Test do
  use ExUnit.Case

  alias ExPgQuery.Parser

  describe "parse/1 for SELECT statements" do
    test "parses a simple query" do
      {:ok, result} = Parser.parse("SELECT 1")

      assert assert_tables(result, [])
      assert result.cte_names == []
      assert_statement_types(result, [:select_stmt])
    end

    test "handles errors" do
      {:error, %{message: message}} = Parser.parse("SELECT 'ERR")
      assert message =~ "unterminated quoted string"
    end

    test "parses real queries" do
      {:ok, result} =
        Parser.parse("""
          SELECT memory_total_bytes, memory_free_bytes, memory_pagecache_bytes,
                 memory_buffers_bytes, memory_applications_bytes,
                 (memory_swap_total_bytes - memory_swap_free_bytes) AS swap,
                 date_part($0, s.collected_at) AS collected_at
          FROM snapshots s
          JOIN system_snapshots ON (snapshot_id = s.id)
          WHERE s.database_id = $0
          AND s.collected_at BETWEEN $0 AND $0
          ORDER BY collected_at
        """)

      assert_tables(result, ["snapshots", "system_snapshots"])
      assert_select_tables(result, ["snapshots", "system_snapshots"])
    end

    test "parses empty queries" do
      {:ok, result} = Parser.parse("-- nothing")

      assert assert_tables(result, [])
      assert result.cte_names == []
      assert_statement_types(result, [])
    end

    test "parses floats with leading dot" do
      {:ok, result} = Parser.parse("SELECT .1")
      assert_statement_types(result, [:select_stmt])
    end

    test "parses floats with trailing dot" do
      {:ok, result} = Parser.parse("SELECT 1.")
      assert_statement_types(result, [:select_stmt])
    end

    test "parses bit strings (binary notation)" do
      {:ok, result} = Parser.parse("SELECT B'0101'")
      assert_statement_types(result, [:select_stmt])
    end

    test "parses bit strings (hex notation)" do
      {:ok, result} = Parser.parse("SELECT X'EFFF'")
      assert_statement_types(result, [:select_stmt])
    end
  end

  describe "parse/1 for DDL statements" do
    test "parses ALTER TABLE" do
      {:ok, result} = Parser.parse("ALTER TABLE test ADD PRIMARY KEY (gid)")

      assert_tables(result, ["test"])
      assert_ddl_tables(result, ["test"])
      assert_statement_types(result, [:alter_table_stmt])
    end

    test "parses ALTER VIEW" do
      {:ok, result} = Parser.parse("ALTER VIEW test SET (security_barrier = TRUE)")

      assert_tables(result, ["test"])
      assert_ddl_tables(result, ["test"])
      assert_statement_types(result, [:alter_table_stmt])
    end

    test "parses ALTER INDEX" do
      {:ok, result} = Parser.parse("ALTER INDEX my_index_name SET (fastupdate = on)")

      assert_tables(result, [])
      assert_ddl_tables(result, [])
      assert_statement_types(result, [:alter_table_stmt])
    end

    test "parses CREATE TABLE" do
      {:ok, result} = Parser.parse("CREATE TABLE test (a int4)")

      assert_tables(result, ["test"])
      assert_ddl_tables(result, ["test"])
      assert_statement_types(result, [:create_stmt])
    end

    test "fails to parse CREATE TABLE WITH OIDS" do
      {:error, error} = Parser.parse("CREATE TABLE test (a int4) WITH OIDS")
      assert error.message =~ "syntax error at or near \"OIDS\""
    end

    test "parses CREATE INDEX" do
      {:ok, result} =
        Parser.parse("""
          CREATE INDEX testidx ON test
          USING btree (a, (lower(b) || upper(c)))
          WHERE pow(a, 2) > 25
        """)

      assert_tables(result, ["test"])
      assert_ddl_tables(result, ["test"])
      assert_call_functions(result, ["lower", "upper", "pow"])
      assert result.filter_columns == [[nil, "a"]]
    end

    test "parses CREATE SCHEMA" do
      {:ok, result} = Parser.parse("CREATE SCHEMA IF NOT EXISTS test AUTHORIZATION joe")

      assert_tables(result, [])
      assert_statement_types(result, [:create_schema_stmt])
    end

    test "parses CREATE VIEW" do
      {:ok, result} = Parser.parse("CREATE VIEW myview AS SELECT * FROM mytab")

      assert_tables(result, ["myview", "mytab"])
      assert_ddl_tables(result, ["myview"])
      assert_select_tables(result, ["mytab"])
      assert_statement_types(result, [:view_stmt])
    end

    test "parses REFRESH MATERIALIZED VIEW" do
      {:ok, result} = Parser.parse("REFRESH MATERIALIZED VIEW myview")

      assert_tables(result, ["myview"])
      assert_ddl_tables(result, ["myview"])
      assert_statement_types(result, [:refresh_mat_view_stmt])
    end

    test "parses CREATE RULE" do
      {:ok, result} =
        Parser.parse("""
          CREATE RULE shoe_ins_protect AS ON INSERT TO shoe
          DO INSTEAD NOTHING
        """)

      assert_tables(result, ["shoe"])
      assert_statement_types(result, [:rule_stmt])
    end

    test "parses CREATE TRIGGER" do
      {:ok, result} =
        Parser.parse("""
          CREATE TRIGGER check_update
          BEFORE UPDATE ON accounts
          FOR EACH ROW
          EXECUTE PROCEDURE check_account_update()
        """)

      assert_tables(result, ["accounts"])
      assert_statement_types(result, [:create_trig_stmt])
    end

    test "parses DROP SCHEMA" do
      {:ok, result} = Parser.parse("DROP SCHEMA myschema")

      assert_tables(result, [])
      assert_statement_types(result, [:drop_stmt])
    end

    test "parses DROP VIEW" do
      {:ok, result} = Parser.parse("DROP VIEW myview, myview2")

      assert_tables(result, [])
      assert_statement_types(result, [:drop_stmt])
    end

    test "parses DROP INDEX" do
      {:ok, result} = Parser.parse("DROP INDEX CONCURRENTLY myindex")

      assert_tables(result, [])
      assert_statement_types(result, [:drop_stmt])
    end

    test "parses DROP RULE" do
      {:ok, result} = Parser.parse("DROP RULE myrule ON mytable CASCADE")

      assert_tables(result, ["mytable"])
      assert_statement_types(result, [:drop_stmt])
    end

    test "parses DROP TRIGGER" do
      {:ok, result} = Parser.parse("DROP TRIGGER IF EXISTS mytrigger ON mytable RESTRICT")

      assert_tables(result, ["mytable"])
      assert_statement_types(result, [:drop_stmt])
    end

    test "parses DROP TYPE" do
      {:ok, result} = Parser.parse("DROP TYPE IF EXISTS repack.pk_something")

      assert_tables(result, [])
      assert_statement_types(result, [:drop_stmt])
    end
  end

  describe "parse/1 for DML statements" do
    test "parses TRUNCATE" do
      {:ok, result} = Parser.parse(~c'TRUNCATE bigtable, "fattable" RESTART IDENTITY')

      assert_tables(result, ["bigtable", "fattable"])
      assert_ddl_tables(result, ["bigtable", "fattable"])
      assert_statement_types(result, [:truncate_stmt])
    end

    test "parses GRANT" do
      {:ok, result} = Parser.parse("GRANT INSERT, UPDATE ON mytable TO myuser")

      assert_tables(result, ["mytable"])
      assert_ddl_tables(result, ["mytable"])
      assert_statement_types(result, [:grant_stmt])
    end

    test "parses REVOKE" do
      {:ok, result} = Parser.parse("REVOKE admins FROM joe")

      assert_tables(result, [])
      assert_statement_types(result, [:grant_role_stmt])
    end
  end

  describe "parse/1 for transaction statements" do
    test "parses COMMIT" do
      {:ok, result} = Parser.parse("COMMIT")

      assert_statement_types(result, [:transaction_stmt])
    end
  end

  describe "parse/1 for system statements" do
    test "parses SET" do
      {:ok, result} = Parser.parse("SET statement_timeout=0")

      assert_tables(result, [])
      assert_statement_types(result, [:variable_set_stmt])
    end

    test "parses SHOW" do
      {:ok, result} = Parser.parse("SHOW work_mem")

      assert_tables(result, [])
      assert_statement_types(result, [:variable_show_stmt])
    end

    test "parses CHECKPOINT" do
      {:ok, result} = Parser.parse("CHECKPOINT")

      assert_statement_types(result, [:check_point_stmt])
    end
  end

  describe "parse/1 for utility statements" do
    test "parses VACUUM" do
      {:ok, result} = Parser.parse("VACUUM my_table")

      assert_tables(result, ["my_table"])
      assert_ddl_tables(result, ["my_table"])
      assert_statement_types(result, [:vacuum_stmt])
    end

    test "parses EXPLAIN" do
      {:ok, result} = Parser.parse("EXPLAIN DELETE FROM test")

      assert_tables(result, ["test"])
      assert_statement_types(result, [:explain_stmt])
    end

    test "parses COPY" do
      {:ok, result} = Parser.parse("COPY test (id) TO stdout")

      assert_tables(result, ["test"])
      assert_statement_types(result, [:copy_stmt])
    end

    test "parses LOCK" do
      {:ok, result} = Parser.parse("LOCK TABLE public.schema_migrations IN ACCESS SHARE MODE")

      assert_tables(result, ["public.schema_migrations"])
      assert_statement_types(result, [:lock_stmt])
    end
  end

  # Helper functions
  defp assert_statement_types(result, expected) do
    assert Parser.statement_types(result) == expected
  end

  defp assert_tables(result, expected) do
    assert Enum.sort(Parser.tables(result)) == Enum.sort(expected)
  end

  defp assert_select_tables(result, expected) do
    assert Enum.sort(Parser.select_tables(result)) == Enum.sort(expected)
  end

  defp assert_ddl_tables(result, expected) do
    assert Enum.sort(Parser.ddl_tables(result)) == Enum.sort(expected)
  end

  defp assert_dml_tables(result, expected) do
    assert Enum.sort(Parser.dml_tables(result)) == Enum.sort(expected)
  end

  defp assert_functions(result, expected) do
    assert Enum.sort(Parser.functions(result)) == Enum.sort(expected)
  end

  defp assert_call_functions(result, expected) do
    assert Enum.sort(Parser.call_functions(result)) == Enum.sort(expected)
  end

  defp assert_ddl_functions(result, expected) do
    assert Enum.sort(Parser.ddl_functions(result)) == Enum.sort(expected)
  end
end
