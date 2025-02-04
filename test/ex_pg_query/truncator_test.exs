defmodule ExPgQuery.TruncatorTest do
  use ExUnit.Case

  alias ExPgQuery.Protobuf
  alias ExPgQuery.Truncator

  doctest ExPgQuery.Truncator

  describe "truncate/2" do
    test "omits target list" do
      query = "SELECT a, b, c, d, e, f FROM xyz WHERE a = b"
      expected = "SELECT ... FROM xyz WHERE a = b"
      assert_truncate_eq(query, 40, expected)
    end

    test "omits with part of CTEs" do
      query = "WITH x AS (SELECT * FROM y) SELECT * FROM x"
      expected = "WITH x AS (...) SELECT * FROM x"
      assert_truncate_eq(query, 40, expected)
    end

    test "omits where clause" do
      query = "SELECT * FROM z WHERE a = b AND x = y"
      expected = "SELECT * FROM z WHERE ..."
      assert_truncate_eq(query, 30, expected)
    end

    test "omits INSERT field list" do
      query = "INSERT INTO \"x\" (a, b, c, d, e, f) VALUES ($1, $2, $3, $4, $5, $6)"
      expected = "INSERT INTO x (...) VALUES (...)"
      assert_truncate_eq(query, 32, expected)
    end

    test "omits comments" do
      query = "SELECT $1 /* application:test */"
      expected = "SELECT $1"
      assert_truncate_eq(query, 100, expected)
    end

    test "performs a simple truncation if necessary" do
      query = "SELECT * FROM t"
      expected = "SELECT ..."
      assert_truncate_eq(query, 10, expected)
    end

    test "works problematic cases" do
      query = "SELECT CASE WHEN $2.typtype = $3 THEN $2.typtypmod ELSE $1.atttypmod END"
      expected = "SELECT ..."
      assert_truncate_eq(query, 50, expected)
    end

    test "handles UPDATE target list" do
      query = "UPDATE x SET a = 1, c = 2, e = 'str'"
      expected = "UPDATE x SET ... = ..."
      assert_truncate_eq(query, 30, expected)
    end

    test "handles ON CONFLICT target list" do
      query =
        "INSERT INTO y(a) VALUES(1, 2, 3, 4) ON CONFLICT DO UPDATE SET a = 123456789"

      expected = "INSERT INTO y (a) VALUES (...) ON CONFLICT DO UPDATE SET ... = ..."
      assert_truncate_eq(query, 66, expected)
    end

    test "handles complex ON CONFLICT target lists" do
      query =
        """
        INSERT INTO foo (a, b, c, d)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29)
        ON CONFLICT (id)
        DO UPDATE SET (a, b, c, d) = (excluded.a,excluded.b,excluded.c,case when foo.d = excluded.d then excluded.d end)
        """

      expected =
        "INSERT INTO foo (a, b, c, d) VALUES (...) ON CONFLICT (id) DO UPDATE SET ... = ..."

      assert_truncate_eq(query, 100, expected)
    end

    test "handles GRANT access privileges" do
      query = "GRANT SELECT (abc, def, ghj) ON TABLE t1 TO r1"
      expected = "GRANT select (abc, def, ghj) ON ..."

      assert_truncate_eq(query, 35, expected)
    end

    test "leaves short queries unchanged" do
      query = "SELECT * FROM users"
      assert_truncate_eq(query, 100, query)
    end

    test "truncates SELECT statement target list" do
      query =
        "SELECT id, first_name, last_name, email, phone, address, city, state, zip FROM users"

      assert_truncate_eq(query, 30, "SELECT ... FROM users")
    end

    test "truncates WHERE clause" do
      query = """
      SELECT * FROM users
      WHERE first_name = 'John' AND last_name = 'Smith' AND age > 21 AND city = 'New York'
      """

      assert_truncate_eq(query, 50, "SELECT * FROM users WHERE ...")
    end

    test "truncates VALUES in INSERT" do
      query = """
      INSERT INTO users (first_name, last_name, email)
      VALUES ('John', 'Smith', 'john@example.com'),
             ('Jane', 'Doe', 'jane@example.com'),
             ('Bob', 'Wilson', 'bob@example.com')
      """

      assert_truncate_eq(
        query,
        70,
        "INSERT INTO users (first_name, last_name, email) VALUES (...)"
      )
    end

    test "truncates column list in INSERT" do
      query = """
      INSERT INTO users (id, first_name, last_name, email, phone, address, city, state, zip, created_at)
      VALUES (1, 'John', 'Smith', 'john@example.com', '123-456-7890', '123 Main St', 'NY', 'NY', '12345', NOW())
      """

      assert_truncate_eq(query, 50, "INSERT INTO users (...) VALUES (...)")
    end

    test "handles multiple possible truncation points" do
      query = """
      SELECT id, name, email, phone
      FROM users
      WHERE active = true
      AND created_at > '2023-01-01'
      AND email LIKE '%@example.com'
      """

      # Should truncate the longer part (WHERE clause) first
      assert_truncate_eq(query, 50, "SELECT id, name, email, phone FROM users WHERE ...")
    end

    test "falls back to hard truncation when smart truncation isn't enough" do
      query = "SELECT * FROM really_really_really_really_long_table_name"
      assert_truncate_eq(query, 20, "SELECT * FROM rea...")
    end

    test "handles multi-statement queries" do
      query = """
      BEGIN;
      UPDATE users SET status = 'active' WHERE id = 1;
      INSERT INTO audit_log (user_id, action) VALUES (1, 'status_update');
      COMMIT;
      """

      trunc_length = 80

      assert_truncate_eq(
        query,
        trunc_length,
        "BEGIN; UPDATE users SET ... = ... WHERE ...; INSERT INTO audit_log (...) VALU..."
      )

      assert_truncate_length(query, trunc_length, trunc_length)
    end
  end

  defp assert_truncate_eq(query, truncate_length, expected) do
    {:ok, parse_result} = Protobuf.from_sql(query)
    {:ok, result} = Truncator.truncate(parse_result, truncate_length)
    assert result == expected
  end

  defp assert_truncate_length(query, truncate_length, expected) do
    {:ok, parse_result} = Protobuf.from_sql(query)
    {:ok, result} = Truncator.truncate(parse_result, truncate_length)
    assert String.length(result) == expected
  end
end
