defmodule ExPgQuery.TruncatorTest do
  use ExUnit.Case

  alias ExPgQuery.Truncator

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

    defp assert_truncate_eq(query, truncate_length, expected) do
      {:ok, parse_result} = ExPgQuery.parse_protobuf(query)
      {:ok, result} = Truncator.truncate(parse_result, truncate_length)
      assert result == expected
    end

    #   test "leaves short queries unchanged" do
    #     query = "SELECT * FROM users"
    #     {:ok, parse_result} = ExPgQuery.parse_protobuf(query)
    #     {:ok, result} = Truncator.truncate(parse_result, 100)
    #     assert result == query
    #   end

    #   test "truncates SELECT statement target list" do
    #     query =
    #       "SELECT id, first_name, last_name, email, phone, address, city, state, zip FROM users"

    #     {:ok, parse_result} = ExPgQuery.parse_protobuf(query)
    #     {:ok, result} = Truncator.truncate(parse_result, 30)
    #     assert result == "SELECT ... FROM users"
    #   end

    #   test "truncates WHERE clause" do
    #     query = """
    #     SELECT * FROM users
    #     WHERE first_name = 'John' AND last_name = 'Smith' AND age > 21 AND city = 'New York'
    #     """

    #     {:ok, parse_result} = ExPgQuery.parse_protobuf(query)
    #     {:ok, result} = Truncator.truncate(parse_result, 50)
    #     assert result == "SELECT * FROM users WHERE ..."
    #   end

    #   test "truncates VALUES in INSERT" do
    #     query = """
    #     INSERT INTO users (first_name, last_name, email)
    #     VALUES ('John', 'Smith', 'john@example.com'),
    #            ('Jane', 'Doe', 'jane@example.com'),
    #            ('Bob', 'Wilson', 'bob@example.com')
    #     """

    #     {:ok, parse_result} = ExPgQuery.parse_protobuf(query)
    #     {:ok, result} = Truncator.truncate(parse_result, 70)
    #     assert result == "INSERT INTO users (first_name, last_name, email) VALUES (...)"
    #   end

    #   test "truncates column list in INSERT" do
    #     query = """
    #     INSERT INTO users (id, first_name, last_name, email, phone, address, city, state, zip, created_at)
    #     VALUES (1, 'John', 'Smith', 'john@example.com', '123-456-7890', '123 Main St', 'NY', 'NY', '12345', NOW())
    #     """

    #     {:ok, parse_result} = ExPgQuery.parse_protobuf(query)
    #     {:ok, result} = Truncator.truncate(parse_result, 50) |> IO.inspect()
    #     assert result == "INSERT INTO users (...) VALUES"
    #   end

    #   test "truncates CTE query" do
    #     query = """
    #     WITH user_data AS (
    #       SELECT id, first_name, last_name, email, phone, address
    #       FROM users
    #       WHERE active = true AND created_at > '2023-01-01'
    #     )
    #     SELECT * FROM user_data
    #     """

    #     {:ok, parse_result} = ExPgQuery.parse_protobuf(query)
    #     {:ok, result} = Truncator.truncate(parse_result, 60)
    #     assert result =~ ~r/WITH user_data AS \(SELECT .+\.\.\.\) SELECT/
    #   end

    #   test "handles multiple possible truncation points" do
    #     query = """
    #     SELECT id, name, email, phone
    #     FROM users
    #     WHERE active = true
    #     AND created_at > '2023-01-01'
    #     AND email LIKE '%@example.com'
    #     """

    #     {:ok, parse_result} = ExPgQuery.parse_protobuf(query)
    #     {:ok, result} = Truncator.truncate(parse_result, 50)

    #     # Should truncate the longer part (WHERE clause) first
    #     assert result == "SELECT id, name, email, phone FROM users WHERE ..."
    #   end

    #   test "falls back to hard truncation when smart truncation isn't enough" do
    #     query = "SELECT * FROM really_really_really_really_long_table_name"
    #     {:ok, parse_result} = ExPgQuery.parse_protobuf(query)
    #     {:ok, result} = Truncator.truncate(parse_result, 20) |> IO.inspect()
    #     assert String.length(result) <= 20
    #     assert String.ends_with?(result, "...")
    #   end

    #   test "handles invalid queries" do
    #     # Invalid SQL
    #     query = "SELECT * FROM"
    #     {:error, _} = ExPgQuery.parse_protobuf(query)
    #   end

    #   test "truncates UPDATE SET clause" do
    #     query = """
    #     UPDATE users
    #     SET first_name = 'John', last_name = 'Smith', email = 'john@example.com',
    #         phone = '123-456-7890', address = '123 Main St'
    #     WHERE id = 1
    #     """

    #     {:ok, parse_result} = ExPgQuery.parse_protobuf(query)
    #     {:ok, result} = Truncator.truncate(parse_result, 50)
    #     assert result =~ ~r/UPDATE users SET .+ WHERE id = 1/
    #   end

    #   test "handles multi-statement queries" do
    #     query = """
    #     BEGIN;
    #     UPDATE users SET status = 'active' WHERE id = 1;
    #     INSERT INTO audit_log (user_id, action) VALUES (1, 'status_update');
    #     COMMIT;
    #     """

    #     {:ok, parse_result} = ExPgQuery.parse_protobuf(query)
    #     {:ok, result} = Truncator.truncate(parse_result, 80)
    #     assert String.length(result) <= 80
    #   end

    #   test "preserves query structure after truncation" do
    #     query = """
    #     SELECT id, first_name, last_name, email
    #     FROM users
    #     WHERE active = true
    #     ORDER BY created_at DESC
    #     LIMIT 10
    #     """

    #     {:ok, parse_result} = ExPgQuery.parse_protobuf(query)
    #     {:ok, result} = Truncator.truncate(parse_result, 60)

    #     # Should still have basic SELECT structure
    #     assert result =~ ~r/SELECT .+ FROM .+ WHERE .+ ORDER BY .+ LIMIT \d+/
    #   end
    # end

    # describe "truncate!/2" do
    #   test "returns truncated string directly" do
    #     query = "SELECT * FROM users WHERE name = 'test'"
    #     {:ok, parse_result} = ExPgQuery.parse_protobuf(query)
    #     result = Truncator.truncate!(parse_result, 20)
    #     assert String.length(result) <= 20
    #   end

    #   test "raises error for invalid input" do
    #     assert_raise RuntimeError, fn ->
    #       Truncator.truncate!("invalid parse result", 20)
    #     end
    #   end
  end
end
