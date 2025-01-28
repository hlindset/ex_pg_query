defmodule ExPgQuery.ParserTest do
  use ExUnit.Case

  alias ExPgQuery.Parser

  describe "parse/1 for SELECT statements" do
    test "parses simple SELECT query" do
      {:ok, result} = Parser.parse("SELECT 1")

      assert assert_tables(result, [])
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

      assert_select_tables(result, ["snapshots", "system_snapshots"])
      assert result.cte_names == []
      assert_statement_types(result, [:select_stmt])
    end

    test "parses empty queries" do
      {:ok, result} = Parser.parse("-- nothing")

      assert assert_tables(result, [])
      assert result.cte_names == []
      assert_statement_types(result, [])
    end

    test "parses nested SELECT in FROM clause" do
      {:ok, result} = Parser.parse("SELECT u.* FROM (SELECT * FROM users) u")

      assert_select_tables(result, ["users"])
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

      assert_select_tables(result, ["user_roles", "users"])
      assert result.cte_names == []
      assert_statement_types(result, [:select_stmt])
    end

    test "parses WITH queries (CTEs)" do
      {:ok, result} =
        Parser.parse("""
          WITH cte AS (SELECT * FROM x WHERE x.y = $1)
          SELECT * FROM cte
        """)

      assert_select_tables(result, ["x"])
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

      assert_select_tables(result, ["table_a", "table_b"])
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

      assert_select_tables(result, ["table_a", "table_b"])
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

      assert_select_tables(result, ["table_a", "table_b"])
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

      assert_select_tables(result, ["table_a", "table_b", "table_c", "table_d"])
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

      assert_select_tables(result, ["table_a", "table_b", "table_c", "table_d"])
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

      assert_select_tables(result, ["table_a", "table_b", "table_c"])
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

      assert_select_tables(result, ["table_a", "table_b"])
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

      assert_select_tables(result, ["table_a", "table_b"])
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

      assert_select_tables(result, ["table_a", "table_b", "table_c", "table_d"])
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

      assert_select_tables(result, ["org_tree"])
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

      assert_select_tables(result, [
        "table_a",
        "table_b",
        "table_c",
        "table_d",
        "table_e",
        "table_f"
      ])

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

      assert_select_tables(result, ["table_a", "table_b", "table_c"])
      assert result.cte_names == []
      assert_statement_types(result, [:select_stmt])
    end

    test "parses set operations in JOIN conditions" do
      {:ok, result} =
        Parser.parse("""
          SELECT * FROM table_a a
          LEFT JOIN table_b b ON b.id IN (
            SELECT id FROM table_c
            UNION ALL
            SELECT id FROM table_d
            WHERE id IN (
              SELECT id FROM table_e
              EXCEPT
              SELECT id FROM table_f
            )
          )
        """)

      assert_select_tables(result, [
        "table_a",
        "table_b",
        "table_c",
        "table_d",
        "table_e",
        "table_f"
      ])

      assert result.cte_names == []
      assert_statement_types(result, [:select_stmt])
    end

    test "parses set operations in aggregates and window functions" do
      {:ok, result} =
        Parser.parse("""
          SELECT
            x.id,
            sum(CASE WHEN x.type IN (
              SELECT type FROM special_types
              UNION
              SELECT type FROM extra_types
            ) THEN 1 ELSE 0 END) OVER (PARTITION BY x.group_id) as special_count
          FROM (
            SELECT * FROM items
            UNION ALL
            SELECT * FROM archived_items
          ) x
        """)

      assert_select_tables(result, [
        "archived_items",
        "extra_types",
        "items",
        "special_types"
      ])

      assert result.cte_names == []
      assert_statement_types(result, [:select_stmt])
    end

    test "parses set operations with complex recursive CTE expressions" do
      {:ok, result} =
        Parser.parse("""
          WITH RECURSIVE search_graph(id, link, data, depth, path, cycle) AS (
            SELECT g.id, g.link, g.data, 1,
              ARRAY[g.id],
              false
            FROM graph g
            UNION ALL
            SELECT g.id, g.link, g.data, sg.depth + 1,
              path || g.id,
              g.id = ANY(path)
            FROM graph g, search_graph sg
            WHERE g.link = sg.id AND NOT cycle
          )
          SELECT * FROM search_graph
          WHERE depth < 5
          UNION ALL
          SELECT * FROM (
            SELECT g.*, -1, ARRAY[]::integer[], true
            FROM graph g
            WHERE id IN (
              SELECT link FROM search_graph
              EXCEPT
              SELECT id FROM search_graph
            )
          ) leaf_nodes
        """)

      assert_select_tables(result, ["graph"])
      assert result.cte_names == ["search_graph"]
      assert_statement_types(result, [:select_stmt])
    end

    test "parses set operations with lateral joins" do
      {:ok, result} =
        Parser.parse("""
          SELECT a.*, counts.* FROM (
            SELECT id, name FROM users
            UNION ALL
            SELECT id, name FROM archived_users
          ) a
          CROSS JOIN LATERAL (
            SELECT
              (SELECT COUNT(*) FROM posts WHERE user_id = a.id) as post_count,
              (
                SELECT COUNT(*) FROM comments
                WHERE comment_user_id = a.id
                UNION ALL
                SELECT COUNT(*) FROM archived_comments
                WHERE comment_user_id = a.id
              ) as comment_count
          ) counts
        """)

      assert_select_tables(result, [
        "archived_comments",
        "archived_users",
        "comments",
        "posts",
        "users"
      ])

      assert result.cte_names == []
      assert_statement_types(result, [:select_stmt])
    end

    test "parses set operations in function arguments" do
      {:ok, result} =
        Parser.parse("""
          SELECT jsonb_build_object(
            'users', (
              SELECT json_agg(u) FROM (
                SELECT id, name FROM active_users
                UNION ALL
                SELECT id, name FROM pending_users
              ) u
            ),
            'counts', (
              SELECT json_build_object(
                'total', COUNT(*),
                'active', SUM(CASE WHEN status IN (
                  SELECT status FROM valid_statuses
                  EXCEPT
                  SELECT status FROM excluded_statuses
                ) THEN 1 ELSE 0 END)
              ) FROM users
            )
          )
        """)

      assert_select_tables(result, [
        "active_users",
        "excluded_statuses",
        "pending_users",
        "users",
        "valid_statuses"
      ])

      assert result.cte_names == []
      assert_statement_types(result, [:select_stmt])
    end

    test "parses set operations with different column counts/names" do
      {:ok, result} =
        Parser.parse("""
          SELECT id, name, created_at, updated_at FROM users
          UNION ALL
          SELECT id, name, created_at, NULL FROM legacy_users
          UNION ALL
          SELECT id, full_name, joined_date, last_seen
          FROM (
            SELECT * FROM external_users
            INTERSECT
            SELECT * FROM verified_users
          ) verified
          ORDER BY created_at DESC NULLS LAST
        """)

      assert_select_tables(result, [
        "external_users",
        "legacy_users",
        "users",
        "verified_users"
      ])

      assert result.cte_names == []
      assert_statement_types(result, [:select_stmt])
    end

    test "parses set operations with VALUES clauses" do
      {:ok, result} =
        Parser.parse("""
          SELECT id, 'current' as src FROM table_a
          UNION ALL
          SELECT id, 'archived' FROM table_b
          UNION ALL
          VALUES
            (1, 'synthetic'),
            (2, 'synthetic')
          UNION ALL
          SELECT * FROM (VALUES (3, 'dynamic'), (4, 'dynamic')) AS v(id, src)
          ORDER BY id
        """)

      assert_select_tables(result, ["table_a", "table_b"])
      assert result.cte_names == []
      assert_statement_types(result, [:select_stmt])
    end

    test "extracts functions from queries" do
      {:ok, result} =
        Parser.parse("""
          SELECT
            json_build_object(
              'stats', json_agg(
                json_build_object(
                  'count', count(*),
                  'sum', sum(amount),
                  'avg', avg(amount)
                )
              )
            ),
            array_agg(DISTINCT user_id),
            my_custom_func(col1, col2)
          FROM transactions
          WHERE amount > any(select unnest(array_agg(amount)) from other_transactions)
        """)

      assert_call_functions(result, [
        "array_agg",
        "avg",
        "count",
        "json_agg",
        "json_build_object",
        "my_custom_func",
        "sum",
        "unnest"
      ])
    end

    test "extracts filter columns from WHERE clauses" do
      {:ok, result} =
        Parser.parse("""
          SELECT * FROM users u
          WHERE
            u.status = 'active'
            AND u.created_at > now() - interval '1 day'
            AND u.org_id IN (SELECT id FROM organizations WHERE tier = 'premium')
            AND EXISTS (
              SELECT 1 FROM user_roles ur
              WHERE ur.user_id = u.id AND ur.role = 'admin'
            )
        """)

      expected_filters =
        Enum.sort([
          {nil, "id"},
          {"u", "created_at"},
          {"u", "id"},
          {"u", "org_id"},
          {"u", "status"},
          {"ur", "role"},
          {"ur", "user_id"},
          {nil, "tier"}
        ])

      assert Enum.sort(result.filter_columns) == expected_filters
    end

    test "extracts aliases" do
      {:ok, result} =
        Parser.parse("""
          WITH user_stats AS (
            SELECT
              u.id,
              u.name,
              COUNT(p.id) as post_count,
              SUM(c.likes) as total_likes
            FROM users u
            LEFT JOIN posts p ON p.user_id = u.id
            LEFT JOIN comments c ON c.post_id = p.id
            GROUP BY u.id, u.name
          )
          SELECT
            us.*,
            EXISTS(SELECT 1 FROM admins a WHERE a.user_id = us.id) as is_admin,
            CASE WHEN us.post_count > 10 THEN 'active' ELSE 'inactive' END as status
          FROM user_stats us
        """)

      assert result.table_aliases == %{
               "a" => %{name: "admins", type: :select},
               "c" => %{name: "comments", type: :select},
               "p" => %{name: "posts", type: :select},
               "u" => %{name: "users", type: :select}
             }
    end

    test "extracts filter columns from IN clause" do
      {:ok, result} =
        Parser.parse("""
          SELECT * FROM users u
          WHERE u.org_id IN (SELECT id FROM organizations WHERE tier = 'premium')
        """)

      expected_filters =
        Enum.sort([
          {nil, "id"},
          {nil, "tier"},
          {"u", "org_id"}
        ])

      assert Enum.sort(result.filter_columns) == expected_filters
    end

    test "parses queries with LATERAL joins and preserves aliases" do
      {:ok, result} =
        Parser.parse("""
        SELECT u.*, p.*
        FROM users u,
        LATERAL (
          SELECT p.* FROM posts p
          WHERE p.user_id = u.id
          LIMIT 5
        ) p
        """)

      assert_select_tables(result, ["posts", "users"])

      assert result.table_aliases == %{
               "p" => %{name: "posts", type: :select},
               "u" => %{name: "users", type: :select}
             }

      assert_statement_types(result, [:select_stmt])
    end

    test "parses table functions with aliases" do
      {:ok, result} =
        Parser.parse("""
        SELECT n
        FROM generate_series(1, 10) AS g(n)
        WHERE n > 5
        """)

      # generate_series is a function, not a table
      assert assert_tables(result, [])
      assert result.table_aliases == %{}
      assert_call_functions(result, ["generate_series"])
      assert_statement_types(result, [:select_stmt])
    end

    test "parses VALUES with column aliases" do
      {:ok, result} =
        Parser.parse("""
        SELECT v.x, v.y
        FROM (
          VALUES
            (1, 'a'),
            (2, 'b'),
            (3, 'c')
        ) AS v(x, y)
        WHERE v.x > 1
        """)

      assert assert_tables(result, [])
      assert result.table_aliases == %{}
      assert_statement_types(result, [:select_stmt])
    end

    test "parses different join types with aliases" do
      {:ok, result} =
        Parser.parse("""
        SELECT u.name, p.title, c.text
        FROM users u
        JOIN posts p ON p.user_id = u.id
        LEFT JOIN post_stats ps USING(post_id)
        LEFT JOIN comments c USING(post_id)
        WHERE u.active = true
        """)

      assert_select_tables(result, ["comments", "post_stats", "posts", "users"])

      assert result.table_aliases == %{
               "c" => %{name: "comments", type: :select},
               "p" => %{name: "posts", type: :select},
               "ps" => %{name: "post_stats", type: :select},
               "u" => %{name: "users", type: :select}
             }

      assert_statement_types(result, [:select_stmt])
    end

    test "parses schema qualified tables with aliases" do
      {:ok, result} =
        Parser.parse("""
        SELECT u.*, p.*
        FROM public.users u
        JOIN analytics.posts p ON p.user_id = u.id
        LEFT JOIN stats.user_metrics um ON um.user_id = u.id
        """)

      assert_select_tables(result, ["analytics.posts", "public.users", "stats.user_metrics"])

      assert result.table_aliases == %{
               "p" => %{name: "analytics.posts", type: :select},
               "u" => %{name: "public.users", type: :select},
               "um" => %{name: "stats.user_metrics", type: :select}
             }

      assert_statement_types(result, [:select_stmt])
    end

    test "parses column aliases" do
      {:ok, result} =
        Parser.parse("""
        SELECT
          u.id AS user_id,
          CASE WHEN u.type = 'admin' THEN true ELSE false END AS is_admin,
          COUNT(*) total_count,
          row_number() OVER (PARTITION BY u.type) AS position
        FROM users u
        GROUP BY u.id, u.type
        """)

      assert_select_tables(result, ["users"])

      assert result.table_aliases == %{
               "u" => %{name: "users", type: :select}
             }

      assert_statement_types(result, [:select_stmt])
    end

    test "parses case sensitive and quoted identifiers as aliases" do
      {:ok, result} =
        Parser.parse("""
        SELECT
          "Users".id,
          "Complex Name".value,
          UPPER(col) "UPPER_COL",
          "MixedCase".value "Value"
        FROM users "Users"
        JOIN (SELECT id, 'test' AS value) "Complex Name" ON "Complex Name".id = "Users".id
        JOIN items "MixedCase" ON "MixedCase".user_id = "Users".id
        """)

      assert_select_tables(result, ["items", "users"])

      assert result.table_aliases == %{
               "MixedCase" => %{name: "items", type: :select},
               "Users" => %{name: "users", type: :select}
             }

      assert_statement_types(result, [:select_stmt])
    end

    test "parses reserved keywords as aliases" do
      {:ok, result} =
        Parser.parse("""
        SELECT
          "select".id,
          "order".value,
          "group".name
        FROM users "select"
        JOIN orders "order" ON "order".user_id = "select".id
        JOIN groups "group" ON "group".id = "select".group_id
        """)

      assert_select_tables(result, ["groups", "orders", "users"])

      assert result.table_aliases == %{
               "group" => %{name: "groups", type: :select},
               "order" => %{name: "orders", type: :select},
               "select" => %{name: "users", type: :select}
             }

      assert_statement_types(result, [:select_stmt])
    end

    test "parses complex subqueries with multiple alias levels" do
      {:ok, result} =
        Parser.parse("""
        WITH cte AS (
          SELECT u.* FROM users u WHERE u.active = true
        )
        SELECT
          outer_q.user_id,
          stats.total
        FROM (
          SELECT
            inner_q.id as user_id
          FROM (
            SELECT cte.id
            FROM cte
            WHERE cte.type = 'premium'
          ) inner_q
          JOIN lateral (
            SELECT COUNT(*) AS cnt
            FROM posts p
            WHERE p.user_id = inner_q.id
          ) post_counts ON true
        ) outer_q
        LEFT JOIN LATERAL (
          SELECT SUM(amount) total
          FROM payments pay
          WHERE pay.user_id = outer_q.user_id
        ) stats ON true
        """)

      assert_select_tables(result, ["payments", "posts", "users"])

      assert result.table_aliases == %{
               "p" => %{name: "posts", type: :select},
               "pay" => %{name: "payments", type: :select},
               "u" => %{name: "users", type: :select}
             }

      assert result.cte_names == ["cte"]
      assert_statement_types(result, [:select_stmt])
    end

    test "parses really deep queries" do
      # Old JSON test that was kept for Protobuf version (queries like this have not been seen in the real world)
      query_text =
        "SELECT a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(a(b))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))"

      {:ok, result} = Parser.parse(query_text)
      assert Enum.sort(result.tables) == []
    end

    test "parses really deep queries (2)" do
      # Queries like this are uncommon, but have been seen in the real world.
      query_text = """
      SELECT * FROM "t0"
      JOIN "t1" ON (1) JOIN "t2" ON (1) JOIN "t3" ON (1) JOIN "t4" ON (1) JOIN "t5" ON (1)
      JOIN "t6" ON (1) JOIN "t7" ON (1) JOIN "t8" ON (1) JOIN "t9" ON (1) JOIN "t10" ON (1)
      JOIN "t11" ON (1) JOIN "t12" ON (1) JOIN "t13" ON (1) JOIN "t14" ON (1) JOIN "t15" ON (1)
      JOIN "t16" ON (1) JOIN "t17" ON (1) JOIN "t18" ON (1) JOIN "t19" ON (1) JOIN "t20" ON (1)
      JOIN "t21" ON (1) JOIN "t22" ON (1) JOIN "t23" ON (1) JOIN "t24" ON (1) JOIN "t25" ON (1)
      JOIN "t26" ON (1) JOIN "t27" ON (1) JOIN "t28" ON (1) JOIN "t29" ON (1)
      """

      {:ok, result} = Parser.parse(query_text)
      assert_select_tables(result, Enum.map(0..29, &"t#{&1}"))
    end

    test "parses really deep queries (3)" do
      query_text =
        "SELECT * FROM foo " <>
          Enum.join(Enum.map(1..100, &"JOIN foo_#{&1} ON foo.id = foo_#{&1}.foo_id"), " ")

      {:ok, result} = Parser.parse(query_text)
      assert_select_tables(result, Enum.map(1..100, &"foo_#{&1}") ++ ["foo"])
    end
  end

  # describe "parse/1 for DML statements" do
  #   test "parses INSERT statements" do
  #     {:ok, result} =
  #       Parser.parse("""
  #       INSERT INTO users (name, email, created_at)
  #       VALUES
  #         ('John Doe', 'john@example.com', NOW()),
  #         ('Jane Doe', 'jane@example.com', NOW())
  #       RETURNING id, name
  #       """)

  #     assert_select_tables(result, ["users"])
  #     assert result.cte_names == []
  #     assert_statement_types(result, [:insert_stmt])
  #   end

  #   test "parses INSERT with SELECT" do
  #     {:ok, result} =
  #       Parser.parse("""
  #       WITH archived AS (
  #         SELECT id, name, email FROM archived_users WHERE status = 'active'
  #       )
  #       INSERT INTO users (name, email)
  #       SELECT name, email FROM archived
  #       RETURNING id
  #       """)

  #     assert_select_tables(result, ["archived_users", "users"])
  #     assert result.cte_names == ["archived"]
  #     assert_statement_types(result, [:insert_stmt])
  #   end

  #   test "parses UPDATE statements" do
  #     {:ok, result} =
  #       Parser.parse("""
  #       UPDATE users u
  #       SET
  #         status = 'inactive',
  #         updated_at = NOW()
  #       FROM user_sessions us
  #       WHERE u.id = us.user_id
  #         AND us.last_activity < NOW() - INTERVAL '30 days'
  #       RETURNING u.id, u.status
  #       """)

  #     assert_select_tables(result, ["user_sessions", "users"])
  #     assert result.cte_names == []
  #     assert result.table_aliases == ["u", "us"]
  #     assert_statement_types(result, [:update_stmt])
  #   end

  #   test "parses DELETE statements" do
  #     {:ok, result} =
  #       Parser.parse("""
  #       WITH inactive_users AS (
  #         SELECT id FROM users
  #         WHERE last_login < NOW() - INTERVAL '1 year'
  #       )
  #       DELETE FROM user_data
  #       USING inactive_users
  #       WHERE user_data.user_id = inactive_users.id
  #       RETURNING user_id
  #       """)

  #     assert_select_tables(result, ["user_data", "users"])
  #     assert result.cte_names == ["inactive_users"]
  #     assert_statement_types(result, [:delete_stmt])
  #   end

  #   test "parses MERGE statements" do
  #     {:ok, result} =
  #       Parser.parse("""
  #       MERGE INTO customer_accounts ca
  #       USING payment_transactions pt
  #       ON ca.id = pt.account_id
  #       WHEN MATCHED THEN
  #         UPDATE SET balance = ca.balance + pt.amount
  #       WHEN NOT MATCHED THEN
  #         INSERT (id, balance) VALUES (pt.account_id, pt.amount)
  #       """)

  #     assert_select_tables(result, ["customer_accounts", "payment_transactions"])
  #     assert result.cte_names == []
  #     assert result.table_aliases == ["ca", "pt"]
  #     assert_statement_types(result, [:merge_stmt])
  #   end
  # end

  describe "parse/1 for DDL statements" do
    test "finds the table in a SELECT INTO that is being created" do
      {:ok, result} =
        Parser.parse(~s|SELECT * INTO films_recent FROM films WHERE date_prod >= "2002-01-01"|)

      assert Parser.ddl_tables(result) == ["films_recent"]
      assert Parser.select_tables(result) == ["films"]
    end

    #   test "parses CREATE TABLE statements" do
    #     {:ok, result} =
    #       Parser.parse("""
    #       CREATE TABLE users (
    #         id SERIAL PRIMARY KEY,
    #         name VARCHAR(255) NOT NULL,
    #         email VARCHAR(255) UNIQUE,
    #         status user_status DEFAULT 'pending',
    #         created_at TIMESTAMP DEFAULT NOW(),
    #         CONSTRAINT valid_status CHECK (status IN ('pending', 'active', 'inactive')),
    #         CONSTRAINT unique_email UNIQUE (email)
    #       )
    #       """)

    #     assert_select_tables(result, ["users"])
    #     assert result.cte_names == []
    #     assert_statement_types(result, [:create_stmt])
    #   end

    #   test "parses ALTER TABLE statements" do
    #     {:ok, result} =
    #       Parser.parse("""
    #       ALTER TABLE users
    #         ADD COLUMN last_login TIMESTAMP,
    #         ADD COLUMN login_count INTEGER DEFAULT 0,
    #         DROP COLUMN temporary_token,
    #         ADD CONSTRAINT positive_login_count CHECK (login_count >= 0),
    #         ALTER COLUMN status SET DEFAULT 'pending',
    #         DROP CONSTRAINT IF EXISTS old_constraint
    #       """)

    #     assert_select_tables(result, ["users"])
    #     assert result.cte_names == []
    #     assert_statement_types(result, [:alter_table_stmt])
    #   end

    #   test "parses CREATE INDEX statements" do
    #     {:ok, result} =
    #       Parser.parse("""
    #       CREATE UNIQUE INDEX CONCURRENTLY users_email_idx
    #       ON users (LOWER(email))
    #       WHERE deleted_at IS NULL
    #       """)

    #     assert_select_tables(result, ["users"])
    #     assert result.cte_names == []
    #     assert_statement_types(result, [:index_stmt])
    #   end

    #   test "parses CREATE VIEW statements" do
    #     {:ok, result} =
    #       Parser.parse("""
    #       CREATE OR REPLACE VIEW active_users AS
    #       SELECT u.*, COUNT(s.id) as session_count
    #       FROM users u
    #       LEFT JOIN sessions s ON s.user_id = u.id
    #       WHERE u.status = 'active'
    #       GROUP BY u.id
    #       """)

    #     assert_select_tables(result, ["sessions", "users"])
    #     assert result.cte_names == []
    #     assert result.table_aliases == ["s", "u"]
    #     assert_statement_types(result, [:view_stmt])
    #   end

    #   test "parses DROP statements" do
    #     {:ok, result} =
    #       Parser.parse("""
    #       DROP TABLE IF EXISTS temporary_users CASCADE;
    #       DROP INDEX IF EXISTS users_email_idx;
    #       DROP VIEW IF EXISTS active_users;
    #       DROP SEQUENCE IF EXISTS user_id_seq;
    #       """)

    #     assert_select_tables(result, ["temporary_users"])
    #     assert result.cte_names == []
    #     assert_statement_types(result, [:drop_stmt, :drop_stmt, :drop_stmt, :drop_stmt])
    #   end

    #   test "parses CREATE SEQUENCE statements" do
    #     {:ok, result} =
    #       Parser.parse("""
    #       CREATE SEQUENCE IF NOT EXISTS order_id_seq
    #       INCREMENT BY 1
    #       START WITH 1000
    #       NO MINVALUE
    #       NO MAXVALUE
    #       CACHE 1
    #       """)

    #     assert assert_tables(result, [])
    #     assert result.cte_names == []
    #     assert_statement_types(result, [:create_seq_stmt])
    #   end

    #   test "parses CREATE SCHEMA statements" do
    #     {:ok, result} =
    #       Parser.parse("""
    #       CREATE SCHEMA IF NOT EXISTS analytics
    #       CREATE TABLE daily_stats (
    #         id SERIAL PRIMARY KEY,
    #         date DATE NOT NULL,
    #         visits INTEGER DEFAULT 0
    #       )
    #       CREATE VIEW monthly_stats AS
    #         SELECT date_trunc('month', date) as month, SUM(visits) as total_visits
    #         FROM daily_stats
    #         GROUP BY date_trunc('month', date)
    #       """)

    #     assert_select_tables(result, ["daily_stats"])
    #     assert result.cte_names == []
    #     assert_statement_types(result, [:create_schema_stmt])
    #   end
  end

  # Helper to assert statement types
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
