defmodule ExPgQuery.ParserTest do
  use ExUnit.Case

  alias ExPgQuery.Parser

  describe "parse/1 for SELECT statements" do
    test "parses simple SELECT query" do
      {:ok, result} = Parser.parse("SELECT 1")

      assert_tables_eq(result, [])
      assert_cte_names_eq(result, [])
      assert_statement_types_eq(result, [:select_stmt])
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

      assert_select_tables_eq(result, ["snapshots", "system_snapshots"])
      assert_cte_names_eq(result, [])
      assert_statement_types_eq(result, [:select_stmt])
    end

    test "parses empty queries" do
      {:ok, result} = Parser.parse("-- nothing")

      assert_tables_eq(result, [])
      assert_cte_names_eq(result, [])
      assert_statement_types_eq(result, [])
    end

    test "parses nested SELECT in FROM clause" do
      {:ok, result} = Parser.parse("SELECT u.* FROM (SELECT * FROM users) u")

      assert_select_tables_eq(result, ["users"])
      assert_cte_names_eq(result, [])
      assert_statement_types_eq(result, [:select_stmt])
    end

    test "parses nested SELECT in WHERE clause" do
      {:ok, result} =
        Parser.parse("""
          SELECT users.id
          FROM users
          WHERE 1 = (SELECT COUNT(*) FROM user_roles)
        """)

      assert_select_tables_eq(result, ["user_roles", "users"])
      assert_cte_names_eq(result, [])
      assert_statement_types_eq(result, [:select_stmt])
    end

    test "parses WITH queries (CTEs)" do
      {:ok, result} =
        Parser.parse("""
          WITH cte AS (SELECT * FROM x WHERE x.y = $1)
          SELECT * FROM cte
        """)

      assert_select_tables_eq(result, ["x"])
      assert_cte_names_eq(result, ["cte"])
      assert_table_aliases_eq(result, [])
      assert_statement_types_eq(result, [:select_stmt])
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

      assert_select_tables_eq(result, ["table_a", "table_b"])
      assert_cte_names_eq(result, [])
      assert_statement_types_eq(result, [:select_stmt])
    end

    test "parses INTERSECT query" do
      {:ok, result} =
        Parser.parse("""
          SELECT id FROM table_a
          INTERSECT
          SELECT id FROM table_b
        """)

      assert_select_tables_eq(result, ["table_a", "table_b"])
      assert_cte_names_eq(result, [])
      assert_statement_types_eq(result, [:select_stmt])
    end

    test "parses EXCEPT query" do
      {:ok, result} =
        Parser.parse("""
          SELECT id FROM table_a
          EXCEPT
          SELECT id FROM table_b
        """)

      assert_select_tables_eq(result, ["table_a", "table_b"])
      assert_cte_names_eq(result, [])
      assert_statement_types_eq(result, [:select_stmt])
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

      assert_select_tables_eq(result, ["table_a", "table_b", "table_c", "table_d"])
      assert_table_aliases_eq(result, [])
      assert Enum.sort(result.cte_names) == ["cte_a", "cte_b"]
      assert_statement_types_eq(result, [:select_stmt])
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

      assert_select_tables_eq(result, ["table_a", "table_b", "table_c", "table_d"])
      assert_cte_names_eq(result, [])
      assert_statement_types_eq(result, [:select_stmt])
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

      assert_select_tables_eq(result, ["table_a", "table_b", "table_c"])
      assert_cte_names_eq(result, [])
      assert_statement_types_eq(result, [:select_stmt])
    end

    test "parses INTERSECT with ALL option" do
      {:ok, result} =
        Parser.parse("""
          SELECT id FROM table_a
          INTERSECT ALL
          SELECT id FROM table_b
        """)

      assert_select_tables_eq(result, ["table_a", "table_b"])
      assert_table_aliases_eq(result, [])
      assert_cte_names_eq(result, [])
      assert_statement_types_eq(result, [:select_stmt])
    end

    test "parses EXCEPT with ALL option" do
      {:ok, result} =
        Parser.parse("""
          SELECT id FROM table_a
          EXCEPT ALL
          SELECT id FROM table_b
        """)

      assert_select_tables_eq(result, ["table_a", "table_b"])
      assert_cte_names_eq(result, [])
      assert_statement_types_eq(result, [:select_stmt])
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

      assert_select_tables_eq(result, ["table_a", "table_b", "table_c", "table_d"])
      assert_table_aliases_eq(result, [])
      assert Enum.sort(result.cte_names) == ["cte_a", "cte_b"]
      assert_statement_types_eq(result, [:select_stmt])
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

      assert_select_tables_eq(result, ["org_tree"])
      assert_cte_names_eq(result, ["tree"])

      assert_table_aliases_eq(result, [
        %{alias: "child", location: 245, relation: "org_tree", schema: nil}
      ])

      assert_statement_types_eq(result, [:select_stmt])
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

      assert_select_tables_eq(result, [
        "table_a",
        "table_b",
        "table_c",
        "table_d",
        "table_e",
        "table_f"
      ])

      assert_cte_names_eq(result, [])
      assert_statement_types_eq(result, [:select_stmt])
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

      assert_select_tables_eq(result, ["table_a", "table_b", "table_c"])
      assert_cte_names_eq(result, [])
      assert_statement_types_eq(result, [:select_stmt])
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

      assert_select_tables_eq(result, [
        "table_a",
        "table_b",
        "table_c",
        "table_d",
        "table_e",
        "table_f"
      ])

      assert_cte_names_eq(result, [])
      assert_statement_types_eq(result, [:select_stmt])
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

      assert_select_tables_eq(result, [
        "archived_items",
        "extra_types",
        "items",
        "special_types"
      ])

      assert_cte_names_eq(result, [])
      assert_statement_types_eq(result, [:select_stmt])
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

      assert_select_tables_eq(result, ["graph"])

      assert_table_aliases_eq(result, [
        %{alias: "g", location: 147, relation: "graph", schema: nil},
        %{alias: "g", location: 268, relation: "graph", schema: nil},
        %{alias: "g", location: 467, relation: "graph", schema: nil}
      ])

      assert_cte_names_eq(result, ["search_graph"])
      assert_statement_types_eq(result, [:select_stmt])
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

      assert_select_tables_eq(result, [
        "archived_comments",
        "archived_users",
        "comments",
        "posts",
        "users"
      ])

      assert_cte_names_eq(result, [])
      assert_table_aliases_eq(result, [])
      assert_statement_types_eq(result, [:select_stmt])
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

      assert_select_tables_eq(result, [
        "active_users",
        "excluded_statuses",
        "pending_users",
        "users",
        "valid_statuses"
      ])

      assert_cte_names_eq(result, [])
      assert_table_aliases_eq(result, [])
      assert_statement_types_eq(result, [:select_stmt])
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

      assert_select_tables_eq(result, [
        "external_users",
        "legacy_users",
        "users",
        "verified_users"
      ])

      assert_cte_names_eq(result, [])
      assert_statement_types_eq(result, [:select_stmt])
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

      assert_select_tables_eq(result, ["table_a", "table_b"])
      assert_cte_names_eq(result, [])
      assert_statement_types_eq(result, [:select_stmt])
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

      assert_call_functions_eq(result, [
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
          {"users", "created_at"},
          {"users", "id"},
          {"users", "org_id"},
          {"users", "status"},
          {"user_roles", "role"},
          {"user_roles", "user_id"},
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

      assert_table_aliases_eq(
        result,
        [
          %{alias: "a", location: 305, relation: "admins", schema: nil},
          %{alias: "c", location: 200, relation: "comments", schema: nil},
          %{alias: "p", location: 158, relation: "posts", schema: nil},
          %{alias: "u", location: 136, relation: "users", schema: nil}
        ]
      )
    end

    test "extracts filter columns from IN clause" do
      {:ok, result} =
        Parser.parse("""
          SELECT * FROM users u
          WHERE u.org_id IN (SELECT id FROM organizations WHERE tier = 'premium')
        """)

      assert_filter_columns_eq(result, [
        {nil, "id"},
        {nil, "tier"},
        {"users", "org_id"}
      ])
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

      assert_select_tables_eq(result, ["posts", "users"])

      assert_table_aliases_eq(
        result,
        [
          %{alias: "u", location: 21, relation: "users", schema: nil},
          %{alias: "p", location: 58, relation: "posts", schema: nil}
        ]
      )

      assert_filter_columns_eq(result, [
        {"users", "id"},
        {"posts", "user_id"}
      ])

      assert_statement_types_eq(result, [:select_stmt])
    end

    test "parses table functions with aliases" do
      {:ok, result} =
        Parser.parse("""
        SELECT n
        FROM generate_series(1, 10) AS g(n)
        WHERE n > 5
        """)

      # generate_series is a function, not a table
      assert_tables_eq(result, [])
      assert_table_aliases_eq(result, [])
      assert_call_functions_eq(result, ["generate_series"])
      assert_statement_types_eq(result, [:select_stmt])
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

      assert_tables_eq(result, [])
      assert_table_aliases_eq(result, [])
      assert_statement_types_eq(result, [:select_stmt])
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

      assert_select_tables_eq(result, ["comments", "post_stats", "posts", "users"])

      assert_table_aliases_eq(
        result,
        [
          %{alias: "c", location: 126, relation: "comments", schema: nil},
          %{alias: "p", location: 49, relation: "posts", schema: nil},
          %{alias: "ps", location: 87, relation: "post_stats", schema: nil},
          %{alias: "u", location: 36, relation: "users", schema: nil}
        ]
      )

      assert_statement_types_eq(result, [:select_stmt])
    end

    test "parses schema qualified tables with aliases" do
      {:ok, result} =
        Parser.parse("""
        SELECT u.*, p.*
        FROM public.users u
        JOIN analytics.posts p ON p.user_id = u.id
        LEFT JOIN stats.user_metrics um ON um.user_id = u.id
        """)

      assert_select_tables_eq(result, ["analytics.posts", "public.users", "stats.user_metrics"])

      assert_table_aliases_eq(
        result,
        [
          %{alias: "p", location: 41, relation: "posts", schema: "analytics"},
          %{alias: "u", location: 21, relation: "users", schema: "public"},
          %{alias: "um", location: 89, relation: "user_metrics", schema: "stats"}
        ]
      )

      assert_statement_types_eq(result, [:select_stmt])
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

      assert_select_tables_eq(result, ["users"])

      assert_table_aliases_eq(
        result,
        [%{alias: "u", location: 176, relation: "users", schema: nil}]
      )

      assert_statement_types_eq(result, [:select_stmt])
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

      assert_select_tables_eq(result, ["items", "users"])

      assert_table_aliases_eq(
        result,
        [
          %{alias: "MixedCase", location: 206, relation: "items", schema: nil},
          %{alias: "Users", location: 104, relation: "users", schema: nil}
        ]
      )

      assert_statement_types_eq(result, [:select_stmt])
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

      assert_select_tables_eq(result, ["groups", "orders", "users"])

      assert_table_aliases_eq(result, [
        %{alias: "group", location: 132, relation: "groups", schema: nil},
        %{alias: "order", location: 79, relation: "orders", schema: nil},
        %{alias: "select", location: 59, relation: "users", schema: nil}
      ])

      assert_statement_types_eq(result, [:select_stmt])
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

      assert_select_tables_eq(result, ["payments", "posts", "users"])

      assert_table_aliases_eq(
        result,
        [
          %{alias: "p", location: 282, relation: "posts", schema: nil},
          %{alias: "pay", location: 411, relation: "payments", schema: nil},
          %{alias: "u", location: 32, relation: "users", schema: nil}
        ]
      )

      assert_cte_names_eq(result, ["cte"])
      assert_statement_types_eq(result, [:select_stmt])
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
      assert_select_tables_eq(result, Enum.map(0..29, &"t#{&1}"))
    end

    test "parses really deep queries (3)" do
      query_text =
        "SELECT * FROM foo " <>
          Enum.join(Enum.map(1..100, &"JOIN foo_#{&1} ON foo.id = foo_#{&1}.foo_id"), " ")

      {:ok, result} = Parser.parse(query_text)
      assert_select_tables_eq(result, Enum.map(1..100, &"foo_#{&1}") ++ ["foo"])
    end

    test "parses subquery alias scopes correctly" do
      Parser.parse("""
      SELECT e.name
      FROM employees AS e
      WHERE e.dept_id IN (
          SELECT d.id
          FROM departments AS d
          WHERE d.location = e.location
      )
      """)

      # todo: make sure all aliases resolve
      assert true
    end
  end

  describe "parse/1 for DML statements" do
    test "parses INSERT statements" do
      {:ok, result} =
        Parser.parse("""
        INSERT INTO users (name, email, created_at)
        VALUES
          ('John Doe', 'john@example.com', NOW()),
          ('Jane Doe', 'jane@example.com', NOW())
        RETURNING id, name
        """)

      assert_tables_eq(result, ["users"])
      assert_dml_tables_eq(result, ["users"])
      assert_select_tables_eq(result, [])
      assert_ddl_tables_eq(result, [])
      assert_cte_names_eq(result, [])
      assert_statement_types_eq(result, [:insert_stmt])
    end

    test "parses INSERT with SELECT" do
      {:ok, result} =
        Parser.parse("""
        WITH archived AS (
          SELECT id, name, email FROM archived_users WHERE status = 'active'
        )
        INSERT INTO users (name, email)
        SELECT name, email FROM archived
        RETURNING id
        """)

      assert_tables_eq(result, ["archived_users", "users"])
      assert_dml_tables_eq(result, ["users"])
      assert_select_tables_eq(result, ["archived_users"])
      assert_cte_names_eq(result, ["archived"])
      assert_statement_types_eq(result, [:insert_stmt])
    end

    test "parses UPDATE statements" do
      {:ok, result} =
        Parser.parse("""
        UPDATE users u
        SET
          status = 'inactive',
          updated_at = NOW()
        FROM user_sessions us
        WHERE u.id = us.user_id
          AND us.last_activity < NOW() - INTERVAL '30 days'
        RETURNING u.id, u.status
        """)

      assert_tables_eq(result, ["user_sessions", "users"])
      assert_dml_tables_eq(result, ["users"])
      assert_select_tables_eq(result, ["user_sessions"])
      assert_cte_names_eq(result, [])

      assert_table_aliases_eq(
        result,
        [
          %{alias: "u", location: 7, relation: "users", schema: nil},
          %{alias: "us", location: 68, relation: "user_sessions", schema: nil}
        ]
      )

      assert_statement_types_eq(result, [:update_stmt])
    end

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

    #     assert_select_tables_eq(result, ["user_data", "users"])
    #     assert_cte_names_eq(result, ["inactive_users"])
    #     assert_statement_types_eq(result, [:delete_stmt])
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

    #     assert_select_tables_eq(result, ["customer_accounts", "payment_transactions"])
    #     assert_cte_names_eq(result, [])
    #     assert result.table_aliases == ["ca", "pt"]
    #     assert_statement_types_eq(result, [:merge_stmt])
    #   end
  end

  describe "parse/1 for DDL statements" do
    test "finds the table in a SELECT INTO that is being created" do
      {:ok, result} =
        Parser.parse(~s|SELECT * INTO films_recent FROM films WHERE date_prod >= "2002-01-01"|)

      assert_ddl_tables_eq(result, ["films_recent"])
      assert_select_tables_eq(result, ["films"])
    end

    test "parses CREATE TABLE statements" do
      {:ok, result} =
        Parser.parse("""
        CREATE TABLE users (
          id SERIAL PRIMARY KEY,
          name VARCHAR(255) NOT NULL,
          email VARCHAR(255) UNIQUE,
          status user_status DEFAULT 'pending',
          created_at TIMESTAMP DEFAULT NOW(),
          CONSTRAINT valid_status CHECK (status IN ('pending', 'active', 'inactive')),
          CONSTRAINT unique_email UNIQUE (email)
        )
        """)

      assert_ddl_tables_eq(result, ["users"])
      assert_cte_names_eq(result, [])
      assert_statement_types_eq(result, [:create_stmt])
    end

    test "parses ALTER TABLE statements" do
      {:ok, result} =
        Parser.parse("""
        ALTER TABLE users
          ADD COLUMN last_login TIMESTAMP,
          ADD COLUMN login_count INTEGER DEFAULT 0,
          DROP COLUMN temporary_token,
          ADD CONSTRAINT positive_login_count CHECK (login_count >= 0),
          ALTER COLUMN status SET DEFAULT 'pending',
          DROP CONSTRAINT IF EXISTS old_constraint
        """)

      assert_ddl_tables_eq(result, ["users"])
      assert_cte_names_eq(result, [])
      assert_statement_types_eq(result, [:alter_table_stmt])
    end

    test "parses CREATE INDEX" do
      {:ok, result} =
        Parser.parse("""
        CREATE INDEX testidx
        ON test
        USING btree (a, (lower(b) || upper(c)))
        WHERE pow(a, 2) > 25
        """)

      assert_tables_eq(result, ["test"])
      assert_ddl_tables_eq(result, ["test"])
      assert_call_functions_eq(result, ["lower", "upper", "pow"])
      assert_filter_columns_eq(result, [{nil, "a"}])
    end

    test "parses CREATE INDEX statements" do
      {:ok, result} =
        Parser.parse("""
        CREATE UNIQUE INDEX CONCURRENTLY users_email_idx
        ON users (LOWER(email))
        WHERE deleted_at IS NULL
        """)

      assert_ddl_tables_eq(result, ["users"])
      assert_cte_names_eq(result, [])
      assert_statement_types_eq(result, [:index_stmt])
    end

    test "parses CREATE VIEW statements" do
      {:ok, result} =
        Parser.parse("""
        CREATE OR REPLACE VIEW active_users AS
        SELECT u.*, COUNT(s.id) as session_count
        FROM users u
        LEFT JOIN sessions s ON s.user_id = u.id
        WHERE u.status = 'active'
        GROUP BY u.id
        """)

      assert_select_tables_eq(result, ["sessions", "users"])
      assert_ddl_tables_eq(result, ["active_users"])
      assert_cte_names_eq(result, [])

      assert_table_aliases_eq(
        result,
        [
          %{alias: "s", location: 103, relation: "sessions", schema: nil},
          %{alias: "u", location: 85, relation: "users", schema: nil}
        ]
      )

      assert_statement_types_eq(result, [:view_stmt])
    end

    test "parses DROP statements" do
      {:ok, result} =
        Parser.parse("""
        DROP TABLE IF EXISTS temporary_users CASCADE;
        DROP TABLE IF EXISTS public.temporary_ids;
        DROP INDEX IF EXISTS users_email_idx;
        DROP VIEW IF EXISTS active_users;
        DROP SEQUENCE IF EXISTS user_id_seq;
        """)

      assert_ddl_tables_eq(result, ["temporary_users", "public.temporary_ids", "active_users"])
      assert_cte_names_eq(result, [])

      assert_statement_types_eq(result, [
        :drop_stmt,
        :drop_stmt,
        :drop_stmt,
        :drop_stmt,
        :drop_stmt
      ])
    end

    test "parses CREATE SEQUENCE statements" do
      {:ok, result} =
        Parser.parse("""
        CREATE SEQUENCE IF NOT EXISTS order_id_seq
        INCREMENT BY 1
        START WITH 1000
        NO MINVALUE
        NO MAXVALUE
        CACHE 1
        """)

      assert_tables_eq(result, [])
      assert_cte_names_eq(result, [])
      assert_statement_types_eq(result, [:create_seq_stmt])
    end

    test "parses CREATE SCHEMA statements" do
      {:ok, result} =
        Parser.parse("""
        CREATE SCHEMA IF NOT EXISTS analytics;
        CREATE TABLE daily_stats (
          id SERIAL PRIMARY KEY,
          date DATE NOT NULL,
          visits INTEGER DEFAULT 0
        );
        CREATE VIEW monthly_stats AS
          SELECT date_trunc('month', date) as month, SUM(visits) as total_visits
          FROM daily_stats
          GROUP BY date_trunc('month', date);
        """)

      assert_tables_eq(result, ["daily_stats", "monthly_stats"])
      assert_ddl_tables_eq(result, ["daily_stats", "monthly_stats"])
      assert_select_tables_eq(result, ["daily_stats"])
      assert_cte_names_eq(result, [])
      assert_table_aliases_eq(result, [])
      assert_statement_types_eq(result, [:create_schema_stmt, :create_stmt, :view_stmt])
    end
  end

  describe "parse" do
    test "parses ALTER TABLE" do
      {:ok, result} = Parser.parse("ALTER TABLE test ADD PRIMARY KEY (gid)")
      assert_tables_eq(result, ["test"])
      assert_ddl_tables_eq(result, ["test"])
    end

    test "parses ALTER VIEW" do
      {:ok, result} = Parser.parse("ALTER VIEW test SET (security_barrier = TRUE)")
      assert_tables_eq(result, ["test"])
      assert_ddl_tables_eq(result, ["test"])
    end

    test "parses ALTER INDEX" do
      {:ok, result} = Parser.parse("ALTER INDEX my_index_name SET (fastupdate = on)")
      assert_tables_eq(result, ["my_index_name"])
      assert_ddl_tables_eq(result, ["my_index_name"])
    end

    test "parses SET" do
      {:ok, result} = Parser.parse("SET statement_timeout=0")
      assert_tables_eq(result, [])
    end

    test "parses SHOW" do
      {:ok, result} = Parser.parse("SHOW work_mem")
      assert_tables_eq(result, [])
    end

    test "parses COPY" do
      {:ok, result} = Parser.parse("COPY test (id) TO stdout")
      assert_tables_eq(result, ["test"])
      assert_select_tables_eq(result, ["test"])
    end

    test "parses DROP TABLE" do
      {:ok, result} = Parser.parse("drop table abc.test123 cascade")
      assert_tables_eq(result, ["abc.test123"])
      assert_ddl_tables_eq(result, ["abc.test123"])
    end

    test "parses COMMIT" do
      {:ok, result} = Parser.parse("COMMIT")
    end

    test "parses CHECKPOINT" do
      {:ok, result} = Parser.parse("CHECKPOINT")
    end

    test "parses VACUUM" do
      {:ok, result} = Parser.parse("VACUUM my_table")
      assert_tables_eq(result, ["my_table"])
      assert_ddl_tables_eq(result, ["my_table"])
    end

    test "parses EXPLAIN" do
      {:ok, result} = Parser.parse("EXPLAIN DELETE FROM test")
      assert_tables_eq(result, ["test"])
    end

    test "parses SELECT INTO" do
      {:ok, result} = Parser.parse("CREATE TEMP TABLE test AS SELECT 1")
      assert_tables_eq(result, ["test"])
      assert_ddl_tables_eq(result, ["test"])
    end

    test "parses LOCK" do
      {:ok, result} = Parser.parse("LOCK TABLE public.schema_migrations IN ACCESS SHARE MODE")
      assert_tables_eq(result, ["public.schema_migrations"])
      assert_select_tables_eq(result, ["public.schema_migrations"])
    end

    test "parses CREATE TABLE" do
      {:ok, result} = Parser.parse("CREATE TABLE test (a int4)")
      assert_tables_eq(result, ["test"])
      assert_ddl_tables_eq(result, ["test"])
    end

    # test "fails to parse CREATE TABLE WITH OIDS" do
    #   expect { described_class.parse("CREATE TABLE test (a int4) WITH OIDS") }.to(raise_error do |error|
    #     expect(error).to be_a(PgQuery::ParseError)
    #     expect(error.location).to eq 33 # 33rd character in query string
    #   end)
    # end

    test "parses CREATE INDEX" do
      {:ok, result} =
        Parser.parse(
          "CREATE INDEX testidx ON test USING btree (a, (lower(b) || upper(c))) WHERE pow(a, 2) > 25"
        )

      assert_tables_eq(result, ["test"])
      assert_ddl_tables_eq(result, ["test"])
      assert_call_functions_eq(result, ["lower", "upper", "pow"])
      assert_filter_columns_eq(result, [{nil, "a"}])
    end

    test "parses CREATE SCHEMA" do
      {:ok, result} = Parser.parse("CREATE SCHEMA IF NOT EXISTS test AUTHORIZATION joe")
      assert_tables_eq(result, [])
    end

    test "parses CREATE VIEW" do
      {:ok, result} = Parser.parse("CREATE VIEW myview AS SELECT * FROM mytab")
      assert_tables_eq(result, ["myview", "mytab"])
      assert_ddl_tables_eq(result, ["myview"])
      assert_select_tables_eq(result, ["mytab"])
    end

    test "parses REFRESH MATERIALIZED VIEW" do
      {:ok, result} = Parser.parse("REFRESH MATERIALIZED VIEW myview")
      assert_tables_eq(result, ["myview"])
      assert_ddl_tables_eq(result, ["myview"])
    end

    test "parses CREATE RULE" do
      {:ok, result} =
        Parser.parse("CREATE RULE shoe_ins_protect AS ON INSERT TO shoe DO INSTEAD NOTHING")

      assert_tables_eq(result, ["shoe"])
    end

    test "parses CREATE TRIGGER" do
      {:ok, result} =
        Parser.parse("""
        CREATE TRIGGER check_update
        BEFORE UPDATE ON accounts
        FOR EACH ROW
        EXECUTE PROCEDURE check_account_update()
        """)

      assert_tables_eq(result, ["accounts"])
      assert_ddl_tables_eq(result, ["accounts"])
    end

    test "parses DROP SCHEMA" do
      {:ok, result} = Parser.parse("DROP SCHEMA myschema")
      assert_tables_eq(result, [])
    end

    test "parses DROP VIEW" do
      {:ok, result} = Parser.parse("DROP VIEW myview, myview2")
      # here it differs from the ruby implemention. for some reason views aren't
      # considered to be "tables" in DROP statements, while they are considered
      # to be tables in CREATE statements. we consider them to be tables in both.
      assert_tables_eq(result, ["myview", "myview2"])
      assert_ddl_tables_eq(result, ["myview", "myview2"])
    end

    test "parses DROP INDEX" do
      {:ok, result} = Parser.parse("DROP INDEX CONCURRENTLY myindex")
      assert_tables_eq(result, [])
    end

    test "parses DROP RULE" do
      {:ok, result} = Parser.parse("DROP RULE myrule ON mytable CASCADE")
      assert_tables_eq(result, ["mytable"])
      assert_ddl_tables_eq(result, ["mytable"])
    end

    test "parses DROP TRIGGER" do
      {:ok, result} = Parser.parse("DROP TRIGGER IF EXISTS mytrigger ON mytable RESTRICT")
      assert_tables_eq(result, ["mytable"])
      assert_ddl_tables_eq(result, ["mytable"])
    end

    test "parses GRANT" do
      {:ok, result} = Parser.parse("GRANT INSERT, UPDATE ON mytable TO myuser")
      assert_tables_eq(result, ["mytable"])
      assert_ddl_tables_eq(result, ["mytable"])
    end

    test "parses REVOKE" do
      {:ok, result} = Parser.parse("REVOKE admins FROM joe")
      assert_tables_eq(result, [])
    end

    test "parses TRUNCATE" do
      {:ok, result} = Parser.parse(~s|TRUNCATE bigtable, "fattable" RESTART IDENTITY|)
      assert_tables_eq(result, ["bigtable", "fattable"])
      assert_ddl_tables_eq(result, ["bigtable", "fattable"])

      assert_raw_tables_eq(result, [
        %{
          inh: true,
          location: 9,
          name: "bigtable",
          relname: "bigtable",
          schemaname: nil,
          type: :ddl,
          relpersistence: "p"
        },
        %{
          inh: true,
          location: 19,
          name: "fattable",
          relname: "fattable",
          schemaname: nil,
          type: :ddl,
          relpersistence: "p"
        }
      ])
    end

    test "parses WITH" do
      {:ok, result} =
        Parser.parse("WITH a AS (SELECT * FROM x WHERE x.y = $1 AND x.z = 1) SELECT * FROM a")

      assert_tables_eq(result, ["x"])
      assert_select_tables_eq(result, ["x"])
      assert_cte_names_eq(result, ["a"])
    end

    test "parses multi-line function definitions" do
      {:ok, result} =
        Parser.parse("""
        CREATE OR REPLACE FUNCTION thing(parameter_thing text)
          RETURNS bigint AS
        $BODY$
        DECLARE
                local_thing_id BIGINT := 0;
        BEGIN
                SELECT thing_id INTO local_thing_id FROM thing_map
                WHERE
                        thing_map_field = parameter_thing
                ORDER BY 1 LIMIT 1;

                IF NOT FOUND THEN
                        local_thing_id = 0;
                END IF;
                RETURN local_thing_id;
        END;
        $BODY$
        LANGUAGE plpgsql STABLE
        """)

      assert_tables_eq(result, [])
      assert_functions_eq(result, ["thing"])
    end

    test "parses table functions" do
      {:ok, result} =
        Parser.parse("""
        CREATE FUNCTION getfoo(int) RETURNS TABLE (f1 int) AS '
          SELECT * FROM foo WHERE fooid = $1;
        ' LANGUAGE SQL
        """)

      assert_tables_eq(result, [])
      assert_functions_eq(result, ["getfoo"])
      assert_ddl_functions_eq(result, ["getfoo"])
      assert_call_functions_eq(result, [])
    end

    test "correctly finds created functions" do
      {:ok, result} =
        Parser.parse("""
          CREATE OR REPLACE FUNCTION foo.testfunc(x integer) RETURNS integer AS $$
          BEGIN
          RETURN x
          END;
          $$ LANGUAGE plpgsql STABLE;
        """)

      assert_tables_eq(result, [])
      assert_functions_eq(result, ["foo.testfunc"])
      assert_ddl_functions_eq(result, ["foo.testfunc"])
      assert_call_functions_eq(result, [])
    end

    test "correctly finds called functions" do
      {:ok, result} = Parser.parse("SELECT foo.testfunc(1);")
      assert_tables_eq(result, [])
      assert_functions_eq(result, ["foo.testfunc"])
      assert_ddl_functions_eq(result, [])
      assert_call_functions_eq(result, ["foo.testfunc"])
    end

    test "correctly finds dropped functions" do
      {:ok, result} = Parser.parse("DROP FUNCTION IF EXISTS foo.testfunc(x integer);")
      assert_tables_eq(result, [])
      assert_functions_eq(result, ["foo.testfunc"])
      assert_ddl_functions_eq(result, ["foo.testfunc"])
      assert_call_functions_eq(result, [])
    end

    test "correctly finds renamed functions" do
      {:ok, result} = Parser.parse("ALTER FUNCTION foo.testfunc(integer) RENAME TO testfunc2;")
      assert_tables_eq(result, [])
      assert_functions_eq(result, ["foo.testfunc", "testfunc2"])
      assert_ddl_functions_eq(result, ["foo.testfunc", "testfunc2"])
      assert_call_functions_eq(result, [])
    end

    test "correctly finds nested tables in select clause" do
      {:ok, result} =
        Parser.parse(
          "select u.email, (select count(*) from enrollments e where e.user_id = u.id) as num_enrollments from users u"
        )

      assert_tables_eq(result, ["users", "enrollments"])
      assert_select_tables_eq(result, ["users", "enrollments"])
    end

    test "correctly separates CTE names from table names" do
      {:ok, result} =
        Parser.parse("WITH cte_name AS (SELECT 1) SELECT * FROM table_name, cte_name")

      assert_cte_names_eq(result, ["cte_name"])
      assert_tables_eq(result, ["table_name"])
      assert_select_tables_eq(result, ["table_name"])
    end

    test "correctly finds nested tables in from clause" do
      {:ok, result} = Parser.parse("select u.* from (select * from users) u")
      assert_tables_eq(result, ["users"])
      assert_select_tables_eq(result, ["users"])
    end

    test "correctly finds nested tables in where clause" do
      {:ok, result} =
        Parser.parse("select users.id from users where 1 = (select count(*) from user_roles)")

      assert_tables_eq(result, ["users", "user_roles"])
      assert_select_tables_eq(result, ["users", "user_roles"])
    end

    test "correctly finds tables in a select that has sub-selects without from clause" do
      {:ok, result} =
        Parser.parse("""
        SELECT *
        FROM pg_catalog.pg_class c
        JOIN (
          SELECT 17650 AS oid
          UNION ALL
          SELECT 17663 AS oid
        ) vals
        ON c.oid = vals.oid
        """)

      assert_tables_eq(result, ["pg_catalog.pg_class"])
      assert_select_tables_eq(result, ["pg_catalog.pg_class"])
      assert_filter_columns_eq(result, [{"pg_catalog.pg_class", "oid"}, {"vals", "oid"}])
    end

    test "traverse boolean expressions in where clause" do
      {:ok, result} =
        Parser.parse("""
          select users.*
          from users
          where users.id IN (
            select user_roles.user_id
            from user_roles
          ) and (users.created_at between '2016-06-01' and '2016-06-30')
        """)

      assert_tables_eq(result, ["users", "user_roles"])
    end

    test "correctly finds nested tables in the order by clause" do
      {:ok, result} =
        Parser.parse("""
          select users.*
          from users
          order by (
            select max(user_roles.role_id)
            from user_roles
            where user_roles.user_id = users.id
          )
        """)

      assert_tables_eq(result, ["users", "user_roles"])
    end

    test "correctly finds nested tables in the order by clause with multiple entries" do
      {:ok, result} =
        Parser.parse("""
          select users.*
          from users
          order by (
            select max(user_roles.role_id)
            from user_roles
            where user_roles.user_id = users.id
          ) asc, (
            select max(user_logins.role_id)
            from user_logins
            where user_logins.user_id = users.id
          ) desc
        """)

      assert_tables_eq(result, ["users", "user_roles", "user_logins"])
    end

    test "correctly finds nested tables in the group by clause" do
      {:ok, result} =
        Parser.parse("""
        select users.*
        from users
        group by (
          select max(user_roles.role_id)
          from user_roles
          where user_roles.user_id = users.id
        )
        """)

      assert_tables_eq(result, ["users", "user_roles"])
    end

    test "correctly finds nested tables in the group by clause with multiple entries" do
      {:ok, result} =
        Parser.parse("""
        select users.*
        from users
        group by (
          select max(user_roles.role_id)
          from user_roles
          where user_roles.user_id = users.id
        ), (
          select max(user_logins.role_id)
          from user_logins
          where user_logins.user_id = users.id
        )
        """)

      assert_tables_eq(result, ["users", "user_roles", "user_logins"])
    end

    test "correctly finds nested tables in the having clause" do
      {:ok, result} =
        Parser.parse("""
        select users.*
        from users
        group by users.id
        having 1 > (
          select count(user_roles.role_id)
          from user_roles
          where user_roles.user_id = users.id
        )
        """)

      assert_tables_eq(result, ["users", "user_roles"])
    end

    test "correctly finds nested tables in the having clause with a boolean expression" do
      {:ok, result} =
        Parser.parse("""
          select users.*
          from users
          group by users.id
          having true and 1 > (
            select count(user_roles.role_id)
            from user_roles
            where user_roles.user_id = users.id
          )
        """)

      assert_tables_eq(result, ["users", "user_roles"])
    end

    test "correctly finds nested tables in a subselect on a join" do
      {:ok, result} =
        Parser.parse("""
          select foo.*
          from foo
          join ( select * from bar ) b
          on b.baz = foo.quux
        """)

      assert_tables_eq(result, ["foo", "bar"])
    end

    test "correctly finds nested tables in a subselect in a join condition" do
      {:ok, result} =
        Parser.parse("""
          SELECT *
          FROM foo
          INNER JOIN join_a
            ON foo.id = join_a.id AND
            join_a.id IN (
              SELECT id
              FROM sub_a
              INNER JOIN sub_b
                ON sub_a.id = sub_b.id
                  AND sub_b.id IN (
                    SELECT id
                    FROM sub_c
                    INNER JOIN sub_d ON sub_c.id IN (SELECT id from sub_e)
                  )
            )
          INNER JOIN join_b
            ON foo.id = join_b.id AND
            join_b.id IN (
              SELECT id FROM sub_f
            )
        """)

      assert_tables_eq(result, [
        "foo",
        "join_a",
        "join_b",
        "sub_a",
        "sub_b",
        "sub_c",
        "sub_d",
        "sub_e",
        "sub_f"
      ])
    end

    test "does not list CTEs as tables after a UNION select" do
      {:ok, result} =
        Parser.parse("""
          with cte_a as (
            select * from table_a
          ), cte_b as (
            select * from table_b
          )

          select id from table_c
          left join cte_b on
            table_c.id = cte_b.c_id
          union
          select * from cte_a
        """)

      # xxx: match_array
      assert_tables_eq(result, ["table_a", "table_b", "table_c"])
      # xxx: match_array
      assert_cte_names_eq(result, ["cte_a", "cte_b"])
    end

    test "does not list CTEs as tables after a EXCEPT select" do
      {:ok, result} =
        Parser.parse("""
          with cte_a as (
            select * from table_a
          ), cte_b as (
            select * from table_b
          )

          select id from table_c
          left join cte_b on
            table_c.id = cte_b.c_id
          except
          select * from cte_a
        """)

      # xxx: match_array
      assert_tables_eq(result, ["table_a", "table_b", "table_c"])

      # xxx: match_array
      assert_cte_names_eq(result, ["cte_a", "cte_b"])
    end

    test "does not list CTEs as tables after a INTERSECT select" do
      {:ok, result} =
        Parser.parse("""
          with cte_a as (
            select * from table_a
          ), cte_b as (
            select * from table_b
          )

          select id from table_c
          left join cte_b on
            table_c.id = cte_b.c_id
          intersect
          select * from cte_a
        """)

      # xxx: match_array
      assert_tables_eq(result, ["table_a", "table_b", "table_c"])
      # xxx: match_array
      assert_cte_names_eq(result, ["cte_a", "cte_b"])
    end

    test "finds tables inside subselects in MIN/MAX and COALESCE functions" do
      {:ok, result} =
        Parser.parse("""
          SELECT GREATEST(
                   date_trunc($1, $2::timestamptz) + $3::interval,
                   COALESCE(
                     (
                       SELECT first_aggregate_starts_at
                         FROM schema_aggregate_infos
                        WHERE base_table = $4 LIMIT $5
                     ),
                     now() + $6::interval
                   )
                ) AS first_hourly_start_ts
        """)

      assert_tables_eq(result, ["schema_aggregate_infos"])
      assert_select_tables_eq(result, ["schema_aggregate_infos"])
      assert_dml_tables_eq(result, [])
      assert_ddl_tables_eq(result, [])
    end

    test "finds tables inside of case statements" do
      {:ok, result} =
        Parser.parse("""
          SELECT
            CASE
              WHEN id IN (SELECT foo_id FROM when_a) THEN (SELECT MAX(id) FROM then_a)
              WHEN id IN (SELECT foo_id FROM when_b) THEN (SELECT MAX(id) FROM then_b)
              ELSE (SELECT MAX(id) FROM elsey)
            END
          FROM foo
        """)

      assert_tables_eq(result, ["foo", "when_a", "when_b", "then_a", "then_b", "elsey"])
      assert_select_tables_eq(result, ["foo", "when_a", "when_b", "then_a", "then_b", "elsey"])
      assert_dml_tables_eq(result, [])
      assert_ddl_tables_eq(result, [])
    end

    test "finds tables inside of casts" do
      {:ok, result} =
        Parser.parse("""
          SELECT 1
          FROM   foo
          WHERE  x = any(cast(array(SELECT a FROM bar) as bigint[]))
              OR x = any(array(SELECT a FROM baz)::bigint[])
        """)

      # xxx: match_array
      assert_tables_eq(result, ["foo", "bar", "baz"])
    end

    test "finds functions in FROM clauses" do
      {:ok, result} =
        Parser.parse("""
        SELECT *
          FROM my_custom_func()
        """)

      assert_tables_eq(result, [])
      assert_select_tables_eq(result, [])
      assert_dml_tables_eq(result, [])
      assert_ddl_tables_eq(result, [])
      assert_ddl_functions_eq(result, [])
      assert_call_functions_eq(result, ["my_custom_func"])
    end

    test "finds functions inside LATERAL clauses" do
      {:ok, result} =
        Parser.parse("""
        SELECT *
          FROM unnest($1::text[]) AS a(x)
          LEFT OUTER JOIN LATERAL (
            SELECT json_build_object($2, "z"."z")
              FROM (
                SELECT *
                  FROM (
                    SELECT row_to_json(
                        (SELECT * FROM (SELECT public.my_function(b) FROM public.c) d)
                    )
                  ) e
            ) f
          ) AS g ON (1)
        """)

      assert_tables_eq(result, ["public.c"])
      assert_select_tables_eq(result, ["public.c"])
      assert_dml_tables_eq(result, [])
      assert_ddl_tables_eq(result, [])
      assert_ddl_functions_eq(result, [])

      assert_call_functions_eq(result, [
        "unnest",
        "json_build_object",
        "row_to_json",
        "public.my_function"
      ])
    end

    test "finds the table in a SELECT INTO that is being created" do
      {:ok, result} =
        Parser.parse("""
        SELECT * INTO films_recent FROM films WHERE date_prod >= '2002-01-01';
        """)

      assert_tables_eq(result, ["films", "films_recent"])
      assert_ddl_tables_eq(result, ["films_recent"])
      assert_select_tables_eq(result, ["films"])
    end
  end

  describe "parsing INSERT" do
    test "finds the table inserted into" do
      {:ok, result} =
        Parser.parse("""
          insert into users(pk, name) values (1, 'bob');
        """)

      assert_tables_eq(result, ["users"])
      assert_dml_tables_eq(result, ["users"])
    end

    test "finds tables in being selected from for insert" do
      {:ok, result} =
        Parser.parse("""
          insert into users(pk, name) select pk, name from other_users;
        """)

      # xxx: match_array
      assert_tables_eq(result, ["users", "other_users"])
    end

    test "finds tables in a CTE" do
      {:ok, result} =
        Parser.parse("""
          with cte as (
            select pk, name from other_users
          )
          insert into users(pk, name) select * from cte;
        """)

      # xxx: match_array
      assert_tables_eq(result, ["users", "other_users"])
    end
  end

  describe "parsing UPDATE" do
    test "finds the table updateed into" do
      {:ok, result} =
        Parser.parse("""
          update users set name = 'bob';
        """)

      assert_tables_eq(result, ["users"])
    end

    test "finds tables in a sub-select" do
      {:ok, result} =
        Parser.parse("""
          update users set name = (select name from other_users limit 1);
        """)

      # xxx: match_array
      assert_tables_eq(result, ["users", "other_users"])
    end

    test "finds tables in a CTE" do
      {:ok, result} =
        Parser.parse("""
          with cte as (
            select name from other_users limit 1
          )
          update users set name = (select name from cte);
        """)

      # xxx: match_array
      assert_tables_eq(result, ["users", "other_users"])
      assert_select_tables_eq(result, ["other_users"])
      assert_dml_tables_eq(result, ["users"])
    end

    test "finds tables referenced in the FROM clause" do
      {:ok, result} =
        Parser.parse("""
          UPDATE users SET name = users_new.name
          FROM users_new
          INNER JOIN join_table ON join_table.user_id = new_users.id
          WHERE users.id = users_new.id
        """)

      assert_tables_eq(result, ["users", "users_new", "join_table"])
      assert_select_tables_eq(result, ["users_new", "join_table"])
      assert_dml_tables_eq(result, ["users"])
      assert_ddl_tables_eq(result, [])
    end
  end

  describe "parsing DELETE" do
    test "finds the deleted table" do
      {:ok, result} =
        Parser.parse("""
          DELETE FROM users;
        """)

      assert_tables_eq(result, ["users"])
      assert_dml_tables_eq(result, ["users"])
    end

    test "finds the used table" do
      {:ok, result} =
        Parser.parse("""
          DELETE FROM users USING foo
            WHERE foo_id = foo.id AND foo.action = 'delete';
        """)

      assert_tables_eq(result, ["users", "foo"])
      assert_dml_tables_eq(result, ["users"])
      assert_select_tables_eq(result, ["foo"])
    end

    test "finds the table in the where subquery" do
      {:ok, result} =
        Parser.parse("""
          DELETE FROM users
            WHERE foo_id IN (SELECT id FROM foo WHERE action = 'delete');
        """)

      assert_tables_eq(result, ["users", "foo"])
      assert_dml_tables_eq(result, ["users"])
      assert_select_tables_eq(result, ["foo"])
    end

    test "finds tables in a CTE" do
      {:ok, result} =
        Parser.parse("""
          with cte as (
            select pk from other_users
          )
          delete from users
          where users.pk in (select pk from cte);
        """)

      # xxx: match_array
      assert_tables_eq(result, ["users", "other_users"])
      assert_select_tables_eq(result, ["other_users"])
      assert_dml_tables_eq(result, ["users"])
    end
  end

  test "handles DROP TYPE" do
    {:ok, result} = Parser.parse("DROP TYPE IF EXISTS repack.pk_something")
    assert_tables_eq(result, [])
  end

  test "handles COPY" do
    {:ok, result} = Parser.parse("COPY (SELECT test FROM abc) TO STDOUT WITH (FORMAT 'csv')")
    assert_tables_eq(result, ["abc"])
  end

  describe "parsing CREATE TABLE AS" do
    test "finds tables in the subquery" do
      {:ok, result} =
        Parser.parse("""
          CREATE TABLE foo AS
            SELECT * FROM bar;
        """)

      assert_tables_eq(result, ["foo", "bar"])
      assert_ddl_tables_eq(result, ["foo"])
      assert_select_tables_eq(result, ["bar"])
    end

    test "finds tables in the subquery with UNION" do
      {:ok, result} =
        Parser.parse("""
          CREATE TABLE foo AS
            SELECT id FROM bar UNION SELECT id from baz;
        """)

      assert_tables_eq(result, ["foo", "bar", "baz"])
      assert_ddl_tables_eq(result, ["foo"])
      assert_select_tables_eq(result, ["bar", "baz"])
    end

    test "finds tables in the subquery with EXCEPT" do
      alias Erl2exVendored.Pipeline.Parse

      {:ok, result} =
        Parser.parse("""
          CREATE TABLE foo AS
            SELECT id FROM bar EXCEPT SELECT id from baz;
        """)

      assert_tables_eq(result, ["foo", "bar", "baz"])
      assert_ddl_tables_eq(result, ["foo"])
      assert_select_tables_eq(result, ["bar", "baz"])
    end

    test "finds tables in the subquery with INTERSECT" do
      {:ok, result} =
        Parser.parse("""
          CREATE TABLE foo AS
            SELECT id FROM bar INTERSECT SELECT id from baz;
        """)

      assert_tables_eq(result, ["foo", "bar", "baz"])
      assert_ddl_tables_eq(result, ["foo"])
      assert_select_tables_eq(result, ["bar", "baz"])
    end
  end

  describe "parsing PREPARE" do
    test "finds tables in the subquery" do
      {:ok, result} =
        Parser.parse("""
        PREPARE qux AS SELECT bar from foo
        """)

      assert_tables_eq(result, ["foo"])
      assert_ddl_tables_eq(result, [])
      assert_select_tables_eq(result, ["foo"])
    end
  end

  test "parses CREATE TEMP TABLE" do
    {:ok, result} =
      Parser.parse("CREATE TEMP TABLE foo AS SELECT 1;")

    assert_tables_eq(result, ["foo"])
    assert_ddl_tables_eq(result, ["foo"])

    assert_raw_tables_eq(result, [
      %{
        inh: true,
        location: 18,
        name: "foo",
        relname: "foo",
        relpersistence: "t",
        schemaname: nil,
        type: :ddl
      }
    ])
  end

  describe "filter_columns" do
    test "finds unqualified names" do
      {:ok, result} = Parser.parse("SELECT * FROM x WHERE y = $1 AND z = 1")
      assert_filter_columns_eq(result, [{nil, "y"}, {nil, "z"}])
    end

    test "finds qualified names" do
      {:ok, result} = Parser.parse("SELECT * FROM x WHERE x.y = $1 AND x.z = 1")
      assert_filter_columns_eq(result, [{"x", "y"}, {"x", "z"}])
    end

    test "traverses into CTEs" do
      {:ok, result} =
        Parser.parse(
          "WITH a AS (SELECT * FROM x WHERE x.y = $1 AND x.z = 1) SELECT * FROM a WHERE b = 5"
        )

      assert_filter_columns_eq(result, [{"x", "y"}, {"x", "z"}, {nil, "b"}])
    end

    test "recognizes boolean tests" do
      {:ok, result} = Parser.parse("SELECT * FROM x WHERE x.y IS TRUE AND x.z IS NOT FALSE")
      assert_filter_columns_eq(result, [{"x", "y"}, {"x", "z"}])
    end

    test "finds COALESCE argument names" do
      {:ok, result} = Parser.parse("SELECT * FROM x WHERE x.y = COALESCE(z.a, z.b)")
      assert_filter_columns_eq(result, [{"x", "y"}, {"z", "a"}, {"z", "b"}])
    end

    for combiner <- ["UNION", "UNION ALL", "EXCEPT", "EXCEPT ALL", "INTERSECT", "INTERSECT ALL"] do
      test "finds unqualified names in #{combiner} query" do
        {:ok, result} =
          Parser.parse(
            "SELECT * FROM x where y = $1 #{unquote(combiner)} SELECT * FROM x where z = $2"
          )

        assert_filter_columns_eq(result, [{nil, "y"}, {nil, "z"}])
      end
    end
  end

  # Helper to assert statement types
  defp assert_statement_types_eq(result, expected) do
    assert Parser.statement_types(result) == expected
  end

  defp assert_tables_eq(result, expected) do
    assert Enum.sort(Parser.tables(result)) == Enum.sort(expected)
  end

  defp assert_select_tables_eq(result, expected) do
    assert Enum.sort(Parser.select_tables(result)) == Enum.sort(expected)
  end

  defp assert_ddl_tables_eq(result, expected) do
    assert Enum.sort(Parser.ddl_tables(result)) == Enum.sort(expected)
  end

  defp assert_dml_tables_eq(result, expected) do
    assert Enum.sort(Parser.dml_tables(result)) == Enum.sort(expected)
  end

  defp assert_raw_tables_eq(result, expected) do
    assert Enum.sort(result.tables) == Enum.sort(expected)
  end

  defp assert_functions_eq(result, expected) do
    assert Enum.sort(Parser.functions(result)) == Enum.sort(expected)
  end

  defp assert_call_functions_eq(result, expected) do
    assert Enum.sort(Parser.call_functions(result)) == Enum.sort(expected)
  end

  defp assert_ddl_functions_eq(result, expected) do
    assert Enum.sort(Parser.ddl_functions(result)) == Enum.sort(expected)
  end

  defp assert_filter_columns_eq(result, expected) do
    assert Enum.sort(result.filter_columns) == Enum.sort(expected)
  end

  defp assert_cte_names_eq(result, expected) do
    assert Enum.sort(result.cte_names) == Enum.sort(expected)
  end

  defp assert_table_aliases_eq(result, expected) do
    assert Enum.sort(result.table_aliases) == Enum.sort(expected)
  end
end
