defmodule ExPgQuery.Parser3Test do
  use ExUnit.Case
  alias ExPgQuery.Parser

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
      assert_tables_eq(result, [])
      assert_ddl_tables_eq(result, [])
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
      {:ok, result} = Parser.parse("CREATE RULE shoe_ins_protect AS ON INSERT TO shoe
                           DO INSTEAD NOTHING")
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
    end

    test "parses DROP INDEX" do
      {:ok, result} = Parser.parse("DROP INDEX CONCURRENTLY myindex")
      assert_tables_eq(result, [])
    end

    test "parses DROP RULE" do
      {:ok, result} = Parser.parse("DROP RULE myrule ON mytable CASCADE")
      assert_tables_eq(result, ["mytable"])
    end

    test "parses DROP TRIGGER" do
      {:ok, result} = Parser.parse("DROP TRIGGER IF EXISTS mytrigger ON mytable RESTRICT")
      assert_tables_eq(result, ["mytable"])
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

    # https://github.com/pganalyze/pg_query/issues/38
    test "correctly finds nested tables in select clause" do
      {:ok, result} =
        Parser.parse(
          "select u.email, (select count(*) from enrollments e where e.user_id = u.id) as num_enrollments from users u"
        )

      assert_tables_eq(result, ["users", "enrollments"])
      assert_select_tables_eq(result, ["users", "enrollments"])
    end

    # https://github.com/pganalyze/pg_query/issues/52
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
        Parser.parse(
          "SELECT * FROM pg_catalog.pg_class c JOIN (SELECT 17650 AS oid UNION ALL SELECT 17663 AS oid) vals ON c.oid = vals.oid"
        )

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
