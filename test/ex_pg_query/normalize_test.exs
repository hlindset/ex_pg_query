defmodule ExPgQuery.NormalizeTest do
  use ExUnit.Case

  describe "normalize" do
    test "normalizes a simple query" do
      {:ok, result} = ExPgQuery.normalize("SELECT 1")
      assert result == "SELECT $1"
    end

    test "normalizes IN(...)" do
      {:ok, result} = ExPgQuery.normalize("SELECT 1 FROM x WHERE y = 12561 AND z = '124' AND b IN (1, 2, 3)")
      assert result == "SELECT $1 FROM x WHERE y = $2 AND z = $3 AND b IN ($4, $5, $6)"
    end

    test "normalizes subselects" do
      {:ok, result} = ExPgQuery.normalize("SELECT 1 FROM x WHERE y = (SELECT 123 FROM a WHERE z = 'bla')")
      assert result == "SELECT $1 FROM x WHERE y = (SELECT $2 FROM a WHERE z = $3)"
    end

    test "normalizes ANY(array[...])" do
      {:ok, result} = ExPgQuery.normalize("SELECT * FROM x WHERE y = ANY(array[1, 2])")
      assert result == "SELECT * FROM x WHERE y = ANY(array[$1, $2])"
    end

    test "normalizes ANY(query)" do
      {:ok, result} = ExPgQuery.normalize("SELECT * FROM x WHERE y = ANY(SELECT 1)")
      assert result == "SELECT * FROM x WHERE y = ANY(SELECT $1)"
    end

    test "works with complicated strings" do
      {:ok, result} = ExPgQuery.normalize("SELECT U&'d\\0061t\\+000061' FROM x")
      assert result == "SELECT $1 FROM x"

      {:ok, result} = ExPgQuery.normalize("SELECT u&'d\\0061t\\+000061'    FROM x")
      assert result == "SELECT $1    FROM x"

      {:ok, result} = ExPgQuery.normalize("SELECT * FROM x WHERE z NOT LIKE E'abc'AND TRUE")
      assert result == "SELECT * FROM x WHERE z NOT LIKE $1AND $2"

      {:ok, result} = ExPgQuery.normalize("SELECT U&'d\\0061t\\+000061'-- comment\nFROM x")
      assert result == "SELECT $1-- comment\nFROM x"
    end

    test "normalizes COPY" do
      {:ok, result} = ExPgQuery.normalize("COPY (SELECT * FROM t WHERE id IN ('1', '2')) TO STDOUT")
      assert result == "COPY (SELECT * FROM t WHERE id IN ($1, $2)) TO STDOUT"
    end

    test "normalizes SETs" do
      {:ok, result} = ExPgQuery.normalize("SET test=123")
      assert result == "SET test=$1"
    end

    test "normalizes weird SETs" do
      {:ok, result} = ExPgQuery.normalize("SET CLIENT_ENCODING = UTF8")
      assert result == "SET CLIENT_ENCODING = $1"
    end

    test "does not fail if it does not understand parts of the statement" do
      {:ok, result} = ExPgQuery.normalize("DEALLOCATE bla; SELECT 1")
      assert result == "DEALLOCATE bla; SELECT $1"
    end

    test "normalizes EPXLAIN" do
      {:ok, result} = ExPgQuery.normalize("EXPLAIN SELECT x FROM y WHERE z = 1")
      assert result == "EXPLAIN SELECT x FROM y WHERE z = $1"
    end

    test "normalizes DECLARE CURSOR" do
      {:ok, result} = ExPgQuery.normalize("DECLARE cursor_b CURSOR FOR SELECT * FROM databases WHERE id = 23")
      assert result == "DECLARE cursor_b CURSOR FOR SELECT * FROM databases WHERE id = $1"
    end
  end
end
