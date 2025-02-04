defmodule ExPgQuery.FingerprintTest do
  use ExUnit.Case

  alias ExPgQuery.Fingerprint

  doctest ExPgQuery.Fingerprint

  defp fingerprint(query) do
    {:ok, result} = Fingerprint.fingerprint(query)
    result
  end

  describe "fingerprint" do
    test "fingerprint data cases" do
      for %{input: input, expected_hash: expected_hash} <- ExPgQuery.TestData.fingerprints() do
        assert fingerprint(input) == expected_hash
      end
    end

    test "returns error on invalid query" do
      assert {:error, "syntax error at or near \"sellect\""} == Fingerprint.fingerprint("sellect 1")
    end

    test "works for basic cases" do
      assert fingerprint("SELECT 1") == fingerprint("SELECT 2")
      assert fingerprint("SELECT  1") == fingerprint("SELECT 2")
      assert fingerprint("SELECT A") == fingerprint("SELECT a")
      assert fingerprint("SELECT \"a\"") == fingerprint("SELECT a")
      assert fingerprint("  SELECT 1;") == fingerprint("SELECT 2")
      assert fingerprint("  ") == fingerprint("")
      assert fingerprint("--comment") == fingerprint("")

      # Test uniqueness
      assert fingerprint("SELECT a") != fingerprint("SELECT b")
      assert fingerprint("SELECT \"A\"") != fingerprint("SELECT a")
      assert fingerprint("SELECT * FROM a") != fingerprint("SELECT * FROM b")
    end

    test "works for multi-statement queries" do
      assert fingerprint("SET x=$1; SELECT A") == fingerprint("SET x=$1; SELECT a")
      assert fingerprint("SET x=$1; SELECT A") != fingerprint("SELECT a")
    end

    test "ignores aliases" do
      assert fingerprint("SELECT a AS b") == fingerprint("SELECT a AS c")
      assert fingerprint("SELECT a") == fingerprint("SELECT a AS c")
      assert fingerprint("SELECT * FROM a AS b") == fingerprint("SELECT * FROM a AS c")
      assert fingerprint("SELECT * FROM a") == fingerprint("SELECT * FROM a AS c")

      assert fingerprint("SELECT * FROM (SELECT * FROM x AS y) AS a") ==
               fingerprint("SELECT * FROM (SELECT * FROM x AS z) AS b")

      assert fingerprint("SELECT a AS b UNION SELECT x AS y") ==
               fingerprint("SELECT a AS c UNION SELECT x AS z")
    end

    # XXX: These are marked as pending in the ruby version, and fails when uncommented,
    #      so will need some upstream changes to pass.
    # test "ignores aliases referenced in query" do
    #   assert fingerprint("SELECT s1.id FROM snapshots s1") == fingerprint("SELECT s2.id FROM snapshots s2")
    #   assert fingerprint("SELECT a AS b ORDER BY b") == fingerprint("SELECT a AS c ORDER BY c")
    # end

    test "ignores param references" do
      assert fingerprint("SELECT $1") == fingerprint("SELECT $2")
    end

    test "ignores SELECT target list ordering" do
      assert fingerprint("SELECT a, b FROM x") == fingerprint("SELECT b, a FROM x")
      assert fingerprint("SELECT $1, b FROM x") == fingerprint("SELECT b, $1 FROM x")
      assert fingerprint("SELECT $1, $2, b FROM x") == fingerprint("SELECT $1, b, $2 FROM x")

      # Test uniqueness
      assert fingerprint("SELECT a, c FROM x") != fingerprint("SELECT b, a FROM x")
      assert fingerprint("SELECT b FROM x") != fingerprint("SELECT b, a FROM x")
    end

    test "ignores INSERT cols ordering" do
      assert fingerprint("INSERT INTO test (a, b) VALUES ($1, $2)") ==
               fingerprint("INSERT INTO test (b, a) VALUES ($1, $2)")

      # Test uniqueness
      assert fingerprint("INSERT INTO test (a, c) VALUES ($1, $2)") !=
               fingerprint("INSERT INTO test (b, a) VALUES ($1, $2)")

      assert fingerprint("INSERT INTO test (b) VALUES ($1, $2)") !=
               fingerprint("INSERT INTO test (b, a) VALUES ($1, $2)")
    end

    test "ignores IN list size (simple)" do
      q1 = "SELECT * FROM x WHERE y IN ($1, $2, $3)"
      q2 = "SELECT * FROM x WHERE y IN ($1)"
      assert fingerprint(q1) == fingerprint(q2)
    end

    test "ignores IN list size (complex)" do
      q1 = "SELECT * FROM x WHERE y IN ( $1::uuid, $2::uuid, $3::uuid )"
      q2 = "SELECT * FROM x WHERE y IN ( $1::uuid )"
      assert fingerprint(q1) == fingerprint(q2)
    end
  end
end
