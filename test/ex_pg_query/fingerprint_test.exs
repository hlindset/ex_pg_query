defmodule ExPgQuery.FingerprintTest do
  use ExUnit.Case

  alias ExPgQuery.Fingerprint

  doctest ExPgQuery.Fingerprint

  describe "fingerprint" do
    # fingerprint_defs.each do |testdef|
    #   it format("returns expected hash parts for '%s'", testdef['input']) do
    #     assert fingerprint_parts(testdef['input'])).to eq(testdef['expectedParts'])
    #   end

    #   it format("returns expected hash value for '%s'", testdef['input']) do
    #     assert Fingerprint.fingerprint(testdef['input'])).to eq(testdef['expectedHash'])
    #   end
    # end

    test "fingerprint data cases" do
      for %{input: input, expected_hash: expected_hash} <- ExPgQuery.TestData.fingerprints() do
        assert Fingerprint.fingerprint(input) == {:ok, expected_hash}
      end
    end


    test "works for basic cases" do
      assert Fingerprint.fingerprint("SELECT 1") == Fingerprint.fingerprint("SELECT 2")
      assert Fingerprint.fingerprint("SELECT  1") == Fingerprint.fingerprint("SELECT 2")
      assert Fingerprint.fingerprint("SELECT A") == Fingerprint.fingerprint("SELECT a")
      assert Fingerprint.fingerprint("SELECT \"a\"") == Fingerprint.fingerprint("SELECT a")
      assert Fingerprint.fingerprint("  SELECT 1;") == Fingerprint.fingerprint("SELECT 2")
      assert Fingerprint.fingerprint("  ") == Fingerprint.fingerprint("")
      assert Fingerprint.fingerprint("--comment") == Fingerprint.fingerprint("")

      # Test uniqueness
      assert Fingerprint.fingerprint("SELECT a") != Fingerprint.fingerprint("SELECT b")
      assert Fingerprint.fingerprint("SELECT \"A\"") != Fingerprint.fingerprint("SELECT a")

      assert Fingerprint.fingerprint("SELECT * FROM a") !=
               Fingerprint.fingerprint("SELECT * FROM b")
    end

    test "works for multi-statement queries" do
      assert Fingerprint.fingerprint("SET x=$1; SELECT A") ==
               Fingerprint.fingerprint("SET x=$1; SELECT a")

      assert Fingerprint.fingerprint("SET x=$1; SELECT A") != Fingerprint.fingerprint("SELECT a")
    end

    test "ignores aliases" do
      assert Fingerprint.fingerprint("SELECT a AS b") == Fingerprint.fingerprint("SELECT a AS c")
      assert Fingerprint.fingerprint("SELECT a") == Fingerprint.fingerprint("SELECT a AS c")

      assert Fingerprint.fingerprint("SELECT * FROM a AS b") ==
               Fingerprint.fingerprint("SELECT * FROM a AS c")

      assert Fingerprint.fingerprint("SELECT * FROM a") ==
               Fingerprint.fingerprint("SELECT * FROM a AS c")

      assert Fingerprint.fingerprint("SELECT * FROM (SELECT * FROM x AS y) AS a") ==
               Fingerprint.fingerprint("SELECT * FROM (SELECT * FROM x AS z) AS b")

      assert Fingerprint.fingerprint("SELECT a AS b UNION SELECT x AS y") ==
               Fingerprint.fingerprint("SELECT a AS c UNION SELECT x AS z")
    end

    # XXX: These are marked as pending in the ruby version, and fails when uncommented,
    #      so will need some upstream changes to pass.
    # test "ignores aliases referenced in query" do
    #   assert Fingerprint.fingerprint("SELECT s1.id FROM snapshots s1") == Fingerprint.fingerprint("SELECT s2.id FROM snapshots s2")
    #   assert Fingerprint.fingerprint("SELECT a AS b ORDER BY b") == Fingerprint.fingerprint("SELECT a AS c ORDER BY c")
    # end

    test "ignores param references" do
      assert Fingerprint.fingerprint("SELECT $1") == Fingerprint.fingerprint("SELECT $2")
    end

    test "ignores SELECT target list ordering" do
      assert Fingerprint.fingerprint("SELECT a, b FROM x") ==
               Fingerprint.fingerprint("SELECT b, a FROM x")

      assert Fingerprint.fingerprint("SELECT $1, b FROM x") ==
               Fingerprint.fingerprint("SELECT b, $1 FROM x")

      assert Fingerprint.fingerprint("SELECT $1, $2, b FROM x") ==
               Fingerprint.fingerprint("SELECT $1, b, $2 FROM x")

      # Test uniqueness
      assert Fingerprint.fingerprint("SELECT a, c FROM x") !=
               Fingerprint.fingerprint("SELECT b, a FROM x")

      assert Fingerprint.fingerprint("SELECT b FROM x") !=
               Fingerprint.fingerprint("SELECT b, a FROM x")
    end

    test "ignores INSERT cols ordering" do
      assert Fingerprint.fingerprint("INSERT INTO test (a, b) VALUES ($1, $2)") ==
               Fingerprint.fingerprint("INSERT INTO test (b, a) VALUES ($1, $2)")

      # Test uniqueness
      assert Fingerprint.fingerprint("INSERT INTO test (a, c) VALUES ($1, $2)") !=
               Fingerprint.fingerprint("INSERT INTO test (b, a) VALUES ($1, $2)")

      assert Fingerprint.fingerprint("INSERT INTO test (b) VALUES ($1, $2)") !=
               Fingerprint.fingerprint("INSERT INTO test (b, a) VALUES ($1, $2)")
    end

    test "ignores IN list size (simple)" do
      q1 = "SELECT * FROM x WHERE y IN ($1, $2, $3)"
      q2 = "SELECT * FROM x WHERE y IN ($1)"
      assert Fingerprint.fingerprint(q1) == Fingerprint.fingerprint(q2)
    end

    test "ignores IN list size (complex)" do
      q1 = "SELECT * FROM x WHERE y IN ( $1::uuid, $2::uuid, $3::uuid )"
      q2 = "SELECT * FROM x WHERE y IN ( $1::uuid )"
      assert Fingerprint.fingerprint(q1) == Fingerprint.fingerprint(q2)
    end
  end
end
