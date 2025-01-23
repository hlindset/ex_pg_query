defmodule ExPgQueryTest do
  use ExUnit.Case
  doctest ExPgQuery

  test "greets the world" do
    assert ExPgQuery.hello() == :world
  end
end
