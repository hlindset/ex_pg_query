defmodule ExPgQuery.MixProject do
  use Mix.Project

  def project do
    [
      app: :ex_pg_query,
      version: "0.1.0",
      elixir: "~> 1.17",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      test_coverage: [tool: ExCoveralls],
      preferred_cli_env: [
        coveralls: :test,
        "coveralls.detail": :test,
        "coveralls.post": :test,
        "coveralls.html": :test,
        "coveralls.json": :test,
        "coveralls.lcov": :test,
        "coveralls.cobertura": :test,
        "test.watch": :test
      ],
      compilers: [:elixir_make] ++ Mix.compilers(),
      # Docs
      name: "ExPgQuery",
      source_url: "https://github.com/hlindset/ex_pg_query",
      docs: &docs/0
    ]
  end

  defp docs do
    [
      # The main page in the docs
      main: "README",
      extras: ["README.md"]
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:protox, "~> 1.7"},
      {:elixir_make, "~> 0.9", runtime: false},
      {:ex_doc, "~> 0.36", only: :dev, runtime: false},
      {:excoveralls, "~> 0.18", only: :test},
      {:junit_formatter, "~> 3.4", only: :test},
      {:mix_test_watch, "~> 1.0", only: [:dev, :test], runtime: false}
    ]
  end
end
