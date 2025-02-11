defmodule ExPgQuery.MixProject do
  use Mix.Project

  @version "0.1.0-dev"

  def project do
    [
      app: :ex_pg_query,
      version: @version,
      elixir: "~> 1.16",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      package: package(),
      description: description(),
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
      make_precompiler: {:nif, CCPrecompiler},
      make_precompiler_url:
        "https://github.com/hlindset/ex_pg_query/releases/download/v#{@version}/@{artefact_filename}",
      make_precompiler_filename: "ex_pg_query",
      make_precompiler_nif_versions: [versions: ["2.16", "2.17"]],
      # Docs
      name: "ExPgQuery",
      source_url: "https://github.com/hlindset/ex_pg_query",
      docs: &docs/0
    ]
  end

  defp package do
    [
      name: "ex_pg_query",
      licenses: ["MIT", "Apache-2.0"],
      source_url: "https://github.com/hlindset/ex_pg_query",
      homepage_url: "https://github.com/hlindset/ex_pg_query",
      links: %{
        "GitHub" => "https://github.com/hlindset/ex_pg_query"
      },
      files: ~w(lib priv .formatter.exs mix.exs README* LICENSE*
        CHANGELOG* src checksum.exs)
    ]
  end

  defp description do
    """
    Elixir library with a C NIF (based on libpg_query) for parsing,
    fingerprinting, normalizing and truncating PostgreSQL queries.
    """
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
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:cc_precompiler, "~> 0.1.10", runtime: false, github: "cocoa-xu/cc_precompiler"},
      {:mix_test_watch, "~> 1.0", only: [:dev, :test], runtime: false},
      {:excoveralls, "~> 0.18", only: :test},
      {:junit_formatter, "~> 3.4", only: :test}
    ]
  end
end
