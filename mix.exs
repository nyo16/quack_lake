defmodule QuackLake.MixProject do
  use Mix.Project

  @version "0.2.5"
  @source_url "https://github.com/nyo16/quack_lake"

  def project do
    [
      app: :quack_lake,
      version: @version,
      elixir: "~> 1.15",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      aliases: aliases(),
      name: "QuackLake",
      description: "Elixir library for easy DuckLake access, setup, and management",
      package: package(),
      docs: docs(),
      source_url: @source_url
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:duckdbex, "~> 0.3.9"},
      {:ecto_sql, "~> 3.12"},
      {:db_connection, "~> 2.7"},
      {:jason, "~> 1.4", optional: true},
      {:ex_doc, "~> 0.34", only: :dev, runtime: false}
    ]
  end

  defp package do
    [
      licenses: ["Apache-2.0"],
      links: %{
        "GitHub" => @source_url
      },
      files: ~w(lib .formatter.exs mix.exs README.md LICENSE)
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp aliases do
    [
      "test.unit": ["test test/unit"],
      "test.integration": ["cmd INTEGRATION=true mix test test/integration"],
      "test.all": ["cmd INTEGRATION=true mix test"]
    ]
  end

  defp docs do
    [
      main: "readme",
      extras: ["README.md"],
      source_ref: "v#{@version}",
      groups_for_modules: [
        Core: [
          QuackLake,
          QuackLake.Connection,
          QuackLake.Query
        ],
        "Lake Management": [
          QuackLake.Lake,
          QuackLake.Snapshot
        ],
        "Cloud Storage": [
          QuackLake.Secret
        ],
        Utilities: [
          QuackLake.Config,
          QuackLake.Extension,
          QuackLake.Error
        ]
      ]
    ]
  end
end
