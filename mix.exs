defmodule QuackLake.MixProject do
  use Mix.Project

  @version "0.1.0"
  @source_url "https://github.com/yourname/quack_lake"

  def project do
    [
      app: :quack_lake,
      version: @version,
      elixir: "~> 1.15",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
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
      {:ex_doc, "~> 0.34", only: :dev, runtime: false}
    ]
  end

  defp package do
    [
      licenses: ["MIT"],
      links: %{
        "GitHub" => @source_url
      },
      files: ~w(lib .formatter.exs mix.exs README.md LICENSE)
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
