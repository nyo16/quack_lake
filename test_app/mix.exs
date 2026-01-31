defmodule TestApp.MixProject do
  use Mix.Project

  def project do
    [
      app: :test_app,
      version: "0.1.0",
      elixir: "~> 1.15",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      aliases: aliases()
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {TestApp.Application, []}
    ]
  end

  defp deps do
    [
      {:quack_lake, path: ".."}
    ]
  end

  defp aliases do
    [
      demo: ["run -e 'TestApp.run_all_demos()'"],
      "demo.connection": ["run -e 'TestApp.Demos.ConnectionDemo.run()'"],
      "demo.query": ["run -e 'TestApp.Demos.QueryDemo.run()'"],
      "demo.extensions": ["run -e 'TestApp.Demos.ExtensionsDemo.run()'"],
      "demo.secrets": ["run -e 'TestApp.Demos.SecretsDemo.run()'"],
      "demo.lake": ["run -e 'TestApp.Demos.LakeManagementDemo.run()'"],
      "demo.appender": ["run -e 'TestApp.Demos.AppenderDemo.run()'"],
      "demo.timetravel": ["run -e 'TestApp.Demos.TimeTravelDemo.run()'"],
      "demo.ecto.duckdb": ["run -e 'TestApp.Demos.EctoDuckDBDemo.run()'"],
      "demo.ecto.ducklake": ["run -e 'TestApp.Demos.EctoDuckLakeDemo.run()'"],
      "demo.postgres": ["run -e 'TestApp.Demos.PostgresScannerDemo.run()'"]
    ]
  end
end
