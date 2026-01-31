defmodule TestApp do
  @moduledoc """
  QuackLake Demo Application.

  This application demonstrates all features of the QuackLake library:

  - Connection management
  - Query patterns
  - DuckDB extensions
  - Cloud storage secrets
  - Lake management (attach/detach)
  - High-performance bulk inserts (Appender API)
  - Time travel and snapshots
  - Ecto adapters (DuckDB and DuckLake)
  - PostgreSQL scanner integration

  ## Running Demos

      # Run all demos
      mix demo

      # Run individual demos
      mix demo.connection
      mix demo.query
      mix demo.extensions
      mix demo.secrets
      mix demo.lake
      mix demo.appender
      mix demo.timetravel
      mix demo.ecto.duckdb
      mix demo.ecto.ducklake
      mix demo.postgres

  ## Prerequisites

  Some demos require Docker services:

      # From the quack_lake root directory
      docker-compose up -d

  """

  alias TestApp.Demos.{
    ConnectionDemo,
    QueryDemo,
    ExtensionsDemo,
    SecretsDemo,
    LakeManagementDemo,
    AppenderDemo,
    TimeTravelDemo,
    EctoDuckDBDemo,
    EctoDuckLakeDemo,
    PostgresScannerDemo
  }

  @doc """
  Runs all demo modules in sequence.
  """
  def run_all_demos do
    IO.puts("""

    ╔══════════════════════════════════════════════════════════════╗
    ║              QUACKLAKE DEMO APPLICATION                      ║
    ║                                                              ║
    ║  Demonstrating all features of the QuackLake library         ║
    ╚══════════════════════════════════════════════════════════════╝
    """)

    demos = [
      {"Connection Management", &ConnectionDemo.run/0},
      {"Query Patterns", &QueryDemo.run/0},
      {"DuckDB Extensions", &ExtensionsDemo.run/0},
      {"Cloud Storage Secrets", &SecretsDemo.run/0},
      {"Lake Management", &LakeManagementDemo.run/0},
      {"Appender API (Bulk Inserts)", &AppenderDemo.run/0},
      {"Time Travel", &TimeTravelDemo.run/0},
      {"Ecto DuckDB Adapter", &EctoDuckDBDemo.run/0},
      {"Ecto DuckLake Adapter", &EctoDuckLakeDemo.run/0},
      {"PostgreSQL Scanner", &PostgresScannerDemo.run/0}
    ]

    results =
      Enum.map(demos, fn {name, demo_fn} ->
        IO.puts("\n>>> Running: #{name}")

        try do
          demo_fn.()
          {:ok, name}
        rescue
          e ->
            IO.puts("⚠ Error in #{name}: #{Exception.message(e)}")
            {:error, name}
        end
      end)

    # Summary
    successful = Enum.count(results, &match?({:ok, _}, &1))
    failed = Enum.count(results, &match?({:error, _}, &1))

    IO.puts("""

    ╔══════════════════════════════════════════════════════════════╗
    ║                      DEMO SUMMARY                            ║
    ╠══════════════════════════════════════════════════════════════╣
    ║  Successful: #{String.pad_trailing(to_string(successful), 3)} demos                                      ║
    ║  Failed:     #{String.pad_trailing(to_string(failed), 3)} demos                                      ║
    ╚══════════════════════════════════════════════════════════════╝

    For Docker-dependent demos, ensure services are running:
        docker-compose up -d

    Run individual demos with:
        mix demo.<name>

    """)
  end
end
