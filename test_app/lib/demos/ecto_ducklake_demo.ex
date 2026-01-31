defmodule TestApp.Demos.EctoDuckLakeDemo do
  @moduledoc """
  Demonstrates Ecto.Adapters.DuckLake (concurrent writers mode).

  Requires Docker services (PostgreSQL + MinIO) to be running.
  """

  def run do
    IO.puts("\n" <> String.duplicate("=", 60))
    IO.puts("ECTO DUCKLAKE ADAPTER DEMO")
    IO.puts(String.duplicate("=", 60))
    IO.puts("Note: This demo requires Docker services (docker-compose up -d)")

    case check_docker_services() do
      :ok ->
        run_demo()

      {:error, reason} ->
        IO.puts("\n⚠ Skipping demo: #{reason}")
        IO.puts("  Run 'docker-compose up -d' from the quack_lake root directory\n")
    end
  end

  defp check_docker_services do
    pg_config = Application.get_env(:test_app, :postgres, [])

    case System.cmd("pg_isready", [
           "-h",
           pg_config[:host] || "localhost",
           "-p",
           to_string(pg_config[:port] || 5432),
           "-U",
           pg_config[:username] || "quacklake"
         ]) do
      {_, 0} -> :ok
      _ -> {:error, "PostgreSQL not available"}
    end
  rescue
    _ -> {:error, "pg_isready command not found"}
  end

  defp run_demo do
    IO.puts("\n--- Starting LakeRepo ---")

    case TestApp.LakeRepo.start_link([]) do
      {:ok, pid} ->
        IO.puts("  LakeRepo started (pid: #{inspect(pid)})")
        demo_concurrent_access()
        demo_lake_queries()
        GenServer.stop(pid)
        IO.puts("\n✓ Ecto DuckLake demo complete!\n")

      {:error, reason} ->
        IO.puts("  Failed to start LakeRepo: #{inspect(reason)}")
        IO.puts("  Check that PostgreSQL and MinIO are running\n")
    end
  end

  defp demo_concurrent_access do
    IO.puts("\n--- Concurrent Access (pool_size: 3) ---")

    # Run multiple queries concurrently
    tasks =
      for i <- 1..5 do
        Task.async(fn ->
          result = Ecto.Adapters.SQL.query!(TestApp.LakeRepo, "SELECT #{i} AS value")
          {i, hd(result.rows)}
        end)
      end

    results = Task.await_many(tasks, 5000)
    IO.puts("  Executed 5 concurrent queries:")

    for {i, [value]} <- results do
      IO.puts("    Query #{i}: value = #{value}")
    end
  end

  defp demo_lake_queries do
    IO.puts("\n--- Lake-Specific Queries ---")

    # Get lake info
    result =
      Ecto.Adapters.SQL.query!(TestApp.LakeRepo, """
        SELECT database_name, type FROM duckdb_databases() WHERE internal = false
      """)

    IO.puts("  Attached databases:")

    for [name, type] <- result.rows do
      IO.puts("    - #{name} (#{type})")
    end

    # Check extensions
    result =
      Ecto.Adapters.SQL.query!(TestApp.LakeRepo, """
        SELECT extension_name FROM duckdb_extensions() WHERE loaded = true LIMIT 5
      """)

    extensions = Enum.map(result.rows, &hd/1)
    IO.puts("  Loaded extensions: #{Enum.join(extensions, ", ")}")

    # Check secrets
    result = Ecto.Adapters.SQL.query!(TestApp.LakeRepo, "SELECT name, type FROM duckdb_secrets()")

    if length(result.rows) > 0 do
      IO.puts("  Configured secrets:")

      for [name, type] <- result.rows do
        IO.puts("    - #{name} (#{type})")
      end
    end
  end
end
