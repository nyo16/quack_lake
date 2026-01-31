defmodule TestApp.Demos.TimeTravelDemo do
  @moduledoc """
  Demonstrates DuckLake time travel features.

  Requires Docker services (PostgreSQL + MinIO) to be running.
  """

  def run do
    IO.puts("\n" <> String.duplicate("=", 60))
    IO.puts("TIME TRAVEL DEMO")
    IO.puts(String.duplicate("=", 60))
    IO.puts("Note: This demo requires Docker services (docker-compose up -d)")

    {:ok, conn} = QuackLake.open()

    case check_docker_services() do
      :ok ->
        setup_lake(conn)
        demo_snapshots(conn)
        demo_query_at_version(conn)
        cleanup(conn)
        IO.puts("\n✓ Time travel demo complete!\n")

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

  @lake_name "timetravel_demo"

  defp setup_lake(conn) do
    IO.puts("\n--- Setting up DuckLake ---")

    # Install and load ducklake extension
    :ok = QuackLake.Extension.ensure(conn, "ducklake")

    pg_config = Application.get_env(:test_app, :postgres, [])

    catalog_string =
      "postgres:host=#{pg_config[:host]};port=#{pg_config[:port]};database=#{pg_config[:database]};user=#{pg_config[:username]};password=#{pg_config[:password]}"

    # Attach lake
    attach_sql =
      "ATTACH '#{catalog_string};ducklake_alias=#{@lake_name}_#{System.system_time(:millisecond)}' AS #{@lake_name} (TYPE DUCKLAKE)"

    :ok = QuackLake.Query.execute(conn, attach_sql)
    IO.puts("  Attached lake: #{@lake_name}")

    # Create table
    :ok =
      QuackLake.Query.execute(conn, """
        CREATE TABLE #{@lake_name}.main.events (
          id INTEGER,
          event_type TEXT,
          value INTEGER
        )
      """)

    IO.puts("  Created events table")
  end

  defp demo_snapshots(conn) do
    IO.puts("\n--- Snapshots and Versions ---")

    # Version 1: Initial insert
    :ok =
      QuackLake.Query.execute(conn, """
        INSERT INTO #{@lake_name}.main.events VALUES
          (1, 'created', 100),
          (2, 'created', 200)
      """)

    IO.puts("  Version 1: Inserted 2 events")

    # Get snapshot
    {:ok, rows} = QuackLake.query(conn, "SELECT ducklake_current_snapshot('#{@lake_name}')")
    snapshot1 = hd(rows) |> Map.values() |> hd()
    IO.puts("  Current snapshot: #{snapshot1}")

    # Version 2: Update
    :ok =
      QuackLake.Query.execute(
        conn,
        "UPDATE #{@lake_name}.main.events SET value = 150 WHERE id = 1"
      )

    IO.puts("  Version 2: Updated event 1 value to 150")

    {:ok, rows} = QuackLake.query(conn, "SELECT ducklake_current_snapshot('#{@lake_name}')")
    snapshot2 = hd(rows) |> Map.values() |> hd()
    IO.puts("  Current snapshot: #{snapshot2}")

    # Version 3: Insert more
    :ok =
      QuackLake.Query.execute(conn, """
        INSERT INTO #{@lake_name}.main.events VALUES (3, 'updated', 300)
      """)

    IO.puts("  Version 3: Inserted event 3")

    {:ok, rows} = QuackLake.query(conn, "SELECT ducklake_current_snapshot('#{@lake_name}')")
    snapshot3 = hd(rows) |> Map.values() |> hd()
    IO.puts("  Current snapshot: #{snapshot3}")

    # Store for later
    Process.put(:snapshots, {snapshot1, snapshot2, snapshot3})
  end

  defp demo_query_at_version(conn) do
    IO.puts("\n--- Query at Historical Snapshots ---")

    {snapshot1, snapshot2, _snapshot3} = Process.get(:snapshots)

    # Current state
    {:ok, rows} = QuackLake.query(conn, "SELECT * FROM #{@lake_name}.main.events ORDER BY id")
    IO.puts("  Current state:")

    for row <- rows do
      IO.puts("    id=#{row["id"]}, type=#{row["event_type"]}, value=#{row["value"]}")
    end

    # Query at snapshot 1 (before update)
    {:ok, rows} =
      QuackLake.query(conn, """
        SELECT * FROM ducklake_snapshot_table('#{@lake_name}', 'main', 'events', #{snapshot1})
        ORDER BY id
      """)

    IO.puts("\n  At snapshot #{snapshot1} (initial insert):")

    for row <- rows do
      IO.puts("    id=#{row["id"]}, type=#{row["event_type"]}, value=#{row["value"]}")
    end

    # Query at snapshot 2 (after update, before insert)
    {:ok, rows} =
      QuackLake.query(conn, """
        SELECT * FROM ducklake_snapshot_table('#{@lake_name}', 'main', 'events', #{snapshot2})
        ORDER BY id
      """)

    IO.puts("\n  At snapshot #{snapshot2} (after update):")

    for row <- rows do
      IO.puts("    id=#{row["id"]}, type=#{row["event_type"]}, value=#{row["value"]}")
    end
  end

  defp cleanup(conn) do
    IO.puts("\n--- Cleanup ---")
    :ok = QuackLake.Query.execute(conn, "DETACH #{@lake_name}")
    IO.puts("  Detached #{@lake_name}")
  end
end
