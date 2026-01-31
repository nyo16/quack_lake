defmodule TestApp.Demos.PostgresScannerDemo do
  @moduledoc """
  Demonstrates querying PostgreSQL directly via postgres_scanner extension.

  Requires Docker services (PostgreSQL) to be running.
  """

  def run do
    IO.puts("\n" <> String.duplicate("=", 60))
    IO.puts("POSTGRES SCANNER DEMO")
    IO.puts(String.duplicate("=", 60))
    IO.puts("Note: This demo requires Docker services (docker-compose up -d)")

    {:ok, conn} = QuackLake.open()

    case check_docker_services() do
      :ok ->
        setup_postgres_extension(conn)
        attach_postgres(conn)
        demo_query_postgres(conn)
        demo_sync_to_duckdb(conn)
        cleanup(conn)
        IO.puts("\n✓ PostgreSQL scanner demo complete!\n")

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

  defp setup_postgres_extension(conn) do
    IO.puts("\n--- Loading postgres_scanner extension ---")

    :ok = QuackLake.Extension.ensure(conn, "postgres_scanner")
    IO.puts("  Loaded postgres_scanner extension")
  end

  defp attach_postgres(conn) do
    IO.puts("\n--- Attaching PostgreSQL Database ---")

    pg_config = Application.get_env(:test_app, :postgres, [])

    attach_sql = """
      ATTACH 'dbname=#{pg_config[:database]} user=#{pg_config[:username]} password=#{pg_config[:password]} host=#{pg_config[:host]} port=#{pg_config[:port]}'
      AS pg (TYPE POSTGRES, READ_ONLY)
    """

    :ok = QuackLake.Query.execute(conn, attach_sql)
    IO.puts("  Attached PostgreSQL as 'pg' (read-only)")
    IO.puts("    host: #{pg_config[:host]}:#{pg_config[:port]}")
    IO.puts("    database: #{pg_config[:database]}")
  end

  defp demo_query_postgres(conn) do
    IO.puts("\n--- Querying PostgreSQL Directly ---")

    # Query information_schema
    {:ok, rows} =
      QuackLake.query(conn, """
        SELECT table_schema, table_name
        FROM pg.information_schema.tables
        WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
        LIMIT 10
      """)

    IO.puts("  Tables in PostgreSQL (excluding system):")

    if length(rows) > 0 do
      for row <- rows do
        IO.puts("    - #{row["table_schema"]}.#{row["table_name"]}")
      end
    else
      IO.puts("    (no user tables found)")
    end

    # Query pg_stat_activity
    {:ok, rows} =
      QuackLake.query(conn, """
        SELECT datname, usename, state
        FROM pg.pg_catalog.pg_stat_activity
        WHERE state IS NOT NULL
        LIMIT 5
      """)

    IO.puts("\n  Active PostgreSQL connections:")

    for row <- rows do
      IO.puts("    - #{row["datname"]}: #{row["usename"]} (#{row["state"]})")
    end
  end

  defp demo_sync_to_duckdb(conn) do
    IO.puts("\n--- Syncing PostgreSQL Data to DuckDB ---")

    # Create a local copy of PostgreSQL system stats
    :ok =
      QuackLake.Query.execute(conn, """
        CREATE TABLE local_pg_stats AS
        SELECT datname, numbackends, xact_commit, xact_rollback
        FROM pg.pg_catalog.pg_stat_database
        WHERE datname IS NOT NULL
      """)

    IO.puts("  Created local_pg_stats from pg_stat_database")

    {:ok, rows} = QuackLake.query(conn, "SELECT * FROM local_pg_stats")
    IO.puts("  Synced #{length(rows)} database stats:")

    for row <- rows do
      IO.puts(
        "    - #{row["datname"]}: commits=#{row["xact_commit"]}, rollbacks=#{row["xact_rollback"]}"
      )
    end

    # Demonstrate joining PostgreSQL and DuckDB data
    :ok =
      QuackLake.Query.execute(conn, """
        CREATE TABLE local_metrics (db_name TEXT, metric_name TEXT, metric_value DOUBLE)
      """)

    :ok =
      QuackLake.Query.execute(conn, """
        INSERT INTO local_metrics VALUES
          ('ducklake_catalog', 'query_count', 42),
          ('ducklake_catalog', 'error_count', 2)
      """)

    {:ok, rows} =
      QuackLake.query(conn, """
        SELECT
          s.datname,
          s.numbackends,
          m.metric_name,
          m.metric_value
        FROM local_pg_stats s
        LEFT JOIN local_metrics m ON s.datname = m.db_name
        WHERE m.metric_name IS NOT NULL
      """)

    IO.puts("\n  Joined PostgreSQL stats with local metrics:")

    for row <- rows do
      IO.puts("    - #{row["datname"]}: #{row["metric_name"]} = #{row["metric_value"]}")
    end
  end

  defp cleanup(conn) do
    IO.puts("\n--- Cleanup ---")
    :ok = QuackLake.Query.execute(conn, "DROP TABLE IF EXISTS local_pg_stats")
    :ok = QuackLake.Query.execute(conn, "DROP TABLE IF EXISTS local_metrics")
    :ok = QuackLake.Query.execute(conn, "DETACH pg")
    IO.puts("  Dropped local tables and detached PostgreSQL")
  end
end
