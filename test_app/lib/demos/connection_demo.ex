defmodule TestApp.Demos.ConnectionDemo do
  @moduledoc """
  Demonstrates QuackLake connection management.
  """

  def run do
    IO.puts("\n" <> String.duplicate("=", 60))
    IO.puts("CONNECTION MANAGEMENT DEMO")
    IO.puts(String.duplicate("=", 60))

    demo_in_memory()
    demo_persistent()
    demo_bang_variants()
    demo_with_options()

    IO.puts("\n✓ Connection demo complete!\n")
  end

  defp demo_in_memory do
    IO.puts("\n--- In-Memory Database ---")

    {:ok, conn} = QuackLake.open()
    IO.puts("  Opened in-memory database")

    {:ok, rows} = QuackLake.query(conn, "SELECT 'Hello from in-memory!' AS greeting")
    IO.puts("  Query result: #{inspect(rows)}")
  end

  defp demo_persistent do
    IO.puts("\n--- Persistent Database ---")

    path = "/tmp/demo_persistent.duckdb"
    File.rm(path)

    {:ok, conn} = QuackLake.open(path: path)
    IO.puts("  Opened persistent database at #{path}")

    :ok = QuackLake.Query.execute(conn, "CREATE TABLE test (id INTEGER, value TEXT)")
    :ok = QuackLake.Query.execute(conn, "INSERT INTO test VALUES (1, 'persisted')")
    IO.puts("  Created table and inserted data")

    {:ok, rows} = QuackLake.query(conn, "SELECT * FROM test")
    IO.puts("  Query result: #{inspect(rows)}")

    # Cleanup
    File.rm(path)
    IO.puts("  Cleaned up #{path}")
  end

  defp demo_bang_variants do
    IO.puts("\n--- Bang Variants (raise on error) ---")

    conn = QuackLake.open!()
    IO.puts("  QuackLake.open!() - opened without error tuple")

    rows = QuackLake.query!(conn, "SELECT 42 AS answer")
    IO.puts("  QuackLake.query!() result: #{inspect(rows)}")

    row = QuackLake.query_one!(conn, "SELECT 'single' AS value")
    IO.puts("  QuackLake.query_one!() result: #{inspect(row)}")
  end

  defp demo_with_options do
    IO.puts("\n--- Connection Options ---")

    # With auto-extension disabled
    {:ok, _conn} =
      QuackLake.open(
        auto_install_extensions: false,
        auto_load_extensions: false
      )

    IO.puts("  Opened with auto_install_extensions: false, auto_load_extensions: false")

    # With explicit database path (Ecto compatibility)
    {:ok, conn} = QuackLake.open(database: ":memory:")
    IO.puts("  Opened with database: ':memory:' (Ecto-style)")

    {:ok, rows} = QuackLake.query(conn, "SELECT version() AS version")
    IO.puts("  DuckDB version: #{hd(rows)["version"]}")
  end
end
