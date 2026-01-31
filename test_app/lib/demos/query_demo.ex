defmodule TestApp.Demos.QueryDemo do
  @moduledoc """
  Demonstrates QuackLake query patterns.
  """

  def run do
    IO.puts("\n" <> String.duplicate("=", 60))
    IO.puts("QUERY PATTERNS DEMO")
    IO.puts(String.duplicate("=", 60))

    {:ok, conn} = QuackLake.open()

    setup_test_data(conn)
    demo_query(conn)
    demo_query_one(conn)
    demo_parameterized(conn)
    demo_execute(conn)
    demo_streaming(conn)

    IO.puts("\n✓ Query demo complete!\n")
  end

  defp setup_test_data(conn) do
    IO.puts("\n--- Setting up test data ---")

    :ok =
      QuackLake.Query.execute(conn, """
        CREATE TABLE demo_users (
          id INTEGER PRIMARY KEY,
          name TEXT,
          email TEXT,
          age INTEGER
        )
      """)

    :ok =
      QuackLake.Query.execute(conn, """
        INSERT INTO demo_users VALUES
          (1, 'Alice', 'alice@example.com', 30),
          (2, 'Bob', 'bob@example.com', 25),
          (3, 'Carol', 'carol@example.com', 35),
          (4, 'David', 'david@example.com', 28),
          (5, 'Eve', 'eve@example.com', 32)
      """)

    IO.puts("  Created demo_users table with 5 rows")
  end

  defp demo_query(conn) do
    IO.puts("\n--- QuackLake.query/2 - Returns list of maps ---")

    {:ok, rows} = QuackLake.query(conn, "SELECT * FROM demo_users ORDER BY id LIMIT 3")
    IO.puts("  Found #{length(rows)} rows:")

    for row <- rows do
      IO.puts("    #{row["id"]}: #{row["name"]} (#{row["email"]})")
    end
  end

  defp demo_query_one(conn) do
    IO.puts("\n--- QuackLake.query_one/2 - Returns single row or nil ---")

    {:ok, user} = QuackLake.query_one(conn, "SELECT * FROM demo_users WHERE id = 1")
    IO.puts("  Found user: #{user["name"]}")

    {:ok, nil_result} = QuackLake.query_one(conn, "SELECT * FROM demo_users WHERE id = 999")
    IO.puts("  Non-existent user: #{inspect(nil_result)}")
  end

  defp demo_parameterized(conn) do
    IO.puts("\n--- Parameterized Queries ---")

    {:ok, rows} = QuackLake.query(conn, "SELECT * FROM demo_users WHERE age > $1", [30])
    IO.puts("  Users older than 30: #{length(rows)}")

    {:ok, rows} =
      QuackLake.query(conn, "SELECT * FROM demo_users WHERE name = $1 AND age = $2", [
        "Alice",
        30
      ])

    IO.puts("  Found Alice age 30: #{length(rows) == 1}")

    {:ok, rows} =
      QuackLake.query(conn, "SELECT * FROM demo_users WHERE email LIKE $1", ["%@example.com"])

    IO.puts("  Users with @example.com: #{length(rows)}")
  end

  defp demo_execute(conn) do
    IO.puts("\n--- QuackLake.Query.execute/3 - DDL and DML ---")

    :ok =
      QuackLake.Query.execute(conn, """
        CREATE TABLE temp_table (id INTEGER, value TEXT)
      """)

    IO.puts("  Created temp_table")

    :ok = QuackLake.Query.execute(conn, "INSERT INTO temp_table VALUES ($1, $2)", [1, "test"])
    IO.puts("  Inserted row with parameters")

    :ok = QuackLake.Query.execute(conn, "UPDATE temp_table SET value = 'updated' WHERE id = 1")
    IO.puts("  Updated row")

    :ok = QuackLake.Query.execute(conn, "DELETE FROM temp_table WHERE id = 1")
    IO.puts("  Deleted row")

    :ok = QuackLake.Query.execute(conn, "DROP TABLE temp_table")
    IO.puts("  Dropped temp_table")
  end

  defp demo_streaming(conn) do
    IO.puts("\n--- QuackLake.Query.stream/2 - Streaming large results ---")

    # Create a larger dataset
    :ok =
      QuackLake.Query.execute(conn, """
        CREATE TABLE large_data AS
        SELECT i AS id, 'row_' || i AS value
        FROM range(1000) t(i)
      """)

    IO.puts("  Created large_data with 1000 rows")

    # Stream and count
    count =
      QuackLake.Query.stream(conn, "SELECT * FROM large_data")
      |> Enum.reduce(0, fn _chunk, acc -> acc + 1 end)

    IO.puts("  Streamed #{count} chunks")

    # Stream and process first few
    first_values =
      QuackLake.Query.stream(conn, "SELECT * FROM large_data ORDER BY id")
      |> Enum.take(3)
      |> Enum.flat_map(& &1)
      |> Enum.map(& &1["id"])

    IO.puts("  First IDs from stream: #{inspect(first_values)}")

    :ok = QuackLake.Query.execute(conn, "DROP TABLE large_data")
  end
end
