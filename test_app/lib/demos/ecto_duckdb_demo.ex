defmodule TestApp.Demos.EctoDuckDBDemo do
  @moduledoc """
  Demonstrates Ecto.Adapters.DuckDB (single writer mode).
  """

  alias TestApp.Repo

  def run do
    IO.puts("\n" <> String.duplicate("=", 60))
    IO.puts("ECTO DUCKDB ADAPTER DEMO")
    IO.puts(String.duplicate("=", 60))

    setup_tables()
    demo_crud()
    demo_transactions()
    demo_raw_query()
    cleanup()

    IO.puts("\n✓ Ecto DuckDB demo complete!\n")
  end

  defp setup_tables do
    IO.puts("\n--- Setting up tables ---")

    # Drop existing tables first to ensure clean state
    Ecto.Adapters.SQL.query!(Repo, "DROP TABLE IF EXISTS users")
    Ecto.Adapters.SQL.query!(Repo, "DROP TABLE IF EXISTS products")

    Ecto.Adapters.SQL.query!(Repo, """
      CREATE TABLE users (
        id INTEGER PRIMARY KEY,
        name TEXT,
        email TEXT,
        active BOOLEAN DEFAULT true
      )
    """)

    IO.puts("  Created users table")

    Ecto.Adapters.SQL.query!(Repo, """
      CREATE TABLE IF NOT EXISTS products (
        id INTEGER PRIMARY KEY,
        name TEXT,
        sku TEXT,
        price DECIMAL(10, 2),
        quantity INTEGER
      )
    """)

    IO.puts("  Created products table")
  end

  defp demo_crud do
    IO.puts("\n--- CRUD Operations ---")

    # Create (INSERT)
    Ecto.Adapters.SQL.query!(Repo, """
      INSERT INTO users (id, name, email, active) VALUES
        (1, 'Alice', 'alice@example.com', true),
        (2, 'Bob', 'bob@example.com', true),
        (3, 'Carol', 'carol@example.com', false)
    """)

    IO.puts("  INSERT: Added 3 users")

    # Read (SELECT)
    result = Ecto.Adapters.SQL.query!(Repo, "SELECT * FROM users ORDER BY id")
    IO.puts("  SELECT: Found #{length(result.rows)} users")

    for row <- result.rows do
      [id, name, email, active] = row
      IO.puts("    #{id}: #{name} (#{email}) - active: #{active}")
    end

    # Update
    Ecto.Adapters.SQL.query!(Repo, "UPDATE users SET active = false WHERE id = 2")
    IO.puts("  UPDATE: Set Bob to inactive")

    # Delete
    Ecto.Adapters.SQL.query!(Repo, "DELETE FROM users WHERE id = 3")
    IO.puts("  DELETE: Removed Carol")

    # Verify
    result = Ecto.Adapters.SQL.query!(Repo, "SELECT COUNT(*) FROM users")
    [[count]] = result.rows
    IO.puts("  Final user count: #{count}")
  end

  defp demo_transactions do
    IO.puts("\n--- Transactions ---")

    # Successful transaction
    Repo.transaction(fn ->
      Ecto.Adapters.SQL.query!(Repo, """
        INSERT INTO users (id, name, email) VALUES (10, 'Transaction User', 'tx@example.com')
      """)
    end)

    IO.puts("  Committed transaction: Added 'Transaction User'")

    # Rollback transaction
    try do
      Repo.transaction(fn ->
        Ecto.Adapters.SQL.query!(Repo, """
          INSERT INTO users (id, name, email) VALUES (11, 'Rollback User', 'rb@example.com')
        """)

        Repo.rollback(:intentional_rollback)
      end)
    catch
      :error, _ -> :ok
    end

    IO.puts("  Rolled back transaction: 'Rollback User' not added")

    # Verify
    result =
      Ecto.Adapters.SQL.query!(Repo, "SELECT name FROM users WHERE id IN (10, 11) ORDER BY id")

    names = Enum.map(result.rows, &hd/1)
    IO.puts("  Users after transactions: #{inspect(names)}")
  end

  defp demo_raw_query do
    IO.puts("\n--- RawQuery Module ---")

    # exec! for raw SQL
    result = Repo.exec!("SELECT 'Hello from RawQuery!' AS message")
    IO.puts("  Repo.exec! result: #{inspect(result.rows)}")

    # Sequential statements (DuckDB requires one statement per query)
    Repo.exec!("CREATE TABLE IF NOT EXISTS temp_raw (id INTEGER)")
    Repo.exec!("INSERT INTO temp_raw VALUES (1), (2), (3)")
    IO.puts("  Created temp_raw and inserted 3 rows")

    result = Repo.exec!("SELECT COUNT(*) FROM temp_raw")
    [[count]] = result.rows
    IO.puts("  SELECT COUNT(*) result: #{count} rows in temp_raw")

    Repo.exec!("DROP TABLE temp_raw")
  end

  defp cleanup do
    IO.puts("\n--- Cleanup ---")
    Ecto.Adapters.SQL.query!(Repo, "DROP TABLE IF EXISTS users")
    Ecto.Adapters.SQL.query!(Repo, "DROP TABLE IF EXISTS products")
    IO.puts("  Dropped tables")
  end
end
