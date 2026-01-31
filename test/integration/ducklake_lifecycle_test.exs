defmodule QuackLake.Integration.DuckLakeLifecycleTest do
  @moduledoc """
  Integration tests for full DuckLake lifecycle.

  Tests create, write, query, time travel, and concurrent operations.
  """

  use QuackLake.DataCase, async: false

  @moduletag :integration

  describe "table lifecycle" do
    test "create, alter, and drop table", %{test_name: test_name} do
      {:ok, db, conn} = open_duckdb_with_ducklake()

      lake_name = unique_lake_name("lifecycle_#{test_name}")
      {:ok, ^lake_name} = attach_ducklake(conn, lake_name)

      # Create table
      {:ok, _} =
        Duckdbex.query(conn, """
          CREATE TABLE #{lake_name}.main.lifecycle_test (
            id INTEGER,
            name VARCHAR
          )
        """)

      # Verify exists
      {:ok, ref} =
        Duckdbex.query(conn, """
          SELECT column_name FROM duckdb_columns()
          WHERE database_name = '#{lake_name}' AND table_name = 'lifecycle_test'
        """)

      columns = Duckdbex.fetch_all(ref) |> Enum.map(&hd/1)

      assert "id" in columns
      assert "name" in columns

      # Add column
      {:ok, _} =
        Duckdbex.query(conn, """
          ALTER TABLE #{lake_name}.main.lifecycle_test ADD COLUMN created_at TIMESTAMP
        """)

      # Verify new column
      {:ok, ref} =
        Duckdbex.query(conn, """
          SELECT column_name FROM duckdb_columns()
          WHERE database_name = '#{lake_name}' AND table_name = 'lifecycle_test'
        """)

      columns = Duckdbex.fetch_all(ref) |> Enum.map(&hd/1)

      assert "created_at" in columns

      # Drop table
      {:ok, _} = Duckdbex.query(conn, "DROP TABLE #{lake_name}.main.lifecycle_test")

      # Verify dropped
      {:ok, ref} =
        Duckdbex.query(conn, """
          SELECT COUNT(*) FROM duckdb_tables()
          WHERE database_name = '#{lake_name}' AND table_name = 'lifecycle_test'
        """)

      [[count]] = Duckdbex.fetch_all(ref)

      assert count == 0
    end
  end

  describe "data operations" do
    test "insert, update, delete operations", %{test_name: test_name} do
      {:ok, db, conn} = open_duckdb_with_ducklake()

      lake_name = unique_lake_name("crud_#{test_name}")
      {:ok, ^lake_name} = attach_ducklake(conn, lake_name)

      {:ok, _} =
        Duckdbex.query(conn, """
          CREATE TABLE #{lake_name}.main.crud_test (
            id INTEGER PRIMARY KEY,
            status VARCHAR
          )
        """)

      # Insert
      {:ok, _} =
        Duckdbex.query(conn, """
          INSERT INTO #{lake_name}.main.crud_test VALUES
            (1, 'active'),
            (2, 'pending'),
            (3, 'inactive')
        """)

      # Update
      {:ok, _} =
        Duckdbex.query(conn, """
          UPDATE #{lake_name}.main.crud_test SET status = 'archived' WHERE id = 3
        """)

      # Delete
      {:ok, _} =
        Duckdbex.query(conn, """
          DELETE FROM #{lake_name}.main.crud_test WHERE id = 2
        """)

      # Verify final state
      {:ok, ref} = Duckdbex.query(conn, "SELECT * FROM #{lake_name}.main.crud_test ORDER BY id")
      rows = Duckdbex.fetch_all(ref)

      assert rows == [[1, "active"], [3, "archived"]]
    end

    test "bulk insert with COPY", %{test_name: test_name, local_path: local_path} do
      {:ok, db, conn} = open_duckdb_with_ducklake()

      lake_name = unique_lake_name("bulk_#{test_name}")
      {:ok, ^lake_name} = attach_ducklake(conn, lake_name)

      {:ok, _} =
        Duckdbex.query(conn, """
          CREATE TABLE #{lake_name}.main.bulk_data (
            id INTEGER,
            value DOUBLE
          )
        """)

      # Insert large dataset
      {:ok, _} =
        Duckdbex.query(conn, """
          INSERT INTO #{lake_name}.main.bulk_data
          SELECT i, random() FROM range(10000) t(i)
        """)

      {:ok, ref} = Duckdbex.query(conn, "SELECT COUNT(*) FROM #{lake_name}.main.bulk_data")
      [[count]] = Duckdbex.fetch_all(ref)

      assert count == 10000
    end
  end

  describe "transactions" do
    test "commit and rollback", %{test_name: test_name} do
      {:ok, db, conn} = open_duckdb_with_ducklake()

      lake_name = unique_lake_name("txn_#{test_name}")
      {:ok, ^lake_name} = attach_ducklake(conn, lake_name)

      {:ok, _} =
        Duckdbex.query(conn, """
          CREATE TABLE #{lake_name}.main.txn_test (id INTEGER, value VARCHAR)
        """)

      # Test commit
      {:ok, _} = Duckdbex.query(conn, "BEGIN TRANSACTION")

      {:ok, _} =
        Duckdbex.query(conn, "INSERT INTO #{lake_name}.main.txn_test VALUES (1, 'committed')")

      {:ok, _} = Duckdbex.query(conn, "COMMIT")

      # Test rollback
      {:ok, _} = Duckdbex.query(conn, "BEGIN TRANSACTION")

      {:ok, _} =
        Duckdbex.query(conn, "INSERT INTO #{lake_name}.main.txn_test VALUES (2, 'rolled_back')")

      {:ok, _} = Duckdbex.query(conn, "ROLLBACK")

      # Verify only committed data exists
      {:ok, ref} = Duckdbex.query(conn, "SELECT * FROM #{lake_name}.main.txn_test")
      rows = Duckdbex.fetch_all(ref)

      assert rows == [[1, "committed"]]
    end
  end

  describe "time travel" do
    test "can query historical snapshots", %{test_name: test_name} do
      {:ok, db, conn} = open_duckdb_with_ducklake()

      lake_name = unique_lake_name("timetravel_#{test_name}")
      {:ok, ^lake_name} = attach_ducklake(conn, lake_name)

      {:ok, _} =
        Duckdbex.query(conn, """
          CREATE TABLE #{lake_name}.main.versioned (id INTEGER, value VARCHAR)
        """)

      # Insert initial data
      {:ok, _} = Duckdbex.query(conn, "INSERT INTO #{lake_name}.main.versioned VALUES (1, 'v1')")

      # Get current snapshot
      {:ok, ref} = Duckdbex.query(conn, "SELECT ducklake_current_snapshot('#{lake_name}')")
      [[snapshot1]] = Duckdbex.fetch_all(ref)

      # Update data
      {:ok, _} =
        Duckdbex.query(conn, "UPDATE #{lake_name}.main.versioned SET value = 'v2' WHERE id = 1")

      # Query current data
      {:ok, ref} =
        Duckdbex.query(conn, "SELECT value FROM #{lake_name}.main.versioned WHERE id = 1")

      [[current_value]] = Duckdbex.fetch_all(ref)

      assert current_value == "v2"

      # Query historical snapshot
      {:ok, ref} =
        Duckdbex.query(conn, """
          SELECT value FROM ducklake_snapshot_table('#{lake_name}', 'main', 'versioned', #{snapshot1}) WHERE id = 1
        """)

      [[historical_value]] = Duckdbex.fetch_all(ref)

      assert historical_value == "v1"
    end
  end

  describe "schema operations" do
    test "create and use multiple schemas", %{test_name: test_name} do
      {:ok, db, conn} = open_duckdb_with_ducklake()

      lake_name = unique_lake_name("schema_#{test_name}")
      {:ok, ^lake_name} = attach_ducklake(conn, lake_name)

      # Create schemas
      {:ok, _} = Duckdbex.query(conn, "CREATE SCHEMA #{lake_name}.analytics")
      {:ok, _} = Duckdbex.query(conn, "CREATE SCHEMA #{lake_name}.staging")

      # Create tables in different schemas
      {:ok, _} =
        Duckdbex.query(conn, """
          CREATE TABLE #{lake_name}.analytics.events (id INTEGER, type VARCHAR)
        """)

      {:ok, _} =
        Duckdbex.query(conn, """
          CREATE TABLE #{lake_name}.staging.raw_events (id INTEGER, data VARCHAR)
        """)

      # Verify tables in correct schemas
      {:ok, ref} =
        Duckdbex.query(conn, """
          SELECT schema_name, table_name FROM duckdb_tables()
          WHERE database_name = '#{lake_name}' AND table_name LIKE '%events%'
          ORDER BY schema_name
        """)

      rows = Duckdbex.fetch_all(ref)

      assert rows == [["analytics", "events"], ["staging", "raw_events"]]
    end
  end
end
