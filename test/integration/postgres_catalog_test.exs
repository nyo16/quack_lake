defmodule QuackLake.Integration.PostgresCatalogTest do
  @moduledoc """
  Integration tests for PostgreSQL as DuckLake metadata catalog.

  Tests the postgres_scanner extension and DuckLake's PostgreSQL catalog backend.
  """

  use QuackLake.DataCase, async: false

  @moduletag :integration

  describe "postgres_scanner extension" do
    test "can attach PostgreSQL database", %{local_path: _local_path} do
      {:ok, db, conn} = open_duckdb_with_ducklake()

      # Install and load postgres_scanner
      {:ok, _} = Duckdbex.query(conn, "INSTALL postgres_scanner")
      {:ok, _} = Duckdbex.query(conn, "LOAD postgres_scanner")

      # Attach PostgreSQL
      catalog = DockerHelper.postgres_catalog_string()
      {:ok, _} = Duckdbex.query(conn, "ATTACH '#{catalog}' AS pg (TYPE POSTGRES)")

      # Verify attachment
      {:ok, ref} =
        Duckdbex.query(
          conn,
          "SELECT database_name FROM duckdb_databases() WHERE database_name = 'pg'"
        )

      rows = Duckdbex.fetch_all(ref)

      assert length(rows) == 1
      assert hd(rows) == ["pg"]
    end

    test "can query PostgreSQL information_schema", %{local_path: _local_path} do
      {:ok, db, conn} = open_duckdb_with_ducklake()

      {:ok, _} = Duckdbex.query(conn, "INSTALL postgres_scanner")
      {:ok, _} = Duckdbex.query(conn, "LOAD postgres_scanner")

      catalog = DockerHelper.postgres_catalog_string()
      {:ok, _} = Duckdbex.query(conn, "ATTACH '#{catalog}' AS pg (TYPE POSTGRES)")

      # Query information_schema
      {:ok, ref} =
        Duckdbex.query(conn, "SELECT table_name FROM pg.information_schema.tables LIMIT 5")

      rows = Duckdbex.fetch_all(ref)

      assert is_list(rows)
    end
  end

  describe "DuckLake with PostgreSQL catalog" do
    test "can attach DuckLake with PostgreSQL catalog", %{test_name: test_name} do
      {:ok, db, conn} = open_duckdb_with_ducklake()

      lake_name = unique_lake_name("pg_catalog_#{test_name}")
      {:ok, ^lake_name} = attach_ducklake(conn, lake_name)

      # Verify lake is attached
      {:ok, ref} =
        Duckdbex.query(
          conn,
          "SELECT database_name FROM duckdb_databases() WHERE database_name = '#{lake_name}'"
        )

      rows = Duckdbex.fetch_all(ref)

      assert length(rows) == 1
    end

    test "can create table in DuckLake", %{test_name: test_name} do
      {:ok, db, conn} = open_duckdb_with_ducklake()

      lake_name = unique_lake_name("table_#{test_name}")
      {:ok, ^lake_name} = attach_ducklake(conn, lake_name)

      # Create a table
      {:ok, _} =
        Duckdbex.query(conn, """
          CREATE TABLE #{lake_name}.main.users (
            id INTEGER PRIMARY KEY,
            name VARCHAR,
            email VARCHAR
          )
        """)

      # Verify table exists
      {:ok, ref} =
        Duckdbex.query(
          conn,
          "SELECT table_name FROM duckdb_tables() WHERE database_name = '#{lake_name}'"
        )

      rows = Duckdbex.fetch_all(ref)

      assert Enum.any?(rows, fn [name] -> name == "users" end)
    end

    test "can insert and query data", %{test_name: test_name} do
      {:ok, db, conn} = open_duckdb_with_ducklake()

      lake_name = unique_lake_name("data_#{test_name}")
      {:ok, ^lake_name} = attach_ducklake(conn, lake_name)

      # Create and populate table
      {:ok, _} =
        Duckdbex.query(conn, """
          CREATE TABLE #{lake_name}.main.products (
            id INTEGER,
            name VARCHAR,
            price DECIMAL(10, 2)
          )
        """)

      {:ok, _} =
        Duckdbex.query(conn, """
          INSERT INTO #{lake_name}.main.products VALUES
            (1, 'Widget', 9.99),
            (2, 'Gadget', 19.99),
            (3, 'Gizmo', 29.99)
        """)

      # Query data
      {:ok, ref} = Duckdbex.query(conn, "SELECT COUNT(*) FROM #{lake_name}.main.products")
      [[count]] = Duckdbex.fetch_all(ref)

      assert count == 3
    end

    test "can detach and reattach lake", %{test_name: test_name} do
      {:ok, db, conn} = open_duckdb_with_ducklake()

      lake_name = unique_lake_name("reattach_#{test_name}")
      {:ok, ^lake_name} = attach_ducklake(conn, lake_name)

      # Create table with data
      {:ok, _} =
        Duckdbex.query(conn, """
          CREATE TABLE #{lake_name}.main.events (
            id INTEGER,
            event_type VARCHAR
          )
        """)

      {:ok, _} =
        Duckdbex.query(
          conn,
          "INSERT INTO #{lake_name}.main.events VALUES (1, 'click'), (2, 'view')"
        )

      # Detach
      :ok = detach_ducklake(conn, lake_name)

      # Reattach
      {:ok, ^lake_name} = attach_ducklake(conn, lake_name)

      # Data should persist
      {:ok, ref} = Duckdbex.query(conn, "SELECT COUNT(*) FROM #{lake_name}.main.events")
      [[count]] = Duckdbex.fetch_all(ref)

      assert count == 2
    end
  end
end
