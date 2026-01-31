defmodule QuackLake.Integration.MinioS3Test do
  @moduledoc """
  Integration tests for MinIO/S3-compatible storage.

  Tests S3 secrets, Parquet read/write, and DuckLake with S3 data storage.
  """

  use QuackLake.DataCase, async: false

  @moduletag :integration

  describe "S3 secrets" do
    test "can create S3 secret", %{local_path: _local_path} do
      {:ok, db, conn} = open_duckdb_with_s3()

      # Secret was already configured via SET commands in open_duckdb_with_s3
      # Test that we can also create a named secret
      sql = MinioHelper.create_s3_secret_sql(:test_secret)
      {:ok, _} = Duckdbex.query(conn, sql)

      # Verify secret exists
      {:ok, ref} =
        Duckdbex.query(conn, "SELECT name FROM duckdb_secrets() WHERE name = 'test_secret'")

      rows = Duckdbex.fetch_all(ref)

      assert length(rows) == 1
    end
  end

  describe "Parquet operations" do
    test "can write Parquet to S3", %{s3_path: s3_path} do
      {:ok, db, conn} = open_duckdb_with_s3()

      parquet_path = "#{s3_path}/test.parquet"

      # Create data and write to S3
      {:ok, _} =
        Duckdbex.query(conn, """
          COPY (SELECT i AS id, 'item_' || i AS name FROM range(100) t(i))
          TO '#{parquet_path}' (FORMAT PARQUET)
        """)

      # Verify file exists by reading it back
      {:ok, ref} = Duckdbex.query(conn, "SELECT COUNT(*) FROM '#{parquet_path}'")
      [[count]] = Duckdbex.fetch_all(ref)

      assert count == 100
    end

    test "can read Parquet from S3", %{s3_path: s3_path} do
      {:ok, db, conn} = open_duckdb_with_s3()

      parquet_path = "#{s3_path}/read_test.parquet"

      # Write test data
      {:ok, _} =
        Duckdbex.query(conn, """
          COPY (
            SELECT
              i AS id,
              'product_' || i AS name,
              (i * 1.5)::DECIMAL(10,2) AS price
            FROM range(50) t(i)
          )
          TO '#{parquet_path}' (FORMAT PARQUET)
        """)

      # Read and verify
      {:ok, ref} =
        Duckdbex.query(conn, "SELECT * FROM '#{parquet_path}' WHERE id < 10 ORDER BY id")

      rows = Duckdbex.fetch_all(ref)

      assert length(rows) == 10
      assert hd(rows) == [0, "product_0", Decimal.new("0.00")]
    end

    test "can query multiple Parquet files with glob", %{s3_path: s3_path} do
      {:ok, db, conn} = open_duckdb_with_s3()

      # Write multiple parquet files
      for i <- 1..3 do
        {:ok, _} =
          Duckdbex.query(conn, """
            COPY (SELECT #{i} AS batch, j AS id FROM range(10) t(j))
            TO '#{s3_path}/batch_#{i}.parquet' (FORMAT PARQUET)
          """)
      end

      # Query all files with glob
      {:ok, ref} = Duckdbex.query(conn, "SELECT COUNT(*) FROM '#{s3_path}/batch_*.parquet'")
      [[count]] = Duckdbex.fetch_all(ref)

      assert count == 30
    end
  end

  describe "DuckLake with S3 data storage" do
    test "can create DuckLake with S3 data_path", %{s3_path: s3_path, test_name: test_name} do
      {:ok, db, conn} = open_duckdb_full()

      lake_name = unique_lake_name("s3_#{test_name}")
      data_path = "#{s3_path}/lake_data"

      {:ok, ^lake_name} = attach_ducklake(conn, lake_name, data_path: data_path)

      # Create table
      {:ok, _} =
        Duckdbex.query(conn, """
          CREATE TABLE #{lake_name}.main.s3_table (
            id INTEGER,
            value VARCHAR
          )
        """)

      {:ok, _} =
        Duckdbex.query(conn, """
          INSERT INTO #{lake_name}.main.s3_table VALUES (1, 'test'), (2, 'data')
        """)

      # Query data
      {:ok, ref} = Duckdbex.query(conn, "SELECT * FROM #{lake_name}.main.s3_table ORDER BY id")
      rows = Duckdbex.fetch_all(ref)

      assert rows == [[1, "test"], [2, "data"]]
    end

    test "persists data across connections with S3 storage", %{
      s3_path: s3_path,
      test_name: test_name
    } do
      lake_name = unique_lake_name("persist_#{test_name}")
      data_path = "#{s3_path}/persist_data"

      # First connection: create and populate
      {:ok, db1, conn1} = open_duckdb_full()
      {:ok, ^lake_name} = attach_ducklake(conn1, lake_name, data_path: data_path)

      {:ok, _} =
        Duckdbex.query(conn1, """
          CREATE TABLE #{lake_name}.main.persistent (id INTEGER, name VARCHAR)
        """)

      {:ok, _} =
        Duckdbex.query(conn1, """
          INSERT INTO #{lake_name}.main.persistent VALUES (1, 'alice'), (2, 'bob')
        """)

      # Second connection: verify data persists
      {:ok, db2, conn2} = open_duckdb_full()
      {:ok, ^lake_name} = attach_ducklake(conn2, lake_name, data_path: data_path)

      {:ok, ref} = Duckdbex.query(conn2, "SELECT COUNT(*) FROM #{lake_name}.main.persistent")
      [[count]] = Duckdbex.fetch_all(ref)

      assert count == 2
    end
  end
end
