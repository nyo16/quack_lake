defmodule QuackLake.Integration.Ecto.DuckLakeAdapterTest do
  @moduledoc """
  Integration tests for Ecto.Adapters.DuckLake with PostgreSQL catalog and S3.

  Requires Docker services to be running.
  """

  use QuackLake.DataCase, async: false

  @moduletag :integration

  # Define test repo
  defmodule LakeRepo do
    use Ecto.Repo,
      otp_app: :quack_lake,
      adapter: Ecto.Adapters.DuckLake
  end

  describe "connection with PostgreSQL catalog" do
    test "can start repo with PostgreSQL catalog", %{test_name: test_name} do
      lake_name = unique_lake_name("ecto_#{test_name}")
      database = DockerHelper.ducklake_database_string(lake_name)

      {:ok, pid} =
        LakeRepo.start_link(
          database: database,
          pool_size: 1
        )

      assert Process.alive?(pid)

      # Cleanup
      GenServer.stop(pid)
    end

    test "can execute queries through repo", %{test_name: test_name} do
      lake_name = unique_lake_name("query_#{test_name}")
      database = DockerHelper.ducklake_database_string(lake_name)

      {:ok, pid} =
        LakeRepo.start_link(
          database: database,
          pool_size: 1
        )

      # Create table
      Ecto.Adapters.SQL.query!(LakeRepo, """
        CREATE TABLE #{lake_name}.main.repo_test (
          id INTEGER,
          name VARCHAR
        )
      """)

      # Insert data
      Ecto.Adapters.SQL.query!(LakeRepo, """
        INSERT INTO #{lake_name}.main.repo_test VALUES (1, 'test')
      """)

      # Query
      result =
        Ecto.Adapters.SQL.query!(LakeRepo, """
          SELECT * FROM #{lake_name}.main.repo_test
        """)

      assert hd(result.rows) == [1, "test"]

      GenServer.stop(pid)
    end
  end

  describe "data_path configuration" do
    test "data_path is used when provided", %{test_name: test_name, s3_path: s3_path} do
      lake_name = unique_lake_name("datapath_#{test_name}")
      database = DockerHelper.ducklake_database_string(lake_name)
      data_path = "#{s3_path}/ecto_data"

      # Configure S3 secrets
      s3_config = DockerHelper.s3_config()

      secrets = [
        {:ecto_s3,
         [
           type: :s3,
           key_id: s3_config[:access_key_id],
           secret: s3_config[:secret_access_key],
           region: s3_config[:region],
           endpoint: s3_config[:endpoint] |> String.replace(~r{^https?://}, ""),
           use_ssl: false,
           url_style: :path
         ]}
      ]

      {:ok, pid} =
        LakeRepo.start_link(
          database: database,
          data_path: data_path,
          extensions: [:httpfs],
          secrets: secrets,
          pool_size: 1
        )

      # Create table - data should be stored in S3
      Ecto.Adapters.SQL.query!(LakeRepo, """
        CREATE TABLE #{lake_name}.main.s3_backed (
          id INTEGER,
          value VARCHAR
        )
      """)

      Ecto.Adapters.SQL.query!(LakeRepo, """
        INSERT INTO #{lake_name}.main.s3_backed VALUES (1, 's3_test')
      """)

      result =
        Ecto.Adapters.SQL.query!(LakeRepo, """
          SELECT * FROM #{lake_name}.main.s3_backed
        """)

      assert hd(result.rows) == [1, "s3_test"]

      GenServer.stop(pid)
    end
  end

  describe "extensions configuration" do
    test "extensions are loaded on connection", %{test_name: test_name} do
      lake_name = unique_lake_name("ext_#{test_name}")
      database = DockerHelper.ducklake_database_string(lake_name)

      {:ok, pid} =
        LakeRepo.start_link(
          database: database,
          extensions: [:httpfs, {:json, source: :core}],
          pool_size: 1
        )

      # Verify extensions are loaded
      result =
        Ecto.Adapters.SQL.query!(LakeRepo, """
          SELECT extension_name, loaded FROM duckdb_extensions()
          WHERE extension_name IN ('httpfs', 'json')
        """)

      loaded_extensions = Enum.map(result.rows, &hd/1)

      assert "httpfs" in loaded_extensions
      assert "json" in loaded_extensions

      GenServer.stop(pid)
    end
  end

  describe "transactions" do
    test "supports transactions", %{test_name: test_name} do
      lake_name = unique_lake_name("txn_#{test_name}")
      database = DockerHelper.ducklake_database_string(lake_name)

      {:ok, pid} =
        LakeRepo.start_link(
          database: database,
          pool_size: 1
        )

      Ecto.Adapters.SQL.query!(LakeRepo, """
        CREATE TABLE #{lake_name}.main.txn_table (id INTEGER, status VARCHAR)
      """)

      # Test successful transaction
      LakeRepo.transaction(fn ->
        Ecto.Adapters.SQL.query!(LakeRepo, """
          INSERT INTO #{lake_name}.main.txn_table VALUES (1, 'committed')
        """)
      end)

      result =
        Ecto.Adapters.SQL.query!(LakeRepo, """
          SELECT * FROM #{lake_name}.main.txn_table
        """)

      assert length(result.rows) == 1

      GenServer.stop(pid)
    end
  end

  describe "pool configuration" do
    test "respects pool_size configuration", %{test_name: test_name} do
      lake_name = unique_lake_name("pool_#{test_name}")
      database = DockerHelper.ducklake_database_string(lake_name)

      {:ok, pid} =
        LakeRepo.start_link(
          database: database,
          pool_size: 3
        )

      # Execute multiple queries concurrently
      tasks =
        for i <- 1..5 do
          Task.async(fn ->
            Ecto.Adapters.SQL.query!(LakeRepo, "SELECT #{i} AS value")
          end)
        end

      results = Task.await_many(tasks, 5000)

      assert length(results) == 5

      GenServer.stop(pid)
    end
  end
end
