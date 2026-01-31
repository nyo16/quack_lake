defmodule QuackLake.DataCase do
  @moduledoc """
  ExUnit case template for tests requiring Docker services (PostgreSQL, MinIO).

  ## Usage

      defmodule MyIntegrationTest do
        use QuackLake.DataCase

        test "writes to S3" do
          # Docker services are available
        end
      end

  Tests using this case will be skipped unless `INTEGRATION=true` is set.
  """

  use ExUnit.CaseTemplate

  alias QuackLake.Test.{DockerHelper, MinioHelper}

  using do
    quote do
      import QuackLake.DataCase
      alias QuackLake.Test.{DockerHelper, MinioHelper}
    end
  end

  setup_all do
    unless integration_tests_enabled?() do
      raise ExUnit.AssertionError,
        message: """
        Integration tests are disabled.
        Run with INTEGRATION=true to enable:

            INTEGRATION=true mix test test/integration
        """
    end

    case DockerHelper.ensure_services_ready() do
      :ok ->
        :ok

      {:error, reason} ->
        raise ExUnit.AssertionError,
          message: """
          Docker services are not ready: #{inspect(reason)}

          Make sure to start Docker services first:

              docker-compose up -d
          """
    end

    :ok
  end

  setup context do
    # Generate unique paths for test isolation
    test_name = Atom.to_string(context[:test])
    test_module = context[:module]

    s3_path = MinioHelper.unique_s3_path(test_module, test_name)
    local_path = MinioHelper.unique_local_path(test_module, test_name)

    # Create local directory if needed
    File.mkdir_p!(local_path)

    on_exit(fn ->
      # Cleanup local test files
      File.rm_rf(local_path)
    end)

    {:ok, s3_path: s3_path, local_path: local_path, test_name: test_name}
  end

  @doc """
  Checks if integration tests are enabled.
  """
  def integration_tests_enabled? do
    System.get_env("INTEGRATION") == "true"
  end

  @doc """
  Opens a DuckDB connection with S3 configured.
  """
  def open_duckdb_with_s3 do
    {:ok, db} = Duckdbex.open()
    {:ok, conn} = Duckdbex.connection(db)

    # Install and load httpfs
    {:ok, _} = Duckdbex.query(conn, "INSTALL httpfs")
    {:ok, _} = Duckdbex.query(conn, "LOAD httpfs")

    # Configure S3
    :ok = MinioHelper.configure_s3_connection(conn)

    {:ok, db, conn}
  end

  @doc """
  Opens a DuckDB connection with DuckLake extension loaded.
  """
  def open_duckdb_with_ducklake do
    {:ok, db} = Duckdbex.open()
    {:ok, conn} = Duckdbex.connection(db)

    # Install and load ducklake
    {:ok, _} = Duckdbex.query(conn, "INSTALL ducklake FROM core")
    {:ok, _} = Duckdbex.query(conn, "LOAD ducklake")

    {:ok, db, conn}
  end

  @doc """
  Opens a DuckDB connection with both httpfs and ducklake extensions.
  """
  def open_duckdb_full do
    {:ok, db} = Duckdbex.open()
    {:ok, conn} = Duckdbex.connection(db)

    # Install and load extensions
    {:ok, _} = Duckdbex.query(conn, "INSTALL httpfs")
    {:ok, _} = Duckdbex.query(conn, "LOAD httpfs")
    {:ok, _} = Duckdbex.query(conn, "INSTALL ducklake FROM core")
    {:ok, _} = Duckdbex.query(conn, "LOAD ducklake")

    # Configure S3
    :ok = MinioHelper.configure_s3_connection(conn)

    {:ok, db, conn}
  end

  @doc """
  Creates a unique lake name for test isolation.
  """
  def unique_lake_name(prefix \\ "test_lake") do
    timestamp = System.system_time(:millisecond)
    "#{prefix}_#{timestamp}"
  end

  @doc """
  Attaches a DuckLake with PostgreSQL catalog.
  """
  def attach_ducklake(conn, lake_name, opts \\ []) do
    catalog_string = DockerHelper.postgres_catalog_string()
    data_path = Keyword.get(opts, :data_path)

    sql =
      if data_path do
        "ATTACH '#{catalog_string};ducklake_alias=#{lake_name}' AS #{lake_name} (TYPE DUCKLAKE, DATA_PATH '#{data_path}')"
      else
        "ATTACH '#{catalog_string};ducklake_alias=#{lake_name}' AS #{lake_name} (TYPE DUCKLAKE)"
      end

    case Duckdbex.query(conn, sql) do
      {:ok, _} -> {:ok, lake_name}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Detaches a DuckLake.
  """
  def detach_ducklake(conn, lake_name) do
    case Duckdbex.query(conn, "DETACH #{lake_name}") do
      {:ok, _} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end
end
