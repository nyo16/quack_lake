defmodule QuackLake.Test.MinioHelper do
  @moduledoc """
  Helper functions for MinIO/S3 integration tests.

  Provides unique path generation and S3 secret configuration for tests.
  """

  alias QuackLake.Test.DockerHelper

  @doc """
  Generates a unique S3 path for test isolation.

  Uses test module name, test name, and timestamp to ensure uniqueness.

  ## Example

      iex> unique_s3_path(MyTest, "test_parquet_write")
      "s3://quacklake-test/tests/MyTest/test_parquet_write/1234567890"
  """
  def unique_s3_path(test_module, test_name) do
    config = DockerHelper.s3_config()
    bucket = config[:bucket]
    timestamp = System.system_time(:millisecond)
    module_name = test_module |> Module.split() |> List.last()

    "s3://#{bucket}/tests/#{module_name}/#{test_name}/#{timestamp}"
  end

  @doc """
  Generates a unique local path for DuckLake data.

  ## Example

      iex> unique_data_path(MyTest, "test_lifecycle")
      "/tmp/quacklake_test/MyTest/test_lifecycle/1234567890"
  """
  def unique_local_path(test_module, test_name) do
    timestamp = System.system_time(:millisecond)
    module_name = test_module |> Module.split() |> List.last()

    Path.join([System.tmp_dir!(), "quacklake_test", module_name, test_name, to_string(timestamp)])
  end

  @doc """
  Creates the S3 secret SQL for DuckDB.

  ## Example

      iex> create_s3_secret_sql(:my_secret)
      "CREATE SECRET my_secret (TYPE S3, KEY_ID '...', SECRET '...', ...)"
  """
  def create_s3_secret_sql(secret_name \\ :test_s3) do
    config = DockerHelper.s3_config()

    # Parse endpoint to extract host without protocol
    endpoint = config[:endpoint]
    endpoint_host = endpoint |> String.replace(~r{^https?://}, "")

    """
    CREATE SECRET #{secret_name} (
      TYPE S3,
      KEY_ID '#{config[:access_key_id]}',
      SECRET '#{config[:secret_access_key]}',
      REGION '#{config[:region]}',
      ENDPOINT '#{endpoint_host}',
      USE_SSL false,
      URL_STYLE 'path'
    )
    """
  end

  @doc """
  Creates an S3 secret via DuckDB connection.
  """
  def setup_s3_secret(conn, secret_name \\ :test_s3) do
    sql = create_s3_secret_sql(secret_name)

    case Duckdbex.query(conn, sql) do
      {:ok, _} -> :ok
      {:error, reason} -> {:error, {:secret_create_failed, reason}}
    end
  end

  @doc """
  Cleans up test files from S3 bucket.

  Note: This is a best-effort cleanup. Test paths use timestamps for isolation,
  so old paths can be cleaned up periodically rather than after each test.
  """
  def cleanup_s3_path(conn, s3_path) do
    # List and delete files - best effort
    list_sql = "SELECT * FROM glob('#{s3_path}/**')"

    case Duckdbex.query(conn, list_sql) do
      {:ok, ref} ->
        files = Duckdbex.fetch_all(ref)

        Enum.each(files, fn [_file_path] ->
          # DuckDB doesn't have direct S3 delete, cleanup via mc or lifecycle policies
          :ok
        end)

        :ok

      {:error, _} ->
        # Path might not exist, that's fine
        :ok
    end
  end

  @doc """
  Returns HTTPFS extension configuration for S3 access.
  """
  def httpfs_s3_config do
    config = DockerHelper.s3_config()
    endpoint_host = config[:endpoint] |> String.replace(~r{^https?://}, "")

    [
      "SET s3_access_key_id = '#{config[:access_key_id]}'",
      "SET s3_secret_access_key = '#{config[:secret_access_key]}'",
      "SET s3_region = '#{config[:region]}'",
      "SET s3_endpoint = '#{endpoint_host}'",
      "SET s3_use_ssl = false",
      "SET s3_url_style = 'path'"
    ]
  end

  @doc """
  Configures DuckDB connection for S3 access via SET commands.
  """
  def configure_s3_connection(conn) do
    httpfs_s3_config()
    |> Enum.reduce_while(:ok, fn sql, :ok ->
      case Duckdbex.query(conn, sql) do
        {:ok, _} -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end
end
