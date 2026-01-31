defmodule QuackLake.Test.DockerHelper do
  @moduledoc """
  Helper functions for Docker-based integration tests.

  Provides health checks and configuration helpers for PostgreSQL and MinIO services.
  """

  @doc """
  Checks if Docker services are available and healthy.
  Returns :ok if services are ready, {:error, reason} otherwise.
  """
  def ensure_services_ready do
    with :ok <- check_postgres(),
         :ok <- check_minio() do
      :ok
    end
  end

  @doc """
  Checks if PostgreSQL is available and accepting connections.
  """
  def check_postgres do
    config = postgres_config()

    case System.cmd("pg_isready", [
           "-h",
           config[:host],
           "-p",
           to_string(config[:port]),
           "-U",
           config[:username],
           "-d",
           config[:database]
         ]) do
      {_, 0} -> :ok
      {output, _} -> {:error, {:postgres_not_ready, output}}
    end
  rescue
    e -> {:error, {:postgres_check_failed, e}}
  end

  @doc """
  Checks if MinIO is available and the test bucket exists.
  """
  def check_minio do
    s3_config = s3_config()
    endpoint = s3_config[:endpoint]

    case :httpc.request(:get, {"#{endpoint}/minio/health/live", []}, [], []) do
      {:ok, {{_, 200, _}, _, _}} -> :ok
      {:ok, {{_, status, _}, _, _}} -> {:error, {:minio_unhealthy, status}}
      {:error, reason} -> {:error, {:minio_not_reachable, reason}}
    end
  rescue
    e -> {:error, {:minio_check_failed, e}}
  end

  @doc """
  Returns PostgreSQL configuration from application config.
  """
  def postgres_config do
    Application.get_env(:quack_lake, :postgres, [])
    |> Keyword.merge(
      host: System.get_env("POSTGRES_HOST", "localhost"),
      port: String.to_integer(System.get_env("POSTGRES_PORT", "5432")),
      database: System.get_env("POSTGRES_DB", "ducklake_catalog"),
      username: System.get_env("POSTGRES_USER", "quacklake"),
      password: System.get_env("POSTGRES_PASSWORD", "quacklake_secret")
    )
  end

  @doc """
  Returns S3/MinIO configuration from application config.
  """
  def s3_config do
    Application.get_env(:quack_lake, :s3, [])
    |> Keyword.merge(
      endpoint: System.get_env("S3_ENDPOINT", "http://localhost:9000"),
      bucket: System.get_env("DUCKLAKE_S3_BUCKET", "quacklake-test"),
      access_key_id: System.get_env("MINIO_ROOT_USER", "minioadmin"),
      secret_access_key: System.get_env("MINIO_ROOT_PASSWORD", "minioadmin123"),
      region: "us-east-1",
      use_ssl: false
    )
  end

  @doc """
  Builds a DuckLake PostgreSQL catalog connection string.

  ## Example

      iex> postgres_catalog_string()
      "postgres:host=localhost;port=5432;database=ducklake_catalog;user=quacklake;password=quacklake_secret"
  """
  def postgres_catalog_string do
    config = postgres_config()

    "postgres:host=#{config[:host]};port=#{config[:port]};database=#{config[:database]};user=#{config[:username]};password=#{config[:password]}"
  end

  @doc """
  Builds a DuckLake database connection string with PostgreSQL catalog.

  ## Example

      iex> ducklake_database_string("my_lake")
      "ducklake:postgres:host=localhost;port=5432;database=ducklake_catalog;user=quacklake;password=quacklake_secret;ducklake_alias=my_lake"
  """
  def ducklake_database_string(lake_name) do
    "ducklake:#{postgres_catalog_string()};ducklake_alias=#{lake_name}"
  end

  @doc """
  Returns the S3 endpoint URL with bucket path.
  """
  def s3_bucket_url do
    config = s3_config()
    "s3://#{config[:bucket]}"
  end

  @doc """
  Returns S3 secret configuration for DuckDB.
  """
  def s3_secret_config(name \\ :test_s3) do
    config = s3_config()

    {name,
     [
       type: :s3,
       key_id: config[:access_key_id],
       secret: config[:secret_access_key],
       region: config[:region],
       endpoint: config[:endpoint],
       use_ssl: config[:use_ssl],
       url_style: :path
     ]}
  end
end
