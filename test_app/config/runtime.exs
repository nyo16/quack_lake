import Config

# Runtime configuration - override with environment variables

if config_env() in [:dev, :prod] do
  # PostgreSQL configuration (for DuckLake catalog)
  pg_host = System.get_env("POSTGRES_HOST", "localhost")
  pg_port = System.get_env("POSTGRES_PORT", "5432")
  pg_db = System.get_env("POSTGRES_DB", "ducklake_catalog")
  pg_user = System.get_env("POSTGRES_USER", "quacklake")
  pg_pass = System.get_env("POSTGRES_PASSWORD", "quacklake_secret")

  # S3/MinIO configuration
  s3_endpoint = System.get_env("S3_ENDPOINT", "http://localhost:9000")
  s3_bucket = System.get_env("DUCKLAKE_S3_BUCKET", "quacklake-test")
  s3_key = System.get_env("MINIO_ROOT_USER", "minioadmin")
  s3_secret = System.get_env("MINIO_ROOT_PASSWORD", "minioadmin123")

  # Build DuckLake connection string
  ducklake_db =
    "ducklake:postgres:host=#{pg_host};port=#{pg_port};database=#{pg_db};user=#{pg_user};password=#{pg_pass};ducklake_alias=test_lake"

  # S3 endpoint without protocol for DuckDB
  s3_endpoint_host = s3_endpoint |> String.replace(~r{^https?://}, "")

  config :test_app, TestApp.LakeRepo,
    database: ducklake_db,
    data_path: "s3://#{s3_bucket}/test_app_data",
    secrets: [
      {:minio_s3,
       [
         type: :s3,
         key_id: s3_key,
         secret: s3_secret,
         region: "us-east-1",
         endpoint: s3_endpoint_host,
         use_ssl: false,
         url_style: :path
       ]}
    ]

  config :test_app, :postgres,
    host: pg_host,
    port: String.to_integer(pg_port),
    database: pg_db,
    username: pg_user,
    password: pg_pass

  config :test_app, :s3,
    endpoint: s3_endpoint,
    bucket: s3_bucket,
    access_key_id: s3_key,
    secret_access_key: s3_secret,
    region: "us-east-1"
end
