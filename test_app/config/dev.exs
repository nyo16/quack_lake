import Config

# DuckDB Repo (single writer, file-based)
# pool_size must be 1 for DuckDB due to single-writer semantics
config :test_app, TestApp.Repo,
  adapter: Ecto.Adapters.DuckDB,
  database: "priv/test_app.duckdb",
  pool_size: 1

# DuckLake Repo (concurrent writers, PostgreSQL catalog + S3)
config :test_app, TestApp.LakeRepo,
  adapter: Ecto.Adapters.DuckLake,
  database:
    "ducklake:postgres:host=localhost;port=5432;database=ducklake_catalog;user=quacklake;password=quacklake_secret;ducklake_alias=test_lake",
  pool_size: 3,
  lake_name: "lake",
  data_path: "s3://quacklake-test/test_app_data",
  extensions: [:httpfs],
  secrets: [
    {:minio_s3,
     [
       type: :s3,
       key_id: "minioadmin",
       secret: "minioadmin123",
       region: "us-east-1",
       endpoint: "localhost:9000",
       use_ssl: false,
       url_style: :path
     ]}
  ]

# PostgreSQL connection for postgres_scanner demos
config :test_app, :postgres,
  host: "localhost",
  port: 5432,
  database: "ducklake_catalog",
  username: "quacklake",
  password: "quacklake_secret"

# S3/MinIO configuration
config :test_app, :s3,
  endpoint: "http://localhost:9000",
  bucket: "quacklake-test",
  access_key_id: "minioadmin",
  secret_access_key: "minioadmin123",
  region: "us-east-1"

config :logger, level: :info
