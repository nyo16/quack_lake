import Config

# Test environment configuration
# Uses Docker services when INTEGRATION=true

# PostgreSQL catalog connection (for DuckLake metadata)
config :quack_lake, :postgres,
  host: System.get_env("POSTGRES_HOST", "localhost"),
  port: String.to_integer(System.get_env("POSTGRES_PORT", "5432")),
  database: System.get_env("POSTGRES_DB", "ducklake_catalog"),
  username: System.get_env("POSTGRES_USER", "quacklake"),
  password: System.get_env("POSTGRES_PASSWORD", "quacklake_secret")

# MinIO/S3 configuration
config :quack_lake, :s3,
  endpoint: System.get_env("S3_ENDPOINT", "http://localhost:9000"),
  bucket: System.get_env("DUCKLAKE_S3_BUCKET", "quacklake-test"),
  access_key_id: System.get_env("MINIO_ROOT_USER", "minioadmin"),
  secret_access_key: System.get_env("MINIO_ROOT_PASSWORD", "minioadmin123"),
  region: "us-east-1",
  use_ssl: false

# Logger configuration for tests
config :logger, level: :warning
