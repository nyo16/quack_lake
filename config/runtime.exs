import Config

# Runtime configuration for production
# This file is executed at runtime, so it can read environment variables

if config_env() == :prod do
  # PostgreSQL catalog connection (for DuckLake metadata)
  config :quack_lake, :postgres,
    host: System.get_env("POSTGRES_HOST") || raise("POSTGRES_HOST not set"),
    port: String.to_integer(System.get_env("POSTGRES_PORT", "5432")),
    database: System.get_env("POSTGRES_DB") || raise("POSTGRES_DB not set"),
    username: System.get_env("POSTGRES_USER") || raise("POSTGRES_USER not set"),
    password: System.get_env("POSTGRES_PASSWORD") || raise("POSTGRES_PASSWORD not set")

  # S3 configuration (optional, only if using S3 storage)
  if System.get_env("S3_ENDPOINT") do
    config :quack_lake, :s3,
      endpoint: System.get_env("S3_ENDPOINT"),
      bucket: System.get_env("DUCKLAKE_S3_BUCKET"),
      access_key_id: System.get_env("AWS_ACCESS_KEY_ID"),
      secret_access_key: System.get_env("AWS_SECRET_ACCESS_KEY"),
      region: System.get_env("AWS_REGION", "us-east-1"),
      use_ssl: System.get_env("S3_USE_SSL", "true") == "true"
  end
end
