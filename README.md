# QuackLake

An Elixir library for easy [DuckLake](https://ducklake.select/) access, setup, and management.

DuckLake is DuckDB's open data lakehouse format that brings ACID transactions, time travel, and schema evolution to your data lake.

## Installation

Add `quack_lake` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:quack_lake, "~> 0.2.5"}
  ]
end
```

## Quick Start

```elixir
# Open a connection (automatically installs and loads the ducklake extension)
{:ok, conn} = QuackLake.open()

# Attach a DuckLake (creates it if it doesn't exist)
:ok = QuackLake.attach(conn, "my_lake", "my_lake.ducklake")

# Create a table and insert data
:ok = QuackLake.Query.execute(conn, "CREATE TABLE my_lake.users (id INT, name TEXT)")
:ok = QuackLake.Query.execute(conn, "INSERT INTO my_lake.users VALUES (1, 'Alice')")

# Query with ergonomic results (returns list of maps)
{:ok, rows} = QuackLake.query(conn, "SELECT * FROM my_lake.users")
# => [%{"id" => 1, "name" => "Alice"}]

# Time travel - list snapshots
{:ok, snapshots} = QuackLake.snapshots(conn, "my_lake")

# Query at a specific version
{:ok, old_rows} = QuackLake.query_at(conn, "SELECT * FROM my_lake.users", version: 1)
```

## Features

- **Ecto Adapters** - Full Ecto integration with `Ecto.Adapters.DuckDB` and `Ecto.Adapters.DuckLake`
- **Simple API** - Ergonomic Elixir interface with `{:ok, result}` / `{:error, reason}` tuples
- **Auto-setup** - Automatically installs and loads the DuckLake extension
- **Result transformation** - Query results returned as lists of maps instead of raw tuples
- **Time travel** - Query historical data at specific versions or timestamps
- **Cloud storage** - Built-in support for S3, Azure Blob Storage, and GCS credentials
- **Bulk Inserts** - High-performance Appender API for bulk data loading

## API Overview

### Connection Management

```elixir
# Open in-memory database
{:ok, conn} = QuackLake.open()

# Open persistent database
{:ok, conn} = QuackLake.open(path: "data.duckdb")

# Bang variant that raises on error
conn = QuackLake.open!()
```

### Lake Management

```elixir
# Attach a local DuckLake
:ok = QuackLake.attach(conn, "my_lake", "my_lake.ducklake")

# Attach with cloud data storage
:ok = QuackLake.attach(conn, "my_lake", "metadata.ducklake",
  data_path: "s3://my-bucket/data/")

# Detach a lake
:ok = QuackLake.detach(conn, "my_lake")

# List attached lakes
{:ok, lakes} = QuackLake.lakes(conn)
```

### Queries

```elixir
# Query returning all rows as maps
{:ok, rows} = QuackLake.query(conn, "SELECT * FROM my_lake.users")

# Query with parameters (use explicit types for arithmetic)
{:ok, rows} = QuackLake.query(conn, "SELECT * FROM my_lake.users WHERE id = $1", [1])

# Get single row (or nil)
{:ok, user} = QuackLake.query_one(conn, "SELECT * FROM my_lake.users WHERE id = $1", [1])

# Bang variants that raise on error
rows = QuackLake.query!(conn, "SELECT * FROM my_lake.users")
user = QuackLake.query_one!(conn, "SELECT * FROM my_lake.users WHERE id = $1", [1])

# Execute statements (CREATE, INSERT, UPDATE, DELETE)
:ok = QuackLake.Query.execute(conn, "INSERT INTO my_lake.users VALUES ($1, $2)", [2, "Bob"])

# Stream large result sets
QuackLake.Query.stream(conn, "SELECT * FROM my_lake.large_table")
|> Stream.each(&process_chunk/1)
|> Stream.run()
```

### Time Travel

```elixir
# List all snapshots
{:ok, snapshots} = QuackLake.snapshots(conn, "my_lake")

# Query at a specific version
{:ok, rows} = QuackLake.query_at(conn, "SELECT * FROM my_lake.users", version: 5)

# Query at a specific timestamp
{:ok, rows} = QuackLake.query_at(conn, "SELECT * FROM my_lake.users",
  timestamp: ~U[2024-01-15 10:00:00Z])

# Get changes between versions
{:ok, changes} = QuackLake.changes(conn, "my_lake", "main", "users", 1, 5)

# Expire old snapshots
:ok = QuackLake.Snapshot.expire(conn, "my_lake", before_version: 5)
```

### Cloud Storage Credentials

```elixir
# AWS S3
:ok = QuackLake.Secret.create_s3(conn, "my_s3",
  key_id: "AKIAIOSFODNN7EXAMPLE",
  secret: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
  region: "us-east-1"
)

# Azure Blob Storage
:ok = QuackLake.Secret.create_azure(conn, "my_azure",
  account_name: "myaccount",
  account_key: "mykey..."
)

# Google Cloud Storage
:ok = QuackLake.Secret.create_gcs(conn, "my_gcs",
  key_id: "GOOG1E...",
  secret: "..."
)

# List secrets
{:ok, secrets} = QuackLake.Secret.list(conn)

# Remove a secret
:ok = QuackLake.Secret.drop(conn, "my_s3")
```

### DuckDB Extensions

DuckDB supports many extensions for additional functionality. QuackLake automatically installs and loads the `ducklake` extension, but you can install others:

```elixir
{:ok, conn} = QuackLake.open()

# Install and load an extension in one call
:ok = QuackLake.Extension.ensure(conn, "httpfs")

# Now you can use httpfs features
{:ok, rows} = QuackLake.query(conn, """
  SELECT * FROM read_parquet('https://example.com/data.parquet') LIMIT 10
""")
```

Common extensions:

| Extension | Description |
|-----------|-------------|
| `httpfs` | HTTP/S3 file system for remote files |
| `spatial` | Geospatial types and functions |
| `json` | JSON parsing and extraction |
| `iceberg` | Apache Iceberg table format |
| `delta` | Delta Lake table format |
| `postgres_scanner` | Query PostgreSQL directly |
| `sqlite_scanner` | Query SQLite databases |
| `mysql_scanner` | Query MySQL directly |
| `excel` | Read Excel files |

Example with spatial extension:

```elixir
:ok = QuackLake.Extension.ensure(conn, "spatial")

:ok = QuackLake.Query.execute(conn, """
  CREATE TABLE my_lake.locations (name TEXT, geom GEOMETRY)
""")

:ok = QuackLake.Query.execute(conn, """
  INSERT INTO my_lake.locations VALUES ('NYC', ST_Point(-74.006, 40.7128))
""")
```

## Real-World Examples

### S3-Backed DuckLake with Runtime Configuration

Store your DuckLake data in S3, with credentials loaded from your application's runtime config.

**config/runtime.exs**

```elixir
import Config

config :my_app, :quack_lake,
  s3: [
    key_id: System.get_env("AWS_ACCESS_KEY_ID"),
    secret: System.get_env("AWS_SECRET_ACCESS_KEY"),
    region: System.get_env("AWS_REGION", "us-east-1"),
    bucket: System.get_env("DUCKLAKE_S3_BUCKET")
  ],
  metadata_path: System.get_env("DUCKLAKE_METADATA_PATH", "priv/lake.ducklake")
```

**lib/my_app/lake.ex**

```elixir
defmodule MyApp.Lake do
  @moduledoc """
  DuckLake connection manager.
  """

  def open do
    config = Application.fetch_env!(:my_app, :quack_lake)
    s3_config = Keyword.fetch!(config, :s3)
    metadata_path = Keyword.fetch!(config, :metadata_path)
    bucket = Keyword.fetch!(s3_config, :bucket)

    with {:ok, conn} <- QuackLake.open(),
         :ok <- setup_s3_secret(conn, s3_config),
         :ok <- QuackLake.attach(conn, "lake", metadata_path,
                  data_path: "s3://#{bucket}/data/") do
      {:ok, conn}
    end
  end

  defp setup_s3_secret(conn, s3_config) do
    QuackLake.Secret.create_s3(conn, "s3_creds",
      key_id: Keyword.fetch!(s3_config, :key_id),
      secret: Keyword.fetch!(s3_config, :secret),
      region: Keyword.fetch!(s3_config, :region)
    )
  end
end
```

**Usage**

```elixir
{:ok, conn} = MyApp.Lake.open()

# Create tables - data stored in S3, metadata in local file
:ok = QuackLake.Query.execute(conn, """
  CREATE TABLE lake.events (
    id INTEGER,
    user_id INTEGER,
    event_type TEXT,
    payload JSON,
    created_at TIMESTAMP
  )
""")

# Insert data
:ok = QuackLake.Query.execute(conn, """
  INSERT INTO lake.events VALUES
    (1, 42, 'page_view', '{"url": "/home"}', NOW())
""")

# Query with time travel
{:ok, rows} = QuackLake.query(conn, "SELECT * FROM lake.events WHERE user_id = $1", [42])
```

### Querying PostgreSQL Directly

Use DuckDB's `postgres_scanner` to query your PostgreSQL database and optionally sync data into your DuckLake.

**config/runtime.exs**

```elixir
import Config

config :my_app, :postgres,
  host: System.get_env("POSTGRES_HOST", "localhost"),
  port: System.get_env("POSTGRES_PORT", "5432"),
  database: System.get_env("POSTGRES_DB"),
  username: System.get_env("POSTGRES_USER"),
  password: System.get_env("POSTGRES_PASSWORD")
```

**lib/my_app/analytics.ex**

```elixir
defmodule MyApp.Analytics do
  @moduledoc """
  Analytics queries combining PostgreSQL and DuckLake data.
  """

  def open do
    with {:ok, conn} <- QuackLake.open(),
         :ok <- QuackLake.Extension.ensure(conn, "postgres_scanner"),
         :ok <- attach_postgres(conn) do
      {:ok, conn}
    end
  end

  defp attach_postgres(conn) do
    pg = Application.fetch_env!(:my_app, :postgres)

    QuackLake.Query.execute(conn, """
      ATTACH 'dbname=#{pg[:database]} user=#{pg[:username]} password=#{pg[:password]} host=#{pg[:host]} port=#{pg[:port]}'
      AS pg (TYPE POSTGRES, READ_ONLY)
    """)
  end

  @doc """
  Query PostgreSQL directly through DuckDB.
  """
  def query_postgres(conn, sql, params \\ []) do
    QuackLake.query(conn, sql, params)
  end

  @doc """
  Sync a PostgreSQL table into DuckLake for fast analytics.
  """
  def sync_table(conn, pg_table, lake_table) do
    QuackLake.Query.execute(conn, """
      CREATE OR REPLACE TABLE #{lake_table} AS
      SELECT * FROM pg.public.#{pg_table}
    """)
  end
end
```

**Usage**

```elixir
{:ok, conn} = MyApp.Analytics.open()

# Query PostgreSQL directly (uses postgres_scanner)
{:ok, users} = MyApp.Analytics.query_postgres(conn, """
  SELECT * FROM pg.public.users WHERE created_at > '2024-01-01'
""")

# Sync PostgreSQL table to DuckLake for faster repeated queries
:ok = MyApp.Analytics.sync_table(conn, "orders", "lake.orders")

# Now query the local copy (much faster for analytics)
{:ok, stats} = QuackLake.query(conn, """
  SELECT
    date_trunc('month', created_at) as month,
    COUNT(*) as order_count,
    SUM(total) as revenue
  FROM lake.orders
  GROUP BY 1
  ORDER BY 1
""")

# Join PostgreSQL and DuckLake data
{:ok, report} = QuackLake.query(conn, """
  SELECT u.email, COUNT(o.id) as orders
  FROM pg.public.users u
  JOIN lake.orders o ON o.user_id = u.id
  GROUP BY 1
  ORDER BY 2 DESC
  LIMIT 10
""")
```

### Combining S3 Storage with PostgreSQL

**lib/my_app/data_platform.ex**

```elixir
defmodule MyApp.DataPlatform do
  @moduledoc """
  Full data platform: S3-backed DuckLake + PostgreSQL access.
  """

  def open do
    config = Application.fetch_env!(:my_app, :quack_lake)
    pg_config = Application.fetch_env!(:my_app, :postgres)
    s3_config = Keyword.fetch!(config, :s3)

    with {:ok, conn} <- QuackLake.open(),
         :ok <- QuackLake.Extension.ensure(conn, "postgres_scanner"),
         :ok <- setup_s3(conn, s3_config),
         :ok <- setup_postgres(conn, pg_config),
         :ok <- setup_lake(conn, config, s3_config) do
      {:ok, conn}
    end
  end

  defp setup_s3(conn, s3) do
    QuackLake.Secret.create_s3(conn, "s3_creds",
      key_id: s3[:key_id],
      secret: s3[:secret],
      region: s3[:region]
    )
  end

  defp setup_postgres(conn, pg) do
    QuackLake.Query.execute(conn, """
      ATTACH 'dbname=#{pg[:database]} user=#{pg[:username]} password=#{pg[:password]} host=#{pg[:host]}'
      AS pg (TYPE POSTGRES, READ_ONLY)
    """)
  end

  defp setup_lake(conn, config, s3) do
    QuackLake.attach(conn, "lake", config[:metadata_path],
      data_path: "s3://#{s3[:bucket]}/lake/"
    )
  end
end
```

### Production DuckLake with PostgreSQL Catalog (AWS RDS)

For production deployments, you can use PostgreSQL (e.g., AWS RDS) as DuckLake's metadata catalog instead of a local file. This provides a reliable, shared catalog with managed backups and replication.

**Architecture:**

```
┌─────────────────┐     ┌─────────────────┐
│  DuckDB/Ecto    │────▶│   AWS RDS       │  (metadata catalog)
│  (your app)     │     │   PostgreSQL    │
└────────┬────────┘     └─────────────────┘
         │
         ▼
┌─────────────────┐
│    AWS S3       │  (actual data - parquet files)
│  s3://bucket/   │
└─────────────────┘
```

**Connection string format:**

```
ducklake:postgres:host=<host>;database=<db>;user=<user>;password=<pass>
```

**With the Ecto adapter:**

```elixir
# config/runtime.exs
config :my_app, MyApp.LakeRepo,
  adapter: Ecto.Adapters.DuckLake,
  database: "ducklake:postgres:host=#{System.get_env("RDS_HOST")};database=#{System.get_env("RDS_DB")};user=#{System.get_env("RDS_USER")};password=#{System.get_env("RDS_PASSWORD")}",
  pool_size: 5,
  lake_name: "lake",  # Short alias for the attached lake
  data_path: "s3://my-bucket/lake-data",
  extensions: [:httpfs, {:ducklake, source: :core}],
  secrets: [
    {:my_s3, [
      type: :s3,
      key_id: System.get_env("AWS_ACCESS_KEY_ID"),
      secret: System.get_env("AWS_SECRET_ACCESS_KEY"),
      region: "us-east-1"
    ]}
  ]
```

```elixir
# lib/my_app/lake_repo.ex
defmodule MyApp.LakeRepo do
  use Ecto.Repo,
    otp_app: :my_app,
    adapter: Ecto.Adapters.DuckLake

  use Ecto.Adapters.DuckDB.RawQuery
end
```

**With raw QuackLake API:**

```elixir
{:ok, conn} = QuackLake.open()

# Setup S3 credentials
:ok = QuackLake.Secret.create_s3(conn, "s3_creds",
  key_id: System.get_env("AWS_ACCESS_KEY_ID"),
  secret: System.get_env("AWS_SECRET_ACCESS_KEY"),
  region: "us-east-1"
)

# Attach DuckLake with PostgreSQL catalog + S3 data storage
:ok = QuackLake.Query.execute(conn, """
  ATTACH 'ducklake:postgres:host=your-instance.rds.amazonaws.com;database=ducklake_meta;user=myuser;password=mypass'
  AS lake (TYPE DUCKLAKE, DATA_PATH 's3://my-bucket/lake-data/')
""")

# Now use the lake
:ok = QuackLake.Query.execute(conn, "CREATE TABLE lake.events (id INT, type TEXT, ts TIMESTAMP)")
{:ok, rows} = QuackLake.query(conn, "SELECT * FROM lake.events")
```

**Benefits of this setup:**

- **Concurrent writers** - Multiple app instances can write simultaneously
- **Managed metadata** - RDS handles backups, failover, encryption at rest
- **Scalable data** - S3 for unlimited, cost-effective storage
- **Time travel** - Snapshots stored in the PostgreSQL catalog
- **High availability** - Use RDS Multi-AZ for automatic failover

**AWS RDS requirements:**

- Create a dedicated database (e.g., `ducklake_meta`) - no special extensions needed
- Security group must allow inbound connections from your app servers
- Recommended: Use IAM authentication or Secrets Manager for credentials
- For SSL: Add `sslmode=require` to the connection string

## Supervised Connection

QuackLake uses plain functions by default (no process overhead). If you want a supervised connection that restarts on failure, wrap it in a GenServer:

**lib/my_app/lake_server.ex**

```elixir
defmodule MyApp.LakeServer do
  @moduledoc """
  Supervised DuckLake connection.
  """
  use GenServer

  # Client API

  def start_link(opts) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  def query(server \\ __MODULE__, sql, params \\ []) do
    GenServer.call(server, {:query, sql, params})
  end

  def query!(server \\ __MODULE__, sql, params \\ []) do
    case query(server, sql, params) do
      {:ok, rows} -> rows
      {:error, reason} -> raise QuackLake.Error, message: "Query failed", reason: reason
    end
  end

  def execute(server \\ __MODULE__, sql, params \\ []) do
    GenServer.call(server, {:execute, sql, params})
  end

  def conn(server \\ __MODULE__) do
    GenServer.call(server, :conn)
  end

  # Server callbacks

  @impl true
  def init(opts) do
    case setup_connection(opts) do
      {:ok, conn} -> {:ok, %{conn: conn, opts: opts}}
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_call({:query, sql, params}, _from, %{conn: conn} = state) do
    {:reply, QuackLake.query(conn, sql, params), state}
  end

  def handle_call({:execute, sql, params}, _from, %{conn: conn} = state) do
    {:reply, QuackLake.Query.execute(conn, sql, params), state}
  end

  def handle_call(:conn, _from, %{conn: conn} = state) do
    {:reply, conn, state}
  end

  defp setup_connection(opts) do
    config = Keyword.get(opts, :config, Application.fetch_env!(:my_app, :quack_lake))
    s3_config = config[:s3]

    with {:ok, conn} <- QuackLake.open(),
         :ok <- maybe_setup_s3(conn, s3_config),
         :ok <- maybe_attach_lake(conn, config, s3_config) do
      {:ok, conn}
    end
  end

  defp maybe_setup_s3(_conn, nil), do: :ok
  defp maybe_setup_s3(conn, s3) do
    QuackLake.Secret.create_s3(conn, "s3_creds",
      key_id: s3[:key_id],
      secret: s3[:secret],
      region: s3[:region]
    )
  end

  defp maybe_attach_lake(_conn, %{lake_name: nil}, _s3), do: :ok
  defp maybe_attach_lake(_conn, %{metadata_path: nil}, _s3), do: :ok
  defp maybe_attach_lake(conn, config, nil) do
    QuackLake.attach(conn, config[:lake_name] || "lake", config[:metadata_path])
  end
  defp maybe_attach_lake(conn, config, s3) do
    QuackLake.attach(conn, config[:lake_name] || "lake", config[:metadata_path],
      data_path: "s3://#{s3[:bucket]}/data/"
    )
  end
end
```

**lib/my_app/application.ex**

```elixir
defmodule MyApp.Application do
  use Application

  @impl true
  def start(_type, _args) do
    children = [
      # ... other children
      {MyApp.LakeServer, name: MyApp.LakeServer}
    ]

    opts = [strategy: :one_for_one, name: MyApp.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
```

**config/runtime.exs**

```elixir
import Config

config :my_app, :quack_lake,
  lake_name: "lake",
  metadata_path: System.get_env("DUCKLAKE_METADATA_PATH", "priv/lake.ducklake"),
  s3: if System.get_env("AWS_ACCESS_KEY_ID") do
    [
      key_id: System.get_env("AWS_ACCESS_KEY_ID"),
      secret: System.get_env("AWS_SECRET_ACCESS_KEY"),
      region: System.get_env("AWS_REGION", "us-east-1"),
      bucket: System.get_env("DUCKLAKE_S3_BUCKET")
    ]
  end
```

**Usage**

```elixir
# Queries go through the supervised connection
{:ok, rows} = MyApp.LakeServer.query("SELECT * FROM lake.users")
rows = MyApp.LakeServer.query!("SELECT * FROM lake.users WHERE id = $1", [1])

# Execute statements
:ok = MyApp.LakeServer.execute("INSERT INTO lake.users VALUES ($1, $2)", [1, "Alice"])

# Get the raw connection for advanced operations
conn = MyApp.LakeServer.conn()
{:ok, snapshots} = QuackLake.snapshots(conn, "lake")
```

> **Note:** A single GenServer serializes all queries. For concurrent workloads, consider a pool (e.g., using `poolboy`) or opening connections per-request.

## Ecto Adapters

QuackLake provides two Ecto adapters for different use cases:

### Ecto.Adapters.DuckDB (Single Writer)

For local analytics with a single writer:

```elixir
# config/config.exs
config :my_app, MyApp.Repo,
  adapter: Ecto.Adapters.DuckDB,
  database: "priv/analytics.duckdb",
  extensions: [:httpfs, :parquet, {:spatial, source: :core}]
```

```elixir
# lib/my_app/repo.ex
defmodule MyApp.Repo do
  use Ecto.Repo,
    otp_app: :my_app,
    adapter: Ecto.Adapters.DuckDB

  # Optional: Add raw query and appender support
  use Ecto.Adapters.DuckDB.RawQuery
end
```

### Ecto.Adapters.DuckLake (Concurrent Writers)

For lakehouse deployments with concurrent writers:

```elixir
# config/config.exs
config :my_app, MyApp.LakeRepo,
  adapter: Ecto.Adapters.DuckLake,
  database: "ducklake:analytics.ducklake",
  pool_size: 5,
  lake_name: "lake",  # Custom short name (optional, overrides auto-generated)
  data_path: "s3://my-bucket/lake-data",
  extensions: [:httpfs, {:ducklake, source: :core}],
  secrets: [
    {:my_s3, [
      type: :s3,
      key_id: System.get_env("AWS_ACCESS_KEY_ID"),
      secret: System.get_env("AWS_SECRET_ACCESS_KEY"),
      region: "us-east-1"
    ]}
  ]
```

**DuckLake Adapter Options:**

| Option | Description |
|--------|-------------|
| `database` | DuckLake connection string (e.g., `ducklake:analytics.ducklake` or `ducklake:postgres:host=...`) |
| `pool_size` | Number of concurrent connections (default: 5) |
| `lake_name` | Custom lake name alias (optional, auto-generated from path if not provided) |
| `data_path` | Storage path for actual data (S3, local, etc.) |
| `extensions` | List of DuckDB extensions to load |
| `secrets` | Cloud storage credentials |

```elixir
# lib/my_app/lake_repo.ex
defmodule MyApp.LakeRepo do
  use Ecto.Repo,
    otp_app: :my_app,
    adapter: Ecto.Adapters.DuckLake

  use Ecto.Adapters.DuckDB.RawQuery
end
```

### Using Ecto with DuckDB

```elixir
# Define schemas
defmodule MyApp.User do
  use Ecto.Schema

  schema "users" do
    field :name, :string
    field :email, :string
    timestamps()
  end
end

# Standard Ecto operations
MyApp.Repo.insert!(%User{name: "Alice", email: "alice@example.com"})
MyApp.Repo.all(User)
MyApp.Repo.get!(User, 1)

# Raw SQL execution (with RawQuery)
MyApp.Repo.exec!("COPY users TO 'users.parquet' (FORMAT PARQUET)")

# High-performance bulk inserts with Appender
{:ok, appender} = MyApp.Repo.appender(User)
Enum.each(users, &MyApp.Repo.append(appender, &1))
MyApp.Repo.close_appender(appender)
```

### Migrations

```elixir
defmodule MyApp.Repo.Migrations.CreateUsers do
  use Ecto.Migration

  def change do
    create table(:users) do
      add :name, :string
      add :email, :string
      add :metadata, :map
      timestamps()
    end

    create index(:users, [:email])
  end
end
```

Run migrations:

```bash
mix ecto.create
mix ecto.migrate
```

## Module Reference

| Module | Description |
|--------|-------------|
| `QuackLake` | Main facade with high-level API |
| `QuackLake.Connection` | Connection lifecycle management |
| `QuackLake.Query` | Query execution and streaming |
| `QuackLake.Lake` | DuckLake attach/detach operations |
| `QuackLake.Snapshot` | Time travel and snapshot management |
| `QuackLake.Secret` | Cloud storage credential management |
| `QuackLake.Extension` | DuckDB extension helpers |
| `QuackLake.Appender` | High-performance bulk insert API |
| `QuackLake.Config` | Configuration struct |
| `QuackLake.Error` | Error exception struct |
| `Ecto.Adapters.DuckDB` | Ecto adapter for DuckDB (single writer) |
| `Ecto.Adapters.DuckLake` | Ecto adapter for DuckLake (concurrent writers) |

## Development

### Prerequisites

- Elixir 1.15+
- Docker and Docker Compose (for integration tests)

### Setup

```bash
# Clone the repository
git clone https://github.com/nyo16/quack_lake.git
cd quack_lake

# Install dependencies
mix deps.get

# Run unit tests (no Docker required)
mix test test/unit
```

### Integration Tests

Integration tests require PostgreSQL and MinIO (S3-compatible storage) running via Docker:

```bash
# Start Docker services
docker-compose up -d

# Wait for services to be healthy
docker-compose ps

# Run all tests including integration
INTEGRATION=true mix test

# Run only integration tests
INTEGRATION=true mix test test/integration

# Run specific integration test file
INTEGRATION=true mix test test/integration/postgres_catalog_test.exs
```

### Docker Services

The `docker-compose.yml` provides:

| Service | Port | Description |
|---------|------|-------------|
| PostgreSQL | 5432 | DuckLake metadata catalog |
| MinIO | 9000 | S3-compatible object storage |
| MinIO Console | 9001 | Web UI for MinIO |

**Default Credentials:**

| Service | Username | Password |
|---------|----------|----------|
| PostgreSQL | `quacklake` | `quacklake_secret` |
| MinIO | `minioadmin` | `minioadmin123` |

### Test Structure

```
test/
├── unit/                    # Unit tests (async, no Docker)
│   ├── config_test.exs
│   └── config/
│       ├── attach_test.exs
│       ├── extension_test.exs
│       └── secret_test.exs
├── integration/             # Integration tests (require Docker)
│   ├── postgres_catalog_test.exs
│   ├── minio_s3_test.exs
│   ├── ducklake_lifecycle_test.exs
│   └── ecto/
│       ├── duckdb_adapter_test.exs
│       └── ducklake_adapter_test.exs
└── support/                 # Test helpers
    ├── data_case.ex
    ├── docker_helper.ex
    └── minio_helper.ex
```

## License

MIT
