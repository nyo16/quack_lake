# QuackLake Demo Application

This application demonstrates all features of the [QuackLake](https://github.com/nyo16/quack_lake) library.

## Features Demonstrated

| Demo | Description | Docker Required |
|------|-------------|-----------------|
| `connection` | Connection management (in-memory, persistent, options) | No |
| `query` | Query patterns (query, query_one, execute, stream) | No |
| `extensions` | DuckDB extension management | No |
| `secrets` | Cloud storage secrets (S3, Azure, GCS, R2, HuggingFace) | No |
| `lake` | Lake management (attach, detach, list) | Yes |
| `appender` | High-performance bulk inserts | No |
| `timetravel` | Time travel and snapshots | Yes |
| `ecto.duckdb` | Ecto.Adapters.DuckDB (single writer) | No |
| `ecto.ducklake` | Ecto.Adapters.DuckLake (concurrent writers) | Yes |
| `postgres` | PostgreSQL scanner integration | Yes |

## Prerequisites

1. **Elixir 1.15+**

2. **Docker** (for demos marked "Docker Required")

## Setup

```bash
# From the quack_lake root directory
cd test_app

# Install dependencies
mix deps.get

# Start Docker services (from quack_lake root)
cd ..
docker-compose up -d
cd test_app
```

## Running Demos

### Run All Demos

```bash
mix demo
```

### Run Individual Demos

```bash
# Connection management
mix demo.connection

# Query patterns
mix demo.query

# DuckDB extensions
mix demo.extensions

# Cloud storage secrets
mix demo.secrets

# Lake management (requires Docker)
mix demo.lake

# Appender API (bulk inserts)
mix demo.appender

# Time travel (requires Docker)
mix demo.timetravel

# Ecto DuckDB adapter
mix demo.ecto.duckdb

# Ecto DuckLake adapter (requires Docker)
mix demo.ecto.ducklake

# PostgreSQL scanner (requires Docker)
mix demo.postgres
```

### Interactive Mode

```bash
iex -S mix

# Then run any demo
iex> TestApp.Demos.ConnectionDemo.run()
iex> TestApp.Demos.QueryDemo.run()
```

## Docker Services

The demos use the following Docker services (defined in `../docker-compose.yml`):

| Service | Port | Credentials |
|---------|------|-------------|
| PostgreSQL | 5432 | `quacklake` / `quacklake_secret` |
| MinIO (S3) | 9000 | `minioadmin` / `minioadmin123` |
| MinIO Console | 9001 | `minioadmin` / `minioadmin123` |

### Start Services

```bash
# From quack_lake root directory
docker-compose up -d

# Check status
docker-compose ps

# View logs
docker-compose logs -f
```

### Stop Services

```bash
docker-compose down

# Remove volumes too
docker-compose down -v
```

## Project Structure

```
test_app/
├── config/
│   ├── config.exs      # Base configuration
│   ├── dev.exs         # Development configuration
│   ├── runtime.exs     # Runtime configuration (env vars)
│   └── test.exs        # Test configuration
├── lib/
│   ├── test_app.ex     # Main module with run_all_demos/0
│   ├── test_app/
│   │   ├── application.ex    # OTP Application
│   │   ├── repo.ex           # Ecto.Adapters.DuckDB repo
│   │   ├── lake_repo.ex      # Ecto.Adapters.DuckLake repo
│   │   ├── lake_server.ex    # Supervised GenServer connection
│   │   └── schemas/          # Ecto schemas
│   └── demos/
│       ├── connection_demo.ex
│       ├── query_demo.ex
│       ├── extensions_demo.ex
│       ├── secrets_demo.ex
│       ├── lake_management_demo.ex
│       ├── appender_demo.ex
│       ├── time_travel_demo.ex
│       ├── ecto_duckdb_demo.ex
│       ├── ecto_ducklake_demo.ex
│       └── postgres_scanner_demo.ex
└── mix.exs
```

## Configuration

Environment variables (with defaults):

```bash
# PostgreSQL
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=ducklake_catalog
POSTGRES_USER=quacklake
POSTGRES_PASSWORD=quacklake_secret

# S3/MinIO
S3_ENDPOINT=http://localhost:9000
DUCKLAKE_S3_BUCKET=quacklake-test
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin123
```

## Expected Output

When running all demos, you should see output like:

```
╔══════════════════════════════════════════════════════════════╗
║              QUACKLAKE DEMO APPLICATION                      ║
╚══════════════════════════════════════════════════════════════╝

>>> Running: Connection Management
============================================================
CONNECTION MANAGEMENT DEMO
============================================================

--- In-Memory Database ---
  Opened in-memory database
  Query result: [%{"greeting" => "Hello from in-memory!"}]

...

╔══════════════════════════════════════════════════════════════╗
║                      DEMO SUMMARY                            ║
╠══════════════════════════════════════════════════════════════╣
║  Successful: 10  demos                                       ║
║  Failed:     0   demos                                       ║
╚══════════════════════════════════════════════════════════════╝
```

## Troubleshooting

### "PostgreSQL not available"

Ensure Docker services are running:

```bash
docker-compose up -d
docker-compose ps  # Should show "healthy" status
```

### "pg_isready command not found"

Install PostgreSQL client tools:

```bash
# macOS
brew install postgresql

# Ubuntu/Debian
apt-get install postgresql-client
```

### Compilation errors

Ensure you're using the local quack_lake dependency:

```bash
cd test_app
mix deps.get
mix compile
```
