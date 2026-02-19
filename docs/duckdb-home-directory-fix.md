# DuckDB Home Directory Fix for Container Environments

## The Problem

DuckDB requires a writable home directory for two purposes:

1. **Extension caching** — When installing/loading extensions, DuckDB writes to `~/.duckdb/extensions/`
2. **Catalog operations** — Internal DuckDB operations reference `home_directory` for temp files

In container environments (Docker, Kubernetes, AWS ECS, Google Cloud Run), the `HOME` environment variable is often:

- Set to `/nonexistent` (common in Debian-based images with `nobody` user)
- Set to `/` (read-only root filesystem)
- Unset entirely

This causes DuckDB to fail with:

```
IO Error: Can't find the home directory
```

## How QuackLake Fixes This

QuackLake applies two fixes on every connection path (raw API, DuckDB Ecto adapter, and DuckLake Ecto adapter):

### 1. Fix `HOME` env var before `Duckdbex.open()`

`QuackLake.Connection.ensure_home_env/1` checks if `HOME` points to a valid directory. If not, it sets `HOME` to a resolved fallback **before** the DuckDB database is opened. This is necessary because DuckDB reads `HOME` at open time for extension paths.

### 2. Run `SET home_directory` after connection open

`QuackLake.Connection.set_home_directory/2` executes `SET home_directory='...'` on the connection. This configures DuckDB's internal `home_directory` setting, which some operations use independently of the `HOME` env var.

### Resolution Order

The effective home directory is resolved by `QuackLake.Config.resolve_home_directory/1`:

| Priority | Source | Description |
|----------|--------|-------------|
| 1 | `:home_directory` config option | Explicit override from application config |
| 2 | `DUCKDB_HOME` env var | Dedicated DuckDB home env var (if directory exists) |
| 3 | `HOME` env var | Standard home directory (if directory exists) |
| 4 | `/tmp` | Final fallback (always exists and is writable) |

## Configuration

### No config needed (automatic)

In most cases, QuackLake detects and fixes the home directory automatically. If `HOME` is valid, nothing changes. If `HOME` is invalid, it falls back to `/tmp`.

### Explicit override

If you want to control exactly where DuckDB stores its files:

```elixir
# Raw API
{:ok, conn} = QuackLake.open(home_directory: "/app/data/duckdb")

# Ecto DuckDB adapter
config :my_app, MyApp.Repo,
  adapter: Ecto.Adapters.DuckDB,
  database: "priv/analytics.duckdb",
  home_directory: "/app/data/duckdb"

# Ecto DuckLake adapter
config :my_app, MyApp.LakeRepo,
  adapter: Ecto.Adapters.DuckLake,
  database: "ducklake:analytics.ducklake",
  home_directory: "/app/data/duckdb"
```

### Using `DUCKDB_HOME` env var

Set the `DUCKDB_HOME` environment variable in your container:

```dockerfile
ENV DUCKDB_HOME=/app/data/duckdb
RUN mkdir -p /app/data/duckdb
```

This takes priority over `HOME` but is overridden by the explicit `:home_directory` config option.

## Dockerfile Example

```dockerfile
FROM elixir:1.17-slim

# Create a writable directory for DuckDB
RUN mkdir -p /app/data/duckdb

# Set DUCKDB_HOME so QuackLake picks it up automatically
ENV DUCKDB_HOME=/app/data/duckdb

WORKDIR /app
COPY . .
RUN mix deps.get && mix release

CMD ["_build/prod/rel/my_app/bin/my_app", "start"]
```

## Mix Release (`rel/env.sh.eex`)

For Elixir releases, you can fix `HOME` before the BEAM even starts. Add to `rel/env.sh.eex`:

```bash
# DuckDB requires a writable HOME for extension caching and catalog operations.
# Container images often set HOME=/nonexistent — override to /tmp.
if [ ! -d "$HOME" ]; then
  export HOME=/tmp
fi
```

This is the earliest possible fix — it runs in the shell wrapper before `erl` is invoked, so DuckDB NIFs see a valid `HOME` from the very first moment. QuackLake's runtime fix (`ensure_home_env/1`) serves as a safety net if this isn't configured.

## Kubernetes Example

```yaml
apiVersion: apps/v1
kind: Deployment
spec:
  template:
    spec:
      containers:
        - name: app
          env:
            - name: DUCKDB_HOME
              value: /tmp/duckdb
```

## Technical Details

- `ensure_home_env/1` mutates the `HOME` process env var. This is safe because Erlang/Elixir env vars are process-wide (not per-OS-thread), and this runs before any DuckDB operations.
- `set_home_directory/2` uses a DuckDB `SET` statement, which is connection-scoped.
- Both functions are idempotent — calling them multiple times is safe.
- The `/tmp` fallback is used because it exists and is writable on virtually all Linux systems, including minimal container images.
