# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- **duckdbex 0.4.1 / DuckDB 1.5.3**: Bumped `duckdbex` from `~> 0.3.9` to `~> 0.4.1`. The removed `%Duckdbex.Config{}` API was not used by this library, so no public API changes

### Fixed

- **`core_functions` extension handling**: DuckDB 1.5 moved core scalar/aggregate functions (`sum`, window aggregates, …) into the `core_functions` extension, which the duckdbex 0.4 build does not load by default. New `QuackLake.Connection.ensure_core_functions/2` loads it (installing on first use when not cached) during connection initialization in `QuackLake.Connection.open/1` and both DBConnection protocols. Honors `:auto_install_extensions` / `:auto_load_extensions` — users who disable them must install/load `core_functions` themselves, and a first-time install requires network access to fetch the extension

## [0.2.8] - 2026-02-18

### Added

- **Container support**: Automatic DuckDB home directory detection and fix for Docker/Kubernetes environments where `HOME=/nonexistent`
- **`home_directory` config option**: Configurable fallback directory for DuckDB extension caching and catalog operations
- `QuackLake.Connection.ensure_home_env/1` — fixes `HOME` env var before connection open
- `QuackLake.Connection.set_home_directory/2` — runs `SET home_directory` on connection after open

### Documentation

- Added "Container Deployment" section to README
- Added `docs/duckdb-home-directory-fix.md` reference guide

## [0.2.7] - 2026-02-14

### Added

- **Test App**: Full demo application (`test_app/`) showcasing connections, Ecto adapters, appender bulk inserts, extensions, lake management, time travel, secrets, and PostgreSQL scanner usage
- **UUID support**: Loaders/dumpers for `:binary_id`, `Ecto.UUID`, and `:uuid` types in DuckDB and DuckLake Ecto adapters, including appender bulk inserts
- **NaiveDatetime & DateTime encoding**: Proper type encoding for `NaiveDateTime` and `DateTime` in query parameters for both adapters
- **Value decoding**: `decode_value` for Decimal tuples and HUGEINT in raw SQL results; `encode_param` for Decimal, DateTime, UUID in query parameters
- **S3 secret options**: `url_style` option (`"vhost"` or `"path"`) for S3-compatible services like MinIO

### Fixed

- **ATTACH SQL generation**: Fixed `TYPE DUCKLAKE` being added when path starts with `ducklake:`, which caused DuckDB to double-parse the connection string and silently drop options like `DATA_PATH`
- **S3 endpoint normalization**: Strip `http://` or `https://` scheme from endpoint URLs since DuckDB expects `host:port` only
- **`prepare_execute` pattern match**: Handle `DBConnection.execute/4` return correctly in both Protocol and LakeProtocol
- **`lake_exists?`**: Assume remote paths (S3/Azure/GCS) exist instead of failing on file checks
- **Test cleanup**: Fix race condition with `try/catch` on `GenServer.stop`

### Changed

- Bumped dependencies (duckdbex)
- Improved ATTACH SQL builder to conditionally include TYPE and DATA_PATH options

## [0.2.5] - 2025-01-31

### Added

- **Docker Compose Infrastructure**: PostgreSQL 16 and MinIO services for local development and testing
  - PostgreSQL as DuckLake metadata catalog backend
  - MinIO as S3-compatible object storage for lake data
  - Auto-initialization of test bucket on startup

- **Configuration Files**: Environment-specific configs (`config/config.exs`, `dev.exs`, `test.exs`, `runtime.exs`)

- **Comprehensive Test Suite**:
  - 95 unit tests for Config, Extension, Secret, Attach, and Error modules
  - Integration tests for PostgreSQL catalog operations
  - Integration tests for MinIO/S3 storage (Parquet read/write, secrets)
  - Integration tests for DuckLake lifecycle (create, write, query, time travel, transactions)
  - Ecto adapter integration tests for both DuckDB and DuckLake adapters

- **Test Support Modules**:
  - `QuackLake.Test.DockerHelper` - Service health checks and connection string builders
  - `QuackLake.Test.MinioHelper` - S3 secret setup and unique path generation
  - `QuackLake.DataCase` - ExUnit case template for tests requiring Docker services

- **New Config Options**:
  - `lake_name` - Custom alias for attached DuckLake (overrides auto-generated name from path)
  - `data_path` - Now properly captured and used for DuckLake S3/local data storage

- **Mix Aliases**:
  - `mix test.unit` - Run unit tests only
  - `mix test.integration` - Run integration tests (requires Docker)
  - `mix test.all` - Run all tests including integration

### Fixed

- **`data_path` config option**: Was not being captured in the Config struct or passed to `maybe_attach_lake/2`
- **`handle_execute/4` return value**: Now correctly returns 4-tuple `{:ok, query, result, state}` as DBConnection expects
- **`RawQuery.exec/2`**: Now correctly extracts result from DBConnection's 3-tuple `{:ok, query, result}` response

### Changed

- Updated `mix.exs` with `elixirc_paths/1` for test support modules
- Updated `test/test_helper.exs` to exclude integration tests by default (run with `INTEGRATION=true`)

### Documentation

- Added Development section to README with Docker setup instructions
- Documented all DuckLake adapter options in a table
- Added test directory structure overview

## [0.2.0] - 2025-01-30

### Added

- Ecto adapters: `Ecto.Adapters.DuckDB` and `Ecto.Adapters.DuckLake`
- PostgreSQL/RDS as DuckLake metadata catalog support
- Supervised connection example
- Cloud storage credentials (S3, Azure, GCS, R2, HuggingFace)
- High-performance Appender API for bulk inserts
- Time travel queries

### Changed

- Initial public release with full API

[0.2.8]: https://github.com/nyo16/quack_lake/compare/v0.2.7...v0.2.8
[0.2.7]: https://github.com/nyo16/quack_lake/compare/v0.2.5...v0.2.7
[0.2.5]: https://github.com/nyo16/quack_lake/compare/v0.2.0...v0.2.5
[0.2.0]: https://github.com/nyo16/quack_lake/releases/tag/v0.2.0
