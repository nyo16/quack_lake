# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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

[0.2.5]: https://github.com/nyo16/quack_lake/compare/v0.2.0...v0.2.5
[0.2.0]: https://github.com/nyo16/quack_lake/releases/tag/v0.2.0
