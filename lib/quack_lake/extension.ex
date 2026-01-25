defmodule QuackLake.Extension do
  @moduledoc """
  Helpers for installing and loading DuckDB extensions.

  DuckDB supports many extensions for additional functionality:

  | Extension | Description |
  |-----------|-------------|
  | `ducklake` | DuckLake data lakehouse format (auto-loaded by QuackLake) |
  | `httpfs` | HTTP/S3 file system for remote files |
  | `spatial` | Geospatial types and functions |
  | `json` | JSON parsing and extraction |
  | `iceberg` | Apache Iceberg table format |
  | `delta` | Delta Lake table format |
  | `postgres_scanner` | Query PostgreSQL directly |
  | `sqlite_scanner` | Query SQLite databases |
  | `mysql_scanner` | Query MySQL directly |
  | `excel` | Read Excel files |

  ## Examples

      # Install and load in one call
      :ok = QuackLake.Extension.ensure(conn, "httpfs")

      # Or separately
      :ok = QuackLake.Extension.install(conn, "spatial")
      :ok = QuackLake.Extension.load(conn, "spatial")

  """

  @doc """
  Installs a DuckDB extension.

  Downloads the extension if not already installed. This is idempotent -
  calling it multiple times is safe.

  ## Examples

      iex> QuackLake.Extension.install(conn, "httpfs")
      :ok

  """
  @spec install(Duckdbex.connection(), String.t()) :: :ok | {:error, term()}
  def install(conn, extension_name) do
    query = "INSTALL #{extension_name}"

    case Duckdbex.query(conn, query) do
      {:ok, _ref} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Loads a DuckDB extension.

  The extension must be installed first. This is idempotent -
  calling it multiple times is safe.

  ## Examples

      iex> QuackLake.Extension.load(conn, "httpfs")
      :ok

  """
  @spec load(Duckdbex.connection(), String.t()) :: :ok | {:error, term()}
  def load(conn, extension_name) do
    query = "LOAD #{extension_name}"

    case Duckdbex.query(conn, query) do
      {:ok, _ref} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Installs and loads an extension in one call.

  This is the recommended way to enable an extension.

  ## Examples

      iex> QuackLake.Extension.ensure(conn, "httpfs")
      :ok

      iex> QuackLake.Extension.ensure(conn, "spatial")
      :ok

  """
  @spec ensure(Duckdbex.connection(), String.t()) :: :ok | {:error, term()}
  def ensure(conn, extension_name) do
    with :ok <- install(conn, extension_name),
         :ok <- load(conn, extension_name) do
      :ok
    end
  end

  @doc """
  Ensures the ducklake extension is installed and loaded.

  This is called automatically by `QuackLake.open/1`.

  ## Examples

      iex> QuackLake.Extension.ensure_ducklake(conn)
      :ok

  """
  @spec ensure_ducklake(Duckdbex.connection()) :: :ok | {:error, term()}
  def ensure_ducklake(conn) do
    ensure(conn, "ducklake")
  end
end
