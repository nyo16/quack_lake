defmodule QuackLake do
  @moduledoc """
  Easy DuckLake access, setup, and management.

  QuackLake provides an ergonomic Elixir interface for working with DuckLake,
  DuckDB's data lakehouse extension.

  ## Quick Start

      # Open a connection with DuckLake extension ready
      {:ok, conn} = QuackLake.open()

      # Attach a DuckLake
      :ok = QuackLake.attach(conn, "my_lake", "my_lake.ducklake")

      # Query with ergonomic results (returns list of maps)
      {:ok, rows} = QuackLake.query(conn, "SELECT * FROM my_lake.users")

      # Time travel
      {:ok, snapshots} = QuackLake.snapshots(conn, "my_lake")
      {:ok, old_rows} = QuackLake.query_at(conn, "SELECT * FROM my_lake.users", version: 5)

  ## Cloud Storage

      # Configure S3 access
      :ok = QuackLake.Secret.create_s3(conn, "my_s3",
        key_id: "AKIA...", secret: "...", region: "us-east-1")

      # Attach with remote data path
      :ok = QuackLake.attach(conn, "remote_lake", "meta.ducklake",
        data_path: "s3://my-bucket/data/")
  """

  alias QuackLake.{Connection, Lake, Query, Snapshot}

  # Connection

  @doc """
  Opens a DuckDB connection with the DuckLake extension ready.

  ## Options

    * `:path` - Path to the DuckDB database file. Defaults to `nil` (in-memory).
    * `:auto_install_extensions` - Whether to auto-install ducklake. Defaults to `true`.
    * `:auto_load_extensions` - Whether to auto-load ducklake. Defaults to `true`.

  ## Examples

      iex> {:ok, conn} = QuackLake.open()
      iex> is_reference(conn)
      true

      iex> {:ok, conn} = QuackLake.open(path: "data.duckdb")
      iex> is_reference(conn)
      true

  """
  @spec open(keyword()) :: {:ok, reference()} | {:error, term()}
  defdelegate open(opts \\ []), to: Connection

  @doc """
  Opens a DuckDB connection, raising on error.
  """
  @spec open!(keyword()) :: reference()
  defdelegate open!(opts \\ []), to: Connection

  @doc """
  Closes a DuckDB connection.
  """
  @spec close(reference()) :: :ok
  defdelegate close(conn), to: Connection

  # Lake management

  @doc """
  Attaches a DuckLake to the connection.

  ## Options

    * `:data_path` - Path for data storage. Can be local or cloud (s3://, az://, etc.).

  ## Examples

      iex> QuackLake.attach(conn, "my_lake", "my_lake.ducklake")
      :ok

      iex> QuackLake.attach(conn, "my_lake", "meta.ducklake", data_path: "s3://bucket/data/")
      :ok

  """
  @spec attach(reference(), String.t(), String.t(), keyword()) :: :ok | {:error, term()}
  defdelegate attach(conn, name, ducklake_path, opts \\ []), to: Lake

  @doc """
  Detaches a DuckLake from the connection.
  """
  @spec detach(reference(), String.t()) :: :ok | {:error, term()}
  defdelegate detach(conn, name), to: Lake

  @doc """
  Lists all attached DuckLakes.
  """
  @spec lakes(reference()) :: {:ok, [map()]} | {:error, term()}
  def lakes(conn), do: Lake.list(conn)

  # Queries

  @doc """
  Executes a query and returns all results as a list of maps.

  ## Examples

      iex> QuackLake.query(conn, "SELECT 1 as num")
      {:ok, [%{"num" => 1}]}

      iex> QuackLake.query(conn, "SELECT * FROM my_lake.users WHERE id = $1", [1])
      {:ok, [%{"id" => 1, "name" => "Alice"}]}

  """
  @spec query(reference(), String.t(), list()) :: {:ok, [map()]} | {:error, term()}
  def query(conn, sql, params \\ []), do: Query.all(conn, sql, params)

  @doc """
  Executes a query and returns all results, raising on error.
  """
  @spec query!(reference(), String.t(), list()) :: [map()]
  def query!(conn, sql, params \\ []) do
    case query(conn, sql, params) do
      {:ok, rows} -> rows
      {:error, reason} -> raise QuackLake.Error, message: "Query failed", reason: reason
    end
  end

  @doc """
  Executes a query and returns the first result, or nil if no results.
  """
  @spec query_one(reference(), String.t(), list()) :: {:ok, map() | nil} | {:error, term()}
  def query_one(conn, sql, params \\ []), do: Query.one(conn, sql, params)

  @doc """
  Executes a query and returns the first result, raising on error.
  """
  @spec query_one!(reference(), String.t(), list()) :: map() | nil
  def query_one!(conn, sql, params \\ []) do
    case query_one(conn, sql, params) do
      {:ok, row} -> row
      {:error, reason} -> raise QuackLake.Error, message: "Query failed", reason: reason
    end
  end

  # Snapshots / Time Travel

  @doc """
  Lists all snapshots for a DuckLake.

  ## Examples

      iex> QuackLake.snapshots(conn, "my_lake")
      {:ok, [%{"snapshot_id" => 1, ...}]}

  """
  @spec snapshots(reference(), String.t()) :: {:ok, [map()]} | {:error, term()}
  defdelegate snapshots(conn, lake_name), to: Snapshot, as: :list

  @doc """
  Executes a query at a specific snapshot version or timestamp.

  ## Options (one required)

    * `:version` - The snapshot version number to query at.
    * `:timestamp` - The timestamp to query at.

  ## Examples

      iex> QuackLake.query_at(conn, "SELECT * FROM my_lake.users", version: 5)
      {:ok, [%{"id" => 1, "name" => "Alice"}]}

  """
  @spec query_at(reference(), String.t(), keyword()) :: {:ok, [map()]} | {:error, term()}
  defdelegate query_at(conn, sql, opts), to: Snapshot

  @doc """
  Gets changes between two snapshot versions for a specific table.

  ## Examples

      iex> QuackLake.changes(conn, "my_lake", "main", "users", 1, 5)
      {:ok, [%{"change_type" => "INSERT", ...}]}

  """
  @spec changes(reference(), String.t(), String.t(), String.t(), integer(), integer()) ::
          {:ok, [map()]} | {:error, term()}
  defdelegate changes(conn, lake_name, schema_name, table_name, from_version, to_version),
    to: Snapshot
end
