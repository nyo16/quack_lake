defmodule QuackLake.Snapshot do
  @moduledoc """
  Time travel and snapshot management for DuckLake.
  """

  alias QuackLake.Query

  @doc """
  Lists all snapshots for a DuckLake.

  ## Examples

      iex> QuackLake.Snapshot.list(conn, "my_lake")
      {:ok, [%{"snapshot_id" => 1, "snapshot_time" => ~U[2024-01-15 10:00:00Z], ...}]}

  """
  @spec list(Duckdbex.connection(), String.t()) :: {:ok, [map()]} | {:error, term()}
  def list(conn, lake_name) do
    Query.all(conn, "SELECT * FROM ducklake_snapshots('#{escape_string(lake_name)}')")
  end

  @doc """
  Executes a query at a specific snapshot version.

  ## Options (one required)

    * `:version` - The snapshot version number to query at.
    * `:timestamp` - The timestamp to query at (will use the snapshot active at that time).

  ## Examples

      iex> QuackLake.Snapshot.query_at(conn, "SELECT * FROM my_lake.users", version: 5)
      {:ok, [%{"id" => 1, "name" => "Alice"}]}

      iex> QuackLake.Snapshot.query_at(conn, "SELECT * FROM my_lake.users", timestamp: ~U[2024-01-15 10:00:00Z])
      {:ok, [%{"id" => 1, "name" => "Alice"}]}

  """
  @spec query_at(Duckdbex.connection(), String.t(), keyword()) ::
          {:ok, [map()]} | {:error, term()}
  def query_at(conn, sql, opts) do
    cond do
      version = opts[:version] ->
        # Use AT SNAPSHOT syntax
        modified_sql = add_version_clause(sql, version)
        Query.all(conn, modified_sql)

      timestamp = opts[:timestamp] ->
        # Use AT TIMESTAMP syntax
        modified_sql = add_timestamp_clause(sql, timestamp)
        Query.all(conn, modified_sql)

      true ->
        {:error, "Must specify either :version or :timestamp option"}
    end
  end

  @doc """
  Gets changes between two snapshot versions for a specific table.

  Returns rows that were inserted, updated, or deleted between the two versions.

  ## Examples

      iex> QuackLake.Snapshot.changes(conn, "my_lake", "main", "users", 1, 5)
      {:ok, [%{"change_type" => "INSERT", "id" => 2, "name" => "Bob"}]}

  """
  @spec changes(Duckdbex.connection(), String.t(), String.t(), String.t(), integer(), integer()) ::
          {:ok, [map()]} | {:error, term()}
  def changes(conn, lake_name, schema_name, table_name, from_version, to_version) do
    sql = """
    SELECT * FROM ducklake_table_changes('#{escape_string(lake_name)}', '#{escape_string(schema_name)}', '#{escape_string(table_name)}', #{from_version}, #{to_version})
    """

    Query.all(conn, sql)
  end

  @doc """
  Expires (removes) snapshots older than the specified version or timestamp.

  ## Options (one required)

    * `:before_version` - Expire snapshots older than this version.
    * `:before_timestamp` - Expire snapshots older than this timestamp.

  ## Examples

      iex> QuackLake.Snapshot.expire(conn, "my_lake", before_version: 5)
      :ok

  """
  @spec expire(Duckdbex.connection(), String.t(), keyword()) :: :ok | {:error, term()}
  def expire(conn, lake_name, opts) do
    cond do
      version = opts[:before_version] ->
        Query.execute(
          conn,
          "CALL ducklake_expire_snapshots('#{escape_string(lake_name)}', #{version})"
        )

      timestamp = opts[:before_timestamp] ->
        ts_str = format_timestamp(timestamp)

        Query.execute(
          conn,
          "CALL ducklake_expire_snapshots('#{escape_string(lake_name)}', TIMESTAMP '#{ts_str}')"
        )

      true ->
        {:error, "Must specify either :before_version or :before_timestamp option"}
    end
  end

  # Private functions

  defp add_version_clause(sql, version) do
    # DuckLake uses AT SNAPSHOT syntax after table references
    # This is a simplified approach - complex queries may need manual adjustment
    "#{sql} AT SNAPSHOT #{version}"
  end

  defp add_timestamp_clause(sql, timestamp) do
    ts_str = format_timestamp(timestamp)
    "#{sql} AT TIMESTAMP '#{ts_str}'"
  end

  defp format_timestamp(%DateTime{} = dt) do
    DateTime.to_iso8601(dt)
  end

  defp format_timestamp(%NaiveDateTime{} = ndt) do
    NaiveDateTime.to_iso8601(ndt)
  end

  defp format_timestamp(ts) when is_binary(ts), do: ts

  defp escape_string(str) do
    String.replace(str, "'", "''")
  end
end
