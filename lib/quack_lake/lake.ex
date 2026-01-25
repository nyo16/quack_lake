defmodule QuackLake.Lake do
  @moduledoc """
  DuckLake attach/detach operations.
  """

  alias QuackLake.Query

  @doc """
  Attaches a DuckLake to the connection.

  ## Options

    * `:data_path` - Path for data storage. Can be local or cloud (s3://, az://, etc.).
    * `:metadata_path` - Path for metadata storage (defaults to the ducklake path).

  ## Examples

      iex> QuackLake.Lake.attach(conn, "my_lake", "my_lake.ducklake")
      :ok

      iex> QuackLake.Lake.attach(conn, "my_lake", "meta.ducklake", data_path: "s3://bucket/data/")
      :ok

  """
  @spec attach(Duckdbex.connection(), String.t(), String.t(), keyword()) ::
          :ok | {:error, term()}
  def attach(conn, name, ducklake_path, opts \\ []) do
    data_path = opts[:data_path]

    sql = build_attach_sql(name, ducklake_path, data_path)
    Query.execute(conn, sql)
  end

  @doc """
  Detaches a DuckLake from the connection.

  ## Examples

      iex> QuackLake.Lake.detach(conn, "my_lake")
      :ok

  """
  @spec detach(Duckdbex.connection(), String.t()) :: :ok | {:error, term()}
  def detach(conn, name) do
    Query.execute(conn, "DETACH #{name}")
  end

  @doc """
  Lists all attached DuckLakes.

  ## Examples

      iex> QuackLake.Lake.list(conn)
      {:ok, [%{"name" => "my_lake", "type" => "ducklake"}]}

  """
  @spec list(Duckdbex.connection()) :: {:ok, [map()]} | {:error, term()}
  def list(conn) do
    Query.all(
      conn,
      "SELECT database_name as name, type FROM duckdb_databases() WHERE type = 'ducklake'"
    )
  end

  # Private functions

  defp build_attach_sql(name, ducklake_path, nil) do
    "ATTACH '#{escape_string(ducklake_path)}' AS #{name} (TYPE DUCKLAKE)"
  end

  defp build_attach_sql(name, ducklake_path, data_path) do
    "ATTACH '#{escape_string(ducklake_path)}' AS #{name} (TYPE DUCKLAKE, DATA_PATH '#{escape_string(data_path)}')"
  end

  defp escape_string(str) do
    String.replace(str, "'", "''")
  end
end
