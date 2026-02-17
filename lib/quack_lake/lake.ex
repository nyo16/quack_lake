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
    * `:metadata_schema` - Schema in the catalog database for DuckLake metadata tables.

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
    metadata_schema = opts[:metadata_schema]

    sql = build_attach_sql(name, ducklake_path, data_path, metadata_schema)
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

  # When the path starts with "ducklake:", DuckDB infers the type automatically.
  # Adding TYPE DUCKLAKE in this case causes the extension to double-parse the
  # connection string and silently drop other options like DATA_PATH.
  defp build_attach_sql(name, ducklake_path, data_path, metadata_schema) do
    opts =
      [
        type_opt(ducklake_path),
        data_path_opt(data_path),
        metadata_schema_opt(metadata_schema)
      ]
      |> Enum.reject(&is_nil/1)
      |> Enum.join(", ")

    case opts do
      "" -> "ATTACH '#{escape_string(ducklake_path)}' AS #{name}"
      _ -> "ATTACH '#{escape_string(ducklake_path)}' AS #{name} (#{opts})"
    end
  end

  defp type_opt("ducklake:" <> _), do: nil
  defp type_opt(_), do: "TYPE DUCKLAKE"

  defp data_path_opt(nil), do: nil
  defp data_path_opt(path), do: "DATA_PATH '#{escape_string(path)}'"

  defp metadata_schema_opt(nil), do: nil
  defp metadata_schema_opt(schema), do: "METADATA_SCHEMA '#{escape_string(schema)}'"

  defp escape_string(str) do
    String.replace(str, "'", "''")
  end
end
