defmodule TestApp.LakeServer do
  @moduledoc """
  Supervised GenServer wrapper for QuackLake connections.

  Demonstrates production-ready patterns:
  - Automatic restart on failure
  - Query/execute through GenServer
  - S3 and lake attachment on init
  """

  use GenServer

  # Client API

  def start_link(opts \\ []) do
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

  def snapshots(server \\ __MODULE__, lake_name) do
    GenServer.call(server, {:snapshots, lake_name})
  end

  # Server callbacks

  @impl true
  def init(opts) do
    case setup_connection(opts) do
      {:ok, conn, lake_name} ->
        {:ok, %{conn: conn, lake_name: lake_name, opts: opts}}

      {:error, reason} ->
        {:stop, reason}
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

  def handle_call({:snapshots, lake_name}, _from, %{conn: conn} = state) do
    {:reply, QuackLake.snapshots(conn, lake_name), state}
  end

  # Private helpers

  defp setup_connection(opts) do
    s3_config = Application.get_env(:test_app, :s3, [])
    pg_config = Application.get_env(:test_app, :postgres, [])
    lake_name = Keyword.get(opts, :lake_name, "demo_lake")
    data_path = Keyword.get(opts, :data_path)

    with {:ok, conn} <- QuackLake.open(),
         :ok <- setup_s3_secret(conn, s3_config),
         :ok <- setup_lake(conn, lake_name, pg_config, data_path) do
      {:ok, conn, lake_name}
    end
  end

  defp setup_s3_secret(_conn, []), do: :ok

  defp setup_s3_secret(conn, s3_config) do
    endpoint = s3_config[:endpoint] |> String.replace(~r{^https?://}, "")

    QuackLake.Secret.create_s3(conn, "s3_creds",
      key_id: s3_config[:access_key_id],
      secret: s3_config[:secret_access_key],
      region: s3_config[:region] || "us-east-1",
      endpoint: endpoint,
      use_ssl: false,
      url_style: "path"
    )
  end

  defp setup_lake(conn, lake_name, pg_config, data_path) do
    catalog_string =
      "postgres:host=#{pg_config[:host]};port=#{pg_config[:port]};database=#{pg_config[:database]};user=#{pg_config[:username]};password=#{pg_config[:password]}"

    attach_sql =
      if data_path do
        "ATTACH '#{catalog_string};ducklake_alias=#{lake_name}' AS #{lake_name} (TYPE DUCKLAKE, DATA_PATH '#{data_path}')"
      else
        "ATTACH '#{catalog_string};ducklake_alias=#{lake_name}' AS #{lake_name} (TYPE DUCKLAKE)"
      end

    case QuackLake.Query.execute(conn, attach_sql) do
      :ok -> :ok
      {:error, reason} -> {:error, {:lake_attach, reason}}
    end
  end
end
