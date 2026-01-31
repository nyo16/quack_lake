defmodule Ecto.Adapters.DuckLake do
  @moduledoc """
  Ecto adapter for DuckLake (concurrent writers supported).

  This adapter provides Ecto integration with DuckLake, DuckDB's lakehouse
  format. Unlike the standard DuckDB adapter, DuckLake supports concurrent
  writers with configurable pool sizes.

  ## Configuration

      config :my_app, MyApp.LakeRepo,
        adapter: Ecto.Adapters.DuckLake,
        database: "ducklake:my_lake.ducklake",
        pool_size: 5,
        data_path: "s3://my-bucket/lake-data",
        extensions: [:httpfs, {:ducklake, source: :core}],
        secrets: [
          {:my_s3, [type: :s3, key_id: "...", secret: "...", region: "us-east-1"]}
        ]

  ## Options

    * `:database` - Path to the DuckLake. Use `ducklake:` prefix for lakehouse format.
    * `:pool_size` - Connection pool size. Defaults to `5`.
    * `:data_path` - Storage path for lakehouse data (S3, Azure, GCS, local).
    * `:extensions` - List of extensions to install and load.
    * `:secrets` - List of secrets for cloud storage access.
    * `:attach` - List of additional databases to attach.

  ## Concurrent Writers

  DuckLake supports multiple concurrent writers because it uses a
  lakehouse architecture with optimistic concurrency control:

      # Multiple processes can write concurrently
      Task.async_stream(records, fn record ->
        MyApp.LakeRepo.insert!(record)
      end, max_concurrency: 5)

  ## Time Travel

  DuckLake supports querying historical snapshots via raw SQL:

      # Query at a specific version
      MyApp.LakeRepo.query!("SELECT * FROM users AT SNAPSHOT 42")

      # Query at a specific timestamp
      MyApp.LakeRepo.query!("SELECT * FROM users AT TIMESTAMP '2024-01-15'")

  ## Example Usage

      # Define a repo
      defmodule MyApp.LakeRepo do
        use Ecto.Repo,
          otp_app: :my_app,
          adapter: Ecto.Adapters.DuckLake

        # Optionally add raw query support
        use Ecto.Adapters.DuckDB.RawQuery
      end

      # Define a schema
      defmodule MyApp.Event do
        use Ecto.Schema

        schema "events" do
          field :type, :string
          field :data, :map
          field :timestamp, :utc_datetime
        end
      end

      # Concurrent inserts
      events
      |> Task.async_stream(&MyApp.LakeRepo.insert!/1, max_concurrency: 5)
      |> Stream.run()

  """

  use Ecto.Adapters.SQL, driver: QuackLake.DBConnection.LakeProtocol

  @behaviour Ecto.Adapter.Storage
  @behaviour Ecto.Adapter.Structure

  @impl Ecto.Adapter
  def ensure_all_started(_config, _type) do
    {:ok, []}
  end

  @doc false
  def default_opts(_repo, config) do
    # Allow configurable pool_size (default: 5) for concurrent writers
    Keyword.put_new(config, :pool_size, 5)
  end

  @impl Ecto.Adapter
  def loaders(:boolean, type), do: [&decode_boolean/1, type]
  def loaders(:binary_id, type), do: [type]
  def loaders(Ecto.UUID, type), do: [type]
  def loaders(:uuid, type), do: [type]
  def loaders(:date, type), do: [&decode_date/1, type]
  def loaders(:time, type), do: [&decode_time/1, type]
  def loaders(:naive_datetime, type), do: [&decode_naive_datetime/1, type]
  def loaders(:naive_datetime_usec, type), do: [&decode_naive_datetime/1, type]
  def loaders(:utc_datetime, type), do: [&decode_utc_datetime/1, type]
  def loaders(:utc_datetime_usec, type), do: [&decode_utc_datetime/1, type]
  def loaders(:map, type), do: [&decode_json/1, type]
  def loaders({:map, _}, type), do: [&decode_json/1, type]
  def loaders(_primitive, type), do: [type]

  @impl Ecto.Adapter
  def dumpers(:boolean, type), do: [type, &encode_boolean/1]
  def dumpers(:binary_id, type), do: [type, &encode_uuid/1]
  def dumpers(Ecto.UUID, type), do: [type, &encode_uuid/1]
  def dumpers(:uuid, type), do: [type, &encode_uuid/1]
  def dumpers(:date, type), do: [type]
  def dumpers(:time, type), do: [type]
  def dumpers(:naive_datetime, type), do: [type, &encode_naive_datetime/1]
  def dumpers(:naive_datetime_usec, type), do: [type, &encode_naive_datetime/1]
  def dumpers(:utc_datetime, type), do: [type, &encode_utc_datetime/1]
  def dumpers(:utc_datetime_usec, type), do: [type, &encode_utc_datetime/1]
  def dumpers(:decimal, type), do: [type, &encode_decimal/1]
  def dumpers(:map, type), do: [type, &encode_json/1]
  def dumpers({:map, _}, type), do: [type, &encode_json/1]
  def dumpers(_primitive, type), do: [type]

  # Storage callbacks

  @impl Ecto.Adapter.Storage
  def storage_up(opts) do
    database = Keyword.fetch!(opts, :database)
    data_path = opts[:data_path]

    # Parse ducklake: prefix
    lake_path = parse_lake_path(database)

    if lake_path && !lake_exists?(lake_path) do
      case create_lake(lake_path, data_path) do
        :ok -> :ok
        {:error, reason} -> {:error, reason}
      end
    else
      {:error, :already_up}
    end
  end

  @impl Ecto.Adapter.Storage
  def storage_down(opts) do
    database = Keyword.fetch!(opts, :database)
    lake_path = parse_lake_path(database)

    if lake_path && lake_exists?(lake_path) do
      # For local lakes, remove the file
      # For remote lakes, this would require more complex cleanup
      if File.exists?(lake_path) do
        File.rm(lake_path)
        :ok
      else
        {:error, :cannot_drop_remote}
      end
    else
      {:error, :already_down}
    end
  end

  @impl Ecto.Adapter.Storage
  def storage_status(opts) do
    database = Keyword.get(opts, :database)
    lake_path = parse_lake_path(database)

    cond do
      # In-memory or default
      is_nil(lake_path) -> :up
      lake_exists?(lake_path) -> :up
      true -> :down
    end
  end

  # Structure callbacks

  @impl Ecto.Adapter.Structure
  def structure_dump(_default, _config) do
    {:error, "DuckLake adapter does not support structure_dump. Use snapshots for versioning."}
  end

  @impl Ecto.Adapter.Structure
  def structure_load(_default, _config) do
    {:error, "DuckLake adapter does not support structure_load. Use snapshots for versioning."}
  end

  @impl Ecto.Adapter.Structure
  def dump_cmd(_args, _opts, _config) do
    raise "DuckLake adapter does not support dump_cmd"
  end

  # Migration callbacks

  @impl Ecto.Adapter.Migration
  def supports_ddl_transaction?, do: false

  @impl Ecto.Adapter.Migration
  def lock_for_migrations(_meta, _opts, fun) do
    # DuckLake supports concurrent writers, so no locking needed
    fun.()
  end

  # Lake helpers

  defp parse_lake_path("ducklake:" <> path), do: path
  defp parse_lake_path(path), do: path

  defp lake_exists?(path) do
    cond do
      # Local file - check filesystem
      File.exists?(path) -> true
      # Remote paths (S3, Azure, GCS) - we cannot easily check existence,
      # so assume they exist if configured to avoid always recreating
      String.starts_with?(path, "s3://") -> true
      String.starts_with?(path, "az://") -> true
      String.starts_with?(path, "gs://") -> true
      # Local path that doesn't exist
      true -> false
    end
  end

  defp create_lake(lake_path, data_path) do
    # Open a temporary connection to create the lake
    case Duckdbex.open() do
      {:ok, db} ->
        {:ok, conn} = Duckdbex.connection(db)

        # Install and load ducklake extension
        with {:ok, _} <- Duckdbex.query(conn, "INSTALL ducklake FROM core"),
             {:ok, _} <- Duckdbex.query(conn, "LOAD ducklake"),
             :ok <- do_create_lake(conn, lake_path, data_path) do
          :ok
        else
          {:error, reason} -> {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp do_create_lake(conn, lake_path, nil) do
    # Create lake without explicit data path
    sql = "ATTACH '#{escape_string(lake_path)}' AS new_lake (TYPE DUCKLAKE)"

    case Duckdbex.query(conn, sql) do
      {:ok, _} ->
        Duckdbex.query(conn, "DETACH new_lake")
        :ok

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp do_create_lake(conn, lake_path, data_path) do
    sql = """
    ATTACH '#{escape_string(lake_path)}' AS new_lake
    (TYPE DUCKLAKE, DATA_PATH '#{escape_string(data_path)}')
    """

    case Duckdbex.query(conn, sql) do
      {:ok, _} ->
        Duckdbex.query(conn, "DETACH new_lake")
        :ok

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp escape_string(str) do
    String.replace(str, "'", "''")
  end

  # Decoder helpers (same as DuckDB adapter)

  defp decode_boolean(0), do: {:ok, false}
  defp decode_boolean(1), do: {:ok, true}
  defp decode_boolean("0"), do: {:ok, false}
  defp decode_boolean("1"), do: {:ok, true}
  defp decode_boolean(false), do: {:ok, false}
  defp decode_boolean(true), do: {:ok, true}
  defp decode_boolean(nil), do: {:ok, nil}
  defp decode_boolean(other), do: {:ok, other}

  defp decode_date({year, month, day}), do: {:ok, Date.new!(year, month, day)}
  defp decode_date(%Date{} = date), do: {:ok, date}
  defp decode_date(nil), do: {:ok, nil}
  defp decode_date(other), do: {:ok, other}

  defp decode_time({hour, minute, second}), do: {:ok, Time.new!(hour, minute, second)}
  defp decode_time(%Time{} = time), do: {:ok, time}
  defp decode_time(nil), do: {:ok, nil}
  defp decode_time(other), do: {:ok, other}

  defp decode_naive_datetime({{y, m, d}, {h, mi, s}}),
    do: {:ok, NaiveDateTime.new!(y, m, d, h, mi, s)}

  defp decode_naive_datetime({{y, m, d}, {h, mi, s, us}}),
    do: {:ok, NaiveDateTime.new!(y, m, d, h, mi, s, {us, 6})}

  defp decode_naive_datetime(%NaiveDateTime{} = ndt), do: {:ok, ndt}
  defp decode_naive_datetime(nil), do: {:ok, nil}
  defp decode_naive_datetime(other), do: {:ok, other}

  defp decode_utc_datetime({{y, m, d}, {h, mi, s}}) do
    ndt = NaiveDateTime.new!(y, m, d, h, mi, s)
    {:ok, DateTime.from_naive!(ndt, "Etc/UTC")}
  end

  defp decode_utc_datetime({{y, m, d}, {h, mi, s, us}}) do
    ndt = NaiveDateTime.new!(y, m, d, h, mi, s, {us, 6})
    {:ok, DateTime.from_naive!(ndt, "Etc/UTC")}
  end

  defp decode_utc_datetime(%DateTime{} = dt), do: {:ok, dt}
  defp decode_utc_datetime(nil), do: {:ok, nil}
  defp decode_utc_datetime(other), do: {:ok, other}

  defp decode_json(nil), do: {:ok, nil}

  defp decode_json(value) when is_binary(value) do
    case json_decode(value) do
      {:ok, decoded} -> {:ok, decoded}
      {:error, _} -> {:ok, value}
    end
  end

  defp decode_json(value) when is_map(value), do: {:ok, value}
  defp decode_json(other), do: {:ok, other}

  # Encoder helpers

  defp encode_boolean(true), do: {:ok, true}
  defp encode_boolean(false), do: {:ok, false}
  defp encode_boolean(nil), do: {:ok, nil}
  defp encode_boolean(other), do: {:ok, other}

  defp encode_uuid(nil), do: {:ok, nil}

  defp encode_uuid(<<_::128>> = uuid) do
    {:ok, Ecto.UUID.cast!(uuid)}
  end

  defp encode_uuid(uuid) when is_binary(uuid), do: {:ok, uuid}
  defp encode_uuid(other), do: {:ok, other}

  defp encode_naive_datetime(%NaiveDateTime{} = ndt) do
    {:ok, NaiveDateTime.to_iso8601(ndt)}
  end

  defp encode_naive_datetime(nil), do: {:ok, nil}
  defp encode_naive_datetime(other), do: {:ok, other}

  defp encode_utc_datetime(%DateTime{} = dt) do
    {:ok, NaiveDateTime.to_iso8601(DateTime.to_naive(dt))}
  end

  defp encode_utc_datetime(other), do: {:ok, other}

  defp encode_decimal(%Decimal{} = d), do: {:ok, Decimal.to_float(d)}
  defp encode_decimal(nil), do: {:ok, nil}
  defp encode_decimal(other), do: {:ok, other}

  defp encode_json(nil), do: {:ok, nil}

  defp encode_json(value) when is_map(value) do
    case json_encode(value) do
      {:ok, encoded} -> {:ok, encoded}
      {:error, _} -> {:ok, value}
    end
  end

  defp encode_json(other), do: {:ok, other}

  # JSON helpers - use built-in JSON (Elixir 1.18+) or Erlang :json (OTP 27+)

  defp json_encode(value) do
    cond do
      Code.ensure_loaded?(JSON) ->
        {:ok, JSON.encode!(value)}

      true ->
        {:ok, :json.encode(value) |> IO.iodata_to_binary()}
    end
  rescue
    _ -> {:error, :encode_failed}
  end

  defp json_decode(value) do
    cond do
      Code.ensure_loaded?(JSON) ->
        {:ok, JSON.decode!(value)}

      true ->
        {:ok, :json.decode(value)}
    end
  rescue
    _ -> {:error, :decode_failed}
  end
end
