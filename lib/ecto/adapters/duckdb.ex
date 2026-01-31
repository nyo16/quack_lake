defmodule Ecto.Adapters.DuckDB do
  @moduledoc """
  Ecto adapter for DuckDB (single-writer mode).

  This adapter provides Ecto integration with DuckDB databases.
  It enforces a pool size of 1 since DuckDB only supports a single
  writer at a time for local database files.

  For concurrent writes, use `Ecto.Adapters.DuckLake` with a DuckLake
  lakehouse backend.

  ## Configuration

      config :my_app, MyApp.Repo,
        adapter: Ecto.Adapters.DuckDB,
        database: "priv/analytics.duckdb",
        extensions: [:httpfs, :parquet, {:spatial, source: :core}],
        secrets: [
          {:my_s3, [type: :s3, key_id: "...", secret: "...", region: "us-east-1"]}
        ]

  ## Options

    * `:database` - Path to the DuckDB database file. Use `nil` for in-memory.
    * `:extensions` - List of extensions to install and load.
    * `:secrets` - List of secrets for cloud storage access.
    * `:attach` - List of databases to attach on connection.
    * `:pool_size` - **Ignored**. Always set to 1 for DuckDB.

  ## Extensions

  Extensions can be specified as atoms or tuples:

      extensions: [
        :httpfs,                          # Install from default repository
        {:spatial, source: :core},        # Install from core
        {:my_ext, source: :community},    # Install from community
        {:custom, source: "https://..."}  # Install from URL
      ]

  ## Secrets

  Secrets provide credentials for cloud storage:

      secrets: [
        {:my_s3, [
          type: :s3,
          key_id: System.get_env("AWS_ACCESS_KEY_ID"),
          secret: System.get_env("AWS_SECRET_ACCESS_KEY"),
          region: "us-east-1"
        ]}
      ]

  ## Example Usage

      # Define a repo
      defmodule MyApp.Repo do
        use Ecto.Repo,
          otp_app: :my_app,
          adapter: Ecto.Adapters.DuckDB
      end

      # Define a schema
      defmodule MyApp.User do
        use Ecto.Schema

        schema "users" do
          field :name, :string
          field :email, :string
          timestamps()
        end
      end

      # CRUD operations
      MyApp.Repo.insert!(%User{name: "Alice"})
      MyApp.Repo.all(User)
      MyApp.Repo.get!(User, 1)

  """

  use Ecto.Adapters.SQL, driver: QuackLake.DBConnection.Protocol

  @behaviour Ecto.Adapter.Storage
  @behaviour Ecto.Adapter.Structure

  @impl Ecto.Adapter
  def ensure_all_started(_config, _type) do
    {:ok, []}
  end

  @doc false
  def default_opts(_repo, config) do
    # Enforce pool_size: 1 for DuckDB (single writer)
    Keyword.put(config, :pool_size, 1)
  end

  @impl Ecto.Adapter
  def loaders(:boolean, type), do: [&decode_boolean/1, type]
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
  def dumpers(:date, type), do: [type]
  def dumpers(:time, type), do: [type]
  def dumpers(:naive_datetime, type), do: [type]
  def dumpers(:naive_datetime_usec, type), do: [type]
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

    if database && !File.exists?(database) do
      # Create the database file by opening and closing a connection
      case Duckdbex.open(database) do
        {:ok, _db} -> :ok
        {:error, reason} -> {:error, reason}
      end
    else
      {:error, :already_up}
    end
  end

  @impl Ecto.Adapter.Storage
  def storage_down(opts) do
    database = Keyword.fetch!(opts, :database)

    if database && File.exists?(database) do
      File.rm(database)
      :ok
    else
      {:error, :already_down}
    end
  end

  @impl Ecto.Adapter.Storage
  def storage_status(opts) do
    database = Keyword.get(opts, :database)

    cond do
      # In-memory is always "up"
      is_nil(database) -> :up
      File.exists?(database) -> :up
      true -> :down
    end
  end

  # Structure callbacks

  @impl Ecto.Adapter.Structure
  def structure_dump(default, config) do
    database = Keyword.fetch!(config, :database)

    case Duckdbex.open(database) do
      {:ok, db} ->
        {:ok, conn} = Duckdbex.connection(db)

        case Duckdbex.query(conn, "EXPORT DATABASE '#{default}' (FORMAT PARQUET)") do
          {:ok, _ref} -> {:ok, default}
          {:error, reason} -> {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  @impl Ecto.Adapter.Structure
  def structure_load(default, config) do
    database = Keyword.fetch!(config, :database)

    case Duckdbex.open(database) do
      {:ok, db} ->
        {:ok, conn} = Duckdbex.connection(db)

        case Duckdbex.query(conn, "IMPORT DATABASE '#{default}'") do
          {:ok, _ref} -> {:ok, default}
          {:error, reason} -> {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  @impl Ecto.Adapter.Structure
  def dump_cmd(_args, _opts, _config) do
    raise "DuckDB adapter does not support dump_cmd"
  end

  # Migration callbacks

  @impl Ecto.Adapter.Migration
  def supports_ddl_transaction?, do: false

  @impl Ecto.Adapter.Migration
  def lock_for_migrations(_meta, _opts, fun) do
    # DuckDB has single-writer semantics, so no explicit locking needed
    fun.()
  end

  # Decoder helpers

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

  defp encode_utc_datetime(%DateTime{} = dt) do
    {:ok, DateTime.to_naive(dt)}
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
