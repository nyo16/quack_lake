defmodule QuackLake.DBConnection.Protocol do
  @moduledoc """
  DBConnection protocol implementation for DuckDB.

  This protocol handles the connection lifecycle and query execution
  for DuckDB databases. It enforces single-writer semantics by design.

  ## Connection State

  The protocol maintains:
  - `db` - The DuckDB database reference
  - `conn` - The DuckDB connection reference
  - `transaction_status` - Current transaction state (`:idle`, `:transaction`, `:error`)
  - `config` - The parsed configuration

  ## Usage

  This protocol is used internally by `Ecto.Adapters.DuckDB`. Users
  typically interact through the Ecto repo interface.
  """

  use DBConnection

  alias QuackLake.Config
  alias QuackLake.Config.{Extension, Secret, Attach}
  alias QuackLake.DBConnection.{Query, Result}

  defstruct [:db, :conn, :config, transaction_status: :idle]

  @type t :: %__MODULE__{
          db: reference() | nil,
          conn: reference() | nil,
          config: Config.t() | nil,
          transaction_status: :idle | :transaction | :error
        }

  @impl DBConnection
  def connect(opts) do
    config = Config.from_ecto_opts(opts)

    with {:ok, db} <- open_database(config),
         {:ok, conn} <- Duckdbex.connection(db),
         :ok <- run_initialization(conn, config) do
      {:ok, %__MODULE__{db: db, conn: conn, config: config, transaction_status: :idle}}
    else
      {:error, reason} -> {:error, reason}
    end
  end

  @impl DBConnection
  def disconnect(_err, %__MODULE__{} = _state) do
    # DuckDB connections are managed by NIFs and cleaned up on GC
    :ok
  end

  @impl DBConnection
  def checkout(state) do
    {:ok, state}
  end

  # Note: checkin is no longer a DBConnection callback as of 2.x
  # Keeping for potential backwards compatibility
  def checkin(state) do
    {:ok, state}
  end

  @impl DBConnection
  def ping(%__MODULE__{conn: conn} = state) do
    case Duckdbex.query(conn, "SELECT 1") do
      {:ok, _ref} -> {:ok, state}
      {:error, reason} -> {:disconnect, reason, state}
    end
  end

  @impl DBConnection
  def handle_status(_opts, %__MODULE__{transaction_status: status} = state) do
    {status, state}
  end

  @impl DBConnection
  def handle_prepare(%Query{} = query, _opts, %__MODULE__{} = state) do
    # DuckDB doesn't have a separate prepare step in duckdbex,
    # so we just return the query as-is
    {:ok, query, state}
  end

  @impl DBConnection
  def handle_execute(
        %Query{statement: statement} = query,
        params,
        _opts,
        %__MODULE__{conn: conn} = state
      ) do
    case execute_query(conn, statement, params) do
      {:ok, result} ->
        {:ok, query, query_result(result), state}

      {:error, reason} ->
        # On error during transaction, mark state as error
        new_state =
          if state.transaction_status == :transaction do
            %{state | transaction_status: :error}
          else
            state
          end

        {:error, reason, new_state}
    end
  end

  @impl DBConnection
  def handle_close(_query, _opts, state) do
    # No explicit close needed for DuckDB queries
    {:ok, nil, state}
  end

  @impl DBConnection
  def handle_begin(_opts, %__MODULE__{transaction_status: :idle, conn: conn} = state) do
    case Duckdbex.query(conn, "BEGIN TRANSACTION") do
      {:ok, _ref} ->
        {:ok, Result.empty(:begin), %{state | transaction_status: :transaction}}

      {:error, reason} ->
        {:error, reason, state}
    end
  end

  def handle_begin(_opts, %__MODULE__{transaction_status: status} = state) do
    {:error, "Cannot begin transaction when status is #{status}", state}
  end

  @impl DBConnection
  def handle_commit(_opts, %__MODULE__{transaction_status: :transaction, conn: conn} = state) do
    case Duckdbex.query(conn, "COMMIT") do
      {:ok, _ref} ->
        {:ok, Result.empty(:commit), %{state | transaction_status: :idle}}

      {:error, reason} ->
        {:error, reason, %{state | transaction_status: :error}}
    end
  end

  def handle_commit(_opts, %__MODULE__{transaction_status: status} = state) do
    {:error, "Cannot commit when transaction status is #{status}", state}
  end

  @impl DBConnection
  def handle_rollback(_opts, %__MODULE__{transaction_status: status, conn: conn} = state)
      when status in [:transaction, :error] do
    case Duckdbex.query(conn, "ROLLBACK") do
      {:ok, _ref} ->
        {:ok, Result.empty(:rollback), %{state | transaction_status: :idle}}

      {:error, reason} ->
        # Even if rollback fails, reset to idle
        {:error, reason, %{state | transaction_status: :idle}}
    end
  end

  def handle_rollback(_opts, %__MODULE__{transaction_status: :idle} = state) do
    {:error, "Cannot rollback when not in a transaction", state}
  end

  @impl DBConnection
  def handle_declare(_query, _params, _opts, state) do
    # Cursors not supported
    {:error, "DuckDB adapter does not support cursors", state}
  end

  @impl DBConnection
  def handle_fetch(_query, _cursor, _opts, state) do
    {:error, "DuckDB adapter does not support cursors", state}
  end

  @impl DBConnection
  def handle_deallocate(_query, _cursor, _opts, state) do
    {:error, "DuckDB adapter does not support cursors", state}
  end

  # Private functions

  defp open_database(%Config{} = config) do
    case Config.database_path(config) do
      nil -> Duckdbex.open()
      path -> Duckdbex.open(path)
    end
  end

  defp run_initialization(conn, %Config{} = config) do
    with :ok <- install_extensions(conn, config),
         :ok <- load_extensions(conn, config),
         :ok <- create_secrets(conn, config),
         :ok <- attach_databases(conn, config) do
      :ok
    end
  end

  defp install_extensions(conn, %Config{parsed_extensions: extensions}) do
    Enum.reduce_while(extensions, :ok, fn ext, :ok ->
      if ext.install do
        sql = Extension.install_sql(ext)

        case Duckdbex.query(conn, sql) do
          {:ok, _ref} -> {:cont, :ok}
          {:error, reason} -> {:halt, {:error, {:extension_install, ext.name, reason}}}
        end
      else
        {:cont, :ok}
      end
    end)
  end

  defp load_extensions(conn, %Config{parsed_extensions: extensions}) do
    Enum.reduce_while(extensions, :ok, fn ext, :ok ->
      if ext.load do
        sql = Extension.load_sql(ext)

        case Duckdbex.query(conn, sql) do
          {:ok, _ref} -> {:cont, :ok}
          {:error, reason} -> {:halt, {:error, {:extension_load, ext.name, reason}}}
        end
      else
        {:cont, :ok}
      end
    end)
  end

  defp create_secrets(conn, %Config{parsed_secrets: secrets}) do
    Enum.reduce_while(secrets, :ok, fn secret, :ok ->
      sql = Secret.create_sql(secret)

      case Duckdbex.query(conn, sql) do
        {:ok, _ref} -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, {:secret_create, secret.name, reason}}}
      end
    end)
  end

  defp attach_databases(conn, %Config{parsed_attach: attachments}) do
    Enum.reduce_while(attachments, :ok, fn attach, :ok ->
      sql = Attach.attach_sql(attach)

      case Duckdbex.query(conn, sql) do
        {:ok, _ref} -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, {:attach, attach.alias, reason}}}
      end
    end)
  end

  defp execute_query(conn, statement, []) do
    case Duckdbex.query(conn, statement) do
      {:ok, ref} -> {:ok, ref}
      {:error, reason} -> {:error, reason}
    end
  end

  defp execute_query(conn, statement, params) do
    case Duckdbex.query(conn, statement, params) do
      {:ok, ref} -> {:ok, ref}
      {:error, reason} -> {:error, reason}
    end
  end

  defp query_result(ref) do
    columns = Duckdbex.columns(ref)
    rows = Duckdbex.fetch_all(ref)
    num_rows = length(rows)
    command = detect_command(columns, rows)

    Result.new(command, columns, rows, num_rows)
  end

  defp detect_command(columns, _rows) when is_list(columns) and length(columns) > 0, do: :select
  defp detect_command(_, _), do: :execute
end
