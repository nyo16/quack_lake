defmodule QuackLake.Connection do
  @moduledoc """
  Connection lifecycle management for DuckDB with DuckLake extension.
  """

  alias QuackLake.{Config, Extension}

  @doc """
  Opens a DuckDB connection with the DuckLake extension ready.

  ## Options

    * `:path` - Path to the DuckDB database file. Defaults to `nil` (in-memory).
    * `:auto_install_extensions` - Whether to auto-install ducklake. Defaults to `true`.
    * `:auto_load_extensions` - Whether to auto-load ducklake. Defaults to `true`.

  ## Examples

      iex> {:ok, conn} = QuackLake.Connection.open()
      iex> is_reference(conn)
      true

      iex> {:ok, conn} = QuackLake.Connection.open(path: "data.duckdb")
      iex> is_reference(conn)
      true

  """
  @spec open(keyword()) :: {:ok, Duckdbex.connection()} | {:error, term()}
  def open(opts \\ []) do
    config = Config.new(opts)
    ensure_home_env(config)

    with {:ok, db} <- open_database(config),
         {:ok, conn} <- Duckdbex.connection(db),
         :ok <- set_home_directory(conn, config),
         :ok <- ensure_core_functions(conn, config),
         :ok <- maybe_setup_extensions(conn, config) do
      {:ok, conn}
    end
  end

  @doc """
  Opens a DuckDB connection, raising on error.
  """
  @spec open!(keyword()) :: Duckdbex.connection()
  def open!(opts \\ []) do
    case open(opts) do
      {:ok, conn} ->
        conn

      {:error, reason} ->
        raise QuackLake.Error, message: "Failed to open connection", reason: reason
    end
  end

  @doc """
  Closes a DuckDB connection.

  Note: DuckDB connections are managed by NIFs and will be cleaned up automatically
  when garbage collected. This function is provided for explicit resource management.
  """
  @spec close(Duckdbex.connection()) :: :ok
  def close(_conn) do
    # duckdbex connections are NIF references that clean up on GC
    # There's no explicit close function needed
    :ok
  end

  @doc """
  Executes a raw SQL statement without returning results.

  ## Examples

      iex> QuackLake.Connection.execute(conn, "CREATE TABLE test (id INT)")
      :ok

  """
  @spec execute(Duckdbex.connection(), String.t()) :: :ok | {:error, term()}
  def execute(conn, sql) do
    case Duckdbex.query(conn, sql) do
      {:ok, _ref} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Ensures the `HOME` environment variable points to a valid, writable directory.

  DuckDB reads `HOME` at startup for extension caching. In container environments
  (Docker, Kubernetes), `HOME` may be `/nonexistent` or unset, causing
  `IO Error: Can't find the home directory`.

  This function checks if `HOME` points to an existing directory. If not, it sets
  `HOME` to the resolved home directory from `Config.resolve_home_directory/1`.

  Must be called **before** `Duckdbex.open()`.
  """
  @spec ensure_home_env(Config.t()) :: :ok
  def ensure_home_env(%Config{} = config) do
    case System.get_env("HOME") do
      nil ->
        System.put_env("HOME", Config.resolve_home_directory(config))

      home ->
        unless File.dir?(home) do
          System.put_env("HOME", Config.resolve_home_directory(config))
        end
    end

    :ok
  end

  @doc """
  Sets the `home_directory` DuckDB configuration on an open connection.

  DuckDB uses `home_directory` internally for extension caching and catalog
  operations. This complements `ensure_home_env/1` by also configuring the
  DuckDB runtime after the connection is opened.

  Must be called **after** opening the connection, **before** extensions/attach.
  """
  @spec set_home_directory(Duckdbex.connection(), Config.t()) :: :ok | {:error, term()}
  def set_home_directory(conn, %Config{} = config) do
    home = Config.resolve_home_directory(config)

    case Duckdbex.query(conn, "SET home_directory='#{escape_sql_string(home)}'") do
      {:ok, _ref} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Ensures DuckDB's `core_functions` extension is available on the connection.

  DuckDB 1.5+ (duckdbex >= 0.4) ships core scalar/aggregate functions such
  as `sum` in the separate `core_functions` extension, which is not loaded
  automatically in this build. Honors `:auto_install_extensions` and
  `:auto_load_extensions`: installing falls back to a network fetch when the
  extension is not cached in the DuckDB home directory, and users who disable
  these flags must install/load `core_functions` themselves.
  """
  @spec ensure_core_functions(Duckdbex.connection(), Config.t()) :: :ok | {:error, term()}
  def ensure_core_functions(conn, %Config{
        auto_install_extensions: true,
        auto_load_extensions: true
      }) do
    case Duckdbex.query(conn, "LOAD core_functions") do
      {:ok, _ref} ->
        :ok

      {:error, _reason} ->
        with {:ok, _ref} <- Duckdbex.query(conn, "INSTALL core_functions"),
             {:ok, _ref} <- Duckdbex.query(conn, "LOAD core_functions") do
          :ok
        else
          {:error, reason} -> {:error, {:extension_load, "core_functions", reason}}
        end
    end
  end

  def ensure_core_functions(conn, %Config{
        auto_install_extensions: true,
        auto_load_extensions: false
      }) do
    case Duckdbex.query(conn, "INSTALL core_functions") do
      {:ok, _ref} -> :ok
      {:error, reason} -> {:error, {:extension_install, "core_functions", reason}}
    end
  end

  def ensure_core_functions(conn, %Config{
        auto_install_extensions: false,
        auto_load_extensions: true
      }) do
    case Duckdbex.query(conn, "LOAD core_functions") do
      {:ok, _ref} -> :ok
      {:error, reason} -> {:error, {:extension_load, "core_functions", reason}}
    end
  end

  def ensure_core_functions(_conn, %Config{
        auto_install_extensions: false,
        auto_load_extensions: false
      }) do
    :ok
  end

  # Private functions

  defp escape_sql_string(str) do
    String.replace(str, "'", "''")
  end

  defp open_database(%Config{path: nil}) do
    Duckdbex.open()
  end

  defp open_database(%Config{path: path}) do
    Duckdbex.open(path)
  end

  defp maybe_setup_extensions(conn, %Config{
         auto_install_extensions: true,
         auto_load_extensions: true
       }) do
    Extension.ensure_ducklake(conn)
  end

  defp maybe_setup_extensions(conn, %Config{
         auto_install_extensions: true,
         auto_load_extensions: false
       }) do
    Extension.install(conn, "ducklake")
  end

  defp maybe_setup_extensions(conn, %Config{
         auto_install_extensions: false,
         auto_load_extensions: true
       }) do
    Extension.load(conn, "ducklake")
  end

  defp maybe_setup_extensions(_conn, %Config{
         auto_install_extensions: false,
         auto_load_extensions: false
       }) do
    :ok
  end
end
