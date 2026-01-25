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

    with {:ok, db} <- open_database(config),
         {:ok, conn} <- Duckdbex.connection(db),
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

  # Private functions

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
