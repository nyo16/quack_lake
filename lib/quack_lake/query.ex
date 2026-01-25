defmodule QuackLake.Query do
  @moduledoc """
  Query execution and result transformation for DuckDB.
  """

  @doc """
  Executes a query and returns all results as a list of maps.

  ## Examples

      iex> QuackLake.Query.all(conn, "SELECT 1 as num, 'hello' as greeting")
      {:ok, [%{"num" => 1, "greeting" => "hello"}]}

      iex> QuackLake.Query.all(conn, "SELECT * FROM users WHERE id = $1", [1])
      {:ok, [%{"id" => 1, "name" => "Alice"}]}

  """
  @spec all(Duckdbex.connection(), String.t(), list()) :: {:ok, [map()]} | {:error, term()}
  def all(conn, sql, params \\ []) do
    with {:ok, ref} <- execute_query(conn, sql, params) do
      columns = Duckdbex.columns(ref)
      rows = Duckdbex.fetch_all(ref)
      {:ok, transform_result(columns, rows)}
    end
  end

  @doc """
  Executes a query and returns the first result, or nil if no results.

  ## Examples

      iex> QuackLake.Query.one(conn, "SELECT 1 as num")
      {:ok, %{"num" => 1}}

      iex> QuackLake.Query.one(conn, "SELECT * FROM users WHERE id = $1", [999])
      {:ok, nil}

  """
  @spec one(Duckdbex.connection(), String.t(), list()) :: {:ok, map() | nil} | {:error, term()}
  def one(conn, sql, params \\ []) do
    case all(conn, sql, params) do
      {:ok, [first | _]} -> {:ok, first}
      {:ok, []} -> {:ok, nil}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Executes a SQL statement without returning results.

  Use this for INSERT, UPDATE, DELETE, CREATE TABLE, etc.

  ## Examples

      iex> QuackLake.Query.execute(conn, "CREATE TABLE test (id INT)")
      :ok

      iex> QuackLake.Query.execute(conn, "INSERT INTO test VALUES ($1)", [1])
      :ok

  """
  @spec execute(Duckdbex.connection(), String.t(), list()) :: :ok | {:error, term()}
  def execute(conn, sql, params \\ []) do
    case execute_query(conn, sql, params) do
      {:ok, _ref} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Returns a stream of result chunks for large queries.

  Note: Chunk size is determined by DuckDB internally.

  ## Examples

      QuackLake.Query.stream(conn, "SELECT * FROM large_table")
      |> Stream.each(&process_chunk/1)
      |> Stream.run()

  """
  @spec stream(Duckdbex.connection(), String.t(), keyword()) :: Enumerable.t()
  def stream(conn, sql, _opts \\ []) do
    Stream.resource(
      fn -> init_stream(conn, sql) end,
      &fetch_chunk/1,
      fn _state -> :ok end
    )
  end

  # Private functions

  defp execute_query(conn, sql, []) do
    Duckdbex.query(conn, sql)
  end

  defp execute_query(conn, sql, params) do
    # duckdbex query/3 accepts params directly
    Duckdbex.query(conn, sql, params)
  end

  defp transform_result(columns, rows) do
    Enum.map(rows, fn row ->
      columns
      |> Enum.zip(row)
      |> Map.new()
    end)
  end

  defp init_stream(conn, sql) do
    case Duckdbex.query(conn, sql) do
      {:ok, ref} ->
        columns = Duckdbex.columns(ref)
        {:ok, ref, columns}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp fetch_chunk({:error, _reason} = error), do: {:halt, error}

  defp fetch_chunk({:ok, ref, columns}) do
    case Duckdbex.fetch_chunk(ref) do
      [] ->
        {:halt, {:ok, ref, columns}}

      rows when is_list(rows) ->
        {[transform_result(columns, rows)], {:ok, ref, columns}}
    end
  end
end
