defmodule QuackLake.Appender do
  @moduledoc """
  High-performance bulk insert API using DuckDB's Appender.

  The Appender is significantly faster than INSERT statements for bulk data
  loading (10-100x faster for large datasets). It batches rows and flushes
  them efficiently to the database.

  ## How It Works

  The Appender buffers rows in memory and writes them in optimized batches
  to the underlying storage. This avoids the overhead of parsing and planning
  individual INSERT statements.

  ## Usage

      {:ok, conn} = QuackLake.open()

      # Create an appender for a table
      {:ok, appender} = QuackLake.Appender.new(conn, "users")

      # Append rows (must match column order)
      QuackLake.Appender.append(appender, [1, "Alice", "alice@example.com"])
      QuackLake.Appender.append(appender, [2, "Bob", "bob@example.com"])

      # Or append multiple rows at once
      QuackLake.Appender.append_rows(appender, [
        [3, "Carol", "carol@example.com"],
        [4, "Dave", "dave@example.com"]
      ])

      # Close flushes remaining rows
      QuackLake.Appender.close(appender)

  ## Transaction Semantics

  - Appenders write data directly to the table (no transaction needed)
  - Data is visible to other queries after `flush/1` or `close/1`
  - If the process crashes before close, buffered data is lost

  ## Performance Tips

  1. **Batch size**: Append many rows before flushing
  2. **Column order**: Ensure row values match table column order exactly
  3. **Type matching**: Provide values in the correct type (avoids conversions)
  4. **Streaming**: For very large files, flush periodically to avoid memory issues

  ## When to Use Appender vs INSERT

  | Scenario | Recommendation |
  |----------|----------------|
  | Loading > 10,000 rows | Use Appender |
  | Single row insert | Use INSERT |
  | Need transaction rollback | Use INSERT in transaction |
  | Streaming data processing | Use Appender with periodic flush |

  """

  defstruct [:ref, :table, :conn]

  @type t :: %__MODULE__{
          ref: reference(),
          table: String.t(),
          conn: reference()
        }

  @doc """
  Creates an appender for a table.

  The table must exist and have a defined schema. Values appended
  must match the column order and types of the table.

  ## Examples

      {:ok, appender} = QuackLake.Appender.new(conn, "users")

      # With schema prefix
      {:ok, appender} = QuackLake.Appender.new(conn, "analytics.events")

  """
  @spec new(reference(), String.t()) :: {:ok, t()} | {:error, term()}
  def new(conn, table) when is_reference(conn) and is_binary(table) do
    case Duckdbex.appender(conn, table) do
      {:ok, ref} ->
        {:ok, %__MODULE__{ref: ref, table: table, conn: conn}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Creates an appender for a table, raising on error.

  ## Examples

      appender = QuackLake.Appender.new!(conn, "users")

  """
  @spec new!(reference(), String.t()) :: t()
  def new!(conn, table) do
    case new(conn, table) do
      {:ok, appender} ->
        appender

      {:error, reason} ->
        raise QuackLake.Error, message: "Failed to create appender", reason: reason
    end
  end

  @doc """
  Appends a single row to the appender.

  The row must be a list of values matching the table's column order.

  ## Examples

      :ok = QuackLake.Appender.append(appender, [1, "Alice", "alice@example.com"])

  """
  @spec append(t(), [term()]) :: :ok | {:error, term()}
  def append(%__MODULE__{ref: ref}, row) when is_list(row) do
    Duckdbex.appender_add_row(ref, row)
  end

  @doc """
  Appends multiple rows at once.

  More efficient than calling `append/2` in a loop for small batches.

  ## Examples

      rows = [
        [1, "Alice", "alice@example.com"],
        [2, "Bob", "bob@example.com"],
        [3, "Carol", "carol@example.com"]
      ]
      :ok = QuackLake.Appender.append_rows(appender, rows)

  """
  @spec append_rows(t(), [[term()]]) :: :ok | {:error, term()}
  def append_rows(%__MODULE__{ref: ref}, rows) when is_list(rows) do
    Duckdbex.appender_add_rows(ref, rows)
  end

  @doc """
  Flushes buffered rows to the database.

  Rows are automatically flushed when the appender is closed,
  but you can flush manually for checkpointing during large imports.

  After flush, the appended data becomes visible to other queries.

  ## Examples

      # Flush every 10,000 rows for progress tracking
      records
      |> Stream.with_index()
      |> Enum.each(fn {record, idx} ->
        QuackLake.Appender.append(appender, record)

        if rem(idx, 10_000) == 0 do
          QuackLake.Appender.flush(appender)
          IO.puts("Flushed \#{idx} rows")
        end
      end)

  """
  @spec flush(t()) :: :ok | {:error, term()}
  def flush(%__MODULE__{ref: ref}) do
    Duckdbex.appender_flush(ref)
  end

  @doc """
  Closes the appender and flushes any remaining rows.

  Always call this when done appending to ensure all data is written.
  After closing, the appender cannot be used again.

  ## Examples

      QuackLake.Appender.close(appender)

  """
  @spec close(t()) :: :ok | {:error, term()}
  def close(%__MODULE__{ref: ref}) do
    Duckdbex.appender_close(ref)
  end

  @doc """
  Returns the table name for this appender.

  ## Examples

      "users" = QuackLake.Appender.table(appender)

  """
  @spec table(t()) :: String.t()
  def table(%__MODULE__{table: table}), do: table
end
