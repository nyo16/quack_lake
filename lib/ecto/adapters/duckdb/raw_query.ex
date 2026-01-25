defmodule Ecto.Adapters.DuckDB.RawQuery do
  @moduledoc """
  Raw SQL execution and Appender support for DuckDB Ecto repos.

  This module provides additional functions that can be added to your
  Ecto repo for executing raw SQL and using DuckDB's high-performance
  Appender API for bulk inserts.

  ## Usage

      defmodule MyApp.Repo do
        use Ecto.Repo,
          otp_app: :my_app,
          adapter: Ecto.Adapters.DuckDB

        use Ecto.Adapters.DuckDB.RawQuery
      end

  This adds the following functions to your repo:

    * `exec!/2` - Execute raw SQL (supports multi-statement)
    * `exec/2` - Execute raw SQL with error tuple return
    * `appender/1` - Create a high-performance appender for bulk inserts
    * `append/2` - Append a record to an appender
    * `flush_appender/1` - Flush buffered rows to disk
    * `close_appender/1` - Close appender and flush remaining rows

  ## Raw SQL Execution

      # Single statement
      MyApp.Repo.exec!("CREATE TABLE test (id INT, name VARCHAR)")

      # Multi-statement execution
      MyApp.Repo.exec!(~s'''
        CREATE TEMP TABLE staging AS SELECT * FROM read_csv('data.csv');
        INSERT INTO users SELECT * FROM staging WHERE valid = true;
        DROP TABLE staging;
      ''')

      # COPY operations
      MyApp.Repo.exec!("COPY users TO 'users.parquet' (FORMAT PARQUET)")

  ## Appender API (Bulk Inserts)

  The Appender API is optimized for high-performance bulk inserts,
  significantly faster than regular INSERT statements for large datasets.

      # Create appender for a schema
      {:ok, appender} = MyApp.Repo.appender(User)

      # Append records
      Enum.each(users, fn user ->
        MyApp.Repo.append(appender, user)
      end)

      # Close and flush
      MyApp.Repo.close_appender(appender)

  For processing large files with progress tracking:

      {:ok, appender} = MyApp.Repo.appender(Event)

      File.stream!("events.csv")
      |> CSV.decode!(headers: true)
      |> Stream.with_index()
      |> Enum.each(fn {row, idx} ->
        MyApp.Repo.append(appender, Event, row)

        if rem(idx, 10_000) == 0 do
          MyApp.Repo.flush_appender(appender)
          IO.puts("Processed \#{idx} rows...")
        end
      end)

      MyApp.Repo.close_appender(appender)

  """

  @doc false
  defmacro __using__(_opts) do
    quote do
      @doc """
      Executes raw SQL and returns the result.

      Supports multi-statement SQL separated by semicolons.

      ## Options

        * `:timeout` - Query timeout in milliseconds

      ## Examples

          MyApp.Repo.exec!("CREATE TABLE test (id INT)")
          MyApp.Repo.exec!("COPY users TO 'users.parquet' (FORMAT PARQUET)")

      """
      @spec exec!(String.t(), keyword()) :: QuackLake.DBConnection.Result.t()
      def exec!(sql, opts \\ []) do
        case exec(sql, opts) do
          {:ok, result} ->
            result

          {:error, reason} ->
            raise Ecto.QueryError, message: "Raw query failed: #{inspect(reason)}"
        end
      end

      @doc """
      Executes raw SQL and returns `{:ok, result}` or `{:error, reason}`.

      ## Examples

          {:ok, result} = MyApp.Repo.exec("SELECT * FROM users")
          {:error, reason} = MyApp.Repo.exec("INVALID SQL")

      """
      @spec exec(String.t(), keyword()) ::
              {:ok, QuackLake.DBConnection.Result.t()} | {:error, term()}
      def exec(sql, opts \\ []) do
        # Get pool from repo config
        query = %QuackLake.DBConnection.Query{statement: sql}
        timeout = Keyword.get(opts, :timeout, 15_000)

        __MODULE__
        |> Ecto.Adapter.lookup_meta()
        |> Map.get(:pid)
        |> DBConnection.execute(query, [], timeout: timeout)
      end

      @doc """
      Creates a high-performance appender for bulk inserts.

      The Appender API is significantly faster than INSERT statements
      for bulk data loading (10-100x faster for large datasets).

      ## Examples

          {:ok, appender} = MyApp.Repo.appender(User)

          Enum.each(users, fn user ->
            MyApp.Repo.append(appender, user)
          end)

          MyApp.Repo.close_appender(appender)

      """
      @spec appender(module()) :: {:ok, QuackLake.Appender.t()} | {:error, term()}
      def appender(schema) when is_atom(schema) do
        Ecto.Adapters.DuckDB.Appender.new(__MODULE__, schema)
      end

      @doc """
      Appends a struct or map to an appender.

      When passing a struct, fields are extracted in schema order.
      When passing a map with a schema, fields are extracted by key.

      ## Examples

          # Append a struct
          MyApp.Repo.append(appender, %User{name: "Alice", email: "alice@example.com"})

          # Append a map with schema
          MyApp.Repo.append(appender, User, %{name: "Bob", email: "bob@example.com"})

      """
      @spec append(QuackLake.Appender.t(), struct()) :: :ok | {:error, term()}
      def append(appender, %{__struct__: _} = struct) do
        Ecto.Adapters.DuckDB.Appender.append(appender, struct)
      end

      @spec append(QuackLake.Appender.t(), module(), map()) :: :ok | {:error, term()}
      def append(appender, schema, map) when is_atom(schema) and is_map(map) do
        Ecto.Adapters.DuckDB.Appender.append(appender, schema, map)
      end

      @doc """
      Appends multiple rows to an appender.

      Rows should be lists of values in column order.

      ## Examples

          rows = [
            [1, "Alice", "alice@example.com"],
            [2, "Bob", "bob@example.com"]
          ]
          MyApp.Repo.append_rows(appender, rows)

      """
      @spec append_rows(QuackLake.Appender.t(), [[term()]]) :: :ok | {:error, term()}
      def append_rows(appender, rows) when is_list(rows) do
        Ecto.Adapters.DuckDB.Appender.append_rows(appender, rows)
      end

      @doc """
      Flushes buffered rows to disk.

      Useful for checkpointing during large imports.

      ## Examples

          Enum.chunk_every(records, 10_000)
          |> Enum.each(fn batch ->
            Enum.each(batch, &MyApp.Repo.append(appender, &1))
            MyApp.Repo.flush_appender(appender)
          end)

      """
      @spec flush_appender(QuackLake.Appender.t()) :: :ok | {:error, term()}
      def flush_appender(appender) do
        Ecto.Adapters.DuckDB.Appender.flush(appender)
      end

      @doc """
      Closes the appender and flushes any remaining rows.

      Always call this when done appending to ensure all data is written.

      ## Examples

          MyApp.Repo.close_appender(appender)

      """
      @spec close_appender(QuackLake.Appender.t()) :: :ok | {:error, term()}
      def close_appender(appender) do
        Ecto.Adapters.DuckDB.Appender.close(appender)
      end
    end
  end
end
