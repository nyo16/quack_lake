defmodule Ecto.Adapters.DuckDB.Appender do
  @moduledoc """
  Ecto-aware Appender for high-performance bulk inserts.

  This module bridges QuackLake's low-level Appender API with Ecto,
  allowing you to append Ecto structs and maps directly.

  ## Usage with Ecto Repo

  The recommended way to use this is through the `Ecto.Adapters.DuckDB.RawQuery`
  macro, which adds convenient functions to your repo:

      defmodule MyApp.Repo do
        use Ecto.Repo, otp_app: :my_app, adapter: Ecto.Adapters.DuckDB
        use Ecto.Adapters.DuckDB.RawQuery
      end

      # Then use via repo functions:
      {:ok, appender} = MyApp.Repo.appender(User)
      MyApp.Repo.append(appender, %User{name: "Alice"})
      MyApp.Repo.close_appender(appender)

  ## Direct Usage

  You can also use this module directly:

      {:ok, appender} = Ecto.Adapters.DuckDB.Appender.new(MyApp.Repo, User)

      Enum.each(users, fn user ->
        Ecto.Adapters.DuckDB.Appender.append(appender, user)
      end)

      Ecto.Adapters.DuckDB.Appender.close(appender)

  ## Schema Field Extraction

  When appending structs, fields are extracted in the order defined by
  `schema.__schema__(:fields)`. This typically excludes virtual fields
  and association keys.

  For maps, you must provide the schema to determine field order:

      Ecto.Adapters.DuckDB.Appender.append(appender, User, %{name: "Bob", email: "bob@ex.com"})

  """

  alias QuackLake.Appender

  defstruct [:appender, :schema, :fields, :repo]

  @type t :: %__MODULE__{
          appender: Appender.t(),
          schema: module(),
          fields: [atom()],
          repo: module()
        }

  @doc """
  Creates an appender for an Ecto schema's table.

  Connects to the database through the repo and creates an appender
  for the schema's source table.

  ## Examples

      {:ok, appender} = Ecto.Adapters.DuckDB.Appender.new(MyApp.Repo, User)

  """
  @spec new(module(), module()) :: {:ok, t()} | {:error, term()}
  def new(repo, schema) when is_atom(repo) and is_atom(schema) do
    table = schema.__schema__(:source)
    fields = get_insertable_fields(schema)

    # Get a connection from the repo's pool
    repo.checkout(fn ->
      # Access the underlying connection
      # The repo should be using DBConnection under the hood
      meta = Ecto.Adapter.lookup_meta(repo)
      pool = meta.pid

      # Run within a checked-out connection
      DBConnection.run(pool, fn conn_state ->
        # Extract the actual DuckDB connection from the protocol state
        conn = get_duckdb_conn(conn_state)

        case Appender.new(conn, table) do
          {:ok, low_level_appender} ->
            {:ok,
             %__MODULE__{
               appender: low_level_appender,
               schema: schema,
               fields: fields,
               repo: repo
             }}

          {:error, reason} ->
            {:error, reason}
        end
      end)
    end)
  rescue
    e -> {:error, Exception.message(e)}
  end

  @doc """
  Creates an appender, raising on error.

  ## Examples

      appender = Ecto.Adapters.DuckDB.Appender.new!(MyApp.Repo, User)

  """
  @spec new!(module(), module()) :: t()
  def new!(repo, schema) do
    case new(repo, schema) do
      {:ok, appender} ->
        appender

      {:error, reason} ->
        raise Ecto.QueryError, message: "Failed to create appender: #{inspect(reason)}"
    end
  end

  @doc """
  Appends an Ecto struct, extracting fields in schema order.

  ## Examples

      :ok = Ecto.Adapters.DuckDB.Appender.append(appender, %User{name: "Alice", email: "alice@ex.com"})

  """
  @spec append(t(), struct()) :: :ok | {:error, term()}
  def append(%__MODULE__{appender: appender, fields: fields}, %{__struct__: _schema} = struct) do
    row = extract_row(struct, fields)
    Appender.append(appender, row)
  end

  @doc """
  Appends a map, converting to row based on schema field order.

  ## Examples

      :ok = Ecto.Adapters.DuckDB.Appender.append(appender, User, %{name: "Bob", email: "bob@ex.com"})

  """
  @spec append(t(), module(), map()) :: :ok | {:error, term()}
  def append(%__MODULE__{appender: appender} = _ctx, schema, map)
      when is_atom(schema) and is_map(map) do
    fields = get_insertable_fields(schema)
    row = Enum.map(fields, &Map.get(map, &1))
    Appender.append(appender, row)
  end

  @doc """
  Appends multiple raw rows at once.

  Rows must be lists of values in the correct column order.
  This bypasses schema field extraction for maximum performance.

  ## Examples

      rows = [
        [1, "Alice", "alice@ex.com"],
        [2, "Bob", "bob@ex.com"]
      ]
      :ok = Ecto.Adapters.DuckDB.Appender.append_rows(appender, rows)

  """
  @spec append_rows(t(), [[term()]]) :: :ok | {:error, term()}
  def append_rows(%__MODULE__{appender: appender}, rows) when is_list(rows) do
    Appender.append_rows(appender, rows)
  end

  @doc """
  Flushes buffered rows to the database.

  ## Examples

      :ok = Ecto.Adapters.DuckDB.Appender.flush(appender)

  """
  @spec flush(t()) :: :ok | {:error, term()}
  def flush(%__MODULE__{appender: appender}) do
    Appender.flush(appender)
  end

  @doc """
  Closes the appender and flushes any remaining rows.

  ## Examples

      :ok = Ecto.Adapters.DuckDB.Appender.close(appender)

  """
  @spec close(t()) :: :ok | {:error, term()}
  def close(%__MODULE__{appender: appender}) do
    Appender.close(appender)
  end

  @doc """
  Returns the schema module for this appender.
  """
  @spec schema(t()) :: module()
  def schema(%__MODULE__{schema: schema}), do: schema

  @doc """
  Returns the field list for this appender.
  """
  @spec fields(t()) :: [atom()]
  def fields(%__MODULE__{fields: fields}), do: fields

  # Private helpers

  defp get_insertable_fields(schema) do
    # Get all fields except primary key if it's autogenerated,
    # and except virtual fields
    all_fields = schema.__schema__(:fields)
    _primary_key = schema.__schema__(:primary_key)

    # Check if primary key is autogenerated
    autogenerate_id = schema.__schema__(:autogenerate_id)

    # Filter out autogenerated primary keys
    if autogenerate_id do
      {pk_field, _pk_type} = autogenerate_id
      Enum.reject(all_fields, &(&1 == pk_field))
    else
      # Include all fields (including primary key if manually set)
      all_fields
    end
  end

  defp extract_row(struct, fields) do
    Enum.map(fields, fn field ->
      value = Map.get(struct, field)
      encode_value(value)
    end)
  end

  defp encode_value(%DateTime{} = dt), do: DateTime.to_naive(dt)
  defp encode_value(%Date{} = d), do: d
  defp encode_value(%Time{} = t), do: t
  defp encode_value(%NaiveDateTime{} = ndt), do: ndt
  defp encode_value(%Decimal{} = d), do: Decimal.to_float(d)

  defp encode_value(value) when is_map(value) and not is_struct(value) do
    # Encode maps as JSON
    cond do
      Code.ensure_loaded?(JSON) -> JSON.encode!(value)
      Code.ensure_loaded?(Jason) -> Jason.encode!(value)
      true -> inspect(value)
    end
  end

  defp encode_value(value), do: value

  defp get_duckdb_conn(%{conn: conn}) when is_reference(conn), do: conn
  defp get_duckdb_conn(%QuackLake.DBConnection.Protocol{conn: conn}), do: conn
  defp get_duckdb_conn(%QuackLake.DBConnection.LakeProtocol{conn: conn}), do: conn

  defp get_duckdb_conn(state) do
    # Try to find conn in the state structure
    cond do
      Map.has_key?(state, :conn) -> state.conn
      Map.has_key?(state, :state) -> get_duckdb_conn(state.state)
      true -> raise "Could not extract DuckDB connection from state: #{inspect(state)}"
    end
  end
end
