defmodule Ecto.Adapters.DuckDB.DataType do
  @moduledoc """
  Type mapping between Ecto and DuckDB types.

  ## Type Mapping

  | Ecto Type | DuckDB Type |
  |-----------|-------------|
  | `:id` | `BIGINT` |
  | `:binary_id` | `UUID` |
  | `:string` | `VARCHAR` |
  | `:binary` | `BLOB` |
  | `:integer` | `INTEGER` |
  | `:bigint` | `BIGINT` |
  | `:float` | `DOUBLE` |
  | `:decimal` | `DECIMAL` |
  | `:boolean` | `BOOLEAN` |
  | `:map` | `JSON` |
  | `:date` | `DATE` |
  | `:time` | `TIME` |
  | `:naive_datetime` | `TIMESTAMP` |
  | `:utc_datetime` | `TIMESTAMPTZ` |
  | `{:array, type}` | `type[]` |

  ## DuckDB-Specific Types

  DuckDB supports additional types that can be used via fragments:

  - `HUGEINT` - 128-bit integer
  - `UINTEGER`, `UBIGINT`, `UHUGEINT` - Unsigned integers
  - `INTERVAL` - Time interval
  - `STRUCT` - Struct/composite type
  - `LIST` - List type (similar to array)
  - `MAP` - Key-value map type
  - `UNION` - Tagged union type

  """

  @type_map %{
    id: "BIGINT",
    serial: "INTEGER",
    bigserial: "BIGINT",
    binary_id: "UUID",
    uuid: "UUID",
    string: "VARCHAR",
    binary: "BLOB",
    integer: "INTEGER",
    bigint: "BIGINT",
    float: "DOUBLE",
    decimal: "DECIMAL",
    boolean: "BOOLEAN",
    map: "JSON",
    date: "DATE",
    time: "TIME",
    time_usec: "TIME",
    naive_datetime: "TIMESTAMP",
    naive_datetime_usec: "TIMESTAMP",
    utc_datetime: "TIMESTAMPTZ",
    utc_datetime_usec: "TIMESTAMPTZ",
    timestamp: "TIMESTAMP"
  }

  @doc """
  Converts an Ecto type to its DuckDB equivalent.

  ## Examples

      iex> Ecto.Adapters.DuckDB.DataType.ecto_to_db(:string)
      "VARCHAR"

      iex> Ecto.Adapters.DuckDB.DataType.ecto_to_db(:integer)
      "INTEGER"

      iex> Ecto.Adapters.DuckDB.DataType.ecto_to_db({:array, :string})
      "VARCHAR[]"

  """
  @spec ecto_to_db(atom() | tuple()) :: String.t()
  def ecto_to_db({:array, type}) do
    ecto_to_db(type) <> "[]"
  end

  def ecto_to_db({:map, _}) do
    "JSON"
  end

  def ecto_to_db(type) when is_atom(type) do
    Map.get(@type_map, type, type |> Atom.to_string() |> String.upcase())
  end

  @doc """
  Converts a DuckDB type to its Ecto equivalent.

  ## Examples

      iex> Ecto.Adapters.DuckDB.DataType.db_to_ecto("VARCHAR")
      :string

      iex> Ecto.Adapters.DuckDB.DataType.db_to_ecto("INTEGER")
      :integer

      iex> Ecto.Adapters.DuckDB.DataType.db_to_ecto("VARCHAR[]")
      {:array, :string}

  """
  @spec db_to_ecto(String.t()) :: atom() | tuple()
  def db_to_ecto(type) when is_binary(type) do
    type = String.upcase(type)

    cond do
      String.ends_with?(type, "[]") ->
        base_type = String.trim_trailing(type, "[]")
        {:array, db_to_ecto(base_type)}

      true ->
        do_db_to_ecto(type)
    end
  end

  defp do_db_to_ecto("BIGINT"), do: :bigint
  defp do_db_to_ecto("INTEGER"), do: :integer
  defp do_db_to_ecto("INT"), do: :integer
  defp do_db_to_ecto("INT4"), do: :integer
  defp do_db_to_ecto("INT8"), do: :bigint
  defp do_db_to_ecto("SMALLINT"), do: :integer
  defp do_db_to_ecto("INT2"), do: :integer
  defp do_db_to_ecto("TINYINT"), do: :integer
  defp do_db_to_ecto("UUID"), do: :binary_id
  defp do_db_to_ecto("VARCHAR"), do: :string
  defp do_db_to_ecto("TEXT"), do: :string
  defp do_db_to_ecto("STRING"), do: :string
  defp do_db_to_ecto("BLOB"), do: :binary
  defp do_db_to_ecto("BYTEA"), do: :binary
  defp do_db_to_ecto("DOUBLE"), do: :float
  defp do_db_to_ecto("FLOAT8"), do: :float
  defp do_db_to_ecto("FLOAT"), do: :float
  defp do_db_to_ecto("FLOAT4"), do: :float
  defp do_db_to_ecto("REAL"), do: :float
  defp do_db_to_ecto("DECIMAL"), do: :decimal
  defp do_db_to_ecto("NUMERIC"), do: :decimal
  defp do_db_to_ecto("BOOLEAN"), do: :boolean
  defp do_db_to_ecto("BOOL"), do: :boolean
  defp do_db_to_ecto("JSON"), do: :map
  defp do_db_to_ecto("DATE"), do: :date
  defp do_db_to_ecto("TIME"), do: :time
  defp do_db_to_ecto("TIMESTAMP"), do: :naive_datetime
  defp do_db_to_ecto("TIMESTAMPTZ"), do: :utc_datetime
  defp do_db_to_ecto("TIMESTAMP WITH TIME ZONE"), do: :utc_datetime
  defp do_db_to_ecto(type), do: String.downcase(type) |> String.to_atom()

  @doc """
  Returns the list of supported Ecto types.
  """
  @spec supported_types() :: [atom()]
  def supported_types do
    Map.keys(@type_map)
  end
end
