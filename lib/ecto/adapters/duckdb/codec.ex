defmodule Ecto.Adapters.DuckDB.Codec do
  @moduledoc """
  Value encoding and decoding between Elixir and DuckDB.

  This module handles the conversion of values between Elixir
  representations and DuckDB's internal format.
  """

  @doc """
  Encodes an Elixir value for DuckDB.

  ## Examples

      iex> Ecto.Adapters.DuckDB.Codec.encode(~D[2024-01-15])
      ~D[2024-01-15]

      iex> Ecto.Adapters.DuckDB.Codec.encode(%{key: "value"})
      "{\"key\":\"value\"}"

  """
  @spec encode(term()) :: term()
  def encode(%Date{} = date), do: date
  def encode(%Time{} = time), do: time
  def encode(%DateTime{} = dt), do: dt
  def encode(%NaiveDateTime{} = ndt), do: ndt

  def encode(%Decimal{} = decimal), do: Decimal.to_float(decimal)

  def encode(value) when is_map(value) and not is_struct(value) do
    json_encode(value)
  end

  def encode(value) when is_list(value) do
    Enum.map(value, &encode/1)
  end

  def encode(value), do: value

  @doc """
  Decodes a DuckDB value to Elixir.

  ## Examples

      iex> Ecto.Adapters.DuckDB.Codec.decode({2024, 1, 15}, :date)
      ~D[2024-01-15]

  """
  @spec decode(term(), atom()) :: term()
  def decode(nil, _type), do: nil

  def decode(value, :date) when is_tuple(value) do
    Date.from_erl!(value)
  end

  def decode(%Date{} = date, :date), do: date

  def decode(value, :time) when is_tuple(value) do
    Time.from_erl!(value)
  end

  def decode(%Time{} = time, :time), do: time

  def decode(value, :naive_datetime) when is_tuple(value) do
    NaiveDateTime.from_erl!(value)
  end

  def decode(%NaiveDateTime{} = ndt, :naive_datetime), do: ndt

  def decode(value, :naive_datetime_usec) when is_tuple(value) do
    NaiveDateTime.from_erl!(value, {0, 6})
  end

  def decode(%NaiveDateTime{} = ndt, :naive_datetime_usec), do: ndt

  def decode(value, :utc_datetime) when is_tuple(value) do
    value
    |> NaiveDateTime.from_erl!()
    |> DateTime.from_naive!("Etc/UTC")
  end

  def decode(%DateTime{} = dt, :utc_datetime), do: dt

  def decode(value, :utc_datetime_usec) when is_tuple(value) do
    value
    |> NaiveDateTime.from_erl!({0, 6})
    |> DateTime.from_naive!("Etc/UTC")
  end

  def decode(%DateTime{} = dt, :utc_datetime_usec), do: dt

  def decode(value, :decimal) when is_float(value) do
    Decimal.from_float(value)
  end

  def decode(value, :decimal) when is_integer(value) do
    Decimal.new(value)
  end

  def decode(%Decimal{} = d, :decimal), do: d

  def decode(value, :map) when is_binary(value) do
    json_decode(value)
  end

  def decode(value, :map) when is_map(value), do: value

  def decode(value, {:map, _}) when is_binary(value) do
    json_decode(value)
  end

  def decode(value, {:map, _}) when is_map(value), do: value

  def decode(value, {:array, inner_type}) when is_list(value) do
    Enum.map(value, &decode(&1, inner_type))
  end

  def decode(value, :binary_id) when is_binary(value) do
    # UUID values from DuckDB come as strings
    value
  end

  def decode(value, :uuid) when is_binary(value), do: value

  def decode(value, :boolean) when value in [0, "0", false], do: false
  def decode(value, :boolean) when value in [1, "1", true], do: true

  def decode(value, _type), do: value

  @doc """
  Decodes a result row based on column types.

  ## Examples

      iex> Ecto.Adapters.DuckDB.Codec.decode_row([1, "Alice", ~D[2024-01-15]], [:integer, :string, :date])
      [1, "Alice", ~D[2024-01-15]]

  """
  @spec decode_row([term()], [atom()]) :: [term()]
  def decode_row(row, types) when is_list(row) and is_list(types) do
    row
    |> Enum.zip(types)
    |> Enum.map(fn {value, type} -> decode(value, type) end)
  end

  # JSON encoding/decoding - use built-in JSON (Elixir 1.18+) or Erlang :json (OTP 27+)

  defp json_encode(value) do
    case Code.ensure_loaded?(JSON) do
      true -> JSON.encode!(value)
      false -> :json.encode(value) |> IO.iodata_to_binary()
    end
  end

  defp json_decode(value) when is_binary(value) do
    case Code.ensure_loaded?(JSON) do
      true -> JSON.decode!(value)
      false -> :json.decode(value)
    end
  end

  defp json_decode(value), do: value
end
