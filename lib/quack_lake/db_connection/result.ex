defmodule QuackLake.DBConnection.Result do
  @moduledoc """
  Result struct for DBConnection protocol.

  Represents the result of a DuckDB query execution.
  """

  defstruct [:command, :columns, :rows, :num_rows]

  @type t :: %__MODULE__{
          command: atom() | nil,
          columns: [String.t()] | nil,
          rows: [[term()]] | nil,
          num_rows: non_neg_integer() | nil
        }

  @doc """
  Creates a new result struct.

  ## Examples

      iex> QuackLake.DBConnection.Result.new(:select, ["id", "name"], [[1, "Alice"]], 1)
      %QuackLake.DBConnection.Result{command: :select, columns: ["id", "name"], rows: [[1, "Alice"]], num_rows: 1}

  """
  @spec new(atom() | nil, [String.t()] | nil, [[term()]] | nil, non_neg_integer() | nil) :: t()
  def new(command, columns, rows, num_rows) do
    %__MODULE__{
      command: command,
      columns: columns,
      rows: rows,
      num_rows: num_rows
    }
  end

  @doc """
  Creates an empty result for commands that don't return rows.

  ## Examples

      iex> QuackLake.DBConnection.Result.empty(:create_table)
      %QuackLake.DBConnection.Result{command: :create_table, columns: nil, rows: nil, num_rows: 0}

  """
  @spec empty(atom()) :: t()
  def empty(command) do
    %__MODULE__{
      command: command,
      columns: nil,
      rows: nil,
      num_rows: 0
    }
  end

  @doc """
  Transforms result rows into a list of maps.

  ## Examples

      iex> result = %QuackLake.DBConnection.Result{columns: ["id", "name"], rows: [[1, "Alice"], [2, "Bob"]]}
      iex> QuackLake.DBConnection.Result.to_maps(result)
      [%{"id" => 1, "name" => "Alice"}, %{"id" => 2, "name" => "Bob"}]

  """
  @spec to_maps(t()) :: [map()]
  def to_maps(%__MODULE__{columns: nil}), do: []
  def to_maps(%__MODULE__{rows: nil}), do: []

  def to_maps(%__MODULE__{columns: columns, rows: rows}) do
    Enum.map(rows, fn row ->
      columns
      |> Enum.zip(row)
      |> Map.new()
    end)
  end
end
