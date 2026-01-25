defmodule QuackLake.DBConnection.Query do
  @moduledoc """
  Query struct for DBConnection protocol.

  Represents a SQL query to be executed against DuckDB.
  """

  defstruct [:name, :statement, :ref, :columns]

  @type t :: %__MODULE__{
          name: String.t() | nil,
          statement: String.t(),
          ref: reference() | nil,
          columns: [String.t()] | nil
        }

  @doc """
  Creates a new query struct.

  ## Examples

      iex> QuackLake.DBConnection.Query.new("SELECT 1")
      %QuackLake.DBConnection.Query{statement: "SELECT 1"}

      iex> QuackLake.DBConnection.Query.new("SELECT 1", name: "my_query")
      %QuackLake.DBConnection.Query{statement: "SELECT 1", name: "my_query"}

  """
  @spec new(String.t(), keyword()) :: t()
  def new(statement, opts \\ []) do
    %__MODULE__{
      name: opts[:name],
      statement: statement,
      ref: nil,
      columns: nil
    }
  end
end

defimpl DBConnection.Query, for: QuackLake.DBConnection.Query do
  def parse(query, _opts), do: query

  def describe(query, _opts), do: query

  def encode(_query, params, _opts), do: params

  def decode(_query, result, _opts), do: result
end

defimpl String.Chars, for: QuackLake.DBConnection.Query do
  def to_string(%{statement: statement}), do: statement
end
