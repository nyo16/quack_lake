defmodule QuackLake.Error do
  @moduledoc """
  Error struct for QuackLake operations.
  """

  defexception [:message, :reason]

  @type t :: %__MODULE__{
          message: String.t(),
          reason: term()
        }

  @impl true
  def exception(opts) when is_list(opts) do
    message = Keyword.get(opts, :message, "QuackLake error")
    reason = Keyword.get(opts, :reason)

    %__MODULE__{message: message, reason: reason}
  end

  def exception(message) when is_binary(message) do
    %__MODULE__{message: message, reason: nil}
  end

  @doc """
  Wraps a duckdbex error into a QuackLake.Error.
  """
  @spec wrap(term()) :: t()
  def wrap({:error, reason}) when is_binary(reason) do
    %__MODULE__{message: reason, reason: reason}
  end

  def wrap({:error, reason}) do
    %__MODULE__{message: inspect(reason), reason: reason}
  end

  def wrap(other) do
    %__MODULE__{message: inspect(other), reason: other}
  end
end
