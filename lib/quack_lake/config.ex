defmodule QuackLake.Config do
  @moduledoc """
  Configuration struct for QuackLake connections.
  """

  defstruct [
    :path,
    auto_install_extensions: true,
    auto_load_extensions: true
  ]

  @type t :: %__MODULE__{
          path: String.t() | nil,
          auto_install_extensions: boolean(),
          auto_load_extensions: boolean()
        }

  @doc """
  Creates a new config from keyword options.

  ## Options

    * `:path` - Path to the DuckDB database file. Defaults to `nil` (in-memory).
    * `:auto_install_extensions` - Whether to auto-install required extensions. Defaults to `true`.
    * `:auto_load_extensions` - Whether to auto-load required extensions. Defaults to `true`.

  ## Examples

      iex> QuackLake.Config.new()
      %QuackLake.Config{path: nil, auto_install_extensions: true, auto_load_extensions: true}

      iex> QuackLake.Config.new(path: "data.duckdb")
      %QuackLake.Config{path: "data.duckdb", auto_install_extensions: true, auto_load_extensions: true}

  """
  @spec new(keyword()) :: t()
  def new(opts \\ []) do
    %__MODULE__{
      path: opts[:path],
      auto_install_extensions: Keyword.get(opts, :auto_install_extensions, true),
      auto_load_extensions: Keyword.get(opts, :auto_load_extensions, true)
    }
  end
end
