defmodule QuackLake.Config.Extension do
  @moduledoc """
  Extension configuration parser for DuckDB extensions.

  Supports multiple configuration formats:

      # Simple atom - installs and loads from default repository
      :httpfs

      # Tuple with options
      {:spatial, source: :core}
      {:ducklake, source: :core, install: true, load: true}

      # Custom source URL
      {:my_extension, source: "https://example.com/extensions"}

  ## Sources

    * `:core` - Core extensions bundled with DuckDB
    * `:community` - Community extensions from DuckDB community repository
    * `"url"` - Custom URL for extension download

  """

  @type source :: :core | :community | String.t() | nil

  defstruct [:name, :source, install: true, load: true]

  @type t :: %__MODULE__{
          name: atom(),
          source: source(),
          install: boolean(),
          load: boolean()
        }

  @doc """
  Parses an extension configuration into an Extension struct.

  ## Examples

      iex> QuackLake.Config.Extension.parse(:httpfs)
      %QuackLake.Config.Extension{name: :httpfs, source: nil, install: true, load: true}

      iex> QuackLake.Config.Extension.parse({:spatial, source: :core})
      %QuackLake.Config.Extension{name: :spatial, source: :core, install: true, load: true}

      iex> QuackLake.Config.Extension.parse({:ducklake, source: :core, load: false})
      %QuackLake.Config.Extension{name: :ducklake, source: :core, install: true, load: false}

  """
  @spec parse(atom() | {atom(), keyword()}) :: t()
  def parse(name) when is_atom(name) do
    %__MODULE__{name: name, source: nil, install: true, load: true}
  end

  def parse({name, opts}) when is_atom(name) and is_list(opts) do
    %__MODULE__{
      name: name,
      source: opts[:source],
      install: Keyword.get(opts, :install, true),
      load: Keyword.get(opts, :load, true)
    }
  end

  @doc """
  Generates the INSTALL SQL statement for this extension.

  ## Examples

      iex> ext = QuackLake.Config.Extension.parse(:httpfs)
      iex> QuackLake.Config.Extension.install_sql(ext)
      "INSTALL httpfs"

      iex> ext = QuackLake.Config.Extension.parse({:spatial, source: :core})
      iex> QuackLake.Config.Extension.install_sql(ext)
      "INSTALL spatial FROM core"

      iex> ext = QuackLake.Config.Extension.parse({:my_ext, source: "https://example.com"})
      iex> QuackLake.Config.Extension.install_sql(ext)
      "INSTALL my_ext FROM 'https://example.com'"

  """
  @spec install_sql(t()) :: String.t()
  def install_sql(%__MODULE__{name: name, source: nil}) do
    "INSTALL #{name}"
  end

  def install_sql(%__MODULE__{name: name, source: :core}) do
    "INSTALL #{name} FROM core"
  end

  def install_sql(%__MODULE__{name: name, source: :community}) do
    "INSTALL #{name} FROM community"
  end

  def install_sql(%__MODULE__{name: name, source: url}) when is_binary(url) do
    "INSTALL #{name} FROM '#{escape_string(url)}'"
  end

  @doc """
  Generates the LOAD SQL statement for this extension.

  ## Examples

      iex> ext = QuackLake.Config.Extension.parse(:httpfs)
      iex> QuackLake.Config.Extension.load_sql(ext)
      "LOAD httpfs"

  """
  @spec load_sql(t()) :: String.t()
  def load_sql(%__MODULE__{name: name}) do
    "LOAD #{name}"
  end

  @doc """
  Parses a list of extension configurations.

  ## Examples

      iex> QuackLake.Config.Extension.parse_all([:httpfs, {:spatial, source: :core}])
      [
        %QuackLake.Config.Extension{name: :httpfs, source: nil, install: true, load: true},
        %QuackLake.Config.Extension{name: :spatial, source: :core, install: true, load: true}
      ]

  """
  @spec parse_all([atom() | {atom(), keyword()}]) :: [t()]
  def parse_all(extensions) when is_list(extensions) do
    Enum.map(extensions, &parse/1)
  end

  defp escape_string(str) do
    String.replace(str, "'", "''")
  end
end
