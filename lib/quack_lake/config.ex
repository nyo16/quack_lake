defmodule QuackLake.Config do
  @moduledoc """
  Configuration struct for QuackLake connections.

  Supports both the raw QuackLake API and Ecto adapter configuration.

  ## Raw API Configuration

      config = QuackLake.Config.new(path: "data.duckdb")
      {:ok, conn} = QuackLake.Connection.open(config)

  ## Ecto Adapter Configuration

      # config/config.exs
      config :my_app, MyApp.Repo,
        adapter: Ecto.Adapters.DuckDB,
        database: "priv/data.duckdb",
        extensions: [:httpfs, {:spatial, source: :core}],
        secrets: [
          {:my_s3, [type: :s3, key_id: "...", secret: "...", region: "us-east-1"]}
        ]

  """

  alias QuackLake.Config.{Attach, Extension, Secret}

  defstruct [
    # Database path (nil for in-memory)
    :path,
    # For Ecto compatibility
    :database,
    # Data path for DuckLake (S3, local, etc.)
    :data_path,
    # Custom lake name (overrides auto-generated name from path)
    :lake_name,
    # Schema in the catalog database for DuckLake metadata tables
    :metadata_schema,
    # Override DuckDB home directory (for container environments)
    :home_directory,
    # Connection pool size (1 for DuckDB, configurable for DuckLake)
    pool_size: 1,
    # Extensions to install/load
    extensions: [],
    # Secrets for cloud storage
    secrets: [],
    # Databases to attach
    attach: [],
    # Legacy options
    auto_install_extensions: true,
    auto_load_extensions: true,
    # Parsed config structs (populated by from_ecto_opts/1)
    parsed_extensions: [],
    parsed_secrets: [],
    parsed_attach: []
  ]

  @type t :: %__MODULE__{
          path: String.t() | nil,
          database: String.t() | nil,
          data_path: String.t() | nil,
          lake_name: String.t() | nil,
          metadata_schema: String.t() | nil,
          home_directory: String.t() | nil,
          pool_size: pos_integer(),
          extensions: [atom() | {atom(), keyword()}],
          secrets: [{atom(), keyword()}],
          attach: [{String.t(), keyword()}],
          auto_install_extensions: boolean(),
          auto_load_extensions: boolean(),
          parsed_extensions: [Extension.t()],
          parsed_secrets: [Secret.t()],
          parsed_attach: [Attach.t()]
        }

  @doc """
  Creates a new config from keyword options.

  ## Options

    * `:path` - Path to the DuckDB database file. Defaults to `nil` (in-memory).
    * `:database` - Alias for `:path` (Ecto compatibility).
    * `:pool_size` - Connection pool size. Defaults to `1`.
    * `:extensions` - List of extensions to install and load.
    * `:secrets` - List of secrets for cloud storage access.
    * `:attach` - List of databases to attach.
    * `:metadata_schema` - Schema in the catalog database for DuckLake metadata tables. Useful for storing metadata in a PostgreSQL schema other than `public`.
    * `:home_directory` - Override DuckDB home directory. Useful in container environments
      where `HOME` is unset or points to a non-writable path. Defaults to `nil` (auto-detected).
    * `:auto_install_extensions` - Whether to auto-install required extensions. Defaults to `true`.
    * `:auto_load_extensions` - Whether to auto-load required extensions. Defaults to `true`.

  ## Examples

      iex> QuackLake.Config.new()
      %QuackLake.Config{path: nil, auto_install_extensions: true, auto_load_extensions: true}

      iex> QuackLake.Config.new(path: "data.duckdb")
      %QuackLake.Config{path: "data.duckdb", auto_install_extensions: true, auto_load_extensions: true}

      iex> QuackLake.Config.new(database: "data.duckdb", extensions: [:httpfs])
      %QuackLake.Config{path: "data.duckdb", database: "data.duckdb", extensions: [:httpfs]}

  """
  @spec new(keyword()) :: t()
  def new(opts \\ []) do
    path = opts[:path] || opts[:database]

    %__MODULE__{
      path: path,
      database: opts[:database],
      data_path: opts[:data_path],
      lake_name: opts[:lake_name],
      metadata_schema: opts[:metadata_schema],
      home_directory: opts[:home_directory],
      pool_size: Keyword.get(opts, :pool_size, 1),
      extensions: Keyword.get(opts, :extensions, []),
      secrets: Keyword.get(opts, :secrets, []),
      attach: Keyword.get(opts, :attach, []),
      auto_install_extensions: Keyword.get(opts, :auto_install_extensions, true),
      auto_load_extensions: Keyword.get(opts, :auto_load_extensions, true)
    }
  end

  @doc """
  Creates a Config from Ecto repo options.

  Parses extensions, secrets, and attach configurations into their
  respective structs for easier handling during connection setup.

  ## Examples

      iex> opts = [database: "data.duckdb", extensions: [:httpfs, {:spatial, source: :core}]]
      iex> config = QuackLake.Config.from_ecto_opts(opts)
      iex> length(config.parsed_extensions)
      2

  """
  @spec from_ecto_opts(keyword()) :: t()
  def from_ecto_opts(opts) do
    config = new(opts)

    %{
      config
      | parsed_extensions: Extension.parse_all(config.extensions),
        parsed_secrets: Secret.parse_all(config.secrets),
        parsed_attach: Attach.parse_all(config.attach)
    }
  end

  @doc """
  Returns the effective database path.

  Prefers `:database` over `:path` for Ecto compatibility.
  """
  @spec database_path(t()) :: String.t() | nil
  def database_path(%__MODULE__{database: db}) when is_binary(db), do: db
  def database_path(%__MODULE__{path: path}), do: path

  @doc """
  Checks if this is an in-memory database configuration.
  """
  @spec in_memory?(t()) :: boolean()
  def in_memory?(%__MODULE__{} = config) do
    database_path(config) == nil
  end

  @doc """
  Checks if this configuration has extensions to install/load.
  """
  @spec has_extensions?(t()) :: boolean()
  def has_extensions?(%__MODULE__{extensions: exts}) when length(exts) > 0, do: true
  def has_extensions?(%__MODULE__{}), do: false

  @doc """
  Checks if this configuration has secrets to create.
  """
  @spec has_secrets?(t()) :: boolean()
  def has_secrets?(%__MODULE__{secrets: secrets}) when length(secrets) > 0, do: true
  def has_secrets?(%__MODULE__{}), do: false

  @doc """
  Checks if this configuration has databases to attach.
  """
  @spec has_attachments?(t()) :: boolean()
  def has_attachments?(%__MODULE__{attach: attach}) when length(attach) > 0, do: true
  def has_attachments?(%__MODULE__{}), do: false

  @doc """
  Resolves the effective home directory for DuckDB.

  DuckDB requires a writable home directory for extension caching and catalog
  operations. In container environments (Docker, Kubernetes), `HOME` is often
  `/nonexistent` or missing, causing `IO Error: Can't find the home directory`.

  Resolution order:

    1. Explicit `:home_directory` from config (if set)
    2. `DUCKDB_HOME` environment variable (if set and directory exists)
    3. Current `HOME` environment variable (if set and directory exists)
    4. `"/tmp"` as final fallback

  """
  @spec resolve_home_directory(t()) :: String.t()
  def resolve_home_directory(%__MODULE__{home_directory: dir}) when is_binary(dir), do: dir

  def resolve_home_directory(%__MODULE__{}) do
    with :error <- check_env_dir("DUCKDB_HOME"),
         :error <- check_env_dir("HOME") do
      "/tmp"
    else
      {:ok, dir} -> dir
    end
  end

  defp check_env_dir(var) do
    case System.get_env(var) do
      nil -> :error
      dir -> if File.dir?(dir), do: {:ok, dir}, else: :error
    end
  end
end
