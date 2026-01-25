defmodule QuackLake.Config.Secret do
  @moduledoc """
  Secret configuration parser for DuckDB secrets.

  Secrets provide credentials for accessing cloud storage (S3, Azure, GCS, etc.).

  ## Configuration Format

      secrets: [
        {:my_s3_secret, [
          type: :s3,
          key_id: System.get_env("AWS_ACCESS_KEY_ID"),
          secret: System.get_env("AWS_SECRET_ACCESS_KEY"),
          region: "us-east-1"
        ]},
        {:my_azure_secret, [
          type: :azure,
          connection_string: System.get_env("AZURE_STORAGE_CONNECTION_STRING")
        ]},
        {:my_gcs_secret, [
          type: :gcs,
          key_id: "GOOG1E...",
          secret: "..."
        ]},
        {:my_r2_secret, [
          type: :r2,
          account_id: "...",
          key_id: "...",
          secret: "..."
        ]},
        {:my_hf_secret, [
          type: :huggingface,
          token: System.get_env("HF_TOKEN")
        ]}
      ]

  ## Secret Types

    * `:s3` - AWS S3 or S3-compatible storage
    * `:azure` - Azure Blob Storage
    * `:gcs` - Google Cloud Storage
    * `:r2` - Cloudflare R2
    * `:huggingface` - HuggingFace Hub

  """

  @type secret_type :: :s3 | :azure | :gcs | :r2 | :huggingface

  defstruct [:name, :type, :options]

  @type t :: %__MODULE__{
          name: atom(),
          type: secret_type(),
          options: keyword()
        }

  @doc """
  Parses a secret configuration into a Secret struct.

  ## Examples

      iex> QuackLake.Config.Secret.parse({:my_s3, [type: :s3, key_id: "AK", secret: "SK", region: "us-east-1"]})
      %QuackLake.Config.Secret{name: :my_s3, type: :s3, options: [type: :s3, key_id: "AK", secret: "SK", region: "us-east-1"]}

  """
  @spec parse({atom(), keyword()}) :: t()
  def parse({name, opts}) when is_atom(name) and is_list(opts) do
    type = Keyword.fetch!(opts, :type)

    %__MODULE__{
      name: name,
      type: type,
      options: opts
    }
  end

  @doc """
  Generates the CREATE SECRET SQL statement for this secret.

  ## Examples

      iex> secret = QuackLake.Config.Secret.parse({:my_s3, [type: :s3, key_id: "AK", secret: "SK", region: "us-east-1"]})
      iex> QuackLake.Config.Secret.create_sql(secret)
      "CREATE SECRET my_s3 (TYPE S3, KEY_ID 'AK', SECRET 'SK', REGION 'us-east-1')"

  """
  @spec create_sql(t()) :: String.t()
  def create_sql(%__MODULE__{name: name, type: :s3, options: opts}) do
    parts =
      [
        "TYPE S3",
        "KEY_ID '#{escape_string(opts[:key_id])}'",
        "SECRET '#{escape_string(opts[:secret])}'",
        "REGION '#{escape_string(opts[:region])}'"
      ]
      |> maybe_add_option(opts, :endpoint, fn v -> "ENDPOINT '#{escape_string(v)}'" end)
      |> maybe_add_option(opts, :use_ssl, fn v -> "USE_SSL #{v}" end)
      |> maybe_add_option(opts, :url_style, fn v -> "URL_STYLE '#{v}'" end)
      |> maybe_add_option(opts, :scope, fn v -> "SCOPE '#{escape_string(v)}'" end)

    "CREATE SECRET #{name} (#{Enum.join(parts, ", ")})"
  end

  def create_sql(%__MODULE__{name: name, type: :azure, options: opts}) do
    parts =
      if connection_string = opts[:connection_string] do
        [
          "TYPE AZURE",
          "CONNECTION_STRING '#{escape_string(connection_string)}'"
        ]
      else
        [
          "TYPE AZURE",
          "ACCOUNT_NAME '#{escape_string(opts[:account_name])}'",
          "ACCOUNT_KEY '#{escape_string(opts[:account_key])}'"
        ]
      end
      |> maybe_add_option(opts, :scope, fn v -> "SCOPE '#{escape_string(v)}'" end)

    "CREATE SECRET #{name} (#{Enum.join(parts, ", ")})"
  end

  def create_sql(%__MODULE__{name: name, type: :gcs, options: opts}) do
    parts =
      [
        "TYPE GCS",
        "KEY_ID '#{escape_string(opts[:key_id])}'",
        "SECRET '#{escape_string(opts[:secret])}'"
      ]
      |> maybe_add_option(opts, :scope, fn v -> "SCOPE '#{escape_string(v)}'" end)

    "CREATE SECRET #{name} (#{Enum.join(parts, ", ")})"
  end

  def create_sql(%__MODULE__{name: name, type: :r2, options: opts}) do
    parts =
      [
        "TYPE R2",
        "ACCOUNT_ID '#{escape_string(opts[:account_id])}'",
        "KEY_ID '#{escape_string(opts[:key_id])}'",
        "SECRET '#{escape_string(opts[:secret])}'"
      ]
      |> maybe_add_option(opts, :scope, fn v -> "SCOPE '#{escape_string(v)}'" end)

    "CREATE SECRET #{name} (#{Enum.join(parts, ", ")})"
  end

  def create_sql(%__MODULE__{name: name, type: :huggingface, options: opts}) do
    parts =
      [
        "TYPE HUGGINGFACE",
        "TOKEN '#{escape_string(opts[:token])}'"
      ]
      |> maybe_add_option(opts, :scope, fn v -> "SCOPE '#{escape_string(v)}'" end)

    "CREATE SECRET #{name} (#{Enum.join(parts, ", ")})"
  end

  @doc """
  Parses a list of secret configurations.

  ## Examples

      iex> secrets = [{:s3, [type: :s3, key_id: "AK", secret: "SK", region: "us-east-1"]}]
      iex> QuackLake.Config.Secret.parse_all(secrets)
      [%QuackLake.Config.Secret{name: :s3, type: :s3, options: [type: :s3, key_id: "AK", secret: "SK", region: "us-east-1"]}]

  """
  @spec parse_all([{atom(), keyword()}]) :: [t()]
  def parse_all(secrets) when is_list(secrets) do
    Enum.map(secrets, &parse/1)
  end

  defp maybe_add_option(parts, opts, key, formatter) do
    case opts[key] do
      nil -> parts
      value -> parts ++ [formatter.(value)]
    end
  end

  defp escape_string(nil), do: ""

  defp escape_string(str) when is_binary(str) do
    String.replace(str, "'", "''")
  end

  defp escape_string(other), do: to_string(other)
end
