defmodule QuackLake.Secret do
  @moduledoc """
  Cloud storage credential management for DuckDB.

  Secrets allow DuckDB to access remote storage like S3, Azure Blob Storage, etc.
  """

  alias QuackLake.Query

  @doc """
  Creates an S3 secret for accessing AWS S3 or S3-compatible storage.

  ## Options

    * `:key_id` - AWS Access Key ID (required)
    * `:secret` - AWS Secret Access Key (required)
    * `:region` - AWS region (required)
    * `:endpoint` - Custom endpoint for S3-compatible storage (optional).
      Accepts full URLs (e.g. `http://localhost:9000`) — the scheme is
      stripped automatically since DuckDB expects `host:port` only.
    * `:use_ssl` - Whether to use SSL. Defaults to `true`.
    * `:url_style` - S3 URL style: `"vhost"` (default) or `"path"`.
      Use `"path"` for S3-compatible services like MinIO.

  ## Examples

      iex> QuackLake.Secret.create_s3(conn, "my_s3",
      ...>   key_id: "AKIAIOSFODNN7EXAMPLE",
      ...>   secret: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
      ...>   region: "us-east-1"
      ...> )
      :ok

      iex> QuackLake.Secret.create_s3(conn, "minio_s3",
      ...>   key_id: "minioadmin",
      ...>   secret: "minioadmin123",
      ...>   region: "us-east-1",
      ...>   endpoint: "http://localhost:9000",
      ...>   use_ssl: false,
      ...>   url_style: "path"
      ...> )
      :ok

  """
  @spec create_s3(Duckdbex.connection(), String.t(), keyword()) :: :ok | {:error, term()}
  def create_s3(conn, name, opts) do
    key_id = Keyword.fetch!(opts, :key_id)
    secret = Keyword.fetch!(opts, :secret)
    region = Keyword.fetch!(opts, :region)
    endpoint = opts[:endpoint]
    use_ssl = Keyword.get(opts, :use_ssl, true)
    url_style = opts[:url_style]

    secret_opts =
      [
        "TYPE S3",
        "KEY_ID '#{escape_string(key_id)}'",
        "SECRET '#{escape_string(secret)}'",
        "REGION '#{escape_string(region)}'"
      ]
      |> maybe_add_endpoint(endpoint)
      |> maybe_add_ssl(use_ssl)
      |> maybe_add_url_style(url_style)
      |> Enum.join(", ")

    sql = "CREATE SECRET #{name} (#{secret_opts})"
    Query.execute(conn, sql)
  end

  @doc """
  Creates an Azure secret for accessing Azure Blob Storage.

  ## Options

    * `:connection_string` - Azure connection string (use this OR account_name + account_key)
    * `:account_name` - Azure storage account name
    * `:account_key` - Azure storage account key

  ## Examples

      iex> QuackLake.Secret.create_azure(conn, "my_azure",
      ...>   account_name: "myaccount",
      ...>   account_key: "mykey..."
      ...> )
      :ok

  """
  @spec create_azure(Duckdbex.connection(), String.t(), keyword()) :: :ok | {:error, term()}
  def create_azure(conn, name, opts) do
    secret_opts =
      cond do
        connection_string = opts[:connection_string] ->
          ["TYPE AZURE", "CONNECTION_STRING '#{escape_string(connection_string)}'"]

        opts[:account_name] && opts[:account_key] ->
          [
            "TYPE AZURE",
            "ACCOUNT_NAME '#{escape_string(opts[:account_name])}'",
            "ACCOUNT_KEY '#{escape_string(opts[:account_key])}'"
          ]

        true ->
          raise ArgumentError,
                "Must provide either :connection_string or both :account_name and :account_key"
      end
      |> Enum.join(", ")

    sql = "CREATE SECRET #{name} (#{secret_opts})"
    Query.execute(conn, sql)
  end

  @doc """
  Creates a GCS (Google Cloud Storage) secret.

  ## Options

    * `:key_id` - GCS Access Key ID (required for HMAC auth)
    * `:secret` - GCS Secret Access Key (required for HMAC auth)

  ## Examples

      iex> QuackLake.Secret.create_gcs(conn, "my_gcs",
      ...>   key_id: "GOOG1E...",
      ...>   secret: "..."
      ...> )
      :ok

  """
  @spec create_gcs(Duckdbex.connection(), String.t(), keyword()) :: :ok | {:error, term()}
  def create_gcs(conn, name, opts) do
    key_id = Keyword.fetch!(opts, :key_id)
    secret = Keyword.fetch!(opts, :secret)

    secret_opts =
      [
        "TYPE GCS",
        "KEY_ID '#{escape_string(key_id)}'",
        "SECRET '#{escape_string(secret)}'"
      ]
      |> Enum.join(", ")

    sql = "CREATE SECRET #{name} (#{secret_opts})"
    Query.execute(conn, sql)
  end

  @doc """
  Lists all secrets.

  ## Examples

      iex> QuackLake.Secret.list(conn)
      {:ok, [%{"name" => "my_s3", "type" => "s3", ...}]}

  """
  @spec list(Duckdbex.connection()) :: {:ok, [map()]} | {:error, term()}
  def list(conn) do
    Query.all(conn, "SELECT * FROM duckdb_secrets()")
  end

  @doc """
  Drops a secret by name.

  ## Examples

      iex> QuackLake.Secret.drop(conn, "my_s3")
      :ok

  """
  @spec drop(Duckdbex.connection(), String.t()) :: :ok | {:error, term()}
  def drop(conn, name) do
    Query.execute(conn, "DROP SECRET #{name}")
  end

  # Private functions

  defp maybe_add_endpoint(opts, nil), do: opts

  defp maybe_add_endpoint(opts, endpoint) do
    opts ++ ["ENDPOINT '#{escape_string(normalize_endpoint(endpoint))}'"]
  end

  defp maybe_add_ssl(opts, true), do: opts ++ ["USE_SSL true"]
  defp maybe_add_ssl(opts, false), do: opts ++ ["USE_SSL false"]

  defp maybe_add_url_style(opts, nil), do: opts
  defp maybe_add_url_style(opts, style), do: opts ++ ["URL_STYLE '#{escape_string(style)}'"]

  # DuckDB expects ENDPOINT as "host:port" without a scheme.
  # Strip http:// or https:// if the caller passes a full URL.
  defp normalize_endpoint(endpoint) do
    endpoint
    |> String.replace(~r{^https?://}, "")
    |> String.trim_trailing("/")
  end

  defp escape_string(str) do
    String.replace(str, "'", "''")
  end
end
