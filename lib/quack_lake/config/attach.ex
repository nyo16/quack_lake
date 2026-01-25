defmodule QuackLake.Config.Attach do
  @moduledoc """
  Attach configuration parser for DuckDB database attachments.

  Attachments allow connecting to external databases or files.

  ## Configuration Format

      attach: [
        {"path/to/database.duckdb", as: :analytics},
        {"s3://bucket/data.parquet", as: :remote_data, read_only: true},
        {"postgres://host/db", as: :pg, type: :postgres}
      ]

  ## Options

    * `:as` - Alias name for the attached database (required)
    * `:type` - Database type (`:duckdb`, `:postgres`, `:sqlite`, `:ducklake`)
    * `:read_only` - Whether to attach in read-only mode
    * `:data_path` - Data path for DuckLake attachments

  """

  @type attach_type :: :duckdb | :postgres | :sqlite | :ducklake | nil

  defstruct [:path, :alias, :type, :read_only, :data_path, :options]

  @type t :: %__MODULE__{
          path: String.t(),
          alias: atom(),
          type: attach_type(),
          read_only: boolean() | nil,
          data_path: String.t() | nil,
          options: keyword()
        }

  @doc """
  Parses an attach configuration into an Attach struct.

  ## Examples

      iex> QuackLake.Config.Attach.parse({"data.duckdb", as: :analytics})
      %QuackLake.Config.Attach{path: "data.duckdb", alias: :analytics, type: nil, read_only: nil, data_path: nil, options: [as: :analytics]}

      iex> QuackLake.Config.Attach.parse({"lake.ducklake", as: :lake, type: :ducklake, data_path: "s3://bucket/data"})
      %QuackLake.Config.Attach{path: "lake.ducklake", alias: :lake, type: :ducklake, read_only: nil, data_path: "s3://bucket/data", options: [as: :lake, type: :ducklake, data_path: "s3://bucket/data"]}

  """
  @spec parse({String.t(), keyword()}) :: t()
  def parse({path, opts}) when is_binary(path) and is_list(opts) do
    %__MODULE__{
      path: path,
      alias: Keyword.fetch!(opts, :as),
      type: opts[:type],
      read_only: opts[:read_only],
      data_path: opts[:data_path],
      options: opts
    }
  end

  @doc """
  Generates the ATTACH SQL statement for this configuration.

  ## Examples

      iex> attach = QuackLake.Config.Attach.parse({"data.duckdb", as: :analytics})
      iex> QuackLake.Config.Attach.attach_sql(attach)
      "ATTACH 'data.duckdb' AS analytics"

      iex> attach = QuackLake.Config.Attach.parse({"data.duckdb", as: :analytics, read_only: true})
      iex> QuackLake.Config.Attach.attach_sql(attach)
      "ATTACH 'data.duckdb' AS analytics (READ_ONLY)"

      iex> attach = QuackLake.Config.Attach.parse({"lake.ducklake", as: :lake, type: :ducklake, data_path: "s3://bucket/data"})
      iex> QuackLake.Config.Attach.attach_sql(attach)
      "ATTACH 'lake.ducklake' AS lake (TYPE DUCKLAKE, DATA_PATH 's3://bucket/data')"

  """
  @spec attach_sql(t()) :: String.t()
  def attach_sql(%__MODULE__{} = attach) do
    base = "ATTACH '#{escape_string(attach.path)}' AS #{attach.alias}"
    options = build_options(attach)

    case options do
      [] -> base
      opts -> "#{base} (#{Enum.join(opts, ", ")})"
    end
  end

  @doc """
  Parses a list of attach configurations.

  ## Examples

      iex> configs = [{"data.duckdb", as: :analytics}, {"other.duckdb", as: :other}]
      iex> QuackLake.Config.Attach.parse_all(configs)
      [
        %QuackLake.Config.Attach{path: "data.duckdb", alias: :analytics, type: nil, read_only: nil, data_path: nil, options: [as: :analytics]},
        %QuackLake.Config.Attach{path: "other.duckdb", alias: :other, type: nil, read_only: nil, data_path: nil, options: [as: :other]}
      ]

  """
  @spec parse_all([{String.t(), keyword()}]) :: [t()]
  def parse_all(attachments) when is_list(attachments) do
    Enum.map(attachments, &parse/1)
  end

  defp build_options(%__MODULE__{} = attach) do
    []
    |> maybe_add_type(attach.type)
    |> maybe_add_read_only(attach.read_only)
    |> maybe_add_data_path(attach.data_path)
  end

  defp maybe_add_type(opts, nil), do: opts
  defp maybe_add_type(opts, :duckdb), do: opts
  defp maybe_add_type(opts, :postgres), do: opts ++ ["TYPE POSTGRES"]
  defp maybe_add_type(opts, :sqlite), do: opts ++ ["TYPE SQLITE"]
  defp maybe_add_type(opts, :ducklake), do: opts ++ ["TYPE DUCKLAKE"]

  defp maybe_add_read_only(opts, nil), do: opts
  defp maybe_add_read_only(opts, false), do: opts
  defp maybe_add_read_only(opts, true), do: opts ++ ["READ_ONLY"]

  defp maybe_add_data_path(opts, nil), do: opts
  defp maybe_add_data_path(opts, path), do: opts ++ ["DATA_PATH '#{escape_string(path)}'"]

  defp escape_string(str) do
    String.replace(str, "'", "''")
  end
end
