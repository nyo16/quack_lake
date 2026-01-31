defmodule TestApp.Demos.ExtensionsDemo do
  @moduledoc """
  Demonstrates DuckDB extension management.
  """

  def run do
    IO.puts("\n" <> String.duplicate("=", 60))
    IO.puts("DUCKDB EXTENSIONS DEMO")
    IO.puts(String.duplicate("=", 60))

    {:ok, conn} = QuackLake.open()

    demo_ensure_extension(conn)
    demo_extension_config()
    demo_available_extensions(conn)

    IO.puts("\n✓ Extensions demo complete!\n")
  end

  defp demo_ensure_extension(conn) do
    IO.puts("\n--- QuackLake.Extension.ensure/2 ---")

    :ok = QuackLake.Extension.ensure(conn, "json")
    IO.puts("  Loaded 'json' extension")

    # Use JSON functions
    {:ok, rows} =
      QuackLake.query(conn, """
        SELECT json_extract('{"name": "Alice", "age": 30}', '$.name') AS name
      """)

    IO.puts("  JSON extract result: #{inspect(hd(rows)["name"])}")

    :ok = QuackLake.Extension.ensure(conn, "httpfs")
    IO.puts("  Loaded 'httpfs' extension (for S3/HTTP access)")
  end

  defp demo_extension_config do
    IO.puts("\n--- Extension Configuration Parsing ---")

    # Simple atom extension
    ext1 = QuackLake.Config.Extension.parse(:httpfs)

    IO.puts(
      "  :httpfs -> install: #{ext1.install}, load: #{ext1.load}, source: #{inspect(ext1.source)}"
    )

    # Extension with source
    ext2 = QuackLake.Config.Extension.parse({:spatial, source: :core})
    IO.puts("  {:spatial, source: :core} -> source: #{ext2.source}")

    # Extension with options
    ext3 = QuackLake.Config.Extension.parse({:ducklake, source: :core, install: true, load: true})
    IO.puts("  {:ducklake, source: :core, ...} -> source: #{ext3.source}")

    # SQL generation
    IO.puts("\n  Generated SQL:")
    IO.puts("    #{QuackLake.Config.Extension.install_sql(ext1)}")
    IO.puts("    #{QuackLake.Config.Extension.install_sql(ext2)}")
    IO.puts("    #{QuackLake.Config.Extension.load_sql(ext1)}")
  end

  defp demo_available_extensions(conn) do
    IO.puts("\n--- Available Extensions ---")

    {:ok, rows} =
      QuackLake.query(conn, """
        SELECT extension_name, installed, loaded
        FROM duckdb_extensions()
        WHERE installed = true
        ORDER BY extension_name
        LIMIT 10
      """)

    IO.puts("  Installed extensions:")

    for row <- rows do
      status = if row["loaded"], do: "loaded", else: "not loaded"
      IO.puts("    - #{row["extension_name"]} (#{status})")
    end

    IO.puts("\n  Common extensions you can load:")
    IO.puts("    - httpfs    : HTTP/S3 file access")
    IO.puts("    - json      : JSON parsing and extraction")
    IO.puts("    - spatial   : Geospatial types and functions")
    IO.puts("    - parquet   : Parquet file support")
    IO.puts("    - postgres_scanner : Query PostgreSQL directly")
    IO.puts("    - sqlite_scanner   : Query SQLite databases")
    IO.puts("    - excel     : Read Excel files")
    IO.puts("    - ducklake  : DuckLake lakehouse format")
  end
end
