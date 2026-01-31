defmodule TestApp.Demos.LakeManagementDemo do
  @moduledoc """
  Demonstrates DuckLake attach/detach/list operations.

  Requires Docker services (PostgreSQL + MinIO) to be running.
  """

  def run do
    IO.puts("\n" <> String.duplicate("=", 60))
    IO.puts("LAKE MANAGEMENT DEMO")
    IO.puts(String.duplicate("=", 60))
    IO.puts("Note: This demo requires Docker services (docker-compose up -d)")

    {:ok, conn} = QuackLake.open()

    case check_docker_services() do
      :ok ->
        setup_s3_secret(conn)
        demo_local_lake(conn)
        demo_postgres_catalog(conn)
        demo_list_lakes(conn)
        IO.puts("\n✓ Lake management demo complete!\n")

      {:error, reason} ->
        IO.puts("\n⚠ Skipping demo: #{reason}")
        IO.puts("  Run 'docker-compose up -d' from the quack_lake root directory\n")
    end
  end

  defp check_docker_services do
    pg_config = Application.get_env(:test_app, :postgres, [])

    case System.cmd("pg_isready", [
           "-h",
           pg_config[:host] || "localhost",
           "-p",
           to_string(pg_config[:port] || 5432),
           "-U",
           pg_config[:username] || "quacklake"
         ]) do
      {_, 0} -> :ok
      _ -> {:error, "PostgreSQL not available"}
    end
  rescue
    _ -> {:error, "pg_isready command not found"}
  end

  defp setup_s3_secret(conn) do
    s3_config = Application.get_env(:test_app, :s3, [])

    if s3_config[:access_key_id] do
      endpoint = s3_config[:endpoint] |> String.replace(~r{^https?://}, "")

      QuackLake.Secret.create_s3(conn, "lake_s3",
        key_id: s3_config[:access_key_id],
        secret: s3_config[:secret_access_key],
        region: s3_config[:region] || "us-east-1",
        endpoint: endpoint,
        use_ssl: false,
        url_style: "path"
      )
    end
  end

  defp demo_local_lake(conn) do
    IO.puts("\n--- Local DuckLake (file-based) ---")

    path = "/tmp/demo_local.ducklake"
    File.rm(path)

    :ok = QuackLake.attach(conn, "local_lake", path)
    IO.puts("  Attached local lake at #{path}")

    :ok =
      QuackLake.Query.execute(conn, """
        CREATE TABLE local_lake.main.items (id INTEGER, name TEXT)
      """)

    :ok =
      QuackLake.Query.execute(
        conn,
        "INSERT INTO local_lake.main.items VALUES (1, 'Local Item')"
      )

    {:ok, rows} = QuackLake.query(conn, "SELECT * FROM local_lake.main.items")
    IO.puts("  Query result: #{inspect(rows)}")

    :ok = QuackLake.detach(conn, "local_lake")
    IO.puts("  Detached local_lake")

    File.rm(path)
  end

  defp demo_postgres_catalog(conn) do
    IO.puts("\n--- PostgreSQL Catalog + S3 Storage ---")

    pg_config = Application.get_env(:test_app, :postgres, [])
    s3_config = Application.get_env(:test_app, :s3, [])

    # Build catalog string
    catalog_string =
      "postgres:host=#{pg_config[:host]};port=#{pg_config[:port]};database=#{pg_config[:database]};user=#{pg_config[:username]};password=#{pg_config[:password]}"

    lake_name = "pg_lake_#{System.system_time(:millisecond)}"
    data_path = "s3://#{s3_config[:bucket]}/demo_lake_data"

    # Attach with PostgreSQL catalog and S3 data path
    attach_sql =
      "ATTACH '#{catalog_string};ducklake_alias=#{lake_name}' AS #{lake_name} (TYPE DUCKLAKE, DATA_PATH '#{data_path}')"

    :ok = QuackLake.Query.execute(conn, attach_sql)
    IO.puts("  Attached lake with PostgreSQL catalog")
    IO.puts("    catalog: PostgreSQL (#{pg_config[:host]}:#{pg_config[:port]})")
    IO.puts("    data_path: #{data_path}")

    # Create table and insert data
    :ok =
      QuackLake.Query.execute(conn, """
        CREATE TABLE #{lake_name}.main.products (
          id INTEGER,
          name TEXT,
          price DECIMAL(10, 2)
        )
      """)

    :ok =
      QuackLake.Query.execute(conn, """
        INSERT INTO #{lake_name}.main.products VALUES
          (1, 'Widget', 9.99),
          (2, 'Gadget', 19.99)
      """)

    {:ok, rows} = QuackLake.query(conn, "SELECT * FROM #{lake_name}.main.products ORDER BY id")
    IO.puts("  Inserted and queried products:")

    for row <- rows do
      IO.puts("    #{row["id"]}: #{row["name"]} - $#{row["price"]}")
    end

    # Detach
    :ok = QuackLake.Query.execute(conn, "DETACH #{lake_name}")
    IO.puts("  Detached #{lake_name}")
  end

  defp demo_list_lakes(conn) do
    IO.puts("\n--- List Attached Databases ---")

    {:ok, rows} =
      QuackLake.query(conn, """
        SELECT database_name, path, type
        FROM duckdb_databases()
        WHERE internal = false
      """)

    IO.puts("  Attached databases:")

    for row <- rows do
      IO.puts("    - #{row["database_name"]} (#{row["type"]})")
    end
  end
end
