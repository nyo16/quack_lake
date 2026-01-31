defmodule TestApp.Demos.AppenderDemo do
  @moduledoc """
  Demonstrates the high-performance Appender API for bulk inserts.
  """

  def run do
    IO.puts("\n" <> String.duplicate("=", 60))
    IO.puts("APPENDER API DEMO (Bulk Inserts)")
    IO.puts(String.duplicate("=", 60))

    {:ok, conn} = QuackLake.open()

    setup_table(conn)
    demo_single_append(conn)
    demo_batch_append(conn)
    demo_performance_comparison(conn)

    IO.puts("\n✓ Appender demo complete!\n")
  end

  defp setup_table(conn) do
    IO.puts("\n--- Setting up table ---")

    :ok =
      QuackLake.Query.execute(conn, """
        CREATE TABLE bulk_products (
          id INTEGER,
          name TEXT,
          sku TEXT,
          price DOUBLE,
          quantity INTEGER
        )
      """)

    IO.puts("  Created bulk_products table")
  end

  defp demo_single_append(conn) do
    IO.puts("\n--- Single Row Append ---")

    {:ok, appender} = QuackLake.Appender.new(conn, "bulk_products")
    IO.puts("  Created appender for 'bulk_products'")

    # Append single rows
    :ok = QuackLake.Appender.append(appender, [1, "Widget", "WDG-001", 9.99, 100])
    :ok = QuackLake.Appender.append(appender, [2, "Gadget", "GDG-002", 19.99, 50])
    :ok = QuackLake.Appender.append(appender, [3, "Gizmo", "GZM-003", 29.99, 25])
    IO.puts("  Appended 3 rows individually")

    :ok = QuackLake.Appender.close(appender)
    IO.puts("  Closed appender (flushed data)")

    {:ok, rows} = QuackLake.query(conn, "SELECT COUNT(*) AS cnt FROM bulk_products")
    IO.puts("  Row count: #{hd(rows)["cnt"]}")
  end

  defp demo_batch_append(conn) do
    IO.puts("\n--- Batch Append (Multiple Rows) ---")

    {:ok, appender} = QuackLake.Appender.new(conn, "bulk_products")

    # Prepare batch data
    rows =
      for i <- 4..13 do
        [i, "Product #{i}", "PRD-#{String.pad_leading(to_string(i), 3, "0")}", i * 1.5, i * 10]
      end

    :ok = QuackLake.Appender.append_rows(appender, rows)
    IO.puts("  Appended batch of #{length(rows)} rows")

    :ok = QuackLake.Appender.close(appender)

    {:ok, result} = QuackLake.query(conn, "SELECT COUNT(*) AS cnt FROM bulk_products")
    IO.puts("  Total row count: #{hd(result)["cnt"]}")
  end

  defp demo_performance_comparison(conn) do
    IO.puts("\n--- Performance Comparison ---")

    # Clean up
    :ok = QuackLake.Query.execute(conn, "DELETE FROM bulk_products")

    row_count = 10_000

    # INSERT approach
    insert_time =
      :timer.tc(fn ->
        :ok =
          QuackLake.Query.execute(conn, """
            INSERT INTO bulk_products
            SELECT
              i,
              'Product ' || i,
              'SKU-' || lpad(i::TEXT, 5, '0'),
              i * 0.99,
              i * 10
            FROM range(#{row_count}) t(i)
          """)
      end)
      |> elem(0)
      |> Kernel./(1000)

    IO.puts("  INSERT #{row_count} rows: #{Float.round(insert_time, 2)} ms")

    # Clean up
    :ok = QuackLake.Query.execute(conn, "DELETE FROM bulk_products")

    # Appender approach
    appender_time =
      :timer.tc(fn ->
        {:ok, appender} = QuackLake.Appender.new(conn, "bulk_products")

        rows =
          for i <- 0..(row_count - 1) do
            [
              i,
              "Product #{i}",
              "SKU-#{String.pad_leading(to_string(i), 5, "0")}",
              i * 0.99,
              i * 10
            ]
          end

        # Append in batches
        rows
        |> Enum.chunk_every(1000)
        |> Enum.each(fn batch ->
          :ok = QuackLake.Appender.append_rows(appender, batch)
        end)

        :ok = QuackLake.Appender.close(appender)
      end)
      |> elem(0)
      |> Kernel./(1000)

    IO.puts("  Appender #{row_count} rows: #{Float.round(appender_time, 2)} ms")

    if insert_time > 0 do
      ratio = Float.round(insert_time / appender_time, 1)
      IO.puts("  Appender is ~#{ratio}x faster for this workload")
    end

    # Verify
    {:ok, result} = QuackLake.query(conn, "SELECT COUNT(*) AS cnt FROM bulk_products")
    IO.puts("  Final row count: #{hd(result)["cnt"]}")
  end
end
