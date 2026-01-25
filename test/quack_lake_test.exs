defmodule QuackLakeTest do
  use ExUnit.Case, async: true

  @moduletag :integration

  setup do
    {:ok, conn} = QuackLake.open()
    %{conn: conn}
  end

  describe "open/1" do
    test "opens an in-memory connection with ducklake loaded" do
      assert {:ok, conn} = QuackLake.open()
      assert is_reference(conn)

      # Verify ducklake extension is loaded
      {:ok, extensions} =
        QuackLake.query(
          conn,
          "SELECT * FROM duckdb_extensions() WHERE extension_name = 'ducklake'"
        )

      assert length(extensions) == 1
      assert hd(extensions)["loaded"] == true
    end

    test "open!/1 returns connection directly" do
      conn = QuackLake.open!()
      assert is_reference(conn)
    end
  end

  describe "attach/4 and detach/2" do
    test "attaches and detaches a ducklake", %{conn: conn} do
      ducklake_path = Path.join(System.tmp_dir!(), "test_#{:rand.uniform(100_000)}.ducklake")

      assert :ok = QuackLake.attach(conn, "test_lake", ducklake_path)

      # Verify it's attached
      {:ok, lakes} = QuackLake.lakes(conn)
      assert Enum.any?(lakes, &(&1["name"] == "test_lake"))

      # Detach
      assert :ok = QuackLake.detach(conn, "test_lake")

      # Verify it's detached
      {:ok, lakes} = QuackLake.lakes(conn)
      refute Enum.any?(lakes, &(&1["name"] == "test_lake"))
    after
      # Cleanup
      File.rm(Path.join(System.tmp_dir!(), "test_*.ducklake"))
    end
  end

  describe "query/3" do
    test "executes a simple query and returns maps", %{conn: conn} do
      assert {:ok, [%{"num" => 1, "greeting" => "hello"}]} =
               QuackLake.query(conn, "SELECT 1 as num, 'hello' as greeting")
    end

    test "executes a query with parameters", %{conn: conn} do
      # DuckDB requires explicit types for arithmetic with params
      assert {:ok, [%{"result" => 42}]} =
               QuackLake.query(conn, "SELECT $1::INTEGER + $2::INTEGER as result", [40, 2])
    end

    test "query!/3 returns rows directly", %{conn: conn} do
      assert [%{"num" => 1}] = QuackLake.query!(conn, "SELECT 1 as num")
    end

    test "query!/3 raises on error", %{conn: conn} do
      assert_raise QuackLake.Error, fn ->
        QuackLake.query!(conn, "SELECT * FROM nonexistent_table")
      end
    end
  end

  describe "query_one/3" do
    test "returns the first row", %{conn: conn} do
      # Use ORDER BY to ensure deterministic results
      assert {:ok, %{"num" => 1}} =
               QuackLake.query_one(conn, "SELECT 1 as num UNION SELECT 2 ORDER BY num")
    end

    test "returns nil for empty results", %{conn: conn} do
      assert {:ok, nil} = QuackLake.query_one(conn, "SELECT 1 WHERE false")
    end

    test "query_one!/3 returns row directly", %{conn: conn} do
      assert %{"num" => 1} = QuackLake.query_one!(conn, "SELECT 1 as num")
    end
  end

  describe "Query module" do
    test "execute/3 runs statements without results", %{conn: conn} do
      assert :ok = QuackLake.Query.execute(conn, "CREATE TABLE test_exec (id INT)")
      assert :ok = QuackLake.Query.execute(conn, "INSERT INTO test_exec VALUES ($1)", [1])
      assert {:ok, [%{"id" => 1}]} = QuackLake.query(conn, "SELECT * FROM test_exec")
    end

    test "stream/3 returns chunks of results", %{conn: conn} do
      # Create some test data
      :ok = QuackLake.Query.execute(conn, "CREATE TABLE stream_test (id INT)")

      for i <- 1..100 do
        :ok = QuackLake.Query.execute(conn, "INSERT INTO stream_test VALUES ($1)", [i])
      end

      chunks =
        QuackLake.Query.stream(conn, "SELECT * FROM stream_test ORDER BY id")
        |> Enum.to_list()

      # DuckDB may return all rows in one chunk for small datasets
      assert length(chunks) >= 1

      # All rows should be present
      all_ids =
        chunks
        |> List.flatten()
        |> Enum.map(& &1["id"])
        |> Enum.sort()

      assert all_ids == Enum.to_list(1..100)
    end
  end

  describe "DuckLake operations" do
    setup %{conn: conn} do
      ducklake_path = Path.join(System.tmp_dir!(), "lake_test_#{:rand.uniform(100_000)}.ducklake")
      :ok = QuackLake.attach(conn, "test", ducklake_path)

      on_exit(fn ->
        File.rm(ducklake_path)
      end)

      %{conn: conn, lake_path: ducklake_path}
    end

    test "creates table and inserts data", %{conn: conn} do
      :ok = QuackLake.Query.execute(conn, "CREATE TABLE test.items (id INT, name TEXT)")
      :ok = QuackLake.Query.execute(conn, "INSERT INTO test.items VALUES (1, 'hello')")
      :ok = QuackLake.Query.execute(conn, "INSERT INTO test.items VALUES (2, 'world')")

      {:ok, rows} = QuackLake.query(conn, "SELECT * FROM test.items ORDER BY id")
      assert rows == [%{"id" => 1, "name" => "hello"}, %{"id" => 2, "name" => "world"}]
    end

    test "lists snapshots", %{conn: conn} do
      :ok = QuackLake.Query.execute(conn, "CREATE TABLE test.snap_test (id INT)")
      :ok = QuackLake.Query.execute(conn, "INSERT INTO test.snap_test VALUES (1)")

      {:ok, snapshots} = QuackLake.snapshots(conn, "test")
      assert is_list(snapshots)
      assert length(snapshots) >= 1
    end
  end
end
