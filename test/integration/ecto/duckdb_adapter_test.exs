defmodule QuackLake.Integration.Ecto.DuckDBAdapterTest do
  @moduledoc """
  Integration tests for Ecto.Adapters.DuckDB.

  Tests basic Ecto operations with file-based DuckDB (no Docker required).
  """

  use ExUnit.Case, async: true

  # Define a test repo module
  defmodule TestRepo do
    use Ecto.Repo,
      otp_app: :quack_lake,
      adapter: Ecto.Adapters.DuckDB
  end

  # Define a test schema
  defmodule Product do
    use Ecto.Schema

    @primary_key {:id, :integer, autogenerate: false}
    schema "products" do
      field(:name, :string)
      field(:price, :decimal)
      field(:quantity, :integer)
    end
  end

  # Schema covering every type shape affected by the tuple-decode fix
  defmodule TypedRecord do
    use Ecto.Schema

    @primary_key {:id, :integer, autogenerate: false}
    schema "typed_records" do
      field(:on_date, :date)
      field(:at_time, :time)
      field(:at_time_usec, :time_usec)
      field(:happened_at, :naive_datetime)
      field(:logged_at, :utc_datetime)
      field(:amount, :decimal)
      field(:wide_amount, :decimal)
    end
  end

  setup do
    # Create a unique database file for each test
    db_path =
      Path.join(System.tmp_dir!(), "duckdb_test_#{System.unique_integer([:positive])}.duckdb")

    # Start the repo
    {:ok, pid} = TestRepo.start_link(database: db_path, pool_size: 1)

    on_exit(fn ->
      # Stop repo and cleanup - catch already stopped errors
      try do
        if Process.alive?(pid), do: GenServer.stop(pid)
      catch
        :exit, _ -> :ok
      end

      File.rm(db_path)
    end)

    {:ok, db_path: db_path, repo: TestRepo}
  end

  describe "DDL operations" do
    test "can create table with raw SQL", %{repo: repo} do
      result =
        Ecto.Adapters.SQL.query!(repo, """
          CREATE TABLE IF NOT EXISTS test_table (
            id INTEGER PRIMARY KEY,
            name VARCHAR
          )
        """)

      assert result.num_rows == 0
    end

    test "can describe table", %{repo: repo} do
      Ecto.Adapters.SQL.query!(repo, """
        CREATE TABLE IF NOT EXISTS describe_test (
          id INTEGER,
          name VARCHAR,
          active BOOLEAN
        )
      """)

      result = Ecto.Adapters.SQL.query!(repo, "DESCRIBE describe_test")

      assert length(result.rows) == 3
    end
  end

  describe "basic CRUD" do
    setup %{repo: repo} do
      Ecto.Adapters.SQL.query!(repo, """
        CREATE TABLE IF NOT EXISTS products (
          id INTEGER PRIMARY KEY,
          name VARCHAR,
          price DECIMAL(10, 2),
          quantity INTEGER
        )
      """)

      :ok
    end

    test "can insert records", %{repo: repo} do
      Ecto.Adapters.SQL.query!(repo, """
        INSERT INTO products (id, name, price, quantity)
        VALUES (1, 'Widget', 9.99, 100)
      """)

      result = Ecto.Adapters.SQL.query!(repo, "SELECT * FROM products WHERE id = 1")

      assert length(result.rows) == 1
      # Raw SQL reads return NIF-native terms: DECIMAL(10,2) is {value, width, scale}
      assert hd(result.rows) == [1, "Widget", {999, 10, 2}, 100]
    end

    test "can update records", %{repo: repo} do
      Ecto.Adapters.SQL.query!(repo, """
        INSERT INTO products (id, name, price, quantity) VALUES (1, 'Old Name', 5.00, 10)
      """)

      Ecto.Adapters.SQL.query!(repo, """
        UPDATE products SET name = 'New Name', price = 7.50 WHERE id = 1
      """)

      result = Ecto.Adapters.SQL.query!(repo, "SELECT name, price FROM products WHERE id = 1")

      assert hd(result.rows) == ["New Name", {750, 10, 2}]
    end

    test "can delete records", %{repo: repo} do
      Ecto.Adapters.SQL.query!(repo, """
        INSERT INTO products (id, name, price, quantity) VALUES (1, 'To Delete', 1.00, 1)
      """)

      Ecto.Adapters.SQL.query!(repo, "DELETE FROM products WHERE id = 1")

      result = Ecto.Adapters.SQL.query!(repo, "SELECT COUNT(*) FROM products WHERE id = 1")

      assert hd(result.rows) == [0]
    end

    test "can query with parameters", %{repo: repo} do
      Ecto.Adapters.SQL.query!(repo, """
        INSERT INTO products (id, name, price, quantity) VALUES
          (1, 'Cheap', 5.00, 100),
          (2, 'Medium', 15.00, 50),
          (3, 'Expensive', 50.00, 10)
      """)

      result =
        Ecto.Adapters.SQL.query!(repo, "SELECT name FROM products WHERE price > $1", [
          Decimal.new("10.00")
        ])

      assert length(result.rows) == 2
      names = Enum.map(result.rows, &hd/1) |> Enum.sort()
      assert names == ["Expensive", "Medium"]
    end
  end

  describe "transactions" do
    setup %{repo: repo} do
      Ecto.Adapters.SQL.query!(repo, """
        CREATE TABLE IF NOT EXISTS txn_test (id INTEGER, value VARCHAR)
      """)

      :ok
    end

    test "commits successful transaction", %{repo: repo} do
      TestRepo.transaction(fn ->
        Ecto.Adapters.SQL.query!(repo, "INSERT INTO txn_test VALUES (1, 'committed')")
      end)

      result = Ecto.Adapters.SQL.query!(repo, "SELECT * FROM txn_test")

      assert hd(result.rows) == [1, "committed"]
    end

    test "rolls back failed transaction", %{repo: repo} do
      try do
        TestRepo.transaction(fn ->
          Ecto.Adapters.SQL.query!(repo, "INSERT INTO txn_test VALUES (1, 'should_rollback')")
          TestRepo.rollback(:intentional)
        end)
      catch
        :error, _ -> :ok
      end

      result = Ecto.Adapters.SQL.query!(repo, "SELECT COUNT(*) FROM txn_test")

      assert hd(result.rows) == [0]
    end
  end

  describe "DuckDB-specific features" do
    test "can use aggregate functions", %{repo: repo} do
      Ecto.Adapters.SQL.query!(repo, """
        CREATE TABLE agg_test AS SELECT i AS value FROM range(100) t(i)
      """)

      result =
        Ecto.Adapters.SQL.query!(repo, """
          SELECT
            COUNT(*) AS cnt,
            SUM(value) AS total,
            AVG(value) AS average,
            MIN(value) AS minimum,
            MAX(value) AS maximum
          FROM agg_test
        """)

      [cnt, total, avg, min, max] = hd(result.rows)

      assert cnt == 100
      assert total == 4950
      assert avg == 49.5
      assert min == 0
      assert max == 99
    end

    test "can use window functions", %{repo: repo} do
      Ecto.Adapters.SQL.query!(repo, """
        CREATE TABLE window_test (category VARCHAR, value INTEGER)
      """)

      Ecto.Adapters.SQL.query!(repo, """
        INSERT INTO window_test VALUES
          ('A', 10), ('A', 20), ('A', 30),
          ('B', 15), ('B', 25)
      """)

      result =
        Ecto.Adapters.SQL.query!(repo, """
          SELECT category, value, SUM(value) OVER (PARTITION BY category) AS category_total
          FROM window_test
          ORDER BY category, value
        """)

      assert length(result.rows) == 5
      # A category totals 60, B category totals 40
      assert Enum.at(result.rows, 0) == ["A", 10, 60]
      assert Enum.at(result.rows, 3) == ["B", 15, 40]
    end
  end

  describe "typed round-trips" do
    setup %{repo: repo} do
      Ecto.Adapters.SQL.query!(repo, """
        CREATE TABLE IF NOT EXISTS typed_records (
          id INTEGER PRIMARY KEY,
          on_date DATE,
          at_time TIME,
          at_time_usec TIME,
          happened_at TIMESTAMP,
          logged_at TIMESTAMP,
          amount DECIMAL(10, 2),
          wide_amount DECIMAL(30, 10)
        )
      """)

      :ok
    end

    test "date, times, datetimes and decimals round-trip exactly" do
      TestRepo.insert!(%TypedRecord{
        id: 1,
        on_date: ~D[2026-07-18],
        at_time: ~T[01:02:03],
        at_time_usec: ~T[01:02:03.456789],
        happened_at: ~N[2026-07-18 01:02:03],
        logged_at: ~U[2026-07-18 01:02:03Z],
        amount: Decimal.new("12.34"),
        wide_amount: Decimal.new("1234567890123456789.1234567890")
      })

      assert [loaded] = TestRepo.all(TypedRecord)
      assert loaded.on_date == ~D[2026-07-18]
      assert loaded.at_time == ~T[01:02:03]
      assert loaded.at_time_usec == ~T[01:02:03.456789]
      assert loaded.happened_at == ~N[2026-07-18 01:02:03]
      assert loaded.logged_at == ~U[2026-07-18 01:02:03Z]
      assert loaded.amount == Decimal.new("12.34")
      assert loaded.wide_amount == Decimal.new("1234567890123456789.1234567890")
    end

    test "negative decimals keep sign and exact digits" do
      TestRepo.insert!(%TypedRecord{
        id: 2,
        amount: Decimal.new("-99.05"),
        wide_amount: Decimal.new("-1234567890123456789.1234567890")
      })

      assert [loaded] = TestRepo.all(TypedRecord)
      assert loaded.amount == Decimal.new("-99.05")
      assert loaded.wide_amount == Decimal.new("-1234567890123456789.1234567890")
    end
  end

  describe "raw value semantics (D1)" do
    test "raw DATE read returns the NIF-native tuple, not a corrupted Decimal", %{repo: repo} do
      result = Ecto.Adapters.SQL.query!(repo, "SELECT DATE '2026-07-18'")

      assert result.rows == [[{2026, 7, 18}]]
    end

    test "raw INTERVAL read returns the NIF-native tuple, not a Decimal", %{repo: repo} do
      result = Ecto.Adapters.SQL.query!(repo, "SELECT INTERVAL 3 MONTH + INTERVAL 5 DAY")

      assert result.rows == [[{3, 5, 0}]]
    end

    test "raw wide DECIMAL read returns the {low, high} hugeint pair", %{repo: repo} do
      result =
        Ecto.Adapters.SQL.query!(
          repo,
          "SELECT 1234567890123456789.1234567890::DECIMAL(30, 10)"
        )

      # Pair order is {low, high} — REVERSED from bare HUGEINT results
      assert result.rows == [[{{5_097_733_593_236_747_986, 669_260_594}, 30, 10}]]
    end

    test "HUGEINT still decodes to an integer", %{repo: repo} do
      result =
        Ecto.Adapters.SQL.query!(
          repo,
          "SELECT 170141183460469231731687303715884105727::HUGEINT"
        )

      assert result.rows == [[170_141_183_460_469_231_731_687_303_715_884_105_727]]
    end

    test "UUID still decodes to a string", %{repo: repo} do
      result =
        Ecto.Adapters.SQL.query!(
          repo,
          "SELECT 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::UUID"
        )

      assert result.rows == [["a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"]]
    end
  end

  describe "raw struct params" do
    setup %{repo: repo} do
      Ecto.Adapters.SQL.query!(repo, """
        CREATE TABLE IF NOT EXISTS raw_params (
          id INTEGER,
          on_date DATE,
          at_time TIME,
          happened_at TIMESTAMP
        )
      """)

      Ecto.Adapters.SQL.query!(repo, """
        INSERT INTO raw_params VALUES
          (1, DATE '2026-07-18', TIME '01:02:03', TIMESTAMP '2026-07-18 01:02:03')
      """)

      :ok
    end

    test "%Date{} param is accepted", %{repo: repo} do
      result =
        Ecto.Adapters.SQL.query!(
          repo,
          "SELECT count(*) FROM raw_params WHERE on_date = $1",
          [~D[2026-07-18]]
        )

      assert result.rows == [[1]]
    end

    test "%Time{} param is accepted", %{repo: repo} do
      result =
        Ecto.Adapters.SQL.query!(
          repo,
          "SELECT count(*) FROM raw_params WHERE at_time = $1",
          [~T[01:02:03]]
        )

      assert result.rows == [[1]]
    end

    test "%NaiveDateTime{} param is accepted", %{repo: repo} do
      result =
        Ecto.Adapters.SQL.query!(
          repo,
          "SELECT count(*) FROM raw_params WHERE happened_at = $1",
          [~N[2026-07-18 01:02:03]]
        )

      assert result.rows == [[1]]
    end

    test "%DateTime{} param is accepted", %{repo: repo} do
      result =
        Ecto.Adapters.SQL.query!(
          repo,
          "SELECT count(*) FROM raw_params WHERE happened_at = $1",
          [~U[2026-07-18 01:02:03Z]]
        )

      assert result.rows == [[1]]
    end

    test "non-UTC %DateTime{} param is shifted to UTC, not wall-clock", %{repo: repo} do
      # Same instant as TIMESTAMP '2026-07-18 01:02:03' UTC, expressed at +02:00
      plus_two = %DateTime{
        year: 2026,
        month: 7,
        day: 18,
        hour: 3,
        minute: 2,
        second: 3,
        microsecond: {0, 0},
        time_zone: "Etc/GMT-2",
        zone_abbr: "+02",
        utc_offset: 7200,
        std_offset: 0
      }

      result =
        Ecto.Adapters.SQL.query!(
          repo,
          "SELECT count(*) FROM raw_params WHERE happened_at = $1",
          [plus_two]
        )

      assert result.rows == [[1]]
    end

    test "%Decimal{} param writes wide decimals losslessly", %{repo: repo} do
      Ecto.Adapters.SQL.query!(repo, "CREATE TABLE wide (v DECIMAL(30, 10))")

      Ecto.Adapters.SQL.query!(repo, "INSERT INTO wide VALUES ($1)", [
        Decimal.new("1234567890123456789.1234567890")
      ])

      result = Ecto.Adapters.SQL.query!(repo, "SELECT CAST(v AS VARCHAR) FROM wide")

      assert result.rows == [["1234567890123456789.1234567890"]]
    end
  end
end
