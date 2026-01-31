defmodule QuackLake.Integration.Ecto.DuckDBAdapterTest do
  @moduledoc """
  Integration tests for Ecto.Adapters.DuckDB.

  Tests basic Ecto operations with file-based DuckDB (no Docker required).
  """

  use ExUnit.Case, async: true

  alias QuackLake.Config

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

  setup do
    # Create a unique database file for each test
    db_path =
      Path.join(System.tmp_dir!(), "duckdb_test_#{System.unique_integer([:positive])}.duckdb")

    # Start the repo
    {:ok, pid} = TestRepo.start_link(database: db_path, pool_size: 1)

    on_exit(fn ->
      # Stop repo and cleanup
      if Process.alive?(pid), do: GenServer.stop(pid)
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
      assert hd(result.rows) == [1, "Widget", Decimal.new("9.99"), 100]
    end

    test "can update records", %{repo: repo} do
      Ecto.Adapters.SQL.query!(repo, """
        INSERT INTO products (id, name, price, quantity) VALUES (1, 'Old Name', 5.00, 10)
      """)

      Ecto.Adapters.SQL.query!(repo, """
        UPDATE products SET name = 'New Name', price = 7.50 WHERE id = 1
      """)

      result = Ecto.Adapters.SQL.query!(repo, "SELECT name, price FROM products WHERE id = 1")

      assert hd(result.rows) == ["New Name", Decimal.new("7.50")]
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
end
