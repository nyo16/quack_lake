defmodule QuackLake.ConnectionCoreFunctionsTest do
  use ExUnit.Case, async: true

  alias QuackLake.{Config, Connection}

  defp open_raw_conn do
    {:ok, db} = Duckdbex.open()
    {:ok, conn} = Duckdbex.connection(db)
    conn
  end

  defp sum_result(conn) do
    case Duckdbex.query(conn, "SELECT sum(x) FROM (VALUES (1), (2)) t(x)") do
      {:ok, ref} -> {:ok, Duckdbex.fetch_all(ref)}
      {:error, reason} -> {:error, reason}
    end
  end

  describe "ensure_core_functions/2" do
    test "loads core_functions so aggregate functions work" do
      conn = open_raw_conn()

      assert :ok = Connection.ensure_core_functions(conn, Config.new())
      assert {:ok, [[_sum]]} = sum_result(conn)
    end

    test "skips everything when both auto flags are disabled" do
      conn = open_raw_conn()

      config = Config.new(auto_install_extensions: false, auto_load_extensions: false)

      assert :ok = Connection.ensure_core_functions(conn, config)
      assert {:error, reason} = sum_result(conn)
      assert reason =~ "core_functions"
    end

    test "installs without loading when auto_load_extensions is disabled" do
      conn = open_raw_conn()

      config = Config.new(auto_install_extensions: true, auto_load_extensions: false)

      assert :ok = Connection.ensure_core_functions(conn, config)
      assert {:error, reason} = sum_result(conn)
      assert reason =~ "core_functions"
    end

    test "load-only succeeds when the extension is already installed" do
      # Warm the shared on-disk extension cache so LOAD without INSTALL succeeds
      warm_conn = open_raw_conn()
      assert :ok = Connection.ensure_core_functions(warm_conn, Config.new())

      conn = open_raw_conn()
      config = Config.new(auto_install_extensions: false, auto_load_extensions: true)

      assert :ok = Connection.ensure_core_functions(conn, config)
      assert {:ok, [[_sum]]} = sum_result(conn)
    end

    test "connection opened via Connection.open/1 has core functions available" do
      {:ok, conn} = Connection.open()

      assert {:ok, [[_sum]]} = sum_result(conn)
    end
  end
end
