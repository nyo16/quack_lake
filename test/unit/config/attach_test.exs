defmodule QuackLake.Config.AttachTest do
  use ExUnit.Case, async: true

  alias QuackLake.Config.Attach

  describe "parse/1" do
    test "parses basic attachment" do
      attach = Attach.parse({"data.duckdb", as: :analytics})

      assert attach.path == "data.duckdb"
      assert attach.alias == :analytics
      assert attach.type == nil
      assert attach.read_only == nil
      assert attach.data_path == nil
    end

    test "parses attachment with type" do
      attach = Attach.parse({"postgres://host/db", as: :pg, type: :postgres})

      assert attach.path == "postgres://host/db"
      assert attach.alias == :pg
      assert attach.type == :postgres
    end

    test "parses attachment with read_only" do
      attach = Attach.parse({"data.duckdb", as: :analytics, read_only: true})

      assert attach.path == "data.duckdb"
      assert attach.alias == :analytics
      assert attach.read_only == true
    end

    test "parses attachment with read_only false" do
      attach = Attach.parse({"data.duckdb", as: :analytics, read_only: false})

      assert attach.read_only == false
    end

    test "parses ducklake attachment with data_path" do
      attach =
        Attach.parse({"lake.ducklake", as: :lake, type: :ducklake, data_path: "s3://bucket/data"})

      assert attach.path == "lake.ducklake"
      assert attach.alias == :lake
      assert attach.type == :ducklake
      assert attach.data_path == "s3://bucket/data"
    end

    test "preserves all options" do
      opts = [as: :analytics, type: :postgres, read_only: true]
      attach = Attach.parse({"postgres://host/db", opts})

      assert attach.options == opts
    end

    test "raises on missing :as option" do
      assert_raise KeyError, fn ->
        Attach.parse({"data.duckdb", []})
      end
    end
  end

  describe "attach_sql/1" do
    test "generates basic attach SQL" do
      attach = Attach.parse({"data.duckdb", as: :analytics})
      sql = Attach.attach_sql(attach)

      assert sql == "ATTACH 'data.duckdb' AS analytics"
    end

    test "generates attach SQL with read_only" do
      attach = Attach.parse({"data.duckdb", as: :analytics, read_only: true})
      sql = Attach.attach_sql(attach)

      assert sql == "ATTACH 'data.duckdb' AS analytics (READ_ONLY)"
    end

    test "generates attach SQL with postgres type" do
      attach = Attach.parse({"postgres://host/db", as: :pg, type: :postgres})
      sql = Attach.attach_sql(attach)

      assert sql == "ATTACH 'postgres://host/db' AS pg (TYPE POSTGRES)"
    end

    test "generates attach SQL with sqlite type" do
      attach = Attach.parse({"data.sqlite", as: :sqlite_db, type: :sqlite})
      sql = Attach.attach_sql(attach)

      assert sql == "ATTACH 'data.sqlite' AS sqlite_db (TYPE SQLITE)"
    end

    test "generates attach SQL with ducklake type" do
      attach = Attach.parse({"catalog.ducklake", as: :lake, type: :ducklake})
      sql = Attach.attach_sql(attach)

      assert sql == "ATTACH 'catalog.ducklake' AS lake (TYPE DUCKLAKE)"
    end

    test "generates attach SQL with ducklake type and data_path" do
      attach =
        Attach.parse(
          {"catalog.ducklake", as: :lake, type: :ducklake, data_path: "s3://bucket/data"}
        )

      sql = Attach.attach_sql(attach)

      assert sql ==
               "ATTACH 'catalog.ducklake' AS lake (TYPE DUCKLAKE, DATA_PATH 's3://bucket/data')"
    end

    test "generates attach SQL with multiple options" do
      attach =
        Attach.parse(
          {"catalog.ducklake",
           as: :lake, type: :ducklake, read_only: true, data_path: "s3://bucket"}
        )

      sql = Attach.attach_sql(attach)

      assert sql =~ "TYPE DUCKLAKE"
      assert sql =~ "READ_ONLY"
      assert sql =~ "DATA_PATH 's3://bucket'"
    end

    test "does not add type option for duckdb type" do
      attach = Attach.parse({"data.duckdb", as: :analytics, type: :duckdb})
      sql = Attach.attach_sql(attach)

      assert sql == "ATTACH 'data.duckdb' AS analytics"
    end

    test "escapes single quotes in path" do
      attach = Attach.parse({"path's/data.duckdb", as: :analytics})
      sql = Attach.attach_sql(attach)

      assert sql == "ATTACH 'path''s/data.duckdb' AS analytics"
    end

    test "escapes single quotes in data_path" do
      attach =
        Attach.parse({"lake.ducklake", as: :lake, type: :ducklake, data_path: "s3://bucket's"})

      sql = Attach.attach_sql(attach)

      assert sql =~ "DATA_PATH 's3://bucket''s'"
    end
  end

  describe "parse_all/1" do
    test "parses empty list" do
      assert Attach.parse_all([]) == []
    end

    test "parses list of attachments" do
      configs = [
        {"data.duckdb", as: :analytics},
        {"other.duckdb", as: :other}
      ]

      parsed = Attach.parse_all(configs)

      assert length(parsed) == 2
      assert Enum.at(parsed, 0).alias == :analytics
      assert Enum.at(parsed, 1).alias == :other
    end
  end
end
