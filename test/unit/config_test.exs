defmodule QuackLake.ConfigTest do
  use ExUnit.Case, async: true

  alias QuackLake.Config

  describe "new/1" do
    test "creates config with defaults" do
      config = Config.new()

      assert config.path == nil
      assert config.database == nil
      assert config.data_path == nil
      assert config.pool_size == 1
      assert config.extensions == []
      assert config.secrets == []
      assert config.attach == []
      assert config.auto_install_extensions == true
      assert config.auto_load_extensions == true
    end

    test "creates config with path" do
      config = Config.new(path: "data.duckdb")

      assert config.path == "data.duckdb"
    end

    test "creates config with database (ecto compatibility)" do
      config = Config.new(database: "data.duckdb")

      assert config.path == "data.duckdb"
      assert config.database == "data.duckdb"
    end

    test "creates config with data_path for DuckLake" do
      config = Config.new(database: "ducklake:...", data_path: "s3://my-bucket/lake-data")

      assert config.data_path == "s3://my-bucket/lake-data"
    end

    test "creates config with custom lake_name" do
      config = Config.new(database: "ducklake:...", lake_name: "my_lake")

      assert config.lake_name == "my_lake"
    end

    test "creates config with extensions" do
      config = Config.new(extensions: [:httpfs, {:spatial, source: :core}])

      assert config.extensions == [:httpfs, {:spatial, source: :core}]
    end

    test "creates config with secrets" do
      secrets = [{:my_s3, [type: :s3, key_id: "AK", secret: "SK", region: "us-east-1"]}]
      config = Config.new(secrets: secrets)

      assert config.secrets == secrets
    end

    test "creates config with attachments" do
      attach = [{"data.duckdb", as: :analytics}]
      config = Config.new(attach: attach)

      assert config.attach == attach
    end

    test "creates config with pool_size" do
      config = Config.new(pool_size: 5)

      assert config.pool_size == 5
    end

    test "creates config with auto_install_extensions disabled" do
      config = Config.new(auto_install_extensions: false)

      assert config.auto_install_extensions == false
    end

    test "creates config with auto_load_extensions disabled" do
      config = Config.new(auto_load_extensions: false)

      assert config.auto_load_extensions == false
    end
  end

  describe "from_ecto_opts/1" do
    test "parses extensions into structs" do
      config = Config.from_ecto_opts(extensions: [:httpfs, {:spatial, source: :core}])

      assert length(config.parsed_extensions) == 2
      assert Enum.at(config.parsed_extensions, 0).name == :httpfs
      assert Enum.at(config.parsed_extensions, 1).name == :spatial
      assert Enum.at(config.parsed_extensions, 1).source == :core
    end

    test "parses secrets into structs" do
      secrets = [{:my_s3, [type: :s3, key_id: "AK", secret: "SK", region: "us-east-1"]}]
      config = Config.from_ecto_opts(secrets: secrets)

      assert length(config.parsed_secrets) == 1
      assert Enum.at(config.parsed_secrets, 0).name == :my_s3
      assert Enum.at(config.parsed_secrets, 0).type == :s3
    end

    test "parses attachments into structs" do
      attach = [{"data.duckdb", as: :analytics}]
      config = Config.from_ecto_opts(attach: attach)

      assert length(config.parsed_attach) == 1
      assert Enum.at(config.parsed_attach, 0).path == "data.duckdb"
      assert Enum.at(config.parsed_attach, 0).alias == :analytics
    end

    test "preserves data_path" do
      config =
        Config.from_ecto_opts(
          database: "ducklake:postgres:...",
          data_path: "s3://bucket/data"
        )

      assert config.data_path == "s3://bucket/data"
    end
  end

  describe "database_path/1" do
    test "returns database when set" do
      config = Config.new(database: "data.duckdb")

      assert Config.database_path(config) == "data.duckdb"
    end

    test "returns path when database not set" do
      config = Config.new(path: "data.duckdb")

      assert Config.database_path(config) == "data.duckdb"
    end

    test "prefers database over path" do
      config = Config.new(database: "ecto.duckdb", path: "raw.duckdb")

      assert Config.database_path(config) == "ecto.duckdb"
    end

    test "returns nil for in-memory database" do
      config = Config.new()

      assert Config.database_path(config) == nil
    end
  end

  describe "in_memory?/1" do
    test "returns true when no path set" do
      config = Config.new()

      assert Config.in_memory?(config) == true
    end

    test "returns false when path set" do
      config = Config.new(path: "data.duckdb")

      assert Config.in_memory?(config) == false
    end

    test "returns false when database set" do
      config = Config.new(database: "data.duckdb")

      assert Config.in_memory?(config) == false
    end
  end

  describe "has_extensions?/1" do
    test "returns true when extensions present" do
      config = Config.new(extensions: [:httpfs])

      assert Config.has_extensions?(config) == true
    end

    test "returns false when no extensions" do
      config = Config.new()

      assert Config.has_extensions?(config) == false
    end
  end

  describe "has_secrets?/1" do
    test "returns true when secrets present" do
      secrets = [{:my_s3, [type: :s3, key_id: "AK", secret: "SK", region: "us-east-1"]}]
      config = Config.new(secrets: secrets)

      assert Config.has_secrets?(config) == true
    end

    test "returns false when no secrets" do
      config = Config.new()

      assert Config.has_secrets?(config) == false
    end
  end

  describe "has_attachments?/1" do
    test "returns true when attachments present" do
      attach = [{"data.duckdb", as: :analytics}]
      config = Config.new(attach: attach)

      assert Config.has_attachments?(config) == true
    end

    test "returns false when no attachments" do
      config = Config.new()

      assert Config.has_attachments?(config) == false
    end
  end
end
