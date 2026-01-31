defmodule QuackLake.Config.ExtensionTest do
  use ExUnit.Case, async: true

  alias QuackLake.Config.Extension

  describe "parse/1" do
    test "parses simple atom extension" do
      ext = Extension.parse(:httpfs)

      assert ext.name == :httpfs
      assert ext.source == nil
      assert ext.install == true
      assert ext.load == true
    end

    test "parses extension with core source" do
      ext = Extension.parse({:spatial, source: :core})

      assert ext.name == :spatial
      assert ext.source == :core
      assert ext.install == true
      assert ext.load == true
    end

    test "parses extension with community source" do
      ext = Extension.parse({:my_ext, source: :community})

      assert ext.name == :my_ext
      assert ext.source == :community
    end

    test "parses extension with custom URL source" do
      ext = Extension.parse({:my_ext, source: "https://example.com/extensions"})

      assert ext.name == :my_ext
      assert ext.source == "https://example.com/extensions"
    end

    test "parses extension with install: false" do
      ext = Extension.parse({:httpfs, install: false})

      assert ext.name == :httpfs
      assert ext.install == false
      assert ext.load == true
    end

    test "parses extension with load: false" do
      ext = Extension.parse({:httpfs, load: false})

      assert ext.name == :httpfs
      assert ext.install == true
      assert ext.load == false
    end

    test "parses extension with all options" do
      ext = Extension.parse({:ducklake, source: :core, install: true, load: true})

      assert ext.name == :ducklake
      assert ext.source == :core
      assert ext.install == true
      assert ext.load == true
    end
  end

  describe "install_sql/1" do
    test "generates basic install SQL" do
      ext = Extension.parse(:httpfs)

      assert Extension.install_sql(ext) == "INSTALL httpfs"
    end

    test "generates install SQL with core source" do
      ext = Extension.parse({:spatial, source: :core})

      assert Extension.install_sql(ext) == "INSTALL spatial FROM core"
    end

    test "generates install SQL with community source" do
      ext = Extension.parse({:my_ext, source: :community})

      assert Extension.install_sql(ext) == "INSTALL my_ext FROM community"
    end

    test "generates install SQL with custom URL" do
      ext = Extension.parse({:my_ext, source: "https://example.com"})

      assert Extension.install_sql(ext) == "INSTALL my_ext FROM 'https://example.com'"
    end

    test "escapes single quotes in URL" do
      ext = Extension.parse({:my_ext, source: "https://example.com/path's"})

      assert Extension.install_sql(ext) == "INSTALL my_ext FROM 'https://example.com/path''s'"
    end
  end

  describe "load_sql/1" do
    test "generates load SQL" do
      ext = Extension.parse(:httpfs)

      assert Extension.load_sql(ext) == "LOAD httpfs"
    end

    test "generates load SQL regardless of source" do
      ext = Extension.parse({:spatial, source: :core})

      assert Extension.load_sql(ext) == "LOAD spatial"
    end
  end

  describe "parse_all/1" do
    test "parses empty list" do
      assert Extension.parse_all([]) == []
    end

    test "parses list of extensions" do
      extensions = [:httpfs, {:spatial, source: :core}]
      parsed = Extension.parse_all(extensions)

      assert length(parsed) == 2
      assert Enum.at(parsed, 0).name == :httpfs
      assert Enum.at(parsed, 1).name == :spatial
      assert Enum.at(parsed, 1).source == :core
    end
  end
end
