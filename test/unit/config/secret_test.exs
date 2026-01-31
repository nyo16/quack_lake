defmodule QuackLake.Config.SecretTest do
  use ExUnit.Case, async: true

  alias QuackLake.Config.Secret

  describe "parse/1" do
    test "parses S3 secret" do
      secret =
        Secret.parse({:my_s3, [type: :s3, key_id: "AK", secret: "SK", region: "us-east-1"]})

      assert secret.name == :my_s3
      assert secret.type == :s3
      assert secret.options[:key_id] == "AK"
      assert secret.options[:secret] == "SK"
      assert secret.options[:region] == "us-east-1"
    end

    test "parses Azure secret with connection string" do
      secret =
        Secret.parse(
          {:my_azure, [type: :azure, connection_string: "DefaultEndpointsProtocol=..."]}
        )

      assert secret.name == :my_azure
      assert secret.type == :azure
      assert secret.options[:connection_string] == "DefaultEndpointsProtocol=..."
    end

    test "parses Azure secret with account credentials" do
      secret =
        Secret.parse({:my_azure, [type: :azure, account_name: "myaccount", account_key: "mykey"]})

      assert secret.name == :my_azure
      assert secret.type == :azure
      assert secret.options[:account_name] == "myaccount"
      assert secret.options[:account_key] == "mykey"
    end

    test "parses GCS secret" do
      secret = Secret.parse({:my_gcs, [type: :gcs, key_id: "GOOG1E", secret: "mysecret"]})

      assert secret.name == :my_gcs
      assert secret.type == :gcs
      assert secret.options[:key_id] == "GOOG1E"
      assert secret.options[:secret] == "mysecret"
    end

    test "parses R2 secret" do
      secret =
        Secret.parse(
          {:my_r2, [type: :r2, account_id: "acc123", key_id: "key123", secret: "secret123"]}
        )

      assert secret.name == :my_r2
      assert secret.type == :r2
      assert secret.options[:account_id] == "acc123"
      assert secret.options[:key_id] == "key123"
      assert secret.options[:secret] == "secret123"
    end

    test "parses HuggingFace secret" do
      secret = Secret.parse({:my_hf, [type: :huggingface, token: "hf_token123"]})

      assert secret.name == :my_hf
      assert secret.type == :huggingface
      assert secret.options[:token] == "hf_token123"
    end

    test "raises on missing type" do
      assert_raise KeyError, fn ->
        Secret.parse({:invalid, [key_id: "AK"]})
      end
    end
  end

  describe "create_sql/1 for S3" do
    test "generates basic S3 secret SQL" do
      secret =
        Secret.parse({:my_s3, [type: :s3, key_id: "AK", secret: "SK", region: "us-east-1"]})

      sql = Secret.create_sql(secret)

      assert sql =~ "CREATE SECRET my_s3"
      assert sql =~ "TYPE S3"
      assert sql =~ "KEY_ID 'AK'"
      assert sql =~ "SECRET 'SK'"
      assert sql =~ "REGION 'us-east-1'"
    end

    test "generates S3 secret SQL with endpoint" do
      secret =
        Secret.parse(
          {:my_s3,
           [
             type: :s3,
             key_id: "AK",
             secret: "SK",
             region: "us-east-1",
             endpoint: "localhost:9000"
           ]}
        )

      sql = Secret.create_sql(secret)

      assert sql =~ "ENDPOINT 'localhost:9000'"
    end

    test "generates S3 secret SQL with use_ssl" do
      secret =
        Secret.parse(
          {:my_s3,
           [
             type: :s3,
             key_id: "AK",
             secret: "SK",
             region: "us-east-1",
             use_ssl: false
           ]}
        )

      sql = Secret.create_sql(secret)

      assert sql =~ "USE_SSL false"
    end

    test "generates S3 secret SQL with url_style" do
      secret =
        Secret.parse(
          {:my_s3,
           [
             type: :s3,
             key_id: "AK",
             secret: "SK",
             region: "us-east-1",
             url_style: :path
           ]}
        )

      sql = Secret.create_sql(secret)

      assert sql =~ "URL_STYLE 'path'"
    end

    test "generates S3 secret SQL with scope" do
      secret =
        Secret.parse(
          {:my_s3,
           [
             type: :s3,
             key_id: "AK",
             secret: "SK",
             region: "us-east-1",
             scope: "s3://my-bucket"
           ]}
        )

      sql = Secret.create_sql(secret)

      assert sql =~ "SCOPE 's3://my-bucket'"
    end

    test "escapes single quotes in values" do
      secret =
        Secret.parse(
          {:my_s3,
           [
             type: :s3,
             key_id: "AK'123",
             secret: "SK'456",
             region: "us-east-1"
           ]}
        )

      sql = Secret.create_sql(secret)

      assert sql =~ "KEY_ID 'AK''123'"
      assert sql =~ "SECRET 'SK''456'"
    end
  end

  describe "create_sql/1 for Azure" do
    test "generates Azure secret SQL with connection string" do
      secret =
        Secret.parse(
          {:my_azure,
           [
             type: :azure,
             connection_string: "DefaultEndpointsProtocol=https;AccountName=myaccount"
           ]}
        )

      sql = Secret.create_sql(secret)

      assert sql =~ "CREATE SECRET my_azure"
      assert sql =~ "TYPE AZURE"
      assert sql =~ "CONNECTION_STRING 'DefaultEndpointsProtocol=https;AccountName=myaccount'"
    end

    test "generates Azure secret SQL with account credentials" do
      secret =
        Secret.parse({:my_azure, [type: :azure, account_name: "myaccount", account_key: "mykey"]})

      sql = Secret.create_sql(secret)

      assert sql =~ "CREATE SECRET my_azure"
      assert sql =~ "TYPE AZURE"
      assert sql =~ "ACCOUNT_NAME 'myaccount'"
      assert sql =~ "ACCOUNT_KEY 'mykey'"
    end
  end

  describe "create_sql/1 for GCS" do
    test "generates GCS secret SQL" do
      secret = Secret.parse({:my_gcs, [type: :gcs, key_id: "GOOG1E", secret: "mysecret"]})
      sql = Secret.create_sql(secret)

      assert sql =~ "CREATE SECRET my_gcs"
      assert sql =~ "TYPE GCS"
      assert sql =~ "KEY_ID 'GOOG1E'"
      assert sql =~ "SECRET 'mysecret'"
    end
  end

  describe "create_sql/1 for R2" do
    test "generates R2 secret SQL" do
      secret =
        Secret.parse(
          {:my_r2, [type: :r2, account_id: "acc123", key_id: "key123", secret: "secret123"]}
        )

      sql = Secret.create_sql(secret)

      assert sql =~ "CREATE SECRET my_r2"
      assert sql =~ "TYPE R2"
      assert sql =~ "ACCOUNT_ID 'acc123'"
      assert sql =~ "KEY_ID 'key123'"
      assert sql =~ "SECRET 'secret123'"
    end
  end

  describe "create_sql/1 for HuggingFace" do
    test "generates HuggingFace secret SQL" do
      secret = Secret.parse({:my_hf, [type: :huggingface, token: "hf_token123"]})
      sql = Secret.create_sql(secret)

      assert sql =~ "CREATE SECRET my_hf"
      assert sql =~ "TYPE HUGGINGFACE"
      assert sql =~ "TOKEN 'hf_token123'"
    end
  end

  describe "parse_all/1" do
    test "parses empty list" do
      assert Secret.parse_all([]) == []
    end

    test "parses list of secrets" do
      secrets = [
        {:s3, [type: :s3, key_id: "AK", secret: "SK", region: "us-east-1"]},
        {:azure, [type: :azure, connection_string: "..."]}
      ]

      parsed = Secret.parse_all(secrets)

      assert length(parsed) == 2
      assert Enum.at(parsed, 0).type == :s3
      assert Enum.at(parsed, 1).type == :azure
    end
  end
end
