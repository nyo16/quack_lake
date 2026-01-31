defmodule TestApp.Demos.SecretsDemo do
  @moduledoc """
  Demonstrates cloud storage secret management.
  """

  def run do
    IO.puts("\n" <> String.duplicate("=", 60))
    IO.puts("CLOUD STORAGE SECRETS DEMO")
    IO.puts(String.duplicate("=", 60))

    {:ok, conn} = QuackLake.open()

    demo_s3_secret(conn)
    demo_secret_config_parsing()
    demo_list_and_drop(conn)

    IO.puts("\n✓ Secrets demo complete!\n")
  end

  defp demo_s3_secret(conn) do
    IO.puts("\n--- S3 Secret (MinIO) ---")

    s3_config = Application.get_env(:test_app, :s3, [])

    if s3_config[:access_key_id] do
      endpoint = s3_config[:endpoint] |> String.replace(~r{^https?://}, "")

      :ok =
        QuackLake.Secret.create_s3(conn, "demo_s3",
          key_id: s3_config[:access_key_id],
          secret: s3_config[:secret_access_key],
          region: s3_config[:region] || "us-east-1",
          endpoint: endpoint,
          use_ssl: false,
          url_style: "path"
        )

      IO.puts("  Created S3 secret 'demo_s3' for MinIO")
      IO.puts("    endpoint: #{endpoint}")
      IO.puts("    bucket: #{s3_config[:bucket]}")
    else
      IO.puts("  Skipped - S3 config not available")
    end
  end

  defp demo_secret_config_parsing do
    IO.puts("\n--- Secret Configuration Parsing ---")

    # S3 Secret
    s3 =
      QuackLake.Config.Secret.parse(
        {:my_s3, [type: :s3, key_id: "AKIAEXAMPLE", secret: "secretkey", region: "us-east-1"]}
      )

    IO.puts("  S3 secret: name=#{s3.name}, type=#{s3.type}")
    IO.puts("    SQL: #{String.slice(QuackLake.Config.Secret.create_sql(s3), 0, 70)}...")

    # Azure Secret
    azure =
      QuackLake.Config.Secret.parse(
        {:my_azure,
         [type: :azure, connection_string: "DefaultEndpointsProtocol=https;AccountName=myaccount"]}
      )

    IO.puts("\n  Azure secret: name=#{azure.name}, type=#{azure.type}")
    IO.puts("    SQL: #{String.slice(QuackLake.Config.Secret.create_sql(azure), 0, 70)}...")

    # GCS Secret
    gcs =
      QuackLake.Config.Secret.parse(
        {:my_gcs, [type: :gcs, key_id: "GOOG1EXAMPLE", secret: "gcssecret"]}
      )

    IO.puts("\n  GCS secret: name=#{gcs.name}, type=#{gcs.type}")
    IO.puts("    SQL: #{String.slice(QuackLake.Config.Secret.create_sql(gcs), 0, 70)}...")

    # R2 Secret
    r2 =
      QuackLake.Config.Secret.parse(
        {:my_r2, [type: :r2, account_id: "acc123", key_id: "key123", secret: "secret123"]}
      )

    IO.puts("\n  R2 secret: name=#{r2.name}, type=#{r2.type}")
    IO.puts("    SQL: #{String.slice(QuackLake.Config.Secret.create_sql(r2), 0, 70)}...")

    # HuggingFace Secret
    hf =
      QuackLake.Config.Secret.parse({:my_hf, [type: :huggingface, token: "hf_token123"]})

    IO.puts("\n  HuggingFace secret: name=#{hf.name}, type=#{hf.type}")
    IO.puts("    SQL: #{QuackLake.Config.Secret.create_sql(hf)}")
  end

  defp demo_list_and_drop(conn) do
    IO.puts("\n--- List and Drop Secrets ---")

    {:ok, rows} = QuackLake.query(conn, "SELECT name, type FROM duckdb_secrets()")
    IO.puts("  Current secrets: #{length(rows)}")

    for row <- rows do
      IO.puts("    - #{row["name"]} (#{row["type"]})")
    end

    if Enum.any?(rows, &(&1["name"] == "demo_s3")) do
      :ok = QuackLake.Secret.drop(conn, "demo_s3")
      IO.puts("  Dropped 'demo_s3' secret")
    end
  end
end
