defmodule TestApp.LakeRepo do
  @moduledoc """
  Ecto repository using DuckLake adapter (concurrent writers).

  This repo uses PostgreSQL as the metadata catalog and S3 for data storage.
  Requires Docker services to be running.
  """

  use Ecto.Repo,
    otp_app: :test_app,
    adapter: Ecto.Adapters.DuckLake

  use Ecto.Adapters.DuckDB.RawQuery
end
