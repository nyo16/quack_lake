defmodule TestApp.Repo do
  @moduledoc """
  Ecto repository using DuckDB adapter (single writer).

  This repo uses a local DuckDB file for storage.
  """

  use Ecto.Repo,
    otp_app: :test_app,
    adapter: Ecto.Adapters.DuckDB

  use Ecto.Adapters.DuckDB.RawQuery
end
