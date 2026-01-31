defmodule TestApp.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      # Start the DuckDB Repo (single writer)
      TestApp.Repo
      # Note: LakeRepo requires Docker services - start manually in demos
      # TestApp.LakeRepo,
      # Supervised lake connection
      # {TestApp.LakeServer, name: TestApp.LakeServer}
    ]

    opts = [strategy: :one_for_one, name: TestApp.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
