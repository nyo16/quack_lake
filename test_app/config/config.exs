import Config

# Base configuration for TestApp

config :test_app,
  ecto_repos: [TestApp.Repo, TestApp.LakeRepo]

# Import environment specific config
import_config "#{config_env()}.exs"
