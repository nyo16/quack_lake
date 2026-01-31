import Config

# Base configuration for QuackLake
# Environment-specific configuration is loaded from {dev,test,prod}.exs

config :quack_lake,
  # Default extensions to load for all environments
  default_extensions: [:httpfs]

# Import environment specific config
import_config "#{config_env()}.exs"
