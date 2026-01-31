# Configure ExUnit
ExUnit.configure(
  exclude: [:integration],
  formatters: [ExUnit.CLIFormatter]
)

# Include integration tests when INTEGRATION=true
if System.get_env("INTEGRATION") == "true" do
  ExUnit.configure(exclude: [])
end

# Start httpc for health checks (needed for Docker health checks)
Application.ensure_all_started(:inets)
Application.ensure_all_started(:ssl)

ExUnit.start()
