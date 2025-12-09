import Config

# Test configuration using BigTable emulator
config :megas_pinakas, :emulator,
  host: "localhost",
  port: 8086,
  project_id: "test-project"

# Smaller pool for tests
config :megas_pinakas, :default_pool_size, 2

# Logger configuration - reduce noise during tests
config :logger, level: :warning
