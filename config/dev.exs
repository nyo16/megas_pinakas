import Config

# Development configuration using BigTable emulator
# Start the emulator with: docker-compose up bigtable-emulator
config :megas_pinakas, :emulator,
  host: "localhost",
  port: 8086,
  project_id: "dev-project"

# Connection pool settings for development
config :megas_pinakas, :default_pool_size, 3

# Logger configuration
config :logger, level: :debug
