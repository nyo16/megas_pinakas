import Config

# Production configuration
# Uses real Google Cloud BigTable - no emulator config

# Connection pool settings for production
config :megas_pinakas, :default_pool_size, 10

# For production, you can also use the modern GrpcConnectionPool.Config format:
# config :megas_pinakas, GrpcConnectionPool,
#   endpoint: [
#     type: :production,
#     host: "bigtable.googleapis.com",
#     port: 443,
#     ssl: []
#   ],
#   pool: [
#     size: 10,
#     name: MegasPinakas.ConnectionPool
#   ],
#   connection: [
#     keepalive: 30_000,
#     ping_interval: 25_000
#   ]

# Logger configuration
config :logger, level: :info
