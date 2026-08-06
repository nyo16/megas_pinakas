import Config

# Production configuration
# Uses real Google Cloud BigTable - no emulator config

# Authentication: configure Goth. Set this to the name of the Goth process in
# your own supervision tree — see the "Production with Goth Authentication"
# section of the README.
#
# Without it, MegasPinakas has no token source in production. The gcloud CLI
# fallback is deliberately DISABLED in production builds: it spawns a
# subprocess per token refresh (measured at 0.85-1.11 s) and requires an
# interactive gcloud login, neither of which belongs in a deployed release.
#
#     config :megas_pinakas, :goth, MyApp.Goth
#
# Prefer setting this in config/runtime.exs so it is read at boot rather than
# baked in at build time.

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
