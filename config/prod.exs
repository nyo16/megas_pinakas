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

# The Data API pool can also be configured in full GrpcConnectionPool.Config
# form. `pool.name` is always forced to MegasPinakas.ConnectionPool (that is the
# name MegasPinakas.Client looks up), so there is no point setting it. Note that
# compile-time config files cannot call dependency code, so spell the TLS
# options out (this is what GrpcConnectionPool.Config.default_production_ssl/0
# returns); `ssl: []` would disable peer verification.
#
# config :megas_pinakas, GrpcConnectionPool,
#   endpoint: [
#     type: :production,
#     host: "bigtable.googleapis.com",
#     port: 443,
#     ssl: [
#       verify: :verify_peer,
#       cacerts: :public_key.cacerts_get(),
#       depth: 3,
#       customize_hostname_check: [
#         match_fun: :public_key.pkix_verify_hostname_match_fun(:https)
#       ]
#     ]
#   ],
#   pool: [size: 10],
#   connection: [
#     keepalive: 30_000,
#     ping_interval: 25_000
#   ]
#
# This key configures only the Data API pool. The Admin API pool
# (MegasPinakas.AdminConnectionPool, bigtableadmin.googleapis.com) is always
# built from :default_pool_size and the emulator settings.

# Logger configuration
config :logger, level: :info
