import Config

# Runtime configuration
# These settings can be overridden by environment variables at runtime.
#
# BIGTABLE_EMULATOR_HOST needs no entry here: MegasPinakas.Config reads it at
# runtime (see `MegasPinakas.Config.emulator_endpoint/0`) and it takes
# precedence over any `:emulator` app config.

if config_env() == :prod do
  # Optional: Configure Goth for authentication. Goth is an optional dependency
  # of MegasPinakas — add {:goth, "~> 1.4"} to your own deps and start a Goth
  # process under this name in your supervision tree.
  if goth_name = System.get_env("MEGAS_PINAKAS_GOTH_NAME") do
    config :megas_pinakas, :goth, String.to_atom(goth_name)
  end

  # Pool size from environment
  if pool_size = System.get_env("MEGAS_PINAKAS_POOL_SIZE") do
    config :megas_pinakas, :default_pool_size, String.to_integer(pool_size)
  end
end
