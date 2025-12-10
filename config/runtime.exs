import Config

# Runtime configuration
# These settings can be overridden by environment variables at runtime

if config_env() == :prod do
  # Check for BIGTABLE_EMULATOR_HOST environment variable
  # This allows overriding production to use an emulator if needed
  if emulator_host = System.get_env("BIGTABLE_EMULATOR_HOST") do
    [host, port] =
      case String.split(emulator_host, ":") do
        [h, p] -> [h, String.to_integer(p)]
        [h] -> [h, 8086]
      end

    config :megas_pinakas, :emulator,
      host: host,
      port: port
  end

  # Optional: Configure Goth for authentication
  # Requires adding {:goth, "~> 1.4"} to dependencies
  if goth_name = System.get_env("MEGAS_PINAKAS_GOTH_NAME") do
    config :megas_pinakas, :goth, String.to_atom(goth_name)
  end

  # Pool size from environment
  if pool_size = System.get_env("MEGAS_PINAKAS_POOL_SIZE") do
    config :megas_pinakas, :default_pool_size, String.to_integer(pool_size)
  end
end
