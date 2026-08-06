# Benchmarks `MegasPinakas.Auth.request_opts/0`, the function every RPC calls.
#
# Run with:
#
#     MIX_ENV=test mix run bench/auth_bench.exs
#
# ## Baseline (before MegasPinakas.Auth.Cache existed)
#
#     Auth.request_opts/0, production path   911.9 ms/call  (6 sequential calls)
#     gcloud print-access-token              0.85 - 1.11 s
#
# There was no caching, so *every* RPC forked a gcloud subprocess. The
# "uncached" job below still pays that cost and stands in for the old behaviour;
# the "cached" job is what a deployed caller pays now in steady state.

alias MegasPinakas.Auth
alias MegasPinakas.Auth.Cache

emulator_config = Application.get_env(:megas_pinakas, :emulator)
env_emulator_host = System.get_env("BIGTABLE_EMULATOR_HOST")

restore = fn ->
  if emulator_config do
    Application.put_env(:megas_pinakas, :emulator, emulator_config)
  else
    Application.delete_env(:megas_pinakas, :emulator)
  end

  if env_emulator_host, do: System.put_env("BIGTABLE_EMULATOR_HOST", env_emulator_host)
  Cache.invalidate()
end

# `Config.emulator?/0` consults both the app env and BIGTABLE_EMULATOR_HOST, so a
# production-path measurement requires clearing both.
enter_production_mode = fn ->
  Application.delete_env(:megas_pinakas, :emulator)
  System.delete_env("BIGTABLE_EMULATOR_HOST")
end

real_source_available? =
  Application.get_env(:megas_pinakas, :goth) != nil or
    (Auth.gcloud_fallback_allowed?() and System.find_executable("gcloud") != nil)

unless real_source_available? do
  IO.puts("""
  No real token source available (no :goth configured, and no usable gcloud).
  Only the emulator path and a synthetic cached path will be measured.
  """)
end

# A synthetic source isolates cache overhead from token-source latency, so the
# "cached" number is not flattered by a fast source or penalised by a slow one.
synthetic_source = fn ->
  {:ok, %{token: "Bearer synthetic", expires_at: System.os_time(:second) + 3_600}}
end

emulator_job = %{
  "request_opts/0 — emulator path (no auth)" => {
    fn _ -> Auth.request_opts() end,
    before_scenario: fn _ ->
      restore.()
      Application.put_env(:megas_pinakas, :emulator, host: "localhost", port: 8086)
    end
  }
}

synthetic_cached_job = %{
  "request_opts/0 — production path, cached (synthetic source)" => {
    fn _ -> Auth.request_opts() end,
    before_scenario: fn _ ->
      enter_production_mode.()
      Application.put_env(:megas_pinakas, :token_source, synthetic_source)
      Cache.invalidate()
      # Prime the cache so the measured calls are pure ETS reads.
      {:ok, _} = Cache.fetch_token()
    end,
    after_scenario: fn _ -> Application.delete_env(:megas_pinakas, :token_source) end
  }
}

real_jobs =
  if real_source_available? do
    %{
      "request_opts/0 — production path, cached (real source)" => {
        fn _ -> Auth.request_opts() end,
        before_scenario: fn _ ->
          enter_production_mode.()
          Cache.invalidate()
          {:ok, _} = Cache.fetch_token()
        end
      },
      # This is the pre-cache cost, measured on every RPC before the cache landed.
      "fetch_fresh_token/0 — UNCACHED (old per-RPC cost)" => {
        fn _ -> Auth.fetch_fresh_token() end,
        before_scenario: fn _ -> enter_production_mode.() end
      }
    }
  else
    %{}
  end

try do
  Benchee.run(
    emulator_job
    |> Map.merge(synthetic_cached_job)
    |> Map.merge(real_jobs),
    warmup: 1,
    time: 3,
    memory_time: 1,
    print: [fast_warning: false]
  )
after
  restore.()
end
