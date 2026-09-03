# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is an Elixir BigTable client library named `MegasPinakas` that provides a high-level interface for interacting with Google Cloud BigTable. The project is built as an OTP application with GRPC-based communication to BigTable services, utilizing connection pooling for optimal performance. Requires Elixir `~> 1.18`.

## Common Development Commands

### Build and Dependencies
```bash
mix deps.get          # Install dependencies
mix compile           # Compile the project
mix deps.update --all # Update all dependencies
```

### Testing
```bash
mix test                                 # Pure builders only; :emulator tests are excluded by default
docker compose up -d bigtable-emulator   # Start the emulator (port 8086)
mix test --include emulator              # Full suite, including everything that talks to BigTable
mix test test/path/to/specific_test.exs  # Run a specific test file
mix test --failed                        # Re-run only failed tests
```

Everything that issues an RPC is tagged `@moduletag :emulator` and uses
`MegasPinakas.Test.Emulator` (`test/support/emulator.ex`); `await_pool!/1` waits
for both pools. CI runs `--include emulator` on Elixir 1.18.3/OTP 26.2 and
1.18.3/OTP 27.2.

The emulator does not implement `PartialUpdateCluster` and **crashes** on it:
never call `MegasPinakas.InstanceAdmin.partial_update_cluster/4` against the
emulator; test only `partial_update_cluster_request/4` (pure request builder).

### Code Quality
```bash
mix format            # Format code according to .formatter.exs
mix format --check-formatted  # Check if code is properly formatted
mix credo             # Lint
mix dialyzer          # Typecheck (.dialyzer_ignore.exs is empty; keep it that way)
```

### Interactive Development
```bash
iex -S mix            # Start IEx with project loaded
```

### BigTable Development
```bash
# For local development with BigTable emulator
docker compose up -d bigtable-emulator        # Start BigTable emulator
export BIGTABLE_EMULATOR_HOST=localhost:8086  # Connect to emulator

# For production BigTable access, add {:goth, "~> 1.4"} to your deps and
# config :megas_pinakas, :goth, MyApp.Goth
# Dev-only fallback: gcloud auth application-default login
```

## Architecture

### Module Structure
```
lib/
├── megas_pinakas.ex                    # Data API (read/write rows, mutations, filters, row sets)
└── megas_pinakas/
    ├── application.ex                  # OTP Application: Auth.Cache + two GrpcConnectionPool children
    ├── client.ex                       # Pool wrapper: execute/2, execute!/2, admin_pool/0; MegasPinakas.Error
    ├── auth.ex                         # Token sources (emulator, Goth, :token_source, gcloud); MegasPinakas.AuthError
    ├── auth/
    │   └── cache.ex                    # ETS token cache GenServer with negative caching
    ├── config.ex                       # Resource paths, emulator detection, pool configs
    ├── response.ex                     # gRPC response/error normalization
    ├── longrunning.ex                  # google.longrunning.Operations gRPC service + stub
    ├── admin.ex                        # Table Admin API (tables, column families, backups, operations, GC rules)
    ├── instance_admin.ex               # Instance Admin API (instances, clusters, app profiles)
    ├── filter.ex                       # RowFilter builders
    ├── row.ex                          # Fluent single-row mutation builder
    ├── batch.ex                        # Multi-row mutation builder (chunks at 100k mutations)
    ├── types.ex                        # Typed encode/decode + typed read/write helpers
    ├── row_assembler.ex                # ReadRows chunk -> Row assembly
    ├── row_key.ex                      # Row-key suffix splitting shared by CounterTTL/TimeSeries
    ├── streaming.ex                    # Stream.resource-based batched reads with retry + telemetry
    ├── stream_error.ex                 # MegasPinakas.StreamError
    ├── cache.ex                        # Key-value cache (term envelope, client-side TTL)
    ├── counter.ex                      # Atomic counters (RMW increment, CAS increment_if_exists)
    ├── counter_ttl.ex                  # Time-bucketed counters / rate limiting
    └── time_series.ex                  # Reverse-timestamp time series with value type tags
```

### Connection pools

`MegasPinakas.Application` starts two `GrpcConnectionPool` children:

- `MegasPinakas.ConnectionPool` - Data API, `bigtable.googleapis.com`. Built by
  `Config.build_pool_config/0`; the only pool the `config :megas_pinakas, GrpcConnectionPool`
  key affects (`pool.name` defaults to `MegasPinakas.ConnectionPool`; invalid config raises at boot).
- `MegasPinakas.AdminConnectionPool` - Admin API, `bigtableadmin.googleapis.com`. Built by
  `Config.build_admin_pool_config/0` from `:default_pool_size` + emulator settings only.
  The Data host answers admin RPCs with `UNIMPLEMENTED`, so `Admin` and `InstanceAdmin`
  MUST use this pool.

In emulator mode (`Config.emulator?/0`: `:emulator` config, `BIGTABLE_EMULATOR_HOST`, or a
`type: :local` `GrpcConnectionPool` endpoint) both pools point at the emulator and no auth
metadata is sent.

### Key Modules

- **MegasPinakas** - Data operations: `read_rows/4` (`:rows`, `:filter`, `:rows_limit`, `:max_rows` -> `{:error, :result_too_large}`), `read_row/5`, `sample_row_keys/4`, `mutate_row/6`, `mutate_rows/5` (accepts `%Row{}` or `%{row_key, mutations}` entries), `check_and_mutate_row/8`, `read_modify_write_row/6`. Mutation builders: `set_cell/4` (`:timestamp_micros` must be `-1` or a multiple of 1000), `delete_from_column/3`, `delete_from_family/1`, `delete_from_row/0`; RMW rules `increment_rule/3`, `append_rule/3`. Row sets: `row_set/1`, `row_set_from_ranges/1`, `row_range/2`, `row_range_prefix/1`, `row_range_open/2`, `row_range_closed/2`, `row_range_open_closed/2`, `row_range_from/1`, `row_range_until/1`, `row_range_unbounded/0`. Row accessors: `row_key/1`, `row_to_map/1`, `get_cell/3`, `get_cells/3`. Filter re-exports delegate to `MegasPinakas.Filter`.

- **MegasPinakas.Filter** - `row_key_regex_filter/1`, `row_key_prefix_filter/1` (emits `Regex.escape(prefix) <> "\\C*"`), `row_sample_filter/1`, `family_filter/1`, `column_filter/2`, `column_qualifier_regex_filter/1`, `column_range_filter/2`, `value_regex_filter/1`, `value_range_filter/1`, `timestamp_range_filter/2`, `cells_per_row_limit_filter/1`, `cells_per_row_offset_filter/1`, `cells_per_column_limit_filter/1`, `strip_value_filter/0`, `pass_all_filter/0`, `block_all_filter/0`, `chain_filters/1`, `interleave_filters/1`, `condition_filter/3`, `latest_only_filter/0`, `column_latest_filter/2`, `time_window_filter/2`. All `*_regex_filter`s are RE2 **full** matches: no implicit `.*`; pad with `\C*` for prefix/suffix/substring.

- **MegasPinakas.Admin** - Table admin: `create_table/4`, `list_tables/3`, `get_table/4`, `delete_table/3`, `modify_column_families/4`, `drop_row_range/4` (`{:error, :no_target}` when neither option given). Backups: `create_backup/6`, `get_backup/4`, `list_backups/4`, `delete_backup/4`, `restore_table/5`. Long-running operations: `get_operation/1`, `wait_operation/2` (`:poll_interval`, `:timeout`; decodes `Table`/`Backup`/`Instance`/`Cluster`/`AppProfile`/`Empty`). Modification builders: `create_column_family/2`, `update_column_family/2`, `drop_column_family/1`. GC rules: `max_versions_gc_rule/1`, `max_age_gc_rule/1`, `intersection_gc_rule/1`, `union_gc_rule/1`.

- **MegasPinakas.InstanceAdmin** - Instances: `create_instance/4`, `get_instance/2`, `list_instances/2`, `partial_update_instance/3`, `delete_instance/2`. Clusters: `create_cluster/5`, `get_cluster/3`, `list_clusters/3`, `update_cluster/4` (`:serve_nodes` required, full replace), `partial_update_cluster/4` (`:serve_nodes` or `:autoscaling`, FieldMask; emulator crashes), `partial_update_cluster_request/4`, `delete_cluster/3`. App profiles: `create_app_profile/4`, `create_app_profile_request/4`, `get_app_profile/3`, `list_app_profiles/3`, `update_app_profile/4`, `update_app_profile_request/4`, `delete_app_profile/4`.

- **MegasPinakas.Longrunning** - `Operations.Service` / `Operations.Stub` for `google.longrunning.Operations` (`GetOperation`, `WaitOperation`, `CancelOperation`, `DeleteOperation`); `googleapis_proto_ex` ships the messages but no service. Used by `Admin.get_operation/1` / `wait_operation/2` on the admin pool.

- **MegasPinakas.Client** - `execute/2` (opts: `:pool`), `execute!/2` (raises `MegasPinakas.Error`), `status/1`, `default_pool/0`, `admin_pool/0`. Never raises for a failure inside the operation; returns `{:error, {status_atom, msg}}`, `{:error, {:auth_error, reason}}`, `{:error, {:pool_error, reason}}`, `{:error, {:execution_error, msg | {kind, reason}}}`. Telemetry `[:megas_pinakas, :request, :start | :stop | :exception]`; `:stop` metadata `%{pool, result: :ok | {:error, tag}}`, `:exception` metadata `%{pool, kind, reason, stacktrace}`.

- **MegasPinakas.Response** - `format/1` normalizes a stub result; `normalize_reason/1` turns a bare `%GRPC.RPCError{}` into `{status_atom, message}`; `status_to_atom/1`. Mid-stream failures surface as `{:error, {:incomplete_read, {status_atom, msg}}}`.

- **MegasPinakas.Config** - Resource paths: `project_path/1`, `location_path/2`, `instance_path/2`, `table_path/3`, `cluster_path/3`, `backup_path/4`, `app_profile_path/3`. Environment: `emulator?/0`, `emulator_config/0`, `emulator_endpoint/0` (parses `BIGTABLE_EMULATOR_HOST`), `production_endpoint/0`, `admin_endpoint/0`. Pools: `build_pool_config/0`, `build_admin_pool_config/0`, `default_pool_size/0`, `default_timeout/0`.

- **MegasPinakas.Auth** - `request_opts/0` returns gRPC opts with `authorization` metadata, or raises `MegasPinakas.AuthError` (converted by `Client.execute/2` to `{:error, {:auth_error, reason}}`; requests are never sent unauthenticated). `get_token/0`, `fetch_fresh_token/0`, `authenticated?/0`, `gcloud_fallback_allowed?/0`. Sources in order: `:token_source` (0-arity fun or MFA), Goth (`:goth` config; optional dep -> `{:error, :goth_not_available}` if absent), gcloud CLI (non-prod only, 10 s timeout). **MegasPinakas.Auth.Cache**: `fetch_token/1`, `invalidate/1`; negative-caches failures for 5 s; never crashes on a raising/exiting token source.

- **MegasPinakas.Streaming** - `stream_rows/4` (`:rows`, `:filter`, `:batch_size` default 10_000, `:rows_limit`, `:max_retries` default 3, `:app_profile_id`), `stream_rows_as_maps/4`, `stream_rows_with_keys/4`, `stream_range/6`, `stream_prefix/5`, `stream_in_chunks/5`, `count_rows/4`, `rows_exist?/4`, `first_row/4`. Each batch is one drained `ReadRows` RPC; retries `:unavailable | :deadline_exceeded | :aborted`; raises `MegasPinakas.StreamError` on permanent failure. Telemetry `[:megas_pinakas, :stream, :start | :stop | :cancelled | :retry | :exception]`, all with `:stream_ref`.

- **MegasPinakas.Row** / **MegasPinakas.Batch** - `Row.new/1`, `put/5` (type inference; raises for atoms/nil/tuples -> use `put_term/5`), `put_string/5`, `put_binary/5`, `put_integer/5`, `put_float/5`, `put_boolean/5`, `put_json/5`, `put_datetime/5`, `put_term/5`, `write/5`, `to_entry/1`, `mutation_count/1`. `Batch.new/0`, `add/2`, `add/3`, `add_all/2`, `write/5` (chunks at 100k mutations, re-bases `index`), `chunk_entries/2`, `to_entries/1`, `size/1`, `empty?/1`, `mutation_count/1`, `row_keys/1`, `clear/1`.

- **MegasPinakas.Types** - `encode/2`, `decode/2` (`:binary`, `:string`, `:json`, `:integer` (int64, raises on overflow), `:float`, `:boolean`, `:datetime`, `:term` (safe decode -> `{:error, :unsafe_or_invalid_term}`)); `set_*/4` mutation builders; `write_*/8` and `read_*/7` helpers; `write_cells/6`, `read_cells/6` (returns every requested key, `{:error, {:decode, "f:q", reason}}` on bad cell).

- **MegasPinakas.Cache** - `get/5`, `put/6`, `delete/5`, `get_or_put/6`, `get_many/5`, `put_many/5`, `delete_many/5`, `exists?/5` (`{:ok, boolean}`). Values are a `Types.encode(:term)` envelope `%{v, exp}`; `:ttl` seconds enforced client-side. No increment/append (use `Counter` / `read_modify_write_row/6`).

- **MegasPinakas.Counter** - `increment/8`, `decrement/8`, `get/7` (latest cell only), `set/8`, `reset/7`, `increment_many/6`, `increment_if_exists/8` (CAS via `check_and_mutate_row`, 5 attempts -> `{:ok, :applied | :not_applied} | {:error, :contention}`), `add_counter/4`, `increment_rule/3`, `extract_counter_value/3`.

- **MegasPinakas.CounterTTL** - `increment/7`, `get_current/7`, `get_window/7` (`:window_size >= 1`), `check_rate_limit/6`, `increment_with_limit/6`, `build_row_key/3`, `parse_row_key/1`, `bucket_to_seconds/1`.

- **MegasPinakas.TimeSeries** - `write_point/6`, `write_points/5` (`{:error, :nil_value}` / `{:error, {:unsupported_value, v}}` before any RPC), `query_recent/5`, `query_range/7` (half-open `[start, end)`, honours `:limit`), `time_series_row_key/2`, `reverse_timestamp/1` (19-digit), `from_reverse_timestamp/1`, `parse_row_key/1`. Stores a `value_type` tag column (`"i" | "f" | "s" | "j"`) beside `value`.

- **MegasPinakas.RowAssembler** - `new/0`, `reduce_all/1`, `apply_chunk/2`. A chunk with omitted family/qualifier and empty value is a real cell, not a commit marker.

### Key Dependencies
- **grpc_connection_pool** (~> 0.5.2) - GRPC connection pooling with health monitoring
- **googleapis_proto_ex** (~> 0.4) - Pre-compiled BigTable protobuf definitions (no Operations service; see `MegasPinakas.Longrunning`)
- **goth** (~> 1.4, `optional: true`) - Google Cloud token management; users add it to their own deps

### Service Stubs Used
- `Google.Bigtable.V2.Bigtable.Stub` - Data API (Data pool)
- `Google.Bigtable.Admin.V2.BigtableTableAdmin.Stub` - Table Admin API (Admin pool)
- `Google.Bigtable.Admin.V2.BigtableInstanceAdmin.Stub` - Instance Admin API (Admin pool)
- `MegasPinakas.Longrunning.Operations.Stub` - Long-running operations (Admin pool)

## Configuration

### Development (config/dev.exs)
```elixir
config :megas_pinakas, :emulator,
  host: "localhost",
  port: 8086,
  project_id: "dev-project"
```

### Production (config/prod.exs)
```elixir
config :megas_pinakas, :default_pool_size, 10   # sizes both pools
# No :emulator config = bigtable.googleapis.com + bigtableadmin.googleapis.com
# Optional full Data-pool config: config :megas_pinakas, GrpcConnectionPool, ...
```

### Runtime Override
`BIGTABLE_EMULATOR_HOST` is read directly by `MegasPinakas.Config` (not copied
into app config by `config/runtime.exs`) and takes precedence over `:emulator`.
Accepted forms: `host`, `host:port`, `[ipv6]`, `[ipv6]:port`; missing port
defaults to 8086. `host:abc`, `host:`, `:8086`, or unbracketed IPv6 raise
`ArgumentError` at boot.

`config/runtime.exs` also reads `MEGAS_PINAKAS_GOTH_NAME` and
`MEGAS_PINAKAS_POOL_SIZE` in `:prod`.

## API Pattern

Data API operations:
```elixir
operation = fn channel ->
  request = %Google.Bigtable.V2.SomeRequest{...}
  auth_opts = Auth.request_opts()
  Google.Bigtable.V2.Bigtable.Stub.some_rpc(channel, request, auth_opts)
end
Client.execute(operation)
```

Admin API operations MUST go through the admin pool:
```elixir
operation = fn channel ->
  request = %Google.Bigtable.Admin.V2.SomeRequest{...}
  auth_opts = Auth.request_opts()
  Google.Bigtable.Admin.V2.BigtableTableAdmin.Stub.some_rpc(channel, request, auth_opts)
end
Client.execute(operation, pool: Client.admin_pool())
```

`Auth.request_opts/0` raising `MegasPinakas.AuthError` inside the closure is the
intended way to abort before the RPC; `Client.execute/2` turns it into
`{:error, {:auth_error, reason}}`. Validate arguments (raise `ArgumentError`)
before building the closure so bad input never reaches the pool.

## Docker Development Environment

The project includes `docker-compose.yml` with the BigTable emulator only:
- Emulator runs on port 8086 (gRPC/h2c only; healthcheck is a TCP probe)
- Data is in-memory only (not persisted)
- No authentication required

```bash
docker compose up -d bigtable-emulator
```
