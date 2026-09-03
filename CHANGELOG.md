# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.7.0] - 2026-09-03

A remediation release. Several of these were production outages or silent
data bugs; read the Breaking section before upgrading.

### Breaking

**Admin API pool.** `MegasPinakas.Admin` and `MegasPinakas.InstanceAdmin` now
run on a second connection pool, `MegasPinakas.AdminConnectionPool`, pointed at
`bigtableadmin.googleapis.com`. Previously every admin RPC went to the Data API
host, which answers them with `UNIMPLEMENTED` — **no admin call has ever worked
against real BigTable**. In emulator mode both pools point at the emulator.
`Config.build_admin_pool_config/0`, `Config.admin_endpoint/0` and
`Client.admin_pool/0` are new; the admin pool is always built from
`:default_pool_size` and the emulator settings, never from the
`GrpcConnectionPool` key.

**Authentication failures are errors, not unauthenticated requests.**
`MegasPinakas.Auth.request_opts/0` raises `MegasPinakas.AuthError` when no token
can be obtained (was: log a warning and send the request with no
`authorization` metadata). `MegasPinakas.Client.execute/2` converts it to
`{:error, {:auth_error, reason}}` before any RPC is sent. Token refresh failures
are negatively cached for 5 s, so a broken credential no longer re-runs the
token source (or logs) on every RPC; `Auth.Cache.invalidate/1` clears it. The
cache no longer crashes when the token source raises, exits, throws, or returns
the wrong shape — failures surface as `{:token_source_error, msg}`,
`{:token_source_exit, kind, reason}` or `{:invalid_token_source_result, term}`,
and a misnamed Goth process yields `{:goth_exit, {:noproc, _}}`.

**Goth is an optional dependency** (`{:goth, "~> 1.4", optional: true}`). Add
`{:goth, "~> 1.4"}` to your own deps. With `:goth` configured but the library
absent, token fetches return `{:error, :goth_not_available}` instead of silently
falling back to the gcloud CLI.

**`Filter.row_key_prefix_filter/1` now matches all keys with the prefix.** It
emits `Regex.escape(prefix) <> "\\C*"`. The old `"^prefix"` pattern was an RE2
*full* match and only ever matched the row keyed exactly `prefix`. BigTable
regex filters have no implicit `.*` on either side; any hand-written
`row_key_regex_filter("^foo")` / `value_regex_filter("substr")` expecting
prefix/substring semantics must be padded with `\C*`. The `Filter` moduledoc
now has a "Regex semantics" section and every doc example was corrected.

**`MegasPinakas.Cache` re-encoded.** Values are stored as an Erlang-term
envelope (`%{v: value, exp: unix_seconds | nil}` via `Types.encode(:term)`)
instead of JSON, so any term — `nil`, integers, atom-keyed maps — roundtrips
exactly. Cache cells written by 0.6.x are not readable through `Cache`
(`get/5` returns `{:error, :unsafe_or_invalid_term}`); rewrite or drop them.
`Cache.exists?/5` returns `{:ok, boolean()} | {:error, term()}` instead of a bare
boolean, so transport errors are no longer reported as `false`.
`Cache.increment/6` and `Cache.append/6` were removed — they wrote bytes `get`
could not decode. Use `MegasPinakas.Counter` or `read_modify_write_row/6`.

**`Counter.increment_if_exists/8` is now a compare-and-swap increment.** It adds
`amount` to the current value instead of overwriting the counter *with*
`amount`. Returns `{:ok, :applied}`, `{:ok, :not_applied}` when the counter
column is absent (nothing written), or `{:error, :contention}` after 5 failed
CAS attempts.

**`MegasPinakas.TimeSeries` stores a type tag.** Each point now writes a
`value_type` column (`"i"`, `"f"`, `"s"`, `"j"`) beside `value` and decodes
from it. Integers were previously mis-decoded as floats because the type was
inferred from the byte length. Points written by earlier versions have no tag
and are returned with a raw binary `:value`; rewrite historical points to
migrate. `write_point/6` / `write_points/5` return `{:error, :nil_value}` for a
missing or `nil` value and `{:error, {:unsupported_value, v}}` for unencodable
terms, without issuing a request (previously crashed or wrote garbage);
`write_points/5` rejects the whole batch. Booleans are now supported (stored as
JSON). `query_range/7` is half-open `[start_time, end_time)` in wall-clock terms
(was `(start_time, end_time]`) and honours `:limit`, which it previously
ignored.

**`Types.encode(:integer, v)` raises on int64 overflow.** `set_integer/4`,
`write_integer/8` and `Row.put_integer/5` raise `ArgumentError` for values
outside the signed 64-bit range instead of silently truncating.

**Mid-stream gRPC errors are normalized.** `read_rows/4`,
`RowAssembler.reduce_all/1`, `sample_row_keys/4` and `mutate_rows/5` now return
`{:error, {:incomplete_read, {status_atom, message}}}` instead of wrapping a raw
`%GRPC.RPCError{}`.

**Removed:** `MegasPinakas.RowAssembler.stream_transform/1` (dead code;
`Streaming` never used it).

Telemetry:
- `[:megas_pinakas, :request, :stop]` metadata gains `result: :ok | {:error, tag}`
  where `tag` is the gRPC status atom, `:auth_error`, `:pool_error`, or the first
  element of a client-side error tuple.
- `[:megas_pinakas, :request, :exception]` metadata is now
  `%{pool, kind, reason, stacktrace}` with `reason` the raw exception/exit/throw
  term (was a string under `:reason`).
- A pool checkout failure emits `:stop` with `result: {:error, :pool_error}`
  instead of `:exception`.

Client:
- `Client.execute!/2` raises `MegasPinakas.Error` (with a `reason` field)
  instead of a `RuntimeError`.
- An operation that `exit`s or `throw`s inside `Client.execute/2` returns
  `{:error, {:execution_error, {kind, reason}}}` instead of crashing the caller.

Argument validation, all before any RPC is issued:
- `MegasPinakas.set_cell/4` raises `ArgumentError` unless `:timestamp_micros` is
  `-1` or a non-negative multiple of `1_000` (tables have millisecond
  granularity; the server rejected such writes anyway).
- `MegasPinakas.read_rows/4` raises `ArgumentError` for a `:max_rows` that is
  not `:infinity`/positive integer or a `:rows_limit` that is not a
  non-negative integer.
- `MegasPinakas.mutate_rows/5` requires a list (`FunctionClauseError`
  otherwise) and raises `ArgumentError` for entries that are not
  `%MegasPinakas.Row{}` or maps with a binary `row_key` and a list `mutations`
  (atom or string keys). `%MegasPinakas.Row{}` entries are now accepted directly.
- `MegasPinakas.check_and_mutate_row/8` and `read_modify_write_row/6` guard
  `is_binary(row_key)` (and list arguments) — non-binary keys raise
  `FunctionClauseError` instead of failing inside the RPC.
- `Row.put/5` raises `ArgumentError` (pointing at `put_term/5`) for atoms,
  `nil`, tuples and other terms it cannot infer a type for, instead of
  `FunctionClauseError`.
- `Filter.column_range_filter/2` and `Filter.value_range_filter/1` raise
  `ArgumentError` when both the `_closed` and `_open` bound are given for the
  same side (previously `_open` silently won).
- `Filter.timestamp_range_filter/2` rejects negative timestamps; `0` is
  documented as the unbounded sentinel, start inclusive / end exclusive.
- `Streaming.stream_rows/4` raises `ArgumentError` at construction for an
  invalid `:batch_size`, `:rows_limit`, `:max_retries`, or a `:rows` that is not
  a `%RowSet{}`/nil (previously the RowSet error surfaced only on the second
  batch).
- `InstanceAdmin.update_cluster/4` requires `:serve_nodes` (positive integer)
  and raises `ArgumentError` without it; it previously sent `serve_nodes: 0`.
- `InstanceAdmin.update_app_profile/4` raises `ArgumentError` when called with
  no updatable option, with `multi_cluster_routing: false`, or with both routing
  options; `create_app_profile/4` raises for `multi_cluster_routing: false`,
  both routing options, or a `:single_cluster_routing` map without a non-empty
  `cluster_id`. The update mask and body are now always consistent.
- `Admin.drop_row_range/4` returns `{:error, :no_target}` when neither
  `:row_key_prefix` nor `:delete_all_data_from_table` is given (previously sent
  an empty request) and raises `ArgumentError` when both are given or the prefix
  is not a binary.
- `Admin.create_table/4` raises `ArgumentError` for a non-map
  `:column_families`, a non-map/struct family config, or non-binary
  `:initial_splits` (previously an opaque `{:error, {:execution_error, ...}}`).
- `CounterTTL.get_window/7` raises `ArgumentError` for `:window_size` < 1
  (previously silently summed two buckets for 0).

Other contract changes:
- `Types.decode(:term, _)` failure reason is `{:error, :unsafe_or_invalid_term}`
  (was `:invalid_term_format`); the `:safe` atom-table constraint is documented.
- `Types.read_cells/6` returns `{:ok, %{"f:q" => nil, ...}}` (every requested
  key) for a missing row instead of `{:ok, %{}}`, and
  `{:error, {:decode, "f:q", reason}}` for a cell that fails to decode instead
  of `nil`.
- `BIGTABLE_EMULATOR_HOST` is validated: unparsable values (`host:abc`,
  `host:`, `:8086`, unbracketed IPv6) raise `ArgumentError` at boot. IPv6
  literals must be written `[addr]:port`. `config/runtime.exs` no longer copies
  the variable into `:emulator` config; `MegasPinakas.Config` reads it directly.
- An invalid `config :megas_pinakas, GrpcConnectionPool, ...` raises
  `ArgumentError` at boot instead of silently falling back to the default pool;
  when present and valid, `pool.name` is always forced to
  `MegasPinakas.ConnectionPool` (the name `Client` looks up).
- Minimum Elixir is `~> 1.18` (the locked `googleapis` 0.1.0, a transitive
  dependency of `grpc`, requires it; the previously declared `~> 1.15` was
  never satisfiable).
- Test support: `MegasPinakas.Test.Emulator.running?/0` removed;
  `await_pool!/1` waits for both pools.

### Fixed
- **A `ReadRows` chunk with omitted family/qualifier and an empty value was
  dropped as a "commit marker".** It is a real cell — the second version of an
  empty-valued column, or any extra version under `strip_value_filter`. Such
  cells are now assembled.
- `MegasPinakas.row_range_prefix/1` for an all-`0xFF` or empty prefix now yields
  `end_key: nil` (scan to end of table) instead of `{:end_key_open, <<>>}`,
  which matched nothing.
- gcloud CLI fallback is bounded to 10 s (`{:error, :gcloud_timeout}`), runs
  with `CLOUDSDK_CORE_DISABLE_PROMPTS=1`, no longer merges stderr into the
  token, and takes the last non-empty stdout line.
- `Auth.Cache.invalidate/1` uses the refresh timeout and returns `:ok` when the
  cache process is not running; `peek/0` returns `:error` instead of raising
  when the ETS table is absent.
- `CounterTTL.increment_with_limit/6` with `limit <= 0` always rate-limits
  without writing; the bucket key is computed once for the read and the
  increment, so a call straddling a bucket boundary cannot read one bucket and
  write another.
- `Admin.intersection_gc_rule/1` doc example said OR.

### Added
- `Admin.get_operation/1` and `Admin.wait_operation/2` for long-running
  operations, over a new `MegasPinakas.Longrunning` Operations gRPC service/stub
  on the admin pool. `wait_operation/2` accepts an `%Operation{}` or its name,
  polls (`:poll_interval` default 1 s, `:timeout` default 5 min) and returns the
  decoded `Table`/`Backup`/`Instance`/`Cluster`/`AppProfile`/`Empty` when the
  operation succeeds, `{:error, {status_atom, msg}}` when it fails, or
  `{:error, :timeout}`.
- `InstanceAdmin.partial_update_cluster/4` (`:serve_nodes` or `:autoscaling`)
  over `PartialUpdateCluster` with a FieldMask. Not supported by the emulator,
  which crashes on the RPC.
- `Cache`: `:ttl` option (seconds) on `put/6`, `put_many/5` and `get_or_put/6`;
  expiry is enforced client-side by `get`, `get_many`, `exists?` and
  `get_or_put`. Invalid `:ttl` raises `ArgumentError`. `get_many/5` honours
  `:app_profile_id`.
- `Streaming` retries a batch on `:unavailable`, `:deadline_exceeded` or
  `:aborted` (including inside `:incomplete_read`) up to `:max_retries`
  (default 3) with exponential backoff (100 ms doubling, capped at 2 s). New
  event `[:megas_pinakas, :stream, :retry]` with measurements `%{attempt: n}`
  and `:reason` in metadata.
- `[:megas_pinakas, :stream, :exception]` is emitted before `StreamError` is
  raised, with `:reason` in metadata. It is always followed by `:cancelled` for
  the same stream; every stream event now carries a `:stream_ref` for joining.
- `MegasPinakas.Response.normalize_reason/1` — normalizes a bare
  `%GRPC.RPCError{}` into `{status_atom, message}`; `format/1` delegates to it.
- `MegasPinakas.AuthError` and `MegasPinakas.Error` exceptions.
- `Config.project_path/1`, `Config.location_path/2`, `Config.admin_endpoint/0`,
  `Config.build_admin_pool_config/0`; `Client.admin_pool/0`.
- `Filter.row_sample_filter/1` accepts any `number()` strictly between 0 and 1
  and always emits a float; `0` and `1` are rejected client-side because
  BigTable answers them with `INVALID_ARGUMENT`.
- `Config.emulator?/0` is also true when the `GrpcConnectionPool` data endpoint
  is `type: :local`; the admin pool follows that endpoint too.

### Changed
- `Batch.write/5` splits batches over 100,000 mutations into sequential
  `MutateRows` requests (BigTable's per-request cap), re-bases each result's
  `index` to its position in the batch, and returns the first chunk error
  (earlier chunks were already applied, later ones not sent). An empty batch
  returns `{:ok, []}` without an RPC. The 256 MiB byte cap is not enforced
  client-side.
- `Streaming` no longer issues a trailing empty `ReadRows` RPC when the last
  batch came back short: scanning N rows costs `ceil(N / batch_size)` RPCs, plus
  one only when N is an exact multiple of the batch size.
- `Streaming.rows_exist?/4` and `first_row/4` request exactly one row in one
  RPC (`rows_limit: 1, batch_size: 1`).
- `Streaming.count_rows/4` applies `strip_value_filter` +
  `cells_per_row_limit_filter(1)` when no `:filter` is given, so only row keys
  cross the wire.
- `Counter.get/7` and `CounterTTL.get_current/7` fetch only the latest cell
  version (`cells_per_column_limit_filter(1)`); `Counter.get/7` accepts
  `:filter`. `Counter`, `CounterTTL` and `Cache` moduledocs state that GC rules
  (`max_versions_gc_rule(1)`, `max_age_gc_rule/1`) must be configured by the
  user — the `Cache` `max_age_gc_rule` is storage reclamation only, not what
  makes an entry expire.
- `CounterTTL.check_rate_limit/6` spec includes `{:error, term()}`.
- `MegasPinakas.StreamError` docs and message clarify that `last_key` is the
  last row delivered to the consumer; resume strictly after it.
- `TimeSeries` doc examples use real 19-digit reverse-timestamp values.

### Removed
- `MegasPinakas.Cache.increment/6`, `MegasPinakas.Cache.append/6` (see Breaking).
- `MegasPinakas.RowAssembler.stream_transform/1` (see Breaking).
- `MegasPinakas.Test.Emulator.running?/0`.
- The duplicate `BIGTABLE_EMULATOR_HOST` parser in `config/runtime.exs`.

### Internal/CI
- CI runs the test suite with `--include emulator` against a
  `docker compose`-started emulator, on both the declared floor
  (Elixir 1.18.3 / OTP 26.2) and Elixir 1.18.3 / OTP 27.2. Without the emulator
  the suite only exercised pure builders.
- `docker-compose.yml`: removed the `app` service and the obsolete `version:`
  key; the healthcheck is a TCP probe (the emulator speaks h2c only, so the old
  HTTP probe never passed).
- Arity-only "module structure" / `__info__` tests deleted across the suite;
  replaced with emulator tests for `check_and_mutate_row`,
  `read_modify_write_row`, every `Filter` builder, `Types.write_*`/`read_*`,
  `Cache`, `Counter`, `CounterTTL`, `TimeSeries`, `Admin`, `InstanceAdmin`, and
  the `Streaming` error path + telemetry events.
- Env-mutating tests use `try/after`; `auth_test.exs` never spawns gcloud.
- `.dialyzer_ignore.exs` is empty; specs use `GrpcConnectionPool.Pool.channel()`.

## [0.6.1] - 2026-08-07

### Fixed
- `MegasPinakas.column_filter/2` no longer emits an unescaped, unanchored column
  qualifier regex. It was a copy of `MegasPinakas.Filter.column_filter/2` that had
  drifted: the qualifier was interpolated raw, so `column_filter("cf", "user.name")`
  produced `{:column_qualifier_regex_filter, "user.name"}`. Qualifier regexes are
  RE2 and unanchored by default, so that matched `userXname` and substring-matched
  `prefix_user_name_suffix` — reading cells the caller never asked for. It now
  produces `"^user\\.name$"`, identical to `MegasPinakas.Filter.column_filter/2`.

  **This changes which cells a read returns.** Callers who deliberately passed a
  regex through `MegasPinakas.column_filter/2` should use
  `MegasPinakas.Filter.column_qualifier_regex_filter/1` instead.

### Changed
- The filter builders re-exported from `MegasPinakas` (`column_filter/2`,
  `family_filter/1`, `cells_per_column_limit_filter/1`, `pass_all_filter/0`,
  `block_all_filter/0`, `chain_filters/1`, `interleave_filters/1`) now delegate to
  `MegasPinakas.Filter` instead of re-implementing it, so the two cannot drift
  apart again. They therefore pick up `MegasPinakas.Filter`'s argument guards:
  `cells_per_column_limit_filter/1` now requires a positive integer, and the others
  require binaries or lists. Previously these accepted any term.

### Internal
- `extract_counter_value/3` is shared by `Counter` and `CounterTTL` rather than
  duplicated verbatim in both.
- Row-key splitting shared by `CounterTTL` and `TimeSeries` moved into
  `MegasPinakas.RowKey`.
- The token-expiry clock has a single implementation in `MegasPinakas.Auth.Cache`.

### Dependencies
- `grpc_connection_pool` `~> 0.5` → `~> 0.5.2` (pulls in `gun ~> 2.4`)

## [0.6.0] - 2026-08-06

### Breaking Changes
- `Client.execute/2` no longer double-wraps responses — returns `{:ok, val}` instead of `{:ok, {:ok, val}}`
- `read_rows/4` now returns assembled `{:ok, [Row.t()]}` instead of raw gRPC stream `{:ok, Enumerable.t()}`
- `mutate_rows/5` now returns `{:ok, [MutateRowsResponse.Entry.t()]}` instead of an unconsumed gRPC stream. It always documented "a list of results, one for each row"; callers who never enumerated the stream were silently discarding every per-entry mutation failure. Results are sorted by `index` to match the entries passed in. **`{:ok, results}` means the RPC succeeded, not that every row applied** — `MutateRows` is partial-success, so check `&1.status.code`
- `sample_row_keys/4` now returns `{:ok, [SampleRowKeysResponse.t()]}` instead of an unconsumed gRPC stream
- `Batch.write/5`, `Cache.delete_many/5` and `TimeSeries.write_points/5` return per-row result lists instead of passing an unconsumed stream through to the caller
- `read_rows/4` returns `{:error, {:incomplete_read, reason}}` when the row stream fails partway through, where it previously logged and returned `{:ok, partial_rows}` — truncated data reported as success
- `Streaming.stream_rows/4` and friends now raise `MegasPinakas.StreamError` on a mid-stream failure instead of halting quietly, which had made `Enum.to_list/1` return partial data indistinguishable from a complete result
- `Streaming.stream_rows/4` default `:batch_size` raised from `1_000` to `10_000` (see Performance)
- The gcloud CLI auth fallback is disabled in production builds. Configure Goth (or `:token_source`); override with `config :megas_pinakas, :allow_gcloud_auth_fallback, true`
- `Auth.request_opts/0` now includes `:timeout` key in all environments
- gRPC errors now return `{:error, {:not_found, msg}}` instead of `{:error, %GRPC.RPCError{status: 5, message: msg}}`
- Pool errors now return `{:error, {:pool_error, reason}}` instead of `{:error, reason}`
- `Client.with_connection/2` removed (was alias for `execute/2`)

### Performance

Measured with Benchee on an Apple M4 Max against the BigTable emulator
(`bench/auth_bench.exs`, `bench/read_path_bench.exs`), figures from two runs.

- **Auth token caching — ~0.8–1.4 s → ~270–330 ns per call.** Every RPC calls
  `Auth.request_opts/0`, which fetched a token with no caching whatsoever; through
  the gcloud CLI fallback that forked a subprocess per request.
  `MegasPinakas.Auth.Cache` now serves tokens from a `read_concurrency: true` ETS
  table via a lock-free read path. Allocation per call fell 93 712 B → 720 B. The
  uncached figure varies with whether gcloud has to refresh, hence the range.
- **Single-flight token refresh.** N concurrent callers arriving at expiry trigger
  one token fetch, not N — verified with 25 concurrent callers. Without it, expiry
  under load produced a thundering herd of ~1 s subprocess spawns.
- **Streaming scans ~2.3–2.8× faster at the default batch size.** Scanning 20 000
  rows, `batch_size: 1_000` (old default, 20 RPCs) took 148–172 ms;
  `batch_size: 10_000` (new default, 2 RPCs) takes 61–63 ms, within ~9% of the
  56–58 ms a single eager RPC achieves. `batch_size: 100` (200 RPCs) costs
  632–896 ms, which is why the default matters. `:batch_size` trades round trips
  against peak memory; documented on `stream_rows/4`.
- **Telemetry span coverage 12% → 99.7–99.9%,** and reported duration now scales
  with result size (11.8 ms at 100 rows → 61.3 ms at 20 000) instead of sitting
  flat at ~7 ms. See Fixed.
- **Row assembly confirmed linear-ish, not quadratic.** `RowAssembler.reduce_all/1`
  on a single row of N columns: 0.354 µs/column at 1 000 columns, 1.069 µs/column
  at 32 000 — a 32× size increase costs ~3× per unit, consistent with BEAM hashmap
  depth in `Map.update/4` rather than an O(n²) scan.

### Added
- `MegasPinakas.Auth.Cache` — ETS-backed access token cache with a lock-free read path and single-flight refresh. Expiry comes from Goth's token metadata; the gcloud fallback, which reports no expiry, assumes a conservative 55-minute TTL
- `:token_source` config — a 0-arity function or `{module, function, args}` returning `{:ok, %{token: token, expires_at: unix_seconds}}`, for workload identity federation, token brokers, or anything Goth does not cover
- `MegasPinakas.RowAssembler` — the `ReadRows` chunk→row state machine, extracted from `MegasPinakas` and exposed as both `reduce_all/1` (eager) and `stream_transform/1` (lazy)
- `MegasPinakas.StreamError` — raised when a lazily-consumed stream fails partway through, carrying `reason` and the `last_key` successfully delivered so a caller can resume
- `:max_rows` option on `read_rows/4` (default `:infinity`) — returns `{:error, :result_too_large}` rather than materializing an unbounded result. Implemented by requesting `max_rows + 1` from the server, so exceeding the cap costs one extra row instead of a full scan
- `:rows_limit` option on `Streaming.stream_rows/4` — now honoured (see Fixed)
- Telemetry events `[:megas_pinakas, :stream, :start | :stop | :cancelled]` for lazily-consumed streams, with `duration`, `rows_emitted` and `batches`. A stream consumed at the caller's pace has no single duration a request span can represent, and `:cancelled` distinguishes early abandonment from exhaustion
- `MegasPinakas.Response` — normalizes gRPC responses into idiomatic `{:ok, result} | {:error, {atom, msg}}` tuples, mapping all 17 gRPC status codes to descriptive atoms
- Configurable gRPC timeout (default 30s) via `Config.default_timeout/0` and `:default_timeout` app config
- Telemetry events: `[:megas_pinakas, :request, :start | :stop | :exception]` with duration and pool metadata
- GitHub Actions CI pipeline — compile, format, credo, test, dialyzer, auto-publish to hex on version tags
- Guard clauses on `mutate_row/6` and `read_row/5`

### Fixed
- **Infinite loop streaming a `row_keys`-only row set.** `Streaming.fetch_batch/1`
  fell through to "keep existing row set" for any shape it did not recognise,
  re-sending a byte-identical request forever. A 3-key row set yielded those rows
  in an endless cycle and `count_rows/4` never returned. The cursor now either
  advances or reports exhaustion, with no catch-all — an unrecognised `:rows`
  value raises instead of looping.
- **Mixed row sets silently lost their discrete keys.** Range rewriting hardcoded
  `row_keys: []`, so a row set combining keys and ranges dropped the keys after
  the first page.
- **Range rewriting could widen a range.** The cursor unconditionally wrote
  `{:start_key_open, last_key}`, which *lowered* a start bound already past
  `last_key` and re-read rows the caller never asked for. It now only ever raises
  a start bound, and drops ranges already fully consumed.
- **`Streaming.stream_rows/4` ignored `:rows_limit`.** It was overwritten by
  `:batch_size`, so `stream_rows(..., rows_limit: 10)` streamed the whole table.
- **Telemetry span excluded stream consumption — coverage 12% at 10k rows.**
  `read_rows/4` consumed the gRPC stream *after* `Client.execute/2` returned, so
  the reported duration measured only the time to obtain the stream handle, and
  stayed flat at ~7 ms while real time grew with result size. Collection moved
  inside the operation function; coverage is now 99.7–99.9% from 100 to 20 000
  rows, and duration scales with result size.
- **A doomed read reported success.** Because consumption happened outside
  `Client.execute/2`'s `try`, a read that raised during consumption emitted a
  **success** `:stop` event and then let the exception escape uncaught. It now
  emits `:exception` and returns `{:error, {:execution_error, _}}`.
- **Chunk assembly: row keys lost on multi-chunk rows.** proto3 renders an unset
  `bytes` field as `""`, so `chunk.row_key || current_key` never fell back. Real
  BigTable sets `row_key` only on a row's first chunk, so every multi-chunk row
  was assigned the empty key.
- **Chunk assembly: column families lost.** `family_name` and `qualifier` are only
  sent when they change; they are now carried in the assembler state instead of
  being read as `nil` for every subsequent cell in the same family.
- **Chunk assembly: values split across chunks were fragmented.** A value spanning
  several `CellChunk`s (`value_size > 0`) became several separate cells instead of
  one concatenated value.
  *These three are unreachable through the emulator, which sets `row_key` on every
  chunk and never splits values; they are covered by unit tests over synthetic
  chunk sequences matching the documented wire protocol.*
- Auth fallback silently returning empty opts on token failure — now logs warning
- Auth rescue catching all exceptions — now catches specific `ErlangError` for missing gcloud
- `build_row_key/3` typespec accepting `nil` timestamp parameter
- Unreachable empty list match in streaming pagination
- Two tautological test assertions (`assert Enumerable.impl_for(stream) != nil`) that compared disjoint types and passed regardless of behaviour

### Changed
- Upgraded `grpc` 0.11 → 1.0, `protobuf` 0.15 → 0.17, `googleapis_proto_ex` 0.3.3 → 0.4, `grpc_connection_pool` 0.2.1 → 0.5.1, `credo` → 1.7.19, `ex_doc` → 0.40
- Removed the explicit `{GRPC.Client.Supervisor, []}` child spec: grpc >= 1.0 starts its own client `DynamicSupervisor`, so there is nothing for us to add to the tree
- Supervisor strategy is `:one_for_one`. An earlier 0.6.0 draft changed it to `:rest_for_one` "so the connection pool restarts when the gRPC supervisor crashes"; under grpc 1.0 that supervisor is no longer our child, so the rationale no longer holds
- `MegasPinakas.Auth.Cache` is started ahead of the connection pool, since pooled connections issue authenticated RPCs
- `config/prod.exs` and the README now document Goth as the required production setup. Previously neither `:emulator` nor `:goth` was configured in `prod.exs`, which made the ~1 s gcloud subprocess fallback the de facto production default
- Optimized `build_row` with single-pass grouping (eliminates multiple `Enum.reverse` + `Enum.group_by` passes)
- Extracted `build_routing_policy/2` helper in `InstanceAdmin` (removed 30 lines duplication)
- Decomposed `process_chunk` into `accumulate_cells`/`apply_row_status`/`resolve_row_key`
- Extracted `columns_to_map`/`latest_cell_value` from `row_to_map`/`get_family`
- Extracted helpers in cache, counter, counter_ttl, types to reduce nesting depth
- Fixed doc groups: replaced phantom `MegasPinakas.Connection` with `MegasPinakas.Client`
- All aliases sorted alphabetically, all large numbers use underscores
- Added `credo` and `dialyxir` as dev/test dependencies
- Full codebase formatted

## [0.5.0] - 2024-12-09

### Added

#### Core Operations
- `MegasPinakas` - Main module with BigTable data operations
  - `read_row/5`, `read_rows/4` - Read operations
  - `mutate_row/5`, `mutate_rows/4` - Write operations
  - `check_and_mutate_row/8` - Conditional mutations
  - `read_modify_write_row/6` - Atomic read-modify-write
  - `sample_row_keys/4` - Key sampling for splits

#### Row Ranges
- `row_set/1`, `row_set_from_ranges/1` - Row set builders
- `row_range/2` - Default range (start inclusive, end exclusive)
- `row_range_prefix/1` - Prefix-based scanning
- `row_range_open/2`, `row_range_closed/2`, `row_range_open_closed/2` - Boundary variants
- `row_range_from/1`, `row_range_until/1`, `row_range_unbounded/0` - Partial ranges

#### Filters (`MegasPinakas.Filter`)
- Row filters: `row_key_regex_filter/1`, `row_sample_filter/1`
- Cell filters: `cells_per_row_limit_filter/1`, `cells_per_row_offset_filter/1`
- Column filters: `column_qualifier_regex_filter/1`, `column_range_filter/2`
- Value filters: `value_regex_filter/1`, `value_range_filter/1`
- Time filters: `timestamp_range_filter/2`, `time_window_filter/2`
- Modifying filters: `strip_value_filter/0`, `apply_label_filter/1`
- Composing filters: `chain_filters/1`, `interleave_filters/1`, `condition_filter/3`
- Convenience: `latest_only_filter/0`, `column_latest_filter/2`

#### Type-Aware Operations (`MegasPinakas.Types`)
- Read/write with automatic encoding: `read_json/7`, `write_json/8`, `read_integer/7`, `write_integer/8`, etc.
- Supported types: binary, string, JSON, integer, float, boolean, datetime, term
- Batch operations: `write_cells/6`, `read_cells/6`
- Mutation builders: `set_json/4`, `set_integer/4`, `set_datetime/4`, etc.

#### Row Builder (`MegasPinakas.Row`)
- Fluent API for building multi-cell rows
- Type-inferred `put/5` with explicit variants (`put_json/5`, `put_integer/5`, etc.)
- Delete operations: `delete_cell/3`, `delete_family/2`, `delete_row/1`

#### Batch Builder (`MegasPinakas.Batch`)
- Batch mutation building with `new/0`, `add/2`, `add_all/2`
- Execute with `write/5`

#### Counters (`MegasPinakas.Counter`)
- Atomic operations: `increment/8`, `decrement/8`, `get/7`, `set/8`, `reset/7`
- Multi-counter: `increment_many/6`

#### Time-Windowed Counters (`MegasPinakas.CounterTTL`)
- Time-bucketed counters for rate limiting
- `increment/7`, `get_current/7`, `get_window/7`
- Rate limiting: `check_rate_limit/6`, `increment_with_limit/7`
- Bucket types: `:second`, `:minute`, `:hour`, `:day`, `:week`

#### Time Series (`MegasPinakas.TimeSeries`)
- Reverse timestamp ordering for recent-first queries
- `write_point/6`, `write_points/5`
- `query_recent/5`, `query_range/7`
- Row key helpers: `time_series_row_key/2`, `reverse_timestamp/1`, `parse_row_key/1`

#### Streaming (`MegasPinakas.Streaming`)
- Memory-efficient streaming with `Stream.resource`
- `stream_rows/4`, `stream_rows_as_maps/4`, `stream_rows_with_keys/4`
- `stream_range/6`, `stream_prefix/5`, `stream_in_chunks/5`
- Utilities: `count_rows/4`, `rows_exist?/4`, `first_row/4`

#### Cache (`MegasPinakas.Cache`)
- Simple key-value cache backed by BigTable
- `get/5`, `put/6`, `delete/5`, `get_or_put/6`
- Multi-key: `get_many/5`, `put_many/5`, `delete_many/5`
- Atomic: `increment/6`, `append/6`, `exists?/5`

#### Administration
- `MegasPinakas.Admin` - Table management (create, delete, modify column families)
- `MegasPinakas.InstanceAdmin` - Instance and cluster management

#### Infrastructure
- `MegasPinakas.Connection` - gRPC connection pooling
- Support for BigTable emulator and production (via Goth)
- Configurable pool size and timeouts

[0.7.0]: https://github.com/nyo16/megas_pinakas/releases/tag/v0.7.0
[0.6.1]: https://github.com/nyo16/megas_pinakas/releases/tag/v0.6.1
[0.6.0]: https://github.com/nyo16/megas_pinakas/releases/tag/v0.6.0
[0.5.0]: https://github.com/nyo16/megas_pinakas/releases/tag/v0.5.0
