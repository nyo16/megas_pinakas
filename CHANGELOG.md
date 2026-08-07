# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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

[0.6.1]: https://github.com/nyo16/megas_pinakas/releases/tag/v0.6.1
[0.6.0]: https://github.com/nyo16/megas_pinakas/releases/tag/v0.6.0
[0.5.0]: https://github.com/nyo16/megas_pinakas/releases/tag/v0.5.0
