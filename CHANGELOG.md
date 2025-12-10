# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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

[0.5.0]: https://github.com/niko-1/megas_pinakas/releases/tag/v0.5.0
