# MegasPinakas

An Elixir client library for Google Cloud BigTable, providing a high-level interface for data operations, table administration, and instance management via gRPC.

## Requirements

Elixir 1.18 or later (the locked `googleapis` 0.1.0, pulled in by `grpc`, requires it).

## Installation

Add `megas_pinakas` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:megas_pinakas, "~> 0.7.0"}
  ]
end
```

## Configuration

The application starts two gRPC connection pools:

- `MegasPinakas.ConnectionPool` - Data API (`bigtable.googleapis.com`), used by
  `MegasPinakas`, `Streaming`, `Types`, `Cache`, `Counter`, and so on.
- `MegasPinakas.AdminConnectionPool` - Admin API (`bigtableadmin.googleapis.com`),
  used by `MegasPinakas.Admin` and `MegasPinakas.InstanceAdmin`. Google serves the
  admin RPCs from a separate host; the Data API host answers them with
  `UNIMPLEMENTED`.

Both pools are sized by `:default_pool_size` (default 10). In emulator mode both
point at the emulator.

### Development with Emulator

For local development, use the BigTable emulator:

```bash
# Start the emulator
docker compose up -d bigtable-emulator
```

Configure your application:

```elixir
# config/dev.exs
config :megas_pinakas, :emulator,
  host: "localhost",
  port: 8086,
  project_id: "dev-project"
```

Or set the environment variable, which takes precedence over the `:emulator`
config and is read directly by `MegasPinakas.Config`:

```bash
export BIGTABLE_EMULATOR_HOST=localhost:8086   # host:port
export BIGTABLE_EMULATOR_HOST=localhost        # port defaults to 8086
export BIGTABLE_EMULATOR_HOST=[::1]:8086       # IPv6 literals must be bracketed
```

An unparsable value (`host:abc`, `host:`, `:8086`, unbracketed IPv6) raises
`ArgumentError` at boot rather than failing later as a connection error.

### Custom Data pool configuration

`config :megas_pinakas, GrpcConnectionPool, ...` (see
`GrpcConnectionPool.Config.from_env/2`) replaces the derived **Data** pool
configuration verbatim; `pool.name` is always forced to `MegasPinakas.ConnectionPool`.
An invalid config raises `ArgumentError` at boot. The Admin pool is never built
from this key: it always follows `:default_pool_size` and the emulator settings,
and a `type: :local` Data endpoint puts both pools in emulator mode
(`MegasPinakas.Config.emulator?/0` returns `true`, no auth metadata is sent).

### Production with Goth Authentication

**Goth is the recommended setup for production.** MegasPinakas has three token
sources — Goth, a custom `:token_source`, and a gcloud CLI fallback — and the
gcloud fallback is **disabled in production builds**. It shells out to
`gcloud auth application-default print-access-token`, which was measured at
0.85–1.11 s per call and needs an interactive login, so it exists only for local
development.

Goth is an **optional** dependency of this library: add `{:goth, "~> 1.4"}` to
your own `deps`. If `:goth` is configured but the library is not compiled in,
token fetches fail with `{:error, :goth_not_available}`; there is no silent
fallback to the gcloud CLI.

Tokens are cached by `MegasPinakas.Auth.Cache`, so a warm `request_opts/0` is a
lock-free ETS read (~270 ns) rather than a token fetch per RPC.

For production, use [Goth](https://github.com/peburrows/goth) for Google Cloud authentication:

```elixir
# Add to dependencies in mix.exs
def deps do
  [
    {:megas_pinakas, "~> 0.7.0"},
    {:goth, "~> 1.4"}
  ]
end
```

Point MegasPinakas at your Goth process:

```elixir
# config/runtime.exs
if config_env() == :prod do
  config :megas_pinakas, :goth, MegasPinakas.Goth
end
```

Add Goth to your application's supervision tree:

```elixir
# lib/my_app/application.ex
def start(_type, _args) do
  credentials =
    "GOOGLE_APPLICATION_CREDENTIALS_JSON"
    |> System.fetch_env!()
    |> Jason.decode!()

  children = [
    {Goth, name: MegasPinakas.Goth, source: {:service_account, credentials}},
    # ... other children
  ]

  Supervisor.start_link(children, strategy: :one_for_one)
end
```

Alternative: Use a credentials file:

```elixir
credentials = "path/to/service-account.json" |> File.read!() |> Jason.decode!()

children = [
  {Goth, name: MegasPinakas.Goth, source: {:service_account, credentials}}
]
```

Configure MegasPinakas to use Goth:

```elixir
# config/prod.exs
config :megas_pinakas, :goth, MegasPinakas.Goth
config :megas_pinakas, :default_pool_size, 10
```

### Authentication failures

When no token can be obtained, requests are **not** sent unauthenticated.
`MegasPinakas.Auth.request_opts/0` raises `MegasPinakas.AuthError`, and
`MegasPinakas.Client.execute/2` converts it to `{:error, {:auth_error, reason}}`
before any RPC is issued, so every high-level function returns that tuple:

```elixir
case MegasPinakas.read_row(project, instance, "users", "user#123") do
  {:ok, row} -> row
  {:error, {:auth_error, reason}} -> Logger.error("BigTable auth failed: #{inspect(reason)}")
  {:error, {:unavailable, msg}} -> :retry
end
```

A refresh failure is cached for 5 seconds, so a broken credential does not
re-run the token source (or log) on every RPC; `MegasPinakas.Auth.Cache.invalidate/1`
clears it once the credential is fixed. Failure reasons you may see:
`{:token_source_error, msg}`, `{:token_source_exit, kind, reason}`,
`{:invalid_token_source_result, term}`, `{:goth_exit, {:noproc, _}}` (Goth
process not started under that name), `:goth_not_available`,
`:gcloud_fallback_disabled`, `:gcloud_timeout`.

### Custom token sources

If Goth does not fit your setup (workload identity federation, a shared token
broker, a vault), provide your own source. It must return the token with its
scheme included, since the value is used verbatim as `authorization` metadata:

```elixir
config :megas_pinakas, :token_source, {MyApp.Auth, :bigtable_token, []}

# @spec bigtable_token() ::
#         {:ok, %{token: String.t(), expires_at: non_neg_integer()}} | {:error, term()}
def bigtable_token do
  {:ok, %{token: "Bearer " <> jwt, expires_at: unix_seconds}}
end
```

`expires_at` is an absolute Unix timestamp in seconds. `MegasPinakas.Auth.Cache`
refreshes once fewer than 60 seconds remain, and collapses concurrent refreshes
so N callers hitting expiry together cost one fetch, not N.

### Re-enabling the gcloud fallback

Only if you genuinely need it in a production build:

```elixir
config :megas_pinakas, :allow_gcloud_auth_fallback, true
```

Every use logs a warning. Expect ~1 s per token refresh. The subprocess runs
with `CLOUDSDK_CORE_DISABLE_PROMPTS=1` and is killed after 10 s
(`{:error, :gcloud_timeout}`); when the fallback is disabled the source returns
`{:error, :gcloud_fallback_disabled}`.

## Usage

### Data Operations

```elixir
# Write a row
mutations = [
  MegasPinakas.set_cell("cf", "name", "John Doe"),
  MegasPinakas.set_cell("cf", "email", "john@example.com")
]
{:ok, _} = MegasPinakas.mutate_row("project", "instance", "users", "user#123", mutations)

# Read a row
{:ok, row} = MegasPinakas.read_row("project", "instance", "users", "user#123")

# Read multiple rows with filter
filter = MegasPinakas.family_filter("cf")
{:ok, rows} = MegasPinakas.read_rows("project", "instance", "users",
  rows: MegasPinakas.row_set(["user#1", "user#2", "user#3"]),
  filter: filter)

# Batch mutations. MutateRows is partial-success: {:ok, results} means the RPC
# succeeded, not that every row applied — check each entry's status.
entries = [
  %{row_key: "row1", mutations: [MegasPinakas.set_cell("cf", "col", "val1")]},
  %{row_key: "row2", mutations: [MegasPinakas.set_cell("cf", "col", "val2")]}
]
{:ok, results} = MegasPinakas.mutate_rows("project", "instance", "table", entries)
failed = Enum.reject(results, &(&1.status.code == 0))

# Atomic increment
rules = [MegasPinakas.increment_rule("cf", "counter", 1)]
{:ok, _} = MegasPinakas.read_modify_write_row("project", "instance", "table", "row", rules)
```

### Table Administration

`MegasPinakas.Admin` and `MegasPinakas.InstanceAdmin` run on the Admin pool
(`bigtableadmin.googleapis.com`); see [Configuration](#configuration).

```elixir
alias MegasPinakas.Admin

# Create a table with column families
{:ok, table} = Admin.create_table("project", "instance", "my-table",
  column_families: %{
    "cf" => %{gc_rule: Admin.max_versions_gc_rule(1)},
    "metadata" => %{gc_rule: Admin.max_age_gc_rule(86400)}
  })

# List tables
{:ok, response} = Admin.list_tables("project", "instance")

# Modify column families
modifications = [
  Admin.create_column_family("new_cf", Admin.max_versions_gc_rule(3)),
  Admin.drop_column_family("old_cf")
]
{:ok, _} = Admin.modify_column_families("project", "instance", "table", modifications)

# Drop rows by prefix. Exactly one of :row_key_prefix / :delete_all_data_from_table
# is required; neither returns {:error, :no_target}, both raise ArgumentError.
{:ok, _} = Admin.drop_row_range("project", "instance", "table", row_key_prefix: "tmp#")

# Delete a table
{:ok, _} = Admin.delete_table("project", "instance", "my-table")
```

### Long-running operations

Backups, restores, and every instance/cluster mutation return a
`Google.Longrunning.Operation`. Resolve it with `Admin.wait_operation/2`, which
polls `GetOperation` on the Admin pool and decodes the result:

```elixir
expire_time = %Google.Protobuf.Timestamp{seconds: System.os_time(:second) + 7 * 86_400}

{:ok, operation} =
  Admin.create_backup("project", "instance", "cluster", "backup-1", "my-table",
    expire_time: expire_time)

# Defaults: poll every second, give up after 5 minutes
case Admin.wait_operation(operation, timeout: :timer.minutes(10)) do
  {:ok, %Google.Bigtable.Admin.V2.Backup{} = backup} -> backup
  {:error, :timeout} -> :still_running
  {:error, {status, msg}} -> {status, msg}
end

# One-shot state check by name
{:ok, %Google.Longrunning.Operation{done: done?}} = Admin.get_operation(operation.name)
```

`wait_operation/2` also accepts the operation name, returns immediately for an
operation that is already `done`, and yields `Table`, `Backup`, `Instance`,
`Cluster`, `AppProfile` or `Google.Protobuf.Empty` depending on the RPC.

### Garbage Collection Rules

Column families support GC rules to automatically delete old data:

```elixir
alias MegasPinakas.Admin

# Keep only the N most recent versions
gc_rule = Admin.max_versions_gc_rule(3)

# Delete cells older than N seconds (TTL)
gc_rule = Admin.max_age_gc_rule(86400)  # 24 hours

# Intersection (AND) - all conditions must be met to GC
# Keep 3 versions AND data must be older than 7 days to be deleted
gc_rule = Admin.intersection_gc_rule([
  Admin.max_versions_gc_rule(3),
  Admin.max_age_gc_rule(604800)
])

# Union (OR) - any condition triggers GC
# Delete if more than 1000 versions OR older than 30 days
gc_rule = Admin.union_gc_rule([
  Admin.max_versions_gc_rule(1000),
  Admin.max_age_gc_rule(2592000)
])
```

Apply GC rules when creating or modifying column families:

```elixir
# At table creation
{:ok, table} = Admin.create_table("project", "instance", "table",
  column_families: %{
    "cf" => %{gc_rule: Admin.max_versions_gc_rule(1)}
  })

# Add column family with GC rule to existing table
modifications = [
  Admin.create_column_family("new_cf", Admin.max_versions_gc_rule(3))
]
{:ok, _} = Admin.modify_column_families("project", "instance", "table", modifications)

# Update GC rule on existing column family
modifications = [
  Admin.update_column_family("cf", Admin.max_age_gc_rule(86400))
]
{:ok, _} = Admin.modify_column_families("project", "instance", "table", modifications)
```

### Instance Administration

```elixir
alias MegasPinakas.InstanceAdmin

# Create an instance
clusters = %{
  "my-cluster" => %{
    location: "us-central1-b",
    serve_nodes: 3,
    storage_type: :SSD
  }
}
{:ok, operation} = InstanceAdmin.create_instance("project", "my-instance", clusters,
  display_name: "My Instance",
  type: :PRODUCTION)
{:ok, %Google.Bigtable.Admin.V2.Instance{}} = MegasPinakas.Admin.wait_operation(operation)

# List instances
{:ok, response} = InstanceAdmin.list_instances("project")

# Resize a cluster. UpdateCluster is a full replace, so :serve_nodes is required
# (omitting it raises ArgumentError rather than silently requesting 0 nodes).
{:ok, operation} = InstanceAdmin.update_cluster("project", "instance", "my-cluster", serve_nodes: 5)

# Change one field, or switch to autoscaling, without touching the rest.
# Not supported by the emulator (it crashes on PartialUpdateCluster).
{:ok, operation} = InstanceAdmin.partial_update_cluster("project", "instance", "my-cluster",
  autoscaling: %{min_serve_nodes: 1, max_serve_nodes: 5, cpu_utilization_percent: 60})
{:ok, %Google.Bigtable.Admin.V2.Cluster{}} = MegasPinakas.Admin.wait_operation(operation)

# Create an app profile. multi_cluster_routing: false and giving both routing
# options raise ArgumentError; the same applies to update_app_profile/4.
{:ok, profile} = InstanceAdmin.create_app_profile("project", "instance", "profile-id",
  description: "My app profile",
  multi_cluster_routing: true)
```

## Filters

```elixir
# Filter by column family
filter = MegasPinakas.family_filter("cf")

# Filter by specific column
filter = MegasPinakas.column_filter("cf", "col")

# Limit cells per column
filter = MegasPinakas.cells_per_column_limit_filter(1)

# Chain filters (AND)
filter = MegasPinakas.chain_filters([
  MegasPinakas.family_filter("cf"),
  MegasPinakas.cells_per_column_limit_filter(1)
])

# Interleave filters (OR)
filter = MegasPinakas.interleave_filters([
  MegasPinakas.family_filter("cf1"),
  MegasPinakas.family_filter("cf2")
])
```

### Regex filters

Every `*_regex_filter` is an RE2 pattern that BigTable matches against the
**whole** row key, qualifier, family name, or value. There is no implicit `.*`
on either side: `Filter.row_key_regex_filter("user#")` matches only the row
keyed exactly `user#`, and `^`/`$` are redundant. To match a prefix, suffix, or
substring, pad the pattern with `\C*` (the RE2 byte wildcard; `.` does not match
`\n` or arbitrary bytes):

```elixir
alias MegasPinakas.Filter

Filter.row_key_prefix_filter("user#")              # prefix; emits Regex.escape("user#") <> "\\C*"
Filter.row_key_regex_filter("\\C*_count")          # suffix
Filter.value_regex_filter("\\C*admin\\C*")         # substring
Filter.column_qualifier_regex_filter("meta_\\C*")  # qualifiers starting with meta_
```

`family_filter/1` and `column_filter/2` escape and anchor their arguments for
you and match exact names.

## Row Ranges

```elixir
# Specific row keys
row_set = MegasPinakas.row_set(["row1", "row2", "row3"])

# Row range (default: start inclusive, end exclusive)
range = MegasPinakas.row_range("user#100", "user#200")
row_set = MegasPinakas.row_set_from_ranges([range])

# Various range types
range = MegasPinakas.row_range_open("a", "z")       # Both exclusive: (a, z)
range = MegasPinakas.row_range_closed("a", "z")    # Both inclusive: [a, z]
range = MegasPinakas.row_range_open_closed("a", "z") # Start exclusive, end inclusive: (a, z]
range = MegasPinakas.row_range_from("user#500")    # From key to end: [user#500, ∞)
range = MegasPinakas.row_range_until("user#500")   # From start to key: [∅, user#500)
range = MegasPinakas.row_range_unbounded()         # All rows: [∅, ∞)
```

### Prefix Scans

Prefix scans efficiently retrieve all rows starting with a given prefix:

```elixir
# All users (rows starting with "user#")
range = MegasPinakas.row_range_prefix("user#")
# Internally creates range: ["user#", "user$") where $ is next char after #.
# An empty or all-0xFF prefix yields end_key: nil (scan to end of table).

# All posts for a specific user
range = MegasPinakas.row_range_prefix("user#123#posts#")
# Matches: user#123#posts#001, user#123#posts#002, etc.

# Combine with read_rows
{:ok, rows} = MegasPinakas.read_rows(project, instance, "table",
  rows: MegasPinakas.row_set_from_ranges([MegasPinakas.row_range_prefix("user#")]),
  rows_limit: 100
)
```

### Start and End Keys with Inclusive/Exclusive Bounds

Understanding inclusive vs exclusive bounds is crucial for pagination and range queries:

```elixir
# INCLUSIVE start, EXCLUSIVE end (default): [start, end)
# Use when: Standard range queries, initial page loads
range = MegasPinakas.row_range("user#100", "user#200")
# Includes: user#100, user#101, ..., user#199
# Excludes: user#200

# EXCLUSIVE start, EXCLUSIVE end: (start, end)
# Use when: Paginating after a known key
range = MegasPinakas.row_range_open("user#100", "user#200")
# Excludes: user#100 and user#200
# Includes: user#101, ..., user#199

# INCLUSIVE start, INCLUSIVE end: [start, end]
# Use when: You want both boundary keys included
range = MegasPinakas.row_range_closed("user#100", "user#200")
# Includes: user#100, user#101, ..., user#199, user#200

# EXCLUSIVE start, INCLUSIVE end: (start, end]
# Use when: Paginating backwards or specific boundary needs
range = MegasPinakas.row_range_open_closed("user#100", "user#200")
# Excludes: user#100
# Includes: user#101, ..., user#200
```

### Pagination Example

```elixir
# First page - start from beginning
first_page_range = MegasPinakas.row_range_prefix("user#")
{:ok, rows} = MegasPinakas.read_rows(project, instance, "users",
  rows: MegasPinakas.row_set_from_ranges([first_page_range]),
  rows_limit: 100
)

# Get last key from results
last_key = rows |> List.last() |> MegasPinakas.row_key()
# => "user#099"

# Next page - use EXCLUSIVE start to skip the last seen key
next_page_range = MegasPinakas.row_range_open(last_key, "user#" <> <<255>>)
# Or use the simpler approach with open start:
next_page_range = %Google.Bigtable.V2.RowRange{
  start_key: {:start_key_open, last_key},
  end_key: {:end_key_open, "user#" <> <<255>>}
}

{:ok, next_rows} = MegasPinakas.read_rows(project, instance, "users",
  rows: MegasPinakas.row_set_from_ranges([next_page_range]),
  rows_limit: 100
)
```

### Combining Prefix with Bounds

```elixir
# All user posts from a specific date range (using composite keys)
# Key format: user#<user_id>#posts#<timestamp>

# Posts for user 123 from 2024-01 only
start_key = "user#123#posts#2024-01-01"
end_key = "user#123#posts#2024-02-01"
range = MegasPinakas.row_range(start_key, end_key)  # [start, end)

# Posts for user 123 AFTER a specific post (for pagination)
last_seen_post = "user#123#posts#2024-01-15T10:30:00"
range = MegasPinakas.row_range_open(last_seen_post, "user#123#posts#" <> <<255>>)
# Excludes the last_seen_post, includes everything after until end of prefix
```

## Type-Aware Operations

The `MegasPinakas.Types` module provides type-safe encoding/decoding:

```elixir
alias MegasPinakas.Types

# Write typed values
Types.write_json(project, instance, "table", "row", "cf", "data", %{name: "John", age: 30})
Types.write_integer(project, instance, "table", "row", "cf", "count", 42)
Types.write_datetime(project, instance, "table", "row", "cf", "created", DateTime.utc_now())

# Read typed values
{:ok, data} = Types.read_json(project, instance, "table", "row", "cf", "data")
{:ok, count} = Types.read_integer(project, instance, "table", "row", "cf", "count")

# Write multiple typed cells
Types.write_cells(project, instance, "table", "row", [
  {:string, "cf", "name", "John Doe"},
  {:integer, "cf", "age", 30},
  {:json, "cf", "profile", %{city: "NYC"}},
  {:datetime, "cf", "created", DateTime.utc_now()}
])
```

Integers are stored as signed 64-bit big-endian; `Types.encode(:integer, v)`,
`Types.write_integer/8`, `Types.set_integer/4` and `Row.put_integer/5` raise
`ArgumentError` for values outside that range instead of truncating.
`Types.read_cells/6` returns every requested `"family:qualifier"` key (with `nil`
for a missing row) and `{:error, {:decode, "f:q", reason}}` for a cell that
fails to decode.

### Cell timestamps

BigTable tables store cell timestamps at millisecond granularity.
`MegasPinakas.set_cell/4` (and every builder on top of it) accepts
`timestamp_micros: -1` (default, server-assigned) or a non-negative multiple of
`1_000`; anything else raises `ArgumentError` before the request is built.

```elixir
MegasPinakas.set_cell("cf", "col", "value", timestamp_micros: 1_234_567_890_000)
```

## Row Builder

Fluent API for building multi-cell rows:

```elixir
alias MegasPinakas.Row

Row.new("user#123")
|> Row.put_string("cf", "name", "John Doe")
|> Row.put_integer("cf", "age", 30)
|> Row.put_json("cf", "profile", %{city: "NYC"})
|> Row.put_boolean("cf", "active", true)
|> Row.put_datetime("cf", "created", DateTime.utc_now())
|> Row.write(project, instance, "users")

# Type inference with put/5
Row.new("user#123")
|> Row.put("cf", "name", "John")      # Infers string
|> Row.put("cf", "age", 30)           # Infers integer
|> Row.put("cf", "score", 98.5)       # Infers float
|> Row.put("cf", "data", %{a: 1})     # Infers JSON
|> Row.write(project, instance, "users")

# Atoms, tuples and nil cannot be inferred: put/5 raises ArgumentError.
# Use put_term/5 for arbitrary Erlang terms.
Row.new("job#1") |> Row.put_term("cf", "state", {:running, 3})
```

## Batch Builder

Build and execute batch mutations:

```elixir
alias MegasPinakas.Batch
alias MegasPinakas.Row

Batch.new()
|> Batch.add(Row.new("user#1") |> Row.put_string("cf", "name", "Alice"))
|> Batch.add(Row.new("user#2") |> Row.put_string("cf", "name", "Bob"))
|> Batch.add(Row.new("user#3") |> Row.put_string("cf", "name", "Charlie"))
|> Batch.write(project, instance, "users")
```

BigTable caps a `MutateRows` request at 100,000 mutations. `Batch.write/5`
splits a larger batch into sequential requests, re-bases each result's `index`
to its position in the batch, and returns the first failing chunk's error
(earlier chunks were already applied, later ones not sent). The 256 MiB byte cap
is not enforced client-side. An empty batch returns `{:ok, []}` without an RPC.

## Advanced Filters

The `MegasPinakas.Filter` module provides comprehensive filter support:

```elixir
alias MegasPinakas.Filter

# Row-level filters (RE2, whole-string match; see "Regex filters" above)
Filter.row_key_prefix_filter("user#")
Filter.row_key_regex_filter("user#\\d+")
Filter.row_sample_filter(0.1)  # 10% sample

# Cell-level filters
Filter.cells_per_row_limit_filter(100)
Filter.cells_per_row_offset_filter(10)
Filter.column_qualifier_regex_filter("meta_\\C*")

# Range filters. Timestamps: start inclusive, end exclusive, 0 = unbounded.
# Giving both the _closed and _open bound for one side raises ArgumentError.
Filter.timestamp_range_filter(start_micros, end_micros)
Filter.value_range_filter(start_value_closed: "A", end_value_closed: "Z")
Filter.column_range_filter("cf", start_qualifier_closed: "a", end_qualifier_open: "m")

# Convenience filters
Filter.latest_only_filter()                    # Only latest version
Filter.time_window_filter(:hour, 24)           # Last 24 hours
Filter.column_latest_filter("cf", "name")      # Specific column, latest

# Composing filters
Filter.chain_filters([f1, f2, f3])              # AND
Filter.interleave_filters([f1, f2])             # OR
Filter.condition_filter(predicate, true_f, false_f)  # IF-THEN-ELSE
```

## Counters

Atomic counter operations. Counter cells are signed 64-bit integers; configure
`Admin.max_versions_gc_rule(1)` on the counter family so old versions are
reclaimed. Reads fetch only the latest cell version.

```elixir
alias MegasPinakas.Counter

# Basic increment/decrement
{:ok, new_value} = Counter.increment(project, instance, "counters", "page#home", "stats", "views")
{:ok, new_value} = Counter.decrement(project, instance, "counters", "item#123", "stock", "count", 5)

# Get current value
{:ok, value} = Counter.get(project, instance, "counters", "page#home", "stats", "views")

# Set/reset
Counter.set(project, instance, "counters", "page#home", "stats", "views", 100)
Counter.reset(project, instance, "counters", "page#home", "stats", "views")

# Atomic multi-counter increment
{:ok, results} = Counter.increment_many(project, instance, "analytics", "user#123", [
  {"stats", "page_views", 1},
  {"stats", "clicks", 3}
])

# Compare-and-swap increment that never creates the counter. Reads the current
# value, then check_and_mutate_row on the exact bytes; retried up to 5 times.
case Counter.increment_if_exists(project, instance, "counters", "page#home", "stats", "views", 1) do
  {:ok, :applied} -> :incremented
  {:ok, :not_applied} -> :counter_missing
  {:error, :contention} -> :lost_every_race
end
```

## Time-Windowed Counters (Rate Limiting)

```elixir
alias MegasPinakas.CounterTTL

# Increment with time bucket
{:ok, count} = CounterTTL.increment(project, instance, "rate_limits", "api:user#123",
  "limits", "requests", bucket: :minute)

# Check rate limit
case CounterTTL.check_rate_limit(project, instance, "rate_limits", "api:user#123", 100, bucket: :minute) do
  {:ok, current_count} -> IO.puts("Under limit: #{current_count}")
  {:error, :rate_limited, reset_at} -> IO.puts("Rate limited until #{reset_at}")
end

# Get window sum (window_size must be >= 1; 0 raises ArgumentError)
{:ok, total} = CounterTTL.get_window(project, instance, "rate_limits", "api:user#123",
  "limits", "requests", bucket: :minute, window_size: 5)
```

`check_rate_limit/6` and `increment_with_limit/6` also return `{:error, term()}`
for transport or auth failures; a `limit <= 0` always rate-limits without
writing. As with `Counter`, configure `Admin.max_versions_gc_rule(1)` (and a
`max_age_gc_rule/1` covering your window) on the counter family.

## Time Series

Time-series data with reverse timestamp ordering for efficient recent-first queries.

### Reverse Timestamp Ordering

BigTable sorts row keys lexicographically. To get recent data first (without scanning the entire table),
we use **reverse timestamps**: `max_timestamp - actual_timestamp`.

```
Normal timestamp (µs):  2024-01-01 → 1704067200000000  (sorts first, oldest)
                        2024-12-31 → 1735689599000000  (sorts last, newest)

Reverse timestamp:      2024-01-01 → "0008295932799999999"  (sorts last)
                        2024-12-31 → "0008264310400999999"  (sorts first, newest!)
```

Row key format: `<metric_id>#<reverse_timestamp>`

This means a prefix scan like `row_range_prefix("cpu:server1#")` returns the most recent data first.

```elixir
alias MegasPinakas.TimeSeries

# Build reverse timestamp row keys manually
row_key = TimeSeries.time_series_row_key("cpu:server1", ~U[2024-01-15 10:00:00Z])
# => "cpu:server1#0008294687199999999"

# Convert timestamps
reverse_ts = TimeSeries.reverse_timestamp(~U[2024-01-15 10:00:00Z])
{:ok, original_dt} = TimeSeries.from_reverse_timestamp(reverse_ts)

# Parse row keys
{:ok, %{metric_id: id, timestamp: ts}} = TimeSeries.parse_row_key(row_key)
```

### Writing Data Points

Each point stores `:value` in the `value` column and its type in a sibling
`value_type` column (`"i"` integer, `"f"` float, `"s"` string, `"j"` JSON for
maps, lists and booleans). Queries decode by that tag, so an integer written
comes back as an integer. A point with no tag (written before 0.7.0) is
returned with its raw binary `:value`.

```elixir
# Write a data point
{:ok, _} = TimeSeries.write_point(project, instance, "metrics", "cpu:server1",
  %{value: 0.85, tags: %{host: "srv1", region: "us-east"}})

# A missing or nil :value is rejected before any request is sent
{:error, :nil_value} = TimeSeries.write_point(project, instance, "metrics", "cpu:server1", %{})
{:error, {:unsupported_value, _}} = TimeSeries.write_point(project, instance, "metrics", "cpu:server1", %{value: :atom})

# Write multiple points (partial-success: check &1.status.code per entry).
# One bad value rejects the whole batch.
{:ok, results} = TimeSeries.write_points(project, instance, "metrics", [
  %{metric_id: "cpu:server1", value: 0.85, timestamp: ~U[2024-01-15 10:00:00Z]},
  %{metric_id: "cpu:server2", value: 0.92, timestamp: ~U[2024-01-15 10:00:00Z]}
])
```

### Querying Data

Points come back as `%{row_key, timestamp, value, tags}`, most recent first.

```elixir
# Query recent points (most recent first due to reverse timestamps)
{:ok, points} = TimeSeries.query_recent(project, instance, "metrics", "cpu:server1", limit: 100)

# Query the half-open range [start_time, end_time): a point stamped exactly
# start_time is included, one stamped exactly end_time is not, so adjacent
# ranges partition a series without overlap. :limit caps the result. An empty
# range returns {:ok, []} without a request; start > end raises ArgumentError.
{:ok, points} = TimeSeries.query_range(project, instance, "metrics", "cpu:server1",
  ~U[2024-01-01 00:00:00Z], ~U[2024-01-02 00:00:00Z], limit: 1_000)
```

## Eager reads vs streaming

`read_rows/4` is **eager**: it assembles every matching row into a list before
returning. That is the right shape for a bounded read — a key set, a narrow
range, anything with a `:rows_limit`.

It is the wrong shape for a large scan. `read_rows(p, i, "big_table")` with no
`:rows` and no `:rows_limit` materializes the whole table (~3.4 KB per row in our
benchmark fixture, so 1 M rows is several GB). Two ways to avoid that:

```elixir
# 1. Cap it — turns an accidental full-table read into an error, not an OOM.
#    Costs one row beyond the cap, not a full scan.
case MegasPinakas.read_rows(project, instance, "events", max_rows: 100_000) do
  {:ok, rows} -> rows
  {:error, :result_too_large} -> :too_big
end

# 2. Stream it — memory bounded by :batch_size, not by result size.
MegasPinakas.Streaming.stream_prefix(project, instance, "events", "2026-08-")
|> Stream.map(&MegasPinakas.row_to_map/1)
|> Enum.reduce(0, fn _row, n -> n + 1 end)
```

The same applies to helpers built on `read_rows/4` — notably `Cache.get_many/5`
and `CounterTTL.get_window/7`, which are bounded by the keys or window you pass.

## Streaming

Memory-efficient streaming with `Stream.resource`.

Rows are fetched in batches as you consume them, so memory is bounded by
`:batch_size` (default `10_000`) rather than by the size of the result set.
`:batch_size` trades round trips against peak memory: each batch is fetched in
full before its rows are yielded. Lower it when rows are large or you expect to
stop early; raise it for long scans.

Streams emit `[:megas_pinakas, :stream, :start]` on first demand, then exactly
one of `:stop` (ran to exhaustion) or `:cancelled` (consumer stopped early, or
the stream failed). Every event carries `:project`, `:instance`, `:table`,
`:batch_size` and a `:stream_ref` unique to the stream, so events can be joined.

A batch whose RPC fails with `:unavailable`, `:deadline_exceeded` or `:aborted`
is retried up to `:max_retries` times (default 3) with exponential backoff
(100 ms doubling, capped at 2 s); each retry emits
`[:megas_pinakas, :stream, :retry]` with `%{attempt: n}` and `:reason`. A
permanent failure emits `[:megas_pinakas, :stream, :exception]` (with `:reason`)
and then raises `MegasPinakas.StreamError` rather than halting quietly, so
partial results are never mistaken for a complete read. `:exception` is always
followed by `:cancelled` for the same `:stream_ref`. `StreamError.last_key` is
the last row delivered to the consumer; resume strictly after it.

Invalid `:batch_size`, `:rows_limit`, `:max_retries` or `:rows` raise
`ArgumentError` when the stream is built, before any request is issued.

```elixir
alias MegasPinakas.Streaming

# Stream all rows with a prefix
Streaming.stream_prefix(project, instance, "users", "user#active:")
|> Stream.map(&MegasPinakas.row_to_map/1)
|> Stream.filter(fn data -> data["cf"]["status"] == "active" end)
|> Enum.take(100)

# Stream a range
Streaming.stream_range(project, instance, "table", "a", "z")
|> Stream.each(&process_row/1)
|> Stream.run()

# Stream with keys
Streaming.stream_rows_with_keys(project, instance, "users",
  rows: MegasPinakas.row_set_from_ranges([MegasPinakas.row_range_prefix("user#")])
)
|> Enum.into(%{})  # Map of row_key => data

# Chunked processing
Streaming.stream_in_chunks(project, instance, "logs",
  [rows: row_set],
  chunk_size: 100,
  process_fn: fn chunk -> process_batch(chunk) end
)
|> Enum.sum()

# Cap the total rows a stream will yield
Streaming.stream_prefix(project, instance, "users", "user#", rows_limit: 500)
|> Enum.to_list()

# Trade round trips against peak memory
Streaming.stream_prefix(project, instance, "wide_rows", "k#", batch_size: 100)
|> Enum.each(&process_row/1)

# Give a flaky link more attempts per batch
Streaming.stream_prefix(project, instance, "events", "2026-", max_retries: 5)
|> Stream.run()

# Utilities. count_rows strips values and reads one cell per row when no :filter
# is given; rows_exist?/first_row fetch exactly one row in one RPC.
count = Streaming.count_rows(project, instance, "users", rows: row_set)
exists? = Streaming.rows_exist?(project, instance, "users", rows: row_set)
{:ok, first} = Streaming.first_row(project, instance, "users", rows: row_set)
```

## Cache

Key-value cache backed by BigTable. Any Elixir term can be cached (maps, lists,
strings, integers, `nil`): each entry is a single cell holding an Erlang-term
envelope of the value and its optional expiry, so values roundtrip exactly (atom
keys stay atoms). Cells written by other means (`Types.write_json/8`, 0.6.x
`Cache`) are not readable through this module.

```elixir
alias MegasPinakas.Cache

# Basic operations
{:ok, _} = Cache.put(project, instance, "cache", "user:123", %{name: "John", age: 30})
{:ok, %{name: "John", age: 30}} = Cache.get(project, instance, "cache", "user:123")
{:ok, _} = Cache.delete(project, instance, "cache", "user:123")

# Expire after five minutes. :ttl is seconds; an invalid value raises ArgumentError.
{:ok, _} = Cache.put(project, instance, "cache", "session:abc", token, ttl: 300)
{:ok, nil} = Cache.get(project, instance, "cache", "session:abc")   # after 300 s

# Get or compute (also accepts :ttl)
{:ok, value} = Cache.get_or_put(project, instance, "cache", "expensive:key", fn ->
  expensive_computation()
end, ttl: 3_600)

# Multi-key operations
{:ok, results} = Cache.get_many(project, instance, "cache", ["key1", "key2", "key3"])
{:ok, _} = Cache.put_many(project, instance, "cache", [{"key1", val1}, {"key2", val2}], ttl: 60)
{:ok, _} = Cache.delete_many(project, instance, "cache", ["key1", "key2"])

# Existence check. Transport errors are reported, not collapsed into false.
{:ok, true} = Cache.exists?(project, instance, "cache", "user:123")
```

Expiry is enforced **client-side**: `get`, `get_many`, `exists?` and `get_or_put`
treat an entry whose TTL has passed as absent (a stored `nil` is a hit for
`get_or_put`, so negative results can be cached), but the cell stays in the table
until overwritten or garbage-collected. Configure
`Admin.max_age_gc_rule/1` on the cache family, at least as long as your longest
TTL, to reclaim storage; the GC rule is not what makes an entry expire.

For atomic numeric counters use `MegasPinakas.Counter`; for appends use
`MegasPinakas.read_modify_write_row/6` with `MegasPinakas.append_rule/3`.

## Errors

`MegasPinakas.Client.execute/2` never raises for a failure inside the operation.
Every high-level function returns one of:

| Shape | Meaning |
| --- | --- |
| `{:error, {status_atom, message}}` | The RPC ran and the server rejected it (`:not_found`, `:unavailable`, `:permission_denied`, ...) |
| `{:error, {:auth_error, reason}}` | No access token could be obtained; nothing was sent |
| `{:error, {:pool_error, reason}}` | No connection could be checked out of the pool |
| `{:error, {:execution_error, message}}` | The operation raised; `message` is `Exception.message/1` |
| `{:error, {:execution_error, {:exit \| :throw, term}}}` | The operation exited or threw |
| `{:error, {:incomplete_read, {status_atom, message}}}` | A streaming RPC (`read_rows/4`, `sample_row_keys/4`, `mutate_rows/5`) failed mid-stream |
| `{:error, :result_too_large}` | `read_rows/4` exceeded `:max_rows` |

`MegasPinakas.Response.normalize_reason/1` turns a bare `%GRPC.RPCError{}` into
`{status_atom, message}`. `Client.execute!/2` raises `MegasPinakas.Error` (with a
`reason` field) for any of the above.

Argument errors (wrong timestamp granularity, negative limits, malformed
`mutate_rows/5` entries, both bounds on one side of a range filter, missing
`:serve_nodes`, ...) raise `ArgumentError` before any request is built.

## Telemetry

Every `Client.execute/2` call emits `[:megas_pinakas, :request, :start]` and then
exactly one of:

- `[:megas_pinakas, :request, :stop]` - measurements `%{duration: native}`,
  metadata `%{pool: atom, result: :ok | {:error, tag}}` where `tag` is the gRPC
  status atom, `:auth_error`, `:pool_error`, or the first element of a
  client-side error tuple. Emitted whenever the request completed, even if the
  server answered with an error.
- `[:megas_pinakas, :request, :exception]` - measurements `%{duration: native}`,
  metadata `%{pool: atom, kind: :error | :exit | :throw, reason: term, stacktrace: list}`.
  `reason` is the raw exception struct (or exit/throw value). Emitted when the
  operation itself blew up.

`MegasPinakas.Streaming` emits its own `[:megas_pinakas, :stream, :*]` events
(`:start`, `:stop`, `:cancelled`, `:retry`, `:exception`); see [Streaming](#streaming).

## Testing

Pure builders run without any backend. Everything that talks to BigTable is
tagged `:emulator` and excluded by default:

```bash
docker compose up -d bigtable-emulator
mix test --include emulator
```

`MegasPinakas.InstanceAdmin.partial_update_cluster/4` is never exercised against
the emulator, which crashes on `PartialUpdateCluster`.

## License

Apache 2.0
