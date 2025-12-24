# MegasPinakas

An Elixir client library for Google Cloud BigTable, providing a high-level interface for data operations, table administration, and instance management via gRPC.

## Installation

Add `megas_pinakas` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:megas_pinakas, "~> 0.5.0"}
  ]
end
```

## Configuration

### Development with Emulator

For local development, use the BigTable emulator:

```bash
# Start the emulator
docker-compose up bigtable-emulator
```

Configure your application:

```elixir
# config/dev.exs
config :megas_pinakas, :emulator,
  host: "localhost",
  port: 8086,
  project_id: "dev-project"
```

Or set the environment variable:

```bash
export BIGTABLE_EMULATOR_HOST=localhost:8086
```

### Production with Goth Authentication

For production, use [Goth](https://github.com/peburrows/goth) for Google Cloud authentication:

```elixir
# Add to dependencies in mix.exs
def deps do
  [
    {:megas_pinakas, "~> 0.5.0"},
    {:goth, "~> 1.4"}
  ]
end
```

Configure Goth with your service account credentials:

```elixir
# config/runtime.exs
if config_env() == :prod do
  # Option 1: From environment variable (JSON string)
  credentials = System.get_env("GOOGLE_APPLICATION_CREDENTIALS_JSON") |> Jason.decode!()

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
{:ok, stream} = MegasPinakas.read_rows("project", "instance", "users",
  rows: MegasPinakas.row_set(["user#1", "user#2", "user#3"]),
  filter: filter)

# Batch mutations
entries = [
  %{row_key: "row1", mutations: [MegasPinakas.set_cell("cf", "col", "val1")]},
  %{row_key: "row2", mutations: [MegasPinakas.set_cell("cf", "col", "val2")]}
]
{:ok, stream} = MegasPinakas.mutate_rows("project", "instance", "table", entries)

# Atomic increment
rules = [MegasPinakas.increment_rule("cf", "counter", 1)]
{:ok, _} = MegasPinakas.read_modify_write_row("project", "instance", "table", "row", rules)
```

### Table Administration

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

# Delete a table
{:ok, _} = Admin.delete_table("project", "instance", "my-table")
```

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

# List instances
{:ok, response} = InstanceAdmin.list_instances("project")

# Create an app profile
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
# Internally creates range: ["user#", "user$") where $ is next char after #

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

## Advanced Filters

The `MegasPinakas.Filter` module provides comprehensive filter support:

```elixir
alias MegasPinakas.Filter

# Row-level filters
Filter.row_key_regex_filter("^user#")
Filter.row_sample_filter(0.1)  # 10% sample

# Cell-level filters
Filter.cells_per_row_limit_filter(100)
Filter.cells_per_row_offset_filter(10)
Filter.column_qualifier_regex_filter("^meta_")

# Range filters
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

Atomic counter operations:

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

# Get window sum
{:ok, total} = CounterTTL.get_window(project, instance, "rate_limits", "api:user#123",
  "limits", "requests", bucket: :minute, window_size: 5)
```

## Time Series

Time-series data with reverse timestamp ordering for efficient recent-first queries.

### Reverse Timestamp Ordering

BigTable sorts row keys lexicographically. To get recent data first (without scanning the entire table),
we use **reverse timestamps**: `max_timestamp - actual_timestamp`.

```
Normal timestamp:    2024-01-01 → "1704067200"  (sorts first, oldest)
                     2024-12-31 → "1735689599"  (sorts last, newest)

Reverse timestamp:   2024-01-01 → "8295932799"  (sorts last)
                     2024-12-31 → "8264310400"  (sorts first, newest!)
```

Row key format: `<metric_id>#<reverse_timestamp>`

This means a prefix scan like `row_range_prefix("cpu:server1#")` returns the most recent data first.

```elixir
alias MegasPinakas.TimeSeries

# Build reverse timestamp row keys manually
row_key = TimeSeries.time_series_row_key("cpu:server1", ~U[2024-01-15 10:00:00Z])
# => "cpu:server1#8296..."

# Convert timestamps
reverse_ts = TimeSeries.reverse_timestamp(~U[2024-01-15 10:00:00Z])
{:ok, original_dt} = TimeSeries.from_reverse_timestamp(reverse_ts)

# Parse row keys
{:ok, %{metric_id: id, timestamp: ts}} = TimeSeries.parse_row_key(row_key)
```

### Writing Data Points

```elixir
# Write a data point
TimeSeries.write_point(project, instance, "metrics", "cpu:server1",
  %{value: 0.85, tags: %{host: "srv1", region: "us-east"}})

# Write multiple points
TimeSeries.write_points(project, instance, "metrics", [
  %{metric_id: "cpu:server1", value: 0.85, timestamp: ~U[2024-01-15 10:00:00Z]},
  %{metric_id: "cpu:server2", value: 0.92, timestamp: ~U[2024-01-15 10:00:00Z]}
])
```

### Querying Data

```elixir
# Query recent points (most recent first due to reverse timestamps)
{:ok, points} = TimeSeries.query_recent(project, instance, "metrics", "cpu:server1", limit: 100)

# Query time range
{:ok, points} = TimeSeries.query_range(project, instance, "metrics", "cpu:server1",
  ~U[2024-01-01 00:00:00Z], ~U[2024-01-02 00:00:00Z])
```

## Streaming

Memory-efficient streaming with `Stream.resource`:

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

# Utilities
count = Streaming.count_rows(project, instance, "users", rows: row_set)
exists? = Streaming.rows_exist?(project, instance, "users", rows: row_set)
{:ok, first} = Streaming.first_row(project, instance, "users", rows: row_set)
```

## Cache

Simple key-value cache:

```elixir
alias MegasPinakas.Cache

# Basic operations
{:ok, _} = Cache.put(project, instance, "cache", "user:123", %{name: "John", age: 30})
{:ok, data} = Cache.get(project, instance, "cache", "user:123")
{:ok, _} = Cache.delete(project, instance, "cache", "user:123")

# Get or compute
{:ok, value} = Cache.get_or_put(project, instance, "cache", "expensive:key", fn ->
  expensive_computation()
end)

# Multi-key operations
{:ok, results} = Cache.get_many(project, instance, "cache", ["key1", "key2", "key3"])
{:ok, _} = Cache.put_many(project, instance, "cache", [{"key1", val1}, {"key2", val2}])
{:ok, _} = Cache.delete_many(project, instance, "cache", ["key1", "key2"])

# Existence check
exists? = Cache.exists?(project, instance, "cache", "user:123")

# Atomic operations
{:ok, new_val} = Cache.increment(project, instance, "cache", "counter", 1)
{:ok, new_val} = Cache.append(project, instance, "cache", "log", "new entry\n")
```

## License

Apache 2.0
