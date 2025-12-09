# MegasPinakas

An Elixir client library for Google Cloud BigTable, providing a high-level interface for data operations, table administration, and instance management via gRPC.

## Installation

Add `megas_pinakas` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:megas_pinakas, "~> 0.1.0"}
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
    {:megas_pinakas, "~> 0.1.0"},
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

# Row range
range = MegasPinakas.row_range("user#100", "user#200")
row_set = MegasPinakas.row_set_from_ranges([range])

# Prefix scan
range = MegasPinakas.row_range_prefix("user#123#")
```

## License

Apache 2.0
