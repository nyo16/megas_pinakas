# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is an Elixir BigTable client library named `MegasPinakas` that provides a high-level interface for interacting with Google Cloud BigTable. The project is built as an OTP application with GRPC-based communication to BigTable services, utilizing connection pooling for optimal performance.

## Common Development Commands

### Build and Dependencies
```bash
mix deps.get          # Install dependencies
mix compile           # Compile the project
mix deps.update --all # Update all dependencies
```

### Testing
```bash
mix test              # Run all tests
mix test test/path/to/specific_test.exs  # Run a specific test file
mix test --failed     # Re-run only failed tests
```

### Code Quality
```bash
mix format            # Format code according to .formatter.exs
mix format --check-formatted  # Check if code is properly formatted
```

### Interactive Development
```bash
iex -S mix            # Start IEx with project loaded
```

### BigTable Development
```bash
# For local development with BigTable emulator
docker-compose up bigtable-emulator  # Start BigTable emulator
export BIGTABLE_EMULATOR_HOST=localhost:8086  # Connect to emulator

# For production BigTable access
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"
# Or use: gcloud auth application-default login
```

## Architecture

### Module Structure
```
lib/
├── megas_pinakas.ex                    # Data API (read/write rows, mutations, filters)
└── megas_pinakas/
    ├── application.ex                  # OTP Application, starts connection pool
    ├── client.ex                       # Low-level connection pool wrapper
    ├── auth.ex                         # Authentication (emulator detection, Goth, gcloud)
    ├── config.ex                       # Configuration and resource path helpers
    ├── admin.ex                        # Table Admin API (tables, column families, backups)
    └── instance_admin.ex               # Instance Admin API (instances, clusters, app profiles)
```

### Key Modules

- **MegasPinakas** - Data operations: `read_rows/4`, `read_row/5`, `mutate_row/6`, `mutate_rows/5`, `check_and_mutate_row/8`, `read_modify_write_row/6`. Also provides mutation builders (`set_cell/4`, `delete_from_column/3`), filter builders (`column_filter/2`, `family_filter/1`), and row set builders.

- **MegasPinakas.Admin** - Table admin: `create_table/4`, `list_tables/3`, `get_table/4`, `delete_table/3`, `modify_column_families/4`, `drop_row_range/4`. Backup operations: `create_backup/6`, `get_backup/4`, `list_backups/4`, `delete_backup/4`, `restore_table/5`. GC rule builders: `max_versions_gc_rule/1`, `max_age_gc_rule/1`.

- **MegasPinakas.InstanceAdmin** - Instance operations: `create_instance/4`, `get_instance/2`, `list_instances/2`, `delete_instance/2`. Cluster operations: `create_cluster/5`, `get_cluster/3`, `list_clusters/3`, `update_cluster/4`, `delete_cluster/3`. App profile operations: `create_app_profile/4`, `get_app_profile/3`, `list_app_profiles/3`, `update_app_profile/4`, `delete_app_profile/4`.

- **MegasPinakas.Client** - Low-level pool wrapper: `execute/2` runs operations with connection from pool.

- **MegasPinakas.Config** - Resource path builders: `table_path/3`, `instance_path/2`, `cluster_path/3`, `backup_path/4`, `app_profile_path/3`. Environment detection: `emulator?/0`, `emulator_endpoint/0`.

- **MegasPinakas.Auth** - Authentication: `request_opts/0` returns GRPC metadata with auth token. Supports emulator (no auth), Goth library, and gcloud CLI fallback.

### Key Dependencies
- **grpc_connection_pool** (~> 0.2.1) - GRPC connection pooling with health monitoring
- **googleapis_proto_ex** (~> 0.3.3) - Pre-compiled BigTable protobuf definitions

### Service Stubs Used
- `Google.Bigtable.V2.Bigtable.Stub` - Data API
- `Google.Bigtable.Admin.V2.BigtableTableAdmin.Stub` - Table Admin API
- `Google.Bigtable.Admin.V2.BigtableInstanceAdmin.Stub` - Instance Admin API

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
config :megas_pinakas, :default_pool_size, 10
# No :emulator config = uses production bigtable.googleapis.com
```

### Runtime Override
Set `BIGTABLE_EMULATOR_HOST=host:port` to override production to use emulator.

## API Pattern

All operations follow this pattern:
```elixir
operation = fn channel ->
  request = %Google.Bigtable.V2.SomeRequest{...}
  auth_opts = Auth.request_opts()
  Google.Bigtable.V2.Bigtable.Stub.some_rpc(channel, request, auth_opts)
end
Client.execute(operation)
```

## Docker Development Environment

The project includes `docker-compose.yml` with BigTable emulator:
- Emulator runs on port 8086
- Data is in-memory only (not persisted)
- No authentication required

```bash
docker-compose up bigtable-emulator
```
