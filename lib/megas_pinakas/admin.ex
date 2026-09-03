defmodule MegasPinakas.Admin do
  @moduledoc """
  Table administration operations for BigTable.

  This module provides functions for creating, modifying, and deleting tables,
  as well as managing column families and backups, and for following the
  long-running operations that slow admin calls return.

  Admin RPCs are served by `bigtableadmin.googleapis.com`, not the Data API host,
  so every function here runs on `MegasPinakas.Client.admin_pool/0` (in emulator
  mode both pools point at the emulator).

  ## Long-running operations

  `create_backup/6` and `restore_table/5` (and the instance/cluster functions in
  `MegasPinakas.InstanceAdmin`) return a `Google.Longrunning.Operation` rather
  than the finished resource. Poll it with `wait_operation/2`, which resolves the
  operation into the decoded resource:

      {:ok, operation} = MegasPinakas.Admin.create_backup(
        "project", "instance", "cluster", "my-backup", "my-table", expire_time: ts)
      {:ok, %Google.Bigtable.Admin.V2.Backup{}} = MegasPinakas.Admin.wait_operation(operation)
  """

  alias MegasPinakas.{Auth, Client, Config, Response}
  alias MegasPinakas.Longrunning.Operations

  alias Google.Longrunning.{GetOperationRequest, Operation}

  alias Google.Bigtable.Admin.V2.{
    AppProfile,
    Backup,
    BigtableTableAdmin.Stub,
    Cluster,
    ColumnFamily,
    CreateBackupRequest,
    CreateTableRequest,
    DeleteBackupRequest,
    DeleteTableRequest,
    DropRowRangeRequest,
    GcRule,
    GetBackupRequest,
    GetTableRequest,
    Instance,
    ListBackupsRequest,
    ListBackupsResponse,
    ListTablesRequest,
    ListTablesResponse,
    ModifyColumnFamiliesRequest,
    RestoreTableRequest,
    Table
  }

  # Resource types an admin LRO's `response` Any may carry. Anything else is
  # handed back as the raw Operation so callers can decode it themselves.
  @operation_response_types %{
    "google.bigtable.admin.v2.AppProfile" => AppProfile,
    "google.bigtable.admin.v2.Backup" => Backup,
    "google.bigtable.admin.v2.Cluster" => Cluster,
    "google.bigtable.admin.v2.Instance" => Instance,
    "google.bigtable.admin.v2.Table" => Table,
    "google.protobuf.Empty" => Google.Protobuf.Empty
  }

  @default_poll_interval 1_000
  @default_wait_timeout 300_000

  # ============================================================================
  # Table Operations
  # ============================================================================

  @doc """
  Creates a new BigTable table.

  ## Options

    * `:column_families` - Map of column family name to a config map. The config
      may contain `:gc_rule` (or `"gc_rule"`); an empty map means no GC rule.
    * `:initial_splits` - List of row keys to use for initial table splits

  Raises `ArgumentError` when `:column_families` is not a map of maps or
  `:initial_splits` is not a list of binaries.

  ## Examples

      # Create a simple table with one column family
      {:ok, table} = MegasPinakas.Admin.create_table("project", "instance", "my-table",
        column_families: %{"cf" => %{}})

      # Create with GC rules
      {:ok, table} = MegasPinakas.Admin.create_table("project", "instance", "my-table",
        column_families: %{
          "cf" => %{gc_rule: MegasPinakas.Admin.max_versions_gc_rule(1)}
        })
  """
  @spec create_table(String.t(), String.t(), String.t(), keyword()) ::
          {:ok, Table.t()} | {:error, term()}
  def create_table(project_id, instance_id, table_id, opts \\ []) do
    column_families = build_column_families(Keyword.get(opts, :column_families, %{}))
    initial_splits = build_initial_splits(Keyword.get(opts, :initial_splits, []))

    operation = fn channel ->
      request = %CreateTableRequest{
        parent: Config.instance_path(project_id, instance_id),
        table_id: table_id,
        table: %Table{column_families: column_families},
        initial_splits: initial_splits
      }

      auth_opts = Auth.request_opts()
      Stub.create_table(channel, request, auth_opts)
    end

    Client.execute(operation, pool: Client.admin_pool())
  end

  @doc """
  Lists tables in a BigTable instance.

  ## Options

    * `:view` - Table view (`:NAME_ONLY`, `:SCHEMA_VIEW`, `:REPLICATION_VIEW`, `:ENCRYPTION_VIEW`, `:FULL`)
    * `:page_size` - Maximum number of tables to return
    * `:page_token` - Page token for pagination

  ## Examples

      {:ok, response} = MegasPinakas.Admin.list_tables("project", "instance")
  """
  @spec list_tables(String.t(), String.t(), keyword()) ::
          {:ok, ListTablesResponse.t()} | {:error, term()}
  def list_tables(project_id, instance_id, opts \\ []) do
    operation = fn channel ->
      request = %ListTablesRequest{
        parent: Config.instance_path(project_id, instance_id),
        view: Keyword.get(opts, :view, :NAME_ONLY),
        page_size: Keyword.get(opts, :page_size, 0),
        page_token: Keyword.get(opts, :page_token, "")
      }

      auth_opts = Auth.request_opts()
      Stub.list_tables(channel, request, auth_opts)
    end

    Client.execute(operation, pool: Client.admin_pool())
  end

  @doc """
  Gets details about a BigTable table.

  ## Options

    * `:view` - Table view (`:NAME_ONLY`, `:SCHEMA_VIEW`, `:REPLICATION_VIEW`, `:ENCRYPTION_VIEW`, `:FULL`)

  ## Examples

      {:ok, table} = MegasPinakas.Admin.get_table("project", "instance", "my-table")
  """
  @spec get_table(String.t(), String.t(), String.t(), keyword()) ::
          {:ok, Table.t()} | {:error, term()}
  def get_table(project_id, instance_id, table_id, opts \\ []) do
    operation = fn channel ->
      request = %GetTableRequest{
        name: Config.table_path(project_id, instance_id, table_id),
        view: Keyword.get(opts, :view, :SCHEMA_VIEW)
      }

      auth_opts = Auth.request_opts()
      Stub.get_table(channel, request, auth_opts)
    end

    Client.execute(operation, pool: Client.admin_pool())
  end

  @doc """
  Deletes a BigTable table.

  ## Examples

      {:ok, _} = MegasPinakas.Admin.delete_table("project", "instance", "my-table")
  """
  @spec delete_table(String.t(), String.t(), String.t()) ::
          {:ok, Google.Protobuf.Empty.t()} | {:error, term()}
  def delete_table(project_id, instance_id, table_id) do
    operation = fn channel ->
      request = %DeleteTableRequest{
        name: Config.table_path(project_id, instance_id, table_id)
      }

      auth_opts = Auth.request_opts()
      Stub.delete_table(channel, request, auth_opts)
    end

    Client.execute(operation, pool: Client.admin_pool())
  end

  @doc """
  Modifies column families in a table.

  ## Examples

      # Add a column family
      modifications = [
        MegasPinakas.Admin.create_column_family("new_cf", max_versions_gc_rule(1))
      ]
      {:ok, table} = MegasPinakas.Admin.modify_column_families("project", "instance", "table", modifications)

      # Update a column family
      modifications = [
        MegasPinakas.Admin.update_column_family("cf", max_age_gc_rule(86400))
      ]
      {:ok, table} = MegasPinakas.Admin.modify_column_families("project", "instance", "table", modifications)

      # Drop a column family
      modifications = [
        MegasPinakas.Admin.drop_column_family("old_cf")
      ]
      {:ok, table} = MegasPinakas.Admin.modify_column_families("project", "instance", "table", modifications)
  """
  @spec modify_column_families(
          String.t(),
          String.t(),
          String.t(),
          [ModifyColumnFamiliesRequest.Modification.t()]
        ) :: {:ok, Table.t()} | {:error, term()}
  def modify_column_families(project_id, instance_id, table_id, modifications) do
    operation = fn channel ->
      request = %ModifyColumnFamiliesRequest{
        name: Config.table_path(project_id, instance_id, table_id),
        modifications: modifications
      }

      auth_opts = Auth.request_opts()
      Stub.modify_column_families(channel, request, auth_opts)
    end

    Client.execute(operation, pool: Client.admin_pool())
  end

  @doc """
  Drops a range of rows from a table.

  Exactly one target must be given. With neither option the call returns
  `{:error, :no_target}` without touching the network; with both it raises
  `ArgumentError`.

  ## Options

    * `:row_key_prefix` - Delete all rows with this prefix
    * `:delete_all_data_from_table` - Delete all data (use with caution!)

  ## Examples

      # Delete rows with prefix
      {:ok, _} = MegasPinakas.Admin.drop_row_range("project", "instance", "table",
        row_key_prefix: "user#123#")

      # Delete all data
      {:ok, _} = MegasPinakas.Admin.drop_row_range("project", "instance", "table",
        delete_all_data_from_table: true)
  """
  @spec drop_row_range(String.t(), String.t(), String.t(), keyword()) ::
          {:ok, Google.Protobuf.Empty.t()} | {:error, :no_target | term()}
  def drop_row_range(project_id, instance_id, table_id, opts \\ []) do
    case drop_row_range_target(opts) do
      nil ->
        {:error, :no_target}

      target ->
        operation = fn channel ->
          request = %DropRowRangeRequest{
            name: Config.table_path(project_id, instance_id, table_id),
            target: target
          }

          auth_opts = Auth.request_opts()
          Stub.drop_row_range(channel, request, auth_opts)
        end

        Client.execute(operation, pool: Client.admin_pool())
    end
  end

  # ============================================================================
  # Backup Operations
  # ============================================================================

  @doc """
  Creates a backup of a table.

  Returns a long-running operation; pass it to `wait_operation/2` to block until
  the `Google.Bigtable.Admin.V2.Backup` is ready.

  ## Options

    * `:expire_time` - When the backup should expire (Google.Protobuf.Timestamp)

  ## Examples

      {:ok, operation} = MegasPinakas.Admin.create_backup(
        "project", "instance", "cluster", "my-backup", "my-table",
        expire_time: expire_timestamp)
      {:ok, backup} = MegasPinakas.Admin.wait_operation(operation)
  """
  @spec create_backup(String.t(), String.t(), String.t(), String.t(), String.t(), keyword()) ::
          {:ok, Operation.t()} | {:error, term()}
  def create_backup(project_id, instance_id, cluster_id, backup_id, source_table_id, opts \\ []) do
    operation = fn channel ->
      backup = %Backup{
        source_table: Config.table_path(project_id, instance_id, source_table_id),
        expire_time: Keyword.get(opts, :expire_time)
      }

      request = %CreateBackupRequest{
        parent: Config.cluster_path(project_id, instance_id, cluster_id),
        backup_id: backup_id,
        backup: backup
      }

      auth_opts = Auth.request_opts()
      Stub.create_backup(channel, request, auth_opts)
    end

    Client.execute(operation, pool: Client.admin_pool())
  end

  @doc """
  Gets details about a backup.

  ## Examples

      {:ok, backup} = MegasPinakas.Admin.get_backup("project", "instance", "cluster", "my-backup")
  """
  @spec get_backup(String.t(), String.t(), String.t(), String.t()) ::
          {:ok, Backup.t()} | {:error, term()}
  def get_backup(project_id, instance_id, cluster_id, backup_id) do
    operation = fn channel ->
      request = %GetBackupRequest{
        name: Config.backup_path(project_id, instance_id, cluster_id, backup_id)
      }

      auth_opts = Auth.request_opts()
      Stub.get_backup(channel, request, auth_opts)
    end

    Client.execute(operation, pool: Client.admin_pool())
  end

  @doc """
  Lists backups in a cluster.

  ## Options

    * `:filter` - Filter expression
    * `:order_by` - Order by expression
    * `:page_size` - Maximum number of backups to return
    * `:page_token` - Page token for pagination

  ## Examples

      {:ok, response} = MegasPinakas.Admin.list_backups("project", "instance", "cluster")
  """
  @spec list_backups(String.t(), String.t(), String.t(), keyword()) ::
          {:ok, ListBackupsResponse.t()} | {:error, term()}
  def list_backups(project_id, instance_id, cluster_id, opts \\ []) do
    operation = fn channel ->
      request = %ListBackupsRequest{
        parent: Config.cluster_path(project_id, instance_id, cluster_id),
        filter: Keyword.get(opts, :filter, ""),
        order_by: Keyword.get(opts, :order_by, ""),
        page_size: Keyword.get(opts, :page_size, 0),
        page_token: Keyword.get(opts, :page_token, "")
      }

      auth_opts = Auth.request_opts()
      Stub.list_backups(channel, request, auth_opts)
    end

    Client.execute(operation, pool: Client.admin_pool())
  end

  @doc """
  Deletes a backup.

  ## Examples

      {:ok, _} = MegasPinakas.Admin.delete_backup("project", "instance", "cluster", "my-backup")
  """
  @spec delete_backup(String.t(), String.t(), String.t(), String.t()) ::
          {:ok, Google.Protobuf.Empty.t()} | {:error, term()}
  def delete_backup(project_id, instance_id, cluster_id, backup_id) do
    operation = fn channel ->
      request = %DeleteBackupRequest{
        name: Config.backup_path(project_id, instance_id, cluster_id, backup_id)
      }

      auth_opts = Auth.request_opts()
      Stub.delete_backup(channel, request, auth_opts)
    end

    Client.execute(operation, pool: Client.admin_pool())
  end

  @doc """
  Restores a table from a backup.

  Returns a long-running operation; pass it to `wait_operation/2` to block until
  the restored `Google.Bigtable.Admin.V2.Table` is ready.

  ## Examples

      {:ok, operation} = MegasPinakas.Admin.restore_table(
        "project", "instance", "restored-table", "cluster", "my-backup")
      {:ok, table} = MegasPinakas.Admin.wait_operation(operation)
  """
  @spec restore_table(String.t(), String.t(), String.t(), String.t(), String.t()) ::
          {:ok, Operation.t()} | {:error, term()}
  def restore_table(project_id, instance_id, table_id, cluster_id, backup_id) do
    operation = fn channel ->
      request = %RestoreTableRequest{
        parent: Config.instance_path(project_id, instance_id),
        table_id: table_id,
        source: {:backup, Config.backup_path(project_id, instance_id, cluster_id, backup_id)}
      }

      auth_opts = Auth.request_opts()
      Stub.restore_table(channel, request, auth_opts)
    end

    Client.execute(operation, pool: Client.admin_pool())
  end

  # ============================================================================
  # Long-running Operations
  # ============================================================================

  @doc """
  Fetches the current state of a long-running operation by its full name
  (`operations/...` or `projects/.../operations/...`).

  ## Examples

      {:ok, %Google.Longrunning.Operation{done: done}} =
        MegasPinakas.Admin.get_operation(operation.name)
  """
  @spec get_operation(String.t()) :: {:ok, Operation.t()} | {:error, term()}
  def get_operation(operation_name) when is_binary(operation_name) do
    operation = fn channel ->
      request = %GetOperationRequest{name: operation_name}

      auth_opts = Auth.request_opts()
      Operations.Stub.get_operation(channel, request, auth_opts)
    end

    Client.execute(operation, pool: Client.admin_pool())
  end

  @doc """
  Polls a long-running operation until it completes.

  Accepts either a `Google.Longrunning.Operation` (as returned by
  `create_backup/6`, `restore_table/5`, or the `MegasPinakas.InstanceAdmin`
  functions) or its name. An operation that is already `done` is resolved
  without any RPC.

  ## Return values

    * `{:ok, resource}` - the operation succeeded and its response was one of the
      admin resource types (`Table`, `Backup`, `Instance`, `Cluster`,
      `AppProfile`, or `Google.Protobuf.Empty`), decoded from the `Any`.
    * `{:ok, %Google.Longrunning.Operation{}}` - the operation succeeded but
      carried no response or one of an unrecognized type; decode
      `operation.result` yourself.
    * `{:error, {status_atom, message}}` - the operation itself failed, or a
      `GetOperation` poll failed.
    * `{:error, :timeout}` - the operation did not complete within `:timeout`.

  ## Options

    * `:poll_interval` - Milliseconds between polls (default: 1000)
    * `:timeout` - Maximum milliseconds to wait in total (default: 300000)

  ## Examples

      {:ok, operation} = MegasPinakas.InstanceAdmin.create_cluster(
        "project", "instance", "new-cluster", "us-east1-b", serve_nodes: 3)

      {:ok, %Google.Bigtable.Admin.V2.Cluster{}} =
        MegasPinakas.Admin.wait_operation(operation, timeout: :timer.minutes(10))
  """
  @spec wait_operation(Operation.t() | String.t(), keyword()) ::
          {:ok, struct()} | {:error, :timeout | term()}
  def wait_operation(operation_or_name, opts \\ [])

  def wait_operation(%Operation{done: true} = operation, opts) do
    validate_wait_opts!(opts)
    resolve_operation(operation)
  end

  def wait_operation(%Operation{name: name}, opts), do: wait_operation(name, opts)

  def wait_operation(operation_name, opts) when is_binary(operation_name) do
    {poll_interval, timeout} = validate_wait_opts!(opts)

    # `:get_fun` is an undocumented seam so the polling loop can be tested
    # without a server that produces slow operations.
    get_fun = Keyword.get(opts, :get_fun, &get_operation/1)
    deadline = System.monotonic_time(:millisecond) + timeout

    poll_operation(operation_name, get_fun, poll_interval, deadline)
  end

  defp validate_wait_opts!(opts) do
    poll_interval = Keyword.get(opts, :poll_interval, @default_poll_interval)
    timeout = Keyword.get(opts, :timeout, @default_wait_timeout)

    unless is_integer(poll_interval) and poll_interval > 0 do
      raise ArgumentError,
            ":poll_interval must be a positive integer, got: #{inspect(poll_interval)}"
    end

    unless is_integer(timeout) and timeout > 0 do
      raise ArgumentError, ":timeout must be a positive integer, got: #{inspect(timeout)}"
    end

    {poll_interval, timeout}
  end

  defp poll_operation(name, get_fun, poll_interval, deadline) do
    case get_fun.(name) do
      {:ok, %Operation{done: true} = operation} ->
        resolve_operation(operation)

      {:ok, %Operation{}} ->
        remaining = deadline - System.monotonic_time(:millisecond)

        if remaining <= 0 do
          {:error, :timeout}
        else
          Process.sleep(min(poll_interval, remaining))
          poll_operation(name, get_fun, poll_interval, deadline)
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp resolve_operation(%Operation{result: {:error, %Google.Rpc.Status{} = status}}) do
    {:error, {Response.status_to_atom(status.code), status.message}}
  end

  defp resolve_operation(%Operation{result: {:response, %Google.Protobuf.Any{} = any}} = op) do
    # type_url is "type.googleapis.com/<full proto name>"; only the name matters.
    type_name = any.type_url |> String.split("/") |> List.last()

    case Map.fetch(@operation_response_types, type_name) do
      {:ok, module} -> {:ok, module.decode(any.value)}
      :error -> {:ok, op}
    end
  end

  defp resolve_operation(%Operation{} = operation), do: {:ok, operation}

  # ============================================================================
  # Column Family Modification Builders
  # ============================================================================

  @doc """
  Creates a modification to add a new column family.

  ## Examples

      MegasPinakas.Admin.create_column_family("cf", max_versions_gc_rule(1))
  """
  @spec create_column_family(String.t(), GcRule.t() | nil) ::
          ModifyColumnFamiliesRequest.Modification.t()
  def create_column_family(family_name, gc_rule \\ nil) do
    %ModifyColumnFamiliesRequest.Modification{
      id: family_name,
      mod: {:create, %ColumnFamily{gc_rule: gc_rule}}
    }
  end

  @doc """
  Creates a modification to update an existing column family.

  ## Examples

      MegasPinakas.Admin.update_column_family("cf", max_age_gc_rule(86400))
  """
  @spec update_column_family(String.t(), GcRule.t() | nil) ::
          ModifyColumnFamiliesRequest.Modification.t()
  def update_column_family(family_name, gc_rule \\ nil) do
    %ModifyColumnFamiliesRequest.Modification{
      id: family_name,
      mod: {:update, %ColumnFamily{gc_rule: gc_rule}}
    }
  end

  @doc """
  Creates a modification to drop a column family.

  ## Examples

      MegasPinakas.Admin.drop_column_family("old_cf")
  """
  @spec drop_column_family(String.t()) :: ModifyColumnFamiliesRequest.Modification.t()
  def drop_column_family(family_name) do
    %ModifyColumnFamiliesRequest.Modification{
      id: family_name,
      mod: {:drop, true}
    }
  end

  # ============================================================================
  # GC Rule Builders
  # ============================================================================

  @doc """
  Creates a GC rule that keeps the N most recent versions of each cell.

  ## Examples

      MegasPinakas.Admin.max_versions_gc_rule(1)  # Keep only latest version
      MegasPinakas.Admin.max_versions_gc_rule(3)  # Keep last 3 versions
  """
  @spec max_versions_gc_rule(integer()) :: GcRule.t()
  def max_versions_gc_rule(max_num_versions) do
    %GcRule{rule: {:max_num_versions, max_num_versions}}
  end

  @doc """
  Creates a GC rule that deletes cells older than a specified age.

  The age is specified in seconds.

  ## Examples

      MegasPinakas.Admin.max_age_gc_rule(86400)    # 1 day
      MegasPinakas.Admin.max_age_gc_rule(604800)   # 1 week
  """
  @spec max_age_gc_rule(integer()) :: GcRule.t()
  def max_age_gc_rule(max_age_seconds) do
    duration = %Google.Protobuf.Duration{
      seconds: max_age_seconds,
      nanos: 0
    }

    %GcRule{rule: {:max_age, duration}}
  end

  @doc """
  Creates a GC rule that combines multiple rules with AND logic.

  All rules must be satisfied for data to be garbage collected.

  ## Examples

      # Delete only cells that are both beyond the 3 newest AND older than 7 days
      rule = MegasPinakas.Admin.intersection_gc_rule([
        MegasPinakas.Admin.max_versions_gc_rule(3),
        MegasPinakas.Admin.max_age_gc_rule(604800)
      ])
  """
  @spec intersection_gc_rule([GcRule.t()]) :: GcRule.t()
  def intersection_gc_rule(rules) do
    %GcRule{rule: {:intersection, %GcRule.Intersection{rules: rules}}}
  end

  @doc """
  Creates a GC rule that combines multiple rules with OR logic.

  If any rule is satisfied, data will be garbage collected.

  ## Examples

      # Delete if more than 1000 versions OR older than 30 days
      rule = MegasPinakas.Admin.union_gc_rule([
        MegasPinakas.Admin.max_versions_gc_rule(1000),
        MegasPinakas.Admin.max_age_gc_rule(2592000)
      ])
  """
  @spec union_gc_rule([GcRule.t()]) :: GcRule.t()
  def union_gc_rule(rules) do
    %GcRule{rule: {:union, %GcRule.Union{rules: rules}}}
  end

  # ============================================================================
  # Private Helpers
  # ============================================================================

  # Argument shape is checked here, before Client.execute/2, so a bad call
  # raises ArgumentError instead of surfacing as {:error, {:execution_error, _}}.
  defp build_column_families(families) when is_map(families) do
    Map.new(families, fn
      {name, config} when is_binary(name) and is_map(config) and not is_struct(config) ->
        gc_rule = Map.get(config, :gc_rule) || Map.get(config, "gc_rule")
        {name, %ColumnFamily{gc_rule: gc_rule}}

      {name, config} ->
        raise ArgumentError,
              ":column_families entries must be {family_name :: String.t(), config :: map()}, " <>
                "got: #{inspect({name, config})}"
    end)
  end

  defp build_column_families(other) do
    raise ArgumentError,
          ":column_families must be a map of family name to config map " <>
            "(e.g. %{\"cf\" => %{gc_rule: rule}}), got: #{inspect(other)}"
  end

  defp build_initial_splits(keys) when is_list(keys) do
    Enum.map(keys, fn
      key when is_binary(key) ->
        %CreateTableRequest.Split{key: key}

      other ->
        raise ArgumentError, ":initial_splits must be a list of binaries, got: #{inspect(other)}"
    end)
  end

  defp build_initial_splits(other) do
    raise ArgumentError, ":initial_splits must be a list of binaries, got: #{inspect(other)}"
  end

  defp drop_row_range_target(opts) do
    prefix = Keyword.fetch(opts, :row_key_prefix)
    delete_all = Keyword.get(opts, :delete_all_data_from_table, false)

    case {prefix, delete_all} do
      {{:ok, _}, true} ->
        raise ArgumentError,
              ":row_key_prefix and :delete_all_data_from_table are mutually exclusive"

      {{:ok, prefix}, _} when is_binary(prefix) ->
        {:row_key_prefix, prefix}

      {{:ok, other}, _} ->
        raise ArgumentError, ":row_key_prefix must be a binary, got: #{inspect(other)}"

      {:error, true} ->
        {:delete_all_data_from_table, true}

      {:error, false} ->
        nil

      {:error, other} ->
        raise ArgumentError,
              ":delete_all_data_from_table must be a boolean, got: #{inspect(other)}"
    end
  end
end
