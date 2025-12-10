defmodule MegasPinakas.Admin do
  @moduledoc """
  Table administration operations for BigTable.

  This module provides functions for creating, modifying, and deleting tables,
  as well as managing column families and backups.
  """

  alias MegasPinakas.{Auth, Client, Config}

  alias Google.Bigtable.Admin.V2.{
    BigtableTableAdmin.Stub,
    Backup,
    ColumnFamily,
    CreateBackupRequest,
    CreateTableRequest,
    DeleteBackupRequest,
    DeleteTableRequest,
    DropRowRangeRequest,
    GcRule,
    GetBackupRequest,
    GetTableRequest,
    ListBackupsRequest,
    ListBackupsResponse,
    ListTablesRequest,
    ListTablesResponse,
    ModifyColumnFamiliesRequest,
    RestoreTableRequest,
    Table
  }

  # ============================================================================
  # Table Operations
  # ============================================================================

  @doc """
  Creates a new BigTable table.

  ## Options

    * `:column_families` - Map of column family names to their configurations
    * `:initial_splits` - List of row keys to use for initial table splits

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
    operation = fn channel ->
      column_families =
        opts
        |> Keyword.get(:column_families, %{})
        |> Enum.map(fn {name, config} ->
          gc_rule = Map.get(config, :gc_rule) || Map.get(config, "gc_rule")
          {name, %ColumnFamily{gc_rule: gc_rule}}
        end)
        |> Map.new()

      initial_splits =
        opts
        |> Keyword.get(:initial_splits, [])
        |> Enum.map(fn key ->
          %CreateTableRequest.Split{key: key}
        end)

      table = %Table{
        column_families: column_families
      }

      request = %CreateTableRequest{
        parent: Config.instance_path(project_id, instance_id),
        table_id: table_id,
        table: table,
        initial_splits: initial_splits
      }

      auth_opts = Auth.request_opts()
      Stub.create_table(channel, request, auth_opts)
    end

    Client.execute(operation)
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

    Client.execute(operation)
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

    Client.execute(operation)
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

    Client.execute(operation)
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

    Client.execute(operation)
  end

  @doc """
  Drops a range of rows from a table.

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
          {:ok, Google.Protobuf.Empty.t()} | {:error, term()}
  def drop_row_range(project_id, instance_id, table_id, opts \\ []) do
    operation = fn channel ->
      target =
        cond do
          Keyword.has_key?(opts, :row_key_prefix) ->
            {:row_key_prefix, Keyword.get(opts, :row_key_prefix)}

          Keyword.get(opts, :delete_all_data_from_table) ->
            {:delete_all_data_from_table, true}

          true ->
            nil
        end

      request = %DropRowRangeRequest{
        name: Config.table_path(project_id, instance_id, table_id),
        target: target
      }

      auth_opts = Auth.request_opts()
      Stub.drop_row_range(channel, request, auth_opts)
    end

    Client.execute(operation)
  end

  # ============================================================================
  # Backup Operations
  # ============================================================================

  @doc """
  Creates a backup of a table.

  Returns a long-running operation that can be monitored.

  ## Options

    * `:expire_time` - When the backup should expire (Google.Protobuf.Timestamp)

  ## Examples

      {:ok, operation} = MegasPinakas.Admin.create_backup(
        "project", "instance", "cluster", "my-backup", "my-table",
        expire_time: expire_timestamp)
  """
  @spec create_backup(String.t(), String.t(), String.t(), String.t(), String.t(), keyword()) ::
          {:ok, Google.Longrunning.Operation.t()} | {:error, term()}
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

    Client.execute(operation)
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

    Client.execute(operation)
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

    Client.execute(operation)
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

    Client.execute(operation)
  end

  @doc """
  Restores a table from a backup.

  Returns a long-running operation that can be monitored.

  ## Examples

      {:ok, operation} = MegasPinakas.Admin.restore_table(
        "project", "instance", "restored-table", "cluster", "my-backup")
  """
  @spec restore_table(String.t(), String.t(), String.t(), String.t(), String.t()) ::
          {:ok, Google.Longrunning.Operation.t()} | {:error, term()}
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

    Client.execute(operation)
  end

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

      # Keep last 3 versions OR data younger than 7 days
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
end
