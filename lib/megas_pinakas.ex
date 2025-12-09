defmodule MegasPinakas do
  @moduledoc """
  High-level API for Google Cloud BigTable operations via gRPC.

  This module provides functions for reading and writing data to BigTable tables.
  For administrative operations (creating tables, managing instances), see
  `MegasPinakas.Admin` and `MegasPinakas.InstanceAdmin`.

  ## Configuration

  Configure the connection in your config files:

      # For emulator (development/testing)
      config :megas_pinakas, :emulator,
        host: "localhost",
        port: 8086

      # For production
      config :megas_pinakas, GrpcConnectionPool,
        endpoint: [type: :production, host: "bigtable.googleapis.com", port: 443, ssl: []],
        pool: [size: 10, name: MegasPinakas.ConnectionPool]

  ## Usage

      # Read a single row
      {:ok, row} = MegasPinakas.read_row("my-project", "my-instance", "my-table", "row-key")

      # Write a row
      mutations = [MegasPinakas.set_cell("cf", "col", "value")]
      {:ok, _} = MegasPinakas.mutate_row("my-project", "my-instance", "my-table", "row-key", mutations)
  """

  alias MegasPinakas.{Auth, Client, Config}

  # Aliases for protobuf modules
  alias Google.Bigtable.V2.{
    Bigtable.Stub,
    CheckAndMutateRowRequest,
    CheckAndMutateRowResponse,
    MutateRowRequest,
    MutateRowResponse,
    MutateRowsRequest,
    Mutation,
    ReadModifyWriteRowRequest,
    ReadModifyWriteRowResponse,
    ReadModifyWriteRule,
    ReadRowsRequest,
    ReadRowsResponse,
    Row,
    RowFilter,
    RowRange,
    RowSet,
    SampleRowKeysRequest
  }

  # ============================================================================
  # Read Operations
  # ============================================================================

  @doc """
  Reads rows from a BigTable table.

  Returns a stream of row chunks that need to be assembled into complete rows.

  ## Options

    * `:rows` - A `RowSet` specifying which rows to read
    * `:filter` - A `RowFilter` to apply
    * `:rows_limit` - Maximum number of rows to return
    * `:app_profile_id` - App profile to use

  ## Examples

      # Read all rows
      {:ok, stream} = MegasPinakas.read_rows("project", "instance", "table")

      # Read specific row keys
      {:ok, stream} = MegasPinakas.read_rows("project", "instance", "table",
        rows: MegasPinakas.row_set(["row1", "row2", "row3"]))

      # Read with filter
      {:ok, stream} = MegasPinakas.read_rows("project", "instance", "table",
        filter: MegasPinakas.column_filter("cf", "col"))
  """
  @spec read_rows(String.t(), String.t(), String.t(), keyword()) ::
          {:ok, Enumerable.t()} | {:error, term()}
  def read_rows(project_id, instance_id, table_id, opts \\ []) do
    operation = fn channel ->
      request = %ReadRowsRequest{
        table_name: Config.table_path(project_id, instance_id, table_id),
        app_profile_id: Keyword.get(opts, :app_profile_id, ""),
        rows: Keyword.get(opts, :rows),
        filter: Keyword.get(opts, :filter),
        rows_limit: Keyword.get(opts, :rows_limit, 0),
        request_stats_view: :REQUEST_STATS_VIEW_UNSPECIFIED
      }

      auth_opts = Auth.request_opts()
      Stub.read_rows(channel, request, auth_opts)
    end

    Client.execute(operation)
  end

  @doc """
  Reads a single row from a BigTable table.

  This is a convenience function that wraps `read_rows/4` for single-row lookups.

  ## Options

    * `:filter` - A `RowFilter` to apply
    * `:app_profile_id` - App profile to use

  ## Examples

      {:ok, row} = MegasPinakas.read_row("project", "instance", "table", "my-row-key")
  """
  @spec read_row(String.t(), String.t(), String.t(), binary(), keyword()) ::
          {:ok, Row.t() | nil} | {:error, term()}
  def read_row(project_id, instance_id, table_id, row_key, opts \\ []) do
    rows = row_set([row_key])
    opts = Keyword.put(opts, :rows, rows)
    opts = Keyword.put(opts, :rows_limit, 1)

    case read_rows(project_id, instance_id, table_id, opts) do
      {:ok, {:ok, stream}} ->
        # Collect all chunks and assemble into rows
        rows = collect_read_rows_stream(stream)

        case rows do
          [row | _] -> {:ok, row}
          [] -> {:ok, nil}
        end

      {:ok, {:error, reason}} ->
        {:error, reason}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Samples row keys from a BigTable table.

  Returns a stream of sample row keys that can be used to split the table
  into segments for parallel processing.

  ## Options

    * `:app_profile_id` - App profile to use

  ## Examples

      {:ok, stream} = MegasPinakas.sample_row_keys("project", "instance", "table")
  """
  @spec sample_row_keys(String.t(), String.t(), String.t(), keyword()) ::
          {:ok, Enumerable.t()} | {:error, term()}
  def sample_row_keys(project_id, instance_id, table_id, opts \\ []) do
    operation = fn channel ->
      request = %SampleRowKeysRequest{
        table_name: Config.table_path(project_id, instance_id, table_id),
        app_profile_id: Keyword.get(opts, :app_profile_id, "")
      }

      auth_opts = Auth.request_opts()
      Stub.sample_row_keys(channel, request, auth_opts)
    end

    Client.execute(operation)
  end

  # ============================================================================
  # Write Operations
  # ============================================================================

  @doc """
  Mutates a single row in a BigTable table.

  ## Options

    * `:app_profile_id` - App profile to use

  ## Examples

      mutations = [
        MegasPinakas.set_cell("cf", "col1", "value1"),
        MegasPinakas.set_cell("cf", "col2", "value2")
      ]
      {:ok, _} = MegasPinakas.mutate_row("project", "instance", "table", "row-key", mutations)
  """
  @spec mutate_row(String.t(), String.t(), String.t(), binary(), [Mutation.t()], keyword()) ::
          {:ok, MutateRowResponse.t()} | {:error, term()}
  def mutate_row(project_id, instance_id, table_id, row_key, mutations, opts \\ []) do
    operation = fn channel ->
      request = %MutateRowRequest{
        table_name: Config.table_path(project_id, instance_id, table_id),
        app_profile_id: Keyword.get(opts, :app_profile_id, ""),
        row_key: row_key,
        mutations: mutations
      }

      auth_opts = Auth.request_opts()
      Stub.mutate_row(channel, request, auth_opts)
    end

    Client.execute(operation)
  end

  @doc """
  Mutates multiple rows in a BigTable table.

  Returns a stream of responses indicating the status of each row mutation.

  ## Options

    * `:app_profile_id` - App profile to use

  ## Examples

      entries = [
        %{row_key: "row1", mutations: [MegasPinakas.set_cell("cf", "col", "val1")]},
        %{row_key: "row2", mutations: [MegasPinakas.set_cell("cf", "col", "val2")]}
      ]
      {:ok, stream} = MegasPinakas.mutate_rows("project", "instance", "table", entries)
  """
  @spec mutate_rows(String.t(), String.t(), String.t(), [map()], keyword()) ::
          {:ok, Enumerable.t()} | {:error, term()}
  def mutate_rows(project_id, instance_id, table_id, entries, opts \\ []) do
    operation = fn channel ->
      request_entries =
        Enum.map(entries, fn entry ->
          %MutateRowsRequest.Entry{
            row_key: entry[:row_key] || entry["row_key"],
            mutations: entry[:mutations] || entry["mutations"]
          }
        end)

      request = %MutateRowsRequest{
        table_name: Config.table_path(project_id, instance_id, table_id),
        app_profile_id: Keyword.get(opts, :app_profile_id, ""),
        entries: request_entries
      }

      auth_opts = Auth.request_opts()
      Stub.mutate_rows(channel, request, auth_opts)
    end

    Client.execute(operation)
  end

  @doc """
  Performs a conditional mutation on a row.

  If the predicate filter matches any cells, `true_mutations` are applied;
  otherwise `false_mutations` are applied.

  ## Options

    * `:app_profile_id` - App profile to use

  ## Examples

      # Set a value only if the row doesn't exist
      predicate = MegasPinakas.pass_all_filter()
      true_mutations = []
      false_mutations = [MegasPinakas.set_cell("cf", "col", "initial")]

      {:ok, response} = MegasPinakas.check_and_mutate_row(
        "project", "instance", "table", "row-key",
        predicate, true_mutations, false_mutations)
  """
  @spec check_and_mutate_row(
          String.t(),
          String.t(),
          String.t(),
          binary(),
          RowFilter.t() | nil,
          [Mutation.t()],
          [Mutation.t()],
          keyword()
        ) :: {:ok, CheckAndMutateRowResponse.t()} | {:error, term()}
  def check_and_mutate_row(
        project_id,
        instance_id,
        table_id,
        row_key,
        predicate_filter,
        true_mutations,
        false_mutations,
        opts \\ []
      ) do
    operation = fn channel ->
      request = %CheckAndMutateRowRequest{
        table_name: Config.table_path(project_id, instance_id, table_id),
        app_profile_id: Keyword.get(opts, :app_profile_id, ""),
        row_key: row_key,
        predicate_filter: predicate_filter,
        true_mutations: true_mutations,
        false_mutations: false_mutations
      }

      auth_opts = Auth.request_opts()
      Stub.check_and_mutate_row(channel, request, auth_opts)
    end

    Client.execute(operation)
  end

  @doc """
  Performs an atomic read-modify-write operation on a row.

  ## Options

    * `:app_profile_id` - App profile to use

  ## Examples

      # Increment a counter
      rules = [MegasPinakas.increment_rule("cf", "counter", 1)]
      {:ok, response} = MegasPinakas.read_modify_write_row(
        "project", "instance", "table", "row-key", rules)

      # Append to a value
      rules = [MegasPinakas.append_rule("cf", "log", "new entry\\n")]
      {:ok, response} = MegasPinakas.read_modify_write_row(
        "project", "instance", "table", "row-key", rules)
  """
  @spec read_modify_write_row(
          String.t(),
          String.t(),
          String.t(),
          binary(),
          [ReadModifyWriteRule.t()],
          keyword()
        ) :: {:ok, ReadModifyWriteRowResponse.t()} | {:error, term()}
  def read_modify_write_row(project_id, instance_id, table_id, row_key, rules, opts \\ []) do
    operation = fn channel ->
      request = %ReadModifyWriteRowRequest{
        table_name: Config.table_path(project_id, instance_id, table_id),
        app_profile_id: Keyword.get(opts, :app_profile_id, ""),
        row_key: row_key,
        rules: rules
      }

      auth_opts = Auth.request_opts()
      Stub.read_modify_write_row(channel, request, auth_opts)
    end

    Client.execute(operation)
  end

  # ============================================================================
  # Mutation Builders
  # ============================================================================

  @doc """
  Creates a SetCell mutation.

  ## Options

    * `:timestamp_micros` - Timestamp in microseconds. Defaults to -1 (server-assigned).

  ## Examples

      MegasPinakas.set_cell("column_family", "column_qualifier", "value")
      MegasPinakas.set_cell("cf", "col", "value", timestamp_micros: 1234567890000)
  """
  @spec set_cell(String.t(), binary(), binary(), keyword()) :: Mutation.t()
  def set_cell(family_name, column_qualifier, value, opts \\ []) do
    timestamp = Keyword.get(opts, :timestamp_micros, -1)

    %Mutation{
      mutation:
        {:set_cell,
         %Mutation.SetCell{
           family_name: family_name,
           column_qualifier: column_qualifier,
           timestamp_micros: timestamp,
           value: value
         }}
    }
  end

  @doc """
  Creates a DeleteFromColumn mutation.

  Deletes cells from a specific column, optionally within a time range.

  ## Options

    * `:time_range` - A `TimestampRange` to limit deletion

  ## Examples

      MegasPinakas.delete_from_column("cf", "col")
  """
  @spec delete_from_column(String.t(), binary(), keyword()) :: Mutation.t()
  def delete_from_column(family_name, column_qualifier, opts \\ []) do
    time_range = Keyword.get(opts, :time_range)

    %Mutation{
      mutation:
        {:delete_from_column,
         %Mutation.DeleteFromColumn{
           family_name: family_name,
           column_qualifier: column_qualifier,
           time_range: time_range
         }}
    }
  end

  @doc """
  Creates a DeleteFromFamily mutation.

  Deletes all cells from a column family in the row.

  ## Examples

      MegasPinakas.delete_from_family("cf")
  """
  @spec delete_from_family(String.t()) :: Mutation.t()
  def delete_from_family(family_name) do
    %Mutation{
      mutation:
        {:delete_from_family,
         %Mutation.DeleteFromFamily{
           family_name: family_name
         }}
    }
  end

  @doc """
  Creates a DeleteFromRow mutation.

  Deletes all cells from the row.

  ## Examples

      MegasPinakas.delete_from_row()
  """
  @spec delete_from_row() :: Mutation.t()
  def delete_from_row do
    %Mutation{
      mutation: {:delete_from_row, %Mutation.DeleteFromRow{}}
    }
  end

  # ============================================================================
  # ReadModifyWrite Rule Builders
  # ============================================================================

  @doc """
  Creates an increment rule for read-modify-write operations.

  ## Examples

      MegasPinakas.increment_rule("cf", "counter", 1)
      MegasPinakas.increment_rule("cf", "counter", -5)
  """
  @spec increment_rule(String.t(), binary(), integer()) :: ReadModifyWriteRule.t()
  def increment_rule(family_name, column_qualifier, increment_amount) do
    %ReadModifyWriteRule{
      family_name: family_name,
      column_qualifier: column_qualifier,
      rule: {:increment_amount, increment_amount}
    }
  end

  @doc """
  Creates an append rule for read-modify-write operations.

  ## Examples

      MegasPinakas.append_rule("cf", "log", "new entry\\n")
  """
  @spec append_rule(String.t(), binary(), binary()) :: ReadModifyWriteRule.t()
  def append_rule(family_name, column_qualifier, append_value) do
    %ReadModifyWriteRule{
      family_name: family_name,
      column_qualifier: column_qualifier,
      rule: {:append_value, append_value}
    }
  end

  # ============================================================================
  # RowSet Builders
  # ============================================================================

  @doc """
  Creates a RowSet from a list of row keys.

  ## Examples

      MegasPinakas.row_set(["row1", "row2", "row3"])
  """
  @spec row_set([binary()]) :: RowSet.t()
  def row_set(row_keys) when is_list(row_keys) do
    %RowSet{row_keys: row_keys, row_ranges: []}
  end

  @doc """
  Creates a RowSet from row ranges.

  ## Examples

      ranges = [MegasPinakas.row_range("a", "z")]
      MegasPinakas.row_set_from_ranges(ranges)
  """
  @spec row_set_from_ranges([RowRange.t()]) :: RowSet.t()
  def row_set_from_ranges(row_ranges) when is_list(row_ranges) do
    %RowSet{row_keys: [], row_ranges: row_ranges}
  end

  @doc """
  Creates a row range with closed start and open end.

  ## Examples

      MegasPinakas.row_range("user#100", "user#200")
  """
  @spec row_range(binary(), binary()) :: RowRange.t()
  def row_range(start_key, end_key) do
    %RowRange{
      start_key: {:start_key_closed, start_key},
      end_key: {:end_key_open, end_key}
    }
  end

  @doc """
  Creates a row range with prefix matching.

  ## Examples

      MegasPinakas.row_range_prefix("user#")
  """
  @spec row_range_prefix(binary()) :: RowRange.t()
  def row_range_prefix(prefix) do
    # Calculate the end key by incrementing the last byte
    end_key = calculate_prefix_end(prefix)

    %RowRange{
      start_key: {:start_key_closed, prefix},
      end_key: {:end_key_open, end_key}
    }
  end

  # ============================================================================
  # Filter Builders
  # ============================================================================

  @doc """
  Creates a filter that matches a specific column.

  ## Examples

      MegasPinakas.column_filter("cf", "col")
  """
  @spec column_filter(String.t(), binary()) :: RowFilter.t()
  def column_filter(family_name, column_qualifier) do
    %RowFilter{
      filter:
        {:chain,
         %RowFilter.Chain{
           filters: [
             %RowFilter{filter: {:family_name_regex_filter, "^#{Regex.escape(family_name)}$"}},
             %RowFilter{filter: {:column_qualifier_regex_filter, column_qualifier}}
           ]
         }}
    }
  end

  @doc """
  Creates a filter that matches a column family.

  ## Examples

      MegasPinakas.family_filter("cf")
  """
  @spec family_filter(String.t()) :: RowFilter.t()
  def family_filter(family_name) do
    %RowFilter{filter: {:family_name_regex_filter, "^#{Regex.escape(family_name)}$"}}
  end

  @doc """
  Creates a filter that limits cells per column.

  ## Examples

      MegasPinakas.cells_per_column_limit_filter(1)
  """
  @spec cells_per_column_limit_filter(integer()) :: RowFilter.t()
  def cells_per_column_limit_filter(limit) do
    %RowFilter{filter: {:cells_per_column_limit_filter, limit}}
  end

  @doc """
  Creates a filter that passes all cells.

  ## Examples

      MegasPinakas.pass_all_filter()
  """
  @spec pass_all_filter() :: RowFilter.t()
  def pass_all_filter do
    %RowFilter{filter: {:pass_all_filter, true}}
  end

  @doc """
  Creates a filter that blocks all cells.

  ## Examples

      MegasPinakas.block_all_filter()
  """
  @spec block_all_filter() :: RowFilter.t()
  def block_all_filter do
    %RowFilter{filter: {:block_all_filter, true}}
  end

  @doc """
  Chains multiple filters together (AND logic).

  ## Examples

      filters = [
        MegasPinakas.family_filter("cf"),
        MegasPinakas.cells_per_column_limit_filter(1)
      ]
      MegasPinakas.chain_filters(filters)
  """
  @spec chain_filters([RowFilter.t()]) :: RowFilter.t()
  def chain_filters(filters) do
    %RowFilter{filter: {:chain, %RowFilter.Chain{filters: filters}}}
  end

  @doc """
  Interleaves multiple filters (OR logic).

  ## Examples

      filters = [
        MegasPinakas.family_filter("cf1"),
        MegasPinakas.family_filter("cf2")
      ]
      MegasPinakas.interleave_filters(filters)
  """
  @spec interleave_filters([RowFilter.t()]) :: RowFilter.t()
  def interleave_filters(filters) do
    %RowFilter{filter: {:interleave, %RowFilter.Interleave{filters: filters}}}
  end

  # ============================================================================
  # Private Helpers
  # ============================================================================

  defp calculate_prefix_end(prefix) when byte_size(prefix) == 0 do
    <<>>
  end

  defp calculate_prefix_end(prefix) do
    # Get all bytes except the last
    prefix_size = byte_size(prefix) - 1
    <<head::binary-size(prefix_size), last_byte>> = prefix

    if last_byte == 255 do
      # If last byte is 255, we need to increment the previous byte
      calculate_prefix_end(head)
    else
      <<head::binary, last_byte + 1>>
    end
  end

  defp collect_read_rows_stream(stream) do
    stream
    |> Enum.reduce({[], nil, []}, &process_read_rows_chunk/2)
    |> finalize_rows()
  end

  defp process_read_rows_chunk({:ok, %ReadRowsResponse{chunks: chunks}}, acc) do
    Enum.reduce(chunks, acc, &process_chunk/2)
  end

  defp process_read_rows_chunk({:error, _reason}, acc), do: acc
  defp process_read_rows_chunk(_, acc), do: acc

  defp process_chunk(chunk, {rows, current_row, current_cells}) do
    # Build cell data from chunk
    new_cells =
      if chunk.value do
        [
          %{
            family: chunk.family_name && chunk.family_name.value,
            qualifier: chunk.qualifier && chunk.qualifier.value,
            timestamp: chunk.timestamp_micros,
            value: chunk.value,
            labels: chunk.labels
          }
          | current_cells
        ]
      else
        current_cells
      end

    # row_status is a oneof field: {:commit_row, true} | {:reset_row, true} | nil
    case chunk.row_status do
      {:commit_row, true} ->
        row_key = chunk.row_key || (current_row && current_row.key)
        row = build_row(row_key, new_cells)
        {[row | rows], nil, []}

      {:reset_row, true} ->
        # Reset current row, discard accumulated cells
        {rows, nil, []}

      _ ->
        # Continue accumulating cells for current row
        new_row = %{key: chunk.row_key || (current_row && current_row.key)}
        {rows, new_row, new_cells}
    end
  end

  defp build_row(key, cells) do
    families =
      cells
      |> Enum.reverse()
      |> Enum.group_by(& &1.family)
      |> Enum.map(fn {family, family_cells} ->
        columns =
          family_cells
          |> Enum.group_by(& &1.qualifier)
          |> Enum.map(fn {qualifier, qual_cells} ->
            %Google.Bigtable.V2.Column{
              qualifier: qualifier,
              cells:
                Enum.map(qual_cells, fn c ->
                  %Google.Bigtable.V2.Cell{
                    timestamp_micros: c.timestamp,
                    value: c.value,
                    labels: c.labels || []
                  }
                end)
            }
          end)

        %Google.Bigtable.V2.Family{
          name: family,
          columns: columns
        }
      end)

    %Row{
      key: key,
      families: families
    }
  end

  defp finalize_rows({rows, _current_row, _current_cells}) do
    Enum.reverse(rows)
  end
end
