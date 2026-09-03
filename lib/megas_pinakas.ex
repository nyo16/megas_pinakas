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

  ## Eager reads vs streaming

  `read_rows/4` is **eager**: it assembles every matching row into a list before
  returning. That is the right shape for bounded reads — a key set, a narrow
  range, anything with a `:rows_limit` — and it is what makes the returned value
  a plain list you can pattern-match.

  It is the wrong shape for a large scan. `read_rows(p, i, "big_table")` with no
  `:rows` and no `:rows_limit` materializes the whole table. Pass `:max_rows` to
  turn that into `{:error, :result_too_large}` rather than an out-of-memory crash.

  For large or open-ended scans use `MegasPinakas.Streaming`, which fetches in
  batches as you consume and bounds memory by `:batch_size`:

      # Bounded — fine eagerly
      {:ok, rows} = MegasPinakas.read_rows(p, i, "users", rows: MegasPinakas.row_set(keys))

      # Unbounded — stream it
      MegasPinakas.Streaming.stream_prefix(p, i, "events", "2026-08-")
      |> Stream.map(&MegasPinakas.row_to_map/1)
      |> Enum.reduce(0, fn _row, n -> n + 1 end)

  The same applies to helpers built on `read_rows/4`, notably
  `MegasPinakas.Cache.get_many/5` and `MegasPinakas.CounterTTL.get_window/7`:
  they are bounded by the keys or window you pass in, so keep those bounded.
  """

  alias MegasPinakas.{Auth, Client, Config, Filter, Response, RowAssembler}
  alias MegasPinakas.Row, as: RowBuilder

  # Aliases for protobuf modules
  alias Google.Bigtable.V2.{
    Bigtable.Stub,
    CheckAndMutateRowRequest,
    CheckAndMutateRowResponse,
    MutateRowRequest,
    MutateRowResponse,
    MutateRowsRequest,
    MutateRowsResponse,
    Mutation,
    ReadModifyWriteRowRequest,
    ReadModifyWriteRowResponse,
    ReadModifyWriteRule,
    ReadRowsRequest,
    Row,
    RowFilter,
    RowRange,
    RowSet,
    SampleRowKeysRequest,
    SampleRowKeysResponse
  }

  require Logger

  # ============================================================================
  # Read Operations
  # ============================================================================

  @doc """
  Reads rows from a BigTable table.

  Returns a list of assembled rows matching the request criteria.

  ## This function is eager

  Every matching row is assembled into memory before this returns. With no
  `:rows_limit` and no `:rows`, `read_rows(p, i, "big_table")` materializes the
  **entire table** — roughly 3.4 KB per row in the benchmark fixture, so 1 M rows
  is several gigabytes.

  For large or unbounded scans use `MegasPinakas.Streaming.stream_rows/4`, which
  bounds memory by `:batch_size` instead of by the size of the result set. Use
  `:max_rows` below to turn an accidental full-table read into an error rather
  than an out-of-memory crash.

  ## Options

    * `:rows` - A `RowSet` specifying which rows to read
    * `:filter` - A `RowFilter` to apply
    * `:rows_limit` - Maximum number of rows to return; a non-negative integer,
      where `0` (the default) means no limit
    * `:max_rows` - Safety cap (default `:infinity`). When the result would exceed
      it, returns `{:error, :result_too_large}` instead of a list. Implemented by
      asking the server for at most `max_rows + 1` rows, so exceeding the cap
      costs one extra row, not a full scan. Must be `:infinity` or a positive
      integer; anything else raises `ArgumentError`.
    * `:app_profile_id` - App profile to use

  ## Examples

      # Read all rows
      {:ok, rows} = MegasPinakas.read_rows("project", "instance", "table")

      # Read specific row keys
      {:ok, rows} = MegasPinakas.read_rows("project", "instance", "table",
        rows: MegasPinakas.row_set(["row1", "row2", "row3"]))

      # Read with filter
      {:ok, rows} = MegasPinakas.read_rows("project", "instance", "table",
        filter: MegasPinakas.column_filter("cf", "col"))

      # Refuse to materialize more than 100k rows
      case MegasPinakas.read_rows("project", "instance", "table", max_rows: 100_000) do
        {:ok, rows} -> rows
        {:error, :result_too_large} -> :use_streaming_instead
      end
  """
  @spec read_rows(String.t(), String.t(), String.t(), keyword()) ::
          {:ok, [Row.t()]} | {:error, term()}
  def read_rows(project_id, instance_id, table_id, opts \\ []) do
    max_rows = validate_max_rows!(Keyword.get(opts, :max_rows, :infinity))
    rows_limit = validate_rows_limit!(Keyword.get(opts, :rows_limit, 0))

    operation = fn channel ->
      request = %ReadRowsRequest{
        table_name: Config.table_path(project_id, instance_id, table_id),
        app_profile_id: Keyword.get(opts, :app_profile_id, ""),
        rows: Keyword.get(opts, :rows),
        filter: Keyword.get(opts, :filter),
        rows_limit: effective_rows_limit(rows_limit, max_rows),
        request_stats_view: :REQUEST_STATS_VIEW_UNSPECIFIED
      }

      auth_opts = Auth.request_opts()

      # Collect inside the operation, not after Client.execute/2 returns, so the
      # `[:megas_pinakas, :request, :*]` span and the `rescue` in Client.execute/2
      # both cover stream consumption. Consuming afterwards reported only the time
      # to obtain the stream handle — 12% of the real duration at 10k rows — and
      # let consumption exceptions escape uncaught after a *success* :stop event.
      # `with` passes a non-matching value straight through, so RPC errors and
      # assembly errors both surface unchanged.
      with {:ok, stream} <- Stub.read_rows(channel, request, auth_opts),
           {:ok, rows} <- RowAssembler.reduce_all(stream) do
        enforce_max_rows(rows, max_rows)
      end
    end

    Client.execute(operation)
  end

  # Both options are validated here, before the closure is built, so a bad
  # value raises at the call site rather than inside Client.execute/2 where it
  # would be rescued into an `{:error, _}` and emit a bogus :exception event.
  defp validate_max_rows!(:infinity), do: :infinity
  defp validate_max_rows!(n) when is_integer(n) and n > 0, do: n

  defp validate_max_rows!(other) do
    raise ArgumentError,
          ":max_rows must be :infinity or a positive integer, got: #{inspect(other)}"
  end

  defp validate_rows_limit!(n) when is_integer(n) and n >= 0, do: n

  defp validate_rows_limit!(other) do
    raise ArgumentError, ":rows_limit must be a non-negative integer, got: #{inspect(other)}"
  end

  # Asks the server for one row beyond the cap. That extra row is what makes the
  # overflow detectable, and asking for it is what keeps the cap cheap — the
  # server stops there instead of streaming a whole table we would then discard.
  defp effective_rows_limit(rows_limit, :infinity), do: rows_limit

  defp effective_rows_limit(rows_limit, max_rows) do
    # A `:rows_limit` of 0 means "no limit" in the ReadRows proto.
    if rows_limit > 0, do: min(rows_limit, max_rows + 1), else: max_rows + 1
  end

  defp enforce_max_rows(rows, :infinity), do: {:ok, rows}

  defp enforce_max_rows(rows, max_rows) do
    if length(rows) > max_rows, do: {:error, :result_too_large}, else: {:ok, rows}
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
  def read_row(project_id, instance_id, table_id, row_key, opts \\ [])
      when is_binary(row_key) do
    opts =
      opts
      |> Keyword.put(:rows, row_set([row_key]))
      |> Keyword.put(:rows_limit, 1)

    case read_rows(project_id, instance_id, table_id, opts) do
      {:ok, [row | _]} -> {:ok, row}
      {:ok, []} -> {:ok, nil}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Samples row keys from a BigTable table.

  Returns a list of sample row keys that can be used to split the table into
  segments for parallel processing. Each sample carries a `row_key` and the
  approximate `offset_bytes` of data preceding it.

  ## Options

    * `:app_profile_id` - App profile to use

  ## Examples

      {:ok, samples} = MegasPinakas.sample_row_keys("project", "instance", "table")

      # Split the table into ranges for parallel scans
      boundaries = Enum.map(samples, & &1.row_key)

  > #### Changed in 0.6.0 {: .warning}
  >
  > Previously returned an unconsumed gRPC stream. It now returns a list,
  > materialized inside the request span so failures surface as `{:error, _}`
  > rather than when the caller happens to enumerate.
  """
  @spec sample_row_keys(String.t(), String.t(), String.t(), keyword()) ::
          {:ok, [SampleRowKeysResponse.t()]} | {:error, term()}
  def sample_row_keys(project_id, instance_id, table_id, opts \\ []) do
    operation = fn channel ->
      request = %SampleRowKeysRequest{
        table_name: Config.table_path(project_id, instance_id, table_id),
        app_profile_id: Keyword.get(opts, :app_profile_id, "")
      }

      auth_opts = Auth.request_opts()

      case Stub.sample_row_keys(channel, request, auth_opts) do
        {:ok, stream} -> collect_response_stream(stream)
        other -> other
      end
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
  def mutate_row(project_id, instance_id, table_id, row_key, mutations, opts \\ [])
      when is_binary(row_key) and is_list(mutations) do
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

  Returns one result per entry, ordered by `index`, which is the position of the
  entry in the list you passed in.

  ## Per-entry failures are not request failures

  `MutateRows` is partial-success: the RPC can succeed while individual rows
  fail. `{:ok, results}` therefore does **not** mean every mutation applied —
  you must inspect each status:

      {:ok, results} = MegasPinakas.mutate_rows(project, instance, table, entries)

      case Enum.reject(results, &(&1.status.code == 0)) do
        [] -> :all_applied
        failed -> {:error, Enum.map(failed, &{&1.index, &1.status.message})}
      end

  ## Entries

  Each entry is a `MegasPinakas.Row` or a map with `row_key` and `mutations`
  keys (atom or string). Entries are validated before any RPC is made; a
  malformed entry raises `ArgumentError`.

  ## Options

    * `:app_profile_id` - App profile to use

  ## Examples

      entries = [
        %{row_key: "row1", mutations: [MegasPinakas.set_cell("cf", "col", "val1")]},
        MegasPinakas.Row.new("row2") |> MegasPinakas.Row.put_string("cf", "col", "val2")
      ]
      {:ok, results} = MegasPinakas.mutate_rows("project", "instance", "table", entries)

  > #### Breaking change in 0.6.0 {: .warning}
  >
  > Previously returned an unconsumed gRPC stream despite documenting "a list of
  > results, one for each row". A caller who never enumerated that stream
  > silently discarded every per-entry failure. It now returns the list it always
  > claimed to.
  """
  @spec mutate_rows(String.t(), String.t(), String.t(), [map() | RowBuilder.t()], keyword()) ::
          {:ok, [MutateRowsResponse.Entry.t()]} | {:error, term()}
  def mutate_rows(project_id, instance_id, table_id, entries, opts \\ []) when is_list(entries) do
    request_entries = Enum.map(entries, &to_request_entry!/1)

    operation = fn channel ->
      request = %MutateRowsRequest{
        table_name: Config.table_path(project_id, instance_id, table_id),
        app_profile_id: Keyword.get(opts, :app_profile_id, ""),
        entries: request_entries
      }

      auth_opts = Auth.request_opts()

      case Stub.mutate_rows(channel, request, auth_opts) do
        {:ok, stream} -> collect_mutate_rows_stream(stream)
        other -> other
      end
    end

    Client.execute(operation)
  end

  # Runs outside the operation closure so a bad entry raises at the call site
  # instead of surfacing as a rescued `{:error, _}` from Client.execute/2.
  defp to_request_entry!(%RowBuilder{} = row) do
    row |> RowBuilder.to_entry() |> to_request_entry!()
  end

  defp to_request_entry!(%{} = entry) do
    row_key = Map.get(entry, :row_key, Map.get(entry, "row_key"))
    mutations = Map.get(entry, :mutations, Map.get(entry, "mutations"))

    if is_binary(row_key) and is_list(mutations) do
      %MutateRowsRequest.Entry{row_key: row_key, mutations: mutations}
    else
      raise ArgumentError,
            "mutate_rows entry must have a binary :row_key and a list of :mutations, " <>
              "got: #{inspect(entry)}"
    end
  end

  defp to_request_entry!(other) do
    raise ArgumentError,
          "mutate_rows entry must be a %MegasPinakas.Row{} or a map, got: #{inspect(other)}"
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
      )
      when is_binary(row_key) and is_list(true_mutations) and is_list(false_mutations) do
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
  def read_modify_write_row(project_id, instance_id, table_id, row_key, rules, opts \\ [])
      when is_binary(row_key) and is_list(rules) do
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

    * `:timestamp_micros` - Cell timestamp in microseconds. Defaults to `-1`,
      which lets the server assign the current time. BigTable tables store
      timestamps at millisecond granularity, so an explicit value must be a
      multiple of `1_000`; anything else is rejected by the server on write,
      so it raises `ArgumentError` here instead.

  ## Examples

      MegasPinakas.set_cell("column_family", "column_qualifier", "value")
      MegasPinakas.set_cell("cf", "col", "value", timestamp_micros: 1_234_567_890_000)
  """
  @spec set_cell(String.t(), binary(), binary(), keyword()) :: Mutation.t()
  def set_cell(family_name, column_qualifier, value, opts \\ []) do
    timestamp = validate_timestamp_micros!(Keyword.get(opts, :timestamp_micros, -1))

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

  defp validate_timestamp_micros!(-1), do: -1

  defp validate_timestamp_micros!(ts) when is_integer(ts) and ts >= 0 and rem(ts, 1000) == 0,
    do: ts

  defp validate_timestamp_micros!(other) do
    raise ArgumentError,
          ":timestamp_micros must be -1 or a non-negative multiple of 1000 " <>
            "(millisecond granularity), got: #{inspect(other)}"
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
  Creates a row range covering every key that starts with `prefix`.

  The end bound is the prefix with its last non-`0xFF` byte incremented. A
  prefix made entirely of `0xFF` bytes has no such successor, so the range is
  left open-ended (`end_key: nil`) rather than given an empty end key, which
  BigTable would read as "before every key" and return nothing. An empty
  prefix is the whole table.

  ## Examples

      MegasPinakas.row_range_prefix("user#")
      # => %RowRange{start_key: {:start_key_closed, "user#"}, end_key: {:end_key_open, "user$"}}
  """
  @spec row_range_prefix(binary()) :: RowRange.t()
  def row_range_prefix(prefix) when is_binary(prefix) do
    end_key =
      case calculate_prefix_end(prefix) do
        nil -> nil
        key -> {:end_key_open, key}
      end

    %RowRange{start_key: {:start_key_closed, prefix}, end_key: end_key}
  end

  @doc """
  Creates a row range with both keys exclusive (open).

  ## Examples

      MegasPinakas.row_range_open("user#100", "user#200")
  """
  @spec row_range_open(binary(), binary()) :: RowRange.t()
  def row_range_open(start_key, end_key) do
    %RowRange{
      start_key: {:start_key_open, start_key},
      end_key: {:end_key_open, end_key}
    }
  end

  @doc """
  Creates a row range with both keys inclusive (closed).

  ## Examples

      MegasPinakas.row_range_closed("user#100", "user#200")
  """
  @spec row_range_closed(binary(), binary()) :: RowRange.t()
  def row_range_closed(start_key, end_key) do
    %RowRange{
      start_key: {:start_key_closed, start_key},
      end_key: {:end_key_closed, end_key}
    }
  end

  @doc """
  Creates a row range with open start and closed end.

  ## Examples

      MegasPinakas.row_range_open_closed("user#100", "user#200")
  """
  @spec row_range_open_closed(binary(), binary()) :: RowRange.t()
  def row_range_open_closed(start_key, end_key) do
    %RowRange{
      start_key: {:start_key_open, start_key},
      end_key: {:end_key_closed, end_key}
    }
  end

  @doc """
  Creates a row range from start_key to the end of the table.

  ## Examples

      MegasPinakas.row_range_from("user#500")
  """
  @spec row_range_from(binary()) :: RowRange.t()
  def row_range_from(start_key) do
    %RowRange{
      start_key: {:start_key_closed, start_key},
      end_key: nil
    }
  end

  @doc """
  Creates a row range from the beginning of the table to end_key.

  ## Examples

      MegasPinakas.row_range_until("user#500")
  """
  @spec row_range_until(binary()) :: RowRange.t()
  def row_range_until(end_key) do
    %RowRange{
      start_key: nil,
      end_key: {:end_key_open, end_key}
    }
  end

  @doc """
  Creates an unbounded row range (all rows).

  ## Examples

      MegasPinakas.row_range_unbounded()
  """
  @spec row_range_unbounded() :: RowRange.t()
  def row_range_unbounded do
    %RowRange{
      start_key: nil,
      end_key: nil
    }
  end

  # ============================================================================
  # Filter Builders
  # ============================================================================

  # These are convenience re-exports of the most common builders in
  # `MegasPinakas.Filter`. They delegate rather than re-implement: when they were
  # separate copies, `column_filter/2` drifted and started emitting an unescaped,
  # unanchored qualifier regex, which silently over-matched columns.

  @doc """
  Creates a filter that matches a specific column.

  ## Examples

      MegasPinakas.column_filter("cf", "col")
  """
  @spec column_filter(String.t(), String.t()) :: RowFilter.t()
  defdelegate column_filter(family_name, column_qualifier), to: Filter

  @doc """
  Creates a filter that matches a column family.

  ## Examples

      MegasPinakas.family_filter("cf")
  """
  @spec family_filter(String.t()) :: RowFilter.t()
  defdelegate family_filter(family_name), to: Filter

  @doc """
  Creates a filter that limits cells per column.

  ## Examples

      MegasPinakas.cells_per_column_limit_filter(1)
  """
  @spec cells_per_column_limit_filter(pos_integer()) :: RowFilter.t()
  defdelegate cells_per_column_limit_filter(limit), to: Filter

  @doc """
  Creates a filter that passes all cells.

  ## Examples

      MegasPinakas.pass_all_filter()
  """
  @spec pass_all_filter() :: RowFilter.t()
  defdelegate pass_all_filter(), to: Filter

  @doc """
  Creates a filter that blocks all cells.

  ## Examples

      MegasPinakas.block_all_filter()
  """
  @spec block_all_filter() :: RowFilter.t()
  defdelegate block_all_filter(), to: Filter

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
  defdelegate chain_filters(filters), to: Filter

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
  defdelegate interleave_filters(filters), to: Filter

  # ============================================================================
  # Private Helpers
  # ============================================================================

  # Materializes a server-streaming response inside the request span.
  #
  # Returning the raw stream let the RPC fail *after* Client.execute/2 had already
  # reported success, and a caller who never enumerated it never learned anything
  # went wrong.
  defp collect_response_stream(stream) do
    Enum.reduce_while(stream, {:ok, []}, fn
      {:ok, response}, {:ok, acc} ->
        {:cont, {:ok, [response | acc]}}

      {:trailers, _trailers}, acc ->
        {:cont, acc}

      {:error, reason}, _acc ->
        Logger.warning("BigTable response stream error: #{inspect(reason)}")
        {:halt, {:error, {:incomplete_read, Response.normalize_reason(reason)}}}

      unexpected, _acc ->
        Logger.warning("BigTable unexpected stream element: #{inspect(unexpected)}")
        {:halt, {:error, {:unexpected_stream_element, unexpected}}}
    end)
    |> case do
      {:ok, responses} -> {:ok, Enum.reverse(responses)}
      {:error, reason} -> {:error, reason}
    end
  end

  # MutateRows splits its per-entry results across an arbitrary number of
  # responses, so flatten them and restore the caller's entry order.
  defp collect_mutate_rows_stream(stream) do
    case collect_response_stream(stream) do
      {:ok, responses} ->
        {:ok, responses |> Enum.flat_map(& &1.entries) |> Enum.sort_by(& &1.index)}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # Calculates the exclusive end key for a prefix scan by incrementing the last
  # byte that is not 0xFF, dropping any trailing 0xFF bytes. Returns `nil` when
  # every byte is 0xFF (or the prefix is empty): there is no key greater than
  # such a prefix, so the range must run to the end of the table.
  defp calculate_prefix_end(<<>>), do: nil

  defp calculate_prefix_end(prefix) do
    prefix_size = byte_size(prefix) - 1
    <<head::binary-size(^prefix_size), last_byte>> = prefix

    if last_byte == 255 do
      calculate_prefix_end(head)
    else
      <<head::binary, last_byte + 1>>
    end
  end

  # ===========================================================================
  # Row Data Helper Functions
  # ===========================================================================

  @doc """
  Converts a BigTable row to a simple nested map.

  Returns a map with the structure:
  `%{family => %{qualifier => value}}` where value is the most recent cell value.

  ## Examples

      iex> {:ok, row} = MegasPinakas.read_row(project, instance, table, "user#123")
      iex> MegasPinakas.row_to_map(row)
      %{"cf" => %{"name" => "John Doe", "email" => "john@example.com"}}

  """
  @spec row_to_map(Row.t()) :: map()
  def row_to_map(%Row{families: families}) do
    Map.new(families, fn %{name: family_name, columns: columns} ->
      {family_name, columns_to_map(columns)}
    end)
  end

  def row_to_map(nil), do: %{}

  @doc """
  Gets a single cell value from a row by family and qualifier.

  Returns the most recent value for the cell, or `nil` if not found.

  ## Examples

      iex> {:ok, row} = MegasPinakas.read_row(project, instance, table, "user#123")
      iex> MegasPinakas.get_cell(row, "cf", "name")
      "John Doe"

  """
  @spec get_cell(Row.t() | nil, String.t(), String.t()) :: binary() | nil
  def get_cell(%Row{families: families}, family, qualifier) do
    with %{columns: columns} <- Enum.find(families, &(&1.name == family)),
         %{cells: [%{value: value} | _]} <- Enum.find(columns, &(&1.qualifier == qualifier)) do
      value
    else
      _ -> nil
    end
  end

  def get_cell(nil, _family, _qualifier), do: nil

  @doc """
  Gets all cell versions for a column, including timestamps.

  Returns a list of `%{value: binary(), timestamp: integer()}` maps,
  sorted by timestamp descending (most recent first).

  ## Examples

      iex> {:ok, row} = MegasPinakas.read_row(project, instance, table, "user#123")
      iex> MegasPinakas.get_cells(row, "cf", "name")
      [%{value: "John Doe", timestamp: 1765323352546000}]

  """
  @spec get_cells(Row.t() | nil, String.t(), String.t()) :: [map()]
  def get_cells(%Row{families: families}, family, qualifier) do
    with %{columns: columns} <- Enum.find(families, &(&1.name == family)),
         %{cells: cells} <- Enum.find(columns, &(&1.qualifier == qualifier)) do
      Enum.map(cells, fn %{value: value, timestamp_micros: ts} ->
        %{value: value, timestamp: ts}
      end)
    else
      _ -> []
    end
  end

  def get_cells(nil, _family, _qualifier), do: []

  @doc """
  Gets all columns in a family as a map.

  Returns `%{qualifier => value}` for the most recent values.

  ## Examples

      iex> {:ok, row} = MegasPinakas.read_row(project, instance, table, "user#123")
      iex> MegasPinakas.get_family(row, "cf")
      %{"name" => "John Doe", "email" => "john@example.com"}

  """
  @spec get_family(Row.t() | nil, String.t()) :: map()
  def get_family(%Row{families: families}, family) do
    case Enum.find(families, &(&1.name == family)) do
      %{columns: columns} -> columns_to_map(columns)
      nil -> %{}
    end
  end

  def get_family(nil, _family), do: %{}

  @doc """
  Gets the row key from a row.

  ## Examples

      iex> {:ok, row} = MegasPinakas.read_row(project, instance, table, "user#123")
      iex> MegasPinakas.row_key(row)
      "user#123"

  """
  @spec row_key(Row.t() | nil) :: binary() | nil
  def row_key(%Row{key: key}), do: key
  def row_key(nil), do: nil

  @doc """
  Converts multiple rows to a list of maps.

  Each row becomes `%{key: row_key, data: %{family => %{qualifier => value}}}`.

  ## Examples

      iex> {:ok, rows} = MegasPinakas.read_rows(project, instance, table, rows: row_set)
      iex> MegasPinakas.rows_to_list(rows)
      [
        %{key: "user#1", data: %{"cf" => %{"name" => "Alice"}}},
        %{key: "user#2", data: %{"cf" => %{"name" => "Bob"}}}
      ]

  """
  @spec rows_to_list([Row.t()]) :: [map()]
  def rows_to_list(rows) when is_list(rows) do
    Enum.map(rows, fn row ->
      %{key: row_key(row), data: row_to_map(row)}
    end)
  end

  # Converts a list of columns to a map of qualifier => latest value
  defp columns_to_map(columns) do
    Map.new(columns, fn %{qualifier: qualifier, cells: cells} ->
      {qualifier, latest_cell_value(cells)}
    end)
  end

  defp latest_cell_value([%{value: v} | _]), do: v
  defp latest_cell_value([]), do: nil
end
