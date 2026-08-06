defmodule MegasPinakas.Streaming do
  @moduledoc """
  Streaming module for BigTable reads using Elixir Streams.

  Provides Stream-compatible iterators for reading large datasets efficiently
  without loading all data into memory at once.

  Compatible with `Stream.resource/3` and standard Stream operations.

  ## Examples

      # Stream all rows in a range
      MegasPinakas.Streaming.stream_rows(project, instance, "users",
        rows: MegasPinakas.row_set_from_ranges([MegasPinakas.row_range_prefix("user#")])
      )
      |> Stream.take(100)
      |> Enum.to_list()

      # Stream with transformation
      MegasPinakas.Streaming.stream_rows(project, instance, "metrics",
        rows: MegasPinakas.row_set_from_ranges([MegasPinakas.row_range_prefix("cpu:")])
      )
      |> Stream.map(fn row -> MegasPinakas.row_to_map(row) end)
      |> Stream.filter(fn data -> data["cf"]["value"] > 0.9 end)
      |> Enum.take(10)

      # Process in chunks
      MegasPinakas.Streaming.stream_rows(project, instance, "logs",
        rows: MegasPinakas.row_set_from_ranges([MegasPinakas.row_range_prefix("log:")])
      )
      |> Stream.chunk_every(100)
      |> Stream.each(fn chunk -> process_batch(chunk) end)
      |> Stream.run()
  """

  require Logger

  alias Google.Bigtable.V2.RowSet
  alias MegasPinakas

  # ============================================================================
  # Row Streaming
  # ============================================================================

  # Each batch is one ReadRows RPC that is fetched and drained in full.
  #
  # This is deliberate. A single long-lived server stream consumed lazily would
  # cost one RPC instead of ceil(N/batch_size), but abandoning such a stream
  # mid-iteration leaks a gRPC stream process and its buffered rows permanently —
  # measured at 250 processes and 1.28 GB for 250 abandoned streams. Reclaiming
  # them needs `GRPC.Stub.cancel/1`, which requires the `%GRPC.Client.Stream{}`
  # struct that `Stub.read_rows/3` does not return; killing the owning process
  # does not reap it either. Draining every batch is structurally leak-free and
  # needs no gRPC internals.
  #
  # The cost is round trips, so the default batch is large. Scanning 20 000 rows:
  #
  #     single RPC (eager read_rows)    56.4 ms    1 RPC
  #     batch_size 10_000               61.1 ms    2 RPCs   <- default
  #     batch_size  1_000              171.8 ms   20 RPCs   <- previous default
  #     batch_size    100              896.5 ms  200 RPCs
  #
  # 10_000 lands within 8% of the single-RPC ideal.
  @default_batch_size 10_000

  @doc """
  Creates a Stream that yields rows from BigTable.

  Rows are fetched in batches as the stream is consumed, so memory is bounded by
  `:batch_size` rather than by the size of the result set.

  ## Options

    * `:rows` - RowSet specifying which rows to read
    * `:filter` - RowFilter to apply
    * `:batch_size` - Rows to fetch per round trip (default: #{@default_batch_size})
    * `:rows_limit` - Maximum number of rows the stream will ever yield
    * `:app_profile_id` - App profile to use

  ## Choosing a batch size

  `:batch_size` trades round trips against peak memory: each batch is fetched in
  full before its rows are yielded, so the buffer holds up to `:batch_size` rows
  (roughly 3.4 KB per row in the benchmark fixture). Lower it when rows are large
  or when you expect to stop early; raise it for long scans.

  ## Telemetry

  Emits `[:megas_pinakas, :stream, :start]` on first demand, then exactly one of:

    * `[:megas_pinakas, :stream, :stop]` — the stream ran to exhaustion
    * `[:megas_pinakas, :stream, :cancelled]` — the consumer stopped early

  These are distinct from the per-batch `[:megas_pinakas, :request, :*]` spans; a
  lazily-consumed stream has no single duration a request span could represent.

  ## Examples

      # Stream all users
      stream = MegasPinakas.Streaming.stream_rows(project, instance, "users",
        rows: MegasPinakas.row_set_from_ranges([MegasPinakas.row_range_prefix("user#")])
      )

      # Take first 10
      stream |> Enum.take(10)

      # Count all
      stream |> Enum.count()

      # Filter and map
      stream
      |> Stream.map(&MegasPinakas.row_to_map/1)
      |> Stream.filter(fn data -> data["cf"]["active"] == true end)
      |> Enum.to_list()
  """
  @spec stream_rows(String.t(), String.t(), String.t(), keyword()) :: Enumerable.t()
  def stream_rows(project, instance, table, opts \\ []) do
    batch_size = Keyword.get(opts, :batch_size, @default_batch_size)

    # A `:rows_limit` of 0 means "no limit" in the ReadRows proto; normalize it so
    # it cannot be confused with "yield nothing".
    remaining =
      case Keyword.get(opts, :rows_limit) do
        nil -> :infinity
        0 -> :infinity
        limit when is_integer(limit) and limit > 0 -> limit
      end

    read_opts = Keyword.drop(opts, [:batch_size, :rows_limit])

    Stream.resource(
      fn -> init_stream(project, instance, table, read_opts, batch_size, remaining) end,
      fn state -> next_rows(state) end,
      fn state -> finish_stream(state) end
    )
  end

  @doc """
  Creates a Stream that yields rows as maps.

  Convenience wrapper that converts each row to a map using `MegasPinakas.row_to_map/1`.

  ## Examples

      MegasPinakas.Streaming.stream_rows_as_maps(project, instance, "users",
        rows: MegasPinakas.row_set_from_ranges([MegasPinakas.row_range_prefix("user#")])
      )
      |> Stream.filter(fn %{"cf" => cf} -> cf["status"] == "active" end)
      |> Enum.take(100)
  """
  @spec stream_rows_as_maps(String.t(), String.t(), String.t(), keyword()) :: Enumerable.t()
  def stream_rows_as_maps(project, instance, table, opts \\ []) do
    stream_rows(project, instance, table, opts)
    |> Stream.map(&MegasPinakas.row_to_map/1)
  end

  @doc """
  Creates a Stream that yields {row_key, data_map} tuples.

  Useful when you need both the row key and the data.

  ## Examples

      MegasPinakas.Streaming.stream_rows_with_keys(project, instance, "users",
        rows: MegasPinakas.row_set_from_ranges([MegasPinakas.row_range_prefix("user#")])
      )
      |> Enum.into(%{})  # Creates a map of row_key => data
  """
  @spec stream_rows_with_keys(String.t(), String.t(), String.t(), keyword()) :: Enumerable.t()
  def stream_rows_with_keys(project, instance, table, opts \\ []) do
    stream_rows(project, instance, table, opts)
    |> Stream.map(fn row ->
      {MegasPinakas.row_key(row), MegasPinakas.row_to_map(row)}
    end)
  end

  # ============================================================================
  # Row Range Streaming
  # ============================================================================

  @doc """
  Creates a Stream over a row key range.

  ## Examples

      # Stream rows from "a" to "z"
      MegasPinakas.Streaming.stream_range(project, instance, "table", "a", "z")
      |> Enum.to_list()

      # Stream with a prefix
      MegasPinakas.Streaming.stream_prefix(project, instance, "table", "user#")
      |> Enum.take(100)
  """
  @spec stream_range(String.t(), String.t(), String.t(), binary(), binary(), keyword()) ::
          Enumerable.t()
  def stream_range(project, instance, table, start_key, end_key, opts \\ []) do
    row_range = MegasPinakas.row_range(start_key, end_key)
    row_set = MegasPinakas.row_set_from_ranges([row_range])

    opts = Keyword.put(opts, :rows, row_set)
    stream_rows(project, instance, table, opts)
  end

  @doc """
  Creates a Stream over rows matching a key prefix.

  ## Examples

      MegasPinakas.Streaming.stream_prefix(project, instance, "users", "user#active:")
      |> Stream.map(&MegasPinakas.row_to_map/1)
      |> Enum.to_list()
  """
  @spec stream_prefix(String.t(), String.t(), String.t(), binary(), keyword()) :: Enumerable.t()
  def stream_prefix(project, instance, table, prefix, opts \\ []) do
    row_range = MegasPinakas.row_range_prefix(prefix)
    row_set = MegasPinakas.row_set_from_ranges([row_range])

    opts = Keyword.put(opts, :rows, row_set)
    stream_rows(project, instance, table, opts)
  end

  # ============================================================================
  # Chunked Operations
  # ============================================================================

  @doc """
  Streams rows and processes them in chunks, returning chunk results.

  Useful for batch processing with results accumulation.

  ## Examples

      MegasPinakas.Streaming.stream_in_chunks(project, instance, "users",
        [rows: row_set],
        chunk_size: 100,
        process_fn: fn chunk ->
          # Process batch and return count
          length(chunk)
        end
      )
      |> Enum.sum()  # Total rows processed
  """
  @spec stream_in_chunks(String.t(), String.t(), String.t(), keyword(), keyword()) ::
          Enumerable.t()
  def stream_in_chunks(project, instance, table, read_opts, opts) do
    chunk_size = Keyword.get(opts, :chunk_size, 100)
    process_fn = Keyword.get(opts, :process_fn, fn chunk -> chunk end)

    stream_rows(project, instance, table, read_opts)
    |> Stream.chunk_every(chunk_size)
    |> Stream.map(process_fn)
  end

  # ============================================================================
  # Stream Utilities
  # ============================================================================

  @doc """
  Counts rows in a stream without loading all data.

  More efficient than `Enum.count/1` as it doesn't need to keep rows in memory.

  ## Examples

      count = MegasPinakas.Streaming.count_rows(project, instance, "users",
        rows: MegasPinakas.row_set_from_ranges([MegasPinakas.row_range_prefix("user#")])
      )
  """
  @spec count_rows(String.t(), String.t(), String.t(), keyword()) :: non_neg_integer()
  def count_rows(project, instance, table, opts \\ []) do
    stream_rows(project, instance, table, opts)
    |> Enum.reduce(0, fn _row, acc -> acc + 1 end)
  end

  @doc """
  Checks if any rows exist matching the criteria.

  Stops as soon as one row is found.

  ## Examples

      exists? = MegasPinakas.Streaming.rows_exist?(project, instance, "users",
        rows: MegasPinakas.row_set_from_ranges([MegasPinakas.row_range_prefix("admin#")])
      )
  """
  @spec rows_exist?(String.t(), String.t(), String.t(), keyword()) :: boolean()
  def rows_exist?(project, instance, table, opts \\ []) do
    stream_rows(project, instance, table, opts)
    |> Enum.take(1)
    |> length() > 0
  end

  @doc """
  Gets the first row matching the criteria, if any.

  ## Examples

      case MegasPinakas.Streaming.first_row(project, instance, "users",
        rows: MegasPinakas.row_set_from_ranges([MegasPinakas.row_range_prefix("user#")])
      ) do
        {:ok, row} -> process_row(row)
        :none -> handle_empty()
      end
  """
  @spec first_row(String.t(), String.t(), String.t(), keyword()) :: {:ok, term()} | :none
  def first_row(project, instance, table, opts \\ []) do
    case stream_rows(project, instance, table, opts) |> Enum.take(1) do
      [row] -> {:ok, row}
      [] -> :none
    end
  end

  # ============================================================================
  # Private Helpers - Stream Implementation
  # ============================================================================

  defp init_stream(project, instance, table, opts, batch_size, remaining) do
    metadata = %{project: project, instance: instance, table: table, batch_size: batch_size}

    :telemetry.execute(
      [:megas_pinakas, :stream, :start],
      %{system_time: System.system_time()},
      metadata
    )

    %{
      project: project,
      instance: instance,
      table: table,
      opts: opts,
      batch_size: batch_size,
      remaining: remaining,
      buffer: [],
      last_key: nil,
      done: false,
      # For the closing telemetry event.
      metadata: metadata,
      start_time: System.monotonic_time(),
      emitted: 0,
      batches: 0
    }
  end

  # `Stream.resource/3` runs this on both natural exhaustion and early
  # termination, and does not say which. `done: true` is only ever set by a clause
  # that halted the stream itself, so it distinguishes the two.
  defp finish_stream(%{done: true} = state), do: emit_stream_event(:stop, state)
  defp finish_stream(state), do: emit_stream_event(:cancelled, state)

  defp emit_stream_event(event, state) do
    :telemetry.execute(
      [:megas_pinakas, :stream, event],
      %{
        duration: System.monotonic_time() - state.start_time,
        rows_emitted: state.emitted,
        batches: state.batches
      },
      state.metadata
    )

    :ok
  end

  defp next_rows(%{done: true} = state) do
    {:halt, state}
  end

  # The caller's :rows_limit is satisfied. Previously this option was overwritten
  # by :batch_size and silently ignored, so `stream_rows(..., rows_limit: 10)`
  # streamed the entire table.
  defp next_rows(%{remaining: 0} = state) do
    {:halt, %{state | done: true}}
  end

  defp next_rows(%{buffer: [row | rest]} = state) do
    {[row],
     %{
       state
       | buffer: rest,
         last_key: MegasPinakas.row_key(row),
         emitted: state.emitted + 1,
         remaining: decrement(state.remaining)
     }}
  end

  defp next_rows(%{buffer: []} = state) do
    # Need to fetch more rows
    case fetch_batch(state) do
      :exhausted ->
        {:halt, %{state | done: true}}

      {:ok, []} ->
        {:halt, %{state | done: true}}

      {:ok, [first | rest]} ->
        {[first],
         %{
           state
           | buffer: rest,
             last_key: MegasPinakas.row_key(first),
             emitted: state.emitted + 1,
             remaining: decrement(state.remaining),
             batches: state.batches + 1
         }}

      {:error, reason} ->
        # Raise rather than halt. Halting quietly would make `Enum.to_list/1`
        # return the rows fetched so far, which the caller cannot distinguish
        # from a complete result — the streaming form of the silent truncation
        # fixed in `read_rows/4`.
        Logger.warning("BigTable stream pagination error: #{inspect(reason)}")

        raise MegasPinakas.StreamError, reason: reason, last_key: state.last_key
    end
  end

  # The first request uses the caller's row set verbatim.
  defp fetch_batch(%{last_key: nil, opts: opts} = state) do
    read_batch(state, opts)
  end

  defp fetch_batch(%{last_key: last_key, opts: opts} = state) do
    case advance_row_set(Keyword.get(opts, :rows), last_key) do
      {:ok, row_set} -> read_batch(state, Keyword.put(opts, :rows, row_set))
      :exhausted -> :exhausted
    end
  end

  defp read_batch(
         %{
           project: project,
           instance: instance,
           table: table,
           batch_size: batch_size,
           remaining: remaining
         },
         opts
       ) do
    # Never request more than the caller's outstanding :rows_limit, so the last
    # batch of a limited stream does not fetch rows that will be thrown away.
    MegasPinakas.read_rows(
      project,
      instance,
      table,
      Keyword.put(opts, :rows_limit, batch_limit(batch_size, remaining))
    )
  end

  defp batch_limit(batch_size, :infinity), do: batch_size

  defp batch_limit(batch_size, remaining) when is_integer(remaining),
    do: min(batch_size, remaining)

  defp decrement(:infinity), do: :infinity
  defp decrement(n) when is_integer(n) and n > 0, do: n - 1

  # ============================================================================
  # Pagination cursor
  # ============================================================================

  # Advances a row set so the next request begins strictly after `last_key`.
  #
  # Returns `:exhausted` when no forward progress is possible, i.e. the previous
  # request already covered everything the row set can match. This distinction is
  # the whole point: the previous implementation fell through to "keep existing
  # row set" for any shape it did not recognise, which re-sent a byte-identical
  # request forever. A `row_keys`-only row set hit that path, so
  # `stream_rows(..., rows: row_set(["k1", "k2", "k3"]))` yielded those three rows
  # in an endless cycle and `count_rows/4` never returned.

  # No row set means "all rows", so the cursor is an open-ended range.
  defp advance_row_set(nil, last_key) do
    {:ok, MegasPinakas.row_set_from_ranges([MegasPinakas.row_range_from(last_key <> <<0>>)])}
  end

  # A RowSet with neither keys nor ranges also means "all rows".
  defp advance_row_set(%RowSet{row_keys: [], row_ranges: []}, last_key) do
    advance_row_set(nil, last_key)
  end

  defp advance_row_set(%RowSet{row_keys: keys, row_ranges: ranges}, last_key) do
    # Discrete keys have no cursor of their own; drop the ones already returned.
    # BigTable serves a row set in sorted key order, so anything at or below
    # `last_key` has been delivered.
    remaining_keys = Enum.filter(keys, &(&1 > last_key))
    remaining_ranges = advance_ranges(ranges, last_key)

    if remaining_keys == [] and remaining_ranges == [] do
      :exhausted
    else
      {:ok, %RowSet{row_keys: remaining_keys, row_ranges: remaining_ranges}}
    end
  end

  defp advance_row_set(other, _last_key) do
    raise ArgumentError, """
    Cannot paginate #{inspect(other)}.

    The :rows option must be a %Google.Bigtable.V2.RowSet{} (see \
    MegasPinakas.row_set/1 and MegasPinakas.row_set_from_ranges/1) or nil.
    """
  end

  # Drops ranges already fully consumed and raises the start bound of the rest.
  defp advance_ranges(ranges, last_key) do
    ranges
    |> Enum.reject(&range_consumed?(&1, last_key))
    |> Enum.map(&advance_range(&1, last_key))
  end

  defp range_consumed?(%{end_key: {:end_key_open, end_key}}, last_key), do: end_key <= last_key
  defp range_consumed?(%{end_key: {:end_key_closed, end_key}}, last_key), do: end_key <= last_key
  defp range_consumed?(_range, _last_key), do: false

  # Only ever raises the start bound. Rewriting a start that is already past
  # `last_key` would widen the range and re-read rows the caller never asked for.
  defp advance_range(%{start_key: start_key} = range, last_key) do
    if start_at_or_before?(start_key, last_key) do
      %{range | start_key: {:start_key_open, last_key}}
    else
      range
    end
  end

  defp start_at_or_before?(nil, _last_key), do: true
  defp start_at_or_before?({:start_key_closed, start_key}, last_key), do: start_key <= last_key
  defp start_at_or_before?({:start_key_open, start_key}, last_key), do: start_key <= last_key
end
