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

  alias MegasPinakas

  # ============================================================================
  # Row Streaming
  # ============================================================================

  @doc """
  Creates a Stream that yields rows from BigTable.

  The stream reads rows lazily, fetching data in batches as needed.
  This is memory-efficient for large datasets.

  ## Options

    * `:rows` - RowSet specifying which rows to read
    * `:filter` - RowFilter to apply
    * `:batch_size` - Number of rows to fetch per batch (default: 1000)
    * `:app_profile_id` - App profile to use

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
    batch_size = Keyword.get(opts, :batch_size, 1000)
    read_opts = Keyword.drop(opts, [:batch_size])

    Stream.resource(
      fn -> init_stream(project, instance, table, read_opts, batch_size) end,
      fn state -> next_rows(state) end,
      fn _state -> :ok end
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
  @spec stream_range(String.t(), String.t(), String.t(), binary(), binary(), keyword()) :: Enumerable.t()
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
  @spec stream_in_chunks(String.t(), String.t(), String.t(), keyword(), keyword()) :: Enumerable.t()
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

  defp init_stream(project, instance, table, opts, batch_size) do
    %{
      project: project,
      instance: instance,
      table: table,
      opts: opts,
      batch_size: batch_size,
      buffer: [],
      last_key: nil,
      done: false
    }
  end

  defp next_rows(%{done: true} = state) do
    {:halt, state}
  end

  defp next_rows(%{buffer: [row | rest]} = state) do
    {[row], %{state | buffer: rest, last_key: MegasPinakas.row_key(row)}}
  end

  defp next_rows(%{buffer: []} = state) do
    # Need to fetch more rows
    case fetch_batch(state) do
      {:ok, []} ->
        {:halt, %{state | done: true}}

      {:ok, rows} ->
        case rows do
          [first | rest] ->
            {[first], %{state | buffer: rest, last_key: MegasPinakas.row_key(first)}}

          [] ->
            {:halt, %{state | done: true}}
        end

      {:error, _reason} ->
        {:halt, %{state | done: true}}
    end
  end

  defp fetch_batch(%{project: project, instance: instance, table: table, opts: opts, batch_size: batch_size, last_key: last_key}) do
    # Build updated opts with pagination
    read_opts =
      if last_key do
        # Modify row set to start after last_key
        case Keyword.get(opts, :rows) do
          nil ->
            # No rows specified, create range from last_key
            row_range = MegasPinakas.row_range_from(last_key <> <<0>>)
            Keyword.put(opts, :rows, MegasPinakas.row_set_from_ranges([row_range]))

          %Google.Bigtable.V2.RowSet{row_ranges: ranges} when length(ranges) > 0 ->
            # Update first range to start after last_key
            [first_range | rest] = ranges

            updated_range = %{first_range |
              start_key: {:start_key_open, last_key}
            }

            updated_row_set = %Google.Bigtable.V2.RowSet{
              row_keys: [],
              row_ranges: [updated_range | rest]
            }

            Keyword.put(opts, :rows, updated_row_set)

          row_set ->
            # Keep existing row set
            Keyword.put(opts, :rows, row_set)
        end
      else
        opts
      end

    read_opts = Keyword.put(read_opts, :rows_limit, batch_size)

    MegasPinakas.read_rows(project, instance, table, read_opts)
  end
end
