defmodule MegasPinakas.RowAssembler do
  @moduledoc """
  Assembles a BigTable `ReadRows` chunk stream into `Row` structs.

  `ReadRows` does not return rows. It returns a stream of `CellChunk`s that must
  be folded into rows by a small state machine, because a single row may span
  many chunks and a single chunk may complete a row.

  Two entry points share that state machine:

    * `reduce_all/1` — eager. Consumes the whole stream and returns
      `{:ok, [Row.t()]}` or `{:error, {:incomplete_read, reason}}`. Used by
      `MegasPinakas.read_rows/4`.
    * `stream_transform/1` — lazy. Emits each row as its `commit_row` chunk
      arrives, so a caller can process a large scan without materializing it.
      Used by `MegasPinakas.Streaming`.

  ## Chunk protocol

  Three parts of the wire protocol are easy to get wrong, because proto3 gives
  unset `bytes` fields the value `""` rather than `nil`:

    * **`row_key` appears only on a row's first chunk.** Later chunks carry `""`.
      Treating `""` as present assigns the empty key to every continuation chunk.
    * **`family_name` and `qualifier` are only sent when they change.** A second
      cell in the same family arrives with `family_name: nil`, so the current
      family and qualifier have to be carried in the state.
    * **A value split across chunks sets `value_size > 0`** on every chunk but
      the last. Each fragment must be concatenated into one cell, not recorded as
      a separate cell.

  The BigTable emulator exercises none of these: it sets `row_key` on every
  chunk and never splits values. They are covered by unit tests built from
  synthetic chunk sequences that mirror the documented protocol.
  """

  require Logger

  alias Google.Bigtable.V2.Cell
  alias Google.Bigtable.V2.Column
  alias Google.Bigtable.V2.Family
  alias Google.Bigtable.V2.ReadRowsResponse
  alias Google.Bigtable.V2.Row
  alias MegasPinakas.StreamError

  @type state :: %{
          row_key: binary() | nil,
          family: String.t() | nil,
          qualifier: binary() | nil,
          cells: [map()],
          partial: map() | nil
        }

  @empty_state %{row_key: nil, family: nil, qualifier: nil, cells: [], partial: nil}

  @doc """
  Returns the initial assembler state.
  """
  @spec new() :: state()
  def new, do: @empty_state

  # ==========================================================================
  # Eager
  # ==========================================================================

  @doc """
  Consumes an entire chunk stream into a list of rows.

  Returns `{:error, {:incomplete_read, reason}}` if the stream fails partway
  through, rather than `{:ok, partial_rows}` — a truncated result the caller
  cannot distinguish from a complete one.
  """
  @spec reduce_all(Enumerable.t()) :: {:ok, [Row.t()]} | {:error, term()}
  def reduce_all(stream) do
    case Enum.reduce_while(stream, {:ok, {[], new()}}, &reduce_element/2) do
      {:ok, {rows, _state}} -> {:ok, Enum.reverse(rows)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp reduce_element({:ok, %ReadRowsResponse{chunks: chunks}}, {:ok, {rows, state}}) do
    {:cont, {:ok, Enum.reduce(chunks, {rows, state}, &collect_chunk/2)}}
  end

  # Only emitted when `:return_headers` is set, which this library never does.
  # Skipped rather than treated as a failure so enabling it cannot break reads.
  defp reduce_element({:trailers, _trailers}, acc), do: {:cont, acc}

  defp reduce_element({:error, reason}, _acc) do
    Logger.warning("BigTable read_rows stream error: #{inspect(reason)}")
    {:halt, {:error, {:incomplete_read, reason}}}
  end

  # An unrecognised element means the response shape changed underneath us. Fail
  # loudly — continuing would drop data and report success.
  defp reduce_element(unexpected, _acc) do
    Logger.warning("BigTable read_rows unexpected chunk: #{inspect(unexpected)}")
    {:halt, {:error, {:unexpected_read_rows_chunk, unexpected}}}
  end

  defp collect_chunk(chunk, {rows, state}) do
    case apply_chunk(chunk, state) do
      {:row, row, next_state} -> {[row | rows], next_state}
      {:cont, next_state} -> {rows, next_state}
    end
  end

  # ==========================================================================
  # Lazy
  # ==========================================================================

  @doc """
  Wraps a chunk stream in a lazy stream of rows.

  Each row is emitted as soon as its `commit_row` chunk arrives, so memory is
  bounded by the largest single row rather than by the result set.

  Raises `MegasPinakas.StreamError` if the underlying stream fails. A lazy stream
  has no return value to carry an error tuple, and halting quietly would make
  `Enum.to_list/1` return partial data indistinguishable from a complete result.
  """
  @spec stream_transform(Enumerable.t()) :: Enumerable.t()
  def stream_transform(stream) do
    Stream.transform(stream, &new/0, &transform_element/2, & &1)
  end

  defp transform_element({:ok, %ReadRowsResponse{chunks: chunks}}, state) do
    {rows, next_state} = Enum.reduce(chunks, {[], state}, &collect_chunk/2)
    {Enum.reverse(rows), next_state}
  end

  defp transform_element({:trailers, _trailers}, state), do: {[], state}

  defp transform_element({:error, reason}, state) do
    raise StreamError, reason: reason, last_key: state.row_key
  end

  defp transform_element(unexpected, state) do
    raise StreamError,
      reason: {:unexpected_read_rows_chunk, unexpected},
      last_key: state.row_key
  end

  # ==========================================================================
  # Shared state machine
  # ==========================================================================

  @doc """
  Applies one `CellChunk` to the assembler state.

  Returns `{:row, row, next_state}` when the chunk commits a row, or
  `{:cont, next_state}` when the row is still being assembled.
  """
  @spec apply_chunk(struct(), state()) :: {:row, Row.t(), state()} | {:cont, state()}
  def apply_chunk(chunk, state) do
    case chunk.row_status do
      # Discards the partially-assembled row; the server will resend it.
      {:reset_row, true} ->
        {:cont, new()}

      {:commit_row, true} ->
        state = state |> track_row_key(chunk) |> accumulate(chunk)
        {:row, build_row(state.row_key, state.cells), new()}

      _other ->
        {:cont, state |> track_row_key(chunk) |> accumulate(chunk)}
    end
  end

  # `row_key` is only set on a row's first chunk; proto3 renders the rest as "".
  defp track_row_key(state, %{row_key: key}) when is_binary(key) and key != "" do
    %{state | row_key: key}
  end

  defp track_row_key(state, _chunk), do: state

  defp accumulate(state, chunk) do
    state = state |> track_family(chunk) |> track_qualifier(chunk)

    cond do
      # Continuation of a value split across chunks: concatenate, do not add a
      # second cell.
      state.partial != nil ->
        settle(state, %{state.partial | value: state.partial.value <> chunk.value}, chunk)

      # A bare commit marker carries no cell data of its own.
      marker_chunk?(chunk) ->
        state

      true ->
        settle(state, new_cell(state, chunk), chunk)
    end
  end

  # `value_size > 0` means more fragments of this value are still coming.
  defp settle(state, cell, %{value_size: value_size}) when value_size > 0 do
    %{state | partial: cell}
  end

  defp settle(state, cell, _chunk) do
    %{state | partial: nil, cells: [cell | state.cells]}
  end

  # Family and qualifier are only sent when they change, so they persist across
  # cells within a row.
  defp track_family(state, %{family_name: %{value: family}}), do: %{state | family: family}
  defp track_family(state, _chunk), do: state

  defp track_qualifier(state, %{qualifier: %{value: qualifier}}) do
    %{state | qualifier: qualifier}
  end

  defp track_qualifier(state, _chunk), do: state

  defp marker_chunk?(chunk) do
    chunk.family_name == nil and chunk.qualifier == nil and chunk.value in [nil, ""]
  end

  defp new_cell(state, chunk) do
    %{
      family: state.family,
      qualifier: state.qualifier,
      timestamp: chunk.timestamp_micros,
      value: chunk.value || "",
      labels: chunk.labels
    }
  end

  # Single pass: group cells by {family, qualifier} while reversing the
  # prepend-accumulated list, so cells stay in the order the server sent them
  # (newest timestamp first) and no O(n^2) concatenation is needed.
  defp build_row(key, cells) do
    grouped =
      Enum.reduce(cells, %{}, fn cell, acc ->
        bigtable_cell = %Cell{
          timestamp_micros: cell.timestamp,
          value: cell.value,
          labels: cell.labels || []
        }

        Map.update(acc, {cell.family, cell.qualifier}, [bigtable_cell], &[bigtable_cell | &1])
      end)

    families =
      grouped
      |> Enum.group_by(fn {{family, _qualifier}, _cells} -> family end)
      |> Enum.map(fn {family, entries} ->
        columns =
          Enum.map(entries, fn {{_family, qualifier}, column_cells} ->
            %Column{qualifier: qualifier, cells: column_cells}
          end)

        %Family{name: family, columns: columns}
      end)

    %Row{key: key, families: families}
  end
end
