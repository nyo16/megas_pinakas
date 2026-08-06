defmodule MegasPinakas.StreamTelemetryTest do
  @moduledoc """
  Covers the `[:megas_pinakas, :stream, :*]` events and the `:rows_limit` /
  `:batch_size` options on `MegasPinakas.Streaming.stream_rows/4`.

  A lazily-consumed stream has no single duration a request span could
  represent — the consumer sets the pace and may abandon it — so streams get
  their own event pair, plus a distinct `:cancelled` for early termination.
  """

  use ExUnit.Case, async: false

  alias MegasPinakas.Streaming
  alias MegasPinakas.Test.Emulator

  @moduletag :emulator

  @table "stream_telemetry_test"
  @row_count 25

  setup_all do
    Emulator.setup_table(@table, ["cf"])
    Emulator.seed_rows(@table, @row_count, prefix: "st#")
    :ok
  end

  setup do
    handler_id = {__MODULE__, System.unique_integer([:positive])}
    test_pid = self()

    :telemetry.attach_many(
      handler_id,
      [
        [:megas_pinakas, :stream, :start],
        [:megas_pinakas, :stream, :stop],
        [:megas_pinakas, :stream, :cancelled],
        [:megas_pinakas, :request, :start]
      ],
      fn [_, kind, event], measurements, metadata, _config ->
        send(test_pid, {:telemetry, kind, event, measurements, metadata})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)
    :ok
  end

  defp events do
    receive do
      {:telemetry, kind, event, measurements, metadata} ->
        [{kind, event, measurements, metadata} | events()]
    after
      0 -> []
    end
  end

  defp stream_events, do: Enum.filter(events(), &(elem(&1, 0) == :stream))

  defp request_count do
    events() |> Enum.count(&(elem(&1, 0) == :request and elem(&1, 1) == :start))
  end

  describe ":start" do
    test "is not emitted until the stream is consumed" do
      _stream = Streaming.stream_rows(Emulator.project(), Emulator.instance(), @table)

      assert stream_events() == []
    end

    test "carries the table and batch size" do
      Streaming.stream_rows(Emulator.project(), Emulator.instance(), @table, batch_size: 7)
      |> Enum.to_list()

      assert [{:stream, :start, _, metadata} | _] = stream_events()

      assert metadata.table == @table
      assert metadata.project == Emulator.project()
      assert metadata.instance == Emulator.instance()
      assert metadata.batch_size == 7
    end
  end

  describe ":stop vs :cancelled" do
    test "a fully consumed stream emits :stop" do
      rows =
        Streaming.stream_rows(Emulator.project(), Emulator.instance(), @table) |> Enum.to_list()

      assert length(rows) == @row_count

      kinds = stream_events() |> Enum.map(&elem(&1, 1))

      assert :start in kinds
      assert :stop in kinds
      refute :cancelled in kinds
    end

    test "a stream abandoned early emits :cancelled" do
      rows =
        Streaming.stream_rows(Emulator.project(), Emulator.instance(), @table) |> Enum.take(3)

      assert length(rows) == 3

      kinds = stream_events() |> Enum.map(&elem(&1, 1))

      assert :start in kinds
      assert :cancelled in kinds
      refute :stop in kinds
    end

    test "exactly one closing event is emitted" do
      Streaming.stream_rows(Emulator.project(), Emulator.instance(), @table) |> Enum.to_list()

      closing = stream_events() |> Enum.filter(&(elem(&1, 1) in [:stop, :cancelled]))

      assert length(closing) == 1
    end

    test ":stop reports rows emitted and duration" do
      Streaming.stream_rows(Emulator.project(), Emulator.instance(), @table) |> Enum.to_list()

      assert [{:stream, :stop, measurements, _}] =
               stream_events() |> Enum.filter(&(elem(&1, 1) == :stop))

      assert measurements.rows_emitted == @row_count
      assert is_integer(measurements.duration)
      assert measurements.batches >= 1
    end

    test ":cancelled reports only the rows actually delivered" do
      Streaming.stream_rows(Emulator.project(), Emulator.instance(), @table) |> Enum.take(4)

      assert [{:stream, :cancelled, measurements, _}] =
               stream_events() |> Enum.filter(&(elem(&1, 1) == :cancelled))

      assert measurements.rows_emitted == 4
    end

    test "an empty result still emits :stop" do
      Streaming.stream_prefix(Emulator.project(), Emulator.instance(), @table, "absent#")
      |> Enum.to_list()

      # stream_events/0 drains the mailbox, so capture once.
      captured = stream_events()

      assert :start in Enum.map(captured, &elem(&1, 1))

      assert [{:stream, :stop, %{rows_emitted: 0}, _}] =
               Enum.filter(captured, &(elem(&1, 1) == :stop))
    end
  end

  describe ":rows_limit" do
    # Previously :rows_limit was overwritten by :batch_size and silently ignored,
    # so a limited stream scanned the whole table.
    test "caps the number of rows yielded" do
      rows =
        Streaming.stream_rows(Emulator.project(), Emulator.instance(), @table, rows_limit: 10)
        |> Enum.to_list()

      assert length(rows) == 10
    end

    test "is honoured when smaller than batch_size" do
      rows =
        Streaming.stream_rows(Emulator.project(), Emulator.instance(), @table,
          rows_limit: 3,
          batch_size: 1_000
        )
        |> Enum.to_list()

      assert length(rows) == 3
    end

    test "is honoured when larger than batch_size" do
      rows =
        Streaming.stream_rows(Emulator.project(), Emulator.instance(), @table,
          rows_limit: 12,
          batch_size: 5
        )
        |> Enum.to_list()

      assert length(rows) == 12
      assert rows |> Enum.map(&MegasPinakas.row_key/1) |> Enum.uniq() |> length() == 12
    end

    test "a limit above the row count yields everything and terminates" do
      rows =
        Streaming.stream_rows(Emulator.project(), Emulator.instance(), @table, rows_limit: 1_000)
        |> Enum.to_list()

      assert length(rows) == @row_count
    end

    test "rows_limit: 0 means unlimited, matching the ReadRows proto" do
      rows =
        Streaming.stream_rows(Emulator.project(), Emulator.instance(), @table, rows_limit: 0)
        |> Enum.to_list()

      assert length(rows) == @row_count
    end

    test "a limited stream stops issuing requests once satisfied" do
      Streaming.stream_rows(Emulator.project(), Emulator.instance(), @table,
        rows_limit: 4,
        batch_size: 2
      )
      |> Enum.to_list()

      # 4 rows at 2 per batch is 2 requests; allow one extra for the terminating
      # probe, but not a full-table scan.
      assert request_count() <= 3
    end
  end

  describe ":batch_size" do
    test "defaults to 10_000, so a small table needs one request" do
      Streaming.stream_rows(Emulator.project(), Emulator.instance(), @table) |> Enum.to_list()

      # One request returns all 25 rows; a second confirms exhaustion.
      assert request_count() <= 2
    end

    test "a smaller batch size costs more requests for the same rows" do
      Streaming.stream_rows(Emulator.project(), Emulator.instance(), @table, batch_size: 5)
      |> Enum.to_list()

      assert request_count() >= 5
    end

    test "batch size does not change the rows returned" do
      for batch_size <- [1, 3, 25, 1_000] do
        rows =
          Streaming.stream_rows(Emulator.project(), Emulator.instance(), @table,
            batch_size: batch_size
          )
          |> Enum.to_list()
          |> Enum.map(&MegasPinakas.row_key/1)

        assert length(rows) == @row_count,
               "batch_size: #{batch_size} yielded #{length(rows)} rows"

        assert rows == Enum.sort(rows)
        assert rows == Enum.uniq(rows)
      end
    end
  end
end
