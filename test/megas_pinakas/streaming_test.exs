defmodule MegasPinakas.StreamingTest do
  # Not async: several tests below count or refute
  # [:megas_pinakas, :request, :start] events. Telemetry handlers are global, so
  # a concurrently running test that issues any RPC would trip those assertions.
  use ExUnit.Case, async: false

  alias Google.Bigtable.V2.Row
  alias MegasPinakas.StreamError
  alias MegasPinakas.Streaming
  alias MegasPinakas.Test.Emulator

  @table "streaming_streaming_test"
  @row_count 25

  setup do
    handler_id = {__MODULE__, System.unique_integer([:positive])}
    test_pid = self()

    :telemetry.attach_many(
      handler_id,
      [
        [:megas_pinakas, :request, :start],
        [:megas_pinakas, :stream, :start],
        [:megas_pinakas, :stream, :stop],
        [:megas_pinakas, :stream, :cancelled],
        [:megas_pinakas, :stream, :exception],
        [:megas_pinakas, :stream, :retry]
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

  describe "laziness" do
    test "constructing a stream issues no request" do
      _stream = Streaming.stream_rows("proj", "inst", "table")
      _stream = Streaming.stream_prefix("proj", "inst", "table", "user#")
      _stream = Streaming.stream_range("proj", "inst", "table", "a", "z")

      assert request_count() == 0
    end

    test "composing Stream operations issues no request" do
      Streaming.stream_rows("proj", "inst", "table")
      |> Stream.map(& &1)
      |> Stream.filter(fn _ -> true end)
      |> Stream.take(10)

      assert request_count() == 0
    end
  end

  describe "option validation" do
    test "batch_size must be a positive integer" do
      for bad <- [0, -1, 1.5, "10", nil] do
        assert_raise ArgumentError, ~r/:batch_size/, fn ->
          Streaming.stream_rows("proj", "inst", "table", batch_size: bad)
        end
      end
    end

    test "rows_limit must be a non-negative integer or nil" do
      for bad <- [-1, 2.0, "10", :all] do
        assert_raise ArgumentError, ~r/:rows_limit/, fn ->
          Streaming.stream_rows("proj", "inst", "table", rows_limit: bad)
        end
      end
    end

    test "max_retries must be a non-negative integer" do
      for bad <- [-1, 1.0, "3"] do
        assert_raise ArgumentError, ~r/:max_retries/, fn ->
          Streaming.stream_rows("proj", "inst", "table", max_retries: bad)
        end
      end
    end

    test "validation happens at construction, before any request" do
      assert_raise ArgumentError, fn ->
        Streaming.stream_rows("proj", "inst", "table", batch_size: 0)
      end

      assert request_count() == 0
    end
  end

  # These drive the retry path with a fake fetcher so it is deterministic and
  # needs no network. The fake has the signature of `MegasPinakas.read_rows/4`.
  describe "transient failure retry" do
    defp row(key), do: %Row{key: key, families: []}

    # Returns a fetcher that replays `responses` in order and records each call.
    defp scripted_reader(responses) do
      {:ok, agent} = Agent.start_link(fn -> {responses, 0} end)

      reader = fn _project, _instance, _table, _opts ->
        Agent.get_and_update(agent, fn
          {[response | rest], calls} -> {response, {rest, calls + 1}}
          {[], calls} -> {{:ok, []}, {[], calls + 1}}
        end)
      end

      {reader, fn -> Agent.get(agent, &elem(&1, 1)) end}
    end

    test "re-issues a batch that fails with :unavailable and yields its rows" do
      {reader, calls} =
        scripted_reader([
          {:error, {:unavailable, "try again"}},
          {:error, {:unavailable, "try again"}},
          {:ok, [row("k1"), row("k2")]}
        ])

      rows =
        Streaming.stream_rows("proj", "inst", "table", read_fun: reader, batch_size: 10)
        |> Enum.to_list()

      assert Enum.map(rows, & &1.key) == ["k1", "k2"]
      assert calls.() == 3

      retries = stream_events() |> Enum.filter(&(elem(&1, 1) == :retry))

      assert Enum.map(retries, fn {_, _, %{attempt: attempt}, _} -> attempt end) == [1, 2]

      assert Enum.all?(retries, fn {_, _, _, metadata} ->
               metadata.reason == {:unavailable, "try again"} and metadata.table == "table"
             end)
    end

    test "retries :deadline_exceeded, :aborted and an :incomplete_read wrapping one" do
      {reader, calls} =
        scripted_reader([
          {:error, {:deadline_exceeded, "slow"}},
          {:error, {:aborted, "conflict"}},
          {:error, {:incomplete_read, {:unavailable, "mid-stream"}}},
          {:ok, [row("k1")]}
        ])

      rows =
        Streaming.stream_rows("proj", "inst", "table", read_fun: reader, batch_size: 10)
        |> Enum.to_list()

      assert Enum.map(rows, & &1.key) == ["k1"]
      assert calls.() == 4
    end

    test "gives up after :max_retries and raises with the last reason" do
      {reader, calls} =
        scripted_reader([
          {:error, {:unavailable, "1"}},
          {:error, {:unavailable, "2"}},
          {:ok, [row("never")]}
        ])

      error =
        assert_raise StreamError, fn ->
          Streaming.stream_rows("proj", "inst", "table", read_fun: reader, max_retries: 1)
          |> Enum.to_list()
        end

      assert error.reason == {:unavailable, "2"}
      assert error.last_key == nil
      assert calls.() == 2
    end

    test "max_retries: 0 disables retrying" do
      {reader, calls} = scripted_reader([{:error, {:unavailable, "x"}}, {:ok, [row("k1")]}])

      assert_raise StreamError, fn ->
        Streaming.stream_rows("proj", "inst", "table", read_fun: reader, max_retries: 0)
        |> Enum.to_list()
      end

      assert calls.() == 1
      refute Enum.any?(stream_events(), &(elem(&1, 1) == :retry))
    end

    test "a non-transient status is not retried" do
      {reader, calls} =
        scripted_reader([{:error, {:permission_denied, "no"}}, {:ok, [row("k1")]}])

      error =
        assert_raise StreamError, fn ->
          Streaming.stream_rows("proj", "inst", "table", read_fun: reader) |> Enum.to_list()
        end

      assert error.reason == {:permission_denied, "no"}
      assert calls.() == 1
    end

    test "a failure after rows were delivered reports the last delivered key" do
      {reader, _calls} =
        scripted_reader([
          {:ok, [row("k1"), row("k2")]},
          {:error, {:internal, "boom"}}
        ])

      error =
        assert_raise StreamError, ~r/after delivering row "k2"/, fn ->
          Streaming.stream_rows("proj", "inst", "table", read_fun: reader, batch_size: 2)
          |> Enum.to_list()
        end

      assert error.last_key == "k2"
    end

    test "a batch shorter than requested ends the stream without another fetch" do
      {reader, calls} = scripted_reader([{:ok, [row("k1"), row("k2")]}])

      rows =
        Streaming.stream_rows("proj", "inst", "table", read_fun: reader, batch_size: 5)
        |> Enum.to_list()

      assert length(rows) == 2
      assert calls.() == 1
    end

    test "a full batch is followed by exactly one more fetch to confirm exhaustion" do
      {reader, calls} = scripted_reader([{:ok, [row("k1"), row("k2")]}, {:ok, []}])

      rows =
        Streaming.stream_rows("proj", "inst", "table", read_fun: reader, batch_size: 2)
        |> Enum.to_list()

      assert length(rows) == 2
      assert calls.() == 2
    end
  end

  describe "against the emulator" do
    @describetag :emulator

    # Per-test rather than setup_all: the latter cannot live inside a describe,
    # and hoisting it to the module would demand the emulator for the pure tests.
    setup do
      Emulator.setup_table(@table, ["cf"])
      Emulator.seed_rows(@table, @row_count, prefix: "st#")
      # Seeding issued RPCs of its own; drop their events before the test counts.
      _ = events()
      :ok
    end

    defp key(i), do: Emulator.row_key("st#", i)

    test "rows_exist?/4 is true for a populated prefix and false for an empty one" do
      assert Streaming.rows_exist?(Emulator.project(), Emulator.instance(), @table,
               rows: MegasPinakas.row_set_from_ranges([MegasPinakas.row_range_prefix("st#")])
             )

      refute Streaming.rows_exist?(Emulator.project(), Emulator.instance(), @table,
               rows: MegasPinakas.row_set_from_ranges([MegasPinakas.row_range_prefix("zz#")])
             )
    end

    test "rows_exist?/4 issues a single request for a single row" do
      assert Streaming.rows_exist?(Emulator.project(), Emulator.instance(), @table)

      # events/0 drains the mailbox, so capture once.
      captured = events()

      assert Enum.count(captured, &(elem(&1, 0) == :request)) == 1
      assert [{:stream, :start, _, %{batch_size: 1}} | _] = captured
    end

    test "first_row/4 returns the lowest matching key in one request" do
      assert {:ok, row} =
               Streaming.first_row(Emulator.project(), Emulator.instance(), @table,
                 rows: MegasPinakas.row_set_from_ranges([MegasPinakas.row_range_from(key(7))])
               )

      assert MegasPinakas.row_key(row) == key(7)
      assert request_count() == 1
    end

    test "first_row/4 returns :none when nothing matches" do
      assert :none ==
               Streaming.first_row(Emulator.project(), Emulator.instance(), @table,
                 rows: MegasPinakas.row_set(["absent"])
               )
    end

    test "stream_rows_as_maps/4 yields family => qualifier => value maps" do
      maps =
        Streaming.stream_rows_as_maps(Emulator.project(), Emulator.instance(), @table,
          rows: MegasPinakas.row_set([key(0), key(1)])
        )
        |> Enum.to_list()

      assert maps == [%{"cf" => %{"n" => "0"}}, %{"cf" => %{"n" => "1"}}]
    end

    test "stream_rows_with_keys/4 yields {row_key, map} tuples" do
      pairs =
        Streaming.stream_rows_with_keys(Emulator.project(), Emulator.instance(), @table,
          rows: MegasPinakas.row_set([key(2), key(3)])
        )
        |> Enum.to_list()

      assert pairs == [{key(2), %{"cf" => %{"n" => "2"}}}, {key(3), %{"cf" => %{"n" => "3"}}}]
    end

    test "stream_in_chunks/5 applies process_fn to each chunk" do
      sizes =
        Streaming.stream_in_chunks(
          Emulator.project(),
          Emulator.instance(),
          @table,
          [batch_size: 4],
          chunk_size: 10,
          process_fn: &length/1
        )
        |> Enum.to_list()

      assert sizes == [10, 10, 5]
    end

    test "count_rows/4 counts every row and strips values from the wire" do
      assert Streaming.count_rows(Emulator.project(), Emulator.instance(), @table) == @row_count

      # The default counting filter must not change which rows are counted.
      assert Streaming.count_rows(Emulator.project(), Emulator.instance(), @table,
               filter: MegasPinakas.Filter.pass_all_filter()
             ) == @row_count
    end

    test "a failing batch raises StreamError and emits :exception then :cancelled" do
      error =
        assert_raise StreamError, fn ->
          Streaming.stream_rows(
            Emulator.project(),
            Emulator.instance(),
            "streaming_no_such_table"
          )
          |> Enum.to_list()
        end

      assert {:not_found, _message} = error.reason
      assert error.last_key == nil

      captured = stream_events()

      assert [:start, :exception, :cancelled] = Enum.map(captured, &elem(&1, 1))

      [
        {_, :start, _, start_meta},
        {_, :exception, exc_meas, exc_meta},
        {_, :cancelled, _, c_meta}
      ] =
        captured

      assert is_reference(start_meta.stream_ref)
      assert exc_meta.stream_ref == start_meta.stream_ref
      assert c_meta.stream_ref == start_meta.stream_ref
      assert exc_meta.reason == error.reason
      assert exc_meas.rows_emitted == 0
      assert exc_meas.batches == 0
      assert is_integer(exc_meas.duration)
    end
  end
end
