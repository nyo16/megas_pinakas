defmodule MegasPinakas.StreamingTest do
  # Not async: the laziness tests below assert that *no*
  # [:megas_pinakas, :request, :start] event fires. Telemetry handlers are global,
  # so a concurrently running test that issues any RPC would trip that assertion.
  use ExUnit.Case, async: false

  alias MegasPinakas.Streaming

  # Each constructor must return a lazy enumerable, not a materialized list.
  #
  # The two shapes below are the only ones the implementation can legitimately
  # produce: `Stream.resource/3` returns a reducer function, while anything
  # composed with `Stream.map/2` and friends returns a `%Stream{}` struct.
  # A materialized result resolves to `Enumerable.List`, so this check fails
  # loudly if laziness is ever lost.
  #
  # The previous `assert Enumerable.impl_for(stream) != nil` compared disjoint
  # types and passed regardless of what the function returned.
  @lazy_impls [Enumerable.Stream, Enumerable.Function]

  defp assert_lazy(stream) do
    refute is_list(stream), "expected a lazy enumerable, got a materialized list"
    assert Enumerable.impl_for(stream) in @lazy_impls
  end

  describe "stream_rows/4" do
    test "returns a lazy enumerable" do
      assert_lazy(Streaming.stream_rows("proj", "inst", "table"))
    end
  end

  describe "stream_rows_as_maps/4" do
    test "returns a lazy enumerable" do
      assert_lazy(Streaming.stream_rows_as_maps("proj", "inst", "table"))
    end
  end

  describe "stream_rows_with_keys/4" do
    test "returns a lazy enumerable" do
      assert_lazy(Streaming.stream_rows_with_keys("proj", "inst", "table"))
    end
  end

  describe "stream_range/6" do
    test "returns a lazy enumerable" do
      assert_lazy(Streaming.stream_range("proj", "inst", "table", "a", "z"))
    end
  end

  describe "stream_prefix/5" do
    test "returns a lazy enumerable" do
      assert_lazy(Streaming.stream_prefix("proj", "inst", "table", "user#"))
    end
  end

  describe "stream_in_chunks/5" do
    test "returns a lazy enumerable" do
      assert_lazy(Streaming.stream_in_chunks("proj", "inst", "table", [], chunk_size: 100))
    end
  end

  describe "laziness" do
    setup do
      handler_id = {__MODULE__, System.unique_integer([:positive])}
      test_pid = self()

      :telemetry.attach(
        handler_id,
        [:megas_pinakas, :request, :start],
        fn _event, _measurements, _metadata, _config -> send(test_pid, :request_started) end,
        nil
      )

      on_exit(fn -> :telemetry.detach(handler_id) end)
      :ok
    end

    test "constructing a stream issues no request" do
      _stream = Streaming.stream_rows("proj", "inst", "table")
      _stream = Streaming.stream_prefix("proj", "inst", "table", "user#")
      _stream = Streaming.stream_range("proj", "inst", "table", "a", "z")

      refute_received :request_started
    end

    test "composing Stream operations issues no request" do
      Streaming.stream_rows("proj", "inst", "table")
      |> Stream.map(& &1)
      |> Stream.filter(fn _ -> true end)
      |> Stream.take(10)

      refute_received :request_started
    end
  end

  describe "module structure" do
    test "exports stream_rows function" do
      functions = Streaming.__info__(:functions)
      assert {:stream_rows, 3} in functions
      assert {:stream_rows, 4} in functions
    end

    test "exports stream_rows_as_maps function" do
      functions = Streaming.__info__(:functions)
      assert {:stream_rows_as_maps, 3} in functions
      assert {:stream_rows_as_maps, 4} in functions
    end

    test "exports stream_rows_with_keys function" do
      functions = Streaming.__info__(:functions)
      assert {:stream_rows_with_keys, 3} in functions
      assert {:stream_rows_with_keys, 4} in functions
    end

    test "exports stream_range function" do
      functions = Streaming.__info__(:functions)
      assert {:stream_range, 5} in functions
      assert {:stream_range, 6} in functions
    end

    test "exports stream_prefix function" do
      functions = Streaming.__info__(:functions)
      assert {:stream_prefix, 4} in functions
      assert {:stream_prefix, 5} in functions
    end

    test "exports stream_in_chunks function" do
      functions = Streaming.__info__(:functions)
      assert {:stream_in_chunks, 5} in functions
    end

    test "exports count_rows function" do
      functions = Streaming.__info__(:functions)
      assert {:count_rows, 3} in functions
      assert {:count_rows, 4} in functions
    end

    test "exports rows_exist? function" do
      functions = Streaming.__info__(:functions)
      assert {:rows_exist?, 3} in functions
      assert {:rows_exist?, 4} in functions
    end

    test "exports first_row function" do
      functions = Streaming.__info__(:functions)
      assert {:first_row, 3} in functions
      assert {:first_row, 4} in functions
    end
  end
end
