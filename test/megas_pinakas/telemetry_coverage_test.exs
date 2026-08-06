defmodule MegasPinakas.TelemetryCoverageTest do
  @moduledoc """
  Asserts the `[:megas_pinakas, :request, :stop]` span actually covers the work
  the caller waits for.

  Before this was fixed, `read_rows/4` consumed the gRPC server stream *after*
  `Client.execute/2` returned, so the reported duration measured only the time
  to obtain the stream handle. Coverage measured 7.7% at 20k rows: 6.34 ms
  reported against 82.35 ms of real time, and the reported figure stayed flat
  at ~7 ms while real time grew 8x with result size.
  """

  use ExUnit.Case, async: false

  alias MegasPinakas.Test.Emulator

  @moduletag :emulator

  @table "telemetry_coverage_test"
  # The unary control writes a row, so it gets its own table — otherwise it
  # would change the row count the read assertions depend on.
  @control_table "telemetry_coverage_control"
  @row_count 10_000

  # The span should account for nearly all the wall clock the caller waits for.
  # Allowed slack covers request building and telemetry overhead outside the span.
  @min_coverage 0.80

  setup_all do
    Emulator.setup_table(@table, ["cf"])
    Emulator.seed_rows(@table, @row_count, prefix: "tel#")
    Emulator.setup_table(@control_table, ["cf"])
    :ok
  end

  setup do
    handler_id = {__MODULE__, System.unique_integer([:positive])}
    test_pid = self()

    :telemetry.attach_many(
      handler_id,
      [
        [:megas_pinakas, :request, :start],
        [:megas_pinakas, :request, :stop],
        [:megas_pinakas, :request, :exception]
      ],
      fn event, measurements, metadata, _config ->
        send(test_pid, {:telemetry, event, measurements, metadata})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)
    :ok
  end

  # Runs `fun`, returning {result, measured_native, reported_native}.
  defp measure(fun) do
    started = System.monotonic_time()
    result = fun.()
    measured = System.monotonic_time() - started

    assert_received {:telemetry, [:megas_pinakas, :request, :start], _, _}

    reported =
      receive do
        {:telemetry, [:megas_pinakas, :request, :stop], %{duration: duration}, _} ->
          duration

        {:telemetry, [:megas_pinakas, :request, :exception], _, metadata} ->
          flunk("expected a successful :stop event, got :exception #{inspect(metadata)}")
      after
        0 -> flunk("no [:megas_pinakas, :request, :stop] event was emitted")
      end

    {result, measured, reported}
  end

  defp coverage(measured, reported), do: reported / measured

  describe "read_rows/4 span coverage" do
    test "reported duration covers stream consumption" do
      {{:ok, rows}, measured, reported} =
        measure(fn ->
          MegasPinakas.read_rows(Emulator.project(), Emulator.instance(), @table)
        end)

      assert length(rows) == @row_count

      ratio = coverage(measured, reported)

      assert ratio >= @min_coverage, """
      Telemetry span covered only #{Float.round(ratio * 100, 1)}% of the call.

        reported: #{System.convert_time_unit(reported, :native, :microsecond)} us
        measured: #{System.convert_time_unit(measured, :native, :microsecond)} us
        rows:     #{length(rows)}

      This means stream consumption happens outside the span.
      """
    end

    test "coverage does not degrade as the result set grows" do
      # This is the shape of the bug, not just its magnitude: a span that
      # excludes consumption looks fine on tiny reads (97.8% at 100 rows) and
      # collapses as results grow (7.7% at 20k), because the reported duration
      # stays flat while real time scales with row count.
      measurements =
        for limit <- [100, 1_000, @row_count] do
          {{:ok, rows}, measured, reported} =
            measure(fn ->
              MegasPinakas.read_rows(Emulator.project(), Emulator.instance(), @table,
                rows_limit: limit
              )
            end)

          assert length(rows) == limit
          {limit, coverage(measured, reported)}
        end

      failing = for {limit, ratio} <- measurements, ratio < @min_coverage, do: {limit, ratio}

      assert failing == [], """
      Span coverage fell below #{trunc(@min_coverage * 100)}% for some result sizes:

      #{Enum.map_join(measurements, "\n", fn {limit, ratio} -> "  #{String.pad_leading(Integer.to_string(limit), 6)} rows: #{Float.round(ratio * 100, 1)}%" end)}

      Coverage that shrinks with result size means the span measures only the
      time to obtain the stream handle, not the time to consume it.
      """
    end
  end

  describe "mutate_row/6 span coverage (unary control)" do
    test "reported duration covers the unary call" do
      {{:ok, _}, measured, reported} =
        measure(fn ->
          MegasPinakas.mutate_row(
            Emulator.project(),
            Emulator.instance(),
            @control_table,
            "tel#control",
            [MegasPinakas.set_cell("cf", "n", "1")]
          )
        end)

      ratio = coverage(measured, reported)

      assert ratio >= @min_coverage,
             "unary span coverage regressed to #{Float.round(ratio * 100, 1)}%"
    end
  end
end
