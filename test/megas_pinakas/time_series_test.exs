defmodule MegasPinakas.TimeSeriesTest do
  use ExUnit.Case, async: true

  alias MegasPinakas.TimeSeries

  describe "reverse_timestamp/1" do
    test "converts DateTime to a 19-digit reverse timestamp" do
      assert TimeSeries.reverse_timestamp(~U[2024-01-15 10:00:00.000000Z]) ==
               "0008294687199999999"
    end

    test "earlier timestamps produce larger reverse timestamps" do
      earlier = ~U[2024-01-01 00:00:00Z]
      later = ~U[2024-12-31 23:59:59Z]

      assert TimeSeries.reverse_timestamp(earlier) > TimeSeries.reverse_timestamp(later)
    end

    test "preserves microsecond precision" do
      dt = ~U[2024-01-15 10:00:00.123456Z]
      reverse = TimeSeries.reverse_timestamp(dt)

      assert TimeSeries.from_reverse_timestamp(reverse) == {:ok, dt}
    end
  end

  describe "from_reverse_timestamp/1" do
    test "converts reverse timestamp back to DateTime" do
      assert TimeSeries.from_reverse_timestamp("0008294687199999999") ==
               {:ok, ~U[2024-01-15 10:00:00.000000Z]}
    end

    test "returns error for invalid format" do
      assert {:error, :invalid_format} = TimeSeries.from_reverse_timestamp("not_a_number")
    end
  end

  describe "time_series_row_key/2" do
    test "builds row key with metric_id and reverse timestamp" do
      assert TimeSeries.time_series_row_key("cpu:server1", ~U[2024-01-15 10:00:00Z]) ==
               "cpu:server1#0008294687199999999"
    end

    test "row keys for same metric sort by recency" do
      earlier = ~U[2024-01-01 00:00:00Z]
      later = ~U[2024-12-31 23:59:59Z]

      key1 = TimeSeries.time_series_row_key("cpu", earlier)
      key2 = TimeSeries.time_series_row_key("cpu", later)

      # Later timestamp should sort first (smaller key due to reverse timestamp)
      assert key2 < key1
    end
  end

  describe "parse_row_key/1" do
    test "parses a valid row key" do
      dt = ~U[2024-01-15 10:00:00.000000Z]
      row_key = TimeSeries.time_series_row_key("cpu:server1", dt)

      assert TimeSeries.parse_row_key(row_key) ==
               {:ok, %{metric_id: "cpu:server1", timestamp: dt}}
    end

    test "handles metric_id with hash signs" do
      dt = ~U[2024-01-15 10:00:00.000000Z]
      row_key = TimeSeries.time_series_row_key("cpu#server#1", dt)

      {:ok, parsed} = TimeSeries.parse_row_key(row_key)

      assert parsed.metric_id == "cpu#server#1"
    end
  end

  describe "value validation" do
    test "write_point/6 rejects a nil value before issuing a request" do
      assert TimeSeries.write_point("p", "i", "t", "m", %{tags: %{a: 1}}) == {:error, :nil_value}
      assert TimeSeries.write_point("p", "i", "t", "m", %{value: nil}) == {:error, :nil_value}
    end

    test "write_point/6 rejects terms with no encoding" do
      assert TimeSeries.write_point("p", "i", "t", "m", %{value: :atom}) ==
               {:error, {:unsupported_value, :atom}}
    end

    test "write_point/6 returns an error tuple for a map Jason cannot encode" do
      assert TimeSeries.write_point("p", "i", "t", "m", %{value: %{pair: {1, 2}}}) ==
               {:error, {:unsupported_value, %{pair: {1, 2}}}}
    end

    test "write_points/5 rejects the whole batch when any point has a nil value" do
      points = [%{metric_id: "m", value: 1}, %{metric_id: "m", value: nil}]

      assert TimeSeries.write_points("p", "i", "t", points) == {:error, :nil_value}
    end
  end
end

defmodule MegasPinakas.TimeSeriesEmulatorTest do
  @moduledoc """
  Emulator-backed contract tests for `MegasPinakas.TimeSeries`: values come
  back with the type they were written as, and `query_range/7` is half-open
  `[start_time, end_time)`.
  """

  use ExUnit.Case, async: false

  alias MegasPinakas.Test.Emulator
  alias MegasPinakas.TimeSeries

  @moduletag :emulator

  @table "highlevel_time_series_test"
  @family "data"

  @t0 ~U[2024-03-01 00:00:00.000000Z]
  @t1 ~U[2024-03-01 00:01:00.000000Z]
  @t2 ~U[2024-03-01 00:02:00.000000Z]
  @t3 ~U[2024-03-01 00:03:00.000000Z]

  setup_all do
    Emulator.setup_table(@table, [@family])
    :ok
  end

  setup %{test: test} do
    {:ok, metric: "metric:#{test}"}
  end

  defp write(metric, value, timestamp, opts \\ []) do
    TimeSeries.write_point(
      Emulator.project(),
      Emulator.instance(),
      @table,
      metric,
      Map.merge(%{value: value}, Map.new(opts)),
      timestamp: timestamp
    )
  end

  defp recent(metric, opts \\ []),
    do: TimeSeries.query_recent(Emulator.project(), Emulator.instance(), @table, metric, opts)

  defp range(metric, from, to, opts \\ []),
    do:
      TimeSeries.query_range(
        Emulator.project(),
        Emulator.instance(),
        @table,
        metric,
        from,
        to,
        opts
      )

  defp values(points), do: Enum.map(points, & &1.value)
  defp timestamps(points), do: Enum.map(points, & &1.timestamp)

  describe "typed roundtrip through query_recent/5" do
    test "integer comes back as an integer, float as float", %{metric: metric} do
      assert {:ok, _} = write(metric, 42, @t0)
      assert {:ok, _} = write(metric, 0.85, @t1)

      assert {:ok, points} = recent(metric)
      # Most recent first.
      assert values(points) == [0.85, 42]
      assert [%{value: 0.85}, %{value: 42}] = points
      assert is_integer(Enum.at(points, 1).value)
    end

    test "string, map, list and boolean values", %{metric: metric} do
      assert {:ok, _} = write(metric, "up", @t0)
      assert {:ok, _} = write(metric, %{"cpu" => 1, "mem" => 2}, @t1)
      assert {:ok, _} = write(metric, [1, 2, 3], @t2)
      assert {:ok, _} = write(metric, true, @t3)

      assert {:ok, points} = recent(metric)
      assert values(points) == [true, [1, 2, 3], %{"cpu" => 1, "mem" => 2}, "up"]
    end

    test "timestamps and tags roundtrip", %{metric: metric} do
      assert {:ok, _} = write(metric, 1, @t0, tags: %{host: "srv1"})

      assert {:ok, [point]} = recent(metric)
      assert point.timestamp == @t0
      assert point.tags == %{"host" => "srv1"}
      assert point.row_key == TimeSeries.time_series_row_key(metric, @t0)
    end

    test "write_points/5 writes a batch and :limit caps query_recent", %{metric: metric} do
      points =
        Enum.map([{@t0, 10}, {@t1, 20}, {@t2, 30}], fn {ts, v} ->
          %{metric_id: metric, value: v, timestamp: ts}
        end)

      assert {:ok, results} =
               TimeSeries.write_points(Emulator.project(), Emulator.instance(), @table, points)

      assert length(results) == 3
      assert Enum.all?(results, &(&1.status.code == 0))

      assert {:ok, recent} = recent(metric, limit: 2)
      assert values(recent) == [30, 20]
    end

    test "a metric with no points yields an empty list", %{metric: metric} do
      assert recent(metric) == {:ok, []}
    end
  end

  describe "query_range/7" do
    setup %{metric: metric} do
      for {ts, v} <- [{@t0, 0}, {@t1, 1}, {@t2, 2}, {@t3, 3}], do: {:ok, _} = write(metric, v, ts)
      :ok
    end

    test "includes start_time and excludes end_time", %{metric: metric} do
      assert {:ok, points} = range(metric, @t1, @t3)
      assert timestamps(points) == [@t2, @t1]
      assert values(points) == [2, 1]
    end

    test "adjacent ranges partition the series without overlap", %{metric: metric} do
      assert {:ok, first} = range(metric, @t0, @t2)
      assert {:ok, second} = range(metric, @t2, @t3)

      assert values(first) == [1, 0]
      assert values(second) == [2]
    end

    test "an empty range yields nothing without a request", %{metric: metric} do
      assert range(metric, @t1, @t1) == {:ok, []}
    end

    # BigTable rejects start_key >= end_key with INVALID_ARGUMENT; the emulator
    # would silently return [], so this must be a client-side error.
    test "an inverted range raises", %{metric: metric} do
      assert_raise ArgumentError, ~r/is after end_time/, fn -> range(metric, @t2, @t1) end
    end

    test ":limit caps the number of points, most recent first", %{metric: metric} do
      assert {:ok, points} = range(metric, @t0, @t3, limit: 2)
      assert values(points) == [2, 1]
    end
  end
end
