defmodule MegasPinakas.TimeSeriesTest do
  use ExUnit.Case, async: true

  alias MegasPinakas.TimeSeries

  describe "reverse_timestamp/1" do
    test "converts DateTime to reverse timestamp string" do
      dt = ~U[2024-01-15 10:00:00.000000Z]
      result = TimeSeries.reverse_timestamp(dt)

      assert is_binary(result)
      assert String.length(result) == 19  # Padded to 19 chars
    end

    test "earlier timestamps produce larger reverse timestamps" do
      earlier = ~U[2024-01-01 00:00:00Z]
      later = ~U[2024-12-31 23:59:59Z]

      reverse_earlier = TimeSeries.reverse_timestamp(earlier)
      reverse_later = TimeSeries.reverse_timestamp(later)

      # Earlier time should have larger reverse timestamp (sorts first)
      assert reverse_earlier > reverse_later
    end

    test "preserves microsecond precision" do
      dt = ~U[2024-01-15 10:00:00.123456Z]
      reverse = TimeSeries.reverse_timestamp(dt)

      {:ok, recovered} = TimeSeries.from_reverse_timestamp(reverse)
      assert recovered == dt
    end
  end

  describe "from_reverse_timestamp/1" do
    test "converts reverse timestamp back to DateTime" do
      dt = ~U[2024-01-15 10:00:00.000000Z]
      reverse = TimeSeries.reverse_timestamp(dt)

      {:ok, recovered} = TimeSeries.from_reverse_timestamp(reverse)
      assert recovered == dt
    end

    test "returns error for invalid format" do
      assert {:error, :invalid_format} = TimeSeries.from_reverse_timestamp("not_a_number")
    end
  end

  describe "time_series_row_key/2" do
    test "builds row key with metric_id and reverse timestamp" do
      dt = ~U[2024-01-15 10:00:00Z]
      row_key = TimeSeries.time_series_row_key("cpu:server1", dt)

      assert String.starts_with?(row_key, "cpu:server1#")
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

      {:ok, parsed} = TimeSeries.parse_row_key(row_key)

      assert parsed.metric_id == "cpu:server1"
      assert parsed.timestamp == dt
    end

    test "handles metric_id with hash signs" do
      dt = ~U[2024-01-15 10:00:00.000000Z]
      row_key = TimeSeries.time_series_row_key("cpu#server#1", dt)

      {:ok, parsed} = TimeSeries.parse_row_key(row_key)

      assert parsed.metric_id == "cpu#server#1"
    end
  end

  describe "module structure" do
    test "exports write_point function" do
      functions = TimeSeries.__info__(:functions)
      assert {:write_point, 5} in functions
      assert {:write_point, 6} in functions
    end

    test "exports write_points function" do
      functions = TimeSeries.__info__(:functions)
      assert {:write_points, 4} in functions
      assert {:write_points, 5} in functions
    end

    test "exports query_recent function" do
      functions = TimeSeries.__info__(:functions)
      assert {:query_recent, 4} in functions
      assert {:query_recent, 5} in functions
    end

    test "exports query_range function" do
      functions = TimeSeries.__info__(:functions)
      assert {:query_range, 6} in functions
      assert {:query_range, 7} in functions
    end

    test "exports time_series_row_key function" do
      functions = TimeSeries.__info__(:functions)
      assert {:time_series_row_key, 2} in functions
    end

    test "exports reverse_timestamp function" do
      functions = TimeSeries.__info__(:functions)
      assert {:reverse_timestamp, 1} in functions
    end

    test "exports from_reverse_timestamp function" do
      functions = TimeSeries.__info__(:functions)
      assert {:from_reverse_timestamp, 1} in functions
    end

    test "exports parse_row_key function" do
      functions = TimeSeries.__info__(:functions)
      assert {:parse_row_key, 1} in functions
    end
  end
end
