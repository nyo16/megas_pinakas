defmodule MegasPinakas.CounterTTLTest do
  use ExUnit.Case, async: true

  alias MegasPinakas.CounterTTL

  describe "bucket_to_seconds/1" do
    test "converts :second to 1" do
      assert CounterTTL.bucket_to_seconds(:second) == 1
    end

    test "converts :minute to 60" do
      assert CounterTTL.bucket_to_seconds(:minute) == 60
    end

    test "converts :hour to 3600" do
      assert CounterTTL.bucket_to_seconds(:hour) == 3600
    end

    test "converts :day to 86400" do
      assert CounterTTL.bucket_to_seconds(:day) == 86400
    end

    test "converts :week to 604800" do
      assert CounterTTL.bucket_to_seconds(:week) == 604_800
    end
  end

  describe "build_row_key/3" do
    test "builds row key with minute bucket" do
      # Use a known timestamp
      timestamp = 1704067200  # 2024-01-01 00:00:00 UTC
      row_key = CounterTTL.build_row_key("user#123", :minute, timestamp)

      # Should be aligned to minute boundary
      assert String.starts_with?(row_key, "user#123#")
      assert String.ends_with?(row_key, "1704067200")
    end

    test "builds row key with hour bucket" do
      timestamp = 1704067200  # 2024-01-01 00:00:00 UTC (already aligned)
      row_key = CounterTTL.build_row_key("api:requests", :hour, timestamp)

      assert row_key == "api:requests#1704067200"
    end

    test "aligns timestamp to bucket boundary" do
      # 1704067245 = 2024-01-01 00:00:45 UTC (45 seconds into the minute)
      timestamp = 1704067245
      row_key = CounterTTL.build_row_key("user#123", :minute, timestamp)

      # Should align to minute boundary (1704067200)
      assert row_key == "user#123#1704067200"
    end

    test "uses current time when timestamp is nil" do
      row_key = CounterTTL.build_row_key("user#123", :minute)

      assert String.starts_with?(row_key, "user#123#")
      # Should contain a numeric timestamp
      [_prefix, _key, ts] = String.split(row_key, "#")
      {timestamp, ""} = Integer.parse(ts)
      assert timestamp > 0
    end
  end

  describe "parse_row_key/1" do
    test "parses a valid row key" do
      result = CounterTTL.parse_row_key("user#123#1704067200")

      assert {:ok, parsed} = result
      assert parsed.key == "user#123"
      assert parsed.bucket_timestamp == 1704067200
    end

    test "handles keys with multiple hash signs" do
      result = CounterTTL.parse_row_key("api#v1#endpoint#1704067200")

      assert {:ok, parsed} = result
      assert parsed.key == "api#v1#endpoint"
      assert parsed.bucket_timestamp == 1704067200
    end

    test "returns error for invalid format (no timestamp)" do
      result = CounterTTL.parse_row_key("user#123#abc")

      assert result == {:error, :invalid_format}
    end

    test "handles simple key" do
      result = CounterTTL.parse_row_key("mykey#1704067200")

      assert {:ok, parsed} = result
      assert parsed.key == "mykey"
      assert parsed.bucket_timestamp == 1704067200
    end
  end

  describe "module structure" do
    test "exports increment function" do
      functions = CounterTTL.__info__(:functions)
      assert {:increment, 6} in functions
      assert {:increment, 7} in functions
    end

    test "exports get_current function" do
      functions = CounterTTL.__info__(:functions)
      assert {:get_current, 6} in functions
      assert {:get_current, 7} in functions
    end

    test "exports get_window function" do
      functions = CounterTTL.__info__(:functions)
      assert {:get_window, 6} in functions
      assert {:get_window, 7} in functions
    end

    test "exports check_rate_limit function" do
      functions = CounterTTL.__info__(:functions)
      assert {:check_rate_limit, 5} in functions
      assert {:check_rate_limit, 6} in functions
    end

    test "exports increment_with_limit function" do
      functions = CounterTTL.__info__(:functions)
      assert {:increment_with_limit, 5} in functions
      assert {:increment_with_limit, 6} in functions
    end

    test "exports build_row_key function" do
      functions = CounterTTL.__info__(:functions)
      assert {:build_row_key, 2} in functions
      assert {:build_row_key, 3} in functions
    end

    test "exports parse_row_key function" do
      functions = CounterTTL.__info__(:functions)
      assert {:parse_row_key, 1} in functions
    end

    test "exports bucket_to_seconds function" do
      functions = CounterTTL.__info__(:functions)
      assert {:bucket_to_seconds, 1} in functions
    end
  end
end
