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

    test "converts :day to 86_400" do
      assert CounterTTL.bucket_to_seconds(:day) == 86_400
    end

    test "converts :week to 604800" do
      assert CounterTTL.bucket_to_seconds(:week) == 604_800
    end
  end

  describe "build_row_key/3" do
    test "builds row key with minute bucket" do
      # 2024-01-01 00:00:00 UTC
      timestamp = 1_704_067_200
      row_key = CounterTTL.build_row_key("user#123", :minute, timestamp)

      assert row_key == "user#123#1704067200"
    end

    test "builds row key with hour bucket" do
      # 2024-01-01 00:00:00 UTC (already aligned)
      timestamp = 1_704_067_200
      row_key = CounterTTL.build_row_key("api:requests", :hour, timestamp)

      assert row_key == "api:requests#1704067200"
    end

    test "aligns timestamp to bucket boundary" do
      # 1704067245 = 2024-01-01 00:00:45 UTC (45 seconds into the minute)
      timestamp = 1_704_067_245
      row_key = CounterTTL.build_row_key("user#123", :minute, timestamp)

      assert row_key == "user#123#1704067200"
    end

    test "uses current time when timestamp is nil" do
      row_key = CounterTTL.build_row_key("user#123", :minute)

      assert String.starts_with?(row_key, "user#123#")
      [_prefix, _key, ts] = String.split(row_key, "#")
      {timestamp, ""} = Integer.parse(ts)
      assert rem(timestamp, 60) == 0
      assert_in_delta timestamp, System.system_time(:second), 60
    end
  end

  describe "parse_row_key/1" do
    test "parses a valid row key" do
      assert {:ok, %{key: "user#123", bucket_timestamp: 1_704_067_200}} =
               CounterTTL.parse_row_key("user#123#1704067200")
    end

    test "handles keys with multiple hash signs" do
      assert {:ok, %{key: "api#v1#endpoint", bucket_timestamp: 1_704_067_200}} =
               CounterTTL.parse_row_key("api#v1#endpoint#1704067200")
    end

    test "returns error for invalid format (no timestamp)" do
      assert CounterTTL.parse_row_key("user#123#abc") == {:error, :invalid_format}
    end

    test "handles simple key" do
      assert {:ok, %{key: "mykey", bucket_timestamp: 1_704_067_200}} =
               CounterTTL.parse_row_key("mykey#1704067200")
    end
  end

  describe "get_window/7 argument validation" do
    test "rejects a zero window before issuing any request" do
      assert_raise ArgumentError, ~r/:window_size must be a positive integer/, fn ->
        CounterTTL.get_window("p", "i", "t", "k", "cf", "n", window_size: 0)
      end
    end

    test "rejects a negative or non-integer window" do
      assert_raise ArgumentError, fn ->
        CounterTTL.get_window("p", "i", "t", "k", "cf", "n", window_size: -1)
      end

      assert_raise ArgumentError, fn ->
        CounterTTL.get_window("p", "i", "t", "k", "cf", "n", window_size: 2.0)
      end
    end
  end
end

defmodule MegasPinakas.CounterTTLEmulatorTest do
  @moduledoc """
  Emulator-backed contract tests for `MegasPinakas.CounterTTL`: bucketed
  increments, window sums across buckets, and rate limiting.

  Uses `:day` buckets so a bucket boundary is vanishingly unlikely to roll
  between the writes and reads of a single test.
  """

  use ExUnit.Case, async: false

  alias MegasPinakas.Counter
  alias MegasPinakas.CounterTTL
  alias MegasPinakas.Test.Emulator

  @moduletag :emulator

  @table "highlevel_counter_ttl_test"
  @family "counters"
  @qualifier "count"
  @bucket :day

  setup_all do
    Emulator.setup_table(@table, [@family])
    :ok
  end

  setup %{test: test} do
    {:ok, key: "ttl:#{test}"}
  end

  defp increment(key, opts \\ []) do
    CounterTTL.increment(
      Emulator.project(),
      Emulator.instance(),
      @table,
      key,
      @family,
      @qualifier,
      Keyword.put(opts, :bucket, @bucket)
    )
  end

  defp get_window(key, window_size) do
    CounterTTL.get_window(
      Emulator.project(),
      Emulator.instance(),
      @table,
      key,
      @family,
      @qualifier,
      bucket: @bucket,
      window_size: window_size
    )
  end

  # Seeds an earlier bucket directly, since increments only ever hit "now".
  defp seed_bucket(key, buckets_ago, value) do
    ts = System.system_time(:second) - buckets_ago * CounterTTL.bucket_to_seconds(@bucket)
    row_key = CounterTTL.build_row_key(key, @bucket, ts)

    {:ok, _} =
      Counter.set(
        Emulator.project(),
        Emulator.instance(),
        @table,
        row_key,
        @family,
        @qualifier,
        value
      )
  end

  describe "increment/7 and get_current/7" do
    test "increments the current bucket and reads it back", %{key: key} do
      assert increment(key) == {:ok, 1}
      assert increment(key, amount: 4) == {:ok, 5}

      assert CounterTTL.get_current(
               Emulator.project(),
               Emulator.instance(),
               @table,
               key,
               @family,
               @qualifier,
               bucket: @bucket
             ) == {:ok, 5}
    end

    test "an untouched bucket reads as nil", %{key: key} do
      assert CounterTTL.get_current(
               Emulator.project(),
               Emulator.instance(),
               @table,
               key,
               @family,
               @qualifier,
               bucket: @bucket
             ) == {:ok, nil}
    end
  end

  describe "get_window/7" do
    test "a single bucket window is the current bucket's count", %{key: key} do
      for _ <- 1..3, do: {:ok, _} = increment(key)
      assert get_window(key, 1) == {:ok, 3}
    end

    test "sums N consecutive buckets and ignores older ones", %{key: key} do
      {:ok, _} = increment(key, amount: 1)
      seed_bucket(key, 1, 10)
      seed_bucket(key, 2, 100)
      seed_bucket(key, 3, 1000)

      assert get_window(key, 1) == {:ok, 1}
      assert get_window(key, 2) == {:ok, 11}
      assert get_window(key, 3) == {:ok, 111}
      # Missing buckets inside the window contribute zero.
      assert get_window(key, 10) == {:ok, 1111}
    end

    test "an empty window sums to zero", %{key: key} do
      assert get_window(key, 5) == {:ok, 0}
    end
  end

  describe "check_rate_limit/6" do
    test "under the limit returns the current count, zero when untouched", %{key: key} do
      opts = [bucket: @bucket, family: @family, qualifier: @qualifier]

      assert CounterTTL.check_rate_limit(
               Emulator.project(),
               Emulator.instance(),
               @table,
               key,
               3,
               opts
             ) ==
               {:ok, 0}

      {:ok, _} = increment(key, amount: 2)

      assert CounterTTL.check_rate_limit(
               Emulator.project(),
               Emulator.instance(),
               @table,
               key,
               3,
               opts
             ) ==
               {:ok, 2}
    end

    test "at the limit reports rate_limited with the next bucket start", %{key: key} do
      opts = [bucket: @bucket, family: @family, qualifier: @qualifier]
      {:ok, _} = increment(key, amount: 3)

      assert {:error, :rate_limited, %DateTime{} = reset_at} =
               CounterTTL.check_rate_limit(
                 Emulator.project(),
                 Emulator.instance(),
                 @table,
                 key,
                 3,
                 opts
               )

      bucket_seconds = CounterTTL.bucket_to_seconds(@bucket)

      {:ok, %{bucket_timestamp: start}} =
        CounterTTL.parse_row_key(CounterTTL.build_row_key(key, @bucket))

      assert DateTime.to_unix(reset_at) == start + bucket_seconds
    end
  end

  describe "increment_with_limit/6" do
    test "increments while under the limit, then rate-limits without writing", %{key: key} do
      opts = [bucket: @bucket, family: @family, qualifier: @qualifier]

      call = fn ->
        CounterTTL.increment_with_limit(
          Emulator.project(),
          Emulator.instance(),
          @table,
          key,
          2,
          opts
        )
      end

      assert call.() == {:ok, 1}
      assert call.() == {:ok, 2}
      assert {:error, :rate_limited, %DateTime{}} = call.()
      assert get_window(key, 1) == {:ok, 2}
    end

    test "a non-positive limit always rate-limits and never writes", %{key: key} do
      opts = [bucket: @bucket, family: @family, qualifier: @qualifier]

      assert {:error, :rate_limited, %DateTime{}} =
               CounterTTL.increment_with_limit(
                 Emulator.project(),
                 Emulator.instance(),
                 @table,
                 key,
                 0,
                 opts
               )

      assert get_window(key, 1) == {:ok, 0}

      # check_rate_limit/6 must agree with increment_with_limit/6 on an
      # untouched bucket: 0 is not under a limit of 0.
      assert {:error, :rate_limited, %DateTime{}} =
               CounterTTL.check_rate_limit(
                 Emulator.project(),
                 Emulator.instance(),
                 @table,
                 key,
                 0,
                 opts
               )
    end

    test "honours :amount", %{key: key} do
      opts = [bucket: @bucket, family: @family, qualifier: @qualifier, amount: 5]

      assert CounterTTL.increment_with_limit(
               Emulator.project(),
               Emulator.instance(),
               @table,
               key,
               10,
               opts
             ) ==
               {:ok, 5}

      assert CounterTTL.increment_with_limit(
               Emulator.project(),
               Emulator.instance(),
               @table,
               key,
               10,
               opts
             ) ==
               {:ok, 10}

      assert {:error, :rate_limited, _} =
               CounterTTL.increment_with_limit(
                 Emulator.project(),
                 Emulator.instance(),
                 @table,
                 key,
                 10,
                 opts
               )
    end
  end
end
