defmodule MegasPinakas.Auth.CacheTest do
  use ExUnit.Case, async: false

  alias MegasPinakas.Auth
  alias MegasPinakas.Auth.Cache

  # Counts how many times the underlying token source actually ran, which is the
  # property that matters: an uncached fetch cost 911.9 ms per RPC.
  defp with_counting_source(fun, opts \\ []) do
    ttl = Keyword.get(opts, :ttl, 3_600)
    delay = Keyword.get(opts, :delay, 0)
    result = Keyword.get(opts, :result, :ok)

    counter = :counters.new(1, [:write_concurrency])
    original = Application.get_env(:megas_pinakas, :token_source)

    Application.put_env(:megas_pinakas, :token_source, fn ->
      :counters.add(counter, 1, 1)
      if delay > 0, do: Process.sleep(delay)

      case result do
        :ok ->
          n = :counters.get(counter, 1)
          {:ok, %{token: "Bearer token-#{n}", expires_at: System.os_time(:second) + ttl}}

        {:error, reason} ->
          {:error, reason}
      end
    end)

    Cache.invalidate()

    try do
      fun.(counter)
    after
      if original do
        Application.put_env(:megas_pinakas, :token_source, original)
      else
        Application.delete_env(:megas_pinakas, :token_source)
      end

      Cache.invalidate()
    end
  end

  defp fetch_count(counter), do: :counters.get(counter, 1)

  describe "caching" do
    test "a second fetch issues no new token fetch" do
      with_counting_source(fn counter ->
        assert {:ok, token} = Cache.fetch_token()
        assert fetch_count(counter) == 1

        assert {:ok, ^token} = Cache.fetch_token()
        assert fetch_count(counter) == 1
      end)
    end

    test "many sequential fetches still cost one token fetch" do
      with_counting_source(fn counter ->
        tokens = for _ <- 1..50, do: Cache.fetch_token()

        assert Enum.uniq(tokens) |> length() == 1
        assert fetch_count(counter) == 1
      end)
    end

    test "request_opts/0 reuses the cached token" do
      emulator = Application.get_env(:megas_pinakas, :emulator)
      env_host = System.get_env("BIGTABLE_EMULATOR_HOST")
      Application.delete_env(:megas_pinakas, :emulator)
      System.delete_env("BIGTABLE_EMULATOR_HOST")

      try do
        with_counting_source(fn counter ->
          opts = Auth.request_opts()
          assert %{"authorization" => "Bearer token-1"} = Keyword.fetch!(opts, :metadata)
          assert fetch_count(counter) == 1

          for _ <- 1..10, do: Auth.request_opts()
          assert fetch_count(counter) == 1
        end)
      after
        if emulator, do: Application.put_env(:megas_pinakas, :emulator, emulator)
        if env_host, do: System.put_env("BIGTABLE_EMULATOR_HOST", env_host)
      end
    end

    test "invalidate/1 forces a refetch" do
      with_counting_source(fn counter ->
        assert {:ok, "Bearer token-1"} = Cache.fetch_token()
        assert :ok = Cache.invalidate()
        assert {:ok, "Bearer token-2"} = Cache.fetch_token()
        assert fetch_count(counter) == 2
      end)
    end
  end

  describe "expiry" do
    test "a token near expiry is refreshed" do
      # 30s of life left is inside the 60s refresh margin.
      with_counting_source(
        fn counter ->
          assert {:ok, "Bearer token-1"} = Cache.fetch_token()
          assert {:ok, "Bearer token-2"} = Cache.fetch_token()
          assert fetch_count(counter) == 2
        end,
        ttl: 30
      )
    end

    test "a token comfortably in date is not refreshed" do
      with_counting_source(
        fn counter ->
          assert {:ok, "Bearer token-1"} = Cache.fetch_token()
          assert {:ok, "Bearer token-1"} = Cache.fetch_token()
          assert fetch_count(counter) == 1
        end,
        ttl: 120
      )
    end

    test "peek/0 exposes the stored expiry" do
      now = System.os_time(:second)

      with_counting_source(
        fn _counter ->
          assert {:ok, _token} = Cache.fetch_token()
          assert {:ok, "Bearer token-1", expires_at} = Cache.peek()
          assert_in_delta expires_at, now + 3_600, 5
        end,
        ttl: 3_600
      )
    end
  end

  describe "single-flight refresh" do
    test "N concurrent callers at expiry trigger one token fetch" do
      callers = 25

      with_counting_source(
        fn counter ->
          results =
            1..callers
            |> Enum.map(fn _ -> Task.async(fn -> Cache.fetch_token() end) end)
            |> Enum.map(&Task.await(&1, 5_000))

          assert Enum.all?(results, &match?({:ok, "Bearer token-1"}, &1)),
                 "expected every caller to receive the same token, got #{inspect(Enum.uniq(results))}"

          assert fetch_count(counter) == 1, """
          Expected 1 token fetch for #{callers} concurrent callers, got #{fetch_count(counter)}.

          Without a single-flight refresh, token expiry under load produces a
          thundering herd of ~900 ms gcloud subprocess spawns.
          """
        end,
        # Hold the refresh open long enough that all callers pile up behind it.
        delay: 200
      )
    end
  end

  describe "failures" do
    test "a failing source returns the error and caches no token" do
      with_counting_source(
        fn counter ->
          assert {:error, :boom} = Cache.fetch_token()
          assert Cache.peek() == :error
          assert fetch_count(counter) == 1
        end,
        result: {:error, :boom}
      )
    end

    test "a failure is remembered: N calls within the window cost one source call" do
      with_counting_source(
        fn counter ->
          results = for _ <- 1..50, do: Cache.fetch_token()

          assert Enum.uniq(results) == [{:error, :boom}]

          assert fetch_count(counter) == 1, """
          Expected the failing token source to run once for 50 calls, got #{fetch_count(counter)}.

          Without a negative cache, a broken credential re-runs the ~1 s token
          fetch (and logs) on every RPC.
          """
        end,
        result: {:error, :boom}
      )
    end

    test "concurrent callers behind a failing refresh share one source call" do
      with_counting_source(
        fn counter ->
          results =
            1..25
            |> Enum.map(fn _ -> Task.async(fn -> Cache.fetch_token() end) end)
            |> Enum.map(&Task.await(&1, 5_000))

          assert Enum.uniq(results) == [{:error, :boom}]
          assert fetch_count(counter) == 1
        end,
        result: {:error, :boom},
        delay: 100
      )
    end

    test "invalidate/1 forgets the failure so the source is retried" do
      with_counting_source(
        fn counter ->
          assert {:error, :boom} = Cache.fetch_token()
          assert :ok = Cache.invalidate()
          assert {:error, :boom} = Cache.fetch_token()
          assert fetch_count(counter) == 2
        end,
        result: {:error, :boom}
      )
    end

    test "a raising source is contained and its failure is cached" do
      cache_pid = Process.whereis(Cache)
      original = Application.get_env(:megas_pinakas, :token_source)
      counter = :counters.new(1, [])

      Application.put_env(:megas_pinakas, :token_source, fn ->
        :counters.add(counter, 1, 1)
        raise ArgumentError, "bad credentials"
      end)

      Cache.invalidate()

      try do
        for _ <- 1..10 do
          assert {:error, {:token_source_error, "bad credentials"}} = Cache.fetch_token()
        end

        assert :counters.get(counter, 1) == 1
        assert Process.whereis(Cache) == cache_pid
      after
        if original,
          do: Application.put_env(:megas_pinakas, :token_source, original),
          else: Application.delete_env(:megas_pinakas, :token_source)

        Cache.invalidate()
      end
    end

    test "a stale token plus a remembered failure does not re-run the source" do
      # A token inside the refresh margin is still in the table when its
      # refresh fails. The stale token must not keep triggering refreshes that
      # the negative cache was meant to suppress.
      with_counting_source(
        fn counter ->
          # Fetch 1 caches a token with 30 s left — already inside the 60 s
          # margin — so fetch 2 refreshes.
          assert {:ok, "Bearer token-1"} = Cache.fetch_token()
          assert fetch_count(counter) == 1

          Application.put_env(:megas_pinakas, :token_source, fn ->
            :counters.add(counter, 1, 1)
            {:error, :revoked}
          end)

          assert {:error, :revoked} = Cache.fetch_token()
          assert fetch_count(counter) == 2

          for _ <- 1..20, do: assert({:error, :revoked} = Cache.fetch_token())
          assert fetch_count(counter) == 2
        end,
        ttl: 30
      )
    end
  end

  describe "gcloud fallback gating" do
    test "is allowed outside production builds" do
      original = Application.get_env(:megas_pinakas, :allow_gcloud_auth_fallback)
      Application.delete_env(:megas_pinakas, :allow_gcloud_auth_fallback)

      try do
        # This suite runs under MIX_ENV=test.
        assert Auth.gcloud_fallback_allowed?()
      after
        if original != nil do
          Application.put_env(:megas_pinakas, :allow_gcloud_auth_fallback, original)
        end
      end
    end

    test "explicit config overrides the compile-time default" do
      original = Application.get_env(:megas_pinakas, :allow_gcloud_auth_fallback)
      Application.put_env(:megas_pinakas, :allow_gcloud_auth_fallback, false)

      try do
        refute Auth.gcloud_fallback_allowed?()
      after
        if original == nil do
          Application.delete_env(:megas_pinakas, :allow_gcloud_auth_fallback)
        else
          Application.put_env(:megas_pinakas, :allow_gcloud_auth_fallback, original)
        end
      end
    end

    test "fetch_fresh_token/0 reports the gate rather than shelling out" do
      goth = Application.get_env(:megas_pinakas, :goth)
      Application.delete_env(:megas_pinakas, :goth)
      Application.put_env(:megas_pinakas, :allow_gcloud_auth_fallback, false)

      try do
        assert {:error, :gcloud_fallback_disabled} = Auth.fetch_fresh_token()
      after
        Application.delete_env(:megas_pinakas, :allow_gcloud_auth_fallback)
        if goth, do: Application.put_env(:megas_pinakas, :goth, goth)
      end
    end
  end

  # The single clock behind token expiry. Auth stamps `expires_at` with it and
  # Cache compares against it, so the two must not drift apart.
  describe "now/0" do
    test "returns the current unix time in seconds" do
      before = System.os_time(:second)
      now = Cache.now()
      later = System.os_time(:second)

      assert is_integer(now)
      assert now >= before
      assert now <= later
    end
  end
end
