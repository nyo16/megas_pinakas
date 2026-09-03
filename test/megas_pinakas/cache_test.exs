defmodule MegasPinakas.CacheTest do
  @moduledoc """
  Emulator-backed contract tests for `MegasPinakas.Cache`: any term roundtrips,
  TTL is enforced on read, and errors propagate instead of masquerading as
  misses.
  """

  use ExUnit.Case, async: false

  alias MegasPinakas.Cache
  alias MegasPinakas.Test.Emulator

  @moduletag :emulator

  @table "highlevel_cache_test"

  setup_all do
    Emulator.setup_table(@table, ["cache"])
    :ok
  end

  setup %{test: test} do
    # Unique key per test so tests never observe each other's entries.
    {:ok, key: "k:#{test}"}
  end

  defp put(key, value, opts \\ []),
    do: Cache.put(Emulator.project(), Emulator.instance(), @table, key, value, opts)

  defp get(key, opts \\ []),
    do: Cache.get(Emulator.project(), Emulator.instance(), @table, key, opts)

  defp exists?(key), do: Cache.exists?(Emulator.project(), Emulator.instance(), @table, key)

  describe "put/get roundtrip" do
    test "map with atom keys", %{key: key} do
      assert {:ok, _} = put(key, %{name: "John", age: 30})
      assert {:ok, %{name: "John", age: 30}} = get(key)
    end

    test "list", %{key: key} do
      assert {:ok, _} = put(key, [1, "two", :three])
      assert get(key) == {:ok, [1, "two", :three]}
    end

    test "string", %{key: key} do
      assert {:ok, _} = put(key, "hello")
      assert get(key) == {:ok, "hello"}
    end

    test "integer stays an integer", %{key: key} do
      assert {:ok, _} = put(key, 42)
      assert get(key) == {:ok, 42}
    end

    test "nil is a stored entry: get reads nil, exists? reports present", %{key: key} do
      assert {:ok, _} = put(key, nil)
      assert get(key) == {:ok, nil}
      assert exists?(key) == {:ok, true}
    end

    test "missing key reads as nil and does not exist", %{key: key} do
      assert get(key) == {:ok, nil}
      assert exists?(key) == {:ok, false}
    end

    test "custom family/qualifier are honoured on both sides", %{key: key} do
      opts = [qualifier: "alt"]
      assert {:ok, _} = put(key, :alt, opts)
      assert get(key, opts) == {:ok, :alt}
      # The default column was never written.
      assert get(key) == {:ok, nil}
    end
  end

  describe ":ttl" do
    # ttl: 2 rather than 1: expiry is whole-second, so a 1 s entry written at
    # S.999 is already expired at S+1.000 and the immediate read could flake.
    test "entry is readable before expiry and absent after", %{key: key} do
      assert {:ok, _} = put(key, "ephemeral", ttl: 2)
      assert get(key) == {:ok, "ephemeral"}
      assert exists?(key) == {:ok, true}

      Process.sleep(2100)

      assert get(key) == {:ok, nil}
      assert exists?(key) == {:ok, false}
    end

    test "rejects a non-positive or non-integer ttl before any request", %{key: key} do
      assert_raise ArgumentError, ~r/:ttl must be a positive integer/, fn ->
        put(key, "x", ttl: 0)
      end

      assert_raise ArgumentError, fn -> put(key, "x", ttl: "60") end
    end
  end

  describe "get_or_put/6" do
    test "miss computes, stores and returns the value", %{key: key} do
      assert {:ok, %{computed: true}} =
               Cache.get_or_put(Emulator.project(), Emulator.instance(), @table, key, fn ->
                 %{computed: true}
               end)

      assert get(key) == {:ok, %{computed: true}}
    end

    test "hit returns the cached value without invoking the function", %{key: key} do
      assert {:ok, _} = put(key, "cached")

      assert {:ok, "cached"} =
               Cache.get_or_put(Emulator.project(), Emulator.instance(), @table, key, fn ->
                 flunk("default_fn must not run on a cache hit")
               end)
    end

    test "a stored nil is a hit, so negative results are cached", %{key: key} do
      assert {:ok, _} = put(key, nil)

      assert {:ok, nil} =
               Cache.get_or_put(Emulator.project(), Emulator.instance(), @table, key, fn ->
                 flunk("default_fn must not run for a stored nil")
               end)
    end

    test "an expired entry counts as a miss", %{key: key} do
      assert {:ok, _} = put(key, "old", ttl: 1)
      Process.sleep(1100)

      assert {:ok, "new"} =
               Cache.get_or_put(Emulator.project(), Emulator.instance(), @table, key, fn ->
                 "new"
               end)

      assert get(key) == {:ok, "new"}
    end
  end

  describe "put_many/5 and get_many/5" do
    test "returns every requested key, nil for missing and expired", %{key: key} do
      present = key <> ":present"
      expired = key <> ":expired"
      missing = key <> ":missing"

      assert {:ok, _} =
               Cache.put_many(Emulator.project(), Emulator.instance(), @table, [
                 {present, %{ok: true}}
               ])

      assert {:ok, _} = put(expired, "soon gone", ttl: 1)
      Process.sleep(1100)

      assert {:ok, results} =
               Cache.get_many(Emulator.project(), Emulator.instance(), @table, [
                 present,
                 expired,
                 missing
               ])

      assert results == %{present => %{ok: true}, expired => nil, missing => nil}
    end

    test "put_many applies :ttl to every entry", %{key: key} do
      a = key <> ":a"
      b = key <> ":b"

      assert {:ok, _} =
               Cache.put_many(Emulator.project(), Emulator.instance(), @table, [{a, 1}, {b, 2}],
                 ttl: 2
               )

      assert {:ok, %{^a => 1, ^b => 2}} =
               Cache.get_many(Emulator.project(), Emulator.instance(), @table, [a, b])

      Process.sleep(2100)

      assert {:ok, %{^a => nil, ^b => nil}} =
               Cache.get_many(Emulator.project(), Emulator.instance(), @table, [a, b])
    end
  end

  describe "delete/5 and delete_many/5" do
    test "delete removes the entry", %{key: key} do
      assert {:ok, _} = put(key, "bye")
      assert {:ok, _} = Cache.delete(Emulator.project(), Emulator.instance(), @table, key)
      assert get(key) == {:ok, nil}
      assert exists?(key) == {:ok, false}
    end

    test "delete_many removes every listed key", %{key: key} do
      keys = [key <> ":1", key <> ":2"]
      Enum.each(keys, &put(&1, "bye"))

      assert {:ok, results} =
               Cache.delete_many(Emulator.project(), Emulator.instance(), @table, keys)

      assert Enum.all?(results, &(&1.status.code == 0))

      assert {:ok, gone} = Cache.get_many(Emulator.project(), Emulator.instance(), @table, keys)
      assert Enum.all?(gone, fn {_k, v} -> is_nil(v) end)
    end
  end

  describe "errors" do
    test "a request failure surfaces from get and exists? instead of reading as a miss",
         %{key: key} do
      assert {:error, _} =
               Cache.get(Emulator.project(), Emulator.instance(), "no_such_table", key)

      assert {:error, _} =
               Cache.exists?(Emulator.project(), Emulator.instance(), "no_such_table", key)
    end
  end
end
