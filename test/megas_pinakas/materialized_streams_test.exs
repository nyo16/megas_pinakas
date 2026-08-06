defmodule MegasPinakas.MaterializedStreamsTest do
  @moduledoc """
  Covers the 0.6.0 contract change: RPCs that used to hand back an unconsumed
  gRPC stream now return materialized lists.

  The bug this pins down is not cosmetic. `mutate_rows/5` documented "a list of
  results, one for each row" but returned a `#Stream<>`, so a caller who never
  enumerated it discarded every per-entry mutation failure — and `Batch.write/5`,
  `Cache.delete_many/5` and `TimeSeries.write_points/5` all passed that stream
  straight through to users.
  """

  use ExUnit.Case, async: false

  alias MegasPinakas.Batch
  alias MegasPinakas.Cache
  alias MegasPinakas.Test.Emulator

  @moduletag :emulator

  @table "materialized_streams_test"

  setup do
    # "cache" is MegasPinakas.Cache's default column family.
    Emulator.setup_table(@table, ["cf", "cache"])
    :ok
  end

  defp entry(key, value, family \\ "cf") do
    %{row_key: key, mutations: [MegasPinakas.set_cell(family, "a", value)]}
  end

  describe "mutate_rows/5" do
    test "returns a list, not a stream" do
      entries = [entry("m#1", "1"), entry("m#2", "2")]

      assert {:ok, results} =
               MegasPinakas.mutate_rows(Emulator.project(), Emulator.instance(), @table, entries)

      assert is_list(results)
      refute match?(%Stream{}, results)
    end

    test "returns one result per entry, in the caller's order" do
      entries = for i <- 1..25, do: entry("m##{i}", "v#{i}")

      assert {:ok, results} =
               MegasPinakas.mutate_rows(Emulator.project(), Emulator.instance(), @table, entries)

      assert length(results) == 25
      assert Enum.map(results, & &1.index) == Enum.to_list(0..24)
    end

    test "successful entries carry status code 0" do
      entries = [entry("m#1", "1"), entry("m#2", "2")]

      assert {:ok, results} =
               MegasPinakas.mutate_rows(Emulator.project(), Emulator.instance(), @table, entries)

      assert Enum.all?(results, &(&1.status.code == 0))
    end

    test "a per-entry failure is visible to the caller" do
      # Partial success: the RPC succeeds, one row fails. Before materialization
      # this failure was unobservable unless the caller enumerated the stream.
      entries = [
        entry("m#ok", "1"),
        entry("m#bad", "1", "no_such_family"),
        entry("m#ok2", "2")
      ]

      assert {:ok, results} =
               MegasPinakas.mutate_rows(Emulator.project(), Emulator.instance(), @table, entries)

      assert length(results) == 3

      failed = Enum.reject(results, &(&1.status.code == 0))

      assert [%{index: 1, status: status}] = failed
      assert status.code != 0
      assert status.message =~ "no_such_family"
    end

    test "the successful entries in a partially failed batch still applied" do
      entries = [entry("m#ok", "1"), entry("m#bad", "1", "no_such_family")]

      assert {:ok, _results} =
               MegasPinakas.mutate_rows(Emulator.project(), Emulator.instance(), @table, entries)

      assert {:ok, row} =
               MegasPinakas.read_row(Emulator.project(), Emulator.instance(), @table, "m#ok")

      assert MegasPinakas.get_cell(row, "cf", "a") == "1"
    end

    test "an empty entry list is rejected by the server, surfaced as an error" do
      # Documents real behaviour: BigTable requires at least one mutation, and
      # that now arrives as an error tuple instead of an unconsumed stream.
      assert {:error, {:invalid_argument, _}} =
               MegasPinakas.mutate_rows(Emulator.project(), Emulator.instance(), @table, [])
    end

    test "a request-level failure returns an error tuple" do
      entries = [entry("m#1", "1")]

      assert {:error, _} =
               MegasPinakas.mutate_rows(
                 Emulator.project(),
                 Emulator.instance(),
                 "no_such_table",
                 entries
               )
    end
  end

  describe "sample_row_keys/4" do
    test "returns a list of samples, not a stream" do
      entries = for i <- 1..10, do: entry("s##{i}", "v")

      {:ok, _} =
        MegasPinakas.mutate_rows(Emulator.project(), Emulator.instance(), @table, entries)

      assert {:ok, samples} =
               MegasPinakas.sample_row_keys(Emulator.project(), Emulator.instance(), @table)

      assert is_list(samples)
      refute Enum.empty?(samples)
      assert Enum.all?(samples, &is_binary(&1.row_key))
      assert Enum.all?(samples, &is_integer(&1.offset_bytes))
    end

    test "a request-level failure returns an error tuple" do
      assert {:error, _} =
               MegasPinakas.sample_row_keys(
                 Emulator.project(),
                 Emulator.instance(),
                 "no_such_table"
               )
    end
  end

  describe "callers that used to leak a stream" do
    test "Batch.write/5 returns per-row results" do
      batch =
        Batch.new()
        |> Batch.add(MegasPinakas.Row.new("b#1") |> MegasPinakas.Row.put_string("cf", "a", "1"))
        |> Batch.add(MegasPinakas.Row.new("b#2") |> MegasPinakas.Row.put_string("cf", "a", "2"))

      assert {:ok, results} =
               Batch.write(batch, Emulator.project(), Emulator.instance(), @table)

      assert is_list(results)
      assert length(results) == 2
      assert Enum.all?(results, &(&1.status.code == 0))
    end

    test "Batch.write/5 preserves insertion order" do
      batch =
        Enum.reduce(1..10, Batch.new(), fn i, acc ->
          Batch.add(
            acc,
            MegasPinakas.Row.new("b##{i}") |> MegasPinakas.Row.put_string("cf", "a", "#{i}")
          )
        end)

      assert {:ok, results} =
               Batch.write(batch, Emulator.project(), Emulator.instance(), @table)

      assert Enum.map(results, & &1.index) == Enum.to_list(0..9)
    end

    test "Cache.delete_many/5 returns per-key results" do
      # Cache stores JSON, so values must be maps or lists.
      {:ok, _} = Cache.put(Emulator.project(), Emulator.instance(), @table, "c#1", %{"v" => 1})
      {:ok, _} = Cache.put(Emulator.project(), Emulator.instance(), @table, "c#2", %{"v" => 2})

      assert {:ok, results} =
               Cache.delete_many(Emulator.project(), Emulator.instance(), @table, ["c#1", "c#2"])

      assert is_list(results)
      assert length(results) == 2
      assert Enum.all?(results, &(&1.status.code == 0))
    end
  end
end
