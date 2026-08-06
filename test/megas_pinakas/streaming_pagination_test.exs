defmodule MegasPinakas.StreamingPaginationTest do
  @moduledoc """
  Regression tests for `MegasPinakas.Streaming` pagination termination.

  Every test here wraps consumption in a `Task` with an explicit timeout so a
  non-terminating stream fails the test instead of hanging CI forever.
  """

  use ExUnit.Case, async: false

  alias MegasPinakas.Streaming
  alias MegasPinakas.Test.Emulator

  @moduletag :emulator

  @table "streaming_pagination_test"
  @consume_timeout 5_000

  setup_all do
    Emulator.setup_table(@table, ["cf"])
    Emulator.seed_rows(@table, 25, prefix: "pag#")
    :ok
  end

  defp consume!(fun) do
    fun
    |> Task.async()
    |> Task.await(@consume_timeout)
  end

  describe "discrete row key sets" do
    test "a row_keys-only row set terminates" do
      keys = Enum.map(0..2, &Emulator.row_key("pag#", &1))

      rows =
        consume!(fn ->
          Streaming.stream_rows(Emulator.project(), Emulator.instance(), @table,
            rows: MegasPinakas.row_set(keys)
          )
          |> Enum.to_list()
        end)

      assert length(rows) == 3
      assert Enum.map(rows, &MegasPinakas.row_key/1) == keys
    end

    test "a row_keys-only row set does not repeat rows" do
      keys = Enum.map(0..4, &Emulator.row_key("pag#", &1))

      taken =
        consume!(fn ->
          Streaming.stream_rows(Emulator.project(), Emulator.instance(), @table,
            rows: MegasPinakas.row_set(keys)
          )
          # Ask for far more than exist — a looping stream would happily supply them.
          |> Enum.take(20)
        end)

      assert length(taken) == 5
      assert Enum.map(taken, &MegasPinakas.row_key/1) == keys
    end

    test "count_rows/4 terminates on a row_keys-only row set" do
      keys = Enum.map(0..4, &Emulator.row_key("pag#", &1))

      count =
        consume!(fn ->
          Streaming.count_rows(Emulator.project(), Emulator.instance(), @table,
            rows: MegasPinakas.row_set(keys)
          )
        end)

      assert count == 5
    end

    test "a row_keys set smaller than batch_size still terminates" do
      keys = Enum.map(0..1, &Emulator.row_key("pag#", &1))

      rows =
        consume!(fn ->
          Streaming.stream_rows(Emulator.project(), Emulator.instance(), @table,
            rows: MegasPinakas.row_set(keys),
            batch_size: 1_000
          )
          |> Enum.to_list()
        end)

      assert length(rows) == 2
    end

    test "a row_keys set larger than batch_size yields every key exactly once" do
      keys = Enum.map(0..9, &Emulator.row_key("pag#", &1))

      rows =
        consume!(fn ->
          Streaming.stream_rows(Emulator.project(), Emulator.instance(), @table,
            rows: MegasPinakas.row_set(keys),
            batch_size: 3
          )
          |> Enum.to_list()
        end)

      assert Enum.map(rows, &MegasPinakas.row_key/1) == keys
    end
  end

  describe "range row sets" do
    test "a prefix range terminates and yields each row once" do
      rows =
        consume!(fn ->
          Streaming.stream_prefix(Emulator.project(), Emulator.instance(), @table, "pag#")
          |> Enum.to_list()
        end)

      keys = Enum.map(rows, &MegasPinakas.row_key/1)

      assert length(keys) == 25
      assert keys == Enum.sort(keys)
      assert keys == Enum.uniq(keys)
    end

    test "a prefix range paginates across several batches without duplicates" do
      rows =
        consume!(fn ->
          Streaming.stream_rows(Emulator.project(), Emulator.instance(), @table,
            rows: MegasPinakas.row_set_from_ranges([MegasPinakas.row_range_prefix("pag#")]),
            batch_size: 4
          )
          |> Enum.to_list()
        end)

      keys = Enum.map(rows, &MegasPinakas.row_key/1)

      assert length(keys) == 25
      assert keys == Enum.uniq(keys)
    end

    test "an unspecified row set streams the whole table and terminates" do
      count =
        consume!(fn ->
          Streaming.count_rows(Emulator.project(), Emulator.instance(), @table, batch_size: 7)
        end)

      assert count == 25
    end
  end

  describe "mixed row sets" do
    # The old cursor hardcoded `row_keys: []` when rewriting ranges, so discrete
    # keys were silently dropped after the first page.
    test "keys and ranges together are both honoured across pages" do
      keys = [Emulator.row_key("pag#", 0), Emulator.row_key("pag#", 1)]

      range =
        MegasPinakas.row_range_closed(
          Emulator.row_key("pag#", 20),
          Emulator.row_key("pag#", 24)
        )

      row_set = %Google.Bigtable.V2.RowSet{row_keys: keys, row_ranges: [range]}

      rows =
        consume!(fn ->
          Streaming.stream_rows(Emulator.project(), Emulator.instance(), @table,
            rows: row_set,
            batch_size: 2
          )
          |> Enum.to_list()
        end)

      got = Enum.map(rows, &MegasPinakas.row_key/1)
      expected = keys ++ Enum.map(20..24, &Emulator.row_key("pag#", &1))

      assert got == expected
    end

    test "an empty row set streams the whole table and terminates" do
      row_set = %Google.Bigtable.V2.RowSet{row_keys: [], row_ranges: []}

      count =
        consume!(fn ->
          Streaming.count_rows(Emulator.project(), Emulator.instance(), @table,
            rows: row_set,
            batch_size: 6
          )
        end)

      assert count == 25
    end
  end

  describe "empty results" do
    test "a row_keys set matching nothing terminates with no rows" do
      rows =
        consume!(fn ->
          Streaming.stream_rows(Emulator.project(), Emulator.instance(), @table,
            rows: MegasPinakas.row_set(["absent#1", "absent#2"])
          )
          |> Enum.to_list()
        end)

      assert rows == []
    end

    test "a range matching nothing terminates with no rows" do
      rows =
        consume!(fn ->
          Streaming.stream_prefix(Emulator.project(), Emulator.instance(), @table, "nothing#")
          |> Enum.to_list()
        end)

      assert rows == []
    end
  end
end
