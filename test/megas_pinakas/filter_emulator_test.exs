defmodule MegasPinakas.FilterEmulatorTest do
  @moduledoc """
  Runs each `MegasPinakas.Filter` builder against real data in the emulator.

  The unit tests in `filter_test.exs` only prove the proto we build; these
  prove the server interprets it the way the docs claim. The headline case is
  `row_key_prefix_filter/1`: BigTable regexes are RE2 *full* matches, so the
  old `"^prefix"` pattern matched only the row keyed exactly `prefix`.
  """

  use ExUnit.Case, async: false

  alias MegasPinakas.Admin
  alias MegasPinakas.Filter
  alias MegasPinakas.Test.Emulator

  @moduletag :emulator

  @table "datapath_filter_test"

  # Fixed, millisecond-aligned timestamps so timestamp filters are deterministic.
  @t1 1_000_000
  @t2 2_000_000
  @t3 3_000_000

  setup_all do
    Emulator.await_pool!()
    _ = Admin.delete_table(Emulator.project(), Emulator.instance(), @table)

    # No GC rule: the cells_per_column_limit test needs several versions to
    # survive, and the emulator would otherwise be free to collect them.
    {:ok, _} =
      Admin.create_table(Emulator.project(), Emulator.instance(), @table,
        column_families: %{"cf" => %{}, "meta" => %{}}
      )

    entries = [
      row("user#", [{"cf", "name", "root", @t1}]),
      row("user#1", [
        {"cf", "name", "alice", @t1},
        {"cf", "name", "alice-v2", @t2},
        {"cf", "name", "alice-v3", @t3},
        {"cf", "status", "error: disk full", @t2},
        {"meta", "role", "user", @t1}
      ]),
      row("user#2", [{"cf", "name", "bob", @t2}, {"meta", "role", "admin", @t1}]),
      row("admin#1", [{"cf", "name", "carol", @t3}, {"cf", "status", "ok", @t3}])
    ]

    {:ok, results} =
      MegasPinakas.mutate_rows(Emulator.project(), Emulator.instance(), @table, entries)

    assert Enum.all?(results, &(&1.status.code == 0))
    :ok
  end

  defp row(key, cells) do
    %{
      row_key: key,
      mutations:
        Enum.map(cells, fn {family, qualifier, value, ts} ->
          MegasPinakas.set_cell(family, qualifier, value, timestamp_micros: ts)
        end)
    }
  end

  defp read(filter) do
    {:ok, rows} =
      MegasPinakas.read_rows(Emulator.project(), Emulator.instance(), @table, filter: filter)

    rows
  end

  defp keys(rows), do: rows |> Enum.map(&MegasPinakas.row_key/1) |> Enum.sort()

  describe "row_key_prefix_filter/1" do
    test "matches every key with the prefix, including the bare prefix itself" do
      assert keys(read(Filter.row_key_prefix_filter("user#"))) == ["user#", "user#1", "user#2"]
    end

    test "does not match keys that merely contain the prefix" do
      assert keys(read(Filter.row_key_prefix_filter("ser#"))) == []
    end

    test "a bare regex without \\C* is an exact match, which is why the prefix filter pads it" do
      assert keys(read(Filter.row_key_regex_filter("user#"))) == ["user#"]
    end
  end

  describe "family_filter/1" do
    test "returns only cells from that family" do
      rows = read(Filter.family_filter("meta"))

      assert keys(rows) == ["user#1", "user#2"]
      assert Enum.all?(rows, fn r -> Enum.map(r.families, & &1.name) == ["meta"] end)
    end
  end

  describe "column_filter/2" do
    test "returns only the named column" do
      rows = read(Filter.column_filter("cf", "status"))

      assert keys(rows) == ["admin#1", "user#1"]

      for r <- rows do
        assert MegasPinakas.get_family(r, "cf") |> Map.keys() == ["status"]
      end
    end
  end

  describe "value_regex_filter/1" do
    test "substring match needs \\C* on both sides" do
      rows = read(Filter.value_regex_filter("\\C*error\\C*"))

      assert keys(rows) == ["user#1"]
      assert [row] = rows
      assert MegasPinakas.get_cell(row, "cf", "status") == "error: disk full"
      assert MegasPinakas.get_cell(row, "cf", "name") == nil
    end
  end

  describe "timestamp_range_filter/2" do
    test "start is inclusive and end is exclusive" do
      rows = read(Filter.timestamp_range_filter(@t2, @t3))

      # user#1: name@t2 and status@t2 survive; name@t1/t3 and meta@t1 do not.
      # user#2: name@t2 survives; meta@t1 does not. admin#1 is all @t3 -> gone.
      assert keys(rows) == ["user#1", "user#2"]

      user1 = Enum.find(rows, &(MegasPinakas.row_key(&1) == "user#1"))
      assert MegasPinakas.get_cells(user1, "cf", "name") == [%{value: "alice-v2", timestamp: @t2}]
      assert MegasPinakas.get_cell(user1, "cf", "status") == "error: disk full"
      assert MegasPinakas.get_family(user1, "meta") == %{}
    end

    test "0 as the end means unbounded" do
      rows = read(Filter.timestamp_range_filter(@t3, 0))

      assert keys(rows) == ["admin#1", "user#1"]
    end
  end

  describe "cells_per_column_limit_filter/1" do
    test "caps the versions returned per column, newest first" do
      unfiltered = read(Filter.column_filter("cf", "name"))
      user1 = Enum.find(unfiltered, &(MegasPinakas.row_key(&1) == "user#1"))
      assert length(MegasPinakas.get_cells(user1, "cf", "name")) == 3

      rows =
        read(
          Filter.chain_filters([
            Filter.column_filter("cf", "name"),
            Filter.cells_per_column_limit_filter(2)
          ])
        )

      user1 = Enum.find(rows, &(MegasPinakas.row_key(&1) == "user#1"))

      assert MegasPinakas.get_cells(user1, "cf", "name") == [
               %{value: "alice-v3", timestamp: @t3},
               %{value: "alice-v2", timestamp: @t2}
             ]
    end
  end

  describe "chain_filters/1" do
    test "ANDs its members" do
      rows =
        read(
          Filter.chain_filters([
            Filter.row_key_prefix_filter("user#"),
            Filter.column_filter("meta", "role"),
            Filter.value_regex_filter("admin")
          ])
        )

      assert keys(rows) == ["user#2"]
      assert [row] = rows
      assert MegasPinakas.row_to_map(row) == %{"meta" => %{"role" => "admin"}}
    end
  end

  describe "interleave_filters/1" do
    test "ORs its members" do
      rows =
        read(
          Filter.interleave_filters([
            Filter.column_filter("cf", "status"),
            Filter.column_filter("meta", "role")
          ])
        )

      assert keys(rows) == ["admin#1", "user#1", "user#2"]

      user1 = Enum.find(rows, &(MegasPinakas.row_key(&1) == "user#1"))

      assert MegasPinakas.row_to_map(user1) == %{
               "cf" => %{"status" => "error: disk full"},
               "meta" => %{"role" => "user"}
             }
    end
  end

  describe "condition_filter/3" do
    test "applies the true branch to rows the predicate matches and the false branch otherwise" do
      rows =
        read(
          Filter.condition_filter(
            Filter.column_filter("meta", "role"),
            Filter.column_filter("meta", "role"),
            Filter.column_filter("cf", "name")
          )
        )

      by_key = Map.new(rows, &{MegasPinakas.row_key(&1), MegasPinakas.row_to_map(&1)})

      assert by_key == %{
               "user#" => %{"cf" => %{"name" => "root"}},
               "user#1" => %{"meta" => %{"role" => "user"}},
               "user#2" => %{"meta" => %{"role" => "admin"}},
               "admin#1" => %{"cf" => %{"name" => "carol"}}
             }
    end
  end
end
