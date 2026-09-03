defmodule MegasPinakas.CounterTest do
  use ExUnit.Case, async: true

  alias Google.Bigtable.V2.{Cell, Column, Family, ReadModifyWriteRowResponse, ReadModifyWriteRule}
  alias Google.Bigtable.V2.Row, as: BigtableRow
  alias MegasPinakas.Counter
  alias MegasPinakas.Row
  alias MegasPinakas.Types

  describe "increment_rule/3" do
    test "creates an increment rule" do
      rule = Counter.increment_rule("cf", "views", 1)

      assert %ReadModifyWriteRule{} = rule
      assert rule.family_name == "cf"
      assert rule.column_qualifier == "views"
      assert rule.rule == {:increment_amount, 1}
    end

    test "creates increment rule with custom amount" do
      rule = Counter.increment_rule("cf", "count", 10)

      assert rule.rule == {:increment_amount, 10}
    end

    test "creates increment rule with negative amount" do
      rule = Counter.increment_rule("cf", "stock", -5)

      assert rule.rule == {:increment_amount, -5}
    end
  end

  describe "add_counter/4" do
    test "adds a counter to a row with default initial value" do
      row = Row.new("counter#1") |> Counter.add_counter("cf", "views")

      mutations = Row.to_mutations(row)
      assert length(mutations) == 1

      [mutation] = mutations
      assert mutation.mutation |> elem(0) == :set_cell

      set_cell = mutation.mutation |> elem(1)
      assert set_cell.family_name == "cf"
      assert set_cell.column_qualifier == "views"
      # Initial value 0 as 64-bit big-endian
      assert set_cell.value == <<0, 0, 0, 0, 0, 0, 0, 0>>
    end

    test "adds a counter with custom initial value" do
      row = Row.new("counter#1") |> Counter.add_counter("cf", "views", 100)

      [mutation] = Row.to_mutations(row)
      set_cell = mutation.mutation |> elem(1)
      # 100 as 64-bit big-endian
      assert set_cell.value == <<0, 0, 0, 0, 0, 0, 0, 100>>
    end

    test "adds multiple counters to a row" do
      row =
        Row.new("counter#1")
        |> Counter.add_counter("cf", "views", 0)
        |> Counter.add_counter("cf", "clicks", 0)
        |> Counter.add_counter("stats", "sessions", 10)

      mutations = Row.to_mutations(row)
      assert length(mutations) == 3
    end
  end

  # Shared by Counter and CounterTTL, which both decode a counter out of a
  # read-modify-write response. Previously duplicated verbatim in both modules.
  describe "extract_counter_value/3" do
    test "returns nil when the response carries no row" do
      response = %ReadModifyWriteRowResponse{row: nil}

      assert Counter.extract_counter_value(response, "cf", "views") == {:ok, nil}
    end

    test "returns nil when the row lacks the requested column" do
      response = counter_response("cf", "clicks", Types.encode(:integer, 7))

      assert Counter.extract_counter_value(response, "cf", "views") == {:ok, nil}
    end

    test "returns nil when the row lacks the requested family" do
      response = counter_response("other", "views", Types.encode(:integer, 7))

      assert Counter.extract_counter_value(response, "cf", "views") == {:ok, nil}
    end

    test "decodes the counter value" do
      response = counter_response("cf", "views", Types.encode(:integer, 42))

      assert Counter.extract_counter_value(response, "cf", "views") == {:ok, 42}
    end

    test "decodes a negative counter value" do
      response = counter_response("cf", "views", Types.encode(:integer, -5))

      assert Counter.extract_counter_value(response, "cf", "views") == {:ok, -5}
    end

    test "surfaces a decode error for a malformed value" do
      response = counter_response("cf", "views", "not-eight-bytes")

      assert Counter.extract_counter_value(response, "cf", "views") ==
               {:error, :invalid_integer_format}
    end
  end

  defp counter_response(family, qualifier, value) do
    %ReadModifyWriteRowResponse{
      row: %BigtableRow{
        key: "counter#1",
        families: [
          %Family{
            name: family,
            columns: [%Column{qualifier: qualifier, cells: [%Cell{value: value}]}]
          }
        ]
      }
    }
  end
end

defmodule MegasPinakas.CounterEmulatorTest do
  @moduledoc """
  Emulator-backed contract tests for `MegasPinakas.Counter`: atomic increments
  roundtrip, reads see only the latest version, and `increment_if_exists/8`
  is a real compare-and-swap that never creates a counter.
  """

  use ExUnit.Case, async: false

  alias MegasPinakas.Counter
  alias MegasPinakas.Test.Emulator

  @moduletag :emulator

  @table "highlevel_counter_test"
  @family "cf"
  @qualifier "n"

  setup_all do
    Emulator.setup_table(@table, [@family])
    :ok
  end

  setup %{test: test} do
    {:ok, row: "counter:#{test}"}
  end

  defp increment(row, amount),
    do:
      Counter.increment(
        Emulator.project(),
        Emulator.instance(),
        @table,
        row,
        @family,
        @qualifier,
        amount
      )

  defp get(row),
    do: Counter.get(Emulator.project(), Emulator.instance(), @table, row, @family, @qualifier)

  defp increment_if_exists(row, amount),
    do:
      Counter.increment_if_exists(
        Emulator.project(),
        Emulator.instance(),
        @table,
        row,
        @family,
        @qualifier,
        amount
      )

  describe "increment/8 and get/7" do
    test "a missing counter reads as nil", %{row: row} do
      assert get(row) == {:ok, nil}
    end

    test "increment creates the counter and returns the running total", %{row: row} do
      assert increment(row, 1) == {:ok, 1}
      assert increment(row, 5) == {:ok, 6}
      assert get(row) == {:ok, 6}
    end

    test "negative increments and decrement/8 subtract", %{row: row} do
      assert increment(row, 10) == {:ok, 10}
      assert increment(row, -3) == {:ok, 7}

      assert Counter.decrement(
               Emulator.project(),
               Emulator.instance(),
               @table,
               row,
               @family,
               @qualifier,
               7
             ) == {:ok, 0}

      assert get(row) == {:ok, 0}
    end

    test "get returns the latest value after many read-modify-writes", %{row: row} do
      for _ <- 1..5, do: {:ok, _} = increment(row, 1)
      assert get(row) == {:ok, 5}
    end

    test "set/8 and reset/7 overwrite", %{row: row} do
      assert {:ok, _} =
               Counter.set(
                 Emulator.project(),
                 Emulator.instance(),
                 @table,
                 row,
                 @family,
                 @qualifier,
                 100
               )

      assert get(row) == {:ok, 100}

      assert {:ok, _} =
               Counter.reset(
                 Emulator.project(),
                 Emulator.instance(),
                 @table,
                 row,
                 @family,
                 @qualifier
               )

      assert get(row) == {:ok, 0}
    end
  end

  describe "increment_many/6" do
    test "increments several columns of one row atomically", %{row: row} do
      assert {:ok, %{"cf:a" => 1, "cf:b" => 3}} =
               Counter.increment_many(Emulator.project(), Emulator.instance(), @table, row, [
                 {@family, "a", 1},
                 {@family, "b", 3}
               ])

      assert {:ok, %{"cf:a" => 2, "cf:b" => 6}} =
               Counter.increment_many(Emulator.project(), Emulator.instance(), @table, row, [
                 {@family, "a", 1},
                 {@family, "b", 3}
               ])
    end
  end

  describe "increment_if_exists/8" do
    test "does not create a missing counter", %{row: row} do
      assert increment_if_exists(row, 1) == {:ok, :not_applied}
      assert get(row) == {:ok, nil}
    end

    test "adds to an existing counter: 41 + 1 -> 42", %{row: row} do
      assert increment(row, 41) == {:ok, 41}
      assert increment_if_exists(row, 1) == {:ok, :applied}
      assert get(row) == {:ok, 42}
    end

    test "applies negative amounts", %{row: row} do
      assert increment(row, 10) == {:ok, 10}
      assert increment_if_exists(row, -4) == {:ok, :applied}
      assert get(row) == {:ok, 6}
    end

    test "adds to the latest value when older cell versions are present", %{row: row} do
      # Several RMWs leave older cell versions behind until GC runs; the CAS
      # must read and add to the newest (3), not to a stale version.
      for _ <- 1..3, do: {:ok, _} = increment(row, 1)
      assert increment_if_exists(row, 10) == {:ok, :applied}
      assert get(row) == {:ok, 13}
    end

    test "a counter in another column does not count as existing", %{row: row} do
      assert {:ok, 1} =
               Counter.increment(
                 Emulator.project(),
                 Emulator.instance(),
                 @table,
                 row,
                 @family,
                 "other",
                 1
               )

      assert increment_if_exists(row, 1) == {:ok, :not_applied}
      assert get(row) == {:ok, nil}
    end
  end
end
