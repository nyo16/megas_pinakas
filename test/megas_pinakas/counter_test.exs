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

  describe "module structure" do
    test "exports increment function" do
      functions = Counter.__info__(:functions)
      assert {:increment, 7} in functions
      assert {:increment, 8} in functions
    end

    test "exports decrement function" do
      functions = Counter.__info__(:functions)
      assert {:decrement, 7} in functions
      assert {:decrement, 8} in functions
    end

    test "exports get function" do
      functions = Counter.__info__(:functions)
      assert {:get, 6} in functions
      assert {:get, 7} in functions
    end

    test "exports set function" do
      functions = Counter.__info__(:functions)
      assert {:set, 7} in functions
      assert {:set, 8} in functions
    end

    test "exports reset function" do
      functions = Counter.__info__(:functions)
      assert {:reset, 6} in functions
      assert {:reset, 7} in functions
    end

    test "exports increment_many function" do
      functions = Counter.__info__(:functions)
      assert {:increment_many, 5} in functions
      assert {:increment_many, 6} in functions
    end

    test "exports increment_if_exists function" do
      functions = Counter.__info__(:functions)
      assert {:increment_if_exists, 7} in functions
      assert {:increment_if_exists, 8} in functions
    end

    test "exports add_counter function" do
      functions = Counter.__info__(:functions)
      assert {:add_counter, 3} in functions
      assert {:add_counter, 4} in functions
    end

    test "exports increment_rule function" do
      functions = Counter.__info__(:functions)
      assert {:increment_rule, 3} in functions
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
