defmodule MegasPinakasTest do
  use ExUnit.Case, async: true

  alias Google.Bigtable.V2.{Mutation, ReadModifyWriteRule, RowFilter, RowRange, RowSet}

  describe "mutation builders" do
    test "set_cell/3 creates a SetCell mutation with default timestamp" do
      mutation = MegasPinakas.set_cell("cf", "col", "value")

      assert %Mutation{mutation: {:set_cell, set_cell}} = mutation
      assert set_cell.family_name == "cf"
      assert set_cell.column_qualifier == "col"
      assert set_cell.value == "value"
      assert set_cell.timestamp_micros == -1
    end

    test "set_cell/4 creates a SetCell mutation with custom timestamp" do
      mutation = MegasPinakas.set_cell("cf", "col", "value", timestamp_micros: 1234567890)

      assert %Mutation{mutation: {:set_cell, set_cell}} = mutation
      assert set_cell.timestamp_micros == 1234567890
    end

    test "delete_from_column/2 creates a DeleteFromColumn mutation" do
      mutation = MegasPinakas.delete_from_column("cf", "col")

      assert %Mutation{mutation: {:delete_from_column, delete}} = mutation
      assert delete.family_name == "cf"
      assert delete.column_qualifier == "col"
      assert delete.time_range == nil
    end

    test "delete_from_family/1 creates a DeleteFromFamily mutation" do
      mutation = MegasPinakas.delete_from_family("cf")

      assert %Mutation{mutation: {:delete_from_family, delete}} = mutation
      assert delete.family_name == "cf"
    end

    test "delete_from_row/0 creates a DeleteFromRow mutation" do
      mutation = MegasPinakas.delete_from_row()

      assert %Mutation{mutation: {:delete_from_row, %Mutation.DeleteFromRow{}}} = mutation
    end
  end

  describe "read-modify-write rule builders" do
    test "increment_rule/3 creates an increment rule" do
      rule = MegasPinakas.increment_rule("cf", "counter", 5)

      assert %ReadModifyWriteRule{} = rule
      assert rule.family_name == "cf"
      assert rule.column_qualifier == "counter"
      assert rule.rule == {:increment_amount, 5}
    end

    test "increment_rule/3 handles negative increments" do
      rule = MegasPinakas.increment_rule("cf", "counter", -3)

      assert rule.rule == {:increment_amount, -3}
    end

    test "append_rule/3 creates an append rule" do
      rule = MegasPinakas.append_rule("cf", "log", "new entry\n")

      assert %ReadModifyWriteRule{} = rule
      assert rule.family_name == "cf"
      assert rule.column_qualifier == "log"
      assert rule.rule == {:append_value, "new entry\n"}
    end
  end

  describe "row set builders" do
    test "row_set/1 creates a RowSet from row keys" do
      row_set = MegasPinakas.row_set(["row1", "row2", "row3"])

      assert %RowSet{} = row_set
      assert row_set.row_keys == ["row1", "row2", "row3"]
      assert row_set.row_ranges == []
    end

    test "row_set_from_ranges/1 creates a RowSet from row ranges" do
      range = MegasPinakas.row_range("a", "z")
      row_set = MegasPinakas.row_set_from_ranges([range])

      assert %RowSet{} = row_set
      assert row_set.row_keys == []
      assert length(row_set.row_ranges) == 1
    end

    test "row_range/2 creates a row range with closed start and open end" do
      range = MegasPinakas.row_range("start", "end")

      assert %RowRange{} = range
      assert range.start_key == {:start_key_closed, "start"}
      assert range.end_key == {:end_key_open, "end"}
    end

    test "row_range_prefix/1 creates a prefix-based row range" do
      range = MegasPinakas.row_range_prefix("user#")

      assert %RowRange{} = range
      assert range.start_key == {:start_key_closed, "user#"}
      # The end key should be "user$" (# + 1 = $)
      assert range.end_key == {:end_key_open, "user$"}
    end

    test "row_range_prefix/1 handles byte overflow" do
      # Test with a string ending in 0xFF
      range = MegasPinakas.row_range_prefix(<<97, 255>>)

      assert %RowRange{} = range
      assert range.start_key == {:start_key_closed, <<97, 255>>}
      # Should increment the previous byte
      assert range.end_key == {:end_key_open, <<98>>}
    end
  end

  describe "filter builders" do
    test "column_filter/2 creates a filter for specific column" do
      filter = MegasPinakas.column_filter("cf", "col")

      assert %RowFilter{filter: {:chain, chain}} = filter
      assert length(chain.filters) == 2
    end

    test "family_filter/1 creates a filter for column family" do
      filter = MegasPinakas.family_filter("cf")

      assert %RowFilter{filter: {:family_name_regex_filter, regex}} = filter
      assert regex == "^cf$"
    end

    test "family_filter/1 escapes regex special characters" do
      filter = MegasPinakas.family_filter("cf.test")

      assert %RowFilter{filter: {:family_name_regex_filter, regex}} = filter
      assert regex == "^cf\\.test$"
    end

    test "cells_per_column_limit_filter/1 creates a limit filter" do
      filter = MegasPinakas.cells_per_column_limit_filter(3)

      assert %RowFilter{filter: {:cells_per_column_limit_filter, 3}} = filter
    end

    test "pass_all_filter/0 creates a pass-all filter" do
      filter = MegasPinakas.pass_all_filter()

      assert %RowFilter{filter: {:pass_all_filter, true}} = filter
    end

    test "block_all_filter/0 creates a block-all filter" do
      filter = MegasPinakas.block_all_filter()

      assert %RowFilter{filter: {:block_all_filter, true}} = filter
    end

    test "chain_filters/1 chains filters with AND logic" do
      filter1 = MegasPinakas.family_filter("cf")
      filter2 = MegasPinakas.cells_per_column_limit_filter(1)

      chained = MegasPinakas.chain_filters([filter1, filter2])

      assert %RowFilter{filter: {:chain, chain}} = chained
      assert length(chain.filters) == 2
    end

    test "interleave_filters/1 interleaves filters with OR logic" do
      filter1 = MegasPinakas.family_filter("cf1")
      filter2 = MegasPinakas.family_filter("cf2")

      interleaved = MegasPinakas.interleave_filters([filter1, filter2])

      assert %RowFilter{filter: {:interleave, interleave}} = interleaved
      assert length(interleave.filters) == 2
    end
  end
end
