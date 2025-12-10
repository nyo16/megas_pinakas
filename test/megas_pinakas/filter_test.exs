defmodule MegasPinakas.FilterTest do
  use ExUnit.Case, async: true

  alias MegasPinakas.Filter
  alias Google.Bigtable.V2.{RowFilter, ColumnRange, TimestampRange, ValueRange}

  # ============================================================================
  # Row Level Filters
  # ============================================================================

  describe "row_key_regex_filter/1" do
    test "creates a row key regex filter" do
      filter = Filter.row_key_regex_filter("^user#")

      assert %RowFilter{filter: {:row_key_regex_filter, "^user#"}} = filter
    end

    test "accepts complex regex patterns" do
      filter = Filter.row_key_regex_filter("^(user|admin)#[0-9]+$")

      assert %RowFilter{filter: {:row_key_regex_filter, "^(user|admin)#[0-9]+$"}} = filter
    end
  end

  describe "row_sample_filter/1" do
    test "creates a row sample filter" do
      filter = Filter.row_sample_filter(0.5)

      assert %RowFilter{filter: {:row_sample_filter, 0.5}} = filter
    end

    test "accepts 0.0 probability" do
      filter = Filter.row_sample_filter(0.0)

      assert %RowFilter{filter: {:row_sample_filter, 0.0}} = filter
    end

    test "accepts 1.0 probability" do
      filter = Filter.row_sample_filter(1.0)

      assert %RowFilter{filter: {:row_sample_filter, 1.0}} = filter
    end

    test "accepts small probabilities" do
      filter = Filter.row_sample_filter(0.001)

      assert %RowFilter{filter: {:row_sample_filter, 0.001}} = filter
    end
  end

  # ============================================================================
  # Cell Level Filters
  # ============================================================================

  describe "cells_per_row_limit_filter/1" do
    test "creates a cells per row limit filter" do
      filter = Filter.cells_per_row_limit_filter(100)

      assert %RowFilter{filter: {:cells_per_row_limit_filter, 100}} = filter
    end

    test "accepts limit of 1" do
      filter = Filter.cells_per_row_limit_filter(1)

      assert %RowFilter{filter: {:cells_per_row_limit_filter, 1}} = filter
    end
  end

  describe "cells_per_row_offset_filter/1" do
    test "creates a cells per row offset filter" do
      filter = Filter.cells_per_row_offset_filter(10)

      assert %RowFilter{filter: {:cells_per_row_offset_filter, 10}} = filter
    end

    test "accepts offset of 0" do
      filter = Filter.cells_per_row_offset_filter(0)

      assert %RowFilter{filter: {:cells_per_row_offset_filter, 0}} = filter
    end
  end

  describe "cells_per_column_limit_filter/1" do
    test "creates a cells per column limit filter" do
      filter = Filter.cells_per_column_limit_filter(3)

      assert %RowFilter{filter: {:cells_per_column_limit_filter, 3}} = filter
    end

    test "accepts limit of 1" do
      filter = Filter.cells_per_column_limit_filter(1)

      assert %RowFilter{filter: {:cells_per_column_limit_filter, 1}} = filter
    end
  end

  describe "column_qualifier_regex_filter/1" do
    test "creates a column qualifier regex filter" do
      filter = Filter.column_qualifier_regex_filter("^meta_")

      assert %RowFilter{filter: {:column_qualifier_regex_filter, "^meta_"}} = filter
    end

    test "accepts complex regex" do
      filter = Filter.column_qualifier_regex_filter("^(name|email|phone)$")

      assert %RowFilter{filter: {:column_qualifier_regex_filter, "^(name|email|phone)$"}} = filter
    end
  end

  # ============================================================================
  # Range Filters
  # ============================================================================

  describe "column_range_filter/2" do
    test "creates a column range filter with closed start and open end" do
      filter = Filter.column_range_filter("cf",
        start_qualifier_closed: "a",
        end_qualifier_open: "m"
      )

      assert %RowFilter{filter: {:column_range_filter, range}} = filter
      assert %ColumnRange{family_name: "cf"} = range
      assert range.start_qualifier == {:start_qualifier_closed, "a"}
      assert range.end_qualifier == {:end_qualifier_open, "m"}
    end

    test "creates a column range filter with open start and closed end" do
      filter = Filter.column_range_filter("cf",
        start_qualifier_open: "a",
        end_qualifier_closed: "z"
      )

      assert %RowFilter{filter: {:column_range_filter, range}} = filter
      assert range.start_qualifier == {:start_qualifier_open, "a"}
      assert range.end_qualifier == {:end_qualifier_closed, "z"}
    end

    test "creates a column range filter with only start" do
      filter = Filter.column_range_filter("cf",
        start_qualifier_closed: "x"
      )

      assert %RowFilter{filter: {:column_range_filter, range}} = filter
      assert range.start_qualifier == {:start_qualifier_closed, "x"}
      assert range.end_qualifier == nil
    end

    test "creates a column range filter with only end" do
      filter = Filter.column_range_filter("cf",
        end_qualifier_open: "m"
      )

      assert %RowFilter{filter: {:column_range_filter, range}} = filter
      assert range.start_qualifier == nil
      assert range.end_qualifier == {:end_qualifier_open, "m"}
    end

    test "creates a column range filter with no bounds" do
      filter = Filter.column_range_filter("cf")

      assert %RowFilter{filter: {:column_range_filter, range}} = filter
      assert range.family_name == "cf"
      assert range.start_qualifier == nil
      assert range.end_qualifier == nil
    end
  end

  describe "timestamp_range_filter/2" do
    test "creates a timestamp range filter" do
      filter = Filter.timestamp_range_filter(1000, 2000)

      assert %RowFilter{filter: {:timestamp_range_filter, range}} = filter
      assert %TimestampRange{} = range
      assert range.start_timestamp_micros == 1000
      assert range.end_timestamp_micros == 2000
    end

    test "handles large timestamp values" do
      now = System.system_time(:microsecond)
      hour_ago = now - 3_600_000_000

      filter = Filter.timestamp_range_filter(hour_ago, now)

      assert %RowFilter{filter: {:timestamp_range_filter, range}} = filter
      assert range.start_timestamp_micros == hour_ago
      assert range.end_timestamp_micros == now
    end
  end

  describe "value_range_filter/1" do
    test "creates a value range filter with closed bounds" do
      filter = Filter.value_range_filter(
        start_value_closed: "A",
        end_value_closed: "Z"
      )

      assert %RowFilter{filter: {:value_range_filter, range}} = filter
      assert %ValueRange{} = range
      assert range.start_value == {:start_value_closed, "A"}
      assert range.end_value == {:end_value_closed, "Z"}
    end

    test "creates a value range filter with open bounds" do
      filter = Filter.value_range_filter(
        start_value_open: "A",
        end_value_open: "Z"
      )

      assert %RowFilter{filter: {:value_range_filter, range}} = filter
      assert range.start_value == {:start_value_open, "A"}
      assert range.end_value == {:end_value_open, "Z"}
    end

    test "creates a value range filter with binary values" do
      filter = Filter.value_range_filter(
        start_value_closed: <<0, 0, 0, 100>>,
        end_value_open: <<0, 0, 0, 200>>
      )

      assert %RowFilter{filter: {:value_range_filter, range}} = filter
      assert range.start_value == {:start_value_closed, <<0, 0, 0, 100>>}
      assert range.end_value == {:end_value_open, <<0, 0, 0, 200>>}
    end

    test "creates an empty value range filter" do
      filter = Filter.value_range_filter()

      assert %RowFilter{filter: {:value_range_filter, range}} = filter
      assert range.start_value == nil
      assert range.end_value == nil
    end
  end

  describe "value_regex_filter/1" do
    test "creates a value regex filter" do
      filter = Filter.value_regex_filter("error")

      assert %RowFilter{filter: {:value_regex_filter, "error"}} = filter
    end

    test "accepts complex regex patterns" do
      filter = Filter.value_regex_filter("^[a-f0-9]{8}-[a-f0-9]{4}")

      assert %RowFilter{filter: {:value_regex_filter, "^[a-f0-9]{8}-[a-f0-9]{4}"}} = filter
    end
  end

  # ============================================================================
  # Family and Column Filters
  # ============================================================================

  describe "family_filter/1" do
    test "creates a family filter" do
      filter = Filter.family_filter("cf")

      assert %RowFilter{filter: {:family_name_regex_filter, "^cf$"}} = filter
    end

    test "escapes special regex characters" do
      filter = Filter.family_filter("cf.test")

      assert %RowFilter{filter: {:family_name_regex_filter, "^cf\\.test$"}} = filter
    end
  end

  describe "family_regex_filter/1" do
    test "creates a family regex filter without escaping" do
      filter = Filter.family_regex_filter("^cf_")

      assert %RowFilter{filter: {:family_name_regex_filter, "^cf_"}} = filter
    end
  end

  describe "column_filter/2" do
    test "creates a column filter as a chain" do
      filter = Filter.column_filter("cf", "name")

      assert %RowFilter{filter: {:chain, chain}} = filter
      assert length(chain.filters) == 2
    end
  end

  # ============================================================================
  # Modifying Filters
  # ============================================================================

  describe "strip_value_filter/0" do
    test "creates a strip value filter" do
      filter = Filter.strip_value_filter()

      assert %RowFilter{filter: {:strip_value_transformer, true}} = filter
    end
  end

  describe "apply_label_filter/1" do
    test "creates an apply label filter" do
      filter = Filter.apply_label_filter("important")

      assert %RowFilter{filter: {:apply_label_transformer, "important"}} = filter
    end
  end

  # ============================================================================
  # Pass/Block Filters
  # ============================================================================

  describe "pass_all_filter/0" do
    test "creates a pass all filter" do
      filter = Filter.pass_all_filter()

      assert %RowFilter{filter: {:pass_all_filter, true}} = filter
    end
  end

  describe "block_all_filter/0" do
    test "creates a block all filter" do
      filter = Filter.block_all_filter()

      assert %RowFilter{filter: {:block_all_filter, true}} = filter
    end
  end

  # ============================================================================
  # Composing Filters
  # ============================================================================

  describe "chain_filters/1" do
    test "chains multiple filters" do
      filter = Filter.chain_filters([
        Filter.family_filter("cf"),
        Filter.cells_per_column_limit_filter(1)
      ])

      assert %RowFilter{filter: {:chain, chain}} = filter
      assert length(chain.filters) == 2
    end

    test "chains empty list" do
      filter = Filter.chain_filters([])

      assert %RowFilter{filter: {:chain, chain}} = filter
      assert chain.filters == []
    end

    test "chains single filter" do
      filter = Filter.chain_filters([Filter.pass_all_filter()])

      assert %RowFilter{filter: {:chain, chain}} = filter
      assert length(chain.filters) == 1
    end
  end

  describe "interleave_filters/1" do
    test "interleaves multiple filters" do
      filter = Filter.interleave_filters([
        Filter.family_filter("cf1"),
        Filter.family_filter("cf2")
      ])

      assert %RowFilter{filter: {:interleave, interleave}} = filter
      assert length(interleave.filters) == 2
    end

    test "interleaves empty list" do
      filter = Filter.interleave_filters([])

      assert %RowFilter{filter: {:interleave, interleave}} = filter
      assert interleave.filters == []
    end
  end

  describe "condition_filter/3" do
    test "creates a condition filter with both branches" do
      filter = Filter.condition_filter(
        Filter.column_filter("cf", "admin"),
        Filter.pass_all_filter(),
        Filter.block_all_filter()
      )

      assert %RowFilter{filter: {:condition, condition}} = filter
      assert %RowFilter.Condition{} = condition
      assert condition.predicate_filter != nil
      assert condition.true_filter != nil
      assert condition.false_filter != nil
    end

    test "creates a condition filter with only true branch" do
      filter = Filter.condition_filter(
        Filter.value_regex_filter("error"),
        Filter.apply_label_filter("has_error")
      )

      assert %RowFilter{filter: {:condition, condition}} = filter
      assert condition.predicate_filter != nil
      assert condition.true_filter != nil
      assert condition.false_filter == nil
    end
  end

  describe "sink_filter/0" do
    test "creates a sink filter" do
      filter = Filter.sink_filter()

      assert %RowFilter{filter: {:sink, true}} = filter
    end
  end

  # ============================================================================
  # Convenience Builders
  # ============================================================================

  describe "latest_only_filter/0" do
    test "creates a filter for latest version only" do
      filter = Filter.latest_only_filter()

      assert %RowFilter{filter: {:cells_per_column_limit_filter, 1}} = filter
    end
  end

  describe "column_latest_filter/2" do
    test "creates a filter for a column with latest version" do
      filter = Filter.column_latest_filter("cf", "name")

      assert %RowFilter{filter: {:chain, chain}} = filter
      assert length(chain.filters) == 2
    end
  end

  describe "time_window_filter/2" do
    test "creates a filter for the last hour" do
      filter = Filter.time_window_filter(:hour)

      assert %RowFilter{filter: {:timestamp_range_filter, range}} = filter
      now = System.system_time(:microsecond)
      # Should be within a few milliseconds of now
      assert range.end_timestamp_micros > now - 100_000
      assert range.end_timestamp_micros <= now
      assert range.start_timestamp_micros < range.end_timestamp_micros
    end

    test "creates a filter for multiple units" do
      filter = Filter.time_window_filter(:day, 7)

      assert %RowFilter{filter: {:timestamp_range_filter, range}} = filter
      # 7 days in microseconds = 7 * 86400 * 1_000_000 = 604_800_000_000
      diff = range.end_timestamp_micros - range.start_timestamp_micros
      assert_in_delta diff, 604_800_000_000, 1_000_000  # Allow 1 second tolerance
    end

    test "supports various time units" do
      # Just verify each unit doesn't crash
      assert %RowFilter{} = Filter.time_window_filter(:second)
      assert %RowFilter{} = Filter.time_window_filter(:minute)
      assert %RowFilter{} = Filter.time_window_filter(:hour)
      assert %RowFilter{} = Filter.time_window_filter(:day)
      assert %RowFilter{} = Filter.time_window_filter(:week)
    end
  end

  describe "row_key_prefix_filter/1" do
    test "creates a filter for row key prefix" do
      filter = Filter.row_key_prefix_filter("user#")

      assert %RowFilter{filter: {:row_key_regex_filter, regex}} = filter
      assert regex == "^user\\#"
    end

    test "escapes special characters" do
      filter = Filter.row_key_prefix_filter("test.prefix")

      assert %RowFilter{filter: {:row_key_regex_filter, regex}} = filter
      assert regex == "^test\\.prefix"
    end
  end

  # ============================================================================
  # Module Exports
  # ============================================================================

  describe "module exports" do
    test "exports all expected functions" do
      functions = Filter.__info__(:functions)

      # Row level filters
      assert {:row_key_regex_filter, 1} in functions
      assert {:row_sample_filter, 1} in functions

      # Cell level filters
      assert {:cells_per_row_limit_filter, 1} in functions
      assert {:cells_per_row_offset_filter, 1} in functions
      assert {:cells_per_column_limit_filter, 1} in functions
      assert {:column_qualifier_regex_filter, 1} in functions

      # Range filters
      assert {:column_range_filter, 1} in functions
      assert {:column_range_filter, 2} in functions
      assert {:timestamp_range_filter, 2} in functions
      assert {:value_range_filter, 0} in functions
      assert {:value_range_filter, 1} in functions
      assert {:value_regex_filter, 1} in functions

      # Family and column filters
      assert {:family_filter, 1} in functions
      assert {:family_regex_filter, 1} in functions
      assert {:column_filter, 2} in functions

      # Modifying filters
      assert {:strip_value_filter, 0} in functions
      assert {:apply_label_filter, 1} in functions

      # Pass/block filters
      assert {:pass_all_filter, 0} in functions
      assert {:block_all_filter, 0} in functions

      # Composing filters
      assert {:chain_filters, 1} in functions
      assert {:interleave_filters, 1} in functions
      assert {:condition_filter, 2} in functions
      assert {:condition_filter, 3} in functions
      assert {:sink_filter, 0} in functions

      # Convenience builders
      assert {:latest_only_filter, 0} in functions
      assert {:column_latest_filter, 2} in functions
      assert {:time_window_filter, 1} in functions
      assert {:time_window_filter, 2} in functions
      assert {:row_key_prefix_filter, 1} in functions
    end
  end
end
