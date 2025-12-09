defmodule MegasPinakasTest do
  use ExUnit.Case, async: true

  alias Google.Bigtable.V2.{Cell, Column, Family, Mutation, ReadModifyWriteRule, Row, RowFilter, RowRange, RowSet}

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

  # Helper to build test rows
  defp build_test_row(key, families_data) do
    families =
      Enum.map(families_data, fn {family_name, columns_data} ->
        columns =
          Enum.map(columns_data, fn {qualifier, cells_data} ->
            cells =
              Enum.map(cells_data, fn {value, timestamp} ->
                %Cell{value: value, timestamp_micros: timestamp, labels: []}
              end)

            %Column{qualifier: qualifier, cells: cells}
          end)

        %Family{name: family_name, columns: columns}
      end)

    %Row{key: key, families: families}
  end

  describe "row_to_map/1" do
    test "converts a row to a nested map" do
      row = build_test_row("user#123", [
        {"cf", [
          {"name", [{"John Doe", 1000}]},
          {"email", [{"john@example.com", 2000}]}
        ]}
      ])

      result = MegasPinakas.row_to_map(row)

      assert result == %{
        "cf" => %{
          "name" => "John Doe",
          "email" => "john@example.com"
        }
      }
    end

    test "returns most recent value when multiple versions exist" do
      row = build_test_row("user#123", [
        {"cf", [
          {"name", [{"New Name", 2000}, {"Old Name", 1000}]}
        ]}
      ])

      result = MegasPinakas.row_to_map(row)

      assert result == %{"cf" => %{"name" => "New Name"}}
    end

    test "handles multiple families" do
      row = build_test_row("user#123", [
        {"cf", [{"name", [{"John", 1000}]}]},
        {"metadata", [{"created", [{"2024-01-01", 1000}]}]}
      ])

      result = MegasPinakas.row_to_map(row)

      assert result == %{
        "cf" => %{"name" => "John"},
        "metadata" => %{"created" => "2024-01-01"}
      }
    end

    test "returns empty map for nil" do
      assert MegasPinakas.row_to_map(nil) == %{}
    end

    test "handles empty columns" do
      row = %Row{key: "test", families: [%Family{name: "cf", columns: []}]}

      assert MegasPinakas.row_to_map(row) == %{"cf" => %{}}
    end
  end

  describe "get_cell/3" do
    test "returns the cell value for given family and qualifier" do
      row = build_test_row("user#123", [
        {"cf", [
          {"name", [{"John Doe", 1000}]},
          {"email", [{"john@example.com", 2000}]}
        ]}
      ])

      assert MegasPinakas.get_cell(row, "cf", "name") == "John Doe"
      assert MegasPinakas.get_cell(row, "cf", "email") == "john@example.com"
    end

    test "returns most recent value when multiple versions exist" do
      row = build_test_row("user#123", [
        {"cf", [
          {"name", [{"New Name", 2000}, {"Old Name", 1000}]}
        ]}
      ])

      assert MegasPinakas.get_cell(row, "cf", "name") == "New Name"
    end

    test "returns nil for non-existent family" do
      row = build_test_row("user#123", [
        {"cf", [{"name", [{"John", 1000}]}]}
      ])

      assert MegasPinakas.get_cell(row, "other", "name") == nil
    end

    test "returns nil for non-existent qualifier" do
      row = build_test_row("user#123", [
        {"cf", [{"name", [{"John", 1000}]}]}
      ])

      assert MegasPinakas.get_cell(row, "cf", "other") == nil
    end

    test "returns nil for nil row" do
      assert MegasPinakas.get_cell(nil, "cf", "name") == nil
    end
  end

  describe "get_cells/3" do
    test "returns all cell versions with timestamps" do
      row = build_test_row("user#123", [
        {"cf", [
          {"name", [{"New Name", 2000}, {"Old Name", 1000}]}
        ]}
      ])

      result = MegasPinakas.get_cells(row, "cf", "name")

      assert result == [
        %{value: "New Name", timestamp: 2000},
        %{value: "Old Name", timestamp: 1000}
      ]
    end

    test "returns single cell as list" do
      row = build_test_row("user#123", [
        {"cf", [{"name", [{"John", 1000}]}]}
      ])

      assert MegasPinakas.get_cells(row, "cf", "name") == [%{value: "John", timestamp: 1000}]
    end

    test "returns empty list for non-existent family" do
      row = build_test_row("user#123", [
        {"cf", [{"name", [{"John", 1000}]}]}
      ])

      assert MegasPinakas.get_cells(row, "other", "name") == []
    end

    test "returns empty list for non-existent qualifier" do
      row = build_test_row("user#123", [
        {"cf", [{"name", [{"John", 1000}]}]}
      ])

      assert MegasPinakas.get_cells(row, "cf", "other") == []
    end

    test "returns empty list for nil row" do
      assert MegasPinakas.get_cells(nil, "cf", "name") == []
    end
  end

  describe "get_family/2" do
    test "returns all columns in a family as a map" do
      row = build_test_row("user#123", [
        {"cf", [
          {"name", [{"John Doe", 1000}]},
          {"email", [{"john@example.com", 2000}]}
        ]}
      ])

      result = MegasPinakas.get_family(row, "cf")

      assert result == %{"name" => "John Doe", "email" => "john@example.com"}
    end

    test "returns empty map for non-existent family" do
      row = build_test_row("user#123", [
        {"cf", [{"name", [{"John", 1000}]}]}
      ])

      assert MegasPinakas.get_family(row, "other") == %{}
    end

    test "returns empty map for nil row" do
      assert MegasPinakas.get_family(nil, "cf") == %{}
    end
  end

  describe "row_key/1" do
    test "returns the row key" do
      row = build_test_row("user#123", [])

      assert MegasPinakas.row_key(row) == "user#123"
    end

    test "returns nil for nil row" do
      assert MegasPinakas.row_key(nil) == nil
    end
  end

  describe "rows_to_list/1" do
    test "converts multiple rows to a list of maps" do
      rows = [
        build_test_row("user#1", [{"cf", [{"name", [{"Alice", 1000}]}]}]),
        build_test_row("user#2", [{"cf", [{"name", [{"Bob", 1000}]}]}])
      ]

      result = MegasPinakas.rows_to_list(rows)

      assert result == [
        %{key: "user#1", data: %{"cf" => %{"name" => "Alice"}}},
        %{key: "user#2", data: %{"cf" => %{"name" => "Bob"}}}
      ]
    end

    test "handles empty list" do
      assert MegasPinakas.rows_to_list([]) == []
    end

    test "handles rows with multiple families" do
      rows = [
        build_test_row("row#1", [
          {"cf", [{"col1", [{"val1", 1000}]}]},
          {"meta", [{"created", [{"2024", 1000}]}]}
        ])
      ]

      result = MegasPinakas.rows_to_list(rows)

      assert result == [
        %{key: "row#1", data: %{"cf" => %{"col1" => "val1"}, "meta" => %{"created" => "2024"}}}
      ]
    end
  end
end
