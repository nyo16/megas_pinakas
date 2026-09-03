defmodule MegasPinakasEmulatorTest do
  @moduledoc """
  Exercises the single-row data RPCs that have no other emulator coverage:
  `check_and_mutate_row/8`, `read_modify_write_row/6`, `mutate_rows/5` entry
  normalization, `row_range_prefix/1` at the 0xFF edge, and
  `Types.read_cells/6` result shapes.
  """

  use ExUnit.Case, async: false

  alias MegasPinakas.Batch
  alias MegasPinakas.Filter
  alias MegasPinakas.Row
  alias MegasPinakas.Test.Emulator
  alias MegasPinakas.Types

  @moduletag :emulator

  @table "datapath_data_ops_test"

  setup do
    Emulator.setup_table(@table, ["cf"])
    :ok
  end

  defp p, do: Emulator.project()
  defp i, do: Emulator.instance()

  defp write(key, qualifier, value) do
    {:ok, _} =
      MegasPinakas.mutate_row(p(), i(), @table, key, [
        MegasPinakas.set_cell("cf", qualifier, value)
      ])
  end

  defp cell(key, qualifier) do
    {:ok, row} = MegasPinakas.read_row(p(), i(), @table, key)
    MegasPinakas.get_cell(row, "cf", qualifier)
  end

  describe "check_and_mutate_row/8" do
    test "applies true_mutations when the predicate matches" do
      write("cam#1", "state", "active")

      assert {:ok, %{predicate_matched: true}} =
               MegasPinakas.check_and_mutate_row(
                 p(),
                 i(),
                 @table,
                 "cam#1",
                 Filter.column_filter("cf", "state"),
                 [MegasPinakas.set_cell("cf", "seen", "yes")],
                 [MegasPinakas.set_cell("cf", "seen", "no")]
               )

      assert cell("cam#1", "seen") == "yes"
    end

    test "applies false_mutations when the predicate does not match" do
      assert {:ok, %{predicate_matched: false}} =
               MegasPinakas.check_and_mutate_row(
                 p(),
                 i(),
                 @table,
                 "cam#missing",
                 Filter.column_filter("cf", "state"),
                 [MegasPinakas.set_cell("cf", "seen", "yes")],
                 [MegasPinakas.set_cell("cf", "seen", "no")]
               )

      assert cell("cam#missing", "seen") == "no"
    end

    test "an empty branch applies nothing" do
      write("cam#2", "state", "active")

      assert {:ok, %{predicate_matched: false}} =
               MegasPinakas.check_and_mutate_row(
                 p(),
                 i(),
                 @table,
                 "cam#2",
                 Filter.column_filter("cf", "other"),
                 [MegasPinakas.set_cell("cf", "state", "clobbered")],
                 []
               )

      assert cell("cam#2", "state") == "active"
    end
  end

  describe "read_modify_write_row/6" do
    test "increment on a missing cell starts from zero" do
      assert {:ok, response} =
               MegasPinakas.read_modify_write_row(p(), i(), @table, "rmw#1", [
                 MegasPinakas.increment_rule("cf", "n", 5)
               ])

      assert MegasPinakas.get_cell(response.row, "cf", "n") == Types.encode(:integer, 5)
      assert Types.decode(:integer, cell("rmw#1", "n")) == {:ok, 5}
    end

    test "increment adds to an existing 64-bit big-endian value" do
      write("rmw#2", "n", Types.encode(:integer, 40))

      assert {:ok, response} =
               MegasPinakas.read_modify_write_row(p(), i(), @table, "rmw#2", [
                 MegasPinakas.increment_rule("cf", "n", 2)
               ])

      assert Types.decode(:integer, MegasPinakas.get_cell(response.row, "cf", "n")) == {:ok, 42}
      assert Types.decode(:integer, cell("rmw#2", "n")) == {:ok, 42}
    end

    test "append concatenates onto the existing value" do
      write("rmw#3", "log", "a")

      assert {:ok, response} =
               MegasPinakas.read_modify_write_row(p(), i(), @table, "rmw#3", [
                 MegasPinakas.append_rule("cf", "log", "b")
               ])

      assert MegasPinakas.get_cell(response.row, "cf", "log") == "ab"
      assert cell("rmw#3", "log") == "ab"
    end
  end

  describe "mutate_rows/5 entry normalization" do
    test "accepts %Row{} structs, atom-key maps, and string-key maps in one call" do
      entries = [
        Row.new("mr#row") |> Row.put_string("cf", "v", "from-row"),
        %{row_key: "mr#atom", mutations: [MegasPinakas.set_cell("cf", "v", "from-atom")]},
        %{
          "row_key" => "mr#string",
          "mutations" => [MegasPinakas.set_cell("cf", "v", "from-string")]
        }
      ]

      assert {:ok, results} = MegasPinakas.mutate_rows(p(), i(), @table, entries)
      assert Enum.map(results, & &1.index) == [0, 1, 2]
      assert Enum.all?(results, &(&1.status.code == 0))

      assert cell("mr#row", "v") == "from-row"
      assert cell("mr#atom", "v") == "from-atom"
      assert cell("mr#string", "v") == "from-string"
    end
  end

  describe "Batch.write/5" do
    test "an empty batch makes no request" do
      assert Batch.new() |> Batch.write(p(), i(), @table) == {:ok, []}
    end

    test "results are indexed by batch position" do
      batch =
        Batch.new()
        |> Batch.add("b#1", [MegasPinakas.set_cell("cf", "v", "1")])
        |> Batch.add("b#2", [MegasPinakas.set_cell("cf", "v", "2")])
        |> Batch.add("b#3", [MegasPinakas.set_cell("cf", "v", "3")])

      assert {:ok, results} = Batch.write(batch, p(), i(), @table)
      assert Enum.map(results, & &1.index) == [0, 1, 2]
      assert cell("b#3", "v") == "3"
    end
  end

  describe "row_range_prefix/1" do
    test "an all-0xFF prefix scans to the end of the table" do
      write(<<255, 255>>, "v", "ff")
      write(<<255, 255, 1>>, "v", "ff01")
      write(<<254>>, "v", "fe")

      assert {:ok, rows} =
               MegasPinakas.read_rows(p(), i(), @table,
                 rows:
                   MegasPinakas.row_set_from_ranges([MegasPinakas.row_range_prefix(<<255, 255>>)])
               )

      assert Enum.map(rows, &MegasPinakas.row_key/1) == [<<255, 255>>, <<255, 255, 1>>]
    end
  end

  describe "Types.read_cells/6" do
    test "returns every requested key, nil for missing cells" do
      {:ok, _} = Types.write_cells(p(), i(), @table, "rc#1", [{:string, "cf", "name", "Ann"}])

      assert Types.read_cells(p(), i(), @table, "rc#1", [
               {:string, "cf", "name"},
               {:integer, "cf", "age"}
             ]) == {:ok, %{"cf:name" => "Ann", "cf:age" => nil}}
    end

    test "a missing row has the same shape as a row with missing cells" do
      assert Types.read_cells(p(), i(), @table, "rc#nope", [
               {:string, "cf", "name"},
               {:integer, "cf", "age"}
             ]) == {:ok, %{"cf:name" => nil, "cf:age" => nil}}
    end

    test "a cell that does not decode fails the call instead of reading as nil" do
      write("rc#2", "age", "not eight bytes")

      assert Types.read_cells(p(), i(), @table, "rc#2", [{:integer, "cf", "age"}]) ==
               {:error, {:decode, "cf:age", :invalid_integer_format}}
    end
  end
end
