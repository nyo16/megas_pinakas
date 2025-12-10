defmodule MegasPinakas.BatchTest do
  use ExUnit.Case, async: true

  alias MegasPinakas.Batch
  alias MegasPinakas.Row
  alias Google.Bigtable.V2.Mutation

  describe "new/0" do
    test "creates an empty batch" do
      batch = Batch.new()

      assert %Batch{entries: []} = batch
    end
  end

  describe "add/2 with Row struct" do
    test "adds a Row to the batch" do
      row = Row.new("user#123") |> Row.put_string("cf", "name", "John")

      batch = Batch.new() |> Batch.add(row)

      assert Batch.size(batch) == 1
      [entry] = Batch.to_entries(batch)
      assert entry.row_key == "user#123"
      assert length(entry.mutations) == 1
    end

    test "adds multiple rows in order" do
      row1 = Row.new("user#1") |> Row.put_string("cf", "name", "Alice")
      row2 = Row.new("user#2") |> Row.put_string("cf", "name", "Bob")

      batch =
        Batch.new()
        |> Batch.add(row1)
        |> Batch.add(row2)

      assert Batch.size(batch) == 2
      [e1, e2] = Batch.to_entries(batch)
      assert e1.row_key == "user#1"
      assert e2.row_key == "user#2"
    end
  end

  describe "add/3 with row_key and mutations" do
    test "adds a row with inline mutations" do
      mutation = MegasPinakas.set_cell("cf", "name", "John")

      batch = Batch.new() |> Batch.add("user#123", [mutation])

      assert Batch.size(batch) == 1
      [entry] = Batch.to_entries(batch)
      assert entry.row_key == "user#123"
      assert length(entry.mutations) == 1
    end

    test "adds multiple mutations per row" do
      mutations = [
        MegasPinakas.set_cell("cf", "name", "John"),
        MegasPinakas.set_cell("cf", "age", "30")
      ]

      batch = Batch.new() |> Batch.add("user#123", mutations)

      [entry] = Batch.to_entries(batch)
      assert length(entry.mutations) == 2
    end
  end

  describe "add_all/2" do
    test "adds multiple rows at once" do
      rows = [
        Row.new("user#1") |> Row.put_string("cf", "name", "Alice"),
        Row.new("user#2") |> Row.put_string("cf", "name", "Bob"),
        Row.new("user#3") |> Row.put_string("cf", "name", "Charlie")
      ]

      batch = Batch.new() |> Batch.add_all(rows)

      assert Batch.size(batch) == 3
    end

    test "preserves insertion order" do
      rows = [
        Row.new("user#1") |> Row.put_string("cf", "name", "First"),
        Row.new("user#2") |> Row.put_string("cf", "name", "Second"),
        Row.new("user#3") |> Row.put_string("cf", "name", "Third")
      ]

      batch = Batch.new() |> Batch.add_all(rows)
      keys = Batch.row_keys(batch)

      assert keys == ["user#1", "user#2", "user#3"]
    end

    test "handles empty list" do
      batch = Batch.new() |> Batch.add_all([])

      assert Batch.empty?(batch) == true
    end
  end

  describe "to_entries/1" do
    test "returns entries in insertion order" do
      batch =
        Batch.new()
        |> Batch.add(Row.new("first") |> Row.put_string("cf", "col", "1"))
        |> Batch.add(Row.new("second") |> Row.put_string("cf", "col", "2"))
        |> Batch.add(Row.new("third") |> Row.put_string("cf", "col", "3"))

      entries = Batch.to_entries(batch)

      assert length(entries) == 3
      [e1, e2, e3] = entries
      assert e1.row_key == "first"
      assert e2.row_key == "second"
      assert e3.row_key == "third"
    end

    test "returns empty list for empty batch" do
      batch = Batch.new()

      assert Batch.to_entries(batch) == []
    end
  end

  describe "size/1" do
    test "returns zero for empty batch" do
      assert Batch.size(Batch.new()) == 0
    end

    test "returns correct count" do
      batch =
        Batch.new()
        |> Batch.add(Row.new("r1") |> Row.put_string("cf", "c", "v"))
        |> Batch.add(Row.new("r2") |> Row.put_string("cf", "c", "v"))

      assert Batch.size(batch) == 2
    end
  end

  describe "empty?/1" do
    test "returns true for new batch" do
      assert Batch.empty?(Batch.new()) == true
    end

    test "returns false after adding" do
      batch = Batch.new() |> Batch.add(Row.new("key") |> Row.put_string("cf", "c", "v"))

      assert Batch.empty?(batch) == false
    end
  end

  describe "mutation_count/1" do
    test "returns zero for empty batch" do
      assert Batch.mutation_count(Batch.new()) == 0
    end

    test "counts mutations across all rows" do
      batch =
        Batch.new()
        |> Batch.add(
             Row.new("r1")
             |> Row.put_string("cf", "c1", "v1")
             |> Row.put_string("cf", "c2", "v2")
           )
        |> Batch.add(
             Row.new("r2")
             |> Row.put_string("cf", "c1", "v1")
           )

      assert Batch.mutation_count(batch) == 3
    end
  end

  describe "row_keys/1" do
    test "returns empty list for empty batch" do
      assert Batch.row_keys(Batch.new()) == []
    end

    test "returns keys in insertion order" do
      batch =
        Batch.new()
        |> Batch.add(Row.new("z") |> Row.put_string("cf", "c", "v"))
        |> Batch.add(Row.new("a") |> Row.put_string("cf", "c", "v"))
        |> Batch.add(Row.new("m") |> Row.put_string("cf", "c", "v"))

      assert Batch.row_keys(batch) == ["z", "a", "m"]
    end
  end

  describe "clear/1" do
    test "removes all entries" do
      batch =
        Batch.new()
        |> Batch.add(Row.new("r1") |> Row.put_string("cf", "c", "v"))
        |> Batch.add(Row.new("r2") |> Row.put_string("cf", "c", "v"))

      cleared = Batch.clear(batch)

      assert Batch.empty?(cleared) == true
      assert Batch.size(cleared) == 0
    end
  end

  describe "chaining operations" do
    test "supports fluent API with mixed add types" do
      row1 = Row.new("user#1") |> Row.put_string("cf", "name", "Alice")
      mutation = MegasPinakas.set_cell("cf", "name", "Bob")

      batch =
        Batch.new()
        |> Batch.add(row1)
        |> Batch.add("user#2", [mutation])

      assert Batch.size(batch) == 2
      assert Batch.row_keys(batch) == ["user#1", "user#2"]
    end
  end

  describe "module exports" do
    test "exports all expected functions" do
      functions = Batch.__info__(:functions)

      assert {:new, 0} in functions
      assert {:add, 2} in functions
      assert {:add, 3} in functions
      assert {:add_all, 2} in functions
      assert {:write, 4} in functions
      assert {:write, 5} in functions
      assert {:to_entries, 1} in functions
      assert {:size, 1} in functions
      assert {:empty?, 1} in functions
      assert {:mutation_count, 1} in functions
      assert {:row_keys, 1} in functions
      assert {:clear, 1} in functions
    end
  end
end
