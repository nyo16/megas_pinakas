defmodule MegasPinakas.RowTest do
  use ExUnit.Case, async: true

  alias MegasPinakas.Row
  alias MegasPinakas.Types
  alias Google.Bigtable.V2.Mutation

  describe "new/1" do
    test "creates a row with the given key" do
      row = Row.new("user#123")

      assert %Row{row_key: "user#123", mutations: []} = row
    end

    test "creates a row with binary key" do
      row = Row.new(<<1, 2, 3>>)

      assert row.row_key == <<1, 2, 3>>
    end
  end

  describe "put/5 type inference" do
    test "infers string type for binary values" do
      row = Row.new("key") |> Row.put("cf", "col", "hello")

      [mutation] = Row.to_mutations(row)
      assert %Mutation{mutation: {:set_cell, set_cell}} = mutation
      assert set_cell.value == "hello"
    end

    test "infers integer type for integer values" do
      row = Row.new("key") |> Row.put("cf", "col", 42)

      [mutation] = Row.to_mutations(row)
      assert %Mutation{mutation: {:set_cell, set_cell}} = mutation
      assert set_cell.value == <<0, 0, 0, 0, 0, 0, 0, 42>>
    end

    test "infers float type for float values" do
      row = Row.new("key") |> Row.put("cf", "col", 3.14)

      [mutation] = Row.to_mutations(row)
      assert %Mutation{mutation: {:set_cell, set_cell}} = mutation
      {:ok, decoded} = Types.decode(:float, set_cell.value)
      assert_in_delta decoded, 3.14, 0.0001
    end

    test "infers boolean type for boolean values" do
      row = Row.new("key") |> Row.put("cf", "col", true)

      [mutation] = Row.to_mutations(row)
      assert %Mutation{mutation: {:set_cell, set_cell}} = mutation
      assert set_cell.value == <<1>>
    end

    test "infers JSON type for map values" do
      row = Row.new("key") |> Row.put("cf", "col", %{name: "John"})

      [mutation] = Row.to_mutations(row)
      assert %Mutation{mutation: {:set_cell, set_cell}} = mutation
      assert Jason.decode!(set_cell.value) == %{"name" => "John"}
    end

    test "infers JSON type for list values" do
      row = Row.new("key") |> Row.put("cf", "col", [1, 2, 3])

      [mutation] = Row.to_mutations(row)
      assert %Mutation{mutation: {:set_cell, set_cell}} = mutation
      assert Jason.decode!(set_cell.value) == [1, 2, 3]
    end

    test "infers datetime type for DateTime values" do
      dt = ~U[2024-01-15 10:30:00Z]
      row = Row.new("key") |> Row.put("cf", "col", dt)

      [mutation] = Row.to_mutations(row)
      assert %Mutation{mutation: {:set_cell, set_cell}} = mutation
      {:ok, decoded} = Types.decode(:datetime, set_cell.value)
      assert DateTime.truncate(decoded, :second) == dt
    end
  end

  describe "put_string/5" do
    test "adds a string mutation" do
      row = Row.new("key") |> Row.put_string("cf", "name", "John")

      [mutation] = Row.to_mutations(row)
      assert %Mutation{mutation: {:set_cell, set_cell}} = mutation
      assert set_cell.family_name == "cf"
      assert set_cell.column_qualifier == "name"
      assert set_cell.value == "John"
    end
  end

  describe "put_binary/5" do
    test "adds a binary mutation" do
      row = Row.new("key") |> Row.put_binary("cf", "data", <<1, 2, 3>>)

      [mutation] = Row.to_mutations(row)
      assert %Mutation{mutation: {:set_cell, set_cell}} = mutation
      assert set_cell.value == <<1, 2, 3>>
    end
  end

  describe "put_json/5" do
    test "adds a JSON-encoded mutation" do
      row = Row.new("key") |> Row.put_json("cf", "profile", %{age: 30, city: "NYC"})

      [mutation] = Row.to_mutations(row)
      assert %Mutation{mutation: {:set_cell, set_cell}} = mutation
      assert Jason.decode!(set_cell.value) == %{"age" => 30, "city" => "NYC"}
    end
  end

  describe "put_integer/5" do
    test "adds an integer mutation" do
      row = Row.new("key") |> Row.put_integer("cf", "count", 1000)

      [mutation] = Row.to_mutations(row)
      assert %Mutation{mutation: {:set_cell, set_cell}} = mutation
      {:ok, decoded} = Types.decode(:integer, set_cell.value)
      assert decoded == 1000
    end

    test "handles negative integers" do
      row = Row.new("key") |> Row.put_integer("cf", "balance", -500)

      [mutation] = Row.to_mutations(row)
      assert %Mutation{mutation: {:set_cell, set_cell}} = mutation
      {:ok, decoded} = Types.decode(:integer, set_cell.value)
      assert decoded == -500
    end
  end

  describe "put_float/5" do
    test "adds a float mutation" do
      row = Row.new("key") |> Row.put_float("cf", "score", 98.5)

      [mutation] = Row.to_mutations(row)
      assert %Mutation{mutation: {:set_cell, set_cell}} = mutation
      {:ok, decoded} = Types.decode(:float, set_cell.value)
      assert_in_delta decoded, 98.5, 0.0001
    end
  end

  describe "put_boolean/5" do
    test "adds true boolean mutation" do
      row = Row.new("key") |> Row.put_boolean("cf", "active", true)

      [mutation] = Row.to_mutations(row)
      assert %Mutation{mutation: {:set_cell, set_cell}} = mutation
      assert set_cell.value == <<1>>
    end

    test "adds false boolean mutation" do
      row = Row.new("key") |> Row.put_boolean("cf", "active", false)

      [mutation] = Row.to_mutations(row)
      assert %Mutation{mutation: {:set_cell, set_cell}} = mutation
      assert set_cell.value == <<0>>
    end
  end

  describe "put_datetime/5" do
    test "adds a datetime mutation" do
      dt = ~U[2024-01-15 10:30:00.123456Z]
      row = Row.new("key") |> Row.put_datetime("cf", "created", dt)

      [mutation] = Row.to_mutations(row)
      assert %Mutation{mutation: {:set_cell, set_cell}} = mutation
      {:ok, decoded} = Types.decode(:datetime, set_cell.value)
      assert decoded == dt
    end
  end

  describe "put_term/5" do
    test "adds an Elixir term mutation" do
      term = {:ok, %{data: [1, 2, 3]}}
      row = Row.new("key") |> Row.put_term("cf", "data", term)

      [mutation] = Row.to_mutations(row)
      assert %Mutation{mutation: {:set_cell, set_cell}} = mutation
      {:ok, decoded} = Types.decode(:term, set_cell.value)
      assert decoded == term
    end
  end

  describe "delete_cell/3" do
    test "adds a delete cell mutation" do
      row = Row.new("key") |> Row.delete_cell("cf", "col")

      [mutation] = Row.to_mutations(row)
      assert %Mutation{mutation: {:delete_from_column, delete}} = mutation
      assert delete.family_name == "cf"
      assert delete.column_qualifier == "col"
    end
  end

  describe "delete_family/2" do
    test "adds a delete family mutation" do
      row = Row.new("key") |> Row.delete_family("cf")

      [mutation] = Row.to_mutations(row)
      assert %Mutation{mutation: {:delete_from_family, delete}} = mutation
      assert delete.family_name == "cf"
    end
  end

  describe "delete_row/1" do
    test "adds a delete row mutation" do
      row = Row.new("key") |> Row.delete_row()

      [mutation] = Row.to_mutations(row)
      assert %Mutation{mutation: {:delete_from_row, _}} = mutation
    end
  end

  describe "to_mutations/1" do
    test "returns mutations in insertion order" do
      row =
        Row.new("key")
        |> Row.put_string("cf", "col1", "first")
        |> Row.put_string("cf", "col2", "second")
        |> Row.put_string("cf", "col3", "third")

      mutations = Row.to_mutations(row)
      assert length(mutations) == 3

      [m1, m2, m3] = mutations
      assert m1.mutation |> elem(1) |> Map.get(:column_qualifier) == "col1"
      assert m2.mutation |> elem(1) |> Map.get(:column_qualifier) == "col2"
      assert m3.mutation |> elem(1) |> Map.get(:column_qualifier) == "col3"
    end
  end

  describe "to_entry/1" do
    test "returns entry map for batch operations" do
      row =
        Row.new("user#123")
        |> Row.put_string("cf", "name", "John")

      entry = Row.to_entry(row)

      assert entry.row_key == "user#123"
      assert length(entry.mutations) == 1
    end
  end

  describe "row_key/1" do
    test "returns the row key" do
      row = Row.new("user#456")

      assert Row.row_key(row) == "user#456"
    end
  end

  describe "mutation_count/1" do
    test "returns zero for empty row" do
      row = Row.new("key")

      assert Row.mutation_count(row) == 0
    end

    test "returns correct count after mutations" do
      row =
        Row.new("key")
        |> Row.put_string("cf", "col1", "a")
        |> Row.put_string("cf", "col2", "b")
        |> Row.put_string("cf", "col3", "c")

      assert Row.mutation_count(row) == 3
    end
  end

  describe "empty?/1" do
    test "returns true for new row" do
      row = Row.new("key")

      assert Row.empty?(row) == true
    end

    test "returns false after adding mutations" do
      row = Row.new("key") |> Row.put_string("cf", "col", "val")

      assert Row.empty?(row) == false
    end
  end

  describe "chaining multiple operations" do
    test "supports fluent API with multiple put types" do
      dt = ~U[2024-01-15 10:30:00Z]

      row =
        Row.new("user#123")
        |> Row.put_string("cf", "name", "John Doe")
        |> Row.put_integer("cf", "age", 30)
        |> Row.put_float("cf", "score", 98.5)
        |> Row.put_boolean("cf", "active", true)
        |> Row.put_json("cf", "profile", %{city: "NYC"})
        |> Row.put_datetime("cf", "created", dt)

      assert Row.row_key(row) == "user#123"
      assert Row.mutation_count(row) == 6
      assert Row.empty?(row) == false

      mutations = Row.to_mutations(row)
      assert length(mutations) == 6
    end

    test "supports mixed put and delete operations" do
      row =
        Row.new("key")
        |> Row.put_string("cf", "name", "John")
        |> Row.delete_cell("cf", "old_field")
        |> Row.put_integer("cf", "count", 1)

      mutations = Row.to_mutations(row)
      assert length(mutations) == 3

      [m1, m2, m3] = mutations
      assert match?(%Mutation{mutation: {:set_cell, _}}, m1)
      assert match?(%Mutation{mutation: {:delete_from_column, _}}, m2)
      assert match?(%Mutation{mutation: {:set_cell, _}}, m3)
    end
  end

  describe "module exports" do
    test "exports all expected functions" do
      functions = Row.__info__(:functions)

      assert {:new, 1} in functions
      assert {:put, 4} in functions
      assert {:put, 5} in functions
      assert {:put_binary, 4} in functions
      assert {:put_binary, 5} in functions
      assert {:put_string, 4} in functions
      assert {:put_string, 5} in functions
      assert {:put_json, 4} in functions
      assert {:put_json, 5} in functions
      assert {:put_integer, 4} in functions
      assert {:put_integer, 5} in functions
      assert {:put_float, 4} in functions
      assert {:put_float, 5} in functions
      assert {:put_boolean, 4} in functions
      assert {:put_boolean, 5} in functions
      assert {:put_datetime, 4} in functions
      assert {:put_datetime, 5} in functions
      assert {:put_term, 4} in functions
      assert {:put_term, 5} in functions
      assert {:delete_cell, 3} in functions
      assert {:delete_family, 2} in functions
      assert {:delete_row, 1} in functions
      assert {:write, 4} in functions
      assert {:write, 5} in functions
      assert {:to_mutations, 1} in functions
      assert {:to_entry, 1} in functions
      assert {:row_key, 1} in functions
      assert {:mutation_count, 1} in functions
      assert {:empty?, 1} in functions
    end
  end
end
