defmodule MegasPinakas.TypesTest do
  use ExUnit.Case, async: true

  import Bitwise, only: [<<<: 2]

  alias Google.Bigtable.V2.Mutation
  alias MegasPinakas.Types

  describe "encode/2" do
    test "encodes binary as-is" do
      assert Types.encode(:binary, "hello") == "hello"
      assert Types.encode(:binary, <<1, 2, 3>>) == <<1, 2, 3>>
    end

    test "encodes string as-is" do
      assert Types.encode(:string, "hello") == "hello"
      assert Types.encode(:string, "こんにちは") == "こんにちは"
    end

    test "encodes map as JSON" do
      encoded = Types.encode(:json, %{name: "John", age: 30})
      assert Jason.decode!(encoded) == %{"name" => "John", "age" => 30}
    end

    test "encodes list as JSON" do
      assert Types.encode(:json, [1, 2, 3]) == "[1,2,3]"
    end

    test "encodes nested map as JSON" do
      data = %{user: %{name: "John", tags: ["admin", "user"]}}
      encoded = Types.encode(:json, data)

      assert Jason.decode!(encoded) == %{
               "user" => %{"name" => "John", "tags" => ["admin", "user"]}
             }
    end

    test "encodes positive integer as 64-bit big-endian" do
      assert Types.encode(:integer, 42) == <<0, 0, 0, 0, 0, 0, 0, 42>>
      assert Types.encode(:integer, 256) == <<0, 0, 0, 0, 0, 0, 1, 0>>
    end

    test "encodes negative integer as signed 64-bit big-endian" do
      encoded = Types.encode(:integer, -1)
      assert byte_size(encoded) == 8
      # -1 in two's complement 64-bit is all 1s
      assert encoded == <<255, 255, 255, 255, 255, 255, 255, 255>>
    end

    test "encodes zero integer" do
      assert Types.encode(:integer, 0) == <<0, 0, 0, 0, 0, 0, 0, 0>>
    end

    test "encodes large integer" do
      # Max int64
      large = 9_223_372_036_854_775_807
      encoded = Types.encode(:integer, large)
      assert byte_size(encoded) == 8
    end

    test "encodes float as 64-bit IEEE 754" do
      encoded = Types.encode(:float, 3.14)
      assert byte_size(encoded) == 8
    end

    test "encodes zero float" do
      encoded = Types.encode(:float, 0.0)
      assert byte_size(encoded) == 8
    end

    test "encodes negative float" do
      encoded = Types.encode(:float, -273.15)
      assert byte_size(encoded) == 8
    end

    test "encodes true boolean" do
      assert Types.encode(:boolean, true) == <<1>>
    end

    test "encodes false boolean" do
      assert Types.encode(:boolean, false) == <<0>>
    end

    test "encodes DateTime as microseconds since epoch" do
      dt = ~U[2024-01-15 10:30:00.123456Z]
      encoded = Types.encode(:datetime, dt)
      assert byte_size(encoded) == 8
      <<micros::signed-big-64>> = encoded
      assert micros == DateTime.to_unix(dt, :microsecond)
    end

    test "encodes DateTime at Unix epoch" do
      dt = ~U[1970-01-01 00:00:00Z]
      assert Types.encode(:datetime, dt) == <<0, 0, 0, 0, 0, 0, 0, 0>>
    end

    test "encodes Elixir term" do
      term = {:ok, [1, 2, %{a: "b"}]}
      encoded = Types.encode(:term, term)
      assert is_binary(encoded)
      assert :erlang.binary_to_term(encoded) == term
    end

    test "encodes complex term with tuples and structs" do
      term = %{date: ~D[2024-01-15], time: ~T[10:30:00]}
      encoded = Types.encode(:term, term)
      assert :erlang.binary_to_term(encoded) == term
    end
  end

  describe "decode/2" do
    test "decodes binary as-is" do
      assert Types.decode(:binary, "hello") == {:ok, "hello"}
      assert Types.decode(:binary, <<1, 2, 3>>) == {:ok, <<1, 2, 3>>}
    end

    test "decodes string as-is" do
      assert Types.decode(:string, "hello") == {:ok, "hello"}
      assert Types.decode(:string, "こんにちは") == {:ok, "こんにちは"}
    end

    test "decodes JSON to map" do
      assert Types.decode(:json, ~s({"name":"John"})) == {:ok, %{"name" => "John"}}
    end

    test "decodes JSON to list" do
      assert Types.decode(:json, "[1,2,3]") == {:ok, [1, 2, 3]}
    end

    test "returns error for invalid JSON" do
      assert {:error, _} = Types.decode(:json, "not json")
    end

    test "decodes positive integer" do
      assert Types.decode(:integer, <<0, 0, 0, 0, 0, 0, 0, 42>>) == {:ok, 42}
    end

    test "decodes negative integer" do
      assert Types.decode(:integer, <<255, 255, 255, 255, 255, 255, 255, 255>>) == {:ok, -1}
    end

    test "decodes zero integer" do
      assert Types.decode(:integer, <<0, 0, 0, 0, 0, 0, 0, 0>>) == {:ok, 0}
    end

    test "returns error for invalid integer format" do
      assert Types.decode(:integer, <<1, 2, 3>>) == {:error, :invalid_integer_format}
      assert Types.decode(:integer, "not binary") == {:error, :invalid_integer_format}
    end

    test "decodes float" do
      encoded = Types.encode(:float, 3.14)
      {:ok, decoded} = Types.decode(:float, encoded)
      assert_in_delta decoded, 3.14, 0.0001
    end

    test "returns error for invalid float format" do
      assert Types.decode(:float, <<1, 2, 3>>) == {:error, :invalid_float_format}
    end

    test "decodes true boolean" do
      assert Types.decode(:boolean, <<1>>) == {:ok, true}
    end

    test "decodes false boolean" do
      assert Types.decode(:boolean, <<0>>) == {:ok, false}
    end

    test "returns error for invalid boolean format" do
      assert Types.decode(:boolean, <<2>>) == {:error, :invalid_boolean_format}
      assert Types.decode(:boolean, <<>>) == {:error, :invalid_boolean_format}
    end

    test "decodes DateTime" do
      dt = ~U[2024-01-15 10:30:00.123456Z]
      encoded = Types.encode(:datetime, dt)
      assert Types.decode(:datetime, encoded) == {:ok, dt}
    end

    test "decodes DateTime at Unix epoch" do
      assert Types.decode(:datetime, <<0, 0, 0, 0, 0, 0, 0, 0>>) ==
               {:ok, ~U[1970-01-01 00:00:00.000000Z]}
    end

    test "returns error for invalid datetime format" do
      assert Types.decode(:datetime, <<1, 2, 3>>) == {:error, :invalid_datetime_format}
    end

    test "decodes Elixir term" do
      term = {:ok, [1, 2, %{a: "b"}]}
      encoded = Types.encode(:term, term)
      assert Types.decode(:term, encoded) == {:ok, term}
    end

    test "returns error for a binary that is not a term" do
      assert Types.decode(:term, "not a valid term binary") == {:error, :unsafe_or_invalid_term}
    end

    test "refuses to create atoms that do not already exist" do
      # Hand-build SMALL_ATOM_UTF8_EXT for a name never spelled as a literal, so
      # the atom cannot already be in this node's table.
      name = "megas_pinakas_unseen_atom_#{System.unique_integer([:positive])}"
      unseen = <<131, 119, byte_size(name), name::binary>>

      assert Types.decode(:term, unseen) == {:error, :unsafe_or_invalid_term}
    end
  end

  describe "decode!/2" do
    test "returns decoded value on success" do
      assert Types.decode!(:integer, <<0, 0, 0, 0, 0, 0, 0, 42>>) == 42
      assert Types.decode!(:boolean, <<1>>) == true
    end

    test "raises on decode error" do
      assert_raise ArgumentError, fn ->
        Types.decode!(:integer, <<1, 2, 3>>)
      end
    end
  end

  describe "set_json/4" do
    test "creates SetCell mutation with JSON-encoded value" do
      mutation = Types.set_json("cf", "data", %{name: "John"})

      assert %Mutation{mutation: {:set_cell, set_cell}} = mutation
      assert set_cell.family_name == "cf"
      assert set_cell.column_qualifier == "data"
      assert Jason.decode!(set_cell.value) == %{"name" => "John"}
    end

    test "handles nested data" do
      mutation = Types.set_json("cf", "data", %{users: [%{id: 1}, %{id: 2}]})

      assert %Mutation{mutation: {:set_cell, set_cell}} = mutation
      decoded = Jason.decode!(set_cell.value)
      assert decoded["users"] == [%{"id" => 1}, %{"id" => 2}]
    end
  end

  describe "set_integer/4" do
    test "creates SetCell mutation with encoded integer" do
      mutation = Types.set_integer("cf", "count", 42)

      assert %Mutation{mutation: {:set_cell, set_cell}} = mutation
      assert set_cell.family_name == "cf"
      assert set_cell.column_qualifier == "count"
      assert set_cell.value == <<0, 0, 0, 0, 0, 0, 0, 42>>
    end

    test "handles negative integers" do
      mutation = Types.set_integer("cf", "count", -100)

      assert %Mutation{mutation: {:set_cell, set_cell}} = mutation
      {:ok, decoded} = Types.decode(:integer, set_cell.value)
      assert decoded == -100
    end
  end

  describe "set_float/4" do
    test "creates SetCell mutation with encoded float" do
      mutation = Types.set_float("cf", "score", 3.14)

      assert %Mutation{mutation: {:set_cell, set_cell}} = mutation
      assert set_cell.family_name == "cf"
      assert set_cell.column_qualifier == "score"
      {:ok, decoded} = Types.decode(:float, set_cell.value)
      assert_in_delta decoded, 3.14, 0.0001
    end
  end

  describe "set_boolean/4" do
    test "creates SetCell mutation with true" do
      mutation = Types.set_boolean("cf", "active", true)

      assert %Mutation{mutation: {:set_cell, set_cell}} = mutation
      assert set_cell.value == <<1>>
    end

    test "creates SetCell mutation with false" do
      mutation = Types.set_boolean("cf", "active", false)

      assert %Mutation{mutation: {:set_cell, set_cell}} = mutation
      assert set_cell.value == <<0>>
    end
  end

  describe "set_datetime/4" do
    test "creates SetCell mutation with encoded datetime" do
      dt = ~U[2024-01-15 10:30:00Z]
      mutation = Types.set_datetime("cf", "created", dt)

      assert %Mutation{mutation: {:set_cell, set_cell}} = mutation
      {:ok, decoded} = Types.decode(:datetime, set_cell.value)
      assert DateTime.truncate(decoded, :second) == dt
    end
  end

  describe "set_term/4" do
    test "creates SetCell mutation with encoded term" do
      term = {:tuple, [1, 2, 3], %{key: "value"}}
      mutation = Types.set_term("cf", "data", term)

      assert %Mutation{mutation: {:set_cell, set_cell}} = mutation
      {:ok, decoded} = Types.decode(:term, set_cell.value)
      assert decoded == term
    end
  end

  describe "roundtrip encoding" do
    test "integer roundtrip preserves value" do
      for val <- [-1_000_000, -1, 0, 1, 1_000_000] do
        encoded = Types.encode(:integer, val)
        assert {:ok, ^val} = Types.decode(:integer, encoded)
      end
    end

    test "integer roundtrip at the signed 64-bit boundaries" do
      min = -9_223_372_036_854_775_808
      max = 9_223_372_036_854_775_807

      assert Types.encode(:integer, min) == <<0x80, 0, 0, 0, 0, 0, 0, 0>>
      assert Types.encode(:integer, max) == <<0x7F, 255, 255, 255, 255, 255, 255, 255>>
      assert Types.decode(:integer, Types.encode(:integer, min)) == {:ok, min}
      assert Types.decode(:integer, Types.encode(:integer, max)) == {:ok, max}
    end

    test "integer overflow raises instead of silently truncating" do
      # <<v::signed-big-64>> would wrap 2^63 to -2^63 without complaint.
      assert_raise ArgumentError, ~r/9223372036854775808 does not fit/, fn ->
        Types.encode(:integer, 9_223_372_036_854_775_808)
      end

      assert_raise ArgumentError, ~r/-9223372036854775809 does not fit/, fn ->
        Types.encode(:integer, -9_223_372_036_854_775_809)
      end

      assert_raise ArgumentError, fn -> Types.set_integer("cf", "n", 1 <<< 64) end
    end

    test "float roundtrip preserves value" do
      for val <- [-273.15, 0.0, 3.14159, 1.0e10] do
        encoded = Types.encode(:float, val)
        {:ok, decoded} = Types.decode(:float, encoded)
        assert_in_delta decoded, val, 0.0001
      end
    end

    test "boolean roundtrip preserves value" do
      for val <- [true, false] do
        encoded = Types.encode(:boolean, val)
        assert {:ok, ^val} = Types.decode(:boolean, encoded)
      end
    end

    test "datetime roundtrip preserves value" do
      for dt <- [
            ~U[1970-01-01 00:00:00Z],
            ~U[2024-01-15 10:30:00.123456Z],
            ~U[2099-12-31 23:59:59Z]
          ] do
        encoded = Types.encode(:datetime, dt)
        {:ok, decoded} = Types.decode(:datetime, encoded)
        # Compare at microsecond precision
        assert DateTime.to_unix(decoded, :microsecond) == DateTime.to_unix(dt, :microsecond)
      end
    end

    test "json roundtrip preserves structure" do
      data = %{
        "string" => "hello",
        "number" => 42,
        "float" => 3.14,
        "bool" => true,
        "null" => nil,
        "array" => [1, 2, 3],
        "nested" => %{"a" => "b"}
      }

      encoded = Types.encode(:json, data)
      {:ok, decoded} = Types.decode(:json, encoded)
      assert decoded == data
    end

    test "term roundtrip preserves complex structures" do
      term = {
        :ok,
        %{
          date: ~D[2024-01-15],
          time: ~T[10:30:00],
          list: [1, 2, 3],
          tuple: {:a, :b, :c}
        }
      }

      encoded = Types.encode(:term, term)
      {:ok, decoded} = Types.decode(:term, encoded)
      assert decoded == term
    end
  end

  describe "integer sortability" do
    test "encoded integers maintain sort order for non-negative numbers" do
      values = [0, 1, 100, 1000, 1_000_000, 9_223_372_036_854_775_807]
      encoded = Enum.map(values, &Types.encode(:integer, &1))

      assert Enum.sort(encoded) == encoded
    end

    test "negative integers sort after every non-negative one (two's complement sign bit)" do
      # This is the documented limitation: the encoding is not order-preserving
      # across zero, so callers who need that must offset their values.
      negative = Types.encode(:integer, -1)
      positive = Types.encode(:integer, 9_223_372_036_854_775_807)

      assert negative > positive
      assert Types.decode(:integer, negative) == {:ok, -1}
    end
  end
end
