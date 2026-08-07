defmodule MegasPinakas.RowKeyTest do
  use ExUnit.Case, async: true

  alias MegasPinakas.RowKey

  describe "split_suffix/1" do
    test "splits a key into its prefix and trailing segment" do
      assert RowKey.split_suffix("mykey#1704067200") == {"mykey", "1704067200"}
    end

    test "keeps interior separators in the prefix" do
      assert RowKey.split_suffix("api#v1#endpoint#1704067200") ==
               {"api#v1#endpoint", "1704067200"}
    end

    test "returns an empty prefix when the key has no separator" do
      assert RowKey.split_suffix("1704067200") == {"", "1704067200"}
    end

    test "returns an empty suffix for a trailing separator" do
      assert RowKey.split_suffix("mykey#") == {"mykey", ""}
    end

    test "handles an empty key" do
      assert RowKey.split_suffix("") == {"", ""}
    end
  end
end
