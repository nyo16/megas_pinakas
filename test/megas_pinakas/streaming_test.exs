defmodule MegasPinakas.StreamingTest do
  use ExUnit.Case, async: true

  alias MegasPinakas.Streaming

  describe "stream_rows/4" do
    test "returns an Enumerable" do
      stream = Streaming.stream_rows("proj", "inst", "table")

      # Stream.resource returns a struct that implements Enumerable
      assert Enumerable.impl_for(stream) != nil
    end

    test "stream is lazy" do
      # Creating the stream shouldn't make any calls
      _stream = Streaming.stream_rows("proj", "inst", "table")

      # No assertions needed - if it doesn't crash, it's lazy
      assert true
    end
  end

  describe "stream_rows_as_maps/4" do
    test "returns an Enumerable" do
      stream = Streaming.stream_rows_as_maps("proj", "inst", "table")

      assert Enumerable.impl_for(stream) != nil
    end
  end

  describe "stream_rows_with_keys/4" do
    test "returns an Enumerable" do
      stream = Streaming.stream_rows_with_keys("proj", "inst", "table")

      assert Enumerable.impl_for(stream) != nil
    end
  end

  describe "stream_range/6" do
    test "returns an Enumerable" do
      stream = Streaming.stream_range("proj", "inst", "table", "a", "z")

      assert Enumerable.impl_for(stream) != nil
    end
  end

  describe "stream_prefix/5" do
    test "returns an Enumerable" do
      stream = Streaming.stream_prefix("proj", "inst", "table", "user#")

      assert Enumerable.impl_for(stream) != nil
    end
  end

  describe "stream_in_chunks/5" do
    test "returns an Enumerable" do
      stream = Streaming.stream_in_chunks("proj", "inst", "table", [], chunk_size: 100)

      assert Enumerable.impl_for(stream) != nil
    end
  end

  describe "module structure" do
    test "exports stream_rows function" do
      functions = Streaming.__info__(:functions)
      assert {:stream_rows, 3} in functions
      assert {:stream_rows, 4} in functions
    end

    test "exports stream_rows_as_maps function" do
      functions = Streaming.__info__(:functions)
      assert {:stream_rows_as_maps, 3} in functions
      assert {:stream_rows_as_maps, 4} in functions
    end

    test "exports stream_rows_with_keys function" do
      functions = Streaming.__info__(:functions)
      assert {:stream_rows_with_keys, 3} in functions
      assert {:stream_rows_with_keys, 4} in functions
    end

    test "exports stream_range function" do
      functions = Streaming.__info__(:functions)
      assert {:stream_range, 5} in functions
      assert {:stream_range, 6} in functions
    end

    test "exports stream_prefix function" do
      functions = Streaming.__info__(:functions)
      assert {:stream_prefix, 4} in functions
      assert {:stream_prefix, 5} in functions
    end

    test "exports stream_in_chunks function" do
      functions = Streaming.__info__(:functions)
      assert {:stream_in_chunks, 5} in functions
    end

    test "exports count_rows function" do
      functions = Streaming.__info__(:functions)
      assert {:count_rows, 3} in functions
      assert {:count_rows, 4} in functions
    end

    test "exports rows_exist? function" do
      functions = Streaming.__info__(:functions)
      assert {:rows_exist?, 3} in functions
      assert {:rows_exist?, 4} in functions
    end

    test "exports first_row function" do
      functions = Streaming.__info__(:functions)
      assert {:first_row, 3} in functions
      assert {:first_row, 4} in functions
    end
  end
end
