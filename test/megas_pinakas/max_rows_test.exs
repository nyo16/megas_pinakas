defmodule MegasPinakas.MaxRowsTest do
  @moduledoc """
  Covers the `:max_rows` safety cap on `MegasPinakas.read_rows/4`.

  `read_rows/4` accumulates every matching row, so a call with no `:rows` and no
  `:rows_limit` materializes an entire table. `:max_rows` converts that into an
  error instead of an out-of-memory crash. It defaults to `:infinity` so existing
  callers doing legitimate large eager scans are unaffected.
  """

  use ExUnit.Case, async: false

  alias MegasPinakas.Test.Emulator

  @moduletag :emulator

  @table "max_rows_test"
  @row_count 50

  setup_all do
    Emulator.setup_table(@table, ["cf"])
    Emulator.seed_rows(@table, @row_count, prefix: "mx#")
    :ok
  end

  defp read(opts),
    do: MegasPinakas.read_rows(Emulator.project(), Emulator.instance(), @table, opts)

  describe "default behaviour" do
    test "no :max_rows means no cap" do
      assert {:ok, rows} = read([])
      assert length(rows) == @row_count
    end

    test "an explicit :infinity means no cap" do
      assert {:ok, rows} = read(max_rows: :infinity)
      assert length(rows) == @row_count
    end
  end

  describe "under the cap" do
    test "a result smaller than the cap is returned normally" do
      assert {:ok, rows} = read(max_rows: @row_count + 10)
      assert length(rows) == @row_count
    end

    test "a result exactly at the cap is allowed" do
      assert {:ok, rows} = read(max_rows: @row_count)
      assert length(rows) == @row_count
    end
  end

  describe "over the cap" do
    test "returns {:error, :result_too_large}" do
      assert {:error, :result_too_large} = read(max_rows: @row_count - 1)
    end

    test "a cap of 1 against many rows errors" do
      assert {:error, :result_too_large} = read(max_rows: 1)
    end

    test "no rows are returned alongside the error" do
      # The caller gets an error, not a truncated list that looks like a result.
      assert {:error, :result_too_large} = read(max_rows: 5)
    end
  end

  describe "interaction with :rows_limit" do
    test "a rows_limit below the cap wins and no error is raised" do
      assert {:ok, rows} = read(rows_limit: 10, max_rows: 20)
      assert length(rows) == 10
    end

    test "a rows_limit equal to the cap is allowed" do
      assert {:ok, rows} = read(rows_limit: 20, max_rows: 20)
      assert length(rows) == 20
    end

    test "a rows_limit above the cap still trips the cap" do
      assert {:error, :result_too_large} = read(rows_limit: 40, max_rows: 10)
    end

    test "the cap bounds server work rather than scanning the table" do
      # max_rows: 3 asks the server for at most 4 rows. If the cap were enforced
      # by reading everything and then checking length, this would assemble all
      # 50 rows first.
      me = self()
      handler = {__MODULE__, System.unique_integer([:positive])}

      :telemetry.attach(
        handler,
        [:megas_pinakas, :request, :stop],
        fn _e, %{duration: d}, _m, _c -> send(me, {:duration, d}) end,
        nil
      )

      on_exit(fn -> :telemetry.detach(handler) end)

      assert {:error, :result_too_large} = read(max_rows: 3)
      assert_received {:duration, capped_duration}

      assert {:ok, _} = read([])
      assert_received {:duration, full_duration}

      assert capped_duration < full_duration, """
      A capped read (max_rows: 3) took #{capped_duration} vs #{full_duration} for a
      full #{@row_count}-row scan, suggesting the cap did not bound server work.
      """
    end
  end

  describe "read_row/5 is unaffected" do
    test "still returns a single row" do
      key = Emulator.row_key("mx#", 0)

      assert {:ok, row} =
               MegasPinakas.read_row(Emulator.project(), Emulator.instance(), @table, key)

      assert MegasPinakas.row_key(row) == key
    end
  end
end
