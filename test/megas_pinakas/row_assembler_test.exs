defmodule MegasPinakas.RowAssemblerTest do
  @moduledoc """
  Unit tests for the ReadRows chunk-to-row state machine.

  These build synthetic `CellChunk` sequences because the BigTable emulator does
  not produce the interesting ones: it sets `row_key` on every chunk and never
  splits a value across chunks, so the continuation paths that real BigTable
  exercises are unreachable through an integration test.
  """

  use ExUnit.Case, async: true

  alias Google.Bigtable.V2.ReadRowsResponse
  alias Google.Bigtable.V2.ReadRowsResponse.CellChunk
  alias MegasPinakas.RowAssembler

  defp chunk(fields) do
    fields =
      fields
      |> Keyword.update(:family_name, nil, &wrap_string/1)
      |> Keyword.update(:qualifier, nil, &wrap_bytes/1)

    struct!(
      CellChunk,
      Keyword.merge([row_key: "", value: "", value_size: 0, labels: []], fields)
    )
  end

  defp wrap_string(nil), do: nil
  defp wrap_string(value), do: %Google.Protobuf.StringValue{value: value}

  defp wrap_bytes(nil), do: nil
  defp wrap_bytes(value), do: %Google.Protobuf.BytesValue{value: value}

  defp response(chunks), do: {:ok, %ReadRowsResponse{chunks: chunks}}

  # A complete single-cell row, the shape the emulator always sends.
  defp simple_row(key, family, qualifier, value) do
    response([
      chunk(
        row_key: key,
        family_name: family,
        qualifier: qualifier,
        value: value,
        timestamp_micros: 1_000,
        row_status: {:commit_row, true}
      )
    ])
  end

  describe "reduce_all/1 — basic assembly" do
    test "assembles a single-cell row" do
      assert {:ok, [row]} = RowAssembler.reduce_all([simple_row("r1", "cf", "col", "v1")])

      assert row.key == "r1"
      assert [%{name: "cf", columns: [%{qualifier: "col", cells: [cell]}]}] = row.families
      assert cell.value == "v1"
      assert cell.timestamp_micros == 1_000
    end

    test "assembles multiple rows across multiple responses" do
      stream = [
        simple_row("r1", "cf", "col", "v1"),
        simple_row("r2", "cf", "col", "v2")
      ]

      assert {:ok, rows} = RowAssembler.reduce_all(stream)
      assert Enum.map(rows, & &1.key) == ["r1", "r2"]
    end

    test "assembles multiple rows delivered in one response" do
      stream = [
        response([
          chunk(
            row_key: "r1",
            family_name: "cf",
            qualifier: "col",
            value: "v1",
            row_status: {:commit_row, true}
          ),
          chunk(
            row_key: "r2",
            family_name: "cf",
            qualifier: "col",
            value: "v2",
            row_status: {:commit_row, true}
          )
        ])
      ]

      assert {:ok, rows} = RowAssembler.reduce_all(stream)
      assert Enum.map(rows, & &1.key) == ["r1", "r2"]
    end

    test "an empty stream yields no rows" do
      assert {:ok, []} = RowAssembler.reduce_all([])
    end

    test "a response with no chunks yields no rows" do
      assert {:ok, []} = RowAssembler.reduce_all([response([])])
    end
  end

  describe "reduce_all/1 — row_key appears only on the first chunk" do
    # Real BigTable sets row_key once per row. proto3 renders the rest as "", so
    # `chunk.row_key || current_key` returned "" instead of falling back, giving
    # every multi-chunk row the empty key.
    test "the row key carries across continuation chunks" do
      stream = [
        response([
          chunk(row_key: "r1", family_name: "cf", qualifier: "a", value: "v1"),
          chunk(qualifier: "b", value: "v2"),
          chunk(qualifier: "c", value: "v3", row_status: {:commit_row, true})
        ])
      ]

      assert {:ok, [row]} = RowAssembler.reduce_all(stream)
      assert row.key == "r1"
    end

    test "the commit chunk alone does not reset the key to empty" do
      stream = [
        response([
          chunk(row_key: "r1", family_name: "cf", qualifier: "a", value: "v1"),
          chunk(qualifier: "b", value: "v2", row_status: {:commit_row, true})
        ])
      ]

      assert {:ok, [row]} = RowAssembler.reduce_all(stream)
      assert row.key == "r1"
    end

    # The protocol has no bare "commit marker". A chunk with omitted family and
    # qualifier and an empty value is a real cell — here the second version of an
    # empty-valued column — and dropping it lost data.
    test "a sparse commit chunk is a cell, not a marker" do
      stream = [
        response([
          chunk(
            row_key: "r1",
            family_name: "cf",
            qualifier: "flag",
            value: "",
            timestamp_micros: 2_000
          ),
          chunk(value: "", timestamp_micros: 1_000, row_status: {:commit_row, true})
        ])
      ]

      assert {:ok, [row]} = RowAssembler.reduce_all(stream)
      assert [%{name: "cf", columns: [%{qualifier: "flag", cells: cells}]}] = row.families
      assert Enum.map(cells, & &1.timestamp_micros) == [2_000, 1_000]
      assert Enum.map(cells, & &1.value) == ["", ""]
    end

    test "labels are carried onto the assembled cell" do
      stream = [
        response([
          chunk(
            row_key: "r1",
            family_name: "cf",
            qualifier: "a",
            value: "v1",
            labels: ["hot"],
            row_status: {:commit_row, true}
          )
        ])
      ]

      assert {:ok, [row]} = RowAssembler.reduce_all(stream)
      assert [%{columns: [%{cells: [cell]}]}] = row.families
      assert cell.labels == ["hot"]
    end
  end

  describe "reduce_all/1 — family and qualifier persist until changed" do
    # BigTable only sends family_name/qualifier when they change.
    test "a later cell in the same family inherits the family name" do
      stream = [
        response([
          chunk(row_key: "r1", family_name: "cf", qualifier: "a", value: "v1"),
          chunk(qualifier: "b", value: "v2", row_status: {:commit_row, true})
        ])
      ]

      assert {:ok, [row]} = RowAssembler.reduce_all(stream)
      assert [%{name: "cf", columns: columns}] = row.families

      assert columns |> Enum.map(& &1.qualifier) |> Enum.sort() == ["a", "b"]
    end

    test "multiple versions of one column inherit both family and qualifier" do
      stream = [
        response([
          chunk(
            row_key: "r1",
            family_name: "cf",
            qualifier: "a",
            value: "newest",
            timestamp_micros: 3_000
          ),
          chunk(value: "middle", timestamp_micros: 2_000),
          chunk(value: "oldest", timestamp_micros: 1_000, row_status: {:commit_row, true})
        ])
      ]

      assert {:ok, [row]} = RowAssembler.reduce_all(stream)
      assert [%{name: "cf", columns: [%{qualifier: "a", cells: cells}]}] = row.families

      # Server order is newest-first, and that order must survive assembly
      # because MegasPinakas.get_cell/3 takes the head as the current value.
      assert Enum.map(cells, & &1.value) == ["newest", "middle", "oldest"]
      assert Enum.map(cells, & &1.timestamp_micros) == [3_000, 2_000, 1_000]
    end

    test "a family change is picked up" do
      stream = [
        response([
          chunk(row_key: "r1", family_name: "cf1", qualifier: "a", value: "v1"),
          chunk(family_name: "cf2", qualifier: "b", value: "v2", row_status: {:commit_row, true})
        ])
      ]

      assert {:ok, [row]} = RowAssembler.reduce_all(stream)

      assert row.families |> Enum.map(& &1.name) |> Enum.sort() == ["cf1", "cf2"]
    end
  end

  describe "reduce_all/1 — values split across chunks" do
    # value_size > 0 marks every fragment but the last. Fragments must be
    # concatenated into one cell, not recorded as separate cells.
    test "fragments concatenate into a single cell" do
      stream = [
        response([
          chunk(
            row_key: "r1",
            family_name: "cf",
            qualifier: "blob",
            value: "abc",
            value_size: 9,
            timestamp_micros: 1_000
          ),
          chunk(value: "def", value_size: 9),
          chunk(value: "ghi", row_status: {:commit_row, true})
        ])
      ]

      assert {:ok, [row]} = RowAssembler.reduce_all(stream)
      assert [%{columns: [%{qualifier: "blob", cells: [cell]}]}] = row.families
      assert cell.value == "abcdefghi"
      assert cell.timestamp_micros == 1_000
    end

    test "fragments spanning response boundaries still concatenate" do
      stream = [
        response([
          chunk(
            row_key: "r1",
            family_name: "cf",
            qualifier: "blob",
            value: "abc",
            value_size: 6
          )
        ]),
        response([chunk(value: "def", row_status: {:commit_row, true})])
      ]

      assert {:ok, [row]} = RowAssembler.reduce_all(stream)
      assert [%{columns: [%{cells: [cell]}]}] = row.families
      assert cell.value == "abcdef"
    end

    test "a split value followed by a normal cell keeps them separate" do
      stream = [
        response([
          chunk(row_key: "r1", family_name: "cf", qualifier: "blob", value: "ab", value_size: 4),
          chunk(value: "cd"),
          chunk(qualifier: "other", value: "z", row_status: {:commit_row, true})
        ])
      ]

      assert {:ok, [row]} = RowAssembler.reduce_all(stream)
      assert [%{columns: columns}] = row.families

      by_qualifier = Map.new(columns, &{&1.qualifier, Enum.map(&1.cells, fn c -> c.value end)})
      assert by_qualifier == %{"blob" => ["abcd"], "other" => ["z"]}
    end
  end

  describe "reduce_all/1 — reset_row" do
    test "discards the partially assembled row" do
      stream = [
        response([
          chunk(row_key: "r1", family_name: "cf", qualifier: "a", value: "discarded"),
          chunk(row_status: {:reset_row, true}),
          chunk(
            row_key: "r1",
            family_name: "cf",
            qualifier: "a",
            value: "kept",
            row_status: {:commit_row, true}
          )
        ])
      ]

      assert {:ok, [row]} = RowAssembler.reduce_all(stream)
      assert row.key == "r1"
      assert [%{columns: [%{cells: [cell]}]}] = row.families
      assert cell.value == "kept"
    end

    test "discards an in-flight split value" do
      stream = [
        response([
          chunk(row_key: "r1", family_name: "cf", qualifier: "a", value: "par", value_size: 6),
          chunk(row_status: {:reset_row, true}),
          chunk(
            row_key: "r1",
            family_name: "cf",
            qualifier: "a",
            value: "whole",
            row_status: {:commit_row, true}
          )
        ])
      ]

      assert {:ok, [row]} = RowAssembler.reduce_all(stream)
      assert [%{columns: [%{cells: [cell]}]}] = row.families
      assert cell.value == "whole"
    end

    test "already-committed rows survive a later reset" do
      stream = [
        simple_row("r1", "cf", "col", "v1"),
        response([
          chunk(row_key: "r2", family_name: "cf", qualifier: "a", value: "discarded"),
          chunk(row_status: {:reset_row, true})
        ])
      ]

      assert {:ok, [row]} = RowAssembler.reduce_all(stream)
      assert row.key == "r1"
    end
  end

  describe "reduce_all/1 — error handling (no silent truncation)" do
    # This is the finding: logging the error and returning the accumulator
    # unchanged produced {:ok, partial_rows}, which the caller cannot tell apart
    # from a complete result.
    test "a mid-stream error returns an error, never {:ok, partial}" do
      stream = [
        simple_row("r1", "cf", "col", "v1"),
        simple_row("r2", "cf", "col", "v2"),
        {:error, :connection_lost},
        simple_row("r3", "cf", "col", "v3")
      ]

      assert {:error, {:incomplete_read, :connection_lost}} = RowAssembler.reduce_all(stream)
    end

    test "an error on the very first element returns an error" do
      assert {:error, {:incomplete_read, :boom}} = RowAssembler.reduce_all([{:error, :boom}])
    end

    test "an error after rows were assembled still fails" do
      stream = for i <- 1..100, do: simple_row("r#{i}", "cf", "col", "v")
      stream = stream ++ [{:error, :deadline_exceeded}]

      refute match?({:ok, _}, RowAssembler.reduce_all(stream))
      assert {:error, {:incomplete_read, :deadline_exceeded}} = RowAssembler.reduce_all(stream)
    end

    test "a gRPC error is normalized to {status_atom, message}" do
      stream = [
        simple_row("r1", "cf", "col", "v1"),
        {:error, %GRPC.RPCError{status: 14, message: "connection reset"}}
      ]

      assert {:error, {:incomplete_read, {:unavailable, "connection reset"}}} =
               RowAssembler.reduce_all(stream)
    end

    test "an unrecognised element fails loudly" do
      stream = [simple_row("r1", "cf", "col", "v1"), :something_unexpected]

      assert {:error, {:unexpected_read_rows_chunk, :something_unexpected}} =
               RowAssembler.reduce_all(stream)
    end

    test "trailers are skipped, not treated as a failure" do
      stream = [simple_row("r1", "cf", "col", "v1"), {:trailers, %{"grpc-status" => "0"}}]

      assert {:ok, [row]} = RowAssembler.reduce_all(stream)
      assert row.key == "r1"
    end

    test "stops consuming the stream at the error" do
      # Proves the fold halts rather than draining the rest of the stream.
      consumed = :counters.new(1, [])

      stream =
        Stream.map(
          [
            simple_row("r1", "cf", "col", "v1"),
            {:error, :boom},
            simple_row("r2", "cf", "c", "v")
          ],
          fn element ->
            :counters.add(consumed, 1, 1)
            element
          end
        )

      assert {:error, {:incomplete_read, :boom}} = RowAssembler.reduce_all(stream)
      assert :counters.get(consumed, 1) == 2
    end
  end
end
