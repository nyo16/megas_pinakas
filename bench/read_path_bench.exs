# Benchmarks the read path against a running BigTable emulator.
#
# Run with:
#
#     docker compose up -d bigtable-emulator
#     mix run bench/read_path_bench.exs
#
# This covers two things the perf work targets:
#
#   1. `read_rows/4` — eager collection, so cost should scale with result size.
#   2. `Streaming.*`  — lazily consumed. Before the Phase 3 rewrite each batch
#      was a *fresh* ReadRows RPC, so scanning N rows at `batch_size: B` cost
#      ceil(N/B) round trips, each paying a full `Auth.request_opts/0`. After
#      the rewrite it should be one RPC regardless of `batch_size`, which shows
#      up here as `batch_size` no longer affecting the time.

alias MegasPinakas.Streaming
alias MegasPinakas.Test.Emulator

unless Code.ensure_loaded?(Emulator) do
  IO.puts("""
  MegasPinakas.Test.Emulator is not loaded.
  Run this benchmark with MIX_ENV=test:

      MIX_ENV=test mix run bench/read_path_bench.exs
  """)

  System.halt(1)
end

unless Emulator.running?() do
  IO.puts("""
  No BigTable emulator reachable. Start one first:

      docker compose up -d bigtable-emulator
  """)

  System.halt(1)
end

table = "read_path_bench"
row_count = 20_000

IO.puts("Seeding #{row_count} rows into #{table}...")
Emulator.setup_table(table, ["cf"])
Emulator.seed_rows(table, row_count, prefix: "bench#")
IO.puts("Seeded.\n")

project = Emulator.project()
instance = Emulator.instance()

read_rows = fn limit ->
  fn ->
    {:ok, rows} = MegasPinakas.read_rows(project, instance, table, rows_limit: limit)
    length(rows)
  end
end

stream_all = fn batch_size ->
  fn ->
    Streaming.stream_rows(project, instance, table, batch_size: batch_size)
    |> Enum.reduce(0, fn _row, acc -> acc + 1 end)
  end
end

Benchee.run(
  %{
    "read_rows 100" => read_rows.(100),
    "read_rows 1_000" => read_rows.(1_000),
    "read_rows 20_000" => read_rows.(20_000),
    # Identical work, different batch_size. A per-batch-RPC implementation makes
    # the small batch dramatically slower; a single-stream implementation does not.
    "stream_rows batch_size: 100" => stream_all.(100),
    "stream_rows batch_size: 1_000" => stream_all.(1_000),
    "stream_rows batch_size: 10_000" => stream_all.(10_000)
  },
  warmup: 2,
  time: 10,
  memory_time: 2,
  print: [fast_warning: false]
)
