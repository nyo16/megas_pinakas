defmodule MegasPinakas.Test.Emulator do
  @moduledoc """
  Helpers for tests tagged `:emulator`, which require a running BigTable
  emulator on the host/port from `config/test.exs`.

  Start one with:

      docker compose up -d bigtable-emulator

  Then run the tagged tests with `mix test --include emulator`.
  """

  alias MegasPinakas.Admin

  @project "test-project"
  @instance "test-instance"

  @doc "Project id used by emulator-backed tests."
  def project, do: @project

  @doc "Instance id used by emulator-backed tests."
  def instance, do: @instance

  @doc """
  Creates a table with the given column families, deleting any prior table of
  the same name first so each test starts from a clean slate.

  Returns the table id.
  """
  def setup_table(table, families \\ ["cf"]) do
    await_pool!()
    _ = Admin.delete_table(@project, @instance, table)

    column_families =
      Map.new(families, fn family ->
        {family, %{gc_rule: Admin.max_versions_gc_rule(1)}}
      end)

    {:ok, _} = Admin.create_table(@project, @instance, table, column_families: column_families)

    table
  end

  @doc """
  Writes `count` rows keyed `prefix <> zero-padded index`, each with a single
  cell in `family`/`qualifier` holding the index as a string.

  Rows are written in batches so large fixtures do not build one enormous
  `MutateRows` request.
  """
  def seed_rows(table, count, opts \\ []) do
    prefix = Keyword.get(opts, :prefix, "row#")
    family = Keyword.get(opts, :family, "cf")
    qualifier = Keyword.get(opts, :qualifier, "n")
    pad = Keyword.get(opts, :pad, 6)

    0..(count - 1)
    |> Enum.map(fn i ->
      %{
        row_key: row_key(prefix, i, pad),
        mutations: [MegasPinakas.set_cell(family, qualifier, Integer.to_string(i))]
      }
    end)
    |> Enum.chunk_every(1_000)
    |> Enum.each(fn batch ->
      {:ok, _} = MegasPinakas.mutate_rows(@project, @instance, table, batch)
    end)

    :ok
  end

  @doc "Builds the row key `seed_rows/3` would generate for index `i`."
  def row_key(prefix, i, pad \\ 6) do
    prefix <> String.pad_leading(Integer.to_string(i), pad, "0")
  end

  @doc """
  Blocks until the connection pool reports `:healthy`.

  The pool connects asynchronously after the application starts, so the first
  RPC in a run would otherwise race it and fail with `{:pool_error,
  :not_connected}`.
  """
  def await_pool!(timeout \\ 10_000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_await_pool(deadline)
  end

  defp do_await_pool(deadline) do
    case MegasPinakas.Client.status() do
      %{status: :healthy} ->
        :ok

      status ->
        if System.monotonic_time(:millisecond) >= deadline do
          raise "BigTable connection pool never became healthy: #{inspect(status)}"
        end

        Process.sleep(100)
        do_await_pool(deadline)
    end
  end

  @doc """
  Returns true when the configured emulator is accepting connections.

  Used to fail tagged tests with a clear message instead of an opaque gRPC
  error when the emulator was never started.
  """
  def running? do
    case MegasPinakas.Config.emulator_endpoint() do
      nil ->
        false

      {host, port} ->
        case :gen_tcp.connect(String.to_charlist(host), port, [:binary, active: false], 500) do
          {:ok, socket} ->
            :gen_tcp.close(socket)
            true

          {:error, _} ->
            false
        end
    end
  end
end
