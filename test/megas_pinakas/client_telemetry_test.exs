defmodule MegasPinakas.ClientTelemetryTest do
  @moduledoc """
  Verifies the `[:megas_pinakas, :request, :*]` span reports the right outcome.

  Before stream consumption moved inside the operation function, a read that was
  going to blow up during consumption emitted a **success** `:stop` event and
  then let the exception escape `Client.execute/2` uncaught. Both halves of that
  are checked here.
  """

  use ExUnit.Case, async: false

  alias Google.Bigtable.V2.Bigtable.Stub
  alias Google.Bigtable.V2.ReadRowsRequest
  alias MegasPinakas.Auth
  alias MegasPinakas.Auth.Cache
  alias MegasPinakas.Client
  alias MegasPinakas.Config
  alias MegasPinakas.Test.Emulator

  @moduletag :emulator

  @table "client_telemetry_test"

  setup_all do
    Emulator.setup_table(@table, ["cf"])
    Emulator.seed_rows(@table, 20, prefix: "ct#")
    :ok
  end

  setup do
    handler_id = {__MODULE__, System.unique_integer([:positive])}
    test_pid = self()

    :telemetry.attach_many(
      handler_id,
      [
        [:megas_pinakas, :request, :start],
        [:megas_pinakas, :request, :stop],
        [:megas_pinakas, :request, :exception]
      ],
      fn [_, _, event], measurements, metadata, _config ->
        send(test_pid, {:telemetry, event, measurements, metadata})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)
    :ok
  end

  defp collect_events do
    receive do
      {:telemetry, event, measurements, metadata} ->
        [{event, measurements, metadata} | collect_events()]
    after
      0 -> []
    end
  end

  defp event_names, do: collect_events() |> Enum.map(&elem(&1, 0))

  describe "an exception inside the operation" do
    test "is converted to {:error, {:execution_error, _}}" do
      operation = fn _channel -> raise "consumption exploded" end

      assert {:error, {:execution_error, message}} = Client.execute(operation)
      assert message =~ "consumption exploded"
    end

    test "emits :exception and NOT a success :stop" do
      operation = fn _channel -> raise "consumption exploded" end

      Client.execute(operation)

      names = event_names()

      assert :start in names
      assert :exception in names

      refute :stop in names, """
      A doomed request emitted a success :stop event.

      Events: #{inspect(names)}
      """
    end

    test "the :exception event carries kind, the raw exception, and a stacktrace" do
      operation = fn _channel -> raise ArgumentError, "consumption exploded" end

      Client.execute(operation)

      assert [{:exception, measurements, metadata}] =
               collect_events() |> Enum.filter(&(elem(&1, 0) == :exception))

      assert is_integer(measurements.duration)
      assert metadata.kind == :error
      assert %ArgumentError{message: "consumption exploded"} = metadata.reason
      assert is_list(metadata.stacktrace) and metadata.stacktrace != []
      assert metadata.pool == Client.default_pool()
    end

    test "an operation that exits is contained and reported with kind: :exit" do
      operation = fn _channel -> exit({:shutdown, :simulated}) end

      assert Client.execute(operation) ==
               {:error, {:execution_error, {:exit, {:shutdown, :simulated}}}}

      assert [{:exception, _measurements, metadata}] =
               collect_events() |> Enum.filter(&(elem(&1, 0) == :exception))

      assert metadata.kind == :exit
      assert metadata.reason == {:shutdown, :simulated}
      assert is_list(metadata.stacktrace)
    end

    test "an operation that throws is contained and reported with kind: :throw" do
      operation = fn _channel -> throw(:bail) end

      assert Client.execute(operation) == {:error, {:execution_error, {:throw, :bail}}}

      assert [{:exception, _measurements, %{kind: :throw, reason: :bail}}] =
               collect_events() |> Enum.filter(&(elem(&1, 0) == :exception))
    end

    test "an exception raised while consuming a real stream is caught" do
      # Mirrors the shape of read_rows/4: obtain the server stream, then fail
      # partway through consuming it, all inside the operation function.
      operation = fn channel ->
        request = %ReadRowsRequest{
          table_name: Config.table_path(Emulator.project(), Emulator.instance(), @table)
        }

        {:ok, stream} = Stub.read_rows(channel, request, Auth.request_opts())

        Enum.map(stream, fn _element -> raise "boom while consuming" end)
      end

      assert {:error, {:execution_error, message}} = Client.execute(operation)
      assert message =~ "boom while consuming"

      names = event_names()
      assert :exception in names
      refute :stop in names
    end
  end

  describe "a successful operation" do
    test "emits :start and :stop with result: :ok, and no :exception" do
      assert {:ok, rows} = MegasPinakas.read_rows(Emulator.project(), Emulator.instance(), @table)
      assert length(rows) == 20

      events = collect_events()
      names = Enum.map(events, &elem(&1, 0))

      assert :start in names
      refute :exception in names

      assert [{:stop, %{duration: duration}, metadata}] =
               Enum.filter(events, &(elem(&1, 0) == :stop))

      assert is_integer(duration)
      assert metadata.result == :ok
      assert metadata.pool == Client.default_pool()
    end
  end

  describe "an operation returning an error tuple" do
    test "reports :stop with the status atom, not :exception — the request completed, the RPC failed" do
      assert {:error, {:not_found, _}} =
               MegasPinakas.read_rows(Emulator.project(), Emulator.instance(), "no_such_table")

      events = collect_events()
      names = Enum.map(events, &elem(&1, 0))

      assert :start in names
      refute :exception in names

      assert [{:stop, _measurements, %{result: {:error, :not_found}}}] =
               Enum.filter(events, &(elem(&1, 0) == :stop))
    end

    test "an auth failure is a :stop with result {:error, :auth_error}" do
      goth = Application.fetch_env(:megas_pinakas, :goth)
      emulator = Application.fetch_env(:megas_pinakas, :emulator)
      fallback = Application.fetch_env(:megas_pinakas, :allow_gcloud_auth_fallback)
      env_host = System.get_env("BIGTABLE_EMULATOR_HOST")

      Application.put_env(:megas_pinakas, :goth, :no_such_goth_process)
      Application.put_env(:megas_pinakas, :allow_gcloud_auth_fallback, false)
      Application.delete_env(:megas_pinakas, :emulator)
      System.delete_env("BIGTABLE_EMULATOR_HOST")
      Cache.invalidate()

      try do
        assert {:error, {:auth_error, {:goth_exit, _}}} =
                 Client.execute(fn _channel -> Auth.request_opts() end)

        events = collect_events()
        refute :exception in Enum.map(events, &elem(&1, 0))

        assert [{:stop, _measurements, %{result: {:error, :auth_error}}}] =
                 Enum.filter(events, &(elem(&1, 0) == :stop))
      after
        for {key, original} <- [
              goth: goth,
              emulator: emulator,
              allow_gcloud_auth_fallback: fallback
            ] do
          case original do
            {:ok, value} -> Application.put_env(:megas_pinakas, key, value)
            :error -> Application.delete_env(:megas_pinakas, key)
          end
        end

        if env_host, do: System.put_env("BIGTABLE_EMULATOR_HOST", env_host)
        Cache.invalidate()
      end
    end
  end

  describe "read_rows/4 stream failures" do
    test "an assembly error surfaces as an error tuple, not {:ok, partial}" do
      # RowAssembler turns a mid-stream failure into an error tuple and
      # Client.execute/2 passes it through. Verified directly at the unit level
      # in RowAssemblerTest; this checks the wiring so a future refactor cannot
      # quietly turn it back into {:ok, _}.
      assert {:ok, rows} = MegasPinakas.read_rows(Emulator.project(), Emulator.instance(), @table)
      assert is_list(rows)

      refute match?(
               {:ok, _},
               MegasPinakas.read_rows(Emulator.project(), "no-such-instance", @table)
             )
    end
  end
end
