defmodule MegasPinakas.ClientTest do
  use ExUnit.Case, async: true

  alias MegasPinakas.Client

  describe "pool names" do
    test "default_pool/0 is the Data API pool" do
      assert Client.default_pool() == MegasPinakas.ConnectionPool
    end

    test "admin_pool/0 is the Admin API pool" do
      assert Client.admin_pool() == MegasPinakas.AdminConnectionPool
    end
  end

  describe "execute/2" do
    test "returns a pool error when the pool is not started" do
      operation = fn _channel -> :ok end

      # Since grpc_connection_pool 0.3.5, get_channel/1 returns
      # {:error, :not_connected} rather than raising when the pool is missing.
      assert Client.execute(operation, pool: :nonexistent_pool) ==
               {:error, {:pool_error, :not_connected}}
    end

    test "does not invoke the operation when no channel is available" do
      parent = self()
      operation = fn _channel -> send(parent, :operation_ran) end

      Client.execute(operation, pool: :nonexistent_pool)

      refute_received :operation_ran
    end

    test "a pool error is a completed request: :stop with result {:error, :pool_error}" do
      handler_id = {__MODULE__, System.unique_integer([:positive])}
      test_pid = self()

      :telemetry.attach_many(
        handler_id,
        [[:megas_pinakas, :request, :stop], [:megas_pinakas, :request, :exception]],
        fn [_, _, event], _measurements, metadata, _ -> send(test_pid, {event, metadata}) end,
        nil
      )

      try do
        Client.execute(fn _channel -> :ok end, pool: :nonexistent_pool)

        assert_received {:stop, %{pool: :nonexistent_pool, result: {:error, :pool_error}}}
        refute_received {:exception, _}
      after
        :telemetry.detach(handler_id)
      end
    end
  end

  describe "execute!/2" do
    test "raises MegasPinakas.Error carrying the reason" do
      operation = fn _channel -> :ok end

      error =
        assert_raise MegasPinakas.Error, fn ->
          Client.execute!(operation, pool: :nonexistent_pool)
        end

      assert error.reason == {:pool_error, :not_connected}
      assert Exception.message(error) =~ "{:pool_error, :not_connected}"
    end
  end
end
