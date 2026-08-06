defmodule MegasPinakas.ClientTest do
  use ExUnit.Case, async: true

  alias MegasPinakas.Client

  describe "default_pool/0" do
    test "returns the default pool name" do
      assert Client.default_pool() == MegasPinakas.ConnectionPool
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
  end

  describe "execute!/2" do
    test "raises when pool is not started" do
      operation = fn _channel -> :ok end

      assert_raise RuntimeError,
                   "BigTable operation failed: {:pool_error, :not_connected}",
                   fn ->
                     Client.execute!(operation, pool: :nonexistent_pool)
                   end
    end
  end

  describe "module exports" do
    test "exports all expected functions" do
      functions = Client.__info__(:functions)

      assert {:execute, 1} in functions
      assert {:execute, 2} in functions
      assert {:execute!, 1} in functions
      assert {:execute!, 2} in functions
      assert {:status, 0} in functions
      assert {:status, 1} in functions
      assert {:default_pool, 0} in functions
    end
  end
end
