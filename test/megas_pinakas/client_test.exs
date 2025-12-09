defmodule MegasPinakas.ClientTest do
  use ExUnit.Case, async: true

  alias MegasPinakas.Client

  describe "default_pool/0" do
    test "returns the default pool name" do
      assert Client.default_pool() == MegasPinakas.ConnectionPool
    end
  end

  describe "execute/2" do
    test "raises ArgumentError when pool registry doesn't exist" do
      operation = fn _channel -> :ok end

      # GrpcConnectionPool raises ArgumentError when registry doesn't exist
      assert_raise ArgumentError, fn ->
        Client.execute(operation, pool: :nonexistent_pool)
      end
    end
  end

  describe "execute!/2" do
    test "raises when pool is not started" do
      operation = fn _channel -> :ok end

      # Can raise either ArgumentError (registry not found) or RuntimeError (our wrapper)
      assert_raise ArgumentError, fn ->
        Client.execute!(operation, pool: :nonexistent_pool)
      end
    end
  end

  describe "with_connection/2" do
    test "behaves same as execute/2" do
      operation = fn _channel -> :ok end

      # Both should raise the same error
      assert_raise ArgumentError, fn ->
        Client.with_connection(operation, pool: :nonexistent_pool)
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
      assert {:with_connection, 1} in functions
      assert {:with_connection, 2} in functions
      assert {:default_pool, 0} in functions
    end
  end
end
