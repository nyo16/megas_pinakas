defmodule MegasPinakas.ResponseTest do
  use ExUnit.Case, async: true

  alias MegasPinakas.Response

  describe "format/1" do
    test "passes through {:ok, result}" do
      assert Response.format({:ok, %{data: "hello"}}) == {:ok, %{data: "hello"}}
    end

    test "strips headers from {:ok, result, headers}" do
      headers = %{headers: [{"x-request-id", "abc"}]}
      assert Response.format({:ok, %{data: "hello"}, headers}) == {:ok, %{data: "hello"}}
    end

    test "converts GRPC.RPCError to {status_atom, message}" do
      error = %GRPC.RPCError{status: 5, message: "Row not found"}
      assert Response.format({:error, error}) == {:error, {:not_found, "Row not found"}}
    end

    test "converts all standard GRPC status codes" do
      cases = [
        {0, :ok},
        {1, :cancelled},
        {2, :unknown},
        {3, :invalid_argument},
        {4, :deadline_exceeded},
        {5, :not_found},
        {6, :already_exists},
        {7, :permission_denied},
        {8, :resource_exhausted},
        {9, :failed_precondition},
        {10, :aborted},
        {11, :out_of_range},
        {12, :unimplemented},
        {13, :internal},
        {14, :unavailable},
        {15, :data_loss},
        {16, :unauthenticated}
      ]

      for {code, expected_atom} <- cases do
        error = %GRPC.RPCError{status: code, message: "msg"}
        assert {:error, {^expected_atom, "msg"}} = Response.format({:error, error})
      end
    end

    test "maps unknown GRPC status code to :unknown" do
      error = %GRPC.RPCError{status: 99, message: "wat"}
      assert Response.format({:error, error}) == {:error, {:unknown, "wat"}}
    end

    test "passes through non-GRPC errors" do
      assert Response.format({:error, :timeout}) == {:error, :timeout}
      assert Response.format({:error, "something broke"}) == {:error, "something broke"}
    end

    test "wraps unexpected responses" do
      assert Response.format(:unexpected) == {:error, {:unexpected_response, :unexpected}}
      assert Response.format(42) == {:error, {:unexpected_response, 42}}
    end
  end

  describe "status_to_atom/1" do
    test "maps known codes" do
      assert Response.status_to_atom(0) == :ok
      assert Response.status_to_atom(5) == :not_found
      assert Response.status_to_atom(14) == :unavailable
      assert Response.status_to_atom(16) == :unauthenticated
    end

    test "maps unknown codes to :unknown" do
      assert Response.status_to_atom(42) == :unknown
      assert Response.status_to_atom(-1) == :unknown
    end
  end
end
