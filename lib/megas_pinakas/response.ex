defmodule MegasPinakas.Response do
  @moduledoc """
  Normalizes gRPC responses into idiomatic Elixir tuples.

  Converts raw gRPC return values into consistent `{:ok, result} | {:error, reason}`
  tuples, translating `GRPC.RPCError` status codes into meaningful atoms.

  ## Error format

      {:error, {:not_found, "Row not found"}}
      {:error, {:unavailable, "Service temporarily unavailable"}}
      {:error, {:permission_denied, "Caller does not have permission"}}

  ## Examples

      iex> MegasPinakas.Response.format({:ok, %SomeResponse{}})
      {:ok, %SomeResponse{}}

      iex> MegasPinakas.Response.format({:error, %GRPC.RPCError{status: 5, message: "not found"}})
      {:error, {:not_found, "not found"}}
  """

  @status_atoms %{
    0 => :ok,
    1 => :cancelled,
    2 => :unknown,
    3 => :invalid_argument,
    4 => :deadline_exceeded,
    5 => :not_found,
    6 => :already_exists,
    7 => :permission_denied,
    8 => :resource_exhausted,
    9 => :failed_precondition,
    10 => :aborted,
    11 => :out_of_range,
    12 => :unimplemented,
    13 => :internal,
    14 => :unavailable,
    15 => :data_loss,
    16 => :unauthenticated
  }

  @doc """
  Formats a raw gRPC response into a normalized `{:ok, result} | {:error, reason}` tuple.

  Handles all gRPC return shapes:
  - `{:ok, result}` — passed through
  - `{:ok, result, headers}` — headers stripped, result passed through
  - `{:error, %GRPC.RPCError{}}` — converted to `{:error, {status_atom, message}}`
  - `{:error, other}` — passed through
  """
  @spec format(term()) :: {:ok, term()} | {:error, term()}
  def format({:ok, result}), do: {:ok, result}
  def format({:ok, result, _headers}), do: {:ok, result}
  def format({:error, reason}), do: {:error, normalize_reason(reason)}
  def format(other), do: {:error, {:unexpected_response, other}}

  @doc """
  Normalizes a bare error reason.

  `%GRPC.RPCError{}` becomes `{status_atom, message}`; every other term is
  returned unchanged. Use this on error elements that surface *inside* a
  streaming response (where `format/1` never sees them) so callers observe
  the same `{status_atom, message}` shape as unary failures.

  ## Examples

      iex> MegasPinakas.Response.normalize_reason(%GRPC.RPCError{status: 14, message: "down"})
      {:unavailable, "down"}

      iex> MegasPinakas.Response.normalize_reason(:timeout)
      :timeout
  """
  @spec normalize_reason(term()) :: term()
  def normalize_reason(%GRPC.RPCError{status: status, message: message}) do
    {status_to_atom(status), message}
  end

  def normalize_reason(reason), do: reason

  @doc """
  Converts a gRPC status code integer to a descriptive atom.

  ## Examples

      iex> MegasPinakas.Response.status_to_atom(0)
      :ok

      iex> MegasPinakas.Response.status_to_atom(5)
      :not_found

      iex> MegasPinakas.Response.status_to_atom(14)
      :unavailable
  """
  @spec status_to_atom(non_neg_integer()) :: atom()
  def status_to_atom(code) when is_integer(code) do
    Map.get(@status_atoms, code, :unknown)
  end
end
