defmodule MegasPinakas.Client do
  @moduledoc """
  Low-level client for executing gRPC operations against BigTable.

  This module wraps the GrpcConnectionPool to provide a simple interface
  for executing operations with automatic connection management.
  """

  @default_pool MegasPinakas.ConnectionPool

  @doc """
  Execute a gRPC operation using a connection from the pool.

  The operation function receives a channel and should return the result
  of calling a gRPC stub method.

  ## Options

    * `:pool` - The pool name to use (default: `MegasPinakas.ConnectionPool`)

  ## Examples

      operation = fn channel ->
        request = %Google.Bigtable.V2.ReadRowsRequest{table_name: "..."}
        Google.Bigtable.V2.Bigtable.Stub.read_rows(channel, request, [])
      end

      MegasPinakas.Client.execute(operation)
  """
  @spec execute((GRPC.Channel.t() -> any()), keyword()) :: {:ok, any()} | {:error, term()}
  def execute(operation_fn, opts \\ []) when is_function(operation_fn, 1) do
    pool_name = Keyword.get(opts, :pool, @default_pool)

    case GrpcConnectionPool.get_channel(pool_name) do
      {:ok, channel} ->
        try do
          result = operation_fn.(channel)
          {:ok, result}
        rescue
          e -> {:error, {:execution_error, e}}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Execute a gRPC operation and unwrap the result.

  Similar to `execute/2` but returns just the result on success,
  or raises on error.

  ## Examples

      operation = fn channel ->
        request = %Google.Bigtable.V2.ReadRowsRequest{table_name: "..."}
        Google.Bigtable.V2.Bigtable.Stub.read_rows(channel, request, [])
      end

      MegasPinakas.Client.execute!(operation)
  """
  @spec execute!((GRPC.Channel.t() -> any()), keyword()) :: any()
  def execute!(operation_fn, opts \\ []) do
    case execute(operation_fn, opts) do
      {:ok, result} -> result
      {:error, reason} -> raise "BigTable operation failed: #{inspect(reason)}"
    end
  end

  @doc """
  Returns the status of the connection pool.

  ## Options

    * `:pool` - The pool name to check (default: `MegasPinakas.ConnectionPool`)

  ## Examples

      MegasPinakas.Client.status()
      # => %{status: :healthy, current_size: 5, expected_size: 5}
  """
  @spec status(keyword()) :: map()
  def status(opts \\ []) do
    pool_name = Keyword.get(opts, :pool, @default_pool)
    GrpcConnectionPool.status(pool_name)
  end

  @doc """
  Returns the default pool name.
  """
  @spec default_pool() :: atom()
  def default_pool, do: @default_pool

  @doc """
  Alias for `execute/2` for backward compatibility.
  """
  @spec with_connection((GRPC.Channel.t() -> any()), keyword()) :: {:ok, any()} | {:error, term()}
  def with_connection(fun, opts \\ []) when is_function(fun, 1) do
    execute(fun, opts)
  end
end
