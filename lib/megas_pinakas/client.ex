defmodule MegasPinakas.Client do
  @moduledoc """
  Low-level client for executing gRPC operations against BigTable.

  This module wraps the GrpcConnectionPool to provide a simple interface
  for executing operations with automatic connection management.

  All gRPC responses are normalized through `MegasPinakas.Response.format/1`,
  converting raw gRPC errors into idiomatic `{:error, {status_atom, message}}` tuples.
  """

  alias MegasPinakas.Response

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
    start_time = System.monotonic_time()
    metadata = %{pool: pool_name}

    :telemetry.execute(
      [:megas_pinakas, :request, :start],
      %{system_time: System.system_time()},
      metadata
    )

    case GrpcConnectionPool.get_channel(pool_name) do
      {:ok, channel} ->
        try do
          result = operation_fn.(channel) |> Response.format()
          duration = System.monotonic_time() - start_time
          :telemetry.execute([:megas_pinakas, :request, :stop], %{duration: duration}, metadata)
          result
        rescue
          e ->
            duration = System.monotonic_time() - start_time

            :telemetry.execute(
              [:megas_pinakas, :request, :exception],
              %{duration: duration},
              Map.put(metadata, :reason, Exception.message(e))
            )

            {:error, {:execution_error, Exception.message(e)}}
        end

      {:error, reason} ->
        duration = System.monotonic_time() - start_time

        :telemetry.execute(
          [:megas_pinakas, :request, :exception],
          %{duration: duration},
          Map.put(metadata, :reason, reason)
        )

        {:error, {:pool_error, reason}}
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
end
