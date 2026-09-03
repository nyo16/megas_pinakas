defmodule MegasPinakas.Error do
  @moduledoc """
  Raised by `MegasPinakas.Client.execute!/2` when the underlying operation
  returns `{:error, reason}`.

  `reason` is exactly what `MegasPinakas.Client.execute/2` would have returned
  inside the error tuple, e.g. `{:not_found, "..."}` or `{:pool_error, :not_connected}`.
  """

  defexception [:reason]

  @impl true
  def message(%__MODULE__{reason: reason}) do
    "BigTable operation failed: #{inspect(reason)}"
  end
end

defmodule MegasPinakas.Client do
  @moduledoc """
  Low-level client for executing gRPC operations against BigTable.

  This module wraps the GrpcConnectionPool to provide a simple interface
  for executing operations with automatic connection management.

  All gRPC responses are normalized through `MegasPinakas.Response.format/1`,
  converting raw gRPC errors into idiomatic `{:error, {status_atom, message}}` tuples.

  ## Error shapes

  `execute/2` never raises for a failure inside the operation. It returns:

    * `{:error, {status_atom, message}}` — the RPC ran and the server rejected it
    * `{:error, {:auth_error, reason}}` — no access token could be obtained
      (`MegasPinakas.AuthError` raised by `MegasPinakas.Auth.request_opts/0`)
    * `{:error, {:pool_error, reason}}` — no connection was available
    * `{:error, {:execution_error, message}}` — the operation raised; `message`
      is `Exception.message/1` of the exception
    * `{:error, {:execution_error, {:exit | :throw, term}}}` — the operation
      exited or threw

  ## Telemetry

  Every call emits `[:megas_pinakas, :request, :start]` and then exactly one of:

    * `[:megas_pinakas, :request, :stop]` — measurements `%{duration: native}`,
      metadata `%{pool: atom, result: :ok | {:error, tag}}` where `tag` is the
      gRPC status atom, `:auth_error`, `:pool_error`, or the first element of a
      client-side error tuple. Emitted whenever the request *completed*, even if
      the server answered with an error.
    * `[:megas_pinakas, :request, :exception]` — measurements `%{duration: native}`,
      metadata `%{pool: atom, kind: :error | :exit | :throw, reason: term,
      stacktrace: list}`. `reason` is the raw exception struct (or exit/throw
      value), not a string. Emitted when the operation itself blew up.
  """

  alias MegasPinakas.Response

  @default_pool MegasPinakas.ConnectionPool
  @admin_pool MegasPinakas.AdminConnectionPool

  @typedoc "A checked-out channel; the argument every operation function receives."
  @type channel :: GrpcConnectionPool.Pool.channel()

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
  @spec execute((channel() -> any()), keyword()) :: {:ok, any()} | {:error, term()}
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
      {:ok, channel} -> run(operation_fn, channel, start_time, metadata)
      {:error, reason} -> stop(start_time, metadata, {:error, {:pool_error, reason}})
    end
  end

  defp run(operation_fn, channel, start_time, metadata) do
    result = operation_fn.(channel) |> Response.format()
    stop(start_time, metadata, result)
  rescue
    # Auth failure is a completed request with a known outcome, not a crash:
    # the operation deliberately aborted before sending anything.
    e in MegasPinakas.AuthError ->
      stop(start_time, metadata, {:error, {:auth_error, e.reason}})

    e ->
      exception(start_time, metadata, :error, e, __STACKTRACE__)
  catch
    kind, reason ->
      exception(start_time, metadata, kind, reason, __STACKTRACE__)
  end

  defp stop(start_time, metadata, result) do
    :telemetry.execute(
      [:megas_pinakas, :request, :stop],
      %{duration: System.monotonic_time() - start_time},
      Map.put(metadata, :result, telemetry_result(result))
    )

    result
  end

  defp exception(start_time, metadata, kind, reason, stacktrace) do
    :telemetry.execute(
      [:megas_pinakas, :request, :exception],
      %{duration: System.monotonic_time() - start_time},
      Map.merge(metadata, %{kind: kind, reason: reason, stacktrace: stacktrace})
    )

    {:error, {:execution_error, execution_reason(kind, reason)}}
  end

  # `rescue` always sees a normalized exception struct, so `:error` carries a
  # message; exits and throws carry arbitrary terms and are tagged instead.
  defp execution_reason(:error, exception), do: Exception.message(exception)
  defp execution_reason(kind, reason), do: {kind, reason}

  # Collapses a result to the low-cardinality tag telemetry consumers can group
  # by: the gRPC status atom, or the client-side error tag.
  defp telemetry_result({:ok, _}), do: :ok
  defp telemetry_result({:error, reason}) when is_atom(reason), do: {:error, reason}

  defp telemetry_result({:error, reason})
       when is_tuple(reason) and tuple_size(reason) > 0 and is_atom(elem(reason, 0)),
       do: {:error, elem(reason, 0)}

  defp telemetry_result({:error, _reason}), do: {:error, :unknown}

  @doc """
  Execute a gRPC operation and unwrap the result.

  Similar to `execute/2` but returns just the result on success,
  or raises `MegasPinakas.Error` on error.

  ## Examples

      operation = fn channel ->
        request = %Google.Bigtable.V2.ReadRowsRequest{table_name: "..."}
        Google.Bigtable.V2.Bigtable.Stub.read_rows(channel, request, [])
      end

      MegasPinakas.Client.execute!(operation)
  """
  @spec execute!((channel() -> any()), keyword()) :: any()
  def execute!(operation_fn, opts \\ []) do
    case execute(operation_fn, opts) do
      {:ok, result} -> result
      {:error, reason} -> raise MegasPinakas.Error, reason: reason
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
  Returns the default (Data API) pool name.
  """
  @spec default_pool() :: atom()
  def default_pool, do: @default_pool

  @doc """
  Returns the Admin API pool name.

  Table and instance admin RPCs are served from `bigtableadmin.googleapis.com`,
  so `MegasPinakas.Admin` and `MegasPinakas.InstanceAdmin` pass
  `pool: admin_pool()` to `execute/2`. In emulator mode both pools point at the
  emulator.
  """
  @spec admin_pool() :: atom()
  def admin_pool, do: @admin_pool
end
