defmodule MegasPinakas.Auth.Cache do
  @moduledoc """
  Caches the OAuth access token used to authenticate BigTable RPCs.

  Every RPC calls `MegasPinakas.Auth.request_opts/0`, so an uncached token fetch
  is paid once per request. Measured before this cache existed, with the gcloud
  CLI fallback in play, that was **911.9 ms per call** — the token fetch, not
  the RPC, dominated every operation.

  ## Read path

  Reads are lock-free. `fetch_token/1` does a plain `:ets.lookup/2` against a
  `read_concurrency: true` table and returns immediately when the cached token
  has more than #{60} seconds of life left. No message passes through the
  GenServer on the hot path.

  ## Refresh path

  On a miss or near-expiry, the caller issues a `GenServer.call/3`. Because the
  GenServer serializes, and because `handle_call/3` re-checks the table before
  fetching, N concurrent callers arriving at expiry trigger **one** token fetch
  rather than N. Without that re-check, token expiry under load would produce a
  thundering herd of ~900 ms `gcloud` subprocess spawns.

  ## Expiry

  Goth reports an absolute expiry, which is used directly. The gcloud CLI
  fallback prints only the token with no expiry, so a conservative 55-minute TTL
  is assumed (Google access tokens live one hour).
  """

  use GenServer

  require Logger

  alias MegasPinakas.Auth

  @table __MODULE__

  # Refresh once the token has less than this much life left, so callers are
  # never handed a token that expires mid-flight.
  @refresh_margin_seconds 60

  # A refresh may shell out to gcloud, which was measured at 0.85-1.11 s.
  @refresh_timeout 30_000

  @doc false
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Returns a cached access token, refreshing it if absent or near expiry.

  Returns `{:error, reason}` when no token can be obtained.
  """
  @spec fetch_token(GenServer.server()) :: {:ok, String.t()} | {:error, term()}
  def fetch_token(server \\ __MODULE__) do
    case lookup_valid() do
      {:ok, token} -> {:ok, token}
      :stale -> request_refresh(server)
    end
  end

  @doc """
  Drops any cached token so the next `fetch_token/1` refetches.

  Intended for tests and for recovering from a token revoked server-side.
  """
  @spec invalidate(GenServer.server()) :: :ok
  def invalidate(server \\ __MODULE__) do
    case :ets.whereis(@table) do
      :undefined -> :ok
      _tid -> GenServer.call(server, :invalidate)
    end
  end

  @doc """
  Returns the cached entry as `{:ok, token, expires_at}`, or `:error`.

  Exposed for tests and diagnostics; does not trigger a refresh.
  """
  @spec peek() :: {:ok, String.t(), non_neg_integer()} | :error
  def peek do
    case :ets.whereis(@table) do
      :undefined ->
        :error

      tid ->
        case :ets.lookup(tid, :token) do
          [{:token, token, expires_at}] -> {:ok, token, expires_at}
          [] -> :error
        end
    end
  end

  # ==========================================================================
  # Read path — no GenServer involvement
  # ==========================================================================

  defp lookup_valid do
    case peek() do
      {:ok, token, expires_at} ->
        if expires_at - now() > @refresh_margin_seconds, do: {:ok, token}, else: :stale

      :error ->
        :stale
    end
  end

  defp request_refresh(server) do
    GenServer.call(server, :refresh, @refresh_timeout)
  catch
    # The cache is part of the application's supervision tree, so this only
    # happens when the library is used without its application started (or
    # mid-shutdown). Degrade to an uncached fetch rather than failing the RPC.
    :exit, reason ->
      Logger.warning("""
      MegasPinakas.Auth.Cache is unavailable (#{inspect(reason)}); \
      fetching an uncached token. Every request will pay the full token cost \
      until the cache is running.
      """)

      case Auth.fetch_fresh_token() do
        {:ok, %{token: token}} -> {:ok, token}
        {:error, reason} -> {:error, reason}
      end
  end

  # ==========================================================================
  # GenServer
  # ==========================================================================

  @impl true
  def init(_opts) do
    :ets.new(@table, [
      :set,
      :named_table,
      :protected,
      read_concurrency: true
    ])

    {:ok, %{}}
  end

  @impl true
  def handle_call(:refresh, _from, state) do
    # Re-check before fetching. Callers that queued behind an in-flight refresh
    # find a fresh token here, which is what collapses N concurrent misses into
    # a single token fetch.
    case lookup_valid() do
      {:ok, token} -> {:reply, {:ok, token}, state}
      :stale -> {:reply, do_refresh(), state}
    end
  end

  @impl true
  def handle_call(:invalidate, _from, state) do
    :ets.delete(@table, :token)
    {:reply, :ok, state}
  end

  defp do_refresh do
    case Auth.fetch_fresh_token() do
      {:ok, %{token: token, expires_at: expires_at}} ->
        :ets.insert(@table, {:token, token, expires_at})
        {:ok, token}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp now, do: System.os_time(:second)
end
