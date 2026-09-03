defmodule MegasPinakas.Auth.Cache do
  # Refresh once the token has less than this much life left, so callers are
  # never handed a token that expires mid-flight.
  @refresh_margin_seconds 60

  # How long a refresh failure is served from the table before the source is
  # tried again. Long enough to absorb a burst of RPCs, short enough that a
  # fixed credential is picked up promptly.
  @failure_ttl_seconds 5

  @moduledoc """
  Caches the OAuth access token used to authenticate BigTable RPCs.

  Every RPC calls `MegasPinakas.Auth.request_opts/0`, so an uncached token fetch
  is paid once per request. Measured before this cache existed, with the gcloud
  CLI fallback in play, that was **911.9 ms per call** — the token fetch, not
  the RPC, dominated every operation.

  ## Read path

  Reads are lock-free. `fetch_token/1` does a plain `:ets.lookup/2` against a
  `read_concurrency: true` table and returns immediately when the cached token
  has more than #{@refresh_margin_seconds} seconds of life left. No message passes through the
  GenServer on the hot path.

  ## Refresh path

  On a miss or near-expiry, the caller issues a `GenServer.call/3`. Because the
  GenServer serializes, and because `handle_call/3` re-checks the table before
  fetching, N concurrent callers arriving at expiry trigger **one** token fetch
  rather than N. Without that re-check, token expiry under load would produce a
  thundering herd of ~900 ms `gcloud` subprocess spawns.

  ## Failures

  The token source runs inside this process, so it is wrapped: an exception,
  exit, or throw from the source becomes `{:error, {:token_source_error, msg}}`
  or `{:error, {:token_source_exit, kind, reason}}` instead of crashing the
  cache (and, with it, every in-flight RPC waiting on the refresh).

  A failure is remembered for #{@failure_ttl_seconds} seconds. During that window `fetch_token/1`
  returns the cached `{:error, reason}` without touching the source, so a broken
  credential does not re-run a ~1 s subprocess — or emit a log line — per RPC.
  `invalidate/1` clears it.

  ## Expiry

  Goth reports an absolute expiry, which is used directly. The gcloud CLI
  fallback prints only the token with no expiry, so a conservative 55-minute TTL
  is assumed (Google access tokens live one hour).
  """

  use GenServer

  require Logger

  alias MegasPinakas.Auth

  @table __MODULE__

  # A refresh may shell out to gcloud, which was measured at 0.85-1.11 s.
  @refresh_timeout 30_000

  @doc false
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Returns a cached access token, refreshing it if absent or near expiry.

  Returns `{:error, reason}` when no token can be obtained. The error is cached
  for a few seconds (see the moduledoc), so repeated calls do not repeatedly
  invoke a failing token source.
  """
  @spec fetch_token(GenServer.server()) :: {:ok, String.t()} | {:error, term()}
  def fetch_token(server \\ __MODULE__) do
    case lookup_valid() do
      :stale -> request_refresh(server)
      result -> result
    end
  end

  @doc """
  Drops any cached token *and* any remembered failure so the next
  `fetch_token/1` refetches.

  Intended for tests and for recovering from a token revoked server-side.
  Returns `:ok` when the cache is not running; there is nothing to drop.
  """
  @spec invalidate(GenServer.server()) :: :ok
  def invalidate(server \\ __MODULE__) do
    case :ets.whereis(@table) do
      :undefined -> :ok
      _tid -> GenServer.call(server, :invalidate, @refresh_timeout)
    end
  catch
    :exit, {:noproc, _} -> :ok
  end

  @doc """
  Returns the cached token as `{:ok, token, expires_at}`, or `:error`.

  Exposed for tests and diagnostics; does not trigger a refresh and does not
  report remembered failures.
  """
  @spec peek() :: {:ok, String.t(), non_neg_integer()} | :error
  def peek do
    case :ets.lookup(@table, :token) do
      [{:token, token, expires_at}] -> {:ok, token, expires_at}
      [] -> :error
    end
  rescue
    # The table is owned by the GenServer; it vanishes if the cache is down.
    ArgumentError -> :error
  end

  # ==========================================================================
  # Read path — no GenServer involvement
  # ==========================================================================

  # A stale token stays in the table when its refresh fails, so the failure
  # must be consulted whenever the token is unusable — not only when absent —
  # or a stale token would keep re-triggering the refresh the failure suppresses.
  defp lookup_valid do
    now = now()

    case peek() do
      {:ok, token, expires_at} when expires_at - now > @refresh_margin_seconds -> {:ok, token}
      _ -> recent_failure(now)
    end
  end

  defp recent_failure(now) do
    case :ets.lookup(@table, :failure) do
      [{:failure, reason, until}] when until > now -> {:error, reason}
      _ -> :stale
    end
  rescue
    ArgumentError -> :stale
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

      case fetch_from_source() do
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
    # find a fresh token (or its fresh failure) here, which is what collapses N
    # concurrent misses into a single token fetch.
    case lookup_valid() do
      :stale -> {:reply, do_refresh(), state}
      result -> {:reply, result, state}
    end
  end

  @impl true
  def handle_call(:invalidate, _from, state) do
    :ets.delete_all_objects(@table)
    {:reply, :ok, state}
  end

  defp do_refresh do
    case fetch_from_source() do
      {:ok, %{token: token, expires_at: expires_at}} ->
        :ets.delete(@table, :failure)
        :ets.insert(@table, {:token, token, expires_at})
        {:ok, token}

      {:error, reason} ->
        :ets.insert(@table, {:failure, reason, now() + @failure_ttl_seconds})
        {:error, reason}
    end
  end

  # Total wrapper around the token source: whatever it does — raise, exit,
  # throw, or return garbage — comes back as a tuple.
  @spec fetch_from_source() ::
          {:ok, %{token: String.t(), expires_at: integer()}} | {:error, term()}
  defp fetch_from_source do
    case Auth.fetch_fresh_token() do
      {:ok, %{token: token, expires_at: expires_at}} = ok
      when is_binary(token) and is_integer(expires_at) ->
        ok

      {:error, _reason} = error ->
        error

      other ->
        {:error, {:invalid_token_source_result, other}}
    end
  rescue
    e -> {:error, {:token_source_error, Exception.message(e)}}
  catch
    kind, reason -> {:error, {:token_source_exit, kind, reason}}
  end

  # The single clock behind token expiry. `MegasPinakas.Auth` stamps `expires_at`
  # with this and the cache compares against it, so the two must not drift apart.
  @doc false
  @spec now() :: integer()
  def now, do: System.os_time(:second)
end
