defmodule MegasPinakas.AuthError do
  @moduledoc """
  Raised by `MegasPinakas.Auth.request_opts/0` when no access token can be
  obtained outside emulator mode.

  `MegasPinakas.Client.execute/2` rescues this into
  `{:error, {:auth_error, reason}}`, so callers of the public API never see the
  exception; it exists so an operation closure fails *before* sending an
  unauthenticated RPC rather than after a round-trip that Google rejects.
  """

  defexception [:reason]

  @impl true
  def message(%__MODULE__{reason: reason}) do
    "BigTable authentication failed: #{inspect(reason)}"
  end
end

defmodule MegasPinakas.Auth do
  @moduledoc """
  Authentication handling for BigTable gRPC requests.

  Supports multiple authentication strategies:

  1. Emulator mode - skip authentication entirely
  2. Goth library - Google Cloud token management (**recommended for production**)
  3. gcloud CLI fallback - local development only, disabled in production

  ## Token caching

  Tokens are cached by `MegasPinakas.Auth.Cache`, so `request_opts/0` is a
  lock-free ETS read on the hot path. This matters: an uncached fetch through
  the gcloud CLI fallback was measured at **911.9 ms**, paid once per RPC.

  ## Failure behaviour

  When no token can be obtained, `request_opts/0` raises
  `MegasPinakas.AuthError`; `MegasPinakas.Client.execute/2` turns that into
  `{:error, {:auth_error, reason}}`. The cache remembers the failure for a few
  seconds so a broken token source is not re-invoked (and re-logged) on every
  RPC.

  ## Goth

  Goth is an *optional* dependency. Add `{:goth, "~> 1.4"}` to your own deps
  and point `:goth` at your Goth process:

      config :megas_pinakas, :goth, MyApp.Goth

  If `:goth` is configured but the library is not compiled in, token fetches
  fail with `{:error, :goth_not_available}` rather than silently falling back
  to the gcloud CLI.

  ## The gcloud CLI fallback

  `gcloud auth application-default print-access-token` spawns a subprocess and
  takes ~1 s. It exists for local development where Goth is not configured, and
  is **disabled when the library is compiled with `MIX_ENV=prod`**. Override
  explicitly if you must:

      config :megas_pinakas, :allow_gcloud_auth_fallback, true
  """

  require Logger

  alias MegasPinakas.Auth.Cache
  alias MegasPinakas.Config

  # Resolved at compile time. When this library is compiled as a dependency of a
  # release, `Mix.env()` is the release's MIX_ENV — which is what we want to gate
  # on, and `Mix` is not available at runtime in a release anyway.
  #
  # Stored as the boolean rather than the env atom so there is no runtime
  # comparison against a constant.
  @gcloud_fallback_default Mix.env() != :prod

  # `print-access-token` reports no expiry. Google access tokens live one hour;
  # assume slightly less so a cached token is never served past its life.
  @gcloud_assumed_ttl_seconds 55 * 60

  # A healthy gcloud call takes ~1 s. Anything past this is a hung subprocess
  # (network, or gcloud waiting on a prompt we suppressed), not a slow one.
  @gcloud_timeout 10_000

  @doc """
  Returns gRPC request options including authentication metadata.

  When running against the emulator, returns only `:timeout` (no auth required).
  Otherwise attaches a cached OAuth token as gRPC metadata.

  Raises `MegasPinakas.AuthError` when no token can be obtained. This is the
  intended way for an operation closure to abort before the RPC is sent;
  `MegasPinakas.Client.execute/2` converts it to `{:error, {:auth_error, reason}}`.
  """
  @spec request_opts() :: keyword()
  def request_opts do
    timeout = Config.default_timeout()

    if Config.emulator?() do
      [timeout: timeout]
    else
      case get_token() do
        {:ok, token} -> [metadata: %{"authorization" => token}, timeout: timeout]
        {:error, reason} -> raise MegasPinakas.AuthError, reason: reason
      end
    end
  end

  @doc """
  Retrieves an OAuth token for BigTable API access.

  Served from `MegasPinakas.Auth.Cache`; only refreshes when the cached token is
  absent or within a minute of expiry. Use `fetch_fresh_token/0` to bypass the
  cache.
  """
  @spec get_token() :: {:ok, String.t()} | {:error, term()}
  def get_token do
    Cache.fetch_token()
  end

  @doc """
  Fetches a token directly from the configured source, bypassing the cache.

  Returns `{:ok, %{token: token, expires_at: unix_seconds}}`. This is the
  primitive `MegasPinakas.Auth.Cache` calls on a miss; prefer `get_token/0`.

  Attempts, in order:

  1. `:token_source`, when configured (see below)
  2. Goth (when `:goth` is configured; `{:error, :goth_not_available}` if the
     library is not compiled in)
  3. gcloud CLI (non-production only)

  ## Custom token sources

  Set `:token_source` to a 0-arity function or an `{module, function, args}`
  tuple returning `{:ok, %{token: token, expires_at: unix_seconds}}` or
  `{:error, reason}`. The `token` must include its scheme (e.g. `"Bearer ..."`),
  since it is used verbatim as the `authorization` metadata value.

      config :megas_pinakas, :token_source, {MyApp.Auth, :bigtable_token, []}
  """
  @spec fetch_fresh_token() ::
          {:ok, %{token: String.t(), expires_at: non_neg_integer()}} | {:error, term()}
  def fetch_fresh_token do
    case Application.get_env(:megas_pinakas, :token_source) do
      nil -> default_token_source()
      fun when is_function(fun, 0) -> fun.()
      {module, function, args} -> apply(module, function, args)
    end
  end

  defp default_token_source do
    case Application.get_env(:megas_pinakas, :goth) do
      nil -> gcloud_token()
      goth_name -> goth_token(goth_name)
    end
  end

  @doc """
  Returns true if authentication is available.
  """
  @spec authenticated?() :: boolean()
  def authenticated? do
    Config.emulator?() or match?({:ok, _}, get_token())
  end

  @doc """
  Returns true when the gcloud CLI fallback may be used.

  False by default in production builds, because shelling out to a subprocess on
  every token refresh is not acceptable there. Override with
  `config :megas_pinakas, :allow_gcloud_auth_fallback, true`.
  """
  @spec gcloud_fallback_allowed?() :: boolean()
  def gcloud_fallback_allowed? do
    case Application.fetch_env(:megas_pinakas, :allow_gcloud_auth_fallback) do
      {:ok, allowed?} -> allowed?
      :error -> @gcloud_fallback_default
    end
  end

  # ==========================================================================
  # Token sources
  # ==========================================================================

  defp goth_token(goth_name) do
    if Code.ensure_loaded?(Goth) do
      fetch_goth_token(goth_name)
    else
      {:error, :goth_not_available}
    end
  end

  defp fetch_goth_token(goth_name) do
    case Goth.fetch(goth_name) do
      {:ok, %{token: token, type: type} = goth_token} ->
        {:ok, %{token: "#{type} #{token}", expires_at: goth_expires_at(goth_token)}}

      {:error, reason} ->
        # Fall back where permitted, but keep the Goth error if that also fails
        # — it is the more actionable of the two.
        case gcloud_token() do
          {:ok, result} -> {:ok, result}
          {:error, _} -> {:error, {:goth_error, reason}}
        end
    end
  catch
    # `Goth.fetch/1` is a GenServer.call; a misnamed or not-yet-started Goth
    # process exits with `{:noproc, _}`. That must not take the token cache
    # down with it.
    :exit, reason -> {:error, {:goth_exit, reason}}
  end

  # Goth reports an absolute Unix expiry. Guard against a source that omits it
  # rather than caching a token with a bogus lifetime.
  defp goth_expires_at(%{expires: expires}) when is_integer(expires) and expires > 0, do: expires
  defp goth_expires_at(_token), do: now() + @gcloud_assumed_ttl_seconds

  defp gcloud_token do
    if gcloud_fallback_allowed?() do
      Logger.warning("""
      Falling back to `gcloud auth application-default print-access-token` for \
      BigTable authentication. This spawns a subprocess (~1 s) and is not \
      suitable for production — configure Goth instead: \
      config :megas_pinakas, :goth, MyApp.Goth
      """)

      run_gcloud()
    else
      {:error, :gcloud_fallback_disabled}
    end
  end

  # Runs under a Task so a hung gcloud cannot block the token cache forever.
  # `gcloud_cmd/0` is total (never raises), so the linked task cannot crash the
  # caller; `Task.shutdown/2` reaps it on timeout.
  defp run_gcloud do
    task = Task.async(&gcloud_cmd/0)

    case Task.yield(task, @gcloud_timeout) || Task.shutdown(task, :brutal_kill) do
      {:ok, result} -> result
      {:exit, reason} -> {:error, {:gcloud_error, reason}}
      nil -> {:error, :gcloud_timeout}
    end
  end

  defp gcloud_cmd do
    # stderr is left alone: gcloud prints update nags and warnings there, and
    # merging them into stdout used to corrupt the token. Only the last
    # non-empty stdout line is the token.
    case System.cmd("gcloud", ["auth", "application-default", "print-access-token"],
           env: [{"CLOUDSDK_CORE_DISABLE_PROMPTS", "1"}]
         ) do
      {output, 0} ->
        case last_non_empty_line(output) do
          nil ->
            {:error, {:gcloud_error, :empty_output}}

          token ->
            {:ok, %{token: "Bearer #{token}", expires_at: now() + @gcloud_assumed_ttl_seconds}}
        end

      {_output, status} ->
        {:error, {:gcloud_error, {:exit_status, status}}}
    end
  rescue
    e in ErlangError ->
      {:error, {:gcloud_not_found, Exception.message(e)}}

    e ->
      {:error, {:gcloud_error, Exception.message(e)}}
  end

  defp last_non_empty_line(output) do
    output
    |> String.split("\n")
    |> Enum.map(&String.trim/1)
    |> Enum.reject(&(&1 == ""))
    |> List.last()
  end

  defp now, do: Cache.now()
end
