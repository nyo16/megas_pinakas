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

  ## The gcloud CLI fallback

  `gcloud auth application-default print-access-token` spawns a subprocess and
  takes ~1 s. It exists for local development where Goth is not configured, and
  is **disabled when the library is compiled with `MIX_ENV=prod`**. Override
  explicitly if you must:

      config :megas_pinakas, :allow_gcloud_auth_fallback, true

  For production, configure Goth instead:

      config :megas_pinakas, :goth, MyApp.Goth
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

  @doc """
  Returns gRPC request options including authentication metadata.

  When running against the emulator, returns only `:timeout` (no auth required).
  For production, attaches a cached OAuth token as gRPC metadata.
  """
  @spec request_opts() :: keyword()
  def request_opts do
    timeout = Config.default_timeout()

    base_opts =
      if Config.emulator?() do
        []
      else
        case get_token() do
          {:ok, token} ->
            [metadata: %{"authorization" => token}]

          {:error, reason} ->
            Logger.warning("BigTable auth token fetch failed: #{inspect(reason)}")
            []
        end
      end

    Keyword.put(base_opts, :timeout, timeout)
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
  2. Goth (when `:goth` is configured and the library is available)
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
      gcloud_token()
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

  defp run_gcloud do
    case System.cmd("gcloud", ["auth", "application-default", "print-access-token"],
           stderr_to_stdout: true
         ) do
      {token_output, 0} ->
        token = String.trim(token_output)
        {:ok, %{token: "Bearer #{token}", expires_at: now() + @gcloud_assumed_ttl_seconds}}

      {error_output, _} ->
        {:error, {:gcloud_error, String.trim(error_output)}}
    end
  rescue
    e in ErlangError ->
      {:error, {:gcloud_not_found, Exception.message(e)}}

    e ->
      {:error, {:auth_error, Exception.message(e)}}
  end

  defp now, do: Cache.now()
end
