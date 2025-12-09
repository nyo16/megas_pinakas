defmodule MegasPinakas.Auth do
  @moduledoc """
  Authentication handling for BigTable gRPC requests.

  Supports multiple authentication strategies:
  1. Emulator mode - skip authentication entirely
  2. Goth library - Google Cloud token management
  3. gcloud CLI fallback - local development authentication
  """

  alias MegasPinakas.Config

  @doc """
  Returns gRPC request options including authentication metadata.

  When running against the emulator, returns empty options (no auth required).
  For production, attempts to get a valid OAuth token.
  """
  @spec request_opts() :: keyword()
  def request_opts do
    if Config.emulator?() do
      # Emulator: Skip authentication entirely
      []
    else
      # Production: Add authentication
      case get_token() do
        {:ok, token} ->
          [metadata: %{"authorization" => token}]

        {:error, _reason} ->
          []
      end
    end
  end

  @doc """
  Retrieves an OAuth token for BigTable API access.

  Attempts authentication in the following order:
  1. Goth library (if configured)
  2. gcloud CLI (fallback for local development)
  """
  @spec get_token() :: {:ok, String.t()} | {:error, term()}
  def get_token do
    case Application.get_env(:megas_pinakas, :goth) do
      nil ->
        get_token_fallback()

      goth_name ->
        get_token_from_goth(goth_name)
    end
  end

  defp get_token_from_goth(goth_name) do
    if Code.ensure_loaded?(Goth) do
      case Goth.fetch(goth_name) do
        {:ok, %{token: token, type: type}} ->
          {:ok, "#{type} #{token}"}

        {:error, reason} ->
          # Try fallback if Goth fails
          case get_token_fallback() do
            {:ok, token} -> {:ok, token}
            {:error, _} -> {:error, {:goth_error, reason}}
          end
      end
    else
      get_token_fallback()
    end
  end

  defp get_token_fallback do
    try do
      case System.cmd("gcloud", ["auth", "application-default", "print-access-token"],
             stderr_to_stdout: true
           ) do
        {token_output, 0} ->
          token = String.trim(token_output)
          {:ok, "Bearer #{token}"}

        {error_output, _} ->
          {:error, {:gcloud_error, String.trim(error_output)}}
      end
    rescue
      _ -> {:error, :no_auth_available}
    catch
      _ -> {:error, :no_auth_available}
    end
  end

  @doc """
  Returns true if authentication is available.
  """
  @spec authenticated?() :: boolean()
  def authenticated? do
    Config.emulator?() or match?({:ok, _}, get_token())
  end
end
