defmodule MegasPinakas.AuthTest do
  use ExUnit.Case, async: false

  alias MegasPinakas.Auth
  alias MegasPinakas.Auth.Cache
  alias MegasPinakas.AuthError
  alias MegasPinakas.Client
  alias MegasPinakas.Test.Emulator

  @env_var "BIGTABLE_EMULATOR_HOST"
  @keys [:emulator, :goth, :token_source, :allow_gcloud_auth_fallback]

  # Applies `overrides` (a keyword of app-env keys; `nil` deletes) plus the OS
  # env var, runs `fun`, and restores everything. The cache is invalidated on
  # both sides so a token or remembered failure never leaks between tests.
  defp with_auth_env(overrides, env_value, fun) do
    originals = Map.new(@keys, &{&1, Application.fetch_env(:megas_pinakas, &1)})
    original_env = System.get_env(@env_var)

    Enum.each(overrides, fn {key, value} -> put_app_env(key, value) end)
    put_os_env(env_value)
    Cache.invalidate()

    try do
      fun.()
    after
      Enum.each(originals, fn
        {key, {:ok, value}} -> Application.put_env(:megas_pinakas, key, value)
        {key, :error} -> Application.delete_env(:megas_pinakas, key)
      end)

      put_os_env(original_env)
      Cache.invalidate()
    end
  end

  defp put_app_env(key, nil), do: Application.delete_env(:megas_pinakas, key)
  defp put_app_env(key, value), do: Application.put_env(:megas_pinakas, key, value)

  defp put_os_env(nil), do: System.delete_env(@env_var)
  defp put_os_env(value), do: System.put_env(@env_var, value)

  # Production-shaped environment: no emulator, gcloud never spawned.
  @production [emulator: nil, allow_gcloud_auth_fallback: false]

  describe "request_opts/0 in emulator mode" do
    test "returns timeout only when emulator is configured" do
      with_auth_env([emulator: [host: "localhost", port: 8086]], nil, fn ->
        opts = Auth.request_opts()
        assert Keyword.has_key?(opts, :timeout)
        refute Keyword.has_key?(opts, :metadata)
      end)
    end

    test "returns timeout only when BIGTABLE_EMULATOR_HOST is set" do
      with_auth_env([emulator: nil], "localhost:8086", fn ->
        opts = Auth.request_opts()
        assert Keyword.has_key?(opts, :timeout)
        refute Keyword.has_key?(opts, :metadata)
      end)
    end
  end

  describe "request_opts/0 with a token" do
    test "attaches the token verbatim as authorization metadata" do
      source = fn -> {:ok, %{token: "Bearer abc", expires_at: Cache.now() + 3600}} end

      with_auth_env(@production ++ [token_source: source], nil, fn ->
        opts = Auth.request_opts()
        assert opts[:metadata] == %{"authorization" => "Bearer abc"}
        assert is_integer(opts[:timeout])
      end)
    end
  end

  describe "request_opts/0 when no token can be obtained" do
    test "raises AuthError carrying the source's reason" do
      with_auth_env(@production ++ [token_source: fn -> {:error, :boom} end], nil, fn ->
        error = assert_raise(AuthError, fn -> Auth.request_opts() end)
        assert error.reason == :boom
        assert Exception.message(error) =~ ":boom"
      end)
    end

    test "a misnamed Goth process yields {:goth_exit, {:noproc, _}} without crashing the cache" do
      cache_pid = Process.whereis(Cache)
      assert is_pid(cache_pid)

      with_auth_env(@production ++ [goth: :no_such_goth_process], nil, fn ->
        error = assert_raise(AuthError, fn -> Auth.request_opts() end)
        assert {:goth_exit, {:noproc, _}} = error.reason
      end)

      assert Process.whereis(Cache) == cache_pid
    end

    test "a token source that raises yields {:token_source_error, _} and the cache survives" do
      cache_pid = Process.whereis(Cache)
      source = fn -> raise "credentials file is unreadable" end

      with_auth_env(@production ++ [token_source: source], nil, fn ->
        assert {:error, {:token_source_error, message}} = Auth.get_token()
        assert message =~ "credentials file is unreadable"
      end)

      assert Process.whereis(Cache) == cache_pid
    end

    # Needs a checked-out channel, so a live pool: the operation closure is
    # where request_opts/0 runs, and Client.execute/2 must turn the raise into
    # a tuple *before* any RPC goes out.
    @tag :emulator
    test "Client.execute/2 converts AuthError to {:error, {:auth_error, reason}} without sending" do
      Emulator.await_pool!()
      cache_pid = Process.whereis(Cache)
      parent = self()

      operation = fn _channel ->
        Auth.request_opts()
        send(parent, :rpc_attempted)
      end

      with_auth_env(@production ++ [goth: :no_such_goth_process], nil, fn ->
        assert {:error, {:auth_error, {:goth_exit, {:noproc, _}}}} = Client.execute(operation)
        refute_received :rpc_attempted
      end)

      source = fn -> raise "credentials file is unreadable" end

      with_auth_env(@production ++ [token_source: source], nil, fn ->
        assert {:error, {:auth_error, {:token_source_error, _}}} = Client.execute(operation)
        refute_received :rpc_attempted
      end)

      assert Process.whereis(Cache) == cache_pid
    end

    test "a token source that exits or throws is contained" do
      cache_pid = Process.whereis(Cache)

      with_auth_env(@production ++ [token_source: fn -> exit(:kaboom) end], nil, fn ->
        assert {:error, {:token_source_exit, :exit, :kaboom}} = Auth.get_token()
      end)

      with_auth_env(@production ++ [token_source: fn -> throw(:oops) end], nil, fn ->
        assert {:error, {:token_source_exit, :throw, :oops}} = Auth.get_token()
      end)

      assert Process.whereis(Cache) == cache_pid
    end

    test "a token source returning the wrong shape is rejected" do
      with_auth_env(@production ++ [token_source: fn -> {:ok, "Bearer x"} end], nil, fn ->
        assert {:error, {:invalid_token_source_result, {:ok, "Bearer x"}}} = Auth.get_token()
      end)
    end
  end

  describe "authenticated?/0" do
    test "returns true when emulator is configured" do
      with_auth_env([emulator: [host: "localhost", port: 8086]], nil, fn ->
        assert Auth.authenticated?()
      end)
    end

    test "returns false when the token source fails" do
      with_auth_env(@production ++ [token_source: fn -> {:error, :nope} end], nil, fn ->
        refute Auth.authenticated?()
      end)
    end
  end

  describe "get_token/0 with no Goth configured" do
    test "reports the gcloud gate instead of shelling out" do
      with_auth_env(@production ++ [goth: nil, token_source: nil], nil, fn ->
        assert Auth.get_token() == {:error, :gcloud_fallback_disabled}
      end)
    end
  end

  describe "fetch_fresh_token/0 dispatch" do
    test "calls an {m, f, a} token source" do
      source = {Kernel, :apply, [fn -> {:ok, %{token: "Bearer mfa", expires_at: 1}} end, []]}

      with_auth_env([token_source: source], nil, fn ->
        assert Auth.fetch_fresh_token() == {:ok, %{token: "Bearer mfa", expires_at: 1}}
      end)
    end
  end
end
