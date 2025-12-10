defmodule MegasPinakas.AuthTest do
  use ExUnit.Case, async: false

  alias MegasPinakas.Auth

  describe "request_opts/0" do
    test "returns empty list when emulator is configured" do
      original = Application.get_env(:megas_pinakas, :emulator)
      Application.put_env(:megas_pinakas, :emulator, host: "localhost", port: 8086)

      assert Auth.request_opts() == []

      # Restore
      if original do
        Application.put_env(:megas_pinakas, :emulator, original)
      else
        Application.delete_env(:megas_pinakas, :emulator)
      end
    end

    test "returns empty list when BIGTABLE_EMULATOR_HOST is set" do
      original_config = Application.get_env(:megas_pinakas, :emulator)
      original_env = System.get_env("BIGTABLE_EMULATOR_HOST")

      Application.delete_env(:megas_pinakas, :emulator)
      System.put_env("BIGTABLE_EMULATOR_HOST", "localhost:8086")

      assert Auth.request_opts() == []

      # Restore
      System.delete_env("BIGTABLE_EMULATOR_HOST")
      if original_env, do: System.put_env("BIGTABLE_EMULATOR_HOST", original_env)
      if original_config, do: Application.put_env(:megas_pinakas, :emulator, original_config)
    end
  end

  describe "authenticated?/0" do
    test "returns true when emulator is configured" do
      original = Application.get_env(:megas_pinakas, :emulator)
      Application.put_env(:megas_pinakas, :emulator, host: "localhost", port: 8086)

      assert Auth.authenticated?() == true

      # Restore
      if original do
        Application.put_env(:megas_pinakas, :emulator, original)
      else
        Application.delete_env(:megas_pinakas, :emulator)
      end
    end
  end

  describe "get_token/0" do
    test "returns ok or error tuple when no Goth configured" do
      original_config = Application.get_env(:megas_pinakas, :emulator)
      original_goth = Application.get_env(:megas_pinakas, :goth)
      original_env = System.get_env("BIGTABLE_EMULATOR_HOST")

      Application.delete_env(:megas_pinakas, :emulator)
      Application.delete_env(:megas_pinakas, :goth)
      System.delete_env("BIGTABLE_EMULATOR_HOST")

      # This will try gcloud CLI - may succeed or fail depending on environment
      result = Auth.get_token()

      # Should return a tuple in either case
      assert match?({:ok, _}, result) or match?({:error, _}, result)

      # If successful, token should be a Bearer token
      case result do
        {:ok, token} -> assert String.starts_with?(token, "Bearer ")
        {:error, _} -> :ok
      end

      # Restore
      if original_env, do: System.put_env("BIGTABLE_EMULATOR_HOST", original_env)
      if original_config, do: Application.put_env(:megas_pinakas, :emulator, original_config)
      if original_goth, do: Application.put_env(:megas_pinakas, :goth, original_goth)
    end
  end
end
