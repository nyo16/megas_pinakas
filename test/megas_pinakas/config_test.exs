defmodule MegasPinakas.ConfigTest do
  use ExUnit.Case, async: true

  alias MegasPinakas.Config

  describe "resource path builders" do
    test "project_path/1 builds correct path" do
      assert Config.project_path("my-project") == "projects/my-project"
    end

    test "instance_path/2 builds correct path" do
      assert Config.instance_path("my-project", "my-instance") ==
               "projects/my-project/instances/my-instance"
    end

    test "table_path/3 builds correct path" do
      assert Config.table_path("my-project", "my-instance", "my-table") ==
               "projects/my-project/instances/my-instance/tables/my-table"
    end

    test "cluster_path/3 builds correct path" do
      assert Config.cluster_path("my-project", "my-instance", "my-cluster") ==
               "projects/my-project/instances/my-instance/clusters/my-cluster"
    end

    test "backup_path/4 builds correct path" do
      assert Config.backup_path("my-project", "my-instance", "my-cluster", "my-backup") ==
               "projects/my-project/instances/my-instance/clusters/my-cluster/backups/my-backup"
    end

    test "app_profile_path/3 builds correct path" do
      assert Config.app_profile_path("my-project", "my-instance", "my-profile") ==
               "projects/my-project/instances/my-instance/appProfiles/my-profile"
    end

    test "location_path/2 builds correct path" do
      assert Config.location_path("my-project", "us-central1-b") ==
               "projects/my-project/locations/us-central1-b"
    end
  end

  describe "emulator detection" do
    test "emulator?/0 returns false when no emulator config" do
      # Clear any emulator config
      original = Application.get_env(:megas_pinakas, :emulator)
      Application.delete_env(:megas_pinakas, :emulator)
      System.delete_env("BIGTABLE_EMULATOR_HOST")

      refute Config.emulator?()

      # Restore
      if original, do: Application.put_env(:megas_pinakas, :emulator, original)
    end

    test "emulator?/0 returns true when emulator config is set" do
      original = Application.get_env(:megas_pinakas, :emulator)
      Application.put_env(:megas_pinakas, :emulator, host: "localhost", port: 8086)

      assert Config.emulator?()

      # Restore
      if original do
        Application.put_env(:megas_pinakas, :emulator, original)
      else
        Application.delete_env(:megas_pinakas, :emulator)
      end
    end

    test "emulator_endpoint/0 returns nil when no config" do
      original = Application.get_env(:megas_pinakas, :emulator)
      Application.delete_env(:megas_pinakas, :emulator)
      System.delete_env("BIGTABLE_EMULATOR_HOST")

      assert Config.emulator_endpoint() == nil

      # Restore
      if original, do: Application.put_env(:megas_pinakas, :emulator, original)
    end

    test "emulator_endpoint/0 returns tuple from config" do
      original = Application.get_env(:megas_pinakas, :emulator)
      Application.put_env(:megas_pinakas, :emulator, host: "myhost", port: 9999)
      System.delete_env("BIGTABLE_EMULATOR_HOST")

      assert Config.emulator_endpoint() == {"myhost", 9999}

      # Restore
      if original do
        Application.put_env(:megas_pinakas, :emulator, original)
      else
        Application.delete_env(:megas_pinakas, :emulator)
      end
    end

    test "emulator_endpoint/0 parses BIGTABLE_EMULATOR_HOST env var" do
      original_config = Application.get_env(:megas_pinakas, :emulator)
      original_env = System.get_env("BIGTABLE_EMULATOR_HOST")

      Application.delete_env(:megas_pinakas, :emulator)
      System.put_env("BIGTABLE_EMULATOR_HOST", "emulator-host:8765")

      assert Config.emulator_endpoint() == {"emulator-host", 8765}

      # Restore
      System.delete_env("BIGTABLE_EMULATOR_HOST")
      if original_env, do: System.put_env("BIGTABLE_EMULATOR_HOST", original_env)
      if original_config, do: Application.put_env(:megas_pinakas, :emulator, original_config)
    end
  end

  describe "production_endpoint/0" do
    test "returns BigTable production host and port" do
      assert Config.production_endpoint() == {"bigtable.googleapis.com", 443}
    end
  end

  describe "default_pool_size/0" do
    test "returns configured pool size" do
      original = Application.get_env(:megas_pinakas, :default_pool_size)
      Application.put_env(:megas_pinakas, :default_pool_size, 15)

      assert Config.default_pool_size() == 15

      # Restore
      if original do
        Application.put_env(:megas_pinakas, :default_pool_size, original)
      else
        Application.delete_env(:megas_pinakas, :default_pool_size)
      end
    end

    test "returns default of 5 when not configured" do
      original = Application.get_env(:megas_pinakas, :default_pool_size)
      Application.delete_env(:megas_pinakas, :default_pool_size)

      assert Config.default_pool_size() == 5

      # Restore
      if original, do: Application.put_env(:megas_pinakas, :default_pool_size, original)
    end
  end
end
