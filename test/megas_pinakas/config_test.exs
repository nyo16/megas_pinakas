defmodule MegasPinakas.ConfigTest do
  # Mutates application and OS environment; must not interleave with other
  # suites that read them.
  use ExUnit.Case, async: false

  alias MegasPinakas.Config

  @env_var "BIGTABLE_EMULATOR_HOST"

  # Runs `fun` with the `:emulator` app config and BIGTABLE_EMULATOR_HOST set as
  # given (`nil` = unset), restoring both afterwards whatever happens.
  defp with_emulator_env(config, env_value, fun) do
    original_config = Application.fetch_env(:megas_pinakas, :emulator)
    original_env = System.get_env(@env_var)

    put_app_env(:emulator, config)
    put_os_env(env_value)

    try do
      fun.()
    after
      case original_config do
        {:ok, value} -> Application.put_env(:megas_pinakas, :emulator, value)
        :error -> Application.delete_env(:megas_pinakas, :emulator)
      end

      put_os_env(original_env)
    end
  end

  defp with_app_env(key, value, fun) do
    original = Application.fetch_env(:megas_pinakas, key)
    put_app_env(key, value)

    try do
      fun.()
    after
      case original do
        {:ok, v} -> Application.put_env(:megas_pinakas, key, v)
        :error -> Application.delete_env(:megas_pinakas, key)
      end
    end
  end

  defp put_app_env(key, nil), do: Application.delete_env(:megas_pinakas, key)
  defp put_app_env(key, value), do: Application.put_env(:megas_pinakas, key, value)

  defp put_os_env(nil), do: System.delete_env(@env_var)
  defp put_os_env(value), do: System.put_env(@env_var, value)

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

  describe "emulator?/0" do
    test "is false with no emulator config, env var, or local pool endpoint" do
      with_emulator_env(nil, nil, fn ->
        with_app_env(GrpcConnectionPool, nil, fn ->
          refute Config.emulator?()
        end)
      end)
    end

    test "is true when emulator config is set" do
      with_emulator_env([host: "localhost", port: 8086], nil, fn ->
        assert Config.emulator?()
      end)
    end

    test "is true when BIGTABLE_EMULATOR_HOST is set" do
      with_emulator_env(nil, "localhost:8086", fn ->
        assert Config.emulator?()
      end)
    end

    test "is true when the GrpcConnectionPool data endpoint is type: :local" do
      with_emulator_env(nil, nil, fn ->
        pool = [endpoint: [type: :local, host: "127.0.0.1", port: 8086]]

        with_app_env(GrpcConnectionPool, pool, fn ->
          assert Config.emulator?()
        end)
      end)
    end

    test "is false when the GrpcConnectionPool data endpoint is production" do
      with_emulator_env(nil, nil, fn ->
        pool = [endpoint: [type: :production, host: "bigtable.googleapis.com", port: 443]]

        with_app_env(GrpcConnectionPool, pool, fn ->
          refute Config.emulator?()
        end)
      end)
    end
  end

  describe "emulator_endpoint/0" do
    test "returns nil when no config" do
      with_emulator_env(nil, nil, fn ->
        assert Config.emulator_endpoint() == nil
      end)
    end

    test "returns tuple from config" do
      with_emulator_env([host: "myhost", port: 9999], nil, fn ->
        assert Config.emulator_endpoint() == {"myhost", 9999}
      end)
    end

    test "fills in defaults for a partial config" do
      with_emulator_env([project_id: "p"], nil, fn ->
        assert Config.emulator_endpoint() == {"localhost", 8086}
      end)
    end

    test "env var takes precedence over config" do
      with_emulator_env([host: "myhost", port: 9999], "emulator-host:8765", fn ->
        assert Config.emulator_endpoint() == {"emulator-host", 8765}
      end)
    end

    test "parses host:port" do
      with_emulator_env(nil, "emulator-host:8765", fn ->
        assert Config.emulator_endpoint() == {"emulator-host", 8765}
      end)
    end

    test "defaults the port when only a host is given" do
      with_emulator_env(nil, "emulator-host", fn ->
        assert Config.emulator_endpoint() == {"emulator-host", 8086}
      end)
    end

    test "parses a bracketed IPv6 literal with port" do
      with_emulator_env(nil, "[::1]:8086", fn ->
        assert Config.emulator_endpoint() == {"::1", 8086}
      end)
    end

    test "parses a bracketed IPv6 literal without port" do
      with_emulator_env(nil, "[::1]", fn ->
        assert Config.emulator_endpoint() == {"::1", 8086}
      end)
    end

    test "raises on a non-numeric port, naming the value" do
      with_emulator_env(nil, "host:abc", fn ->
        error = assert_raise(ArgumentError, fn -> Config.emulator_endpoint() end)
        assert error.message =~ "host:abc"
        assert error.message =~ "abc"
      end)
    end

    test "raises on an out-of-range port" do
      with_emulator_env(nil, "host:70000", fn ->
        assert_raise ArgumentError, ~r/70000/, fn -> Config.emulator_endpoint() end
      end)
    end

    test "raises on a trailing colon with no port" do
      with_emulator_env(nil, "host:", fn ->
        assert_raise ArgumentError, fn -> Config.emulator_endpoint() end
      end)
    end

    test "raises on an unbracketed IPv6 literal" do
      with_emulator_env(nil, "::1", fn ->
        assert_raise ArgumentError, ~r/\[addr\]:port/, fn -> Config.emulator_endpoint() end
      end)
    end

    test "raises on an empty host" do
      with_emulator_env(nil, ":8086", fn ->
        assert_raise ArgumentError, ~r/empty host/, fn -> Config.emulator_endpoint() end
      end)
    end
  end

  describe "build_pool_config/0" do
    test "targets the emulator with the default pool name" do
      with_emulator_env([host: "emu", port: 1234], nil, fn ->
        config = Config.build_pool_config()

        assert %GrpcConnectionPool.Config{} = config
        assert config.endpoint.type == :local
        assert {"emu", 1234, _opts} = GrpcConnectionPool.Config.get_endpoint(config)
        assert config.pool.name == MegasPinakas.ConnectionPool
      end)
    end

    test "targets production Data API when no emulator is configured" do
      with_emulator_env(nil, nil, fn ->
        config = Config.build_pool_config()

        assert config.endpoint.type == :production

        assert {"bigtable.googleapis.com", 443, _} =
                 GrpcConnectionPool.Config.get_endpoint(config)

        assert config.pool.name == MegasPinakas.ConnectionPool
      end)
    end

    test "uses the GrpcConnectionPool config when present, defaulting the pool name" do
      with_emulator_env(nil, nil, fn ->
        pool = [endpoint: [type: :local, host: "127.0.0.1", port: 9090], pool: [size: 7]]

        with_app_env(GrpcConnectionPool, pool, fn ->
          config = Config.build_pool_config()

          assert {"127.0.0.1", 9090, _} = GrpcConnectionPool.Config.get_endpoint(config)
          assert config.pool.size == 7
          assert config.pool.name == MegasPinakas.ConnectionPool
        end)
      end)
    end

    # Client hard-codes the lookup name; honouring a custom one would start a
    # pool no RPC can reach.
    test "overrides an explicitly named pool with the name Client looks up" do
      pool = [endpoint: [type: :local, host: "127.0.0.1", port: 9090], pool: [name: My.Pool]]

      with_app_env(GrpcConnectionPool, pool, fn ->
        assert Config.build_pool_config().pool.name == MegasPinakas.ConnectionPool
      end)
    end

    test "raises on an invalid GrpcConnectionPool config instead of using defaults" do
      with_app_env(GrpcConnectionPool, [endpoint: [type: :local]], fn ->
        assert_raise ArgumentError, ~r/GrpcConnectionPool/, fn -> Config.build_pool_config() end
      end)
    end
  end

  describe "build_admin_pool_config/0" do
    test "targets the admin host in production" do
      with_emulator_env(nil, nil, fn ->
        config = Config.build_admin_pool_config()

        assert config.endpoint.type == :production

        assert {"bigtableadmin.googleapis.com", 443, _} =
                 GrpcConnectionPool.Config.get_endpoint(config)

        assert config.pool.name == MegasPinakas.AdminConnectionPool
      end)
    end

    test "follows the emulator endpoint" do
      with_emulator_env(nil, "emu:1234", fn ->
        config = Config.build_admin_pool_config()

        assert config.endpoint.type == :local
        assert {"emu", 1234, _} = GrpcConnectionPool.Config.get_endpoint(config)
      end)
    end

    test "follows a local GrpcConnectionPool data endpoint" do
      with_emulator_env(nil, nil, fn ->
        pool = [endpoint: [type: :local, host: "127.0.0.1", port: 9090]]

        with_app_env(GrpcConnectionPool, pool, fn ->
          config = Config.build_admin_pool_config()

          assert config.endpoint.type == :local
          assert {"127.0.0.1", 9090, _} = GrpcConnectionPool.Config.get_endpoint(config)
          assert config.pool.name == MegasPinakas.AdminConnectionPool
        end)
      end)
    end
  end

  describe "production_endpoint/0" do
    test "returns BigTable production host and port" do
      assert Config.production_endpoint() == {"bigtable.googleapis.com", 443}
    end
  end

  describe "admin_endpoint/0" do
    test "returns the admin host and port" do
      assert Config.admin_endpoint() == {"bigtableadmin.googleapis.com", 443}
    end
  end

  describe "default_pool_size/0" do
    test "returns configured pool size" do
      with_app_env(:default_pool_size, 15, fn ->
        assert Config.default_pool_size() == 15
      end)
    end

    test "returns default of 5 when not configured" do
      with_app_env(:default_pool_size, nil, fn ->
        assert Config.default_pool_size() == 5
      end)
    end
  end
end
