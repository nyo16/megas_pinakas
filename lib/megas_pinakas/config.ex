defmodule MegasPinakas.Config do
  @moduledoc """
  Configuration helpers and resource path builders for BigTable operations.
  """

  @default_pool_size 5
  @default_emulator_host "localhost"
  @default_emulator_port 8086
  @production_host "bigtable.googleapis.com"
  @admin_host "bigtableadmin.googleapis.com"
  @production_port 443
  @default_timeout 30_000

  # Resource Path Builders

  @doc """
  Builds the full resource path for a BigTable instance.

  ## Examples

      iex> MegasPinakas.Config.instance_path("my-project", "my-instance")
      "projects/my-project/instances/my-instance"
  """
  @spec instance_path(String.t(), String.t()) :: String.t()
  def instance_path(project_id, instance_id) do
    "projects/#{project_id}/instances/#{instance_id}"
  end

  @doc """
  Builds the full resource path for a BigTable table.

  ## Examples

      iex> MegasPinakas.Config.table_path("my-project", "my-instance", "my-table")
      "projects/my-project/instances/my-instance/tables/my-table"
  """
  @spec table_path(String.t(), String.t(), String.t()) :: String.t()
  def table_path(project_id, instance_id, table_id) do
    "#{instance_path(project_id, instance_id)}/tables/#{table_id}"
  end

  @doc """
  Builds the full resource path for a BigTable cluster.

  ## Examples

      iex> MegasPinakas.Config.cluster_path("my-project", "my-instance", "my-cluster")
      "projects/my-project/instances/my-instance/clusters/my-cluster"
  """
  @spec cluster_path(String.t(), String.t(), String.t()) :: String.t()
  def cluster_path(project_id, instance_id, cluster_id) do
    "#{instance_path(project_id, instance_id)}/clusters/#{cluster_id}"
  end

  @doc """
  Builds the full resource path for a BigTable backup.

  ## Examples

      iex> MegasPinakas.Config.backup_path("my-project", "my-instance", "my-cluster", "my-backup")
      "projects/my-project/instances/my-instance/clusters/my-cluster/backups/my-backup"
  """
  @spec backup_path(String.t(), String.t(), String.t(), String.t()) :: String.t()
  def backup_path(project_id, instance_id, cluster_id, backup_id) do
    "#{cluster_path(project_id, instance_id, cluster_id)}/backups/#{backup_id}"
  end

  @doc """
  Builds the full resource path for a BigTable app profile.

  ## Examples

      iex> MegasPinakas.Config.app_profile_path("my-project", "my-instance", "my-profile")
      "projects/my-project/instances/my-instance/appProfiles/my-profile"
  """
  @spec app_profile_path(String.t(), String.t(), String.t()) :: String.t()
  def app_profile_path(project_id, instance_id, app_profile_id) do
    "#{instance_path(project_id, instance_id)}/appProfiles/#{app_profile_id}"
  end

  @doc """
  Builds the project path.

  ## Examples

      iex> MegasPinakas.Config.project_path("my-project")
      "projects/my-project"
  """
  @spec project_path(String.t()) :: String.t()
  def project_path(project_id) do
    "projects/#{project_id}"
  end

  @doc """
  Builds the location path for a project.

  ## Examples

      iex> MegasPinakas.Config.location_path("my-project", "us-central1-b")
      "projects/my-project/locations/us-central1-b"
  """
  @spec location_path(String.t(), String.t()) :: String.t()
  def location_path(project_id, location) do
    "#{project_path(project_id)}/locations/#{location}"
  end

  # Environment Detection

  @doc """
  Returns true if running against the BigTable emulator.

  True when an emulator endpoint is configured (`:emulator` app config or
  `BIGTABLE_EMULATOR_HOST`), or when the Data API pool is configured under the
  `GrpcConnectionPool` key with a `type: :local` endpoint — a plaintext local
  endpoint is an emulator by definition, and must not receive auth metadata.
  """
  @spec emulator?() :: boolean()
  def emulator? do
    emulator_endpoint() != nil or configured_local_endpoint() != nil
  end

  @doc """
  Returns the emulator configuration if set, nil otherwise.
  """
  @spec emulator_config() :: keyword() | nil
  def emulator_config do
    Application.get_env(:megas_pinakas, :emulator)
  end

  @doc """
  Returns the emulator host and port from config or environment variable.

  `BIGTABLE_EMULATOR_HOST` takes precedence over the `:emulator` app config and
  is parsed as `host`, `host:port`, `[ipv6]`, or `[ipv6]:port`; a missing port
  defaults to #{@default_emulator_port}. Raises `ArgumentError` when the value
  cannot be parsed, so a typo fails at boot rather than as a connection error.

  ## Examples

      BIGTABLE_EMULATOR_HOST=localhost:8086  # => {"localhost", 8086}
      BIGTABLE_EMULATOR_HOST=emulator        # => {"emulator", 8086}
      BIGTABLE_EMULATOR_HOST=[::1]:9000      # => {"::1", 9000}
  """
  @spec emulator_endpoint() :: {String.t(), :inet.port_number()} | nil
  def emulator_endpoint do
    case System.get_env("BIGTABLE_EMULATOR_HOST") do
      nil ->
        case emulator_config() do
          nil ->
            nil

          config ->
            host = Keyword.get(config, :host, @default_emulator_host)
            port = Keyword.get(config, :port, @default_emulator_port)
            {host, port}
        end

      value ->
        parse_host_port!(value)
    end
  end

  defp parse_host_port!(value) do
    case split_host_port(value) do
      {"", _port} ->
        raise ArgumentError, "BIGTABLE_EMULATOR_HOST has an empty host: #{inspect(value)}"

      {host, nil} ->
        {host, @default_emulator_port}

      {host, port} ->
        {host, parse_port!(value, port)}
    end
  end

  # `[::1]:8086` — bracketed IPv6 literal with optional port.
  defp split_host_port("[" <> rest) do
    case String.split(rest, "]", parts: 2) do
      [host, ""] -> {host, nil}
      [host, ":" <> port] -> {host, port}
      _ -> raise ArgumentError, "BIGTABLE_EMULATOR_HOST is malformed: #{inspect("[" <> rest)}"
    end
  end

  defp split_host_port(value) do
    case String.split(value, ":") do
      [host] ->
        {host, nil}

      [host, port] ->
        {host, port}

      _ ->
        # A bare IPv6 literal is ambiguous with `host:port`; require brackets.
        raise ArgumentError,
              "BIGTABLE_EMULATOR_HOST #{inspect(value)} has more than one ':'; " <>
                "write IPv6 literals as [addr]:port"
    end
  end

  defp parse_port!(value, port) do
    case Integer.parse(port) do
      {n, ""} when n in 1..65_535 ->
        n

      _ ->
        raise ArgumentError,
              "BIGTABLE_EMULATOR_HOST #{inspect(value)} has an invalid port #{inspect(port)}"
    end
  end

  # Host/port of the Data API pool when it is configured under the
  # `GrpcConnectionPool` key with a `type: :local` endpoint; nil otherwise.
  # Used to keep `emulator?/0` and the admin pool consistent with that pool.
  defp configured_local_endpoint do
    with config when is_list(config) <- Application.get_env(:megas_pinakas, GrpcConnectionPool),
         endpoint when is_list(endpoint) <- Keyword.get(config, :endpoint),
         :local <- Keyword.get(endpoint, :type) do
      {Keyword.get(endpoint, :host), Keyword.get(endpoint, :port)}
    else
      _ -> nil
    end
  end

  # Connection Pool Configuration

  @doc """
  Builds the Data API connection pool configuration based on environment.

  When `config :megas_pinakas, GrpcConnectionPool, ...` is present it is used
  verbatim (see `GrpcConnectionPool.Config.from_env/2`), except that the pool
  name is always `MegasPinakas.ConnectionPool`: `MegasPinakas.Client` looks the
  pool up by that name, so a custom `pool.name` would start a pool nothing can
  find. Otherwise the pool is derived from `emulator_endpoint/0`, falling back
  to the production Data API host.

  Raises `ArgumentError` when the `GrpcConnectionPool` config is present but
  invalid; a misconfigured pool must not silently degrade to the defaults.
  """
  @spec build_pool_config() :: GrpcConnectionPool.Config.t()
  def build_pool_config do
    if Application.get_env(:megas_pinakas, GrpcConnectionPool) == nil do
      build_endpoint_config(@production_host, MegasPinakas.ConnectionPool)
    else
      case GrpcConnectionPool.Config.from_env(:megas_pinakas) do
        {:ok, %{pool: pool} = config} ->
          %{config | pool: %{pool | name: MegasPinakas.ConnectionPool}}

        {:error, message} ->
          raise ArgumentError, "invalid :megas_pinakas GrpcConnectionPool config: #{message}"
      end
    end
  end

  @doc """
  Builds the Admin API connection pool configuration.

  Google serves `BigtableTableAdmin` and `BigtableInstanceAdmin` from
  `#{@admin_host}`, not from the Data API host; the data host answers
  admin RPCs with `UNIMPLEMENTED`. The emulator serves both on one port, so in
  emulator mode (including a `type: :local` `GrpcConnectionPool` endpoint) this
  is the same endpoint as `build_pool_config/0`.
  """
  @spec build_admin_pool_config() :: GrpcConnectionPool.Config.t()
  def build_admin_pool_config do
    build_endpoint_config(@admin_host, MegasPinakas.AdminConnectionPool)
  end

  defp build_endpoint_config(production_host, pool_name) do
    pool_size = Application.get_env(:megas_pinakas, :default_pool_size, @default_pool_size)

    case emulator_endpoint() || configured_local_endpoint() do
      nil ->
        {:ok, config} =
          GrpcConnectionPool.Config.production(
            host: production_host,
            port: @production_port,
            pool_name: pool_name,
            pool_size: pool_size
          )

        config

      {host, port} ->
        {:ok, config} =
          GrpcConnectionPool.Config.local(
            host: host,
            port: port,
            pool_name: pool_name,
            pool_size: pool_size
          )

        config
    end
  end

  @doc """
  Returns the default pool size.
  """
  @spec default_pool_size() :: integer()
  def default_pool_size do
    Application.get_env(:megas_pinakas, :default_pool_size, @default_pool_size)
  end

  @doc """
  Returns the production Data API endpoint.
  """
  @spec production_endpoint() :: {String.t(), integer()}
  def production_endpoint do
    {@production_host, @production_port}
  end

  @doc """
  Returns the production Admin API endpoint.
  """
  @spec admin_endpoint() :: {String.t(), integer()}
  def admin_endpoint do
    {@admin_host, @production_port}
  end

  @doc """
  Returns the default gRPC timeout in milliseconds.

  Configurable via `:default_timeout` in the `:megas_pinakas` application config.
  Defaults to #{@default_timeout}ms.
  """
  @spec default_timeout() :: non_neg_integer()
  def default_timeout do
    Application.get_env(:megas_pinakas, :default_timeout, @default_timeout)
  end
end
