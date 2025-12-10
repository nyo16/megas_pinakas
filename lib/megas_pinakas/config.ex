defmodule MegasPinakas.Config do
  @moduledoc """
  Configuration helpers and resource path builders for BigTable operations.
  """

  @default_pool_size 5
  @default_emulator_host "localhost"
  @default_emulator_port 8086
  @production_host "bigtable.googleapis.com"
  @production_port 443

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
  """
  @spec emulator?() :: boolean()
  def emulator? do
    emulator_config() != nil or System.get_env("BIGTABLE_EMULATOR_HOST") != nil
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
  """
  @spec emulator_endpoint() :: {String.t(), integer()} | nil
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

      env_host ->
        case String.split(env_host, ":") do
          [host, port_str] ->
            {host, String.to_integer(port_str)}

          [host] ->
            {host, @default_emulator_port}
        end
    end
  end

  # Connection Pool Configuration

  @doc """
  Builds the connection pool configuration based on environment.
  """
  @spec build_pool_config() :: map()
  def build_pool_config do
    case GrpcConnectionPool.Config.from_env(:megas_pinakas) do
      {:ok, config} ->
        config

      {:error, _} ->
        build_legacy_config()
    end
  end

  defp build_legacy_config do
    pool_size = Application.get_env(:megas_pinakas, :default_pool_size, @default_pool_size)

    case emulator_endpoint() do
      nil ->
        # Production Google Cloud BigTable
        {:ok, config} =
          GrpcConnectionPool.Config.production(
            host: @production_host,
            port: @production_port,
            pool_name: MegasPinakas.ConnectionPool,
            pool_size: pool_size
          )

        config

      {host, port} ->
        # Local emulator
        {:ok, config} =
          GrpcConnectionPool.Config.local(
            host: host,
            port: port,
            pool_name: MegasPinakas.ConnectionPool,
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
  Returns the production BigTable endpoint.
  """
  @spec production_endpoint() :: {String.t(), integer()}
  def production_endpoint do
    {@production_host, @production_port}
  end
end
