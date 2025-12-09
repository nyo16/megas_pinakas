defmodule MegasPinakas.InstanceAdmin do
  @moduledoc """
  Instance and cluster administration operations for BigTable.

  This module provides functions for creating, modifying, and deleting instances,
  clusters, and app profiles.
  """

  alias MegasPinakas.{Auth, Client, Config}

  alias Google.Bigtable.Admin.V2.{
    BigtableInstanceAdmin.Stub,
    AppProfile,
    Cluster,
    CreateAppProfileRequest,
    CreateClusterRequest,
    CreateInstanceRequest,
    DeleteAppProfileRequest,
    DeleteClusterRequest,
    DeleteInstanceRequest,
    GetAppProfileRequest,
    GetClusterRequest,
    GetInstanceRequest,
    Instance,
    ListAppProfilesRequest,
    ListAppProfilesResponse,
    ListClustersRequest,
    ListClustersResponse,
    ListInstancesRequest,
    ListInstancesResponse,
    PartialUpdateInstanceRequest,
    UpdateAppProfileRequest
  }

  # ============================================================================
  # Instance Operations
  # ============================================================================

  @doc """
  Creates a new BigTable instance.

  Returns a long-running operation that can be monitored.

  ## Options

    * `:display_name` - Human-readable name for the instance
    * `:type` - Instance type (`:PRODUCTION` or `:DEVELOPMENT`)
    * `:labels` - Map of labels for the instance

  ## Examples

      clusters = %{
        "my-cluster" => %{
          location: "us-central1-b",
          serve_nodes: 3,
          storage_type: :SSD
        }
      }
      {:ok, operation} = MegasPinakas.InstanceAdmin.create_instance(
        "project", "my-instance", clusters,
        display_name: "My Instance",
        type: :PRODUCTION)
  """
  @spec create_instance(String.t(), String.t(), map(), keyword()) ::
          {:ok, Google.Longrunning.Operation.t()} | {:error, term()}
  def create_instance(project_id, instance_id, clusters_config, opts \\ []) do
    operation = fn channel ->
      instance = %Instance{
        display_name: Keyword.get(opts, :display_name, instance_id),
        type: Keyword.get(opts, :type, :PRODUCTION),
        labels: Keyword.get(opts, :labels, %{})
      }

      clusters =
        Enum.map(clusters_config, fn {cluster_id, config} ->
          location =
            Config.location_path(project_id, config[:location] || config["location"])

          cluster = %Cluster{
            location: location,
            serve_nodes: config[:serve_nodes] || config["serve_nodes"] || 3,
            default_storage_type: config[:storage_type] || config["storage_type"] || :SSD
          }

          {cluster_id, cluster}
        end)
        |> Map.new()

      request = %CreateInstanceRequest{
        parent: Config.project_path(project_id),
        instance_id: instance_id,
        instance: instance,
        clusters: clusters
      }

      auth_opts = Auth.request_opts()
      Stub.create_instance(channel, request, auth_opts)
    end

    Client.execute(operation)
  end

  @doc """
  Gets details about a BigTable instance.

  ## Examples

      {:ok, instance} = MegasPinakas.InstanceAdmin.get_instance("project", "my-instance")
  """
  @spec get_instance(String.t(), String.t()) :: {:ok, Instance.t()} | {:error, term()}
  def get_instance(project_id, instance_id) do
    operation = fn channel ->
      request = %GetInstanceRequest{
        name: Config.instance_path(project_id, instance_id)
      }

      auth_opts = Auth.request_opts()
      Stub.get_instance(channel, request, auth_opts)
    end

    Client.execute(operation)
  end

  @doc """
  Lists instances in a project.

  ## Options

    * `:page_token` - Page token for pagination

  ## Examples

      {:ok, response} = MegasPinakas.InstanceAdmin.list_instances("project")
  """
  @spec list_instances(String.t(), keyword()) ::
          {:ok, ListInstancesResponse.t()} | {:error, term()}
  def list_instances(project_id, opts \\ []) do
    operation = fn channel ->
      request = %ListInstancesRequest{
        parent: Config.project_path(project_id),
        page_token: Keyword.get(opts, :page_token, "")
      }

      auth_opts = Auth.request_opts()
      Stub.list_instances(channel, request, auth_opts)
    end

    Client.execute(operation)
  end

  @doc """
  Partially updates a BigTable instance.

  Returns a long-running operation that can be monitored.

  ## Options

    * `:display_name` - New display name
    * `:type` - New instance type
    * `:labels` - New labels

  ## Examples

      {:ok, operation} = MegasPinakas.InstanceAdmin.partial_update_instance(
        "project", "my-instance",
        display_name: "New Name")
  """
  @spec partial_update_instance(String.t(), String.t(), keyword()) ::
          {:ok, Google.Longrunning.Operation.t()} | {:error, term()}
  def partial_update_instance(project_id, instance_id, opts \\ []) do
    operation = fn channel ->
      instance = %Instance{
        name: Config.instance_path(project_id, instance_id),
        display_name: Keyword.get(opts, :display_name),
        type: Keyword.get(opts, :type),
        labels: Keyword.get(opts, :labels)
      }

      # Build update mask paths
      paths =
        []
        |> maybe_add_path(opts, :display_name, "display_name")
        |> maybe_add_path(opts, :type, "type")
        |> maybe_add_path(opts, :labels, "labels")

      update_mask = %Google.Protobuf.FieldMask{paths: paths}

      request = %PartialUpdateInstanceRequest{
        instance: instance,
        update_mask: update_mask
      }

      auth_opts = Auth.request_opts()
      Stub.partial_update_instance(channel, request, auth_opts)
    end

    Client.execute(operation)
  end

  @doc """
  Deletes a BigTable instance.

  ## Examples

      {:ok, _} = MegasPinakas.InstanceAdmin.delete_instance("project", "my-instance")
  """
  @spec delete_instance(String.t(), String.t()) ::
          {:ok, Google.Protobuf.Empty.t()} | {:error, term()}
  def delete_instance(project_id, instance_id) do
    operation = fn channel ->
      request = %DeleteInstanceRequest{
        name: Config.instance_path(project_id, instance_id)
      }

      auth_opts = Auth.request_opts()
      Stub.delete_instance(channel, request, auth_opts)
    end

    Client.execute(operation)
  end

  # ============================================================================
  # Cluster Operations
  # ============================================================================

  @doc """
  Creates a new cluster in an instance.

  Returns a long-running operation that can be monitored.

  ## Options

    * `:serve_nodes` - Number of nodes to serve (default: 3)
    * `:storage_type` - Storage type (`:SSD` or `:HDD`, default: `:SSD`)

  ## Examples

      {:ok, operation} = MegasPinakas.InstanceAdmin.create_cluster(
        "project", "instance", "new-cluster", "us-east1-b",
        serve_nodes: 3,
        storage_type: :SSD)
  """
  @spec create_cluster(String.t(), String.t(), String.t(), String.t(), keyword()) ::
          {:ok, Google.Longrunning.Operation.t()} | {:error, term()}
  def create_cluster(project_id, instance_id, cluster_id, location, opts \\ []) do
    operation = fn channel ->
      cluster = %Cluster{
        location: Config.location_path(project_id, location),
        serve_nodes: Keyword.get(opts, :serve_nodes, 3),
        default_storage_type: Keyword.get(opts, :storage_type, :SSD)
      }

      request = %CreateClusterRequest{
        parent: Config.instance_path(project_id, instance_id),
        cluster_id: cluster_id,
        cluster: cluster
      }

      auth_opts = Auth.request_opts()
      Stub.create_cluster(channel, request, auth_opts)
    end

    Client.execute(operation)
  end

  @doc """
  Gets details about a cluster.

  ## Examples

      {:ok, cluster} = MegasPinakas.InstanceAdmin.get_cluster("project", "instance", "cluster")
  """
  @spec get_cluster(String.t(), String.t(), String.t()) ::
          {:ok, Cluster.t()} | {:error, term()}
  def get_cluster(project_id, instance_id, cluster_id) do
    operation = fn channel ->
      request = %GetClusterRequest{
        name: Config.cluster_path(project_id, instance_id, cluster_id)
      }

      auth_opts = Auth.request_opts()
      Stub.get_cluster(channel, request, auth_opts)
    end

    Client.execute(operation)
  end

  @doc """
  Lists clusters in an instance.

  ## Options

    * `:page_token` - Page token for pagination

  ## Examples

      {:ok, response} = MegasPinakas.InstanceAdmin.list_clusters("project", "instance")
  """
  @spec list_clusters(String.t(), String.t(), keyword()) ::
          {:ok, ListClustersResponse.t()} | {:error, term()}
  def list_clusters(project_id, instance_id, opts \\ []) do
    operation = fn channel ->
      request = %ListClustersRequest{
        parent: Config.instance_path(project_id, instance_id),
        page_token: Keyword.get(opts, :page_token, "")
      }

      auth_opts = Auth.request_opts()
      Stub.list_clusters(channel, request, auth_opts)
    end

    Client.execute(operation)
  end

  @doc """
  Updates a cluster.

  Returns a long-running operation that can be monitored.

  ## Options

    * `:serve_nodes` - New number of serve nodes

  ## Examples

      {:ok, operation} = MegasPinakas.InstanceAdmin.update_cluster(
        "project", "instance", "cluster",
        serve_nodes: 5)
  """
  @spec update_cluster(String.t(), String.t(), String.t(), keyword()) ::
          {:ok, Google.Longrunning.Operation.t()} | {:error, term()}
  def update_cluster(project_id, instance_id, cluster_id, opts \\ []) do
    operation = fn channel ->
      # UpdateCluster RPC takes a Cluster directly
      cluster = %Cluster{
        name: Config.cluster_path(project_id, instance_id, cluster_id),
        serve_nodes: Keyword.get(opts, :serve_nodes, 0)
      }

      auth_opts = Auth.request_opts()
      Stub.update_cluster(channel, cluster, auth_opts)
    end

    Client.execute(operation)
  end

  @doc """
  Deletes a cluster.

  ## Examples

      {:ok, _} = MegasPinakas.InstanceAdmin.delete_cluster("project", "instance", "cluster")
  """
  @spec delete_cluster(String.t(), String.t(), String.t()) ::
          {:ok, Google.Protobuf.Empty.t()} | {:error, term()}
  def delete_cluster(project_id, instance_id, cluster_id) do
    operation = fn channel ->
      request = %DeleteClusterRequest{
        name: Config.cluster_path(project_id, instance_id, cluster_id)
      }

      auth_opts = Auth.request_opts()
      Stub.delete_cluster(channel, request, auth_opts)
    end

    Client.execute(operation)
  end

  # ============================================================================
  # App Profile Operations
  # ============================================================================

  @doc """
  Creates a new app profile.

  ## Options

    * `:description` - Description of the app profile
    * `:multi_cluster_routing` - Enable multi-cluster routing (boolean)
    * `:single_cluster_routing` - Single cluster routing config map with `:cluster_id` and `:allow_transactional_writes`

  ## Examples

      # Multi-cluster routing
      {:ok, profile} = MegasPinakas.InstanceAdmin.create_app_profile(
        "project", "instance", "profile-id",
        description: "My profile",
        multi_cluster_routing: true)

      # Single cluster routing
      {:ok, profile} = MegasPinakas.InstanceAdmin.create_app_profile(
        "project", "instance", "profile-id",
        single_cluster_routing: %{
          cluster_id: "my-cluster",
          allow_transactional_writes: true
        })
  """
  @spec create_app_profile(String.t(), String.t(), String.t(), keyword()) ::
          {:ok, AppProfile.t()} | {:error, term()}
  def create_app_profile(project_id, instance_id, app_profile_id, opts \\ []) do
    operation = fn channel ->
      routing_policy =
        cond do
          Keyword.get(opts, :multi_cluster_routing) ->
            {:multi_cluster_routing_use_any, %AppProfile.MultiClusterRoutingUseAny{}}

          single_cluster = Keyword.get(opts, :single_cluster_routing) ->
            {:single_cluster_routing,
             %AppProfile.SingleClusterRouting{
               cluster_id: single_cluster[:cluster_id] || single_cluster["cluster_id"],
               allow_transactional_writes:
                 single_cluster[:allow_transactional_writes] ||
                   single_cluster["allow_transactional_writes"] || false
             }}

          true ->
            {:multi_cluster_routing_use_any, %AppProfile.MultiClusterRoutingUseAny{}}
        end

      app_profile = %AppProfile{
        description: Keyword.get(opts, :description, ""),
        routing_policy: routing_policy
      }

      request = %CreateAppProfileRequest{
        parent: Config.instance_path(project_id, instance_id),
        app_profile_id: app_profile_id,
        app_profile: app_profile,
        ignore_warnings: Keyword.get(opts, :ignore_warnings, false)
      }

      auth_opts = Auth.request_opts()
      Stub.create_app_profile(channel, request, auth_opts)
    end

    Client.execute(operation)
  end

  @doc """
  Gets details about an app profile.

  ## Examples

      {:ok, profile} = MegasPinakas.InstanceAdmin.get_app_profile("project", "instance", "profile")
  """
  @spec get_app_profile(String.t(), String.t(), String.t()) ::
          {:ok, AppProfile.t()} | {:error, term()}
  def get_app_profile(project_id, instance_id, app_profile_id) do
    operation = fn channel ->
      request = %GetAppProfileRequest{
        name: Config.app_profile_path(project_id, instance_id, app_profile_id)
      }

      auth_opts = Auth.request_opts()
      Stub.get_app_profile(channel, request, auth_opts)
    end

    Client.execute(operation)
  end

  @doc """
  Lists app profiles in an instance.

  ## Options

    * `:page_size` - Maximum number of profiles to return
    * `:page_token` - Page token for pagination

  ## Examples

      {:ok, response} = MegasPinakas.InstanceAdmin.list_app_profiles("project", "instance")
  """
  @spec list_app_profiles(String.t(), String.t(), keyword()) ::
          {:ok, ListAppProfilesResponse.t()} | {:error, term()}
  def list_app_profiles(project_id, instance_id, opts \\ []) do
    operation = fn channel ->
      request = %ListAppProfilesRequest{
        parent: Config.instance_path(project_id, instance_id),
        page_size: Keyword.get(opts, :page_size, 0),
        page_token: Keyword.get(opts, :page_token, "")
      }

      auth_opts = Auth.request_opts()
      Stub.list_app_profiles(channel, request, auth_opts)
    end

    Client.execute(operation)
  end

  @doc """
  Updates an app profile.

  Returns a long-running operation that can be monitored.

  ## Options

    * `:description` - New description
    * `:multi_cluster_routing` - Enable multi-cluster routing
    * `:single_cluster_routing` - Single cluster routing config
    * `:ignore_warnings` - Ignore warnings (default: false)

  ## Examples

      {:ok, operation} = MegasPinakas.InstanceAdmin.update_app_profile(
        "project", "instance", "profile",
        description: "Updated description")
  """
  @spec update_app_profile(String.t(), String.t(), String.t(), keyword()) ::
          {:ok, Google.Longrunning.Operation.t()} | {:error, term()}
  def update_app_profile(project_id, instance_id, app_profile_id, opts \\ []) do
    operation = fn channel ->
      routing_policy =
        cond do
          Keyword.get(opts, :multi_cluster_routing) ->
            {:multi_cluster_routing_use_any, %AppProfile.MultiClusterRoutingUseAny{}}

          single_cluster = Keyword.get(opts, :single_cluster_routing) ->
            {:single_cluster_routing,
             %AppProfile.SingleClusterRouting{
               cluster_id: single_cluster[:cluster_id] || single_cluster["cluster_id"],
               allow_transactional_writes:
                 single_cluster[:allow_transactional_writes] ||
                   single_cluster["allow_transactional_writes"] || false
             }}

          true ->
            nil
        end

      app_profile = %AppProfile{
        name: Config.app_profile_path(project_id, instance_id, app_profile_id),
        description: Keyword.get(opts, :description),
        routing_policy: routing_policy
      }

      # Build update mask
      paths =
        []
        |> maybe_add_path(opts, :description, "description")
        |> maybe_add_routing_path(opts)

      update_mask = %Google.Protobuf.FieldMask{paths: paths}

      request = %UpdateAppProfileRequest{
        app_profile: app_profile,
        update_mask: update_mask,
        ignore_warnings: Keyword.get(opts, :ignore_warnings, false)
      }

      auth_opts = Auth.request_opts()
      Stub.update_app_profile(channel, request, auth_opts)
    end

    Client.execute(operation)
  end

  @doc """
  Deletes an app profile.

  ## Options

    * `:ignore_warnings` - Ignore warnings (default: false)

  ## Examples

      {:ok, _} = MegasPinakas.InstanceAdmin.delete_app_profile("project", "instance", "profile")
  """
  @spec delete_app_profile(String.t(), String.t(), String.t(), keyword()) ::
          {:ok, Google.Protobuf.Empty.t()} | {:error, term()}
  def delete_app_profile(project_id, instance_id, app_profile_id, opts \\ []) do
    operation = fn channel ->
      request = %DeleteAppProfileRequest{
        name: Config.app_profile_path(project_id, instance_id, app_profile_id),
        ignore_warnings: Keyword.get(opts, :ignore_warnings, false)
      }

      auth_opts = Auth.request_opts()
      Stub.delete_app_profile(channel, request, auth_opts)
    end

    Client.execute(operation)
  end

  # ============================================================================
  # Private Helpers
  # ============================================================================

  defp maybe_add_path(paths, opts, key, path_name) do
    if Keyword.has_key?(opts, key) do
      [path_name | paths]
    else
      paths
    end
  end

  defp maybe_add_routing_path(paths, opts) do
    cond do
      Keyword.has_key?(opts, :multi_cluster_routing) ->
        ["multi_cluster_routing_use_any" | paths]

      Keyword.has_key?(opts, :single_cluster_routing) ->
        ["single_cluster_routing" | paths]

      true ->
        paths
    end
  end
end
