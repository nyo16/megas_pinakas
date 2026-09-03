defmodule MegasPinakas.InstanceAdmin do
  @moduledoc """
  Instance and cluster administration operations for BigTable.

  This module provides functions for creating, modifying, and deleting instances,
  clusters, and app profiles.

  Instance admin RPCs are served by `bigtableadmin.googleapis.com`, not the Data
  API host, so every function here runs on `MegasPinakas.Client.admin_pool/0`
  (in emulator mode both pools point at the emulator).

  Functions documented as returning a long-running operation hand back a
  `Google.Longrunning.Operation`; resolve it with
  `MegasPinakas.Admin.wait_operation/2`.
  """

  alias MegasPinakas.{Auth, Client, Config}

  alias Google.Bigtable.Admin.V2.{
    AppProfile,
    AutoscalingLimits,
    AutoscalingTargets,
    BigtableInstanceAdmin.Stub,
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
    PartialUpdateClusterRequest,
    PartialUpdateInstanceRequest,
    UpdateAppProfileRequest
  }

  # ============================================================================
  # Instance Operations
  # ============================================================================

  @doc """
  Creates a new BigTable instance.

  Returns a long-running operation; resolve it with
  `MegasPinakas.Admin.wait_operation/2` to get the created
  `Google.Bigtable.Admin.V2.Instance`.

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
      {:ok, instance} = MegasPinakas.Admin.wait_operation(operation)
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

    Client.execute(operation, pool: Client.admin_pool())
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

    Client.execute(operation, pool: Client.admin_pool())
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

    Client.execute(operation, pool: Client.admin_pool())
  end

  @doc """
  Partially updates a BigTable instance.

  Only the options given are written; everything else on the instance is left
  untouched. Returns a long-running operation; resolve it with
  `MegasPinakas.Admin.wait_operation/2` to get the updated
  `Google.Bigtable.Admin.V2.Instance`.

  ## Options

    * `:display_name` - New display name
    * `:type` - New instance type
    * `:labels` - New labels

  ## Examples

      {:ok, operation} = MegasPinakas.InstanceAdmin.partial_update_instance(
        "project", "my-instance",
        display_name: "New Name")
      {:ok, instance} = MegasPinakas.Admin.wait_operation(operation)
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

    Client.execute(operation, pool: Client.admin_pool())
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

    Client.execute(operation, pool: Client.admin_pool())
  end

  # ============================================================================
  # Cluster Operations
  # ============================================================================

  @doc """
  Creates a new cluster in an instance.

  Returns a long-running operation; resolve it with
  `MegasPinakas.Admin.wait_operation/2` to get the created
  `Google.Bigtable.Admin.V2.Cluster`.

  ## Options

    * `:serve_nodes` - Number of nodes to serve (default: 3)
    * `:storage_type` - Storage type (`:SSD` or `:HDD`, default: `:SSD`)

  ## Examples

      {:ok, operation} = MegasPinakas.InstanceAdmin.create_cluster(
        "project", "instance", "new-cluster", "us-east1-b",
        serve_nodes: 3,
        storage_type: :SSD)
      {:ok, cluster} = MegasPinakas.Admin.wait_operation(operation)
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

    Client.execute(operation, pool: Client.admin_pool())
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

    Client.execute(operation, pool: Client.admin_pool())
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

    Client.execute(operation, pool: Client.admin_pool())
  end

  @doc """
  Replaces a cluster's node count.

  `UpdateCluster` is a full-replace RPC: whatever is sent becomes the cluster's
  configuration, so `:serve_nodes` is required and the call raises
  `ArgumentError` without it (omitting it would silently request 0 nodes). To
  change a single field, or to switch to autoscaling, use
  `partial_update_cluster/4`.

  Returns a long-running operation; resolve it with
  `MegasPinakas.Admin.wait_operation/2` to get the updated
  `Google.Bigtable.Admin.V2.Cluster`.

  ## Options

    * `:serve_nodes` - New number of serve nodes (required, positive integer)

  ## Examples

      {:ok, operation} = MegasPinakas.InstanceAdmin.update_cluster(
        "project", "instance", "cluster",
        serve_nodes: 5)
      {:ok, cluster} = MegasPinakas.Admin.wait_operation(operation)
  """
  @spec update_cluster(String.t(), String.t(), String.t(), keyword()) ::
          {:ok, Google.Longrunning.Operation.t()} | {:error, term()}
  def update_cluster(project_id, instance_id, cluster_id, opts \\ []) do
    serve_nodes = fetch_serve_nodes!(opts)

    operation = fn channel ->
      # UpdateCluster RPC takes a Cluster directly
      cluster = %Cluster{
        name: Config.cluster_path(project_id, instance_id, cluster_id),
        serve_nodes: serve_nodes
      }

      auth_opts = Auth.request_opts()
      Stub.update_cluster(channel, cluster, auth_opts)
    end

    Client.execute(operation, pool: Client.admin_pool())
  end

  @doc """
  Updates only the given fields of a cluster.

  Unlike `update_cluster/4`, fields not mentioned keep their current values.
  At least one option is required; giving none raises `ArgumentError`. Setting
  `:serve_nodes` on an autoscaled cluster switches it to manual scaling, and
  setting `:autoscaling` on a manually scaled cluster enables autoscaling;
  giving both raises `ArgumentError`.

  Returns a long-running operation; resolve it with
  `MegasPinakas.Admin.wait_operation/2` to get the updated
  `Google.Bigtable.Admin.V2.Cluster`.

  ## Options

    * `:serve_nodes` - New number of serve nodes (positive integer)
    * `:autoscaling` - Map with `:min_serve_nodes`, `:max_serve_nodes`,
      `:cpu_utilization_percent`, and optional `:storage_utilization_gib_per_node`
      (string keys are accepted too)

  ## Examples

      {:ok, operation} = MegasPinakas.InstanceAdmin.partial_update_cluster(
        "project", "instance", "cluster",
        autoscaling: %{min_serve_nodes: 1, max_serve_nodes: 5, cpu_utilization_percent: 60})
      {:ok, cluster} = MegasPinakas.Admin.wait_operation(operation)

  > #### Emulator {: .warning}
  >
  > The BigTable emulator does not implement `PartialUpdateCluster` (it crashes
  > on the call); only use this against real BigTable.
  """
  @spec partial_update_cluster(String.t(), String.t(), String.t(), keyword()) ::
          {:ok, Google.Longrunning.Operation.t()} | {:error, term()}
  def partial_update_cluster(project_id, instance_id, cluster_id, opts) do
    request = partial_update_cluster_request(project_id, instance_id, cluster_id, opts)

    operation = fn channel ->
      auth_opts = Auth.request_opts()
      Stub.partial_update_cluster(channel, request, auth_opts)
    end

    Client.execute(operation, pool: Client.admin_pool())
  end

  # Public so the mask/body agreement can be tested without a server; the
  # emulator crashes on PartialUpdateCluster.
  @doc false
  @spec partial_update_cluster_request(String.t(), String.t(), String.t(), keyword()) ::
          PartialUpdateClusterRequest.t()
  def partial_update_cluster_request(project_id, instance_id, cluster_id, opts) do
    {cluster_fields, paths} = partial_cluster_update!(opts)

    cluster =
      struct!(Cluster, [
        {:name, Config.cluster_path(project_id, instance_id, cluster_id)} | cluster_fields
      ])

    %PartialUpdateClusterRequest{
      cluster: cluster,
      update_mask: %Google.Protobuf.FieldMask{paths: paths}
    }
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

    Client.execute(operation, pool: Client.admin_pool())
  end

  # ============================================================================
  # App Profile Operations
  # ============================================================================

  @doc """
  Creates a new app profile.

  Routing defaults to multi-cluster when neither routing option is given. Giving
  both, or `multi_cluster_routing: false`, raises `ArgumentError` (to route to
  a single cluster, pass `:single_cluster_routing` instead).

  ## Options

    * `:description` - Description of the app profile
    * `:multi_cluster_routing` - `true` to route to any cluster
    * `:single_cluster_routing` - Map with `:cluster_id` (required) and
      `:allow_transactional_writes` (default `false`); string keys are accepted
    * `:ignore_warnings` - Ignore warnings (default: false)

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
    request = create_app_profile_request(project_id, instance_id, app_profile_id, opts)

    operation = fn channel ->
      auth_opts = Auth.request_opts()
      Stub.create_app_profile(channel, request, auth_opts)
    end

    Client.execute(operation, pool: Client.admin_pool())
  end

  @doc false
  @spec create_app_profile_request(String.t(), String.t(), String.t(), keyword()) ::
          CreateAppProfileRequest.t()
  def create_app_profile_request(project_id, instance_id, app_profile_id, opts) do
    routing_policy =
      case routing_policy!(opts) do
        nil -> {:multi_cluster_routing_use_any, %AppProfile.MultiClusterRoutingUseAny{}}
        {policy, _path} -> policy
      end

    app_profile = %AppProfile{
      description: Keyword.get(opts, :description, ""),
      routing_policy: routing_policy
    }

    %CreateAppProfileRequest{
      parent: Config.instance_path(project_id, instance_id),
      app_profile_id: app_profile_id,
      app_profile: app_profile,
      ignore_warnings: Keyword.get(opts, :ignore_warnings, false)
    }
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

    Client.execute(operation, pool: Client.admin_pool())
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

    Client.execute(operation, pool: Client.admin_pool())
  end

  @doc """
  Updates an app profile.

  Only the options given are written: the update mask and the request body are
  built from the same keys, so omitted fields keep their current values. At
  least one of `:description`, `:multi_cluster_routing`, or
  `:single_cluster_routing` is required. Giving both routing options, or
  `multi_cluster_routing: false`, raises `ArgumentError` (to route to a single
  cluster, pass `:single_cluster_routing` instead).

  Returns a long-running operation; resolve it with
  `MegasPinakas.Admin.wait_operation/2` to get the updated
  `Google.Bigtable.Admin.V2.AppProfile`.

  ## Options

    * `:description` - New description
    * `:multi_cluster_routing` - `true` to route to any cluster
    * `:single_cluster_routing` - Map with `:cluster_id` (required) and
      `:allow_transactional_writes` (default `false`); string keys are accepted
    * `:ignore_warnings` - Ignore warnings (default: false)

  ## Examples

      {:ok, operation} = MegasPinakas.InstanceAdmin.update_app_profile(
        "project", "instance", "profile",
        description: "Updated description")
      {:ok, profile} = MegasPinakas.Admin.wait_operation(operation)
  """
  @spec update_app_profile(String.t(), String.t(), String.t(), keyword()) ::
          {:ok, Google.Longrunning.Operation.t()} | {:error, term()}
  def update_app_profile(project_id, instance_id, app_profile_id, opts \\ []) do
    request = update_app_profile_request(project_id, instance_id, app_profile_id, opts)

    operation = fn channel ->
      auth_opts = Auth.request_opts()
      Stub.update_app_profile(channel, request, auth_opts)
    end

    Client.execute(operation, pool: Client.admin_pool())
  end

  # Public so the mask/body agreement can be tested without a server; the
  # emulator does not implement UpdateAppProfile.
  @doc false
  @spec update_app_profile_request(String.t(), String.t(), String.t(), keyword()) ::
          UpdateAppProfileRequest.t()
  def update_app_profile_request(project_id, instance_id, app_profile_id, opts) do
    {routing_policy, paths} =
      case routing_policy!(opts) do
        nil -> {nil, []}
        {policy, path} -> {policy, [path]}
      end

    paths = maybe_add_path(paths, opts, :description, "description")

    if paths == [] do
      raise ArgumentError,
            "update_app_profile/4 needs at least one of :description, " <>
              ":multi_cluster_routing, or :single_cluster_routing"
    end

    app_profile = %AppProfile{
      name: Config.app_profile_path(project_id, instance_id, app_profile_id),
      description: Keyword.get(opts, :description, ""),
      routing_policy: routing_policy
    }

    %UpdateAppProfileRequest{
      app_profile: app_profile,
      update_mask: %Google.Protobuf.FieldMask{paths: paths},
      ignore_warnings: Keyword.get(opts, :ignore_warnings, false)
    }
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

    Client.execute(operation, pool: Client.admin_pool())
  end

  # ============================================================================
  # Private Helpers
  # ============================================================================

  # Argument shape is checked in these helpers, before Client.execute/2, so a
  # bad call raises ArgumentError instead of surfacing as
  # {:error, {:execution_error, _}} from inside the operation closure.

  defp maybe_add_path(paths, opts, key, path_name) do
    if Keyword.has_key?(opts, key) do
      [path_name | paths]
    else
      paths
    end
  end

  defp fetch_serve_nodes!(opts) do
    case Keyword.fetch(opts, :serve_nodes) do
      {:ok, n} when is_integer(n) and n > 0 ->
        n

      {:ok, other} ->
        raise ArgumentError, ":serve_nodes must be a positive integer, got: #{inspect(other)}"

      :error ->
        raise ArgumentError,
              "update_cluster/4 requires :serve_nodes because UpdateCluster replaces the whole " <>
                "cluster; use partial_update_cluster/4 to change other fields"
    end
  end

  # Returns {cluster struct fields, field mask paths} for PartialUpdateCluster.
  defp partial_cluster_update!(opts) do
    has_serve_nodes = Keyword.has_key?(opts, :serve_nodes)
    has_autoscaling = Keyword.has_key?(opts, :autoscaling)

    cond do
      has_serve_nodes and has_autoscaling ->
        raise ArgumentError,
              ":serve_nodes and :autoscaling are mutually exclusive; a cluster is either " <>
                "manually scaled or autoscaled"

      has_serve_nodes ->
        # Disabling autoscaling requires clearing cluster_autoscaling_config AND
        # setting serve_nodes in the same mask; `config: nil` in the body plus
        # this path is what clears it. A mask of just "serve_nodes" is rejected
        # on an autoscaled cluster.
        {[serve_nodes: fetch_serve_nodes!(opts)],
         ["serve_nodes", "cluster_config.cluster_autoscaling_config"]}

      has_autoscaling ->
        config = %Cluster.ClusterConfig{
          cluster_autoscaling_config: autoscaling_config!(Keyword.fetch!(opts, :autoscaling))
        }

        {[config: {:cluster_config, config}], ["cluster_config.cluster_autoscaling_config"]}

      true ->
        raise ArgumentError,
              "partial_update_cluster/4 needs at least one of :serve_nodes or :autoscaling"
    end
  end

  defp autoscaling_config!(config) when is_map(config) do
    min_nodes = fetch_positive_int!(config, :min_serve_nodes)
    max_nodes = fetch_positive_int!(config, :max_serve_nodes)
    cpu = fetch_positive_int!(config, :cpu_utilization_percent)

    storage =
      config[:storage_utilization_gib_per_node] || config["storage_utilization_gib_per_node"]

    if max_nodes < min_nodes do
      raise ArgumentError,
            ":autoscaling max_serve_nodes (#{max_nodes}) must be >= min_serve_nodes (#{min_nodes})"
    end

    unless is_nil(storage) or (is_integer(storage) and storage > 0) do
      raise ArgumentError,
            ":autoscaling storage_utilization_gib_per_node must be a positive integer, " <>
              "got: #{inspect(storage)}"
    end

    %Cluster.ClusterAutoscalingConfig{
      autoscaling_limits: %AutoscalingLimits{
        min_serve_nodes: min_nodes,
        max_serve_nodes: max_nodes
      },
      autoscaling_targets: %AutoscalingTargets{
        cpu_utilization_percent: cpu,
        storage_utilization_gib_per_node: storage || 0
      }
    }
  end

  defp autoscaling_config!(other) do
    raise ArgumentError,
          ":autoscaling must be a map with :min_serve_nodes, :max_serve_nodes, and " <>
            ":cpu_utilization_percent, got: #{inspect(other)}"
  end

  defp fetch_positive_int!(config, key) do
    case config[key] || config[Atom.to_string(key)] do
      n when is_integer(n) and n > 0 ->
        n

      other ->
        raise ArgumentError,
              ":autoscaling #{key} must be a positive integer, got: #{inspect(other)}"
    end
  end

  # Returns {routing_policy oneof, field mask path} for the routing option
  # given, or nil when neither routing option is present.
  defp routing_policy!(opts) do
    multi = Keyword.fetch(opts, :multi_cluster_routing)
    single = Keyword.fetch(opts, :single_cluster_routing)

    case {multi, single} do
      {{:ok, _}, {:ok, _}} ->
        raise ArgumentError,
              ":multi_cluster_routing and :single_cluster_routing are mutually exclusive"

      {{:ok, true}, :error} ->
        {{:multi_cluster_routing_use_any, %AppProfile.MultiClusterRoutingUseAny{}},
         "multi_cluster_routing_use_any"}

      {{:ok, other}, :error} ->
        raise ArgumentError,
              ":multi_cluster_routing must be true (pass :single_cluster_routing to route " <>
                "to one cluster), got: #{inspect(other)}"

      {:error, {:ok, config}} ->
        {{:single_cluster_routing, single_cluster_routing!(config)}, "single_cluster_routing"}

      {:error, :error} ->
        nil
    end
  end

  defp single_cluster_routing!(config) when is_map(config) do
    cluster_id = config[:cluster_id] || config["cluster_id"]

    unless is_binary(cluster_id) and cluster_id != "" do
      raise ArgumentError,
            ":single_cluster_routing requires a non-empty :cluster_id, got: #{inspect(config)}"
    end

    allow_writes =
      case {Map.fetch(config, :allow_transactional_writes),
            Map.fetch(config, "allow_transactional_writes")} do
        {{:ok, value}, _} -> value
        {:error, {:ok, value}} -> value
        {:error, :error} -> false
      end

    unless is_boolean(allow_writes) do
      raise ArgumentError,
            ":single_cluster_routing allow_transactional_writes must be a boolean, " <>
              "got: #{inspect(allow_writes)}"
    end

    %AppProfile.SingleClusterRouting{
      cluster_id: cluster_id,
      allow_transactional_writes: allow_writes
    }
  end

  defp single_cluster_routing!(other) do
    raise ArgumentError,
          ":single_cluster_routing must be a map with :cluster_id, got: #{inspect(other)}"
  end
end
