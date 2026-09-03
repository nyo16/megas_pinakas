defmodule MegasPinakas.InstanceAdminTest do
  use ExUnit.Case, async: true

  alias Google.Bigtable.Admin.V2.{
    AppProfile,
    AutoscalingLimits,
    AutoscalingTargets,
    Cluster,
    CreateAppProfileRequest,
    PartialUpdateClusterRequest,
    UpdateAppProfileRequest
  }

  alias MegasPinakas.{Config, InstanceAdmin}
  alias MegasPinakas.Test.Emulator

  @project Emulator.project()
  @instance Emulator.instance()
  @cluster "admin_cluster"
  @profile "admin_profile"

  # Every rejection below must happen before any RPC is attempted, so these
  # tests need neither the emulator nor a connected pool.

  describe "update_cluster/4 argument validation" do
    test "raises when :serve_nodes is missing (UpdateCluster is full-replace)" do
      assert_raise ArgumentError, ~r/requires :serve_nodes/, fn ->
        InstanceAdmin.update_cluster(@project, @instance, @cluster)
      end
    end

    test "raises when :serve_nodes is not a positive integer" do
      for bad <- [0, -1, "3", nil] do
        assert_raise ArgumentError, ~r/:serve_nodes must be a positive integer/, fn ->
          InstanceAdmin.update_cluster(@project, @instance, @cluster, serve_nodes: bad)
        end
      end
    end
  end

  describe "partial_update_cluster/4 argument validation" do
    test "raises when no updatable field is given" do
      assert_raise ArgumentError, ~r/needs at least one of :serve_nodes or :autoscaling/, fn ->
        InstanceAdmin.partial_update_cluster(@project, @instance, @cluster, [])
      end
    end

    test "raises when :serve_nodes and :autoscaling are both given" do
      assert_raise ArgumentError, ~r/mutually exclusive/, fn ->
        InstanceAdmin.partial_update_cluster(@project, @instance, @cluster,
          serve_nodes: 3,
          autoscaling: %{min_serve_nodes: 1, max_serve_nodes: 3, cpu_utilization_percent: 50}
        )
      end
    end

    test "raises on an incomplete or inconsistent :autoscaling map" do
      assert_raise ArgumentError, ~r/min_serve_nodes must be a positive integer/, fn ->
        InstanceAdmin.partial_update_cluster(@project, @instance, @cluster,
          autoscaling: %{max_serve_nodes: 3, cpu_utilization_percent: 50}
        )
      end

      assert_raise ArgumentError,
                   ~r/max_serve_nodes \(1\) must be >= min_serve_nodes \(3\)/,
                   fn ->
                     InstanceAdmin.partial_update_cluster(@project, @instance, @cluster,
                       autoscaling: %{
                         min_serve_nodes: 3,
                         max_serve_nodes: 1,
                         cpu_utilization_percent: 50
                       }
                     )
                   end

      assert_raise ArgumentError, ~r/:autoscaling must be a map/, fn ->
        InstanceAdmin.partial_update_cluster(@project, @instance, @cluster, autoscaling: true)
      end
    end
  end

  describe "app profile routing validation" do
    test "update_app_profile/4 raises when nothing would be updated" do
      assert_raise ArgumentError, ~r/needs at least one of/, fn ->
        InstanceAdmin.update_app_profile(@project, @instance, @profile)
      end

      assert_raise ArgumentError, ~r/needs at least one of/, fn ->
        InstanceAdmin.update_app_profile(@project, @instance, @profile, ignore_warnings: true)
      end
    end

    test "multi_cluster_routing: false is rejected instead of sending an empty routing policy" do
      assert_raise ArgumentError, ~r/:multi_cluster_routing must be true/, fn ->
        InstanceAdmin.update_app_profile(@project, @instance, @profile,
          multi_cluster_routing: false
        )
      end

      assert_raise ArgumentError, ~r/:multi_cluster_routing must be true/, fn ->
        InstanceAdmin.create_app_profile(@project, @instance, @profile,
          multi_cluster_routing: false
        )
      end
    end

    test "both routing options together are rejected" do
      assert_raise ArgumentError, ~r/mutually exclusive/, fn ->
        InstanceAdmin.create_app_profile(@project, @instance, @profile,
          multi_cluster_routing: true,
          single_cluster_routing: %{cluster_id: "c"}
        )
      end
    end

    test "single_cluster_routing requires a non-empty cluster_id and boolean writes flag" do
      assert_raise ArgumentError, ~r/requires a non-empty :cluster_id/, fn ->
        InstanceAdmin.create_app_profile(@project, @instance, @profile,
          single_cluster_routing: %{allow_transactional_writes: true}
        )
      end

      assert_raise ArgumentError, ~r/requires a non-empty :cluster_id/, fn ->
        InstanceAdmin.update_app_profile(@project, @instance, @profile,
          single_cluster_routing: %{"cluster_id" => ""}
        )
      end

      assert_raise ArgumentError, ~r/allow_transactional_writes must be a boolean/, fn ->
        InstanceAdmin.create_app_profile(@project, @instance, @profile,
          single_cluster_routing: %{cluster_id: "c", allow_transactional_writes: "yes"}
        )
      end

      assert_raise ArgumentError, ~r/:single_cluster_routing must be a map/, fn ->
        InstanceAdmin.create_app_profile(@project, @instance, @profile,
          single_cluster_routing: "c"
        )
      end
    end
  end

  describe "partial_update_cluster_request/4" do
    test "serve_nodes produces a manual-scaling body with a matching mask" do
      request =
        InstanceAdmin.partial_update_cluster_request(@project, @instance, @cluster,
          serve_nodes: 4
        )

      assert %PartialUpdateClusterRequest{
               cluster: %Cluster{name: cluster_name, serve_nodes: 4, config: nil},
               update_mask: %Google.Protobuf.FieldMask{
                 paths: ["serve_nodes", "cluster_config.cluster_autoscaling_config"]
               }
             } = request

      assert cluster_name == Config.cluster_path(@project, @instance, @cluster)
    end

    test "a string-keyed autoscaling map produces an autoscaling body with a matching mask" do
      request =
        InstanceAdmin.partial_update_cluster_request(@project, @instance, @cluster,
          autoscaling: %{
            "min_serve_nodes" => 1,
            "max_serve_nodes" => 4,
            "cpu_utilization_percent" => 70,
            "storage_utilization_gib_per_node" => 2_560
          }
        )

      assert %PartialUpdateClusterRequest{
               cluster: %Cluster{
                 serve_nodes: 0,
                 config:
                   {:cluster_config,
                    %Cluster.ClusterConfig{
                      cluster_autoscaling_config: %Cluster.ClusterAutoscalingConfig{
                        autoscaling_limits: %AutoscalingLimits{
                          min_serve_nodes: 1,
                          max_serve_nodes: 4
                        },
                        autoscaling_targets: %AutoscalingTargets{
                          cpu_utilization_percent: 70,
                          storage_utilization_gib_per_node: 2_560
                        }
                      }
                    }}
               },
               update_mask: %Google.Protobuf.FieldMask{
                 paths: ["cluster_config.cluster_autoscaling_config"]
               }
             } = request
    end

    test "storage utilization target is optional" do
      request =
        InstanceAdmin.partial_update_cluster_request(@project, @instance, @cluster,
          autoscaling: %{min_serve_nodes: 2, max_serve_nodes: 2, cpu_utilization_percent: 50}
        )

      {:cluster_config, %Cluster.ClusterConfig{cluster_autoscaling_config: autoscaling}} =
        request.cluster.config

      assert %AutoscalingTargets{cpu_utilization_percent: 50, storage_utilization_gib_per_node: 0} =
               autoscaling.autoscaling_targets
    end
  end

  describe "app profile request builders" do
    test "create defaults to multi-cluster routing" do
      request =
        InstanceAdmin.create_app_profile_request(@project, @instance, @profile, description: "d")

      assert %CreateAppProfileRequest{
               parent: parent,
               app_profile_id: @profile,
               ignore_warnings: false,
               app_profile: %AppProfile{
                 description: "d",
                 routing_policy:
                   {:multi_cluster_routing_use_any, %AppProfile.MultiClusterRoutingUseAny{}}
               }
             } = request

      assert parent == Config.instance_path(@project, @instance)
    end

    test "create accepts a string-keyed single-cluster routing map" do
      request =
        InstanceAdmin.create_app_profile_request(@project, @instance, @profile,
          single_cluster_routing: %{
            "cluster_id" => @cluster,
            "allow_transactional_writes" => true
          },
          ignore_warnings: true
        )

      assert %CreateAppProfileRequest{
               ignore_warnings: true,
               app_profile: %AppProfile{
                 routing_policy:
                   {:single_cluster_routing,
                    %AppProfile.SingleClusterRouting{
                      cluster_id: @cluster,
                      allow_transactional_writes: true
                    }}
               }
             } = request
    end

    test "single-cluster allow_transactional_writes defaults to false" do
      request =
        InstanceAdmin.create_app_profile_request(@project, @instance, @profile,
          single_cluster_routing: %{cluster_id: @cluster}
        )

      assert {:single_cluster_routing,
              %AppProfile.SingleClusterRouting{allow_transactional_writes: false}} =
               request.app_profile.routing_policy
    end

    test "update with only a description masks only the description and leaves routing unset" do
      request =
        InstanceAdmin.update_app_profile_request(@project, @instance, @profile,
          description: "only the description",
          ignore_warnings: true
        )

      assert %UpdateAppProfileRequest{
               ignore_warnings: true,
               update_mask: %Google.Protobuf.FieldMask{paths: ["description"]},
               app_profile: %AppProfile{
                 name: name,
                 description: "only the description",
                 routing_policy: nil
               }
             } = request

      assert name == Config.app_profile_path(@project, @instance, @profile)
    end

    test "update with multi-cluster routing masks the routing field and sets the body" do
      request =
        InstanceAdmin.update_app_profile_request(@project, @instance, @profile,
          multi_cluster_routing: true
        )

      assert %UpdateAppProfileRequest{
               update_mask: %Google.Protobuf.FieldMask{paths: ["multi_cluster_routing_use_any"]},
               app_profile: %AppProfile{
                 routing_policy:
                   {:multi_cluster_routing_use_any, %AppProfile.MultiClusterRoutingUseAny{}}
               }
             } = request
    end

    test "update with single-cluster routing and description masks both" do
      request =
        InstanceAdmin.update_app_profile_request(@project, @instance, @profile,
          description: "d",
          single_cluster_routing: %{cluster_id: @cluster, allow_transactional_writes: false}
        )

      assert Enum.sort(request.update_mask.paths) == ["description", "single_cluster_routing"]

      assert {:single_cluster_routing,
              %AppProfile.SingleClusterRouting{
                cluster_id: @cluster,
                allow_transactional_writes: false
              }} = request.app_profile.routing_policy

      assert request.app_profile.description == "d"
    end
  end

  describe "against the emulator" do
    @describetag :emulator

    setup do
      Emulator.await_pool!()
      :ok
    end

    # The emulator does not implement the instance admin service. The calls
    # below are the ones confirmed to fail cleanly there; each must still travel
    # the admin pool and come back as an :unimplemented status, proving the
    # request was built and sent rather than failing inside the closure.
    # (PartialUpdateCluster and others crash the emulator; never call them here.)

    test "instance and cluster reads reach the admin service" do
      assert {:error, {:unimplemented, _}} = InstanceAdmin.list_instances(@project)
      assert {:error, {:unimplemented, _}} = InstanceAdmin.get_instance(@project, @instance)
      assert {:error, {:unimplemented, _}} = InstanceAdmin.list_clusters(@project, @instance)
    end

    test "app profile calls reach the admin service" do
      assert {:error, {:unimplemented, _}} =
               InstanceAdmin.list_app_profiles(@project, @instance)

      assert {:error, {:unimplemented, _}} =
               InstanceAdmin.create_app_profile(@project, @instance, @profile,
                 description: "default multi-cluster"
               )
    end
  end
end
