defmodule MegasPinakas.InstanceAdminTest do
  use ExUnit.Case, async: true

  alias MegasPinakas.InstanceAdmin

  describe "module structure" do
    test "exports instance operations" do
      functions = InstanceAdmin.__info__(:functions)

      assert {:create_instance, 3} in functions
      assert {:create_instance, 4} in functions
      assert {:get_instance, 2} in functions
      assert {:list_instances, 1} in functions
      assert {:list_instances, 2} in functions
      assert {:partial_update_instance, 2} in functions
      assert {:partial_update_instance, 3} in functions
      assert {:delete_instance, 2} in functions
    end

    test "exports cluster operations" do
      functions = InstanceAdmin.__info__(:functions)

      assert {:create_cluster, 4} in functions
      assert {:create_cluster, 5} in functions
      assert {:get_cluster, 3} in functions
      assert {:list_clusters, 2} in functions
      assert {:list_clusters, 3} in functions
      assert {:update_cluster, 3} in functions
      assert {:update_cluster, 4} in functions
      assert {:delete_cluster, 3} in functions
    end

    test "exports app profile operations" do
      functions = InstanceAdmin.__info__(:functions)

      assert {:create_app_profile, 3} in functions
      assert {:create_app_profile, 4} in functions
      assert {:get_app_profile, 3} in functions
      assert {:list_app_profiles, 2} in functions
      assert {:list_app_profiles, 3} in functions
      assert {:update_app_profile, 3} in functions
      assert {:update_app_profile, 4} in functions
      assert {:delete_app_profile, 3} in functions
      assert {:delete_app_profile, 4} in functions
    end
  end
end
