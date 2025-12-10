defmodule MegasPinakas.AdminTest do
  use ExUnit.Case, async: true

  alias MegasPinakas.Admin
  alias Google.Bigtable.Admin.V2.{ColumnFamily, GcRule, ModifyColumnFamiliesRequest}

  describe "GC rule builders" do
    test "max_versions_gc_rule/1 creates a max versions rule" do
      rule = Admin.max_versions_gc_rule(3)

      assert %GcRule{rule: {:max_num_versions, 3}} = rule
    end

    test "max_age_gc_rule/1 creates a max age rule" do
      rule = Admin.max_age_gc_rule(86400)

      assert %GcRule{rule: {:max_age, duration}} = rule
      assert duration.seconds == 86400
      assert duration.nanos == 0
    end

    test "intersection_gc_rule/1 combines rules with AND logic" do
      rule1 = Admin.max_versions_gc_rule(3)
      rule2 = Admin.max_age_gc_rule(86400)

      combined = Admin.intersection_gc_rule([rule1, rule2])

      assert %GcRule{rule: {:intersection, intersection}} = combined
      assert length(intersection.rules) == 2
    end

    test "union_gc_rule/1 combines rules with OR logic" do
      rule1 = Admin.max_versions_gc_rule(1000)
      rule2 = Admin.max_age_gc_rule(2_592_000)

      combined = Admin.union_gc_rule([rule1, rule2])

      assert %GcRule{rule: {:union, union}} = combined
      assert length(union.rules) == 2
    end
  end

  describe "column family modification builders" do
    test "create_column_family/2 creates a create modification" do
      gc_rule = Admin.max_versions_gc_rule(1)
      mod = Admin.create_column_family("new_cf", gc_rule)

      assert %ModifyColumnFamiliesRequest.Modification{} = mod
      assert mod.id == "new_cf"
      assert {:create, %ColumnFamily{gc_rule: ^gc_rule}} = mod.mod
    end

    test "create_column_family/1 creates a modification without GC rule" do
      mod = Admin.create_column_family("new_cf")

      assert %ModifyColumnFamiliesRequest.Modification{} = mod
      assert mod.id == "new_cf"
      assert {:create, %ColumnFamily{gc_rule: nil}} = mod.mod
    end

    test "update_column_family/2 creates an update modification" do
      gc_rule = Admin.max_age_gc_rule(3600)
      mod = Admin.update_column_family("existing_cf", gc_rule)

      assert %ModifyColumnFamiliesRequest.Modification{} = mod
      assert mod.id == "existing_cf"
      assert {:update, %ColumnFamily{gc_rule: ^gc_rule}} = mod.mod
    end

    test "drop_column_family/1 creates a drop modification" do
      mod = Admin.drop_column_family("old_cf")

      assert %ModifyColumnFamiliesRequest.Modification{} = mod
      assert mod.id == "old_cf"
      assert mod.mod == {:drop, true}
    end
  end

  describe "complex GC rule scenarios" do
    test "creates rule: keep latest version OR delete after 7 days" do
      rule =
        Admin.union_gc_rule([
          Admin.max_versions_gc_rule(1),
          Admin.max_age_gc_rule(604_800)
        ])

      assert %GcRule{rule: {:union, _}} = rule
    end

    test "creates rule: keep 3 versions AND younger than 30 days" do
      rule =
        Admin.intersection_gc_rule([
          Admin.max_versions_gc_rule(3),
          Admin.max_age_gc_rule(2_592_000)
        ])

      assert %GcRule{rule: {:intersection, _}} = rule
    end
  end
end
