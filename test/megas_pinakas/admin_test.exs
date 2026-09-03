defmodule MegasPinakas.AdminTest do
  use ExUnit.Case, async: true

  alias Google.Bigtable.Admin.V2.{ColumnFamily, GcRule, ModifyColumnFamiliesRequest, Table}
  alias Google.Longrunning.Operation
  alias MegasPinakas.Admin
  alias MegasPinakas.Test.Emulator

  @project Emulator.project()
  @instance Emulator.instance()

  describe "GC rule builders" do
    test "max_versions_gc_rule/1 creates a max versions rule" do
      assert %GcRule{rule: {:max_num_versions, 3}} = Admin.max_versions_gc_rule(3)
    end

    test "max_age_gc_rule/1 creates a max age rule in whole seconds" do
      assert %GcRule{rule: {:max_age, %Google.Protobuf.Duration{seconds: 86_400, nanos: 0}}} =
               Admin.max_age_gc_rule(86_400)
    end

    test "intersection_gc_rule/1 nests the given rules in order under AND" do
      rule =
        Admin.intersection_gc_rule([
          Admin.max_versions_gc_rule(3),
          Admin.max_age_gc_rule(2_592_000)
        ])

      assert %GcRule{
               rule:
                 {:intersection,
                  %GcRule.Intersection{
                    rules: [
                      %GcRule{rule: {:max_num_versions, 3}},
                      %GcRule{
                        rule: {:max_age, %Google.Protobuf.Duration{seconds: 2_592_000, nanos: 0}}
                      }
                    ]
                  }}
             } = rule
    end

    test "union_gc_rule/1 nests the given rules in order under OR" do
      rule =
        Admin.union_gc_rule([
          Admin.max_versions_gc_rule(1),
          Admin.max_age_gc_rule(604_800)
        ])

      assert %GcRule{
               rule:
                 {:union,
                  %GcRule.Union{
                    rules: [
                      %GcRule{rule: {:max_num_versions, 1}},
                      %GcRule{
                        rule: {:max_age, %Google.Protobuf.Duration{seconds: 604_800, nanos: 0}}
                      }
                    ]
                  }}
             } = rule
    end

    test "combinators nest arbitrarily deep" do
      rule =
        Admin.union_gc_rule([
          Admin.intersection_gc_rule([Admin.max_versions_gc_rule(5), Admin.max_age_gc_rule(60)]),
          Admin.max_versions_gc_rule(100)
        ])

      assert %GcRule{
               rule:
                 {:union,
                  %GcRule.Union{
                    rules: [
                      %GcRule{
                        rule:
                          {:intersection,
                           %GcRule.Intersection{
                             rules: [
                               %GcRule{rule: {:max_num_versions, 5}},
                               %GcRule{rule: {:max_age, %Google.Protobuf.Duration{seconds: 60}}}
                             ]
                           }}
                      },
                      %GcRule{rule: {:max_num_versions, 100}}
                    ]
                  }}
             } = rule
    end
  end

  describe "column family modification builders" do
    test "create_column_family/2 creates a create modification with the GC rule" do
      gc_rule = Admin.max_versions_gc_rule(1)

      assert %ModifyColumnFamiliesRequest.Modification{
               id: "cf",
               mod: {:create, %ColumnFamily{gc_rule: ^gc_rule}}
             } = Admin.create_column_family("cf", gc_rule)
    end

    test "create_column_family/1 creates a modification without GC rule" do
      assert %ModifyColumnFamiliesRequest.Modification{
               id: "cf",
               mod: {:create, %ColumnFamily{gc_rule: nil}}
             } = Admin.create_column_family("cf")
    end

    test "update_column_family/2 creates an update modification" do
      gc_rule = Admin.max_age_gc_rule(3600)

      assert %ModifyColumnFamiliesRequest.Modification{
               id: "cf",
               mod: {:update, %ColumnFamily{gc_rule: ^gc_rule}}
             } = Admin.update_column_family("cf", gc_rule)
    end

    test "drop_column_family/1 creates a drop modification" do
      assert %ModifyColumnFamiliesRequest.Modification{id: "old", mod: {:drop, true}} =
               Admin.drop_column_family("old")
    end
  end

  describe "create_table/4 argument validation" do
    test "rejects :column_families that is not a map before any RPC" do
      assert_raise ArgumentError, ~r/:column_families must be a map/, fn ->
        Admin.create_table(@project, @instance, "admin_bad_shape", column_families: ["cf"])
      end
    end

    test "rejects a column family whose config is not a map" do
      assert_raise ArgumentError, ~r/:column_families entries must be/, fn ->
        Admin.create_table(@project, @instance, "admin_bad_shape",
          column_families: %{"cf" => Admin.max_versions_gc_rule(1)}
        )
      end
    end

    test "rejects non-binary :initial_splits" do
      assert_raise ArgumentError, ~r/:initial_splits must be a list of binaries/, fn ->
        Admin.create_table(@project, @instance, "admin_bad_shape",
          column_families: %{"cf" => %{}},
          initial_splits: [1, 2]
        )
      end
    end
  end

  describe "drop_row_range/4 argument validation" do
    test "returns {:error, :no_target} when neither target option is given" do
      assert {:error, :no_target} = Admin.drop_row_range(@project, @instance, "admin_any")

      assert {:error, :no_target} =
               Admin.drop_row_range(@project, @instance, "admin_any",
                 delete_all_data_from_table: false
               )
    end

    test "raises when both targets are given" do
      assert_raise ArgumentError, ~r/mutually exclusive/, fn ->
        Admin.drop_row_range(@project, @instance, "admin_any",
          row_key_prefix: "x",
          delete_all_data_from_table: true
        )
      end
    end

    test "raises when :row_key_prefix is not a binary" do
      assert_raise ArgumentError, ~r/:row_key_prefix must be a binary/, fn ->
        Admin.drop_row_range(@project, @instance, "admin_any", row_key_prefix: :user)
      end
    end
  end

  describe "wait_operation/2 polling" do
    # Returns a get_fun that answers `pending` times with done: false and then
    # once with `final`, recording every call in the test process mailbox.
    defp scripted_get_fun(pending, final) do
      {:ok, counter} = Agent.start_link(fn -> 0 end)
      test_pid = self()

      fn name ->
        send(test_pid, {:polled, name})
        n = Agent.get_and_update(counter, &{&1, &1 + 1})

        if n < pending do
          {:ok, %Operation{name: name, done: false}}
        else
          {:ok, final}
        end
      end
    end

    defp encoded_table_response(table_name) do
      %Google.Protobuf.Any{
        type_url: "type.googleapis.com/google.bigtable.admin.v2.Table",
        value: Table.encode(%Table{name: table_name})
      }
    end

    test "polls until done and decodes a known response type" do
      final = %Operation{
        name: "operations/op-1",
        done: true,
        result: {:response, encoded_table_response("projects/p/instances/i/tables/t")}
      }

      assert {:ok, %Table{name: "projects/p/instances/i/tables/t"}} =
               Admin.wait_operation("operations/op-1",
                 get_fun: scripted_get_fun(2, final),
                 poll_interval: 1
               )

      assert_received {:polled, "operations/op-1"}
      assert_received {:polled, "operations/op-1"}
      assert_received {:polled, "operations/op-1"}
      refute_received {:polled, _}
    end

    test "accepts an Operation struct and polls by its name" do
      final = %Operation{name: "operations/op-2", done: true}

      assert {:ok, %Operation{name: "operations/op-2", done: true}} =
               Admin.wait_operation(%Operation{name: "operations/op-2", done: false},
                 get_fun: scripted_get_fun(1, final),
                 poll_interval: 1
               )

      assert_received {:polled, "operations/op-2"}
      assert_received {:polled, "operations/op-2"}
    end

    test "resolves an already-done operation without polling" do
      never = fn _ -> flunk("should not poll an operation that is already done") end

      op = %Operation{
        name: "operations/op-3",
        done: true,
        result: {:response, encoded_table_response("projects/p/instances/i/tables/done")}
      }

      assert {:ok, %Table{name: "projects/p/instances/i/tables/done"}} =
               Admin.wait_operation(op, get_fun: never)
    end

    test "returns the operation's error status as a normalized error tuple" do
      final = %Operation{
        name: "operations/op-4",
        done: true,
        result: {:error, %Google.Rpc.Status{code: 6, message: "backup already exists"}}
      }

      assert {:error, {:already_exists, "backup already exists"}} =
               Admin.wait_operation("operations/op-4",
                 get_fun: scripted_get_fun(0, final),
                 poll_interval: 1
               )
    end

    test "hands back the raw operation when the response type is unknown" do
      any = %Google.Protobuf.Any{type_url: "type.googleapis.com/some.Other", value: <<>>}
      final = %Operation{name: "operations/op-5", done: true, result: {:response, any}}

      assert {:ok, %Operation{name: "operations/op-5", result: {:response, ^any}}} =
               Admin.wait_operation("operations/op-5",
                 get_fun: scripted_get_fun(0, final),
                 poll_interval: 1
               )
    end

    test "propagates a failed poll" do
      failing = fn _ -> {:error, {:permission_denied, "nope"}} end

      assert {:error, {:permission_denied, "nope"}} =
               Admin.wait_operation("operations/op-6", get_fun: failing, poll_interval: 1)
    end

    test "gives up with {:error, :timeout} once the deadline passes" do
      pending = fn name -> {:ok, %Operation{name: name, done: false}} end

      assert {:error, :timeout} =
               Admin.wait_operation("operations/op-7",
                 get_fun: pending,
                 poll_interval: 5,
                 timeout: 20
               )
    end

    test "rejects non-positive :poll_interval and :timeout" do
      assert_raise ArgumentError, ~r/:poll_interval/, fn ->
        Admin.wait_operation("operations/x", poll_interval: 0)
      end

      assert_raise ArgumentError, ~r/:timeout/, fn ->
        Admin.wait_operation("operations/x", timeout: -1)
      end
    end
  end

  describe "against the emulator" do
    @describetag :emulator

    @gc_table "admin_gc_rules"
    @list_table "admin_list_tables"
    @modify_table "admin_modify_families"
    @drop_table "admin_drop_row_range"

    setup do
      Emulator.await_pool!()
      :ok
    end

    test "create_table/4 persists GC rules visible through get_table/4" do
      _ = Admin.delete_table(@project, @instance, @gc_table)

      versions = Admin.max_versions_gc_rule(2)
      age = Admin.max_age_gc_rule(3600)
      both = Admin.intersection_gc_rule([versions, age])

      assert {:ok, %Table{}} =
               Admin.create_table(@project, @instance, @gc_table,
                 column_families: %{
                   "v" => %{gc_rule: versions},
                   "a" => %{"gc_rule" => age},
                   "both" => %{gc_rule: both},
                   "none" => %{}
                 }
               )

      assert {:ok, %Table{column_families: families}} =
               Admin.get_table(@project, @instance, @gc_table)

      assert %{
               "v" => %ColumnFamily{gc_rule: ^versions},
               "a" => %ColumnFamily{gc_rule: ^age},
               "both" => %ColumnFamily{gc_rule: ^both},
               "none" => %ColumnFamily{gc_rule: nil}
             } = families

      assert map_size(families) == 4
    end

    test "create_table/4 reports an existing table as already_exists" do
      Emulator.setup_table(@gc_table <> "_dup")

      assert {:error, {:already_exists, _}} =
               Admin.create_table(@project, @instance, @gc_table <> "_dup",
                 column_families: %{"cf" => %{}}
               )
    end

    test "list_tables/3 includes a created table and omits a deleted one" do
      Emulator.setup_table(@list_table)
      path = MegasPinakas.Config.table_path(@project, @instance, @list_table)

      assert {:ok, %{tables: tables}} = Admin.list_tables(@project, @instance)
      assert path in Enum.map(tables, & &1.name)

      assert {:ok, %Google.Protobuf.Empty{}} =
               Admin.delete_table(@project, @instance, @list_table)

      assert {:ok, %{tables: tables}} = Admin.list_tables(@project, @instance)
      refute path in Enum.map(tables, & &1.name)

      assert {:error, {:not_found, _}} = Admin.get_table(@project, @instance, @list_table)
    end

    test "modify_column_families/4 creates, updates, and drops families" do
      Emulator.setup_table(@modify_table, ["keep"])
      new_rule = Admin.max_versions_gc_rule(5)
      updated_rule = Admin.max_age_gc_rule(60)

      assert {:ok, %Table{column_families: families}} =
               Admin.modify_column_families(@project, @instance, @modify_table, [
                 Admin.create_column_family("added", new_rule)
               ])

      assert %ColumnFamily{gc_rule: ^new_rule} = families["added"]
      assert Map.has_key?(families, "keep")

      assert {:ok, %Table{column_families: families}} =
               Admin.modify_column_families(@project, @instance, @modify_table, [
                 Admin.update_column_family("added", updated_rule),
                 Admin.drop_column_family("keep")
               ])

      assert %ColumnFamily{gc_rule: ^updated_rule} = families["added"]
      refute Map.has_key?(families, "keep")

      assert {:ok, %Table{column_families: families}} =
               Admin.get_table(@project, @instance, @modify_table)

      assert Map.keys(families) == ["added"]
    end

    test "drop_row_range/4 with :row_key_prefix removes only matching rows" do
      Emulator.setup_table(@drop_table)
      Emulator.seed_rows(@drop_table, 5, prefix: "a#")
      Emulator.seed_rows(@drop_table, 5, prefix: "b#")

      assert {:ok, %Google.Protobuf.Empty{}} =
               Admin.drop_row_range(@project, @instance, @drop_table, row_key_prefix: "a#")

      assert {:ok, rows} = MegasPinakas.read_rows(@project, @instance, @drop_table)
      keys = Enum.map(rows, & &1.key)

      assert length(keys) == 5
      assert Enum.all?(keys, &String.starts_with?(&1, "b#"))
    end

    test "drop_row_range/4 with :delete_all_data_from_table empties the table" do
      table = @drop_table <> "_all"
      Emulator.setup_table(table)
      Emulator.seed_rows(table, 7, prefix: "x#")

      assert {:ok, %Google.Protobuf.Empty{}} =
               Admin.drop_row_range(@project, @instance, table, delete_all_data_from_table: true)

      assert {:ok, []} = MegasPinakas.read_rows(@project, @instance, table)
      assert {:ok, %Table{}} = Admin.get_table(@project, @instance, table)
    end

    test "get_operation/1 is routed to the admin service" do
      # The emulator has no Operations service, so a well-formed request comes
      # back as :unimplemented rather than a transport or client-side error.
      assert {:error, {:unimplemented, _}} = Admin.get_operation("operations/does-not-exist")
    end
  end
end
