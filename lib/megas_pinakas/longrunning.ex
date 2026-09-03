defmodule MegasPinakas.Longrunning do
  @moduledoc """
  gRPC service and stub definitions for `google.longrunning.Operations`.

  The BigTable admin APIs return `Google.Longrunning.Operation` structs for
  slow work (creating instances and clusters, backups, restores). Polling those
  operations goes through the generic Operations service. The message types
  (`Google.Longrunning.*`, `Google.Rpc.Status`) come from the `googleapis`
  package, a transitive dependency via `grpc_core`, which ships no service
  definition — so it is declared here.
  `MegasPinakas.Admin.get_operation/1` and `MegasPinakas.Admin.wait_operation/2`
  build on the stub defined here; most callers never need this module directly.
  """

  defmodule Operations.Service do
    @moduledoc false

    use GRPC.Service,
      name: "google.longrunning.Operations",
      protoc_gen_elixir_version: "0.17.0"

    rpc(:GetOperation, Google.Longrunning.GetOperationRequest, Google.Longrunning.Operation)
    rpc(:WaitOperation, Google.Longrunning.WaitOperationRequest, Google.Longrunning.Operation)
    rpc(:CancelOperation, Google.Longrunning.CancelOperationRequest, Google.Protobuf.Empty)
    rpc(:DeleteOperation, Google.Longrunning.DeleteOperationRequest, Google.Protobuf.Empty)
  end

  defmodule Operations.Stub do
    @moduledoc false

    use GRPC.Stub, service: MegasPinakas.Longrunning.Operations.Service
  end
end
