defmodule MegasPinakas.Application do
  @moduledoc """
  OTP Application module for MegasPinakas BigTable client.

  Starts the supervision tree including the gRPC client supervisor
  and connection pool for BigTable operations.
  """

  use Application

  alias MegasPinakas.Config

  @impl true
  def start(_type, _args) do
    pool_config = Config.build_pool_config()

    children = [
      # gRPC client supervisor for managing connections
      {GRPC.Client.Supervisor, []},
      # Connection pool for BigTable
      {GrpcConnectionPool, pool_config}
    ]

    opts = [strategy: :one_for_one, name: MegasPinakas.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
