defmodule MegasPinakas.Application do
  @moduledoc """
  OTP Application module for MegasPinakas BigTable client.

  Starts the supervision tree including the connection pool for
  BigTable operations.
  """

  use Application

  alias MegasPinakas.Config

  @impl true
  def start(_type, _args) do
    children = [
      # Auth token cache first: the pool's connections issue authenticated RPCs,
      # so the cache must own its ETS table before any request can be made.
      MegasPinakas.Auth.Cache,
      # Connection pools for BigTable. grpc >= 1.0 starts its own client
      # DynamicSupervisor (registered as GRPC.Client.Supervisor), so there is
      # nothing for us to add to the tree here.
      #
      # Data and Admin APIs live on different hosts in production, so each gets
      # its own pool. Both point at the emulator when one is configured.
      Supervisor.child_spec({GrpcConnectionPool, Config.build_pool_config()},
        id: MegasPinakas.ConnectionPool
      ),
      Supervisor.child_spec({GrpcConnectionPool, Config.build_admin_pool_config()},
        id: MegasPinakas.AdminConnectionPool
      )
    ]

    opts = [strategy: :one_for_one, name: MegasPinakas.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
