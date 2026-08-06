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
    pool_config = Config.build_pool_config()

    children = [
      # Auth token cache first: the pool's connections issue authenticated RPCs,
      # so the cache must own its ETS table before any request can be made.
      MegasPinakas.Auth.Cache,
      # Connection pool for BigTable. grpc >= 1.0 starts its own client
      # DynamicSupervisor (registered as GRPC.Client.Supervisor), so there is
      # nothing for us to add to the tree here.
      {GrpcConnectionPool, pool_config}
    ]

    opts = [strategy: :one_for_one, name: MegasPinakas.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
