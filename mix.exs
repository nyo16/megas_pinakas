defmodule MegasPinakas.MixProject do
  use Mix.Project

  def project do
    [
      app: :megas_pinakas,
      version: "0.1.0",
      elixir: "~> 1.19",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      mod: {MegasPinakas.Application, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:grpc_connection_pool, "~> 0.2.1"},
      {:googleapis_proto_ex, "~> 0.3.3"},
      {:goth, "~> 1.4"}
    ]
  end
end
