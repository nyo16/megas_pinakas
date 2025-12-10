defmodule MegasPinakas.MixProject do
  use Mix.Project

  @version "0.5.0"
  @source_url "https://github.com/nyo16/megas_pinakas"

  def project do
    [
      app: :megas_pinakas,
      version: @version,
      elixir: "~> 1.15",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      name: "MegasPinakas",
      description: description(),
      package: package(),
      docs: docs(),
      source_url: @source_url
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {MegasPinakas.Application, []}
    ]
  end

  defp description do
    """
    An Elixir client library for Google Cloud BigTable with high-level APIs for
    data operations, streaming, caching, counters, time-series, and more.
    """
  end

  defp package do
    [
      name: "megas_pinakas",
      licenses: ["Apache-2.0"],
      links: %{
        "GitHub" => @source_url,
        "Changelog" => "#{@source_url}/blob/main/CHANGELOG.md"
      },
      files: ~w(lib .formatter.exs mix.exs README.md LICENSE CHANGELOG.md)
    ]
  end

  defp docs do
    [
      main: "readme",
      extras: ["README.md", "CHANGELOG.md"],
      source_ref: "v#{@version}",
      source_url: @source_url,
      groups_for_modules: [
        "Core": [
          MegasPinakas,
          MegasPinakas.Connection
        ],
        "High-Level APIs": [
          MegasPinakas.Cache,
          MegasPinakas.Counter,
          MegasPinakas.CounterTTL,
          MegasPinakas.TimeSeries,
          MegasPinakas.Streaming
        ],
        "Builders": [
          MegasPinakas.Row,
          MegasPinakas.Batch,
          MegasPinakas.Filter,
          MegasPinakas.Types
        ],
        "Administration": [
          MegasPinakas.Admin,
          MegasPinakas.InstanceAdmin
        ]
      ]
    ]
  end

  defp deps do
    [
      {:grpc_connection_pool, "~> 0.2.1"},
      {:googleapis_proto_ex, "~> 0.3.3"},
      {:goth, "~> 1.4"},
      {:ex_doc, "~> 0.31", only: :dev, runtime: false}
    ]
  end
end
