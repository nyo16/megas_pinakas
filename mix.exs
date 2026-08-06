defmodule MegasPinakas.MixProject do
  use Mix.Project

  @version "0.6.0"
  @source_url "https://github.com/nyo16/megas_pinakas"

  def project do
    [
      app: :megas_pinakas,
      version: @version,
      elixir: "~> 1.15",
      start_permanent: Mix.env() == :prod,
      elixirc_paths: elixirc_paths(Mix.env()),
      deps: deps(),
      name: "MegasPinakas",
      description: description(),
      package: package(),
      docs: docs(),
      source_url: @source_url,
      dialyzer: [
        plt_add_apps: [:mix],
        plt_file: {:no_warn, "priv/plts/dialyzer.plt"},
        ignore_warnings: ".dialyzer_ignore.exs",
        list_unused_filters: true
      ]
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {MegasPinakas.Application, []}
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

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
        Core: [
          MegasPinakas,
          MegasPinakas.Client,
          MegasPinakas.Response
        ],
        "High-Level APIs": [
          MegasPinakas.Cache,
          MegasPinakas.Counter,
          MegasPinakas.CounterTTL,
          MegasPinakas.TimeSeries,
          MegasPinakas.Streaming
        ],
        Builders: [
          MegasPinakas.Row,
          MegasPinakas.Batch,
          MegasPinakas.Filter,
          MegasPinakas.Types
        ],
        Administration: [
          MegasPinakas.Admin,
          MegasPinakas.InstanceAdmin
        ]
      ]
    ]
  end

  defp deps do
    [
      {:grpc_connection_pool, "~> 0.5"},
      {:googleapis_proto_ex, "~> 0.4"},
      {:goth, "~> 1.4"},
      {:benchee, "~> 1.3", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.40", only: :dev, runtime: false},
      {:credo, "~> 1.7.19", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false}
    ]
  end
end
