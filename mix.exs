defmodule BatchPlease.Mixfile do
  use Mix.Project

  def project do
    [app: :batch_please,
     version: "0.1.0",
     elixir: "~> 1.3",
     build_embedded: Mix.env == :prod,
     start_permanent: Mix.env == :prod,
     deps: deps()]
  end

  def application do
    [applications: [:logger]]
  end

  defp deps do
    [
      {:poison, "> 1.0.0", optional: true},
      {:ex_spec, "~> 2.0", only: :test},
    ]
  end
end

