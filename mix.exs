defmodule BatchPlease.Mixfile do
  use Mix.Project

  def project do
    [app: :batch_please,
     description: "A library for collecting and processing batches of data",
     package: package(),
     version: "0.1.0",
     elixir: "~> 1.3",
     build_embedded: Mix.env == :prod,
     start_permanent: Mix.env == :prod,
     deps: deps()]
  end

  def package do
    [
      maintainers: ["pete gamache <pete@appcues.com>"],
      licenses: ["MIT"],
      links: %{github: "https://github.com/appcues/batch-please"},
    ]
  end

  def application do
    [applications: [:logger]]
  end

  defp deps do
    [
      {:poison, "> 1.0.0", optional: true},
      {:ex_spec, "~> 2.0", only: :test},
      {:ex_doc, "> 0.0.0", only: :dev},
    ]
  end
end

