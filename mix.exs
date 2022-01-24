defmodule Clickhousex.Mixfile do
  use Mix.Project

  def project do
    [
      app: :clickhousex,
      version: "0.5.0",
      elixir: "~> 1.5",
      elixirc_paths: elixirc_paths(Mix.env()),
      deps: deps(),
      package: package(),
      source_url: "https://github.com/clickhouse-elixir/clickhousex"
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [
      {:db_connection, "~> 2.4"},
      {:mint, "~> 1.4"},
      {:jason, "~> 1.2"},
      {:ex_doc, "~> 0.22", only: :dev},
      {:benchee, "~> 1.0", only: [:dev, :test]},
      {:credo, "~> 1.5", only: :dev},
      {:castore, "~> 0.1"}
    ]
  end

  defp package do
    [
      name: "clickhousex",
      description: description(),
      maintainers: maintainers(),
      licenses: ["Apache 2.0"],
      links: %{"GitHub" => "https://github.com/clickhouse-elixir/clickhousex"}
    ]
  end

  defp description do
    "ClickHouse driver for Elixir (uses HTTP)."
  end

  defp maintainers do
    [
      "Roman Chudov",
      "Konstantin Grabar",
      "Ivan Zinoviev",
      "Evgeniy Shurmin",
      "Alexey Lukyanov",
      "Yaroslav Rogov",
      "Ivan Sokolov",
      "Georgy Sychev"
    ]
  end
end
