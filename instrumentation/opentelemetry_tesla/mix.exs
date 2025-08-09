defmodule OpentelemetryTesla.MixProject do
  use Mix.Project

  @version "2.4.0"

  def project do
    [
      app: :opentelemetry_tesla,
      description: description(),
      version: @version,
      elixir: "~> 1.14",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      name: "Opentelemetry Tesla",
      docs: [
        source_url_pattern:
          "https://github.com/open-telemetry/opentelemetry-erlang-contrib/blob/main/instrumentation/opentelemetry_tesla/%{path}#L%{line}",
        main: "Tesla.Middleware.OpenTelemetry",
        extras: ["README.md"]
      ],
      elixirc_paths: elixirc_paths(Mix.env()),
      package: package(),
      source_url:
        "https://github.com/open-telemetry/opentelemetry-erlang-contrib/tree/main/instrumentation/opentelemetry_tesla"
    ]
  end

  defp description() do
    "Tesla middleware that creates OpenTelemetry spans and injects tracing headers into HTTP requests for Tesla clients."
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp package do
    [
      description: "OpenTelemetry tracing for Tesla",
      files: ~w(lib .formatter.exs mix.exs README* LICENSE* CHANGELOG*),
      licenses: ["Apache-2.0"],
      links: %{
        "GitHub" =>
          "https://github.com/open-telemetry/opentelemetry-erlang-contrib/tree/main/instrumentation/opentelemetry_tesla",
        "OpenTelemetry Erlang" => "https://github.com/open-telemetry/opentelemetry-erlang",
        "OpenTelemetry Erlang Contrib" =>
          "https://github.com/open-telemetry/opentelemetry-erlang-contrib",
        "OpenTelemetry.io" => "https://opentelemetry.io"
      }
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:jason, "~> 1.3"},
      {:nimble_options, "~> 1.1"},
      {:opentelemetry_api, "~> 1.4"},
      {:opentelemetry_semantic_conventions, "~> 1.27"},
      {:otel_http, "~> 0.2"},
      {:tesla, "~> 1.4"},
      {:ex_doc, "~> 0.38", only: [:dev, :test]},
      {:opentelemetry_exporter, "~> 1.8", only: [:test]},
      {:opentelemetry, "~> 1.5", only: :test},
      {:bypass, "~> 2.1", only: :test},
      {:plug, ">= 1.15.0", only: [:test]},
      {:dialyxir, "~> 1.1", only: [:dev, :test], runtime: false}
    ]
  end
end
