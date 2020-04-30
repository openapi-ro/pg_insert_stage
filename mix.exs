defmodule PgInsertStage.Mixfile do
  use Mix.Project

  def project do
    [app: :pg_insert_stage,
     version: "0.1.0",
     elixir: "~> 1.4",
     build_embedded: Mix.env == :prod,
     start_permanent: Mix.env == :prod,
     deps: deps()]
  end

  # Configuration for the OTP application
  #
  # Type "mix help compile.app" for more information
  def application do
    # Specify extra applications you'll use from Erlang/Elixir
    [
      mod: {PgInsertStage,[]},
      extra_applications: [:logger, :ecto,:postgrex, :gen_stage, :csv, :os_mon, :alarm_handlex],
      docs: [
        main: "PgInsertStage", # The main page in the docs
        extras: ["README.md"]
      ]
    ]
  end

  # Dependencies can be Hex packages:
  #
  #   {:my_dep, "~> 0.3.0"}
  #
  # Or git/path repositories:
  #
  #   {:my_dep, git: "https://github.com/elixir-lang/my_dep.git", tag: "0.1.0"}
  #
  # Type "mix help deps" for more examples and options
  defp deps do
    openapi_deps([
      :oaex,
      :alarm_handlex
    ]) ++
    [
      {:postgrex, ">= 0.0.0"},
      {:ecto_sql, "~> 3.2"},
      {:gen_stage, "~>0.12"},
      {:alarm_handlex, path: "../alarm_handlex"},
      {:csv, "~> 2.0"},
      {:ex_doc, "~> 0.14", only: :dev, runtime: false}
    ]
  end
  def openapi_deps(packages) do
    Enum.map(packages, fn
      package when is_atom(package) -> openapi_dep(Mix.env, package)
      {package, opts} when is_atom(package) and is_list(opts) ->
        {package, default_opts } = openapi_dep(Mix.env, package)
        {package, Keyword.merge(default_opts, opts)}
      end)
  end
  def openapi_dep(:prod, package) when is_atom(package) do
    {package, git: "git@github.com:openapi-ro/#{package}.git"}
  end
  def openapi_dep(_, package)  when is_atom(package)do
    {package, path: "../#{package}"}
  end
end
