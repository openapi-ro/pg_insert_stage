use Mix.Config

config :pg_insert_stage, TestRepo,
  adapter: Ecto.Adapters.Postgres,
  database: "pg_insert_stage_test_repo",
  username: "paul",
  password: "bubumare",
  hostname: "localhost"
config :pg_insert_stage,
      ecto_repos: [TestRepo]