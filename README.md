# PgInsertStage

`PgInsertStage` inserts rows in a pseudo-transacted way using postgres 
`COPY` commands.
`PgInsertStage.bulk_insert/2` accepts
## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `pg_insert_stage` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [{:pg_insert_stage, "~> 0.1.0"}]
end
```

Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc)
and published on [HexDocs](https://hexdocs.pm). Once published, the docs can
be found at [https://hexdocs.pm/pg_insert_stage](https://hexdocs.pm/pg_insert_stage).

