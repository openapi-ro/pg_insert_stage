
defmodule PgInsertStage do
@mod_string_name String.trim_leading(to_string(__MODULE__), "Elixir.")
@moduledoc """
  Package for ordered bulk-inserts using Postgres `COPY` command.

  PgInsertStage offers an interface to insert in bulk rows into postgres tables.
  The rows themselves must be supplied as `Ecto.Schema` structs of mapped entities

  Use `bulk_insert/2` to insert rows into their tables, preserving the relative order of inserts
  within rows stemming from the same process and transaction (see details in `bulk_insert/2` docs )



### Example:

"""
  require Logger
  @doc false
  def start(_type,_args) do
    import Supervisor.Spec
    children = test_repo() ++ [
      supervisor(Registry, [:duplicate, PgInsertStage.Registry]),
      worker(PgInsertStage.WorkSeq, []),
      worker(PgInsertStage.Producer, []),
      worker(PgInsertStage.Consumer,[PgInsertStage.Producer]),

    ]
    opts = [strategy: :one_for_one, name: RoCrawlers.Supervisor]
    Supervisor.start_link(children,opts)
  end
  defp test_repo do
    import Supervisor.Spec
    if Mix.env == :test do
      Code.load_file "test/test_repo.ex"
     [worker( TestRepo , [] )]
    else
      []
    end
  end
  @doc """
  Inserts `rows` into their table.

  Each `row` is a struct using `t:Ecto.Schema.t`, and thus provides the specific field aqnd table mapping from `Ecto`.
  The repository for the operation can be provided as:
  * `config :pg_insert_stage :repo MyRepo`
  * as an option to `bulk_insert/2`
  * using set_repo(repo)


  Valid options are

    * `repo: MyRepo` - the `Ecto.Repo` to use to insert the `rows`. The repository must be a _started Postgres repository_
    * `contine: true|false` - continue the last (previously started) transaction.
    This will error out if no `bulk_insert/2` has previously been called in the current process.

  The `:continue` option is used to ensure that the order of the inserts within any retruned `current_transaction_id`
  is preserved when inserting into the database.

  **Note** that -_in this context_ `current_transaction_id` is _not_ a database transaction, but just a transaction
  (specific to `#{@mod_string_name}`) ensuring the relative insert order within itself.
  The resulting `COPY` operation might also contain rows from other transactions.

  Thus the order is only preserved within each of these transactions, but:

   * not amongst different transactions
   * not amongst rows from different transactions

  """
  @spec bulk_insert([Ecto.Schema.t], [continue: boolean() , repo: Ecto.Repo.t]) :: current_transaction_id::integer
    | {:error , :no_current_transaction}
  def bulk_insert(rows, options\\[]) do
    alias PgInsertStage.{Producer,WorkSeq}
    transaction_id =
    if Keyword.get(options, :continue, false) do
      WorkSeq.next()
    else
      WorkSeq.current() || WorkSeq.next()
    end
    repo =
      case Keyword.get(options,:repo) do
        nil-> get_repo()
        repo->repo
      end
    send PgInsertStage.Producer , {:append_work, transaction_id,repo, rows}
  end
  @doc """
    Sets the default repo for the current process
  """
  @spec set_repo(Ecto.Repo.t) :: none
  def set_repo(repo) do
    Registry.unregister(PgInsertStage.Registry, :repo)
    Registry.register(PgInsertStage.Registry, :repo, repo)
  end
  @doc """
    gets the default `t:Ecto.Repo` as set with `set_repo/1`
  """
  @spec get_repo() :: Ecto.Repo.t
  def get_repo() do
    me = self()
    Registry.lookup(PgInsertStage.Registry, :repo)
    |> Enum.reduce_while(nil,fn
        {^me,value}, nil-> {:halt, value}
        _, nil->{:cont, nil}
      end)
  end
end
