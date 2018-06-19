
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
      worker(Registry, [:duplicate, PgInsertStage.AlarmRegistry],id: PgInsertStage.AlarmRegistry )
    ]
    opts = [strategy: :one_for_one, name: PgInsertStage.Supervisor]
    #:gen_event.add_handler(:alarm_handler,PgInsertStage.AlarmHandler,:gen_event_init)
    #:gen_event.swap_handler(:alarm_handler,{:alarm_handler,:swap},{PgInsertStage.AlarmHandler, :gen_event_init})
    Supervisor.start_link(children,opts)
  end
  defp test_repo do
    import Supervisor.Spec
    if (Mix.env == :test  and
      # this second condition only starts the repo if
      # `mix test` is executed in `:pg_insert_stage` mix project dir itself!
      Mix.Project.app_path() == Application.app_dir(:pg_insert_stage)
      )
    do
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

  Alternatively, any row can be a `Ecto.multi`, which will be executed.
  The execution order is respected, meaning that an Ecto.Multi will interrupt
  the stream.
  assuming a transaction, where all `Ecto.Schema.t` insert into the same table and repo, and are
  within the same transaction
  |op | method 1       | method 2                 |
  |---|----------------|--------------------------|
  1   | `Ecto.Schema.t`| `Ecto.Schema.t`          |
  2   | `Ecto.Multi`   | `Ecto.Schema.t`          |
  3   | `Ecto.Schema.t`| `Ecto.Multi`             |
  |less efficient(3 op)| more efficient (2 op)    |

  The more efficient variant is because 1 and 2 can be executed within the same
  `COPY` command.

  Valid options are

    * `repo: MyRepo` - the `Ecto.Repo` to use to insert the `rows`. The repository must be a _started Postgres repository_
    * `continue: true|false` - continue the last (previously started) transaction.
    This will error out if no `bulk_insert/2` has previously been called in the current process.

  The `:continue` option is used to ensure that the order of the inserts within any retruned `current_transaction_id`
  is preserved when inserting into the database.

  **Note** that -_in this context_ `current_transaction_id` is _not_ a database transaction, but just a transaction
  (specific to `#{@mod_string_name}`) ensuring the relative insert order within itself.
  The resulting `COPY` operation might also contain rows from other transactions.

  Thus the order is only preserved within each of these transactions, but:

   * not amongst different transactions
   * not amongst rows from different transactions

   The call will wait for the producer to reply, and in case it does not, it times out if the option `timeout: integer()` is set.

  """
  @spec bulk_insert([Ecto.Schema.t], [continue: boolean() , repo: Ecto.Repo.t, timeout: (integer()|:infinity)]) :: current_transaction_id::integer
    | {:error , :no_current_transaction}
  def bulk_insert(rows, options\\[]) do
    alias PgInsertStage.{Producer,WorkSeq}
    transaction_id =
    if Keyword.get(options, :continue, false) do
      WorkSeq.next()
    else
      WorkSeq.current() || WorkSeq.next()
    end
    timeout = Keyword.get(options, :timeout, :infinity)
    repo =
      case Keyword.get(options,:repo) do
        nil-> get_repo()
        repo->repo
      end
    GenStage.call(PgInsertStage.Producer, {:append_work, transaction_id,repo, rows} , timeout)
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
  @spec get_repo(pid) :: Ecto.Repo.t
  def get_repo(pid \\ nil) do
    me =  pid || self()
    Registry.lookup(PgInsertStage.Registry, :repo)
    |> Enum.reduce_while(nil,fn
        {^me,value}, nil-> {:halt, value}
        _, nil->{:cont, nil}
      end)
  end
end
