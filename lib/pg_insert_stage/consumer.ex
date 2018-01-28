defmodule PgInsertStage.Consumer do
  @mod_string_name String.trim_leading(to_string(__MODULE__), "Elixir.")
	require Logger
	use GenStage
  alias PgInsertStage.EctoCsv
  def start_link(module) when is_atom(module) do
    start_link([module])
  end
	def start_link(subscribe_to\\[]) do
		GenStage.start_link(__MODULE__, %{
          prefix: "public",
          subscribe_to: subscribe_to
        },
        name: PgInsertStage.Consumer)
	end
  defp try_later(time \\10_000) do
    Process.send_after(self(), :try_spawn_inserter, time)
  end
	def init(%{}=global_defaults) do
    {subscribe_to, global_defaults} = Map.pop(global_defaults, :subscribe_to)
		Registry.register(PgInsertStage.Registry, Consumer, nil)
		Logger.info "Inserter Consumer #{inspect( self())} announced"
		state= %{
      total_count: 0,
      max_procs: System.schedulers_online(),
      cache: %{},
      cache_count: 0,
      blocked_tx_by_pid: %{},
      timer: try_later, # The timer ensures that inserters are spawned if work is to be done even if Tasks crash
      defaults: %{
        global: global_defaults
      }
		 }
    if subscribe_to != [] do
      Logger.info "Consumer #{inspect self() } requests registration from #{inspect subscribe_to}"
    end
	 	{:consumer, state, subscribe_to: subscribe_to}
	end
  @doc """
    Function requesting query_inserters to subscribe to current process which is assumed to be a producer
  """
	def subscribe_me(defaults \\%{}) do
		me = self()
		unless defaults[:repo] do
      raise "#{__MODULE__}.subscribe_me/1 needs a repo argument if none is supplied in the configuration"
    end
    Registry.dispatch(PgInsertStage.Registry, Consumer, fn entries ->
			for {pid,nil} <- entries do
				Logger.info("I (#{inspect self()} am requesting inserter #{inspect pid} to subscribe")
				send pid, {:register_with, me, defaults}
			end
		end)
	end
	def handle_info({:register_with, producer_pid, defaults}, state) do
		Logger.info   "registering with #{inspect producer_pid}"
		GenStage.async_subscribe(self(),to: producer_pid , defaults: defaults)
		{:noreply, [], state}
	end
  @doc """
    This is used to register that an inserter Task has finished
    It also receives the count of inserted rows, and schedules an immediate try_spawn_inserter
  """
  def handle_info({:inserter_task_finished , rows}, state) do
     state= Map.put(state, :total_count, state.total_count+rows)
     unless round(state.total_count/1000) == round((state.total_count-rows) /1000) do
       Logger.error("PgInsertStage: exceeded #{round((state.total_count-rows) /1000)}k inserted rows")
     end
     Logger.info("finished task with #{rows} rows. still have #{state.cache_count} to go")
     try_later(0)
     {:noreply, [],state}
  end
  def handle_info(:try_spawn_inserter, state ) do
    Process.cancel_timer(state.timer)
    proc_count = length(Registry.lookup(PgInsertStage.Registry, :inserter))
    state=
      case state.max_procs-proc_count do
        0->  state
        num_procs_to_spawn->
          do_spawn(num_procs_to_spawn, state)
      end
      |> Map.put(:timer, try_later())
    {:noreply,[], state}
  end
  def do_spawn(procs, state ) do
    work = state.cache
    total = state.cache_count
    split_each =
      case Integer.floor_div total, procs do
        0 -> total
        non_zero-> non_zero
      end
    blocked_ids=
      Registry.lookup(PgInsertStage.Registry, :inserter)
        |> Enum.reduce(MapSet.new(), fn {_,blocked_tx_ids_per_task}, acc -> MapSet.union(acc, blocked_tx_ids_per_task) end)
    work_partition=
      work
      |> Enum.group_by(fn{{tx_id,_,_},_} ->
        if MapSet.member?(blocked_ids, tx_id) do
          :blocked
        else
          :not_blocked
        end
      end)
    {grouped, not_blocked_len}=
      work_partition
      |>Map.get(:not_blocked,[])
      |> Enum.group_by(
        fn {{tx_id,_,_}, _val}->tx_id  end )
      |> Enum.reduce({[],0}, fn {tx_id, tx_val} ,{ret, offset} ->
          l = Enum.reduce(tx_val, 0,  fn {key, rows} , count->  count+ length(rows) end)
          {[{tx_val, offset, l}|ret], offset+l}
        end)
    work_chunks=
      grouped
      |> Enum.chunk_by(fn {tx_chunk,offset,len} ->
            Integer.floor_div(offset, split_each) != Integer.floor_div(offset+len, split_each)
        end)
      |> Enum.map(fn chunk->
          {work_per_transaction, contained_tx_ids}=
            Enum.reduce(chunk, {[], MapSet.new()} ,fn
                {tx_val,_offset,_len}, {work_per_transaction, contained_tx_ids} ->
                stripped_tx_id=
                  tx_val
                  |>Enum.map( fn {{tx_id,repo,tbl}, rows}->{{repo,tbl}, rows} end)
                contained_tx_ids=
                  Enum.reduce(tx_val,contained_tx_ids, fn {{tx_id,_repo,_tbl}, _rows}, set-> MapSet.put(set,tx_id) end)
                {[stripped_tx_id|work_per_transaction], contained_tx_ids}
            end)
          {OA.Keyword.myers_merge(work_per_transaction, fn _k,v1,v2->v1++v2 end), contained_tx_ids}
        end)
      caller = self()
      if length(work_chunks) > 0 do
        Logger.info "Spawning #{length(work_chunks)} tasks, out of #{procs} possible"
      end
      Enum.map(work_chunks, fn {by_key, contained_tx_ids}  ->
        {:ok, pid} = Task.start(fn ->
          reg = Registry.register(PgInsertStage.Registry, :inserter, contained_tx_ids)
          row_count=
            by_key
            |> Enum.reduce( 0, fn {{repo,table}, entries} , num->
                res = EctoCsv.to_csv(entries, [repo: repo])
                repo.transaction(fn ->
                  res
                  |> Enum.each( fn
                    {:stream,query, content }->
                      stream = Ecto.Adapters.SQL.stream(repo, query)
                      Enum.into(content, stream)
                    {:exec, multi} ->
                      repo.transaction(multi)
                    end)
                end)
                num+length(entries)
              end)
          Registry.unregister PgInsertStage.Registry, :inserter
          send caller, {:inserter_task_finished, row_count}
         end) #task
      end)
    state=
      state
      |> Map.put(:cache, Map.get(work_partition, :blocked, %{}) |> Map.new())
      |> Map.put(:cache_count, state.cache_count-not_blocked_len)
      #TODO: store work by task_pids and get back if task fails?
    state
  end
  # merges the options as supplied
  # * on startup
  # * overridden by options supplied on subscription
  # * overridden by options supplied for each individual event
  defp get_options([_h|_rest]=options,producer_pid,state) do
    get_options Map.new(options),producer_pid ,state
  end
  defp get_options(%{}=options,producer_pid,state) do
    options= [
      state.defaults.global || %{},
      state.defaults[producer_pid] || %{},
      options
    ]
    |> Enum.reduce(%{} , fn opts, acc ->
        Map.merge acc, opts, fn
            _k, nil, v -> v
            _k, v, nil ->v
            _v,_v1, v-> v
          end
      end)
  end
  def handle_subscribe(:producer, options, producer_pid, state) do
    Logger.info "Registration of Consumer #{inspect self()} to Producer #{inspect producer_pid} complete!"
    subscription_defaults =
      options
      |> Keyword.get( :defaults,[])
      |> Map.new()
    {
      :automatic,
      %{state | defaults: Map.put(state.defaults, producer_pid, subscription_defaults )}
    }
  end
  @doc """
  handles entities to insert into database

    * `repo`  is a `Ecto.Repo` module
    * `transaction_id` is an integer, which ensures that any entries with the same `transaction_id`
    is executed in the order of arrival. the parameter can be generated using `PgInsertStage.WorkSeq`

  This consumer receives events from any producer which has previously called `subscribe_me/1`

  This implements `c:GenStage.handle_events/3` callback
  """
  @type event :: {transaction_id::integer, repo::module, Ecto.Schema.t}
  @spec handle_events([event, ...], from::reference, state::map) :: {:noreply, [event, ...], state::map }
	def handle_events(events,from,state) do
    {options,cache_count,entries_by_table} =
      events
      |> Enum.reduce({nil,state.cache_count,state.cache}, fn
        {transaction_id,repo,next}=entry, {prev_options,total,acc} ->
          {options, entry} =
            case next do
              %Ecto.Multi{}=multi ->
                {[
                  transaction_id: transaction_id,
                  repo: repo,
                  key: {transaction_id, repo, "func"}
                ],
                next
                }
              %{__struct__: module} when is_atom(module) ->
                Code.ensure_loaded( module)
                if  function_exported?(module, :__schema__, 1) do
                  {table,prefix} = {
                      module.__schema__(:source),
                      module.__schema__(:prefix)
                    }
                  {[
                    table: table,
                    prefix: prefix,
                    key: {transaction_id, repo, prefix && "#{prefix}.#{table}" || "#{table}"},
                    transaction_id: transaction_id,
                    repo: repo
                  ],  next}
                else
                  Logger.error("Cannot interpret struct: #{inspect next}. Skipping")
                  require IEx
                  IEx.pry
                  {[transaction_id: transaction_id, repo: repo, skip: true],  next}
                end
            end
          options = get_options(options,from, state)
          unless options[:skip] do
            key = options[:key]
            acc =
              if Map.has_key? acc, key do
                {options, total+ 1, Map.put(acc,key, acc[key] ++ [entry])}
              else
                {options, total + 1, Map.put(acc, key, [entry])}
              end
          else
            #options.skip was true, a error message should have been logged
            {prev_options,total, acc}
          end
        end)
    state =
      state
      |>Map.put(:cache, entries_by_table)
      |>Map.put(:cache_count, cache_count)
    Process.send_after self(), :try_spawn_inserter, 0
    {:noreply,[], state}
	end
  def handle_cancel(cancellation_reason, from, state) do
    Map.delete(state.defaults, from)
    {:noreply, [], state}
  end
end