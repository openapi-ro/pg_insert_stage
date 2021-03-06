defmodule PgInsertStage.Producer do
  @mod_string_name String.trim_leading(to_string(__MODULE__), "Elixir.")
  @moduledoc """
  `#{@mod_string_name}` acts as entrypoint for insert Operations which do not implement `GenStage`'s producer interface.

  The common way to use this module is via `PgInsertStage.bulk_insert/2`

  To use it directly see `handle_info/2`'s `{:append_work, transaction,repo,rows}` clause.
  """
  require AlarmHandlex
  use GenStage
  require Logger
  def start_link() do
    GenStage.start_link(__MODULE__,:ok, name: __MODULE__)
  end
  @doc false
  def init(:ok) do
    state= %{
      work: [],
      work_length: 0,
      open_demand: 0,
      append_work_reply_list: []
      }
    state = subscribe_alarm(state)
    {:producer, state}
  end
  def subscribe_alarm(state) do 
    me = self()
    AlarmHandlex.on_set_alarm(fn
      :system_memory_high_watermark ,_alarm_desc ->
        send(me, :set_alarm )
      _alarm_id, _alarm_desc ->
        nil
    end)
    AlarmHandlex.on_clear_alarm(fn
      :system_memory_high_watermark ->
        send(me, :clear_alarm )
      _alarm_id ->
        nil
    end)
    Map.put(state, :alarm_set, false)
  end
  def handle_info(:set_alarm, state) do
    {:noreply, [],  Map.put(state, :alarm_set, true)}
   end
   def handle_info(:clear_alarm, state) do
     state.append_work_reply_list
     |> Enum.each( fn client ->
        GenStage.reply(client, :ok)
     end)
     {:noreply, [], %{state| alarm_set: false, append_work_reply_list: []}}
   end
  def handle_demand(demand,state) do
     %{work_length: work_length, open_demand: open_demand, work: work}=state
     demand = demand+open_demand
     if demand < work_length do
       {emit, work} = Enum.split(work, demand)
       state= %{state|
        open_demand: 0,
        work_length: work_length-demand,
        work: work,
       }
       Logger.debug "PgInsertStage.Producer: Length after emitting #{demand}: #{work_length-demand}"
       {:noreply, emit, state}
    else
      state= %{state|
        open_demand: demand-work_length,
        work_length: 0,
        work: []
       }
      {:noreply, work, state}
    end
  end
  @doc """
  Used to schedule `rows` for insertion.

  See `PgInsertStage.bulk_insert/2` for the way the inserts are pseudo-transacted (only the order is preserved)

  The `repo` argument can be `nil` if a default repo has been provided at configuration time.
  Any `repo` provided here will, however override the configuration default
  """
  @spec handle_call(
    {:append_work, transaction::integer,  Ecto.Repo.t, rows::[Ecto.Schema.t]},from::any, any
    ) :: {:noreply, [], state::any}
  def handle_call({:append_work, transaction, repo, rows} , from, state) do
    state =
      %{state|
        work: state.work ++ Enum.map(rows, fn row-> {transaction,repo, row} end),
        work_length: state.work_length+length(rows)
      }
      {:noreply,events, state} = handle_demand(0,state)
    if state.alarm_set do
      state = Map.put(state, :append_work_reply_list,  [from| state.append_work_reply_list])
      {:noreply, events, state}
    else
      {:reply, :ok, events, state}
    end
  end
end