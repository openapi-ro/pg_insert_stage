defmodule PgInsertStage.MemoryAlarm do
  @moduledoc """
    Handles Memory alarms by suppressing `&GenStage.ask/1` requests when
    the `:system_memory_high_watermark` is set.

    This implementation use `AlarmHandlex` receiving alarms.
  """
  defmacro __using__([ask_count: ask_count]) do
    quote location: :keep do
      require AlarmHandlex
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
        ask_more(state)
      end
      def ask_more(%{alarm_set: true}=state) do
        Logger.warn("__MODULE__: Not asking for more Events, memory consumption too high")
        state
      end
      def ask_more(state) do
        subscriptions=
        state.subscriptions
        |> Enum.map( fn
          {proc_ref, open_demand} when open_demand < unquote(ask_count) ->
            GenStage.ask(proc_ref,unquote(ask_count)-open_demand)
            {proc_ref, unquote(ask_count) }
          {proc_ref, unquote(ask_count)} ->
            {proc_ref, unquote(ask_count)}
          end)
        |> Map.new()
        %{state| subscriptions: subscriptions}
      end
      def handle_info(:set_alarm, state) do
       {:noreply, Map.put(state, :alarm_set, true)}
      end
      def handle_info(:clear_alarm, state) do
        state
        {:noreply, Map.put(state, :alarm_set, false)}
      end
    end
  end
end