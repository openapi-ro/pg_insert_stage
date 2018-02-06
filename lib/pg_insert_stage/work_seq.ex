defmodule PgInsertStage.WorkSeq do
  @mod_string_name String.trim_leading(to_string(__MODULE__), "Elixir.")
  @moduledoc """
    Assigns a  globally unique sequence value to the calling Process.

    `#{@mod_string_name}` uses the `Registry` module to store a current sequence value,
    which is assigned to the calling process alone.

    The sequence is an increasing integer. While the increment is 1 globally, each individual
    Process will observe gaps in the sequence if multiple Processes call `next`

    The sequence value will be unique for the Process lifetime of `#{@mod_string_name}`.
    The state, however is _not persisted across `#{@mod_string_name}` process or vm restarts_.
  """
  @doc false
  def start_link() do
    Agent.start_link(fn -> %{last: 0} end, name: __MODULE__)
  end
  @doc """
    retrieves the next squence value and assigns it to the calling process.
  """
  @spec next() :: integer
  def next() do
    value=
      Agent.get_and_update(__MODULE__, fn state-> {
        state.last+1,
        %{state| last: state.last+1}
        }
      end)
    Registry.unregister(PgInsertStage.Registry, :current_work_index)
    Registry.register(PgInsertStage.Registry, :current_work_index, value)
    value
  end
  @doc """
    Retrieves the current value for the calling process.

    Note that the return might be null if `next()` has not been called before by the same process.
  """
  @spec current() :: integer
  def current() do
    me=self()
    Registry.lookup(PgInsertStage.Registry, :current_work_index)
    |> Enum.reduce_while(nil, fn
        {^me,idx},nil -> {:halt,idx}
        _, nil        -> {:cont, nil}
      end)
  end
  @doc """
    unassign the transaction id currently associated with the caller process
  """
  @spec unassign_current() :: {:ok, :unassigned | :was_not_assigned}
  def unassign_current do
    if current() do
      #Unregisters all entries for the given key associated to the current process in registry.
      #so that is *only|* for the current process
      Registry.unregister(PgInsertStage.Registry, :current_work_index)
      {:ok, :unassigned}
    else
      {:ok, :was_not_assigned}
    end

  end
end