defmodule ConsumerTest do
	require Logger
	defmodule TestProducer do
		use GenStage
		def start_link() do
			GenStage.start_link(TestProducer, :ok	, name: TestProducer)
		end
		def init(:ok) do
			Logger.info "subscribing (in producer. #{inspect self()})"
			ret =PgInsertStage.Consumer.subscribe_me(repo: PGInsertStage.TestRepo)
			Logger.info("Subscribed : #{inspect ret}")
      {:producer, {10000000,1}}
		end
		def handle_demand(demand, {stock, current}) do
      Logger.info "received demand of #{demand}"
			{data, to}=
				cond do
					stock == 0 -> {[], current}
					true->
            to = current+min(demand, stock)
            Logger.info "from: #{current}, to: #{to}"
            ret=
              current..to
    						|> Enum.map(&({ 1, TestRepo, %DataEntry{id: &1}}))
				    {ret,to}
        end
      #Logger.error "DATA: #{inspect data}"
			{:noreply, data, {max(stock-demand, 0), to+1}}
		end
	end
	use ExUnit.Case, async: false
  #setup do
  #  :ok = Ecto.Adapters.SQL.Sandbox.checkout( PGInsertStage.TestRepo )
  #  # Setting the shared mode must be done only after checkout
  #  Ecto.Adapters.SQL.Sandbox.mode( PGInsertStage.TestRepo , {:shared, self()})
  #end

  setup do
    TestRepo.delete_all DataEntry, timeout: :infinity
    :ok
  end
	test "Register by registry" do
		TestProducer.start_link()
		#require IEx
		#IEx.pry
    Process.sleep(1000)
	end
end