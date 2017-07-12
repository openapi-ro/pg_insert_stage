ExUnit.start()
#Mix.Ecto.ensure_started PGInsertStage.TestRepo, []
defmodule DataEntry do
    use Ecto.Schema
    schema "test_data_entry" do

    end
end