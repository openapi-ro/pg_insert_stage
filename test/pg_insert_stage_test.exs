defmodule PgInsertStageTest do
  use ExUnit.Case
  doctest PgInsertStage

  setup do
    TestRepo.delete_all DataEntry, timeout: :infinity
    :ok
  end
  test "the truth" do
    assert 1 + 1 == 2
  end
  @tag timeout: :infinity
  test "insert using one transaction" do
    PgInsertStage.set_repo( TestRepo )
    1..10000000
    |> Enum.map(&(%DataEntry{id: &1}))
    |> PgInsertStage.bulk_insert()
    #Process.sleep(1000000)
  end
end
