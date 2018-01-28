defmodule PgInsertStage.EctoCsv do
  require Logger
  def csv_row(entity) do
    entity.__struct__().__schema__(:types)
    |>Enum.map(fn {name,type} -> {name,type,Map.get(entity,name)} end)
    |>Enum.map(fn
      {name, t, nil} -> ""
      {name, :boolean, value} -> value && "true" || "false"
      {name, t, value} when  t in [:integer, :id] -> Integer.to_string value
      {name, :float, value} -> Float.to_string value
      {name, Ecto.Date, value} -> Ecto.Date.to_string value
      {name, Ecto.UUID, value} -> value
      {name, :naive_datetime, value} -> Ecto.DateTime.to_string value
      {name, t, value} when t in [:string, :binary_id] -> value
      {name, t, value} when t in [:json,:jsonb, :map] ->Poison.encode! value
      {name, :array, value}  ->Poison.encode! value
      {name, {:array,_arr_type} , value}  ->
        Poison.encode!(value)
        |> String.replace_leading( "[", "{")
        |> String.replace_trailing("]", "}")
      end)
  end
  @doc """
    transforms a list of entities to a list of executable queries.
    each element of the query can be one of:
    {:stream, query, enumerable} (usable with `Enum.into`)
    {:exec , Ecto.Multi} A multi to execute in a transaction
  """
  def to_csv(entities, options \\ nil) do
		options =
      case options do
        %{} -> Enum.to_list(options)
        nil-> []
        _l when is_list(_l)-> options
      end
      entities
      |> Enum.map(fn
          %Ecto.Changeset{}=c -> Ecto.Changeset.apply_changes(c)
          %Ecto.Multi{}=multi -> {:exec, multi}
          func when is_function(func) -> 
            multi=
              Ecto.Multi.new()
                |> Ecto.Multi.run( "function #{inspect func}", func )
            {:exec, multi}
          entity -> {:stream, entity}
          e->e
        end)
      |> Enum.chunk_by(fn
          {tag, _other} -> tag
        end)
      |> Enum.map( fn
          # transforms list of same type into single entries of same type:
          #[{:stream, e1} , {:stream, e2}] -> {:stream, query, [e1,e2]}
          [{:stream,first} |_rest ] = list ->
            list=
              list
              |> Enum.reduce([], fn {:stream, elm}, acc -> [elm| acc] end)
              |> Enum.reverse()
              |> Enum.map(&csv_row/1)
              |> CSV.encode(options)
              |> Enum.to_list
              |> Enum.join
            first =
              case first do
                %Ecto.Changeset{}=c -> Ecto.Changeset.apply_changes(c);
                e->e
              end
            table =
              [options[:prefix] || first.__struct__().__schema__(:prefix) || "public",
               options[:table] || first.__struct__().__schema__(:source)]
              |> Enum.map(fn s -> "\"#{s}\"" end)
              |> Enum.join(".")
            fields = first.__struct__().__schema__(:types) |> Enum.map(fn {name,type}-> "\"#{name}\"" end)|> Enum.join(",")
            {:stream,
              "COPY #{table} (#{fields}) FROM STDIN(FORMAT csv)",
              [list]
            }
          [{:exec, %Ecto.Multi{}=multi}] ->
            # Single multi
            {:exec, multi}
          [{:exec, %Ecto.Multi{}=acc}| list]->
            #multiple multis, compact them into the first multi
            multi=
              list
              |> Enum.reduce(acc, fn {:exec, multi} , acc-> Ecto.Multi.append(acc, multi) end)
            {:exec, multi}
      end)
	end
end