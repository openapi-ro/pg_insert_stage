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
  def to_csv(entities, options \\ nil) do
		options =
      case options do
        %{} -> Enum.to_list(options)
        nil-> []
        _l when is_list(_l)-> options
      end
    rows =
      entities
      |> Enum.map(fn
          %Ecto.Changeset{}=c -> Ecto.Changeset.apply_changes(c);
          e->e
        end)
      |> Enum.map(&csv_row/1)
      |> CSV.encode(options)
      |> Enum.to_list
      |> Enum.join
    first =
      case hd(entities) do
        %Ecto.Changeset{}=c -> Ecto.Changeset.apply_changes(c);
        e->e
      end
    table =
      [options[:prefix] || first.__struct__().__schema__(:prefix) || "public",
       options[:table] || first.__struct__().__schema__(:source)]
      |> Enum.map(fn s -> "\"#{s}\"" end)
      |> Enum.join(".")

    fields = first.__struct__().__schema__(:types) |> Enum.map(fn {name,type}-> "\"#{name}\"" end)|> Enum.join(",")
    {"COPY #{table} (#{fields}) FROM STDIN(FORMAT csv)",
      [rows]
    }
	end
end