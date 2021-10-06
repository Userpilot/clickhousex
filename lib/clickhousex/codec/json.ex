defmodule Clickhousex.Codec.JSON do
  @moduledoc """
  `Clickhousex.Codec` implementation for JSON output format.

  See [JSON][1], [JSONCompact][2].

  [1]: https://clickhouse.tech/docs/en/interfaces/formats/#json
  [2]: https://clickhouse.tech/docs/en/interfaces/formats/#jsoncompact
  """

  alias Clickhousex.Codec
  @behaviour Codec

  @impl Codec
  defdelegate encode(query, replacements, params), to: Codec.Values

  @impl Codec
  def request_format do
    "Values"
  end

  @impl Codec
  def response_format do
    "JSONCompact"
  end

  @impl Codec
  def new do
    []
  end

  @impl Codec
  def append(state, data) do
    [state, data]
  end

  @impl Codec
  def decode(response) do
    with {:ok, %{"meta" => meta, "data" => data, "rows" => row_count}} <- Jason.decode(response) do
      column_names = Enum.map(meta, & &1["name"])
      column_types = Enum.map(meta, & &1["type"])
      rows = Enum.map(data, &decode_row(&1, column_types))

      {:ok, %{column_names: column_names, rows: rows, count: row_count}}
    end
  end

  @spec decode_row([term], [atom]) :: [term]
  def decode_row(row, column_types) do
    column_types
    |> Enum.zip(row)
    |> Enum.map(fn {type, raw_value} ->
      to_native(type, raw_value)
    end)
  end

  defp to_native(_, nil) do
    nil
  end

  defp to_native(<<"Nullable(", type::binary>>, value) do
    type = String.replace_suffix(type, ")", "")
    to_native(type, value)
  end

  defp to_native(<<"Array(", type::binary>>, value) do
    type = String.replace_suffix(type, ")", "")
    Enum.map(value, &to_native(type, &1))
  end

  defp to_native("Tuple(" <> types, values) do
    types =
      types
      |> String.replace_suffix(")", "")
      |> String.split(", ")

    values
    |> Enum.zip(types)
    |> Enum.map(fn {value, type} -> to_native(type, value) end)
    |> List.to_tuple()
  end

  defp to_native("Float" <> _, value) when is_integer(value) do
    1.0 * value
  end

  defp to_native("Int64", value) do
    String.to_integer(value)
  end

  defp to_native("Date", value) do
    {:ok, date} = Date.from_iso8601(value)
    date
  end

  defp to_native("DateTime" <> _, value) do
    with {:ok, naive} <- NaiveDateTime.from_iso8601(value) do
      naive
    end
  end

  defp to_native("UInt" <> _, value) when is_bitstring(value) do
    String.to_integer(value)
  end

  defp to_native("Int" <> _, value) when is_bitstring(value) do
    String.to_integer(value)
  end

  defp to_native(_, value) do
    value
  end
end
