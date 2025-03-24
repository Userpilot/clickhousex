defmodule Clickhousex.Query do
  @moduledoc """
  Query struct returned from a successfully prepared query.
  """

  @type t :: %__MODULE__{
          name: iodata,
          type: :select | :insert | :alter | :create | :drop,
          param_count: integer,
          params: iodata | nil,
          columns: [String.t()] | nil
        }

  defstruct name: nil,
            statement: "",
            type: :select,
            params: [],
            param_count: 0,
            columns: []

  def new(statement) do
    DBConnection.Query.parse(%__MODULE__{statement: statement}, [])
  end
end

defimpl DBConnection.Query, for: Clickhousex.Query do
  alias Clickhousex.HTTPRequest

  @values_regex ~r/VALUES/i
  @select_query_regex ~r/\bSELECT\b/i
  @alter_query_regex ~r/\bALTER\b/i

  @create_query_keyword "CREATE"
  @insert_query_keyword "INSERT"

  @escaped_question_mark_literal "\\?"

  @codec Application.get_env(:clickhousex, :codec, Clickhousex.Codec.JSON)

  def parse(%{statement: statement} = query, _opts) do
    param_count =
      statement
      |> String.replace(@escaped_question_mark_literal, "")
      |> String.codepoints()
      |> Enum.count(fn s -> s == "?" end)

    query_type = query_type(statement)

    %{
      query
      | type: query_type,
        param_count: param_count,
        statement: String.replace(statement, @escaped_question_mark_literal, "?")
    }
  end

  def describe(query, _opts) do
    query
  end

  @doc """
    Special handling for zero-param SELECT queries (i.e. raw queries) indicated by
    `type: :select` and `param_count: 0` where the encoded query statement is to placed
    inside of the POST Body instead of placing it on the query string to overcome existing issues
    where the encoded query is Too Large to fit into the query params of the Request's URI, which was observed
    frequently on our error logging system under the message: `Request-URI Too Large`.
  """
  def encode(%Clickhousex.Query{type: type, param_count: 0} = query, params, _opts)
      when type in [:select, :insert] do
    {query_statement, _post_body_part} = do_parse(query)
    encoded_query_statement = @codec.encode(query, query_statement, params)

    HTTPRequest.with_post_data(HTTPRequest.new(), encoded_query_statement)
  end

  def encode(query, params, _opts) do
    {query_part, _post_body_part} = do_parse(query)
    encoded_params = @codec.encode(query, query_part, params)

    HTTPRequest.new()
    |> HTTPRequest.with_query_string_data(encoded_params)
  end

  def decode(_query, result, _opts) do
    result
  end

  defp do_parse(%{type: :insert, statement: statement}) do
    with true <- Regex.match?(@values_regex, statement),
         [fragment, substitutions] <- String.split(statement, @values_regex),
         true <- String.contains?(substitutions, "?") do
      {fragment <> " FORMAT #{@codec.request_format}", substitutions}
    else
      _ ->
        {statement, ""}
    end
  end

  defp do_parse(%{statement: statement}) do
    {statement, ""}
  end

  defp query_type(statement) do
    cond do
      starts_with_keyword?(statement, @create_query_keyword) -> :create
      starts_with_keyword?(statement, @insert_query_keyword) -> :insert
      Regex.match?(@select_query_regex, statement) -> :select
      Regex.match?(@alter_query_regex, statement) -> :alter
      true -> :update
    end
  end

  defp starts_with_keyword?(statement, keyword) do
    downcased_keyword = String.downcase(keyword)

    statement
    |> String.trim_leading()
    |> String.downcase()
    |> String.starts_with?(downcased_keyword)
  end
end

defimpl String.Chars, for: Clickhousex.Query do
  def to_string(%Clickhousex.Query{statement: statement}) do
    IO.iodata_to_binary(statement)
  end
end
