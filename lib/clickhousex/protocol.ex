defmodule Clickhousex.Protocol do
  @moduledoc false

  @behaviour DBConnection

  alias Clickhousex.HTTPClient, as: Client
  alias Clickhousex.Error

  defstruct conn_opts: [], base_address: ""

  @type state :: %__MODULE__{
          conn_opts: Keyword.t(),
          base_address: String.t()
        }

  @type query :: Clickhousex.Query.t()
  @type result :: Clickhousex.Result.t()
  @type cursor :: any
  @type post_date :: any
  @type query_string_data :: any

  @ping_query Clickhousex.Query.new("SELECT 1") |> DBConnection.Query.parse([])
  @ping_params DBConnection.Query.encode(@ping_query, [], [])

  @default_opts [
    scheme: :http,
    hostname: "localhost",
    port: 8123,
    database: "default",
    username: nil,
    password: nil,
    timeout: Clickhousex.timeout()
  ]

  @impl DBConnection
  @spec connect(opts :: Keyword.t()) ::
          {:ok, state}
          | {:error, Exception.t()}
  def connect(opts) do
    opts = merge_opts(opts)
    base_address = build_base_address(opts[:scheme], opts[:hostname], opts[:port])

    with {:ok, :selected, _, _} <-
           Client.send(
             @ping_query,
             @ping_params,
             base_address,
             opts[:timeout],
             opts[:username],
             opts[:password],
             opts[:database]
           ) do
      {:ok, %__MODULE__{conn_opts: opts, base_address: base_address}}
    end
  end

  @impl DBConnection
  @spec disconnect(err :: Exception.t(), state) :: :ok
  def disconnect(_err, _state) do
    :ok
  end

  @impl DBConnection
  @spec ping(state) ::
          {:ok, state}
          | {:disconnect, Exception.t(), state}
  def ping(state) do
    case do_query(@ping_query, @ping_params, [], state) do
      {:ok, _, _, new_state} -> {:ok, new_state}
      {:error, reason, new_state} -> {:disconnect, reason, new_state}
      other -> other
    end
  end

  @impl DBConnection
  @spec checkin(state) :: {:ok, state}
  def checkin(state) do
    {:ok, state}
  end

  @impl DBConnection
  @spec checkout(state) :: {:ok, state}
  def checkout(state) do
    {:ok, state}
  end

  @impl DBConnection
  def handle_status(_, state) do
    {:idle, state}
  end

  @impl DBConnection
  @spec handle_prepare(query, Keyword.t(), state) :: {:ok, query, state}
  def handle_prepare(query, _, state) do
    {:ok, query, state}
  end

  @impl DBConnection
  @spec handle_execute(
          query,
          %{post_data: post_date, query_string_data: query_string_data},
          opts :: Keyword.t(),
          state
        ) ::
          {:ok, query, result, state}
          | {:error | :disconnect, Exception.t(), state}
  def handle_execute(query, params, opts, state) do
    do_query(query, params, opts, state)
  end

  @impl DBConnection
  def handle_declare(_query, _params, _opts, state) do
    msg = "cursors_not_supported"
    {:error, Error.exception(msg), state}
  end

  @impl DBConnection
  def handle_deallocate(_query, _cursor, _opts, state) do
    msg = "cursors_not_supported"
    {:error, Error.exception(msg), state}
  end

  @impl DBConnection
  def handle_fetch(_query, _cursor, _opts, state) do
    msg = "cursors_not_supported"
    {:error, Error.exception(msg), state}
  end

  @impl DBConnection
  @spec handle_begin(opts :: Keyword.t(), state) :: {:ok, result, state}
  def handle_begin(_opts, state) do
    {:ok, %Clickhousex.Result{}, state}
  end

  @impl DBConnection
  @spec handle_close(query, Keyword.t(), state) :: {:ok, result, state}
  def handle_close(_query, _opts, state) do
    {:ok, %Clickhousex.Result{}, state}
  end

  @impl DBConnection
  @spec handle_commit(opts :: Keyword.t(), state) :: {:ok, result, state}
  def handle_commit(_opts, state) do
    {:ok, %Clickhousex.Result{}, state}
  end

  @impl DBConnection
  @spec handle_rollback(opts :: Keyword.t(), state) :: {:ok, result, state}
  def handle_rollback(_opts, state) do
    {:ok, %Clickhousex.Result{}, state}
  end

  #
  # @spec handle_info(opts :: Keyword.t(), state) :: {:ok, state}
  # def handle_info(_msg, state) do
  #   {:ok, state}
  # end

  # @spec reconnect(new_opts :: Keyword.t(), state) :: {:ok, state}
  # def reconnect(new_opts, state) do
  #   with :ok <- disconnect(Error.exception("Reconnecting"), state) do
  #     connect(new_opts)
  #   end
  # end

  ## Private functions

  defp do_query(query, params, _opts, state) do
    %{base_address: base_address, conn_opts: conn_opts} = state
    username = opts[:username] || conn_opts[:username]
    password = opts[:password] || conn_opts[:password]
    timeout = opts[:timeout] || conn_opts[:timeout]
    database = opts[:database] || conn_opts[:database]

    query
    |> Client.send(params, base_address, timeout, username, password, database)
    |> wrap_errors()
    |> case do
      {:ok, :selected, columns, rows} ->
        {
          :ok,
          query,
          %Clickhousex.Result{
            command: :selected,
            columns: columns,
            rows: rows,
            num_rows: Enum.count(rows)
          },
          state
        }

      {:ok, :updated, count} ->
        {
          :ok,
          query,
          %Clickhousex.Result{
            command: :updated,
            columns: ["count"],
            rows: [[count]],
            num_rows: 1
          },
          state
        }

      {:ok, :created} ->
        {
          :ok,
          query,
          %Clickhousex.Result{
            command: :created
          },
          state
        }

      {:error, %Error{code: :connection_exception} = reason} ->
        {:disconnect, reason, state}

      {:error, reason} ->
        {:error, reason, state}
    end
  end

  defp wrap_errors({:error, reason}), do: {:error, Error.exception(reason)}
  defp wrap_errors(term), do: term

  defp build_base_address(scheme, hostname, port) do
    "#{Atom.to_string(scheme)}://#{hostname}:#{port}/"
  end

  defp merge_opts(opts) do
    opts = Keyword.take(opts, Keyword.keys(@default_opts))
    Keyword.merge(@default_opts, opts)
  end
end
