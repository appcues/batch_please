defmodule BatchPlease do
  @moduledoc ~S"""


  Usage example:

      iex> defmodule MyBatcher do
      ...>   use BatchPlease
      ...>   @behaviour BatchPlease
      ...>
      ...>   def init_batch(_opts) do
      ...>     {:ok, %{items: []}}
      ...>   end
      ...>
      ...>   def add_item_to_batch(state, item) do
      ...>     {:ok, %{state | items: [item | state.items]}}
      ...>   end
      ...>
      ...>   def process_batch(state) do
      ...>     state.items
      ...>     |> Enum.reverse
      ...>     |> Enum.each(&IO.inspect/1)
      ...>
      ...>     :ok
      ...>   end
      ...> end
      iex> {:ok, batch_server} = GenServer.start_link(MyBatcher, max_batch_size: 2)
      iex> BatchPlease.add_item(batch_server, 1)
      :ok
      # no output
      iex> BatchPlease.add_item(batch_server, 2)
      :ok
      # output:
      # 1
      # 2
      iex> BatchPlease.add_item(batch_server, 3)
      :ok
      # no output
      iex> BatchPlease.flush(batch_server)
      :ok
      # output:
      # 3

  """

  #### Internal types

  @type batch_server :: pid

  @type batch_server_state :: %{
    opts: opts,

    init_batch: ((opts) -> batch_return),
    add_item_to_batch: ((batch, item) -> batch_return),
    process_batch: ((batch) -> ok_or_error),
    terminate_batch: ((batch) -> ok_or_error),

    max_batch_size: non_neg_integer | nil,
    max_batch_time_ms: non_neg_integer | nil,

    batch: batch,
    flush_count: non_neg_integer,
    current_item_count: non_neg_integer,
    total_item_count: non_neg_integer,

    last_flush_system_time: non_neg_integer | nil, # :erlang.system_time(:milli_seconds)
    last_flush_monotonic_time: integer | nil,      # :erlang.monotonic_time(:milli_seconds)
    last_item: item | nil,
  }

  @type batch :: %{}

  @type batch_return :: {:ok, batch} | {:error, String.t}

  @type ok_or_error :: :ok | {:error, String.t}

  @type item :: any

  @type opts :: Keyword.t



  #### Behaviour stuff

  @doc ~S"""
  Creates a new batch state, given the configuration options in `opts`.
  This function is called not just once, but every time a new batch
  is created (i.e., at startup and after every flush).
  Defaults to creating an empty map, returning `{:ok, %{}}`.

  Returns the updated batch state or an error message.
  """
  @callback init_batch(opts) :: batch_return
  def init_batch(_opts), do: %{}
  defoverridable init_batch: 1


  @doc ~S"""
  Adds an item to the batch represented in `batch`.

  Returns the updated batch state or an error message.
  """
  @callback add_item_to_batch(batch, item) :: batch_return


  @doc ~S"""
  Processes the batch, whatever that entails.

  This function may operate synchronously or asynchronously,
  according to developer preference.  If it operates synchronously,
  calls to `BatchPlease.flush/1` will block until finished.

  Returns `:ok` on success, or `{:error, message}` otherwise.
  """
  @callback process_batch(batch) :: ok_or_error


  @doc ~S"""
  Cleans up batch state before the batcher process is terminated.
  Defaults to no-op.

  Returns `:ok` on success, or `{:error, message}` otherwise.
  """
  @callback terminate_batch(batch) :: ok_or_error
  def terminate_batch(_batch), do: :ok
  defoverridable terminate_batch: 1



  #### Public functions

  @doc ~S"""
  Adds an item to a batch.
  Returns `:ok` on success, or `{:error, message}` otherwise.
  """
  @spec add_item(batch_server, item) :: :ok | {:error, String.t}
  def add_item(batch_server, item) do
    GenServer.call(batch_server, {:add_item, item})
  end

  @doc ~S"""
  Forces the processing and flushing of a batch.
  Returns `:ok` on success, or `{:error, message}` otherwise.
  """
  @spec flush(batch_server) :: :ok | {:error, String.t}
  def flush(batch_server) do
    GenServer.call(batch_server, {:flush})
  end



  #### GenServer implementation

  @doc false
  @spec init(Keyword.t, atom) :: batch_return
  def init(opts, module) do
    state = %{
      opts: opts,

      init_batch: opts[:init_batch] || &module.init_batch/1,
      add_item_to_batch: opts[:add_item_to_batch] || &module.add_item_to_batch/2,
      process_batch: opts[:process_batch] || &module.process_batch/1,

      max_batch_size: opts[:max_batch_size],
      max_batch_time_ms: opts[:max_batch_time_ms],

      batch: nil,
      flush_count: 0,
      current_item_count: 0,
      total_item_count: 0,

      last_flush_system_time: nil,
      last_flush_monotonic_time: nil,
      last_item: nil,
    }

    {:ok, batch} = state.init_batch.(opts)

    {:ok, %{state | batch: batch}}
  end

  @doc false
  def handle_call({:add_item, item}, _from, state) do
    case state.add_item_to_batch.(state.batch, item) do
      {:ok, init_batch} ->
        {:reply, :ok, %{state |
          batch: init_batch,
          current_item_count: state.current_item_count + 1,
          total_item_count: state.total_item_count + 1,
          last_item: item,
        }}
      {:error, msg} ->
        {:reply, {:error, msg}, state}
    end
  end

  def handle_call({:flush}, _from, state) do
    case state.process_batch.(state.batch) do
      :ok ->
        {:ok, new_batch} = state.init_batch.(state.opts)
        {:reply, :ok, %{state |
          batch: new_batch,
          flush_count: state.flush_count + 1,
          last_flush_monotonic_time: :erlang.monotonic_time(:milli_seconds),
          last_flush_system_time: :erlang.system_time(:milli_seconds),
          current_item_count: 0,
        }}
      {:error, msg} ->
        {:reply, {:error, msg}, state}
    end
  end


  #### `use BatchPlease`

  @doc false
  defmacro __using__(args) do
    quote do
      use GenServer
      def init(args), do: BatchPlease.init(args, __MODULE__)
      def handle_call(msg, from, state), do: BatchPlease.handle_call(msg, from, state)
    end
  end
end
