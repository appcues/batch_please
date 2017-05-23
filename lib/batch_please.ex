defmodule BatchPlease do
  @moduledoc ~S"""


  Usage example:

      iex> defmodule MyBatcher do
      ...>   use BatchPlease
      ...>   @behaviour BatchPlease
      ...>
      ...>   def batch_init(_opts) do
      ...>     {:ok, %{items: []}}
      ...>   end
      ...>
      ...>   def batch_add_item(state, item) do
      ...>     {:ok, %{state | items: [item | state.items]}}
      ...>   end
      ...>
      ...>   def batch_process(state) do
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

  @typedoc ~S"""
  A GenServer performing as a batch server.
  """
  @type batch_server :: pid

  @typedoc ~S"""
  A map representing the internal state of a batch server.  This map
  contains a `batch` key, representing the state of the current batch.
  """
  @type batch_server_state :: %{
    opts: opts,

    batch_init: ((opts) -> batch_return),
    batch_add_item: ((batch, item) -> batch_return),
    batch_process: ((batch) -> ok_or_error),
    batch_terminate: ((batch) -> ok_or_error),

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

  @typedoc ~S"""
  A map representing the state of the current batch.
  Contents of this map are implementation-specific.
  """
  @type batch :: %{}

  @typedoc ~S"""
  Represents the return value of functions which generate a new batch state
  (`batch_init/1` and `batch_add_item/2`).
  """
  @type batch_return :: {:ok, batch} | {:error, String.t}

  @typedoc ~S"""
  Return value of functions which may fail, but do not return a new batch
  state (`batch_process/1` and `batch_terminate/1`).
  """
  @type ok_or_error :: :ok | {:error, String.t}

  @typedoc ~S"""
  Any item that can be added to a batch.
  """
  @type item :: any

  @typedoc ~S"""
  Configuration parameters for a batch server.
  """
  @type opts :: Keyword.t



  #### Behaviour stuff

  @doc ~S"""
  Creates a new batch state, given the configuration options in `opts`.
  This function is called not just once, but every time a new batch
  is created (i.e., at startup and after every flush).
  Defaults to creating an empty map, returning `{:ok, %{}}`.

  Returns the updated batch state or an error message.
  """
  @callback batch_init(opts) :: batch_return


  @doc ~S"""
  Adds an item to the batch represented in `batch`.

  Returns the updated batch state or an error message.
  """
  @callback batch_add_item(batch, item) :: batch_return


  @doc ~S"""
  Performs some pre-processing on the batch, before it is passed to
  `batch_process/1`.  This is an optional callback.

  Returns the updated batch state or an error message.
  """
  @callback batch_pre_process(batch) :: batch_return


  @doc ~S"""
  Processes the batch, whatever that entails.

  This function may operate synchronously or asynchronously,
  according to developer preference.  If it operates synchronously,
  calls to `BatchPlease.flush/1` will block until finished.

  Returns `:ok` on success, or `{:error, message}` otherwise.
  """
  @callback batch_process(batch) :: ok_or_error


  @doc ~S"""
  Cleans up batch state before the batcher process is terminated.
  Defaults to no-op.  This is not guaranteed to be called at
  termination time -- for more information, see:
  https://hexdocs.pm/elixir/GenServer.html#c:terminate/2

  Returns `:ok` on success, or `{:error, message}` otherwise.
  """
  @callback batch_terminate(batch) :: ok_or_error

  @optional_callbacks batch_terminate: 1, batch_pre_process: 1



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



  #### dynamic invocations

  @doc false
  def do_batch_init(state, opts) do
    cond do
      state.batch_init ->
        state.batch_init.(opts)
      function_exported?(state.module, :batch_init, 1) ->
        state.module.batch_init(opts)
      :else ->
        raise UndefinedFunctionError, message: "batch_init/1 is not defined locally or in module #{state.module}"
    end
  end

  @doc false
  def do_batch_add_item(state, batch, item) do
    cond do
      state.batch_add_item ->
        state.batch_add_item.(batch, item)
      function_exported?(state.module, :batch_add_item, 2) ->
        state.module.batch_add_item(batch, item)
      :else ->
        raise UndefinedFunctionError, message: "batch_add_item/2 is not defined locally or in module #{state.module}"
    end
  end

  @doc false
  def do_batch_pre_process(state, batch) do
    cond do
      state.batch_pre_process ->
        state.batch_pre_process.(batch)
      function_exported?(state.module, :batch_pre_process, 1) ->
        state.module.batch_pre_process(batch)
      :else ->
        {:ok, batch}
    end
  end

  @doc false
  def do_batch_process(state, batch) do
    {:ok, batch} = do_batch_pre_process(state, batch)

    cond do
      state.batch_process ->
        state.batch_process.(batch)
      function_exported?(state.module, :batch_process, 1) ->
        state.module.batch_process(batch)
      :else ->
        raise UndefinedFunctionError, message: "batch_process/1 is not defined locally or in module #{state.module}"
    end
  end

  @doc false
  def do_batch_terminate(state, batch) do
    cond do
      state.batch_terminate ->
        state.batch_terminate.(state.batch)
      function_exported?(state.module, :batch_terminate, 1) ->
        state.module.batch_terminate(state.batch)
      :else ->
        :ok
    end
  end



  #### GenServer implementation

  @doc false
  @spec init(Keyword.t, atom) :: batch_return
  def init(opts, module) do
    state = %{
      opts: opts,     ## invocation opts
      module: module, ## module containing implementation (if any)

      batch_init: opts[:batch_init],           ## overrides module impl
      batch_add_item: opts[:batch_add_item],   ## overrides module impl
      batch_pre_process: opts[:batch_pre_process],     ## overrides module impl
      batch_process: opts[:batch_process],     ## overrides module impl
      batch_terminate: opts[:batch_terminate], ## overrides module impl

      max_batch_size: opts[:max_batch_size],       ## nil == no limit

      ## TODO
      #max_batch_time_ms: opts[:max_batch_time_ms], ## nil == no limit

      batch: nil,            ## batch state
      flush_count: 0,        ## number of flushes since launch
      current_item_count: 0, ## number of items in current batch
      total_item_count: 0,   ## number of items added since launch

      last_flush_system_time: nil,    ## milliseconds since unix epoch
      last_flush_monotonic_time: nil, ## monotonic milliseconds
      last_item: nil,                 ## last item added
    }

    {:ok, batch} = do_batch_init(state, opts)

    {:ok, %{state | batch: batch}}
  end


  @doc false
  def handle_call({:add_item, item}, _from, state) do
    ## FIXME duplicated logic from :flush
    state = cond do
      state.max_batch_size && state.max_batch_size <= state.current_item_count ->
        :ok = do_batch_process(state, state.batch)
        {:ok, new_batch} = do_batch_init(state, state.opts)
        %{state |
          batch: new_batch,
          last_flush_monotonic_time: :erlang.monotonic_time(:milli_seconds),
          last_flush_system_time: :erlang.system_time(:milli_seconds),
          flush_count: state.flush_count + 1,
          current_item_count: 0
        }
      :else ->
        state
    end

    case do_batch_add_item(state, state.batch, item) do
      {:ok, batch} ->
        {:reply, :ok, %{state |
          batch: batch,
          current_item_count: state.current_item_count + 1,
          total_item_count: state.total_item_count + 1,
          last_item: item,
        }}
      {:error, msg} ->
        {:reply, {:error, msg}, state}
    end
  end

  def handle_call({:flush}, _from, state) do
    case do_batch_process(state, state.batch) do
      :ok ->
        {:ok, new_batch} = do_batch_init(state, state.opts)
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

  @doc false
  def terminate(reason, state) do
    do_batch_terminate(state, state.batch)
  end


  #### `use BatchPlease`

  @doc false
  defmacro __using__(opts) do
    quote do
      use GenServer
      def init(args), do: BatchPlease.init(unquote(opts) ++ args, __MODULE__)
      def handle_call(msg, from, state), do: BatchPlease.handle_call(msg, from, state)
      def terminate(reason, state), do: BatchPlease.terminate(reason, state)
      @behaviour BatchPlease
    end
  end
end
