defmodule BatchPlease do
  @moduledoc ~S"""
  BatchPlease is a tool for collecting batches of items, and doing something
  with each batch when it reaches a certain size or age.

  It is built on top of GenServer, implemented as a behaviour,
  and invoked through `use BatchPlease`.

  It is useful to build specialized batch collectors on top of BatchPlease,
  in order to abstract more details from the end user.  Examples of this
  approach include `BatchPlease.MemoryBatcher` (which stores items in memory
  when batching) and `BatchPlease.FileBatcher` (which encodes items to
  string format and accumulates them in an on-disk file until ready for
  processing).

  Simple/trivial usage example:

      defmodule Summer do
        use BatchPlease, max_batch_size: 3

        def batch_init(_opts) do
          {:ok, %{sum: 0}}
        end

        def batch_add_item(batch, item) do
          {:ok, %{batch | sum: batch.sum + item}}
        end

        def batch_process(batch) do
          IO.puts("This batch added up to #{batch.sum}")
          :ok
        end
      end

      {:ok, pid} = GenServer.start_link(Summer, [])

      BatchPlease.add_item(pid, 1)
      BatchPlease.add_item(pid, 2)
      BatchPlease.add_item(pid, 3) # prints "This batch added up to 6"

      BatchPlease.add_item(pid, 4)
      BatchPlease.add_item(pid, 5)
      BatchPlease.add_item(pid, 6) # prints "This batch added up to 15"
  """



  #### `use BatchPlease`

  @doc false
  defmacro __using__(opts) do
    quote do
      use GenServer
      def init(args), do: BatchPlease.Impl.init(args ++ unquote(opts), __MODULE__)
      def handle_call(msg, from, state), do: BatchPlease.Impl.handle_call(msg, from, state)
      def handle_cast(msg, state), do: BatchPlease.Impl.handle_cast(msg, state)
      def handle_info(msg, state), do: BatchPlease.Impl.handle_info(msg, state)
      def terminate(reason, state), do: BatchPlease.Impl.terminate(reason, state)
      @behaviour BatchPlease
    end
  end


  #### Internal types

  @typedoc ~S"""
  A GenServer performing as a batch server.
  """
  @type batch_server :: pid

  @typedoc ~S"""
  A map representing the internal state of a batch server.  This map
  contains a `batch` key, representing the state of the current batch.
  """
  @type state :: %{
    opts: opts,
    module: atom,
    batch: batch,
    last_item: item | nil,
    flush_timer: pid,
    config: state_config,
    overrides: state_overrides,
    counts: state_counts,
    times: state_times,
  }

  @type state_config :: %{
    lazy_flush: boolean | nil,
    max_batch_size: non_neg_integer | nil,
    max_time_since_last_flush: non_neg_integer | nil,
    max_time_since_first_item: non_neg_integer | nil,
    flush_interval: non_neg_integer | nil,
  }

  @type state_overrides :: %{
    batch_init: ((opts) -> batch_return) | nil,
    batch_add_item: ((batch, item) -> batch_return) | nil,
    batch_pre_process: ((batch) -> batch_return) | nil,
    batch_process: ((batch) -> ok_or_error) | nil,
    batch_post_process: ((batch) -> ok_or_error) | nil,
    batch_terminate: ((batch) -> ok_or_error) | nil,
    should_flush: ((state) -> boolean) | nil,
  }

  @type state_counts :: %{
    batch_items: non_neg_integer,
    total_items: non_neg_integer,
    flushes: non_neg_integer,
  }

  @type state_times :: %{
    first_item_of_batch: integer | nil,
    last_flush: integer | nil,
  }

  @typep state_return :: {:ok, state} | {:error, String.t}

  @typep reply_return :: {ok_or_error, state}

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
  @type opts :: [option]

  @type option ::
    {:eager_flush, boolean} |
    {:max_batch_size, non_neg_integer | nil} |
    {:max_time_since_last_flush, non_neg_integer | nil} |
    {:max_time_since_first_item, non_neg_integer | nil}



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
  `batch_process/1`.  Returns the updated batch state or an error message.

  This is an optional callback.
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
  Performs some post-processing on the batch, after `batch_process/1`
  has completed successfully.  Does not return an updated batch, because
  this operation is immediately followed by `batch_init/1` to create a new
  batch.

  Returns `:ok` on success, or `{:error, message}` otherwise.

  This is an optional callback.
  """
  @callback batch_post_process(batch) :: ok_or_error


  @doc ~S"""
  Cleans up batch state before the batcher process is terminated.
  Defaults to no-op.  This is not guaranteed to be called at
  termination time -- for more information, see:
  https://hexdocs.pm/elixir/GenServer.html#c:terminate/2

  Returns `:ok` on success, or `{:error, message}` otherwise.

  This is an optional callback.
  """
  @callback batch_terminate(batch) :: ok_or_error


  @doc ~S"""
  Given the current module state, returns whether the current batch
  should be processed now.  Precedes the handling of `max_batch_size`
  (but does not replace it).

  This is an optional callback.
  """
  @callback should_flush(state) :: boolean

  @optional_callbacks [
    batch_terminate: 1,
    batch_pre_process: 1,
    batch_post_process: 1,
    should_flush: 1,
  ]


  #### Public API

  @doc ~S"""
  Adds an item to a batch asynchronously.

  Set `opts[:error_pid]` to receive a message of the form `{:error, msg}`
  in case this operation fails.  Alternately, use `sync_add_item/2` for
  a synchronous version of this function.

  Returns `:ok`.
  """
  @spec add_item(batch_server, item, Keyword.t) :: :ok
  def add_item(batch_server, item, opts \\ []) do
    GenServer.cast(batch_server, {:add_item, item, opts[:error_pid]})
  end


  @doc ~S"""
  Adds an item to a batch synchronously.

  Returns `:ok` or `{:error, msg}`.
  """
  @spec sync_add_item(batch_server, item) :: :ok | {:error, String.t}
  def sync_add_item(batch_server, item) do
    GenServer.call(batch_server, {:add_item, item})
  end


  @doc ~S"""
  Forces the processing and flushing of a batch asynchronously.

  Set `opts[:error_pid]` to receive a message of the form `{:error, msg}`
  in case this operation fails.  Alternately, use `sync_flush/1` for
  a synchronous version of this function.

  Returns `:ok`.
  """
  @spec flush(batch_server, Keyword.t) :: :ok | {:error, String.t}
  def flush(batch_server, opts \\ []) do
    GenServer.cast(batch_server, {:flush, opts[:error_pid]})
  end

  @doc ~S"""
  Forces the processing and flushing of a batch synchronously.

  Returns `:ok` or `{:error, msg}`.
  """
  @spec sync_flush(batch_server) :: :ok | {:error, String.t}
  def sync_flush(batch_server) do
    GenServer.call(batch_server, :flush)
  end


  @doc false
  def get_internal_state(batch_server) do
    GenServer.call(batch_server, :get_internal_state)
  end

end

