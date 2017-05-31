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

      {:ok, pid} = GenServer.start_link(Summer)

      BatchPlease.add_item(pid, 1)
      BatchPlease.add_item(pid, 2)
      BatchPlease.add_item(pid, 3) # prints "This batch added up to 6"

      BatchPlease.add_item(pid, 4)
      BatchPlease.add_item(pid, 5)
      BatchPlease.add_item(pid, 6) # prints "This batch added up to 15"
  """

  import BatchPlease.DynamicResolvers


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
    GenServer.call(batch_server, :flush)
  end

  @doc false
  def get_internal_state(batch_server) do
    GenServer.call(batch_server, :get_internal_state)
  end


  #### GenServer implementation

  @doc false
  @spec init(Keyword.t, atom) :: batch_return
  def init(opts, module) do
    state = %{
      opts: opts,     ## invocation opts
      module: module, ## module containing implementation (if any)
      batch: nil,     ## batch state
      last_item: nil, ## last item added

      config: %{
        lazy_flush: opts[:lazy_flush],
        max_batch_size: opts[:max_batch_size],
        max_time_since_last_flush: opts[:max_time_since_last_flush],
        max_time_since_first_item: opts[:max_time_since_first_item],
      },

      overrides: %{
        batch_init: opts[:batch_init],
        batch_add_item: opts[:batch_add_item],
        batch_pre_process: opts[:batch_pre_process],
        batch_post_process: opts[:batch_post_process],
        batch_process: opts[:batch_process],
        batch_terminate: opts[:batch_terminate],
        should_flush: opts[:should_flush],
      },

      counts: %{
        flushes: 0,
        batch_items: 0,
        total_items: 0,
      },

      times: %{
        last_flush: mono_now(),
        first_item_of_batch: nil,
      },
    }

    {:ok, batch} = do_batch_init(state, opts)

    {:ok, %{state | batch: batch}}
  end


  @doc false
  def handle_call({:add_item, item}, _from, state) do
    {reply, state} = handle_add_item(state, item)
    {:reply, reply, state}
  end

  def handle_call(:flush, _from, state) do
    {reply, state} = handle_flush(state)
    {:reply, reply, state}
  end

  def handle_call(:get_internal_state, _from, state) do
    {:reply, state, state}
  end

  @doc false
  def terminate(_reason, state) do
    do_batch_terminate(state, state.batch)
  end




  #### internal impl

  ## Adds an item to the state, flushing as necessary.
  @spec handle_add_item(state, item) :: reply_return
  defp handle_add_item(state, item) do
    with {:ok, state} <- autoflush_state(state),
         {:ok, state} <- add_item_to_state(state, item),
         {:ok, state} <- eager_flush_state(state)
    do
      {:ok, state}
    else
      {:error, msg} -> {{:error, msg}, state}
    end
  end

  @spec add_item_to_state(state, item) :: state_return
  defp add_item_to_state(state, item) do
    with {:ok, batch} <- do_batch_add_item(state, state.batch, item)
    do
      {:ok, %{state |
        batch: batch,
        counts: %{state.counts |
          batch_items: state.counts.batch_items + 1,
          total_items: state.counts.total_items + 1,
        },
        times: %{state.times |
          first_item_of_batch: state.times.first_item_of_batch || mono_now(),
        },
        last_item: item,
      }}
    end
  end


  @spec autoflush_state(state) :: state_return
  defp autoflush_state(state) do
    should_flush = do_should_flush(state) ||
                   should_flush_on_size?(state) ||
                   should_flush_on_age?(state)

    if should_flush do
      handle_flush(state)
    else
      {:ok, state}
    end
  end

  @spec should_flush_on_size?(state) :: boolean
  defp should_flush_on_size?(state) do
    case state.config.max_batch_size do
      nil -> false
      size -> state.counts.batch_items >= size
    end
  end

  @spec should_flush_on_age?(state) :: boolean
  defp should_flush_on_age?(state) do
    mtslf = state.config.max_time_since_last_flush
    mtsfi = state.config.max_time_since_first_item

    cond do
      mtslf && (mono_now() >= mtslf + state.times.last_flush) ->
        true
      mtsfi && (mono_now() >= mtsfi + state.times.first_item_of_batch) ->
        true
      :else ->
        false
    end
  end

  @spec eager_flush_state(state) :: state_return
  defp eager_flush_state(state) do
    if state.config.lazy_flush, do: {:ok, state}, else: autoflush_state(state)
  end


  @spec handle_flush(state) :: reply_return
  defp handle_flush(state) do
    case process(state, state.batch) do
      :ok ->
        {:ok, new_batch} = do_batch_init(state, state.opts)
        {:ok, %{state |
          batch: new_batch,
          counts: %{state.counts |
            flushes: state.counts.flushes + 1,
            batch_items: 0,
          },
          times: %{state.times |
            last_flush: mono_now(),
          },
        }}
      {:error, msg} ->
        {{:error, msg}, state}
    end
  end



  ## Performs pre, regular, and post processing
  @spec process(state, batch) :: ok_or_error
  defp process(state, batch) do
    with {:ok, batch} <- do_batch_pre_process(state, batch),
         :ok <- do_batch_process(state, batch)
    do
      do_batch_post_process(state, batch)
    end
  end


  defp mono_now, do: :erlang.monotonic_time(:milli_seconds)
end

