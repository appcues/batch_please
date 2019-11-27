defmodule BatchPlease.Impl do
  @moduledoc false

  require Logger
  import BatchPlease.DynamicResolvers


  #### GenServer implementation

  @doc false
  @spec init(Keyword.t, atom) :: BatchPlease.batch_return
  def init(opts, module) do
    state = %{
      opts: opts,     ## invocation opts
      module: module, ## module containing implementation (if any)
      batch: nil,     ## batch state
      last_item: nil, ## last item added
      flush_timer: nil,  ## erlang timer for periodic flush

      config: %{
        max_batch_size: opts[:max_batch_size],
        max_time_since_last_flush: opts[:max_time_since_last_flush],
        max_time_since_first_item: opts[:max_time_since_first_item],
        flush_interval: opts[:flush_interval],
      },

      overrides: %{
        batch_init: opts[:batch_init],
        batch_add_item: opts[:batch_add_item],
        batch_pre_flush: opts[:batch_pre_flush],
        batch_flush: opts[:batch_flush],
        batch_post_flush: opts[:batch_post_flush],
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
  def handle_cast({:add_item, item, error_pid}, state) do
    case handle_add_item(state, item) do
      {{:error, msg}, _state} ->
        if error_pid, do: send(error_pid, {:error, msg})
        {:noreply, state}
      {_reply, new_state} ->
        {:noreply, new_state}
    end
  end

  def handle_cast({:flush, error_pid}, state) do
    case handle_flush(state) do
      {{:error, msg}, _state} ->
        if error_pid, do: send(error_pid, {:error, msg})
        {:noreply, state}
      {_reply, new_state} ->
        {:noreply, new_state}
    end
  end

  @doc false
  def handle_info({:timeout, _timer, :flush}, state) do
    GenServer.cast(self(), {:flush, nil})  # TODO put error handling here
    {:noreply, cancel_timer(state)}
  end

  def handle_info(msg, state) do
    Logger.warn("unrecognized message to handle_info: #{inspect(msg)}")
    Logger.warn("state: #{inspect(state)}")
    {:noreply, state}
  end


  @doc false
  def terminate(_reason, state) do
    do_batch_terminate(state, state.batch)
  end



  #### internal impl

  ## Adds an item to the state, flushing as necessary.
  @spec handle_add_item(BatchPlease.state, BatchPlease.item) :: BatchPlease.reply_return
  defp handle_add_item(state, item) do
    with {:ok, state} <- autoflush_state(state),
         {:ok, state} <- add_item_to_state(state, item),
         {:ok, state} <- autoflush_state(state)
    do
      {:ok, set_timer_if_nil(state)}
    else
      {{:error, msg}, _state} -> {{:error, msg}, state}
    end
  end

  @spec add_item_to_state(BatchPlease.state, BatchPlease.item) :: BatchPlease.state_return
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


  @spec autoflush_state(BatchPlease.state) :: BatchPlease.state_return
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

  @spec should_flush_on_size?(BatchPlease.state) :: boolean
  defp should_flush_on_size?(state) do
    case state.config.max_batch_size do
      nil -> false
      size -> state.counts.batch_items >= size
    end
  end

  @spec should_flush_on_age?(BatchPlease.state) :: boolean
  defp should_flush_on_age?(state) do
    mtslf = state.config.max_time_since_last_flush
    lf = state.times.last_flush
    mtsfi = state.config.max_time_since_first_item
    fi = state.times.first_item_of_batch

    cond do
      mtslf && lf && (mono_now() >= mtslf + lf) ->
        true
      mtsfi && fi && (mono_now() >= mtsfi + fi) ->
        true
      :else ->
        false
    end
  end


  @spec handle_flush(BatchPlease.state) :: BatchPlease.reply_return
  defp handle_flush(state) do
    case process(state, state.batch) do
      :ok ->
        {:ok, new_batch} = do_batch_init(state, state.opts)
        {:ok, %{cancel_timer(state) |
          batch: new_batch,
          counts: %{state.counts |
            flushes: state.counts.flushes + 1,
            batch_items: 0,
          },
          times: %{state.times |
            last_flush: mono_now(),
            first_item_of_batch: nil,
          },
        }}
      {:error, msg} ->
        {{:error, msg}, state}
    end
  end


  defp set_timer_if_nil(%{flush_timer: nil}=state), do: set_timer(state)

  defp set_timer_if_nil(state), do: state


  defp set_timer(%{config: %{flush_interval: nil}}=state), do: state

  defp set_timer(state) do
    # add 0-20% jitter
    jitter = :random.uniform(div(state.config.flush_interval, 5))
    timer = :erlang.start_timer(state.config.flush_interval + jitter, self(), :flush)
    %{cancel_timer(state) | flush_timer: timer}
  end


  defp cancel_timer(%{flush_timer: nil}=state), do: state

  defp cancel_timer(%{flush_timer: timer}=state) do
    :erlang.cancel_timer(timer)
    %{state | flush_timer: nil}
  end


  ## Performs pre, regular, and post processing
  @spec process(BatchPlease.state, BatchPlease.batch) :: BatchPlease.ok_or_error
  defp process(state, batch) do
    with {:ok, batch} <- do_batch_pre_flush(state, batch),
         {:ok, _} <- do_batch_flush(state, batch)
    do
      do_batch_post_flush(state, batch)
    end
  end


  defp mono_now, do: :erlang.monotonic_time(:milli_seconds)

end
