defmodule BatchPlease.DynamicResolvers do
  @moduledoc false

  @definitely_not_a_module_doc """
  This module provides dynamic function resolution for BatchPlease's `batch_*`
  callbacks.

  For example, a function `do_XXXX(state, batch)` in this module would do
  the following:

  1. If `state.XXXX` is defined, run that.
  2. If a function `XXXX` of the correct arity exists in module `state.module`,
     run that.
  3. Raise exception or pass input through.
  """

  @doc false
  @spec do_batch_init(BatchPlease.batch_server_state, BatchPlease.opts) :: BatchPlease.batch_return | no_return
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
  @spec do_batch_add_item(BatchPlease.batch_server_state, BatchPlease.batch, BatchPlease.item) :: BatchPlease.batch_return | no_return
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
  @spec do_batch_pre_process(BatchPlease.batch_server_state, BatchPlease.batch) :: BatchPlease.batch_return
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
  @spec do_batch_process(BatchPlease.batch_server_state, BatchPlease.batch) :: BatchPlease.ok_or_error | no_return
  def do_batch_process(state, batch) do
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
  @spec do_batch_post_process(BatchPlease.batch_server_state, BatchPlease.batch) :: BatchPlease.ok_or_error
  def do_batch_post_process(state, batch) do
    cond do
      state.batch_post_process ->
        state.batch_post_process.(batch)
      function_exported?(state.module, :batch_post_process, 1) ->
        state.module.batch_post_process(batch)
      :else ->
        :ok
    end
  end

  @doc false
  @spec do_batch_terminate(BatchPlease.batch_server_state, BatchPlease.batch) :: BatchPlease.ok_or_error
  def do_batch_terminate(state, batch) do
    cond do
      state.batch_terminate ->
        state.batch_terminate.(batch)
      function_exported?(state.module, :batch_terminate, 1) ->
        state.module.batch_terminate(batch)
      :else ->
        :ok
    end
  end

  @doc false
  @spec do_should_flush(BatchPlease.batch_server_state) :: boolean
  def do_should_flush(state) do
    cond do
      state.should_flush ->
        state.should_flush.(state)
      function_exported?(state.module, :should_flush, 1) ->
        state.module.should_flush(state)
      :else
        false
    end
  end
end

