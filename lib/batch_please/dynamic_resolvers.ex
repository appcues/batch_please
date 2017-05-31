defmodule BatchPlease.DynamicResolvers do
  @moduledoc false

  @definitely_not_a_module_doc """
  This module provides dynamic function resolution for BatchPlease's `batch_*`
  callbacks.

  For example, a function `do_XXXX(state, batch)` in this module would do
  the following:

  1. If `state.config.XXXX` is defined, run that.
  2. If a function `XXXX` of the correct arity exists in module `state.module`,
     run that.
  3. Raise exception or pass input through.
  """

  @doc false
  @spec do_batch_init(BatchPlease.state, BatchPlease.opts) :: BatchPlease.batch_return | no_return
  def do_batch_init(state, opts) do
    cond do
      state.overrides.batch_init ->
        state.overrides.batch_init.(opts)
      function_exported?(state.module, :batch_init, 1) ->
        state.module.batch_init(opts)
      :else ->
        raise UndefinedFunctionError, message: "batch_init/1 is not defined locally or in module #{state.module}"
    end
  end

  @doc false
  @spec do_batch_add_item(BatchPlease.state, BatchPlease.batch, BatchPlease.item) :: BatchPlease.batch_return | no_return
  def do_batch_add_item(state, batch, item) do
    cond do
      state.overrides.batch_add_item ->
        state.overrides.batch_add_item.(batch, item)
      function_exported?(state.module, :batch_add_item, 2) ->
        state.module.batch_add_item(batch, item)
      :else ->
        raise UndefinedFunctionError, message: "batch_add_item/2 is not defined locally or in module #{state.module}"
    end
  end

  @doc false
  @spec do_batch_pre_process(BatchPlease.state, BatchPlease.batch) :: BatchPlease.batch_return
  def do_batch_pre_process(state, batch) do
    cond do
      state.overrides.batch_pre_process ->
        state.overrides.batch_pre_process.(batch)
      function_exported?(state.module, :batch_pre_process, 1) ->
        state.module.batch_pre_process(batch)
      :else ->
        {:ok, batch}
    end
  end

  @doc false
  @spec do_batch_process(BatchPlease.state, BatchPlease.batch) :: BatchPlease.ok_or_error | no_return
  def do_batch_process(state, batch) do
    cond do
      state.overrides.batch_process ->
        state.overrides.batch_process.(batch)
      function_exported?(state.module, :batch_process, 1) ->
        state.module.batch_process(batch)
      :else ->
        raise UndefinedFunctionError, message: "batch_process/1 is not defined locally or in module #{state.module}"
    end
  end

  @doc false
  @spec do_batch_post_process(BatchPlease.state, BatchPlease.batch) :: BatchPlease.ok_or_error
  def do_batch_post_process(state, batch) do
    cond do
      state.overrides.batch_post_process ->
        state.overrides.batch_post_process.(batch)
      function_exported?(state.module, :batch_post_process, 1) ->
        state.module.batch_post_process(batch)
      :else ->
        :ok
    end
  end

  @doc false
  @spec do_batch_terminate(BatchPlease.state, BatchPlease.batch) :: BatchPlease.ok_or_error
  def do_batch_terminate(state, batch) do
    cond do
      state.overrides.batch_terminate ->
        state.overrides.batch_terminate.(batch)
      function_exported?(state.module, :batch_terminate, 1) ->
        state.module.batch_terminate(batch)
      :else ->
        :ok
    end
  end

  @doc false
  @spec do_should_flush(BatchPlease.state) :: boolean
  def do_should_flush(state) do
    cond do
      state.overrides.should_flush ->
        state.overrides.should_flush.(state)
      function_exported?(state.module, :should_flush, 1) ->
        state.module.should_flush(state)
      :else ->
        false
    end
  end
end

