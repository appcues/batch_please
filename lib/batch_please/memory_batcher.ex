defmodule BatchPlease.MemoryBatcher do
  @doc ~S"""
  Do something with the batch, before starting a new one.
  """
  @callback batch_flush(BatchPlease.batch) :: BatchPlease.ok_or_error

  @doc ~S"""
  Perform any cleanup necessary before terminating this batch server.
  """
  @callback batch_terminate(BatchPlease.batch) :: BatchPlease.ok_or_error

  @optional_callbacks batch_terminate: 1

  defmacro __using__(opts) do
    quote do
      use BatchPlease, unquote(opts)

      def batch_init(opts), do: BatchPlease.MemoryBatcher.batch_init(opts)
      def batch_add_item(batch, item), do: BatchPlease.MemoryBatcher.batch_add_item(batch, item)
      def batch_pre_flush(batch), do: BatchPlease.MemoryBatcher.batch_pre_flush(batch)
    end
  end

  @doc false
  def batch_init(opts) do
    {:ok, %{
      opts: opts,
      items: []
    }}
  end

  @doc false
  ## accumulate items in reverse order for performance
  def batch_add_item(batch, item) do
    {:ok, %{batch | items: [item | batch.items]}}
  end

  @doc false
  ## reverse items at the end of the batch
  def batch_pre_flush(batch) do
    {:ok, %{batch | items: Enum.reverse(batch.items)}}
  end
end

