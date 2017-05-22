defmodule BatchPlease.MemoryBatcher do
  defmacro __using__(opts) do
    quote do
      use BatchPlease, unquote(opts)
      def init_batch(opts), do: BatchPlease.MemoryBatcher.init_batch(opts)
      def add_item_to_batch(batch, item), do: BatchPlease.MemoryBatcher.add_item_to_batch(batch, item)
      def process_batch(batch), do: :ok
      def terminate_batch(batch), do: :ok
    end
  end

  @doc false
  def init_batch(_opts) do
    {:ok, %{items: []}}
  end

  @doc false
  def add_item_to_batch(batch, item) do
    {:ok, %{batch | items: [item | batch.items]}}
  end
end

