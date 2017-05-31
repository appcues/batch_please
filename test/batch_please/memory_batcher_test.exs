defmodule BatchPlease.MemoryBatcherTest do
  use ExSpec, async: true
  doctest BatchPlease.MemoryBatcher

  defmodule TestBatcher do
    use BatchPlease.MemoryBatcher, lazy_flush: true
    def batch_process(_batch), do: :ok
  end

  def get_state(server) do
    BatchPlease.get_internal_state(server)
  end

  context "MemoryBatcher" do
    it "batches stuff" do
      {:ok, batch_server} = GenServer.start_link(TestBatcher, max_batch_size: 2)

      BatchPlease.add_item(batch_server, 1)
      BatchPlease.add_item(batch_server, 2)

      state = get_state(batch_server)
      assert([2, 1] == state.batch.items)

      BatchPlease.add_item(batch_server, 3)
      state = get_state(batch_server)
      assert([3] == state.batch.items)

      GenServer.stop(batch_server)
    end

    it "reverses order before flushing" do
      {:ok, b0} = GenServer.start_link(TestBatcher, max_batch_size: 200)
      {:ok, b1} = GenServer.start_link(TestBatcher,
        max_batch_size: 2,
        batch_process: fn (batch) ->
          BatchPlease.add_item(b0, batch.items)
        end,
      )

      BatchPlease.add_item(b1, 1)
      BatchPlease.add_item(b1, 2)
      BatchPlease.add_item(b1, 3)

      b0_state = get_state(b0)
      assert([[1, 2]] == b0_state.batch.items)

      GenServer.stop(b0)
      GenServer.stop(b1)
    end
  end


end

