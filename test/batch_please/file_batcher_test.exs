defmodule BatchPlease.FileBatcherTest do
  use ExSpec, async: true
  doctest BatchPlease.FileBatcher

  defmodule TestBatcher do
    use BatchPlease.FileBatcher, lazy_flush: true
    def batch_process(_batch), do: :ok
  end

  def get_state(server) do
    BatchPlease.get_internal_state(server)
  end

  context "FileBatcher" do
    it "batches stuff" do
      {:ok, b0} = GenServer.start_link(TestBatcher, max_batch_size: 2)

      BatchPlease.add_item(b0, 1)
      BatchPlease.add_item(b0, 2)

      state = get_state(b0)
      assert(2 == state.counts.batch_items)
      assert(2 == state.counts.total_items)

      BatchPlease.add_item(b0, 3)

      state = get_state(b0)
      assert(1 == state.counts.batch_items)
      assert(3 == state.counts.total_items)

      GenServer.stop(b0)
    end

    it "actually puts items in a file when batching" do
      {:ok, b0} = GenServer.start_link(TestBatcher, max_batch_size: 5)

      BatchPlease.add_item(b0, 1)
      BatchPlease.add_item(b0, 2)
      BatchPlease.add_item(b0, nil)
      BatchPlease.add_item(b0, 4)
      BatchPlease.add_item(b0, 5)

      state = get_state(b0)
      filename1 = state.batch.filename
      contents = File.read!(filename1)
      assert("1\n2\nnull\n4\n5\n" == contents)

      BatchPlease.add_item(b0, 6)

      state = get_state(b0)
      filename2 = state.batch.filename
      assert(filename1 != filename2)

      contents = File.read!(filename2)
      assert("6\n" == contents)

      GenServer.stop(b0)
    end
  end
end

