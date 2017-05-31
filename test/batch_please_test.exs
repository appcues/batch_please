defmodule BatchPleaseTest do
  use ExSpec, async: true
  doctest BatchPlease

  defmodule TestBatcher do
    use BatchPlease, max_batch_size: 2
    def batch_init(opts), do: {:ok, %{items: []}}
    def batch_add_item(batch, item), do: {:ok, %{batch | items: [item |batch.items]}}
    def batch_process(batch), do: :ok
  end

  context "config" do
    it "overrides `use BatchPlease` params with GenServer.start_link opts" do
      {:ok, pid} = GenServer.start_link(TestBatcher, max_batch_size: 10)

      BatchPlease.add_item(pid, 1)
      BatchPlease.add_item(pid, 2)
      BatchPlease.add_item(pid, 3)
      BatchPlease.add_item(pid, 4)
      BatchPlease.add_item(pid, 5)

      state = BatchPlease.get_internal_state(pid)
      assert(state.counts.batch_items == 5)

      GenServer.stop(pid)
    end

    it "respects max_batch_size and lazy_flush" do
      {:ok, pid} = GenServer.start_link(TestBatcher, [])
      BatchPlease.add_item(pid, 1)
      BatchPlease.add_item(pid, 2)
      state = BatchPlease.get_internal_state(pid)
      assert(state.counts.batch_items == 0)
      GenServer.stop(pid)

      {:ok, pid} = GenServer.start_link(TestBatcher, lazy_flush: true)
      BatchPlease.add_item(pid, 1)
      BatchPlease.add_item(pid, 2)
      state = BatchPlease.get_internal_state(pid)
      assert(state.counts.batch_items == 2)
      BatchPlease.add_item(pid, 3)
      state = BatchPlease.get_internal_state(pid)
      assert(state.counts.batch_items == 1)
      GenServer.stop(pid)
    end

    it "respects max_time_since_last_flush" do
      {:ok, pid} = GenServer.start_link(TestBatcher,
        max_batch_size: nil, max_time_since_last_flush: 500)

      BatchPlease.add_item(pid, 1)
      BatchPlease.add_item(pid, 2)
      state = BatchPlease.get_internal_state(pid)
      assert(state.counts.batch_items == 2)

      :timer.sleep(1000)

      BatchPlease.add_item(pid, 3)
      state = BatchPlease.get_internal_state(pid)
      assert(state.counts.batch_items == 1)

      GenServer.stop(pid)
    end

    it "respects max_time_since_first_item" do
      {:ok, pid} = GenServer.start_link(TestBatcher,
        max_batch_size: nil, max_time_since_first_item: 500)

      BatchPlease.add_item(pid, 1)

      :timer.sleep(1000)

      BatchPlease.add_item(pid, 2)
      state = BatchPlease.get_internal_state(pid)
      assert(state.counts.batch_items == 1)

      GenServer.stop(pid)
    end
  end


  defp make_listener(parent_pid) do
    spawn_link fn ->
      receive do
        :hi -> send(parent_pid, :ok)
      end
    end
  end

  context "overrides" do
    it "respects batch_init" do
      listener = make_listener(self)
      {:ok, pid} = GenServer.start_link(TestBatcher,
        batch_init: fn (_) -> send(listener, :hi); {:ok, %{items: []}} end)
      BatchPlease.add_item(pid, 1)
      receive do
        :ok -> assert(true)
      after
        1000 -> assert(false, "never received reply from listener")
      end
      GenServer.stop(pid)
    end

    it "respects batch_add_item" do
      listener = make_listener(self)
      {:ok, pid} = GenServer.start_link(TestBatcher,
        batch_add_item: fn (batch, _) -> send(listener, :hi); {:ok, batch} end)
      BatchPlease.add_item(pid, 1)
      receive do
        :ok -> assert(true)
      after
        1000 -> assert(false, "never received reply from listener")
      end
      GenServer.stop(pid)
    end

    it "respects batch_pre_process" do
      listener = make_listener(self)
      {:ok, pid} = GenServer.start_link(TestBatcher,
        batch_pre_process: fn (batch) -> send(listener, :hi); {:ok, batch} end)
      BatchPlease.add_item(pid, 1)
      BatchPlease.add_item(pid, 2)
      BatchPlease.add_item(pid, 3)
      receive do
        :ok -> assert(true)
      after
        1000 -> assert(false, "never received reply from listener")
      end
      GenServer.stop(pid)
    end

    it "respects batch_process" do
      listener = make_listener(self)
      {:ok, pid} = GenServer.start_link(TestBatcher,
        batch_process: fn (_batch) -> send(listener, :hi); :ok end)
      BatchPlease.add_item(pid, 1)
      BatchPlease.add_item(pid, 2)
      BatchPlease.add_item(pid, 3)
      receive do
        :ok -> assert(true)
      after
        1000 -> assert(false, "never received reply from listener")
      end
      GenServer.stop(pid)
    end

    it "respects batch_post_process" do
      listener = make_listener(self)
      {:ok, pid} = GenServer.start_link(TestBatcher,
        batch_post_process: fn (_batch) -> send(listener, :hi); :ok end)
      BatchPlease.add_item(pid, 1)
      BatchPlease.add_item(pid, 2)
      BatchPlease.add_item(pid, 3)
      receive do
        :ok -> assert(true)
      after
        1000 -> assert(false, "never received reply from listener")
      end
      GenServer.stop(pid)
    end

    it "respects batch_terminate" do
      listener = make_listener(self)
      {:ok, pid} = GenServer.start_link(TestBatcher,
        batch_terminate: fn (_batch) -> send(listener, :hi); :ok end)
      BatchPlease.add_item(pid, 1)
      BatchPlease.add_item(pid, 2)
      BatchPlease.add_item(pid, 3)
      GenServer.stop(pid)
      receive do
        :ok -> assert(true)
      after
        1000 -> assert(false, "never received reply from listener")
      end
    end

    it "respects should_flush" do
      listener = make_listener(self)
      {:ok, pid} = GenServer.start_link(TestBatcher,
        should_flush: fn (_batch) -> send(listener, :hi); false end)
      BatchPlease.add_item(pid, 1)
      receive do
        :ok -> assert(true)
      after
        1000 -> assert(false, "never received reply from listener")
      end
      GenServer.stop(pid)
    end
  end
end

