defmodule BatchPlease.MemoryBatcherTest do
  use ExSpec, async: true
  doctest BatchPlease.MemoryBatcher

  defmodule NoBehaviourTest do
    use BatchPlease.MemoryBatcher
  end
end

