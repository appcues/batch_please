defmodule BatchPleaseTest do
  use ExUnit.Case
  doctest BatchPlease

  test "the truth" do
    assert 1 + 1 == 2
  end
end

defmodule Poop do
  def poop1 do
    quote do
      use Butts
      use Poopers
    end
  end

  def poop2 do
    quote do
      @behaviour Butts
      @tag lols
    end
  end

  defmacro poop do
    poop1() ++ poop2()
  end
end

