# BatchPlease

BatchPlease is an Elixir library for collecting batches of items,
and doing something with each batch when it reaches a certain size
or age.

## Should I use this in production?

As of version 0.2.x: no, not yet.

## Installation

1. Add `batch_please` to your list of dependencies in `mix.exs`:

  ```elixir
  def deps do
    [{:batch_please, "~> 0.2.0"}]
  end
  ```

2. Ensure `batch_please` is started before your application:

  ```elixir
  def application do
    [applications: [:batch_please]]
  end
  ```

## Copyright and License

Copyright (c) 2017, Appcues, Inc.  All rights reversed.

This software is released under the
[MIT License](https://opensource.org/licenses/MIT)
and is offered without warranty or guarantee of any kind.

