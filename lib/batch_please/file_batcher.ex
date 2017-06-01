defmodule BatchPlease.FileBatcher do

  @typedoc ~S"""
  The state of a single FileBatcher batch.

  `filename` is a string holding the name of the file containing items
  from the current batch.

  `file` is a filehandle to the current batch file.

  `encode` is a function which takes an item as input and encodes it
  into string format.  It returns either `{:ok, encoded_item}` or
  {:error, message}`.  Defaults to `Poison.encode/1`.

  `decode` is a function which takes a string-encoded item as input
  and decodes it, returning either `{:ok, item}` or `{:error, message}`.
  Defaults to `Poison.decode/1`.
  """
  @type batch :: %{
    filename: String.t,
    file: File.io_device,
    encode: ((item) -> {:ok, binary} | {:error, String.t}),
  }

  @typedoc ~S"""
  Items can be of any type that is representable with the given
  `encode` and `decode` functions (which, by default, use JSON).
  """
  @type item :: any


  @doc ~S"""
  Callback to create a new FileBatcher batch.

  `opts[:batch_directory]` can be used to specify where to put batch files
  (default `/tmp`).

  `opts[:encode]` can be used to manually specify an encoder function.
  """
  def batch_init(opts) do
    dir = (opts[:batch_directory] || "/tmp") |> String.replace_trailing("/", "")
    filename = make_filename(dir)

    with :ok <- File.mkdir_p!(dir),
         {:ok, file} <- File.open(filename, [:write])
    do
      {:ok, %{
        filename: filename,
        file: file,
        encode: opts[:encode],
      }}
    end
  end

  @doc false
  def batch_add_item(batch, item) do
    with {:ok, enc_item} <- do_encode(batch, item),
         encoded_item <- String.replace_trailing(enc_item, "\n", ""),
         :ok <- IO.binwrite(batch.file, encoded_item <> "\n")
    do
      {:ok, batch}
    end
  end

  @doc false
  def batch_pre_process(batch) do
    with :ok <- File.close(batch.file)
    do
      {:ok, batch}
    end
  end

  @doc false
  def batch_post_process(batch) do
    File.rm(batch.filename)
  end

  defp do_encode(batch, item) do
    cond do
      batch.encode ->
        batch.encode.(item)
      {:module, _} = Code.ensure_loaded(Poison) ->
        Poison.encode(item)
      :else ->
        raise UndefinedFunctionError, message: "no `encode` function was provided, and `Poison.encode/1` is not available"
    end
  end

  defp make_filename(dir) do
    "#{dir}/#{:erlang.system_time(:milli_seconds)}.batch"
  end

  defmacro __using__(opts) do
    quote do
      use BatchPlease, unquote(opts)
      def batch_init(opts), do: BatchPlease.FileBatcher.batch_init(opts)
      def batch_add_item(batch, item), do: BatchPlease.FileBatcher.batch_add_item(batch, item)
      def batch_pre_process(batch), do: BatchPlease.FileBatcher.batch_pre_process(batch)
      def batch_post_process(batch), do: BatchPlease.FileBatcher.batch_post_process(batch)
    end
  end
end

