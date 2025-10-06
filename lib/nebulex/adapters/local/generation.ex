defmodule Nebulex.Adapters.Local.Generation do
  @moduledoc """
  This module implements the generation manager and garbage collector.

  The generational garbage collector manages the heap as several sub-heaps,
  known as generations, based on the age of the objects. An object is allocated
  in the youngest generation, sometimes called the nursery, and is promoted to
  an older generation if its lifetime exceeds the threshold of its current
  generation (defined by option `:gc_interval`). Every time the GC runs, a new
  cache generation is created, and the oldest one is deleted.

  The GC is triggered upon the following events:

    * When the `:gc_interval` times outs.
    * When the scheduled memory check times out. The memory check is executed
      when the `:max_size` or `:allocated_memory` options (or both) are
      configured. Furthermore, the memory check interval is determined by
      option `:gc_memory_check_interval`.

  The oldest generation is deleted in two steps. First, the underlying ETS table
  is flushed to release space and only marked for deletion as there may still be
  processes referencing it. The actual deletion of the ETS table happens at the
  next GC run. However, flushing is a blocking operation. Once started,
  processes wanting to access the table must wait until it finishes.
  To circumvent this, the flush can be delayed by configuring `:gc_cleanup_delay`
  to allow time for these processes to complete their work without being
  blocked.
  """

  # Internal state
  defstruct cache: nil,
            name: nil,
            telemetry: nil,
            telemetry_prefix: nil,
            meta_tab: nil,
            backend: nil,
            backend_opts: nil,
            stats_counter: nil,
            gc_interval: nil,
            gc_heartbeat_ref: nil,
            max_size: nil,
            allocated_memory: nil,
            gc_memory_check_interval: nil,
            gc_healthcheck_ref: nil,
            gc_cleanup_delay: nil

  use GenServer

  alias Nebulex.Adapter
  alias Nebulex.Adapters.Common.Info.Stats
  alias Nebulex.Adapters.Local.{Backend, Metadata, Options}
  alias Nebulex.Telemetry

  @type t() :: %__MODULE__{}
  @type server_ref() :: pid() | atom() | :ets.tid()
  @type opts() :: Nebulex.Cache.opts()

  ## API

  @doc """
  Starts the garbage collector for the built-in local cache adapter.
  """
  @spec start_link(opts()) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @doc """
  Creates a new cache generation. Once the max number of generations
  is reached, when a new generation is created, the oldest one is
  deleted.

  ## Options

  #{Nebulex.Adapters.Local.Options.gc_runtime_options_docs()}

  ## Example

      Nebulex.Adapters.Local.Generation.new(MyCache)

      Nebulex.Adapters.Local.Generation.new(MyCache, gc_interval_reset: false)

  """
  @spec new(server_ref(), opts()) :: [atom()]
  def new(server_ref, opts \\ []) do
    # Validate options
    opts = Options.validate_gc_runtime_opts!(opts)

    do_call(server_ref, {:new_generation, Keyword.fetch!(opts, :gc_interval_reset)})
  end

  @doc """
  Removes or flushes all entries from the cache (including all its generations).

  ## Example

      Nebulex.Adapters.Local.Generation.delete_all(MyCache)

  """
  @spec delete_all(server_ref()) :: :ok
  def delete_all(server_ref) do
    do_call(server_ref, :delete_all)
  end

  @doc """
  Reallocates the block of memory that was previously allocated for the given
  `server_ref` with the new `size`. In other words, reallocates the max memory
  size for a cache generation.

  ## Example

      Nebulex.Adapters.Local.Generation.realloc(MyCache, 1_000_000)

  """
  @spec realloc(server_ref(), pos_integer()) :: :ok
  def realloc(server_ref, size) do
    do_call(server_ref, {:realloc, size})
  end

  @doc """
  Returns the memory info in a tuple form `{used_mem, total_mem}`.

  ## Example

      Nebulex.Adapters.Local.Generation.memory_info(MyCache)

  """
  @spec memory_info(server_ref()) :: {used_mem :: non_neg_integer(), total_mem :: non_neg_integer()}
  def memory_info(server_ref) do
    do_call(server_ref, :memory_info)
  end

  @doc """
  Resets the GC interval.

  ## Example

      Nebulex.Adapters.Local.Generation.reset_gc_interval(MyCache)

  """
  @spec reset_gc_interval(server_ref()) :: :ok
  def reset_gc_interval(server_ref) do
    server_ref
    |> server()
    |> GenServer.cast(:gc_interval_reset)
  end

  @doc """
  Returns the list of the generations in the form `[newer, older]`.

  ## Example

      Nebulex.Adapters.Local.Generation.list(MyCache)

  """
  @spec list(server_ref()) :: [:ets.tid()]
  def list(server_ref) do
    server_ref
    |> get_meta_tab()
    |> Metadata.get(:generations, [])
  end

  @doc """
  Returns the newer generation.

  ## Example

      Nebulex.Adapters.Local.Generation.newer(MyCache)

  """
  @spec newer(server_ref()) :: :ets.tid()
  def newer(server_ref) do
    server_ref
    |> get_meta_tab()
    |> Metadata.get(:generations, [])
    |> hd()
  end

  @doc """
  Returns the PID of the GC server for the given `server_ref`.

  ## Example

      Nebulex.Adapters.Local.Generation.server(MyCache)

  """
  @spec server(server_ref()) :: pid
  def server(server_ref) do
    server_ref
    |> get_meta_tab()
    |> Metadata.fetch!(:gc_pid)
  end

  @doc """
  A convenience function for retrieving the state.
  """
  @spec get_state(server_ref()) :: t()
  def get_state(server_ref) do
    server_ref
    |> server()
    |> GenServer.call(:get_state)
  end

  defp do_call(tab, message) do
    tab
    |> server()
    |> GenServer.call(message)
  end

  defp get_meta_tab(server_ref) when is_atom(server_ref) or is_pid(server_ref) do
    server_ref
    |> Adapter.lookup_meta()
    |> Map.fetch!(:meta_tab)
  end

  defp get_meta_tab(server_ref), do: server_ref

  ## GenServer Callbacks

  @impl true
  def init(opts) do
    # Trap exit signals to run eviction tasks
    _ = Process.flag(:trap_exit, true)

    # Get adapter metadata
    adapter_meta = Keyword.fetch!(opts, :adapter_meta)

    # Add the GC PID to the meta table
    meta_tab = Map.fetch!(adapter_meta, :meta_tab)
    :ok = Metadata.put(meta_tab, :gc_pid, self())

    # Initial state
    state =
      struct(
        __MODULE__,
        opts
        |> Map.new()
        |> Map.merge(adapter_meta)
      )

    # Timer ref
    {:ok, ref} =
      if state.gc_interval,
        do: {new_gen(state), start_timer(state.gc_interval)},
        else: {new_gen(state), nil}

    # Update state
    state = %{state | gc_heartbeat_ref: ref}

    {:ok, state, {:continue, :setup_mem_check_interval}}
  end

  @impl true
  def handle_continue(
        :setup_mem_check_interval,
        %__MODULE__{
          max_size: max_size,
          allocated_memory: allocated_memory,
          gc_memory_check_interval: mem_check_interval
        } = state
      ) do
    # Init healthcheck timer
    healthcheck_ref =
      cond do
        not is_nil(max_size) ->
          eval_mem_check_interval(:size, 0, max_size, nil, mem_check_interval)

        not is_nil(allocated_memory) ->
          eval_mem_check_interval(:memory, 0, allocated_memory, nil, mem_check_interval)

        true ->
          nil
      end

    {:noreply, %{state | gc_healthcheck_ref: healthcheck_ref}}
  end

  @impl true
  def terminate(_reason, state) do
    if ref = state.stats_counter, do: Telemetry.detach(ref)
  end

  @impl true
  def handle_call(:delete_all, _from, %__MODULE__{} = state) do
    :ok = new_gen(state)

    :ok =
      state.meta_tab
      |> list()
      |> Enum.each(&state.backend.delete_all_objects(&1))

    {:reply, :ok, %{state | gc_heartbeat_ref: maybe_reset_heartbeat(true, state)}}
  end

  def handle_call({:new_generation, gc_interval_reset?}, _from, state) do
    # Create new generation
    :ok = new_gen(state)

    # Maybe reset heartbeat timer
    heartbeat_ref = maybe_reset_heartbeat(gc_interval_reset?, state)

    {:reply, :ok, %{state | gc_heartbeat_ref: heartbeat_ref}}
  end

  def handle_call(
        :memory_info,
        _from,
        %__MODULE__{backend: backend, meta_tab: meta_tab, allocated_memory: allocated} = state
      ) do
    {:reply, {memory_info(backend, meta_tab), allocated}, state}
  end

  def handle_call({:realloc, mem_size}, _from, state) do
    {:reply, :ok, %{state | allocated_memory: mem_size}}
  end

  def handle_call(:get_state, _from, state) do
    {:reply, state, state}
  end

  @impl true
  def handle_cast(:gc_interval_reset, state) do
    {:noreply, %{state | gc_heartbeat_ref: maybe_reset_heartbeat(true, state)}}
  end

  @impl true
  def handle_info(
        :heartbeat,
        %__MODULE__{
          gc_interval: gc_interval,
          gc_heartbeat_ref: heartbeat_ref
        } = state
      ) do
    # Create new generation
    :ok = new_gen(state)

    # Reset heartbeat timer
    heartbeat_ref = start_timer(gc_interval, heartbeat_ref)

    {:noreply, %{state | gc_heartbeat_ref: heartbeat_ref}}
  end

  def handle_info(:healthcheck, state) do
    # Check size first, if the healthcheck is done, skip checking the memory;
    # otherwise, check the memory too.
    {_, state} =
      with {false, state} <- check_size(state) do
        check_memory(state)
      end

    {:noreply, state}
  end

  def handle_info(
        {:cleanup_older_gen, gen_tab},
        %__MODULE__{
          meta_tab: meta_tab,
          backend: backend
        } = state
      ) do
    _ = Backend.delete(backend, meta_tab, gen_tab)

    {:noreply, state}
  end

  ## Private Functions

  defp start_timer(time, ref \\ nil, event \\ :heartbeat)

  defp start_timer(nil, _, _) do
    nil
  end

  defp start_timer(time, ref, event) do
    _ = if ref, do: Process.cancel_timer(ref)

    Process.send_after(self(), event, time)
  end

  defp maybe_reset_heartbeat(_, %__MODULE__{gc_interval: nil} = state) do
    state.gc_heartbeat_ref
  end

  defp maybe_reset_heartbeat(false, state) do
    state.gc_heartbeat_ref
  end

  defp maybe_reset_heartbeat(true, %__MODULE__{} = state) do
    start_timer(state.gc_interval, state.gc_heartbeat_ref)
  end

  defp new_gen(%__MODULE__{
         meta_tab: meta_tab,
         backend: backend,
         backend_opts: backend_opts,
         stats_counter: stats_counter,
         gc_cleanup_delay: gc_cleanup_delay
       }) do
    # Create new generation
    gen_tab = Backend.new(backend, meta_tab, backend_opts)

    # Update generation list
    case list(meta_tab) do
      [newer, older] ->
        # Update generations
        :ok = Metadata.put(meta_tab, :generations, [gen_tab, newer])

        # Schedule cleanup of older generation to give grace period for ongoing
        # operations
        _ref = Process.send_after(self(), {:cleanup_older_gen, older}, gc_cleanup_delay)

        # Since the older generation is deleted, update evictions count
        :ok = Stats.incr(stats_counter, :evictions, backend.info(older, :size))

      [newer] ->
        # Update generations
        :ok = Metadata.put(meta_tab, :generations, [gen_tab, newer])

      [] ->
        # update generations
        :ok = Metadata.put(meta_tab, :generations, [gen_tab])
    end
  end

  defp check_size(%__MODULE__{max_size: max_size} = state) when not is_nil(max_size) do
    maybe_run_eviction(:size, state)
  end

  defp check_size(state) do
    {false, state}
  end

  defp check_memory(%__MODULE__{allocated_memory: allocated} = state) when not is_nil(allocated) do
    maybe_run_eviction(:memory, state)
  end

  defp check_memory(state) do
    {false, state}
  end

  defp maybe_run_eviction(
         info,
         %__MODULE__{
           cache: cache,
           name: name,
           gc_healthcheck_ref: healthcheck_ref,
           gc_interval: gc_interval,
           gc_heartbeat_ref: heartbeat_ref,
           gc_memory_check_interval: mem_check_interval
         } = state
       ) do
    case eviction_info(info, state) do
      {size, max_size} when size >= max_size ->
        # Create a new generation
        :ok = new_gen(state)

        # Delete expired entries
        _ = cache.delete_all(name, [query: :expired], [])

        # Reset the heartbeat timer
        heartbeat_ref = start_timer(gc_interval, heartbeat_ref)

        # Since the eviction has already been done, recalculate the info
        {size, max_size} = eviction_info(info, state)

        # Reset the healthcheck timer
        healthcheck_ref =
          eval_mem_check_interval(info, size, max_size, healthcheck_ref, mem_check_interval)

        {true, %{state | gc_heartbeat_ref: heartbeat_ref, gc_healthcheck_ref: healthcheck_ref}}

      {size, max_size} ->
        # Reset the healthcheck timer
        healthcheck_ref =
          eval_mem_check_interval(info, size, max_size, healthcheck_ref, mem_check_interval)

        {false, %{state | gc_healthcheck_ref: healthcheck_ref}}
    end
  end

  defp eval_mem_check_interval(_info, _size, _max_size, timer_ref, timeout)
       when is_integer(timeout) do
    start_timer(timeout, timer_ref, :healthcheck)
  end

  defp eval_mem_check_interval(info, size, max_size, timer_ref, timeout)
       when is_function(timeout, 3) do
    timeout.(info, size, max_size)
    |> start_timer(timer_ref, :healthcheck)
  end

  defp eviction_info(:size, %__MODULE__{backend: mod, meta_tab: tab, max_size: max}) do
    {size_info(mod, tab), max}
  end

  defp eviction_info(:memory, %__MODULE__{backend: mod, meta_tab: tab, allocated_memory: max}) do
    {memory_info(mod, tab), max}
  end

  defp size_info(backend, meta_tab) do
    meta_tab
    |> list()
    |> Enum.reduce(0, &(backend.info(&1, :size) + &2))
  end

  defp memory_info(backend, meta_tab) do
    meta_tab
    |> list()
    |> Enum.reduce(0, fn gen, acc ->
      gen
      |> backend.info(:memory)
      |> Kernel.*(:erlang.system_info(:wordsize))
      |> Kernel.+(acc)
    end)
  end
end
