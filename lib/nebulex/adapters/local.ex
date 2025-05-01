defmodule Nebulex.Adapters.Local do
  @moduledoc """
  A Local Generation Cache adapter for Nebulex; inspired by
  [epocxy cache](https://github.com/duomark/epocxy/blob/master/src/cxy_cache.erl).

  Generational caching using an ETS table (or multiple ones when used with
  `:shards`) for each generation of cached data. Accesses hit the newer
  generation first, and migrate from the older generation to the newer
  generation when retrieved from the stale table. When a new generation
  is started, the oldest one is deleted. This is a form of mass garbage
  collection which avoids using timers and expiration of individual
  cached elements.

  This implementation of generation cache uses only two generations, referred
  to as the `new` and the `old` generation.

  See `Nebulex.Adapters.Local.Generation` to learn more about generation
  management and garbage collection.

  ## Overall features

    * Configurable backend (`ets` or `:shards`).
    * Expiration - A status based on TTL (Time To Live) option. To maintain
      cache performance, expired entries may not be immediately removed or
      evicted, they are expired or evicted on-demand, when the key is read.
    * Eviction - [Generational Garbage Collection][gc].
    * Sharding - For intensive workloads, the Cache may also be partitioned
      (by using `:shards` backend and specifying the `:partitions` option).
    * Support for transactions via Erlang global name registration facility.
      See `Nebulex.Adapter.Transaction`.
    * Support for stats.

  [gc]: http://hexdocs.pm/nebulex/3.0.0-rc.1/Nebulex.Adapters.Local.Generation.html

  ## Configuration options

  The following options can be used to configure the adapter:

  #{Nebulex.Adapters.Local.Options.adapter_options_docs()}

  ## Usage

  `Nebulex.Cache` is the wrapper around the cache. We can define a
  local cache as follows:

      defmodule MyApp.LocalCache do
        use Nebulex.Cache,
          otp_app: :my_app,
          adapter: Nebulex.Adapters.Local
      end

  Where the configuration for the cache must be in your application
  environment, usually defined in your `config/config.exs`:

      config :my_app, MyApp.LocalCache,
        gc_interval: :timer.hours(12),
        max_size: 1_000_000,
        allocated_memory: 2_000_000_000,
        gc_memory_check_interval: :timer.seconds(10)

  For intensive workloads, the Cache may also be partitioned using `:shards`
  as cache backend (`backend: :shards`) and configuring the desired number of
  partitions via the `:partitions` option. Defaults to
  `System.schedulers_online()`.

      config :my_app, MyApp.LocalCache,
        backend: :shards,
        gc_interval: :timer.hours(12),
        max_size: 1_000_000,
        allocated_memory: 2_000_000_000,
        gc_memory_check_interval: :timer.seconds(10)
        partitions: System.schedulers_online() * 2

  If your application was generated with a supervisor (by passing `--sup`
  to `mix new`) you will have a `lib/my_app/application.ex` file containing
  the application start callback that defines and starts your supervisor.
  You just need to edit the `start/2` function to start the cache as a
  supervisor on your application's supervisor:

      def start(_type, _args) do
        children = [
          {MyApp.LocalCache, []},
          ...
        ]

  See `Nebulex.Cache` for more information.

  ## The `:ttl` option

  The `:ttl` is a runtime option meant to set a key's expiration time. It is
  evaluated on-demand when a key is retrieved, and if it has expired, it is
  removed from the cache. Hence, it can not be used as an eviction method;
  it is more for maintaining the cache's integrity and consistency. For this
  reason, you should always configure the eviction or GC options. See the
  ["Eviction policy"](#module-eviction-policy) section for more information.

  ### Caveats when using `:ttl` option:

    * When using the `:ttl` option, ensure it is less than `:gc_interval`.
      Otherwise, the key may be evicted, and the `:ttl` hasn't happened yet
      because the garbage collector may run before a fetch operation has
      evaluated the `:ttl` and expired the key.
    * Consider the following scenario based on the previous caveat. You have
      `:gc_interval` set to 1 hrs. Then you put a new key with `:ttl` set to
      2 hrs. One minute later, the GC runs, creating a new generation, and the
      key ends up in the older generation. Therefore, if the next GC cycle
      occurs (1 hr later) before the key is fetched (moving it to the newer
      generation), it is evicted from the cache when the GC removes the older
      generation so it won't be retrievable anymore.

  ## Eviction policy

  This adapter implements a generational cache, which means its primary eviction
  mechanism pushes a new cache generation and removes the oldest one. This
  mechanism ensures the garbage collector removes the least frequently used keys
  when it runs and deletes the oldest generation. At the same time, only the
  most frequently used keys are always available in the newer generation. In
  other words, the generation cache also enforces an LRU (Least Recently Used)
  eviction policy.

  The following conditions trigger the garbage collector to run:

    * When the time interval defined by `:gc_interval` is completed. This
      makes the garbage-collector process to run creating a new generation
      and forcing to delete the oldest one. This interval defines how often
      you want to evict the least frequently used entries or the retention
      period for the cached entries. The retention period for the least
      frequently used entries is equivalent to two garbage collection cycles
      (since we keep two generations), which means the GC removes all entries
      not accessed in the cache during that time.

    * When the time interval defined by `:gc_memory_check_interval` is
      completed. Beware: This option works alongside the `:max_size` and
      `:allocated_memory` options. The interval defines when the GC must
      run to validate the cache size and memory and release space if any
      of the limits are exceeded. It is mainly for keeping the cached data
      under the configured memory size limits and avoiding running out of
      memory at some point.

  ### Configuring the GC options

  This section helps you understand how the different configuration options work
  and gives you an idea of what values to set, especially if this is your first
  time using Nebulex with the local adapter.

  Understanding a few things in advance is essential to configure the cache
  with appropriate values. For example, the average size of an entry so we
  can configure a reasonable value for the max size or allocated memory.
  Also, the reads and writes load. The problem is that sometimes it is
  challenging to have this information in advance, especially when it is
  a new app or when we use the cache for the first time. The following
  are tips to help you to configure the cache (especially if it is your
  for the first time):

    * To configure the GC, consider the retention period for the least
      frequently used entries you desire. For example, if the GC is 1 hr, you
      will keep only those entries accessed periodically during the last 2 hrs
      (two GC cycles, as outlined above). If it is your first time using the
      local adapter, you may start configuring the `:gc_interval` to 12 hrs to
      ensure daily data retention. Then, you can analyze the data and change the
      value based on your findings.

    * Configure the `:max_size` or `:allocated_memory` option (or both) to keep
      memory healthy under the given limits (avoid running out of memory).
      Configuring these options will ensure the GC releases memory space
      whenever a limit is reached or exceeded. For example, one may assign 50%
      of the total memory to the `:allocated_memory`. It depends on how much
      memory you need and how much your app needs to run. For the `:max_size`,
      consider how many entries you expect to keep in the cache; you could start
      with something between `100_000` and `1_000_000`.

    * Finally, when configuring `:max_size` or `:allocated_memory` (or both),
      you must also configure `:gc_memory_check_interval` (defaults to 10 sec).
      By default, the GC will run every 10 seconds to validate the cache size
      and memory.

  ## Queryable API

  Since the adapter implementation uses ETS tables underneath, the query must be
  a valid [**ETS Match Spec**](https://www.erlang.org/doc/man/ets#match_spec).
  However, there are some predefined or shorthand queries you can use. See the
  ["Predefined queries"](#module-predefined-queries) section for information.

  The adapter defines an entry as a tuple `{:entry, key, value, touched, ttl}`,
  meaning the match pattern within the ETS Match Spec must be like
  `{:entry, :"$1", :"$2", :"$3", :"$4"}`. To make query building easier,
  you can use the `Ex2ms` library.

      iex> match_spec = [
      ...>   {
      ...>     {:entry, :"$1", :"$2", :_, :_},
      ...>     [{:>, :"$2", 1}],
      ...>     [{{:"$1", :"$2"}}]
      ...>   }
      ...> ]
      iex> MyCache.get_all(query: match_spec)
      {:ok, [b: 1, c: 3]}

  > You can use the `Ex2ms` or `MatchSpec` library to build queries easier.

  ## Transaction API

  This adapter inherits the default implementation provided by
  `Nebulex.Adapter.Transaction`. Therefore, the `transaction` command accepts
  the following options:

  #{Nebulex.Adapter.Transaction.Options.options_docs()}

  ## Extended API (extra functions)

  This adapter provides some additional convenience functions to the
  `Nebulex.Cache` API.

  Creating new generations:

      MyCache.new_generation()
      MyCache.new_generation(gc_interval_reset: false)

  Retrieving the current generations:

      MyCache.generations()

  Retrieving the newer generation:

      MyCache.newer_generation()

  """

  # Provide Cache Implementation
  @behaviour Nebulex.Adapter
  @behaviour Nebulex.Adapter.KV
  @behaviour Nebulex.Adapter.Queryable

  # Inherit default transaction implementation
  use Nebulex.Adapter.Transaction

  # Inherit default info implementation
  use Nebulex.Adapters.Common.Info

  # Inherit default observable implementation
  use Nebulex.Adapter.Observable

  import Nebulex.Utils
  import Record

  alias Nebulex.Adapters.Common.Info.Stats
  alias Nebulex.Adapters.Local.{Backend, Generation, Metadata}
  alias Nebulex.Time

  ## Types & Internal definitions

  @typedoc "Adapter's backend type"
  @type backend() :: :ets | :shards

  @typedoc """
  The type for the `:gc_memory_check_interval` option value.

  The `:gc_memory_check_interval` value can be:

    * A positive integer with the time in milliseconds.
    * An anonymous function to call in runtime and must return the next interval
      in milliseconds. The function receives three arguments:
      * The first argument is an atom indicating the limit, whether it is
        `:size` or `:memory`.
      * The second argument is the current value for the limit. For example,
        if the limit in the first argument is `:size`, the second argument tells
        the current cache size (number of entries in the cache). If the limit is
        `:memory`, it means the recent cache memory in bytes.
      * The third argument is the maximum limit provided in the configuration.
        When the limit in the first argument is `:size`, it is the `:max_size`.
        On the other hand, if the limit is `:memory`, it is the
        `:allocated_memory`.

  """
  @type mem_check_interval() ::
          pos_integer()
          | (limit :: :size | :memory, current :: non_neg_integer(), max :: non_neg_integer() ->
               timeout :: pos_integer())

  # Cache Entry
  defrecord(:entry,
    key: nil,
    value: nil,
    touched: nil,
    exp: nil
  )

  ## Nebulex.Adapter

  @impl true
  defmacro __before_compile__(_env) do
    quote do
      @doc """
      A convenience function for creating new generations.
      """
      def new_generation(opts \\ []) do
        Generation.new(get_dynamic_cache(), opts)
      end

      @doc """
      A convenience function for reset the GC interval.
      """
      def reset_gc_interval do
        Generation.reset_gc_interval(get_dynamic_cache())
      end

      @doc """
      A convenience function for retrieving the current generations.
      """
      def generations do
        Generation.list(get_dynamic_cache())
      end

      @doc """
      A convenience function for retrieving the newer generation.
      """
      def newer_generation do
        Generation.newer(get_dynamic_cache())
      end
    end
  end

  @impl true
  def init(opts) do
    # Validate options
    opts = __MODULE__.Options.validate_adapter_opts!(opts)

    # Required options
    cache = Keyword.fetch!(opts, :cache)
    telemetry = Keyword.fetch!(opts, :telemetry)
    telemetry_prefix = Keyword.fetch!(opts, :telemetry_prefix)

    # Init internal metadata table
    meta_tab = opts[:meta_tab] || Metadata.init()

    # Init stats_counter
    stats_counter =
      if Keyword.fetch!(opts, :stats) == true do
        Stats.init(telemetry_prefix)
      end

    # Resolve the backend to be used
    backend = Keyword.fetch!(opts, :backend)

    # Internal option for max nested match specs based on number of keys
    purge_chunk_size = Keyword.fetch!(opts, :purge_chunk_size)

    # Build adapter metadata
    adapter_meta = %{
      name: opts[:name] || cache,
      telemetry: telemetry,
      telemetry_prefix: telemetry_prefix,
      meta_tab: meta_tab,
      stats_counter: stats_counter,
      backend: backend,
      purge_chunk_size: purge_chunk_size,
      started_at: DateTime.utc_now()
    }

    # Build adapter child_spec
    child_spec = Backend.child_spec(backend, [adapter_meta: adapter_meta] ++ opts)

    {:ok, child_spec, adapter_meta}
  end

  ## Nebulex.Adapter.KV

  @impl true
  def fetch(%{name: name, meta_tab: meta_tab, backend: backend}, key, _opts) do
    meta_tab
    |> list_gen()
    |> do_fetch(name, backend, key)
    |> return(:value)
  end

  defp do_fetch([newer], name, backend, key) do
    fetch_entry(name, backend, newer, key)
  end

  defp do_fetch([newer, older], name, backend, key) do
    with {:error, _} <- fetch_entry(name, backend, newer, key),
         {:ok, cached} <- pop_entry(name, backend, older, key) do
      true = backend.insert(newer, cached)

      {:ok, cached}
    end
  end

  @impl true
  def put(adapter_meta, key, value, on_write, ttl, keep_ttl?, _opts) do
    now = Time.now()
    entry = entry(key: key, value: value, touched: now, exp: exp(now, ttl))

    do_put(on_write, adapter_meta.meta_tab, adapter_meta.backend, entry, keep_ttl?)
    |> wrap_ok()
  end

  defp do_put(:put, meta_tab, backend, entry, keep_ttl?) do
    put_entry(meta_tab, backend, entry, keep_ttl?)
  end

  defp do_put(:put_new, meta_tab, backend, entry, _keep_ttl?) do
    put_new_entries(meta_tab, backend, entry)
  end

  defp do_put(
         :replace,
         meta_tab,
         backend,
         entry(key: key, value: value, touched: touched, exp: exp),
         keep_ttl?
       ) do
    changes = if keep_ttl?, do: [], else: [{4, touched}, {5, exp}]

    update_entry(meta_tab, backend, key, [{3, value} | changes])
  end

  @impl true
  def put_all(adapter_meta, entries, on_write, ttl, _opts) do
    now = Time.now()
    exp = exp(now, ttl)

    do_put_all(
      on_write,
      adapter_meta.meta_tab,
      adapter_meta.backend,
      adapter_meta.purge_chunk_size,
      Enum.map(entries, &entry(key: elem(&1, 0), value: elem(&1, 1), touched: now, exp: exp))
    )
    |> wrap_ok()
  end

  defp do_put_all(:put, meta_tab, backend, chunk_size, entries) do
    put_entries(meta_tab, backend, entries, chunk_size)
  end

  defp do_put_all(:put_new, meta_tab, backend, chunk_size, entries) do
    put_new_entries(meta_tab, backend, entries, chunk_size)
  end

  @impl true
  def delete(adapter_meta, key, _opts) do
    adapter_meta.meta_tab
    |> list_gen()
    |> Enum.each(&adapter_meta.backend.delete(&1, key))
  end

  @impl true
  def take(%{name: name, meta_tab: meta_tab, backend: backend}, key, _opts) do
    meta_tab
    |> list_gen()
    |> Enum.reduce_while(nil, fn gen, _acc ->
      case pop_entry(name, backend, gen, key) do
        {:ok, res} -> {:halt, return({:ok, res}, :value)}
        error -> {:cont, error}
      end
    end)
  end

  @impl true
  def update_counter(
        %{name: name, meta_tab: meta_tab, backend: backend},
        key,
        amount,
        default,
        ttl,
        _opts
      ) do
    # Current time
    now = Time.now()

    # Verify if the key has expired
    _ =
      meta_tab
      |> list_gen()
      |> do_fetch(name, backend, key)

    # Run the counter operation
    meta_tab
    |> newer_gen()
    |> backend.update_counter(
      key,
      {3, amount},
      entry(key: key, value: default, touched: now, exp: exp(now, ttl))
    )
    |> wrap_ok()
  end

  @impl true
  def has_key?(adapter_meta, key, _opts) do
    case fetch(adapter_meta, key, []) do
      {:ok, _} -> {:ok, true}
      {:error, _} -> {:ok, false}
    end
  end

  @impl true
  def ttl(%{name: name, meta_tab: meta_tab, backend: backend}, key, _opts) do
    with {:ok, res} <- meta_tab |> list_gen() |> do_fetch(name, backend, key) do
      {:ok, entry_ttl(res)}
    end
  end

  defp entry_ttl(entry(exp: :infinity)), do: :infinity

  defp entry_ttl(entry(exp: exp)) do
    exp - Time.now()
  end

  defp entry_ttl(entries) when is_list(entries) do
    Enum.map(entries, &entry_ttl/1)
  end

  @impl true
  def expire(adapter_meta, key, ttl, _opts) do
    now = Time.now()

    adapter_meta.meta_tab
    |> update_entry(adapter_meta.backend, key, [{4, now}, {5, exp(now, ttl)}])
    |> wrap_ok()
  end

  @impl true
  def touch(adapter_meta, key, _opts) do
    adapter_meta.meta_tab
    |> update_entry(adapter_meta.backend, key, [{4, Time.now()}])
    |> wrap_ok()
  end

  ## Nebulex.Adapter.Queryable

  @impl true
  def execute(adapter_meta, query_meta, opts)

  def execute(_adapter_meta, %{op: :get_all, query: {:in, []}}, _opts) do
    {:ok, []}
  end

  def execute(_adapter_meta, %{op: op, query: {:in, []}}, _opts)
      when op in [:count_all, :delete_all] do
    {:ok, 0}
  end

  def execute(
        %{meta_tab: meta_tab, backend: backend},
        %{op: :count_all, query: {:q, nil}},
        _opts
      ) do
    meta_tab
    |> list_gen()
    |> Enum.reduce(0, fn gen, acc ->
      gen
      |> backend.info(:size)
      |> Kernel.+(acc)
    end)
    |> wrap_ok()
  end

  def execute(
        %{meta_tab: meta_tab} = adapter_meta,
        %{op: :delete_all, query: {:q, nil}} = query_spec,
        _opts
      ) do
    with {:ok, count_all} <- execute(adapter_meta, %{query_spec | op: :count_all}, []) do
      :ok = Generation.delete_all(meta_tab)

      {:ok, count_all}
    end
  end

  def execute(
        %{meta_tab: meta_tab, backend: backend},
        %{op: :count_all, query: {:in, keys}},
        opts
      )
      when is_list(keys) do
    chunk_size = Keyword.get(opts, :chunk_size, 10)

    meta_tab
    |> list_gen()
    |> Enum.reduce(0, fn gen, acc ->
      do_count_all(backend, gen, keys, chunk_size) + acc
    end)
    |> wrap_ok()
  end

  def execute(
        %{meta_tab: meta_tab, backend: backend},
        %{op: :delete_all, query: {:in, keys}},
        opts
      )
      when is_list(keys) do
    chunk_size = Keyword.get(opts, :chunk_size, 10)

    meta_tab
    |> list_gen()
    |> Enum.reduce(0, fn gen, acc ->
      do_delete_all(backend, gen, keys, chunk_size) + acc
    end)
    |> wrap_ok()
  end

  def execute(
        %{meta_tab: meta_tab, backend: backend},
        %{op: :get_all, query: {:in, keys}, select: select},
        opts
      )
      when is_list(keys) do
    chunk_size = Keyword.get(opts, :chunk_size, 10)

    meta_tab
    |> list_gen()
    |> Enum.reduce([], fn gen, acc ->
      do_get_all(backend, gen, keys, match_return(select), chunk_size) ++ acc
    end)
    |> wrap_ok()
  end

  def execute(
        %{meta_tab: meta_tab, backend: backend},
        %{op: op, query: {:q, ms}, select: select},
        _opts
      ) do
    ms =
      ms
      |> assert_match_spec(select)
      |> maybe_match_spec_return_true(op)

    {reducer, acc_in} =
      case op do
        :get_all -> {&(backend.select(&1, ms) ++ &2), []}
        :count_all -> {&(backend.select_count(&1, ms) + &2), 0}
        :delete_all -> {&(backend.select_delete(&1, ms) + &2), 0}
      end

    meta_tab
    |> list_gen()
    |> Enum.reduce(acc_in, reducer)
    |> wrap_ok()
  end

  @impl true
  def stream(adapter_meta, query_meta, opts)

  def stream(%{meta_tab: meta_tab, backend: backend}, %{query: {:in, keys}, select: select}, opts) do
    keys
    |> Stream.chunk_every(Keyword.fetch!(opts, :max_entries))
    |> Stream.map(fn chunk ->
      meta_tab
      |> list_gen()
      |> Enum.reduce([], &(backend.select(&1, in_match_spec(chunk, select)) ++ &2))
    end)
    |> Stream.flat_map(& &1)
    |> wrap_ok()
  end

  def stream(adapter_meta, %{query: {:q, ms}, select: select}, opts) do
    adapter_meta
    |> do_stream(
      assert_match_spec(ms, select),
      Keyword.get(opts, :max_entries, 20)
    )
    |> wrap_ok()
  end

  defp do_stream(%{meta_tab: meta_tab, backend: backend}, match_spec, page_size) do
    Stream.resource(
      fn ->
        [newer | _] = generations = list_gen(meta_tab)

        {backend.select(newer, match_spec, page_size), generations}
      end,
      fn
        {:"$end_of_table", [_gen]} ->
          {:halt, []}

        {:"$end_of_table", [_gen | generations]} ->
          result =
            generations
            |> hd()
            |> backend.select(match_spec, page_size)

          {[], {result, generations}}

        {{elements, cont}, [_ | _] = generations} ->
          {elements, {backend.select(cont), generations}}
      end,
      & &1
    )
  end

  ## Nebulex.Adapter.Info

  @impl true
  def info(adapter_meta, spec, opts)

  def info(%{meta_tab: meta_tab} = adapter_meta, :all, opts) do
    with {:ok, base_info} <- super(adapter_meta, :all, opts) do
      {:ok, Map.merge(base_info, %{memory: memory_info(meta_tab)})}
    end
  end

  def info(%{meta_tab: meta_tab}, :memory, _opts) do
    {:ok, memory_info(meta_tab)}
  end

  def info(adapter_meta, spec, opts) when is_list(spec) do
    Enum.reduce(spec, {:ok, %{}}, fn s, {:ok, acc} ->
      {:ok, info} = info(adapter_meta, s, opts)

      {:ok, Map.put(acc, s, info)}
    end)
  end

  def info(adapter_meta, spec, opts) do
    super(adapter_meta, spec, opts)
  end

  defp memory_info(meta_tab) do
    {mem_size, max_size} = Generation.memory_info(meta_tab)

    %{total: max_size, used: mem_size}
  end

  ## Helpers

  # Inline common instructions
  @compile {:inline, fetch_entry: 4, pop_entry: 4, list_gen: 1, newer_gen: 1, match_key: 2}

  defmacrop backend_call(name, backend, tab, fun, key) do
    quote do
      case unquote(backend).unquote(fun)(unquote(tab), unquote(key)) do
        [] ->
          wrap_error Nebulex.KeyError, key: unquote(key), cache: unquote(name)

        [entry(exp: :infinity) = entry] ->
          {:ok, entry}

        [entry() = entry] ->
          validate_exp(entry, unquote(backend), unquote(tab), unquote(name))

        entries when is_list(entries) ->
          now = Time.now()

          entries =
            for entry(touched: touched, exp: exp) = e <- entries, now < exp, do: e

          {:ok, entries}
      end
    end
  end

  defp fetch_entry(name, backend, tab, key) do
    backend_call(name, backend, tab, :lookup, key)
  end

  defp pop_entry(name, backend, tab, key) do
    backend_call(name, backend, tab, :take, key)
  end

  defp list_gen(meta_tab) do
    Metadata.fetch!(meta_tab, :generations)
  end

  defp newer_gen(meta_tab) do
    meta_tab
    |> Metadata.fetch!(:generations)
    |> hd()
  end

  defp validate_exp(entry(key: key, exp: exp) = entry, backend, tab, name) do
    if Time.now() >= exp do
      true = backend.delete(tab, key)

      wrap_error Nebulex.KeyError, key: key, cache: name, reason: :expired
    else
      {:ok, entry}
    end
  end

  defp exp(_now, :infinity), do: :infinity
  defp exp(now, ttl), do: now + ttl

  defp put_entry(
         meta_tab,
         backend,
         entry(key: key, value: val, touched: touched, exp: exp) = entry,
         keep_ttl?
       ) do
    case {list_gen(meta_tab), keep_ttl?} do
      {[newer_gen], false} ->
        backend.insert(newer_gen, entry)

      {[newer_gen], true} ->
        update_or_insert(backend, newer_gen, entry, {3, val})

      {[newer_gen, older_gen], false} ->
        true = update_or_insert(backend, newer_gen, entry, [{3, val}, {4, touched}, {5, exp}])

        true = backend.delete(older_gen, key)

      {[newer_gen, older_gen], true} ->
        true = update_or_insert(backend, newer_gen, entry, [{3, val}, {4, touched}])

        true = backend.delete(older_gen, key)
    end
  end

  defp update_or_insert(backend, table, entry(key: key) = entry, changes) do
    with false <- backend.update_element(table, key, changes) do
      backend.insert(table, entry)
    end
  end

  defp put_entries(meta_tab, backend, entries, chunk_size) when is_list(entries) do
    do_put_entries(meta_tab, backend, entries, fn older_gen ->
      keys = Enum.map(entries, fn entry(key: key) -> key end)

      do_delete_all(backend, older_gen, keys, chunk_size)
    end)
  end

  defp do_put_entries(meta_tab, backend, entry_or_entries, purge_fun) do
    case list_gen(meta_tab) do
      [newer_gen] ->
        backend.insert(newer_gen, entry_or_entries)

      [newer_gen, older_gen] ->
        _ = purge_fun.(older_gen)

        backend.insert(newer_gen, entry_or_entries)
    end
  end

  defp put_new_entries(meta_tab, backend, entries, chunk_size \\ 0)

  defp put_new_entries(meta_tab, backend, entry(key: key) = entry, _chunk_size) do
    do_put_new_entries(meta_tab, backend, entry, fn newer_gen, older_gen ->
      with true <- backend.insert_new(older_gen, entry) do
        true = backend.delete(older_gen, key)

        backend.insert_new(newer_gen, entry)
      end
    end)
  end

  defp put_new_entries(meta_tab, backend, entries, chunk_size) when is_list(entries) do
    do_put_new_entries(meta_tab, backend, entries, fn newer_gen, older_gen ->
      with true <- backend.insert_new(older_gen, entries) do
        keys = Enum.map(entries, fn entry(key: key) -> key end)

        _ = do_delete_all(backend, older_gen, keys, chunk_size)

        backend.insert_new(newer_gen, entries)
      end
    end)
  end

  defp do_put_new_entries(meta_tab, backend, entry_or_entries, purge_fun) do
    case list_gen(meta_tab) do
      [newer_gen] ->
        backend.insert_new(newer_gen, entry_or_entries)

      [newer_gen, older_gen] ->
        purge_fun.(newer_gen, older_gen)
    end
  end

  defp update_entry(meta_tab, backend, key, updates) do
    case list_gen(meta_tab) do
      [newer_gen] ->
        backend.update_element(newer_gen, key, updates)

      [newer_gen, older_gen] ->
        with false <- backend.update_element(newer_gen, key, updates),
             [entry() = entry] <- backend.take(older_gen, key) do
          entry =
            Enum.reduce(updates, entry, fn
              {3, value}, acc -> entry(acc, value: value)
              {4, value}, acc -> entry(acc, touched: value)
              {5, value}, acc -> entry(acc, exp: value)
            end)

          backend.insert(newer_gen, entry)
        else
          [] -> false
          other -> other
        end
    end
  end

  defp do_count_all(backend, tab, keys, chunk_size) do
    ets_select_keys(
      keys,
      chunk_size,
      0,
      &new_match_spec/1,
      &backend.select_count(tab, &1),
      &(&1 + &2)
    )
  end

  defp do_delete_all(backend, tab, keys, chunk_size) do
    ets_select_keys(
      keys,
      chunk_size,
      0,
      &new_match_spec/1,
      &backend.select_delete(tab, &1),
      &(&1 + &2)
    )
  end

  defp do_get_all(backend, tab, keys, select, chunk_size) do
    ets_select_keys(
      keys,
      chunk_size,
      [],
      &new_match_spec(&1, select),
      &backend.select(tab, &1),
      &Kernel.++/2
    )
  end

  defp ets_select_keys([k], chunk_size, acc, ms_fun, chunk_fun, after_fun) do
    k = if is_tuple(k), do: tuple_to_match_spec(k), else: k

    ets_select_keys(
      [],
      2,
      chunk_size,
      match_key(k, Time.now()),
      acc,
      ms_fun,
      chunk_fun,
      after_fun
    )
  end

  defp ets_select_keys([k1, k2 | keys], chunk_size, acc, ms_fun, chunk_fun, after_fun) do
    k1 = if is_tuple(k1), do: tuple_to_match_spec(k1), else: k1
    k2 = if is_tuple(k2), do: tuple_to_match_spec(k2), else: k2
    now = Time.now()

    ets_select_keys(
      keys,
      2,
      chunk_size,
      {:orelse, match_key(k1, now), match_key(k2, now)},
      acc,
      ms_fun,
      chunk_fun,
      after_fun
    )
  end

  defp ets_select_keys([], _count, _chunk_size, chunk_acc, acc, ms_fun, chunk_fun, after_fun) do
    chunk_acc
    |> ms_fun.()
    |> chunk_fun.()
    |> after_fun.(acc)
  end

  defp ets_select_keys(keys, count, chunk_size, chunk_acc, acc, ms_fun, chunk_fun, after_fun)
       when count >= chunk_size do
    acc =
      chunk_acc
      |> ms_fun.()
      |> chunk_fun.()
      |> after_fun.(acc)

    ets_select_keys(keys, chunk_size, acc, ms_fun, chunk_fun, after_fun)
  end

  defp ets_select_keys([k | keys], count, chunk_size, chunk_acc, acc, ms_fun, chunk_fun, after_fun) do
    k = if is_tuple(k), do: tuple_to_match_spec(k), else: k

    ets_select_keys(
      keys,
      count + 1,
      chunk_size,
      {:orelse, chunk_acc, match_key(k, Time.now())},
      acc,
      ms_fun,
      chunk_fun,
      after_fun
    )
  end

  defp tuple_to_match_spec(data) do
    data
    |> :erlang.tuple_to_list()
    |> tuple_to_match_spec([])
  end

  defp tuple_to_match_spec([], acc) do
    {acc |> Enum.reverse() |> :erlang.list_to_tuple()}
  end

  defp tuple_to_match_spec([e | tail], acc) do
    e = if is_tuple(e), do: tuple_to_match_spec(e), else: e

    tuple_to_match_spec(tail, [e | acc])
  end

  defp return({:ok, entry(value: value)}, :value) do
    {:ok, value}
  end

  defp return({:ok, entries}, :value) when is_list(entries) do
    {:ok, for(entry(value: value) <- entries, do: value)}
  end

  defp return(other, _field) do
    other
  end

  defp assert_match_spec(spec, select) when spec in [nil, :expired] do
    [
      {
        entry(key: :"$1", value: :"$2", touched: :"$3", exp: :"$4"),
        [comp_match_spec(spec)],
        [match_return(select)]
      }
    ]
  end

  defp assert_match_spec(spec, _select) do
    case :ets.test_ms(test_ms(), spec) do
      {:ok, _result} ->
        spec

      {:error, _result} ->
        msg = """
        invalid query, expected one of:

        - `nil` - match all entries
        - `:ets.match_spec()` - ETS match spec

        but got:

        #{inspect(spec, pretty: true)}
        """

        raise Nebulex.QueryError, message: msg, query: spec
    end
  end

  defp comp_match_spec(nil) do
    {:orelse, {:"=:=", :"$4", :infinity}, {:<, Time.now(), :"$4"}}
  end

  defp comp_match_spec(:expired) do
    {:not, comp_match_spec(nil)}
  end

  defp maybe_match_spec_return_true([{pattern, conds, _ret}], op)
       when op in [:delete_all, :count_all] do
    [{pattern, conds, [true]}]
  end

  defp maybe_match_spec_return_true(match_spec, _op) do
    match_spec
  end

  defp in_match_spec([k], select) do
    match_key(k, Time.now())
    |> new_match_spec(match_return(select))
  end

  defp in_match_spec([k1, k2 | keys], select) do
    now = Time.now()

    keys
    |> Enum.reduce(
      {:orelse, match_key(k1, now), match_key(k2, now)},
      &{:orelse, &2, match_key(&1, now)}
    )
    |> new_match_spec(match_return(select))
  end

  defp new_match_spec(conds, return \\ true) do
    [
      {
        entry(key: :"$1", value: :"$2", touched: :"$3", exp: :"$4"),
        [conds],
        [return]
      }
    ]
  end

  defp match_return(select) do
    case select do
      :key -> :"$1"
      :value -> :"$2"
      {:key, :value} -> {{:"$1", :"$2"}}
    end
  end

  defp match_key(k, now) do
    {:andalso, {:"=:=", :"$1", k}, {:orelse, {:"=:=", :"$4", :infinity}, {:<, now, :"$4"}}}
  end

  defp test_ms, do: entry(key: 1, value: 1, touched: Time.now(), exp: 1000)
end
