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
    * Eviction - Generational Garbage Collection
      (see `Nebulex.Adapters.Local.Generation`).
    * Sharding - For intensive workloads, the Cache may also be partitioned
      (by using `:shards` backend and specifying the `:partitions` option).
    * Support for transactions via `Nebulex.Locks`, a lightweight ETS-based
      locking mechanism optimized for single-node scenarios. Provides atomic
      lock acquisition, deadlock prevention, and automatic stale lock cleanup.
      See `Nebulex.Locks` and `Nebulex.Adapter.Transaction`.
    * Support for stats.
    * Automatic retry logic for handling race conditions during garbage
      collection (see [Concurrency and resilience](#module-concurrency-and-resilience)).

  ## Concurrency and resilience

  The local adapter implements automatic retry logic to handle race conditions
  that may occur when accessing ETS tables during garbage collection cycles.
  When the garbage collector deletes an old generation, processes holding
  references to that generation's ETS table may encounter `ArgumentError`
  exceptions when attempting to access it.

  To ensure resilience and prevent crashes, all cache operations automatically
  retry up to **3 times** when encountering such errors. The retry mechanism:

    * **Catches `ArgumentError` exceptions** that occur due to deleted ETS
      tables.
    * **Re-fetches fresh generation references** from the metadata table.
    * **Retries the operation** with the updated references.
    * **Prevents infinite loops** by limiting retries to a maximum of 3
      attempts.
    * **Adds a small delay** (10ms) between retries to allow GC operations
      to complete.

  This behavior is transparent to users and ensures that:

    * Operations remain reliable even under high concurrency.
    * Cache operations succeed despite concurrent generation changes.
    * No manual error handling is required for GC-related race conditions.
    * The system gracefully handles generation transitions.

  ### The role of `:gc_cleanup_delay`

  It's important to note that while the automatic retry logic provides an extra
  layer of safety, **race conditions are unlikely to occur in practice** due to
  the `:gc_cleanup_delay` configuration option (defaults to 10 seconds).

  When garbage collection runs and creates a new generation, the old generation
  is not deleted immediately. Instead, it's kept alive for the duration
  specified by `:gc_cleanup_delay`. This grace period allows ongoing operations
  that hold references to the old generation to complete successfully before the
  table is actually deleted.

  The automatic retry mechanism serves as a **safeguard** for edge cases where:

    * Operations take longer than the cleanup delay.
    * Extremely high concurrency scenarios.
    * Systems under heavy load.

  **Recommendation**: Configure `:gc_cleanup_delay` with a reasonable timeout
  (the default 10 seconds is appropriate for most use cases). This ensures
  concurrent operations have sufficient time to transition gracefully, making
  the retry mechanism rarely necessary.

  ### Example scenario

  Consider this race condition scenario:

  1. **Process A** fetches generation references and starts a `fetch/2`
    operation.
  2. **Process B** (GC) deletes the old generation while Process A is accessing
    it.
  3. **Process A** encounters an `ArgumentError` when trying to access the
    deleted table.
  4. **Automatic retry** catches the error, fetches fresh generation references,
    and retries.
  5. **Operation succeeds** using the updated generation references.

  > **This automatic retry logic applies to ALL cache operations.**

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

  The adapter defines an entry as a tuple `{:entry, key, value, touched, ttl, tag}`,
  meaning the match pattern within the ETS Match Spec must be like
  `{:entry, :"$1", :"$2", :"$3", :"$4", :"$5"}`. To make query building easier,
  you can use the `Ex2ms` library.

      # Using raw ETS match spec
      iex> match_spec = [
      ...>   {
      ...>     {:entry, :"$1", :"$2", :_, :_, :_},
      ...>     [{:>, :"$2", 1}],
      ...>     [{{:"$1", :"$2"}}]
      ...>   }
      ...> ]
      iex> MyCache.get_all(query: match_spec)
      {:ok, [{:b, 2}, {:c, 3}]}

      # Using Ex2ms for easier query building
      iex> import Ex2ms
      iex> match_spec = fun do
      ...>   {_, key, value, _, _, _} when value > 1 -> {key, value}
      ...> end
      iex> MyCache.get_all(query: match_spec)
      {:ok, [{:b, 2}, {:c, 3}]}

  > You can use the `Ex2ms` or `MatchSpec` library to build queries easier.

  ## Building Match Specs with QueryHelper

  The `Nebulex.Adapters.Local.QueryHelper` module provides a user-friendly,
  SQL-like syntax for building ETS match specifications without needing to know
  the internal entry tuple structure. This is especially useful when working
  with the local adapter's queryable API.

  ### Why use QueryHelper?

  When building match specs manually, you need to know that entries are stored
  as `{:entry, key, value, touched, exp, tag}` tuples. QueryHelper abstracts
  this away, letting you work with named field bindings instead:

      # Without QueryHelper - you need to know the exact tuple structure
      import Ex2ms
      match_spec = fun do
        {:entry, k, v, _, _, _} when k == :foo -> v
      end

      # With QueryHelper - clean, declarative syntax
      use Nebulex.Adapters.Local.QueryHelper
      match_spec = match_spec key: k, value: v, where: k == :foo, select: v

  ### Getting started

  Use `Nebulex.Adapters.Local.QueryHelper` in your module to enable the
  `match_spec/1` macro and `Ex2ms` support:

      defmodule MyCache.Queries do
        use Nebulex.Adapters.Local.QueryHelper

        def by_key(key) do
          match_spec key: k, value: v, where: k == key, select: v
        end

        def by_tag(tag) do
          match_spec tag: t, where: t == tag, select: true
        end

        def expensive_keys do
          match_spec key: k, value: v, where: is_integer(v) and v > 100, select: k
        end
      end

  ### Syntax

  The `match_spec/1` macro accepts a keyword list with:

    * **Field bindings** - `:key`, `:value`, `:touched`, `:exp`, `:tag` - Bind
      entry fields to variables. Fields not mentioned are automatically
      wildcarded.
    * **`:where`** - Optional guard clause with conditions (supports all ETS
      guard functions).
    * **`:select`** - Required return expression specifying what to return.

  ### Examples

      # Match all entries where value is greater than 10
      match_spec value: v, where: v > 10, select: v

      # Match entries with a specific tag
      match_spec key: k, tag: t, where: t == :important, select: k

      # Complex guards with multiple conditions
      match_spec key: k, value: v, exp: e,
                 where: is_integer(v) and e != :infinity,
                 select: {k, v, e}

      # Match without guards (all entries)
      match_spec key: k, value: v, select: {k, v}

      # Return entire entry
      match_spec key: k, tag: t, where: t == :session, select: :"$_"

      # Query only specific fields
      match_spec tag: t, where: t == :cache_group, select: true

  ### Using with cache operations

  QueryHelper match specs work seamlessly with all queryable operations:

      use Nebulex.Adapters.Local.QueryHelper

      # Get all values where key is an integer greater than 10
      ms = match_spec key: k, value: v, where: is_integer(k) and k > 10, select: v
      MyCache.get_all!(query: ms)

      # Count entries with a specific tag
      ms = match_spec tag: t, where: t == :user_session, select: true
      MyCache.count_all!(query: ms)

      # Delete expired entries (exp is not :infinity and less than now)
      now = System.system_time(:millisecond)
      ms = match_spec exp: e, where: e != :infinity and e < now, select: true
      MyCache.delete_all!(query: ms)

      # Stream entries in batches
      ms = match_spec value: v, where: is_binary(v), select: v
      MyCache.stream!(query: ms) |> Enum.take(100)

  ### Practical examples

  Here are some common patterns using QueryHelper:

      defmodule MyApp.CacheQueries do
        use Nebulex.Adapters.Local.QueryHelper

        # Find all entries for a specific user
        def user_entries(user_id) do
          match_spec tag: t, where: t == {:user, user_id}, select: :"$_"
        end

        # Find entries expiring soon (within next hour)
        def expiring_soon do
          cutoff = System.system_time(:millisecond) + :timer.hours(1)
          match_spec exp: e, where: e != :infinity and e < cutoff, select: true
        end

        # Get all cached integers
        def integer_values do
          match_spec value: v, where: is_integer(v), select: v
        end

        # Complex filtering with multiple conditions
        def recent_tagged_entries(tag, min_time) do
          match_spec key: k,
                     value: v,
                     tag: t,
                     touched: ts,
                     where: t == tag and ts > min_time,
                     select: {k, v}
        end
      end

      # Use in your application
      MyApp.Cache.get_all!(query: MyApp.CacheQueries.user_entries(123))
      MyApp.Cache.delete_all!(query: MyApp.CacheQueries.expiring_soon())

  ### Working with cache references

  When using the `:references` option with `Nebulex.Caching` decorators (like
  `@decorate cacheable/3`), Nebulex creates reference entries to track
  dependencies between cached values. The `keyref_match_spec/2` helper makes it
  easy to find and clean up these reference entries.

  The function builds a match spec that finds all cache keys (reference keys)
  that point to a specific referenced key. This is useful for:

    * Invalidating all entries that depend on a specific key
    * Counting how many references point to a key
    * Getting a list of dependent cache keys

  #### Examples

      import Nebulex.Adapters.Local.QueryHelper

      # Delete all references to a specific key (any cache)
      ms = keyref_match_spec(:user_123)
      MyCache.delete_all!(query: ms)

      # Delete references in a specific cache only
      ms = keyref_match_spec(:user_123, cache: MyApp.UserCache)
      MyCache.delete_all!(query: ms)

      # Count how many cache entries reference a key
      ms = keyref_match_spec(:product_456)
      count = MyCache.count_all!(query: ms)

      # Get all cache keys that reference a specific key
      ms = keyref_match_spec(:user_123)
      reference_keys = MyCache.get_all!(query: ms)

  #### Example: Invalidating cached method results

  When using the `:references` option with caching decorators, you can easily
  invalidate all cached results that depend on a specific entity:

      defmodule MyApp.UserAccounts do
          use Nebulex.Caching, cache: MyApp.Cache
          use Nebulex.Adapters.Local.QueryHelper

          @decorate cacheable(key: id)
          def get_user_account(id) do
            # your logic ...
          end

          @decorate cacheable(key: email, references: &(&1 && &1.id))
          def get_user_account_by_email(email) do
            # your logic ...
          end

          @decorate cacheable(key: token, references: &(&1 && &1.id))
          def get_user_account_by_token(token) do
            # your logic ...
          end

          @decorate cache_evict(key: user.id, query: &__MODULE__.keyref_query/1)
          def update_user_account(user, attrs) do
            # your logic ...
          end

          def keyref_query(%{args: [user | _]} = _context) do
            keyref_match_spec(user.id)
          end
        end
      end

  > See `Nebulex.Adapters.Local.QueryHelper` for complete documentation.

  ## Tagging entries

  The local adapter supports tagging cache entries with arbitrary terms via the
  `:tag` option. Tags provide a powerful way to organize and query cache entries
  by associating metadata with them. This is especially useful for:

    * **Logical grouping** - Group related entries (e.g., all entries for a
      specific user, session, or feature).
    * **Selective invalidation** - Delete all entries with a specific tag
      without affecting other cached data.
    * **Efficient filtering** - Query entries by tag using ETS match specs.

  ### Tagging entries

  You can tag entries when using `put/3`, `put!/3`, `put_all/2`, `put_all!/2`,
  and related operations:

      # Tag a single entry
      MyCache.put("user:123:profile", user_data, tag: :user_123)

      # Tag multiple entries at once
      MyCache.put_all(
        [
          {"session:abc:data", session_data},
          {"session:abc:prefs", preferences},
        ],
        tag: :session_abc
      )

      # Different tags for different entry groups
      MyCache.put_all([a: 1, b: 2, c: 3], tag: :group_a)
      MyCache.put_all([d: 4, e: 5, f: 6], tag: :group_b)

  When you don't provide a tag, entries are stored with a `nil` tag value.

  ### Querying by tag

  You can query entries by tag using either **QueryHelper** (recommended for
  cleaner syntax) or **Ex2ms**:

      # Using QueryHelper (recommended)
      use Nebulex.Adapters.Local.QueryHelper

      # Get all values for entries with a specific tag
      match_spec = match_spec value: v, tag: t, where: t == :group_a, select: v
      MyCache.get_all!(query: match_spec)
      #=> [1, 2, 3]

      # Delete all entries with a specific tag
      match_spec = match_spec tag: t, where: t == :group_a, select: true
      MyCache.delete_all!(query: match_spec)

      # Query entries with multiple tags (return key-value tuples)
      match_spec = match_spec key: k, value: v, tag: t,
                             where: t == :group_a or t == :group_b,
                             select: {k, v}
      MyCache.get_all!(query: match_spec)
      #=> [{:a, 1}, {:b, 2}, {:c, 3}, {:d, 4}, {:e, 5}, {:f, 6}]

      # Count entries with a specific tag
      match_spec = match_spec tag: t, where: t == :group_a, select: true
      MyCache.count_all!(query: match_spec)
      #=> 3

  Alternatively, you can use **Ex2ms** if you need to work with the raw tuple
  structure:

      # Using Ex2ms (alternative approach)
      import Ex2ms

      # Get all values for entries with a specific tag
      match_spec = fun do
        {_, _, value, _, _, tag} when tag == :group_a -> value
      end

      MyCache.get_all!(query: match_spec)
      #=> [1, 2, 3]

      # Query entries with multiple tags
      match_spec = fun do
        {_, key, value, _, _, tag} when tag == :group_a or tag == :group_b ->
          {key, value}
      end

      MyCache.get_all!(query: match_spec)
      #=> [{:a, 1}, {:b, 2}, {:c, 3}, {:d, 4}, {:e, 5}, {:f, 6}]

  ### Practical example

  Here's a complete example showing how to use tags for user session management:

      # Store user session data with tags
      user_id = 123
      session_id = "abc-def-ghi"

      MyCache.put_all([
        {"user:\#{user_id}:profile", user_profile},
        {"user:\#{user_id}:settings", user_settings},
        {"user:\#{user_id}:permissions", permissions}
      ], tag: {:user, user_id})

      # Later, invalidate all data for this user using QueryHelper
      use Nebulex.Adapters.Local.QueryHelper

      invalidate_user = match_spec tag: t, where: t == {:user, 123}, select: true
      MyCache.delete_all!(query: invalidate_user)

      # Or using Ex2ms (alternative)
      import Ex2ms

      invalidate_user = fun do
        {_, _, _, _, _, tag} when tag == {:user, 123} -> true
      end

      MyCache.delete_all!(query: invalidate_user)

  Tags can be any Elixir term (atoms, tuples, strings, etc.), giving you
  flexibility in how you organize your cache entries.

  ## Using Caching Decorators with QueryHelper

  The `Nebulex.Caching` decorators (`@decorate`) integrate seamlessly with
  QueryHelper and tagging for powerful cache management patterns. This section
  shows practical examples of combining decorators with QueryHelper and tags.

  ### Entry Tagging with Decorators

  You can tag entries automatically when using `@decorate cacheable` and
  `@decorate cache_put` by specifying the `:tag` option:

      defmodule MyApp.UserCache do
        use Nebulex.Caching, cache: MyApp.Cache
        use Nebulex.Adapters.Local.QueryHelper

        # Cache user data with automatic tagging
        @decorate cacheable(key: user_id, opts: [tag: :users])
        def get_user(user_id) do
          # fetch user from database
          {:ok, user}
        end

        # Cache user permissions with automatic tagging
        @decorate cacheable(key: user_id, opts: [tag: :permissions])
        def get_user_permissions(user_id) do
          # fetch permissions from database
          {:ok, permissions}
        end

        # Store session data with automatic tagging
        @decorate cache_put(key: session_id, opts: [tag: :sessions])
        def create_session(session_id, data) do
          data
        end
      end

  ### Selective Cache Invalidation with Tags

  Use the `@decorate cache_evict` decorator with QueryHelper to invalidate
  entries by tag. This is useful for clearing related cached data:

      defmodule MyApp.UserCache do
        use Nebulex.Caching, cache: MyApp.Cache
        use Nebulex.Adapters.Local.QueryHelper

        # ... cacheable functions as above ...

        # Evict all cached data for a specific user
        @decorate cache_evict(query: &evict_user_query/1)
        def invalidate_user(user_id) do
          :ok
        end

        defp evict_user_query(context) do
          # Return a QueryHelper match spec to evict entries by tag
          match_spec tag: t, where: t == :users, select: true
        end
      end

  ### Cache Reference Invalidation with keyref_match_spec

  When using the `:references` option to track cache dependencies, you can
  use `keyref_match_spec` with `cache_evict` to invalidate all dependent
  entries:

      defmodule MyApp.UserAccounts do
        use Nebulex.Caching, cache: MyApp.Cache
        use Nebulex.Adapters.Local.QueryHelper

        # Cache user account by ID
        @decorate cacheable(key: user_id)
        def get_user_account(user_id) do
          fetch_user(user_id)
        end

        # Cache user account by email, referencing the user ID
        @decorate cacheable(key: email, references: &(&1 && &1.id))
        def get_user_account_by_email(email) do
          user = fetch_user_by_email(email)
          {:ok, user}
        end

        # Cache user account by token, also referencing the user ID
        @decorate cacheable(key: token, references: &(&1 && &1.id))
        def get_user_account_by_token(token) do
          user = fetch_user_by_token(token)
          {:ok, user}
        end

        # Evict all cache entries referencing a specific user
        # This invalidates all lookups (by id, email, token) in one operation
        @decorate cache_evict(key: user_id, query: &invalidate_refs/1)
        def update_user_account(user_id, attrs) do
          :ok
        end

        defp invalidate_refs(%{args: [user_id | _]} = _context) do
          keyref_match_spec(user_id)
        end
      end

  ### Practical Pattern: Clearing All Session Data

  Here's a complete example showing how to manage user sessions with automatic
  tagging and selective eviction:

      defmodule MyApp.Sessions do
        use Nebulex.Caching, cache: MyApp.Cache
        use Nebulex.Adapters.Local.QueryHelper

        # Store session data with automatic tagging by user ID
        @decorate cache_put(key: session_id, opts: [tag: {:session, user_id}])
        def store_session(user_id, session_id, data) do
          data
        end

        # Evict all sessions for a specific user when they log out
        @decorate cache_evict(query: &evict_user_sessions_query/1)
        def logout_user(user_id) do
          :ok
        end

        defp evict_user_sessions_query(%{args: [user_id]} = _context) do
          match_spec tag: t, where: t == {:session, user_id}
        end

        # Clear all sessions across all users (e.g., during maintenance)
        @decorate cache_evict(query: &evict_all_sessions_query/1)
        def clear_all_sessions do
          :ok
        end

        defp evict_all_sessions_query(_context) do
          # Match all entries with a session tag (pattern {:session, _})
          match_spec tag: t, where: is_tuple(t) and elem(t, 0) == :session
        end
      end

  > The combination of decorators, QueryHelper, and tagging provides a clean,
  > declarative way to manage cache lifecycles with minimal boilerplate.

  ## Transaction API

  This adapter implements the `Nebulex.Adapter.Transaction` behaviour using
  `Nebulex.Locks`, a lightweight ETS-based locking mechanism optimized for
  single-node scenarios. This implementation provides significantly better
  performance compared to distributed locking mechanisms (e.g., `:global`)
  while maintaining the same transaction API.

  The `transaction` command accepts the following options:

  #{Nebulex.Locks.Options.options_docs()}

  The locks manager can be customized via the `:lock_opts` configuration
  option when starting the cache. See the configuration options above for
  more details.

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
  @behaviour Nebulex.Adapter.Transaction

  # Inherit default info implementation
  use Nebulex.Adapters.Common.Info

  # Inherit default observable implementation
  use Nebulex.Adapter.Observable

  import Nebulex.Utils
  import Record

  alias Nebulex.Adapters.Common.Info.Stats
  alias Nebulex.Adapters.Local.{Backend, Generation, Metadata}
  alias Nebulex.Locks
  alias Nebulex.Locks.Options, as: LockOptions
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
    exp: nil,
    tag: nil
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
    with_retry(fn ->
      meta_tab
      |> list_gen()
      |> do_fetch(name, backend, key)
      |> return(:value)
    end)
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
  def put(adapter_meta, key, value, on_write, ttl, keep_ttl?, opts) do
    now = Time.now()
    {has_tag?, tag} = get_tag(opts)
    entry = entry(key: key, value: value, touched: now, exp: exp(now, ttl), tag: tag)

    with_retry(fn ->
      do_put(on_write, adapter_meta.meta_tab, adapter_meta.backend, entry, keep_ttl?, has_tag?)
      |> wrap_ok()
    end)
  end

  defp do_put(:put, meta_tab, backend, entry, keep_ttl?, _has_tag?) do
    put_entry(meta_tab, backend, entry, keep_ttl?)
  end

  defp do_put(:put_new, meta_tab, backend, entry, _keep_ttl?, _has_tag?) do
    put_new_entries(meta_tab, backend, entry)
  end

  defp do_put(
         :replace,
         meta_tab,
         backend,
         entry(key: key, value: value, touched: touched, exp: exp, tag: tag),
         keep_ttl?,
         has_tag?
       ) do
    changes = if has_tag?, do: [], else: [{6, tag}]
    changes = if keep_ttl?, do: changes, else: [{4, touched}, {5, exp}]

    update_entry(meta_tab, backend, key, [{3, value} | changes])
  end

  @impl true
  def put_all(adapter_meta, entries, on_write, ttl, opts) do
    now = Time.now()
    exp = exp(now, ttl)
    {_has_tag?, tag} = get_tag(opts)

    with_retry(fn ->
      do_put_all(
        on_write,
        adapter_meta.meta_tab,
        adapter_meta.backend,
        adapter_meta.purge_chunk_size,
        Enum.map(
          entries,
          &entry(key: elem(&1, 0), value: elem(&1, 1), touched: now, exp: exp, tag: tag)
        )
      )
      |> wrap_ok()
    end)
  end

  defp do_put_all(:put, meta_tab, backend, chunk_size, entries) do
    put_entries(meta_tab, backend, entries, chunk_size)
  end

  defp do_put_all(:put_new, meta_tab, backend, chunk_size, entries) do
    put_new_entries(meta_tab, backend, entries, chunk_size)
  end

  @impl true
  def delete(adapter_meta, key, _opts) do
    with_retry(fn ->
      adapter_meta.meta_tab
      |> list_gen()
      |> Enum.each(&adapter_meta.backend.delete(&1, key))
    end)
  end

  @impl true
  def take(%{name: name, meta_tab: meta_tab, backend: backend}, key, _opts) do
    with_retry(fn ->
      do_take(meta_tab, backend, name, key)
    end)
  end

  defp do_take(meta_tab, backend, name, key) do
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
    with_retry(fn ->
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
    end)
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
    with_retry(fn ->
      with {:ok, res} <- meta_tab |> list_gen() |> do_fetch(name, backend, key) do
        {:ok, entry_ttl(res)}
      end
    end)
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

    with_retry(fn ->
      adapter_meta.meta_tab
      |> update_entry(adapter_meta.backend, key, [{4, now}, {5, exp(now, ttl)}])
      |> wrap_ok()
    end)
  end

  @impl true
  def touch(adapter_meta, key, _opts) do
    with_retry(fn ->
      adapter_meta.meta_tab
      |> update_entry(adapter_meta.backend, key, [{4, Time.now()}])
      |> wrap_ok()
    end)
  end

  ## Nebulex.Adapter.Queryable

  @impl true
  def execute(adapter_meta, query_meta, opts) do
    do_execute(adapter_meta, query_meta, opts)
  end

  defp do_execute(_adapter_meta, %{op: :get_all, query: {:in, []}}, _opts) do
    {:ok, []}
  end

  defp do_execute(_adapter_meta, %{op: op, query: {:in, []}}, _opts)
       when op in [:count_all, :delete_all] do
    {:ok, 0}
  end

  defp do_execute(
         %{meta_tab: meta_tab, backend: backend},
         %{op: :count_all, query: {:q, nil}},
         _opts
       ) do
    with_retry(fn ->
      meta_tab
      |> list_gen()
      |> Enum.reduce(0, fn gen, acc ->
        gen
        |> backend.info(:size)
        |> Kernel.+(acc)
      end)
      |> wrap_ok()
    end)
  end

  defp do_execute(
         %{meta_tab: meta_tab} = adapter_meta,
         %{op: :delete_all, query: {:q, nil}} = query_spec,
         _opts
       ) do
    with {:ok, count_all} <- do_execute(adapter_meta, %{query_spec | op: :count_all}, []) do
      :ok = Generation.delete_all(meta_tab)

      {:ok, count_all}
    end
  end

  defp do_execute(
         %{meta_tab: meta_tab, backend: backend},
         %{op: :count_all, query: {:in, keys}},
         opts
       )
       when is_list(keys) do
    chunk_size = Keyword.get(opts, :chunk_size, 10)

    with_retry(fn ->
      meta_tab
      |> list_gen()
      |> Enum.reduce(0, fn gen, acc ->
        do_count_all(backend, gen, keys, chunk_size) + acc
      end)
      |> wrap_ok()
    end)
  end

  defp do_execute(
         %{meta_tab: meta_tab, backend: backend},
         %{op: :delete_all, query: {:in, keys}},
         opts
       )
       when is_list(keys) do
    chunk_size = Keyword.get(opts, :chunk_size, 10)

    with_retry(fn ->
      meta_tab
      |> list_gen()
      |> Enum.reduce(0, fn gen, acc ->
        do_delete_all(backend, gen, keys, chunk_size) + acc
      end)
      |> wrap_ok()
    end)
  end

  defp do_execute(
         %{meta_tab: meta_tab, backend: backend},
         %{op: :get_all, query: {:in, keys}, select: select},
         opts
       )
       when is_list(keys) do
    chunk_size = Keyword.get(opts, :chunk_size, 10)

    with_retry(fn ->
      meta_tab
      |> list_gen()
      |> Enum.reduce([], fn gen, acc ->
        do_get_all(backend, gen, keys, match_return(select), chunk_size) ++ acc
      end)
      |> wrap_ok()
    end)
  end

  defp do_execute(
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

    with_retry(fn ->
      meta_tab
      |> list_gen()
      |> Enum.reduce(acc_in, reducer)
      |> wrap_ok()
    end)
  end

  @impl true
  def stream(adapter_meta, query_meta, opts) do
    do_stream(adapter_meta, query_meta, opts)
  end

  defp do_stream(
         %{meta_tab: meta_tab, backend: backend},
         %{query: {:in, keys}, select: select},
         opts
       ) do
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

  defp do_stream(adapter_meta, %{query: {:q, ms}, select: select}, opts) do
    adapter_meta
    |> build_stream(
      assert_match_spec(ms, select),
      Keyword.get(opts, :max_entries, 20)
    )
    |> wrap_ok()
  end

  defp build_stream(%{meta_tab: meta_tab, backend: backend}, match_spec, page_size) do
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

  ## Nebulex.Adapter.Transaction

  @impl true
  def transaction(%{cache: cache, pid: pid} = adapter_meta, fun, opts) do
    opts = LockOptions.validate!(opts)

    adapter_meta
    |> do_in_transaction?()
    |> do_transaction(
      pid,
      adapter_meta[:name] || cache,
      adapter_meta.meta_tab,
      opts,
      fun
    )
  end

  @impl true
  def in_transaction?(adapter_meta, _opts) do
    wrap_ok do_in_transaction?(adapter_meta)
  end

  defp do_in_transaction?(%{pid: pid}) do
    !!Process.get({pid, self()})
  end

  defp do_transaction(true, _pid, _name, _meta_tab, _opts, fun) do
    {:ok, fun.()}
  end

  defp do_transaction(false, pid, name, meta_tab, opts, fun) do
    locks_table = Metadata.fetch!(meta_tab, :locks_table)
    keys = Keyword.fetch!(opts, :keys)
    ids = lock_ids(name, keys)

    case Locks.acquire(locks_table, ids, opts) do
      :ok ->
        try do
          _ = Process.put({pid, self()}, keys)

          {:ok, fun.()}
        after
          _ = Process.delete({pid, self()})

          Locks.release(locks_table, ids)
        end

      {:error, :timeout} ->
        wrap_error Nebulex.Error, reason: :transaction_aborted, cache: name
    end
  end

  defp lock_ids(name, []), do: [name]
  defp lock_ids(name, keys), do: Enum.map(keys, &{name, &1})

  ## Helpers

  # Inline common instructions
  @compile {:inline, fetch_entry: 4, pop_entry: 4, list_gen: 1, newer_gen: 1, match_key: 2}

  @max_retries 3
  def with_retry(fun, retries \\ @max_retries)

  def with_retry(fun, 0) do
    fun.()
  end

  def with_retry(fun, retries) when retries > 0 do
    fun.()
  rescue
    ArgumentError ->
      # Retry will force fetching fresh generation references
      :ok = Process.sleep(10)

      with_retry(fun, retries - 1)
  end

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

  defp get_tag(opts) do
    case Keyword.fetch(opts, :tag) do
      {:ok, tag} -> {true, tag}
      :error -> {false, nil}
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
              {6, value}, acc -> entry(acc, tag: value)
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
        entry(key: :"$1", value: :"$2", touched: :"$3", exp: :"$4", tag: :"$5"),
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
