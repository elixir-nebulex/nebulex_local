defmodule Nebulex.Locks do
  @moduledoc """
  A simple, ETS-based locking mechanism for local synchronization.

  This module provides a lightweight locking system suitable for single-node
  use cases, such as implementing transactions in local cache adapters. It
  uses ETS for lock storage with atomic operations for acquiring and releasing
  locks.

  ## Features

    * **Atomic lock acquisition** - Uses `insert_new` for atomic operations.
    * **Deadlock prevention** - Locks are acquired in sorted key order.
    * **Dual stale lock cleanup** - Automatic cleanup via two mechanisms:
      * **On-demand**: During lock acquisition attempts
      * **Periodic**: Background cleanup at configurable intervals
    * **Fine-grained locking** - Lock specific keys for better concurrency.
    * **Retry with jitter** - Configurable retries with jitter to prevent
      thundering herd.
    * **Scalable cleanup** - Batch-based cleanup using ETS continuation to
      handle large numbers of locks efficiently.

  ## Usage

  The module supports two modes of operation:

  ### Named mode (recommended)

  When you provide a `:name` option, both the GenServer and the ETS table
  are registered with that name, allowing direct access without calling
  `get_table/1`:

      # Start with a name
      children = [
        {Nebulex.Locks, name: MyApp.Locks}
      ]

      Supervisor.start_link(children, strategy: :one_for_one)

      # Use the name directly for acquire/release
      case Nebulex.Locks.acquire(MyApp.Locks, [:key1, :key2], retries: 5) do
        :ok ->
          try do
            # Critical section
          after
            Nebulex.Locks.release(MyApp.Locks, [:key1, :key2])
          end

        {:error, :timeout} ->
          # Failed to acquire locks
      end

  ### Anonymous mode

  When `:name` is not provided or is `nil`, the GenServer starts unnamed
  and you must use `get_table/1` to obtain the table reference:

      # Start without a name
      {:ok, pid} = Nebulex.Locks.start_link([])

      # Get the table reference
      table = Nebulex.Locks.get_table(pid)

      # Use the table reference
      Nebulex.Locks.acquire(table, [:key1], retries: 3)

  ## Start Options

  #{Nebulex.Locks.Options.start_options_docs()}

  ## Lock Entry Structure

  Locks are stored in ETS as records:

      {:lock, lock_key, owner_pid, timestamp, timeout}

  Where:

    * `lock_key` - The key being locked (sorted to prevent deadlocks).
    * `owner_pid` - The PID of the process holding the lock.
    * `timestamp` - When the lock was acquired (for stale detection).
    * `timeout` - Maximum time the lock can be held before being stale.

  ## Stale Lock Detection and Cleanup

  This module implements a defense-in-depth approach to stale lock cleanup,
  combining two complementary mechanisms:

  ### On-Demand Cleanup

  When a process attempts to acquire a lock and finds an existing lock, it
  checks if the lock is stale before failing the acquisition:

    * **Dead process detection**: Uses `Process.alive?/1` to detect if the
      lock owner has crashed.
    * **Timeout detection**: Compares the lock's timestamp with its configured
      `lock_timeout` to detect locks held longer than allowed.

  If a stale lock is detected, it is immediately removed and the acquiring
  process can obtain the lock. This ensures low latency for lock acquisition
  on frequently accessed keys.

  ### Periodic Cleanup

  A background cleanup process runs at the configured `:cleanup_interval`
  (default: 5 minutes) to proactively remove stale locks. Locks are processed
  in batches (default: 100 locks per batch) using ETS continuation to avoid
  loading the entire table into memory at once.

  This periodic cleanup ensures that stale locks from infrequently accessed
  keys don't accumulate in memory, preventing memory leaks.

  ### Why Two Mechanisms?

    * **On-demand cleanup**: Provides immediate cleanup for actively used keys
      with minimal latency.
    * **Periodic cleanup**: Prevents memory leaks by cleaning up locks for keys
      that are no longer accessed.
    * **Together**: Ensures robust cleanup under all usage patterns.

  ## Performance Characteristics

  ### Lock Acquisition and Release

    * **No GenServer bottleneck**: Lock operations hit ETS directly via atomic
      `insert_new` operations. Only `get_table/1` requires a GenServer call.
    * **High concurrency**: Public ETS table with `:write_concurrency` and
      `:read_concurrency` enabled.

  ### Lock Fairness

    * **No FIFO guarantee**: When multiple processes compete for the same lock,
      the winner is determined by ETS race conditions and scheduler timing.
    * **Practical fairness**: With retry intervals and jitter, starvation is
      virtually impossible in practice.
    * **Trade-off**: Prioritizes performance over strict ordering. For use cases
      requiring FIFO fairness, consider `sleeplocks` or a queue-based approach.

  """

  use GenServer

  alias Nebulex.Locks.Options
  alias Nebulex.Time

  import Record

  ## Internals

  # Lock Entry
  defrecord(:lock,
    key: nil,
    owner: nil,
    timestamp: nil,
    timeout: nil
  )

  # Internal state
  defstruct table: nil, cleanup_interval: nil, cleanup_batch_size: nil

  ## API

  @doc """
  Starts the locks GenServer.

  ## Options

  #{Nebulex.Locks.Options.start_options_docs()}

  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    opts = Options.validate_start_opts!(opts)

    case Keyword.get(opts, :name) do
      nil ->
        GenServer.start_link(__MODULE__, opts)

      name when is_atom(name) ->
        GenServer.start_link(__MODULE__, opts, name: name)
    end
  end

  @doc """
  Returns the locks table from a running GenServer.

  This function is needed when starting the GenServer without a name.
  For named GenServers, you can use the name directly with `acquire/3`
  and `release/2`.

  ## Examples

      # Named mode - table name is the same as GenServer name
      iex> {:ok, _pid} = Nebulex.Locks.start_link(name: MyLocks)
      iex> table = Nebulex.Locks.get_table(MyLocks)
      iex> table == MyLocks
      true

      # Anonymous mode - returns a table reference
      iex> {:ok, pid} = Nebulex.Locks.start_link([])
      iex> table = Nebulex.Locks.get_table(pid)
      iex> is_reference(table)
      true

  """
  @spec get_table(GenServer.server()) :: :ets.table()
  def get_table(server) do
    GenServer.call(server, :get_table)
  end

  @doc """
  Attempts to acquire locks for the given keys.

  Locks are acquired atomically in a sorted order to prevent deadlocks.
  If any lock cannot be acquired, all partially acquired locks are released
  and the operation is retried according to the options.

  The first parameter can be either a named table (atom) or a table reference.

  ## Options

  #{Nebulex.Locks.Options.options_docs()}

  ## Examples

      # Using a named table
      iex> {:ok, _pid} = Nebulex.Locks.start_link(name: MyLocks)
      iex> Nebulex.Locks.acquire(MyLocks, [:key1, :key2])
      :ok

      # Using a table reference
      iex> {:ok, pid} = Nebulex.Locks.start_link([])
      iex> table = Nebulex.Locks.get_table(pid)
      iex> Nebulex.Locks.acquire(table, [:key1])
      :ok

  """
  @spec acquire(:ets.table(), [any()], keyword()) :: :ok | {:error, :timeout}
  def acquire(table, keys, opts \\ []) do
    opts = Options.validate!(opts)
    retries = Keyword.fetch!(opts, :retries)
    retry_interval = Keyword.fetch!(opts, :retry_interval)
    lock_timeout = Keyword.fetch!(opts, :lock_timeout)
    lock_keys = build_lock_keys(keys)

    do_acquire(table, lock_keys, retries, retry_interval, lock_timeout, 0)
  end

  @doc """
  Releases locks for the given keys.

  This function is typically called in an `after` block to ensure
  locks are released even if an exception occurs.

  The first parameter can be either a named table (atom) or a table reference.

  ## Examples

      # Using a named table
      iex> {:ok, _pid} = Nebulex.Locks.start_link(name: MyLocks)
      iex> Nebulex.Locks.acquire(MyLocks, [:key1, :key2])
      :ok
      iex> Nebulex.Locks.release(MyLocks, [:key1, :key2])
      :ok

      # Using a table reference
      iex> {:ok, pid} = Nebulex.Locks.start_link([])
      iex> table = Nebulex.Locks.get_table(pid)
      iex> Nebulex.Locks.acquire(table, [:key1])
      :ok
      iex> Nebulex.Locks.release(table, [:key1])
      :ok

  """
  @spec release(:ets.table(), [any()]) :: :ok
  def release(table, keys) do
    keys
    |> build_lock_keys()
    |> Enum.each(&:ets.delete(table, &1))
  end

  ## GenServer Callbacks

  @impl true
  def init(opts) do
    name = Keyword.get(opts, :name)
    cleanup_interval = Keyword.fetch!(opts, :cleanup_interval)
    cleanup_batch_size = Keyword.fetch!(opts, :cleanup_batch_size)
    init_callback = Keyword.get(opts, :init_callback)

    # Initialize the locks table
    table = init_table(name)

    # Invoke the init callback if provided
    if init_callback do
      {m, f, a} = init_callback

      apply(m, f, [table | a])
    end

    # Create the initial state
    state = %__MODULE__{
      table: table,
      cleanup_interval: cleanup_interval,
      cleanup_batch_size: cleanup_batch_size
    }

    {:ok, state, {:continue, :setup_cleanup}}
  end

  @impl true
  def handle_continue(:setup_cleanup, %__MODULE__{cleanup_interval: cleanup_interval} = state) do
    # Schedule first cleanup
    Process.send_after(self(), :cleanup, cleanup_interval)

    {:noreply, state}
  end

  @impl true
  def handle_call(:get_table, _from, %__MODULE__{table: table} = state) do
    {:reply, table, state}
  end

  @impl true
  def handle_info(
        :cleanup,
        %__MODULE__{
          table: table,
          cleanup_interval: cleanup_interval,
          cleanup_batch_size: cleanup_batch_size
        } = state
      ) do
    cleanup_stale_locks(table, cleanup_batch_size)

    # Schedule next cleanup
    Process.send_after(self(), :cleanup, cleanup_interval)

    {:noreply, state}
  end

  ## Helpers

  # Initialize a new locks ETS table
  defp init_table(nil) do
    :ets.new(:locks_table, [
      :set,
      :public,
      keypos: 2,
      read_concurrency: true,
      write_concurrency: true
    ])
  end

  defp init_table(name) do
    :ets.new(name, [
      :set,
      :public,
      :named_table,
      keypos: 2,
      read_concurrency: true,
      write_concurrency: true
    ])
  end

  # Build and sort lock keys to prevent deadlocks
  defp build_lock_keys([]) do
    # Global lock when no keys specified
    [:__global_lock__]
  end

  defp build_lock_keys(keys) do
    # Sort keys deterministically to prevent deadlocks
    Enum.sort(keys)
  end

  # Retry logic for lock acquisition
  defp do_acquire(_table, _lock_keys, 0, _retry_interval, _lock_timeout, _attempt) do
    {:error, :timeout}
  end

  defp do_acquire(table, lock_keys, :infinity, retry_interval, lock_timeout, attempt) do
    with :error <- try_acquire_all(table, lock_keys, lock_timeout) do
      sleep_with_jitter(retry_interval, attempt)

      do_acquire(table, lock_keys, :infinity, retry_interval, lock_timeout, attempt + 1)
    end
  end

  defp do_acquire(table, lock_keys, retries, retry_interval, lock_timeout, attempt)
       when is_integer(retries) and retries > 0 do
    with :error <- try_acquire_all(table, lock_keys, lock_timeout) do
      if attempt < retries do
        sleep_with_jitter(retry_interval, attempt)

        do_acquire(table, lock_keys, retries, retry_interval, lock_timeout, attempt + 1)
      else
        {:error, :timeout}
      end
    end
  end

  # Try to acquire all locks atomically (all-or-nothing)
  defp try_acquire_all(table, keys, timeout) do
    now = Time.now()
    owner = self()

    # Build all lock records upfront
    locks = Enum.map(keys, &lock(key: &1, owner: owner, timestamp: now, timeout: timeout))

    # Fast path: try to insert all locks at once (O(1))
    with :error <- insert_new(table, locks),
         # Slow path: check for stale locks with early termination
         :ok <- cleanup_stale_locks(table, keys, now) do
      # Stale locks were cleaned, retry batch insert (inner retry)
      insert_new(table, locks)
    end
  end

  # Insert new lock records atomically
  defp insert_new(table, lock_records) do
    case :ets.insert_new(table, lock_records) do
      true -> :ok
      false -> :error
    end
  end

  # Check all keys for stale locks with early termination on valid lock
  defp cleanup_stale_locks(table, lock_keys, now) do
    with stale_keys when is_list(stale_keys) <-
           Enum.reduce_while(lock_keys, [], &check_stale_key(&1, &2, table, now)) do
      # Delete all stale locks (could be empty if locks were released)
      Enum.each(stale_keys, &:ets.delete(table, &1))
    end
  end

  defp check_stale_key(lock_key, stale_acc, table, now) do
    case :ets.lookup(table, lock_key) do
      # This branch handles the race condition where a lock exists during
      # the batch insert attempt but is released before we check it during
      # stale lock detection. Difficult to test deterministically without
      # introducing flaky tests.
      # coveralls-ignore-start
      [] ->
        # No lock exists, continue checking
        {:cont, stale_acc}

      # coveralls-ignore-stop

      [lock(key: ^lock_key, owner: pid, timestamp: timestamp, timeout: timeout)] ->
        if stale_lock?(pid, timestamp, now, timeout) do
          # Stale lock found, mark for cleanup and continue
          {:cont, [lock_key | stale_acc]}
        else
          # Valid lock found, stop immediately
          {:halt, :error}
        end
    end
  end

  # Check if a lock is stale (dead process or timed out)
  defp stale_lock?(pid, timestamp, now, lock_timeout) do
    !Process.alive?(pid) or now - timestamp > lock_timeout
  end

  # Sleep with custom interval (function-based, no jitter)
  defp sleep_with_jitter(interval_fun, attempt) when is_function(interval_fun, 1) do
    case interval_fun.(attempt) do
      interval when is_integer(interval) and interval >= 0 ->
        Process.sleep(interval)

      other ->
        raise ArgumentError,
              "retry_interval function must return a non-negative integer, got: #{inspect(other)}"
    end
  end

  # Sleep with random jitter to prevent thundering herd (integer or :infinity)
  defp sleep_with_jitter(interval, _attempt) do
    jitter = :rand.uniform(interval)
    sleep_time = interval + jitter

    Process.sleep(sleep_time)
  end

  # Cleanup stale locks from the table
  defp cleanup_stale_locks(table, cleanup_batch_size) do
    now = Time.now()

    # Fix the table to prevent inconsistencies during cleanup
    :ets.safe_fixtable(table, true)

    # Select locks in batches using continuation
    case :ets.select(table, [{:"$1", [], [:"$1"]}], cleanup_batch_size) do
      :"$end_of_table" ->
        :ok

      {locks, continuation} ->
        cleanup_batch(table, locks, now)
        cleanup_with_continuation(table, continuation, now)
    end
  after
    # Unfix the table
    :ets.safe_fixtable(table, false)
  end

  # Cleanup locks using continuation
  defp cleanup_with_continuation(_table, :"$end_of_table", _now) do
    :ok
  end

  defp cleanup_with_continuation(table, continuation, now) do
    case :ets.select(continuation) do
      :"$end_of_table" ->
        :ok

      {locks, next_continuation} ->
        cleanup_batch(table, locks, now)
        cleanup_with_continuation(table, next_continuation, now)
    end
  end

  # Cleanup a batch of locks
  defp cleanup_batch(table, locks, now) do
    Enum.each(locks, fn lock(key: key, owner: pid, timestamp: ts, timeout: timeout) ->
      if stale_lock?(pid, ts, now, timeout) do
        :ets.delete(table, key)
      end
    end)
  end
end
