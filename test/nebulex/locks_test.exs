defmodule Nebulex.LocksTest do
  use ExUnit.Case, async: true

  alias Nebulex.Locks

  import Record

  # Import lock record definition
  defrecord(:lock, key: nil, owner: nil, timestamp: nil, lock_timeout: nil)

  describe "named mode" do
    setup do
      pid = start_supervised!({Locks, name: __MODULE__})

      {:ok, table: __MODULE__, pid: pid}
    end

    test "start_link with name registers both GenServer and ETS table", %{table: table, pid: pid} do
      assert Process.whereis(table) == pid

      # Table should be named and accessible
      assert :ets.info(table) != :undefined
    end

    test "acquire and release using table name", %{table: table} do
      # Acquire locks using the name directly
      assert :ok = Locks.acquire(table, [:key1, :key2])

      # Verify locks exist in ETS
      assert [lock(key: :key1)] = :ets.lookup(table, :key1)
      assert [lock(key: :key2)] = :ets.lookup(table, :key2)
    end

    test "concurrent acquire attempts with named table", %{table: table} do
      # First process acquires lock
      assert :ok = Locks.acquire(table, [:shared_key])

      # Second process should timeout
      task =
        Task.async(fn ->
          Locks.acquire(table, [:shared_key], retries: 2, retry_interval: 10)
        end)

      assert Task.await(task) == {:error, :timeout}

      # Release lock
      assert Locks.release(table, [:shared_key]) == :ok
    end
  end

  describe "anonymous mode" do
    setup do
      pid = start_supervised!(Locks)
      table = Locks.get_table(pid)

      {:ok, table: table, pid: pid}
    end

    test "start_link without name returns unnamed GenServer", %{table: table} do
      assert Process.whereis(:unnamed_locks) == nil
      assert is_reference(table)
    end

    test "acquire and release using table reference", %{table: table} do
      # Acquire locks using table reference
      assert Locks.acquire(table, [:key1, :key2]) == :ok

      # Verify locks exist in ETS
      assert [lock(key: :key1)] = :ets.lookup(table, :key1)
      assert [lock(key: :key2)] = :ets.lookup(table, :key2)

      # Release locks using table reference
      assert Locks.release(table, [:key1, :key2]) == :ok

      # Verify locks are removed
      assert :ets.lookup(table, :key1) == []
      assert :ets.lookup(table, :key2) == []
    end

    test "concurrent acquire attempts with table reference", %{table: table} do
      # First process acquires lock
      assert Locks.acquire(table, [:shared_key]) == :ok

      # Second process should timeout
      task =
        Task.async(fn ->
          Locks.acquire(table, [:shared_key], retries: 2, retry_interval: 10)
        end)

      assert Task.await(task) == {:error, :timeout}

      # Release lock
      assert Locks.release(table, [:shared_key]) == :ok
    end
  end

  describe "lock acquisition" do
    setup do
      pid = start_supervised!({Locks, name: __MODULE__})
      table = Locks.get_table(__MODULE__)

      {:ok, table: table, pid: pid}
    end

    test "acquire with empty keys uses global lock", %{table: table} do
      assert :ok = Locks.acquire(table, [])

      # Should create a global lock entry
      assert [lock(key: :__global_lock__)] = :ets.lookup(table, :__global_lock__)

      assert Locks.release(table, []) == :ok
    end

    test "acquire sorts keys to prevent deadlocks", %{table: table} do
      # Keys should be sorted regardless of input order
      assert Locks.acquire(table, [:z, :a, :m]) == :ok

      # Verify all keys are locked
      assert [lock(key: :a)] = :ets.lookup(table, :a)
      assert [lock(key: :m)] = :ets.lookup(table, :m)
      assert [lock(key: :z)] = :ets.lookup(table, :z)

      assert Locks.release(table, [:z, :a, :m]) == :ok
    end

    test "stale lock detection - dead process", %{table: table} do
      # Create a lock from a dead process
      dead_pid = spawn(fn -> :ok end)
      Process.sleep(10)
      refute Process.alive?(dead_pid)

      # Manually insert stale lock
      :ets.insert(
        table,
        lock(
          key: :stale_key,
          owner: dead_pid,
          timestamp: System.system_time(:millisecond),
          lock_timeout: :timer.seconds(30)
        )
      )

      # Should be able to acquire the stale lock
      assert Locks.acquire(table, [:stale_key]) == :ok

      # Verify lock is now owned by current process
      assert [lock(key: :stale_key, owner: pid)] = :ets.lookup(table, :stale_key)
      assert pid == self()

      assert Locks.release(table, [:stale_key]) == :ok
    end

    test "stale lock detection - timed out", %{table: table} do
      # Manually insert an old lock
      old_timestamp = System.system_time(:millisecond) - 60_000

      :ets.insert(
        table,
        lock(key: :old_key, owner: self(), timestamp: old_timestamp, lock_timeout: 1000)
      )

      # Should be able to acquire the timed-out lock
      assert Locks.acquire(table, [:old_key], lock_timeout: 1000) == :ok

      # Verify lock timestamp is updated
      assert [lock(key: :old_key, timestamp: new_timestamp)] = :ets.lookup(table, :old_key)
      assert new_timestamp > old_timestamp

      assert Locks.release(table, [:old_key]) == :ok
    end

    test "all-or-nothing acquisition", %{table: table} do
      # Lock one of the keys
      assert Locks.acquire(table, [:key1]) == :ok

      # Try to acquire multiple keys including the locked one
      task =
        Task.async(fn ->
          Locks.acquire(table, [:key1, :key2, :key3], retries: 1, retry_interval: 10)
        end)

      assert Task.await(task) == {:error, :timeout}

      # Verify that key2 and key3 were not locked
      assert :ets.lookup(table, :key2) == []
      assert :ets.lookup(table, :key3) == []

      assert Locks.release(table, [:key1]) == :ok
    end

    test "retry with jitter", %{table: table} do
      # First process locks a key
      assert Locks.acquire(table, [:retry_key]) == :ok

      # Second process tries to acquire with retries
      parent = self()

      task =
        Task.async(fn ->
          # Notify parent when we start
          send(parent, :started)

          # Try to acquire with retries
          result = Locks.acquire(table, [:retry_key], retries: 3, retry_interval: 50)

          send(parent, {:result, result})
        end)

      # Wait for task to start
      assert_receive :started, 100

      # Release lock after a short delay
      Process.sleep(100)
      assert Locks.release(table, [:retry_key]) == :ok

      # Task should eventually succeed
      assert_receive {:result, :ok}, 1000

      Task.await(task)
    end

    test "infinite retries", %{table: table} do
      # First process locks a key
      assert Locks.acquire(table, [:infinite_key]) == :ok

      parent = self()

      # Second process tries with infinite retries
      task =
        Task.async(fn ->
          send(parent, :started)

          Locks.acquire(table, [:infinite_key], retries: :infinity, retry_interval: 20)
        end)

      assert_receive :started, 100

      # Release after a delay
      Process.sleep(100)
      assert Locks.release(table, [:infinite_key]) == :ok

      # Should succeed eventually
      assert Task.await(task, 1000) == :ok
    end

    test "zero retries returns timeout immediately", %{table: table} do
      # First process locks a key
      assert Locks.acquire(table, [:zero_retry_key]) == :ok

      # Second process tries with zero retries
      assert Locks.acquire(table, [:zero_retry_key], retries: 0, retry_interval: 10) ==
               {:error, :timeout}

      assert Locks.release(table, [:zero_retry_key]) == :ok
    end

    test "retry_interval zero performs immediate retries without crashing", %{table: table} do
      # First process locks a key
      assert Locks.acquire(table, [:zero_interval_key]) == :ok

      # Integer retry_interval: 0 should not crash and should timeout quickly
      assert Locks.acquire(table, [:zero_interval_key], retries: 2, retry_interval: 0) ==
               {:error, :timeout}

      assert Locks.release(table, [:zero_interval_key]) == :ok
    end
  end

  describe "periodic cleanup" do
    setup do
      pid =
        start_supervised!({Locks, name: __MODULE__, cleanup_interval: 100, cleanup_batch_size: 5})

      table = Locks.get_table(__MODULE__)

      {:ok, table: table, pid: pid}
    end

    test "cleans up stale locks from dead processes", %{table: table} do
      # Create a lock from a dead process
      dead_pid = spawn(fn -> :ok end)
      Process.sleep(10)
      refute Process.alive?(dead_pid)

      # Manually insert stale lock
      :ets.insert(
        table,
        lock(
          key: :cleanup_key,
          owner: dead_pid,
          timestamp: System.system_time(:millisecond),
          lock_timeout: :timer.seconds(30)
        )
      )

      # Verify lock exists
      assert [lock(key: :cleanup_key)] = :ets.lookup(table, :cleanup_key)

      # Wait for cleanup to run (cleanup_interval + buffer)
      Process.sleep(200)

      # Lock should be cleaned up
      assert :ets.lookup(table, :cleanup_key) == []
    end

    test "cleans up timed-out locks", %{table: table} do
      # Insert an old lock (expired)
      old_timestamp = System.system_time(:millisecond) - :timer.seconds(60)

      :ets.insert(
        table,
        lock(
          key: :old_cleanup_key,
          owner: self(),
          timestamp: old_timestamp,
          lock_timeout: :timer.seconds(30)
        )
      )

      # Verify lock exists
      assert [lock(key: :old_cleanup_key)] = :ets.lookup(table, :old_cleanup_key)

      # Wait for cleanup to run
      Process.sleep(200)

      # Lock should be cleaned up
      assert :ets.lookup(table, :old_cleanup_key) == []
    end

    test "cleans up large number of stale locks using continuation", %{table: table} do
      # Insert more than 100 stale locks to test continuation
      dead_pid = spawn(fn -> :ok end)
      Process.sleep(10)
      refute Process.alive?(dead_pid)

      for i <- 1..10 do
        :ets.insert(
          table,
          lock(
            key: :"cleanup_key_#{i}",
            owner: dead_pid,
            timestamp: System.system_time(:millisecond),
            lock_timeout: :timer.seconds(30)
          )
        )
      end

      # Verify locks exist
      assert :ets.info(table, :size) == 10

      # Wait for cleanup to run
      Process.sleep(200)

      # All locks should be cleaned up
      assert :ets.info(table, :size) == 0
    end

    test "cleanup handles empty table", %{table: table} do
      # Table is empty
      assert :ets.info(table, :size) == 0

      # Wait for cleanup to run (should handle empty table gracefully)
      Process.sleep(200)

      # Table should still be empty
      assert :ets.info(table, :size) == 0
    end

    test "cleanup handles exactly batch size locks", %{table: table} do
      # Insert exactly 100 locks (the batch size)
      dead_pid = spawn(fn -> :ok end)
      Process.sleep(10)
      refute Process.alive?(dead_pid)

      for i <- 1..100 do
        :ets.insert(
          table,
          lock(
            key: :"exact_#{i}",
            owner: dead_pid,
            timestamp: System.system_time(:millisecond),
            lock_timeout: :timer.seconds(30)
          )
        )
      end

      # Verify locks exist
      assert :ets.info(table, :size) == 100

      # Wait for cleanup to run
      Process.sleep(200)

      # All locks should be cleaned up
      assert :ets.info(table, :size) == 0
    end
  end

  describe "function-based retry_interval" do
    setup do
      pid = start_supervised!({Locks, name: __MODULE__})
      table = Locks.get_table(__MODULE__)

      {:ok, table: table, pid: pid}
    end

    test "exponential backoff with function", %{table: table} do
      # First process locks a key
      assert Locks.acquire(table, [:exp_backoff_key]) == :ok

      parent = self()

      # Track intervals used during retries
      task =
        Task.async(fn ->
          send(parent, :started)

          # Try to acquire with exponential backoff
          result =
            Locks.acquire(
              table,
              [:exp_backoff_key],
              retries: 3,
              retry_interval: fn attempt -> round(10 * :math.pow(2, attempt)) end
            )

          send(parent, {:result, result})
        end)

      # Wait for task to start
      assert_receive :started, 100

      # Should timeout after retries (10ms, 20ms, 40ms)
      assert_receive {:result, {:error, :timeout}}, 1000

      Task.await(task)

      # Clean up
      assert Locks.release(table, [:exp_backoff_key]) == :ok
    end

    test "linear backoff with function", %{table: table} do
      # First process locks a key
      assert Locks.acquire(table, [:linear_backoff_key]) == :ok

      parent = self()

      task =
        Task.async(fn ->
          send(parent, :started)

          # Linear backoff: 10ms, 20ms, 30ms
          result =
            Locks.acquire(
              table,
              [:linear_backoff_key],
              retries: 3,
              retry_interval: fn attempt -> 10 + attempt * 10 end
            )

          send(parent, {:result, result})
        end)

      assert_receive :started, 100
      assert_receive {:result, {:error, :timeout}}, 1000

      Task.await(task)

      # Clean up
      assert Locks.release(table, [:linear_backoff_key]) == :ok
    end

    test "custom strategy with manual jitter", %{table: table} do
      # First process locks a key
      assert Locks.acquire(table, [:custom_strategy_key]) == :ok

      parent = self()

      task =
        Task.async(fn ->
          send(parent, :started)

          # Custom strategy with manual jitter
          result =
            Locks.acquire(
              table,
              [:custom_strategy_key],
              retries: 2,
              retry_interval: fn attempt ->
                base = 10 * (attempt + 1)
                jitter = :rand.uniform(base)
                base + jitter
              end
            )

          send(parent, {:result, result})
        end)

      assert_receive :started, 100
      assert_receive {:result, {:error, :timeout}}, 1000

      Task.await(task)

      # Clean up
      assert Locks.release(table, [:custom_strategy_key]) == :ok
    end

    test "function returning invalid value raises error", %{table: table} do
      # First process locks a key
      assert Locks.acquire(table, [:invalid_return_key]) == :ok

      # Try to acquire with function that returns invalid value
      assert_raise ArgumentError,
                   ~r/retry_interval function must return a non-negative integer/,
                   fn ->
                     Locks.acquire(
                       table,
                       [:invalid_return_key],
                       retries: 1,
                       retry_interval: fn _attempt -> :invalid end
                     )
                   end

      # Clean up
      assert Locks.release(table, [:invalid_return_key]) == :ok
    end

    test "function returning zero succeeds with immediate retry", %{table: table} do
      # First process locks a key
      assert Locks.acquire(table, [:zero_return_key]) == :ok

      parent = self()

      # Try to acquire with function that returns zero (immediate retry)
      task =
        Task.async(fn ->
          send(parent, :started)

          result =
            Locks.acquire(
              table,
              [:zero_return_key],
              retries: 2,
              retry_interval: fn _attempt -> 0 end
            )

          send(parent, {:result, result})
        end)

      # Should timeout but very quickly (no sleep between retries)
      assert_receive :started, 100
      assert_receive {:result, {:error, :timeout}}, 100

      Task.await(task)

      # Clean up
      assert Locks.release(table, [:zero_return_key]) == :ok
    end

    test "function returning negative value raises error", %{table: table} do
      # First process locks a key
      assert Locks.acquire(table, [:negative_return_key]) == :ok

      # Try to acquire with function that returns negative value
      assert_raise ArgumentError,
                   ~r/retry_interval function must return a non-negative integer/,
                   fn ->
                     Locks.acquire(
                       table,
                       [:negative_return_key],
                       retries: 1,
                       retry_interval: fn _attempt -> -10 end
                     )
                   end

      # Clean up
      assert Locks.release(table, [:negative_return_key]) == :ok
    end
  end

  describe "init_callback option" do
    test "invokes callback with table as first argument" do
      parent = self()
      callback_key = :test_locks_table

      start_supervised!(
        {Locks,
         name: InitCallbackTest, init_callback: {__MODULE__, :store_table, [parent, callback_key]}}
      )

      # Verify callback was invoked with correct arguments
      assert_receive {:callback_invoked, InitCallbackTest, ^callback_key}, 1000
    end
  end

  ## Private functions

  # Callback module for testing init_callback
  def store_table(table, pid, key) do
    send(pid, {:callback_invoked, table, key})
  end
end
