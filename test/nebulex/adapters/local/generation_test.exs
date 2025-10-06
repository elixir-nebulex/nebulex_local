defmodule Nebulex.Adapters.Local.GenerationTest do
  use ExUnit.Case, async: true

  defmodule LocalWithSizeLimit do
    @moduledoc false
    use Nebulex.Cache,
      otp_app: :nebulex_local,
      adapter: Nebulex.Adapters.Local
  end

  import Nebulex.CacheCase

  alias Nebulex.Adapters.Local.Generation
  alias Nebulex.Adapters.Local.GenerationTest.LocalWithSizeLimit
  alias Nebulex.Adapters.Local.TestCache, as: Cache

  describe "init" do
    test "ok: with default options" do
      assert {:ok, _pid} = LocalWithSizeLimit.start_link()

      assert %Nebulex.Adapters.Local.Generation{
               allocated_memory: nil,
               backend: :ets,
               backend_opts: [
                 :set,
                 :public,
                 {:keypos, 2},
                 {:read_concurrency, true},
                 {:write_concurrency, true}
               ],
               gc_healthcheck_ref: nil,
               gc_heartbeat_ref: nil,
               gc_interval: nil,
               gc_memory_check_interval: 10_000,
               max_size: nil,
               meta_tab: meta_tab,
               stats_counter: {:write_concurrency, _}
             } = Generation.get_state(LocalWithSizeLimit)

      assert is_reference(meta_tab)

      :ok = LocalWithSizeLimit.stop()
    end

    test "ok: with custom options" do
      assert {:ok, _pid} =
               LocalWithSizeLimit.start_link(
                 gc_interval: 10,
                 max_size: 10,
                 allocated_memory: 1000
               )

      assert %Nebulex.Adapters.Local.Generation{
               allocated_memory: 1000,
               backend: :ets,
               backend_opts: [
                 :set,
                 :public,
                 {:keypos, 2},
                 {:read_concurrency, true},
                 {:write_concurrency, true}
               ],
               gc_healthcheck_ref: gc_healthcheck_ref,
               gc_heartbeat_ref: gc_heartbeat_ref,
               gc_interval: 10,
               gc_memory_check_interval: 10_000,
               max_size: 10,
               meta_tab: meta_tab,
               stats_counter: {:write_concurrency, _}
             } = Generation.get_state(LocalWithSizeLimit)

      assert is_reference(gc_healthcheck_ref)
      assert is_reference(gc_heartbeat_ref)
      assert is_reference(meta_tab)

      :ok = LocalWithSizeLimit.stop()
    end

    test "error: invalid :gc_memory_check_interval option" do
      _ = Process.flag(:trap_exit, true)

      assert {:error, {%NimbleOptions.ValidationError{message: err}, _}} =
               LocalWithSizeLimit.start_link(
                 gc_interval: 3600,
                 gc_memory_check_interval: -1
               )

      assert Regex.match?(~r/invalid value for :gc_memory_check_interval/, err)

      assert {:error, {%NimbleOptions.ValidationError{message: err}, _}} =
               LocalWithSizeLimit.start_link(
                 gc_interval: 3600,
                 gc_memory_check_interval: & &1
               )

      assert Regex.match?(~r/invalid value for :gc_memory_check_interval/, err)
    end
  end

  for backend <- [:ets, :shards] do
    describe "(#{backend} gc" do
      setup_with_dynamic_cache Cache,
                               :gc_test,
                               backend: unquote(backend),
                               gc_interval: 1000,
                               compressed: true

      test "create generations", %{cache: cache, name: name} do
        assert generations_len(name) == 1

        :ok = Process.sleep(1020)
        assert generations_len(name) == 2

        assert cache.delete_all!() == 0

        :ok = Process.sleep(1020)
        assert generations_len(name) == 2
      end

      test "create new generation and reset timeout", %{cache: cache, name: name} do
        assert generations_len(name) == 1

        :ok = Process.sleep(800)

        cache.with_dynamic_cache(name, fn ->
          cache.new_generation()
        end)

        assert generations_len(name) == 2

        :ok = Process.sleep(500)
        assert generations_len(name) == 2

        :ok = Process.sleep(520)
        assert generations_len(name) == 2
      end

      test "create new generation without reset timeout", %{cache: cache, name: name} do
        assert generations_len(name) == 1

        :ok = Process.sleep(800)

        cache.with_dynamic_cache(name, fn ->
          cache.new_generation(gc_interval_reset: false)
        end)

        assert generations_len(name) == 2

        :ok = Process.sleep(500)
        assert generations_len(name) == 2
      end

      test "reset GC interval", %{cache: cache, name: name} do
        assert generations_len(name) == 1

        :ok = Process.sleep(800)

        cache.with_dynamic_cache(name, fn ->
          cache.reset_gc_interval()
        end)

        :ok = Process.sleep(220)
        assert generations_len(name) == 1

        :ok = Process.sleep(1000)
        assert generations_len(name) == 2
      end
    end

    describe "(#{backend} allocated memory" do
      test "healthcheck is triggered when max generation size is reached" do
        {:ok, _pid} =
          LocalWithSizeLimit.start_link(
            gc_interval: :timer.hours(1),
            gc_memory_check_interval: &__MODULE__.mem_check_interval/3,
            allocated_memory: 100_000,
            gc_cleanup_delay: 1,
            backend: unquote(backend)
          )

        assert generations_len(LocalWithSizeLimit) == 1

        {mem_size, _} = Generation.memory_info(LocalWithSizeLimit)
        :ok = Generation.realloc(LocalWithSizeLimit, mem_size * 2)

        # Trigger the healthcheck event
        :ok = check_cache_size(LocalWithSizeLimit, 1000)

        assert generations_len(LocalWithSizeLimit) == 1
        assert_mem_size(:<=)

        :ok = flood_cache(mem_size, mem_size * 2)

        # Wait until the mem is less than the max (healthcheck has run many times)
        wait_until(fn ->
          assert generations_len(LocalWithSizeLimit) == 2
          assert_mem_size(:<=)
        end)

        :ok = flood_cache(mem_size, mem_size * 2)

        wait_until(fn ->
          assert generations_len(LocalWithSizeLimit) == 2
          assert_mem_size(:>)
        end)

        :ok = flood_cache(mem_size, mem_size * 2)

        wait_until(fn ->
          assert generations_len(LocalWithSizeLimit) == 2
          assert_mem_size(:>)
        end)

        # Trigger the healthcheck event
        :ok = Enum.each(1..3, fn _ -> check_cache_size(LocalWithSizeLimit) end)

        wait_until(fn ->
          assert generations_len(LocalWithSizeLimit) == 2
          assert_mem_size(:<=)
        end)

        :ok = LocalWithSizeLimit.stop()
      end

      test "healthcheck while cache is being used" do
        {:ok, _pid} =
          LocalWithSizeLimit.start_link(
            gc_interval: :timer.hours(1),
            gc_memory_check_interval: :timer.seconds(1),
            allocated_memory: 100,
            backend: unquote(backend)
          )

        assert generations_len(LocalWithSizeLimit) == 1

        tasks = for i <- 1..3, do: Task.async(fn -> task_fun(LocalWithSizeLimit, i) end)

        for _ <- 1..100 do
          :ok = Process.sleep(10)

          LocalWithSizeLimit
          |> Generation.server()
          |> send(:healthcheck)
        end

        :ok = Enum.each(tasks, &Task.shutdown/1)

        :ok = LocalWithSizeLimit.stop()
      end
    end

    describe "(#{backend} max size" do
      test "healthcheck is triggered when size limit is reached" do
        {:ok, _pid} =
          LocalWithSizeLimit.start_link(
            gc_interval: :timer.hours(1),
            max_size: 3,
            gc_memory_check_interval: :timer.seconds(1),
            backend: unquote(backend)
          )

        # Initially there should be only 1 generation and no entries
        assert generations_len(LocalWithSizeLimit) == 1
        assert LocalWithSizeLimit.count_all!() == 0

        # Put some entries to exceed the max size
        _ = cache_put(LocalWithSizeLimit, 1..4)

        # Validate current size
        assert LocalWithSizeLimit.count_all!() == 4

        # Wait the max healthcheck timeout
        :ok = Process.sleep(1600)

        # There should be 2 generation now
        assert generations_len(LocalWithSizeLimit) == 2

        # The entries should be now in the older generation
        assert LocalWithSizeLimit.count_all!() == 4

        # Wait the min healthcheck timeout since max size is exceeded
        :ok = Process.sleep(1100)

        # Cache should be empty now
        assert LocalWithSizeLimit.count_all!() == 0

        # Put some entries without exceeding the max size
        _ = cache_put(LocalWithSizeLimit, 5..6)

        # Validate current size
        assert LocalWithSizeLimit.count_all!() == 2

        # Wait the max healthcheck timeout
        :ok = Process.sleep(1600)

        # The entries should be in the newer generation yet
        assert LocalWithSizeLimit.count_all!() == 2

        # Put some entries to exceed the max size
        _ = cache_put(LocalWithSizeLimit, 7..8)

        # Wait the max healthcheck timeout
        :ok = Process.sleep(1600)

        # The entries should be in the newer generation yet
        assert LocalWithSizeLimit.count_all!() == 4

        # Wait the min healthcheck timeout since max size is exceeded
        :ok = Process.sleep(1100)

        # Cache should be empty now
        assert LocalWithSizeLimit.count_all!() == 0

        # Stop the cache
        :ok = LocalWithSizeLimit.stop()
      end

      test "healthcheck works ok when gc_interval not set or is nil" do
        {:ok, _pid} =
          LocalWithSizeLimit.start_link(
            max_size: 3,
            gc_memory_check_interval: :timer.seconds(1),
            backend: unquote(backend)
          )

        # Put some entries to exceed the max size
        _ = cache_put(LocalWithSizeLimit, 1..4)

        # Wait the max healthcheck timeout
        :ok = Process.sleep(1600)

        # Assert not crashed
        assert LocalWithSizeLimit.count_all!() == 4

        # Stop the cache
        :ok = LocalWithSizeLimit.stop()
      end
    end
  end

  ## Private Functions

  defp check_cache_size(cache, sleep \\ nil) do
    :healthcheck =
      cache
      |> Generation.server()
      |> send(:healthcheck)

    if is_integer(sleep) and sleep > 0 do
      Process.sleep(sleep)
    else
      :ok
    end
  end

  defp flood_cache(mem_size, max_size) when mem_size > max_size do
    :ok
  end

  defp flood_cache(mem_size, max_size) when mem_size <= max_size do
    :ok =
      100_000
      |> :rand.uniform()
      |> LocalWithSizeLimit.put(generate_value(1000))

    :ok = Process.sleep(500)
    {mem_size, _} = Generation.memory_info(LocalWithSizeLimit)

    flood_cache(mem_size, max_size)
  end

  defp assert_mem_size(greater_or_less) do
    {mem_size, max_size} = Generation.memory_info(LocalWithSizeLimit)

    assert apply(Kernel, greater_or_less, [mem_size, max_size])
  end

  defp generate_value(n) do
    Enum.map(1..n, &to_string/1)
  end

  defp generations_len(name) do
    name
    |> Generation.list()
    |> Enum.count()
  end

  defp task_fun(cache, i) do
    :ok = cache.put("#{inspect(self())}.#{i}", i)

    :ok = Process.sleep(1)

    task_fun(cache, i + 1)
  end

  def mem_check_interval(_info, size, max_size) do
    mem_check_interval(size, max_size, :timer.seconds(1), :timer.seconds(3))
  end

  defp mem_check_interval(size, _max_size, _min_timeout, max_timeout) when size <= 0 do
    max_timeout
  end

  defp mem_check_interval(size, max_size, min_timeout, _max_timeout) when size >= max_size do
    min_timeout
  end

  defp mem_check_interval(size, max_size, min_timeout, max_timeout) do
    round((min_timeout - max_timeout) / max_size * size + max_timeout)
  end
end
