defmodule Nebulex.Adapters.Local.InfoStatsTest do
  use ExUnit.Case, async: true

  defmodule Cache do
    @moduledoc false
    use Nebulex.Cache,
      otp_app: :nebulex_local,
      adapter: Nebulex.Adapters.Local
  end

  import Nebulex.CacheCase

  alias Nebulex.Adapters.Common.Info.Stats

  ## Tests

  describe "stats/0" do
    setup_with_cache Cache, stats: true

    test "hits and misses" do
      :ok = Cache.put_all!(a: 1, b: 2)

      assert Cache.get!(:a) == 1
      assert Cache.has_key?(:a)
      assert Cache.ttl!(:b) == :infinity
      refute Cache.get!(:c)
      refute Cache.get!(:d)

      assert Cache.get_all!(in: [:a, :b, :c, :d]) |> Map.new() == %{a: 1, b: 2}

      assert Cache.info!(:stats) == %{
               hits: 5,
               misses: 4,
               writes: 2,
               evictions: 0,
               expirations: 0,
               deletions: 0,
               updates: 0
             }
    end

    test "writes and updates" do
      assert Cache.put_all!(a: 1, b: 2) == :ok
      assert Cache.put_all(%{a: 1, b: 2}) == :ok
      refute Cache.put_new_all!(a: 1, b: 2)
      assert Cache.put_new_all!(c: 3, d: 4, e: 3)
      assert Cache.put!(1, 1) == :ok
      refute Cache.put_new!(1, 2)
      refute Cache.replace!(2, 2)
      assert Cache.put_new!(2, 2)
      assert Cache.replace!(2, 22)
      assert Cache.incr!(:counter) == 1
      assert Cache.incr!(:counter) == 2
      refute Cache.expire!(:f, 1000)
      assert Cache.expire!(:a, 1000)
      refute Cache.touch!(:f)
      assert Cache.touch!(:b)

      _ = t_sleep(1100)

      refute Cache.get!(:a)

      wait_until(fn ->
        assert Cache.info!(:stats) == %{
                 hits: 0,
                 misses: 1,
                 writes: 10,
                 evictions: 1,
                 expirations: 1,
                 deletions: 1,
                 updates: 4
               }
      end)
    end

    test "deletions" do
      entries = for x <- 1..10, do: {x, x}
      :ok = Cache.put_all!(entries)

      assert Cache.delete!(1) == :ok
      assert Cache.take!(2) == 2

      assert_raise Nebulex.KeyError, fn ->
        Cache.take!(20)
      end

      assert Cache.info!(:stats) == %{
               hits: 1,
               misses: 1,
               writes: 10,
               evictions: 0,
               expirations: 0,
               deletions: 2,
               updates: 0
             }

      assert Cache.delete_all!() == 8

      assert Cache.info!(:stats) == %{
               hits: 1,
               misses: 1,
               writes: 10,
               evictions: 0,
               expirations: 0,
               deletions: 10,
               updates: 0
             }
    end

    test "expirations" do
      :ok = Cache.put_all!(a: 1, b: 2)
      :ok = Cache.put_all!([c: 3, d: 4], ttl: 1000)

      assert Cache.get_all!(in: [:a, :b, :c, :d]) |> Map.new() == %{a: 1, b: 2, c: 3, d: 4}

      _ = t_sleep(1100)

      # The `get_all` doesn't trigger the expiration
      assert Cache.get_all!(in: [:a, :b, :c, :d]) |> Map.new() == %{a: 1, b: 2}

      # The `get` will trigger the expiration
      refute Cache.get!(:c)
      refute Cache.get!(:d)

      wait_until(fn ->
        assert Cache.info!(:stats) == %{
                 hits: 6,
                 misses: 4,
                 writes: 4,
                 evictions: 2,
                 expirations: 2,
                 deletions: 2,
                 updates: 0
               }
      end)
    end
  end

  describe "disabled stats:" do
    setup_with_cache Cache, stats: false

    test "c:Nebulex.Cache.stats/1 returns empty stats when counter is not set" do
      assert Cache.info!(:stats) == Stats.new()
    end
  end
end
