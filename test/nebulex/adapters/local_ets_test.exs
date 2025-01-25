defmodule Nebulex.Adapters.LocalEtsTest do
  use ExUnit.Case, async: true

  # Inherit tests
  use Nebulex.Adapters.LocalTest
  use Nebulex.Adapters.Local.CacheTestCase

  import Nebulex.CacheCase, only: [setup_with_dynamic_cache: 3, t_sleep: 1]

  alias Nebulex.Adapter
  alias Nebulex.Adapters.Local.TestCache, as: Cache

  setup_with_dynamic_cache Cache, :local_with_ets, purge_chunk_size: 10

  describe "ets" do
    test "backend", %{name: name} do
      assert Adapter.lookup_meta(name).backend == :ets
    end
  end

  test "replace!/3 [keep_ttl: true]", %{cache: cache, name: name} do
    assert cache.put!("foo", "bar", ttl: 500) == :ok
    assert cache.has_key?("foo") == {:ok, true}

    cache.with_dynamic_cache(name, fn ->
      cache.new_generation()
    end)

    assert cache.replace!("foo", "bar bar", keep_ttl: true) == true
    assert cache.replace!("foo", "bar bar", keep_ttl: true) == true

    _ = t_sleep(600)

    assert cache.has_key?("foo") == {:ok, false}
  end
end
