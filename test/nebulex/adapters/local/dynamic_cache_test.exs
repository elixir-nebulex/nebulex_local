defmodule Nebulex.Adapters.Local.DynamicCacheTest do
  use ExUnit.Case, async: false

  alias Nebulex.Adapters.Local.TestCache, as: Cache

  for backend <- [:ets, :shards], default_started? <- [false, true] do
    describe "#{backend} unnamed cache with default instance #{default_started?}" do
      setup do
        if unquote(default_started?) do
          _ = start_supervised!({Cache, backend: unquote(backend)})

          assert Cache.put(:key, :default_instance) == :ok
        end

        pid =
          {Cache, name: nil, backend: unquote(backend)}
          |> Supervisor.child_spec(id: :unnamed_cache)
          |> start_supervised!()

        _ = Cache.put_dynamic_cache(pid)
        assert Cache.put(:key, :isolated, ttl: 60_000) == :ok
        assert Cache.new_generation() == :ok

        :ok
      end

      test "fetch promotes the selected instance's entry" do
        assert Cache.fetch(:key) == {:ok, :isolated}
      end

      test "get_all promotes the selected instance's entry" do
        assert Cache.get_all(in: [:key]) == {:ok, [key: :isolated]}
      end

      test "ttl promotes the selected instance's entry" do
        assert {:ok, ttl} = Cache.ttl(:key)

        assert ttl in 1..60_000
      end
    end
  end
end
