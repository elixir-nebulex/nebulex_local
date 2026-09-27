defmodule Nebulex.Adapters.LocalOrderedSetTest do
  use ExUnit.Case, async: true

  alias Nebulex.Adapters.LocalOrderedSetTest.{ETS, Shards}

  ## Internals

  defmodule ETS do
    use Nebulex.Cache,
      otp_app: :nebulex_local,
      adapter: Nebulex.Adapters.Local
  end

  defmodule Shards do
    use Nebulex.Cache,
      otp_app: :nebulex_local,
      adapter: Nebulex.Adapters.Local
  end

  ## Tests

  setup do
    {:ok, ets} = ETS.start_link(backend_type: :ordered_set)
    {:ok, shards} = Shards.start_link(backend: :shards, backend_type: :ordered_set)

    on_exit(fn ->
      :ok = Process.sleep(100)

      if Process.alive?(ets), do: ETS.stop()

      if Process.alive?(shards), do: Shards.stop()
    end)

    {:ok, caches: [ETS, Shards]}
  end

  describe "{:in, keys} queries on ordered_set" do
    test "match keys with the table's native == semantics", %{caches: caches} do
      for_all_caches(caches, fn cache ->
        :ok = cache.put(1, "one")

        assert cache.get!(1.0) == "one"
        assert cache.get_all!(in: [1.0]) == [{1, "one"}]
        assert cache.count_all!(in: [1.0]) == 1
        assert cache.stream!(in: [1.0]) |> Enum.to_list() == [{1, "one"}]
        assert cache.delete_all!(in: [1.0]) == 1

        assert cache.get!(1) == nil
      end)
    end

    test "process ==-equal duplicate keys once", %{caches: caches} do
      for_all_caches(caches, fn cache ->
        :ok = cache.put(1, "one")

        assert cache.get_all!(in: [1, 1.0]) == [{1, "one"}]
        assert cache.stream!(in: [1, 1.0]) |> Enum.to_list() == [{1, "one"}]
        assert cache.stream!([in: [1, 2, 1.0]], max_entries: 2) |> Enum.to_list() == [{1, "one"}]
        assert cache.count_all!(in: [1, 1.0]) == 1
        assert cache.delete_all!(in: [1, 1.0]) == 1

        assert cache.get!(1) == nil
      end)
    end

    test "count_all skips expired entries and delete_all lazily removes them", %{caches: caches} do
      for_all_caches(caches, fn cache ->
        :ok = cache.put(1, "one", ttl: 10)

        :ok = Process.sleep(50)

        # `count_all` is read-only: the expired entry stays in the table.
        assert cache.count_all!(in: [1]) == 0
        assert cache.count_all!(query: :expired) == 1

        # `delete_all` does not count the expired entry, but removes it.
        assert cache.delete_all!(in: [1]) == 0

        assert cache.count_all!(query: :expired) == 0
      end)
    end

    test "match non-indexable keys through lookups", %{caches: caches} do
      for_all_caches(caches, fn cache ->
        :ok = cache.put_all(%{%{a: 1} => 1, :"$1" => 2, :_ => 3})

        assert cache.get_all!(in: [%{a: 1}]) == [{%{a: 1}, 1}]
        assert cache.count_all!(in: [%{a: 1}, :"$1", :_]) == 3
        assert cache.delete_all!(in: [%{a: 1}, :"$1"]) == 2

        assert cache.count_all!() == 1
      end)
    end

    test "match keys in the older generation", %{caches: caches} do
      for_all_caches(caches, fn cache ->
        :ok = cache.put(1, "one")

        _ = cache.new_generation()

        assert cache.count_all!(in: [1.0]) == 1
        assert cache.delete_all!(in: [1.0]) == 1

        assert cache.get!(1) == nil
      end)
    end
  end

  ## Helpers

  defp for_all_caches(caches, fun) do
    Enum.each(caches, fn cache ->
      fun.(cache)
    end)
  end
end
