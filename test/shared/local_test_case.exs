defmodule Nebulex.Adapters.LocalTest do
  @moduledoc false

  import Nebulex.CacheCase

  deftests do
    import Ex2ms
    import Nebulex.CacheCase, only: [cache_put: 2, cache_put: 3, cache_put: 4, t_sleep: 1]

    alias Nebulex.Adapter

    describe "error" do
      test "on init because invalid backend", %{cache: cache} do
        _ = Process.flag(:trap_exit, true)

        assert {:error, {%NimbleOptions.ValidationError{message: msg}, _}} =
                 cache.start_link(name: :invalid_backend, backend: :xyz)

        assert Regex.match?(~r|invalid value for :backend option|, msg)
      end

      test "because cache is stopped", %{cache: cache, name: name} do
        :ok = stop_supervised!(name)

        ops = [
          fn -> cache.put(1, 13) end,
          fn -> cache.put!(1, 13) end,
          fn -> cache.get!(1) end,
          fn -> cache.delete!(1) end
        ]

        for fun <- ops do
          assert_raise Nebulex.CacheNotFoundError, ~r/unable to find cache: #{inspect(name)}/, fun
        end
      end
    end

    describe "KV API" do
      test "get_and_update", %{cache: cache} do
        fun = fn
          nil -> {nil, 1}
          val -> {val, val * 2}
        end

        assert cache.get_and_update!(1, fun) == {nil, 1}
        assert cache.get_and_update!(1, &{&1, &1 * 2}) == {1, 2}
        assert cache.get_and_update!(1, &{&1, &1 * 3}) == {2, 6}
        assert cache.get_and_update!(1, &{&1, nil}) == {6, 6}
        assert cache.get!(1) == 6
        assert cache.get_and_update!(1, fn _ -> :pop end) == {6, nil}
        assert cache.get_and_update!(1, fn _ -> :pop end) == {nil, nil}
        assert cache.get_and_update!(3, &{&1, 3}) == {nil, 3}
      end

      test "get_and_update fails because function returns invalid value", %{cache: cache} do
        assert_raise ArgumentError, fn ->
          cache.get_and_update(1, fn _ -> :other end)
        end
      end

      test "get_and_update fails because cache is not started", %{cache: cache} do
        :ok = cache.stop()

        assert_raise Nebulex.CacheNotFoundError, ~r/unable to find cache/, fn ->
          assert cache.get_and_update!(1, fn _ -> :pop end)
        end
      end

      test "incr and update", %{cache: cache} do
        assert cache.incr!(:counter) == 1
        assert cache.incr!(:counter) == 2

        assert cache.get_and_update!(:counter, &{&1, &1 * 2}) == {2, 4}
        assert cache.incr!(:counter) == 5

        assert cache.update!(:counter, 1, &(&1 * 2)) == 10
        assert cache.incr!(:counter, -10) == 0

        assert cache.put("foo", "bar") == :ok

        assert_raise ArgumentError, fn ->
          cache.incr!("foo")
        end
      end

      test "incr with ttl", %{cache: cache} do
        assert cache.incr!(:counter_with_ttl, 1, ttl: 1000) == 1
        assert cache.incr!(:counter_with_ttl) == 2
        assert cache.fetch!(:counter_with_ttl) == 2

        t = t_sleep(1010)

        assert {:error, %Nebulex.KeyError{key: :counter_with_ttl}} = cache.fetch(:counter_with_ttl)

        assert cache.incr!(:counter_with_ttl, 1, ttl: 5000) == 1
        assert {:ok, ttl} = cache.ttl(:counter_with_ttl)
        assert ttl > 1000

        assert cache.expire(:counter_with_ttl, 500) == {:ok, true}

        _ = t_sleep(t + 600)

        assert {:error, %Nebulex.KeyError{key: :counter_with_ttl}} = cache.fetch(:counter_with_ttl)
      end

      test "incr existing entry", %{cache: cache} do
        assert cache.put(:counter, 0) == :ok
        assert cache.incr!(:counter) == 1
        assert cache.incr!(:counter, 2) == 3
      end

      test "fetch_or_store stores the value in the cache if the key does not exist", %{cache: cache} do
        assert cache.fetch_or_store("lazy", fn -> {:ok, "value"} end) == {:ok, "value"}
        assert cache.get!("lazy") == "value"

        assert cache.fetch_or_store("lazy", fn -> {:ok, "new value"} end) == {:ok, "value"}
        assert cache.get!("lazy") == "value"
      end

      test "fetch_or_store returns error if the function returns an error", %{cache: cache} do
        assert {:error, %Nebulex.Error{reason: "error"}} =
                 cache.fetch_or_store("lazy", fn -> {:error, "error"} end)

        refute cache.get!("lazy")
      end

      test "fetch_or_store raises if the function returns an invalid value", %{cache: cache} do
        msg =
          "the supplied lambda function must return {:ok, value} or " <>
            "{:error, reason}, got: :invalid"

        assert_raise RuntimeError, msg, fn ->
          cache.fetch_or_store!("lazy", fn -> :invalid end)
        end
      end

      test "fetch_or_store! stores the value in the cache if the key does not exist", %{
        cache: cache
      } do
        assert cache.fetch_or_store!("lazy", fn -> {:ok, "value"} end) == "value"
        assert cache.get!("lazy") == "value"

        assert cache.fetch_or_store!("lazy", fn -> {:ok, "new value"} end) == "value"
        assert cache.get!("lazy") == "value"
      end

      test "fetch_or_store! raises if an error occurs", %{cache: cache} do
        assert_raise Nebulex.Error, ~r"error", fn ->
          cache.fetch_or_store!("lazy", fn -> {:error, "error"} end)
        end

        refute cache.get!("lazy")
      end

      test "fetch_or_store! stores the value with TTL", %{cache: cache} do
        assert cache.fetch_or_store!("lazy", fn -> {:ok, "value"} end, ttl: :timer.seconds(1)) ==
                 "value"

        assert cache.get!("lazy") == "value"

        _ = t_sleep(:timer.seconds(1) + 100)

        assert cache.fetch_or_store!("lazy", fn -> {:ok, "new value"} end, ttl: :timer.seconds(1)) ==
                 "new value"

        assert cache.get!("lazy") == "new value"
      end

      test "get_or_store stores what the function returns if the key does not exist", %{
        cache: cache
      } do
        ["value", {:ok, "value"}, {:error, "error"}]
        |> Enum.with_index()
        |> Enum.each(fn {ret, i} ->
          assert cache.get_or_store(i, fn -> ret end) == {:ok, ret}
          assert cache.get!(i) == ret
        end)
      end

      test "get_or_store! stores what the function returns if the key does not exist", %{
        cache: cache
      } do
        ["value", {:ok, "value"}, {:error, "error"}]
        |> Enum.with_index()
        |> Enum.each(fn {ret, i} ->
          assert cache.get_or_store!(i, fn -> ret end) == ret
          assert cache.get!(i) == ret
        end)
      end

      test "get_or_store! stores the value with TTL", %{cache: cache} do
        assert cache.get_or_store!("ttl", fn -> "value" end, ttl: :timer.seconds(1)) == "value"
        assert cache.get!("ttl") == "value"

        _ = t_sleep(:timer.seconds(1) + 100)

        assert cache.get_or_store!("ttl", fn -> "new value" end) == "new value"
        assert cache.get!("ttl") == "new value"
      end
    end

    describe "Queryable API" do
      test "raises an exception because invalid query", %{cache: cache} do
        for action <- [:get_all, :stream] do
          assert_raise Nebulex.QueryError, ~r"invalid query, expected one of", fn ->
            get_all_or_stream(cache, action, query: :invalid)
          end
        end
      end

      test "ETS match_spec queries", %{cache: cache, name: name} do
        values = cache_put(cache, 1..5, &(&1 * 2))
        _ = new_generation(cache, name)
        values = values ++ cache_put(cache, 6..10, &(&1 * 2))

        assert cache.stream!([select: :value], max_entries: 3)
               |> Enum.to_list()
               |> :lists.usort() == values

        {_, expected} = Enum.split(values, 5)

        test_ms =
          fun do
            {_, _, value, _, _} when value > 10 -> value
          end

        for action <- [:get_all!, :stream!] do
          assert get_all_or_stream(
                   cache,
                   action,
                   [query: test_ms, select: :value],
                   max_entries: 3
                 ) == expected
        end
      end

      test "getting expired and unexpired entries", %{cache: cache} do
        Enum.reduce([:get_all!, :stream!], 0, fn action, acc ->
          expired = cache_put(cache, 1..5, &(&1 * 2), ttl: 1000)
          unexpired = cache_put(cache, 6..10, &(&1 * 2))
          all = expired ++ unexpired

          expired_q = [query: :expired, select: :value]
          unexpired_q = [query: nil, select: :value]
          opts = [page_size: 3]

          assert get_all_or_stream(cache, action, [select: :value], opts) == all
          assert get_all_or_stream(cache, action, unexpired_q, opts) == all
          assert get_all_or_stream(cache, action, expired_q, opts) == []

          acc = t_sleep(acc + 1100)

          assert get_all_or_stream(cache, action, unexpired_q, opts) == unexpired
          assert get_all_or_stream(cache, action, expired_q, opts) == expired

          acc
        end)
      end

      test "get_all unexpired entries", %{cache: cache} do
        assert cache.put_all([a: 1, b: 2, c: 3], ttl: 5000) == :ok

        assert all = cache.get_all!(query: nil)
        assert Enum.count(all) == 3
      end

      test "delete all expired and unexpired entries", %{cache: cache} do
        _ = cache_put(cache, 1..5, & &1, ttl: 1500)
        _ = cache_put(cache, 6..10)

        assert cache.delete_all!(query: :expired) == 0
        assert cache.count_all!(query: :expired) == 0

        _ = t_sleep(1600)

        assert cache.delete_all!(query: :expired) == 5
        assert cache.count_all!(query: :expired) == 0
        assert cache.count_all!(query: nil) == 5

        assert cache.delete_all!(query: nil) == 5
        assert cache.count_all!(query: nil) == 0
        assert cache.count_all!() == 0
      end

      test "delete all matched entries", %{cache: cache, name: name} do
        values = cache_put(cache, 1..5)

        _ = new_generation(cache, name)

        values = values ++ cache_put(cache, 6..10)

        assert cache.count_all!() == 10

        test_ms =
          fun do
            {_, _, value, _, _} when rem(value, 2) == 0 -> value
          end

        {expected, rem} = Enum.split_with(values, &(rem(&1, 2) == 0))

        assert cache.count_all!(query: test_ms) == 5
        assert cache.get_all!(query: test_ms) |> Enum.sort() == Enum.sort(expected)

        assert cache.delete_all!(query: test_ms) == 5
        assert cache.count_all!(query: test_ms) == 0
        assert cache.get_all!(select: :value) |> Enum.sort() == Enum.sort(rem)
      end

      test "delete all entries with special query {:in, keys} (nested tuples)", %{cache: cache} do
        [
          {1, {:foo, "bar"}},
          {2, {nil, nil}},
          {3, {nil, {nil, nil}}},
          {4, {nil, {nil, nil, {nil, nil}}}},
          {5, {:a, {:b, {:c, {:d, {:e, "f"}}}}}},
          {6, {:a, :b, {:c, :d, {:e, :f, {:g, :h, {:i, :j, "k"}}}}}}
        ]
        |> Enum.each(fn {k, v} ->
          :ok = cache.put(k, v)

          assert cache.count_all!() == 1
          assert cache.delete_all!(in: [k]) == 1
          assert cache.count_all!() == 0
        end)
      end

      test "stream with max_entries", %{cache: cache} do
        entries = for x <- 1..5, do: {x, x}

        :ok = cache.put_all(entries)

        assert {:ok, stream} = cache.stream([in: [1, 2, 3, 4, 5]], max_entries: 2)
        assert Enum.to_list(stream) |> Enum.sort() == Enum.sort(entries)
      end

      test "stream returns empty when is evaluated", %{cache: cache} do
        assert cache.stream!(in: []) |> Enum.to_list() == []
      end
    end

    describe "older generation hitted on" do
      test "put/3 (key is removed from older generation)", %{cache: cache, name: name} do
        :ok = cache.put("foo", "bar")

        _ = new_generation(cache, name)

        refute get_from_new(cache, name, "foo")
        assert get_from_old(cache, name, "foo") == "bar"

        :ok = cache.put("foo", "bar bar", ttl: 500, keep_ttl: true)

        assert get_from_new(cache, name, "foo") == "bar bar"
        refute get_from_old(cache, name, "foo")
      end

      test "put_new/3 (fallback to older generation)", %{cache: cache, name: name} do
        assert cache.put_new!("foo", "bar") == true

        _ = new_generation(cache, name)

        refute get_from_new(cache, name, "foo")
        assert get_from_old(cache, name, "foo") == "bar"

        assert cache.put_new!("foo", "bar") == false

        refute get_from_new(cache, name, "foo")
        assert get_from_old(cache, name, "foo") == "bar"

        _ = new_generation(cache, name)

        assert cache.put_new!("foo", "bar") == true

        assert get_from_new(cache, name, "foo") == "bar"
        refute get_from_old(cache, name, "foo")
      end

      test "replace/3 (fallback to older generation)", %{cache: cache, name: name} do
        assert cache.replace!("foo", "bar bar") == false

        :ok = cache.put("foo", "bar")

        assert cache.replace!("foo", "bar bar") == true

        _ = new_generation(cache, name)

        refute get_from_new(cache, name, "foo")
        assert get_from_old(cache, name, "foo") == "bar bar"

        assert cache.replace!("foo", "bar bar bar") == true
        assert cache.replace!("foo", "bar bar bar bar") == true

        assert get_from_new(cache, name, "foo") == "bar bar bar bar"
        refute get_from_old(cache, name, "foo")

        _ = new_generation(cache, name)
        _ = new_generation(cache, name)

        assert cache.replace!("foo", "bar") == false
      end

      test "put_all/2 (keys are removed from older generation)", %{cache: cache, name: name} do
        entries = Enum.map(1..100, &{{:key, &1}, &1})

        :ok = cache.put_all(entries)

        _ = new_generation(cache, name)

        Enum.each(entries, fn {k, v} ->
          refute get_from_new(cache, name, k)
          assert get_from_old(cache, name, k) == v
        end)

        :ok = cache.put_all(entries)

        Enum.each(entries, fn {k, v} ->
          assert get_from_new(cache, name, k) == v
          refute get_from_old(cache, name, k)
        end)
      end

      test "put_new_all/2 (fallback to older generation)", %{cache: cache, name: name} do
        entries = Enum.map(1..100, &{&1, &1})

        assert cache.put_new_all!(entries) == true

        _ = new_generation(cache, name)

        Enum.each(entries, fn {k, v} ->
          refute get_from_new(cache, name, k)
          assert get_from_old(cache, name, k) == v
        end)

        assert cache.put_new_all!(entries) == false

        Enum.each(entries, fn {k, v} ->
          refute get_from_new(cache, name, k)
          assert get_from_old(cache, name, k) == v
        end)

        _ = new_generation(cache, name)

        assert cache.put_new_all!(entries) == true

        Enum.each(entries, fn {k, v} ->
          assert get_from_new(cache, name, k) == v
          refute get_from_old(cache, name, k)
        end)
      end

      test "expire/3 (fallback to older generation)", %{cache: cache, name: name} do
        assert cache.put("foo", "bar") == :ok

        _ = new_generation(cache, name)

        refute get_from_new(cache, name, "foo")
        assert get_from_old(cache, name, "foo") == "bar"

        assert cache.expire!("foo", 200) == true

        assert get_from_new(cache, name, "foo") == "bar"
        refute get_from_old(cache, name, "foo")

        _ = t_sleep(210)

        refute cache.get!("foo")
      end

      test "incr/3 (fallback to older generation)", %{cache: cache, name: name} do
        assert cache.put(:counter, 0, ttl: 200) == :ok

        _ = new_generation(cache, name)

        refute get_from_new(cache, name, :counter)
        assert get_from_old(cache, name, :counter) == 0

        assert cache.incr!(:counter) == 1
        assert cache.incr!(:counter) == 2

        assert get_from_new(cache, name, :counter) == 2
        refute get_from_old(cache, name, :counter)

        _ = t_sleep(210)

        assert cache.incr!(:counter) == 1
      end

      test "get_all!/2 (no duplicates)", %{cache: cache, name: name} do
        entries = for x <- 1..20, into: %{}, do: {x, x}
        keys = Map.keys(entries) |> Enum.sort()

        :ok = cache.put_all(entries)

        assert cache.count_all!() == 20
        assert cache.get_all!(select: :key) |> Enum.sort() == keys

        _ = new_generation(cache, name)

        :ok = cache.put_all(entries)

        assert cache.count_all!() == 20
        assert cache.get_all!(select: :key) |> Enum.sort() == keys

        _ = new_generation(cache, name)

        more_entries = for x <- 10..30, into: %{}, do: {x, x}
        more_keys = Map.keys(more_entries) |> Enum.sort()

        :ok = cache.put_all(more_entries)

        assert cache.count_all!() == 30
        assert cache.get_all!(select: :key) |> Enum.sort() == (keys ++ more_keys) |> Enum.uniq()

        _ = new_generation(cache, name)

        assert cache.count_all!() == 21
        assert cache.get_all!(select: :key) |> Enum.sort() == more_keys
      end
    end

    describe "generation" do
      test "created with unexpired entries", %{cache: cache, name: name} do
        assert cache.put("foo", "bar") == :ok
        assert cache.fetch!("foo") == "bar"
        assert cache.ttl("foo") == {:ok, :infinity}

        _ = new_generation(cache, name)

        assert cache.fetch!("foo") == "bar"
      end

      test "lifecycle", %{cache: cache, name: name} do
        # should be empty
        assert {:error, %Nebulex.KeyError{key: 1}} = cache.fetch(1)

        # set some entries
        for x <- 1..2, do: cache.put(x, x)

        # fetch one entry from new generation
        assert cache.fetch!(1) == 1

        # fetch non-existent entries
        assert {:error, %Nebulex.KeyError{key: 3}} = cache.fetch(3)
        assert {:error, %Nebulex.KeyError{key: :non_existent}} = cache.fetch(:non_existent)

        # create a new generation
        _ = new_generation(cache, name)

        # both entries should be in the old generation
        refute get_from_new(cache, name, 1)
        refute get_from_new(cache, name, 2)
        assert get_from_old(cache, name, 1) == 1
        assert get_from_old(cache, name, 2) == 2

        # fetch entry 1 and put it into the new generation
        assert cache.fetch!(1) == 1
        assert get_from_new(cache, name, 1) == 1
        refute get_from_new(cache, name, 2)
        refute get_from_old(cache, name, 1)
        assert get_from_old(cache, name, 2) == 2

        # create a new generation, the old generation should be deleted
        _ = new_generation(cache, name)

        # entry 1 should be into the old generation and entry 2 deleted
        refute get_from_new(cache, name, 1)
        refute get_from_new(cache, name, 2)
        assert get_from_old(cache, name, 1) == 1
        refute get_from_old(cache, name, 2)
      end

      test "creation with ttl", %{cache: cache, name: name} do
        assert cache.put(1, 1, ttl: 1000) == :ok
        assert cache.fetch!(1) == 1

        _ = new_generation(cache, name)

        refute get_from_new(cache, name, 1)
        assert get_from_old(cache, name, 1) == 1
        assert cache.fetch!(1) == 1

        _ = t_sleep(1100)

        assert {:error, %Nebulex.KeyError{key: 1}} = cache.fetch(1)
        refute get_from_new(cache, name, 1)
        refute get_from_old(cache, name, 1)
      end
    end

    ## Helpers

    defp new_generation(cache, name) do
      cache.with_dynamic_cache(name, fn ->
        cache.new_generation()
      end)
    end

    defp get_from_new(cache, name, key) do
      cache.with_dynamic_cache(name, fn ->
        get_from(cache.newer_generation(), name, key)
      end)
    end

    defp get_from_old(cache, name, key) do
      cache.with_dynamic_cache(name, fn ->
        cache.generations()
        |> List.last()
        |> get_from(name, key)
      end)
    end

    defp get_from(gen, name, key) do
      case Adapter.lookup_meta(name).backend.lookup(gen, key) do
        [] -> nil
        [{_, ^key, val, _, _}] -> val
      end
    end

    defp get_all_or_stream(cache, action, query, opts \\ [])

    defp get_all_or_stream(cache, op, query, opts) when op in [:get_all, :get_all!] do
      query
      |> cache.get_all!(opts)
      |> handle_query_result()
    end

    defp get_all_or_stream(cache, op, query, opts) when op in [:stream, :stream!] do
      query
      |> cache.stream!(opts)
      |> handle_query_result()
    end

    defp handle_query_result(list) when is_list(list) do
      :lists.usort(list)
    end

    defp handle_query_result(stream) do
      stream
      |> Enum.to_list()
      |> :lists.usort()
    end
  end
end
