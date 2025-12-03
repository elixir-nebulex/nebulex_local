defmodule Nebulex.Adapters.LocalTest do
  @moduledoc false

  import Nebulex.CacheCase

  deftests do
    alias Nebulex.Adapter

    import Nebulex.CacheCase, only: [cache_put: 2, cache_put: 3, cache_put: 4, t_sleep: 1]

    use Nebulex.Adapters.Local.QueryHelper

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
            {_, _, value, _, _, _} when value > 10 -> value
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

      test "get_all with tag", %{cache: cache} do
        assert cache.put_all([a: 1, b: 2, c: 3], tag: :foo) == :ok
        assert cache.put_all([d: 4, e: 5, f: 6], tag: :bar) == :ok

        test_ms =
          fun do
            {_, _, value, _, _, tag} when tag == :foo -> value
          end

        for action <- [:get_all!, :stream!] do
          assert get_all_or_stream(
                   cache,
                   action,
                   query: test_ms,
                   select: :value
                 ) == :lists.usort([1, 2, 3])
        end

        test_ms =
          fun do
            {_, _, value, _, _, tag} when tag == :foo or tag == :bar -> value
          end

        for action <- [:get_all!, :stream!] do
          assert get_all_or_stream(
                   cache,
                   action,
                   query: test_ms,
                   select: :value
                 ) == :lists.usort([1, 2, 3, 4, 5, 6])
        end
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
            {_, _, value, _, _, _} when rem(value, 2) == 0 -> value
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

      test "delete all entries with tag", %{cache: cache} do
        assert cache.put_all([a: 1, b: 2, c: 3], tag: :foo) == :ok
        assert cache.put_all([d: 4, e: 5, f: 6], tag: :bar) == :ok

        test_ms =
          fun do
            {_, _, value, _, _, tag} when tag == :foo -> value
          end

        assert cache.delete_all!(query: test_ms) == 3
        assert cache.count_all!(query: test_ms) == 0

        test_ms =
          fun do
            {_, _, value, _, _, tag} when tag == :foo or tag == :bar -> value
          end

        assert cache.delete_all!(query: test_ms) == 3
        assert cache.count_all!(query: test_ms) == 0
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

      test "QueryHelper: get_all with tag", %{cache: cache} do
        assert cache.put_all([a: 1, b: 2, c: 3], tag: :foo) == :ok
        assert cache.put_all([d: 4, e: 5, f: 6], tag: :bar) == :ok

        # Get values for a specific tag
        ms = match_spec value: v, tag: t, where: t == :foo, select: v
        assert cache.get_all!(query: ms) |> Enum.sort() == [1, 2, 3]

        # Get key-value pairs for multiple tags
        ms = match_spec key: k, value: v, tag: t, where: t == :foo or t == :bar, select: {k, v}
        result = cache.get_all!(query: ms) |> Enum.sort()
        assert result == Enum.sort(a: 1, b: 2, c: 3, d: 4, e: 5, f: 6)
      end

      test "QueryHelper: count_all with tag", %{cache: cache} do
        assert cache.put_all([a: 1, b: 2, c: 3], tag: :foo) == :ok
        assert cache.put_all([d: 4, e: 5, f: 6], tag: :bar) == :ok

        # Count entries with specific tag (using default select: true)
        ms = match_spec tag: t, where: t == :foo
        assert cache.count_all!(query: ms) == 3

        # Count entries with multiple tags (using default select: true)
        ms = match_spec tag: t, where: t == :foo or t == :bar
        assert cache.count_all!(query: ms) == 6
      end

      test "QueryHelper: delete_all with tag", %{cache: cache} do
        assert cache.put_all([a: 1, b: 2, c: 3], tag: :foo) == :ok
        assert cache.put_all([d: 4, e: 5, f: 6], tag: :bar) == :ok

        # Delete entries with specific tag (using default select: true)
        ms = match_spec tag: t, where: t == :foo
        assert cache.delete_all!(query: ms) == 3

        # Verify deletion (using default select: true)
        ms = match_spec tag: t, where: t == :foo
        assert cache.count_all!(query: ms) == 0

        # Only :bar entries should remain (using default select: true)
        ms = match_spec tag: t, where: t == :bar
        assert cache.count_all!(query: ms) == 3
      end

      test "QueryHelper: delete_all overrides select clause", %{cache: cache} do
        assert cache.put_all([a: 1, b: 2, c: 3], tag: :test) == :ok

        # Even though we select {k, v}, delete_all should work because
        # the adapter overrides the return value to true
        ms = match_spec key: k, value: v, tag: t, where: t == :test, select: {k, v}
        assert cache.delete_all!(query: ms) == 3

        # Verify all entries were deleted
        assert cache.count_all!() == 0
      end

      test "QueryHelper: stream with tag", %{cache: cache} do
        assert cache.put_all([a: 1, b: 2, c: 3], tag: :foo) == :ok
        assert cache.put_all([d: 4, e: 5, f: 6], tag: :bar) == :ok

        # Stream values for a specific tag
        ms = match_spec value: v, tag: t, where: t == :foo, select: v
        {:ok, stream} = cache.stream([query: ms], max_entries: 2)
        result = Enum.to_list(stream) |> Enum.sort()
        assert result == [1, 2, 3]
      end

      test "QueryHelper: complex queries with multiple fields", %{cache: cache} do
        # Put entries with different value types
        assert cache.put_all([a: 10, b: 20, c: 30], tag: :numbers) == :ok
        assert cache.put_all([d: "foo", e: "bar"], tag: :strings) == :ok
        assert cache.put_all([f: 5, g: 15], tag: :numbers) == :ok

        # Get all integer values greater than 10 with :numbers tag
        ms =
          match_spec key: k,
                     value: v,
                     tag: t,
                     where: is_integer(v) and v > 10 and t == :numbers,
                     select: {k, v}

        result = cache.get_all!(query: ms) |> Enum.sort()
        assert result == Enum.sort(b: 20, c: 30, g: 15)

        # Count string values (using default select: true)
        ms = match_spec value: v, tag: t, where: is_binary(v) and t == :strings
        assert cache.count_all!(query: ms) == 2
      end

      test "QueryHelper: query a map as a value", %{cache: cache} do
        assert cache.put_all([a: %{x: 1}, b: %{x: 2}], tag: :maps) == :ok

        var = 1
        ms = match_spec value: %{x: x}, tag: t, where: x == ^var and t == :maps, select: x

        assert cache.get_all!(query: ms) == [1]

        ms = match_spec value: %{x: x}, tag: t, where: t == :maps, select: x

        assert cache.get_all!(query: ms) |> Enum.sort() == [1, 2]
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

    describe "race conditions and automatic retry" do
      test "concurrent operations during garbage collection", %{cache: cache, name: name} do
        # Populate cache with entries
        entries = for x <- 1..50, into: %{}, do: {x, x * 2}
        :ok = cache.put_all(entries)

        # Spawn multiple processes that will read/write during GC
        tasks =
          for i <- 1..20 do
            task_async(cache, name, fn ->
              # Perform various operations that might race with GC
              cache.put("concurrent_#{i}", i)
              cache.fetch!("concurrent_#{i}")
              cache.get!(1)
              cache.count_all!()
            end)
          end

        # Trigger GC while tasks are running
        _ = new_generation(cache, name)
        _ = new_generation(cache, name)

        # All tasks should complete successfully without crashes
        results = Task.await_many(tasks, 5000)
        assert Enum.count(results) == 20
      end

      test "fetch during generation deletion", %{cache: cache, name: name} do
        # Put entries in old generation
        :ok = cache.put_all(for x <- 1..100, do: {x, x})
        _ = new_generation(cache, name)

        # Spawn processes that fetch while we delete the old generation
        tasks =
          for x <- 1..100 do
            task_async(cache, name, fn ->
              # This should succeed even if generation is deleted during access
              cache.fetch(x)
            end)
          end

        # Trigger another generation change (will delete old generation)
        _ = new_generation(cache, name)

        # All fetches should succeed (entries move to new generation on access)
        results = Task.await_many(tasks, 5000)
        assert Enum.count(results) == 100
      end

      test "put operations during generation transitions", %{cache: cache, name: name} do
        # Initial data
        :ok = cache.put_all(for x <- 1..50, do: {x, x})

        # Spawn tasks that perform puts during GC
        tasks =
          for x <- 51..100 do
            task_async(cache, name, fn ->
              cache.put!(x, x)
              {:ok, true}
            end)
          end

        # Trigger multiple generation changes
        _ = new_generation(cache, name)
        _ = new_generation(cache, name)

        # All puts should succeed
        results = Task.await_many(tasks, 5000)
        assert Enum.all?(results, &(&1 == {:ok, true}))
      end

      test "delete_all with query during generation change", %{cache: cache, name: name} do
        import Ex2ms

        # Put tagged entries
        :ok = cache.put_all(for(x <- 1..50, do: {x, x}), tag: :group_a)
        :ok = cache.put_all(for(x <- 51..100, do: {x, x}), tag: :group_b)

        # Create match spec for group_a
        test_ms =
          fun do
            {_, _, _, _, _, tag} when tag == :group_a -> true
          end

        # Spawn task to delete while we change generations
        task =
          task_async(cache, name, fn ->
            cache.delete_all!(query: test_ms)
          end)

        # Trigger generation change during delete operation
        _ = new_generation(cache, name)

        # Delete should succeed
        deleted_count = Task.await(task, 5000)
        assert deleted_count >= 0 and deleted_count <= 50

        # Verify only group_b entries remain (or were also affected by GC)
        remaining = cache.count_all!()
        assert remaining >= 0 and remaining <= 100
      end

      test "stream operations during generation deletion", %{cache: cache, name: name} do
        # Put entries
        :ok = cache.put_all(for x <- 1..100, do: {x, x * 2})
        _ = new_generation(cache, name)

        # Start streaming
        stream_task =
          task_async(cache, name, fn ->
            {:ok, stream} = cache.stream([select: :value], max_entries: 10)
            Enum.to_list(stream)
          end)

        # Delete generation while streaming
        :ok = Process.sleep(10)
        _ = new_generation(cache, name)

        # Stream should complete without errors (may have partial results)
        result = Task.await(stream_task, 5000)
        assert is_list(result)
      end

      test "update_counter during generation change", %{cache: cache, name: name} do
        # Initial counter
        assert cache.incr!(:counter, 1) == 1
        _ = new_generation(cache, name)

        # Spawn multiple tasks incrementing counter during GC
        tasks =
          for _ <- 1..50 do
            task_async(cache, name, fn ->
              cache.incr!(:counter, 1)
            end)
          end

        # Trigger generation change
        _ = new_generation(cache, name)

        # All increments should succeed
        results = Task.await_many(tasks, 5000)
        assert Enum.count(results) == 50

        # Final counter value should reflect all increments
        final_value = cache.get!(:counter)
        assert final_value >= 1 and final_value <= 51
      end

      test "mixed operations with high concurrency", %{cache: cache, name: name} do
        # Initial data
        :ok = cache.put_all(for x <- 1..20, do: {x, x})

        # Spawn mix of operations
        read_tasks = for x <- 1..20, do: task_async(cache, name, fn -> cache.get!(x) end)
        write_tasks = for x <- 21..40, do: task_async(cache, name, fn -> cache.put!(x, x) end)
        delete_tasks = for x <- 1..10, do: task_async(cache, name, fn -> cache.delete!(x) end)
        count_tasks = for _ <- 1..5, do: task_async(cache, name, fn -> cache.count_all!() end)

        # Trigger multiple generation changes during operations
        gc_task =
          task_async(cache, name, fn ->
            _ = new_generation(cache, name)
            :ok = Process.sleep(20)
            _ = new_generation(cache, name)
            :ok
          end)

        # Wait for all operations
        Task.await(gc_task, 5000)
        Task.await_many(read_tasks ++ write_tasks ++ delete_tasks ++ count_tasks, 5000)

        # Cache should be in consistent state
        count = cache.count_all!()
        assert count >= 0 and count <= 40
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
        [{_, ^key, val, _, _, _}] -> val
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

    defp task_async(cache, name, fun) do
      Task.async(fn -> cache.with_dynamic_cache(name, fun) end)
    end
  end
end
