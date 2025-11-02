defmodule Nebulex.Adapters.Local.QueryHelperTest do
  use ExUnit.Case, async: true

  use Nebulex.Adapters.Local.QueryHelper

  describe "match_spec/1" do
    test "transforms simple key and value pattern" do
      ms = match_spec key: k, value: v, where: k == :foo, select: v

      # Equivalent to: fun do {:entry, k, v, _, _, _} when k == :foo -> v end
      expected =
        fun do
          {:entry, k, v, _, _, _} when k == :foo -> v
        end

      assert ms == expected
    end

    test "transforms pattern with tag" do
      ms = match_spec key: k, value: v, tag: t, where: t == :important, select: {k, v}

      expected =
        fun do
          {:entry, k, v, _, _, t} when t == :important -> {k, v}
        end

      assert ms == expected
    end

    test "transforms pattern with multiple fields" do
      ms = match_spec key: k, value: v, exp: e, tag: t, where: e != :infinity, select: {k, v, t}

      expected =
        fun do
          {:entry, k, v, _, e, t} when e != :infinity -> {k, v, t}
        end

      assert ms == expected
    end

    test "transforms pattern without guards" do
      ms = match_spec key: k, value: v, select: {k, v}

      expected =
        fun do
          {:entry, k, v, _, _, _} -> {k, v}
        end

      assert ms == expected
    end

    test "transforms pattern with complex guards" do
      ms = match_spec key: k, value: v, where: is_integer(k) and k > 10, select: {k, v}

      expected =
        fun do
          {:entry, k, v, _, _, _} when is_integer(k) and k > 10 -> {k, v}
        end

      assert ms == expected
    end

    test "transforms pattern with or guard" do
      ms = match_spec key: k, tag: t, where: t == :foo or t == :bar, select: k

      expected =
        fun do
          {:entry, k, _, _, _, t} when t == :foo or t == :bar -> k
        end

      assert ms == expected
    end

    test "transforms pattern returning entire entry with :\"\$_\"" do
      ms = match_spec key: k, value: v, where: k > 100, select: :"$_"

      expected =
        fun do
          {:entry, k, v, _, _, _} = e when k > 100 -> e
        end

      assert ms == expected
    end

    test "transforms pattern with only value field" do
      ms = match_spec value: v, where: v > 10, select: v

      expected =
        fun do
          {:entry, _, v, _, _, _} when v > 10 -> v
        end

      assert ms == expected
    end

    test "transforms pattern with only tag field" do
      ms = match_spec tag: t, where: t == :group_a, select: true

      expected =
        fun do
          {:entry, _, _, _, _, t} when t == :group_a -> true
        end

      assert ms == expected
    end

    test "transforms pattern with touched and exp fields" do
      ms = match_spec key: k, touched: ts, exp: e, where: e > ts, select: k

      expected =
        fun do
          {:entry, k, _, ts, e, _} when e > ts -> k
        end

      assert ms == expected
    end

    test "fields can be specified in any order" do
      ms1 = match_spec key: k, value: v, tag: t, select: {k, v, t}
      ms2 = match_spec tag: t, key: k, value: v, select: {k, v, t}
      ms3 = match_spec value: v, tag: t, key: k, select: {k, v, t}

      # All should produce the same match spec
      assert ms1 == ms2
      assert ms2 == ms3
    end

    test "works with complex return expressions" do
      ms = match_spec key: k, value: v, tag: t, where: is_binary(v), select: {t, k}

      expected =
        fun do
          {:entry, k, v, _, _, t} when is_binary(v) -> {t, k}
        end

      assert ms == expected
    end

    test "uses default select: true when :select is not provided" do
      ms = match_spec key: k, value: v, where: k == :foo

      # Equivalent to: fun do {:entry, k, v, _, _, _} when k == :foo -> true end
      expected =
        fun do
          {:entry, k, v, _, _, _} when k == :foo -> true
        end

      assert ms == expected
    end

    test "default select: true works without where clause" do
      ms = match_spec tag: t

      expected =
        fun do
          {:entry, _, _, _, _, t} -> true
        end

      assert ms == expected
    end

    test "raises with invalid field name" do
      assert_raise ArgumentError, ~r/Invalid field\(s\): \[:invalid\]/, fn ->
        Code.eval_quoted(
          quote do
            require Nebulex.Adapters.Local.QueryHelper
            import Nebulex.Adapters.Local.QueryHelper

            match_spec invalid: x, select: x
          end
        )
      end
    end
  end

  describe "match_spec/1 integration with ETS" do
    setup do
      # Create a test ETS table with unique name
      table_name = :"test_table_#{:erlang.unique_integer([:positive])}"

      # Use keypos: 2 since the entry record is {:entry, key, value, ...}
      # and we want the 2nd element (key) to be the actual ETS key
      table = :ets.new(table_name, [:set, :public, :named_table, keypos: 2])

      # Insert some test entries using the same record format as Local adapter
      true = :ets.insert(table, {:entry, 1, "value1", 1000, 2000, :tag_a})
      true = :ets.insert(table, {:entry, 2, "value2", 1100, 2100, :tag_b})
      true = :ets.insert(table, {:entry, 3, "value3", 1200, :infinity, :tag_a})
      true = :ets.insert(table, {:entry, 4, 100, 1300, 2300, :tag_c})
      true = :ets.insert(table, {:entry, 5, 200, 1400, 2400, :tag_c})
      true = :ets.insert(table, {:entry, {:key, 6}, 300, 1500, 2500, :tag_d})

      # Verify all entries were inserted
      assert :ets.info(table, :size) == 6

      on_exit(fn ->
        if :ets.whereis(table_name) != :undefined do
          :ets.delete(table)
        end
      end)

      %{table: table}
    end

    test "selects entries by key", %{table: table} do
      ms = match_spec key: k, value: v, where: k == 1, select: v

      assert :ets.select(table, ms) == ["value1"]
    end

    test "selects entries by tag", %{table: table} do
      ms = match_spec key: k, tag: t, where: t == :tag_a, select: k
      result = :ets.select(table, ms)

      assert Enum.sort(result) == [1, 3]
    end

    test "selects entries with value guard", %{table: table} do
      ms = match_spec key: k, value: v, where: is_integer(v) and v > 100, select: {k, v}

      assert :ets.select(table, ms) |> Enum.sort() == [{5, 200}, {{:key, 6}, 300}] |> Enum.sort()
    end

    test "selects entries with exp guard", %{table: table} do
      ms = match_spec key: k, exp: e, where: e == :infinity, select: k

      assert :ets.select(table, ms) == [3]
    end

    test "selects entries with complex guards", %{table: table} do
      ms =
        match_spec value: v,
                   tag: t,
                   where: is_integer(v) and (t == :tag_c or t == :tag_a),
                   select: v

      result = :ets.select(table, ms)

      assert Enum.sort(result) == [100, 200]
    end

    test "selects entire entries", %{table: table} do
      ms = match_spec key: k, tag: t, where: k > 3, select: :"$_"
      result = :ets.select(table, ms)

      assert Enum.sort(result) ==
               Enum.sort([
                 {:entry, 4, 100, 1300, 2300, :tag_c},
                 {:entry, 5, 200, 1400, 2400, :tag_c},
                 {:entry, {:key, 6}, 300, 1500, 2500, :tag_d}
               ])
    end

    test "counts entries with match spec", %{table: table} do
      ms = match_spec tag: t, where: t == :tag_c, select: true
      result = :ets.select_count(table, ms)

      assert result == 2
    end

    test "deletes entries with match spec", %{table: table} do
      ms = match_spec key: k, tag: t, where: t == :tag_b, select: true
      deleted_count = :ets.select_delete(table, ms)

      assert deleted_count == 1

      # Verify the entry was deleted
      assert :ets.lookup(table, 2) == []
      # Other entries should still exist
      assert :ets.info(table, :size) == 5
    end

    test "matches without guards", %{table: table} do
      ms = match_spec key: k, value: v, select: k
      result = :ets.select(table, ms)

      assert Enum.sort(result) |> Enum.sort() == [1, 2, 3, 4, 5, {:key, 6}] |> Enum.sort()
    end

    test "matches with only specific field binding", %{table: table} do
      ms = match_spec value: v, where: is_binary(v), select: v
      result = :ets.select(table, ms)

      assert Enum.sort(result) == ["value1", "value2", "value3"]
    end

    test "complex query with multiple conditions", %{table: table} do
      ms =
        match_spec key: k,
                   value: v,
                   tag: t,
                   where: is_integer(v) and v >= 100 and t == :tag_c,
                   select: {k, v}

      result = :ets.select(table, ms)

      assert Enum.sort(result) == [{4, 100}, {5, 200}]
    end

    test "selects entries with key as tuple", %{table: table} do
      ms = match_spec key: k, value: v, where: k == {:key, 6}, select: {k, v}

      assert :ets.select(table, ms) == [{{:key, 6}, 300}]
    end
  end

  describe "keyref_match_spec/2" do
    test "returns a valid match spec" do
      ms = keyref_match_spec(:user_123)

      # Should return a valid ETS match spec (list of tuples)
      assert is_list(ms)
      assert length(ms) == 1
      assert {_pattern, _guards, _select} = hd(ms)
    end

    test "returns a valid match spec with cache filter" do
      ms = keyref_match_spec(:user_123, cache: MyApp.Cache)

      assert is_list(ms)
      assert length(ms) == 1
    end
  end

  describe "keyref_match_spec/2 integration with ETS" do
    setup do
      table_name = :"test_keyref_table_#{:erlang.unique_integer([:positive])}"
      table = :ets.new(table_name, [:set, :public, :named_table, keypos: 2])

      # Insert some regular entries
      true = :ets.insert(table, {:entry, :key1, "value1", 1000, 2000, nil})
      true = :ets.insert(table, {:entry, :key2, "value2", 1100, 2100, nil})

      # Insert keyref entries (cache references)
      # Reference to :user_123 in nil cache (local reference)
      true =
        :ets.insert(
          table,
          {:entry, :ref1, {:"$nbx_keyref_spec", nil, :user_123, nil}, 1200, :infinity, nil}
        )

      # Reference to :user_123 in MyApp.Cache
      true =
        :ets.insert(
          table,
          {:entry, :ref2, {:"$nbx_keyref_spec", MyApp.Cache, :user_123, nil}, 1300, :infinity, nil}
        )

      # Reference to :user_456 in MyApp.Cache
      true =
        :ets.insert(
          table,
          {:entry, :ref3, {:"$nbx_keyref_spec", MyApp.Cache, :user_456, nil}, 1400, :infinity, nil}
        )

      # Reference to :user_123 in AnotherCache
      true =
        :ets.insert(
          table,
          {:entry, :ref4, {:"$nbx_keyref_spec", AnotherCache, :user_123, nil}, 1500, :infinity, nil}
        )

      on_exit(fn ->
        if :ets.whereis(table_name) != :undefined do
          :ets.delete(table)
        end
      end)

      %{table: table}
    end

    test "gets all reference keys pointing to a specific key (any cache)", %{table: table} do
      ms = keyref_match_spec(:user_123)
      result = :ets.select(table, ms) |> Enum.sort()

      # Should return the keys of all reference entries
      assert result == [:ref1, :ref2, :ref4]
    end

    test "gets reference keys for a specific cache", %{table: table} do
      ms = keyref_match_spec(:user_123, cache: MyApp.Cache)
      result = :ets.select(table, ms)

      # Should return only :ref2 (reference to :user_123 in MyApp.Cache)
      assert result == [:ref2]
    end

    test "returns empty when no references exist", %{table: table} do
      ms = keyref_match_spec(:nonexistent_key)
      result = :ets.select(table, ms)

      assert result == []
    end
  end
end
