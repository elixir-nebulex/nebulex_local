defmodule Nebulex.Adapters.LocalOrderedSetTest do
  use ExUnit.Case, async: true

  defmodule Cache do
    use Nebulex.Cache,
      otp_app: :nebulex_local,
      adapter: Nebulex.Adapters.Local
  end

  setup do
    {:ok, pid} = Cache.start_link(backend_type: :ordered_set)

    on_exit(fn ->
      :ok = Process.sleep(100)

      if Process.alive?(pid), do: Cache.stop()
    end)

    :ok
  end

  describe "{:in, keys} queries on ordered_set" do
    test "match keys with the table's native == semantics" do
      :ok = Cache.put(1, "one")

      assert Cache.get!(1.0) == "one"
      assert Cache.get_all!(in: [1.0]) == [{1, "one"}]
      assert Cache.count_all!(in: [1.0]) == 1
      assert Cache.stream!(in: [1.0]) |> Enum.to_list() == [{1, "one"}]
      assert Cache.delete_all!(in: [1.0]) == 1
      assert Cache.get!(1) == nil
    end

    test "process ==-equal duplicate keys once" do
      :ok = Cache.put(1, "one")

      assert Cache.get_all!(in: [1, 1.0]) == [{1, "one"}]
      assert Cache.stream!(in: [1, 1.0]) |> Enum.to_list() == [{1, "one"}]
      assert Cache.count_all!(in: [1, 1.0]) == 1
      assert Cache.delete_all!(in: [1, 1.0]) == 1
      assert Cache.get!(1) == nil
    end

    test "count_all skips expired entries and delete_all lazily removes them" do
      :ok = Cache.put(1, "one", ttl: 10)

      :ok = Process.sleep(50)

      # `count_all` is read-only: the expired entry stays in the table
      assert Cache.count_all!(in: [1]) == 0
      assert Cache.count_all!(query: :expired) == 1

      # `delete_all` does not count the expired entry, but removes it
      assert Cache.delete_all!(in: [1]) == 0
      assert Cache.count_all!(query: :expired) == 0
    end

    test "match non-indexable keys through lookups" do
      :ok = Cache.put_all(%{%{a: 1} => 1, :"$1" => 2, :_ => 3})

      assert Cache.get_all!(in: [%{a: 1}]) == [{%{a: 1}, 1}]
      assert Cache.count_all!(in: [%{a: 1}, :"$1", :_]) == 3
      assert Cache.delete_all!(in: [%{a: 1}, :"$1"]) == 2
      assert Cache.count_all!() == 1
    end

    test "match keys in the older generation" do
      :ok = Cache.put(1, "one")

      _ = Cache.new_generation()

      assert Cache.count_all!(in: [1.0]) == 1
      assert Cache.delete_all!(in: [1.0]) == 1
      assert Cache.get!(1) == nil
    end
  end
end
