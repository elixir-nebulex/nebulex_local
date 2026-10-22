defmodule Nebulex.Adapters.Local.Metadata do
  @moduledoc false

  @type tab() :: :ets.tid() | atom()

  @spec init() :: tab()
  def init do
    tab = :ets.new(__MODULE__, [:public, read_concurrency: true])

    :ok =
      put(tab, :entry_locks, :ets.new(__MODULE__, [:ordered_set, :public, write_concurrency: true]))

    tab
  end

  @spec get(tab(), term(), term()) :: term()
  def get(tab, key, default \\ nil) do
    :ets.lookup_element(tab, key, 2)
  rescue
    ArgumentError -> default
  end

  @spec fetch!(tab(), term()) :: term()
  def fetch!(tab, key) do
    :ets.lookup_element(tab, key, 2)
  end

  @spec put(tab(), term(), term()) :: :ok
  def put(tab, key, value) do
    true = :ets.insert(tab, {key, value})

    :ok
  end

  @doc false
  @spec with_lock(tab(), term(), (-> result)) :: result when result: var
  def with_lock(tab, key, fun) do
    tab
    |> fetch!(:entry_locks)
    |> do_with_lock(key, fun)
  end

  defp do_with_lock(tab, key, fun) do
    lock = {key, self()}

    if :ets.insert_new(tab, lock) do
      try do
        fun.()
      after
        :ets.delete_object(tab, lock)
      end
    else
      wait_for_lock(:ets.lookup(tab, key), tab, key, fun)
    end
  end

  defp wait_for_lock([{_, owner}], _tab, _key, fun) when owner == self() do
    fun.()
  end

  defp wait_for_lock(locks, tab, key, fun) do
    Enum.each(locks, fn {_, owner} = lock ->
      if not Process.alive?(owner), do: :ets.delete_object(tab, lock)
    end)

    :erlang.yield()

    do_with_lock(tab, key, fun)
  end
end
