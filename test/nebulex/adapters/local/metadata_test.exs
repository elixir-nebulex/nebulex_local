defmodule Nebulex.Adapters.Local.MetadataTest do
  use ExUnit.Case, async: true

  alias Nebulex.Adapters.Local.Metadata

  setup do
    %{table: Metadata.init(), supervisor: start_supervised!(Task.Supervisor)}
  end

  test "locks are reentrant and released after returning", %{table: table, supervisor: supervisor} do
    assert Metadata.with_lock(table, :key, fn ->
             Metadata.with_lock(table, :key, fn -> :nested end)
           end) == :nested

    task =
      Task.Supervisor.async_nolink(supervisor, fn ->
        Metadata.with_lock(table, :key, fn -> :ok end)
      end)

    assert Task.await(task) == :ok
  end

  test "locks are released after an exception", %{table: table, supervisor: supervisor} do
    assert_raise RuntimeError, "failed", fn ->
      Metadata.with_lock(table, :key, fn -> raise "failed" end)
    end

    task =
      Task.Supervisor.async_nolink(supervisor, fn ->
        Metadata.with_lock(table, :key, fn -> :ok end)
      end)

    assert Task.await(task) == :ok
  end

  test "an abandoned lock can be acquired after its owner dies", %{
    table: table,
    supervisor: supervisor
  } do
    parent = self()

    task =
      Task.Supervisor.async_nolink(supervisor, fn ->
        Metadata.with_lock(table, :key, fn ->
          send(parent, :locked)

          receive do
            :continue -> :ok
          end
        end)
      end)

    assert_receive :locked
    Process.exit(task.pid, :kill)
    assert_receive {:DOWN, ref, :process, _, :killed} when ref == task.ref

    assert Metadata.with_lock(table, :key, fn -> :recovered end) == :recovered
  end

  test "different keys do not block each other", %{table: table, supervisor: supervisor} do
    Metadata.with_lock(table, :first, fn ->
      task =
        Task.Supervisor.async_nolink(supervisor, fn ->
          Metadata.with_lock(table, :second, fn -> :independent end)
        end)

      assert Task.await(task) == :independent
    end)
  end

  test "concurrent updates serialize, including numerically equal keys", %{
    table: table,
    supervisor: supervisor
  } do
    :ok = Metadata.put(table, :counter, 0)

    tasks =
      for key <- [1, 1.0, 1, 1.0, 1, 1.0, 1, 1.0] do
        Task.Supervisor.async_nolink(supervisor, fn ->
          for _ <- 1..100 do
            Metadata.with_lock(table, key, fn ->
              counter = Metadata.fetch!(table, :counter)
              :erlang.yield()

              Metadata.put(table, :counter, counter + 1)
            end)
          end
        end)
      end

    _ = Task.await_many(tasks)

    assert Metadata.fetch!(table, :counter) == 800
  end
end
