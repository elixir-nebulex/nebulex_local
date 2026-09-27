defmodule Nebulex.Adapters.LocalWithShardsTest do
  use ExUnit.Case, async: true

  # Inherit tests
  use Nebulex.Adapters.Local.CacheTestCase
  use Nebulex.Adapters.LocalTest

  import Nebulex.CacheCase, only: [setup_with_dynamic_cache: 3]

  alias Nebulex.Adapter
  alias Nebulex.Adapters.Local.TestCache, as: Cache

  setup_with_dynamic_cache Cache, :local_with_shards, backend: :shards

  describe "shards" do
    test "backend", %{name: name} do
      assert Adapter.lookup_meta(name).backend == :shards
    end

    test "custom partitions" do
      defmodule CustomPartitions do
        use Nebulex.Cache,
          otp_app: :nebulex,
          adapter: Nebulex.Adapters.Local
      end

      :ok = Application.put_env(:nebulex, CustomPartitions, backend: :shards, partitions: 2)
      {:ok, _pid} = CustomPartitions.start_link()

      assert CustomPartitions.newer_generation()
             |> :shards.table_meta()
             |> :shards_meta.partitions() == 2

      :ok = CustomPartitions.stop()
    end
  end
end
