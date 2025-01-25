defmodule Nebulex.Adapters.Local.InfoTest do
  use ExUnit.Case, async: true

  import Nebulex.CacheCase

  alias Nebulex.Adapter
  alias Nebulex.Adapters.Common.Info.Stats

  ## Internals

  defmodule Cache do
    @moduledoc false
    use Nebulex.Cache,
      otp_app: :nebulex_local,
      adapter: Nebulex.Adapters.Local
  end

  @empty_stats Stats.new()

  ## Tests

  describe "c:Nebulex.Cache.info/1" do
    setup_with_cache Cache, stats: true, allocated_memory: 1_000_000

    test "ok: returns all info" do
      assert info = Cache.info!()
      assert Cache.info!(:all) == info

      assert info[:server] == server_info()
      assert info[:memory][:total] == 1_000_000
      assert info[:memory][:used] > 0
      assert info[:stats] == @empty_stats
    end

    test "ok: returns item's info" do
      assert Cache.info!(:server) == server_info()

      assert memory = Cache.info!(:memory)
      assert memory[:total] == 1_000_000
      assert memory[:used] > 0

      assert Cache.info!(:stats) == @empty_stats
    end

    test "ok: returns multiple items info" do
      assert Cache.info!([:server]) == %{server: server_info()}

      assert %{memory: memory} = Cache.info!([:memory])
      assert memory[:total] == 1_000_000
      assert memory[:used] > 0

      assert Cache.info!([:server, :memory]) == %{server: server_info(), memory: memory}
    end

    test "error: raises an exception because the requested item doesn't exist" do
      for spec <- [:unknown, [:memory, :unknown], [:unknown, :unknown]] do
        assert_raise ArgumentError, ~r"invalid information specification key :unknown", fn ->
          Cache.info!(spec)
        end
      end
    end
  end

  describe "c:Nebulex.Cache.info/1 (stats disabled)" do
    setup_with_cache Cache, stats: false

    test "ok: returns all info" do
      assert info = Cache.info!()

      assert info[:server] == server_info()
      refute info[:memory][:total]
      assert info[:stats] == @empty_stats
    end
  end

  ## Private functions

  defp server_info do
    adapter_meta = Adapter.lookup_meta(Cache)

    %{
      nbx_version: Nebulex.vsn(),
      cache_module: adapter_meta[:cache],
      cache_adapter: adapter_meta[:adapter],
      cache_name: adapter_meta[:name],
      cache_pid: adapter_meta[:pid]
    }
  end
end
