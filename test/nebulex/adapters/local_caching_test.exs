defmodule Nebulex.Adapters.LocalCachingTest do
  use ExUnit.Case, async: true

  defmodule Cache do
    @moduledoc false
    use Nebulex.Cache,
      otp_app: :nebulex_local,
      adapter: Nebulex.Adapters.Local
  end

  use Nebulex.Caching, cache: Cache
  use Nebulex.Adapters.Local.QueryHelper

  import Nebulex.CacheCase

  ## Tests

  setup_with_cache Cache

  setup do
    _ = Cache.delete_all!()

    :ok
  end

  describe "cacheable with entry tagging" do
    test "caches value with tag" do
      assert get_user(1) == "User 1"
      assert Cache.get!(1) == "User 1"

      # Verify tag was stored by querying with match_spec
      ms = match_spec key: k, tag: t, where: t == :users, select: k
      assert Cache.get_all(query: ms) == {:ok, [1]}
    end

    test "caches multiple values with different tags" do
      assert get_user(1) == "User 1"
      assert get_user(2) == "User 2"
      assert get_product(100) == "Product 100"
      assert get_product(200) == "Product 200"

      # Count users
      ms_users = match_spec key: k, tag: t, where: t == :users, select: k
      assert Cache.get_all!(query: ms_users) |> Enum.sort() == [1, 2]

      # Count products
      ms_products = match_spec key: k, tag: t, where: t == :products, select: k
      assert Cache.get_all!(query: ms_products) |> Enum.sort() == [100, 200]
    end

    test "caches with tag and retrieves via QueryHelper" do
      assert get_user(5) == "User 5"

      # Query entries by tag to verify they were cached
      ms = match_spec key: k, tag: t, where: t == :users, select: k
      assert Cache.get_all!(query: ms) == [5]
    end
  end

  describe "cache_put with entry tagging" do
    test "cache_put with tag stores value" do
      # Store score via cache_put
      assert set_user_score(1, 100) == 100
      assert Cache.get!(1) == 100

      # Update score
      assert set_user_score(1, 150) == 150
      assert Cache.get!(1) == 150

      # Verify tag
      ms = match_spec key: k, tag: t, where: t == :user_scores, select: k
      assert Cache.get_all!(query: ms) == [1]
    end

    test "cache_put with different tags" do
      assert put_important_data(10) == {:ok, "important"}
      assert put_temp_data(20) == {:ok, "temporary"}

      # Count important entries
      ms_important = match_spec key: k, tag: t, where: t == :important, select: k
      assert Cache.get_all!(query: ms_important) == [10]

      # Count temporary entries
      ms_temporary = match_spec key: k, tag: t, where: t == :temporary, select: k
      assert Cache.get_all!(query: ms_temporary) == [20]
    end
  end

  describe "cache_evict with QueryHelper" do
    test "evict entries identified by QueryHelper" do
      # Cache some data first
      assert get_user(1) == "User 1"
      assert get_user(2) == "User 2"

      # Verify before evict
      ms = match_spec key: k, tag: t, where: t == :users, select: k
      assert Cache.get_all!(query: ms) |> Enum.sort() == [1, 2]

      # Evict via QueryHelper query
      assert evict_users() == :ok

      # Verify entries were evicted
      assert Cache.get_all!(query: ms) == []
    end

    test "evict with complex condition using QueryHelper" do
      assert put_score(1, 100) == 100
      assert put_score(2, 200) == 200
      assert put_score(3, 300) == 300

      # Verify before evict (scores > 150 = 200, 300)
      ms_high = match_spec key: k, value: v, where: v > 150, select: k
      assert Cache.get_all!(query: ms_high) |> Enum.sort() == [2, 3]

      # Evict high scores (> 150)
      assert evict_high_scores() == :ok

      # Verify
      assert Cache.get!(1) == 100
      refute Cache.get!(2)
      refute Cache.get!(3)
    end

    test "evict entries by tag using QueryHelper" do
      assert put_session_data(:user_1, "session_1") == "session_1"
      assert put_session_data(:user_2, "session_2") == "session_2"

      # Verify before evict
      ms = match_spec key: k, tag: t, where: t == :sessions, select: k
      assert Cache.get_all!(query: ms) |> Enum.sort() == [:user_1, :user_2]

      # Evict all sessions
      assert evict_sessions() == :ok

      # Verify all sessions evicted
      assert Cache.get_all!(query: ms) == []
    end
  end

  describe "cache_evict with keyref_match_spec" do
    test "evict reference entries for a specific key" do
      # Cache entries that reference a specific key
      assert get_user_with_ref("user_1") == %{id: "user_1", name: "Alice"}
      assert get_user_by_email("user_1@example.com") == %{id: "user_1", name: "Alice"}

      # Verify reference entry exists for "user_1"
      ms = keyref_match_spec("user_1")
      assert length(Cache.get_all!(query: ms)) == 1

      # Evict all references to "user_1"
      assert evict_user_references("user_1") == :ok

      # Verify reference entry is gone
      assert Cache.get_all!(query: ms) == []
    end

    test "evict references for one key without affecting others" do
      # Cache entries that reference different keys
      assert get_user_with_ref("user_1") == %{id: "user_1", name: "Alice"}
      assert get_user_with_ref("user_2") == %{id: "user_2", name: "Bob"}

      # Verify reference entries exist for both
      ms_user1 = keyref_match_spec("user_1")
      ms_user2 = keyref_match_spec("user_2")
      assert length(Cache.get_all!(query: ms_user1)) == 1
      assert length(Cache.get_all!(query: ms_user2)) == 1

      # Evict only references to "user_1"
      assert evict_user_references("user_1") == :ok

      # Verify only user_1 reference is gone, user_2 remains
      assert Cache.get_all!(query: ms_user1) == []
      assert Cache.get_all!(query: ms_user2) == ["user_2"]
    end
  end

  describe "QueryHelper integration with decorators" do
    test "select entries by tag after cacheable" do
      assert get_user(10) == "User 10"
      assert get_product(100) == "Product 100"

      # Query users
      ms_users = match_spec key: k, tag: t, where: t == :users, select: k
      assert Cache.get_all!(query: ms_users) == [10]

      # Query products
      ms_products = match_spec key: k, tag: t, where: t == :products, select: k
      assert Cache.get_all!(query: ms_products) == [100]
    end

    test "count entries by tag using QueryHelper" do
      assert get_user(1) == "User 1"
      assert get_user(2) == "User 2"
      assert get_user(3) == "User 3"

      # Count users
      ms = match_spec key: k, tag: t, where: t == :users, select: k
      assert Cache.get_all!(query: ms) |> Enum.sort() == [1, 2, 3]
    end

    test "select entries with complex guards" do
      assert put_score(1, 50) == 50
      assert put_score(2, 150) == 150
      assert put_score(3, 250) == 250

      # Query scores > 100 (should return 150 and 250)
      ms = match_spec key: k, value: v, where: v > 100, select: {k, v}
      assert Cache.get_all!(query: ms) |> Enum.sort() == [{2, 150}, {3, 250}]
    end

    test "batch operations with decorators and tags" do
      # Cache multiple entries with same tag
      assert get_user(5) == "User 5"
      assert get_user(6) == "User 6"
      assert get_user(7) == "User 7"

      # Query all users
      ms = match_spec key: k, tag: t, where: t == :users, select: k
      assert Cache.get_all!(query: ms) |> Enum.sort() == [5, 6, 7]

      # Evict all users
      assert evict_users() == :ok
      assert Cache.get_all!(query: ms) == []
    end
  end

  ## Decorated Functions with Tags

  @decorate cacheable(key: id, opts: [tag: :users])
  def get_user(id) do
    "User #{id}"
  end

  @decorate cacheable(key: id, opts: [tag: :products])
  def get_product(id) do
    "Product #{id}"
  end

  @decorate cacheable(key: user_id, references: &(&1 && &1.id))
  def get_user_with_ref(user_id) do
    %{id: user_id, name: user_name(user_id)}
  end

  @decorate cacheable(key: email, references: &(&1 && &1.id))
  def get_user_by_email(email) do
    user_id = email_to_user_id(email)
    %{id: user_id, name: user_name(user_id)}
  end

  @decorate cache_put(key: id, opts: [tag: :user_scores])
  def set_user_score(id, score) do
    _ = id
    score
  end

  @decorate cache_put(key: id, match: &match_ok/1, opts: [tag: :important])
  def put_important_data(id) do
    _ = id
    {:ok, "important"}
  end

  @decorate cache_put(key: id, match: &match_ok/1, opts: [tag: :temporary])
  def put_temp_data(id) do
    _ = id
    {:ok, "temporary"}
  end

  @decorate cache_put(key: key, opts: [tag: :sessions])
  def put_session_data(key, value) do
    _ = key
    value
  end

  @decorate cache_put(key: id)
  def put_score(id, score) do
    _ = id
    score
  end

  @decorate cache_evict(query: &evict_users_query/1)
  def evict_users do
    :ok
  end

  @decorate cache_evict(query: &evict_high_scores_query/1)
  def evict_high_scores do
    :ok
  end

  @decorate cache_evict(query: &evict_sessions_query/1)
  def evict_sessions do
    :ok
  end

  @decorate cache_evict(query: &evict_user_references_query/1)
  def evict_user_references(user_id) do
    _ = user_id
    :ok
  end

  ## Query Functions for cache_evict

  defp evict_users_query(_args) do
    match_spec tag: t, where: t == :users, select: true
  end

  defp evict_high_scores_query(_args) do
    match_spec value: v, where: v > 150, select: true
  end

  defp evict_sessions_query(_args) do
    match_spec tag: t, where: t == :sessions, select: true
  end

  defp evict_user_references_query(ctx) do
    [user_id] = ctx.args
    keyref_match_spec(user_id)
  end

  ## Match Functions

  defp match_ok({:ok, _}), do: true
  defp match_ok(_), do: false

  ## Helper Functions

  defp user_name("user_1"), do: "Alice"
  defp user_name("user_2"), do: "Bob"
  defp user_name("user_3"), do: "Charlie"

  defp email_to_user_id("user_1@example.com"), do: "user_1"
  defp email_to_user_id("user_2@example.com"), do: "user_2"
  defp email_to_user_id("user_3@example.com"), do: "user_3"
end
