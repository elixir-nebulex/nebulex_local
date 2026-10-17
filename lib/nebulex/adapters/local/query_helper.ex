if Code.ensure_loaded?(Ex2ms) do
  defmodule Nebulex.Adapters.Local.QueryHelper do
    @moduledoc """
    Builds ETS match specifications for `Nebulex.Adapters.Local` from named
    fields, so you do not need to write the internal entry tuple
    `{:entry, key, value, touched, exp, tag}` yourself.

    The `match_spec/1` macro turns the named fields into the entry tuple and
    uses `Ex2ms` to generate the match spec. It requires the optional `:ex2ms`
    dependency.

    ## Getting Started

    The simplest way to use this module is with `use`:

        defmodule MyCache.Queries do
          use Nebulex.Adapters.Local.QueryHelper

          def by_key(key) do
            match_spec key: k, value: v, where: k == key, select: v
          end

          def by_tag(tag) do
            match_spec tag: t, where: t == tag, select: true
          end
        end

    ## Examples

        # Simple match on key
        match_spec key: k, value: v, where: k == :foo, select: v

        # Match with tag
        match_spec key: k, value: v, tag: t,
                   where: t == :important,
                   select: {k, v}

        # Complex guards
        match_spec key: k, value: v,
                   where: is_integer(k) and k > 10,
                   select: {k, v}

        # Without guards
        match_spec key: k, value: v, select: {k, v}

        # Return entire entry
        match_spec key: k, value: v, where: k > 100, select: :"$_"

        # Match only on specific fields
        match_spec tag: t, where: t == :important, select: true

        # Complex conditions
        match_spec key: k, value: v, tag: t, exp: e,
                   where: k > t and e != :infinity,
                   select: {k, v, e}

    ## Supported field bindings

    - `:key` - The cache key
    - `:value` - The cached value
    - `:touched` - Timestamp when entry was last touched
    - `:exp` - Expiration timestamp (or `:infinity`)
    - `:tag` - Optional tag metadata

    Fields not mentioned in the pattern will be wildcarded with `_`.

    ## Clauses

    - Field bindings (`:key`, `:value`, `:tag`, `:touched`, `:exp`) - Bind entry
      fields to variables.
    - `:where` - Optional guard conditions (supports all ETS guard functions).
    - `:select` - What to return (optional, defaults to `true`).

    ## Using match specs with cache operations

    Use the match specs with `get_all`, `count_all`, `delete_all`, and
    `stream`:

        use Nebulex.Adapters.Local.QueryHelper

        # Get all values where the key is an integer greater than 10
        ms = match_spec key: k, value: v, where: is_integer(k) and k > 10, select: v
        MyCache.get_all!(query: ms)

        # Count the entries with a specific tag
        ms = match_spec tag: t, where: t == :user_session
        MyCache.count_all!(query: ms)

        # Delete expired entries (exp is not :infinity and less than now)
        now = System.system_time(:millisecond)
        ms = match_spec exp: e, where: e != :infinity and e < now
        MyCache.delete_all!(query: ms)

        # Stream entries in batches
        ms = match_spec value: v, where: is_binary(v), select: v
        MyCache.stream!(query: ms) |> Enum.take(100)

    To reuse queries, put them in a module:

        defmodule MyApp.CacheQueries do
          use Nebulex.Adapters.Local.QueryHelper

          # Find all entries for a specific user
          def user_entries(user_id) do
            match_spec tag: t, where: t == {:user, user_id}, select: :"$_"
          end

          # Find entries expiring within the next hour (not expired yet)
          def expiring_soon do
            now = System.system_time(:millisecond)
            cutoff = now + :timer.hours(1)

            match_spec exp: e, where: e != :infinity and e > now and e < cutoff
          end

          # Filter by tag and the time the entry was last written or touched
          def recent_tagged_entries(tag, min_time) do
            match_spec key: k,
                       value: v,
                       tag: t,
                       touched: ts,
                       where: t == tag and ts > min_time,
                       select: {k, v}
          end
        end

        MyApp.Cache.get_all!(query: MyApp.CacheQueries.user_entries(123))
        MyApp.Cache.delete_all!(query: MyApp.CacheQueries.expiring_soon())

    ## Tags and caching decorators

    The `Nebulex.Caching` decorators can tag the entries they write, and
    `cache_evict` can take a QueryHelper match spec to evict entries by tag or
    by reference.

    ### Tagging entries from decorators

    Pass the tag through the decorator's `:opts` option:

        defmodule MyApp.UserCache do
          use Nebulex.Caching, cache: MyApp.Cache
          use Nebulex.Adapters.Local.QueryHelper

          # Cache user data, tagged with the user ID
          @decorate cacheable(key: {:user, user_id}, opts: [tag: {:user, user_id}])
          def get_user(user_id) do
            # fetch user from database
            {:ok, user}
          end

          # Cache user permissions under their own key, with the same tag
          @decorate cacheable(key: {:permissions, user_id}, opts: [tag: {:user, user_id}])
          def get_user_permissions(user_id) do
            # fetch permissions from database
            {:ok, permissions}
          end

          # Store session data, tagged as a session
          @decorate cache_put(key: session_id, opts: [tag: :sessions])
          def create_session(session_id, data) do
            data
          end
        end

    Tags are metadata, not part of the key. With the default `:set` table
    type, writing the same key replaces its entry regardless of the tag, so
    give each kind of data its own key.

    ### Evicting entries by tag

    Use `cache_evict` with a function that returns the match spec:

        defmodule MyApp.UserCache do
          use Nebulex.Caching, cache: MyApp.Cache
          use Nebulex.Adapters.Local.QueryHelper

          # ... cacheable functions as above ...

          # Evict all cached data for a specific user
          @decorate cache_evict(query: &evict_user_query/1)
          def invalidate_user(user_id) do
            :ok
          end

          defp evict_user_query(%{args: [user_id]} = _context) do
            match_spec tag: t, where: t == {:user, user_id}
          end
        end

    The same approach clears all the sessions of a user, or all sessions:

        defmodule MyApp.Sessions do
          use Nebulex.Caching, cache: MyApp.Cache
          use Nebulex.Adapters.Local.QueryHelper

          @decorate cache_put(key: session_id, opts: [tag: {:session, user_id}])
          def store_session(user_id, session_id, data) do
            data
          end

          # Evict all sessions of a user when they log out
          @decorate cache_evict(query: &evict_user_sessions_query/1)
          def logout_user(user_id) do
            :ok
          end

          # Evict all sessions of all users (e.g., during maintenance)
          @decorate cache_evict(query: &evict_all_sessions_query/1)
          def clear_all_sessions do
            :ok
          end

          defp evict_user_sessions_query(%{args: [user_id]} = _context) do
            match_spec tag: t, where: t == {:session, user_id}
          end

          defp evict_all_sessions_query(_context) do
            match_spec tag: t, where: is_tuple(t) and elem(t, 0) == :session
          end
        end

    ### Evicting cache references

    When a decorator caches a value under several keys with the `:references`
    option, `keyref_match_spec/2` finds the reference entries that point to a
    key, so `cache_evict` can remove them all at once:

        defmodule MyApp.UserAccounts do
          use Nebulex.Caching, cache: MyApp.Cache
          use Nebulex.Adapters.Local.QueryHelper

          @decorate cacheable(key: user_id)
          def get_user_account(user_id) do
            fetch_user(user_id)
          end

          @decorate cacheable(key: email, references: &(&1 && &1.id))
          def get_user_account_by_email(email) do
            fetch_user_by_email(email)
          end

          @decorate cacheable(key: token, references: &(&1 && &1.id))
          def get_user_account_by_token(token) do
            fetch_user_by_token(token)
          end

          # Evict the user and all the lookups (by email and token) that
          # reference it
          @decorate cache_evict(key: user_id, query: &invalidate_refs/1)
          def update_user_account(user_id, attrs) do
            :ok
          end

          defp invalidate_refs(%{args: [user_id | _]} = _context) do
            keyref_match_spec(user_id)
          end
        end

    ## Internal entry format

    The internal ETS entry is an Erlang record with the structure:
    `{:entry, key, value, touched, exp, tag}`

    The `match_spec/1` macro builds this tuple for you.
    """

    import Ex2ms

    ## API

    @doc """
    Imports the query helper macros and `Ex2ms` for building match specifications.

    When you `use` this module, it automatically imports:
    - `Ex2ms` - For the underlying match spec generation (`fun do ... end`)
    - `Nebulex.Adapters.Local.QueryHelper` - For the `match_spec/1` macro

    ## Usage

        defmodule MyModule do
          use Nebulex.Adapters.Local.QueryHelper

          def my_query do
            match_spec key: k, value: v, where: k == :foo, select: v
          end
        end

    ## Alternative: Manual Import

    If you prefer more control, you can import modules manually:

        defmodule MyModule do
          import Ex2ms
          import Nebulex.Adapters.Local.QueryHelper

          def my_query do
            match_spec key: k, value: v, where: k == :foo, select: v
          end
        end

    ## Note

    The `match_spec/1` macro requires `Ex2ms` to be available. If you don't use
    this module or manually import `Ex2ms`, you'll get a compilation error when
    trying to use `match_spec/1`.
    """
    defmacro __using__(_opts) do
      quote do
        import Ex2ms
        import Nebulex.Adapters.Local.QueryHelper
      end
    end

    @doc """
    Builds an ETS match specification from a SQL-like declarative syntax.

    The macro accepts a keyword list with field bindings, optional guards
    (`:where`), and a return expression (`:select`).

    ## Requirements

    This macro requires `Ex2ms` to be available. The recommended approach is to
    use this module:

        use Nebulex.Adapters.Local.QueryHelper

    Alternatively, you can manually import both modules:

        import Ex2ms
        import Nebulex.Adapters.Local.QueryHelper

    ## Syntax

        match_spec field: var, ..., where: guards, select: return_value

    Where:
    - Field bindings (`:key`, `:value`, `:tag`, `:touched`, `:exp`) bind entry
      fields to variables.
    - `:where` - Optional guard clause with conditions.
    - `:select` - Optional return expression (defaults to `true`).

    ## Examples

        # Match all entries where key equals :foo
        match_spec key: k, value: v, where: k == :foo, select: v

        # Match entries with specific tag
        match_spec key: k, tag: t, where: t == :important, select: k

        # Complex guards and return values
        match_spec key: k, value: v, exp: e,
                   where: is_integer(v) and e != :infinity,
                   select: {k, v, e}

        # Without guards
        match_spec key: k, value: v, select: {k, v}

        # Return the entire entry
        match_spec value: v, tag: t, where: t == :foo, select: :"$_"

        # Using default select: true (useful for count_all/delete_all)
        match_spec tag: t, where: t == :important
        # Same as: match_spec tag: t, where: t == :important, select: true

    ## Notes

    - Fields not mentioned will be wildcarded (`:_`).
    - The order of fields doesn't matter.
    - Guards support all ETS guard functions (via Ex2ms).
    - The `:select` clause is optional and defaults to `true`.
    """
    defmacro match_spec(opts) do
      # Separate field bindings from where/select clauses.
      {select, opts} = Keyword.pop(opts, :select, true)
      {where, field_bindings} = Keyword.pop(opts, :where)

      # Build the ETS tuple from field bindings.
      entry_pattern = build_entry_tuple(field_bindings)

      # Build the clause for Ex2ms.
      clause =
        if where do
          quote do
            unquote(entry_pattern) when unquote(where) -> unquote(select)
          end
        else
          quote do
            unquote(entry_pattern) -> unquote(select)
          end
        end

      # Generate the Ex2ms fun (Ex2ms should be imported by the caller).
      quote do
        Ex2ms.fun do
          unquote(clause)
        end
      end
    end

    @doc """
    Builds a match spec for finding cache reference entries (keyrefs).

    This is useful for invalidating all reference keys that point to a specific
    referenced key. Cache references are created using the `keyref/2` function
    in Nebulex caching decorators.

    The match spec returns the reference key (the cache key that points to the
    referenced key). This works seamlessly with `get_all/1`, `delete_all/1`, and
    `count_all/1` operations.

    ## Parameters

      * `referenced_key` - The key that references point to (required).
      * `opts` - Optional keyword list:
        * `:cache` - Filter references to a specific cache (optional). When not
          provided, matches references in any cache (including `nil` for local
          references).

    ## Examples

        # Delete all reference entries pointing to a specific key (any cache)
        ms = keyref_match_spec(:user_123)
        MyCache.delete_all!(query: ms)

        # Delete reference entries pointing to a key in a specific cache
        ms = keyref_match_spec(:user_123, cache: MyApp.UserCache)
        MyCache.delete_all!(query: ms)

        # Count how many references point to a key
        ms = keyref_match_spec(:product_456)
        MyCache.count_all!(query: ms)

        # Get all cache keys that are references to a specific key
        ms = keyref_match_spec(:user_123)
        reference_keys = MyCache.get_all!(query: ms)

    ## Background

    When using the `:references` option with caching decorators, Nebulex stores
    reference entries as keyref records with the structure:
    `{:"$nbx_keyref_spec", cache, key, ttl}`. This function helps you query and
    clean up these reference entries.

    ## See also

      * `Nebulex.Caching` - For information about cache references and the
        `:references` option.

    """
    @spec keyref_match_spec(term(), keyword()) :: :ets.match_spec()
    def keyref_match_spec(referenced_key, opts \\ []) do
      {cache_filter, _opts} = Keyword.pop(opts, :cache)

      # Always return the reference key (the cache key that points to the
      # referenced key).
      # This works for get_all (returns the keys), and the adapter overrides it for
      # delete_all/count_all to return true as needed.
      if cache_filter do
        fun do
          {:entry, k, {:"$nbx_keyref_spec", c, ref_k, _t}, _, _, _}
          when ref_k == ^referenced_key and c == ^cache_filter ->
            k
        end
      else
        fun do
          {:entry, k, {:"$nbx_keyref_spec", _c, ref_k, _t}, _, _, _}
          when ref_k == ^referenced_key ->
            k
        end
      end
    end

    ## Private functions

    # Build the ETS tuple `{:entry, key, value, touched, exp, tag}` from the
    # user bindings.
    defp build_entry_tuple(fields) do
      # Convert keyword list to map for easier lookup.
      field_map = Map.new(fields)

      # Validate that only valid fields are provided.
      valid_fields = [:key, :value, :touched, :exp, :tag]
      invalid_fields = Map.keys(field_map) -- valid_fields

      unless Enum.empty?(invalid_fields) do
        raise ArgumentError, """
        Invalid field(s): #{inspect(invalid_fields)}

        Valid fields are: #{inspect(valid_fields)}
        """
      end

      # The order of the fields in the entry record:
      # `{:entry, key, value, touched, exp, tag}`.
      ordered_fields = [:key, :value, :touched, :exp, :tag]

      # Build the tuple elements.
      elements = [
        :entry | Enum.map(ordered_fields, &Map.get(field_map, &1, {:_, [], Elixir}))
      ]

      # Return as a tuple AST node.
      {:{}, [], elements}
    end
  end
end
