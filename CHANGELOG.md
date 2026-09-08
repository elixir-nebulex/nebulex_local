# Changelog

All notable changes to this project will be documented in this file.

This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Bug fixes

- [Nebulex.Adapters.Local] Fixed two race conditions in the promotion of
  entries from the older generation into the newer one on `fetch` (and the
  commands built on it). First, the promotion removed the entry from the
  older generation before it inserted the entry into the newer one; during
  that window, a concurrent read of the same key reported a false miss for
  a cached entry. The promotion now inserts first and deletes after, and a
  reader that misses both generations checks the newer one again before it
  reports a miss. Second, the promotion could override a value a concurrent
  write had already stored for the same key; the promotion now uses
  `insert_new`, so the concurrent write wins.
  [#10](https://github.com/elixir-nebulex/nebulex_local/issues/10).
- [Nebulex.Adapters.Local] Fixed severe performance degradation of `get_all`,
  `count_all`, `delete_all`, and `stream` with `{:in, keys}` queries. Keys are
  now bound in the ETS match head (or fetched per key), so ETS uses the key
  index instead of scanning the whole table per chunk of keys — O(keys)
  instead of O(table size × keys). The same fix applies to the older
  generation purge performed by `put_all` and `put_new_all`; consequently,
  the `:purge_chunk_size` option is now deprecated and ignored.
  [#8](https://github.com/elixir-nebulex/nebulex_local/issues/8).
- [Nebulex.Adapters.Local] Fixed a data-loss bug in `{:in, keys}` queries: a
  key shaped like a match-spec variable (e.g. `:"$1"`) was compared inside
  the ETS match-spec guard, where it became a self-referential variable
  (`{:"=:=", :"$1", :"$1"}`, always true) instead of a literal value — so
  e.g. `delete_all(in: [:"$1"])` deleted every entry in the table rather
  than the one entry for that key. Keys are now bound directly in the match
  head, or compared as literal `{:const, key}` terms when that isn't
  possible (e.g. `:_`, `:"$N"` atoms, maps, structs), so reserved-looking
  keys are always matched as literal values. `count_all`, `delete_all`, and
  `stream` batch such non-indexable keys given in one call into chunked
  table scans (bounded per scan to stay under the ETS match-spec guard
  limit), instead of scanning once per such key.
- [Nebulex.Adapters.Local] On `:ordered_set` tables, `{:in, keys}` queries
  now compare keys with the table's native `==` semantics across `get_all`,
  `count_all`, `delete_all`, and `stream`, consistent with the single-key
  commands (e.g., an entry stored under the integer `1` matches the key
  `1.0`). Previously, `count_all`, `delete_all`, and `stream` compared keys
  with `=:=` and could disagree with `get_all` — an entry readable via
  `get_all(in: [1.0])` was not deletable via `delete_all(in: [1.0])`. These
  operations now look the keys up directly instead of using match specs on
  this backend type, and `delete_all` lazily removes (without counting)
  expired entries stored under the given keys.
- [Nebulex.Adapters.Local] Duplicate keys given to `{:in, keys}` queries are
  now processed once instead of once per occurrence.
- [Nebulex.Adapters.Local] `{:in, keys}` queries no longer silently ignore
  entries written with the `:tag` option, and `stream/2` no longer crashes
  when the given keys are tuples.
- [Nebulex.Adapters.Local] `get_all` with `{:in, keys}` now moves entries from
  the older generation into the newer one and lazily removes expired entries
  on read (like `fetch/2`), restoring the v2 read behavior. Note: entries
  removed this way during `get_all` with `{:in, keys}` are not currently
  reflected in the evictions/expirations/deletions stats counters; fixing
  that needs a change in Nebulex core, not this adapter.

## [v3.0.0](https://github.com/elixir-nebulex/nebulex_local/tree/v3.0.0) (2026-02-21)
> [Full Changelog](https://github.com/elixir-nebulex/nebulex_local/compare/v3.0.0-rc.2...v3.0.0)

### Enhancements

- [Nebulex.Adapters.Local] Implemented adapter-specific transaction support using
  `Nebulex.Locks`, a lightweight ETS-based locking mechanism optimized for
  single-node scenarios. This replaces the previous reliance on `:global` for
  distributed locking, providing significantly better performance for local
  cache transactions while maintaining the same public API. The locks manager
  can be customized via the new `:lock_opts` configuration option (e.g.,
  `:cleanup_interval`, `:cleanup_batch_size`). This change aligns with the
  removal of the default transaction implementation from Nebulex core, allowing
  adapters to provide implementations tailored to their specific needs.
  [#7](https://github.com/elixir-nebulex/nebulex_local/issues/7).
- [Nebulex.Locks] Improved retry behavior when `:retry_interval` is `0` by
  handling immediate retries without jitter. This prevents errors during lock
  acquisition retries and keeps zero-delay retry strategies working as expected.

## [v3.0.0-rc.2](https://github.com/elixir-nebulex/nebulex_local/tree/v3.0.0-rc.2) (2025-12-07)
> [Full Changelog](https://github.com/elixir-nebulex/nebulex_local/compare/v3.0.0-rc.1...v3.0.0-rc.2)

### Enhancements

- [Nebulex.Adapters.Local] Improved generation garbage collection performance by
  deleting ETS tables directly instead of flushing all objects first. This
  change significantly improves GC performance for large caches, reducing
  generation deletion from O(n) to O(1) complexity. The deprecated generation
  is now deleted after a grace period defined by the new `:gc_cleanup_delay`
  option (formerly `:gc_flush_delay`), which allows ongoing operations to
  complete safely before table removal.
  [#2](https://github.com/elixir-nebulex/nebulex_local/issues/2).
- [Nebulex.Adapters.Local] Added automatic retry logic to handle race conditions
  when accessing deleted generations during garbage collection. Operations now
  retry up to 3 times when encountering `ArgumentError` due to deleted ETS
  tables, automatically fetching fresh generation references. This prevents
  crashes and improves resilience during GC cycles, especially under high
  concurrency.
  [#3](https://github.com/elixir-nebulex/nebulex_local/issues/3).
- [Nebulex.Adapters.Local] Added support for entry tagging via the `:tag` option.
  Cache entries can now be tagged with arbitrary terms (atoms, tuples, strings,
  etc.) to enable logical grouping, selective invalidation, and efficient
  filtering using ETS match specifications. This feature is particularly useful
  for organizing related entries (e.g., user sessions, feature groups) and
  performing bulk operations on tagged subsets of the cache. Tags can be
  specified when using `put/3`, `put_all/2`, and related operations, and entries
  can be queried by tag using match specs.
  [#4](https://github.com/elixir-nebulex/nebulex_local/issues/4).
- [Nebulex.Adapters.Local] Added `Nebulex.Adapters.Local.QueryHelper` module
  providing a user-friendly, SQL-like syntax for building ETS match
  specifications. Instead of requiring users to know the internal entry tuple
  structure `{:entry, key, value, touched, exp, tag}`, QueryHelper offers named
  field bindings (`:key`, `:value`, `:tag`, `:touched`, `:exp`) with a
  declarative syntax. The `:select` clause is optional and defaults to `true`,
  making count and delete operations more concise. This dramatically improves
  the developer experience when working with the queryable API, especially for
  tag-based queries. Example:
  `match_spec value: v, tag: t, where: t == :group_a, select: v`.
  [#5](https://github.com/elixir-nebulex/nebulex_local/issues/5).
- [Nebulex.Adapters.Local] Added `keyref_match_spec/2` helper function to
  `Nebulex.Adapters.Local.QueryHelper` for managing cache reference entries
  (keyrefs). This helper simplifies finding and cleaning up reference entries
  created when using the `:references` option with `Nebulex.Caching` decorators.
  Users can now easily invalidate all cached representations of an entity with a
  simple function call, without needing to know the internal keyref structure
  `{:"$nbx_keyref_spec", cache, key, ttl}`. Example:
  `keyref_match_spec(:user_123) |> MyCache.delete_all!(query: ...)`. This works
  seamlessly with `get_all/1`, `count_all/1`, and `delete_all/1` operations, and
  supports optional cache filtering.
  [#6](https://github.com/elixir-nebulex/nebulex_local/issues/6).

### Backwards incompatible changes

- [Nebulex.Adapters.Local] Renamed `:gc_flush_delay` option to
  `:gc_cleanup_delay` to better reflect its purpose. The option now controls
  the delay before the deprecated generation is deleted (not just flushed).
  Please update your configuration accordingly.

## [v3.0.0-rc.1](https://github.com/elixir-nebulex/nebulex_local/tree/v3.0.0-rc.1) (2025-05-01)
> [Full Changelog](https://github.com/elixir-nebulex/nebulex_local/compare/b7b9c8924f0c4cbfa37c84bdbc152b23aaed067c...v3.0.0-rc.1)

### Enhancements

- [Nebulex.Adapters.Local] Added `:gc_memory_check_interval` option to run size
  and memory checks. The option receives a positive integer with the time in
  milliseconds or an anonymous function to get the timeout at runtime.

### Backwards incompatible changes

- [Nebulex.Adapters.Local] Removed `:gc_cleanup_min_timeout` option.
  Please use `:gc_memory_check_interval` instead.
- [Nebulex.Adapters.Local] Removed `:gc_cleanup_max_timeout` option.
  Please use `:gc_memory_check_interval` instead.

### Closed issues

- Migrate local adapter to Nebulex v3
  [#1](https://github.com/elixir-nebulex/nebulex_local/issues/1)



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
