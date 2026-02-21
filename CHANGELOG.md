# Changelog

All notable changes to this project will be documented in this file.

This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
