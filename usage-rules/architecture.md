# Architecture

This guide covers what the code and the module docs do not: how the modules
split responsibility and the non-negotiable rules for every change. Read the
"Non-Negotiables" section at session start; read the rest before structural
changes.

For how the adapter works and how to configure it, read the module docs:

- `Nebulex.Adapters.Local` - generational caching, the retry on tables
  deleted by the GC, the `:ttl` option, eviction, and queries.
- `Nebulex.Adapters.Local.Generation` - the generation manager and the GC.
- `Nebulex.Adapters.Local.QueryHelper` - the match-spec builder.

For the broader Nebulex context (the adapter pattern, behaviour contracts,
and shared non-negotiables), read the
[core Nebulex architecture guide][core-arch].

[core-arch]: https://hexdocs.pm/nebulex/architecture.html

---

## Purpose

The core `nebulex` package defines the caching abstraction but ships no
production cache. `nebulex_local` is the local in-memory layer: a
generational ETS-based cache for single-node, high-throughput use. The
Partitioned, Replicated, and Coherent adapters in `nebulex_distributed` use
it as their per-node storage, so a regression here affects every topology
built on it.

---

## Module Boundaries

This section covers how the modules split responsibility; the file layout
is in `lib/` and `test/`.

- `Nebulex.Adapters.Local` is the adapter entry point. It implements the
  behaviours and delegates the table lifecycle to `Generation`.
- `Generation` is the GenServer that owns the generation tables and runs
  the GC (table rotation); it creates and deletes tables through
  `Backend`. `Metadata` holds the current generation references and the
  stats counters.
- `Backend` defines the ETS/`:shards` abstraction; `Backend.ETS` and
  `Backend.Shards` implement it (see Non-Negotiable #2).
- `QueryHelper` builds match specs for queries. `Options` and
  `Locks.Options` hold the NimbleOptions schemas; all config is validated
  there.
- `Nebulex.Locks` provides the ETS-based locks that back transactions.

---

## Non-Negotiables

These rules apply in addition to the shared non-negotiables in the core
Nebulex architecture guide.

### 1. GC correctness is critical

The generational GC is the core mechanism of this adapter. Any change
touching `Generation`, `Metadata`, or backend ETS access must be tested
under concurrent conditions. Race conditions between GC runs and cache
operations are subtle — do not remove or weaken the retry logic in
`local.ex` without a compelling, documented reason.

### 2. Both backends must stay in sync

`Backend.ETS` and `Backend.Shards` must implement the same behaviour
contract. A feature or fix applied to one must be applied to the other.
Tests for both backends (`local_ets_test.exs` and `local_shards_test.exs`)
must pass before any PR is merged.

### 3. Optional deps must remain optional

`:shards`, `:ex2ms`, and `:telemetry` are optional. No code path in the
adapter may raise or fail when these deps are absent. Use
`Code.ensure_loaded?/1` guards where needed.

### 4. Keep this document up to date

After any structural change (changing the module boundaries, adding an
optional dependency, or adding a non-negotiable), review this document and
update it if needed. When you add, move, or remove a file, check that this
section still holds. Do not copy here what the module docs already say;
link to them. A stale architecture guide is worse than no guide.

### 5. `mix precommit` must pass

```bash
NEBULEX_PATH=nebulex mix precommit
```

It includes the docs build with warnings as errors; fix the root cause of a
docs warning, do not suppress it.

### 6. Read why before changing subtle behavior

The reasons behind the subtle behavior (the promotion order, the retry on
deleted tables, and the per-key `{:in, keys}` lookups) are in the module docs,
the code comments, and the tests. Read them, and the history (`git log -S`),
before changing that code. Do not reverse such behavior without a test that
covers the original problem.

---

## Further Reading

- [Core Nebulex Architecture](https://hexdocs.pm/nebulex/architecture.html)
- [Creating a New Adapter](https://hexdocs.pm/nebulex/creating-new-adapter.html)
- [nebulex_local HexDocs](https://hexdocs.pm/nebulex_local)
