# Architecture

This guide explains why `nebulex_local` exists, how it is structured
internally, and the non-negotiable rules that govern every contribution.
Read this before making any significant change to the codebase.

For the broader Nebulex ecosystem context — the adapter pattern, behaviour
contracts, and shared non-negotiables — read the
[core Nebulex architecture guide][core-arch] first.

[core-arch]: https://hexdocs.pm/nebulex/architecture.html

---

## Why This Package Exists

The core `nebulex` package defines the caching abstraction but ships no
production cache implementation. `nebulex_local` provides the local in-memory
layer: a generational ETS-based cache designed for single-node, high-throughput
scenarios.

It is the foundation most Nebulex topologies build on — the Partitioned,
Replicated, and Coherent adapters in `nebulex_distributed` all use
`nebulex_local` as their per-node primary storage.

---

## How It Works — Generational Caching

The core mechanism is **generational garbage collection**, inspired by
[epocxy cache](https://github.com/duomark/epocxy/blob/master/src/cxy_cache.erl).

Instead of expiring individual entries with timers, the adapter maintains
**two ETS table generations** (`:new` and `:old`):

```ascii
  Write        Read (hit in new)    Read (hit in old)
    |                 |                    |
    v                 v                    v
+--------+       +--------+          +--------+
|  new   |<------|  new   |          |  new   |<-- migrated
+--------+       +--------+          +--------+
|  old   |       |  old   |  miss -> |  old   |
+--------+       +--------+          +--------+
```

- Writes always go to the `:new` generation.
- Reads check `:new` first, then `:old`. A hit in `:old` **migrates** the
  entry to `:new` (keeping hot data alive).
- When the GC runs (on `:gc_interval` timeout or memory threshold), a new
  generation is created and the oldest is deleted — no per-entry timer needed.

This design makes TTL-based expiration approximate: entries expire somewhere
between one and two GC intervals after their TTL elapses.

---

## Module Structure

```ascii
lib/
+-- nebulex/
    +-- adapters/
    |   +-- local.ex              # Main adapter — public API + behaviour impl
    |   +-- local/
    |       +-- generation.ex     # GC manager (GenServer) — owns ETS tables
    |       +-- metadata.ex       # ETS metadata table (generation refs, stats)
    |       +-- backend.ex        # Backend behaviour (ETS vs Shards)
    |       +-- backend/
    |       |   +-- ets.ex        # Default ETS backend
    |       |   +-- shards.ex     # Shards backend (partitioned ETS)
    |       +-- options.ex        # NimbleOptions schema for all config
    |       +-- query_helper.ex   # Match spec builder DSL for queries
    +-- locks.ex                  # ETS-based locking for transactions
    +-- locks/
        +-- options.ex            # NimbleOptions schema for lock options
```

### Key modules

| Module | Responsibility |
|---|---|
| `Nebulex.Adapters.Local` | Adapter entry point — implements all behaviours, delegates to generation/backend |
| Nebulex.Adapters.Local.Generation | GenServer managing GC lifecycle and ETS table rotation |
| Nebulex.Adapters.Local.Metadata | ETS metadata table holding current generation references and stats counters |
| Nebulex.Adapters.Local.Backend | Behaviour defining the ETS/Shards abstraction |
| Nebulex.Adapters.Local.Backend.ETS | Default single-table ETS backend |
| Nebulex.Adapters.Local.Backend.Shards | Partitioned ETS backend via `:shards` (optional dep) |
| `Nebulex.Adapters.Local.QueryHelper` | DSL for building ETS match specs for `get_all/delete_all` queries |
| `Nebulex.Adapters.Local.Options` | NimbleOptions schema — all adapter config validated here |
| `Nebulex.Locks` | Lightweight ETS-based locking for transaction support |

---

## Implemented Behaviours

| Behaviour | Purpose |
|---|---|
| `Nebulex.Adapter` | Required — `init/1`, adapter lifecycle |
| `Nebulex.Adapter.KV` | `fetch`, `put`, `delete`, `take`, `has_key?`, `ttl`, `expire`, `put_all`, `get_all` |
| `Nebulex.Adapter.Queryable` | `get_all`, `count_all`, `delete_all` via ETS match specs |
| `Nebulex.Adapter.Transaction` | Optimistic locking via `Nebulex.Locks` |
| `Nebulex.Adapter.Observable` | Cache entry events via `Nebulex.Streams` (optional) |
| `Nebulex.Adapter.Info` | Stats counters (hits, misses, evictions, etc.) |

---

## Optional Dependencies

| Dep | Purpose | Default |
|---|---|---|
| `:shards` | Partitioned ETS backend for high-concurrency workloads | No |
| `:ex2ms` | Elixir-friendly match spec builder (used by QueryHelper) | No |
| `:telemetry` | Telemetry span events for cache operations | No |

The adapter works without any of these. Shards support is activated by
setting `backend: :shards` in config.

---

## Running Tests

```bash
# With local nebulex core checkout (recommended during development)
NEBULEX_PATH=nebulex mix test

# Against published nebulex package
mix test
```

Both ETS and Shards backends are tested by separate test files:
- `test/nebulex/adapters/local_ets_test.exs` — default ETS backend
- `test/nebulex/adapters/local_shards_test.exs` — Shards backend

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
Tests for both backends must pass before any PR is merged.

### 3. Optional deps must remain optional

`:shards`, `:ex2ms`, and `:telemetry` are optional. No code path in the
adapter may raise or fail when these deps are absent. Use
`Code.ensure_loaded?/1` guards where needed.

### 4. Keep this document up to date

After any structural change — adding or removing modules, changing implemented
behaviours, adding optional dependencies, or modifying the GC/backend
boundaries — review this document and update it if needed. A stale architecture
guide is worse than no guide.

### 5. `mix test.ci` must pass

```bash
mix test.ci
```

### 6. `mix docs` must produce no warnings

```bash
mix docs
```

---

## Further Reading

- [Core Nebulex Architecture](https://hexdocs.pm/nebulex/architecture.html)
- [Creating a New Adapter](https://hexdocs.pm/nebulex/creating-new-adapter.html)
- [nebulex_local HexDocs](https://hexdocs.pm/nebulex_local)
