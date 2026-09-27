## Local Scope First (`nebulex_local`)

This repository is `nebulex_local`, the generational local cache adapter for
[Nebulex](https://github.com/elixir-nebulex/nebulex), not Nebulex core. When
imported Nebulex sections reference missing `usage-rules/*.md` paths or
Nebulex-core files, treat them as upstream guidance and prioritize this
repository's local files and modules.

Read `usage-rules/architecture.md` first. It covers why this package exists,
how generational caching works, the module structure, the implemented
behaviours, and the non-negotiable contribution rules.

### Running Tests

Run `mix deps.get` first. It also fetches the rule files that the `@deps/…`
references at the bottom of this file point to; they do not exist in a fresh
clone.

The test suite reuses the shared test cases from the Nebulex core repository,
which are not part of the Hex package. `mix nbx.setup` clones the `main`
branch into `./nebulex`, which is what CI runs against:

```bash
mix deps.get
mix nbx.setup
NEBULEX_PATH=nebulex mix test
```

To run against another checkout, point `NEBULEX_PATH` at it.

### Local Rule Precedence (for this repo)

When rules conflict, apply them in this order. Items 2–5 refer to the
rule files referenced via `@deps/…` at the bottom of this file; load
them as part of session bootstrap.

1. This local preface.
2. `nebulex:workflow`.
3. `nebulex:nebulex` (as framework guidance).
4. `nebulex:elixir-style` and `nebulex:elixir`.
5. `usage_rules` and `usage_rules:otp`.

### Style Rules Are Non-Negotiable

Every change in `lib/` and `test/` must follow `nebulex:elixir-style`. Do not
treat these rules as suggestions, and do not skip them because
`mix format` and `mix credo --strict` pass; the tools do not check most of
them. Before you finish a change, check the diff against the rules. Pay
attention to these rules, which are easy to miss:

- All clauses of a function use the same form: all single-line (`do:`) or
  all multiline (`do ... end`). Do not mix the two forms.
- A function body with more than one expression has a blank line before the
  final expression. This includes `fn` bodies and `case`/`cond` clause
  bodies.
- Add a blank line after a multiline assignment.
- A function that returns a pipe writes the pipe on multiple lines.
- Do not pipe once from a value that is not a function call.

### Local Key Files

> Keep this list current when modules are added, moved, or removed.
> A brief one-line description per file is enough.

- `lib/nebulex/adapters/local.ex` - Generational local adapter (KV, CompositeKV, Queryable, Info, and Transaction callbacks).
- `lib/nebulex/adapters/local/backend.ex` - Backend behaviour and shared child-spec helpers.
- `lib/nebulex/adapters/local/backend/ets.ex` - ETS backend.
- `lib/nebulex/adapters/local/backend/shards.ex` - `:shards` backend (partitioned tables).
- `lib/nebulex/adapters/local/generation.ex` - Generation manager and garbage collector (GenServer).
- `lib/nebulex/adapters/local/metadata.ex` - ETS-backed adapter metadata store.
- `lib/nebulex/adapters/local/options.ex` - Adapter option definitions and docs.
- `lib/nebulex/adapters/local/query_helper.ex` - SQL-like match-spec builder and `keyref_match_spec/2`.
- `lib/nebulex/locks.ex` - ETS-based local locks used by transactions.
- `lib/nebulex/locks/options.ex` - `Nebulex.Locks` option definitions and docs.
- `usage-rules/architecture.md` - Architecture, non-negotiables, and source-of-truth hierarchy.
- `test/shared/local_test_case.exs` - Adapter-specific shared suite (`deftests`), run on both backends.
- `test/shared/cache_test_cache.exs` - Pulls in the generic Nebulex shared cache suites.
- `test/support/test_cache.exs` - Test cache module used by the shared suites.
- `test/nebulex/adapters/local_ets_test.exs` - Shared suites on the ETS backend.
- `test/nebulex/adapters/local_shards_test.exs` - Shared suites on the `:shards` backend.
- `test/nebulex/adapters/local_ordered_set_test.exs` - `{:in, keys}` semantics on `:ordered_set` tables.
- `test/nebulex/adapters/local_duplicate_keys_test.exs` - Duplicate-key semantics on `:duplicate_bag` tables.
- `test/nebulex/adapters/local_caching_test.exs` - Caching decorators with `QueryHelper`.
- `test/nebulex/adapters/local_error_test.exs` - Error-path tests.
- `test/nebulex/adapters/local/*_test.exs` - Generation, info, stats, and query helper tests.
- `test/nebulex/locks_test.exs` - `Nebulex.Locks` tests.
- `README.md` - Public usage and configuration for this package.
- `CHANGELOG.md` - Package release history.

<!-- usage-rules-start -->
<!-- nebulex:workflow-start -->
## nebulex:workflow usage
@deps/nebulex/usage-rules/workflow.md
<!-- nebulex:workflow-end -->
<!-- nebulex:nebulex-start -->
## nebulex:nebulex usage
@deps/nebulex/usage-rules/nebulex.md
<!-- nebulex:nebulex-end -->
<!-- nebulex:elixir-style-start -->
## nebulex:elixir-style usage
@deps/nebulex/usage-rules/elixir-style.md
<!-- nebulex:elixir-style-end -->
<!-- nebulex:elixir-start -->
## nebulex:elixir usage
@deps/nebulex/usage-rules/elixir.md
<!-- nebulex:elixir-end -->
<!-- usage_rules-start -->
## usage_rules usage
_A config-driven dev tool for Elixir projects to manage AGENTS.md files and agent skills from dependencies_

@deps/usage_rules/usage-rules.md
<!-- usage_rules-end -->
<!-- usage_rules:otp-start -->
## usage_rules:otp usage
@deps/usage_rules/usage-rules/otp.md
<!-- usage_rules:otp-end -->
<!-- usage-rules-end -->
