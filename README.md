# Nebulex Local
> A generational local cache adapter for [Nebulex][Nebulex].

[Nebulex]: https://github.com/cabol/nebulex

![CI](https://github.com/elixir-nebulex/nebulex_local/workflows/CI/badge.svg)
[![Codecov](https://codecov.io/gh/elixir-nebulex/nebulex_local/graph/badge.svg)](https://codecov.io/gh/elixir-nebulex/nebulex_local)
[![Hex Version](https://img.shields.io/hexpm/v/nebulex_local.svg)](https://hex.pm/packages/nebulex_local)
[![Documentation](https://img.shields.io/badge/Documentation-ff69b4)](https://hexdocs.pm/nebulex_local)

## About

`nebulex_local` is an in-memory cache adapter for [Nebulex][Nebulex], inspired
by [epocxy](https://github.com/duomark/epocxy). It stores entries in ETS and
uses two generations, newer and older, to manage eviction. Fetching an entry
from the older generation moves it into the newer one. When the cache creates
a new generation, it retires the oldest.

See the [module documentation][online_docs] for details on expiration,
eviction, concurrency, and configuration.

## Installation

Add `:nebulex_local` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:nebulex_local, "~> 3.1"},
    {:telemetry, "~> 0.4 or ~> 1.0"}, # For observability/telemetry support
    {:decorator, "~> 1.4"},           # For declarative caching
    {:shards, "~> 1.1"},              # For high concurrency workloads with partitioning
    {:ex2ms, "~> 1.7"}                # For match specification query helpers
  ]
end
```

The following dependencies are optional. Add the ones your application uses:

  - **`:telemetry`** - To emit and consume Telemetry events for cache
    operations (recommended).
  - **`:decorator`** - To use the declarative caching decorators
    (recommended).
  - **`:shards`** - To use `backend: :shards`, which partitions the cache
    across several ETS tables for intensive workloads.
  - **`:ex2ms`** - To build match spec queries with
    `Nebulex.Adapters.Local.QueryHelper`.

## Usage

Define a cache module:

```elixir
defmodule MyApp.LocalCache do
  use Nebulex.Cache,
    otp_app: :my_app,
    adapter: Nebulex.Adapters.Local
end
```

Configure the cache in your application environment, usually in
`config/config.exs`:

```elixir
config :my_app, MyApp.LocalCache,
  gc_interval: :timer.hours(12),
  max_size: 1_000_000,
  allocated_memory: 2_000_000_000,
  gc_memory_check_interval: :timer.seconds(10)
```

Add the cache to your application's supervision tree, usually in
`lib/my_app/application.ex`:

```elixir
def start(_type, _args) do
  children = [
    MyApp.LocalCache
  ]

  opts = [strategy: :one_for_one, name: MyApp.Supervisor]

  Supervisor.start_link(children, opts)
end
```

See the [online documentation][online_docs] for more information.

[online_docs]: https://hexdocs.pm/nebulex_local/

## Testing

The test suite uses support modules and shared tests from the Nebulex
repository, which are not included in the Hex package. Fetch the
dependencies, clone Nebulex with `mix nbx.setup`, and run the tests with
`NEBULEX_PATH` pointing to that checkout:

```
mix deps.get
mix nbx.setup
NEBULEX_PATH=nebulex mix test
```

Running tests with coverage:

```
NEBULEX_PATH=nebulex mix coveralls.html
```

You will find the coverage report within `cover/excoveralls.html`.

## Benchmarks

The [benchmarks](./benchmarks) directory contains benchmarks for both
backends, written with [Benchee](https://github.com/bencheeorg/benchee). To
run one:

```
$ MIX_ENV=test mix run benchmarks/BENCH_TEST_FILE
```

Where `BENCH_TEST_FILE` can be any of:

  * `local_with_ets_bench.exs`: benchmark for the local adapter using
    `:ets` backend.
  * `local_with_shards_bench.exs`: benchmark for the local adapter using
    `:shards` backend.

For example, to run the benchmark for the `:shards` backend:

```
$ MIX_ENV=test mix run benchmarks/local_with_shards_bench.exs
```

You can also run performance tests with `:basho_bench`. See the
[nebulex_bench example](https://github.com/elixir-nebulex/nebulex_examples/tree/master/nebulex_bench).

## Contributing

Use the [issue tracker](https://github.com/elixir-nebulex/nebulex_local/issues)
for bug reports and feature requests. Contributions are welcome through
[pull requests](https://github.com/elixir-nebulex/nebulex_local/pulls).

Include tests for new or changed behavior, and leave
[CHANGELOG.md](CHANGELOG.md) unchanged. Before submitting a pull request, run
`NEBULEX_PATH=nebulex mix precommit` and fix any failures.

## Copyright and License

Copyright (c) 2024 Carlos Andres Bolaños R.A.

`nebulex_local` source code is licensed under the [MIT License](LICENSE.md).
