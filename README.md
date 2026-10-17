# Nebulex Local
> A generational local cache adapter for [Nebulex][Nebulex].

[Nebulex]: https://github.com/cabol/nebulex

![CI](https://github.com/elixir-nebulex/nebulex_local/workflows/CI/badge.svg)
[![Codecov](https://codecov.io/gh/elixir-nebulex/nebulex_local/graph/badge.svg)](https://codecov.io/gh/elixir-nebulex/nebulex_local)
[![Hex Version](https://img.shields.io/hexpm/v/nebulex_local.svg)](https://hex.pm/packages/nebulex_local)
[![Documentation](https://img.shields.io/badge/Documentation-ff69b4)](https://hexdocs.pm/nebulex_local)

## About

This adapter provides a high-performance generational local cache for Nebulex,
inspired by [epocxy](https://github.com/duomark/epocxy). It uses a
generational garbage collection with two-generation approach (new and old)
for efficient memory management.

See the [module documentation][online_docs] for detailed information about
generational caching, concurrency handling, and configuration options.

## Installation

Add `:nebulex_local` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:nebulex_local, "~> 3.0"},
    {:telemetry, "~> 0.4 or ~> 1.0"}, # For observability/telemetry support
    {:decorator, "~> 1.4"},           # For declarative caching
    {:shards, "~> 1.1"},              # For high concurrency workloads with partitioning
    {:ex2ms, "~> 1.7"}                # For match specification query helpers
  ]
end
```

To provide more flexibility and load only the needed dependencies, this adapter
makes some dependencies optional:

  - **`:telemetry`** - Add when you want to emit and consume telemetry events
    for monitoring cache operations (recommended).
   - **`:decorator`** - Add when you want to enable declarative decorator-based
    caching (recommended).
  - **`:shards`** - Add when using `backend: :shards` for intensive workloads
    that benefit from partitioning across multiple ETS tables.
  - **`:ex2ms`** - Add when using match specification query helpers for advanced
    querying.

## Usage

You can define a cache as follows:

```elixir
defmodule MyApp.LocalCache do
  use Nebulex.Cache,
    otp_app: :my_app,
    adapter: Nebulex.Adapters.Local
end
```

Where the configuration for the cache must be in your application
environment, usually defined in your `config/config.exs`:

```elixir
config :my_app, MyApp.LocalCache,
  gc_interval: :timer.hours(12),
  max_size: 1_000_000,
  allocated_memory: 2_000_000_000,
  gc_memory_check_interval: :timer.seconds(10)
```

If your application was generated with a supervisor (by passing `--sup`
to `mix new`) you will have a `lib/my_app/application.ex` file containing
the application start callback that defines and starts your supervisor.
You just need to edit the `start/2` function to start the cache as a
supervisor on your application's supervisor:

```elixir
def start(_type, _args) do
  children = [
    {MyApp.LocalCache, []},
  ]

  ...
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

The adapter provides a set of basic benchmark tests using the library
[benchee](https://github.com/PragTob/benchee), and they are located within
the directory [benchmarks](./benchmarks).

To run a benchmark test you have to run:

```
$ MIX_ENV=test mix run benchmarks/BENCH_TEST_FILE
```

Where `BENCH_TEST_FILE` can be any of:

  * `local_with_ets_bench.exs`: benchmark for the local adapter using
    `:ets` backend.
  * `local_with_shards_bench.exs`: benchmark for the local adapter using
    `:shards` backend.

For example, for running the benchmark for the local adapter using `:shards`
backend:

```
$ MIX_ENV=test mix run benchmarks/local_with_shards_bench.exs
```

Additionally, you can also run performance tests using `:basho_bench`.
See [nebulex_bench example](https://github.com/elixir-nebulex/nebulex_examples/tree/master/nebulex_bench)
for more information.

## Contributing

Bug reports, feature requests, and pull requests are welcome. Use the
[issue tracker](https://github.com/elixir-nebulex/nebulex_local/issues) for
bug reports and feature requests, and open a
[pull request](https://github.com/elixir-nebulex/nebulex_local/pulls) when
you are ready to contribute.

Include tests for new or changed behavior, and leave
[CHANGELOG.md](CHANGELOG.md) unchanged. Before submitting a pull request, run
`NEBULEX_PATH=nebulex mix precommit` and fix any failures.

## Copyright and License

Copyright (c) 2024 Carlos Andres Bolaños R.A.

`nebulex_local` source code is licensed under the [MIT License](LICENSE.md).
