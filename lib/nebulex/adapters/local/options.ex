defmodule Nebulex.Adapters.Local.Options do
  @moduledoc """
  Option definitions for the local adapter.
  """

  # Adapter option definitions
  adapter_opts = [
    cache: [
      type: :atom,
      required: true,
      doc: """
      The defined cache module.
      """
    ],
    stats: [
      type: :boolean,
      required: false,
      default: true,
      doc: """
      A flag to determine whether to collect cache stats.
      """
    ],
    backend: [
      type: {:in, [:ets, :shards]},
      type_doc: "`t:backend/0`",
      required: false,
      default: :ets,
      doc: """
      The backend or storage to be used for the adapter.
      """
    ],
    read_concurrency: [
      type: :boolean,
      required: false,
      default: true,
      doc: """
      Since the adapter uses ETS tables internally, this option is when creating
      a new table or generation. See `:ets.new/2` options.
      """
    ],
    write_concurrency: [
      type: :boolean,
      required: false,
      default: true,
      doc: """
      Since the adapter uses ETS tables internally, this option is when creating
      a new table or generation. See `:ets.new/2` options.
      """
    ],
    compressed: [
      type: :boolean,
      required: false,
      default: false,
      doc: """
      Since the adapter uses ETS tables internally, this option is when creating
      a new table or generation. See `:ets.new/2` options.
      """
    ],
    backend_type: [
      type: {:in, [:set, :ordered_set, :bag, :duplicate_bag]},
      required: false,
      default: :set,
      doc: """
      Since the adapter uses ETS tables internally, this option is when creating
      a new table or generation. See `:ets.new/2` options.
      """
    ],
    partitions: [
      type: :pos_integer,
      required: false,
      doc: """
      The number of ETS partitions when using the `:shards` backend.
      See `:shards.new/2`.

      The default value is `System.schedulers_online()`.
      """
    ],
    purge_chunk_size: [
      type: :pos_integer,
      required: false,
      default: 100,
      doc: """
      This option limits the max nested match specs based on the number of keys
      when purging the older cache generation.
      """
    ],
    gc_interval: [
      type: :pos_integer,
      required: false,
      doc: """
      The interval time in milliseconds for garbage collection to run, create
      a new generation, make it the newer one, make the previous new generation
      the old one, and finally remove the previous old one. If not provided
      (or `nil`), the garbage collection never runs, so new generations must be
      created explicitly, e.g., `MyCache.new_generation(opts)` (the default);
      however, the adapter does not recommend this.

      > #### Usage {: .warning}
      >
      > Always provide the `:gc_interval` option so the garbage collector can
      > work appropriately out of the box. Unless you explicitly want to turn
      > off the garbage collection or handle it yourself.
      """
    ],
    max_size: [
      type: :pos_integer,
      required: false,
      doc: """
      The maximum number of entries to store in the cache. If not provided
      (or `nil`), the health check to validate and release memory is not
      performed (the default).
      """
    ],
    allocated_memory: [
      type: :pos_integer,
      required: false,
      doc: """
      The maximum size in bytes for the cache storage. If not provided
      (or `nil`), the health check to validate and release memory is not
      performed (the default).
      """
    ],
    gc_memory_check_interval: [
      type: {:or, [:pos_integer, {:fun, 3}]},
      type_doc: "`t:mem_check_interval/0`",
      required: false,
      default: :timer.seconds(10),
      doc: """
      The interval time in milliseconds for garbage collection to run the size
      and memory checks.

      > #### Usage {: .warning}
      >
      > Beware: For the `:gc_memory_check_interval` option to work, you must
      > configure one of `:max_size` or `:allocated_memory` (or both).
      """
    ],
    gc_cleanup_delay: [
      type: :pos_integer,
      required: false,
      default: :timer.seconds(10),
      doc: """
      The delay in milliseconds before the oldest generation is deleted.
      This grace period allows ongoing operations to complete before the
      generation is removed.
      """
    ],
    lock_opts: [
      type: :keyword_list,
      required: false,
      default: [],
      doc: """
      Options to customize the locks manager used for cache transactions.

      The local adapter uses `Nebulex.Locks` for implementing transactions with
      an ETS-based locking mechanism optimized for single-node scenarios. This
      option allows you to customize the behavior of the locks manager, such as
      adjusting the cleanup interval or batch size for stale lock cleanup.

      **Available Options:**

        * `:cleanup_interval` - The interval in milliseconds for periodic
          cleanup of stale locks (defaults to 5 minutes).
        * `:cleanup_batch_size` - The number of locks to process per batch
          during cleanup (defaults to 100).

      Note: The `:name` and `:init_callback` options are managed internally by
      the adapter and should not be provided. See the "Start Options" section
      in `Nebulex.Locks` for all available options.

      **Examples:**

          # Use default lock options (recommended for most cases)
          MyCache.start_link()

          # Customize cleanup for high-throughput scenarios
          MyCache.start_link(
            lock_opts: [
              cleanup_interval: :timer.minutes(1),
              cleanup_batch_size: 500
            ]
          )

          # More frequent cleanup for memory-constrained environments
          MyCache.start_link(
            lock_opts: [
              cleanup_interval: :timer.seconds(30)
            ]
          )

      """
    ]
  ]

  # GC runtime option definitions
  gc_runtime_opts = [
    gc_interval_reset: [
      type: :boolean,
      required: false,
      default: true,
      doc: """
      Whether the `:gc_interval` should be reset or not.
      """
    ]
  ]

  # Adapter options schema
  @adapter_opts_schema NimbleOptions.new!(adapter_opts)

  # GC runtime options schema
  @gc_runtime_opts_schema NimbleOptions.new!(gc_runtime_opts)

  # Nebulex common option keys
  @nbx_start_opts Nebulex.Cache.Options.__compile_opts__() ++ Nebulex.Cache.Options.__start_opts__()

  ## Docs API

  # coveralls-ignore-start

  @spec adapter_options_docs() :: binary()
  def adapter_options_docs do
    NimbleOptions.docs(@adapter_opts_schema)
  end

  @spec gc_runtime_options_docs() :: binary()
  def gc_runtime_options_docs do
    NimbleOptions.docs(@gc_runtime_opts_schema)
  end

  # coveralls-ignore-stop

  ## Validation API

  @spec validate_adapter_opts!(keyword()) :: keyword()
  def validate_adapter_opts!(opts) do
    adapter_opts =
      opts
      |> Keyword.drop(@nbx_start_opts)
      |> NimbleOptions.validate!(@adapter_opts_schema)

    Keyword.merge(opts, adapter_opts)
  end

  @spec validate_gc_runtime_opts!(keyword()) :: keyword()
  def validate_gc_runtime_opts!(opts) do
    NimbleOptions.validate!(opts, @gc_runtime_opts_schema)
  end
end
