defmodule Nebulex.Locks.Options do
  @moduledoc false

  # Start options
  start_opts_defs = [
    name: [
      type: :atom,
      required: false,
      doc: """
      Optional name to register both the GenServer and ETS table.
      When provided, allows direct access to the locks table using the name.
      When `nil` or not provided, the GenServer starts unnamed.
      """
    ],
    cleanup_interval: [
      type: :timeout,
      required: false,
      default: :timer.minutes(5),
      doc: """
      The interval in milliseconds for the periodic cleanup of stale locks.
      Stale locks are detected by checking if the owner process is alive and
      if the lock has exceeded the `:lock_timeout`.
      """
    ],
    cleanup_batch_size: [
      type: :pos_integer,
      required: false,
      default: 100,
      doc: """
      The number of locks to process per batch during periodic cleanup.
      Larger values reduce iteration overhead but increase memory usage
      during cleanup. Smaller values reduce memory spikes but increase
      the number of ETS select operations.
      """
    ],
    init_callback: [
      type: :mfa,
      required: false,
      doc: """
      An optional callback MFA (module, function, arguments) to be invoked
      after the locks table is created during GenServer initialization.

      The callback is invoked as `apply(module, function, [table | args])`,
      where `table` is the newly created ETS locks table prepended to the
      provided argument list. The return value is ignored.

      This is useful for integration scenarios where the locks table reference
      needs to be stored in external state, such as an adapter's metadata table.

      ## Example

          # Store the locks table in the adapter's metadata
          {Nebulex.Locks, [
            name: MyApp.Locks,
            init_callback: {MyApp, :init, [:some_arg]}
          ]}

          # Will call: MyApp.init(table, :some_arg)

      """
    ]
  ]

  # Lock acquisition options
  opts_defs = [
    keys: [
      type: {:list, :any},
      required: false,
      default: [],
      doc: """
      The list of keys to lock. If empty or not provided, a global lock
      is used. For better concurrency, always specify the keys involved
      in the transaction to enable fine-grained locking.
      """
    ],
    retries: [
      type: {:or, [:non_neg_integer, {:in, [:infinity]}]},
      type_doc: "`:infinity` | `t:non_neg_integer/0`",
      required: false,
      default: 3,
      doc: """
      The number of times to retry acquiring locks before giving up.
      If set to `:infinity`, the process will retry indefinitely until
      locks are acquired.
      """
    ],
    retry_interval: [
      type: {:or, [:non_neg_integer, {:fun, 1}]},
      type_doc: "`t:non_neg_integer/0` | `(attempt :: non_neg_integer() -> non_neg_integer())`",
      required: false,
      default: 10,
      doc: """
      The time in milliseconds to wait between lock acquisition retries.

      Can be either:

        * A timeout value (`non_neg_integer()`) - A fixed interval. When using
          a non-negative integer, a small random jitter is added to prevent
          thundering herd issues.
        * An anonymous function - Receives the current attempt number
          (0-indexed) and must return a non-negative integer representing the
          interval in milliseconds. No jitter is added, giving you full control
          over the retry strategy. Returning `0` results in immediate retry with
          no delay.

      **Examples:**

          # Fixed interval with automatic jitter
          retry_interval: 10

          # Exponential backoff
          retry_interval: fn attempt ->
            round(min(100 * :math.pow(2, attempt), 5000))
          end

          # Linear backoff
          retry_interval: fn attempt -> 10 + (attempt * 50) end

          # Immediate retry on first attempt, then backoff
          retry_interval: fn
            0 -> 0
            attempt -> attempt * 100
          end

          # Custom strategy with manual jitter
          retry_interval: fn attempt ->
            base = 100 * attempt
            jitter = :rand.uniform(max(base, 1))
            base + jitter
          end

      """
    ],
    lock_timeout: [
      type: :timeout,
      required: false,
      default: :timer.seconds(30),
      doc: """
      The maximum time in milliseconds a lock can be held before being
      considered stale. Stale locks (from crashed processes or timeouts)
      are automatically cleaned up during acquisition attempts.
      """
    ]
  ]

  # Start options schema
  @start_opts_schema NimbleOptions.new!(start_opts_defs)

  # Start options
  @start_opts Keyword.keys(@start_opts_schema.schema)

  # Lock acquisition options schema
  @opts_schema NimbleOptions.new!(opts_defs)

  # Lock acquisition options
  @opts Keyword.keys(@opts_schema.schema)

  ## Docs API

  # coveralls-ignore-start

  @spec start_options_docs() :: binary()
  def start_options_docs do
    NimbleOptions.docs(@start_opts_schema)
  end

  @spec options_docs() :: binary()
  def options_docs do
    NimbleOptions.docs(@opts_schema)
  end

  # coveralls-ignore-stop

  ## Validation API

  @spec validate_start_opts!(keyword()) :: keyword()
  def validate_start_opts!(opts) do
    opts
    |> Keyword.take(@start_opts)
    |> NimbleOptions.validate!(@start_opts_schema)
  end

  @spec validate!(keyword()) :: keyword()
  def validate!(opts) do
    opts
    |> Keyword.take(@opts)
    |> NimbleOptions.validate!(@opts_schema)
  end
end
