if Code.ensure_loaded?(Ex2ms) do
  defmodule Nebulex.Adapters.Local.QueryHelper do
    @moduledoc """
    Helper for building ETS match specifications without exposing the internal
    entry tuple structure.

    This module provides a user-friendly, SQL-like syntax for building
    match specs that hides the internal ETS entry record format.
    Instead of requiring users to know the exact tuple structure
    `{:entry, key, value, touched, exp, tag}`, they can use named
    field bindings with a declarative syntax.

    Under the hood, this module transforms the user-friendly syntax into the
    proper ETS tuple format and delegates to `Ex2ms` for match spec generation.

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

    ## Internal entry format

    The internal ETS entry is an Erlang record with the structure:
    `{:entry, key, value, touched, exp, tag}`

    This module abstracts away this structure so users don't need to know about
    it.
    """

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
      # Separate field bindings from where/select clauses
      {select, opts} = Keyword.pop(opts, :select, true)
      {where, field_bindings} = Keyword.pop(opts, :where)

      # Build the ETS tuple from field bindings
      entry_pattern = build_entry_tuple(field_bindings)

      # Build the clause for Ex2ms
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

      # Generate the Ex2ms fun (Ex2ms should be imported by the caller)
      quote do
        Ex2ms.fun do
          unquote(clause)
        end
      end
    end

    # Build the ETS tuple {:entry, key, value, touched, exp, tag} from user bindings
    defp build_entry_tuple(fields) do
      # Convert keyword list to map for easier lookup
      field_map = Map.new(fields)

      # Validate that only valid fields are provided
      valid_fields = [:key, :value, :touched, :exp, :tag]
      invalid_fields = Map.keys(field_map) -- valid_fields

      unless Enum.empty?(invalid_fields) do
        raise ArgumentError, """
        Invalid field(s): #{inspect(invalid_fields)}

        Valid fields are: #{inspect(valid_fields)}
        """
      end

      # Define the order of fields in the entry record
      # {:entry, key, value, touched, exp, tag}
      ordered_fields = [:key, :value, :touched, :exp, :tag]

      # Build the tuple elements
      elements = [
        :entry
        | Enum.map(ordered_fields, fn field ->
            Map.get(field_map, field, {:_, [], Elixir})
          end)
      ]

      # Return as a tuple AST node
      {:{}, [], elements}
    end
  end
end
