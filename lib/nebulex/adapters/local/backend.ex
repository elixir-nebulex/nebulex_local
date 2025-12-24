defmodule Nebulex.Adapters.Local.Backend do
  @moduledoc false

  alias Nebulex.Adapters.Local.Metadata

  @doc false
  defmacro __using__(_opts) do
    quote do
      alias Nebulex.Adapters.Local.Generation

      defp generation_spec(opts, extra \\ []) do
        opts = parse_opts(opts, extra)

        Supervisor.child_spec({Generation, opts}, id: Module.concat([__MODULE__, GC]))
      end

      defp locks_spec(opts) do
        meta_tab = Keyword.fetch!(opts, :adapter_meta).meta_tab
        {lock_opts, _opts} = Keyword.pop!(opts, :lock_opts)

        Supervisor.child_spec(
          {Nebulex.Locks,
           [init_callback: {unquote(__MODULE__), :init_callback, [meta_tab]}] ++ lock_opts},
          id: Module.concat([__MODULE__, Locks])
        )
      end

      defp sup_spec(children) do
        %{
          id: Module.concat([__MODULE__, Supervisor]),
          start: {Supervisor, :start_link, [children, [strategy: :one_for_all]]},
          type: :supervisor
        }
      end

      defp parse_opts(opts, extra \\ []) do
        type = Keyword.fetch!(opts, :backend_type)

        compressed =
          case Keyword.fetch!(opts, :compressed) do
            true -> [:compressed]
            false -> []
          end

        backend_opts =
          [
            :public,
            type,
            compressed,
            extra,
            keypos: 2,
            read_concurrency: Keyword.fetch!(opts, :read_concurrency),
            write_concurrency: Keyword.fetch!(opts, :write_concurrency)
          ]
          |> List.flatten()
          |> Enum.filter(&(&1 != :named_table))

        Keyword.put(opts, :backend_opts, backend_opts)
      end
    end
  end

  @doc """
  Helper function for returning the child spec for the given backend.
  """
  def child_spec(backend, opts) do
    get_mod(backend).child_spec(opts)
  end

  @doc """
  Helper function for creating a new table for the given backend.
  """
  def new(backend, meta_tab, tab_opts) do
    get_mod(backend).new(meta_tab, tab_opts)
  end

  @doc """
  Helper function for deleting a table for the given backend.
  """
  def delete(backend, meta_tab, gen_tab) do
    get_mod(backend).delete(meta_tab, gen_tab)
  end

  @doc """
  Helper function for initializing the locks table.
  """
  def init_callback(table, meta_tab) do
    Metadata.put(meta_tab, :locks_table, table)
  end

  ## Private functions

  defp get_mod(:ets), do: Nebulex.Adapters.Local.Backend.ETS

  if Code.ensure_loaded?(:shards) do
    defp get_mod(:shards), do: Nebulex.Adapters.Local.Backend.Shards
  end
end
