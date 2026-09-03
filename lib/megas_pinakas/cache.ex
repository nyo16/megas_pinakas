defmodule MegasPinakas.Cache do
  @moduledoc """
  Simple key-value cache backed by BigTable.

  Any Elixir term can be cached — maps, lists, strings, integers, `nil`. Each
  entry is stored as a single cell holding an Erlang-term envelope of the value
  and its optional expiry, so a value roundtrips exactly as it was written
  (atom keys stay atoms, integers stay integers).

  ## Expiry

  `:ttl` (seconds) is enforced **client-side**: `get/5`, `get_many/5`,
  `exists?/5` and `get_or_put/6` treat an entry whose expiry has passed as
  absent. Expired cells are not deleted by a read; they remain in the table until
  overwritten or garbage-collected. To reclaim storage, configure a
  `MegasPinakas.Admin.max_age_gc_rule/1` on the cache family that is at least as
  long as the longest TTL you use — the GC rule is storage reclamation only and
  is not what makes an entry expire.

  Because every entry is a self-describing term envelope, values written by
  `MegasPinakas.Types.write_json/8` or other raw writers are not readable through
  this module. For atomic numeric counters use `MegasPinakas.Counter`.

  ## Examples

      # Basic get/put
      {:ok, _} = MegasPinakas.Cache.put(project, instance, "cache", "user:123", %{name: "John"})
      {:ok, %{name: "John"}} = MegasPinakas.Cache.get(project, instance, "cache", "user:123")

      # Expire after five minutes
      {:ok, _} = MegasPinakas.Cache.put(project, instance, "cache", "session:abc", token, ttl: 300)

      # Get or compute
      {:ok, value} = MegasPinakas.Cache.get_or_put(project, instance, "cache", "user:123", fn ->
        expensive_computation()
      end)

      # Multi-key operations
      {:ok, _} = MegasPinakas.Cache.put_many(project, instance, "cache", [
        {"key1", value1},
        {"key2", value2}
      ])

      {:ok, results} = MegasPinakas.Cache.get_many(project, instance, "cache", ["key1", "key2"])
  """

  alias MegasPinakas
  alias MegasPinakas.Batch
  alias MegasPinakas.Filter
  alias MegasPinakas.Row
  alias MegasPinakas.Types

  @default_family "cache"
  @default_qualifier "value"

  # ============================================================================
  # Basic Operations
  # ============================================================================

  @doc """
  Gets a cached value by key.

  Returns `{:ok, nil}` when the key is absent **or** its TTL has elapsed.

  ## Options

    * `:family` - Column family (default: "cache")
    * `:qualifier` - Column qualifier (default: "value")
    * `:app_profile_id` - App profile to use for the request

  ## Examples

      {:ok, value} = MegasPinakas.Cache.get(project, instance, "cache", "user:123")
  """
  @spec get(String.t(), String.t(), String.t(), String.t(), keyword()) ::
          {:ok, term() | nil} | {:error, term()}
  def get(project, instance, table, key, opts \\ []) do
    case lookup(project, instance, table, key, opts) do
      {:hit, value} -> {:ok, value}
      :miss -> {:ok, nil}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Stores a value in the cache.

  Any term is accepted, including `nil`. A `nil` value is a real entry: `get/5`
  returns `{:ok, nil}` for it, exactly as for a missing key, but `exists?/5`
  returns `{:ok, true}`.

  ## Options

    * `:ttl` - Seconds until the entry expires (positive integer). Omit for no
      expiry. Expiry is evaluated at whole-second granularity, so the effective
      lifetime is within `(ttl - 1, ttl]` seconds.
    * `:family` - Column family (default: "cache")
    * `:qualifier` - Column qualifier (default: "value")
    * `:app_profile_id` - App profile to use for the request

  ## Examples

      {:ok, _} = MegasPinakas.Cache.put(project, instance, "cache", "user:123", %{name: "John"})
      {:ok, _} = MegasPinakas.Cache.put(project, instance, "cache", "otp:123", 493_201, ttl: 60)
  """
  @spec put(String.t(), String.t(), String.t(), String.t(), term(), keyword()) ::
          {:ok, term()} | {:error, term()}
  def put(project, instance, table, key, value, opts \\ []) do
    {family, qualifier} = column(opts)
    entry = envelope(value, opts)

    Types.write_term(project, instance, table, key, family, qualifier, entry, opts)
  end

  @doc """
  Deletes a cached value.

  ## Examples

      {:ok, _} = MegasPinakas.Cache.delete(project, instance, "cache", "user:123")
  """
  @spec delete(String.t(), String.t(), String.t(), String.t(), keyword()) ::
          {:ok, term()} | {:error, term()}
  def delete(project, instance, table, key, opts \\ []) do
    {family, qualifier} = column(opts)

    mutations = [MegasPinakas.delete_from_column(family, qualifier)]
    MegasPinakas.mutate_row(project, instance, table, key, mutations, opts)
  end

  @doc """
  Gets a value, computing and storing it if not present or expired.

  A stored `nil` is a hit: `default_fn` is not called and `{:ok, nil}` is
  returned, so negative results can be cached.

  Accepts the same options as `put/6`, so `:ttl` applies to the value stored on
  a miss.

  ## Examples

      {:ok, value} = MegasPinakas.Cache.get_or_put(project, instance, "cache", "key", fn ->
        expensive_computation()
      end, ttl: 600)
  """
  @spec get_or_put(String.t(), String.t(), String.t(), String.t(), (-> term()), keyword()) ::
          {:ok, term()} | {:error, term()}
  def get_or_put(project, instance, table, key, default_fn, opts \\ [])
      when is_function(default_fn, 0) do
    case lookup(project, instance, table, key, opts) do
      {:hit, value} ->
        {:ok, value}

      :miss ->
        value = default_fn.()

        case put(project, instance, table, key, value, opts) do
          {:ok, _} -> {:ok, value}
          {:error, reason} -> {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  # ============================================================================
  # Multi-Key Operations
  # ============================================================================

  @doc """
  Gets multiple values by keys.

  Returns a map of key => value pairs. Keys that are absent or expired map to
  `nil`.

  ## Options

    * `:family` - Column family (default: "cache")
    * `:qualifier` - Column qualifier (default: "value")
    * `:app_profile_id` - App profile to use for the request

  ## Examples

      {:ok, results} = MegasPinakas.Cache.get_many(project, instance, "cache", ["key1", "key2", "key3"])
      # => {:ok, %{"key1" => value1, "key2" => nil, "key3" => value3}}
  """
  @spec get_many(String.t(), String.t(), String.t(), [String.t()], keyword()) ::
          {:ok, map()} | {:error, term()}
  def get_many(project, instance, table, keys, opts \\ []) when is_list(keys) do
    {family, qualifier} = column(opts)

    read_opts =
      family
      |> read_opts(qualifier, opts)
      |> Keyword.put(:rows, MegasPinakas.row_set(keys))

    with {:ok, rows} <- MegasPinakas.read_rows(project, instance, table, read_opts),
         {:ok, found} <- decode_rows(rows, family, qualifier) do
      {:ok, Map.new(keys, fn key -> {key, Map.get(found, key)} end)}
    end
  end

  @doc """
  Stores multiple values.

  Accepts the same options as `put/6`; `:ttl` applies to every entry.

  ## Examples

      {:ok, _} = MegasPinakas.Cache.put_many(project, instance, "cache", [
        {"key1", %{a: 1}},
        {"key2", %{b: 2}}
      ])
  """
  @spec put_many(String.t(), String.t(), String.t(), [{String.t(), term()}], keyword()) ::
          {:ok, term()} | {:error, term()}
  def put_many(project, instance, table, entries, opts \\ []) when is_list(entries) do
    {family, qualifier} = column(opts)

    batch =
      Enum.reduce(entries, Batch.new(), fn {key, value}, batch ->
        row = Row.put_term(Row.new(key), family, qualifier, envelope(value, opts))
        Batch.add(batch, row)
      end)

    Batch.write(batch, project, instance, table, opts)
  end

  @doc """
  Deletes multiple cached values.

  Returns one result per key, ordered to match `keys`. `{:ok, results}` means the
  RPC succeeded, **not** that every key was deleted — check `&1.status.code == 0`
  per entry.

  ## Examples

      {:ok, results} = MegasPinakas.Cache.delete_many(project, instance, "cache", ["key1", "key2"])

  > #### Breaking change in 0.6.0 {: .warning}
  >
  > Previously returned an unconsumed `#Stream<>`, so failed deletes were silently
  > discarded unless the caller enumerated it.
  """
  @spec delete_many(String.t(), String.t(), String.t(), [String.t()], keyword()) ::
          {:ok, [Google.Bigtable.V2.MutateRowsResponse.Entry.t()]} | {:error, term()}
  def delete_many(project, instance, table, keys, opts \\ []) when is_list(keys) do
    {family, qualifier} = column(opts)

    entries =
      Enum.map(keys, fn key ->
        %{
          row_key: key,
          mutations: [MegasPinakas.delete_from_column(family, qualifier)]
        }
      end)

    MegasPinakas.mutate_rows(project, instance, table, entries, opts)
  end

  # ============================================================================
  # Existence Checks
  # ============================================================================

  @doc """
  Checks whether a key holds an unexpired entry.

  Unlike `get/5`, a stored `nil` counts as present. Transport and decode errors
  are returned, not folded into `false`.

  ## Examples

      {:ok, true} = MegasPinakas.Cache.exists?(project, instance, "cache", "key")
  """
  @spec exists?(String.t(), String.t(), String.t(), String.t(), keyword()) ::
          {:ok, boolean()} | {:error, term()}
  def exists?(project, instance, table, key, opts \\ []) do
    case lookup(project, instance, table, key, opts) do
      {:hit, _value} -> {:ok, true}
      :miss -> {:ok, false}
      {:error, reason} -> {:error, reason}
    end
  end

  # ============================================================================
  # Private Helpers
  # ============================================================================

  defp column(opts) do
    {Keyword.get(opts, :family, @default_family),
     Keyword.get(opts, :qualifier, @default_qualifier)}
  end

  # Only the cache column, latest version only: RMW-free writes still accumulate
  # versions until GC runs, and we never want to transfer stale ones.
  defp read_opts(family, qualifier, opts) do
    filter =
      Filter.chain_filters([
        Filter.column_filter(family, qualifier),
        Filter.cells_per_column_limit_filter(1)
      ])

    opts |> Keyword.take([:app_profile_id]) |> Keyword.put(:filter, filter)
  end

  defp envelope(value, opts) do
    exp =
      case Keyword.get(opts, :ttl) do
        nil ->
          nil

        ttl when is_integer(ttl) and ttl > 0 ->
          now() + ttl

        other ->
          raise ArgumentError,
                ":ttl must be a positive integer number of seconds, got: #{inspect(other)}"
      end

    %{v: value, exp: exp}
  end

  defp now, do: System.system_time(:second)

  defp decode_rows(rows, family, qualifier) do
    now = now()

    Enum.reduce_while(rows, {:ok, %{}}, fn row, {:ok, acc} ->
      case decode_entry(MegasPinakas.get_cell(row, family, qualifier), now) do
        {:ok, value} -> {:cont, {:ok, Map.put(acc, MegasPinakas.row_key(row), value)}}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  # `{:hit, value}` for a live entry (including a stored nil), `:miss` for an
  # absent or expired one. `get/5` collapses both nil cases; `get_or_put/6`
  # must not.
  defp lookup(project, instance, table, key, opts) do
    {family, qualifier} = column(opts)

    case MegasPinakas.read_row(project, instance, table, key, read_opts(family, qualifier, opts)) do
      {:ok, row} -> classify(MegasPinakas.get_cell(row, family, qualifier), now())
      {:error, reason} -> {:error, reason}
    end
  end

  defp classify(nil, _now), do: :miss

  defp classify(raw, now) do
    with {:ok, entry} <- unwrap(raw) do
      if expired?(entry, now), do: :miss, else: {:hit, entry.v}
    end
  end

  defp decode_entry(raw, now) do
    case classify(raw, now) do
      {:hit, value} -> {:ok, value}
      :miss -> {:ok, nil}
      {:error, reason} -> {:error, reason}
    end
  end

  defp unwrap(raw) do
    case Types.decode(:term, raw) do
      {:ok, %{v: _, exp: exp} = entry} when is_nil(exp) or is_integer(exp) -> {:ok, entry}
      {:ok, _other} -> {:error, :invalid_cache_entry}
      {:error, reason} -> {:error, reason}
    end
  end

  defp expired?(%{exp: nil}, _now), do: false
  defp expired?(%{exp: exp}, now), do: exp <= now
end
