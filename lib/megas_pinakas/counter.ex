defmodule MegasPinakas.Counter do
  @moduledoc """
  Atomic counter operations using BigTable's read-modify-write.

  Provides high-level operations for counters that need atomic increments/decrements.
  Uses BigTable's `read_modify_write_row` for atomicity guarantees.

  ## Column family configuration

  Every increment writes a new cell version; without garbage collection those
  versions accumulate indefinitely. Configure the counter family with
  `MegasPinakas.Admin.max_versions_gc_rule(1)`. Reads in this module only ever
  fetch the latest version, so the extra versions cost storage, not correctness.

  ## Examples

      # Increment a page view counter
      {:ok, new_value} = MegasPinakas.Counter.increment(
        project, instance, "counters", "page#homepage", "stats", "views"
      )

      # Decrement stock count
      {:ok, new_value} = MegasPinakas.Counter.decrement(
        project, instance, "inventory", "product#123", "stock", "available", 5
      )

      # Get current counter value
      {:ok, value} = MegasPinakas.Counter.get(
        project, instance, "counters", "page#homepage", "stats", "views"
      )

      # Increment multiple counters atomically
      {:ok, results} = MegasPinakas.Counter.increment_many(
        project, instance, "analytics", "user#123", [
          {"stats", "page_views", 1},
          {"stats", "clicks", 3}
        ]
      )
  """

  alias MegasPinakas
  alias MegasPinakas.Filter
  alias MegasPinakas.Types

  # Compare-and-swap attempts before `increment_if_exists/8` reports contention.
  @cas_attempts 5

  # ============================================================================
  # Basic Counter Operations
  # ============================================================================

  @doc """
  Atomically increments a counter and returns the new value.

  ## Options

    * `:app_profile_id` - App profile to use for the request

  ## Examples

      {:ok, new_value} = MegasPinakas.Counter.increment(
        project, instance, "counters", "row1", "cf", "views"
      )

      {:ok, new_value} = MegasPinakas.Counter.increment(
        project, instance, "counters", "row1", "cf", "views", 5
      )
  """
  @spec increment(
          String.t(),
          String.t(),
          String.t(),
          binary(),
          String.t(),
          String.t(),
          integer(),
          keyword()
        ) ::
          {:ok, integer()} | {:error, term()}
  def increment(project, instance, table, row_key, family, qualifier, amount \\ 1, opts \\ [])
      when is_integer(amount) do
    rules = [MegasPinakas.increment_rule(family, qualifier, amount)]

    case MegasPinakas.read_modify_write_row(project, instance, table, row_key, rules, opts) do
      {:ok, response} ->
        extract_counter_value(response, family, qualifier)

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Atomically decrements a counter and returns the new value.

  This is equivalent to incrementing by a negative amount.

  ## Examples

      {:ok, new_value} = MegasPinakas.Counter.decrement(
        project, instance, "counters", "row1", "cf", "stock"
      )

      {:ok, new_value} = MegasPinakas.Counter.decrement(
        project, instance, "counters", "row1", "cf", "stock", 5
      )
  """
  @spec decrement(
          String.t(),
          String.t(),
          String.t(),
          binary(),
          String.t(),
          String.t(),
          integer(),
          keyword()
        ) ::
          {:ok, integer()} | {:error, term()}
  def decrement(project, instance, table, row_key, family, qualifier, amount \\ 1, opts \\ [])
      when is_integer(amount) do
    increment(project, instance, table, row_key, family, qualifier, -amount, opts)
  end

  @doc """
  Gets the current value of a counter.

  Returns `{:ok, nil}` if the counter doesn't exist. Only the latest cell
  version is fetched; a caller-supplied `:filter` is chained in front of that
  restriction.

  ## Options

    * `:filter` - Additional `RowFilter` applied before the latest-version restriction
    * `:app_profile_id` - App profile to use for the request

  ## Examples

      {:ok, value} = MegasPinakas.Counter.get(
        project, instance, "counters", "row1", "cf", "views"
      )
  """
  @spec get(String.t(), String.t(), String.t(), binary(), String.t(), String.t(), keyword()) ::
          {:ok, integer() | nil} | {:error, term()}
  def get(project, instance, table, row_key, family, qualifier, opts \\ []) do
    read_opts = Keyword.put(opts, :filter, latest_cell_filter(family, qualifier, opts))
    Types.read_integer(project, instance, table, row_key, family, qualifier, read_opts)
  end

  @doc """
  Sets a counter to a specific value (non-atomic - use with caution).

  This overwrites any existing value. For atomic operations, use `increment/8`.

  ## Examples

      {:ok, _} = MegasPinakas.Counter.set(
        project, instance, "counters", "row1", "cf", "views", 100
      )
  """
  @spec set(
          String.t(),
          String.t(),
          String.t(),
          binary(),
          String.t(),
          String.t(),
          integer(),
          keyword()
        ) ::
          {:ok, term()} | {:error, term()}
  def set(project, instance, table, row_key, family, qualifier, value, opts \\ [])
      when is_integer(value) do
    Types.write_integer(project, instance, table, row_key, family, qualifier, value, opts)
  end

  @doc """
  Resets a counter to zero.

  ## Examples

      {:ok, _} = MegasPinakas.Counter.reset(
        project, instance, "counters", "row1", "cf", "views"
      )
  """
  @spec reset(String.t(), String.t(), String.t(), binary(), String.t(), String.t(), keyword()) ::
          {:ok, term()} | {:error, term()}
  def reset(project, instance, table, row_key, family, qualifier, opts \\ []) do
    set(project, instance, table, row_key, family, qualifier, 0, opts)
  end

  # ============================================================================
  # Multi-Counter Operations
  # ============================================================================

  @doc """
  Atomically increments multiple counters in the same row.

  All increments are applied atomically. Returns a map of family:qualifier to new values.

  ## Examples

      {:ok, results} = MegasPinakas.Counter.increment_many(
        project, instance, "analytics", "user#123", [
          {"stats", "page_views", 1},
          {"stats", "clicks", 3},
          {"engagement", "time_spent", 60}
        ]
      )
      # => {:ok, %{"stats:page_views" => 42, "stats:clicks" => 15, "engagement:time_spent" => 3600}}
  """
  @spec increment_many(
          String.t(),
          String.t(),
          String.t(),
          binary(),
          [{String.t(), String.t(), integer()}],
          keyword()
        ) ::
          {:ok, map()} | {:error, term()}
  def increment_many(project, instance, table, row_key, counters, opts \\ [])
      when is_list(counters) do
    rules =
      Enum.map(counters, fn {family, qualifier, amount} ->
        MegasPinakas.increment_rule(family, qualifier, amount)
      end)

    case MegasPinakas.read_modify_write_row(project, instance, table, row_key, rules, opts) do
      {:ok, response} ->
        extract_all_counter_values(response, counters)

      {:error, reason} ->
        {:error, reason}
    end
  end

  # ============================================================================
  # Conditional Counter
  # ============================================================================

  @doc """
  Increments a counter only if it already exists, via compare-and-swap.

  Reads the counter's current value, then issues a `check_and_mutate_row` whose
  predicate matches only if the latest cell still holds exactly those bytes; the
  true-branch writes `current + amount`. A concurrent writer makes the predicate
  miss, in which case the read/CAS cycle is retried up to #{@cas_attempts} times.

  Returns:

    * `{:ok, :applied}` - the counter existed and was incremented
    * `{:ok, :not_applied}` - the counter does not exist; nothing was written
    * `{:error, :contention}` - every attempt lost a race with another writer

  Unlike `increment/8`, this never creates the counter and does not return the
  new value; call `get/7` if you need it.

  ## Options

    * `:app_profile_id` - App profile to use for the request

  ## Examples

      {:ok, :applied} = MegasPinakas.Counter.increment_if_exists(
        project, instance, "counters", "row1", "cf", "views", 1
      )
  """
  @spec increment_if_exists(
          String.t(),
          String.t(),
          String.t(),
          binary(),
          String.t(),
          String.t(),
          integer(),
          keyword()
        ) ::
          {:ok, :applied | :not_applied} | {:error, term()}
  def increment_if_exists(
        project,
        instance,
        table,
        row_key,
        family,
        qualifier,
        amount \\ 1,
        opts \\ []
      )
      when is_integer(amount) do
    target = %{
      project: project,
      instance: instance,
      table: table,
      row_key: row_key,
      family: family,
      qualifier: qualifier,
      opts: opts
    }

    cas_increment(target, amount, @cas_attempts)
  end

  defp cas_increment(_target, _amount, 0), do: {:error, :contention}

  defp cas_increment(target, amount, attempts) do
    %{project: project, instance: instance, table: table, row_key: row_key} = target
    %{family: family, qualifier: qualifier, opts: opts} = target
    read_opts = Keyword.put(opts, :filter, latest_cell_filter(family, qualifier, []))

    with {:ok, row} <- MegasPinakas.read_row(project, instance, table, row_key, read_opts),
         old_bytes when is_binary(old_bytes) <- MegasPinakas.get_cell(row, family, qualifier),
         {:ok, old} <- Types.decode(:integer, old_bytes) do
      case cas_swap(target, old_bytes, old + amount) do
        {:ok, %{predicate_matched: true}} -> {:ok, :applied}
        {:ok, %{predicate_matched: false}} -> cas_increment(target, amount, attempts - 1)
        {:error, reason} -> {:error, reason}
      end
    else
      nil -> {:ok, :not_applied}
      {:error, reason} -> {:error, reason}
    end
  end

  # Restrict to the latest version before comparing bytes: an un-GC'd older
  # version holding the same value must not satisfy the predicate.
  defp cas_swap(target, old_bytes, new_value) do
    %{family: family, qualifier: qualifier} = target

    predicate =
      Filter.chain_filters([
        latest_cell_filter(family, qualifier, []),
        Filter.value_range_filter(start_value_closed: old_bytes, end_value_closed: old_bytes)
      ])

    MegasPinakas.check_and_mutate_row(
      target.project,
      target.instance,
      target.table,
      target.row_key,
      predicate,
      [Types.set_integer(family, qualifier, new_value)],
      [],
      target.opts
    )
  end

  # ============================================================================
  # Helpers for Building Counter Rows
  # ============================================================================

  @doc """
  Creates a counter mutation for use with Row builder.

  ## Examples

      row = MegasPinakas.Row.new("counters#123")
            |> MegasPinakas.Counter.add_counter("cf", "views", 0)
            |> MegasPinakas.Counter.add_counter("cf", "clicks", 0)
            |> MegasPinakas.Row.write(project, instance, "counters")
  """
  @spec add_counter(MegasPinakas.Row.t(), String.t(), String.t(), integer()) ::
          MegasPinakas.Row.t()
  def add_counter(row, family, qualifier, initial_value \\ 0) when is_integer(initial_value) do
    MegasPinakas.Row.put_integer(row, family, qualifier, initial_value)
  end

  @doc """
  Creates an increment rule for use with read_modify_write_row.

  ## Examples

      rule = MegasPinakas.Counter.increment_rule("cf", "views", 1)
  """
  @spec increment_rule(String.t(), String.t(), integer()) ::
          %Google.Bigtable.V2.ReadModifyWriteRule{}
  def increment_rule(family, qualifier, amount) when is_integer(amount) do
    MegasPinakas.increment_rule(family, qualifier, amount)
  end

  # Shared with `MegasPinakas.CounterTTL`, which decodes counters out of the same
  # read-modify-write response shape. Public only so CounterTTL can reach it.
  @doc false
  @spec extract_counter_value(map(), String.t(), String.t()) ::
          {:ok, integer() | nil} | {:error, term()}
  def extract_counter_value(response, family, qualifier) do
    case response.row do
      nil ->
        {:ok, nil}

      row ->
        value = MegasPinakas.get_cell(row, family, qualifier)

        if value do
          Types.decode(:integer, value)
        else
          {:ok, nil}
        end
    end
  end

  # ============================================================================
  # Private Helpers
  # ============================================================================

  # The counter column, latest version only. A caller-provided `:filter` runs
  # first so it can only narrow the result, never widen it to older versions.
  defp latest_cell_filter(family, qualifier, opts) do
    base = [Filter.column_filter(family, qualifier), Filter.cells_per_column_limit_filter(1)]

    case Keyword.get(opts, :filter) do
      nil -> Filter.chain_filters(base)
      extra -> Filter.chain_filters([extra | base])
    end
  end

  defp extract_all_counter_values(response, counters) do
    case response.row do
      nil ->
        {:ok, %{}}

      row ->
        results =
          Enum.reduce(counters, %{}, fn {family, qualifier, _amount}, acc ->
            key = "#{family}:#{qualifier}"
            value = MegasPinakas.get_cell(row, family, qualifier)
            Map.put(acc, key, decode_integer_value(value))
          end)

        {:ok, results}
    end
  end

  defp decode_integer_value(nil), do: nil

  defp decode_integer_value(value) do
    case Types.decode(:integer, value) do
      {:ok, v} -> v
      {:error, _} -> nil
    end
  end
end
