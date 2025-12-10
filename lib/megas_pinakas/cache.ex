defmodule MegasPinakas.Cache do
  @moduledoc """
  Simple key-value cache backed by BigTable.

  Provides basic cache operations with optional TTL support via GC rules.
  Values are automatically serialized as JSON for complex types.

  ## Examples

      # Basic get/put
      {:ok, _} = MegasPinakas.Cache.put(project, instance, "cache", "user:123", %{name: "John"})
      {:ok, data} = MegasPinakas.Cache.get(project, instance, "cache", "user:123")

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
  alias MegasPinakas.Types
  alias MegasPinakas.Row
  alias MegasPinakas.Batch

  @default_family "cache"
  @default_qualifier "value"

  # ============================================================================
  # Basic Operations
  # ============================================================================

  @doc """
  Gets a cached value by key.

  ## Options

    * `:family` - Column family (default: "cache")
    * `:qualifier` - Column qualifier (default: "value")

  ## Examples

      {:ok, value} = MegasPinakas.Cache.get(project, instance, "cache", "user:123")
  """
  @spec get(String.t(), String.t(), String.t(), String.t(), keyword()) ::
          {:ok, term() | nil} | {:error, term()}
  def get(project, instance, table, key, opts \\ []) do
    family = Keyword.get(opts, :family, @default_family)
    qualifier = Keyword.get(opts, :qualifier, @default_qualifier)

    Types.read_json(project, instance, table, key, family, qualifier, opts)
  end

  @doc """
  Stores a value in the cache.

  ## Options

    * `:family` - Column family (default: "cache")
    * `:qualifier` - Column qualifier (default: "value")

  ## Examples

      {:ok, _} = MegasPinakas.Cache.put(project, instance, "cache", "user:123", %{name: "John"})
  """
  @spec put(String.t(), String.t(), String.t(), String.t(), term(), keyword()) ::
          {:ok, term()} | {:error, term()}
  def put(project, instance, table, key, value, opts \\ []) do
    family = Keyword.get(opts, :family, @default_family)
    qualifier = Keyword.get(opts, :qualifier, @default_qualifier)

    Types.write_json(project, instance, table, key, family, qualifier, value, opts)
  end

  @doc """
  Deletes a cached value.

  ## Examples

      {:ok, _} = MegasPinakas.Cache.delete(project, instance, "cache", "user:123")
  """
  @spec delete(String.t(), String.t(), String.t(), String.t(), keyword()) ::
          {:ok, term()} | {:error, term()}
  def delete(project, instance, table, key, opts \\ []) do
    family = Keyword.get(opts, :family, @default_family)
    qualifier = Keyword.get(opts, :qualifier, @default_qualifier)

    mutations = [MegasPinakas.delete_from_column(family, qualifier)]
    MegasPinakas.mutate_row(project, instance, table, key, mutations, opts)
  end

  @doc """
  Gets a value, computing and storing it if not present.

  ## Options

    * `:family` - Column family (default: "cache")
    * `:qualifier` - Column qualifier (default: "value")

  ## Examples

      {:ok, value} = MegasPinakas.Cache.get_or_put(project, instance, "cache", "key", fn ->
        expensive_computation()
      end)
  """
  @spec get_or_put(String.t(), String.t(), String.t(), String.t(), (-> term()), keyword()) ::
          {:ok, term()} | {:error, term()}
  def get_or_put(project, instance, table, key, default_fn, opts \\ []) when is_function(default_fn, 0) do
    case get(project, instance, table, key, opts) do
      {:ok, nil} ->
        value = default_fn.()

        case put(project, instance, table, key, value, opts) do
          {:ok, _} -> {:ok, value}
          {:error, reason} -> {:error, reason}
        end

      {:ok, value} ->
        {:ok, value}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # ============================================================================
  # Multi-Key Operations
  # ============================================================================

  @doc """
  Gets multiple values by keys.

  Returns a map of key => value pairs. Keys not found will have `nil` values.

  ## Examples

      {:ok, results} = MegasPinakas.Cache.get_many(project, instance, "cache", ["key1", "key2", "key3"])
      # => {:ok, %{"key1" => value1, "key2" => nil, "key3" => value3}}
  """
  @spec get_many(String.t(), String.t(), String.t(), [String.t()], keyword()) ::
          {:ok, map()} | {:error, term()}
  def get_many(project, instance, table, keys, opts \\ []) when is_list(keys) do
    family = Keyword.get(opts, :family, @default_family)
    qualifier = Keyword.get(opts, :qualifier, @default_qualifier)

    row_set = MegasPinakas.row_set(keys)

    case MegasPinakas.read_rows(project, instance, table, rows: row_set) do
      {:ok, rows} ->
        results =
          rows
          |> Enum.map(fn row ->
            key = MegasPinakas.row_key(row)
            raw_value = MegasPinakas.get_cell(row, family, qualifier)

            value =
              if raw_value do
                case Types.decode(:json, raw_value) do
                  {:ok, v} -> v
                  {:error, _} -> nil
                end
              else
                nil
              end

            {key, value}
          end)
          |> Map.new()

        # Add missing keys with nil values
        all_results =
          Enum.reduce(keys, results, fn key, acc ->
            Map.put_new(acc, key, nil)
          end)

        {:ok, all_results}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Stores multiple values.

  ## Examples

      {:ok, _} = MegasPinakas.Cache.put_many(project, instance, "cache", [
        {"key1", %{a: 1}},
        {"key2", %{b: 2}}
      ])
  """
  @spec put_many(String.t(), String.t(), String.t(), [{String.t(), term()}], keyword()) ::
          {:ok, term()} | {:error, term()}
  def put_many(project, instance, table, entries, opts \\ []) when is_list(entries) do
    family = Keyword.get(opts, :family, @default_family)
    qualifier = Keyword.get(opts, :qualifier, @default_qualifier)

    batch =
      Enum.reduce(entries, Batch.new(), fn {key, value}, batch ->
        row =
          Row.new(key)
          |> Row.put_json(family, qualifier, value)

        Batch.add(batch, row)
      end)

    Batch.write(batch, project, instance, table, opts)
  end

  @doc """
  Deletes multiple cached values.

  ## Examples

      {:ok, _} = MegasPinakas.Cache.delete_many(project, instance, "cache", ["key1", "key2"])
  """
  @spec delete_many(String.t(), String.t(), String.t(), [String.t()], keyword()) ::
          {:ok, term()} | {:error, term()}
  def delete_many(project, instance, table, keys, opts \\ []) when is_list(keys) do
    family = Keyword.get(opts, :family, @default_family)
    qualifier = Keyword.get(opts, :qualifier, @default_qualifier)

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
  Checks if a key exists in the cache.

  ## Examples

      exists? = MegasPinakas.Cache.exists?(project, instance, "cache", "key")
  """
  @spec exists?(String.t(), String.t(), String.t(), String.t(), keyword()) :: boolean()
  def exists?(project, instance, table, key, opts \\ []) do
    case get(project, instance, table, key, opts) do
      {:ok, nil} -> false
      {:ok, _} -> true
      {:error, _} -> false
    end
  end

  # ============================================================================
  # Atomic Operations
  # ============================================================================

  @doc """
  Atomically increments a numeric cache value.

  Uses BigTable's read-modify-write for atomicity.

  ## Examples

      {:ok, new_value} = MegasPinakas.Cache.increment(project, instance, "cache", "counter:views")
  """
  @spec increment(String.t(), String.t(), String.t(), String.t(), integer(), keyword()) ::
          {:ok, integer()} | {:error, term()}
  def increment(project, instance, table, key, amount \\ 1, opts \\ []) when is_integer(amount) do
    family = Keyword.get(opts, :family, @default_family)
    qualifier = Keyword.get(opts, :qualifier, @default_qualifier)

    MegasPinakas.Counter.increment(project, instance, table, key, family, qualifier, amount, opts)
  end

  @doc """
  Atomically appends to a string cache value.

  ## Examples

      {:ok, new_value} = MegasPinakas.Cache.append(project, instance, "cache", "log:123", "new entry\n")
  """
  @spec append(String.t(), String.t(), String.t(), String.t(), binary(), keyword()) ::
          {:ok, binary()} | {:error, term()}
  def append(project, instance, table, key, value, opts \\ []) when is_binary(value) do
    family = Keyword.get(opts, :family, @default_family)
    qualifier = Keyword.get(opts, :qualifier, @default_qualifier)

    rules = [MegasPinakas.append_rule(family, qualifier, value)]

    case MegasPinakas.read_modify_write_row(project, instance, table, key, rules, opts) do
      {:ok, response} ->
        extract_value(response, family, qualifier)

      {:error, reason} ->
        {:error, reason}
    end
  end

  # ============================================================================
  # Private Helpers
  # ============================================================================

  defp extract_value(response, family, qualifier) do
    case response.row do
      nil ->
        {:ok, nil}

      row ->
        value = MegasPinakas.get_cell(row, family, qualifier)
        {:ok, value}
    end
  end
end
