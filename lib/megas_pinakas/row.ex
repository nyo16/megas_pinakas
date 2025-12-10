defmodule MegasPinakas.Row do
  @moduledoc """
  Fluent row builder for constructing multi-cell mutations.

  Provides a pipeline-friendly API for building rows with typed cells,
  supporting automatic type inference and explicit type specification.

  ## Examples

      # Basic usage with explicit types
      MegasPinakas.Row.new("user#123")
      |> MegasPinakas.Row.put_string("cf", "name", "John Doe")
      |> MegasPinakas.Row.put_integer("cf", "age", 30)
      |> MegasPinakas.Row.put_json("cf", "profile", %{city: "NYC"})
      |> MegasPinakas.Row.write(project, instance, "users")

      # Using type inference
      MegasPinakas.Row.new("user#123")
      |> MegasPinakas.Row.put("cf", "name", "John Doe")       # Infers string
      |> MegasPinakas.Row.put("cf", "age", 30)                # Infers integer
      |> MegasPinakas.Row.put("cf", "score", 98.5)            # Infers float
      |> MegasPinakas.Row.put("cf", "active", true)           # Infers boolean
      |> MegasPinakas.Row.put("cf", "profile", %{city: "NYC"}) # Infers JSON
      |> MegasPinakas.Row.write(project, instance, "users")

      # Building for batch operations
      row = MegasPinakas.Row.new("user#123")
            |> MegasPinakas.Row.put_string("cf", "name", "John")

      mutations = MegasPinakas.Row.to_mutations(row)
      entry = MegasPinakas.Row.to_entry(row)
  """

  alias MegasPinakas
  alias MegasPinakas.Types

  @enforce_keys [:row_key]
  defstruct row_key: nil, mutations: []

  @type t :: %__MODULE__{
          row_key: binary(),
          mutations: [Google.Bigtable.V2.Mutation.t()]
        }

  # ============================================================================
  # Constructor
  # ============================================================================

  @doc """
  Creates a new row builder with the given row key.

  ## Examples

      row = MegasPinakas.Row.new("user#123")
  """
  @spec new(binary()) :: t()
  def new(row_key) when is_binary(row_key) do
    %__MODULE__{row_key: row_key, mutations: []}
  end

  # ============================================================================
  # Type-Inferred Put
  # ============================================================================

  @doc """
  Adds a cell with automatic type inference based on the value.

  Type inference rules:
  - `binary` (not map/list) -> raw binary/string
  - `map` or `list` -> JSON encoded
  - `integer` -> 64-bit big-endian integer
  - `float` -> 64-bit IEEE 754 float
  - `boolean` -> single byte (<<1>> or <<0>>)
  - `%DateTime{}` -> microseconds since epoch

  ## Examples

      row
      |> MegasPinakas.Row.put("cf", "name", "John")         # string
      |> MegasPinakas.Row.put("cf", "age", 30)              # integer
      |> MegasPinakas.Row.put("cf", "score", 98.5)          # float
      |> MegasPinakas.Row.put("cf", "active", true)         # boolean
      |> MegasPinakas.Row.put("cf", "data", %{a: 1})        # JSON
      |> MegasPinakas.Row.put("cf", "created", ~U[2024-01-15 10:00:00Z])  # datetime
  """
  @spec put(t(), String.t(), String.t(), term(), keyword()) :: t()
  def put(row, family, qualifier, value, opts \\ [])

  def put(%__MODULE__{} = row, family, qualifier, value, opts) when is_binary(value) do
    put_string(row, family, qualifier, value, opts)
  end

  def put(%__MODULE__{} = row, family, qualifier, value, opts) when is_integer(value) do
    put_integer(row, family, qualifier, value, opts)
  end

  def put(%__MODULE__{} = row, family, qualifier, value, opts) when is_float(value) do
    put_float(row, family, qualifier, value, opts)
  end

  def put(%__MODULE__{} = row, family, qualifier, value, opts) when is_boolean(value) do
    put_boolean(row, family, qualifier, value, opts)
  end

  # DateTime must come before map/list since DateTime is a struct (which is a map)
  def put(%__MODULE__{} = row, family, qualifier, %DateTime{} = value, opts) do
    put_datetime(row, family, qualifier, value, opts)
  end

  def put(%__MODULE__{} = row, family, qualifier, value, opts) when is_map(value) or is_list(value) do
    put_json(row, family, qualifier, value, opts)
  end

  # ============================================================================
  # Explicit Type Puts
  # ============================================================================

  @doc """
  Adds a raw binary cell value.
  """
  @spec put_binary(t(), String.t(), String.t(), binary(), keyword()) :: t()
  def put_binary(%__MODULE__{} = row, family, qualifier, value, opts \\ []) when is_binary(value) do
    mutation = MegasPinakas.set_cell(family, qualifier, value, opts)
    add_mutation(row, mutation)
  end

  @doc """
  Adds a UTF-8 string cell value (same encoding as binary).
  """
  @spec put_string(t(), String.t(), String.t(), String.t(), keyword()) :: t()
  def put_string(%__MODULE__{} = row, family, qualifier, value, opts \\ []) when is_binary(value) do
    mutation = MegasPinakas.set_cell(family, qualifier, value, opts)
    add_mutation(row, mutation)
  end

  @doc """
  Adds a JSON-encoded cell value from a map or list.
  """
  @spec put_json(t(), String.t(), String.t(), map() | list(), keyword()) :: t()
  def put_json(%__MODULE__{} = row, family, qualifier, value, opts \\ [])
      when is_map(value) or is_list(value) do
    mutation = Types.set_json(family, qualifier, value, opts)
    add_mutation(row, mutation)
  end

  @doc """
  Adds a 64-bit big-endian integer cell value.
  """
  @spec put_integer(t(), String.t(), String.t(), integer(), keyword()) :: t()
  def put_integer(%__MODULE__{} = row, family, qualifier, value, opts \\ [])
      when is_integer(value) do
    mutation = Types.set_integer(family, qualifier, value, opts)
    add_mutation(row, mutation)
  end

  @doc """
  Adds a 64-bit float cell value.
  """
  @spec put_float(t(), String.t(), String.t(), float(), keyword()) :: t()
  def put_float(%__MODULE__{} = row, family, qualifier, value, opts \\ []) when is_float(value) do
    mutation = Types.set_float(family, qualifier, value, opts)
    add_mutation(row, mutation)
  end

  @doc """
  Adds a boolean cell value.
  """
  @spec put_boolean(t(), String.t(), String.t(), boolean(), keyword()) :: t()
  def put_boolean(%__MODULE__{} = row, family, qualifier, value, opts \\ [])
      when is_boolean(value) do
    mutation = Types.set_boolean(family, qualifier, value, opts)
    add_mutation(row, mutation)
  end

  @doc """
  Adds a DateTime cell value (stored as microseconds since Unix epoch).
  """
  @spec put_datetime(t(), String.t(), String.t(), DateTime.t(), keyword()) :: t()
  def put_datetime(%__MODULE__{} = row, family, qualifier, %DateTime{} = value, opts \\ []) do
    mutation = Types.set_datetime(family, qualifier, value, opts)
    add_mutation(row, mutation)
  end

  @doc """
  Adds any Elixir term as a cell value (via erlang term_to_binary).
  """
  @spec put_term(t(), String.t(), String.t(), term(), keyword()) :: t()
  def put_term(%__MODULE__{} = row, family, qualifier, value, opts \\ []) do
    mutation = Types.set_term(family, qualifier, value, opts)
    add_mutation(row, mutation)
  end

  # ============================================================================
  # Delete Operations
  # ============================================================================

  @doc """
  Adds a delete mutation for a specific cell (column).
  """
  @spec delete_cell(t(), String.t(), String.t()) :: t()
  def delete_cell(%__MODULE__{} = row, family, qualifier) do
    mutation = MegasPinakas.delete_from_column(family, qualifier)
    add_mutation(row, mutation)
  end

  @doc """
  Adds a delete mutation for an entire column family.
  """
  @spec delete_family(t(), String.t()) :: t()
  def delete_family(%__MODULE__{} = row, family) do
    mutation = MegasPinakas.delete_from_family(family)
    add_mutation(row, mutation)
  end

  @doc """
  Adds a delete mutation for the entire row.
  """
  @spec delete_row(t()) :: t()
  def delete_row(%__MODULE__{} = row) do
    mutation = MegasPinakas.delete_from_row()
    add_mutation(row, mutation)
  end

  # ============================================================================
  # Execution
  # ============================================================================

  @doc """
  Executes the row mutation against BigTable.

  ## Examples

      {:ok, response} = MegasPinakas.Row.new("user#123")
                        |> MegasPinakas.Row.put_string("cf", "name", "John")
                        |> MegasPinakas.Row.write(project, instance, "users")
  """
  @spec write(t(), String.t(), String.t(), String.t(), keyword()) ::
          {:ok, term()} | {:error, term()}
  def write(%__MODULE__{row_key: row_key, mutations: mutations}, project, instance, table, opts \\ []) do
    # Reverse mutations to maintain insertion order
    MegasPinakas.mutate_row(project, instance, table, row_key, Enum.reverse(mutations), opts)
  end

  # ============================================================================
  # Conversion
  # ============================================================================

  @doc """
  Returns the list of mutations in insertion order.

  Useful for manual mutation handling or debugging.
  """
  @spec to_mutations(t()) :: [Google.Bigtable.V2.Mutation.t()]
  def to_mutations(%__MODULE__{mutations: mutations}) do
    Enum.reverse(mutations)
  end

  @doc """
  Converts the row to a batch entry format for use with `MegasPinakas.mutate_rows/4`.

  Returns a map with `:row_key` and `:mutations` keys.

  ## Examples

      entry = MegasPinakas.Row.new("user#123")
              |> MegasPinakas.Row.put_string("cf", "name", "John")
              |> MegasPinakas.Row.to_entry()

      # => %{row_key: "user#123", mutations: [...]}
  """
  @spec to_entry(t()) :: %{row_key: binary(), mutations: [Google.Bigtable.V2.Mutation.t()]}
  def to_entry(%__MODULE__{row_key: row_key, mutations: mutations}) do
    %{row_key: row_key, mutations: Enum.reverse(mutations)}
  end

  @doc """
  Returns the row key.
  """
  @spec row_key(t()) :: binary()
  def row_key(%__MODULE__{row_key: key}), do: key

  @doc """
  Returns the number of mutations in the row.
  """
  @spec mutation_count(t()) :: non_neg_integer()
  def mutation_count(%__MODULE__{mutations: mutations}), do: length(mutations)

  @doc """
  Returns true if the row has no mutations.
  """
  @spec empty?(t()) :: boolean()
  def empty?(%__MODULE__{mutations: []}), do: true
  def empty?(%__MODULE__{}), do: false

  # ============================================================================
  # Private Helpers
  # ============================================================================

  defp add_mutation(%__MODULE__{mutations: mutations} = row, mutation) do
    %{row | mutations: [mutation | mutations]}
  end
end
