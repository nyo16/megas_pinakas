defmodule MegasPinakas.Batch do
  @moduledoc """
  Batch builder for multi-row mutations.

  Provides a fluent API for building batch operations that affect multiple rows,
  which can then be executed with a single `mutate_rows` call.

  ## Examples

      # Build and execute a batch
      MegasPinakas.Batch.new()
      |> MegasPinakas.Batch.add(
           MegasPinakas.Row.new("user#1")
           |> MegasPinakas.Row.put_string("cf", "name", "Alice")
         )
      |> MegasPinakas.Batch.add(
           MegasPinakas.Row.new("user#2")
           |> MegasPinakas.Row.put_string("cf", "name", "Bob")
         )
      |> MegasPinakas.Batch.write(project, instance, "users")

      # Add rows with inline mutations
      MegasPinakas.Batch.new()
      |> MegasPinakas.Batch.add("user#1", [MegasPinakas.set_cell("cf", "name", "Alice")])
      |> MegasPinakas.Batch.add("user#2", [MegasPinakas.set_cell("cf", "name", "Bob")])
      |> MegasPinakas.Batch.write(project, instance, "users")

      # Add multiple rows at once
      rows = [
        MegasPinakas.Row.new("user#1") |> MegasPinakas.Row.put_string("cf", "name", "Alice"),
        MegasPinakas.Row.new("user#2") |> MegasPinakas.Row.put_string("cf", "name", "Bob")
      ]

      MegasPinakas.Batch.new()
      |> MegasPinakas.Batch.add_all(rows)
      |> MegasPinakas.Batch.write(project, instance, "users")
  """

  alias MegasPinakas
  alias MegasPinakas.Row

  defstruct entries: []

  @type entry :: %{row_key: binary(), mutations: [Google.Bigtable.V2.Mutation.t()]}
  @type t :: %__MODULE__{entries: [entry()]}

  # ============================================================================
  # Constructor
  # ============================================================================

  @doc """
  Creates a new empty batch builder.

  ## Examples

      batch = MegasPinakas.Batch.new()
  """
  @spec new() :: t()
  def new do
    %__MODULE__{entries: []}
  end

  # ============================================================================
  # Adding Rows
  # ============================================================================

  @doc """
  Adds a Row struct to the batch.

  ## Examples

      batch
      |> MegasPinakas.Batch.add(
           MegasPinakas.Row.new("user#123")
           |> MegasPinakas.Row.put_string("cf", "name", "John")
         )
  """
  @spec add(t(), Row.t()) :: t()
  def add(%__MODULE__{entries: entries} = batch, %Row{} = row) do
    entry = Row.to_entry(row)
    %{batch | entries: [entry | entries]}
  end

  @doc """
  Adds a row with inline mutations to the batch.

  ## Examples

      batch
      |> MegasPinakas.Batch.add("user#123", [
           MegasPinakas.set_cell("cf", "name", "John"),
           MegasPinakas.set_cell("cf", "age", "30")
         ])
  """
  @spec add(t(), binary(), [Google.Bigtable.V2.Mutation.t()]) :: t()
  def add(%__MODULE__{entries: entries} = batch, row_key, mutations)
      when is_binary(row_key) and is_list(mutations) do
    entry = %{row_key: row_key, mutations: mutations}
    %{batch | entries: [entry | entries]}
  end

  @doc """
  Adds multiple Row structs to the batch.

  ## Examples

      rows = [
        MegasPinakas.Row.new("user#1") |> MegasPinakas.Row.put_string("cf", "name", "Alice"),
        MegasPinakas.Row.new("user#2") |> MegasPinakas.Row.put_string("cf", "name", "Bob")
      ]

      batch |> MegasPinakas.Batch.add_all(rows)
  """
  @spec add_all(t(), [Row.t()]) :: t()
  def add_all(%__MODULE__{} = batch, rows) when is_list(rows) do
    Enum.reduce(rows, batch, fn row, acc -> add(acc, row) end)
  end

  # ============================================================================
  # Execution
  # ============================================================================

  @doc """
  Executes the batch mutation against BigTable.

  Returns a list of results, one for each row in the batch. Each result
  indicates whether the mutation for that row succeeded or failed.

  ## Examples

      {:ok, results} = batch |> MegasPinakas.Batch.write(project, instance, "users")
  """
  @spec write(t(), String.t(), String.t(), String.t(), keyword()) ::
          {:ok, term()} | {:error, term()}
  def write(%__MODULE__{entries: entries}, project, instance, table, opts \\ []) do
    # Reverse to maintain insertion order
    MegasPinakas.mutate_rows(project, instance, table, Enum.reverse(entries), opts)
  end

  # ============================================================================
  # Inspection
  # ============================================================================

  @doc """
  Returns the entries list for manual use with `MegasPinakas.mutate_rows/4`.

  Entries are returned in insertion order.

  ## Examples

      entries = batch |> MegasPinakas.Batch.to_entries()
      MegasPinakas.mutate_rows(project, instance, "users", entries)
  """
  @spec to_entries(t()) :: [entry()]
  def to_entries(%__MODULE__{entries: entries}) do
    Enum.reverse(entries)
  end

  @doc """
  Returns the number of rows in the batch.

  ## Examples

      MegasPinakas.Batch.size(batch)
      # => 3
  """
  @spec size(t()) :: non_neg_integer()
  def size(%__MODULE__{entries: entries}) do
    length(entries)
  end

  @doc """
  Returns true if the batch has no entries.

  ## Examples

      MegasPinakas.Batch.empty?(MegasPinakas.Batch.new())
      # => true
  """
  @spec empty?(t()) :: boolean()
  def empty?(%__MODULE__{entries: []}), do: true
  def empty?(%__MODULE__{}), do: false

  @doc """
  Returns the total number of mutations across all rows in the batch.

  ## Examples

      MegasPinakas.Batch.mutation_count(batch)
      # => 10
  """
  @spec mutation_count(t()) :: non_neg_integer()
  def mutation_count(%__MODULE__{entries: entries}) do
    Enum.reduce(entries, 0, fn entry, acc ->
      acc + length(entry.mutations)
    end)
  end

  @doc """
  Returns a list of all row keys in the batch (in insertion order).

  ## Examples

      MegasPinakas.Batch.row_keys(batch)
      # => ["user#1", "user#2", "user#3"]
  """
  @spec row_keys(t()) :: [binary()]
  def row_keys(%__MODULE__{entries: entries}) do
    entries
    |> Enum.reverse()
    |> Enum.map(& &1.row_key)
  end

  @doc """
  Clears all entries from the batch.

  ## Examples

      cleared_batch = MegasPinakas.Batch.clear(batch)
      MegasPinakas.Batch.empty?(cleared_batch)
      # => true
  """
  @spec clear(t()) :: t()
  def clear(%__MODULE__{} = _batch) do
    %__MODULE__{entries: []}
  end
end
