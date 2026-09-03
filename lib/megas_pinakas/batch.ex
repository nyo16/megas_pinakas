defmodule MegasPinakas.Batch do
  @moduledoc """
  Batch builder for multi-row mutations.

  Provides a fluent API for building batch operations that affect multiple rows,
  which can then be executed with `write/5`.

  BigTable caps a `MutateRows` request at 100,000 mutations and 256 MiB.
  `write/5` transparently splits larger batches by mutation count; see its docs
  for how results and failures are reported across chunks.

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

  @type entry :: %{row_key: binary(), mutations: [%Google.Bigtable.V2.Mutation{}]}
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
  @spec add(t(), binary(), [%Google.Bigtable.V2.Mutation{}]) :: t()
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

  @max_mutations_per_request 100_000

  @doc """
  Executes the batch mutation against BigTable.

  Returns a list of results, one for each row in the batch, ordered to match
  insertion order.

  `{:ok, results}` means the RPC succeeded, **not** that every row was written —
  `MutateRows` is partial-success, so check each entry:

      {:ok, results} = batch |> MegasPinakas.Batch.write(project, instance, "users")
      failed = Enum.reject(results, &(&1.status.code == 0))

  ## Request limits

  A single `MutateRows` request may carry at most 100,000 mutations and 256 MiB
  of data. Batches over the mutation cap are split into consecutive requests,
  each sent in turn; the results are concatenated and each result's `index` is
  re-based so it still refers to the entry's position in the batch. The first
  request to fail stops the sequence and its error is returned — entries in
  earlier chunks have already been applied, later chunks were not sent. The
  byte cap is not enforced client-side; a single row's mutations exceeding it is
  rejected by the server. An empty batch makes no request and returns `{:ok, []}`.

  ## Examples

      {:ok, results} = batch |> MegasPinakas.Batch.write(project, instance, "users")

  > #### Breaking change in 0.6.0 {: .warning}
  >
  > Previously returned an unconsumed `#Stream<>` from `MegasPinakas.mutate_rows/5`,
  > so per-row failures were silently discarded unless the caller enumerated it.
  """
  @spec write(t(), String.t(), String.t(), String.t(), keyword()) ::
          {:ok, [%Google.Bigtable.V2.MutateRowsResponse.Entry{}]} | {:error, term()}
  def write(%__MODULE__{} = batch, project, instance, table, opts \\ []) do
    batch
    |> to_entries()
    |> chunk_entries(@max_mutations_per_request)
    |> Enum.reduce_while({:ok, [], 0}, fn chunk, {:ok, acc, offset} ->
      case MegasPinakas.mutate_rows(project, instance, table, chunk, opts) do
        {:ok, results} ->
          rebased = Enum.map(results, &%{&1 | index: &1.index + offset})
          {:cont, {:ok, [rebased | acc], offset + length(chunk)}}

        {:error, reason} ->
          {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, chunks, _offset} -> {:ok, chunks |> Enum.reverse() |> Enum.concat()}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc false
  # Splits entries (insertion order) into consecutive runs whose total mutation
  # count stays within `limit`. Entry order is preserved so `write/5` can
  # re-base result indexes by chunk offset. A single entry larger than `limit`
  # gets its own chunk; the server rejects it with a clear error, whereas
  # splitting one row's mutations across requests would break atomicity.
  @spec chunk_entries([entry()], pos_integer()) :: [[entry()]]
  def chunk_entries(entries, limit) when is_list(entries) and is_integer(limit) and limit > 0 do
    entries
    |> Enum.chunk_while(
      {[], 0},
      fn entry, {chunk, count} ->
        size = length(entry.mutations)

        if chunk != [] and count + size > limit do
          {:cont, Enum.reverse(chunk), {[entry], size}}
        else
          {:cont, {[entry | chunk], count + size}}
        end
      end,
      fn
        {[], _} -> {:cont, []}
        {chunk, _} -> {:cont, Enum.reverse(chunk), []}
      end
    )
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
