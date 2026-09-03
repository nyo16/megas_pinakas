defmodule MegasPinakas.Filter do
  @moduledoc ~S"""
  BigTable filter builders for read operations.

  Filters reduce data returned from reads, improving performance.
  Three categories:
  - **Limiting filters**: Control which rows/cells are included
  - **Modifying filters**: Transform cell data/metadata
  - **Composing filters**: Combine multiple filters (AND/OR/conditional)

  ## Regex semantics

  Every `*_regex_filter` is an [RE2](https://github.com/google/re2/wiki/Syntax)
  pattern that BigTable matches against the **whole** byte string — row key,
  qualifier, family name, or value. There is no implicit `.*` on either side, so
  `row_key_regex_filter("user#")` matches only the row whose key is exactly
  `user#`. `^` and `$` are accepted but redundant.

  To match a prefix, suffix, or substring, pad the pattern with `\C*`, the RE2
  byte wildcard (`.` does not match `\n` and, in UTF-8 mode, refuses arbitrary
  bytes). `row_key_prefix_filter/1` does this for you:

      MegasPinakas.Filter.row_key_regex_filter("user#\\C*")   # prefix
      MegasPinakas.Filter.row_key_regex_filter("\\C*_count")  # suffix
      MegasPinakas.Filter.row_key_regex_filter("\\C*admin\\C*") # substring

  ## Examples

      # Get only the "name" column from "cf" family
      filter = MegasPinakas.Filter.column_filter("cf", "name")
      {:ok, rows} = MegasPinakas.read_rows(project, instance, "users", filter: filter)

      # Get recent cells only (last hour)
      now = System.system_time(:microsecond)
      hour_ago = now - 3_600_000_000
      filter = MegasPinakas.Filter.timestamp_range_filter(hour_ago, now)

      # Sample 10% of rows randomly
      filter = MegasPinakas.Filter.row_sample_filter(0.1)

      # Complex filter: family "cf", column "name", only latest version
      filter = MegasPinakas.Filter.chain_filters([
        MegasPinakas.Filter.family_filter("cf"),
        MegasPinakas.Filter.column_qualifier_regex_filter("name"),
        MegasPinakas.Filter.cells_per_column_limit_filter(1)
      ])
  """

  alias Google.Bigtable.V2.{ColumnRange, RowFilter, TimestampRange, ValueRange}

  # ============================================================================
  # Limiting Filters - Row Level
  # ============================================================================

  @doc ~S"""
  Creates a filter that matches row keys by regex pattern.

  Uses RE2 regex syntax and matches the whole key; see "Regex semantics" in the
  module docs.

  ## Examples

      # Match row keys starting with "user#"
      MegasPinakas.Filter.row_key_regex_filter("user#\\C*")

      # Match row keys containing "admin"
      MegasPinakas.Filter.row_key_regex_filter("\\C*admin\\C*")
  """
  @spec row_key_regex_filter(String.t()) :: %RowFilter{}
  def row_key_regex_filter(regex) when is_binary(regex) do
    %RowFilter{filter: {:row_key_regex_filter, regex}}
  end

  @doc """
  Creates a filter that randomly samples rows.

  Probability must be strictly between 0 and 1: BigTable rejects `0` and `1`
  with `INVALID_ARGUMENT`. Use `block_all_filter/0` or `pass_all_filter/0` for
  the degenerate cases.

  ## Examples

      # Sample approximately 10% of rows
      MegasPinakas.Filter.row_sample_filter(0.1)

      # Sample approximately 50% of rows
      MegasPinakas.Filter.row_sample_filter(0.5)
  """
  @spec row_sample_filter(number()) :: %RowFilter{}
  def row_sample_filter(probability)
      when is_number(probability) and probability > 0 and probability < 1 do
    %RowFilter{filter: {:row_sample_filter, probability / 1}}
  end

  # ============================================================================
  # Limiting Filters - Cell Level
  # ============================================================================

  @doc """
  Creates a filter that limits total cells per row.

  ## Examples

      # Return at most 100 cells per row
      MegasPinakas.Filter.cells_per_row_limit_filter(100)
  """
  @spec cells_per_row_limit_filter(pos_integer()) :: %RowFilter{}
  def cells_per_row_limit_filter(limit) when is_integer(limit) and limit > 0 do
    %RowFilter{filter: {:cells_per_row_limit_filter, limit}}
  end

  @doc """
  Creates a filter that skips the first N cells per row.

  ## Examples

      # Skip the first 10 cells in each row
      MegasPinakas.Filter.cells_per_row_offset_filter(10)
  """
  @spec cells_per_row_offset_filter(non_neg_integer()) :: %RowFilter{}
  def cells_per_row_offset_filter(offset) when is_integer(offset) and offset >= 0 do
    %RowFilter{filter: {:cells_per_row_offset_filter, offset}}
  end

  @doc """
  Creates a filter that limits cells per column (versions).

  ## Examples

      # Return only the latest version of each column
      MegasPinakas.Filter.cells_per_column_limit_filter(1)

      # Return the 3 most recent versions
      MegasPinakas.Filter.cells_per_column_limit_filter(3)
  """
  @spec cells_per_column_limit_filter(pos_integer()) :: %RowFilter{}
  def cells_per_column_limit_filter(limit) when is_integer(limit) and limit > 0 do
    %RowFilter{filter: {:cells_per_column_limit_filter, limit}}
  end

  @doc ~S"""
  Creates a filter that matches column qualifiers by regex.

  Uses RE2 regex syntax and matches the whole qualifier; see "Regex semantics"
  in the module docs.

  ## Examples

      # Match columns starting with "meta_"
      MegasPinakas.Filter.column_qualifier_regex_filter("meta_\\C*")

      # Match columns ending with "_count"
      MegasPinakas.Filter.column_qualifier_regex_filter("\\C*_count")
  """
  @spec column_qualifier_regex_filter(String.t()) :: %RowFilter{}
  def column_qualifier_regex_filter(regex) when is_binary(regex) do
    %RowFilter{filter: {:column_qualifier_regex_filter, regex}}
  end

  # ============================================================================
  # Range Filters
  # ============================================================================

  @doc """
  Creates a filter for a range of column qualifiers.

  Give at most one start bound and at most one end bound; passing both the
  `_closed` and `_open` variant for the same side raises `ArgumentError`.

  ## Options

    * `:start_qualifier_closed` - Start of range (inclusive)
    * `:start_qualifier_open` - Start of range (exclusive)
    * `:end_qualifier_closed` - End of range (inclusive)
    * `:end_qualifier_open` - End of range (exclusive)

  ## Examples

      # Columns from "a" (inclusive) to "m" (exclusive)
      MegasPinakas.Filter.column_range_filter("cf",
        start_qualifier_closed: "a",
        end_qualifier_open: "m"
      )

      # All columns after "z" (exclusive)
      MegasPinakas.Filter.column_range_filter("cf",
        start_qualifier_open: "z"
      )
  """
  @spec column_range_filter(String.t(), keyword()) :: %RowFilter{}
  def column_range_filter(family, opts \\ []) when is_binary(family) do
    range = %ColumnRange{
      family_name: family,
      start_qualifier: range_bound(opts, :start_qualifier_closed, :start_qualifier_open),
      end_qualifier: range_bound(opts, :end_qualifier_closed, :end_qualifier_open)
    }

    %RowFilter{filter: {:column_range_filter, range}}
  end

  @doc """
  Creates a filter for a timestamp range.

  Timestamps are microseconds since the Unix epoch. The start is inclusive, the
  end is exclusive, and `0` on either side means "unbounded" — so
  `timestamp_range_filter(0, t)` is "strictly before `t`" and
  `timestamp_range_filter(t, 0)` is "at or after `t`".

  ## Examples

      # Get cells from the last hour
      now = System.system_time(:microsecond)
      hour_ago = now - 3_600_000_000
      MegasPinakas.Filter.timestamp_range_filter(hour_ago, now)

      # Get cells from a specific time range
      start_micros = DateTime.to_unix(~U[2024-01-01 00:00:00Z], :microsecond)
      end_micros = DateTime.to_unix(~U[2024-02-01 00:00:00Z], :microsecond)
      MegasPinakas.Filter.timestamp_range_filter(start_micros, end_micros)
  """
  @spec timestamp_range_filter(non_neg_integer(), non_neg_integer()) :: %RowFilter{}
  def timestamp_range_filter(start_timestamp_micros, end_timestamp_micros)
      when is_integer(start_timestamp_micros) and start_timestamp_micros >= 0 and
             is_integer(end_timestamp_micros) and end_timestamp_micros >= 0 do
    range = %TimestampRange{
      start_timestamp_micros: start_timestamp_micros,
      end_timestamp_micros: end_timestamp_micros
    }

    %RowFilter{filter: {:timestamp_range_filter, range}}
  end

  @doc """
  Creates a filter for a value range.

  Give at most one start bound and at most one end bound; passing both the
  `_closed` and `_open` variant for the same side raises `ArgumentError`.

  ## Options

    * `:start_value_closed` - Start of range (inclusive)
    * `:start_value_open` - Start of range (exclusive)
    * `:end_value_closed` - End of range (inclusive)
    * `:end_value_open` - End of range (exclusive)

  ## Examples

      # Values from "A" to "Z"
      MegasPinakas.Filter.value_range_filter(
        start_value_closed: "A",
        end_value_closed: "Z"
      )

      # Binary values in a range
      MegasPinakas.Filter.value_range_filter(
        start_value_closed: <<0, 0, 0, 0, 0, 0, 0, 100>>,
        end_value_open: <<0, 0, 0, 0, 0, 0, 0, 200>>
      )
  """
  @spec value_range_filter(keyword()) :: %RowFilter{}
  def value_range_filter(opts \\ []) do
    range = %ValueRange{
      start_value: range_bound(opts, :start_value_closed, :start_value_open),
      end_value: range_bound(opts, :end_value_closed, :end_value_open)
    }

    %RowFilter{filter: {:value_range_filter, range}}
  end

  # Resolves one side of a range to its proto oneof tuple. Passing both the
  # `_closed` and `_open` bound for one side is a caller error.
  defp range_bound(opts, closed_key, open_key) do
    case {Keyword.get(opts, closed_key), Keyword.get(opts, open_key)} do
      {nil, nil} ->
        nil

      {closed, nil} ->
        {closed_key, closed}

      {nil, open} ->
        {open_key, open}

      _ ->
        raise ArgumentError,
              "#{inspect(closed_key)} and #{inspect(open_key)} are mutually exclusive, got both"
    end
  end

  @doc ~S"""
  Creates a filter that matches cell values by regex.

  Uses RE2 regex syntax and matches the whole value; see "Regex semantics" in
  the module docs.

  ## Examples

      # Match values containing "error"
      MegasPinakas.Filter.value_regex_filter("\\C*error\\C*")

      # Match values that are valid UUIDs
      MegasPinakas.Filter.value_regex_filter("[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}")
  """
  @spec value_regex_filter(String.t()) :: %RowFilter{}
  def value_regex_filter(regex) when is_binary(regex) do
    %RowFilter{filter: {:value_regex_filter, regex}}
  end

  # ============================================================================
  # Family and Column Filters (convenience)
  # ============================================================================

  @doc """
  Creates a filter that matches a column family by name.

  The family name is escaped for use in a regex.

  ## Examples

      MegasPinakas.Filter.family_filter("cf")
      MegasPinakas.Filter.family_filter("user_data")
  """
  @spec family_filter(String.t()) :: %RowFilter{}
  def family_filter(family_name) when is_binary(family_name) do
    %RowFilter{filter: {:family_name_regex_filter, "^#{Regex.escape(family_name)}$"}}
  end

  @doc ~S"""
  Creates a filter that matches a column family by regex.

  Uses RE2 regex syntax and matches the whole family name; see "Regex
  semantics" in the module docs.

  ## Examples

      # Match families starting with "cf_"
      MegasPinakas.Filter.family_regex_filter("cf_\\C*")
  """
  @spec family_regex_filter(String.t()) :: %RowFilter{}
  def family_regex_filter(regex) when is_binary(regex) do
    %RowFilter{filter: {:family_name_regex_filter, regex}}
  end

  @doc """
  Creates a filter that matches a specific column (family + qualifier).

  ## Examples

      MegasPinakas.Filter.column_filter("cf", "name")
  """
  @spec column_filter(String.t(), String.t()) :: %RowFilter{}
  def column_filter(family_name, column_qualifier)
      when is_binary(family_name) and is_binary(column_qualifier) do
    chain_filters([
      family_filter(family_name),
      %RowFilter{filter: {:column_qualifier_regex_filter, "^#{Regex.escape(column_qualifier)}$"}}
    ])
  end

  # ============================================================================
  # Modifying Filters (Transformers)
  # ============================================================================

  @doc """
  Creates a filter that removes cell values, keeping only metadata.

  Useful for getting row structure without large values.

  ## Examples

      # Get all columns but strip their values
      MegasPinakas.Filter.strip_value_filter()
  """
  @spec strip_value_filter() :: %RowFilter{}
  def strip_value_filter do
    %RowFilter{filter: {:strip_value_transformer, true}}
  end

  @doc """
  Creates a filter that adds a label to matching cells.

  Labels are included in the response and can be used for
  identifying which branch of a conditional filter matched.

  ## Examples

      MegasPinakas.Filter.apply_label_filter("important")
  """
  @spec apply_label_filter(String.t()) :: %RowFilter{}
  def apply_label_filter(label) when is_binary(label) do
    %RowFilter{filter: {:apply_label_transformer, label}}
  end

  # ============================================================================
  # Pass/Block Filters
  # ============================================================================

  @doc """
  Creates a filter that passes all cells unchanged.

  Useful as a default branch in conditional filters.

  ## Examples

      MegasPinakas.Filter.pass_all_filter()
  """
  @spec pass_all_filter() :: %RowFilter{}
  def pass_all_filter do
    %RowFilter{filter: {:pass_all_filter, true}}
  end

  @doc """
  Creates a filter that blocks all cells.

  Useful as a branch in conditional filters to exclude matches.

  ## Examples

      MegasPinakas.Filter.block_all_filter()
  """
  @spec block_all_filter() :: %RowFilter{}
  def block_all_filter do
    %RowFilter{filter: {:block_all_filter, true}}
  end

  # ============================================================================
  # Composing Filters
  # ============================================================================

  @doc """
  Chains multiple filters together (AND logic).

  Cells must pass ALL filters to be included.

  ## Examples

      # Family "cf", column "name", only latest version
      MegasPinakas.Filter.chain_filters([
        MegasPinakas.Filter.family_filter("cf"),
        MegasPinakas.Filter.column_qualifier_regex_filter("name"),
        MegasPinakas.Filter.cells_per_column_limit_filter(1)
      ])
  """
  @spec chain_filters([%RowFilter{}]) :: %RowFilter{}
  def chain_filters(filters) when is_list(filters) do
    %RowFilter{filter: {:chain, %RowFilter.Chain{filters: filters}}}
  end

  @doc """
  Interleaves multiple filters (OR logic).

  Cells matching ANY filter are included. Duplicates are possible
  if a cell matches multiple filters.

  ## Examples

      # Get "name" OR "email" columns
      MegasPinakas.Filter.interleave_filters([
        MegasPinakas.Filter.column_filter("cf", "name"),
        MegasPinakas.Filter.column_filter("cf", "email")
      ])
  """
  @spec interleave_filters([%RowFilter{}]) :: %RowFilter{}
  def interleave_filters(filters) when is_list(filters) do
    %RowFilter{filter: {:interleave, %RowFilter.Interleave{filters: filters}}}
  end

  @doc ~S"""
  Creates a conditional filter (if-then-else logic).

  If the predicate filter matches any cells in the row, the true_filter
  is applied; otherwise the false_filter is applied.

  Note: The predicate filter doesn't output cells - it just tests for matches.

  ## Examples

      # If row has "admin" column, return all cells; otherwise return nothing
      MegasPinakas.Filter.condition_filter(
        MegasPinakas.Filter.column_filter("cf", "admin"),
        MegasPinakas.Filter.pass_all_filter(),
        MegasPinakas.Filter.block_all_filter()
      )

      # Label cells based on value
      MegasPinakas.Filter.condition_filter(
        MegasPinakas.Filter.value_regex_filter("\\C*error\\C*"),
        MegasPinakas.Filter.apply_label_filter("has_error"),
        MegasPinakas.Filter.pass_all_filter()
      )
  """
  @spec condition_filter(%RowFilter{}, %RowFilter{} | nil, %RowFilter{} | nil) :: %RowFilter{}
  def condition_filter(predicate_filter, true_filter, false_filter \\ nil) do
    condition = %RowFilter.Condition{
      predicate_filter: predicate_filter,
      true_filter: true_filter,
      false_filter: false_filter
    }

    %RowFilter{filter: {:condition, condition}}
  end

  @doc """
  Creates a sink filter that prevents cells from being passed to subsequent filters.

  When combined with interleave, this can be used to implement complex
  filtering logic where earlier filters take precedence.

  ## Examples

      MegasPinakas.Filter.sink_filter()
  """
  @spec sink_filter() :: %RowFilter{}
  def sink_filter do
    %RowFilter{filter: {:sink, true}}
  end

  # ============================================================================
  # Convenience Builders
  # ============================================================================

  @doc """
  Creates a filter for getting only the latest version of each cell.

  Equivalent to `cells_per_column_limit_filter(1)`.

  ## Examples

      MegasPinakas.Filter.latest_only_filter()
  """
  @spec latest_only_filter() :: %RowFilter{}
  def latest_only_filter do
    cells_per_column_limit_filter(1)
  end

  @doc """
  Creates a filter for a specific column with only the latest version.

  ## Examples

      MegasPinakas.Filter.column_latest_filter("cf", "name")
  """
  @spec column_latest_filter(String.t(), String.t()) :: %RowFilter{}
  def column_latest_filter(family, qualifier) do
    chain_filters([
      column_filter(family, qualifier),
      cells_per_column_limit_filter(1)
    ])
  end

  @doc """
  Creates a filter that returns cells from within a time window.

  ## Examples

      # Cells from the last hour
      MegasPinakas.Filter.time_window_filter(:hour)

      # Cells from the last day
      MegasPinakas.Filter.time_window_filter(:day)

      # Cells from the last 30 minutes
      MegasPinakas.Filter.time_window_filter(:minute, 30)
  """
  @spec time_window_filter(atom(), pos_integer()) :: %RowFilter{}
  def time_window_filter(unit, count \\ 1) do
    now = System.system_time(:microsecond)

    micros_per_unit =
      case unit do
        :second -> 1_000_000
        :minute -> 60_000_000
        :hour -> 3_600_000_000
        :day -> 86_400_000_000
        :week -> 604_800_000_000
      end

    start_time = now - micros_per_unit * count
    timestamp_range_filter(start_time, now)
  end

  @doc ~S"""
  Creates a filter for rows whose key starts with `prefix`.

  Builds `row_key_regex_filter(Regex.escape(prefix) <> "\\C*")`. The `\C*`
  tail is what makes this a prefix match: BigTable regexes must match the
  whole key, so a bare escaped prefix would only match the row keyed exactly
  `prefix`.

  ## Examples

      MegasPinakas.Filter.row_key_prefix_filter("user#")
      # => matches "user#", "user#1", "user#2", not "admin#1"
  """
  @spec row_key_prefix_filter(String.t()) :: %RowFilter{}
  def row_key_prefix_filter(prefix) when is_binary(prefix) do
    row_key_regex_filter(Regex.escape(prefix) <> "\\C*")
  end
end
