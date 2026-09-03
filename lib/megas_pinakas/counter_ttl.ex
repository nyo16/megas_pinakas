defmodule MegasPinakas.CounterTTL do
  @moduledoc """
  Time-windowed counters using timestamp-based row keys.

  Useful for rate limiting, daily/hourly metrics, and sliding windows.
  Row keys include time buckets, allowing for efficient time-based queries.

  ## Row Key Format

  Row keys are formatted as: `<key>#<bucket_timestamp>`

  Where `bucket_timestamp` is the Unix timestamp (in seconds) of the bucket start.

  ## Column family configuration

  Nothing here expires data on its own. Old buckets stay in the table until you
  configure garbage collection on the counter family — typically
  `MegasPinakas.Admin.max_age_gc_rule/1` sized to the longest window you query,
  combined with `MegasPinakas.Admin.max_versions_gc_rule(1)` so increments do not
  pile up cell versions. Reads in this module only fetch the latest version.

  ## Families and qualifiers

  `increment/7`, `get_current/7` and `get_window/7` take `family` and
  `qualifier` as positional arguments. `check_rate_limit/6` and
  `increment_with_limit/6` take them as the `:family` / `:qualifier` options
  (defaults `"counters"` / `"count"`).

  ## Examples

      # Increment a rate limit counter (buckets per minute)
      {:ok, new_count} = MegasPinakas.CounterTTL.increment(
        project, instance, "rate_limits", "api:user#123", "limits", "requests",
        bucket: :minute
      )

      # Check if rate limited
      case MegasPinakas.CounterTTL.check_rate_limit(
        project, instance, "rate_limits", "api:user#123", 100,
        bucket: :minute, family: "limits", qualifier: "requests"
      ) do
        {:ok, count} -> # under limit
        {:error, :rate_limited, reset_at} -> # over limit
        {:error, reason} -> # request failed
      end

      # Get total count in current window
      {:ok, count} = MegasPinakas.CounterTTL.get_window(
        project, instance, "hourly_stats", "page#homepage", "stats", "views",
        bucket: :hour, window_size: 24  # last 24 hours
      )
  """

  alias MegasPinakas
  alias MegasPinakas.Counter
  alias MegasPinakas.Filter
  alias MegasPinakas.RowKey
  alias MegasPinakas.Types

  @default_family "counters"
  @default_qualifier "count"

  # ============================================================================
  # Basic Operations
  # ============================================================================

  @doc """
  Increments a time-bucketed counter and returns the new value.

  ## Options

    * `:bucket` - Time bucket size: `:second`, `:minute`, `:hour`, `:day`, `:week` (default: `:minute`)
    * `:amount` - Amount to add; may be negative (default: 1)
    * `:app_profile_id` - App profile to use

  ## Examples

      {:ok, new_value} = MegasPinakas.CounterTTL.increment(
        project, instance, "rate_limits", "user#123", "limits", "requests",
        bucket: :minute
      )
  """
  @spec increment(
          String.t(),
          String.t(),
          String.t(),
          String.t(),
          String.t(),
          String.t(),
          keyword()
        ) ::
          {:ok, integer()} | {:error, term()}
  def increment(project, instance, table, key, family, qualifier, opts \\ []) do
    bucket = Keyword.get(opts, :bucket, :minute)
    row_key = build_row_key(key, bucket)

    increment_row(project, instance, table, row_key, family, qualifier, opts)
  end

  @doc """
  Gets the current value of the counter for the current time bucket.

  Returns `{:ok, nil}` when no increment has hit the bucket yet.

  ## Options

    * `:bucket` - Time bucket size (default: `:minute`)
    * `:timestamp` - Unix seconds to query instead of now
    * `:app_profile_id` - App profile to use

  ## Examples

      {:ok, value} = MegasPinakas.CounterTTL.get_current(
        project, instance, "rate_limits", "user#123", "limits", "requests",
        bucket: :minute
      )
  """
  @spec get_current(
          String.t(),
          String.t(),
          String.t(),
          String.t(),
          String.t(),
          String.t(),
          keyword()
        ) ::
          {:ok, integer() | nil} | {:error, term()}
  def get_current(project, instance, table, key, family, qualifier, opts \\ []) do
    bucket = Keyword.get(opts, :bucket, :minute)
    timestamp = Keyword.get(opts, :timestamp, System.system_time(:second))
    row_key = build_row_key(key, bucket, timestamp)

    Counter.get(project, instance, table, row_key, family, qualifier, opts)
  end

  @doc """
  Gets the sum of counter values across a time window.

  The window covers the current bucket and the `window_size - 1` buckets before
  it. Buckets with no counter contribute 0.

  ## Options

    * `:bucket` - Time bucket size (default: `:minute`)
    * `:window_size` - Number of buckets to include; must be positive (default: 1)
    * `:app_profile_id` - App profile to use

  Raises `ArgumentError` when `:window_size` is not a positive integer.

  ## Examples

      # Get total requests in the last 5 minutes
      {:ok, total} = MegasPinakas.CounterTTL.get_window(
        project, instance, "rate_limits", "user#123", "limits", "requests",
        bucket: :minute, window_size: 5
      )
  """
  @spec get_window(
          String.t(),
          String.t(),
          String.t(),
          String.t(),
          String.t(),
          String.t(),
          keyword()
        ) ::
          {:ok, integer()} | {:error, term()}
  def get_window(project, instance, table, key, family, qualifier, opts \\ []) do
    bucket = Keyword.get(opts, :bucket, :minute)
    window_size = Keyword.get(opts, :window_size, 1)

    unless is_integer(window_size) and window_size > 0 do
      raise ArgumentError,
            ":window_size must be a positive integer, got: #{inspect(window_size)}"
    end

    bucket_seconds = bucket_to_seconds(bucket)
    current_bucket = bucket_start(System.system_time(:second), bucket_seconds)

    row_keys =
      Enum.map(0..(window_size - 1)//1, fn offset ->
        "#{key}##{current_bucket - offset * bucket_seconds}"
      end)

    filter =
      Filter.chain_filters([
        Filter.column_filter(family, qualifier),
        Filter.cells_per_column_limit_filter(1)
      ])

    read_opts =
      opts
      |> Keyword.take([:app_profile_id])
      |> Keyword.put(:rows, MegasPinakas.row_set(row_keys))
      |> Keyword.put(:filter, filter)

    case MegasPinakas.read_rows(project, instance, table, read_opts) do
      {:ok, rows} ->
        total =
          Enum.reduce(rows, 0, fn row, acc ->
            acc + sum_counter_from_row(row, family, qualifier)
          end)

        {:ok, total}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # ============================================================================
  # Rate Limiting
  # ============================================================================

  @doc """
  Checks if a rate limit has been exceeded.

  Returns `{:ok, current_count}` if under limit, `{:error, :rate_limited, reset_at}`
  if the limit has been reached, or `{:error, reason}` if the read failed.

  ## Options

    * `:bucket` - Time bucket size (default: `:minute`)
    * `:family` - Column family (default: "counters")
    * `:qualifier` - Column qualifier (default: "count")
    * `:app_profile_id` - App profile to use

  ## Examples

      case MegasPinakas.CounterTTL.check_rate_limit(
        project, instance, "rate_limits", "api:user#123", 100,
        bucket: :minute
      ) do
        {:ok, count} ->
          IO.puts("Requests this minute: \#{count}")
        {:error, :rate_limited, reset_at} ->
          IO.puts("Rate limited. Resets at: \#{reset_at}")
        {:error, reason} ->
          IO.puts("Lookup failed: \#{inspect(reason)}")
      end
  """
  @spec check_rate_limit(String.t(), String.t(), String.t(), String.t(), integer(), keyword()) ::
          {:ok, integer()} | {:error, :rate_limited, DateTime.t()} | {:error, term()}
  def check_rate_limit(project, instance, table, key, limit, opts \\ []) when is_integer(limit) do
    family = Keyword.get(opts, :family, @default_family)
    qualifier = Keyword.get(opts, :qualifier, @default_qualifier)
    {row_key, reset_at} = current_bucket(key, Keyword.get(opts, :bucket, :minute))

    # A missing bucket counts as 0 so it still rate-limits when `limit <= 0`,
    # matching `increment_with_limit/6`.
    case Counter.get(project, instance, table, row_key, family, qualifier, opts) do
      {:ok, nil} when 0 < limit -> {:ok, 0}
      {:ok, count} when is_integer(count) and count < limit -> {:ok, count}
      {:ok, _count} -> {:error, :rate_limited, reset_at}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Increments a counter only if it's under the specified limit.

  Returns `{:ok, new_count}` if incremented, `{:error, :rate_limited, reset_at}`
  if the limit has been reached (nothing is written), or `{:error, reason}` if a
  request failed. A `limit` of zero or less always rate-limits.

  The bucket row key is computed once and used for both the read and the
  increment, so a bucket boundary crossing between the two cannot split them
  across buckets. The check and the increment are still two RPCs, not one
  atomic operation: concurrent callers can collectively overshoot `limit` by up
  to the number of in-flight requests. For strict limits, use the returned
  `new_count` and compensate, or move the check into the data store.

  ## Options

    * `:bucket` - Time bucket size (default: `:minute`)
    * `:amount` - Amount to add (default: 1)
    * `:family` - Column family (default: "counters")
    * `:qualifier` - Column qualifier (default: "count")
    * `:app_profile_id` - App profile to use

  ## Examples

      case MegasPinakas.CounterTTL.increment_with_limit(
        project, instance, "rate_limits", "api:user#123", 100,
        bucket: :minute
      ) do
        {:ok, new_count} ->
          IO.puts("Incremented to: \#{new_count}")
        {:error, :rate_limited, reset_at} ->
          IO.puts("Rate limited")
        {:error, reason} ->
          IO.puts("Request failed: \#{inspect(reason)}")
      end
  """
  @spec increment_with_limit(
          String.t(),
          String.t(),
          String.t(),
          String.t(),
          integer(),
          keyword()
        ) ::
          {:ok, integer()} | {:error, :rate_limited, DateTime.t()} | {:error, term()}
  def increment_with_limit(project, instance, table, key, limit, opts \\ [])
      when is_integer(limit) do
    family = Keyword.get(opts, :family, @default_family)
    qualifier = Keyword.get(opts, :qualifier, @default_qualifier)
    {row_key, reset_at} = current_bucket(key, Keyword.get(opts, :bucket, :minute))

    if limit <= 0 do
      {:error, :rate_limited, reset_at}
    else
      case Counter.get(project, instance, table, row_key, family, qualifier, opts) do
        {:ok, count} when is_nil(count) or count < limit ->
          increment_row(project, instance, table, row_key, family, qualifier, opts)

        {:ok, _count} ->
          {:error, :rate_limited, reset_at}

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  # ============================================================================
  # Helpers
  # ============================================================================

  @doc """
  Builds a time-bucketed row key.

  ## Examples

      MegasPinakas.CounterTTL.build_row_key("user#123", :minute)
      # => "user#123#1704067200"
  """
  @spec build_row_key(String.t(), atom(), integer() | nil) :: String.t()
  def build_row_key(key, bucket, timestamp \\ nil) do
    ts = timestamp || System.system_time(:second)
    "#{key}##{bucket_start(ts, bucket_to_seconds(bucket))}"
  end

  @doc """
  Parses a time-bucketed row key.

  ## Examples

      {:ok, %{key: "user#123", bucket_timestamp: 1704067200}} =
        MegasPinakas.CounterTTL.parse_row_key("user#123#1704067200")
  """
  @spec parse_row_key(String.t()) :: {:ok, map()} | {:error, :invalid_format}
  def parse_row_key(row_key) do
    {key, timestamp_str} = RowKey.split_suffix(row_key)

    case Integer.parse(timestamp_str) do
      {timestamp, ""} ->
        {:ok, %{key: key, bucket_timestamp: timestamp}}

      _ ->
        {:error, :invalid_format}
    end
  end

  @doc """
  Converts a bucket type to seconds.
  """
  @spec bucket_to_seconds(atom()) :: pos_integer()
  def bucket_to_seconds(:second), do: 1
  def bucket_to_seconds(:minute), do: 60
  def bucket_to_seconds(:hour), do: 3600
  def bucket_to_seconds(:day), do: 86_400
  def bucket_to_seconds(:week), do: 604_800

  # ============================================================================
  # Private Helpers
  # ============================================================================

  defp bucket_start(ts, bucket_seconds), do: div(ts, bucket_seconds) * bucket_seconds

  # Resolves "now" exactly once so the row key and the reset time describe the
  # same bucket, even if the wall clock crosses a boundary mid-call.
  defp current_bucket(key, bucket) do
    bucket_seconds = bucket_to_seconds(bucket)
    start = bucket_start(System.system_time(:second), bucket_seconds)
    {"#{key}##{start}", DateTime.from_unix!(start + bucket_seconds)}
  end

  defp increment_row(project, instance, table, row_key, family, qualifier, opts) do
    amount = Keyword.get(opts, :amount, 1)
    rules = [MegasPinakas.increment_rule(family, qualifier, amount)]

    case MegasPinakas.read_modify_write_row(project, instance, table, row_key, rules, opts) do
      {:ok, response} -> Counter.extract_counter_value(response, family, qualifier)
      {:error, reason} -> {:error, reason}
    end
  end

  defp sum_counter_from_row(row, family, qualifier) do
    case MegasPinakas.get_cell(row, family, qualifier) do
      nil ->
        0

      value ->
        case Types.decode(:integer, value) do
          {:ok, v} -> v
          {:error, _} -> 0
        end
    end
  end
end
