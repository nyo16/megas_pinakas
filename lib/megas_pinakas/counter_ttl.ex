defmodule MegasPinakas.CounterTTL do
  @moduledoc """
  Time-windowed counters using timestamp-based row keys.

  Useful for rate limiting, daily/hourly metrics, and sliding windows.
  Row keys include time buckets, allowing for efficient time-based queries
  and automatic data expiration via GC rules.

  ## Row Key Format

  Row keys are formatted as: `<key>#<bucket_timestamp>`

  Where `bucket_timestamp` is the Unix timestamp (in seconds) of the bucket start.

  ## Examples

      # Increment a rate limit counter (buckets per minute)
      {:ok, new_count} = MegasPinakas.CounterTTL.increment(
        project, instance, "rate_limits", "api:user#123", "limits", "requests",
        bucket: :minute
      )

      # Check if rate limited
      case MegasPinakas.CounterTTL.check_rate_limit(
        project, instance, "rate_limits", "api:user#123", 100,
        bucket: :minute
      ) do
        {:ok, count} -> # under limit
        {:error, :rate_limited, reset_at} -> # over limit
      end

      # Get total count in current window
      {:ok, count} = MegasPinakas.CounterTTL.get_window(
        project, instance, "hourly_stats", "page#homepage", "stats", "views",
        bucket: :hour, window_size: 24  # last 24 hours
      )
  """

  alias MegasPinakas
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
    * `:family` - Column family (default: "counters")
    * `:app_profile_id` - App profile to use

  ## Examples

      {:ok, new_value} = MegasPinakas.CounterTTL.increment(
        project, instance, "rate_limits", "user#123", "limits", "requests",
        bucket: :minute
      )
  """
  @spec increment(String.t(), String.t(), String.t(), String.t(), String.t(), String.t(), keyword()) ::
          {:ok, integer()} | {:error, term()}
  def increment(project, instance, table, key, family, qualifier, opts \\ []) do
    amount = Keyword.get(opts, :amount, 1)
    bucket = Keyword.get(opts, :bucket, :minute)
    row_key = build_row_key(key, bucket)

    rules = [MegasPinakas.increment_rule(family, qualifier, amount)]

    case MegasPinakas.read_modify_write_row(project, instance, table, row_key, rules, opts) do
      {:ok, response} ->
        extract_counter_value(response, family, qualifier)

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Gets the current value of the counter for the current time bucket.

  ## Options

    * `:bucket` - Time bucket size (default: `:minute`)
    * `:timestamp` - Specific timestamp to query (default: now)

  ## Examples

      {:ok, value} = MegasPinakas.CounterTTL.get_current(
        project, instance, "rate_limits", "user#123", "limits", "requests",
        bucket: :minute
      )
  """
  @spec get_current(String.t(), String.t(), String.t(), String.t(), String.t(), String.t(), keyword()) ::
          {:ok, integer() | nil} | {:error, term()}
  def get_current(project, instance, table, key, family, qualifier, opts \\ []) do
    bucket = Keyword.get(opts, :bucket, :minute)
    timestamp = Keyword.get(opts, :timestamp, System.system_time(:second))
    row_key = build_row_key(key, bucket, timestamp)

    Types.read_integer(project, instance, table, row_key, family, qualifier, opts)
  end

  @doc """
  Gets the sum of counter values across a time window.

  ## Options

    * `:bucket` - Time bucket size (default: `:minute`)
    * `:window_size` - Number of buckets to include (default: 1)

  ## Examples

      # Get total requests in the last 5 minutes
      {:ok, total} = MegasPinakas.CounterTTL.get_window(
        project, instance, "rate_limits", "user#123", "limits", "requests",
        bucket: :minute, window_size: 5
      )
  """
  @spec get_window(String.t(), String.t(), String.t(), String.t(), String.t(), String.t(), keyword()) ::
          {:ok, integer()} | {:error, term()}
  def get_window(project, instance, table, key, family, qualifier, opts \\ []) do
    bucket = Keyword.get(opts, :bucket, :minute)
    window_size = Keyword.get(opts, :window_size, 1)
    now = System.system_time(:second)

    # Build row keys for the window
    bucket_seconds = bucket_to_seconds(bucket)
    current_bucket = div(now, bucket_seconds) * bucket_seconds

    row_keys =
      Enum.map(0..(window_size - 1), fn offset ->
        bucket_start = current_bucket - offset * bucket_seconds
        "#{key}##{bucket_start}"
      end)

    # Read all rows
    row_set = MegasPinakas.row_set(row_keys)

    case MegasPinakas.read_rows(project, instance, table, rows: row_set) do
      {:ok, rows} ->
        total =
          Enum.reduce(rows, 0, fn row, acc ->
            case MegasPinakas.get_cell(row, family, qualifier) do
              nil ->
                acc

              value ->
                case Types.decode(:integer, value) do
                  {:ok, v} -> acc + v
                  {:error, _} -> acc
                end
            end
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

  Returns `{:ok, current_count}` if under limit, or `{:error, :rate_limited, reset_at}`
  if the limit has been exceeded.

  ## Options

    * `:bucket` - Time bucket size (default: `:minute`)
    * `:family` - Column family (default: "counters")
    * `:qualifier` - Column qualifier (default: "count")

  ## Examples

      case MegasPinakas.CounterTTL.check_rate_limit(
        project, instance, "rate_limits", "api:user#123", 100,
        bucket: :minute
      ) do
        {:ok, count} ->
          IO.puts("Requests this minute: \#{count}")
        {:error, :rate_limited, reset_at} ->
          IO.puts("Rate limited. Resets at: \#{reset_at}")
      end
  """
  @spec check_rate_limit(String.t(), String.t(), String.t(), String.t(), pos_integer(), keyword()) ::
          {:ok, integer()} | {:error, :rate_limited, DateTime.t()}
  def check_rate_limit(project, instance, table, key, limit, opts \\ []) when is_integer(limit) do
    bucket = Keyword.get(opts, :bucket, :minute)
    family = Keyword.get(opts, :family, @default_family)
    qualifier = Keyword.get(opts, :qualifier, @default_qualifier)

    case get_current(project, instance, table, key, family, qualifier, opts) do
      {:ok, nil} ->
        {:ok, 0}

      {:ok, count} when count < limit ->
        {:ok, count}

      {:ok, _count} ->
        reset_at = calculate_reset_time(bucket)
        {:error, :rate_limited, reset_at}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Increments a counter only if it's under the specified limit.

  Returns `{:ok, new_count}` if incremented, or `{:error, :rate_limited, reset_at}`
  if the limit has been reached.

  Note: This is not truly atomic - there's a race condition between check and increment.
  For strict rate limiting, consider using a separate service or Redis.

  ## Examples

      case MegasPinakas.CounterTTL.increment_with_limit(
        project, instance, "rate_limits", "api:user#123", 100,
        bucket: :minute
      ) do
        {:ok, new_count} ->
          IO.puts("Incremented to: \#{new_count}")
        {:error, :rate_limited, reset_at} ->
          IO.puts("Rate limited")
      end
  """
  @spec increment_with_limit(String.t(), String.t(), String.t(), String.t(), pos_integer(), keyword()) ::
          {:ok, integer()} | {:error, :rate_limited, DateTime.t()} | {:error, term()}
  def increment_with_limit(project, instance, table, key, limit, opts \\ []) when is_integer(limit) do
    bucket = Keyword.get(opts, :bucket, :minute)
    family = Keyword.get(opts, :family, @default_family)
    qualifier = Keyword.get(opts, :qualifier, @default_qualifier)

    # Check current value
    case get_current(project, instance, table, key, family, qualifier, opts) do
      {:ok, nil} ->
        # No current value, safe to increment
        increment(project, instance, table, key, family, qualifier, opts)

      {:ok, count} when count < limit ->
        # Under limit, increment
        increment(project, instance, table, key, family, qualifier, opts)

      {:ok, _count} ->
        # At or over limit
        reset_at = calculate_reset_time(bucket)
        {:error, :rate_limited, reset_at}

      {:error, reason} ->
        {:error, reason}
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
  @spec build_row_key(String.t(), atom(), integer()) :: String.t()
  def build_row_key(key, bucket, timestamp \\ nil) do
    ts = timestamp || System.system_time(:second)
    bucket_seconds = bucket_to_seconds(bucket)
    bucket_start = div(ts, bucket_seconds) * bucket_seconds
    "#{key}##{bucket_start}"
  end

  @doc """
  Parses a time-bucketed row key.

  ## Examples

      {:ok, %{key: "user#123", bucket_timestamp: 1704067200}} =
        MegasPinakas.CounterTTL.parse_row_key("user#123#1704067200")
  """
  @spec parse_row_key(String.t()) :: {:ok, map()} | {:error, :invalid_format}
  def parse_row_key(row_key) do
    case String.split(row_key, "#") |> Enum.reverse() do
      [timestamp_str | rest] ->
        key = rest |> Enum.reverse() |> Enum.join("#")

        case Integer.parse(timestamp_str) do
          {timestamp, ""} ->
            {:ok, %{key: key, bucket_timestamp: timestamp}}

          _ ->
            {:error, :invalid_format}
        end

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
  def bucket_to_seconds(:day), do: 86400
  def bucket_to_seconds(:week), do: 604_800

  # ============================================================================
  # Private Helpers
  # ============================================================================

  defp extract_counter_value(response, family, qualifier) do
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

  defp calculate_reset_time(bucket) do
    now = System.system_time(:second)
    bucket_seconds = bucket_to_seconds(bucket)
    current_bucket = div(now, bucket_seconds) * bucket_seconds
    next_bucket = current_bucket + bucket_seconds
    DateTime.from_unix!(next_bucket)
  end
end
