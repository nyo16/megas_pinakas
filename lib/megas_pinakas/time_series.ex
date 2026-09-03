defmodule MegasPinakas.TimeSeries do
  @moduledoc """
  Time-series data patterns for metrics, events, and logs.

  Uses reverse timestamp row keys for efficient recent-first queries.
  Row key design: `<metric_id>#<reverse_timestamp>`

  Reverse timestamp = `max_timestamp - actual_timestamp` in microseconds, where
  `max_timestamp` is `9_999_999_999_999_999` (year 2286), zero-padded to 19
  digits so keys sort lexicographically with the most recent point first.

  ## Value encoding

  Each point stores its value in the `value` column and the value's type in a
  sibling `value_type` column holding one of `"i"` (integer), `"f"` (float),
  `"s"` (string) or `"j"` (JSON: maps, lists, booleans). Queries decode the
  value according to that tag, so an integer written comes back as an integer,
  never as a float that happens to share the same byte width. A point whose tag
  is missing or unrecognised is returned with its raw binary value.

  `nil` cannot be stored as a value; `write_point/6` and `write_points/5`
  return `{:error, :nil_value}` rather than writing a point with no value.

  ## Examples

      # Write a data point
      {:ok, _} = MegasPinakas.TimeSeries.write_point(
        project, instance, "metrics", "cpu:server1",
        %{value: 0.85, tags: %{host: "srv1"}}
      )

      # Query recent points
      {:ok, points} = MegasPinakas.TimeSeries.query_recent(
        project, instance, "metrics", "cpu:server1",
        limit: 100
      )

      # Query the half-open time range [start, end)
      {:ok, points} = MegasPinakas.TimeSeries.query_range(
        project, instance, "metrics", "cpu:server1",
        ~U[2024-01-01 00:00:00Z], ~U[2024-01-02 00:00:00Z]
      )
  """

  alias Google.Bigtable.V2.RowRange
  alias MegasPinakas
  alias MegasPinakas.Row
  alias MegasPinakas.RowKey
  alias MegasPinakas.Types

  # Max timestamp for reverse ordering (year 2286 in microseconds)
  @max_timestamp 9_999_999_999_999_999

  @default_family "data"
  @value_qualifier "value"
  @type_qualifier "value_type"
  @timestamp_qualifier "ts"
  @tags_qualifier "tags"

  # ============================================================================
  # Write Operations
  # ============================================================================

  @doc """
  Writes a single data point.

  `data` is a map with a required `:value` (integer, float, string, boolean,
  map or list) and an optional `:tags` map.

  Returns `{:error, :nil_value}` when `:value` is missing or `nil`, and
  `{:error, {:unsupported_value, value}}` for any other term that cannot be
  encoded, in both cases without issuing a request.

  ## Options

    * `:timestamp` - DateTime for the point (default: now)
    * `:family` - Column family (default: "data")
    * `:app_profile_id` - App profile to use

  ## Examples

      MegasPinakas.TimeSeries.write_point(
        project, instance, "metrics", "cpu:server1",
        %{value: 0.85, tags: %{host: "srv1", region: "us-east"}}
      )
  """
  @spec write_point(String.t(), String.t(), String.t(), String.t(), map(), keyword()) ::
          {:ok, term()} | {:error, term()}
  def write_point(project, instance, table, metric_id, data, opts \\ []) do
    timestamp = Keyword.get(opts, :timestamp, DateTime.utc_now())
    family = Keyword.get(opts, :family, @default_family)

    with {:ok, row} <- build_row(metric_id, timestamp, data, family) do
      Row.write(row, project, instance, table, opts)
    end
  end

  @doc """
  Writes multiple data points in a batch.

  Each point is a map with `:metric_id`, `:value` and optional `:timestamp` and
  `:tags`, encoded as described in `write_point/6`. If any point has a `nil` or
  unsupported value the whole call returns that point's error and nothing is
  written.

  Returns one result per point, ordered to match `points`. `{:ok, results}` means
  the RPC succeeded, **not** that every point was written — check
  `&1.status.code == 0` per entry.

  ## Examples

      points = [
        %{metric_id: "cpu:server1", value: 0.85, timestamp: ~U[2024-01-15 10:00:00Z]},
        %{metric_id: "cpu:server2", value: 0.92, timestamp: ~U[2024-01-15 10:00:00Z]}
      ]
      {:ok, results} = MegasPinakas.TimeSeries.write_points(project, instance, "metrics", points)

  > #### Breaking change in 0.6.0 {: .warning}
  >
  > Previously returned an unconsumed `#Stream<>`, so failed writes were silently
  > discarded unless the caller enumerated it.
  """
  @spec write_points(String.t(), String.t(), String.t(), [map()], keyword()) ::
          {:ok, [%Google.Bigtable.V2.MutateRowsResponse.Entry{}]} | {:error, term()}
  def write_points(project, instance, table, points, opts \\ []) when is_list(points) do
    family = Keyword.get(opts, :family, @default_family)

    entries =
      Enum.reduce_while(points, {:ok, []}, fn point, {:ok, acc} ->
        metric_id = Map.fetch!(point, :metric_id)
        timestamp = Map.get(point, :timestamp, DateTime.utc_now())

        case build_row(metric_id, timestamp, point, family) do
          {:ok, row} -> {:cont, {:ok, [Row.to_entry(row) | acc]}}
          {:error, _} = error -> {:halt, error}
        end
      end)

    with {:ok, reversed} <- entries do
      MegasPinakas.mutate_rows(project, instance, table, Enum.reverse(reversed), opts)
    end
  end

  # ============================================================================
  # Query Operations
  # ============================================================================

  @doc """
  Queries the most recent data points for a metric.

  Points are returned most recent first. Each point is a map with `:row_key`,
  `:timestamp`, `:value` (decoded per its type tag) and `:tags`.

  ## Options

    * `:limit` - Maximum number of points to return (default: 100)
    * `:family` - Column family (default: "data")
    * `:app_profile_id` - App profile to use

  ## Examples

      {:ok, points} = MegasPinakas.TimeSeries.query_recent(
        project, instance, "metrics", "cpu:server1",
        limit: 50
      )
  """
  @spec query_recent(String.t(), String.t(), String.t(), String.t(), keyword()) ::
          {:ok, [map()]} | {:error, term()}
  def query_recent(project, instance, table, metric_id, opts \\ []) do
    limit = Keyword.get(opts, :limit, 100)

    # Use prefix range for this metric
    row_range = MegasPinakas.row_range_prefix("#{metric_id}#")

    read_points(project, instance, table, row_range, limit, opts)
  end

  @doc """
  Queries data points within the half-open time range `[start_time, end_time)`.

  A point stamped exactly `start_time` is included; one stamped exactly
  `end_time` is not, so adjacent ranges partition a series without overlap.
  Points are returned most recent first. An empty range (`start_time ==
  end_time`) returns `{:ok, []}` without a request; an inverted range
  (`start_time > end_time`) raises `ArgumentError`, because BigTable rejects a
  row range whose start is not below its end.

  ## Options

    * `:limit` - Maximum number of points to return (default: unlimited)
    * `:family` - Column family (default: "data")
    * `:app_profile_id` - App profile to use

  ## Examples

      {:ok, points} = MegasPinakas.TimeSeries.query_range(
        project, instance, "metrics", "cpu:server1",
        ~U[2024-01-01 00:00:00Z], ~U[2024-01-02 00:00:00Z]
      )
  """
  @spec query_range(
          String.t(),
          String.t(),
          String.t(),
          String.t(),
          DateTime.t(),
          DateTime.t(),
          keyword()
        ) ::
          {:ok, [map()]} | {:error, term()}
  def query_range(project, instance, table, metric_id, start_time, end_time, opts \\ []) do
    # BigTable answers start_key >= end_key with INVALID_ARGUMENT (the emulator
    # silently returns nothing), so settle both degenerate cases client-side.
    case DateTime.compare(start_time, end_time) do
      :eq ->
        {:ok, []}

      :gt ->
        raise ArgumentError,
              "query_range/7 start_time #{inspect(start_time)} is after end_time #{inspect(end_time)}"

      :lt ->
        limit = Keyword.get(opts, :limit, 0)

        # Reverse timestamps flip the order: the later `end_time` becomes the
        # smaller key. Excluding it (open start) and including `start_time`
        # (closed end) yields [start_time, end_time) in wall-clock terms.
        row_range = %RowRange{
          start_key: {:start_key_open, time_series_row_key(metric_id, end_time)},
          end_key: {:end_key_closed, time_series_row_key(metric_id, start_time)}
        }

        read_points(project, instance, table, row_range, limit, opts)
    end
  end

  # ============================================================================
  # Row Key Helpers
  # ============================================================================

  @doc """
  Builds a time-series row key with reverse timestamp for recent-first ordering.

  ## Examples

      MegasPinakas.TimeSeries.time_series_row_key("cpu:server1", ~U[2024-01-15 10:00:00Z])
      # => "cpu:server1#<reverse_timestamp>"
  """
  @spec time_series_row_key(String.t(), DateTime.t()) :: String.t()
  def time_series_row_key(metric_id, %DateTime{} = timestamp) do
    "#{metric_id}##{reverse_timestamp(timestamp)}"
  end

  @doc """
  Converts a timestamp to a reverse timestamp for recent-first ordering.

  ## Examples

      MegasPinakas.TimeSeries.reverse_timestamp(~U[2024-01-15 10:00:00Z])
      # => "0008294687199999999"
  """
  @spec reverse_timestamp(DateTime.t()) :: String.t()
  def reverse_timestamp(%DateTime{} = timestamp) do
    micros = DateTime.to_unix(timestamp, :microsecond)
    reverse = @max_timestamp - micros
    # Pad to fixed width for proper sorting
    String.pad_leading(Integer.to_string(reverse), 19, "0")
  end

  @doc """
  Converts a reverse timestamp string back to a DateTime.

  ## Examples

      {:ok, dt} = MegasPinakas.TimeSeries.from_reverse_timestamp("0008294687199999999")
  """
  @spec from_reverse_timestamp(String.t()) :: {:ok, DateTime.t()} | {:error, term()}
  def from_reverse_timestamp(reverse_str) do
    case Integer.parse(reverse_str) do
      {reverse, ""} ->
        micros = @max_timestamp - reverse
        DateTime.from_unix(micros, :microsecond)

      _ ->
        {:error, :invalid_format}
    end
  end

  @doc """
  Parses a time-series row key.

  ## Examples

      {:ok, %{metric_id: "cpu:server1", timestamp: ~U[2024-01-15 10:00:00.000000Z]}} =
        MegasPinakas.TimeSeries.parse_row_key("cpu:server1#0008294687199999999")
  """
  @spec parse_row_key(String.t()) :: {:ok, map()} | {:error, term()}
  def parse_row_key(row_key) do
    {metric_id, reverse_ts} = RowKey.split_suffix(row_key)

    case from_reverse_timestamp(reverse_ts) do
      {:ok, timestamp} ->
        {:ok, %{metric_id: metric_id, timestamp: timestamp}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # ============================================================================
  # Private Helpers
  # ============================================================================

  defp build_row(metric_id, %DateTime{} = timestamp, data, family) do
    with {:ok, {tag, bytes}} <- encode_value(Map.get(data, :value)) do
      row =
        metric_id
        |> time_series_row_key(timestamp)
        |> Row.new()
        |> Row.put_datetime(family, @timestamp_qualifier, timestamp)
        |> Row.put_binary(family, @value_qualifier, bytes)
        |> Row.put_string(family, @type_qualifier, tag)

      case Map.get(data, :tags, %{}) do
        tags when map_size(tags) > 0 -> {:ok, Row.put_json(row, family, @tags_qualifier, tags)}
        _ -> {:ok, row}
      end
    end
  end

  defp encode_value(nil), do: {:error, :nil_value}
  defp encode_value(v) when is_integer(v), do: {:ok, {"i", Types.encode(:integer, v)}}
  defp encode_value(v) when is_float(v), do: {:ok, {"f", Types.encode(:float, v)}}
  defp encode_value(v) when is_binary(v), do: {:ok, {"s", v}}
  # Booleans, maps and lists go through Jason directly: `Types.encode(:json)`
  # only accepts maps/lists and raises on unencodable terms, but the contract
  # here is an error tuple, never a raise.
  defp encode_value(v) when is_boolean(v) or is_map(v) or is_list(v) do
    case Jason.encode(v) do
      {:ok, json} -> {:ok, {"j", json}}
      {:error, _} -> {:error, {:unsupported_value, v}}
    end
  end

  defp encode_value(v), do: {:error, {:unsupported_value, v}}

  defp read_points(project, instance, table, row_range, limit, opts) do
    family = Keyword.get(opts, :family, @default_family)

    # Only :app_profile_id is forwarded. A caller filter could drop the
    # value_type/ts columns and silently degrade decoding.
    read_opts =
      opts
      |> Keyword.take([:app_profile_id])
      |> Keyword.put(:rows, MegasPinakas.row_set_from_ranges([row_range]))
      |> Keyword.put(:rows_limit, limit)

    case MegasPinakas.read_rows(project, instance, table, read_opts) do
      {:ok, rows} -> {:ok, Enum.map(rows, &parse_point(&1, family))}
      {:error, reason} -> {:error, reason}
    end
  end

  defp parse_point(row, family) do
    timestamp =
      case MegasPinakas.get_cell(row, family, @timestamp_qualifier) do
        nil -> nil
        data -> decode_datetime(data)
      end

    value =
      case MegasPinakas.get_cell(row, family, @value_qualifier) do
        nil -> nil
        data -> decode_value(data, MegasPinakas.get_cell(row, family, @type_qualifier))
      end

    tags =
      case MegasPinakas.get_cell(row, family, @tags_qualifier) do
        nil -> %{}
        data -> decode_json(data)
      end

    %{
      row_key: MegasPinakas.row_key(row),
      timestamp: timestamp,
      value: value,
      tags: tags
    }
  end

  defp decode_datetime(data) do
    case Types.decode(:datetime, data) do
      {:ok, dt} -> dt
      {:error, _} -> nil
    end
  end

  # Decoded per the stored type tag; a missing or unknown tag, or bytes that do
  # not decode as tagged, fall back to the raw binary rather than a guess.
  defp decode_value(data, "i"), do: decode_or_raw(:integer, data)
  defp decode_value(data, "f"), do: decode_or_raw(:float, data)
  defp decode_value(data, "s"), do: data
  defp decode_value(data, "j"), do: decode_or_raw(:json, data)
  defp decode_value(data, _tag), do: data

  defp decode_or_raw(type, data) do
    case Types.decode(type, data) do
      {:ok, v} -> v
      {:error, _} -> data
    end
  end

  defp decode_json(data) do
    case Types.decode(:json, data) do
      {:ok, v} -> v
      {:error, _} -> %{}
    end
  end
end
