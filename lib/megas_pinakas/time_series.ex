defmodule MegasPinakas.TimeSeries do
  @moduledoc """
  Time-series data patterns for metrics, events, and logs.

  Uses reverse timestamp row keys for efficient recent-first queries.
  Row key design: `<metric_id>#<reverse_timestamp>`

  Reverse timestamp = `max_timestamp - actual_timestamp` where max_timestamp
  is a large constant (e.g., 9999999999999999), ensuring recent data sorts first.

  ## Examples

      # Write a data point
      {:ok, _} = MegasPinakas.TimeSeries.write_point(
        project, instance, "metrics", "cpu:server1",
        %{value: 0.85, host: "srv1"}
      )

      # Query recent points
      {:ok, points} = MegasPinakas.TimeSeries.query_recent(
        project, instance, "metrics", "cpu:server1",
        limit: 100
      )

      # Query time range
      {:ok, points} = MegasPinakas.TimeSeries.query_range(
        project, instance, "metrics", "cpu:server1",
        ~U[2024-01-01 00:00:00Z], ~U[2024-01-02 00:00:00Z]
      )
  """

  alias MegasPinakas
  alias MegasPinakas.Types
  alias MegasPinakas.Row

  # Max timestamp for reverse ordering (year 2286 in microseconds)
  @max_timestamp 9_999_999_999_999_999

  @default_family "data"
  @value_qualifier "value"
  @timestamp_qualifier "ts"
  @tags_qualifier "tags"

  # ============================================================================
  # Write Operations
  # ============================================================================

  @doc """
  Writes a single data point.

  ## Options

    * `:timestamp` - DateTime for the point (default: now)
    * `:family` - Column family (default: "data")
    * `:tags` - Map of tags/labels for the point

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

    row_key = time_series_row_key(metric_id, timestamp)
    value = Map.get(data, :value)
    tags = Map.get(data, :tags, %{})

    row =
      Row.new(row_key)
      |> Row.put_datetime(family, @timestamp_qualifier, timestamp)

    row =
      cond do
        is_float(value) -> Row.put_float(row, family, @value_qualifier, value)
        is_integer(value) -> Row.put_integer(row, family, @value_qualifier, value)
        is_binary(value) -> Row.put_string(row, family, @value_qualifier, value)
        true -> Row.put_json(row, family, @value_qualifier, value)
      end

    row =
      if map_size(tags) > 0 do
        Row.put_json(row, family, @tags_qualifier, tags)
      else
        row
      end

    Row.write(row, project, instance, table, opts)
  end

  @doc """
  Writes multiple data points in a batch.

  ## Examples

      points = [
        %{metric_id: "cpu:server1", value: 0.85, timestamp: ~U[2024-01-15 10:00:00Z]},
        %{metric_id: "cpu:server2", value: 0.92, timestamp: ~U[2024-01-15 10:00:00Z]}
      ]
      MegasPinakas.TimeSeries.write_points(project, instance, "metrics", points)
  """
  @spec write_points(String.t(), String.t(), String.t(), [map()], keyword()) ::
          {:ok, term()} | {:error, term()}
  def write_points(project, instance, table, points, opts \\ []) when is_list(points) do
    family = Keyword.get(opts, :family, @default_family)

    entries =
      Enum.map(points, fn point ->
        metric_id = Map.fetch!(point, :metric_id)
        timestamp = Map.get(point, :timestamp, DateTime.utc_now())
        value = Map.get(point, :value)
        tags = Map.get(point, :tags, %{})

        row_key = time_series_row_key(metric_id, timestamp)

        row =
          Row.new(row_key)
          |> Row.put_datetime(family, @timestamp_qualifier, timestamp)

        row =
          cond do
            is_float(value) -> Row.put_float(row, family, @value_qualifier, value)
            is_integer(value) -> Row.put_integer(row, family, @value_qualifier, value)
            is_binary(value) -> Row.put_string(row, family, @value_qualifier, value)
            true -> Row.put_json(row, family, @value_qualifier, value)
          end

        row =
          if map_size(tags) > 0 do
            Row.put_json(row, family, @tags_qualifier, tags)
          else
            row
          end

        Row.to_entry(row)
      end)

    MegasPinakas.mutate_rows(project, instance, table, entries, opts)
  end

  # ============================================================================
  # Query Operations
  # ============================================================================

  @doc """
  Queries the most recent data points for a metric.

  ## Options

    * `:limit` - Maximum number of points to return (default: 100)
    * `:family` - Column family (default: "data")

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
    family = Keyword.get(opts, :family, @default_family)

    # Use prefix range for this metric
    row_range = MegasPinakas.row_range_prefix("#{metric_id}#")

    read_opts =
      opts
      |> Keyword.put(:rows, MegasPinakas.row_set_from_ranges([row_range]))
      |> Keyword.put(:rows_limit, limit)

    case MegasPinakas.read_rows(project, instance, table, read_opts) do
      {:ok, rows} ->
        points = Enum.map(rows, fn row -> parse_point(row, family) end)
        {:ok, points}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Queries data points within a time range.

  ## Options

    * `:family` - Column family (default: "data")
    * `:limit` - Maximum number of points

  ## Examples

      {:ok, points} = MegasPinakas.TimeSeries.query_range(
        project, instance, "metrics", "cpu:server1",
        ~U[2024-01-01 00:00:00Z], ~U[2024-01-02 00:00:00Z]
      )
  """
  @spec query_range(String.t(), String.t(), String.t(), String.t(), DateTime.t(), DateTime.t(), keyword()) ::
          {:ok, [map()]} | {:error, term()}
  def query_range(project, instance, table, metric_id, start_time, end_time, opts \\ []) do
    family = Keyword.get(opts, :family, @default_family)

    # For reverse timestamps, end_time becomes start_key and start_time becomes end_key
    start_key = "#{metric_id}##{reverse_timestamp(end_time)}"
    end_key = "#{metric_id}##{reverse_timestamp(start_time)}"

    row_range = MegasPinakas.row_range(start_key, end_key)

    read_opts =
      opts
      |> Keyword.put(:rows, MegasPinakas.row_set_from_ranges([row_range]))

    case MegasPinakas.read_rows(project, instance, table, read_opts) do
      {:ok, rows} ->
        points = Enum.map(rows, fn row -> parse_point(row, family) end)
        {:ok, points}

      {:error, reason} ->
        {:error, reason}
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
      # => "9998290376400000000"
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

      {:ok, dt} = MegasPinakas.TimeSeries.from_reverse_timestamp("9998290376400000000")
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

      {:ok, %{metric_id: "cpu:server1", timestamp: ~U[...]}} =
        MegasPinakas.TimeSeries.parse_row_key("cpu:server1#9998290376400000000")
  """
  @spec parse_row_key(String.t()) :: {:ok, map()} | {:error, term()}
  def parse_row_key(row_key) do
    case String.split(row_key, "#") |> Enum.reverse() do
      [reverse_ts | rest] ->
        metric_id = rest |> Enum.reverse() |> Enum.join("#")

        case from_reverse_timestamp(reverse_ts) do
          {:ok, timestamp} ->
            {:ok, %{metric_id: metric_id, timestamp: timestamp}}

          {:error, reason} ->
            {:error, reason}
        end

      _ ->
        {:error, :invalid_format}
    end
  end

  # ============================================================================
  # Private Helpers
  # ============================================================================

  defp parse_point(row, family) do
    row_key = MegasPinakas.row_key(row)

    timestamp =
      case MegasPinakas.get_cell(row, family, @timestamp_qualifier) do
        nil -> nil
        data -> decode_datetime(data)
      end

    value =
      case MegasPinakas.get_cell(row, family, @value_qualifier) do
        nil -> nil
        data -> decode_value(data)
      end

    tags =
      case MegasPinakas.get_cell(row, family, @tags_qualifier) do
        nil -> %{}
        data -> decode_json(data)
      end

    %{
      row_key: row_key,
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

  defp decode_value(data) do
    # Try different decodings
    case Types.decode(:float, data) do
      {:ok, v} ->
        v

      {:error, _} ->
        case Types.decode(:integer, data) do
          {:ok, v} -> v
          {:error, _} -> data
        end
    end
  end

  defp decode_json(data) do
    case Types.decode(:json, data) do
      {:ok, v} -> v
      {:error, _} -> %{}
    end
  end
end
