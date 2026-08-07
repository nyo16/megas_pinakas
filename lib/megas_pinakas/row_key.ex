defmodule MegasPinakas.RowKey do
  @moduledoc false

  # Shared row-key parsing for the modules that encode a trailing segment into
  # the key: `MegasPinakas.CounterTTL` (`<key>#<bucket_timestamp>`) and
  # `MegasPinakas.TimeSeries` (`<metric_id>#<reverse_timestamp>`). Both need the
  # same split, and only differ in how they interpret the trailing segment.

  @separator "#"

  @doc """
  Splits a row key into `{prefix, suffix}` on the last `#`.

  Separators inside the prefix are preserved, so `"api#v1#endpoint#123"` yields
  `{"api#v1#endpoint", "123"}`. A key with no separator yields an empty prefix.
  """
  @spec split_suffix(String.t()) :: {String.t(), String.t()}
  def split_suffix(row_key) when is_binary(row_key) do
    [suffix | reversed_prefix] =
      row_key
      |> String.split(@separator)
      |> Enum.reverse()

    prefix =
      reversed_prefix
      |> Enum.reverse()
      |> Enum.join(@separator)

    {prefix, suffix}
  end
end
