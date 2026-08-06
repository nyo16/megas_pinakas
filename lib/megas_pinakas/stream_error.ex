defmodule MegasPinakas.StreamError do
  @moduledoc """
  Raised when a lazily-consumed BigTable stream fails partway through.

  A lazy stream has no return value to carry an error tuple, so the only way to
  tell a consumer that iteration ended early is to raise. Halting quietly would
  make `Enum.to_list/1` return partial data that is indistinguishable from a
  complete result — the same silent-truncation failure that `read_rows/4` avoids
  by returning `{:error, {:incomplete_read, reason}}`.

  Rescue it if partial results are acceptable for your use case:

      try do
        Streaming.stream_rows(project, instance, table) |> Enum.to_list()
      rescue
        e in MegasPinakas.StreamError -> {:partial, e.reason}
      end
  """

  defexception [:reason, :last_key]

  @type t :: %__MODULE__{reason: term(), last_key: binary() | nil}

  @impl true
  def message(%__MODULE__{reason: reason, last_key: nil}) do
    "BigTable stream failed before any row was read: #{inspect(reason)}"
  end

  @impl true
  def message(%__MODULE__{reason: reason, last_key: last_key}) do
    """
    BigTable stream failed after row #{inspect(last_key)}: #{inspect(reason)}

    Rows up to and including that key were delivered; the rest were not. \
    Resume from this key rather than restarting if the read is idempotent.
    """
  end
end
