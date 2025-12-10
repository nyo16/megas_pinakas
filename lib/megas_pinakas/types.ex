defmodule MegasPinakas.Types do
  @moduledoc """
  Type-aware cell operations with automatic serialization/deserialization.

  Supports JSON, binary, integers, floats, booleans, timestamps, and Erlang terms.
  All encoding formats are designed to be sortable where applicable (big-endian integers).

  ## Supported Types

  - `:binary` - Raw bytes, no encoding
  - `:string` - UTF-8 encoded string (same as binary but semantically different)
  - `:json` - Maps and lists encoded as JSON strings
  - `:integer` - 64-bit signed big-endian integers (sortable)
  - `:float` - 64-bit IEEE 754 floats
  - `:boolean` - Single byte: <<1>> for true, <<0>> for false
  - `:datetime` - Microseconds since Unix epoch as 64-bit big-endian
  - `:term` - Any Elixir term via `:erlang.term_to_binary/1`

  ## Examples

      # Write a JSON document
      MegasPinakas.Types.write_json(project, instance, "users", "user#123", "cf", "profile", %{name: "John", age: 30})

      # Read it back
      {:ok, profile} = MegasPinakas.Types.read_json(project, instance, "users", "user#123", "cf", "profile")
      # => {:ok, %{"name" => "John", "age" => 30}}

      # Write multiple typed cells
      MegasPinakas.Types.write_cells(project, instance, "users", "user#123", [
        {:string, "cf", "name", "John Doe"},
        {:integer, "cf", "age", 30},
        {:datetime, "cf", "created", DateTime.utc_now()}
      ])
  """

  alias MegasPinakas
  alias Google.Bigtable.V2.Mutation

  # ============================================================================
  # Encoding/Decoding Helpers
  # ============================================================================

  @doc """
  Encodes a value to binary based on its type.

  ## Examples

      iex> MegasPinakas.Types.encode(:integer, 42)
      <<0, 0, 0, 0, 0, 0, 0, 42>>

      iex> MegasPinakas.Types.encode(:boolean, true)
      <<1>>

      iex> MegasPinakas.Types.encode(:json, %{a: 1})
      ~s({"a":1})
  """
  @spec encode(atom(), term()) :: binary()
  def encode(:binary, value) when is_binary(value), do: value
  def encode(:string, value) when is_binary(value), do: value

  def encode(:json, value) when is_map(value) or is_list(value) do
    Jason.encode!(value)
  end

  def encode(:integer, value) when is_integer(value) do
    <<value::signed-big-64>>
  end

  def encode(:float, value) when is_float(value) do
    <<value::float-64>>
  end

  def encode(:boolean, true), do: <<1>>
  def encode(:boolean, false), do: <<0>>

  def encode(:datetime, %DateTime{} = dt) do
    micros = DateTime.to_unix(dt, :microsecond)
    <<micros::signed-big-64>>
  end

  def encode(:term, value) do
    :erlang.term_to_binary(value)
  end

  @doc """
  Decodes a binary value to its typed representation.

  Returns `{:ok, value}` on success or `{:error, reason}` on failure.

  ## Examples

      iex> MegasPinakas.Types.decode(:integer, <<0, 0, 0, 0, 0, 0, 0, 42>>)
      {:ok, 42}

      iex> MegasPinakas.Types.decode(:boolean, <<1>>)
      {:ok, true}
  """
  @spec decode(atom(), binary()) :: {:ok, term()} | {:error, term()}
  def decode(:binary, value) when is_binary(value), do: {:ok, value}
  def decode(:string, value) when is_binary(value), do: {:ok, value}

  def decode(:json, value) when is_binary(value) do
    Jason.decode(value)
  end

  def decode(:integer, <<value::signed-big-64>>) do
    {:ok, value}
  end

  def decode(:integer, _), do: {:error, :invalid_integer_format}

  def decode(:float, <<value::float-64>>) do
    {:ok, value}
  end

  def decode(:float, _), do: {:error, :invalid_float_format}

  def decode(:boolean, <<1>>), do: {:ok, true}
  def decode(:boolean, <<0>>), do: {:ok, false}
  def decode(:boolean, _), do: {:error, :invalid_boolean_format}

  def decode(:datetime, <<micros::signed-big-64>>) do
    case DateTime.from_unix(micros, :microsecond) do
      {:ok, dt} -> {:ok, dt}
      {:error, reason} -> {:error, reason}
    end
  end

  def decode(:datetime, _), do: {:error, :invalid_datetime_format}

  def decode(:term, value) when is_binary(value) do
    try do
      {:ok, :erlang.binary_to_term(value, [:safe])}
    rescue
      ArgumentError -> {:error, :invalid_term_format}
    end
  end

  @doc """
  Decodes a binary value to its typed representation, raising on error.

  ## Examples

      iex> MegasPinakas.Types.decode!(:integer, <<0, 0, 0, 0, 0, 0, 0, 42>>)
      42
  """
  @spec decode!(atom(), binary()) :: term()
  def decode!(type, value) do
    case decode(type, value) do
      {:ok, result} -> result
      {:error, reason} -> raise ArgumentError, "Failed to decode #{type}: #{inspect(reason)}"
    end
  end

  # ============================================================================
  # Typed Mutation Builders
  # ============================================================================

  @doc """
  Creates a SetCell mutation with a JSON-encoded value.

  ## Examples

      mutation = MegasPinakas.Types.set_json("cf", "data", %{name: "John"})
  """
  @spec set_json(String.t(), String.t(), map() | list(), keyword()) :: Mutation.t()
  def set_json(family, qualifier, data, opts \\ []) when is_map(data) or is_list(data) do
    MegasPinakas.set_cell(family, qualifier, encode(:json, data), opts)
  end

  @doc """
  Creates a SetCell mutation with a 64-bit big-endian integer value.
  """
  @spec set_integer(String.t(), String.t(), integer(), keyword()) :: Mutation.t()
  def set_integer(family, qualifier, integer, opts \\ []) when is_integer(integer) do
    MegasPinakas.set_cell(family, qualifier, encode(:integer, integer), opts)
  end

  @doc """
  Creates a SetCell mutation with a 64-bit float value.
  """
  @spec set_float(String.t(), String.t(), float(), keyword()) :: Mutation.t()
  def set_float(family, qualifier, float, opts \\ []) when is_float(float) do
    MegasPinakas.set_cell(family, qualifier, encode(:float, float), opts)
  end

  @doc """
  Creates a SetCell mutation with a boolean value.
  """
  @spec set_boolean(String.t(), String.t(), boolean(), keyword()) :: Mutation.t()
  def set_boolean(family, qualifier, bool, opts \\ []) when is_boolean(bool) do
    MegasPinakas.set_cell(family, qualifier, encode(:boolean, bool), opts)
  end

  @doc """
  Creates a SetCell mutation with a DateTime value (microseconds since epoch).
  """
  @spec set_datetime(String.t(), String.t(), DateTime.t(), keyword()) :: Mutation.t()
  def set_datetime(family, qualifier, %DateTime{} = datetime, opts \\ []) do
    MegasPinakas.set_cell(family, qualifier, encode(:datetime, datetime), opts)
  end

  @doc """
  Creates a SetCell mutation with any Elixir term (via erlang term_to_binary).
  """
  @spec set_term(String.t(), String.t(), term(), keyword()) :: Mutation.t()
  def set_term(family, qualifier, term, opts \\ []) do
    MegasPinakas.set_cell(family, qualifier, encode(:term, term), opts)
  end

  # ============================================================================
  # Single-Cell Write Operations
  # ============================================================================

  @doc """
  Writes a raw binary value to a cell.
  """
  @spec write_binary(String.t(), String.t(), String.t(), String.t(), String.t(), String.t(), binary(), keyword()) ::
          {:ok, term()} | {:error, term()}
  def write_binary(project, instance, table, row_key, family, qualifier, binary, opts \\ [])
      when is_binary(binary) do
    mutations = [MegasPinakas.set_cell(family, qualifier, binary, opts)]
    MegasPinakas.mutate_row(project, instance, table, row_key, mutations, opts)
  end

  @doc """
  Writes a UTF-8 string value to a cell.
  """
  @spec write_string(String.t(), String.t(), String.t(), String.t(), String.t(), String.t(), String.t(), keyword()) ::
          {:ok, term()} | {:error, term()}
  def write_string(project, instance, table, row_key, family, qualifier, string, opts \\ [])
      when is_binary(string) do
    write_binary(project, instance, table, row_key, family, qualifier, string, opts)
  end

  @doc """
  Writes a JSON-encoded map or list to a cell.
  """
  @spec write_json(String.t(), String.t(), String.t(), String.t(), String.t(), String.t(), map() | list(), keyword()) ::
          {:ok, term()} | {:error, term()}
  def write_json(project, instance, table, row_key, family, qualifier, data, opts \\ [])
      when is_map(data) or is_list(data) do
    mutations = [set_json(family, qualifier, data, opts)]
    MegasPinakas.mutate_row(project, instance, table, row_key, mutations, opts)
  end

  @doc """
  Writes a 64-bit signed integer to a cell (big-endian for sortability).
  """
  @spec write_integer(String.t(), String.t(), String.t(), String.t(), String.t(), String.t(), integer(), keyword()) ::
          {:ok, term()} | {:error, term()}
  def write_integer(project, instance, table, row_key, family, qualifier, integer, opts \\ [])
      when is_integer(integer) do
    mutations = [set_integer(family, qualifier, integer, opts)]
    MegasPinakas.mutate_row(project, instance, table, row_key, mutations, opts)
  end

  @doc """
  Writes a 64-bit float to a cell.
  """
  @spec write_float(String.t(), String.t(), String.t(), String.t(), String.t(), String.t(), float(), keyword()) ::
          {:ok, term()} | {:error, term()}
  def write_float(project, instance, table, row_key, family, qualifier, float, opts \\ [])
      when is_float(float) do
    mutations = [set_float(family, qualifier, float, opts)]
    MegasPinakas.mutate_row(project, instance, table, row_key, mutations, opts)
  end

  @doc """
  Writes a boolean to a cell.
  """
  @spec write_boolean(String.t(), String.t(), String.t(), String.t(), String.t(), String.t(), boolean(), keyword()) ::
          {:ok, term()} | {:error, term()}
  def write_boolean(project, instance, table, row_key, family, qualifier, bool, opts \\ [])
      when is_boolean(bool) do
    mutations = [set_boolean(family, qualifier, bool, opts)]
    MegasPinakas.mutate_row(project, instance, table, row_key, mutations, opts)
  end

  @doc """
  Writes a DateTime to a cell (microseconds since Unix epoch).
  """
  @spec write_datetime(String.t(), String.t(), String.t(), String.t(), String.t(), String.t(), DateTime.t(), keyword()) ::
          {:ok, term()} | {:error, term()}
  def write_datetime(project, instance, table, row_key, family, qualifier, %DateTime{} = datetime, opts \\ []) do
    mutations = [set_datetime(family, qualifier, datetime, opts)]
    MegasPinakas.mutate_row(project, instance, table, row_key, mutations, opts)
  end

  @doc """
  Writes any Elixir term to a cell (via erlang term_to_binary).
  """
  @spec write_term(String.t(), String.t(), String.t(), String.t(), String.t(), String.t(), term(), keyword()) ::
          {:ok, term()} | {:error, term()}
  def write_term(project, instance, table, row_key, family, qualifier, term, opts \\ []) do
    mutations = [set_term(family, qualifier, term, opts)]
    MegasPinakas.mutate_row(project, instance, table, row_key, mutations, opts)
  end

  # ============================================================================
  # Single-Cell Read Operations
  # ============================================================================

  @doc """
  Reads a raw binary value from a cell.
  """
  @spec read_binary(String.t(), String.t(), String.t(), String.t(), String.t(), String.t(), keyword()) ::
          {:ok, binary() | nil} | {:error, term()}
  def read_binary(project, instance, table, row_key, family, qualifier, opts \\ []) do
    read_cell_as(project, instance, table, row_key, family, qualifier, :binary, opts)
  end

  @doc """
  Reads a UTF-8 string value from a cell.
  """
  @spec read_string(String.t(), String.t(), String.t(), String.t(), String.t(), String.t(), keyword()) ::
          {:ok, String.t() | nil} | {:error, term()}
  def read_string(project, instance, table, row_key, family, qualifier, opts \\ []) do
    read_cell_as(project, instance, table, row_key, family, qualifier, :string, opts)
  end

  @doc """
  Reads and decodes a JSON value from a cell.
  """
  @spec read_json(String.t(), String.t(), String.t(), String.t(), String.t(), String.t(), keyword()) ::
          {:ok, map() | list() | nil} | {:error, term()}
  def read_json(project, instance, table, row_key, family, qualifier, opts \\ []) do
    read_cell_as(project, instance, table, row_key, family, qualifier, :json, opts)
  end

  @doc """
  Reads and decodes a 64-bit integer from a cell.
  """
  @spec read_integer(String.t(), String.t(), String.t(), String.t(), String.t(), String.t(), keyword()) ::
          {:ok, integer() | nil} | {:error, term()}
  def read_integer(project, instance, table, row_key, family, qualifier, opts \\ []) do
    read_cell_as(project, instance, table, row_key, family, qualifier, :integer, opts)
  end

  @doc """
  Reads and decodes a 64-bit float from a cell.
  """
  @spec read_float(String.t(), String.t(), String.t(), String.t(), String.t(), String.t(), keyword()) ::
          {:ok, float() | nil} | {:error, term()}
  def read_float(project, instance, table, row_key, family, qualifier, opts \\ []) do
    read_cell_as(project, instance, table, row_key, family, qualifier, :float, opts)
  end

  @doc """
  Reads and decodes a boolean from a cell.
  """
  @spec read_boolean(String.t(), String.t(), String.t(), String.t(), String.t(), String.t(), keyword()) ::
          {:ok, boolean() | nil} | {:error, term()}
  def read_boolean(project, instance, table, row_key, family, qualifier, opts \\ []) do
    read_cell_as(project, instance, table, row_key, family, qualifier, :boolean, opts)
  end

  @doc """
  Reads and decodes a DateTime from a cell.
  """
  @spec read_datetime(String.t(), String.t(), String.t(), String.t(), String.t(), String.t(), keyword()) ::
          {:ok, DateTime.t() | nil} | {:error, term()}
  def read_datetime(project, instance, table, row_key, family, qualifier, opts \\ []) do
    read_cell_as(project, instance, table, row_key, family, qualifier, :datetime, opts)
  end

  @doc """
  Reads and decodes an Elixir term from a cell.
  """
  @spec read_term(String.t(), String.t(), String.t(), String.t(), String.t(), String.t(), keyword()) ::
          {:ok, term() | nil} | {:error, term()}
  def read_term(project, instance, table, row_key, family, qualifier, opts \\ []) do
    read_cell_as(project, instance, table, row_key, family, qualifier, :term, opts)
  end

  # ============================================================================
  # Batch Operations
  # ============================================================================

  @doc """
  Writes multiple typed cells to a single row.

  ## Examples

      MegasPinakas.Types.write_cells(project, instance, "users", "user#123", [
        {:string, "cf", "name", "John Doe"},
        {:integer, "cf", "age", 30},
        {:json, "cf", "profile", %{city: "NYC"}},
        {:datetime, "cf", "created", DateTime.utc_now()}
      ])
  """
  @spec write_cells(String.t(), String.t(), String.t(), String.t(), list(), keyword()) ::
          {:ok, term()} | {:error, term()}
  def write_cells(project, instance, table, row_key, cells, opts \\ []) when is_list(cells) do
    mutations =
      Enum.map(cells, fn
        {:binary, family, qualifier, value} ->
          MegasPinakas.set_cell(family, qualifier, value, opts)

        {:string, family, qualifier, value} ->
          MegasPinakas.set_cell(family, qualifier, value, opts)

        {:json, family, qualifier, value} ->
          set_json(family, qualifier, value, opts)

        {:integer, family, qualifier, value} ->
          set_integer(family, qualifier, value, opts)

        {:float, family, qualifier, value} ->
          set_float(family, qualifier, value, opts)

        {:boolean, family, qualifier, value} ->
          set_boolean(family, qualifier, value, opts)

        {:datetime, family, qualifier, value} ->
          set_datetime(family, qualifier, value, opts)

        {:term, family, qualifier, value} ->
          set_term(family, qualifier, value, opts)
      end)

    MegasPinakas.mutate_row(project, instance, table, row_key, mutations, opts)
  end

  @doc """
  Reads multiple typed cells from a single row.

  Returns a map with "family:qualifier" keys and decoded values.

  ## Examples

      {:ok, data} = MegasPinakas.Types.read_cells(project, instance, "users", "user#123", [
        {:string, "cf", "name"},
        {:integer, "cf", "age"},
        {:json, "cf", "profile"}
      ])
      # => {:ok, %{"cf:name" => "John Doe", "cf:age" => 30, "cf:profile" => %{"city" => "NYC"}}}
  """
  @spec read_cells(String.t(), String.t(), String.t(), String.t(), list(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def read_cells(project, instance, table, row_key, cells, opts \\ []) when is_list(cells) do
    case MegasPinakas.read_row(project, instance, table, row_key, opts) do
      {:ok, nil} ->
        {:ok, %{}}

      {:ok, row} ->
        result =
          cells
          |> Enum.map(fn {type, family, qualifier} ->
            key = "#{family}:#{qualifier}"
            raw_value = MegasPinakas.get_cell(row, family, qualifier)

            decoded_value =
              if raw_value do
                case decode(type, raw_value) do
                  {:ok, v} -> v
                  {:error, _} -> nil
                end
              else
                nil
              end

            {key, decoded_value}
          end)
          |> Map.new()

        {:ok, result}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # ============================================================================
  # Private Helpers
  # ============================================================================

  defp read_cell_as(project, instance, table, row_key, family, qualifier, type, opts) do
    case MegasPinakas.read_row(project, instance, table, row_key, opts) do
      {:ok, nil} ->
        {:ok, nil}

      {:ok, row} ->
        case MegasPinakas.get_cell(row, family, qualifier) do
          nil -> {:ok, nil}
          raw_value -> decode(type, raw_value)
        end

      {:error, reason} ->
        {:error, reason}
    end
  end
end
