defmodule MegasPinakas.CacheTest do
  use ExUnit.Case, async: true

  alias MegasPinakas.Cache

  describe "module structure" do
    test "exports get function" do
      functions = Cache.__info__(:functions)
      assert {:get, 4} in functions
      assert {:get, 5} in functions
    end

    test "exports put function" do
      functions = Cache.__info__(:functions)
      assert {:put, 5} in functions
      assert {:put, 6} in functions
    end

    test "exports delete function" do
      functions = Cache.__info__(:functions)
      assert {:delete, 4} in functions
      assert {:delete, 5} in functions
    end

    test "exports get_or_put function" do
      functions = Cache.__info__(:functions)
      assert {:get_or_put, 5} in functions
      assert {:get_or_put, 6} in functions
    end

    test "exports get_many function" do
      functions = Cache.__info__(:functions)
      assert {:get_many, 4} in functions
      assert {:get_many, 5} in functions
    end

    test "exports put_many function" do
      functions = Cache.__info__(:functions)
      assert {:put_many, 4} in functions
      assert {:put_many, 5} in functions
    end

    test "exports delete_many function" do
      functions = Cache.__info__(:functions)
      assert {:delete_many, 4} in functions
      assert {:delete_many, 5} in functions
    end

    test "exports exists? function" do
      functions = Cache.__info__(:functions)
      assert {:exists?, 4} in functions
      assert {:exists?, 5} in functions
    end

    test "exports increment function" do
      functions = Cache.__info__(:functions)
      assert {:increment, 4} in functions
      assert {:increment, 5} in functions
      assert {:increment, 6} in functions
    end

    test "exports append function" do
      functions = Cache.__info__(:functions)
      assert {:append, 5} in functions
      assert {:append, 6} in functions
    end
  end
end
