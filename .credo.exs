%{
  configs: [
    %{
      name: "default",
      strict: true,
      checks: %{
        disabled: [
          # Specs deliberately use struct literals (`%RowFilter{}`) for the
          # generated Google.* proto types instead of `RowFilter.t()`.
          # googleapis_proto_ex marks every module @moduledoc false, so `.t()`
          # references make ex_doc emit a "hidden module" warning per spec;
          # struct literals are not autolinked and dialyzer checks them the same.
          {Credo.Check.Warning.SpecWithStruct, []}
        ]
      }
    }
  ]
}
