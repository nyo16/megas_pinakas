# Dialyzer warnings to ignore, with the reason for each.
#
# Keep this list narrow. Every entry should be an upstream typing artifact we
# cannot fix from here, not one of our own bugs.

[
  # grpc 1.0 defines %GRPC.Channel{} with `defstruct` but no `@type t()`.
  # `Client.execute/2` and `execute!/2` have documented `GRPC.Channel.t()` in
  # their specs since before the 0.11 -> 1.0 upgrade.
  ~r/Unknown type: GRPC\.Channel\.t\/0/
]
