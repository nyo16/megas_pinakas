ExUnit.start()

# Tests tagged `:emulator` talk to a real BigTable emulator and are excluded by
# default so `mix test` stays hermetic. Run them with:
#
#     docker compose up -d bigtable-emulator
#     mix test --include emulator
ExUnit.configure(exclude: [:emulator])
