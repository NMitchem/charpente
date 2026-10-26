# Attempt 1: the controller's gates (plan Task 22 step 3)

These ran against the registry built at `1106cdf0`, offline, on macOS aarch64 with rustc 1.99.0. The builder's box was Linux aarch64 with rustc 1.99.1. Raw logs are in `gates/`.

| Gate | Result | Verdict |
|---|---|---|
| framework patches | `pricewatch/Cargo.toml` has no `[patch]` and no `path`. Every charpente crate in `Cargo.lock` comes from the registry. The registry inside the box is byte-identical to the local one: 5985 files, and the sorted per-file sha256 listing is `a0f7b732…` on both sides. | PASS |
| sealing | The box-side `seal-check.sh` hits are reads of `GUIDE.md` and of `examples/restart_under_faults.rs`, an `ls` of the crate root, `examples/` and `tests/`, an `ls /opt/registry` (serde_json's version), and writes of the builder's own files. No `src/` and no other `tests/` file was read. The 106 Bash calls that were allowed carry exactly 118 `./box` invocations, which equals the box's 118 start lines; 22 more were refused by the permission check. | PASS |
| clippy (R30) | `cargo clippy --all-targets --locked --offline -- -D warnings` exits 0 (`gates/t22-clippy.log`) | PASS |
| `cargo test` | exit 0, with 3 `test result: ok` lines (`gates/t22-cargotest.log`) | PASS |
| final batch replay | `charpente test --sim --master-seed 0x2c0cbfefeb3ad9c2` (charpente 0.3.0 from the registry) exits 0. Its 28-line batch is byte-identical to the builder's (box call 112), across OS, toolchain patch and machine (`gates/t22-sim-replay2.log`). | PASS |
| controller `--mutants` | `--file src/merge.rs --file src/lib.rs --test-package pricewatch --jobs 3 --timeout 300`: `mutants: 82 total — 59 caught, 0 missed, 23 unviable, 0 timed out`, exit 0. This equals the builder's (`gates/t22-mutants2.log`). | PASS |

The controller's first pass was not valid. `cargo install charpente-cli` there read none of the project's `.cargo/config.toml`, because `cargo install` reads only `$CARGO_HOME`'s config. Its replay and mutants run therefore went through an older `charpente` already on the machine. Those numbers are set aside: the replay and mutants rows above are from the second pass, run under `charpente 0.3.0` installed with `--config` flags (`gates/t22-install2.log`).
