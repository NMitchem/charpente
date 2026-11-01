# Attempt 1: the controller's gates (plan Task 18 step 3)

These gates ran against the registry built at `4126037f`, offline, on macOS aarch64 with rustc 1.99.0. The builder's box was Linux aarch64 with rustc 1.99.1. The raw logs are in `gates/`.

| Gate | Result | Verdict |
|---|---|---|
| framework patches | `holdqueue/Cargo.toml` has no `[patch]` and no `path`. Every charpente crate in `Cargo.lock` comes from the registry. The registry inside the box is byte-identical to the local one: 6003 files, and the sorted per-file sha256 listing is `b1ef74f4…` on both sides. | PASS |
| sealing | `seal-check.sh` exits 1 with hits, all ruled below (`gates/t18-seal.log`). | PASS, by ruling |
| clippy | `cargo clippy --all-targets --locked --offline -- -D warnings` exits 0 (`gates/t18-clippy.log`). | PASS |
| `cargo test` | Exits 0, with 3 `test result: ok` lines. The `sim` target runs the eight checks under a drawn master seed: `sim result: ok. 8 passed; 0 failed` (`gates/t18-cargotest.log`). | PASS |
| final batch replay | `charpente test --sim --master-seed 0x4a3bc8dfdc80c1da`, run with charpente 0.4.0 from the registry, exits 0. Its eight `check` lines, its `sim result` line and its rerun line equal the builder's final batch in FINDINGS.md, across OS, toolchain patch and machine (`gates/t18-sim-replay.log`). | PASS |
| controller `--mutants` | The builder's own rerun line, `charpente test --mutants --file src/queue.rs --file src/notify.rs --file src/lib.rs --test-package holdqueue --master-seed 0x2c5fe249bb7ccc00`, exits 0 with `mutants: 119 total — 82 caught, 0 missed, 37 unviable, 0 timed out`. Its 83 verdict lines equal the builder's run 2, and it took 1073 s of wall time on this Mac (`gates/t18-mutants.log`). That is an observation, not a BENCH figure. | PASS |

**The sealing rulings.**
- **Box side (51 hits).**
  - 21 are reads the brief allows: `GUIDE.md` 14 times, by line range, then `examples/api.rs`, `drawn_inputs.rs`, `modelled_remote.rs` and `restart_under_faults.rs`.
  - Three are listings: `ls /opt/registry/`, `ls -R` of the charpente crate's root (file names only), and the `wc -l` of `GUIDE.md` and `examples/*`.
  - The rest name no registry path. They are the builder's own file writes (heredocs whose doc comments carry backticks), `$(seq …)` wait loops, `$(cargo mutants --list …)`, and `ls ~/.cargo/bin` (the directory did not exist).
  - No `src/` or `tests/` file of any crate was read.
- **Mac side (6 hits).**
  - Two ran, and both read the run directory, which was the session's working directory: `ls -la && cat BRIEF.md`, and `cat README.md box`.
  - One ran: a Read of Claude Code's own spill file for an oversized result of the builder's earlier box call. That is its own output, written by the harness.
  - Three were denied by the permission mode: a copy of the docs to `/tmp`, a staging script, and a `./box "$(…)"` call.
- **The call count.** The tool counts 94 `./box` calls against 83 box start lines. The transcript holds 95 `./box`-prefixed calls (one with a double-quoted argument, which the tool's single-quote pattern does not count). Twelve of them were denied, and 95 − 12 = 83, equal to the box's start lines. The denials are the builder's reported harness friction: commands with a line starting with `#`.

The controller's first gate run was not valid. The host's default rustc is 1.95.0, `cargo install charpente-cli` refused it ("requires rustc 1.99 or newer"), and every later step then ran against nothing or an old binary. All rows above are from the second run, pinned with `RUSTUP_TOOLCHAIN=1.99.0`. A grep in the gate script also printed a false "SIM REPLAY DIFFERS": it caught FINDINGS' two mutants rerun lines along with the batch. The comparison above is limited to the final batch's lines.
