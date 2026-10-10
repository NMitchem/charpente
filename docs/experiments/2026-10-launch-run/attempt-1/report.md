I worked through the README's "Try it" section on the box. Every step behaved as claimed except the clippy step, which I couldn't run: the box is offline and its Rust toolchain doesn't include clippy.

**What I did and saw**

1. **`cargo install charpente-cli`** installed v0.1.0. Cargo got it from a local copy of the registry at `/opt/registry`, not from crates.io, so this run doesn't test the real crates.io install.
2. **`charpente new myapp`** wrote the six files the README lists: one table, one route, one job, one event and two sim properties. One oddity: `charpente --version` prints the help text instead of a version.
3. **`cargo clippy --all-targets -- -D warnings`** failed with "`cargo-clippy` is not installed". `rustup component add clippy` couldn't download it (DNS failure), and no clippy binary exists anywhere on the box. As a weaker substitute I ran `RUSTFLAGS="-D warnings" cargo check --all-targets`, which passed, but that is not a clippy run.
4. **`charpente test --sim`** passed: 256 universes per property, no failures. However, most universes gave up without reaching a verdict (164/256 and 167/256), so only 181 of 512 runs actually tested anything.
5. **Deleted the `tx.enqueue` line and reran:** it went red, exit code 1, with 89 failures, all in `acked_post_implies_the_echo_after_recovery`. The message was "POST /notes/1 was acked with 200 but no Echo job is durable — the enqueue did not commit with the row." Each failure came with its seed, the fault settings and a command to replay it. The first property stayed green, as it should, since it only checks the row.
6. **Replay:** running seed `0x37a242310fbecbfe` twice gave identical failing output both times. Rerunning the whole suite with `--master-seed 0x7f63491f307a3a76` reproduced the full red run exactly. With the enqueue line put back, the same seed passed.

**One more claim I checked:** the README says a route, table, job or event that doesn't declare its access or tenant won't compile. I removed `public` from the route, and `global` from the table, job and event, one at a time. Each refused to compile with a specific error saying what to add and that there is no default. I restored the original afterwards and it compiled.

**What I couldn't check:** the 215 records/sec and ~31× group-commit figures, replication and failover, SSE resume, auth, the archive and restore, and the TypeScript client generator. These rely on docs (BENCH.md, GUIDE.md, DESIGN.md) that aren't in this folder, and the README itself says no Linux bare-metal benchmark exists. I followed the rule and didn't read charpente's source.

Everything is in `/root` on the box: `myapp/` is back to its original state, and the logs are `install.log`, `clippy.log`, `sim1.log` to `sim3.log`, and `replay1.log`/`replay2.log`.
