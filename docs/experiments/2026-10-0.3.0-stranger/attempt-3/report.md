# Attempt 3: the stranger's report and its follow-up answer, verbatim

## Report (the headless session's final message)

Everything reproduced. Here's what I did and what I saw.

## What I did

Working only from `README.md`, on the fresh Linux box via `./box`:

1. **Installed the CLI** — `cargo install charpente-cli`. The box has no general internet, but cargo is source-replaced onto a vendored registry at `/opt/registry`, so it built and installed `charpente` (v`0.3.0`; the README text says `0.1.0` on crates.io, but the mirror carries 0.3.0).
2. **Scaffolded an app** — `charpente new myapp` wrote a complete crate: `Cargo.toml`, `src/lib.rs` (one table `Note`, one `public` route `add_note`, one job `Echo`, one event `NoteAdded`, all three effects in one `tx`), `tests/sim.rs` (three properties), and `clippy.toml`.
3. **Ran the sim** — `charpente test --sim`.
4. **Ran the two "watch it fail" experiments** the README's pitch hinges on, then restored the file and confirmed green again.

## What I saw

- **`charpente test --sim` passed green**: 3 checks × 256 seeded runs each under injected faults (write/sync errors, torn writes, crashes, clock skew, corruption, …). Skipped runs were honestly accounted for ("no POST was acked, so durability has nothing to prove"), and the batch prints a `--master-seed` to replay the whole run.

- **Deleting `tx.enqueue`** turned `acked_post_implies_the_echo_after_recovery` **FAILED** — "1 POST acked with 200 but only 0 Echo job(s) are durable — the enqueue did not commit with the row." It **shrank** to the minimal input (`notes=1, body="alpha"`), listed the faults it switched off as unnecessary, and printed a `repro:` command. I ran that command verbatim and it **reproduced the identical failure deterministically**.

- **Deleting `tx.emit`** instead turned the *third* check (`..._its_event_after_recovery`) **FAILED** — "its NoteAdded is in the event log 0 time(s), not once" — while the echo check went back to green. Exactly the behavior the README describes.

- **`charpente describe`** emitted the machine-readable typed surface — tables (with fingerprints/indexes), the route (`auth=false public_scope=none`, typed params/request/response/error), jobs, cron, and config — backing the "introspectable by machines" claim.

So the README's central pitch reproduces as written: three effects (row, job, event) commit as one unit, a deterministic fault-injection sim proves it, and removing any one effect produces a named, shrunk, seed-replayable failure.

## One thing I could not reproduce

**Hard rule 1 (clippy) couldn't run.** `cargo clippy` isn't installed, and `rustup component add clippy` — which the README anticipates — needs `static.rust-lang.org`, which the box can't reach (no DNS). The clippy binary isn't in the vendored registry either. This is an environment limitation (no internet, mirror doesn't carry the toolchain component), not a charpente failure; `clippy.toml` with the disallowed-method/type lints was scaffolded correctly.

The app source is restored to its original scaffold and the sim is green.

## Follow-up

Asked once, by resuming the same session: `In two or three sentences, what did the last sim run you did tell you?`

The last run was on the restored scaffold, and all three checks passed: 205, 253 and 205 of 256 runs actually tested the property, and the rest were skipped because a fault stopped them before the check could run. So restoring the file brought it back to green, and no tested run on that batch broke durability, exactly-once echo, or the event. That's evidence for those claims, not proof. I filtered the output to the result lines, so I didn't record that batch's master seed and can't replay it exactly.
