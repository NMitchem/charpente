# Attempt 2: the stranger's report and its follow-up answer, verbatim

## Report (the headless session's final message)

Done. Here's what I did and what I saw.

## What I did

Working only from `README.md`, on the fresh Linux box (via `./box`), I walked the README's own "Try it" recipe end to end:

1. **`cargo install charpente-cli`** → installed `charpente 0.3.0` (binary at `/usr/local/cargo/bin/charpente`). It pulled its deps from a local registry mirror; the box has no live internet (confirmed below).
2. **`charpente new myapp`** → scaffolded a crate: `Cargo.toml`, `clippy.toml`, `src/lib.rs`, `src/main.rs`, `tests/sim.rs`. The scaffold is the README's example in miniature — one `#[table] Note`, one `#[route] add_note` that does `tx.put` + `tx.enqueue(Echo)` + `tx.emit(NoteAdded)` in one transaction, and three sim properties.
3. **`charpente test --sim`** → ran it green.
4. **Falsification demo #1:** deleted the `tx.enqueue` line, reran.
5. **Falsification demo #2:** restored, deleted the `tx.emit` line instead, reran.
6. Restored the scaffold to original.

## What I saw

- **Green baseline:** 3 checks × 256 runs = `768 runs`, `3 passed; 0 failed`, master seed `0x82dc8265b9aba391`. The output is honest about the ~100 runs that *skipped* (a fault hit before the claim could be tested — "no POST was acked," "app never booted," etc.) rather than counting them as passes.

- **Delete `tx.enqueue`:** `acked_post_implies_the_echo_after_recovery` went **FAILED** — "ack-durability violated: 1 POST(s) were acked with 200 but only 0 Echo job(s) are durable." It **shrank** to the minimal input (`notes=1, order=[1], body="alpha"`, 5 draws → 3) and printed a **`repro:` command**. I ran that command and got the byte-identical failure from the seed alone — deterministic replay, as guarantee #3 claims. The other two checks stayed green.

- **Delete `tx.emit`:** `acked_post_implies_its_event_after_recovery` went **FAILED** — "event-durability violated: note 1 … its NoteAdded is in the event log 0 time(s), not once," again shrunk to one note with a replay seed, other two green.

So the central pitch — one commit point (state + job + event commit together, and dropping either effect is caught structurally) plus deterministic, seed-replayable fault-injection with shrinking — reproduced exactly as written.

## One caveat

**Hard rule 1, `cargo clippy --all-targets -- -D warnings`, I could not run.** The clippy component isn't in the toolchain, and `rustup component add clippy` fails with a DNS error (`static.rust-lang.org` unreachable) — the box has no live network, so the one install step that isn't served by the local crate mirror can't complete. That's an environment limitation, not a charpente claim I could falsify; everything else in "Try it" ran as advertised.

## Follow-up

Asked once, by resuming the same session: `In two or three sentences, what did the last sim run you did tell you?`

The last sim run was the one with `tx.emit` deleted (master seed `0x2e7df5b4debd7fa7`). It showed that `acked_post_implies_its_event_after_recovery` failed in all 219 runs that tested it: a note was acked with 200, but its `NoteAdded` event never reached the durable log. The failure shrank to a single note with a seed that replays it, while the other two checks stayed green. After that I copied the original `lib.rs` back but didn't run the sim again, so no run confirms the restored state. It is a byte copy of the version that passed in the first run.
