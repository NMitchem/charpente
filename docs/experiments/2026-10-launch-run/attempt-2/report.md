The README's "Try it" claim held up on the fresh Linux box (aarch64, rustc 1.99.0). Deleting `tx.enqueue` turns the sim red with a seed, and that seed replays exactly. I used only the README and the CLI's own output.

**What I did and saw**

1. **`cargo install charpente-cli`** installed `charpente-cli v0.1.0` with no errors or warnings in the log.
2. **`charpente new myapp`** wrote the six files it says it writes. The scaffold is simpler than the README's example: one `Note` table marked `global`, a `public` route, a job and an event (no `Org`, no `auth`). The route does the three effects in one commit as described, and `tests/sim.rs` has two sim properties.
3. **`cargo clippy --all-targets -- -D warnings`** failed the first time because clippy isn't installed on this machine's toolchain. That's the machine, not charpente. After `rustup component add clippy` it passed cleanly.
4. **`charpente test --sim`** passed: 256 universes per property, no failures. But only 165 of the 512 runs reached a verdict; the other 347 gave up.
5. **Deleting `tx.enqueue(Echo { note: id });` and rerunning** went red. Every universe of the second property that reached a verdict failed (78 of them) with "acked with 200 but no Echo job is durable". Each failure came with its seed, its fault settings, the faults that fired, and a command to replay it. The first property stayed green, which fits because it only checks that the note itself survives.
6. **Replay:**
   - I replayed seed `0xd04099cad12ff0d2` twice. Both runs gave the same failure, byte for byte.
   - Rerunning the whole broken run from its master seed `0x7894bb0aadb2ce8b` reproduced the original output exactly.
   - **Control with the line restored:** the same 256 universes gave 94 verdicts and 0 failures. Of the first 15 failing seeds, 14 pass when replayed against the correct app. The 15th, `0xd04099cad12ff0d2`, gives up instead of passing, which is plausibly because the restored job's own writes hit the injected sync error.

I put `src/lib.rs` back to its original state afterwards.

**Things worth knowing**

- **Lots of give-ups.** About two-thirds of universes reach no verdict, so the default run decides only about 80–95 universes per property. The tool says so openly, and for one seed with nothing decided it called the property "vacuous". But "256 universes" overstates how much was actually tested.
- **"Second property" in the README** works whether it means guarantee #2 or the scaffold's second sim test; both go red.
- **No `--version` flag.** `charpente --version` just prints usage.

**Not reproduced:** the README's other claims depend on docs that aren't here or on different hardware. These include the BENCH numbers (215 records/sec and ~31× with group commit, measured on a Mac laptop), the 10,000-universe run of the invoicing example, replication and failover, SSE resume, and archive/restore. I didn't attempt them.
