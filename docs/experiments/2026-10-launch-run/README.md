# The launch run

M3 sub-project 5 and exit criterion (a): a stranger, given only charpente's published README
and one sentence, reproduces README's "Try it" — install, scaffold, sim green, delete the
`tx.enqueue` line, red with a seed, replay the seed — with every command it issues running in a
fresh container through a logged `./box`. First as a rehearsal against the packaged crates
(no network in the box), then against crates.io after the publish. Spec:
[`docs/superpowers/specs/2026-10-06-launch-run-design.md`](../../superpowers/specs/2026-10-06-launch-run-design.md);
plan: [`docs/superpowers/plans/2026-10-06-launch-run.md`](../../superpowers/plans/2026-10-06-launch-run.md).

- **Brief:** [`BRIEF.md`](BRIEF.md), sha256 `dae25ebbfa812bb93f9d650d254e77863ab651005b82c35ebf257aafdc674dd9`,
  byte-identical for every attempt in both phases.
- **Image:** `rust@sha256:6ff07edce8775d0f64be7aba9197229407301bddf2054d62c27b541a6238a181`
  (`rust:latest` on 2026-10-09: rustc 1.99.0, aarch64; no clippy, no rustfmt). A real run after
  a toolchain release records the newer digest.
- **The stranger:** one Opus agent, run headless from its run directory so that no project
  instructions, memory or repository context reach it:
  `claude -p "<prompt>" --model opus --restricted --tools Bash,Read --permission-mode dontAsk
  --allowedTools Read "Bash(./box:*)" --strict-mcp-config --output-format stream-json --verbose`.
  Its tools are Bash and Read, and Read is confined to the run directory. Bash is pre-allowed
  for `./box` calls only, but Claude Code still auto-allows read-only commands inside the run
  directory under `dontAsk`. That is how both strangers read `BRIEF.md`, `README.md` and the
  box wrapper with `cat`, and why `./box …; sleep N; ./box …` chains ran in attempt 2. A
  reach for anything else is denied and stays in the transcript as testimony.
- **The tools** are in [`tools/`](tools/): `mkbox.sh` and `box.template` (the box and its log),
  `make-registry.sh` (the rehearsal's stand-in registry), `seal-check.sh`, `block.sh`,
  `replay.sh`, each with a selftest.

**Verdict: PASS on the first real attempt (`attempt-2/`), signed by Noah 2026-10-10.** A
stranger with only the published README installed `charpente-cli` 0.1.0 from crates.io and
scaffolded an app. It went green, deleted `tx.enqueue`, went red with a seed, and replayed
the seed byte for byte. The controller's replay of that seed in a second fresh box, which
installed from crates.io itself, printed the same block. That is M3 exit criterion (a). The
same attempt's install, `charpente new`, `cargo clippy --all-targets -- -D warnings` (after
`rustup component add clippy`, calls 10–11) and green `charpente test --sim` (call 13) are the
publish-path spec's §10.2 "post-publish run against the registry". The rehearsal
(`attempt-1/`) passed first, also signed.

## Attempts

| Attempt | Phase | Sha / version | README sha256 | Image | Network | Resumes | (a) | (b) | (c) | (d) | (e) | (f) | Master seed (pristine) | Master seed (red) | Seed replayed | Verdict | Noah's sign-off |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| [1](attempt-1/) | rehearsal | `e3f7f0c`; charpente-cli 0.1.0 packaged from it | `affa4644…5e96` | `6ff07edc…a181` | none | 0 | PASS | PASS | PASS | PASS | PASS | PASS | `0x15a02a91b60257df` | `0x7f63491f307a3a76` | `0x37a242310fbecbfe` | PASS | 2026-10-10 |
| [2](attempt-2/) | real | 0.1.0 from crates.io (repo `0db7e7e`) | `affa4644…5e96` | `6ff07edc…a181` | bridge | 0 | PASS | PASS | PASS | PASS | PASS | PASS | `0x912313adf8115ad0` | `0x7894bb0aadb2ce8b` | `0xd04099cad12ff0d2` | PASS | 2026-10-10 |

Record directories are numbered across both phases. The rehearsal installed from a
directory-source registry of the crates packaged at the pinned sha, in a box with no
network; the real attempt installed from crates.io, in a box with the default bridge
network. Both read the same README bytes: the published crate's README is the one the
rehearsal read. Each value comes from the attempt's own files: the master seeds from the
batch summaries in `box.jsonl`, the replayed seed from `batch.block`. (c) is the stranger's
replay; (d) is the controller's, in a fresh box (`replay/`).

| Attempt | Green batch | Red batch | Agent | Box calls |
|---|---|---|---|---|
| 1 | 181 verdicts, 331 gave up, no failures | 75 verdicts, 348 gave up, 89 failures, all `acked_post_implies_the_echo_after_recovery` | 19 turns, 142 s | 27 |
| 2 | 165 verdicts, 347 gave up, no failures | 100 verdicts, 334 gave up, 78 failures, all `acked_post_implies_the_echo_after_recovery` | 20 turns, 425 s | 22 |

Both batches ran 256 universes per test over the scaffold's two properties. Both strangers'
scaffolds are byte-identical (`src/lib.rs` sha256 `624eb197…`). Neither stranger asked a
question, and neither session needed a resume.

## Testimony

Each attempt's closing message (`attempt-N/report.md`) and transcript narrative are read as
testimony, not as findings. Every claim about charpente was checked against the published
README (`R:`, sha256 `affa4644…`), the code (`cli/` is `crates/charpente-cli/`), or the box
log (`call N` is seq N of that attempt's `box.jsonl`). **Attempt 1 made 12 claims, all
right. Attempt 2 made 19: 17 right, 1 right with a wrong detail, 1 plausible but unproven.**

- **Give-ups, both attempts: the run's strongest finding.** Two strangers out of two read the
  give-up counts as overstating what was tested: "only 181 of 512 runs actually tested
  anything" (attempt 1) and "'256 universes' overstates how much was actually tested"
  (attempt 2). Their numbers are right. The CLI prints `256 universe(s) per test` first and
  the verdict and give-up counts after it. No output, human or `--format json`, says why a
  universe gave up: the template's `give_up` reasons (`cli/templates/sim.rs.tmpl:35`, `:49`,
  `:56`, `:100`, `:106`, `:116`, `:121`, `:181`) never reach the user. The controller asked
  for `S`'s restored-line replay as JSON: it carries `coverage` (`fault:sync_error: 1`,
  `universe:gave-up: 1`) and no reason.
- **#25: right count, wrong detail.** 14 of the first 15 failing seeds pass with the line
  restored (call 21). But `S` is the first of the 15, not "the 15th".
- **#26: plausible, unproven.** That `S` gives up with the line restored "plausibly because
  the restored job's own writes hit the injected sync error". It is consistent with the
  template: a universe gives up only on fault fallout or an unacked POST, and the restored
  `Echo` commit is the only extra IO in the recovery loop (`sim.rs.tmpl:173-176`). It is also
  consistent with the run: a sync error fired in that universe. But no output names the
  write that took it.
- **README's clippy step assumes the clippy component.** The official image's minimal
  profile has none. Offline (attempt 1) it could not be added, and the stranger substituted
  `RUSTFLAGS="-D warnings" cargo check --all-targets` while saying plainly it was not clippy.
  With network (attempt 2), `rustup component add clippy` fixed it and clippy passed clean.
- **`charpente --version` prints the usage text** (both attempts; `cli/src/main.rs:31`, the
  fallback arm). It shipped that way in 0.1.0 and is carried to 0.1.1.
- **README's doc links were dead in attempt 1,** because on crates.io they resolve against
  the repository, which was private then. It was public before attempt 2, and every link
  target resolved unauthenticated.

### The census

| # | Attempt | Claim | Ruling | Evidence |
|---|---|---|---|---|
| 1 | 1 | `charpente new` writes six files | right | `cli/src/new.rs:98-103` |
| 2 | 1 | the scaffold has one table, one route, one job, one event | right | `cli/templates/lib.rs:13`, `:27`, `:34`, `:52` |
| 3 | 1 | `tests/sim.rs` has two properties, and the first checks only the row | right | `cli/templates/sim.rs.tmpl:95-120`, `:168` |
| 4 | 1 | `charpente --version` prints the usage text | right | `cli/src/main.rs:31` |
| 5 | 1 | README's clippy step cannot run in this image, and offline clippy cannot be added | right | calls 10, 13 ("'cargo-clippy' is not installed"); call 11's `rustup component add` offline; a rehearsal artifact |
| 6 | 1 | "only 181 of 512 runs actually tested anything" | right | call 19 |
| 7 | 1 | the four default-secure surfaces refuse to compile, each naming its fix and "no default" | right | call 27 |
| 8 | 1 | the master seed reproduces the whole red batch; `S` passes with the line restored | right | calls 24, 26 |
| 9 | 1 | the install came from a local registry, so it did not test crates.io | right | the rehearsal's design (`tools/make-registry.sh`) |
| 10 | 1 | README's doc links point at files not beside it | right | R: relative links; repository private at the time |
| 11 | 1 | no Linux bare-metal benchmark exists | right | R:142 |
| 12 | 1 | `sleep 60` was blocked by the harness, not by charpente | right | transcript; Claude Code's permission check |
| 13 | 2 | the install of 0.1.0 printed no warning or error | right | call 5 (the `grep` printed nothing) |
| 14 | 2 | six files; one `global` table, a `public` route, a `global` job and event, no `Org`, no `auth` | right | `cli/templates/lib.rs:13`, `:27`, `:34`, `:52` |
| 15 | 2 | the route's three effects commit in one transaction | right | `cli/templates/lib.rs:52-62` |
| 16 | 2 | `tests/sim.rs` has two sim properties | right | `cli/templates/sim.rs.tmpl:95`, `:168` |
| 17 | 2 | clippy is missing from the toolchain; after `rustup component add clippy` it passes | right | calls 9–12 |
| 18 | 2 | 165 of 512 runs reached a verdict; 347 gave up | right | call 13 |
| 19 | 2 | every universe of the second property that reached a verdict failed (78) | right | call 16: 178 of 256 gave up, 78 failures |
| 20 | 2 | each failure carries its seed, fault settings, fired faults and a repro command | right | `attempt-2/batch.block` |
| 21 | 2 | the first property stays green because it checks only the note | right | `cli/templates/sim.rs.tmpl:95-120` |
| 22 | 2 | `S` replayed twice, byte for byte | right | call 18; container `replay1.log` = `replay2.log` |
| 23 | 2 | the master seed reproduces the red batch exactly | right | call 19 (`MASTER_RUN_IDENTICAL`) |
| 24 | 2 | with the line restored the same 256 universes give 94 verdicts, 0 failures | right | call 20 (`--format json`) |
| 25 | 2 | 14 of the first 15 failing seeds pass restored; `S`, "the 15th", gives up | right, wrong detail | call 21: `S` is the first |
| 26 | 2 | `S` gives up restored because the job's own writes hit the injected sync error | plausible, unproven | `sim.rs.tmpl:173-176`; coverage `fault:sync_error: 1`; no reason in any output |
| 27 | 2 | "256 universes" overstates what was tested | right | calls 13, 16; no output names a give-up reason |
| 28 | 2 | for a seed that decided nothing the CLI calls the property "vacuous" | right | call 19 |
| 29 | 2 | README's "the second property goes red" works for guarantee 2 or the second sim test | right | R:166-167; both go red |
| 30 | 2 | there is no `--version` flag | right | call 22; `cli/src/main.rs:31` |
| 31 | 2 | BENCH's 215/s and ~31× were measured on a Mac laptop | right | R:97-98, R:138-141 say "dev laptop under `F_FULLFSYNC`", a macOS `fcntl` |

## What the run leaves for after launch

None of these blocks the verdict, and none is fixed here.

1. **Say why a universe gave up.** The `give_up` reasons exist and never reach the user, and
   both strangers read "256 universes" as more coverage than they got.
2. **`charpente --version`.** 0.1.1.
3. **README's clippy line** could say what to do when the component is missing
   (`rustup component add clippy`).
4. **"The second property"** could name the property it means:
   `acked_post_implies_the_echo_after_recovery`.
