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
  Its tools are Bash and Read; Read is confined to the run directory, and Bash runs `./box`
  calls only. A reach for anything else is denied and stays in the transcript as testimony.
- **The tools** are in [`tools/`](tools/): `mkbox.sh` and `box.template` (the box and its log),
  `make-registry.sh` (the rehearsal's stand-in registry), `seal-check.sh`, `block.sh`,
  `replay.sh`, each with a selftest.

## Attempts

| Attempt | Phase | Sha / version | README sha256 | Image | Network | Resumes | (a) | (b) | (c) | (d) | (e) | (f) | Master seed (pristine) | Master seed (red) | Seed replayed | Verdict | Noah's sign-off |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|

## Testimony
