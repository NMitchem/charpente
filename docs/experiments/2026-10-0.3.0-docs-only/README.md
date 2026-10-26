# The 0.3.0 docs-only build

This is M3 sub-project 3's bar, re-run for 0.3.0 (spec §6.8). A fresh agent works offline and gets only what ships in the 0.3.0 crates: README, and the `charpente` crate's `GUIDE.md` and `examples/`. It builds an app in a new domain, a price watch, whose correctness rests on an order-sensitive merge. The brief gives the domain only. It has no hints about drawn inputs or shrinking.

- **Spec:** [`docs/superpowers/specs/2026-10-22-0.3.0-design.md`](../../superpowers/specs/2026-10-22-0.3.0-design.md) §6.8. **Plan:** [`docs/superpowers/plans/2026-10-22-0.3.0.md`](../../superpowers/plans/2026-10-22-0.3.0.md) Task 22.
- **Brief:** [`BRIEF.md`](BRIEF.md), committed before the run.
- **The machine:** the launch run's box ([`../2026-10-launch-run/tools/`](../2026-10-launch-run/tools/)), with no network. Its cargo is pointed at a directory registry of the six 0.3.0 crates, packaged at the pinned sha, beside their vendored dependency closure.
  - The image is `charpente-t22-box:mutants-27.1.0` (id `sha256:f7db863a76ac520bbb584d40243cbc8ac9be1be76955dccbd58ce48eef5dd145`). It is the 0.3.0 stranger's `rust@sha256:931c2c05…` image (rustc 1.99.1, no clippy) plus `cargo install cargo-mutants --version 27.1.0 --locked`, because the bar runs `charpente test --mutants`.
- **The builder:** one Opus agent, run headless from its run directory as the launch run's stranger was (Ruling P1-final). It has Bash for `./box` calls and Read confined to the run directory.

## The bar, written before the run

The bar is M3 sub-project 3's:
- zero framework patches;
- zero Criticals in review that a platform oracle should have caught;
- `charpente test --mutants` clean over its sim properties, with no hand mutation pass.

Findings are ledgered as testimony, and a platform defect is fixed before the publish.

## Attempts

| Attempt | Sha | Image | Network | Resumes | Framework patches | Criticals a platform oracle should have caught | `--mutants` | Verdict |
|---|---|---|---|---|---|---|---|---|
