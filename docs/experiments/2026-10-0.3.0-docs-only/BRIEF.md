# pricewatch: product brief for a docs-only build (requirements, never implementation)

You are building a small backend on the charpente platform, from what charpente ships and nothing else.

You have a fresh Linux machine for this, with no network access. Run every command there with `./box '<command>'` from this directory. The command runs in that machine's shell, in a working directory that persists between calls. The machine's cargo is already pointed at a registry that holds charpente 0.3.0 and its dependencies.

Your sources are the following, and nothing else:
- this brief;
- `README.md` beside it;
- the documentation that ships inside the `charpente` crate: its `GUIDE.md`, its `examples/`, and the four files under its `tests/` that GUIDE names as worked examples: `tests/clock.rs`, `tests/outbound.rs`, `tests/describe.rs` and `tests/realtime.rs`. This machine's cargo reads crates from a directory registry, so they are at `/opt/registry/charpente-0.3.0/GUIDE.md`, `/opt/registry/charpente-0.3.0/examples/` and `/opt/registry/charpente-0.3.0/tests/`.

Do not read the platform's implementation. That means no `src/` of any charpente crate, no other file under `tests/`, and nothing online. If the documentation does not tell you how to do something, record that in `FINDINGS.md` and choose the simplest thing it does support. Never patch the framework.

## Setup

Install the CLI with `cargo install charpente-cli`. Then run `charpente new pricewatch` and `git init` inside `pricewatch/`, since the scaffold does neither. Every commit touches `pricewatch/` only.

## Domain

- Products are registered by an operator. Each product has a name.
- Sources are price feeds: a shop's crawler, a partner's export, a user's report. Each source has an id. A source submits a price report for a product. A report gives the product id, an amount in cents and the time the source observed the price. No credential is needed beyond the source's own API key, issued by an operator as GUIDE §5 describes.
- Reports arrive late, out of order and more than once. Several sources report the same product, so a report observed earlier can arrive after one observed later.
- For each product the app keeps two things:
  - the **lowest price** ever reported, with the source and observation time of that report;
  - the **latest report**: the one with the latest observation time, whatever its amount. If two reports carry the same observation time, the latest report is the one from the source with the smaller id.
- Anyone can read a product's lowest price and latest report.
- An operator can list the reports one source has made.

## Behaviours

- An acknowledged report is never lost, across crashes and restarts.
- Neither kept value ever depends on the order reports arrived in. The lowest price is the minimum over every acknowledged report. The latest report is the maximum by observation time, with ties going to the smaller source id.
- A report delivered twice counts once.
- An amount of zero, or a negative amount, is refused by name, and the refusal says what a valid amount is.

## Proof (the part that matters)

Write `tests/sim.rs` using the platform's simulation harness, with properties that prove the behaviours above under the full fault matrix. Run `charpente test --sim`. It must be green; record what it prints.

Then run `charpente test --mutants` (GUIDE §8) over the files that carry the merge, and record its full output in `FINDINGS.md`. The target is every mutant caught or unviable. A MISSED mutant is a one-sided property, and you must fix it before you are done.

## Deliverables

- `pricewatch/` as its own git repository, building with `cargo test` and `cargo clippy --all-targets -- -D warnings`.
- `FINDINGS.md` in the repository, a friction ledger in your own words, dated as you go. It records:
  - how many times you restarted an approach;
  - every GUIDE section you had to re-read, and why;
  - what you could not find in the docs;
  - what the sim's output told you, and whether it was ever unclear;
  - the `--mutants` output.
- Commit as you go: small commits, with named paths.

When you're done, say what you built, what you saw, and where the documentation failed you.
