# statuspage — product brief for a docs-only build (requirements, never implementation)

You are building a small backend on the charpente platform, from its documentation alone.
Your ONLY sources are this brief, `docs/README.md` and `docs/GUIDE.md` in the directory
you are in. Do not read anything outside this directory — not the platform's source, not
its other docs, not its examples. If the docs do not tell you how to do something, record
that in `FINDINGS.md` and choose the simplest thing the docs DO support — never patch the
framework. Every commit touches `statuspage/` only.

## Setup
The `charpente` CLI is on your PATH. Run `charpente new statuspage` in this directory,
then `git init` inside `statuspage/` — the scaffold does neither. Append these lines to
the scaffold's `Cargo.toml` exactly as written; they point the two dependencies at a
local checkout instead of the registry, and they are the only lines you may add that
name a path outside this directory:

    [patch.crates-io]
    charpente = { path = "../../charpente/crates/charpente" }
    charpente-sim = { path = "../../charpente/crates/charpente-sim" }

## Domain
- Orgs. Operators belong to exactly one org and authenticate by session login and API
  keys as GUIDE §5 describes.
- Components belong to an org and carry a current status: Operational, Degraded, Down.
- Incidents belong to an org, affect one or more of its components, and move
  Investigating → Identified → Monitoring → Resolved. Any other transition is an
  application error with a message an operator could act on. Incident updates (a posted
  note) belong to an incident.
- A subscriber is one email address across every page it follows: one row, however many
  orgs it subscribes to. A subscription joins a subscriber to an org.

## Public surface
Anyone, with no credential, can:
- read an org's current status — its components and its unresolved incidents — by the
  org's id in the path;
- subscribe an email address to an org's page;
- stream that org's incident events (`IncidentOpened`, `IncidentUpdated`,
  `IncidentResolved`) over SSE with exact `Last-Event-ID` resume (GUIDE §11).

No public read, feed, or notification may ever carry another org's rows. Every other
write is operator-only and scoped to the operator's own org: no route may return, modify,
or count another org's rows.

## Behaviours
- Resolving an incident, in ONE transaction: marks it Resolved, enqueues a
  notify-subscribers job, and emits `IncidentResolved`. The job writes one notification
  row per subscription of that org (a table; no external IO).
- Maintenance windows: an operator schedules a window (start, end, components). A cron
  tick opens a maintenance incident when a window's start passes and resolves it when its
  end passes. Each window opens at most once and resolves at most once, never before its
  start or its end.
- `Incident` ships at `version = 1`. A second, separate commit bumps it to `version = 2`,
  adding a `severity` field (Minor, Major, Critical) with a migration (GUIDE §12). Old
  rows read back with a default severity.
- The TypeScript client: `charpente gen-client` output committed; `charpente
  check-client` exits 0 (GUIDE §14).

## Proof (the part that matters)
`tests/sim.rs` using the platform's simulation harness, with properties for:
- acked writes are durable across crashes; replay is identical from a seed;
- notifications ≡ resolved incidents × that org's subscriptions at resolve time, exactly,
  under the full fault matrix;
- no org ever observes another org's components, incidents, subscriptions, or events —
  on the public page, in the feed, and in notifications — under the fault matrix;
- maintenance windows: each opened at most once and resolved at most once, never early,
  over virtual weeks.

`charpente test --sim --universes 10000` must be green; record the master seed it prints.
Then run `charpente test --mutants` (GUIDE §8) over the files carrying the job, the cron
sweep, and the incident state machine, with `--test-package statuspage`, and record its
full output in `FINDINGS.md`. Every mutant caught or unviable is the target; a MISSED is a
one-sided property to fix before you are done.

## Deliverables
- `statuspage/` as its own git repository, building with `cargo test` and
  `cargo clippy --all-targets -- -D warnings`.
- `FINDINGS.md` in the repository: a friction ledger in your own words, dated as you go;
  how many times you restarted an approach; every GUIDE section you had to re-read and
  why; what you could not find in the docs; the master seed; the `--mutants` output.
- Commit as you go, small commits, named paths.
