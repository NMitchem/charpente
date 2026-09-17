# Third docs-only build — design

**Date:** 2026-09-17. **Status:** approved in chat (Noah), spec committed for the plan.
**Predecessor:** the 2026-09-13 roadmap ruling (docs/LEDGER.md "complete defined") that
defined M3 — Launch and named this as its third sub-project; default-secure surfaces
(`848c799`, spec `2026-09-16-default-secure-surfaces-design.md`) is the platform change
this build is the first outsider to meet. Sub-project 5 (the launch run) stands on this
one; sub-project 4 (claims audit) runs beside it.

## Context

The M0 exit criterion — an agent builds an app from the docs alone, with zero framework
patches — is the only milestone criterion that has ever been re-run against the platform.
On 2026-09-03 it failed: the docs-only helpdesk agent shipped an unauthenticated route, an
unscoped existence check, and five one-sided sim properties
(`docs/experiments/2026-09-helpdesk/README.md`). Each of the three now has a mechanical
answer — `#[route]` refuses a missing stance, `#[table]` refuses a missing stance,
`charpente test --mutants` names a one-sided property — and none of the three answers has
been met by a stranger.

This sub-project re-runs the criterion. MANIFESTO §3 states what it measures: an agent
succeeds or fails on how fast and how precisely the framework says *no*. So the brief is
written to reach for the shapes where the platform now asks — a route that must be
`public`, a table that must be `global`, anonymous reads and authenticated writes on one
tenant's rows — and says nothing about how to get there. What the agent does with that
pressure, and what the docs led it to, is the finding. The app is not the deliverable; the
testimony and the verdict are.

## §1 Decisions already made (Noah, 2026-09-17)

1. **Domain: a status page.** Orgs own components and incidents; the public reads an org's
   status, subscribes to it, and streams its incident feed; operators write. Not
   invoicing, not helpdesk.
2. **Location: its own repository, scaffolded by `charpente new`**, outside the workspace,
   against the local crates through `[patch.crates-io]`. First outsider use of the
   scaffold and of the app-side fence GUIDE §1 promises. No `members` edit, no
   `doc_inventory` coupling.
3. **On a fail: fix the platform, re-run fresh.** A fail is a platform finding; the fix
   lands on charpente's `main` as its own bounded change with a pin; the same brief is then
   re-run by a fresh agent in a fresh directory. "Third build" names the brief, not the
   attempt. The sub-project closes only on a pass.
4. **Builder: one Opus subagent, sealed.** Non-interactive; every point of confusion goes
   to FINDINGS.md. Opus because it is what an adopter would use, and because a weaker
   builder makes a doc gap look like an agent gap.

## §2 The bar

PLAN.md states the bar in three clauses. This section makes each one checkable.

**(a) Zero framework patches.** The app's manifest reaches charpente only through
`[patch.crates-io]` (or the path-dep fallback §4 names) at a pinned sha. At every gate,
`git -C charpente status --porcelain` is empty and `git -C charpente diff <sha> --stat --
crates Cargo.toml Cargo.lock` is empty: the code the builder compiled is the code at the
sha. Docs-only commits may land on `main` during an attempt (sub-project 4 runs beside
this one); the builder reads frozen copies, so they change nothing it saw. The builder's transcript is grepped for any tool call naming a path outside
the run directory (§4); a hit voids the attempt — not a fail of the bar, a void, because
nothing about the attempt is then docs-only.

**(b) Zero Criticals a platform oracle should have caught.** The reviewer (§6) classifies
every Critical against a **named platform claim**: a README guarantee, a GUIDE section, a
compile-time refusal, `charpente test --sim`, or `charpente test --mutants`. If the
platform claims to catch that class and did not, the Critical is a platform finding and the
attempt fails. If no oracle claims it — a domain-logic bug, a wrong state machine, a
missed requirement — it is the agent's bug: ledgered as testimony, not a fail. The
controller records each ruling with its citation; the verdict goes to Noah before the
ledger.

**(c) `--mutants` clean over its sim properties.** The controller runs
`charpente test --mutants --dir <app> --file <f> --test-package statuspage` over every
file carrying the job, the cron sweep, and the incident state machine; every mutant is
caught or unviable. A MISSED is a fail; a timeout is not a catch and is re-run with a
longer `--timeout` before it counts either way. The agent's own recorded run (§3) is
compared, not trusted.

**(d) The brief's own definition of done,** which is the precondition for (b) and (c)
meaning anything: `charpente test --sim --universes 10000` green with the master seed
recorded, and the controller's replay of that master seed (`--master-seed`, `--format
json`) identical to the agent's recorded output. A replay that differs voids the attempt
until explained — a nondeterministic app or harness is a platform finding in its own
right.

## §3 The brief

`BRIEF.md` is written in the shape of `docs/experiments/2026-09-helpdesk/BRIEF.md` —
requirements, never implementation — from the list below. Its preamble seals the run: the
only sources are the brief, `docs/README.md` and `docs/GUIDE.md` in the run directory; do
not read anything outside the run directory; if the docs do not say how, record it in
FINDINGS.md and choose the simplest thing the docs do support; never patch the framework.

**Domain.**
- Orgs. Operators belong to exactly one org and authenticate by session login and API
  keys as GUIDE §5 describes.
- Components belong to an org and carry a current status (Operational, Degraded, Down).
- Incidents belong to an org, affect one or more of its components, and move
  Investigating → Identified → Monitoring → Resolved. Any other transition is an
  application error with a message an operator could act on. Incident updates (a posted
  note) belong to an incident.
- A subscriber is one email address across every page it follows: one row however many
  orgs it subscribes to. A subscription joins a subscriber to an org.

**Public surface.** Anyone, with no credential, can:
- read an org's current status — its components and its unresolved incidents — by the
  org's id in the path;
- subscribe an email address to an org's page;
- stream that org's incident events (`IncidentOpened`, `IncidentUpdated`,
  `IncidentResolved`) over SSE with exact `Last-Event-ID` resume (GUIDE §11).

No public read, feed, or notification may ever carry another org's rows. Every other
write is operator-only and scoped to the operator's own org: no route may return, modify,
or count another org's rows.

**Behaviours.**
- Resolving an incident, in ONE transaction: marks it Resolved, enqueues a
  notify-subscribers job, emits `IncidentResolved`. The job writes one notification row
  per subscription of that org (a table; no external IO).
- Maintenance windows: an operator schedules a window (start, end, components). A cron
  tick opens a maintenance incident when a window's start passes and resolves it when its
  end passes. Each window opens at most once and resolves at most once, never before its
  start or end.
- `Incident` ships at `version = 1`. A second, separate commit bumps it to `version = 2`,
  adding a `severity` field (Minor, Major, Critical) with a migration (GUIDE §12); old
  rows read back with a default severity.
- The TypeScript client: `charpente gen-client` output committed; `charpente
  check-client` exits 0 (GUIDE §14).

**Proof (the part that matters).** `tests/sim.rs` using the platform's simulation
harness, with properties for:
- acked writes are durable across crashes; replay is identical from a seed;
- notifications ≡ resolved incidents × that org's subscriptions at resolve time, exactly,
  under the full fault matrix;
- no org ever observes another org's components, incidents, subscriptions, or events —
  on the public page, the feed, and in notifications — under the fault matrix;
- maintenance windows: each opened at most once and resolved at most once, never early,
  over virtual weeks.

`charpente test --sim --universes 10000` must be green; record the master seed it prints.
Then run `charpente test --mutants` (GUIDE §8) over the files carrying the job, the cron
sweep, and the incident state machine, with `--test-package statuspage`, and record its
output in FINDINGS.md: every mutant caught or unviable is the target; a MISSED is a
one-sided property to fix before you are done.

**Deliverables.**
- `statuspage/` as its own git repository, created by `charpente new statuspage` and
  `git init`, building with `cargo test` and `cargo clippy --all-targets -- -D warnings`.
- `FINDINGS.md` in the repository: friction ledger in your own words, dated as you go; how
  many times you restarted an approach; every GUIDE section you had to re-read and why;
  what you could not find in the docs; the master seed and the `--mutants` output.
- Commit as you go, small commits, named paths.

**Deliberately absent.** No confirmation token on subscribe: a handler receives `&mut
Tx` and nothing else (GUIDE §4), so an app cannot draw entropy inside a route by design,
and a brief that asked for it would teach a workaround. No hint of how anonymous reads
and authenticated writes coexist on one tenant's rows: GUIDE §16's `authorize` is one hook
per scope type receiving `Option<&Principal>`, and whether the docs lead a stranger from
that to a sound shape is the experiment.

## §4 Setup and sealing

All by the controller, before dispatch, and proven by the plan's preflight task.

- **Pinned sha.** charpente `main`, tree clean. Recorded in the run's README before
  anything else.
- **Tooling.** `cargo install --path crates/charpente-cli --locked --force` so the
  `charpente` on PATH is built from the pinned sha (the CLI has no `--version`; the sha is
  the version). `cargo install cargo-mutants --version 27.1.0 --locked` present.
- **Run directory.** `~/Documents/GitHub/statuspage-run/` (attempt N ≥ 2:
  `statuspage-run-N/`), a sibling of the charpente checkout, holding exactly `BRIEF.md`,
  `docs/README.md`, `docs/GUIDE.md` copied from the pinned sha. The agent works only
  here; it creates `statuspage/` inside it.
- **The patch block.** The brief carries, verbatim, the lines to append to the
  scaffold's `Cargo.toml`, with paths relative to that manifest:

  ```toml
  [patch.crates-io]
  charpente = { path = "../../charpente/crates/charpente" }
  charpente-sim = { path = "../../charpente/crates/charpente-sim" }
  ```

  The workspace declares `charpente-core` and `charpente-macros` with `path` and
  `version`, so the two patches resolve the rest locally. **Preflight proves it**: the
  controller runs `charpente new preflight` in a scratch directory, appends the block,
  and requires `cargo clippy --all-targets -- -D warnings` and `charpente test --sim`
  green on the scaffold's own two properties, then deletes it. If the crates.io name
  reservation defeats `[patch]` (cargo needs a matching version in the index), the
  fallback is path deps in the two dependency lines themselves —
  `charpente = { path = "../../charpente/crates/charpente" }` — the same edit an adopter
  makes for a git dependency. The brief carries whichever form preflight proved, and
  the run's README says which.
- **Sealing.** Instructional in the brief, and mechanical after the fact: the builder's
  transcript is grepped for tool calls naming a path outside the run directory
  (`../charpente`, `crates/`, `DESIGN.md`, `PLAN.md`, `.superpowers/`, absolute paths
  into the checkout). Cargo compiling the patched crates is not a read; the agent
  opening one is. A hit voids the attempt.

## §5 Run protocol

- **One dispatch**, Opus, general-purpose, no name (a named dispatch routes through the
  teammate protocol and loses its return path). Its prompt is the brief and the run
  directory's path — nothing about charpente, the experiment, or the bar.
- **Resume, not restart.** If the agent stops before every deliverable exists — the
  verification tail is where implementers stall — the controller resumes the same agent
  with the fixed message "continue from your FINDINGS.md and git log", verbatim, and
  counts the resume in the run's README. No hints, no answers, no edits to the run
  directory between resumes.
- **Nothing runs in charpente's tree** during the build except reads. The app has its own
  `target/`; the controller's later gates (§6) run after the builder reports done.
- **Done** is the agent's report that every deliverable in §3 exists. The controller
  believes none of it until §6.

## §6 Review and adjudication

In this order, so the reviewer reads a verified artifact and a fail is attributed to the
first thing that actually failed:

1. **Frame.** charpente tree clean at the pinned sha; the transcript grep (§4) is empty;
   the app repository's log is inspected for the separate `version = 2` commit.
2. **App gates.** In the app: `cargo clippy --all-targets -- -D warnings`; `cargo test`;
   `charpente check-client` exit 0.
3. **Replay.** `charpente test --sim --universes 10000 --master-seed <recorded>
   --format json`, diffed against the agent's recorded output. Any difference voids the
   attempt until explained.
4. **Mutants.** The controller's `--mutants` runs per §2(c), output kept beside the
   agent's for comparison.
5. **Review.** One Opus reviewer, given the brief, the app, and GUIDE — **not**
   FINDINGS.md — writes its full review to `attempt-N/review.md` before returning a
   summary. Findings are Critical / Important / Minor; every Critical names the platform
   claim that should have refused it, or says "no claim".
6. **Adjudication.** The controller rules on each Critical per §2(b) in progress.md, with
   the citation, then reads FINDINGS.md as testimony and checks every claim it makes about
   charpente against the code — right, wrong, or right for the wrong reason — as the
   helpdesk write-up did.
7. **Verdict to Noah.** Pass or fail, with the rulings, before the ledger entry is written.

## §7 The fail loop

A fail names one platform finding — the first oracle that should have refused and did
not. The fix is its own bounded change on charpente `main`: an oracle, a refusal message,
a GUIDE section, each landed with a pin so it cannot regress silently — a compile-fail
fixture, a sim property, or a `doc_truth`-pinned block. If the fix contradicts a
DECISIONS.md entry, that entry is reopened for Noah's sign-off first (CLAUDE.md). Then
attempt N+1: a fresh Opus agent, `statuspage-run-N/`, the same `BRIEF.md` byte for byte
(its sha256 is in the record), `docs/` re-copied from the new pinned sha. Each attempt has
its own record and ledger entry. Domain-logic bugs, Importants, Minors, and friction never
trigger a re-run.

## §8 The record

`docs/experiments/2026-09-statuspage/` in charpente:

- `BRIEF.md` — byte-identical to what every attempt received; its sha256 in `README.md`.
- `README.md` — the write-up: pinned sha per attempt, dependency form used, resume
  count, verdict with every Critical's ruling and citation, master seed and replay
  result, the controller's `--mutants` counts, and a "read FINDINGS as testimony" section
  in the helpdesk write-up's shape.
- `attempt-N/FINDINGS.md` — copied from the app repository at its final commit — and
  `attempt-N/review.md`.

The app repository lives outside charpente. Whether it goes to GitHub as a launch artifact
is Noah's call at close-out; the write-up carries its final commit sha either way, and the
controller keeps a `git bundle` of it beside the run directory until that call is made.

Also in the same commits (hard rule 6): a ledger entry per attempt; on the pass, PLAN.md
marks sub-project 3 `[shipped <date>; record docs/experiments/2026-09-statuspage/]`. No
crate is added, so CLAUDE.md's inventory is untouched. No DECISIONS.md entry is
contradicted by this sub-project itself; a fix from a fail is checked on its own.

## §9 Oracles on the sub-project itself

What is mechanical about the experiment, as opposed to the app:

- the pinned sha and the clean-tree check bound "framework patches" to a `git` fact;
- the transcript grep bounds "docs-only" to a fact about tool calls;
- the master-seed replay bounds "green" to a reproduced batch, not a reported one;
- the controller's own `--mutants` run bounds "clean" to an instrument's output, not a
  FINDINGS claim;
- `BRIEF.md`'s sha256 bounds "same brief" across attempts;
- the reviewer writes to a file first, so a lost delivery costs a read, not a re-review.

The adjudication in §2(b) is the one judgment call, and it is bounded by requiring a
citation: a Critical fails the bar only against a claim the platform's own docs make.

## §10 Refused, so the plan does not grow them

- **Designing the platform's answer to public-read/authenticated-write now.** The
  experiment exists to find out what the docs yield; pre-empting it is the wrong order.
- **A second example crate in the workspace.** The record is docs; the app lives
  outside, like the helpdesk's branch.
- **Interactive building.** A session Noah drives makes his answers an uncontrolled input.
- **Capping the attempts.** A second fail is a second finding, not a reason to stop
  measuring; Noah sees each verdict and can stop it himself.
- **Publishing first.** Sub-project 5 owns the crates.io path; this build proves the
  scaffold and the fence, not the registry.

## §11 Exit criteria

1. An attempt passes §2(a)–(d) with every ruling cited, and Noah has signed the verdict.
2. `docs/experiments/2026-09-statuspage/` carries the brief, the write-up, and every
   attempt's FINDINGS and review.
3. Every platform fix taken between attempts is on `main` with its pin, and its ledger
   entry names the attempt that found it.
4. PLAN.md marks sub-project 3 shipped; the ledger's final entry names the next step
   (sub-project 5, the launch run, once sub-project 4 and the real publish are done).
