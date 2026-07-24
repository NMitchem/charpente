# M1 sub-project 6 — introspection polish + the Vörtex-style edge harness: design

Status: APPROVED — Noah ruled 2026-07-24: all eight §6 rulings adopted as recommended
(runtime registry walk; dev HTTP route deferred; classifier typing + pinning in scope,
wire-protocol typed errors deferred; new dev-only `crates/edge-harness` with on-demand
CI; introspection phase first; v0 minimal events enumeration; ephemeral describe disk;
trybuild adopted, scoped to demo-agent-hit errors).

## M1 decomposition (context)

M1 runs as six sub-projects (canonical list in the wal-segment-management spec :8-11,
approved 2026-07-07): (1) WAL segment management, (2) replication, (3) realtime,
(4) auth, (5) migrations, (6) **this spec** — "introspection polish + the Vörtex-style
edge harness." Sub-projects 1–5 are done. This is M1's last sub-project.

Both PLAN M1 exit criteria are already discharged — a follower promoted mid-write-load
loses nothing (sub-project 2, property 8 + the kill-9/promote demo-of-record) and a
reconnecting realtime client provably misses no events (sub-project 3, the reconnecting-
client oracle + the `Last-Event-ID` demo-of-record). This sub-project owns the remaining
two M1 bullets (PLAN.md:45-49), which have no exit criterion of their own:

- **Error/introspection polish:** `charpente describe`, structured sim-failure reports —
  the DESIGN §7 agent-interface quality bar gets its own test suite.
- **Edge harness:** the Vörtex-style black-box rig for the hyper edge — stock binary,
  TCP-proxy fault injection, process kill/pause. The sim proves the kernel; this covers
  the wire (`serve`).

## Scope

Three deliverables, plus a sort of the accumulated error-surface backlog:

1. **`charpente describe`** — the machine-readable contract emitter DESIGN §7 promises
   and the CLI already advertises (`charpente-cli/Cargo.toml:3`, `main.rs:1-3` both name
   it; no match arm exists). Discharges the schema-version surfacing deferred by the
   migrations spec (:373-374) and the event/route enumeration deferred by the realtime
   spec (:127).
2. **The error-quality test suite** — the test suite DESIGN §7 says "error quality is a
   feature with." Its spine is the long-deferred `trybuild` compile-fail suite for the
   three macros (tracked since M0 step 6; deferral rulings at migrations plan :649,
   macro-path spec :64, http spec :160-163 — resolved here), plus typing the sim fault
   classifier and tightening the pinning discipline on the highest-value runtime strings.
3. **The edge harness** — a Vörtex-shaped black-box rig: the stock invoicing release
   binary, a TCP proxy for network faults, real `kill -9`/`SIGSTOP` process control,
   driving real HTTP/1.1. It automates the wire-level versions of properties the sim
   already proves in-process, and reaches the classes the in-process sim structurally
   cannot (real hyper, real TCP framing, real process death).

Non-goals are enumerated in §7. This is the last M1 sub-project, so §5 sorts every
ledgered error-surface follow-up into folds-in-here or explicitly-deferred — no silent
drops before M1 closes.

## 1. `charpente describe`

### 1.1 The extraction mechanism

The contract is assembled at runtime, not statically. An app's routes/jobs/cron/tables
are known only after its `build_app` runs `.mount(routes![…])`, `.mount_jobs(jobs![…])`,
`.schedule(…)`, `.table::<R>()` — there is no static manifest to read, and the macros
emit registration code, not a contract file. So `describe` must run app code, exactly
as `charpente test --sim` must (it builds and execs the app's `sim` target). Consistent
with "app crates depend on `charpente` alone," the mechanism is:

- **`App::describe(&self) -> Contract`** — a pure walk of the already-constructed `App`'s
  private registries (`router.routes`, `job_defs`, `cron_entries`, the `Db`'s `Schema`
  tables + versions/fingerprints + migration steps, `AppConfig`/`DbConfig`, the OIDC
  mount state), returning a serde-serializable `Contract`. It reads registries only —
  no disk writes, no socket, no kernel. Lives in `charpente` (the `Contract` type and
  the method), so the app depends on nothing new.
- **A standard binary entry** — the app's `main` delegates argv dispatch to charpente:
  `charpente::launch(app)` (or the app keeps its hand-written `main` and calls
  `app.describe()` itself). `launch` inspects `argv`: `describe [--format json|human]`
  serializes `app.describe()` to stdout and exits 0; anything else serves as today.
  The invoicing `main.rs` migrates from `serve::serve(app)?` + park-loop to
  `charpente::launch(app)` (one-line change, and it makes `describe` free for the demo).
- **The CLI verb** — `charpente describe [--dir PATH] [--format json|human]` builds the
  app's default binary (`cargo build --message-format=json`, locate the artifact) and
  execs it with `describe --format …`, streaming stdout and propagating the exit code —
  the same build-then-exec shape as `sim::run`, joining `main.rs`'s `(args[0], args[1])`
  dispatch as a third arm. Build errors surface verbatim (the compiler is the oracle),
  exit 0/1/2 like the other verbs.

Rejected alternative — a compile-time manifest emitted by the macros and read by the CLI
without running app code — is dishonest: it cannot see which registrations `build_app`
actually performs (conditional mounts, config-driven schedules), so it would describe a
contract the running app does not have. The runtime walk is the only surface that is
true by construction.

### 1.2 The disk question

`App::describe` needs a constructed `App`, and construction opens the `Db` (needs a
`Disk`). Describe must not mutate or require the production data dir. Under the standard
`launch` entry, `describe` mode constructs the app against an **ephemeral throwaway disk**
(a fresh temp dir, discarded on exit) rather than the configured `./data` — the contract
is entirely static registry state, independent of stored rows, so an empty ephemeral
store yields the identical contract. This is a `launch`-owned decision: `launch` selects
the disk based on the verb, which is why the app delegates disk selection to it (an app
that keeps a hand-written `main` and calls `app.describe()` directly owns this choice).
See ruling 7.

### 1.3 Output shape

`Contract` is designed here (DESIGN §7 gives only the noun list). One serde struct,
sections in a fixed order, deterministic by construction (BTreeMap-ordered tables;
registration-order Vecs for routes/jobs/cron — both stable across runs):

```
Contract {
  charpente_version: str,          // build metadata: the framework version + git describe
  tables: [ TableInfo {
    name, version: u32,            // schema version — discharges the migrations deferral
    fingerprint: Option<u64>,      // the row-shape fingerprint (from TableOps)
    indexes: [ str ],              // secondary index field names
    migrations: [ { from: u32, to: u32 } ],   // registered steps, from_version → to_version
    builtin: bool,                 // charpente.jobs / .events / Session / ApiKey / CronRow
  } ],
  routes: [ RouteInfo {
    method, pattern, name, auth: bool,
    stream: bool,                  // RouteGlue::Sse discriminant — marks SSE routes
  } ],
  jobs: [ JobInfo { name, retry: RetryInfo, builtin: bool } ],
  cron: [ CronInfo { name, interval_nanos: u64 } ],
  events: [ EventInfo { name } ],  // see 1.4 — enumeration source is a ruling
  config: ConfigInfo {             // the operationally-load-bearing knobs, flattened
    body_cap, session_ttl,
    retain_segments, checkpoint_every, event_retention, migration_batch_size,
    store_path, oidc: bool,
  },
  telemetry: TelemetryInfo { … },  // see 1.5 — presently near-empty; M2 fills it
}
```

Human format (`--format human`, the default) is a compact rendering of the same struct —
grouped sections, one line per item — for the operator eye. JSON (`--format json`) is the
agent-facing surface and the pinned one. Both are produced from the one `Contract`, so
they cannot drift from each other.

### 1.4 Events enumeration

There is no `events![]` registry today (realtime deferred it, spec :127) and `Route`
carries only the `RouteGlue::Sse` discriminant, not the event *type* behind a stream
(`glue` is a `fn`). So the v0 minimal surface enumerates events by their **subscribable
identity**: each `#[route(SSE …)]` contributes its stream, and the `events` list is the
distinct set of event NAMEs reachable through those routes. Whether describe carries the
full emit-side event catalog (every `#[event]` type, whether or not an SSE route exposes
it) requires either a new `events![]` registry or a macro change to stamp `E::NAME` onto
the `Route` — see ruling 6. TS codegen, which would consume the full catalog, stays a
non-goal (§7).

### 1.5 Telemetry

DESIGN §7 lists "telemetry" in the noun list, but typed telemetry with change-time checks
is an M2 deliverable (PLAN M2). So v0 `describe` emits a `telemetry` section that is
honestly near-empty: build/version metadata only. The section exists so the shape is
stable when M2 fills it; the spec states plainly that the typed-telemetry contract is not
in this sub-project.

### 1.6 The HTTP dev surface

DESIGN §7 promises "a matching HTTP surface in dev." Whether that ships now (a built-in
dev-only `GET /_charpente/describe` returning the same `Contract` JSON) or is deferred is
ruling 2. Recommendation: **defer**, ship the CLI verb only. The `Contract` type and
`App::describe` are the reusable core; wiring a dev route is a small, low-risk follow-on
that can land when an app actually needs in-process introspection, and keeping it out now
avoids a debate about auth/exposure of the dev endpoint in this sub-project.

### 1.7 Testing describe

- **Shape pinned by test**, exactly as sim-cli pins its JSON (:139): the `Contract` JSON
  schema is asserted against a golden for a fixture app, so any field add/rename is a
  visible diff.
- **Determinism property**: two `describe` runs on the same app produce byte-identical
  output (the ordering guarantees make this hold; the test enforces it so a future
  HashMap regression is caught).
- **The invoicing worked example**: `examples/invoicing`'s `describe` output is pinned,
  and GUIDE gains a describe section quoting it (docs-in-same-commit, hard rule 6) — the
  demo app is the end-to-end proof that the contract an agent reads matches the app it
  built.

## 2. `charpente describe` in the CLI (surface summary)

Joins `main.rs`'s dispatch as `(Some("describe"), _) => describe::run(&args)`. New module
`charpente-cli/src/describe.rs`. Flags: `--dir PATH` (app crate location, default cwd,
matching `sim.rs`), `--format json|human` (default human). Exit 0 success, 1 the app
binary exited nonzero, 2 build/usage error. `USAGE` const added and joined to the
combined usage message. No config file, no target discovery beyond the default bin — thin,
like `test --sim` (sim-cli spec :98-106).

## 3. The error-quality test suite

`charpente test --sim`'s `--format json` already emits per-failure structured records
(sim-cli spec :86-93). So "structured sim-failure reports" is **not** a new report format —
the honest gaps are three, and this is where the repeated "not yet" trybuild rulings
finally resolve.

### 3.1 The trybuild compile-fail suite (the spine)

Add `trybuild` as a dev-dependency of `charpente-macros` (its first appearance anywhere —
resolving progress.md:69 and the deferral rulings). Compile-fail cases for `#[table]`,
`#[route]`, `#[job]` — each `.stderr` file pins the exact diagnostic byte-for-byte, giving
hard rule 5 ("error messages are product surface; write tests against error output") its
**compile-time** arm (the runtime arm — sim failure text — already exists).

Coverage is prioritized by what the demo agent actually hit (FINDINGS.md, per the brief
and CLAUDE.md:262-263), not by enumerating all ~39 syn::Error sites:

- The `routes![…]`/`jobs![…]` **bare-identifier-only** rejection — FINDINGS "no rules
  expected `::`" cost the demo agent a compile cycle. Pin the message; and separately
  decide whether the macro should accept path-qualified idents (a small `macro_rules!`
  change) or the message should name the fix explicitly ("use a bare identifier; `use` the
  handler into scope first"). The pinned `.stderr` makes either choice a product decision,
  not an accident.
- `#[index]` on an **enum / non-`Codec`-bound field** — FINDINGS §4 hit uncertainty here;
  pin the actual rejection (or, if it silently compiles today, the suite surfaces that gap).
- The high-value existing rejections the agent relies on (`#[table]` :9/:17/:23/:44/:52/
  :76/:147; the `#[route]` argument-convention errors; the `#[job]` `ACCEPTED_FORMS`
  rejection) — a representative pin per macro so drift is caught, not exhaustive coverage.

The router mount-time conflict *panic* (`router.rs:101-107`) and app mount-time panics
(`app.rs:123/218/227/245`) are runtime, not compile-time; they stay pinned by their
existing unit tests. The suite does not move them to trybuild.

### 3.2 Typing the sim fault classifier

`checks::is_fault_fallout` (and the internal guard it productized) matches on **substring
wording** across three families (injected-fault text, our poison text, redb latch text).
Substring matching is brittle: a reworded error silently reclassifies a fault as unrelated
poison (progress.md:15 flags exactly this, "consider typed sim errors, M1"). This
sub-project introduces a **typed fault-reason enum** the classifier returns/matches
(`FaultReason::{Injected, Poisoned, RedbLatch, Unrelated}`), so classification keys on a
stable variant, not prose. The human strings stay (product surface); the classification
stops depending on their exact words. Self-contained in `charpente-sim`; the internal
suites and app workloads move to the typed matcher. This is the "structured" in
"structured sim-failure reports" that goes beyond the shipped JSON.

### 3.3 Pinning discipline

Several runtime trust-boundary strings are **fragment-pinned** (substring asserts), not
byte-exact: the diverged-family refusals (class-wide, noted at the diverged-behind close-
out), and the replication `NotPrimary`/re-Hello/`RESTORE_NEEDED` seams. Where these are
genuine product surface an agent or operator reads, the suite tightens them to byte-exact
assertions (mirroring the auth 401 family and the migrations error strings, both already
byte-exact and GUIDE-quoted). This is applied to the highest-value strings, not a blanket
rewrite of every fragment assert.

Whether the broader **wire-protocol typed error kinds** (the replication trust-boundary
seams as typed variants carried on the wire, named twice in the replication and auth close-
outs) land here or stay a follow-up is ruling 3. Recommendation: the *sim classifier*
typing (3.2) and the *pinning tightening* (3.3) land here; the *wire-protocol* typed error
kinds stay a follow-up — they touch the replication codec and correctness paths, and this
sub-project's risk budget belongs to describe + the edge harness.

## 4. The edge harness

Vörtex-shaped (NEIGHBORS.md:43-45): stock release binaries, TCP-proxy network faults, real
process kill/pause, driving real HTTP/1.1. It is the black-box complement to the in-process
sim — it does not replace it.

### 4.1 Shape

- **Target: the stock invoicing release binary** (built via `cargo build --release`,
  unmodified — the same artifact `charpente serve`/`serve_replicated` produces). Real
  process, real hyper edge, real TCP.
- **Network faults: a TCP proxy** the harness owns, sitting between the client and the
  app's listen port (and between replication peers for the failover oracle). It drops,
  delays, and partitions whole connections — the Vörtex model. Fault granularity is
  deliberately coarse (connection-level, process-level); see 4.4.
- **Process control**: the harness spawns the app as a child and can `kill -9`,
  `SIGSTOP`/`SIGCONT`-pause, and restart it at arbitrary points relative to observed acks.
- **Client: real HTTP/1.1** (a thin blocking client, or `curl` subprocesses for the SSE
  tail) — real wire bytes, the surface the in-process sim (which calls `App::handle`
  directly) cannot reach.

### 4.2 Oracles — the wire-level versions of what the sim proves

1. **Acked-implies-durable at the wire.** Fire real HTTP writes; on each `200`, record the
   write; `kill -9` the child at arbitrary points (including mid-commit, driven by the
   proxy observing the ack); restart; read every recorded write back over HTTP; assert
   none lost. This is the **wire-level `kill -9` rig the demo-app spec (:135) explicitly
   deferred to this sub-project** — the in-sim acked-durability property proven in-process
   at M0 complete, now proven end-to-end through hyper.
2. **Promote/failover.** Two `serve_replicated` invoicing nodes over loopback; write load
   on the primary; `kill -9` the primary; `charpente promote` the follower; assert no
   acked write lost and the follower now accepts writes. This **automates the replication
   close-out's manual demo-of-record** (50/50 acked curls survive kill-9 + promote).
3. **Realtime resume.** `curl -N` SSE tail against the invoicing binary; `kill` mid-stream;
   reconnect with `Last-Event-ID`; assert exactly the missed frames arrive, and that a
   garbage/cross-timeline cursor gets the pinned 400/409. This **automates the realtime
   close-out's manual demo-of-record**.

### 4.3 Where it lives + the deny-list

The rig spawns processes, opens real sockets, and reads the wall clock — all ambient IO,
all deny-listed inside the deterministic boundary. It lives in a **new dev-only crate,
`crates/edge-harness`** (not published; not a workspace default-member of the platform
crates), which takes the same site-local `#[allow(clippy::disallowed_methods)]` + rationale
treatment `charpente-cli` already uses for `std::process::Command` (sim-cli T7) and the
harness thread pool uses for `std::thread::spawn`. It depends on `examples/invoicing` as a
dev target and on the `charpente` CLI verbs (`promote`) it drives. A dedicated crate (vs
folding into `charpente-cli`) keeps wall-clock/process-control test infrastructure out of
the shipping binary — see ruling 4.

### 4.4 Non-determinism honesty (load-bearing)

This rig is **deliberately outside the sim**. It uses real wall-clock time, real OS
scheduling, real TCP. Consequences, stated plainly:

- **No seed promise.** A failure here is not byte-reproducible the way a sim seed is. What
  replaces the seed: the harness records the exact scenario it drove — the fault script
  (which connections dropped/paused when, the kill point relative to observed acks) and the
  observed divergence — and reruns from that recorded script. Timing is best-effort, so a
  rerun is a strong repro, not a byte-identical one.
- **The sim stays the reproduction authority.** Hard rule 3 is intact: an edge-harness
  finding that indicts the kernel or core gets **reproduced in the sim with a pinned seed
  before it is fixed** — the edge harness *finds*, the sim *reproduces*. A finding that is
  genuinely edge-only (serve.rs wire handling the sim cannot model) lands a deterministic
  edge regression test instead. The edge harness never becomes a place bugs get "fixed"
  without a reproduction.
- **The Jepsen granularity caveat (NEIGHBORS.md:46-49).** Byte-level corruption is a
  reproducible fault axis and belongs in the sim (WAL-scoped, per the wal-segment ruling).
  The edge harness stays at connection/process granularity — it does not attempt single-
  byte wire corruption, because that fault has no reproducible home here. The two rigs
  partition the fault space by where each fault can be honestly reproduced.

### 4.5 Named targets — the bugs it is built to catch

The harness earns its keep by catching classes the sim structurally cannot. Concrete first
hunts (from the ledger, research item 8):

- **`Db::poisoned()` read-side gap** (progress.md:123): poison is tracked apply-side, not
  redb read-side, so a read-serving node could 500 persistently after a transient read
  fault. A black-box rig driving real HTTP with an injected read fault is well-positioned
  to expose this. If reproduced, the fix lands with a sim regression seed (per 4.4).
- **serve.rs hardening (#25)**, open since M0 step 5 — direct edge territory; the harness
  is the vehicle that surfaces what needs hardening (slow-loris, half-open connections,
  oversized/short bodies at the wire).
- **ctl connect-refusal + malformed-reply** and the **`CtrlSwitchover` secret arm** — the
  harness drives `charpente promote`/`switchover` against a real (or absent, or
  misbehaving) node end-to-end.

### 4.6 The meta-test — proving the rig has teeth

The harness equivalent of the split-done-mark meta-test (the house pattern — a rig that
cannot catch its target is worthless). A **deliberately-broken build** the harness MUST
red: either the demo-app's recorded dual-write bug (the outbox split-transaction escape
hatch, WRONG commit c0dbcea, already reverted) rebuilt at the wire, or a serve-edge variant
that acks before the WAL sync (ack-before-durable). The meta-test asserts the acked-implies-
durable oracle turns that build red; a guard-reverted run proves the oracle would pass the
correct build. Without this, the harness proves nothing.

## 5. Ledgered follow-ups: in-scope vs deferred

Every named follow-up from the research (item 8), sorted. No silent drops.

**Folds into this sub-project:**

- *Typed error kinds at trust-boundary seams* → §3.2 (the sim classifier typing lands
  here); the wire-protocol half is ruling 3 (recommended deferred).
- *ctl connect-refusal + malformed-reply tests* → §4.5 (edge harness drives them end-to-
  end); the cheap deterministic cases can also land as `charpente-cli` unit tests in §3.
- *`CtrlSwitchover` secret arm untested end-to-end* → §4.5 (edge harness). *`Some("")`
  behaves as `None`* → a deterministic normalization test in §3 (cheap, no rig needed).
- *serve.rs hardening #25* → §4.5 (the harness is the vehicle; actual hardening fixes land
  as findings with reproductions per 4.4).
- *`Db::poisoned()` read-side gap* → §4.5 (named first hunt).
- *realtime encode-error whole-node-fatal contract has no pinning meta-test* (a hard-rule-3
  gap) → §3, add the pinning meta-test (a determinism/contract test, sim-side, cheap).
- *diverged-family refusals fragment-pinned not byte-exact* → §3.3 (the pinning-discipline
  tightening).
- *Doc hygiene* — the stale `fault:conn-drop` token (repl spec §4) and the DESIGN §1 same-
  step wording → fixed under the docs-in-same-commit discipline (hard rule 6) as the
  relevant sections are touched.

**Explicitly deferred (with reason):**

- *Wire-protocol typed error kinds* (RESTORE_NEEDED etc. as typed wire variants) — ruling
  3; touches the replication codec and correctness paths, outside this sub-project's risk
  budget. The sim classifier typing (3.2) is the in-scope portion.
- *realtime "gentler per-tail-drop shape"* decision, and *gap-frame `id:` line defeats
  EventSource auto-heal* — these are realtime *behavior* redesigns, not error/introspection
  polish; they belong to a realtime follow-up, not here. (The encode-error *pinning* test
  is in-scope; the *design change* is not.)
- *`EVENT_SINK_BUFFER` applied twice (~512 effective)* and *T7 pump-linger under silent-
  disconnect churn* — realtime internals; the pump-linger is edge-harness-*adjacent* (a
  silent-disconnect scenario) but fixing it is a realtime change. Note as a candidate
  scenario; defer the fix.
- *Replication-suite hygiene* — seed 0x94a70e2360154b09 unpinned; swarm inconclusive-rate/
  runtime tuning; `read_from` read_error test; Seeder wire-id assert + seed-path error
  text; `EmptySource` debug_assert; backup-test flipped wiring — these are replication-
  suite maintenance, neither introspection nor edge. Deferred to a dedicated replication
  cleanup pass; listed here so they are not lost at M1 close.

## 6. Proposed rulings (Noah)

1. **Describe extraction mechanism.** Recommend: **runtime registry walk** (`App::describe`
   run via the app binary through `charpente::launch`, execed by the CLI verb — §1.1).
   Alternative: a compile-time manifest read without running app code — rejected as
   dishonest (cannot see conditional/config-driven registration).

2. **HTTP dev surface now or later.** Recommend: **defer** — ship the CLI verb + `Contract`
   + `App::describe` now; the dev-only `GET /_charpente/describe` route is a small low-risk
   follow-on (§1.6). Alternative: ship the dev route in this sub-project.

3. **Typed error kinds — here or follow-up.** Recommend: the **sim fault classifier typing
   (3.2) and pinning tightening (3.3) land here**; the **wire-protocol typed error variants
   stay a follow-up** (codec + correctness-path risk). Alternative: do the full wire-
   protocol typing now.

4. **Edge harness home + CI story.** Recommend home: a **new dev-only crate
   `crates/edge-harness`** with site-local deny-list allows (§4.3), not folded into
   `charpente-cli`. Recommend CI: **on-demand / not per-push** — like the replication swarm
   (~27 min), it spawns processes and takes wall-time; a short single-scenario acked-
   durability smoke *may* run per-push, the full matrix runs on-demand (a `just`/script
   target, and locally before an M1-close claim). Alternative: fold into `charpente-cli`;
   or run the whole matrix in CI.

5. **Phase ordering.** Recommend: **introspection first, edge harness second** — describe +
   the error-quality suite are lower-risk and self-contained, and the edge harness can
   consume `describe`'s contract for its oracles (e.g. enumerating routes to drive).
   Alternative: harness first (front-load the highest-uncertainty work); or interleave.

6. **Events enumeration in describe.** Recommend: **v0 minimal — enumerate events by
   subscribable identity** (distinct event NAMEs reachable through `#[route(SSE …)]`
   streams), no new registry, no macro change (§1.4). Alternative: introduce `events![]`
   (or stamp `E::NAME` onto `Route`) to carry the full emit-side catalog — pulls the
   deferred realtime enumeration mechanism forward, and is the prerequisite for TS codegen
   (still a non-goal either way).

7. **Describe's disk.** Recommend: **ephemeral throwaway disk** — `launch` constructs the
   app against a fresh temp dir in describe mode, so `describe` never reads or mutates the
   production data dir (§1.2). Alternative: open the configured `./data` (creating it if
   absent) — simpler wiring, but a describe touches production state.

8. **trybuild adoption.** Recommend: **yes, finally — scoped to the demo-agent-hit macros
   first** (§3.1), not exhaustive coverage of all ~39 sites in v0. This is the resolution of
   the repeated "not yet" deferral rulings. Alternative: keep deferring; or go exhaustive
   now.

## 7. Non-goals

- **TS client codegen** for events and routes (PLAN M1 lists it under Realtime; realtime
  spec deferred it; it needs the full event catalog ruling 6 recommends deferring). Not
  here.
- **WebSocket** anything (realtime non-goal, still).
- **OpenAPI / JSON-Schema derivation** from routes — `describe`'s `Contract` is charpente's
  own machine-readable shape, not an interop spec format.
- **The hosted control plane's introspection** — this is the single-binary `describe`, not
  a fleet/telemetry dashboard.
- **Typed telemetry with change-time checks** — M2 (§1.5 leaves the section stubbed).
- **`events![]` registry / macro event-catalog** — deferred with ruling 6's minimal
  recommendation.
- **Typed route/job auto-fuzzing** (driving the app from its registry) — an M1+ layer atop
  `test --sim` (sim-cli spec :18-19), not this sub-project.
- **Byte-level wire fault injection** in the edge harness — stays in the sim, where it is
  reproducible (§4.4).
- **Graceful shutdown / production process supervision** — the edge harness controls the
  child process for testing; it is not a supervisor for prod.

## 8. Testing / evidence bar

**Describe (deterministic — sim-first where it applies):**
- `Contract` JSON shape pinned against a fixture golden (like sim-cli's JSON, spec :139);
  any field change is a visible diff.
- Determinism property: two runs, byte-identical output.
- `examples/invoicing`'s `describe` pinned + quoted in GUIDE (docs-in-same-commit).

**Error-quality suite:**
- trybuild `.stderr` files pinned byte-for-byte for the prioritized macro errors (§3.1).
- The typed fault-reason classifier: a test proving reclassification-on-reword can no
  longer happen (the old substring matcher would misclassify; the typed one does not).
- The tightened runtime strings asserted byte-exact (§3.3).
- The realtime encode-error whole-node-fatal pinning meta-test (the hard-rule-3 gap).

**Edge harness (meta-tested — the rig must have teeth):**
- The meta-test of §4.6: a deliberately-broken build (dual-write outbox at the wire, or
  ack-before-sync) that the acked-durability oracle MUST red; a guard-reverted run passes
  the correct build. This is the harness's split-done-mark equivalent — it proves the rig
  catches what it must.
- Each oracle (acked-durability, promote/failover, realtime resume) green against the
  stock invoicing binary over a fault matrix of connection drops/pauses + kill points.
- Non-determinism honesty enforced procedurally: any core/kernel finding lands a sim
  reproduction seed before its fix (hard rule 3); edge-only findings land a deterministic
  edge regression.

**Demo-of-record:**
- The three edge oracles run against the stock invoicing binary are the demo-of-record —
  the automated successors to the two manual demos-of-record from the replication and
  realtime close-outs, plus the wire-level `kill -9` acked-durability rig the demo-app spec
  deferred here. Transcript in the ledger, per house practice.

**Standard close-out sweep:** `cargo fmt --check`, `cargo clippy --workspace --all-targets
-D warnings` (deny-list intact; the new dev crate's ambient IO under site-local allows),
`cargo test --workspace`, the pinned debug/release replay seeds (0x8bee1752811ae873,
0x348270b38f166529, 0xff159ed76d2806e4), a fresh invoicing 1024-universe batch, and the
guide gate — all green before the M1-close claim.
