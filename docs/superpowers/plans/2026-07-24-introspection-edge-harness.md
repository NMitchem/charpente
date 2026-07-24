# M1 Sub-project 6 — Introspection Polish + Edge Harness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship `charpente describe` (runtime contract emitter), the error-quality test
suite (trybuild + typed sim fault classifier + byte-exact pinning), and the Vörtex-style
black-box edge harness — closing M1.

**Architecture:** Three phases per the approved spec
(docs/superpowers/specs/2026-07-23-introspection-edge-harness-design.md, all 8 §6 rulings
adopted as recommended + 3 dated pre-flight amendments). Phase A: a read-only registry
walk (`App::describe -> Contract`) surfaced via `charpente::launch` and a CLI
build-then-exec verb. Phase B: compile-fail pins for the macros, a typed
`FaultReason` classifier in `charpente-sim::checks`, byte-exact tightening of the
replication/role error strings, and the empty-peer-secret named config error. Phase C: a
new dev-only `crates/edge-harness` crate driving a committed invoicing `node` binary over
real HTTP/TCP with kill -9/SIGSTOP process control and a TCP fault proxy, with a
meta-test proving the rig has teeth.

**Tech Stack:** Rust (edition 2024), serde/serde_json (already deps of `charpente`),
`trybuild` (new dev-dep, first appearance), std::net/std::process in the dev-only edge
crate under site-local deny-list allows. No other new dependencies.

## Global Constraints

Copied from the spec and CLAUDE.md; every task's requirements implicitly include these.

- **Spec governs.** The spec is docs/superpowers/specs/2026-07-23-introspection-edge-harness-design.md
  at commit ea7fe4c (8 rulings + 3 amendments). Deviations go back to the controller, not
  into code.
- **No ambient nondeterminism inside the core** (CLAUDE.md hard rule 1). New ambient-IO
  sites (launch's temp dir, the edge crate's sockets/processes/clocks) take the
  site-local `#[allow(clippy::disallowed_methods)]` + rationale-comment pattern
  (exemplars: crates/charpente-cli/src/sim.rs:46-51, crates/charpente-sim/src/harness.rs:285-298).
- **Error messages are product surface** (hard rule 5): a contract violation names the
  contract, the site, and the fix; every NEW named error string in this plan is pinned
  byte-exact by a test in the same task.
- **Assertions in the TigerBeetle style** (hard rule 4): assert invariants and negative
  space; assertions stay on in release.
- **Docs in the same commit** (hard rule 6) — discharged at the phase docs tasks (T5,
  T14) per house precedent; any task that CHANGES an existing documented behavior fixes
  the doc in its own commit.
- **Sim-first** (hard rule 3): any core/kernel bug found by the edge harness gets a sim
  reproduction with a pinned seed BEFORE its fix; edge-only findings land a deterministic
  edge regression test instead (spec §4.4).
- **Contract determinism**: two `describe` runs on the same app produce byte-identical
  output. Tables render in BTreeMap (sorted-name) order; routes/jobs/cron in
  registration order. No HashMap anywhere in the Contract path.
- **`charpente_version`** is `env!("CARGO_PKG_VERSION")` only ("0.0.0" today). The
  spec's "git describe" half is deliberately dropped: a build.rs shelling to git breaks
  non-git builds and adds build-time ambient IO for zero contract value at version
  0.0.0. (Plan-level resolution; final review may re-open.)
- **Frozen replay contract**: `run_universes!`, every pinned seed, and the legacy
  sampling order are untouchable. New sim tests use the swarm runner / fresh base seeds
  in each file's established range convention.
- **Workflow**: commit directly to `main`, no Co-Authored-By trailers. One commit per
  task unless a task says otherwise.
- **Edge harness CI story (ruling 4)**: every `edge-harness` test is `#[ignore]` — the
  full matrix runs on demand (`scripts/edge-harness.sh` + locally before the M1-close
  claim), never per-push. `.github/workflows/ci.yml` is NOT modified by this plan.
- **Kill -9 honesty**: killing a process does NOT drop OS-page-cache writes — process
  death is not power loss. The harness therefore proves ACK ORDERING (nothing is acked
  before the commit path ran) and end-to-end crash recovery through real hyper/TCP; the
  sim owns power-loss/torn-write durability (its fault matrix). This boundary is stated
  in the harness crate docs (T10) and shapes the meta-test (T11).

---

## Phase A — `charpente describe` (introspection first, ruling 5)

### Task 1: Core read surface — index names into the registry, `Db::describe_tables`

**Files:**
- Modify: `crates/charpente-core/src/db.rs` (TableOps at :118-127, `Schema::table` at
  :144-236 region, new `Db` accessor near `table_names`/`schema_digest` at :1575-1591)
- Test: `crates/charpente-core/tests/engine.rs` (or a new
  `crates/charpente-core/tests/describe_tables.rs` if engine.rs has no Schema fixtures —
  implementer's call, say which in the report)

**Interfaces:**
- Consumes: existing `TableOps { apply_put, apply_delete, rebuild_indexes, version, fingerprint }`
  (private), `Row::INDEXES: &'static [IndexDef<Self>]`, `Schema::migrations_for`,
  `MigrationStep` (crates/charpente-core/src/migration.rs — read it for the exact
  from/to field names before writing code; the registered steps carry both versions).
- Produces (relied on by T2):
  ```rust
  /// One registered table's introspectable identity — the describe walk's raw
  /// material (spec §1.3). Read-only; constructed only by `Db::describe_tables`.
  #[derive(Debug, Clone, PartialEq, Eq)]
  pub struct TableDescription {
      pub name: &'static str,
      pub version: u32,
      pub fingerprint: Option<u64>,
      pub index_names: Vec<&'static str>,
      /// Registered migration steps as (from_version, to_version), ascending.
      pub migrations: Vec<(u32, u32)>,
  }
  impl Db {
      /// Every registered table in schema (sorted-name) order — the describe
      /// walk (spec §1). Like `table_names`, this is a read of registry state
      /// only; it never touches rows.
      pub fn describe_tables(&self) -> Vec<TableDescription>;
  }
  ```

- [ ] **Step 1: Write the failing test**

In the chosen test file (uses the existing sim `Disk` fixtures the file already uses —
follow its established `Db::open` pattern):

```rust
#[test]
fn describe_tables_reports_name_version_fingerprint_indexes_and_migrations() {
    // Fixture rows: reuse the file's existing versioned test Row impls if it has
    // them (typed_tables/migration fixtures), else declare a minimal hand-impl
    // Row with one IndexDef and VERSION 2 plus a registered migration step,
    // exactly as crates/charpente/tests/migration_gate.rs does.
    let db = /* open with .table::<Widget>() and .migration::<WidgetV1ToV2>("Widget") */;
    let tables = db.describe_tables();
    let widget = tables.iter().find(|t| t.name == "Widget").unwrap();
    assert_eq!(widget.version, 2);
    assert!(widget.fingerprint.is_some());
    assert_eq!(widget.index_names, vec!["by_org"]);
    assert_eq!(widget.migrations, vec![(1, 2)]);
    // Platform tables ride along, in sorted order, with their real versions:
    let names: Vec<_> = tables.iter().map(|t| t.name).collect();
    let mut sorted = names.clone();
    sorted.sort();
    assert_eq!(names, sorted, "describe_tables must be sorted-name ordered");
    assert!(names.contains(&"charpente.jobs"));
    assert!(names.contains(&"charpente.events"));
}
```

- [ ] **Step 2: Run it, verify it fails** — `cargo test -p charpente-core --test <file>
  describe_tables_reports -- --nocapture`. Expected: compile error, `describe_tables`
  not found.

- [ ] **Step 3: Implement.** In `db.rs`:
  1. `TableOps` gains `index_names: Vec<&'static str>` (each name IS `&'static`, but a
     static slice OF them does not exist — `IndexDef<R>` is generic, so the names must
     be projected at registration time). Built in `Schema::table::<R>()`:
     `index_names: R::INDEXES.iter().map(|d| d.name).collect()`.
     (`TableOps` is `Clone`; a `Vec` clones fine. Do NOT try to make this a `&'static
     [&'static str]` — that would need a new const on `Row` and a macro change.)
  2. `Db::describe_tables` walks `self.engine.machine().schema` (same reach as
     `table_names`): for each `(name, ops)` in the BTreeMap, emit `TableDescription`
     with `migrations: schema.migrations_for(name).iter().map(|s| (s.<from>, s.<to>)).collect()`
     using the REAL field names from migration.rs.
  3. Doc comments on both: constraint-style, per house rules (why this is the only
     reach: "a live `Db` never hands its `Schema` back" — mirror the `table_names` doc).

- [ ] **Step 4: Run the test + neighbors** — the new test passes; then
  `cargo test -p charpente-core` green (the registry change must not disturb anything;
  `TableOps` is private so nothing else can see the new field).

- [ ] **Step 5: Commit** — `core: TableOps carries index names; Db::describe_tables read
  surface (describe T1)`.

### Task 2: `Contract` + `App::describe` + determinism/golden tests

**Files:**
- Create: `crates/charpente/src/describe.rs`
- Modify: `crates/charpente/src/lib.rs` (add `pub mod describe;`),
  `crates/charpente/src/http/app.rs` (pub(crate) read accessors),
  `crates/charpente/src/http/router.rs` (pub(crate) `routes()` accessor)
- Test: `crates/charpente/tests/describe.rs`

**Interfaces:**
- Consumes: T1's `Db::describe_tables`; `App { router, job_defs, cron_entries, oidc, config }`
  (crates/charpente/src/http/app.rs:156-172); `Route { method, pattern, auth, name, glue }`;
  `JobDef { name, retry, glue }`; `RetryPolicy { max, backoff }`; `CronEntry { name, interval, enqueue }`;
  `AppConfig { body_cap, db, session_ttl }`; `DbConfig` fields (db.rs:32-63).
- Produces (relied on by T3/T4/T5):
  ```rust
  // crates/charpente/src/describe.rs — all types derive serde::Serialize.
  // Field ORDER in these structs is the JSON order; it is part of the pinned
  // shape. No HashMap anywhere.
  #[derive(serde::Serialize)]
  pub struct Contract {
      pub charpente_version: &'static str,      // env!("CARGO_PKG_VERSION")
      pub tables: Vec<TableInfo>,               // sorted-name order (from T1)
      pub routes: Vec<RouteInfo>,               // registration order
      pub jobs: Vec<JobInfo>,                   // registration order
      pub cron: Vec<CronInfo>,                  // registration order
      pub config: ConfigInfo,
      pub telemetry: TelemetryInfo,             // stub — M2 fills it (spec §1.5)
  }
  #[derive(serde::Serialize)]
  pub struct TableInfo {
      pub name: &'static str,
      pub version: u32,
      pub fingerprint: Option<u64>,
      pub indexes: Vec<&'static str>,
      pub migrations: Vec<MigrationInfo>,       // { from: u32, to: u32 }
      pub builtin: bool,
  }
  #[derive(serde::Serialize)]
  pub struct MigrationInfo { pub from: u32, pub to: u32 }
  #[derive(serde::Serialize)]
  pub struct RouteInfo {
      pub method: &'static str,                 // "GET" | "POST" | ... | "GET" for SSE
      pub pattern: &'static str,
      pub name: &'static str,
      pub auth: bool,
      pub stream: bool,                         // RouteGlue::Sse discriminant
  }
  #[derive(serde::Serialize)]
  pub struct JobInfo { pub name: &'static str, pub retry: RetryInfo, pub builtin: bool }
  #[derive(serde::Serialize)]
  pub struct RetryInfo { pub max: u32, pub backoff_nanos: u64 }
  #[derive(serde::Serialize)]
  pub struct CronInfo { pub name: &'static str, pub interval_nanos: u64, pub builtin: bool }
  #[derive(serde::Serialize)]
  pub struct ConfigInfo {
      pub body_cap: usize,
      pub session_ttl: u64,
      pub store_path: String,
      pub retain_segments: usize,
      pub checkpoint_every: u64,
      pub event_retention: u64,
      pub migration_batch_size: u64,
      pub oidc: bool,
  }
  #[derive(serde::Serialize)]
  pub struct TelemetryInfo {}                   // deliberately empty until M2

  impl App {
      /// The machine-readable contract (DESIGN §7, spec §1): a pure walk of
      /// this App's registries. Reads registries only — no disk writes, no
      /// socket, no kernel.
      pub fn describe(&self) -> Contract;
  }
  pub fn to_json(c: &Contract) -> String;       // serde_json::to_string_pretty + '\n'
  pub fn to_human(c: &Contract) -> String;      // grouped sections, one line per item
  ```
  NOTE per the 2026-07-24 spec amendment: there is NO `events` field. SSE visibility is
  `RouteInfo.stream`.

- [ ] **Step 1: Failing tests.** `crates/charpente/tests/describe.rs` builds a small
  fixture app the way crates/charpente/tests/http.rs builds its Apps (sim `Disk` +
  `Entropy` from the `charpente-sim` dev-dep, one `#[table]` row with an `#[index]`
  field, one `#[route]`, one SSE `#[route]`, one `#[job]`, one `.schedule`). Three
  tests:

```rust
#[test]
fn contract_json_shape_is_pinned() {
    let app = fixture_app();
    let json = charpente::describe::to_json(&app.describe());
    // The FULL golden, as a raw string literal, assert_eq! — not substrings.
    // Write the implementation first at Step 3, paste its output here, then
    // REVIEW the pasted golden field-by-field against the Contract struct
    // (this is a pin, so the paste must be deliberate, not blind).
    assert_eq!(json, GOLDEN);
}

#[test]
fn describe_is_deterministic_across_runs_and_reopens() {
    let a = charpente::describe::to_json(&fixture_app().describe());
    let b = charpente::describe::to_json(&fixture_app().describe());
    assert_eq!(a, b, "two fresh constructions must render byte-identical");
}

#[test]
fn builtin_flags_mark_exactly_the_platform_tables_jobs_and_cron() {
    let c = fixture_app().describe();
    let builtins: Vec<_> =
        c.tables.iter().filter(|t| t.builtin).map(|t| t.name).collect();
    // The exact platform set — Session/ApiKey/CronRow (App::open) +
    // charpente.jobs/charpente.events/SchemaVersion (Db::open). Use the real
    // TABLE consts, not string guesses:
    // charpente::core::db::JobRow::TABLE etc. — resolve each at write time.
    /* assert the exact set, and that the fixture's own table is !builtin */
}
```

- [ ] **Step 2: Run, verify failure** — compile errors (`describe` missing).

- [ ] **Step 3: Implement.**
  - `app.rs`: add `pub(crate) fn router(&self) -> &Router`, `pub(crate) fn
    job_defs(&self) -> &[JobDef]`, `pub(crate) fn cron_entries(&self) -> &[CronEntry]`,
    `pub(crate) fn oidc_mounted(&self) -> bool`. Doc: read-only, describe-only reach.
  - `router.rs`: `pub(crate) fn routes(&self) -> &[Route]`.
  - `describe.rs`: the walk. `method` string via a `match` on `Method` (SSE routes are
    `Method::Get` + `stream: true`). `builtin` for tables: name is in the platform
    const set (use each platform Row's `TABLE` const — Session, ApiKey, CronRow,
    JobRow, EventRow, SchemaVersion; import from their real paths, checked at compile
    time). `builtin` for jobs/cron: the name(s) App::open itself registers — read
    app.rs/cron.rs for the platform sweep job + cron names and use the same consts the
    registration uses (grep `session_sweep`); an app-registered name is never builtin.
  - `to_human`: fixed section headers (`tables:`, `routes:`, `jobs:`, `cron:`,
    `config:`), one line per item, no trailing whitespace, LF endings — this string is
    part of T5's invoicing golden, keep it boring.
- [ ] **Step 4: Fill the golden, run all three tests** — green;
  `cargo test -p charpente --test describe`.
- [ ] **Step 5: Full neighbor check** — `cargo test -p charpente` (accessors must not
  disturb anything), `cargo clippy --workspace --all-targets -- -D warnings`, `cargo fmt --all`.
- [ ] **Step 6: Commit** — `charpente: Contract + App::describe registry walk, pinned
  golden + determinism (describe T2)`.

### Task 3: `charpente::launch` — argv dispatch, ephemeral describe disk, invoicing main

**Files:**
- Create: `crates/charpente/src/launch.rs`
- Modify: `crates/charpente/src/lib.rs` (`pub mod launch;` + `pub use launch::launch;`),
  `examples/invoicing/src/main.rs`
- Test: `crates/charpente/tests/describe.rs` (extend), plus launch.rs unit tests

**Interfaces:**
- Consumes: T2's `App::describe`/`to_json`/`to_human`; `serve::serve`;
  `prod::{ProdDisk, ProdEntropy}`.
- Produces (relied on by T4/T10):
  ```rust
  /// The standard app entry (spec §1.1): argv dispatch owned by charpente so
  /// `describe` is free for every app. `build` runs against the disk THIS
  /// function selects — the production dir for serving, an ephemeral temp dir
  /// for describe (spec §1.2, ruling 7) — which is why the app hands over a
  /// builder, not a built App.
  pub fn launch(
      addr: &str,
      data_dir: &str,
      build: impl Fn(Box<dyn Disk>, Box<dyn Entropy>) -> io::Result<App>,
  ) -> io::Result<()>;
  ```
  Behavior (argv after the binary name):
  - `[]` → serve: `ProdDisk::new(data_dir)`, build, `serve::serve(addr, app)?`, park
    loop (exactly what invoicing's main does today — moved here).
  - `["describe"]` / `["describe", "--format", "json"|"human"]` → ephemeral disk
    (`std::env::temp_dir().join(format!("charpente-describe-{:016x}", suffix))` with a
    `ProdEntropy` suffix — the established test pattern in serve.rs/prod.rs), build,
    print `to_json`/`to_human` (human is the default) to stdout, best-effort
    `remove_dir_all` the temp dir, return Ok → process exits 0.
  - `["describe", "--format", other]` → stderr
    `error: --format must be json or human — got '{other}'` + LAUNCH_USAGE, exit 2.
  - any other first arg → stderr `error: unknown verb '{arg}'` + LAUNCH_USAGE, exit 2.
    (Plan-level resolution of the spec's "anything else serves as today" sketch: a
    TYPO must not silently boot a server; serving is only the no-args path. Surface
    this in the task report for the final review.)
  - `LAUNCH_USAGE` const: `usage: <app> [describe [--format json|human]]  — no verb serves the app`.
- Testable split: `launch` parses argv then delegates to
  `pub(crate) fn dispatch(args: &[String], addr, data_dir, build) -> io::Result<i32>`
  so unit tests drive `dispatch` with synthetic argv (never spawning a server: only the
  describe/error arms are unit-tested; the serve arm stays covered by the existing
  loopback smoke test's use of `serve::serve`).

- [ ] **Step 1: Failing tests** — in launch.rs `#[cfg(test)]`: `dispatch(["describe"])`
  on a tiny builder returns 0 and the human header appears on captured output;
  `--format json` output starts with `{`; bad format returns 2; unknown verb returns 2;
  and (negative space) **the describe arm must never touch `data_dir`** — point
  `data_dir` at a path that does not exist and assert it still succeeds AND the path
  does not exist afterward.
- [ ] **Step 2: Run, verify failures.**
- [ ] **Step 3: Implement** launch.rs. Of the ambient-IO calls, only
  `std::fs::remove_dir_all` is actually deny-listed (plan review checked clippy.toml —
  `temp_dir`/`process::exit`/`env::args` are not); that one site gets the site-local
  allow + the rationale comment ("edge entry tooling, outside the deterministic core — DESIGN §1;
  the ephemeral dir exists so describe can never read or mutate production data").
  `dispatch` returns the exit code; `launch` calls `std::process::exit` only for
  nonzero (Ok(0) falls through to normal return in serve mode's park loop — mirror the
  exact semantics: describe mode exits the process, serve mode never returns).
- [ ] **Step 4: invoicing main.rs** becomes:
  ```rust
  fn main() -> std::io::Result<()> {
      charpente::launch("0.0.0.0:8080", "./data", invoicing::build_app)
  }
  ```
- [ ] **Step 5: Run** — launch unit tests green; `cargo test -p charpente`;
  `cargo build -p invoicing` (main still compiles); manual smoke:
  `cargo run -p invoicing -- describe --format json | head -5` prints the contract.
- [ ] **Step 6: Commit** — `charpente: launch() argv entry — describe on an ephemeral
  disk, serve as the no-verb default; invoicing main migrates (describe T3)`.

### Task 4: The CLI verb — `charpente describe`

**Files:**
- Create: `crates/charpente-cli/src/describe.rs`
- Modify: `crates/charpente-cli/src/main.rs` (third arm + combined usage)
- Test: `crates/charpente-cli/tests/describe.rs`

**Interfaces:**
- Consumes: T3's binary behavior (`<bin> describe --format F`), sim.rs's build-then-exec
  pattern (crates/charpente-cli/src/sim.rs — `build_and_locate` :122-157,
  `exit_code_for` :64-84, the hand-rolled artifact-JSON scan :168-196, the
  site-local-allow pattern :46-51).
- Produces: `describe::run(&args) -> i32`, `describe::USAGE`.
  - `USAGE`: `usage: charpente describe [--dir PATH] [--format json|human] [--bin NAME]\n       --dir PATH   the app crate directory to build and describe (default: .)\n       --bin NAME   which binary to describe when the crate has several (default: the sole binary, else the one named like the package)`
  - Build: `cargo build --message-format=json` in `--dir` (NOT `--release`; NOT the
    test target — this is `cargo build`, distinct from sim.rs's `cargo test --no-run`).
  - Locate: scan compiler-artifact lines for non-null `"executable":"…"` (bins are the
    only executables `cargo build` emits). Zero → exit 2 naming the fix. One → use it.
    Several → prefer the artifact whose `"name":"…"` (hand-scan, sim.rs style) equals
    the package name (single-line hand-parse of `<dir>/Cargo.toml`'s first
    `name = "…"`); if still ambiguous and no `--bin`, exit 2 listing the bin names and
    naming `--bin` as the fix.
  - Exec: the located binary with `["describe", "--format", fmt]`, streaming
    stdout/stderr; exit code passthrough 0/1, signal/launch failure → 2 (reuse
    sim.rs's `exit_code_for` logic — copy the ~20-line function, do not refactor
    sim.rs to share; two small copies beat a premature module).
- main.rs dispatch gains `(Some("describe"), _) => describe::run(&args[1..])` and the
  fallthrough usage becomes the three USAGE consts joined.

- [ ] **Step 1: Failing tests** — `crates/charpente-cli/tests/describe.rs`:
  usage errors (`--format bogus`, unknown flag) exit 2 with USAGE on stderr (drive the
  compiled CLI via `env!("CARGO_BIN_EXE_charpente")`, the established cli-test
  pattern); a `--dir` pointing at a directory with no Cargo.toml exits 2 with the cargo
  error verbatim ("the compiler is the oracle"); the happy path —
  `charpente describe --dir <repo>/examples/invoicing --format json` exits 0 and stdout
  parses as the Contract (assert it contains `"charpente_version"` and
  `"pattern": "/events/feed"` — the FULL golden pin lives in T5's invoicing test, not
  here, so this test survives route additions with only the T5 golden updating).
- [ ] **Step 2: Run, verify failures** (module missing → compile error).
- [ ] **Step 3: Implement** describe.rs per the interface block; site-local allows +
  rationale on each `Command` site.
- [ ] **Step 4: Run** — `cargo test -p charpente-cli` green (the invoicing build the
  happy-path test triggers is slow once, then cached — same cost profile as the
  existing sim-fixture dogfood tests).
- [ ] **Step 5: Commit** — `cli: charpente describe — build-then-exec the app's default
  bin (describe T4)`.

### Task 5: Phase A docs + the invoicing golden

**Files:**
- Modify: `DESIGN.md` (repo root; §7 — replace the describe promise with the shipped
  mechanism: Contract via runtime registry walk, launch entry, CLI verb, ephemeral
  disk, the no-events-in-v0 note), `docs/GUIDE.md` (new §13 "describe — the machine-
  readable contract": launch migration recipe, the CLI verb, and the invoicing JSON
  output quoted VERBATIM from the pinned golden), `CLAUDE.md` only at T14 (not here).
- Test: `examples/invoicing/tests/describe_golden.rs` (new)

**Interfaces:**
- Consumes: T2 `describe`/`to_json`, invoicing's `build_app`.

- [ ] **Step 1: The invoicing golden test** — builds the app with sim disk/entropy
  (exactly how examples/invoicing/tests/sim.rs constructs it), asserts
  `to_json(&app.describe())` equals the full pasted golden (raw string literal), and a
  second construction is byte-identical. Run it RED first with an empty golden, paste
  the real output, review the paste against invoicing's actual surface — 12 routes,
  and EXPECT THE BUILTINS (plan review): 4 jobs (3 app + the builtin-flagged
  SessionSweep) and 2 cron (the app's daily-overdue-check + the builtin-flagged
  platform sweep), plus the platform tables — then green.
- [ ] **Step 1b: CLI↔golden cross-check** — extend T4's cli happy-path test to assert
  the CLI's json stdout (ephemeral-disk path) equals the SAME golden literal
  byte-for-byte (comment in each file cross-referencing the other pin) — closing the
  sim-disk vs ephemeral-disk equivalence claim with an assert instead of an argument.
- [ ] **Step 2: GUIDE §13** — written AFTER the golden exists so every quoted string is
  sourced from the pinning test (the auth-T9 discipline). Includes the one-line main
  migration, `charpente describe` usage, and the quoted JSON. GUIDE code blocks
  compile: `cargo build -p guide-examples` green.
- [ ] **Step 3: DESIGN §7** — mechanism paragraphs, including the two honesty notes
  (ephemeral disk: a describe never touches production data; events: not enumerated in
  v0, stream-flagged routes only, deferred with ruling 6).
- [ ] **Step 4: Run** — invoicing tests + guide gate + `cargo test --workspace` once
  (Phase A is done; prove nothing drifted).
- [ ] **Step 5: Commit** — `docs+invoicing: describe worked example pinned; DESIGN §7 +
  GUIDE §13 (describe T5)`.

---
## Phase B — the error-quality suite

### Task 6: trybuild — the compile-fail suite (+ the `routes!`/`jobs!` path widening)

**Files:**
- Modify: `crates/charpente-macros/Cargo.toml` (add `[dev-dependencies]`:
  `trybuild = "1"`, `charpente = { path = "../charpente" }` — the cyclic dev-dep is
  legal cargo; if the workspace build rejects it, FALLBACK: move the whole suite to
  `crates/charpente/tests/trybuild.rs` + `crates/charpente/tests/compile_fail/` and say
  so in the report), `crates/charpente/src/http/router.rs:46-51` (`routes!` matcher),
  `crates/charpente/src/kernel/dispatch.rs:70-75` (`jobs!` matcher),
  `docs/GUIDE.md` (two doc notes, same commit — hard rule 6)
- Create: `crates/charpente-macros/tests/trybuild.rs`,
  `crates/charpente-macros/tests/compile_fail/*.rs` + `.stderr`,
  `crates/charpente-macros/tests/compile_pass/*.rs`

**The macro_rules widening (spec §3.1's open choice, resolved product-forward):**
`$handler:ident` → `$handler:path` in BOTH `routes!` and `jobs!`. The expansion
`<$handler as $crate::http::RouteDef>::route()` is already path-compatible; `:path`
matches every bare ident it matched before, so nothing breaks. This FIXES the
demo-agent friction (FINDINGS.md:128-133, "no rules expected `::`") instead of
documenting around it. GUIDE §5/§6 each gain one line stating path-qualified handlers
are accepted.

**Cases (each is a tiny standalone `fn main() {}` file; every `.stderr` pinned
byte-for-byte by trybuild):**

compile_fail/ — the demo-agent-priority + representative set (exact messages are in
crates/charpente-macros/src/table.rs, route.rs, job.rs at the cited lines; the `.stderr`
files are GENERATED by `TRYBUILD=overwrite cargo test -p charpente-macros --test
trybuild`, then REVIEWED line-by-line against the source error strings before commit):
1. `table_generic.rs` — `#[table] struct W<T> { id: Id<W<T>>, x: T }` → table.rs:9
   ("cannot be generic").
2. `table_enum.rs` — `#[table] enum E { A }` → table.rs:17 ("applies to structs").
3. `table_tuple_fields.rs` — `#[table] struct W(u64);` → table.rs:23 ("needs named fields").
4. `table_index_on_id.rs` — `#[index]` on the `id` field → table.rs:44 ("`id` is the
   primary key").
5. `table_id_untyped.rs` — `id: u64` → table.rs:52 ("must be `Id<W>`").
6. `table_missing_id.rs` — no id field → table.rs:76 ("missing `id: Id<W>`").
7. `route_missing_tx.rs` — handler without `tx: &mut Tx` → route.rs:115.
8. `route_unknown_method.rs` — `#[route(FETCH "/x")]` → route.rs:80 ("one of GET,
   POST, PUT, PATCH, DELETE, SSE").
9. `job_bad_retry.rs` — `#[job(retry(max = "three"))]` → job.rs's ACCEPTED_FORMS text.
   NOTE: table.rs:147 from the spec's list is GENERATED RUNTIME text (decode_row
   trailing-bytes), not a compile error — it is deliberately absent here; state that in
   the task report so the final review sees the spec deviation was factual.

compile_pass/ (trybuild `t.pass(...)`) — regressions that must KEEP compiling:
10. `index_on_enum.rs` — `#[index]` on a hand-`Codec` enum field (FINDINGS §4's
    uncertainty, proven unfounded: the macro imposes no bound beyond `Codec`). GUIDE
    gains the one-line bound statement (`#[index]` needs `Codec`, nothing more).
11. `path_qualified_registration.rs` — `routes![handlers::ping]` + `jobs![jobs::Tick]`
    through modules (locks in the widening).

- [ ] **Step 1:** Write `trybuild.rs` harness + all 11 case files. Run — cases 1-9 fail
  the trybuild run (no `.stderr` yet), 11 fails to compile (RED for the widening).
- [ ] **Step 2:** Widen the two matchers; case 11 passes.
- [ ] **Step 3:** `TRYBUILD=overwrite`, review every generated `.stderr` against the
  macro source strings, commit them as pins.
- [ ] **Step 4:** GUIDE notes; `cargo build -p guide-examples`; full
  `cargo test -p charpente-macros -p charpente`; clippy/fmt.
- [ ] **Step 5: Commit** — `macros: trybuild compile-fail suite (9 pins + 2 pass
  regressions); routes!/jobs! accept path-qualified handlers (error-suite T6)`.

### Task 7: The typed sim fault classifier

**Files:**
- Modify: `crates/charpente-sim/src/checks.rs`
- Modify (migration to the shared classifier): `crates/charpente/tests/transactions.rs:501`,
  `crates/charpente/tests/kernel.rs:661`, `crates/charpente/tests/http.rs:1122` (the
  three local `is_injected` defs and their ~16 call sites)
- Test: checks.rs unit tests (extend the existing
  `recognizes_all_recognized_families_and_nothing_else` fixture at checks.rs:145-167)

**Interfaces:**
- Produces:
  ```rust
  /// Why a universe's error text is (or is not) excusable fault fallout —
  /// the typed successor to substring-matching at call sites (spec §3.2).
  /// The prose stays product surface; classification keys on the variant.
  #[derive(Debug, Clone, Copy, PartialEq, Eq)]
  pub enum FaultReason {
      /// The sim's own injected-fault wordings (write/sync/read/misdirected).
      Injected,
      /// Our WAL/engine poison wording.
      Poisoned,
      /// redb's latch ("close and re-open the database").
      RedbLatch,
      /// The WAL's durable-horizon hard error (corruption of acked records —
      /// loud by design, not a bug).
      DurableHorizon,
      /// Everything else: NOT fallout; fails the universe for adjudication.
      Unrelated,
  }
  pub fn classify(text: &str) -> FaultReason;
  ```
  `is_fault_fallout(text)` becomes `!matches!(classify(text), FaultReason::Unrelated)` —
  the public bool surface and all its 20 existing call sites are UNTOUCHED in behavior.
  (The spec's enum sketch named three families + Unrelated; the fourth family,
  DurableHorizon, already exists in the shipped classifier doc at checks.rs:9-22 — the
  enum carries all four. State this spec-sketch delta in the report.)

**The migration:** each of the three local `is_injected` fns is REPLACED by
`charpente_sim::checks::is_fault_fallout` ONLY IF its substring set is semantically
identical to the shared one; where a local fn is deliberately narrower, keep the local
fn but reimplement its body over `classify` with a rationale comment stating the
delta. Plan-review ground truth to expect: transactions.rs:501 and http.rs:1122 are
NARROWER (3 needles — simulated/injected/durable-horizon, no poison, no latch,
deliberately) — those become e.g. `matches!(classify(t), Injected | DurableHorizon)`,
NOT the 5-needle shared bool (wholesale replacement would LOOSEN the oracle);
kernel.rs:661 is the identical-4-family case and can take the shared fn. No oracle may
get looser: for each file, list old-vs-new recognized sets in the task report.

**The reword-pinning meta-test (spec §8):** the honest claim is a SINGLE point of
prose-coupling. Extend the checks.rs fixture so every variant is pinned to its exemplar
strings (`classify(s) == Injected` for each injected exemplar, etc. — including the two
RESTORE-class strings from log.rs:182-188 and db.rs:1120-1126 as `DurableHorizon`/
`Unrelated`? — NO: verify against current behavior first; `is_fault_fallout` today
matches "durable horizon" only, and db.rs:1120's compacted-log text contains neither
needle — it is Unrelated today and workloads handle it separately (replication.rs:3527).
PIN THAT TRUTH: `classify(<db.rs:1120 text>) == Unrelated`, with a comment naming the
replication workload's own arm as the handler). Then add the discriminator: a
DELIBERATE reword of an exemplar that shares NO needle with any family (e.g. "the
write-ahead log was corrupted by an earlier fault" — NOT "WAL was poisoned earlier",
which still contains "poisoned" and would classify `Poisoned`) must classify
`Unrelated` — proving a product-text reword now fails HERE, loudly and by name, instead
of silently reclassifying inside some workload. Honesty note for the task report and
the enum's doc: the typed classifier CENTRALIZES substring matching into `classify`;
it does not eliminate prose-coupling — "a single point of prose-coupling" is the claim.

- [ ] **Step 1:** RED — write the extended fixture + enum tests (enum doesn't exist).
- [ ] **Step 2:** Implement `classify`; reimplement `is_fault_fallout` over it.
- [ ] **Step 3:** Migrate the three test files' local fns per the rule above.
- [ ] **Step 4:** `cargo test -p charpente-sim -p charpente -p sim-fixture` — every
  suite that consumes the classifier, green; clippy/fmt.
- [ ] **Step 5: Commit** — `sim: typed FaultReason classifier — one prose-coupling
  point, call sites keyed on variants (error-suite T7)`.

### Task 8: Byte-exact pinning + the empty-peer-secret named config error

**Files:**
- Modify: `crates/charpente/src/repl/session.rs` (NOTE, plan-review corrected:
  diverged_ahead :621 / diverged_crc :635 / diverged_behind :651 are PRIVATE fns, not
  pub(crate) — do not widen their visibility; tests reconstruct expected text with the
  same format args from scenario-known values),
  `crates/charpente-core/src/engine.rs:144-151` (re-Hello string gains a shared marker
  const), `crates/charpente/src/repl/follower.rs:364` (matches the marker const),
  `crates/charpente/src/repl/mod.rs` (empty-secret refusal at `Repl` construction),
  `crates/charpente-cli/src/ctl.rs` (`--secret` empty value → usage exit 2)
- Test: `crates/charpente/tests/replication.rs` (needle-list asserts at :1091, :1231,
  :1480, :1562, :1622-1645, :3814-3826 upgraded), `crates/charpente/tests/http.rs`
  (:243-257 upgraded), `crates/charpente/src/repl/role.rs` in-file tests (:218-262
  upgraded), new empty-secret tests

**The tightening rule (spec §3.3):** each fragment-pinned string family upgrades to
`assert_eq!` against the expected text RECONSTRUCTED with the same format arguments the
test already knows (the auth-401 discipline). Dynamic parts (seqs, CRCs, addrs) come
from the test's own scenario values. **CRC carve-out (plan-review Important #2):** `diverged_ahead` and `diverged_crc`
embed frame CRCs (session.rs:627, :637-638) the test neither computes nor receives as
scenario input — these two families STAY fragment-pinned, upgraded to needle +
known-value asserts (the seqs/base the test does know). Byte-exact applies to:
diverged_behind, NotPrimary-421, the Hello not-a-primary refusal, protocol-version
mismatch, role-file mismatch, and the re-Hello engine string. REHELLO_MARKER stays a
plain `&str` const — not a typed wire variant (ruling 3 holds).
Families in scope, with today's substring sites:
- diverged_behind byte-exact; diverged_ahead + diverged_crc needle + known-value
  (session.rs:621-663; tests replication.rs:1091/:1231/:1480/:1562)
- NotPrimary 421 body (app.rs:511-517; tests http.rs:243-257, replication.rs:3814-3826)
- Hello not-a-primary refusal (session.rs:571-577) and protocol-version mismatch
  (session.rs:559-564; test replication.rs:1622-1645)
- role-file mismatch + demote-requires-re-seed (role.rs:87-101; tests role.rs:218-262)
- the two RESTORE-class core strings (log.rs:182-188, db.rs:1120-1126) — pinned
  byte-exact where they are already deterministically produced (log.rs's has the
  horizon-collapse regressions in charpente/tests/transactions.rs; db.rs's has the
  compacted-log test — find each string's existing deterministic producer and tighten
  THERE; do not build a new rig for this). CAVEAT (plan review): db.rs:1120 embeds the
  redb open error as `{e}` — byte-exact only if the producer's `{e}` is provably
  stable; otherwise needle + known-value (base seq), stated in the report.
NOT in scope: converting any of these to typed WIRE variants (ruling 3 deferred that).

**The re-Hello marker:** production code at follower.rs:364 substring-matches
`"re-Hello"` — a reword silently breaks reconnect logic, not just a test. Add
`pub const REHELLO_MARKER: &str = "re-Hello";` beside the engine error (engine.rs), use
it in the format! AND in follower.rs's `contains` — plus a test asserting the full
engine string byte-exact and CONTAINING the marker (so neither side can drift alone).

**The empty-secret config error (2026-07-24 amendment):** at `Repl` construction (the
seam every serve_replicated/backup node passes through — find the constructor in
repl/mod.rs and place the check before any socket/state work):
```rust
if repl_cfg.peer_secret.as_deref() == Some("") {
    return Err(io::Error::new(io::ErrorKind::InvalidInput,
        "ReplConfig.peer_secret is Some(\"\") — an empty peer secret authenticates \
         any peer that presents no proof, which is almost never what a configured \
         secret intends; set a real secret, or use None to explicitly accept all \
         peers (DESIGN §8)"));
}
```
(Exact text above IS the product surface — pin it byte-exact.) CLI: `--secret` with an
empty value (`--secret ''`) → the same usage path as a valueless `--secret`, exit 2.
Tests: a deterministic two-host sim test asserting serve-side refusal by full string;
a cli test for the flag.

- [ ] **Step 1:** RED — upgrade every listed assert to byte-exact (they fail only if
  the reconstruction is wrong — run to prove they PASS against current text, then
  mutation-check ONE: perturb one format string locally, watch the test fail, revert;
  note the mutation-check in the report), and write the empty-secret tests (these are
  genuinely RED — the check doesn't exist).
- [ ] **Step 2:** Implement the marker const + empty-secret refusal + ctl arm.
- [ ] **Step 3:** `cargo test -p charpente --test replication --test http` +
  `cargo test -p charpente-cli` + role.rs unit tests; clippy/fmt. Replication is the
  long suite (~25-30 min) — run it ONCE here, in full.
- [ ] **Step 4: Commit** — `repl+core+cli: error strings byte-exact (diverged family,
  NotPrimary, role, re-Hello marker), empty peer secret refused by name (error-suite T8)`.

### Task 9: Encode-error pinning meta-test + ctl deterministic edge-case tests

**Files:**
- Modify: `crates/charpente/src/kernel/tail.rs` (the contract comment, same commit as
  its pinning test — hard rule 6)
- Test: `crates/charpente/tests/realtime.rs` (the meta-test),
  `crates/charpente-cli/tests/ctl.rs` (new)

**The encode-error meta-test (spec §5 in-scope bullet):** the shipped contract is that
an event encode error during tail delivery is whole-node-fatal (`?` chain:
tail.rs:90-104 → kernel/mod.rs:339-346 → step → run → serve's kernel thread exits). No
test pins it and no code comment states it. Build the pin exactly the way realtime.rs
builds its kernels: an SSE subscription whose `SubEntry.encode_json` is a hand-built
failing fn — construct the full `Subscription` in a HAND-WRITTEN SSE route (no
`#[route]` macro; implement `RouteDef` by hand returning `RouteGlue::Sse(glue)` whose
glue builds `Subscription { entries: vec![SubEntry { name: "note_added", encode_json:
|_| Err(io::Error::other("encode exploded")) }], key: None, cursor: None }` — the
struct has `key`/`cursor` fields, http/mod.rs:200). The kernel `Tail` is private and
`deliver_unit` is unreachable from an integration test — subscribe via the route,
emit a matching event, and drive `Kernel::step` until the Tail arm runs; assert
`step`/`run` returns `Err` whose text contains "encode exploded". Then the
comment in tail.rs states the contract IN CODE: "an encode error here is deliberately
whole-node-fatal (bubbles out of Kernel::run) — a tail that cannot render its frames is
a build bug, not a client condition; the gentler per-tail-drop shape is a ledgered
follow-up, and THIS test is its pin: realtime.rs::<test name>". (The design CHANGE
stays deferred per spec §5.)

**ctl tests (spec §5's cheap deterministic cases):** in `charpente-cli/tests/ctl.rs`,
driving `env!("CARGO_BIN_EXE_charpente")`:
1. `promote_against_a_dead_port_exits_2_with_the_connect_message` — pick an unused
   loopback port (bind-then-drop to find one), run
   `charpente promote --node 127.0.0.1:<port>`, assert exit 2 and stderr CONTAINS the
   fragment "is the node running with replication configured" plus the addr (the
   string at ctl.rs:201 embeds the OS error `{e}`, which varies by platform —
   `contains`, not byte-exact, per plan review).
2. `promote_against_a_garbage_reply_exits_2` — a std::net::TcpListener test stub
   (site-local allow + rationale: test drives the real CLI over loopback, the
   established serve.rs-test pattern) that accepts, writes `b"not a frame"`, closes;
   assert exit 2 and the decode/timeout error path's message.
3. `switchover_usage_errors_exit_2` — missing `--to`, valueless `--secret`: exit 2 +
   USAGE.

- [ ] **Step 1:** RED — the realtime meta-test (fails today only if the contract does
  NOT hold — run it; if it PASSES immediately, that IS the pin working; prove teeth by
  a local mutation: swap the `?` at kernel/mod.rs:346 for a log-and-continue locally,
  watch the test fail, revert, note in report). ctl tests genuinely RED where the
  behavior is untested (they should pass against current code — same mutation
  discipline: break the exit-code mapping locally once, watch them fail, revert).
- [ ] **Step 2:** The tail.rs contract comment.
- [ ] **Step 3:** `cargo test -p charpente --test realtime` + `cargo test -p
  charpente-cli`; clippy/fmt.
- [ ] **Step 4: Commit** — `kernel+cli: encode-error whole-node-fatal contract pinned
  (meta-test + in-code contract note); ctl refusal paths tested (error-suite T9)`.

---
## Phase C — the edge harness

### Task 10: The invoicing `node` bin + the `edge-harness` crate scaffold

**Files:**
- Create: `examples/invoicing/src/bin/node.rs`
- Create: `crates/edge-harness/` — `Cargo.toml`, `src/lib.rs`, `src/child.rs` (process
  control), `src/proxy.rs` (TCP fault proxy), `src/client.rs` (blocking HTTP/1.1 +
  SSE tail), `src/scenario.rs` (fault-script recording)
- Modify: root `Cargo.toml` `members` (+ `"crates/edge-harness"`)
- Create: `scripts/edge-harness.sh` (runs `cargo test -p edge-harness -- --ignored
  --test-threads=1`)
- Test: `crates/edge-harness/tests/smoke.rs`

**The node bin (2026-07-24 spec amendment):** plain `std::env::args` parsing, no new
deps:
```
node --addr A --data-dir D                                  # plain serve
node --addr A --data-dir D --repl-listen R --role primary [--secret S]
node --addr A --data-dir D --repl-listen R --role follower --primary P [--secret S]
node --data-dir D --repl-listen R --role backup --primary P [--secret S]
```
Maps 1:1 onto `build_app` + `serve::serve` / `serve::serve_replicated` /
`serve::backup` + the park loop (the GUIDE §10 worked-example shapes,
docs/GUIDE.md:2398-2438, are the reference — same ReplConfig construction). Bad/missing
flags → stderr usage, exit 2. This is a stock, committed repo artifact; `main.rs` (the
GUIDE demo binary) is untouched. Note for T4 interplay: invoicing now has TWO bins —
`charpente describe --dir examples/invoicing` must still resolve to the `invoicing` bin
via the package-name default (T4 built exactly this disambiguation; add a cli test
asserting it now that the second bin exists).

**The harness crate:** `[package] name = "edge-harness"`, not published
(`publish = false`), depends on nothing in the workspace at runtime (it drives built
binaries; add `charpente-sim` only if a helper is genuinely reused — default no deps
beyond std). All ambient IO under module-level rationale + site-local allows, mirroring
sim.rs/harness.rs exemplars. Crate-level doc comment states the §4.4 honesty contract
verbatim (no seed promise; scenario-script repro; the sim is the reproduction
authority; connection/process fault granularity only; kill -9 ≠ power loss — ack
ordering + crash recovery are the claims here, power-loss durability is the sim's).

Core APIs (Produces — relied on by T11-T13):
```rust
// child.rs
pub struct ChildNode { /* Child + args + data_dir + log paths */ }
impl ChildNode {
    /// Spawns `bin` with `args` in a fresh per-node temp dir; stdout/stderr
    /// tee'd to files under the scenario dir. Waits for the HTTP (or repl)
    /// port to accept before returning.
    pub fn spawn(bin: &Path, args: &[String], scenario: &mut Scenario) -> io::Result<ChildNode>;
    pub fn kill9(&mut self, scenario: &mut Scenario);          // SIGKILL via Child::kill
    pub fn pause(&mut self, scenario: &mut Scenario);          // SIGSTOP via `kill -STOP <pid>`
    pub fn resume(&mut self, scenario: &mut Scenario);         // SIGCONT
    pub fn restart(&mut self, scenario: &mut Scenario) -> io::Result<()>; // same args+dir
}
/// Builds every binary the rig drives, once per process (release): the
/// invoicing `node` bin and the `charpente` CLI (T12's promote/switchover
/// runs the REAL verb) — `cargo build --release -p invoicing -p charpente-cli
/// --bins`, artifact paths located exactly like charpente-cli/src/sim.rs's
/// message-format=json scan.
pub fn build_bins() -> io::Result<Bins>; // { node: PathBuf, charpente: PathBuf }

// proxy.rs — the Vörtex fault layer: listener L forwards to upstream U,
// per-connection control.
pub struct TcpProxy { /* listen thread + Arc<Mutex<ProxyState>> */ }
impl TcpProxy {
    pub fn start(upstream: &str) -> io::Result<TcpProxy>;      // ephemeral local port
    pub fn addr(&self) -> String;
    pub fn drop_all(&self, scenario: &mut Scenario);           // sever every live conn
    pub fn partition(&self, on: bool, scenario: &mut Scenario);// refuse/black-hole new conns
    pub fn delay_ms(&self, ms: u64, scenario: &mut Scenario);  // add per-chunk delay
}

// client.rs — hand-rolled blocking HTTP/1.1 over TcpStream (the serve.rs
// loopback-smoke lineage; no reqwest/curl dependency). Content-Length bodies,
// cookie jar for the session, header access for Last-Event-ID.
pub struct Http { /* base addr + session cookie */ }
impl Http {
    pub fn post(&mut self, path: &str, body: &str) -> io::Result<HttpResponse>;
    pub fn get(&mut self, path: &str) -> io::Result<HttpResponse>;
    pub fn sse_tail(&mut self, path: &str, last_event_id: Option<&str>) -> io::Result<SseTail>;
}
pub struct SseTail { /* stream + frame parser */ }
impl SseTail { pub fn next_frame(&mut self, timeout: Duration) -> io::Result<Option<SseFrame>>; }
pub struct SseFrame { pub id: String, pub event: String, pub data: String }

// scenario.rs — the §4.4 repro story: every fault action + observed ack is
// appended with a monotonic step counter and wall timestamp; on failure the
// script dumps to the scenario dir and the panic message names the path.
pub struct Scenario { /* Vec<ScenarioEvent>, dir: PathBuf */ }
```

- [ ] **Step 1:** node.rs + workspace member + crate skeleton compile:
  `cargo build -p invoicing --bins && cargo build -p edge-harness`.
- [ ] **Step 2:** The smoke test (`#[ignore]`, like every edge test):
  `smoke_single_node_serves_and_survives_restart` — build bins, spawn one node behind a
  proxy, POST /login (invoicing's login route), POST a customer, GET it back through
  the proxy, `kill9`, `restart`, GET it again. Run:
  `cargo test -p edge-harness -- --ignored smoke`. Expected: green, and a scenario
  script file exists.
- [ ] **Step 3:** `scripts/edge-harness.sh` + crate docs; clippy (`--all-targets`
  covers the ignored tests' code) + fmt.
- [ ] **Step 4: Commit** — `edge: invoicing node bin + edge-harness scaffold (child
  control, TCP proxy, HTTP/SSE client, scenario scripts) (edge T10)`.

### Task 11: Oracle 1 — acked-implies-durable at the wire, with the teeth meta-test

**Files:**
- Create: `crates/edge-harness/tests/acked_durable.rs`,
  `crates/edge-harness/patches/wrong-ack-before-commit.patch`
- Test: both tests below are `#[ignore]`

**The oracle:** N rounds (default 40, env-overridable): each round POSTs a customer
with a client-chosen id through the proxy, records the id on 200 (and ONLY on 200);
at randomized rounds the harness `kill9`s the node at a randomized small delay after
an observed ack (0/1/5/20 ms — recorded in the scenario), restarts, and re-logins.
After the final restart: GET every recorded id; ANY missing acked id fails with the
scenario-script path in the message. This is the wire-level successor to the M0 in-sim
acked-durability property (demo-app spec :135's deferral, discharged here).

**The meta-test (spec §4.6 — the rig must have teeth):** the committed patch makes the
kernel's request arm ack BEFORE the write is even ATTEMPTED (plan-review Important #1:
the transaction runs inside `handle_or_subscribe` at app.rs:412 — there is no
"transact → respond" pair in the kernel arm to reorder, and an ack-before-*fsync*
variant would survive kill -9 via the page cache, i.e. a toothless meta-test). The
wrong build: in the `Ready::Request` arm of `crates/charpente/src/kernel/mod.rs`,
inject a synthetic `Response { status: 200, … }` via the arm's reply channel BEFORE
calling `handle_or_subscribe`, `std::thread::sleep(100ms)`, then run
`let _ = self.app.handle_or_subscribe(…)` and discard its real response — the ack
precedes the write ATTEMPT, so a kill in the window loses the write entirely (nothing
in the page cache to recover). The oracle keys on status + client-chosen id, so the
synthetic 200 is sufficient. (The sleep is the WRONG build's sanctioned exaggeration,
exactly like the WRONG-commit c0dbcea demo.) Generate it by: `git worktree add` a
throwaway at HEAD, hand-apply that injection,
`git diff > patches/wrong-ack-before-commit.patch`, remove the worktree. The meta-test:
1. `git worktree add <scratch> HEAD` + `git apply patches/wrong-ack-before-commit.patch`
   + build invoicing's node bin there (release);
2. run the SAME oracle scenario with kill-on-ack forced every round against the wrong
   build → the oracle MUST report missing acked ids (assert `Err`, i.e. red);
3. the ordinary Oracle-1 run against the stock build is the guard-reverted green half
   (spec §4.6's "guard-reverted run proves the oracle would pass the correct build").
If the patch ever stops applying (kernel drift), the meta-test fails with "regenerate
the patch" in its message — that is the pin doing its job; regenerate deliberately.

- [ ] **Step 1:** Write the oracle test; run against stock — must be green
  (`cargo test -p edge-harness -- --ignored acked`). Any red here is a REAL FINDING:
  stop, capture the scenario script, and follow §4.4 (sim repro before fix; controller
  decides scope).
- [ ] **Step 2:** Produce the patch; write the meta-test; run — red half red, green
  half green.
- [ ] **Step 3:** clippy/fmt; commit — `edge: acked-implies-durable wire oracle + the
  ack-before-commit teeth meta-test (edge T11)`.

### Task 12: Oracles 2 + 3 — promote/failover and realtime resume at the wire

**Files:**
- Create: `crates/edge-harness/tests/failover.rs`, `crates/edge-harness/tests/realtime_resume.rs`
- Both `#[ignore]`.

**Oracle 2 (automates the replication demo-of-record):** primary + follower node bins
(distinct temp dirs, shared `--secret`), the peer link through a `TcpProxy`; 50 acked
writes under way with kills interleaved on the PRIMARY only; after the last ack:
`kill9(primary)`; run the REAL CLI — `charpente promote --node <follower-repl-addr>
--secret <s>` via the charpente binary (build it once with the invoicing bins) — assert
exit 0 and stdout naming the DECISIONS §6 bounded-loss window (byte-substring from the
T8-tightened text); then GET all 50 acked ids on the follower's HTTP addr (0 missing)
and POST one new write (follower-now-primary accepts). Also the negative arm: restart
the old primary, assert its stderr prints the demote/re-seed refusal (the pinned
role.rs text) and it exits nonzero.
**Bounded-loss honesty:** replication is async — an acked-on-primary write may not have
shipped when the kill lands. The oracle therefore drives acks THROUGH a
proxy-observed ship-ack barrier: after each POST, wait until the follower's
`/customers/:id` serves the row (read-your-replication), THEN count it "converged";
the zero-loss assertion is over converged ids, and the (acked − converged) tail is
asserted ≤ the in-flight window and REPORTED in the scenario (this mirrors property
8's converged-follower premise; a converged id missing after promote is the failure).
**Anti-vacuity (plan-review Important #3):** assert `converged.len() >= 40` (of the
50 writes) BEFORE the promote/zero-loss assertion — an empty or near-empty converged
set must fail the oracle, never pass it (property-8 promised-nonempty parity).

**Oracle 3 (automates the realtime demo-of-record):** one node; an SSE tail on
`/events/feed` (invoicing's stream); generate events (finalize invoices / record
payments — whatever emits on that stream per invoicing's routes); read K frames;
`kill9` mid-stream; restart; reconnect with `Last-Event-ID: <last seen id>`; assert the
resumed frames are EXACTLY the missed ids (compare against a concurrently-maintained
expected ledger from acked writes — set-equality with order); then the two refusals:
a garbage cursor (`Last-Event-ID: not-a-cursor`) → the pinned 400 byte-exact, and a
below-floor/foreign cursor arm ONLY IF cheaply arrangeable (a cursor from before a
retention horizon after enough events) — otherwise state in the report that the
cross-timeline 409 stays covered by the in-sim multi-node property and the T8 pins.

- [ ] **Step 1:** failover.rs; run stock-green (`--ignored failover`). Real findings
  follow §4.4, as in T11.
- [ ] **Step 2:** realtime_resume.rs; run stock-green.
- [ ] **Step 3:** clippy/fmt; commit — `edge: promote/failover + realtime-resume wire
  oracles (edge T12)`.

### Task 13: The named hunts

**Files:**
- Create: `crates/edge-harness/tests/hunts.rs` (`#[ignore]`)
- Create (sim side, primary vehicle for hunt 1): a test in
  `crates/charpente/tests/kernel.rs`
- Findings land per §4.4: sim seed first for core indictments; edge regression for
  edge-only.

**Hunt 1 — `Db::poisoned()` read-side gap (the ledgered production follow-up,
progress.md:238 / spec §4.5).** The sim, not the harness, is the right injector (the
harness cannot fault real disk reads). Deterministic kernel test: drive a read-serving
request path with an injected `read_error` that redb LATCHES ("Previous I/O error…
close and re-open the database"); assert the shipped behavior — subsequent requests
KEEP 500ing while `Db::poisoned()` stays false and the kernel keeps serving (the gap,
pinned as a characterization test with a comment naming it a KNOWN GAP). THEN STOP —
the fix (read-side poison → kernel stop/restart semantics) is a product-design
decision that touches step-7's request-arm ruling; write the characterization + a
one-paragraph fix proposal in the task report and HALT the task for a controller/Noah
ruling before implementing any fix. (The edge test in hunts.rs is a thin companion:
persistent-500 visibility at the wire is what the operator sees; it drives GETs only
and never asserts the fix.)

**Hunt 2 — serve.rs hardening (#25, open since M0 step 5).** Probes in hunts.rs, each
its own `#[ignore]` test, each ASSERTING today's contract where one exists and
RECORDING (report, not assert) where none does: (a) oversized body at the wire
(> 1 MiB) → 413, connection stays sane; (b) truncated body (Content-Length 100, send
40, close) → connection-level failure, node keeps serving the NEXT request; (c)
slow-loris: 20 connections dripping one header byte/sec — the node must still answer a
fresh 21st connection within 2s (liveness assert); (d) half-open peer (SIGSTOP'd
client) doesn't wedge the accept loop. Any hang/crash is a finding: edge-only →
deterministic edge regression here; kernel-indicting → sim seed first (§4.4).

**Hunt 3 — ctl against a real (or absent, or misbehaving) node end-to-end + the
`CtrlSwitchover` secret arm.** With two live nodes from the T12 rig: `charpente
switchover --primary <p> --to <f> --secret wrong` → exit 1 with the peer-secret
refusal — byte-exact against a HARDCODED copy of the expected string in the edge test
(`PEER_SECRET_REFUSAL` is pub(crate) at session.rs:512, not importable from the
edge-harness crate; the hardcoded copy is itself a pin, its comment says so). This
closes the ledgered "CtrlSwitchover secret arm untested end-to-end"; with the right secret → exit 0 and
the handoff completes (write gate flips, old primary 421s naming the new).

- [ ] **Step 1:** Hunt 1 sim characterization test (RED-first against the CLAIM if the
  gap is real — it should PASS as a characterization; mutation-prove it would catch
  the fix landing by flipping the assert) + report + HALT-for-ruling marker in the
  ledger.
- [ ] **Step 2:** Hunt 2 probes; run; triage findings per §4.4.
- [ ] **Step 3:** Hunt 3; run.
- [ ] **Step 4:** clippy/fmt; commit — `edge+kernel: named hunts — read-side poison gap
  characterized (HELD for ruling), serve hardening probes, switchover secret arm
  end-to-end (edge T13)`.

### Task 14: Close-out — docs, doc hygiene, the sweep, M1 CLOSE

**Files:**
- Modify: `DESIGN.md` (repo root; §7 edge-harness paragraph: what the rig proves, the §4.4
  honesty contract, where it lives; the §1 same-step wording fix — spec §5 doc-hygiene
  bullet), the replication spec's stale `fault:conn-drop` token
  (docs/superpowers/specs/2026-07-08-replication-design.md §4 — correct it to the
  shipped `conn_drop` key), `docs/GUIDE.md` (edge-harness one-paragraph pointer — it is
  dev tooling, not app surface; plus anything T6-T13 left), `CLAUDE.md` (the
  sub-project 6 entry + **M1 COMPLETE**), `.superpowers/sdd/progress.md` (close-out
  entry)
- No code changes in this task (fix waves from the final review are their own commits).

- [ ] **Step 1: The final whole-branch review** (controller dispatches per the SDD
  skill — opus, whole diff from the pre-T1 base). Fix wave if needed, then re-verify.
- [ ] **Step 2: The close-out sweep** (spec §8, all green before any M1-close claim):
  `cargo fmt --all --check`; `cargo clippy --workspace --all-targets -- -D warnings`;
  `cargo test --workspace` ×2; pinned debug replays 0x8bee1752811ae873 +
  0x348270b38f166529; release replays 0x8bee1752811ae873 + 0xff159ed76d2806e4 (the
  exact ci.yml invocations); a fresh invoicing 1024-universe batch (record the master
  seed); `cargo build -p guide-examples`; and the full edge matrix
  (`scripts/edge-harness.sh`) — the three oracles green against stock is the
  demo-of-record, transcript (scenario scripts + output) recorded in the ledger.
- [ ] **Step 3: Docs + CLAUDE.md + ledger**, evidence-first (every claim written from
  the sweep's actual output). Commit — `docs: M1 sub-project 6 done — introspection +
  edge harness; M1 COMPLETE (close-out)`.
- [ ] **Step 4: Push** (CLAUDE.md GitHub-CI discipline: local-first verification done
  above; one metered CI run).

---

## Verification summary (what "done" means)

- Phase A: describe golden + determinism + invoicing golden pinned; GUIDE §13
  compile-gated; `charpente describe --dir examples/invoicing` exits 0.
- Phase B: 9 `.stderr` pins + 2 pass regressions; `FaultReason` with the reword
  discriminator; every listed string byte-exact; empty-secret refusal pinned.
- Phase C: three wire oracles green against stock; the ack-before-commit meta-test red
  against the wrong build; hunt findings triaged per §4.4 (sim seed before any core
  fix); read-side-poison characterization HELD for ruling.
- Close-out: the spec §8 sweep, all green, then M1 CLOSE + push.

## Deviations from spec text, resolved in this plan (for the final review's eyes)

1. `charpente_version` omits "git describe" (Global Constraints — rationale there).
2. `launch` treats an unknown verb as a usage error (exit 2), not as serve (T3 —
   a typo must not boot a server).
3. spec §3.1's table.rs:147 target is runtime text, not a compile error — absent from
   the trybuild set by fact, not choice (T6).
4. `FaultReason` carries four families + Unrelated (the shipped classifier's four),
   not the spec sketch's three (T7).
5. The three Noah-ruled pre-flight amendments (events dropped in v0; empty-secret named
   config error; the committed node bin) are spec text now, commit ea7fe4c.
6. `CronInfo` gains a `builtin` flag the spec's sketch lacked (T2) — the platform
   session-sweep cron is registered unconditionally, and an agent reading the contract
   should be able to tell it from the app's own schedules; visible in the pinned golden.
7. The CLI verb gains `--bin NAME` (T4), absent from spec §2 — forced by the amendment
   that gives invoicing a second committed binary; default resolution (sole bin, else
   package-name match) keeps the spec's "no target discovery" spirit.


