# M0 Step 6 — Queue + Cron + Deterministic Edge Executor: Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Jobs run: post-commit dispatch with typed retry, cron ticks as transactions, and the deterministic kernel event loop multiplexing HTTP + jobs + cron per DESIGN §1's same-shape rule.

**Architecture:** Handlers are pure state transitions (handler effects + Done-mark = one atomic log record → exactly-once state effects). The `JobRow` platform table lives in core (the enqueue effect materializes it at apply, ids from a meta counter). A new deterministic `charpente::kernel` module owns the event loop; `RequestSource` is the only edge seam; when several sources are ready, one entropy draw picks. Cron is a recurring enqueuer. Spec: `docs/superpowers/specs/2026-07-02-queue-cron-design.md` — read it first; it rules every judgment call.

**Tech Stack:** Existing workspace only — no new dependencies.

## Global Constraints

- CLAUDE.md hard rules bind every task: no ambient nondeterminism (deny-list; BTreeMap/BTreeSet only), error messages name contract/site/fix and are tested, assertions in the TigerBeetle style, sim-first.
- Before every commit: `cargo fmt --all`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test --workspace` — all clean. No Co-Authored-By trailers.
- `run_universes!`/`run_universes_from!` and all pinned seeds are frozen; new property tests use `run_universes_swarm!`.
- Pre-release: the v0 record format changes in place (enqueue effect gains a key field). No compatibility shims.
- Time never enters a transaction ambiently: the kernel computes `now`/deadlines outside and writes them as row data.
- `#[route]` cannot be used inside the charpente crate itself; `#[table]` and the new `#[job]` CAN (they emit `::charpente_core` paths).

## File Map

| File | Responsibility |
|---|---|
| `crates/charpente-core/src/job.rs` (new) | `JobRow` (manual `Row` impl), `JobStatus`, `RetryPolicy` |
| `crates/charpente-core/src/db.rs` (modify) | keyed enqueue effect, `Tx::enqueue_keyed`, apply-time materialization, `Db::drain_enqueued`, auto-register `JobRow` |
| `crates/charpente-core/src/table.rs` (modify) | `Writer::materialize_job` (meta-counter ids); delete raw `JOBS` table, `enqueue_raw`, `Snapshot::jobs` |
| `crates/charpente-macros/src/job.rs` (new) + `lib.rs` | `#[job]` attribute macro |
| `crates/charpente/src/kernel/mod.rs` (new) | `Kernel`, `RequestSource`, `Reply`, `Wake`, `SimSource`, the loop |
| `crates/charpente/src/kernel/dispatch.rs` (new) | `Dispatcher`, `JobDef`, `JobError`, `jobs![]` |
| `crates/charpente/src/kernel/cron.rs` (new) | `CronRow` table, `CronScheduler`, builder registration |
| `crates/charpente/src/http/app.rs` (modify) | `mount_jobs`, `schedule`, kernel accessors |
| `crates/charpente/src/serve.rs` (modify) | `ProdSource`; kernel thread runs `Kernel::run` |
| `crates/charpente/tests/kernel.rs` (new) | dispatcher/cron/loop integration + swarm properties + meta-test |
| `crates/charpente/tests/{transactions,http}.rs` (modify) | `snap.jobs()` → `scan::<JobRow>()` migration; #24 fold |

---

### Task 1: Keyed enqueue effect in core

**Files:**
- Modify: `crates/charpente-core/src/db.rs`

**Interfaces:**
- Consumes: existing `Codec` (`Option<String>` already has a codec), `TxEffects`, record encode/decode.
- Produces: `Effect::Enqueue { job: String, payload: Vec<u8>, key: Option<String> }`; `Tx::enqueue_keyed<J: Job>(&mut self, key: impl Into<String>, job: J)`; `TxEffects.enqueues: Vec<(&'static str, Vec<u8>, Option<String>)>`. Record wire format for tag 2 becomes `[job][payload][key: Option<String>]`.

- [ ] **Step 1: Write the failing tests** — extend the existing `#[cfg(test)] mod tests` in `db.rs`. Update `effects_fixture()` and `record_roundtrip_is_canonical` for the new tuple/variant shape, and add:

```rust
#[test]
fn keyed_enqueue_roundtrips_and_empty_key_is_a_contract_violation() {
    let mut effects = TxEffects::default();
    effects
        .enqueues
        .push(("send_receipt", vec![1], Some("receipt:10".to_string())));
    effects.enqueues.push(("ping", vec![2], None));
    let decoded = decode_record(&encode_record(&effects)).unwrap();
    assert_eq!(
        decoded,
        vec![
            Effect::Enqueue {
                job: "send_receipt".into(),
                payload: vec![1],
                key: Some("receipt:10".into())
            },
            Effect::Enqueue {
                job: "ping".into(),
                payload: vec![2],
                key: None
            },
        ]
    );
}
```

Also update `effects_fixture()`'s enqueue line to `effects.enqueues.push(("send_receipt", vec![10, 0], None));` and the expected `Effect::Enqueue` in `record_roundtrip_is_canonical` to carry `key: None`. And the contract test the first test's name promises — `Tx` cannot be constructed in unit tests (private snapshot), so this lands in `crates/charpente/tests/transactions.rs`:

```rust
#[test]
#[should_panic(expected = "cannot be empty")]
fn empty_idempotency_key_is_a_contract_violation() {
    let sim = Sim::with_faults(0x601, FaultConfig::none());
    let mut db = Db::open(Box::new(sim.disk()), DbConfig::default(), Schema::new()).unwrap();
    let _ = db.transact(|tx| {
        tx.enqueue_keyed("", Ping { i: 0 });
        Ok::<(), std::io::Error>(())
    });
}
```

(Adapt the `Ping` job and imports to what `transactions.rs` already defines; rename the first db.rs test to `keyed_enqueue_roundtrips` since the panic half now lives here.)

- [ ] **Step 2: Run to verify failure** — `cargo test -p charpente-core record_roundtrip` → FAIL (tuple arity / missing field).

- [ ] **Step 3: Implement.** In `db.rs`:
  - `TxEffects.enqueues: Vec<(&'static str, Vec<u8>, Option<String>)>`.
  - `Effect::Enqueue { job: String, payload: Vec<u8>, key: Option<String> }`.
  - `encode_record`: for enqueues, after the payload bytes, `key.encode(&mut out)` — `Option<String>` uses the existing codecs (`0`/`1` tag then length-prefixed string).
  - `decode_record` tag 2: `key: Option::<String>::decode(input)?`.
  - `Tx::enqueue`: push `(J::NAME, payload, None)`.
  - New method:

```rust
/// Enqueues with an idempotency key: the job's stable identity to the world
/// OUTSIDE the log (consumed by M1's external-IO surface). Inside the
/// deterministic boundary the key means nothing — exactly-once state effects
/// are structural, so the platform neither indexes nor dedupes on it.
pub fn enqueue_keyed<J: Job>(&mut self, key: impl Into<String>, job: J) {
    let key = key.into();
    assert!(
        !key.is_empty(),
        "contract: an idempotency key names the job's external identity — it cannot be empty"
    );
    let mut payload = Vec::new();
    job.encode(&mut payload);
    self.effects.enqueues.push((J::NAME, payload, Some(key)));
}
```

  - `TableMachine::apply`'s `Effect::Enqueue` arm: destructure the new field, pass `key.as_deref()` through to the (still-current) `enqueue_raw` — Task 2 replaces that call; for THIS task keep the raw table write compiling by ignoring the key there: `w.enqueue_raw(seq, job_idx, job, payload)?` unchanged, `let _ = key;` with no comment (removed next task).

- [ ] **Step 4: Run** — `cargo test -p charpente-core` → PASS; then workspace fmt/clippy/test clean (integration tests in `charpente/tests` don't touch the tuple internals, only `tx.enqueue`, which kept its signature).

- [ ] **Step 5: Commit** — `core: enqueue effects carry an optional idempotency key (external identity; no platform semantics inside the boundary)`

---

### Task 2: `JobRow` platform table + apply-time materialization

**Files:**
- Create: `crates/charpente-core/src/job.rs`
- Modify: `crates/charpente-core/src/lib.rs` (add `pub mod job;`), `crates/charpente-core/src/table.rs`, `crates/charpente-core/src/db.rs`
- Modify (migrate): `crates/charpente/tests/transactions.rs`, `crates/charpente/tests/http.rs`

**Interfaces:**
- Consumes: Task 1's keyed enqueue effect; existing `Row`/`Codec`/`Writer`/meta table.
- Produces: `charpente_core::job::{JobRow, JobStatus, RetryPolicy}`; `Writer::materialize_job(&mut self, name: &str, payload: &[u8], key: Option<&str>) -> io::Result<u64>`; `Db::drain_enqueued(&mut self) -> Vec<Id<JobRow>>`; `Db::open` registers `JobRow` itself (registering it in an app `Schema` panics as a duplicate). Raw `JOBS` table, `Writer::enqueue_raw`, `Snapshot::jobs()` are GONE.

- [ ] **Step 1: Write `job.rs`** (implementation-first is wrong — but the Row impl is pure data; the failing tests in Step 2 drive the db/table changes). Complete file:

```rust
//! The job substrate (DESIGN §6, spec 2026-07-02-queue-cron-design.md): a
//! platform table materialized at apply from enqueue effects. Lives in core
//! because the apply path must know its codec; dispatch lives in
//! `charpente::kernel`. Ids come from the `next_job_id` meta counter, bumped
//! at apply — deterministic across replay.

use std::io;
use std::time::Duration;

use crate::table::{Codec, Id, IndexDef, Row};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum JobStatus {
    Pending,
    Done,
    Dead,
}

impl Codec for JobStatus {
    fn encode(&self, out: &mut Vec<u8>) {
        out.push(match self {
            JobStatus::Pending => 0,
            JobStatus::Done => 1,
            JobStatus::Dead => 2,
        });
    }
    fn decode(input: &mut &[u8]) -> io::Result<JobStatus> {
        match u8::decode(input)? {
            0 => Ok(JobStatus::Pending),
            1 => Ok(JobStatus::Done),
            2 => Ok(JobStatus::Dead),
            other => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("row decode: job status byte was {other}, must be 0..=2"),
            )),
        }
    }
}

/// Typed retry policy carried on the `Job` trait (`#[job(retry(...))]`).
/// Backoff doubles per attempt: `backoff * 2^(attempts - 1)`.
#[derive(Clone, Copy, Debug)]
pub struct RetryPolicy {
    pub max: u32,
    pub backoff: Duration,
}

impl RetryPolicy {
    pub const DEFAULT: RetryPolicy = RetryPolicy {
        max: 5,
        backoff: Duration::from_secs(30),
    };

    /// Nanos until the next attempt after `attempts` failures (attempts >= 1).
    pub fn backoff_nanos(&self, attempts: u32) -> u64 {
        assert!(attempts >= 1, "backoff is asked after a failure, never before");
        let base = u64::try_from(self.backoff.as_nanos()).expect("backoff exceeds u64 nanos");
        base.saturating_mul(1u64.checked_shl(attempts - 1).unwrap_or(u64::MAX))
    }
}

/// A job as the platform stores it. `key` is the job's stable identity to
/// the world outside the log (spec: no platform semantics in v0).
pub struct JobRow {
    pub id: Id<JobRow>,
    pub name: String,
    pub payload: Vec<u8>,
    pub key: Option<String>,
    pub status: JobStatus,
    pub attempts: u32,
    pub due_at: u64,
    /// The last failure's message (handler error or absorbed panic) — the
    /// dead-letter record's substance (hard rule 5).
    pub last_error: Option<String>,
}

// Manual impl: core cannot use the #[table] macro (macros depend on core).
impl Row for JobRow {
    const TABLE: &'static str = "charpente.jobs";
    const INDEXES: &'static [IndexDef<JobRow>] = &[];

    fn id(&self) -> Id<JobRow> {
        self.id
    }

    fn encode_row(&self, out: &mut Vec<u8>) {
        self.id.encode(out);
        self.name.encode(out);
        self.payload.encode(out);
        self.key.encode(out);
        self.status.encode(out);
        self.attempts.encode(out);
        self.due_at.encode(out);
        self.last_error.encode(out);
    }

    fn decode_row(bytes: &[u8]) -> io::Result<JobRow> {
        let input = &mut &bytes[..];
        let row = JobRow {
            id: Id::decode(input)?,
            name: String::decode(input)?,
            payload: Vec::<u8>::decode(input)?,
            key: Option::<String>::decode(input)?,
            status: JobStatus::decode(input)?,
            attempts: u32::decode(input)?,
            due_at: u64::decode(input)?,
            last_error: Option::<String>::decode(input)?,
        };
        if !input.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "row decode: {} trailing bytes after a JobRow — schema drift or corruption",
                    input.len()
                ),
            ));
        }
        Ok(row)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn job_row_roundtrips() {
        let row = JobRow {
            id: Id::from_raw(7),
            name: "send_receipt".into(),
            payload: vec![1, 2],
            key: Some("receipt:10".into()),
            status: JobStatus::Pending,
            attempts: 3,
            due_at: 99,
            last_error: Some("boom".into()),
        };
        let mut bytes = Vec::new();
        row.encode_row(&mut bytes);
        let back = JobRow::decode_row(&bytes).unwrap();
        assert_eq!(back.id, row.id);
        assert_eq!(back.name, row.name);
        assert_eq!(back.key, row.key);
        assert_eq!(back.status, JobStatus::Pending);
        assert_eq!((back.attempts, back.due_at), (3, 99));
        assert_eq!(back.last_error.as_deref(), Some("boom"));
    }

    #[test]
    fn backoff_doubles_and_saturates() {
        let p = RetryPolicy {
            max: 5,
            backoff: Duration::from_secs(30),
        };
        let s = 30_000_000_000u64;
        assert_eq!(p.backoff_nanos(1), s);
        assert_eq!(p.backoff_nanos(2), 2 * s);
        assert_eq!(p.backoff_nanos(3), 4 * s);
        assert_eq!(p.backoff_nanos(200), u64::MAX); // shift overflow saturates
    }
}
```

Add `pub mod job;` to `crates/charpente-core/src/lib.rs`.

- [ ] **Step 2: Write the failing materialization test** — in `db.rs` tests:

```rust
use crate::job::{JobRow, JobStatus};
use crate::table::View;
use charpente_sim::{FaultConfig, Sim};
// NOTE: charpente-core has charpente-sim as a dev-dependency already if the
// wal/engine tests use it; if not, add `charpente-sim = { path = "../charpente-sim" }`
// to [dev-dependencies] in charpente-core/Cargo.toml.

struct Ping;
impl Codec for Ping {
    fn encode(&self, _out: &mut Vec<u8>) {}
    fn decode(_input: &mut &[u8]) -> io::Result<Ping> {
        Ok(Ping)
    }
}
impl Job for Ping {
    const NAME: &'static str = "ping";
}

#[test]
fn enqueues_materialize_job_rows_with_counter_ids() {
    let sim = Sim::with_faults(0x600, FaultConfig::none());
    let mut db = Db::open(Box::new(sim.disk()), DbConfig::default(), Schema::new()).unwrap();
    db.transact(|tx: &mut Tx| {
        tx.enqueue(Ping);
        tx.enqueue_keyed("k:1", Ping);
        Ok::<(), io::Error>(())
    })
    .unwrap();
    let drained = db.drain_enqueued();
    assert_eq!(drained.iter().map(|id| id.raw()).collect::<Vec<_>>(), vec![0, 1]);
    assert!(db.drain_enqueued().is_empty(), "drain drains");

    let jobs = db.snapshot().unwrap().scan::<JobRow>().unwrap();
    assert_eq!(jobs.len(), 2);
    assert_eq!(jobs[0].name, "ping");
    assert_eq!(jobs[0].key, None);
    assert_eq!(jobs[1].key.as_deref(), Some("k:1"));
    assert!(jobs.iter().all(|j| j.status == JobStatus::Pending
        && j.attempts == 0
        && j.due_at == 0));

    // Ids keep counting across transactions.
    db.transact(|tx: &mut Tx| {
        tx.enqueue(Ping);
        Ok::<(), io::Error>(())
    })
    .unwrap();
    assert_eq!(db.drain_enqueued()[0].raw(), 2);
}
```

- [ ] **Step 3: Run to verify failure** — `cargo test -p charpente-core enqueues_materialize` → FAIL (`drain_enqueued` undefined).

- [ ] **Step 4: Implement.**
  - `table.rs`: delete the `JOBS` const, `Writer::enqueue_raw`, and `Snapshot::jobs()`. Add:

```rust
/// Materializes one enqueue effect as a `JobRow`, id from the monotonic
/// `next_job_id` meta counter — bumped in this same store transaction, so
/// ids are deterministic across replay. Platform apply-path surface.
pub fn materialize_job(
    &mut self,
    name: &str,
    payload: &[u8],
    key: Option<&str>,
) -> io::Result<u64> {
    let mut meta = self.txn.open_table(META).map_err(redb_err)?;
    let id = meta
        .get("next_job_id")
        .map_err(redb_err)?
        .map_or(0, |g| g.value());
    meta.insert("next_job_id", id + 1).map_err(redb_err)?;
    drop(meta);
    self.put(&crate::job::JobRow {
        id: crate::table::Id::from_raw(id),
        name: name.to_string(),
        payload: payload.to_vec(),
        key: key.map(str::to_string),
        status: crate::job::JobStatus::Pending,
        attempts: 0,
        due_at: 0,
        last_error: None,
    })?;
    Ok(id)
}
```

  - `db.rs`: `TableMachine` gains `enqueued: Vec<u64>`; the `Enqueue` arm becomes `self.enqueued.push(w.materialize_job(job, payload, key.as_deref())?)` — note the closure passed to `store.apply` can't borrow `self.enqueued` mutably while `self.schema` is borrowed; restructure the apply loop to collect new ids into a local `Vec` inside the closure and extend `self.enqueued` after (the closure returns them). The `job_idx` counter disappears with `enqueue_raw`.
  - `Db::open`: build the schema as `schema.table::<JobRow>()` before constructing `TableMachine` (apps registering `JobRow` themselves hit the existing duplicate-registration assert, whose message already names the fix). Drain the machine's `enqueued` buffer after `Engine::recover` (recovery replay must not leak into the kernel's incremental path — the dispatcher rebuilds from a scan): `machine.enqueued.clear()` via a small `Db` internal, or clear inside `Db::open` after recover.
  - `Db::drain_enqueued`:

```rust
/// Ids of jobs materialized by transactions since the last drain — the
/// kernel's incremental feed for its due-heap. Recovery replay does not
/// appear here; the dispatcher rebuilds from a Pending scan at open.
pub fn drain_enqueued(&mut self) -> Vec<Id<JobRow>> {
    self.engine
        .machine_mut()
        .enqueued
        .drain(..)
        .map(Id::from_raw)
        .collect()
}
```

  - `engine.rs`: add `pub fn machine_mut(&mut self) -> &mut S` next to `machine()`.

- [ ] **Step 5: Migrate dependent tests.** `crates/charpente/tests/transactions.rs` and `crates/charpente/tests/http.rs` use `snap.jobs()` (returns `(seq, idx, name, payload)`). Replace with `scan::<JobRow>()`: import `charpente_core::job::JobRow`; the dual-write assertions become — jobs count comparisons unchanged; per-job checks change from `(seq, idx, name, i)` tuples to `JobRow` fields. In `http.rs`'s `http_state_matches`, the per-job loop becomes:

```rust
let jobs = snap.scan::<JobRow>().map_err(|e| format!("jobs: {e}"))?;
if jobs.len() as u64 != n {
    return Err(format!(
        "dual-write torn over HTTP: {n} acked but {} jobs",
        jobs.len()
    ));
}
for (i, job) in jobs.iter().enumerate() {
    let ping = Ping::decode(&mut job.payload.as_slice()).map_err(|e| format!("job decode: {e}"))?;
    if (job.id.raw(), job.name.as_str(), ping.i) != (i as u64, "ping", i as u64) {
        return Err(format!(
            "job {i} diverges: id {} {} i={}",
            job.id.raw(),
            job.name,
            ping.i
        ));
    }
}
```

Apply the same shape in `transactions.rs` wherever `snap.jobs()` appears (same field mapping; consult the compiler).

- [ ] **Step 6: Run everything** — `cargo test --workspace` → PASS, including both migrated swarm property suites over their fresh-seed batches. fmt/clippy clean.

- [ ] **Step 7: Commit** — `core: JobRow platform table — enqueues materialize rows with meta-counter ids; raw jobs substrate removed`

---

### Task 3: `#[job]` macro, `JobError`, `JobDef`, `jobs![]`, `App::mount_jobs`

**Files:**
- Create: `crates/charpente-macros/src/job.rs`, `crates/charpente/src/kernel/mod.rs` (skeleton), `crates/charpente/src/kernel/dispatch.rs` (types only), `crates/charpente/tests/kernel.rs`
- Modify: `crates/charpente-macros/src/lib.rs`, `crates/charpente-core/src/db.rs` (Job trait gains RETRY), `crates/charpente/src/lib.rs` (add `pub mod kernel;` + re-export `job` macro), `crates/charpente/src/http/app.rs`

**Interfaces:**
- Consumes: `charpente_core::job::RetryPolicy`, `charpente_core::db::{Job, Tx}`, Task 2's `JobRow`.
- Produces:
  - core: `trait Job: Codec { const NAME: &'static str; const RETRY: RetryPolicy = RetryPolicy::DEFAULT; }`
  - `charpente::kernel::{JobError, JobDef, JobEntry}`:
    `pub enum JobError { App(String), Io(io::Error) }` (+ `JobError::app(msg)` ctor,
    `From<io::Error>` → `Io`, `Display`, `Debug`). The App/Io split is load-bearing
    (review finding 2026-07-02): `Io` means the platform failed mid-handler — NOT an
    attempt, never fed to retry; only `App` consumes retry budget.
    `pub struct JobDef { pub name: &'static str, pub retry: RetryPolicy, pub glue: fn(&mut Tx, &[u8]) -> Result<(), JobError> }`
    `pub trait JobEntry { fn def() -> JobDef; }`
  - `jobs![A, B]` macro (in `charpente`, mirrors `routes![]`): `Vec<JobDef>`.
  - `#[job]` / `#[job(retry(max = N, backoff = "30s"))]` attribute macro: emits the struct, a field-wise `Codec` impl (`::charpente_core` paths — same generation as `#[table]`'s row codec minus the id), `impl Job` (NAME = type name, RETRY from the attribute or default), and `impl ::charpente::kernel::JobEntry` whose glue decodes the payload and calls the struct's inherent `fn run(self, tx: &mut Tx) -> Result<(), JobError>`. Like `#[route]`, the JobEntry impl works in dependent crates and integration tests, not charpente's own src.
  - `App::mount_jobs(self, Vec<JobDef>) -> App` (mount-time duplicate-name panic naming both) and `App::schedule` arrives in Task 5. `App` gains `pub(crate) fn db_mut(&mut self) -> &mut Db`, `pub(crate) fn entropy(&self) -> &dyn Entropy`, `pub(crate) fn take_job_defs(&mut self) -> Vec<JobDef>`.

- [ ] **Step 1: Write the failing integration test** — new file `crates/charpente/tests/kernel.rs`:

```rust
//! Kernel-layer tests (PLAN M0 step 6): #[job] surface, dispatcher, cron,
//! the event loop, and the step-6 swarm properties.

use charpente::http::{App, AppConfig};
use charpente::kernel::{JobDef, JobEntry, JobError};
use charpente::{Schema, Tx, job, jobs, table};
use charpente_core::db::Job;
use charpente_core::table::{Id, View};
use charpente_sim::{FaultConfig, Sim};

#[table]
struct Counter {
    id: Id<Counter>,
    hits: u64,
}

#[job(retry(max = 3, backoff = "10s"))]
struct CountUp {
    slot: u64,
}

impl CountUp {
    fn run(self, tx: &mut Tx) -> Result<(), JobError> {
        let hits = tx.get(Id::<Counter>::from_raw(self.slot))?.map_or(0, |c| c.hits);
        tx.put(&Counter { id: Id::from_raw(self.slot), hits: hits + 1 });
        Ok(())
    }
}

#[test]
fn job_macro_derives_codec_name_retry_and_glue() {
    assert_eq!(CountUp::NAME, "CountUp");
    assert_eq!(CountUp::RETRY.max, 3);
    assert_eq!(CountUp::RETRY.backoff, std::time::Duration::from_secs(10));

    let defs = jobs![CountUp];
    assert_eq!(defs.len(), 1);
    assert_eq!(defs[0].name, "CountUp");

    // The glue decodes a payload and runs the handler inside a transaction.
    let sim = Sim::with_faults(0x610, FaultConfig::none());
    let mut app = App::open(
        Box::new(sim.disk()),
        Schema::new().table::<Counter>(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )
    .unwrap();
    let def: JobDef = <CountUp as JobEntry>::def();
    let mut payload = Vec::new();
    charpente_core::table::Codec::encode(&CountUp { slot: 4 }, &mut payload);
    app.db_for_tests()
        .transact(|tx| (def.glue)(tx, &payload))
        .unwrap();
    let counters = app.db_for_tests().snapshot().unwrap().scan::<Counter>().unwrap();
    assert_eq!((counters[0].id.raw(), counters[0].hits), (4, 1));
}
```

Note: `db_for_tests` — add `pub fn db_for_tests(&mut self) -> &mut Db` on `App` guarded by `#[doc(hidden)]` (integration tests cannot see `pub(crate)`; the kernel uses the `pub(crate)` accessor). Keep both.

- [ ] **Step 2: Run to verify failure** — `cargo test -p charpente --test kernel` → FAIL (no `kernel` module / no `job` macro).

- [ ] **Step 3: Implement.**
  - core `db.rs`: `pub trait Job: Codec { const NAME: &'static str; const RETRY: crate::job::RetryPolicy = crate::job::RetryPolicy::DEFAULT; }`.
  - `crates/charpente/src/kernel/dispatch.rs` (types only this task):

```rust
//! Job dispatch types. The Dispatcher itself lands with the due-heap task.

use std::fmt;
use std::io;

use charpente_core::db::Tx;
use charpente_core::job::RetryPolicy;

/// Why a job run did not complete. The App/Io split mirrors `TxError` and is
/// load-bearing: `App` is the handler's own rejection (or an absorbed panic,
/// or a corrupt payload) — a real attempt that feeds retry budget and lands
/// in the row's `last_error`. `Io` is a platform fault surfaced through the
/// handler's reads — NOT an attempt: the job stays Pending, nothing is
/// recorded, the error bubbles (restart-and-recover). Folding Io into the
/// retry path would let injected faults Dead-letter healthy jobs.
pub enum JobError {
    App(String),
    Io(io::Error),
}

impl JobError {
    pub fn app(message: impl Into<String>) -> JobError {
        JobError::App(message.into())
    }
}

impl From<io::Error> for JobError {
    fn from(e: io::Error) -> JobError {
        JobError::Io(e)
    }
}

impl fmt::Display for JobError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JobError::App(message) => f.write_str(message),
            JobError::Io(e) => {
                write!(f, "platform IO failure during the handler (not an attempt): {e}")
            }
        }
    }
}

impl fmt::Debug for JobError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "JobError({self})")
    }
}

/// One registered job type: the descriptor `jobs![…]` collects (explicit
/// registration — distributed registration stays rejected, like routes).
pub struct JobDef {
    pub name: &'static str,
    pub retry: RetryPolicy,
    pub glue: fn(&mut Tx, &[u8]) -> Result<(), JobError>,
}

/// Implemented by the marker impl `#[job]` emits, so `jobs![a, b]` collects
/// descriptors explicitly.
pub trait JobEntry {
    fn def() -> JobDef;
}

#[macro_export]
macro_rules! jobs {
    ($($job:ident),* $(,)?) => {
        ::std::vec![$(<$job as $crate::kernel::JobEntry>::def()),*]
    };
}
```

  - `crates/charpente/src/kernel/mod.rs` (skeleton this task):

```rust
//! The deterministic kernel (DESIGN §1, spec 2026-07-02-queue-cron-design.md):
//! the event loop multiplexing HTTP requests, due jobs, and cron ticks — all
//! inside the simulation boundary. The loop lands with the executor task.

mod dispatch;

pub use dispatch::{JobDef, JobEntry, JobError};
```

  - `charpente/src/lib.rs`: `pub mod kernel;` and re-export the proc macro: `pub use charpente_macros::job;`.
  - `crates/charpente-macros/src/job.rs`: parse `retry(max = <int>, backoff = "<n><unit>")` (both optional, defaults `max = 5`, `backoff = "30s"`; units `ms|s|m|h`; anything else is a compile error naming the accepted forms). Reuse `table.rs`'s field-collection pattern for the Codec generation. Expansion target (for input `#[job(retry(max = 3, backoff = "10s"))] struct CountUp { slot: u64 }`):

```rust
struct CountUp { slot: u64 }

impl ::charpente_core::table::Codec for CountUp {
    fn encode(&self, out: &mut ::std::vec::Vec<u8>) {
        ::charpente_core::table::Codec::encode(&self.slot, out);
    }
    fn decode(input: &mut &[u8]) -> ::std::io::Result<Self> {
        let slot = <u64 as ::charpente_core::table::Codec>::decode(input)?;
        ::std::result::Result::Ok(Self { slot })
    }
}

impl ::charpente_core::db::Job for CountUp {
    const NAME: &'static str = "CountUp";
    const RETRY: ::charpente_core::job::RetryPolicy = ::charpente_core::job::RetryPolicy {
        max: 3u32,
        backoff: ::std::time::Duration::from_millis(10000u64),
    };
}

impl ::charpente::kernel::JobEntry for CountUp {
    fn def() -> ::charpente::kernel::JobDef {
        fn __charpente_job_glue(
            tx: &mut ::charpente::Tx,
            payload: &[u8],
        ) -> ::std::result::Result<(), ::charpente::kernel::JobError> {
            let input = &mut &payload[..];
            let job = <CountUp as ::charpente_core::table::Codec>::decode(input)
                .map_err(|e| ::charpente::kernel::JobError::App(::std::format!(
                    "job decode for CountUp: {e} — schema drift or corruption"
                )))?;
            if !input.is_empty() {
                return ::std::result::Result::Err(::charpente::kernel::JobError::App(
                    ::std::format!(
                        "job decode: {} trailing bytes after a CountUp payload — schema drift or corruption",
                        input.len()
                    ),
                ));
            }
            job.run(tx)
        }
        ::charpente::kernel::JobDef {
            name: <CountUp as ::charpente_core::db::Job>::NAME,
            retry: <CountUp as ::charpente_core::db::Job>::RETRY,
            glue: __charpente_job_glue,
        }
    }
}
```

  Wire `#[proc_macro_attribute] pub fn job` in `charpente-macros/src/lib.rs` mirroring `route`/`table`.
  - `app.rs`: `mount_jobs` stores `Vec<JobDef>` on `App` (duplicate-name panic: "job \"{name}\" registered twice — each #[job] type is mounted once"), plus the three accessors from the Interfaces block.

- [ ] **Step 4: Run** — `cargo test -p charpente --test kernel` → PASS; workspace clean.

- [ ] **Step 5: Commit** — `macros+charpente: #[job] — typed payloads, retry policy, explicit jobs![] registration`

---

### Task 4: Dispatcher — due-heap, execute, retry, dead, panic absorption

**Files:**
- Modify: `crates/charpente/src/kernel/dispatch.rs`, `crates/charpente/src/kernel/mod.rs` (re-exports), `crates/charpente/tests/kernel.rs`

**Interfaces:**
- Consumes: Task 2's `JobRow`/`JobStatus`/`drain_enqueued`, Task 3's `JobDef`/`JobError`.
- Produces:

```rust
pub enum JobOutcome {
    Done { id: u64 },
    Retried { id: u64, attempts: u32, due_at: u64 },
    Dead { id: u64, attempts: u32 },
}

pub struct Dispatcher { /* defs: BTreeMap<&'static str, JobDef>, due: BTreeSet<(u64, u64)> */ }

impl Dispatcher {
    pub fn new(defs: Vec<JobDef>) -> Dispatcher;
    /// Rebuilds the due-heap from Pending rows (called at kernel start —
    /// the store is the only truth; the heap is a cache of it).
    pub fn rebuild(&mut self, snap: &Snapshot) -> io::Result<()>;
    /// Feeds ids drained from `Db::drain_enqueued` (fresh enqueues are due
    /// immediately: due_at 0).
    pub fn note_enqueued(&mut self, ids: Vec<Id<JobRow>>);
    pub fn next_deadline(&self) -> Option<u64>;
    pub fn has_due(&self, now: u64) -> bool;
    /// Runs the earliest due job. Ok(outcome) covers handler success and
    /// handler failure (retry/dead — both are committed records); Err is a
    /// PLATFORM failure (TxError::Io): the job stays Pending in the store,
    /// the heap entry is restored, and the caller decides (restart).
    pub fn run_due(&mut self, db: &mut Db, now: u64) -> io::Result<JobOutcome>;
}
```

- [ ] **Step 1: Write the failing tests** — append to `tests/kernel.rs`:

```rust
use charpente::kernel::{Dispatcher, JobOutcome};
use charpente_core::job::{JobRow, JobStatus};

#[job(retry(max = 2, backoff = "10s"))]
struct AlwaysFails {}

impl AlwaysFails {
    fn run(self, _tx: &mut Tx) -> Result<(), JobError> {
        Err(JobError::app("the widget was upside down"))
    }
}

#[job(retry(max = 2, backoff = "10s"))]
struct Panics {}

impl Panics {
    fn run(self, _tx: &mut Tx) -> Result<(), JobError> {
        panic!("handler bug: unreachable state");
    }
}

fn quiet_kernel_app(seed: u64) -> (Sim, App) {
    let sim = Sim::with_faults(seed, FaultConfig::none());
    let app = App::open(
        Box::new(sim.disk()),
        Schema::new().table::<Counter>(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )
    .unwrap()
    .mount_jobs(jobs![CountUp, AlwaysFails, Panics, ReadFaults]);
    (sim, app)
}

#[test]
fn dispatcher_runs_due_job_with_atomic_done_mark() {
    let (_sim, mut app) = quiet_kernel_app(0x620);
    let mut dispatcher = Dispatcher::new(app.take_job_defs_for_tests());
    let db = app.db_for_tests();
    db.transact(|tx| {
        tx.enqueue(CountUp { slot: 1 });
        Ok::<(), std::io::Error>(())
    })
    .unwrap();
    dispatcher.note_enqueued(db.drain_enqueued());
    assert!(dispatcher.has_due(0));

    let outcome = dispatcher.run_due(db, 0).unwrap();
    assert!(matches!(outcome, JobOutcome::Done { id: 0 }));
    let snap = db.snapshot().unwrap();
    assert_eq!(snap.scan::<Counter>().unwrap()[0].hits, 1);
    assert_eq!(snap.scan::<JobRow>().unwrap()[0].status, JobStatus::Done);
    assert!(!dispatcher.has_due(u64::MAX), "done jobs leave the heap");
}

#[test]
fn failing_job_follows_backoff_then_dies_with_the_story() {
    let (_sim, mut app) = quiet_kernel_app(0x621);
    let mut dispatcher = Dispatcher::new(app.take_job_defs_for_tests());
    let db = app.db_for_tests();
    db.transact(|tx| {
        tx.enqueue(AlwaysFails {});
        Ok::<(), std::io::Error>(())
    })
    .unwrap();
    dispatcher.note_enqueued(db.drain_enqueued());

    let now0 = 1_000;
    let JobOutcome::Retried { attempts, due_at, .. } = dispatcher.run_due(db, now0).unwrap()
    else {
        panic!("first failure retries");
    };
    assert_eq!(attempts, 1);
    assert_eq!(due_at, now0 + 10_000_000_000); // backoff 10s * 2^0
    assert!(!dispatcher.has_due(now0), "not due again until backoff passes");
    assert!(dispatcher.has_due(due_at));

    let JobOutcome::Dead { attempts, .. } = dispatcher.run_due(db, due_at).unwrap() else {
        panic!("attempts == max means dead");
    };
    assert_eq!(attempts, 2);
    let row = &db.snapshot().unwrap().scan::<JobRow>().unwrap()[0];
    assert_eq!(row.status, JobStatus::Dead);
    assert!(
        row.last_error.as_deref().unwrap().contains("upside down"),
        "the dead-letter record carries the last error: {:?}",
        row.last_error
    );
    // Handler effects from failed attempts never committed.
    assert!(db.snapshot().unwrap().scan::<Counter>().unwrap().is_empty());
}

#[test]
fn panicking_handler_is_absorbed_into_the_retry_path() {
    let (_sim, mut app) = quiet_kernel_app(0x622);
    let mut dispatcher = Dispatcher::new(app.take_job_defs_for_tests());
    let db = app.db_for_tests();
    db.transact(|tx| {
        tx.enqueue(Panics {});
        Ok::<(), std::io::Error>(())
    })
    .unwrap();
    dispatcher.note_enqueued(db.drain_enqueued());
    let JobOutcome::Retried { .. } = dispatcher.run_due(db, 0).unwrap() else {
        panic!("a panic is a failure, not a crash");
    };
    let row = &db.snapshot().unwrap().scan::<JobRow>().unwrap()[0];
    assert!(
        row.last_error.as_deref().unwrap().contains("unreachable state"),
        "{:?}",
        row.last_error
    );
}

#[job]
struct ReadFaults {}

impl ReadFaults {
    fn run(self, _tx: &mut Tx) -> Result<(), JobError> {
        // Stands in for a platform read fault mid-handler (real injection
        // arrives with the latent-read-error fault-matrix extension).
        Err(JobError::Io(std::io::Error::other(
            "simulated read fault (injected)",
        )))
    }
}

#[test]
fn platform_io_during_a_handler_is_not_an_attempt() {
    let (_sim, mut app) = quiet_kernel_app(0x625);
    let mut dispatcher = Dispatcher::new(app.take_job_defs_for_tests());
    let db = app.db_for_tests();
    db.transact(|tx| {
        tx.enqueue(ReadFaults {});
        Ok::<(), std::io::Error>(())
    })
    .unwrap();
    dispatcher.note_enqueued(db.drain_enqueued());

    let err = dispatcher.run_due(db, 0).unwrap_err();
    assert!(err.to_string().contains("simulated read fault"), "{err}");
    let row = &db.snapshot().unwrap().scan::<JobRow>().unwrap()[0];
    assert_eq!(row.status, JobStatus::Pending, "an IO fault is not an attempt");
    assert_eq!(row.attempts, 0, "retry budget untouched by platform faults");
    assert!(row.last_error.is_none(), "nothing was recorded");
    assert!(dispatcher.has_due(0), "the job goes straight back on the heap");
}

#[test]
fn unregistered_job_name_fails_with_the_fix() {
    let (_sim, mut app) = quiet_kernel_app(0x623);
    let mut dispatcher = Dispatcher::new(vec![]); // nothing registered
    let db = app.db_for_tests();
    db.transact(|tx| {
        tx.enqueue(CountUp { slot: 0 });
        Ok::<(), std::io::Error>(())
    })
    .unwrap();
    dispatcher.note_enqueued(db.drain_enqueued());
    dispatcher.run_due(db, 0).unwrap();
    let row = &db.snapshot().unwrap().scan::<JobRow>().unwrap()[0];
    let msg = row.last_error.as_deref().unwrap();
    assert!(msg.contains("CountUp") && msg.contains("mount_jobs"), "{msg}");
}

#[test]
fn rebuild_restores_pending_jobs_after_reopen() {
    let sim = Sim::with_faults(0x624, FaultConfig::none());
    let open = |sim: &Sim| {
        App::open(
            Box::new(sim.disk()),
            Schema::new().table::<Counter>(),
            Box::new(sim.entropy()),
            AppConfig::default(),
        )
        .unwrap()
        .mount_jobs(jobs![CountUp, AlwaysFails, Panics])
    };
    let mut app = open(&sim);
    app.db_for_tests()
        .transact(|tx| {
            tx.enqueue(CountUp { slot: 9 });
            Ok::<(), std::io::Error>(())
        })
        .unwrap();
    sim.restart();
    drop(app);
    let mut app = open(&sim);
    let mut dispatcher = Dispatcher::new(app.take_job_defs_for_tests());
    dispatcher
        .rebuild(&app.db_for_tests().snapshot().unwrap())
        .unwrap();
    assert!(dispatcher.has_due(0), "pending jobs survive recovery into the heap");
    let outcome = dispatcher.run_due(app.db_for_tests(), 0).unwrap();
    assert!(matches!(outcome, JobOutcome::Done { .. }));
}
```

Add `pub fn take_job_defs_for_tests(&mut self) -> Vec<JobDef>` (`#[doc(hidden)]`) alongside the `pub(crate)` accessor.

- [ ] **Step 2: Run to verify failure** — `cargo test -p charpente --test kernel dispatcher` → FAIL (`Dispatcher` undefined).

- [ ] **Step 3: Implement `Dispatcher`** in `dispatch.rs`:

```rust
use std::collections::{BTreeMap, BTreeSet};
use std::panic::{AssertUnwindSafe, catch_unwind};

use charpente_core::db::{Db, TxError};
use charpente_core::job::{JobRow, JobStatus, RetryPolicy};
use charpente_core::table::{Id, Snapshot, View};

pub enum JobOutcome {
    Done { id: u64 },
    Retried { id: u64, attempts: u32, due_at: u64 },
    Dead { id: u64, attempts: u32 },
}

pub struct Dispatcher {
    defs: BTreeMap<&'static str, JobDef>,
    /// (due_at, id) — BTreeSet keeps the earliest-due first; ids break ties.
    due: BTreeSet<(u64, u64)>,
}

impl Dispatcher {
    pub fn new(defs: Vec<JobDef>) -> Dispatcher {
        let mut map = BTreeMap::new();
        for def in defs {
            let clash = map.insert(def.name, def);
            assert!(
                clash.is_none(),
                "job registered twice — each #[job] type is mounted once in jobs![…]"
            );
        }
        Dispatcher { defs: map, due: BTreeSet::new() }
    }

    pub fn rebuild(&mut self, snap: &Snapshot) -> std::io::Result<()> {
        self.due.clear();
        for job in snap.scan::<JobRow>()? {
            if job.status == JobStatus::Pending {
                self.due.insert((job.due_at, job.id.raw()));
            }
        }
        Ok(())
    }

    pub fn note_enqueued(&mut self, ids: Vec<Id<JobRow>>) {
        for id in ids {
            self.due.insert((0, id.raw())); // fresh enqueues are due immediately
        }
    }

    pub fn next_deadline(&self) -> Option<u64> {
        self.due.first().map(|&(due_at, _)| due_at)
    }

    pub fn has_due(&self, now: u64) -> bool {
        self.next_deadline().is_some_and(|d| d <= now)
    }

    pub fn run_due(&mut self, db: &mut Db, now: u64) -> std::io::Result<JobOutcome> {
        let &(due_at, id) = self.due.first().expect("run_due called with an empty heap");
        assert!(
            due_at <= now,
            "dispatcher bug: run_due at {now} but the earliest job is due at {due_at}"
        );
        self.due.remove(&(due_at, id));

        let attempt = db.transact(|tx| {
            let mut row = tx
                .get(Id::<JobRow>::from_raw(id))?
                .unwrap_or_else(|| panic!("dispatcher invariant: due job {id} has no row"));
            assert_eq!(
                row.status,
                JobStatus::Pending,
                "dispatcher invariant: job {id} in the heap is not Pending"
            );
            let Some(def) = self.defs.get(row.name.as_str()) else {
                return Err(JobError::App(format!(
                    "job \"{}\" is not registered — add it to the jobs![…] list passed to \
                     App::mount_jobs (or this log outlived a renamed job type)",
                    row.name
                )));
            };
            match catch_unwind(AssertUnwindSafe(|| (def.glue)(tx, &row.payload))) {
                Ok(result) => result?,
                Err(payload) => {
                    // A panic is the handler's own deterministic bug: an
                    // attempt (poison-pill absorption), never a platform fault.
                    return Err(JobError::App(format!(
                        "handler panicked: {}",
                        panic_text(&*payload)
                    )));
                }
            }
            row.status = JobStatus::Done;
            row.last_error = None;
            tx.put(&row);
            Ok(())
        });

        match attempt {
            Ok(()) => Ok(JobOutcome::Done { id }),
            // Platform faults — the transaction machinery's own, or one the
            // handler's reads surfaced — are NOT attempts: the row is
            // untouched and Pending, the heap entry comes back, no ledger
            // write, and the error bubbles (restart-and-recover).
            Err(TxError::Io(e)) | Err(TxError::App(JobError::Io(e))) => {
                self.due.insert((due_at, id)); // still Pending in the store
                Err(e)
            }
            Err(TxError::App(JobError::App(err))) => {
                let retry = self
                    .defs
                    .get(
                        db.snapshot()?
                            .get(Id::<JobRow>::from_raw(id))?
                            .expect("row existed a moment ago")
                            .name
                            .as_str(),
                    )
                    .map_or(RetryPolicy::DEFAULT, |d| d.retry);
                let outcome = db
                    .transact(|tx| {
                        let mut row = tx
                            .get(Id::<JobRow>::from_raw(id))?
                            .expect("row existed a moment ago");
                        row.attempts += 1;
                        row.last_error = Some(err.clone());
                        let outcome = if row.attempts >= retry.max {
                            row.status = JobStatus::Dead;
                            JobOutcome::Dead { id, attempts: row.attempts }
                        } else {
                            row.due_at = now.saturating_add(retry.backoff_nanos(row.attempts));
                            JobOutcome::Retried { id, attempts: row.attempts, due_at: row.due_at }
                        };
                        tx.put(&row);
                        Ok::<_, std::io::Error>(outcome)
                    })
                    .map_err(|e| match e {
                        TxError::Io(e) | TxError::App(e) => e,
                    })?;
                if let JobOutcome::Retried { due_at, .. } = outcome {
                    self.due.insert((due_at, id));
                }
                Ok(outcome)
            }
        }
    }
}

fn panic_text(payload: &(dyn std::any::Any + Send)) -> &str {
    // &*payload, not &payload: a &Box would itself coerce into the trait
    // object and every downcast would miss (the swarm-runner lesson).
    if let Some(s) = payload.downcast_ref::<&str>() {
        s
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s
    } else {
        "<non-string panic payload>"
    }
}
```

Note the borrow snag: `run_due`'s first transact closure borrows `self.defs` while `db` is borrowed mutably — `Db::transact` takes `&mut self` on `db` only, and the closure captures `&self.defs`; `self.due` is not touched inside the closure, so a plain `&self.defs` local (`let defs = &self.defs;`) before the closure keeps the borrow checker happy. If the checker still objects, restructure: read the row's name in a snapshot FIRST, resolve `def` and copy `retry` before the transact, and let the closure capture only the resolved `glue` fn pointer (fn pointers are Copy). The second form is the fallback — prefer whichever compiles cleanly with the smallest code.

Re-export from `kernel/mod.rs`: `pub use dispatch::{Dispatcher, JobDef, JobEntry, JobError, JobOutcome};`.

- [ ] **Step 4: Run** — `cargo test -p charpente --test kernel` → PASS; workspace clean.

- [ ] **Step 5: Commit** — `charpente: kernel dispatcher — due-heap, atomic done-mark, typed retry to dead-letter, panic absorption`

---

### Task 5: Cron — schedules are rows, ticks are transactions

**Files:**
- Create: `crates/charpente/src/kernel/cron.rs`
- Modify: `crates/charpente/src/kernel/mod.rs`, `crates/charpente/src/http/app.rs`, `crates/charpente/tests/kernel.rs`

**Interfaces:**
- Consumes: Tasks 2–4 (`JobRow` via the normal enqueue path, `Dispatcher::note_enqueued` fed by `drain_enqueued`).
- Produces:
  - `#[table] CronRow { id: Id<CronRow>, #[index] name: String, interval_nanos: u64, next_due: u64 }` — registered by `App::open` (like `Session`).
  - `App::schedule<J: Job + 'static>(self, name: &'static str, every: Duration, make: impl Fn() -> J + Send + 'static) -> App` — mount-time duplicate-name panic; `App` gains `pub(crate) fn take_cron_entries(&mut self)` + `#[doc(hidden)] pub fn take_cron_entries_for_tests(&mut self)`.
  - `CronScheduler::new(entries) -> CronScheduler`; `upsert(&mut self, db: &mut Db, now: u64) -> io::Result<()>` (new names get `next_due = now + interval`; existing rows keep `next_due`, interval updated); `next_deadline(&self) -> Option<u64>`; `has_due(&self, now: u64) -> bool`; `tick(&mut self, db: &mut Db, now: u64) -> io::Result<(&'static str, bool)>` (runs ONE due entry: one transaction sets `next_due = now + interval` — catch-up fires once, no backfill — and enqueues via the entry's closure; returns the name plus `catchup: true` when at least one whole interval was missed, i.e. `now >= due + interval` — the kernel's stats consume it).

- [ ] **Step 1: Write the failing tests** — append to `tests/kernel.rs`:

```rust
use charpente::kernel::CronScheduler;
use std::time::Duration;

const HOUR: u64 = 3_600_000_000_000;

fn cron_app(seed: u64) -> (Sim, App) {
    let sim = Sim::with_faults(seed, FaultConfig::none());
    let app = App::open(
        Box::new(sim.disk()),
        Schema::new().table::<Counter>(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )
    .unwrap()
    .mount_jobs(jobs![CountUp])
    .schedule("hourly-count", Duration::from_secs(3600), || CountUp { slot: 0 });
    (sim, app)
}

#[test]
fn cron_upsert_ticks_and_catches_up_once() {
    let (_sim, mut app) = cron_app(0x630);
    let mut cron = CronScheduler::new(app.take_cron_entries_for_tests());
    let db = app.db_for_tests();

    cron.upsert(db, 0).unwrap();
    assert_eq!(cron.next_deadline(), Some(HOUR));
    assert!(!cron.has_due(HOUR - 1) && cron.has_due(HOUR));

    // A tick advances the schedule and enqueues through the normal path.
    let (name, catchup) = cron.tick(db, HOUR).unwrap();
    assert_eq!((name, catchup), ("hourly-count", false));
    assert_eq!(db.drain_enqueued().len(), 1);
    assert_eq!(cron.next_deadline(), Some(2 * HOUR));

    // Downtime: three intervals pass unserved. Catch-up fires ONCE and the
    // schedule jumps past now — no backfill.
    let much_later = 5 * HOUR + 7;
    assert!(cron.has_due(much_later));
    let (_, catchup) = cron.tick(db, much_later).unwrap();
    assert!(catchup, "a tick past a whole missed interval reports catch-up");
    assert_eq!(db.drain_enqueued().len(), 1, "one catch-up tick, not three");
    assert_eq!(cron.next_deadline(), Some(much_later + HOUR));
    assert!(!cron.has_due(much_later));
}

#[test]
fn cron_schedule_survives_reopen_without_resetting_phase() {
    let sim = Sim::with_faults(0x631, FaultConfig::none());
    let open = |sim: &Sim| {
        App::open(
            Box::new(sim.disk()),
            Schema::new().table::<Counter>(),
            Box::new(sim.entropy()),
            AppConfig::default(),
        )
        .unwrap()
        .mount_jobs(jobs![CountUp])
        .schedule("hourly-count", Duration::from_secs(3600), || CountUp { slot: 0 })
    };
    let mut app = open(&sim);
    let mut cron = CronScheduler::new(app.take_cron_entries_for_tests());
    cron.upsert(app.db_for_tests(), 0).unwrap();

    sim.restart();
    drop(app);
    let mut app = open(&sim);
    let mut cron = CronScheduler::new(app.take_cron_entries_for_tests());
    // Re-upsert at a later now: the existing row keeps its phase.
    cron.upsert(app.db_for_tests(), 30 * 60 * 1_000_000_000).unwrap();
    assert_eq!(
        cron.next_deadline(),
        Some(HOUR),
        "an existing schedule keeps next_due across restarts"
    );
}

#[test]
#[should_panic(expected = "scheduled twice")]
fn duplicate_cron_names_panic_at_mount() {
    let sim = Sim::with_faults(0x632, FaultConfig::none());
    let _ = App::open(
        Box::new(sim.disk()),
        Schema::new(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )
    .unwrap()
    .schedule("n", Duration::from_secs(1), || CountUp { slot: 0 })
    .schedule("n", Duration::from_secs(2), || CountUp { slot: 1 });
}
```

- [ ] **Step 2: Run to verify failure** — `cargo test -p charpente --test kernel cron` → FAIL.

- [ ] **Step 3: Implement `cron.rs`:**

```rust
//! Cron (DESIGN §6): schedules are rows, ticks are transactions, and a tick
//! is just a recurring enqueue — jobs inherit retry/status/observability.
//! Catch-up fires once (next_due jumps past now); backfill is a non-goal.

use std::io;
use std::time::Duration;

use charpente_core::db::{Db, Tx, TxError};
use charpente_core::table::{Id, View};
use charpente_macros::table;

#[table]
pub struct CronRow {
    pub id: Id<CronRow>,
    #[index]
    pub name: String,
    pub interval_nanos: u64,
    pub next_due: u64,
}

pub struct CronEntry {
    pub name: &'static str,
    pub interval: Duration,
    pub enqueue: Box<dyn Fn(&mut Tx) + Send>,
}

pub struct CronScheduler {
    /// (entry, cached next_due) — the cache mirrors the row; the store is
    /// the truth and upsert/tick keep them identical.
    entries: Vec<(CronEntry, u64)>,
}

impl CronScheduler {
    pub fn new(entries: Vec<CronEntry>) -> CronScheduler {
        CronScheduler {
            entries: entries.into_iter().map(|e| (e, 0)).collect(),
        }
    }

    pub fn upsert(&mut self, db: &mut Db, now: u64) -> io::Result<()> {
        for (entry, cached) in &mut self.entries {
            let interval = nanos(entry.interval);
            let existing = {
                let snap = db.snapshot()?;
                CronRow::by_name(&snap, &entry.name.to_string())?.pop()
            };
            let row = match existing {
                Some(mut row) => {
                    row.interval_nanos = interval;
                    row
                }
                None => {
                    let snap = db.snapshot()?;
                    let next_id = snap
                        .scan::<CronRow>()?
                        .last()
                        .map_or(0, |r| r.id.raw() + 1);
                    CronRow {
                        id: Id::from_raw(next_id),
                        name: entry.name.to_string(),
                        interval_nanos: interval,
                        next_due: now + interval,
                    }
                }
            };
            *cached = row.next_due;
            db.transact(|tx| {
                tx.put(&row);
                Ok::<(), io::Error>(())
            })
            .map_err(flatten)?;
        }
        Ok(())
    }

    pub fn next_deadline(&self) -> Option<u64> {
        self.entries.iter().map(|&(_, due)| due).min()
    }

    pub fn has_due(&self, now: u64) -> bool {
        self.next_deadline().is_some_and(|d| d <= now)
    }

    pub fn tick(&mut self, db: &mut Db, now: u64) -> io::Result<(&'static str, bool)> {
        let (entry, cached) = self
            .entries
            .iter_mut()
            .filter(|(_, due)| *due <= now)
            .min_by_key(|(_, due)| *due)
            .expect("tick called with nothing due");
        let interval = nanos(entry.interval);
        let catchup = now >= (*cached).saturating_add(interval); // a whole interval missed
        let next = now + interval; // catch-up fires once: jump past now
        let name = entry.name;
        let enqueue = &entry.enqueue;
        db.transact(|tx| {
            let mut row = CronRow::by_name(tx, &name.to_string())?
                .pop()
                .unwrap_or_else(|| panic!("cron invariant: schedule \"{name}\" has no row"));
            row.next_due = next;
            tx.put(&row);
            enqueue(tx);
            Ok::<(), io::Error>(())
        })
        .map_err(flatten)?;
        *cached = next;
        Ok((name, catchup))
    }
}

fn nanos(d: Duration) -> u64 {
    u64::try_from(d.as_nanos()).expect("cron interval exceeds u64 nanos")
}

fn flatten(e: TxError<io::Error>) -> io::Error {
    match e {
        TxError::App(e) | TxError::Io(e) => e,
    }
}
```

  - `app.rs`: `schedule` builds `CronEntry { name, interval: every, enqueue: Box::new(move |tx| tx.enqueue(make())) }`, panicking on duplicates: `cron schedule "{name}" scheduled twice — schedule each name once`. `App::open` registers `CronRow` in the schema (alongside `Session`).
  - `kernel/mod.rs`: `mod cron; pub use cron::{CronRow, CronScheduler};` (CronEntry stays crate-internal except the `#[doc(hidden)]` test accessor path).
  - Upsert-vs-cache subtlety: `upsert` runs one transaction per entry for simplicity (entries are few; each is one put). The `by_name(tx, …)` read inside `tick` uses the `View` impl on `Tx` — read-your-own-writes is irrelevant here but the trait bound is the same.

- [ ] **Step 4: Run** — `cargo test -p charpente --test kernel` → PASS; workspace clean.

- [ ] **Step 5: Commit** — `charpente: cron — schedules are rows, a tick is one transaction that re-arms and enqueues`

---

### Task 6: The kernel event loop — `Kernel`, `RequestSource`, `SimSource`

**Files:**
- Modify: `crates/charpente/src/kernel/mod.rs`, `crates/charpente/tests/kernel.rs`

**Interfaces:**
- Consumes: Tasks 3–5 (`Dispatcher`, `CronScheduler`, App accessors), `charpente_core::io::{Clock, Instant}`, `App::handle`.
- Produces (all in `charpente::kernel`):

```rust
pub type Reply = Box<dyn FnOnce(Response) + Send>;
pub enum Wake { Ready, Deadline, Shutdown }

pub trait RequestSource {
    /// Non-blocking. A returned request must eventually get its Reply called.
    fn poll(&mut self) -> Option<(Request, Reply)>;
    /// Parks until a request arrives (Ready), the deadline passes (Deadline),
    /// or the source is exhausted (Shutdown). Must not drop requests: one
    /// observed during the park surfaces from the next poll().
    fn wait_until(&mut self, deadline: Option<Instant>) -> Wake;
}

#[derive(Default, Clone, Copy, Debug)]
pub struct KernelStats {
    pub requests: u64,
    pub jobs_done: u64,
    pub jobs_retried: u64,
    pub jobs_dead: u64,
    pub cron_ticks: u64,
    pub cron_catchups: u64,
}

pub struct Kernel { /* app, dispatcher, cron, clock, pending, stats */ }
impl Kernel {
    /// Consumes the mounted App: upserts cron rows, rebuilds the due-heap
    /// from Pending rows. `now` for cron phase comes from `clock`.
    pub fn new(app: App, clock: Box<dyn Clock>) -> io::Result<Kernel>;
    /// The event loop (DESIGN §1 same-shape rule): identical code under
    /// serve() and under the sim; only the RequestSource impl differs.
    /// Returns Ok(()) on source Shutdown; Err on platform IO failure
    /// (poisoned engine — restart to recover).
    pub fn run(&mut self, source: &mut dyn RequestSource) -> io::Result<()>;
    pub fn stats(&self) -> KernelStats;
    pub fn into_app(self) -> App; // post-run inspection in tests
}

/// Deterministic scripted source for sim tests: requests arrive at virtual
/// times; replies record (status, body); the horizon bounds the universe.
pub struct SimSource { /* clock, script: VecDeque<(u64, Request)>, responses, horizon */ }
impl SimSource {
    pub fn new(clock: Box<dyn Clock>, horizon_nanos: u64) -> SimSource;
    pub fn push_at(&mut self, at_nanos: u64, request: Request);
    pub fn responses(&self) -> Arc<Mutex<Vec<(u16, Vec<u8>)>>>;
}
```

- [ ] **Step 1: Write the failing tests** — append to `tests/kernel.rs`:

```rust
use charpente::http::{Method, Request};
use charpente::kernel::{Kernel, SimSource};
use charpente::routes;
use charpente::http::ApiError;
use charpente::route;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct Hit {
    slot: u64,
}

#[route(POST "/hit")]
fn hit(tx: &mut Tx, body: Hit) -> Result<u64, ApiError> {
    tx.enqueue(CountUp { slot: body.slot });
    Ok(body.slot)
}

const SEC: u64 = 1_000_000_000;

fn loop_app(sim: &Sim) -> App {
    App::open(
        Box::new(sim.disk()),
        Schema::new().table::<Counter>(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )
    .unwrap()
    .mount(routes![hit])
    .mount_jobs(jobs![CountUp])
    .schedule("hourly-count", Duration::from_secs(3600), || CountUp { slot: 777 })
}

#[test]
fn loop_serves_requests_runs_jobs_and_ticks_cron() {
    let sim = Sim::with_faults(0x640, FaultConfig::none());
    let mut source = SimSource::new(Box::new(sim.clock()), 2 * HOUR + SEC);
    for i in 0..3u64 {
        source.push_at(i * SEC, Request::new(Method::Post, "/hit").json(&Hit { slot: i }));
    }
    let responses = source.responses();

    let mut kernel = Kernel::new(loop_app(&sim), Box::new(sim.clock())).unwrap();
    kernel.run(&mut source).unwrap(); // returns at the horizon (Shutdown)

    let stats = kernel.stats();
    assert_eq!(stats.requests, 3);
    assert_eq!(stats.cron_ticks, 2, "two hours inside the horizon");
    assert_eq!(stats.jobs_done, 5, "3 request jobs + 2 cron jobs");
    assert_eq!((stats.jobs_retried, stats.jobs_dead), (0, 0));

    let recorded = responses.lock().unwrap();
    assert_eq!(recorded.len(), 3);
    assert!(recorded.iter().all(|(status, _)| *status == 200));

    let mut app = kernel.into_app();
    let snap = app.db_for_tests().snapshot().unwrap();
    let counters = snap.scan::<Counter>().unwrap();
    // slots 0,1,2 hit once each; cron slot 777 hit twice.
    assert_eq!(counters.len(), 4);
    assert_eq!(
        counters.iter().map(|c| (c.id.raw(), c.hits)).collect::<Vec<_>>(),
        vec![(0, 1), (1, 1), (2, 1), (777, 2)]
    );
    let jobs = snap.scan::<JobRow>().unwrap();
    assert!(jobs.iter().all(|j| j.status == JobStatus::Done));
}

#[test]
fn loop_replays_identically_for_the_same_seed() {
    let trace = |seed: u64| {
        let sim = Sim::new(seed); // default faults, including latency
        let mut source = SimSource::new(Box::new(sim.clock()), HOUR);
        for i in 0..5u64 {
            source.push_at(i * SEC, Request::new(Method::Post, "/hit").json(&Hit { slot: i }));
        }
        let responses = source.responses();
        let run = (|| -> Result<(), std::io::Error> {
            let mut kernel = Kernel::new(loop_app(&sim), Box::new(sim.clock()))?;
            kernel.run(&mut source)
        })();
        let outcome = format!("{run:?}");
        let recorded = responses.lock().unwrap().clone();
        (outcome, recorded, sim.durable_fingerprint(), sim.now())
    };
    for seed in [0x7, 0xB0B, 0xC0FFEE] {
        assert_eq!(trace(seed), trace(seed), "seed {seed:#x}");
    }
}
```

(`into_app` hands the App back for post-run inspection through the Task-3 `db_for_tests` accessor.)

- [ ] **Step 2: Run to verify failure** — `cargo test -p charpente --test kernel loop_` → FAIL (`Kernel` undefined).

- [ ] **Step 3: Implement in `kernel/mod.rs`.** The loop — the same-shape rule made code:

```rust
use std::io;
use std::sync::{Arc, Mutex};

use charpente_core::io::{Clock, Instant};

use crate::http::{App, Request, Response};

#[derive(Clone, Copy)]
enum Ready {
    Request,
    Job,
    Cron,
}

impl Kernel {
    pub fn new(mut app: App, clock: Box<dyn Clock>) -> io::Result<Kernel> {
        let mut dispatcher = Dispatcher::new(app.take_job_defs());
        let mut cron = CronScheduler::new(app.take_cron_entries());
        let now = clock.now().as_nanos();
        cron.upsert(app.db_mut(), now)?;
        dispatcher.rebuild(&app.db_mut().snapshot()?)?;
        Ok(Kernel {
            app,
            dispatcher,
            cron,
            clock,
            pending: None,
            stats: KernelStats::default(),
        })
    }

    pub fn run(&mut self, source: &mut dyn RequestSource) -> io::Result<()> {
        loop {
            if self.pending.is_none() {
                self.pending = source.poll();
            }
            let now = self.clock.now().as_nanos();
            let mut ready = Vec::with_capacity(3);
            if self.pending.is_some() {
                ready.push(Ready::Request);
            }
            if self.dispatcher.has_due(now) {
                ready.push(Ready::Job);
            }
            if self.cron.has_due(now) {
                ready.push(Ready::Cron);
            }
            if ready.is_empty() {
                let deadline = [self.dispatcher.next_deadline(), self.cron.next_deadline()]
                    .into_iter()
                    .flatten()
                    .min()
                    .map(Instant::from_nanos);
                match source.wait_until(deadline) {
                    Wake::Shutdown => return Ok(()),
                    Wake::Ready | Wake::Deadline => continue,
                }
            }
            // The same-shape decision site (DESIGN §1): one entropy draw picks
            // among ready sources, only when contended.
            let choice = if ready.len() == 1 {
                ready[0]
            } else {
                ready[(self.app.entropy().next_u64() % ready.len() as u64) as usize]
            };
            match choice {
                Ready::Request => {
                    let (request, reply) = self.pending.take().expect("checked above");
                    let response = self.app.handle(request);
                    reply(response);
                    self.stats.requests += 1;
                }
                Ready::Job => {
                    match self.dispatcher.run_due(self.app.db_mut(), now)? {
                        JobOutcome::Done { .. } => self.stats.jobs_done += 1,
                        JobOutcome::Retried { .. } => self.stats.jobs_retried += 1,
                        JobOutcome::Dead { .. } => self.stats.jobs_dead += 1,
                    }
                }
                Ready::Cron => {
                    let (_, catchup) = self.cron.tick(self.app.db_mut(), now)?;
                    self.stats.cron_ticks += 1;
                    if catchup {
                        self.stats.cron_catchups += 1;
                    }
                }
            }
            // Any serviced source may have enqueued (handlers can enqueue too).
            self.dispatcher.note_enqueued(self.app.db_mut().drain_enqueued());
        }
    }
}
```

`SimSource` (same file — deterministic, no ambient IO; `Arc<Mutex>` is the honest uncontended pattern from the sim world):

```rust
pub struct SimSource {
    clock: Box<dyn Clock>,
    script: std::collections::VecDeque<(u64, Request)>,
    responses: Arc<Mutex<Vec<(u16, Vec<u8>)>>>,
    horizon: u64,
}

impl SimSource {
    pub fn new(clock: Box<dyn Clock>, horizon_nanos: u64) -> SimSource {
        SimSource {
            clock,
            script: Default::default(),
            responses: Arc::new(Mutex::new(Vec::new())),
            horizon: horizon_nanos,
        }
    }

    /// Requests must be pushed in arrival order (asserted).
    pub fn push_at(&mut self, at_nanos: u64, request: Request) {
        assert!(
            self.script.back().is_none_or(|&(t, _)| t <= at_nanos),
            "SimSource script must be pushed in arrival order"
        );
        self.script.push_back((at_nanos, request));
    }

    pub fn responses(&self) -> Arc<Mutex<Vec<(u16, Vec<u8>)>>> {
        self.responses.clone()
    }
}

impl RequestSource for SimSource {
    fn poll(&mut self) -> Option<(Request, Reply)> {
        let now = self.clock.now().as_nanos();
        if self.script.front().is_some_and(|&(at, _)| at <= now) {
            let (_, request) = self.script.pop_front().expect("checked");
            let sink = self.responses.clone();
            let reply: Reply = Box::new(move |response: Response| {
                sink.lock()
                    .expect("responses lock poisoned by an earlier panic")
                    .push((response.status, response.body));
            });
            return Some((request, reply));
        }
        None
    }

    fn wait_until(&mut self, deadline: Option<Instant>) -> Wake {
        let now = self.clock.now().as_nanos();
        let arrival = self.script.front().map(|&(at, _)| at);
        let target = [arrival, deadline.map(Instant::as_nanos)]
            .into_iter()
            .flatten()
            .min();
        match target {
            Some(t) if t <= self.horizon => {
                if t > now {
                    self.clock.sleep(std::time::Duration::from_nanos(t - now));
                }
                if arrival == Some(t) { Wake::Ready } else { Wake::Deadline }
            }
            _ => Wake::Shutdown, // nothing left inside this universe's horizon
        }
    }
}
```

- [ ] **Step 4: Run** — `cargo test -p charpente --test kernel` → PASS; workspace clean. Sanity-check the stats arithmetic in `loop_serves_requests_runs_jobs_and_ticks_cron` against the horizon you chose — if the counts differ, the bug is real (deadlines vs horizon boundary): investigate, don't adjust the assertion to match.

- [ ] **Step 5: Commit** — `charpente: the kernel event loop — one entropy-drawn pick over ready sources (DESIGN §1 same-shape rule)`

---

### Task 7: serve.rs reshape — `ProdSource`

**Files:**
- Modify: `crates/charpente/src/serve.rs`

**Interfaces:**
- Consumes: Task 6's `Kernel`, `RequestSource`, `Reply`, `Wake`; `crate::prod::ProdClock`.
- Produces: `serve(addr, app)` signature unchanged; the kernel thread runs `Kernel::run(&mut ProdSource { .. })`; the mpsc channel item becomes `(Request, Reply)`.

- [ ] **Step 1: Reshape.** In `serve.rs`:
  - Channel: `mpsc::channel::<(Request, kernel::Reply)>()`.
  - Hyper side: where it currently builds `(request, tx)` with a oneshot, build the Reply: `let reply: kernel::Reply = Box::new(move |response| { let _ = tx.send(response); });` and send `(request, reply)`; the `rx.await` handling is unchanged.
  - Kernel thread:

```rust
std::thread::spawn(move || {
    let clock = crate::prod::ProdClock::new();
    let mut kernel = match crate::kernel::Kernel::new(app, Box::new(crate::prod::ProdClock::new())) {
        Ok(kernel) => kernel,
        Err(e) => {
            eprintln!("charpente kernel failed to start — fix the store/log and restart: {e}");
            return;
        }
    };
    let mut source = ProdSource { rx: kernel_rx, stash: None, clock };
    if let Err(e) = kernel.run(&mut source) {
        eprintln!("charpente kernel stopped on a platform IO failure — restart to recover: {e}");
    }
});
```

  - `ProdSource` (edge module — ambient IO sanctioned here):

```rust
struct ProdSource {
    rx: mpsc::Receiver<(Request, crate::kernel::Reply)>,
    stash: Option<(Request, crate::kernel::Reply)>,
    clock: crate::prod::ProdClock,
}

impl crate::kernel::RequestSource for ProdSource {
    fn poll(&mut self) -> Option<(Request, crate::kernel::Reply)> {
        self.stash.take().or_else(|| self.rx.try_recv().ok())
    }

    fn wait_until(&mut self, deadline: Option<Instant>) -> crate::kernel::Wake {
        use crate::kernel::Wake;
        use charpente_core::io::Clock;
        match deadline {
            Some(deadline) => {
                let now = self.clock.now();
                if deadline <= now {
                    return Wake::Deadline;
                }
                match self.rx.recv_timeout(deadline.duration_since(now)) {
                    Ok(item) => {
                        self.stash = Some(item);
                        Wake::Ready
                    }
                    Err(mpsc::RecvTimeoutError::Timeout) => Wake::Deadline,
                    Err(mpsc::RecvTimeoutError::Disconnected) => Wake::Shutdown,
                }
            }
            None => match self.rx.recv() {
                Ok(item) => {
                    self.stash = Some(item);
                    Wake::Ready
                }
                Err(_) => Wake::Shutdown,
            },
        }
    }
}
```

  - `deadline.duration_since(now)` — wait: that's our `Instant`'s panicking-if-backwards method; the `deadline <= now` early return above guards it. Add `Instant::as_nanos` import if needed.

- [ ] **Step 2: Run the existing edge tests** — `cargo test -p charpente --lib serve` (the two loopback smoke tests) → PASS: a route-only app has no jobs and no cron, so `wait_until(None)` parks on `recv` exactly like the old loop.

- [ ] **Step 3: Full suite** — `cargo test --workspace` → PASS; fmt/clippy clean.

- [ ] **Step 4: Commit** — `charpente: serve feeds the kernel loop through ProdSource — the blocking recv loop is gone`

---

### Task 8: The step-6 swarm properties + the negative-space meta-test

**Files:**
- Modify: `crates/charpente/tests/kernel.rs`

**Interfaces:** consumes everything above; produces no new API.

- [ ] **Step 1: The flagship property.** Append:

```rust
use charpente::http::Response;
use charpente_core::log::WalConfig;
use charpente_sim::run_universes_swarm;

fn crash_config() -> AppConfig {
    AppConfig {
        body_cap: 1 << 20,
        db: charpente::DbConfig {
            wal: WalConfig { dir: "wal".to_string(), segment_bytes: 4096 },
            store_path: "tables.redb".to_string(),
        },
    }
}

#[job(retry(max = 3, backoff = "5s"))]
struct Flaky {
    roll: u64,
}

impl Flaky {
    fn run(self, tx: &mut Tx) -> Result<(), JobError> {
        // Deterministic per-payload failure: rolls 0/1 always fail (their
        // retries march to Dead through the log); 2.. succeed and count.
        if self.roll < 2 {
            return Err(JobError::app(format!("flaky roll {} refused", self.roll)));
        }
        let hits = tx.get(Id::<Counter>::from_raw(self.roll))?.map_or(0, |c| c.hits);
        tx.put(&Counter { id: Id::from_raw(self.roll), hits: hits + 1 });
        Ok(())
    }
}

fn kernel_crash_app(sim: &Sim) -> std::io::Result<App> {
    Ok(App::open(
        Box::new(sim.disk()),
        Schema::new().table::<Counter>(),
        Box::new(sim.entropy()),
        crash_config(),
    )?
    .mount(routes![hit])
    .mount_jobs(jobs![CountUp, Flaky])
    .schedule("minutely", Duration::from_secs(60), || CountUp { slot: 999 }))
}

fn is_injected(text: &str) -> bool {
    text.contains("simulated") || text.contains("injected")
}

/// The exactly-once invariant: every Done CountUp{slot} bumped its counter
/// exactly once — hits ≡ Done-count per slot, regardless of crashes.
fn exactly_once_holds(app: &mut App) -> Result<(), String> {
    let snap = app.db_for_tests().snapshot().map_err(|e| e.to_string())?;
    let jobs = snap.scan::<JobRow>().map_err(|e| e.to_string())?;
    let counters: std::collections::BTreeMap<u64, u64> = snap
        .scan::<Counter>()
        .map_err(|e| e.to_string())?
        .into_iter()
        .map(|c| (c.id.raw(), c.hits))
        .collect();
    let mut done: std::collections::BTreeMap<u64, u64> = Default::default();
    for job in &jobs {
        if job.status == JobStatus::Done && job.name == "CountUp" {
            let slot = u64::decode(&mut &job.payload[..]).map_err(|e| e.to_string())?;
            *done.entry(slot).or_insert(0) += 1;
        }
    }
    for (&slot, &done_count) in &done {
        let hits = counters.get(&slot).copied().unwrap_or(0);
        if hits != done_count {
            return Err(format!(
                "exactly-once violated at slot {slot}: {done_count} Done jobs but {hits} hits"
            ));
        }
    }
    // And no counter without a Done job (effects never outrun the done-mark —
    // this holds for cron's slot 999 too; its hits also commit with a done-mark).
    for (&slot, &hits) in &counters {
        if done.get(&slot).copied().unwrap_or(0) == 0 && hits > 0 {
            return Err(format!("slot {slot} has {hits} hits but no Done job"));
        }
    }
    Ok(())
}

fn kernel_workload(sim: &mut Sim) -> Result<(), String> {
    use charpente_core::table::Codec;
    let entropy = sim.entropy();
    for _round in 0..3 {
        sim.crash_disk_after(64 + entropy.next_u64() % 768);
        let mut app = match kernel_crash_app(sim) {
            Ok(app) => app,
            Err(e) => {
                if !is_injected(&e.to_string()) {
                    return Err(format!("open failed non-injected: {e}"));
                }
                sim.restart();
                continue;
            }
        };
        exactly_once_holds(&mut app)?;

        let horizon = sim.now().as_nanos() + 400 * SEC;
        let mut source = SimSource::new(Box::new(sim.clock()), horizon);
        let base = sim.now().as_nanos();
        for i in 0..(1 + entropy.next_u64() % 6) {
            let slot = entropy.next_u64() % 8;
            source.push_at(
                base + i * SEC,
                Request::new(Method::Post, "/hit").json(&Hit { slot }),
            );
        }
        // A couple of Flaky jobs enqueued directly (retry path under faults).
        let _ = app.db_for_tests().transact(|tx| {
            tx.enqueue(Flaky { roll: entropy.next_u64() % 4 });
            Ok::<(), std::io::Error>(())
        });

        let run = (|| -> Result<(), std::io::Error> {
            let mut kernel = Kernel::new(app, Box::new(sim.clock()))?;
            let out = kernel.run(&mut source);
            let stats = kernel.stats();
            if stats.jobs_done > 0 { sim.cover("job:done"); }
            if stats.jobs_retried > 0 { sim.cover("job:retried"); }
            if stats.jobs_dead > 0 { sim.cover("job:dead"); }
            if stats.cron_ticks > 0 { sim.cover("cron:tick"); }
            if stats.cron_catchups > 0 { sim.cover("cron:catchup"); }
            out
        })();
        if let Err(e) = run {
            if !is_injected(&e.to_string()) {
                return Err(format!("kernel stopped non-injected: {e}"));
            }
        }
        sim.restart();
    }

    // Final quiet verification (step-4/5 convention): reopen until injected
    // faults relent, then check the invariant one last time.
    for _attempt in 0..8 {
        match kernel_crash_app(sim) {
            Ok(mut app) => return exactly_once_holds(&mut app),
            Err(e) if is_injected(&e.to_string()) => sim.restart(),
            Err(e) => return Err(format!("final open failed non-injected: {e}")),
        }
    }
    Ok(())
}

#[test]
fn kernel_jobs_are_exactly_once_under_the_fault_matrix() {
    run_universes_swarm!(256, kernel_workload, require: [
        "job:done",
        "job:retried",
        "cron:tick",
        "fault:crash",
        "crash:torn",
        "restart",
    ]);
}
```

Notes for the implementer: `job:dead` and `cron:catchup` are NOT required keys — dead needs 3 failures of the same Flaky job to land inside one universe's horizon and catchup needs particular crash timing; both are asserted deterministically below instead (a required key that needs luck is a flaky batch).

- [ ] **Step 2: The deterministic guarantees tests.** Append:

```rust
#[test]
fn always_failing_job_dies_and_the_monthly_cron_ticks_120_times() {
    let sim = Sim::with_faults(0x650, FaultConfig::none());
    let app = App::open(
        Box::new(sim.disk()),
        Schema::new().table::<Counter>(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )
    .unwrap()
    .mount_jobs(jobs![AlwaysFails, CountUp])
    .schedule("monthly", Duration::from_secs(30 * 24 * 3600), || CountUp { slot: 1 });

    // Kernel::new consumes the App: all seeding happens BEFORE construction.
    let mut app = app;
    app.db_for_tests()
        .transact(|tx| {
            tx.enqueue(AlwaysFails {});
            Ok::<(), std::io::Error>(())
        })
        .unwrap();
    let mut kernel = Kernel::new(app, Box::new(sim.clock())).unwrap();
    let mut source = SimSource::new(
        Box::new(sim.clock()),
        10 * 365 * 24 * 3600 * SEC, // ten years — DESIGN §6's promise, literally
    );
    kernel.run(&mut source).unwrap();

    let stats = kernel.stats();
    assert_eq!(stats.jobs_dead, 1, "AlwaysFails dies at max attempts");
    assert_eq!(stats.jobs_retried, 1, "max 2: one retry, then dead");
    // First tick lands one interval in; ten years of 30-day months:
    assert_eq!(stats.cron_ticks, 121);
    assert_eq!(stats.cron_catchups, 0);
    let app = kernel.into_app();
    // ...and this whole decade of queue traffic ran in well under a second
    // of wall time, or the suite would be unshippable. Virtual time works.
    drop(app);
}
```

(The `let mut app = app;` rebind exists only because the builder chain above returned a non-mut binding — collapse it if you declared `mut` at the start.)

- [ ] **Step 3: The negative-space meta-test** — prove the harness would catch the dual-write bug this design deletes:

```rust
/// A deliberately WRONG executor: handler effects commit first, the
/// done-mark commits separately. The exactly-once invariant must be able to
/// see the difference under a crash between the two — otherwise our flagship
/// property is theater.
#[test]
fn harness_catches_a_split_done_mark() {
    let sim = Sim::with_faults(0x660, FaultConfig::none());
    let mut app = App::open(
        Box::new(sim.disk()),
        Schema::new().table::<Counter>(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )
    .unwrap()
    .mount_jobs(jobs![CountUp]);
    let db = app.db_for_tests();
    db.transact(|tx| {
        tx.enqueue(CountUp { slot: 5 });
        Ok::<(), std::io::Error>(())
    })
    .unwrap();
    let id = db.drain_enqueued()[0];

    // Wrong executor, transaction 1: handler effects WITHOUT the done-mark.
    db.transact(|tx| {
        let hits = tx.get(Id::<Counter>::from_raw(5))?.map_or(0, |c| c.hits);
        tx.put(&Counter { id: Id::from_raw(5), hits: hits + 1 });
        Ok::<(), std::io::Error>(())
    })
    .unwrap();
    // Crash lands here, before transaction 2 (the separate done-mark).
    sim.restart();
    drop(app);

    // Recovery: the job is still Pending; a correct re-execution double-counts.
    let mut app = App::open(
        Box::new(sim.disk()),
        Schema::new().table::<Counter>(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )
    .unwrap()
    .mount_jobs(jobs![CountUp]);
    let mut dispatcher = Dispatcher::new(app.take_job_defs_for_tests());
    dispatcher.rebuild(&app.db_for_tests().snapshot().unwrap()).unwrap();
    assert!(dispatcher.has_due(0), "the split executor left the job Pending");
    dispatcher.run_due(app.db_for_tests(), 0).unwrap();

    let err = exactly_once_holds(&mut app).unwrap_err();
    assert!(
        err.contains("exactly-once violated at slot 5"),
        "the invariant must catch the split done-mark: {err}"
    );
    let _ = id;
}
```

- [ ] **Step 4: Key round-trip + starvation.** Append:

```rust
#[test]
fn idempotency_key_rides_the_row_and_means_nothing_to_the_platform() {
    let (_sim, mut app) = quiet_kernel_app(0x670);
    let db = app.db_for_tests();
    db.transact(|tx| {
        tx.enqueue_keyed("receipt:42", CountUp { slot: 1 });
        tx.enqueue_keyed("receipt:42", CountUp { slot: 2 }); // same key: BOTH exist
        Ok::<(), std::io::Error>(())
    })
    .unwrap();
    let jobs = db.snapshot().unwrap().scan::<JobRow>().unwrap();
    assert_eq!(jobs.len(), 2, "keys are identity, not dedupe — spec ruling");
    assert!(jobs.iter().all(|j| j.key.as_deref() == Some("receipt:42")));
}

#[test]
fn due_jobs_are_not_starved_by_a_request_flood() {
    let sim = Sim::with_faults(0x680, FaultConfig::none());
    let mut source = SimSource::new(Box::new(sim.clock()), 200 * SEC);
    for i in 0..100u64 {
        // All requests arrive at t=0: maximal contention with the due job.
        source.push_at(0, Request::new(Method::Post, "/hit").json(&Hit { slot: i }));
    }
    let mut app = loop_app(&sim);
    app.db_for_tests()
        .transact(|tx| {
            tx.enqueue(CountUp { slot: 7777 });
            Ok::<(), std::io::Error>(())
        })
        .unwrap();
    let mut kernel = Kernel::new(app, Box::new(sim.clock())).unwrap();
    kernel.run(&mut source).unwrap();
    assert_eq!(
        kernel.stats().jobs_done,
        101,
        "every job ran despite the flood (100 request jobs + the seeded one)"
    );
}
```

- [ ] **Step 5: Run the batch twice** — `cargo test -p charpente --test kernel` twice → PASS both, all required coverage keys present in the printed summary. Then full workspace.

- [ ] **Step 6: Commit** — `charpente: step-6 properties — exactly-once effects under the fault matrix, dead-letter, 120 monthly ticks in one second, split-done-mark meta-test`

---

### Task 9: Followup #24 fold — response-sequence determinism + is_injected checkpoint routing

**Files:**
- Modify: `crates/charpente/tests/http.rs`, `crates/charpente/tests/transactions.rs`

- [ ] **Step 1:** In `http.rs`, extend `http_replays_identically_for_the_same_seed`: change `http_workload` to also accumulate every `(response.status, response.body)` it observes into a `Vec<(u16, Vec<u8>)>` returned alongside (wrap: `fn http_workload_traced(sim) -> Result<Vec<(u16, Vec<u8>)>, String>`, and keep `http_workload` as `http_workload_traced(sim).map(|_| ())` so the swarm property test is untouched). The determinism test compares `(responses, durable_fingerprint, now)` across two runs of each seed.
- [ ] **Step 2:** In `transactions.rs`, find the property workload's checkpoint call (`checkpoint().is_err()` used as a plain break condition) and route it through the file's `is_injected` helper: a checkpoint error whose text is NOT injected is a real bug → `return Err(...)`; an injected one keeps the existing break behavior.
- [ ] **Step 3:** Run both suites twice (fresh swarm seeds) + full workspace; commit — `charpente: determinism test captures the response trace; checkpoint faults route through is_injected (followup #24)`

---

### Task 10: Docs — the spec is the docs

**Files:**
- Modify: `DESIGN.md`, `CLAUDE.md`

- [ ] **Step 1: DESIGN.md.**
  - §2 record payload: enqueue effect line becomes `` `0x02 [job][payload][key: Option<String>]` enqueue `` with a sentence: keys are the job's stable external identity (spec ruling: no platform semantics inside the boundary in v0; M1's external-IO surface consumes them).
  - §4 jobs-table paragraph: replace the `(seq, idx)`-key description with the `JobRow` shape (meta-counter ids, status/attempts/due_at/last_error, materialized at apply).
  - §6 Queue bullet: reality has landed — handlers are pure state transitions in v0 (exactly-once state effects structurally; the at-least-once + idempotency framing applies to M1's external-effect handlers); typed retry to dead-letter; cron as recurring enqueuer with fire-once catch-up.
  - §1: add one sentence to the executor paragraph noting the rule is now implemented by `charpente::kernel` (`Kernel::run` + `RequestSource`).
- [ ] **Step 2: CLAUDE.md** current-state: add the step-6-done bullet (kernel loop with entropy-picked ready set; JobRow in core with meta-counter ids; pure-state-transition handlers, exactly-once structurally; typed retry to dead-letter with absorbed panics; cron rows with fire-once catch-up; keys as external identity). Update "Next" to step 7 (`charpente test --sim` CLI) and note step 8 (demo app) behind it. State the evidence: which property tests, universe counts, runs.
- [ ] **Step 3:** Commit — `docs: M0 step 6 done — queue + cron + kernel loop landed; step 7 (test --sim CLI) is next`

---

## Evidence bar (hard rule 3 / CLAUDE.md)

Every property/batch test green over its full fresh-seed batch at least twice; the flagship exactly-once property at 256 swarm universes × 2 runs minimum; required coverage keys present every run. Full `cargo fmt`/`clippy -D warnings`/`cargo test --workspace` before every commit. Any property failure: STOP, pin the seed, adjudicate — never weaken an assertion to pass (the step-4 sessions found three real bugs this way).
