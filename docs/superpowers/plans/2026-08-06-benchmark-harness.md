# Benchmark Harness Implementation Plan (M2 sub-project 1)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `crates/bench-harness` — the four-layer honest-benchmark instrument (device / core / kernel / wire) of docs/superpowers/specs/2026-08-06-benchmark-harness-design.md, ending in `docs/BENCH.md` with the first dev-labeled numbers of record and the sync-bound prediction's verdict.

**Architecture:** One new edge crate (std-only, single `charpente` dependency, `charpente-sim` as dev-dependency) holding the canonical bench app as a lib, a `bench` binary with per-layer subcommands, and a `bench-app` serve vehicle. Measurement (CPU time, heartbeat sleep-guard, percentiles, totals cross-checks) and reporting (hand-rolled JSON pinned by test, human render, the no-number-without-its-ceiling refusal) are their own modules. Wire load generation is a hand-rolled keep-alive HTTP/1.1 client — the edge-harness client is Connection:-close-per-request and cannot be reused.

**Tech Stack:** Rust (edition 2024, workspace lints), zero external crates. `getrusage` via a hand-declared `unsafe extern "C"` block. Child processes + cargo-JSON binary location copied in shape from `crates/edge-harness/src/child.rs`.

## Global Constraints

- **Zero diffs inside `crates/charpente-core` and `crates/charpente`** (spec §9.4). If a seam is missing, STOP AND ASK — do not work around.
- Dependencies: `charpente = { path = "../charpente" }` only; `charpente-sim` as dev-dependency (the repo-wide idiom — invoicing does the same). No external crates, no serde/serde_json use in this crate: JSON is hand-rolled (spec §1).
- Deny-list: every file starts `#![allow(clippy::disallowed_methods, clippy::disallowed_types)]` under a module doc stating why (edge tooling outside the deterministic core — DESIGN §1's fence is the core, not this crate). Exactly the `crates/edge-harness/src/child.rs:1-12` idiom.
- Test discipline (edge-harness crate-doc criterion): any test touching a process, socket, or the real filesystem is `#[ignore]` and runs only via `scripts/bench.sh --smoke`; pure logic tests run un-ignored in `cargo test --workspace`.
- Import paths: all names are re-exported from `charpente`'s root, `charpente::prod`, `charpente::http`, `charpente::kernel`, or `charpente::core` (GUIDE-documented). If rustc rejects a `use` line, fix the path per the compiler — never vendor or redefine a type.
- Commits to `main`, named paths only (another session may share the tree), no Co-Authored-By trailers. Prefix: `bench:`.
- Ledger: append a dated entry per task to `.superpowers/sdd/2026-08-06-benchmark-harness/progress.md` (create in Task 1; the directory is gitignored).
- Seed namespace for this sub-project: `0xBE9C_00xx`.
- `docs/BENCH.md` must contain no ```` ```rust ```` blocks (it is not compile-gated and must not accidentally become so — spec §6).

---

### Task 1: Crate scaffold + the canonical bench app + deterministic sim proof

**Files:**
- Modify: `Cargo.toml` (workspace root — members array only)
- Create: `crates/bench-harness/Cargo.toml`
- Create: `crates/bench-harness/src/lib.rs`
- Test: `crates/bench-harness/tests/app.rs`
- Create: `.superpowers/sdd/2026-08-06-benchmark-harness/progress.md`

**Interfaces:**
- Consumes: `charpente` macros/types per GUIDE (`#[table]`, `#[route]`, `#[job]`, `#[event]`, `App::open`, `Schema::new`, `routes![]`, `jobs![]`); `charpente_sim::{Sim, FaultConfig}`; `charpente::kernel::{Kernel, SimSource}`; `charpente::http::{Method, Request}`.
- Produces (later tasks rely on these exact names): `bench_harness::SLOTS: u64`, `bench_harness::Slot { id: Id<Slot>, hits: u64 }`, `bench_harness::Bump { slot: u64 }`, `bench_harness::SlotBumped { slot: u64 }`, `bench_harness::schema() -> Schema`, `bench_harness::build(Box<dyn Disk>, Box<dyn Entropy>) -> io::Result<App>`, routes `POST /hit/:id` and `GET /slot/:id`.

- [ ] **Step 1: Workspace membership + crate manifest**

Append `"crates/bench-harness",` to the workspace `members` array (root `Cargo.toml`, after `"crates/edge-harness",`). Create `crates/bench-harness/Cargo.toml`:

```toml
[package]
name = "bench-harness"
description = "M2 sub-project 1: the honest-benchmarks instrument — four measurement layers (device/core/kernel/wire) against the prod IO stack and real binaries."
version.workspace = true
edition.workspace = true
license.workspace = true
publish = false

[lints]
workspace = true

[dependencies]
charpente = { path = "../charpente" }

[dev-dependencies]
charpente-sim = { path = "../charpente-sim" }
```

- [ ] **Step 2: Write the failing test**

`crates/bench-harness/tests/app.rs`:

```rust
//! Deterministic app-level proof that the canonical bench transaction works.
//! Sim-driven, no syscalls — runs un-ignored in the ordinary workspace pass.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use bench_harness::build;
use charpente::http::{Method, Request};
use charpente::kernel::{Kernel, SimSource};
use charpente_sim::{FaultConfig, Sim};

#[test]
fn canonical_txn_round_trips_under_sim() {
    let sim = Sim::with_faults(0xBE9C_0001, FaultConfig::none());
    let app = build(Box::new(sim.disk()), Box::new(sim.entropy())).expect("app builds");
    let mut source = SimSource::new(Box::new(sim.clock()), 10_000_000_000);
    for i in 0..3u64 {
        source.push_at(i, Request::new(Method::Post, "/hit/7"));
    }
    source.push_at(3, Request::new(Method::Get, "/slot/7"));
    let responses = source.responses();
    let mut kernel = Kernel::new(app, Box::new(sim.clock())).expect("kernel");
    kernel.run(&mut source).expect("run");

    let responses = responses.lock().expect("responses lock");
    assert_eq!(responses.len(), 4, "all four scripted requests answered");
    for (status, body) in responses.iter() {
        assert_eq!(*status, 200, "body: {}", String::from_utf8_lossy(body));
    }
    assert_eq!(
        String::from_utf8_lossy(&responses[3].1),
        "3",
        "GET /slot/7 sees all three hits"
    );
    let stats = kernel.stats();
    assert_eq!(stats.jobs_done, 3, "each hit enqueued one Bump and all ran");
}
```

- [ ] **Step 3: Run it, verify it fails**

Run: `cargo test -p bench-harness --test app`
Expected: FAIL to compile — `bench_harness::build` does not exist yet.

- [ ] **Step 4: Implement the canonical app**

`crates/bench-harness/src/lib.rs`:

```rust
//! bench-harness: the M2 sub-project 1 instrument
//! (docs/superpowers/specs/2026-08-06-benchmark-harness-design.md).
//!
//! Everything in this crate is edge tooling outside the deterministic core —
//! real clocks, threads, sockets and processes here are not determinism
//! violations; DESIGN §1's fence is the core, not this crate.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use charpente::http::ApiError;
use charpente::{event, job, jobs, route, routes, table};
use charpente::{App, AppConfig, Disk, Entropy, Id, JobError, Schema, Tx};

/// Row-id space for the canonical workload: bounded so the store's working
/// set stays cache-resident (the spec measures the commit path, not paging).
pub const SLOTS: u64 = 1024;

#[table]
pub struct Slot {
    pub id: Id<Slot>,
    pub hits: u64,
}

#[event]
pub struct SlotBumped {
    pub slot: u64,
}

#[job]
pub struct Bump {
    pub slot: u64,
}

impl Bump {
    /// The canonical no-op state transition: the handler writes nothing; its
    /// Done-mark still commits one log record (spec §3 — that is the point).
    pub fn run(self, _tx: &mut Tx) -> Result<(), JobError> {
        Ok(())
    }
}

/// The canonical write transaction, README-shaped: get + put + enqueue + emit.
#[route(POST "/hit/:id")]
pub fn hit(tx: &mut Tx, id: u64) -> Result<u64, ApiError> {
    let slot_id = Id::<Slot>::from_raw(id % SLOTS);
    let hits = tx.get(slot_id)?.map_or(0, |s| s.hits) + 1;
    tx.put(&Slot { id: slot_id, hits });
    tx.enqueue(Bump { slot: id % SLOTS });
    tx.emit(SlotBumped { slot: id % SLOTS });
    Ok(hits)
}

#[route(GET "/slot/:id")]
pub fn read_slot(tx: &mut Tx, id: u64) -> Result<u64, ApiError> {
    Ok(tx.get(Id::<Slot>::from_raw(id % SLOTS))?.map_or(0, |s| s.hits))
}

/// The invoicing idiom (examples/invoicing/src/lib.rs:19): one free function
/// shared by the App builder and any bare-`Db` consumer (the core layer).
pub fn schema() -> Schema {
    Schema::new().table::<Slot>()
}

pub fn build(disk: Box<dyn Disk>, entropy: Box<dyn Entropy>) -> std::io::Result<App> {
    Ok(App::open(disk, schema(), entropy, AppConfig::default())?
        .mount(routes![hit, read_slot])
        .mount_jobs(jobs![Bump]))
}
```

If rustc rejects any `use` path, fix per the compiler (Global Constraints) — e.g. `ApiError` may live at the root, `Disk`/`Entropy` may need `charpente::core::io`.

- [ ] **Step 5: Run the test, verify it passes**

Run: `cargo test -p bench-harness --test app`
Expected: PASS (1 test).

- [ ] **Step 6: Create the ledger and commit**

Create `.superpowers/sdd/2026-08-06-benchmark-harness/progress.md` with a dated Task 1 entry (crate scaffolded, sim proof green, seed 0xBE9C_0001). Then — first run `git diff Cargo.toml` and confirm the diff contains ONLY the one members line; anything else is another session's uncommitted work (this tree is shared — CLAUDE.md workflow): stop and ask, never stage it blindly. Then:

```bash
git add Cargo.toml crates/bench-harness
git commit -m "bench: scaffold bench-harness with the canonical app + sim proof (T1)"
```

(The ledger directory is gitignored — do not `git add` it.)

---

### Task 2: `measure.rs` — CPU time, heartbeat sleep-guard, percentiles, totals cross-check

> **HISTORICAL — this task is shipped, and its code blocks below are the pre-review
> versions. The committed source is authoritative.** Task 4's review (I-5) reopened this
> file: the sleep-guard is now two arms, `suspension_verdict` and `heartbeat_verdict`,
> composed by `verdict` (T4 re-review, M-D). In particular the test block below asserts
> `contains("suspended")` on a *starved* heartbeat — that reason string now reads
> "starved", and the shipped test asserts accordingly.

**Files:**
- Create: `crates/bench-harness/src/measure.rs`
- Modify: `crates/bench-harness/src/lib.rs` (add `pub mod measure;`)

**Interfaces:**
- Consumes: nothing from other tasks.
- Produces: `measure::process_cpu() -> Duration`; `measure::Heartbeat` (`start() -> Heartbeat`, `gate() -> Arc<Mutex<()>>`, `finish(self) -> Option<String>`); `measure::heartbeat_verdict(wall: Duration, interval: Duration, observed: u64) -> Option<String>`; `measure::percentile(sorted: &[u64], p: f64) -> u64` (values are latency micros); `measure::crosscheck(per_thread: &[u64], grand: u64) -> Result<u64, String>`.

- [ ] **Step 1: Write the failing tests** (in `measure.rs`'s `#[cfg(test)] mod tests`)

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    #[test]
    fn percentiles_pick_the_documented_ranks() {
        let sorted: Vec<u64> = (1..=100).collect();
        assert_eq!(percentile(&sorted, 0.0), 1);
        assert_eq!(percentile(&sorted, 50.0), 50);
        assert_eq!(percentile(&sorted, 99.0), 99);
        assert_eq!(percentile(&sorted, 100.0), 100);
        assert_eq!(percentile(&[7], 99.9), 7);
    }

    #[test]
    #[should_panic(expected = "percentile input must be sorted")]
    fn percentile_refuses_unsorted_input() {
        percentile(&[3, 1], 50.0);
    }

    #[test]
    fn crosscheck_accepts_agreement_and_names_disagreement() {
        assert_eq!(crosscheck(&[2, 3], 5), Ok(5));
        let err = crosscheck(&[2, 2], 5).unwrap_err();
        assert!(err.contains("totals disagree"), "got: {err}");
        assert!(err.contains("INVALID"), "got: {err}");
    }

    #[test]
    fn heartbeat_verdict_flags_a_starved_run_and_passes_a_healthy_one() {
        let interval = Duration::from_millis(10);
        assert!(heartbeat_verdict(Duration::from_secs(30), interval, 2900).is_none());
        let v = heartbeat_verdict(Duration::from_secs(30), interval, 100).expect("starved");
        assert!(v.contains("INVALID"), "got: {v}");
        assert!(v.contains("suspended"), "got: {v}");
    }

    #[test]
    fn process_cpu_advances_under_load() {
        let before = process_cpu();
        let spin_until = Instant::now() + Duration::from_millis(50);
        let mut x = 0u64;
        while Instant::now() < spin_until {
            x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        }
        std::hint::black_box(x);
        let delta = process_cpu() - before;
        assert!(delta >= Duration::from_millis(20), "cpu delta: {delta:?}");
    }

    #[test]
    fn live_heartbeat_ticks_when_healthy() {
        let hb = Heartbeat::start();
        std::thread::sleep(Duration::from_millis(300));
        assert!(hb.finish().is_none(), "healthy heartbeat must not flag");
    }
}
```

- [ ] **Step 2: Run tests, verify they fail**

Run: `cargo test -p bench-harness measure`
Expected: FAIL to compile — module does not exist.

- [ ] **Step 3: Implement**

`crates/bench-harness/src/measure.rs`:

```rust
//! Measurement discipline (spec §5): process CPU time, the heartbeat
//! sleep-guard (laptops sleep mid-run and wall clocks cannot tell — the
//! charpente measurement-method rule), percentiles, totals-two-ways.
//! Edge tooling outside the deterministic core (DESIGN §1).
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[repr(C)]
struct Timeval {
    tv_sec: i64,
    tv_usec: i64,
}

/// ru_utime + ru_stime lead on both macOS and 64-bit Linux; the 14 trailing
/// c_longs differ in meaning but not layout. RUSAGE_SELF is 0 on both.
#[repr(C)]
struct Rusage {
    ru_utime: Timeval,
    ru_stime: Timeval,
    pad: [i64; 14],
}

unsafe extern "C" {
    fn getrusage(who: i32, usage: *mut Rusage) -> i32;
}

/// User + system CPU time consumed by this process so far.
pub fn process_cpu() -> Duration {
    let mut ru = Rusage {
        ru_utime: Timeval { tv_sec: 0, tv_usec: 0 },
        ru_stime: Timeval { tv_sec: 0, tv_usec: 0 },
        pad: [0; 14],
    };
    let rc = unsafe { getrusage(0, &mut ru) };
    assert_eq!(rc, 0, "getrusage(RUSAGE_SELF) cannot fail with valid arguments");
    Duration::from_secs((ru.ru_utime.tv_sec + ru.ru_stime.tv_sec) as u64)
        + Duration::from_micros((ru.ru_utime.tv_usec + ru.ru_stime.tv_usec) as u64)
}

const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(10);

/// A ticking thread: if the process is suspended (laptop sleep), ticks stop
/// while wall time inflates — `finish` then marks the run INVALID. The tick
/// thread takes `gate` on every tick; holding it is the test seam that
/// starves the heartbeat through its real path.
pub struct Heartbeat {
    ticks: Arc<AtomicU64>,
    stop: Arc<AtomicBool>,
    gate: Arc<Mutex<()>>,
    started: Instant,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Heartbeat {
    pub fn start() -> Heartbeat {
        let ticks = Arc::new(AtomicU64::new(0));
        let stop = Arc::new(AtomicBool::new(false));
        let gate = Arc::new(Mutex::new(()));
        let (t, s, g) = (ticks.clone(), stop.clone(), gate.clone());
        let thread = std::thread::spawn(move || {
            while !s.load(Ordering::Relaxed) {
                std::thread::sleep(HEARTBEAT_INTERVAL);
                drop(g.lock().expect("heartbeat gate poisoned"));
                t.fetch_add(1, Ordering::Relaxed);
            }
        });
        Heartbeat { ticks, stop, gate, started: Instant::now(), thread: Some(thread) }
    }

    pub fn gate(&self) -> Arc<Mutex<()>> {
        self.gate.clone()
    }

    /// None = run valid; Some(reason) = INVALID.
    pub fn finish(mut self) -> Option<String> {
        self.stop.store(true, Ordering::Relaxed);
        let wall = self.started.elapsed();
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
        heartbeat_verdict(wall, HEARTBEAT_INTERVAL, self.ticks.load(Ordering::Relaxed))
    }
}

pub fn heartbeat_verdict(wall: Duration, interval: Duration, observed: u64) -> Option<String> {
    let expected = (wall.as_nanos() / interval.as_nanos()).max(1) as u64;
    if observed * 2 < expected {
        Some(format!(
            "INVALID: heartbeat observed {observed} ticks of ~{expected} expected over \
             {wall:?} — the process was suspended mid-run (laptop sleep?); \
             wall-clock rates from this run are meaningless"
        ))
    } else {
        None
    }
}

/// Nearest-rank percentile over an already-sorted slice (latency micros).
pub fn percentile(sorted: &[u64], p: f64) -> u64 {
    assert!(!sorted.is_empty(), "percentile of an empty set is meaningless");
    assert!((0.0..=100.0).contains(&p), "percentile out of range: {p}");
    assert!(
        sorted.windows(2).all(|w| w[0] <= w[1]),
        "percentile input must be sorted"
    );
    let rank = ((p / 100.0) * (sorted.len() - 1) as f64).round() as usize;
    sorted[rank]
}

/// Totals-two-ways (spec §5): per-thread counters vs the grand counter.
/// Disagreement is a harness bug, and the run that produced it is INVALID.
pub fn crosscheck(per_thread: &[u64], grand: u64) -> Result<u64, String> {
    let sum: u64 = per_thread.iter().sum();
    if sum == grand {
        Ok(sum)
    } else {
        Err(format!(
            "INVALID: totals disagree — per-thread sum {sum} vs grand counter {grand}; \
             this is a harness bug, not a measurement"
        ))
    }
}
```

Add `pub mod measure;` to `lib.rs`.

- [ ] **Step 4: Run tests, verify they pass**

Run: `cargo test -p bench-harness measure`
Expected: PASS (6 tests).

- [ ] **Step 5: Ledger + commit**

Append the Task 2 entry to the ledger, then:

```bash
git add crates/bench-harness
git commit -m "bench: measurement discipline — cpu time, heartbeat sleep-guard, percentiles, totals cross-check (T2)"
```

---

### Task 3: `report.rs` — the report model, renderers, shape pin, ceiling refusal

**Files:**
- Create: `crates/bench-harness/src/report.rs`
- Modify: `crates/bench-harness/src/lib.rs` (add `pub mod report;`)

**Interfaces:**
- Consumes: nothing from other tasks.
- Produces:

```rust
pub struct Report {
    pub hardware: String,
    pub os: String,
    pub filesystem: String,
    pub cores: usize,
    pub sync_semantics: &'static str,
    pub warmup_secs: f64,
    pub duration_secs: f64,
    pub device: Option<DeviceReport>,
    pub core: Option<CoreReport>,
    pub kernel: Option<KernelReport>,
    pub wire: Vec<WireReport>,
    pub invalid: Vec<String>,
}
pub struct DeviceReport { pub syncs_per_sec: f64, pub appends_per_sec: f64 }
pub struct CoreReport {
    pub acked_writes_per_sec: f64,
    pub log_records_per_sec: f64,
    pub reads_per_sec: f64,
    pub device_ratio: f64,
}
pub struct KernelReport {
    pub acked_writes_per_sec: f64,
    pub log_records_per_sec: f64,
    pub requests_total: u64,
    pub jobs_done: u64,
}
pub struct WireReport { pub target: String, pub points: Vec<SweepPoint> }
pub struct SweepPoint {
    pub connections: usize,
    pub throughput_per_sec: f64,
    pub log_records_per_sec: f64,
    pub p50_us: u64,
    pub p99_us: u64,
    pub p999_us: u64,
    pub client_busy_fraction: f64,
    pub saturated: bool,
}
pub fn render_json(r: &Report) -> Result<String, String>
pub fn render_human(r: &Report) -> String       // never refuses; used for stderr progress
pub const CEILING_REFUSAL: &str = "a number without its device ceiling is not publishable output — run the device layer in the same invocation (spec §5)";
```

- [ ] **Step 1: Write the failing tests** (`#[cfg(test)] mod tests` in `report.rs`)

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn fixed_report() -> Report {
        Report {
            hardware: "Test \"Box\" 1".to_string(),
            os: "TestOS 1.0 arm64".to_string(),
            filesystem: "TestFS".to_string(),
            cores: 8,
            sync_semantics: "F_FULLFSYNC via sync_all",
            warmup_secs: 5.0,
            duration_secs: 30.0,
            device: Some(DeviceReport { syncs_per_sec: 1500.25, appends_per_sec: 1400.5 }),
            core: Some(CoreReport {
                acked_writes_per_sec: 1450.0,
                log_records_per_sec: 1450.0,
                reads_per_sec: 800000.0,
                device_ratio: 0.97,
            }),
            kernel: Some(KernelReport {
                acked_writes_per_sec: 1300.0,
                log_records_per_sec: 2600.0,
                requests_total: 50000,
                jobs_done: 50000,
            }),
            wire: vec![WireReport {
                target: "bench-app".to_string(),
                points: vec![SweepPoint {
                    connections: 4,
                    throughput_per_sec: 1200.0,
                    log_records_per_sec: 2400.0,
                    p50_us: 800,
                    p99_us: 2100,
                    p999_us: 5000,
                    client_busy_fraction: 0.12,
                    saturated: false,
                }],
            }],
            invalid: vec![],
        }
    }

    #[test]
    fn json_shape_is_pinned() {
        let json = render_json(&fixed_report()).expect("renders");
        assert_eq!(
            json,
            "{\"bench\":{\"hardware\":\"Test \\\"Box\\\" 1\",\"os\":\"TestOS 1.0 arm64\",\
             \"filesystem\":\"TestFS\",\"cores\":8,\
             \"sync_semantics\":\"F_FULLFSYNC via sync_all\",\"warmup_secs\":5.00,\
             \"duration_secs\":30.00,\"device\":{\"syncs_per_sec\":1500.25,\
             \"appends_per_sec\":1400.50},\"core\":{\"acked_writes_per_sec\":1450.00,\
             \"log_records_per_sec\":1450.00,\"reads_per_sec\":800000.00,\
             \"device_ratio\":0.970},\"kernel\":{\"acked_writes_per_sec\":1300.00,\
             \"log_records_per_sec\":2600.00,\"requests_total\":50000,\"jobs_done\":50000},\
             \"wire\":[{\"target\":\"bench-app\",\"points\":[{\"connections\":4,\
             \"throughput_per_sec\":1200.00,\"log_records_per_sec\":2400.00,\
             \"p50_us\":800,\"p99_us\":2100,\"p999_us\":5000,\
             \"client_busy_fraction\":0.120,\"saturated\":false}]}],\"invalid\":[]}}"
        );
    }

    #[test]
    fn a_layer_without_the_device_ceiling_refuses_to_render() {
        let mut r = fixed_report();
        r.device = None;
        let err = render_json(&r).unwrap_err();
        assert_eq!(err, CEILING_REFUSAL);
        r.core = None;
        r.kernel = None;
        r.wire.clear();
        assert!(render_json(&r).is_ok(), "a device-less report with no layers is fine");
    }

    #[test]
    fn invalid_reasons_survive_into_the_artifact() {
        let mut r = fixed_report();
        r.invalid.push("INVALID: heartbeat observed 1 ticks".to_string());
        let json = render_json(&r).expect("renders");
        assert!(json.contains("\"invalid\":[\"INVALID: heartbeat observed 1 ticks\"]"));
        let human = render_human(&r);
        assert!(human.contains("INVALID"), "human render must shout invalidity");
    }
}
```

- [ ] **Step 2: Run tests, verify they fail**

Run: `cargo test -p bench-harness report`
Expected: FAIL to compile.

- [ ] **Step 3: Implement**

`crates/bench-harness/src/report.rs` — the structs from Interfaces verbatim, plus:

```rust
//! The report is the artifact (spec §5-§6): every honesty-contract field is a
//! field here, not prose elsewhere. JSON is hand-rolled (no deps) and its
//! shape is pinned byte-exact by test, the sim-harness discipline.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

pub const CEILING_REFUSAL: &str = "a number without its device ceiling is not publishable \
output — run the device layer in the same invocation (spec §5)";

fn esc(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out
}

pub fn render_json(r: &Report) -> Result<String, String> {
    let has_layers = r.core.is_some() || r.kernel.is_some() || !r.wire.is_empty();
    if has_layers && r.device.is_none() {
        return Err(CEILING_REFUSAL.to_string());
    }
    let mut s = String::new();
    s.push_str(&format!(
        "{{\"bench\":{{\"hardware\":\"{}\",\"os\":\"{}\",\"filesystem\":\"{}\",\"cores\":{},\
         \"sync_semantics\":\"{}\",\"warmup_secs\":{:.2},\"duration_secs\":{:.2}",
        esc(&r.hardware), esc(&r.os), esc(&r.filesystem), r.cores, esc(r.sync_semantics),
        r.warmup_secs, r.duration_secs
    ));
    if let Some(d) = &r.device {
        s.push_str(&format!(
            ",\"device\":{{\"syncs_per_sec\":{:.2},\"appends_per_sec\":{:.2}}}",
            d.syncs_per_sec, d.appends_per_sec
        ));
    }
    if let Some(c) = &r.core {
        s.push_str(&format!(
            ",\"core\":{{\"acked_writes_per_sec\":{:.2},\"log_records_per_sec\":{:.2},\
             \"reads_per_sec\":{:.2},\"device_ratio\":{:.3}}}",
            c.acked_writes_per_sec, c.log_records_per_sec, c.reads_per_sec, c.device_ratio
        ));
    }
    if let Some(k) = &r.kernel {
        s.push_str(&format!(
            ",\"kernel\":{{\"acked_writes_per_sec\":{:.2},\"log_records_per_sec\":{:.2},\
             \"requests_total\":{},\"jobs_done\":{}}}",
            k.acked_writes_per_sec, k.log_records_per_sec, k.requests_total, k.jobs_done
        ));
    }
    if !r.wire.is_empty() {
        s.push_str(",\"wire\":[");
        for (wi, w) in r.wire.iter().enumerate() {
            if wi > 0 { s.push(','); }
            s.push_str(&format!("{{\"target\":\"{}\",\"points\":[", esc(&w.target)));
            for (pi, p) in w.points.iter().enumerate() {
                if pi > 0 { s.push(','); }
                s.push_str(&format!(
                    "{{\"connections\":{},\"throughput_per_sec\":{:.2},\
                     \"log_records_per_sec\":{:.2},\"p50_us\":{},\
                     \"p99_us\":{},\"p999_us\":{},\"client_busy_fraction\":{:.3},\
                     \"saturated\":{}}}",
                    p.connections, p.throughput_per_sec, p.log_records_per_sec,
                    p.p50_us, p.p99_us, p.p999_us,
                    p.client_busy_fraction, p.saturated
                ));
            }
            s.push_str("]}");
        }
        s.push(']');
    }
    s.push_str(",\"invalid\":[");
    for (i, reason) in r.invalid.iter().enumerate() {
        if i > 0 { s.push(','); }
        s.push_str(&format!("\"{}\"", esc(reason)));
    }
    s.push_str("]}}");
    Ok(s)
}

pub fn render_human(r: &Report) -> String {
    let mut s = String::new();
    s.push_str(&format!(
        "bench: {} | {} | {} | {} cores | sync: {} | warmup {:.0}s, measure {:.0}s\n",
        r.hardware, r.os, r.filesystem, r.cores, r.sync_semantics, r.warmup_secs,
        r.duration_secs
    ));
    if let Some(d) = &r.device {
        s.push_str(&format!(
            "  device: {:.0} syncs/s, {:.0} appends/s (the ceiling)\n",
            d.syncs_per_sec, d.appends_per_sec
        ));
    }
    if let Some(c) = &r.core {
        s.push_str(&format!(
            "  core:   {:.0} acked writes/s ({:.2}x device), {:.0} log records/s, {:.0} reads/s\n",
            c.acked_writes_per_sec, c.device_ratio, c.log_records_per_sec, c.reads_per_sec
        ));
    }
    if let Some(k) = &r.kernel {
        s.push_str(&format!(
            "  kernel: {:.0} acked writes/s, {:.0} log records/s ({} requests, {} jobs)\n",
            k.acked_writes_per_sec, k.log_records_per_sec, k.requests_total, k.jobs_done
        ));
    }
    for w in &r.wire {
        s.push_str(&format!("  wire [{}]:\n", w.target));
        for p in &w.points {
            s.push_str(&format!(
                "    {:>4} conns: {:>8.0} req/s  {:>8.0} rec/s  p50 {:>6}us  p99 {:>6}us  \
                 p99.9 {:>6}us  client busy {:.0}%{}\n",
                p.connections, p.throughput_per_sec, p.log_records_per_sec, p.p50_us,
                p.p99_us, p.p999_us,
                p.client_busy_fraction * 100.0,
                if p.saturated { "  CLIENT-SATURATED (point invalid)" } else { "" }
            ));
        }
    }
    for reason in &r.invalid {
        s.push_str(&format!("  !! {reason}\n"));
    }
    s
}
```

Add `pub mod report;` to `lib.rs`.

- [ ] **Step 4: Run tests, verify they pass**

Run: `cargo test -p bench-harness report`
Expected: PASS (3 tests). If the shape-pin string disagrees on float formatting, fix the *implementation* to match the pin (the pin is the spec's shape), never loosen the pin to `contains`.

- [ ] **Step 5: Ledger + commit**

Append the Task 3 entry to the ledger, then:

```bash
git add crates/bench-harness
git commit -m "bench: report model, pinned JSON shape, the ceiling refusal (T3)"
```

---

### Task 4: `device.rs` + the `bench` binary skeleton

> **HISTORICAL — this task is shipped, and its code blocks below are the pre-review
> versions. The committed source is authoritative.** Two review rounds changed it
> substantially: `--dir` plus the RAM-backed and undeterminable-filesystem guards, the
> zero-op refusal, `invalid_reasons` as the single exit gate, the split suspension/
> starvation arms, and the `temp_dir`/`filesystem`/`base_report`/`run_layers` signature
> changes recorded under **Interfaces** above. Do not copy `filesystem()`, `base_report`,
> `temp_dir` or `run_layers` from the blocks below.

**Files:**
- Create: `crates/bench-harness/src/device.rs`
- Create: `crates/bench-harness/src/bin/bench.rs`
- Modify: `crates/bench-harness/src/lib.rs` (add `pub mod device;`)
- Test: `crates/bench-harness/tests/harness.rs` (new; `#[ignore]`d smokes accumulate here)

**Interfaces:**
- Consumes: `charpente::prod::ProdDisk`; `measure::Heartbeat`; `report::*`.
- Produces: `device::run(dir: &str, warmup: Duration, duration: Duration) -> io::Result<DeviceReport>`; the `bench` binary with `device` subcommand, `--json`, `--duration S`, `--warmup S` flags; `bin/bench.rs` helper fns later subcommands extend: `fn temp_dir(tag: &str) -> String`, `fn hardware() -> String`, `fn os() -> String`, `fn filesystem() -> String`, `fn sync_semantics() -> &'static str`, `fn base_report(warmup: f64, duration: f64) -> Report`.

> **AMENDED at Task 4's review and re-review (2026-08-07) — three of those
> signatures are stale as written above; the shipped ones are authoritative
> (re-review finding I-A).** `fn temp_dir(tag: &str, base: Option<&str>) ->
> Result<String, String>`, `fn filesystem(dir: &str) -> String`, and
> `fn base_report(warmup: f64, duration: f64, dir: &str) -> Report`. All three
> follow from I-1: the ceiling must be measured on — and labelled with — the
> directory the operator actually named, so the measured dir has to reach the
> probe and the report; `temp_dir`'s `Result` is M-A/M-I (a bad `--dir` is a
> usage error, not a panic, and is never created). `run_layers` also changed:
> `(args, report) -> io::Result<()>` became `(args, dir, report) ->
> io::Result<bool>`. Call sites in later tasks must be adapted, not reverted —
> see the warning in Task 9 Step 3.

- [ ] **Step 1: Implement `device.rs`**

```rust
//! The hardware ceiling (spec §2): durable-sync rate through the production
//! Disk impl. Every other layer's number is published against this one.
//! Edge tooling outside the deterministic core (DESIGN §1).
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use crate::report::DeviceReport;
use charpente::prod::ProdDisk;
use charpente::Disk;
use std::io;
use std::time::{Duration, Instant};

fn timed_loop(
    warmup: Duration,
    duration: Duration,
    mut op: impl FnMut() -> io::Result<()>,
) -> io::Result<f64> {
    let warm_end = Instant::now() + warmup;
    while Instant::now() < warm_end {
        op()?;
    }
    let start = Instant::now();
    let end = start + duration;
    let mut ops = 0u64;
    while Instant::now() < end {
        op()?;
        ops += 1;
    }
    Ok(ops as f64 / start.elapsed().as_secs_f64())
}

pub fn run(dir: &str, warmup: Duration, duration: Duration) -> io::Result<DeviceReport> {
    let disk = ProdDisk::new(dir)?;
    // Dirty-byte sync: minimal dirty state, repeated durable sync — the pure
    // sync rate. (A clean-file sync could be elided by the OS; one byte keeps
    // it honest.)
    let file = disk.open("bench-sync.dat")?;
    let syncs_per_sec = timed_loop(warmup, duration, || {
        file.write_at(0, &[1u8])?;
        file.sync()
    })?;
    // Append-shaped: write_at the tail + sync, the WAL's op shape.
    let file = disk.open("bench-append.dat")?;
    let mut off = 0u64;
    let frame = [0u8; 256];
    let appends_per_sec = timed_loop(warmup, duration, || {
        file.write_at(off, &frame)?;
        off += frame.len() as u64;
        file.sync()
    })?;
    Ok(DeviceReport { syncs_per_sec, appends_per_sec })
}
```

Add `pub mod device;` to `lib.rs`.

- [ ] **Step 2: Implement the binary skeleton**

`crates/bench-harness/src/bin/bench.rs`:

```rust
//! The bench binary (spec §1): device/core/kernel/wire/all/smoke subcommands.
//! Human render → stderr (always); pinned JSON → stdout (with --json).
//! Exit 0 = valid run; 1 = INVALID (or a layer error); 2 = usage.
//! Edge tooling outside the deterministic core (DESIGN §1).
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use bench_harness::measure::Heartbeat;
use bench_harness::report::{render_human, render_json, Report};
use bench_harness::device;
use std::process::ExitCode;
use std::time::Duration;

const USAGE: &str = "usage: bench <device|core|kernel|wire|all|smoke> \
[--json] [--duration SECS] [--warmup SECS] [--requests N] \
[--target bench-app|invoicing] [--connections N,N,...]";

struct Args {
    cmd: String,
    json: bool,
    duration: Duration,
    warmup: Duration,
    requests: u64,
    target: String,
    connections: Vec<usize>,
}

fn parse(argv: &[String]) -> Result<Args, String> {
    let mut args = Args {
        cmd: argv.first().cloned().ok_or(USAGE)?,
        json: false,
        duration: Duration::from_secs(30),
        warmup: Duration::from_secs(5),
        requests: 50_000,
        target: "bench-app".to_string(),
        connections: vec![1, 4, 16, 64, 256],
    };
    if !["device", "core", "kernel", "wire", "all", "smoke"].contains(&args.cmd.as_str()) {
        return Err(format!("unknown subcommand {:?}\n{USAGE}", args.cmd));
    }
    let mut it = argv[1..].iter();
    while let Some(flag) = it.next() {
        let mut val = |name: &str| -> Result<String, String> {
            it.next().cloned().ok_or(format!("{name} needs a value\n{USAGE}"))
        };
        match flag.as_str() {
            "--json" => args.json = true,
            "--duration" => {
                args.duration = Duration::from_secs_f64(
                    val("--duration")?.parse().map_err(|e| format!("--duration: {e}"))?,
                )
            }
            "--warmup" => {
                args.warmup = Duration::from_secs_f64(
                    val("--warmup")?.parse().map_err(|e| format!("--warmup: {e}"))?,
                )
            }
            "--requests" => {
                args.requests = val("--requests")?.parse().map_err(|e| format!("--requests: {e}"))?
            }
            "--target" => args.target = val("--target")?,
            "--connections" => {
                args.connections = val("--connections")?
                    .split(',')
                    .map(|s| s.parse().map_err(|e| format!("--connections: {e}")))
                    .collect::<Result<_, _>>()?
            }
            other => return Err(format!("unknown flag {other:?}\n{USAGE}")),
        }
    }
    Ok(args)
}

fn temp_dir(tag: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock after 1970")
        .as_nanos();
    let dir = std::env::temp_dir().join(format!("bench-harness-{tag}-{}-{nanos}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("scratch dir creates");
    dir.to_string_lossy().into_owned()
}

fn sh(cmd: &str, args: &[&str]) -> Option<String> {
    let out = std::process::Command::new(cmd).args(args).output().ok()?;
    out.status.success().then(|| String::from_utf8_lossy(&out.stdout).trim().to_string())
}

fn hardware() -> String {
    if cfg!(target_os = "macos") {
        sh("sysctl", &["-n", "machdep.cpu.brand_string"])
    } else {
        std::fs::read_to_string("/proc/cpuinfo").ok().and_then(|s| {
            s.lines()
                .find(|l| l.starts_with("model name"))
                .and_then(|l| l.split(':').nth(1))
                .map(|v| v.trim().to_string())
        })
    }
    .unwrap_or_else(|| std::env::consts::ARCH.to_string())
}

fn os() -> String {
    sh("uname", &["-srm"]).unwrap_or_else(|| std::env::consts::OS.to_string())
}

/// The filesystem under the bench scratch dirs (spec §5 names it: sync
/// semantics are an OS/filesystem pair — F_FULLFSYNC's cost is APFS-on-macOS
/// truth, not a portable constant). Both bench dirs live under temp_dir(),
/// which sits on the root volume group on macOS.
fn filesystem() -> String {
    if cfg!(target_os = "macos") {
        // `mount` line for /: "/dev/disk3s1s1 on / (apfs, sealed, ...)".
        sh("mount", &[]).and_then(|out| {
            out.lines()
                .find(|l| l.contains(" on / ("))
                .and_then(|l| l.split('(').nth(1))
                .and_then(|rest| rest.split([',', ')']).next())
                .map(|t| t.trim().to_string())
        })
    } else {
        // POSIX df has no -T on macOS (there it filters by type); on Linux
        // `df -PT` prints "Filesystem Type ..." with the type in column 2.
        sh("df", &["-PT", "/tmp"]).and_then(|out| {
            out.lines()
                .nth(1)
                .and_then(|l| l.split_whitespace().nth(1))
                .map(|s| s.to_string())
        })
    }
    .unwrap_or_else(|| "unknown".to_string())
}

fn sync_semantics() -> &'static str {
    if cfg!(target_os = "macos") {
        "F_FULLFSYNC via sync_all"
    } else {
        "fsync via sync_all"
    }
}

fn base_report(warmup: f64, duration: f64) -> Report {
    Report {
        hardware: hardware(),
        os: os(),
        filesystem: filesystem(),
        cores: std::thread::available_parallelism().map_or(0, |n| n.get()),
        sync_semantics: sync_semantics(),
        warmup_secs: warmup,
        duration_secs: duration,
        device: None,
        core: None,
        kernel: None,
        wire: Vec::new(),
        invalid: Vec::new(),
    }
}

fn main() -> ExitCode {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let args = match parse(&argv) {
        Ok(a) => a,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::from(2);
        }
    };
    let mut report = base_report(args.warmup.as_secs_f64(), args.duration.as_secs_f64());
    let outcome = run_layers(&args, &mut report);
    if let Err(e) = outcome {
        eprintln!("bench: layer failed: {e}");
        return ExitCode::from(1);
    }
    eprint!("{}", render_human(&report));
    if args.json {
        match render_json(&report) {
            Ok(json) => println!("{json}"),
            Err(refusal) => {
                eprintln!("bench: {refusal}");
                return ExitCode::from(1);
            }
        }
    }
    if report.invalid.is_empty() {
        ExitCode::SUCCESS
    } else {
        ExitCode::from(1)
    }
}

fn run_layers(args: &Args, report: &mut Report) -> std::io::Result<()> {
    // Every subcommand carries the ceiling (spec §5): device runs first
    // unconditionally, so no layer can render without it.
    let hb = Heartbeat::start();
    let dir = temp_dir("device");
    report.device = Some(device::run(&dir, args.warmup, args.duration)?);
    let _ = std::fs::remove_dir_all(&dir);
    if let Some(reason) = hb.finish() {
        report.invalid.push(reason);
    }
    // Later tasks extend this match with core/kernel/wire/all/smoke arms.
    match args.cmd.as_str() {
        "device" => {}
        other => eprintln!("bench: subcommand {other:?} not implemented yet"),
    }
    Ok(())
}
```

- [ ] **Step 3: Add the ignored device smoke**

`crates/bench-harness/tests/harness.rs`:

```rust
//! Syscall-touching smokes for the bench harness — every test here is
//! #[ignore] (edge-harness convention) and runs via `scripts/bench.sh --smoke`.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use std::time::Duration;

fn scratch(tag: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock after 1970")
        .as_nanos();
    let dir = std::env::temp_dir().join(format!("bench-smoke-{tag}-{}-{nanos}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("scratch dir creates");
    dir.to_string_lossy().into_owned()
}

#[test]
#[ignore]
fn device_layer_produces_nonzero_rates() {
    let dir = scratch("device");
    let d = bench_harness::device::run(&dir, Duration::from_millis(200), Duration::from_secs(1))
        .expect("device layer runs");
    let _ = std::fs::remove_dir_all(&dir);
    assert!(d.syncs_per_sec > 0.0, "syncs_per_sec: {}", d.syncs_per_sec);
    assert!(d.appends_per_sec > 0.0, "appends_per_sec: {}", d.appends_per_sec);
}
```

- [ ] **Step 4: Verify**

Run: `cargo test -p bench-harness --test harness -- --ignored` → PASS (1 test).
Run: `cargo run -p bench-harness --bin bench -- device --duration 1 --warmup 0.2 --json` → human summary on stderr, one-line JSON on stdout, exit 0.
Run: `cargo run -p bench-harness --bin bench -- bogus` → usage on stderr, exit 2.

- [ ] **Step 5: Ledger + commit**

Append the Task 4 entry to the ledger, then:

```bash
git add crates/bench-harness
git commit -m "bench: device layer (the ceiling) + the bench binary skeleton (T4)"
```

---

### Task 5: `core.rs` — the commit path alone

**Files:**
- Create: `crates/bench-harness/src/core_bench.rs`
- Modify: `crates/bench-harness/src/lib.rs` (add `pub mod core_bench;`)
- Modify: `crates/bench-harness/src/bin/bench.rs` (add the `core` arm)
- Modify: `crates/bench-harness/tests/harness.rs` (add smoke)

**Interfaces:**
- Consumes: `bench_harness::{schema, Slot, Bump, SlotBumped, SLOTS}`; `charpente::{Db, DbConfig, Id, TxError, View}` (**corrected at implementation, 2026-08-08** — these four are TOP-LEVEL re-exports; the original `charpente::core::{...}` does not compile, see the Step 1 note); `charpente::prod::ProdDisk`; `measure::{Heartbeat, crosscheck}`; `report::CoreReport`.
- Produces: `core_bench::run(dir: &str, warmup: Duration, duration: Duration, device_syncs_per_sec: f64) -> io::Result<(CoreReport, Vec<String>)>` — the `Vec<String>` is invalidity reasons for `report.invalid`.

- [ ] **Step 1: Implement**

> **CORRECTED at implementation (2026-08-08).** The import block below originally read
> `use charpente::core::{Db, DbConfig, TxError, View};`, which does not compile. `Db`,
> `DbConfig`, `TxError`, `Id`, `Snapshot` and `View` are re-exported at the **top level**
> of the `charpente` crate (`crates/charpente/src/lib.rs:9-11`); `charpente_core` declares
> `pub mod db;`/`pub mod table;` with no crate-root re-exports of its own, so
> `charpente::core::Db` would require `charpente_core::Db`, which does not exist.
> `charpente::core::` **is** the right prefix for `Disk`/`Entropy`/`migration`/
> `table::Codec` — which is presumably why the original block generalized it. Every
> existing committed caller (the charpente test suites, `examples/invoicing`) imports
> these as `charpente::{...}`. The block below is the corrected, as-shipped form.

```rust
//! Layer 2 (spec §2): Db::transact in a closed loop against the prod Disk —
//! the commit path alone (WAL append + sync + apply + store materialization).
//! The spec's Context prediction is about this module's number.
//! Edge tooling outside the deterministic core (DESIGN §1).
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use crate::measure::{crosscheck, Heartbeat};
use crate::report::CoreReport;
use crate::{schema, Bump, Slot, SlotBumped, SLOTS};
use charpente::prod::ProdDisk;
use charpente::{Db, DbConfig, Id, TxError, View};
use std::io;
use std::time::{Duration, Instant};

fn canonical(db: &mut Db, i: u64) -> io::Result<u64> {
    db.transact(|tx| -> Result<u64, io::Error> {
        let slot_id = Id::<Slot>::from_raw(i % SLOTS);
        let hits = tx.get(slot_id)?.map_or(0, |s| s.hits) + 1;
        tx.put(&Slot { id: slot_id, hits });
        tx.enqueue(Bump { slot: i % SLOTS });
        tx.emit(SlotBumped { slot: i % SLOTS });
        Ok(hits)
    })
    .map_err(|e| match e {
        TxError::Io(io) => io,
        TxError::App(io) => io,
        other => io::Error::other(format!("unexpected transact refusal: {other:?}")),
    })
}

pub fn run(
    dir: &str,
    warmup: Duration,
    duration: Duration,
    device_syncs_per_sec: f64,
) -> io::Result<(CoreReport, Vec<String>)> {
    let mut db = Db::open(Box::new(ProdDisk::new(dir)?), DbConfig::default(), schema())?;
    let mut invalid = Vec::new();
    let mut i = 0u64;

    let warm_end = Instant::now() + warmup;
    while Instant::now() < warm_end {
        canonical(&mut db, i)?;
        i += 1;
    }

    let hb = Heartbeat::start();
    let start = Instant::now();
    let end = start + duration;
    let measured_from = i;
    let mut grand = 0u64;
    while Instant::now() < end {
        canonical(&mut db, i)?;
        i += 1;
        grand += 1;
    }
    let elapsed = start.elapsed();
    if let Some(reason) = hb.finish() {
        invalid.push(reason);
    }
    // Totals two ways (spec §5): the loop counter delta vs the grand counter.
    // Structurally vacuous today — both increment in the same statement of
    // the same single-threaded loop — kept so every layer's throughput has a
    // uniform cross-check shape; it becomes a real check only if this loop
    // ever gains concurrency (the wire/kernel layers' cross-checks are the
    // genuinely independent ones).
    match crosscheck(&[i - measured_from], grand) {
        Ok(_) => {}
        Err(reason) => invalid.push(reason),
    }
    let acked = grand as f64 / elapsed.as_secs_f64();

    // Reads: one snapshot, typed gets. No log involvement — this number shows
    // where the write ceiling does NOT apply (spec §3). Measured under the
    // SAME warmup/duration the caller passed: the Report's warmup_secs/
    // duration_secs fields are global, and a silently different read window
    // would belie them (spec §5 — fields, not caveats buried in code).
    let snap = db.snapshot()?;
    let read_warm_end = Instant::now() + warmup;
    let mut r = 0u64;
    while Instant::now() < read_warm_end {
        let _ = snap.get(Id::<Slot>::from_raw(r % SLOTS))?;
        r += 1;
    }
    let read_start = Instant::now();
    let read_end = read_start + duration;
    let mut reads = 0u64;
    while Instant::now() < read_end {
        let _ = snap.get(Id::<Slot>::from_raw(reads % SLOTS))?;
        reads += 1;
    }
    let reads_per_sec = reads as f64 / read_start.elapsed().as_secs_f64();

    Ok((
        CoreReport {
            acked_writes_per_sec: acked,
            // No dispatcher runs at this layer: exactly one effects record
            // commits per transact, so records/sec == acked/sec here. The
            // kernel layer is where the job Done-marks double it (spec §3).
            log_records_per_sec: acked,
            reads_per_sec,
            device_ratio: acked / device_syncs_per_sec,
        },
        invalid,
    ))
}
```

Add `pub mod core_bench;` to `lib.rs`.

- [ ] **Step 2: Wire the subcommand arm**

In `bin/bench.rs`'s `run_layers`, replace the placeholder match with the block below.

> **AMENDED at Task 4's re-review (2026-08-07, finding I-A).** `temp_dir` shipped as
> `fn temp_dir(tag: &str, base: Option<&str>) -> Result<String, String>` — the `base`
> parameter keeps every layer on the operator's `--dir` filesystem rather than silently
> falling back to `$TMPDIR` (T4 I-1), and the `Result` makes a bad `--dir` a usage error
> instead of a panic (M-A/M-I). The match arm must also keep returning the `implemented`
> flag (M-7); the `other =>` arm below sets it to `false` rather than printing and
> falling through to a success exit.

```rust
    let implemented = match args.cmd.as_str() {
        "device" => true,
        "core" => {
            let dir = temp_dir("core", args.dir.as_deref()).map_err(std::io::Error::other)?;
            let ceiling = report.device.as_ref().expect("device ran first").syncs_per_sec;
            let (core, invalid) = bench_harness::core_bench::run(&dir, args.warmup, args.duration, ceiling)?;
            let _ = std::fs::remove_dir_all(&dir);
            report.core = Some(core);
            report.invalid.extend(invalid);
            true
        }
        other => {
            eprintln!("bench: subcommand {other:?} not implemented yet");
            false
        }
    };
    Ok(implemented)
```

- [ ] **Step 3: Add the ignored smoke** (in `tests/harness.rs`)

```rust
#[test]
#[ignore]
fn core_layer_commits_and_reports_a_ratio() {
    let dir = scratch("core");
    let (core, invalid) =
        bench_harness::core_bench::run(&dir, Duration::from_millis(200), Duration::from_secs(1), 1000.0)
            .expect("core layer runs");
    let _ = std::fs::remove_dir_all(&dir);
    assert!(invalid.is_empty(), "short healthy run must be valid: {invalid:?}");
    assert!(core.acked_writes_per_sec > 0.0);
    assert!(core.reads_per_sec > core.acked_writes_per_sec, "reads never fsync");
    assert!(core.device_ratio > 0.0);
}
```

- [ ] **Step 4: Verify**

Run: `cargo test -p bench-harness --test harness -- --ignored` → PASS (2 tests).
Run: `cargo run -p bench-harness --bin bench -- core --duration 2 --warmup 0.5 --json` → JSON contains `"core":{...,"device_ratio":...}`, exit 0.

- [ ] **Step 5: Ledger + commit**

Append the Task 5 entry to the ledger, then:

```bash
git add crates/bench-harness
git commit -m "bench: core layer — the commit path against its device ceiling (T5)"
```

---

### Task 6: `kernel_bench.rs` — the loop + dispatch overhead

> **AMENDED 2026-08-08, after the accounting/measuring interlude
> (`docs/superpowers/specs/2026-08-08-bench-accounting-split-design.md`, commits
> `f78f16a..efe26c7`).** The text below rev. 1 shipped put every accounting decision — both
> crosschecks, the rate divisions, the 2x `log_records_per_sec` rule, the heartbeat push and a
> bare `assert_eq!` on the response status — inline in `run`, fused to a `Kernel::run` call.
> That is precisely the shape the interlude was convened between T5 and T6 to stop T6 from
> copying: interlude spec §5 rules that T6 adopts the seam from inception, "so they inherit a
> tested pure function instead of copying an untested pattern". This amendment restructures
> Task 6 accordingly. **The measurement is unchanged** — same batch model, same
> `SimSource`/`ProdClock` construction, same published fields; only the location of the
> arithmetic moves, plus the three deliberate improvements called out below. Rev. 1's inline
> `run` body is superseded and must not be used as a reference for "what it used to do".
>
> Three changes beyond relocation, each with its reason:
> 1. **Three totals sources, not two.** `KernelStats` exposes `requests` as well as
>    `jobs_done` (verified at `kernel/mod.rs:80-87`); rev. 1 crosschecked `responses.len()` and
>    `stats.jobs_done` against the planned count and silently ignored `stats.requests`. It is a
>    genuinely independent third counter and it is free.
> 2. **A non-200 response refuses by name instead of panicking.** Rev. 1 used
>    `assert_eq!(*status, 200, …)` inside the driver. A batch in which any request failed is
>    not a slow measurement, it is not a measurement — which is exactly the condition
>    `read_accounting`'s `hits != reads` arm already models as a named `Err`. As an assert it
>    is also unreachable from a unit test, which is the whole complaint the interlude exists to
>    answer.
> 3. **The degenerate floor applies here too.** `measure::degenerate_window_reason` is already
>    shared by `device` and `core`; the kernel layer becomes its third *consumer*, which is
>    sharing, not the third *copy* interlude spec §5 warns about. A 3-request batch cannot
>    produce a rate any more than a 3-op window can.

**Files:**
- Create: `crates/bench-harness/src/kernel_accounting.rs` (the pure layer)
- Create: `crates/bench-harness/src/kernel_bench.rs` (the impure driver)
- Modify: `crates/bench-harness/src/lib.rs` (add both `pub mod` lines, alphabetical)
- Modify: `crates/bench-harness/src/bin/bench.rs` (add the `kernel` arm)
- Modify: `crates/bench-harness/tests/harness.rs` (add smoke)

**Interfaces:**
- Consumes: `bench_harness::{build, SLOTS}`; `charpente::kernel::{Kernel, SimSource}`; `charpente::http::{Method, Request}`; `charpente::prod::{ProdClock, ProdDisk, ProdEntropy}`; `measure::{Heartbeat, crosscheck, degenerate_window_reason}`; `report::KernelReport`.
- Produces:
  - `pub struct KernelObs { pub requests_planned: u64, pub responses_recorded: u64, pub stats_requests: u64, pub stats_jobs_done: u64, pub first_failure: Option<String>, pub elapsed: Duration, pub heartbeat: Option<String> }` — `Debug + Clone + Default`, every field `pub`
  - `pub struct KernelAccounting { pub acked_writes_per_sec: f64, pub log_records_per_sec: f64, pub invalid: Vec<String> }`
  - `pub fn kernel_accounting(k: &KernelObs) -> Result<KernelAccounting, String>`
  - `kernel_bench::run(dir: &str, requests: u64) -> io::Result<(KernelReport, Vec<String>)>`

**The accounting module carries NO blanket `#![allow(clippy::disallowed_methods, clippy::disallowed_types)]`** — same as `core_accounting.rs`, for the same reason: the module is *defined* by not reading a clock or touching IO, so the workspace deny-list should be what enforces that rather than a grep. `kernel_bench.rs` keeps its allow; it calls `Instant::now()` and spawns the progress thread.

Design note (verified against `kernel/mod.rs:592-628`): `SimSource` is clock-generic. With `ProdClock` and `horizon: 0`, all requests pushed at `at_nanos: 0` are immediately ready; once the script drains, any remaining deadline (the platform cron, ~a day out) exceeds the horizon and `wait_until` returns `Wake::Shutdown`, so `Kernel::run` returns after draining requests **and** all enqueued jobs. Batch model: no warmup phase; elapsed includes the shutdown checkpoint — both disclosed by reporting `requests_total`/`jobs_done` alongside the rate. Two `ProdClock`s (source + kernel) mirror `serve.rs`'s own known precision-only skew.

- [ ] **Step 1a: Create `crates/bench-harness/src/kernel_accounting.rs` (the pure layer)**

```rust
//! The accounting half of layer 3. Interlude spec
//! docs/superpowers/specs/2026-08-08-bench-accounting-split-design.md §5 rules
//! that this layer adopts the observations seam from inception rather than
//! having it retrofitted: every crosscheck, rate division and INVALID decision
//! the kernel layer makes is a pure function over observations already taken.
//!
//! Nothing here reads a clock, touches a database, a disk, a socket or a
//! thread. That is enforced rather than promised: unlike `kernel_bench` beside
//! it, this module carries no blanket
//! `#![allow(clippy::disallowed_methods, clippy::disallowed_types)]`, so hard
//! rule 1's workspace deny-list is what holds it.

use crate::measure::{crosscheck, degenerate_window_reason};
use std::time::Duration;

/// What one measured kernel batch observed. Every field is a count, a duration
/// or a verdict that was actually observed; nothing here is derived and
/// nothing here is a decision.
#[derive(Debug, Clone, Default)]
pub struct KernelObs {
    /// Requests pushed into the source before `Kernel::run`.
    pub requests_planned: u64,
    /// Responses the recording sink actually collected — totals source 1.
    pub responses_recorded: u64,
    /// `KernelStats::requests` — totals source 2, counted by the kernel's own
    /// dispatch rather than by the sink.
    pub stats_requests: u64,
    /// `KernelStats::jobs_done` — totals source 3. Every `/hit/:id` enqueues
    /// exactly one `Bump` (lib.rs's canonical route), so a drained batch must
    /// finish exactly as many jobs as it served requests.
    pub stats_jobs_done: u64,
    /// The first non-200 response, already rendered, or `None` if every
    /// request succeeded. Rendering happens in the driver because it needs the
    /// body bytes; the DECISION about it happens here.
    pub first_failure: Option<String>,
    /// Monotonic time `Kernel::run` took to drain the batch.
    pub elapsed: Duration,
    /// `Heartbeat::finish()` spanning the whole batch.
    pub heartbeat: Option<String>,
}

/// What the kernel layer publishes, plus every non-fatal reason it collected,
/// in push order.
#[derive(Debug, Clone)]
pub struct KernelAccounting {
    pub acked_writes_per_sec: f64,
    pub log_records_per_sec: f64,
    pub invalid: Vec<String>,
}

/// The kernel layer's accounting. `Err` refuses the run — both refusals mean
/// the batch is not a measurement of the serving path at all. `Ok` carries the
/// published values plus every non-fatal reason in push order.
///
/// Decision order mirrors `core_accounting::read_accounting` deliberately: the
/// heartbeat reason is collected first and is non-fatal, then the two
/// conditions that mean there is nothing to measure refuse by name, then the
/// non-fatal crosschecks. Unlike the core layer there is no expensive work
/// downstream of this call, so no short-circuit ordering constraint applies
/// (interlude spec §2 is about `run`'s population loop, which has no analogue
/// here) — the order is for reader parity across layers, and it is pinned.
pub fn kernel_accounting(k: &KernelObs) -> Result<KernelAccounting, String> {
    let mut invalid = Vec::new();
    if let Some(reason) = &k.heartbeat {
        invalid.push(reason.clone());
    }
    if let Some(reason) =
        degenerate_window_reason("kernel batch", k.requests_planned, k.elapsed)
    {
        return Err(reason);
    }
    if let Some(failure) = &k.first_failure {
        let planned = k.requests_planned;
        return Err(format!(
            "INVALID: a canonical request did not return 200 — the batch pushed {planned} \
             requests and at least one failed, so this is not a measurement of the kernel's \
             serving path at all, it is a measurement of how fast it can refuse; first \
             failure: {failure}"
        ));
    }
    // Totals THREE ways (spec §5), each against the planned count: the
    // recording sink, the kernel's own request counter, and its job counter.
    // These are genuinely independent at this layer, unlike the core write
    // loop's structurally-vacuous pair.
    if let Err(reason) = crosscheck(&[k.responses_recorded], k.requests_planned) {
        invalid.push(format!(
            "{reason} (recorded responses must equal the requests pushed)"
        ));
    }
    if let Err(reason) = crosscheck(&[k.stats_requests], k.requests_planned) {
        invalid.push(format!(
            "{reason} (KernelStats::requests must equal the requests pushed)"
        ));
    }
    if let Err(reason) = crosscheck(&[k.stats_jobs_done], k.requests_planned) {
        invalid.push(format!(
            "{reason} (jobs_done must equal requests: every /hit/:id enqueues exactly one Bump)"
        ));
    }

    let secs = k.elapsed.as_secs_f64();
    Ok(KernelAccounting {
        acked_writes_per_sec: k.requests_planned as f64 / secs,
        // Each request commits one effects record; each Bump's Done-mark
        // commits another (spec §3) — 2x by construction at this layer, which
        // is why this is a SUM and not a copy of the ack rate. Deriving it
        // from `stats_jobs_done` rather than from `requests_planned` twice is
        // deliberate: if the job counter ever disagrees, the published number
        // shows it instead of hiding it behind an assumed multiplier.
        log_records_per_sec: (k.requests_planned + k.stats_jobs_done) as f64 / secs,
        invalid,
    })
}
```

- [ ] **Step 1b: Create `crates/bench-harness/src/kernel_bench.rs` (the impure driver)**

```rust
//! Layer 3 (spec §2): the kernel loop fed in-process through RequestSource —
//! adds scheduling, dispatch, and job execution to the core layer's cost.
//! Batch model: N pre-built requests, measured across Kernel::run.
//! Edge tooling outside the deterministic core (DESIGN §1).
//!
//! This module keeps the loop, the Kernel, the Heartbeat and the progress
//! thread, fills a `KernelObs`, and makes no arithmetic decision of its own —
//! every one of those lives in `kernel_accounting` (interlude spec §5).
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use crate::kernel_accounting::{self, KernelObs};
use crate::measure::Heartbeat;
use crate::report::KernelReport;
use crate::{build, SLOTS};
use charpente::http::{Method, Request};
use charpente::kernel::{Kernel, SimSource};
use charpente::prod::{ProdClock, ProdDisk, ProdEntropy};
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

pub fn run(dir: &str, requests: u64) -> io::Result<(KernelReport, Vec<String>)> {
    let app = build(Box::new(ProdDisk::new(dir)?), Box::new(ProdEntropy::new()?))?;
    let mut source = SimSource::new(Box::new(ProdClock::new()), 0);
    for i in 0..requests {
        source.push_at(0, Request::new(Method::Post, &format!("/hit/{}", i % SLOTS)));
    }
    let responses = source.responses();
    let mut kernel = Kernel::new(app, Box::new(ProdClock::new()))?;

    let mut obs = KernelObs {
        requests_planned: requests,
        ..Default::default()
    };
    let hb = Heartbeat::start();
    // Kernel::run is one blocking call over the whole batch — a watcher
    // thread reports drain progress on stderr so a slow (fsync-bound) run is
    // distinguishable from a hung one. It reads the shared response vec the
    // source already exposes: one uncontended lock every 5 s.
    let stop_progress = Arc::new(AtomicBool::new(false));
    let progress = {
        let (responses, stop) = (responses.clone(), stop_progress.clone());
        let started = Instant::now();
        std::thread::spawn(move || {
            // 100 ms stop-poll so the post-run join returns promptly; one
            // progress line every 50th tick (~5 s).
            let mut ticks = 0u32;
            loop {
                std::thread::sleep(Duration::from_millis(100));
                if stop.load(Ordering::Relaxed) {
                    break;
                }
                ticks += 1;
                if ticks % 50 == 0 {
                    let done = responses.lock().expect("responses lock").len();
                    eprintln!(
                        "bench: kernel {done}/{requests} requests ({:.0}/s so far)",
                        done as f64 / started.elapsed().as_secs_f64()
                    );
                }
            }
        })
    };
    let start = Instant::now();
    let run_outcome = kernel.run(&mut source);
    obs.elapsed = start.elapsed();
    stop_progress.store(true, Ordering::Relaxed);
    run_outcome?;
    let _ = progress.join();
    obs.heartbeat = hb.finish();

    {
        let responses = responses.lock().expect("responses lock");
        obs.responses_recorded = responses.len() as u64;
        // Rendered here because it needs the body bytes; the decision about
        // it belongs to the accounting, which is why this records the first
        // failure rather than asserting on it (T6 amendment, change 2).
        obs.first_failure = responses.iter().find(|(status, _)| *status != 200).map(
            |(status, body)| format!("HTTP {status}: {}", String::from_utf8_lossy(body)),
        );
    }
    let stats = kernel.stats();
    obs.stats_requests = stats.requests;
    obs.stats_jobs_done = stats.jobs_done;

    let a = kernel_accounting::kernel_accounting(&obs).map_err(io::Error::other)?;
    Ok((
        KernelReport {
            acked_writes_per_sec: a.acked_writes_per_sec,
            log_records_per_sec: a.log_records_per_sec,
            requests_total: obs.requests_planned,
            jobs_done: obs.stats_jobs_done,
        },
        a.invalid,
    ))
}
```

Add `pub mod kernel_accounting;` and `pub mod kernel_bench;` to `lib.rs`, keeping the block
alphabetical (`core_accounting`, `core_bench`, `device`, `kernel_accounting`, `kernel_bench`,
`measure`, `report`).

- [ ] **Step 1c: The accounting's unit tests, teeth proven by mutation**

Append to `kernel_accounting.rs`. **Nine tests.** Count them when done; the number is a
close-out gate.

These pin behaviour written moments earlier in Step 1a, so — exactly as interlude spec §7
ruled for the core layer — they **arrive passing** and their teeth are proven by **mutation**:
mutate the accounting, capture the real RED, restore byte-exact (`shasum -a 256` both sides),
report both halves. **A compile error is not a RED.** Take a pristine copy first and restore
with `cp`, never `git checkout --` — at this point the tests are uncommitted and a checkout
would delete the work being tested (the T2 execution review's Critical).

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::measure::MIN_MEASURED_OPS;

    /// A plausible healthy batch: 10,000 requests drained in 2s, all three
    /// totals agreeing, every response 200. Every derived quantity is exact in
    /// f64 (10000/2 = 5000, 20000/2 = 10000 — both representable, both exact
    /// quotients), so these assertions are equalities, not tolerances. This is
    /// also the ABOVE-the-floor case; the boundary test below covers at/under.
    fn healthy() -> KernelObs {
        KernelObs {
            requests_planned: 10_000,
            responses_recorded: 10_000,
            stats_requests: 10_000,
            stats_jobs_done: 10_000,
            first_failure: None,
            elapsed: Duration::from_secs(2),
            heartbeat: None,
        }
    }

    #[test]
    fn a_healthy_batch_publishes_and_flags_nothing() {
        let a = kernel_accounting(&healthy()).expect("a healthy batch publishes");
        assert!(a.invalid.is_empty(), "got: {:?}", a.invalid);
        assert_eq!(a.acked_writes_per_sec, 5_000.0);
    }

    /// The layer's headline accounting rule (spec §3): one effects record per
    /// request PLUS one per Bump Done-mark, so the log rate is exactly twice
    /// the ack rate on a healthy batch — by construction, not by measurement.
    #[test]
    fn the_log_record_rate_is_the_sum_of_requests_and_job_done_marks() {
        let a = kernel_accounting(&healthy()).expect("publishes");
        assert_eq!(a.log_records_per_sec, 10_000.0);
        assert_eq!(
            a.log_records_per_sec,
            2.0 * a.acked_writes_per_sec,
            "2x by construction at this layer"
        );
    }

    /// …and it is derived from the OBSERVED job count, not from a hardcoded
    /// multiplier — so a batch whose jobs did not all finish publishes a lower
    /// log rate rather than an assumed one. (It is also flagged; that is the
    /// crosscheck test below. Both matter: one is what we PUBLISH, the other
    /// is what we SAY.)
    #[test]
    fn a_short_job_count_lowers_the_published_log_rate_rather_than_being_assumed() {
        let mut o = healthy();
        o.stats_jobs_done = 9_000;
        let a = kernel_accounting(&o).expect("a mismatch is named, not refused");
        assert_eq!(a.log_records_per_sec, 9_500.0);
        assert!(
            a.log_records_per_sec < 2.0 * a.acked_writes_per_sec,
            "must not publish the assumed 2x"
        );
    }

    #[test]
    fn a_below_floor_batch_refuses_and_an_at_floor_one_does_not() {
        let mut o = healthy();
        o.requests_planned = MIN_MEASURED_OPS - 1;
        o.responses_recorded = MIN_MEASURED_OPS - 1;
        o.stats_requests = MIN_MEASURED_OPS - 1;
        o.stats_jobs_done = MIN_MEASURED_OPS - 1;
        let err = kernel_accounting(&o).expect_err("below the floor refuses");
        assert!(err.contains("INVALID"), "got: {err}");
        assert!(err.contains("kernel batch"), "must name which loop: {err}");

        o.requests_planned = MIN_MEASURED_OPS;
        o.responses_recorded = MIN_MEASURED_OPS;
        o.stats_requests = MIN_MEASURED_OPS;
        o.stats_jobs_done = MIN_MEASURED_OPS;
        let a = kernel_accounting(&o).expect("the floor itself publishes");
        assert!(a.invalid.is_empty(), "got: {:?}", a.invalid);
    }

    /// T6 amendment, change 2: a failed request makes the batch not a
    /// measurement, so it refuses by name rather than panicking in the driver
    /// where no test could reach it.
    #[test]
    fn a_non_200_response_refuses_by_name_and_quotes_the_failure() {
        let mut o = healthy();
        o.first_failure = Some("HTTP 500: boom".to_string());
        let err = kernel_accounting(&o).expect_err("a failed request refuses");
        assert!(err.contains("INVALID"), "got: {err}");
        assert!(err.contains("did not return 200"), "got: {err}");
        assert!(
            err.contains("HTTP 500: boom"),
            "must quote the failure it saw: {err}"
        );
        assert!(
            err.contains("10000"),
            "must name the batch size that was attempted: {err}"
        );
    }

    /// Each of the three totals sources is checked SEPARATELY and named
    /// distinguishably — a single merged crosscheck would tell an operator
    /// that something disagreed without saying which counter, which is the
    /// whole value of having three.
    #[test]
    fn each_of_the_three_totals_sources_is_named_when_it_alone_disagrees() {
        for (label, needle, mutate) in [
            (
                "sink",
                "recorded responses",
                (|o: &mut KernelObs| o.responses_recorded -= 1) as fn(&mut KernelObs),
            ),
            ("kernel", "KernelStats::requests", |o: &mut KernelObs| {
                o.stats_requests -= 1
            }),
            ("jobs", "every /hit/:id enqueues", |o: &mut KernelObs| {
                o.stats_jobs_done -= 1
            }),
        ] {
            let mut o = healthy();
            mutate(&mut o);
            let a = kernel_accounting(&o).expect("a totals mismatch is named, not refused");
            assert_eq!(a.invalid.len(), 1, "{label}: got {:?}", a.invalid);
            assert!(
                a.invalid[0].contains(needle),
                "{label}: must name which counter disagreed: {:?}",
                a.invalid
            );
            assert!(a.invalid[0].contains("INVALID"), "{label}: {:?}", a.invalid);
        }
    }

    #[test]
    fn the_heartbeat_reason_is_carried_through_and_is_not_fatal() {
        let mut o = healthy();
        o.heartbeat = Some("INVALID: heartbeat starved".to_string());
        let a = kernel_accounting(&o).expect("a starved heartbeat is no refusal");
        assert_eq!(a.invalid, vec!["INVALID: heartbeat starved".to_string()]);
        assert_eq!(a.acked_writes_per_sec, 5_000.0);
    }

    /// Reasons are pushed in a fixed order and the renderers derive from this
    /// one list, so a reordering is invisible to every other assertion here
    /// and visible in the published artifact. Force all four at once.
    #[test]
    fn the_reasons_are_pushed_in_a_fixed_order() {
        let mut o = healthy();
        o.heartbeat = Some("INVALID: heartbeat starved".to_string());
        o.responses_recorded -= 1;
        o.stats_requests -= 2;
        o.stats_jobs_done -= 3;
        let a = kernel_accounting(&o).expect("all four of these are non-fatal");
        let shape: Vec<&str> = a
            .invalid
            .iter()
            .map(|r| {
                if r.contains("heartbeat starved") {
                    "heartbeat"
                } else if r.contains("recorded responses") {
                    "sink"
                } else if r.contains("KernelStats::requests") {
                    "kernel"
                } else if r.contains("every /hit/:id enqueues") {
                    "jobs"
                } else {
                    "UNKNOWN"
                }
            })
            .collect();
        assert_eq!(
            shape,
            vec!["heartbeat", "sink", "kernel", "jobs"],
            "got: {:?}",
            a.invalid
        );
    }

    /// The floor refuses BEFORE the non-200 arm, so a degenerate batch that
    /// also failed names the floor — the condition an operator must fix first.
    #[test]
    fn the_floor_refuses_before_the_non_200_arm() {
        let mut o = healthy();
        o.requests_planned = 1;
        o.responses_recorded = 1;
        o.stats_requests = 1;
        o.stats_jobs_done = 1;
        o.first_failure = Some("HTTP 500: boom".to_string());
        let err = kernel_accounting(&o).expect_err("refuses");
        assert!(err.contains("kernel batch"), "the floor wins: {err}");
        assert!(!err.contains("did not return 200"), "got: {err}");
    }
}
```

**The mutation table.** Apply each, run **the whole module** (`cargo test -p bench-harness
--lib kernel_accounting`, not one named test — the T2 review's M-1), quote the RED, restore.

| # | Mutation (in `kernel_accounting.rs`) | Test that must go RED |
|---|---|---|
| 1 | `acked_writes_per_sec: … / secs` → `… / (secs * 2.0)` | `a_healthy_batch_publishes_and_flags_nothing` |
| 2 | `(k.requests_planned + k.stats_jobs_done)` → `(k.requests_planned * 2)` | `a_short_job_count_lowers_the_published_log_rate_rather_than_being_assumed` (and NOT the 2x test — report that it stays green, which is the point of having both) |
| 3 | `(k.requests_planned + k.stats_jobs_done)` → `k.requests_planned` | `the_log_record_rate_is_the_sum_of_requests_and_job_done_marks` |
| 4 | delete the `return Err(reason)` in the floor arm; `let _ = reason;` to keep the binding used | `a_below_floor_batch_refuses_and_an_at_floor_one_does_not` |
| 5 | `degenerate_window_reason("kernel batch", …)` → `("kernel KATCH", …)` | same test, on the `contains("kernel batch")` assertion specifically |
| 6 | delete the `return Err(...)` in the non-200 arm; `let _ = failure;` | `a_non_200_response_refuses_by_name_and_quotes_the_failure` |
| 7 | in the non-200 message, drop `first failure: {failure}` | same test, on the quote-the-failure assertion |
| 8 | swap the `responses_recorded` and `stats_requests` crosscheck blocks | `the_reasons_are_pushed_in_a_fixed_order` |
| 9 | drop the `(recorded responses must equal …)` suffix, leaving the bare `{reason}` | `each_of_the_three_totals_sources_is_named_when_it_alone_disagrees` |
| 10 | move the non-200 arm ABOVE the floor arm | `the_floor_refuses_before_the_non_200_arm` |
| 11 | delete the heartbeat push block | `the_heartbeat_reason_is_carried_through_and_is_not_fatal` |

Row 2 is the one that earns its keep: it is the mutation an "obviously equivalent"
simplification would make, and only the short-job-count test catches it. **If any row does not
go red, that is a finding to report, not a row to drop.**

- [ ] **Step 2: Wire the subcommand arm** (in `run_layers`)

Same two amendments as Task 5's arm (T4 re-review, I-A): `temp_dir` takes the `--dir`
base and returns a `Result`, and the arm evaluates to `true` for the `implemented` flag.

```rust
        "kernel" => {
            let dir = temp_dir("kernel", args.dir.as_deref()).map_err(std::io::Error::other)?;
            let (kernel, invalid) = bench_harness::kernel_bench::run(&dir, args.requests)?;
            let _ = std::fs::remove_dir_all(&dir);
            report.kernel = Some(kernel);
            report.invalid.extend(invalid);
            true
        }
```

- [ ] **Step 3: Add the ignored smoke** (in `tests/harness.rs`)

```rust
#[test]
#[ignore]
fn kernel_layer_drains_the_batch_with_every_job_done() {
    let dir = scratch("kernel");
    let (k, invalid) = bench_harness::kernel_bench::run(&dir, 500).expect("kernel layer runs");
    let _ = std::fs::remove_dir_all(&dir);
    assert!(invalid.is_empty(), "short healthy run must be valid: {invalid:?}");
    assert_eq!(k.requests_total, 500);
    assert_eq!(k.jobs_done, 500);
    assert!(k.acked_writes_per_sec > 0.0);
}
```

- [ ] **Step 4: Verify**

**Counts, corrected at the T6 amendment** — rev. 1 said "PASS (3 tests)" for the harness
target, which was true when it was written and has been stale since Task 5. Measured at
`efe26c7` (the interlude close-out, this task's base):

- `cargo test -p bench-harness` → lib **42**, bin 15, app 1, harness 0 passed / **6 ignored**,
  doc 0. After this task: lib **51** (42 + Step 1c's nine accounting tests), harness **7
  ignored** (6 + Step 3's smoke). Bin, app and doc unchanged. **Workspace delta: +9 passed,
  +1 ignored** against a measured base of 641/37 → expect **650 passed / 38 ignored** over 54
  result lines. If any other number moves, find out what changed — the resolution is never to
  delete a test to make the arithmetic agree.
- `cargo test -p bench-harness --test harness -- --ignored` → **7 tests**, all PASS.
- `cargo test -p bench-harness --lib kernel_accounting` → **9 passed**.

Gates, same as every task: `cargo fmt --all --check`; `cargo clippy --workspace --all-targets
-- -D warnings`; `cargo clippy -p bench-harness --all-targets --target
x86_64-unknown-linux-gnu -- -D warnings`; and `git diff --stat -- crates/charpente-core
crates/charpente` **empty** (the Global Constraint).

Exit-criterion check on the new pure module, in the corrected form the interlude landed
(anchored to an attribute, because the bare-string grep matches the module's own doc comment):

```bash
grep -nE 'Instant::now|SystemTime|\bDb\b|\bDisk\b|thread|std::fs|File' \
  crates/bench-harness/src/kernel_accounting.rs | grep -vE ':\s*(///|//!|//)'
grep -nE '^\s*#!?\[allow\(clippy::disallowed' crates/bench-harness/src/kernel_accounting.rs
```

Both expected: **no output at all.**

Run: `cargo run -p bench-harness --bin bench -- kernel --requests 2000 --json` → JSON has
`"kernel":{...}`, exit 0, and `log_records_per_sec` is exactly twice `acked_writes_per_sec` on
a healthy run. If `Kernel::run` fails to terminate (the `SimSource`+`ProdClock` contingency),
STOP AND ASK — do not patch `charpente` (Global Constraints).

- [ ] **Step 5: Ledger + commit**

Append the Task 6 entry to the ledger, then:

```bash
git add crates/bench-harness
git commit -m "bench: kernel layer — batch model through RequestSource (T6)"
```

---

### Task 7: keep-alive client + `bench-app` vehicle + the wire layer

**Rev. 2 (2026-08-09, T7 pre-flight; rev. 1 replaced, not retained).** Rev. 1 predated the
accounting/measuring interlude and, exactly like Task 6's rev. 1, put every accounting
decision inline in the measuring function: the crosscheck, the throughput and `client_busy_fraction` divisions,
the saturation threshold, the `records_per_request` multiply and the empty-latency refusal
all lived in `sweep_point`, fused to a spawned child process and a thread pool. Interlude
spec §5 names T7 alongside T6 as adopting the seam *from inception* — "so they inherit a
tested pure function instead of copying an untested pattern." Restructured into
`wire_accounting.rs` (pure, no blanket clippy allow, so the deny-list enforces the
no-clock/no-socket property) plus `wire.rs` (child process, threads, `Heartbeat`, no
arithmetic decision of its own).

Four changes beyond relocation:

1. **The device-ceiling gate reaches the headline layer (Noah ruling, 2026-08-09).**
   Rev. 1 had no ceiling and no gate — the wire layer, whose number *is* what the README
   claim is about, was the only one of the three with no implausibility check. The
   commit path takes one `sync()` per transaction with no group commit anywhere
   (`log.rs:289`), so `bench-app`'s two records per acked request are bounded by the
   device ceiling one-for-one, the same physics layer 3 established. `sweep` now takes
   `device_ceiling: f64` the way `core_bench::run` and `kernel_bench::run` already do,
   and `SweepPoint` gains a published `device_ratio` — the ruling matching T6's I-4,
   which added `window_secs`/`device_ratio` to `KernelReport` for the same reason. This
   moves the byte-exact literal in `report::json_shape_is_pinned`; that is the pin doing
   its job, and Step 1 is the only place in this task permitted to move it.
   **The prose differs from layer 3's and that is deliberate.** This layer cannot see
   the server's log from outside the process, so its record rate is DERIVED
   (`records_per_request * throughput`) where layer 3 OBSERVES `KernelStats::jobs_done`.
   A ratio above 1.0 therefore has two possible causes here, and the reason names both.

2. **A window floor, not just an op floor.** Rev. 1 had neither. The op floor alone is
   not enough at this layer: five ops across 256 connections can clear
   `MIN_MEASURED_OPS` inside a 4 ms `--duration`, and the rate published from that
   window is the exact shape of the T6 review's C-1 (83 acked writes/s from a 46 ms
   window, exit 0, `invalid: []`). The op floor is `degenerate_window_reason`'s fourth
   CONSUMER — sharing, not the third COPY spec §5 warns about. The window floor compares
   against the same `measure::MIN_MEASURED_WINDOW` layer 3 uses and carries its own
   prose, on the T6 ruling's principle that the RULE is shared and the PROSE is not:
   this layer's window IS `--duration`, so its remedy is "raise --duration", where layer
   3's batch-determined window says "raise --requests".

3. **The crosscheck's honesty, carried over from the T6 review's I-2.** `per_thread`,
   `grand` and the latency vector are all bumped in one unbranched block inside the
   worker loop, so as a check on the MEASURED PATH they are one check, not three. They
   stay genuinely independent as a check on this harness's own collection plumbing — a
   dropped `join`, a mis-accumulated sum. The observations struct says so at the field,
   rather than letting a future reader read three sources as three witnesses.

4. **`--target invoicing` refuses by name instead of panicking.** Rev. 1's `unreachable!
   ("wired in T8")` sits behind a flag value the shipped usage string advertises, so
   `bench wire --target invoicing` would abort the binary with a panic — a contract
   violation with no named remedy (hard rule 5). The CLI arm refuses it by name until
   T8 lands; the `unreachable!`s stay as the internal assertion they are.

Verification counts, measured at `b3cb252` and stated as a delta: lib 59, bin 15, app 1,
harness 7 ignored, workspace 658 passed / 38 ignored.

**Rev. 3 (2026-08-09, after the first implementation attempt). The ratio gate is bracketed,
because rev. 2's shipped as a false-alarm generator.** Rev. 2's gate divided by the
CLI-threaded layer-1 ceiling and flagged above `1.0 + DEVICE_RATIO_SLACK` (1e-6). Run
against real hardware it fired on healthy runs, twice, at large margins:

| run | layer-1 ceiling | 1 conn | 4 conns | outcome |
|---|---|---|---|---|
| harness smoke | 198 syncs/s | 236 rec/s → **1.19x** | 245 rec/s → **1.24x** | smoke FAILED |
| same smoke, re-run | (higher) | — | — | 8/8 passed |
| `bench wire --json` | 243 syncs/s | 228 rec/s → 0.94x | 253 rec/s → **1.04x** | exit 1 |

**The derivation is not wrong and the measurement is not wrong.** At 1 connection p50 was
8189 µs; a 243 syncs/s ceiling is 4.1 ms per sync and two syncs is 8.2 ms. charpente is
running at the device ceiling, which is the CORRECT result for an ack-after-durable design.
The wire number was stable (228–253 rec/s) while the layer-1 ceiling swung 198 → 243 — the
denominator is the noisy term, and the ledger already records a 220–310 syncs/s cross-window
dispersion, with the T6 review noting the kernel layer sits at 0.99x, "well inside the band
where the dispersion would carry it past 1.0 unnoticed."

So a layer that is *supposed* to sit at 1.0 cannot be gated at `1.0 + 1e-6` against a
ceiling measured in a different window. This is the class interlude §6 named as unreachable
by pure-function testing and findable only by running the binary; it was found by running
the binary.

**Noah ruling (2026-08-09): bracket the ceiling.** Each sweep point takes its own device
sample before spawning its child and after killing it, and the gate divides by
`max(before, after)`. Both samples are published so the ratio carries a visible error bar.

**Three deliberate divergences from the core layer's bracket shape, recorded so they are not
"consistency"-fixed later:**

1. **The core layer publishes brackets but does NOT divide by them** — `core_accounting`
   gates on the threaded `device_ceiling` and cites the brackets only as prose evidence
   (`device_ratio_reason`'s signature takes `before`/`after` for the message alone). The
   wire layer divides by the bracket max. The reason is the 0.99x-vs-0.75x difference above:
   the core layer sits well below its ceiling, so a noisy denominator rarely crosses 1.0
   there, and it crosses constantly here.
2. **Consequently `SweepPoint::device_ratio` is not comparable to `CoreReport::device_ratio`
   or `KernelReport::device_ratio`** — different denominators. `docs/BENCH.md` (T10) must
   say so; do not present the three side by side as one column.
3. **Brackets are per POINT, not per sweep.** A bracket sample is
   `BRACKET_WARMUP + BRACKET_DURATION` = 1.1s against a spec-default 35s point (~6%), and
   per-sweep brackets on a five-point sweep would sit minutes from the middle points, which
   is the locality problem this ruling exists to fix.

**Superseding rev. 2 in the blocks below** (the blocks are otherwise unchanged; where they
disagree with this list, this list wins):

- `WireObs::device_ceiling: f64` becomes **`device_before: f64` and `device_after: f64`**.
- `point_accounting` computes `let ceiling = w.device_before.max(w.device_after);` and
  `device_ratio = log_records_per_sec / ceiling`. `implausible_ratio` is unchanged and still
  the shared predicate.
- The ceiling reason cites **both** samples, the `core_accounting::device_ratio_reason`
  precedent: `"… the bracket samples either side of this point ({before:.2} before / {after:.2} after) …"`.
- `SweepPoint` gains **`device_syncs_per_sec_before: f64` and `device_syncs_per_sec_after:
  f64`** beside `device_ratio`, named exactly as `CoreReport`'s equivalents. All three go
  into `render_json`, `render_human`, `collect_non_finite`, `fixed_report()` and the pin.
- `wire::sweep` **drops its `device_ceiling: f64` parameter** — the layer measures its own —
  and `bin/bench.rs`'s `wire` arm no longer reads `report.device`. Layer 1 still runs first
  and still publishes the `device:` line; the wire layer simply no longer divides by it.
- `sweep_point` calls `device_bracket_sample` **before `spawn_target` and after `drop(node)`**.
  Sampling while the child is live would contend with it for the device and under-measure the
  ceiling, which is the failure this ruling fixes, not a variant of it. Use a sibling
  directory of the point dir, never the child's own data dir.
- `core_bench::device_bracket_sample` is currently private. **Promote it to
  `pub(crate)` and reuse it** — do not hand-roll a second sync loop (it already reuses
  `device::timed_loop`, and its `"bracket"` name string was fixed in T5's re-review N-11 to
  avoid a doubled "device device" rendering). Moving it to `device.rs` beside `timed_loop`
  is acceptable and probably better; copying it is not.

**Added tests** (on top of Step 7b's thirteen): the gate divides by the bracket MAX not the
min and not `before`; an under-measured single sample does not by itself flag a healthy
point; both bracket samples reach the published point; a non-finite bracket flows to
`collect_non_finite`. **Added mutation rows**: `max` → `min`; `max` → `before`; drop either
published bracket field from `render_json`.

**The smoke changes shape.** `assert!(invalid.is_empty())` is what went flaky, and it stays —
but it is now a real assertion rather than a coin flip, and the smoke must additionally
assert `p.device_ratio == p.log_records_per_sec / p.device_syncs_per_sec_before.max(p.device_syncs_per_sec_after)`.
If it flakes again, that is a finding to report, not a threshold to widen.

**Rev. 4 (2026-08-09). It flaked again — and rev. 3 disproved its own premise, so the gate
moves to a quantity that is counted rather than derived.**

Rev. 3's brackets worked exactly as designed, and what they measured refuted the model
behind them. Taken either side of each point with the child stopped, the samples agree to
within ~1%:

| point | bracket before / after | derived rec/s | ratio |
|---|---|---|---|
| 1 conn | 241.80 / 244.48 | 250 | 1.02x |
| 4 conns | 241.92 / 239.95 | 258 | 1.07x |
| 1 conn (run 2) | 241.31 / 244.48 | 254 | 1.04x |
| run 3 | — | 260, 270 | 1.05x, 1.10x |

**The ceiling is not noisy when measured locally; rev. 2's diagnosis was wrong.** The
198→243 swing it was built on was cross-window drift, which bracketing correctly removed.
Yet the smoke failed **3 of 3** — reproducible, not flaky — because the *numerator* is the
uncertain term. `log_records_per_sec` is `records_per_request * throughput`, and that
assumes every enqueued `Bump` drains inside the window. On a 2s window the queue is still
building a small backlog, so real records are fewer than 2x requests and the derivation
overshoots by exactly the backlog fraction. 2–7% is what a modest backlog looks like. This
is the second cause rev. 3's own refusal prose already named: it was right, and it was
firing.

**A derived quantity cannot be made gate-worthy by improving the quantity it is divided
by.** That is the general lesson, and it is why rev. 3 was the wrong fix rather than an
insufficient one.

**Noah ruling (2026-08-09): gate on the acked-request rate; publish the record rate.**

- The gate is `ack_ratio = throughput_per_sec / max(before, after)`. `throughput_per_sec`
  is **counted**, and every acked request commits at least its own effects record and pays
  at least one durable sync for it (ack-after-durable), so exceeding the ceiling is
  impossible **by construction rather than by assumption**. Measured margin on this
  hardware: 114–135 req/s against a ~242 ceiling, ≈0.5x. It has no false-alarm path.
- `device_ratio` (the derived record rate over the same bracket max) and both bracket
  samples stay **published and ungated**. Read it as "how close this run came to the
  ceiling, if the 2-records-per-request derivation held." Above 1.0 on short windows is
  expected and is not a defect.
- **The gate is weaker on purpose.** It would not catch a 2x error in `records_per_request`.
  That is the price of soundness, it is stated in `BENCH.md` (T10), and it is not to be
  "strengthened" back onto `device_ratio` — mutation row 15 exists to redden precisely that
  change.
- Rev. 3's divergence #2 is **withdrawn**: with no record-level gate here, there is no
  cross-layer gate comparison to mis-read. `device_ratio` remains non-comparable across
  layers as a *number*, and `BENCH.md` still says so.

**Test consequences** (all landed): `a_rate_above_the_device_ceiling_...` becomes
`an_ack_rate_above_...`; `the_ceiling_refusal_names_the_derivation_as_a_possible_cause` is
replaced by `the_ceiling_refusal_names_the_ack_after_durable_argument`; and the new
`a_derived_record_rate_above_the_ceiling_does_not_flag_by_itself` is **the regression test
for this whole finding**, fixturing the ceiling (800) between the counted ack rate (500)
and the derived record rate (1000) — the exact shape the hardware produced.

**Two tests went vacuous under the new gate and were re-tuned, which is worth recording
because it is the trap this sub-project keeps hitting.** `the_ratio_gate_divides_by_the_
bracket_max_not_the_min_or_before` and `an_under_measured_single_bracket_sample_...` used
900/1200 brackets against a 500/s ack rate: under rev. 4 both `max` and `min` land far
below 1.0, so neither could fail either way. Re-fixtured to 450/600, where `max` gives 0.83
and clears while `min`/`before` gives 1.11 and would flag. A gate change silently converts
its own boundary tests into assertions that cannot fail; re-check every fixture that
straddled the old threshold.

**Files:**
- Create: `crates/bench-harness/src/client.rs`
- Create: `crates/bench-harness/src/wire_accounting.rs`
- Create: `crates/bench-harness/src/wire.rs`
- Create: `crates/bench-harness/src/bin/bench-app.rs`
- Modify: `crates/bench-harness/src/lib.rs` (add `pub mod client; pub mod wire; pub mod wire_accounting;`)
- Modify: `crates/bench-harness/src/report.rs` (`SweepPoint::device_ratio` and every site that already handles a sweep-point field)
- Modify: `crates/bench-harness/src/bin/bench.rs` (the `wire` arm)
- Modify: `crates/bench-harness/tests/harness.rs` (add smoke)

**Interfaces:**
- Consumes: `measure::{process_cpu, crosscheck, percentile, Heartbeat, degenerate_window_reason, implausible_ratio, MIN_MEASURED_WINDOW}`; `report::{WireReport, SweepPoint}`; `charpente::serve::serve` (NOT `charpente::launch` — see Step 6).
- Produces:
  - `client::Conn` — `connect(addr: &str) -> io::Result<Conn>`, `request(&mut self, method: &str, path: &str, headers: &[(String, String)], body: &[u8]) -> io::Result<(u16, Vec<u8>)>`; `client::read_response(r: &mut impl BufRead) -> io::Result<(u16, Vec<u8>)>` (pure-parsable, unit-tested un-ignored).
  - `wire_accounting::{WireObs, WireAccounting, point_accounting, SATURATION_FRACTION}` — pure.
  - `wire::Target` enum (`BenchApp`, `Invoicing`) with `records_per_request`, `session_setup`, `one_request` (Task 8 fills the `Invoicing` arms; this task ships them as `unreachable!("wired in T8")`, reachable only through a CLI arm that refuses first).
  - `wire::build_bin(package: &str, bin: &str) -> io::Result<PathBuf>` (cargo `--message-format=json` string-scan, the `edge-harness/src/child.rs:302-330` shape).
  - `wire::sweep(bin: &Path, target: Target, connections: &[usize], warmup: Duration, duration: Duration, device_ceiling: f64) -> io::Result<(WireReport, Vec<String>)>`.
  - `bench-app` binary: `--addr A --data-dir D` (usage → stderr, exit 2).

- [ ] **Step 1: `SweepPoint` gains `device_ratio`, and the pin moves once**

Add `pub device_ratio: f64` to `SweepPoint` immediately after `log_records_per_sec`,
mirroring `KernelReport`'s field order. Then update **every** site in `report.rs` that
already handles a sweep-point field, in that file's existing style:

- `render_json`'s per-point object,
- `render_human`'s per-point line,
- `collect_non_finite`'s wire walk (`wire[i].points[j].device_ratio` — a zero device
  ceiling must reach the central non-finite collector here exactly as it does at layers
  2 and 3; interlude §4's standing rule is that the accounting must not grow its own
  non-finite check),
- the `fixed_report()` test fixture,
- the byte-exact literal in `json_shape_is_pinned`, and `render_human`'s pin if it
  covers a sweep line.

Add one test that a non-finite wire `device_ratio` is collected by name, beside the
existing non-finite tests.

**This is the only step in Task 7 permitted to edit a pinned literal.** The report must
show the pin's before/after diff explicitly. If any *other* pinned string moves during
this task, something changed behaviour that was not supposed to.

- [ ] **Step 2: Write the failing parse test** (in `client.rs`'s `#[cfg(test)] mod tests` — pure, un-ignored)

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::io::BufReader;

    #[test]
    fn parses_a_keep_alive_response() {
        let raw: &[u8] = b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\n\
                           content-length: 2\r\n\r\n42HTTP/1.1 404 NO\r\ncontent-length: 0\r\n\r\n";
        let mut r = BufReader::new(raw);
        assert_eq!(read_response(&mut r).unwrap(), (200, b"42".to_vec()));
        assert_eq!(read_response(&mut r).unwrap(), (404, Vec::new()));
    }

    #[test]
    fn chunked_is_refused_by_name() {
        let raw: &[u8] = b"HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n";
        let err = read_response(&mut BufReader::new(raw)).unwrap_err();
        assert!(err.to_string().contains("chunked"), "got: {err}");
    }
}
```

- [ ] **Step 3: Run it, verify it fails** — `cargo test -p bench-harness client` → compile FAIL.

- [ ] **Step 4: Implement `client.rs`**

```rust
//! Hand-rolled keep-alive HTTP/1.1 client (spec §4). The edge-harness client
//! is deliberately Connection: close per request (client.rs:333-365) and
//! cannot drive a keep-alive load loop — this one reuses one TcpStream per
//! thread and frames responses by content-length.
//! Edge tooling outside the deterministic core (DESIGN §1).
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::Duration;

const CLIENT_TIMEOUT: Duration = Duration::from_secs(10);

pub struct Conn {
    reader: BufReader<TcpStream>,
}

impl Conn {
    pub fn connect(addr: &str) -> io::Result<Conn> {
        let sock: SocketAddr = addr
            .parse()
            .map_err(|e| io::Error::other(format!("bad addr {addr:?}: {e}")))?;
        let stream = TcpStream::connect_timeout(&sock, CLIENT_TIMEOUT)?;
        stream.set_nodelay(true)?;
        stream.set_read_timeout(Some(CLIENT_TIMEOUT))?;
        stream.set_write_timeout(Some(CLIENT_TIMEOUT))?;
        Ok(Conn { reader: BufReader::new(stream) })
    }

    pub fn request(
        &mut self,
        method: &str,
        path: &str,
        headers: &[(String, String)],
        body: &[u8],
    ) -> io::Result<(u16, Vec<u8>)> {
        let mut req = format!(
            "{method} {path} HTTP/1.1\r\nhost: bench\r\ncontent-length: {}\r\n",
            body.len()
        );
        for (k, v) in headers {
            req.push_str(&format!("{k}: {v}\r\n"));
        }
        req.push_str("\r\n");
        let mut bytes = req.into_bytes();
        bytes.extend_from_slice(body);
        self.reader.get_mut().write_all(&bytes)?;
        read_response(&mut self.reader)
    }
}

pub fn read_response(r: &mut impl BufRead) -> io::Result<(u16, Vec<u8>)> {
    let mut line = String::new();
    r.read_line(&mut line)?;
    let status: u16 = line
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| io::Error::other(format!("bad status line: {line:?}")))?;
    let mut content_length: Option<usize> = None;
    loop {
        let mut h = String::new();
        r.read_line(&mut h)?;
        if h == "\r\n" || h == "\n" || h.is_empty() {
            break;
        }
        let lower = h.to_ascii_lowercase();
        if let Some(v) = lower.strip_prefix("content-length:") {
            content_length = Some(
                v.trim()
                    .parse()
                    .map_err(|e| io::Error::other(format!("bad content-length {v:?}: {e}")))?,
            );
        }
        if lower.starts_with("transfer-encoding:") && lower.contains("chunked") {
            return Err(io::Error::other(
                "chunked responses are unsupported by the bench client — \
                 the bench routes must reply with content-length framing",
            ));
        }
    }
    let len = content_length
        .ok_or_else(|| io::Error::other("response carried neither content-length nor a named refusal"))?;
    let mut body = vec![0u8; len];
    r.read_exact(&mut body)?;
    Ok((status, body))
}
```

- [ ] **Step 5: Run the parse tests, verify they pass** — `cargo test -p bench-harness client` → PASS (2 tests).

- [ ] **Step 6: Implement `bin/bench-app.rs`**

```rust
//! The wire-layer vehicle (spec §1, as amended at the plan fix-wave): the
//! node.rs precedent — parse flags, build, serve::serve, park. Deliberately
//! NOT `charpente::launch`: launch() re-reads the live process argv
//! (launch.rs:31) and serves only on ZERO argv, so a child spawned with
//! `--addr`/`--data-dir` would exit 2 as an unknown verb and never bind.
//! Edge tooling outside the deterministic core (DESIGN §1).
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use charpente::prod::{ProdDisk, ProdEntropy};
use std::process::ExitCode;

fn main() -> ExitCode {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let mut addr = None;
    let mut dir = None;
    let mut it = argv.iter();
    while let Some(flag) = it.next() {
        match flag.as_str() {
            "--addr" => addr = it.next().cloned(),
            "--data-dir" => dir = it.next().cloned(),
            other => {
                eprintln!("bench-app: unknown flag {other:?}");
                eprintln!("usage: bench-app --addr HOST:PORT --data-dir DIR");
                return ExitCode::from(2);
            }
        }
    }
    let (Some(addr), Some(dir)) = (addr, dir) else {
        eprintln!("usage: bench-app --addr HOST:PORT --data-dir DIR");
        return ExitCode::from(2);
    };
    let serve = || -> std::io::Result<()> {
        let disk = ProdDisk::new(&dir)?;
        let entropy = ProdEntropy::new()?;
        let app = bench_harness::build(Box::new(disk), Box::new(entropy))?;
        // serve returns a handle; dropping it does not stop serving (launch's
        // own serve arm drops it the same way) — the park loop keeps the
        // process alive until the harness kills it.
        charpente::serve::serve(&addr, app)?;
        loop {
            std::thread::park();
        }
    };
    match serve() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("bench-app: {e}");
            ExitCode::from(1)
        }
    }
}
```

- [ ] **Step 7: Implement `wire_accounting.rs`**

```rust
//! The accounting half of layer 4. Interlude spec
//! docs/superpowers/specs/2026-08-08-bench-accounting-split-design.md §5 rules
//! that this layer adopts the observations seam from inception rather than
//! having it retrofitted: every floor, crosscheck, rate division and INVALID
//! decision the wire layer makes is a pure function over observations already
//! taken.
//!
//! Nothing here reads a clock, spawns a process, opens a socket or touches a
//! file. As in `kernel_accounting`, that half is enforced rather than
//! promised: this module carries no blanket
//! `#![allow(clippy::disallowed_methods, clippy::disallowed_types)]`, so hard
//! rule 1's workspace deny-list is what holds it. The no-`Conn`/no-`Child`
//! half is NOT on the deny-list and is held by review instead — adding either
//! to anything in this module defeats the seam with CI still green.
//!
//! **What this layer's `log_records_per_sec` is, stated here because this is
//! the one place a reader can check it.** Layer 3 OBSERVES
//! `KernelStats::jobs_done`. This layer cannot see the server's log from
//! outside the process, so its record rate is DERIVED —
//! `records_per_request * throughput` — and that derivation assumes the job
//! queue is at steady state inside the window. A `device_ratio` above 1.0
//! therefore has two possible causes here where layer 3 has one, and the
//! reason names both.

use crate::measure::{
    crosscheck, degenerate_window_reason, implausible_ratio, percentile, MIN_MEASURED_WINDOW,
};
use crate::report::SweepPoint;
use std::time::Duration;

/// Above this, the client's own CPU — not the server — is what the number
/// measures. Published per point rather than used as a refusal: a saturated
/// point is a real observation about this client, and the sweep's shape is
/// what makes it readable.
pub const SATURATION_FRACTION: f64 = 0.9;

#[derive(Debug, Clone, Default)]
pub struct WireObs {
    /// This point's connection count — an INPUT, and the label every reason
    /// this point raises is prefixed with.
    pub connections: usize,
    /// Per-worker measured request counts, collected through `join`.
    pub per_thread: Vec<u64>,
    /// The shared atomic every worker also bumped. Note what this pair does
    /// and does not prove: `per_thread`, `grand` and `latencies_us` are all
    /// bumped in ONE unbranched block in the worker loop, so as a check on
    /// the MEASURED PATH they are one check, not three (the T6 review's I-2
    /// finding, which applies here verbatim). They stay genuinely independent
    /// as a check on this harness's own collection plumbing — a dropped
    /// `join`, a mis-accumulated sum — which is the only thing the crosscheck
    /// is claimed to catch.
    pub grand: u64,
    /// Per-request latencies in microseconds, measured window only, in
    /// completion order. `point_accounting` sorts its own copy: the driver
    /// hands these over UNSORTED so that `percentile`'s sorted-input
    /// precondition is held by a function a unit test can reach.
    pub latencies_us: Vec<u64>,
    /// The measured window — `Instant::elapsed` across the `--duration`
    /// sleep, warmup excluded.
    pub elapsed: Duration,
    /// This process's user+system CPU across the window. The server is a
    /// separate process, so this is the CLIENT's cost and nothing else.
    pub client_cpu: Duration,
    /// `device.syncs_per_sec`, measured by layer 1 in this process and
    /// threaded in — an INPUT, not an observation of this layer.
    pub device_ceiling: f64,
    /// Log records committed per acked request for this target — an INPUT,
    /// the per-target constant from `Target::records_per_request` (spec §3).
    pub records_per_request: f64,
    /// `Heartbeat::finish()` over the measured window.
    pub heartbeat: Option<String>,
}

#[derive(Debug, Clone)]
pub struct WireAccounting {
    pub point: SweepPoint,
    pub invalid: Vec<String>,
}

/// The whole of this layer's arithmetic, and every decision it makes.
///
/// `Err` means this point is not a measurement at all, and the sweep drops it
/// fail-soft (spec §5): the points already measured are kept and the loss is
/// named. `Ok` carries the point plus any reasons that make the RUN
/// unpublishable — a non-empty `invalid` still exits 1.
pub fn point_accounting(w: &WireObs) -> Result<WireAccounting, String> {
    let n = w.connections;
    let at = |reason: String| format!("[{n} conns] {reason}");
    let ops = w.latencies_us.len() as u64;

    // Two floors, in this order, both refusing.
    //
    // The op floor is `degenerate_window_reason`'s fourth CONSUMER (device,
    // core's two windows, and this one) — sharing, not the third COPY
    // interlude spec §5 warns about. It also protects `percentile`'s
    // non-empty assert structurally: MIN_MEASURED_OPS > 0, so nothing below
    // reaches a percentile of an empty slice, and rev. 1's separate
    // empty-latency check is subsumed rather than dropped.
    //
    // The window floor compares against the same `MIN_MEASURED_WINDOW` layer
    // 3 uses but carries its own prose, on the T6 ruling's principle that the
    // RULE is shared and the PROSE is not: this layer's window IS
    // `--duration`. Without it, five ops across 256 connections can clear the
    // op floor inside a 4 ms window and publish a rate from it — the exact
    // shape of the T6 review's C-1. It is also what keeps `secs` off zero on
    // every published path, so no `inf` rate reaches `collect_non_finite`
    // through this layer's throughput division.
    if let Some(reason) = degenerate_window_reason("wire", ops, w.elapsed) {
        return Err(at(reason));
    }
    if w.elapsed < MIN_MEASURED_WINDOW {
        return Err(at(format!(
            "INVALID: the wire measured window was {:?}, under the \
             {MIN_MEASURED_WINDOW:?} minimum — raise --duration; a window this \
             short is dominated by connection ramp-up and scheduler noise \
             rather than by the path being measured",
            w.elapsed
        )));
    }

    // Fixed push order, pinned by a test: heartbeat, then the crosscheck,
    // then the ratio gate. Both renderers derive from this list, so a
    // reordering would be invisible to most assertions and visible in the
    // artifact (interlude §3.3).
    let mut invalid = Vec::new();
    if let Some(reason) = w.heartbeat.clone() {
        invalid.push(at(reason));
    }
    let total = match crosscheck(&w.per_thread, w.grand) {
        Ok(t) => t,
        Err(reason) => {
            invalid.push(at(reason));
            w.grand
        }
    };

    let secs = w.elapsed.as_secs_f64();
    let throughput = total as f64 / secs;
    let log_records_per_sec = w.records_per_request * throughput;
    let device_ratio = log_records_per_sec / w.device_ceiling;
    if implausible_ratio(device_ratio) {
        invalid.push(at(format!(
            "INVALID: log_records_per_sec {log_records_per_sec:.0} is \
             {device_ratio:.2}x the measured device ceiling of {:.0} durable \
             syncs/s — unlike layer 3 this rate is DERIVED as {} records per \
             acked request rather than observed, so either the measurement is \
             impossible or the job queue was not at steady state inside the \
             window",
            w.device_ceiling, w.records_per_request
        )));
    }

    let mut sorted = w.latencies_us.clone();
    sorted.sort_unstable();
    let client_busy_fraction = w.client_cpu.as_secs_f64() / (n as f64 * secs);
    Ok(WireAccounting {
        point: SweepPoint {
            connections: n,
            throughput_per_sec: throughput,
            // Spec §3's pairing at this layer; see the module doc for why this
            // is a per-target derivation and not a count.
            log_records_per_sec,
            device_ratio,
            p50_us: percentile(&sorted, 50.0),
            p99_us: percentile(&sorted, 99.0),
            p999_us: percentile(&sorted, 99.9),
            client_busy_fraction,
            saturated: client_busy_fraction > SATURATION_FRACTION,
        },
        invalid,
    })
}
```

- [ ] **Step 7b: The accounting unit tests**

Twelve `#[test]`s in `wire_accounting.rs`'s `mod tests`, sharing a `healthy()` fixture
builder that every test mutates from — the `kernel_accounting` convention, full-sentence
test names:

1. `a_healthy_point_publishes_and_flags_nothing`
2. `the_record_rate_is_the_per_target_multiple_of_throughput` (2.0 and 1.0 both)
3. `a_below_floor_point_refuses_and_an_at_floor_one_does_not` (`MIN_MEASURED_OPS` both sides)
4. `a_short_window_refuses_even_when_the_op_floor_is_cleared` (the C-1 shape: ops well above the floor, window under `MIN_MEASURED_WINDOW`)
5. `the_window_floor_admits_a_window_exactly_at_the_minimum` (boundary, other side)
6. `the_op_floor_refuses_before_the_window_floor` (both violated → op-floor prose, matching `degenerate_batch_reason`'s own internal precedence)
7. `a_disagreeing_crosscheck_is_named_and_falls_back_to_the_grand_counter`
8. `a_rate_above_the_device_ceiling_is_flagged_and_at_the_ceiling_is_not` (both sides of `DEVICE_RATIO_SLACK`)
9. `the_ceiling_refusal_names_the_derivation_as_a_possible_cause` (the layer-3/layer-4 difference is in the prose, not only in this plan)
10. `saturation_is_flagged_above_the_fraction_and_not_at_it`
11. `every_reason_carries_its_connection_count` (the `[{n} conns]` prefix, on all three `invalid` arms and both refusals)
12. `a_zero_device_ceiling_leaves_a_non_finite_ratio_for_the_central_collector` (interlude §4's standing rule: no local non-finite check)

Plus one that earns its keep by construction rather than by boundary:
13. `percentiles_come_from_a_sorted_copy_of_unsorted_observations` — hand the fixture
    latencies in a deliberately unsorted order and assert p50/p99/p999 are the sorted
    answers. This is what makes the "driver hands them over unsorted" contract real; if
    a future driver sorts first, this test still passes, and if the accounting stops
    sorting, it goes red.

**On the interlude's protocol:** these pin behaviour this task is itself introducing, so
unlike the interlude's own tests they arrive RED, not passing. Steps 7 and 7b are
therefore ordinary TDD — write the tests, watch them fail to compile, implement, watch
them pass. The mutation table below is still required, because a test that passes for
the wrong reason is the failure mode this sub-project has now hit five times.

- [ ] **Step 7c: The mutation table** (whole module per round, pristine copy taken first, `shasum -a 256` both sides, never `git checkout --`)

**Amended at execution time (2026-08-10), because rev. 3 and rev. 4 both changed this table
and neither wrote the change down.** Rev. 3 added three rows in prose only ("Added mutation
rows: `max` → `min`; `max` → `before`; drop either published bracket field from
`render_json`"), and rev. 4's ruling cites "mutation row 15" — a row that existed nowhere.
The effective table is therefore fifteen rows, written out here so a future reader is not
reconstructing it from three revision blocks. Row 11's mutation target also had to move:
rev. 4 replaced the "steady state" ceiling prose it named, along with the test that pinned
it. Rows are identified by test NAME where rev. 2's numbering no longer resolves.

| # | Mutation | Predicted RED | Observed RED (2026-08-10) |
|---|---|---|---|
| 1 | Delete the window-floor check entirely | 4, 5 | 4; **11**, not 5 — see below |
| 2 | Change the window floor to `<=` | 5 | 5, **+ 10** (`saturation_fixture`'s window is exactly `MIN_MEASURED_WINDOW`) |
| 3 | Swap the two floors' order | 6 | 6 |
| 4 | Drop the `at()` prefix from the ratio reason | 11 | 11 |
| 5 | Use `per_thread.iter().sum()` instead of the crosscheck's fallback on disagreement | 7 | 7, **+ 11** (the three-reason arm drops to two) |
| 6 | Replace `records_per_request * throughput` with a hardcoded `2.0 * throughput` | 2 (the invoicing arm) | 2 |
| 7 | Change `saturated` to `>=` | 10 | 10 |
| 8 | Add a local `if !device_ratio.is_finite() { return ... }` guard | 12 | 12, **+ `a_non_finite_bracket_sample_…`** |
| 9 | Sort `w.latencies_us` in place via a `&mut` parameter, or drop the sort | 13 | 13 |
| 10 | Push the ratio reason before the heartbeat reason | the fixed-order assertion in 11 | 11 |
| 11 | **Rev. 4 adaptation:** drop the ack-after-durable clause from the ceiling prose (rev. 2's "steady state" wording no longer exists) | `the_ceiling_refusal_names_the_ack_after_durable_argument` | that test |
| 12 | **Rev. 3 row:** bracket `max` → `min` | `the_ratio_gate_divides_by_the_bracket_max_not_the_min_or_before` | that test, **+ `an_under_measured_single_bracket_sample_…`** |
| 13 | **Rev. 3 row:** bracket `max` → `before` alone | same | that test, **+ `a_non_finite_bracket_sample_…`** |
| 14 | **Rev. 3 row:** drop a published bracket field from the wire arm of `render_json` (and its argument, so the round is not a compile error) | `json_shape_is_pinned` | that test, **+ `the_wire_bracket_sample_fields_are_scanned_for_non_finite`** |
| 15 | **Rev. 4 row:** move the gate back onto `device_ratio` — the change rev. 4's ruling forbids | `a_derived_record_rate_above_the_ceiling_does_not_flag_by_itself` | that test, **+ 8, + 12's max-fixture, + `an_under_measured_…`** |

A compile error is not a RED for an assertion (the T5 finding: novelty is not teeth).
Row 9's "drop the sort" half is the one to run if the `&mut` half will not compile.

**Row 1's predicted RED was wrong, and the row still has teeth.** Test 5
(`the_window_floor_admits_a_window_exactly_at_the_minimum`) asserts that the boundary
window **publishes**; deleting the floor makes it publish more readily, so that test cannot
detect this mutation in principle. What actually reddens beside test 4 is
`every_reason_carries_its_connection_count`'s own window-floor arm. Recorded rather than
quietly corrected, because a predicted-RED cell that names a test which cannot fail is the
same vacuity trap this sub-project keeps hitting — here it was the prediction that was
vacuous, not the test.

- [ ] **Step 8: Implement `wire.rs`** — the driver, which makes no arithmetic decision of its own

```rust
//! Layer 4 (spec §2, §4): real HTTP/1.1 keep-alive load against a spawned
//! child binary — the number the README claim is about. Closed-loop, with
//! coordinated-omission and client-saturation disclosed in the artifact.
//! Edge tooling outside the deterministic core (DESIGN §1).
//!
//! This module keeps the child process, the thread pool, the Heartbeat and
//! the two clocks, fills a `WireObs` per point, and makes no published
//! arithmetic decision of its own — every one of those lives in
//! `wire_accounting` (interlude spec §5).
//!
//! **This layer's window**: `--warmup` then `--duration`, both read, unlike
//! layer 3 which reads neither. Each point gets a fresh child and a fresh
//! data dir, so store growth under one connection count cannot confound the
//! next; the child's own startup costs sit before the warmup and are excluded
//! from the measured window by construction.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use crate::client::Conn;
use crate::measure::{process_cpu, Heartbeat};
use crate::report::WireReport;
use crate::wire_accounting::{point_accounting, WireObs};
use crate::SLOTS;
use std::io;
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const SPAWN_TIMEOUT: Duration = Duration::from_secs(10);

pub enum Target {
    BenchApp,
    Invoicing,
}

impl Target {
    pub fn name(&self) -> &'static str {
        match self {
            Target::BenchApp => "bench-app",
            Target::Invoicing => "invoicing",
        }
    }

    /// Log records committed per acked request, by construction — see
    /// `wire_accounting`'s module doc for why this layer derives its record
    /// rate instead of counting it. bench-app's canonical txn commits one
    /// effects record and its Bump's Done-mark commits another (jobs ≡ acked
    /// requests is the platform's property-tested dual-write invariant), so
    /// 2.0 at steady state; invoicing's create_customer is get + put only —
    /// one effects record, no job (routes.rs:53-71, verified).
    fn records_per_request(&self) -> f64 {
        match self {
            Target::BenchApp => 2.0,
            Target::Invoicing => 1.0,
        }
    }

    /// Per-thread setup returning the headers every request carries. `_conn`
    /// is unused only until T8's Invoicing arm logs in through it.
    fn session_setup(&self, _conn: &mut Conn) -> io::Result<Vec<(String, String)>> {
        match self {
            Target::BenchApp => Ok(Vec::new()),
            Target::Invoicing => unreachable!("wired in T8"),
        }
    }

    fn one_request(
        &self,
        conn: &mut Conn,
        headers: &[(String, String)],
        ids: &AtomicU64,
    ) -> io::Result<(u16, Vec<u8>)> {
        match self {
            Target::BenchApp => {
                let id = ids.fetch_add(1, Ordering::Relaxed) % SLOTS;
                conn.request("POST", &format!("/hit/{id}"), headers, b"")
            }
            Target::Invoicing => unreachable!("wired in T8"),
        }
    }
}

/// Locate a built release binary via cargo's JSON output — the
/// edge-harness/src/child.rs shape (plain string scan, no JSON dep).
pub fn build_bin(package: &str, bin: &str) -> io::Result<PathBuf> {
    let root = workspace_root();
    let output = Command::new("cargo")
        .args(["build", "--release", "-p", package, "--bins", "--message-format=json"])
        .current_dir(&root)
        .output()?;
    if !output.status.success() {
        return Err(io::Error::other(format!(
            "cargo build -p {package} failed:\n{}",
            String::from_utf8_lossy(&output.stderr)
        )));
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    for line in stdout.lines() {
        if !line.contains("\"reason\":\"compiler-artifact\"") {
            continue;
        }
        if let Some(exe) = extract_field(line, "\"executable\":\"") {
            if Path::new(&exe).file_name().is_some_and(|n| n == bin) {
                return Ok(PathBuf::from(exe));
            }
        }
    }
    Err(io::Error::other(format!("no executable named {bin:?} in cargo's output for {package}")))
}

fn extract_field(line: &str, prefix: &str) -> Option<String> {
    let start = line.find(prefix)? + prefix.len();
    let end = line[start..].find('"')? + start;
    Some(line[start..end].to_string())
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("bench-harness lives at crates/bench-harness inside the charpente workspace")
        .to_path_buf()
}

pub fn free_addr() -> String {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("loopback bind")
        .local_addr()
        .expect("bound listener has an address")
        .to_string()
}

struct BenchChild {
    child: Child,
}

impl Drop for BenchChild {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn spawn_target(bin: &Path, addr: &str, dir: &str) -> io::Result<BenchChild> {
    // Child stdio tees to sibling files of the data dir (never inside it —
    // the data dir is charpente's), the edge-harness ChildNode::spawn
    // discipline: a spawn failure must be self-diagnosing. Stdio::null()
    // here once hid an argv-contract exit 2 behind an opaque "exited during
    // spawn" (plan-review C-1). Siblings are cleaned on point success and
    // deliberately left behind on failure.
    let out = std::fs::File::create(format!("{dir}.child.out"))?;
    let err_path = format!("{dir}.child.err");
    let err = std::fs::File::create(&err_path)?;
    let child = Command::new(bin)
        .args(["--addr", addr, "--data-dir", dir])
        .stdout(Stdio::from(out))
        .stderr(Stdio::from(err))
        .spawn()?;
    let mut node = BenchChild { child };
    let deadline = Instant::now() + SPAWN_TIMEOUT;
    loop {
        if TcpStream::connect(addr).is_ok() {
            return Ok(node);
        }
        if let Some(status) = node.child.try_wait()? {
            let stderr = std::fs::read_to_string(&err_path).unwrap_or_default();
            return Err(io::Error::other(format!(
                "target exited during spawn: {status}\nchild stderr ({err_path}):\n{stderr}"
            )));
        }
        if Instant::now() > deadline {
            return Err(io::Error::other(format!("target never answered on {addr}")));
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

pub fn sweep(
    bin: &Path,
    target: Target,
    connections: &[usize],
    warmup: Duration,
    duration: Duration,
    device_ceiling: f64,
) -> io::Result<(WireReport, Vec<String>)> {
    let target = Arc::new(target);
    let mut points = Vec::new();
    let mut invalid = Vec::new();
    for &n in connections {
        match sweep_point(bin, &target, n, warmup, duration, device_ceiling) {
            Ok((point, mut point_invalid)) => {
                invalid.append(&mut point_invalid);
                points.push(point);
            }
            // Fail-soft (plan-review I-4, the spec §5 posture): one lost
            // point — a spawn hiccup, a timeout at 256 conns, a non-200, a
            // window the accounting refused — must not discard the points
            // already measured. The report keeps what was measured and names
            // what was lost; a non-empty `invalid` still exits 1, so a lost
            // point can never pass silently.
            Err(e) => invalid.push(format!("[{n} conns] point lost: {e}")),
        }
    }
    Ok((WireReport { target: target.name().to_string(), points }, invalid))
}

fn sweep_point(
    bin: &Path,
    target: &Arc<Target>,
    n: usize,
    warmup: Duration,
    duration: Duration,
    device_ceiling: f64,
) -> io::Result<(crate::report::SweepPoint, Vec<String>)> {
    // A fresh child + data dir per point: store growth from one point
    // must not confound the next.
    let dir = point_dir(target.name(), n);
    let addr = free_addr();
    let node = spawn_target(bin, &addr, &dir)?;

    let measuring = Arc::new(AtomicBool::new(false));
    let stop = Arc::new(AtomicBool::new(false));
    let grand = Arc::new(AtomicU64::new(0));
    let ids = Arc::new(AtomicU64::new(0));
    let mut workers = Vec::new();
    for _ in 0..n {
        let (addr, target) = (addr.clone(), target.clone());
        let (measuring, stop, grand, ids) =
            (measuring.clone(), stop.clone(), grand.clone(), ids.clone());
        workers.push(std::thread::spawn(move || -> io::Result<(u64, Vec<u64>)> {
            let mut conn = Conn::connect(&addr)?;
            let headers = target.session_setup(&mut conn)?;
            let mut count = 0u64;
            let mut lat = Vec::new();
            while !stop.load(Ordering::Relaxed) {
                let t0 = Instant::now();
                let (status, body) = target.one_request(&mut conn, &headers, &ids)?;
                if status != 200 {
                    let shown: String = String::from_utf8_lossy(&body).chars().take(256).collect();
                    return Err(io::Error::other(format!(
                        "request failed with HTTP {status}: {shown}"
                    )));
                }
                if measuring.load(Ordering::Relaxed) {
                    lat.push(t0.elapsed().as_micros() as u64);
                    count += 1;
                    grand.fetch_add(1, Ordering::Relaxed);
                }
            }
            Ok((count, lat))
        }));
    }

    std::thread::sleep(warmup);
    let hb = Heartbeat::start();
    let cpu0 = process_cpu();
    let start = Instant::now();
    measuring.store(true, Ordering::Relaxed);
    std::thread::sleep(duration);
    measuring.store(false, Ordering::Relaxed);
    let elapsed = start.elapsed();
    let client_cpu = process_cpu() - cpu0;
    stop.store(true, Ordering::Relaxed);
    // `Heartbeat` has no `Drop`; finish it before any `?` below can return
    // early, or a 10 ms tick thread leaks for the life of the process
    // (T6 review, M-3).
    let heartbeat = hb.finish();

    let mut per_thread = Vec::new();
    let mut latencies_us = Vec::new();
    let mut joined = Ok(());
    for w in workers {
        match w.join().map_err(|_| io::Error::other("a client thread panicked")) {
            Ok(Ok((count, lat))) => {
                per_thread.push(count);
                latencies_us.extend(lat);
            }
            // Join every worker before propagating: an early `?` here leaves
            // the rest of the pool detached and racing the child's kill.
            Ok(Err(e)) | Err(e) => {
                if joined.is_ok() {
                    joined = Err(e);
                }
            }
        }
    }
    drop(node);
    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::remove_file(format!("{dir}.child.out"));
    let _ = std::fs::remove_file(format!("{dir}.child.err"));
    joined?;

    let obs = WireObs {
        connections: n,
        per_thread,
        grand: grand.load(Ordering::Relaxed),
        latencies_us,
        elapsed,
        client_cpu,
        device_ceiling,
        records_per_request: target.records_per_request(),
        heartbeat,
    };
    let a = point_accounting(&obs).map_err(io::Error::other)?;
    Ok((a.point, a.invalid))
}

pub fn point_dir(target: &str, conns: usize) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock after 1970")
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "bench-wire-{target}-{conns}-{}-{nanos}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).expect("scratch dir creates");
    dir.to_string_lossy().into_owned()
}
```

Add `pub mod client;`, `pub mod wire;` and `pub mod wire_accounting;` to `lib.rs`.

Two things in the above are corrections to rev. 1's driver, not relocation, and the
report should say so:

- **`records_per_request` is now the target's own method feeding an observation field**,
  not a multiply performed in the driver. The driver reads the constant; the accounting
  does the arithmetic.
- **Workers are all joined before any error propagates.** Rev. 1's `w.join()...??` inside
  the loop returned on the first failing worker, leaving the rest of the pool detached and
  racing `drop(node)`'s `kill`. The first error is still the one reported.

- [ ] **Step 9: Wire the subcommand arm** (in `run_layers`)

```rust
        "wire" => {
            let target = match args.target.as_str() {
                "bench-app" => bench_harness::wire::Target::BenchApp,
                // T8 fills the Invoicing arms. Until then this refuses by
                // name: `--target invoicing` is advertised in the usage
                // string, so reaching the internal `unreachable!` through a
                // documented flag value would be a panic where a named
                // remedy belongs (hard rule 5).
                "invoicing" => {
                    return Err(std::io::Error::other(
                        "--target invoicing is not wired yet (bench-harness Task 8) — \
                         use --target bench-app",
                    ))
                }
                other => return Err(std::io::Error::other(format!("unknown --target {other:?}"))),
            };
            let (package, bin) = match &target {
                bench_harness::wire::Target::BenchApp => ("bench-harness", "bench-app"),
                bench_harness::wire::Target::Invoicing => ("invoicing", "node"),
            };
            let ceiling = report.device.as_ref().expect("device ran first").syncs_per_sec;
            let bin = bench_harness::wire::build_bin(package, bin)?;
            let (wire, invalid) = bench_harness::wire::sweep(
                &bin, target, &args.connections, args.warmup, args.duration, ceiling,
            )?;
            report.wire.push(wire);
            report.invalid.extend(invalid);
            true
        }
```

- [ ] **Step 10: Add the ignored smoke** (in `tests/harness.rs`)

Measure a real device ceiling first — the T6 review's I-5 ruled out a smoke that hands a
layer a made-up ceiling, because the ratio assertion then compares a number with itself.

```rust
#[test]
#[ignore]
fn wire_layer_sweeps_the_bench_app() {
    // `scratch(tag) -> String` is tests/harness.rs's own helper (harness.rs:7),
    // not bin/bench.rs's two-argument `temp_dir`.
    let dir = scratch("wire-device");
    let device = bench_harness::device::run(&dir, Duration::from_millis(200), Duration::from_secs(1))
        .expect("device probe runs");
    let _ = std::fs::remove_dir_all(&dir);

    let bin = bench_harness::wire::build_bin("bench-harness", "bench-app").expect("bin builds");
    let (wire, invalid) = bench_harness::wire::sweep(
        &bin,
        bench_harness::wire::Target::BenchApp,
        &[1, 4],
        Duration::from_millis(300),
        Duration::from_secs(2),
        device.syncs_per_sec,
    )
    .expect("sweep runs");
    assert!(invalid.is_empty(), "short healthy run must be valid: {invalid:?}");
    assert_eq!(wire.points.len(), 2);
    for p in &wire.points {
        assert!(p.throughput_per_sec > 0.0);
        // The derivation, asserted rather than assumed (T6 review, I-5: a
        // `> 0.0` on a number the caller supplied is not an assertion).
        assert_eq!(p.log_records_per_sec, 2.0 * p.throughput_per_sec);
        assert_eq!(p.device_ratio, p.log_records_per_sec / device.syncs_per_sec);
        assert!(p.p50_us <= p.p99_us && p.p99_us <= p.p999_us, "percentile ordering");
    }
}
```

The `--duration` is 2s, not rev. 1's 1s: `MIN_MEASURED_WINDOW` is 1s and a smoke that
sits exactly on a refusal boundary is a flake waiting for a slow CI box.

Note for the implementer: this smoke runs `cargo build` for `bench-harness`'s own bins
from inside a `bench-harness` test. The outer cargo has released the target-dir lock by
the time tests run, and the edge-harness precedent does the same across packages — but
if it blocks, that is the known cargo target-dir lock and not a harness bug. The test is
`#[ignore]`, so it only runs under `scripts/bench.sh`.

- [ ] **Step 11: Verify**

**The counts below were written at rev. 2 and were never updated as rev. 3 and rev. 4 added
tests; corrected at execution time (2026-08-10) with the derivation shown, per this step's
own "say so rather than restating the prediction" instruction.** The rev.-2 line read
"13 accounting tests", "lib 59 → 74" and "658 → 673 passed, 38 → 39 ignored". Rev. 3 added
three accounting tests plus one report test, and one further accounting test
(`a_non_finite_bracket_sample_…`); rev. 4 added the regression test
`a_derived_record_rate_above_the_ceiling_does_not_flag_by_itself`. So the accounting module
is 13 + 5 = **18**, and the lib delta is 2 client + 18 accounting + 2 report = **22**, not
16. Note also that rev. 2's own arithmetic did not close: 59 + 16 is 75, and it predicted 74.

Run: `cargo test -p bench-harness client` → PASS (2 tests).
Run: `cargo test -p bench-harness --lib wire_accounting` → PASS (18 tests).
Run: `cargo test -p bench-harness` → lib 59 → 81, bin 15, app 1, 8 ignored, 0 failed.
Run: `cargo test -p bench-harness --test harness -- --ignored --test-threads=1` → 8/8.
Run: `cargo test --workspace` twice → 658 → 680 passed, 38 → 39 ignored; reconcile the
delta exactly (2 client + 18 accounting + 2 report = 22 lib; if the arithmetic does not
close, say so rather than restating the prediction).
Run: `cargo fmt --all --check`; `cargo clippy --workspace --all-targets -- -D warnings`;
`cargo clippy -p bench-harness --all-targets --target x86_64-unknown-linux-gnu -- -D warnings`;
`cargo build -p guide-examples`.
Run: `git diff --stat -- crates/charpente-core crates/charpente` → **empty** (Global Constraint).
Run: `cargo run -p bench-harness --bin bench -- wire --connections 1,4 --duration 2 --warmup 0.5 --json`
→ JSON has `"wire":[{"target":"bench-app",...,"device_ratio":...}]`, exit 0.
Run: `cargo run -p bench-harness --bin bench -- wire --target invoicing` → exit 1, the named refusal.
Run: `cargo run -p bench-harness --bin bench -- wire --connections 1 --duration 0.1 --warmup 0`
→ exit 1, refused by a floor rather than published.

- [ ] **Step 12: Ledger + commit**

Append the Task 7 entry to the ledger, then:

```bash
git add crates/bench-harness docs/superpowers/plans/2026-08-06-benchmark-harness.md
git commit -m "bench: wire layer — keep-alive client, bench-app vehicle, connection sweep (T7)"
```

---

### Task 8: the invoicing wire target

**Handoff from T7 rev. 2 — do not miss this.** T7 ships a named CLI refusal for
`--target invoicing` in `bin/bench.rs`'s `wire` arm (its Step 9), because reaching the
internal `unreachable!("wired in T8")` through a flag the usage string advertises would be
a panic where a named remedy belongs. **T8 must delete that refusal arm** and let
`"invoicing"` construct `Target::Invoicing` — otherwise this task's own smoke passes
in-process while the shipped binary still refuses the target it just wired. T7's Step 11
pins the refusal, so that test is the one that must be updated here, and its update is the
evidence the handoff was taken.

Also note `session_setup`'s parameter is named `_conn` as T7 ships it (the `BenchApp` arm
does not use it); Step 1 below renames it to `conn`.

**Ruling (Noah, 2026-08-10) — T7 rev. 4's ack-rate gate has no margin for a
one-record-per-request target, and the smoke tolerates it rather than the gate being
weakened.** T8's first green run exposed what rev. 4 could not have seen with only one
target: invoicing commits ONE log record per request, so its acked-request rate and the
device ceiling are the same physical quantity, the gate sits at exactly 1.0x, and it fired
on roughly one healthy run in three (0.95x, 0.99x, 1.03x, 1.05x across runs).

**Measured on an Apple M4 Pro / apfs / `F_FULLFSYNC`, because the first ruling was to fix
the denominator and the numbers refuted it:** the raw fsync bracket reads 219–246/s; a
`Wal::append` + `Wal::sync` loop — the exact pair an acked request pays for, built and
measured for this ruling — reads 224–231/s, i.e. **the same number or lower, not higher**;
a bare in-process `Db::transact` loop reads 246/s, **1.00x the ceiling with no HTTP at
all**; and this target reads 237–257/s. The commit path is entirely fsync-bound, so all
four measure one physical quantity to within each one's own ±5%. **There is no measurement
path meaningfully faster than the thing being bounded**, so relocating the ceiling cannot
fix this; the WAL sampler was written, measured, and reverted.

rev. 4's "it has no false-alarm path" was established at ≈0.5x on bench-app alone — the
only target that existed then, and the one whose 2.0 records-per-request buys a 2x margin.
That is the same shape of error rev. 4 itself diagnosed in rev. 3: validating a gate where
it has headroom and generalizing to where it has none.

**The resolution is the precedent this crate already set one layer down.**
`core_layer_commits_and_reports_a_ratio` faced exactly this and deliberately does not
assert `invalid.is_empty()`; it tolerates only the named ratio reason, on the rationale
that "a gate that never fires on a short window would be the bug." T8's invoicing smoke
does the same, pinned to the two substrings unique to the ack-gate reason. **The gate
itself is unchanged and unweakened** — `implausible_ratio`, `DEVICE_RATIO_SLACK` and the
refusal prose are all untouched, and it still catches a genuinely impossible number.
bench-app's smoke keeps its strict `invalid.is_empty()`, because it has the margin.

**Files:**
- Modify: `crates/bench-harness/src/wire.rs` (fill the two `Invoicing` arms)
- Modify: `crates/bench-harness/src/bin/bench.rs` (delete T7's `--target invoicing` refusal)
- Modify: `crates/bench-harness/tests/harness.rs` (add smoke)

**Interfaces:**
- Consumes: invoicing's routes (verified): `POST /login` (no auth, body = bare JSON string subject, response = bare JSON string token), `POST /customers/:id` (auth; body `{"name": String, "email": String}`; refuses an existing id with 400 `"customer {id} already exists"`); the session cookie name `charpente_session`.
- Produces: a working `--target invoicing` wire run.

- [ ] **Step 1: Fill the `Invoicing` arms in `wire.rs`**

```rust
    fn session_setup(&self, conn: &mut Conn) -> io::Result<Vec<(String, String)>> {
        match self {
            Target::BenchApp => Ok(Vec::new()),
            Target::Invoicing => {
                let (status, body) = conn.request("POST", "/login", &[], b"\"bench\"")?;
                if status != 200 {
                    return Err(io::Error::other(format!(
                        "invoicing login failed with {status}: {}",
                        String::from_utf8_lossy(&body)
                    )));
                }
                // The response body is a bare JSON string: the raw session token.
                let token = String::from_utf8_lossy(&body).trim_matches('"').to_string();
                Ok(vec![("cookie".to_string(), format!("charpente_session={token}"))])
            }
        }
    }

    fn one_request(
        &self,
        conn: &mut Conn,
        headers: &[(String, String)],
        ids: &AtomicU64,
    ) -> io::Result<(u16, Vec<u8>)> {
        match self {
            Target::BenchApp => {
                let id = ids.fetch_add(1, Ordering::Relaxed) % SLOTS;
                conn.request("POST", &format!("/hit/{id}"), headers, b"")
            }
            Target::Invoicing => {
                // create_customer refuses an existing id, so ids never wrap:
                // every request creates a fresh row (the realistic-app shape —
                // insert-heavy, spec §3).
                let id = ids.fetch_add(1, Ordering::Relaxed) + 1;
                conn.request(
                    "POST",
                    &format!("/customers/{id}"),
                    headers,
                    b"{\"name\":\"Bench Load\",\"email\":\"bench@example.com\"}",
                )
            }
        }
    }
```

(Remove the two `unreachable!("wired in T8")` placeholders; nothing else changes.)

- [ ] **Step 2: Add the ignored smoke** (in `tests/harness.rs`)

```rust
#[test]
#[ignore]
fn wire_layer_drives_invoicing_through_login() {
    let bin = bench_harness::wire::build_bin("invoicing", "node").expect("node builds");
    let (wire, invalid) = bench_harness::wire::sweep(
        &bin,
        bench_harness::wire::Target::Invoicing,
        &[2],
        Duration::from_millis(300),
        Duration::from_secs(1),
    )
    .expect("sweep runs");
    assert!(invalid.is_empty(), "short healthy run must be valid: {invalid:?}");
    assert_eq!(wire.points.len(), 1);
    assert!(wire.points[0].throughput_per_sec > 0.0);
}
```

- [ ] **Step 3: Verify**

Run: `cargo test -p bench-harness --test harness -- --ignored --test-threads=1` → PASS
(**9** tests, not the 5 written here at rev. 1 — T4/T5/T6/T7 each added smokes; corrected at
execution time, 2026-08-10, per the same "reconcile rather than restate" instruction Step 11
of Task 7 carries).
Run: `cargo run -p bench-harness --bin bench -- wire --target invoicing --connections 1,4 --duration 2 --warmup 0.5` → human summary shows `wire [invoicing]`, exit 0.

Note on that second command's exit code, given the ruling above: on a 1-record-per-request
target the ack-rate gate legitimately fires on some healthy runs, and a non-empty `invalid`
exits 1 by design. Exit 0 is the common case, not a guarantee, and an exit 1 whose only
reason is the named ack-rate one is not a regression. Any other reason is.

- [ ] **Step 4: Ledger + commit**

Append the Task 8 entry to the ledger, then:

```bash
git add crates/bench-harness
git commit -m "bench: invoicing wire target — login flow + create_customer load (T8)"
```

---

### Task 9: `all`/`smoke` subcommands, forced INVALID arms, `scripts/bench.sh`

**Files:**
- Modify: `crates/bench-harness/src/bin/bench.rs` (add `all` + `smoke` arms)
- Modify: `crates/bench-harness/src/wire.rs` (make `SATURATION_FRACTION` `pub`)
- Modify: `crates/bench-harness/tests/harness.rs` (forced-arm tests + binary-driving smokes)
- Create: `scripts/bench.sh` (mode `+x`)

**Interfaces:**
- Consumes: everything from T4–T8.
- Produces: `bench all` (device + core + kernel + wire×2, one assembled report), `bench smoke` (= `all` at 1 s durations, 2000 requests, connections 1,4), `scripts/bench.sh` / `scripts/bench.sh --smoke`.

Note: `all` is a sixth subcommand beyond spec §1's original five, now recorded there by
amendment — assembly lives in Rust because JSON is hand-rolled in this crate; merging JSON
fragments in bash without a JSON library would be worse.

- [ ] **Step 1: Add the forced INVALID-arm tests** (spec §7: each arm proven by construction, not luck)

In `tests/harness.rs`:

```rust
#[test]
fn a_starved_heartbeat_flags_invalid_through_its_real_path() {
    // Not #[ignore]: threads only, no ports/processes/fs. Holding the gate
    // starves the tick thread through the exact mutex it takes every tick.
    let hb = bench_harness::measure::Heartbeat::start();
    let gate = hb.gate();
    let held = gate.lock().expect("test holds the heartbeat gate");
    std::thread::sleep(Duration::from_millis(300));
    drop(held);
    let verdict = hb.finish().expect("a starved heartbeat must flag the run");
    assert!(verdict.contains("INVALID"), "got: {verdict}");
    // AMENDED at Task 4's re-review (2026-08-07, finding I-A): this rig
    // starves the heartbeat thread while the process stays AWAKE, which is
    // the STARVATION arm, not the suspension arm. T4's I-5 fix split the two
    // and reworded this reason accordingly, so asserting "suspended" here
    // now fails. Assert the arm this test actually exercises.
    assert!(verdict.contains("starved"), "got: {verdict}");
}

#[test]
fn a_saturated_client_is_flagged_on_the_point() {
    // The saturation rule is arithmetic on measured CPU (spec §4); force it
    // by construction: busy_fraction 1.0 > the 0.9 threshold.
    let p = bench_harness::report::SweepPoint {
        connections: 1,
        throughput_per_sec: 1.0,
        log_records_per_sec: 2.0,
        p50_us: 1,
        p99_us: 1,
        p999_us: 1,
        client_busy_fraction: 1.0,
        saturated: 1.0 > bench_harness::wire::SATURATION_FRACTION,
    };
    assert!(p.saturated, "a fully-busy client must mark its point saturated");
    let human = bench_harness::report::render_human(&bench_harness::report::Report {
        hardware: "h".into(), os: "o".into(), cores: 1,
        sync_semantics: "s", warmup_secs: 0.0, duration_secs: 0.0,
        device: Some(bench_harness::report::DeviceReport { syncs_per_sec: 1.0, appends_per_sec: 1.0 }),
        core: None, kernel: None,
        wire: vec![bench_harness::report::WireReport { target: "t".into(), points: vec![p] }],
        invalid: vec![],
    });
    assert!(human.contains("CLIENT-SATURATED"), "got: {human}");
}
```

Make `SATURATION_FRACTION` `pub` in `wire.rs` (it is consumed here and documented in BENCH.md).

Also add three `#[ignore]`d tests to `tests/harness.rs` (they spawn processes, so they run
only via `scripts/bench.sh --smoke`). The first two close spec §7 bullet 1's real gap: every
other smoke calls library fns — nothing else drives the `bench` binary's parse/dispatch/
exit-code contract, so a regression there would otherwise be invisible (plan-review C-2).
The third forces the saturation INVALID arm through `wire::sweep`'s REAL CPU arithmetic, not
a hand-built literal (plan-review I-4) — the unit test above proves rendering; this one
proves the classification:

```rust
#[test]
#[ignore]
fn the_bench_binary_smokes_end_to_end() {
    // Spec §7 bullet 1: drive the real `bench` binary — argv parse, dispatch,
    // exit-code contract, JSON emission. build_bin builds all of the
    // package's bins, `bench` included.
    let bin = bench_harness::wire::build_bin("bench-harness", "bench").expect("bench builds");
    let out = std::process::Command::new(&bin)
        .args(["smoke", "--json"])
        .output()
        .expect("bench smoke runs");
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        out.status.success(),
        "bench smoke must exit 0 on a healthy box\nstderr:\n{stderr}\nstdout:\n{stdout}"
    );
    for key in ["\"bench\":{", "\"device\":{", "\"core\":{", "\"kernel\":{", "\"wire\":["] {
        assert!(stdout.contains(key), "smoke JSON missing {key}: {stdout}");
    }
    assert!(
        stdout.contains("\"duration_secs\":1.00"),
        "smoke's forced 1 s duration must reach the report: {stdout}"
    );
}

#[test]
#[ignore]
fn a_bogus_subcommand_is_a_usage_error() {
    let bin = bench_harness::wire::build_bin("bench-harness", "bench").expect("bench builds");
    let out = std::process::Command::new(&bin)
        .arg("bogus")
        .output()
        .expect("bench bogus runs");
    assert_eq!(out.status.code(), Some(2), "usage errors exit 2");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("usage: bench"), "got: {stderr}");
}

#[test]
#[ignore]
fn a_saturated_client_is_flagged_through_the_real_sweep() {
    // Force client_busy_fraction through wire::sweep's real formula
    // (cpu / (n * elapsed)): two spinner threads burn this process's CPU
    // during a 1-conn sweep, driving the measured fraction to ~2 > 0.9.
    let bin = bench_harness::wire::build_bin("bench-harness", "bench-app").expect("bin builds");
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let spinners: Vec<_> = (0..2)
        .map(|_| {
            let stop = stop.clone();
            std::thread::spawn(move || {
                let mut x = 0u64;
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                }
                std::hint::black_box(x);
            })
        })
        .collect();
    let result = bench_harness::wire::sweep(
        &bin,
        bench_harness::wire::Target::BenchApp,
        &[1],
        Duration::from_millis(200),
        Duration::from_secs(1),
    );
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    for s in spinners {
        let _ = s.join();
    }
    let (wire, _invalid) = result.expect("sweep runs");
    assert!(
        wire.points[0].saturated,
        "2 spinner threads over a 1-conn sweep must saturate the client: busy fraction {}",
        wire.points[0].client_busy_fraction
    );
}
```

- [ ] **Step 2: Run them** — `cargo test -p bench-harness --test harness` → the two new forced-arm tests PASS un-ignored (the rest stay ignored; the three new ignored tests run in Step 5's smoke).

- [ ] **Step 3: Add the `all` and `smoke` arms** — replace the ENTIRE existing `run_layers`
function (the per-arm dispatch T5–T8 accumulated, including its standalone `"wire"` arm —
delete those arms wholesale, leave no dead code behind) with this literal function. Two
things are deliberate: the smoke-parameter selection happens BEFORE the device run, so
`bench smoke`'s ceiling loops are 1 s too (spec §7 says ~1 s durations; selecting after the
device run would silently leave it at the 5 s + 30 s defaults — over a minute of device
loops in a "smoke"), and the report's `warmup_secs`/`duration_secs` are overwritten with
the selected values so the artifact states what actually ran:

> **AMENDED at Task 4's re-review (2026-08-07, finding I-A). The literal below was
> rewritten; the version this plan originally carried is pre-Task-4 and pasting it would
> silently revert four shipped fixes in one action** — I-1's dir coupling (its own
> `temp_dir("device")` re-created the measurement dir *inside* `run_layers`, so the loops
> and `main`'s `filesystem()` label diverged again, which is the second half of the very
> defect I-1 was raised for — and that one is **not a compile error**), M-5 (cleanup ran
> after `?`, so an erroring device run leaked its scratch dir), M-7 (returning `()` loses
> the `implemented` signal that keeps an unimplemented subcommand off exit 0), and I-4's
> per-arm heartbeat comment. The compiler catches the arity change but does **not**
> constrain the direction of the repair. Adapt the accumulated arms into this shape; do
> not restore the old signature.

```rust
fn run_layers(args: &Args, dir: &str, report: &mut Report) -> std::io::Result<bool> {
    // smoke = the full matrix at forced-short parameters (spec §7). Selected
    // BEFORE the device run so the ceiling loops are short too.
    let (warmup, duration, requests, connections) = if args.cmd == "smoke" {
        (Duration::from_millis(200), Duration::from_secs(1), 2000, vec![1usize, 4])
    } else {
        (args.warmup, args.duration, args.requests, args.connections.clone())
    };
    report.warmup_secs = warmup.as_secs_f64();
    report.duration_secs = duration.as_secs_f64();

    // Every subcommand carries the ceiling (spec §5): device runs first
    // unconditionally, so no layer can render without it. `dir` is created and
    // probed by `main` (T4 review, I-1) — the directory measured here and the
    // one `filesystem()` labelled the report with MUST be the same one, so it
    // is passed in, never re-created here. This Heartbeat covers ONLY the
    // device measurement; each later layer starts and finishes its own around
    // its own measured window (T4 review, I-4), which is why there is no
    // single heartbeat spanning the whole process — under one, `bench all`'s
    // guard would span `wire::build_bin`'s cargo build and fire spuriously.
    let hb = Heartbeat::start();
    let result = device::run(dir, warmup, duration);
    let _ = std::fs::remove_dir_all(dir); // T4 review, M-5: clean up on Err too.
    report.device = Some(result?);
    if let Some(reason) = hb.finish() {
        report.invalid.push(reason);
    }

    let cmds: Vec<&str> = match args.cmd.as_str() {
        "device" => vec![],
        "all" | "smoke" => vec!["core", "kernel", "wire-bench-app", "wire-invoicing"],
        "wire" => vec![match args.target.as_str() {
            "bench-app" => "wire-bench-app",
            "invoicing" => "wire-invoicing",
            other => return Err(std::io::Error::other(format!("unknown --target {other:?}"))),
        }],
        single => vec![single],
    };
    for cmd in cmds {
        match cmd {
            "core" => {
                let dir = temp_dir("core", args.dir.as_deref()).map_err(std::io::Error::other)?;
                let ceiling = report.device.as_ref().expect("device ran first").syncs_per_sec;
                let (core, invalid) =
                    bench_harness::core_bench::run(&dir, warmup, duration, ceiling)?;
                let _ = std::fs::remove_dir_all(&dir);
                report.core = Some(core);
                report.invalid.extend(invalid);
            }
            "kernel" => {
                let dir = temp_dir("kernel", args.dir.as_deref()).map_err(std::io::Error::other)?;
                let (kernel, invalid) = bench_harness::kernel_bench::run(&dir, requests)?;
                let _ = std::fs::remove_dir_all(&dir);
                report.kernel = Some(kernel);
                report.invalid.extend(invalid);
            }
            "wire-bench-app" | "wire-invoicing" => {
                let (package, bin, target) = if cmd == "wire-bench-app" {
                    ("bench-harness", "bench-app", bench_harness::wire::Target::BenchApp)
                } else {
                    ("invoicing", "node", bench_harness::wire::Target::Invoicing)
                };
                let bin = bench_harness::wire::build_bin(package, bin)?;
                let (wire, invalid) =
                    bench_harness::wire::sweep(&bin, target, &connections, warmup, duration)?;
                report.wire.push(wire);
                report.invalid.extend(invalid);
            }
            other => unreachable!("unmapped subcommand {other}"),
        }
    }
    // T4 review, M-7: `main` gates the exit code on this. By T9 every
    // subcommand has an arm, so this is `true` — the plumbing stays as the
    // structural guarantee that a future unmapped subcommand cannot exit 0.
    Ok(true)
}
```

- [ ] **Step 4: Create `scripts/bench.sh`**

```bash
#!/usr/bin/env bash
# The honest-benchmark instrument (M2 sub-project 1, spec
# docs/superpowers/specs/2026-08-06-benchmark-harness-design.md).
# Default: the full matrix — one assembled report; JSON to
# target/bench/report.json, human render to the terminal (stderr).
# --smoke: the harness's own #[ignore]d correctness checks.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
if [[ "${1:-}" == "--smoke" ]]; then
  exec cargo test -p bench-harness -- --ignored --test-threads=1
fi
mkdir -p target/bench
cargo run --release -p bench-harness --bin bench -- all --json "$@" > target/bench/report.json
echo "JSON report: target/bench/report.json" >&2
```

`chmod +x scripts/bench.sh`.

- [ ] **Step 5: Verify**

Run: `scripts/bench.sh --smoke` → all bench-harness tests pass (ignored + un-ignored).
Run: `cargo run -p bench-harness --bin bench -- smoke --json` → one assembled report with device+core+kernel+both wire targets, exit 0, JSON carries `"duration_secs":1.00` (the smoke override reached the report), and the whole run completes in well under 2 minutes wall — if a wire sweep alone takes 30 s+ per point, the shortened smoke parameters did not reach it: that is a bug in the Step 3 restructure, not a slow machine.

- [ ] **Step 6: Ledger + commit**

Append the Task 9 entry to the ledger, then:

```bash
git add crates/bench-harness scripts/bench.sh
git commit -m "bench: all/smoke assembly, forced INVALID arms, scripts/bench.sh (T9)"
```

---

### Task 10: `docs/BENCH.md` — the first numbers of record + close-out

**Files:**
- Create: `docs/BENCH.md`
- Modify: `CLAUDE.md` (Current state entry)
- Modify: `README.md` (one line, see Step 3)

**Interfaces:**
- Consumes: a full `scripts/bench.sh` run on the laptop (plugged in, lid open, nothing heavy running — the heartbeat guard will catch a sleep, not contention).

- [ ] **Step 1: Run the full matrix**

Run: `scripts/bench.sh` (expect ~10 minutes: 2 device loops + core + kernel + 2 wire sweeps × 5 points, each 5 s warmup + 30 s measure). Keep `target/bench/report.json`. If any INVALID appears — including a `point lost:` entry from the wire layer's fail-soft path (a lost point keeps the other points' data but still invalidates the run) — fix the environment (not the thresholds) and re-run. The kernel layer prints progress every ~5 s; if it goes quiet for ~2 minutes with no progress lines, Ctrl-C and sanity-check with `cargo run --release -p bench-harness --bin bench -- kernel --requests 5000` before committing to the full run.

- [ ] **Step 2: Write `docs/BENCH.md`**

Structure (prose from the spec's honesty contract; numbers from the report — no ```` ```rust ```` blocks):

```markdown
# Benchmarks

Honest numbers or none (PLAN's own risk rule). Method before results.

## Method
[The four layers and what each isolates — device/core/kernel/wire, one
paragraph each from spec §2. The closed-loop + coordinated-omission
disclosure and the client-saturation rule (>90% client busy invalidates a
point) from spec §4. The run-validity rules (heartbeat sleep-guard,
totals-two-ways) from spec §5. The canonical transaction defined (README's
get + put + enqueue + emit shape) and the two write metrics explained —
acked writes/sec vs log records/sec (≈2x at kernel/wire for the canonical
txn: the job Done-mark). State plainly that the wire layer's records/sec is
DERIVED per target (2x for bench-app via the dual-write invariant, 1x for
invoicing's get+put route), not separately counted — the client cannot see
the server's log.]

## Numbers of record — dev hardware (NOT the launch claim)
Hardware: [report.hardware, report.os, report.filesystem, report.cores cores]
Sync semantics: [report.sync_semantics] — on macOS every measured write is
F_FULLFSYNC-durable, which is drastically slower than Linux-server fsync;
these numbers UNDERSELL a server deliberately rather than oversell it.
Warmup [W]s, measure [D]s per point. Repro: `scripts/bench.sh`.

| layer | metric | value | repro |
[one row per number in the report, verbatim; the repro column carries the
layer's own one-command line, e.g.
`cargo run --release -p bench-harness --bin bench -- core --json`
(spec §9.2 — every row reproducible on its own)]

## The prediction's verdict (spec Context)
[State the measured core device_ratio. If ≈1x: the commit path is sync-bound
at the device ceiling; the gap to MANIFESTO §7's headline number is a
group-commit question (engine.rs:137-162's follower batch path already
proves the shape) — record the sized gap as the follow-up's opening
evidence. If not ≈1x: state what the layers show instead — and be explicit
that this instrument does not isolate redb's own contribution (the core
layer bundles WAL append + sync + apply + store materialization as one
number). Attempt a rough attribution from what exists: core's number against
device's append-shaped loop isolates WAL-shaped device cost from the rest of
core's bundle. If the attribution stays ambiguous, say plainly that
DECISIONS §7 trigger (a) remains OPEN pending finer profiling — the refuted
branch does not close it the way the confirmed branch does, and this section
must not imply otherwise. Either way: DECISIONS §7 trigger (a) reads THIS
section.]

## What these numbers are not
Not the launch claim (that requires the bare-metal Linux box run — spec
ruling 4). Not a comparison with any other stack (spec §8). Not a latency
guarantee: closed-loop tails understate open-loop tails (coordinated
omission, disclosed above).
```

- [ ] **Step 3: Correct README's platform-surface line**

The audit found README line 8 overclaims ("...auth, and the blob store" — no blob store exists). While touching the docs surface, fix just that clause: change "auth, and the blob store" to "and auth" (the blob store stays in DESIGN §4's future Snapshots-&-reads sketch). One-line diff. This is out of the benchmark spec's scope — a pre-existing docs inaccuracy fixed opportunistically — so the Step 6 commit message must name it as an incidental, spec-unrelated correction.

- [ ] **Step 4: CLAUDE.md close-out entry**

Add to Current state (after the hard-rule-5 interlude entry), following the house shape: sub-project name + spec/plan paths, what shipped (crate layout, the four layers, validity mechanics, BENCH.md), the prediction's verdict with the measured ratio, evidence (see Step 5's sweep), and the follow-up it opens (group-commit spec if the prediction held, sized by the recorded gap). Update the "Next:" bullet: benchmark harness DONE; next per PLAN/backlog.

- [ ] **Step 5: The final-bytes sweep** (per the sub-project's scale — bench-harness touches no core code, so the full 11-stage sweep is not owed; state exactly what ran)

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace          # twice; bench-harness adds its un-ignored tests
cargo build -p guide-examples   # untouched, must stay green
scripts/bench.sh --smoke
git log --format='%h %s' --grep='^bench:' -- crates/charpente-core crates/charpente
# ^ MUST print nothing (spec §9.4): no commit of THIS sub-project (the
#   `bench:` prefix) touched the deterministic boundary. Filtering by the
#   prefix, not a ref range, is deliberate: this plan commits directly to
#   main, so `git merge-base HEAD main` is HEAD itself and a HEAD..HEAD
#   diff is vacuously empty — it could never fail (plan-review C-1) — while
#   a start-SHA range would false-positive on another session's interleaved
#   core commits. If anything prints, STOP — find and revert before closing.
git log --oneline --grep='^bench:' | head -3
# ^ positive control: MUST print this sub-project's commits (verified empty
#   before T1 — no historical commit matches '^bench:'). Proves the prefix
#   convention held, so the empty pathspec-filtered list above is meaningful
#   rather than an artifact of no bench: commits existing at all.
```

All green + the empty pathspec-filtered list + the non-empty positive control is the exit evidence. Record each stage's outcome in the ledger (tee, never tail).

- [ ] **Step 6: Commit**

First run `git diff CLAUDE.md README.md` and confirm the diff contains ONLY this task's own
edits (the Current-state entry, the Next bullet, the one README clause). CLAUDE.md is the
most contested file in this shared tree — `git add` stages the whole file, so another
session's half-written uncommitted edit would be silently absorbed into this commit. If
anything else appears, stop and ask; never stage it blindly. Then:

```bash
git add docs/BENCH.md CLAUDE.md README.md
git commit -m "bench: BENCH.md first numbers of record + prediction verdict; close out M2 sub-project 1

The README platform-surface line is an incidental docs correction (no blob
store exists), not benchmark scope."
```

---

## Self-review notes (run before handing off)

- Spec coverage: §1 (T1, T4 binary, T7 vehicle), §2 (T4/T5/T6/T7), §3 (T1 app + T5/T6 metrics + T8 invoicing), §4 (T7 client/sweep), §5 (T2 validity + T3 fields + T4 embedding), §6 (T3 JSON pin + T10 BENCH.md), §7 (T4–T8 smokes + T9 forced arms + T3 shape pin), §8 (nothing in any task implements a non-goal), §9 (T9 bench.sh, T10 exit criteria).
- The `#[job]`/`#[route]`/`#[event]` visibility (`pub` items) must match what the macros require; if a macro rejects `pub` or requires it, follow the compiler — the sim proof in T1 gates every later task on the app actually working.
- `SATURATION_FRACTION` is defined in T7, made `pub` in T9, consumed by name in T9's test and BENCH.md's method prose — keep the name.

---

## Plan-review fix wave (2026-08-07)

Two adversarial reviews (`.superpowers/sdd/2026-08-06-benchmark-harness/plan-review-spec-fidelity.md`
and `plan-review-execution.md`, both APPROVE-WITH-FIXES) were applied to this plan before
execution. Every Critical/Important finding was source-verified before its fix landed:

- **Execution C-1** (source-verified at `launch.rs:31` + `serve.rs:38`): `bench-app` cannot
  use `charpente::launch` — it re-reads live process argv and exits 2 on `--addr`. T7
  rewritten to the `node.rs` precedent (`serve::serve` + park); spec §1 amended with a dated
  note. `spawn_target` now tees child stdio to sibling files and embeds the child's stderr
  in the exited-during-spawn error, so this class of failure is self-diagnosing.
- **Fidelity C-1**: T10's zero-core-diff check was `merge-base HEAD main`..HEAD on main —
  vacuously empty, could never fail. Replaced with the `^bench:`-prefix pathspec grep plus a
  positive control (verified: no historical commit matches `^bench:`).
- **Fidelity C-2**: nothing drove the `bench` binary itself. T9 gains two `#[ignore]`d
  binary-driving smokes (end-to-end `smoke --json` with exit/shape asserts; `bogus` → exit 2).
- **Fidelity I-1**: `Report` gains `filesystem` (spec §5 names it); helper, JSON pin, both
  renderers, and BENCH.md template updated.
- **Fidelity I-2**: `SweepPoint` gains `log_records_per_sec`, derived per target
  (`records_per_request`: bench-app 2.0 via the dual-write invariant; invoicing 1.0 —
  `create_customer` is get+put only, verified at `routes.rs:53-71`). The derivation is
  disclosed in code and BENCH.md, never presented as a count.
- **Fidelity I-3**: core's read loop now uses the caller's warmup/duration (the hardcoded
  500 ms/5 s window silently contradicted the report's own fields).
- **Fidelity I-4**: T9 gains an `#[ignore]`d test forcing saturation through `wire::sweep`'s
  REAL CPU arithmetic (spinner threads), complementing the render-only unit test.
- **Fidelity I-5**: BENCH.md's refuted-prediction branch now explicitly states that this
  instrument does not isolate redb, instructs the core-vs-device-append attribution attempt,
  and forbids implying trigger (a) closes in that branch.
- **Execution I-1**: the explicit `pub mod X;` lib.rs line added to T4/T5/T6/T7 (was present
  only in T2/T3).
- **Execution I-2** (+ a defect both reviews missed, found while fixing): T9 Step 3 is now
  the full literal `run_layers`, not comment placeholders — and the smoke-parameter
  selection moved BEFORE the device run (as placed, `bench smoke` would have run the device
  loops at the 5 s + 30 s defaults: a minute-plus "1 s smoke").
- **Execution I-3**: shared-file staging guards (diff-then-add) added to T1 (Cargo.toml) and
  T10 (CLAUDE.md/README.md).
- **Execution I-4**: `wire::sweep` restructured fail-soft — a lost point is reported in
  `invalid` (still exits 1) instead of discarding the whole sweep; plus an empty-latency
  guard so `percentile`'s non-empty assert cannot panic the run.
- **Execution I-5**: kernel layer gains a progress watcher thread (5 s cadence, 100 ms
  stop-poll) + T10 stall guidance.
- **Minors**: fidelity M-1 (README fix named incidental in the commit), M-2 (ledger step per
  task), M-3 (vacuous-crosscheck comment), M-4/exec M-2 (Interfaces `one_request` drift),
  M-5 (`all` sixth-subcommand deviation recorded in spec §1 + T9); exec M-1 (T9 Files list
  gains wire.rs). Exec M-3 (CLIENT_TIMEOUT) is de-risked by the I-4 fail-soft change and
  otherwise accepted as-is.
