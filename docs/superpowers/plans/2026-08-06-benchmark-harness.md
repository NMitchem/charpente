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

Create `.superpowers/sdd/2026-08-06-benchmark-harness/progress.md` with a dated Task 1 entry (crate scaffolded, sim proof green, seed 0xBE9C_0001). Then:

```bash
git add Cargo.toml crates/bench-harness
git commit -m "bench: scaffold bench-harness with the canonical app + sim proof (T1)"
```

(The ledger directory is gitignored — do not `git add` it.)

---

### Task 2: `measure.rs` — CPU time, heartbeat sleep-guard, percentiles, totals cross-check

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

- [ ] **Step 5: Commit**

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
             \"cores\":8,\"sync_semantics\":\"F_FULLFSYNC via sync_all\",\"warmup_secs\":5.00,\
             \"duration_secs\":30.00,\"device\":{\"syncs_per_sec\":1500.25,\
             \"appends_per_sec\":1400.50},\"core\":{\"acked_writes_per_sec\":1450.00,\
             \"log_records_per_sec\":1450.00,\"reads_per_sec\":800000.00,\
             \"device_ratio\":0.970},\"kernel\":{\"acked_writes_per_sec\":1300.00,\
             \"log_records_per_sec\":2600.00,\"requests_total\":50000,\"jobs_done\":50000},\
             \"wire\":[{\"target\":\"bench-app\",\"points\":[{\"connections\":4,\
             \"throughput_per_sec\":1200.00,\"p50_us\":800,\"p99_us\":2100,\"p999_us\":5000,\
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
        "{{\"bench\":{{\"hardware\":\"{}\",\"os\":\"{}\",\"cores\":{},\
         \"sync_semantics\":\"{}\",\"warmup_secs\":{:.2},\"duration_secs\":{:.2}",
        esc(&r.hardware), esc(&r.os), r.cores, esc(r.sync_semantics),
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
                    "{{\"connections\":{},\"throughput_per_sec\":{:.2},\"p50_us\":{},\
                     \"p99_us\":{},\"p999_us\":{},\"client_busy_fraction\":{:.3},\
                     \"saturated\":{}}}",
                    p.connections, p.throughput_per_sec, p.p50_us, p.p99_us, p.p999_us,
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
        "bench: {} | {} | {} cores | sync: {} | warmup {:.0}s, measure {:.0}s\n",
        r.hardware, r.os, r.cores, r.sync_semantics, r.warmup_secs, r.duration_secs
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
                "    {:>4} conns: {:>8.0} req/s  p50 {:>6}us  p99 {:>6}us  p99.9 {:>6}us  \
                 client busy {:.0}%{}\n",
                p.connections, p.throughput_per_sec, p.p50_us, p.p99_us, p.p999_us,
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

- [ ] **Step 5: Commit**

```bash
git add crates/bench-harness
git commit -m "bench: report model, pinned JSON shape, the ceiling refusal (T3)"
```

---

### Task 4: `device.rs` + the `bench` binary skeleton

**Files:**
- Create: `crates/bench-harness/src/device.rs`
- Create: `crates/bench-harness/src/bin/bench.rs`
- Modify: `crates/bench-harness/src/lib.rs` (add `pub mod device;`)
- Test: `crates/bench-harness/tests/harness.rs` (new; `#[ignore]`d smokes accumulate here)

**Interfaces:**
- Consumes: `charpente::prod::ProdDisk`; `measure::Heartbeat`; `report::*`.
- Produces: `device::run(dir: &str, warmup: Duration, duration: Duration) -> io::Result<DeviceReport>`; the `bench` binary with `device` subcommand, `--json`, `--duration S`, `--warmup S` flags; `bin/bench.rs` helper fns later subcommands extend: `fn temp_dir(tag: &str) -> String`, `fn hardware() -> String`, `fn os() -> String`, `fn sync_semantics() -> &'static str`, `fn base_report(warmup: f64, duration: f64) -> Report`.

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

- [ ] **Step 5: Commit**

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
- Consumes: `bench_harness::{schema, Slot, Bump, SlotBumped, SLOTS}`; `charpente::core::{Db, DbConfig, TxError, View}` (fix paths per rustc); `charpente::prod::ProdDisk`; `measure::{Heartbeat, crosscheck}`; `report::CoreReport`.
- Produces: `core_bench::run(dir: &str, warmup: Duration, duration: Duration, device_syncs_per_sec: f64) -> io::Result<(CoreReport, Vec<String>)>` — the `Vec<String>` is invalidity reasons for `report.invalid`.

- [ ] **Step 1: Implement**

```rust
//! Layer 2 (spec §2): Db::transact in a closed loop against the prod Disk —
//! the commit path alone (WAL append + sync + apply + store materialization).
//! The spec's Context prediction is about this module's number.
//! Edge tooling outside the deterministic core (DESIGN §1).
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use crate::measure::{crosscheck, Heartbeat};
use crate::report::CoreReport;
use crate::{schema, Bump, Slot, SlotBumped, SLOTS};
use charpente::core::{Db, DbConfig, TxError, View};
use charpente::prod::ProdDisk;
use charpente::Id;
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
    match crosscheck(&[i - measured_from], grand) {
        Ok(_) => {}
        Err(reason) => invalid.push(reason),
    }
    let acked = grand as f64 / elapsed.as_secs_f64();

    // Reads: one snapshot, typed gets. No log involvement — this number shows
    // where the write ceiling does NOT apply (spec §3).
    let snap = db.snapshot()?;
    let read_warm_end = Instant::now() + Duration::from_millis(500);
    let mut r = 0u64;
    while Instant::now() < read_warm_end {
        let _ = snap.get(Id::<Slot>::from_raw(r % SLOTS))?;
        r += 1;
    }
    let read_start = Instant::now();
    let read_end = read_start + duration.min(Duration::from_secs(5));
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

- [ ] **Step 2: Wire the subcommand arm**

In `bin/bench.rs`'s `run_layers`, replace the placeholder match with:

```rust
    match args.cmd.as_str() {
        "device" => {}
        "core" => {
            let dir = temp_dir("core");
            let ceiling = report.device.as_ref().expect("device ran first").syncs_per_sec;
            let (core, invalid) = bench_harness::core_bench::run(&dir, args.warmup, args.duration, ceiling)?;
            let _ = std::fs::remove_dir_all(&dir);
            report.core = Some(core);
            report.invalid.extend(invalid);
        }
        other => eprintln!("bench: subcommand {other:?} not implemented yet"),
    }
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

- [ ] **Step 5: Commit**

```bash
git add crates/bench-harness
git commit -m "bench: core layer — the commit path against its device ceiling (T5)"
```

---

### Task 6: `kernel_bench.rs` — the loop + dispatch overhead

**Files:**
- Create: `crates/bench-harness/src/kernel_bench.rs`
- Modify: `crates/bench-harness/src/lib.rs` (add `pub mod kernel_bench;`)
- Modify: `crates/bench-harness/src/bin/bench.rs` (add the `kernel` arm)
- Modify: `crates/bench-harness/tests/harness.rs` (add smoke)

**Interfaces:**
- Consumes: `bench_harness::{build, SLOTS}`; `charpente::kernel::{Kernel, SimSource}`; `charpente::http::{Method, Request}`; `charpente::prod::{ProdClock, ProdDisk, ProdEntropy}`; `measure::{Heartbeat, crosscheck}`; `report::KernelReport`.
- Produces: `kernel_bench::run(dir: &str, requests: u64) -> io::Result<(KernelReport, Vec<String>)>`.

Design note (verified against `kernel/mod.rs:592-628`): `SimSource` is clock-generic. With `ProdClock` and `horizon: 0`, all requests pushed at `at_nanos: 0` are immediately ready; once the script drains, any remaining deadline (the platform cron, ~a day out) exceeds the horizon and `wait_until` returns `Wake::Shutdown`, so `Kernel::run` returns after draining requests **and** all enqueued jobs. Batch model: no warmup phase; elapsed includes the shutdown checkpoint — both disclosed by reporting `requests_total`/`jobs_done` alongside the rate. Two `ProdClock`s (source + kernel) mirror `serve.rs`'s own known precision-only skew.

- [ ] **Step 1: Implement**

```rust
//! Layer 3 (spec §2): the kernel loop fed in-process through RequestSource —
//! adds scheduling, dispatch, and job execution to the core layer's cost.
//! Batch model: N pre-built requests, measured across Kernel::run.
//! Edge tooling outside the deterministic core (DESIGN §1).
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use crate::measure::{crosscheck, Heartbeat};
use crate::report::KernelReport;
use crate::{build, SLOTS};
use charpente::http::{Method, Request};
use charpente::kernel::{Kernel, SimSource};
use charpente::prod::{ProdClock, ProdDisk, ProdEntropy};
use std::io;
use std::time::Instant;

pub fn run(dir: &str, requests: u64) -> io::Result<(KernelReport, Vec<String>)> {
    let app = build(Box::new(ProdDisk::new(dir)?), Box::new(ProdEntropy::new()?))?;
    let mut source = SimSource::new(Box::new(ProdClock::new()), 0);
    for i in 0..requests {
        source.push_at(0, Request::new(Method::Post, &format!("/hit/{}", i % SLOTS)));
    }
    let responses = source.responses();
    let mut kernel = Kernel::new(app, Box::new(ProdClock::new()))?;

    let mut invalid = Vec::new();
    let hb = Heartbeat::start();
    let start = Instant::now();
    kernel.run(&mut source)?;
    let elapsed = start.elapsed();
    if let Some(reason) = hb.finish() {
        invalid.push(reason);
    }

    let responses = responses.lock().expect("responses lock");
    for (status, body) in responses.iter() {
        assert_eq!(
            *status,
            200,
            "a canonical request failed: {}",
            String::from_utf8_lossy(body)
        );
    }
    let stats = kernel.stats();
    // Totals two ways (spec §5): the recorded-response count vs KernelStats.
    if let Err(reason) = crosscheck(&[responses.len() as u64], requests) {
        invalid.push(reason);
    }
    if let Err(reason) = crosscheck(&[stats.jobs_done], requests) {
        invalid.push(format!("{reason} (jobs_done must equal requests: every hit enqueues one Bump)"));
    }

    let acked = requests as f64 / elapsed.as_secs_f64();
    Ok((
        KernelReport {
            acked_writes_per_sec: acked,
            // Each request commits one effects record; each Bump's Done-mark
            // commits another (spec §3): 2x by construction at this layer.
            log_records_per_sec: (requests + stats.jobs_done) as f64 / elapsed.as_secs_f64(),
            requests_total: requests,
            jobs_done: stats.jobs_done,
        },
        invalid,
    ))
}
```

- [ ] **Step 2: Wire the subcommand arm** (in `run_layers`)

```rust
        "kernel" => {
            let dir = temp_dir("kernel");
            let (kernel, invalid) = bench_harness::kernel_bench::run(&dir, args.requests)?;
            let _ = std::fs::remove_dir_all(&dir);
            report.kernel = Some(kernel);
            report.invalid.extend(invalid);
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

Run: `cargo test -p bench-harness --test harness -- --ignored` → PASS (3 tests).
Run: `cargo run -p bench-harness --bin bench -- kernel --requests 2000 --json` → JSON has `"kernel":{...}`, exit 0. If `Kernel::run` fails to terminate (the `SimSource`+`ProdClock` contingency), STOP AND ASK — do not patch `charpente` (Global Constraints).

- [ ] **Step 5: Commit**

```bash
git add crates/bench-harness
git commit -m "bench: kernel layer — batch model through RequestSource (T6)"
```

---

### Task 7: keep-alive client + `bench-app` vehicle + the wire layer

**Files:**
- Create: `crates/bench-harness/src/client.rs`
- Create: `crates/bench-harness/src/wire.rs`
- Create: `crates/bench-harness/src/bin/bench-app.rs`
- Modify: `crates/bench-harness/src/lib.rs` (add `pub mod client; pub mod wire;`)
- Modify: `crates/bench-harness/src/bin/bench.rs` (add the `wire` arm)
- Modify: `crates/bench-harness/tests/harness.rs` (add smoke)

**Interfaces:**
- Consumes: `measure::{process_cpu, crosscheck, percentile, Heartbeat}`; `report::{WireReport, SweepPoint}`; `charpente::launch`.
- Produces:
  - `client::Conn` — `connect(addr: &str) -> io::Result<Conn>`, `request(&mut self, method: &str, path: &str, headers: &[(String, String)], body: &[u8]) -> io::Result<(u16, Vec<u8>)>`; `client::read_response(r: &mut impl BufRead) -> io::Result<(u16, Vec<u8>)>` (pure-parsable, unit-tested un-ignored).
  - `wire::Target` enum (`BenchApp`, `Invoicing`) with `fn request_for(&self, conn: &mut Conn, ids: &AtomicU64) -> io::Result<(u16, Vec<u8>)>` and `fn session_setup(&self, conn: &mut Conn) -> io::Result<Vec<(String, String)>>` (Task 8 fills the `Invoicing` arms; this task ships them as `unreachable!("wired in T8")`).
  - `wire::build_bin(package: &str, bin: &str) -> io::Result<PathBuf>` (cargo `--message-format=json` string-scan, the `edge-harness/src/child.rs:302-330` shape).
  - `wire::sweep(bin: &Path, target: Target, connections: &[usize], warmup: Duration, duration: Duration) -> io::Result<(WireReport, Vec<String>)>`.
  - `bench-app` binary: `--addr A --data-dir D` (usage → stderr, exit 2), body = `charpente::launch(&addr, &dir, bench_harness::build)`.

- [ ] **Step 1: Write the failing parse test** (in `client.rs`'s `#[cfg(test)] mod tests` — pure, un-ignored)

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

- [ ] **Step 2: Run it, verify it fails** — `cargo test -p bench-harness client` → compile FAIL.

- [ ] **Step 3: Implement `client.rs`**

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

- [ ] **Step 4: Run the parse tests, verify they pass** — `cargo test -p bench-harness client` → PASS (2 tests).

- [ ] **Step 5: Implement `bin/bench-app.rs`**

```rust
//! The wire-layer vehicle (spec §1): the standard charpente app fn main.
//! Edge tooling outside the deterministic core (DESIGN §1).
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

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
            // Unknown flags fall through untouched: launch() argv-dispatches
            // verbs like `describe` itself.
            _ => {}
        }
    }
    let (Some(addr), Some(dir)) = (addr, dir) else {
        eprintln!("usage: bench-app --addr HOST:PORT --data-dir DIR");
        return ExitCode::from(2);
    };
    match charpente::launch(&addr, &dir, |disk, entropy| bench_harness::build(disk, entropy)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("bench-app: {e}");
            ExitCode::from(1)
        }
    }
}
```

- [ ] **Step 6: Implement `wire.rs`**

```rust
//! Layer 4 (spec §2, §4): real HTTP/1.1 keep-alive load against a spawned
//! child binary — the number the README claim is about. Closed-loop, with
//! coordinated-omission and client-saturation disclosed in the artifact.
//! Edge tooling outside the deterministic core (DESIGN §1).
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use crate::client::Conn;
use crate::measure::{crosscheck, percentile, process_cpu, Heartbeat};
use crate::report::{SweepPoint, WireReport};
use crate::SLOTS;
use std::io;
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const SATURATION_FRACTION: f64 = 0.9;
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

    /// Per-thread setup returning the headers every request carries.
    fn session_setup(&self, conn: &mut Conn) -> io::Result<Vec<(String, String)>> {
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
    let child = Command::new(bin)
        .args(["--addr", addr, "--data-dir", dir])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;
    let mut node = BenchChild { child };
    let deadline = Instant::now() + SPAWN_TIMEOUT;
    loop {
        if TcpStream::connect(addr).is_ok() {
            return Ok(node);
        }
        if let Some(status) = node.child.try_wait()? {
            return Err(io::Error::other(format!("target exited during spawn: {status}")));
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
) -> io::Result<(WireReport, Vec<String>)> {
    let target = Arc::new(target);
    let mut points = Vec::new();
    let mut invalid = Vec::new();
    for &n in connections {
        // A fresh child + data dir per point: store growth from one point
        // must not confound the next.
        let dir = crate::wire::point_dir(target.name(), n);
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
                        return Err(io::Error::other(format!(
                            "request failed with {status}: {}",
                            String::from_utf8_lossy(&body)
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
        let cpu = process_cpu() - cpu0;
        stop.store(true, Ordering::Relaxed);
        if let Some(reason) = hb.finish() {
            invalid.push(format!("[{} conns] {reason}", n));
        }

        let mut per_thread = Vec::new();
        let mut latencies = Vec::new();
        for w in workers {
            let (count, lat) = w
                .join()
                .map_err(|_| io::Error::other("a client thread panicked"))??;
            per_thread.push(count);
            latencies.extend(lat);
        }
        drop(node);
        let _ = std::fs::remove_dir_all(&dir);

        let total = match crosscheck(&per_thread, grand.load(Ordering::Relaxed)) {
            Ok(t) => t,
            Err(reason) => {
                invalid.push(format!("[{} conns] {reason}", n));
                grand.load(Ordering::Relaxed)
            }
        };
        latencies.sort_unstable();
        let client_busy_fraction = cpu.as_secs_f64() / (n as f64 * elapsed.as_secs_f64());
        points.push(SweepPoint {
            connections: n,
            throughput_per_sec: total as f64 / elapsed.as_secs_f64(),
            p50_us: percentile(&latencies, 50.0),
            p99_us: percentile(&latencies, 99.0),
            p999_us: percentile(&latencies, 99.9),
            client_busy_fraction,
            saturated: client_busy_fraction > SATURATION_FRACTION,
        });
    }
    Ok((WireReport { target: target.name().to_string(), points }, invalid))
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

- [ ] **Step 7: Wire the subcommand arm** (in `run_layers`)

```rust
        "wire" => {
            let target = match args.target.as_str() {
                "bench-app" => bench_harness::wire::Target::BenchApp,
                "invoicing" => bench_harness::wire::Target::Invoicing,
                other => return Err(std::io::Error::other(format!("unknown --target {other:?}"))),
            };
            let (package, bin) = match &target {
                bench_harness::wire::Target::BenchApp => ("bench-harness", "bench-app"),
                bench_harness::wire::Target::Invoicing => ("invoicing", "node"),
            };
            let bin = bench_harness::wire::build_bin(package, bin)?;
            let (wire, invalid) = bench_harness::wire::sweep(
                &bin, target, &args.connections, args.warmup, args.duration,
            )?;
            report.wire.push(wire);
            report.invalid.extend(invalid);
        }
```

- [ ] **Step 8: Add the ignored smoke** (in `tests/harness.rs`)

```rust
#[test]
#[ignore]
fn wire_layer_sweeps_the_bench_app() {
    let bin = bench_harness::wire::build_bin("bench-harness", "bench-app").expect("bin builds");
    let (wire, invalid) = bench_harness::wire::sweep(
        &bin,
        bench_harness::wire::Target::BenchApp,
        &[1, 4],
        Duration::from_millis(300),
        Duration::from_secs(1),
    )
    .expect("sweep runs");
    assert!(invalid.is_empty(), "short healthy run must be valid: {invalid:?}");
    assert_eq!(wire.points.len(), 2);
    for p in &wire.points {
        assert!(p.throughput_per_sec > 0.0);
        assert!(p.p50_us <= p.p99_us && p.p99_us <= p.p999_us, "percentile ordering");
    }
}
```

- [ ] **Step 9: Verify**

Run: `cargo test -p bench-harness client` → PASS.
Run: `cargo test -p bench-harness --test harness -- --ignored --test-threads=1` → PASS (4 tests).
Run: `cargo run -p bench-harness --bin bench -- wire --connections 1,4 --duration 2 --warmup 0.5 --json` → JSON has `"wire":[{"target":"bench-app",...}]`, exit 0.

- [ ] **Step 10: Commit**

```bash
git add crates/bench-harness
git commit -m "bench: wire layer — keep-alive client, bench-app vehicle, connection sweep (T7)"
```

---

### Task 8: the invoicing wire target

**Files:**
- Modify: `crates/bench-harness/src/wire.rs` (fill the two `Invoicing` arms)
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

Run: `cargo test -p bench-harness --test harness -- --ignored --test-threads=1` → PASS (5 tests).
Run: `cargo run -p bench-harness --bin bench -- wire --target invoicing --connections 1,4 --duration 2 --warmup 0.5` → human summary shows `wire [invoicing]`, exit 0.

- [ ] **Step 4: Commit**

```bash
git add crates/bench-harness
git commit -m "bench: invoicing wire target — login flow + create_customer load (T8)"
```

---

### Task 9: `all`/`smoke` subcommands, forced INVALID arms, `scripts/bench.sh`

**Files:**
- Modify: `crates/bench-harness/src/bin/bench.rs` (add `all` + `smoke` arms)
- Modify: `crates/bench-harness/tests/harness.rs` (forced-arm tests)
- Create: `scripts/bench.sh` (mode `+x`)

**Interfaces:**
- Consumes: everything from T4–T8.
- Produces: `bench all` (device + core + kernel + wire×2, one assembled report), `bench smoke` (= `all` at 1 s durations, 2000 requests, connections 1,4), `scripts/bench.sh` / `scripts/bench.sh --smoke`.

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
    assert!(verdict.contains("suspended"), "got: {verdict}");
}

#[test]
fn a_saturated_client_is_flagged_on_the_point() {
    // The saturation rule is arithmetic on measured CPU (spec §4); force it
    // by construction: busy_fraction 1.0 > the 0.9 threshold.
    let p = bench_harness::report::SweepPoint {
        connections: 1,
        throughput_per_sec: 1.0,
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

- [ ] **Step 2: Run them** — `cargo test -p bench-harness --test harness` → the two new tests PASS un-ignored (the rest stay ignored).

- [ ] **Step 3: Add the `all` and `smoke` arms** (in `run_layers`, replacing the per-arm dispatch with a shared list)

```rust
    let cmds: Vec<&str> = match args.cmd.as_str() {
        "all" | "smoke" => vec!["core", "kernel", "wire-bench-app", "wire-invoicing"],
        "wire" => vec![match args.target.as_str() {
            "bench-app" => "wire-bench-app",
            "invoicing" => "wire-invoicing",
            other => return Err(std::io::Error::other(format!("unknown --target {other:?}"))),
        }],
        single => vec![single],
    };
    let (warmup, duration, requests, connections) = if args.cmd == "smoke" {
        (Duration::from_millis(200), Duration::from_secs(1), 2000, vec![1usize, 4])
    } else {
        (args.warmup, args.duration, args.requests, args.connections.clone())
    };
    for cmd in cmds {
        match cmd {
            "device" => {} // already ran above
            "core" => { /* the T5 arm, using `warmup`/`duration` */ }
            "kernel" => { /* the T6 arm, using `requests` */ }
            "wire-bench-app" => { /* the T7 arm with Target::BenchApp, `connections` */ }
            "wire-invoicing" => { /* the T8 arm with Target::Invoicing, `connections` */ }
            other => unreachable!("unmapped subcommand {other}"),
        }
    }
```

(Restructure the existing arms into this dispatch — same bodies, parameterized by the chosen `warmup`/`duration`/`requests`/`connections`. `device` still runs first unconditionally.)

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
Run: `cargo run -p bench-harness --bin bench -- smoke --json` → one assembled report with device+core+kernel+both wire targets, exit 0.

- [ ] **Step 6: Commit**

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

Run: `scripts/bench.sh` (expect ~10 minutes: 2 device loops + core + kernel + 2 wire sweeps × 5 points, each 5 s warmup + 30 s measure). Keep `target/bench/report.json`. If any INVALID appears, fix the environment (not the thresholds) and re-run.

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
acked writes/sec vs log records/sec (≈2x at kernel/wire: the job Done-mark).]

## Numbers of record — dev hardware (NOT the launch claim)
Hardware: [report.hardware, report.os, report.cores cores]
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
evidence. If not ≈1x: state what the layers show instead — where the time
went, per the layer deltas. Either way: DECISIONS §7 trigger (a) reads THIS
section.]

## What these numbers are not
Not the launch claim (that requires the bare-metal Linux box run — spec
ruling 4). Not a comparison with any other stack (spec §8). Not a latency
guarantee: closed-loop tails understate open-loop tails (coordinated
omission, disclosed above).
```

- [ ] **Step 3: Correct README's platform-surface line**

The audit found README line 8 overclaims ("...auth, and the blob store" — no blob store exists). While touching the docs surface, fix just that clause: change "auth, and the blob store" to "and auth" (the blob store stays in DESIGN §4's future Snapshots-&-reads sketch). One-line diff, named in the commit.

- [ ] **Step 4: CLAUDE.md close-out entry**

Add to Current state (after the hard-rule-5 interlude entry), following the house shape: sub-project name + spec/plan paths, what shipped (crate layout, the four layers, validity mechanics, BENCH.md), the prediction's verdict with the measured ratio, evidence (see Step 5's sweep), and the follow-up it opens (group-commit spec if the prediction held, sized by the recorded gap). Update the "Next:" bullet: benchmark harness DONE; next per PLAN/backlog.

- [ ] **Step 5: The final-bytes sweep** (per the sub-project's scale — bench-harness touches no core code, so the full 11-stage sweep is not owed; state exactly what ran)

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace          # twice; bench-harness adds its un-ignored tests
cargo build -p guide-examples   # untouched, must stay green
scripts/bench.sh --smoke
git diff --name-only $(git merge-base HEAD main)..HEAD -- crates/charpente-core crates/charpente
# ^ MUST print nothing (spec §9.4). If it prints anything, STOP — the
#   constraint was violated somewhere; find and revert before closing.
```

All green + the empty diff is the exit evidence. Record each stage's outcome in the ledger (tee, never tail).

- [ ] **Step 6: Commit**

```bash
git add docs/BENCH.md CLAUDE.md README.md
git commit -m "bench: BENCH.md first numbers of record + prediction verdict; close out M2 sub-project 1"
```

---

## Self-review notes (run before handing off)

- Spec coverage: §1 (T1, T4 binary, T7 vehicle), §2 (T4/T5/T6/T7), §3 (T1 app + T5/T6 metrics + T8 invoicing), §4 (T7 client/sweep), §5 (T2 validity + T3 fields + T4 embedding), §6 (T3 JSON pin + T10 BENCH.md), §7 (T4–T8 smokes + T9 forced arms + T3 shape pin), §8 (nothing in any task implements a non-goal), §9 (T9 bench.sh, T10 exit criteria).
- The `#[job]`/`#[route]`/`#[event]` visibility (`pub` items) must match what the macros require; if a macro rejects `pub` or requires it, follow the compiler — the sim proof in T1 gates every later task on the app actually working.
- `SATURATION_FRACTION` is defined in T7, made `pub` in T9, consumed by name in T9's test and BENCH.md's method prose — keep the name.
