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
//!
//! **The device ceiling is bracketed per point (T7 rev. 3 ruling), not
//! threaded in from the CLI's own one-shot device layer.** Each point takes
//! its own `device::device_bracket_sample` before spawning its child and
//! after killing it, on a sibling scratch directory the child never touches,
//! so sampling never contends with a live child for the device. What
//! bracketing removed was cross-window drift, which was real — but it was
//! NOT the cause of the false alarms rev. 3 set out to fix. With a
//! bracket-stable ceiling the derived record rate still read over 1.0 on
//! three consecutive healthy runs, and rev. 4 moved the gate onto the
//! COUNTED acked-request rate instead. See `wire_accounting`'s module doc
//! for the readings and the ruling.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use crate::SLOTS;
use crate::client::Conn;
use crate::device;
use crate::measure::{Heartbeat, process_cpu};
use crate::report::WireReport;
use crate::wire_accounting::{WireObs, point_accounting};
use std::io;
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
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

    /// Durable syncs paid per acked request, by construction (2026-08-10
    /// group-commit interlude, T1 fix wave, C-1). Equal to
    /// `records_per_request` ONLY while one commit costs one fsync —
    /// `Engine::submit` today is unconditionally `append`→`sync`→`apply` with
    /// no batching, so every committed record pays its own durable sync and
    /// the two constants coincide. A future group-commit primary path is
    /// where this stops holding: N records can share one fsync, and at that
    /// point this needs a real server-reported sync count (a client outside
    /// the process cannot count the server's `wal.sync()` calls any more than
    /// it can count its log records) — see `wire_accounting`'s module doc for
    /// what is owed and why this is not yet built.
    fn syncs_per_request(&self) -> f64 {
        self.records_per_request()
    }

    /// Per-thread setup returning the headers every request carries. Run once
    /// per worker on its own connection, before the warmup, so no login
    /// round-trip lands inside a measured window.
    fn session_setup(&self, conn: &mut Conn) -> io::Result<Vec<(String, String)>> {
        match self {
            // bench-app mounts no auth, so there is nothing to carry.
            Target::BenchApp => Ok(Vec::new()),
            Target::Invoicing => {
                let (status, body) = conn.request("POST", "/login", &[], b"\"bench\"")?;
                if status != 200 {
                    return Err(io::Error::other(format!(
                        "invoicing login failed with {status}: {}",
                        String::from_utf8_lossy(&body)
                    )));
                }
                // The response body is a bare JSON string: the raw session
                // token. Sessions are hashed at rest (M1 sub-project 4), but
                // the token the client presents is this raw value.
                let token = String::from_utf8_lossy(&body).trim_matches('"').to_string();
                Ok(vec![(
                    "cookie".to_string(),
                    format!("charpente_session={token}"),
                )])
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
                // insert-heavy, spec §3). `ids` is shared across workers, so
                // no two connections can collide on an id and turn a measured
                // request into a 400.
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
}

/// Locate a built release binary via cargo's JSON output — the
/// edge-harness/src/child.rs shape (plain string scan, no JSON dep).
pub fn build_bin(package: &str, bin: &str) -> io::Result<PathBuf> {
    let root = workspace_root();
    let output = Command::new("cargo")
        .args([
            "build",
            "--release",
            "-p",
            package,
            "--bins",
            "--message-format=json",
        ])
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
        if let Some(exe) = extract_field(line, "\"executable\":\"")
            && Path::new(&exe).file_name().is_some_and(|n| n == bin)
        {
            return Ok(PathBuf::from(exe));
        }
    }
    Err(io::Error::other(format!(
        "no executable named {bin:?} in cargo's output for {package}"
    )))
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
            // Carry the child's stderr here too, not only on the exited arm
            // above. This arm shipped without it, and when T8 hit it once the
            // failure was undiagnosable for exactly that reason — the same
            // "spawn failure must be self-diagnosing" rule that plan-review
            // C-1 already enforced on the sibling arm. The child is still
            // alive on this path, so its stderr may be empty; say which case
            // it is rather than printing a bare blank.
            let stderr = std::fs::read_to_string(&err_path).unwrap_or_default();
            let tail = if stderr.trim().is_empty() {
                "<empty — the child is still running but never bound>".to_string()
            } else {
                stderr
            };
            return Err(io::Error::other(format!(
                "target never answered on {addr} within {SPAWN_TIMEOUT:?}\n\
                 child stderr ({err_path}):\n{tail}"
            )));
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
        match sweep_point(bin, &target, n, warmup, duration) {
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
    Ok((
        WireReport {
            target: target.name().to_string(),
            points,
        },
        invalid,
    ))
}

fn sweep_point(
    bin: &Path,
    target: &Arc<Target>,
    n: usize,
    warmup: Duration,
    duration: Duration,
) -> io::Result<(crate::report::SweepPoint, Vec<String>)> {
    // A fresh child + data dir per point: store growth from one point
    // must not confound the next.
    let dir = point_dir(target.name(), n);
    // A sibling of the child's own data dir, never inside it (T7 rev. 3):
    // the bracket samples must not touch, or be confounded by, the child's
    // own store.
    let bracket_dir = format!("{dir}.bracket");
    std::fs::create_dir_all(&bracket_dir)?;

    // Sampled with the child NOT running — before it spawns, and again below
    // after it is killed. Sampling while it is live would contend with it
    // for the device and under-measure the ceiling, which would bias every
    // ratio computed from it upward. (Rev. 3 believed an under-measured
    // ceiling was also the cause of its false alarms; rev. 4 measured a
    // bracket-stable ceiling and found the DERIVED numerator was. The
    // child-stopped discipline stands on its own either way.)
    let device_before = device::device_bracket_sample(&bracket_dir)?;

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
        workers.push(std::thread::spawn(
            move || -> io::Result<(u64, Vec<u64>)> {
                let mut conn = Conn::connect(&addr)?;
                let headers = target.session_setup(&mut conn)?;
                let mut count = 0u64;
                let mut lat = Vec::new();
                while !stop.load(Ordering::Relaxed) {
                    let t0 = Instant::now();
                    let (status, body) = target.one_request(&mut conn, &headers, &ids)?;
                    if status != 200 {
                        let shown: String =
                            String::from_utf8_lossy(&body).chars().take(256).collect();
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
            },
        ));
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
        match w
            .join()
            .map_err(|_| io::Error::other("a client thread panicked"))
        {
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
    // The second bracket sample, taken after the child is killed (T7 rev. 3)
    // — deferred as a `Result` rather than `?`ed immediately so a worker's
    // own error (which named the actual failure) still wins over a bracket
    // sampling failure below, mirroring the worker-join precedence above.
    let device_after = device::device_bracket_sample(&bracket_dir);
    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::remove_dir_all(&bracket_dir);
    let _ = std::fs::remove_file(format!("{dir}.child.out"));
    let _ = std::fs::remove_file(format!("{dir}.child.err"));
    joined?;
    let device_after = device_after?;

    let acked = grand.load(Ordering::Relaxed);
    let obs = WireObs {
        connections: n,
        per_thread,
        grand: acked,
        latencies_us,
        elapsed,
        client_cpu,
        device_before,
        device_after,
        records_per_request: target.records_per_request(),
        // 2026-08-10 group-commit interlude, T1 fix wave, C-1: derived
        // honestly via the per-target multiplier, not copied from `grand`.
        // The review found the shipped form (`syncs: acked`) wrong by 2x for
        // BenchApp — its canonical route commits TWO durable records per
        // acked request (the request's own effects record and its Bump's
        // Done-mark), so `records_per_request` (== `syncs_per_request` while
        // one commit costs one fsync) is 2.0, not 1.0. Numerically unchanged
        // for Invoicing (1.0). This remains a DERIVED input, not a counted
        // one — see `Target::syncs_per_request`'s doc and
        // `wire_accounting`'s module doc for what would make it counted.
        syncs: (target.syncs_per_request() * acked as f64) as u64,
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
