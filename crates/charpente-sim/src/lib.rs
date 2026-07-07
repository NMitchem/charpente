//! Simulation implementations of the core IO traits (DESIGN.md §1, PLAN.md M0
//! step 1): one seed drives everything — virtual time, latency, entropy, and
//! the fault matrix (torn writes, failed syncs, crash-restart between any two
//! disk operations, partitions). A failure prints its seed; the seed is the
//! reproduction.
//!
//! The world is a single `Mutex<State>` — not for parallelism (the simulation
//! is strictly single-threaded; the lock is never contended) but because
//! storage engines mounted behind the boundary require `Send + Sync` handles
//! (`File`'s supertrait). Every container with order-dependent iteration is a
//! BTree — replaying a seed must be byte-identical, forever.

pub mod checks;
mod disk;
pub mod harness;
mod net;
pub mod rng;

use std::collections::BTreeMap;
use std::io;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use charpente_core::io::{Clock, Conn, Disk, Entropy, File, Instant, Listener, Net};

use crate::disk::{DiskWorld, FileState, PendingOp};
use crate::net::{ConnState, NetWorld, host_of, normalize};
use crate::rng::{Rng, splitmix64};

/// Fault-injection probabilities and latency ranges. The defaults are the
/// standard matrix; `none()` turns everything off for logic-only tests.
#[derive(Clone, Debug)]
pub struct FaultConfig {
    /// P(a `write_at` returns an injected IO error).
    pub write_error: f64,
    /// P(a `sync` fails, leaving the durability of prior writes unknown).
    pub sync_error: f64,
    /// P(an unsynced op is fully durable after a crash).
    pub crash_apply: f64,
    /// P(an unsynced write survives a crash torn); the rest are lost entirely.
    pub crash_torn: f64,
    /// P(an unsynced directory entry is lost (create) or resurrected
    /// (remove) at crash).
    pub dir_entry_loss: f64,
    pub disk_latency: (Duration, Duration),
    pub net_latency: (Duration, Duration),
}

impl Default for FaultConfig {
    fn default() -> FaultConfig {
        FaultConfig {
            write_error: 0.02,
            sync_error: 0.02,
            crash_apply: 0.4,
            crash_torn: 0.3,
            dir_entry_loss: 0.0,
            disk_latency: (Duration::from_micros(20), Duration::from_millis(2)),
            net_latency: (Duration::from_micros(100), Duration::from_millis(20)),
        }
    }
}

impl FaultConfig {
    pub fn none() -> FaultConfig {
        FaultConfig {
            write_error: 0.0,
            sync_error: 0.0,
            crash_apply: 0.0,
            crash_torn: 0.0,
            dir_entry_loss: 0.0,
            disk_latency: (Duration::ZERO, Duration::ZERO),
            net_latency: (Duration::ZERO, Duration::ZERO),
        }
    }

    /// Samples a per-universe fault configuration (swarm testing: Groce 2012,
    /// TigerBeetle VOPR's default). Each fault axis is independently in or out
    /// of play; active axes draw rates from wide ranges, including extremes
    /// `default()` never visits. Deterministic in `rng`; the draw order below
    /// is part of the replay contract once shipped (docs/superpowers/specs/
    /// 2026-07-02-sim-swarm-coverage.md) — do not reorder.
    pub fn swarm(rng: &mut Rng) -> FaultConfig {
        let write_error = if rng.chance(0.5) {
            rng.below(150) as f64 / 1000.0
        } else {
            0.0
        };
        let sync_error = if rng.chance(0.5) {
            rng.below(150) as f64 / 1000.0
        } else {
            0.0
        };
        let (crash_apply, crash_torn) = match rng.below(4) {
            0 => (1.0, 0.0), // all-survive
            1 => (0.0, 0.0), // all-drop
            2 => (0.0, 1.0), // all-torn
            _ => {
                let crash_apply = rng.below(1001) as f64 / 1000.0;
                let crash_torn = (rng.below(1001) as f64 / 1000.0) * (1.0 - crash_apply);
                (crash_apply, crash_torn)
            }
        };
        const DISK_HI: [Duration; 4] = [
            Duration::ZERO,
            Duration::from_micros(100),
            Duration::from_millis(2),
            Duration::from_millis(50),
        ];
        const NET_HI: [Duration; 4] = [
            Duration::ZERO,
            Duration::from_millis(1),
            Duration::from_millis(20),
            Duration::from_millis(250),
        ];
        // The draw order (disk, then net) is part of the frozen replay
        // contract — hoisted so a struct-field reorder cannot change it.
        let disk_hi = DISK_HI[rng.below(4) as usize];
        let net_hi = NET_HI[rng.below(4) as usize];
        let cfg = FaultConfig {
            write_error,
            sync_error,
            crash_apply,
            crash_torn,
            // Not yet swarm-sampled: this axis draws from a field-keyed
            // sub-stream once it lands (segment-mgmt design §4), so it
            // cannot perturb the sequential draws above — a future field
            // composes order-independently. Zero here takes no draw.
            dir_entry_loss: 0.0,
            disk_latency: (Duration::ZERO, disk_hi),
            net_latency: (Duration::ZERO, net_hi),
        };
        cfg.validate(); // a sampler that produces an invalid config is a harness bug
        cfg
    }

    fn validate(&self) {
        for (name, p) in [
            ("write_error", self.write_error),
            ("sync_error", self.sync_error),
            ("crash_apply", self.crash_apply),
            ("crash_torn", self.crash_torn),
            ("dir_entry_loss", self.dir_entry_loss),
        ] {
            assert!(
                (0.0..=1.0).contains(&p),
                "FaultConfig.{name} = {p} — must be in [0, 1]"
            );
        }
        assert!(
            self.crash_apply + self.crash_torn <= 1.0,
            "FaultConfig: crash_apply + crash_torn must not exceed 1 (the rest is the drop probability)"
        );
        assert!(
            self.disk_latency.0 <= self.disk_latency.1,
            "disk_latency range inverted"
        );
        assert!(
            self.net_latency.0 <= self.net_latency.1,
            "net_latency range inverted"
        );
    }
}

struct State {
    /// Virtual time, nanoseconds since universe start.
    now: u64,
    /// Independent streams so app-visible entropy draws don't perturb fault
    /// decisions and vice versa.
    faults: Rng,
    entropy: Rng,
    latency: Rng,
    disk: DiskWorld,
    net: NetWorld,
    /// Sometimes-coverage: named-event counts across the universe (Antithesis
    /// "sometimes assertions", adapted) — see `Sim::cover`.
    coverage: BTreeMap<&'static str, u64>,
}

impl State {
    fn cover(&mut self, key: &'static str) {
        *self.coverage.entry(key).or_insert(0) += 1;
    }

    fn advance(&mut self, (lo, hi): (Duration, Duration)) {
        let lo = u64::try_from(lo.as_nanos()).expect("latency exceeds u64 nanos");
        let hi = u64::try_from(hi.as_nanos()).expect("latency exceeds u64 nanos");
        if hi == 0 {
            return;
        }
        let step = self.latency.between(lo, hi);
        self.advance_by(step);
    }

    fn advance_by(&mut self, nanos: u64) {
        self.now = self
            .now
            .checked_add(nanos)
            .expect("virtual clock overflowed u64 nanos");
    }

    /// Charged by every disk operation: the injected-crash countdown ticks
    /// here, so a crash can land between any two operations.
    fn disk_op(&mut self, cfg: &FaultConfig) -> io::Result<()> {
        if self.disk.crashed {
            return Err(crashed_error());
        }
        if let Some(countdown) = self.disk.crash_countdown {
            if countdown == 0 {
                self.disk.crashed = true;
                self.cover("fault:crash"); // count the transition only
                return Err(crashed_error());
            }
            self.disk.crash_countdown = Some(countdown - 1);
        }
        self.advance(cfg.disk_latency);
        Ok(())
    }
}

struct World {
    seed: u64,
    cfg: FaultConfig,
    state: Mutex<State>,
}

impl World {
    fn lock(&self) -> MutexGuard<'_, State> {
        // Single-threaded: never contended; poison only follows a panic.
        self.state
            .lock()
            .expect("sim world lock poisoned by an earlier panic")
    }
}

fn crashed_error() -> io::Error {
    io::Error::other("simulated crash: the disk is gone until Sim::restart()")
}

pub struct Sim {
    world: Arc<World>,
}

impl Sim {
    pub fn new(seed: u64) -> Sim {
        Sim::with_faults(seed, FaultConfig::default())
    }

    pub fn with_faults(seed: u64, cfg: FaultConfig) -> Sim {
        cfg.validate();
        let mut sm = seed;
        let faults = Rng::new(splitmix64(&mut sm));
        let entropy = Rng::new(splitmix64(&mut sm));
        let latency = Rng::new(splitmix64(&mut sm));
        Sim {
            world: Arc::new(World {
                seed,
                cfg,
                state: Mutex::new(State {
                    now: 0,
                    faults,
                    entropy,
                    latency,
                    disk: DiskWorld::default(),
                    net: NetWorld::default(),
                    coverage: BTreeMap::new(),
                }),
            }),
        }
    }

    pub fn seed(&self) -> u64 {
        self.world.seed
    }

    /// Marks a named event for sometimes-coverage accounting. Workload-side
    /// marks use an "<area>:" prefix by convention (e.g. `"wal:recovered"`).
    pub fn cover(&self, what: &'static str) {
        self.world.lock().cover(what);
    }

    /// The event counts marked so far in this universe (`cover` plus the
    /// sim-internal auto-coverage: `fault:*`, `crash:*`, `restart`).
    pub fn coverage(&self) -> BTreeMap<&'static str, u64> {
        self.world.lock().coverage.clone()
    }

    pub fn now(&self) -> Instant {
        Instant::from_nanos(self.world.lock().now)
    }

    pub fn clock(&self) -> SimClock {
        SimClock {
            world: self.world.clone(),
        }
    }

    pub fn entropy(&self) -> SimEntropy {
        SimEntropy {
            world: self.world.clone(),
        }
    }

    pub fn disk(&self) -> SimDisk {
        SimDisk {
            world: self.world.clone(),
        }
    }

    /// The network endpoint for a named simulated host.
    pub fn net(&self, host: &str) -> SimNet {
        SimNet {
            world: self.world.clone(),
            host: host.to_string(),
        }
    }

    /// Injects a crash on the `ops`-th subsequent disk operation (0 = the
    /// very next one). The op fails, and the disk is gone until `restart()`.
    pub fn crash_disk_after(&self, ops: u64) {
        self.world.lock().disk.crash_countdown = Some(ops);
    }

    pub fn crash_disk_now(&self) {
        let mut guard = self.world.lock();
        if !guard.disk.crashed {
            // Same contract as the countdown path: fault:crash marks the
            // transition into the crashed state, once.
            guard.cover("fault:crash");
        }
        guard.disk.crashed = true;
    }

    /// Power-cycles the disk: every unsynced write independently survives,
    /// tears, or vanishes (see `disk::collapse`), open handles die, and any
    /// pending crash countdown is cleared. Called after a crash — or without
    /// one; a clean power cycle collapses the page cache the same way.
    pub fn restart(&self) {
        let mut guard = self.world.lock();
        guard.cover("restart");
        let State {
            disk,
            faults,
            coverage,
            ..
        } = &mut *guard;
        for file in disk.files.values_mut() {
            crate::disk::collapse(file, faults, &self.world.cfg, coverage);
        }
        // Zero-draw fast path: off by default, so every seed that predates
        // this fault axis replays with the exact same rng sequence (the
        // frozen replay contract).
        if self.world.cfg.dir_entry_loss > 0.0 {
            crate::disk::collapse_dir_entries(
                disk,
                faults,
                self.world.cfg.dir_entry_loss,
                coverage,
            );
        }
        disk.epoch += 1;
        disk.crashed = false;
        disk.crash_countdown = None;
    }

    pub fn partition(&self, a: &str, b: &str) {
        self.world.lock().net.partitions.insert(normalize(a, b));
    }

    pub fn repair(&self, a: &str, b: &str) {
        self.world.lock().net.partitions.remove(&normalize(a, b));
    }

    /// FNV-1a over all durable file contents — for determinism assertions:
    /// the same seed must produce the same fingerprint, forever.
    pub fn durable_fingerprint(&self) -> u64 {
        let guard = self.world.lock();
        let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
        let mut eat = |bytes: &[u8]| {
            for &b in bytes {
                hash = (hash ^ u64::from(b)).wrapping_mul(0x0000_0100_0000_01b3);
            }
        };
        for (name, file) in &guard.disk.files {
            eat(name.as_bytes());
            eat(&(file.durable.len() as u64).to_le_bytes());
            eat(&file.durable);
        }
        hash
    }
}

// ---------------------------------------------------------------- Clock

pub struct SimClock {
    world: Arc<World>,
}

impl Clock for SimClock {
    fn now(&self) -> Instant {
        Instant::from_nanos(self.world.lock().now)
    }

    fn sleep(&self, duration: Duration) {
        let nanos = u64::try_from(duration.as_nanos()).expect("sleep exceeds u64 nanos");
        self.world.lock().advance_by(nanos);
    }
}

// ---------------------------------------------------------------- Entropy

pub struct SimEntropy {
    world: Arc<World>,
}

impl Entropy for SimEntropy {
    fn fill(&self, buf: &mut [u8]) {
        self.world.lock().entropy.fill(buf);
    }
}

// ---------------------------------------------------------------- Disk

pub struct SimDisk {
    world: Arc<World>,
}

impl Disk for SimDisk {
    fn open(&self, path: &str) -> io::Result<Box<dyn File>> {
        let mut guard = self.world.lock();
        guard.disk_op(&self.world.cfg)?;
        if !guard.disk.files.contains_key(path) {
            // A fresh name (including a reopen of one still in
            // pending_removes — that entry is left alone, so its stashed
            // durable bytes can still resurrect): its directory entry is not
            // durable until sync_dir.
            guard
                .disk
                .files
                .insert(path.to_string(), FileState::default());
            guard.disk.pending_creates.insert(path.to_string());
        }
        let epoch = guard.disk.epoch;
        Ok(Box::new(SimFile {
            world: self.world.clone(),
            path: path.to_string(),
            epoch,
        }))
    }

    fn remove(&self, path: &str) -> io::Result<()> {
        let mut guard = self.world.lock();
        guard.disk_op(&self.world.cfg)?;
        let Some(file) = guard.disk.files.remove(path) else {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("remove(\"{path}\"): no such simulated file"),
            ));
        };
        if !guard.disk.pending_creates.remove(path) {
            // The create was already durable (or predates this fault axis
            // mattering); the removal itself is what's now pending — a
            // crash before sync_dir can resurrect these durable bytes.
            guard
                .disk
                .pending_removes
                .insert(path.to_string(), file.durable);
        }
        // else: never durable, so dropping both entries loses nothing a
        // crash could resurrect.
        Ok(())
    }

    fn list(&self, prefix: &str) -> io::Result<Vec<String>> {
        let mut guard = self.world.lock();
        guard.disk_op(&self.world.cfg)?;
        Ok(guard
            .disk
            .files
            .keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect())
    }

    fn sync_dir(&self, prefix: &str) -> io::Result<()> {
        let mut guard = self.world.lock();
        guard.disk_op(&self.world.cfg)?;
        if guard.faults.chance(self.world.cfg.sync_error) {
            guard.cover("fault:sync_error");
            // Same contract as File::sync: a failed barrier leaves the
            // durability of every entry it would have covered unknown.
            return Err(io::Error::other(
                "simulated fsync failure on a directory barrier — durability of the directory \
                 entries it covers is unknown",
            ));
        }
        guard
            .disk
            .pending_creates
            .retain(|name| !name.starts_with(prefix));
        guard
            .disk
            .pending_removes
            .retain(|name, _| !name.starts_with(prefix));
        Ok(())
    }
}

pub struct SimFile {
    world: Arc<World>,
    path: String,
    epoch: u64,
}

impl SimFile {
    /// Epoch check + op charge, then the file state — one lock for the op.
    fn op<T>(
        &self,
        f: impl FnOnce(&mut State, &FaultConfig, &str) -> io::Result<T>,
    ) -> io::Result<T> {
        let mut guard = self.world.lock();
        if guard.disk.epoch != self.epoch {
            return Err(io::Error::other(format!(
                "file handle for \"{}\" predates a simulated crash — contract: reopen files after Sim::restart()",
                self.path
            )));
        }
        guard.disk_op(&self.world.cfg)?;
        f(&mut guard, &self.world.cfg, &self.path)
    }
}

fn file_state<'a>(state: &'a mut State, path: &str) -> &'a mut disk::FileState {
    state
        .disk
        .files
        .get_mut(path)
        .unwrap_or_else(|| panic!("simulated file \"{path}\" removed while a handle was open"))
}

impl File for SimFile {
    fn len(&self) -> io::Result<u64> {
        self.op(|state, _, path| Ok(file_state(state, path).view().len() as u64))
    }

    fn read_at(&self, offset: u64, out: &mut [u8]) -> io::Result<()> {
        self.op(|state, _, path| {
            let view = file_state(state, path).view();
            let end = offset.checked_add(out.len() as u64).expect("read_at range overflowed u64");
            if end > view.len() as u64 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    format!(
                        "read_at past end of \"{path}\": requested [{offset}, {end}) but len is {} — contract: read_at reads exactly out.len() bytes inside the file",
                        view.len()
                    ),
                ));
            }
            out.copy_from_slice(&view[offset as usize..end as usize]);
            Ok(())
        })
    }

    fn write_at(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        self.op(|state, cfg, path| {
            if data.is_empty() {
                return Ok(());
            }
            if state.faults.chance(cfg.write_error) {
                state.cover("fault:write_error");
                return Err(io::Error::other(
                    "simulated disk write error (injected fault)",
                ));
            }
            file_state(state, path).pending.push(PendingOp::Write {
                offset,
                data: data.to_vec(),
            });
            Ok(())
        })
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
        self.op(|state, _, path| {
            file_state(state, path)
                .pending
                .push(PendingOp::SetLen { len });
            Ok(())
        })
    }

    fn sync(&self) -> io::Result<()> {
        self.op(|state, cfg, path| {
            if state.faults.chance(cfg.sync_error) {
                state.cover("fault:sync_error");
                // Real fsync failure semantics: nothing can be assumed. The
                // buffer stays volatile; a later crash collapses it as usual.
                return Err(io::Error::other(
                    "simulated fsync failure — durability of writes since the last successful sync is unknown",
                ));
            }
            let file = file_state(state, path);
            file.durable = file.view();
            file.pending.clear();
            Ok(())
        })
    }
}

// ---------------------------------------------------------------- Net

pub struct SimNet {
    world: Arc<World>,
    host: String,
}

impl Net for SimNet {
    fn listen(&self, addr: &str) -> io::Result<Box<dyn Listener>> {
        assert_eq!(
            host_of(addr),
            self.host,
            "a simulated host can only listen on its own name"
        );
        let mut guard = self.world.lock();
        if guard.net.listeners.contains_key(addr) {
            return Err(io::Error::new(
                io::ErrorKind::AddrInUse,
                format!("\"{addr}\" already has a simulated listener"),
            ));
        }
        guard
            .net
            .listeners
            .insert(addr.to_string(), Default::default());
        Ok(Box::new(SimListener {
            world: self.world.clone(),
            addr: addr.to_string(),
        }))
    }

    fn connect(&self, addr: &str) -> io::Result<Box<dyn Conn>> {
        let target = host_of(addr).to_string();
        let mut guard = self.world.lock();
        if guard.net.partitioned(&self.host, &target) {
            return Err(io::Error::new(
                io::ErrorKind::ConnectionRefused,
                format!(
                    "simulated partition between \"{}\" and \"{target}\"",
                    self.host
                ),
            ));
        }
        if !guard.net.listeners.contains_key(addr) {
            return Err(io::Error::new(
                io::ErrorKind::ConnectionRefused,
                format!("nothing is listening on \"{addr}\" in this simulation"),
            ));
        }
        let id = guard.net.next_conn;
        guard.net.next_conn += 1;
        guard.net.conns.insert(
            id,
            ConnState {
                host_a: self.host.clone(),
                host_b: target,
                listen_addr: addr.to_string(),
                to_a: Default::default(),
                to_b: Default::default(),
                open_a: true,
                open_b: true,
            },
        );
        guard
            .net
            .listeners
            .get_mut(addr)
            .expect("checked above")
            .push_back(id);
        Ok(Box::new(SimConn {
            world: self.world.clone(),
            id,
            is_a: true,
        }))
    }
}

pub struct SimListener {
    world: Arc<World>,
    addr: String,
}

impl Listener for SimListener {
    fn accept(&self) -> io::Result<Option<Box<dyn Conn>>> {
        let mut guard = self.world.lock();
        let backlog = guard
            .net
            .listeners
            .get_mut(&self.addr)
            .expect("listener state removed while the handle was open");
        match backlog.pop_front() {
            Some(id) => Ok(Some(Box::new(SimConn {
                world: self.world.clone(),
                id,
                is_a: false,
            }))),
            None => Ok(None),
        }
    }

    fn local_addr(&self) -> String {
        self.addr.clone()
    }
}

impl Drop for SimListener {
    fn drop(&mut self) {
        self.world.lock().net.listeners.remove(&self.addr);
    }
}

pub struct SimConn {
    world: Arc<World>,
    id: u64,
    is_a: bool,
}

impl Conn for SimConn {
    fn send(&self, data: &[u8]) -> io::Result<()> {
        let mut guard = self.world.lock();
        let deliver_at = {
            let (lo, hi) = self.world.cfg.net_latency;
            let lo = u64::try_from(lo.as_nanos()).expect("latency exceeds u64 nanos");
            let hi = u64::try_from(hi.as_nanos()).expect("latency exceeds u64 nanos");
            let latency = if hi == 0 {
                0
            } else {
                guard.latency.between(lo, hi)
            };
            guard.now + latency
        };
        let state = guard
            .net
            .conns
            .get(&self.id)
            .expect("connection state removed");
        let partitioned = guard.net.partitioned(&state.host_a, &state.host_b);
        let state = guard
            .net
            .conns
            .get_mut(&self.id)
            .expect("connection state removed");
        let (my_open, peer_open) = if self.is_a {
            (state.open_a, state.open_b)
        } else {
            (state.open_b, state.open_a)
        };
        assert!(my_open, "send on a connection this side already closed");
        if !peer_open {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "simulated peer closed the connection",
            ));
        }
        if partitioned {
            // Partition semantics: the send succeeds locally and the packet
            // is lost, like the real thing.
            return Ok(());
        }
        let queue = if self.is_a {
            &mut state.to_b
        } else {
            &mut state.to_a
        };
        queue.push_back((deliver_at, data.to_vec()));
        Ok(())
    }

    fn recv(&self, out: &mut [u8]) -> io::Result<Option<usize>> {
        let mut guard = self.world.lock();
        let now = guard.now;
        let state = guard
            .net
            .conns
            .get_mut(&self.id)
            .expect("connection state removed");
        let (queue, peer_open) = if self.is_a {
            (&mut state.to_a, state.open_b)
        } else {
            (&mut state.to_b, state.open_a)
        };
        match queue.front() {
            Some((deliver_at, data)) if *deliver_at <= now => {
                assert!(
                    out.len() >= data.len(),
                    "recv buffer ({}) smaller than message ({}) — sim Net is message-oriented; size the buffer to the protocol's largest message",
                    out.len(),
                    data.len()
                );
                let n = data.len();
                out[..n].copy_from_slice(data);
                queue.pop_front();
                Ok(Some(n))
            }
            Some(_) => Ok(None), // in flight; virtual time hasn't reached it
            None if !peer_open => Ok(Some(0)),
            None => Ok(None),
        }
    }

    fn peer_addr(&self) -> String {
        let guard = self.world.lock();
        let state = guard
            .net
            .conns
            .get(&self.id)
            .expect("connection state removed");
        if self.is_a {
            state.listen_addr.clone()
        } else {
            state.host_a.clone()
        }
    }
}

impl Drop for SimConn {
    fn drop(&mut self) {
        if let Some(state) = self.world.lock().net.conns.get_mut(&self.id) {
            if self.is_a {
                state.open_a = false;
            } else {
                state.open_b = false;
            }
        }
    }
}

// ---------------------------------------------------------------- Harness

/// Runs `universes` seeded universes from a fresh base seed. A failure panics
/// with the seed and a one-command repro (DESIGN.md §7: the failure message
/// IS the reproduction) — call as `run_universes!(N, f)`; the macro fills in
/// the calling package and test file so the repro command is exact. Set
/// `CHARPENTE_SEED=0x…` to replay one universe.
#[macro_export]
macro_rules! run_universes {
    ($universes:expr, $f:expr) => {
        $crate::run_universes_in(env!("CARGO_PKG_NAME"), file!(), $universes, $f)
    };
}

/// `run_universes!` with a caller-chosen base seed, for tests that must be
/// deterministic in CI (e.g. the harness-catches-the-bug meta-tests).
#[macro_export]
macro_rules! run_universes_from {
    ($base:expr, $universes:expr, $f:expr) => {
        $crate::run_universes_from_in(env!("CARGO_PKG_NAME"), file!(), $base, $universes, $f)
    };
}

#[doc(hidden)]
pub fn run_universes_in(
    package: &str,
    file: &str,
    universes: u64,
    f: impl Fn(&mut Sim) -> Result<(), String>,
) {
    run_universes_from_in(package, file, ambient_base_seed(), universes, f);
}

#[doc(hidden)]
pub fn run_universes_from_in(
    package: &str,
    file: &str,
    base: u64,
    universes: u64,
    f: impl Fn(&mut Sim) -> Result<(), String>,
) {
    let test_file = test_name(file);
    if let Ok(seed_var) = std::env::var("CHARPENTE_SEED") {
        let seed = parse_seed(&seed_var);
        eprintln!("charpente-sim [{test_file}]: replaying seed {seed:#018x}");
        run_one(package, test_file, seed, &f);
        return;
    }
    eprintln!("charpente-sim [{test_file}]: {universes} universes from base seed {base:#018x}");
    let mut state = base;
    for _ in 0..universes {
        run_one(package, test_file, splitmix64(&mut state), &f);
    }
}

/// `"crates/charpente/tests/typed_tables.rs"` → `"typed_tables"`.
fn test_name(file: &str) -> &str {
    let stem = file.rsplit('/').next().unwrap_or(file);
    stem.strip_suffix(".rs").unwrap_or(stem)
}

fn run_one(package: &str, test_file: &str, seed: u64, f: &impl Fn(&mut Sim) -> Result<(), String>) {
    let mut sim = Sim::new(seed);
    if let Err(msg) = f(&mut sim) {
        panic!("{}", failure_report(package, test_file, seed, &msg));
    }
}

fn failure_report(package: &str, test_file: &str, seed: u64, msg: &str) -> String {
    format!(
        "\nsim failure in {test_file}: {msg}\n  seed:  {seed:#018x}\n  repro: CHARPENTE_SEED={seed:#x} cargo test -p {package} --test {test_file} -- --nocapture\n"
    )
}

fn parse_seed(raw: &str) -> u64 {
    let hex = raw
        .strip_prefix("0x")
        .or_else(|| raw.strip_prefix("0X"))
        .unwrap_or(raw);
    u64::from_str_radix(hex, 16).unwrap_or_else(|_| {
        panic!("CHARPENTE_SEED must be a hex seed like 0xdeadbeef — got \"{raw}\"")
    })
}

// The single sanctioned wall-clock read in the workspace: universes must be
// fresh each run (the evidence bar is "green over N fresh seeds"), and a
// failure prints its exact seed, so freshness costs no reproducibility.
#[allow(clippy::disallowed_methods, clippy::disallowed_types)]
fn ambient_base_seed() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is set before 1970")
        .as_nanos();
    let mut state = nanos as u64;
    splitmix64(&mut state)
}

// -------------------------------------------------------- Swarm harness

/// Runs `universes` seeded universes, each sampling its own `FaultConfig` via
/// `FaultConfig::swarm` (swarm testing) instead of one fixed matrix for every
/// universe, and accumulating sometimes-coverage across the batch. Add
/// `require: [...]` to assert each named key was covered at least once in the
/// batch — call as `run_universes_swarm!(N, f)` or `run_universes_swarm!(N,
/// f, require: ["key", ...])`. `run_universes!`/`run_one` are a separate,
/// frozen runner — this one is new. Set `CHARPENTE_SEED=0x…` to replay one
/// universe (its config re-derives from the seed; the coverage requirement
/// check is skipped, since one universe can't be expected to hit everything).
#[macro_export]
macro_rules! run_universes_swarm {
    ($universes:expr, $f:expr) => {
        $crate::run_universes_swarm_in(env!("CARGO_PKG_NAME"), file!(), $universes, &[], $f)
    };
    ($universes:expr, $f:expr, require: [$($key:expr),* $(,)?]) => {
        $crate::run_universes_swarm_in(env!("CARGO_PKG_NAME"), file!(), $universes, &[$($key),*], $f)
    };
}

#[doc(hidden)]
pub fn run_universes_swarm_in(
    package: &str,
    file: &str,
    universes: u64,
    require: &[&str],
    f: impl Fn(&mut Sim) -> Result<(), String>,
) {
    // Ambient base, no config override — delegates to the `_with` runner
    // added alongside `run_universes_swarm_with!`; a no-op adjust makes this
    // byte-for-byte the same seed→behavior mapping as before that macro
    // existed (`run_universe`, below, is untouched and still the shared
    // contract with the CLI harness).
    run_universes_swarm_with_in(
        package,
        file,
        ambient_base_seed(),
        universes,
        require,
        |_cfg: &mut FaultConfig| {},
        f,
    );
}

/// `run_universes_swarm!` with a per-universe `FaultConfig` override: after
/// the swarm samples a universe's config, `adjust` runs on it before the
/// universe executes — for tests that want the swarm's per-universe variety
/// with one axis forced hot (e.g. `dir_entry_loss`, not yet swarm-sampled).
/// Unlike `run_universes_swarm!`, the base seed is explicit — this is for
/// permanent, always-in-the-suite properties that must not flake batch to
/// batch. A NEW macro: `run_universe`'s frozen replay contract is untouched
/// (`run_universe_with_adjust`, below, is a separate function). Call as
/// `run_universes_swarm_with!(base, N, adjust, f)` or with `require: [...]`.
#[macro_export]
macro_rules! run_universes_swarm_with {
    ($base:expr, $universes:expr, $adjust:expr, $f:expr) => {
        $crate::run_universes_swarm_with_in(
            env!("CARGO_PKG_NAME"),
            file!(),
            $base,
            $universes,
            &[],
            $adjust,
            $f,
        )
    };
    ($base:expr, $universes:expr, $adjust:expr, $f:expr, require: [$($key:expr),* $(,)?]) => {
        $crate::run_universes_swarm_with_in(
            env!("CARGO_PKG_NAME"),
            file!(),
            $base,
            $universes,
            &[$($key),*],
            $adjust,
            $f,
        )
    };
}

#[doc(hidden)]
#[allow(clippy::too_many_arguments)]
pub fn run_universes_swarm_with_in(
    package: &str,
    file: &str,
    base: u64,
    universes: u64,
    require: &[&str],
    adjust: impl Fn(&mut FaultConfig),
    f: impl Fn(&mut Sim) -> Result<(), String>,
) {
    let test_file = test_name(file);
    if let Ok(seed_var) = std::env::var("CHARPENTE_SEED") {
        let seed = parse_seed(&seed_var);
        eprintln!(
            "charpente-sim [{test_file}]: replaying seed {seed:#018x} — coverage requirements skipped under CHARPENTE_SEED"
        );
        run_one_swarm_with(package, test_file, seed, &adjust, &f);
        return;
    }
    eprintln!(
        "charpente-sim [{test_file}]: {universes} swarm universes from base seed {base:#018x}"
    );
    let mut state = base;
    let mut coverage: BTreeMap<&'static str, u64> = BTreeMap::new();
    for _ in 0..universes {
        let seed = splitmix64(&mut state);
        for (key, count) in run_one_swarm_with(package, test_file, seed, &adjust, &f) {
            *coverage.entry(key).or_insert(0) += count;
        }
    }
    eprintln!("charpente-sim [{test_file}]: coverage over {universes} universes:");
    for (key, count) in &coverage {
        eprintln!("  {key}: {count}");
    }
    let missing: Vec<&str> = require
        .iter()
        .filter(|key| coverage.get(*key).copied().unwrap_or(0) == 0)
        .copied()
        .collect();
    assert!(
        missing.is_empty(),
        "sim coverage requirement failed in {test_file}: {missing:?} never occurred over \
         {universes} universes — raise the universe count, adjust the workload, or drop the \
         requirement (a required state that never occurs is an untested claim)"
    );
}

/// One universe, shared by the frozen swarm macro and the CLI harness
/// (harness.rs). The seed→behavior mapping here is the frozen replay
/// contract: config derives from `splitmix64(seed)`, the workload runs
/// under catch_unwind, Ok returns coverage. Do not reorder anything.
// `FaultConfig` crossed clippy's large-Err threshold the moment
// `dir_entry_loss` landed; boxing the Err would touch this frozen function's
// signature (and every caller) for a lint, not a behavior change.
#[allow(clippy::result_large_err)]
pub(crate) fn run_universe(
    seed: u64,
    f: &dyn Fn(&mut Sim) -> Result<(), String>,
) -> Result<BTreeMap<&'static str, u64>, (FaultConfig, String)> {
    let mut s = seed;
    let cfg_seed = splitmix64(&mut s);
    let cfg = FaultConfig::swarm(&mut Rng::new(cfg_seed));
    let mut sim = Sim::with_faults(seed, cfg.clone());
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| f(&mut sim))) {
        Ok(Ok(())) => Ok(sim.coverage()),
        Ok(Err(msg)) => Err((cfg, msg)),
        Err(payload) => Err((cfg, panic_message(&*payload))),
    }
}

/// Like `run_universe`, but applies `adjust` to the sampled config first — a
/// separate function so `run_universe`'s frozen contract is never touched.
#[allow(clippy::result_large_err)] // see run_universe
fn run_universe_with_adjust(
    seed: u64,
    adjust: &dyn Fn(&mut FaultConfig),
    f: &dyn Fn(&mut Sim) -> Result<(), String>,
) -> Result<BTreeMap<&'static str, u64>, (FaultConfig, String)> {
    let mut s = seed;
    let cfg_seed = splitmix64(&mut s);
    let mut cfg = FaultConfig::swarm(&mut Rng::new(cfg_seed));
    adjust(&mut cfg);
    cfg.validate(); // an adjust closure that produces an invalid config is a workload bug
    let mut sim = Sim::with_faults(seed, cfg.clone());
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| f(&mut sim))) {
        Ok(Ok(())) => Ok(sim.coverage()),
        Ok(Err(msg)) => Err((cfg, msg)),
        Err(payload) => Err((cfg, panic_message(&*payload))),
    }
}

fn run_one_swarm_with(
    package: &str,
    test_file: &str,
    seed: u64,
    adjust: &impl Fn(&mut FaultConfig),
    f: &impl Fn(&mut Sim) -> Result<(), String>,
) -> BTreeMap<&'static str, u64> {
    match run_universe_with_adjust(seed, adjust, f) {
        Ok(coverage) => coverage,
        Err((cfg, msg)) => panic!(
            "{}",
            swarm_failure_report(package, test_file, seed, &cfg, &msg)
        ),
    }
}

fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "<non-string panic payload>".to_string()
    }
}

fn swarm_failure_report(
    package: &str,
    test_file: &str,
    seed: u64,
    cfg: &FaultConfig,
    msg: &str,
) -> String {
    format!(
        "\nsim failure in {test_file}: {msg}\n  seed:   {seed:#018x}\n  config: {cfg:?}\n  repro:  CHARPENTE_SEED={seed:#x} cargo test -p {package} --test {test_file} -- --nocapture\n"
    )
}

// ------------------------------------------------------------ App harness

/// Generates the app's `tests/sim.rs` main (spec §App-side convention):
/// explicit registration, `routes![]`/`jobs![]` idiom. Requires
/// `harness = false` on the test target.
#[macro_export]
macro_rules! sim_tests {
    ($($name:ident $(require [$($key:expr),* $(,)?])?),+ $(,)?) => {
        fn main() {
            let tests: &[$crate::harness::SimTest] = &[$(
                $crate::harness::SimTest {
                    name: stringify!($name),
                    run: $name,
                    require: &[$($($key),*)?],
                },
            )+];
            let args = match $crate::harness::HarnessArgs::parse(::std::env::args().skip(1)) {
                Ok(args) => args,
                Err(usage) => {
                    ::std::eprintln!("{usage}");
                    ::std::process::exit(2);
                }
            };
            let code = $crate::harness::run(tests, &args, &mut ::std::io::stdout().lock());
            ::std::process::exit(code);
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn virtual_time_advances_only_by_simulation() {
        let sim = Sim::with_faults(1, FaultConfig::none());
        assert_eq!(sim.now(), Instant::ZERO);
        sim.clock().sleep(Duration::from_secs(3600));
        assert_eq!(sim.now(), Instant::from_nanos(3_600_000_000_000));
    }

    #[test]
    fn disk_ops_advance_virtual_time() {
        let sim = Sim::new(2);
        let disk = sim.disk();
        let before = sim.now();
        disk.open("f").unwrap();
        assert!(sim.now() > before, "a disk op must cost virtual latency");
    }

    #[test]
    fn sim_file_handles_are_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>(_: &T) {}
        let sim = Sim::new(5);
        let file = sim.disk().open("f").unwrap();
        assert_send_sync(&file);
    }

    #[test]
    fn synced_data_survives_restart_and_handles_die() {
        let sim = Sim::with_faults(3, FaultConfig::none());
        let disk = sim.disk();
        let file = disk.open("f").unwrap();
        file.write_at(0, b"durable").unwrap();
        file.sync().unwrap();
        file.write_at(7, b" volatile").unwrap();

        sim.restart();

        let err = file.len().unwrap_err();
        assert!(
            err.to_string()
                .contains("reopen files after Sim::restart()"),
            "{err}"
        );

        let file = disk.open("f").unwrap();
        // FaultConfig::none() drops every unsynced write at restart.
        assert_eq!(file.len().unwrap(), 7);
        let mut out = [0u8; 7];
        file.read_at(0, &mut out).unwrap();
        assert_eq!(&out, b"durable");
    }

    #[test]
    fn crash_countdown_kills_the_nth_op() {
        let sim = Sim::with_faults(4, FaultConfig::none());
        let disk = sim.disk();
        let file = disk.open("f").unwrap();
        sim.crash_disk_after(2);
        file.write_at(0, b"a").unwrap(); // countdown 2 -> 1
        file.write_at(1, b"b").unwrap(); // countdown 1 -> 0
        let err = file.sync().unwrap_err(); // countdown 0 -> crash
        assert!(err.to_string().contains("simulated crash"), "{err}");
        // Everything after the crash fails too.
        assert!(file.len().is_err());
        assert!(disk.open("g").is_err());
    }

    #[test]
    fn crash_disk_now_covers_the_transition_once() {
        let sim = Sim::with_faults(6, FaultConfig::none());
        sim.crash_disk_now();
        sim.crash_disk_now(); // already crashed: not a second transition
        assert_eq!(sim.coverage().get("fault:crash"), Some(&1));
    }

    #[test]
    fn same_seed_same_fingerprint() {
        let run = |seed| {
            let sim = Sim::new(seed);
            let disk = sim.disk();
            let file = disk.open("f").unwrap();
            let mut payload = [0u8; 64];
            sim.entropy().fill(&mut payload);
            let _ = file.write_at(0, &payload);
            let _ = file.sync();
            sim.restart();
            (sim.durable_fingerprint(), sim.now())
        };
        assert_eq!(run(99), run(99));
    }

    #[test]
    fn failure_report_contains_seed_and_repro() {
        let report = failure_report(
            "charpente-sim",
            "crash_recovery",
            0xABCD,
            "record 3 corrupt",
        );
        assert!(report.contains("seed:  0x000000000000abcd"), "{report}");
        assert!(
            report.contains(
                "CHARPENTE_SEED=0xabcd cargo test -p charpente-sim --test crash_recovery"
            ),
            "{report}"
        );
    }

    #[test]
    fn test_name_strips_path_and_extension() {
        assert_eq!(
            test_name("crates/charpente/tests/typed_tables.rs"),
            "typed_tables"
        );
        assert_eq!(test_name("wal.rs"), "wal");
    }

    // ---------------------------------------------------------- swarm

    #[test]
    fn swarm_same_seed_same_config() {
        let a = FaultConfig::swarm(&mut Rng::new(0x5EED));
        let b = FaultConfig::swarm(&mut Rng::new(0x5EED));
        assert_eq!(format!("{a:?}"), format!("{b:?}"));
    }

    #[test]
    fn swarm_configs_vary() {
        let mut rng = Rng::new(0xC0FFEE);
        let mut debug_strings = std::collections::BTreeSet::new();
        let mut saw_write_error_zero = false;
        let mut saw_write_error_nonzero = false;
        for _ in 0..64 {
            let cfg = FaultConfig::swarm(&mut rng);
            cfg.validate();
            assert!(cfg.crash_apply + cfg.crash_torn <= 1.0);
            assert!(cfg.disk_latency.0 <= cfg.disk_latency.1);
            assert!(cfg.net_latency.0 <= cfg.net_latency.1);
            if cfg.write_error == 0.0 {
                saw_write_error_zero = true;
            } else {
                saw_write_error_nonzero = true;
            }
            debug_strings.insert(format!("{cfg:?}"));
        }
        assert!(debug_strings.len() >= 2, "swarm configs never varied");
        assert!(saw_write_error_zero, "never sampled write_error == 0.0");
        assert!(saw_write_error_nonzero, "never sampled write_error > 0.0");
    }

    #[test]
    fn cover_counts_marks() {
        let sim = Sim::new(1);
        sim.cover("x");
        sim.cover("x");
        assert_eq!(sim.coverage()["x"], 2);
    }

    #[test]
    fn auto_coverage_fires() {
        let sim = Sim::with_faults(
            2,
            FaultConfig {
                write_error: 1.0,
                ..FaultConfig::none()
            },
        );
        let file = sim.disk().open("f").unwrap();
        let err = file.write_at(0, b"x").unwrap_err();
        assert!(err.to_string().contains("injected fault"), "{err}");
        assert!(sim.coverage()["fault:write_error"] >= 1);

        let sim = Sim::with_faults(3, FaultConfig::none());
        let file = sim.disk().open("f").unwrap();
        file.write_at(0, b"unsynced").unwrap();
        sim.restart();
        assert_eq!(sim.coverage()["restart"], 1);
        assert!(
            sim.coverage().keys().any(|k| k.starts_with("crash:")),
            "{:?}",
            sim.coverage()
        );
    }

    #[test]
    fn swarm_runner_reports_seed_on_panic() {
        let payload = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            run_universes_swarm_in(
                "charpente-sim",
                "swarm_meta.rs",
                1,
                &[],
                |_sim: &mut Sim| -> Result<(), String> { panic!("boom") },
            );
        }))
        .expect_err("the runner must panic when the workload panics");
        let msg = panic_message(&*payload);
        assert!(msg.contains("seed:"), "{msg}");
        assert!(msg.contains("config:"), "{msg}");
        assert!(msg.contains("CHARPENTE_SEED="), "{msg}");
    }

    #[test]
    fn swarm_runner_requirement_failure_names_the_key() {
        let payload = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            run_universes_swarm_in(
                "charpente-sim",
                "swarm_meta.rs",
                4,
                &["never:happens"],
                |_sim: &mut Sim| Ok(()),
            );
        }))
        .expect_err("a never-covered required key must fail the batch");
        let msg = panic_message(&*payload);
        assert!(msg.contains("never:happens"), "{msg}");
        assert!(msg.contains("raise the universe count"), "{msg}");
    }

    #[test]
    fn swarm_universe_replays_identically() {
        let run = |seed: u64| {
            let mut s = seed;
            let cfg_seed = splitmix64(&mut s);
            let cfg = FaultConfig::swarm(&mut Rng::new(cfg_seed));
            let sim = Sim::with_faults(seed, cfg);
            let disk = sim.disk();
            let file = disk.open("f").unwrap();
            let mut payload = [0u8; 64];
            sim.entropy().fill(&mut payload);
            let _ = file.write_at(0, &payload);
            let _ = file.sync();
            sim.restart();
            (sim.durable_fingerprint(), sim.now())
        };
        assert_eq!(run(0x51E4), run(0x51E4));
    }
}
