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
pub mod object;
pub mod rng;

use std::collections::BTreeMap;
use std::io;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use charpente_core::io::{Clock, Conn, Disk, Entropy, File, Instant, Listener, Net};

use crate::disk::{DiskWorld, FileState, PendingOp, SECTOR};
use crate::net::{ConnState, ListenerState, NetWorld, host_of, normalize};
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
    /// P(`Sim::restart` flips one random bit of one random byte in one
    /// random file's already-durable content, before per-file crash
    /// collapse runs) — bit-level corruption at rest (the bitrot model).
    pub bit_flip: f64,
    /// P(a `write_at`'s payload lands at `offset + 512*(1 + below(4))`
    /// instead of the requested `offset` — the intended target stays
    /// stale, and the call still returns `Ok`: a disk that silently wrote
    /// the right bytes in the wrong place).
    pub misdirected_write: f64,
    /// P(a `read_at` returns a transient injected IO error — the next read
    /// of the same bytes may succeed).
    pub read_error: f64,
    /// P(an `ObjectOp::Put` fails and the object does NOT land).
    pub object_put_error: f64,
    /// P(an `ObjectOp::Get`/`List` returns a transient injected IO error).
    pub object_get_error: f64,
    /// P(a `Put` lands durably but the caller is told it failed — the case
    /// that makes retry-idempotence load-bearing. Not a duplicate of
    /// `object_put_error`: that one leaves the object absent, this one does
    /// not. There is deliberately no torn-object axis; a PUT is atomic.
    pub object_lost_ack: f64,
    /// P(an `ObjectOp::Delete` fails). The archive's only destructive op, so
    /// this is the axis that asks whether reclamation stays correct and keeps
    /// advancing when deletes fail. Rolled from its OWN stream
    /// (`keyed_stream(seed, "object_store_delete")`), not the shared
    /// object-store stream — see `SimObjectStore::with_faults`.
    pub object_delete_error: f64,
    /// Path prefix scoping the durable-corruption axes: `bit_flip` and
    /// `misdirected_write` only affect files whose path starts with this
    /// prefix. These two axes exist to prove the WAL-layer fences (frame
    /// CRC + the recovery-horizon rule) — the domain v0 can honestly
    /// defend; corruption of the table store's own bytes is beneath those
    /// fences and defeats redb's crash consistency independently of
    /// anything this codebase does (Noah's ruling, 2026-07-07: redb + M1
    /// restore is the v0 answer, M2's own checksummed engine is the fix).
    /// `read_error` and `dir_entry_loss` stay global — they produce
    /// errors, never silent wrongness. Not swarm-sampled (constant
    /// default; not a drawn field).
    pub corruption_scope: String,
    /// P(a `send` kills the connection: the wire dies mid-conversation —
    /// the sender gets `ConnectionReset`, the peer reads EOF, and every
    /// in-flight message in both directions is dropped). Rolls on the
    /// global fault stream: a connection spans two hosts, and the wire
    /// belongs to neither.
    pub conn_drop: f64,
    /// P(a `send`'s drawn latency is ×64 — a latency spike; the message
    /// is delayed, never lost). The replication spec's `ship_latency`,
    /// landed as a superset over every connection: the sim cannot and
    /// should not know which conns are "replication".
    pub net_spike: f64,
    /// The realtime tail's slow/bounded-consumer rate. Unlike every other
    /// axis above, nothing inside this crate reads it — `SimSink`'s stall
    /// closure lives in the `charpente` crate (T5) and takes no `Sim`
    /// reference at all, so there is no internal consumption site to wire.
    /// This field exists purely so a workload can read it back
    /// (`Sim::subscriber_stall`) to drive its own closure at a per-universe
    /// rate, the same way every other extended field varies per universe —
    /// the roll itself, and the `fault:subscriber_stall` coverage mark, are
    /// the workload's own responsibility (realtime T6).
    pub subscriber_stall: f64,
    pub disk_latency: (Duration, Duration),
    pub net_latency: (Duration, Duration),
}

const CORRUPTION_SCOPE_DEFAULT: &str = "wal/";

impl Default for FaultConfig {
    fn default() -> FaultConfig {
        FaultConfig {
            write_error: 0.02,
            sync_error: 0.02,
            crash_apply: 0.4,
            crash_torn: 0.3,
            dir_entry_loss: 0.0,
            bit_flip: 0.0,
            misdirected_write: 0.0,
            read_error: 0.0,
            object_put_error: 0.0,
            object_get_error: 0.0,
            object_lost_ack: 0.0,
            object_delete_error: 0.0,
            corruption_scope: CORRUPTION_SCOPE_DEFAULT.to_string(),
            conn_drop: 0.0,
            net_spike: 0.0,
            subscriber_stall: 0.0,
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
            bit_flip: 0.0,
            misdirected_write: 0.0,
            read_error: 0.0,
            object_put_error: 0.0,
            object_get_error: 0.0,
            object_lost_ack: 0.0,
            object_delete_error: 0.0,
            corruption_scope: CORRUPTION_SCOPE_DEFAULT.to_string(),
            conn_drop: 0.0,
            net_spike: 0.0,
            subscriber_stall: 0.0,
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
            // These ten axes draw from field-keyed sub-streams
            // (`sample_extended_fields`, applied in `run_universe`/
            // `run_universe_with_adjust` right after this function returns)
            // rather than this sequential stream, so they cannot perturb the
            // draws above — each composes order-independently and adding
            // one changes no other field's draws. Zero here takes no draw;
            // a caller that constructs `FaultConfig::swarm` directly (e.g.
            // this module's own unit tests) without also calling
            // `sample_extended_fields` gets these off, deterministically.
            dir_entry_loss: 0.0,
            bit_flip: 0.0,
            misdirected_write: 0.0,
            read_error: 0.0,
            object_put_error: 0.0,
            object_get_error: 0.0,
            object_lost_ack: 0.0,
            object_delete_error: 0.0,
            conn_drop: 0.0,
            net_spike: 0.0,
            subscriber_stall: 0.0,
            // Constant, never drawn: the scope is a property of what the
            // corruption axes are FOR (the WAL fences), not a fault rate.
            corruption_scope: CORRUPTION_SCOPE_DEFAULT.to_string(),
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
            ("bit_flip", self.bit_flip),
            ("misdirected_write", self.misdirected_write),
            ("read_error", self.read_error),
            ("object_put_error", self.object_put_error),
            ("object_get_error", self.object_get_error),
            ("object_lost_ack", self.object_lost_ack),
            ("object_delete_error", self.object_delete_error),
            ("conn_drop", self.conn_drop),
            ("net_spike", self.net_spike),
            ("subscriber_stall", self.subscriber_stall),
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

/// Extended fault fields draw from per-field sub-streams keyed by (universe
/// seed, field name) — never from `FaultConfig::swarm`'s legacy sequential
/// stream above (this file's frozen replay contract). Adding a field here
/// changes no existing universe's legacy draws and no other extended
/// field's draws: each stream is independent of the seed's position in any
/// other stream. Called from `run_universe`/`run_universe_with_adjust`
/// immediately after `FaultConfig::swarm`, so every swarm-sampled universe
/// (the production CLI harness and every workspace swarm test alike) also
/// gets these axes sampled — this is also how Task 1's `dir_entry_loss`
/// becomes swarm-sampled, since it draws here too.
pub fn sample_extended_fields(seed: u64, cfg: &mut FaultConfig) {
    let mut r = keyed_stream(seed, "bit_flip");
    cfg.bit_flip = if r.chance(0.25) {
        r.below(20) as f64 / 1000.0
    } else {
        0.0
    };
    let mut r = keyed_stream(seed, "misdirected_write");
    cfg.misdirected_write = if r.chance(0.25) {
        r.below(10) as f64 / 1000.0
    } else {
        0.0
    };
    let mut r = keyed_stream(seed, "read_error");
    cfg.read_error = if r.chance(0.25) {
        r.below(50) as f64 / 1000.0
    } else {
        0.0
    };
    let mut r = keyed_stream(seed, "dir_entry_loss");
    cfg.dir_entry_loss = if r.chance(0.25) {
        r.below(500) as f64 / 1000.0
    } else {
        0.0
    };
    let mut r = keyed_stream(seed, "conn_drop");
    cfg.conn_drop = if r.chance(0.25) {
        r.below(30) as f64 / 1000.0
    } else {
        0.0
    };
    let mut r = keyed_stream(seed, "net_spike");
    cfg.net_spike = if r.chance(0.25) {
        r.below(200) as f64 / 1000.0
    } else {
        0.0
    };
    let mut r = keyed_stream(seed, "subscriber_stall");
    cfg.subscriber_stall = if r.chance(0.25) {
        r.below(250) as f64 / 1000.0
    } else {
        0.0
    };
    let mut r = keyed_stream(seed, "object_put_error");
    cfg.object_put_error = if r.chance(0.25) {
        r.below(50) as f64 / 1000.0
    } else {
        0.0
    };
    let mut r = keyed_stream(seed, "object_get_error");
    cfg.object_get_error = if r.chance(0.25) {
        r.below(50) as f64 / 1000.0
    } else {
        0.0
    };
    let mut r = keyed_stream(seed, "object_lost_ack");
    cfg.object_lost_ack = if r.chance(0.20) {
        r.below(20) as f64 / 1000.0
    } else {
        0.0
    };
    let mut r = keyed_stream(seed, "object_delete_error");
    cfg.object_delete_error = if r.chance(0.20) {
        r.below(20) as f64 / 1000.0
    } else {
        0.0
    };
}

/// A stream independent of both the legacy sequential draw and every other
/// keyed stream: `seed` XORed with an FNV-1a hash of `name`, expanded through
/// splitmix64 into a fresh xoshiro256++ state. Used for `sample_extended_fields`'s
/// per-field fault axes and for a host's `faults`/`entropy` streams
/// (`HostWorld::new`) — same construction, so a host's rolls never depend on
/// other hosts' op interleaving or on the frozen global streams.
fn keyed_stream(seed: u64, name: &str) -> Rng {
    let mut h = 0xcbf2_9ce4_8422_2325u64; // FNV-1a, matching crc.rs's style
    for b in name.bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    let mut s = seed ^ h;
    Rng::new(splitmix64(&mut s))
}

/// One simulated host's disk + fault/entropy streams. Reuses `DiskWorld`
/// unchanged (`crashed`/`crash_countdown`/`epoch` already carry a single
/// host's crash model); `faults`/`entropy` are keyed sub-streams
/// (`keyed_stream`), so a host's rolls never depend on other hosts' op
/// interleaving, and no host perturbs the frozen global `State.faults`/
/// `State.entropy` streams. Latency is NOT per-host: virtual time is one
/// dimension shared by the whole universe (`State.latency`/`State.now`).
struct HostWorld {
    disk: DiskWorld,
    faults: Rng,
    entropy: Rng,
}

impl HostWorld {
    fn new(seed: u64, host: &str) -> HostWorld {
        HostWorld {
            disk: DiskWorld::default(),
            faults: keyed_stream(seed, &format!("host-faults:{host}")),
            entropy: keyed_stream(seed, &format!("host-entropy:{host}")),
        }
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
    /// Per-host worlds, created lazily by `Sim::host`. NEW state: no existing
    /// single-node universe ever populates this map, so the frozen global
    /// world above is untouched by this task (the replay contract).
    hosts: BTreeMap<String, HostWorld>,
}

impl State {
    fn cover(&mut self, key: &'static str) {
        *self.coverage.entry(key).or_insert(0) += 1;
    }

    /// Panics if `name` was never created via `Sim::host` — a `SimHost`
    /// handle can only be produced by `Sim::host`, which creates the entry
    /// first, so reaching this with a missing name is a framework bug, not a
    /// caller error.
    fn host_mut(&mut self, name: &str) -> &mut HostWorld {
        self.hosts.get_mut(name).unwrap_or_else(|| {
            panic!("simulated host \"{name}\" used before Sim::host(\"{name}\") created it")
        })
    }

    fn world_of(&mut self, host: &Option<String>) -> &mut DiskWorld {
        match host {
            None => &mut self.disk,
            Some(name) => &mut self.host_mut(name).disk,
        }
    }

    fn faults_of(&mut self, host: &Option<String>) -> &mut Rng {
        match host {
            None => &mut self.faults,
            Some(name) => &mut self.host_mut(name).faults,
        }
    }

    fn entropy_of(&mut self, host: &Option<String>) -> &mut Rng {
        match host {
            None => &mut self.entropy,
            Some(name) => &mut self.host_mut(name).entropy,
        }
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
    /// here, so a crash can land between any two operations. `host` selects
    /// the global world (`None`) or a per-host world (`Some`, `crash:host`
    /// coverage instead of `fault:crash` — a host crash is a new axis, not
    /// the global disk crash) — either way the transition also tears down
    /// that host's network presence (`NetWorld::teardown_host`).
    fn disk_op(&mut self, cfg: &FaultConfig, host: &Option<String>) -> io::Result<()> {
        let world = self.world_of(host);
        if world.crashed {
            return Err(crashed_error());
        }
        let mut newly_crashed = false;
        if let Some(countdown) = world.crash_countdown {
            if countdown == 0 {
                world.crashed = true;
                newly_crashed = true;
            } else {
                world.crash_countdown = Some(countdown - 1);
            }
        }
        if newly_crashed {
            match host {
                Some(name) => {
                    self.cover("crash:host"); // count the transition only
                    self.net.teardown_host(name);
                }
                None => self.cover("fault:crash"), // count the transition only
            }
            return Err(crashed_error());
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

/// FNV-1a over a disk world's durable file contents — shared by
/// `Sim::durable_fingerprint` and `SimHost::durable_fingerprint`, so both
/// produce the exact same bytes over the file map each is given.
fn fingerprint_files(files: &BTreeMap<String, FileState>) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    let mut eat = |bytes: &[u8]| {
        for &b in bytes {
            hash = (hash ^ u64::from(b)).wrapping_mul(0x0000_0100_0000_01b3);
        }
    };
    for (name, file) in files {
        eat(name.as_bytes());
        eat(&(file.durable.len() as u64).to_le_bytes());
        eat(&file.durable);
    }
    hash
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
                    hosts: BTreeMap::new(),
                }),
            }),
        }
    }

    pub fn seed(&self) -> u64 {
        self.world.seed
    }

    /// This universe's sampled `subscriber_stall` rate — the one extended
    /// field with no internal consumption site (see the field's doc). A
    /// workload reads this to drive its own `SimSink` stall closure (`charpente`'s
    /// `SimSink` lives outside this crate and has no `Sim` reference of its
    /// own) at this universe's rate, and marks `fault:subscriber_stall`
    /// coverage itself when the roll actually fires.
    pub fn subscriber_stall(&self) -> f64 {
        self.world.cfg.subscriber_stall
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
            host: None,
        }
    }

    pub fn disk(&self) -> SimDisk {
        SimDisk {
            world: self.world.clone(),
            host: None,
        }
    }

    /// The network endpoint for a named simulated host.
    pub fn net(&self, host: &str) -> SimNet {
        SimNet {
            world: self.world.clone(),
            host: host.to_string(),
        }
    }

    /// A named simulated host: its own disk, its own keyed fault/entropy
    /// streams, and (via `SimHost::net`) the shared `NetWorld` under its
    /// name. Created lazily on first call and reused by name thereafter —
    /// `sim.host("a")` twice returns handles onto the same host world.
    pub fn host(&self, name: &str) -> SimHost {
        let mut guard = self.world.lock();
        let seed = self.world.seed;
        guard
            .hosts
            .entry(name.to_string())
            .or_insert_with(|| HostWorld::new(seed, name));
        SimHost {
            world: self.world.clone(),
            name: name.to_string(),
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
        // Bit-level corruption of already-durable bytes (the bitrot model),
        // before per-file crash collapse: one restart-time roll picks at
        // most one file (by rng index over sorted names — BTreeMap iterates
        // sorted already) with non-empty durable content inside
        // `corruption_scope` and flips one random bit of one random byte.
        // Zero-draw fast path: off by default, so every seed that predates
        // this fault axis replays with the exact same rng sequence (the
        // frozen replay contract).
        if self.world.cfg.bit_flip > 0.0 && faults.chance(self.world.cfg.bit_flip) {
            let names: Vec<String> = disk
                .files
                .iter()
                .filter(|(name, f)| {
                    name.starts_with(&self.world.cfg.corruption_scope) && !f.durable.is_empty()
                })
                .map(|(name, _)| name.clone())
                .collect();
            if !names.is_empty() {
                let name = &names[faults.below(names.len() as u64) as usize];
                let file = disk
                    .files
                    .get_mut(name)
                    .expect("name was just read from disk.files");
                let byte_idx = faults.below(file.durable.len() as u64) as usize;
                let bit = faults.below(8) as u32;
                file.durable[byte_idx] ^= 1 << bit;
                *coverage.entry("fault:bit_flip").or_insert(0) += 1;
            }
        }
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
        fingerprint_files(&self.world.lock().disk.files)
    }

    /// The earliest virtual time at which a currently in-flight network
    /// message becomes deliverable (over every connection), or `None` when no
    /// message is in flight. A multi-node test scheduler jumps here when no
    /// node has immediate work — the deterministic analogue of blocking on a
    /// socket. Read-only: draws no randomness and mutates nothing, so it
    /// never perturbs a seed's replay.
    pub fn next_wakeup(&self) -> Option<u64> {
        let guard = self.world.lock();
        let now = guard.now;
        let mut next: Option<u64> = None;
        for conn in guard.net.conns.values() {
            // A conn delivers its queues in FIFO order, so only the front of
            // each matters; a front already due (<= now) was drained by
            // pump_io, and one for a dead conn no live node reads is ignored.
            for (deliver_at, _) in [conn.to_a.front(), conn.to_b.front()].into_iter().flatten() {
                if *deliver_at > now {
                    next = Some(next.map_or(*deliver_at, |n| n.min(*deliver_at)));
                }
            }
        }
        next
    }
}

/// A named simulated host: its own disk world, its own keyed fault/entropy
/// streams, and the shared `NetWorld` under its name. Obtained from
/// `Sim::host` (which creates the underlying world on first call); `Clone`
/// just duplicates the handle onto that same world.
#[derive(Clone)]
pub struct SimHost {
    world: Arc<World>,
    name: String,
}

impl SimHost {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn disk(&self) -> SimDisk {
        SimDisk {
            world: self.world.clone(),
            host: Some(self.name.clone()),
        }
    }

    pub fn entropy(&self) -> SimEntropy {
        SimEntropy {
            world: self.world.clone(),
            host: Some(self.name.clone()),
        }
    }

    /// Same as `sim.net(host.name())` — `NetWorld` is not split per host
    /// (listeners/conns are already keyed by address/id), only the disk and
    /// fault/entropy streams are.
    pub fn net(&self) -> SimNet {
        SimNet {
            world: self.world.clone(),
            host: self.name.clone(),
        }
    }

    /// Crashes this host now: its disk starts erroring until `restart()`,
    /// and (on the transition into crashed) its network presence tears down
    /// (`NetWorld::teardown_host`) — mirrors `Sim::crash_disk_now`, scoped to
    /// this host.
    pub fn crash_now(&self) {
        let mut guard = self.world.lock();
        let host = guard.host_mut(&self.name);
        let was_crashed = host.disk.crashed;
        host.disk.crashed = true;
        if !was_crashed {
            guard.cover("crash:host");
            guard.net.teardown_host(&self.name);
        }
    }

    /// Injects a crash on the `ops`-th subsequent disk operation *on this
    /// host* — mirrors `Sim::crash_disk_after`.
    pub fn crash_after(&self, ops: u64) {
        self.world.lock().host_mut(&self.name).disk.crash_countdown = Some(ops);
    }

    /// Power-cycles this host: same crash-collapse model as `Sim::restart`
    /// (bit-level corruption roll, then per-file collapse, then dir-entry
    /// collapse), scoped to this host's disk and fault stream, then this
    /// host's network presence tears down — a clean power cycle drops
    /// connections too, and this also makes a crashed host's re-`listen`
    /// possible even if `crash_now` was never called (teardown is
    /// idempotent).
    pub fn restart(&self) {
        let mut guard = self.world.lock();
        let cfg = self.world.cfg.clone();
        guard.host_mut(&self.name); // asserts the invariant; borrow ends immediately
        {
            let State {
                hosts, coverage, ..
            } = &mut *guard;
            let host = hosts.get_mut(&self.name).expect("checked above");
            let HostWorld { disk, faults, .. } = host;
            // Bit-level corruption of already-durable bytes, before per-file
            // crash collapse — see `Sim::restart`'s doc for the full model;
            // this is that same roll, scoped to this host's files and faults
            // stream.
            if cfg.bit_flip > 0.0 && faults.chance(cfg.bit_flip) {
                let names: Vec<String> = disk
                    .files
                    .iter()
                    .filter(|(name, f)| {
                        name.starts_with(&cfg.corruption_scope) && !f.durable.is_empty()
                    })
                    .map(|(name, _)| name.clone())
                    .collect();
                if !names.is_empty() {
                    let name = &names[faults.below(names.len() as u64) as usize];
                    let file = disk
                        .files
                        .get_mut(name)
                        .expect("name was just read from disk.files");
                    let byte_idx = faults.below(file.durable.len() as u64) as usize;
                    let bit = faults.below(8) as u32;
                    file.durable[byte_idx] ^= 1 << bit;
                    *coverage.entry("fault:bit_flip").or_insert(0) += 1;
                }
            }
            for file in disk.files.values_mut() {
                crate::disk::collapse(file, faults, &cfg, coverage);
            }
            if cfg.dir_entry_loss > 0.0 {
                crate::disk::collapse_dir_entries(disk, faults, cfg.dir_entry_loss, coverage);
            }
            disk.epoch += 1;
            disk.crashed = false;
            disk.crash_countdown = None;
        }
        guard.net.teardown_host(&self.name);
    }

    /// FNV-1a over this host's durable file contents — see
    /// `Sim::durable_fingerprint`.
    pub fn durable_fingerprint(&self) -> u64 {
        let mut guard = self.world.lock();
        fingerprint_files(&guard.host_mut(&self.name).disk.files)
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
    host: Option<String>,
}

impl Entropy for SimEntropy {
    fn fill(&self, buf: &mut [u8]) {
        self.world.lock().entropy_of(&self.host).fill(buf);
    }
}

// `next_u64` (used by the host-entropy test below, and by every workload
// that wants a raw draw) is `Entropy`'s own default method, built on `fill`
// — no inherent method needed here, and adding a shadowing one would only
// make every other call site's `Entropy` import look unused to clippy.

// ---------------------------------------------------------------- Disk

pub struct SimDisk {
    world: Arc<World>,
    /// `None` = the frozen global world; `Some(host)` = that host's world.
    host: Option<String>,
}

impl Disk for SimDisk {
    fn open(&self, path: &str) -> io::Result<Box<dyn File>> {
        let mut guard = self.world.lock();
        guard.disk_op(&self.world.cfg, &self.host)?;
        if !guard.world_of(&self.host).files.contains_key(path) {
            // A fresh name (including a reopen of one still in
            // pending_removes — that entry is left alone, so its stashed
            // durable bytes can still resurrect): its directory entry is not
            // durable until sync_dir.
            guard
                .world_of(&self.host)
                .files
                .insert(path.to_string(), FileState::default());
            guard
                .world_of(&self.host)
                .pending_creates
                .insert(path.to_string());
        }
        let epoch = guard.world_of(&self.host).epoch;
        Ok(Box::new(SimFile {
            world: self.world.clone(),
            host: self.host.clone(),
            path: path.to_string(),
            epoch,
        }))
    }

    fn remove(&self, path: &str) -> io::Result<()> {
        let mut guard = self.world.lock();
        guard.disk_op(&self.world.cfg, &self.host)?;
        let Some(file) = guard.world_of(&self.host).files.remove(path) else {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("remove(\"{path}\"): no such simulated file"),
            ));
        };
        if !guard.world_of(&self.host).pending_creates.remove(path) {
            // The create was already durable (or predates this fault axis
            // mattering); the removal itself is what's now pending — a
            // crash before sync_dir can resurrect these durable bytes.
            guard
                .world_of(&self.host)
                .pending_removes
                .insert(path.to_string(), file.durable);
        }
        // else: never durable, so dropping both entries loses nothing a
        // crash could resurrect.
        Ok(())
    }

    fn list(&self, prefix: &str) -> io::Result<Vec<String>> {
        let mut guard = self.world.lock();
        guard.disk_op(&self.world.cfg, &self.host)?;
        Ok(guard
            .world_of(&self.host)
            .files
            .keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect())
    }

    fn sync_dir(&self, prefix: &str) -> io::Result<()> {
        let mut guard = self.world.lock();
        guard.disk_op(&self.world.cfg, &self.host)?;
        if guard
            .faults_of(&self.host)
            .chance(self.world.cfg.sync_error)
        {
            guard.cover("fault:sync_error");
            // Same contract as File::sync: a failed barrier leaves the
            // durability of every entry it would have covered unknown.
            return Err(io::Error::other(
                "simulated fsync failure on a directory barrier — durability of the directory \
                 entries it covers is unknown",
            ));
        }
        guard
            .world_of(&self.host)
            .pending_creates
            .retain(|name| !name.starts_with(prefix));
        guard
            .world_of(&self.host)
            .pending_removes
            .retain(|name, _| !name.starts_with(prefix));
        Ok(())
    }
}

pub struct SimFile {
    world: Arc<World>,
    host: Option<String>,
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
        if guard.world_of(&self.host).epoch != self.epoch {
            return Err(io::Error::other(format!(
                "file handle for \"{}\" predates a simulated crash — contract: reopen files after Sim::restart()",
                self.path
            )));
        }
        guard.disk_op(&self.world.cfg, &self.host)?;
        f(&mut guard, &self.world.cfg, &self.path)
    }
}

fn file_state<'a>(
    state: &'a mut State,
    host: &Option<String>,
    path: &str,
) -> &'a mut disk::FileState {
    state
        .world_of(host)
        .files
        .get_mut(path)
        .unwrap_or_else(|| panic!("simulated file \"{path}\" removed while a handle was open"))
}

impl File for SimFile {
    fn len(&self) -> io::Result<u64> {
        self.op(|state, _, path| Ok(file_state(state, &self.host, path).view_len()))
    }

    fn read_at(&self, offset: u64, out: &mut [u8]) -> io::Result<()> {
        self.op(|state, cfg, path| {
            if cfg.read_error > 0.0 && state.faults_of(&self.host).chance(cfg.read_error) {
                state.cover("fault:read_error");
                // Transient: the disk itself is fine, so a later read of the
                // same bytes may succeed — unlike a write/sync fault, this
                // never touches the file's content or pending queue.
                return Err(io::Error::other("injected read error"));
            }
            let file = file_state(state, &self.host, path);
            let len = file.view_len();
            let end = offset.checked_add(out.len() as u64).expect("read_at range overflowed u64");
            if end > len {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    format!(
                        "read_at past end of \"{path}\": requested [{offset}, {end}) but len is {len} — contract: read_at reads exactly out.len() bytes inside the file"
                    ),
                ));
            }
            file.read_into(offset, out);
            Ok(())
        })
    }

    fn write_at(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        self.op(|state, cfg, path| {
            if data.is_empty() {
                return Ok(());
            }
            if state.faults_of(&self.host).chance(cfg.write_error) {
                state.cover("fault:write_error");
                return Err(io::Error::other(
                    "simulated disk write error (injected fault)",
                ));
            }
            // Misdirected write: the payload lands past the requested
            // offset instead of at it — the intended target stays stale,
            // and the call still returns `Ok`. That's the whole treachery:
            // nothing here or at the caller can tell the write went wrong.
            // Scoped to `corruption_scope` (see its field docs); the scope
            // check precedes the roll, so out-of-scope writes consume no
            // draws — a zero-draw fast path in both dimensions.
            let mut target_offset = offset;
            if cfg.misdirected_write > 0.0
                && path.starts_with(&cfg.corruption_scope)
                && state.faults_of(&self.host).chance(cfg.misdirected_write)
            {
                state.cover("fault:misdirected_write");
                target_offset = offset + SECTOR * (1 + state.faults_of(&self.host).below(4));
            }
            file_state(state, &self.host, path)
                .pending
                .push(PendingOp::Write {
                    offset: target_offset,
                    data: data.to_vec(),
                });
            Ok(())
        })
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
        self.op(|state, _, path| {
            file_state(state, &self.host, path)
                .pending
                .push(PendingOp::SetLen { len });
            Ok(())
        })
    }

    fn sync(&self) -> io::Result<()> {
        self.op(|state, cfg, path| {
            if state.faults_of(&self.host).chance(cfg.sync_error) {
                state.cover("fault:sync_error");
                // Real fsync failure semantics: nothing can be assumed. The
                // buffer stays volatile; a later crash collapses it as usual.
                return Err(io::Error::other(
                    "simulated fsync failure — durability of writes since the last successful sync is unknown",
                ));
            }
            let file = file_state(state, &self.host, path);
            // Same result as `file.durable = file.view()`, minus the clone of the
            // durable image that `view()` would make and then discard — the
            // shape `collapse` already uses.
            let pending = std::mem::take(&mut file.pending);
            file.durable = crate::disk::apply(std::mem::take(&mut file.durable), &pending);
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
        let token = guard.net.next_listener;
        guard.net.next_listener += 1;
        guard.net.listeners.insert(
            addr.to_string(),
            ListenerState {
                backlog: Default::default(),
                token,
            },
        );
        Ok(Box::new(SimListener {
            world: self.world.clone(),
            addr: addr.to_string(),
            token,
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
            .backlog
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
    /// This listen generation's identity — a crashed host's re-`listen` on
    /// the same address gets a fresh token, so a stale handle from before
    /// the crash can't accept on (or, in `Drop`, clobber) the new one.
    token: u64,
}

impl Listener for SimListener {
    fn accept(&self) -> io::Result<Option<Box<dyn Conn>>> {
        let mut guard = self.world.lock();
        let listener = match guard.net.listeners.get_mut(&self.addr) {
            Some(l) if l.token == self.token => l,
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::NotConnected,
                    format!(
                        "listener \"{}\" is dead — its host crashed or it was already closed",
                        self.addr
                    ),
                ));
            }
        };
        match listener.backlog.pop_front() {
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
        let mut guard = self.world.lock();
        if guard.net.listeners.get(&self.addr).map(|l| l.token) == Some(self.token) {
            guard.net.listeners.remove(&self.addr);
        }
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
        // Wire death, rolled before the latency draw. Both net axes roll on
        // the GLOBAL fault stream: a connection spans two hosts, and the
        // wire belongs to neither. Zero-draw when off — every seed that
        // predates these axes replays with the exact same rng sequence (the
        // frozen replay contract). The sender's side closes (this call gets
        // the reset; a retry gets NotConnected) and both in-flight queues
        // drop; the peer's side stays nominally open so it observes the
        // death the way a socket shows it — EOF on recv, BrokenPipe on send.
        if self.world.cfg.conn_drop > 0.0 && guard.faults.chance(self.world.cfg.conn_drop) {
            guard.cover("fault:conn_drop");
            let state = guard
                .net
                .conns
                .get_mut(&self.id)
                .expect("connection state removed");
            if self.is_a {
                state.open_a = false;
            } else {
                state.open_b = false;
            }
            state.to_a.clear();
            state.to_b.clear();
            return Err(io::Error::new(
                io::ErrorKind::ConnectionReset,
                "injected connection drop — the wire died mid-conversation; reconnect",
            ));
        }
        let deliver_at = {
            let (lo, hi) = self.world.cfg.net_latency;
            let lo = u64::try_from(lo.as_nanos()).expect("latency exceeds u64 nanos");
            let hi = u64::try_from(hi.as_nanos()).expect("latency exceeds u64 nanos");
            let mut latency = if hi == 0 {
                0
            } else {
                guard.latency.between(lo, hi)
            };
            // Latency spike: this message's drawn latency ×64 — delayed,
            // never lost. `.max(1)` keeps a spike observable even under a
            // zero-latency config.
            if self.world.cfg.net_spike > 0.0 && guard.faults.chance(self.world.cfg.net_spike) {
                guard.cover("fault:net_spike");
                latency = latency.saturating_mul(64).max(1);
            }
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
        if !my_open {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "send on a connection whose host crashed or closed it — reconnect after restart",
            ));
        }
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
        let (queue, my_open, peer_open) = if self.is_a {
            (&mut state.to_a, state.open_a, state.open_b)
        } else {
            (&mut state.to_b, state.open_b, state.open_a)
        };
        if !my_open {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "recv on a connection whose host crashed or closed it — reconnect after restart",
            ));
        }
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
        &[],
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
/// `run_universes_swarm_with!(base, N, adjust, f)`, with `require: [...]`, or
/// with `require: [...], floor: [("key", N), ...]` — a floor is `require:`'s
/// at-least-once generalized to an at-least-N measured rate, for a property
/// whose oracle can decay toward vacuous without ever reaching zero.
#[macro_export]
macro_rules! run_universes_swarm_with {
    ($base:expr, $universes:expr, $adjust:expr, $f:expr) => {
        $crate::run_universes_swarm_with_in(
            env!("CARGO_PKG_NAME"),
            file!(),
            $base,
            $universes,
            &[],
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
            &[],
            $adjust,
            $f,
        )
    };
    ($base:expr, $universes:expr, $adjust:expr, $f:expr,
     require: [$($key:expr),* $(,)?],
     floor: [$(($fkey:expr, $min:expr)),* $(,)?]) => {
        $crate::run_universes_swarm_with_in(
            env!("CARGO_PKG_NAME"),
            file!(),
            $base,
            $universes,
            &[$($key),*],
            &[$(($fkey, $min)),*],
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
    floors: &[(&str, u64)],
    adjust: impl Fn(&mut FaultConfig),
    f: impl Fn(&mut Sim) -> Result<(), String>,
) {
    let test_file = test_name(file);
    if let Ok(seed_var) = std::env::var("CHARPENTE_SEED") {
        let seed = parse_seed(&seed_var);
        eprintln!(
            "charpente-sim [{test_file}]: replaying seed {seed:#018x} — coverage requirements skipped under CHARPENTE_SEED"
        );
        let coverage = run_one_swarm_with(package, test_file, seed, &adjust, &f);
        // The batch path reports coverage; a single-seed replay was throwing it
        // away, which is what you most want when asking why *this* universe
        // behaved (or cost) the way it did.
        eprintln!("charpente-sim [{test_file}]: coverage for this universe:");
        for (key, count) in &coverage {
            eprintln!("  {key}: {count}");
        }
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
    // `require:` has a floor of one, which catches an oracle that stopped firing
    // entirely and misses one that decayed to a token handful. `floor:` is the
    // same check with the measured rate as the threshold.
    let below: Vec<String> = floors
        .iter()
        .filter_map(|(key, min)| {
            let seen = coverage.get(*key).copied().unwrap_or(0);
            (seen < *min).then(|| format!("{key}: {seen} of {min} required"))
        })
        .collect();
    assert!(
        below.is_empty(),
        "sim coverage floor failed in {test_file}: {below:?} over {universes} universes. \
         A floor is a MEASURED rate, not a target: it fell because the property reaches \
         that state less often than when the number was taken, and why is the finding. \
         Investigate before touching it — lowering a floor to match is how a real oracle \
         becomes decorative with the suite still green."
    );
}

/// One universe, shared by the frozen swarm macro and the CLI harness
/// (harness.rs). The seed→behavior mapping here is the frozen replay
/// contract: config derives from `splitmix64(seed)`, the workload runs
/// under catch_unwind, Ok returns coverage. Do not reorder anything.
/// `sample_extended_fields` runs right after `FaultConfig::swarm` — its own
/// sub-streams (keyed off `seed`, not `cfg_seed`) never touch the legacy
/// draw above, so this addition changes no existing universe's replay
/// (segment-mgmt T5's pin test proves it).
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
    let mut cfg = FaultConfig::swarm(&mut Rng::new(cfg_seed));
    sample_extended_fields(seed, &mut cfg);
    let mut sim = Sim::with_faults(seed, cfg.clone());
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| f(&mut sim))) {
        Ok(Ok(())) => Ok(sim.coverage()),
        Ok(Err(msg)) => Err((cfg, msg)),
        Err(payload) => Err((cfg, panic_message(&*payload))),
    }
}

/// Like `run_universe`, but applies `adjust` to the sampled config first — a
/// separate function so `run_universe`'s frozen contract is never touched.
/// `sample_extended_fields` runs before `adjust`, same placement as
/// `run_universe`, so a test forcing one axis hot (`adjust`) always wins
/// over whatever that axis's sub-stream happened to sample.
#[allow(clippy::result_large_err)] // see run_universe
fn run_universe_with_adjust(
    seed: u64,
    adjust: &dyn Fn(&mut FaultConfig),
    f: &dyn Fn(&mut Sim) -> Result<(), String>,
) -> Result<BTreeMap<&'static str, u64>, (FaultConfig, String)> {
    let mut s = seed;
    let cfg_seed = splitmix64(&mut s);
    let mut cfg = FaultConfig::swarm(&mut Rng::new(cfg_seed));
    sample_extended_fields(seed, &mut cfg);
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
    fn extended_field_sampling_never_touches_the_legacy_stream() {
        // The frozen contract's discriminating test: for a fixed seed, the
        // legacy fields must be IDENTICAL with and without extended sampling.
        let mut s = 0x5EED_F00D_u64;
        let cfg_seed = splitmix64(&mut s);
        let legacy = FaultConfig::swarm(&mut Rng::new(cfg_seed));
        let mut extended = legacy.clone();
        sample_extended_fields(0x5EED_F00D, &mut extended);
        assert_eq!(legacy.write_error, extended.write_error);
        assert_eq!(legacy.sync_error, extended.sync_error);
        assert_eq!(legacy.crash_apply, extended.crash_apply);
        assert_eq!(legacy.crash_torn, extended.crash_torn);
        // And the extended fields are deterministic per (seed, field name):
        let mut again = legacy.clone();
        sample_extended_fields(0x5EED_F00D, &mut again);
        assert_eq!(extended.bit_flip, again.bit_flip);
        assert_eq!(extended.misdirected_write, again.misdirected_write);
        assert_eq!(extended.read_error, again.read_error);
        assert_eq!(extended.dir_entry_loss, again.dir_entry_loss);
        assert_eq!(extended.conn_drop, again.conn_drop);
        assert_eq!(extended.net_spike, again.net_spike);
        assert_eq!(extended.subscriber_stall, again.subscriber_stall);
    }

    #[test]
    fn extended_sampling_of_net_axes_is_order_independent() {
        // Same contract as the pin test above, extended to the net axes: the
        // legacy sequential draws are byte-identical with and without extended
        // sampling, and each net axis depends only on (seed, field name) —
        // recomputing it from its own keyed stream alone, with no other field
        // sampled, yields exactly what `sample_extended_fields` produced.
        let seed = 0x0D20_5EED_u64;
        let mut s = seed;
        let cfg_seed = splitmix64(&mut s);
        let legacy = FaultConfig::swarm(&mut Rng::new(cfg_seed));
        let mut extended = legacy.clone();
        sample_extended_fields(seed, &mut extended);
        assert_eq!(legacy.write_error, extended.write_error);
        assert_eq!(legacy.sync_error, extended.sync_error);
        assert_eq!(legacy.crash_apply, extended.crash_apply);
        assert_eq!(legacy.crash_torn, extended.crash_torn);
        assert_eq!(legacy.disk_latency, extended.disk_latency);
        assert_eq!(legacy.net_latency, extended.net_latency);

        let mut r = keyed_stream(seed, "conn_drop");
        let conn_drop = if r.chance(0.25) {
            r.below(30) as f64 / 1000.0
        } else {
            0.0
        };
        assert_eq!(extended.conn_drop, conn_drop);
        let mut r = keyed_stream(seed, "net_spike");
        let net_spike = if r.chance(0.25) {
            r.below(200) as f64 / 1000.0
        } else {
            0.0
        };
        assert_eq!(extended.net_spike, net_spike);
    }

    // ---------------------------------------------- fault-matrix extensions

    #[test]
    fn read_error_fires_transiently_and_is_covered() {
        let sim = Sim::with_faults(
            0x2EAD_0001,
            FaultConfig {
                read_error: 1.0,
                ..FaultConfig::none()
            },
        );
        let file = sim.disk().open("f").unwrap();
        file.write_at(0, b"hello").unwrap();
        file.sync().unwrap();
        let mut out = [0u8; 5];
        let err = file.read_at(0, &mut out).unwrap_err();
        assert!(err.to_string().contains("injected read error"), "{err}");
        assert_eq!(sim.coverage()["fault:read_error"], 1);

        // Off by default: no draw, no error.
        let sim = Sim::with_faults(0x2EAD_0002, FaultConfig::none());
        let file = sim.disk().open("f").unwrap();
        file.write_at(0, b"hello").unwrap();
        file.sync().unwrap();
        let mut out = [0u8; 5];
        file.read_at(0, &mut out).unwrap();
        assert_eq!(&out, b"hello");
    }

    #[test]
    fn misdirected_write_leaves_the_intended_offset_stale_but_returns_ok() {
        let sim = Sim::with_faults(
            0x0FF_0001,
            FaultConfig {
                misdirected_write: 1.0,
                ..FaultConfig::none()
            },
        );
        let file = sim.disk().open("wal/f").unwrap();
        file.write_at(0, b"payload!").unwrap(); // still Ok — the treachery
        file.sync().unwrap();
        let mut out = [0u8; 8];
        file.read_at(0, &mut out).unwrap();
        assert_ne!(
            &out, b"payload!",
            "the intended offset must stay stale — the bytes landed elsewhere"
        );
        assert!(
            file.len().unwrap() > 8,
            "the misdirected payload must land beyond the intended offset"
        );
        assert_eq!(sim.coverage()["fault:misdirected_write"], 1);
    }

    #[test]
    fn misdirected_write_never_touches_files_outside_the_corruption_scope() {
        // Even at probability 1.0, a write outside `corruption_scope`
        // (default "wal/") lands exactly where it was aimed — the axis
        // exists to prove the WAL fences, not to model store-file bitrot
        // (Noah's ruling, 2026-07-07).
        let sim = Sim::with_faults(
            0x0FF_0002,
            FaultConfig {
                misdirected_write: 1.0,
                ..FaultConfig::none()
            },
        );
        let file = sim.disk().open("tables.redb").unwrap();
        file.write_at(0, b"payload!").unwrap();
        file.sync().unwrap();
        let mut out = [0u8; 8];
        file.read_at(0, &mut out).unwrap();
        assert_eq!(&out, b"payload!", "out-of-scope writes land where aimed");
        assert_eq!(file.len().unwrap(), 8);
        assert_eq!(sim.coverage().get("fault:misdirected_write"), None);
    }

    #[test]
    fn bit_flip_corrupts_exactly_one_bit_of_one_file_at_restart() {
        let sim = Sim::with_faults(
            0xB17_0001,
            FaultConfig {
                bit_flip: 1.0,
                ..FaultConfig::none()
            },
        );
        let file = sim.disk().open("wal/f").unwrap();
        let original = vec![0xAAu8; 64];
        file.write_at(0, &original).unwrap();
        file.sync().unwrap();

        sim.restart();

        let file = sim.disk().open("wal/f").unwrap();
        let mut after = vec![0u8; 64];
        file.read_at(0, &mut after).unwrap();
        let mut diff_bytes = 0;
        let mut total_flipped_bits = 0u32;
        for (a, b) in original.iter().zip(&after) {
            if a != b {
                diff_bytes += 1;
                total_flipped_bits += (a ^ b).count_ones();
            }
        }
        assert_eq!(diff_bytes, 1, "exactly one byte must differ: {after:?}");
        assert_eq!(
            total_flipped_bits, 1,
            "exactly one bit must differ: {after:?}"
        );
        assert_eq!(sim.coverage()["fault:bit_flip"], 1);
    }

    #[test]
    fn bit_flip_never_picks_a_file_outside_the_corruption_scope() {
        // A durable out-of-scope file next to an empty scope: nothing to
        // corrupt, even at probability 1.0 (Noah's ruling, 2026-07-07).
        let sim = Sim::with_faults(
            0xB17_0003,
            FaultConfig {
                bit_flip: 1.0,
                ..FaultConfig::none()
            },
        );
        let file = sim.disk().open("tables.redb").unwrap();
        let original = vec![0x55u8; 64];
        file.write_at(0, &original).unwrap();
        file.sync().unwrap();

        sim.restart();

        let file = sim.disk().open("tables.redb").unwrap();
        let mut after = vec![0u8; 64];
        file.read_at(0, &mut after).unwrap();
        assert_eq!(after, original, "out-of-scope durable bytes stay intact");
        assert_eq!(sim.coverage().get("fault:bit_flip"), None);
    }

    #[test]
    fn bit_flip_is_a_no_op_with_no_non_empty_durable_file() {
        // No file has ever been synced: bit_flip has nothing to corrupt, and
        // must not panic reaching for one.
        let sim = Sim::with_faults(
            0xB17_0002,
            FaultConfig {
                bit_flip: 1.0,
                ..FaultConfig::none()
            },
        );
        let _file = sim.disk().open("wal/f").unwrap(); // never written or synced
        sim.restart();
        assert_eq!(sim.coverage().get("fault:bit_flip"), None);
    }

    #[test]
    fn conn_drop_kills_the_connection_and_is_covered() {
        let sim = Sim::with_faults(
            0xD20_0001,
            FaultConfig {
                conn_drop: 1.0,
                ..FaultConfig::none()
            },
        );
        let a = sim.host("a");
        let b = sim.host("b");
        let listener = b.net().listen("b:1").unwrap();
        let conn = a.net().connect("b:1").unwrap();
        let server = poll_accept(&sim, &*listener);
        let err = conn.send(b"doomed").unwrap_err();
        assert!(err.to_string().contains("injected"), "{err}");
        let mut buf = [0u8; 8];
        assert_eq!(
            server.recv(&mut buf).unwrap(),
            Some(0),
            "the other side sees close"
        );
        assert_eq!(sim.coverage()["fault:conn_drop"], 1);
    }

    #[test]
    fn net_spike_delays_but_delivers() {
        let mut cfg = FaultConfig::none();
        cfg.net_latency = (Duration::from_millis(1), Duration::from_millis(1));
        cfg.net_spike = 1.0;
        let sim = Sim::with_faults(0xD20_0002, cfg);
        let a = sim.host("a");
        let b = sim.host("b");
        let listener = b.net().listen("b:1").unwrap();
        let conn = a.net().connect("b:1").unwrap();
        let server = poll_accept(&sim, &*listener);
        conn.send(b"spiked").unwrap();
        let mut buf = [0u8; 8];
        sim.clock().sleep(Duration::from_millis(1));
        assert_eq!(
            server.recv(&mut buf).unwrap(),
            None,
            "not delivered at the un-spiked latency"
        );
        sim.clock().sleep(Duration::from_millis(63));
        assert_eq!(
            server.recv(&mut buf).unwrap(),
            Some(6),
            "delivered after the ×64 spike"
        );
        assert_eq!(&buf[..6], b"spiked");
        assert_eq!(sim.coverage()["fault:net_spike"], 1);
    }

    #[test]
    fn subscriber_stall_defaults_off_and_reads_back_through_the_accessor() {
        // Unlike every other extended field, `subscriber_stall` has no
        // internal consumption site in this crate (see the field's doc) —
        // `Sim::subscriber_stall` is the whole surface, so this pins that
        // read-back rather than an internal fault-firing effect.
        let sim = Sim::with_faults(0x2EB2_0000, FaultConfig::none());
        assert_eq!(sim.subscriber_stall(), 0.0);
        let sim = Sim::with_faults(
            0x2EB2_0001,
            FaultConfig {
                subscriber_stall: 0.42,
                ..FaultConfig::none()
            },
        );
        assert_eq!(sim.subscriber_stall(), 0.42);
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
    fn swarm_runner_floor_failure_names_the_shortfall() {
        let universes_seen = std::sync::atomic::AtomicUsize::new(0);
        let payload = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            run_universes_swarm_with_in(
                "charpente-sim",
                "swarm_meta.rs",
                0x5100_0001,
                4,
                &[],
                &[("rare:key", 3)],
                |_cfg: &mut FaultConfig| {},
                |sim: &mut Sim| {
                    // Covered in the FIRST universe only: 1 seen against a
                    // floor of 3 is the shortfall shape `require:` would wave
                    // through, since the key did occur.
                    if universes_seen.fetch_add(1, std::sync::atomic::Ordering::Relaxed) == 0 {
                        sim.cover("rare:key");
                    }
                    Ok(())
                },
            );
        }))
        .expect_err("a key below its floor must fail the batch");
        let msg = panic_message(&*payload);
        // The count is in the message because "it dropped" is useless without
        // "to what": 1-of-3 and 2-of-3 are different investigations.
        assert!(msg.contains("rare:key: 1 of 3 required"), "{msg}");
        assert!(msg.contains("MEASURED rate"), "{msg}");
    }

    #[test]
    fn a_floor_that_is_met_does_not_fail_the_batch() {
        run_universes_swarm_with_in(
            "charpente-sim",
            "swarm_meta.rs",
            0x5100_0002,
            4,
            &[],
            &[("common:key", 4)],
            |_cfg: &mut FaultConfig| {},
            |sim: &mut Sim| {
                sim.cover("common:key");
                Ok(())
            },
        );
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

    // ------------------------------------------------------------ hosts

    #[test]
    fn host_worlds_are_isolated_disks() {
        let sim = Sim::with_faults(0x805_0001, FaultConfig::none());
        let a = sim.host("a");
        let b = sim.host("b");
        let fa = a.disk().open("wal/x").unwrap();
        fa.write_at(0, b"aaa").unwrap();
        fa.sync().unwrap();
        assert!(
            b.disk().list("wal/").unwrap().is_empty(),
            "b must not see a's files"
        );
        assert!(
            sim.disk().list("wal/").unwrap().is_empty(),
            "the global world must not see host files"
        );
        assert_ne!(a.durable_fingerprint(), b.durable_fingerprint());
    }

    #[test]
    fn host_crash_is_scoped_to_that_host() {
        let sim = Sim::with_faults(0x805_0002, FaultConfig::none());
        let a = sim.host("a");
        let b = sim.host("b");
        let fa = a.disk().open("f").unwrap();
        fa.write_at(0, b"unsynced").unwrap();
        let fb = b.disk().open("f").unwrap();
        fb.write_at(0, b"unsynced").unwrap();
        a.crash_now();
        assert!(fa.len().is_err(), "a's handles die at a's crash");
        assert_eq!(fb.len().unwrap(), 8, "b is untouched");
        a.restart();
        let fa = a.disk().open("f").unwrap();
        assert_eq!(fa.len().unwrap(), 0, "none() drops a's unsynced writes");
        assert_eq!(fb.len().unwrap(), 8, "b's page cache survives a's restart");
        assert_eq!(sim.coverage()["crash:host"], 1);
    }

    #[test]
    fn host_entropy_streams_are_independent_of_interleaving() {
        // Draw from a twice then b once vs b once then a twice: a's draws and
        // b's draws must be identical either way (field-keyed sub-streams).
        let draws = |order_ab: bool| {
            let sim = Sim::with_faults(0x805_0003, FaultConfig::none());
            let (a, b) = (sim.host("a"), sim.host("b"));
            let mut out = Vec::new();
            if order_ab {
                out.push(("a", a.entropy().next_u64()));
                out.push(("a", a.entropy().next_u64()));
                out.push(("b", b.entropy().next_u64()));
            } else {
                out.push(("b", b.entropy().next_u64()));
                out.push(("a", a.entropy().next_u64()));
                out.push(("a", a.entropy().next_u64()));
            }
            out.sort();
            out
        };
        assert_eq!(draws(true), draws(false));
    }

    #[test]
    fn host_crash_closes_conns_and_listeners() {
        let sim = Sim::with_faults(0x805_0004, FaultConfig::none());
        let a = sim.host("a");
        let b = sim.host("b");
        let listener = b.net().listen("b:7000").unwrap();
        let conn_a = a.net().connect("b:7000").unwrap();
        let conn_b = poll_accept(&sim, &*listener);
        conn_a.send(b"hi").unwrap();
        sim.clock().sleep(Duration::from_secs(1));
        let mut buf = [0u8; 16];
        assert_eq!(conn_b.recv(&mut buf).unwrap(), Some(2));

        b.crash_now();
        b.restart();
        // The surviving side observes the close; the crashed side's old handles error.
        assert_eq!(conn_a.recv(&mut buf).unwrap(), Some(0), "peer closed");
        assert!(
            conn_b.send(b"x").is_err(),
            "a crashed host's old conn handle is dead"
        );
        // The listener slot is free again after restart.
        let _relisten = b
            .net()
            .listen("b:7000")
            .expect("crash freed the listener name");
    }

    /// Clock-stepping accept loop: retries `accept` until a connection lands
    /// in the backlog, advancing virtual time between polls so a nonzero
    /// `net_latency` config still resolves.
    fn poll_accept(sim: &Sim, listener: &dyn Listener) -> Box<dyn Conn> {
        for _ in 0..1000 {
            if let Some(conn) = listener.accept().unwrap() {
                return conn;
            }
            sim.clock().sleep(Duration::from_millis(1));
        }
        panic!("poll_accept: no connection arrived after 1000 polls");
    }

    #[test]
    fn creating_hosts_never_perturbs_the_global_world() {
        let run = |with_hosts: bool| {
            let sim = Sim::new(0x805_0005); // default faults — draws happen
            if with_hosts {
                let h = sim.host("bystander");
                let f = h.disk().open("noise").unwrap();
                let _ = f.write_at(0, b"noise");
                let _ = f.sync();
            }
            let disk = sim.disk();
            let f = disk.open("f").unwrap();
            let mut payload = [0u8; 256];
            sim.entropy().fill(&mut payload);
            let _ = f.write_at(0, &payload);
            let _ = f.sync();
            sim.restart();
            sim.durable_fingerprint()
        };
        assert_eq!(
            run(false),
            run(true),
            "host activity must not shift global draws"
        );
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
