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

pub use charpente_core::io::{OutboundCall, OutboundReply};

pub mod checks;
mod disk;
pub mod harness;
mod net;
pub mod object;
pub mod outbound;
pub mod rng;

use std::collections::BTreeMap;
use std::io;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use charpente_core::io::{
    CLOCK_TOLERANCE, Clock, Conn, Disk, Entropy, File, Instant, Listener, Net, WallTime,
};

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
    /// P(an outbound call is refused before it reaches the remote — the kernel
    /// sees an immediate `ConnectionRefused`). Rolled from its own stream.
    pub outbound_refused: f64,
    /// P(an outbound request vanishes: the remote never sees it, the kernel
    /// sees silence until its deadline).
    pub outbound_lost_request: f64,
    /// P(the remote applies a call but its reply never arrives — the ambiguous
    /// case idempotency keys exist for; `object_lost_ack`'s outbound twin).
    pub outbound_lost_reply: f64,
    /// P(the remote answers 429 or 503 without handling the call).
    pub outbound_throttle: f64,
    /// P(a call carries extra think time from [10·hi, 100·hi], hi =
    /// `outbound_latency.1`, or [1 s, 100 s] when hi is zero — the LLM tail
    /// that outlives a deadline (plan amendment A15).
    pub outbound_slow: f64,
    /// One-way latency range for an outbound request and for its reply.
    pub outbound_latency: (Duration, Duration),
    pub disk_latency: (Duration, Duration),
    pub net_latency: (Duration, Duration),
    /// The largest skew, in nanoseconds, of any clock's wall reading from true
    /// time (`Sim::true_wall`). Each clock (`Sim::clock`, and each host's
    /// `SimHost::clock`) draws its own skew once, uniform in ±|clock_skew|,
    /// from a stream of its own, so two hosts' clocks disagree; the sign is
    /// ignored.
    pub clock_skew: i64,
    /// Per wall read: the chance the host's clock steps back, and the largest
    /// step. A step's size is uniform in (0, max], and the clock stays where
    /// the step left it.
    pub clock_step_back: (f64, Duration),
    /// Per power cycle (`Sim::restart`, `SimHost::restart`): the chance the
    /// host's wall clock jumps forward, as if the machine had been down, and
    /// the largest jump. Virtual time does not move; only that host's wall
    /// reading does.
    pub clock_downtime: (f64, Duration),
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
            outbound_refused: 0.0,
            outbound_lost_request: 0.0,
            outbound_lost_reply: 0.0,
            outbound_throttle: 0.0,
            outbound_slow: 0.0,
            outbound_latency: (Duration::from_millis(1), Duration::from_millis(50)),
            disk_latency: (Duration::from_micros(20), Duration::from_millis(2)),
            net_latency: (Duration::from_micros(100), Duration::from_millis(20)),
            clock_skew: 0,
            clock_step_back: (0.0, Duration::ZERO),
            clock_downtime: (0.0, Duration::ZERO),
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
            outbound_refused: 0.0,
            outbound_lost_request: 0.0,
            outbound_lost_reply: 0.0,
            outbound_throttle: 0.0,
            outbound_slow: 0.0,
            outbound_latency: (Duration::ZERO, Duration::ZERO),
            disk_latency: (Duration::ZERO, Duration::ZERO),
            net_latency: (Duration::ZERO, Duration::ZERO),
            clock_skew: 0,
            clock_step_back: (0.0, Duration::ZERO),
            clock_downtime: (0.0, Duration::ZERO),
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
            // These axes (`dir_entry_loss` … `subscriber_stall`, and the six
            // `outbound_*`) draw from field-keyed sub-streams
            // (`sample_extended_fields`, applied in `run_app_universe`/
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
            outbound_refused: 0.0,
            outbound_lost_request: 0.0,
            outbound_lost_reply: 0.0,
            outbound_throttle: 0.0,
            outbound_slow: 0.0,
            outbound_latency: (Duration::ZERO, Duration::ZERO),
            // Constant, never drawn: the scope is a property of what the
            // corruption axes are FOR (the WAL fences), not a fault rate.
            corruption_scope: CORRUPTION_SCOPE_DEFAULT.to_string(),
            disk_latency: (Duration::ZERO, disk_hi),
            net_latency: (Duration::ZERO, net_hi),
            // Off, and drawn by `sample_clock_fields` on the app harness's
            // path only (`run_app_universe`), so no platform suite's
            // universes move.
            clock_skew: 0,
            clock_step_back: (0.0, Duration::ZERO),
            clock_downtime: (0.0, Duration::ZERO),
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
            ("outbound_refused", self.outbound_refused),
            ("outbound_lost_request", self.outbound_lost_request),
            ("outbound_lost_reply", self.outbound_lost_reply),
            ("outbound_throttle", self.outbound_throttle),
            ("outbound_slow", self.outbound_slow),
            ("clock_step_back", self.clock_step_back.0),
            ("clock_downtime", self.clock_downtime.0),
        ] {
            assert!(
                (0.0..=1.0).contains(&p),
                "FaultConfig.{name} = {p} — must be in [0, 1]"
            );
        }
        assert!(
            self.clock_skew != i64::MIN,
            "FaultConfig.clock_skew = i64::MIN — its magnitude must fit in i64 nanoseconds"
        );
        for (name, (p, max)) in [
            ("clock_step_back", self.clock_step_back),
            ("clock_downtime", self.clock_downtime),
        ] {
            assert!(
                p == 0.0 || !max.is_zero(),
                "FaultConfig.{name} = ({p}, 0ns) — a chance above zero needs a largest size above \
                 zero"
            );
            assert!(
                i64::try_from(max.as_nanos()).is_ok(),
                "FaultConfig.{name}'s largest size {max:?} exceeds i64 nanoseconds"
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
        assert!(
            self.outbound_latency.0 <= self.outbound_latency.1,
            "outbound_latency range inverted"
        );
    }
}

/// Extended fault fields draw from per-field sub-streams keyed by (universe
/// seed, field name) — never from `FaultConfig::swarm`'s legacy sequential
/// stream above (this file's frozen replay contract). Adding a field here
/// changes no existing universe's legacy draws and no other extended
/// field's draws: each stream is independent of the seed's position in any
/// other stream. Called from `run_app_universe`/`run_universe_with_adjust`
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
    let mut r = keyed_stream(seed, "outbound_refused");
    cfg.outbound_refused = if r.chance(0.25) {
        r.below(50) as f64 / 1000.0
    } else {
        0.0
    };
    let mut r = keyed_stream(seed, "outbound_lost_request");
    cfg.outbound_lost_request = if r.chance(0.25) {
        r.below(30) as f64 / 1000.0
    } else {
        0.0
    };
    let mut r = keyed_stream(seed, "outbound_lost_reply");
    cfg.outbound_lost_reply = if r.chance(0.25) {
        r.below(30) as f64 / 1000.0
    } else {
        0.0
    };
    let mut r = keyed_stream(seed, "outbound_throttle");
    cfg.outbound_throttle = if r.chance(0.25) {
        r.below(100) as f64 / 1000.0
    } else {
        0.0
    };
    let mut r = keyed_stream(seed, "outbound_slow");
    cfg.outbound_slow = if r.chance(0.25) {
        r.below(50) as f64 / 1000.0
    } else {
        0.0
    };
    const OUTBOUND_HI: [Duration; 4] = [
        Duration::ZERO,
        Duration::from_millis(10),
        Duration::from_millis(200),
        Duration::from_secs(2),
    ];
    let mut r = keyed_stream(seed, "outbound_latency");
    cfg.outbound_latency = (Duration::ZERO, OUTBOUND_HI[r.below(4) as usize]);
}

/// The clock axes, each on in a quarter of runs and drawn from its own keyed
/// stream, so no existing field moves (`clock_axes_move_no_existing_draw`).
/// Called by `run_app_universe` only: a platform suite that wants them turns
/// them on in its `run_universes_swarm_with!` `adjust`, so no pinned platform
/// seed moves either. The skew and step sizes span both sides of
/// `CLOCK_TOLERANCE` (60 s), so a run sees both the kernel's hold and its stop.
fn sample_clock_fields(seed: u64, cfg: &mut FaultConfig) {
    let mut r = keyed_stream(seed, "clock_skew");
    cfg.clock_skew = if r.chance(0.25) {
        r.between(0, 120_000_000_000) as i64
    } else {
        0
    };
    let mut r = keyed_stream(seed, "clock_step_back");
    cfg.clock_step_back = if r.chance(0.25) {
        (r.below(20) as f64 / 1000.0, Duration::from_secs(120))
    } else {
        (0.0, Duration::ZERO)
    };
    let mut r = keyed_stream(seed, "clock_downtime");
    cfg.clock_downtime = if r.chance(0.25) {
        (0.5, Duration::from_secs(3 * 86_400))
    } else {
        (0.0, Duration::ZERO)
    };
}

// A frozen copy of `sample_extended_fields` as of f21b491e, before the
// outbound axes: the "without" arm of `outbound_axes_perturb_no_existing_draw`.
// The next extended axis turns that test red by design, so add that axis to
// this copy when it lands.
#[cfg(test)]
fn sample_extended_fields_without_outbound_for_tests(seed: u64, cfg: &mut FaultConfig) {
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
    /// This host's directory-barrier stream — see `State::dir_faults`.
    dir_faults: Rng,
    /// How many durable-corruption faults (`bit_flip`, `misdirected_write`)
    /// fired on this host's disk — `SimHost::durable_corruption_fired`.
    /// Counted alongside the universe-wide coverage keys; it draws nothing.
    durable_corruption: u64,
    /// How many directory entries `dir_entry_loss` lost or resurrected on this
    /// host's disk — `SimHost::dir_entry_loss_fired`. Counted from the
    /// universe-wide `fault:dir-entry` key around this host's collapse; it
    /// draws nothing.
    entry_loss: u64,
    /// This host's wall clock (`SimHost::clock`).
    wall: WallClock,
}

impl HostWorld {
    fn new(seed: u64, host: &str, skew: i64) -> HostWorld {
        HostWorld {
            disk: DiskWorld::default(),
            faults: keyed_stream(seed, &format!("host-faults:{host}")),
            entropy: keyed_stream(seed, &format!("host-entropy:{host}")),
            dir_faults: keyed_stream(seed, &format!("host-dir-faults:{host}")),
            durable_corruption: 0,
            entry_loss: 0,
            wall: WallClock::new(seed, &format!("host-wall:{host}"), skew),
        }
    }
}

/// One clock's wall-time faults. Its reading is true time plus its offset:
/// its own `skew`, drawn once, plus `drift`, the sum of its step-backs
/// (negative) and downtime jumps (positive). Each axis draws from a keyed
/// stream of this clock's own, so a clock's faults never depend on another
/// clock's reads or on the frozen global streams, and with an axis off
/// nothing is drawn for it.
struct WallClock {
    skew: i64,
    drift: i64,
    /// Whether `clock:skewed` was marked: once per clock, at its first read.
    skew_marked: bool,
    /// Downtime jumps since the last read: `clock:downtime` is marked when a
    /// reading first carries them.
    jumps_unread: u64,
    step_back: Rng,
    downtime: Rng,
}

impl WallClock {
    /// `skew` is `FaultConfig::clock_skew`: this clock's own skew is uniform
    /// in ±|skew|, from the `"{name}:skew"` stream.
    fn new(seed: u64, name: &str, skew: i64) -> WallClock {
        let magnitude = skew.unsigned_abs();
        let skew = if magnitude == 0 {
            0
        } else {
            let mut r = keyed_stream(seed, &format!("{name}:skew"));
            // `validate` holds the magnitude within i64, so both ends fit.
            (i128::from(r.between(0, 2 * magnitude)) - i128::from(magnitude)) as i64
        };
        WallClock {
            skew,
            drift: 0,
            skew_marked: false,
            jumps_unread: 0,
            step_back: keyed_stream(seed, &format!("{name}:step-back")),
            downtime: keyed_stream(seed, &format!("{name}:downtime")),
        }
    }

    /// This clock's offset from true time: its skew plus its drift.
    fn offset(&self, host: Option<&str>) -> i64 {
        self.skew.checked_add(self.drift).unwrap_or_else(|| {
            panic!(
                "{}'s offset overflowed i64 nanoseconds: skew {} ns plus drift {} ns",
                clock_name(host),
                self.skew,
                self.drift
            )
        })
    }

    /// One wall read at virtual time `now` on `host`'s clock (`None`: the one
    /// `Sim::clock` reads): rolls a step back first, unless `faults_off`, so
    /// the step lands in this reading.
    fn read(
        &mut self,
        now: u64,
        cfg: &FaultConfig,
        faults_off: bool,
        coverage: &mut BTreeMap<&'static str, u64>,
        host: Option<&str>,
    ) -> WallTime {
        let (p, max) = cfg.clock_step_back;
        if !faults_off && p > 0.0 && self.step_back.chance(p) {
            // `validate` holds the largest size within i64 nanoseconds.
            let size = self.step_back.between(1, max.as_nanos() as u64) as i64;
            self.drift = self.drift.checked_sub(size).unwrap_or_else(|| {
                panic!(
                    "{}'s step-backs overflowed i64 nanoseconds of drift",
                    clock_name(host)
                )
            });
            *coverage.entry("clock:stepped-back").or_insert(0) += 1;
            let class = if size as u128 > CLOCK_TOLERANCE.as_nanos() {
                "clock:stepped-back:beyond-tolerance"
            } else {
                "clock:stepped-back:within-tolerance"
            };
            *coverage.entry(class).or_insert(0) += 1;
        }
        if self.skew != 0 && !self.skew_marked {
            self.skew_marked = true;
            *coverage.entry("clock:skewed").or_insert(0) += 1;
        }
        if self.jumps_unread > 0 {
            *coverage.entry("clock:downtime").or_insert(0) += self.jumps_unread;
            self.jumps_unread = 0;
        }
        let offset = self.offset(host);
        let nanos = now
            .checked_add(SIM_WALL_BASE_NANOS)
            .and_then(|t| t.checked_add_signed(offset))
            .unwrap_or_else(|| {
                panic!(
                    "{}'s wall reading left u64 nanoseconds: virtual time {now} ns + \
                     SIM_WALL_BASE_NANOS {SIM_WALL_BASE_NANOS} ns + offset {offset} ns (skew {} \
                     ns, step-backs and downtime {} ns)",
                    clock_name(host),
                    self.skew,
                    self.drift
                )
            });
        WallTime::from_nanos(nanos)
    }

    /// A power cycle of `host` (`None`: the global disk's): rolls the
    /// downtime jump, unless `faults_off`.
    fn power_cycle(&mut self, cfg: &FaultConfig, faults_off: bool, host: Option<&str>) {
        let (p, max) = cfg.clock_downtime;
        if !faults_off && p > 0.0 && self.downtime.chance(p) {
            let jump = self.downtime.between(1, max.as_nanos() as u64) as i64;
            self.drift = self.drift.checked_add(jump).unwrap_or_else(|| {
                panic!(
                    "{}'s downtime overflowed i64 nanoseconds of drift",
                    clock_name(host)
                )
            });
            self.jumps_unread += 1;
        }
    }
}

/// How a clock names itself in its panics.
fn clock_name(host: Option<&str>) -> String {
    match host {
        None => "the sim's clock (Sim::clock)".to_string(),
        Some(name) => format!("host \"{name}\"'s clock (SimHost::clock)"),
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
    /// The directory barrier's own fault stream, split off `faults` on purpose.
    /// `sync_dir` is the one disk op whose CALL SITES move as a consequence of
    /// durability fixes rather than of workload changes, and `Rng::chance` draws
    /// before it compares — so a barrier sharing `faults` displaces every later
    /// read/write/sync roll and re-rolls every pinned universe merely by being
    /// added or relocated. DESIGN.md made this same split for the object store's
    /// delete arm for the same reason; `a_directory_barrier_does_not_perturb_the_
    /// other_disk_faults` is the oracle that holds it.
    dir_faults: Rng,
    disk: DiskWorld,
    net: NetWorld,
    /// Sometimes-coverage: named-event counts across the universe (Antithesis
    /// "sometimes assertions", adapted) — see `Sim::cover`.
    coverage: BTreeMap<&'static str, u64>,
    /// Set by `Sim::give_up`: this universe could not adjudicate its property.
    /// It lives on the world, not in the workload's returned `Result`, because
    /// the returned `Result` is exactly what a `finish_universe`-style wrapper
    /// launders into `Ok(())` before the runner ever sees it.
    gave_up: Option<(&'static str, Option<String>)>,
    /// The workload's drawn inputs (`Sim::input`): own stream, own record.
    inputs: InputLog,
    /// Per-host worlds, created lazily by `Sim::host`. NEW state: no existing
    /// single-node universe ever populates this map, so the frozen global
    /// world above is untouched by this task (the replay contract).
    hosts: BTreeMap<String, HostWorld>,
    /// Set by `Sim::quiesce_disk_faults`: every later disk-fault roll is
    /// skipped without drawing. False for the whole of every universe that
    /// never calls it, so those replay exactly as before.
    disk_faults_off: bool,
    /// Set by `Sim::quiesce_subscriber_stalls`: `Sim::subscriber_stall` reads
    /// 0.0 from then on. False for the whole of every universe that never
    /// calls it.
    stalls_off: bool,
    /// The outside world behind `Sim::outbound` handles. Keyed streams only,
    /// and nothing is drawn until a handle starts a call, so a universe that
    /// never builds one replays exactly as before.
    outbound: outbound::OutboundWorld,
    /// The wall clock `Sim::clock` reads: keyed streams only, drawn from only
    /// while a clock axis is on.
    wall: WallClock,
    /// Set by `Sim::quiesce_clock_faults`: no clock steps back or jumps from
    /// then on, and each skipped roll draws nothing. False for the whole of
    /// every universe that never calls it.
    clock_faults_off: bool,
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

    fn dir_faults_of(&mut self, host: &Option<String>) -> &mut Rng {
        match host {
            None => &mut self.dir_faults,
            Some(name) => &mut self.host_mut(name).dir_faults,
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

/// What `Sim::input` has drawn: from `rng` on a normal run, from `replay`
/// (then 0s) when the harness replays a recorded sequence. `choices` holds
/// each draw's choice as the draw used it, reduced into its range.
struct InputLog {
    rng: Rng,
    replay: Option<(Vec<u64>, usize)>,
    choices: Vec<u64>,
    drawn: Vec<(&'static str, String)>,
    /// Whether every draw used the choice the seed's own stream gives it, so
    /// the seed alone replays these inputs. Always true without a replay.
    follows_seed: bool,
}

impl InputLog {
    /// The next choice in `0..span` (the whole `u64` when `span` is `None`,
    /// a `between` over every value): the raw value reduced by the span, the
    /// smallest raw value that draws the same value. That is what the run
    /// records, so a choice and the value it draws are one-to-one for a
    /// draw, a smaller choice is a smaller value, and two equal values drawn
    /// with the same bounds are equal choices.
    ///
    /// The stream is read once per draw under a replay too, and its value
    /// reduced by this draw's span: a replay with the seed alone would run
    /// the same up to this draw and draw exactly that, so comparing the two
    /// says whether it replays this run. Comparing against the first
    /// failure's choices would not: a fault turned off can change a draw's
    /// span, and the same choice then sits on another raw value.
    fn choose(&mut self, span: Option<u64>) -> u64 {
        let reduce = |raw: u64| span.map_or(raw, |span| raw % span);
        let own = reduce(self.rng.next_u64());
        let choice = match &mut self.replay {
            Some((c, at)) => {
                let v = c.get(*at).copied().unwrap_or(0);
                *at += 1;
                reduce(v)
            }
            None => own,
        };
        self.follows_seed &= choice == own;
        self.choices.push(choice);
        choice
    }

    /// `lo` plus a choice below the span: `lo..=hi` holds `hi - lo + 1`
    /// values, or every `u64` when that overflows, and then `lo` is 0.
    fn between(&mut self, lo: u64, hi: u64) -> u64 {
        lo + self.choose((hi - lo).checked_add(1))
    }
}

/// A workload's input draws; see `Sim::input`.
pub struct Input<'a> {
    sim: &'a Sim,
}

impl Input<'_> {
    fn log(&self) -> MutexGuard<'_, State> {
        self.sim.world.lock()
    }

    /// A number in `lo..=hi`; shrinks toward `lo`. Panics if `lo > hi`.
    pub fn between(&self, name: &'static str, lo: u64, hi: u64) -> u64 {
        assert!(
            lo <= hi,
            "sim.input().between(\"{name}\", {lo}, {hi}): lo is above hi, so no value fits; \
             pass the smaller bound first"
        );
        let mut g = self.log();
        let v = g.inputs.between(lo, hi);
        g.inputs.drawn.push((name, format!("{v:?}")));
        v
    }

    /// One of `items`; shrinks toward the first. Panics on an empty slice.
    pub fn pick<'t, T: std::fmt::Debug>(&self, name: &'static str, items: &'t [T]) -> &'t T {
        assert!(
            !items.is_empty(),
            "sim.input().pick(\"{name}\", ..): the slice is empty, so there is nothing to pick; \
             pass at least one item"
        );
        let mut g = self.log();
        let item = &items[g.inputs.choose(Some(items.len() as u64)) as usize];
        g.inputs.drawn.push((name, format!("{item:?}")));
        item
    }

    /// Reorders `items` in place; shrinks toward the order given.
    pub fn shuffle<T: std::fmt::Debug>(&self, name: &'static str, items: &mut [T]) {
        let mut g = self.log();
        for i in 0..items.len() {
            let j = i + g.inputs.choose(Some((items.len() - i) as u64)) as usize;
            items.swap(i, j);
        }
        g.inputs.drawn.push((name, format!("{items:?}")));
    }

    /// Between `min_words` and `max_words` words from `words`, space-joined;
    /// shrinks toward `min_words` copies of the first word. Panics if
    /// `min_words > max_words`, or on an empty `words` when any are wanted.
    pub fn text(
        &self,
        name: &'static str,
        words: &[&str],
        min_words: usize,
        max_words: usize,
    ) -> String {
        assert!(
            min_words <= max_words,
            "sim.input().text(\"{name}\", .., {min_words}, {max_words}): min_words is above max_words; \
             pass the smaller count first"
        );
        assert!(
            !words.is_empty() || max_words == 0,
            "sim.input().text(\"{name}\", ..): `words` is empty but up to {max_words} words are wanted; \
             pass at least one word or a max_words of 0"
        );
        let mut g = self.log();
        let n = g.inputs.between(min_words as u64, max_words as u64) as usize;
        let mut out = Vec::with_capacity(n);
        for _ in 0..n {
            out.push(words[g.inputs.choose(Some(words.len() as u64)) as usize]);
        }
        let text = out.join(" ");
        g.inputs.drawn.push((name, format!("{text:?}")));
        text
    }
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
        let skew = cfg.clock_skew;
        let mut sm = seed;
        let faults = Rng::new(splitmix64(&mut sm));
        let entropy = Rng::new(splitmix64(&mut sm));
        let latency = Rng::new(splitmix64(&mut sm));
        // Drawn last: appending keeps every stream above byte-identical, so
        // introducing this split perturbs nothing through the constructor.
        let dir_faults = Rng::new(splitmix64(&mut sm));
        Sim {
            world: Arc::new(World {
                seed,
                cfg,
                state: Mutex::new(State {
                    now: 0,
                    faults,
                    entropy,
                    latency,
                    dir_faults,
                    disk: DiskWorld::default(),
                    net: NetWorld::default(),
                    coverage: BTreeMap::new(),
                    gave_up: None,
                    inputs: InputLog {
                        rng: keyed_stream(seed, "inputs"),
                        replay: None,
                        choices: Vec::new(),
                        drawn: Vec::new(),
                        follows_seed: true,
                    },
                    hosts: BTreeMap::new(),
                    disk_faults_off: false,
                    stalls_off: false,
                    outbound: outbound::OutboundWorld::new(seed),
                    wall: WallClock::new(seed, "wall", skew),
                    clock_faults_off: false,
                }),
            }),
        }
    }

    pub fn seed(&self) -> u64 {
        self.world.seed
    }

    /// The workload's inputs: the values the property must hold for, drawn
    /// from their own stream (`keyed_stream(seed, "inputs")`), so drawing
    /// moves no fault draw. Every draw records the choice it used, the raw
    /// value reduced into its range, which the harness replays and shrinks.
    ///
    /// This is the workload's own script only. A modelled remote (an outbound
    /// stand-in) keeps its own `Rng`. Values shrink toward `lo`, the first
    /// item and the identity order. A drawn input tests how the app handles
    /// what its model stand-in returns, never the real model's behaviour.
    pub fn input(&self) -> Input<'_> {
        Input { sim: self }
    }

    /// Feeds `choices` to later `input()` draws in order, then 0s.
    pub(crate) fn replay_choices(&self, choices: &[u64]) {
        self.world.lock().inputs.replay = Some((choices.to_vec(), 0));
    }

    fn input_record(&self) -> (Vec<u64>, Vec<(&'static str, String)>) {
        let g = self.world.lock();
        (g.inputs.choices.clone(), g.inputs.drawn.clone())
    }

    /// This universe's sampled `subscriber_stall` rate — the one extended
    /// field with no internal consumption site (see the field's doc) — or 0.0
    /// once `quiesce_subscriber_stalls` has been called. A
    /// workload reads this to drive its own `SimSink` stall closure (`charpente`'s
    /// `SimSink` lives outside this crate and has no `Sim` reference of its
    /// own) at this universe's rate, and marks `fault:subscriber_stall`
    /// coverage itself when the roll actually fires.
    pub fn subscriber_stall(&self) -> f64 {
        if self.world.lock().stalls_off {
            return 0.0;
        }
        self.world.cfg.subscriber_stall
    }

    /// Ends subscriber-stall injection for the rest of this universe:
    /// `subscriber_stall` reads 0.0 from here on, so a stall closure keyed on
    /// it and built after this call never refuses a send and draws nothing.
    /// For a settle phase whose verdict rests on a delivery completing: a
    /// stall there is a fault the property excuses, so it would turn the
    /// verdict into no verdict. The disk-fault counterpart is
    /// `quiesce_disk_faults`; a universe that never calls this is unchanged.
    pub fn quiesce_subscriber_stalls(&self) {
        self.world.lock().stalls_off = true;
    }

    /// Marks a named event for sometimes-coverage accounting. Workload-side
    /// marks use an "<area>:" prefix by convention (e.g. `"wal:recovered"`).
    pub fn cover(&self, what: &'static str) {
        self.world.lock().cover(what);
    }

    /// The event counts marked so far in this universe (`cover` plus the
    /// sim-internal auto-coverage: `fault:*`, `crash:*`, `restart`,
    /// `run:skipped`).
    pub fn coverage(&self) -> BTreeMap<&'static str, u64> {
        self.world.lock().coverage.clone()
    }

    /// Declares that this universe could not adjudicate its property — the
    /// injected faults were severe enough that the workload never reached a
    /// verdict. That is neither a pass nor a failure, and until this existed
    /// the runner could not tell it from a pass: a give-up rode inside an
    /// `Err(String)` marker that the workload's own wrapper mapped back to
    /// `Ok(())` *before* the runner saw it. Recorded on the world instead, so
    /// no wrapper above the runner can launder it away.
    ///
    /// First call wins: the earliest give-up is the cause, later ones are its
    /// consequences. A workload that gives up and then returns `Err` is still
    /// a failure — failures outrank give-ups.
    ///
    /// **The call must be terminal for the universe — nothing enforces this,
    /// and it is the one way to make the report lie.** `gave_up` is sticky, so
    /// a workload that gives up inside a retry arm and then goes on to
    /// adjudicate its property is counted as a give-up despite having reached
    /// a verdict. The error runs in the conservative direction (an honest
    /// verdict is under-counted, never a give-up over-counted), and the
    /// negative space cannot be asserted here: `Sim` has no way to know what
    /// "adjudicating" means for a caller's property, so only the caller can
    /// hold the rule. The shape where this will bite first is a multi-round
    /// checker driven on the caller's own world — `checks::acked_implies_durable`
    /// is the one in this crate: give up in round 1's fault-fallout arm and
    /// rounds 2 and 3 still run, so their verdict is thrown away silently.
    /// In a loop, `give_up` and `return`/`break` out belong on the same arm.
    ///
    /// Also marks `run:skipped` coverage, so a give-up *rate* can be
    /// capped with the swarm runners' `ceiling:` (a `floor:` on it would bound
    /// give-ups from below, which guards nothing).
    ///
    /// `reason` is fixed text: the CLI groups give-ups by it, so it must not
    /// embed anything that varies per run. `detail` is that run's own text (an
    /// error, a count, a seq), which `--seed` shows; an empty detail is none.
    pub fn give_up(&self, reason: &'static str, detail: impl std::fmt::Display) {
        let mut guard = self.world.lock();
        if guard.gave_up.is_none() {
            // Inside the first-call guard on purpose: the key counts
            // UNIVERSES that gave up, not calls to `give_up`, so a floor on
            // it reads as a rate and cross-checks the runner's reported count
            // exactly.
            guard.cover("run:skipped");
            let detail = detail.to_string();
            guard.gave_up = Some((reason, (!detail.is_empty()).then_some(detail)));
        }
    }

    /// The fixed reason this universe gave up, if it did.
    pub fn gave_up(&self) -> Option<&'static str> {
        self.world
            .lock()
            .gave_up
            .as_ref()
            .map(|(reason, _)| *reason)
    }

    /// This universe's own text for its give-up, if it gave one.
    pub fn gave_up_detail(&self) -> Option<String> {
        self.world
            .lock()
            .gave_up
            .as_ref()
            .and_then(|(_, d)| d.clone())
    }

    pub fn now(&self) -> Instant {
        Instant::from_nanos(self.world.lock().now)
    }

    pub fn clock(&self) -> SimClock {
        SimClock {
            world: self.world.clone(),
            host: None,
        }
    }

    /// True wall time: virtual time plus `SIM_WALL_BASE_NANOS`, with no host's
    /// skew, step-backs or downtime in it. A property judges expiry and lateness
    /// against this, never against a reading some host's clock gave.
    pub fn true_wall(&self) -> WallTime {
        let now = self.world.lock().now;
        WallTime::from_nanos(now.checked_add(SIM_WALL_BASE_NANOS).unwrap_or_else(|| {
            panic!(
                "true wall time left u64 nanoseconds: virtual time {now} ns + SIM_WALL_BASE_NANOS"
            )
        }))
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
            .or_insert_with(|| HostWorld::new(seed, name, self.world.cfg.clock_skew));
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

    /// Ends disk fault injection for the rest of this universe, on every disk
    /// (the global one and every host's): pending crash countdowns are
    /// disarmed, and from here on no injected read, write, sync or
    /// directory-barrier error, misdirected write or restart bit flip fires.
    /// Each skipped roll draws nothing, and a universe that never calls this
    /// is unchanged. For an end-of-universe oracle whose own IO must not be
    /// perturbed: a universe in which no fault has fired so far is "quiet"
    /// only about its past, and the oracle's checkpoint, close and reads would
    /// otherwise still roll this universe's `FaultConfig`. A restart after
    /// this still power-cycles: unsynced writes and directory entries collapse
    /// under the crash model as usual. Network and object-store faults are
    /// untouched.
    pub fn quiesce_disk_faults(&self) {
        let mut guard = self.world.lock();
        guard.disk_faults_off = true;
        guard.disk.crash_countdown = None;
        for host in guard.hosts.values_mut() {
            host.disk.crash_countdown = None;
        }
    }

    /// Ends clock fault injection for the rest of this universe, on every
    /// clock: no reading steps back and no power cycle jumps a clock forward
    /// from here on, so each clock's offset stays where its faults left it,
    /// skew included. Each skipped roll draws nothing, and a universe that
    /// never calls this is unchanged. For a fault-free tail: a node whose
    /// clock an earlier step left behind its store's recorded time is refused
    /// at open, and with the offset frozen, the clock reaches the instant the
    /// refusal names as virtual time passes (`checks::clock_reopens_after`).
    pub fn quiesce_clock_faults(&self) {
        self.world.lock().clock_faults_off = true;
    }

    /// Whether the disk has crashed and stays gone until `restart()`: every op
    /// on it fails until then. A boot that failed on a crashed disk needs a
    /// power cycle before the next one can succeed; one that failed on an
    /// injected IO error does not (`checks::boot_through_fallout`). Reads
    /// state only, so it draws nothing and replays unchanged.
    pub fn disk_crashed(&self) -> bool {
        self.world.lock().disk.crashed
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
    /// tears, or vanishes (see `disk::collapse`), open file handles die, and any
    /// pending crash countdown is cleared. Called after a crash — or without
    /// one; a clean power cycle collapses the page cache the same way. A
    /// process restart that keeps the page cache is `restart_process`.
    pub fn restart(&self) {
        let mut guard = self.world.lock();
        guard.cover("restart");
        let State {
            disk,
            faults,
            coverage,
            disk_faults_off,
            wall,
            clock_faults_off,
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
        if !*disk_faults_off
            && self.world.cfg.bit_flip > 0.0
            && faults.chance(self.world.cfg.bit_flip)
        {
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
        wall.power_cycle(&self.world.cfg, *clock_faults_off, None);
    }

    /// Restarts the PROCESS, not the machine: every open file handle dies (it
    /// predates the restart, the same contract as after `restart`) and
    /// nothing else happens. The page cache survives — every unsynced write
    /// and directory entry stays pending, readable by the next open exactly
    /// as the dead process left it, and still at the mercy of the next power
    /// cycle's collapse. That is what a supervisor restarting a killed or
    /// exited process sees, and `restart` cannot model it, because there
    /// unsynced state collapses first. Readable is not durable: a node that
    /// persists state derived from what it read back here, before making it
    /// durable, loses the ground under that state at the next power cycle
    /// (found 2026-10-01: recovery replayed an unsynced WAL frame and the store
    /// outran the log, realtime seed 0x7df8c43a1d2591e8).
    ///
    /// Call it before dropping the old process's handles to model a kill
    /// (nothing flushed through its open files reaches the disk), after to
    /// model an exit. Only file handles are epoch-gated, as after `restart`:
    /// a destructor that opens a file afresh through a held `SimDisk` still
    /// reaches the disk. `unsynced_files` reads what the restart left
    /// unsynced. Draws
    /// nothing, collapses nothing and clears no countdown — a pending crash
    /// countdown stays armed, since power loss does not wait for a process —
    /// so a universe that never calls it is unchanged. Marks
    /// `restart:process`, never `restart`. The global disk only.
    pub fn restart_process(&self) {
        let mut guard = self.world.lock();
        assert!(
            !guard.disk.crashed,
            "Sim::restart_process on a crashed disk: a crash is power loss, and only \
             Sim::restart (the power cycle) brings the disk back — call that instead"
        );
        guard.cover("restart:process");
        guard.disk.epoch += 1;
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

    /// The files on the global disk holding writes no sync has taken yet,
    /// sorted by name: what the next power cycle may tear or drop. Read right
    /// after `restart_process`, it proves the restart left the next process
    /// something unsynced to read back; a restart that flushed would leave it
    /// empty. Read-only: draws nothing and marks nothing.
    pub fn unsynced_files(&self) -> Vec<String> {
        self.world
            .lock()
            .disk
            .files
            .iter()
            .filter(|(_, file)| !file.pending.is_empty())
            .map(|(name, _)| name.clone())
            .collect()
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

    /// Registers the model for `host` (spec §10). One model per host; a second
    /// registration is a contract panic.
    pub fn remote(&self, host: &str, model: impl outbound::Remote) {
        let mut guard = self.world.lock();
        let rng = keyed_stream(self.world.seed, &format!("outbound-model:{host}"));
        let name = std::any::type_name_of_val(&model);
        let clash = guard
            .outbound
            .models
            .insert(host.to_string(), (Box::new(model), rng, name));
        assert!(
            clash.is_none(),
            "contract: sim.remote(\"{host}\", …) registered twice"
        );
    }

    /// A new process's outbound handle. `hosts` is the app's declared host list
    /// (`App::outbound_hosts`) — plain data, since this crate cannot take an
    /// `App`. Refuses a host with no model, by name.
    pub fn outbound(&self, hosts: &[&'static str]) -> io::Result<outbound::SimOutbound> {
        let mut guard = self.world.lock();
        for host in hosts {
            if !guard.outbound.models.contains_key(*host) {
                return Err(io::Error::other(outbound::no_model(host)));
            }
        }
        let handle = guard.outbound.next_handle;
        guard.outbound.next_handle += 1;
        Ok(outbound::SimOutbound {
            world: self.world.clone(),
            handle,
        })
    }

    /// Reads `host`'s model after every delivery due by now has run (amendment
    /// A1: a reference cannot escape the world lock).
    pub fn with_remote<M: outbound::Remote, T>(&self, host: &str, f: impl FnOnce(&M) -> T) -> T {
        let mut guard = self.world.lock();
        let now = guard.now;
        guard.outbound.deliver_due(now);
        let (model, _, registered) = guard.outbound.models.get(host).unwrap_or_else(|| {
            panic!(
                "contract: sim.with_remote(\"{host}\", …) found no model for {host} — \
                 register one with sim.remote(\"{host}\", …) first"
            )
        });
        let any: &dyn std::any::Any = model.as_ref();
        f(any.downcast_ref::<M>().unwrap_or_else(|| {
            panic!(
                "contract: sim.with_remote(\"{host}\", …) asked for {}, but {host}'s model is \
                 {registered} — name the registered type; a wrapped model is read as its \
                 wrapper, then .inner()",
                std::any::type_name::<M>()
            )
        }))
    }

    /// Ends outbound fault injection for the rest of this universe: the
    /// fault-free tail's outbound half, beside `quiesce_disk_faults`. No call
    /// started after this is refused, lost, throttled, slowed or loses its
    /// reply, and each skipped roll draws nothing, so a universe that never
    /// calls this is unchanged. Latency stays, since it is not a fault, and a
    /// request already in flight keeps the fate it drew. For an outbound
    /// settle check (`checks::outbound_settles_by_horizon`): from here every
    /// call reaches its remote and its reply comes back, so every outbound job
    /// settles or retries to Dead.
    pub fn quiesce_outbound_faults(&self) {
        self.world.lock().outbound.quiet = true;
    }

    /// Every request that reached a remote so far, after running deliveries due by now.
    pub fn outbound_deliveries(&self) -> Vec<outbound::Delivered> {
        let mut guard = self.world.lock();
        let now = guard.now;
        guard.outbound.deliver_due(now);
        guard.outbound.deliveries.clone()
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

    /// This host's clock: virtual time for `now`, and a wall reading carrying
    /// this host's offset from true time (its own skew, within
    /// ±`FaultConfig::clock_skew`, plus its own step-backs and downtime).
    pub fn clock(&self) -> SimClock {
        SimClock {
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

    /// True when a durable-corruption fault (`bit_flip` or `misdirected_write`)
    /// actually fired on *this host's* disk: `checks::durable_corruption_fired`
    /// scoped to one host. A multi-node workload needs this form. A
    /// below-horizon refusal on one node is fallout only when corruption fired
    /// on that node, and the universe-wide answer would excuse it on the
    /// strength of a fault on another node.
    pub fn durable_corruption_fired(&self) -> bool {
        self.world.lock().host_mut(&self.name).durable_corruption > 0
    }

    /// True when the `dir_entry_loss` axis actually fired on *this host's*
    /// disk at one of its restarts (a pending create vanished or a pending
    /// removal came back): `checks::dir_entry_loss_fired` scoped to one host,
    /// for the same reason as `durable_corruption_fired` above. A gapped log
    /// directory on one node is fallout only when entry loss fired on that
    /// node.
    pub fn dir_entry_loss_fired(&self) -> bool {
        self.world.lock().host_mut(&self.name).entry_loss > 0
    }

    /// How many durable-corruption faults have fired on this host's disk so
    /// far: the count `durable_corruption_fired` compares with zero. It only
    /// grows. A workload that wipes this host's data (a re-seed deletes every
    /// file) reads it at the wipe and explains a later refusal only by a fault
    /// since then, since the wipe deleted every byte an earlier fault touched.
    pub fn durable_corruption_count(&self) -> u64 {
        self.world.lock().host_mut(&self.name).durable_corruption
    }

    /// How many directory entries `dir_entry_loss` has lost on this host's
    /// disk so far: the count `dir_entry_loss_fired` compares with zero. It
    /// only grows, and is read at a wipe for the reason
    /// `durable_corruption_count` gives.
    pub fn dir_entry_loss_count(&self) -> u64 {
        self.world.lock().host_mut(&self.name).entry_loss
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
                hosts,
                coverage,
                disk_faults_off,
                clock_faults_off,
                ..
            } = &mut *guard;
            let host = hosts.get_mut(&self.name).expect("checked above");
            let HostWorld {
                disk,
                faults,
                durable_corruption,
                entry_loss,
                wall,
                ..
            } = host;
            // Bit-level corruption of already-durable bytes, before per-file
            // crash collapse — see `Sim::restart`'s doc for the full model;
            // this is that same roll, scoped to this host's files and faults
            // stream.
            if !*disk_faults_off && cfg.bit_flip > 0.0 && faults.chance(cfg.bit_flip) {
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
                    *durable_corruption += 1;
                }
            }
            for file in disk.files.values_mut() {
                crate::disk::collapse(file, faults, &cfg, coverage);
            }
            if cfg.dir_entry_loss > 0.0 {
                let lost = |coverage: &BTreeMap<&'static str, u64>| {
                    coverage.get("fault:dir-entry").copied().unwrap_or(0)
                };
                let before = lost(coverage);
                crate::disk::collapse_dir_entries(disk, faults, cfg.dir_entry_loss, coverage);
                *entry_loss += lost(coverage) - before;
            }
            disk.epoch += 1;
            disk.crashed = false;
            disk.crash_countdown = None;
            wall.power_cycle(&cfg, *clock_faults_off, Some(&self.name));
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

/// 2026-01-01T00:00:00Z: virtual time zero reads as this wall time.
pub const SIM_WALL_BASE_NANOS: u64 = 1_767_225_600_000_000_000;

pub struct SimClock {
    world: Arc<World>,
    host: Option<String>,
}

impl SimClock {
    /// This clock's offset from true time now, in nanoseconds: its skew plus
    /// the step-backs and downtime it has taken so far, so `wall()` reads
    /// `sim.true_wall()` plus this, unless that read steps back. Draws
    /// nothing and marks nothing: a property reads it to judge a reading.
    pub fn offset(&self) -> i64 {
        let mut guard = self.world.lock();
        match &self.host {
            None => guard.wall.offset(None),
            Some(name) => guard.host_mut(name).wall.offset(Some(name)),
        }
    }
}

impl Clock for SimClock {
    fn now(&self) -> Instant {
        Instant::from_nanos(self.world.lock().now)
    }

    fn wall(&self) -> WallTime {
        let cfg = &self.world.cfg;
        let mut guard = self.world.lock();
        let State {
            now,
            wall,
            hosts,
            coverage,
            clock_faults_off,
            ..
        } = &mut *guard;
        match &self.host {
            None => wall.read(*now, cfg, *clock_faults_off, coverage, None),
            Some(name) => hosts
                .get_mut(name)
                .expect("a SimClock for a host is made by SimHost::clock, after Sim::host")
                .wall
                .read(*now, cfg, *clock_faults_off, coverage, Some(name)),
        }
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
        if !guard.disk_faults_off
            && guard
                .dir_faults_of(&self.host)
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
                "file handle for \"{}\" predates a simulated restart — contract: reopen files after Sim::restart() or Sim::restart_process()",
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
            if !state.disk_faults_off
                && cfg.read_error > 0.0
                && state.faults_of(&self.host).chance(cfg.read_error)
            {
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
            if !state.disk_faults_off && state.faults_of(&self.host).chance(cfg.write_error) {
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
            if !state.disk_faults_off
                && cfg.misdirected_write > 0.0
                && path.starts_with(&cfg.corruption_scope)
                && state.faults_of(&self.host).chance(cfg.misdirected_write)
            {
                state.cover("fault:misdirected_write");
                if let Some(host) = &self.host {
                    state.host_mut(host).durable_corruption += 1;
                }
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
            // A file with nothing unsynced has nothing to flush and nothing to
            // fail, so its sync rolls no fault (DESIGN §1). Recovery and boot
            // reads sync files a power cycle already left clean; a roll there
            // would inject failures no disk produces and re-key every universe
            // that recovers. It is still a disk op: the countdown and the
            // latency were charged above.
            let dirty = !file_state(state, &self.host, path).pending.is_empty();
            if dirty
                && !state.disk_faults_off
                && state.faults_of(&self.host).chance(cfg.sync_error)
            {
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
        let seed = parse_seed("CHARPENTE_SEED", &seed_var);
        eprintln!("charpente-sim [{test_file}]: replaying seed {seed:#018x}");
        let outcome = run_one(package, test_file, seed, &f);
        eprintln!(
            "charpente-sim [{test_file}]: this universe {}",
            describe_outcome(&outcome)
        );
        return;
    }
    eprintln!("charpente-sim [{test_file}]: {universes} universes from base seed {base:#018x}");
    let mut state = base;
    let mut gave_up = 0u64;
    for _ in 0..universes {
        if run_one(package, test_file, splitmix64(&mut state), &f).gave_up() {
            gave_up += 1;
        }
    }
    report_outcomes(test_file, universes, gave_up);
}

/// The line that makes MANIFESTO §9's sentence sayable: how many of a batch's
/// universes actually reached a verdict. Printed by both runners, always —
/// "0 gave up" is the claim being made, not noise.
fn report_outcomes(test_file: &str, universes: u64, gave_up: u64) {
    let verdicts = universes - gave_up;
    eprintln!(
        "charpente-sim [{test_file}]: {verdicts} of {universes} universes reached a verdict, \
         {gave_up} gave up"
    );
    if gave_up == universes && universes > 0 {
        eprintln!(
            "charpente-sim [{test_file}]: every universe gave up — this property adjudicated \
             nothing over {universes} universes"
        );
    }
}

fn describe_outcome(outcome: &Outcome) -> String {
    match outcome {
        Outcome::Verdict => "reached a verdict".to_string(),
        Outcome::GaveUp {
            reason,
            detail: None,
        } => format!("gave up: {reason}"),
        Outcome::GaveUp {
            reason,
            detail: Some(detail),
        } => format!("gave up: {reason}: {detail}"),
    }
}

/// `"crates/charpente/tests/typed_tables.rs"` → `"typed_tables"`.
fn test_name(file: &str) -> &str {
    let stem = file.rsplit('/').next().unwrap_or(file);
    stem.strip_suffix(".rs").unwrap_or(stem)
}

fn run_one(
    package: &str,
    test_file: &str,
    seed: u64,
    f: &impl Fn(&mut Sim) -> Result<(), String>,
) -> Outcome {
    let mut sim = Sim::new(seed);
    if let Err(msg) = f(&mut sim) {
        panic!("{}", failure_report(package, test_file, seed, &msg));
    }
    match sim.gave_up() {
        Some(reason) => Outcome::GaveUp {
            reason,
            detail: sim.gave_up_detail(),
        },
        None => Outcome::Verdict,
    }
}

fn failure_report(package: &str, test_file: &str, seed: u64, msg: &str) -> String {
    format!(
        "\nsim failure in {test_file}: {msg}\n  seed:  {seed:#018x}\n  repro: CHARPENTE_SEED={seed:#x} cargo test -p {package} --test {test_file} -- --nocapture\n"
    )
}

/// `var` is the variable `raw` was read from, so a bad value is named where
/// it was set. Separators are accepted because pinned bases are written with
/// them (`0x2EB2_5A10`): a base copied from the source parses as written.
fn parse_seed(var: &str, raw: &str) -> u64 {
    let hex = raw
        .strip_prefix("0x")
        .or_else(|| raw.strip_prefix("0X"))
        .unwrap_or(raw)
        .replace('_', "");
    u64::from_str_radix(&hex, 16)
        .unwrap_or_else(|_| panic!("{var} must be a hex seed like 0xdeadbeef — got \"{raw}\""))
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
/// f, require: ["key", ...])` — then `floor: [("key", N), ...]` to fail the
/// batch when a key was covered fewer than N times, `ceiling: [("key", N),
/// ...]` to fail it when a key that must stay rare was covered more than N
/// times, or both, in that order (see `run_universes_swarm_with!`). This base
/// is ambient, so the counts move batch to batch and either bound is a margin
/// around a measured spread, never an exact count. `run_universes!`/`run_one` are a
/// separate, frozen runner — this one is new. Set `CHARPENTE_SEED=0x…` to replay one
/// universe (its config re-derives from the seed; the coverage requirement
/// check is skipped, since one universe can't be expected to hit everything).
#[macro_export]
macro_rules! run_universes_swarm {
    ($universes:expr, $f:expr) => {
        $crate::run_universes_swarm_in(env!("CARGO_PKG_NAME"), file!(), $universes, &[], &[], &[], $f)
    };
    ($universes:expr, $f:expr, require: [$($key:expr),* $(,)?]) => {
        $crate::run_universes_swarm_in(env!("CARGO_PKG_NAME"), file!(), $universes, &[$($key),*], &[], &[], $f)
    };
    ($universes:expr, $f:expr,
     require: [$($key:expr),* $(,)?],
     ceiling: [$(($ckey:expr, $max:expr)),* $(,)?]) => {
        $crate::run_universes_swarm_in(
            env!("CARGO_PKG_NAME"),
            file!(),
            $universes,
            &[$($key),*],
            &[],
            &[$(($ckey, $max)),*],
            $f,
        )
    };
    ($universes:expr, $f:expr,
     require: [$($key:expr),* $(,)?],
     floor: [$(($fkey:expr, $min:expr)),* $(,)?]) => {
        $crate::run_universes_swarm_in(
            env!("CARGO_PKG_NAME"),
            file!(),
            $universes,
            &[$($key),*],
            &[$(($fkey, $min)),*],
            &[],
            $f,
        )
    };
    ($universes:expr, $f:expr,
     require: [$($key:expr),* $(,)?],
     floor: [$(($fkey:expr, $min:expr)),* $(,)?],
     ceiling: [$(($ckey:expr, $max:expr)),* $(,)?]) => {
        $crate::run_universes_swarm_in(
            env!("CARGO_PKG_NAME"),
            file!(),
            $universes,
            &[$($key),*],
            &[$(($fkey, $min)),*],
            &[$(($ckey, $max)),*],
            $f,
        )
    };
}

#[doc(hidden)]
pub fn run_universes_swarm_in(
    package: &str,
    file: &str,
    universes: u64,
    require: &[&str],
    floors: &[(&str, u64)],
    ceilings: &[(&str, u64)],
    f: impl Fn(&mut Sim) -> Result<(), String>,
) {
    // Ambient base, no config override — delegates to the `_with` runner
    // added alongside `run_universes_swarm_with!`; a no-op adjust makes this
    // byte-for-byte the same seed→behavior mapping as before that macro
    // existed (`run_universe`, below, is untouched). The CLI harness shared
    // it until the clock axes; it now runs `run_app_universe`.
    run_universes_swarm_with_in(
        package,
        file,
        ambient_base_seed(),
        universes,
        require,
        floors,
        ceilings,
        |_seed: u64, _cfg: &mut FaultConfig| {},
        f,
    );
}

/// `run_universes_swarm!` with a per-universe `FaultConfig` override: after
/// the swarm samples a universe's config, `adjust(seed, &mut cfg)` runs on it
/// before the universe executes — for tests that want the swarm's
/// per-universe variety with one axis forced hot (e.g. `dir_entry_loss`, not
/// yet swarm-sampled). `seed` is the universe's: an adjust that draws its own
/// values keys them by it, on a stream of its own (`sample_extended_fields`'
/// way), so a change to `FaultConfig` moves none of them, and one that ignores
/// it draws nothing.
/// Unlike `run_universes_swarm!`, the base seed is explicit — this is for
/// permanent, always-in-the-suite properties that must not flake batch to
/// batch. A NEW macro: `run_universe`'s frozen replay contract is untouched
/// (`run_universe_with_adjust`, below, is a separate function). Call as
/// `run_universes_swarm_with!(base, N, adjust, f)`, with `require: [...]`, or
/// with `require: [...], floor: [("key", N), ...]` — a floor is `require:`'s
/// at-least-once generalized to an at-least-N measured rate, for a property
/// whose oracle can decay toward vacuous without ever reaching zero. A
/// `ceiling: [("key", N), ...]` after either is the same check turned around:
/// the batch fails when the key was covered more than N times. Its use is
/// `("run:skipped", N)`, since a give-up is reported and never failed, so
/// without a cap a property can drift toward adjudicating nothing with every
/// batch green. Like floors and requires, ceilings are skipped under
/// `CHARPENTE_SEED` and `CHARPENTE_BASE_SEED`.
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
            &[],
            $adjust,
            $f,
        )
    };
    ($base:expr, $universes:expr, $adjust:expr, $f:expr,
     require: [$($key:expr),* $(,)?],
     ceiling: [$(($ckey:expr, $max:expr)),* $(,)?]) => {
        $crate::run_universes_swarm_with_in(
            env!("CARGO_PKG_NAME"),
            file!(),
            $base,
            $universes,
            &[$($key),*],
            &[],
            &[$(($ckey, $max)),*],
            $adjust,
            $f,
        )
    };
    ($base:expr, $universes:expr, $adjust:expr, $f:expr,
     require: [$($key:expr),* $(,)?],
     floor: [$(($fkey:expr, $min:expr)),* $(,)?],
     ceiling: [$(($ckey:expr, $max:expr)),* $(,)?]) => {
        $crate::run_universes_swarm_with_in(
            env!("CARGO_PKG_NAME"),
            file!(),
            $base,
            $universes,
            &[$($key),*],
            &[$(($fkey, $min)),*],
            &[$(($ckey, $max)),*],
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
    ceilings: &[(&str, u64)],
    adjust: impl Fn(u64, &mut FaultConfig),
    f: impl Fn(&mut Sim) -> Result<(), String>,
) {
    let test_file = test_name(file);
    if let Ok(seed_var) = std::env::var("CHARPENTE_SEED") {
        let seed = parse_seed("CHARPENTE_SEED", &seed_var);
        eprintln!(
            "charpente-sim [{test_file}]: replaying seed {seed:#018x} — coverage requirements skipped under CHARPENTE_SEED"
        );
        let end = run_one_swarm_with(package, test_file, seed, &adjust, &f);
        eprintln!(
            "charpente-sim [{test_file}]: this universe {}",
            describe_outcome(&end.outcome)
        );
        // The batch path reports coverage; a single-seed replay was throwing it
        // away, which is what you most want when asking why *this* universe
        // behaved (or cost) the way it did.
        eprintln!("charpente-sim [{test_file}]: coverage for this universe:");
        for (key, count) in &end.coverage {
            eprintln!("  {key}: {count}");
        }
        return;
    }
    // A pinned base is the right instrument for a FLOOR — a rate needs a fixed
    // denominator — and the wrong one for a CORRECTNESS oracle, which then only
    // ever examines the same N universes while reading as though it examined the
    // space. `CHARPENTE_BASE_SEED` re-bases any pinned property so that claim can
    // be TESTED, which is how two realtime properties were found passing at their
    // pinned base and failing at 5 of 11 others (docs/LEDGER.md 2026-09-07).
    //
    // Floors and requires are skipped under the override, for the reason
    // archive.rs's floor doc already gives: they are rates measured AT a base, so
    // evaluating them at a different one compares against a denominator that was
    // never taken there. The property's own assertions are what this mode tests,
    // and they are untouched. Same precedent as CHARPENTE_SEED above.
    let rebased = std::env::var("CHARPENTE_BASE_SEED")
        .ok()
        .map(|v| parse_seed("CHARPENTE_BASE_SEED", &v));
    let (base, require, floors, ceilings) = match rebased {
        Some(b) => {
            eprintln!(
                "charpente-sim [{test_file}]: re-basing {base:#018x} -> {b:#018x} under \
                 CHARPENTE_BASE_SEED — coverage requirements, floors and ceilings skipped \
                 (they are rates measured at the pinned base)"
            );
            (b, &[][..], &[][..], &[][..])
        }
        None => (base, require, floors, ceilings),
    };
    eprintln!(
        "charpente-sim [{test_file}]: {universes} swarm universes from base seed {base:#018x}"
    );
    let mut state = base;
    let mut coverage: BTreeMap<&'static str, u64> = BTreeMap::new();
    let mut gave_up = 0u64;
    for _ in 0..universes {
        let seed = splitmix64(&mut state);
        let end = run_one_swarm_with(package, test_file, seed, &adjust, &f);
        if end.outcome.gave_up() {
            gave_up += 1;
        }
        for (key, count) in end.coverage {
            *coverage.entry(key).or_insert(0) += count;
        }
    }
    eprintln!("charpente-sim [{test_file}]: coverage over {universes} universes:");
    for (key, count) in &coverage {
        eprintln!("  {key}: {count}");
    }
    report_outcomes(test_file, universes, gave_up);
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
    // `floor:` turned around, for a key that must stay rare. On
    // `run:skipped` it is the one guard on a verdict rate: a give-up is
    // reported, never failed, so without a cap a property can drift toward
    // adjudicating nothing with every batch still green.
    let above: Vec<String> = ceilings
        .iter()
        .filter_map(|(key, max)| {
            let seen = coverage.get(*key).copied().unwrap_or(0);
            (seen > *max).then(|| format!("{key}: {seen} over a cap of {max}"))
        })
        .collect();
    assert!(
        above.is_empty(),
        "sim coverage ceiling failed in {test_file}: {above:?} over {universes} universes. \
         A ceiling caps a key that must stay rare: it rose because the property reaches that \
         state more often than when the cap was set, and why is the finding. On \
         run:skipped it means fewer universes reached a verdict. Investigate before \
         touching it — raising a cap to match is how a property stops adjudicating with the \
         suite still green."
    );
}

/// How a universe ended. Until this type existed `run_universe` returned
/// `Ok(coverage)` for both "reached a verdict" and "gave up cleanly", so a
/// property could report green having concluded nothing — MANIFESTO §9 rests
/// on being able to say how many universes actually adjudicated.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// The workload adjudicated its property.
    Verdict,
    /// The workload could not adjudicate: see `Sim::give_up`.
    GaveUp {
        reason: &'static str,
        detail: Option<String>,
    },
}

impl Outcome {
    pub fn gave_up(&self) -> bool {
        matches!(self, Outcome::GaveUp { .. })
    }
}

/// One universe's end: how it ended, and the coverage it marked. The Ok
/// payload of every single-universe runner — a give-up and a verdict are
/// distinct values here, where they used to be the same `Ok(coverage_map)`.
#[derive(Debug, Clone)]
pub struct UniverseEnd {
    pub outcome: Outcome,
    pub coverage: BTreeMap<&'static str, u64>,
    /// The input choices the run used, each reduced into its draw's range,
    /// and each input by name.
    pub choices: Vec<u64>,
    pub drawn: Vec<(&'static str, String)>,
}

/// Reads the outcome off the world the workload just ran in. Called only on
/// the success path: a workload that gave up AND then failed is a failure.
fn end_of(sim: &Sim) -> UniverseEnd {
    let (choices, drawn) = sim.input_record();
    UniverseEnd {
        choices,
        drawn,
        outcome: match sim.gave_up() {
            Some(reason) => Outcome::GaveUp {
                reason,
                detail: sim.gave_up_detail(),
            },
            None => Outcome::Verdict,
        },
        coverage: sim.coverage(),
    }
}

/// One universe, as the CLI harness ran it before the clock axes (0.3.0's
/// named step S3). The seed→behavior mapping here is the frozen replay
/// contract: config derives from `splitmix64(seed)`, the workload runs
/// under catch_unwind, Ok returns the universe's end. Do not reorder
/// anything. The swarm runners keep this mapping (`run_universe_with_adjust`
/// with a no-op adjust) and the app harness now runs `run_app_universe`, so
/// this stays only as the reference `app_runs_with_the_clock_axes_off_decide_
/// as_before` holds `run_app_universe` to.
/// `sample_extended_fields` runs right after `FaultConfig::swarm` — its own
/// sub-streams (keyed off `seed`, not `cfg_seed`) never touch the legacy
/// draw above, so this addition changes no existing universe's replay
/// (segment-mgmt T5's pin test proves it).
// `FaultConfig` crossed clippy's large-Err threshold the moment
// `dir_entry_loss` landed; boxing the Err would touch this frozen function's
// signature (and every caller) for a lint, not a behavior change.
#[allow(clippy::result_large_err)]
#[cfg(test)]
pub(crate) fn run_universe(
    seed: u64,
    f: &dyn Fn(&mut Sim) -> Result<(), String>,
) -> Result<UniverseEnd, (FaultConfig, BTreeMap<&'static str, u64>, String)> {
    let mut s = seed;
    let cfg_seed = splitmix64(&mut s);
    let mut cfg = FaultConfig::swarm(&mut Rng::new(cfg_seed));
    sample_extended_fields(seed, &mut cfg);
    let mut sim = Sim::with_faults(seed, cfg.clone());
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| f(&mut sim))) {
        Ok(Ok(())) => Ok(end_of(&sim)),
        Ok(Err(msg)) => Err((cfg, sim.coverage(), msg)),
        Err(payload) => Err((cfg, sim.coverage(), panic_message(&*payload))),
    }
}

/// One app-harness run's failure (`run_app_universe`): what the harness
/// files it with (`config`, the `coverage` that fired, the workload's
/// `message`) and what a replay starts from (`choices`, the run's input
/// sequence, and `drawn`, each input by name). Crate-private: only the harness obtains one.
#[derive(Debug)]
pub(crate) struct UniverseFailure {
    pub config: FaultConfig,
    pub coverage: BTreeMap<&'static str, u64>,
    pub message: String,
    pub choices: Vec<u64>,
    pub drawn: Vec<(&'static str, String)>,
    /// Whether the seed alone draws `choices`: every draw used the choice the
    /// seed's own stream gives it (`InputLog::choose`).
    pub follows_seed: bool,
}

/// The config one run of `charpente test --sim` runs under: `run_universe`'s
/// mapping plus the clock axes (`sample_clock_fields`), then every
/// `FaultConfig` field named in `faults_off` turned off. The clock axes sample
/// last, from their own streams, so with them off a run decides exactly as
/// before them. `faults_off` applies after every draw, so turning an axis off
/// moves no other field (`turning_an_axis_off_moves_no_other_field`).
fn app_config(seed: u64, faults_off: &[&'static str]) -> FaultConfig {
    let mut s = seed;
    let cfg_seed = splitmix64(&mut s);
    let mut cfg = FaultConfig::swarm(&mut Rng::new(cfg_seed));
    sample_extended_fields(seed, &mut cfg);
    sample_clock_fields(seed, &mut cfg);
    for field in faults_off {
        turn_off(&mut cfg, field);
    }
    cfg.validate();
    cfg
}

/// One run of `charpente test --sim` (harness.rs), under `app_config`.
/// `choices` is the input sequence a replay feeds the workload (then 0s).
// The same large-Err trade as `run_universe`.
#[allow(clippy::result_large_err)]
pub(crate) fn run_app_universe(
    seed: u64,
    choices: Option<&[u64]>,
    faults_off: &[&'static str],
    f: &dyn Fn(&mut Sim) -> Result<(), String>,
) -> Result<UniverseEnd, UniverseFailure> {
    let cfg = app_config(seed, faults_off);
    let mut sim = Sim::with_faults(seed, cfg.clone());
    if let Some(c) = choices {
        sim.replay_choices(c);
    }
    let failure = |sim: &Sim, message: String| {
        let (choices, drawn) = sim.input_record();
        UniverseFailure {
            config: cfg.clone(),
            coverage: sim.coverage(),
            message,
            choices,
            drawn,
            follows_seed: sim.world.lock().inputs.follows_seed,
        }
    };
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| f(&mut sim))) {
        Ok(Ok(())) => Ok(end_of(&sim)),
        Ok(Err(msg)) => Err(failure(&sim, msg)),
        Err(payload) => Err(failure(&sim, panic_message(&*payload))),
    }
}

/// Every `FaultConfig` field `turn_off` takes, in field order: all of them but
/// `corruption_scope`, which scopes two axes rather than being one. The
/// harness's `--faults-off` names these, and its shrinker turns them off in
/// this order (`fault_axes_are_every_field_but_the_scope`).
pub(crate) const FAULT_AXES: [&str; 26] = [
    "write_error",
    "sync_error",
    "crash_apply",
    "crash_torn",
    "dir_entry_loss",
    "bit_flip",
    "misdirected_write",
    "read_error",
    "object_put_error",
    "object_get_error",
    "object_lost_ack",
    "object_delete_error",
    "conn_drop",
    "net_spike",
    "subscriber_stall",
    "outbound_refused",
    "outbound_lost_request",
    "outbound_lost_reply",
    "outbound_throttle",
    "outbound_slow",
    "outbound_latency",
    "disk_latency",
    "net_latency",
    "clock_skew",
    "clock_step_back",
    "clock_downtime",
];

/// Whether the axis `field` is on in `cfg`: it could fire, and turning it off
/// would change the config, judged by `turn_off` itself. A clock axis is a
/// chance and a size, and a chance of 0 never fires whatever the size, so it
/// is off even though `turn_off` would still zero its size (`sample_clock_fields`
/// can draw a step-back chance of 0 beside its 120 s size).
pub(crate) fn axis_on(cfg: &FaultConfig, field: &str) -> bool {
    let never_fires = match field {
        "clock_step_back" => cfg.clock_step_back.0 == 0.0,
        "clock_downtime" => cfg.clock_downtime.0 == 0.0,
        _ => false,
    };
    let mut off = cfg.clone();
    turn_off(&mut off, field);
    !never_fires && format!("{off:?}") != format!("{cfg:?}")
}

/// Turns off the `FaultConfig` field named `field`: a probability or a skew
/// to zero, a range or a clock axis to its off pair. The harness checks names
/// against `FAULT_AXES` before a run, so an unknown one here is a harness bug.
fn turn_off(cfg: &mut FaultConfig, field: &str) {
    const NONE: (Duration, Duration) = (Duration::ZERO, Duration::ZERO);
    match field {
        "write_error" => cfg.write_error = 0.0,
        "sync_error" => cfg.sync_error = 0.0,
        "crash_apply" => cfg.crash_apply = 0.0,
        "crash_torn" => cfg.crash_torn = 0.0,
        "dir_entry_loss" => cfg.dir_entry_loss = 0.0,
        "bit_flip" => cfg.bit_flip = 0.0,
        "misdirected_write" => cfg.misdirected_write = 0.0,
        "read_error" => cfg.read_error = 0.0,
        "object_put_error" => cfg.object_put_error = 0.0,
        "object_get_error" => cfg.object_get_error = 0.0,
        "object_lost_ack" => cfg.object_lost_ack = 0.0,
        "object_delete_error" => cfg.object_delete_error = 0.0,
        "conn_drop" => cfg.conn_drop = 0.0,
        "net_spike" => cfg.net_spike = 0.0,
        "subscriber_stall" => cfg.subscriber_stall = 0.0,
        "outbound_refused" => cfg.outbound_refused = 0.0,
        "outbound_lost_request" => cfg.outbound_lost_request = 0.0,
        "outbound_lost_reply" => cfg.outbound_lost_reply = 0.0,
        "outbound_throttle" => cfg.outbound_throttle = 0.0,
        "outbound_slow" => cfg.outbound_slow = 0.0,
        "outbound_latency" => cfg.outbound_latency = NONE,
        "disk_latency" => cfg.disk_latency = NONE,
        "net_latency" => cfg.net_latency = NONE,
        "clock_skew" => cfg.clock_skew = 0,
        "clock_step_back" => cfg.clock_step_back = (0.0, Duration::ZERO),
        "clock_downtime" => cfg.clock_downtime = (0.0, Duration::ZERO),
        _ => panic!("faults_off names \"{field}\", which is no FaultConfig fault axis"),
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
    adjust: &dyn Fn(u64, &mut FaultConfig),
    f: &dyn Fn(&mut Sim) -> Result<(), String>,
) -> Result<UniverseEnd, (FaultConfig, BTreeMap<&'static str, u64>, String)> {
    let mut s = seed;
    let cfg_seed = splitmix64(&mut s);
    let mut cfg = FaultConfig::swarm(&mut Rng::new(cfg_seed));
    sample_extended_fields(seed, &mut cfg);
    adjust(seed, &mut cfg);
    cfg.validate(); // an adjust closure that produces an invalid config is a workload bug
    let mut sim = Sim::with_faults(seed, cfg.clone());
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| f(&mut sim))) {
        Ok(Ok(())) => Ok(end_of(&sim)),
        Ok(Err(msg)) => Err((cfg, sim.coverage(), msg)),
        Err(payload) => Err((cfg, sim.coverage(), panic_message(&*payload))),
    }
}

fn run_one_swarm_with(
    package: &str,
    test_file: &str,
    seed: u64,
    adjust: &impl Fn(u64, &mut FaultConfig),
    f: &impl Fn(&mut Sim) -> Result<(), String>,
) -> UniverseEnd {
    match run_universe_with_adjust(seed, adjust, f) {
        Ok(end) => end,
        Err((cfg, coverage, msg)) => panic!(
            "{}",
            swarm_failure_report(package, test_file, seed, &cfg, &coverage, &msg)
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
    coverage: &BTreeMap<&'static str, u64>,
    msg: &str,
) -> String {
    format!(
        "\nsim failure in {test_file}: {msg}\n  seed:   {seed:#018x}\n  config: {cfg:?}\n  fired:  {}\n  repro:  CHARPENTE_SEED={seed:#x} cargo test -p {package} --test {test_file} -- --nocapture\n",
        render_fired(coverage)
    )
}

/// The faults and events that ACTUALLY fired in a universe, for a failure
/// report. `config:` above lists the axes that were ENABLED and their
/// probabilities — it says nothing about what happened, and reading a
/// diagnosis off it is reasoning from a probability (docs/LEDGER.md
/// 2026-09-07, where an acked-write finding was attributed to
/// `dir_entry_loss: 0.477` by every reader, including two agents and the
/// controller, with no artifact anywhere recording whether that axis fired
/// at all). "nothing" is a real answer and must print as one: a failure with
/// no faults fired is a plain bug, which is the single most useful thing a
/// report can say.
fn render_fired(coverage: &BTreeMap<&'static str, u64>) -> String {
    if coverage.is_empty() {
        return "nothing (no faults or events marked coverage in this universe)".to_string();
    }
    coverage
        .iter()
        .map(|(key, count)| format!("{key}: {count}"))
        .collect::<Vec<_>>()
        .join(", ")
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

    fn draw_some(sim: &Sim) -> (u64, &'static str, String) {
        let i = sim.input();
        (
            i.between("n", 3, 900),
            i.pick("p", &["a", "b", "c"]),
            i.text("t", &["x", "y", "z"], 1, 4),
        )
    }

    #[test]
    fn drawn_inputs_replay_from_their_own_stream() {
        let (a, b) = (Sim::new(7), Sim::new(7));
        assert_eq!(draw_some(&a), draw_some(&b));
        assert_ne!(draw_some(&Sim::new(8)), draw_some(&Sim::new(7)));
    }

    #[test]
    fn drawing_moves_no_fault_draw() {
        let cfg = FaultConfig::swarm(&mut Rng::new(5));
        let run = |draw: bool| {
            let sim = Sim::with_faults(11, cfg.clone());
            if draw {
                draw_some(&sim);
            }
            let mut rolls = Vec::new();
            for _ in 0..40 {
                rolls.push(sim.world.lock().faults.next_u64());
                rolls.push(sim.world.lock().entropy.next_u64());
            }
            (rolls, sim.coverage())
        };
        assert_eq!(run(true), run(false));
        let a = run_app_universe(3, None, &[], &|s| {
            draw_some(s);
            Ok(())
        });
        let b = run_app_universe(3, None, &[], &|_| Ok(()));
        let (a, b) = (a.unwrap(), b.unwrap());
        assert_eq!(a.coverage, b.coverage);
    }

    #[test]
    fn a_replayed_choice_sequence_reproduces_the_values() {
        let first = Sim::new(21);
        let want = draw_some(&first);
        let (choices, drawn) = first.input_record();
        let again = Sim::new(99);
        again.replay_choices(&choices);
        assert_eq!(draw_some(&again), want);
        assert_eq!(again.input_record(), (choices, drawn));
    }

    /// Each draw records the choice it used: the raw stream value reduced
    /// into the draw's range, the smallest raw value that draws the same
    /// value. The value is the one the raw value gives, and the stream is
    /// read once per draw.
    #[test]
    fn a_draw_records_the_choice_it_used_reduced_into_its_range() {
        let sim = Sim::new(21);
        let i = sim.input();
        let n = i.between("n", 3, 900);
        let p = *i.pick("p", &["a", "b", "c"]);
        let mut order = [1, 2, 3, 4];
        i.shuffle("order", &mut order);
        let t = i.text("t", &["x", "y", "z"], 1, 4);
        let mut stream = keyed_stream(21, "inputs");
        let mut raw = Vec::new();
        let mut want = Vec::new();
        let mut take = |span: u64| {
            let r = stream.next_u64();
            raw.push(r);
            want.push(r % span);
            r % span
        };
        assert_eq!(n, 3 + take(898));
        assert_eq!(p, ["a", "b", "c"][take(3) as usize]);
        let mut shuffled = [1, 2, 3, 4];
        for i in 0..4 {
            let j = i + take(4 - i as u64) as usize;
            shuffled.swap(i, j);
        }
        assert_eq!(order, shuffled);
        let words: Vec<&str> = (0..1 + take(4))
            .map(|_| ["x", "y", "z"][take(3) as usize])
            .collect();
        assert_eq!(t, words.join(" "));
        let (choices, _) = sim.input_record();
        assert_eq!(
            choices, want,
            "each choice is its raw value reduced: {raw:?}"
        );
        assert_ne!(choices, raw, "non-vacuous: a raw value was above its span");
    }

    /// A choice above its draw's range, as a hand-typed `--inputs` can give,
    /// draws what its reduced form draws, and is recorded reduced.
    #[test]
    fn a_replayed_choice_above_its_range_draws_as_its_reduced_form() {
        let run = |choices: &[u64]| {
            let sim = Sim::new(1);
            sim.replay_choices(choices);
            (draw_some(&sim), sim.input_record())
        };
        let (big, big_record) = run(&[898 + 7, 3 + 2, 4 + 2, 3 + 1]);
        let (small, small_record) = run(&[7, 2, 2, 1]);
        assert_eq!(big, (10, "c", "y x x".to_string()));
        assert_eq!(big, small);
        assert_eq!(big_record, small_record);
        assert_eq!(big_record.0, [7, 2, 2, 1, 0, 0]);
    }

    #[test]
    fn exhausted_choices_read_as_zero_and_give_the_simplest_values() {
        let sim = Sim::new(21);
        sim.replay_choices(&[]);
        assert_eq!(draw_some(&sim), (3, "a", "x".to_string()));
        assert!(sim.input_record().0.iter().all(|&c| c == 0));
    }

    /// A choice maps into `lo..=hi` as `lo + choice % (hi - lo + 1)`, so a
    /// span of 898 for 3..=900: 897 is the top value and 898 wraps to `lo`.
    /// Every value stays inside the bounds whatever the choice, the full range
    /// above a nonzero `lo` included.
    #[test]
    fn between_maps_a_choice_into_its_bounds_by_their_span() {
        let sim = Sim::new(1);
        sim.replay_choices(&[0, 5, 897, 898, u64::MAX, u64::MAX]);
        let i = sim.input();
        let got: Vec<u64> = (0..5).map(|_| i.between("n", 3, 900)).collect();
        assert_eq!(got, [3, 8, 900, 3, 3 + u64::MAX % 898]);
        // 1..=u64::MAX spans u64::MAX values, so the choice u64::MAX wraps to 1.
        assert_eq!(i.between("n", 1, u64::MAX), 1);
    }

    #[test]
    fn shuffle_with_zero_choices_is_identity() {
        let sim = Sim::new(1);
        sim.replay_choices(&[]);
        let mut v = [1, 2, 3, 4, 5];
        sim.input().shuffle("order", &mut v);
        assert_eq!(v, [1, 2, 3, 4, 5]);
        let live = Sim::new(1);
        let mut w: Vec<u32> = (0..20).collect();
        live.input().shuffle("order", &mut w);
        assert_ne!(w, (0..20).collect::<Vec<u32>>());
    }

    #[test]
    fn drawn_values_are_recorded_by_name_in_draw_order() {
        let sim = Sim::new(4);
        let i = sim.input();
        let n = i.between("count", 0, u64::MAX);
        let p = *i.pick("kind", &["a", "b"]);
        let (choices, drawn) = sim.input_record();
        assert_eq!(choices.len(), 2);
        assert_eq!(n, choices[0]);
        assert_eq!(drawn[0], ("count", format!("{n:?}")));
        assert_eq!(drawn[1], ("kind", format!("{p:?}")));
    }

    #[test]
    fn a_failing_app_universe_carries_its_choices_and_replays_them() {
        let body = |s: &mut Sim| {
            let n = s.input().between("n", 0, 1_000_000);
            Err(format!("n={n}"))
        };
        let f = run_app_universe(9, None, &[], &body).unwrap_err();
        assert_eq!(f.choices.len(), 1);
        assert_eq!(f.drawn[0].0, "n");
        let g = run_app_universe(1234, Some(&f.choices), &[], &body).unwrap_err();
        assert_eq!(g.message, f.message);
        let z = run_app_universe(9, Some(&[]), &[], &body).unwrap_err();
        assert_eq!(z.message, "n=0");
    }

    #[test]
    #[should_panic(expected = "between(\"n\", 5, 2): lo is above hi")]
    fn between_with_lo_above_hi_names_the_call() {
        Sim::new(1).input().between("n", 5, 2);
    }

    #[test]
    #[should_panic(expected = "pick(\"p\", ..): the slice is empty")]
    fn pick_on_empty_names_the_call() {
        Sim::new(1).input().pick::<u8>("p", &[]);
    }

    #[test]
    #[should_panic(expected = "sim.input().text(\"memo\", .., 3, 1): min_words is above \
                               max_words; pass the smaller count first")]
    fn text_with_min_above_max_names_the_call() {
        Sim::new(1).input().text("memo", &["rush"], 3, 1);
    }

    #[test]
    #[should_panic(
        expected = "sim.input().text(\"memo\", ..): `words` is empty but up to 2 \
                               words are wanted; pass at least one word or a max_words of 0"
    )]
    fn text_with_no_words_names_the_call() {
        Sim::new(1).input().text("memo", &[], 0, 2);
    }

    /// The empty-`words` refusal's other operand: wanting no words from no
    /// words is a draw of `""`, never a panic.
    #[test]
    fn text_with_no_words_and_a_max_of_zero_draws_nothing() {
        assert_eq!(Sim::new(1).input().text("memo", &[], 0, 0), "");
    }

    #[test]
    fn virtual_time_advances_only_by_simulation() {
        let sim = Sim::with_faults(1, FaultConfig::none());
        assert_eq!(sim.now(), Instant::ZERO);
        sim.clock().sleep(Duration::from_secs(3600));
        assert_eq!(sim.now(), Instant::from_nanos(3_600_000_000_000));
    }

    #[test]
    fn sim_clock_wall_is_virtual_time_plus_the_base() {
        let sim = Sim::with_faults(1, FaultConfig::none());
        let clock = sim.clock();
        assert_eq!(clock.wall().as_nanos(), SIM_WALL_BASE_NANOS);
        clock.sleep(Duration::from_secs(3));
        assert_eq!(clock.wall().as_nanos(), SIM_WALL_BASE_NANOS + 3_000_000_000);
    }

    /// A host's wall reading is true time plus that host's offset: its own
    /// skew, drawn once, then whatever its own step-backs and downtime added.
    /// `SimClock::offset` reports it without drawing. Each axis marks its key
    /// when a reading first shows it.
    #[test]
    fn a_host_wall_reading_is_true_time_plus_its_offset() {
        const SEC: i128 = 1_000_000_000;
        let offset = |clock: &SimClock, sim: &Sim| {
            clock.wall().as_nanos() as i128 - sim.true_wall().as_nanos() as i128
        };
        let fired = |sim: &Sim, key: &str| sim.coverage().get(key).copied().unwrap_or(0);

        // Skew: each clock draws its own, within ±|clock_skew|, keeps it, and
        // marks `clock:skewed` once, at its first read.
        let sim = Sim::with_faults(
            0xC10C_8001,
            FaultConfig {
                clock_skew: 90_000_000_000,
                ..FaultConfig::none()
            },
        );
        sim.clock().sleep(Duration::from_secs(5));
        assert_eq!(
            sim.true_wall().as_nanos(),
            SIM_WALL_BASE_NANOS + 5_000_000_000
        );
        let (global, a) = (sim.clock(), sim.host("a").clock());
        let (skew, skew_a) = (i128::from(global.offset()), i128::from(a.offset()));
        assert_eq!(fired(&sim, "clock:skewed"), 0, "offset() marks nothing");
        assert!((-90 * SEC..=90 * SEC).contains(&skew), "{skew} ns");
        assert!((-90 * SEC..=90 * SEC).contains(&skew_a), "{skew_a} ns");
        assert_ne!(skew, skew_a, "two clocks drew one skew");
        assert_eq!(offset(&global, &sim), skew);
        assert_eq!(offset(&global, &sim), skew);
        assert_eq!(offset(&a, &sim), skew_a);
        assert_eq!(fired(&sim, "clock:skewed"), 2, "once per clock that read");
        let again = Sim::with_faults(
            0xC10C_8001,
            FaultConfig {
                clock_skew: 90_000_000_000,
                ..FaultConfig::none()
            },
        );
        assert_eq!(
            (again.clock().offset(), again.host("a").clock().offset()),
            (skew as i64, skew_a as i64),
            "a seed names each clock's skew"
        );

        // A step back: at chance 1 every read steps, by (0, 120 s], and the
        // clock stays where the step left it. Host a's steps are its own. Each
        // step also marks its size against CLOCK_TOLERANCE.
        let sim = Sim::with_faults(
            0xC10C_8002,
            FaultConfig {
                clock_step_back: (1.0, Duration::from_secs(120)),
                ..FaultConfig::none()
            },
        );
        let clock = sim.clock();
        let mut last = 0;
        let mut beyond = 0;
        for read in 1..=10 {
            let now = offset(&clock, &sim);
            assert!(
                (1..=120 * SEC).contains(&(last - now)),
                "read {read} moved the clock from {last} ns to {now} ns"
            );
            beyond += u64::from(last - now > 60 * SEC);
            assert_eq!(
                i128::from(clock.offset()),
                now,
                "offset() is the last reading's"
            );
            last = now;
        }
        assert_eq!(fired(&sim, "clock:stepped-back"), 10);
        assert_eq!(fired(&sim, "clock:stepped-back:beyond-tolerance"), beyond);
        assert_eq!(
            fired(&sim, "clock:stepped-back:within-tolerance"),
            10 - beyond
        );
        assert!(
            0 < beyond && beyond < 10,
            "this seed steps on both sides of the tolerance: {beyond} of 10 beyond"
        );
        assert!(last < -120 * SEC, "ten steps on this seed: {last} ns");
        let a = offset(&sim.host("a").clock(), &sim);
        assert!(
            (-120 * SEC..0).contains(&a),
            "host a's first read steps from its own offset, 0, not the global clock's {last} ns: {a} ns"
        );
        assert_eq!(fired(&sim, "clock:stepped-back"), 11);
        assert_eq!(fired(&sim, "clock:skewed"), 0, "no skew, no mark");

        // Downtime: a power cycle jumps that host's clock forward by
        // (0, 3 days], and no other host's. A process restart is not one. The
        // jump is marked at the first reading that shows it.
        let sim = Sim::with_faults(
            0xC10C_8003,
            FaultConfig {
                clock_downtime: (1.0, Duration::from_secs(3 * 86_400)),
                ..FaultConfig::none()
            },
        );
        assert_eq!(offset(&sim.clock(), &sim), 0);
        sim.restart_process();
        assert_eq!(
            offset(&sim.clock(), &sim),
            0,
            "a process restart is not downtime"
        );
        sim.restart();
        assert_eq!(fired(&sim, "clock:downtime"), 0, "no reading shows it yet");
        let jumped = offset(&sim.clock(), &sim);
        assert!((1..=3 * 86_400 * SEC).contains(&jumped), "{jumped} ns");
        assert_eq!(fired(&sim, "clock:downtime"), 1);
        let host = sim.host("a");
        assert_eq!(
            offset(&host.clock(), &sim),
            0,
            "the global power cycle left host a alone"
        );
        host.restart();
        assert_eq!(fired(&sim, "clock:downtime"), 1, "host a's clock is unread");
        let host_jumped = offset(&host.clock(), &sim);
        assert!(
            (1..=3 * 86_400 * SEC).contains(&host_jumped),
            "{host_jumped} ns"
        );
        assert_eq!(
            offset(&sim.clock(), &sim),
            jumped,
            "host a's power cycle left the global clock alone"
        );
        assert_eq!(fired(&sim, "clock:downtime"), 2);
    }

    /// `quiesce_clock_faults` freezes every clock's offset where its faults
    /// left it, skew included, and draws nothing from then on.
    #[test]
    fn quiesce_clock_faults_freezes_every_offset_and_draws_nothing() {
        let cfg = FaultConfig {
            clock_skew: 90_000_000_000,
            clock_step_back: (1.0, Duration::from_secs(120)),
            clock_downtime: (1.0, Duration::from_secs(3 * 86_400)),
            ..FaultConfig::none()
        };
        let sim = Sim::with_faults(0xC10C_8005, cfg.clone());
        let (global, host) = (sim.clock(), sim.host("a"));
        global.wall();
        host.clock().wall();
        sim.restart();
        host.restart();
        sim.quiesce_clock_faults();
        let frozen = (global.offset(), host.clock().offset());
        let coverage = sim.coverage();
        for _ in 0..10 {
            global.wall();
            host.clock().wall();
        }
        sim.restart();
        host.restart();
        global.sleep(Duration::from_secs(30));
        let true_wall = sim.true_wall().as_nanos() as i128;
        assert_eq!((global.offset(), host.clock().offset()), frozen);
        assert_eq!(
            global.wall().as_nanos() as i128 - true_wall,
            i128::from(frozen.0)
        );
        let mut after = sim.coverage();
        // The jumps before the quiesce are marked at the first read after it.
        let jumps = after.remove("clock:downtime");
        assert_eq!(jumps, Some(2), "the two pre-quiesce jumps, and no more");
        let mut before = coverage;
        before.remove("clock:downtime");
        after.remove("restart");
        before.remove("restart");
        assert_eq!(after, before, "a quiesced clock still stepped back");
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
    fn quiesce_disk_faults_ends_every_disk_fault_on_every_disk() {
        let mut cfg = FaultConfig::none();
        cfg.write_error = 1.0;
        cfg.sync_error = 1.0;
        cfg.read_error = 1.0;
        cfg.misdirected_write = 1.0;
        cfg.bit_flip = 1.0;
        cfg.corruption_scope = String::new();
        let sim = Sim::with_faults(0x5145_0001, cfg);
        let host = sim.host("h");
        let (global, hosted) = (sim.disk(), host.disk());
        // The control: at these rates every roll fires on both disks.
        for disk in [&global, &hosted] {
            let file = disk.open("f").unwrap();
            assert!(file.write_at(0, b"x").is_err());
            // The failed write left nothing to flush, and a clean sync cannot
            // fail; `set_len` rolls no fault, so it gives the sync work to do.
            file.set_len(1).unwrap();
            assert!(file.sync().is_err());
            assert!(disk.sync_dir("").is_err());
        }
        sim.crash_disk_after(0);
        host.crash_after(0);

        sim.quiesce_disk_faults();
        let fault_keys = |sim: &Sim| -> Vec<(&'static str, u64)> {
            sim.coverage()
                .into_iter()
                .filter(|(k, _)| k.starts_with("fault:"))
                .collect()
        };
        let before = fault_keys(&sim);
        for disk in [&global, &hosted] {
            let file = disk.open("f").unwrap();
            file.write_at(0, b"abcdefgh").unwrap(); // the armed countdowns are gone
            file.sync().unwrap();
            disk.sync_dir("").unwrap();
            let mut buf = [0u8; 8];
            file.read_at(0, &mut buf).unwrap();
            assert_eq!(&buf, b"abcdefgh", "the write landed where it was aimed");
        }
        sim.restart();
        host.restart();
        for disk in [&global, &hosted] {
            let mut buf = [0u8; 8];
            disk.open("f").unwrap().read_at(0, &mut buf).unwrap();
            assert_eq!(&buf, b"abcdefgh", "no bit flipped at restart");
        }
        assert_eq!(
            fault_keys(&sim),
            before,
            "no disk fault fired after quiescing"
        );
    }

    #[test]
    fn a_process_restart_keeps_the_page_cache_and_a_power_cycle_does_not() {
        // FaultConfig::none(): a power cycle drops every unsynced write, so
        // whatever is still readable after a restart is what the mode kept.
        let sim = Sim::with_faults(0x5145_0002, FaultConfig::none());
        let disk = sim.disk();
        let old = disk.open("f").unwrap();
        old.write_at(0, b"unsynced").unwrap();
        disk.open("g").unwrap(); // a name whose directory entry is not durable

        sim.restart_process();
        assert!(
            old.len()
                .unwrap_err()
                .to_string()
                .contains("predates a simulated restart"),
            "the dead process's handle must not reach the disk"
        );
        let reopened = disk.open("f").unwrap();
        assert_eq!(
            reopened.len().unwrap(),
            8,
            "a process restart keeps the page cache: the unsynced write is still there"
        );
        let mut buf = [0u8; 8];
        reopened.read_at(0, &mut buf).unwrap();
        assert_eq!(
            &buf, b"unsynced",
            "a process restart keeps the page cache: the next process reads the unsynced write back"
        );
        assert_eq!(
            disk.list("").unwrap(),
            vec!["f".to_string(), "g".to_string()],
            "a process restart keeps a directory entry that was never made durable"
        );
        assert_eq!(
            sim.unsynced_files(),
            vec!["f".to_string()],
            "a process restart leaves the dead process's unsynced write unsynced"
        );
        let coverage = sim.coverage();
        assert_eq!(
            coverage.get("restart:process"),
            Some(&1),
            "a process restart marks restart:process once"
        );
        assert!(
            !coverage
                .keys()
                .any(|k| *k == "restart" || k.starts_with("crash:")),
            "a process restart is not a power cycle: nothing collapsed: {coverage:?}"
        );

        sim.restart(); // the power cycle drops what the process restart kept
        assert_eq!(
            disk.open("f").unwrap().len().unwrap(),
            0,
            "a power cycle drops the unsynced write the process restart kept — if it is still \
             here, the write was made durable before the power cycle"
        );
        assert!(
            sim.unsynced_files().is_empty(),
            "a power cycle leaves nothing unsynced"
        );
    }

    #[test]
    fn a_process_restart_draws_from_no_stream() {
        // Twin universes, one with a process restart and one without, must
        // collapse identically at the next power cycle and draw the same
        // entropy after it: the mode adds a coverage key and nothing else, so
        // introducing it re-keys no existing universe.
        let run = |process_restart: bool| {
            let sim = Sim::with_faults(
                0x5145_0004,
                FaultConfig {
                    crash_apply: 0.4,
                    crash_torn: 0.3,
                    ..FaultConfig::none()
                },
            );
            let disk = sim.disk();
            for i in 0..16u8 {
                let file = disk.open(&format!("f{i}")).unwrap();
                file.write_at(0, &[i; 1500]).unwrap();
            }
            if process_restart {
                sim.restart_process();
            }
            sim.restart();
            let mut coverage = sim.coverage();
            coverage.remove("restart:process");
            (
                sim.durable_fingerprint(),
                sim.entropy().next_u64(),
                coverage,
            )
        };
        assert_eq!(
            run(true),
            run(false),
            "a process restart changed the next power cycle's collapse, the entropy after it, or \
             the coverage: it drew from a stream or collapsed something, so introducing it \
             re-keys universes"
        );
    }

    #[test]
    #[should_panic(expected = "only Sim::restart (the power cycle) brings the disk back")]
    fn a_process_restart_refuses_a_crashed_disk() {
        let sim = Sim::with_faults(0x5145_0003, FaultConfig::none());
        sim.crash_disk_now();
        sim.restart_process();
    }

    #[test]
    fn a_directory_barrier_does_not_perturb_the_other_disk_faults() {
        // A barrier's fault roll must come off its own stream. Sharing one with
        // reads, writes and file syncs means that merely ADDING a barrier — the
        // exact shape of `Wal::open_with_horizon`'s durability fix — displaces
        // every later roll and re-rolls every pinned universe. That is what
        // forced convergence-floor re-measures in three separate files on
        // 2026-09-07; DESIGN.md's object-store delete split is the same repair
        // for the same hazard.
        fn run(extra_barrier: bool) -> BTreeMap<&'static str, u64> {
            let mut cfg = FaultConfig::none();
            cfg.write_error = 0.3;
            cfg.read_error = 0.3;
            // `sync_error` stays at zero deliberately. `Rng::chance` draws BEFORE
            // it compares, so a barrier still consumes its draw at p = 0 — which
            // means the perturbation is still exercised, while `fault:sync_error`
            // never marks. The two runs can then be compared as WHOLE coverage
            // maps instead of a filtered subset that could hide a difference.
            let sim = Sim::with_faults(0x9012_0100, cfg);
            let disk = sim.disk();
            if extra_barrier {
                // Nothing is pending under this prefix, so the barrier is a
                // semantic no-op: the only thing it can alter is the schedule.
                disk.sync_dir("unrelated/").unwrap();
            }
            let file = disk.open("f").unwrap();
            for i in 0..40u64 {
                let _ = file.write_at(i * 8, b"abcdefgh");
                let _ = file.sync();
                let mut buf = [0u8; 8];
                let _ = file.read_at(i * 8, &mut buf);
            }
            sim.coverage()
        }

        let baseline = run(false);
        let with_barrier = run(true);
        assert!(
            baseline.values().sum::<u64>() > 0,
            "anti-vacuity: this workload must actually inject faults, or the \
             comparison below passes by comparing two empty maps"
        );
        assert_eq!(
            baseline, with_barrier,
            "inserting a no-op directory barrier changed the fault schedule: the \
             barrier is drawing from the shared per-host stream, so every later \
             read/write/sync roll shifted. Key it separately — DESIGN.md made the \
             same repair for the object store's delete arm."
        );
    }

    #[test]
    fn a_sync_with_nothing_to_flush_draws_no_fault() {
        // Twin universes, one of which also syncs a file it never wrote,
        // before every op of the workload: a clean sync has nothing to flush
        // and nothing to fail, so it must neither fail nor move a single later
        // roll. Syncing durable files is the exact shape of a durability fix
        // made after a power cycle (recovery's per-segment sync).
        fn run(clean_syncs: bool) -> BTreeMap<&'static str, u64> {
            let mut cfg = FaultConfig::none();
            cfg.write_error = 0.3;
            cfg.read_error = 0.3;
            cfg.sync_error = 0.3;
            let sim = Sim::with_faults(0x5145_0005, cfg);
            let disk = sim.disk();
            let clean = disk.open("clean").unwrap();
            let file = disk.open("f").unwrap();
            for i in 0..40u64 {
                if clean_syncs {
                    clean
                        .sync()
                        .expect("a sync with nothing to flush must not fail");
                }
                let _ = file.write_at(i * 8, b"abcdefgh");
                let _ = file.sync();
                let mut buf = [0u8; 8];
                let _ = file.read_at(i * 8, &mut buf);
            }
            sim.coverage()
        }

        let baseline = run(false);
        assert!(
            baseline.get("fault:sync_error").copied().unwrap_or(0) > 0,
            "anti-vacuity: a dirty file's sync must still fail, or the comparison below \
             passes with the sync fault switched off entirely: {baseline:?}"
        );
        assert_eq!(
            baseline,
            run(true),
            "syncing a file with nothing to flush changed the fault schedule: a clean \
             sync rolled a fault it can never fire honestly, so every later roll shifted"
        );
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
    fn a_seed_parses_with_or_without_its_prefix_and_separators() {
        let parsed: Vec<u64> = ["0x5A9E_0600", "0X5a9e0600", "5a9e_06_00", "0x_5A9E0600"]
            .iter()
            .map(|raw| parse_seed("CHARPENTE_BASE_SEED", raw))
            .collect();
        assert_eq!(parsed, [0x5A9E_0600; 4]);
    }

    #[test]
    fn a_seed_that_is_not_hex_panics_naming_the_variable_it_came_from() {
        for var in ["CHARPENTE_SEED", "CHARPENTE_BASE_SEED"] {
            let payload = std::panic::catch_unwind(|| parse_seed(var, "0xnope"))
                .expect_err("a non-hex seed panics");
            assert_eq!(
                payload.downcast_ref::<String>().map(String::as_str),
                Some(format!("{var} must be a hex seed like 0xdeadbeef — got \"0xnope\"").as_str()),
            );
        }
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
    fn durable_corruption_fired_names_the_host_it_fired_on() {
        // A misdirected write on `a`'s disk marks `a` alone.
        let sim = Sim::with_faults(
            0xB17_0003,
            FaultConfig {
                misdirected_write: 1.0,
                ..FaultConfig::none()
            },
        );
        let (a, b) = (sim.host("a"), sim.host("b"));
        assert!(!a.durable_corruption_fired() && !b.durable_corruption_fired());
        let out_of_scope = a.disk().open("tables.redb").unwrap();
        out_of_scope.write_at(0, b"payload!").unwrap();
        assert!(
            !a.durable_corruption_fired(),
            "a write outside the corruption scope fires nothing, so marks nothing"
        );
        let file = a.disk().open("wal/f").unwrap();
        file.write_at(0, b"payload!").unwrap();
        assert_eq!(sim.coverage()["fault:misdirected_write"], 1);
        assert!(a.durable_corruption_fired());
        assert!(!b.durable_corruption_fired(), "b's disk saw no fault");

        // A bit flip at `b`'s restart marks `b` alone.
        let sim = Sim::with_faults(
            0xB17_0004,
            FaultConfig {
                bit_flip: 1.0,
                ..FaultConfig::none()
            },
        );
        let (a, b) = (sim.host("a"), sim.host("b"));
        let file = b.disk().open("wal/g").unwrap();
        file.write_at(0, &[0xAA; 64]).unwrap();
        file.sync().unwrap();
        a.restart();
        assert_eq!(
            sim.coverage().get("fault:bit_flip"),
            None,
            "a has no durable in-scope bytes, so its restart flips nothing"
        );
        assert!(!a.durable_corruption_fired());
        b.restart();
        assert_eq!(sim.coverage()["fault:bit_flip"], 1);
        assert!(b.durable_corruption_fired());
        assert!(!a.durable_corruption_fired(), "the flip was on b's disk");
    }

    #[test]
    fn dir_entry_loss_fired_names_the_host_it_fired_on() {
        let sim = Sim::with_faults(
            0xD1E_0001,
            FaultConfig {
                dir_entry_loss: 1.0,
                ..FaultConfig::none()
            },
        );
        let (a, b) = (sim.host("a"), sim.host("b"));
        b.restart();
        assert!(
            !b.durable_corruption_fired() && !b.dir_entry_loss_fired(),
            "b has no pending directory entry, so its restart loses none"
        );
        // A create whose directory entry was never synced vanishes at `a`'s
        // restart: entry loss on `a` alone.
        let file = a.disk().open("wal/f").unwrap();
        file.write_at(0, b"payload!").unwrap();
        file.sync().unwrap();
        a.restart();
        assert_eq!(sim.coverage()["fault:dir-entry"], 1);
        assert!(a.dir_entry_loss_fired());
        assert!(
            !a.durable_corruption_fired(),
            "entry loss is not durable corruption"
        );
        assert!(!b.dir_entry_loss_fired(), "the entry was lost on a's disk");
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
    fn outbound_axes_are_off_in_none_and_in_a_bare_swarm() {
        let none = FaultConfig::none();
        assert_eq!(
            (
                none.outbound_refused,
                none.outbound_lost_request,
                none.outbound_lost_reply
            ),
            (0.0, 0.0, 0.0)
        );
        assert_eq!((none.outbound_throttle, none.outbound_slow), (0.0, 0.0));
        assert_eq!(none.outbound_latency, (Duration::ZERO, Duration::ZERO));
        let swarm = FaultConfig::swarm(&mut Rng::new(7));
        assert_eq!(
            swarm.outbound_lost_reply, 0.0,
            "swarm alone draws no extended axis"
        );
    }

    /// The A14 property, at the source: sampling the outbound axes perturbs no
    /// axis that existed before them, for any seed.
    #[test]
    fn outbound_axes_perturb_no_existing_draw() {
        for seed in [0u64, 1, 0x912313adf8115ad0, 0x483e230baed4922b, u64::MAX] {
            let mut with = FaultConfig::swarm(&mut Rng::new(seed));
            sample_extended_fields(seed, &mut with);
            let mut without = with.clone();
            without.outbound_refused = 0.0;
            without.outbound_lost_request = 0.0;
            without.outbound_lost_reply = 0.0;
            without.outbound_throttle = 0.0;
            without.outbound_slow = 0.0;
            without.outbound_latency = (Duration::ZERO, Duration::ZERO);
            let mut legacy = FaultConfig::swarm(&mut Rng::new(seed));
            sample_extended_fields_without_outbound_for_tests(seed, &mut legacy);
            assert_eq!(
                format!("{without:?}"),
                format!("{legacy:?}"),
                "seed {seed:#x}"
            );
        }
    }

    /// Spec §4: adding the clock axes moves none of the swarm config's
    /// existing draws. They sample from their own keyed streams, after
    /// everything else, so every field that existed before them is the same
    /// with and without them. The counts keep it from passing vacuously.
    #[test]
    fn clock_axes_move_no_existing_draw() {
        let mut on = [0u32; 3];
        for seed in 0..1_000u64 {
            let mut s = seed;
            let cfg_seed = splitmix64(&mut s);
            let mut without = FaultConfig::swarm(&mut Rng::new(cfg_seed));
            sample_extended_fields(seed, &mut without);
            let mut with = without.clone();
            sample_clock_fields(seed, &mut with);
            with.validate();
            on[0] += u32::from(with.clock_skew != 0);
            on[1] += u32::from(with.clock_step_back != (0.0, Duration::ZERO));
            on[2] += u32::from(with.clock_downtime != (0.0, Duration::ZERO));
            assert!(
                with.clock_skew.unsigned_abs() <= 120_000_000_000,
                "seed {seed}: skew {} ns",
                with.clock_skew
            );
            with.clock_skew = 0;
            with.clock_step_back = (0.0, Duration::ZERO);
            with.clock_downtime = (0.0, Duration::ZERO);
            assert_eq!(format!("{with:?}"), format!("{without:?}"), "seed {seed}");
        }
        // Each axis is on in a quarter of runs.
        for (axis, count) in ["clock_skew", "clock_step_back", "clock_downtime"]
            .iter()
            .zip(on)
        {
            assert!(
                (200..=300).contains(&count),
                "{axis} was on in {count} of 1000 seeds"
            );
        }
    }

    /// Spec §4, at the universe: with the clock axes turned off, an app run
    /// decides exactly as the harness did before them (`run_universe`) —
    /// same outcome, coverage, message and every other config field. With
    /// them on, some runs differ, so the comparison is not vacuous.
    #[test]
    fn app_runs_with_the_clock_axes_off_decide_as_before() {
        fn workload(sim: &mut Sim) -> Result<(), String> {
            let file = sim.disk().open("wal/f").map_err(|e| e.to_string())?;
            let mut payload = [0u8; 64];
            sim.entropy().fill(&mut payload);
            let wrote = file.write_at(0, &payload).is_ok() && file.sync().is_ok();
            sim.cover(if wrote {
                "toy:wrote"
            } else {
                "toy:write-failed"
            });
            let before = sim.clock().wall();
            sim.restart();
            let after = sim.clock().wall();
            if payload[0] % 5 == 0 {
                return Err(format!("toy failure from {before} to {after}"));
            }
            if payload[1] % 3 == 0 {
                sim.give_up("toy give-up", "");
            }
            Ok(())
        }
        type Seen = (String, BTreeMap<&'static str, u64>, String);
        let off = |cfg: &FaultConfig| {
            let mut cfg = cfg.clone();
            cfg.clock_skew = 0;
            cfg.clock_step_back = (0.0, Duration::ZERO);
            cfg.clock_downtime = (0.0, Duration::ZERO);
            format!("{cfg:?}")
        };
        let app = |seed: u64, faults_off: &[&'static str]| -> Seen {
            match run_app_universe(seed, None, faults_off, &workload) {
                Ok(end) => (format!("{:?}", end.outcome), end.coverage, String::new()),
                Err(f) => (off(&f.config), f.coverage, f.message),
            }
        };
        let mut differed = 0;
        for seed in 0..300u64 {
            let before: Seen = match run_universe(seed, &workload) {
                Ok(end) => (format!("{:?}", end.outcome), end.coverage, String::new()),
                Err((config, coverage, message)) => (off(&config), coverage, message),
            };
            assert_eq!(
                app(seed, &["clock_skew", "clock_step_back", "clock_downtime"]),
                before,
                "seed {seed:#x}"
            );
            differed += u32::from(app(seed, &[]) != before);
        }
        assert!(differed > 0, "the clock axes changed no run of 300");
    }

    /// With the clock axes on, a clock draws from no stream but its own: on a
    /// workload whose decisions read only the disk and entropy, on the sim and
    /// on a host, each run decides exactly as with the axes off. Same outcome,
    /// message and coverage, once the `clock:*` keys are set aside, and those
    /// keys fire, so the axes acted. A step back or a jump drawn from a disk's
    /// fault stream would move that disk's later faults, and this goes red.
    #[test]
    fn the_clock_axes_draw_from_no_stream_but_their_own() {
        fn workload(sim: &mut Sim) -> Result<(), String> {
            let host = sim.host("a");
            let mut payload = [0u8; 16];
            for round in 0..4u64 {
                sim.clock().wall();
                host.clock().wall();
                for (disk, key) in [(sim.disk(), "toy:sim"), (host.disk(), "toy:host")] {
                    let file = disk.open("wal/f").map_err(|e| e.to_string())?;
                    sim.entropy().fill(&mut payload);
                    let wrote = file.write_at(round * 16, &payload).is_ok() && file.sync().is_ok();
                    sim.cover(key);
                    sim.cover(if wrote {
                        "toy:wrote"
                    } else {
                        "toy:write-failed"
                    });
                }
                sim.clock().wall();
                host.clock().wall();
                if round % 2 == 0 {
                    sim.restart();
                } else {
                    host.restart();
                }
            }
            if payload[0] % 5 == 0 {
                return Err(format!("toy failure on byte {}", payload[0]));
            }
            if payload[1] % 3 == 0 {
                sim.give_up("toy give-up", "");
            }
            Ok(())
        }
        type Seen = (String, BTreeMap<&'static str, u64>, String);
        let clock_off = |cfg: &FaultConfig| {
            let mut cfg = cfg.clone();
            cfg.clock_skew = 0;
            cfg.clock_step_back = (0.0, Duration::ZERO);
            cfg.clock_downtime = (0.0, Duration::ZERO);
            format!("{cfg:?}")
        };
        let mut fired: BTreeMap<&'static str, u64> = BTreeMap::new();
        let mut run = |seed: u64, faults_off: &[&'static str]| -> Seen {
            let (seen, mut coverage) = match run_app_universe(seed, None, faults_off, &workload) {
                Ok(end) => ((format!("{:?}", end.outcome), String::new()), end.coverage),
                Err(f) => ((clock_off(&f.config), f.message), f.coverage),
            };
            coverage.retain(|key, n| {
                if key.starts_with("clock:") {
                    *fired.entry(key).or_insert(0) += *n;
                }
                !key.starts_with("clock:")
            });
            (seen.0, coverage, seen.1)
        };
        for seed in 0..300u64 {
            let on = run(seed, &[]);
            let off = run(seed, &["clock_skew", "clock_step_back", "clock_downtime"]);
            assert_eq!(on, off, "seed {seed:#x}");
        }
        for key in [
            "clock:skewed",
            "clock:stepped-back",
            "clock:stepped-back:within-tolerance",
            "clock:stepped-back:beyond-tolerance",
            "clock:downtime",
        ] {
            assert!(
                fired.get(key).copied().unwrap_or(0) > 0,
                "{key} never fired in 300 runs: {fired:?}"
            );
        }
    }

    #[test]
    #[should_panic(
        expected = "host \"a\"'s clock (SimHost::clock)'s wall reading left u64 nanoseconds"
    )]
    fn a_wall_reading_before_the_epoch_panics_naming_the_host() {
        // A step back of up to i64::MAX ns; on this seed host a's first one
        // is past the 2026 base, so the reading would precede the epoch.
        let sim = Sim::with_faults(
            0xC10C_8004,
            FaultConfig {
                clock_step_back: (1.0, Duration::from_nanos(i64::MAX as u64)),
                ..FaultConfig::none()
            },
        );
        sim.host("a").clock().wall();
    }

    #[test]
    fn some_seed_turns_each_outbound_axis_on() {
        let mut seen = [false; 6];
        for seed in 0..512u64 {
            let mut cfg = FaultConfig::swarm(&mut Rng::new(seed));
            sample_extended_fields(seed, &mut cfg);
            seen[0] |= cfg.outbound_refused > 0.0;
            seen[1] |= cfg.outbound_lost_request > 0.0;
            seen[2] |= cfg.outbound_lost_reply > 0.0;
            seen[3] |= cfg.outbound_throttle > 0.0;
            seen[4] |= cfg.outbound_slow > 0.0;
            seen[5] |= cfg.outbound_latency.1 > Duration::ZERO;
            cfg.validate();
        }
        assert_eq!(seen, [true; 6]);
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
    fn quiesce_subscriber_stalls_ends_the_rate_for_the_rest_of_the_universe() {
        let sim = Sim::with_faults(
            0x2EB2_0002,
            FaultConfig {
                subscriber_stall: 0.42,
                ..FaultConfig::none()
            },
        );
        assert_eq!(
            sim.subscriber_stall(),
            0.42,
            "the control: the rate is live"
        );
        sim.quiesce_subscriber_stalls();
        assert_eq!(sim.subscriber_stall(), 0.0);
        assert_eq!(sim.subscriber_stall(), 0.0, "and it stays off");
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
                &[],
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
                &[],
                |_seed: u64, _cfg: &mut FaultConfig| {},
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
    fn the_ambient_runner_enforces_a_floor_too() {
        let universes_seen = std::sync::atomic::AtomicUsize::new(0);
        let payload = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            crate::run_universes_swarm!(
                4,
                |sim: &mut Sim| {
                    if universes_seen.fetch_add(1, std::sync::atomic::Ordering::Relaxed) == 0 {
                        sim.cover("rare:key");
                    }
                    Ok(())
                },
                require: [],
                floor: [("rare:key", 3)]
            );
        }))
        .expect_err("an ambient batch below its floor must fail too");
        let msg = panic_message(&*payload);
        assert!(msg.contains("rare:key: 1 of 3 required"), "{msg}");
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
            &[],
            |_seed: u64, _cfg: &mut FaultConfig| {},
            |sim: &mut Sim| {
                sim.cover("common:key");
                Ok(())
            },
        );
    }

    #[test]
    fn give_up_keeps_the_reason_and_the_detail_apart() {
        let sim = Sim::new(0x10_0001);
        sim.give_up("fixed reason", format!("seq {}", 7));
        assert_eq!(sim.gave_up(), Some("fixed reason"));
        assert_eq!(sim.gave_up_detail().as_deref(), Some("seq 7"));
        assert_eq!(
            end_of(&sim).outcome,
            Outcome::GaveUp {
                reason: "fixed reason",
                detail: Some("seq 7".to_string())
            }
        );
    }

    #[test]
    fn the_first_give_up_wins() {
        let sim = Sim::new(0x10_0002);
        sim.give_up("first", "one");
        sim.give_up("second", "two");
        assert_eq!(sim.gave_up(), Some("first"));
        assert_eq!(sim.gave_up_detail().as_deref(), Some("one"));
        // The other half: a first call with no detail keeps none, even when a
        // later call brings one.
        let sim = Sim::new(0x10_0005);
        sim.give_up("first", "");
        sim.give_up("second", "two");
        assert_eq!(sim.gave_up(), Some("first"));
        assert_eq!(sim.gave_up_detail(), None);
    }

    #[test]
    fn fault_axes_are_every_field_but_the_scope() {
        let fields: Vec<String> = format!("{:?}", FaultConfig::default())
            .split(&['{', '}', ',', ' '][..])
            .filter(|t| t.ends_with(':'))
            .map(|t| t.trim_end_matches(':').to_string())
            .filter(|f| f != "corruption_scope")
            .collect();
        assert_eq!(fields, FAULT_AXES, "FAULT_AXES lists the fields in order");
        let ms = Duration::from_millis(1);
        let every_axis_on = FaultConfig {
            write_error: 0.1,
            sync_error: 0.1,
            crash_apply: 0.1,
            crash_torn: 0.1,
            dir_entry_loss: 0.1,
            bit_flip: 0.1,
            misdirected_write: 0.1,
            read_error: 0.1,
            object_put_error: 0.1,
            object_get_error: 0.1,
            object_lost_ack: 0.1,
            object_delete_error: 0.1,
            corruption_scope: CORRUPTION_SCOPE_DEFAULT.to_string(),
            conn_drop: 0.1,
            net_spike: 0.1,
            subscriber_stall: 0.1,
            outbound_refused: 0.1,
            outbound_lost_request: 0.1,
            outbound_lost_reply: 0.1,
            outbound_throttle: 0.1,
            outbound_slow: 0.1,
            outbound_latency: (ms, ms),
            disk_latency: (ms, ms),
            net_latency: (ms, ms),
            clock_skew: 1,
            clock_step_back: (0.1, ms),
            clock_downtime: (0.1, ms),
        };
        for axis in FAULT_AXES {
            assert!(
                !axis_on(&FaultConfig::none(), axis),
                "{axis} is off in none()"
            );
            assert!(axis_on(&every_axis_on, axis), "{axis} is on");
            // `turn_off` turns off the axis it names and no other.
            let mut cfg = every_axis_on.clone();
            turn_off(&mut cfg, axis);
            for other in FAULT_AXES {
                assert_eq!(axis_on(&cfg, other), other != axis, "{axis} off, {other}");
            }
        }
        // A clock axis with a chance of 0 cannot fire, whatever its size.
        let mut cfg = every_axis_on.clone();
        cfg.clock_step_back = (0.0, Duration::from_secs(120));
        cfg.clock_downtime = (0.0, Duration::from_secs(3 * 86_400));
        assert!(!axis_on(&cfg, "clock_step_back"));
        assert!(!axis_on(&cfg, "clock_downtime"));
    }

    #[test]
    fn turning_an_axis_off_moves_no_other_field() {
        for seed in 0..300u64 {
            let sampled = app_config(seed, &[]);
            for axis in FAULT_AXES {
                let mut want = sampled.clone();
                turn_off(&mut want, axis);
                assert_eq!(
                    format!("{:?}", app_config(seed, &[axis])),
                    format!("{want:?}"),
                    "seed {seed:#x}, {axis} off"
                );
            }
        }
    }

    #[test]
    fn an_empty_detail_is_none() {
        let sim = Sim::new(0x10_0003);
        sim.give_up("fixed reason", "");
        assert_eq!(sim.gave_up(), Some("fixed reason"));
        assert_eq!(sim.gave_up_detail(), None);
    }

    #[test]
    fn the_skipped_key_is_run_skipped() {
        let sim = Sim::new(0x10_0005);
        sim.give_up("a", "");
        assert_eq!(sim.coverage().get("run:skipped"), Some(&1));
        assert_eq!(sim.coverage().get("universe:gave-up"), None);
    }

    #[test]
    fn give_up_marks_the_skipped_key_once_per_run() {
        let sim = Sim::new(0x10_0004);
        sim.give_up("a", "x");
        sim.give_up("b", "y");
        assert_eq!(sim.coverage().get("run:skipped"), Some(&1));
    }

    #[test]
    fn a_give_up_rate_above_its_ceiling_fails_the_batch_naming_key_count_and_cap() {
        let payload = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            run_universes_swarm_with_in(
                "charpente-sim",
                "swarm_meta.rs",
                0x5100_0003,
                4,
                &[],
                &[],
                &[("run:skipped", 0)],
                |_seed: u64, _cfg: &mut FaultConfig| {},
                |sim: &mut Sim| {
                    sim.give_up("this workload adjudicates nothing", "");
                    Ok(())
                },
            );
        }))
        .expect_err("a key above its ceiling must fail the batch");
        let msg = panic_message(&*payload);
        assert!(
            msg.contains("sim coverage ceiling failed in swarm_meta"),
            "{msg}"
        );
        assert!(msg.contains("run:skipped: 4 over a cap of 0"), "{msg}");
    }

    #[test]
    fn a_ceiling_that_holds_does_not_fail_the_batch() {
        let batch = |cap: u64, give_up: bool| {
            run_universes_swarm_with_in(
                "charpente-sim",
                "swarm_meta.rs",
                0x5100_0004,
                4,
                &[],
                &[],
                &[("run:skipped", cap)],
                |_seed: u64, _cfg: &mut FaultConfig| {},
                |sim: &mut Sim| {
                    if give_up {
                        sim.give_up("this workload adjudicates nothing", "");
                    }
                    Ok(())
                },
            )
        };
        // A key never marked counts as zero, which a cap of zero allows.
        batch(0, false);
        // At the cap is not above it...
        batch(4, true);
        // ...and one under it is.
        let payload = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| batch(3, true)))
            .expect_err("4 give-ups against a cap of 3 must fail the batch");
        let msg = panic_message(&*payload);
        assert!(msg.contains("run:skipped: 4 over a cap of 3"), "{msg}");
    }

    #[test]
    fn swarm_runner_requirement_failure_names_the_key() {
        let payload = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            run_universes_swarm_in(
                "charpente-sim",
                "swarm_meta.rs",
                4,
                &["never:happens"],
                &[],
                &[],
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
