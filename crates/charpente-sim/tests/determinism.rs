//! Replay determinism is the entire verification story (DESIGN.md §1): the
//! same seed must produce the byte-identical universe, forever. If this test
//! ever fails, some ambient nondeterminism has leaked inside the boundary —
//! stop everything and find it.

use std::cell::Cell;
use std::io;
use std::time::Duration;

use charpente::auth::ApiKey;
use charpente::core::io::CLOCK_TOLERANCE;
use charpente::http::{ApiError, App, AppConfig, Method, Request};
use charpente::kernel::{CronRow, JobError};
use charpente::{Id, Schema, Tx, View, job, jobs, route, routes, table};
use charpente_core::io::{Disk, Entropy};
use charpente_sim::drive::Drive;
use charpente_sim::rng::Rng;
use charpente_sim::{FaultConfig, Sim, run_universes_swarm_with};

/// A fault-heavy workload; returns everything observable about the universe.
fn trace(seed: u64) -> (u64, u64, Vec<u8>) {
    let sim = Sim::new(seed);
    let disk = sim.disk();
    let entropy = sim.entropy();

    let mut observed = Vec::new();
    for round in 0..3u64 {
        sim.crash_disk_after(entropy.next_u64() % 32);
        if let Ok(file) = disk.open("t") {
            for i in 0..8u64 {
                let mut payload = vec![0u8; 700];
                entropy.fill(&mut payload);
                if file.write_at(round * 8_000 + i * 1_000, &payload).is_err() {
                    break;
                }
                if file.sync().is_err() {
                    break;
                }
            }
            if let Ok(len) = file.len() {
                observed.extend_from_slice(&len.to_le_bytes());
            }
        }
        sim.restart();
    }
    (sim.durable_fingerprint(), sim.now().as_nanos(), observed)
}

#[test]
fn same_seed_replays_identically() {
    for seed in [0x1, 0xDEAD_BEEF, 0x0123_4567_89AB_CDEF] {
        assert_eq!(
            trace(seed),
            trace(seed),
            "seed {seed:#x} did not replay identically"
        );
    }
}

/// A sim model is written against the outbound call types, which the
/// simulator re-exports so a test names them without a `charpente` import.
#[test]
fn outbound_call_types_are_reexported() {
    use charpente_sim::{OutboundCall, OutboundReply};
    let _: fn(&OutboundCall) -> OutboundReply = |_| OutboundReply {
        status: 200,
        headers: Vec::new(),
        body: Vec::new(),
    };
}

// ------------------------------------------------- tx.now() (0.5.0 spec §5 A)

/// One reading of `tx.now()`, numbered in commit order: the id is the count of
/// rows before it, so a route's and a job's readings share one sequence.
#[table(global)]
struct Probe {
    id: Id<Probe>,
    at: u64,
}

fn record(tx: &mut Tx) -> io::Result<()> {
    let n = tx.scan::<Probe>()?.len() as u64;
    tx.put(&Probe {
        id: Id::from_raw(n + 1),
        at: tx.now().as_nanos(),
    });
    Ok(())
}

#[route(POST "/probe", public)]
fn probe_route(tx: &mut Tx) -> Result<(), ApiError> {
    record(tx).map_err(|e| ApiError::Internal(e.to_string()))
}

/// What the one-second schedule enqueues: a reading from a job's step.
#[job(global)]
struct Beat {}

impl Beat {
    fn run(self, tx: &mut Tx) -> Result<(), JobError> {
        record(tx).map_err(|e| JobError::app(e.to_string()))
    }
}

/// The store as a boot recovered it, before its kernel ran: the newest probe
/// id (the boundary between processes) and the newest time the store
/// recorded, read from the rows the open-time floor reads.
#[derive(Clone, Copy, Default)]
struct AtBoot {
    last_probe: u64,
    floor: u64,
}

fn probe_app(sim: &Sim, boot: &Cell<AtBoot>) -> io::Result<App> {
    let app = App::open(
        Box::new(sim.disk()),
        Schema::new().table::<Probe>(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )?
    .mount(routes![probe_route])
    .mount_jobs(jobs![Beat])
    .schedule("beat", Duration::from_secs(1), || Beat {});
    let snap = app.db().snapshot()?;
    let keys = snap.scan::<ApiKey>()?.into_iter().map(|k| k.created_at);
    let ticks = snap.scan::<CronRow>()?.into_iter().map(|r| {
        r.next_due
            .saturating_sub(Duration::from_nanos(r.interval_nanos))
    });
    boot.set(AtBoot {
        last_probe: snap.scan::<Probe>()?.len() as u64,
        floor: keys.chain(ticks).map(|t| t.as_nanos()).max().unwrap_or(0),
    });
    Ok(app)
}

/// The faulted process's readings, the tail's, and the tail boot's floor.
type Readings = (Vec<u64>, Vec<u64>, u64);

/// Every reading the universe durably recorded, in commit order, split at the
/// final boot: the faulted process's, then the tail's, and the floor the tail's
/// boot recovered. `None` when the run gave up.
fn probe_readings(sim: &Sim) -> Result<Option<Readings>, String> {
    let boot = Cell::new(AtBoot::default());
    let drive = Drive::new(sim, |s| probe_app(s, &boot));
    let requests: Vec<(Duration, Request)> = (0..20u64)
        .map(|i| {
            (
                Duration::from_millis(i * 500),
                Request::new(Method::Post, "/probe"),
            )
        })
        .collect();
    let phase = drive.faulted_phase(&requests, Duration::from_secs(12))?;
    let Some(recovered) = drive.recover(&phase, Duration::from_secs(10))? else {
        return Ok(None);
    };
    let at_tail_boot = boot.get();
    let rows = recovered
        .kernel
        .app()
        .db()
        .snapshot()
        .map_err(|e| e.to_string())?
        .scan::<Probe>()
        .map_err(|e| e.to_string())?;
    assert!(
        rows.iter().zip(1u64..).all(|(r, n)| r.id.raw() == n),
        "contract: the probe ids are the commit sequence 1, 2, 3, …"
    );
    let cut = at_tail_boot.last_probe as usize;
    let phase_readings = rows[..cut].iter().map(|r| r.at).collect();
    let tail_readings = rows[cut..].iter().map(|r| r.at).collect();
    Ok(Some((phase_readings, tail_readings, at_tail_boot.floor)))
}

/// Turns the three clock axes on for every universe, so some universe skews,
/// steps back, or sleeps through a downtime. Sizes span both sides of the
/// tolerance, as `sample_clock_fields` draws them.
fn clock_axes_on(seed: u64, cfg: &mut FaultConfig) {
    let mut r = Rng::new(seed ^ 0x7A_C10C);
    cfg.clock_skew = r.between(0, 120_000_000_000) as i64;
    cfg.clock_step_back = (0.01, Duration::from_secs(120));
    cfg.clock_downtime = (0.5, Duration::from_secs(3 * 86_400));
}

/// 256 universes reach the downtime axis in well over 100 of them (the floor
/// below is measured; downtime rolls at 0.5 on each power cycle).
const TX_NOW_UNIVERSES: u64 = 256;
const TX_NOW_DOWNTIME_FLOOR: u64 = 100;

#[test]
fn tx_now_is_seed_stable_and_never_runs_backwards_within_a_process() {
    run_universes_swarm_with!(
        0x7A_0001,
        TX_NOW_UNIVERSES,
        clock_axes_on,
        |sim| {
            let Some((faulted, tail, floor)) = probe_readings(sim)? else {
                return Ok(());
            };
            for (name, readings) in [("faulted process", &faulted), ("tail process", &tail)] {
                if let Some(w) = readings.windows(2).find(|w| w[1] < w[0]) {
                    return Err(format!(
                        "tx.now() ran backwards within the {name}: read {} after {}",
                        w[1], w[0]
                    ));
                }
            }
            if let (Some(&last), Some(&first)) = (faulted.last(), tail.first()) {
                sim.cover("tx_now:restart-compared");
                let bound = last.saturating_sub(floor) + CLOCK_TOLERANCE.as_nanos() as u64;
                if last.saturating_sub(first) > bound {
                    return Err(format!(
                        "tx.now() after a restart read {first}, behind the previous process's \
                         last reading {last} by {} ns; the bound is {bound} ns (that reading \
                         less the store's recorded floor {floor}, plus CLOCK_TOLERANCE)",
                        last - first
                    ));
                }
            }
            Ok(())
        },
        require: ["clock:downtime", "tx_now:restart-compared"],
        floor: [("clock:downtime", TX_NOW_DOWNTIME_FLOOR)]
    );
}

/// Two runs of one seed record the same readings, clock axes on: the time a
/// transaction holds is a function of the seed.
#[test]
fn tx_now_readings_replay_identically() {
    // 0x7A_0001's universes, plus two more: every one boots through the clock axes.
    for seed in [0x7A_0001u64, 0x7A_0002, 0x7A_0003] {
        let run = || {
            let mut cfg = FaultConfig::none();
            clock_axes_on(seed, &mut cfg);
            let sim = Sim::with_faults(seed, cfg);
            probe_readings(&sim)
        };
        assert_eq!(run(), run(), "seed {seed:#x} recorded different readings");
    }
}
