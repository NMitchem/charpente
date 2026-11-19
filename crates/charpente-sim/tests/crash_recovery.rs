//! The first crash-recovery property test (PLAN.md M0 step 1).
//!
//! A toy checksummed append-log runs under the full fault matrix: injected
//! write errors, failed syncs, and crashes landing between any two disk
//! operations, with unsynced writes torn or lost at restart. The durability
//! invariant: every acknowledged (synced) record is recovered intact, in
//! order; an unacknowledged tail may be dropped but must never surface
//! corrupt.
//!
//! The meta-test at the bottom is the point of the exercise: a deliberately
//! naive recovery that trusts the length prefix without verifying the
//! checksum MUST be caught by the harness — if it ever stops being caught,
//! the safety equipment itself has broken.
//!
//! `mod dead_stage`, at the bottom, is app-level: a crash at every disk op
//! of the step that commits an `on_dead` hook's Dead mark (0.5.0 §7.3 b).

use std::io;

use charpente_core::io::{Disk, File};
use charpente_sim::{Sim, run_universes, run_universes_from};

const LOG: &str = "wal/000001.log";

/// Found by `harness_catches_naive_recovery` on 2026-07-01 and pinned here:
/// under this seed a crash tears an unsynced record (subset-of-sectors tear)
/// and naive recovery returns record 6 corrupt — right length, wrong bytes.
/// Hard rule 3: a bug found in sim gets its seed recorded in the regression
/// suite.
const REGRESSION_SEED_TORN_WRITE: u64 = 0x8bee_1752_811a_e873;

/// Fixed base for the meta-tests so CI is deterministic; the positive
/// property test uses fresh seeds every run.
const META_TEST_BASE: u64 = 0xC4A2_9E17_0001;

fn crc32(data: &[u8]) -> u32 {
    let mut crc = !0u32;
    for &byte in data {
        crc ^= u32::from(byte);
        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (0xEDB8_8320 & mask);
        }
    }
    !crc
}

/// Frame: [payload len: u32 le][crc32(payload): u32 le][payload].
fn append(file: &dyn File, end: u64, payload: &[u8]) -> io::Result<u64> {
    let mut frame = Vec::with_capacity(8 + payload.len());
    frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    frame.extend_from_slice(&crc32(payload).to_le_bytes());
    frame.extend_from_slice(payload);
    file.write_at(end, &frame)?;
    file.sync()?;
    Ok(end + frame.len() as u64)
}

/// Scans frames from the start; stops at the first invalid one. `checked`
/// verifies the checksum; the naive variant trusts the length prefix — the
/// bug the harness must catch.
fn recover(disk: &dyn Disk, checked: bool) -> io::Result<(Vec<Vec<u8>>, u64)> {
    let file = disk.open(LOG)?;
    let len = file.len()?;
    let mut records = Vec::new();
    let mut pos = 0u64;
    loop {
        if pos + 8 > len {
            break;
        }
        let mut header = [0u8; 8];
        file.read_at(pos, &mut header)?;
        let payload_len = u64::from(u32::from_le_bytes(header[0..4].try_into().unwrap()));
        let crc = u32::from_le_bytes(header[4..8].try_into().unwrap());
        if payload_len == 0 || pos + 8 + payload_len > len {
            break;
        }
        let mut payload = vec![0u8; payload_len as usize];
        file.read_at(pos + 8, &mut payload)?;
        if checked && crc32(&payload) != crc {
            break; // torn or corrupt tail — discard from here
        }
        records.push(payload);
        pos += 8 + payload_len;
    }
    Ok((records, pos))
}

fn check(recovered: &[Vec<u8>], appended: &[Vec<u8>], acked: usize) -> Result<(), String> {
    if recovered.len() < acked {
        return Err(format!(
            "durability violated: {acked} acknowledged records but only {} recovered — an acked write was lost",
            recovered.len()
        ));
    }
    if recovered.len() > appended.len() {
        return Err(format!(
            "recovery invented records: {} recovered but only {} were ever appended",
            recovered.len(),
            appended.len()
        ));
    }
    for (i, (got, want)) in recovered.iter().zip(appended).enumerate() {
        if got != want {
            return Err(format!(
                "record {i} corrupt after recovery: a torn write surfaced ({} bytes recovered, {} appended)",
                got.len(),
                want.len()
            ));
        }
    }
    Ok(())
}

/// One universe: rounds of recover → check → append-until-death → restart,
/// with the crash point, workload sizes, and payloads all drawn from the
/// sim's seeded entropy. Any IO error is treated as the process dying there.
fn crash_workload(sim: &mut Sim, checked: bool) -> Result<(), String> {
    let disk = sim.disk();
    let entropy = sim.entropy();
    use charpente_core::io::Entropy;

    let mut appended: Vec<Vec<u8>> = Vec::new();
    let mut acked = 0usize;

    for _round in 0..4 {
        // The crash lands somewhere in this round's ops — including during
        // recovery itself.
        sim.crash_disk_after(entropy.next_u64() % 64);

        let (recovered, end) = match recover(&disk, checked) {
            Ok(result) => result,
            Err(_) => {
                sim.restart();
                continue; // died during recovery; the invariant is re-checked next round
            }
        };
        check(&recovered, &appended, acked)?;

        // Recovery defines the new truth: unacked survivors are history now.
        appended = recovered;
        acked = appended.len();

        let file = match disk.open(LOG) {
            Ok(file) => file,
            Err(_) => {
                sim.restart();
                continue;
            }
        };
        let mut end = end;
        let appends = 1 + entropy.next_u64() % 12;
        for _ in 0..appends {
            // Multi-sector payloads so torn writes bite mid-record.
            let len = 600 + (entropy.next_u64() % 2400) as usize;
            let mut payload = vec![0u8; len];
            entropy.fill(&mut payload);
            appended.push(payload.clone());
            match append(&*file, end, &payload) {
                Ok(new_end) => {
                    end = new_end;
                    acked += 1;
                }
                Err(_) => break, // the process died here (crash or EIO)
            }
        }
        sim.restart(); // power-cycle between rounds regardless
    }

    // Final quiet recovery: no crash is scheduled after the last restart.
    let (recovered, _) =
        recover(&disk, checked).map_err(|e| format!("final recovery failed: {e}"))?;
    check(&recovered, &appended, acked)
}

/// The positive property, over fresh seeds every run: checksummed recovery
/// upholds the durability invariant under the full fault matrix.
#[test]
fn checked_recovery_survives_the_fault_matrix() {
    run_universes!(512, |sim: &mut Sim| crash_workload(sim, true));
}

/// The meta-test: the harness MUST catch naive recovery. Runs the naive
/// workload across universes from a fixed base until a seed produces a
/// violation, then replays that exact seed to prove the repro contract.
#[test]
fn harness_catches_naive_recovery() {
    let mut state = META_TEST_BASE;
    let mut caught: Option<(u64, String)> = None;
    for _ in 0..400 {
        let seed = charpente_sim::rng::splitmix64(&mut state);
        let mut sim = Sim::new(seed);
        if let Err(msg) = crash_workload(&mut sim, false) {
            caught = Some((seed, msg));
            break;
        }
    }
    let (seed, msg) = caught.expect(
        "the harness no longer catches checksum-free recovery under torn writes — \
         the fault injection itself has regressed",
    );
    eprintln!("caught naive recovery: seed {seed:#018x}: {msg}");

    // The seed IS the reproduction: the same seed must fail the same way.
    let mut replay = Sim::new(seed);
    let replayed = crash_workload(&mut replay, false);
    assert_eq!(
        replayed,
        Err(msg),
        "replaying the seed must reproduce the identical failure"
    );
}

/// Hard rule 3: the recorded regression seed, replayed exactly.
#[test]
fn regression_torn_write_seed() {
    let mut sim = Sim::new(REGRESSION_SEED_TORN_WRITE);
    assert!(
        crash_workload(&mut sim, false).is_err(),
        "seed {REGRESSION_SEED_TORN_WRITE:#018x} no longer catches naive recovery"
    );
}

/// Hard rule 5: a sim failure's output IS the reproduction — it must name
/// the seed and the one-command repro.
#[test]
fn failure_output_names_seed_and_repro() {
    let result = std::panic::catch_unwind(|| {
        run_universes_from!(META_TEST_BASE, 400, |sim: &mut Sim| {
            crash_workload(sim, false)
        });
    });
    let payload = result.expect_err("naive recovery must fail under the meta-test base seed");
    let msg = payload
        .downcast_ref::<String>()
        .expect("panic payload should be the failure report");
    assert!(msg.contains("seed:  0x"), "missing seed line:\n{msg}");
    assert!(
        msg.contains("repro: CHARPENTE_SEED=0x") && msg.contains("--test crash_recovery"),
        "missing one-command repro:\n{msg}"
    );
}

/// The Dead stage under a crash at every disk op (0.5.0 spec §7.3 b). An
/// `on_dead` hook's writes and its job's Dead mark commit as one record, so
/// no crash point recovers to the hook's row without the mark, or to the mark
/// without the row, and a hook that fails still leaves the mark, alone. The
/// step swept is one `Dispatcher::run_due` over a job whose one attempt fails:
/// the attempt, the failure record that runs the hook, and the group commit
/// that makes them durable.
mod dead_stage {
    use std::time::Duration;

    use charpente::http::{App, AppConfig};
    use charpente::kernel::{Dispatcher, JobError, JobOutcome, Now};
    use charpente::{Db, DbConfig, Dead, Schema, Tx, job, jobs, table};
    use charpente_core::io::{Instant, WallTime};
    use charpente_core::job::{HOOK_FAILED, JobRow, JobStatus};
    use charpente_core::table::{Id, Snapshot, UnscopedJob, View};
    use charpente_sim::checks::dead_hooks_succeeded;
    use charpente_sim::{FaultConfig, Sim};

    const JOB_ERROR: &str = "the widget was upside down";
    const HOOK_ERROR: &str = "the hook could not tell anyone";

    /// Every disk op costs exactly this much virtual time, and nothing else
    /// in the step moves the sim's clock, so the time an uncrashed step takes
    /// counts its disk ops.
    const OP: Duration = Duration::from_micros(1);

    /// Each crash model is a `FaultConfig` with no fault but the armed crash,
    /// and says what a power cycle does to each write no sync has taken yet.
    fn models() -> [(&'static str, FaultConfig); 4] {
        let model = |crash_apply, crash_torn| FaultConfig {
            crash_apply,
            crash_torn,
            disk_latency: (OP, OP),
            ..FaultConfig::none()
        };
        [
            ("lost", model(0.0, 0.0)),
            ("applied", model(1.0, 0.0)),
            ("torn", model(0.0, 1.0)),
            ("mixed", model(0.4, 0.3)),
        ]
    }

    /// The seeds matter only to the crash model's fate for each unsynced
    /// write (applied, torn and where, or lost): the run is otherwise
    /// fault-free and its latency fixed. There are five, as
    /// `repl::witnesses`' torn-overwrite sweep has, so a torn write's cut
    /// lands at several offsets and the mixed model draws several fates.
    const SEEDS: [u64; 5] = [
        0xDEAD_5EED_0001,
        0xDEAD_5EED_0002,
        0xDEAD_5EED_0003,
        0xDEAD_5EED_0004,
        0xDEAD_5EED_0005,
    ];

    /// What a hook wrote.
    #[table(global)]
    struct Tombstone {
        id: Id<Tombstone>,
        attempts: u32,
    }

    /// Fails its one attempt; its hook writes the hook's row.
    #[job(global, on_dead, retry(max = 1))]
    struct Mourned {}

    impl Mourned {
        fn run(self, _tx: &mut Tx) -> Result<(), JobError> {
            Err(JobError::app(JOB_ERROR))
        }

        fn on_dead(self, tx: &mut Tx, dead: Dead<Self>) -> Result<(), JobError> {
            tx.put(&Tombstone {
                id: Id::from_raw(0),
                attempts: dead.attempts(),
            });
            Ok(())
        }
    }

    /// Fails its one attempt; its hook writes the hook's row, then fails.
    #[job(global, on_dead, retry(max = 1))]
    struct Hapless {}

    impl Hapless {
        fn run(self, _tx: &mut Tx) -> Result<(), JobError> {
            Err(JobError::app(JOB_ERROR))
        }

        fn on_dead(self, tx: &mut Tx, dead: Dead<Self>) -> Result<(), JobError> {
            tx.put(&Tombstone {
                id: Id::from_raw(0),
                attempts: dead.attempts(),
            });
            Err(JobError::app(HOOK_ERROR))
        }
    }

    /// The app, checkpointing after every commit. At the default cadence the
    /// Dead step's only disk ops are its record's WAL append and sync, so the
    /// commit point is the step's last op and no crash point lands after it.
    /// A checkpoint in the same step (the store's writes and syncs, the WAL
    /// segments it retires) puts disk ops after the record is durable.
    fn open_app(sim: &Sim) -> App {
        let config = AppConfig {
            db: DbConfig {
                checkpoint_every: 1,
                ..DbConfig::default()
            },
            ..AppConfig::default()
        };
        App::open(
            Box::new(sim.disk()),
            Schema::new().table::<Tombstone>(),
            Box::new(sim.entropy()),
            config,
        )
        .unwrap_or_else(|e| panic!("seed {:#x}: the app opens: {e}", sim.seed()))
        .mount_jobs(jobs![Mourned, Hapless])
    }

    fn enqueue(db: &mut Db, job: impl UnscopedJob) {
        db.transact(|tx| {
            tx.enqueue(job);
            Ok::<(), std::io::Error>(())
        })
        .unwrap();
    }

    fn at_zero() -> Now {
        Now {
            mono: Instant::from_nanos(0),
            wall: WallTime::from_nanos(0),
        }
    }

    /// The one job's row, and how many hook rows the store holds.
    fn read(snap: &Snapshot) -> (JobRow, usize) {
        let mut jobs = snap.scan::<JobRow>().unwrap();
        assert_eq!(jobs.len(), 1, "one job was enqueued, and nothing enqueues");
        (jobs.remove(0), snap.scan::<Tombstone>().unwrap().len())
    }

    /// A fresh universe whose store holds `enqueue`'s one job durably, and a
    /// dispatcher with it due.
    fn universe(seed: u64, cfg: &FaultConfig, enqueue: fn(&mut Db)) -> (Sim, App, Dispatcher) {
        let sim = Sim::with_faults(seed, cfg.clone());
        let mut app = open_app(&sim);
        let mut dispatcher = Dispatcher::new(app.take_job_defs_for_tests());
        let db = app.db_for_tests();
        enqueue(db);
        dispatcher.note_enqueued(db.drain_enqueued());
        (sim, app, dispatcher)
    }

    /// Sweeps a crash over every disk op of the `run_due` step that marks
    /// `enqueue`'s job Dead, under each crash model and seed. At each crash
    /// point the universe is power-cycled and reopened, and `judge` reads the
    /// recovered store and says whether the Dead mark committed. Recovery
    /// then goes on, so the job runs again where its mark did not commit, and
    /// `settled` reads the end.
    ///
    /// The op count is taken twice. An uncrashed run's virtual time gives
    /// it, and the sweep then proves it: a crash armed at op `n` fires inside
    /// the step for every `n` below the count and not at the count. So the
    /// sweep covers every op of the step. It also asserts crash points on
    /// both sides of the commit point, so a sweep that only ever crashed
    /// before the commit (or only after it) fails rather than passing.
    fn sweep(
        enqueue: fn(&mut Db),
        judge: impl Fn(&str, &Snapshot) -> bool,
        settled: impl Fn(&str, &Snapshot),
    ) {
        for (model, cfg) in models() {
            for seed in SEEDS {
                let ops = {
                    let (sim, mut app, mut dispatcher) = universe(seed, &cfg, enqueue);
                    let start = sim.now().as_nanos();
                    let outcome = dispatcher.run_due(app.db_for_tests(), at_zero()).unwrap();
                    assert!(
                        matches!(outcome, JobOutcome::Dead { id: 0, attempts: 1 }),
                        "{outcome:?}"
                    );
                    (sim.now().as_nanos() - start) / OP.as_nanos() as u64
                };
                let mut committed = Vec::new();
                for n in 0.. {
                    let at = format!(
                        "{model} crash model, seed {seed:#x}, crash at op {n} of the Dead \
                         step's {ops}"
                    );
                    let (sim, mut app, mut dispatcher) = universe(seed, &cfg, enqueue);
                    sim.crash_disk_after(n);
                    let _ = dispatcher.run_due(app.db_for_tests(), at_zero());
                    assert_eq!(
                        sim.disk_crashed(),
                        n < ops,
                        "{at}: a crash armed at op n fires inside the step exactly when the \
                         uncrashed step took more than n disk ops"
                    );
                    if n == ops {
                        break;
                    }
                    drop(dispatcher);
                    drop(app);
                    sim.restart();
                    let mut app = open_app(&sim);
                    let mut dispatcher = Dispatcher::new(app.take_job_defs_for_tests());
                    let db = app.db_for_tests();
                    {
                        let snap = db.snapshot().unwrap();
                        if judge(&at, &snap) {
                            committed.push(n);
                        }
                        dispatcher.rebuild(&snap).unwrap();
                    }
                    if dispatcher.has_due(WallTime::from_nanos(0)) {
                        let outcome = dispatcher.run_due(db, at_zero()).unwrap();
                        assert!(
                            matches!(outcome, JobOutcome::Dead { id: 0, attempts: 1 }),
                            "{at}: the recovered job runs again to its Dead mark: {outcome:?}"
                        );
                    }
                    settled(&at, &db.snapshot().unwrap());
                }
                let commit_at = committed.first().copied().unwrap_or(ops);
                assert!(
                    commit_at > 0 && commit_at < ops,
                    "{model} crash model, seed {seed:#x}: the sweep must land crash points on \
                     both sides of the commit point, but of the Dead step's {ops} disk ops, the \
                     crashes that recovered its Dead mark were {committed:?}"
                );
                assert_eq!(
                    committed,
                    (commit_at..ops).collect::<Vec<_>>(),
                    "{model} crash model, seed {seed:#x}: the step has one commit point: a crash \
                     before it recovers the job as enqueued, and one at or after it the Dead mark"
                );
                eprintln!(
                    "{model} crash model, seed {seed:#x}: the Dead step takes {ops} disk ops; a \
                     crash at ops 0..{commit_at} recovers the job as enqueued, at \
                     {commit_at}..{ops} its Dead mark"
                );
            }
        }
    }

    #[test]
    fn a_crash_anywhere_in_the_dead_stage_leaves_mark_and_effects_together() {
        sweep(
            |db| enqueue(db, Mourned {}),
            |at, snap| {
                let (row, hook_rows) = read(snap);
                let dead = row.status == JobStatus::Dead;
                assert_eq!(
                    hook_rows == 1,
                    dead,
                    "{at}: the hook's row exists exactly when the Dead mark does, but the job is \
                     {:?} at attempt {} with {hook_rows} hook row(s)",
                    row.status,
                    row.attempts
                );
                assert_eq!(dead_hooks_succeeded(snap), Ok(()), "{at}");
                let state = (row.status, row.attempts, row.last_error, hook_rows);
                if dead {
                    assert_eq!(
                        state,
                        (JobStatus::Dead, 1, Some(JOB_ERROR.to_string()), 1),
                        "{at}"
                    );
                } else {
                    assert_eq!(
                        state,
                        (JobStatus::Pending, 0, None, 0),
                        "{at}: a crash before the commit point leaves the job as enqueued"
                    );
                }
                dead
            },
            |at, snap| {
                let (row, hook_rows) = read(snap);
                assert_eq!(
                    (row.status, row.attempts, hook_rows),
                    (JobStatus::Dead, 1, 1),
                    "{at}: recovery ends with the Dead mark and the hook's one row"
                );
            },
        );
    }

    #[test]
    fn a_failing_hook_still_leaves_the_mark() {
        let mark_alone =
            format!("{HOOK_FAILED}{HOOK_ERROR} — the job's own last error: {JOB_ERROR}");
        sweep(
            |db| enqueue(db, Hapless {}),
            |at, snap| {
                let (row, hook_rows) = read(snap);
                let dead = row.status == JobStatus::Dead;
                assert_eq!(hook_rows, 0, "{at}: a failed hook's row never commits");
                assert_eq!(
                    dead_hooks_succeeded(snap).is_err(),
                    dead,
                    "{at}: the check fails exactly where the hook's failure committed"
                );
                let state = (row.status, row.attempts, row.last_error);
                if dead {
                    assert_eq!(
                        state,
                        (JobStatus::Dead, 1, Some(mark_alone.clone())),
                        "{at}"
                    );
                } else {
                    assert_eq!(
                        state,
                        (JobStatus::Pending, 0, None),
                        "{at}: a crash before the commit point leaves the job as enqueued"
                    );
                }
                dead
            },
            |at, snap| {
                let (row, hook_rows) = read(snap);
                assert_eq!(
                    (
                        row.status,
                        row.attempts,
                        row.last_error.as_deref(),
                        hook_rows
                    ),
                    (JobStatus::Dead, 1, Some(mark_alone.as_str()), 0),
                    "{at}: recovery ends with the Dead mark alone, carrying the prefix"
                );
                assert_eq!(
                    dead_hooks_succeeded(snap),
                    Err(
                        "dead_hooks_succeeded: job 0 (\"Hapless\") is Dead and its on_dead hook \
                         failed: the hook could not tell anyone — the job's own last error: the \
                         widget was upside down — the Dead mark committed alone, so what the hook \
                         was to do (tell someone, re-issue the effect) did not happen; fix the \
                         hook and replay this seed"
                            .to_string()
                    ),
                    "{at}"
                );
            },
        );
    }
}
