//! The app-side convention, exercised end-to-end (spec §App-side
//! convention). Also the meta-test's target: `sometimes_fails` is gated
//! behind CHARPENTE_FIXTURE_FAIL so the workspace suite stays green.

use charpente::core::io::{Entropy, Instant, WallTime};
use charpente::http::{App, AppConfig};
use charpente::kernel::{Dispatcher, Now};
use charpente::{Db, DbConfig, Id, Schema, TxError, View, jobs};
use charpente_sim::Sim;
use sim_fixture::{BumpCounter, FixtureCounter, NoteAdded};

charpente_sim::sim_tests! {
    counter_roundtrip require ["restart"],
    fixture_replays,
    acked_counter_survives_rounds require ["restart", "fixture:acked-rows-checked"],
    counter_bump_job_changes_the_row,
    counter_bump_job_leaves_the_row_readable,
    sometimes_fails,
}

fn counter_roundtrip(sim: &mut Sim) -> Result<(), String> {
    counter_roundtrip_result(sim).map(|_| ())
}

/// How many boots one `Db::open` or `App::open` here gets, each failed boot
/// followed by a restart, as a supervised process restarts
/// (`checks::boot_through_fallout`).
const BOOT_TRIES: u32 = 8;

/// Wires `replays_identically` to a real app roundtrip: two fresh worlds on
/// the same seed must retrace `counter_roundtrip_result` byte-for-byte. Uses
/// `sim.seed()` only — the restarts happen inside the two inner `Sim`s the
/// checker constructs, not on this test's own outer `sim`, so this test
/// carries no `require` (nothing here marks the outer sim's coverage).
fn fixture_replays(sim: &mut Sim) -> Result<(), String> {
    charpente_sim::checks::replays_identically(sim.seed(), counter_roundtrip_result)
}

/// Puts a row, crashes, reopens: an acked commit must survive. Returns the
/// row's `hits` value read back after restart, or `None` when there's
/// nothing to assert (write not acked, or an open/read hit fault fallout —
/// the harness's own fault paths, not the app's contract, are under test
/// here). `counter_roundtrip` collapses this to pass/fail;
/// `fixture_replays` additionally demands the `Option<u64>` itself replay
/// identically across two independent worlds on the same seed.
///
/// Every `Ok(None)` here is a universe that adjudicated NOTHING — the faults
/// stopped it before the claim could be tested. Each one calls `sim.give_up`,
/// so the batch report states how many universes actually reached a verdict
/// instead of counting these as passes. (`fixture_replays` drives this same
/// function on two private `Sim`s of its own, so the give-ups it records land
/// on those worlds, not on the harness's.)
fn counter_roundtrip_result(sim: &mut Sim) -> Result<Option<u64>, String> {
    use charpente_sim::checks::{boot_through_fallout, is_fault_fallout};

    let schema = || Schema::new().table::<FixtureCounter>();
    let open = || Db::open(Box::new(sim.disk()), DbConfig::default(), schema());
    let Some(mut db) =
        boot_through_fallout(sim, BOOT_TRIES, is_fault_fallout, open).map_err(|e| e.to_string())?
    else {
        sim.give_up(
            "Db::open before the crash never booted",
            format!("in {BOOT_TRIES} boots"),
        );
        return Ok(None);
    };
    // Only an injected fault excuses a write that never acked; any other
    // error is the platform's, and fails.
    let acked = match db.transact(|tx| {
        let id = Id::<FixtureCounter>::from_raw(7);
        tx.put(&FixtureCounter { id, hits: 1 });
        tx.emit(NoteAdded { note: id });
        Ok::<_, std::io::Error>(())
    }) {
        Ok(()) => true,
        Err(TxError::Io(e)) if is_fault_fallout(&e.to_string()) => false,
        Err(e) => return Err(format!("{e:?}")),
    };
    sim.restart();
    drop(db);
    let open = || Db::open(Box::new(sim.disk()), DbConfig::default(), schema());
    let Some(db) =
        boot_through_fallout(sim, BOOT_TRIES, is_fault_fallout, open).map_err(|e| e.to_string())?
    else {
        sim.give_up(
            "Db::open after the crash never booted",
            format!("in {BOOT_TRIES} boots"),
        );
        return Ok(None);
    };
    if !acked {
        sim.give_up(
            "the write was never acked, so durability has nothing to prove",
            "",
        );
        return Ok(None);
    }
    let snap = match db.snapshot() {
        Ok(snap) => snap,
        Err(e) if is_fault_fallout(&e.to_string()) => {
            sim.give_up("snapshot hit fault fallout", e);
            return Ok(None);
        }
        Err(e) => return Err(e.to_string()),
    };
    let row = match snap.get::<FixtureCounter>(Id::from_raw(7)) {
        Ok(row) => row,
        Err(e) if is_fault_fallout(&e.to_string()) => {
            sim.give_up("the read-back hit fault fallout", e);
            return Ok(None);
        }
        Err(e) => return Err(e.to_string()),
    };
    match row {
        Some(row) => Ok(Some(row.hits)),
        // Acked-but-lost is excused ONLY when the harness actually injected
        // durable corruption in this universe (bit_flip/misdirected_write on
        // a WAL file): corruption of an acked record above the checkpoint
        // horizon is silently cut at recovery, by the WAL convention — see
        // `durable_corruption_fired`'s docs. In every universe where no
        // corruption fired, this stays the hard check it always was.
        None if charpente_sim::checks::durable_corruption_fired(sim) => {
            sim.give_up("durable corruption fired, so the acked row is excused", "");
            Ok(None)
        }
        None => Err("acked FixtureCounter row lost across a crash".into()),
    }
}

/// Rounds `acked_counter_survives_rounds` runs.
const ROUNDS: u32 = 3;

/// `acked_implies_durable` against a real app, and the end-to-end proof that it
/// credits coverage on the world the HARNESS is watching.
///
/// This workload carries `require ["restart"]` and never calls `sim.restart()`
/// itself — the only thing that can mark that key here is the checker's own
/// restart between rounds. So the `require` IS the assertion: if the checker
/// ever goes back to building a private `Sim`, this test fails by name instead
/// of passing while testing a world nobody reads.
///
/// Contrast `fixture_replays` above, which correctly carries NO require: that
/// checker genuinely does its work in two worlds of its own, because determinism
/// needs a pair identical at time zero. Its doc comment had the mechanism right
/// all along; nothing had applied the same reasoning to this checker, which has
/// the opposite requirement and took a seed anyway until 2026-09-05.
fn acked_counter_survives_rounds(sim: &mut Sim) -> Result<(), String> {
    use charpente_sim::checks::{
        acked_implies_durable, boot_through_fallout, durable_corruption_fired, is_fault_fallout,
    };

    let mut round: u64 = 0;
    // `acked_implies_durable` runs every round even after `verify` gives up,
    // so both closures end the universe there: a round after a give-up would
    // adjudicate nothing (`Sim::give_up`'s doc names this checker).
    let drive = move |sim: &mut Sim| -> Result<Vec<String>, String> {
        if sim.gave_up().is_some() {
            return Ok(vec![]);
        }
        round += 1;
        let open = || {
            let schema = Schema::new().table::<FixtureCounter>();
            Db::open(Box::new(sim.disk()), DbConfig::default(), schema)
        };
        // A round whose boots run out acks nothing, as one failed boot did.
        let Some(mut db) = boot_through_fallout(sim, BOOT_TRIES, is_fault_fallout, open)
            .map_err(|e| e.to_string())?
        else {
            return Ok(vec![]);
        };
        let acked = match db.transact(|tx| {
            tx.put(&FixtureCounter {
                id: Id::<FixtureCounter>::from_raw(round),
                hits: round,
            });
            Ok::<_, std::io::Error>(())
        }) {
            Ok(()) => true,
            Err(TxError::Io(e)) if is_fault_fallout(&e.to_string()) => false,
            Err(e) => return Err(format!("{e:?}")),
        };
        Ok(if acked {
            vec![round.to_string()]
        } else {
            vec![]
        })
    };

    // A round's check that meets fault fallout is skipped. While a later round
    // remains that loses nothing: `acked` is cumulative, so the next round's
    // `verify` checks every row this one would have. A skip in the last round
    // leaves a verdict only if an earlier round checked acked rows; otherwise
    // the universe checked nothing and gives up (S3b item 5: every skip used
    // to pass).
    let mut verified_rounds = 0u32;
    let mut checked_any = false;
    let verify = move |sim: &mut Sim, acked: &[String]| -> Result<(), String> {
        verified_rounds += 1;
        let skip = |sim: &mut Sim, what: String| {
            if verified_rounds == ROUNDS && !checked_any {
                sim.cover("fixture:last-verify-fallout");
                sim.give_up(
                    "the last round's check was skipped, and no earlier round checked an acked row",
                    what,
                );
            }
            Ok(())
        };
        if sim.gave_up().is_some() {
            return Ok(());
        }
        let open = || {
            let schema = Schema::new().table::<FixtureCounter>();
            Db::open(Box::new(sim.disk()), DbConfig::default(), schema)
        };
        let Some(db) = boot_through_fallout(sim, BOOT_TRIES, is_fault_fallout, open)
            .map_err(|e| e.to_string())?
        else {
            return skip(sim, format!("never booted in {BOOT_TRIES} boots"));
        };
        let snap = match db.snapshot() {
            Ok(snap) => snap,
            Err(e) if is_fault_fallout(&e.to_string()) => {
                return skip(sim, format!("hit fault fallout taking a snapshot: {e}"));
            }
            Err(e) => return Err(e.to_string()),
        };
        for id in acked {
            let raw: u64 = id.parse().expect("ack ids are decimal round numbers");
            match snap.get::<FixtureCounter>(Id::from_raw(raw)) {
                Ok(Some(_)) => {}
                // Same excuse rule as `counter_roundtrip_result`: an acked row
                // may vanish ONLY where durable corruption actually fired, and
                // the universe then gives up, since durability was not
                // adjudicated. That helper reads `sim.coverage()`, which is the
                // second reason this checker must run on the caller's world —
                // asked of a private sim it would answer about faults nobody
                // injected.
                Ok(None) if durable_corruption_fired(sim) => {
                    sim.cover("fixture:acked-round-cut");
                    sim.give_up(
                        "durable corruption cut an acked round, so durability was not adjudicated",
                        format!("round {raw}"),
                    );
                    return Ok(());
                }
                Ok(None) => return Err(format!("acked round {raw} lost across a restart")),
                Err(e) if is_fault_fallout(&e.to_string()) => {
                    return skip(sim, format!("hit fault fallout reading round {raw}: {e}"));
                }
                Err(e) => return Err(e.to_string()),
            }
        }
        if !acked.is_empty() {
            sim.cover("fixture:acked-rows-checked");
        }
        checked_any |= !acked.is_empty();
        Ok(())
    };

    acked_implies_durable(sim, ROUNDS, drive, verify)
}

fn sometimes_fails(sim: &mut Sim) -> Result<(), String> {
    if std::env::var_os("CHARPENTE_FIXTURE_FAIL").is_none() {
        return Ok(());
    }
    let mut byte = [0u8; 1];
    sim.entropy().fill(&mut byte);
    if byte[0] % 5 == 0 {
        Err(format!(
            "fixture failure: entropy byte {} ≡ 0 (mod 5)",
            byte[0]
        ))
    } else {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// The one-sided-property demonstration (oracle-density plan, Task 10).
//
// These two properties are DELIBERATE FIXTURES. They exercise the same job the
// same way and differ ONLY in what they assert, which is the whole point: with
// `BumpCounter::run`'s body mutated to `Ok(())` — the job dispatched, reported
// success, and did nothing — one of them goes red and the other stays green.
//
// Do not "fix" `counter_bump_job_leaves_the_row_readable`. Its weakness is the
// subject under test. Do not merge them, and do not delete either: the pair is
// the acceptance test for `charpente test --mutants`, and docs/GUIDE.md walks
// through running it.
// ---------------------------------------------------------------------------

/// Drives the fixture's one job end to end: open an app with `BumpCounter`
/// mounted, put a row at `hits = 1`, enqueue the job, dispatch it once, and
/// read the row back. Returns the row's `hits` afterwards.
///
/// `Ok(None)` is a universe that adjudicated NOTHING — injected faults stopped
/// it before the claim could be observed — and every one of them calls
/// `sim.give_up`, exactly as `counter_roundtrip_result` does, so these
/// universes are reported rather than counted as passes.
fn dispatch_one_bump(sim: &mut Sim) -> Result<Option<u64>, String> {
    use charpente_sim::checks::{boot_through_fallout, is_fault_fallout};

    macro_rules! fallible {
        ($what:expr, $e:expr) => {
            match $e {
                Ok(v) => v,
                Err(e) if is_fault_fallout(&e.to_string()) => {
                    sim.give_up(concat!($what, " hit fault fallout"), e);
                    return Ok(None);
                }
                Err(e) => return Err(e.to_string()),
            }
        };
    }

    let id = Id::<FixtureCounter>::from_raw(1);
    let open = || {
        App::open(
            Box::new(sim.disk()),
            Schema::new().table::<FixtureCounter>(),
            Box::new(sim.entropy()),
            AppConfig::default(),
        )
    };
    let Some(app) =
        boot_through_fallout(sim, BOOT_TRIES, is_fault_fallout, open).map_err(|e| e.to_string())?
    else {
        sim.give_up("App::open never booted", format!("in {BOOT_TRIES} boots"));
        return Ok(None);
    };
    let mut app = app.mount_jobs(jobs![BumpCounter]);
    let mut dispatcher = Dispatcher::new(app.take_job_defs_for_tests());
    let db = app.db_for_tests();

    // A transaction an injected fault kept from committing was never
    // acknowledged, so there is no claim to test in this universe. Any other
    // error is the platform's, and fails, as this file's other properties do.
    match db.transact(|tx| {
        tx.put(&FixtureCounter { id, hits: 1 });
        tx.enqueue(BumpCounter { note: id });
        Ok::<(), std::io::Error>(())
    }) {
        Ok(()) => {}
        Err(TxError::Io(e)) if is_fault_fallout(&e.to_string()) => {
            sim.give_up(
                "the seeding transaction never committed, so no job was ever enqueued",
                "",
            );
            return Ok(None);
        }
        Err(e) => return Err(format!("{e:?}")),
    }

    dispatcher.note_enqueued(db.drain_enqueued());
    let now = Now {
        mono: Instant::ZERO,
        wall: WallTime::UNIX_EPOCH,
    };
    fallible!("dispatching the job", dispatcher.run_due(db, now));

    let snap = fallible!("the snapshot after dispatch", db.snapshot());
    match fallible!("reading the row back", snap.get::<FixtureCounter>(id)) {
        Some(row) => Ok(Some(row.hits)),
        None => Err("the row the job was enqueued against is gone".to_string()),
    }
}

/// **The honest half.** It asserts the job's actual EFFECT, so neutering
/// `BumpCounter::run` makes it fail by name. This is what a property that
/// earns its keep looks like.
fn counter_bump_job_changes_the_row(sim: &mut Sim) -> Result<(), String> {
    match dispatch_one_bump(sim)? {
        None => Ok(()),
        Some(2) => Ok(()),
        Some(hits) => Err(format!(
            "the job was dispatched and reported success, but the row still reads hits = {hits} \
             instead of 2 — the handler's effect never landed"
        )),
    }
}

/// **The one-sided half, wrong on purpose.** It asserts only that the row is
/// still readable after the dispatch, which is true whether the job did its
/// work or did nothing at all. It therefore passes against a neutered handler,
/// and no amount of running it produces evidence about the job.
///
/// This is the exact shape that shipped five times in the helpdesk app, where
/// deleting the entire SLA sweep loop left the suite green.
fn counter_bump_job_leaves_the_row_readable(sim: &mut Sim) -> Result<(), String> {
    match dispatch_one_bump(sim)? {
        // Deliberately ignores the value. That is the defect being demonstrated.
        None | Some(_) => Ok(()),
    }
}
