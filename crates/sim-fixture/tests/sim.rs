//! The app-side convention, exercised end-to-end (spec §App-side
//! convention). Also the meta-test's target: `sometimes_fails` is gated
//! behind CHARPENTE_FIXTURE_FAIL so the workspace suite stays green.

use charpente::core::io::Entropy;
use charpente::{Db, DbConfig, Id, Schema, View};
use charpente_sim::Sim;
use sim_fixture::{FixtureCounter, NoteAdded};

charpente_sim::sim_tests! {
    counter_roundtrip require ["restart"],
    fixture_replays,
    acked_counter_survives_rounds require ["restart"],
    sometimes_fails,
}

fn counter_roundtrip(sim: &mut Sim) -> Result<(), String> {
    counter_roundtrip_result(sim).map(|_| ())
}

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
    use charpente_sim::checks::is_fault_fallout;

    let schema = || Schema::new().table::<FixtureCounter>();
    let mut db = match Db::open(Box::new(sim.disk()), DbConfig::default(), schema()) {
        Ok(db) => db,
        Err(e) if is_fault_fallout(&e.to_string()) => {
            sim.give_up(format!("Db::open before the crash hit fault fallout: {e}"));
            return Ok(None);
        }
        Err(e) => return Err(e.to_string()),
    };
    let acked = db
        .transact(|tx| {
            let id = Id::<FixtureCounter>::from_raw(7);
            tx.put(&FixtureCounter { id, hits: 1 });
            tx.emit(NoteAdded { note: id });
            Ok::<_, std::io::Error>(())
        })
        .is_ok();
    sim.restart();
    drop(db);
    let db = match Db::open(Box::new(sim.disk()), DbConfig::default(), schema()) {
        Ok(db) => db,
        Err(e) if is_fault_fallout(&e.to_string()) => {
            sim.give_up(format!("Db::open after the crash hit fault fallout: {e}"));
            return Ok(None);
        }
        Err(e) => return Err(e.to_string()),
    };
    if !acked {
        sim.give_up("the write was never acked, so durability has nothing to prove");
        return Ok(None);
    }
    let snap = match db.snapshot() {
        Ok(snap) => snap,
        Err(e) if is_fault_fallout(&e.to_string()) => {
            sim.give_up(format!("snapshot hit fault fallout: {e}"));
            return Ok(None);
        }
        Err(e) => return Err(e.to_string()),
    };
    let row = match snap.get::<FixtureCounter>(Id::from_raw(7)) {
        Ok(row) => row,
        Err(e) if is_fault_fallout(&e.to_string()) => {
            sim.give_up(format!("the read-back hit fault fallout: {e}"));
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
            sim.give_up("durable corruption fired, so the acked row is excused");
            Ok(None)
        }
        None => Err("acked FixtureCounter row lost across a crash".into()),
    }
}

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
        acked_implies_durable, durable_corruption_fired, is_fault_fallout,
    };

    let mut round: u64 = 0;
    let drive = move |sim: &mut Sim| -> Result<Vec<String>, String> {
        round += 1;
        let schema = Schema::new().table::<FixtureCounter>();
        let mut db = match Db::open(Box::new(sim.disk()), DbConfig::default(), schema) {
            Ok(db) => db,
            Err(e) if is_fault_fallout(&e.to_string()) => return Ok(vec![]),
            Err(e) => return Err(e.to_string()),
        };
        let acked = db
            .transact(|tx| {
                tx.put(&FixtureCounter {
                    id: Id::<FixtureCounter>::from_raw(round),
                    hits: round,
                });
                Ok::<_, std::io::Error>(())
            })
            .is_ok();
        Ok(if acked {
            vec![round.to_string()]
        } else {
            vec![]
        })
    };

    let verify = |sim: &mut Sim, acked: &[String]| -> Result<(), String> {
        let schema = Schema::new().table::<FixtureCounter>();
        let db = match Db::open(Box::new(sim.disk()), DbConfig::default(), schema) {
            Ok(db) => db,
            Err(e) if is_fault_fallout(&e.to_string()) => return Ok(()),
            Err(e) => return Err(e.to_string()),
        };
        let snap = match db.snapshot() {
            Ok(snap) => snap,
            Err(e) if is_fault_fallout(&e.to_string()) => return Ok(()),
            Err(e) => return Err(e.to_string()),
        };
        for id in acked {
            let raw: u64 = id.parse().expect("ack ids are decimal round numbers");
            match snap.get::<FixtureCounter>(Id::from_raw(raw)) {
                Ok(Some(_)) => {}
                // Same excuse rule as `counter_roundtrip_result`: an acked row
                // may vanish ONLY where durable corruption actually fired. That
                // helper reads `sim.coverage()`, which is the second reason this
                // checker must run on the caller's world — asked of a private
                // sim it would answer about faults nobody injected.
                Ok(None) if durable_corruption_fired(sim) => {}
                Ok(None) => return Err(format!("acked round {raw} lost across a restart")),
                Err(e) if is_fault_fallout(&e.to_string()) => return Ok(()),
                Err(e) => return Err(e.to_string()),
            }
        }
        Ok(())
    };

    acked_implies_durable(sim, 3, drive, verify)
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
