//! The app-side convention, exercised end-to-end (spec §App-side
//! convention). Also the meta-test's target: `sometimes_fails` is gated
//! behind CHARPENTE_FIXTURE_FAIL so the workspace suite stays green.

use charpente::core::io::Entropy;
use charpente::{Db, DbConfig, Id, Schema, View};
use charpente_sim::Sim;
use sim_fixture::FixtureCounter;

charpente_sim::sim_tests! {
    counter_roundtrip require ["restart"],
    fixture_replays,
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
/// nothing to assert (write not acked, or either open hit fault fallout —
/// the harness's own fault paths, not the app's contract, are under test
/// here). `counter_roundtrip` collapses this to pass/fail;
/// `fixture_replays` additionally demands the `Option<u64>` itself replay
/// identically across two independent worlds on the same seed.
fn counter_roundtrip_result(sim: &mut Sim) -> Result<Option<u64>, String> {
    use charpente_sim::checks::is_fault_fallout;

    let schema = || Schema::new().table::<FixtureCounter>();
    let mut db = match Db::open(Box::new(sim.disk()), DbConfig::default(), schema()) {
        Ok(db) => db,
        Err(e) if is_fault_fallout(&e.to_string()) => return Ok(None),
        Err(e) => return Err(e.to_string()),
    };
    let acked = db
        .transact(|tx| {
            tx.put(&FixtureCounter {
                id: Id::from_raw(7),
                hits: 1,
            });
            Ok::<_, std::io::Error>(())
        })
        .is_ok();
    sim.restart();
    drop(db);
    let db = match Db::open(Box::new(sim.disk()), DbConfig::default(), schema()) {
        Ok(db) => db,
        Err(e) if is_fault_fallout(&e.to_string()) => return Ok(None),
        Err(e) => return Err(e.to_string()),
    };
    if !acked {
        return Ok(None);
    }
    let snap = db.snapshot().map_err(|e| e.to_string())?;
    let row = snap
        .get::<FixtureCounter>(Id::from_raw(7))
        .map_err(|e| e.to_string())?;
    match row {
        Some(row) => Ok(Some(row.hits)),
        None => Err("acked FixtureCounter row lost across a crash".into()),
    }
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
