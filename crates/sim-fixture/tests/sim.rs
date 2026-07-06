//! The app-side convention, exercised end-to-end (spec §App-side
//! convention). Also the meta-test's target: `sometimes_fails` is gated
//! behind CHARPENTE_FIXTURE_FAIL so the workspace suite stays green.

use charpente::{Db, DbConfig, Id, Schema, View};
use charpente_core::io::Entropy;
use charpente_sim::Sim;
use sim_fixture::FixtureCounter;

charpente_sim::sim_tests! {
    counter_roundtrip require ["restart"],
    sometimes_fails,
}

fn counter_roundtrip(sim: &mut Sim) -> Result<(), String> {
    // Put a row, crash, reopen: an acked commit must survive. Faults are
    // expected in swarm universes — classified fallout is a pass for THIS
    // fixture (the harness, not the app, is under test here).
    use charpente_sim::checks::is_fault_fallout;
    let fallout_ok = |e: String| if is_fault_fallout(&e) { Ok(()) } else { Err(e) };

    let schema = || Schema::new().table::<FixtureCounter>();
    let mut db = match Db::open(Box::new(sim.disk()), DbConfig::default(), schema()) {
        Ok(db) => db,
        Err(e) => return fallout_ok(e.to_string()),
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
        Err(e) => return fallout_ok(e.to_string()),
    };
    if acked {
        let snap = db.snapshot().map_err(|e| e.to_string())?;
        let row = snap
            .get::<FixtureCounter>(Id::from_raw(7))
            .map_err(|e| e.to_string())?;
        if row.is_none() {
            return Err("acked FixtureCounter row lost across a crash".into());
        }
    }
    Ok(())
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
