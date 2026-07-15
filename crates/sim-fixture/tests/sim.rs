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
            let id = Id::<FixtureCounter>::from_raw(7);
            tx.put(&FixtureCounter { id, hits: 1 });
            tx.emit(NoteAdded { note: id.raw() });
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
    let snap = match db.snapshot() {
        Ok(snap) => snap,
        Err(e) if is_fault_fallout(&e.to_string()) => return Ok(None),
        Err(e) => return Err(e.to_string()),
    };
    let row = match snap.get::<FixtureCounter>(Id::from_raw(7)) {
        Ok(row) => row,
        Err(e) if is_fault_fallout(&e.to_string()) => return Ok(None),
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
        None if charpente_sim::checks::durable_corruption_fired(sim) => Ok(None),
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
