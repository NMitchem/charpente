//! `seed_foreign_job`: a tiny standalone seeding process for `edge-harness`'s
//! process-level oracle that a kernel stop under `charpente::launch` actually
//! ends the process (`tests/smoke.rs`). Opens the target data dir directly
//! through `charpente_core::db::Db::open` (no `App`, no `db_for_tests` — the
//! lowest-level public entry point that can write a real row) with an
//! otherwise-empty schema, enqueues one Pending job under the real
//! `SendReceipt` name but a hand-rolled shape with a different fingerprint,
//! then exits — so the next process to open this data dir with the REAL
//! `invoicing::build_app` (which mounts the real `SendReceipt`) finds a
//! foreign-shape Pending row and its kernel refuses to start (spec §5).
//! Writes only the platform jobs table; `invoicing::build_app`'s own tables
//! are all still unwritten (adoption, not a schema conflict) when the real
//! binary opens next.
//!
//! This is the one place in `edge-harness` that links `charpente` at all
//! (`../lib.rs`'s module doc) — a single opt-in bin, built and driven exactly
//! like the stock binaries (`invoicing`, `node`, `charpente`) this crate
//! spawns as external processes over real TCP; the rig's actual fault-
//! injection code and its tests still depend on nothing else in the
//! workspace.
//!
//! ```text
//! seed_foreign_job --data-dir D
//! ```

use charpente::core::table::Codec;
use charpente::prod::ProdDisk;
use charpente::{Db, DbConfig, Job, Schema};

struct FakeSendReceipt;

impl Codec for FakeSendReceipt {
    fn encode(&self, _out: &mut Vec<u8>) {}
    fn decode(_input: &mut &[u8]) -> std::io::Result<FakeSendReceipt> {
        Ok(FakeSendReceipt)
    }
}

impl Job for FakeSendReceipt {
    // The real job's name (`examples/invoicing/src/jobs.rs`) — this is what
    // makes it "foreign shape of a MOUNTED name" rather than an unrelated,
    // unmounted job type.
    const NAME: &'static str = "SendReceipt";
    // Arbitrary and deliberately not the macro-derived fingerprint of the
    // real `SendReceipt` — the mismatch, not the specific value, is what
    // `refuse_foreign_pending_jobs` acts on.
    const FINGERPRINT: u64 = 0xF0F0_F0F0_F0F0_F0F0;
}

fn main() -> std::io::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let data_dir = args
        .iter()
        .position(|a| a == "--data-dir")
        .and_then(|i| args.get(i + 1))
        .expect("usage: seed_foreign_job --data-dir D");

    let disk = ProdDisk::new(data_dir)?;
    let mut db = Db::open(Box::new(disk), DbConfig::default(), Schema::new())?;
    db.transact(|tx| {
        tx.enqueue(FakeSendReceipt);
        Ok::<(), std::io::Error>(())
    })
    .expect("enqueue on a freshly opened writable node cannot fail");
    Ok(())
}
