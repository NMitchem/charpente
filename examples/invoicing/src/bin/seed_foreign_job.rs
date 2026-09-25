//! `seed_foreign_job`: a tiny third harness vehicle for `examples/invoicing`
//! (alongside `node`/`tenant_node`), for `edge-harness`'s process-level
//! oracle that a kernel stop under `charpente::launch` actually ends the
//! process. Opens the target data dir with an otherwise-empty schema and
//! enqueues one Pending job under the real `SendReceipt` name but a
//! hand-rolled shape with a different fingerprint, then exits — so the next
//! process to open this data dir with the REAL `invoicing::build_app`
//! (which mounts the real `SendReceipt`) finds a foreign-shape Pending row
//! and its kernel refuses to start (spec §5). Writes only the platform jobs
//! table; `invoicing::build_app`'s own tables are all still unwritten
//! (adoption, not a schema conflict) when the real binary opens next.
//!
//! ```text
//! seed_foreign_job --data-dir D
//! ```

use charpente::core::table::Codec;
use charpente::http::{App, AppConfig};
use charpente::prod::{ProdDisk, ProdEntropy};
use charpente::{Job, Schema};

struct FakeSendReceipt;

impl Codec for FakeSendReceipt {
    fn encode(&self, _out: &mut Vec<u8>) {}
    fn decode(_input: &mut &[u8]) -> std::io::Result<FakeSendReceipt> {
        Ok(FakeSendReceipt)
    }
}

impl Job for FakeSendReceipt {
    // The real job's name (jobs.rs) — this is what makes it "foreign shape
    // of a MOUNTED name" rather than an unrelated, unmounted job type.
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
    let entropy = ProdEntropy::new()?;
    let mut app = App::open(
        Box::new(disk),
        Schema::new(),
        Box::new(entropy),
        AppConfig::default(),
    )?;
    app.db_for_tests()
        .transact(|tx| {
            tx.enqueue(FakeSendReceipt);
            Ok::<(), std::io::Error>(())
        })
        .expect("enqueue on a freshly opened writable node cannot fail");
    Ok(())
}
