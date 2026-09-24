//! The wire-layer vehicle (spec §1, as amended at the plan fix-wave): the
//! node.rs precedent — parse flags, build, serve::serve, wait. Deliberately
//! NOT `charpente::launch`: launch() re-reads the live process argv
//! (launch.rs:31) and serves only on ZERO argv, so a child spawned with
//! `--addr`/`--data-dir` would exit 2 as an unknown verb and never bind.
//! Edge tooling outside the deterministic core (DESIGN §1).
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use charpente::prod::{ProdDisk, ProdEntropy};
use std::process::ExitCode;

fn main() -> ExitCode {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let mut addr = None;
    let mut dir = None;
    let mut it = argv.iter();
    while let Some(flag) = it.next() {
        match flag.as_str() {
            "--addr" => addr = it.next().cloned(),
            "--data-dir" => dir = it.next().cloned(),
            other => {
                eprintln!("bench-app: unknown flag {other:?}");
                eprintln!("usage: bench-app --addr HOST:PORT --data-dir DIR");
                return ExitCode::from(2);
            }
        }
    }
    let (Some(addr), Some(dir)) = (addr, dir) else {
        eprintln!("usage: bench-app --addr HOST:PORT --data-dir DIR");
        return ExitCode::from(2);
    };
    let serve = || -> std::io::Result<i32> {
        // Count durable syncs and publish the counter BEFORE serving, so the
        // `/__syncs` route can report a real number instead of the client-side
        // derivation group commit made unsound. The wrapper only counts — it
        // changes no outcome — so wrapping unconditionally cannot perturb what
        // this binary is here to measure.
        // Scoped to `wal/`: the gate's denominator is an append+sync ceiling
        // on one growing file, so the numerator must be WAL syncs and not the
        // store's checkpoints and `.horizon`'s too. See `counting::with_prefix`.
        let (disk, syncs) = bench_harness::counting::CountingDisk::with_prefix(
            Box::new(ProdDisk::new(&dir)?),
            Some("wal/"),
        );
        let _ = bench_harness::SYNC_COUNTER.set(syncs);
        let entropy = ProdEntropy::new()?;
        let app = bench_harness::build(Box::new(disk), Box::new(entropy))?;
        // A stopped kernel ends the process with its code (spec §5), rather
        // than parking behind an edge that would only ever answer 500.
        Ok(charpente::serve::serve(&addr, app)?.wait())
    };
    match serve() {
        Ok(code) => ExitCode::from(code as u8),
        Err(e) => {
            eprintln!("bench-app: {e}");
            ExitCode::from(1)
        }
    }
}
