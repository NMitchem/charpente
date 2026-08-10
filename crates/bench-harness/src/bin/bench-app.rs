//! The wire-layer vehicle (spec §1, as amended at the plan fix-wave): the
//! node.rs precedent — parse flags, build, serve::serve, park. Deliberately
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
    let serve = || -> std::io::Result<()> {
        let disk = ProdDisk::new(&dir)?;
        let entropy = ProdEntropy::new()?;
        let app = bench_harness::build(Box::new(disk), Box::new(entropy))?;
        // serve returns a handle; dropping it does not stop serving (launch's
        // own serve arm drops it the same way) — the park loop keeps the
        // process alive until the harness kills it.
        charpente::serve::serve(&addr, app)?;
        loop {
            std::thread::park();
        }
    };
    match serve() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("bench-app: {e}");
            ExitCode::from(1)
        }
    }
}
