//! The crate's §4.4 repro story: every fault action and observed outcome a
//! rig performs is appended to a `Scenario` with a monotonic step counter and
//! a wall-clock offset (edge tooling outside the deterministic core — a real
//! `Instant`/`SystemTime` here is not a determinism violation, DESIGN §1's
//! fence is the core, not this crate). The accumulated script is dumped to a
//! file under the scenario's directory whenever the `Scenario` drops — on
//! success (so "a scenario script file exists" always holds) and, loudly, on
//! a panic — so a failure names a file an operator can open and replay by
//! hand instead of a seed (this crate makes no seed promise, see the crate
//! doc comment).
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use std::fmt::Write as _;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

pub struct Scenario {
    dir: PathBuf,
    start: Instant,
    events: Vec<(u64, u128, String)>,
    dumped: bool,
}

impl Scenario {
    /// Creates a fresh, uniquely named directory under `root` (typically
    /// `std::env::temp_dir()`) to hold this scenario's script and any child
    /// processes' tee'd stdout/stderr logs.
    pub fn new(root: &Path, name: &str) -> io::Result<Scenario> {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock reads after the epoch")
            .as_nanos();
        let dir = root.join(format!("{name}-{}-{nanos:x}", std::process::id()));
        std::fs::create_dir_all(&dir)?;
        Ok(Scenario {
            dir,
            start: Instant::now(),
            events: Vec::new(),
            dumped: false,
        })
    }

    /// This scenario's directory — where child log files and the dumped
    /// script live.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Appends one line to the script: a fault action taken or an
    /// observation made, in the order the rig performed them.
    pub fn record(&mut self, text: impl Into<String>) {
        let step = self.events.len() as u64;
        let at_millis = self.start.elapsed().as_millis();
        self.events.push((step, at_millis, text.into()));
    }

    /// Writes the accumulated script to `<dir>/scenario.log` and returns its
    /// path. Idempotent — later calls (or the `Drop` safety net) overwrite
    /// with whatever has accumulated since.
    pub fn dump(&mut self) -> io::Result<PathBuf> {
        let path = self.dir.join("scenario.log");
        let mut out = String::new();
        for (step, at_millis, text) in &self.events {
            let _ = writeln!(out, "[{step:04}] +{at_millis}ms  {text}");
        }
        std::fs::write(&path, out)?;
        self.dumped = true;
        Ok(path)
    }
}

impl Drop for Scenario {
    fn drop(&mut self) {
        if self.dumped {
            return;
        }
        match self.dump() {
            Ok(path) if std::thread::panicking() => {
                eprintln!(
                    "edge-harness: scenario script (failure) at {}",
                    path.display()
                );
            }
            Ok(_) => {}
            Err(e) => eprintln!("edge-harness: failed to dump scenario script: {e}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[ignore]
    fn record_then_dump_writes_every_line_in_order() {
        let root = std::env::temp_dir();
        let mut scenario = Scenario::new(&root, "scenario_unit_test").unwrap();
        scenario.record("first");
        scenario.record("second");
        let path = scenario.dump().unwrap();
        let contents = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = contents.lines().collect();
        assert_eq!(lines.len(), 2);
        assert!(lines[0].contains("first"), "{contents}");
        assert!(lines[1].contains("second"), "{contents}");
        std::fs::remove_dir_all(scenario.dir()).unwrap();
    }

    #[test]
    #[ignore]
    fn dump_happens_even_with_no_explicit_call() {
        let root = std::env::temp_dir();
        let dir = {
            let mut scenario = Scenario::new(&root, "scenario_drop_test").unwrap();
            scenario.record("only line");
            scenario.dir().to_path_buf()
        };
        let path = dir.join("scenario.log");
        assert!(
            path.exists(),
            "Drop must dump even without an explicit call"
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
