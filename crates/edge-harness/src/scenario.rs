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
    /// `events.len()` as of the last successful [`Scenario::dump`], or `None`
    /// if this scenario has never been dumped. `Drop` re-dumps exactly when
    /// lines have accumulated since — so the file on disk is always the
    /// complete script, and a `record` after an explicit `dump` is not lost.
    dumped_at: Option<usize>,
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
            dumped_at: None,
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
    /// path. Idempotent — later calls rewrite the file with whatever has
    /// accumulated since, and so does the `Drop` safety net, but only when
    /// something *has* (`dumped_at`): a scenario dumped as its last act pays
    /// for exactly one write, while one that keeps recording afterwards still
    /// ends up with a complete file, which is this crate's whole repro story.
    /// One caveat for callers that clean up on success: if the scenario's
    /// directory is removed after a dump, a later `record` has nowhere to go —
    /// `Drop` then says so on stderr rather than dropping it silently.
    pub fn dump(&mut self) -> io::Result<PathBuf> {
        let path = self.dir.join("scenario.log");
        let mut out = String::new();
        for (step, at_millis, text) in &self.events {
            let _ = writeln!(out, "[{step:04}] +{at_millis}ms  {text}");
        }
        std::fs::write(&path, out)?;
        self.dumped_at = Some(self.events.len());
        Ok(path)
    }
}

impl Drop for Scenario {
    fn drop(&mut self) {
        // Nothing recorded since the last explicit dump: the file on disk is
        // already the complete script, so don't rewrite it (and don't fail
        // noisily for a caller that cleaned its scenario directory up after
        // dumping, which the oracles do on success).
        if self.dumped_at == Some(self.events.len()) {
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

    /// The `dump`-then-keep-recording trap (final whole-branch review, M-5):
    /// nothing recorded after an explicit dump may be lost, because the
    /// script file is this crate's only repro artifact.
    #[test]
    #[ignore]
    fn a_line_recorded_after_an_explicit_dump_still_reaches_the_file() {
        let root = std::env::temp_dir();
        let dir = {
            let mut scenario = Scenario::new(&root, "scenario_late_record_test").unwrap();
            scenario.record("before the dump");
            scenario.dump().unwrap();
            scenario.record("after the dump");
            scenario.dir().to_path_buf()
        };
        let contents = std::fs::read_to_string(dir.join("scenario.log")).unwrap();
        assert!(contents.contains("before the dump"), "{contents}");
        assert!(contents.contains("after the dump"), "{contents}");
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
