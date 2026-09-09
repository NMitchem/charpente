//! `scripts/compile-bench.sh`'s own self-test, run as part of the ordinary
//! suite.
//!
//! **Why this file exists.** Spec §4 — retry a refusal about the machine,
//! never one about the measurement — is enforced at three hops: `verdict`,
//! `classify_phase`/`exit_code` (covered in `compile_accounting`), and the
//! shell script's `case` arms on the binary's exit status. The last hop had no
//! oracle at all: swapping the script's `1)` and `3)` arms leaves the entire
//! Rust suite green while silently converting "never retry `Unstable`" into
//! "always retry `Unstable`", and the only visible symptom would be a
//! published number that had been rerolled until it looked tight.
//!
//! **Why it is not `#[ignore]`d**, unlike this crate's `compile` suite. The
//! criterion there is a test that spawns real *builds* — `cargo`, minutes of
//! wall clock, a target directory. This one drives the script against a stub
//! binary that exits with a chosen code: about twenty short-lived shells, no
//! cargo, no port, no build, well under a second. Ignoring it would mean the
//! §4 arms are checked only by a command someone has to remember, which is the
//! condition CI's pinned-seed comments describe at length as the thing that
//! leaves a guard silent.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use std::path::{Path, PathBuf};
use std::process::Command;

fn repo() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// The number of assertions the script's `--self-test` made when this test was
/// written. A floor rather than an equality so cases can be added freely — its
/// job is to catch a self-test that has quietly stopped running most of them
/// and still exits 0.
const EXPECTED_CHECKS_AT_LEAST: usize = 25;

#[test]
fn the_runner_script_never_retries_a_refusal_about_the_measurement() {
    let script = repo().join("scripts/compile-bench.sh");
    let out = Command::new("bash")
        .arg(&script)
        .arg("--self-test")
        .output()
        .expect("bash --self-test");

    let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
    assert!(
        out.status.success(),
        "scripts/compile-bench.sh --self-test failed ({}). Run it directly to see which \
         case: bash scripts/compile-bench.sh --self-test\n--- stdout ---\n{stdout}\n\
         --- stderr ---\n{stderr}",
        out.status
    );

    // A script whose `--self-test` exits 0 having run nothing would pass the
    // assertion above. These are the two cases the whole file exists for, and
    // they are named so that deleting one reddens here rather than quietly
    // shrinking the oracle.
    for case in [
        "a final refusal stops the run and is never retried",
        "a recorded final refusal refuses the NEXT night's run",
    ] {
        assert!(
            stdout.contains(case),
            "the self-test no longer covers '{case}':\n{stdout}"
        );
    }
    let checks = stdout.matches("  ok    ").count();
    assert!(
        checks >= EXPECTED_CHECKS_AT_LEAST,
        "the self-test made {checks} checks, expected at least \
         {EXPECTED_CHECKS_AT_LEAST}:\n{stdout}"
    );
}
