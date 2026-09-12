//! F1: `Store::open`'s redb open-fence false alarm (`crates/charpente-core/src/
//! table.rs`'s `catch_unwind` around redb's open-time assert, added `1c439d5`).
//! The fence converts redb's open-time panic into an ordinary open failure so
//! `Db::open`'s discard-and-rebuild path engages — but the DEFAULT panic hook
//! had already printed `panicked at ...page_manager.rs` before the fence's
//! `catch_unwind` caught it. 134 such lines showed up on a fully GREEN
//! 10,000-universe batch. A docs-only agent read that as failure and
//! discarded a correct full-App-stack property; a later hunt's own
//! controller was caught by the same trace. DESIGN §7 and hard rule 5 make
//! the fast, precise *no* the product; this was its inverse — a false alarm
//! that cost a competent agent good work.
//!
//! This crate spawns built binaries, it never links against them
//! (`src/lib.rs`'s module doc) — an architectural guarantee enforced
//! mechanically by an EMPTY `[dev-dependencies]`, not by review (F1 round 2,
//! Important-3 ruling). Every test below shells out to a `cargo test`
//! subprocess and asserts on its captured output; none of them link
//! `charpente_sim` or `charpente_core`. The actual hook-install and
//! fence-triggering code that the last two tests drive lives in
//! `crates/charpente-sim/tests/open_fence.rs`, on the other side of that
//! boundary.
//!
//! `open_fence_does_not_print_a_raw_redb_panic_trace`'s child is NOT marked
//! with an env var. An earlier draft set `CHARPENTE_F1_CHILD=1` on that
//! spawned `cargo test` "as a marker the child's output can be attributed
//! to", and nothing ever read it — dead scaffolding that looked load-bearing
//! (mechanical-fences Item 3). That child runs a test in a DIFFERENT crate
//! (`-p charpente`), so it cannot re-enter this test and needs no recursion
//! guard either. This file's OTHER two tests are different: their children
//! DO set `CHARPENTE_F1_PANIC_CHILD=1` (below, on the spawned `charpente-sim`
//! child), and that one is load-bearing — `crates/charpente-sim/tests/
//! open_fence.rs` reads it to gate those tests to run only as a spawned
//! child, and removing it would make them fire inside the ordinary sweep. Do
//! not read the dead-marker finding above as license to strip that one too.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

/// The existing deterministic fence regression this oracle drives (read, not
/// modified — see `crates/charpente/tests/transactions.rs:1282`).
const CHILD_TEST_NAME: &str = "unopenable_store_with_compacted_wal_is_a_named_hard_error";

/// F1: the fence catches redb's open-time assert, but the DEFAULT panic hook
/// prints `panicked at ...page_manager.rs` before the catch. 134 such lines on
/// a fully green 10k-universe batch taught a competent agent to read green as
/// red, and later caught a hunt's own controller. Hard rule 5 makes the precise
/// *no* the product; a false alarm is its inverse.
///
/// The child process is the oracle: a panic hook's output cannot be captured
/// in-process without installing a hook, which is the thing under test.
#[test]
fn open_fence_does_not_print_a_raw_redb_panic_trace() {
    let out = std::process::Command::new(env!("CARGO"))
        .args([
            "test",
            "-p",
            "charpente",
            "--test",
            "transactions",
            "--",
            CHILD_TEST_NAME,
            "--nocapture",
            "--exact",
        ])
        .output()
        .expect("child cargo test runs");

    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );

    assert!(
        out.status.success(),
        "the fence regression itself must pass; this test asserts only on its output:\n{combined}"
    );
    // F1 round 2, Important-2: a bogus or renamed `--exact` target still
    // exits 0 with "0 passed" and everything in the binary filtered out — the
    // three assertions below are all NEGATIVE (absence checks), so they pass
    // vacuously against output from a test that never ran. This positive
    // check is the proof it actually executed.
    assert!(
        combined.contains(&format!("test {CHILD_TEST_NAME} ... ok")),
        "the driven test's own pass line is missing from the child's output — either \
         it didn't run (a stale/renamed `--exact` target selects nothing and cargo \
         still exits 0) or it failed; every assertion below is vacuous without \
         this:\n{combined}"
    );
    assert!(
        combined.contains("test result: ok. 1 passed; 0 failed"),
        "expected exactly one test selected and passing under `--exact`; a different \
         count means the filter matched the wrong set (or nothing):\n{combined}"
    );
    assert!(
        !combined.contains("page_manager.rs"),
        "raw redb panic trace reached the log — this is F1, the false alarm that \
         cost an agent a correct property:\n{combined}"
    );
    assert!(
        !combined.contains("panicked at"),
        "a `panicked at` line reached the log from a benign fenced open:\n{combined}"
    );
}

/// The fence must not become a blanket mute. A panic OUTSIDE the fence still
/// reaches the hook in full. Drives
/// `crates/charpente-sim/tests/open_fence.rs`'s matching child arm.
#[test]
fn a_panic_outside_the_open_fence_still_prints() {
    let out = std::process::Command::new(env!("CARGO"))
        .args([
            "test",
            "-p",
            "charpente-sim",
            "--test",
            "open_fence",
            "a_panic_outside_the_open_fence_still_prints",
            "--",
            "--nocapture",
            "--exact",
        ])
        .env("CHARPENTE_F1_PANIC_CHILD", "1")
        .output()
        .expect("child runs");
    let err = String::from_utf8_lossy(&out.stderr).to_string();
    assert!(
        err.contains("deliberate out-of-fence panic"),
        "the hook swallowed a real panic — strictly worse than F1:\n{err}"
    );
}

/// F1 round 2, Important-1's oracle: a panic reachable INSIDE the fence but
/// NOT originating in redb (a stand-in for `SimFile`'s own panics,
/// `charpente-sim/src/lib.rs:1133`/`:1153`, reachable through `DiskBackend`
/// during the exact window the fence's guard is active) must still print in
/// full — the mute is scoped to redb's own assert, not to the time window.
/// Drives `crates/charpente-sim/tests/open_fence.rs`'s matching child arm.
#[test]
fn a_non_redb_panic_inside_the_open_fence_still_prints() {
    let out = std::process::Command::new(env!("CARGO"))
        .args([
            "test",
            "-p",
            "charpente-sim",
            "--test",
            "open_fence",
            "a_non_redb_panic_inside_the_open_fence_still_prints",
            "--",
            "--nocapture",
            "--exact",
        ])
        .env("CHARPENTE_F1_PANIC_CHILD", "1")
        .output()
        .expect("child runs");
    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        combined.contains("deliberate non-redb panic reachable inside the open fence"),
        "a non-redb panic reachable inside the fence was muted alongside redb's own — \
         strictly worse than F1, since this window can carry a genuine simulator bug \
         (SimFile's own panics, lib.rs:1133/:1153):\n{combined}"
    );
    assert!(
        !combined.contains("redb open-fence engaged"),
        "the fence's benign message must not appear for a non-redb panic:\n{combined}"
    );
}
