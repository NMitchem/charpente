//! F1: an unopenable table store must not print a raw panic trace. redb
//! 4.1.0 asserted at open on a store file whose length disagreed with its
//! durable header's layout, and `Store::open` caught that panic to turn it
//! into an open failure — but the DEFAULT panic hook had already printed
//! `panicked at ...page_manager.rs` before the catch. 134 such lines showed
//! up on a fully GREEN 10,000-universe batch. A docs-only agent read that as
//! failure and discarded a correct full-App-stack property; a later hunt's
//! own controller was caught by the same trace. DESIGN §7 and hard rule 5
//! make the fast, precise *no* the product; this was its inverse — a false
//! alarm that cost a competent agent good work.
//!
//! The first fix muted that one trace with a filtering panic hook. redb 4.3.0
//! returns `StorageError::Corrupted` for both states instead of asserting, so
//! the catch and the hook are both gone and there is no benign panic left to
//! mute. This oracle is what keeps F1 closed without them: the store is
//! unopenable, the named refusal is returned, and nothing that looks like a
//! panic reaches the log.
//!
//! This crate spawns built binaries, it never links against them
//! (`src/lib.rs`'s module doc) — an architectural guarantee enforced
//! mechanically by an EMPTY `[dev-dependencies]`, not by review. The test
//! below shells out to a `cargo test` subprocess and asserts on its captured
//! output; it links neither `charpente_sim` nor `charpente_core`.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

/// The deterministic regression this oracle drives (read, not modified — see
/// `crates/charpente/tests/transactions.rs`): a store truncated below its
/// header's layout, under a log compacted past seq 0.
const CHILD_TEST_NAME: &str = "unopenable_store_with_compacted_wal_is_a_named_hard_error";

/// The child process is the oracle: what a panic hook prints cannot be
/// captured in-process without installing a hook, and the repo has none
/// (`crates/guide-examples/tests/hook_census.rs` holds that at zero).
#[test]
fn an_unopenable_store_prints_no_raw_panic_trace() {
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
        "the driven regression itself must pass; this test asserts only on its output:\n{combined}"
    );
    // A bogus or renamed `--exact` target still exits 0 with "0 passed" and
    // everything in the binary filtered out — the two assertions below are
    // NEGATIVE (absence checks), so they pass vacuously against output from
    // a test that never ran. This positive check is the proof it executed.
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
        "a `panicked at` line reached the log from an unopenable store:\n{combined}"
    );
}
