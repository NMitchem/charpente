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
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

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
            "unopenable_store_with_compacted_wal_is_a_named_hard_error",
            "--nocapture",
            "--exact",
        ])
        .env("CHARPENTE_F1_CHILD", "1")
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
/// reaches the hook in full.
#[test]
fn a_panic_outside_the_open_fence_still_prints() {
    if std::env::var("CHARPENTE_F1_PANIC_CHILD").is_ok() {
        charpente_sim::harness::install_open_fence_hook();
        assert!(!charpente_core::table::in_open_fence());
        panic!("deliberate out-of-fence panic");
    }
    let out = std::process::Command::new(env!("CARGO"))
        .args([
            "test",
            "-p",
            "edge-harness",
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
