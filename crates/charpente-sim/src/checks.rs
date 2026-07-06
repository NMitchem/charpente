//! Reusable oracles for sim workloads (step-7 spec §Platform-checker
//! library). v0 ships exactly three; more earn their way in when the demo
//! app demands them.

/// The three recognized fault-fallout families (extracted from the internal
/// suite, which had hand-rolled this four times):
///  1. the sim's own injected-fault wordings,
///  2. our WAL/engine poison wording,
///  3. redb's latch ("close and re-open the database") — its internal
///     already-saw-an-IO-error signal.
///
/// Anything else is NOT fallout and must fail the universe for adjudication.
/// Recognizing these cannot mask a real bug so long as the workload's
/// end-state oracle (e.g. exactly-once on reopen) still runs either way.
pub fn is_fault_fallout(text: &str) -> bool {
    text.contains("simulated")
        || text.contains("injected")
        || text.contains("poisoned")
        || text.contains("close and re-open the database")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recognizes_all_three_families_and_nothing_else() {
        for fallout in [
            "simulated disk write error (injected fault)",
            "simulated fsync failure — durability of writes since the last successful sync is unknown",
            "WAL poisoned by an earlier IO failure",
            "engine poisoned: a durable record failed to apply",
            "Storage error: close and re-open the database",
        ] {
            assert!(is_fault_fallout(fallout), "must classify: {fallout}");
        }
        for real_bug in [
            "row decode: needed 8 bytes for u64, 3 left — schema drift or corruption",
            "assertion failed: durable.is_some_and(|d| d >= applied)",
            "exactly-once violated: hits 3, done 2 for key 7",
        ] {
            assert!(!is_fault_fallout(real_bug), "must NOT classify: {real_bug}");
        }
    }
}
