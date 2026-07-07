//! Reusable oracles for sim workloads (step-7 spec §Platform-checker
//! library). v0 ships exactly three; more earn their way in when the demo
//! app demands them.

use crate::Sim;

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

/// Determinism oracle: runs `workload` twice, each on its own fresh
/// `Sim::new(seed)` world, and asserts both the returned value and the
/// durable end-state (`Sim::durable_fingerprint`) match. Always constructs
/// its worlds via plain `Sim::new` — never swarm `FaultConfig` derivation —
/// so a caller replaying a swarm universe's exact fault schedule must
/// pre-wrap `workload` to reconstruct that `Sim` itself; this checker has no
/// way to recover a swarm config from a bare seed.
pub fn replays_identically<T: PartialEq + std::fmt::Debug>(
    seed: u64,
    workload: impl Fn(&mut Sim) -> Result<T, String>,
) -> Result<(), String> {
    let mut first = Sim::new(seed);
    let r1 = workload(&mut first)
        .map_err(|e| format!("replays_identically: first run failed under seed {seed:#x}: {e}"))?;
    let fp1 = first.durable_fingerprint();

    let mut second = Sim::new(seed);
    let r2 = workload(&mut second)
        .map_err(|e| format!("replays_identically: second run failed under seed {seed:#x}: {e}"))?;
    let fp2 = second.durable_fingerprint();

    if r1 != r2 {
        return Err(format!(
            "replays_identically violated under seed {seed:#x}: results diverged — {r1:?} vs {r2:?}"
        ));
    }
    if fp1 != fp2 {
        return Err(format!(
            "replays_identically violated under seed {seed:#x}: durable fingerprints diverged — {fp1:#x} vs {fp2:#x} (same result, different durable state)"
        ));
    }
    Ok(())
}

/// Ack-durability oracle: runs `rounds` rounds of drive-then-restart-then-
/// verify on one `Sim::new(seed)`. Each round, `drive` performs one round of
/// app work and returns the ack ids it answered success for — `drive`
/// classifies its own faults via `is_fault_fallout` so an id only appears
/// here if the app actually promised durability for it. `sim.restart()` then
/// collapses the disk as usual, and `verify` checks that every id acked so
/// far (not just this round's) is still present. A `verify` failure fails
/// the whole check, prefixed with the round and seed so a repro can target
/// the exact round.
pub fn acked_implies_durable(
    seed: u64,
    rounds: u32,
    mut drive: impl FnMut(&mut Sim) -> Result<Vec<String>, String>,
    mut verify: impl FnMut(&mut Sim, &[String]) -> Result<(), String>,
) -> Result<(), String> {
    let mut sim = Sim::new(seed);
    let mut acked: Vec<String> = Vec::new();
    for round in 1..=rounds {
        let mut new_acks = drive(&mut sim).map_err(|e| {
            format!(
                "acked-implies-durable: drive failed in round {round} under seed {seed:#x}: {e}"
            )
        })?;
        acked.append(&mut new_acks);
        sim.restart();
        verify(&mut sim, &acked).map_err(|e| {
            format!("acked-implies-durable violated after round {round} under seed {seed:#x}: {e}")
        })?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use charpente_core::io::{Disk, Entropy};
    use std::sync::atomic::{AtomicU64, Ordering};

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

    // ---------------------------------------------------- replays_identically

    #[test]
    fn replays_identically_catches_a_workload_that_reads_ambient_state() {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let seed = 1;
        let err = replays_identically(seed, |_: &mut Sim| {
            Ok::<u64, String>(COUNTER.fetch_add(1, Ordering::SeqCst))
        })
        .unwrap_err();
        assert!(err.contains("results diverged"), "{err}");
        assert!(err.contains(&format!("{seed:#x}")), "{err}");
    }

    #[test]
    fn replays_identically_passes_an_honest_workload() {
        replays_identically(42, |sim: &mut Sim| {
            let mut byte = [0u8; 1];
            sim.entropy().fill(&mut byte);
            Ok::<u8, String>(byte[0])
        })
        .unwrap();
    }

    // ------------------------------------------------- acked_implies_durable

    /// A toy app: each round writes one byte at an advancing offset in a sim
    /// file, optionally syncing before acking it. Faults from `open`/
    /// `write_at`/`sync` are classified via `is_fault_fallout` and turned
    /// into "no ack this round" rather than a hard failure — a real app's
    /// `drive` is expected to do the same (per the doc comment above).
    fn toy_drive(sync_before_ack: bool) -> impl FnMut(&mut Sim) -> Result<Vec<String>, String> {
        let mut round: u64 = 0;
        move |sim: &mut Sim| {
            let file = match sim.disk().open("toy") {
                Ok(f) => f,
                Err(e) if is_fault_fallout(&e.to_string()) => return Ok(vec![]),
                Err(e) => return Err(e.to_string()),
            };
            if let Err(e) = file.write_at(round, &[0xAA]) {
                let msg = e.to_string();
                return if is_fault_fallout(&msg) {
                    Ok(vec![])
                } else {
                    Err(msg)
                };
            }
            if sync_before_ack && let Err(e) = file.sync() {
                let msg = e.to_string();
                return if is_fault_fallout(&msg) {
                    Ok(vec![])
                } else {
                    Err(msg)
                };
            }
            let id = format!("byte-{round}");
            round += 1;
            Ok(vec![id])
        }
    }

    fn toy_verify(sim: &mut Sim, acked: &[String]) -> Result<(), String> {
        let file = sim.disk().open("toy").map_err(|e| e.to_string())?;
        let len = file.len().map_err(|e| e.to_string())?;
        for id in acked {
            let offset: u64 = id
                .strip_prefix("byte-")
                .expect("toy ack ids are \"byte-N\"")
                .parse()
                .expect("toy ack ids carry a numeric offset");
            if offset >= len {
                return Err(format!("{id} missing after restart (file len {len})"));
            }
            let mut byte = [0u8];
            file.read_at(offset, &mut byte).map_err(|e| e.to_string())?;
            if byte[0] != 0xAA {
                return Err(format!("{id} corrupted after restart"));
            }
        }
        Ok(())
    }

    #[test]
    fn acked_implies_durable_passes_when_synced_before_ack() {
        acked_implies_durable(0xACED, 5, toy_drive(true), toy_verify).unwrap();
    }

    #[test]
    fn acked_implies_durable_catches_a_toy_that_never_syncs() {
        // Pinned: under this seed the round-1 write's crash-collapse draw
        // lands on "dropped" rather than "applied" (a 1-byte write's only
        // outcomes are those two — see disk::tear), so the missing-sync bug
        // is caught on round 1, deterministically, forever.
        let seed = 0x5eed_ca11;
        let err = acked_implies_durable(seed, 5, toy_drive(false), toy_verify).unwrap_err();
        assert!(
            err.starts_with("acked-implies-durable violated after round"),
            "{err}"
        );
        assert!(err.contains(&format!("{seed:#x}")), "{err}");
    }
}
