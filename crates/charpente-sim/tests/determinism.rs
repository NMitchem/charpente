//! Replay determinism is the entire verification story (DESIGN.md §1): the
//! same seed must produce the byte-identical universe, forever. If this test
//! ever fails, some ambient nondeterminism has leaked inside the boundary —
//! stop everything and find it.

use charpente_core::io::{Disk, Entropy};
use charpente_sim::Sim;

/// A fault-heavy workload; returns everything observable about the universe.
fn trace(seed: u64) -> (u64, u64, Vec<u8>) {
    let sim = Sim::new(seed);
    let disk = sim.disk();
    let entropy = sim.entropy();

    let mut observed = Vec::new();
    for round in 0..3u64 {
        sim.crash_disk_after(entropy.next_u64() % 32);
        if let Ok(file) = disk.open("t") {
            for i in 0..8u64 {
                let mut payload = vec![0u8; 700];
                entropy.fill(&mut payload);
                if file.write_at(round * 8_000 + i * 1_000, &payload).is_err() {
                    break;
                }
                if file.sync().is_err() {
                    break;
                }
            }
            if let Ok(len) = file.len() {
                observed.extend_from_slice(&len.to_le_bytes());
            }
        }
        sim.restart();
    }
    (sim.durable_fingerprint(), sim.now().as_nanos(), observed)
}

#[test]
fn same_seed_replays_identically() {
    for seed in [0x1, 0xDEAD_BEEF, 0x0123_4567_89AB_CDEF] {
        assert_eq!(
            trace(seed),
            trace(seed),
            "seed {seed:#x} did not replay identically"
        );
    }
}
