//! WAL + apply-loop crash-recovery properties (PLAN.md M0 step 2), under the
//! full fault matrix with 4 KiB segments so rotation happens constantly.
//!
//! The standing invariant: after any crash, the machine recovers to exactly
//! `fold(apply, <prefix of submitted records>)`, the prefix is at least every
//! acknowledged record, and nothing is ever corrupt, reordered, or invented.

use charpente_core::engine::{Engine, StateMachine};
use charpente_core::io::{Disk, Entropy};
use charpente_core::log::{Wal, WalConfig};
use charpente_sim::{FaultConfig, Sim, run_universes};

fn small_segments() -> WalConfig {
    WalConfig {
        dir: "wal".to_string(),
        segment_bytes: 4096,
    }
}

/// Toy state machine: keeps every record and an order-sensitive FNV hash, and
/// asserts the apply stream is gapless from zero.
#[derive(Default)]
struct Ledger {
    records: Vec<Vec<u8>>,
    hash: u64,
}

impl StateMachine for Ledger {
    fn apply(&mut self, seq: u64, payload: &[u8]) {
        assert_eq!(seq, self.records.len() as u64, "apply stream skipped a seq");
        let mut hash = if self.hash == 0 {
            0xcbf2_9ce4_8422_2325
        } else {
            self.hash
        };
        for &b in payload {
            hash = (hash ^ u64::from(b)).wrapping_mul(0x0000_0100_0000_01b3);
        }
        self.hash = hash;
        self.records.push(payload.to_vec());
    }
}

fn check_prefix(recovered: &[Vec<u8>], submitted: &[Vec<u8>], acked: usize) -> Result<(), String> {
    if recovered.len() < acked {
        return Err(format!(
            "durability violated: {acked} acknowledged records but only {} recovered",
            recovered.len()
        ));
    }
    if recovered.len() > submitted.len() {
        return Err(format!(
            "recovery invented records: {} recovered, {} ever submitted",
            recovered.len(),
            submitted.len()
        ));
    }
    for (i, (got, want)) in recovered.iter().zip(submitted).enumerate() {
        if got != want {
            return Err(format!(
                "record {i} corrupt after recovery ({} vs {} bytes)",
                got.len(),
                want.len()
            ));
        }
    }
    Ok(())
}

/// One universe: recover → check → submit-until-death → power-cycle, with
/// every choice seeded. `payload_len` picks the size distribution.
fn wal_workload(sim: &mut Sim, payload_len: impl Fn(&dyn Entropy) -> usize) -> Result<(), String> {
    let entropy = sim.entropy();
    let mut submitted: Vec<Vec<u8>> = Vec::new();
    let mut acked = 0usize;

    for _round in 0..4 {
        sim.crash_disk_after(entropy.next_u64() % 96);
        let mut engine =
            match Engine::recover(Box::new(sim.disk()), small_segments(), Ledger::default()) {
                Ok(engine) => engine,
                Err(_) => {
                    sim.restart();
                    continue;
                }
            };
        check_prefix(&engine.machine().records, &submitted, acked)?;

        submitted = engine.machine().records.clone();
        acked = submitted.len();

        for _ in 0..1 + entropy.next_u64() % 16 {
            let mut payload = vec![0u8; payload_len(&entropy)];
            entropy.fill(&mut payload);
            submitted.push(payload.clone());
            match engine.submit(&payload) {
                Ok(seq) => {
                    if seq != (acked as u64) {
                        return Err(format!("submit returned seq {seq}, expected {acked}"));
                    }
                    acked += 1;
                }
                Err(_) => break, // died here; anything unacked may or may not survive
            }
        }
        sim.restart();
    }

    // Recovery itself can hit injected write/fsync errors (it truncates and
    // syncs the cut) — seed 0x5cafb3ac016dccf9 found that. It is idempotent,
    // so the real-world response is restart-and-retry.
    let mut recovered = None;
    for _attempt in 0..8 {
        match Engine::recover(Box::new(sim.disk()), small_segments(), Ledger::default()) {
            Ok(engine) => {
                recovered = Some(engine);
                break;
            }
            Err(_) => sim.restart(),
        }
    }
    let engine =
        recovered.ok_or("final recovery failed 8 times in a row — not transient faults")?;
    check_prefix(&engine.machine().records, &submitted, acked)?;

    // The hash must equal a from-scratch fold of the recovered records —
    // state is a function of the log and nothing else.
    let mut replay = Ledger::default();
    for (seq, record) in engine.machine().records.iter().enumerate() {
        replay.apply(seq as u64, record);
    }
    if replay.hash != engine.machine().hash {
        return Err("machine hash diverged from a clean replay of its own records".to_string());
    }
    Ok(())
}

#[test]
fn wal_survives_the_fault_matrix() {
    run_universes!(512, |sim: &mut Sim| {
        wal_workload(sim, |entropy| 100 + (entropy.next_u64() % 1400) as usize)
    });
}

/// Same property with every payload exactly 512 bytes, so every regenerated
/// frame lands where a same-length discarded frame may lie beyond it. This
/// is the stale-frame-resurrection trap; it only stays green because
/// recovery truncates (and syncs) at the cut point.
#[test]
fn wal_fixed_size_payloads_cannot_resurrect_stale_frames() {
    run_universes!(512, |sim: &mut Sim| wal_workload(sim, |_| 512));
}

#[test]
fn wal_replays_identically_for_the_same_seed() {
    let trace = |seed: u64| {
        let mut sim = Sim::new(seed);
        let result = wal_workload(&mut sim, |e| 100 + (e.next_u64() % 1400) as usize);
        assert_eq!(result, Ok(()), "seed {seed:#x}");
        (sim.durable_fingerprint(), sim.now())
    };
    for seed in [0x11, 0xFEED_FACE, 0x0123_4567_89AB_CDEF] {
        assert_eq!(trace(seed), trace(seed));
    }
}

// ------------------------------------------------------------- unit tests
// (FaultConfig::none(): no injected faults, deterministic surgical damage)

fn quiet_sim() -> Sim {
    Sim::with_faults(0xA11, FaultConfig::none())
}

fn build_log(sim: &Sim, records: &[&[u8]]) {
    let mut engine =
        Engine::recover(Box::new(sim.disk()), small_segments(), Ledger::default()).unwrap();
    for record in records {
        engine.submit(record).unwrap();
    }
}

#[test]
fn rotation_spreads_records_across_segments_and_recovers() {
    let sim = quiet_sim();
    let records: Vec<Vec<u8>> = (0..40u8).map(|i| vec![i; 300]).collect();
    let refs: Vec<&[u8]> = records.iter().map(|r| r.as_slice()).collect();
    build_log(&sim, &refs);

    let segments = sim.disk().list("wal/").unwrap();
    assert!(
        segments.len() > 1,
        "4 KiB segments must have rotated: {segments:?}"
    );

    let engine =
        Engine::recover(Box::new(sim.disk()), small_segments(), Ledger::default()).unwrap();
    assert_eq!(engine.machine().records, records);
    assert_eq!(engine.last_applied(), Some(39));
}

#[test]
fn bit_flip_cuts_the_log_there_and_deletes_later_segments() {
    let sim = quiet_sim();
    // 300-byte payloads -> 316-byte frames -> 12 per 4 KiB segment.
    let records: Vec<Vec<u8>> = (0..40u8).map(|i| vec![i; 300]).collect();
    let refs: Vec<&[u8]> = records.iter().map(|r| r.as_slice()).collect();
    build_log(&sim, &refs);
    let before = sim.disk().list("wal/").unwrap();

    // Flip one payload byte of record 3 (segment 0, frame 3).
    let segment = sim.disk().open(&before[0]).unwrap();
    let offset = 3 * 316 + 16 + 100;
    let mut byte = [0u8; 1];
    segment.read_at(offset, &mut byte).unwrap();
    segment.write_at(offset, &[byte[0] ^ 0xFF]).unwrap();
    segment.sync().unwrap();

    let engine =
        Engine::recover(Box::new(sim.disk()), small_segments(), Ledger::default()).unwrap();
    assert_eq!(
        engine.machine().records,
        records[..3],
        "the cut lands on the flipped frame"
    );

    let after = sim.disk().list("wal/").unwrap();
    assert_eq!(
        after,
        vec![before[0].clone()],
        "later segments hold only post-cut data and must go"
    );
    let tail = sim.disk().open(&after[0]).unwrap();
    assert_eq!(
        tail.len().unwrap(),
        3 * 316,
        "recovery truncates the damaged segment at the cut"
    );
}

#[test]
fn recovered_log_accepts_appends_after_a_cut() {
    let sim = quiet_sim();
    build_log(&sim, &[b"one", b"two", b"three"]);

    // Damage the last frame's checksum byte.
    let segments = sim.disk().list("wal/").unwrap();
    let file = sim.disk().open(&segments[0]).unwrap();
    let last_frame = 2 * (16 + 3) as u64;
    file.write_at(last_frame + 12, &[0xFF]).unwrap();
    file.sync().unwrap();

    let mut engine =
        Engine::recover(Box::new(sim.disk()), small_segments(), Ledger::default()).unwrap();
    assert_eq!(
        engine.machine().records,
        vec![b"one".to_vec(), b"two".to_vec()]
    );
    assert_eq!(
        engine.submit(b"three-again").unwrap(),
        2,
        "seq continues from the cut"
    );

    let engine =
        Engine::recover(Box::new(sim.disk()), small_segments(), Ledger::default()).unwrap();
    assert_eq!(engine.machine().records[2], b"three-again");
}

#[test]
fn foreign_file_in_the_log_directory_is_refused_by_name() {
    let sim = quiet_sim();
    build_log(&sim, &[b"x"]);
    sim.disk().open("wal/notes.txt").unwrap();

    let Err(err) = Engine::recover(Box::new(sim.disk()), small_segments(), Ledger::default())
    else {
        panic!("recovery must refuse a log directory holding foreign files");
    };
    let msg = err.to_string();
    assert!(
        msg.contains("wal/notes.txt") && msg.contains("refusing"),
        "{msg}"
    );
}

#[test]
fn segment_gap_is_refused_with_the_expected_seq() {
    let sim = quiet_sim();
    let records: Vec<Vec<u8>> = (0..40u8).map(|i| vec![i; 300]).collect();
    let refs: Vec<&[u8]> = records.iter().map(|r| r.as_slice()).collect();
    build_log(&sim, &refs);
    let segments = sim.disk().list("wal/").unwrap();
    assert!(segments.len() >= 2);
    sim.disk().remove(&segments[0]).unwrap();

    let Err(err) = Engine::recover(Box::new(sim.disk()), small_segments(), Ledger::default())
    else {
        panic!("recovery must refuse a log with a missing segment");
    };
    let msg = err.to_string();
    assert!(msg.contains("seq 0 was expected"), "{msg}");
}

#[test]
fn empty_directory_starts_at_seq_zero() {
    let sim = quiet_sim();
    let mut engine =
        Engine::recover(Box::new(sim.disk()), small_segments(), Ledger::default()).unwrap();
    assert_eq!(engine.last_applied(), None);
    assert_eq!(engine.submit(b"first").unwrap(), 0);
}

#[test]
#[should_panic(expected = "WAL records are 1..=")]
fn empty_records_are_a_contract_violation() {
    let sim = quiet_sim();
    let mut wal = Wal::open(Box::new(sim.disk()), small_segments(), |_, _| {}).unwrap();
    let _ = wal.append(b"");
}
