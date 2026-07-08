//! WAL + apply-loop crash-recovery properties (PLAN.md M0 step 2), under the
//! full fault matrix with 4 KiB segments so rotation happens constantly.
//!
//! The standing invariant: after any crash, the machine recovers to exactly
//! `fold(apply, <prefix of submitted records>)`, the prefix is at least every
//! acknowledged record, and nothing is ever corrupt, reordered, or invented.

use std::io;

use charpente_core::engine::{Engine, StateMachine};
use charpente_core::io::{Disk, Entropy};
use charpente_core::log::{Wal, WalConfig};
use charpente_sim::{
    FaultConfig, Sim, run_universes, run_universes_swarm, run_universes_swarm_with,
};

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
    fn apply(&mut self, seq: u64, payload: &[u8]) -> io::Result<()> {
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
        Ok(())
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
        let mut engine = match Engine::recover(
            Box::new(sim.disk()),
            small_segments(),
            Ledger::default(),
            None,
        ) {
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
        match Engine::recover(
            Box::new(sim.disk()),
            small_segments(),
            Ledger::default(),
            None,
        ) {
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
        replay.apply(seq as u64, record).unwrap();
    }
    if replay.hash != engine.machine().hash {
        return Err("machine hash diverged from a clean replay of its own records".to_string());
    }
    Ok(())
}

#[test]
fn wal_survives_the_fault_matrix() {
    run_universes_swarm!(
        512,
        |sim: &mut Sim| { wal_workload(sim, |entropy| 100 + (entropy.next_u64() % 1400) as usize) },
        require: [
            "fault:write_error",
            "fault:sync_error",
            "fault:crash",
            "crash:applied",
            "crash:torn",
            "crash:dropped",
            "restart",
        ]
    );
}

/// The fault matrix with dir-entry loss forced hot: every universe's
/// FaultConfig gets dir_entry_loss = 0.5 stamped on top of the swarm draw.
/// Acked records must still always recover — this is the meta-evidence that
/// the rotate/recovery dir barriers (log.rs) are load-bearing: commenting out
/// the rotate barrier turns this red (records vanish with the unsynced
/// segment name).
#[test]
fn wal_survives_dir_entry_loss() {
    run_universes_swarm_with!(
        0x01D_D1E5,
        256,
        |cfg: &mut FaultConfig| { cfg.dir_entry_loss = 0.5; },
        |sim| wal_workload(sim, |entropy| 100 + (entropy.next_u64() % 1400) as usize),
        require: ["fault:dir-entry", "restart"]
    );
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
    let mut engine = Engine::recover(
        Box::new(sim.disk()),
        small_segments(),
        Ledger::default(),
        None,
    )
    .unwrap();
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

    let engine = Engine::recover(
        Box::new(sim.disk()),
        small_segments(),
        Ledger::default(),
        None,
    )
    .unwrap();
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

    let engine = Engine::recover(
        Box::new(sim.disk()),
        small_segments(),
        Ledger::default(),
        None,
    )
    .unwrap();
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
fn corruption_below_the_horizon_is_a_hard_error_not_a_cut() {
    let sim = quiet_sim();
    // Same construction as bit_flip_cuts_the_log_there_and_deletes_later_segments.
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

    // Horizon 7 sits above the flipped frame (seq 3): the corruption is below
    // the store's durable horizon, so it must be a named hard error, never a cut.
    let err = Wal::open_with_horizon(Box::new(sim.disk()), small_segments(), Some(7), |_, _| {
        Ok(())
    })
    .unwrap_err();
    let msg = err.to_string();
    for needle in ["invalid", "durable horizon", "acknowledged", "restore"] {
        assert!(
            msg.contains(needle),
            "horizon error must name the contract: {msg}"
        );
    }

    // The hard-error attempt mutated nothing: reopening with horizon None
    // still cuts at frame 3 exactly as before.
    let engine = Engine::recover(
        Box::new(sim.disk()),
        small_segments(),
        Ledger::default(),
        None,
    )
    .unwrap();
    assert_eq!(
        engine.machine().records,
        records[..3],
        "the cut still lands on the flipped frame — the hard-error attempt mutated nothing"
    );
    let after = sim.disk().list("wal/").unwrap();
    assert_eq!(
        after,
        vec![before[0].clone()],
        "the real recovery still deletes later segments after the cut"
    );
}

#[test]
fn corruption_above_the_horizon_still_cuts() {
    let sim = quiet_sim();
    let records: Vec<Vec<u8>> = (0..40u8).map(|i| vec![i; 300]).collect();
    let refs: Vec<&[u8]> = records.iter().map(|r| r.as_slice()).collect();
    build_log(&sim, &refs);
    let before = sim.disk().list("wal/").unwrap();

    let segment = sim.disk().open(&before[0]).unwrap();
    let offset = 3 * 316 + 16 + 100;
    let mut byte = [0u8; 1];
    segment.read_at(offset, &mut byte).unwrap();
    segment.write_at(offset, &[byte[0] ^ 0xFF]).unwrap();
    segment.sync().unwrap();

    // Horizon 2 sits below the flipped frame (seq 3): recovery cuts there
    // exactly as Wal::open (horizon None) does today, recovering seqs 0..=2.
    let mut recovered: Vec<(u64, Vec<u8>)> = Vec::new();
    Wal::open_with_horizon(
        Box::new(sim.disk()),
        small_segments(),
        Some(2),
        |seq, payload| {
            recovered.push((seq, payload.to_vec()));
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(
        recovered.iter().map(|(s, _)| *s).collect::<Vec<_>>(),
        vec![0, 1, 2],
        "recovery cuts at the flipped frame and recovers only seqs 0..=2"
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

    let mut engine = Engine::recover(
        Box::new(sim.disk()),
        small_segments(),
        Ledger::default(),
        None,
    )
    .unwrap();
    assert_eq!(
        engine.machine().records,
        vec![b"one".to_vec(), b"two".to_vec()]
    );
    assert_eq!(
        engine.submit(b"three-again").unwrap(),
        2,
        "seq continues from the cut"
    );

    let engine = Engine::recover(
        Box::new(sim.disk()),
        small_segments(),
        Ledger::default(),
        None,
    )
    .unwrap();
    assert_eq!(engine.machine().records[2], b"three-again");
}

#[test]
fn foreign_file_in_the_log_directory_is_refused_by_name() {
    let sim = quiet_sim();
    build_log(&sim, &[b"x"]);
    sim.disk().open("wal/notes.txt").unwrap();

    let Err(err) = Engine::recover(
        Box::new(sim.disk()),
        small_segments(),
        Ledger::default(),
        None,
    ) else {
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
    // Removing the FIRST segment is no longer a gap: since `gc`, recovery
    // starts at whichever segment is oldest, so that looks like a legitimate
    // gc'd floor (covered by the gc tests below). A genuine gap is a MIDDLE
    // segment going missing — the contiguity check still catches that.
    let sim = quiet_sim();
    let records: Vec<Vec<u8>> = (0..40u8).map(|i| vec![i; 300]).collect();
    let refs: Vec<&[u8]> = records.iter().map(|r| r.as_slice()).collect();
    build_log(&sim, &refs);
    let segments = sim.disk().list("wal/").unwrap();
    assert!(segments.len() >= 3, "need a middle segment to remove");
    sim.disk().remove(&segments[1]).unwrap();

    let Err(err) = Engine::recover(
        Box::new(sim.disk()),
        small_segments(),
        Ledger::default(),
        None,
    ) else {
        panic!("recovery must refuse a log with a missing segment");
    };
    let msg = err.to_string();
    assert!(msg.contains("seq 12 was expected"), "{msg}");
}

#[test]
fn empty_directory_starts_at_seq_zero() {
    let sim = quiet_sim();
    let mut engine = Engine::recover(
        Box::new(sim.disk()),
        small_segments(),
        Ledger::default(),
        None,
    )
    .unwrap();
    assert_eq!(engine.last_applied(), None);
    assert_eq!(engine.submit(b"first").unwrap(), 0);
}

#[test]
#[should_panic(expected = "WAL records are 1..=")]
fn empty_records_are_a_contract_violation() {
    let sim = quiet_sim();
    let mut wal = Wal::open(Box::new(sim.disk()), small_segments(), |_, _| Ok(())).unwrap();
    let _ = wal.append(b"");
}

/// A machine that refuses one specific seq — the hook for proving the
/// engine's poison-on-apply-failure contract.
struct Balky {
    ledger: Ledger,
    refuse_seq: Option<u64>,
}

impl StateMachine for Balky {
    fn apply(&mut self, seq: u64, payload: &[u8]) -> io::Result<()> {
        if self.refuse_seq == Some(seq) {
            return Err(io::Error::other("injected apply failure"));
        }
        self.ledger.apply(seq, payload)
    }
}

#[test]
fn apply_failure_poisons_the_engine_until_recovery() {
    let sim = quiet_sim();
    let mut engine = Engine::recover(
        Box::new(sim.disk()),
        small_segments(),
        Balky {
            ledger: Ledger::default(),
            refuse_seq: Some(1),
        },
        None,
    )
    .unwrap();
    engine.submit(b"zero").unwrap();

    let err = engine.submit(b"one").unwrap_err();
    assert_eq!(err.to_string(), "injected apply failure");

    // Poisoned: even a healthy record must be refused now.
    let err = engine.submit(b"two").unwrap_err();
    assert!(err.to_string().contains("restart and recover"), "{err}");

    // The refused record was durable before apply failed: recovery (with a
    // machine that accepts it) surfaces it — durable but unacknowledged.
    let engine = Engine::recover(
        Box::new(sim.disk()),
        small_segments(),
        Balky {
            ledger: Ledger::default(),
            refuse_seq: None,
        },
        None,
    )
    .unwrap();
    assert_eq!(
        engine.machine().ledger.records,
        vec![b"zero".to_vec(), b"one".to_vec()]
    );
}

// ------------------------------------------------------- dir-entry barrier

#[test]
fn unsynced_segment_name_vanishes_at_crash_without_a_dir_barrier() {
    // Raw-disk test: create a file, sync its BYTES but not its NAME, crash
    // with dir_entry_loss = 1.0 — the file must be gone.
    let cfg = FaultConfig {
        dir_entry_loss: 1.0,
        crash_apply: 1.0,
        ..FaultConfig::none()
    };
    let sim = Sim::with_faults(0xD1E_0001, cfg);
    let disk = sim.disk();
    let f = disk.open("wal/00000000000000000000.log").unwrap();
    f.write_at(0, b"acked").unwrap();
    f.sync().unwrap();
    sim.crash_disk_now();
    sim.restart();
    assert!(
        disk.list("wal/").unwrap().is_empty(),
        "unsynced dirent must be lost"
    );

    // Same again, but with the barrier — the file must survive.
    let sim = Sim::with_faults(
        0xD1E_0002,
        FaultConfig {
            dir_entry_loss: 1.0,
            crash_apply: 1.0,
            ..FaultConfig::none()
        },
    );
    let disk = sim.disk();
    let f = disk.open("wal/00000000000000000000.log").unwrap();
    f.write_at(0, b"acked").unwrap();
    f.sync().unwrap();
    disk.sync_dir("wal/").unwrap();
    sim.crash_disk_now();
    sim.restart();
    assert_eq!(
        disk.list("wal/").unwrap().len(),
        1,
        "barriered dirent must survive"
    );
}

#[test]
fn unsynced_removal_resurrects_at_crash_without_a_dir_barrier() {
    let cfg = FaultConfig {
        dir_entry_loss: 1.0,
        crash_apply: 1.0,
        ..FaultConfig::none()
    };
    let sim = Sim::with_faults(0xD1E_0003, cfg);
    let disk = sim.disk();
    let f = disk.open("wal/00000000000000000000.log").unwrap();
    f.write_at(0, b"stale").unwrap();
    f.sync().unwrap();
    disk.sync_dir("wal/").unwrap();
    disk.remove("wal/00000000000000000000.log").unwrap();
    sim.crash_disk_now();
    sim.restart();
    let names = disk.list("wal/").unwrap();
    assert_eq!(names.len(), 1, "unsynced removal must resurrect the file");
    let f = disk.open(&names[0]).unwrap();
    let mut out = [0u8; 5];
    f.read_at(0, &mut out).unwrap();
    assert_eq!(
        &out, b"stale",
        "resurrection restores the last durable content"
    );
}

// ------------------------------------------------------------------- gc

fn tiny_segments() -> WalConfig {
    WalConfig {
        dir: "wal".to_string(),
        segment_bytes: 64,
    }
}

fn base_of(name: &str) -> u64 {
    name.strip_prefix("wal/")
        .and_then(|s| s.strip_suffix(".log"))
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| panic!("not a segment name: {name}"))
}

#[test]
fn gc_deletes_only_whole_segments_below_keep_from() {
    // 64-byte segments, 8-byte payloads -> 24-byte frames, 2 per segment;
    // rotation happens every ~2 records.
    let sim = quiet_sim();
    let mut wal = Wal::open(Box::new(sim.disk()), tiny_segments(), |_, _| Ok(())).unwrap();
    let records: Vec<Vec<u8>> = (0..40u8).map(|i| vec![i; 8]).collect();
    for record in &records {
        wal.append(record).unwrap();
    }
    wal.sync().unwrap();

    wal.gc(20, 1).unwrap();

    let names = sim.disk().list("wal/").unwrap();
    assert!(!names.is_empty());
    let first_base = base_of(&names[0]);
    assert!(
        first_base <= 20,
        "the retained floor must start at or below keep_from"
    );
    assert_eq!(wal.base_seq(), first_base);

    let mut recovered: Vec<(u64, Vec<u8>)> = Vec::new();
    Wal::open(Box::new(sim.disk()), tiny_segments(), |seq, payload| {
        recovered.push((seq, payload.to_vec()));
        Ok(())
    })
    .unwrap();

    assert_eq!(recovered.len(), 40 - first_base as usize);
    assert_eq!(recovered.first().unwrap().0, first_base);
    assert_eq!(recovered.last().unwrap().0, 39);
    for (seq, payload) in &recovered {
        assert_eq!(
            *payload, records[*seq as usize],
            "seq {seq} content mismatch"
        );
    }
}

#[test]
fn gc_respects_the_retain_floor() {
    let sim = quiet_sim();
    let mut wal = Wal::open(Box::new(sim.disk()), tiny_segments(), |_, _| Ok(())).unwrap();
    let records: Vec<Vec<u8>> = (0..40u8).map(|i| vec![i; 8]).collect();
    for record in &records {
        wal.append(record).unwrap();
    }
    wal.sync().unwrap();

    wal.gc(u64::MAX, 3).unwrap();

    assert!(
        sim.disk().list("wal/").unwrap().len() >= 3,
        "floor holds even when keep_from allows more"
    );
}

/// One universe: fill a tiny-segment log, gc at a random floor, crash right
/// after, reopen. The barrier-after-each-removal design means a crash can
/// resurrect at most the most-recently-removed segment — always adjacent to
/// the retained set, never a gap. `dir_entry_loss` is forced hot so
/// resurrection is common, not rare.
fn gc_crash_workload(sim: &mut Sim) -> Result<(), String> {
    let entropy = sim.entropy();

    // Even opening a fresh (empty-dir) log barriers a directory entry, so
    // this can transiently fail under injected sync errors — unrelated to
    // gc, and handled the same way as any other transient recovery failure
    // elsewhere in this file: restart and retry.
    let mut opened = None;
    for _ in 0..8 {
        match Wal::open(Box::new(sim.disk()), tiny_segments(), |_, _| Ok(())) {
            Ok(wal) => {
                opened = Some(wal);
                break;
            }
            Err(_) => sim.restart(),
        }
    }
    let mut wal = opened.ok_or("initial open failed 8 times in a row — not transient faults")?;

    let n = 20 + entropy.next_u64() % 40;
    let mut submitted: Vec<Vec<u8>> = Vec::new();
    let mut acked = 0usize;
    for i in 0..n {
        let payload = vec![(i % 251) as u8; 8];
        if wal.append(&payload).is_err() {
            break; // never written at all
        }
        // Pushed before the sync outcome is known: once the bytes are
        // written, a crash can still make them durable (crash_apply doesn't
        // care whether the app's own `sync` call happened to fail) — only a
        // failed `append` means the bytes truly never existed.
        submitted.push(payload);
        if wal.sync().is_err() {
            break; // durability of this record (and anything later) is unknown
        }
        acked += 1;
    }

    if !wal.poisoned() {
        let keep_from = entropy.next_u64() % (submitted.len() as u64 + 1);
        // gc itself can hit an injected fault mid-loop (poisoning the wal,
        // fsyncgate-style) — that is expected and exactly what this property
        // must survive, not an error to propagate. Note the in-memory
        // base_seq is NOT a bound on the post-crash floor: a remove can
        // succeed while its following barrier fails (base_seq unadvanced),
        // and the crash coin can still make that removal stick — the floor
        // then legitimately lands above base_seq. Gap-freedom is the
        // property, and the reopen's "a segment is missing" check owns it.
        let _ = wal.gc(keep_from, 2);
    }

    sim.crash_disk_now();
    sim.restart();

    let mut recovered: Option<Vec<(u64, Vec<u8>)>> = None;
    for _attempt in 0..8 {
        let mut records: Vec<(u64, Vec<u8>)> = Vec::new();
        match Wal::open(Box::new(sim.disk()), tiny_segments(), |seq, payload| {
            records.push((seq, payload.to_vec()));
            Ok(())
        }) {
            Ok(_) => {
                recovered = Some(records);
                break;
            }
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("a segment is missing") {
                    return Err(format!("gc left a gap after crash: {msg}"));
                }
                sim.restart(); // transient fault on recovery itself — retry
            }
        }
    }
    let records =
        recovered.ok_or("final recovery failed 8 times in a row — not transient faults")?;

    for (i, (seq, payload)) in records.iter().enumerate() {
        if i > 0 && *seq != records[i - 1].0 + 1 {
            return Err(format!(
                "recovered seqs not contiguous: {seq} follows {}",
                records[i - 1].0
            ));
        }
        let want = submitted
            .get(*seq as usize)
            .ok_or_else(|| format!("recovered seq {seq} was never submitted"))?;
        if payload != want {
            return Err(format!("record {seq} corrupt after recovery"));
        }
    }
    if acked > 0 {
        let last_acked_seq = acked as u64 - 1;
        let recovered_last = records.last().map(|(s, _)| *s);
        if recovered_last.is_none_or(|s| s < last_acked_seq) {
            return Err(format!(
                "durability violated: last acked seq {last_acked_seq} not recovered (got {recovered_last:?})"
            ));
        }
    }
    Ok(())
}

#[test]
fn gc_interrupted_by_crash_never_leaves_a_gap() {
    run_universes_swarm_with!(
        0x6C_0001,
        256,
        |cfg: &mut FaultConfig| {
            cfg.dir_entry_loss = 0.5;
        },
        gc_crash_workload,
        require: ["restart"]
    );
}

#[test]
fn recovery_propagates_apply_failure() {
    let sim = quiet_sim();
    build_log(&sim, &[b"x"]);
    let err = Engine::recover(
        Box::new(sim.disk()),
        small_segments(),
        Balky {
            ledger: Ledger::default(),
            refuse_seq: Some(0),
        },
        None,
    )
    .unwrap_err();
    assert_eq!(err.to_string(), "injected apply failure");
}
