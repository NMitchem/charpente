//! WAL + apply-loop crash-recovery properties (PLAN.md M0 step 2), under the
//! full fault matrix with 4 KiB segments so rotation happens constantly.
//!
//! The standing invariant: after any crash, the machine recovers to exactly
//! `fold(apply, <prefix of submitted records>)`, the prefix is at least every
//! acknowledged record, and nothing is ever corrupt, reordered, or invented.

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use charpente_core::engine::{Engine, StateMachine};
use charpente_core::io::{Disk, Entropy, File};
use charpente_core::log::{MAX_RECORD_BYTES, Wal, WalConfig, frame_crc};
use charpente_sim::checks::is_fault_fallout;
use charpente_sim::{
    FaultConfig, Sim, SimDisk, run_universes, run_universes_swarm, run_universes_swarm_with,
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
/// every choice seeded. `payload_len` picks the size distribution. Threads
/// the durable horizon (`acked - 1`, mirroring `Db::open`'s use of the
/// store's `applied_seq` — segment-mgmt T4/T5) into every recovery: once
/// `bit_flip`/`misdirected_write` can corrupt already-durable bytes, a
/// horizon-less recovery could silently accept fewer than `acked` records
/// (an ordinary cut, since nothing tells it those bytes were ever
/// acknowledged) — exactly the bug the horizon rule exists to convert into
/// a named, loud error instead.
fn wal_workload(sim: &mut Sim, payload_len: impl Fn(&dyn Entropy) -> usize) -> Result<(), String> {
    let entropy = sim.entropy();
    let mut submitted: Vec<Vec<u8>> = Vec::new();
    let mut acked = 0usize;

    for _round in 0..4 {
        sim.crash_disk_after(entropy.next_u64() % 96);
        let horizon = acked.checked_sub(1).map(|s| s as u64);
        let mut engine = match Engine::recover(
            Box::new(sim.disk()),
            small_segments(),
            Ledger::default(),
            horizon,
        ) {
            Ok(engine) => engine,
            Err(e) => {
                let msg = e.to_string();
                if !is_fault_fallout(&msg) {
                    return Err(format!("recovery failed with a non-fallout error: {msg}"));
                }
                // The horizon hard error is permanent — the corrupted bytes
                // don't un-corrupt on retry, and DESIGN §2's own remedy
                // (restore from a replica or backup) is out of scope for
                // this workload — so the universe ends here, successfully:
                // the fence held, and every acked record has already
                // cleared `check_prefix` in an earlier round.
                if msg.contains("durable horizon") {
                    return Ok(());
                }
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
    // so the real-world response is restart-and-retry — except the horizon
    // hard error, which is permanent (see above) and ends the universe here.
    // `read_error` is transient but independently re-rolled per read, so a
    // long run of injected read errors across a big log's full scan can
    // outlast a small attempt budget without ever indicating a real bug —
    // same as `transactions.rs`'s final-recovery convention, a budget
    // exhausted entirely by recognized fault-fallout ends the universe here
    // too (every acked record has already cleared `check_prefix` up to the
    // last successful round).
    let mut recovered = None;
    for _attempt in 0..32 {
        let horizon = acked.checked_sub(1).map(|s| s as u64);
        match Engine::recover(
            Box::new(sim.disk()),
            small_segments(),
            Ledger::default(),
            horizon,
        ) {
            Ok(engine) => {
                recovered = Some(engine);
                break;
            }
            Err(e) => {
                if !is_fault_fallout(&e.to_string()) {
                    return Err(format!(
                        "final recovery failed with a non-fallout error: {e}"
                    ));
                }
                sim.restart();
            }
        }
    }
    let Some(engine) = recovered else {
        return Ok(()); // every attempt died to recognized faults (incl. the horizon fence)
    };
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

// ------------------------------------------- fault-matrix extensions (T5)
//
// Three axes new to this task — bit-level corruption of durable bytes,
// misdirected writes, and latent read errors (DESIGN §1) — each gets two
// properties: a meta-test proving the harness catches a deliberately naive
// victim (the safety equipment itself, checked), and an always-hot property
// proving the REAL `Wal` + recovery-horizon rule survives the axis (the
// system under test).

/// Like `wal_workload`, but a single round and an explicit horizon: build a
/// log, ack it fully, restart (the forced axis may fire), then recover with
/// horizon = the last acked seq — mirroring `Db::open`'s production use of
/// the store's `applied_seq` as the horizon threaded into `Engine::recover`.
/// Never silent loss: either every acked record comes back, or recovery
/// fails with a recognized fault-fallout error (the horizon hard error and
/// the injected read error both count — `checks::is_fault_fallout`
/// classifies both).
fn wal_survives_hot_axis(sim: &mut Sim) -> Result<(), String> {
    let entropy = sim.entropy();
    let mut engine = match Engine::recover(
        Box::new(sim.disk()),
        small_segments(),
        Ledger::default(),
        None,
    ) {
        Ok(engine) => engine,
        Err(e) => {
            let msg = e.to_string();
            return if is_fault_fallout(&msg) {
                Ok(())
            } else {
                Err(format!(
                    "initial open failed with a non-fallout error: {msg}"
                ))
            };
        }
    };

    let n = 4 + entropy.next_u64() % 20;
    let mut submitted: Vec<Vec<u8>> = Vec::new();
    let mut acked = 0usize;
    for _ in 0..n {
        let mut payload = vec![0u8; 100 + (entropy.next_u64() % 900) as usize];
        entropy.fill(&mut payload);
        submitted.push(payload.clone());
        match engine.submit(&payload) {
            Ok(seq) => {
                if seq != acked as u64 {
                    return Err(format!("submit returned seq {seq}, expected {acked}"));
                }
                acked += 1;
            }
            Err(e) => {
                if !is_fault_fallout(&e.to_string()) {
                    return Err(format!("submit failed with a non-fallout error: {e}"));
                }
                break;
            }
        }
    }
    drop(engine);
    sim.restart(); // the forced axis (and any swarm-sampled ones) may fire here

    let horizon = acked.checked_sub(1).map(|s| s as u64);
    match Engine::recover(
        Box::new(sim.disk()),
        small_segments(),
        Ledger::default(),
        horizon,
    ) {
        Ok(engine) => check_prefix(&engine.machine().records, &submitted, acked),
        Err(e) => {
            let msg = e.to_string();
            if is_fault_fallout(&msg) {
                Ok(()) // the fence held (or an ordinary transient fault) — not silent loss
            } else {
                Err(format!(
                    "recovery failed with a non-fault-fallout error: {msg}"
                ))
            }
        }
    }
}

#[test]
fn wal_survives_bit_flip() {
    run_universes_swarm_with!(
        0xB17_F11D_0100,
        256,
        |cfg: &mut FaultConfig| {
            cfg.bit_flip = 0.1;
        },
        wal_survives_hot_axis,
        require: ["fault:bit_flip"]
    );
}

#[test]
fn wal_survives_misdirected_write() {
    run_universes_swarm_with!(
        0x1DEC_7ED0_0200,
        256,
        |cfg: &mut FaultConfig| {
            cfg.misdirected_write = 0.1;
        },
        wal_survives_hot_axis,
        require: ["fault:misdirected_write"]
    );
}

#[test]
fn wal_survives_read_error() {
    run_universes_swarm_with!(
        0x2EAD_0300,
        256,
        |cfg: &mut FaultConfig| {
            cfg.read_error = 0.1;
        },
        wal_survives_hot_axis,
        require: ["fault:read_error"]
    );
}

// ---------------------------------------------- meta-tests: naive victims
//
// Each victim reuses a small, self-contained toy log format (independent of
// the real `Wal`'s frame layout) — the point is to prove the HARNESS (the
// fault injection + the property-test scanning idiom) reliably exposes each
// class of bug, not to re-implement the real `Wal`. Each forces exactly one
// axis hot and zeroes every other axis (`*cfg = FaultConfig::none()` first)
// so a catch is unambiguously attributable to the axis under test.
//
// Path convention: the bit_flip/misdirected_write victims live under "wal/"
// because those two axes are scoped to `FaultConfig::corruption_scope`
// (default "wal/" — Noah's ruling, 2026-07-07); the read_error victim
// deliberately lives under "toy/" because read errors are global, and its
// staying caught there is itself evidence of that.

fn toy_crc32(data: &[u8]) -> u32 {
    let mut crc = !0u32;
    for &byte in data {
        crc ^= u32::from(byte);
        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (0xEDB8_8320 & mask);
        }
    }
    !crc
}

/// Frame: [payload len: u32 le][crc32(payload): u32 le][payload] — same
/// shape as `crash_recovery.rs`'s toy log, reused here against a different
/// fault axis.
fn toy_append(file: &dyn File, end: u64, payload: &[u8]) -> io::Result<u64> {
    let mut frame = Vec::with_capacity(8 + payload.len());
    frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    frame.extend_from_slice(&toy_crc32(payload).to_le_bytes());
    frame.extend_from_slice(payload);
    file.write_at(end, &frame)?;
    file.sync()?;
    Ok(end + frame.len() as u64)
}

/// The victim for the bit_flip meta-test: trusts the length prefix, never
/// re-checks the checksum — crash_recovery.rs's naive recovery (`checked =
/// false`), reused here to demonstrate the same class of bug against
/// bit-level durable corruption instead of torn writes.
fn toy_recover_naive(disk: &dyn Disk, path: &str) -> io::Result<Vec<Vec<u8>>> {
    let file = disk.open(path)?;
    let len = file.len()?;
    let mut records = Vec::new();
    let mut pos = 0u64;
    loop {
        if pos + 8 > len {
            break;
        }
        let mut header = [0u8; 8];
        file.read_at(pos, &mut header)?;
        let payload_len = u64::from(u32::from_le_bytes(header[0..4].try_into().unwrap()));
        if payload_len == 0 || pos + 8 + payload_len > len {
            break;
        }
        let mut payload = vec![0u8; payload_len as usize];
        file.read_at(pos + 8, &mut payload)?;
        records.push(payload);
        pos += 8 + payload_len;
    }
    Ok(records)
}

/// Append a small toy log (all synced), restart (bit_flip may fire), then
/// recover with the naive length-trusting reader. A flipped payload byte
/// keeps the frame's length intact — the naive reader accepts it and
/// silently returns the wrong bytes.
fn bit_flip_naive_recovery_victim(sim: &mut Sim) -> Result<(), String> {
    let disk = sim.disk();
    let entropy = sim.entropy();
    let file = disk.open("wal/naive.log").map_err(|e| e.to_string())?;
    let mut end = 0u64;
    let mut submitted = Vec::new();
    for _ in 0..8 {
        let mut payload = vec![0u8; 200 + (entropy.next_u64() % 800) as usize];
        entropy.fill(&mut payload);
        end = toy_append(&*file, end, &payload).map_err(|e| e.to_string())?;
        submitted.push(payload);
    }
    drop(file);
    sim.restart(); // bit_flip forced hot: may corrupt one durable byte

    let recovered = toy_recover_naive(&disk, "wal/naive.log").map_err(|e| e.to_string())?;
    if recovered.len() != submitted.len() {
        return Err(format!(
            "naive recovery lost records: {} recovered, {} appended",
            recovered.len(),
            submitted.len()
        ));
    }
    for (i, (got, want)) in recovered.iter().zip(&submitted).enumerate() {
        if got != want {
            return Err(format!(
                "record {i} corrupt: naive length-trusting recovery accepted a bit-flipped payload"
            ));
        }
    }
    Ok(())
}

#[test]
fn harness_catches_recovery_that_trusts_lengths_without_crc() {
    let payload = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        run_universes_swarm_with!(
            0xB17_F11D_0001,
            64,
            |cfg: &mut FaultConfig| {
                *cfg = FaultConfig::none();
                cfg.bit_flip = 0.25;
            },
            bit_flip_naive_recovery_victim
        );
    }))
    .expect_err(
        "the harness no longer catches naive length-only recovery under bit-flip corruption \
         within 64 seeds — the bit_flip fault injection itself has regressed",
    );
    let msg = payload
        .downcast_ref::<String>()
        .cloned()
        .unwrap_or_else(|| "<non-string panic payload>".to_string());
    eprintln!("caught naive length-only recovery: {msg}");
    assert!(msg.contains("seed:"), "{msg}");
    assert!(
        msg.contains("naive length-trusting recovery accepted a bit-flipped payload")
            || msg.contains("naive recovery lost records"),
        "{msg}"
    );
}

/// Victim for the misdirected-write meta-test: writes then simply trusts
/// the call's `Ok` — no read-back verification, which is exactly what
/// `misdirected_write`'s treachery defeats: the call still returns `Ok`
/// even though the payload landed elsewhere and the intended offset stays
/// stale.
fn never_reads_back_victim(sim: &mut Sim) -> Result<(), String> {
    let disk = sim.disk();
    let entropy = sim.entropy();
    let file = disk.open("wal/store.bin").map_err(|e| e.to_string())?;
    let mut expected = Vec::new();
    for i in 0..16u64 {
        let offset = i * 64;
        let mut record = vec![0u8; 32];
        entropy.fill(&mut record);
        file.write_at(offset, &record).map_err(|e| e.to_string())?; // still Ok — the treachery
        expected.push((offset, record));
    }
    file.sync().map_err(|e| e.to_string())?;

    // The audit: read back what a "trust the write" store never checks.
    for (offset, want) in &expected {
        let mut got = vec![0u8; want.len()];
        file.read_at(*offset, &mut got).map_err(|e| e.to_string())?;
        if &got != want {
            return Err(format!(
                "misdirected write: offset {offset} landed wrong — a store that never reads \
                 back would have believed this write succeeded"
            ));
        }
    }
    Ok(())
}

#[test]
fn harness_catches_a_store_that_never_reads_back() {
    let payload = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        run_universes_swarm_with!(
            0x1DEC_7ED0_0002,
            64,
            |cfg: &mut FaultConfig| {
                *cfg = FaultConfig::none();
                cfg.misdirected_write = 0.25;
            },
            never_reads_back_victim
        );
    }))
    .expect_err(
        "the harness no longer catches a never-reads-back store under misdirected writes \
         within 64 seeds — the misdirected_write fault injection itself has regressed",
    );
    let msg = payload
        .downcast_ref::<String>()
        .cloned()
        .unwrap_or_else(|| "<non-string panic payload>".to_string());
    eprintln!("caught never-reads-back store: {msg}");
    assert!(msg.contains("seed:"), "{msg}");
    assert!(msg.contains("landed wrong"), "{msg}");
}

/// The victim for the read_error meta-test: recovery that treats ANY
/// `read_at` error (including a transient injected one) as a clean end of
/// the log — with `read_error` forced hot it silently drops acked records
/// instead of surfacing the error and retrying.
fn toy_recover_swallows_read_errors(disk: &dyn Disk, path: &str) -> io::Result<Vec<Vec<u8>>> {
    let file = disk.open(path)?;
    let len = file.len()?;
    let mut records = Vec::new();
    let mut pos = 0u64;
    loop {
        if pos + 8 > len {
            break;
        }
        let mut header = [0u8; 8];
        if file.read_at(pos, &mut header).is_err() {
            break; // the bug: any read error reads as "nothing more to recover"
        }
        let payload_len = u64::from(u32::from_le_bytes(header[0..4].try_into().unwrap()));
        let crc = u32::from_le_bytes(header[4..8].try_into().unwrap());
        if payload_len == 0 || pos + 8 + payload_len > len {
            break;
        }
        let mut payload = vec![0u8; payload_len as usize];
        if file.read_at(pos + 8, &mut payload).is_err() {
            break;
        }
        if toy_crc32(&payload) != crc {
            break;
        }
        records.push(payload);
        pos += 8 + payload_len;
    }
    Ok(records)
}

fn read_error_naive_recovery_victim(sim: &mut Sim) -> Result<(), String> {
    let disk = sim.disk();
    let entropy = sim.entropy();
    let file = disk.open("toy/checked.log").map_err(|e| e.to_string())?;
    let mut end = 0u64;
    let mut submitted = Vec::new();
    for _ in 0..8 {
        let mut payload = vec![0u8; 200 + (entropy.next_u64() % 800) as usize];
        entropy.fill(&mut payload);
        end = toy_append(&*file, end, &payload).map_err(|e| e.to_string())?;
        submitted.push(payload);
    }
    drop(file);

    let recovered =
        toy_recover_swallows_read_errors(&disk, "toy/checked.log").map_err(|e| e.to_string())?;
    if recovered.len() < submitted.len() {
        return Err(format!(
            "naive recovery silently dropped {} acked record(s) after treating an injected \
             read error as clean end",
            submitted.len() - recovered.len()
        ));
    }
    for (i, (got, want)) in recovered.iter().zip(&submitted).enumerate() {
        if got != want {
            return Err(format!("record {i} diverged after recovery"));
        }
    }
    Ok(())
}

#[test]
fn harness_catches_recovery_that_reads_through_errors_as_eof() {
    let payload = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        run_universes_swarm_with!(
            0x2EAD_0003,
            64,
            |cfg: &mut FaultConfig| {
                *cfg = FaultConfig::none();
                cfg.read_error = 0.25;
            },
            read_error_naive_recovery_victim
        );
    }))
    .expect_err(
        "the harness no longer catches read-error-as-EOF recovery within 64 seeds — the \
         read_error fault injection itself has regressed",
    );
    let msg = payload
        .downcast_ref::<String>()
        .cloned()
        .unwrap_or_else(|| "<non-string panic payload>".to_string());
    eprintln!("caught read-error-as-EOF recovery: {msg}");
    assert!(msg.contains("seed:"), "{msg}");
    assert!(msg.contains("silently dropped"), "{msg}");
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

/// A FAILED directory barrier leaves the segment's name pending, and until
/// 2026-09-07 nothing ever re-established it: `Wal::open_with_horizon`
/// barriers the prefix when it CREATES the first segment and again on the
/// recovery path that cuts or removes something, but the clean path — every
/// frame valid, nothing to truncate — fell out of the loop with no barrier at
/// all, while the comment on the cut-path barrier claims "recovery leaves a
/// durably-clean directory".
///
/// So a log could accept and ACKNOWLEDGE appends into a segment whose name was
/// never made durable, and a crash took the whole segment with them. Found end
/// to end by `sim-fixture`'s `acked_counter_survives_rounds` at seed
/// 0xb957ceaaf1716fd5 (docs/LEDGER.md 2026-09-07), where an injected
/// `sync_error` failed the create-path barrier in one round and the next round
/// reopened cleanly and acked into it.
///
/// The pending-name state is built directly rather than by injecting
/// `sync_error`, which at a rate high enough to fail the barrier would also
/// fail the `wal.sync()` this test needs to SUCCEED — the ack is the whole
/// premise.
#[test]
fn reopening_a_log_re_barriers_a_name_left_pending_by_a_failed_dir_sync() {
    let sim = Sim::with_faults(
        0xD1E_0010,
        FaultConfig {
            dir_entry_loss: 1.0,
            crash_apply: 1.0,
            ..FaultConfig::none()
        },
    );
    let disk = sim.disk();
    // Exactly what a failed `sync_dir` leaves behind: the file exists, its
    // directory entry is not durable.
    disk.open("wal/00000000000000000000.log").unwrap();

    let mut wal = Wal::open(Box::new(sim.disk()), small_segments(), |_, _| Ok(())).unwrap();
    wal.append(b"acked").unwrap();
    wal.sync().unwrap(); // the acknowledgement

    sim.crash_disk_now();
    sim.restart();
    assert_eq!(
        disk.list("wal/").unwrap().len(),
        1,
        "an acked append's segment vanished at crash: its name was left pending by a failed \
         barrier and reopening the log never re-established it"
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

    // Threads the durable horizon (`acked - 1`), same reasoning as
    // `wal_workload`: once bit_flip/misdirected_write can corrupt an
    // already-durable frame, a horizon-less reopen could silently accept
    // fewer than `acked` records instead of surfacing the named hard error.
    let horizon = acked.checked_sub(1).map(|s| s as u64);
    let mut recovered: Option<Vec<(u64, Vec<u8>)>> = None;
    for _attempt in 0..32 {
        let mut records: Vec<(u64, Vec<u8>)> = Vec::new();
        match Wal::open_with_horizon(
            Box::new(sim.disk()),
            tiny_segments(),
            horizon,
            |seq, payload| {
                records.push((seq, payload.to_vec()));
                Ok(())
            },
        ) {
            Ok(_) => {
                recovered = Some(records);
                break;
            }
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("a segment is missing") {
                    return Err(format!("gc left a gap after crash: {msg}"));
                }
                if !is_fault_fallout(&msg) {
                    return Err(format!(
                        "final reopen failed with a non-fallout error: {msg}"
                    ));
                }
                sim.restart(); // transient fault (or the horizon fence) — retry
            }
        }
    }
    let Some(records) = recovered else {
        return Ok(()); // every attempt died to recognized faults (incl. the horizon fence)
    };

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

// -------------------------------------- recovery-cut atomicity (interlude)
//
// Regression for the recovery-cut atomicity bug found via replication seed
// 0x4177819b4a2ba984: a non-tail cut used to truncate the cut segment BEFORE
// removing orphaned later segments. Truncation and removal are two separate
// durability events, and a crash that stranded the (now durable) truncation
// before the removals left a durable gap [shortened segment, surviving
// orphan] — which the gaplessness check then refuses to recover across,
// bricking the node on above-horizon data it was entitled to discard. The
// fix removes orphans first (highest base first, each barriered), truncating
// last, so every crash-intermediate is a contiguous prefix.
//
// This constructs the exact vulnerable state (a non-tail, above-horizon
// corrupt frame → a cut with orphaned later segments) and injects a crash at
// EVERY op index of recovery — including each durability point of the cut
// path — asserting recovery always completes idempotently to the pre-cut
// prefix and NEVER produces the gap refusal. `crash_apply = 1.0` is the
// investigation's mechanism precisely: an unsynced op fully persists on the
// power cycle (bytes reached the platter, fsync just never confirmed), so a
// truncation that outran its removals STAYS a durable gap — the property is
// RED against truncate-then-remove and green only once the order is fixed.

/// Builds a 10-segment log (tiny 64 B segments, 8 B payloads = 24 B frames, 2
/// per segment: seqs {0,1},{2,3},{4,5},…,{18,19}), then corrupts seq 5 (the
/// 2nd frame of segment base 4, a non-tail segment). Recovery with a horizon
/// below seq 5 then makes an above-horizon, non-tail cut at offset 24 of
/// segment base 4, with orphaned segments base 6..=18 to remove — the shape
/// the fix protects.
fn build_corrupted_multi_segment_log(dir_entry_loss: f64) -> Sim {
    let sim = Sim::with_faults(
        0x4C07_A701,
        FaultConfig {
            crash_apply: 1.0,
            dir_entry_loss,
            ..FaultConfig::none()
        },
    );
    {
        let mut wal = Wal::open(Box::new(sim.disk()), tiny_segments(), |_, _| Ok(())).unwrap();
        for i in 0..20u64 {
            wal.append(&[(i % 251) as u8; 8]).unwrap();
            wal.sync().unwrap();
        }
    }
    // Flip a durable, synced payload byte of seq 5 (segment base 4, 2nd frame
    // at offset 24; 16 B header, so the payload starts at 40). An
    // above-horizon non-tail cut, deterministically.
    let seg = sim.disk().open("wal/00000000000000000004.log").unwrap();
    let mut byte = [0u8; 1];
    seg.read_at(24 + 16, &mut byte).unwrap();
    seg.write_at(24 + 16, &[byte[0] ^ 0xFF]).unwrap();
    seg.sync().unwrap();
    sim
}

/// Recovers with horizon 4 (seq 5 sits above it, so the corruption is a cut,
/// not the below-horizon hard error), retrying restart-and-recover through
/// injected faults. Returns the recovered seqs. Asserts the negative space —
/// recovery must NEVER report the gap refusal (that would mean the cut left a
/// durable gap), and any other failure must be recognized fault fallout.
fn recover_prefix_or_gap(sim: &Sim, at: u64) -> Vec<u64> {
    for _ in 0..8 {
        let mut got = Vec::new();
        match Wal::open_with_horizon(Box::new(sim.disk()), tiny_segments(), Some(4), |seq, _| {
            got.push(seq);
            Ok(())
        }) {
            Ok(_) => return got,
            Err(e) => {
                let msg = e.to_string();
                assert!(
                    !(msg.contains("a segment is missing") || msg.contains("refusing to guess")),
                    "crash@{at}: recovery cut left a durable gap — orphaned segment survived a \
                     truncation: {msg}"
                );
                assert!(
                    is_fault_fallout(&msg),
                    "crash@{at}: unexpected non-fallout recovery error: {msg}"
                );
                sim.restart();
            }
        }
    }
    panic!("crash@{at}: recovery never completed in 8 restart attempts");
}

fn scan_recovery_cut_crash_points(dir_entry_loss: f64) {
    // Baseline (no crash): the cut recovers exactly seqs 0..=4 and removes the
    // seven orphaned segments, leaving bases 0, 2, 4 — the state every crashed
    // run must converge to.
    let sim = build_corrupted_multi_segment_log(dir_entry_loss);
    assert_eq!(recover_prefix_or_gap(&sim, u64::MAX), vec![0, 1, 2, 3, 4]);
    assert_eq!(
        sim.disk().list("wal/").unwrap().len(),
        3,
        "the cut removes every orphaned later segment (bases 0, 2, 4 remain)"
    );

    // Crash at every op index of recovery, spanning the whole cut path
    // (orphan removals, each barrier, the truncation, its sync). Each run must
    // finish idempotently at the same prefix and never gap.
    for at in 0..48u64 {
        let sim = build_corrupted_multi_segment_log(dir_entry_loss);
        sim.crash_disk_after(at);
        assert_eq!(
            recover_prefix_or_gap(&sim, at),
            vec![0, 1, 2, 3, 4],
            "crash@{at}: recovery must be idempotent to the pre-cut prefix"
        );
        // A second, clean recovery must see the same durable prefix — the
        // recovered directory is stable, not merely reachable once.
        assert_eq!(recover_prefix_or_gap(&sim, at), vec![0, 1, 2, 3, 4]);
    }
}

#[test]
fn recovery_cut_never_leaves_a_gap_at_any_crash_point() {
    // Un-barriered removes stick (dir_entry_loss = 0.0): the exact
    // investigation mechanism. Truncate-then-remove leaves the gap here.
    scan_recovery_cut_crash_points(0.0);
}

#[test]
fn recovery_cut_survives_orphan_resurrection() {
    // dir_entry_loss forced hot: a crash mid-removal resurrects the
    // most-recently-removed (un-barriered) orphan. Removing highest base first
    // and barriering each keeps the resurrected orphan adjacent to the
    // retained prefix — never a gap — and recovery re-removes it. The same
    // exposure `gc` already accepts (module docs), now proven for the cut path.
    scan_recovery_cut_crash_points(1.0);
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

// ------------------------------------------------------- read_from (repl T3)
//
// The primary-side shipper's read surface: a verified read-back of
// already-appended frames, never trusting anything it hasn't re-checksummed.

#[test]
fn read_from_returns_verified_frames_across_segments() {
    // 64-byte segments, 8-byte payloads -> 24-byte frames, 2 per segment —
    // 40 records forces many rotations (same shape as the gc tests above).
    let sim = quiet_sim();
    let mut wal = Wal::open(Box::new(sim.disk()), tiny_segments(), |_, _| Ok(())).unwrap();
    let records: Vec<Vec<u8>> = (0..40u8).map(|i| vec![i; 8]).collect();
    for (i, record) in records.iter().enumerate() {
        wal.append(record).unwrap();
        if i % 4 == 3 {
            wal.sync().unwrap();
        }
    }
    wal.sync().unwrap();

    let segments = sim.disk().list("wal/").unwrap();
    assert!(
        segments.len() > 1,
        "64-byte segments must have rotated: {segments:?}"
    );

    let got = wal.read_from(13, 27, usize::MAX).unwrap();
    assert_eq!(
        got.iter().map(|(s, _)| *s).collect::<Vec<_>>(),
        (13..=27).collect::<Vec<_>>(),
        "exactly seqs 13..=27"
    );
    for (seq, payload) in &got {
        assert_eq!(
            *payload, records[*seq as usize],
            "seq {seq} content mismatch"
        );
    }

    let capped = wal.read_from(0, 39, 64).unwrap();
    assert!(!capped.is_empty(), "must return at least one frame");
    assert_eq!(capped[0].0, 0, "starts at from_seq");
    let total: usize = capped.iter().map(|(_, p)| p.len()).sum();
    assert!(
        total >= 64,
        "stops only once max_bytes worth of payload has accumulated: {total}"
    );
    assert!(
        capped.len() < 40,
        "the 64-byte cap must have stopped it well short of the whole log"
    );
    for (seq, payload) in &capped {
        assert_eq!(
            *payload, records[*seq as usize],
            "seq {seq} content mismatch"
        );
    }
}

#[test]
fn read_from_refuses_a_corrupt_frame_below_up_to() {
    let sim = quiet_sim();
    let mut wal = Wal::open(Box::new(sim.disk()), small_segments(), |_, _| Ok(())).unwrap();
    let records: Vec<Vec<u8>> = (0..10u8).map(|i| vec![i; 8]).collect();
    for record in &records {
        wal.append(record).unwrap();
    }
    wal.sync().unwrap();

    // Flip one durable payload byte of frame 4 (24-byte frames: 16-byte
    // header + 8-byte payload; all 10 fit in one 4 KiB segment).
    let segments = sim.disk().list("wal/").unwrap();
    let segment = sim.disk().open(&segments[0]).unwrap();
    let offset = 4 * 24 + 16;
    let mut byte = [0u8; 1];
    segment.read_at(offset, &mut byte).unwrap();
    segment.write_at(offset, &[byte[0] ^ 0xFF]).unwrap();
    segment.sync().unwrap();

    let err = wal.read_from(0, 9, usize::MAX).unwrap_err();
    let msg = err.to_string();
    for needle in ["frame 4", "shipped", "restore"] {
        assert!(msg.contains(needle), "{msg}");
    }
}

#[test]
fn read_from_below_base_is_a_named_error() {
    let sim = quiet_sim();
    let mut wal = Wal::open(Box::new(sim.disk()), tiny_segments(), |_, _| Ok(())).unwrap();
    let records: Vec<Vec<u8>> = (0..40u8).map(|i| vec![i; 8]).collect();
    for record in &records {
        wal.append(record).unwrap();
    }
    wal.sync().unwrap();
    wal.gc(20, 1).unwrap();
    let base = wal.base_seq();
    assert!(base > 0, "gc must have advanced the floor");

    let err = wal.read_from(base - 1, base, usize::MAX).unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains(&base.to_string()), "must name the base: {msg}");
    assert!(
        msg.contains("snapshot"),
        "must name the snapshot path: {msg}"
    );
}

/// `parse_segment_name` guards on two separate operands — a 20-character length
/// and all-ASCII-digits — and only the digit half was pinned. The first real
/// finding from `charpente test --mutants` (2026-09-07) was that replacing the
/// `||` between them with `&&` left every test in the workspace green.
///
/// With `&&` a name is refused only when it is BOTH the wrong length AND
/// non-numeric, so a short all-digit name like `wal/123.log` parses as segment
/// 123 and the documented contract — "refusing to recover around foreign
/// files" — stops holding without a single test noticing. This pins the length
/// operand on its own, which is what the mutant proved was missing.
#[test]
fn a_short_all_digit_name_is_a_foreign_file_not_a_segment() {
    let sim = Sim::with_faults(0xD1E_0011, FaultConfig::none());
    let disk = sim.disk();
    disk.open("wal/00000000000000000000.log").unwrap();
    disk.open("wal/123.log").unwrap(); // all digits, wrong length

    let err = Wal::open(Box::new(sim.disk()), small_segments(), |_, _| Ok(()))
        .expect_err("a 3-digit name is not a segment name — opening must refuse, not adopt it");
    assert!(
        err.to_string()
            .contains("refusing to recover around foreign files"),
        "the refusal must name the contract it enforces: {err}"
    );
}

// ---------------------------------------------------------------------------
// Boundaries the 2026-09-10 mutation sweep found unpinned (spec
// docs/superpowers/specs/2026-09-12-wal-one-sided-properties-design.md §3).
// Each of these was watched going RED under its named mutant before landing.

#[test]
fn a_frame_that_exactly_fills_the_segment_stays_in_it() {
    // 64-byte segments, 16-byte payloads -> 32-byte frames: two fill a
    // segment to the byte. The rotation gate is `tail_offset + frame_len >
    // segment_bytes`; at exactly `==` the frame belongs to THIS segment, and
    // the `>=` mutant rotated it one record early with nothing noticing.
    let sim = quiet_sim();
    let mut wal = Wal::open(Box::new(sim.disk()), tiny_segments(), |_, _| Ok(())).unwrap();
    wal.append(&[1u8; 16]).unwrap();
    wal.append(&[2u8; 16]).unwrap();
    wal.sync().unwrap();
    let names = sim.disk().list("wal/").unwrap();
    assert_eq!(names.len(), 1, "an exact fit does not rotate: {names:?}");
    assert_eq!(
        sim.disk().open(&names[0]).unwrap().len().unwrap(),
        64,
        "the segment is full to the byte"
    );
    wal.append(&[3u8; 16]).unwrap();
    wal.sync().unwrap();
    let names = sim.disk().list("wal/").unwrap();
    assert_eq!(
        names.len(),
        2,
        "one byte over the capacity rotates: {names:?}"
    );
}

#[test]
fn read_from_includes_up_to_when_its_frame_opens_the_next_segment() {
    // The log of read_from_returns_verified_frames_across_segments: 8-byte
    // payloads, two 24-byte frames per 64-byte segment, bases 0, 2, 4, ….
    // `up_to = 26` is the FIRST frame of a segment, so the scan reaches the
    // clean end of [24, 25] with `seq == up_to` still unread — the boundary
    // the sibling's odd `up_to` never touches, and where both `>` mutants
    // (`==` and `>=`) break out of the segment loop and drop the last frame.
    let sim = quiet_sim();
    let mut wal = Wal::open(Box::new(sim.disk()), tiny_segments(), |_, _| Ok(())).unwrap();
    let records: Vec<Vec<u8>> = (0..40u8).map(|i| vec![i; 8]).collect();
    for record in &records {
        wal.append(record).unwrap();
    }
    wal.sync().unwrap();
    let names = sim.disk().list("wal/").unwrap();
    assert!(
        names.iter().any(|n| base_of(n) == 26),
        "26 must open a segment for this to be the boundary: {names:?}"
    );

    let got = wal.read_from(13, 26, usize::MAX).unwrap();
    assert_eq!(
        got.iter().map(|(s, _)| *s).collect::<Vec<_>>(),
        (13..=26).collect::<Vec<_>>(),
        "up_to is inclusive even when its frame opens the next segment"
    );
    assert_eq!(got.last().unwrap().1, records[26]);
}

#[test]
fn gc_deletes_the_segment_whose_successor_starts_at_keep_from() {
    // Bases 0, 2, 4, …: `gc(4, 1)` may delete [0,1] and [2,3] — every frame
    // in both is below 4 — and must stop at [4,5]. The sibling
    // gc_deletes_only_whole_segments_below_keep_from asserts only
    // `first_base <= keep_from`, which a gc that stops one segment early
    // satisfies; this pins the boundary exactly.
    let sim = quiet_sim();
    let mut wal = Wal::open(Box::new(sim.disk()), tiny_segments(), |_, _| Ok(())).unwrap();
    for i in 0..40u8 {
        wal.append(&[i; 8]).unwrap();
    }
    wal.sync().unwrap();

    wal.gc(4, 1).unwrap();

    let names = sim.disk().list("wal/").unwrap();
    assert_eq!(
        base_of(&names[0]),
        4,
        "the segment ending exactly at keep_from is deletable: {names:?}"
    );
    assert_eq!(wal.base_seq(), 4);
}

/// A disk whose files fail `sync` once, on demand, then behave. Poison is
/// one-shot by nature — after one failed fsync the durability of everything
/// since the last good one is unknown, and a later fsync that succeeds
/// cannot un-know it — so the property needs a disk that fails exactly once
/// and heals, a shape the fault matrix's `sync_error` RATE does not express.
/// Test-local on purpose (spec §6): not a fault mode, an injection.
struct FailOneSync {
    inner: SimDisk,
    /// Set by the test right before the sync that must fail; the file clears
    /// it as it fails, so every later sync goes through.
    arm: Arc<AtomicBool>,
}

struct FailOneSyncFile {
    inner: Box<dyn File>,
    arm: Arc<AtomicBool>,
}

impl Disk for FailOneSync {
    fn open(&self, path: &str) -> io::Result<Box<dyn File>> {
        Ok(Box::new(FailOneSyncFile {
            inner: self.inner.open(path)?,
            arm: self.arm.clone(),
        }))
    }
    fn remove(&self, path: &str) -> io::Result<()> {
        self.inner.remove(path)
    }
    fn list(&self, prefix: &str) -> io::Result<Vec<String>> {
        self.inner.list(prefix)
    }
    fn sync_dir(&self, prefix: &str) -> io::Result<()> {
        self.inner.sync_dir(prefix)
    }
}

impl File for FailOneSyncFile {
    fn len(&self) -> io::Result<u64> {
        self.inner.len()
    }
    fn read_at(&self, offset: u64, out: &mut [u8]) -> io::Result<()> {
        self.inner.read_at(offset, out)
    }
    fn write_at(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        self.inner.write_at(offset, data)
    }
    fn set_len(&self, len: u64) -> io::Result<()> {
        self.inner.set_len(len)
    }
    fn sync(&self) -> io::Result<()> {
        if self.arm.swap(false, Ordering::SeqCst) {
            return Err(io::Error::other("injected fsync failure"));
        }
        self.inner.sync()
    }
}

#[test]
fn a_failed_sync_poisons_the_log_and_every_later_call_is_refused() {
    // The WAL's own poison, as distinct from the Engine/Db flag the
    // apply_failure_poisons_the_engine_until_recovery test covers: the
    // accessor was read by no test, and check_poison's body could be
    // replaced with Ok(()) — a poisoned log that refuses nothing — with
    // nothing noticing (three mutants, two functions, 2026-09-10 sweep).
    let sim = quiet_sim();
    let arm = Arc::new(AtomicBool::new(false));
    let disk = FailOneSync {
        inner: sim.disk(),
        arm: arm.clone(),
    };
    let mut wal = Wal::open(Box::new(disk), tiny_segments(), |_, _| Ok(())).unwrap();
    wal.append(&[1u8; 8]).unwrap();
    assert!(!wal.poisoned(), "a healthy log is not poisoned");

    arm.store(true, Ordering::SeqCst);
    let err = wal.sync().unwrap_err();
    assert_eq!(err.to_string(), "injected fsync failure");
    assert!(wal.poisoned(), "one failed fsync poisons the log");
    assert!(
        !arm.load(Ordering::SeqCst),
        "the injection fired exactly once; the disk is healthy from here"
    );

    for (what, result) in [
        ("append", wal.append(&[2u8; 8]).map(|_| ())),
        ("sync", wal.sync()),
        ("gc", wal.gc(0, 1)),
    ] {
        let err = result.unwrap_err();
        assert!(
            err.to_string()
                .contains("WAL poisoned by an earlier IO failure"),
            "{what} after poison must be refused by name; got: {err}"
        );
    }
    assert!(wal.poisoned(), "refusals do not clear the poison");
}

/// One frame in the WAL's real format — `[seq u64 le][len u32 le][crc u32
/// le][payload]`, crc over seq‖len‖payload as `frame_crc` computes it — so a
/// hand-built log differs from an appended one only where the test says.
/// `declared_len` lets a frame LIE about its length; the crc is over the
/// bytes actually supplied, which is what recovery recomputes when it reads
/// `declared_len` bytes.
fn raw_frame(seq: u64, declared_len: u32, payload: &[u8]) -> Vec<u8> {
    let mut frame = Vec::with_capacity(16 + payload.len());
    frame.extend_from_slice(&seq.to_le_bytes());
    frame.extend_from_slice(&declared_len.to_le_bytes());
    frame.extend_from_slice(&frame_crc(seq, payload).to_le_bytes());
    frame.extend_from_slice(payload);
    frame
}

const FIRST_SEGMENT: &str = "wal/00000000000000000000.log";

#[test]
fn read_frame_cuts_zero_length_oversized_and_past_end_frames() {
    // One valid frame, then a frame the three-way size check must refuse.
    // Under the `||` -> `&&` mutant the check is unsatisfiable and dead, and
    // each case shows it its own way: (a) a declared length of 0 with a
    // correct crc is recovered as an EMPTY record, which append forbids;
    // (b) a length above MAX with 8 bytes present and (c) a length of 64
    // with 8 bytes present both read past EOF, and recovery errors instead
    // of cutting. The crc in (a) is deliberately right so the size arm is the
    // only thing between the frame and Valid.
    let valid = raw_frame(0, 8, &[7u8; 8]);
    let cases: [(&str, Vec<u8>); 3] = [
        ("zero-length", raw_frame(1, 0, &[])),
        (
            "oversized",
            raw_frame(1, (MAX_RECORD_BYTES + 1) as u32, &[9u8; 8]),
        ),
        ("past-end", raw_frame(1, 64, &[9u8; 8])),
    ];
    for (name, bad) in cases {
        let sim = quiet_sim();
        let file = sim.disk().open(FIRST_SEGMENT).unwrap();
        file.write_at(0, &valid).unwrap();
        file.write_at(valid.len() as u64, &bad).unwrap();
        file.sync().unwrap();
        sim.disk().sync_dir("wal/").unwrap();

        let mut recovered = Vec::new();
        Wal::open(Box::new(sim.disk()), small_segments(), |seq, p| {
            recovered.push((seq, p.to_vec()));
            Ok(())
        })
        .unwrap_or_else(|e| panic!("{name}: recovery must cut, not fail: {e}"));
        assert_eq!(
            recovered,
            vec![(0, vec![7u8; 8])],
            "{name}: exactly the valid frame is recovered"
        );
        assert_eq!(
            sim.disk().open(FIRST_SEGMENT).unwrap().len().unwrap(),
            valid.len() as u64,
            "{name}: the cut truncates at the bad frame"
        );
    }
}

#[test]
fn a_maximum_size_record_round_trips_and_one_byte_over_is_invalid() {
    // append accepts exactly MAX_RECORD_BYTES; recovery's size arm is
    // `payload_len > MAX`, so a record AT the maximum is the boundary the
    // `>=` mutant cuts, and a frame declaring one byte more — bytes present,
    // crc correct — is the record the `==` mutant recovers.
    let sim = quiet_sim();
    let big = vec![0xABu8; MAX_RECORD_BYTES];
    let mut wal = Wal::open(Box::new(sim.disk()), small_segments(), |_, _| Ok(())).unwrap();
    wal.append(&big).unwrap();
    wal.sync().unwrap();
    let mut recovered = Vec::new();
    Wal::open(Box::new(sim.disk()), small_segments(), |seq, payload| {
        recovered.push((seq, payload.len(), payload == big.as_slice()));
        Ok(())
    })
    .unwrap();
    assert_eq!(
        recovered,
        vec![(0, MAX_RECORD_BYTES, true)],
        "a record of exactly MAX_RECORD_BYTES survives recovery"
    );

    let sim = quiet_sim();
    let over = vec![0xCDu8; MAX_RECORD_BYTES + 1];
    let file = sim.disk().open(FIRST_SEGMENT).unwrap();
    file.write_at(0, &raw_frame(0, (MAX_RECORD_BYTES + 1) as u32, &over))
        .unwrap();
    file.sync().unwrap();
    sim.disk().sync_dir("wal/").unwrap();
    let mut count = 0;
    Wal::open(Box::new(sim.disk()), small_segments(), |_, _| {
        count += 1;
        Ok(())
    })
    .unwrap();
    assert_eq!(
        count, 0,
        "a frame declaring MAX_RECORD_BYTES + 1 is invalid even with its bytes present"
    );
    assert_eq!(
        sim.disk().open(FIRST_SEGMENT).unwrap().len().unwrap(),
        0,
        "…and the cut truncates it away"
    );
}
