//! The first crash-recovery property test (PLAN.md M0 step 1).
//!
//! A toy checksummed append-log runs under the full fault matrix: injected
//! write errors, failed syncs, and crashes landing between any two disk
//! operations, with unsynced writes torn or lost at restart. The durability
//! invariant: every acknowledged (synced) record is recovered intact, in
//! order; an unacknowledged tail may be dropped but must never surface
//! corrupt.
//!
//! The meta-test at the bottom is the point of the exercise: a deliberately
//! naive recovery that trusts the length prefix without verifying the
//! checksum MUST be caught by the harness — if it ever stops being caught,
//! the safety equipment itself has broken.

use std::io;

use charpente_core::io::{Disk, File};
use charpente_sim::{Sim, run_universes, run_universes_from};

const LOG: &str = "wal/000001.log";

/// Found by `harness_catches_naive_recovery` on 2026-07-01 and pinned here:
/// under this seed a crash tears an unsynced record (subset-of-sectors tear)
/// and naive recovery returns record 6 corrupt — right length, wrong bytes.
/// Hard rule 3: a bug found in sim gets its seed recorded in the regression
/// suite.
const REGRESSION_SEED_TORN_WRITE: u64 = 0x8bee_1752_811a_e873;

/// Fixed base for the meta-tests so CI is deterministic; the positive
/// property test uses fresh seeds every run.
const META_TEST_BASE: u64 = 0xC4A2_9E17_0001;

fn crc32(data: &[u8]) -> u32 {
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

/// Frame: [payload len: u32 le][crc32(payload): u32 le][payload].
fn append(file: &dyn File, end: u64, payload: &[u8]) -> io::Result<u64> {
    let mut frame = Vec::with_capacity(8 + payload.len());
    frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    frame.extend_from_slice(&crc32(payload).to_le_bytes());
    frame.extend_from_slice(payload);
    file.write_at(end, &frame)?;
    file.sync()?;
    Ok(end + frame.len() as u64)
}

/// Scans frames from the start; stops at the first invalid one. `checked`
/// verifies the checksum; the naive variant trusts the length prefix — the
/// bug the harness must catch.
fn recover(disk: &dyn Disk, checked: bool) -> io::Result<(Vec<Vec<u8>>, u64)> {
    let file = disk.open(LOG)?;
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
        let crc = u32::from_le_bytes(header[4..8].try_into().unwrap());
        if payload_len == 0 || pos + 8 + payload_len > len {
            break;
        }
        let mut payload = vec![0u8; payload_len as usize];
        file.read_at(pos + 8, &mut payload)?;
        if checked && crc32(&payload) != crc {
            break; // torn or corrupt tail — discard from here
        }
        records.push(payload);
        pos += 8 + payload_len;
    }
    Ok((records, pos))
}

fn check(recovered: &[Vec<u8>], appended: &[Vec<u8>], acked: usize) -> Result<(), String> {
    if recovered.len() < acked {
        return Err(format!(
            "durability violated: {acked} acknowledged records but only {} recovered — an acked write was lost",
            recovered.len()
        ));
    }
    if recovered.len() > appended.len() {
        return Err(format!(
            "recovery invented records: {} recovered but only {} were ever appended",
            recovered.len(),
            appended.len()
        ));
    }
    for (i, (got, want)) in recovered.iter().zip(appended).enumerate() {
        if got != want {
            return Err(format!(
                "record {i} corrupt after recovery: a torn write surfaced ({} bytes recovered, {} appended)",
                got.len(),
                want.len()
            ));
        }
    }
    Ok(())
}

/// One universe: rounds of recover → check → append-until-death → restart,
/// with the crash point, workload sizes, and payloads all drawn from the
/// sim's seeded entropy. Any IO error is treated as the process dying there.
fn crash_workload(sim: &mut Sim, checked: bool) -> Result<(), String> {
    let disk = sim.disk();
    let entropy = sim.entropy();
    use charpente_core::io::Entropy;

    let mut appended: Vec<Vec<u8>> = Vec::new();
    let mut acked = 0usize;

    for _round in 0..4 {
        // The crash lands somewhere in this round's ops — including during
        // recovery itself.
        sim.crash_disk_after(entropy.next_u64() % 64);

        let (recovered, end) = match recover(&disk, checked) {
            Ok(result) => result,
            Err(_) => {
                sim.restart();
                continue; // died during recovery; the invariant is re-checked next round
            }
        };
        check(&recovered, &appended, acked)?;

        // Recovery defines the new truth: unacked survivors are history now.
        appended = recovered;
        acked = appended.len();

        let file = match disk.open(LOG) {
            Ok(file) => file,
            Err(_) => {
                sim.restart();
                continue;
            }
        };
        let mut end = end;
        let appends = 1 + entropy.next_u64() % 12;
        for _ in 0..appends {
            // Multi-sector payloads so torn writes bite mid-record.
            let len = 600 + (entropy.next_u64() % 2400) as usize;
            let mut payload = vec![0u8; len];
            entropy.fill(&mut payload);
            appended.push(payload.clone());
            match append(&*file, end, &payload) {
                Ok(new_end) => {
                    end = new_end;
                    acked += 1;
                }
                Err(_) => break, // the process died here (crash or EIO)
            }
        }
        sim.restart(); // power-cycle between rounds regardless
    }

    // Final quiet recovery: no crash is scheduled after the last restart.
    let (recovered, _) =
        recover(&disk, checked).map_err(|e| format!("final recovery failed: {e}"))?;
    check(&recovered, &appended, acked)
}

/// The positive property, over fresh seeds every run: checksummed recovery
/// upholds the durability invariant under the full fault matrix.
#[test]
fn checked_recovery_survives_the_fault_matrix() {
    run_universes!(512, |sim: &mut Sim| crash_workload(sim, true));
}

/// The meta-test: the harness MUST catch naive recovery. Runs the naive
/// workload across universes from a fixed base until a seed produces a
/// violation, then replays that exact seed to prove the repro contract.
#[test]
fn harness_catches_naive_recovery() {
    let mut state = META_TEST_BASE;
    let mut caught: Option<(u64, String)> = None;
    for _ in 0..400 {
        let seed = charpente_sim::rng::splitmix64(&mut state);
        let mut sim = Sim::new(seed);
        if let Err(msg) = crash_workload(&mut sim, false) {
            caught = Some((seed, msg));
            break;
        }
    }
    let (seed, msg) = caught.expect(
        "the harness no longer catches checksum-free recovery under torn writes — \
         the fault injection itself has regressed",
    );
    eprintln!("caught naive recovery: seed {seed:#018x}: {msg}");

    // The seed IS the reproduction: the same seed must fail the same way.
    let mut replay = Sim::new(seed);
    let replayed = crash_workload(&mut replay, false);
    assert_eq!(
        replayed,
        Err(msg),
        "replaying the seed must reproduce the identical failure"
    );
}

/// Hard rule 3: the recorded regression seed, replayed exactly.
#[test]
fn regression_torn_write_seed() {
    let mut sim = Sim::new(REGRESSION_SEED_TORN_WRITE);
    assert!(
        crash_workload(&mut sim, false).is_err(),
        "seed {REGRESSION_SEED_TORN_WRITE:#018x} no longer catches naive recovery"
    );
}

/// Hard rule 5: a sim failure's output IS the reproduction — it must name
/// the seed and the one-command repro.
#[test]
fn failure_output_names_seed_and_repro() {
    let result = std::panic::catch_unwind(|| {
        run_universes_from!(META_TEST_BASE, 400, |sim: &mut Sim| {
            crash_workload(sim, false)
        });
    });
    let payload = result.expect_err("naive recovery must fail under the meta-test base seed");
    let msg = payload
        .downcast_ref::<String>()
        .expect("panic payload should be the failure report");
    assert!(msg.contains("seed:  0x"), "missing seed line:\n{msg}");
    assert!(
        msg.contains("repro: CHARPENTE_SEED=0x") && msg.contains("--test crash_recovery"),
        "missing one-command repro:\n{msg}"
    );
}
