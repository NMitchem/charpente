//! The hardware ceiling (spec §2): durable-sync rate through the production
//! Disk impl. Every other layer's number is published against this one.
//! Edge tooling outside the deterministic core (DESIGN §1's fence is the
//! core, not this crate).
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use crate::report::DeviceReport;
use charpente::core::io::Disk;
use charpente::prod::ProdDisk;
use std::io;
use std::time::{Duration, Instant};

// T5 review, C-1 / Noah ruling #1: exposed to `core_bench` so its device-
// ceiling bracket samples reuse this loop rather than hand-rolling a second
// one.
pub(crate) fn timed_loop(
    name: &str,
    warmup: Duration,
    duration: Duration,
    mut op: impl FnMut() -> io::Result<()>,
) -> io::Result<f64> {
    let warm_end = Instant::now() + warmup;
    while Instant::now() < warm_end {
        op()?;
    }
    let start = Instant::now();
    let end = start + duration;
    let mut ops = 0u64;
    while Instant::now() < end {
        op()?;
        ops += 1;
    }
    // T4 review, I-2 (the zero-op case) + T5 review C-3 / Noah ruling #2 (the
    // general floor, spec §5 amended in the same commit): `ops == 0` divides
    // into a FINITE `0.0`, not NaN/Infinity — `report::collect_non_finite`
    // cannot catch it — and a small nonzero count is the same defect one
    // step later (a rate that is really just one operation's own latency
    // inverted). Shared with core_bench.rs's write/read loops via
    // `crate::measure::degenerate_window_reason`/`MIN_MEASURED_OPS` so both
    // layers refuse by the same rule.
    if let Some(reason) =
        crate::measure::degenerate_window_reason(&format!("device {name}"), ops, duration)
    {
        return Err(io::Error::other(reason));
    }
    Ok(ops as f64 / start.elapsed().as_secs_f64())
}

/// A fixed (not all-zero, not a repeated byte) 256-byte pattern for the
/// append-shaped loop's frame (T4 review, M-3): a real WAL frame carries real
/// payload bytes, and a zero-eliding or compressing filesystem (ZFS/btrfs)
/// would make a trivial frame's append cheaper than a real one's. Generated
/// deterministically (splitmix64, a fixed seed) — reproducible, not
/// cryptographic, just non-trivial.
///
/// High entropy inside one frame defeats zero-elision, but writing the SAME
/// 256 bytes at every offset would still leave the file one block repeated,
/// which an LZ-family compressor (ZFS `lz4`, btrfs `zstd`) squeezes nearly as
/// hard as zeros (T4 re-review, M-F). The caller therefore stamps each frame
/// with its own offset before writing, exactly as a real WAL frame carries a
/// distinct header — so no two blocks are identical.
pub(crate) const FRAME_BYTES: usize = 256;

fn frame_pattern() -> [u8; FRAME_BYTES] {
    let mut state: u64 = 0x9E3779B97F4A7C15;
    let mut out = [0u8; FRAME_BYTES];
    for chunk in out.chunks_mut(8) {
        state = state.wrapping_add(0x9E3779B97F4A7C15);
        let mut z = state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^= z >> 31;
        chunk.copy_from_slice(&z.to_le_bytes()[..chunk.len()]);
    }
    out
}

pub fn run(dir: &str, warmup: Duration, duration: Duration) -> io::Result<DeviceReport> {
    let disk = ProdDisk::new(dir)?;
    // Dirty-byte sync: minimal dirty state, repeated durable sync — the pure
    // sync rate. (A clean-file sync could be elided by the OS; one byte keeps
    // it honest.)
    let file = disk.open("bench-sync.dat")?;
    let syncs_per_sec = timed_loop("sync", warmup, duration, || {
        file.write_at(0, &[1u8])?;
        file.sync()
    })?;
    // Append-shaped: write_at the tail + sync, the WAL's op shape. The frame
    // is a high-entropy pattern (T4 review, M-3) stamped with its own offset
    // (T4 re-review, M-F), not all-zero and not one block repeated: a
    // zero-eliding or compressing filesystem (ZFS/btrfs) would otherwise make
    // this payload's append cheaper than a real WAL frame's, silently no
    // longer measuring what the doc comment claims.
    let file = disk.open("bench-append.dat")?;
    let mut off = 0u64;
    let mut frame = frame_pattern();
    let appends_per_sec = timed_loop("append", warmup, duration, || {
        frame[..8].copy_from_slice(&off.to_le_bytes());
        file.write_at(off, &frame)?;
        off += frame.len() as u64;
        file.sync()
    })?;
    Ok(DeviceReport {
        syncs_per_sec,
        appends_per_sec,
    })
}

// T5 review, C-1 / Noah ruling #1: a short, independent device-sync sample on
// the caller's own directory, for bracketing a measured window elsewhere in
// this crate. Reuses `timed_loop` rather than hand-rolling a second sync
// loop. Fixed (not user-configurable) duration: this is a bracket around a
// measured window, not a second `bench device` invocation, and a full
// spec-default device run (5s warmup + 30s duration) on both sides would
// roughly triple the caller's own wall time for a supporting measurement.
const BRACKET_WARMUP: Duration = Duration::from_millis(100);
const BRACKET_DURATION: Duration = Duration::from_secs(1);

/// Promoted here from `core_bench.rs` at T7 rev. 3 so the wire layer's
/// per-point brackets reuse the SAME function the core layer's write-window
/// brackets do (plan: "moving it to device.rs beside timed_loop is
/// acceptable and probably better; copying it is not").
/// Each sample gets its own file, so every one of them is an append to a
/// GROWING file rather than a rewrite of the previous sample's extents — the
/// WAL's shape, which is the whole point of the interlude's §4.1.
static BRACKET_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

pub(crate) fn device_bracket_sample(dir: &str) -> io::Result<f64> {
    let disk = ProdDisk::new(dir)?;
    let n = BRACKET_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let file = disk.open(&format!("bench-bracket-{n}.dat"))?;
    // Interlude spec §4.1 / Noah ruling 2026-08-10: this is a sequential
    // append + sync, NOT the `write_at(0, &[1u8])` rewrite it was through T8.
    // The bracket's job is to bound a WAL append, and on apfs the two shapes
    // do not cost the same: measured over 15 paired `bench device` runs the
    // append shape is ~4.2% FASTER (mean paired ratio 1.0421, faster in 12 of
    // 15), with loop order ruled out as the cause by an order-alternating
    // probe (1.0433 sync-first vs 1.0411 append-first). A denominator ~4%
    // below the path it bounds is not a ceiling, and this gate divides by it.
    //
    // This reinstates T8's Ruling 1, whose n=2 refutation could not have
    // detected a 4% effect inside 10%+ noise (spec §7).
    let mut off = 0u64;
    let mut frame = frame_pattern();
    // T5 re-review, N-11 (Minor): `timed_loop` formats its degenerate-window
    // message as "device {name}", so passing "core bracket" here rendered
    // "the device core bracket loop" — a doubled "device" reading. "bracket"
    // alone reads as "the device bracket loop".
    timed_loop("bracket", BRACKET_WARMUP, BRACKET_DURATION, || {
        frame[..8].copy_from_slice(&off.to_le_bytes());
        file.write_at(off, &frame)?;
        off += frame.len() as u64;
        file.sync()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    // T4 review, I-2: a zero-op measured window (`--duration 0`, or any
    // window shorter than one durable sync) computed a FINITE `0.0` rate
    // before this fix — `collect_non_finite` never saw it, so the run
    // published a meaningless ceiling as if it were valid. `ops == 0` must
    // be a named error, per hard rule 5, not a silently-wrong number.
    #[test]
    fn a_zero_op_window_is_a_named_error_not_a_silent_zero() {
        let err = timed_loop("sync", Duration::ZERO, Duration::ZERO, || Ok(())).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("INVALID"), "got: {msg}");
        assert!(msg.contains("0 ops"), "got: {msg}");
        // T4 re-review, M-C: `contains("sync")` could NOT fail — the message's
        // own tail carries the literal words "durable sync", so it passed with
        // `{name}` deleted from the format string entirely (proven by
        // mutation). Assert the interpolated phrase, and assert the append
        // loop separately, or neither loop is actually named.
        assert!(
            msg.contains("the device sync loop"),
            "must name which loop: {msg}"
        );
        assert!(msg.contains("--duration"), "must name the fix: {msg}");
    }

    #[test]
    fn a_zero_op_window_names_the_append_loop_too() {
        let err = timed_loop("append", Duration::ZERO, Duration::ZERO, || Ok(())).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("the device append loop"),
            "the discriminating case — a message naming only 'sync' would pass \
             the test above for the wrong reason: {msg}"
        );
    }

    // T5 review, C-3 / Noah ruling #2: a window that completed a SMALL
    // NONZERO op count is the same defect as zero ops, one step later — the
    // review reproduced this through the shipped CLI (`--duration 0.004`,
    // one op, ratios up to 1.543). Force a small nonzero count deterministically
    // via a slow op (not a mock of the floor itself) — a naive `ops == 0`
    // check, which is what shipped before this round, would let this pass.
    #[test]
    fn a_below_floor_nonzero_op_window_is_still_refused() {
        let err = timed_loop("sync", Duration::ZERO, Duration::from_millis(45), || {
            std::thread::sleep(Duration::from_millis(15));
            Ok(())
        })
        .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("INVALID"), "got: {msg}");
        assert!(
            !msg.contains("only 0 ops"),
            "must be the nonzero-but-below-floor case, not the zero-op case: {msg}"
        );
        assert!(msg.contains("the device sync loop"), "got: {msg}");
    }

    // T4 review, M-3: the frame must not be all-zero or otherwise trivially
    // compressible — that would let a compressing/zero-eliding filesystem
    // make the append rate cheaper than a real WAL frame's, silently.
    #[test]
    fn the_append_frame_is_not_degenerate() {
        let frame = frame_pattern();
        assert_eq!(frame.len(), 256);
        assert!(
            frame.iter().any(|&b| b != 0),
            "frame must not be all-zero (zero-eliding filesystems make that free)"
        );
        assert!(
            frame.iter().collect::<std::collections::HashSet<_>>().len() > 1,
            "frame must not be a single repeated byte"
        );
    }

    /// The interlude's §6.1 discriminating test. `device_bracket_sample` must
    /// measure the op shape it BOUNDS — a sequential append, the WAL's shape —
    /// not a rewrite of offset 0 of a one-byte file. Measured at n=15 paired
    /// (order confound ruled out at n=14): the append shape runs ~4.2% faster
    /// on apfs, so the rewrite shape is not an upper bound on the path it was
    /// being divided into.
    ///
    /// Keyed on the artifact rather than the rate, because a rate assertion
    /// here would be a machine-speed assertion. The old shape leaves a
    /// ONE-BYTE file however fast the machine is; the new one leaves at least
    /// two frames however slow it is, because `timed_loop` runs one warmup op
    /// and one timed op before it can look at the clock again.
    ///
    /// That is also why a refused rate is accepted here. On a disk slower
    /// than `MIN_MEASURED_OPS` syncs a second — a CI runner under the lib
    /// suite's parallel IO did 4, twice (2026-09-12, 2026-09-30) — `timed_loop`
    /// refuses the RATE by name, as it must, and the file it leaves still
    /// shows the SHAPE. Any other error still fails the test.
    #[test]
    fn a_bracket_sample_appends_rather_than_rewriting_one_block() {
        let dir = std::env::temp_dir().join(format!(
            "charpente-bracket-shape-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        std::fs::create_dir_all(&dir).expect("scratch dir");
        let path = dir.to_str().expect("utf-8 scratch path");

        match device_bracket_sample(path) {
            Ok(rate) => assert!(rate > 0.0, "rate: {rate}"),
            Err(e) => assert!(
                e.to_string()
                    .starts_with("INVALID: the device bracket loop completed only"),
                "bracket sample: {e}"
            ),
        }

        let biggest = std::fs::read_dir(&dir)
            .expect("read scratch dir")
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().starts_with("bench-bracket"))
            .map(|e| e.metadata().expect("metadata").len())
            .max()
            .expect("the bracket wrote a file");
        std::fs::remove_dir_all(&dir).ok();

        let floor = 2 * FRAME_BYTES as u64;
        assert!(
            biggest >= floor,
            "the bracket must APPEND: its file is {biggest} bytes, but a loop that \
             ran a warmup op and a timed op, each appending a {FRAME_BYTES}-byte frame, \
             must leave at least {floor}. A one-byte file means the bracket is still \
             rewriting offset 0, which measures a shape ~4% slower than the WAL \
             append it is used to bound"
        );
    }

    #[test]
    fn a_nonzero_op_window_still_reports_a_rate() {
        let mut ops = 0u64;
        let rate = timed_loop("sync", Duration::ZERO, Duration::from_millis(5), || {
            ops += 1;
            Ok(())
        })
        .expect("at least one op fits in 5ms of a no-op closure");
        assert!(rate.is_finite() && rate > 0.0, "rate: {rate}");
    }
}
