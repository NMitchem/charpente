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
fn frame_pattern() -> [u8; 256] {
    let mut state: u64 = 0x9E3779B97F4A7C15;
    let mut out = [0u8; 256];
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
