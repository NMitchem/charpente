//! The simulated disk's state and crash model.
//!
//! Writes buffer in `pending` until a successful `sync` makes them durable —
//! the page cache. At crash time every pending op independently survives in
//! full, torn (an arbitrary strict subset of its sectors), or not at all;
//! because decisions are independent across ops and across sectors, later
//! data can survive earlier data's loss — unordered page-cache flushes, the
//! ALICE/TigerBeetle crash-consistency model. Not yet modeled: directory-
//! entry durability (created files survive crashes without a directory sync)
//! and read faults — both revisit with WAL segment management.

use std::collections::BTreeMap;

use crate::FaultConfig;
use crate::rng::Rng;

pub(crate) const SECTOR: u64 = 512;

#[derive(Default)]
pub(crate) struct DiskWorld {
    pub files: BTreeMap<String, FileState>,
    /// Bumped on every restart; handles from an older epoch are dead.
    pub epoch: u64,
    /// True between an injected crash and the next `restart()`.
    pub crashed: bool,
    /// Ops until the next injected crash; the op that hits zero fails.
    pub crash_countdown: Option<u64>,
}

#[derive(Default)]
pub(crate) struct FileState {
    pub durable: Vec<u8>,
    pub pending: Vec<PendingOp>,
}

pub(crate) enum PendingOp {
    Write { offset: u64, data: Vec<u8> },
    SetLen { len: u64 },
}

/// Applies ops to a byte image. Gaps left by writes past the end read as
/// zeros (pwrite semantics).
pub(crate) fn apply(mut bytes: Vec<u8>, ops: &[PendingOp]) -> Vec<u8> {
    for op in ops {
        match op {
            PendingOp::Write { offset, data } => {
                let offset = usize::try_from(*offset).expect("file offset exceeds usize");
                let end = offset + data.len();
                if bytes.len() < end {
                    bytes.resize(end, 0);
                }
                bytes[offset..end].copy_from_slice(data);
            }
            PendingOp::SetLen { len } => {
                bytes.resize(usize::try_from(*len).expect("file length exceeds usize"), 0);
            }
        }
    }
    bytes
}

impl FileState {
    /// What reads see: durable bytes with the page cache overlaid.
    pub fn view(&self) -> Vec<u8> {
        apply(self.durable.clone(), &self.pending)
    }
}

/// Crash-collapses one file: every pending op independently survives, tears,
/// or vanishes, then the survivors become the durable image. Each op's fate
/// is marked into `coverage` (`crash:applied` / `crash:torn` / `crash:dropped`,
/// sometimes-coverage accounting — see the sim-swarm-coverage spec).
pub(crate) fn collapse(
    file: &mut FileState,
    rng: &mut Rng,
    cfg: &FaultConfig,
    coverage: &mut BTreeMap<&'static str, u64>,
) {
    let mut mark = |key: &'static str| *coverage.entry(key).or_insert(0) += 1;
    let mut survivors = Vec::new();
    for op in file.pending.drain(..) {
        match op {
            PendingOp::SetLen { len } => {
                if rng.chance(cfg.crash_apply + cfg.crash_torn) {
                    mark("crash:applied");
                    survivors.push(PendingOp::SetLen { len });
                } else {
                    mark("crash:dropped");
                }
            }
            PendingOp::Write { offset, data } => {
                let roll = rng.chance(cfg.crash_apply);
                if roll {
                    mark("crash:applied");
                    survivors.push(PendingOp::Write { offset, data });
                } else if rng.chance(cfg.crash_torn / (1.0 - cfg.crash_apply)) {
                    mark("crash:torn");
                    tear(offset, data, rng, &mut survivors);
                } else {
                    mark("crash:dropped");
                }
            }
        }
    }
    file.durable = apply(std::mem::take(&mut file.durable), &survivors);
}

/// A torn write. Sectors flush unordered, so an arbitrary strict subset of a
/// multi-sector write survives — including patterns where the tail persists
/// but the middle is lost, the case that defeats length-prefix-only recovery.
/// A sub-sector write tears to a strict byte prefix.
fn tear(offset: u64, data: Vec<u8>, rng: &mut Rng, survivors: &mut Vec<PendingOp>) {
    assert!(
        !data.is_empty(),
        "empty writes are filtered before buffering"
    );
    let len = data.len() as u64;
    if len <= SECTOR {
        let keep = rng.below(len) as usize;
        if keep > 0 {
            survivors.push(PendingOp::Write {
                offset,
                data: data[..keep].to_vec(),
            });
        }
        return;
    }
    let sectors = len.div_ceil(SECTOR);
    // Force at least one sector lost (a tear is a strict subset); every other
    // sector independently survives.
    let lost = rng.below(sectors);
    for s in 0..sectors {
        if s == lost || !rng.chance(0.5) {
            continue;
        }
        let start = (s * SECTOR) as usize;
        let end = data.len().min(((s + 1) * SECTOR) as usize);
        survivors.push(PendingOp::Write {
            offset: offset + start as u64,
            data: data[start..end].to_vec(),
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(apply: f64, torn: f64) -> FaultConfig {
        FaultConfig {
            crash_apply: apply,
            crash_torn: torn,
            ..FaultConfig::none()
        }
    }

    #[test]
    fn view_overlays_pending_on_durable() {
        let file = FileState {
            durable: vec![1, 1, 1, 1],
            pending: vec![
                PendingOp::Write {
                    offset: 2,
                    data: vec![9, 9, 9],
                },
                PendingOp::SetLen { len: 4 },
            ],
        };
        assert_eq!(file.view(), vec![1, 1, 9, 9]);
        // durable untouched until sync
        assert_eq!(file.durable, vec![1, 1, 1, 1]);
    }

    #[test]
    fn write_past_end_zero_fills_gap() {
        let bytes = apply(
            vec![7],
            &[PendingOp::Write {
                offset: 3,
                data: vec![5],
            }],
        );
        assert_eq!(bytes, vec![7, 0, 0, 5]);
    }

    #[test]
    fn collapse_with_certain_apply_keeps_everything() {
        let mut file = FileState {
            durable: vec![],
            pending: vec![PendingOp::Write {
                offset: 0,
                data: vec![1, 2, 3],
            }],
        };
        let mut coverage = BTreeMap::new();
        collapse(&mut file, &mut Rng::new(0), &cfg(1.0, 0.0), &mut coverage);
        assert_eq!(file.durable, vec![1, 2, 3]);
        assert!(file.pending.is_empty());
        assert_eq!(coverage["crash:applied"], 1);
    }

    #[test]
    fn collapse_with_certain_drop_loses_everything() {
        let mut file = FileState {
            durable: vec![9],
            pending: vec![PendingOp::Write {
                offset: 0,
                data: vec![1, 2, 3],
            }],
        };
        let mut coverage = BTreeMap::new();
        collapse(&mut file, &mut Rng::new(0), &cfg(0.0, 0.0), &mut coverage);
        assert_eq!(file.durable, vec![9], "durable data must survive any crash");
        assert_eq!(coverage["crash:dropped"], 1);
    }

    #[test]
    fn tear_subsector_is_a_strict_byte_prefix() {
        let mut rng = Rng::new(11);
        for _ in 0..200 {
            let data: Vec<u8> = (0..200).map(|i| i as u8).collect();
            let mut survivors = Vec::new();
            tear(0, data.clone(), &mut rng, &mut survivors);
            assert!(survivors.len() <= 1);
            if let Some(PendingOp::Write { offset, data: kept }) = survivors.first() {
                assert_eq!(*offset, 0);
                assert!(kept.len() < data.len());
                assert_eq!(kept[..], data[..kept.len()]);
            }
        }
    }

    #[test]
    fn tear_multisector_keeps_a_strict_sector_subset() {
        let mut rng = Rng::new(13);
        let data: Vec<u8> = (0..3 * SECTOR).map(|i| (i % 251) as u8).collect();
        let mut saw_non_prefix = false;
        for _ in 0..500 {
            let mut survivors = Vec::new();
            tear(0, data.clone(), &mut rng, &mut survivors);
            let mut kept_bytes = 0;
            for op in &survivors {
                let PendingOp::Write { offset, data: kept } = op else {
                    unreachable!()
                };
                assert_eq!(offset % SECTOR, 0, "torn sectors are sector-aligned");
                assert_eq!(
                    kept[..],
                    data[*offset as usize..*offset as usize + kept.len()],
                    "surviving sectors carry the written bytes"
                );
                kept_bytes += kept.len();
                if *offset > 0 && survivors.len() == 1 {
                    saw_non_prefix = true; // e.g. only the tail survived
                }
            }
            assert!(
                kept_bytes < data.len(),
                "a tear must lose at least one sector"
            );
        }
        assert!(
            saw_non_prefix,
            "subset tears must include non-prefix patterns"
        );
    }
}
