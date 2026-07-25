//! The simulated disk's state and crash model.
//!
//! Writes buffer in `pending` until a successful `sync` makes them durable —
//! the page cache. At crash time every pending op independently survives in
//! full, torn (an arbitrary strict subset of its sectors), or not at all;
//! because decisions are independent across ops and across sectors, later
//! data can survive earlier data's loss — unordered page-cache flushes, the
//! ALICE/TigerBeetle crash-consistency model. Directory-entry durability is
//! modeled too (`pending_creates`/`pending_removes`, collapsed by
//! `collapse_dir_entries`): a created or removed name is not durable until
//! `Disk::sync_dir`. Not yet modeled: read faults — revisit with the
//! fault-matrix extensions (bit-level corruption, misdirected writes,
//! latent read errors).

use std::collections::{BTreeMap, BTreeSet};

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
    /// Created names whose directory entry is not yet durable.
    pub pending_creates: BTreeSet<String>,
    /// Removed names whose removal is not yet durable, keyed to the durable
    /// content a crash would resurrect.
    pub pending_removes: BTreeMap<String, Vec<u8>>,
}

#[derive(Default)]
pub(crate) struct FileState {
    pub durable: Vec<u8>,
    pub pending: Vec<PendingOp>,
}

#[derive(Debug)] // so a differential-test failure can name the exact op sequence
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
    /// What reads see: durable bytes with the page cache overlaid. This is the
    /// reference definition — `view_len`/`read_into` must agree with it byte for
    /// byte, and the differential test in this module is what holds them to it.
    /// Test-only since those two replaced it on every production path: reads no
    /// longer rebuild the whole image to answer a length or a range.
    #[cfg(test)]
    pub fn view(&self) -> Vec<u8> {
        apply(self.durable.clone(), &self.pending)
    }

    /// `view().len()` without building the image.
    pub fn view_len(&self) -> u64 {
        let mut len = self.durable.len() as u64;
        for op in &self.pending {
            match op {
                // A write only ever extends: `apply` resizes when the write
                // ends past the current length, never when it ends inside.
                PendingOp::Write { offset, data } => len = len.max(offset + data.len() as u64),
                PendingOp::SetLen { len: l } => len = *l,
            }
        }
        len
    }

    /// `view()[offset..offset + out.len()]` without building the image. The
    /// caller bounds-checks against `view_len` first (reads past the end are a
    /// contract violation, reported there).
    pub fn read_into(&self, offset: u64, out: &mut [u8]) {
        // Zero is what every unsourced byte reads as: past the durable image,
        // inside the gap a write past the end leaves behind, and beyond a
        // truncation that a later op re-extends over.
        out.fill(0);
        let mut len = self.durable.len() as u64;
        splice(&self.durable, 0, offset, out);
        for op in &self.pending {
            match op {
                PendingOp::Write { offset: at, data } => {
                    splice(data, *at, offset, out);
                    len = len.max(at + data.len() as u64);
                }
                PendingOp::SetLen { len: l } => {
                    // Truncation drops bytes the window may already hold; a
                    // later extension brings them back as zeros, never as the
                    // old content.
                    if *l < len {
                        zero(*l, len, offset, out);
                    }
                    len = *l;
                }
            }
        }
    }
}

/// Copies `src` — which occupies absolute `[at, at + src.len())` in the file —
/// into the read window that starts at absolute `from`. Anything outside the
/// window is dropped.
fn splice(src: &[u8], at: u64, from: u64, out: &mut [u8]) {
    let lo = at.max(from);
    let hi = (at + src.len() as u64).min(from + out.len() as u64);
    if lo >= hi {
        return;
    }
    let (d, s, n) = ((lo - from) as usize, (lo - at) as usize, (hi - lo) as usize);
    out[d..d + n].copy_from_slice(&src[s..s + n]);
}

/// Zeros absolute `[a, b)` inside the same window.
fn zero(a: u64, b: u64, from: u64, out: &mut [u8]) {
    let lo = a.max(from);
    let hi = b.min(from + out.len() as u64);
    if lo >= hi {
        return;
    }
    out[(lo - from) as usize..(hi - from) as usize].fill(0);
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

/// Crash-collapses pending directory entries. Only called when
/// `dir_entry_loss > 0.0` — the zero-draw fast path that keeps every pinned
/// seed's rng sequence untouched while this fault axis is off (the frozen
/// replay contract). An affected pending create vanishes: its file is
/// dropped even if its bytes are durable, because losing the *name* is the
/// whole point of the fence. An affected pending remove resurrects: the file
/// returns with its last durable content. For a name pending both a remove
/// and a re-create (the rebuild path — remove, then reopen before
/// `sync_dir`), the remove is rolled first: a resurrection discards whatever
/// state the new create left behind; only if the remove sticks does the
/// create get its own roll. Every affected entry marks `"fault:dir-entry"`.
pub(crate) fn collapse_dir_entries(
    disk: &mut DiskWorld,
    rng: &mut Rng,
    p: f64,
    coverage: &mut BTreeMap<&'static str, u64>,
) {
    let mut mark = |key: &'static str| *coverage.entry(key).or_insert(0) += 1;
    let names: BTreeSet<String> = disk
        .pending_creates
        .iter()
        .cloned()
        .chain(disk.pending_removes.keys().cloned())
        .collect();
    for name in names {
        let had_create = disk.pending_creates.remove(&name);
        if let Some(durable) = disk.pending_removes.remove(&name)
            && rng.chance(p)
        {
            mark("fault:dir-entry");
            disk.files.insert(
                name,
                FileState {
                    durable,
                    pending: Vec::new(),
                },
            );
            continue; // resurrection discards any pending re-create
        }
        if had_create && rng.chance(p) {
            mark("fault:dir-entry");
            disk.files.remove(&name);
        }
    }
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

    /// `view()` is the oracle. The fast paths must agree with it for every
    /// (durable, pending) shape the fault matrix can produce — writes past the
    /// end, truncations, re-extensions after a truncation, empty writes that
    /// extend by gap alone — and for reads straddling all of them. A fast path
    /// that disagrees here would make the simulator lie rather than fail, which
    /// is the one failure mode the sim cannot afford.
    #[test]
    fn fast_paths_agree_with_the_view_reference() {
        let mut rng = Rng::new(0xD15C_0001);
        for _ in 0..3000 {
            let mut durable = vec![0u8; rng.below(1500) as usize];
            rng.fill(&mut durable);
            let mut file = FileState {
                durable,
                pending: Vec::new(),
            };
            for _ in 0..rng.below(7) {
                if rng.chance(0.3) {
                    file.pending.push(PendingOp::SetLen {
                        len: rng.below(2000),
                    });
                } else {
                    let offset = rng.below(2000);
                    let mut data = vec![0u8; rng.below(400) as usize];
                    rng.fill(&mut data);
                    file.pending.push(PendingOp::Write { offset, data });
                }
            }

            // Op shapes, not op contents: the payload bytes are random and
            // reproducible from the seed, so dumping them buries the shape that
            // actually explains the failure.
            let shape: Vec<String> = file
                .pending
                .iter()
                .map(|op| match op {
                    PendingOp::Write { offset, data } => format!("W@{offset}+{}", data.len()),
                    PendingOp::SetLen { len } => format!("L={len}"),
                })
                .collect();
            let shape = format!("durable {} then [{}]", file.durable.len(), shape.join(", "));

            let reference = file.view();
            assert_eq!(
                file.view_len(),
                reference.len() as u64,
                "view_len disagreed with view() over {shape}"
            );
            if reference.is_empty() {
                continue;
            }
            let start = rng.below(reference.len() as u64);
            let n = rng.below(reference.len() as u64 - start + 1);
            let mut out = vec![0xAA; n as usize];
            file.read_into(start, &mut out);
            assert_eq!(
                out,
                reference[start as usize..(start + n) as usize],
                "read_into({start}, {n}) disagreed with view() over {shape}"
            );
        }
    }
}
