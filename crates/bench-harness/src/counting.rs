//! A `Disk` that COUNTS durable syncs, shared by every layer that can count
//! rather than derive.
//!
//! This existed twice before this module: `kernel_bench`'s (group-commit T7,
//! C-7) and the one its doc cites in `charpente/tests/group_commit.rs` (T5).
//! The wire layer needed a third, which is the point at which this repo
//! consolidates rather than copies — the `classify` and `finish` precedents
//! both cost real time by waiting for a fourth.
//!
//! **Scope, carried forward in intent from `kernel_bench`'s fix round M-2,
//! because neither choice is wrong but neither was written down until then.**
//! ALWAYS EXCLUDED — `Disk::sync_dir` (the directory-entry barrier at
//! rotation, store creation and `.horizon` writes) is a separate `Disk` method
//! this wrapper delegates unwrapped, so it never reaches the counter.
//! `File::sync` is counted subject to the path filter: `new` counts every path
//! (the WAL's fsyncs and the store's checkpoints and `.horizon`'s alike);
//! `with_prefix` counts only paths under the given prefix. Which one a layer
//! wants is not a style choice — it depends on what its denominator measures,
//! and `with_prefix`'s doc has the argument.
//!
//! Counting is the whole contract: this wrapper never changes an outcome, so
//! a layer can wrap unconditionally without perturbing what it measures.

use charpente::core::io::{Disk, File};
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Wraps any `Disk`, incrementing `syncs` once per `File::sync`.
pub struct CountingDisk {
    inner: Box<dyn Disk>,
    syncs: Arc<AtomicU64>,
    /// `None` counts every path; `Some(p)` counts only paths starting with `p`.
    prefix: Option<&'static str>,
}

impl CountingDisk {
    /// Wrap `inner`, counting EVERY `File::sync` on every path. The counter is
    /// handed back rather than owned privately so a caller that cannot reach
    /// the disk later — a `#[route]` handler, which takes only `&mut Tx` and
    /// has no state injection — can still read it.
    pub fn new(inner: Box<dyn Disk>) -> (Self, Arc<AtomicU64>) {
        Self::with_prefix(inner, None)
    }

    /// Count only syncs on paths starting with `prefix` (e.g. `"wal/"`).
    ///
    /// **Why this exists, and it is a measurement-semantics fix rather than a
    /// convenience.** The device-ratio gate divides a sync rate by
    /// `device_bracket_sample`'s ceiling, which measures *sequential append +
    /// sync on one growing file* — the WAL's op shape, chosen deliberately by
    /// the 2026-08-10 device-ratio interlude precisely because the previous
    /// one-byte-rewrite shape "was not an upper bound at all". An unscoped
    /// counter is not the same quantity as that denominator: it also counts the
    /// store's checkpoint syncs and the `.horizon` sidecar's, and a sync of a
    /// file with no dirty pages is nearly free, so a total-sync rate can exceed
    /// an append-sync ceiling **without anything being wrong**. Measured: at 1
    /// connection (no batching) bench-app counted 290 syncs/s against 124 acked
    /// requests/s — 2.34 per request where the WAL alone owes 2.0 — and tripped
    /// the gate at 1.15x.
    ///
    /// Scoping the numerator to `wal/` makes numerator and denominator the same
    /// physical quantity, which is what makes the ratio mean anything. This is
    /// NOT a widened threshold: the tolerance is untouched and a WAL sync rate
    /// above the WAL ceiling is still refused.
    ///
    /// **Known and deliberately not changed here:** `kernel_bench`'s counter is
    /// unscoped and so has the same apples-to-oranges comparison. Its
    /// `device_ratio` (0.392 at last measurement) is far enough under 1.0 that
    /// the discrepancy has never surfaced there. Rescoping it would move a
    /// published number and belongs to whoever next touches that layer.
    pub fn with_prefix(
        inner: Box<dyn Disk>,
        prefix: Option<&'static str>,
    ) -> (Self, Arc<AtomicU64>) {
        let syncs = Arc::new(AtomicU64::new(0));
        (
            Self {
                inner,
                syncs: syncs.clone(),
                prefix,
            },
            syncs,
        )
    }
}

impl Disk for CountingDisk {
    fn open(&self, path: &str) -> io::Result<Box<dyn File>> {
        let file = self.inner.open(path)?;
        // Decided once at open, not per sync: the path is fixed for the life of
        // the handle, so a counted file carries the counter and an uncounted one
        // carries none.
        let counts = self.prefix.is_none_or(|p| path.starts_with(p));
        Ok(Box::new(CountingFile {
            inner: file,
            syncs: counts.then(|| self.syncs.clone()),
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

struct CountingFile {
    inner: Box<dyn File>,
    /// `None` when this path is outside the counted prefix.
    syncs: Option<Arc<AtomicU64>>,
}

impl File for CountingFile {
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
        if let Some(syncs) = &self.syncs {
            syncs.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.sync()
    }
}
