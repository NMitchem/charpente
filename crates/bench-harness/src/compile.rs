//! The compile-latency instrument's measure half: it runs builds and records
//! what happened. It decides nothing — every rule about whether a recording
//! may be published lives in [`crate::compile_accounting`], which carries no
//! deny-list suppression precisely so that split is mechanical.
//!
//! PLAN.md's risk table calls compile latency the accepted headline risk and
//! DESIGN §9.5 calls it *the* adoption risk. This is the first instrument in
//! the repo that measures it.
//!
//! **The child-process problem is the whole difficulty.** `cargo` does almost
//! nothing itself; `rustc` does the work, in processes this one never touches.
//! `measure::process_cpu()` (RUSAGE_SELF) would report the few milliseconds
//! the parent spent waiting and call it the cost of a compile.
//! [`measure::children_cpu`] (RUSAGE_CHILDREN) is the correct probe, and
//! `compile_accounting::Invalid::ChildCpuNotAggregated` refuses any
//! observation where the two are not clearly distinguishable — so getting this
//! wrong fails loudly rather than publishing a small confident number.
//!
//! **Why the window closes after the wait.** RUSAGE_CHILDREN counts nothing
//! until a child is reaped. Sampling around the spawn instead of around the
//! wait reads zero, which is indistinguishable from a build that did no work.
//!
//! **One caveat this instrument cannot resolve, stated rather than buried.**
//! The heartbeat guard was built for windows where the harness controls the
//! load, and a `cargo build` saturates every core on purpose. If it fires on a
//! healthy build, that is the guard meeting a workload it was not designed
//! for, not a suspended machine — and the reading it refuses should be
//! re-examined, not waved through.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Instant;

use crate::compile_accounting::{Cache, Observation, Phase};
use crate::measure;

/// A source edit held for the duration of a measurement and reverted when it
/// drops — including on panic, which is why this is a guard rather than a pair
/// of calls. A measurement that left the tree edited would silently change
/// every later phase.
pub struct Edit {
    path: PathBuf,
    original: String,
}

impl Edit {
    /// Replaces the single occurrence of `find` with `replace`.
    ///
    /// Asserting on exactly one occurrence is the negative-space check (hard
    /// rule 4): a marker that matches nothing edits nothing, the build is
    /// then a no-op rebuild, and the phase reports an incremental time for an
    /// edit that never happened — a fast, plausible, entirely false number.
    pub fn apply(path: &Path, find: &str, replace: &str) -> std::io::Result<Edit> {
        let original = std::fs::read_to_string(path)?;
        let hits = original.matches(find).count();
        assert_eq!(
            hits,
            1,
            "compile phase marker {find:?} occurs {hits} times in {} — it must occur exactly \
             once. Zero means the edit silently did nothing and the phase would time a no-op \
             rebuild; more than one means the edit is not the one the phase names.",
            path.display()
        );
        std::fs::write(path, original.replace(find, replace))?;
        Ok(Edit {
            path: path.to_path_buf(),
            original,
        })
    }
}

impl Drop for Edit {
    fn drop(&mut self) {
        let _ = std::fs::write(&self.path, &self.original);
    }
}

/// Removes the target directory's build artifacts for `dir`'s package, so the
/// next build is genuinely [`Cache::Cold`].
pub fn clean(dir: &Path) -> std::io::Result<()> {
    let status = Command::new("cargo")
        .args(["clean"])
        .current_dir(dir)
        .status()?;
    assert!(status.success(), "cargo clean failed: {status}");
    Ok(())
}

/// Runs one `cargo build` in `dir` and records it. The returned observation is
/// raw: nothing here decides whether it is publishable.
pub fn measure_build(dir: &Path, phase: Phase, cache: Cache) -> std::io::Result<Observation> {
    let cores = std::thread::available_parallelism().map_or(1, |n| n.get());
    let load_before = measure::load_average();

    let child_before = measure::children_cpu();
    let parent_before = measure::process_cpu();
    let heartbeat = measure::Heartbeat::start();
    let started = Instant::now();

    // Process spawning is edge tooling, outside the deterministic core
    // (DESIGN.md §1) — this crate lives outside the boundary by construction.
    let status = Command::new("cargo")
        .args(["build"])
        .current_dir(dir)
        .status()?;

    // AFTER the wait: RUSAGE_CHILDREN counts only reaped children.
    let wall = started.elapsed();
    let child_cpu = measure::children_cpu().saturating_sub(child_before);
    let parent_cpu = measure::process_cpu().saturating_sub(parent_before);
    let suspension = heartbeat.finish();
    let load_after = measure::load_average();

    Ok(Observation {
        phase,
        cache,
        wall,
        child_cpu,
        parent_cpu,
        cores,
        load_before,
        load_after,
        suspension,
        succeeded: status.success(),
    })
}
