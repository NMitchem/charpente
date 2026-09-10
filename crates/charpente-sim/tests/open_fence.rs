//! F1 child arms, driven by `crates/edge-harness/tests/open_fence.rs` as
//! separate `cargo test -p charpente-sim --test open_fence <name>`
//! subprocesses (F1 round 2, Important-3 ruling).
//!
//! They live here rather than in `edge-harness` because `edge-harness`'s
//! empty `[dev-dependencies]` is a MECHANICAL guarantee, not a review
//! convention: the edge rig spawns built binaries, it never links against
//! them (`crates/edge-harness/src/lib.rs`'s module doc), so the rig can
//! never shortcut through the simulator it exists to test from outside.
//! Both tests below call `charpente_sim::harness::install_open_fence_hook`
//! and reach into `charpente_core::table` directly, which is exactly the
//! link `edge-harness` must not carry — so the code that DOES that lives on
//! this side of the boundary, and `edge-harness` only ever shells out to it.
//!
//! Each test is gated on `CHARPENTE_F1_PANIC_CHILD`: a no-op unless a caller
//! sets it, so a plain `cargo test -p charpente-sim` run (including Step 10's
//! full-package sweep) stays quiet — these deliberately provoke panics that
//! are only interesting when a parent process is watching this one's stderr.

use charpente_core::io::{Disk, File};
use charpente_core::table::Store;

/// F1: the fence must not become a blanket mute. A panic OUTSIDE the fence
/// still reaches the hook in full.
#[test]
fn a_panic_outside_the_open_fence_still_prints() {
    if std::env::var("CHARPENTE_F1_PANIC_CHILD").is_ok() {
        charpente_sim::harness::install_open_fence_hook();
        assert!(!charpente_core::table::in_open_fence());
        panic!("deliberate out-of-fence panic");
    }
}

/// The literal panic message both `PanickyFile`'s methods raise and
/// `crates/edge-harness/tests/open_fence.rs`'s matching test asserts on —
/// duplicated across the crate boundary the same way
/// `"deliberate out-of-fence panic"` already is above; the two are proven to
/// agree by the child actually running (Important-2's fix), not by sharing a
/// constant.
const NON_REDB_PANIC: &str =
    "deliberate non-redb panic reachable inside the open fence (F1 Important-1)";

/// A `Disk`/`File` whose every method panics from a location inside THIS
/// file — never inside redb's own source. `Store::open` hands a `DiskBackend`
/// wrapping this to `Builder::create_with_backend`, which calls straight
/// through to one of these methods synchronously, while `Store::open`'s
/// fence guard is active — the same shape `SimFile`'s own panics
/// (`crates/charpente-sim/src/lib.rs`'s `file_state` and `read_at`) would
/// take in production, stood in with a fixture that fires reliably instead
/// of needing a specific disk-corruption scenario to reach them.
struct PanickyDisk;

impl Disk for PanickyDisk {
    fn open(&self, _path: &str) -> std::io::Result<Box<dyn File>> {
        Ok(Box::new(PanickyFile))
    }

    fn remove(&self, _path: &str) -> std::io::Result<()> {
        Ok(())
    }

    fn list(&self, _prefix: &str) -> std::io::Result<Vec<String>> {
        Ok(Vec::new())
    }

    fn sync_dir(&self, _prefix: &str) -> std::io::Result<()> {
        Ok(())
    }
}

struct PanickyFile;

impl File for PanickyFile {
    fn len(&self) -> std::io::Result<u64> {
        panic!("{NON_REDB_PANIC}");
    }

    fn read_at(&self, _offset: u64, _out: &mut [u8]) -> std::io::Result<()> {
        panic!("{NON_REDB_PANIC}");
    }

    fn write_at(&self, _offset: u64, _data: &[u8]) -> std::io::Result<()> {
        panic!("{NON_REDB_PANIC}");
    }

    fn set_len(&self, _len: u64) -> std::io::Result<()> {
        panic!("{NON_REDB_PANIC}");
    }

    fn sync(&self) -> std::io::Result<()> {
        panic!("{NON_REDB_PANIC}");
    }
}

/// F1 Important-1: the mute must be scoped to redb's OWN assert, not merely
/// to the fence's time window. A panic reachable inside the fence but NOT
/// originating in redb must still reach the log in full.
#[test]
fn a_non_redb_panic_inside_the_open_fence_still_prints() {
    if std::env::var("CHARPENTE_F1_PANIC_CHILD").is_ok() {
        charpente_sim::harness::install_open_fence_hook();
        // `Store::open`'s own fence catches this internally (any open-time
        // panic becomes an `Err`, same as redb's) and returns — the point
        // under test is what the HOOK printed before that, not this call's
        // return value.
        let _ = Store::open(&PanickyDisk, "whatever.redb");
    }
}
