//! The latch flag's boundary (spec §6). Every *disk-boundary* error latches —
//! that is redb's own condition, mirrored deliberately. What must never latch
//! is a store error that no disk operation produced, or a clean shutdown.

use charpente_core::table::Store;
use charpente_sim::{FaultConfig, Sim};

fn store(sim: &Sim) -> Store {
    Store::open(&sim.disk(), "store.redb").unwrap()
}

#[test]
fn a_fresh_store_is_not_latched() {
    let sim = Sim::with_faults(0x0DED_0003, FaultConfig::none());
    let store = store(&sim);
    assert!(
        !store.failed(),
        "a store that has seen no IO error must not report a latch"
    );
}

#[test]
fn a_missing_table_does_not_latch() {
    let sim = Sim::with_faults(0x0DED_0004, FaultConfig::none());
    let store = store(&sim);
    // `applied_seq` on a virgin store takes the TableDoesNotExist arm
    // (table.rs:329) — an ordinary Ok(None), no disk error involved.
    assert_eq!(store.applied_seq().unwrap(), None);
    assert!(
        !store.failed(),
        "TableDoesNotExist is an ordinary result, not a disk failure — latching \
         here would mean the flag had drifted into 'any error whatsoever is fatal'"
    );
}

#[test]
fn a_graceful_close_does_not_latch() {
    let sim = Sim::with_faults(0x0DED_0005, FaultConfig::none());
    // redb's own CheckedBackend::close sets io_failed unconditionally
    // (cached_file.rs:148). charpente deliberately does not mirror that, so a
    // store dropped cleanly and re-opened must come back unlatched.
    {
        let store = store(&sim);
        store.checkpoint().unwrap();
        assert!(
            !store.failed(),
            "checkpoint on a healthy store must not latch"
        );
    }
    let reopened = store(&sim);
    assert!(
        !reopened.failed(),
        "a store re-opened after a clean close must not report a latch"
    );
}
