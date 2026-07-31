//! The latch flag's boundary (spec §6). Every *disk-boundary* error latches —
//! that is redb's own condition, mirrored deliberately. What must never latch
//! is a store error that no disk operation produced, or a healthy sync.
//!
//! The clean-shutdown half of the boundary is NOT assertable here: `Store::open`
//! mints a fresh flag (table.rs:307), so anything re-opened reads clean whether
//! or not `close` latched. It is guarded one layer down, against `DiskBackend`
//! itself, by `table.rs`'s `latch_tests::a_close_does_not_latch`.

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
fn a_healthy_checkpoint_and_reopen_do_not_latch() {
    let sim = Sim::with_faults(0x0DED_0005, FaultConfig::none());
    {
        let store = store(&sim);
        // A checkpoint is the only store sync charpente makes (Store::apply
        // commits at Durability::None), so this is the exact call the latch
        // fires on when the disk is faulty — here it must not.
        store.checkpoint().unwrap();
        assert!(
            !store.failed(),
            "checkpoint on a healthy store must not latch"
        );
    }
    // Not evidence about `close` — see the module comment. What it does carry:
    // opening over an EXISTING checkpointed file reads redb's header at the
    // disk boundary, and those reads must not latch either.
    let reopened = store(&sim);
    assert!(
        !reopened.failed(),
        "re-opening a healthy checkpointed store must not report a latch"
    );
}
