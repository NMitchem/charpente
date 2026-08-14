//! bench-harness: the M2 sub-project 1 instrument
//! (docs/superpowers/specs/2026-08-06-benchmark-harness-design.md).
//!
//! Everything in this crate is edge tooling outside the deterministic core —
//! real clocks, threads, sockets and processes here are not determinism
//! violations; DESIGN §1's fence is the core, not this crate.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use charpente::core::io::{Disk, Entropy};
use charpente::http::{ApiError, App, AppConfig};
use charpente::kernel::JobError;
use charpente::{Id, Schema, Tx, View};
use charpente::{event, job, jobs, route, routes, table};

pub mod client;
pub mod core_accounting;
pub mod core_bench;
pub mod counting;
pub mod device;
pub mod kernel_accounting;
pub mod kernel_bench;
pub mod measure;
pub mod report;
pub mod wire;
pub mod wire_accounting;

/// Row-id space for the canonical workload: bounded so the store's working
/// set stays cache-resident (the spec measures the commit path, not paging).
pub const SLOTS: u64 = 1024;

#[table]
pub struct Slot {
    pub id: Id<Slot>,
    pub hits: u64,
}

#[event]
pub struct SlotBumped {
    pub slot: u64,
}

#[job]
pub struct Bump {
    pub slot: u64,
}

impl Bump {
    /// The canonical no-op state transition: the handler writes nothing; its
    /// Done-mark still commits one log record (spec §3 — that is the point).
    pub fn run(self, _tx: &mut Tx) -> Result<(), JobError> {
        Ok(())
    }
}

/// The canonical write transaction, README-shaped: get + put + enqueue + emit.
#[route(POST "/hit/:id")]
pub fn hit(tx: &mut Tx, id: u64) -> Result<u64, ApiError> {
    let slot_id = Id::<Slot>::from_raw(id % SLOTS);
    let hits = tx.get(slot_id)?.map_or(0, |s| s.hits) + 1;
    tx.put(&Slot { id: slot_id, hits });
    tx.enqueue(Bump { slot: id % SLOTS });
    tx.emit(SlotBumped { slot: id % SLOTS });
    Ok(hits)
}

#[route(GET "/slot/:id")]
pub fn read_slot(tx: &mut Tx, id: u64) -> Result<u64, ApiError> {
    Ok(tx
        .get(Id::<Slot>::from_raw(id % SLOTS))?
        .map_or(0, |s| s.hits))
}

/// The server-reported durable-sync count `wire_accounting`'s module doc has
/// owed since the 2026-08-10 T1 fix wave: "Either a server-reported sync
/// counter ships (surfaced over HTTP or in the child's exit output), or this
/// gate is re-scoped honestly." Group commit is what made the client-side
/// derivation (`syncs_per_request * throughput`) unsound — N commits can now
/// share one fsync — and T10 attempt 1 duly refused 8 of 8 contended points.
///
/// **Over HTTP rather than at exit, and that is forced, not preferred.** The
/// harness stops the child with `BenchChild::drop`'s `child.kill()`, which is
/// SIGKILL on Unix and uncatchable, so a child cannot print a final count on
/// the way out. Exit output would require replacing the stop mechanism with a
/// catchable signal plus a handler — strictly more machinery than one route on
/// a binary nobody deploys.
///
/// **Why a static.** `#[route]` handlers take `&mut Tx` and nothing else;
/// there is no app-state injection to hand this the disk's counter, so
/// `bench-app` publishes the `Arc` here before it serves. `OnceLock` rather
/// than a mutable static so the publish is single-assignment and needs no
/// unsafe. A layer that never publishes reads 0, which the wire layer treats
/// as "this target does not count" rather than as zero syncs.
///
/// This route is read ONCE, after the measured window closes, so it lands in
/// no throughput figure. It is a read, so it commits nothing and pays no sync
/// of its own — reading the counter cannot move it.
pub static SYNC_COUNTER: std::sync::OnceLock<std::sync::Arc<std::sync::atomic::AtomicU64>> =
    std::sync::OnceLock::new();

#[route(GET "/__syncs")]
pub fn syncs(tx: &mut Tx) -> Result<u64, ApiError> {
    // `#[route]` requires the first parameter to be named exactly `tx` — an
    // `_tx` is a compile error naming the contract, the site and the fix. This
    // handler reads no state, so the binding is discarded rather than renamed.
    let _ = tx;
    Ok(SYNC_COUNTER
        .get()
        .map_or(0, |c| c.load(std::sync::atomic::Ordering::SeqCst)))
}

/// The invoicing idiom (examples/invoicing/src/lib.rs:19): one free function
/// shared by the App builder and any bare-`Db` consumer (the core layer).
pub fn schema() -> Schema {
    Schema::new().table::<Slot>()
}

pub fn build(disk: Box<dyn Disk>, entropy: Box<dyn Entropy>) -> std::io::Result<App> {
    Ok(App::open(disk, schema(), entropy, AppConfig::default())?
        .mount(routes![hit, read_slot, syncs])
        .mount_jobs(jobs![Bump]))
}
