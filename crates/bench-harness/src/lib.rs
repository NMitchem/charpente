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

pub mod measure;
pub mod report;

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

/// The invoicing idiom (examples/invoicing/src/lib.rs:19): one free function
/// shared by the App builder and any bare-`Db` consumer (the core layer).
pub fn schema() -> Schema {
    Schema::new().table::<Slot>()
}

pub fn build(disk: Box<dyn Disk>, entropy: Box<dyn Entropy>) -> std::io::Result<App> {
    Ok(App::open(disk, schema(), entropy, AppConfig::default())?
        .mount(routes![hit, read_slot])
        .mount_jobs(jobs![Bump]))
}
