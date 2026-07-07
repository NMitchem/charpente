//! The platform surface applications link against: typed tables, routes,
//! jobs, events, config, telemetry, and the HTTP edge (DESIGN.md §3, §5–§6).
//! Re-exports the deterministic core and the derive macros.

// The macros emit `::charpente::` paths; this alias makes them resolve in
// charpente's own src too (Session, CronRow), not just in dependent crates.
extern crate self as charpente;

pub use charpente_core as core;
pub use charpente_core::db::{Db, DbConfig, Event, Job, Schema, Tx, TxError};
pub use charpente_core::table::{Id, Snapshot, View};
pub use charpente_macros::{job, route, table};

pub mod http;
pub mod kernel;
pub mod prod;
pub mod serve;
