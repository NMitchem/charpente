//! The platform surface applications link against: typed tables, routes,
//! jobs, events, config, telemetry, and the HTTP edge (DESIGN.md §3, §5–§6).
//! Re-exports the deterministic core and the derive macros.

pub use charpente_core as core;
pub use charpente_core::db::{Db, DbConfig, Event, Job, Schema, Tx, TxError};
pub use charpente_core::table::{Id, Snapshot, View};
pub use charpente_macros::table;

pub mod prod;
