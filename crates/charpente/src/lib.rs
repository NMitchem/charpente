//! The platform surface applications link against: typed tables, routes,
//! jobs, events, config, telemetry, and the HTTP edge (DESIGN.md §3, §5–§6).
//! Re-exports the deterministic core and the derive macros.

// The macros emit `::charpente::` paths; this alias makes them resolve in
// charpente's own src too (Session, CronRow), not just in dependent crates.
extern crate self as charpente;

pub use charpente_core as core;
pub use charpente_core::db::{
    AllTenants, CrossTenant, Db, DbConfig, Event, Job, Schema, ScopedTx, Seeder, Tenant,
    TenantAdmission, Tx, TxError,
};
pub use charpente_core::table::{Id, Snapshot, View};
pub use charpente_macros::{Wire, event, job, route, row_shape, table};
// Re-exported so `#[event]`'s emitted `Serialize` derive resolves through
// `::charpente::serde` — app crates emitting events stay single-dep on
// `charpente` (macro-path unification, DESIGN.md §3).
pub use serde;

pub mod archive;
pub mod auth;
pub mod describe;
pub mod http;
pub mod kernel;
pub mod launch;
pub mod outbound;
pub mod prod;
pub mod repl;
pub mod serve;
pub mod tenant;
pub mod wire;

pub use launch::launch;
// Re-exported at the root as public API, the same flat-path convention
// `Wire` (from `charpente-macros`) already gets. The macros' output names
// `::charpente::tenant::tenant_name::<S>()`, whose bound is `TenantScope`.
pub use tenant::{Scoped, TenantScope};
// Re-exported at the root so a tenant-feed handler writes
// `use charpente::Subscribe;` beside `Scoped`, the other trait a scoped
// handler brings into scope.
pub use http::Subscribe;
