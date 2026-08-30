//! The tenant-scope membership contract (M2 tenant-scoping sub-project,
//! docs/superpowers/specs/2026-08-30-tenant-scoping-design.md §1). Lives
//! here, not in `charpente-core`: the trait's signature names `Principal`
//! (`crate::auth::Principal`) and `ApiError` (`crate::http::ApiError`),
//! both platform-edge types with no reverse dependency from
//! `charpente-core` — DESIGN.md's deferred multi-tenancy note (§8's last
//! bullet, DESIGN.md:1377) is answered by the full design here, not by
//! moving auth types into the deterministic core.

use charpente_core::db::Tx;
use charpente_core::table::Id;

use crate::auth::Principal;
use crate::http::ApiError;

/// The app's own membership check for a tenant scope. `#[table(scoped_by =
/// Org)]` requires `impl TenantScope for Org` to exist — enforced by a
/// macro-emitted const assert, the same shape `#[table(version = …)]` uses
/// to require a `TableMigration` impl (DESIGN §3). The platform calls
/// `authorize` exactly once, when a handler opens a scope (a later task);
/// it never interprets `scope` itself (the standing `emit_keyed` ruling,
/// DESIGN.md:1060-1062).
#[diagnostic::on_unimplemented(
    message = "`{Self}` is used as a tenant scope but has no `TenantScope` impl",
    label = "no `impl TenantScope for {Self}`",
    note = "a scoped table needs the app's own membership check; add:\n\
            impl TenantScope for {Self} {{ fn authorize(tx, who, scope) -> Result<(), ApiError> {{ … }} }}"
)]
pub trait TenantScope: Sized {
    fn authorize(tx: &Tx, who: Option<&Principal>, scope: Id<Self>) -> Result<(), ApiError>;
}
