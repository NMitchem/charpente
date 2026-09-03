//! The tenant-scope membership contract (M2 tenant-scoping sub-project,
//! docs/superpowers/specs/2026-08-30-tenant-scoping-design.md §1). Lives
//! here, not in `charpente-core`: the trait's signature names `Principal`
//! (`crate::auth::Principal`) and `ApiError` (`crate::http::ApiError`),
//! both platform-edge types with no reverse dependency from
//! `charpente-core` — DESIGN.md's deferred multi-tenancy note (§8's last
//! bullet, DESIGN.md:1377) is answered by the full design here, not by
//! moving auth types into the deterministic core.

use std::io;

use charpente_core::db::Tx;
use charpente_core::table::{Codec, Id, Row};

use crate::auth::Principal;
use crate::http::{ApiError, Ctx};

/// The app's own membership check for a tenant scope. `#[table(scoped_by =
/// Org)]` requires `impl TenantScope for Org` to exist — enforced by a
/// macro-emitted const assert, the same shape `#[table(version = …)]` uses
/// to require a `TableMigration` impl (DESIGN §3). The platform calls
/// `authorize` exactly once, when a handler opens a scope (a later task);
/// it never interprets `scope` itself (the standing `emit_keyed` ruling,
/// DESIGN.md:1060-1062).
///
/// Declaration surface only, today: nothing yet reads `Row::Scope` to
/// change how a table is stored or read, so a `scoped_by` table is not yet
/// partitioned — the scoped read path (`ScopedTx`, the composite key) is a
/// later task in this same sub-project.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is used as a tenant scope but has no `TenantScope` impl",
    label = "no `impl TenantScope for {Self}`",
    note = "a scoped table needs the app's own membership check; add:\n\
            impl TenantScope for {Self} {{\n    \
            fn authorize(tx: &Tx, who: Option<&Principal>, scope: Id<Self>) -> Result<(), ApiError> {{ … }}\n\
            }}"
)]
pub trait TenantScope: Sized {
    fn authorize(tx: &Tx, who: Option<&Principal>, scope: Id<Self>) -> Result<(), ApiError>;
}

/// A transaction narrowed to one tenant of one scope type — the only handle
/// an app has on a scoped table (spec §1). `Tx::scope` (the `Scoped`
/// extension trait below) is the only constructor, and it runs
/// `S::authorize` first, so the one auditable line per handler the design's
/// thesis rests on is structural rather than conventional: there is no
/// second way to reach a scoped row, and no way to reach one without the
/// app's own membership check having run.
///
/// Two lifetimes, not one. `'t` is the borrow of the transaction; `'a` is
/// the transaction's own borrow of the snapshot it reads from. They cannot
/// be collapsed: `&'t mut Tx<'a>` is invariant in `'a`, so a handler holding
/// `&'t mut Tx<'a>` (with `'a` strictly longer, since the snapshot outlives
/// the transaction) can never produce the `&'t mut Tx<'t>` a single-lifetime
/// `ScopedTx<'t>` would need. Handlers write `tx.scope::<Org>(org, ctx)?`
/// and never name either.
///
/// `Deref`/`DerefMut` to `Tx` are required, not conveniences: opening a
/// scope must not lock a handler out of unscoped tables (spec §1). Method
/// resolution does NOT fall through the deref when an inherent method of the
/// same name exists but its bound fails, so an unscoped access from inside a
/// scope is written through an explicit deref:
///
/// ```ignore
/// let tx = tx.scope::<Org>(org, ctx)?;
/// tx.scan::<Invoice>()?;      // scoped:   Invoice is scoped_by = Org
/// (*tx).scan::<Region>()?;    // unscoped: Region has no tenant
/// ```
pub struct ScopedTx<'t, 'a, S> {
    tx: &'t mut Tx<'a>,
    scope: Id<S>,
}

impl<'a, S: 'static> ScopedTx<'_, 'a, S> {
    /// The tenant every read and write on this handle is bounded to (R18:
    /// the scope is explicit, and this field is its source). Also the reason
    /// a scope cannot be nested: this shadows the `Scoped::scope` the deref
    /// would otherwise reach, and spec §8 rules nested scopes out.
    pub fn scope(&self) -> Id<S> {
        self.scope
    }

    pub fn get<R: Row<Scope = S>>(&self, id: Id<R>) -> io::Result<Option<R>> {
        self.tx.get_in(self.scope, id)
    }

    /// Every row of `R` belonging to this tenant, in id order.
    pub fn scan<R: Row<Scope = S>>(&self) -> io::Result<Vec<R>> {
        self.tx.scan_in(self.scope)
    }

    /// Exact-match secondary index lookup within this tenant, in id order.
    pub fn scan_by<R: Row<Scope = S>>(
        &self,
        index_name: &str,
        key: &impl Codec,
    ) -> io::Result<Vec<R>> {
        self.tx.scan_by_in(self.scope, index_name, key)
    }

    /// This tenant's rows with `id >= from_id`, in id order, at most `limit`.
    pub fn scan_from<R: Row<Scope = S>>(&self, from_id: u64, limit: usize) -> io::Result<Vec<R>> {
        self.tx.scan_from_in(self.scope, from_id, limit)
    }

    pub fn put<R: Row<Scope = S>>(&mut self, row: &R) {
        self.tx.put_in(self.scope, row)
    }

    pub fn delete<R: Row<Scope = S>>(&mut self, id: Id<R>) {
        self.tx.delete_in(self.scope, id)
    }
}

impl<'a, S> std::ops::Deref for ScopedTx<'_, 'a, S> {
    type Target = Tx<'a>;
    fn deref(&self) -> &Tx<'a> {
        self.tx
    }
}

impl<'a, S> std::ops::DerefMut for ScopedTx<'_, 'a, S> {
    fn deref_mut(&mut self) -> &mut Tx<'a> {
        self.tx
    }
}

/// `tx.scope::<Org>(org, ctx)?` — the one line that opens a tenant scope.
///
/// An extension trait rather than an inherent method because `Tx` is a
/// `charpente-core` type and this signature names `Ctx`, `ApiError` and
/// (through `TenantScope`) `Principal`, all of which live here (ruling R2).
/// Re-exported at the crate root, so a handler's `use charpente::…` brings
/// it into scope with everything else.
pub trait Scoped<'a> {
    fn scope<S: TenantScope + 'static>(
        &mut self,
        scope: Id<S>,
        ctx: &Ctx,
    ) -> Result<ScopedTx<'_, 'a, S>, ApiError>;
}

impl<'a> Scoped<'a> for Tx<'a> {
    /// `authorize` runs here and only here. The platform does not interpret
    /// `scope` (the standing `emit_keyed` ruling, DESIGN.md:1060-1062) — it
    /// only refuses to hand back a `ScopedTx` until the app's own check has
    /// passed.
    fn scope<S: TenantScope + 'static>(
        &mut self,
        scope: Id<S>,
        ctx: &Ctx,
    ) -> Result<ScopedTx<'_, 'a, S>, ApiError> {
        S::authorize(self, ctx.principal(), scope)?;
        Ok(ScopedTx { tx: self, scope })
    }
}
