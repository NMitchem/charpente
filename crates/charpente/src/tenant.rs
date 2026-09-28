//! The platform's specialization of the tenant-scope membership contract
//! (M2 tenant-scoping sub-project,
//! docs/superpowers/specs/2026-08-30-tenant-scoping-design.md §1).
//!
//! The contract itself — `Tenant`, `Tx::open_scope`, `ScopedTx` — lives in
//! `charpente-core`, because the seam it guards (the six scope-taking
//! primitives on `Tx`) is private to that crate and Rust has no cross-crate
//! "friend" visibility: a `pub` seam is reachable from any handler holding a
//! bare `&mut Tx`. What stays here is everything that names a platform-edge
//! type — `Principal` (`crate::auth::Principal`), `ApiError`
//! (`crate::http::ApiError`) and `Ctx`, none of which `charpente-core` may
//! depend on (ruling R2, amended by R45; DESIGN.md §8's multi-tenancy
//! bullet).
//!
//! # What the guarantee covers (ruling R48)
//!
//! The guarantee is about the typed surface the framework hands app logic. A
//! handler and a job receive a `Tx`; through a `Tx`, a scoped table is
//! unreachable without `tx.scope::<Org>(org, ctx)?`, which runs that tenant's
//! own `authorize` — or, on a route declared `public(Org)`, admits by that
//! declaration — before it hands back anything that can read or write. That
//! is the property a reviewer can check by reading one line per handler, and
//! it is enforced by the compiler rather than by convention.
//!
//! It is not a sandbox around the process. An app's own `main` builds the
//! `App` it hands `launch`, so it holds `App::db()`, and a `Db` reaches every
//! tenant — `Db::snapshot` yields scoped and raw reads that take a scope on
//! the caller's word with no `authorize` on the path, and those reads stay
//! public because replication seeding and the archive are built on them.
//! Whoever holds the `Db` holds the store, with the same standing as whoever
//! holds the data directory: an operator, not a request. Code outside a
//! handler that wants the guarantee should go through a transaction and
//! `tx.scope::<Org>(org, ctx)?` like a handler does.

use charpente_core::db::{ScopedTx, Tenant, Tx};
use charpente_core::table::Id;

use crate::auth::Principal;
use crate::http::{ApiError, Ctx};

/// The app's own membership check, as an app writes it: `Tenant` with the
/// platform's two types filled in. Blanket-implemented, so implementing
/// `Tenant` for a scope type is what earns it — there is nothing to write
/// here directly.
///
/// `who` is `None` only for a direct `Tx::open_scope(scope, None)` call.
/// `tx.scope` never passes `None`: rule 3 answers a caller with no credential
/// with a 401 instead (`Scoped::scope`). So an `authorize` that admits `None`
/// admits exactly those direct calls.
///
/// It is a named trait rather than the bound spelled out at each use because
/// `#[table(scoped_by = Org)]`'s const assert names it
/// (`::charpente::TenantScope`), and because
/// `#[diagnostic::on_unimplemented]` needs somewhere to hang: a bare
/// `Tenant<Who = Principal, Error = ApiError>` bound fails as an
/// associated-type mismatch, which no custom diagnostic can decorate.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is used as a tenant scope but has no `Tenant` impl",
    label = "no `impl Tenant for {Self}`",
    note = "a scoped table needs the app's own membership check; add:\n\
            impl Tenant for {Self} {{\n    \
            type Who = Principal;\n    \
            type Error = ApiError;\n    \
            fn authorize(tx: &Tx, who: Option<&Principal>, scope: Id<Self>) -> Result<(), ApiError> {{ … }}\n\
            }}"
)]
pub trait TenantScope: Tenant<Who = Principal, Error = ApiError> {}

/// `do_not_recommend`, so a scope type with no `Tenant` impl at all is told
/// to add one by the diagnostic above, instead of being walked through this
/// impl's own unsatisfied `Tenant<Who = …, Error = …>` obligation.
#[diagnostic::do_not_recommend]
impl<T: Tenant<Who = Principal, Error = ApiError>> TenantScope for T {}

/// `tx.scope::<Org>(org, ctx)?` — the one line that opens a tenant scope.
///
/// An extension trait rather than an inherent method because `Tx` is a
/// `charpente-core` type and this signature names `Ctx` and `ApiError`, both
/// of which live here (ruling R2). Re-exported at the crate root, so a
/// handler's `use charpente::…` brings it into scope with everything else.
pub trait Scoped<'a> {
    fn scope<S: TenantScope>(
        &mut self,
        scope: Id<S>,
        ctx: &Ctx,
    ) -> Result<ScopedTx<'_, 'a, S>, ApiError>;
}

impl<'a> Scoped<'a> for Tx<'a> {
    /// The three rules, in order (job-tenancy spec §4):
    ///
    /// 1. The route declared `public(S)` for this `S`: admitted by that
    ///    declaration, and no `authorize` runs, whoever the caller is.
    /// 2. A principal is present: `open_scope` runs `S::authorize` once,
    ///    about that principal, and its refusal is the answer.
    /// 3. Otherwise, the named 401, and nothing below the scope line runs.
    ///
    /// An `auth` route never reaches rule 3, because `build_ctx` refused the
    /// anonymous caller before the handler ran. So the platform never asks
    /// `authorize` about nobody.
    fn scope<S: TenantScope>(
        &mut self,
        scope: Id<S>,
        ctx: &Ctx,
    ) -> Result<ScopedTx<'_, 'a, S>, ApiError> {
        if let Some(admission) = ctx.public_admission::<S>() {
            return Ok(self.admit_scope(scope, admission));
        }
        match ctx.principal() {
            Some(principal) => self.open_scope(scope, Some(principal)),
            None => Err(ctx.anonymous_scope::<S>()),
        }
    }
}
