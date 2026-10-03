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
//! handler receives a `Tx`, and a scoped job the `ScopedTx` of the tenant it
//! was enqueued in; through a `Tx`, a scoped table is unreachable without
//! `tx.scope::<Org>(org, ctx)?`, which runs that tenant's own `authorize` —
//! or, on a route declared `public(Org)`, admits by that declaration —
//! before it hands back anything that can read or write. That
//! is the property a reviewer can check by reading one line per handler, and
//! it is enforced by the compiler rather than by convention.
//!
//! It is not a sandbox around the process. An app's own `main` builds the
//! `App` it hands `launch`, so it holds `App::db()`, and a `Db` reaches every
//! tenant — `Db::snapshot` yields scoped and raw reads that take a scope on
//! the caller's word with no `authorize` on the path, and those reads stay
//! public because replication seeding and the archive are built on them.
//! Whoever holds the `Db` holds the store, with the same standing as whoever
//! holds the data directory: an operator, not a request.
//!
//! Code outside a handler never takes a scope on its own word. A job declares
//! `#[job(scoped_by = Org)]` and runs in the tenant its enqueuer opened or its
//! fan-out named, and nothing else decides which: no app code can write the
//! job row that carries that tenant (`JobRow`'s scope is `PlatformOwned`, so
//! `Tx::put`/`Tx::delete` refuse it; the dispatcher settles one through
//! `Tx::settle_job`), and `Group::stage` refuses an enqueue inside a tenant of
//! another scope type than the job's mounted `#[job]` declares, since the row
//! records the tenant's id and not its type. `JobScope::open` holds a mounted
//! job's glue to that same type at dispatch. That check holds within one
//! binary. Across binaries, a job's fingerprint carries its tenant type's
//! `NAME`, so a Pending row enqueued under a tenant type of another `NAME` is
//! refused at startup as a foreign shape. What still runs unchecked, in the type
//! the dispatching binary declares, is a row whose shape matches: one a
//! hand-written `impl Job` enqueued from a binary that did not mount the job, or
//! one enqueued under another tenant type with the same `NAME`, which is that
//! tenant by declaration. GUIDE §6 and DESIGN.md §3's Jobs bullet state that
//! residual and the drain that avoids it. Operator code that must act inside one tenant — seeding, a test oracle —
//! opens it with a `TenantAdmission` minted from a `Group` (`Tx::admit_scope`),
//! the seal `CrossTenant` already carries, which is the operator's standing
//! made explicit rather than an exception to the guarantee. *(Corrected
//! 2026-09-27, job-tenancy spec: the earlier remedy named a `Ctx` that no code
//! outside a handler holds.)*

use charpente_core::db::{ScopedTx, Tenant, Tx};
use charpente_core::table::Id;

use crate::auth::Principal;
use crate::http::{ApiError, Ctx};

/// The app's own membership check, as an app writes it: `Tenant` with the
/// platform's two types filled in. Blanket-implemented, so implementing
/// `Tenant` for a scope type is what earns it — there is nothing to write
/// here directly.
///
/// `authorize` is asked only about a real principal: `tx.scope`'s rule 3
/// answers a caller with no credential with a 401, and a route declared
/// `public(S)` admits without asking (`Scoped::scope`).
///
/// It is a named trait rather than the bound spelled out at each use because
/// `tenant_name` is bounded on it, and `#[table(scoped_by = Org)]`,
/// `public(Org)` and `#[job(scoped_by = Org)]` all emit that, and because
/// `#[diagnostic::on_unimplemented]` needs somewhere to hang: a bare
/// `Tenant<Who = Principal, Error = ApiError>` bound fails as an
/// associated-type mismatch, which no custom diagnostic can decorate.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is used as a tenant scope but has no `Tenant` impl",
    label = "no `impl Tenant for {Self}`",
    note = "a tenant scope needs a name and the app's own membership check; add:\n\
            impl Tenant for {Self} {{\n    \
            const NAME: &'static str = \"{Self}\";\n    \
            type Who = Principal;\n    \
            type Error = ApiError;\n    \
            fn authorize(tx: &Tx, who: &Principal, scope: Id<Self>) -> Result<(), ApiError> {{ … }}\n\
            }}"
)]
pub trait TenantScope: Tenant<Who = Principal, Error = ApiError> {}

/// `do_not_recommend`, so a scope type with no `Tenant` impl at all is told
/// to add one by the diagnostic above, instead of being walked through this
/// impl's own unsatisfied `Tenant<Who = …, Error = …>` obligation.
#[diagnostic::do_not_recommend]
impl<T: Tenant<Who = Principal, Error = ApiError>> TenantScope for T {}

/// A tenant type's `NAME`, read through the platform's bound.
/// `#[table(scoped_by = S)]`, `#[route(…, public(S))]` and
/// `#[job(scoped_by = S)]` read `S`'s identity through this, not through
/// `<S as Tenant>::NAME`. Through this, a scope type with no `Tenant` impl
/// gets `TenantScope`'s diagnostic above and nothing else. The bare
/// projection adds rustc's own "the trait bound `S: Tenant` is not
/// satisfied" beside it. This bound is the one those macros' const asserts
/// carried, so it replaces them (tenancy-completion spec §4). It is a
/// `const fn`, so `SCOPE_NAME`, `PublicScope` and a job's `FINGERPRINT` stay
/// constants.
#[doc(hidden)]
pub const fn tenant_name<S: TenantScope>() -> &'static str {
    S::NAME
}

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
    /// An `auth` request never reaches rule 3, because `build_ctx` refused
    /// the anonymous caller before the handler ran. So the platform never
    /// asks `authorize` about nobody.
    fn scope<S: TenantScope>(
        &mut self,
        scope: Id<S>,
        ctx: &Ctx,
    ) -> Result<ScopedTx<'_, 'a, S>, ApiError> {
        if let Some(admission) = ctx.public_admission::<S>() {
            return Ok(self.admit_scope(scope, admission));
        }
        match ctx.principal() {
            Some(principal) => self.open_scope(scope, principal),
            None => Err(ctx.anonymous_scope::<S>()),
        }
    }
}
