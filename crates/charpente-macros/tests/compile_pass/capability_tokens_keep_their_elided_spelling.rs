// Tying the capability tokens and `Ctx` to their group (tenancy-completion
// spec §5) changes no source an app writes: handlers still take `ctx: &Ctx`
// and `cap: &CrossTenant`, a helper that passes `ctx` on still compiles, a
// job's `run` still takes `cap: &CrossTenant`, and operator code still mints
// a token and then stages with it in the same group. The one spelling that
// changes is a helper returning a borrow out of a `Ctx`, which names that
// borrow (`subject_of`); left elided it is E0106. trybuild runs `main`.
use charpente::auth::Principal;
use charpente::http::{ApiError, Ctx, Cursor, RouteDef, Subscription};
use charpente::kernel::{JobEntry, JobError};
use charpente::wire::WireU64;
use charpente::{
    CrossTenant, Db, Id, Scoped, ScopedTx, Tenant, TenantAdmission, Tx, event, job, route, table,
};

#[table(global)]
struct Org {
    id: Id<Org>,
    name: String,
}

impl Tenant for Org {
    const NAME: &'static str = "Org";
    type Who = Principal;
    type Error = ApiError;
    fn authorize(_tx: &Tx, _who: &Principal, _scope: Id<Org>) -> Result<(), ApiError> {
        Ok(())
    }
}

#[table(scoped_by = Org)]
struct Memo {
    id: Id<Memo>,
    text: String,
}

#[event(global)]
struct MemoNoted {
    n: WireU64,
}

/// A helper that takes `ctx` and passes it on, as handlers share bodies.
fn memo_count(tx: &mut Tx, org: Id<Org>, ctx: &Ctx) -> Result<WireU64, ApiError> {
    let tx = tx.scope::<Org>(org, ctx)?;
    Ok(WireU64(tx.scan::<Memo>()?.len() as u64))
}

/// A helper that returns a borrow out of a `Ctx` names which borrow.
fn subject_of<'c>(ctx: &'c Ctx) -> Option<&'c str> {
    ctx.principal().map(Principal::subject)
}

#[route(GET "/orgs/:org/memos", auth)]
fn memos(tx: &mut Tx, org: Id<Org>, ctx: &Ctx) -> Result<WireU64, ApiError> {
    let _ = subject_of(ctx);
    memo_count(tx, org, ctx)
}

#[route(GET "/orgs/:org/everything", public, cross_tenant)]
fn everything(
    tx: &mut Tx,
    org: Id<Org>,
    cap: &CrossTenant,
    ctx: &Ctx,
) -> Result<WireU64, ApiError> {
    let _ = (org, ctx.cap().is_some());
    Ok(WireU64(
        tx.across_tenants::<Org>(cap).scan::<Memo>()?.len() as u64,
    ))
}

#[route(SSE "/orgs/:org/feed", auth, emits(MemoNoted))]
fn feed(
    tx: &mut Tx,
    org: Id<Org>,
    ctx: &Ctx,
    cursor: Option<Cursor>,
) -> Result<Subscription, ApiError> {
    let _ = (tx, org, subject_of(ctx));
    Ok(Subscription::of::<MemoNoted>().cursor(cursor))
}

#[job(global, cross_tenant)]
struct Sweep {}

impl Sweep {
    fn run(self, tx: &mut Tx, cap: &CrossTenant) -> Result<(), JobError> {
        for org in [1, 2] {
            tx.enqueue_in(Id::<Org>::from_raw(org), Touch { n: org }, cap);
        }
        Ok(())
    }
}

#[job(scoped_by = Org, cross_tenant)]
struct Touch {
    n: u64,
}

impl Touch {
    fn run(self, tx: &mut ScopedTx<'_, '_, Org>, cap: &CrossTenant) -> Result<(), JobError> {
        let _ = tx.across_tenants::<Org>(cap).scan::<Memo>()?;
        tx.put(&Memo {
            id: Id::from_raw(self.n),
            text: String::new(),
        });
        Ok(())
    }
}

/// Operator code: mints both tokens from a group, then stages with them in
/// that same group, the shape the dispatcher and the request path share.
fn seed(db: &mut Db, org: Id<Org>) -> std::io::Result<()> {
    let mut group = db.begin_group()?;
    let admission = group.tenant_admission();
    let cap = group.cross_tenant_cap();
    let staged = group.stage(|tx| {
        let before = tx.across_tenants::<Org>(&cap).scan::<Memo>()?.len() as u64;
        tx.admit_scope::<Org>(org, &admission).put(&Memo {
            id: Id::from_raw(before),
            text: "seeded".to_string(),
        });
        Ok::<(), std::io::Error>(())
    });
    group.commit().map_err(|failure| failure.error)?;
    staged.map_err(|e| std::io::Error::other(format!("{e:?}")))
}

/// A helper that hands the admission on, as `tenant.rs`'s `transact_admitted`
/// does: `&TenantAdmission` in a function type's arguments, elided.
fn with_admission(db: &mut Db, f: fn(&mut Tx, &TenantAdmission)) -> std::io::Result<()> {
    let mut group = db.begin_group()?;
    let admission = group.tenant_admission();
    let staged = group.stage(|tx| {
        f(tx, &admission);
        Ok::<(), std::io::Error>(())
    });
    group.commit().map_err(|failure| failure.error)?;
    staged.map_err(|e| std::io::Error::other(format!("{e:?}")))
}

fn main() {
    assert!(everything::route().cross_tenant);
    assert!(!memos::route().cross_tenant);
    assert!(!feed::route().cross_tenant);
    assert!(<Sweep as JobEntry>::def().cross_tenant);
    assert!(<Touch as JobEntry>::def().cross_tenant);
    let _ = (seed, with_admission);
}
