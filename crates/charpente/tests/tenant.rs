//! The tenant membership contract (M2 tenant-scoping sub-project,
//! docs/superpowers/specs/2026-08-30-tenant-scoping-design.md §1): opening a
//! scope runs the app's own `authorize`, and nothing reaches a tenant's rows
//! without it. The compile-time half of the property is pinned by trybuild
//! (`crates/charpente-macros/tests/compile_fail/scoped_*.rs`); this file pins
//! the runtime half, which no compile-fail case can express — that the one
//! auditable line per handler actually calls the app back, and that its
//! refusal is what the client gets.

use std::sync::atomic::{AtomicU64, Ordering};

use charpente::auth::Principal;
use charpente::http::{ApiError, App, AppConfig, Ctx, Method, Request, Response};
use charpente::wire::WireU64;
use charpente::{Schema, Scoped, Tenant, Tx, route, routes, table};
use charpente_core::table::Id;
use charpente_sim::{FaultConfig, Sim};

/// The scope type whose membership check actually refuses. Every other
/// `Tenant` impl in the suite is unconditionally `Ok(())`, and a suite of
/// only-`Ok` impls cannot tell whether `authorize` runs at all — which is why
/// this one exists.
///
/// A `#[table]` rather than a bare marker only so `Id<Org>` can be a route
/// path parameter (`Id<R>`'s `Wire` impl is bounded on `R: Row`); nothing
/// reads the table. A scope type owes nothing but `authorize`, and the
/// platform never interprets the scope (the standing `emit_keyed` ruling,
/// DESIGN.md:1060-1062).
#[table]
struct Org {
    id: Id<Org>,
    name: String,
}

/// The one tenant `authorize` admits. A scope-dependent verdict, not a blanket
/// one: a test whose `authorize` always refused could not tell "the check ran"
/// from "the route is broken".
const ADMITTED: u64 = 1;
const REFUSED: u64 = 2;

/// Counts entries into `authorize`, so the test can assert the check ran at
/// all — an assertion on the status code alone would also pass if the platform
/// refused for its own reasons and never asked the app.
static AUTHORIZE_CALLS: AtomicU64 = AtomicU64::new(0);

/// Counts handler bodies that got PAST `scope()`. This is the "no scoped read
/// happened" half: a refusal that still let the body run would leave this
/// nonzero even with the response mapped to 401.
static BODIES_ENTERED: AtomicU64 = AtomicU64::new(0);

impl Tenant for Org {
    type Who = Principal;
    type Error = ApiError;

    fn authorize(_tx: &Tx, _who: Option<&Principal>, scope: Id<Org>) -> Result<(), ApiError> {
        AUTHORIZE_CALLS.fetch_add(1, Ordering::Relaxed);
        if scope.raw() == ADMITTED {
            Ok(())
        } else {
            Err(ApiError::BadRequest(format!(
                "org {} is not one of this principal's orgs",
                scope.raw()
            )))
        }
    }
}

#[table(scoped_by = Org)]
struct Memo {
    id: Id<Memo>,
    text: String,
}

/// The shape spec §1 prescribes: one `scope()` line, everything below it
/// bounded to that tenant. Returns the tenant's row count BEFORE the write, so
/// a body that ran when it should not have is visible in the response too.
#[route(POST "/memo/:org")]
fn write_memo(tx: &mut Tx, org: Id<Org>, ctx: &Ctx) -> Result<WireU64, ApiError> {
    let mut tx = tx.scope::<Org>(org, ctx)?;
    BODIES_ENTERED.fetch_add(1, Ordering::Relaxed);
    let before = tx.scan::<Memo>()?.len() as u64;
    tx.put(&Memo {
        id: Id::from_raw(7),
        text: format!("memo for org {}", org.raw()),
    });
    Ok(WireU64(before))
}

/// Reads one tenant's rows back, so the refused tenant's emptiness is checked
/// on the store and not only on the counters.
#[route(GET "/memo/:org")]
fn count_memos(tx: &mut Tx, org: Id<Org>, ctx: &Ctx) -> Result<WireU64, ApiError> {
    let tx = tx.scope::<Org>(org, ctx)?;
    Ok(WireU64(tx.scan::<Memo>()?.len() as u64))
}

fn memo_app(seed: u64) -> (Sim, App) {
    let sim = Sim::with_faults(seed, FaultConfig::none());
    let app = App::open(
        Box::new(sim.disk()),
        Schema::new().table::<Memo>(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )
    .unwrap()
    .mount(routes![write_memo, count_memos]);
    (sim, app)
}

fn body_text(response: &Response) -> String {
    String::from_utf8(response.body.clone()).unwrap()
}

/// A refusing `authorize` is what the client gets, and nothing below the
/// `scope()` line runs.
///
/// Four of its assertions go RED if the `S::authorize(...)?` call disappears
/// from `Tx::open_scope`, the single place it runs — this one from the app's
/// side of the platform boundary, `charpente-core/tests/engine.rs`'s
/// `a_refused_scope_hands_back_the_apps_own_error_and_no_handle` from core's.
/// Without them that deletion is invisible, and the design's whole
/// audit-surface thesis rests on an unpinned line.
#[test]
fn a_refusing_authorize_is_the_answer_and_nothing_below_the_scope_line_runs() {
    let (_sim, mut app) = memo_app(0x7E4A_1701);
    AUTHORIZE_CALLS.store(0, Ordering::Relaxed);
    BODIES_ENTERED.store(0, Ordering::Relaxed);

    // Positive control first: the admitted tenant proves the route works, so a
    // 400 below is the membership verdict and not a broken handler.
    let admitted = app.handle(Request::new(Method::Post, format!("/memo/{ADMITTED}")));
    assert_eq!(admitted.status, 200, "{}", body_text(&admitted));
    assert_eq!(
        AUTHORIZE_CALLS.load(Ordering::Relaxed),
        1,
        "opening a scope must call the app's own authorize, exactly once"
    );
    assert_eq!(
        BODIES_ENTERED.load(Ordering::Relaxed),
        1,
        "an admitted tenant's handler body runs"
    );

    let refused = app.handle(Request::new(Method::Post, format!("/memo/{REFUSED}")));
    assert_eq!(
        refused.status,
        400,
        "the app's own refusal maps to its own ApiError, not a platform error: {}",
        body_text(&refused)
    );
    assert_eq!(
        body_text(&refused),
        "{\"error\":\"org 2 is not one of this principal's orgs\"}",
        "the client gets the refusal `authorize` returned, byte for byte"
    );
    assert_eq!(
        AUTHORIZE_CALLS.load(Ordering::Relaxed),
        2,
        "the refused request must have ASKED the app — a platform-side refusal \
         that never called authorize would pass a status-code assertion alone"
    );
    assert_eq!(
        BODIES_ENTERED.load(Ordering::Relaxed),
        1,
        "no handler body ran past the refused `scope()` line"
    );

    // And on the store: the refused tenant has no rows, so no scoped write or
    // read reached it. Asked through the ADMITTED tenant's own scope, since
    // asking through the refused one is (correctly) impossible.
    let admitted_rows = app.handle(Request::new(Method::Get, format!("/memo/{ADMITTED}")));
    assert_eq!(admitted_rows.status, 200, "{}", body_text(&admitted_rows));
    assert_eq!(
        body_text(&admitted_rows),
        "\"1\"",
        "the admitted tenant kept its one row"
    );
    assert_eq!(
        app.db().last_applied(),
        Some(0),
        "exactly one write record: the refused request appended nothing"
    );
}
