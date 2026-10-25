//! The tenant membership contract (M2 tenant-scoping sub-project,
//! docs/superpowers/specs/2026-08-30-tenant-scoping-design.md §1): opening a
//! scope runs the app's own `authorize`, and nothing reaches a tenant's rows
//! without it. The compile-time half of the property is pinned by trybuild
//! (`crates/charpente-macros/tests/compile_fail/scoped_*.rs`); this file pins
//! the runtime half, which no compile-fail case can express — that the one
//! auditable line per handler actually calls the app back, and that its
//! refusal is what the client gets.
//!
//! Job tenancy (docs/superpowers/specs/2026-09-27-job-tenancy-design.md §4,
//! oracles 2 and 3) adds the route's stance: `public(Org)` admits by
//! declaration with no `authorize`, and a caller with no credential at any
//! other scope line gets a named 401 instead of a call about nobody.
//!
//! It also carries the sub-project's flagship swarm property (hard rule 3):
//! across the full fault matrix, and after every recovery, every scoped read
//! returns ONLY its own tenant's rows — `get`, `scan`, `scan_from` and
//! `scan_by`, two tenants sharing an id space and an index key. Tasks 1-7
//! landed the scoping machinery with compile-fail cases and unit tests; this
//! is the fault-injection oracle they shipped without, and the two naive
//! victims below it are what make its greenness mean something.

use std::cell::{Cell, RefCell};

use charpente::auth::Principal;
use charpente::http::{
    ApiError, App, AppConfig, Ctx, Cursor, Method, Request, Response, Subscription,
};
use charpente::kernel::JobError;
use charpente::wire::WireU64;
use charpente::{
    CrossTenant, Db, DbConfig, Event, Schema, Scoped, ScopedTx, Snapshot, Subscribe, Tenant,
    TenantAdmission, Tx, TxError, event, events, job, jobs, route, routes, table,
};
use charpente_core::db::TableReadRefusal;
use charpente_core::io::Entropy;
use charpente_core::log::WalConfig;
use charpente_core::table::{Codec, Id, Row, View};
use charpente_sim::{FaultConfig, Sim, run_universes_swarm_with};

/// The scope type whose membership check actually refuses. Every other
/// `Tenant` impl in the suite is unconditionally `Ok(())`, and a suite of
/// only-`Ok` impls cannot tell whether `authorize` runs at all — which is why
/// this one exists.
///
/// A `#[table]` rather than a bare marker only so `Id<Org>` can be a route
/// path parameter (`Id<R>`'s `Wire` impl is bounded on `R: Row`); nothing
/// reads the table. A scope type owes its `NAME` and `authorize`, and the
/// platform never DERIVES a scope — it only records one, beside a scoped job
/// or event (the standing `emit_keyed` ruling, DESIGN.md §6).
#[table(global)]
struct Org {
    id: Id<Org>,
    name: String,
}

/// The one tenant `authorize` admits. A scope-dependent verdict, not a blanket
/// one: a test whose `authorize` always refused could not tell "the check ran"
/// from "the route is broken".
const ADMITTED: u64 = 1;
const REFUSED: u64 = 2;

thread_local! {
    /// The subject of every call into `Org::authorize`, in order. So a test
    /// asserts that the check ran at all AND whom it was asked about: a
    /// status code alone would also pass if the platform refused for its own
    /// reasons and never asked the app.
    ///
    /// Thread-local, not a static (job-tenancy amendment A4): seven tests read
    /// these, `cargo test` runs them on parallel threads of one binary, and a
    /// request is handled on the thread that called `App::handle`.
    static AUTHORIZE_CALLS: RefCell<Vec<String>> = const { RefCell::new(Vec::new()) };

    /// Handler bodies that got PAST `scope()`. The "no scoped read happened"
    /// half: a refusal that still let the body run would leave this nonzero
    /// even with the response mapped to an error.
    static BODIES_ENTERED: Cell<u64> = const { Cell::new(0) };
}

fn reset_counters() {
    AUTHORIZE_CALLS.with(|calls| calls.borrow_mut().clear());
    BODIES_ENTERED.with(|n| n.set(0));
}

fn authorize_calls() -> Vec<String> {
    AUTHORIZE_CALLS.with(|calls| calls.borrow().clone())
}

fn bodies_entered() -> u64 {
    BODIES_ENTERED.with(Cell::get)
}

impl Tenant for Org {
    const NAME: &'static str = "Org";
    type Who = Principal;
    type Error = ApiError;

    fn authorize(_tx: &Tx, who: &Principal, scope: Id<Org>) -> Result<(), ApiError> {
        AUTHORIZE_CALLS.with(|calls| calls.borrow_mut().push(who.subject().to_string()));
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
/// Shared by the stance twins below, so they differ in their declaration and
/// nothing else.
fn memo_write(tx: &mut Tx, org: Id<Org>, ctx: &Ctx) -> Result<WireU64, ApiError> {
    let mut tx = tx.scope::<Org>(org, ctx)?;
    BODIES_ENTERED.with(|n| n.set(n.get() + 1));
    let before = tx.scan::<Memo>()?.len() as u64;
    tx.put(&Memo {
        id: Id::from_raw(7),
        text: format!("memo for org {}", org.raw()),
    });
    Ok(WireU64(before))
}

/// Members only: rule 2, with the `auth` gate in front of it.
#[route(POST "/memo/:org", auth)]
fn write_memo(tx: &mut Tx, org: Id<Org>, ctx: &Ctx) -> Result<WireU64, ApiError> {
    memo_write(tx, org, ctx)
}

/// Every caller reaches the handler; the membership check decides the
/// tenant (rule 2), and a caller with no credential gets rule 3's 401.
#[route(POST "/memo-public/:org", public)]
fn write_memo_public(tx: &mut Tx, org: Id<Org>, ctx: &Ctx) -> Result<WireU64, ApiError> {
    memo_write(tx, org, ctx)
}

/// Every caller may act inside the org it names: rule 1, no membership check.
#[route(POST "/memo-open/:org", public(Org))]
fn write_memo_open(tx: &mut Tx, org: Id<Org>, ctx: &Ctx) -> Result<WireU64, ApiError> {
    memo_write(tx, org, ctx)
}

/// Declared `public(Org)`, but it opens `Team`: the declaration names one scope
/// type and admits that type only.
#[route(POST "/memo-team/:team", public(Org))]
fn write_team_memo(tx: &mut Tx, team: Id<Team>, ctx: &Ctx) -> Result<WireU64, ApiError> {
    let tx = tx.scope::<Team>(team, ctx)?;
    BODIES_ENTERED.with(|n| n.set(n.get() + 1));
    Ok(WireU64(tx.scope().raw()))
}

/// Declares `cross_tenant` beside plain `public`. Tenancy-completion spec §7,
/// item 12: rule 3's 401 must not tell the author to drop that flag — its fix
/// used to quote `#[route(POST "/memo-cross/:org", public(Org))]`, which does
/// not compile beside the `cap` argument below.
#[route(POST "/memo-cross/:org", public, cross_tenant)]
fn write_memo_cross(
    tx: &mut Tx,
    org: Id<Org>,
    cap: &CrossTenant,
    ctx: &Ctx,
) -> Result<WireU64, ApiError> {
    memo_write(tx, org, ctx)?;
    Ok(WireU64(
        tx.across_tenants::<Org>(cap).scan::<Memo>()?.len() as u64
    ))
}

/// Reads one tenant's rows back, so the refused tenant's emptiness is checked
/// on the store and not only on the counters.
#[route(GET "/memo/:org", auth)]
fn count_memos(tx: &mut Tx, org: Id<Org>, ctx: &Ctx) -> Result<WireU64, ApiError> {
    let tx = tx.scope::<Org>(org, ctx)?;
    Ok(WireU64(tx.scan::<Memo>()?.len() as u64))
}

/// Scoped by `Org`: the three feeds below build it on `Org`'s `ScopedTx`, so
/// each carries the one org its scope line admitted (tenancy-completion spec
/// §6).
#[event(scoped_by = Org)]
struct MemoNoted {
    n: WireU64,
}

/// A stream route that opens a scope. Driven through `App::handle`, which has
/// no sink for a subscription, so the only answers it can give a test are its
/// errors — which is all rule 3 needs.
#[route(SSE "/memo-feed/:org", public, emits(MemoNoted))]
fn memo_feed(
    tx: &mut Tx,
    org: Id<Org>,
    ctx: &Ctx,
    cursor: Option<Cursor>,
) -> Result<Subscription, ApiError> {
    let tx = tx.scope::<Org>(org, ctx)?;
    Ok(tx.subscription::<MemoNoted>().cursor(cursor).into())
}

#[route(SSE "/memo-feed-open/:org", public(Org), emits(MemoNoted))]
fn memo_feed_open(
    tx: &mut Tx,
    org: Id<Org>,
    ctx: &Ctx,
    cursor: Option<Cursor>,
) -> Result<Subscription, ApiError> {
    let tx = tx.scope::<Org>(org, ctx)?;
    Ok(tx.subscription::<MemoNoted>().cursor(cursor).into())
}

#[route(SSE "/memo-feed-auth/:org", auth, emits(MemoNoted))]
fn memo_feed_auth(
    tx: &mut Tx,
    org: Id<Org>,
    ctx: &Ctx,
    cursor: Option<Cursor>,
) -> Result<Subscription, ApiError> {
    let tx = tx.scope::<Org>(org, ctx)?;
    Ok(tx.subscription::<MemoNoted>().cursor(cursor).into())
}

#[route(POST "/memo-login", public)]
fn memo_login(tx: &mut Tx, subject: String, ctx: &Ctx) -> Result<String, ApiError> {
    Ok(ctx.create_session(tx, subject))
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
    .mount(routes![
        memo_login,
        write_memo,
        write_memo_public,
        write_memo_open,
        write_team_memo,
        write_memo_cross,
        count_memos,
        memo_feed,
        memo_feed_open,
        memo_feed_auth
    ]);
    (sim, app)
}

/// Logs `subject` in and returns its bearer token. The login is one write
/// record, which the `last_applied` assertions below count.
fn login(app: &mut App, subject: &str) -> String {
    let response = app.handle(Request::new(Method::Post, "/memo-login").json(&subject));
    assert_eq!(response.status, 200, "{}", body_text(&response));
    serde_json::from_slice(&response.body).unwrap()
}

fn as_user(request: Request, token: &str) -> Request {
    request.header("Authorization", format!("Bearer {token}"))
}

/// The `error` field of the `{"error": …}` envelope every non-200 carries.
fn error_text(response: &Response) -> String {
    let body: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
    body["error"]
        .as_str()
        .expect("a non-200 body is an error envelope")
        .to_string()
}

fn body_text(response: &Response) -> String {
    String::from_utf8(response.body.clone()).unwrap()
}

/// Oracle 2 (e). A refusing `authorize` is what the client gets, and nothing
/// below the `scope()` line runs — now on `auth` routes, where the caller is
/// always a principal, so the assertion can name WHICH principal was asked.
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
    let token = login(&mut app, "alice");
    reset_counters();

    // Positive control first: the admitted tenant proves the route works, so a
    // 400 below is the membership verdict and not a broken handler.
    let admitted = app.handle(as_user(
        Request::new(Method::Post, format!("/memo/{ADMITTED}")),
        &token,
    ));
    assert_eq!(admitted.status, 200, "{}", body_text(&admitted));
    assert_eq!(
        authorize_calls(),
        vec!["alice".to_string()],
        "opening a scope must call the app's own authorize, exactly once, about the caller"
    );
    assert_eq!(
        bodies_entered(),
        1,
        "an admitted tenant's handler body runs"
    );

    let refused = app.handle(as_user(
        Request::new(Method::Post, format!("/memo/{REFUSED}")),
        &token,
    ));
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
        authorize_calls(),
        vec!["alice".to_string(), "alice".to_string()],
        "the refused request must have ASKED the app, about this principal — a \
         platform-side refusal that never called authorize would pass a status-code \
         assertion alone"
    );
    assert_eq!(
        bodies_entered(),
        1,
        "no handler body ran past the refused `scope()` line"
    );

    // And on the store: the refused tenant has no rows, so no scoped write or
    // read reached it. Asked through the ADMITTED tenant's own scope, since
    // asking through the refused one is (correctly) impossible.
    let admitted_rows = app.handle(as_user(
        Request::new(Method::Get, format!("/memo/{ADMITTED}")),
        &token,
    ));
    assert_eq!(admitted_rows.status, 200, "{}", body_text(&admitted_rows));
    assert_eq!(
        body_text(&admitted_rows),
        "\"1\"",
        "the admitted tenant kept its one row"
    );
    assert_eq!(
        app.db().last_applied(),
        Some(1),
        "exactly two write records, the login and the admitted memo: the refused \
         request appended nothing"
    );
}

/// Oracle 2 (a), spec §4 rule 3. A caller with no credential on a plain
/// `public` route reaches the handler, but not the tenant: the scope line
/// answers the named 401, no `authorize` runs (there is no principal to ask
/// about), and nothing below the line runs or writes.
#[test]
fn an_anonymous_caller_on_a_plain_public_route_that_opens_a_scope_gets_the_named_401() {
    let (_sim, mut app) = memo_app(0x7E4A_1702);
    reset_counters();
    let refused = app.handle(Request::new(
        Method::Post,
        format!("/memo-public/{ADMITTED}"),
    ));
    assert_eq!(refused.status, 401, "{}", body_text(&refused));
    assert_eq!(
        error_text(&refused),
        format!(
            "authentication required — `POST /memo-public/:org` is declared `public` and \
             opened the tenant scope `Org` for a caller with no credential; only a route \
             declared `public(Org)` admits one. Send \"Authorization: Bearer <token>\" or the \
             charpente_session cookie, or, if every caller may act inside this tenant here, \
             declare `public(Org)` in place of `public` in this route's `#[route(…)]` \
             attribute, keeping its other flags. A tenant type is named here by its NAME; \
             name the type in the attribute: `Org` is the tenant type `{}`",
            std::any::type_name::<Org>()
        )
    );
    assert_eq!(
        authorize_calls(),
        Vec::<String>::new(),
        "rule 3 refuses before any membership check — authorize is never asked about nobody"
    );
    assert_eq!(bodies_entered(), 0, "nothing below the scope line ran");
    assert_eq!(app.db().last_applied(), None, "and nothing was written");
}

/// Oracle 2 (d), rule 2 on a plain `public` route: a caller WITH a credential
/// is asked about exactly once, as on an `auth` route. The control for (a):
/// the 401 there is about the missing credential, not about the route.
#[test]
fn a_credentialed_caller_on_a_plain_public_route_is_asked_about_once() {
    let (_sim, mut app) = memo_app(0x7E4A_1704);
    let token = login(&mut app, "alice");
    reset_counters();
    let admitted = app.handle(as_user(
        Request::new(Method::Post, format!("/memo-public/{ADMITTED}")),
        &token,
    ));
    assert_eq!(admitted.status, 200, "{}", body_text(&admitted));
    assert_eq!(authorize_calls(), vec!["alice".to_string()]);
    assert_eq!(bodies_entered(), 1);
}

/// Oracle 2 (g). Rule 3 on a stream: the site in the 401 is the word the
/// route was declared with, `SSE`, not the `GET` it is served on. The route
/// also declares `emits(MemoNoted)`, and the author's fix keeps it: the 401
/// names the stance to change in place and restates no attribute
/// (tenancy-completion spec §7, item 12).
#[test]
fn an_anonymous_subscriber_that_opens_a_scope_gets_the_401_naming_the_stream() {
    let (_sim, mut app) = memo_app(0x7E4A_1707);
    reset_counters();
    let refused = app.handle(Request::new(Method::Get, format!("/memo-feed/{ADMITTED}")));
    assert_eq!(refused.status, 401, "{}", body_text(&refused));
    let text = error_text(&refused);
    assert_eq!(
        text,
        format!(
            "authentication required — `SSE /memo-feed/:org` is declared `public` and opened \
             the tenant scope `Org` for a caller with no credential; only a route declared \
             `public(Org)` admits one. Send \"Authorization: Bearer <token>\" or the \
             charpente_session cookie, or, if every caller may act inside this tenant here, \
             declare `public(Org)` in place of `public` in this route's `#[route(…)]` \
             attribute, keeping its other flags. A tenant type is named here by its NAME; \
             name the type in the attribute: `Org` is the tenant type `{}`",
            std::any::type_name::<Org>()
        )
    );
    assert!(
        !text.contains("#[route(SSE"),
        "the fix restates no attribute, so `emits(MemoNoted)` cannot drop out of it: {text}"
    );
    assert_eq!(authorize_calls(), Vec::<String>::new());
}

/// Oracle 2 (b), rule 1: a `public(Org)` route admits a caller with no
/// credential into the org it names, and asks no membership check.
#[test]
fn public_scope_admits_an_anonymous_caller_with_no_authorize() {
    let (_sim, mut app) = memo_app(0x7E4A_1703);
    reset_counters();
    let admitted = app.handle(Request::new(Method::Post, format!("/memo-open/{ADMITTED}")));
    assert_eq!(admitted.status, 200, "{}", body_text(&admitted));
    assert_eq!(
        authorize_calls(),
        Vec::<String>::new(),
        "rule 1: the declaration admits, and no membership check runs"
    );
    assert_eq!(bodies_entered(), 1);
    assert_eq!(
        app.db().last_applied(),
        Some(0),
        "the admitted write committed"
    );
}

/// Oracle 2 (c), rule 1 before rule 2, and Review Focus 5: on `public(Org)`
/// the declaration beats the membership check. A credentialed caller whose
/// `authorize` WOULD refuse this tenant is admitted with zero calls, and the
/// same caller on the route's `auth` twin is refused by the app's own error.
#[test]
fn public_scope_admits_a_credentialed_caller_its_membership_check_would_refuse() {
    let (_sim, mut app) = memo_app(0x7E4A_1705);
    let token = login(&mut app, "alice");
    reset_counters();

    let control = app.handle(as_user(
        Request::new(Method::Post, format!("/memo/{REFUSED}")),
        &token,
    ));
    assert_eq!(control.status, 400, "{}", body_text(&control));
    assert_eq!(
        error_text(&control),
        "org 2 is not one of this principal's orgs"
    );
    assert_eq!(authorize_calls(), vec!["alice".to_string()]);
    assert_eq!(bodies_entered(), 0);

    reset_counters();
    let admitted = app.handle(as_user(
        Request::new(Method::Post, format!("/memo-open/{REFUSED}")),
        &token,
    ));
    assert_eq!(admitted.status, 200, "{}", body_text(&admitted));
    assert_eq!(
        authorize_calls(),
        Vec::<String>::new(),
        "the route said every caller may act in this org, so nobody's membership is asked — \
         not even a principal's"
    );
    assert_eq!(bodies_entered(), 1);
}

/// Oracle 2 (f). `public(Org)` admits `Org` scopes only. The route below opens
/// `Team`, so an anonymous caller meets rule 3, and the 401 names the stance
/// the route really declared.
#[test]
fn public_scope_for_one_scope_type_does_not_admit_another() {
    let (_sim, mut app) = memo_app(0x7E4A_1706);
    reset_counters();
    let refused = app.handle(Request::new(Method::Post, "/memo-team/5"));
    assert_eq!(refused.status, 401, "{}", body_text(&refused));
    assert_eq!(
        error_text(&refused),
        format!(
            "authentication required — `POST /memo-team/:team` is declared `public(Org)` and \
             opened the tenant scope `Team` for a caller with no credential; only a route \
             declared `public(Team)` admits one. Send \"Authorization: Bearer <token>\" or the \
             charpente_session cookie, or, if every caller may act inside this tenant here, \
             declare `public(Team)` in place of `public(Org)` in this route's `#[route(…)]` \
             attribute, keeping its other flags. A tenant type is named here by its NAME; \
             name the type in the attribute: `Team` is the tenant type `{}`, and `Org` is `{}`",
            std::any::type_name::<Team>(),
            std::any::type_name::<Org>()
        )
    );
    assert_eq!(bodies_entered(), 0);
}

/// Item 12. On a `public, cross_tenant` route the 401 names the stance to
/// change and keeps the route's other flags, so the `cross_tenant` its `cap`
/// argument needs survives the author's fix.
#[test]
fn the_401_on_a_cross_tenant_route_keeps_the_flag_in_its_fix() {
    let (_sim, mut app) = memo_app(0x7C06_0601);
    reset_counters();
    let refused = app.handle(Request::new(
        Method::Post,
        format!("/memo-cross/{ADMITTED}"),
    ));
    assert_eq!(refused.status, 401, "{}", body_text(&refused));
    let text = error_text(&refused);
    assert_eq!(
        text,
        format!(
            "authentication required — `POST /memo-cross/:org` is declared `public` and \
             opened the tenant scope `Org` for a caller with no credential; only a route \
             declared `public(Org)` admits one. Send \"Authorization: Bearer <token>\" or the \
             charpente_session cookie, or, if every caller may act inside this tenant here, \
             declare `public(Org)` in place of `public` in this route's `#[route(…)]` \
             attribute, keeping its other flags. A tenant type is named here by its NAME; \
             name the type in the attribute: `Org` is the tenant type `{}`",
            std::any::type_name::<Org>()
        )
    );
    assert!(
        !text.contains("#[route(POST"),
        "the fix restates no attribute, so `cross_tenant` cannot drop out of it: {text}"
    );
    assert_eq!(authorize_calls(), Vec::<String>::new());
    assert_eq!(bodies_entered(), 0, "nothing below the scope line ran");
    assert_eq!(app.db().last_applied(), None, "and nothing was written");
}

/// Spec §4: the `emits(...)` drift probe meets the three rules exactly as a
/// request does. A `public(Org)` stream is probeable, because the probe mints
/// the admission a request would. The probe presents no credential and does
/// not enforce `auth`, so on a plain `public` or an `auth` stream the scope
/// line is rule 3, and the probe's refusal quotes the route's real stance
/// (amendment A11).
#[test]
fn the_drift_probe_meets_the_three_rules_as_a_request_does() {
    let (_sim, mut app) = memo_app(0x7E4A_1708);
    assert_eq!(
        app.subscription_event_names("memo_feed_open"),
        Ok(vec!["memo_noted"])
    );
    let public = app.subscription_event_names("memo_feed").unwrap_err();
    assert!(
        public.contains(
            "`SSE /memo-feed/:org` is declared `public` and opened the tenant scope `Org`"
        ),
        "{public}"
    );
    // Byte-exact: an `auth` route already requires a credential, so its
    // refusal must not suggest opening it to every caller with `public(Org)`.
    // The handler's error reads as a request would receive it, its status and
    // its message, with no Rust `Debug` syntax around it.
    assert_eq!(
        app.subscription_event_names("memo_feed_auth"),
        Err(format!(
            "route `memo_feed_auth`'s subscribe handler answered 401 under the emits(...) drift \
             probe, so what it subscribes to cannot be read — a handler that rejects a \
             credential-free request with `0` for every :param is not probeable; the 401 reads: \
             authentication required — `SSE /memo-feed-auth/:org` is declared `auth` and opened \
             the tenant scope `Org` (the tenant type `{}`) for a caller with no credential; the \
             route requires a credential, so a request without one is refused before its \
             handler runs, and a caller that presents none, as the emits(...) drift probe does, \
             cannot run it — leave this route out of the drift test",
            std::any::type_name::<Org>()
        ))
    );
}

// ------------------------------------------------------- the tenant swarm

/// The swarm property's own scope type, deliberately NOT `Org`.
///
/// `Org::authorize` admits exactly one tenant (`ADMITTED`) and must keep
/// refusing `REFUSED`, so it cannot host the N >= 2 writable tenants a leak
/// is only expressible with.
///
/// `Firm` admits every tenant: this property is about the row BOUND, not
/// about the membership check, which the unit test above owns.
#[table(global)]
struct Firm {
    id: Id<Firm>,
    name: String,
}

impl Tenant for Firm {
    const NAME: &'static str = "Firm";
    type Who = Principal;
    type Error = ApiError;

    /// Never called: the swarm is operator code, and opens its tenants with
    /// an admission (spec §7).
    fn authorize(_tx: &Tx, _who: &Principal, _scope: Id<Firm>) -> Result<(), ApiError> {
        Ok(())
    }
}

/// Modeled on `replication.rs`'s `SeedLedger` (its own doc comment carries
/// the argument): a SHARED indexed field — every tenant writes the same
/// `bucket` values, so a `scan_by` that forgot its scope prefix has
/// somewhere to leak from — plus a `marker` DERIVED FROM THE TENANT, so a
/// leaked or collapsed row is detectable by its CONTENT and not only by the
/// key it was found under.
///
/// Without the shared index key the property certifies less than it appears
/// to; without the marker a two-tenant collapse onto one id reads as a
/// perfectly ordinary row.
#[table(scoped_by = Firm)]
struct Ledger {
    id: Id<Ledger>,
    #[index]
    bucket: u32,
    marker: u64,
}

/// The tenants. Two, sharing an id space on purpose: a single-tenant
/// universe cannot detect a leak, because every row it can see legitimately
/// belongs to the only tenant there is.
const TENANTS: [u64; 2] = [1, 2];

/// Ids BOTH tenants write. A collapse of two tenants onto one row is only
/// expressible when they hold the same id.
const SHARED_IDS: u64 = 5;

/// Index key values, shared across tenants by construction (`bucket_of`).
const BUCKETS: u32 = 3;

/// Base for the id each tenant holds ALONE — the `get` half of the property:
/// a foreign `(scope, id)` must come back ABSENT, which needs an id that
/// exists in one tenant and not the other. Also the top of each tenant's id
/// range, which is what `scan_from` past the last row is measured against.
const PRIVATE_ID_BASE: u64 = 100;

/// Markers are `tenant * STRIDE + id`, so the tenant that wrote a row is
/// recoverable from the row alone. `SHARED_IDS` and `PRIVATE_ID_BASE` are
/// both far below the stride, so `marker / STRIDE` is exactly the tenant.
const MARKER_STRIDE: u64 = 1_000_000;

fn marker(tenant: u64, id: u64) -> u64 {
    tenant * MARKER_STRIDE + id
}

fn marker_tenant(m: u64) -> u64 {
    m / MARKER_STRIDE
}

fn private_id(tenant: u64) -> u64 {
    PRIVATE_ID_BASE + tenant
}

fn bucket_of(id: u64) -> u32 {
    (id % BUCKETS as u64) as u32
}

/// Every id one tenant writes: the shared ones, then its own.
fn own_ids(tenant: u64) -> Vec<u64> {
    (0..SHARED_IDS).chain([private_id(tenant)]).collect()
}

fn ledger_row(tenant: u64, id: u64) -> Ledger {
    Ledger {
        id: Id::from_raw(id),
        bucket: bucket_of(id),
        marker: marker(tenant, id),
    }
}

fn ledger_schema() -> Schema {
    Schema::new().table::<Ledger>()
}

fn ledger_cfg() -> DbConfig {
    DbConfig {
        wal: WalConfig {
            dir: "wal".to_string(),
            segment_bytes: 4096,
        },
        store_path: "tables.redb".to_string(),
        ..DbConfig::default()
    }
}

/// This binary's fault classifier, the narrow form `transactions.rs` uses and
/// for the same reason: this workload breaks and restarts the universe on the
/// first Io error, so it never sees a poisoned or redb-latched handle, and
/// accepting those wordings here could only ever LOOSEN the oracle. Nor the
/// durable-horizon refusal: only a fired durable-corruption fault excuses it,
/// so every open site asks `checks::durable_refusal` first (S3b item 0;
/// `boot_through_fallout` does it for the round boots).
fn is_injected(text: &str) -> bool {
    matches!(
        charpente_sim::checks::classify(text),
        charpente_sim::checks::FaultReason::Injected
    )
}

// ------------------------------------------------------------ the oracle

/// THE ORACLE. Every row a tenant's read returned must carry that tenant's
/// own marker.
///
/// Shared, deliberately: the property's four read paths and both naive
/// victims below run this exact function. A victim caught by an oracle of its
/// own would prove nothing about the one the property depends on.
///
/// Keys on CONTENT, not on the key the row was found under, so it catches
/// both shapes a broken bound produces — another tenant's row surfacing in my
/// range, and two tenants collapsed onto one id where only the marker says
/// which write survived.
fn only_own_rows(tenant: u64, path: &str, rows: &[Ledger]) -> Result<(), String> {
    for row in rows {
        if marker_tenant(row.marker) != tenant {
            return Err(format!(
                "tenant leak: {path} for tenant {tenant} returned Ledger id {} carrying tenant \
                 {}'s marker {} (bucket {}) — a scoped read must return ONLY its own tenant's \
                 rows",
                row.id.raw(),
                marker_tenant(row.marker),
                row.marker,
                row.bucket
            ));
        }
    }
    Ok(())
}

/// All four scoped read paths for one tenant, each checked against the shared
/// oracle. Returns how many rows the tenant's `scan` saw, which is what the
/// multi-tenant coverage key is decided on.
///
/// Io errors come back as their own text so the caller can classify them
/// (`is_injected`) — a leak message never looks like an injected fault, and
/// an injected read error never reaches the oracle.
fn bounded_reads(stx: &ScopedTx<'_, '_, Firm>, tenant: u64) -> Result<usize, String> {
    // scan — the range bound.
    let all = stx.scan::<Ledger>().map_err(|e| e.to_string())?;
    only_own_rows(tenant, "scan", &all)?;

    // get — a foreign (scope, id) must be ABSENT, not merely filtered out of
    // a wider result. This is the one read path where "filtered" and "absent"
    // are distinguishable at all, so it is where the distinction is pinned.
    for other in TENANTS {
        if other == tenant {
            continue;
        }
        let foreign = Id::<Ledger>::from_raw(private_id(other));
        if let Some(row) = stx.get::<Ledger>(foreign).map_err(|e| e.to_string())? {
            return Err(format!(
                "tenant leak: get({}) for tenant {tenant} returned tenant {}'s private row \
                 (marker {}) — a foreign (scope, id) must be ABSENT, not filtered",
                foreign.raw(),
                marker_tenant(row.marker),
                row.marker
            ));
        }
    }
    // get — and my own ids resolve to MY rows, not to a collapsed overlay's.
    for id in own_ids(tenant) {
        if let Some(row) = stx
            .get::<Ledger>(Id::from_raw(id))
            .map_err(|e| e.to_string())?
        {
            only_own_rows(tenant, "get", std::slice::from_ref(&row))?;
        }
    }

    // scan_from — the paginated bound, from the bottom of the id space...
    let page = stx.scan_from::<Ledger>(0, 64).map_err(|e| e.to_string())?;
    only_own_rows(tenant, "scan_from", &page)?;
    // ...and from PAST this tenant's last row, which is the case that catches
    // a missing upper bound: rows sort by (scope, id), so the next row after
    // this tenant's last one belongs to the next tenant.
    let past_end = stx
        .scan_from::<Ledger>(private_id(tenant) + 1, 64)
        .map_err(|e| e.to_string())?;
    only_own_rows(tenant, "scan_from past the last row", &past_end)?;
    if !past_end.is_empty() {
        return Err(format!(
            "tenant leak: scan_from({}) for tenant {tenant} starts past this tenant's last row \
             ({}) and returned {} row(s) — the upper bound must stop at this tenant's slice",
            private_id(tenant) + 1,
            private_id(tenant),
            past_end.len()
        ));
    }

    // scan_by — the index prefix, with BOTH tenants indexing the SAME key
    // value. Through the macro-generated accessor, which is the surface an
    // app actually calls.
    for bucket in 0..BUCKETS {
        let hits = Ledger::by_bucket(stx, &bucket).map_err(|e| e.to_string())?;
        only_own_rows(tenant, "scan_by", &hits)?;
        for row in &hits {
            if row.bucket != bucket {
                return Err(format!(
                    "tenant leak: scan_by(\"bucket\", {bucket}) for tenant {tenant} returned \
                     Ledger id {} whose bucket is {} — the index range matched the wrong key",
                    row.id.raw(),
                    row.bucket
                ));
            }
        }
    }

    Ok(all.len())
}

/// Runs the oracle for every tenant against a live `Db`. Called after EVERY
/// recovery, which is the property's contract: whatever the fault matrix did
/// to the durable state, what a tenant can read back is still only its own.
///
/// Credits `tenant:multi_tenant_write` exactly when every tenant has at least
/// one visible row — the condition under which the oracle is non-vacuous. A
/// batch where that never happened is a batch that could not have detected a
/// leak, and `require:` turns that into a red property rather than a quiet
/// one.
fn oracle(sim: &Sim, db: &mut Db) -> Result<(), String> {
    let mut visible = [0usize; TENANTS.len()];
    for (i, tenant) in TENANTS.iter().copied().enumerate() {
        let verdict = transact_admitted(db, |tx, admission| -> Result<usize, String> {
            bounded_reads(
                &tx.admit_scope(Id::<Firm>::from_raw(tenant), admission),
                tenant,
            )
        });
        match verdict {
            Ok(n) => visible[i] = n,
            Err(TxError::App(msg)) => return Err(msg),
            Err(TxError::Io(e)) => return Err(e.to_string()),
            Err(TxError::NotPrimary { .. }) | Err(TxError::Verifying { .. }) => {
                unreachable!("this workload never gates writes")
            }
        }
    }
    if visible.iter().all(|n| *n > 0) {
        sim.cover("tenant:multi_tenant_write");
    }
    Ok(())
}

// ---------------------------------------------------------- the universe

/// How many reopens the final recovery gets before the universe gives up.
/// Every attempt either runs the oracle or dies to injected fault fallout, so
/// a larger budget costs time only in universes that would otherwise have
/// concluded nothing. With the rounds aimed, 16 -> 64 took give-ups over ten
/// fresh bases from 234 to 65 for +2.8% CPU (2026-10-01, S5 review I1). Before
/// S5 a failed reopen rebuilt the table store, and 32 cost +34%.
const FINAL_ATTEMPTS: u32 = 64;

/// How many boots each round gets before it restarts and moves on
/// (`checks::boot_through_fallout`). A round whose boot fails skips its oracle
/// and its writes, so this budget is reach: over ten fresh bases at 256
/// universes, booting once left 3,381 rounds unbooted, and 16, 32 and 64 left
/// 444, 167 and 43, for +10.0%, +13.2% and +15.1% CPU (2026-10-01, S5 review
/// I1). More rounds mean more to recover at the end: give-ups went 27 -> 56,
/// 60 and 65, against 137 for the same bases before S5.
const ROUND_BOOT_TRIES: u32 = 64;

/// One universe: recover -> oracle -> a round of scoped writes, some of them
/// staged as ONE batch carrying two tenants -> crash. Three rounds, then a
/// final recovery that runs the oracle once more.
fn tenant_universe(sim: &mut Sim) -> Result<(), String> {
    let entropy = sim.entropy();
    let cfg = ledger_cfg();

    for _round in 0..3u64 {
        // Sized to redb 4.3.0's disk-op volume per round: DESIGN §1, "countdowns".
        sim.crash_disk_after(43 + entropy.next_u64() % 340);
        let Some(mut db) =
            charpente_sim::checks::boot_through_fallout(sim, ROUND_BOOT_TRIES, is_injected, || {
                Db::open(Box::new(sim.disk()), cfg.clone(), ledger_schema())
            })
            .map_err(|e| format!("recovery failed with a non-injected error: {e}"))?
        else {
            // A durable refusal already gave the universe up, by name.
            if sim.gave_up().is_some() {
                return Ok(());
            }
            sim.restart();
            continue;
        };

        // THE ORACLE, at every recovery.
        if let Err(msg) = oracle(sim, &mut db) {
            if !is_injected(&msg) {
                return Err(msg);
            }
            sim.restart();
            continue;
        }

        let mut died = false;
        for step in 0..1 + entropy.next_u64() % 6 {
            if entropy.next_u64().is_multiple_of(3) {
                // The two-tenant BATCH (ruling R58). `Db::transact` is a group
                // of one, and with N = 1 there is no *between* — so the batch
                // is built here: one group, two transactions, two DIFFERENT
                // tenants staging the SAME id. That is the overlay-aliasing
                // shape, and the fault matrix gets to land inside the
                // append -> sync -> apply window that holds both.
                //
                // One scope per transaction (spec §8) is preserved: each
                // `stage` runs against its own `Tx`.
                let was_poisoned = db.poisoned();
                let id = entropy.next_u64() % SHARED_IDS;
                let mut g = match db.begin_group() {
                    Ok(g) => g,
                    Err(_) => {
                        died = true;
                        break;
                    }
                };
                let admission = g.tenant_admission();
                let mut stage_failed = false;
                for tenant in TENANTS {
                    match g.stage(|tx| -> Result<(), String> {
                        tx.admit_scope(Id::<Firm>::from_raw(tenant), &admission)
                            .put(&ledger_row(tenant, id));
                        Ok(())
                    }) {
                        Ok(()) => {}
                        Err(TxError::App(msg)) => return Err(msg),
                        // A staged read hit an injected fault; `Group::stage`
                        // never touches its record queue before `f` returns
                        // `Ok`, so nothing already staged is lost.
                        Err(TxError::Io(_)) => {
                            stage_failed = true;
                            break;
                        }
                        Err(TxError::NotPrimary { .. }) | Err(TxError::Verifying { .. }) => {
                            unreachable!("this workload never gates writes")
                        }
                    }
                }
                let staged = g.len();
                // `commit` runs on every path — `Group`'s Drop panics on a
                // non-empty group, and `commit` is what empties the queue.
                if let Err(failure) = g.commit() {
                    // Ruling R58, the whole reason this key is written this
                    // way. `applied == 0` is the fault landing INSIDE the
                    // batch's append -> sync -> apply window, and that window
                    // has exactly two reachable states: between two appends
                    // pre-sync (nothing acked), or after the sync and before
                    // the apply (all N durable, the store N behind). Both
                    // leave a two-tenant batch half-resolved, which is the
                    // state the oracle then reads at the next recovery.
                    //
                    // `applied == staged` — the checkpoint arm — is NOT
                    // credited: those records are durable and applied, and the
                    // failure is bookkeeping after the fact, outside the
                    // window. A partial apply INSIDE `apply_batch` would be a
                    // third state, and it is unreachable by construction (the
                    // run's row effects commit atomically with `applied_seq`
                    // in one redb write transaction), so no key here claims
                    // it: a `require:` key that cannot fire certifies nothing.
                    if staged >= 2 && failure.applied == 0 && !was_poisoned {
                        sim.cover("tenant:crash_mid_batch");
                    }
                    died = true;
                    break;
                }
                if stage_failed {
                    died = true;
                    break;
                }
            } else {
                // The ordinary path: one tenant, one transaction.
                let tenant = TENANTS[(step as usize) % TENANTS.len()];
                let ids = own_ids(tenant);
                let id = ids[(entropy.next_u64() % ids.len() as u64) as usize];
                match transact_admitted(&mut db, |tx, admission| -> Result<(), String> {
                    tx.admit_scope(Id::<Firm>::from_raw(tenant), admission)
                        .put(&ledger_row(tenant, id));
                    Ok(())
                }) {
                    Ok(()) => {}
                    Err(TxError::App(msg)) => return Err(msg),
                    Err(TxError::Io(_)) => {
                        died = true;
                        break;
                    }
                    Err(TxError::NotPrimary { .. }) | Err(TxError::Verifying { .. }) => {
                        unreachable!("this workload never gates writes")
                    }
                }
            }
            if entropy.next_u64().is_multiple_of(4)
                && let Err(e) = db.checkpoint()
            {
                if !is_injected(&e.to_string()) {
                    return Err(format!("non-injected checkpoint failure: {e}"));
                }
                died = true;
                break;
            }
        }

        // The oracle again on the live handle, so a leak introduced by THIS
        // round's writes is caught in the round that made it and not only
        // after a restart has re-derived the store from the log.
        if !died
            && let Err(msg) = oracle(sim, &mut db)
            && !is_injected(&msg)
        {
            return Err(msg);
        }

        sim.restart();
    }

    // Final recovery: the last round's writes get the oracle too. Under
    // persistent injected faults an open may legitimately never succeed, so
    // an injected failure ends the universe rather than failing it — as a
    // give-up, since the last round's writes were never checked.
    for _attempt in 0..FINAL_ATTEMPTS {
        let mut db = match Db::open(Box::new(sim.disk()), cfg.clone(), ledger_schema()) {
            Ok(db) => db,
            Err(e) => {
                let text = e.to_string();
                // No restart clears a durable refusal (S5 re-review R-M2), and
                // only a fired durable-corruption fault excuses one (S3b item 0).
                if charpente_sim::checks::durable_refusal(sim, &text)? {
                    return Ok(());
                }
                if !is_injected(&text) {
                    return Err(format!(
                        "final recovery failed with a non-injected error: {e}"
                    ));
                }
                sim.restart();
                continue;
            }
        };
        return match oracle(sim, &mut db) {
            Ok(()) => Ok(()),
            Err(msg) if is_injected(&msg) => {
                sim.restart();
                continue;
            }
            Err(msg) => Err(msg),
        };
    }
    sim.give_up(
        "the final recovery never ran the oracle: every attempt hit injected faults, so the \
         last round's writes were never checked",
        "",
    );
    Ok(())
}

/// The give-up cap, exact at the pinned base: 4 of 256 universes gave up in
/// two identical batches with the round boots aimed and 64 final attempts
/// (2026-10-01, S5 review I1). It was 16 at S5's parent and 17 at S5 before
/// the aiming; over ten fresh bases the mean went 13.1 -> 7.1.
const TENANT_GAVE_UP_CEILING: u64 = 4;

/// The reach floor, exact at the pinned base: 1,091 checked recoveries saw two
/// tenants' writes in two identical batches (2026-10-01, S5 review I1). A cap
/// bounds what the property fails to conclude; this bounds what it checks,
/// which S5 cut by 11% before the round boots were aimed with no cap
/// noticing. Over ten fresh bases the mean is 1,071.6 against 707.2 at S5's
/// parent.
const TENANT_MULTI_TENANT_WRITE_FLOOR: u64 = 1091;

/// The flagship tenant property (spec §1 as a mechanical oracle, hard rule
/// 3): across the full fault matrix, and after every recovery, every scoped
/// read returns ONLY its own tenant's rows — `get`, `scan`, `scan_from` and
/// `scan_by`, with two tenants sharing an id space and an index key.
///
/// Tasks 1-7 landed the scoping machinery with compile-fail cases and unit
/// tests; this is the fault-injection half they shipped without. The two
/// naive victims below are what make its greenness mean something.
///
/// Fixed base, 256 universes — the shape every permanent swarm property in
/// this repo uses: a property that must not flake batch to batch does not
/// take an ambient seed. Fresh bases are run and recorded rather than
/// swapped in here.
/// A durable refusal at the final recovery ends it at once, by name (S5 re-review
/// R-M2). Seed `0x15725b7dd3ceaac5`, at the batch's base, meets one there. No restart
/// clears it, and the loop used to spend its whole budget on it, then give
/// up as though every attempt had hit an injected fault ("the final recovery never ran the oracle").
#[test]
fn a_durable_refusal_at_the_final_recovery_ends_it_at_once_by_name() {
    const SEED: u64 = 0x1572_5b7d_d3ce_aac5;
    let mut state = SEED;
    let cfg_seed = charpente_sim::rng::splitmix64(&mut state);
    let mut cfg = FaultConfig::swarm(&mut charpente_sim::rng::Rng::new(cfg_seed));
    charpente_sim::sample_extended_fields(SEED, &mut cfg);
    let mut sim = Sim::with_faults(SEED, cfg);

    if let Err(e) = tenant_universe(&mut sim) {
        panic!("seed {SEED:#x}: {e}");
    }
    assert_eq!(
        sim.coverage().get("boot:durable-refusal"),
        Some(&1),
        "seed {SEED:#x}: {:?}",
        sim.coverage()
    );
    let reason = sim
        .gave_up()
        .unwrap_or_else(|| panic!("seed {SEED:#x}: a durable refusal must give up, not pass"));
    assert!(
        reason.starts_with(
            "the boot was refused for durable corruption at or below the store's horizon"
        ),
        "seed {SEED:#x} must give up for the refusal, at once: {reason}"
    );
}

#[test]
fn every_scoped_read_returns_only_its_own_tenants_rows() {
    run_universes_swarm_with!(
        0x7E4A_0001,
        256,
        |_seed: u64, _cfg: &mut FaultConfig| {},
        tenant_universe,
        require: [
            "tenant:multi_tenant_write",
            "tenant:crash_mid_batch",
            "boot:booted-after-retry"
        ],
        floor: [("tenant:multi_tenant_write", TENANT_MULTI_TENANT_WRITE_FLOOR)],
        ceiling: [("run:skipped", TENANT_GAVE_UP_CEILING)]
    );
}

/// A universe whose final recovery never ran the oracle adjudicated nothing
/// about its last round, so it gives up rather than passing (LEDGER
/// 2026-09-30, follow-up 9). Every disk op faults here, so every attempt dies
/// to injected faults.
#[test]
fn a_final_recovery_that_never_runs_the_oracle_gives_up_rather_than_passing() {
    // Control: the same seed with no faults reaches a verdict.
    let mut control = Sim::with_faults(0x7E4A_0901, FaultConfig::none());
    assert_eq!(tenant_universe(&mut control), Ok(()));
    assert_eq!(
        control.gave_up(),
        None,
        "a fault-free universe reaches a verdict"
    );

    let mut sim = Sim::with_faults(
        0x7E4A_0901,
        FaultConfig {
            read_error: 1.0,
            write_error: 1.0,
            sync_error: 1.0,
            ..FaultConfig::none()
        },
    );
    assert_eq!(tenant_universe(&mut sim), Ok(()));
    let reason = sim
        .gave_up()
        .expect("a universe whose final recovery never ran the oracle must give up, not pass");
    assert!(
        reason.contains("the final recovery never ran the oracle"),
        "and it must be THIS give-up: {reason}"
    );
}

// ----------------------------------------------------- the naive victims

/// A fault-free store holding both tenants' rows — what the victims below
/// read, and the control the correct path is checked against on the same
/// bytes.
fn two_tenant_db(seed: u64) -> (Sim, Db) {
    let sim = Sim::with_faults(seed, FaultConfig::none());
    let mut db = Db::open(Box::new(sim.disk()), ledger_cfg(), ledger_schema()).unwrap();
    for tenant in TENANTS {
        for id in own_ids(tenant) {
            transact_admitted(&mut db, |tx, admission| -> Result<(), String> {
                tx.admit_scope(Id::<Firm>::from_raw(tenant), admission)
                    .put(&ledger_row(tenant, id));
                Ok(())
            })
            .unwrap();
        }
    }
    (sim, db)
}

/// `Db::transact`, with the operator's admission (job-tenancy spec §7): the
/// same begin_group -> stage -> commit sequence and the same commit rule,
/// line for line. So the swarm writes the bytes it wrote when these sites
/// said `open_scope(…, None)`, and Task 1's coverage baseline measures that.
fn transact_admitted<T, E>(
    db: &mut Db,
    f: impl FnOnce(&mut Tx, &TenantAdmission) -> Result<T, E>,
) -> Result<T, TxError<E>> {
    let mut g = db.begin_group().map_err(TxError::Io)?;
    let admission = g.tenant_admission();
    let out = g.stage(|tx| f(tx, &admission))?;
    let staged = !g.is_empty();
    if let Err(failure) = g.commit() {
        let reported_elsewhere = db.poisoned() || db.store_failed();
        if failure.invalidates(0, staged) || !reported_elsewhere {
            return Err(TxError::Io(failure.error));
        }
    }
    Ok(out)
}

/// Naive victim (a): a `scan` whose range bound was dropped.
///
/// The scoped read bounds the composite-keyed table to
/// `scoped_key(scope, 0)..=scoped_key(scope, u64::MAX)`, which is exactly one
/// tenant's contiguous slice. Without that bound the walk is the whole table.
/// There is no feature flag and no way to write this bug through a route —
/// `ScopedTx::scan` is the only scoped read an app has — so, per this suite's
/// idiom, the victim drives the table primitive directly: `scan_raw` IS the
/// unbounded walk, over the same physical table.
fn naive_scan_without_the_range_bound(snap: &Snapshot) -> Vec<Ledger> {
    snap.scan_raw(Ledger::TABLE, true)
        .unwrap()
        .iter()
        .map(|(_, _, bytes)| Ledger::decode_row(bytes).unwrap())
        .collect()
}

/// Naive victim (b): a `scan_by` whose index range omits the scope prefix.
///
/// A scoped index entry is `[scope u64 be][keylen u32 le][key][id u64 be]`,
/// and the scoped read bounds itself to `scope||key||0 ..= scope||key||MAX`.
/// An implementation that forgot the scope would range on `keylen||key`
/// alone and match EVERY tenant's entries for that key.
///
/// That read is reconstructed here from the raw index bytes and the raw
/// rows, because the platform offers no unbounded scoped-index read to call —
/// which is the point of the bound. Entries are matched on their
/// `keylen||key` SUFFIX, ignoring whatever scope prefixed them, and each
/// match is resolved to the row it names. What this models is an index range
/// that is not bounded to one tenant; it does not claim to model a store
/// whose entries were never prefixed at all.
fn naive_scan_by_without_the_scope_prefix(snap: &Snapshot, bucket: u32) -> Vec<Ledger> {
    let mut key = Vec::new();
    bucket.encode(&mut key);
    // `[keylen u32 le][key]` — the entry an unprefixed encoder would write,
    // and the whole of what an unprefixed range would match on.
    let mut unprefixed = (key.len() as u32).to_le_bytes().to_vec();
    unprefixed.extend_from_slice(&key);

    let rows = snap.scan_raw(Ledger::TABLE, true).unwrap();
    let mut out = Vec::new();
    for entry in snap.scan_index_raw::<Ledger>("bucket").unwrap() {
        let (head, id_bytes) = entry.split_at(entry.len() - 8);
        if !head.ends_with(&unprefixed) {
            continue;
        }
        let id = u64::from_be_bytes(id_bytes.try_into().unwrap());
        let scope = u64::from_be_bytes(head[..8].try_into().unwrap());
        let (_, _, bytes) = rows
            .iter()
            .find(|(s, i, _)| (*s, *i) == (scope, id))
            .expect("an index entry names a row");
        out.push(Ledger::decode_row(bytes).unwrap());
    }
    out
}

/// The victims' control: the CORRECT reads pass the shared oracle on the very
/// same store the victims leak from. Without it, a red victim would also be
/// explained by an oracle that reds on anything.
#[test]
fn the_correct_reads_pass_the_oracle_on_a_two_tenant_store() {
    let (sim, mut db) = two_tenant_db(0x7E4A_600D);
    oracle(&sim, &mut db).expect("the scoped read paths must pass their own oracle");
    for tenant in TENANTS {
        assert_eq!(
            db.snapshot()
                .unwrap()
                .scan_scoped::<Ledger>(tenant)
                .unwrap()
                .len(),
            own_ids(tenant).len(),
            "anti-vacuity: tenant {tenant} must actually have rows for the oracle to check"
        );
    }
}

/// Hard rule 3, victim (a): a `scan` that dropped its range bound turns the
/// property's own oracle RED, and the message names the tenant that leaked.
#[test]
fn the_oracle_catches_a_scan_that_dropped_its_range_bound() {
    let (_sim, db) = two_tenant_db(0x7E4A_DEAD);
    let snap = db.snapshot().unwrap();

    let leaked = naive_scan_without_the_range_bound(&snap);
    assert_eq!(
        leaked.len(),
        TENANTS.iter().map(|t| own_ids(*t).len()).sum::<usize>(),
        "the unbounded walk must actually have seen every tenant's rows"
    );

    let err = only_own_rows(TENANTS[0], "scan", &leaked)
        .expect_err("the oracle must catch a scan that walked every tenant's rows");
    assert!(
        err.contains("tenant leak")
            && err.contains(&format!("tenant {}'s marker", TENANTS[1]))
            && err.contains(&format!("for tenant {}", TENANTS[0])),
        "the oracle must name the leak, the tenant that read and the tenant that wrote: {err}"
    );
}

/// Hard rule 3, victim (b): a `scan_by` whose index range omitted the scope
/// prefix turns the same oracle RED. This is the case the shared index key
/// exists for — both tenants index `bucket`, so the unprefixed range matches
/// entries from both, and only the marker says which tenant wrote the row.
#[test]
fn the_oracle_catches_a_scan_by_that_omitted_the_index_scope_prefix() {
    let (sim, mut db) = two_tenant_db(0x7E4A_BEEF);
    // The correct `scan_by` on this store is clean, so the red below is the
    // dropped prefix and not the fixture.
    oracle(&sim, &mut db).expect("the scoped read paths must pass their own oracle");

    let snap = db.snapshot().unwrap();
    let bucket = bucket_of(0);
    let leaked = naive_scan_by_without_the_scope_prefix(&snap, bucket);
    let own: Vec<&Ledger> = leaked
        .iter()
        .filter(|r| marker_tenant(r.marker) == TENANTS[0])
        .collect();
    assert!(
        leaked.len() > own.len(),
        "the unprefixed range must have matched another tenant's entries: {} of {} rows were \
         tenant {}'s own",
        own.len(),
        leaked.len(),
        TENANTS[0]
    );

    let err = only_own_rows(TENANTS[0], "scan_by", &leaked)
        .expect_err("the oracle must catch a scan_by that matched every tenant's index entries");
    assert!(
        err.contains("tenant leak")
            && err.contains(&format!("tenant {}'s marker", TENANTS[1]))
            && err.contains(&format!("bucket {bucket}")),
        "the oracle must name the leak, the tenant that wrote, and the shared index key: {err}"
    );
}

/// Determinism (spec §7): the tenant workload replays to the same durable
/// state from the same seed, so the swarm's seeds are replayable repro lines
/// and not merely labels.
#[test]
fn the_tenant_workload_replays_identically_for_the_same_seed() {
    let trace = |seed: u64| {
        let mut sim = Sim::new(seed);
        let result = tenant_universe(&mut sim);
        assert_eq!(result, Ok(()), "seed {seed:#x}");
        (sim.durable_fingerprint(), sim.now())
    };
    for seed in [0x7E4A_5EED, 0x0BAD_5EED_0BAD_5EED] {
        assert_eq!(trace(seed), trace(seed));
    }
}

// ----------------------------------------------- the declared global reader

/// A second scope type, whose `authorize` admits every tenant — the cross-
/// tenant reader below needs rows in MORE than one tenant, and `Org`'s
/// `authorize` (deliberately) refuses every tenant but one, so it cannot
/// write the second tenant's rows to read back.
#[table(global)]
struct Team {
    id: Id<Team>,
    name: String,
}

impl Tenant for Team {
    const NAME: &'static str = "Team";
    type Who = Principal;
    type Error = ApiError;
    fn authorize(_tx: &Tx, _who: &Principal, _scope: Id<Team>) -> Result<(), ApiError> {
        Ok(())
    }
}

#[table(scoped_by = Team)]
struct Note {
    id: Id<Note>,
    text: String,
}

/// The wire shape of one cross-tenant row: the tenant travels WITH the row,
/// which is the property spec §3 puts in the type (`Vec<(Id<Team>, Note)>`,
/// never `Vec<Note>`). Pinning it on the wire too means a handler that
/// dropped the scope on the way out is a failed assertion, not a silently
/// tenant-less list.
#[derive(charpente::Wire, serde::Serialize)]
struct NoteRow {
    team: Id<Team>,
    note: Id<Note>,
}

#[route(PUT "/note/:team/:id", public(Team))]
fn write_note(tx: &mut Tx, team: Id<Team>, id: Id<Note>, ctx: &Ctx) -> Result<WireU64, ApiError> {
    let mut tx = tx.scope::<Team>(team, ctx)?;
    tx.put(&Note {
        id,
        text: format!("note {} of team {}", id.raw(), team.raw()),
    });
    Ok(WireU64(id.raw()))
}

/// One tenant's view, for the contrast the assertion needs: the same table,
/// read through a scope, must still see only its own tenant.
#[route(GET "/note/:team", public(Team))]
fn count_notes(tx: &mut Tx, team: Id<Team>, ctx: &Ctx) -> Result<WireU64, ApiError> {
    let tx = tx.scope::<Team>(team, ctx)?;
    Ok(WireU64(tx.scan::<Note>()?.len() as u64))
}

/// The declared capability (spec §3). `cap` is the token the kernel mints
/// only because this route said `cross_tenant`; without the flag the macro
/// refuses the argument, and without the argument there is no `&CrossTenant`
/// to hand `across_tenants` (`compile_fail/across_tenants_without_capability`).
#[route(GET "/admin/notes", public, cross_tenant)]
fn all_notes(tx: &mut Tx, cap: &CrossTenant) -> Result<Vec<NoteRow>, ApiError> {
    Ok(tx
        .across_tenants::<Team>(cap)
        .scan::<Note>()?
        .into_iter()
        .map(|(team, note)| NoteRow {
            team,
            note: note.id,
        })
        .collect())
}

fn note_app(seed: u64) -> (Sim, App) {
    let sim = Sim::with_faults(seed, FaultConfig::none());
    let app = App::open(
        Box::new(sim.disk()),
        Schema::new().table::<Note>(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )
    .unwrap()
    .mount(routes![write_note, count_notes, all_notes]);
    (sim, app)
}

fn put(app: &mut App, path: &str) -> Response {
    app.handle(Request::new(Method::Put, path.to_string()))
}

fn get(app: &mut App, path: &str) -> Response {
    app.handle(Request::new(Method::Get, path.to_string()))
}

/// The runtime half of spec §3: a declared route enumerates every tenant,
/// and each row comes back carrying the tenant it belongs to.
///
/// Two tenants share an id (`id = 1` exists in both team 1 and team 2), which
/// is what makes the pairing load-bearing: a reader that merged by bare id
/// would return three rows, not four, and could not say which tenant the
/// survivor came from.
#[test]
fn a_declared_route_reads_every_tenant_with_the_scope_on_every_row() {
    let (_sim, mut app) = note_app(0x7E4A_0001);
    assert_eq!(put(&mut app, "/note/1/1").status, 200);
    assert_eq!(put(&mut app, "/note/1/2").status, 200);
    assert_eq!(put(&mut app, "/note/2/1").status, 200);
    assert_eq!(put(&mut app, "/note/2/9").status, 200);

    // The floor: each tenant's own scoped read still sees only its own rows,
    // so what follows is a wider read, not a broken scope.
    assert_eq!(body_text(&get(&mut app, "/note/1")), "\"2\"");
    assert_eq!(body_text(&get(&mut app, "/note/2")), "\"2\"");

    let all = get(&mut app, "/admin/notes");
    assert_eq!(all.status, 200);
    assert_eq!(
        body_text(&all),
        r#"[{"team":"1","note":"1"},{"team":"1","note":"2"},{"team":"2","note":"1"},{"team":"2","note":"9"}]"#,
        "every tenant's rows, in (scope, id) order, each paired with its tenant"
    );
}

/// The transaction's own staged writes are visible to a cross-tenant read,
/// under the same txn -> group -> snapshot order every other read uses — and
/// a tombstone staged for one tenant removes only that tenant's row.
#[test]
fn a_cross_tenant_read_sees_staged_writes_and_tombstones_per_tenant() {
    let (_sim, mut app) = note_app(0x7E4A_0002);
    assert_eq!(put(&mut app, "/note/1/1").status, 200);
    assert_eq!(put(&mut app, "/note/2/1").status, 200);

    let db = app.db_for_tests();
    let mut group = db.begin_group().unwrap();
    // The operator path to both tokens: `Group` is the only constructor, which
    // is exactly why a handler cannot mint one (`CrossTenant`).
    let cap = group.cross_tenant_cap();
    let admission = group.tenant_admission();
    let seen: Vec<(u64, u64)> = group
        .stage(|tx| {
            {
                let mut scoped = tx.admit_scope(Id::<Team>::from_raw(2), &admission);
                scoped.delete::<Note>(Id::from_raw(1));
                scoped.put(&Note {
                    id: Id::from_raw(5),
                    text: "staged".to_string(),
                });
            }
            Ok::<_, ApiError>(
                tx.across_tenants::<Team>(&cap)
                    .scan::<Note>()?
                    .into_iter()
                    .map(|(team, note)| (team.raw(), note.id.raw()))
                    .collect(),
            )
        })
        .unwrap();
    assert_eq!(
        seen,
        vec![(1, 1), (2, 5)],
        "team 2's staged tombstone removed only team 2's id 1, and its staged row is visible"
    );
    group.commit().unwrap();
}

/// Tenant `0` is a REAL tenant, and a staged write for it must survive a
/// cross-tenant read.
///
/// `db.rs`'s `GroupOverlay` doc settles the meaning (R35): "a scoped table's
/// `0` slot is tenant 0, a real tenant, not an unset marker". Nothing guards
/// against reaching it — `Id::from_raw` takes any `u64`, `FromParam for
/// Id<T>` passes straight through it, and this fixture's `Team::authorize`
/// admits every scope — so `PUT /note/0/1` is an ordinary request an app
/// serves. This test is deliberately the previous one with a single literal
/// changed (tenant `2` -> tenant `0`), which is the whole point: if the two
/// disagree, the disagreement is about the number zero and nothing else.
///
/// The negative space it pins is severe rather than cosmetic. The failure
/// mode is not a wrong answer but an `assert_ne!` inside
/// `merge_overlay_all_scopes`, and assertions stay on in release (hard rule
/// 4) while the sole `catch_unwind` in `crates/charpente/src`
/// (`kernel/dispatch.rs`, around a JOB handler's glue) covers neither a route
/// handler nor this read — so the panic unwinds out of the kernel loop and
/// takes the node with it.
/// Worse, `merge_overlay_all_scopes` also reads the GROUP overlay, which
/// accumulates across a batch: one request writing to tenant 0 can kill a
/// different request's cross-tenant read committed alongside it.
#[test]
fn tenant_zero_is_a_real_tenant_to_a_cross_tenant_read() {
    let (_sim, mut app) = note_app(0x7E4A_0003);
    assert_eq!(put(&mut app, "/note/0/1").status, 200);
    assert_eq!(put(&mut app, "/note/1/1").status, 200);

    let db = app.db_for_tests();
    let mut group = db.begin_group().unwrap();
    let cap = group.cross_tenant_cap();
    let admission = group.tenant_admission();
    let seen: Vec<(u64, u64)> = group
        .stage(|tx| {
            {
                let mut scoped = tx.admit_scope(Id::<Team>::from_raw(0), &admission);
                scoped.put(&Note {
                    id: Id::from_raw(7),
                    text: "staged for tenant zero".to_string(),
                });
            }
            Ok::<_, ApiError>(
                tx.across_tenants::<Team>(&cap)
                    .scan::<Note>()?
                    .into_iter()
                    .map(|(team, note)| (team.raw(), note.id.raw()))
                    .collect(),
            )
        })
        .unwrap();
    assert_eq!(
        seen,
        vec![(0, 1), (0, 7), (1, 1)],
        "tenant 0's committed row, its staged row, and tenant 1's row — \
         scope 0 is a tenant like any other"
    );
    group.commit().unwrap();
}

// ------------------------------------------ a table name keeps one Row type

/// `Note` under another scope type: the same identifier and fields, so the
/// same `TABLE`, `VERSION` and `FINGERPRINT` (a `#[table]` fingerprint folds
/// field names and type spellings, not `scoped_by`), and the same scope
/// shape. Only the Rust type tells the two apart.
mod firm_note {
    use super::*;

    #[table(scoped_by = Firm)]
    pub(super) struct Note {
        pub(super) id: Id<Note>,
        pub(super) text: String,
    }
}

/// The refusal a write through `firm_note::Note` gets, spelled out
/// independently of the product's `format!`. The type names come from
/// `type_name`, whose rendering the compiler does not promise to keep.
fn firm_note_refusal() -> String {
    let registered = std::any::type_name::<Note>();
    let writer = std::any::type_name::<firm_note::Note>();
    format!(
        "table \"Note\" is registered as `{registered}`, but this transaction wrote it through \
         `{writer}` — two Row types declare TABLE = \"Note\", and apply treats every write to a \
         table as its registered type's, so this one would have been refused after the WAL sync \
         or applied as a `{registered}` write; the transaction was refused and nothing was \
         written, so a retry cannot succeed — write `{registered}` instead, or give `{writer}` a \
         table name of its own (a #[table] type's table name is its struct name)"
    )
}

/// Over a `Db` holding `Team` 7's note 1, stages `write` (given the group's
/// admission) and commits, then reads back the stage's outcome, whether the
/// log stood still, whether the store is poisoned, and `Team` 7's note 1.
fn through_firm_note(
    seed: u64,
    write: impl FnOnce(&mut Tx, &TenantAdmission),
) -> (Result<(), String>, bool, bool, Option<String>) {
    let sim = Sim::with_faults(seed, FaultConfig::none());
    let mut db = Db::open(
        Box::new(sim.disk()),
        DbConfig::default(),
        Schema::new().table::<Note>(),
    )
    .unwrap();
    let mut group = db.begin_group().unwrap();
    let admission = group.tenant_admission();
    group
        .stage(|tx| {
            tx.admit_scope(Id::<Team>::from_raw(7), &admission)
                .put(&Note {
                    id: Id::from_raw(1),
                    text: "Team 7's note".to_string(),
                });
            Ok::<(), ApiError>(())
        })
        .unwrap();
    group
        .commit()
        .unwrap_or_else(|f| panic!("commit: {}", f.error));
    let before = db.last_applied();
    let mut group = db.begin_group().unwrap();
    let admission = group.tenant_admission();
    let staged = group
        .stage(|tx| {
            write(tx, &admission);
            Ok::<(), ApiError>(())
        })
        .map_err(|e| match e {
            TxError::Io(e) => e.to_string(),
            other => format!("{other:?}"),
        });
    group
        .commit()
        .unwrap_or_else(|f| panic!("commit: {}", f.error));
    let team_7 = db
        .snapshot()
        .unwrap()
        .get_scoped::<Note>(7, Id::from_raw(1))
        .unwrap()
        .map(|n| n.text);
    (staged, db.last_applied() == before, db.poisoned(), team_7)
}

/// A write admitted for `Firm` 7 through `firm_note::Note` lands, by table
/// name and raw scope id, in the registered table as `Team` 7's row. Nothing
/// fails at apply: the bytes decode as a `Note`. It is refused before the
/// record exists, by the writing type.
#[test]
fn a_write_through_the_same_table_under_another_scope_type_is_refused() {
    assert_eq!(
        through_firm_note(0x5A17_000F, |tx, admission| {
            tx.admit_scope(Id::<Firm>::from_raw(7), admission)
                .put(&firm_note::Note {
                    id: Id::from_raw(1),
                    text: "written as Firm 7".to_string(),
                });
        }),
        (
            Err(firm_note_refusal()),
            true,
            false,
            Some("Team 7's note".to_string())
        )
    );
}

/// The delete half: a tombstone carries only an id and a scope id, so one
/// admitted for `Firm` 7 through `firm_note::Note` deleted `Team` 7's row.
#[test]
fn a_delete_through_the_same_table_under_another_scope_type_is_refused() {
    assert_eq!(
        through_firm_note(0x5A17_0010, |tx, admission| {
            tx.admit_scope(Id::<Firm>::from_raw(7), admission)
                .delete::<firm_note::Note>(Id::from_raw(1));
        }),
        (
            Err(firm_note_refusal()),
            true,
            false,
            Some("Team 7's note".to_string())
        )
    );
}

/// The refusal a read through `Reader` gets, spelled out apart from the
/// product's `format!`, as `firm_note_refusal` is for a write.
fn note_read_refusal<Reader>(read: &str) -> String {
    let registered = std::any::type_name::<Note>();
    let reader = std::any::type_name::<Reader>();
    format!(
        "table \"Note\" is registered as `{registered}`, but this transaction read it through \
         `{reader}` ({read}) — two Row types declare TABLE = \"Note\", and a read keys on the \
         table name and the raw tenant id, so it returns the registered type's rows, from the \
         registered scope type's tenant of the same id when the two types' scoped_by differ; \
         the read was refused and returned nothing — read `{registered}` instead, or give \
         `{reader}` a table name of its own (a #[table] type's table name is its struct name)"
    )
}

/// Amendment A18, the read half of the two tests above. A read admitted for
/// `Firm` 7 through `firm_note::Note` keys on the table name and the raw scope
/// id, so it returned `Team` 7's note. Each scoped read through it, and an
/// `across_tenants` scan, is refused before it reads and returns nothing; the
/// registered type still reads `Team` 7's row. `firm_note::Note` declares no
/// index, so `scan_by` reaches its type check and nothing else: without the
/// check it panics naming the missing index.
#[test]
fn a_read_through_the_same_table_under_another_scope_type_is_refused() {
    let sim = Sim::with_faults(0x5A17_0011, FaultConfig::none());
    let mut db = Db::open(
        Box::new(sim.disk()),
        DbConfig::default(),
        Schema::new().table::<Note>(),
    )
    .unwrap();
    let mut group = db.begin_group().unwrap();
    let admission = group.tenant_admission();
    group
        .stage(|tx| {
            tx.admit_scope(Id::<Team>::from_raw(7), &admission)
                .put(&Note {
                    id: Id::from_raw(1),
                    text: "Team 7's note".to_string(),
                });
            Ok::<(), ApiError>(())
        })
        .unwrap();
    group
        .commit()
        .unwrap_or_else(|f| panic!("commit: {}", f.error));

    let refused = |e: std::io::Error| {
        assert!(
            TableReadRefusal::of(&e).is_some(),
            "a TableReadRefusal, not another io error: {e}"
        );
        e.to_string()
    };
    let mut group = db.begin_group().unwrap();
    let admission = group.tenant_admission();
    let cap = group.cross_tenant_cap();
    let as_firm = group
        .stage(|tx| {
            let firm = tx.admit_scope(Id::<Firm>::from_raw(7), &admission);
            Ok::<_, ApiError>((
                firm.get::<firm_note::Note>(Id::from_raw(1))
                    .map(|note| note.map(|note| note.text))
                    .map_err(refused),
                firm.scan::<firm_note::Note>()
                    .map(|notes| notes.len())
                    .map_err(refused),
                firm.scan_by::<firm_note::Note>("text", &"Team 7's note".to_string())
                    .map(|notes| notes.len())
                    .map_err(refused),
                firm.scan_from::<firm_note::Note>(0, 10)
                    .map(|notes| notes.len())
                    .map_err(refused),
            ))
        })
        .unwrap();
    let across = group
        .stage(|tx| {
            Ok::<_, ApiError>(
                tx.across_tenants::<Firm>(&cap)
                    .scan::<firm_note::Note>()
                    .map(|notes| notes.len())
                    .map_err(refused),
            )
        })
        .unwrap();
    let as_team = group
        .stage(|tx| {
            tx.admit_scope(Id::<Team>::from_raw(7), &admission)
                .get::<Note>(Id::from_raw(1))
                .map(|note| note.map(|note| note.text))
                .map_err(|e| e.to_string())
        })
        .unwrap();
    group
        .commit()
        .unwrap_or_else(|f| panic!("commit: {}", f.error));
    assert_eq!(
        (as_firm, across, as_team),
        (
            (
                Err(note_read_refusal::<firm_note::Note>("ScopedTx::get")),
                Err(note_read_refusal::<firm_note::Note>("ScopedTx::scan")),
                Err(note_read_refusal::<firm_note::Note>("ScopedTx::scan_by")),
                Err(note_read_refusal::<firm_note::Note>("ScopedTx::scan_from")),
            ),
            Err(note_read_refusal::<firm_note::Note>(
                "across_tenants().scan"
            )),
            Some("Team 7's note".to_string()),
        )
    );
}

/// A route that reads through `firm_note::Note`, whose table is registered
/// as `Note`, and returns the read's error with `?`.
#[route(GET "/firm-note/:firm", public(Firm))]
fn read_firm_note(tx: &mut Tx, firm: Id<Firm>, ctx: &Ctx) -> Result<WireU64, ApiError> {
    let tx = tx.scope::<Firm>(firm, ctx)?;
    let note = tx.get::<firm_note::Note>(Id::from_raw(1))?;
    Ok(WireU64(note.map_or(0, |note| note.id.raw())))
}

/// Final review triage T9, GUIDE §4's claim at the route: a handler that
/// returns a `TableReadRefusal` with `?` answers a 500 whose body carries
/// the refusal's own text, as a `TableWriteRefusal` does.
#[test]
fn a_route_returning_a_read_refusal_answers_a_500_with_its_text() {
    let sim = Sim::with_faults(0x5A17_0014, FaultConfig::none());
    let mut app = App::open(
        Box::new(sim.disk()),
        Schema::new().table::<Note>(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )
    .unwrap()
    .mount(routes![read_firm_note]);
    let refused = get(&mut app, "/firm-note/7");
    assert_eq!(refused.status, 500, "{}", body_text(&refused));
    assert_eq!(
        error_text(&refused),
        note_read_refusal::<firm_note::Note>("ScopedTx::get")
    );
}

/// `Note` declared `global`: the same identifier and fields, so the same
/// `TABLE`, and no tenant at all. Its reads are `Tx`'s own `View` reads.
mod global_note {
    use super::*;

    #[table(global)]
    pub(super) struct Note {
        pub(super) id: Id<Note>,
        pub(super) text: String,
    }
}

/// Amendment A18 on `Tx`'s own `View` reads (S1 amendment review I2). An
/// unscoped read keys on the table name and `UNSCOPED`, which is 0. Once the
/// row is in the store, a `global` type naming a scoped table fails to open it
/// (`unscoped_open_err`). Before commit, `Tx` reads the transaction's and the
/// group's overlays first, so `global_note::Note` read `Team` 0's note staged
/// earlier in the same batch. Each of the four reads is refused before it
/// reads and returns nothing; the registered type still reads `Team` 0's row.
/// `global_note::Note` declares no index, so without the check `scan_by`
/// panics naming the missing index.
#[test]
fn a_transactions_read_through_a_global_twin_is_refused_before_commit() {
    let sim = Sim::with_faults(0x5A17_0013, FaultConfig::none());
    let mut db = Db::open(
        Box::new(sim.disk()),
        DbConfig::default(),
        Schema::new().table::<Note>(),
    )
    .unwrap();
    let mut group = db.begin_group().unwrap();
    let admission = group.tenant_admission();
    group
        .stage(|tx| {
            tx.admit_scope(Id::<Team>::from_raw(0), &admission)
                .put(&Note {
                    id: Id::from_raw(1),
                    text: "Team 0's note".to_string(),
                });
            Ok::<(), ApiError>(())
        })
        .unwrap();
    let refused = |e: std::io::Error| {
        assert!(
            TableReadRefusal::of(&e).is_some(),
            "a TableReadRefusal, not another io error: {e}"
        );
        e.to_string()
    };
    let in_batch = group
        .stage(|tx| {
            Ok::<_, ApiError>((
                tx.get::<global_note::Note>(Id::from_raw(1))
                    .map(|note| note.map(|note| note.text))
                    .map_err(refused),
                tx.scan::<global_note::Note>()
                    .map(|notes| notes.len())
                    .map_err(refused),
                tx.scan_by::<global_note::Note>("text", &"Team 0's note".to_string())
                    .map(|notes| notes.len())
                    .map_err(refused),
                tx.scan_from::<global_note::Note>(0, 10)
                    .map(|notes| notes.len())
                    .map_err(refused),
            ))
        })
        .unwrap();
    let as_team = group
        .stage(|tx| {
            tx.admit_scope(Id::<Team>::from_raw(0), &admission)
                .get::<Note>(Id::from_raw(1))
                .map(|note| note.map(|note| note.text))
                .map_err(|e| e.to_string())
        })
        .unwrap();
    group
        .commit()
        .unwrap_or_else(|f| panic!("commit: {}", f.error));
    assert_eq!(
        (in_batch, as_team),
        (
            (
                Err(note_read_refusal::<global_note::Note>("Tx::get")),
                Err(note_read_refusal::<global_note::Note>("Tx::scan")),
                Err(note_read_refusal::<global_note::Note>("Tx::scan_by")),
                Err(note_read_refusal::<global_note::Note>("Tx::scan_from")),
            ),
            Some("Team 0's note".to_string()),
        )
    );
}

/// Amendment A18's refusal where `Debug` renders it (hard rule 5). An
/// `io::Error`'s `Debug` is what `main`'s `Error: …`, an `.unwrap()`'s panic
/// and GUIDE §8's sim check (`Err(e) => return Err(format!("{e:?}"))`) print,
/// so a `TableReadRefusal` there must print exactly as an `io::Error`
/// carrying its text alone does (`Refusal`'s `Debug`): the table, both types
/// and the fix, never an empty `error: `.
#[test]
fn a_read_refusal_prints_its_text_under_debug() {
    let sim = Sim::with_faults(0x5A17_0015, FaultConfig::none());
    let mut db = Db::open(
        Box::new(sim.disk()),
        DbConfig::default(),
        Schema::new().table::<Note>(),
    )
    .unwrap();
    let refused = db
        .transact(|tx| tx.get::<global_note::Note>(Id::from_raw(1)).map(|_| ()))
        .unwrap_err();
    let TxError::App(e) = &refused else {
        panic!("the read's own io::Error, returned through the closure: {refused:?}")
    };
    assert!(TableReadRefusal::of(e).is_some(), "a TableReadRefusal: {e}");
    let text = note_read_refusal::<global_note::Note>("Tx::get");
    let bare = TxError::<std::io::Error>::App(std::io::Error::other(text));
    assert_eq!(format!("{refused:?}"), format!("{bare:?}"));
}

// ------------------------------------------ the statuspage regression (oracle 3)

/// Job-tenancy spec §9 item 3. The statuspage experiment's tenant policy, in
/// the shape of `6037704:src/lib.rs:31-59`: `authorize` admitted `None`,
/// because its public page, its public feed and its jobs all opened an org
/// with `open_scope(org, None)`. Its finding I-1: the same policy made every
/// `auth` write one word away from an anonymous write. Flip `open_incident`
/// from `auth` to `public`, and any anonymous caller opens incidents in any
/// org. Under the three rules the flipped route meets rule 3, and the store
/// is unchanged. The `None` arm that made I-1 possible is gone with the
/// `Option` (spec §3): the platform never asks about nobody.
mod statuspage {
    use charpente::auth::Principal;
    use charpente::http::{ApiError, Ctx};
    use charpente::wire::WireU64;
    use charpente::{Id, Scoped, Tenant, Tx, route, table};

    #[table(global)]
    pub struct Org {
        pub id: Id<Org>,
        pub name: String,
    }

    impl Tenant for Org {
        const NAME: &'static str = "Org";
        type Who = Principal;
        type Error = ApiError;

        fn authorize(_tx: &Tx, who: &Principal, scope: Id<Org>) -> Result<(), ApiError> {
            if who.subject() == format!("operator:{}", scope.raw()) {
                Ok(())
            } else {
                Err(ApiError::BadRequest(format!(
                    "{} is not an operator of org {}",
                    who.subject(),
                    scope.raw()
                )))
            }
        }
    }

    #[table(scoped_by = Org)]
    pub struct Incident {
        pub id: Id<Incident>,
        pub title: String,
    }

    #[derive(charpente::Wire, serde::Serialize, serde::Deserialize)]
    pub struct OpenIncidentBody {
        pub title: String,
    }

    /// `6037704:src/incidents.rs:335-357`'s route, with I-1's one-word edit
    /// (`auth` became `public`) and its body reduced to the write.
    #[route(POST "/orgs/:org/incidents", public)]
    pub fn open_incident(
        tx: &mut Tx,
        org: Id<Org>,
        body: OpenIncidentBody,
        ctx: &Ctx,
    ) -> Result<WireU64, ApiError> {
        let mut tx = tx.scope::<Org>(org, ctx)?;
        tx.put(&Incident {
            id: Id::from_raw(1),
            title: body.title,
        });
        Ok(WireU64(1))
    }
}

#[test]
fn statuspage_i1_an_incident_route_flipped_to_public_refuses_every_anonymous_write() {
    let sim = Sim::with_faults(0x7E4A_1709, FaultConfig::none());
    let mut app = App::open(
        Box::new(sim.disk()),
        Schema::new().table::<statuspage::Incident>(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )
    .unwrap()
    .mount(routes![statuspage::open_incident]);

    let refused = app.handle(Request::new(Method::Post, "/orgs/1/incidents").json(
        &statuspage::OpenIncidentBody {
            title: "database down".to_string(),
        },
    ));
    assert_eq!(refused.status, 401, "{}", body_text(&refused));
    // This literal is also GUIDE §5's pinned rule-3 block: doc_truth finds
    // the guide's text here.
    assert_eq!(
        error_text(&refused),
        format!(
            "authentication required — `POST /orgs/:org/incidents` is declared `public` and \
             opened the tenant scope `Org` for a caller with no credential; only a route \
             declared `public(Org)` admits one. Send \"Authorization: Bearer <token>\" or the \
             charpente_session cookie, or, if every caller may act inside this tenant here, \
             declare `public(Org)` in place of `public` in this route's `#[route(…)]` \
             attribute, keeping its other flags. A tenant type is named here by its NAME; \
             name the type in the attribute: `Org` is the tenant type `{}`",
            std::any::type_name::<statuspage::Org>()
        )
    );
    assert_eq!(
        app.db().last_applied(),
        None,
        "the store is unchanged: no incident, and no record of one"
    );
}

// ------------------------------------- a scope type renders as its NAME

/// Two tenant types spelled `Org`, in two modules, with two NAMEs
/// (tenancy-completion spec §4). `authorize` answers `Ok` for everyone:
/// every test below is about a caller with no credential, and `authorize`
/// is never asked about one.
mod billing {
    use charpente::auth::Principal;
    use charpente::http::ApiError;
    use charpente::{Id, Tenant, Tx};

    pub struct Org;

    impl Tenant for Org {
        const NAME: &'static str = "BillingOrg";
        type Who = Principal;
        type Error = ApiError;
        fn authorize(_tx: &Tx, _who: &Principal, _scope: Id<Org>) -> Result<(), ApiError> {
            Ok(())
        }
    }
}

mod crm {
    use charpente::auth::Principal;
    use charpente::http::ApiError;
    use charpente::{Id, Tenant, Tx};

    pub struct Org;

    impl Tenant for Org {
        const NAME: &'static str = "CrmOrg";
        type Who = Principal;
        type Error = ApiError;
        fn authorize(_tx: &Tx, _who: &Principal, _scope: Id<Org>) -> Result<(), ApiError> {
            Ok(())
        }
    }
}

/// A generic tenant type: its one impl gives every instantiation one NAME.
mod wrapped {
    use std::marker::PhantomData;

    use charpente::auth::Principal;
    use charpente::http::ApiError;
    use charpente::{Id, Tenant, Tx};

    pub struct Wrap<B>(PhantomData<B>);

    impl<B: 'static> Tenant for Wrap<B> {
        const NAME: &'static str = "Wrap";
        type Who = Principal;
        type Error = ApiError;
        fn authorize(_tx: &Tx, _who: &Principal, _scope: Id<Wrap<B>>) -> Result<(), ApiError> {
            Ok(())
        }
    }

    pub struct Alpha;

    pub struct Gamma;
}

/// Declared `public(billing::Org)`, but it opens `crm::Org`. Rule 3 applies
/// to a caller with no credential, and the 401 must tell the two `Org`s
/// apart. Path-parameter-free: `Id<S>` is a wire type only for `S: Row`.
#[route(POST "/crm-leads", public(billing::Org))]
fn write_crm_lead(tx: &mut Tx, ctx: &Ctx) -> Result<WireU64, ApiError> {
    let _tx = tx.scope::<crm::Org>(Id::from_raw(1), ctx)?;
    Ok(WireU64(1))
}

/// Opens a tenant of a generic tenant type on a plain `public` route.
#[route(POST "/wrapped", public)]
fn write_wrapped(tx: &mut Tx, ctx: &Ctx) -> Result<WireU64, ApiError> {
    let _tx = tx.scope::<wrapped::Wrap<wrapped::Alpha>>(Id::from_raw(1), ctx)?;
    Ok(WireU64(1))
}

fn rendering_app(seed: u64) -> (Sim, App) {
    let sim = Sim::with_faults(seed, FaultConfig::none());
    let app = App::open(
        Box::new(sim.disk()),
        Schema::new(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )
    .unwrap()
    .mount(routes![write_crm_lead, write_wrapped]);
    (sim, app)
}

/// Spec §4 and the re-review's NB1 (a), at the 401: the route's stance and
/// the scope it opened are two different types spelled `Org`. Each is named
/// by its NAME, and the fix gives each NAME its type's path, since neither
/// NAME is source text (final review M7): the route is written
/// `public(billing::Org)`, and the fix is `public(crm::Org)`.
#[test]
fn the_401_tells_two_tenant_types_spelled_alike_apart_by_their_names() {
    let (_sim, mut app) = rendering_app(0x7E4A_1710);
    let refused = app.handle(Request::new(Method::Post, "/crm-leads"));
    assert_eq!(refused.status, 401, "{}", body_text(&refused));
    assert_eq!(
        error_text(&refused),
        format!(
            "authentication required — `POST /crm-leads` is declared `public(BillingOrg)` and \
             opened the tenant scope `CrmOrg` for a caller with no credential; only a route \
             declared `public(CrmOrg)` admits one. Send \"Authorization: Bearer <token>\" or \
             the charpente_session cookie, or, if every caller may act inside this tenant \
             here, declare `public(CrmOrg)` in place of `public(BillingOrg)` in this route's \
             `#[route(…)]` attribute, keeping its other flags. A tenant type is named here by \
             its NAME; name the type in the attribute: `CrmOrg` is the tenant type `{}`, and \
             `BillingOrg` is `{}`",
            std::any::type_name::<crm::Org>(),
            std::any::type_name::<billing::Org>()
        )
    );
}

/// Spec §4 and NB1 (b), at the 401: a generic tenant type renders its NAME,
/// never a cut of its `type_name` (`Alpha>`). The path beside it is the whole
/// `type_name`, generic arguments and all.
#[test]
fn the_401_names_a_generic_tenant_type_by_its_name() {
    let (_sim, mut app) = rendering_app(0x7E4A_1711);
    let refused = app.handle(Request::new(Method::Post, "/wrapped"));
    assert_eq!(refused.status, 401, "{}", body_text(&refused));
    assert_eq!(
        error_text(&refused),
        format!(
            "authentication required — `POST /wrapped` is declared `public` and opened the \
             tenant scope `Wrap` for a caller with no credential; only a route declared \
             `public(Wrap)` admits one. Send \"Authorization: Bearer <token>\" or the \
             charpente_session cookie, or, if every caller may act inside this tenant here, \
             declare `public(Wrap)` in place of `public` in this route's `#[route(…)]` \
             attribute, keeping its other flags. A tenant type is named here by its NAME; \
             name the type in the attribute: `Wrap` is the tenant type `{}`",
            std::any::type_name::<wrapped::Wrap<wrapped::Alpha>>()
        )
    );
}

// --------------------------------------------- one NAME, one tenant type

/// Two tenant types that both follow the convention, `NAME` = identifier, and
/// so both claim `"Org"` (tenancy-completion spec §4). An app that names both,
/// through any two of its mount paths, is refused before it serves.
mod twins {
    pub mod north {
        use charpente::auth::Principal;
        use charpente::http::ApiError;
        use charpente::{Id, Tenant, Tx};

        pub struct Org;

        impl Tenant for Org {
            const NAME: &'static str = "Org";
            type Who = Principal;
            type Error = ApiError;
            fn authorize(_tx: &Tx, _who: &Principal, _scope: Id<Org>) -> Result<(), ApiError> {
                Ok(())
            }
        }
    }

    pub mod south {
        use charpente::auth::Principal;
        use charpente::http::ApiError;
        use charpente::{Id, Tenant, Tx};

        pub struct Org;

        impl Tenant for Org {
            const NAME: &'static str = "Org";
            type Who = Principal;
            type Error = ApiError;
            fn authorize(_tx: &Tx, _who: &Principal, _scope: Id<Org>) -> Result<(), ApiError> {
                Ok(())
            }
        }
    }
}

/// A tenant type whose `NAME` names nothing.
mod nameless {
    use charpente::auth::Principal;
    use charpente::http::ApiError;
    use charpente::{Id, Tenant, Tx};

    pub struct Org;

    impl Tenant for Org {
        const NAME: &'static str = "";
        type Who = Principal;
        type Error = ApiError;
        fn authorize(_tx: &Tx, _who: &Principal, _scope: Id<Org>) -> Result<(), ApiError> {
            Ok(())
        }
    }
}

#[table(scoped_by = twins::north::Org)]
struct NorthBill {
    id: Id<NorthBill>,
    cents: u64,
}

#[table(scoped_by = twins::south::Org)]
struct SouthLead {
    id: Id<SouthLead>,
    cents: u64,
}

#[table(scoped_by = nameless::Org)]
struct Unnamed {
    id: Id<Unnamed>,
    cents: u64,
}

#[route(POST "/south-leads", public(twins::south::Org))]
fn open_south_lead(tx: &mut Tx, ctx: &Ctx) -> Result<WireU64, ApiError> {
    let _tx = tx.scope::<twins::south::Org>(Id::from_raw(1), ctx)?;
    Ok(WireU64(1))
}

#[job(scoped_by = twins::south::Org)]
struct Nurture {
    n: u64,
}

impl Nurture {
    fn run(self, tx: &mut ScopedTx<'_, '_, twins::south::Org>) -> Result<(), JobError> {
        let _ = (self.n, tx.scope());
        Ok(())
    }
}

/// One generic tenant impl, two instantiations: one NAME, `"Wrap"`, for two
/// types. The Review Focus case.
#[table(scoped_by = wrapped::Wrap<wrapped::Alpha>)]
struct AlphaNote {
    id: Id<AlphaNote>,
    cents: u64,
}

#[table(scoped_by = wrapped::Wrap<wrapped::Gamma>)]
struct GammaNote {
    id: Id<GammaNote>,
    cents: u64,
}

/// `public(Wrap<Alpha>)`: every caller may act inside the `Wrap<Alpha>` it
/// opens. It writes the table and enqueues the job that name the same type.
#[route(POST "/alpha-notes", public(wrapped::Wrap<wrapped::Alpha>))]
fn open_alpha_note(tx: &mut Tx, ctx: &Ctx) -> Result<WireU64, ApiError> {
    let mut tx = tx.scope::<wrapped::Wrap<wrapped::Alpha>>(Id::from_raw(1), ctx)?;
    tx.put(&AlphaNote {
        id: Id::from_raw(1),
        cents: 5,
    });
    tx.enqueue(AlphaDigest { n: 1 });
    Ok(WireU64(1))
}

#[job(scoped_by = wrapped::Wrap<wrapped::Alpha>)]
struct AlphaDigest {
    n: u64,
}

impl AlphaDigest {
    fn run(self, tx: &mut ScopedTx<'_, '_, wrapped::Wrap<wrapped::Alpha>>) -> Result<(), JobError> {
        let _ = (self.n, tx.scan::<AlphaNote>()?);
        Ok(())
    }
}

fn identity_app(sim: &Sim, schema: Schema) -> App {
    App::open(
        Box::new(sim.disk()),
        schema,
        Box::new(sim.entropy()),
        AppConfig::default(),
    )
    .unwrap()
}

/// Runs an app's mount calls and returns what they panicked with.
fn mount_refusal(build: impl FnOnce() -> App) -> String {
    let payload = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        drop(build());
    }))
    .expect_err("the app mounted two tenant types under one NAME, or a nameless one");
    payload
        .downcast_ref::<String>()
        .cloned()
        .unwrap_or_else(|| "a panic payload that is not a String".to_string())
}

/// The uniqueness refusal, spelled out here rather than taken from the
/// product's `format!`, so the comparison pins the words.
fn two_named(name: &str, first: &str, second: &str) -> String {
    format!(
        "two tenant types are named \"{name}\": {first} and {second} — a tenant's NAME is its \
         identity (it is inside every scoped job's and event's fingerprint), so give each type \
         its own `const NAME` in its `impl Tenant`"
    )
}

/// The two twins' full `type_name`s, in the order a refusal names them.
fn north_then_south() -> (&'static str, &'static str) {
    (
        std::any::type_name::<twins::north::Org>(),
        std::any::type_name::<twins::south::Org>(),
    )
}

/// Two scoped tables. `App::open` notes them in `TABLE` order, so the first
/// type named is `NorthBill`'s.
#[test]
fn two_tenant_types_under_one_name_are_refused_when_their_tables_open() {
    let sim = Sim::with_faults(0x7E4A_1712, FaultConfig::none());
    let refusal = mount_refusal(|| {
        identity_app(
            &sim,
            Schema::new().table::<NorthBill>().table::<SouthLead>(),
        )
    });
    let (north, south) = north_then_south();
    assert_eq!(refusal, two_named("Org", north, south));
}

/// A scoped table, then a `public(S)` route of the other type.
#[test]
fn a_public_route_naming_a_second_tenant_type_under_one_name_is_refused_at_mount() {
    let sim = Sim::with_faults(0x7E4A_1713, FaultConfig::none());
    let refusal = mount_refusal(|| {
        identity_app(&sim, Schema::new().table::<NorthBill>()).mount(routes![open_south_lead])
    });
    let (north, south) = north_then_south();
    assert_eq!(refusal, two_named("Org", north, south));
}

/// A scoped table, then a scoped job of the other type.
#[test]
fn a_scoped_job_naming_a_second_tenant_type_under_one_name_is_refused_at_mount() {
    let sim = Sim::with_faults(0x7E4A_1714, FaultConfig::none());
    let refusal = mount_refusal(|| {
        identity_app(&sim, Schema::new().table::<NorthBill>()).mount_jobs(jobs![Nurture])
    });
    let (north, south) = north_then_south();
    assert_eq!(refusal, two_named("Org", north, south));
}

#[test]
fn a_tenant_type_with_an_empty_name_is_refused_when_its_table_opens() {
    let sim = Sim::with_faults(0x7E4A_1715, FaultConfig::none());
    let refusal = mount_refusal(|| identity_app(&sim, Schema::new().table::<Unnamed>()));
    assert_eq!(
        refusal,
        format!(
            "tenant type {} has an empty NAME — give its `impl Tenant` a `const NAME` that \
             names it",
            std::any::type_name::<nameless::Org>()
        )
    );
}

/// Review Focus: a generic tenant impl gives every instantiation one NAME.
/// An app that mounts two instantiations mounts two tenant types under
/// `"Wrap"`, and the refusal names both in full, since their NAME cannot
/// tell them apart.
#[test]
fn two_instantiations_of_one_generic_tenant_type_are_refused_naming_both() {
    let sim = Sim::with_faults(0x7E4A_1716, FaultConfig::none());
    let refusal = mount_refusal(|| {
        identity_app(
            &sim,
            Schema::new().table::<AlphaNote>().table::<GammaNote>(),
        )
    });
    assert_eq!(
        refusal,
        two_named(
            "Wrap",
            std::any::type_name::<wrapped::Wrap<wrapped::Alpha>>(),
            std::any::type_name::<wrapped::Wrap<wrapped::Gamma>>()
        )
    );
}

/// The positive control. One tenant type, `Wrap<Alpha>`, named through every
/// mount path that exists: a scoped table (`App::open`), a `public(S)` route
/// (`mount`) and a scoped job (`mount_jobs`). The app mounts, serves, and
/// renders the type as `"Wrap"`. A check that refused any second sighting,
/// not only a second type, would fail here.
#[test]
fn one_tenant_type_named_through_a_table_a_route_and_a_job_mounts() {
    let sim = Sim::with_faults(0x7E4A_1717, FaultConfig::none());
    let mut app = identity_app(&sim, Schema::new().table::<AlphaNote>())
        .mount(routes![open_alpha_note])
        .mount_jobs(jobs![AlphaDigest]);
    let c = app.describe();
    let route = c
        .routes
        .iter()
        .find(|r| r.name == "open_alpha_note")
        .unwrap();
    assert_eq!(route.public_scope, Some("Wrap"));
    let job = c.jobs.iter().find(|j| j.name == "AlphaDigest").unwrap();
    assert_eq!(job.scoped_by, Some("Wrap"));
    let served = app.handle(Request::new(Method::Post, "/alpha-notes"));
    assert_eq!(served.status, 200, "{}", body_text(&served));
}

/// The same `type_name` under one NAME with two `TypeId`s is not two tenant
/// types: a hand-built `JobDef` whose `scope_type` names a different type
/// than its `scope_type_name` and `scoped_by` do. The refusal must name that
/// cause, not prescribe a second `const NAME` for one type.
#[test]
fn a_jobdef_whose_scope_type_disagrees_with_its_type_name_is_refused_naming_the_cause() {
    let sim = Sim::with_faults(0x7E4A_1718, FaultConfig::none());
    let refusal = mount_refusal(|| {
        let mut def = <AlphaDigest as charpente::kernel::JobEntry>::def();
        def.scope_type = std::any::TypeId::of::<wrapped::Wrap<wrapped::Gamma>>();
        identity_app(&sim, Schema::new().table::<AlphaNote>()).mount_jobs(vec![def])
    });
    assert_eq!(
        refusal,
        format!(
            "two registrations of the tenant type {} (NAME \"Wrap\") are different types — a \
             hand-built `JobDef` or `EventDef` whose `scope_type` and `scope_type_name` name \
             different types (build it from its `#[job]`'s or `#[event]`'s `def()`, or set both \
             fields from the type its `scoped_by` names), or two versions of one crate in the \
             build (unify them)",
            std::any::type_name::<wrapped::Wrap<wrapped::Alpha>>()
        )
    );
}

/// The event twin of the case above (Task 11 fix round 1, review I1). Every
/// `EventDef` field is public, so an app can hand-build one whose
/// `scope_type` disagrees with its `scope_type_name`. `mount_events` hands
/// it to the same census, whose refusal must name the `EventDef` as the
/// cause it can be.
#[event(scoped_by = wrapped::Wrap<wrapped::Alpha>)]
struct AlphaNoted {
    n: WireU64,
}

#[test]
fn an_eventdef_whose_scope_type_disagrees_with_its_type_name_is_refused_naming_the_cause() {
    use charpente::describe::EventEntry;

    let sim = Sim::with_faults(0x7E4A_1719, FaultConfig::none());
    let refusal = mount_refusal(|| {
        let mut def = <AlphaNoted as EventEntry>::def();
        def.scope_type = std::any::TypeId::of::<wrapped::Wrap<wrapped::Gamma>>();
        identity_app(&sim, Schema::new().table::<AlphaNote>()).mount_events(vec![def])
    });
    assert_eq!(
        refusal,
        format!(
            "two registrations of the tenant type {} (NAME \"Wrap\") are different types — a \
             hand-built `JobDef` or `EventDef` whose `scope_type` and `scope_type_name` name \
             different types (build it from its `#[job]`'s or `#[event]`'s `def()`, or set both \
             fields from the type its `scoped_by` names), or two versions of one crate in the \
             build (unify them)",
            std::any::type_name::<wrapped::Wrap<wrapped::Alpha>>()
        )
    );
}

/// Review M2 (hard rule 4): an `EventDef` whose `scoped_by` and
/// `scope_type` disagree is refused at `mount_events`, naming the event,
/// both fields and the fix, in both directions. A scoped type with no
/// `scoped_by` would otherwise skip the census and be described as global,
/// and a `scoped_by` over `Unscoped` would put a non-tenant in the census.
#[test]
fn an_eventdef_whose_scoped_by_and_scope_type_disagree_is_refused_at_mount() {
    use charpente::describe::EventEntry;

    let sim = Sim::with_faults(0x7E4A_171A, FaultConfig::none());
    let unscoped = std::any::type_name::<charpente::core::table::Unscoped>();
    let refusal = mount_refusal(|| {
        let mut def = <noted_org::Noted as EventEntry>::def();
        def.scoped_by = None;
        identity_app(&sim, Schema::new()).mount_events(vec![def])
    });
    assert_eq!(
        refusal,
        format!(
            "contract: event \"noted\" was mounted with `scoped_by: None` and `scope_type` {} \
             — an `EventDef` names its tenant in both, or, for a global event, in neither \
             (`Unscoped`), so this hand-built def would skip the tenant census and be \
             described as global; fix: build it from its `#[event]`'s `def()`, or set \
             `scoped_by`, `scope_type` and `scope_type_name` from the one type its `type \
             Scope` names",
            std::any::type_name::<Org>()
        )
    );
    let refusal = mount_refusal(|| {
        let mut def = <noted_global::Noted as EventEntry>::def();
        def.scoped_by = Some("Org");
        identity_app(&sim, Schema::new()).mount_events(vec![def])
    });
    assert_eq!(
        refusal,
        format!(
            "contract: event \"noted\" was mounted with `scoped_by: Some(\"Org\")` and \
             `scope_type` {unscoped} — an `EventDef` names its tenant in both, or, for a global \
             event, in neither (`Unscoped`), so this hand-built def would enter the tenant \
             census under a type that is no tenant; fix: build it from its `#[event]`'s \
             `def()`, or set `scoped_by`, `scope_type` and `scope_type_name` from the one type \
             its `type Scope` names"
        )
    );
}

// ----------------------------------------- event tenancy (spec §4, §6)

/// Three events with one `NAME` ("noted") and one field list: a global one,
/// one scoped by `Org`, one scoped by `Team`. Only their declarations
/// differ, so only their fingerprints can tell their rows apart.
mod noted_global {
    use charpente::event;
    use charpente::wire::WireU64;

    #[event(global)]
    pub struct Noted {
        pub n: WireU64,
    }
}

mod noted_org {
    use super::Org;
    use charpente::event;
    use charpente::wire::WireU64;

    /// `pub(super)`, not `pub`: the root `Org` is private, and an `impl
    /// Event`'s `type Scope` may not name a type less visible than the event
    /// (E0446).
    #[event(scoped_by = Org)]
    pub(super) struct Noted {
        pub n: WireU64,
    }
}

mod noted_team {
    use super::Team;
    use charpente::event;
    use charpente::wire::WireU64;

    /// `pub(super)` for `noted_org::Noted`'s reason.
    #[event(scoped_by = Team)]
    pub(super) struct Noted {
        pub n: WireU64,
    }
}

/// Scoped by Task 2's `billing::Org`, whose `NAME` ("BillingOrg") is not its
/// identifier. Every other tenant type here follows A4 (`NAME` equals the
/// identifier), so this is the one case that tells "mixes the tenant's
/// `NAME`" from "mixes the scope type's final `::` component", which is
/// `Org` here.
mod noted_billing {
    use super::billing::Org;
    use charpente::event;
    use charpente::wire::WireU64;

    #[event(scoped_by = Org)]
    pub struct Noted {
        pub n: WireU64,
    }
}

/// Tenancy-completion spec §4, §6: a scoped event's fingerprint is its
/// fields' fold continued by `scoped_fingerprint` with its tenant's `NAME`.
/// A `global` event and its `scoped_by` twin never share a shape. Nor do two
/// same-named events scoped by two tenant types whose raw ids may coincide,
/// which is what closes DESIGN §8's shared-raw-id limit for events. The
/// literals are FNV-1a over `"n\0WireU64\0"`, then continued over
/// `"#scoped_by\0Org\0"` and `"#scoped_by\0Team\0"`. They were computed
/// independently of the macro. The `scoped_fingerprint` lines pin the
/// operands, the fold and the `NAME`, and not only that the values differ.
/// The `billing` lines pin that the operand is the `NAME`, not a spelling
/// of the path, in the fingerprint and in the def that `describe` and the
/// tenant census read. `PAYLOAD_FINGERPRINT` is the bare fold for all four
/// (Task 11 fix round 1).
#[test]
fn a_scoped_events_fingerprint_mixes_its_tenants_name() {
    use charpente::describe::EventEntry;

    let global = <noted_global::Noted as Event>::FINGERPRINT;
    let org = <noted_org::Noted as Event>::FINGERPRINT;
    let team = <noted_team::Noted as Event>::FINGERPRINT;
    let billing = <noted_billing::Noted as Event>::FINGERPRINT;
    assert_eq!(
        global, 0x9caf_3f93_9b10_65e1,
        "a global event's is the bare fold"
    );
    assert_eq!(org, 0xeb60_a110_53ba_40dc);
    assert_eq!(team, 0x4874_6e82_94a2_6e7d);
    assert_eq!(
        org,
        charpente::core::db::scoped_fingerprint(global, <Org as Tenant>::NAME),
        "the fold continued by the tenant's NAME, and nothing else"
    );
    assert_eq!(
        billing,
        charpente::core::db::scoped_fingerprint(global, "BillingOrg"),
        "billing::Org's NAME, never the `Org` its path is spelled with"
    );
    assert_ne!(billing, org, "billing::Org and Org are two tenants");
    assert_eq!(
        (
            <noted_global::Noted as Event>::PAYLOAD_FINGERPRINT,
            <noted_org::Noted as Event>::PAYLOAD_FINGERPRINT,
            <noted_team::Noted as Event>::PAYLOAD_FINGERPRINT,
            <noted_billing::Noted as Event>::PAYLOAD_FINGERPRINT
        ),
        (global, global, global, global),
        "every one states the bare fold as its payload fold, which the emit and \
         subscribe sites check its FINGERPRINT against"
    );
    assert_eq!(
        <noted_billing::Noted as EventEntry>::def().scoped_by,
        Some("BillingOrg"),
        "the def names the tenant by NAME too"
    );
    assert_eq!(
        (
            <noted_global::Noted as Event>::NAME,
            <noted_org::Noted as Event>::NAME,
            <noted_team::Noted as Event>::NAME,
            <noted_billing::Noted as Event>::NAME
        ),
        ("noted", "noted", "noted", "noted"),
        "one NAME: only the fingerprint tells the four apart"
    );
}

/// An `Org` of another module, under the root `Org`'s `NAME`. This is what an
/// app gets when two modules each declare a tenant type and both follow the
/// identifier convention. Named through an event beside the root `Org`, it
/// must be refused at mount like the table, route and job cases Task 3 pins.
/// (Not `billing`: Task 2's `billing::Org` in this file is named
/// "BillingOrg" on purpose, and a second module of that name would clash.)
mod accounts {
    use charpente::auth::Principal;
    use charpente::http::ApiError;
    use charpente::wire::WireU64;
    use charpente::{Id, Tenant, Tx, event, table};

    #[table(global)]
    pub struct Org {
        pub id: Id<Org>,
        pub name: String,
    }

    impl Tenant for Org {
        const NAME: &'static str = "Org";
        type Who = Principal;
        type Error = ApiError;
        fn authorize(_tx: &Tx, _who: &Principal, _scope: Id<Org>) -> Result<(), ApiError> {
            Ok(())
        }
    }

    #[event(scoped_by = Org)]
    pub struct InvoiceNoted {
        pub n: WireU64,
    }
}

/// Tenancy-completion spec §4: an event's scope joins the mount-time tenant
/// census. `memo_app` already names the root `Org` (`Memo`'s scope, and its
/// `public(Org)` routes). `accounts::InvoiceNoted` names `accounts::Org` under
/// the same `NAME`. The two types would share every scoped fingerprint's
/// tenant half, so the app is refused at this call, before it serves, by
/// name, with both full type names. The literal is Task 3's pinned text,
/// spelled out here.
#[test]
#[should_panic(
    expected = "two tenant types are named \"Org\": tenant::Org and tenant::accounts::Org — a \
                tenant's NAME is its identity (it is inside every scoped job's and event's \
                fingerprint), so give each type its own `const NAME` in its `impl Tenant`"
)]
fn an_event_scoped_by_a_second_type_under_one_name_is_refused_at_mount() {
    let (_sim, app) = memo_app(0x7E4A_E7E1);
    let _ = app.mount_events(events![accounts::InvoiceNoted]);
}

/// The control: one tenant type, named through a table (`Memo`), the
/// `public(Org)` routes and a scoped event (`MemoNoted`), is one tenant and
/// mounts. Without it, a census that refused every scoped event would pass
/// the test above.
#[test]
fn an_event_scoped_by_the_apps_own_tenant_type_mounts() {
    let (_sim, app) = memo_app(0x7E4A_E7E2);
    let app = app.mount_events(events![MemoNoted]);
    let events: Vec<&str> = app.describe().events.iter().map(|e| e.name).collect();
    assert_eq!(events, vec!["memo_noted"]);
}
