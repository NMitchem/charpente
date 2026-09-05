//! The tenant membership contract (M2 tenant-scoping sub-project,
//! docs/superpowers/specs/2026-08-30-tenant-scoping-design.md §1): opening a
//! scope runs the app's own `authorize`, and nothing reaches a tenant's rows
//! without it. The compile-time half of the property is pinned by trybuild
//! (`crates/charpente-macros/tests/compile_fail/scoped_*.rs`); this file pins
//! the runtime half, which no compile-fail case can express — that the one
//! auditable line per handler actually calls the app back, and that its
//! refusal is what the client gets.
//!
//! It also carries the sub-project's flagship swarm property (hard rule 3):
//! across the full fault matrix, and after every recovery, every scoped read
//! returns ONLY its own tenant's rows — `get`, `scan`, `scan_from` and
//! `scan_by`, two tenants sharing an id space and an index key. Tasks 1-7
//! landed the scoping machinery with compile-fail cases and unit tests; this
//! is the fault-injection oracle they shipped without, and the two naive
//! victims below it are what make its greenness mean something.

use std::sync::atomic::{AtomicU64, Ordering};

use charpente::auth::Principal;
use charpente::http::{ApiError, App, AppConfig, Ctx, Method, Request, Response};
use charpente::wire::WireU64;
use charpente::{
    CrossTenant, Db, DbConfig, Schema, Scoped, ScopedTx, Snapshot, Tenant, Tx, TxError, route,
    routes, table,
};
use charpente_core::io::Entropy;
use charpente_core::log::WalConfig;
use charpente_core::table::{Codec, Id, Row};
use charpente_sim::{FaultConfig, Sim, run_universes_swarm_with};

/// The scope type whose membership check actually refuses. Every other
/// `Tenant` impl in the suite is unconditionally `Ok(())`, and a suite of
/// only-`Ok` impls cannot tell whether `authorize` runs at all — which is why
/// this one exists.
///
/// A `#[table]` rather than a bare marker only so `Id<Org>` can be a route
/// path parameter (`Id<R>`'s `Wire` impl is bounded on `R: Row`); nothing
/// reads the table. A scope type owes nothing but `authorize`, and the
/// platform never DERIVES a scope — it only renders one, as the subscription
/// key `ScopedTx::emit` attaches (`ScopedTx::event_key`, spec §4; the standing
/// `emit_keyed` ruling, DESIGN.md §6).
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

// ------------------------------------------------------- the tenant swarm

/// The swarm property's own scope type, deliberately NOT `Org`.
///
/// Two mechanical reasons, neither stylistic. `Org::authorize` admits exactly
/// one tenant (`ADMITTED`) and must keep refusing `REFUSED`, so it cannot
/// host the N >= 2 writable tenants a leak is only expressible with. And
/// `AUTHORIZE_CALLS`/`BODIES_ENTERED` are process-wide statics that the unit
/// test above resets and then asserts are exactly 1 — a swarm running
/// thousands of `Org::authorize` calls on another thread of the same test
/// binary would make that assertion flake.
///
/// `Firm` admits every tenant: this property is about the row BOUND, not
/// about the membership check, which the unit test above owns.
#[table]
struct Firm {
    id: Id<Firm>,
    name: String,
}

impl Tenant for Firm {
    type Who = Principal;
    type Error = ApiError;

    fn authorize(_tx: &Tx, _who: Option<&Principal>, _scope: Id<Firm>) -> Result<(), ApiError> {
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
/// accepting those wordings here could only ever LOOSEN the oracle.
fn is_injected(text: &str) -> bool {
    matches!(
        charpente_sim::checks::classify(text),
        charpente_sim::checks::FaultReason::Injected
            | charpente_sim::checks::FaultReason::DurableHorizon
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
        let verdict = db.transact(|tx| -> Result<usize, String> {
            let stx = tx
                .open_scope(Id::<Firm>::from_raw(tenant), None)
                .map_err(|e| format!("Firm::authorize refused tenant {tenant}: {e:?}"))?;
            bounded_reads(&stx, tenant)
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

/// One universe: recover -> oracle -> a round of scoped writes, some of them
/// staged as ONE batch carrying two tenants -> crash. Three rounds, then a
/// final recovery that runs the oracle once more.
fn tenant_universe(sim: &mut Sim) -> Result<(), String> {
    let entropy = sim.entropy();
    let cfg = ledger_cfg();

    for _round in 0..3u64 {
        sim.crash_disk_after(64 + entropy.next_u64() % 512);
        let mut db = match Db::open(Box::new(sim.disk()), cfg.clone(), ledger_schema()) {
            Ok(db) => db,
            Err(e) => {
                if !is_injected(&e.to_string()) {
                    return Err(format!("recovery failed with a non-injected error: {e}"));
                }
                sim.restart();
                continue;
            }
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
                let mut stage_failed = false;
                for tenant in TENANTS {
                    match g.stage(|tx| -> Result<(), String> {
                        tx.open_scope(Id::<Firm>::from_raw(tenant), None)
                            .map_err(|e| format!("Firm::authorize refused: {e:?}"))?
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
                match db.transact(|tx| -> Result<(), String> {
                    tx.open_scope(Id::<Firm>::from_raw(tenant), None)
                        .map_err(|e| format!("Firm::authorize refused: {e:?}"))?
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
    // persistent injected faults an open may legitimately never succeed
    // (the oracle already ran at every successful open above), so an
    // injected failure ends the universe rather than failing it.
    for _attempt in 0..8 {
        let mut db = match Db::open(Box::new(sim.disk()), cfg.clone(), ledger_schema()) {
            Ok(db) => db,
            Err(e) => {
                if !is_injected(&e.to_string()) {
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
    Ok(())
}

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
#[test]
fn every_scoped_read_returns_only_its_own_tenants_rows() {
    run_universes_swarm_with!(
        0x7E4A_0001,
        256,
        |_cfg: &mut FaultConfig| {},
        tenant_universe,
        require: ["tenant:multi_tenant_write", "tenant:crash_mid_batch"]
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
            db.transact(|tx| -> Result<(), String> {
                tx.open_scope(Id::<Firm>::from_raw(tenant), None)
                    .map_err(|e| format!("{e:?}"))?
                    .put(&ledger_row(tenant, id));
                Ok(())
            })
            .unwrap();
        }
    }
    (sim, db)
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
#[table]
struct Team {
    id: Id<Team>,
    name: String,
}

impl Tenant for Team {
    type Who = Principal;
    type Error = ApiError;
    fn authorize(_tx: &Tx, _who: Option<&Principal>, _scope: Id<Team>) -> Result<(), ApiError> {
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

#[route(PUT "/note/:team/:id")]
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
#[route(GET "/note/:team")]
fn count_notes(tx: &mut Tx, team: Id<Team>, ctx: &Ctx) -> Result<WireU64, ApiError> {
    let tx = tx.scope::<Team>(team, ctx)?;
    Ok(WireU64(tx.scan::<Note>()?.len() as u64))
}

/// The declared capability (spec §3). `cap` is the token the kernel mints
/// only because this route said `cross_tenant`; without the flag the macro
/// refuses the argument, and without the argument there is no `&CrossTenant`
/// to hand `across_tenants` (`compile_fail/across_tenants_without_capability`).
#[route(GET "/admin/notes", cross_tenant)]
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
    // The operator path to the token: `Group` is the only constructor, which
    // is exactly why a handler cannot mint one (`CrossTenant`).
    let cap = group.cross_tenant_cap();
    let seen: Vec<(u64, u64)> = group
        .stage(|tx| {
            {
                let mut scoped = tx.open_scope(Id::<Team>::from_raw(2), None)?;
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
/// 4) while `crates/charpente/src` contains no `catch_unwind` at all — so
/// the panic unwinds out of the kernel loop and takes the node with it.
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
    let seen: Vec<(u64, u64)> = group
        .stage(|tx| {
            {
                let mut scoped = tx.open_scope(Id::<Team>::from_raw(0), None)?;
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
