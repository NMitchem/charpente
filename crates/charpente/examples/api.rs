//! The API on one page: each attribute with every argument it takes, every
//! method a handler or a job calls on its transaction, the `App` builder,
//! the kernel, the end-state reads, and everything a sim test calls, each
//! used once with its types. Read it; it is not run (`main` is empty). It
//! compiles, so a signature here is the crate's. GUIDE (`charpente::guide`)
//! teaches each one; this page lists them, and a comment says only what the
//! types do not.
//!
//! The sections, in order: `attributes` (with what a handler, a job and a
//! tenant type are handed), `tx`, `scoped_tx`, `app`, `kernel`, `snapshot`
//! (the end state), `suites` (how a sim test file runs its checks), `sim`,
//! `input`, `remotes`, `checks`, `drive`. A sim test of an app under faults
//! starts at `drive`, which boots the app, runs the requests and recovers,
//! so it rarely calls `kernel` or the boot helpers in `checks` itself.
//!
//! `crates/guide-examples/tests/api_census.rs` holds this page: a public
//! function added to a surface it lists fails that test unless the section
//! holding it already calls a function of that name.

// Nothing here is called, since `main` is empty.
#![allow(dead_code)]

use std::any::TypeId;
use std::collections::BTreeMap;
use std::io;
use std::time::Duration;

use charpente::archive::Archive;
use charpente::auth::{OidcConfig, Principal};
use charpente::core::db::{CommitFailure, Group};
use charpente::core::io::{
    Disk, Entropy, IdempotencyKey, Instant, ObjectStore, OutboundCall, OutboundReply, WallTime,
};
use charpente::core::job::JobRow;
use charpente::core::migration::MigrationStep;
use charpente::core::table::Row;
use charpente::describe::Contract;
use charpente::http::{
    ApiError, App, AppConfig, Ctx, Cursor, Handled, Method, Request, Response, StagedDispatch,
    Subscription,
};
use charpente::kernel::{
    Answers, EventFrame, Kernel, KernelStats, RecordedResponses, SimSource, SinkHandles, Step,
};
use charpente::outbound::Host;
use charpente::repl::Repl;
use charpente::{
    CrossTenant, Id, Schema, Scoped, ScopedTx, Snapshot, Subscribe, TenantAdmission, Tx, TxError,
    View, events, jobs, routes,
};
use charpente_sim::checks::{
    FaultReason, acked_implies_durable, boot_through_fallout, calls_have_durable_jobs, classify,
    clock_fallout, clock_refused, clock_reopens_after, dir_entry_loss_fired,
    durable_corruption_fired, durable_refusal, events, events_scoped, is_fault_fallout,
    keys_are_per_job, outbound_settles_by_horizon, replays_identically, wait_out_clock_refusal,
};
use charpente_sim::drive::{Drive, Phase, Recovered};
use charpente_sim::object::SimObjectStore;
use charpente_sim::outbound::{Delivered, Idempotent, Recorder, Remote, SimOutbound, from_fn};
use charpente_sim::rng::Rng;
use charpente_sim::{FaultConfig, Input, Sim, SimClock, SimDisk, SimEntropy, SimHost, SimNet};

use attributes::{
    Charge, Memo, MemoAdded, MemoDeleted, Note, NoteAdded, NoteBody, NoteDeleted, Notify, Org,
    Reindex, Rollup,
};

fn main() {}

/// The app built from every route, job and event on this page, on the disk
/// and entropy it is handed: the builder `charpente::launch` takes. It runs
/// once per boot, so it builds the same app every time.
fn open_app(disk: Box<dyn Disk>, entropy: Box<dyn Entropy>) -> io::Result<App> {
    let schema = Schema::new()
        .table::<Org>()
        .table::<Note>()
        .migration::<Note>("Note")
        .table::<Memo>();
    Ok(App::open(disk, schema, entropy, AppConfig::default())?
        .mount(routes![
            attributes::put_note,
            attributes::relabel_note,
            attributes::delete_note,
            attributes::login,
            attributes::mint_key,
            attributes::logout,
            attributes::count_memos,
            attributes::add_memo,
            attributes::all_memos,
            attributes::memo_feed,
            attributes::note_feed,
        ])
        .mount_jobs(jobs![Reindex, Notify, Rollup, Charge])
        .mount_events(events![NoteAdded, NoteDeleted, MemoAdded, MemoDeleted])
        .schedule("reindex", Duration::from_secs(3600), || Reindex {})
        .outbound(Host::new("pay.example").bearer_env("PAY_KEY")))
}

/// `open_app` on the sim's disk and entropy: the function `Drive::new` takes.
fn sim_app(sim: &Sim) -> io::Result<App> {
    open_app(Box::new(sim.disk()), Box::new(sim.entropy()))
}

/// Each attribute with every argument it takes, and what a handler, a job
/// and a tenant type are handed: `Ctx`, `Call`, `Reply`, `Tenant::authorize`.
mod attributes {
    use charpente::auth::{Principal, Via};
    use charpente::core::migration::{MigrationError, TableMigration};
    use charpente::http::{ApiError, Ctx, Cursor, Response, Session, Subscription};
    use charpente::kernel::JobError;
    use charpente::outbound::{Call, IdempotencyKey, Reply};
    use charpente::wire::WireU64;
    use charpente::{
        CrossTenant, Id, Scoped, ScopedTx, Subscribe, Tenant, Tx, View, Wire, event, job, route,
        row_shape, table,
    };
    use serde::{Deserialize, Serialize};

    // A table, a job and an event each say their tenancy: `global`, or
    // `scoped_by = <tenant type>`. Neither is the default; a bare one does
    // not compile.
    #[table(global)]
    pub struct Org {
        pub id: Id<Org>,
        pub name: String,
    }

    // What `scoped_by = Org` names. A `#[table]` only so `Id<Org>` can be a
    // path parameter; a scope that never crosses the wire can be a bare struct.
    impl Tenant for Org {
        // Its identity, inside every scoped job's and event's fingerprint:
        // renaming it is a shape change.
        const NAME: &'static str = "Org";
        type Who = Principal;
        type Error = ApiError;

        // The app's own membership rule. Runs once per credentialed
        // `tx.scope::<Org>(..)`, never on a route declared `public(Org)`;
        // its `Err` is the handler's answer.
        fn authorize(_tx: &Tx, who: &Principal, scope: Id<Org>) -> Result<(), ApiError> {
            if who.subject() == format!("org:{}", scope.raw()) {
                Ok(())
            } else {
                Err(ApiError::BadRequest(format!(
                    "not a member of org {}",
                    scope.raw()
                )))
            }
        }
    }

    // A superseded shape of `Note`: a codec only (no table, no index), kept
    // so `TableMigration` can read the rows an older binary wrote.
    #[row_shape(version = 1)]
    pub struct NoteV1 {
        pub id: Id<NoteV1>,
        pub text: String,
    }

    // `version = N` (default 1) is the row's shape version. Above 1 it does
    // not compile without `impl TableMigration` from the previous shape, and
    // the schema registers the step (`Schema::migration`, GUIDE §12).
    #[table(global, version = 2)]
    pub struct Note {
        pub id: Id<Note>,
        pub text: String,
        // Generates `Note::by_label(&view, &key)`: exact match, in id order.
        #[index]
        pub label: String,
    }

    impl TableMigration for Note {
        type Prev = NoteV1;
        fn migrate(prev: NoteV1) -> Result<Note, MigrationError> {
            Ok(Note {
                id: Id::from_raw(prev.id.raw()),
                text: prev.text,
                label: String::new(),
            })
        }
    }

    // Keyed by `(tenant, id)`: a bare `Tx` cannot read it, and its
    // `Memo::by_author` takes the tenant's `ScopedTx`.
    #[table(scoped_by = Org)]
    pub struct Memo {
        pub id: Id<Memo>,
        #[index]
        pub author: String,
        pub text: String,
    }

    // Every route parameter, body, response and event payload is `Wire`.
    // The derive reads serde's own attributes and refuses, by name, one it
    // cannot describe (`flatten`, `default`, `untagged`); a bare `u64` or
    // `i64` does not compile (`Id<T>`, or `WireU64`/`WireI64`).
    #[derive(Wire, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct NoteBody {
        pub text: String,
        pub label: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub due_day: Option<String>,
    }

    // An event derives its own `Serialize` and `Wire`: a subscriber receives
    // its JSON. Its `NAME` is the type's snake_case, `note_added`.
    #[event(global)]
    pub struct NoteAdded {
        pub id: Id<Note>,
    }

    #[event(global)]
    pub struct NoteDeleted {
        pub id: Id<Note>,
    }

    // Emitted only on `Org`'s `ScopedTx`, delivered only to that tenant's feed.
    #[event(scoped_by = Org)]
    pub struct MemoAdded {
        pub id: Id<Memo>,
    }

    #[event(scoped_by = Org)]
    pub struct MemoDeleted {
        pub id: Id<Memo>,
    }

    // `retry(max = N, backoff = "<n><unit>")`, unit `ms`, `s`, `m` or `h`:
    // `max` attempts in all (default 5), the wait doubling from `backoff`
    // (default 30s); then the job is Dead.
    #[job(global, retry(max = 3, backoff = "10s"))]
    pub struct Reindex {}

    impl Reindex {
        // You write `run`; `#[job]` derives none. It is its own transaction:
        // `Err(JobError::App(..))` is a failed attempt and commits nothing.
        // `Err(JobError::Io(..))` (what `?` on a read gives) is a platform
        // fault, not an attempt: the kernel stops.
        fn run(self, tx: &mut Tx) -> Result<(), JobError> {
            let _notes: Vec<Note> = tx.scan()?;
            Ok(())
        }
    }

    // Runs inside the tenant it was enqueued in, so `run` takes its `ScopedTx`.
    #[job(scoped_by = Org)]
    pub struct Notify {
        pub memo: Id<Memo>,
    }

    impl Notify {
        fn run(self, tx: &mut ScopedTx<'_, '_, Org>) -> Result<(), JobError> {
            let _memo: Option<Memo> = tx.get(self.memo)?;
            Ok(())
        }
    }

    // `cross_tenant`: `run` is handed the capability after `tx`, the token
    // `across_tenants` and `enqueue_in` take.
    #[job(global, cross_tenant)]
    pub struct Rollup {}

    impl Rollup {
        fn run(self, tx: &mut Tx, cap: &CrossTenant) -> Result<(), JobError> {
            let memos: Vec<(Id<Org>, Memo)> = tx.across_tenants::<Org>(cap).scan()?;
            for (org, memo) in memos {
                tx.enqueue_in::<Org>(org, Notify { memo: memo.id }, cap);
            }
            Ok(())
        }
    }

    // `outbound = "<host>"` (a bare DNS name, declared on the `App` too)
    // calls that host after commit, outside any transaction: write `call`
    // and `settle` in place of `run`. `timeout` bounds the wait for the
    // reply (default 30s) and needs `outbound`; `cross_tenant` beside it is
    // refused.
    #[job(scoped_by = Org, outbound = "pay.example", timeout = "5s")]
    pub struct Charge {
        pub cents: u64,
    }

    impl Charge {
        // Reads, cannot write (`&ScopedTx`), and builds the request. `key` is
        // minted per job and kept across every retry; it is sent only where
        // you put it. `Ok(None)` finishes the job with no call.
        fn call(
            &self,
            _tx: &ScopedTx<'_, '_, Org>,
            key: &IdempotencyKey,
        ) -> Result<Option<Call>, JobError> {
            Ok(Some(
                Call::post("/charges")
                    .header("idempotency-key", key.as_str())
                    .form(&[("amount", self.cents.to_string())]),
            ))
        }

        // Writes the reply's consequences in the transaction that marks the
        // job Done. Every status arrives here, a 3xx too: redirects are not
        // followed.
        fn settle(self, _tx: &mut ScopedTx<'_, '_, Org>, reply: Reply) -> Result<(), JobError> {
            let _receipt: Option<&str> = reply.header("receipt-id");
            let _bytes: &[u8] = reply.body();
            match reply.status() {
                // `JobError::App` when the body is not that type's JSON.
                200 => reply.json::<serde_json::Value>().map(|_| ()),
                s => Err(JobError::app(format!("pay.example answered {s}"))),
            }
        }
    }

    // The other verbs. A path begins with `/` and goes to the declared host.
    // `json` and `form` set the body with its content-type, which replaces
    // any earlier one; `body` sets raw bytes with none.
    fn verbs() -> [Call; 4] {
        [
            Call::get("/charges/1"),
            Call::put("/charges/1").json(&serde_json::json!({ "cents": 100 })),
            Call::patch("/charges/1")
                .body(b"{}".to_vec())
                .header("content-type", "application/json"),
            Call::delete("/charges/1"),
        ]
    }

    // `METHOD "/path/:param"` (GET, POST, PUT, PATCH, DELETE, SSE), then
    // exactly one of `auth`, `public` or `public(<tenant type>)`. The
    // arguments, in this order: `tx: &mut Tx`; one per `:param`, named after
    // it, in path order; at most one JSON body; optionally `cap:
    // &CrossTenant` on a `cross_tenant` route; optionally `ctx: &Ctx` last.
    // It returns `Result<T, ApiError>`, `T: Serialize + Wire`, answered as a
    // 200.
    #[route(PUT "/notes/:id", public)]
    fn put_note(
        tx: &mut Tx,
        id: Id<Note>,
        body: NoteBody,
        ctx: &Ctx,
    ) -> Result<Id<Note>, ApiError> {
        // `None` on a route without `auth` when the caller sent no credential.
        let _who: Option<&Principal> = ctx.principal();
        tx.put(&Note {
            id,
            text: body.text,
            label: body.label,
        });
        tx.emit(NoteAdded { id });
        Ok(id)
    }

    #[route(PATCH "/notes/:id", auth)]
    fn relabel_note(tx: &mut Tx, id: Id<Note>, body: NoteBody) -> Result<(), ApiError> {
        // `?` on a read makes an IO error `ApiError::Internal`.
        let mut note: Note = tx.get(id)?.ok_or(ApiError::NotFound)?;
        note.label = body.label;
        tx.put(&note);
        // What an `Err` is answered with: 400, 401, 404 or 500 by variant,
        // the message as `{"error": ..}`.
        let _answer: Response = ApiError::NotFound.response();
        Ok(())
    }

    #[route(DELETE "/notes/:id", auth)]
    fn delete_note(tx: &mut Tx, id: Id<Note>) -> Result<(), ApiError> {
        tx.delete(id);
        tx.emit(NoteDeleted { id });
        Ok(())
    }

    // `auth`: the caller presented a credential (a session, an API key or an
    // OIDC token), so `ctx.principal()` is `Some`. A login trades a key or a
    // token for a session in the caller's own subject. charpente has no
    // password check, so never mint for a subject a request body names: that
    // lets anyone log in as anyone.
    #[route(POST "/login", auth)]
    fn login(tx: &mut Tx, ctx: &Ctx) -> Result<String, ApiError> {
        let principal: &Principal = ctx.principal().expect("auth route");
        // `Session` and `ApiKey` carry the matched row's id; `Oidc` has no row.
        // A session that could buy a session would outlive its TTL and logout.
        if let Via::Session(_) = principal.via() {
            return Err(ApiError::BadRequest(
                "log in with an API key or an OIDC token, not a session".to_string(),
            ));
        }
        // A session that expires after the app's session TTL. One mint per
        // request, shared with `create_api_key`.
        Ok(ctx.create_session(tx, principal.subject().to_string()))
    }

    #[route(POST "/keys", auth)]
    fn mint_key(tx: &mut Tx, ctx: &Ctx) -> Result<String, ApiError> {
        let subject = ctx.principal().map(|p| p.subject().to_string());
        // `"ck_"` and 64 hex characters; only its hash is stored.
        Ok(ctx.create_api_key(tx, subject.unwrap_or_default()))
    }

    #[route(POST "/logout", auth)]
    fn logout(tx: &mut Tx, ctx: &Ctx) -> Result<(), ApiError> {
        // `Some` only when the caller authenticated with a session.
        let _session: Option<&Session> = ctx.session();
        // Deletes the current session; a no-op without one.
        ctx.revoke(tx);
        Ok(())
    }

    // The scope line: `tx.scope::<Org>(org, ctx)?` runs `Org::authorize` for
    // the caller and returns the only handle that reaches `Org`'s rows.
    #[route(GET "/orgs/:org/memos", auth)]
    fn count_memos(tx: &mut Tx, org: Id<Org>, ctx: &Ctx) -> Result<WireU64, ApiError> {
        let tx = tx.scope::<Org>(org, ctx)?;
        Ok(WireU64(
            Memo::by_author(&tx, &"ann".to_string())?.len() as u64
        ))
    }

    // `public(Org)`: every caller, credentialed or not, may act inside the
    // `Org` it opens, and `authorize` does not run.
    #[route(POST "/orgs/:org/memos/:id", public(Org))]
    fn add_memo(
        tx: &mut Tx,
        org: Id<Org>,
        id: Id<Memo>,
        body: NoteBody,
        ctx: &Ctx,
    ) -> Result<Id<Memo>, ApiError> {
        let mut tx = tx.scope::<Org>(org, ctx)?;
        tx.put(&Memo {
            id,
            author: body.label,
            text: body.text,
        });
        tx.emit(MemoAdded { id });
        tx.enqueue(Charge { cents: 100 });
        Ok(id)
    }

    // `cross_tenant` declares the capability to enumerate a scoped table
    // across every tenant, and `charpente describe` lists the route.
    #[route(GET "/admin/memos", auth, cross_tenant)]
    fn all_memos(tx: &mut Tx, cap: &CrossTenant, ctx: &Ctx) -> Result<WireU64, ApiError> {
        // The same token, read off the request: `Some` exactly on a route
        // that declared `cross_tenant`.
        let _declared: Option<&CrossTenant> = ctx.cap();
        let all: Vec<(Id<Org>, Memo)> = tx.across_tenants::<Org>(cap).scan()?;
        Ok(WireU64(all.len() as u64))
    }

    // `SSE` is a stream: its handler ends `ctx: &Ctx, cursor: Option<Cursor>`
    // and returns `Result<Subscription, ApiError>`, spelled out (an alias is
    // refused). `cursor` comes from `Last-Event-ID`, else the `cursor` query
    // parameter; `None` tails from now. `emits(..)` declares what it
    // carries, for `describe` and the generated client.
    #[route(SSE "/orgs/:org/feed", auth, emits(MemoAdded, MemoDeleted))]
    fn memo_feed(
        tx: &mut Tx,
        org: Id<Org>,
        ctx: &Ctx,
        cursor: Option<Cursor>,
    ) -> Result<Subscription, ApiError> {
        let tx = tx.scope::<Org>(org, ctx)?;
        Ok(tx
            .subscription::<MemoAdded>()
            .and::<MemoDeleted>()
            .cursor(cursor)
            .into())
    }

    // A global feed: `Subscription::of::<Ev>()`, more global types with
    // `.and::<Ev>()` (a scoped event does not compile in either), narrowed
    // by `.key(..)` to events emitted with that key.
    #[route(SSE "/feed", public, emits(NoteAdded, NoteDeleted))]
    fn note_feed(tx: &mut Tx, ctx: &Ctx, cursor: Option<Cursor>) -> Result<Subscription, ApiError> {
        let _ = (tx, ctx);
        Ok(Subscription::of::<NoteAdded>()
            .and::<NoteDeleted>()
            .key("user:7")
            .cursor(cursor))
    }
}

/// `Tx`: a route's, a job's or a cron tick's transaction. It reads its own
/// writes. It reaches unscoped tables only; a scoped table is behind
/// `tx.scope::<Org>(..)` (`scoped_tx`).
#[allow(clippy::too_many_arguments)]
fn tx(
    tx: &mut Tx,
    note: Note,
    org: Id<Org>,
    memo: Id<Memo>,
    ctx: &Ctx,
    who: &Principal,
    cap: &CrossTenant,
    admission: &TenantAdmission,
    job: &JobRow,
) -> Result<(), ApiError> {
    let id = note.id;
    // `View`'s four reads (`use charpente::View`), in id order. A scoped row
    // type or a platform table does not compile here.
    let _one: Option<Note> = tx.get(id)?;
    let _all: Vec<Note> = tx.scan()?;
    // By the `#[index]` field's name; `Note::by_label` is the typed form.
    let _labelled: Vec<Note> = tx.scan_by("label", &"urgent".to_string())?;
    // Rows with `id >= from_id`, at most `limit`: page from the last id + 1.
    let _page: Vec<Note> = tx.scan_from(0, 100)?;
    // The step's held wall reading, the same for every transaction of one
    // kernel step. Panics by name on a bare `Db::transact` or `begin_group`,
    // which carry none: see `Db::transact_at` and `Group::set_now`.
    let _at: WallTime = tx.now();
    tx.put(&note);
    tx.delete(id);
    // A `global` job; a `scoped_by` one does not compile on a bare `Tx`. An
    // outbound job is enqueued the same way, in a route's, job's or tick's
    // transaction.
    tx.enqueue(Reindex {});
    // The key is the app's label, carried on the job's row: never
    // deduplicated on, and not the outbound idempotency key.
    tx.enqueue_keyed("nightly", Reindex {});
    // Due at a wall time, or a delay after `tx.now()`; a past time is due at once.
    tx.enqueue_at(WallTime::from_nanos(1_000), Reindex {});
    tx.enqueue_after(Duration::from_secs(60), Reindex {});
    // Fan-out into one tenant, from a route or job that declared
    // `cross_tenant`: the job runs as if enqueued inside `org`.
    tx.enqueue_in::<Org>(org, Notify { memo }, cap);
    // A `global` event; a scoped one is emitted on its tenant's `ScopedTx`.
    tx.emit(NoteAdded { id });
    // The key is a filter a feed selects with `.key(..)`, never a tenant.
    tx.emit_keyed("user:7", NoteAdded { id });
    // Every tenant's rows, each paired with its tenant.
    let _everyone: Vec<(Id<Org>, Memo)> = tx.across_tenants::<Org>(cap).scan()?;
    // The scope line a handler writes (`use charpente::Scoped`): a
    // `public(Org)` route admits, a principal is asked `Org::authorize`, and
    // an anonymous caller gets a 401.
    let _count: usize = tx.scope::<Org>(org, ctx)?.scan::<Memo>()?.len();
    // `authorize` asked about `who` directly. A transaction opens one
    // tenant: a second, different one panics.
    let _asked: Result<Id<Org>, ApiError> = tx.open_scope::<Org>(org, who).map(|s| s.scope());
    // Operator-only: these take a `TenantAdmission`, which only a `Group`
    // mints (`group.tenant_admission()`), so no handler or job can call them.
    let _admitted: Id<Org> = tx.admit_scope::<Org>(org, admission).scope();
    let _row: Option<JobRow> = tx.get_job(job.id, admission)?;
    tx.settle_job(job, admission);
    Ok(())
}

/// `ScopedTx`: one tenant's transaction, from `tx.scope::<Org>(org, ctx)?` in
/// a handler, or handed to a scoped job. Every read and write is bounded to
/// `scope()`, and it derefs to the `Tx` underneath for unscoped tables.
fn scoped_tx(tx: &mut ScopedTx<'_, '_, Org>, memo: Memo) -> Result<(), ApiError> {
    let id = memo.id;
    // The tenant. A second scope cannot be opened inside this one.
    let _org: Id<Org> = tx.scope();
    // `Tx`'s four reads, over this tenant's rows only.
    let _one: Option<Memo> = tx.get(id)?;
    let _all: Vec<Memo> = tx.scan()?;
    let _by_author: Vec<Memo> = tx.scan_by("author", &"ann".to_string())?;
    let _page: Vec<Memo> = tx.scan_from(0, 100)?;
    tx.put(&memo);
    tx.delete(id);
    // Recorded with this tenant, so only this tenant's feed delivers it.
    tx.emit(MemoDeleted { id });
    tx.emit_keyed("author:ann", MemoAdded { id });
    // A scoped job, run in this tenant.
    tx.enqueue(Notify { memo: id });
    tx.enqueue_keyed("memo-saved", Notify { memo: id });
    tx.enqueue_at(WallTime::from_nanos(1_000), Notify { memo: id });
    tx.enqueue_after(Duration::from_secs(60), Notify { memo: id });
    // A `global` event or job goes through the deref: `(*tx)` on an owned
    // `ScopedTx`, `(**tx)` on a `&mut ScopedTx` as here.
    (**tx).emit(NoteAdded {
        id: Id::from_raw(id.raw()),
    });
    // This tenant's feed (`use charpente::Subscribe`), with more of its
    // event types through `.and`; `.into()` makes it an SSE handler's
    // `Subscription`.
    let _feed: Subscription = tx
        .subscription::<MemoAdded>()
        .and::<MemoDeleted>()
        .key("author:ann")
        .cursor(None)
        .into();
    Ok(())
}

/// `App`: the schema, routes, jobs, events, schedules and outbound hosts,
/// built once and moved into a `Kernel`. Each mount panics at its call on a
/// conflict or a duplicate name, before anything serves.
fn app(sim: &Sim, oidc: OidcConfig) -> io::Result<()> {
    let schema = Schema::new()
        .table::<Org>()
        .table::<Note>()
        .migration::<Note>("Note")
        .table::<Memo>();
    // Read back by the platform: the names sorted, each scoped table's
    // tenant type, a table's migration steps, and its `(version,
    // fingerprint)`.
    let _names: Vec<&'static str> = schema.table_names();
    let _tenants: Vec<(&'static str, TypeId, &'static str)> = schema.table_tenants();
    let _steps: &[MigrationStep] = schema.migrations_for("Note");
    let _meta: Option<(u32, Option<u64>)> = schema.table_meta("Note");
    // Opens the store on `disk`, recovering what it holds, with the app's
    // tables and the platform's own (sessions, API keys, cron).
    let mut app = App::open(
        Box::new(sim.disk()),
        schema,
        Box::new(sim.entropy()),
        AppConfig::default(),
    )?
    .mount(routes![attributes::put_note, attributes::count_memos])
    // Registers each job's shape and tenancy with the store, so an enqueue
    // under another shape or tenancy refuses its transaction.
    .mount_jobs(jobs![Reindex, Notify, Rollup, Charge])
    // For `describe` and the generated client only: an unmounted `#[event]`
    // still emits and delivers.
    .mount_events(events![NoteAdded, MemoAdded])
    // Enqueues `make()` every `every`. A tick is a bare `Tx`, so `global`
    // jobs only; per-tenant work fans out with `enqueue_in`.
    .schedule("reindex", Duration::from_secs(3600), || Reindex {})
    // One `Host` per host a mounted job calls, checked when the kernel is
    // built. Credentials come from the environment, never from code.
    .outbound(
        Host::new("pay.example")
            .bearer_env("PAY_KEY")
            .max_request_bytes(1 << 16)
            .max_reply_bytes(1 << 20),
    )
    // Calls in flight at once (default 64); a pure job never waits on it.
    .outbound_max_in_flight(64)
    // Parses the JWKS once, here: a bad document panics at this call, never
    // at a request.
    .with_oidc(oidc);
    // The other credential forms; each panics when the host already injects
    // that header.
    let _mail: Host =
        Host::new("mail.example").header_env("x-postmark-server-token", "POSTMARK_TOKEN");
    let _ledger: Host = Host::new("ledger.example").basic_env("LEDGER_USER", "LEDGER_PASS");
    // A binary's `main`. With no verb it serves on `addr` with its store in
    // `data_dir`; `describe` prints the contract and `restore` restores from
    // an archive. It picks the disk (a temporary one for `describe`), so it
    // takes the builder, not an `App`.
    let _main = || charpente::launch("0.0.0.0:8080", "./data", open_app);
    // The hosts the mounted jobs call, sorted: what `sim.outbound(..)` takes.
    // Read it before building a kernel, which takes the job defs.
    let _hosts: Vec<&'static str> = app.outbound_hosts();
    // The machine-readable contract (GUIDE §13).
    let _contract: Contract = app.describe();
    let _config: &AppConfig = app.config();
    // The store. Whoever holds it reaches every tenant with no `authorize`.
    let _snap: Snapshot = app.db().snapshot()?;
    // One request with no kernel. A stream route answers 500 here: only a
    // kernel serves a subscription. `now` is the Unix epoch.
    let _answer: Response = app.handle(Request::new(Method::Get, "/orgs/1/memos"));
    // `handle` at a clock reading you choose, for a session-expiry test.
    let _later: Response = app.handle_at(
        Request::new(Method::Post, "/logout"),
        WallTime::from_nanos(1),
    );
    // `handle_at`, handing back a stream route's `Subscription`.
    let _either: Handled =
        app.handle_or_subscribe(Request::new(Method::Get, "/feed"), WallTime::from_nanos(1));
    // Built by hand, as a check states the answer it expects. `error`'s body
    // is `{"error": message}`; `sse_headers` is the empty 200 a stream opens
    // with.
    let _expected: [Response; 3] = [
        Response::json(200, &7),
        Response::error(404, "not found"),
        Response::sse_headers(),
    ];
    // A transaction's time, `tx.now()`: the kernel's step reading for a route,
    // job or cron tick. A group from `Db::begin_group` carries none until
    // `set_now`; `transact_at` is a lone transaction with one. Both are for
    // tests; `App::begin_group` below sets it from its argument.
    let mut own: Group = app.db_for_tests().begin_group()?;
    own.set_now(WallTime::from_nanos(1));
    let _own_now: Result<WallTime, TxError<ApiError>> =
        own.stage(|tx: &mut Tx| -> Result<WallTime, ApiError> { Ok(tx.now()) });
    let _ = own.commit();
    let _at: Result<WallTime, TxError<ApiError>> = app.db_for_tests().transact_at(
        WallTime::from_nanos(1),
        |tx: &mut Tx| -> Result<WallTime, ApiError> { Ok(tx.now()) },
    );
    // Many requests staged into one group and committed with one sync, as
    // the kernel batches them. A non-empty `Group` that is dropped, not
    // committed, panics.
    let (mut group, dispatch): (Group, StagedDispatch) =
        app.begin_group(WallTime::from_nanos(1))?;
    // One request, routed and staged. Each stage sees every earlier stage's
    // writes; none is durable before `commit`.
    let _staged: Handled = dispatch.stage_or_subscribe(
        &mut group,
        Request::new(Method::Get, "/feed"),
        WallTime::from_nanos(1),
    );
    // A transaction of your own. `Err`, or no effects, leaves the group as
    // it was.
    let _count: Result<usize, TxError<ApiError>> =
        group.stage(|tx: &mut Tx| -> Result<usize, ApiError> { Ok(tx.scan::<Note>()?.len()) });
    // The only constructors of the tokens a `cross_tenant` route or job is
    // handed and `admit_scope` takes.
    let _cap: CrossTenant = group.cross_tenant_cap();
    let _admission: TenantAdmission = group.tenant_admission();
    // Records awaiting `commit`: a stage that returned `Err` or wrote
    // nothing adds none.
    let _records: usize = group.len();
    let _nothing: bool = group.is_empty();
    // On `Err`, whether a request that ran when the group held
    // `records_before` records, and staged one or not, saw a write that
    // failed to apply, and so cannot be answered as committed.
    let committed: Result<(), CommitFailure> = group.commit();
    if let Err(failure) = &committed {
        let _unsure: bool = failure.invalidates(0, false);
    }
    Ok(())
}

/// `Kernel`: the event loop, the same code under `serve` and under the sim;
/// only the `RequestSource` differs. It keeps its `App` for life. A sim test
/// on `drive` lets the drive build and run it.
fn kernel(
    sim: &Sim,
    repl: impl Fn() -> Repl,
    archive: impl Fn() -> (Archive, Box<dyn ObjectStore>),
) -> io::Result<()> {
    // An `Err` (a named refusal) for an app that mounts an outbound job (use
    // `with_outbound`), a Pending job enqueued under another shape, a wall
    // clock more than `CLOCK_TOLERANCE` behind the time the data directory
    // recorded, or a `Host` set that does not match the jobs' hosts.
    let mut kernel: Kernel = Kernel::new(sim_app(sim)?, Box::new(sim.clock()))?;
    // Both `horizon_nanos` and `push_at`'s time are absolute virtual
    // nanoseconds, not offsets: add them to `sim.now()`.
    let start = sim.now().as_nanos();
    let mut source = SimSource::new(Box::new(sim.clock()), start + 60_000_000_000);
    // In arrival order (asserted). A `?query` is split off the path, with no
    // percent-decoding.
    source.push_at(
        start + 1_000_000,
        Request::new(Method::Get, "/feed?cursor=5.9"),
    );
    source.push_at(
        start + 2_000_000,
        Request::new(Method::Put, "/notes/1")
            .header("authorization", "Bearer <token>")
            .json(&serde_json::json!({ "text": "hi", "label": "urgent" })),
    );
    // Upper case only, and no `SSE`: a stream route is requested with `GET`.
    let _patch: Option<Method> = Method::parse("PATCH");
    let _name: &'static str = Method::Patch.name();
    // A stall for the next SSE upgrade: `true` makes a send report `Full`.
    source.push_stall(Box::new(|| false));
    // Slot `i` is the `i`th `push_at`'s reply, `None` while unanswered.
    let _answers: Answers = source.answers();
    // The same `(status, body)` replies in answer order, without the slots.
    let _responses: RecordedResponses = source.responses();
    // A handle per SSE upgrade, in upgrade order.
    let sinks: SinkHandles = source.sinks();
    // `Ok(())` once nothing is left inside the horizon. After `Ok` it can run
    // again on another source; after `Err` it has stopped for good.
    kernel.run(&mut source)?;
    // The frames each upgrade's subscriber accepted, and `closed` once the
    // kernel dropped its tail. A frame's `id` and `seq` are the cursor a
    // client resumes with (an error frame's are not).
    for sink in sinks.lock().expect("no earlier panic").iter() {
        let frames: Vec<EventFrame> = sink.frames();
        // The event's `NAME` (or a platform frame's `charpente.*` name), and its
        // payload as JSON text.
        let _event: Option<(&str, &str)> = frames.first().map(|f| (f.name, f.json.as_str()));
        let _resume: Option<String> = frames.last().map(|f| {
            Cursor {
                id: f.id,
                seq: f.seq,
            }
            .wire()
        });
        let _ended: bool = sink.closed();
    }
    // `None` unless the text is `"<id>.<seq>"`; the kernel answers such a
    // cursor with a 400 before the handler runs.
    let _cursor: Option<Cursor> = Cursor::parse("5.9");
    // Test-only, between two runs of one kernel: both methods are hidden from
    // the docs. One minting transaction on the kernel's App, so it can
    // enqueue an outbound job too; the next `run` picks the job up as it
    // starts.
    let _seeded: Result<(), TxError<ApiError>> =
        kernel
            .app_mut_for_tests()
            .transact_for_tests(|tx: &mut Tx| -> Result<(), ApiError> {
                tx.enqueue(Reindex {});
                Ok(())
            });
    // One iteration: `Worked`, or `Idle { deadline }` without waiting.
    let _step: Step = kernel.step(&mut source)?;
    let _stats: KernelStats = kernel.stats();
    // The end state, through `snapshot` below.
    let _app: &App = kernel.app();
    // `false` on a follower or a draining primary; a plain kernel is primary.
    let _primary: bool = kernel.is_primary();
    // The archive's last notice (a gap, an ownership refusal), taken; always
    // `None` without an archive.
    let _notice: Option<String> = kernel.take_archive_notice();
    // For an app with outbound jobs. The handle comes from the app's hosts,
    // read before the app moves.
    let app = sim_app(sim)?;
    let calls: SimOutbound = sim.outbound(&app.outbound_hosts())?;
    let _calling: Kernel = Kernel::with_outbound(app, Box::new(sim.clock()), Box::new(calls))?;
    // A replicated node (GUIDE §10): a follower serves reads, never jobs or
    // cron.
    let _replicated: Kernel =
        Kernel::with_replication(sim_app(sim)?, Box::new(sim.clock()), repl())?;
    // `Archive::open` must already have run against this app's store (GUIDE
    // §15).
    let (shipper, store) = archive();
    let _archived: Kernel =
        Kernel::with_archive(sim_app(sim)?, Box::new(sim.clock()), shipper, store)?;
    // Any of the three together.
    let (shipper, store) = archive();
    let _all: Kernel = Kernel::builder(sim_app(sim)?, Box::new(sim.clock()))
        .replication(repl())
        .archive(shipper, store)
        .outbound(Box::new(sim.outbound(&["pay.example"])?))
        .build()?;
    Ok(())
}

/// `Snapshot`: the end state a check judges, from
/// `kernel.app().db().snapshot()` after a run. It reads every tenant on the
/// caller's word: no `authorize` runs.
fn snapshot(kernel: &Kernel, org: Id<Org>, id: Id<Memo>) -> io::Result<()> {
    let snap: Snapshot = kernel.app().db().snapshot()?;
    // An unscoped table reads through `View`, as on a `Tx`.
    let _notes: Vec<Note> = snap.scan()?;
    // A scoped table takes the tenant's raw id. On an unscoped table these
    // four panic.
    let _one: Option<Memo> = snap.get_scoped(org.raw(), id)?;
    let _all: Vec<Memo> = snap.scan_scoped(org.raw())?;
    let _page: Vec<Memo> = snap.scan_from_scoped(org.raw(), 0, 100)?;
    let _by_author: Vec<Memo> = snap.scan_by_scoped(org.raw(), "author", &"ann".to_string())?;
    // Events decode as the app's types through `checks::events`, not below.
    // The platform's raw reads, for replication and the archive: undecoded
    // bytes, every tenant.
    let _rows: Vec<(u64, u64, Vec<u8>)> = snap.scan_raw(Memo::TABLE, true)?;
    let _batch: Vec<(u64, u64, Vec<u8>)> = snap.scan_raw_from(Memo::TABLE, true, None, 100)?;
    let _keys: Vec<Vec<u8>> = snap.scan_index_raw::<Memo>("author")?;
    // As of this snapshot: the log position it reflects, the next ids, the
    // oldest stored event's id.
    let _seq: Option<u64> = snap.applied_seq()?;
    let _next_job: u64 = snap.next_job_id()?;
    let _next_event: u64 = snap.next_event_id()?;
    let _first_event: Option<u64> = snap.first_event_id()?;
    // `Some((seq, crc))` only on a store built by the seed path.
    let _frontier: Option<(u64, u32)> = snap.seed_frontier()?;
    Ok(())
}

/// How a sim test file runs its checks. An app's `tests/sim.rs` lists them in
/// `sim_tests!` for `charpente test --sim`; the `run_universes` macros are
/// the runners of charpente's own `cargo test` suites.
fn suites() {
    // A check: one universe, `Ok(())` when the property held.
    fn memo_survives(sim: &mut Sim) -> Result<(), String> {
        sim.cover("memo:judged");
        Ok(())
    }
    // Writes the file's `main`. A `require` key must be marked in at least
    // one of that check's runs in a batch; a `--seed` replay checks none.
    charpente_sim::sim_tests! { memo_survives require ["memo:judged"] }
    // One fault matrix for every universe, from a fresh base seed or, with
    // `_from`, a fixed one. `CHARPENTE_SEED=0x…` replays one universe and
    // skips every `require:`, `floor:` and `ceiling:`.
    charpente_sim::run_universes!(64, memo_survives);
    charpente_sim::run_universes_from!(0x5eed, 64, memo_survives);
    // Each universe samples its own `FaultConfig::swarm`. `ceiling:` fails the
    // batch when a key is marked more than N times.
    charpente_sim::run_universes_swarm!(
        64,
        memo_survives,
        require: ["memo:judged"],
        ceiling: [("run:skipped", 8)]
    );
    // A fixed base, and `adjust` edits each universe's config, handed that
    // universe's seed. `floor:` fails the batch when a key is marked fewer
    // than N times.
    charpente_sim::run_universes_swarm_with!(
        0x5eed,
        64,
        |_seed: u64, cfg: &mut FaultConfig| cfg.write_error = 0.0,
        memo_survives,
        require: ["memo:judged"],
        floor: [("memo:judged", 32)]
    );
}

/// `Sim`: one seeded universe, with virtual time, disks, clocks, entropy, a
/// network and remote models, handed to each check. Everything it does is
/// a function of its seed.
fn sim(seed: u64) -> io::Result<()> {
    // `sim_tests!` builds the `Sim` a check gets. Built by hand, `new` runs
    // the default fault rates (2% write and sync errors among them) and
    // `with_faults(seed, FaultConfig::none())` runs none.
    let _defaults: Sim = Sim::new(seed);
    let sim: Sim = Sim::with_faults(seed, FaultConfig::none());
    // Each axis in or out, an active one's rate drawn from a wide range that
    // includes extremes `default()` never visits: the sampler
    // `run_universes_swarm!` uses.
    let _drawn: FaultConfig = FaultConfig::swarm(&mut Rng::new(seed));
    let _seed: u64 = sim.seed();
    // The run's drawn inputs: see `input`.
    let _input: Input = sim.input();
    // What an app and a kernel are built on, all in this one world.
    let _disk: SimDisk = sim.disk();
    let _entropy: SimEntropy = sim.entropy();
    let clock: SimClock = sim.clock();
    // Its offset from `sim.true_wall()` in nanoseconds (skew, step-backs,
    // downtime); it draws nothing.
    let _offset: i64 = clock.offset();
    // Virtual nanoseconds since the universe began; no table stores one.
    // `duration_since` panics on a later argument.
    let now: Instant = sim.now();
    let _ns: u64 = now.as_nanos();
    let _elapsed: Duration = now.duration_since(Instant::from_nanos(0));
    // Virtual time on the wall, with no host's skew: judge expiry and
    // lateness against this, never against a host's clock. Nanoseconds since
    // the Unix epoch, the type of every stored time.
    let wall: WallTime = sim.true_wall();
    let _wall_ns: u64 = wall.as_nanos();
    let _minute_ago: WallTime = wall.saturating_sub(Duration::from_secs(60));
    // Zero when the argument is later.
    let _since: Duration = wall.saturating_duration_since(WallTime::from_nanos(0));
    // Marks a named event (by convention `"<area>:<what>"`), for `require`.
    sim.cover("memo:judged");
    let _marks: BTreeMap<&'static str, u64> = sim.coverage();
    // A run that cannot reach a verdict says so, then returns `Ok(())`.
    // `reason` is fixed text the batch groups by; `detail` is the run's own.
    // The first give-up wins.
    sim.give_up("no memo landed, so there was nothing to judge", "");
    let _reason: Option<&'static str> = sim.gave_up();
    let _detail: Option<String> = sim.gave_up_detail();
    // A crash on the `ops`-th next disk operation, or now; the disk stays
    // gone until `restart`.
    sim.crash_disk_after(3);
    sim.crash_disk_now();
    let _gone: bool = sim.disk_crashed();
    // A power cycle: each unsynced write survives, tears or vanishes, and
    // open handles die.
    sim.restart();
    // A process restart: open handles die, the page cache survives.
    sim.restart_process();
    // What the next power cycle may tear or drop.
    let _pending: Vec<String> = sim.unsynced_files();
    // Faults off for the rest of the run, axis by axis. `drive`'s tail calls
    // the first two, and the third when the app declares hosts.
    sim.quiesce_disk_faults();
    sim.quiesce_clock_faults();
    sim.quiesce_outbound_faults();
    sim.quiesce_subscriber_stalls();
    // This run's subscriber-stall rate, for the workload's own stall closure
    // (`SimSource::push_stall`); 0.0 once quiesced.
    let _stall_rate: f64 = sim.subscriber_stall();
    // Over every durable byte: the same seed gives the same value, forever.
    let _fingerprint: u64 = sim.durable_fingerprint();
    // Several nodes: a host has its own disk and fault streams; `net` is a
    // host's endpoint on the shared network, cut and healed by name.
    let replica: SimHost = sim.host("replica");
    let _endpoint: SimNet = sim.net("primary");
    sim.partition("primary", "replica");
    sim.repair("primary", "replica");
    // A node built on this host: its own disk, entropy and clock (with its
    // own skew), and its endpoint, `sim.net(replica.name())`.
    let _host: &str = replica.name();
    let _node: (SimDisk, SimEntropy, SimClock, SimNet) = (
        replica.disk(),
        replica.entropy(),
        replica.clock(),
        replica.net(),
    );
    // `Sim`'s crash and power cycle, on this host alone. `crash_now` and
    // `restart` also tear down its network presence.
    replica.crash_after(3);
    replica.crash_now();
    replica.restart();
    let _replica_bytes: u64 = replica.durable_fingerprint();
    // `checks::durable_corruption_fired` and `dir_entry_loss_fired` on this
    // host alone: a refusal on one node is excused only by a fault on that
    // node. The counts only grow; read them at a wipe, and excuse a later
    // refusal only by a fault since.
    let _corrupted: bool = replica.durable_corruption_fired();
    let _lost: bool = replica.dir_entry_loss_fired();
    let _corruptions: u64 = replica.durable_corruption_count();
    let _losses: u64 = replica.dir_entry_loss_count();
    // When the next message in flight can be delivered, for a multi-node
    // scheduler to jump to.
    let _wake: Option<u64> = sim.next_wakeup();
    // One model per host (a second registration panics): see `remotes`.
    sim.remote("pay.example", Recorder::new());
    // A process's outbound handle; `Err` naming a host with no model.
    let _calls: SimOutbound = sim.outbound(&["pay.example"])?;
    // A host's model, after every delivery due by now. Panics when `M` is not
    // the registered type.
    let _seen: usize = sim.with_remote("pay.example", |r: &Recorder| r.calls().len());
    // Every request that reached a remote so far.
    let _delivered: Vec<Delivered> = sim.outbound_deliveries();
    // The archive's bucket (GUIDE §15). `with_faults` draws its put, get,
    // lost-ack and delete faults on streams no other fault moves.
    let _calm: SimObjectStore = SimObjectStore::new();
    let bucket: SimObjectStore = SimObjectStore::with_faults(seed, FaultConfig::default());
    // The next put fails after the store accepted it.
    bucket.fail_next_put();
    let _objects: Vec<String> = bucket.keys();
    Ok(())
}

/// `Input`: the values the property must hold for, drawn from the seed's own
/// stream, so a draw moves no fault. Each draw is named: a failure prints
/// them on its `inputs:` line, shrunk, and `--inputs` replays them.
fn input(sim: &Sim) {
    let input: Input = sim.input();
    // In `lo..=hi`; shrinks toward `lo`. Panics when `lo > hi`.
    let count: u64 = input.between("memos", 1, 6);
    // One of the items; shrinks toward the first. Panics on an empty slice.
    let _author: &&str = input.pick("author", &["ann", "bo"]);
    // In place; shrinks toward the order given.
    let mut order: Vec<u64> = (0..count).collect();
    input.shuffle("order", &mut order);
    // `min..=max` words, space-joined; shrinks toward `min` copies of the
    // first word.
    let _text: String = input.text("text", &["rush", "late", "gift"], 1, 3);
}

/// The outside world behind each declared host: a model `sim.remote(host,
/// model)` registers, run when a call reaches it. Its variance comes from the
/// `Rng` it is handed, and it never calls back into the `Sim`.
fn remotes(sim: &Sim) {
    sim.remote(
        "mail.example",
        from_fn(|_call: &OutboundCall| OutboundReply {
            status: 202,
            headers: Vec::new(),
            body: Vec::new(),
        }),
    );
    // Answers 200 with an empty body and keeps every call, in delivery order.
    let recorder: Recorder = Recorder::new();
    let _seen: &[OutboundCall] = recorder.calls();
    // Idempotency over an inner model, keyed by what the app sent (a header,
    // or `by` a function of the call): a repeated key replays the first
    // reply, a reused key with other parameters answers 409, a call with no
    // key is applied every time.
    let keyed: Idempotent<Recorder> = Idempotent::header("idempotency-key", Recorder::new());
    let _by_body: Idempotent<Recorder> = Idempotent::by(
        |call| Some(String::from_utf8_lossy(&call.body).into_owned()),
        Recorder::new(),
    );
    // The keys that reached the inner model; count applications through
    // `inner()`'s own state.
    let _applied: &BTreeMap<String, u32> = keyed.applied();
    let _inner: &Recorder = keyed.inner();
    sim.remote("pay.example", keyed);

    // A model of your own, read back with `sim.with_remote`.
    struct Ledger {
        posted: u64,
    }
    impl Remote for Ledger {
        // Runs when the request reaches the remote, at its drawn delivery time.
        fn handle(&mut self, _call: &OutboundCall, rng: &mut Rng) -> OutboundReply {
            // The model's own seeded stream: `chance(p)`, `below(n)` in
            // `0..n`, `between(lo, hi)` inclusive, and raw `next_u64`/`fill`.
            let status = if rng.chance(0.1) { 503 } else { 200 };
            let _shard: u64 = rng.below(4);
            let _cents: u64 = rng.between(1, 500);
            let mut receipt = [0u8; 8];
            rng.fill(&mut receipt);
            self.posted += u64::from(status == 200);
            OutboundReply {
                status,
                headers: vec![("receipt-id".into(), rng.next_u64().to_string())],
                body: Vec::new(),
            }
        }
    }
    sim.remote("ledger.example", Ledger { posted: 0 });
}

/// `charpente_sim::checks`: the platform's oracles and its fault-fallout
/// vocabulary. A test on `drive` uses the end-state ones; the boot and clock
/// helpers are what `drive` runs.
fn checks(sim: &mut Sim, snap: &Snapshot, org: Id<Org>, job: &JobRow) -> Result<(), String> {
    // Every durable event of type `E`, as the app's own type, in log order,
    // every page of the log. A row of another shape is an `Err` naming it.
    let _added: Vec<NoteAdded> = events::<NoteAdded>(snap)?;
    // One tenant's, by its raw id; a `global` event does not compile here.
    let _memos: Vec<MemoAdded> = events_scoped::<MemoAdded>(snap, org.raw())?;
    // Excuses an acked row's loss only in a run where durable corruption
    // (`bit_flip`, `misdirected_write`) fired: give the run up, never pass it.
    // On several nodes, ask the node's `SimHost` instead.
    let _excused: bool = durable_corruption_fired(sim);
    // The same, for a directory entry lost or brought back
    // (`dir_entry_loss`): a missing-segment refusal is excused only there.
    let _gap_excused: bool = dir_entry_loss_fired(sim);
    // Outbound: every call carries a durable job's key; no two jobs share a
    // key; and, after a fault-free tail past the last backoff, every
    // outbound job is Done or Dead.
    calls_have_durable_jobs(sim, snap)?;
    keys_are_per_job(snap)?;
    outbound_settles_by_horizon(snap)?;
    // An outbound job's key, rebuilt from its row to find among the calls a
    // remote received.
    if let Some(raw) = job.outbound_key {
        let key: IdempotencyKey = IdempotencyKey::from_bytes(raw);
        let _hex: &str = key.as_str();
        let _bytes: &[u8; 16] = key.as_bytes();
    }
    // An error's text sorted into a fault family. `is_fault_fallout` is true
    // for the families text alone excuses: injected, poisoned, redb's latch.
    let _family: FaultReason = classify("injected read error");
    let _fallout: bool = is_fault_fallout("injected read error");
    // A clock stop or refusal is fallout only where a clock fault fired; an
    // app's filter is `is_fault_fallout(t) || clock_fallout(sim, t)`.
    let stopped = "clock went backwards: this node's wall clock reads \
                   2026-01-01T00:00:39.000Z, 61s behind 2026-01-01T00:01:40.000Z, … and the \
                   node runs again after 2026-01-01T00:00:40.000Z.";
    let _clock: bool = clock_refused(stopped);
    let _clock_fallout: bool = clock_fallout(sim, stopped);
    // The instant a clock refusal names; `None` for other text and for the
    // stall stop.
    let _reopens: Option<WallTime> = clock_reopens_after(stopped);
    // A durable-horizon refusal: `Ok(true)` gave the run up because durable
    // corruption fired, `Err` when nothing excuses it.
    let _ended: bool = durable_refusal(
        sim,
        "log frame 7 is invalid but at-or-below the store's durable horizon 9 — …",
    )?;
    // Up to `tries` boots through the fallout `is_fallout` accepts; `Ok(None)`
    // when every boot met it, or a durable refusal gave the run up.
    let _app: Option<App> = boot_through_fallout(
        sim,
        8,
        |t| is_fault_fallout(t) || clock_fallout(sim, t),
        || sim_app(sim),
    )
    .map_err(|e| e.to_string())?;
    // A tail's boot, after `sim.quiesce_clock_faults()`: waits past the
    // instant a clock refusal names and boots again; a third refusal is an
    // `Err` naming a platform clock defect, never a give-up.
    let _booted: Option<App> = wait_out_clock_refusal(sim, || {
        boot_through_fallout(sim, 8, is_fault_fallout, || sim_app(sim)).map_err(|e| e.to_string())
    })?;
    // `workload` twice, each on a fresh `Sim::new(seed)`: the results and the
    // durable bytes must match.
    replays_identically(
        sim.seed(),
        |fresh: &mut Sim| Ok(fresh.durable_fingerprint()),
    )?;
    // `rounds` of drive, power cycle, verify, on this `Sim`: every id acked
    // so far must still be there. A run that acked nothing gives up.
    acked_implies_durable(
        sim,
        3,
        |_sim: &mut Sim| -> Result<Vec<String>, String> { Ok(Vec::new()) },
        |_sim: &mut Sim, _acked: &[String]| Ok(()),
    )?;
    Ok(())
}

/// `charpente_sim::drive`: a faulted phase, a power cycle, and a fault-free
/// tail that re-sends what went unanswered, in two calls. A check of an app
/// under faults starts here (GUIDE §8, "A fault-free tail for an end-of-run
/// check").
fn drive(sim: &Sim, body: NoteBody) -> Result<(), String> {
    // `sim_app` builds the app on the sim's disk, once per boot.
    let drive: Drive = Drive::new(sim, sim_app);
    // `(offset from the phase's start, request)`, in arrival order.
    let requests: Vec<(Duration, Request)> = vec![(
        Duration::from_millis(1),
        Request::new(Method::Post, "/orgs/1/memos/7")
            .header("authorization", "Bearer <token>")
            // Any `Serialize` value; it sets the body only, no content-type,
            // and panics if the value does not serialize.
            .json(&body),
    )];
    // Boot through fallout, then run the requests under the run's faults
    // until nothing is left inside `horizon`. Fault fallout ends the phase,
    // not the run. `Err`: an error that is not fallout, or a declared host
    // with no `sim.remote`.
    let phase: Phase = drive.faulted_phase(&requests, Duration::from_secs(2))?;
    // `requests[i]`'s status (`phase.answers[i]` holds the body too), `None`
    // when the phase left it unanswered.
    let _acked: Option<u16> = phase.status(0);
    // Each request's answer as `(status, body)`, the body the response's raw
    // bytes (JSON for a route that returns a value).
    let _answers: &Vec<Option<(u16, Vec<u8>)>> = &phase.answers;
    // Power-cycle, quiesce disk and clock faults (outbound too when the app
    // declares hosts), wait out a clock refusal, boot, re-send each request
    // with no answer or a fallout 5xx, and run `tail` past the boot so jobs,
    // retries and cron drain. `Ok(None)`: the run gave up by name, so the
    // check returns `Ok(())`.
    let Some(recovered): Option<Recovered> = drive.recover(&phase, Duration::from_secs(30))? else {
        return Ok(());
    };
    // The tail's answer to a re-sent request (`recovered.resent[i]`), `None`
    // when it was not re-sent. Merging it with the phase's is the check's
    // call: an unanswered request may have landed before the fault, so its
    // re-send can be its second arrival.
    let _status: Option<u16> = recovered.status(0).or(phase.status(0));
    // Each re-send's `(status, body)`, by the index of the request it re-sent.
    let _resent: &Vec<Option<(u16, Vec<u8>)>> = &recovered.resent;
    let snap: Snapshot = recovered
        .kernel
        .app()
        .db()
        .snapshot()
        .map_err(|e| e.to_string())?;
    let _memos: Vec<MemoAdded> = events_scoped::<MemoAdded>(&snap, 1)?;
    Ok(())
}
