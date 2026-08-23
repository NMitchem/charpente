//! The app kernel: sessions inside the commit point (DESIGN §8), and `App` —
//! the owner of the `Db` that turns each request into one transaction.

use std::cell::Cell;
use std::collections::BTreeMap;
use std::io;
use std::sync::Arc;
use std::time::Duration;

use charpente_core::db::{Db, DbConfig, Group, Job, Schema, Tx, TxError};
use charpente_core::io::{Disk, Entropy};
use charpente_core::table::{Id, View};
use charpente_macros::table;

use crate::auth::sweep::{self, SessionSweep};
use crate::auth::{
    ApiKey, AuthReject, OidcConfig, OidcVerifier, Presented, Principal, Via, hash_token, resolve,
};
use crate::describe::EventDef;
use crate::http::router::{RouteGlue, RouteMatch, Router};
use crate::http::{ApiError, Cursor, Request, Response, Route, Subscription, hex};
use crate::kernel::JobDef;
use crate::kernel::JobEntry;
use crate::kernel::cron::{CronEntry, CronRow};

/// Shared by `db_mut` and `begin_group`, which cannot share the *call* — the
/// ruling behind `begin_group`'s field projection forbids it reaching for
/// `db_mut()`. One const so a reword of the message cannot drift between the
/// two sites.
const DB_TAKEN: &str = "db taken for reseed — a kernel bug left it out";

// ---------------------------------------------------------------- session

/// Default session lifetime: 30 days, in nanoseconds — the clock's unit
/// throughout the kernel (`Clock::now().as_nanos()`).
pub const SESSION_TTL: u64 = 30 * 24 * 60 * 60 * 1_000_000_000;

/// A session row inside the commit point (DESIGN §8). The token is stored
/// hashed (M1 "auth, properly") — only `token_hash` ever reaches disk; the
/// raw token lives only in the response that mints it. `expires_at` is a
/// clock reading (nanos); a row past it is treated as absent, never valid.
#[table]
pub struct Session {
    pub id: Id<Session>,
    #[index]
    pub token_hash: String,
    pub subject: String,
    pub expires_at: u64,
}

// ---------------------------------------------------------------- ctx

/// Per-request context handed to handlers: the authenticated principal (if
/// any) and one fresh entropy-derived token for session/key creation. `now`
/// and `session_ttl` are the clock reading `handle_or_subscribe` took for
/// this request and the app's configured TTL — both needed to mint
/// `expires_at`.
pub struct Ctx {
    principal: Option<Principal>,
    /// Compat cache for `session()`: the `Session` row, re-looked-up during
    /// `build_ctx`, only when `principal.via == Via::Session(_)` — `None`
    /// for an `ApiKey`/`Oidc` principal or no principal at all.
    session: Option<Session>,
    fresh_token: String,
    now: u64,
    session_ttl: u64,
    created: Cell<bool>,
}

impl Ctx {
    /// The resolution ladder's output (auth T4) — `None` on a route without
    /// `auth` and no credential presented.
    pub fn principal(&self) -> Option<&Principal> {
        self.principal.as_ref()
    }

    /// Compatibility accessor: the `Session` row, only when the principal
    /// was authenticated `via` a session (bearer or cookie) — `None` for an
    /// `ApiKey`/`Oidc` principal.
    pub fn session(&self) -> Option<&Session> {
        self.session.as_ref()
    }

    /// Creates a session for `subject` and returns its token (login). One
    /// per request — the fresh token is drawn before the transaction, so a
    /// second call could only reuse it. Stores the hash, never the raw
    /// token; `expires_at` is this request's `now` plus the app's TTL.
    pub fn create_session(&self, tx: &mut Tx, subject: impl Into<String>) -> String {
        self.mint_credential();
        let id = u64::from_str_radix(&self.fresh_token[..16], 16).expect("token is hex");
        tx.put(&Session {
            id: Id::from_raw(id),
            token_hash: hash_token(&self.fresh_token),
            subject: subject.into(),
            expires_at: self.now + self.session_ttl,
        });
        self.fresh_token.clone()
    }

    /// Mints an API key for `subject` and returns the raw key (`"ck_"` plus
    /// 64 hex chars — the same pre-transaction entropy draw `create_session`
    /// uses, per request). One per request, sharing `create_session`'s mint
    /// budget. Stores `hash_token` of the full raw key (prefix included),
    /// never the raw key itself.
    pub fn create_api_key(&self, tx: &mut Tx, subject: impl Into<String>) -> String {
        self.mint_credential();
        let id = u64::from_str_radix(&self.fresh_token[..16], 16).expect("token is hex");
        let raw = format!("ck_{}", self.fresh_token);
        tx.put(&ApiKey {
            id: Id::from_raw(id),
            key_hash: hash_token(&raw),
            subject: subject.into(),
            created_at: self.now,
        });
        raw
    }

    /// Deletes the current session (logout). A no-op without one.
    pub fn revoke(&self, tx: &mut Tx) {
        if let Some(session) = &self.session {
            tx.delete::<Session>(session.id);
        }
    }

    /// The shared one-mint-per-request gate: `create_session` and
    /// `create_api_key` both draw from the single pre-transaction
    /// `fresh_token` entropy draw, so a request minting a second credential
    /// (of either kind) would silently reuse it — refused instead.
    fn mint_credential(&self) {
        assert!(
            !self.created.replace(true),
            "Ctx::create_session/create_api_key called twice in one request — one credential \
             mint per request"
        );
    }
}

// ---------------------------------------------------------------- app

pub struct AppConfig {
    /// Bodies over the cap get 413 before any decoding — this also fences
    /// `Db::transact`'s MAX_RECORD_BYTES assert from network input.
    pub body_cap: usize,
    pub db: DbConfig,
    /// How long a minted session stays valid, in nanoseconds — added to the
    /// minting request's `now` to set `Session::expires_at`.
    pub session_ttl: u64,
}

impl Default for AppConfig {
    fn default() -> AppConfig {
        AppConfig {
            body_cap: 1 << 20,
            db: DbConfig::default(),
            session_ttl: SESSION_TTL,
        }
    }
}

/// The deterministic application kernel. Owns the `Db`; `handle` takes
/// `&mut self`, so requests are serializable by construction. The sim drives
/// `handle` directly; `crate::serve` feeds it from hyper in production.
pub struct App {
    /// `Option` only so the replication reseed swap can `take` the `Db`, hand
    /// it to `Repl::run_seed` (which consumes and returns a rebuilt one), and
    /// `put` it back — the slot is `Some` at every other observable moment
    /// (`db()`/`db_mut()` expect it).
    db: Option<Db>,
    entropy: Box<dyn Entropy>,
    config: AppConfig,
    router: Router,
    job_defs: Vec<JobDef>,
    /// Describe-only (TS-codegen spec §2). Delivery reads nothing here: an
    /// event reaches a subscriber through the encoder `Subscription::of::
    /// <Ev>()` captured statically at the subscribe site, registered or not.
    event_defs: Vec<EventDef>,
    cron_entries: Vec<CronEntry>,
    /// `None` until `App::with_oidc` mounts a verifier. The `Arc` is now
    /// incidental, not load-bearing: it existed so `handle_or_subscribe`
    /// could clone an owned handle before `db_mut()` took `&mut self`, and
    /// group-commit T5's borrow-split removed that need — `begin_group`
    /// hands `StagedDispatch` a plain `&OidcVerifier` via `as_deref()`, and
    /// nothing in the crate clones this field any more. A `Box` would do.
    oidc: Option<Arc<OidcVerifier>>,
}

impl App {
    /// Opens the Db with the app's schema plus the platform's `Session`,
    /// `ApiKey`, and `CronRow` tables — registered here so recovery can
    /// always replay logins, minted keys, and schedules. The built-in daily
    /// session-expiry sweep (auth T6) is seeded the same unconditional way:
    /// its `#[job]` def and cron entry land in `job_defs`/`cron_entries`
    /// before any app-level `mount_jobs`/`schedule` call, so every app gets
    /// it with no opt-in.
    pub fn open(
        disk: Box<dyn Disk>,
        schema: Schema,
        entropy: Box<dyn Entropy>,
        config: AppConfig,
    ) -> io::Result<App> {
        let db = Db::open(
            disk,
            config.db.clone(),
            schema
                .table::<Session>()
                .table::<ApiKey>()
                .table::<CronRow>(),
        )?;
        Ok(App {
            db: Some(db),
            entropy,
            config,
            router: Router::new(),
            job_defs: vec![<SessionSweep as JobEntry>::def()],
            // No platform events to seed: the built-in surface emits none.
            event_defs: Vec::new(),
            cron_entries: vec![sweep::platform_cron_entry()],
            oidc: None,
        })
    }

    pub fn mount(mut self, routes: Vec<Route>) -> App {
        self.router.mount(routes);
        self
    }

    /// Mounts the OIDC static-JWKS verifier (spec §3): `config.jwks_json` is
    /// parsed once, here, at mount time — a bad key, unsupported `alg`, or
    /// malformed document panics with the named `OidcConfigError` text
    /// (never a request-time error), mirroring `mount`/`mount_jobs`/
    /// `schedule`'s mount-time conflict panics elsewhere in this file.
    pub fn with_oidc(mut self, config: OidcConfig) -> App {
        let verifier = OidcVerifier::from_config(config).unwrap_or_else(|e| panic!("{e}"));
        self.oidc = Some(Arc::new(verifier));
        self
    }

    /// Registers `#[job]` descriptors (mount-time duplicate-name panic — each
    /// job type is mounted once, like routes).
    pub fn mount_jobs(mut self, defs: Vec<JobDef>) -> App {
        for def in defs {
            assert!(
                !self.job_defs.iter().any(|d| d.name == def.name),
                "job \"{}\" registered twice — each #[job] type is mounted once",
                def.name
            );
            self.job_defs.push(def);
        }
        self
    }

    /// Registers `#[event]` descriptors for `describe` (mount-time
    /// duplicate-name panic — each event type is mounted once, like jobs).
    ///
    /// This registry is describe-only, and mounting is therefore optional:
    /// an unregistered `#[event]` still emits and still delivers, it just
    /// does not appear in `Contract.events` (nor its payload in
    /// `Contract.types`), so a generated client cannot see it.
    pub fn mount_events(mut self, defs: Vec<EventDef>) -> App {
        for def in defs {
            assert!(
                !self.event_defs.iter().any(|d| d.name == def.name),
                "event \"{}\" registered twice — each #[event] type is mounted once",
                def.name
            );
            // Negative space: an events entry that names no type would leave
            // a describe consumer with a payload it cannot resolve.
            // `#[event]` always supplies a name (its `Wire` derive sets
            // `WIRE_NAME` to the struct's name), so reaching this means a
            // hand-written `EventEntry` impl.
            assert!(
                !def.payload.is_empty(),
                "event \"{}\" registers an empty payload name — every Contract.events \
                 entry must name a type in Contract.types, and \"\" is `Wire`'s \
                 \"anonymous/inline\" marker; make the payload a #[derive(Wire)] type \
                 and pass its WIRE_NAME",
                def.name
            );
            self.event_defs.push(def);
        }
        self
    }

    /// Registers a cron schedule (mount-time duplicate-name panic — each
    /// name is scheduled once). `make` builds the job enqueued on each tick.
    pub fn schedule<J: Job + 'static>(
        mut self,
        name: &'static str,
        every: Duration,
        make: impl Fn() -> J + Send + 'static,
    ) -> App {
        assert!(
            !self.cron_entries.iter().any(|e| e.name == name),
            "cron schedule \"{name}\" scheduled twice — schedule each name once"
        );
        self.cron_entries.push(CronEntry {
            name,
            interval: every,
            // `now` rides the widened seam (T6) but plain user schedules
            // don't need it — `make` stays the simple `() -> J` shape.
            enqueue: Box::new(move |tx, _now| tx.enqueue(make())),
        });
        self
    }

    /// Snapshot/checkpoint access for the kernel loop and tests — handlers
    /// never see it.
    pub fn db(&self) -> &Db {
        self.db
            .as_ref()
            .expect("db taken for reseed — a kernel bug left it out")
    }

    pub fn config(&self) -> &AppConfig {
        &self.config
    }

    /// Describe-only reach: the mounted router, for `describe::Contract`'s
    /// route walk. Never used by request handling itself.
    pub(crate) fn router(&self) -> &Router {
        &self.router
    }

    /// Describe-only reach: the registered job descriptors, in registration
    /// order. Never used by request handling itself.
    pub(crate) fn job_defs(&self) -> &[JobDef] {
        &self.job_defs
    }

    /// Describe-only reach: the registered event descriptors, in
    /// registration order (`describe` sorts them by name). Never used by
    /// request handling or by delivery — see the field's own comment.
    pub(crate) fn event_defs(&self) -> &[EventDef] {
        &self.event_defs
    }

    /// Describe-only reach: the registered cron schedules, in registration
    /// order. Never used by request handling itself.
    pub(crate) fn cron_entries(&self) -> &[CronEntry] {
        &self.cron_entries
    }

    /// Describe-only reach: whether `App::with_oidc` mounted a verifier.
    /// Never used by request handling itself (that reaches `self.oidc`
    /// through `begin_group`'s `as_deref`).
    pub(crate) fn oidc_mounted(&self) -> bool {
        self.oidc.is_some()
    }

    pub(crate) fn db_mut(&mut self) -> &mut Db {
        self.db.as_mut().expect(DB_TAKEN)
    }

    /// Opens a group over this app's `Db` (group-commit T5, spec §1) plus the
    /// shared half of the dispatch path — everything `stage_or_subscribe`
    /// needs besides the `Db` itself. Returns both as disjoint sub-borrows so
    /// a caller can stage many requests into one `Group` before committing.
    ///
    /// The returned `Group` MUST be committed: dropping a non-empty one
    /// panics by contract (`Group`'s `Drop`). `Err` here means the store
    /// snapshot the group reads through could not be opened.
    ///
    /// Field projections, NOT method calls: `self.db.as_mut()` and the
    /// shared field borrows split cleanly here, where `self.db_mut()`
    /// would re-borrow all of `*self` and poison the split. See the T5
    /// pre-flight's finding G for the failure this shape exists to avoid.
    pub fn begin_group(&mut self) -> io::Result<(Group<'_>, StagedDispatch<'_>)> {
        let staged = StagedDispatch {
            router: &self.router,
            entropy: &*self.entropy,
            config: &self.config,
            oidc: self.oidc.as_deref(),
        };
        // Not `self.db_mut()` — see the doc above.
        let group = self.db.as_mut().expect(DB_TAKEN).begin_group()?;
        Ok((group, staged))
    }

    /// Lifts the `Db` out for the replication reseed swap (the kernel's
    /// `Ready::Repl` arm): `Repl::run_seed` consumes it and returns a rebuilt
    /// one that `put_db` restores. Between the two calls `db()`/`db_mut()`
    /// would panic — the arm holds them adjacent, so no other code runs.
    pub(crate) fn take_db(&mut self) -> Db {
        self.db
            .take()
            .expect("db taken twice — the reseed swap is not re-entrant")
    }

    pub(crate) fn put_db(&mut self, db: Db) {
        self.db = Some(db);
    }

    pub(crate) fn entropy(&self) -> &dyn Entropy {
        &*self.entropy
    }

    pub(crate) fn take_job_defs(&mut self) -> Vec<JobDef> {
        std::mem::take(&mut self.job_defs)
    }

    /// Test-only equivalent of `take_job_defs` — integration tests cannot see
    /// `pub(crate)`; the kernel itself uses `take_job_defs`.
    #[doc(hidden)]
    pub fn take_job_defs_for_tests(&mut self) -> Vec<JobDef> {
        self.take_job_defs()
    }

    pub(crate) fn take_cron_entries(&mut self) -> Vec<CronEntry> {
        std::mem::take(&mut self.cron_entries)
    }

    /// Test-only equivalent of `take_cron_entries` — integration tests cannot
    /// see `pub(crate)`; the kernel itself uses `take_cron_entries`.
    #[doc(hidden)]
    pub fn take_cron_entries_for_tests(&mut self) -> Vec<CronEntry> {
        self.take_cron_entries()
    }

    /// Test-only equivalent of `db_mut` — integration tests cannot see
    /// `pub(crate)`; the kernel itself uses `db_mut`.
    #[doc(hidden)]
    pub fn db_for_tests(&mut self) -> &mut Db {
        self.db_mut()
    }

    /// Test-only equivalents of `take_db`/`put_db` — integration tests cannot
    /// see `pub(crate)`; the kernel itself uses the unsuffixed pair.
    #[doc(hidden)]
    pub fn take_db_for_tests(&mut self) -> Db {
        self.take_db()
    }

    #[doc(hidden)]
    pub fn put_db_for_tests(&mut self, db: Db) {
        self.put_db(db);
    }

    /// The plain surface: routes to a `Response` (`handle_or_subscribe`'s
    /// `Response` outcome), and turns a `Subscribe` outcome into a 500 — an
    /// SSE route matched, but a bare caller has no sink to hand the tail.
    /// Only the kernel (`Kernel::step`, T5) has one, so only it should ever
    /// see a `Handled::Subscribe`. Defaults `now` to 0 — every current
    /// caller relies on this implicitly for sessions that outlive a single
    /// scenario; a test that must observe the expiry boundary calls
    /// `handle_at` directly instead.
    pub fn handle(&mut self, request: Request) -> Response {
        self.handle_at(request, 0)
    }

    /// `handle`, with the clock reading `handle_or_subscribe` needs made
    /// explicit — the seam a caller that tracks its own `now` (tests driving
    /// session expiry) uses instead of the 0-default.
    pub fn handle_at(&mut self, request: Request, now: u64) -> Response {
        match self.handle_or_subscribe(request, now) {
            Handled::Response(response) => response,
            Handled::Subscribe(_) => Response::error(
                500,
                "an SSE route matched App::handle, which has no sink to hand the tail — \
                 subscribe requests must go through Kernel::run",
            ),
        }
    }

    /// The one-request wrapper (group-commit T5, spec §1): a single-request
    /// group, staged then committed immediately — exactly `Db::transact`'s
    /// "batch of one" reasoning, one level up. This is the whole dispatch
    /// path most callers use; a kernel drain loop (T6) instead calls
    /// `begin_group`/`stage_or_subscribe` directly to stage many requests
    /// under one `commit`.
    ///
    /// One behaviour change this task does NOT preserve, recorded because
    /// every other path here is byte-identical: opening the group takes the
    /// store snapshot BEFORE routing, where the old code snapshotted only
    /// after a route matched. A request that would have returned 404/405/413
    /// or a cursor-400 therefore returns a 500 instead if `Store::snapshot`
    /// itself fails. This ordering is inherent to group commit rather than
    /// incidental — under T6 the group opens once at the top of a batch, so
    /// every batched request's snapshot necessarily precedes its own routing
    /// — so it is named here rather than worked around.
    pub fn handle_or_subscribe(&mut self, request: Request, now: u64) -> Handled {
        let (mut group, staged) = match self.begin_group() {
            Ok(pair) => pair,
            Err(e) => return Handled::Response(tx_error_response(TxError::Io(e))),
        };
        let handled = staged.stage_or_subscribe(&mut group, request, now);
        let wrote = !group.is_empty();
        match group.commit() {
            Ok(()) => handled,
            // Routed through the SAME rule as `Db::transact` and the kernel's
            // batched tail (T6 review I-2): this path used to map any
            // `CommitFailure` to a 500 regardless of `applied`, which made it
            // the divergent one the moment the other two started honouring it
            // — a checkpoint failure the kernel answers 200 for was answered
            // 500 here. `records_before` is 0: a batch of one sees nothing
            // staged ahead of it.
            Err(failure) if failure.invalidates(0, wrote) => {
                Handled::Response(tx_error_response(TxError::Io(failure.error)))
            }
            Err(_) => handled,
        }
    }
}

/// Everything `stage_or_subscribe` needs besides the `Db` — the shared half
/// of `App::begin_group`'s borrow-split (group-commit T5, Noah ruling). Every
/// field here is reached through a `&self` borrow (`Router::find`,
/// `Entropy::fill`) or is `Copy` (`Option<&OidcVerifier>`), so it can coexist
/// with the `Group`'s mutable loan on `App::db`.
///
/// Named `StagedDispatch`, NOT `Dispatcher` — `charpente::kernel::Dispatcher`
/// (the job dispatcher, `kernel/dispatch.rs`) already owns that name and is
/// reachable from the same file T6's drain loop lives in; a second public
/// `Dispatcher` would collide (E0252) or, where it technically compiled,
/// leave a `dispatcher` binding beside `Kernel::dispatcher` meaning something
/// entirely different.
pub struct StagedDispatch<'a> {
    router: &'a Router,
    entropy: &'a dyn Entropy,
    config: &'a AppConfig,
    oidc: Option<&'a OidcVerifier>,
}

impl<'a> StagedDispatch<'a> {
    /// Routes, then dispatches by the matched route's flavor, staging the
    /// handler's effects into `group` instead of committing them — the
    /// factoring behind both `handle_or_subscribe`'s one-request wrapper and
    /// a batched drain loop (T6). A plain `#[route]` stages its handler and
    /// returns `Response`; a `#[route(SSE …)]` extracts the resume cursor
    /// (before the handler runs — an unparseable one is a 400, never a
    /// silent from-now), stages its handler, and returns the validated
    /// `Subscription`. `now` is the caller's clock reading (the kernel's own
    /// `Clock`, in production) — threaded into `Ctx` for session expiry and
    /// minting.
    ///
    /// Both `RouteGlue` arms route through `group.stage(...)` identically
    /// (T5 pre-flight finding F): an SSE glue closure may legitimately write
    /// (`ctx.create_session` is the obvious case), and `Group::stage`'s
    /// empty-effects check — not a special case here — is what makes a
    /// typical read-only subscribe cost no record.
    pub fn stage_or_subscribe(&self, group: &mut Group, request: Request, now: u64) -> Handled {
        let (route, captures) = match self.router.find(request.method, &request.path) {
            RouteMatch::Found(route, captures) => (route, captures),
            RouteMatch::PathNotFound => {
                return Handled::Response(Response::error(404, "not found"));
            }
            RouteMatch::MethodNotAllowed => {
                return Handled::Response(Response::error(405, "method not allowed for this path"));
            }
        };
        if request.body.len() > self.config.body_cap {
            return Handled::Response(Response::error(
                413,
                &format!(
                    "request body is {} bytes but the cap is {} — raise AppConfig.body_cap \
                     or send less",
                    request.body.len(),
                    self.config.body_cap
                ),
            ));
        }
        let auth_required = route.auth;
        let glue = route.glue;
        let cursor = match glue {
            RouteGlue::Sse(_) => match extract_cursor(&request) {
                Ok(cursor) => cursor,
                Err(response) => return Handled::Response(response),
            },
            RouteGlue::Http(_) => None,
        };

        let mut fresh = [0u8; 32];
        self.entropy.fill(&mut fresh);
        let fresh_token = hex(&fresh);
        let presented = bearer_or_cookie(&request.headers);
        let session_ttl = self.config.session_ttl;
        let oidc = self.oidc;

        match glue {
            RouteGlue::Http(glue) => {
                let result = group.stage(|tx| {
                    let ctx = build_ctx(
                        tx,
                        &presented,
                        auth_required,
                        fresh_token,
                        now,
                        session_ttl,
                        oidc,
                    )?;
                    glue(tx, &ctx, &captures, &request.body)
                });
                Handled::Response(result.unwrap_or_else(tx_error_response))
            }
            RouteGlue::Sse(glue) => {
                let result = group.stage(|tx| {
                    let ctx = build_ctx(
                        tx,
                        &presented,
                        auth_required,
                        fresh_token,
                        now,
                        session_ttl,
                        oidc,
                    )?;
                    glue(tx, &ctx, &captures, cursor)
                });
                match result {
                    Ok(subscription) => Handled::Subscribe(subscription),
                    Err(e) => Handled::Response(tx_error_response(e)),
                }
            }
        }
    }
}

/// What matching a route and running its handler produced. `Response` ends
/// the exchange; `Subscribe` is an SSE route's validated subscription —
/// meaningful only to a caller with a sink to hand the tail (the kernel),
/// which is why `App::handle` maps it to a 500 instead of returning it.
pub enum Handled {
    Response(Response),
    Subscribe(Subscription),
}

/// Runs the resolution ladder (`auth::resolve`, spec §2) on the bearer/
/// cookie token, maps a rejected credential to its named 401, gates
/// `auth`-required routes on a missing principal (the no-credential 401 —
/// `resolve` itself never produces this: it returns `Ok(None)` for "nothing
/// presented"), and builds the `Ctx` the handler sees. `oidc` is the mounted
/// verifier (spec §3) — `None` until `App::with_oidc` runs.
fn build_ctx(
    tx: &mut Tx,
    presented: &Presented,
    auth_required: bool,
    fresh_token: String,
    now: u64,
    session_ttl: u64,
    oidc: Option<&OidcVerifier>,
) -> Result<Ctx, ApiError> {
    let principal = resolve(tx, presented, oidc, now).map_err(|reject| match reject {
        AuthReject::Unrecognized => ApiError::UnrecognizedCredential,
        AuthReject::Expired => ApiError::SessionExpired,
        AuthReject::Oidc(reject) => ApiError::Oidc(reject),
        AuthReject::Io(e) => ApiError::from(e),
    })?;
    if auth_required && principal.is_none() {
        return Err(ApiError::Unauthorized);
    }
    let session = match &principal {
        Some(Principal {
            via: Via::Session(id),
            ..
        }) => tx.get(*id)?,
        _ => None,
    };
    Ok(Ctx {
        principal,
        session,
        fresh_token,
        now,
        session_ttl,
        created: Cell::new(false),
    })
}

/// `TxError<ApiError>` -> `Response`, shared by both route flavors: `App`
/// names the handler's own rejection; `Io`/`NotPrimary`/`Verifying` name the
/// platform failure and the fix (hard rule 5).
/// The 500 a request gets when the platform cannot tell it whether its write
/// committed. One function, two callers: this path and the kernel's batched
/// commit-failure tail (group-commit T6), which must produce byte-identical
/// bytes — the wording is product surface (DESIGN §7) and is pinned by test.
/// A batched request's 500 means "unknown", never "rejected": per spec §4 its
/// record may still be durable and apply on restart.
pub(crate) fn io_failure_response(e: &io::Error) -> Response {
    Response::error(
        500,
        &format!(
            "internal storage failure — the request may or may not have \
             committed; retry after the server recovers: {e}"
        ),
    )
}

fn tx_error_response(err: TxError<ApiError>) -> Response {
    match err {
        TxError::App(e) => e.response(),
        TxError::Io(e) => io_failure_response(&e),
        TxError::NotPrimary { primary } => Response::error(
            421,
            &format!(
                "this node is a read-only follower — writes go to the primary at \
                 {primary}; retry there (the request was not committed)"
            ),
        ),
        TxError::Verifying { unreached } => Response::error(503, &verifying_message(&unreached)),
    }
}

/// The witness-verification 503 body (M2 sub-project 0b): this node cannot
/// name a primary, unlike `NotPrimary`, because it does not know whether one
/// exists elsewhere — it may still be the legitimate one. Pluralises
/// "follower"/"node"/"it" when more than one witness is still unreached
/// (hard rule 5: all three renderings are pinned byte-exact, `http.rs`).
/// The list itself is bounded (`render_witness_list`, task 5's O-6): the
/// witness set grows without bound under a peer that reconnects from
/// changing ephemeral ports, and every address in it lands in this body.
fn verifying_message(unreached: &[String]) -> String {
    assert!(
        !unreached.is_empty(),
        "verifying_message called with an empty witness list — Db::verify_writes already \
         refuses to construct TxError::Verifying with one, so this is a caller bug, not a \
         renderable state"
    );
    let list = crate::repl::witnesses::render_witness_list(unreached);
    if unreached.len() > 1 {
        format!(
            "this node is not accepting writes yet: it was a primary before restarting and has \
             not confirmed that its followers {list} were never promoted — reads are being \
             served normally, and writes resume automatically once those nodes answer; if they \
             are gone for good, see GUIDE §10's witness-set remedy"
        )
    } else {
        format!(
            "this node is not accepting writes yet: it was a primary before restarting and has \
             not confirmed that its follower {list} was never promoted — reads are being \
             served normally, and writes resume automatically once that node answers; if it is \
             gone for good, see GUIDE §10's witness-set remedy"
        )
    }
}

/// The client cursor, extracted before the handler runs (spec §2):
/// `Last-Event-ID` wins (browser-native SSE resume), else the `cursor`
/// query parameter, else `None` ("from now"). A present-but-unparseable
/// value is the named 400 — never a silent from-now.
fn extract_cursor(request: &Request) -> Result<Option<Cursor>, Response> {
    let raw = request
        .headers
        .get("last-event-id")
        .or_else(|| request.query.get("cursor"));
    let Some(raw) = raw else {
        return Ok(None);
    };
    Cursor::parse(raw).map(Some).ok_or_else(|| {
        Response::error(
            400,
            &format!("cursor must be \"<id>.<seq>\" — got \"{raw}\"; omit it to tail from now"),
        )
    })
}

/// `Authorization: Bearer <t>` wins; else the `charpente_session` cookie.
/// Tagged with its source (`Presented`) so the ladder's two-dot JWT-shape
/// check (spec §2 rung 1) applies to a bearer only — a cookie value always
/// goes down the hash path, dots or not.
fn bearer_or_cookie(headers: &BTreeMap<String, String>) -> Presented {
    if let Some(auth) = headers.get("authorization")
        && let Some(token) = auth.strip_prefix("Bearer ")
    {
        return Presented::Bearer(token.trim().to_string());
    }
    let Some(cookie_header) = headers.get("cookie") else {
        return Presented::None;
    };
    for pair in cookie_header.split(';') {
        if let Some((name, value)) = pair.trim().split_once('=')
            && name == "charpente_session"
        {
            return Presented::Cookie(value.to_string());
        }
    }
    Presented::None
}
