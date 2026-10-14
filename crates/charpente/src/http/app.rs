//! The app kernel: sessions inside the commit point (DESIGN §8), and `App` —
//! the owner of the `Db` that turns each request into one transaction.

use std::any::TypeId;
use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::sync::Arc;
use std::time::Duration;

use charpente_core::db::{
    CrossTenant, Db, DbConfig, Group, JobShapeRefusal, Schema, TableWriteRefusal, Tenant,
    TenantAdmission, Tx, TxError,
};
use charpente_core::io::{Disk, Entropy};
use charpente_core::table::{Id, Unscoped, UnscopedJob, View};
use charpente_macros::table;

use crate::auth::sweep::{self, SessionSweep};
use crate::auth::{
    ApiKey, AuthReject, OidcConfig, OidcVerifier, Presented, Principal, Via, hash_token, resolve,
};
use crate::describe::EventDef;
use crate::http::router::{PublicScope, RouteGlue, RouteMatch, Router};
use crate::http::{ApiError, Cursor, Request, Response, Route, Subscription, hex};
use crate::kernel::JobDef;
use crate::kernel::JobEntry;
use crate::kernel::cron::{CronEntry, CronRow};
use crate::outbound::{DEFAULT_MAX_IN_FLIGHT, Host};

/// Shared by `db_mut` and `begin_group`, which cannot share the *call* — the
/// ruling behind `begin_group`'s field projection forbids it reaching for
/// `db_mut()`. One const so a reword of the message cannot drift between the
/// two sites.
const DB_TAKEN: &str = "db taken for reseed — a kernel bug left it out";

/// What `App::subscription_event_names` feeds each `:param` of the stream
/// route it probes. `"0"` parses for every `FromParam` the platform ships
/// (`u64`, `String`, `Id<T>`); a hand-written one that rejects it turns the
/// probe into a named `Err`, not a silently empty answer.
const PROBE_CAPTURE: &str = "0";

/// The `Ctx` fresh-token slot for the same probe: a fixed literal rather
/// than an `Entropy` draw, so probing a route cannot shift the app's entropy
/// stream and change every value drawn after it. 64 hex chars, the shape
/// `Ctx::create_session` slices its first 16 of.
const PROBE_TOKEN: &str = "0000000000000000000000000000000000000000000000000000000000000000";

/// Negative space, at compile time (hard rule 4): `Ctx::create_session` and
/// `create_api_key` slice `fresh_token[..16]`, so a shortened `PROBE_TOKEN`
/// would turn a probe of a session-minting subscribe handler into a slice
/// panic — reachable only from that one handler shape, i.e. exactly the bug
/// a test run would not find.
const _: () = assert!(PROBE_TOKEN.len() >= 16);

// ---------------------------------------------------------------- session

/// Default session lifetime: 30 days, in nanoseconds — the clock's unit
/// throughout the kernel (`Clock::now().as_nanos()`).
pub const SESSION_TTL: u64 = 30 * 24 * 60 * 60 * 1_000_000_000;

/// A session row inside the commit point (DESIGN §8). The token is stored
/// hashed (M1 "auth, properly") — only `token_hash` ever reaches disk; the
/// raw token lives only in the response that mints it. `expires_at` is a
/// clock reading (nanos); a row past it is treated as absent, never valid.
#[table(global)]
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
///
/// `'g` is the request's group: `Ctx` holds the tokens minted from it (the
/// `CrossTenant` of a `cross_tenant` route, the `TenantAdmission` of a
/// `public(S)` route), so neither it nor anything borrowed out of it
/// outlives that group (tenancy-completion spec §5). Handlers write
/// `ctx: &Ctx`; the lifetime is elided.
pub struct Ctx<'g> {
    principal: Option<Principal>,
    /// Compat cache for `session()`: the `Session` row, re-looked-up during
    /// `build_ctx`, only when `principal.via == Via::Session(_)` — `None`
    /// for an `ApiKey`/`Oidc` principal or no principal at all.
    session: Option<Session>,
    fresh_token: String,
    now: u64,
    session_ttl: u64,
    created: Cell<bool>,
    /// `Some` exactly when the matched route declared `cross_tenant` (spec
    /// §3) — the token `Tx::across_tenants` requires. `None` on every other
    /// route, which is why `cap()` is an `Option`: an undeclared route
    /// carries no capability, not a dormant one.
    cap: Option<CrossTenant<'g>>,
    /// The matched route's stance (job-tenancy spec §4): the admission a
    /// `public(S)` route carries, and the site and declaration rule 3's 401
    /// quotes. Private, and never exposed — a handler cannot reach the
    /// admission, only `Scoped::scope` can.
    stance: Stance<'g>,
}

impl<'g> Ctx<'g> {
    /// The resolution ladder's output (auth T4) — `None` on a route without
    /// `auth` and no credential presented.
    pub fn principal(&self) -> Option<&Principal> {
        self.principal.as_ref()
    }

    /// The cross-tenant capability this request carries, or `None` when the
    /// matched route did not declare `cross_tenant` (spec §3). A handler
    /// that needs it takes `cap: &CrossTenant` after its path/body
    /// arguments, which `#[route]` fills from here — and refuses to compile
    /// on a route that never declared the capability, so this accessor is a
    /// reflective read ("did this request come in declared?"), not the
    /// route to the token.
    pub fn cap(&self) -> Option<&CrossTenant<'g>> {
        self.cap.as_ref()
    }

    /// Rule 1 of `Scoped::scope` (job-tenancy spec §4): the admission this
    /// request carries when its route declared `public(S)` for exactly this
    /// `S`. `None` otherwise, including on a `public(Org)` route that opens
    /// some other scope type, which then meets rules 2 and 3 like any route.
    pub(crate) fn public_admission<S: 'static>(&self) -> Option<&TenantAdmission<'g>> {
        match &self.stance.public {
            Some((declared, admission)) if (declared.type_id)() == TypeId::of::<S>() => {
                Some(admission)
            }
            _ => None,
        }
    }

    /// Rule 3's refusal: a caller with no credential reached a scope line
    /// that its route did not declare `public(S)` for. Names the contract,
    /// the site, and both fixes, the caller's and the author's (hard rule 5)
    /// — except on an `auth` route, where the author has no fix to make. The
    /// author's fix names the change, `public(S)` in place of the route's
    /// stance, and restates no attribute: a restated one dropped whatever
    /// else the route declared (`cross_tenant`, `emits(…)`, any later flag),
    /// so a pasted fix either stopped compiling or silently lost a
    /// declaration (tenancy-completion spec §7, item 12).
    pub(crate) fn anonymous_scope<S: Tenant>(&self) -> ApiError {
        let Stance {
            method,
            pattern,
            auth,
            public,
        } = &self.stance;
        // The tenant type's identity (tenancy-completion spec §4). A final
        // `::` component read two types spelled alike as one, and a generic as
        // `B>`. `type_name` is printed beside it for the reader only, to say
        // which type the attribute names (final review M7): a `NAME` need not
        // be any path's spelling, and it is never compared. It is not offered
        // as a path to paste (final re-review N1): it leads with the defining
        // crate's name, which does not resolve inside that crate (E0433).
        let scope = S::NAME;
        let scope_type = std::any::type_name::<S>();
        if *auth {
            // `build_ctx` refuses a request to an `auth` route that carries no
            // credential before its handler runs, so only a caller that does
            // not enforce `auth` (the emits(...) drift probe) gets here.
            // Suggesting `public(S)` would tell the author to open the route
            // to every caller so that a test can read it.
            return ApiError::AnonymousScope(format!(
                "authentication required — `{method} {pattern}` is declared `auth` and opened \
                 the tenant scope `{scope}` (the tenant type `{scope_type}`) for a caller with \
                 no credential; the route requires a credential, so a request without one is \
                 refused before its handler runs, and a caller that presents none, as the \
                 emits(...) drift probe does, cannot run it — leave this route out of the \
                 drift test"
            ));
        }
        let (declared, types) = match public {
            Some((declared, _)) => (
                format!("public({})", declared.name),
                format!(
                    "`{scope}` is the tenant type `{scope_type}`, and `{}` is `{}`",
                    declared.name,
                    (declared.type_name)()
                ),
            ),
            None => (
                "public".to_string(),
                format!("`{scope}` is the tenant type `{scope_type}`"),
            ),
        };
        ApiError::AnonymousScope(format!(
            "authentication required — `{method} {pattern}` is declared `{declared}` and \
             opened the tenant scope `{scope}` for a caller with no credential; only a route \
             declared `public({scope})` admits one. Send \"Authorization: Bearer <token>\" or \
             the charpente_session cookie, or, if every caller may act inside this tenant \
             here, declare `public({scope})` in place of `{declared}` in this route's \
             `#[route(…)]` attribute, keeping its other flags. A tenant type is named here by \
             its NAME; name the type in the attribute: {types}"
        ))
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

/// A matched route's declaration, copied out of the router: the half of a
/// `Stance` that needs no `Group` (job-tenancy amendment A3). It is split
/// from `Stance` so the drift probe can read it before `App::begin_group`
/// borrows the whole `App`, the reason the probe already reads
/// `cross_tenant` first.
#[derive(Clone, Copy)]
struct Declared {
    /// `SSE` for a stream route, else the HTTP method: the word the route was
    /// declared with, which the 401's site and its fix both quote.
    method: &'static str,
    pattern: &'static str,
    auth: bool,
    public_scope: Option<PublicScope>,
}

impl Declared {
    fn of(route: &Route) -> Declared {
        Declared {
            method: match route.glue {
                RouteGlue::Sse(_) => "SSE",
                RouteGlue::Http(_) => route.method.name(),
            },
            pattern: route.pattern,
            auth: route.auth,
            public_scope: route.public_scope,
        }
    }

    /// Mints the admission for a `public(S)` route, and only for one.
    fn admitted<'g>(self, group: &Group<'g>) -> Stance<'g> {
        Stance {
            method: self.method,
            pattern: self.pattern,
            auth: self.auth,
            public: self
                .public_scope
                .map(|scope| (scope, group.tenant_admission())),
        }
    }
}

/// What `Scoped::scope` reads off the route to apply its three rules.
struct Stance<'g> {
    method: &'static str,
    pattern: &'static str,
    /// Recorded for the 401's text only (amendment A11): the drift probe
    /// does not enforce `auth`, so an `auth` stream route that opens a scope
    /// meets rule 3 there, and its text must name the real stance.
    auth: bool,
    public: Option<(PublicScope, TenantAdmission<'g>)>,
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
    hosts: Vec<Host>,
    max_in_flight: usize,
    /// `None` until `App::with_oidc` mounts a verifier. The `Arc` is now
    /// incidental, not load-bearing: it existed so `handle_or_subscribe`
    /// could clone an owned handle before `db_mut()` took `&mut self`, and
    /// group-commit T5's borrow-split removed that need — `begin_group`
    /// hands `StagedDispatch` a plain `&OidcVerifier` via `as_deref()`, and
    /// nothing in the crate clones this field any more. A `Box` would do.
    oidc: Option<Arc<OidcVerifier>>,
    /// Every tenant type this app names, by `NAME`: its `TypeId`, and its
    /// `type_name` for the refusal's text. Filled by `App::open` from the
    /// schema's scoped tables, by `mount` from `public(S)` routes, by
    /// `mount_jobs` from scoped jobs, and by `mount_events` from scoped
    /// events, through `note_tenant`, which refuses a second type under a
    /// seen `NAME` (tenancy-completion spec §4).
    tenants: BTreeMap<&'static str, (TypeId, &'static str)>,
}

impl App {
    /// Opens the Db with the app's schema plus the platform's `Session`,
    /// `ApiKey`, and `CronRow` tables — registered here so recovery can
    /// always replay logins, minted keys, and schedules. The built-in daily
    /// session-expiry sweep (auth T6) is seeded the same unconditional way:
    /// its `#[job]` def and cron entry land in `job_defs`/`cron_entries`
    /// before any app-level `mount_jobs`/`schedule` call, and its payload
    /// shape is registered with the `Db` as `mount_jobs` registers every other
    /// job's, so every app gets it with no opt-in.
    pub fn open(
        disk: Box<dyn Disk>,
        schema: Schema,
        entropy: Box<dyn Entropy>,
        config: AppConfig,
    ) -> io::Result<App> {
        let table_tenants = schema.table_tenants();
        let mut db = Db::open(
            disk,
            config.db.clone(),
            schema
                .table::<Session>()
                .table::<ApiKey>()
                .table::<CronRow>(),
        )?;
        let sweep = <SessionSweep as JobEntry>::def();
        db.register_job_shapes(&[(
            sweep.name,
            sweep.fingerprint,
            sweep.scoped_by.map(|scope| (scope, sweep.scope_type)),
        )]);
        let mut app = App {
            db: Some(db),
            entropy,
            config,
            router: Router::new(),
            job_defs: vec![sweep],
            // No platform events to seed: the built-in surface emits none.
            event_defs: Vec::new(),
            cron_entries: vec![sweep::platform_cron_entry()],
            hosts: Vec::new(),
            max_in_flight: DEFAULT_MAX_IN_FLIGHT,
            oidc: None,
            tenants: BTreeMap::new(),
        };
        for (name, type_id, type_name) in table_tenants {
            app.note_tenant(name, type_id, type_name);
        }
        Ok(app)
    }

    /// Records one tenant type this app names, and refuses a second type
    /// under a `NAME` already seen, or an empty `NAME`, by name, before the
    /// app serves (tenancy-completion spec §4). A tenant's `NAME` is its
    /// identity: it is mixed into every scoped job's and event's fingerprint,
    /// so two types under one `NAME` are one tenant to every shape check. This is the one
    /// place the texts live. `App::open`, `mount` and `mount_jobs` call it,
    /// and `mount_events` does for scoped events. The same `type_name` under
    /// one `NAME` with two `TypeId`s is not two tenant types but a hand-built
    /// `JobDef` or `EventDef` whose `scope_type` disagrees with its
    /// `scope_type_name`, or two versions of one crate, and is refused with
    /// that cause.
    fn note_tenant(&mut self, name: &'static str, type_id: TypeId, type_name: &'static str) {
        assert!(
            !name.is_empty(),
            "tenant type {type_name} has an empty NAME — give its `impl Tenant` a `const NAME` \
             that names it"
        );
        let (seen, seen_name) = *self.tenants.entry(name).or_insert((type_id, type_name));
        assert!(
            seen == type_id || seen_name != type_name,
            "two registrations of the tenant type {type_name} (NAME \"{name}\") are different \
             types — a hand-built `JobDef` or `EventDef` whose `scope_type` and \
             `scope_type_name` name different types (build it from its `#[job]`'s or \
             `#[event]`'s `def()`, or set both fields from the type its `scoped_by` names), or \
             two versions of one crate in the build (unify them)"
        );
        assert!(
            seen == type_id,
            "two tenant types are named \"{name}\": {seen_name} and {type_name} — a tenant's \
             NAME is its identity (it is inside every scoped job's and event's fingerprint), so \
             give each type its own `const NAME` in its `impl Tenant`"
        );
    }

    pub fn mount(mut self, routes: Vec<Route>) -> App {
        for scope in routes.iter().filter_map(|route| route.public_scope) {
            self.note_tenant(scope.name, (scope.type_id)(), (scope.type_name)());
        }
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

    /// Declares one outside service its jobs call (outbound-effects spec §9):
    /// exactly one entry per host a mounted job names, checked when the
    /// kernel is built, so mount order is free (plan amendment A5).
    pub fn outbound(mut self, host: Host) -> App {
        assert!(
            !self.hosts.iter().any(|h| h.name() == host.name()),
            "contract: app.outbound(Host::new(\"{}\")) declared twice — one Host entry per host",
            host.name()
        );
        self.hosts.push(host);
        self
    }

    /// How many outbound calls this node keeps in flight at once (default
    /// 64). A due outbound job waits while the table is full; a pure job
    /// never waits behind it (spec §4).
    pub fn outbound_max_in_flight(mut self, n: usize) -> App {
        assert!(
            n > 0,
            "contract: App::outbound_max_in_flight(0) would never start a call — give at least 1"
        );
        self.max_in_flight = n;
        self
    }

    /// The hosts this app's mounted jobs call, sorted and deduplicated — what
    /// `sim.outbound(…)` takes (spec §10). Read it before building a kernel,
    /// which takes the job defs.
    pub fn outbound_hosts(&self) -> Vec<&'static str> {
        self.job_defs
            .iter()
            .filter_map(|d| d.outbound.as_ref().map(|o| o.host))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    /// Registers `#[job]` descriptors (mount-time duplicate-name panic — each
    /// job type is mounted once, like routes), and each one's payload shape
    /// and declared tenancy (its `scoped_by` with its `scope_type`) with the
    /// `Db` (`Db::register_job_shapes`) in the same step, so the enqueue
    /// refusal holds for every transaction through this App — before any
    /// kernel exists, too — and the startup refusal knows both.
    pub fn mount_jobs(mut self, defs: Vec<JobDef>) -> App {
        let shapes: Vec<_> = defs
            .iter()
            .map(|d| {
                (
                    d.name,
                    d.fingerprint,
                    d.scoped_by.map(|scope| (scope, d.scope_type)),
                )
            })
            .collect();
        for def in &defs {
            if let Some(name) = def.scoped_by {
                self.note_tenant(name, def.scope_type, def.scope_type_name);
            }
        }
        for def in defs {
            assert!(
                !self.job_defs.iter().any(|d| d.name == def.name),
                "job \"{}\" registered twice — each #[job] type is mounted once",
                def.name
            );
            self.job_defs.push(def);
        }
        self.db_mut().register_job_shapes(&shapes);
        self
    }

    /// Registers `#[event]` descriptors for `describe` (mount-time
    /// duplicate-name panic — each event type is mounted once, like jobs).
    ///
    /// A scoped event's tenant type joins the app's tenant census here, as a
    /// scoped table's does at `App::open` and a scoped job's at `mount_jobs`
    /// (tenancy-completion spec §4). Two types under one `Tenant::NAME`, or
    /// an empty `NAME`, are refused at this call, before the app serves.
    ///
    /// Delivery never reads this registry, and mounting is therefore
    /// optional: an unregistered `#[event]` still emits and still delivers,
    /// it just does not appear in `Contract.events` (nor its payload in
    /// `Contract.types`), so a generated client cannot see it, and its
    /// tenant type is not in the census.
    ///
    /// A def's `scoped_by` and `scope_type` agree, or it is refused by name:
    /// a scoped def names its tenant in both (`scope_type` other than
    /// `Unscoped`), and a global one in neither. `#[event]`'s `def()` always
    /// agrees, so a disagreement is a hand-built `EventDef`, which would
    /// otherwise skip the census or list a scoped event as global. Whether
    /// `scope_type` and `scope_type_name` name one type is `note_tenant`'s.
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
            assert!(
                def.scoped_by.is_some() == (def.scope_type != TypeId::of::<Unscoped>()),
                "contract: event \"{}\" was mounted with `scoped_by: {:?}` and `scope_type` \
                 {} — an `EventDef` names its tenant in both, or, for a global event, in \
                 neither (`Unscoped`), so this hand-built def would {}; fix: build it from its \
                 `#[event]`'s `def()`, or set `scoped_by`, `scope_type` and `scope_type_name` \
                 from the one type its `type Scope` names",
                def.name,
                def.scoped_by,
                def.scope_type_name,
                if def.scoped_by.is_some() {
                    "enter the tenant census under a type that is no tenant"
                } else {
                    "skip the tenant census and be described as global"
                }
            );
            if let Some(scope_name) = def.scoped_by {
                self.note_tenant(scope_name, def.scope_type, def.scope_type_name);
            }
            self.event_defs.push(def);
        }
        self
    }

    /// Registers a cron schedule (mount-time duplicate-name panic — each
    /// name is scheduled once). `make` builds the job enqueued on each tick.
    /// A schedule enqueues on a bare `Tx`, so it takes `global` jobs only; a
    /// sweep that acts per tenant is a `global, cross_tenant` job that fans
    /// out with `Tx::enqueue_in` (GUIDE §7).
    pub fn schedule<J: UnscopedJob + 'static>(
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
    ///
    /// The app's own `main` does, though, because the app builds the `App` it
    /// hands `launch`. That is the trust boundary (ruling R48): a `Db` reaches
    /// every tenant through `Db::snapshot`, whose scoped and raw reads take a
    /// scope on the caller's word with no `Tenant::authorize` anywhere on the
    /// path. The tenancy guarantee covers the typed surface the framework
    /// hands app logic — `Tx` in handlers and jobs, where a scoped table is
    /// unreachable without `tx.scope::<Org>(org, ctx)?` — and whoever holds
    /// the `App` is trusted as the operator, the same standing as whoever
    /// holds the data directory.
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

    pub(crate) fn take_outbound(&mut self) -> (Vec<Host>, usize) {
        (std::mem::take(&mut self.hosts), self.max_in_flight)
    }

    /// The declared `Host` entries, for `describe`.
    pub(crate) fn outbound_config(&self) -> &[Host] {
        &self.hosts
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
    /// The group mints (plan amendment A9): an outbound job a route enqueues
    /// draws its idempotency key from this app's entropy when it is staged.
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
        let group = self
            .db
            .as_mut()
            .expect(DB_TAKEN)
            .begin_group_minting(&*self.entropy)?;
        Ok((group, staged))
    }

    /// A minting group for the kernel's job, cron and outbound arms (plan
    /// amendment A9): a job, a cron tick or a `settle` can enqueue an
    /// outbound job, whose key is drawn from this app's entropy. Field
    /// projections, as in `begin_group`, so the borrows split.
    pub(crate) fn begin_job_group(&mut self) -> io::Result<Group<'_>> {
        let entropy = &*self.entropy;
        self.db
            .as_mut()
            .expect(DB_TAKEN)
            .begin_group_minting(entropy)
    }

    /// Test-only: one minting transaction — `Db::transact` refuses an
    /// outbound enqueue, so a test seeds one through here. Every commit
    /// failure is `Err` (a test has no request to answer).
    #[doc(hidden)]
    pub fn transact_for_tests<T, E>(
        &mut self,
        f: impl FnOnce(&mut Tx) -> Result<T, E>,
    ) -> Result<T, TxError<E>> {
        let mut group = self.begin_job_group().map_err(TxError::Io)?;
        let out = group.stage(f)?;
        group
            .commit()
            .map_err(|failure| TxError::Io(failure.error))?;
        Ok(out)
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
    /// `pub(crate)`; the kernel itself uses `db_mut`. On `db`'s trust
    /// boundary, and mutably: a `&mut Db` reaches every tenant, for reads and
    /// writes alike, with no `Tenant::authorize` on the path (ruling R48).
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

    /// The `emits(...)` drift oracle's reach (TS-codegen spec §2/§5): runs
    /// one stream route's subscribe glue and reports the event names the
    /// `Subscription` it returned actually carries, so a test can compare
    /// them against what `#[route(SSE …, emits(…))]` declared. A declaration
    /// nothing verifies is a decorative pin; this is the "nothing" made into
    /// something.
    ///
    /// It really runs the handler — it does not simulate one. The glue is
    /// staged into its own group, the same way `handle_or_subscribe` stages
    /// a real subscribe. It is NOT the same commit rule, deliberately: the
    /// real path forgives a `CommitFailure` that `invalidates(0, wrote)`
    /// says left the record durable, and this one maps every `CommitFailure`
    /// to `Err`, because a probe has no request to answer.
    ///
    /// **A subscribe handler that WRITES is refused, not committed.** A
    /// read-only subscribe handler — which is all of them in practice —
    /// stages no record at all (`Group::stage`'s empty-effects check) and
    /// this costs nothing. But a handler that stages (`ctx.create_session`
    /// on the subscribe path is the realistic case) would otherwise have its
    /// write committed into the app under test by what reads as a
    /// describe-time query, and with `PROBE_TOKEN` fixed that write is a
    /// FIXED, publicly derivable credential. So the probe checks
    /// `Group::is_empty` after staging and returns a named `Err` on the
    /// write path. The group is still committed either way — `Group`'s
    /// `Drop` panics on a non-empty group that was never committed, so
    /// refusing cannot mean "skip the commit"; refusing means the caller is
    /// told rather than left to discover a row it never asked for.
    ///
    /// Three deliberate departures from a real request, all so the probe can
    /// reach EVERY stream route rather than only the trivially reachable
    /// ones: `auth` is not enforced (no credential is presented and none is
    /// demanded, so an `auth` stream route is probeable) — unless it opens a
    /// tenant scope: with no principal, `tx.scope` meets rule 3 exactly as a
    /// request would, and the probe reports the route as not probeable,
    /// quoting that 401. A `public(S)` route's admission is minted for the
    /// probe as it is for a request; each `:param` is
    /// fed the literal `"0"`, which parses for every `FromParam` the
    /// platform ships (`u64`, `String`, `Id<T>`); and the fresh-token slot
    /// is a fixed literal rather than an `Entropy` draw, so probing cannot
    /// shift the app's entropy stream and perturb everything after it (hard
    /// rule 1). A handler that rejects that synthetic input comes back as
    /// `Err` naming the route and quoting what a request would receive, the
    /// status and the envelope's message, never as a silently empty name
    /// list.
    #[doc(hidden)]
    pub fn subscription_event_names(&mut self, route: &str) -> Result<Vec<&'static str>, String> {
        let Some(found) = self.router.routes().iter().find(|r| r.name == route) else {
            return Err(format!("no route named `{route}` is mounted"));
        };
        let RouteGlue::Sse(glue) = found.glue else {
            return Err(format!(
                "route `{route}` is not a stream route — only `#[route(SSE …)]` returns a \
                 Subscription to read event names from"
            ));
        };
        let captures: Vec<String> = found
            .params
            .iter()
            .map(|_| PROBE_CAPTURE.to_string())
            .collect();
        let session_ttl = self.config.session_ttl;
        // Read before `begin_group` takes `&mut self` — and honoured rather
        // than hardcoded to `None`, so probing a declared stream route gives
        // its handler the same `Ctx` a request would.
        let declares_cross_tenant = found.cross_tenant;
        let declared = Declared::of(found);
        let (mut group, _) = self
            .begin_group()
            .map_err(|e| format!("route `{route}`: the probe could not open a group: {e}"))?;
        let cap = declares_cross_tenant.then(|| group.cross_tenant_cap());
        let stance = declared.admitted(&group);
        let staged = group.stage(|tx| {
            let ctx = build_ctx(
                tx,
                &Presented::None,
                false,
                PROBE_TOKEN.to_string(),
                0,
                session_ttl,
                None,
                cap,
                stance,
            )?;
            glue(tx, &ctx, &captures, None)
        });
        // Read BEFORE the commit that empties it, and used below — the same
        // `Group::is_empty` reading `handle_or_subscribe` takes for `wrote`.
        let wrote = !group.is_empty();
        let names = match staged {
            Ok(sub) => Ok(sub.entries.iter().map(|e| e.name).collect()),
            Err(e) => {
                // Rendered as a request would receive it (hard rule 5): the
                // status and the envelope's message, through the renderer
                // requests use, never the Rust `Debug` of the `TxError`.
                let response = tx_error_response(e);
                let envelope: serde_json::Value = serde_json::from_slice(&response.body)
                    .expect("tx_error_response always answers with Response::error's envelope");
                let status = response.status;
                let message = envelope["error"]
                    .as_str()
                    .expect("Response::error's envelope carries its message as \"error\"");
                Err(format!(
                    "route `{route}`'s subscribe handler answered {status} under the emits(...) \
                     drift probe, so what it subscribes to cannot be read — a handler that \
                     rejects a credential-free request with `0` for every :param is not \
                     probeable; the {status} reads: {message}"
                ))
            }
        };
        // Unconditional, and before returning on any path: `Group`'s `Drop`
        // panics on a non-empty group that was never committed, so the write
        // the guard below refuses still has to be committed — the guard's job
        // is to say so, not to pretend it did not happen.
        group.commit().map_err(|f| {
            format!(
                "route `{route}`: the probe's group failed to commit: {}",
                f.error
            )
        })?;
        if wrote {
            return Err(format!(
                "route `{route}`'s subscribe handler staged a write, which the emits(...) \
                 drift probe has just committed into this app — a probe is not a request, so \
                 nothing here asked for that row; move the write out of the subscribe path, \
                 or leave this route out of the drift test"
            ));
        }
        names
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
        // Minted here, before `group.stage` borrows the group mutably, and
        // only for a route that declared the capability: `Group` is the one
        // handle a handler never holds, which is what keeps `describe`'s
        // cross-tenant listing an inventory rather than a hint (spec §3,
        // `CrossTenant`).
        let cap = route.cross_tenant.then(|| group.cross_tenant_cap());
        // Beside the capability, for the same reason: a `public(S)` route's
        // admission is minted from the `Group`, which a handler never holds
        // (job-tenancy spec §4).
        let stance = Declared::of(route).admitted(group);
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
                        cap,
                        stance,
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
                        cap,
                        stance,
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
// Two more independent request facts than clippy's limit, each already its
// own argument: the declared capability and the route's stance join the
// credential, the auth gate, the fresh token, the clock reading, the TTL and
// the verifier. Bundling
// them into a struct would name no concept the kernel otherwise has —
// `crate::repl::session`'s ladder carries the same allow for the same
// reason.
#[allow(clippy::too_many_arguments)]
fn build_ctx<'g>(
    tx: &mut Tx,
    presented: &Presented,
    auth_required: bool,
    fresh_token: String,
    now: u64,
    session_ttl: u64,
    oidc: Option<&OidcVerifier>,
    cap: Option<CrossTenant<'g>>,
    stance: Stance<'g>,
) -> Result<Ctx<'g>, ApiError> {
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
        cap,
        stance,
    })
}

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

/// `TxError<ApiError>` -> `Response`, shared by both route flavors: `App`
/// names the handler's own rejection; `Io`/`NotPrimary`/`Verifying` name the
/// platform failure and the fix (hard rule 5). A `TxError::Io` this path can
/// reach comes from `begin_group`'s own snapshot (`handle_or_subscribe`), a
/// `group.commit()` failure, or `Group::stage` (a job handler's own
/// transaction never runs on an HTTP route — the kernel dispatches jobs, so
/// only `Group::stage`'s own checks, not `Db::pending_job_shape_refusal`'s
/// startup scan, can raise a refusal here). `Group::stage`'s two refusals,
/// `JobShapeRefusal` and `TableWriteRefusal`, are the only ones any of them
/// raises, so an `Io` carrying either is answered with its own text instead
/// of the storage-failure one: nothing was written, and a retry cannot
/// succeed. Any OTHER `Refusal` here (a table-gate barrier, a witness
/// conflict) would name node state, not this request, and must fall through
/// to `io_failure_response` like a genuine commit failure — which is why this
/// matches the two wrappers' own `of`, not `Refusal::of`.
fn tx_error_response(err: TxError<ApiError>) -> Response {
    match err {
        TxError::App(e) => e.response(),
        TxError::Io(e)
            if JobShapeRefusal::of(&e).is_some() || TableWriteRefusal::of(&e).is_some() =>
        {
            Response::error(500, &e.to_string())
        }
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
