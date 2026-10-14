//! `charpente describe` (spec docs/superpowers/specs/2026-07-23-
//! introspection-edge-harness-design.md §1, describe T2): `Contract` is the
//! machine-readable shape of one mounted `App` — a pure walk of its
//! registries (tables, routes, events, jobs, cron, config). No disk writes,
//! no socket, no kernel. Every type here derives `serde::Serialize`; field
//! ORDER is the JSON order and is part of the pinned shape (contract
//! determinism, this plan's Global Constraints): tables render in
//! `Db::describe_tables`'s sorted-name (`BTreeMap`) order, events in
//! event-name order, routes/jobs/cron in registration order — no `HashMap`
//! anywhere in this path.

use std::any::TypeId;
use std::fmt::Write as _;
use std::time::Duration;

use serde::Serialize;

use charpente_core::db::{Job, TableDescription};
use charpente_core::event::EventRow;
use charpente_core::job::JobRow;
use charpente_core::migration::SchemaVersion;
use charpente_core::table::Row;

use crate::auth::sweep::SESSION_SWEEP_NAME;
use crate::auth::{ApiKey, SessionSweep};
use crate::http::{App, EmittedEvent, Route, RouteGlue, Session};
use crate::kernel::JobDef;
use crate::kernel::cron::{CronEntry, CronRow};
use crate::wire::{TypeDef, TypeRegistry, TypeSchema};

#[derive(Serialize)]
pub struct Contract {
    pub charpente_version: &'static str,
    pub tables: Vec<TableInfo>,
    pub routes: Vec<RouteInfo>,
    /// Every `#[event]` type registered with `mount_events(events![…])`,
    /// sorted by event name. Empty for an app that registers none — an
    /// unregistered event still emits and still delivers (delivery has no
    /// registry at all, see `EventDef`), it just isn't in the contract.
    pub events: Vec<EventInfo>,
    /// The transitive closure of every named type a route's params, request,
    /// response, or error references, plus every event type a stream route
    /// declares with `emits(...)` (registered by the ROUTE, so a declared
    /// payload is here even when the event is never mounted — task 7), plus
    /// every registered event's payload
    /// — `TypeRegistry::into_sorted()`, a `BTreeMap` walk, so this is sorted
    /// by construction (spec §4's determinism requirement holds without a
    /// separate sort call someone could delete).
    pub types: Vec<TypeDef>,
    pub jobs: Vec<JobInfo>,
    pub cron: Vec<CronInfo>,
    /// Every `Host` entry in declaration order: names only, never a value (spec §9).
    pub outbound_hosts: Vec<HostInfo>,
    pub config: ConfigInfo,
    pub telemetry: TelemetryInfo,
}

#[derive(Serialize)]
pub struct TableInfo {
    pub name: &'static str,
    pub version: u32,
    pub fingerprint: Option<u64>,
    pub indexes: Vec<&'static str>,
    pub migrations: Vec<MigrationInfo>,
    /// `#[table(scoped_by = …)]`: every row lives under a tenant, so a scan
    /// against this table without `across_tenants` sees one tenant's rows
    /// only. Beside `builtin` because both are the classification bits an
    /// audit asks of a table's shape first — is it the platform's or the
    /// app's, and does it partition by tenant or not — computed once at
    /// `Schema::table::<R>()` registration (`row_is_scoped::<R>()`) and
    /// carried here unchanged, not re-derived.
    pub scoped: bool,
    pub builtin: bool,
}

#[derive(Serialize)]
pub struct MigrationInfo {
    pub from: u32,
    pub to: u32,
}

#[derive(Serialize)]
pub struct RouteInfo {
    pub method: &'static str,
    pub pattern: &'static str,
    pub name: &'static str,
    pub auth: bool,
    /// `#[route(…, public(Org))]`: the tenant scope this route admits every
    /// caller to, with no `authorize` (job-tenancy spec §4), by the tenant
    /// type's `Tenant::NAME`. `None` for `auth` and plain `public`. Beside `auth`
    /// because together they answer "who reaches what here": `auth=false`
    /// with `None` is plain public, and with `Some` it admits everyone to
    /// that tenant.
    pub public_scope: Option<&'static str>,
    /// `#[route(…, cross_tenant)]`: this endpoint declared the capability to
    /// enumerate a scoped table across every tenant (spec §3). Beside `auth`
    /// because it is the second thing an audit asks of a route, and here
    /// rather than left to a grep because a declared capability is what
    /// makes that audit an inventory.
    pub cross_tenant: bool,
    pub stream: bool,
    pub params: Vec<ParamInfo>,
    /// `None` for routes with no JSON body.
    pub request: Option<TypeSchema>,
    pub response: TypeSchema,
    pub error: TypeSchema,
    /// Event types a stream route declares via `#[route(SSE …, emits(Ev, …))]`,
    /// in declaration order. Each element carries BOTH names — the
    /// `Event::NAME` an SSE frame's `event:` field uses, and the payload's
    /// `Wire::WIRE_NAME`, which IS a key into `Contract.types` (see
    /// `EmittedEvent`). Empty for non-stream routes, and for a stream route
    /// that has not adopted the declaration.
    pub emits: Vec<EmittedEvent>,
}

#[derive(Debug, PartialEq, Serialize)]
pub struct ParamInfo {
    pub name: &'static str,
    pub schema: TypeSchema,
}

#[derive(Serialize)]
pub struct EventInfo {
    /// `Event::NAME` — the snake_case name `#[event]` computed from the type
    /// name, and the name a delivered SSE frame carries.
    pub name: &'static str,
    /// The payload's `Wire::WIRE_NAME`, i.e. its entry in `Contract.types`.
    /// An `#[event]` payload is exactly the JSON a subscriber receives, so
    /// it is a wire type like any route's response.
    pub payload: &'static str,
    /// `#[event(scoped_by = Org)]`: the tenant this event belongs to, by its
    /// tenant type's `Tenant::NAME`; `None` for a `#[event(global)]` one
    /// (tenancy-completion spec §8). It says which feeds can carry the event:
    /// a scoped event reaches only its own tenant's feed, and a global one
    /// only `Subscription::of` feeds. It replaces the old always-false key
    /// flag. An app key is a call-site filter no static contract can know,
    /// while an event's tenancy is a declaration on its type.
    pub scoped_by: Option<&'static str>,
}

/// One registered event type: the descriptor `events![…]` collects (explicit
/// registration, exactly like `jobs![…]`).
///
/// **Deliberately here and not in `kernel/`, where `JobDef`/`JobEntry` live.**
/// There are two event registries, for two different purposes. DELIVERY has
/// none: `Subscription::of::<Ev>()` captures its encoder statically at the
/// subscribe site, so "unregistered event at delivery" is inexpressible
/// rather than an error case (realtime spec §3). DESCRIBE has this one, and
/// it is read by `App::describe`, and by `App::mount_events`' tenant census
/// at mount, and by nothing else — no type under `kernel/` or in
/// `charpente-core` may name it, which is what keeps the delivery property
/// mechanically true instead of merely intended.
pub struct EventDef {
    pub name: &'static str,
    pub payload: &'static str,
    pub register_types: fn(&mut TypeRegistry),
    /// `#[event(scoped_by = Org)]`: the tenant this event belongs to, by its
    /// scope type's `Tenant::NAME`; `None` for a `global` event
    /// (tenancy-completion spec §6, §8). `describe` lists it, and
    /// `App::mount_events` puts it into the app's tenant census.
    pub scoped_by: Option<&'static str>,
    /// `TypeId::of::<<Ev as Event>::Scope>()`. The census refuses two tenant
    /// types under one `NAME` by comparing these (spec §4).
    pub scope_type: TypeId,
    /// `std::any::type_name` of the same type, for the census's refusal texts
    /// only: it words a refusal and never decides one, which `scope_type`
    /// does. Never persisted: the compiler's printer is not an identity
    /// (spec §1).
    pub scope_type_name: &'static str,
}

/// Implemented by the marker impl `#[event]` emits, so `events![a, b]`
/// collects descriptors explicitly (mirrors `kernel::JobEntry`).
pub trait EventEntry {
    fn def() -> EventDef;
}

#[macro_export]
macro_rules! events {
    ($($event:path),* $(,)?) => {
        ::std::vec![$(<$event as $crate::describe::EventEntry>::def()),*]
    };
}

#[derive(Serialize)]
pub struct JobInfo {
    pub name: &'static str,
    pub retry: RetryInfo,
    /// `#[job(scoped_by = Org)]`: the tenant scope this job acts in, by the
    /// tenant type's `Tenant::NAME`; `None` for a `global` job (job-tenancy spec §8).
    pub scoped_by: Option<&'static str>,
    /// `cross_tenant` on a job (`#[job(global, cross_tenant)]`, say) — see
    /// `RouteInfo::cross_tenant`. The inventory
    /// covers both surfaces that receive a `Tx`, or it covers neither.
    pub cross_tenant: bool,
    pub builtin: bool,
    /// `#[job(…, outbound = "<host>")]`: the host and the kernel's deadline
    /// (outbound-effects spec §11); `None` for a pure job.
    pub outbound: Option<OutboundInfo>,
}

#[derive(Serialize)]
pub struct OutboundInfo {
    pub host: &'static str,
    pub timeout_ms: u64,
}

/// One `Host` entry: which variables an operator must set, and which headers
/// they fill — names only; a value never reaches `describe` (spec §9).
#[derive(Serialize)]
pub struct HostInfo {
    pub host: &'static str,
    pub env: Vec<&'static str>,
    pub injects: Vec<String>,
    pub max_request_bytes: usize,
    pub max_reply_bytes: usize,
}

#[derive(Serialize)]
pub struct RetryInfo {
    pub max: u32,
    pub backoff_nanos: u64,
}

#[derive(Serialize)]
pub struct CronInfo {
    pub name: &'static str,
    pub interval_nanos: u64,
    pub builtin: bool,
}

#[derive(Serialize)]
pub struct ConfigInfo {
    pub body_cap: usize,
    pub session_ttl: u64,
    pub store_path: String,
    pub retain_segments: usize,
    pub checkpoint_every: u64,
    pub event_retention: u64,
    pub migration_batch_size: u64,
    pub oidc: bool,
}

/// Deliberately empty until M2 fills it in (spec §1.5).
#[derive(Serialize)]
pub struct TelemetryInfo {}

impl App {
    /// The machine-readable contract (DESIGN §7, spec §1): a pure walk of
    /// this App's registries. Reads registries only — no disk writes, no
    /// socket, no kernel.
    pub fn describe(&self) -> Contract {
        let mut reg = TypeRegistry::default();
        for r in self.router().routes() {
            (r.register_types)(&mut reg);
        }
        // Event payloads join the same closure: a subscriber decodes the
        // frame body against this type, so a client that can't name it can't
        // type the subscription.
        for e in self.event_defs() {
            (e.register_types)(&mut reg);
        }
        Contract {
            charpente_version: env!("CARGO_PKG_VERSION"),
            tables: self
                .db()
                .describe_tables()
                .into_iter()
                .map(table_info)
                .collect(),
            routes: self.router().routes().iter().map(route_info).collect(),
            events: {
                // Sorted by name, unlike routes/jobs/cron (registration
                // order): an event's registration position carries no
                // meaning — nothing dispatches through this list — so name
                // order is the stable one to render.
                let mut v: Vec<EventInfo> = self.event_defs().iter().map(event_info).collect();
                v.sort_unstable_by_key(|e| e.name);
                v
            },
            types: reg.into_sorted(),
            jobs: self.job_defs().iter().map(job_info).collect(),
            cron: self.cron_entries().iter().map(cron_info).collect(),
            outbound_hosts: self
                .outbound_config()
                .iter()
                .map(|h| HostInfo {
                    host: h.name(),
                    env: h.env_vars(),
                    injects: h.injected_headers(),
                    max_request_bytes: h.max_request(),
                    max_reply_bytes: h.max_reply(),
                })
                .collect(),
            config: config_info(self),
            telemetry: TelemetryInfo {},
        }
    }
}

fn table_info(t: TableDescription) -> TableInfo {
    TableInfo {
        name: t.name,
        version: t.version,
        fingerprint: t.fingerprint,
        indexes: t.index_names,
        migrations: t
            .migrations
            .into_iter()
            .map(|(from, to)| MigrationInfo { from, to })
            .collect(),
        scoped: t.scoped,
        builtin: is_builtin_table(t.name),
    }
}

/// The exact platform-owned table set: `Session`/`ApiKey`/`CronRow`
/// (`App::open`) plus `charpente.jobs`/`charpente.events`/`SchemaVersion`
/// (`Db::open`) — keyed on each Row's own `TABLE` const, never a typed-in
/// string literal, so a rename of any platform table can't silently drift
/// out of sync with this list.
fn is_builtin_table(name: &str) -> bool {
    name == Session::TABLE
        || name == ApiKey::TABLE
        || name == CronRow::TABLE
        || name == JobRow::TABLE
        || name == EventRow::TABLE
        || name == SchemaVersion::TABLE
}

fn route_info(route: &Route) -> RouteInfo {
    RouteInfo {
        method: route.method.name(),
        pattern: route.pattern,
        name: route.name,
        auth: route.auth,
        public_scope: route.public_scope.map(|s| s.name),
        cross_tenant: route.cross_tenant,
        stream: matches!(route.glue, RouteGlue::Sse(_)),
        params: route
            .params
            .iter()
            .map(|(name, f)| ParamInfo { name, schema: f() })
            .collect(),
        request: route.request.map(|f| f()),
        response: (route.response)(),
        error: (route.error)(),
        emits: route.emits.to_vec(),
    }
}

fn event_info(def: &EventDef) -> EventInfo {
    EventInfo {
        name: def.name,
        payload: def.payload,
        scoped_by: def.scoped_by,
    }
}

fn job_info(def: &JobDef) -> JobInfo {
    JobInfo {
        name: def.name,
        retry: RetryInfo {
            max: def.retry.max,
            backoff_nanos: nanos(def.retry.backoff),
        },
        scoped_by: def.scoped_by,
        cross_tenant: def.cross_tenant,
        // The platform sweep JOB's name is `SessionSweep` (the `#[job]`
        // struct's own `Job::NAME`) — distinct from the CRON entry's name
        // (`SESSION_SWEEP_NAME`, "charpente.session_sweep") used below.
        // Two different registrations, two different consts.
        builtin: def.name == <SessionSweep as Job>::NAME,
        outbound: def.outbound.as_ref().map(|o| OutboundInfo {
            host: o.host,
            timeout_ms: u64::try_from(o.timeout.as_millis()).expect("a timeout fits u64 millis"),
        }),
    }
}

fn cron_info(entry: &CronEntry) -> CronInfo {
    CronInfo {
        name: entry.name,
        interval_nanos: nanos(entry.interval),
        builtin: entry.name == SESSION_SWEEP_NAME,
    }
}

fn nanos(d: Duration) -> u64 {
    u64::try_from(d.as_nanos()).expect("describe: duration exceeds u64 nanos")
}

fn config_info(app: &App) -> ConfigInfo {
    let config = app.config();
    ConfigInfo {
        body_cap: config.body_cap,
        session_ttl: config.session_ttl,
        store_path: config.db.store_path.clone(),
        retain_segments: config.db.retain_segments,
        checkpoint_every: config.db.checkpoint_every,
        event_retention: config.db.event_retention,
        migration_batch_size: config.db.migration_batch_size,
        oidc: app.oidc_mounted(),
    }
}

/// `serde_json::to_string_pretty` plus a trailing newline — the shape a
/// human pastes into a file or `charpente describe` pipes to stdout.
pub fn to_json(c: &Contract) -> String {
    let mut s = serde_json::to_string_pretty(c).expect("Contract serialization cannot fail");
    s.push('\n');
    s
}

/// Fixed section headers, one line per item, no trailing whitespace, LF
/// endings — kept boring on purpose (T5's invoicing golden pins this).
pub fn to_human(c: &Contract) -> String {
    let mut out = String::new();
    writeln!(out, "tables:").unwrap();
    for t in &c.tables {
        let fp = t
            .fingerprint
            .map_or_else(|| "none".to_string(), |f| format!("{f:#x}"));
        let indexes = t.indexes.join(", ");
        let migrations = t
            .migrations
            .iter()
            .map(|m| format!("{}->{}", m.from, m.to))
            .collect::<Vec<_>>()
            .join(", ");
        writeln!(
            out,
            "  {} v{} fingerprint={fp} indexes=[{indexes}] migrations=[{migrations}] \
             scoped={} builtin={}",
            t.name, t.version, t.scoped, t.builtin
        )
        .unwrap();
    }
    writeln!(out, "routes:").unwrap();
    for r in &c.routes {
        writeln!(
            out,
            "  {} {} name={} auth={} public_scope={} cross_tenant={} stream={}",
            r.method,
            r.pattern,
            r.name,
            r.auth,
            r.public_scope.unwrap_or("none"),
            r.cross_tenant,
            r.stream
        )
        .unwrap();
        // Task 5: a route's type info (params/request/response/error/emits)
        // is included here, on its own indented line, rather than left
        // JSON-only — `to_human` exists to be a complete agent-readable
        // contract dump, and a route's types are now as central to what it
        // IS as its method/pattern. `emits` is included too (fix round 1,
        // F9): it was omitted at first, which contradicted this same
        // "complete contract dump" rationale — `RouteInfo` carries it and
        // the JSON prints it, so the human dump must too. `Debug` formatting
        // (not a bespoke renderer): boring on purpose, matching this
        // function's existing style, and `TypeSchema` has no `Display`.
        // What is deliberately NOT here: the full `Contract.types` closure
        // catalog — that belongs to the JSON format (structured, and
        // consumed by the TS emitter, not skimmed by a human), and dumping
        // every named type's full expansion inline per-route would work
        // against this format's "boring, fixed-width" design intent for an
        // app with many routes/types.
        //
        // `emits` is the one field that does NOT use `Debug`: ruling 46 made
        // its element a two-field struct, and `[EmittedEvent { event:
        // "widget_added", payload: "WidgetAdded" }, …]` buries both names in
        // boilerplate. `event->payload` shows the same two facts, in the same
        // order, in a fraction of the width.
        let emits = r
            .emits
            .iter()
            .map(|e| format!("{}->{}", e.event, e.payload))
            .collect::<Vec<_>>()
            .join(", ");
        writeln!(
            out,
            "    params={:?} request={:?} response={:?} error={:?} emits=[{emits}]",
            r.params, r.request, r.response, r.error
        )
        .unwrap();
    }
    writeln!(out, "events:").unwrap();
    for e in &c.events {
        writeln!(
            out,
            "  {} payload={} scoped_by={}",
            e.name,
            e.payload,
            e.scoped_by.unwrap_or("none")
        )
        .unwrap();
    }
    writeln!(out, "jobs:").unwrap();
    for j in &c.jobs {
        let outbound = match &j.outbound {
            Some(o) => format!("outbound={} timeout_ms={}", o.host, o.timeout_ms),
            None => "outbound=none".to_string(),
        };
        writeln!(
            out,
            "  {} max={} backoff_nanos={} scoped_by={} cross_tenant={} builtin={} {outbound}",
            j.name,
            j.retry.max,
            j.retry.backoff_nanos,
            j.scoped_by.unwrap_or("none"),
            j.cross_tenant,
            j.builtin
        )
        .unwrap();
    }
    writeln!(out, "cron:").unwrap();
    for e in &c.cron {
        writeln!(
            out,
            "  {} interval_nanos={} builtin={}",
            e.name, e.interval_nanos, e.builtin
        )
        .unwrap();
    }
    writeln!(out, "outbound_hosts:").unwrap();
    for h in &c.outbound_hosts {
        let list = |v: Vec<String>| {
            if v.is_empty() {
                "none".to_string()
            } else {
                v.join(",")
            }
        };
        writeln!(
            out,
            "  {} env={} injects={} max_request_bytes={} max_reply_bytes={}",
            h.host,
            list(h.env.iter().map(|s| s.to_string()).collect()),
            list(h.injects.clone()),
            h.max_request_bytes,
            h.max_reply_bytes
        )
        .unwrap();
    }
    writeln!(out, "config:").unwrap();
    writeln!(out, "  body_cap={}", c.config.body_cap).unwrap();
    writeln!(out, "  session_ttl={}", c.config.session_ttl).unwrap();
    writeln!(out, "  store_path={}", c.config.store_path).unwrap();
    writeln!(out, "  retain_segments={}", c.config.retain_segments).unwrap();
    writeln!(out, "  checkpoint_every={}", c.config.checkpoint_every).unwrap();
    writeln!(out, "  event_retention={}", c.config.event_retention).unwrap();
    writeln!(
        out,
        "  migration_batch_size={}",
        c.config.migration_batch_size
    )
    .unwrap();
    writeln!(out, "  oidc={}", c.config.oidc).unwrap();
    out
}
