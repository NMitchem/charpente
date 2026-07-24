//! `charpente describe` (spec docs/superpowers/specs/2026-07-23-
//! introspection-edge-harness-design.md §1, describe T2): `Contract` is the
//! machine-readable shape of one mounted `App` — a pure walk of its
//! registries (tables, routes, jobs, cron, config). No disk writes, no
//! socket, no kernel. Every type here derives `serde::Serialize`; field
//! ORDER is the JSON order and is part of the pinned shape (contract
//! determinism, this plan's Global Constraints): tables render in
//! `Db::describe_tables`'s sorted-name (`BTreeMap`) order, routes/jobs/cron
//! in registration order — no `HashMap` anywhere in this path.

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
use crate::http::{App, Route, RouteGlue, Session};
use crate::kernel::JobDef;
use crate::kernel::cron::{CronEntry, CronRow};

#[derive(Serialize)]
pub struct Contract {
    pub charpente_version: &'static str,
    pub tables: Vec<TableInfo>,
    pub routes: Vec<RouteInfo>,
    pub jobs: Vec<JobInfo>,
    pub cron: Vec<CronInfo>,
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
    pub stream: bool,
}

#[derive(Serialize)]
pub struct JobInfo {
    pub name: &'static str,
    pub retry: RetryInfo,
    pub builtin: bool,
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
        Contract {
            charpente_version: env!("CARGO_PKG_VERSION"),
            tables: self
                .db()
                .describe_tables()
                .into_iter()
                .map(table_info)
                .collect(),
            routes: self.router().routes().iter().map(route_info).collect(),
            jobs: self.job_defs().iter().map(job_info).collect(),
            cron: self.cron_entries().iter().map(cron_info).collect(),
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
        stream: matches!(route.glue, RouteGlue::Sse(_)),
    }
}

fn job_info(def: &JobDef) -> JobInfo {
    JobInfo {
        name: def.name,
        retry: RetryInfo {
            max: def.retry.max,
            backoff_nanos: nanos(def.retry.backoff),
        },
        // The platform sweep JOB's name is `SessionSweep` (the `#[job]`
        // struct's own `Job::NAME`) — distinct from the CRON entry's name
        // (`SESSION_SWEEP_NAME`, "charpente.session_sweep") used below.
        // Two different registrations, two different consts.
        builtin: def.name == <SessionSweep as Job>::NAME,
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
            "  {} v{} fingerprint={fp} indexes=[{indexes}] migrations=[{migrations}] builtin={}",
            t.name, t.version, t.builtin
        )
        .unwrap();
    }
    writeln!(out, "routes:").unwrap();
    for r in &c.routes {
        writeln!(
            out,
            "  {} {} name={} auth={} stream={}",
            r.method, r.pattern, r.name, r.auth, r.stream
        )
        .unwrap();
    }
    writeln!(out, "jobs:").unwrap();
    for j in &c.jobs {
        writeln!(
            out,
            "  {} max={} backoff_nanos={} builtin={}",
            j.name, j.retry.max, j.retry.backoff_nanos, j.builtin
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
