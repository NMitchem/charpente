//! `charpente describe` — Contract shape + determinism (describe T2, spec
//! docs/superpowers/specs/2026-07-23-introspection-edge-harness-design.md
//! §1). The fixture app mirrors tests/http.rs's App-building pattern (sim
//! `Disk`/`Entropy`); one #[table] row with a #[index] field, one plain
//! #[route], one SSE #[route], one #[job], one .schedule — enough surface to
//! pin every `Contract` field without pulling in auth/OIDC.

use std::time::Duration;

use charpente::http::{ApiError, App, AppConfig, Ctx, Cursor, Subscription};
use charpente::kernel::JobError;
use charpente::{Schema, Tx, event, job, jobs, route, routes, table};
use charpente_core::table::Id;
use charpente_sim::{FaultConfig, Sim};

#[table]
struct Widget {
    id: Id<Widget>,
    #[index]
    org: u64,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct CreateWidget {
    org: u64,
}

#[route(POST "/widgets")]
fn create_widget(tx: &mut Tx, body: CreateWidget) -> Result<u64, ApiError> {
    tx.put(&Widget {
        id: Id::from_raw(body.org),
        org: body.org,
    });
    Ok(body.org)
}

#[event]
struct WidgetAdded {
    org: u64,
}

#[route(SSE "/widgets/feed")]
fn widget_feed(tx: &mut Tx, ctx: &Ctx, cursor: Option<Cursor>) -> Result<Subscription, ApiError> {
    let _ = (tx, ctx);
    Ok(Subscription::of::<WidgetAdded>().cursor(cursor))
}

#[job]
struct Ping {
    n: u64,
}

impl Ping {
    fn run(self, _tx: &mut Tx) -> Result<(), JobError> {
        Ok(())
    }
}

fn fixture_app() -> App {
    let sim = Sim::with_faults(0x2EC2_0001, FaultConfig::none());
    App::open(
        Box::new(sim.disk()),
        Schema::new().table::<Widget>(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )
    .unwrap()
    .mount(routes![create_widget, widget_feed])
    .mount_jobs(jobs![Ping])
    .schedule("ping-hourly", Duration::from_secs(3600), || Ping { n: 0 })
}

const GOLDEN: &str = r#"{
  "charpente_version": "0.0.0",
  "tables": [
    {
      "name": "ApiKey",
      "version": 1,
      "fingerprint": 11308777448940268015,
      "indexes": [
        "key_hash"
      ],
      "migrations": [],
      "builtin": true
    },
    {
      "name": "CronRow",
      "version": 1,
      "fingerprint": 13750309978683829658,
      "indexes": [
        "name"
      ],
      "migrations": [],
      "builtin": true
    },
    {
      "name": "Session",
      "version": 1,
      "fingerprint": 11676348087434866846,
      "indexes": [
        "token_hash"
      ],
      "migrations": [],
      "builtin": true
    },
    {
      "name": "Widget",
      "version": 1,
      "fingerprint": 8188356196272451442,
      "indexes": [
        "org"
      ],
      "migrations": [],
      "builtin": false
    },
    {
      "name": "charpente.events",
      "version": 1,
      "fingerprint": null,
      "indexes": [],
      "migrations": [],
      "builtin": true
    },
    {
      "name": "charpente.jobs",
      "version": 1,
      "fingerprint": null,
      "indexes": [],
      "migrations": [],
      "builtin": true
    },
    {
      "name": "charpente.schema_versions",
      "version": 1,
      "fingerprint": null,
      "indexes": [],
      "migrations": [],
      "builtin": true
    }
  ],
  "routes": [
    {
      "method": "POST",
      "pattern": "/widgets",
      "name": "create_widget",
      "auth": false,
      "stream": false
    },
    {
      "method": "GET",
      "pattern": "/widgets/feed",
      "name": "widget_feed",
      "auth": false,
      "stream": true
    }
  ],
  "jobs": [
    {
      "name": "SessionSweep",
      "retry": {
        "max": 5,
        "backoff_nanos": 30000000000
      },
      "builtin": true
    },
    {
      "name": "Ping",
      "retry": {
        "max": 5,
        "backoff_nanos": 30000000000
      },
      "builtin": false
    }
  ],
  "cron": [
    {
      "name": "charpente.session_sweep",
      "interval_nanos": 86400000000000,
      "builtin": true
    },
    {
      "name": "ping-hourly",
      "interval_nanos": 3600000000000,
      "builtin": false
    }
  ],
  "config": {
    "body_cap": 1048576,
    "session_ttl": 2592000000000000,
    "store_path": "tables.redb",
    "retain_segments": 2,
    "checkpoint_every": 4096,
    "event_retention": 65536,
    "migration_batch_size": 1000,
    "oidc": false
  },
  "telemetry": {}
}
"#;

#[test]
fn contract_json_shape_is_pinned() {
    let app = fixture_app();
    let json = charpente::describe::to_json(&app.describe());
    // The FULL golden, as a raw string literal, assert_eq! — not substrings.
    assert_eq!(json, GOLDEN);
}

#[test]
fn describe_is_deterministic_across_runs_and_reopens() {
    let a = charpente::describe::to_json(&fixture_app().describe());
    let b = charpente::describe::to_json(&fixture_app().describe());
    assert_eq!(a, b, "two fresh constructions must render byte-identical");
}

#[test]
fn builtin_flags_mark_exactly_the_platform_tables_jobs_and_cron() {
    use charpente::auth::sweep::SESSION_SWEEP_NAME;
    use charpente::auth::{ApiKey, SessionSweep};
    use charpente::http::Session;
    use charpente::kernel::CronRow;
    use charpente_core::db::Job;
    use charpente_core::event::EventRow;
    use charpente_core::job::JobRow;
    use charpente_core::migration::SchemaVersion;
    use charpente_core::table::Row;

    let c = fixture_app().describe();

    let mut builtin_tables: Vec<_> = c
        .tables
        .iter()
        .filter(|t| t.builtin)
        .map(|t| t.name)
        .collect();
    builtin_tables.sort_unstable();
    let mut expected_tables = vec![
        Session::TABLE,
        ApiKey::TABLE,
        CronRow::TABLE,
        JobRow::TABLE,
        EventRow::TABLE,
        SchemaVersion::TABLE,
    ];
    expected_tables.sort_unstable();
    assert_eq!(builtin_tables, expected_tables);

    let widget = c.tables.iter().find(|t| t.name == "Widget").unwrap();
    assert!(
        !widget.builtin,
        "the fixture's own table must not be marked builtin"
    );

    let builtin_jobs: Vec<_> = c
        .jobs
        .iter()
        .filter(|j| j.builtin)
        .map(|j| j.name)
        .collect();
    // The platform sweep JOB's own name (`Job::NAME`), distinct from
    // `SESSION_SWEEP_NAME` (the CRON entry's name, checked below).
    assert_eq!(builtin_jobs, vec![<SessionSweep as Job>::NAME]);
    let ping = c.jobs.iter().find(|j| j.name == "Ping").unwrap();
    assert!(!ping.builtin, "the fixture's own job must not be builtin");

    let builtin_cron: Vec<_> = c
        .cron
        .iter()
        .filter(|e| e.builtin)
        .map(|e| e.name)
        .collect();
    assert_eq!(builtin_cron, vec![SESSION_SWEEP_NAME]);
    let ping_cron = c.cron.iter().find(|e| e.name == "ping-hourly").unwrap();
    assert!(
        !ping_cron.builtin,
        "the fixture's own cron schedule must not be builtin"
    );
}
