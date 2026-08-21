//! `charpente describe` — Contract shape + determinism (describe T2, spec
//! docs/superpowers/specs/2026-07-23-introspection-edge-harness-design.md
//! §1). The fixture app mirrors tests/http.rs's App-building pattern (sim
//! `Disk`/`Entropy`); one #[table] row with a #[index] field, one plain
//! #[route] with a body and a named response (task 5: also a `Vec<T>`-
//! returning route, to pin the container-response case), one SSE #[route],
//! one #[job], one .schedule — enough surface to pin every `Contract` field
//! without pulling in auth/OIDC.

use std::time::Duration;

use charpente::http::{ApiError, App, AppConfig, Ctx, Cursor, Subscription};
use charpente::kernel::JobError;
use charpente::wire::{TypeSchema, WireU64};
use charpente::{Schema, Tx, View, Wire, event, job, jobs, route, routes, table};
use charpente_core::table::Id;
use charpente_sim::{FaultConfig, Sim};

#[table]
struct Widget {
    id: Id<Widget>,
    #[index]
    org: u64,
}

#[derive(Wire, serde::Serialize, serde::Deserialize)]
struct CreateWidget {
    org: WireU64,
}

#[derive(Wire, serde::Serialize)]
struct WidgetView {
    id: Id<Widget>,
    org: WireU64,
}

#[route(POST "/widgets")]
fn create_widget(tx: &mut Tx, body: CreateWidget) -> Result<WidgetView, ApiError> {
    let id = Id::from_raw(body.org.0);
    tx.put(&Widget {
        id,
        org: body.org.0,
    });
    Ok(WidgetView { id, org: body.org })
}

/// A `Vec<T>`-returning route (Ruling 24): pins the container-response case
/// — `response` must come back as `Seq(Box::new(Ref("WidgetView")))`, not
/// `None`/inlined.
#[route(GET "/widgets")]
fn list_widgets(tx: &mut Tx) -> Result<Vec<WidgetView>, ApiError> {
    Ok(tx
        .scan::<Widget>()?
        .into_iter()
        .map(|w| WidgetView {
            id: w.id,
            org: WireU64(w.org),
        })
        .collect())
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
    .mount(routes![create_widget, list_widgets, widget_feed])
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
      "stream": false,
      "params": [],
      "request": {
        "Ref": "CreateWidget"
      },
      "response": {
        "Ref": "WidgetView"
      },
      "error": {
        "Ref": "ApiError"
      },
      "emits": []
    },
    {
      "method": "GET",
      "pattern": "/widgets",
      "name": "list_widgets",
      "auth": false,
      "stream": false,
      "params": [],
      "request": null,
      "response": {
        "Seq": {
          "Ref": "WidgetView"
        }
      },
      "error": {
        "Ref": "ApiError"
      },
      "emits": []
    },
    {
      "method": "GET",
      "pattern": "/widgets/feed",
      "name": "widget_feed",
      "auth": false,
      "stream": true,
      "params": [],
      "request": null,
      "response": "Null",
      "error": {
        "Ref": "ApiError"
      },
      "emits": []
    }
  ],
  "types": [
    {
      "name": "ApiError",
      "schema": {
        "Enum": {
          "External": {
            "variants": [
              [
                "BadRequest",
                "String"
              ],
              [
                "Unauthorized",
                null
              ],
              [
                "UnrecognizedCredential",
                null
              ],
              [
                "SessionExpired",
                null
              ],
              [
                "Oidc",
                {
                  "Ref": "OidcReject"
                }
              ],
              [
                "NotFound",
                null
              ],
              [
                "Internal",
                "String"
              ]
            ]
          }
        }
      }
    },
    {
      "name": "CreateWidget",
      "schema": {
        "Struct": {
          "fields": [
            {
              "name": "org",
              "schema": {
                "Int64": {
                  "signed": false
                }
              },
              "optional": false
            }
          ]
        }
      }
    },
    {
      "name": "OidcReject",
      "schema": {
        "Enum": {
          "External": {
            "variants": [
              [
                "Malformed",
                "String"
              ],
              [
                "UnsupportedAlg",
                "String"
              ],
              [
                "BadSignature",
                null
              ],
              [
                "Expired",
                null
              ],
              [
                "NotYetValid",
                null
              ],
              [
                "WrongIssuer",
                {
                  "Struct": {
                    "fields": [
                      {
                        "name": "got",
                        "schema": "String",
                        "optional": false
                      },
                      {
                        "name": "configured",
                        "schema": "String",
                        "optional": false
                      }
                    ]
                  }
                }
              ],
              [
                "WrongAudience",
                {
                  "Struct": {
                    "fields": [
                      {
                        "name": "got",
                        "schema": "String",
                        "optional": false
                      },
                      {
                        "name": "configured",
                        "schema": "String",
                        "optional": false
                      }
                    ]
                  }
                }
              ]
            ]
          }
        }
      }
    },
    {
      "name": "WidgetView",
      "schema": {
        "Struct": {
          "fields": [
            {
              "name": "id",
              "schema": {
                "Id": "Widget"
              },
              "optional": false
            },
            {
              "name": "org",
              "schema": {
                "Int64": {
                  "signed": false
                }
              },
              "optional": false
            }
          ]
        }
      }
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

/// Task 5, Ruling 24's shape: a route with a body names its request type,
/// and both response and error are `Ref`s into `Contract.types` (not bare
/// names — `Vec<T>`'s `WIRE_NAME` is `""`, so a name-only field can't
/// describe a container response; see the next test).
#[test]
fn a_route_carries_its_request_and_response_types() {
    let app = fixture_app();
    let c = app.describe();
    let r = c.routes.iter().find(|r| r.name == "create_widget").unwrap();
    assert_eq!(
        r.request,
        Some(TypeSchema::Ref("CreateWidget")),
        "a route with a body must name its request type"
    );
    assert_eq!(r.response, TypeSchema::Ref("WidgetView"));
    assert_eq!(r.error, TypeSchema::Ref("ApiError"));
}

/// The whole reason Ruling 24 exists: a `Vec<T>`-returning route's response
/// must come back as `Seq(Ref(..))`, not `None`/inlined — a name-only field
/// (the plan's original `response_name: &'static str`) cannot express this
/// at all, because `Vec<T>`'s own `WIRE_NAME` is `""`.
#[test]
fn a_container_response_keeps_its_element_reference() {
    let app = fixture_app();
    let c = app.describe();
    let r = c.routes.iter().find(|r| r.name == "list_widgets").unwrap();
    assert_eq!(r.request, None, "a GET route with no body has no request");
    assert_eq!(
        r.response,
        TypeSchema::Seq(Box::new(TypeSchema::Ref("WidgetView")))
    );
    assert_eq!(r.error, TypeSchema::Ref("ApiError"));
}

#[test]
fn the_contract_carries_the_transitive_type_closure() {
    let app = fixture_app();
    let c = app.describe();
    let names: Vec<&str> = c.types.iter().map(|t| t.name).collect();
    assert!(names.contains(&"WidgetView"), "types: {names:?}");
    assert!(
        names.contains(&"ApiError"),
        "the error type must be described too: {names:?}"
    );
    // Transitive, not just direct: ApiError::Oidc wraps OidcReject, which
    // no route names directly — it only reaches the registry by walking
    // ApiError's own fields (ApiError::register()).
    assert!(
        names.contains(&"OidcReject"),
        "the closure must walk INTO ApiError, not just register it: {names:?}"
    );
}

/// A negative oracle (Global Constraints): `assert_eq!(names, sorted)` is
/// green when `types` is empty, so this owes a floor (non-empty, contains
/// named entries reached via the closure, not just a route's own direct
/// response/error) as well as the sortedness check itself.
///
/// `into_sorted` is a `BTreeMap` walk, so sortedness holds by construction —
/// no mutation inside `wire.rs`/`describe.rs` can redden the sortedness half
/// without also breaking the dozens of other tests that read `TypeRegistry`
/// output. The floor is what carries this test's teeth: swap
/// `reg.into_sorted()` for `Vec::new()` in `describe()` (`describe.rs`) and
/// this test catches it (empty types) where a bare sortedness check would
/// stay green (an empty list is trivially sorted).
#[test]
fn the_type_list_is_sorted_so_generation_is_deterministic() {
    let app = fixture_app();
    let c = app.describe();
    let names: Vec<&str> = c.types.iter().map(|t| t.name).collect();
    assert!(
        names.len() >= 4,
        "the floor: at least the four types this fixture's routes reach \
         (transitively, via ApiError -> OidcReject) — got {names:?}"
    );
    let mut sorted = names.clone();
    sorted.sort_unstable();
    assert_eq!(names, sorted, "Contract.types must be sorted (spec §4)");
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
