//! `charpente describe` — Contract shape + determinism (describe T2, spec
//! docs/superpowers/specs/2026-07-23-introspection-edge-harness-design.md
//! §1). The fixture app mirrors tests/http.rs's App-building pattern (sim
//! `Disk`/`Entropy`); one #[table] row with a #[index] field, one plain
//! #[route] with a body and a named response (task 5: also a `Vec<T>`-
//! returning route, to pin the container-response case), one SSE #[route],
//! one #[job], one #[event] (task 6), one .schedule — enough surface to pin
//! every `Contract` field without pulling in auth/OIDC.

use std::time::Duration;

use charpente::describe::ParamInfo;
use charpente::http::{ApiError, App, AppConfig, Ctx, Cursor, EmittedEvent, Subscription};
use charpente::kernel::JobError;
use charpente::wire::{TypeSchema, WireU64};
use charpente::{
    CrossTenant, Schema, Scoped, ScopedTx, Tx, View, Wire, event, events, job, jobs, route, routes,
    table,
};
use charpente_core::table::Id;
use charpente_sim::{FaultConfig, Sim};

#[table(global)]
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

#[route(POST "/widgets", public)]
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
#[route(GET "/widgets", public)]
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

/// Two path params (fix round 1, F10): `param_schema_entries` indexes
/// `non_ctx[i]` by param position, and no route anywhere else in the
/// workspace has two — so nothing pinned that params come back in PATH
/// order (not alphabetical, not declaration order of some other kind) with
/// distinct schemas attached to the right names. `org: String` and
/// `id: Id<Widget>` are deliberately different shapes so a mix-up (schemas
/// swapped between names) would be visible, not just a coincidental match.
#[route(GET "/orgs/:org/widgets/:id", public)]
fn get_org_widget(tx: &mut Tx, org: String, id: Id<Widget>) -> Result<WidgetView, ApiError> {
    let _ = org;
    let w = tx.get(id)?.ok_or(ApiError::NotFound)?;
    Ok(WidgetView {
        id: w.id,
        org: WireU64(w.org),
    })
}

/// `org: WireU64`, not a bare `u64` (task 6): an `#[event]` payload is
/// exactly the JSON a subscriber receives, so it is a wire type — the
/// `Wire` derive `#[event]` emits refuses a bare 64-bit integer here for the
/// same reason a route body's does.
#[event(global)]
struct WidgetAdded {
    org: WireU64,
}

/// A second event, with a payload shape unlike `WidgetAdded`'s — the two are
/// registered in REVERSE name order below, which is what makes
/// `Contract.events`' sort a real assertion rather than a coincidence.
#[event(global)]
struct WidgetRemoved {
    id: Id<Widget>,
}

/// A subscribe handler that WRITES — the one shape the drift probe refuses
/// (task 7 review, Important 4). Deliberately NOT mounted in `fixture_app`:
/// it would change the pinned golden and it exists only for the guard's own
/// test, which builds its own app around it. `create_session` on a subscribe
/// path is the realistic case the guard exists for — minting a credential
/// for the stream the client is about to open.
#[route(SSE "/widgets/minting-feed", public, emits(WidgetAdded))]
fn minting_feed(tx: &mut Tx, ctx: &Ctx, cursor: Option<Cursor>) -> Result<Subscription, ApiError> {
    let _ = ctx.create_session(tx, "probe-subject");
    Ok(Subscription::of::<WidgetAdded>().cursor(cursor))
}

/// A subscribe handler that REJECTS the probe's synthetic request — the
/// third of the probe's three error strings (task 7 review, Minor 3). Not
/// mounted in `fixture_app`, for the same reason `minting_feed` is not.
#[route(SSE "/widgets/picky-feed", public, emits(WidgetAdded))]
fn picky_feed(tx: &mut Tx, ctx: &Ctx, cursor: Option<Cursor>) -> Result<Subscription, ApiError> {
    let _ = (tx, ctx, cursor);
    Err(ApiError::Unauthorized)
}

/// Two declared events, not one (task 7): with a single event the drift
/// oracle below cannot tell "the declaration and the subscription agree"
/// from "both happen to be one element long", and narrowing the declaration
/// to prove the oracle red would have nothing to narrow to.
#[route(SSE "/widgets/feed", public, emits(WidgetAdded, WidgetRemoved))]
fn widget_feed(tx: &mut Tx, ctx: &Ctx, cursor: Option<Cursor>) -> Result<Subscription, ApiError> {
    let _ = (tx, ctx);
    Ok(Subscription::of::<WidgetAdded>()
        .and::<WidgetRemoved>()
        .cursor(cursor))
}

#[job(global)]
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
    .mount(routes![
        create_widget,
        list_widgets,
        get_org_widget,
        widget_feed
    ])
    .mount_jobs(jobs![Ping])
    // Reverse name order on purpose: `describe()` sorts events by name, so
    // registering them sorted would leave that sort unpinned.
    .mount_events(events![WidgetRemoved, WidgetAdded])
    .schedule("ping-hourly", Duration::from_secs(3600), || Ping { n: 0 })
}

const GOLDEN: &str = r#"{
  "charpente_version": "0.3.0",
  "tables": [
    {
      "name": "ApiKey",
      "version": 1,
      "fingerprint": 13155888733427888359,
      "indexes": [
        "key_hash"
      ],
      "migrations": [],
      "scoped": false,
      "builtin": true
    },
    {
      "name": "CronRow",
      "version": 1,
      "fingerprint": 6876731707294550864,
      "indexes": [
        "name"
      ],
      "migrations": [],
      "scoped": false,
      "builtin": true
    },
    {
      "name": "Session",
      "version": 1,
      "fingerprint": 50804230146766996,
      "indexes": [
        "token_hash"
      ],
      "migrations": [],
      "scoped": false,
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
      "scoped": false,
      "builtin": false
    },
    {
      "name": "charpente.events",
      "version": 1,
      "fingerprint": null,
      "indexes": [],
      "migrations": [],
      "scoped": false,
      "builtin": true
    },
    {
      "name": "charpente.jobs",
      "version": 1,
      "fingerprint": null,
      "indexes": [],
      "migrations": [],
      "scoped": false,
      "builtin": true
    },
    {
      "name": "charpente.schema_versions",
      "version": 1,
      "fingerprint": null,
      "indexes": [],
      "migrations": [],
      "scoped": false,
      "builtin": true
    }
  ],
  "routes": [
    {
      "method": "POST",
      "pattern": "/widgets",
      "name": "create_widget",
      "auth": false,
      "public_scope": null,
      "cross_tenant": false,
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
      "public_scope": null,
      "cross_tenant": false,
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
      "pattern": "/orgs/:org/widgets/:id",
      "name": "get_org_widget",
      "auth": false,
      "public_scope": null,
      "cross_tenant": false,
      "stream": false,
      "params": [
        {
          "name": "org",
          "schema": "String"
        },
        {
          "name": "id",
          "schema": {
            "Id": "Widget"
          }
        }
      ],
      "request": null,
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
      "pattern": "/widgets/feed",
      "name": "widget_feed",
      "auth": false,
      "public_scope": null,
      "cross_tenant": false,
      "stream": true,
      "params": [],
      "request": null,
      "response": "Null",
      "error": {
        "Ref": "ApiError"
      },
      "emits": [
        {
          "event": "widget_added",
          "payload": "WidgetAdded"
        },
        {
          "event": "widget_removed",
          "payload": "WidgetRemoved"
        }
      ]
    }
  ],
  "events": [
    {
      "name": "widget_added",
      "payload": "WidgetAdded",
      "scoped_by": null
    },
    {
      "name": "widget_removed",
      "payload": "WidgetRemoved",
      "scoped_by": null
    }
  ],
  "types": [
    {
      "name": "ApiError",
      "schema": {
        "Struct": {
          "fields": [
            {
              "name": "error",
              "schema": "String",
              "optional": false
            }
          ]
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
      "name": "WidgetAdded",
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
      "name": "WidgetRemoved",
      "schema": {
        "Struct": {
          "fields": [
            {
              "name": "id",
              "schema": {
                "Id": "Widget"
              },
              "optional": false
            }
          ]
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
      "scoped_by": null,
      "cross_tenant": false,
      "builtin": true,
      "outbound": null
    },
    {
      "name": "Ping",
      "retry": {
        "max": 5,
        "backoff_nanos": 30000000000
      },
      "scoped_by": null,
      "cross_tenant": false,
      "builtin": false,
      "outbound": null
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
  "outbound_hosts": [],
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

/// Fix round 1, F9: `to_human`'s new route line (`describe.rs:288-293`) had
/// zero test coverage — "the human golden is pinned too" (the original
/// brief's claim, deciding the `to_human` question) was false, and neither
/// the implementer nor the controller checked. A `contains` on one known
/// route's exact line is enough to catch a field being dropped, reordered,
/// or renamed, without pinning the WHOLE human dump byte-for-byte the way
/// `contract_json_shape_is_pinned` pins the JSON.
#[test]
fn to_human_pins_a_known_routes_type_line() {
    let out = charpente::describe::to_human(&fixture_app().describe());
    assert!(
        out.contains(
            "  POST /widgets name=create_widget auth=false public_scope=none cross_tenant=false stream=false\n    \
             params=[] request=Some(Ref(\"CreateWidget\")) response=Ref(\"WidgetView\") \
             error=Ref(\"ApiError\") emits=[]\n"
        ),
        "{out}"
    );
}

/// Task 5, Ruling 24's shape: a route with a body names its request type,
/// and both response and error are `Ref`s into `Contract.types` (not bare
/// names — `Vec<T>`'s `WIRE_NAME` is `""`, so a name-only field can't
/// describe a container response; see the next test).
///
/// **This is the test that actually pins Ruling 24** (fix round 1, F5): the
/// `assert_eq!(r.response, TypeSchema::Ref("WidgetView"))` below is a
/// SCALAR response, where `reference()` and `schema()` diverge —
/// `reference()` returns `Ref("WidgetView")`, `schema()` would inline the
/// whole struct. Swap `reference()` for `schema()` at the response position
/// in `crates/charpente-macros/src/route.rs` and this assertion (and
/// `contract_json_shape_is_pinned`) reddens. The next test's container case
/// CANNOT discriminate the two (see its own doc comment) — that discovery
/// is F5, measured against the exact mutation the original brief asked for.
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

/// A `Vec<T>`-returning route's response must come back as `Seq(Ref(..))`,
/// not `None`/inlined — a name-only field (the plan's original
/// `response_name: &'static str`) cannot express this at all, because
/// `Vec<T>`'s own `WIRE_NAME` is `""`.
///
/// **This does NOT discriminate `reference()` from `schema()`** (fix round
/// 1, F5, measured): `Vec<T>::WIRE_NAME` is `""`, so `Vec<T>::reference()`
/// falls through to `Vec<T>::schema()` — they are the same function for
/// every container, so swapping `reference()` for `schema()` at the
/// response position leaves this test GREEN. The scalar assertion in
/// `a_route_carries_its_request_and_response_types` (above) is the test
/// that carries Ruling 24. What this test DOES catch: changing
/// `Vec<T>::schema()` to inline `T::schema()` instead of `T::reference()`
/// reddens it — but that property is already pinned by
/// `crates/charpente/tests/wire.rs`'s
/// `fidelity_nested_and_vec_wrapped_named_types_match_serde`. Kept anyway
/// as a direct pin on the container-response shape at the `RouteInfo`
/// level, which that fidelity test does not touch.
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

/// Fix round 1, F10: `param_schema_entries` (`crates/charpente-macros/src/route.rs`)
/// indexes `non_ctx[i]` by param position, and before this test no route
/// anywhere in the workspace had two path params — so nothing pinned that
/// `Route.params` comes back in PATH order (not alphabetical, e.g.) with
/// each schema attached to the right name. `get_org_widget`'s two params
/// have deliberately different schemas (`String` vs `Id("Widget")`) so a
/// mix-up (schemas swapped between the two names) would show up here, not
/// just a coincidental match.
#[test]
fn two_path_params_come_back_in_path_order_with_distinct_schemas() {
    let app = fixture_app();
    let c = app.describe();
    let r = c
        .routes
        .iter()
        .find(|r| r.name == "get_org_widget")
        .unwrap();
    assert_eq!(
        r.params,
        vec![
            ParamInfo {
                name: "org",
                schema: TypeSchema::String,
            },
            ParamInfo {
                name: "id",
                schema: TypeSchema::Id("Widget"),
            },
        ],
        "params must come back in the path's own order (:org before :id in \
         \"/orgs/:org/widgets/:id\"), each with its own distinct schema"
    );
}

/// `WidgetView`/`CreateWidget` reach the registry via `register_types`
/// (direct: `create_widget`'s request and response) and `list_widgets`'s
/// `register_types` reaches `WidgetView` by calling `Vec<WidgetView>::register`,
/// which forwards into `WidgetView::register` (`Vec<T>`'s own `WIRE_NAME` is
/// `""`, so it contributes no entry of its own) — the container wrapper
/// does not stop the walk.
///
/// **`OidcReject` must NOT appear here** (fix round 1, F1): before the fix,
/// `ApiError` derived `Wire` and its `Oidc(OidcReject)` variant put a
/// `Ref("OidcReject")` in `ApiError::schema()`, so `ApiError::register()`
/// recursed into it — even though no route names `OidcReject` directly, and
/// even though the wire never actually carries that shape (see `ApiError`'s
/// doc comment in `http/mod.rs`). `ApiError`'s hand-written schema now
/// describes the `{"error": "<message>"}` envelope instead, which has no
/// `Ref` to anything, so `register()` never reaches `OidcReject` and it
/// correctly falls out of `Contract.types` entirely.
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
    assert!(
        !names.contains(&"OidcReject"),
        "ApiError's hand-written schema describes the envelope, which names \
         no other type — OidcReject must not reach Contract.types: {names:?}"
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
        names.len() >= 3,
        "the floor: at least the three types this fixture's routes reach \
         (WidgetView, CreateWidget, ApiError — OidcReject is deliberately \
         NOT among them since fix round 1's F1, see the previous test) — \
         got {names:?}"
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

/// Task 6, Step 1: the contract enumerates every registered `#[event]` with
/// its payload type, and that payload joins the same type closure a route's
/// types do — a TS client cannot type a subscription without both halves.
#[test]
fn the_contract_enumerates_events_with_their_payload_types() {
    let app = fixture_app();
    let c = app.describe();
    let e = c.events.iter().find(|e| e.name == "widget_added").unwrap();
    assert_eq!(e.payload, "WidgetAdded");
    assert!(
        c.types.iter().any(|t| t.name == "WidgetAdded"),
        "an event payload must be in the type closure too: {:?}",
        c.types.iter().map(|t| t.name).collect::<Vec<_>>()
    );
}

/// The events list is sorted by event name, NOT registration order — the
/// fixture registers `events![WidgetRemoved, WidgetAdded]`, so registration
/// order is the exact reverse of what this asserts. Delete
/// `v.sort_unstable_by_key(...)` in `describe()` and this reddens (as does
/// `contract_json_shape_is_pinned`); an `assert!(is_sorted)` on a
/// one-event fixture would not.
///
/// `scoped_by` is pinned `None` here, for both events: both are
/// `#[event(global)]`. The scoped half is
/// `describe_names_every_scoped_event`'s.
#[test]
fn events_render_in_name_order_and_report_their_tenancy() {
    let c = fixture_app().describe();
    let rendered: Vec<(&str, &str, Option<&str>)> = c
        .events
        .iter()
        .map(|e| (e.name, e.payload, e.scoped_by))
        .collect();
    assert_eq!(
        rendered,
        vec![
            ("widget_added", "WidgetAdded", None),
            ("widget_removed", "WidgetRemoved", None),
        ]
    );
}

/// `to_human`'s events section, pinned the same way its route line is (task
/// 5, fix round 1's F9: a new `to_human` section with no test is a section
/// whose fields can be dropped, reordered or renamed silently).
#[test]
fn to_human_pins_the_events_section() {
    let out = charpente::describe::to_human(&fixture_app().describe());
    assert!(
        out.contains(
            "events:\n  widget_added payload=WidgetAdded scoped_by=none\n  \
             widget_removed payload=WidgetRemoved scoped_by=none\n"
        ),
        "{out}"
    );
}

/// The negative-space assert beside the duplicate check (hard rule 4): an
/// `EventInfo` whose payload names nothing in `Contract.types` is a contract
/// a client cannot resolve. `#[event]` can't produce one — its `Wire` derive
/// always names the struct — so the only way to reach it is a hand-written
/// `EventEntry`, which is exactly what this builds.
///
/// The WHOLE message is pinned, remedy included (review round 1, finding 3):
/// hard rule 5 makes the fix half of a diagnostic product surface, and pinning
/// only the first clause lets "make the payload a #[derive(Wire)] type" rot
/// into something that no longer says what to do.
#[test]
#[should_panic(
    expected = "event \"anonymous\" registers an empty payload name — every \
                           Contract.events entry must name a type in Contract.types, and \
                           \"\" is `Wire`'s \"anonymous/inline\" marker; make the payload a \
                           #[derive(Wire)] type and pass its WIRE_NAME"
)]
fn mounting_an_event_whose_payload_names_no_type_panics() {
    struct HandWritten;
    impl charpente::describe::EventEntry for HandWritten {
        fn def() -> charpente::describe::EventDef {
            charpente::describe::EventDef {
                name: "anonymous",
                payload: "",
                register_types: |_| {},
                scoped_by: None,
                scope_type: std::any::TypeId::of::<charpente::core::table::Unscoped>(),
                scope_type_name: std::any::type_name::<charpente::core::table::Unscoped>(),
            }
        }
    }
    let _ = fixture_app().mount_events(events![HandWritten]);
}

/// Mount-time duplicate rejection, with its exact text (hard rule 5) —
/// mirrors `mount_jobs`' own panic, and is why registering the same event
/// twice can't quietly produce two `Contract.events` entries with one name.
#[test]
#[should_panic(
    expected = "event \"widget_added\" registered twice — each #[event] type is \
                           mounted once"
)]
fn mounting_one_event_twice_panics_at_mount_time() {
    let _ = fixture_app().mount_events(events![WidgetAdded]);
}

/// Task 7, Step 1: a stream route names the events it carries, so a generated
/// client can give `subscribe()` a discriminated union of exactly those
/// payloads instead of "subscribe to anything" (`RouteInfo.stream` alone).
///
/// The names come from `<Ev as Event>::NAME` (amendment B1), not from a
/// snake_case rule recomputed in `route.rs` — which is why `WidgetAdded`
/// renders as `widget_added` here without `#[route]` knowing that rule at
/// all. `emits` is empty on the three non-stream routes; the pinned golden
/// (`contract_json_shape_is_pinned`) carries that half.
#[test]
fn a_stream_route_declares_the_events_it_carries() {
    let app = fixture_app();
    let c = app.describe();
    let feed = c.routes.iter().find(|r| r.name == "widget_feed").unwrap();
    assert!(feed.stream);
    // Ruling 46: each element carries BOTH names, so this pins the pairing
    // itself — a payload name silently copied from the event name (or the
    // reverse) fails here, which is the whole reason the pair exists.
    assert_eq!(
        feed.emits,
        vec![
            EmittedEvent {
                event: "widget_added",
                payload: "WidgetAdded",
            },
            EmittedEvent {
                event: "widget_removed",
                payload: "WidgetRemoved",
            },
        ]
    );
    let plain: Vec<&str> = c
        .routes
        .iter()
        .filter(|r| !r.stream)
        .map(|r| r.name)
        .collect();
    // The floor (task 7 review, Minor 4): the loop below asserts a negative,
    // so an empty `plain` would pass it without checking anything.
    assert_eq!(
        plain,
        vec!["create_widget", "list_widgets", "get_org_widget"],
        "the fixture's three non-stream routes, in registration order"
    );
    for r in c.routes.iter().filter(|r| !r.stream) {
        assert!(
            r.emits.is_empty(),
            "a non-stream route carries no emits: {} -> {:?}",
            r.name,
            r.emits
        );
    }
}

/// The drift oracle (spec §2/§5): the `emits(...)` declaration and the
/// handler's own `Subscription` must agree, or a client is generated for
/// events the route never sends. `App::subscription_event_names` gets the
/// second half by actually running the route's subscribe glue.
///
/// Order-sensitive on purpose. Declaration order is the contract's order —
/// it is what the emitter's union order will be — so `emits(B, A)` against
/// `of::<A>().and::<B>()` is a mismatch worth naming, not a set-equality
/// pass. The failure prints both lists, so the diagnosis is one read.
///
/// Undeclared stream routes are skipped rather than failed: `emits(...)` is
/// optional syntax, and `[]` means "not declared", not "declares nothing".
/// That makes the loop vacuously green for an app that declares none, so it
/// owes the floor below — proven by narrowing `widget_feed`'s declaration to
/// one event and watching this redden (task-7 report, Step 5).
#[test]
fn a_declared_emits_set_matches_the_handlers_subscription() {
    let mut app = fixture_app();
    // Ruling 46 widened `emits`'s element to a two-field struct, so the
    // oracle projects the `event` half — the ONLY half a `Subscription`
    // knows, since `Subscription::of::<Ev>()` records `Event::NAME`. The
    // payload half is pinned by `a_stream_route_declares_the_events_it_carries`
    // and by the JSON golden; comparing it here would compare a name against
    // a list that never contained it.
    let declared: Vec<(&'static str, Vec<&'static str>)> = app
        .describe()
        .routes
        .iter()
        .filter(|r| r.stream && !r.emits.is_empty())
        .map(|r| (r.name, r.emits.iter().map(|e| e.event).collect()))
        .collect();
    assert_eq!(
        declared.len(),
        1,
        "the floor: this fixture has exactly one stream route with a declared \
         emits(...), so an empty loop below would be a vacuous pass — got {declared:?}"
    );
    for (name, emits) in declared {
        let actual = app
            .subscription_event_names(name)
            .expect("a stream route must expose its subscription's event names");
        assert_eq!(
            emits, actual,
            "route `{name}` declares emits{emits:?} but its Subscription names {actual:?}"
        );
    }
}

/// `to_human`'s stream-route line, pinned the same way its plain-route line
/// and its events section are (task 5's F9): `emits` is the one field on that
/// line a stream route actually fills, and nothing pinned its rendering.
#[test]
fn to_human_pins_a_stream_routes_emits() {
    let out = charpente::describe::to_human(&fixture_app().describe());
    assert!(
        out.contains(
            "  GET /widgets/feed name=widget_feed auth=false public_scope=none cross_tenant=false stream=true\n    \
             params=[] request=None response=Null error=Ref(\"ApiError\") \
             emits=[widget_added->WidgetAdded, widget_removed->WidgetRemoved]\n"
        ),
        "{out}"
    );
}

/// A declared payload joins the ROUTE's own type closure — the stream
/// route's `register_types` walks `emits(...)` too, not just params/request/
/// response/error — so a client can resolve the union even for an app that
/// never calls `mount_events`.
///
/// Deliberately NOT `fixture_app()` (pin the behaviour, not the function):
/// that app mounts both events, and `describe()` registers every mounted
/// event's payload, so asserting the two names there would stay green with
/// `#(#emit_registers)*` deleted from `expand_sse`. This app mounts none, so
/// the route's own registration is the only path either name can take.
///
/// It is also the live proof that double registration is safe in the fixture
/// app, where BOTH paths run: `TypeRegistry::insert_as` returns quietly for
/// an identical schema under a name already present, and would panic on a
/// genuine collision — `contract_json_shape_is_pinned` reaching its
/// assertion at all is that half.
#[test]
fn a_declared_emits_payload_reaches_the_closure_without_mount_events() {
    let sim = Sim::with_faults(0x2EC2_0007, FaultConfig::none());
    let app = App::open(
        Box::new(sim.disk()),
        Schema::new(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )
    .unwrap()
    .mount(routes![widget_feed]);
    let c = app.describe();
    assert!(
        c.events.is_empty(),
        "this app registers no events on purpose: {:?}",
        c.events.iter().map(|e| e.name).collect::<Vec<_>>()
    );
    let names: Vec<&str> = c.types.iter().map(|t| t.name).collect();
    assert!(names.contains(&"WidgetAdded"), "types: {names:?}");
    assert!(names.contains(&"WidgetRemoved"), "types: {names:?}");
}

/// Two of the probe's three "you cannot probe that" branches, pinned by
/// their exact text (hard rule 5; task 7 review, Minor 3). The third — a
/// handler that rejects the synthetic request — needs an unmounted route, so
/// it has its own test below. `subscription_event_names` is `pub`, and
/// `docs/GUIDE.md` §11 tells app authors to call it, so these are product
/// surface — and before this test no branch was executed by anything at all.
#[test]
fn the_drift_probe_names_a_route_it_cannot_read() {
    let mut app = fixture_app();
    assert_eq!(
        app.subscription_event_names("nope"),
        Err("no route named `nope` is mounted".to_string())
    );
    assert_eq!(
        app.subscription_event_names("create_widget"),
        Err(
            "route `create_widget` is not a stream route — only `#[route(SSE …)]` \
             returns a Subscription to read event names from"
                .to_string()
        )
    );
}

/// The probe's third branch (task 7 review, Minor 3): a subscribe handler
/// that refuses the credential-free, `"0"`-for-every-param request the probe
/// synthesizes comes back as an `Err` naming the route and quoting what the
/// handler returned — never as a silently empty name list, which a drift
/// test would read as "this route declares nothing".
///
/// What it quotes is what a request would receive: the status and the error
/// envelope's message, here `ApiError::Unauthorized`'s 401, never the Rust
/// `Debug` of the `TxError` wrapping it (`App(Unauthorized)`, which this pin
/// read before the probe rendered through the one renderer requests use).
#[test]
fn the_drift_probe_names_a_handler_that_rejected_it() {
    let sim = Sim::with_faults(0x2EC2_0009, FaultConfig::none());
    let mut app = App::open(
        Box::new(sim.disk()),
        Schema::new(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )
    .unwrap()
    .mount(routes![picky_feed]);

    assert_eq!(
        app.subscription_event_names("picky_feed"),
        Err(
            "route `picky_feed`'s subscribe handler answered 401 under the emits(...) drift \
             probe, so what it subscribes to cannot be read — a handler that rejects a \
             credential-free request with `0` for every :param is not probeable; the 401 reads: \
             authentication required — send \"Authorization: Bearer <token>\" or the \
             charpente_session cookie"
                .to_string()
        )
    );
}

/// The write guard (task 7 review, Important 4): a subscribe handler that
/// stages is refused with a named error rather than having its write
/// committed silently into the app under test.
///
/// The second half is what keeps this from being a string test. The guard's
/// message asserts the write "has just committed into this app" — because
/// `Group`'s `Drop` panics on a non-empty uncommitted group, refusing cannot
/// mean skipping the commit. So the test reads the `Session` row back: it is
/// there, exactly one, and that is the fact the message exists to disclose.
/// Delete the `if wrote` guard and the first assertion reddens; make the
/// guard skip the commit instead and the group's `Drop` panics.
#[test]
fn the_drift_probe_refuses_a_subscribe_handler_that_writes() {
    let sim = Sim::with_faults(0x2EC2_0008, FaultConfig::none());
    let mut app = App::open(
        Box::new(sim.disk()),
        Schema::new(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )
    .unwrap()
    .mount(routes![minting_feed]);

    assert_eq!(
        app.subscription_event_names("minting_feed"),
        Err(
            "route `minting_feed`'s subscribe handler staged a write, which the \
             emits(...) drift probe has just committed into this app — a probe is not \
             a request, so nothing here asked for that row; move the write out of the \
             subscribe path, or leave this route out of the drift test"
                .to_string()
        )
    );

    let sessions = app
        .db_for_tests()
        .transact(|tx| Ok::<_, ApiError>(tx.scan::<charpente::http::Session>()?.len()))
        .unwrap();
    assert_eq!(
        sessions, 1,
        "the guard reports a committed write, so the row must really be there — \
         refusing cannot mean skipping the commit"
    );
}

// ------------------------------------------------- cross-tenant capability

/// A scope type and one table scoped by it, so the cross-tenant route below
/// enumerates something real. Neither is in `fixture_app`'s `Schema`: they
/// exist for the routes in this section, which mount their own app rather
/// than change the pinned golden (same reason `minting_feed` stays out).
#[table(global)]
struct Org {
    id: Id<Org>,
    name: String,
}

#[table(scoped_by = Org)]
struct Ledger {
    id: Id<Ledger>,
    cents: u64,
}

impl charpente::Tenant for Org {
    const NAME: &'static str = "Org";
    type Who = charpente::auth::Principal;
    type Error = ApiError;
    fn authorize(
        _tx: &Tx,
        _who: &charpente::auth::Principal,
        _scope: Id<Org>,
    ) -> Result<(), ApiError> {
        Ok(())
    }
}

/// The declared capability (spec §3): the route says `cross_tenant`, so the
/// platform hands the handler the token `across_tenants` requires.
#[route(GET "/admin/revenue", public, cross_tenant)]
fn admin_revenue(tx: &mut Tx, cap: &CrossTenant) -> Result<WireU64, ApiError> {
    let total: u64 = tx
        .across_tenants::<Org>(cap)
        .scan::<Ledger>()?
        .iter()
        .map(|(_, row)| row.cents)
        .sum();
    Ok(WireU64(total))
}

/// The ordinary route beside it — without this, the assertion below could
/// not tell "exactly the declared route" from "every route".
#[route(GET "/admin/plain", public)]
fn admin_plain(tx: &mut Tx) -> Result<WireU64, ApiError> {
    Ok(WireU64(tx.scan::<Widget>()?.len() as u64))
}

/// `public(Org)` (job-tenancy spec §4): every caller may act inside the org
/// this handler opens, with no `authorize`. `describe` must name the scope,
/// or the audit "who reaches a tenant without a membership check" is a grep
/// again.
#[route(GET "/orgs/:org/ledger", public(Org))]
fn org_ledger(tx: &mut Tx, org: Id<Org>, ctx: &Ctx) -> Result<WireU64, ApiError> {
    let tx = tx.scope::<Org>(org, ctx)?;
    Ok(WireU64(tx.scan::<Ledger>()?.len() as u64))
}

/// The job half of the same declaration (spec §3): a background rollup that
/// enumerates every tenant declares the capability exactly as a route does,
/// and its `run` takes the token the dispatcher minted.
#[job(global, cross_tenant)]
struct Rollup {
    day: u64,
}

impl Rollup {
    fn run(self, tx: &mut Tx, cap: &CrossTenant) -> Result<(), JobError> {
        let _ = (self.day, tx.across_tenants::<Org>(cap).scan::<Ledger>()?);
        Ok(())
    }
}

/// A job that acts inside one org (job-tenancy spec §5): `describe` names
/// the scope it acts in.
#[job(scoped_by = Org)]
struct Reconcile {
    day: u64,
}

impl Reconcile {
    fn run(self, tx: &mut ScopedTx<'_, '_, Org>) -> Result<(), JobError> {
        let _ = (self.day, tx.scan::<Ledger>()?);
        Ok(())
    }
}

/// An event that belongs to one org (tenancy-completion spec §6).
/// `describe` names the tenant it belongs to, and that is the inventory
/// "which feeds can carry this org's events".
#[event(scoped_by = Org)]
struct LedgerPosted {
    cents: WireU64,
}

fn capability_app() -> App {
    let sim = Sim::with_faults(0x2EC2_0002, FaultConfig::none());
    App::open(
        Box::new(sim.disk()),
        Schema::new().table::<Widget>().table::<Ledger>(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )
    .unwrap()
    .mount(routes![admin_revenue, admin_plain, org_ledger])
    .mount_jobs(jobs![Rollup, Ping, Reconcile])
    .mount_events(events![WidgetAdded, LedgerPosted])
}

/// What turns "audit the cross-tenant reads" from a grep someone must
/// remember into a mechanical inventory (spec §3): `describe` names every
/// endpoint that declared the capability, and only those.
#[test]
fn describe_names_exactly_the_cross_tenant_routes() {
    let app = capability_app();
    let c = app.describe();

    let all: Vec<&str> = c.routes.iter().map(|r| r.name).collect();
    assert_eq!(
        all,
        vec!["admin_revenue", "admin_plain", "org_ledger"],
        "the floor: both routes are mounted, so the filter below is not vacuous"
    );

    let declared: Vec<&str> = c
        .routes
        .iter()
        .filter(|r| r.cross_tenant)
        .map(|r| r.name)
        .collect();
    assert_eq!(declared, vec!["admin_revenue"]);

    let plain = c.routes.iter().find(|r| r.name == "admin_plain").unwrap();
    assert!(!plain.cross_tenant);
    assert!(
        !plain.auth,
        "cross_tenant is its own flag, beside auth — not a rename of it"
    );

    // Jobs are the other surface that receives a `Tx`, so the inventory has
    // to cover them or it is not an inventory. `Ping` and the platform's own
    // `SessionSweep` are the floor: without them a `filter` over an
    // all-declared list would pass.
    let jobs: Vec<(&str, bool)> = c.jobs.iter().map(|j| (j.name, j.cross_tenant)).collect();
    assert_eq!(
        jobs,
        vec![
            ("SessionSweep", false),
            ("Rollup", true),
            ("Ping", false),
            ("Reconcile", false)
        ]
    );
}

/// The human dump carries the flag too, beside `auth` — `to_human` is the
/// agent-readable contract dump, so a field only the JSON carries is a field
/// half the readers cannot see.
#[test]
fn to_human_carries_the_cross_tenant_flag() {
    let out = charpente::describe::to_human(&capability_app().describe());
    assert!(
        out.contains(
            "  GET /admin/revenue name=admin_revenue auth=false public_scope=none cross_tenant=true stream=false\n"
        ),
        "{out}"
    );
    assert!(
        out.contains(
            "  GET /admin/plain name=admin_plain auth=false public_scope=none cross_tenant=false stream=false\n"
        ),
        "{out}"
    );
}

/// Job-tenancy spec §8: `describe` names, by type, the scope a `public(S)`
/// route admits every caller to and the scope a `scoped_by` job acts in.
/// The `None`s are the floor: without them, a field that always said "Org"
/// would pass.
#[test]
fn describe_names_every_public_scope_route_and_scoped_job() {
    let c = capability_app().describe();
    let routes: Vec<(&str, bool, Option<&str>)> = c
        .routes
        .iter()
        .map(|r| (r.name, r.auth, r.public_scope))
        .collect();
    assert_eq!(
        routes,
        vec![
            ("admin_revenue", false, None),
            ("admin_plain", false, None),
            ("org_ledger", false, Some("Org")),
        ],
        "`auth=false public_scope=none` is plain public; `public_scope=Org` admits every \
         caller to Org"
    );
    let jobs: Vec<(&str, Option<&str>)> = c.jobs.iter().map(|j| (j.name, j.scoped_by)).collect();
    assert_eq!(
        jobs,
        vec![
            ("SessionSweep", None),
            ("Rollup", None),
            ("Ping", None),
            ("Reconcile", Some("Org")),
        ]
    );
}

/// The human dump carries both, in spec §8's positions: `public_scope`
/// after `auth`, and `scoped_by` after the retry policy.
#[test]
fn to_human_carries_public_scope_and_scoped_by() {
    let out = charpente::describe::to_human(&capability_app().describe());
    for line in [
        "  GET /orgs/:org/ledger name=org_ledger auth=false public_scope=Org cross_tenant=false \
         stream=false\n",
        "  GET /admin/plain name=admin_plain auth=false public_scope=none cross_tenant=false \
         stream=false\n",
        "  Reconcile max=5 backoff_nanos=30000000000 scoped_by=Org cross_tenant=false \
         builtin=false outbound=none\n",
        "  Rollup max=5 backoff_nanos=30000000000 scoped_by=none cross_tenant=true \
         builtin=false outbound=none\n",
    ] {
        assert!(out.contains(line), "missing {line:?} in:\n{out}");
    }
}

/// Tenancy-completion spec §8: `describe` names the tenant each event
/// belongs to, by its tenant type's `NAME`, where it used to carry a key flag
/// that was always `false`. `WidgetAdded`'s `None` is the floor: without it,
/// a field that always said "Org" would pass.
#[test]
fn describe_names_every_scoped_event() {
    let c = capability_app().describe();
    let events: Vec<(&str, Option<&str>)> =
        c.events.iter().map(|e| (e.name, e.scoped_by)).collect();
    assert_eq!(
        events,
        vec![("ledger_posted", Some("Org")), ("widget_added", None)]
    );
}

/// The human dump carries it, after the payload, as the tenant's `NAME` or
/// `none`.
#[test]
fn to_human_carries_an_events_scoped_by() {
    let out = charpente::describe::to_human(&capability_app().describe());
    assert!(
        out.contains(
            "events:\n  ledger_posted payload=LedgerPosted scoped_by=Org\n  \
             widget_added payload=WidgetAdded scoped_by=none\n"
        ),
        "{out}"
    );
}

/// A tenant type whose NAME is not its identifier (tenancy-completion spec
/// §4, §8), so a listing that cut a path at its last segment would read
/// `Org` here. It is mounted by its own app, so `capability_app`'s golden is
/// untouched.
mod renamed {
    pub struct Org;

    impl charpente::Tenant for Org {
        const NAME: &'static str = "Workspace";
        type Who = charpente::auth::Principal;
        type Error = charpente::http::ApiError;
        fn authorize(
            _tx: &charpente::Tx,
            _who: &charpente::auth::Principal,
            _scope: charpente::Id<Org>,
        ) -> Result<(), charpente::http::ApiError> {
            Ok(())
        }
    }
}

#[route(GET "/workspace/ledger", public(renamed::Org))]
fn workspace_ledger(tx: &mut Tx, ctx: &Ctx) -> Result<WireU64, ApiError> {
    let tx = tx.scope::<renamed::Org>(Id::from_raw(1), ctx)?;
    Ok(WireU64(tx.scope().raw()))
}

#[job(scoped_by = renamed::Org)]
struct Audit {
    day: u64,
}

impl Audit {
    fn run(self, tx: &mut ScopedTx<'_, '_, renamed::Org>) -> Result<(), JobError> {
        let _ = (self.day, tx.scope());
        Ok(())
    }
}

/// `describe` names a route's `public_scope` and a job's `scoped_by` by the
/// tenant type's `NAME`. That is the identity the fingerprint mixes, not a
/// spelling of the type's path.
#[test]
fn describe_names_a_tenant_scope_by_its_tenant_types_name() {
    let sim = Sim::with_faults(0x2EC2_0003, FaultConfig::none());
    let app = App::open(
        Box::new(sim.disk()),
        Schema::new(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )
    .unwrap()
    .mount(routes![workspace_ledger])
    .mount_jobs(jobs![Audit]);
    let c = app.describe();
    let route = c
        .routes
        .iter()
        .find(|r| r.name == "workspace_ledger")
        .unwrap();
    assert_eq!(route.public_scope, Some("Workspace"));
    let job = c.jobs.iter().find(|j| j.name == "Audit").unwrap();
    assert_eq!(job.scoped_by, Some("Workspace"));
}

/// Scoped by Task 2's `renamed::Org`, whose `NAME` is "Workspace" and whose
/// identifier is `Org`. It is the event half of
/// `describe_names_a_tenant_scope_by_its_tenant_types_name`. Every other
/// scoped event here follows A4 (`NAME` equals the identifier), so only this
/// one tells "renders the tenant's `NAME`" from "renders the type's final
/// `::` component". Mounted by its own app, like that test's route and job, so
/// `capability_app`'s golden is untouched.
#[event(scoped_by = renamed::Org)]
struct AuditPosted {
    day: WireU64,
}

#[test]
fn describe_names_an_events_tenant_by_its_tenant_types_name() {
    let sim = Sim::with_faults(0x2EC2_000A, FaultConfig::none());
    let app = App::open(
        Box::new(sim.disk()),
        Schema::new(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )
    .unwrap()
    .mount_events(events![AuditPosted]);
    let c = app.describe();
    let events: Vec<(&str, Option<&str>)> =
        c.events.iter().map(|e| (e.name, e.scoped_by)).collect();
    assert_eq!(events, vec![("audit_posted", Some("Workspace"))]);
    let out = charpente::describe::to_human(&c);
    assert!(
        out.contains("  audit_posted payload=AuditPosted scoped_by=Workspace\n"),
        "{out}"
    );
}

// ------------------------------------------------------------- table scope

/// What turns "which tables are per-tenant" from a grep someone must
/// remember into a mechanical inventory: `describe` names every table whose
/// rows are tenant-scoped, carried straight from the registry's own
/// `TableOps::scoped` (`Db::table_scoped`'s same value), and only those.
/// `capability_app` (above) already mounts `Ledger` (`#[table(scoped_by =
/// Org)]`) beside the plain `Widget` — this is the first test to assert
/// anything about that pairing.
#[test]
fn describe_names_exactly_the_scoped_tables() {
    let app = capability_app();
    let c = app.describe();

    let scoped: Vec<&str> = c
        .tables
        .iter()
        .filter(|t| t.scoped)
        .map(|t| t.name)
        .collect();
    assert_eq!(
        scoped,
        vec!["Ledger"],
        "Ledger is the only scoped_by table this app mounts — a hard-coded \
         true or false here would pass either a vacuous or an over-eager filter"
    );

    let widget = c.tables.iter().find(|t| t.name == "Widget").unwrap();
    assert!(
        !widget.scoped,
        "Widget is a plain #[table] beside Ledger — without naming it too, \
         hard-coding `scoped: true` for every table would also pass"
    );
}

/// The human dump carries the flag too, beside `builtin` — same rationale as
/// `to_human_carries_the_cross_tenant_flag`: a field only the JSON carries is
/// a field half the readers cannot see. Found by table name rather than a
/// fixed fingerprint, since `Ledger`/`Widget`'s fingerprints are derived from
/// their field layout, not chosen here.
#[test]
fn to_human_carries_the_scoped_flag() {
    let out = charpente::describe::to_human(&capability_app().describe());

    let ledger_line = out
        .lines()
        .find(|l| l.trim_start().starts_with("Ledger "))
        .unwrap_or_else(|| panic!("no Ledger table line in:\n{out}"));
    assert!(ledger_line.contains(" scoped=true "), "{ledger_line}");

    let widget_line = out
        .lines()
        .find(|l| l.trim_start().starts_with("Widget "))
        .unwrap_or_else(|| panic!("no Widget table line in:\n{out}"));
    assert!(widget_line.contains(" scoped=false "), "{widget_line}");
}
