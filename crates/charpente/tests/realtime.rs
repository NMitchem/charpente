//! Realtime tail tests (PLAN M1 sub-project 3, task 6): the `subscriber_stall`
//! fault axis's consumer, the flagship reconnecting-client property (the PLAN
//! exit criterion — "a reconnecting realtime client provably misses no
//! events"), its off-by-one meta-test, and the determinism check.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::io;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use charpente::auth::Principal;
use charpente::event;
use charpente::http::{
    ApiError, App, AppConfig, Ctx, Cursor, EmittedEvent, Method, Request, Route, RouteDef,
    RouteGlue, Subscription,
};
use charpente::kernel::{
    ERROR_FRAME, EventFrame, EventSink, JobError, Kernel, RESHAPED_FRAME, RecordedResponses,
    SimSink, SimSinkHandle, SimSource, SinkHandles, SinkStatus, Step, TAIL_BATCH_EVENTS,
};
use charpente::repl::protocol::{Msg, decode, encode};
use charpente::repl::role::Role;
use charpente::repl::{MAX_MESSAGE_BYTES, Repl, ReplConfig};
use charpente::wire::{TypeSchema, Wire, WireU64};
use charpente::{
    DbConfig, Id, Schema, Scoped, ScopedTx, Subscribe, Tenant, Tx, Wire as WireDerive, job, jobs,
    route, routes, table,
};
use charpente_core::db::{Db, Event};
use charpente_core::event::EventRow;
use charpente_core::io::{Clock, Conn, Disk, Entropy, Net};
use charpente_core::log::WalConfig;
use charpente_core::table::{Codec, Row, View};
use charpente_sim::checks::{
    FaultReason, boot_through_fallout, classify, durable_corruption_fired, durable_refusal,
    is_fault_fallout,
};
use charpente_sim::{FaultConfig, Sim, SimEntropy, SimHost, run_universes_swarm_with};
use serde::{Deserialize, Serialize};

const SEC: u64 = 1_000_000_000;

// ------------------------------------------------------------------ events

// `n: WireU64`, not a bare `u64` (task 6): `#[event]` derives `Wire`, and a
// 64-bit integer only crosses the wire as a string. The stored `Codec` bytes
// are unchanged — `WireU64`'s codec IS `u64`'s — so only the frame JSON
// differs ({"n":"3"} rather than {"n":3}).
#[event(global)]
struct Ping {
    n: WireU64,
}

#[event(global)]
struct Pong {
    n: WireU64,
}

// Two shapes of one event under one NAME — what a deploy that edits an
// #[event]'s fields leaves behind in charpente.events. `#[event]` names an
// event by the snake_case of its TYPE name, so each pair shares a name and
// differs only in fields (payload-shapes spec §6.1–6.2).
mod shapes_v1 {
    use charpente::event;
    use charpente::wire::WireU64;

    #[event(global)]
    pub struct IncidentOpened {
        pub n: WireU64,
    }

    #[event(global)]
    pub struct Swapped {
        pub a: WireU64,
        pub b: WireU64,
    }

    #[event(global)]
    pub struct Dropped {
        pub a: WireU64,
        pub b: WireU64,
    }
}

mod shapes_v2 {
    use charpente::event;
    use charpente::wire::WireU64;

    /// v1 plus a field — the statuspage review's C-1 change.
    #[event(global)]
    pub struct IncidentOpened {
        pub n: WireU64,
        pub severity: WireU64,
    }

    /// v1 with its two same-typed fields swapped.
    #[event(global)]
    pub struct Swapped {
        pub b: WireU64,
        pub a: WireU64,
    }

    /// v1 with its trailing field dropped.
    #[event(global)]
    pub struct Dropped {
        pub a: WireU64,
    }
}

/// `#[event]` fingerprints its FIELDS, not its name. Each pair shares one
/// `NAME`, and each is one of the three things a reshaped event does when the
/// new type decodes the old bytes: an inserted field runs out of input, a
/// same-typed swap decodes with the values traded, a dropped trailing field
/// leaves bytes behind. The fingerprint is what tells them apart.
#[test]
fn an_events_fingerprint_follows_its_fields_not_its_name() {
    let mut v1 = Vec::new();
    shapes_v1::IncidentOpened { n: WireU64(4) }.encode(&mut v1);
    assert!(
        shapes_v2::IncidentOpened::decode(&mut &v1[..]).is_err(),
        "an inserted field runs out of input"
    );

    let mut v1 = Vec::new();
    shapes_v1::Swapped {
        a: WireU64(1),
        b: WireU64(101),
    }
    .encode(&mut v1);
    let swapped = shapes_v2::Swapped::decode(&mut &v1[..]).unwrap();
    assert_eq!(
        (swapped.a.0, swapped.b.0),
        (101, 1),
        "a same-typed swap decodes with the values traded, and no error anywhere"
    );

    let mut v1 = Vec::new();
    shapes_v1::Dropped {
        a: WireU64(1),
        b: WireU64(101),
    }
    .encode(&mut v1);
    let input = &mut &v1[..];
    let dropped = shapes_v2::Dropped::decode(input).unwrap();
    assert_eq!(dropped.a.0, 1);
    assert_eq!(input.len(), 8, "the dropped field's bytes are left behind");

    assert_eq!(
        <shapes_v1::IncidentOpened as Event>::NAME,
        <shapes_v2::IncidentOpened as Event>::NAME
    );
    assert_ne!(
        <shapes_v1::IncidentOpened as Event>::FINGERPRINT,
        <shapes_v2::IncidentOpened as Event>::FINGERPRINT
    );
    assert_ne!(
        <shapes_v1::Swapped as Event>::FINGERPRINT,
        <shapes_v2::Swapped as Event>::FINGERPRINT
    );
    assert_ne!(
        <shapes_v1::Dropped as Event>::FINGERPRINT,
        <shapes_v2::Dropped as Event>::FINGERPRINT
    );
    // The control: identical fields fingerprint identically whatever the
    // name — NAME carries identity, FINGERPRINT carries shape.
    assert_eq!(<Ping as Event>::FINGERPRINT, <Pong as Event>::FINGERPRINT);
}

/// Tenancy-completion spec §6 and amendment A1: a `global` event's
/// fingerprint is exactly the fold a bare `#[event]` computed, so no stored
/// row of an existing event changes shape when its declaration gains
/// `global`, and the scoped fingerprint the plan's Task 11 adds leaves global
/// events alone. The literals are FNV-1a over charpente's own rendering —
/// `"n\0WireU64\0"` and `"a\0WireU64\0b\0WireU64\0"` — and were run green at
/// the plan's base, before any event said `global`.
#[test]
fn a_global_events_fingerprint_is_the_bare_events_fingerprint() {
    assert_eq!(<Ping as Event>::FINGERPRINT, 0x9caf_3f93_9b10_65e1);
    assert_eq!(<Pong as Event>::FINGERPRINT, 0x9caf_3f93_9b10_65e1);
    assert_eq!(
        <shapes_v1::Swapped as Event>::FINGERPRINT,
        0x26f3_bd60_2fcd_ef46
    );
}

#[derive(WireDerive, Serialize, Deserialize)]
struct EmitBody {
    n: WireU64,
    key: Option<String>,
}

#[route(POST "/ping", public)]
fn emit_ping(tx: &mut Tx, body: EmitBody) -> Result<(), ApiError> {
    match body.key {
        Some(key) => tx.emit_keyed(key, Ping { n: body.n }),
        None => tx.emit(Ping { n: body.n }),
    }
    Ok(())
}

#[route(POST "/pong", public)]
fn emit_pong(tx: &mut Tx, body: EmitBody) -> Result<(), ApiError> {
    match body.key {
        Some(key) => tx.emit_keyed(key, Pong { n: body.n }),
        None => tx.emit(Pong { n: body.n }),
    }
    Ok(())
}

/// Three subscribe flavors so the flagship workload exercises the name and
/// key filter branches, not just "everything, unfiltered" (T5 left both
/// unexercised) — one mode, deterministically chosen per universe from the
/// seed, drives every (re)subscribe within that universe.
#[route(SSE "/feed-both", public, emits(Ping, Pong))]
fn feed_both(tx: &mut Tx, ctx: &Ctx, cursor: Option<Cursor>) -> Result<Subscription, ApiError> {
    let _ = (tx, ctx);
    Ok(Subscription::of::<Ping>().and::<Pong>().cursor(cursor))
}

#[route(SSE "/feed-ping", public, emits(Ping))]
fn feed_ping(tx: &mut Tx, ctx: &Ctx, cursor: Option<Cursor>) -> Result<Subscription, ApiError> {
    let _ = (tx, ctx);
    Ok(Subscription::of::<Ping>().cursor(cursor))
}

/// `emits(...)` describes WHICH event types reach the stream, not which rows
/// pass `.key(...)` — the key is a per-emit call-site value the contract
/// cannot know (same reason `EventInfo.keyed` is always false), so a keyed
/// feed declares the same two types the unkeyed one does.
#[route(SSE "/feed-keyed", public, emits(Ping, Pong))]
fn feed_keyed(tx: &mut Tx, ctx: &Ctx, cursor: Option<Cursor>) -> Result<Subscription, ApiError> {
    let _ = (tx, ctx);
    Ok(Subscription::of::<Ping>()
        .and::<Pong>()
        .key("org-a")
        .cursor(cursor))
}

// ------------------------------------------------------- reshaped events
//
// Routes over the `shapes_v1`/`shapes_v2` pairs. The v1 emitters are what the
// OLD binary served, the v2 emitters and feeds what the new one serves; one
// binary hosts both, so a mixed log is one POST away. The feeds declare no
// `emits(...)`: that list takes bare idents, these types live in modules, and
// nothing here generates a client.

#[route(POST "/v1/incident", public)]
fn emit_v1_incident(tx: &mut Tx, body: EmitBody) -> Result<(), ApiError> {
    let ev = shapes_v1::IncidentOpened { n: body.n };
    match body.key {
        Some(key) => tx.emit_keyed(key, ev),
        None => tx.emit(ev),
    }
    Ok(())
}

#[route(POST "/v2/incident", public)]
fn emit_v2_incident(tx: &mut Tx, body: EmitBody) -> Result<(), ApiError> {
    let ev = shapes_v2::IncidentOpened {
        n: body.n,
        severity: WireU64(body.n.0 % 3),
    };
    match body.key {
        Some(key) => tx.emit_keyed(key, ev),
        None => tx.emit(ev),
    }
    Ok(())
}

#[route(POST "/v1/swapped", public)]
fn emit_v1_swapped(tx: &mut Tx, body: EmitBody) -> Result<(), ApiError> {
    tx.emit(shapes_v1::Swapped {
        a: body.n,
        b: WireU64(body.n.0 + 100),
    });
    Ok(())
}

#[route(POST "/v1/dropped", public)]
fn emit_v1_dropped(tx: &mut Tx, body: EmitBody) -> Result<(), ApiError> {
    tx.emit(shapes_v1::Dropped {
        a: body.n,
        b: WireU64(body.n.0 + 100),
    });
    Ok(())
}

#[route(SSE "/v2/incidents-and-pongs", public)]
fn feed_v2_incidents_and_pongs(
    tx: &mut Tx,
    ctx: &Ctx,
    cursor: Option<Cursor>,
) -> Result<Subscription, ApiError> {
    let _ = (tx, ctx);
    Ok(Subscription::of::<shapes_v2::IncidentOpened>()
        .and::<Pong>()
        .cursor(cursor))
}

#[route(SSE "/v2/incidents-org-a", public)]
fn feed_v2_incidents_org_a(
    tx: &mut Tx,
    ctx: &Ctx,
    cursor: Option<Cursor>,
) -> Result<Subscription, ApiError> {
    let _ = (tx, ctx);
    Ok(Subscription::of::<shapes_v2::IncidentOpened>()
        .key("org-a")
        .cursor(cursor))
}

#[route(SSE "/v2/swapped", public)]
fn feed_v2_swapped(
    tx: &mut Tx,
    ctx: &Ctx,
    cursor: Option<Cursor>,
) -> Result<Subscription, ApiError> {
    let _ = (tx, ctx);
    Ok(Subscription::of::<shapes_v2::Swapped>().cursor(cursor))
}

#[route(SSE "/v2/dropped", public)]
fn feed_v2_dropped(
    tx: &mut Tx,
    ctx: &Ctx,
    cursor: Option<Cursor>,
) -> Result<Subscription, ApiError> {
    let _ = (tx, ctx);
    Ok(Subscription::of::<shapes_v2::Dropped>().cursor(cursor))
}

#[route(SSE "/pongs", public)]
fn feed_pongs(tx: &mut Tx, ctx: &Ctx, cursor: Option<Cursor>) -> Result<Subscription, ApiError> {
    let _ = (tx, ctx);
    Ok(Subscription::of::<Pong>().cursor(cursor))
}

/// Default retention (65,536): nothing in these tests may fall behind it, so
/// a gap frame or a below-floor 409 here is a bug, never an excuse.
fn shapes_app(sim: &Sim) -> io::Result<App> {
    Ok(App::open(
        Box::new(sim.disk()),
        Schema::new(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )?
    .mount(routes![
        emit_pong,
        emit_v1_incident,
        emit_v2_incident,
        emit_v1_swapped,
        emit_v1_dropped,
        feed_v2_incidents_and_pongs,
        feed_v2_incidents_org_a,
        feed_v2_swapped,
        feed_v2_dropped,
        feed_pongs
    ]))
}

/// Pushes `requests` at one instant and steps the kernel a fixed 16 times;
/// each caller's assertions on its responses and frames are what show its
/// script was serviced. Every step must succeed: the property these tests pin
/// is that nothing a subscriber does stops the node.
fn run_script(
    kernel: &mut Kernel,
    sim: &Sim,
    requests: Vec<Request>,
) -> (RecordedResponses, SinkHandles) {
    let mut source = SimSource::new(Box::new(sim.clock()), sim.now().as_nanos() + 10 * SEC);
    let t = sim.now().as_nanos();
    for request in requests {
        source.push_at(t, request);
    }
    let (responses, sinks) = (source.responses(), source.sinks());
    for _ in 0..16 {
        if let Err(e) = kernel.step(&mut source) {
            panic!("no step may fail — nothing a subscriber does stops the node: {e}");
        }
    }
    (responses, sinks)
}

/// Emits `(path, n, key)` in order on a fresh node — the mixed log a
/// reshaping deploy leaves — then restarts it (the deploy) and returns the new
/// kernel with every retained event row, so each test builds its cursors and
/// expected frames from real ids and seqs.
fn mixed_log(seed: u64, emits: &[(&str, u64, Option<&str>)]) -> (Sim, Kernel, Vec<EventRow>) {
    let sim = Sim::with_faults(seed, FaultConfig::none());
    let mut kernel = Kernel::new(shapes_app(&sim).unwrap(), Box::new(sim.clock())).unwrap();
    let requests = emits
        .iter()
        .map(|&(path, n, key)| {
            Request::new(Method::Post, path).json(&EmitBody {
                n: WireU64(n),
                key: key.map(str::to_string),
            })
        })
        .collect();
    let (responses, _) = run_script(&mut kernel, &sim, requests);
    assert!(
        statuses(&responses).iter().all(|s| *s == 200),
        "every emit commits: {:?}",
        statuses(&responses)
    );
    sim.restart();
    drop(kernel);
    let kernel = Kernel::new(shapes_app(&sim).unwrap(), Box::new(sim.clock())).unwrap();
    let rows: Vec<EventRow> = kernel
        .app()
        .db()
        .snapshot()
        .unwrap()
        .scan_from(0, 64)
        .unwrap();
    assert_eq!(rows.len(), emits.len(), "every emit is one retained row");
    (sim, kernel, rows)
}

fn statuses(responses: &RecordedResponses) -> Vec<u16> {
    responses.lock().unwrap().iter().map(|(s, _)| *s).collect()
}

fn frames_of(handle: &SimSinkHandle) -> Vec<(u64, u64, String, String)> {
    handle
        .frames()
        .iter()
        .map(|f| (f.id, f.seq, f.name.to_string(), f.json.clone()))
        .collect()
}

fn cursor_of(row: &EventRow) -> String {
    format!("{}.{}", row.id.raw(), row.seq)
}

/// The terminal frame for a foreign row, spelled out here rather than taken
/// from the product so the comparison pins the spec's wording (§3).
fn reshaped_frame(row: &EventRow) -> (u64, u64, String, String) {
    (
        row.id.raw(),
        row.seq,
        "charpente.reshaped".to_string(),
        format!(
            r#"{{"event":"{}","id":{},"fix":"reconnect without a cursor, wait for the stream to open, then read current state from the same node, holding every frame that arrives until that read is installed and then applying the held frames and every later one over it in order, idempotently"}}"#,
            row.name,
            row.id.raw()
        ),
    )
}

/// The reshaped 409's text (spec §3 as amended, A5), spelled out.
fn reshaped_409(row: &EventRow, compiled: u64) -> String {
    let recorded = match row.shape {
        Some(shape) => format!("{shape:#x}"),
        None => "none — written before event shapes were tagged".to_string(),
    };
    format!(
        "cursor {} sits on an \"{}\" event written under a different shape of that event (recorded \
         {recorded}; this binary {compiled:#x}) — this binary will not decode it; reconnect \
         without a cursor, wait for the stream to open, then read current state from the same \
         node, holding every frame that arrives until that read is installed and then applying the \
         held frames and every later one over it in order, idempotently",
        cursor_of(row),
        row.name
    )
}

/// A `Response::error` body's `error` text.
fn error_text(body: &[u8]) -> String {
    let v: serde_json::Value = serde_json::from_slice(body).expect("an error body is JSON");
    v["error"]
        .as_str()
        .expect("an error body has an error string")
        .to_string()
}

/// A record as a pre-shape binary wrote it: one `0x03` emit, no shape.
fn legacy_emit_record(event: &str, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    1u32.encode(&mut out);
    out.push(3);
    event.to_string().encode(&mut out);
    payload.to_vec().encode(&mut out);
    None::<String>.encode(&mut out);
    out
}

/// A record exactly as a shaped binary writes it: one `0x09` emit.
fn shaped_emit_record(event: &str, payload: &[u8], shape: u64) -> Vec<u8> {
    let mut out = Vec::new();
    1u32.encode(&mut out);
    out.push(9);
    event.to_string().encode(&mut out);
    payload.to_vec().encode(&mut out);
    None::<String>.encode(&mut out);
    shape.encode(&mut out);
    out
}

/// Applies one shipped record to a live kernel's store, the way a follower
/// applies a frame — the only way a test can plant bytes the typed API would
/// never write.
fn apply_record(kernel: &mut Kernel, record: Vec<u8>) {
    let db = kernel.app_mut_for_tests().db_for_tests();
    let seq = db.last_applied().map_or(0, |s| s + 1);
    db.apply_shipped(&[(seq, record)]).unwrap();
}

/// Spec §6.1 — the statuspage review's C-1 probe, as a sim test. The old
/// binary emitted `incident_opened` in its v1 shape between pongs; the node
/// restarted as the new binary, whose `incident_opened` gained a field; a
/// client resumes from the pong before it. At c28250e this stopped the kernel
/// (`kernel.step` = Err, "needed 8 bytes for u64, 0 left") and every restart
/// stopped it again. Now the pong before the foreign row is delivered, the
/// foreign row ends the stream with one named frame carrying its own id, the
/// pong after it never arrives, a second tail on the same node keeps flowing,
/// and a client that ignored the frame and reconnects from its id is refused
/// by name.
#[test]
fn a_reshaped_event_ends_its_tail_by_name_and_the_node_keeps_serving() {
    let (sim, mut kernel, rows) = mixed_log(
        0x5A9E_0201,
        &[
            ("/pong", 0, None),
            ("/pong", 1, None),
            ("/v1/incident", 2, None),
            ("/pong", 3, None),
        ],
    );
    let (responses, sinks) = run_script(
        &mut kernel,
        &sim,
        vec![
            Request::new(
                Method::Get,
                format!("/v2/incidents-and-pongs?cursor={}", cursor_of(&rows[0])),
            ),
            Request::new(Method::Get, "/pongs"),
            Request::new(Method::Post, "/pong").json(&EmitBody {
                n: WireU64(4),
                key: None,
            }),
        ],
    );
    assert_eq!(statuses(&responses), vec![200, 200, 200]);
    let handles = sinks.lock().unwrap();
    assert_eq!(
        frames_of(&handles[0]),
        vec![
            (
                rows[1].id.raw(),
                rows[1].seq,
                "pong".to_string(),
                r#"{"n":"1"}"#.to_string()
            ),
            reshaped_frame(&rows[2]),
        ]
    );
    assert_eq!(
        frames_of(&handles[1])
            .into_iter()
            .map(|f| f.3)
            .collect::<Vec<_>>(),
        vec![r#"{"n":"4"}"#.to_string()],
        "a second tail on the same node keeps flowing"
    );
    drop(handles);

    let (responses, _) = run_script(
        &mut kernel,
        &sim,
        vec![Request::new(
            Method::Get,
            format!("/v2/incidents-and-pongs?cursor={}", cursor_of(&rows[2])),
        )],
    );
    let responses = responses.lock().unwrap().clone();
    assert_eq!(responses.len(), 1);
    assert_eq!(responses[0].0, 409);
    assert_eq!(
        error_text(&responses[0].1),
        reshaped_409(&rows[2], <shapes_v2::IncidentOpened as Event>::FINGERPRINT)
    );
}

/// Spec §6.2 — the silent misdecodes. At c28250e this delivered
/// {"b":"1","a":"101"} for an event whose a was 1 and whose b was 101, with no
/// error anywhere.
#[test]
fn a_same_typed_field_swap_is_refused_not_delivered_with_the_values_traded() {
    let (sim, mut kernel, rows) =
        mixed_log(0x5A9E_0202, &[("/pong", 0, None), ("/v1/swapped", 1, None)]);
    let (_, sinks) = run_script(
        &mut kernel,
        &sim,
        vec![Request::new(
            Method::Get,
            format!("/v2/swapped?cursor={}", cursor_of(&rows[0])),
        )],
    );
    assert_eq!(
        frames_of(&sinks.lock().unwrap()[0]),
        vec![reshaped_frame(&rows[1])]
    );
}

/// Spec §6.2 — at c28250e this delivered {"a":"1"}: the event's b silently
/// gone, and the eight bytes it left behind never checked.
#[test]
fn a_dropped_trailing_field_is_refused_not_delivered_short() {
    let (sim, mut kernel, rows) =
        mixed_log(0x5A9E_0203, &[("/pong", 0, None), ("/v1/dropped", 1, None)]);
    let (_, sinks) = run_script(
        &mut kernel,
        &sim,
        vec![Request::new(
            Method::Get,
            format!("/v2/dropped?cursor={}", cursor_of(&rows[0])),
        )],
    );
    assert_eq!(
        frames_of(&sinks.lock().unwrap()[0]),
        vec![reshaped_frame(&rows[1])]
    );
}

/// The key filter runs BEFORE the shape check, on both paths — proven both
/// ways. Another tenant's reshaped event must not end this tenant's tail, and
/// a cursor on it must not be refused, since this subscription would never
/// deliver that row either way; a SAME-key foreign row is the opposite case —
/// it DOES end the tail, and a cursor on it IS refused. An unkeyed
/// subscription passes every key, so org-b's foreign row refuses it too.
#[test]
fn another_keys_reshaped_event_neither_ends_nor_refuses_this_tail() {
    let (sim, mut kernel, rows) = mixed_log(
        0x5A9E_0204,
        &[
            ("/pong", 0, None),
            ("/v1/incident", 1, Some("org-b")),
            ("/v2/incident", 2, Some("org-a")),
            ("/v1/incident", 3, Some("org-a")),
        ],
    );
    assert_ne!(
        rows[1].shape,
        Some(<shapes_v2::IncidentOpened as Event>::FINGERPRINT),
        "the precondition: org-b's row is foreign to this binary's incident_opened"
    );
    let expected = vec![(
        rows[2].id.raw(),
        rows[2].seq,
        "incident_opened".to_string(),
        r#"{"n":"2","severity":"2"}"#.to_string(),
    )];
    let mut with_own_reshaped = expected.clone();
    with_own_reshaped.push(reshaped_frame(&rows[3]));

    let (responses, sinks) = run_script(
        &mut kernel,
        &sim,
        vec![
            Request::new(
                Method::Get,
                format!("/v2/incidents-org-a?cursor={}", cursor_of(&rows[0])),
            ),
            Request::new(
                Method::Get,
                format!("/v2/incidents-org-a?cursor={}", cursor_of(&rows[1])),
            ),
        ],
    );
    assert_eq!(
        statuses(&responses),
        vec![200, 200],
        "neither subscribe is refused"
    );
    let handles = sinks.lock().unwrap();
    assert_eq!(
        frames_of(&handles[0]),
        with_own_reshaped,
        "org-b's foreign row did not end org-a's tail, but org-a's own foreign row does"
    );
    assert_eq!(
        frames_of(&handles[1]),
        with_own_reshaped,
        "a cursor on org-b's foreign row resumes, and still ends at org-a's own foreign row"
    );
    drop(handles);

    let (responses, _) = run_script(
        &mut kernel,
        &sim,
        vec![
            Request::new(
                Method::Get,
                format!("/v2/incidents-org-a?cursor={}", cursor_of(&rows[3])),
            ),
            Request::new(
                Method::Get,
                format!("/v2/incidents-and-pongs?cursor={}", cursor_of(&rows[1])),
            ),
        ],
    );
    let responses = responses.lock().unwrap().clone();
    assert_eq!(responses.len(), 2);
    assert_eq!(responses[0].0, 409, "a same-key foreign cursor is refused");
    assert_eq!(
        error_text(&responses[0].1),
        reshaped_409(&rows[3], <shapes_v2::IncidentOpened as Event>::FINGERPRINT)
    );
    assert_eq!(
        responses[1].0, 409,
        "an unkeyed subscription passes every key, so org-b's foreign row refuses it too"
    );
    assert_eq!(
        error_text(&responses[1].1),
        reshaped_409(&rows[1], <shapes_v2::IncidentOpened as Event>::FINGERPRINT)
    );
}

/// Spec §3: a cursor on a foreign row of an event the subscription does NOT
/// carry is not refused. The foreign row is a `swapped` one, not an
/// `incident_opened` one: `#[event]` fingerprints hash fields, not names, and
/// `shapes_v1::IncidentOpened`'s single `{n: WireU64}` shares Pong's exact
/// shape — a name-blind check couldn't be told apart from a correct one by
/// that row. `shapes_v1::Swapped`'s `{a, b}` can.
#[test]
fn a_cursor_on_a_foreign_row_of_an_unsubscribed_event_is_not_refused() {
    let (sim, mut kernel, rows) =
        mixed_log(0x5A9E_0205, &[("/v1/swapped", 0, None), ("/pong", 1, None)]);
    assert_ne!(
        rows[0].shape,
        Some(<Pong as Event>::FINGERPRINT),
        "the precondition: a name-blind check would refuse this row"
    );
    let (responses, sinks) = run_script(
        &mut kernel,
        &sim,
        vec![Request::new(
            Method::Get,
            format!("/pongs?cursor={}", cursor_of(&rows[0])),
        )],
    );
    assert_eq!(statuses(&responses), vec![200]);
    assert_eq!(
        frames_of(&sinks.lock().unwrap()[0]),
        vec![(
            rows[1].id.raw(),
            rows[1].seq,
            "pong".to_string(),
            r#"{"n":"1"}"#.to_string()
        )]
    );
}

/// Spec §6.6: a row from a pre-shape binary is foreign on both paths — even
/// when its bytes are ones the v2 type WOULD decode. The rule is the recorded
/// shape, not whether a decode happens to succeed.
#[test]
fn a_pre_shape_event_is_foreign_on_both_paths_even_when_its_bytes_would_decode() {
    let (sim, mut kernel, rows) = mixed_log(0x5A9E_0206, &[("/pong", 0, None)]);
    let mut v2_bytes = Vec::new();
    shapes_v2::IncidentOpened {
        n: WireU64(9),
        severity: WireU64(1),
    }
    .encode(&mut v2_bytes);
    apply_record(
        &mut kernel,
        legacy_emit_record("incident_opened", &v2_bytes),
    );
    let legacy: Vec<EventRow> = kernel
        .app()
        .db()
        .snapshot()
        .unwrap()
        .scan_from(1, 1)
        .unwrap();
    assert_eq!(
        legacy[0].shape, None,
        "a 0x03 record materializes with no shape"
    );

    let (_, sinks) = run_script(
        &mut kernel,
        &sim,
        vec![Request::new(
            Method::Get,
            format!("/v2/incidents-and-pongs?cursor={}", cursor_of(&rows[0])),
        )],
    );
    assert_eq!(
        frames_of(&sinks.lock().unwrap()[0]),
        vec![reshaped_frame(&legacy[0])]
    );

    let (responses, _) = run_script(
        &mut kernel,
        &sim,
        vec![Request::new(
            Method::Get,
            format!("/v2/incidents-and-pongs?cursor={}", cursor_of(&legacy[0])),
        )],
    );
    let responses = responses.lock().unwrap().clone();
    assert_eq!(responses[0].0, 409);
    assert_eq!(
        error_text(&responses[0].1),
        reshaped_409(
            &legacy[0],
            <shapes_v2::IncidentOpened as Event>::FINGERPRINT
        )
    );
}

/// Spec §6.5 (amendment A14): the shape matches but the bytes do not decode,
/// so the text names both causes the fingerprint leaves open — corrupt bytes,
/// or a change inside a type a field holds — and both fixes. The frame has no
/// `id:` on the wire (serve.rs's `encode_frame`), so the client's cursor stays
/// before the row, and each reconnect fails at the same row, the way a corrupt
/// table row answers 500 on every read.
#[test]
fn a_corrupt_payload_under_a_matching_shape_fails_its_tail_by_name() {
    let (sim, mut kernel, rows) = mixed_log(0x5A9E_0301, &[("/pong", 0, None)]);
    let v2 = <shapes_v2::IncidentOpened as Event>::FINGERPRINT;
    // v2's payload is 16 bytes; 8 is a truncation under a matching shape.
    apply_record(
        &mut kernel,
        shaped_emit_record("incident_opened", &[0u8; 8], v2),
    );
    let expected_error = format!(
        "event incident_opened failed to decode although its recorded shape matches this \
         binary ({v2:#x}): row decode: needed 8 bytes for u64, 0 left — schema drift or \
         corruption — either a type one of its fields holds changed, which the fingerprint \
         cannot see (GUIDE §12), or the stored bytes are corrupt; if such a type changed, \
         restore that type's old definition and put the new one in a new #[event] type under \
         a new name (GUIDE §11); otherwise restore this node from its archive (GUIDE §15) or \
         re-seed it from its primary (GUIDE §10)"
    );
    for _ in 0..2 {
        let (_, sinks) = run_script(
            &mut kernel,
            &sim,
            vec![Request::new(
                Method::Get,
                format!("/v2/incidents-and-pongs?cursor={}", cursor_of(&rows[0])),
            )],
        );
        let frames = frames_of(&sinks.lock().unwrap()[0]);
        assert_eq!(frames.len(), 1, "{frames:?}");
        assert_eq!(frames[0].2, "charpente.error");
        let body: serde_json::Value = serde_json::from_str(&frames[0].3).unwrap();
        assert_eq!(body["event"], "incident_opened");
        assert_eq!(body["id"], 1);
        assert_eq!(
            body["error"],
            expected_error.as_str(),
            "the same named error on every reconnect"
        );
    }
    let (responses, _) = run_script(
        &mut kernel,
        &sim,
        vec![Request::new(Method::Post, "/pong").json(&EmitBody {
            n: WireU64(5),
            key: None,
        })],
    );
    assert_eq!(statuses(&responses), vec![200], "the node keeps serving");
}

/// `ev_json` checks trailing bytes: a matching shape with bytes left over is a
/// named decode failure too, never a silent short decode.
#[test]
fn leftover_bytes_under_a_matching_shape_fail_the_tail_by_name() {
    let (sim, mut kernel, rows) = mixed_log(0x5A9E_0302, &[("/pong", 0, None)]);
    let v2 = <shapes_v2::IncidentOpened as Event>::FINGERPRINT;
    apply_record(
        &mut kernel,
        shaped_emit_record("incident_opened", &[0u8; 24], v2),
    );
    let (_, sinks) = run_script(
        &mut kernel,
        &sim,
        vec![Request::new(
            Method::Get,
            format!("/v2/incidents-and-pongs?cursor={}", cursor_of(&rows[0])),
        )],
    );
    let frames = frames_of(&sinks.lock().unwrap()[0]);
    assert_eq!(frames.len(), 1, "{frames:?}");
    assert_eq!(frames[0].2, "charpente.error");
    let body: serde_json::Value = serde_json::from_str(&frames[0].3).unwrap();
    assert_eq!(body["event"], "incident_opened");
    assert_eq!(body["id"], 1);
    assert_eq!(
        body["error"],
        format!(
            "event incident_opened failed to decode although its recorded shape matches this \
             binary ({v2:#x}): 8 bytes left over after the payload — either a type one of its \
             fields holds changed, which the fingerprint cannot see (GUIDE §12), or the stored \
             bytes are corrupt; if such a type changed, restore that type's old definition and \
             put the new one in a new #[event] type under a new name (GUIDE §11); otherwise \
             restore this node from its archive (GUIDE §15) or re-seed it from its primary \
             (GUIDE §10)"
        )
        .as_str()
    );
}

/// Review focus 5 (spec amendment A11): a stored `EventRow` whose own bytes do
/// not decode fails its tail, not the node. Planted through a shipped `0x04`
/// migrate-put — the decode-free raw write — over a real row id, so the
/// counter says the row exists and the tail reaches it.
#[test]
fn a_corrupt_event_row_fails_its_tail_not_the_node() {
    let (sim, mut kernel, rows) = mixed_log(0x5A9E_0303, &[("/pong", 0, None), ("/pong", 1, None)]);
    plant_corrupt_event_row(&mut kernel, rows[1].id.raw());
    let (_, sinks) = run_script(
        &mut kernel,
        &sim,
        vec![Request::new(
            Method::Get,
            format!("/pongs?cursor={}", cursor_of(&rows[0])),
        )],
    );
    let frames = frames_of(&sinks.lock().unwrap()[0]);
    assert_eq!(frames.len(), 1, "{frames:?}");
    assert_eq!(frames[0].2, "charpente.error");
    let body: serde_json::Value = serde_json::from_str(&frames[0].3).unwrap();
    assert_eq!(body["id"], rows[1].id.raw());
    assert!(
        body.get("event").is_none(),
        "an envelope-corrupt row's own name is unreadable — no `event` key, per GUIDE §11: {body}"
    );
    assert_eq!(
        body["error"],
        format!(
            "event rows from id {} could not be decoded: row decode: needed 8 bytes for u64, 3 \
             left — schema drift or corruption — the stored bytes are corrupt; restore this node \
             from its archive (GUIDE §15) or re-seed it from its primary (GUIDE §10)",
            rows[1].id.raw()
        )
        .as_str()
    );
}

/// Overwrites stored event row `id` with 3 bytes no `EventRow` decodes, through
/// a shipped `0x04` migrate-put — the decode-free raw write — so the counter
/// still says the row exists.
fn plant_corrupt_event_row(kernel: &mut Kernel, id: u64) {
    let mut record = Vec::new();
    1u32.encode(&mut record);
    record.push(4);
    "charpente.events".to_string().encode(&mut record);
    id.encode(&mut record);
    vec![0xFFu8; 3].encode(&mut record);
    apply_record(kernel, record);
}

/// GUIDE §11: the envelope holds a row's name and key, so a row whose own
/// bytes do not decode ends every tail whose scan reaches it, whatever it
/// subscribes to — and the good rows read in the same batch before it are
/// withheld: the frame's `id` is the first of them.
#[test]
fn an_envelope_corrupt_row_withholds_its_batch_and_ends_every_tail_reaching_it() {
    let (sim, mut kernel, rows) = mixed_log(
        0x5A9E_0308,
        &[("/pong", 0, None), ("/pong", 1, None), ("/pong", 2, None)],
    );
    plant_corrupt_event_row(&mut kernel, rows[2].id.raw());
    let (_, sinks) = run_script(
        &mut kernel,
        &sim,
        vec![
            Request::new(
                Method::Get,
                format!("/pongs?cursor={}", cursor_of(&rows[0])),
            ),
            Request::new(
                Method::Get,
                format!("/v2/dropped?cursor={}", cursor_of(&rows[0])),
            ),
        ],
    );
    let handles = sinks.lock().unwrap();
    assert_eq!(handles.len(), 2);
    for handle in handles.iter() {
        let frames = frames_of(handle);
        assert_eq!(
            frames.len(),
            1,
            "pong 1 is withheld with its batch: {frames:?}"
        );
        assert_eq!(frames[0].2, "charpente.error");
        let body: serde_json::Value = serde_json::from_str(&frames[0].3).unwrap();
        assert_eq!(body["id"], rows[1].id.raw());
    }
}

/// A kernel with one tail registered from a cursor on row 1 of five large
/// rows, over a cache made cold by a restart: the tail's next scan needs row
/// 2's page, which nothing since the restart has read.
fn tail_over_a_cold_cache(seed: u64) -> (Sim, Kernel, SimSource, SinkHandles, Vec<EventRow>) {
    let sim = Sim::with_faults(seed, FaultConfig::none());
    let mut kernel = Kernel::new(shapes_app(&sim).unwrap(), Box::new(sim.clock())).unwrap();
    for _ in 0..5 {
        apply_record(&mut kernel, shaped_emit_record("filler", &[0u8; 6000], 0));
    }
    let rows: Vec<EventRow> = kernel
        .app()
        .db()
        .snapshot()
        .unwrap()
        .scan_from(0, 8)
        .unwrap();
    assert_eq!(rows.len(), 5);
    // Checkpointed, so the reopen replays nothing and reads no event page.
    kernel
        .app_mut_for_tests()
        .db_for_tests()
        .checkpoint()
        .unwrap();
    sim.restart();
    drop(kernel);
    let mut kernel = Kernel::new(shapes_app(&sim).unwrap(), Box::new(sim.clock())).unwrap();
    let mut source = SimSource::new(Box::new(sim.clock()), sim.now().as_nanos() + 10 * SEC);
    source.push_at(
        sim.now().as_nanos(),
        Request::new(
            Method::Get,
            format!("/pongs?cursor={}", cursor_of(&rows[1])),
        ),
    );
    let sinks = source.sinks();
    assert!(matches!(kernel.step(&mut source), Ok(Step::Worked)));
    assert_eq!(
        sinks.lock().unwrap().len(),
        1,
        "the subscribe registered a tail"
    );
    (sim, kernel, source, sinks, rows)
}

/// The corrupt-row arm fails a tail only for the codec's own `InvalidData`: a
/// store failure the tail's scan reaches is the platform's, and it stops the
/// kernel instead of ending one tail with `charpente.error`. Three twins of
/// one state. With no crash, the step after the subscribe services that tail
/// and nothing else. With the crash, a probe shows the tail's scan is the
/// read that reaches the disk (a failed read latches redb, so the probe gets
/// a twin of its own). And the run under test, whose cached reads before the
/// scan succeed, stops on exactly the scan's error.
#[test]
fn a_store_failure_at_the_tail_scan_stops_the_kernel_not_just_the_tail() {
    let (_, mut twin, mut twin_source, _, _) = tail_over_a_cold_cache(0x5A9E_0309);
    assert!(matches!(twin.step(&mut twin_source), Ok(Step::Worked)));
    assert!(
        matches!(twin.step(&mut twin_source), Ok(Step::Idle { .. })),
        "the step after the subscribe is the tail's, and the only one due"
    );

    let (sim, probe, _, _, rows) = tail_over_a_cold_cache(0x5A9E_0309);
    sim.crash_disk_now();
    let scan = probe
        .app()
        .db()
        .snapshot()
        .unwrap()
        .scan_from::<EventRow>(rows[2].id.raw(), TAIL_BATCH_EVENTS)
        .err()
        .expect("the tail's scan reaches the crashed disk");
    assert_ne!(scan.kind(), io::ErrorKind::InvalidData, "{scan}");

    let (sim, mut kernel, mut source, sinks, _) = tail_over_a_cold_cache(0x5A9E_0309);
    sim.crash_disk_now();
    let db = kernel.app().db();
    assert_eq!(
        db.next_event_id().unwrap(),
        5,
        "the ready set's read is served from cache"
    );
    assert_eq!(db.event_floor().unwrap(), 0, "so is the floor read");
    let Err(err) = kernel.step(&mut source) else {
        panic!("a store failure at the tail's scan must stop the kernel");
    };
    assert_eq!(
        err.to_string(),
        scan.to_string(),
        "it stopped on the scan's own error"
    );
    let handle = sinks.lock().unwrap()[0].clone();
    assert!(handle.frames().is_empty(), "{:?}", handle.frames());
    assert!(
        !handle.closed(),
        "the tail was not ended as if the row were corrupt"
    );
}

/// A cursor ON a row whose own bytes do not decode gets a 500 that names the
/// corruption and the fix, not the retryable storage-failure 500: every retry
/// reads the same bytes. The envelope holds no app-defined type, so corrupt
/// bytes are its only cause.
#[test]
fn a_cursor_on_a_corrupt_event_row_is_refused_naming_the_corruption() {
    let (sim, mut kernel, rows) = mixed_log(0x5A9E_0304, &[("/pong", 0, None), ("/pong", 1, None)]);
    plant_corrupt_event_row(&mut kernel, rows[1].id.raw());
    let (responses, sinks) = run_script(
        &mut kernel,
        &sim,
        vec![Request::new(
            Method::Get,
            format!("/pongs?cursor={}", cursor_of(&rows[1])),
        )],
    );
    let responses = responses.lock().unwrap().clone();
    assert_eq!(responses.len(), 1, "{responses:?}");
    assert_eq!(responses[0].0, 500);
    assert_eq!(
        error_text(&responses[0].1),
        format!(
            "cursor {} sits on a stored event row that could not be decoded: row decode: \
             needed 8 bytes for u64, 3 left — schema drift or corruption — the stored bytes \
             are corrupt; restore this node from its archive (GUIDE §15) or re-seed it from \
             its primary (GUIDE §10)",
            cursor_of(&rows[1])
        )
    );
    assert!(sinks.lock().unwrap().is_empty(), "no stream was opened");
}

/// The retention floor is read from the oldest row's KEY, never its bytes,
/// so a corrupt row there fails only a tail whose scan reaches it (spec
/// amendment A11) — here one registered on the empty table, so its first
/// scan starts at the floor. A tail subscribed without a cursor starts past
/// the row, never meets it, and flows.
#[test]
fn a_corrupt_event_row_at_the_retention_floor_fails_only_the_tail_that_reaches_it() {
    let sim = Sim::with_faults(0x5A9E_0305, FaultConfig::none());
    let mut kernel = Kernel::new(shapes_app(&sim).unwrap(), Box::new(sim.clock())).unwrap();
    let (_, early) = run_script(&mut kernel, &sim, vec![Request::new(Method::Get, "/pongs")]);
    let mut pong = Vec::new();
    Pong { n: WireU64(0) }.encode(&mut pong);
    apply_record(
        &mut kernel,
        shaped_emit_record("pong", &pong, <Pong as Event>::FINGERPRINT),
    );
    plant_corrupt_event_row(&mut kernel, 0);
    let (responses, late) = run_script(
        &mut kernel,
        &sim,
        vec![
            Request::new(Method::Get, "/pongs"),
            Request::new(Method::Post, "/pong").json(&EmitBody {
                n: WireU64(1),
                key: None,
            }),
        ],
    );
    assert_eq!(
        statuses(&responses),
        vec![200, 200],
        "the node keeps serving"
    );

    let frames = frames_of(&early.lock().unwrap()[0]);
    assert_eq!(frames.len(), 1, "{frames:?}");
    assert_eq!(frames[0].2, "charpente.error");
    let body: serde_json::Value = serde_json::from_str(&frames[0].3).unwrap();
    assert_eq!(body["id"], 0);
    assert_eq!(
        body["error"],
        "event rows from id 0 could not be decoded: row decode: needed 8 bytes for u64, 3 left \
         — schema drift or corruption — the stored bytes are corrupt; restore this node from \
         its archive (GUIDE §15) or re-seed it from its primary (GUIDE §10)"
    );

    let emitted: Vec<EventRow> = kernel
        .app()
        .db()
        .snapshot()
        .unwrap()
        .scan_from(1, 8)
        .unwrap();
    assert_eq!(
        frames_of(&late.lock().unwrap()[0]),
        vec![(
            1,
            emitted[0].seq,
            "pong".to_string(),
            r#"{"n":"1"}"#.to_string()
        )],
        "a tail that starts past the corrupt row never meets it"
    );
}

/// Retention's GC reads the floor from keys too, and deletes without decoding,
/// so a corrupt row at the floor is collected like any other once it falls
/// out of retention, instead of failing every checkpoint from then on.
#[test]
fn a_corrupt_event_row_at_the_floor_is_collected_by_retention() {
    let sim = Sim::with_faults(0x5A9E_0306, FaultConfig::none());
    let app = App::open(
        Box::new(sim.disk()),
        Schema::new(),
        Box::new(sim.entropy()),
        AppConfig {
            db: DbConfig {
                event_retention: 2,
                checkpoint_every: 1,
                ..DbConfig::default()
            },
            ..AppConfig::default()
        },
    )
    .unwrap()
    .mount(routes![emit_pong, feed_pongs]);
    let mut kernel = Kernel::new(app, Box::new(sim.clock())).unwrap();
    let emit = |n: u64| {
        Request::new(Method::Post, "/pong").json(&EmitBody {
            n: WireU64(n),
            key: None,
        })
    };
    let (responses, _) = run_script(&mut kernel, &sim, vec![emit(0), emit(1)]);
    assert_eq!(statuses(&responses), vec![200, 200]);
    plant_corrupt_event_row(&mut kernel, 0);
    // The third event takes the floor to 1: the checkpoint after this commit
    // collects the corrupt row 0.
    let (responses, _) = run_script(&mut kernel, &sim, vec![emit(2)]);
    assert_eq!(statuses(&responses), vec![200]);
    let db = kernel.app_mut_for_tests().db_for_tests();
    db.checkpoint()
        .expect("a checkpoint over a corrupt row at the floor succeeds");
    assert_eq!(
        db.event_floor().unwrap(),
        1,
        "the corrupt row was collected"
    );
    let retained: Vec<u64> = db
        .snapshot()
        .unwrap()
        .scan_from::<EventRow>(0, 8)
        .unwrap()
        .iter()
        .map(|row| row.id.raw())
        .collect();
    assert_eq!(retained, vec![1, 2]);
}

/// A hand-written event whose `NAME` holds a quote: `Event::NAME` is
/// hand-writable, so the terminal frames must build their JSON as JSON, not
/// by pasting the name between quotes.
#[derive(Serialize)]
struct Quoted {
    n: u64,
}

impl Codec for Quoted {
    fn encode(&self, out: &mut Vec<u8>) {
        self.n.encode(out);
    }
    fn decode(input: &mut &[u8]) -> io::Result<Quoted> {
        Ok(Quoted {
            n: u64::decode(input)?,
        })
    }
}

impl Event for Quoted {
    const NAME: &'static str = "say \"hi\"";
    const FINGERPRINT: u64 = 0x7E57_0A11;
}

#[route(SSE "/quoted", public)]
fn feed_quoted(tx: &mut Tx, ctx: &Ctx, cursor: Option<Cursor>) -> Result<Subscription, ApiError> {
    let _ = (tx, ctx);
    Ok(Subscription::of::<Quoted>().cursor(cursor))
}

#[test]
fn an_event_name_is_escaped_in_the_terminal_frames() {
    let sim = Sim::with_faults(0x5A9E_0307, FaultConfig::none());
    let app = App::open(
        Box::new(sim.disk()),
        Schema::new(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )
    .unwrap()
    .mount(routes![feed_quoted]);
    let mut kernel = Kernel::new(app, Box::new(sim.clock())).unwrap();
    let (_, reshaped) = run_script(
        &mut kernel,
        &sim,
        vec![Request::new(Method::Get, "/quoted")],
    );
    apply_record(
        &mut kernel,
        shaped_emit_record(Quoted::NAME, &[0u8; 8], 0xBAD),
    );
    let (_, undecodable) = run_script(
        &mut kernel,
        &sim,
        vec![Request::new(Method::Get, "/quoted")],
    );
    apply_record(
        &mut kernel,
        shaped_emit_record(Quoted::NAME, &[], Quoted::FINGERPRINT),
    );
    run_script(&mut kernel, &sim, Vec::new());
    for (sinks, frame) in [(reshaped, RESHAPED_FRAME), (undecodable, ERROR_FRAME)] {
        let frames = frames_of(&sinks.lock().unwrap()[0]);
        assert_eq!(frames.len(), 1, "{frames:?}");
        assert_eq!(frames[0].2, frame);
        let body: serde_json::Value = serde_json::from_str(&frames[0].3)
            .unwrap_or_else(|e| panic!("the {frame} frame is not JSON ({e}): {}", frames[0].3));
        assert_eq!(body["event"], Quoted::NAME);
    }
}

/// A subscription carrying two event types of one `NAME` could only ever
/// deliver the first; carrying both is refused by name.
#[test]
#[should_panic(
    expected = "Subscription::and: event \"incident_opened\" is already in this subscription — an \
                event appears once, and two #[event] types sharing a NAME (the type's snake_case) \
                would leave the second undeliverable; give the other type its own name (GUIDE \
                §11, \"Changing an event\")"
)]
fn a_subscription_refuses_two_events_of_one_name() {
    let _ = Subscription::of::<shapes_v1::IncidentOpened>().and::<shapes_v2::IncidentOpened>();
}

fn realtime_app(sim: &Sim) -> io::Result<App> {
    Ok(App::open(
        Box::new(sim.disk()),
        Schema::new(),
        Box::new(sim.entropy()),
        AppConfig {
            body_cap: 1 << 20,
            db: charpente::DbConfig {
                wal: WalConfig {
                    dir: "wal".to_string(),
                    segment_bytes: 4096,
                },
                store_path: "tables.redb".to_string(),
                // Small on purpose: this workload's job is to make a client
                // fall behind retention (the `realtime:gap` path) within a
                // modest number of emits per universe, not just exercise the
                // happy backlog-resume path.
                event_retention: 4,
                checkpoint_every: 2,
                ..charpente::DbConfig::default()
            },
            ..AppConfig::default()
        },
    )?
    .mount(routes![
        emit_ping, emit_pong, feed_both, feed_ping, feed_keyed
    ]))
}

const PING_ONLY: &[&str] = &["ping"];
const ORG_PING_ONLY: &[&str] = &["org_ping"];
const BOTH: &[&str] = &["ping", "pong"];

/// This universe's subscribe path, subscribed event names, and optional key
/// filter — one fixed mode for the whole universe, chosen from the seed so
/// it never perturbs any entropy stream.
fn mode_for(seed: u64) -> (&'static str, &'static [&'static str], Option<&'static str>) {
    match seed % 3 {
        0 => ("/feed-both", BOTH, None),
        1 => ("/feed-ping", PING_ONLY, None),
        _ => ("/feed-keyed", BOTH, Some("org-a")),
    }
}

/// One emit request's route and body — content is arbitrary (varied only to
/// exercise the name/key filter branches); the ledger never trusts what was
/// *sent*, only what the server actually materialized (see
/// `Client::sync_ledger`), so nothing here needs to be remembered by the
/// caller.
fn pick_emit(sim: &Sim) -> (&'static str, EmitBody) {
    let e = sim.entropy();
    let is_ping = e.next_u64().is_multiple_of(2);
    let key = match e.next_u64() % 3 {
        0 => None,
        1 => Some("org-a".to_string()),
        _ => Some("org-b".to_string()),
    };
    let n = WireU64(sim.entropy().next_u64() % 1000);
    if is_ping {
        ("/ping", EmitBody { n, key })
    } else {
        ("/pong", EmitBody { n, key })
    }
}

/// A `SimSink` stall closure keyed on this universe's `subscriber_stall`
/// rate: `FaultConfig.subscriber_stall` has no internal consumption site
/// (`SimSink` lives outside `charpente-sim` and holds no `Sim` reference), so
/// the workload that owns the closure is exactly where the roll belongs.
/// Zero draws when `rate <= 0.0` — the same zero-draw-when-off shape every
/// other axis uses, applied at the workload's own consumption site instead
/// of inside the sim.
fn stall_closure(entropy: SimEntropy, rate: f64) -> Box<dyn FnMut() -> bool + Send> {
    Box::new(move || {
        if rate <= 0.0 {
            return false;
        }
        let unit = (entropy.next_u64() >> 11) as f64 / (1u64 << 53) as f64;
        unit < rate
    })
}

/// The resume contract's core discriminator (DESIGN §6): every frame a
/// client accepts, across every reconnect, must carry a strictly increasing
/// `id` — a resume that ever redelivers (or rewinds past) an id the client
/// already saw is never a legitimate retry. Shared by the flagship property
/// and its off-by-one meta-test, so both exercise the identical oracle.
fn assert_strictly_increasing(id: u64, last_id: &mut Option<u64>) -> Result<(), String> {
    if let Some(prev) = *last_id
        && id <= prev
    {
        return Err(format!(
            "duplicate or out-of-order event id {id} received — a resumed tail must never \
             redeliver an id at or before the last one seen (previous id {prev})"
        ));
    }
    *last_id = Some(id);
    Ok(())
}

// ------------------------------------------------------------------ client

/// What a pending (already-delivered) frame's direct id lookup found in the
/// ledger.
enum MatchOutcome {
    /// The ledger's entry at this id matches the delivered content exactly.
    Matched,
    /// A real content divergence at this id — always a hard failure.
    Mismatch(String),
    /// `sync_ledger` has not scanned this far yet (it may have been skipped
    /// by an injected fault earlier in the same round the event was durably
    /// committed) — try again after a later `sync_ledger` call, never a
    /// verdict on its own.
    Pending,
}

/// `Client::ledger`'s shape: event id to (event name, app key, recorded
/// tenant, the exact JSON a delivery would carry).
type Ledger = BTreeMap<u64, (&'static str, Option<String>, Option<u64>, String)>;

/// The reconnecting client's accumulated state across every epoch and
/// reconnect in one universe: the ledger it must eventually match (keyed by
/// event id — see below), every delivered-but-not-yet-content-verified
/// frame (`pending`), its resume cursor, whether it currently holds a live
/// tail, and the full observed frame trace (the determinism check's
/// oracle).
///
/// The ledger is built from the server's own `charpente.events` table
/// (`sync_ledger`), never from client-side response codes: a `500` does not
/// mean "not committed" — `TxError::Io`'s own contract is "the request may
/// or may not have committed" (e.g. a post-sync apply failure — an injected
/// `write_error`/`sync_error` while checkpointing the table store — still
/// leaves the WAL record durable; the tail can deliver it before this
/// client's own next `sync_ledger` call gets a chance to see it, since that
/// later call reads through the very same connection the apply failure just
/// poisoned). Content verification is therefore deferred (`pending`/
/// `try_match`) and keyed by id rather than by sequential position: a
/// sequential "next expected slot" pointer cannot distinguish "this ledger
/// entry is genuinely forfeited (below a later bare reconnect's boundary)"
/// from "this ledger entry is exactly what a still-pending frame is waiting
/// on" when `sync_ledger` is lagging — both look identical positionally.
/// Direct id lookup sidesteps the distinction entirely: a delivered frame's
/// own id is compared against the ledger entry AT THAT id, full stop.
struct Client {
    mode_names: &'static [&'static str],
    mode_key: Option<&'static str>,
    /// The tenant this client's feed was opened in, `None` for a global
    /// subscription. A row is this client's only if its recorded tenant is
    /// exactly this: presence, not value, so tenant 0 is `Some(0)`.
    mode_scope: Option<u64>,
    /// Every event `sync_ledger` has read back, keyed by id.
    ledger: Ledger,
    /// How far `sync_ledger` has scanned `charpente.events` — server-
    /// relative, persists across reopens.
    next_scan_id: u64,
    /// Ids `sync_ledger` proved aged out of retention: an id a scan stepped
    /// over because its row was gone (first-returned-id > `next_scan_id`;
    /// event ids are dense, so the skipped span was emitted then GC'd). No
    /// future scan will ever return them — recorded here at scan time rather
    /// than silently advancing `next_scan_id` past them, so `drain` can
    /// CONFIRM a delivered frame whose row aged out before it was recorded.
    /// This set is "which ids are gone", NOT "which are forgiven": the
    /// received-only gate stays `pending` membership, in `drain`.
    gone: BTreeSet<u64>,
    /// Ids strictly below this are forfeited: a bare reconnect's
    /// `next_event_id` at push time, so `settled()` never waits on them.
    /// Monotonic (a later forfeit is always a superset of an earlier one),
    /// except that a timeline cut lowers it to the cut (`resync_to_head`).
    floor: u64,
    /// Delivered frames not yet checked against the ledger: `(id, name,
    /// json)`, oldest first. Every entry was RECEIVED live (pushed from a
    /// tail delivery in `absorb`, the only live push site) — the structural
    /// fact `drain` leans on to confirm an aged-out (`gone`) id whose ledger
    /// row disappeared before it could be content-verified, without ever
    /// forgiving an id the client never received.
    pending: VecDeque<(u64, &'static str, String)>,
    /// Every id ever content-verified (matched against the ledger) via
    /// `drain` — the membership bound `settled()` checks against, not a
    /// high-water mark. A high-water mark (max verified id + 1) is UNSOUND:
    /// if a later id gets verified before an earlier one was ever delivered
    /// at all (a reconnect that verifies 0-5, then jumps straight to 7
    /// without ever receiving 6), the mark advances past the gap and
    /// permanently excuses it — the interior-skip meta-test proves
    /// membership catches what the mark could not (T6 review). `floor`
    /// stays a separate, permanent forfeit trim; this set only ever grows,
    /// except that a timeline cut drops the ids it cut (`resync_to_head`).
    verified: BTreeSet<u64>,
    cursor: Option<Cursor>,
    need_subscribe: bool,
    current_handle: Option<SimSinkHandle>,
    frames_consumed: usize,
    last_id: Option<u64>,
    trace: Vec<(u64, &'static str, String)>,
    /// Whether a fault explains a cut of this client's node's timeline: one
    /// that removed a ledgered event. `durable_corruption_fired` for the
    /// single-node workloads; the follower property asks its tail node's own
    /// disk. Only where it answers true is a cut re-synced (`resync_if_cut`);
    /// a cut nothing explains is a lost event, and the universe fails where it
    /// is first seen: a boundary, or for the follower property, which re-syncs
    /// nothing, the drain (S3b items 1 and 3).
    cut_explained: fn(&Sim) -> bool,
}

thread_local! {
    /// Counts the 409-diverged arm's re-syncs on this thread, so a pin can
    /// assert that step of its reason without a coverage key (which would move
    /// every batch's block). Read by `a_bare_tail_after_a_cut...` only.
    static DIVERGED_409_RESYNCS: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
}

impl Client {
    fn new(mode_names: &'static [&'static str], mode_key: Option<&'static str>) -> Client {
        Client {
            mode_names,
            mode_key,
            mode_scope: None,
            ledger: BTreeMap::new(),
            next_scan_id: 0,
            gone: BTreeSet::new(),
            floor: 0,
            pending: VecDeque::new(),
            verified: BTreeSet::new(),
            cursor: None,
            need_subscribe: true,
            current_handle: None,
            frames_consumed: 0,
            last_id: None,
            trace: Vec::new(),
            cut_explained: durable_corruption_fired,
        }
    }

    /// This client, subscribed to a tenant feed of `tenant` rather than to a
    /// global subscription.
    fn in_tenant(mut self, tenant: u64) -> Client {
        self.mode_scope = Some(tenant);
        self
    }

    fn matches(&self, name: &str, key: &Option<String>, scope: Option<u64>) -> bool {
        self.mode_names.contains(&name)
            && self.mode_scope == scope
            && (self.mode_key.is_none() || self.mode_key == key.as_deref())
    }

    /// True once every delivered frame has been content-verified AND
    /// nothing scanned in `[floor, next_scan_id)` that matches this
    /// subscription is unaccounted for — the property's success condition
    /// at quiescence. `floor` excludes forfeited ids outright; anything at
    /// or past `next_scan_id` simply hasn't been looked at yet and is
    /// irrelevant here (a still-open tail would still be delivering it).
    fn settled(&self) -> bool {
        self.pending.is_empty() && self.first_unaccounted().is_none()
    }

    /// True when a settled client checked nothing: no frame was delivered, and
    /// no ledgered row lay in its subscription's window `[floor,
    /// next_scan_id)`, so nothing was owed and nothing could have been
    /// withheld. A row the subscription does not match, left undelivered,
    /// is a check its filter passed, so it counts (S3b item 7, S5 review I2).
    fn adjudicated_nothing(&self) -> bool {
        let lo = self.floor.min(self.next_scan_id);
        self.trace.is_empty() && self.ledger.range(lo..self.next_scan_id).next().is_none()
    }

    /// The give-up for `adjudicated_nothing`, which each settled workload ends
    /// with. It comes after the coverage keys, which count the universe's
    /// shape and stay as they were.
    fn give_up_if_nothing_adjudicated(&self, sim: &Sim) {
        if self.adjudicated_nothing() {
            sim.give_up(format!(
                "the settled client was owed nothing and delivered nothing: no ledgered row \
                 lay in its subscription's window [{}, {}), so nothing was adjudicated",
                self.floor.min(self.next_scan_id),
                self.next_scan_id
            ));
        }
    }

    /// The first matching (subscribed-filter-passing) ledger id in `[floor,
    /// next_scan_id)` that has not been content-verified — `None` at
    /// quiescence. `settled()`'s worker, factored out so a diagnostic can
    /// name the exact missing id (`unsettled_report`, the skip meta-test's
    /// discriminator) instead of only reporting counts. The `.min` clamp on
    /// `lo` is the same floor-trim safety the old bound used — `floor` can
    /// run ahead of `next_scan_id` transiently (a bare reconnect's boundary
    /// observed before this client's own ledger scan has caught up to it),
    /// and `BTreeMap::range` panics on an inverted bound.
    fn first_unaccounted(&self) -> Option<u64> {
        let lo = self.floor.min(self.next_scan_id);
        for (id, (name, key, scope, _)) in self.ledger.range(lo..self.next_scan_id) {
            if self.matches(name, key, *scope) && !self.verified.contains(id) {
                return Some(*id);
            }
        }
        None
    }

    /// The diagnostic `realtime_workload` returns when the settle phase
    /// exhausts its attempts still unsettled — factored onto `Client` so the
    /// skip meta-test can byte-pin the missing id the same way the
    /// off-by-one meta-test byte-pins `duplicate`.
    fn unsettled_report(&self) -> String {
        format!(
            "client never caught up: ledger has {} entries scanned through id {}, floor {}, \
             {} frames still unverified, first unaccounted matching id {:?}, cursor={:?} \
             need_subscribe={}",
            self.ledger.len(),
            self.next_scan_id,
            self.floor,
            self.pending.len(),
            self.first_unaccounted(),
            self.cursor,
            self.need_subscribe
        )
    }

    /// Re-reads the store at the ids this client's ledger already recorded and
    /// returns the first whose CONTENT has changed underneath it.
    ///
    /// `sync_ledger` scans forward from `next_scan_id` and never revisits an id,
    /// so a ledger entry is a snapshot of what the store held at the moment it
    /// was scanned. A commit is append → sync → apply, so nothing the store
    /// holds was ever unsynced — but a synced event can still leave this node's
    /// timeline. On one node, durable corruption (`bit_flip`,
    /// `misdirected_write`) of a frame above the store's checkpoint horizon is
    /// cut at recovery like a torn tail: the sanctioned single-node
    /// media-corruption window (`durable_corruption_fired`'s doc). On a
    /// replicated node, a failover can also lose it inside the replication loss
    /// window. Either way a replacement record may then re-mint the SAME id,
    /// and — when the cut returned the log to the same position — the same seq,
    /// with different content.
    ///
    /// That is not a delivery defect and must not be reported as one
    /// (seed 0x17004e6a572d737d, docs/LEDGER.md 2026-09-07, where a misdirected
    /// write moved `wal/…0.log`'s first frame from offset 0 to 1024, so recovery
    /// cut at 0 and event id 0 was re-minted at seq 1 with new content; and
    /// seed 0xb247eceae0daf689, where a `bit_flip` cut an acked event at id 3).
    ///
    /// Called ONLY on the drain-error path, so a passing universe pays no extra
    /// disk ops and its injected-fault schedule is untouched.
    fn diverged_ledger_entry(&self, db: &Db) -> Option<(u64, String, String)> {
        let snap = db.snapshot().ok()?;
        let next_event_id = snap.next_event_id().ok()?;
        for (&id, (_, _, _, recorded)) in &self.ledger {
            match snap.get::<EventRow>(Id::from_raw(id)).ok()? {
                Some(row) => {
                    let (_, stored) = decode_event_row(&row).ok()?;
                    if stored != *recorded {
                        return Some((id, recorded.clone(), stored));
                    }
                }
                // ABSENT, and the counter says why. `next_event_id` only ever
                // advances on commit — retention GC deletes rows without ever
                // rewinding it — so a recorded id at-or-above the store's
                // current `next_event_id` is one the store has rolled back
                // PAST: this node's timeline no longer contains that event,
                // for one of the two reasons above. It can never be
                // delivered and this universe cannot adjudicate it.
                //
                // An absent id BELOW `next_event_id` is the retention case and
                // is deliberately NOT excused here: the row existed in this
                // timeline, so a client subscribed from below it should have
                // received it, and forgiving that would hide the miss this
                // property exists to catch (`gone`/`first_unaccounted` own
                // that path).
                None if id >= next_event_id => {
                    return Some((
                        id,
                        recorded.clone(),
                        format!("ABSENT — store rolled back to next_event_id {next_event_id}"),
                    ));
                }
                None => {}
            }
        }
        None
    }

    /// Reads every event committed since the last scan and records it,
    /// decoding each row's payload back through the same `Codec` the
    /// platform encoded it with and re-deriving the exact JSON the delivery
    /// path would produce (`ev_json`, `charpente::http`) — so a ledger entry
    /// is never a guess about what was *sent*, only a readback of what was
    /// *stored*.
    fn sync_ledger(&mut self, db: &Db) -> io::Result<()> {
        let snap = db.snapshot()?;
        let rows: Vec<EventRow> = snap.scan_from(self.next_scan_id, 4096)?;
        // On-scan gap detection (never advance past a gap silently): event
        // ids are dense below `next_event_id`, so any id in
        // `[next_scan_id, first-returned-id)` has no row — it was emitted and
        // then aged out of retention (GC deletes an oldest-first prefix). Its
        // ledger row is gone for good; record it so `drain` can confirm a
        // delivered frame at that id BY ID. Derived entirely from the scan
        // already in hand — zero extra disk ops, so the injected-fault
        // schedule is untouched.
        if let Some(first) = rows.first() {
            for gone in self.next_scan_id..first.id.raw() {
                self.gone.insert(gone);
            }
        }
        for row in rows {
            self.next_scan_id = row.id.raw() + 1;
            let (name, json) = decode_event_row(&row)?;
            self.ledger
                .insert(row.id.raw(), (name, row.key, row.scope, json));
        }
        Ok(())
    }

    /// Forgets everything this client learned about ids at or above `head`, the
    /// `next_event_id` of a store whose timeline a recovery cut back to it, so
    /// the next `sync_ledger` re-reads those ids from the timeline the node now
    /// holds. Every piece of state keyed by id goes: a ledger row recorded
    /// there describes an event that no longer exists, a frame delivered there
    /// came from the lost timeline, a `verified` or `gone` mark there would
    /// excuse whatever event is re-minted at that id, and a `floor` above
    /// `head` would forfeit it. What survives is exactly the state for ids the
    /// node still holds; nothing here is forgiven.
    fn resync_to_head(&mut self, head: u64) {
        self.ledger.retain(|&id, _| id < head);
        self.pending.retain(|&(id, ..)| id < head);
        self.verified.retain(|&id| id < head);
        self.gone.retain(|&id| id < head);
        self.floor = self.floor.min(head);
        self.next_scan_id = self.next_scan_id.min(head);
        // `last_id` is the strictly-increasing check's memory of the lost
        // timeline. A cut id was re-minted (or not) on the new one, and a bare
        // tail owes nothing about it, so the first re-mint it delivers may sit
        // at or below an id delivered before the cut. The trim is exactly as
        // safe as `cut_explained` is precise: the flagship's judge is
        // `durable_corruption_fired`, true from the node's FIRST durable
        // corruption on, so it excuses every later cut whatever caused it, and
        // a lost event after that point loses this check's backstop too.
        if self.last_id >= Some(head) {
            self.last_id = head.checked_sub(1);
        }
    }

    /// `head` is a `next_event_id` read off a node (`push_subscribe`'s
    /// boundary). That counter only advances on commit, so a recorded id at or
    /// above it is one a recovery cut away, which on one node takes durable
    /// corruption above the checkpoint horizon. Then the client re-syncs to
    /// the node's timeline (`resync_to_head`): a cut id that was never
    /// re-minted, or was re-minted for another tenant, is no longer owed, and
    /// one re-minted for this subscription is owed again under its new
    /// content, so a re-mint that is never delivered FAILS the universe. A cut
    /// no fault on this node explains (`cut_explained`) is a lost event, and
    /// fails here, where it is first seen (S3b item 3). Until then it stayed
    /// owed and failed only in a settle with a clean attempt, so a settle with
    /// none gave it up.
    fn resync_if_cut(&mut self, sim: &Sim, head: u64) -> Result<(), String> {
        let Some((&id, _)) = self.ledger.range(head..).next() else {
            return Ok(());
        };
        if !(self.cut_explained)(sim) {
            return Err(format!(
                "ledger id {id} was recorded off this node, but its next_event_id is now \
                 {head}: a recovery cut a committed event, and no fault on this node explains \
                 the cut, so this node lost an event it had committed"
            ));
        }
        self.resync_to_head(head);
        sim.cover("realtime:ledger-resynced");
        Ok(())
    }

    /// Direct id lookup — no positional skipping, no forfeit interaction:
    /// this delivered frame's own id is either in the ledger with matching
    /// content, in the ledger with different content (a real bug), or not
    /// scanned yet.
    fn try_match(&self, id: u64, name: &str, json: &str) -> MatchOutcome {
        match self.ledger.get(&id) {
            Some((exp_name, key, scope, exp_json)) if *exp_name == name && exp_json == json => {
                // Content matches the ledger row at this id exactly — but a
                // delivered frame is only legitimate if that row ALSO
                // satisfies this subscription's own filter. Over-delivery
                // (a leaked wrong-name, wrong-tenant or wrong-key row) carries
                // genuine content at its own id, so content equality alone
                // would silently Match it; this closes that direction.
                if !self.matches(exp_name, key, *scope) {
                    let violation = if !self.mode_names.contains(exp_name) {
                        format!(
                            "name {exp_name:?} is not in the subscribed set {:?}",
                            self.mode_names
                        )
                    } else if self.mode_scope != *scope {
                        format!(
                            "its recorded tenant {scope:?} is not the subscription's {:?}",
                            self.mode_scope
                        )
                    } else {
                        format!(
                            "key {key:?} does not match the subscribed key {:?}",
                            self.mode_key
                        )
                    };
                    return MatchOutcome::Mismatch(format!(
                        "over-delivery at event id {id}: delivered frame name {name:?} \
                         violates this subscription's filter — {violation}"
                    ));
                }
                MatchOutcome::Matched
            }
            Some((exp_name, _, _, exp_json)) => MatchOutcome::Mismatch(format!(
                "content mismatch at event id {id}: expected ({exp_name}, {exp_json}) but \
                 received ({name}, {json})"
            )),
            None => MatchOutcome::Pending,
        }
    }

    /// Drains as much of `pending` as the current ledger allows (oldest
    /// first). A delivered frame whose id `sync_ledger` proved aged out of
    /// retention (`gone` — its row is gone and no future scan will ever
    /// return it) can never be content-verified, so it is CONFIRMED BY ID
    /// instead of blocking the drain forever. That confirmation resolves ONLY
    /// ids the client actually RECEIVED: every `pending` entry was pushed
    /// from a live tail delivery (`absorb`), so a `gone` pop here is always a
    /// received-then-aged event, never a missing one — the ledger-scan side
    /// has no business forgiving an id the client never got (that is the
    /// subscribe-side 409 / gap-frame contract's job). A still-retained id
    /// the ledger has not caught up to yet stops the drain (not fails),
    /// retried after the next scan.
    fn drain(&mut self) -> Result<(), String> {
        while let Some(&(id, name, ref json)) = self.pending.front() {
            let json = json.clone();
            match self.try_match(id, name, &json) {
                MatchOutcome::Matched => {
                    self.pending.pop_front();
                    self.verified.insert(id);
                }
                MatchOutcome::Pending if self.gone.contains(&id) => {
                    // Received live (it is in `pending`) but its ledger row
                    // aged out before `sync_ledger` recorded it: confirm by
                    // id. A gone row is unverifiable, never misdelivered —
                    // delivery-time `assert_strictly_increasing` already held
                    // the strictly-increasing resume contract for it.
                    self.pending.pop_front();
                    self.verified.insert(id);
                }
                MatchOutcome::Pending => break,
                MatchOutcome::Mismatch(e) => return Err(e),
            }
        }
        Ok(())
    }

    /// `sync_ledger` (fault-fallout tolerant) then `drain` — the pairing
    /// used at every point new ground truth or new deliveries might exist.
    fn sync_and_drain(&mut self, db: &Db) -> Result<(), String> {
        if let Err(e) = self.sync_ledger(db)
            && !is_fault_fallout(&e.to_string())
        {
            return Err(format!("ledger scan failed non-injected: {e}"));
        }
        // else: skip; the next successful scan (later this epoch, or at the
        // next reopen) picks up anything still durable.
        self.drain()
    }

    /// Absorbs one round's worth of responses and frames. `new_responses` is
    /// exactly what was recorded since the last `absorb` call, in push
    /// order: the subscribe response first (if `subscribe` is `Some`), then
    /// one per emit request pushed this round — a prefix may be missing if a
    /// crash cut the round short mid-way. `subscribe` is `(was_bare,
    /// boundary)`: `boundary` is `next_event_id` read right before the
    /// subscribe was pushed — the precise point a *successful bare*
    /// subscribe resumes from, folded straight into `floor`.
    /// `sync_and_drain`, whose failure names a diverged ledger entry when the
    /// store no longer holds what the ledger recorded (`diverged_ledger_entry`).
    /// The probe runs ONLY after a drain error, so the common path is
    /// unchanged and no passing universe's fault schedule moves.
    ///
    /// Every divergence fails. A cut a fault explains never reaches here: a
    /// recovery is always followed by a subscribe, whose boundary re-syncs the
    /// client to the node's timeline (`resync_if_cut`) before any emit can
    /// re-mint a cut id. A cut nothing explains is the lost event this
    /// property exists to catch. Until S3b items 1 and 3 any divergence gave
    /// the universe up here; with the flagship re-syncing as the scoped
    /// sibling does, that give-up reached 0 of 6,528 universes over 26 + 25
    /// bases, so it went.
    fn sync_and_drain_naming_divergence(&mut self, sim: &Sim, db: &Db) -> Result<(), String> {
        let Err(e) = self.sync_and_drain(db) else {
            return Ok(());
        };
        Err(match self.diverged_ledger_entry(db) {
            Some((id, recorded, stored)) if !(self.cut_explained)(sim) => format!(
                "{e}: ledger diverged at event id {id}: recorded {recorded} when scanned, \
                 store now holds {stored}, and no fault on this node explains the cut, so \
                 this node lost an event it had committed"
            ),
            Some((id, recorded, stored)) => format!(
                "{e}: ledger diverged at event id {id}: recorded {recorded} when scanned, \
                 store now holds {stored}, a cut no boundary re-synced: every recovery is \
                 followed by a subscribe whose boundary re-syncs a cut a fault explains \
                 (`resync_if_cut`), so a drain should never meet one"
            ),
            None => e,
        })
    }

    fn absorb(
        &mut self,
        sim: &Sim,
        db: &Db,
        new_responses: &[(u16, Vec<u8>)],
        subscribe: Option<(bool, u64)>,
        sinks: &SinkHandles,
    ) -> Result<(), String> {
        self.sync_and_drain_naming_divergence(sim, db)?;

        let mut ri = 0usize;
        if subscribe.is_some()
            && let Some((status, body)) = new_responses.first()
        {
            ri += 1;
            let (was_bare, boundary) = subscribe.expect("checked by outer is_some");
            let text = String::from_utf8_lossy(body).to_string();
            match *status {
                200 => {
                    self.need_subscribe = false;
                    if was_bare {
                        self.floor = self.floor.max(boundary);
                    }
                    let handle = sinks
                        .lock()
                        .expect("sink handles lock poisoned by an earlier panic")
                        .last()
                        .cloned()
                        .expect("a 200 subscribe response implies a new recorded sink handle");
                    self.current_handle = Some(handle);
                    self.frames_consumed = 0;
                }
                409 if text.contains("retention floor") => {
                    // Spec §5 / brief: re-sync current state, reconnect
                    // bare, and trim expectation to events after the floor.
                    sim.cover("realtime:gap");
                    self.cursor = None;
                    self.need_subscribe = true;
                }
                // A cut that returned the log to the client's exact cursor
                // position can also re-mint that id and seq as a row of
                // another tenancy (the same loss, a different emit), which
                // validation refuses by tenancy rather than by seq
                // (amendment A16). Same cause, same prune.
                409 if (text.contains("diverged from this node's timeline")
                    || text.contains("outside this feed's tenancy"))
                    && (self.cut_explained)(sim) =>
                {
                    // T6 interlude investigation (t6-realtime-interlude-
                    // investigation.md): a synced-and-delivered event can
                    // still be lost to the sanctioned single-node
                    // media-corruption window (M1 sub-project 1's ruling —
                    // `bit_flip`/`misdirected_write`, "wal/"-scoped,
                    // above-horizon, window `checkpoint_every`). That's a
                    // real loss, not a bug, but it rewinds the store
                    // BACKWARD — unlike the retention-floor arm above, which
                    // only ever advances `floor` forward. Mirroring that
                    // arm's plain `cursor=None; need_subscribe=true` would
                    // leave forfeited-but-remembered ids sitting in
                    // `ledger`/`pending` at or past the store's new (lower)
                    // head, which `first_unaccounted` would then block on
                    // forever with no future emit ever able to satisfy them.
                    // So prune down to the store's current head instead of
                    // just resetting the cursor.
                    //
                    // FIX ROUND 2, M-7: this is a live-`Db` read, so a
                    // latched store makes it fail (M2 sub-project 0a) — the
                    // second such read in this file, and the one the fix
                    // round's census missed. Disposition matches §1's: retry,
                    // never excuse. On fault fallout NOTHING here changes,
                    // so `need_subscribe` stays true exactly as in the
                    // fallout arm below, the client re-subscribes with the
                    // same cursor, and the prune happens on the retry once
                    // the node has recovered. No id is forgiven and no
                    // verdict is skipped: a client that never manages the
                    // prune never settles, and `unsettled_report` names it.
                    match db.next_event_id() {
                        Ok(head) => {
                            DIVERGED_409_RESYNCS.with(|c| c.set(c.get() + 1));
                            self.resync_to_head(head);
                            sim.cover("realtime:gap");
                            self.cursor = None;
                            self.need_subscribe = true;
                        }
                        Err(e) if is_fault_fallout(&e.to_string()) => {}
                        Err(e) => {
                            return Err(format!("next_event_id read failed non-injected: {e}"));
                        }
                    }
                }
                _ if is_fault_fallout(&text) => {
                    // Not acked; need_subscribe stays true, retried next
                    // round with the same cursor.
                }
                _ => return Err(format!("unexpected subscribe response {status}: {text}")),
            }
        }
        for (status, body) in &new_responses[ri..] {
            if *status != 200 {
                let text = String::from_utf8_lossy(body);
                if !is_fault_fallout(&text) {
                    return Err(format!("unexpected emit response {status}: {text}"));
                }
            }
        }

        if let Some(handle) = self.current_handle.clone() {
            let frames = handle.frames();
            let mut saw_gap = false;
            for frame in &frames[self.frames_consumed..] {
                self.trace.push((frame.id, frame.name, frame.json.clone()));
                if frame.name == "charpente.gap" {
                    saw_gap = true;
                    sim.cover("realtime:gap");
                    self.cursor = None;
                    self.need_subscribe = true;
                    break;
                }
                if frame.name == RESHAPED_FRAME || frame.name == ERROR_FRAME {
                    // Every event in this workload has one shape, and the
                    // matrix's corruption axes (`bit_flip`,
                    // `misdirected_write`) are wal/-scoped and checksummed —
                    // a hit surfaces as a cut, never as a stored row that
                    // decodes wrong. So the platform has no reason to send
                    // this here; if it does, that is a finding to
                    // adjudicate, not an excuse to add.
                    return Err(format!(
                        "a tail ended on {} in a workload with one shape per event: {}",
                        frame.name, frame.json
                    ));
                }
                assert_strictly_increasing(frame.id, &mut self.last_id)?;
                self.pending
                    .push_back((frame.id, frame.name, frame.json.clone()));
                self.cursor = Some(Cursor {
                    id: frame.id,
                    seq: frame.seq,
                });
            }
            self.frames_consumed = frames.len();
            if saw_gap {
                self.current_handle = None;
            } else if handle.closed() {
                // The only other way a tail dies (T5 policy): the sink
                // reported Full/Closed. Our closure is the sole source of
                // Full, so this is exactly the stall firing.
                sim.cover("realtime:stall-drop");
                sim.cover("fault:subscriber_stall");
                self.need_subscribe = true;
                self.current_handle = None;
            }
        }

        // This round's fresh deliveries get a fair shot at draining too,
        // against whatever `sync_ledger` already picked up above.
        self.sync_and_drain_naming_divergence(sim, db)
    }
}

// ---------------------------------------------------------------- workload

const EPOCHS: u32 = 5;
const ROUNDS_PER_EPOCH: u32 = 4;
/// Settle attempts: how many times the client is handed a live node to
/// reconnect to and catch up from.
const SETTLE_ATTEMPTS: u32 = 12;
/// Tries `settle_boot` spends getting one of those nodes up, and how many of
/// them share a power cycle — see `settle_boot`.
const SETTLE_BOOT_TRIES: u32 = 64;
const SETTLE_CYCLE_EVERY: u32 = 8;
/// How many boots one epoch's node gets before the epoch is skipped: each
/// failed boot is followed by a restart, as a supervised process restarts
/// (`checks::boot_through_fallout`). An epoch that runs out restarts and moves
/// on, as every epoch did after one failed boot before `Db::open` stopped
/// absorbing a store-open fault; the settle phase still adjudicates or gives
/// up.
const BOOT_TRIES: u32 = 8;

/// Builds this round's subscribe request (if the client needs one) and
/// queues its stall closure, returning `(was_bare, boundary)` for
/// `Client::absorb` — `boundary` is `next_event_id` read right before the
/// push, so it is the exact resume point a bare subscribe gets. Marks
/// `realtime:resume-backlog` when a cursored resume already has backlog
/// waiting.
fn push_subscribe(
    sim: &Sim,
    kernel: &Kernel,
    source: &mut SimSource,
    path: &'static str,
    cursor: Option<Cursor>,
    t: u64,
) -> Result<(bool, u64), String> {
    let entropy = sim.entropy();
    source.push_stall(stall_closure(entropy, sim.subscriber_stall()));
    let boundary = kernel
        .app()
        .db()
        .next_event_id()
        .map_err(|e| format!("next_event_id read failed: {e}"))?;
    let (req, was_bare) = match cursor {
        Some(c) => {
            if boundary > c.id + 1 {
                sim.cover("realtime:resume-backlog");
            }
            (
                Request::new(Method::Get, format!("{path}?cursor={}", c.wire())),
                false,
            )
        }
        None => (Request::new(Method::Get, path), true),
    };
    source.push_at(t, req);
    Ok((was_bare, boundary))
}

/// Brings a node up for one settle attempt, retrying through injected
/// faults. `Ok(None)` means the budget ran out with nothing serving.
///
/// A `Db::open`/`Kernel::new` that died on an injected write error is a
/// process that FAILED TO START — it is not a power cycle, and modelling it
/// as one is what makes a node unbootable rather than merely hard to boot.
/// `Store::apply` commits at `Durability::None`, so every `Sim::restart`
/// vanishes the store's uncheckpointed state and forces the next open to
/// discard, rebuild from the WAL, and re-`persist_store_at_open` — more
/// writes at the same per-write failure odds, on every single try. Measured
/// on seed `0x536007ad99fd90bf` (`write_error` 0.144): power-cycling before
/// each try, **0 of 20_000 boots** ever served a subscribe; retrying in
/// place, the same universe is serving after ~16 power cycles. Retrying in
/// place is also the faithful model — the machine did not lose power, so the
/// page cache is still there for the next process to open.
///
/// The periodic power cycle is not optional: a pending `crash_disk_after`
/// countdown can fire mid-boot, and a crashed sim disk answers every
/// operation with `crashed_error` until `Sim::restart` — so an in-place
/// retry loop with no cycle at all would spin until its budget expired.
/// One event row's `(name, json)`, derived exactly as the delivery path would.
/// Shared by `sync_ledger` and `diverged_ledger_entry` so a re-read can never
/// differ from the original scan for any reason but the store changing.
fn decode_event_row(row: &EventRow) -> io::Result<(&'static str, String)> {
    match row.name.as_str() {
        "ping" => Ok((
            "ping",
            serde_json::to_string(&Ping::decode(&mut &row.payload[..])?).expect("Ping serializes"),
        )),
        "pong" => Ok((
            "pong",
            serde_json::to_string(&Pong::decode(&mut &row.payload[..])?).expect("Pong serializes"),
        )),
        "org_ping" => Ok((
            "org_ping",
            serde_json::to_string(&OrgPing::decode(&mut &row.payload[..])?)
                .expect("OrgPing serializes"),
        )),
        other => Err(io::Error::other(format!(
            "ledger scan found event name {other:?} — this workload only ever emits ping/pong/org_ping"
        ))),
    }
}

fn settle_boot(sim: &Sim) -> Result<Option<Kernel>, String> {
    for try_n in 0..SETTLE_BOOT_TRIES {
        if try_n > 0 && try_n.is_multiple_of(SETTLE_CYCLE_EVERY) {
            sim.restart();
        }
        let app = match realtime_app(sim) {
            Ok(app) => app,
            Err(e) => {
                // No restart clears a durable refusal (S5 re-review R-M2), and
                // only a fired durable-corruption fault excuses one (S3b item 0).
                if durable_refusal(sim, &e.to_string())? {
                    return Ok(None);
                }
                if !is_fault_fallout(&e.to_string()) {
                    return Err(format!("settle open failed non-injected: {e}"));
                }
                continue;
            }
        };
        match Kernel::new(app, Box::new(sim.clock())) {
            Ok(kernel) => return Ok(Some(kernel)),
            Err(e) => {
                if !is_fault_fallout(&e.to_string()) {
                    return Err(format!("settle Kernel::new failed non-injected: {e}"));
                }
            }
        }
    }
    Ok(None)
}

/// One universe: a kernel and one reconnecting client, driven through
/// `EPOCHS` crash/restart-bounded sessions of `ROUNDS_PER_EPOCH` rounds each
/// (emits plus, whenever the client lacks a live tail, a (re)subscribe using
/// its last received id.seq), then a settle phase that drains any remaining
/// backlog through fresh injected faults until the client has caught up to
/// everything the ledger says it should have received. Returns the full
/// observed frame trace (id, name, json) — the determinism check's oracle —
/// on success.
fn realtime_workload(sim: &mut Sim) -> Result<Vec<(u64, &'static str, String)>, String> {
    realtime_workload_judged(sim, durable_corruption_fired)
}

/// `realtime_workload`, its client asking `cut_explained` whether a fault
/// explains a cut: `durable_corruption_fired` for the property, a check that
/// explains nothing for the pins that stand a cut in for a platform durability
/// bug (S3b item 1; `scoped_realtime_workload_judged`'s reason).
fn realtime_workload_judged(
    sim: &mut Sim,
    cut_explained: fn(&Sim) -> bool,
) -> Result<Vec<(u64, &'static str, String)>, String> {
    let (path, mode_names, mode_key) = mode_for(sim.seed());
    let mut client = Client::new(mode_names, mode_key);
    client.cut_explained = cut_explained;

    for _epoch in 0..EPOCHS {
        // A give-up already recorded means this universe adjudicated
        // nothing; carrying on can only turn it into a failure, and the
        // runner ranks a later `Err` above an earlier give-up.
        if sim.gave_up().is_some() {
            return Ok(client.trace);
        }
        let entropy = sim.entropy();
        // Sized to redb 4.3.0's disk-op volume per round: DESIGN §1, "countdowns".
        sim.crash_disk_after(50 + entropy.next_u64() % 399);
        let Some(app) =
            boot_through_fallout(sim, BOOT_TRIES, is_fault_fallout, || realtime_app(sim))
                .map_err(|e| format!("open failed non-injected: {e}"))?
        else {
            sim.restart();
            continue;
        };
        let mut kernel = match Kernel::new(app, Box::new(sim.clock())) {
            Ok(k) => k,
            Err(e) => {
                if !is_fault_fallout(&e.to_string()) {
                    return Err(format!("Kernel::new failed non-injected: {e}"));
                }
                sim.restart();
                continue;
            }
        };
        // A fresh App/Kernel never carries our tail over.
        client.need_subscribe = true;
        client.current_handle = None;

        let mut source = SimSource::new(Box::new(sim.clock()), sim.now().as_nanos() + 2000 * SEC);
        let sinks = source.sinks();
        let responses = source.responses();
        let mut resp_idx = 0usize;
        let mut t = sim.now().as_nanos();

        for _round in 0..ROUNDS_PER_EPOCH {
            let mut subscribe = None;
            if client.need_subscribe {
                // `push_subscribe` reads `next_event_id` off the live `Db`,
                // so it fails whenever the store has latched — classified
                // exactly like the `kernel.run` call below, and recovered the
                // same way (a restart mints a fresh redb handle and a fresh
                // latch flag). Without this the universe fails outright on an
                // injected fault; see the fix-round report's latent finding,
                // seed 0xdc154f8c66271f15.
                match push_subscribe(sim, &kernel, &mut source, path, client.cursor, t) {
                    Ok(info) => {
                        client.resync_if_cut(sim, info.1)?;
                        subscribe = Some(info);
                        t += SEC;
                    }
                    Err(e) if is_fault_fallout(&e) => {
                        sim.restart();
                        client.need_subscribe = true;
                        client.current_handle = None;
                        break;
                    }
                    Err(e) => return Err(format!("push_subscribe failed non-injected: {e}")),
                }
            }

            let n_emits = 1 + sim.entropy().next_u64() % 3;
            for _ in 0..n_emits {
                let (route, body) = pick_emit(sim);
                source.push_at(t, Request::new(Method::Post, route).json(&body));
                t += SEC;
            }

            let run_result = kernel.run(&mut source);

            let new_responses = {
                let guard = responses
                    .lock()
                    .expect("responses lock poisoned by an earlier panic");
                guard[resp_idx..].to_vec()
            };
            resp_idx += new_responses.len();
            client.absorb(sim, kernel.app().db(), &new_responses, subscribe, &sinks)?;

            if let Err(e) = run_result {
                if !is_fault_fallout(&e.to_string()) {
                    return Err(format!("kernel.run failed non-injected: {e}"));
                }
                sim.restart();
                client.need_subscribe = true;
                client.current_handle = None;
                break;
            }
            // Below the `run_result` check, not above it: that failure was
            // observed before `absorb` gave up, and a failure outranks a
            // give-up. A fallout `break` above reaches the next loop-top
            // check: the epoch loop's on every epoch but the last, and the
            // settle loop's on the last, once the epoch loop has ended.
            if sim.gave_up().is_some() {
                return Ok(client.trace);
            }
        }
    }

    // Settle: drain any remaining backlog with no new emits, retrying
    // through whatever injected faults remain, until the client has caught
    // up to everything the ledger says it should have.
    //
    // Each attempt boots its node through `settle_boot` rather than a single
    // `Db::open`, because "the client failed to catch up" and "there was no
    // node to catch up FROM" are different facts and only the first is this
    // property's subject. That distinction became load-bearing with M2
    // sub-project 0a: a latched store now poisons the node, so an epoch whose
    // store latches ends with the client disconnected instead of quietly
    // fed off redb's page cache — the client must then reconnect, and a
    // `write_error` rate high enough to latch a store is also high enough to
    // make `Db::open` itself a coin-flip.
    //
    // This CANNOT mask a missed event, and that is the only thing about it
    // worth checking twice. Retries add chances to RECEIVE, nothing else:
    // `verified` grows solely through `drain` matching a frame the server
    // actually delivered, and a resume reads from `cursor.id + 1`, so an id
    // the platform ever skips is one the cursor has already moved past —
    // no later reconnect requests it, no later reconnect delivers it, and
    // `first_unaccounted` keeps naming it however many attempts follow. The
    // budgets bound how long the client may keep trying; they do not touch
    // what counts as caught up.
    // No subscriber stalls in the settle. A stall is a fault this property
    // excuses, so one that drops a clean attempt's tail leaves that attempt
    // adjudicating nothing, and when it was the only clean attempt the
    // universe failed "client never caught up" on no evidence (seed
    // 0xfddd6c7112629074; the scenario is pinned below). The epochs above
    // keep exercising stall-drop and resume.
    sim.quiesce_subscriber_stalls();
    // How many settle attempts ran all the way through `kernel.run` without
    // fault fallout. Zero means this universe never observed the platform
    // delivering anything, so it cannot testify about a missed event.
    let mut clean_attempts = 0u32;
    for _attempt in 0..SETTLE_ATTEMPTS {
        // A give-up already recorded means this universe adjudicated
        // nothing; carrying on can only turn it into a failure, and the
        // runner ranks a later `Err` above an earlier give-up.
        if sim.gave_up().is_some() {
            return Ok(client.trace);
        }
        if client.settled() {
            break;
        }
        let Some(mut kernel) = settle_boot(sim)? else {
            sim.restart();
            continue;
        };
        client.need_subscribe = true;
        client.current_handle = None;

        let mut source = SimSource::new(Box::new(sim.clock()), sim.now().as_nanos() + 2000 * SEC);
        let sinks = source.sinks();
        let responses = source.responses();
        let t = sim.now().as_nanos();

        // Same classification as the epoch loop above and as this attempt's
        // own `kernel.run` below: a latched store makes this read fail, and
        // the recovery is a restart, not a universe failure.
        let subscribe = match push_subscribe(sim, &kernel, &mut source, path, client.cursor, t) {
            Ok(info) => {
                client.resync_if_cut(sim, info.1)?;
                info
            }
            Err(e) if is_fault_fallout(&e) => {
                sim.restart();
                continue;
            }
            Err(e) => return Err(format!("settle push_subscribe failed non-injected: {e}")),
        };

        let run_result = kernel.run(&mut source);
        let new_responses = responses
            .lock()
            .expect("responses lock poisoned by an earlier panic")
            .clone();
        client.absorb(
            sim,
            kernel.app().db(),
            &new_responses,
            Some(subscribe),
            &sinks,
        )?;
        if let Err(e) = run_result {
            if !is_fault_fallout(&e.to_string()) {
                return Err(format!("settle kernel.run failed non-injected: {e}"));
            }
            sim.restart();
            client.need_subscribe = true;
            client.current_handle = None;
        } else {
            clean_attempts += 1;
            sim.cover("realtime:settle-clean");
        }
        // The loop-top check never runs after the LAST attempt, whose give-up
        // would otherwise fall through to the unsettled verdict below.
        if sim.gave_up().is_some() {
            return Ok(client.trace);
        }
    }

    if !client.settled() {
        // `settle_boot` returns `None` only after every boot failure classified
        // as fault fallout (a non-injected failure returns `Err` at once), and the
        // two `continue` arms above are likewise fault-only. So zero clean attempts
        // means the client never once had a working node to catch up FROM — the
        // fact this loop's comment above says is NOT this property's subject.
        // Reporting it as `unsettled_report()` claimed the platform lost an event
        // on evidence that was never gathered (seed 0x22eac3ba9fa5bef7, pinned
        // above). A universe that adjudicated nothing gives up; it does not fail,
        // and it does not pass either. There is no rolled-back-ledger give-up:
        // every boundary re-syncs a cut a fault explains (`resync_if_cut`), as
        // the scoped sibling's do, so a ledgered id the node rolled back past is
        // one nothing explains, which fails below. That give-up reached 0 of
        // 3,328 universes over 26 bases once the flagship re-synced (S3b item 3).
        if clean_attempts == 0 {
            sim.give_up(format!(
                "no settle attempt completed without fault fallout over \
                 {SETTLE_ATTEMPTS} attempts — there was no node to catch up from ({})",
                client.unsettled_report()
            ));
            return Ok(client.trace);
        }
        return Err(client.unsettled_report());
    }
    client.give_up_if_nothing_adjudicated(sim);
    // The reach key the flagship's floor reads: one mark per frame the client
    // delivered and checked, in a universe that kept its verdict.
    if sim.gave_up().is_none() {
        for _ in &client.trace {
            sim.cover("realtime:frame-verified");
        }
    }
    Ok(client.trace)
}

// ------------------------------------------------------------------ tests

/// A durable refusal at the settle phase's boot ends it at once, by name (S5 re-review
/// R-M2). Seed `0x28a2433c2792b1b9`, at the batch's base, meets one there. No restart
/// clears it, and the loop used to spend its whole budget on it, then give
/// up as though every attempt had hit an injected fault ("no settle attempt
/// completed without fault fallout over 12 attempts").
#[test]
fn a_durable_refusal_at_the_settle_boot_ends_it_at_once_by_name() {
    const SEED: u64 = 0x28a2_433c_2792_b1b9;
    let mut state = SEED;
    let cfg_seed = charpente_sim::rng::splitmix64(&mut state);
    let mut cfg = FaultConfig::swarm(&mut charpente_sim::rng::Rng::new(cfg_seed));
    charpente_sim::sample_extended_fields(SEED, &mut cfg);
    let mut sim = Sim::with_faults(SEED, cfg);

    if let Err(e) = realtime_workload(&mut sim) {
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
fn reconnecting_client_misses_nothing_under_the_fault_matrix() {
    run_universes_swarm_with!(
        0x2EB2_0001,
        128,
        |_cfg: &mut FaultConfig| {},
        |sim: &mut Sim| realtime_workload(sim).map(|_trace| ()),
        require: [
            "boot:booted-after-retry",
            "realtime:resume-backlog",
            "realtime:gap",
            "realtime:stall-drop",
            "fault:subscriber_stall"
        ],
        floor: [("realtime:frame-verified", FLAGSHIP_FRAMES_VERIFIED_FLOOR)],
        ceiling: [("universe:gave-up", FLAGSHIP_GAVE_UP_CEILING)]
    );
}

/// Seed `0xe0779a282feb80eb` (D5, the S1 amend review's I1): a correct universe
/// the property's own client failed. Durable corruption cut an id the client had
/// already been delivered (id 3); the node re-minted it on its new timeline, and
/// the 409 that told the client its cursor had diverged sent it bare. When the
/// round that drew the 409 committed fewer re-mints than were cut, the bare
/// tail starts at or below the client's `last_id`, and the first re-mint
/// delivered failed `assert_strictly_increasing` ("duplicate or out-of-order
/// event id 3 ... previous id 3") although the platform did exactly what a
/// sanctioned fault left it to do. `resync_to_head` kept `last_id` across the
/// cut. The assertion pins the REASON (a cut seen and re-synced, a delivered id
/// at or above the cut head, a frame delivered below an earlier one) and that
/// the universe reaches a verdict; with the trim removed it fails with the text
/// above.
#[test]
fn a_bare_tail_after_a_cut_may_start_below_the_last_id_the_client_saw() {
    const SEED: u64 = 0xe077_9a28_2feb_80eb;
    let mut state = SEED;
    let cfg_seed = charpente_sim::rng::splitmix64(&mut state);
    let mut cfg = FaultConfig::swarm(&mut charpente_sim::rng::Rng::new(cfg_seed));
    charpente_sim::sample_extended_fields(SEED, &mut cfg);
    let mut sim = Sim::with_faults(SEED, cfg);

    DIVERGED_409_RESYNCS.with(|c| c.set(0));
    let trace = realtime_workload(&mut sim).unwrap_or_else(|e| {
        panic!("a cut re-minted below a delivered id must not fail the universe: {e}")
    });
    assert_eq!(sim.gave_up(), None, "and the universe reached a verdict");
    assert!(
        DIVERGED_409_RESYNCS.with(|c| c.get()) >= 1,
        "the diverged-409 refusal must have sent the client bare, or this universe no \
         longer pins the scenario"
    );
    assert!(
        durable_corruption_fired(&sim) && sim.coverage().contains_key("realtime:ledger-resynced"),
        "this universe's reason is a cut by durable corruption, seen and re-synced: {:?}",
        sim.coverage()
    );
    assert!(
        trace.windows(2).any(|w| w[1].0 <= w[0].0),
        "a frame must be delivered at or below an earlier one (the bare tail under the cut \
         id), or this universe no longer pins the scenario: {trace:?}"
    );
}

/// Seed `0x53b010000001` under `write_error` 0.3 and no other fault.
///
/// **This pin has been re-found many times, and that is worth knowing before
/// touching it.** The original was `0x22eac3ba9fa5bef7`, located by re-basing
/// this property (`CHARPENTE_BASE_SEED`, docs/LEDGER.md 2026-09-07); its
/// swarm-universe successors `0xbb93400e0bc80ca4`, `0xdec436b030427fc6`,
/// `0xd0fce29ba31210d0`, `0xaf1ba92646f937ea` and `0xe9110992f2280d36` each
/// stopped reproducing when a later change re-rolled or rescued its universe
/// (a pinned seed names a UNIVERSE). The last went with S3b item 3: its client
/// was owed only an event durable corruption had cut, so once the boundary
/// re-synced the cut it owed nothing, and the settle had nothing to catch up
/// on. The swarm no longer yields the scenario: 0 of 16,384 universes over the
/// 32 bases `0xA11CE002` to `0xA11CE021`, 512 each, and 0 of 3,328 over the
/// batch's base and 25 fresh ones. So this pin builds its universe instead: at
/// `write_error` 0.3, 110 of 400 seeds give this give-up, and this is the first.
/// The assertion pins the give-up REASON and that something was owed, so a
/// universe that reaches the arm with nothing owed cannot keep it green, and
/// with the zero-clean-attempts give-up removed it fails "client never caught
/// up", which is the regression.
///
/// The scenario: every one of `SETTLE_ATTEMPTS` attempts ends with `settle_boot`
/// returning `None`. The node never booted once, so the client never had a single
/// fault-free chance to receive what it was owed. The workload would otherwise
/// return `unsettled_report()`, whose text asserts the platform lost an event.
/// What actually happened is the fact the settle loop's own comment says is NOT
/// this property's subject: "there was no node to catch up FROM". `settle_boot`
/// exists to draw exactly that distinction, and returns `Ok(None)` only after
/// every boot failure classified as fault fallout — a non-injected failure
/// returns `Err` immediately. So a universe with no clean attempt adjudicated
/// NOTHING, and must give up rather than report a miss it never had the chance
/// to observe.
#[test]
fn a_universe_that_never_booted_a_node_gives_up_rather_than_reporting_a_miss() {
    const SEED: u64 = 0x53b0_1000_0001;
    let mut cfg = FaultConfig::none();
    cfg.write_error = 0.3;
    let mut sim = Sim::with_faults(SEED, cfg);

    let result = realtime_workload(&mut sim);
    assert!(
        result.is_ok(),
        "a universe in which no settle attempt ever ran cleanly must not report a missed \
         event — it never observed one: {:?}",
        result.err()
    );
    let reason = sim.gave_up().expect(
        "and it must be RECORDED as a give-up, not counted as a pass: a universe that \
         adjudicated nothing is not evidence for the property",
    );
    assert!(
        reason.contains("there was no node to catch up from")
            && reason.contains("first unaccounted matching id Some("),
        "and it must be THIS give-up — no settle attempt ever ran cleanly while the client \
         was owed an event — rather than some unrelated cause that happens to keep the test \
         green: {reason}"
    );
    assert!(!sim.coverage().contains_key("realtime:settle-clean"));
}

/// Seed `0x02b911a604c2ead4` (base `0x5E1F003A`, universe 226), the scenario
/// of S4's Finding D (2026-09-30). The settle phase has one clean attempt; with
/// `quiesce_subscriber_stalls` removed, an injected subscriber stall drops that
/// attempt's tail, it resumes from cursor 22, and id 23 never arrives. The
/// property excuses a stall, so that universe adjudicated nothing, yet it
/// failed "client never caught up": a harness gap, not lost delivery. The
/// settle runs with stalls quiesced, so that attempt delivers the backlog and
/// the universe passes. Re-pinned 2026-10-01 for S5 with D1, which re-key every
/// universe: the finding's `0xfddd6c7112629074` (base `0x5E1F0002`, universe
/// 70) no longer has a single clean settle attempt. Found by removing the
/// quiesce and scanning 64 fresh bases from `0x5E1F0002` for universes that
/// fail "client never caught up": this is the only one of 4,185 that inject
/// stalls.
#[test]
fn a_stall_cannot_defeat_the_only_clean_settle_attempt() {
    const SEED: u64 = 0x02b9_11a6_04c2_ead4;
    let mut state = SEED;
    let cfg_seed = charpente_sim::rng::splitmix64(&mut state);
    let mut cfg = FaultConfig::swarm(&mut charpente_sim::rng::Rng::new(cfg_seed));
    charpente_sim::sample_extended_fields(SEED, &mut cfg);
    assert!(
        cfg.subscriber_stall > 0.0,
        "this universe must inject stalls, or it no longer pins the scenario"
    );
    let mut sim = Sim::with_faults(SEED, cfg);

    let trace = realtime_workload(&mut sim).unwrap_or_else(|e| {
        panic!("a stall in the settle is excused, so it must not fail the universe: {e}")
    });
    assert_eq!(sim.gave_up(), None, "and the universe reached a verdict");
    assert_eq!(
        sim.coverage().get("realtime:settle-clean"),
        Some(&1),
        "the verdict must rest on one clean settle attempt, the one a stall dropped, or this \
         seed no longer pins the scenario: {:?}",
        sim.coverage()
    );
    assert!(
        trace.iter().any(|(id, _, _)| *id == 23),
        "id 23, the first matching id the failure named as unaccounted, must arrive: {trace:?}"
    );
}

/// Seed `0x6617ca6405d1b99e` (base `0x5E1F000D`, universe 127). A durable,
/// acked event is destroyed by `bit_flip` above the store's checkpoint horizon,
/// recovery cuts there, and the next emit re-mints id 0 with other content:
/// recorded (pong, {"n":"647"}), re-minted (ping, {"n":"697"}). From
/// 2026-09-30 until S3b item 3 the drain probe gave the universe up in the
/// first round after the restart, and this test pinned that give-up and its
/// stop. Now the subscribe that follows the recovery re-syncs the client to
/// the node's timeline at its boundary, as the scoped sibling's do: the
/// recorded id 0 is dropped, and its re-mint, which this subscription matches,
/// is owed and delivered, so the universe reaches a verdict that includes it.
/// With the re-sync standing in for a cut nothing explains it fails at that
/// boundary (`a_cut_nothing_explains_fails_the_flagship_by_name`).
#[test]
fn a_ledgered_event_cut_and_re_minted_is_resynced_and_its_re_mint_is_owed() {
    const SEED: u64 = 0x6617_ca64_05d1_b99e;
    let mut state = SEED;
    let cfg_seed = charpente_sim::rng::splitmix64(&mut state);
    let mut cfg = FaultConfig::swarm(&mut charpente_sim::rng::Rng::new(cfg_seed));
    charpente_sim::sample_extended_fields(SEED, &mut cfg);
    let mut sim = Sim::with_faults(SEED, cfg);

    let trace = realtime_workload(&mut sim)
        .expect("a cut durable corruption explains must not fail the universe");
    assert_eq!(
        sim.gave_up(),
        None,
        "re-synced to the node's timeline, it reaches a verdict"
    );
    assert!(
        durable_corruption_fired(&sim) && sim.coverage().contains_key("realtime:ledger-resynced"),
        "this universe's reason is a cut by durable corruption, seen and re-synced: {:?}",
        sim.coverage()
    );
    assert!(
        trace.contains(&(0, "ping", r#"{"n":"697"}"#.to_string())),
        "the re-mint at id 0 is owed, and delivered: {trace:?}"
    );
}

/// Seeds where the flagship meets a cut a fault explains, run with the cut
/// standing in for one nothing explains (S3b items 1 and 3; the stand-in is
/// `realtime_workload_judged` with a check that explains nothing). Each is then
/// a lost event, and must FAIL at the boundary that first sees it. Until item 1
/// the drain and settle probes gave such a universe up on any diverged entry:
/// `0x6617ca6405d1b99e` met it at the drain probe, and `0xb1bcbb46b1c81e2f`
/// (base `0x5E1F001D`, universe 6, found by scanning 25 bases) at the settle's
/// rolled-back probe ("ledger id 2 is behind this node's timeline").
#[test]
fn a_cut_nothing_explains_fails_the_flagship_by_name() {
    for (seed, id) in [
        (0x6617_ca64_05d1_b99e_u64, 0u64),
        (0xb1bc_bb46_b1c8_1e2f, 2),
    ] {
        let mut state = seed;
        let cfg_seed = charpente_sim::rng::splitmix64(&mut state);
        let mut cfg = FaultConfig::swarm(&mut charpente_sim::rng::Rng::new(cfg_seed));
        charpente_sim::sample_extended_fields(seed, &mut cfg);
        let mut sim = Sim::with_faults(seed, cfg);

        let err = realtime_workload_judged(&mut sim, |_| false)
            .expect_err("a cut nothing explains is a lost event");
        assert!(
            err.starts_with(&format!("ledger id {id} was recorded off this node"))
                && err.contains("no fault on this node explains the cut"),
            "seed {seed:#x}: {err}"
        );
        assert_eq!(
            sim.gave_up(),
            None,
            "seed {seed:#x}: a failure, never a give-up"
        );
    }
}

#[test]
fn realtime_frame_traces_replay_identically_for_the_same_seed() {
    charpente_sim::checks::replays_identically(0x2EB2_5EED, realtime_workload).unwrap();
}

/// The naive victim: a resume that reads from `cursor.id` instead of
/// `validate_subscribe`'s correct `cursor.id + 1` (kernel/mod.rs — untouched,
/// and unreachable to override from outside the crate) — redelivering the
/// cursor's own event. Built directly against the real `charpente.events`
/// table (`EventRow`/`View::scan_from`, the same primitives `deliver_unit`
/// uses), the discriminating-twin pattern from sub-projects 1-2
/// (`harness_catches_a_split_done_mark` in kernel.rs): drive real platform
/// primitives in the wrong order/position, and show the oracle catches it.
#[test]
fn the_harness_catches_a_resume_that_replays_the_cursor_event() {
    let sim = Sim::with_faults(0x2EB2_DEAD, FaultConfig::none());
    let mut app = App::open(
        Box::new(sim.disk()),
        Schema::new(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )
    .unwrap();
    for n in 0..4u64 {
        app.db_for_tests()
            .transact(|tx| {
                tx.emit(Ping { n: WireU64(n) });
                Ok::<(), std::io::Error>(())
            })
            .unwrap();
    }
    let snap = app.db_for_tests().snapshot().unwrap();
    let events: Vec<EventRow> = snap.scan_from(0, 64).unwrap();
    assert_eq!(events.len(), 4);

    // What a correct client would already have received before reconnecting:
    // the first two events.
    let mut last_id = None;
    for e in &events[..2] {
        assert_strictly_increasing(e.id.raw(), &mut last_id).unwrap();
    }
    let cursor = Cursor {
        id: events[1].id.raw(),
        seq: events[1].seq,
    };

    // THE BUG.
    let naive: Vec<EventRow> = snap.scan_from(cursor.id, 64).unwrap();

    let err = naive
        .iter()
        .find_map(|e| assert_strictly_increasing(e.id.raw(), &mut last_id).err())
        .expect("the harness must catch a resume that redelivers the cursor event");
    assert!(err.contains("duplicate"), "{err}");
    assert!(err.contains(&cursor.id.to_string()), "{err}");
}

/// The skip-direction naive victim: a resume that reads from `cursor.id + 2`
/// instead of `cursor.id + 1` — skipping the successor event (id
/// `cursor.id + 1`) outright rather than redelivering the cursor's own event
/// (the off-by-one test's victim, above). `assert_strictly_increasing` does
/// NOT catch this — ids still strictly ascend across the skip, which is
/// legal on its own. Only a membership-based `settled()` can: it finds the
/// skipped id sitting matched-in-the-ledger-but-never-verified at
/// quiescence. RED evidence (T6 review): against the OLD high-water
/// `verified_through` bound, this exact scenario passed `settled()` — the
/// later ids' verification retroactively excused the skipped one. The fixed
/// `first_unaccounted`/`verified` membership check is what turns that into a
/// named failure below.
#[test]
fn the_harness_catches_a_resume_that_skips_the_successor_event() {
    let sim = Sim::with_faults(0x2EB2_51C1, FaultConfig::none());
    let mut app = App::open(
        Box::new(sim.disk()),
        Schema::new(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )
    .unwrap();
    for n in 0..6u64 {
        app.db_for_tests()
            .transact(|tx| {
                tx.emit(Ping { n: WireU64(n) });
                Ok::<(), std::io::Error>(())
            })
            .unwrap();
    }
    let snap = app.db_for_tests().snapshot().unwrap();
    let events: Vec<EventRow> = snap.scan_from(0, 64).unwrap();
    assert_eq!(events.len(), 6);

    let mut client = Client::new(PING_ONLY, None);
    client.sync_ledger(app.db_for_tests()).unwrap();

    for e in &events[..2] {
        let json = serde_json::to_string(&Ping::decode(&mut &e.payload[..]).unwrap())
            .expect("Ping serializes");
        client.pending.push_back((e.id.raw(), "ping", json));
    }
    client.drain().unwrap();
    let cursor_id = events[1].id.raw();

    // THE BUG: resume at cursor.id + 2 instead of cursor.id + 1 — the
    // successor event (id = cursor.id + 1) is never requested and never
    // delivered.
    for e in events.iter().filter(|e| e.id.raw() >= cursor_id + 2) {
        let json = serde_json::to_string(&Ping::decode(&mut &e.payload[..]).unwrap())
            .expect("Ping serializes");
        client.pending.push_back((e.id.raw(), "ping", json));
    }
    client.drain().unwrap();

    let missing = cursor_id + 1;
    assert!(
        !client.settled(),
        "the harness must catch a resume that skips event id {missing}"
    );
    assert_eq!(
        client.first_unaccounted(),
        Some(missing),
        "settled() must name the skipped id precisely, not just detect *a* gap"
    );
    let report = client.unsettled_report();
    assert!(
        report.contains(&missing.to_string()),
        "the failure report must name the missing id: {report}"
    );
}

/// The discriminating twin for the retention-gap confirmation `drain` grew
/// this interlude (the fix for seed 0xbe2f0aa2f20f34c7: a live-delivered
/// frame whose ledger row aged out before `sync_ledger` recorded it used to
/// stall the client's drain forever). The confirmation resolves a
/// delivered-then-aged id BY ID — but it must resolve ONLY ids the client
/// actually RECEIVED, never mask a real delivery-path interior skip. Both
/// halves below face the identical aged-out ledger (real checkpoint-driven
/// GC, `event_retention: 4`); the ONLY difference is whether the still-
/// RETAINED id `floor` was delivered.
///   - honest: every id up to the tail was received live -> the below-floor
///     ids confirm, the retained ids content-verify, the client settles.
///   - skip: the retained id `floor` is never delivered -> the confirmation
///     cannot touch it (it was never in `pending`), and the independent
///     membership check `first_unaccounted` names it, so the client stays
///     RED. This is the same over-excusing failure the T6 review caught the
///     old high-water bound committing, now guarded at the new mechanism.
///     Proven load-bearing by a guard-revert (interlude report): dropping
///     the below-floor arm makes the HONEST half stop settling, while the
///     skip half stays RED either way — the confirmation resolves
///     received-and-aged ids and nothing else.
#[test]
fn the_below_floor_confirm_resolves_only_received_frames() {
    let sim = Sim::with_faults(0x2EB2_6A9F, FaultConfig::none());
    let mut app = realtime_app(&sim).unwrap();
    // Enough emits that checkpoint-driven GC (event_retention: 4,
    // checkpoint_every: 2) ages the oldest rows out while keeping the newest
    // — a real floor > 0 to confirm delivered-then-aged ids against.
    for n in 0..8u64 {
        app.db_for_tests()
            .transact(|tx| {
                tx.emit(Ping { n: WireU64(n) });
                Ok::<(), std::io::Error>(())
            })
            .unwrap();
    }
    let floor = app.db_for_tests().event_floor().unwrap();
    let next = app.db_for_tests().next_event_id().unwrap();
    assert!(
        floor > 0 && floor < next,
        "test needs GC to have aged some rows out but kept others: floor={floor}, next={next}"
    );

    // The delivered frame for an id — content identical to what the store
    // materialized (each emit's `Ping.n` is its own event id, ids dense).
    let frame = |id: u64| -> (u64, &'static str, String) {
        (
            id,
            "ping",
            serde_json::to_string(&Ping { n: WireU64(id) }).expect("Ping serializes"),
        )
    };

    // ---- honest: every id in [0, next) was received live ----
    let mut honest = Client::new(PING_ONLY, None);
    for id in 0..next {
        honest.pending.push_back(frame(id));
    }
    honest.sync_and_drain(app.db_for_tests()).unwrap();
    assert!(
        honest.settled(),
        "delivered-then-aged ids must confirm and the client settle: {}",
        honest.unsettled_report()
    );
    assert!(
        honest.verified.iter().any(|&id| id < floor),
        "the below-floor confirmation must actually have fired here (an aged-out, \
         never-in-ledger id was confirmed) — otherwise this test proves nothing"
    );

    // ---- skip: the still-RETAINED id `floor` is never delivered ----
    let skip = floor;
    let mut skipped = Client::new(PING_ONLY, None);
    for id in 0..next {
        if id == skip {
            continue;
        }
        skipped.pending.push_back(frame(id));
    }
    skipped.sync_and_drain(app.db_for_tests()).unwrap();
    assert!(
        !skipped.settled(),
        "a never-received still-retained id must keep the client unsettled — the retention \
         confirmation must not mask a real delivery-path interior skip"
    );
    assert_eq!(
        skipped.first_unaccounted(),
        Some(skip),
        "the skipped id must be named precisely, not merely detected"
    );
    assert!(
        skipped.unsettled_report().contains(&skip.to_string()),
        "the failure report must name the missing id: {}",
        skipped.unsettled_report()
    );
}

/// The discriminating twin for the durable-corruption divergence arm (T6
/// interlude investigation): the exact same 409 shape, but in a
/// corruption-free universe the gate must stay OFF — `Client::absorb` still
/// hard-fails, proving the excuse can never mask a real (non-corruption)
/// loss. `FaultConfig::none()` never touches `bit_flip`/`misdirected_write`,
/// so `durable_corruption_fired` reads `false` and the new arm's guard
/// doesn't match — the response falls through to the same catch-all `Err`
/// every unrecognized subscribe status hits.
#[test]
fn the_harness_refuses_a_diverged_409_without_durable_corruption() {
    let sim = Sim::with_faults(0x2EB2_C0DE, FaultConfig::none());
    let app = App::open(
        Box::new(sim.disk()),
        Schema::new(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )
    .unwrap();
    assert!(
        !durable_corruption_fired(&sim),
        "a fresh, fault-free sim must not report durable corruption"
    );

    let mut client = Client::new(PING_ONLY, None);
    let body = br#"{"error":"cursor 0.2 diverged from this node's timeline (next event id is 0) -- reconnect without a cursor, wait for the stream to open, then read current state from the same node, holding every frame that arrives until that read is installed and then applying the held frames and every later one over it in order, idempotently"}"#.to_vec();
    let sinks: SinkHandles = Arc::new(Mutex::new(Vec::new()));
    let err = client
        .absorb(&sim, app.db(), &[(409, body)], Some((true, 0)), &sinks)
        .expect_err("the corruption-gated excuse must not fire without durable corruption");
    assert!(err.contains("diverged"), "{err}");
}

/// Group-commit T6 regression: a subscribe arriving in the same instant as an
/// emit must still receive that emit.
///
/// The kernel's request arm drains many requests into one group, but a
/// subscribe's cursor is resolved AFTER the commit — `validate_subscribe`
/// needs `&Db`, which the open group holds. So a sibling staged behind a
/// subscribe would have its events committed before that resolution, the tail
/// would start past them, and the subscriber would silently miss events it
/// must receive. A gap: precisely what this file's membership oracle forbids.
/// The arm therefore ends a batch at a subscribe.
///
/// Found the honest way — by an existing test going red — but by
/// `tail_encode_error_is_whole_node_fatal` (since renamed
/// `an_unencodable_event_fails_its_tail_not_the_node`), whose name promises
/// nothing about batch ordering, so the property gets its own pin here
/// rather than living on as a side effect of that one.
#[test]
fn a_subscribe_still_sees_an_emit_that_arrives_with_it() {
    let sim = Sim::with_faults(0x2EB2_B471, FaultConfig::none());
    let app = realtime_app(&sim).unwrap();
    let mut kernel = Kernel::new(app, Box::new(sim.clock())).unwrap();

    let mut source = SimSource::new(Box::new(sim.clock()), sim.now().as_nanos() + 10 * SEC);
    let t = sim.now().as_nanos();
    // Same instant: both are ready in the same step, so the arm would drain
    // them into one batch if the subscribe did not end it.
    source.push_at(t, Request::new(Method::Get, "/feed-ping"));
    source.push_at(
        t,
        Request::new(Method::Post, "/ping").json(&EmitBody {
            n: WireU64(7),
            key: None,
        }),
    );
    let sinks = source.sinks();

    // Run until the script is exhausted and the tail has been serviced.
    for _ in 0..8 {
        match kernel.step(&mut source) {
            Ok(_) => {}
            Err(e) => panic!("no step may fail here: {e}"),
        }
    }

    let handles = sinks.lock().expect("sinks lock");
    assert_eq!(handles.len(), 1, "exactly one subscribe opened a sink");
    let frames = handles[0].frames();
    // Exact, not merely non-empty (T6 review M-1): `!frames.is_empty()` would
    // also pass on a `charpente.gap` terminal frame or any wrong event, and
    // "could this be green while the property is false" is the standard here.
    assert_eq!(
        frames.len(),
        1,
        "exactly the one emit that arrived with the subscribe — an EMPTY tail \
         means its cursor resolved PAST the sibling's event, which is a silent \
         gap: {:?}",
        frames.iter().map(|f| (f.name, &f.json)).collect::<Vec<_>>()
    );
    assert_eq!(frames[0].name, "ping", "the Ping event, not a gap frame");
    // Exact, not `contains('7')` (review round 1, finding 4): a substring test
    // for one digit also passes on `{"n":17}`, on `{"n":7}`, and on any other
    // encoding of the field — including the one this event does NOT use.
    assert_eq!(
        frames[0].json, r#"{"n":"7"}"#,
        "carrying the emitted payload n=7"
    );
}

/// The literal bytes a subscriber receives, pinned as bytes (review round 1,
/// finding 4). Task 6 moved `Ping.n` from a bare `u64` to `WireU64`, which
/// changed this document: the frame body is now `{"n":"7"}`, a decimal STRING,
/// not `{"n":7}`. That is the most product-visible thing task 6 changed, and
/// every other assertion around it holds only by COMPOSITION —
/// `serde_json::to_string(&Ping { .. })` on both sides agrees with itself no
/// matter what the encoding is, so all of them would stay green if the wire
/// form silently reverted. This one asserts the delivered frame directly, at
/// the boundary a browser's `JSON.parse` reads.
#[test]
fn a_delivered_frame_carries_its_64_bit_field_as_a_decimal_string() {
    let sim = Sim::with_faults(0x2EB2_D511, FaultConfig::none());
    let app = realtime_app(&sim).unwrap();
    let mut kernel = Kernel::new(app, Box::new(sim.clock())).unwrap();

    let mut source = SimSource::new(Box::new(sim.clock()), sim.now().as_nanos() + 10 * SEC);
    let t = sim.now().as_nanos();
    source.push_at(t, Request::new(Method::Get, "/feed-ping"));
    source.push_at(
        t,
        Request::new(Method::Post, "/ping").json(&EmitBody {
            // Above 2^53: the exact value a JSON number would have corrupted,
            // which is why the carrier exists at all.
            n: WireU64(9_007_199_254_740_993),
            key: None,
        }),
    );
    let sinks = source.sinks();
    for _ in 0..8 {
        kernel.step(&mut source).expect("no step may fail here");
    }

    let handles = sinks.lock().expect("sinks lock");
    let frames = handles[0].frames();
    assert_eq!(frames.len(), 1, "exactly one delivered frame");
    assert_eq!(frames[0].name, "ping");
    assert_eq!(
        frames[0].json, r#"{"n":"9007199254740993"}"#,
        "the frame body is the decimal STRING form — a JSON number here would \
         reach a JavaScript client as 9007199254740992"
    );
}

/// Spec §3/§6.5: an encode failure fails its own tail, never the node — this
/// test was `tail_encode_error_is_whole_node_fatal`, the pin of the decision
/// payload shapes reversed (docs/LEDGER.md:286's per-tail follow-up). The
/// failing encoder is hand-built (`FailingFeed`), which covers an encoder
/// that fails regardless of the payload's type — no shape check, no field
/// ever read. `a_real_events_serialize_failure_fails_its_tail_not_the_node`
/// (below) covers the other case, a real `#[event]` whose own field
/// `Serialize` impl fails on its own bytes. The stream ends with
/// `charpente.error`, and the node keeps serving.
struct FailingFeed;

fn failing_feed_glue(
    _tx: &mut Tx,
    _ctx: &Ctx,
    _captures: &[String],
    cursor: Option<Cursor>,
) -> Result<Subscription, ApiError> {
    let mut sub = Subscription::of::<NoteAdded>().cursor(cursor);
    sub.entries[0].encode_json =
        |_: &[u8]| -> io::Result<String> { Err(io::Error::other("encode exploded")) };
    Ok(sub)
}

impl RouteDef for FailingFeed {
    fn route() -> Route {
        // Untyped glue, not a described route: a hand-written `RouteDef`
        // with no typed handler (Amendment D, task-5-brief.md).
        Route {
            method: Method::Get,
            pattern: "/feed-failing",
            auth: false,
            public_scope: None,
            cross_tenant: false,
            name: "FailingFeed",
            glue: RouteGlue::Sse(failing_feed_glue),
            params: &[],
            request: None,
            response: || TypeSchema::Null,
            error: || <ApiError as Wire>::reference(),
            register_types: |_| {},
            emits: &[],
        }
    }
}

#[event(global)]
struct NoteAdded {
    n: WireU64,
}

#[derive(WireDerive, Serialize, Deserialize)]
struct NoteBody {
    n: WireU64,
}

#[route(POST "/note", public)]
fn emit_note(tx: &mut Tx, body: NoteBody) -> Result<(), ApiError> {
    tx.emit(NoteAdded { n: body.n });
    Ok(())
}

#[test]
fn an_unencodable_event_fails_its_tail_not_the_node() {
    let sim = Sim::with_faults(0x2EB2_FA11, FaultConfig::none());
    let app = App::open(
        Box::new(sim.disk()),
        Schema::new(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )
    .unwrap()
    .mount(vec![FailingFeed::route()])
    .mount(routes![emit_note]);
    let mut kernel = Kernel::new(app, Box::new(sim.clock())).unwrap();
    let (_, sinks) = run_script(
        &mut kernel,
        &sim,
        vec![
            Request::new(Method::Get, "/feed-failing"),
            Request::new(Method::Post, "/note").json(&NoteBody { n: WireU64(1) }),
        ],
    );
    let rows: Vec<EventRow> = kernel
        .app()
        .db()
        .snapshot()
        .unwrap()
        .scan_from(0, 8)
        .unwrap();
    assert_eq!(
        frames_of(&sinks.lock().unwrap()[0]),
        vec![(
            rows[0].id.raw(),
            rows[0].seq,
            "charpente.error".to_string(),
            format!(
                r#"{{"event":"note_added","id":{},"error":"encode exploded"}}"#,
                rows[0].id.raw()
            ),
        )],
        "one terminal error frame naming the event and the row"
    );
    let (responses, _) = run_script(
        &mut kernel,
        &sim,
        vec![Request::new(Method::Post, "/note").json(&NoteBody { n: WireU64(2) })],
    );
    assert_eq!(statuses(&responses), vec![200], "the node keeps serving");
}

/// A field type whose `Serialize` always fails: `#[event]`
/// only requires `Codec`/`Wire`/`Serialize` on its fields (charpente-macros
/// event.rs), and `Wire` is an open `pub trait`, so a hand-written field type
/// can make `Subscription::of`'s REAL encoder fail on its own bytes — not
/// `FailingFeed`'s hand-built stub. `Codec`/`Wire` are modeled on `WireU64`:
/// a fixed-shape carrier with nothing to decide, so only `Serialize` is
/// interesting here.
#[derive(Clone, Copy)]
struct Poisoned;

impl Codec for Poisoned {
    fn encode(&self, _out: &mut Vec<u8>) {}
    fn decode(_input: &mut &[u8]) -> io::Result<Poisoned> {
        Ok(Poisoned)
    }
}

impl Wire for Poisoned {
    const WIRE_NAME: &'static str = "";
    fn schema() -> TypeSchema {
        TypeSchema::Null
    }
    fn register(_reg: &mut charpente::wire::TypeRegistry) {}
}

impl Serialize for Poisoned {
    fn serialize<S: serde::Serializer>(&self, _s: S) -> Result<S::Ok, S::Error> {
        Err(serde::ser::Error::custom(
            "poisoned field refuses to serialize",
        ))
    }
}

#[event(global)]
struct PoisonedEvent {
    field: Poisoned,
}

#[route(POST "/poison", public)]
fn emit_poison(tx: &mut Tx) -> Result<(), ApiError> {
    tx.emit(PoisonedEvent { field: Poisoned });
    Ok(())
}

#[route(SSE "/feed-poisoned", public, emits(PoisonedEvent))]
fn feed_poisoned(tx: &mut Tx, ctx: &Ctx, cursor: Option<Cursor>) -> Result<Subscription, ApiError> {
    let _ = (tx, ctx);
    Ok(Subscription::of::<PoisonedEvent>().cursor(cursor))
}

/// The same `charpente.error` frame `an_unencodable_event_…` pins, but
/// reached through a REAL `#[event]`/`Subscription::of` encoder (`ev_json`,
/// http/mod.rs) rather than `FailingFeed`'s hand-built stub — this is what
/// pins the JSON-encode failure text GUIDE §11 quotes
/// (`event <name> failed JSON encoding: <message> — every #[event] payload
/// must be plainly JSON (GUIDE §11); fix the emitting code`), spelled out
/// independently of the product's own `format!`.
#[test]
fn a_real_events_serialize_failure_fails_its_tail_not_the_node() {
    let sim = Sim::with_faults(0x2EB2_FA13, FaultConfig::none());
    let app = App::open(
        Box::new(sim.disk()),
        Schema::new(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )
    .unwrap()
    .mount(routes![feed_poisoned, emit_poison]);
    let mut kernel = Kernel::new(app, Box::new(sim.clock())).unwrap();
    let (_, sinks) = run_script(
        &mut kernel,
        &sim,
        vec![
            Request::new(Method::Get, "/feed-poisoned"),
            Request::new(Method::Post, "/poison"),
        ],
    );
    let rows: Vec<EventRow> = kernel
        .app()
        .db()
        .snapshot()
        .unwrap()
        .scan_from(0, 8)
        .unwrap();
    assert_eq!(
        frames_of(&sinks.lock().unwrap()[0]),
        vec![(
            rows[0].id.raw(),
            rows[0].seq,
            "charpente.error".to_string(),
            format!(
                r#"{{"event":"poisoned_event","id":{},"error":"event poisoned_event failed JSON encoding: poisoned field refuses to serialize — every #[event] payload must be plainly JSON (GUIDE §11); fix the emitting code"}}"#,
                rows[0].id.raw()
            ),
        )],
        "the real Subscription::of encoder fails on its own bytes, byte-exact"
    );
    let (responses, _) = run_script(
        &mut kernel,
        &sim,
        vec![Request::new(Method::Post, "/poison")],
    );
    assert_eq!(statuses(&responses), vec![200], "the node keeps serving");
}

// ------------------------------------ mixed shapes under the fault matrix

/// How many resume attempts a universe gets through injected faults before it
/// gives up. Only an attempt whose `kernel.run` returns without fault fallout
/// is adjudicated.
const SHAPES_SETTLE_ATTEMPTS: u32 = 12;

#[derive(Clone, Copy, Debug)]
enum Era {
    /// The old binary: `incident_opened` in its v1 shape.
    V1,
    /// The new binary: `incident_opened` in its v2 shape.
    V2,
}

/// `settle_boot` over `shapes_app` — the file's own boot-through-faults idiom,
/// copied like `scoped_settle_boot` rather than parameterized, so the
/// flagship's boot path is untouched.
fn shapes_boot(sim: &Sim) -> Result<Option<Kernel>, String> {
    for try_n in 0..SETTLE_BOOT_TRIES {
        if try_n > 0 && try_n.is_multiple_of(SETTLE_CYCLE_EVERY) {
            sim.restart();
        }
        let app = match shapes_app(sim) {
            Ok(app) => app,
            Err(e) => {
                // No restart clears a durable refusal, and only a fired
                // durable-corruption fault excuses one (S3b item 0). The
                // workload checks for the give-up before every phase.
                if durable_refusal(sim, &e.to_string())? {
                    return Ok(None);
                }
                if !is_fault_fallout(&e.to_string()) {
                    return Err(format!("shapes open failed non-injected: {e}"));
                }
                continue;
            }
        };
        match Kernel::new(app, Box::new(sim.clock())) {
            Ok(kernel) => return Ok(Some(kernel)),
            Err(e) if is_fault_fallout(&e.to_string()) => {}
            Err(e) => return Err(format!("shapes Kernel::new failed non-injected: {e}")),
        }
    }
    Ok(None)
}

/// One universe of spec §6.3. The old binary emits `incident_opened` in its v1
/// shape (and pongs); the node restarts as the new binary, which emits the v2
/// shape; then a client resumes `/v2/incidents-and-pongs` from a cursor drawn
/// from the durable log. Every phase runs under the fault matrix. The verdict
/// is taken against that log.
fn shapes_workload(sim: &mut Sim) -> Result<(), String> {
    let entropy = sim.entropy();
    for era in [Era::V1, Era::V2] {
        if sim.gave_up().is_some() {
            return Ok(());
        }
        // Sized to redb 4.3.0's disk-op volume per round: DESIGN §1, "countdowns".
        sim.crash_disk_after(40 + entropy.next_u64() % 320);
        if let Some(mut kernel) = shapes_boot(sim)? {
            let mut source = SimSource::new(Box::new(sim.clock()), sim.now().as_nanos() + 60 * SEC);
            let t = sim.now().as_nanos();
            for i in 0..(2 + entropy.next_u64() % 5) {
                let path = match (era, entropy.next_u64() % 3) {
                    (_, 0) => "/pong",
                    (Era::V1, _) => "/v1/incident",
                    (Era::V2, _) => "/v2/incident",
                };
                source.push_at(
                    t + i * SEC,
                    Request::new(Method::Post, path).json(&EmitBody {
                        n: WireU64(entropy.next_u64() % 1000),
                        key: None,
                    }),
                );
            }
            if let Err(e) = kernel.run(&mut source)
                && !is_fault_fallout(&e.to_string())
            {
                return Err(format!("{era:?} kernel.run failed non-injected: {e}"));
            }
        }
        // The deploy between eras — or the crash, whichever came first.
        sim.restart();
    }
    for _ in 0..SHAPES_SETTLE_ATTEMPTS {
        if sim.gave_up().is_some() {
            return Ok(());
        }
        let Some(mut kernel) = shapes_boot(sim)? else {
            sim.restart();
            continue;
        };
        let rows: Vec<EventRow> = match kernel
            .app()
            .db()
            .snapshot()
            .and_then(|s| s.scan_from(0, 1024))
        {
            Ok(rows) => rows,
            Err(e) if is_fault_fallout(&e.to_string()) => {
                sim.restart();
                continue;
            }
            Err(e) => return Err(format!("event scan failed non-injected: {e}")),
        };
        if rows.is_empty() {
            sim.give_up("no event survived the two eras — nothing to resume across");
            return Ok(());
        }
        // `EventRow` is not `Clone`: the anchor is an index into `rows`.
        let anchor_at = (entropy.next_u64() % rows.len() as u64) as usize;
        let mut source = SimSource::new(Box::new(sim.clock()), sim.now().as_nanos() + 60 * SEC);
        // Whether the stall closure ever refused a send: the one thing that
        // makes a short stream legal. `closed()` cannot say it, because the
        // terminal `charpente.reshaped` frame drops the tail too. The wrapper
        // calls the closure exactly once per send, so the draws are unchanged.
        let stalled = Arc::new(Mutex::new(false));
        let (fired, mut stall) = (
            stalled.clone(),
            stall_closure(sim.entropy(), sim.subscriber_stall()),
        );
        source.push_stall(Box::new(move || {
            let full = stall();
            if full {
                *fired.lock().expect("stall flag lock") = true;
            }
            full
        }));
        source.push_at(
            sim.now().as_nanos(),
            Request::new(
                Method::Get,
                format!(
                    "/v2/incidents-and-pongs?cursor={}",
                    cursor_of(&rows[anchor_at])
                ),
            ),
        );
        let (responses, sinks) = (source.responses(), source.sinks());
        match kernel.run(&mut source) {
            Ok(()) => {
                let stalled = *stalled.lock().expect("stall flag lock");
                match shapes_verdict(sim, &rows, &rows[anchor_at], &responses, &sinks, stalled)? {
                    Adjudged::Settled => return Ok(()),
                    Adjudged::Retry => sim.restart(),
                }
            }
            Err(e) if is_fault_fallout(&e.to_string()) => sim.restart(),
            Err(e) => return Err(format!("resume kernel.run failed non-injected: {e}")),
        }
    }
    sim.give_up(format!(
        "no resume completed without fault fallout in {SHAPES_SETTLE_ATTEMPTS} attempts"
    ));
    Ok(())
}

/// The oracle, against the retained log `rows` the resume ran over. A cursor
/// ON a foreign row is refused with the named 409. Otherwise the frames are a
/// prefix of exactly what a resume must produce — every later v2 or pong row,
/// decoded, up to the first foreign row, which arrives as one
/// `charpente.reshaped` frame that ends the stream. A strict prefix is legal
/// only when `stalled`, meaning the sink's stall closure refused a send. A
/// closed tail is not evidence of that, since the terminal frame closes it
/// too. Nothing is skipped and nothing is decoded from a foreign row. A
/// subscribe answered 500 with fault fallout — an injected fault reached the
/// cursor read — is the flagship's retry, not a verdict.
fn shapes_verdict(
    sim: &Sim,
    rows: &[EventRow],
    anchor: &EventRow,
    responses: &RecordedResponses,
    sinks: &SinkHandles,
    stalled: bool,
) -> Result<Adjudged, String> {
    let v1 = <shapes_v1::IncidentOpened as Event>::FINGERPRINT;
    let v2 = <shapes_v2::IncidentOpened as Event>::FINGERPRINT;
    let pong = <Pong as Event>::FINGERPRINT;
    let foreign = |row: &EventRow| row.name == "incident_opened" && row.shape != Some(v2);
    // `foreign` reads the platform's own stored tag, so the tag is checked
    // against the bytes before anything trusts it: every row carries a shape
    // this workload writes under its name, and decodes as exactly that shape.
    // A v2 row mis-tagged v1 leaves 8 bytes behind and fails here, instead of
    // drawing a reshaped frame the oracle would agree with. Each entry is the
    // row's JSON as a v2 subscriber must see it, or `None` for a foreign row.
    let mut decoded = Vec::with_capacity(rows.len());
    for row in rows {
        decoded.push(match (row.name.as_str(), row.shape) {
            ("incident_opened", Some(tag)) if tag == v1 => {
                decode_exact::<shapes_v1::IncidentOpened>(row, tag)?;
                None
            }
            ("incident_opened", Some(tag)) if tag == v2 => Some(
                serde_json::to_string(&decode_exact::<shapes_v2::IncidentOpened>(row, tag)?)
                    .expect("a v2 incident serializes"),
            ),
            ("pong", Some(tag)) if tag == pong => Some(
                serde_json::to_string(&decode_exact::<Pong>(row, tag)?).expect("a pong serializes"),
            ),
            (name, shape) => {
                let recorded = match shape {
                    Some(tag) => format!("{tag:#x}"),
                    None => "no shape".to_string(),
                };
                return Err(format!(
                    "row {} is a {name:?} event tagged {recorded} — this workload writes \
                     incident_opened tagged {v1:#x} (v1) or {v2:#x} (v2) and pong tagged \
                     {pong:#x}, nothing else; {SHAPE_TAG_CONTRACT}",
                    row.id.raw()
                ));
            }
        });
    }
    let responses = responses.lock().expect("responses lock").clone();
    let [(status, body)] = responses.as_slice() else {
        return Err(format!(
            "expected exactly the subscribe's response, got {responses:?}"
        ));
    };
    if *status == 500 && is_fault_fallout(&String::from_utf8_lossy(body)) {
        return Ok(Adjudged::Retry);
    }
    if foreign(anchor) {
        let expected = reshaped_409(anchor, v2);
        if *status != 409 || error_text(body) != expected {
            return Err(format!(
                "a cursor on foreign row {} must be refused 409 {expected:?}, got {status} {}",
                anchor.id.raw(),
                String::from_utf8_lossy(body)
            ));
        }
        sim.cover("shapes:reshaped-409");
        return Ok(Adjudged::Settled);
    }
    if *status != 200 {
        return Err(format!(
            "a resume from a v2 or pong cursor must open the stream, got {status} {}",
            String::from_utf8_lossy(body)
        ));
    }
    let mut expected = Vec::new();
    for (row, json) in rows
        .iter()
        .zip(&decoded)
        .filter(|(r, _)| r.id.raw() > anchor.id.raw())
    {
        if foreign(row) {
            expected.push(reshaped_frame(row));
            break;
        }
        let json = json
            .clone()
            .expect("the tag check decoded every non-foreign row");
        expected.push((row.id.raw(), row.seq, row.name.clone(), json));
    }
    let handles = sinks.lock().expect("sinks lock");
    let [handle] = handles.as_slice() else {
        return Err(format!("expected one stream, got {}", handles.len()));
    };
    let got = frames_of(handle);
    if got.len() > expected.len() || got[..] != expected[..got.len()] {
        return Err(format!(
            "the resume from {} diverged from the log:\n  expected (a prefix of) {expected:?}\n  got {got:?}",
            cursor_of(anchor)
        ));
    }
    // Two-sided. A refused send ends the tail (every send site in
    // `deliver_unit` returns `Ok(false)` on it), so a stall always leaves the
    // stream short, and a short stream always needs a stall.
    if (got.len() < expected.len()) != stalled {
        return Err(if stalled {
            format!(
                "the resume from {} holds every frame the log owes, yet its sink refused a send \
                 — a refused frame was redelivered or an extra frame was sent: got {got:?}",
                cursor_of(anchor)
            )
        } else {
            format!(
                "the resume from {} stopped short with no stall to explain it: got {got:?} of {expected:?}",
                cursor_of(anchor)
            )
        });
    }
    if stalled {
        sim.cover("fault:subscriber_stall");
    }
    if got.last().is_some_and(|f| f.2 == RESHAPED_FRAME) {
        sim.cover("shapes:reshaped-frame");
    }
    if got.iter().any(|f| f.2 == "incident_opened") {
        sim.cover("shapes:v2-delivered");
    }
    Ok(Adjudged::Settled)
}

/// What one resume attempt's verdict came to: adjudicated, or fault fallout
/// the workload retries within its settle budget.
#[derive(Debug, PartialEq)]
enum Adjudged {
    Settled,
    Retry,
}

/// The contract `shapes_verdict`'s tag check holds the platform to.
const SHAPE_TAG_CONTRACT: &str = "a row's stored shape tag must name the shape its bytes were \
                                  written under (payload-shapes spec §2)";

/// `row`'s payload decoded as `T`, the shape its tag `tag` names, with every
/// byte consumed. Either failure means the stored tag does not match the bytes.
fn decode_exact<T: Codec>(row: &EventRow, tag: u64) -> Result<T, String> {
    let input = &mut &row.payload[..];
    let value = T::decode(input).map_err(|e| {
        format!(
            "row {} is tagged {tag:#x} yet its payload does not decode as that shape: {e} — \
             {SHAPE_TAG_CONTRACT}",
            row.id.raw()
        )
    })?;
    if !input.is_empty() {
        return Err(format!(
            "row {} is tagged {tag:#x} yet its payload leaves {} bytes behind after decoding \
             as that shape — {SHAPE_TAG_CONTRACT}",
            row.id.raw(),
            input.len()
        ));
    }
    Ok(value)
}

/// A durable refusal at a mixed-shapes boot gives the universe up at once, by
/// name (S3b item 0). Seed `0x6439126a0a64fc94`, universe 105 at the batch's
/// base, meets one there. `shapes_boot` used to send it through
/// `is_fault_fallout`, which excused it by its text alone, so every boot of
/// every phase spent its whole budget on bytes no restart clears.
#[test]
fn a_durable_refusal_at_a_shapes_boot_gives_up_at_once_by_name() {
    const SEED: u64 = 0x6439_126a_0a64_fc94;
    let mut state = SEED;
    let cfg_seed = charpente_sim::rng::splitmix64(&mut state);
    let mut cfg = FaultConfig::swarm(&mut charpente_sim::rng::Rng::new(cfg_seed));
    charpente_sim::sample_extended_fields(SEED, &mut cfg);
    let mut sim = Sim::with_faults(SEED, cfg);

    if let Err(e) = shapes_workload(&mut sim) {
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

/// Spec §6.3's property. The universe count was set by timing one batch (see
/// the commit message); the seed base is fixed and replays with
/// `CHARPENTE_SEED`/`CHARPENTE_BASE_SEED` like every swarm here.
#[test]
fn a_resume_across_mixed_shapes_is_exact_or_named_under_the_fault_matrix() {
    run_universes_swarm_with!(
        0x2EB2_5A10,
        180,
        |_cfg: &mut FaultConfig| {},
        |sim: &mut Sim| shapes_workload(sim),
        require: [
            "shapes:reshaped-frame",
            "shapes:reshaped-409",
            "shapes:v2-delivered",
            "fault:subscriber_stall"
        ],
        floor: [
            ("shapes:reshaped-frame", SHAPES_RESHAPED_FRAME_FLOOR),
            ("shapes:reshaped-409", SHAPES_RESHAPED_409_FLOOR),
            ("shapes:v2-delivered", SHAPES_V2_DELIVERED_FLOOR)
        ]
    );
}

/// The floors below are exact counts, so the batch must replay: one universe
/// run twice from one seed gives the same coverage and the same durable
/// state. The seed's universe resumes across the shape change and ends in the
/// named frame, so the replay covers the property's own outcome.
#[test]
fn mixed_shape_resumes_replay_identically_for_the_same_seed() {
    charpente_sim::checks::replays_identically(0x2EB2_5A21, |sim: &mut Sim| {
        shapes_workload(sim)?;
        let coverage = sim.coverage();
        if !coverage.contains_key("shapes:reshaped-frame") {
            return Err(format!(
                "this seed no longer reaches the reshaped frame: {coverage:?}"
            ));
        }
        Ok(coverage)
    })
    .unwrap();
}

/// **Measured 2026-09-24** at base `0x2EB2_5A10`, 180 universes, identical
/// across consecutive batches before and after the stall flag and the tag
/// check (neither moves a draw). Each floor is the exact count: a drop is a
/// finding, not a number to lower. `fault:subscriber_stall` is required, not
/// floored, like every fault axis here; it reads 2 at this base.
///
/// **Re-measured 2026-09-30**, same base and count, two identical batches,
/// for redb 4.3.0 and the crash countdown re-sized to its disk-op volume
/// (DESIGN §1, "countdowns"; `fault:crash` 71 under 4.1.0, 84 now). No floor
/// fell; each is set to its new count.
///
/// | key | was | now |
/// |---|---|---|
/// | `shapes:reshaped-frame` | 11 | 19 |
/// | `shapes:reshaped-409` | 52 | 66 |
/// | `shapes:v2-delivered` | 42 | 56 |
///
/// **Re-set 2026-10-01**, same base and count, two identical batches, for S5
/// and D1 together: `Db::open` no longer discards a store over an IO error,
/// and WAL recovery syncs each segment before replay (a sync with nothing to
/// flush draws no fault). Both re-key every universe. `reshaped-409` rose and
/// is set on its count. The other two fell and are re-set by GUIDE §8's re-key
/// rule, over the same 10 fresh bases, parent of both (796bff2) against this
/// tree: `0x5E5F_0C01`..`0x5E5F_0C0B` without `0x5E5F_0C05`, where the parent
/// fails on the "store outran durable truth" assert D1 fixed (seed
/// `0x99ba30a2993956be`).
///
/// | key | was | now | parent mean (sd) | this tree mean (sd) | difference / Welch SE |
/// |---|---|---|---|---|---|
/// | `shapes:reshaped-frame` | 19 | 14 | 14.4 (4.48) | 13.8 (3.74) | −0.60 / 1.84 = −0.33 |
/// | `shapes:reshaped-409` | 66 | 70 | | | rose |
/// | `shapes:v2-delivered` | 56 | 51 | 56.5 (4.35) | 55.7 (6.11) | −0.80 / 2.37 = −0.34 |
///
/// Both falls are inside one standard error, so neither rate decayed; the
/// pinned base had sat near the top of the parent's spread (19 against a mean
/// of 14.4).
const SHAPES_RESHAPED_FRAME_FLOOR: u64 = 14;
const SHAPES_RESHAPED_409_FLOOR: u64 = 70;
const SHAPES_V2_DELIVERED_FLOOR: u64 = 51;

/// `shapes_verdict`'s stall arm, both sides, called directly. The sink is
/// dropped before every verdict, so the tail is closed the way the terminal
/// `charpente.reshaped` frame leaves it. That alone must not excuse a stream
/// that stops before the log does; only a stall that fired does. And a stall
/// that fired cannot leave the stream full, since a refused send ends the tail.
#[test]
fn a_short_stream_passes_the_shapes_verdict_only_when_a_stall_fired() {
    let (sim, _kernel, rows) = mixed_log(
        0x5A11_0001,
        &[
            ("/pong", 1, None),
            ("/v2/incident", 5, None),
            ("/pong", 7, None),
        ],
    );
    let full = [
        EventFrame {
            id: rows[1].id.raw(),
            seq: rows[1].seq,
            name: "incident_opened",
            json: r#"{"n":"5","severity":"2"}"#.to_string(),
        },
        EventFrame {
            id: rows[2].id.raw(),
            seq: rows[2].seq,
            name: "pong",
            json: r#"{"n":"7"}"#.to_string(),
        },
    ];
    let verdict = |frames: &[EventFrame], stalled: bool| {
        let responses: RecordedResponses = Arc::new(Mutex::new(vec![(200, Vec::new())]));
        let (mut sink, handle) = SimSink::new(Box::new(|| false));
        for frame in frames {
            assert!(matches!(sink.send(frame), SinkStatus::Ok));
        }
        drop(sink);
        assert!(handle.closed());
        let sinks: SinkHandles = Arc::new(Mutex::new(vec![handle]));
        shapes_verdict(&sim, &rows, &rows[0], &responses, &sinks, stalled)
    };

    assert_eq!(
        verdict(&full, false),
        Ok(Adjudged::Settled),
        "the whole resume with no stall is exact"
    );
    let short = verdict(&full[..1], false)
        .expect_err("a strict prefix with no stall must be refused, closed tail or not");
    assert!(
        short.contains("stopped short with no stall to explain it"),
        "refused by the short-stream arm, not some other one: {short}"
    );
    assert_eq!(
        verdict(&full[..1], true),
        Ok(Adjudged::Settled),
        "a strict prefix is legal once a stall fired"
    );
    let redelivered =
        verdict(&full, true).expect_err("a stall that refused a send cannot leave the stream full");
    assert!(
        redelivered.contains("a refused frame was redelivered or an extra frame was sent"),
        "refused by the stalled-but-full arm, not some other one: {redelivered}"
    );
}

/// A subscribe answered 500 because an injected fault reached the cursor
/// read is fault fallout, the flagship's classification: the attempt is
/// retried, never adjudicated as a verdict failure.
#[test]
fn a_fault_fallout_500_on_the_shapes_subscribe_is_retried_not_a_verdict() {
    let (sim, _kernel, rows) = mixed_log(0x5A11_0002, &[("/pong", 1, None)]);
    let body = serde_json::to_vec(&serde_json::json!({
        "error": "internal storage failure validating the subscribe cursor — retry: injected read error"
    }))
    .unwrap();
    let responses: RecordedResponses = Arc::new(Mutex::new(vec![(500, body)]));
    let sinks: SinkHandles = Arc::new(Mutex::new(Vec::new()));
    assert_eq!(
        shapes_verdict(&sim, &rows, &rows[0], &responses, &sinks, false),
        Ok(Adjudged::Retry)
    );
}

// ==================================== multi-node: follower tails + failover
//
// Task 8: two REPLICATED kernels (the rig shape from replication.rs's T8
// section — `KNode`/`spawn_kernel_node`/`kernel_round`/`drive_request`/
// `CtrlConn`, reproduced here since integration-test binaries share no code)
// wired to T6's OWN client model above, reused verbatim (never forked): a
// client tails the FOLLOWER's kernel while emits land on the primary
// (`realtime:follower-tail`); the primary is killed and the follower
// promoted, and the SAME client reconnects with its cursor, proving the
// failover kept exactly what the follower had durably shipped
// (`realtime:failover-resume` — DECISIONS §6's bounded-loss window is
// implicit here: the client's ledger is synced from whichever node it is
// CONNECTED to, so an event never shipped to the follower simply never
// enters the ledger `settled()` waits on, mirroring how
// `failover_preserves_everything_the_follower_acked` checks a PREFIX of the
// primary's timeline rather than "everything ever acked"); and a second,
// deliberately foreign cursor — built by partitioning the follower, driving
// the primary past the follower's shipped position, and recording one
// unshipped event's id.seq before killing the primary — proves the promoted
// follower refuses it by name by name (kernel/mod.rs's `validate_subscribe`
// 409 family) rather than ever serving the wrong content
// (`realtime:failover-diverged`).

/// Marker prefix meaning "this universe's faults were too persistent for the
/// multi-node dance to complete" — a covered skip. This file's own copy of
/// replication.rs's `INCONCLUSIVE` idiom (two separate test binaries share no
/// platform code, only the pattern).
const INCONCLUSIVE: &str = "universe-inconclusive";

const RT_RECOVER_ATTEMPTS: u32 = 200;
const RT_STEP_BUDGET: u32 = 20_000;
const RT_SETTLE_ATTEMPTS: u32 = 40;
const RT_HORIZON: u64 = 300 * SEC;

/// Disables the durable-media-corruption axes (mirrors replication.rs's own
/// `no_media_corruption`): a primary that bitrots its own WAL and silently
/// loses an acked-but-unconverged record is the above-horizon promise's
/// property (M1 replication task 8's `wal_corruption_on_the_primary_...`),
/// not this one — this property's failover loss window is the DELIBERATE,
/// honest one (partition + unshipped tail), never an incidental one from
/// bitrot noise.
fn no_media_corruption(cfg: &mut FaultConfig) {
    cfg.bit_flip = 0.0;
    cfg.misdirected_write = 0.0;
}

fn rt_primary_cfg(addr: &str) -> ReplConfig {
    ReplConfig {
        listen: Some(addr.to_string()),
        initial_role: Role::Primary,
        peer_secret: None,
        strict_witnesses: false,
    }
}

fn rt_follower_cfg(listen: &str, primary: &str) -> ReplConfig {
    ReplConfig {
        listen: Some(listen.to_string()),
        initial_role: Role::Follower {
            primary: primary.to_string(),
        },
        peer_secret: None,
        strict_witnesses: false,
    }
}

fn multi_node_db_cfg() -> DbConfig {
    DbConfig {
        wal: WalConfig {
            dir: "wal".to_string(),
            segment_bytes: 4096,
        },
        store_path: "tables.redb".to_string(),
        event_retention: 64,
        checkpoint_every: 8,
        ..DbConfig::default()
    }
}

/// One replication node driven through its `Kernel` — this file's own copy
/// of replication.rs's `KNode`, scoped to what this property needs (no
/// jobs/cron: this workload only ever mounts the emit/subscribe routes).
struct RNode {
    host: SimHost,
    kernel: Option<Kernel>,
    source: SimSource,
    responses: RecordedResponses,
    sinks: SinkHandles,
    name: String,
    repl_cfg: ReplConfig,
    db_cfg: DbConfig,
    recoveries: u32,
    flipped: bool,
}

/// Build the App + Repl + Kernel for one host, with the same torn-role
/// first-boot re-init idiom `open_kernel` uses in replication.rs.
fn open_rt_kernel(
    sim: &Sim,
    h: &SimHost,
    cfg: &ReplConfig,
    db_cfg: &DbConfig,
    flipped: bool,
) -> Result<Kernel, String> {
    let mut app = App::open(
        Box::new(h.disk()),
        Schema::new(),
        Box::new(h.entropy()),
        AppConfig {
            body_cap: 1 << 20,
            db: db_cfg.clone(),
            ..AppConfig::default()
        },
    )
    .map_err(|e| format!("App::open: {e}"))?
    .mount(routes![
        emit_ping, emit_pong, feed_both, feed_ping, feed_keyed
    ]);

    let repl = match Repl::open(
        Box::new(h.net()),
        Box::new(h.disk()),
        cfg.clone(),
        app.db_for_tests(),
    ) {
        Ok((repl, _notice)) => repl,
        // A torn witness file (M2 sub-project 0b task 2): keyed on its own
        // full, non-overlapping needle ("witness file failed its integrity
        // check"), an independent arm from the role-file arm below (keyed on
        // ITS own full needle, "role file failed its integrity check") — the
        // two are mutually exclusive by construction, so arm ORDER is
        // irrelevant; both need the FULL string rather than the shared
        // "integrity check" substring precisely because that substring alone
        // cannot tell them apart. Unlike the role file, there is no
        // config-derived witness set to re-init from, and witnesses.rs's own
        // fix text is explicit that deleting it is safe ONLY when the
        // operator is certain no follower of this node was ever promoted —
        // a fact this harness cannot prove in a swarm-fuzzed universe (a
        // witness can be recorded by an ordinary running primary with no
        // role flip at all, so `flipped` is not a safe proxy here either,
        // where it exists). Always inconclusive: the operator-restore path
        // is the fix. Counted separately from the generic inconclusive
        // bucket ("repl:witness-file-inconclusive") so this residual,
        // newly-introduced permanent-unavailability path stays measurable,
        // not invisible (fix round 1, Noah's ruling — a genuinely torn
        // NON-empty witness file is a real design fork, escalated
        // separately, not fixed here). (This comment is kept byte-identical
        // at every copy of this arm — replication.rs's two sites, auth.rs,
        // realtime.rs — so a future reviewer can diff them; see
        // task-2-report.md.)
        Err(e)
            if e.to_string()
                .contains("witness file failed its integrity check") =>
        {
            sim.cover("repl:witness-file-inconclusive");
            sim.cover("rt:inconclusive:witness-torn");
            return Err(format!(
                "{INCONCLUSIVE}: {} torn witness file — the operator restore path is the fix: {e}",
                h.name()
            ));
        }
        Err(e)
            if e.to_string()
                .contains("role file failed its integrity check") =>
        {
            if flipped {
                sim.cover("rt:inconclusive:role-file-torn");
                return Err(format!(
                    "{INCONCLUSIVE}: {} torn role file after a role flip — restore, not \
                     re-init, is the fix: {e}",
                    h.name()
                ));
            }
            let _ = h.disk().remove("role");
            let _ = h.disk().sync_dir("role");
            Repl::open(
                Box::new(h.net()),
                Box::new(h.disk()),
                cfg.clone(),
                app.db_for_tests(),
            )
            .map_err(|e| format!("Repl::open (after torn-role re-init): {e}"))?
            .0
        }
        Err(e) => return Err(format!("Repl::open: {e}")),
    };
    Kernel::with_replication(app, Box::new(sim.clock()), repl).map_err(|e| format!("Kernel: {e}"))
}

fn spawn_rt_node(
    sim: &Sim,
    host: &str,
    cfg: ReplConfig,
    db_cfg: DbConfig,
    horizon: u64,
) -> Result<RNode, String> {
    let h = sim.host(host);
    for _ in 0..RT_RECOVER_ATTEMPTS {
        match open_rt_kernel(sim, &h, &cfg, &db_cfg, false) {
            Ok(kernel) => {
                let source = SimSource::new(Box::new(sim.clock()), horizon);
                let responses = source.responses();
                let sinks = source.sinks();
                return Ok(RNode {
                    host: h,
                    kernel: Some(kernel),
                    source,
                    responses,
                    sinks,
                    name: host.to_string(),
                    repl_cfg: cfg,
                    db_cfg,
                    recoveries: 0,
                    flipped: false,
                });
            }
            Err(e) if e.contains(INCONCLUSIVE) => return Err(e),
            // A refusal for durable corruption at or below the horizon is
            // fallout only where that corruption fired on this host's own
            // disk (S3b item 0's rule), and then no power-cycle clears the
            // bytes, so the spawn gives up at once. Unexplained, it falls to
            // the failure arm below. Item 0 missed this loop, as it missed
            // replication's kernel spawn (S3b item 8's sweep).
            Err(e)
                if matches!(classify(&e), FaultReason::DurableHorizon)
                    && h.durable_corruption_fired() =>
            {
                sim.cover("rt:inconclusive:spawn-durable-refusal");
                return Err(format!(
                    "{INCONCLUSIVE}: {host} kernel spawn refused for durable corruption that \
                     fired on {host}'s disk, which no power-cycle clears: {e}"
                ));
            }
            Err(e) if is_fault_fallout(&e) => {
                h.restart();
                continue;
            }
            Err(e) => return Err(format!("{host}: {e}")),
        }
    }
    sim.cover("rt:inconclusive:spawn-exhausted");
    Err(format!(
        "{INCONCLUSIVE}: {host} kernel spawn exhausted {RT_RECOVER_ATTEMPTS} attempts"
    ))
}

/// Restart-and-recover a node in place. Local-corruption-shaped hard errors
/// (no restore rig in this property — it isn't what this task proves) are
/// folded into the same inconclusive skip the Db-level `recover_node` uses in
/// replication.rs, rather than attempting the kernel-level restore dance.
/// `realtime.rs`'s copy of `replication.rs`'s `recover_exhausted_key`.
/// Duplicated for the reason given there: coverage keys must be
/// `&'static str`, and each suite owns its prefix. Exhaustive, so a new
/// `FaultReason` variant is a compile error rather than an unattributed
/// give-up.
fn rt_recover_exhausted_key(last: &str) -> &'static str {
    match classify(last) {
        FaultReason::Injected => "rt:inconclusive:recover-exhausted:injected",
        FaultReason::Poisoned => "rt:inconclusive:recover-exhausted:poisoned",
        FaultReason::RedbLatch => "rt:inconclusive:recover-exhausted:redb-latch",
        FaultReason::DurableHorizon => "rt:inconclusive:recover-exhausted:durable-horizon",
        FaultReason::Unrelated => "rt:inconclusive:recover-exhausted:unrelated",
    }
}

/// A local copy this property has no restore rig for: corruption at or below
/// the durable horizon, or a log directory gapped past guessing. NOT DESIGN
/// §4's compacted-log refusal, although it names the same "restore this node"
/// fix: `recover_rt_node` sends that one on to its fault-fallout arm and
/// otherwise fails, because under redb 4.3.0 the crash model cannot brick the
/// store. (An injected fault inside `Store::open` no longer becomes the
/// refusal at all: `Db::open` returns it as itself, DESIGN §4.)
fn local_copy_unrestorable(e: &str) -> bool {
    (e.contains("restore this node from a replica or backup") || e.contains("refusing to guess"))
        && !e.contains("cannot be rebuilt from the log alone")
}

/// Whether a fault on `host`'s own disk explains its `local_copy_unrestorable`
/// refusal `e` (S3b item 0; the rule of replication.rs's
/// `unexplained_restore_needed`): a gap by entry loss on that disk, anything
/// else by durable corruption on it. A refusal nothing on that node explains
/// is a platform durability bug, never an inconclusive skip. The property that
/// recovers these nodes turns both durable-corruption axes off
/// (`no_media_corruption`), so there only a gap can be explained.
fn unrestorable_explained(host: &SimHost, e: &str) -> bool {
    if e.contains("refusing to guess") {
        host.dir_entry_loss_fired()
    } else {
        host.durable_corruption_fired()
    }
}

/// `recover_rt_node`'s arms run `local_copy_unrestorable`, then fault fallout,
/// then fail. The compacted-log refusal must reach the second (injected cause)
/// or the third (none), never the first — and the first must still take what
/// it exists for. No realtime workload reaches that refusal at its pinned base
/// (none in 20 batches, 2026-09-30), so this is the arm's oracle.
#[test]
fn the_compacted_log_refusal_is_fallout_or_a_failure_never_an_unrestorable_skip() {
    let refusal = |cause: &str| {
        format!(
            "table store cannot open ({cause}) and the log is compacted to seq 3 — the records \
             below the store's last checkpoint no longer exist locally, so the store cannot be \
             rebuilt from the log alone; restore this node from a replica or backup, or widen \
             DbConfig::retain_segments (DESIGN §4)"
        )
    };
    let injected = refusal("table store (redb): I/O error: injected read error");
    assert!(!local_copy_unrestorable(&injected), "{injected}");
    assert!(
        is_fault_fallout(&injected),
        "an injected cause is fallout: {injected}"
    );
    let bare = refusal(
        "table store (redb): DB corrupted: File truncated below stored layout: file_len=4096, \
         layout_len=8192",
    );
    assert!(!local_copy_unrestorable(&bare), "{bare}");
    assert!(
        !is_fault_fallout(&bare),
        "no injected cause, so it fails: {bare}"
    );
    assert!(local_copy_unrestorable(
        "log frame 3 is invalid but at-or-below the store's durable horizon 7 — this is \
         corruption of acknowledged records, not a torn tail, and truncating it would silently \
         lose data; restore this node from a replica or backup (DESIGN §2)"
    ));
    assert!(local_copy_unrestorable("refusing to guess across a gap"));
}

/// S3b item 0: `recover_rt_node` skips a node whose local copy is
/// unrestorable only when a fault on that node's own disk explains the
/// refusal. Here a frame below the store's horizon is corrupted by hand, so
/// nothing fired, which is what a durability bug looks like from the rig: the
/// recovery must fail by name. Until item 0 it skipped the universe as
/// inconclusive. The property behind this rig turns both durable-corruption
/// axes off, so there this refusal could never be explained.
#[test]
fn an_unrestorable_local_copy_is_skipped_only_for_a_fault_on_its_own_disk() {
    let sim = Sim::with_faults(0x53B0_0011, FaultConfig::none());
    let mut p = spawn_rt_node(
        &sim,
        "p",
        rt_primary_cfg("p:1"),
        multi_node_db_cfg(),
        RT_HORIZON,
    )
    .expect("spawns under FaultConfig::none()");
    for n in 0..4u64 {
        let ping = Request::new(Method::Post, "/ping").json(&EmitBody {
            n: WireU64(n),
            key: None,
        });
        let (status, _) = rt_drive_request(&sim, &mut [&mut p], 0, ping, 2_000).unwrap();
        assert_eq!(status, 200);
    }
    // The reopen persists the store at the log's tail, so the horizon now
    // covers every frame written so far.
    recover_rt_node(&sim, &mut p).unwrap();
    let segment = p.host.disk().open("wal/00000000000000000000.log").unwrap();
    let mut byte = [0u8; 1];
    segment.read_at(16, &mut byte).unwrap();
    segment.write_at(16, &[byte[0] ^ 0xFF]).unwrap();
    segment.sync().unwrap();
    assert!(!p.host.durable_corruption_fired());

    let err = recover_rt_node(&sim, &mut p).unwrap_err();
    assert!(
        !err.contains(INCONCLUSIVE)
            && err.contains("durable horizon")
            && err.ends_with(
                "but no fault on p's own disk explains the refusal: a platform durability bug, \
                 not an unrestorable copy"
            ),
        "{err}"
    );
    assert_eq!(
        sim.coverage().get("rt:inconclusive:local-unrestorable"),
        None
    );

    let gap = "a segment is missing … refusing to guess";
    assert!(!unrestorable_explained(&p.host, gap));
    assert!(!unrestorable_explained(&p.host, &err));

    // Each family is explained by its own fault on that disk, and only there.
    let sim = Sim::with_faults(
        0x53B0_0012,
        FaultConfig {
            misdirected_write: 1.0,
            dir_entry_loss: 1.0,
            ..FaultConfig::none()
        },
    );
    let (q, r) = (sim.host("q"), sim.host("r"));
    q.disk()
        .open("wal/x.log")
        .unwrap()
        .write_at(0, &[7; 16])
        .unwrap();
    r.disk().open("wal/new.log").unwrap();
    r.restart();
    assert!(q.durable_corruption_fired() && !q.dir_entry_loss_fired());
    assert!(r.dir_entry_loss_fired() && !r.durable_corruption_fired());
    assert!(unrestorable_explained(&q, &err) && !unrestorable_explained(&q, gap));
    assert!(unrestorable_explained(&r, gap) && !unrestorable_explained(&r, &err));
}

/// S3b item 8's sweep. Item 0 took the refusal below the horizon out of
/// `is_fault_fallout`, and this spawn loop retried only fallout, so a spawn
/// refused for corruption on its own disk failed the universe. It gives up at
/// once now, and only for a fault on that disk.
/// - **Explained:** a bit flip on `p`'s disk at its power-cycle lands below
///   the horizon, and the respawn gives up under
///   `rt:inconclusive:spawn-durable-refusal`.
/// - **Unexplained:** the same frame is corrupted by hand, with a flip fired
///   only on a bystander's disk, and the respawn fails by name.
#[test]
fn a_spawn_refused_below_the_horizon_gives_up_only_for_corruption_on_its_own_disk() {
    let respawn = |sim: &Sim| {
        spawn_rt_node(
            sim,
            "p",
            rt_primary_cfg("p:1"),
            multi_node_db_cfg(),
            RT_HORIZON,
        )
    };
    let pinged = |sim: &Sim| {
        let mut p = respawn(sim).expect("spawns before anything fires");
        for n in 0..4u64 {
            let ping = Request::new(Method::Post, "/ping").json(&EmitBody {
                n: WireU64(n),
                key: None,
            });
            let (status, _) = rt_drive_request(sim, &mut [&mut p], 0, ping, 2_000).unwrap();
            assert_eq!(status, 200);
        }
        p
    };

    let sim = Sim::with_faults(
        0x53B0_0019,
        FaultConfig {
            bit_flip: 1.0,
            ..FaultConfig::none()
        },
    );
    let p = pinged(&sim);
    let host = p.host.clone();
    drop(p);
    host.restart();
    sim.quiesce_disk_faults();
    assert!(
        host.durable_corruption_fired(),
        "the flip fired on p's disk"
    );
    let refused = respawn(&sim)
        .err()
        .expect("the flip lands below p's horizon, or this pin reaches nothing");
    assert!(
        refused.starts_with(INCONCLUSIVE)
            && refused.contains("p kernel spawn refused for durable corruption that fired on p's"),
        "{refused}"
    );
    assert_eq!(
        sim.coverage()
            .get("rt:inconclusive:spawn-durable-refusal")
            .copied(),
        Some(1)
    );

    let sim = Sim::with_faults(
        0x53B0_0017,
        FaultConfig {
            bit_flip: 1.0,
            ..FaultConfig::none()
        },
    );
    let mut p = pinged(&sim);
    // A flip on a bystander's disk, so the universe counts as corrupted and
    // only the per-host check tells the two apart.
    let q = sim.host("q");
    let file = q.disk().open("wal/x.log").unwrap();
    file.write_at(0, &[7; 64]).unwrap();
    file.sync().unwrap();
    q.restart();
    sim.quiesce_disk_faults();
    assert!(durable_corruption_fired(&sim) && !p.host.durable_corruption_fired());
    // The reopen persists the store at the log's tail, so the horizon covers
    // every frame written so far.
    recover_rt_node(&sim, &mut p).unwrap();
    let segment = p.host.disk().open("wal/00000000000000000000.log").unwrap();
    let mut byte = [0u8; 1];
    segment.read_at(16, &mut byte).unwrap();
    segment.write_at(16, &[byte[0] ^ 0xFF]).unwrap();
    segment.sync().unwrap();
    assert!(!p.host.durable_corruption_fired());
    drop(p);
    let refused = respawn(&sim)
        .err()
        .expect("the respawn over a corrupted frame is refused");
    assert!(
        refused.starts_with("p: ") && refused.contains("durable horizon"),
        "nothing fired on p explains it, so it fails by name: {refused}"
    );
}

fn recover_rt_node(sim: &Sim, node: &mut RNode) -> Result<(), String> {
    sim.cover("restart");
    node.recoveries += 1;
    node.kernel = None;
    let mut last = String::new();
    for attempt in 0..RT_RECOVER_ATTEMPTS {
        // Power-cycle every Nth attempt rather than every one — cycling
        // before every attempt is the wrong shape: `settle_boot`'s doc
        // comment above (`:706-712`) measured it directly at 0 of 20_000
        // boots ever succeeding, because every `Sim::restart` discards the
        // uncheckpointed store and forces a full WAL rebuild on every try.
        // Reuses `SETTLE_CYCLE_EVERY` rather than declaring a second
        // constant: one file, one meaning for "how often to power-cycle a
        // stuck boot".
        if attempt.is_multiple_of(SETTLE_CYCLE_EVERY) {
            node.host.restart();
        }
        match open_rt_kernel(sim, &node.host, &node.repl_cfg, &node.db_cfg, node.flipped) {
            Ok(kernel) => {
                node.kernel = Some(kernel);
                return Ok(());
            }
            Err(e) if e.contains(INCONCLUSIVE) => return Err(e),
            Err(e) if local_copy_unrestorable(&e) => {
                if !unrestorable_explained(&node.host, &e) {
                    return Err(format!(
                        "recover {}: {e}, but no fault on {}'s own disk explains the \
                         refusal: a platform durability bug, not an unrestorable copy",
                        node.name, node.name
                    ));
                }
                sim.cover("rt:inconclusive:local-unrestorable");
                return Err(format!(
                    "{INCONCLUSIVE}: {} local copy unrestorable (no restore rig in this \
                     property): {e}",
                    node.name
                ));
            }
            Err(e) if is_fault_fallout(&e) => last = e,
            Err(e) => return Err(format!("recover {}: {e}", node.name)),
        }
    }
    sim.cover("rt:inconclusive:recover-exhausted");
    sim.cover(rt_recover_exhausted_key(&last));
    Err(format!(
        "{INCONCLUSIVE}: recover {} exhausted {RT_RECOVER_ATTEMPTS} attempts (last: {last})",
        node.name
    ))
}

/// One `Kernel::step`, classifying faults exactly like replication.rs's
/// `kstep_node`.
fn kstep_rt_node(sim: &Sim, node: &mut RNode) -> Result<Step, String> {
    let stepped = {
        let kernel = node.kernel.as_mut().expect("kstep on a live node");
        kernel.step(&mut node.source)
    };
    match stepped {
        Ok(step) => Ok(step),
        Err(e) if is_fault_fallout(&e.to_string()) => {
            recover_rt_node(sim, node)?;
            Ok(Step::Worked)
        }
        Err(e) => Err(format!("{} kernel step: {e}", node.name)),
    }
}

/// One scheduler round over kernel nodes — mirrors replication.rs's
/// `kernel_round`.
fn rt_kernel_round(sim: &Sim, nodes: &mut [&mut RNode]) -> Result<bool, String> {
    let mut worked = false;
    let mut deadlines: Vec<u64> = Vec::new();
    for node in nodes.iter_mut() {
        match kstep_rt_node(sim, node)? {
            Step::Worked => worked = true,
            Step::Idle { deadline } => {
                if let Some(d) = deadline {
                    deadlines.push(d.as_nanos());
                }
            }
        }
    }
    if worked {
        return Ok(true);
    }
    let now = sim.now().as_nanos();
    let mut next = sim.next_wakeup();
    for d in deadlines {
        if d > now {
            next = Some(next.map_or(d, |x| x.min(d)));
        }
    }
    match next {
        Some(t) if t > now => {
            sim.clock().sleep(Duration::from_nanos(t - now));
            Ok(true)
        }
        _ => Ok(false),
    }
}

/// The witness boot gate's 503 (M2 sub-project 0b task 5): this workload
/// restarts its primary under fault injection, and a primary that comes back
/// with a recorded witness serves reads but withholds writes until that
/// witness answers. An emit refused there did not happen — no event id was
/// minted and nothing was acked — so it is the same "no write, no bug"
/// outcome the fault-fallout arms already tolerate. Keyed on the 503 body's
/// own leading phrase (`http/app.rs`, pinned byte-exact in `http.rs`).
fn rt_witness_gated(body: &[u8]) -> bool {
    String::from_utf8_lossy(body).contains("this node is not accepting writes yet")
}

/// Push a request onto `target`'s source and drive the whole node set until
/// it is answered — mirrors replication.rs's `drive_request`.
fn rt_drive_request(
    sim: &Sim,
    nodes: &mut [&mut RNode],
    target: usize,
    req: Request,
    budget: u32,
) -> Result<(u16, Vec<u8>), String> {
    let sink = nodes[target].responses.clone();
    let before = sink.lock().expect("responses lock poisoned").len();
    let now = sim.now().as_nanos();
    nodes[target].source.push_at(now, req);
    for _ in 0..budget {
        {
            let guard = sink.lock().expect("responses lock poisoned");
            if guard.len() > before {
                return Ok(guard[before].clone());
            }
        }
        if !rt_kernel_round(sim, nodes)? {
            break;
        }
    }
    let guard = sink.lock().expect("responses lock poisoned");
    if guard.len() > before {
        Ok(guard[before].clone())
    } else {
        sim.cover("rt:inconclusive:request-no-response");
        Err(format!(
            "{INCONCLUSIVE}: no response to a request on node {target} within budget"
        ))
    }
}

/// A control connection to a node's replication listener — mirrors
/// replication.rs's `CtrlConn`.
struct CtrlConn {
    conn: Box<dyn Conn>,
    buf: Vec<u8>,
}

impl CtrlConn {
    fn open(sim: &Sim, addr: &str, verb: &Msg) -> Result<CtrlConn, String> {
        let conn = sim.host("cli").net().connect(addr).map_err(|e| {
            sim.cover("rt:inconclusive:ctrl-connect");
            format!("{INCONCLUSIVE}: ctrl connect {addr}: {e}")
        })?;
        conn.send(&encode(verb)).map_err(|e| {
            sim.cover("rt:inconclusive:ctrl-send");
            format!("{INCONCLUSIVE}: ctrl send: {e}")
        })?;
        Ok(CtrlConn {
            conn,
            buf: vec![0u8; MAX_MESSAGE_BYTES],
        })
    }

    fn poll(&mut self) -> Result<Option<Msg>, String> {
        match self.conn.recv(&mut self.buf) {
            Ok(Some(0)) => Err("control conn closed before a reply".to_string()),
            Ok(Some(n)) => decode(&self.buf[..n]).map(Some).map_err(|e| e.to_string()),
            Ok(None) => Ok(None),
            Err(e) => Err(format!("ctrl recv: {e}")),
        }
    }
}

fn drive_ctrl_kernel(
    sim: &Sim,
    nodes: &mut [&mut RNode],
    cli: &mut CtrlConn,
    budget: u32,
) -> Result<Msg, String> {
    for _ in 0..budget {
        let advanced = rt_kernel_round(sim, nodes)?;
        match cli.poll() {
            Ok(Some(m)) => return Ok(m),
            Ok(None) => {}
            Err(e) => {
                sim.cover("rt:inconclusive:ctrl-poll-faulted");
                return Err(format!("{INCONCLUSIVE}: {e}"));
            }
        }
        if !advanced {
            sim.cover("rt:inconclusive:ctrl-quiescent");
            return Err(format!("{INCONCLUSIVE}: quiescent before a control reply"));
        }
    }
    sim.cover("rt:inconclusive:ctrl-no-reply");
    Err(format!("{INCONCLUSIVE}: no control reply within budget"))
}

/// Drives `client` against `nodes[target]` until it holds a live, settled
/// tail: (re)subscribes whenever it lacks one via `push_subscribe` — T6's own
/// helper, already fully generic over any `Kernel`/`SimSource` pair, reused
/// unmodified — and otherwise drives a bounded burst and drains through
/// `Client::absorb`/`sync_and_drain`, T6's SAME oracle, unmodified. A
/// recovery on the target node (fresh `Kernel`, so any existing sink handle
/// is orphaned — the tail never explicitly closes it, so `absorb` alone would
/// wait forever) forces a resubscribe, mirroring how the single-node flagship
/// resets `need_subscribe`/`current_handle` at every epoch/restart boundary.
fn drive_client_to_settled(
    sim: &Sim,
    nodes: &mut [&mut RNode],
    target: usize,
    client: &mut Client,
    path: &'static str,
) -> Result<(), String> {
    for _ in 0..RT_SETTLE_ATTEMPTS {
        let recoveries_before = nodes[target].recoveries;
        if client.need_subscribe {
            let before = nodes[target]
                .responses
                .lock()
                .expect("responses lock poisoned")
                .len();
            let now = sim.now().as_nanos();
            let subscribe_info = {
                let node = &mut *nodes[target];
                let Some(kernel) = node.kernel.as_ref() else {
                    sim.cover("rt:inconclusive:target-no-kernel");
                    return Err(format!("{INCONCLUSIVE}: target has no kernel"));
                };
                // A latched store makes this read fail (M2 sub-project 0a).
                // FIX ROUND 2, I-1 (Noah decision B): RECOVER the node and
                // keep the verdict. Returning an inconclusive here would
                // propagate through this function's callers into
                // `finish_rt_universe`, which converts it to `Ok(())` — so
                // the phase-1 and phase-2 `client.settled()` checks would be
                // skipped and a client that had already missed an event would
                // be excused rather than reported. `kstep_rt_node` handles
                // this same fault class the same way, and it genuinely
                // recovers: a restart mints a fresh redb handle and a fresh
                // latch flag. The attempt is spent, exactly as if the node
                // had died mid-round. Seed 0xdc154f8c66271f15 is the
                // reproduction of the fault itself.
                match push_subscribe(sim, kernel, &mut node.source, path, client.cursor, now) {
                    Ok(info) => info,
                    Err(e) if is_fault_fallout(&e) => {
                        recover_rt_node(sim, node)?;
                        client.need_subscribe = true;
                        client.current_handle = None;
                        continue;
                    }
                    Err(e) => return Err(e),
                }
            };
            let mut got = None;
            for _ in 0..RT_STEP_BUDGET {
                {
                    let guard = nodes[target]
                        .responses
                        .lock()
                        .expect("responses lock poisoned");
                    if guard.len() > before {
                        got = Some(guard[before].clone());
                    }
                }
                if got.is_some() {
                    break;
                }
                if !rt_kernel_round(sim, nodes)? {
                    break;
                }
            }
            let Some(resp) = got else {
                sim.cover("rt:inconclusive:subscribe-no-reply");
                return Err(format!("{INCONCLUSIVE}: no subscribe reply within budget"));
            };
            let node = &mut *nodes[target];
            let kernel = node.kernel.as_ref().ok_or_else(|| {
                sim.cover("rt:inconclusive:target-no-kernel");
                format!("{INCONCLUSIVE}: target has no kernel")
            })?;
            // A bare subscribe's tail starts at the node's `next_event_id`
            // when the node SERVES it (`validate_subscribe`), not when it was
            // sent: a shipped record can apply on a follower in between, and a
            // floor read at send time then owes events no tail delivers (seeds
            // 0x4aad5f8abb5ca6ef, sent at 3 and served at 4, and
            // 0xcae37b21150fa7c8, sent at 0 and served at 4). This read IS the
            // tail's start: the round that produced the reply stepped the
            // target once, a step runs one arm, and a subscribe ends its
            // request batch, so nothing has committed or applied there since.
            // The send-time read stays a lower bound.
            let subscribe_info = match subscribe_info {
                (true, sent) if resp.0 == 200 => match kernel.app().db().next_event_id() {
                    Ok(start) => {
                        if start > sent {
                            sim.cover("realtime:bare-start-advanced");
                        }
                        (true, start.max(sent))
                    }
                    Err(e) if is_fault_fallout(&e.to_string()) => {
                        recover_rt_node(sim, node)?;
                        client.need_subscribe = true;
                        client.current_handle = None;
                        continue;
                    }
                    Err(e) => {
                        return Err(format!(
                            "reading a bare subscribe's start failed non-injected: {e}"
                        ));
                    }
                },
                other => other,
            };
            client.absorb(
                sim,
                kernel.app().db(),
                &[resp],
                Some(subscribe_info),
                &node.sinks,
            )?;
        } else {
            for _ in 0..64 {
                if !rt_kernel_round(sim, nodes)? {
                    break;
                }
            }
            let node = &mut *nodes[target];
            let kernel = node.kernel.as_ref().ok_or_else(|| {
                sim.cover("rt:inconclusive:target-no-kernel");
                format!("{INCONCLUSIVE}: target has no kernel")
            })?;
            client.absorb(sim, kernel.app().db(), &[], None, &node.sinks)?;
        }
        // Every caller checks again on return: each ends in a `settled()`
        // verdict that a gave-up client cannot meet.
        if sim.gave_up().is_some() {
            return Ok(());
        }
        if nodes[target].recoveries > recoveries_before {
            client.need_subscribe = true;
            client.current_handle = None;
        }
        if client.settled() && !client.need_subscribe {
            return Ok(());
        }
    }
    Ok(())
}

/// The three-phase property. Setup: a primary `p` (emits+writes) and a
/// follower `f` (the client's tail). Phase 1: subscribe on `f`, emit on `p`,
/// prove live delivery through the shipped log — `realtime:follower-tail`.
/// Setup for phases 2/3: partition `f` from `p`, drive `p` past what `f` ever
/// shipped, record one such event's id.seq (an honestly-constructed foreign
/// cursor — never one `f` could have minted), then kill `p` for good and
/// promote `f`. Phase 2: the SAME client reconnects with its (valid, `f`-
/// shipped) cursor and must settle exactly — `realtime:failover-resume`.
/// Phase 3: a fresh subscribe using the deliberately foreign cursor must be
/// refused by name (never served) — `realtime:failover-diverged`.
fn multi_node_realtime_universe(sim: &mut Sim) -> Result<(), String> {
    let db_cfg = multi_node_db_cfg();
    let mut p = spawn_rt_node(sim, "p", rt_primary_cfg("p:1"), db_cfg.clone(), RT_HORIZON)?;
    let mut f = spawn_rt_node(sim, "f", rt_follower_cfg("f:1", "p:1"), db_cfg, RT_HORIZON)?;

    let (path, mode_names, mode_key) = mode_for(sim.seed());
    let mut client = Client::new(mode_names, mode_key);
    // The client tails `f` in every phase, so only a fault on `f`'s own disk
    // can explain a cut of its timeline. This property turns the
    // durable-corruption axes off, so none can (S3b item 1).
    client.cut_explained = |sim| sim.host("f").durable_corruption_fired();

    // ---------------------------------------------------- Phase 1: follower-tail
    drive_client_to_settled(sim, &mut [&mut p, &mut f], 1, &mut client, path)?;
    if sim.gave_up().is_some() {
        return Ok(());
    }
    for _ in 0..6u32 {
        let (route, body) = pick_emit(sim);
        let (status, resp_body) = rt_drive_request(
            sim,
            &mut [&mut p, &mut f],
            0,
            Request::new(Method::Post, route).json(&body),
            RT_STEP_BUDGET,
        )?;
        if status != 200
            && !is_fault_fallout(&String::from_utf8_lossy(&resp_body))
            && !rt_witness_gated(&resp_body)
        {
            return Err(format!(
                "unexpected emit status {status} on p: {resp_body:?}"
            ));
        }
        drive_client_to_settled(sim, &mut [&mut p, &mut f], 1, &mut client, path)?;
        if sim.gave_up().is_some() {
            return Ok(());
        }
    }
    if !client.settled() {
        return Err(client.unsettled_report());
    }
    if !client.trace.is_empty() {
        sim.cover("realtime:follower-tail");
    }

    // ------------------------------------------- Phase 2/3 setup: divergence
    sim.partition("p", "f");
    let mut divergent: Option<Cursor> = None;
    for _ in 0..8u32 {
        if divergent.is_some() || p.kernel.is_none() {
            break;
        }
        let boundary = p
            .kernel
            .as_ref()
            .expect("checked above")
            .app()
            .db()
            .next_event_id()
            .map_err(|e| e.to_string())?;
        let (route, body) = pick_emit(sim);
        let (status, resp_body) = match rt_drive_request(
            sim,
            &mut [&mut p],
            0,
            Request::new(Method::Post, route).json(&body),
            RT_STEP_BUDGET,
        ) {
            Ok(v) => v,
            Err(e) if e.contains(INCONCLUSIVE) => continue,
            Err(e) => return Err(e),
        };
        if status == 200 {
            if let Some(k) = p.kernel.as_ref() {
                let snap = k.app().db().snapshot().map_err(|e| e.to_string())?;
                let rows: Vec<EventRow> = snap.scan_from(boundary, 1).map_err(|e| e.to_string())?;
                if let Some(row) = rows.first() {
                    divergent = Some(Cursor {
                        id: row.id.raw(),
                        seq: row.seq,
                    });
                }
            }
        } else if !is_fault_fallout(&String::from_utf8_lossy(&resp_body))
            && !rt_witness_gated(&resp_body)
        {
            return Err(format!(
                "unexpected emit status {status} on p during the partition window: {resp_body:?}"
            ));
        }
    }
    let Some(divergent) = divergent else {
        sim.cover("rt:inconclusive:no-divergent-cursor");
        return Err(format!(
            "{INCONCLUSIVE}: could not construct a p-only divergent cursor within the retry \
             budget"
        ));
    };

    // Kill p for good — the partitioned tail above never ships.
    p.host.crash_now();

    // Promote f.
    f.flipped = true;
    let reply = CtrlConn::open(sim, "f:1", &Msg::CtrlPromote { secret: None })
        .and_then(|mut cli| drive_ctrl_kernel(sim, &mut [&mut f], &mut cli, RT_STEP_BUDGET))?;
    match reply {
        Msg::CtrlOk { message } => {
            if !message.contains("last_applied") {
                return Err(format!("promote CtrlOk must name last_applied: {message}"));
            }
        }
        Msg::CtrlErr { message } => {
            return Err(format!("promote refused unexpectedly: {message}"));
        }
        other => return Err(format!("unexpected promote reply: {other:?}")),
    }
    if !f.kernel.as_ref().is_some_and(|k| k.is_primary()) {
        return Err(
            "f must report is_primary() immediately after a successful CtrlPromote reply"
                .to_string(),
        );
    }

    // ---------------------------------------------------- Phase 2: failover-resume
    client.need_subscribe = true;
    client.current_handle = None;
    drive_client_to_settled(sim, &mut [&mut f], 0, &mut client, path)?;
    if sim.gave_up().is_some() {
        return Ok(());
    }
    if !client.settled() {
        return Err(client.unsettled_report());
    }
    sim.cover("realtime:failover-resume");

    // ------------------------------------------------- Phase 3: failover-diverged
    let mut got_family = false;
    for _ in 0..8u32 {
        if f.kernel.is_none() {
            break;
        }
        let before = f.responses.lock().expect("responses lock poisoned").len();
        let now = sim.now().as_nanos();
        f.source.push_at(
            now,
            Request::new(Method::Get, format!("{path}?cursor={}", divergent.wire())),
        );
        let mut resp = None;
        for _ in 0..RT_STEP_BUDGET {
            {
                let guard = f.responses.lock().expect("responses lock poisoned");
                if guard.len() > before {
                    resp = Some(guard[before].clone());
                }
            }
            if resp.is_some() {
                break;
            }
            if !rt_kernel_round(sim, &mut [&mut f])? {
                break;
            }
        }
        let Some((status, body)) = resp else {
            sim.cover("rt:inconclusive:foreign-cursor-no-reply");
            return Err(format!(
                "{INCONCLUSIVE}: no reply to the deliberately foreign-cursor subscribe"
            ));
        };
        let text = String::from_utf8_lossy(&body).to_string();
        if status == 409
            && (text.contains("diverged from this node's timeline")
                || text.contains("retention floor"))
        {
            got_family = true;
            break;
        }
        if is_fault_fallout(&text) {
            continue;
        }
        return Err(format!(
            "a cursor referencing an event minted only on p's lost (partitioned, unshipped) \
             tail must be refused by name — diverged or below-floor, never served: got {status} \
             {text}"
        ));
    }
    if !got_family {
        sim.cover("rt:inconclusive:foreign-cursor-no-verdict");
        return Err(format!(
            "{INCONCLUSIVE}: never got a clean diverged/below-floor reply for the foreign \
             cursor within the retry budget"
        ));
    }
    sim.cover("realtime:failover-diverged");

    Ok(())
}

/// `give_up` beside `cover`, same reasoning as replication.rs's
/// `finish_universe`: the coverage key is the floorable cause-side signal, and
/// `give_up` is the scalar the runner reports, which mapping the marker to
/// `Ok(())` here would otherwise hide.
fn finish_rt_universe(sim: &Sim, r: Result<(), String>) -> Result<(), String> {
    match r {
        Err(e) if e.contains(INCONCLUSIVE) => {
            sim.cover("realtime:multi-node-inconclusive");
            sim.give_up(e);
            Ok(())
        }
        other => other,
    }
}

/// The third of these (replication and auth hold the others). `classify` keys
/// on error PROSE, so a reworded exemplar silently becomes `Unrelated` and the
/// wedge it named is filed as `:unrelated` — attributed-looking and wrong.
#[test]
fn every_rt_recover_exhausted_sub_key_is_reachable_and_distinct() {
    let cases = [
        (
            "simulated disk write error (injected fault)",
            "rt:inconclusive:recover-exhausted:injected",
        ),
        (
            "WAL poisoned by an earlier IO failure",
            "rt:inconclusive:recover-exhausted:poisoned",
        ),
        (
            "Storage error: close and re-open the database",
            "rt:inconclusive:recover-exhausted:redb-latch",
        ),
        (
            "log frame 3 is invalid but at-or-below the store's durable horizon 7",
            "rt:inconclusive:recover-exhausted:durable-horizon",
        ),
        ("", "rt:inconclusive:recover-exhausted:unrelated"),
    ];
    for (text, expected) in cases {
        assert_eq!(
            rt_recover_exhausted_key(text),
            expected,
            "classify() no longer routes {text:?} to {expected} — the error was reworded or \
             classify's arms moved, and the give-up it names is now misattributed rather \
             than loudly wrong"
        );
    }
    let mut keys: Vec<&str> = cases
        .iter()
        .map(|(t, _)| rt_recover_exhausted_key(t))
        .collect();
    keys.sort_unstable();
    let before = keys.len();
    keys.dedup();
    assert_eq!(before, keys.len(), "two fault families share a sub-key");
}

/// The two universes D4 found failing "client never caught up" (bases
/// `0x5E1F0105` and `0xD4F2002D`). Each re-subscribed bare on the follower
/// while a shipped event was in flight, so the event applied after the
/// request was sent and before the node served it. The client's floor came
/// from the send, the tail started at the serve, and the client was owed an
/// event no tail delivers. Both now take the floor at the serve, under
/// `realtime:bare-start-advanced`, and settle. A seed names a universe: if one
/// stops covering the key, re-find the scenario by mechanism.
#[test]
fn a_bare_follower_subscribe_owes_what_its_tail_starts_at_not_what_it_sent() {
    for seed in [0x4aad_5f8a_bb5c_a6ef_u64, 0xcae3_7b21_150f_a7c8] {
        let mut state = seed;
        let cfg_seed = charpente_sim::rng::splitmix64(&mut state);
        let mut cfg = FaultConfig::swarm(&mut charpente_sim::rng::Rng::new(cfg_seed));
        charpente_sim::sample_extended_fields(seed, &mut cfg);
        no_media_corruption(&mut cfg);
        let mut sim = Sim::with_faults(seed, cfg);
        let r = multi_node_realtime_universe(&mut sim);
        assert_eq!(finish_rt_universe(&sim, r), Ok(()), "seed {seed:#x}");
        assert_eq!(sim.gave_up(), None, "seed {seed:#x} reaches a verdict");
        let coverage = sim.coverage();
        assert_eq!(
            coverage.get("realtime:bare-start-advanced"),
            Some(&1),
            "seed {seed:#x}: for this reason, or the seed no longer names the scenario: \
             {coverage:?}"
        );
        assert_eq!(
            coverage.get("realtime:failover-resume"),
            Some(&1),
            "seed {seed:#x}: and the client settled through the failover: {coverage:?}"
        );
    }
}

#[test]
fn follower_tails_and_the_failover_resume_divergence_property() {
    run_universes_swarm_with!(
        0x2EB2_0002,
        128,
        no_media_corruption,
        |sim: &mut Sim| {
            let r = multi_node_realtime_universe(sim);
            finish_rt_universe(sim, r)
        },
        require: [
            "realtime:follower-tail",
            "realtime:failover-resume",
            "realtime:failover-diverged"
        ]
    );
}

// ==================== T8a Step 4: follower id identity under a BATCHED primary

/// T8a Step 4 (spec §9.5, task-8-preflight.md C-6): follower id identity
/// under a BATCHED primary. After T6/T7 a "single-node run of the same
/// workload" is batched too — one commit path, hard rule 2 forbids a
/// second — so spec §6's actual claim is primary-vs-follower, not
/// batched-vs-unbatched. This is the only rig in the repo with real
/// `Kernel`s on both hosts AND events (`RNode`/`spawn_rt_node`/
/// `rt_kernel_round`, this file's own multi-node section above);
/// `replication.rs`'s `Node` is `Db`-level with no kernel (`:157`), so its
/// primary cannot batch at all.
///
/// `next_job_id` continuity under replication was already proven elsewhere
/// (the seeder work); `next_event_id` is compared NOWHERE ELSE in the
/// repo (C-6) — that is the genuinely new half this test exists for,
/// alongside a full `scan_raw` comparison (the `converged_read` shape,
/// `replication.rs:601`) over every table.
///
/// Deterministic, not a swarm: `FaultConfig::none()`, one fixed seed. The
/// batch-membership guard loops `step()` up to a small budget rather than
/// asserting on the very first call, because a `Ready::Repl` round can
/// legitimately win arbitration before `Ready::Request` gets its turn —
/// `Kernel::step`'s top-of-step `source.poll()` still only pulls the FIRST
/// pending request either way, so whichever step actually services
/// `Ready::Request` first is guaranteed to drain all four (they are all
/// already ready at the same instant), which is what the loop confirms
/// rather than assumes.
#[test]
fn follower_ids_match_a_batched_primarys() {
    let sim = Sim::with_faults(0x60C1_0004, FaultConfig::none());
    let db_cfg = multi_node_db_cfg();
    let mut p = spawn_rt_node(&sim, "p", rt_primary_cfg("p:1"), db_cfg.clone(), RT_HORIZON)
        .expect("primary spawns under FaultConfig::none()");
    let mut f = spawn_rt_node(&sim, "f", rt_follower_cfg("f:1", "p:1"), db_cfg, RT_HORIZON)
        .expect("follower spawns under FaultConfig::none()");

    // A few quiet rounds so the follower dials and Hellos before the batch.
    for _ in 0..20 {
        if !rt_kernel_round(&sim, &mut [&mut p, &mut f]).expect("quiet rounds") {
            break;
        }
    }

    // Four requests at the SAME instant — genuinely contended, so the
    // primary's request arm must drain all four in ONE batch whenever it
    // finally runs.
    let now = sim.now().as_nanos();
    for n in 0..4u64 {
        p.source.push_at(
            now,
            Request::new(Method::Post, "/ping").json(&EmitBody {
                n: WireU64(n),
                key: None,
            }),
        );
    }
    let requests_before = p.kernel.as_ref().expect("primary alive").stats().requests;
    let mut batch_ran = false;
    for _ in 0..50 {
        kstep_rt_node(&sim, &mut p).expect("primary step under FaultConfig::none()");
        if p.kernel.as_ref().expect("primary alive").stats().requests > requests_before {
            batch_ran = true;
            break;
        }
    }
    assert!(
        batch_ran,
        "the primary's request arm never ran within budget"
    );
    assert_eq!(
        p.kernel.as_ref().expect("primary alive").stats().requests - requests_before,
        4,
        "all four same-instant emits must land in ONE batch — a primary that still serves one \
         request per step proves nothing about follower id identity under batching"
    );

    // Drive to convergence: ship, ack, apply on the follower.
    let mut converged = false;
    for _ in 0..RT_STEP_BUDGET {
        let p_next = p
            .kernel
            .as_ref()
            .expect("primary alive")
            .app()
            .db()
            .next_event_id()
            .expect("primary next_event_id");
        let f_next = f
            .kernel
            .as_ref()
            .expect("follower alive")
            .app()
            .db()
            .next_event_id()
            .expect("follower next_event_id");
        if p_next == f_next && p_next > 0 {
            converged = true;
            break;
        }
        if !rt_kernel_round(&sim, &mut [&mut p, &mut f]).expect("converge rounds") {
            break;
        }
    }
    assert!(
        converged,
        "primary and follower must converge on next_event_id within budget"
    );

    let pdb = p.kernel.as_ref().expect("primary alive").app().db();
    let fdb = f.kernel.as_ref().expect("follower alive").app().db();
    let psnap = pdb.snapshot().unwrap();
    let fsnap = fdb.snapshot().unwrap();

    // The batch must have produced FOUR events, and both sides must say so
    // in absolute terms. Comparing the two sides ALONE is not enough, and
    // that is measured, not argued: T8a review I-2 truncated `Group::commit`
    // to a single record — three of four acked writes silently dropped —
    // and this test stayed GREEN, because a one-event run satisfies both
    // `p_next > 0` and byte-equal tables. The count is what carries this
    // test past spec §7's batch-of-1 identity, which is the case it exists
    // to go beyond. Two quantities, not one: the counter can in principle
    // advance without materializing rows, so the events table is checked
    // directly too.
    assert_eq!(
        pdb.next_event_id().unwrap(),
        4,
        "all four requests' emits must be in the batch — one event would mean a degenerate \
         batch and an oracle that proves nothing about batching"
    );
    assert_eq!(
        psnap.scan_raw(EventRow::TABLE, false).unwrap().len(),
        4,
        "four emits must materialize four event rows on the primary"
    );
    assert_eq!(
        fdb.next_event_id().unwrap(),
        4,
        "the follower must land on the SAME id frontier from its own replay — next_event_id is \
         compared NOWHERE else in the repo before this test (T8a pre-flight C-6)"
    );
    // `next_job_id` is deliberately NOT asserted here: `open_rt_kernel`
    // mounts no jobs and this test enqueues nothing, so both sides are 0
    // and the comparison could not fail (T8a review I-3). Wiring jobs into
    // that shared helper to make it non-vacuous would perturb every
    // multi-node test in this file; job-id continuity under replication is
    // proven by the seeder work instead.
    for table in pdb.table_names() {
        let table_scoped = pdb.table_scoped(table).unwrap();
        let pr = psnap.scan_raw(table, table_scoped).unwrap();
        let fr = fsnap.scan_raw(table, table_scoped).unwrap();
        assert_eq!(
            pr, fr,
            "table {table} diverged between the batched primary and the follower's replay"
        );
    }
}

/// Task 7's drift oracle on the app that actually delivers events: three
/// stream routes, two distinct declared sets, and one of them keyed — so an
/// `emits(...)` that quietly followed `.key(...)`, or that collapsed every
/// feed to the same set, shows up here rather than in the describe fixture's
/// single feed.
///
/// The declaration is the THIRD place these names are written in this file:
/// `Subscription::of/and` in each handler, `PING_ONLY`/`BOTH` in `mode_for`
/// (which the flagship workload uses as data — it subscribes by path and
/// checks the frames it receives against those names), and now `emits(...)`.
/// Task 7 deletes neither of the first two, so the cross-check at the end is
/// what keeps a future edit from moving one without the others.
#[test]
fn every_stream_routes_declared_emits_matches_its_subscription() {
    let sim = Sim::with_faults(0x2EB7_0007, FaultConfig::none());
    let mut app = realtime_app(&sim).expect("the realtime fixture app must open");
    // Ruling 46: `emits` elements now carry (event name, payload name). The
    // oracle compares the event half — the only half a `Subscription` holds
    // — and the payload half rides along in `declared_pairs` below, so
    // widening the type did not quietly drop it from this file's coverage.
    let declared: Vec<(&'static str, Vec<&'static str>)> = app
        .describe()
        .routes
        .iter()
        .filter(|r| r.stream)
        .map(|r| (r.name, r.emits.iter().map(|e| e.event).collect()))
        .collect();
    // The floor for the loop below (an empty one would pass vacuously), and
    // the pin on registration order, which is the contract's route order.
    assert_eq!(
        declared,
        vec![
            ("feed_both", vec!["ping", "pong"]),
            ("feed_ping", vec!["ping"]),
            ("feed_keyed", vec!["ping", "pong"]),
        ]
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
    for seed in 0..3u64 {
        let (path, names, _) = mode_for(seed);
        let emits: Vec<&str> = app
            .describe()
            .routes
            .iter()
            .find(|r| r.pattern == path)
            .map(|r| r.emits.iter().map(|e| e.event).collect())
            .expect("mode_for names a mounted route");
        assert_eq!(
            emits,
            names.to_vec(),
            "mode_for(\"{path}\") and that route's emits(...) must name the same events"
        );
    }

    // The payload half of the pair (ruling 46), which the loops above do not
    // touch: `Ping`/`Pong` are the `Wire::WIRE_NAME`s a generated TypeScript
    // union writes, and they must line up with the event names one-for-one
    // in declaration order. Both names come off the same type in the macro,
    // so a mismatch here means the pairing itself broke — the one failure
    // the projected-to-`event` comparisons above are blind to.
    let declared_pairs: Vec<(&'static str, Vec<EmittedEvent>)> = app
        .describe()
        .routes
        .iter()
        .filter(|r| r.stream)
        .map(|r| (r.name, r.emits.clone()))
        .collect();
    assert_eq!(
        declared_pairs,
        vec![
            (
                "feed_both",
                vec![
                    EmittedEvent {
                        event: "ping",
                        payload: "Ping"
                    },
                    EmittedEvent {
                        event: "pong",
                        payload: "Pong"
                    },
                ]
            ),
            (
                "feed_ping",
                vec![EmittedEvent {
                    event: "ping",
                    payload: "Ping"
                }]
            ),
            (
                "feed_keyed",
                vec![
                    EmittedEvent {
                        event: "ping",
                        payload: "Ping"
                    },
                    EmittedEvent {
                        event: "pong",
                        payload: "Pong"
                    },
                ]
            ),
        ]
    );
}

// ============================================= tenant-scoped event emission
//
// Tenancy-completion spec §6, "one tenancy mechanism, not two": an event
// emitted inside `ScopedTx<Org>(B)` is recorded in tenant B, in the row's own
// field and apart from any app key, and a tenant feed built from
// `ScopedTx<Org>(A)` delivers only rows recorded in A (exit criterion 1). A
// key is the app's filter and never a tenant: a global subscription keyed
// with a tenant's decimal id — what tenancy was before — receives nothing of
// that tenant's.

/// The scope type. A `#[table]` only so `Id<Org>` can be a route path
/// parameter (`Id<R>`'s `Wire` impl is bounded on `R: Row`); nothing reads the
/// table and it is not registered in any `Schema`. A tenant type owes a `NAME`
/// (its identity, unique in the app) and `authorize`.
#[table(global)]
struct Org {
    id: Id<Org>,
    name: String,
}

impl Tenant for Org {
    const NAME: &'static str = "Org";
    type Who = Principal;
    type Error = ApiError;

    /// Always allows, and the scoped routes below do not ask: they are
    /// declared `public(Org)`. This property is about where the platform
    /// records a tenant, not the app's membership check —
    /// `charpente/tests/tenant.rs` owns the refusing case.
    fn authorize(_tx: &Tx, _who: &Principal, _scope: Id<Org>) -> Result<(), ApiError> {
        Ok(())
    }
}

/// The tenant section's own event: every emit inside a scope in this file is
/// an `OrgPing`, and `Ping` stays the flagship's global event. It has
/// `Ping`'s shape and its own type, so that its tenancy can be declared on it
/// (tenancy-completion Task 11) without touching the flagship.
#[event(global)]
struct OrgPing {
    n: WireU64,
}

#[derive(WireDerive, Serialize, Deserialize)]
struct ScopedEmitBody {
    org: WireU64,
    n: WireU64,
}

/// The shape spec §1 prescribes: one `scope()` line, and every effect below it
/// — reads, writes, and emits — bounded to that tenant. The emit is recorded
/// in the tenant (tag `0x0b`), with no key of the app's.
#[route(POST "/scoped-ping", public(Org))]
fn emit_scoped_ping(tx: &mut Tx, body: ScopedEmitBody, ctx: &Ctx) -> Result<(), ApiError> {
    let mut tx = tx.scope::<Org>(Id::from_raw(body.org.0), ctx)?;
    tx.emit(OrgPing { n: body.n });
    Ok(())
}

/// The subscriber half of the same mechanism (amendment A3): the feed is
/// built from the scope that admitted the subscriber, so it carries that
/// tenant and delivers only events recorded in it. No string names the
/// tenant on either side.
#[route(SSE "/scoped-feed/:org", public(Org), emits(OrgPing))]
fn scoped_feed(
    tx: &mut Tx,
    org: Id<Org>,
    ctx: &Ctx,
    cursor: Option<Cursor>,
) -> Result<Subscription, ApiError> {
    let tx = tx.scope::<Org>(org, ctx)?;
    Ok(tx.subscription::<OrgPing>().cursor(cursor).into())
}

/// A global subscription keyed with a string spelled like a tenant's decimal
/// id — the tenant-id key a scoped emit used to attach. A key is an app filter
/// and never a tenant, so this feed receives no event recorded in a tenant,
/// whatever it is keyed with (tenancy-completion spec §2.3).
#[route(SSE "/literal-feed/:key", public, emits(OrgPing))]
fn literal_feed(
    tx: &mut Tx,
    key: String,
    ctx: &Ctx,
    cursor: Option<Cursor>,
) -> Result<Subscription, ApiError> {
    let _ = (tx, ctx);
    Ok(Subscription::of::<OrgPing>().key(key).cursor(cursor))
}

/// The explicit deref inside a scope: a global event, recorded with no
/// tenant (`0x09`) although the transaction holds one (spec §6, "A global
/// event emitted from inside a scope is `(*tx).emit(ev)`").
#[route(POST "/global-ping-in-scope", public(Org))]
fn emit_global_ping_in_scope(tx: &mut Tx, body: ScopedEmitBody, ctx: &Ctx) -> Result<(), ApiError> {
    let mut tx = tx.scope::<Org>(Id::from_raw(body.org.0), ctx)?;
    (*tx).emit(OrgPing { n: body.n });
    Ok(())
}

/// Hands the tenant a scoped job whose `run` emits both ways.
#[route(POST "/notify", public(Org))]
fn enqueue_notify(tx: &mut Tx, body: ScopedEmitBody, ctx: &Ctx) -> Result<(), ApiError> {
    let mut tx = tx.scope::<Org>(Id::from_raw(body.org.0), ctx)?;
    tx.enqueue(NotifyOrg { n: body.n.0 });
    Ok(())
}

/// A scoped job's `run` holds `&mut ScopedTx`: its `tx.emit` is its tenant's
/// event, and its `(**tx).emit` is a global one.
#[job(scoped_by = Org)]
struct NotifyOrg {
    n: u64,
}

impl NotifyOrg {
    fn run(self, tx: &mut ScopedTx<'_, '_, Org>) -> Result<(), JobError> {
        tx.emit(OrgPing { n: WireU64(self.n) });
        (**tx).emit(OrgPing {
            n: WireU64(self.n + 1),
        });
        Ok(())
    }
}

/// A global subscription with no key: every global `org_ping`, and no event
/// recorded in a tenant, tenant 0 included — `None` is not `Some(0)`.
#[route(SSE "/global-feed", public, emits(OrgPing))]
fn global_feed(tx: &mut Tx, ctx: &Ctx, cursor: Option<Cursor>) -> Result<Subscription, ApiError> {
    let _ = (tx, ctx);
    Ok(Subscription::of::<OrgPing>().cursor(cursor))
}

/// A tenant feed widened after `.into()` — the shape `Subscription::and`
/// refuses by name, since delivery compares a row's tenancy with the feed's
/// and a global event is never recorded in a tenant.
#[route(SSE "/widened-feed/:org", public(Org))]
fn widened_feed(
    tx: &mut Tx,
    org: Id<Org>,
    ctx: &Ctx,
    cursor: Option<Cursor>,
) -> Result<Subscription, ApiError> {
    let tx = tx.scope::<Org>(org, ctx)?;
    let feed: Subscription = tx.subscription::<OrgPing>().cursor(cursor).into();
    Ok(feed.and::<Pong>())
}

/// The tenancy tests' app: default retention (nothing here may fall behind
/// it), the scoped and global emit routes, the three feeds, and the scoped
/// job.
fn tenancy_app(sim: &Sim) -> App {
    App::open(
        Box::new(sim.disk()),
        Schema::new(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )
    .unwrap()
    .mount(routes![
        emit_scoped_ping,
        emit_global_ping_in_scope,
        enqueue_notify,
        scoped_feed,
        literal_feed,
        global_feed
    ])
    .mount_jobs(jobs![NotifyOrg])
}

/// Pushes `requests` at one instant and runs the kernel until it idles past
/// the source's deadline: every request serviced, and every commit's rows
/// offered to every tail, however the kernel interleaves them. (`run_script`'s
/// fixed 16 steps cannot promise that for four tails and three commits.)
fn run_to_idle(
    kernel: &mut Kernel,
    sim: &Sim,
    requests: Vec<Request>,
) -> (RecordedResponses, SinkHandles) {
    let mut source = SimSource::new(Box::new(sim.clock()), sim.now().as_nanos() + 10 * SEC);
    let t = sim.now().as_nanos();
    for request in requests {
        source.push_at(t, request);
    }
    let (responses, sinks) = (source.responses(), source.sinks());
    kernel
        .run(&mut source)
        .expect("no step may fail — nothing a subscriber does stops the node");
    (responses, sinks)
}

fn org_emit(path: &str, org: u64, n: u64) -> Request {
    Request::new(Method::Post, path).json(&ScopedEmitBody {
        org: WireU64(org),
        n: WireU64(n),
    })
}

/// Each delivered frame as `"<name> <json>"`.
fn delivered(handle: &SimSinkHandle) -> Vec<String> {
    handle
        .frames()
        .iter()
        .map(|f| format!("{} {}", f.name, f.json))
        .collect()
}

/// The cross-tenancy 409 (amendment A16), spelled out here rather than taken
/// from the product. It names no tenant and no event: the refusal must not
/// tell a subscriber what another tenant's row is.
fn tenancy_409(row: &EventRow) -> String {
    format!(
        "cursor {} sits on an event outside this feed's tenancy — a cursor resumes only the \
         feed that delivered it, and this feed never delivers that event; reconnect without a \
         cursor, wait for the stream to open, then read current state from the same node, \
         holding every frame that arrives until that read is installed and then applying the \
         held frames and every later one over it in order, idempotently",
        cursor_of(row)
    )
}

/// Spec §6, "Delivering", and exit criterion 1, over two tenants, tenant 0
/// among them. Four subscribers: each tenant's feed, a global feed keyed with
/// the decimal id tenancy used to be spelled as, and a global feed with no
/// key. Three emits: one in each tenant, and one global emit made inside
/// tenant 1's scope. The oracle is two-sided — each feed must carry exactly
/// its own events — because a one-sided "A never sees B" passes vacuously
/// when a scoped emit reaches nobody. Tenant 0 is a tenant: the global feed
/// with no key receives none of its event, since `None` is not `Some(0)`.
#[test]
fn a_tenant_feed_receives_only_its_own_tenants_events_tenant_zero_included() {
    let sim = Sim::with_faults(0x2EB1_1005, FaultConfig::none());
    let mut kernel = Kernel::new(tenancy_app(&sim), Box::new(sim.clock())).unwrap();
    let (responses, sinks) = run_to_idle(
        &mut kernel,
        &sim,
        vec![
            Request::new(Method::Get, "/scoped-feed/0"),
            Request::new(Method::Get, "/scoped-feed/1"),
            Request::new(Method::Get, "/literal-feed/0"),
            Request::new(Method::Get, "/global-feed"),
            org_emit("/scoped-ping", 0, 9),
            org_emit("/scoped-ping", 1, 7),
            org_emit("/global-ping-in-scope", 1, 5),
        ],
    );
    assert_eq!(statuses(&responses), vec![200; 7]);
    let handles = sinks.lock().expect("sinks lock");
    assert_eq!(handles.len(), 4, "four subscribes opened four sinks");
    assert_eq!(
        delivered(&handles[0]),
        vec![r#"org_ping {"n":"9"}"#],
        "tenant 0's feed: its own event — not tenant 1's, not the global one"
    );
    assert_eq!(
        delivered(&handles[1]),
        vec![r#"org_ping {"n":"7"}"#],
        "tenant 1's feed: its own event — the global emit made in its scope is not its event"
    );
    assert_eq!(
        delivered(&handles[2]),
        Vec::<String>::new(),
        "a global feed keyed \"0\" receives nothing: a key is an app filter, and tenant 0's \
         event is recorded in tenant 0 with no key"
    );
    assert_eq!(
        delivered(&handles[3]),
        vec![r#"org_ping {"n":"5"}"#],
        "a global feed with no key receives the global event only — tenant 0's event is \
         recorded Some(0), and None is not Some(0)"
    );
}

/// Spec §6: a scoped job's `run` holds `&mut ScopedTx`, so its `tx.emit` is
/// recorded in its tenant, with no key, and its `(**tx).emit` is recorded
/// with no tenant — reaching global subscribers and never the tenant's feed.
/// Tenant 0 again.
#[test]
fn a_scoped_jobs_emits_are_its_tenants_and_its_deref_emits_are_global() {
    let sim = Sim::with_faults(0x2EB1_1006, FaultConfig::none());
    let mut kernel = Kernel::new(tenancy_app(&sim), Box::new(sim.clock())).unwrap();
    let (responses, sinks) = run_to_idle(
        &mut kernel,
        &sim,
        vec![
            Request::new(Method::Get, "/scoped-feed/0"),
            Request::new(Method::Get, "/global-feed"),
            org_emit("/notify", 0, 40),
        ],
    );
    assert_eq!(statuses(&responses), vec![200; 3]);
    let rows: Vec<EventRow> = kernel
        .app()
        .db()
        .snapshot()
        .unwrap()
        .scan_from(0, 16)
        .unwrap();
    assert_eq!(
        rows.iter()
            .map(|r| (r.name.as_str(), r.key.as_deref(), r.scope))
            .collect::<Vec<_>>(),
        vec![("org_ping", None, Some(0)), ("org_ping", None, None)],
        "the job's own emit is recorded in tenant 0 with no key; its deref emit is global — \
         tag 0x09, no tenant"
    );
    let handles = sinks.lock().expect("sinks lock");
    assert_eq!(
        delivered(&handles[0]),
        vec![r#"org_ping {"n":"40"}"#],
        "tenant 0's feed: the job's own emit"
    );
    assert_eq!(
        delivered(&handles[1]),
        vec![r#"org_ping {"n":"41"}"#],
        "the global feed: the job's deref emit, and not its tenant's"
    );
}

/// Amendment A16, spec §9's "a resume across the boundary is refused, not
/// misdelivered". A cursor names a row of the feed that delivered it; a
/// cursor on a row of another tenancy — another tenant's event, a global
/// event on a tenant feed, a tenant's event on a global feed — is one this
/// feed never delivered, so it is refused by name rather than resumed. The
/// two controls resume, and each delivers exactly its own tenancy's later
/// rows.
#[test]
fn a_cursor_from_another_tenancy_is_refused_not_resumed() {
    let sim = Sim::with_faults(0x2EB1_1007, FaultConfig::none());
    let mut kernel = Kernel::new(tenancy_app(&sim), Box::new(sim.clock())).unwrap();
    let (responses, _) = run_to_idle(
        &mut kernel,
        &sim,
        vec![
            org_emit("/scoped-ping", 0, 10),
            org_emit("/scoped-ping", 1, 11),
            org_emit("/global-ping-in-scope", 1, 12),
            org_emit("/scoped-ping", 0, 13),
            org_emit("/global-ping-in-scope", 0, 14),
        ],
    );
    assert_eq!(statuses(&responses), vec![200; 5]);
    let rows: Vec<EventRow> = kernel
        .app()
        .db()
        .snapshot()
        .unwrap()
        .scan_from(0, 16)
        .unwrap();
    assert_eq!(rows.len(), 5, "every emit is one row");

    let (responses, sinks) = run_to_idle(
        &mut kernel,
        &sim,
        vec![
            Request::new(
                Method::Get,
                format!("/scoped-feed/0?cursor={}", cursor_of(&rows[1])),
            ),
            Request::new(
                Method::Get,
                format!("/scoped-feed/0?cursor={}", cursor_of(&rows[2])),
            ),
            Request::new(
                Method::Get,
                format!("/global-feed?cursor={}", cursor_of(&rows[0])),
            ),
            Request::new(
                Method::Get,
                format!("/scoped-feed/0?cursor={}", cursor_of(&rows[0])),
            ),
            Request::new(
                Method::Get,
                format!("/global-feed?cursor={}", cursor_of(&rows[2])),
            ),
        ],
    );
    let responses = responses.lock().unwrap().clone();
    assert_eq!(
        responses.iter().map(|(s, _)| *s).collect::<Vec<_>>(),
        vec![409, 409, 409, 200, 200],
        "three cursors from another tenancy refused, two of the feed's own resumed: {:?}",
        responses
            .iter()
            .map(|(_, b)| String::from_utf8_lossy(b).to_string())
            .collect::<Vec<_>>()
    );
    for (i, row) in [(0, &rows[1]), (1, &rows[2]), (2, &rows[0])] {
        assert_eq!(error_text(&responses[i].1), tenancy_409(row));
    }
    let handles = sinks.lock().unwrap();
    assert_eq!(
        handles.len(),
        2,
        "only the two resumed subscribes opened sinks"
    );
    assert_eq!(
        frames_of(&handles[0]),
        vec![(
            rows[3].id.raw(),
            rows[3].seq,
            "org_ping".to_string(),
            r#"{"n":"13"}"#.to_string()
        )],
        "tenant 0's feed resumes past its own row to its own next row — not tenant 1's, not \
         the global ones"
    );
    assert_eq!(
        frames_of(&handles[1]),
        vec![(
            rows[4].id.raw(),
            rows[4].seq,
            "org_ping".to_string(),
            r#"{"n":"14"}"#.to_string()
        )],
        "the global feed resumes past a global row to the next global row — not tenant 0's"
    );
}

/// A tenant feed widened after `.into()` could never deliver what `.and`
/// adds: delivery compares a row's tenancy with the feed's, and
/// `Subscription::and` adds global events. So the handler that writes it
/// panics by name, with the fix, the way a duplicate `NAME` does.
/// `TenantFeed::and`, the tenant's own way to add an event, never trips it
/// (`http::tests::a_tenant_feeds_and_adds_without_the_widening_assert`).
#[test]
#[should_panic(
    expected = "Subscription::and: event \"pong\" is added to a tenant feed after .into() — \
                Subscription::and adds global events, and a tenant feed receives only its own \
                tenant's events, so this one could never arrive; add the tenant's events with \
                TenantFeed::and before .into(), and serve global events on a subscription of \
                their own"
)]
fn a_tenant_feed_widened_after_into_is_refused_by_name() {
    let sim = Sim::with_faults(0x2EB1_1008, FaultConfig::none());
    let mut app = App::open(
        Box::new(sim.disk()),
        Schema::new(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )
    .unwrap()
    .mount(routes![widened_feed]);
    let _ = app.handle_or_subscribe(
        Request::new(Method::Get, "/widened-feed/1"),
        sim.now().as_nanos(),
    );
}

// ======================= the scoped sibling under the full fault matrix
//
// Hard rule 3 for `ScopedTx::emit` and the tenant feed. The unit properties
// above run one kernel under `FaultConfig::none()`, and the tenant an emit
// records is DURABLE — `db.rs` stores it in `EventRow.scope`, ships it by
// replication, and compares it when a subscriber resumes. A crash between
// emit and durability, a WAL replay, a retention gap and a dropped tail must
// all preserve it, and none of those happen in a universe with no faults in
// it.
//
// A SIBLING of `reconnecting_client_misses_nothing_under_the_fault_matrix`
// rather than a widening of it — the ruling R56 already settled for
// `archive_converges_with_a_scoped_table`. Putting a scoped emit into the
// flagship workload moves what its pinned base seed does, and its seeds are
// pinned; so the flagship keeps its exact bytes and everything below gets its
// own base seed, its own workload, and floors MEASURED at that base.
//
// **What this sibling can prove that the flagship structurally cannot.** The
// flagship's `Client` checks every delivered frame against `charpente.events`
// — the very row whose `scope` column is the thing in question — so an emit
// that records the WRONG tenant writes it into the oracle's own ground truth,
// and the oracle agrees with it: a `ScopedTx::emit` recording a constant
// tenant lands every tenant's event in tenant A, and a `Client` of tenant A's
// feed then expects them all, receives them all and settles. The
// discriminator added here is that each event's payload carries the tenant
// that emitted it, independently of the row (`SCOPED_N_STRIDE`), so
// `scoped_tenant_oracle` can compare the durable tenant against the emitting
// scope instead of against itself.

/// The sibling's own base seed (R56: its own base, not the flagship's).
const SCOPED_REALTIME_BASE: u64 = 0x2EB2_5C09;

/// The subscribing tenant, and the other one that must also be emitting —
/// with one tenant the whole property passes vacuously, which is what
/// `tenant:both-tenants-emitted` is required for.
const SCOPED_TENANT_A: u64 = 1;
const SCOPED_TENANT_B: u64 = 2;
const SCOPED_FEED_A: &str = "/scoped-feed/1";

/// Each event's payload declares its own emitting tenant: `OrgPing.n` is
/// `org * SCOPED_N_STRIDE + r`. This is the entire reason the property has
/// teeth — see the section header. The stride is far above any `r` so the
/// division is exact.
const SCOPED_N_STRIDE: u64 = 1_000_000;

/// `Client::trace`'s shape: the frames a subscriber actually received. Named
/// because the twin helper below hands it back beside the ledger.
type ScopedTrace = Vec<(u64, &'static str, String)>;

/// The tenant a delivered or stored `org_ping` payload declares, read back out
/// of the JSON rather than out of the row.
fn declared_org(json: &str) -> Result<u64, String> {
    let v: serde_json::Value = serde_json::from_str(json)
        .map_err(|e| format!("ping payload {json:?} is not JSON: {e}"))?;
    let n = v
        .get("n")
        .and_then(|n| n.as_str())
        .ok_or_else(|| format!("ping payload {json:?} has no string field `n`"))?;
    let n: u64 = n
        .parse()
        .map_err(|e| format!("ping payload {json:?} field `n` is not a u64: {e}"))?;
    Ok(n / SCOPED_N_STRIDE)
}

/// The sibling's own oracle, on top of everything `Client` already checks
/// (strictly-increasing ids, no missing own-tenant backlog, no over-delivery
/// against the ledger's own recorded tenant). Both arms compare the durable
/// row against the tenant the PAYLOAD declares, which is the only thing in
/// this file that does not take `EventRow.scope` at its word.
///
/// Delivery first, because that is the leak the property is named for; the
/// ledger arm behind it catches the same corruption one layer earlier, at the
/// row, including for events the subscriber never asked for. The meta-test
/// fires both arms separately.
fn scoped_tenant_oracle(
    ledger: &Ledger,
    trace: &[(u64, &'static str, String)],
    subscriber: u64,
) -> Result<(), String> {
    for (id, name, json) in trace {
        if *name == "charpente.gap" {
            continue;
        }
        let org = declared_org(json)?;
        if org != subscriber {
            return Err(format!(
                "cross-tenant delivery: a feed of tenant {subscriber} received event id {id}, \
                 which was emitted inside ScopedTx<Org>({org})"
            ));
        }
    }
    for (id, (name, key, scope, json)) in ledger {
        if *name != "org_ping" {
            continue;
        }
        let org = declared_org(json)?;
        if (key.as_deref(), *scope) != (None, Some(org)) {
            return Err(format!(
                "event id {id} was emitted inside ScopedTx<Org>({org}) but its durable row \
                 reads EventRow.scope {scope:?} and EventRow.key {key:?}, not Some({org}) and \
                 None — the tenant did not survive commit, replay and readback apart from the \
                 app's key, so a tenant feed resuming on it gets another tenant's backlog or \
                 loses its own"
            ));
        }
    }
    Ok(())
}

/// The naive victim, as a route rather than a core mutation so it can sit in
/// the suite permanently. It opens a real scope and emits through
/// `ScopedTx::emit`, but in tenant A whatever tenant the request names —
/// exactly what a `ScopedTx::emit` recording a constant tenant produces, and
/// the direction that LEAKS: tenant B's event is recorded in tenant A, so
/// tenant A's feed receives it and the row agrees with the delivery. Only the
/// payload (`SCOPED_N_STRIDE`) says otherwise, which is the oracle's point.
#[route(POST "/leaky-ping", public(Org))]
fn emit_leaky_ping(tx: &mut Tx, body: ScopedEmitBody, ctx: &Ctx) -> Result<(), ApiError> {
    let mut tx = tx.scope::<Org>(Id::from_raw(SCOPED_TENANT_A), ctx)?;
    tx.emit(OrgPing { n: body.n });
    Ok(())
}

/// The scoped workload's app. Same `DbConfig` as `realtime_app` — small
/// retention on purpose, so a client falls behind it inside a modest number
/// of emits — over the scoped routes. A separate function rather than a
/// parameter on `realtime_app` so the flagship's bytes are untouched (R56).
fn scoped_realtime_app(sim: &Sim) -> io::Result<App> {
    Ok(App::open(
        Box::new(sim.disk()),
        Schema::new(),
        Box::new(sim.entropy()),
        AppConfig {
            body_cap: 1 << 20,
            db: charpente::DbConfig {
                wal: WalConfig {
                    dir: "wal".to_string(),
                    segment_bytes: 4096,
                },
                store_path: "tables.redb".to_string(),
                event_retention: 4,
                checkpoint_every: 2,
                ..charpente::DbConfig::default()
            },
            ..AppConfig::default()
        },
    )?
    .mount(routes![emit_scoped_ping, emit_leaky_ping, scoped_feed]))
}

/// One emit: whichever tenant this draw picks, with the tenant folded into
/// the payload (`SCOPED_N_STRIDE`) so the oracle has a source of truth that
/// is not the row.
fn pick_scoped_emit(sim: &Sim) -> ScopedEmitBody {
    let e = sim.entropy();
    let org = if e.next_u64().is_multiple_of(2) {
        SCOPED_TENANT_A
    } else {
        SCOPED_TENANT_B
    };
    let n = org * SCOPED_N_STRIDE + sim.entropy().next_u64() % SCOPED_N_STRIDE;
    ScopedEmitBody {
        org: WireU64(org),
        n: WireU64(n),
    }
}

/// `settle_boot` over `scoped_realtime_app`. Copied rather than parameterized
/// deliberately: threading an app constructor through `settle_boot` would
/// edit a function the flagship calls, and R56's whole point is that the
/// pinned property's bytes do not move for the sibling's benefit. Every word
/// of `settle_boot`'s rationale applies here unchanged.
fn scoped_settle_boot(sim: &Sim) -> Result<Option<Kernel>, String> {
    for try_n in 0..SETTLE_BOOT_TRIES {
        if try_n > 0 && try_n.is_multiple_of(SETTLE_CYCLE_EVERY) {
            sim.restart();
        }
        let app = match scoped_realtime_app(sim) {
            Ok(app) => app,
            Err(e) => {
                // No restart clears a durable refusal (S5 re-review R-M2), and
                // only a fired durable-corruption fault excuses one (S3b item 0).
                if durable_refusal(sim, &e.to_string())? {
                    return Ok(None);
                }
                if !is_fault_fallout(&e.to_string()) {
                    return Err(format!("settle open failed non-injected: {e}"));
                }
                continue;
            }
        };
        match Kernel::new(app, Box::new(sim.clock())) {
            Ok(kernel) => return Ok(Some(kernel)),
            Err(e) => {
                if !is_fault_fallout(&e.to_string()) {
                    return Err(format!("settle Kernel::new failed non-injected: {e}"));
                }
            }
        }
    }
    Ok(None)
}

/// One universe of the scoped sibling: same epoch/round/settle shape as
/// `realtime_workload`, with two tenants emitting into one node and one
/// tenant-A feed. `Client` is reused — in tenant A (`Client::in_tenant`), its
/// `matches`/`first_unaccounted` demand every tenant-A ledger row above the
/// floor be received, and its `try_match` rejects a delivered row recorded in
/// another tenancy — and `scoped_tenant_oracle` adds the payload-versus-row
/// arms `Client` cannot have.
/// Returns the frame trace, so the replay check has an oracle.
fn scoped_realtime_workload(sim: &mut Sim) -> Result<ScopedTrace, String> {
    scoped_realtime_workload_judged(sim, durable_corruption_fired)
}

/// `scoped_realtime_workload`, its client asking `cut_explained` whether a
/// fault explains a cut. The property passes `durable_corruption_fired`. A pin
/// passes a check that explains nothing, so every cut stands in for one no
/// injected fault explains, which is the S3 re-review's stand-in (re-sync
/// disabled) for a platform durability bug (S3b item 1).
fn scoped_realtime_workload_judged(
    sim: &mut Sim,
    cut_explained: fn(&Sim) -> bool,
) -> Result<ScopedTrace, String> {
    let mut client = Client::new(ORG_PING_ONLY, None).in_tenant(SCOPED_TENANT_A);
    client.cut_explained = cut_explained;
    // Every boundary `push_subscribe` reads goes through `resync_if_cut`. Each
    // recovery is followed by a subscribe before any emit, so every cut is
    // seen there before an id it cut can be re-minted. The flagship's
    // drain-error probe cannot see a cut whose id is re-minted for the other
    // tenant: no frame ever mismatches, and retention can collect the row
    // before the settle. The re-sync reads a value already in hand, so it
    // adds no disk op.

    for _epoch in 0..EPOCHS {
        // A give-up already recorded means this universe adjudicated
        // nothing; carrying on can only turn it into a failure, and the
        // runner ranks a later `Err` above an earlier give-up.
        if sim.gave_up().is_some() {
            return Ok(client.trace);
        }
        let entropy = sim.entropy();
        // Sized to redb 4.3.0's disk-op volume per round: DESIGN §1, "countdowns".
        sim.crash_disk_after(51 + entropy.next_u64() % 410);
        let Some(app) = boot_through_fallout(sim, BOOT_TRIES, is_fault_fallout, || {
            scoped_realtime_app(sim)
        })
        .map_err(|e| format!("open failed non-injected: {e}"))?
        else {
            sim.restart();
            continue;
        };
        let mut kernel = match Kernel::new(app, Box::new(sim.clock())) {
            Ok(k) => k,
            Err(e) => {
                if !is_fault_fallout(&e.to_string()) {
                    return Err(format!("Kernel::new failed non-injected: {e}"));
                }
                sim.restart();
                continue;
            }
        };
        client.need_subscribe = true;
        client.current_handle = None;

        let mut source = SimSource::new(Box::new(sim.clock()), sim.now().as_nanos() + 2000 * SEC);
        let sinks = source.sinks();
        let responses = source.responses();
        let mut resp_idx = 0usize;
        let mut t = sim.now().as_nanos();

        for _round in 0..ROUNDS_PER_EPOCH {
            let mut subscribe = None;
            if client.need_subscribe {
                match push_subscribe(sim, &kernel, &mut source, SCOPED_FEED_A, client.cursor, t) {
                    Ok(info) => {
                        client.resync_if_cut(sim, info.1)?;
                        subscribe = Some(info);
                        t += SEC;
                    }
                    Err(e) if is_fault_fallout(&e) => {
                        sim.restart();
                        client.need_subscribe = true;
                        client.current_handle = None;
                        break;
                    }
                    Err(e) => return Err(format!("push_subscribe failed non-injected: {e}")),
                }
            }

            let n_emits = 1 + sim.entropy().next_u64() % 3;
            for _ in 0..n_emits {
                let body = pick_scoped_emit(sim);
                source.push_at(t, Request::new(Method::Post, "/scoped-ping").json(&body));
                t += SEC;
            }

            let run_result = kernel.run(&mut source);

            let new_responses = {
                let guard = responses
                    .lock()
                    .expect("responses lock poisoned by an earlier panic");
                guard[resp_idx..].to_vec()
            };
            resp_idx += new_responses.len();
            client.absorb(sim, kernel.app().db(), &new_responses, subscribe, &sinks)?;

            if let Err(e) = run_result {
                if !is_fault_fallout(&e.to_string()) {
                    return Err(format!("kernel.run failed non-injected: {e}"));
                }
                sim.restart();
                client.need_subscribe = true;
                client.current_handle = None;
                break;
            }
            // Placed as in `realtime_workload`'s round loop, for its reason.
            if sim.gave_up().is_some() {
                return Ok(client.trace);
            }
        }
    }

    // Settle, identical in shape and rationale to the flagship's: no new
    // emits, no subscriber stalls, boot a node through the remaining faults,
    // let the client reconnect and catch up on its own tenant's backlog.
    sim.quiesce_subscriber_stalls();
    // Same counter, same reason, as `realtime_workload` above: zero clean
    // attempts means this universe never observed a delivery and cannot
    // testify about a missed one.
    let mut clean_attempts = 0u32;
    for _attempt in 0..SETTLE_ATTEMPTS {
        // A give-up already recorded means this universe adjudicated
        // nothing; carrying on can only turn it into a failure, and the
        // runner ranks a later `Err` above an earlier give-up.
        if sim.gave_up().is_some() {
            return Ok(client.trace);
        }
        if client.settled() {
            break;
        }
        let Some(mut kernel) = scoped_settle_boot(sim)? else {
            sim.restart();
            continue;
        };
        client.need_subscribe = true;
        client.current_handle = None;

        let mut source = SimSource::new(Box::new(sim.clock()), sim.now().as_nanos() + 2000 * SEC);
        let sinks = source.sinks();
        let responses = source.responses();
        let t = sim.now().as_nanos();

        let subscribe =
            match push_subscribe(sim, &kernel, &mut source, SCOPED_FEED_A, client.cursor, t) {
                Ok(info) => {
                    client.resync_if_cut(sim, info.1)?;
                    info
                }
                Err(e) if is_fault_fallout(&e) => {
                    sim.restart();
                    continue;
                }
                Err(e) => return Err(format!("settle push_subscribe failed non-injected: {e}")),
            };

        let run_result = kernel.run(&mut source);
        let new_responses = responses
            .lock()
            .expect("responses lock poisoned by an earlier panic")
            .clone();
        client.absorb(
            sim,
            kernel.app().db(),
            &new_responses,
            Some(subscribe),
            &sinks,
        )?;
        if let Err(e) = run_result {
            if !is_fault_fallout(&e.to_string()) {
                return Err(format!("settle kernel.run failed non-injected: {e}"));
            }
            sim.restart();
            client.need_subscribe = true;
            client.current_handle = None;
        } else {
            clean_attempts += 1;
        }
        // The loop-top check never runs after the LAST attempt, whose give-up
        // would otherwise fall through to the unsettled verdict below.
        if sim.gave_up().is_some() {
            return Ok(client.trace);
        }
    }

    if !client.settled() {
        // See `realtime_workload`'s note: `settle_boot` returning `None` is
        // fault-only, so no clean attempt means there was no node to catch up
        // FROM. That universe adjudicated nothing and gives up. There is no
        // rolled-back-ledger give-up here, unlike the flagship's: every boundary
        // re-syncs a cut a fault explains (`resync_if_cut`), so a ledgered id the
        // node rolled back past is one nothing explains, which fails below. That
        // give-up reached 0 of 3,200 universes over 25 bases (S3b item 1), as the
        // S3 re-review measured (0 of 16,384), so it went.
        if clean_attempts == 0 {
            sim.give_up(format!(
                "no settle attempt completed without fault fallout over \
                 {SETTLE_ATTEMPTS} attempts — there was no node to catch up from ({})",
                client.unsettled_report()
            ));
            return Ok(client.trace);
        }
        return Err(client.unsettled_report());
    }
    scoped_tenant_oracle(&client.ledger, &client.trace, SCOPED_TENANT_A)?;

    // Coverage. Every key here is a way this property can go quietly
    // vacuous, which is why each is a `require:` and a measured `floor:`
    // rather than a report.
    let mut saw_a = false;
    let mut saw_b = false;
    for (id, (_, _, scope, _)) in &client.ledger {
        match *scope {
            None => {
                return Err(format!(
                    "event id {id} carries no tenant at all — every emit in this workload goes \
                     through `ScopedTx::emit`, so a row with no tenant means the scope was \
                     dropped on the way to the log"
                ));
            }
            Some(SCOPED_TENANT_A) => saw_a = true,
            Some(_) => saw_b = true,
        }
    }
    if saw_a && saw_b {
        sim.cover("tenant:both-tenants-emitted");
    }
    if !client.verified.is_empty() {
        sim.cover("tenant:own-backlog-verified");
    }
    // The "and none of tenant B's" half, made non-vacuous: a row of another
    // tenant sat in the very window `settled()` scanned and was never
    // delivered. Without this the property could settle over universes where
    // B's rows all aged out before the subscriber was ever exposed to them.
    let received: BTreeSet<u64> = client.trace.iter().map(|&(id, ..)| id).collect();
    let lo = client.floor.min(client.next_scan_id);
    if client
        .ledger
        .range(lo..client.next_scan_id)
        .any(|(id, (_, _, scope, _))| *scope != Some(SCOPED_TENANT_A) && !received.contains(id))
    {
        sim.cover("tenant:foreign-backlog-not-delivered");
    }
    client.give_up_if_nothing_adjudicated(sim);
    Ok(client.trace)
}

/// The sibling. Same oracle shape as
/// `reconnecting_client_misses_nothing_under_the_fault_matrix`, same universe
/// count, its own base seed — and floors pinned to EXACTLY the counts
/// measured at that base on the first run, no margin subtracted, which is the
/// discipline `CONV_SCOPED_*_FLOOR` states: a drop is a finding, not a number
/// to lower.
///
/// Measured over the 128 universes at `SCOPED_REALTIME_BASE`, read off a
/// batch reproduced identically twice: `tenant:both-tenants-emitted` 77,
/// `tenant:own-backlog-verified` 79 (78 since the 2026-09-07 barrier stream
/// split — see `CONV_RESTORED_FLOOR`'s table in `archive.rs`),
/// `tenant:foreign-backlog-not-delivered` 85, `realtime:resume-backlog` 128
/// (125 since that same split),
/// `fault:subscriber_stall` 30, `realtime:stall-drop` 30, `realtime:gap` 8.
/// `realtime:resume-backlog` is floored because a batch that stopped
/// reconnecting with backlog waiting would still settle, vacuously — this
/// property is about what a RESUME carries. `realtime:stall-drop` and
/// `fault:subscriber_stall` are required but not floored: their rate is a
/// fault axis's, already pinned in effect by the flagship, and a second exact
/// pin on it would turn one finding into two properties' failures.
///
/// **The base is pinned, and here that is load-bearing in a way worth
/// stating.** The settle budget these workloads share (`SETTLE_ATTEMPTS`,
/// `SETTLE_BOOT_TRIES`) is not enough for every swarm config, and that is a
/// property of the FLAGSHIP's workload, not of the scope: run
/// `realtime_workload` from this very invocation — one runner, one universe
/// count, so the comparison is like for like — and it fails "client never
/// caught up" at fresh bases `0x9F3A_0001` and `0x41D7_0001`, passes at
/// `0xC58E_0001`, and fails at THIS property's own base, where the scoped
/// workload below is green. The scoped workload's verdicts at those same
/// three fresh bases are identical — fail, fail, pass. **UPDATE 2026-09-07: those
/// fresh-base failures were "client never caught up" raised by a settle phase in
/// which no attempt ever completed cleanly — the budget being too small for a
/// config is a universe that adjudicated NOTHING, and it is now reported as a
/// give-up rather than as a miss (see
/// `a_universe_that_never_booted_a_node_gives_up_rather_than_reporting_a_miss`).
/// So the answer was not "raise the budget", which this comment correctly
/// declined to do; it was to classify the outcome. The paragraph is kept because
/// its measurement was right and it is the record of the limitation being known
/// before it was understood.** Per-SEED comparison is
/// not available and should not be attempted: the two workloads draw
/// different numbers of entropy values per round, so one seed hands them the
/// same `FaultConfig` and two different fault schedules. The base-level
/// verdicts are the like-for-like reading, and they say this sibling is
/// exactly as fresh-base-robust as the property it is a sibling of. Raising
/// the budget is not this task's to do: it would move the flagship's pinned
/// coverage.
#[test]
fn a_reconnecting_tenant_subscriber_misses_nothing_and_leaks_nothing() {
    run_universes_swarm_with!(
        SCOPED_REALTIME_BASE,
        128,
        |_cfg: &mut FaultConfig| {},
        |sim: &mut Sim| scoped_realtime_workload(sim).map(|_trace| ()),
        require: [
            "boot:booted-after-retry",
            "tenant:both-tenants-emitted",
            "tenant:own-backlog-verified",
            "tenant:foreign-backlog-not-delivered",
            "realtime:resume-backlog",
            "realtime:stall-drop",
            "fault:subscriber_stall"
        ],
        floor: [
            ("tenant:both-tenants-emitted", SCOPED_BOTH_TENANTS_FLOOR),
            ("tenant:own-backlog-verified", SCOPED_OWN_BACKLOG_FLOOR),
            (
                "tenant:foreign-backlog-not-delivered",
                SCOPED_FOREIGN_SKIPPED_FLOOR
            ),
            ("realtime:resume-backlog", SCOPED_RESUME_BACKLOG_FLOOR)
        ],
        ceiling: [("universe:gave-up", SCOPED_GAVE_UP_CEILING)]
    );
}

/// **Re-measured 2026-09-07**, at the same base, for two deliberate changes:
/// a settle phase with no clean attempt now gives up rather than reporting a
/// miss, and a drain error now probes the store to tell a rolled-back ledger
/// entry from a real delivery defect (`diverged_ledger_entry`). The probe adds
/// disk ops on the ERROR path only, so universes that hit it diverge in
/// schedule, and one universe now gives up instead of running its epochs to
/// completion — a universe that adjudicated nothing, whose coverage was never
/// evidence for these rates in the first place.
///
/// | key | was | now |
/// |---|---|---|
/// | `tenant:both-tenants-emitted` | 77 | 75 |
/// | `tenant:own-backlog-verified` | 79 | 79 (measures 80) |
/// | `tenant:foreign-backlog-not-delivered` | 85 | 80 |
/// | `realtime:resume-backlog` | 128 | 128 (measures 130) |
///
/// Both unmoved floors are left alone: a floor is a MINIMUM, and raising one
/// that is not failing is a separate decision from repairing one that is.
/// Counts reproduced identically across two consecutive batches before these
/// numbers were taken, the same discipline the original measurement states.
///
/// **Re-measured 2026-09-30**, same base, two identical batches, for redb
/// 4.3.0 and the crash countdown re-sized to its disk-op volume. This workload
/// takes the re-size without the calibration margin (DESIGN §1, "countdowns":
/// with it, `realtime:resume-backlog` fell to 122). The "floor" column is the
/// floor as it stood: the 2026-09-07 directory-barrier split set two of them
/// to 78 and 125 without updating the table above. The "4.1.0" column is the
/// count at the parent commit. The paragraph above left non-failing floors
/// alone during a repair. This time a dependency's IO shape re-rolled every
/// universe, so each floor whose count the upgrade moved is set on its new
/// count. `realtime:resume-backlog` reads 126 at both versions; it did not
/// move, so its floor stays. None fell.
///
/// | key | floor | 4.1.0 | now | new floor |
/// |---|---|---|---|---|
/// | `tenant:both-tenants-emitted` | 75 | 76 | 83 | 83 |
/// | `tenant:own-backlog-verified` | 78 | 78 | 81 | 81 |
/// | `tenant:foreign-backlog-not-delivered` | 80 | 87 | 98 | 98 |
/// | `realtime:resume-backlog` | 125 | 126 | 126 | 125 |
///
/// **Re-set 2026-10-01**, same base, two identical batches, for S5 and D1
/// together: each epoch's boot now boots again through
/// `checks::boot_through_fallout` instead of skipping the epoch on its first
/// faulted boot, `Db::open` no longer discards a store over an IO error, and
/// WAL recovery syncs each segment before replay. None fell; each is set on
/// its new count.
///
/// | key | was | now |
/// |---|---|---|
/// | `tenant:both-tenants-emitted` | 83 | 106 |
/// | `tenant:own-backlog-verified` | 81 | 96 |
/// | `tenant:foreign-backlog-not-delivered` | 98 | 99 |
/// | `realtime:resume-backlog` | 125 | 206 |
const SCOPED_BOTH_TENANTS_FLOOR: u64 = 106;
const SCOPED_OWN_BACKLOG_FLOOR: u64 = 96;
const SCOPED_FOREIGN_SKIPPED_FLOOR: u64 = 99;
const SCOPED_RESUME_BACKLOG_FLOOR: u64 = 206;

/// The give-up caps of the two settled-client properties, exact at their
/// pinned bases (S3b review round 2, 2026-10-02). S3b items 1, 3 and 7 made
/// these universes give up where they passed: the flagship 3 -> 26, the
/// scoped sibling 0 -> 23, most of them clients owed nothing and delivered
/// nothing (item 7). A give-up is never failed, so without a cap a batch that
/// drifted to giving up everything would stay green (GUIDE §8). The scoped
/// sibling floors its reach keys above; the flagship floors
/// `realtime:frame-verified`, one mark per frame a client delivered and
/// checked in a universe that kept its verdict: 1,049 at its base.
const FLAGSHIP_GAVE_UP_CEILING: u64 = 26;
const FLAGSHIP_FRAMES_VERIFIED_FLOOR: u64 = 1049;
const SCOPED_GAVE_UP_CEILING: u64 = 23;

/// Found 2026-09-30 by S3's fresh-base scan, at seed `0x20851976ea4bae36`
/// (base `0x5E1F0013`). A misdirected write moved event id 5's WAL frame (seq
/// 8, tenant A) off its offset, so the next recovery cut the log there, and the
/// following emits re-minted id 5 for tenant B. The client's ledger had
/// already recorded the old id 5 and its resume never saw the re-mint (tenant
/// B's rows are filtered), so the flagship's drain-error probe never ran, and
/// by the settle retention had collected the row the probe would have read.
/// The universe failed "client never caught up" on an event its own node had
/// lost. From 2026-09-30 it gave up; that excuse also swallowed an id
/// re-minted for tenant A and never delivered (review I2). Since 2026-10-01
/// the boundary read after that recovery re-syncs the client to the node's
/// timeline (`Client::resync_if_cut`), so the cut id is B's and not owed, and
/// the universe reaches a verdict on everything A was owed.
///
/// Pinned at seed `0x083bd434c717d60c` (base `0x5E1F0039`, universe 73) since
/// S5 with D1 re-keyed every universe the same day and the original seed
/// stopped meeting durable corruption. It meets the same shape at id 3: one
/// misdirected write, a recovery that cuts id 3 out of tenant A's recorded
/// ledger, and id 3 re-minted for tenant B, which A never receives. With the
/// re-sync disabled it gives up "ledger id 3 is behind this node's timeline",
/// the 2026-09-30 behaviour. Found by disabling the re-sync and scanning 48
/// bases (`0x2EB2_5C09` and `0x5E1F0013`..`0x5E1F0041`, 12,288 universes) for
/// that give-up: 12 universes give it, and this is the only one whose cut id
/// was re-minted rather than left absent.
#[test]
fn a_ledgered_event_cut_by_durable_corruption_and_re_minted_elsewhere_is_resynced() {
    const SEED: u64 = 0x083b_d434_c717_d60c;
    let mut state = SEED;
    let cfg_seed = charpente_sim::rng::splitmix64(&mut state);
    let mut cfg = FaultConfig::swarm(&mut charpente_sim::rng::Rng::new(cfg_seed));
    charpente_sim::sample_extended_fields(SEED, &mut cfg);
    let mut sim = Sim::with_faults(SEED, cfg);

    let trace = scoped_realtime_workload(&mut sim)
        .expect("an event lost to durable corruption must not fail the universe");
    assert_eq!(
        sim.gave_up(),
        None,
        "re-synced to the node's timeline, it reaches a verdict on what A was owed"
    );
    assert!(
        durable_corruption_fired(&sim) && sim.coverage().contains_key("realtime:ledger-resynced"),
        "this universe's reason is a cut by durable corruption, seen and re-synced: {:?}",
        sim.coverage()
    );
    assert!(
        !trace.iter().any(|&(id, ..)| id == 3),
        "id 3 was re-minted for tenant B, so A must never receive it: {trace:?}"
    );
}

/// The same universe with its cut standing in for one nothing explains (S3b
/// item 1, the S3 re-review's stand-in: re-sync disabled): the client's
/// `cut_explained` answers false. That is a lost event, and the universe must
/// FAIL by name, at the boundary that first sees the cut (since item 3; under
/// item 1 it failed later, "client never caught up" on id 3). Until item 1 the
/// settle's rolled-back-ledger arm gave it up ("ledger id 3 is behind this
/// node's timeline") without asking whether anything explained the cut, so a
/// platform durability bug passed as a give-up.
#[test]
fn a_cut_nothing_explains_fails_the_scoped_universe_by_name() {
    const SEED: u64 = 0x083b_d434_c717_d60c;
    let mut state = SEED;
    let cfg_seed = charpente_sim::rng::splitmix64(&mut state);
    let mut cfg = FaultConfig::swarm(&mut charpente_sim::rng::Rng::new(cfg_seed));
    charpente_sim::sample_extended_fields(SEED, &mut cfg);
    let mut sim = Sim::with_faults(SEED, cfg);

    let err = scoped_realtime_workload_judged(&mut sim, |_| false)
        .expect_err("a cut nothing explains is a lost event");
    assert!(
        err.starts_with("ledger id 3 was recorded off this node")
            && err.contains("no fault on this node explains the cut"),
        "{err}"
    );
    assert_eq!(
        sim.gave_up(),
        None,
        "a lost event is a failure, never a give-up"
    );
}

/// `resync_if_cut` drops exactly the ids a cut removed: an id below the
/// store's `next_event_id` is still on the node's timeline and stays owed, so
/// a client missing it fails rather than being excused with the cut one
/// (review I2's mixed set).
#[test]
fn a_cut_drops_only_the_ids_it_cut_and_the_rest_stay_owed() {
    let sim = Sim::with_faults(0x2EB2_0901, FaultConfig::none());
    sim.cover("fault:misdirected_write");
    let mut client = Client::new(PING_ONLY, None).in_tenant(SCOPED_TENANT_A);
    for id in [3, 5] {
        client.ledger.insert(
            id,
            ("ping", None, Some(SCOPED_TENANT_A), format!("old {id}")),
        );
    }
    client.next_scan_id = 6;

    client.resync_if_cut(&sim, 5).unwrap();
    assert_eq!(client.ledger.keys().copied().collect::<Vec<_>>(), [3]);
    assert_eq!(
        client.next_scan_id, 5,
        "the next scan re-reads from the cut"
    );
    assert_eq!(
        client.first_unaccounted(),
        Some(3),
        "id 3 was never cut, so the client still owes it"
    );
}

/// A cut trims `last_id` to the last id the node still holds, `head - 1`: no
/// lower, since an id still on the timeline must never be redelivered (the
/// strictly-increasing check keeps its teeth), and no higher, since the cut id
/// itself is what the node re-mints. An id below the head is untouched, and a
/// head of 0 leaves nothing seen (D5).
#[test]
fn a_cut_trims_last_id_to_the_last_id_the_node_still_holds() {
    for (last, head, want) in [
        (Some(5), 5, Some(4)),
        (Some(7), 5, Some(4)),
        (Some(4), 5, Some(4)),
        (Some(2), 5, Some(2)),
        (Some(0), 0, None),
        (None, 5, None),
    ] {
        let mut client = Client::new(PING_ONLY, None);
        client.last_id = last;
        client.resync_to_head(head);
        assert_eq!(client.last_id, want, "last_id {last:?} cut to head {head}");
    }
}

/// The residual review I2 found: an id cut and re-minted for THIS
/// subscription, then never delivered, must fail. After the re-sync the
/// re-scan records the re-mint, and neither the old verification of the cut
/// event nor a frame the client received from the lost timeline counts for it.
#[test]
fn a_cut_id_re_minted_for_the_subscriber_is_owed_again() {
    let sim = Sim::with_faults(0x2EB2_0902, FaultConfig::none());
    sim.cover("fault:misdirected_write");
    let mut client = Client::new(PING_ONLY, None).in_tenant(SCOPED_TENANT_A);
    client.ledger.insert(
        5,
        ("ping", None, Some(SCOPED_TENANT_A), "old 5".to_string()),
    );
    client.verified.insert(5);
    client.next_scan_id = 6;
    // A bare reconnect above the cut forfeited everything below 6.
    client.floor = 6;
    assert!(
        client.settled(),
        "before the cut, id 5 was delivered and verified"
    );

    client.resync_if_cut(&sim, 5).unwrap();
    // What the next `sync_ledger` reads: id 5 re-minted for tenant A.
    client.ledger.insert(
        5,
        ("ping", None, Some(SCOPED_TENANT_A), "new 5".to_string()),
    );
    client.next_scan_id = 6;
    assert_eq!(
        client.first_unaccounted(),
        Some(5),
        "the re-mint is owed: a client that never receives it fails"
    );
}

/// A frame received from the lost timeline goes with it, and so does an aged-out
/// mark at a cut id: a re-mint delivered there is verified by its content once
/// the re-scan reads it, never confirmed by id before that.
#[test]
fn a_frame_from_the_lost_timeline_is_dropped_and_the_re_mint_is_verified_by_content() {
    let sim = Sim::with_faults(0x2EB2_0904, FaultConfig::none());
    sim.cover("fault:misdirected_write");
    let mut client = Client::new(PING_ONLY, None).in_tenant(SCOPED_TENANT_A);
    client.ledger.insert(
        6,
        ("ping", None, Some(SCOPED_TENANT_A), "old 6".to_string()),
    );
    client.gone.insert(5);
    client.pending.push_back((5, "ping", "old 5".to_string()));
    client.next_scan_id = 7;

    client.resync_if_cut(&sim, 5).unwrap();
    client.pending.push_back((5, "ping", "new 5".to_string()));
    assert_eq!(client.drain(), Ok(()));
    assert!(
        !client.settled(),
        "the re-mint's frame waits for the re-scan: a cut id is not confirmed by id"
    );
    // What the next `sync_ledger` reads: id 5 re-minted for tenant A.
    client.ledger.insert(
        5,
        ("ping", None, Some(SCOPED_TENANT_A), "new 5".to_string()),
    );
    client.next_scan_id = 6;
    assert_eq!(
        client.drain(),
        Ok(()),
        "only the re-mint's frame is left to match"
    );
    assert!(client.settled() && client.verified.contains(&5));
}

/// The other direction: with no durable corruption on record, a recorded id at
/// or above `next_event_id` is a lost event. It fails where it is first seen,
/// and nothing is re-synced away (S3b item 3: it used to stay owed, failing
/// only in a settle with a clean attempt).
#[test]
fn without_durable_corruption_a_cut_is_not_resynced() {
    let sim = Sim::with_faults(0x2EB2_0903, FaultConfig::none());
    let mut client = Client::new(PING_ONLY, None).in_tenant(SCOPED_TENANT_A);
    client.ledger.insert(
        5,
        ("ping", None, Some(SCOPED_TENANT_A), "old 5".to_string()),
    );
    client.next_scan_id = 6;

    let err = client
        .resync_if_cut(&sim, 5)
        .expect_err("a cut nothing explains is a lost event");
    assert_eq!(
        err,
        "ledger id 5 was recorded off this node, but its next_event_id is now 5: a recovery \
         cut a committed event, and no fault on this node explains the cut, so this node lost \
         an event it had committed"
    );
    assert_eq!(
        client.first_unaccounted(),
        Some(5),
        "no corruption fired, so id 5 is still owed"
    );
    assert!(!sim.coverage().contains_key("realtime:ledger-resynced"));
}

/// A settled universe whose client was owed nothing and delivered nothing gives
/// up instead of passing (S3b item 7, S5 review I2): its settle checked an empty
/// window. One seed per workload, each the first at its batch's base, plus
/// `0xe9110992f2280d36`, whose client was owed only an event durable corruption
/// cut, so the re-sync at its boundary left nothing owed (item 3). At the
/// pinned bases 23 flagship and 23 scoped universes give up this way; before,
/// each passed.
#[test]
fn a_settled_client_owed_nothing_and_delivered_nothing_gives_up() {
    for (seed, scoped) in [
        (0x8d90_5371_e979_b020_u64, false),
        (0xe911_0992_f228_0d36, false),
        (0xb1c2_07a8_e3ca_62dd, true),
    ] {
        let mut state = seed;
        let cfg_seed = charpente_sim::rng::splitmix64(&mut state);
        let mut cfg = FaultConfig::swarm(&mut charpente_sim::rng::Rng::new(cfg_seed));
        charpente_sim::sample_extended_fields(seed, &mut cfg);
        let mut sim = Sim::with_faults(seed, cfg);
        let trace = if scoped {
            scoped_realtime_workload(&mut sim)
        } else {
            realtime_workload(&mut sim)
        }
        .unwrap_or_else(|e| panic!("seed {seed:#x}: {e}"));
        assert!(trace.is_empty(), "seed {seed:#x}: {trace:?}");
        let reason = sim
            .gave_up()
            .unwrap_or_else(|| panic!("seed {seed:#x}: an empty settle must give up, not pass"));
        assert!(
            reason.starts_with("the settled client was owed nothing and delivered nothing"),
            "seed {seed:#x}: {reason}"
        );
    }
}

/// The window decides, not the trace alone: a row the subscription does not
/// match, sitting in the window undelivered, is a check its filter passed.
#[test]
fn an_undelivered_row_the_filter_withheld_is_a_verdict_not_nothing() {
    let mut client = Client::new(PING_ONLY, None).in_tenant(SCOPED_TENANT_A);
    assert!(client.adjudicated_nothing(), "empty ledger, empty trace");
    client
        .ledger
        .insert(0, ("ping", None, Some(SCOPED_TENANT_B), "b's".to_string()));
    client.next_scan_id = 1;
    assert!(
        client.settled() && !client.adjudicated_nothing(),
        "tenant B's row was in the window and withheld"
    );
    client.floor = 1;
    assert!(
        client.adjudicated_nothing(),
        "below the floor it was never subscribed to"
    );
}

/// Floors pinned at an exact measurement are only meaningful if the batch is
/// reproducible, which is the same claim the flagship's own replay check
/// makes for its trace.
#[test]
fn scoped_realtime_frame_traces_replay_identically_for_the_same_seed() {
    charpente_sim::checks::replays_identically(0x2EB2_5C0D, scoped_realtime_workload).unwrap();
}

/// One clean kernel, one tenant-A feed, one emit per tenant
/// through `route` — the twin the fireability proof needs, so victim and
/// control face identical bytes and differ in nothing but which emit route
/// ran.
fn scoped_twin_run(route: &'static str) -> (Ledger, ScopedTrace) {
    let sim = Sim::with_faults(0x2EB1_5C09, FaultConfig::none());
    let app = scoped_realtime_app(&sim).unwrap();
    let mut kernel = Kernel::new(app, Box::new(sim.clock())).unwrap();

    let mut source = SimSource::new(Box::new(sim.clock()), sim.now().as_nanos() + 10 * SEC);
    let t = sim.now().as_nanos();
    source.push_at(t, Request::new(Method::Get, SCOPED_FEED_A));
    for org in [SCOPED_TENANT_A, SCOPED_TENANT_B] {
        source.push_at(
            t,
            Request::new(Method::Post, route).json(&ScopedEmitBody {
                org: WireU64(org),
                n: WireU64(org * SCOPED_N_STRIDE + 42),
            }),
        );
    }
    let sinks = source.sinks();
    for _ in 0..16 {
        kernel.step(&mut source).expect("no step may fail here");
    }

    let mut client = Client::new(ORG_PING_ONLY, None).in_tenant(SCOPED_TENANT_A);
    client.sync_ledger(kernel.app().db()).unwrap();
    let trace = sinks.lock().expect("sinks lock")[0]
        .frames()
        .iter()
        .map(|f| (f.id, f.name, f.json.clone()))
        .collect();
    (client.ledger, trace)
}

/// The fireability proof for `a_reconnecting_tenant_subscriber_misses_nothing_and_leaks_nothing`
/// (Task 8's discipline): the property's own oracle, run over data a naive
/// emit produced, must go RED and NAME the offence — and over the correct
/// emit on the same bytes must pass. Without the control half, "the oracle
/// rejected something" would not distinguish a working discriminator from an
/// oracle that rejects everything.
///
/// The victim is `emit_leaky_ping`: a real `ScopedTx::emit`, in tenant A
/// whatever tenant the request names — the leak direction, and exactly the
/// shape a `ScopedTx::emit` recording a constant tenant produces.
#[test]
fn the_scoped_oracle_catches_an_emit_recorded_in_the_wrong_tenant() {
    // ---- control: the correct route, on these exact bytes ----
    let (ledger, trace) = scoped_twin_run("/scoped-ping");
    assert_eq!(
        ledger.len(),
        2,
        "both tenants must have committed an event, or the control proves nothing: {ledger:?}"
    );
    assert_eq!(
        trace.len(),
        1,
        "tenant A's feed must have received exactly its own event: {trace:?}"
    );
    scoped_tenant_oracle(&ledger, &trace, SCOPED_TENANT_A)
        .expect("the correct scoped emit must pass the property's own oracle");

    // ---- victim: the same bytes, the naive emit ----
    let (ledger, trace) = scoped_twin_run("/leaky-ping");
    assert_eq!(ledger.len(), 2, "same two events: {ledger:?}");
    assert_eq!(
        trace.len(),
        2,
        "the leak itself: tenant A's feed received BOTH tenants' events"
    );

    let err = scoped_tenant_oracle(&ledger, &trace, SCOPED_TENANT_A)
        .expect_err("the delivery arm must reject a cross-tenant frame");
    assert!(err.contains("cross-tenant delivery"), "{err}");
    assert!(
        err.contains(&format!("ScopedTx<Org>({SCOPED_TENANT_B})")),
        "the failure must name the tenant whose event leaked: {err}"
    );

    // The ledger arm, fired on its own: the corruption is in the durable row
    // whatever the subscriber happened to receive.
    let err = scoped_tenant_oracle(&ledger, &[], SCOPED_TENANT_A)
        .expect_err("the ledger arm must reject a row whose tenant is not its emitting scope");
    assert!(err.contains("EventRow.scope"), "{err}");
    assert!(
        err.contains(&format!("ScopedTx<Org>({SCOPED_TENANT_B})")),
        "the failure must name the emitting scope: {err}"
    );
}

// ============== the re-sync remedy's order, a change landed in every window

/// A row and an event for one change, so a client can rebuild state two ways:
/// by reading the rows, or by applying the events.
#[table(global)]
struct Mark {
    id: Id<Mark>,
    n: u64,
}

/// The one row every change overwrites: the latest change. A client that
/// installs a stale read over a newer frame regresses it, which a set of marks
/// alone could not show.
#[table(global)]
struct Latest {
    id: Id<Latest>,
    n: u64,
}

#[event(global)]
struct Marked {
    n: WireU64,
}

#[derive(WireDerive, Serialize, Deserialize)]
struct MarkBody {
    n: WireU64,
}

#[route(POST "/mark", public)]
fn put_mark(tx: &mut Tx, body: MarkBody) -> Result<(), ApiError> {
    tx.put(&Mark {
        id: Id::from_raw(body.n.0),
        n: body.n.0,
    });
    tx.put(&Latest {
        id: Id::from_raw(0),
        n: body.n.0,
    });
    tx.emit(Marked { n: body.n });
    Ok(())
}

/// The marks, then the latest change: `1,2,3|3`.
#[route(GET "/marks", public)]
fn read_marks(tx: &mut Tx) -> Result<String, ApiError> {
    let marks = tx.scan::<Mark>()?;
    let latest = tx.get::<Latest>(Id::from_raw(0))?;
    Ok(format!(
        "{}|{}",
        marks
            .iter()
            .map(|m| m.n.to_string())
            .collect::<Vec<_>>()
            .join(","),
        latest.map(|l| l.n.to_string()).unwrap_or_default()
    ))
}

#[route(SSE "/marks-feed", public, emits(Marked))]
fn marks_feed(tx: &mut Tx, ctx: &Ctx, cursor: Option<Cursor>) -> Result<Subscription, ApiError> {
    let _ = (tx, ctx);
    Ok(Subscription::of::<Marked>().cursor(cursor))
}

fn spawn_marks_node(sim: &Sim, host: &str, cfg: ReplConfig) -> RNode {
    let h = sim.host(host);
    let mut app = App::open(
        Box::new(h.disk()),
        Schema::new().table::<Mark>().table::<Latest>(),
        Box::new(h.entropy()),
        AppConfig {
            db: multi_node_db_cfg(),
            ..AppConfig::default()
        },
    )
    .unwrap()
    .mount(routes![put_mark, read_marks, marks_feed]);
    let (repl, _) = Repl::open(
        Box::new(h.net()),
        Box::new(h.disk()),
        cfg.clone(),
        app.db_for_tests(),
    )
    .unwrap();
    let kernel = Kernel::with_replication(app, Box::new(sim.clock()), repl).unwrap();
    let source = SimSource::new(Box::new(sim.clock()), RT_HORIZON);
    RNode {
        host: h,
        kernel: Some(kernel),
        responses: source.responses(),
        sinks: source.sinks(),
        source,
        name: host.to_string(),
        repl_cfg: cfg,
        db_cfg: multi_node_db_cfg(),
        recoveries: 0,
        flipped: false,
    }
}

fn last_applied(node: &RNode) -> Option<u64> {
    node.kernel.as_ref().unwrap().app().db().last_applied()
}

/// Drives both nodes until `node`'s response `index` is recorded, and returns it.
fn await_response(
    sim: &Sim,
    nodes: &mut [&mut RNode],
    node: usize,
    index: usize,
) -> (u16, Vec<u8>) {
    for _ in 0..RT_STEP_BUDGET {
        if let Some(r) = nodes[node].responses.lock().unwrap().get(index) {
            return r.clone();
        }
        assert!(
            rt_kernel_round(sim, nodes).unwrap(),
            "quiescent before response {index}"
        );
    }
    panic!("no response {index} within budget");
}

/// The order a client carries out the re-sync remedy in. The refusals and
/// terminal frames named it read-first until D4 round 2.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Remedy {
    /// As the text reads: the read, then the bare subscribe.
    ReadThenSubscribe,
    /// The bare subscribe, then (once its stream is open) the read.
    SubscribeThenRead,
}

/// What a client holds of the marks app: every mark it knows of, and the latest
/// change.
#[derive(Clone, Debug, Default, PartialEq)]
struct MarkState {
    marks: BTreeSet<u64>,
    latest: Option<u64>,
}

impl MarkState {
    /// `GET /marks`'s body: `1,2,3|3`.
    fn parse(body: &[u8]) -> MarkState {
        let text = String::from_utf8_lossy(body).trim_matches('"').to_string();
        let (marks, latest) = text.split_once('|').expect("marks|latest");
        MarkState {
            marks: marks
                .split(',')
                .filter(|s| !s.is_empty())
                .map(|s| s.parse().unwrap())
                .collect(),
            latest: (!latest.is_empty()).then(|| latest.parse().unwrap()),
        }
    }

    /// One `Marked` frame, applied idempotently: a mark is a set member, and
    /// the latest change is the frame's own.
    fn apply(&mut self, n: u64) {
        self.marks.insert(n);
        self.latest = Some(n);
    }
}

/// One run of the remedy: the changes by window, what the read returned, the
/// frames the tail delivered in stream order, and the node's own state once
/// everything has landed.
struct RemedyRun {
    by_window: [u64; 5],
    read: MarkState,
    frames: Vec<u64>,
    node: MarkState,
}

/// One client carrying out `remedy` against the follower (or the primary),
/// with one change landed in each of the five windows around its two
/// requests: before the first is sent, while it is in flight (sent, not yet
/// served), between its serve and the second's send, while the second is in
/// flight, and after the second is served. Changes are written on the
/// primary. On the follower a change "lands" when the shipped record has
/// applied there; on the primary, when the primary has committed it.
fn remedy_windows(remedy: Remedy, on_follower: bool) -> RemedyRun {
    const IN_FLIGHT: u64 = 100_000_000;
    let sim = Sim::with_faults(0x2EB2_D400, FaultConfig::none());
    let mut p = spawn_marks_node(&sim, "p", rt_primary_cfg("p:1"));
    let mut f = spawn_marks_node(&sim, "f", rt_follower_cfg("f:1", "p:1"));
    let target = if on_follower { 1 } else { 0 };
    let mut nodes = [&mut p, &mut f];
    let mark = |n: u64| Request::new(Method::Post, "/mark").json(&MarkBody { n: WireU64(n) });

    let first_is_subscribe = remedy == Remedy::SubscribeThenRead;
    let mut read = None;
    let mut handle = None;
    let mut by_window = [0u64; 5];

    // Waits until the primary has answered its next write and, on the
    // follower, until the shipped record has applied there.
    let mut p_responses = 0usize;
    let settle = |sim: &Sim, nodes: &mut [&mut RNode], p_responses: &mut usize| {
        let (status, _) = await_response(sim, nodes, 0, *p_responses);
        assert_eq!(status, 200, "a mark is written");
        *p_responses += 1;
        if on_follower {
            for _ in 0..RT_STEP_BUDGET {
                if last_applied(nodes[1]) == last_applied(nodes[0]) {
                    return;
                }
                assert!(rt_kernel_round(sim, nodes).unwrap());
            }
            panic!("the follower never applied the mark");
        }
    };
    // Window 0: before the first request is sent.
    nodes[0].source.push_at(sim.now().as_nanos(), mark(1));
    settle(&sim, &mut nodes, &mut p_responses);
    by_window[0] = 1;

    let mut target_responses = 0usize;
    for (step, is_subscribe) in [(0usize, first_is_subscribe), (1, !first_is_subscribe)] {
        let in_flight = 2 * step as u64 + 2;
        let after = in_flight + 1;
        let request = if is_subscribe {
            Request::new(Method::Get, "/marks-feed")
        } else {
            Request::new(Method::Get, "/marks")
        };
        let sent = sim.now().as_nanos();
        let arrives = sent + IN_FLIGHT;
        if on_follower {
            // Sent: the request is on its way to the follower, arriving later.
            nodes[1].source.push_at(arrives, request);
            nodes[0]
                .source
                .push_at(sim.now().as_nanos(), mark(in_flight));
            settle(&sim, &mut nodes, &mut p_responses);
            assert!(
                sim.now().as_nanos() < arrives,
                "the change must land while the request is in flight"
            );
        } else {
            // Sent: the change, written by another client, reaches the primary
            // first, so it commits before this request is served.
            nodes[0].source.push_at(arrives - 1, mark(in_flight));
            nodes[0].source.push_at(arrives, request);
            sim.clock().sleep(Duration::from_nanos(arrives - 1 - sent));
            settle(&sim, &mut nodes, &mut p_responses);
        }
        by_window[2 * step + 1] = in_flight;
        if sim.now().as_nanos() < arrives {
            sim.clock()
                .sleep(Duration::from_nanos(arrives - sim.now().as_nanos()));
        }
        let index = if on_follower {
            target_responses
        } else {
            p_responses
        };
        let (status, body) = await_response(&sim, &mut nodes, target, index);
        if on_follower {
            target_responses += 1;
        } else {
            p_responses += 1;
        }
        assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
        if is_subscribe {
            handle = Some(nodes[target].sinks.lock().unwrap().last().cloned().unwrap());
        } else {
            read = Some(MarkState::parse(&body));
        }
        // After this request is served (and before the next is sent).
        nodes[0].source.push_at(sim.now().as_nanos(), mark(after));
        settle(&sim, &mut nodes, &mut p_responses);
        by_window[2 * step + 2] = after;
    }

    // Let the tail deliver everything the target has.
    let handle = handle.unwrap();
    let head = nodes[target]
        .kernel
        .as_ref()
        .unwrap()
        .app()
        .db()
        .next_event_id()
        .unwrap();
    for _ in 0..RT_STEP_BUDGET {
        if handle.frames().last().is_some_and(|f| f.id + 1 == head) {
            break;
        }
        assert!(rt_kernel_round(&sim, &mut nodes).unwrap());
    }
    let frames = handle
        .frames()
        .iter()
        .map(|f| {
            let v: serde_json::Value = serde_json::from_str(&f.json).unwrap();
            v["n"].as_str().unwrap().parse::<u64>().unwrap()
        })
        .collect();
    nodes[target]
        .source
        .push_at(sim.now().as_nanos(), Request::new(Method::Get, "/marks"));
    let index = if on_follower {
        target_responses
    } else {
        p_responses
    };
    let (status, body) = await_response(&sim, &mut nodes, target, index);
    assert_eq!(status, 200);
    RemedyRun {
        by_window,
        read: read.unwrap(),
        frames,
        node: MarkState::parse(&body),
    }
}

/// A client that installs the read's result the moment it arrives and applies
/// each frame the moment it arrives.
fn install_on_arrival(read: &MarkState, frames: &[u64], read_after: usize) -> MarkState {
    let mut state = MarkState::default();
    for &n in &frames[..read_after] {
        state.apply(n);
    }
    state = read.clone();
    for &n in &frames[read_after..] {
        state.apply(n);
    }
    state
}

/// The remedy as the texts give it: hold every frame until the read is
/// installed, install it, then apply the held frames and every later one over
/// it in stream order. Where the read landed among the frames no longer
/// matters, which is the point.
fn hold_until_installed(read: &MarkState, frames: &[u64], _read_after: usize) -> MarkState {
    let mut state = read.clone();
    for &n in frames {
        state.apply(n);
    }
    state
}

/// The oracle the remedy answers to: whatever order the read's response and the
/// stream's frames reach the client in (each connection keeps its own order,
/// so the read can land after any prefix of the frames), the client ends
/// holding exactly the node's state. Names the first arrival order that does
/// not.
fn ends_at_the_nodes_state(
    run: &RemedyRun,
    client: fn(&MarkState, &[u64], usize) -> MarkState,
) -> Result<(), String> {
    for read_after in 0..=run.frames.len() {
        let state = client(&run.read, &run.frames, read_after);
        if state != run.node {
            return Err(format!(
                "with the read arriving after {read_after} of the frames {:?}, the client \
                 ends at {state:?}, not the node's {:?} (the read returned {:?})",
                run.frames, run.node, run.read
            ));
        }
    }
    Ok(())
}

/// The remedy every cursor refusal and terminal frame names, carried out
/// subscribe-first, misses nothing: with a change landed in each of the five
/// windows around its two requests, on a follower and on the primary, every
/// change reaches the client by the read, the tail, or both. The two that land
/// between the stream opening and the read being served arrive by both routes,
/// which is why the client applies frames idempotently, keyed by what the
/// change is (here the mark's own number).
#[test]
fn the_resync_remedy_subscribe_first_then_read_misses_no_change_in_any_window() {
    for on_follower in [true, false] {
        let run = remedy_windows(Remedy::SubscribeThenRead, on_follower);
        let (by_window, read) = (run.by_window, run.read.marks);
        let delivered: BTreeSet<u64> = run.frames.into_iter().collect();
        let seen: BTreeSet<u64> = read.union(&delivered).copied().collect();
        let missed: Vec<u64> = by_window
            .iter()
            .copied()
            .filter(|n| !seen.contains(n))
            .collect();
        assert!(
            missed.is_empty(),
            "follower {on_follower}: changes {missed:?} reached neither the read {read:?} nor \
             the tail {delivered:?}"
        );
        assert_eq!(
            read.intersection(&delivered).copied().collect::<Vec<_>>(),
            vec![by_window[2], by_window[3]],
            "follower {on_follower}: the changes between the stream opening and the read \
             arrive by both routes"
        );
    }
}

/// The same five windows with the remedy carried out in the order its text
/// reads, the read and then the bare subscribe: the change landing after the
/// read is served and the one landing while the subscribe is in flight reach
/// the client by neither route, on a follower and on the primary alike.
#[test]
fn the_resync_remedy_read_first_then_subscribe_misses_the_changes_between_its_steps() {
    for on_follower in [true, false] {
        let run = remedy_windows(Remedy::ReadThenSubscribe, on_follower);
        let (by_window, read) = (run.by_window, run.read.marks);
        let delivered: BTreeSet<u64> = run.frames.into_iter().collect();
        let seen: BTreeSet<u64> = read.union(&delivered).copied().collect();
        let missed: Vec<u64> = by_window
            .iter()
            .copied()
            .filter(|n| !seen.contains(n))
            .collect();
        assert_eq!(
            missed,
            vec![by_window[2], by_window[3]],
            "follower {on_follower}: read {read:?}, tail {delivered:?}"
        );
    }
}

/// The remedy judged by the state a client ends in, not by the sets of what
/// reached it (D4 round 3, review I2). Subscribe first, and a client that holds
/// every frame until its read is installed, then applies the held frames over
/// it in order, ends at the node's state for every order the read's response
/// and the frames can arrive in, on a follower and on the primary.
#[test]
fn the_resync_remedy_ends_at_the_nodes_state_when_frames_wait_for_the_read() {
    for on_follower in [true, false] {
        let run = remedy_windows(Remedy::SubscribeThenRead, on_follower);
        assert_eq!(
            run.node,
            MarkState {
                marks: run.by_window.iter().copied().collect(),
                latest: Some(run.by_window[4]),
            },
            "sanity: the node holds every change"
        );
        ends_at_the_nodes_state(&run, hold_until_installed)
            .unwrap_or_else(|e| panic!("follower {on_follower}: {e}"));
    }
}

/// The same oracle, failing where it must. A client that installs its read the
/// moment it arrives ends wrong whenever the frame of a change that landed after
/// the read was served reaches it first: that change is not in the read, the
/// install overwrites it, and nothing delivers it again. Holding frames does
/// not rescue the read-first order, which misses changes outright.
#[test]
fn installing_the_read_on_arrival_or_reading_first_ends_short_of_the_nodes_state() {
    for on_follower in [true, false] {
        let run = remedy_windows(Remedy::SubscribeThenRead, on_follower);
        let err = ends_at_the_nodes_state(&run, install_on_arrival)
            .expect_err("a client that installs the read on arrival must end wrong");
        assert!(
            err.starts_with(&format!(
                "with the read arriving after {} of the frames",
                run.frames.len()
            )),
            "follower {on_follower}: it ends wrong only once every frame, the last change's \
             included, beat the read: {err}"
        );
        let read_first = remedy_windows(Remedy::ReadThenSubscribe, on_follower);
        ends_at_the_nodes_state(&read_first, hold_until_installed)
            .expect_err("reading first misses changes no holding can recover");
    }
}

/// On a primary, "from now" is the subscribe's place in arrival order, even
/// for requests that arrive at one instant and could share a batch: a change
/// that arrived ahead of the subscribe is behind its tail, and one that
/// arrived behind it is delivered.
#[test]
fn a_bare_subscribe_on_a_primary_starts_between_the_changes_that_arrived_around_it() {
    let sim = Sim::with_faults(0x2EB2_D401, FaultConfig::none());
    let mut p = spawn_marks_node(&sim, "p", rt_primary_cfg("p:1"));
    let mut nodes = [&mut p];
    let mark = |n: u64| Request::new(Method::Post, "/mark").json(&MarkBody { n: WireU64(n) });
    let now = sim.now().as_nanos();
    nodes[0].source.push_at(now, mark(1));
    nodes[0]
        .source
        .push_at(now, Request::new(Method::Get, "/marks-feed"));
    nodes[0].source.push_at(now, mark(2));
    for index in 0..3 {
        let (status, body) = await_response(&sim, &mut nodes, 0, index);
        assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
    }
    let handle = nodes[0].sinks.lock().unwrap().last().cloned().unwrap();
    let until = sim.now().as_nanos() + SEC;
    while sim.now().as_nanos() < until && rt_kernel_round(&sim, &mut nodes).unwrap() {}
    let delivered: Vec<String> = handle.frames().iter().map(|f| f.json.clone()).collect();
    assert_eq!(
        delivered,
        vec![r#"{"n":"2"}"#.to_string()],
        "the change that arrived behind the subscribe is delivered, the one ahead of it is not"
    );
}

/// The instrument behind D5's LEDGER rates (not a test: `--ignored`). Runs
/// `SCAN_N` universes derived exactly as the swarm does (a splitmix64 chain
/// from `SCAN_BASE` (decimal, or hex with `0x`), then `FaultConfig::swarm` and `sample_extended_fields`),
/// with `SCAN_BF` (default unset) forcing `bit_flip`. Prints `SCAN ... failures=N`
/// and each failing seed. One process runs one chain; a set is 12 chains, bases
/// `B+0 .. B+11` (no threads: the deny-list forbids them in this crate's tests).
///
/// D5's rates; "before" neutralises the trim's guard first with
/// `sed -i '' 's/if self.last_id >= Some(head) {/if false {/' crates/charpente/tests/realtime.rs`
/// (restore with `git checkout`). Per set, from the repo root:
/// `for t in 0 1 2 3 4 5 6 7 8 9 10 11; do SCAN_BF=$BF SCAN_BASE=$((B+t)) SCAN_N=$N cargo test -p charpente --test realtime last_id_trim_rate_scan -- --ignored --nocapture 2>&1 | grep -E '^(SCAN|FAIL)' & done; wait`
/// summing the `failures=`; build once first so the 12 do not race the compiler.
/// - `BF=0.5 B=0x5D200000 N=5200` (62,400 universes): 26 before, 0 after.
/// - `BF=-1 B=0x5D000000 N=8500` (102,000): 0 before, 0 after.
/// - `BF=-1 B=0x5D100000 N=8500` (102,000): 1 before (`0xb62d99f8f90232c9`), 0 after.
#[test]
#[ignore]
fn last_id_trim_rate_scan() {
    let p = |k: &str, d: &str| std::env::var(k).unwrap_or(d.to_string());
    let raw = p("SCAN_BASE", "0");
    let base = match raw.strip_prefix("0x") {
        Some(hex) => u64::from_str_radix(hex, 16),
        None => raw.parse(),
    }
    .unwrap();
    let per: u64 = p("SCAN_N", "512").parse().unwrap();
    let bf: f64 = p("SCAN_BF", "-1").parse().unwrap();
    let (mut fails, mut state) = (0u64, base);
    for _ in 0..per {
        let seed = charpente_sim::rng::splitmix64(&mut state);
        let mut s = seed;
        let cfg_seed = charpente_sim::rng::splitmix64(&mut s);
        let mut cfg = FaultConfig::swarm(&mut charpente_sim::rng::Rng::new(cfg_seed));
        charpente_sim::sample_extended_fields(seed, &mut cfg);
        if bf >= 0.0 {
            cfg.bit_flip = bf;
        }
        let mut sim = Sim::with_faults(seed, cfg);
        let r =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| realtime_workload(&mut sim)));
        match r {
            Ok(Ok(_)) => {}
            Ok(Err(m)) => {
                fails += 1;
                eprintln!("FAIL {seed:#x}: {m}");
            }
            Err(_) => {
                fails += 1;
                eprintln!("FAIL {seed:#x}: PANIC");
            }
        }
    }
    eprintln!("SCAN base={base:#x} bf={bf} universes={per} failures={fails}");
}
