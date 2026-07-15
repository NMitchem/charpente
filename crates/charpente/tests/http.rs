//! The HTTP kernel under the sim (PLAN.md M0 step 5): routing, decoding,
//! sessions, error mapping — unit tests here; the macro end-to-end tests and
//! the request-level crash property are appended by later plan tasks.

use charpente::http::{
    ApiError, App, AppConfig, Ctx, Cursor, Handled, Method, Request, Response, Route, Subscription,
};
use charpente::{Schema, Tx, event, route, routes, table};
use charpente_core::table::Id;
use charpente_sim::{FaultConfig, Sim};
use serde::{Deserialize, Serialize};

#[table]
struct Note {
    id: Id<Note>,
    #[index]
    owner: u64,
    text: String,
}

#[derive(Serialize, Deserialize)]
struct CreateNote {
    id: u64,
    text: String,
}

#[route(POST "/notes")]
fn create_note(tx: &mut Tx, body: CreateNote, ctx: &Ctx) -> Result<u64, ApiError> {
    let owner = ctx
        .session()
        .and_then(|s| s.subject.parse().ok())
        .unwrap_or(0);
    tx.put(&Note {
        id: Id::from_raw(body.id),
        owner,
        text: body.text,
    });
    Ok(body.id)
}

#[route(GET "/notes/:id")]
fn get_note(tx: &mut Tx, id: Id<Note>) -> Result<String, ApiError> {
    use charpente_core::table::View;
    Ok(tx.get(id)?.ok_or(ApiError::NotFound)?.text)
}

#[route(POST "/login")]
fn login(tx: &mut Tx, subject: String, ctx: &Ctx) -> Result<String, ApiError> {
    Ok(ctx.create_session(tx, subject))
}

#[route(GET "/me", auth)]
fn whoami(tx: &mut Tx, ctx: &Ctx) -> Result<String, ApiError> {
    let _ = tx;
    Ok(ctx.session().expect("auth route").subject.clone())
}

#[route(POST "/logout", auth)]
fn logout(tx: &mut Tx, ctx: &Ctx) -> Result<&'static str, ApiError> {
    ctx.revoke(tx);
    Ok("bye")
}

fn note_routes() -> Vec<Route> {
    routes![create_note, get_note, login, whoami, logout]
}

fn quiet_app(seed: u64) -> (Sim, App) {
    let sim = Sim::with_faults(seed, FaultConfig::none());
    let app = App::open(
        Box::new(sim.disk()),
        Schema::new().table::<Note>(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )
    .unwrap()
    .mount(note_routes());
    (sim, app)
}

fn body_text(response: &Response) -> String {
    String::from_utf8(response.body.clone()).unwrap()
}

#[test]
fn create_then_get_roundtrip() {
    let (_sim, mut app) = quiet_app(0x477);
    let created = app.handle(Request::new(Method::Post, "/notes").json(&CreateNote {
        id: 7,
        text: "hi".into(),
    }));
    assert_eq!(created.status, 200, "{}", body_text(&created));

    let got = app.handle(Request::new(Method::Get, "/notes/7"));
    assert_eq!(got.status, 200);
    assert_eq!(body_text(&got), "\"hi\"");
    assert_eq!(
        app.db().last_applied(),
        Some(0),
        "one write txn, one record"
    );
}

#[test]
fn read_only_requests_consume_no_seq() {
    let (_sim, mut app) = quiet_app(0x478);
    app.handle(Request::new(Method::Post, "/notes").json(&CreateNote {
        id: 1,
        text: "x".into(),
    }));
    let before = app.db().last_applied();
    let missing = app.handle(Request::new(Method::Get, "/notes/99"));
    assert_eq!(missing.status, 404);
    let found = app.handle(Request::new(Method::Get, "/notes/1"));
    assert_eq!(found.status, 200);
    assert_eq!(app.db().last_applied(), before, "GETs appended nothing");
}

#[test]
fn unknown_path_404_wrong_method_405() {
    let (_sim, mut app) = quiet_app(0x479);
    assert_eq!(app.handle(Request::new(Method::Get, "/nope")).status, 404);
    assert_eq!(
        app.handle(Request::new(Method::Delete, "/notes")).status,
        405
    );
}

#[test]
fn decode_errors_are_named_400s() {
    let (_sim, mut app) = quiet_app(0x47A);
    let bad_param = app.handle(Request::new(Method::Get, "/notes/abc"));
    assert_eq!(bad_param.status, 400);
    assert!(
        body_text(&bad_param).contains(":id"),
        "{}",
        body_text(&bad_param)
    );

    let mut no_body = Request::new(Method::Post, "/notes");
    no_body.body = Vec::new();
    let resp = app.handle(no_body);
    assert_eq!(resp.status, 400);
    assert!(
        body_text(&resp).contains("CreateNote"),
        "{}",
        body_text(&resp)
    );
    assert_eq!(
        app.db().last_applied(),
        None,
        "failed decodes write nothing"
    );
}

#[test]
fn body_cap_is_a_413_before_decoding() {
    let sim = Sim::with_faults(0x47B, FaultConfig::none());
    let mut app = App::open(
        Box::new(sim.disk()),
        Schema::new().table::<Note>(),
        Box::new(sim.entropy()),
        AppConfig {
            body_cap: 64,
            ..AppConfig::default()
        },
    )
    .unwrap()
    .mount(note_routes());
    let mut big = Request::new(Method::Post, "/notes");
    big.body = vec![b'x'; 65];
    let resp = app.handle(big);
    assert_eq!(resp.status, 413);
    assert!(
        body_text(&resp).contains("body_cap"),
        "{}",
        body_text(&resp)
    );
}

/// repl T3: `TxError::NotPrimary` maps to 421, naming the primary; reads are
/// unaffected by the gate.
#[test]
fn writes_on_a_follower_get_421_naming_the_primary() {
    let (_sim, mut app) = quiet_app(0x9012_0005);
    let created = app.handle(Request::new(Method::Post, "/notes").json(&CreateNote {
        id: 1,
        text: "hi".into(),
    }));
    assert_eq!(created.status, 200, "{}", body_text(&created));

    app.db_for_tests().forbid_writes("primary-1:9000".into());

    let resp = app.handle(Request::new(Method::Post, "/notes").json(&CreateNote {
        id: 2,
        text: "bye".into(),
    }));
    assert_eq!(resp.status, 421, "{}", body_text(&resp));
    let body = body_text(&resp);
    assert!(body.contains("primary-1:9000"), "{body}");
    assert!(body.contains("follower"), "{body}");

    // A read route still answers 200 — the write gate never touches reads.
    let get = app.handle(Request::new(Method::Get, "/notes/1"));
    assert_eq!(get.status, 200);
    assert_eq!(body_text(&get), "\"hi\"");
}

#[test]
fn sessions_login_auth_logout() {
    let (_sim, mut app) = quiet_app(0x47C);

    // No session: auth routes are 401 with instructions.
    let denied = app.handle(Request::new(Method::Get, "/me"));
    assert_eq!(denied.status, 401);
    assert!(
        body_text(&denied).contains("Authorization: Bearer"),
        "{}",
        body_text(&denied)
    );

    // Login mints a token.
    let login = app.handle(Request::new(Method::Post, "/login").json(&"user-42"));
    assert_eq!(login.status, 200);
    let token: String = serde_json::from_slice(&login.body).unwrap();
    assert_eq!(token.len(), 64, "32 entropy bytes hex-encoded");

    // Bearer works; so does the cookie.
    let me = app.handle(
        Request::new(Method::Get, "/me").header("Authorization", format!("Bearer {token}")),
    );
    assert_eq!(me.status, 200);
    assert_eq!(body_text(&me), "\"user-42\"");
    let me = app.handle(
        Request::new(Method::Get, "/me")
            .header("Cookie", format!("theme=dark; charpente_session={token}")),
    );
    assert_eq!(me.status, 200);

    // A bad token is 401; logout revokes; the old token stops working.
    let bad = app.handle(Request::new(Method::Get, "/me").header("Authorization", "Bearer nope"));
    assert_eq!(bad.status, 401);
    let out = app.handle(
        Request::new(Method::Post, "/logout").header("Authorization", format!("Bearer {token}")),
    );
    assert_eq!(out.status, 200);
    let after = app.handle(
        Request::new(Method::Get, "/me").header("Authorization", format!("Bearer {token}")),
    );
    assert_eq!(after.status, 401, "revoked session must not authenticate");
}

#[test]
fn sessions_survive_recovery() {
    let sim = Sim::with_faults(0x47D, FaultConfig::none());
    let mut app = App::open(
        Box::new(sim.disk()),
        Schema::new().table::<Note>(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )
    .unwrap()
    .mount(note_routes());
    let login = app.handle(Request::new(Method::Post, "/login").json(&"user-9"));
    let token: String = serde_json::from_slice(&login.body).unwrap();

    sim.restart(); // crash with everything open; the WAL replays the login
    drop(app);
    let mut app = App::open(
        Box::new(sim.disk()),
        Schema::new().table::<Note>(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )
    .unwrap()
    .mount(note_routes());
    let me = app.handle(
        Request::new(Method::Get, "/me").header("Authorization", format!("Bearer {token}")),
    );
    assert_eq!(me.status, 200, "the session row is inside the commit point");
}

#[test]
fn handler_api_errors_map_to_statuses() {
    let (_sim, mut app) = quiet_app(0x47E);
    let resp = app.handle(Request::new(Method::Get, "/notes/12345"));
    assert_eq!(resp.status, 404, "handler NotFound becomes 404");
}

#[test]
fn routes_macro_collects_descriptors() {
    let all = note_routes();
    assert_eq!(all.len(), 5);
    assert!(all.iter().any(|r| r.pattern == "/me" && r.auth));
    assert!(all.iter().any(|r| r.pattern == "/notes/:id" && !r.auth));
}

// ---------------------------------------------------- realtime (T4)

#[event]
struct NoteAdded {
    note: u64,
}

#[route(SSE "/feed")]
fn feed(tx: &mut Tx, ctx: &Ctx, cursor: Option<Cursor>) -> Result<Subscription, ApiError> {
    let _ = (tx, ctx);
    Ok(Subscription::of::<NoteAdded>().cursor(cursor))
}

fn sse_app(seed: u64) -> (Sim, App) {
    let sim = Sim::with_faults(seed, FaultConfig::none());
    let app = App::open(
        Box::new(sim.disk()),
        Schema::new(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )
    .unwrap()
    .mount(routes![feed]);
    (sim, app)
}

#[test]
fn cursor_parses_wire_form_and_rejects_garbage() {
    assert_eq!(Cursor::parse("12.34"), Some(Cursor { id: 12, seq: 34 }));
    assert_eq!(Cursor::parse(""), None);
    assert_eq!(Cursor::parse("12"), None);
    assert_eq!(Cursor::parse("12.x"), None);
}

#[test]
fn sse_route_reaches_handler_with_extracted_cursor() {
    let (_sim, mut app) = sse_app(0x9012_0006);

    // The `Last-Event-ID` header wins.
    let handled =
        app.handle_or_subscribe(Request::new(Method::Get, "/feed").header("last-event-id", "5.9"));
    let Handled::Subscribe(sub) = handled else {
        panic!("expected Subscribe from an SSE route")
    };
    assert_eq!(sub.cursor, Some(Cursor { id: 5, seq: 9 }));

    // Same result from the `cursor` query parameter with no header.
    let handled = app.handle_or_subscribe(Request::new(Method::Get, "/feed?cursor=5.9"));
    let Handled::Subscribe(sub) = handled else {
        panic!("expected Subscribe from an SSE route")
    };
    assert_eq!(sub.cursor, Some(Cursor { id: 5, seq: 9 }));

    // An unparseable cursor is a 400 naming the contract — the handler never runs.
    let handled = app
        .handle_or_subscribe(Request::new(Method::Get, "/feed").header("last-event-id", "garbage"));
    let Handled::Response(resp) = handled else {
        panic!("expected Response for an unparseable cursor")
    };
    assert_eq!(resp.status, 400);
    assert!(
        body_text(&resp).contains("cursor must be"),
        "{}",
        body_text(&resp)
    );
}

// ---------------------------------------------------- crash property

use std::io;

use charpente::{Db, Job};
use charpente_core::io::Entropy;
use charpente_core::job::JobRow;
use charpente_core::log::WalConfig;
use charpente_core::table::{Codec, View};
use charpente_sim::run_universes_swarm;

#[table]
struct Item {
    id: Id<Item>,
    content: u64,
}

#[derive(Serialize, Deserialize)]
struct PutItem {
    i: u64,
}

struct Ping {
    i: u64,
}

impl Codec for Ping {
    fn encode(&self, out: &mut Vec<u8>) {
        self.i.encode(out);
    }
    fn decode(input: &mut &[u8]) -> io::Result<Ping> {
        Ok(Ping {
            i: u64::decode(input)?,
        })
    }
}

impl Job for Ping {
    const NAME: &'static str = "ping";
}

#[route(POST "/items")]
fn put_item(tx: &mut Tx, body: PutItem) -> Result<u64, ApiError> {
    tx.put(&Item {
        id: Id::from_raw(body.i),
        content: body.i * 7,
    });
    tx.enqueue(Ping { i: body.i });
    Ok(body.i)
}

fn crash_app_config() -> AppConfig {
    AppConfig {
        body_cap: 1 << 20,
        db: charpente::DbConfig {
            wal: WalConfig {
                dir: "wal".to_string(),
                segment_bytes: 4096,
            },
            store_path: "tables.redb".to_string(),
            ..charpente::DbConfig::default()
        },
    }
}

fn open_crash_app(sim: &Sim) -> io::Result<App> {
    Ok(App::open(
        Box::new(sim.disk()),
        Schema::new().table::<Item>(),
        Box::new(sim.entropy()),
        crash_app_config(),
    )?
    .mount(routes![put_item]))
}

/// Requests 0..n each wrote Item{i, 7i} and one ping job — verify exactly.
fn http_state_matches(db: &Db, n: u64) -> Result<(), String> {
    let snap = db.snapshot().map_err(|e| format!("snapshot: {e}"))?;
    let items = snap.scan::<Item>().map_err(|e| format!("scan: {e}"))?;
    if items.len() as u64 != n {
        return Err(format!("{n} requests acked but {} items", items.len()));
    }
    for (i, item) in items.iter().enumerate() {
        if (item.id.raw(), item.content) != (i as u64, i as u64 * 7) {
            return Err(format!(
                "item {i} diverges: {:?}",
                (item.id.raw(), item.content)
            ));
        }
    }
    let jobs = snap.scan::<JobRow>().map_err(|e| format!("jobs: {e}"))?;
    if jobs.len() as u64 != n {
        return Err(format!(
            "dual-write torn over HTTP: {n} acked but {} jobs",
            jobs.len()
        ));
    }
    for (i, job) in jobs.iter().enumerate() {
        let ping =
            Ping::decode(&mut job.payload.as_slice()).map_err(|e| format!("job decode: {e}"))?;
        if (job.id.raw(), job.name.as_str(), ping.i) != (i as u64, "ping", i as u64) {
            return Err(format!(
                "job {i} diverges: id {} {} i={}",
                job.id.raw(),
                job.name,
                ping.i
            ));
        }
    }
    Ok(())
}

// Two families suffice here (vs kernel.rs's three): this workload breaks and
// restarts on the FIRST Io error, so no second op ever reaches a poisoned WAL
// or redb's latched handle — their wordings cannot surface. The reopen-time
// state checks remain the oracle either way. The WAL's durable-horizon hard
// error (segment-mgmt T4/T5) is recognized too, for the same reason as
// transactions.rs/kernel.rs: a legitimate fault-matrix consequence, not a bug.
fn is_injected(text: &str) -> bool {
    text.contains("simulated") || text.contains("injected") || text.contains("durable horizon")
}

/// One universe: recover → verify → requests through App::handle (an acked
/// 200 must survive anything) → crash. A 500 whose body is not an injected
/// fault is a real bug and fails the universe. Returns every (status, body)
/// observed, in order, so the determinism test can compare the full response
/// trace across replays of the same seed, not just the durable fingerprint.
fn http_workload_traced(sim: &mut Sim) -> Result<Vec<(u16, Vec<u8>)>, String> {
    let entropy = sim.entropy();
    let mut acked = 0u64;
    let mut responses = Vec::new();

    for _round in 0..3 {
        sim.crash_disk_after(64 + entropy.next_u64() % 512);
        let mut app = match open_crash_app(sim) {
            Ok(app) => app,
            Err(e) => {
                if !is_injected(&e.to_string()) {
                    return Err(format!("recovery failed with a non-injected error: {e}"));
                }
                sim.restart();
                continue;
            }
        };
        let recovered = app.db().last_applied().map_or(0, |s| s + 1);
        if recovered < acked {
            // Excused ONLY when the harness actually injected durable
            // corruption in this universe: bit_flip/misdirected_write on a
            // WAL file above the checkpoint horizon is silently cut at
            // recovery, by the WAL convention (see
            // `checks::durable_corruption_fired`); the strong oracle stands
            // in every universe where corruption did not fire.
            if charpente_sim::checks::durable_corruption_fired(sim) {
                return Ok(responses);
            }
            return Err(format!(
                "durability violated: {acked} acked over HTTP, {recovered} recovered"
            ));
        }
        acked = recovered;
        // `http_state_matches`'s scan/index calls are disk reads too — an
        // injected `read_error` can trip one even though the snapshot
        // itself opened fine, so its result needs the same
        // classify-and-reopen treatment as everywhere else (not the bare
        // `?` this used to be).
        if let Err(msg) = http_state_matches(app.db(), acked) {
            if !is_injected(&msg) {
                return Err(msg);
            }
            sim.restart();
            continue;
        }

        for _ in 0..1 + entropy.next_u64() % 8 {
            // Sprinkle guaranteed-no-write requests: they must not consume a seq.
            if entropy.next_u64().is_multiple_of(5) {
                let before = app.db().last_applied();
                let resp = app.handle(Request::new(Method::Get, "/items"));
                responses.push((resp.status, resp.body.clone()));
                if resp.status != 404 && resp.status != 405 {
                    return Err(format!(
                        "GET /items must be unroutable, got {}",
                        resp.status
                    ));
                }
                if app.db().last_applied() != before {
                    return Err("an unrouted request consumed a seq".to_string());
                }
            }
            let resp = app.handle(Request::new(Method::Post, "/items").json(&PutItem { i: acked }));
            responses.push((resp.status, resp.body.clone()));
            match resp.status {
                200 => acked += 1,
                500 => {
                    let text = String::from_utf8_lossy(&resp.body).to_string();
                    if !is_injected(&text) {
                        return Err(format!("non-injected 500: {text}"));
                    }
                    break; // died mid-commit; durable or not, next round decides
                }
                other => return Err(format!("unexpected status {other}")),
            }
            if entropy.next_u64().is_multiple_of(4)
                && let Err(e) = app.db_for_tests().checkpoint()
            {
                if !is_injected(&e.to_string()) {
                    return Err(format!("non-injected checkpoint failure: {e}"));
                }
                break;
            }
        }
        sim.restart();
    }

    // Final verification (step-4 convention): injected-fault failures skip,
    // anything else is real.
    for _attempt in 0..8 {
        let app = match open_crash_app(sim) {
            Ok(app) => app,
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
        let recovered = app.db().last_applied().map_or(0, |s| s + 1);
        if recovered < acked {
            // Same excuse-only-if-injected routing as the round loop above.
            if charpente_sim::checks::durable_corruption_fired(sim) {
                return Ok(responses);
            }
            return Err(format!(
                "durability violated: {acked} acked over HTTP, {recovered} recovered"
            ));
        }
        match http_state_matches(app.db(), recovered) {
            Ok(()) => return Ok(responses),
            Err(msg) if is_injected(&msg) => {
                sim.restart();
                continue;
            }
            Err(msg) => return Err(msg),
        }
    }
    Ok(responses) // every attempt died to injected faults; earlier rounds verified every reachable prefix
}

fn http_workload(sim: &mut Sim) -> Result<(), String> {
    http_workload_traced(sim).map(|_| ())
}

#[test]
fn http_requests_survive_the_fault_matrix() {
    run_universes_swarm!(
        256,
        http_workload,
        require: [
            "fault:write_error",
            "fault:sync_error",
            "fault:crash",
            "crash:applied",
            "crash:torn",
            "crash:dropped",
            "restart",
        ]
    );
}

#[test]
fn http_replays_identically_for_the_same_seed() {
    let trace = |seed: u64| {
        let mut sim = Sim::new(seed);
        let result = http_workload_traced(&mut sim);
        let responses = result.unwrap_or_else(|e| panic!("seed {seed:#x}: {e}"));
        (responses, sim.durable_fingerprint(), sim.now())
    };
    for seed in [0x51, 0xFACE_0FF5, 0x1DEA_C0DE_1DEA_C0DE] {
        assert_eq!(trace(seed), trace(seed));
    }
}
