//! Retry-safe writes, the request path (spec
//! docs/superpowers/specs/2026-11-22-retry-safe-writes-design.md §4.1–§4.3):
//! a write that carries an intent id is looked up in `charpente.intents`
//! inside its own staged transaction, a repeat is answered from the record
//! without running, and a fresh write's reply is recorded in the same
//! transaction as its effects. The kernel's sweep (§4.4) bounds the record:
//! a row lives `INTENT_RETENTION` past the later of its commit and the
//! node's latest open, a process open or a promotion.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use charpente::auth::hash_token;
use charpente::core::intent::{INTENT_RETENTION, IntentReply, IntentRow};
use charpente::http::{ApiError, App, AppConfig, Ctx, Handled, Method, Request, Response, Route};
use charpente::kernel::{INTENT_SWEEP_INTERVAL, Kernel, RequestSource, SimSource, Step, Wake};
use charpente::repl::protocol::{Msg, decode, encode};
use charpente::repl::role::Role;
use charpente::repl::{MAX_MESSAGE_BYTES, Repl, ReplConfig};
use charpente::wire::WireU64;
use charpente::{Schema, Tx, View, route, routes, table};
use charpente_core::io::{Clock, Disk, Entropy, Instant, Net, WallTime};
use charpente_core::table::Id;
use charpente_sim::checks::{
    boot_through_fallout, clock_fallout, clock_reopens_after, is_fault_fallout,
};
use charpente_sim::rng::{Rng, splitmix64};
use charpente_sim::{FaultConfig, Sim, run_universes_swarm_with};

const SEC: u64 = 1_000_000_000;
const MIN: u64 = 60 * SEC;
const HOUR: u64 = 60 * MIN;
/// `INTENT_RETENTION`, in nanoseconds.
const DAY: u64 = INTENT_RETENTION.as_nanos() as u64;

// ------------------------------------------------------------------ app

#[table(global)]
struct Hit {
    id: Id<Hit>,
    tag: u64,
    note: String,
}

/// Puts one new `Hit` (its id is the count so far plus one, so every run
/// that applies adds a row) and answers the count of all hits.
fn put_hit(tx: &mut Tx, tag: u64, note: String) -> Result<u64, ApiError> {
    let n = tx.scan::<Hit>()?.len() as u64 + 1;
    tx.put(&Hit {
        id: Id::from_raw(n),
        tag,
        note,
    });
    Ok(n)
}

#[route(POST "/hits/:id", public)]
fn add_hit(tx: &mut Tx, id: Id<Hit>, note: String) -> Result<WireU64, ApiError> {
    Ok(WireU64(put_hit(tx, id.raw(), note)?))
}

/// A write route whose handler reads and writes nothing.
#[route(POST "/noop", public)]
fn noop(tx: &mut Tx) -> Result<WireU64, ApiError> {
    Ok(WireU64(tx.scan::<Hit>()?.len() as u64))
}

/// Mints a session; the raw token is the reply, the one place it exists.
#[route(POST "/login", public)]
fn login(tx: &mut Tx, subject: String, ctx: &Ctx) -> Result<String, ApiError> {
    Ok(ctx.create_session(tx, subject))
}

/// Mints an API key for the subject the body names: test setup only.
#[route(POST "/keys", public)]
fn mint_key(tx: &mut Tx, subject: String, ctx: &Ctx) -> Result<String, ApiError> {
    Ok(ctx.create_api_key(tx, subject))
}

/// The JSON string of this many `x`s is `BIG_BODY` bytes long.
const BIG_BODY: usize = 70_000;

/// Writes, and answers a reply too large for a record to keep.
#[route(POST "/big", public)]
fn big(tx: &mut Tx) -> Result<String, ApiError> {
    put_hit(tx, 0, "big".to_string())?;
    Ok("x".repeat(BIG_BODY - 2))
}

/// Writes, then refuses: the stage drops the write.
#[route(POST "/fail", public)]
fn fail(tx: &mut Tx) -> Result<WireU64, ApiError> {
    put_hit(tx, 0, "fail".to_string())?;
    Err(ApiError::BadRequest("refused after a write".to_string()))
}

/// Puts one new `Hit` stamped with its transaction's time, so a check can
/// read from the store alone when each write committed.
#[route(POST "/stamped", public)]
fn add_stamped(tx: &mut Tx, note: String) -> Result<WireU64, ApiError> {
    let at = tx.now().as_nanos();
    Ok(WireU64(put_hit(tx, at, note)?))
}

#[route(GET "/hits", public)]
fn count_hits(tx: &mut Tx) -> Result<WireU64, ApiError> {
    Ok(WireU64(tx.scan::<Hit>()?.len() as u64))
}

/// A GET handler that writes: outside the contract (spec §2), so a re-send
/// runs again whatever id it carries.
#[route(GET "/touch", public)]
fn touch(tx: &mut Tx) -> Result<WireU64, ApiError> {
    Ok(WireU64(put_hit(tx, 0, "touch".to_string())?))
}

#[route(POST "/mine/:id", auth)]
fn add_mine(tx: &mut Tx, id: Id<Hit>, ctx: &Ctx) -> Result<String, ApiError> {
    let subject = ctx.principal().expect("auth route").subject().to_string();
    let n = put_hit(tx, id.raw(), subject.clone())?;
    Ok(format!("{subject}:{n}"))
}

fn intent_routes() -> Vec<Route> {
    routes![
        add_hit,
        add_stamped,
        noop,
        login,
        mint_key,
        big,
        fail,
        count_hits,
        touch,
        add_mine
    ]
}

fn intent_app(seed: u64) -> (Sim, App) {
    let sim = Sim::with_faults(seed, FaultConfig::none());
    let app = open_app(Box::new(sim.disk()), Box::new(sim.entropy()));
    (sim, app)
}

fn open_app(disk: Box<dyn Disk>, entropy: Box<dyn Entropy>) -> App {
    try_open_app(disk, entropy).unwrap()
}

fn try_open_app(disk: Box<dyn Disk>, entropy: Box<dyn Entropy>) -> std::io::Result<App> {
    Ok(App::open(
        disk,
        Schema::new().table::<Hit>(),
        entropy,
        AppConfig::default(),
    )?
    .mount(intent_routes()))
}

fn hits(app: &App) -> Vec<Hit> {
    app.db().snapshot().unwrap().scan::<Hit>().unwrap()
}

fn intents(app: &App) -> Vec<IntentRow> {
    app.db().snapshot().unwrap().scan::<IntentRow>().unwrap()
}

fn hit(id: u64, note: &str) -> Request {
    Request::new(Method::Post, format!("/hits/{id}")).json(&note)
}

fn body(response: &Response) -> String {
    String::from_utf8_lossy(&response.body).into_owned()
}

fn replayed(response: &Response) -> Option<&str> {
    response
        .headers
        .get("idempotent-replayed")
        .map(String::as_str)
}

fn response(handled: Handled) -> Response {
    match handled {
        Handled::Response(response) => response,
        Handled::Subscribe(_) => panic!("a write route answers a Response"),
    }
}

/// The 400 every malformed id on `POST /hits/:id` gets (Global Constraints'
/// exact copy, in its JSON envelope).
const MALFORMED_HITS: &str = "{\"error\":\"`POST /hits/:id` needs an Idempotency-Key header, \
     1 to 255 visible ASCII characters: charpente answers a re-sent write once (GUIDE §5). The \
     generated client sends one; a webhook route names its sender's id header with `intent = \
     header(...)`.\"}";

// ---------------------------------------------------------------- tests

/// The contract (spec §2): a re-sent write that carries the same intent id
/// never applies twice, and its client gets the first reply back, marked.
#[test]
fn a_resent_write_answers_its_first_reply_and_applies_once() {
    let (_sim, mut app) = intent_app(0x1D7E_3001);
    let t = WallTime::from_nanos(5 * SEC);
    let first = app.handle_at(hit(1, "a").intent("k1"), t);
    let second = app.handle_at(hit(1, "a").intent("k1"), t);

    assert_eq!(
        hits(&app).len(),
        1,
        "a re-sent write under one intent id applied twice: the first answered {} {}, the \
         second {} {}",
        first.status,
        body(&first),
        second.status,
        body(&second)
    );
    assert_eq!((second.status, &second.body), (first.status, &first.body));
    assert_eq!(
        replayed(&second),
        Some("true"),
        "a replayed reply is marked"
    );
    assert_eq!(
        replayed(&first),
        None,
        "the first reply ran, so it is not marked"
    );
    assert_eq!(
        second.headers.get("content-type").map(String::as_str),
        Some("application/json"),
        "the replay keeps the first reply's headers"
    );
    let rows = intents(&app);
    assert_eq!(rows.len(), 1, "one intent, one record");
    assert_eq!(
        rows[0].committed_at, t,
        "the record carries the step's time"
    );
}

/// The same id on another request is not a repeat: the client reused an id,
/// and is told so rather than answered with another request's reply.
#[test]
fn the_same_id_with_another_body_is_refused_with_422() {
    let (_sim, mut app) = intent_app(0x1D7E_3002);
    let t = WallTime::from_nanos(5 * SEC);
    let first = app.handle_at(hit(1, "a").intent("k1"), t);
    assert_eq!(first.status, 200, "{}", body(&first));
    let other = app.handle_at(hit(1, "b").intent("k1"), t);
    assert_eq!(other.status, 422, "{}", body(&other));
    assert_eq!(
        body(&other),
        "{\"error\":\"Idempotency-Key `k1` was already used for a different request to \
         `POST /hits/:id`: send this request under a new Idempotency-Key.\"}"
    );
    assert_eq!(replayed(&other), None, "a refusal is not a replay");
    assert_eq!(hits(&app).len(), 1, "the refused request did not run");
    assert_eq!(hits(&app)[0].note, "a");
}

/// A write that changed nothing has nothing to repeat (spec §4.2): it runs
/// each time, and records nothing.
#[test]
fn a_write_that_changed_nothing_records_nothing() {
    let (_sim, mut app) = intent_app(0x1D7E_3003);
    let t = WallTime::from_nanos(5 * SEC);
    for _ in 0..2 {
        let reply = app.handle_at(Request::new(Method::Post, "/noop").intent("k1"), t);
        assert_eq!((reply.status, body(&reply)), (200, "\"0\"".to_string()));
        assert_eq!(
            replayed(&reply),
            None,
            "nothing was recorded, so nothing replays"
        );
    }
    assert_eq!(
        intents(&app).len(),
        0,
        "a write that changed nothing records nothing"
    );
}

/// A handler's `Err` drops every effect of its stage, the record included,
/// so a re-send runs again.
#[test]
fn a_failed_write_records_nothing_and_its_resend_runs_again() {
    let (_sim, mut app) = intent_app(0x1D7E_3004);
    let t = WallTime::from_nanos(5 * SEC);
    for _ in 0..2 {
        let reply = app.handle_at(Request::new(Method::Post, "/fail").intent("k1"), t);
        assert_eq!(
            (reply.status, body(&reply)),
            (400, "{\"error\":\"refused after a write\"}".to_string()),
            "the app's own answer, each time"
        );
        assert_eq!(replayed(&reply), None);
    }
    assert_eq!(intents(&app).len(), 0);
    assert_eq!(hits(&app).len(), 0, "the failed write's put was dropped");
}

/// A reply that carried a credential is withheld (spec §4.3): the raw token
/// lives only in the first reply, never in the record, and a re-send is
/// told to send a new request.
#[test]
fn a_resent_login_is_refused_and_its_token_never_reaches_disk() {
    let (sim, mut app) = intent_app(0x1D7E_3005);
    let t = WallTime::from_nanos(5 * SEC);
    let login = || {
        Request::new(Method::Post, "/login")
            .json(&"alice")
            .intent("login-1")
    };
    let first = app.handle_at(login(), t);
    assert_eq!(first.status, 200, "{}", body(&first));
    let token: String = serde_json::from_slice(&first.body).unwrap();
    let second = app.handle_at(login(), t);
    assert_eq!(second.status, 409, "{}", body(&second));
    assert_eq!(
        body(&second),
        "{\"error\":\"this request already committed under Idempotency-Key `login-1`, and its \
         reply carried a credential, which charpente shows once: send a new request under a \
         new Idempotency-Key to get a new one.\"}"
    );
    assert_eq!(replayed(&second), None);

    let rows = intents(&app);
    assert_eq!(rows.len(), 1, "the login was recorded");
    assert_eq!(rows[0].reply, IntentReply::Credential);
    assert_eq!(rows[0].status, 200);

    // Every row of every table, raw: the token is in none of them, and its
    // hash is in one (the scan read the session it minted).
    let db = app.db();
    let snapshot = db.snapshot().unwrap();
    let hashed = hash_token(&token);
    let mut found_hash = false;
    for table in db.table_names() {
        let scoped = db.table_scoped(table).unwrap();
        for (_, _, bytes) in snapshot.scan_raw(table, scoped).unwrap() {
            assert!(
                !bytes.windows(token.len()).any(|w| w == token.as_bytes()),
                "the raw token reached a row of {table}"
            );
            found_hash |= bytes.windows(hashed.len()).any(|w| w == hashed.as_bytes());
        }
    }
    assert!(
        found_hash,
        "the walk read no session row, so it proved nothing"
    );
    // And every durable file, the log included.
    let disk = sim.disk();
    for name in disk.list("").unwrap() {
        let file = disk.open(&name).unwrap();
        let mut bytes = vec![0u8; file.len().unwrap() as usize];
        if !bytes.is_empty() {
            file.read_at(0, &mut bytes).unwrap();
        }
        assert!(
            !bytes.windows(token.len()).any(|w| w == token.as_bytes()),
            "the raw token reached the durable file {name}"
        );
    }
}

/// A reply over 64 KiB is answered in full the first time and withheld from
/// the record (spec §4.3): a re-send is told its size and to read with a GET.
#[test]
fn a_reply_over_64_kib_is_answered_409_on_a_resend() {
    let (_sim, mut app) = intent_app(0x1D7E_3006);
    let t = WallTime::from_nanos(5 * SEC);
    let first = app.handle_at(Request::new(Method::Post, "/big").intent("big-1"), t);
    assert_eq!(first.status, 200);
    assert_eq!(first.body.len(), BIG_BODY, "the first reply is whole");
    let second = app.handle_at(Request::new(Method::Post, "/big").intent("big-1"), t);
    // 70_000 body bytes plus `content-type: application/json`, 28.
    assert_eq!(second.status, 409, "{}", body(&second));
    assert_eq!(
        body(&second),
        "{\"error\":\"this request already committed under Idempotency-Key `big-1`, and its \
         reply was 70028 bytes, over the 65536 charpente keeps: read the result with a GET.\"}"
    );
    assert_eq!(hits(&app).len(), 1, "the re-send did not run");
    assert_eq!(
        intents(&app)[0].reply,
        IntentReply::TooLarge { bytes: 70_028 }
    );
}

fn bearer(request: Request, key: &str) -> Request {
    request.header("Authorization", format!("Bearer {key}"))
}

/// The record's key includes the principal (spec §4.2), so two callers who
/// happen to pick the same id are two intents.
#[test]
fn one_intent_id_under_two_principals_is_two_intents() {
    let (_sim, mut app) = intent_app(0x1D7E_3007);
    let t = WallTime::from_nanos(5 * SEC);
    let mut key = |subject: &str| {
        let reply = app.handle_at(Request::new(Method::Post, "/keys").json(&subject), t);
        assert_eq!(reply.status, 200, "{}", body(&reply));
        serde_json::from_slice::<String>(&reply.body).unwrap()
    };
    let (alice, bob) = (key("alice"), key("bob"));
    let mine = |key: &str| bearer(Request::new(Method::Post, "/mine/1").intent("same"), key);

    let a = app.handle_at(mine(&alice), t);
    let b = app.handle_at(mine(&bob), t);
    assert_eq!((a.status, body(&a)), (200, "\"alice:1\"".to_string()));
    assert_eq!((b.status, body(&b)), (200, "\"bob:2\"".to_string()));
    assert_eq!((replayed(&a), replayed(&b)), (None, None));
    assert_eq!(hits(&app).len(), 2, "both applied");
    let mut principals: Vec<_> = intents(&app)
        .into_iter()
        .map(|row| row.principal.unwrap())
        .collect();
    principals.sort();
    assert_eq!(principals, ["alice", "bob"]);

    let again = app.handle_at(mine(&alice), t);
    assert_eq!((again.status, &again.body), (200, &a.body));
    assert_eq!(
        replayed(&again),
        Some("true"),
        "each principal's own repeat replays"
    );
    assert_eq!(hits(&app).len(), 2);
}

/// Two copies of one intent staged in one group (spec §3): the second reads
/// the first's record through the group overlay and is answered from it.
#[test]
fn a_repeat_in_the_same_group_answers_from_its_siblings_record() {
    let (_sim, mut app) = intent_app(0x1D7E_3008);
    let t = WallTime::from_nanos(5 * SEC);
    let (mut g, staged) = app.begin_group(t).unwrap();
    let first = response(staged.stage_or_subscribe(&mut g, hit(1, "a").intent("k1"), t));
    assert_eq!(g.len(), 1, "the first copy staged its record");
    let second = response(staged.stage_or_subscribe(&mut g, hit(1, "a").intent("k1"), t));
    assert_eq!(g.len(), 1, "the repeat staged nothing");
    g.commit().unwrap();

    assert_eq!(first.status, 200, "{}", body(&first));
    assert_eq!((second.status, &second.body), (first.status, &first.body));
    assert_eq!(replayed(&second), Some("true"));
    assert_eq!(hits(&app).len(), 1);
    assert_eq!(intents(&app).len(), 1);
}

/// The same, but the commit fails: the repeat was answered from a record no
/// durable copy holds, so `CommitFailure::invalidates` must fail it with its
/// first copy, or its client would be acked a write that never happened.
#[test]
fn a_repeat_in_the_same_group_fails_with_its_first_copy() {
    let (sim, mut app) = intent_app(0x1D7E_3009);
    let t = WallTime::from_nanos(5 * SEC);
    let (mut g, staged) = app.begin_group(t).unwrap();
    let mut participants = Vec::new();
    for _ in 0..2 {
        let before = g.len();
        let reply = response(staged.stage_or_subscribe(&mut g, hit(1, "a").intent("k1"), t));
        participants.push((before, g.len() > before, reply));
    }
    assert_eq!(
        replayed(&participants[1].2),
        Some("true"),
        "the repeat was answered from its sibling's uncommitted record"
    );
    sim.crash_disk_now();
    let failure = g
        .commit()
        .expect_err("every disk op fails, so the commit cannot apply");
    assert_eq!(failure.applied, 0);
    for (i, (records_before, staged, _)) in participants.iter().enumerate() {
        assert!(
            failure.invalidates(*records_before, *staged),
            "participant {i} (records_before {records_before}, staged {staged}) kept its answer"
        );
    }
}

/// `App::handle` carries no time (`tx.now()` panics there), so the record's
/// `committed_at` is the `now` the request path was handed: the epoch.
#[test]
fn handle_without_time_records_committed_at_as_the_epoch() {
    let (_sim, mut app) = intent_app(0x1D7E_300A);
    let reply = app.handle(hit(1, "a").intent("k1"));
    assert_eq!(reply.status, 200, "{}", body(&reply));
    let rows = intents(&app);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].committed_at, WallTime::UNIX_EPOCH);
    let again = app.handle(hit(1, "a").intent("k1"));
    assert_eq!(replayed(&again), Some("true"));
}

/// Reads are outside the contract (spec §2): a GET route never reads an id,
/// so a writing GET runs on every re-send and a malformed id is not refused.
#[test]
fn a_get_route_ignores_an_idempotency_key() {
    let (_sim, mut app) = intent_app(0x1D7E_300B);
    let t = WallTime::from_nanos(5 * SEC);
    let first = app.handle_at(Request::new(Method::Get, "/touch").intent("k1"), t);
    let second = app.handle_at(Request::new(Method::Get, "/touch").intent("k1"), t);
    assert_eq!(
        (body(&first), body(&second)),
        ("\"1\"".to_string(), "\"2\"".to_string()),
        "both ran"
    );
    assert_eq!((replayed(&first), replayed(&second)), (None, None));
    assert_eq!(intents(&app).len(), 0, "a GET records nothing");
    let read = app.handle_at(
        Request::new(Method::Get, "/hits").header("Idempotency-Key", "not valid"),
        t,
    );
    assert_eq!((read.status, body(&read)), (200, "\"2\"".to_string()));
}

/// The ids the 400 refuses: empty, over 255 bytes, a space, DEL, non-ASCII.
fn malformed_ids() -> [String; 5] {
    [
        String::new(),
        "a".repeat(256),
        "a b".to_string(),
        "a\u{7f}".to_string(),
        "caf\u{e9}".to_string(),
    ]
}

/// A malformed id is refused by name wherever the request came from: a
/// test's `App::handle` or the kernel's request arm. Nothing runs.
#[test]
fn a_malformed_key_is_refused_with_400_from_any_source() {
    let (_sim, mut app) = intent_app(0x1D7E_300C);
    for id in malformed_ids() {
        let reply = app.handle(hit(1, "a").intent(id.clone()));
        assert_eq!(
            (reply.status, body(&reply)),
            (400, MALFORMED_HITS.to_string()),
            "{id:?}"
        );
    }

    let (sim, app) = intent_app(0x1D7E_300D);
    let mut kernel = Kernel::new(app, Box::new(sim.clock())).unwrap();
    let mut source = SimSource::new(Box::new(sim.clock()), 10 * SEC);
    for id in malformed_ids() {
        source.push_at(SEC, hit(1, "a").intent(id));
    }
    let responses = source.responses();
    kernel.run(&mut source).unwrap();
    let responses = responses.lock().unwrap();
    assert_eq!(responses.len(), malformed_ids().len());
    for (status, reply) in responses.iter() {
        assert_eq!(
            (*status, String::from_utf8_lossy(reply).into_owned()),
            (400, MALFORMED_HITS.to_string())
        );
    }
}

/// The refusal comes before the request's `fresh_token` draw, so a refused
/// request moves no entropy stream: the next login mints the token it would
/// have minted had the refused request never arrived.
#[test]
fn a_malformed_key_draws_no_entropy() {
    let seed = 0x1D7E_300E;
    let login = |app: &mut App| {
        let reply = app.handle(Request::new(Method::Post, "/login").json(&"alice"));
        serde_json::from_slice::<String>(&reply.body).unwrap()
    };
    let (_sim, mut alone) = intent_app(seed);
    let expected = login(&mut alone);

    let (_sim, mut refused) = intent_app(seed);
    assert_eq!(refused.handle(hit(1, "a").intent("a b")).status, 400);
    assert_eq!(login(&mut refused), expected, "the refused request drew");

    // The control: a request that reaches the draw moves the stream, so the
    // comparison above can fail.
    let (_sim, mut ran) = intent_app(seed);
    assert_eq!(ran.handle(hit(1, "a").intent("ok")).status, 200);
    assert_ne!(login(&mut ran), expected, "a request that ran drew nothing");
}

// ------------------------------------------------- retention and the sweep

/// Runs `kernel` until virtual time `horizon`, with each request arriving at
/// its virtual time, and returns each request's `(status, body)` in push
/// order.
fn run_kernel(
    sim: &Sim,
    kernel: &mut Kernel,
    requests: Vec<(u64, Request)>,
    horizon: u64,
) -> Vec<(u16, String)> {
    let mut source = SimSource::new(Box::new(sim.clock()), horizon);
    for (at, request) in requests {
        source.push_at(at, request);
    }
    let answers = source.answers();
    kernel.run(&mut source).unwrap();
    let answers = answers.lock().unwrap();
    answers
        .iter()
        .map(|answer| {
            let (status, body) = answer.clone().expect("every request was answered");
            (status, String::from_utf8_lossy(&body).into_owned())
        })
        .collect()
}

fn answered(status: u16, body: &str) -> (u16, String) {
    (status, body.to_string())
}

/// The intent ids the record holds, sorted (rows sort by a hash of their
/// key, not by commit).
fn recorded(app: &App) -> Vec<String> {
    let mut ids: Vec<String> = intents(app).into_iter().map(|row| row.intent).collect();
    ids.sort();
    ids
}

/// The lookup ignores age (spec §4.4), and the sweep waits a full retention
/// after an open, so a re-send just after a power cycle that jumped the wall
/// clock 3 days, the most the sim's clock-downtime axis jumps it, is answered
/// from its record.
#[test]
fn a_resend_after_a_three_day_power_cycle_is_answered_from_its_record() {
    let (sim, app) = intent_app(0x1D7E_3101);
    let mut kernel = Kernel::new(app, Box::new(sim.clock())).unwrap();
    let first = run_kernel(
        &sim,
        &mut kernel,
        vec![(SEC, hit(1, "a").intent("k"))],
        2 * SEC,
    );
    assert_eq!(first, [answered(200, "\"1\"")]);
    let committed_at = intents(kernel.app())[0].committed_at;
    drop(kernel);

    sim.clock().sleep(Duration::from_secs(3 * 86_400));
    sim.restart();
    let app = open_app(Box::new(sim.disk()), Box::new(sim.entropy()));
    let mut kernel = Kernel::new(app, Box::new(sim.clock())).unwrap();
    let opened_at = kernel.wall_for_sim();
    assert!(
        opened_at.saturating_duration_since(committed_at) >= Duration::from_secs(3 * 86_400),
        "the reopen is 3 days after the commit: committed {committed_at}, opened {opened_at}"
    );
    let open = sim.now().as_nanos();
    let again = run_kernel(
        &sim,
        &mut kernel,
        vec![(open + SEC, hit(1, "a").intent("k"))],
        open + 2 * SEC,
    );
    assert_eq!(
        again, first,
        "a re-send just after the reopen ran again instead of being answered from its record"
    );
    assert_eq!(
        hits(kernel.app()).len(),
        1,
        "the re-send applied a second time"
    );
    let wall = kernel.wall_for_sim();
    let replay = kernel
        .app_mut_for_tests()
        .handle_at(hit(1, "a").intent("k"), wall);
    assert_eq!(replayed(&replay), Some("true"), "{}", body(&replay));
}

/// The sweep deletes nothing until the node has been open a full
/// `INTENT_RETENTION`, then deletes rows committed more than that before the
/// sweep runs, and keeps younger ones. A re-send whose row was swept runs
/// again: the documented limit (GUIDE §5).
#[test]
fn the_sweep_waits_a_full_retention_after_open_then_drops_older_records() {
    let (sim, app) = intent_app(0x1D7E_3102);
    let mut kernel = Kernel::new(app, Box::new(sim.clock())).unwrap();
    let opened_at = kernel.wall_for_sim();
    // k1 commits at the open, k2 a minute short of a retention after it.
    let first = run_kernel(
        &sim,
        &mut kernel,
        vec![
            (0, hit(1, "a").intent("k1")),
            (DAY - MIN, hit(2, "b").intent("k2")),
        ],
        DAY - MIN,
    );
    assert_eq!(first, [answered(200, "\"1\""), answered(200, "\"2\"")]);
    assert_eq!(
        recorded(kernel.app()),
        ["k1", "k2"],
        "a minute short of a retention after the open, the record holds both"
    );
    let k1 = intents(kernel.app())
        .into_iter()
        .find(|row| row.intent == "k1")
        .unwrap();
    assert_eq!(k1.committed_at, opened_at, "k1 commits at the open");

    // A sweep a retention after the open, which k1 (exactly a retention old)
    // survives, and one an interval after that: k1 ages out the nanosecond
    // after the first, and a sweep that leaves records waits an interval.
    run_kernel(&sim, &mut kernel, Vec::new(), DAY + HOUR + MIN);
    assert_eq!(sim.now().as_nanos(), DAY + HOUR, "the kernel's last step");
    assert_eq!(
        recorded(kernel.app()),
        ["k2"],
        "a retention and an hour after the open, k1 (committed at the open) is swept, and k2 \
         (committed 61 minutes before this sweep) is kept"
    );

    let wall = kernel.wall_for_sim();
    let app = kernel.app_mut_for_tests();
    let rerun = app.handle_at(hit(1, "a").intent("k1"), wall);
    assert_eq!(
        (rerun.status, body(&rerun), replayed(&rerun)),
        (200, "\"3\"".to_string(), None),
        "a re-send whose record was swept runs again"
    );
    let replay = app.handle_at(hit(2, "b").intent("k2"), wall);
    assert_eq!(
        (replay.status, body(&replay), replayed(&replay)),
        (200, "\"2\"".to_string(), Some("true")),
        "a re-send whose record is younger than a retention is answered from it"
    );
    assert_eq!(hits(kernel.app()).len(), 3);
}

/// A sweep deletes at most `INTENT_SWEEP_BATCH` (4096) rows. After a full
/// batch it is due again at once, so a backlog drains at one wall time
/// rather than one batch a sweep interval.
#[test]
fn a_full_batch_is_swept_again_without_waiting_an_interval() {
    let (sim, mut app) = intent_app(0x1D7E_3103);
    let db = app.db_for_tests();
    let mut group = db.begin_group().unwrap();
    let access = group.intent_access();
    group
        .stage(|tx| -> Result<(), std::io::Error> {
            for i in 1..=4097u64 {
                tx.put_intent(
                    &IntentRow {
                        id: Id::from_raw(i),
                        intent: format!("old-{i}"),
                        principal: None,
                        route: "POST /hits/:id".to_string(),
                        fingerprint: [0; 32],
                        committed_at: WallTime::from_nanos(SEC),
                        status: 200,
                        reply: IntentReply::Credential,
                    },
                    &access,
                );
            }
            Ok(())
        })
        .unwrap();
    group.commit().unwrap();

    let mut kernel = Kernel::new(app, Box::new(sim.clock())).unwrap();
    let mut source = SimSource::new(Box::new(sim.clock()), u64::MAX);
    sim.clock().sleep(Duration::from_nanos(DAY));
    let mut left = vec![intents(kernel.app()).len()];
    while let Step::Worked = kernel.step(&mut source).unwrap() {
        let now = intents(kernel.app()).len();
        if left.last() != Some(&now) {
            left.push(now);
        }
    }
    assert_eq!(
        left,
        [4097, 1, 0],
        "the rows left after each sweep: one full batch, then the last row, at one wall time"
    );
    assert_eq!(
        sim.now().as_nanos(),
        DAY,
        "no time passed between the sweeps"
    );
}

/// A sweep that leaves the record empty is next due a full retention later,
/// since a record committed from then on cannot age out sooner, so an idle
/// node sweeps once a day. A record committed in between is still deleted
/// the nanosecond after it ages out, not on the daily cadence.
#[test]
fn an_empty_table_schedules_the_next_sweep_a_full_retention_out() {
    let (sim, app) = intent_app(0x1D7E_3105);
    let mut kernel = Kernel::new(app, Box::new(sim.clock())).unwrap();
    let mut source = SimSource::new(Box::new(sim.clock()), u64::MAX);
    sim.clock().sleep(Duration::from_nanos(DAY));
    let parked = loop {
        if let Step::Idle { deadline } = kernel.step(&mut source).unwrap() {
            break deadline;
        }
    };
    assert_eq!(
        parked.map(Instant::as_nanos),
        Some(2 * DAY),
        "after the first sweep found the record empty, the kernel parks for a full retention: \
         nothing, the next sweep included, is due sooner (the platform's daily session sweep is \
         due then too)"
    );

    let committed = DAY + 6 * HOUR;
    run_kernel(
        &sim,
        &mut kernel,
        vec![(committed, hit(1, "a").intent("k"))],
        committed + DAY,
    );
    assert_eq!(
        recorded(kernel.app()),
        ["k"],
        "a retention after its commit, to the nanosecond, the record is not yet older"
    );
    assert_eq!(
        sim.now().as_nanos(),
        2 * DAY,
        "the kernel's last step was the sweep the empty record scheduled, which found k young"
    );
    run_kernel(&sim, &mut kernel, Vec::new(), committed + DAY + SEC);
    assert_eq!(
        recorded(kernel.app()),
        Vec::<String>::new(),
        "the sweep due the nanosecond after k aged out deleted it"
    );
    assert_eq!(sim.now().as_nanos(), committed + DAY + 1);
}

/// A sweep that leaves records is next due when the oldest left ages out:
/// the nanosecond after its `committed_at + INTENT_RETENTION`, since the
/// bound is strict, so the record exactly at that time is deleted by that
/// sweep and time moves on rather than re-running it at one instant. And no
/// sooner than `INTENT_SWEEP_INTERVAL` after the last sweep, so a record that
/// ages out a minute after another waits for the next interval.
#[test]
fn a_sweep_is_due_when_the_oldest_record_ages_out_but_an_interval_apart() {
    let (sim, app) = intent_app(0x1D7E_3106);
    let mut kernel = Kernel::new(app, Box::new(sim.clock())).unwrap();
    // The first sweep, at DAY, finds the record empty and is next due at
    // 2·DAY, which finds a and b young.
    let a = DAY + 6 * HOUR;
    let b = a + MIN;
    run_kernel(
        &sim,
        &mut kernel,
        vec![(a, hit(1, "a").intent("a")), (b, hit(2, "b").intent("b"))],
        a + DAY,
    );
    assert_eq!(
        recorded(kernel.app()),
        ["a", "b"],
        "a retention after a's commit, to the nanosecond, a is not yet older"
    );

    run_kernel(&sim, &mut kernel, Vec::new(), a + DAY + SEC);
    assert_eq!(
        recorded(kernel.app()),
        ["b"],
        "the sweep due the nanosecond after a aged out deleted it"
    );
    assert_eq!(
        sim.now().as_nanos(),
        a + DAY + 1,
        "that sweep ran once and time moved on: it was not due again at its own instant"
    );

    run_kernel(&sim, &mut kernel, Vec::new(), b + DAY + SEC);
    assert_eq!(
        recorded(kernel.app()),
        ["b"],
        "b aged out a minute after a, but the sweep that left it waits an interval"
    );

    run_kernel(&sim, &mut kernel, Vec::new(), a + DAY + HOUR + SEC);
    assert_eq!(
        recorded(kernel.app()),
        Vec::<String>::new(),
        "an interval after the sweep that deleted a, b is gone"
    );
    assert_eq!(sim.now().as_nanos(), a + DAY + 1 + HOUR);
}

/// A promotion counts as an open (spec §4.4): clients have just regained a
/// primary. A node that was a follower for 12 hours, and then is promoted,
/// keeps a 3-day-old record for a full retention past the promotion, and
/// sweeps it after.
#[test]
fn a_promotion_restarts_the_retention_clock() {
    let sim = Sim::with_faults(0x1D7E_3104, FaultConfig::none());
    let host = sim.host("f");
    // An earlier life of this node, as a single node, records k.
    let app = open_app(Box::new(host.disk()), Box::new(host.entropy()));
    let mut kernel = Kernel::new(app, Box::new(host.clock())).unwrap();
    let first = run_kernel(
        &sim,
        &mut kernel,
        vec![(SEC, hit(1, "a").intent("k"))],
        2 * SEC,
    );
    assert_eq!(first, [answered(200, "\"1\"")]);
    drop(kernel);

    // Three days later it reopens as a follower, which sweeps nothing.
    sim.clock().sleep(Duration::from_secs(3 * 86_400));
    host.restart();
    let mut app = open_app(Box::new(host.disk()), Box::new(host.entropy()));
    let cfg = ReplConfig {
        listen: Some("f:1".to_string()),
        initial_role: Role::Follower {
            primary: "p:1".to_string(),
        },
        peer_secret: None,
        strict_witnesses: false,
    };
    let (repl, _) = Repl::open(
        Box::new(host.net()),
        Box::new(host.disk()),
        cfg,
        app.db_for_tests(),
    )
    .unwrap();
    let mut kernel = Kernel::with_replication(app, Box::new(host.clock()), repl).unwrap();
    assert!(!kernel.app().db().writes_allowed(), "it opens a follower");

    // Promoted 12 hours after it opened, at `asked` or within the second
    // after it.
    sim.clock().sleep(Duration::from_nanos(12 * HOUR));
    let asked = sim.now().as_nanos();
    let ctrl = sim.host("cli").net().connect("f:1").unwrap();
    ctrl.send(&encode(&Msg::CtrlPromote { secret: None }))
        .unwrap();
    run_kernel(&sim, &mut kernel, Vec::new(), asked + SEC);
    let mut buf = vec![0u8; MAX_MESSAGE_BYTES];
    let n = ctrl
        .recv(&mut buf)
        .unwrap()
        .expect("the promote was answered");
    let reply = decode(&buf[..n]).unwrap();
    assert!(matches!(reply, Msg::CtrlOk { .. }), "{reply:?}");
    assert!(kernel.app().db().writes_allowed(), "the promote opened it");

    let again = run_kernel(
        &sim,
        &mut kernel,
        vec![(asked + SEC, hit(1, "a").intent("k"))],
        asked + 2 * SEC,
    );
    assert_eq!(
        again, first,
        "a re-send just after the promotion ran again instead of being answered from its record"
    );

    sim.clock().sleep(Duration::from_nanos(
        asked + DAY - MIN - sim.now().as_nanos(),
    ));
    run_kernel(&sim, &mut kernel, Vec::new(), asked + DAY - MIN + SEC);
    assert_eq!(
        recorded(kernel.app()),
        ["k"],
        "a minute short of a retention after the promotion, and a retention and 12 hours \
         after the follower opened, the record is kept"
    );

    sim.clock().sleep(Duration::from_nanos(
        asked + DAY + MIN - sim.now().as_nanos(),
    ));
    run_kernel(&sim, &mut kernel, Vec::new(), asked + DAY + MIN + SEC);
    assert_eq!(
        recorded(kernel.app()),
        Vec::<String>::new(),
        "a retention after the promotion, the sweep deleted the 3-day-old record"
    );
}

// ---------------------------------------- the sweep under injected faults

/// The faulted property's base, pinned so the property examines the same
/// universes every run (`run_universes_swarm_with!`); `CHARPENTE_BASE_SEED`
/// re-bases it.
const SWEEP_BASE: u64 = 0x1D7E_5A00;
const SWEEP_UNIVERSES: u64 = 512;
/// `INTENT_SWEEP_INTERVAL`, in nanoseconds.
const INTERVAL: u64 = INTENT_SWEEP_INTERVAL.as_nanos() as u64;
/// Boots per open attempt (`boot_through_fallout`), open attempts per boot
/// of the node, and reopens per planned life after its kernel stops.
const SWEEP_BOOTS: u32 = 8;
const SWEEP_OPENS: u32 = 16;
const SWEEP_STOPS: u32 = 16;

// The coverage the checks mark, each at most once per universe.
/// A boot in the faulted span, after an injected fault fired, found a
/// write's record gone, as the sweep may take it: the sweep deleted under
/// faults.
const SWEPT_UNDER_FAULTS: &str = "intent:swept-under-faults";
/// A life ended in a power cycle before a record it committed aged out.
const POWER_CYCLE_BEFORE_AGE_OUT: &str = "intent:power-cycle-before-age-out";
/// A re-send was answered from a record that had aged out by its commit
/// before the node's latest open, which alone still kept it.
const OPEN_TERM_HELD: &str = "intent:open-term-held";
/// A re-send was answered, after its life's first sweep was due, from a
/// record committed before then.
const HELD_PAST_A_SWEEP: &str = "intent:held-past-a-sweep";
/// A write ran again once its record could have aged out.
const RAN_AGAIN_AFTER_AGE_OUT: &str = "intent:ran-again-after-age-out";
/// The settle's sweeps took every record, the faulted span's and its own.
const AGED_OUT_IN_SETTLE: &str = "intent:aged-out-in-settle";

/// The swarm's sampled disk axes less its two durable-corruption axes, plus
/// the clock axes. A bit flip or a misdirected write cuts an acked suffix at
/// recovery (`checks::durable_corruption_fired`): excused loss of a write or
/// of its record, which is what this property must not excuse, and those
/// axes have their own suites. The downtime jump is the case the open term
/// exists for, so it is on in three universes of four; step-backs are on
/// in half, at clock.rs's rates, and their sizes and the skew's span
/// `CLOCK_TOLERANCE`. Drawn from a stream keyed by the universe's seed
/// alone.
fn sweep_axes(seed: u64, cfg: &mut FaultConfig) {
    let mut state = seed ^ 0x1D7E_C10C;
    let mut rng = Rng::new(splitmix64(&mut state));
    cfg.bit_flip = 0.0;
    cfg.misdirected_write = 0.0;
    cfg.clock_skew = rng.between(0, 120 * SEC) as i64;
    cfg.clock_step_back = if rng.chance(0.5) {
        (
            rng.between(5, 80) as f64 / 10_000.0,
            Duration::from_secs(120),
        )
    } else {
        (0.0, Duration::ZERO)
    };
    cfg.clock_downtime = if rng.chance(0.75) {
        (0.5, Duration::from_secs(3 * 86_400))
    } else {
        (0.0, Duration::ZERO)
    };
}

/// One planned life of the node: how long it runs in virtual time, what it
/// is asked and when (an offset and a write's index, a first copy or a
/// re-send), whether it ends in a power cycle, and how long the node stays
/// down after.
struct Planned {
    length: u64,
    asks: Vec<(u64, usize)>,
    power_cycle: bool,
    gap: u64,
}

/// Two to four lives, at least one of them open past a retention, so its
/// sweep runs under the faults, and asked again after that sweep is due.
/// Each ask is a new write under a fresh id or, two times in five, a re-send
/// of an earlier one under its first copy's id, so re-sends land at every
/// delay, inside and outside their retention. A fifth of the gaps outlast a
/// retention, as do half the clock's downtime jumps.
fn plan(rng: &mut Rng) -> (Vec<Planned>, usize) {
    let count = rng.between(2, 4) as usize;
    let long = rng.below(count as u64) as usize;
    let mut writes = 0;
    let mut lives = Vec::with_capacity(count);
    for i in 0..count {
        let length = if i == long || rng.chance(0.25) {
            DAY + rng.between(HOUR, 8 * HOUR)
        } else {
            rng.between(10 * MIN, 8 * HOUR)
        };
        let n = rng.between(3, 8);
        let mut times: Vec<u64> = (0..n).map(|_| rng.below(length)).collect();
        if length > DAY {
            let late = rng.between(2, 5);
            times.extend((0..late).map(|_| rng.between(DAY, length)));
        }
        times.sort_unstable();
        let mut asks = Vec::with_capacity(times.len());
        for at in times {
            let write = if writes > 0 && rng.chance(0.4) {
                rng.below(writes as u64) as usize
            } else {
                writes += 1;
                writes - 1
            };
            asks.push((at, write));
        }
        let gap = match rng.below(10) {
            0..5 => rng.between(SEC, 10 * MIN),
            5..8 => rng.between(HOUR, 20 * HOUR),
            _ => rng.between(DAY, 2 * DAY),
        };
        lives.push(Planned {
            length,
            asks,
            power_cycle: rng.chance(0.7),
            gap,
        });
    }
    (lives, writes)
}

fn note(write: usize) -> String {
    format!("w{write}")
}

/// Write `w`'s request: its note is its intent id, so a hit names its write.
fn stamped(write: usize) -> Request {
    Request::new(Method::Post, "/stamped")
        .json(&note(write))
        .intent(note(write))
}

fn wt(nanos: u64) -> WallTime {
    WallTime::from_nanos(nanos)
}

fn fallout(sim: &Sim, text: &str) -> bool {
    is_fault_fallout(text) || clock_fallout(sim, text)
}

/// Whether a disk, crash or clock fault has fired in this universe yet.
fn fault_fired(sim: &Sim) -> bool {
    sim.coverage().iter().any(|(key, &n)| {
        n > 0
            && (key.starts_with("fault:")
                || key.starts_with("crash:")
                || *key == "clock:stepped-back"
                || *key == "clock:downtime")
    })
}

/// A supervisor's restart after a failed open: a power cycle when the disk
/// crashed, since only that brings it back, and the process otherwise.
fn reboot(sim: &Sim) {
    if sim.disk_crashed() {
        sim.restart();
    } else {
        sim.restart_process();
    }
}

/// One life of the node, in the kernel's walls: the one it opened at, and
/// the latest any of its steps read.
struct Life {
    open: u64,
    end: u64,
    power_cycled: bool,
}

/// An answer, at the wall of the step that gave it.
struct Answer {
    write: usize,
    life: usize,
    wall: u64,
    status: u16,
    body: String,
}

/// A run of a write, as the store holds it: its hit's id, the wall it
/// committed at, and the life and the answer of the step that committed it.
#[derive(Clone, Copy)]
struct Commit {
    hit: u64,
    at: u64,
    life: usize,
    seq: usize,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Check {
    /// A boot in the faulted span, before its kernel is built.
    Boot,
    /// The settle's boot.
    Settle,
    /// The end of the settle.
    End,
}

#[derive(Default)]
struct Rig {
    lives: Vec<Life>,
    answers: Vec<Answer>,
    marks: BTreeSet<&'static str>,
}

impl Rig {
    /// A power cycle or a process restart, after a life.
    fn restart(&mut self, sim: &Sim, power: bool) {
        if power {
            sim.restart();
            if let Some(life) = self.lives.last_mut() {
                life.power_cycled = true;
            }
        } else {
            sim.restart_process();
        }
    }

    /// Whether a record committed at wall `at` in life `from` stayed inside
    /// its retention up to wall `until` in life `to`: no life since its
    /// commit read a wall at or past `max(at, that life's open) +
    /// INTENT_RETENTION`, so no sweep may have taken it.
    fn live(&self, at: u64, from: usize, to: usize, until: u64) -> bool {
        (from..=to).all(|i| {
            let life = &self.lives[i];
            let latest = if i == to { until } else { life.end };
            at.max(life.open) + DAY > latest
        })
    }

    /// The store's runs by intent id, in commit order. A hit's tag is the
    /// wall it committed at, and the step that commits a write answers it,
    /// even when the commit fails, so each run names its step's answer.
    fn commits(&self, hits: &[Hit]) -> Result<BTreeMap<String, Vec<Commit>>, String> {
        let mut commits: BTreeMap<String, Vec<Commit>> = BTreeMap::new();
        for hit in hits {
            let write = hit.note.strip_prefix('w').and_then(|w| w.parse().ok());
            let seq = self
                .answers
                .iter()
                .position(|a| a.wall == hit.tag && Some(a.write) == write)
                .ok_or_else(|| {
                    format!(
                        "rig: write `{}` committed at {} as hit {}, and no step at that wall \
                         answered it, though the step that commits a write answers it",
                        hit.note,
                        wt(hit.tag),
                        hit.id.raw()
                    )
                })?;
            commits.entry(hit.note.clone()).or_default().push(Commit {
                hit: hit.id.raw(),
                at: hit.tag,
                life: self.answers[seq].life,
                seq,
            });
        }
        Ok(commits)
    }

    /// Checks the store at a boot, before its kernel is built, or at the
    /// end, and returns how many records it holds. A boot's read that an
    /// injected fault stops skips its check; the settle's and the end's
    /// reads meet no fault.
    fn check(&mut self, sim: &Sim, app: &App, at: Check) -> Result<usize, String> {
        let read = app
            .db()
            .snapshot()
            .and_then(|snap| Ok((snap.scan::<Hit>()?, snap.scan::<IntentRow>()?)));
        let (hits, rows) = match read {
            Ok(store) => store,
            Err(e) if at == Check::Boot && is_fault_fallout(&e.to_string()) => return Ok(0),
            Err(e) => return Err(format!("reading the store: {e}")),
        };
        let commits = self.commits(&hits)?;

        // Safety, seen from outside: a re-send runs again, and so adds a hit,
        // only once the record its write's last run left may have aged out.
        for (intent, runs) in &commits {
            for pair in runs.windows(2) {
                let (first, again) = (pair[0], pair[1]);
                if self.live(first.at, first.life, again.life, again.at) {
                    let open = self.lives[again.life].open;
                    return Err(format!(
                        "write `{intent}` ran again while its record was live: committed_at \
                         {}, ran again at {} on a node opened at {}, so its record ages out at \
                         max(committed_at, latest open) + INTENT_RETENTION = {}, after the \
                         re-run, and no wall the node read since the commit reached its \
                         age-out — a sweep dropped a live record, or the write committed \
                         without it",
                        wt(first.at),
                        wt(again.at),
                        wt(open),
                        wt(first.at.max(open) + DAY)
                    ));
                }
                self.marks.insert(RAN_AGAIN_AFTER_AGE_OUT);
            }
        }

        // Atomicity: a record is its write's latest run's, committed with it
        // and holding its reply, and a run whose record is gone could have
        // aged out.
        let mut recorded = BTreeSet::new();
        for row in &rows {
            let Some(run) = commits.get(&row.intent).and_then(|runs| runs.last()) else {
                return Err(format!(
                    "the record of `{}`, committed_at {}, has no write: no hit carries its id",
                    row.intent, row.committed_at
                ));
            };
            let reply = format!("\"{}\"", run.hit).into_bytes();
            let kept = matches!(&row.reply, IntentReply::Kept { body, .. } if *body == reply);
            if row.committed_at.as_nanos() != run.at || !kept {
                return Err(format!(
                    "the record of `{}` is not its write's latest run's: it was committed at {} \
                     and keeps {:?}, and the write last committed at {} as hit {}",
                    row.intent,
                    row.committed_at,
                    row.reply,
                    wt(run.at),
                    run.hit
                ));
            }
            recorded.insert(row.intent.as_str());
        }
        for (intent, runs) in &commits {
            if recorded.contains(intent.as_str()) {
                continue;
            }
            let run = runs.last().expect("an intent is listed for its runs");
            let last = self.lives.len() - 1;
            let end = self.lives[last].end;
            if self.live(run.at, run.life, last, end) {
                return Err(format!(
                    "write `{intent}` committed at {} and its record is gone, yet no wall the \
                     node read since reached its age-out (the latest open {}, the latest wall \
                     {}) — a sweep dropped a live record, or the write committed without it",
                    wt(run.at),
                    wt(self.lives[last].open),
                    wt(end)
                ));
            }
            if at == Check::Boot && fault_fired(sim) {
                self.marks.insert(SWEPT_UNDER_FAULTS);
            }
        }
        if at == Check::End {
            self.check_end(sim, &commits, &rows)?;
        }
        Ok(rows.len())
    }

    /// Liveness over the records the settle left, every answer against the
    /// store, and the coverage only the whole run shows.
    fn check_end(
        &mut self,
        sim: &Sim,
        commits: &BTreeMap<String, Vec<Commit>>,
        rows: &[IntentRow],
    ) -> Result<(), String> {
        let settle = self.lives.last().expect("the settle opened");
        let (open, end) = (settle.open, settle.end);
        // A sweep that leaves records is due again by the next age-out, and
        // an interval after itself at the latest, so a record still held an
        // interval past its age-out was never taken.
        for row in rows {
            let ages_out = row.committed_at.as_nanos().max(open) + DAY;
            if ages_out + INTERVAL <= end {
                return Err(format!(
                    "the record of `{}`, committed_at {}, aged out at {} (max(committed_at, \
                     latest open {}) + INTENT_RETENTION) and is still held at {}, more than \
                     INTENT_SWEEP_INTERVAL later — the sweep never took it",
                    row.intent,
                    row.committed_at,
                    wt(ages_out),
                    wt(open),
                    wt(end)
                ));
            }
        }

        // Every answer is a 200 carrying its run's reply, its own step's or
        // the one its record kept, or a 5xx naming fault fallout.
        for (seq, answer) in self.answers.iter().enumerate() {
            let intent = note(answer.write);
            match answer.status {
                200 => {}
                500..=599 if fallout(sim, &answer.body) => continue,
                status => {
                    return Err(format!(
                        "write `{intent}` was answered {status} {} at {}: under the faults a \
                         write is answered 200, or a 5xx naming their fallout",
                        answer.body,
                        wt(answer.wall)
                    ));
                }
            }
            let runs = commits.get(&intent).map_or(&[][..], Vec::as_slice);
            let own = runs
                .iter()
                .find(|c| c.at == answer.wall && c.life == answer.life);
            let Some(run) = own.or_else(|| runs.iter().rev().find(|c| c.seq < seq)) else {
                return Err(format!(
                    "write `{intent}` was answered 200 at {}, and the store holds no run of it \
                     from then or before: an ack for a write that never became durable",
                    wt(answer.wall)
                ));
            };
            let reply = format!("\"{}\"", run.hit);
            if answer.body != reply {
                return Err(format!(
                    "write `{intent}` was answered 200 {} at {}, not {reply}, the reply of its run \
                     committed at {}",
                    answer.body,
                    wt(answer.wall),
                    wt(run.at)
                ));
            }
            if own.is_none() && self.live(run.at, run.life, answer.life, answer.wall) {
                let open = self.lives[answer.life].open;
                if run.life < answer.life && run.at + DAY < open {
                    self.marks.insert(OPEN_TERM_HELD);
                }
                if answer.wall >= open + DAY && run.at < open + DAY {
                    self.marks.insert(HELD_PAST_A_SWEEP);
                }
            }
        }

        let cycled = commits.values().flatten().any(|run| {
            let life = &self.lives[run.life];
            life.power_cycled && run.at + DAY > life.end
        });
        if cycled {
            self.marks.insert(POWER_CYCLE_BEFORE_AGE_OUT);
        }
        Ok(())
    }
}

/// Boots the node and builds its kernel, as a supervisor would: through
/// fault fallout, and waiting out a refusal for the clock until the instant
/// it names. Checks the store at each boot. `None` when every attempt met
/// fallout or a boot gave the universe up.
fn open_node(sim: &Sim, rig: &mut Rig, at: Check) -> Result<Option<Kernel>, String> {
    for _ in 0..SWEEP_OPENS {
        let booted = boot_through_fallout(
            sim,
            SWEEP_BOOTS,
            |text| fallout(sim, text),
            || try_open_app(Box::new(sim.disk()), Box::new(sim.entropy())),
        )
        .map_err(|e| format!("a boot failed, and no injected fault explains it: {e}"))?;
        let Some(app) = booted else {
            if sim.gave_up().is_some() {
                return Ok(None);
            }
            reboot(sim);
            continue;
        };
        rig.check(sim, &app, at)?;
        let text = match Kernel::new(app, Box::new(sim.clock())) {
            // `Kernel::run`'s preamble, since this rig drives `step`.
            Ok(mut kernel) => match kernel.app_mut_for_tests().db_for_tests().migrate_schema() {
                Ok(()) => {
                    let open = kernel.wall_for_sim().as_nanos();
                    rig.lives.push(Life {
                        open,
                        end: open,
                        power_cycled: false,
                    });
                    return Ok(Some(kernel));
                }
                Err(e) => e.to_string(),
            },
            Err(e) => e.to_string(),
        };
        if let Some(after) = clock_reopens_after(&text).filter(|_| clock_fallout(sim, &text)) {
            let wait = after
                .as_nanos()
                .saturating_sub(sim.clock().wall().as_nanos())
                + SEC;
            sim.clock().sleep(Duration::from_nanos(wait));
            reboot(sim);
            continue;
        }
        if !fallout(sim, &text) {
            return Err(format!(
                "the node did not open, and no injected fault explains it: {text}"
            ));
        }
        reboot(sim);
    }
    Ok(None)
}

/// Steps `kernel` until nothing is left inside virtual time `horizon`, or
/// until it stops, sending each `(at, write)` at its virtual time, and
/// records each answer at the wall of the step that gave it and the life's
/// latest wall. Returns how many of `asks` the life took (all of them unless
/// it stopped, else those due by then) and the stop's text.
fn run_life(
    sim: &Sim,
    rig: &mut Rig,
    kernel: &mut Kernel,
    asks: &[(u64, usize)],
    horizon: u64,
) -> (usize, Option<String>) {
    let life = rig.lives.len() - 1;
    let mut source = SimSource::new(Box::new(sim.clock()), horizon);
    for &(at, write) in asks {
        source.push_at(at, stamped(write));
    }
    let slots = source.answers();
    let mut taken = vec![false; asks.len()];
    let stop = loop {
        let step = kernel.step(&mut source);
        let wall = kernel.wall_for_sim().as_nanos();
        rig.lives[life].end = rig.lives[life].end.max(wall);
        let slots = slots.lock().expect("answers lock");
        for (i, slot) in slots.iter().enumerate() {
            if let (false, Some((status, body))) = (taken[i], slot) {
                taken[i] = true;
                rig.answers.push(Answer {
                    write: asks[i].1,
                    life,
                    wall,
                    status: *status,
                    body: String::from_utf8_lossy(body).into_owned(),
                });
            }
        }
        drop(slots);
        match step {
            Ok(Step::Worked) => {}
            Ok(Step::Idle { deadline }) => {
                if let Wake::Shutdown = source.wait_until(deadline) {
                    break None;
                }
            }
            Err(e) => break Some(e.to_string()),
        }
    };
    let now = sim.now().as_nanos();
    let taken = match stop {
        None => asks.len(),
        Some(_) => asks.iter().take_while(|&&(at, _)| at <= now).count(),
    };
    (taken, stop)
}

/// The planned lives under the universe's faults, each reopened through
/// every stop, then a fault-free settle that re-sends every write once,
/// writes a few fresh ones, and runs past every record's age-out and an
/// interval. The store is checked at
/// every boot, and the store and every answer at the end (`Rig::check`).
fn sweep_universe(sim: &mut Sim) -> Result<(), String> {
    let mut rng = Rng::new(sim.seed() ^ 0x1D7E_5EED);
    let (plan, writes) = plan(&mut rng);
    let mut rig = Rig::default();
    for planned in &plan {
        let start = sim.now().as_nanos();
        let horizon = start + planned.length;
        let mut asks: Vec<(u64, usize)> = planned
            .asks
            .iter()
            .map(|&(at, write)| (start + at, write))
            .collect();
        for _ in 0..SWEEP_STOPS {
            let Some(mut kernel) = open_node(sim, &mut rig, Check::Boot)? else {
                break;
            };
            if rng.chance(0.3) {
                sim.crash_disk_after(rng.between(20, 800));
            }
            let (taken, stop) = run_life(sim, &mut rig, &mut kernel, &asks, horizon);
            asks.drain(..taken);
            drop(kernel);
            let Some(stop) = stop else {
                break;
            };
            if !fallout(sim, &stop) {
                return Err(format!(
                    "the kernel stopped, and no injected fault explains it: {stop}"
                ));
            }
            let power = sim.disk_crashed() || rng.chance(0.5);
            rig.restart(sim, power);
        }
        rig.restart(sim, planned.power_cycle || sim.disk_crashed());
        sim.clock().sleep(Duration::from_nanos(planned.gap));
    }

    // The settle: a power cycle, maybe a long time down, then no injected
    // fault.
    rig.restart(sim, true);
    if rng.chance(0.5) {
        sim.clock()
            .sleep(Duration::from_nanos(rng.between(DAY, 2 * DAY)));
    }
    sim.quiesce_disk_faults();
    sim.quiesce_clock_faults();
    let Some(mut kernel) = open_node(sim, &mut rig, Check::Settle)? else {
        sim.give_up("the fault-free settle never booted", "");
        return Ok(());
    };
    let open = rig.lives.last().expect("the settle opened").open;
    let rows = kernel
        .app()
        .db()
        .snapshot()
        .and_then(|snap| snap.scan::<IntentRow>())
        .map_err(|e| format!("the settle's read: {e}"))?;
    let latest = rows
        .iter()
        .map(|row| row.committed_at.as_nanos())
        .fold(open, u64::max);
    // Every write once more at the settle's start, and one to three fresh
    // ones over its first six hours, so every settle holds records that age
    // out at different times and its sweeps have to chain to take them all.
    let start = sim.now().as_nanos();
    let mut asks: Vec<(u64, usize)> = (0..writes).map(|write| (start, write)).collect();
    let mut fresh: Vec<u64> = (0..rng.between(1, 3))
        .map(|_| rng.between(MIN, 6 * HOUR))
        .collect();
    fresh.sort_unstable();
    asks.extend(
        fresh
            .iter()
            .enumerate()
            .map(|(i, &at)| (start + at, writes + i)),
    );
    let reach = (latest - open).max(6 * HOUR);
    let horizon = start + reach + DAY + INTERVAL + 10 * MIN;
    let (_, mut stop) = run_life(sim, &mut rig, &mut kernel, &asks, horizon);
    if stop.is_none() {
        // Once more at the horizon itself, so the latest wall the node read,
        // which liveness is judged at, is the horizon's, not that of the last
        // step before the kernel parked.
        let wait = horizon.saturating_sub(sim.now().as_nanos());
        sim.clock().sleep(Duration::from_nanos(wait));
        stop = run_life(sim, &mut rig, &mut kernel, &[], horizon).1;
    }
    if let Some(stop) = stop {
        return Err(format!("the fault-free settle's kernel stopped: {stop}"));
    }
    if rig.check(sim, kernel.app(), Check::End)? == 0 {
        rig.marks.insert(AGED_OUT_IN_SETTLE);
    }
    for mark in &rig.marks {
        sim.cover(mark);
    }
    Ok(())
}

/// The sweep under injected faults (hard rule 3; spec §4.4). Each universe
/// runs two to four lives of a node under the swarm's disk faults and the
/// clock's, with power cycles, crashes and the clock's downtime jumps between
/// them, sends writes under fresh intent ids and re-sends earlier ones under
/// theirs at every delay, then settles free of faults past every record's
/// age-out. Its checks:
///
/// - **Safety**, from outside: a write's re-send runs again, adding a hit,
///   only once the record its last run left may have aged out, that is once
///   the node read a wall at or past `max(committed_at, its latest open) +
///   INTENT_RETENTION`. And a write's record is never gone before then.
/// - **Liveness**: after the settle, no record is held an interval past its
///   age-out.
/// - **Atomicity**: every record is its write's latest run's, at the same
///   commit and with its reply, and every 200 is its run's reply.
#[test]
fn the_intent_sweep_never_drops_a_live_record_under_faults() {
    run_universes_swarm_with!(
        SWEEP_BASE,
        SWEEP_UNIVERSES,
        sweep_axes,
        sweep_universe,
        require: [
            SWEPT_UNDER_FAULTS,
            POWER_CYCLE_BEFORE_AGE_OUT,
            OPEN_TERM_HELD,
            HELD_PAST_A_SWEEP,
            RAN_AGAIN_AFTER_AGE_OUT,
            AGED_OUT_IN_SETTLE,
            "clock:downtime",
            "fault:crash",
        ]
    );
}
