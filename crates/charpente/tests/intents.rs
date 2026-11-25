//! Retry-safe writes, the request path (spec
//! docs/superpowers/specs/2026-11-22-retry-safe-writes-design.md §4.1–§4.3):
//! a write that carries an intent id is looked up in `charpente.intents`
//! inside its own staged transaction, a repeat is answered from the record
//! without running, and a fresh write's reply is recorded in the same
//! transaction as its effects.

use charpente::auth::hash_token;
use charpente::core::intent::{IntentReply, IntentRow};
use charpente::http::{ApiError, App, AppConfig, Ctx, Handled, Method, Request, Response, Route};
use charpente::kernel::{Kernel, SimSource};
use charpente::wire::WireU64;
use charpente::{Schema, Tx, View, route, routes, table};
use charpente_core::io::{Disk, WallTime};
use charpente_core::table::Id;
use charpente_sim::{FaultConfig, Sim};

const SEC: u64 = 1_000_000_000;

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
        add_hit, noop, login, mint_key, big, fail, count_hits, touch, add_mine
    ]
}

fn intent_app(seed: u64) -> (Sim, App) {
    let sim = Sim::with_faults(seed, FaultConfig::none());
    let app = App::open(
        Box::new(sim.disk()),
        Schema::new().table::<Hit>(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )
    .unwrap()
    .mount(intent_routes());
    (sim, app)
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
