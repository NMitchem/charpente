//! The HTTP kernel under the sim (PLAN.md M0 step 5): routing, decoding,
//! sessions, error mapping — unit tests here; the macro end-to-end tests and
//! the request-level crash property are appended by later plan tasks.

use base64ct::{Base64UrlUnpadded, Encoding};
use charpente::auth::{ApiKey, OidcConfig, OidcConfigError, hash_token};
use charpente::http::{
    ApiError, App, AppConfig, Ctx, Cursor, Handled, Method, Request, Response, Route, Session,
    Subscription,
};
use charpente::{Schema, Tx, event, route, routes, table};
use charpente_core::table::Id;
use charpente_sim::{FaultConfig, Sim};
use rsa::traits::PublicKeyParts;
use rsa::{BigUint, Pkcs1v15Sign, RsaPrivateKey, RsaPublicKey};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

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
    // ctx.principal() (not ctx.session()) so this route answers for any
    // ladder rung — session, API key, or (T5) OIDC.
    Ok(ctx.principal().expect("auth route").subject.clone())
}

#[route(POST "/logout", auth)]
fn logout(tx: &mut Tx, ctx: &Ctx) -> Result<&'static str, ApiError> {
    ctx.revoke(tx);
    Ok("bye")
}

fn note_routes() -> Vec<Route> {
    routes![create_note, get_note, login, whoami, logout]
}

// ---------------------------------------------------- api keys (auth T3)

#[route(POST "/keys", auth)]
fn create_key(tx: &mut Tx, ctx: &Ctx) -> Result<String, ApiError> {
    let subject = ctx.session().expect("auth route").subject.clone();
    Ok(ctx.create_api_key(tx, subject))
}

#[route(DELETE "/keys/:id", auth)]
fn revoke_key(tx: &mut Tx, id: Id<ApiKey>, ctx: &Ctx) -> Result<&'static str, ApiError> {
    let _ = ctx;
    tx.delete::<ApiKey>(id);
    Ok("revoked")
}

/// Test-only route driving the one-mint-per-request assert: a handler that
/// calls both mints in the same request.
#[route(POST "/double-mint")]
fn double_mint(tx: &mut Tx, ctx: &Ctx) -> Result<String, ApiError> {
    let _ = ctx.create_session(tx, "x");
    Ok(ctx.create_api_key(tx, "x"))
}

fn key_routes() -> Vec<Route> {
    routes![login, whoami, create_key, revoke_key, double_mint]
}

fn key_app(seed: u64) -> (Sim, App) {
    let sim = Sim::with_faults(seed, FaultConfig::none());
    let app = App::open(
        Box::new(sim.disk()),
        Schema::new().table::<Note>(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )
    .unwrap()
    .mount(key_routes());
    (sim, app)
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

/// The `{"error": "<message>"}` envelope's message, decoded — used to pin
/// the 401 family byte-exact (auth T4) without hand-escaping JSON quotes.
fn error_text(response: &Response) -> String {
    #[derive(Deserialize)]
    struct ErrBody {
        error: String,
    }
    let body: ErrBody = serde_json::from_slice(&response.body).unwrap();
    body.error
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
    // Byte-exact (error-suite T8): reconstructed from app.rs's own format
    // args (the primary address the test just configured) via the same
    // `{"error": ...}` envelope decode the auth-401 tests use.
    assert_eq!(
        error_text(&resp),
        "this node is a read-only follower — writes go to the primary at primary-1:9000; retry \
         there (the request was not committed)"
    );

    // A read route still answers 200 — the write gate never touches reads.
    let get = app.handle(Request::new(Method::Get, "/notes/1"));
    assert_eq!(get.status, 200);
    assert_eq!(body_text(&get), "\"hi\"");
}

/// M2 sub-project 0b task 3: `TxError::Verifying` maps to 503, naming the
/// unreached witness rather than a primary. Unlike `NotPrimary`, this node
/// does not know whether a primary exists elsewhere — it may still be the
/// legitimate one — so there is nothing honest to name; reads are
/// unaffected by the gate, same as `NotPrimary`.
#[test]
fn a_verifying_node_refuses_writes_with_503_and_names_the_unreached_witnesses() {
    const SEED: u64 = 0x9012_0007;
    let (_sim, mut app) = quiet_app(SEED);
    let created = app.handle(Request::new(Method::Post, "/notes").json(&CreateNote {
        id: 1,
        text: "hi".into(),
    }));
    assert_eq!(created.status, 200, "{}", body_text(&created));

    app.db_for_tests()
        .verify_writes(vec!["127.0.0.1:7001".into()]);

    let resp = app.handle(Request::new(Method::Post, "/notes").json(&CreateNote {
        id: 2,
        text: "bye".into(),
    }));
    assert_eq!(resp.status, 503, "{}", body_text(&resp));
    assert_eq!(
        error_text(&resp),
        "this node is not accepting writes yet: it was a primary before restarting and has \
         not confirmed that its follower 127.0.0.1:7001 was never promoted — reads are being \
         served normally, and writes resume automatically once that node answers; if it is \
         gone for good, see GUIDE §10's witness-set remedy"
    );

    // A read route still answers 200 — the write gate never touches reads.
    let get = app.handle(Request::new(Method::Get, "/notes/1"));
    assert_eq!(get.status, 200);
    assert_eq!(body_text(&get), "\"hi\"");
}

/// The plural rendering (hard rule 5: both renderings are pinned byte-exact,
/// not just the singular one the brief quotes verbatim): "its follower X"
/// becomes "its followers X, Y" once more than one witness is unreached.
#[test]
fn a_verifying_node_pluralizes_the_503_body_for_multiple_unreached_witnesses() {
    const SEED: u64 = 0x9012_0008;
    let (_sim, mut app) = quiet_app(SEED);

    app.db_for_tests().verify_writes(vec![
        "127.0.0.1:7001".to_string(),
        "127.0.0.1:7002".to_string(),
    ]);

    let resp = app.handle(Request::new(Method::Post, "/notes").json(&CreateNote {
        id: 1,
        text: "hi".into(),
    }));
    assert_eq!(resp.status, 503, "{}", body_text(&resp));
    assert_eq!(
        error_text(&resp),
        "this node is not accepting writes yet: it was a primary before restarting and has \
         not confirmed that its followers 127.0.0.1:7001, 127.0.0.1:7002 were never promoted \
         — reads are being served normally, and writes resume automatically once those nodes \
         answer; if they are gone for good, see GUIDE §10's witness-set remedy"
    );
}

#[test]
fn sessions_login_auth_logout() {
    let (_sim, mut app) = quiet_app(0x47C);

    // No session: auth routes are 401 with instructions.
    let denied = app.handle(Request::new(Method::Get, "/me"));
    assert_eq!(denied.status, 401);
    assert_eq!(
        error_text(&denied),
        "authentication required — send \"Authorization: Bearer <token>\" or the \
         charpente_session cookie"
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
fn session_token_is_hashed_at_rest_and_expires() {
    let sim = Sim::with_faults(0x005E_5510, FaultConfig::none());
    let mut app = App::open(
        Box::new(sim.disk()),
        Schema::new().table::<Note>(),
        Box::new(sim.entropy()),
        AppConfig {
            session_ttl: 1_000,
            ..AppConfig::default()
        },
    )
    .unwrap()
    .mount(note_routes());

    // 1. Login at now=0 mints a raw token.
    let login = app.handle_at(Request::new(Method::Post, "/login").json(&"alice"), 0);
    assert_eq!(login.status, 200, "{}", body_text(&login));
    let token: String = serde_json::from_slice(&login.body).unwrap();

    // 2. The stored row carries the hash, never the raw token.
    let snap = app.db().snapshot().unwrap();
    let hash = hash_token(&token);
    let rows = Session::by_token_hash(&snap, &hash).unwrap();
    assert_eq!(rows.len(), 1, "exactly one session row");
    assert_eq!(rows[0].token_hash, hash);
    assert_ne!(
        rows[0].token_hash, token,
        "the raw token must never be stored"
    );

    // 3. Still valid well inside the 1µs TTL.
    let me = app.handle_at(
        Request::new(Method::Get, "/me").header("Authorization", format!("Bearer {token}")),
        500,
    );
    assert_eq!(me.status, 200, "{}", body_text(&me));
    assert_eq!(body_text(&me), "\"alice\"");

    // 4. Past expiry: a distinct 401, not the generic unrecognized-credential one.
    let expired = app.handle_at(
        Request::new(Method::Get, "/me").header("Authorization", format!("Bearer {token}")),
        2_000,
    );
    assert_eq!(expired.status, 401, "{}", body_text(&expired));
    assert_eq!(error_text(&expired), "session expired — log in again");
}

#[test]
fn api_key_mints_prefixed_hashes_at_rest_and_revokes() {
    let (_sim, mut app) = key_app(0x47E);

    // Login as alice.
    let login = app.handle(Request::new(Method::Post, "/login").json(&"alice"));
    assert_eq!(login.status, 200, "{}", body_text(&login));
    let token: String = serde_json::from_slice(&login.body).unwrap();

    // 1. Mint an API key; the raw key is "ck_" + 64 hex chars.
    let minted = app.handle(
        Request::new(Method::Post, "/keys").header("Authorization", format!("Bearer {token}")),
    );
    assert_eq!(minted.status, 200, "{}", body_text(&minted));
    let raw_key: String = serde_json::from_slice(&minted.body).unwrap();
    assert!(raw_key.starts_with("ck_"), "{raw_key}");
    assert_eq!(raw_key.len(), "ck_".len() + 64);

    // 2. The stored row carries hash_token(raw_key), never the raw key.
    let snap = app.db().snapshot().unwrap();
    let hash = hash_token(&raw_key);
    let rows = ApiKey::by_key_hash(&snap, &hash).unwrap();
    assert_eq!(rows.len(), 1, "exactly one api key row");
    assert_eq!(rows[0].key_hash, hash);
    assert_ne!(
        rows[0].key_hash, raw_key,
        "the raw key must never be stored"
    );
    assert_eq!(rows[0].subject, "alice");
    let key_id = rows[0].id;

    // 3. Bearer resolution against the ladder is covered end to end by
    //    resolution_ladder_matrix (auth T4) — this test only proves the
    //    row + mint shape.

    // 4. Revoke: DELETE removes it from the table (revoke-as-delete).
    let revoked = app.handle(
        Request::new(Method::Delete, format!("/keys/{}", key_id.raw()))
            .header("Authorization", format!("Bearer {token}")),
    );
    assert_eq!(revoked.status, 200, "{}", body_text(&revoked));
    let after = ApiKey::by_key_hash(&app.db().snapshot().unwrap(), &hash).unwrap();
    assert!(after.is_empty(), "revoked key must not remain in the table");
}

/// The resolution ladder (auth T4, spec §2), end to end over one app: every
/// row of the matrix, exact status + exact 401 text where rejected. `now`
/// advances monotonically across rows so the 1µs session TTL's expiry
/// boundary (row 3) never leaks into a later row's "valid" assertions.
#[test]
fn resolution_ladder_matrix() {
    let sim = Sim::with_faults(0x4444_0001, FaultConfig::none());
    let mut app = App::open(
        Box::new(sim.disk()),
        Schema::new(),
        Box::new(sim.entropy()),
        AppConfig {
            session_ttl: 1_000,
            ..AppConfig::default()
        },
    )
    .unwrap()
    .mount(key_routes());

    const NO_CREDENTIAL: &str = "authentication required — send \"Authorization: Bearer \
                                  <token>\" or the charpente_session cookie";
    const UNRECOGNIZED: &str = "unrecognized credential — it may have been revoked; obtain a \
                                 new one";
    const EXPIRED: &str = "session expired — log in again";

    // 1. No header at all.
    let denied = app.handle_at(Request::new(Method::Get, "/me"), 0);
    assert_eq!(denied.status, 401, "{}", body_text(&denied));
    assert_eq!(error_text(&denied), NO_CREDENTIAL);

    // 2. Bearer, valid session.
    let login_alice = app.handle_at(Request::new(Method::Post, "/login").json(&"alice"), 0);
    let token_alice: String = serde_json::from_slice(&login_alice.body).unwrap();
    let ok = app.handle_at(
        Request::new(Method::Get, "/me").header("Authorization", format!("Bearer {token_alice}")),
        500,
    );
    assert_eq!(ok.status, 200, "{}", body_text(&ok));
    assert_eq!(body_text(&ok), "\"alice\"");

    // 3. Bearer, expired session (now past the 1_000ns TTL).
    let expired = app.handle_at(
        Request::new(Method::Get, "/me").header("Authorization", format!("Bearer {token_alice}")),
        2_000,
    );
    assert_eq!(expired.status, 401, "{}", body_text(&expired));
    assert_eq!(error_text(&expired), EXPIRED);

    // 4. Bearer, valid API key.
    let login_bob = app.handle_at(Request::new(Method::Post, "/login").json(&"bob"), 2_000);
    let token_bob: String = serde_json::from_slice(&login_bob.body).unwrap();
    let minted = app.handle_at(
        Request::new(Method::Post, "/keys").header("Authorization", format!("Bearer {token_bob}")),
        2_000,
    );
    assert_eq!(minted.status, 200, "{}", body_text(&minted));
    let raw_key_bob: String = serde_json::from_slice(&minted.body).unwrap();
    let key_ok = app.handle_at(
        Request::new(Method::Get, "/me").header("Authorization", format!("Bearer {raw_key_bob}")),
        2_500,
    );
    assert_eq!(key_ok.status, 200, "{}", body_text(&key_ok));
    assert_eq!(body_text(&key_ok), "\"bob\"");

    // 5. Bearer, revoked API key.
    let key_hash = hash_token(&raw_key_bob);
    let key_id = ApiKey::by_key_hash(&app.db().snapshot().unwrap(), &key_hash)
        .unwrap()
        .into_iter()
        .next()
        .expect("api key row exists")
        .id;
    let revoked = app.handle_at(
        Request::new(Method::Delete, format!("/keys/{}", key_id.raw()))
            .header("Authorization", format!("Bearer {token_bob}")),
        2_500,
    );
    assert_eq!(revoked.status, 200, "{}", body_text(&revoked));
    let after_revoke = app.handle_at(
        Request::new(Method::Get, "/me").header("Authorization", format!("Bearer {raw_key_bob}")),
        2_600,
    );
    assert_eq!(after_revoke.status, 401, "{}", body_text(&after_revoke));
    assert_eq!(error_text(&after_revoke), UNRECOGNIZED);

    // 6. Bearer, garbage.
    let garbage = app.handle_at(
        Request::new(Method::Get, "/me").header("Authorization", "Bearer nope"),
        2_600,
    );
    assert_eq!(garbage.status, 401, "{}", body_text(&garbage));
    assert_eq!(error_text(&garbage), UNRECOGNIZED);

    // 7. Cookie, valid session.
    let login_carol = app.handle_at(Request::new(Method::Post, "/login").json(&"carol"), 2_600);
    let token_carol: String = serde_json::from_slice(&login_carol.body).unwrap();
    let cookie_ok = app.handle_at(
        Request::new(Method::Get, "/me")
            .header("Cookie", format!("charpente_session={token_carol}")),
        2_700,
    );
    assert_eq!(cookie_ok.status, 200, "{}", body_text(&cookie_ok));
    assert_eq!(body_text(&cookie_ok), "\"carol\"");

    // 8. Bearer, two dots (JWT-shaped), no OIDC configured — judged as a
    //    JWT, never falls through to hash lookup.
    let jwt_shaped = app.handle_at(
        Request::new(Method::Get, "/me").header("Authorization", "Bearer a.b.c"),
        2_700,
    );
    assert_eq!(jwt_shaped.status, 401, "{}", body_text(&jwt_shaped));
    assert_eq!(error_text(&jwt_shaped), UNRECOGNIZED);

    // 9. A garbage bearer presented to a route that does NOT require auth
    //    (`/login` has no `auth` attribute) is still resolved — and
    //    rejected — because `build_ctx` runs the ladder unconditionally
    //    before checking `auth_required` (review-flagged coverage gap:
    //    pins the presented-but-invalid-on-open-route posture T4 landed).
    let garbage_open_route = app.handle_at(
        Request::new(Method::Post, "/login")
            .header("Authorization", "Bearer garbage-not-two-dots")
            .json(&"eve"),
        2_700,
    );
    assert_eq!(
        garbage_open_route.status,
        401,
        "{}",
        body_text(&garbage_open_route)
    );
    assert_eq!(error_text(&garbage_open_route), UNRECOGNIZED);
}

// ---------------------------------------------------- OIDC (auth T5)

/// A fixed RSA-2048 test keypair, generated ONCE with `openssl genrsa` and
/// checked in as its two 1024-bit primes — never generated at test runtime
/// (the deny-list forbids ambient OS entropy; `RsaPrivateKey::from_p_q`
/// rebuilds `n`/`d` deterministically from these, and `.sign(...)` below
/// uses RustCrypto's no-RNG PKCS1v15 path, so every minted token is
/// byte-reproducible).
const TEST_RSA_P: &[u8] = &[
    0xee, 0x28, 0xc5, 0xb6, 0x51, 0xe4, 0x61, 0xc6, 0x53, 0xd5, 0x9a, 0xe4, 0x22, 0x85, 0xa9, 0x52,
    0x2a, 0x43, 0xf7, 0xac, 0x85, 0x04, 0xfd, 0xb5, 0x1c, 0xa7, 0x30, 0x98, 0xc8, 0xf2, 0x5d, 0x71,
    0x58, 0xa5, 0x3f, 0x3a, 0xf8, 0x1f, 0xb1, 0xec, 0x68, 0xc7, 0xd4, 0x72, 0x6e, 0xc6, 0xad, 0xe1,
    0xf1, 0xc1, 0xe3, 0x70, 0xd0, 0x94, 0xca, 0x01, 0x7a, 0x2d, 0x0e, 0xc0, 0x0f, 0xa8, 0x61, 0xe8,
    0xc4, 0x06, 0x87, 0x9a, 0xaa, 0xf9, 0x78, 0xc5, 0x88, 0xd2, 0xba, 0x4d, 0x96, 0x3f, 0x05, 0x56,
    0x18, 0x64, 0xf8, 0x95, 0x42, 0x8d, 0x9c, 0xed, 0x6c, 0xbb, 0x0a, 0xb3, 0x95, 0xc3, 0x6e, 0x33,
    0xe9, 0xd5, 0x4c, 0x3a, 0x6b, 0x05, 0x6d, 0x8d, 0x15, 0x8d, 0xe1, 0x0e, 0x11, 0x21, 0x2f, 0x0e,
    0x16, 0x5a, 0x5f, 0xdc, 0xed, 0x2c, 0xa8, 0x2c, 0x37, 0x88, 0x5f, 0xea, 0xdd, 0x71, 0x73, 0x35,
];
const TEST_RSA_Q: &[u8] = &[
    0xc0, 0xab, 0x74, 0x12, 0x0a, 0x2d, 0xb5, 0xb0, 0x20, 0x84, 0x33, 0x8f, 0xf2, 0x85, 0xef, 0xac,
    0x2e, 0xeb, 0x07, 0x7b, 0x01, 0xcb, 0x66, 0x97, 0x63, 0x87, 0x1c, 0x3e, 0x0d, 0x41, 0xd5, 0xde,
    0x66, 0x63, 0x95, 0xe6, 0x9a, 0xd5, 0x4f, 0xc0, 0xd3, 0x0c, 0x8e, 0x27, 0xfd, 0x19, 0x5f, 0xc4,
    0x5a, 0xb6, 0x41, 0xd5, 0xdc, 0xee, 0x9c, 0xcb, 0x7c, 0x73, 0x80, 0x2f, 0x0f, 0xf6, 0xfe, 0x50,
    0x67, 0xe9, 0x7d, 0x33, 0x2b, 0x26, 0x42, 0x63, 0x64, 0xb3, 0xdd, 0xa8, 0x71, 0xcc, 0xdf, 0xce,
    0xac, 0xac, 0x75, 0x56, 0xf4, 0xc2, 0xc5, 0x6e, 0x22, 0x09, 0x14, 0x9f, 0x24, 0x28, 0x9f, 0xb4,
    0x04, 0x85, 0x98, 0xab, 0xdb, 0x3d, 0x6f, 0xd4, 0x8e, 0x64, 0x32, 0x7c, 0x5f, 0x93, 0x5f, 0x8b,
    0xa0, 0x93, 0xb9, 0xd8, 0xe5, 0x6f, 0x77, 0xdb, 0x86, 0x07, 0xc0, 0xbf, 0x79, 0xbf, 0x97, 0xf3,
];
const TEST_KID: &str = "test-key-1";
const OIDC_ISSUER: &str = "https://idp.test";
const OIDC_AUDIENCE: &str = "charpente";
/// An arbitrary fixed instant (nanos) used as `now` throughout — far enough
/// from zero that `exp`/`nbf` offsets in both directions never underflow.
const OIDC_NOW: u64 = 1_700_000_000_000_000_000;

fn test_rsa_keypair() -> (RsaPrivateKey, RsaPublicKey) {
    let p = BigUint::from_bytes_be(TEST_RSA_P);
    let q = BigUint::from_bytes_be(TEST_RSA_Q);
    let e = BigUint::from(65537u32);
    let private = RsaPrivateKey::from_p_q(p, q, e).expect("fixed test primes are valid");
    let public = RsaPublicKey::from(&private);
    (private, public)
}

fn test_jwks_json(public: &RsaPublicKey) -> String {
    let n = Base64UrlUnpadded::encode_string(&public.n().to_bytes_be());
    let e = Base64UrlUnpadded::encode_string(&public.e().to_bytes_be());
    serde_json::json!({"keys": [{"kty": "RSA", "kid": TEST_KID, "n": n, "e": e}]}).to_string()
}

/// Signs `header_json`.`payload_json` with the fixed test private key —
/// RustCrypto's no-RNG PKCS1v15 `sign` path (never `sign_with_rng`), so
/// minting is deterministic and reproducible byte-for-byte.
fn mint_jwt(
    private: &RsaPrivateKey,
    header_json: &serde_json::Value,
    payload: &serde_json::Value,
) -> String {
    let header_b64 = Base64UrlUnpadded::encode_string(header_json.to_string().as_bytes());
    let payload_b64 = Base64UrlUnpadded::encode_string(payload.to_string().as_bytes());
    let signing_input = format!("{header_b64}.{payload_b64}");
    let digest = Sha256::digest(signing_input.as_bytes());
    let sig = private
        .sign(Pkcs1v15Sign::new::<Sha256>(), &digest)
        .expect("no-rng PKCS1v15 sign");
    let sig_b64 = Base64UrlUnpadded::encode_string(&sig);
    format!("{header_b64}.{payload_b64}.{sig_b64}")
}

fn rs256_header() -> serde_json::Value {
    serde_json::json!({"alg": "RS256", "typ": "JWT", "kid": TEST_KID})
}

fn valid_claims() -> serde_json::Value {
    let now_secs = OIDC_NOW / 1_000_000_000;
    serde_json::json!({
        "iss": OIDC_ISSUER,
        "aud": OIDC_AUDIENCE,
        "sub": "bob",
        "exp": now_secs + 3600,
        "nbf": now_secs - 60,
    })
}

fn oidc_app(seed: u64, public: &RsaPublicKey) -> (Sim, App) {
    let sim = Sim::with_faults(seed, FaultConfig::none());
    let app = App::open(
        Box::new(sim.disk()),
        Schema::new(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )
    .unwrap()
    .mount(note_routes())
    .with_oidc(OidcConfig {
        issuer: OIDC_ISSUER.into(),
        audience: OIDC_AUDIENCE.into(),
        jwks_json: test_jwks_json(public),
    });
    (sim, app)
}

/// The OIDC claim matrix (spec §3), each check violated in isolation → its
/// named 401, byte-exact; the valid token accepted end to end.
#[test]
fn oidc_claim_matrix() {
    const BAD_SIGNATURE: &str = "OIDC token signature does not verify against any configured \
                                  JWKS key — the token was not issued by a trusted key; obtain \
                                  a new one";
    const EXPIRED: &str = "OIDC token expired — its \"exp\" claim is in the past; obtain a new \
                            one";
    const NOT_YET_VALID: &str = "OIDC token is not yet valid — its \"nbf\" claim is in the \
                                  future; wait or mint a fresh token";

    let (private, public) = test_rsa_keypair();

    // 1. Valid token: iss/aud/exp/nbf all ok, sub "bob".
    let valid_jwt = mint_jwt(&private, &rs256_header(), &valid_claims());
    let verifier = charpente::auth::OidcVerifier::from_config(OidcConfig {
        issuer: OIDC_ISSUER.into(),
        audience: OIDC_AUDIENCE.into(),
        jwks_json: test_jwks_json(&public),
    })
    .unwrap();
    assert_eq!(verifier.verify(&valid_jwt, OIDC_NOW).unwrap(), "bob");

    // 2. Bad signature: flip the first byte of the decoded signature.
    let mut segments = valid_jwt.split('.');
    let (h, p, s) = (
        segments.next().unwrap(),
        segments.next().unwrap(),
        segments.next().unwrap(),
    );
    let mut sig_bytes = Base64UrlUnpadded::decode_vec(s).unwrap();
    sig_bytes[0] ^= 0xff;
    let tampered_jwt = format!("{h}.{p}.{}", Base64UrlUnpadded::encode_string(&sig_bytes));
    assert_eq!(
        verifier
            .verify(&tampered_jwt, OIDC_NOW)
            .unwrap_err()
            .to_string(),
        BAD_SIGNATURE
    );

    // 3. exp in the past.
    let now_secs = OIDC_NOW / 1_000_000_000;
    let mut expired_claims = valid_claims();
    expired_claims["exp"] = serde_json::json!(now_secs - 3600);
    let expired_jwt = mint_jwt(&private, &rs256_header(), &expired_claims);
    assert_eq!(
        verifier
            .verify(&expired_jwt, OIDC_NOW)
            .unwrap_err()
            .to_string(),
        EXPIRED
    );

    // 4. nbf in the future.
    let mut not_yet_claims = valid_claims();
    not_yet_claims["exp"] = serde_json::json!(now_secs + 7200);
    not_yet_claims["nbf"] = serde_json::json!(now_secs + 3600);
    let not_yet_jwt = mint_jwt(&private, &rs256_header(), &not_yet_claims);
    assert_eq!(
        verifier
            .verify(&not_yet_jwt, OIDC_NOW)
            .unwrap_err()
            .to_string(),
        NOT_YET_VALID
    );

    // 5. Wrong issuer — names got vs configured.
    let mut wrong_iss_claims = valid_claims();
    wrong_iss_claims["iss"] = serde_json::json!("https://wrong.example");
    let wrong_iss_jwt = mint_jwt(&private, &rs256_header(), &wrong_iss_claims);
    assert_eq!(
        verifier
            .verify(&wrong_iss_jwt, OIDC_NOW)
            .unwrap_err()
            .to_string(),
        format!(
            "OIDC token has issuer \"https://wrong.example\" but this server is configured \
             for issuer \"{OIDC_ISSUER}\" — mint the token against the configured issuer or \
             fix OidcConfig::issuer"
        )
    );

    // 6. Wrong audience — names got vs configured.
    let mut wrong_aud_claims = valid_claims();
    wrong_aud_claims["aud"] = serde_json::json!("someone-else");
    let wrong_aud_jwt = mint_jwt(&private, &rs256_header(), &wrong_aud_claims);
    assert_eq!(
        verifier
            .verify(&wrong_aud_jwt, OIDC_NOW)
            .unwrap_err()
            .to_string(),
        format!(
            "OIDC token has audience \"someone-else\" but this server is configured for \
             audience \"{OIDC_AUDIENCE}\" — mint the token for the configured audience or fix \
             OidcConfig::audience"
        )
    );

    // 7. alg "HS256" in the header — unsupported-alg, never reaches the
    //    (bogus) signature check.
    let hs256_header = serde_json::json!({"alg": "HS256", "typ": "JWT", "kid": TEST_KID});
    let hs256_jwt = mint_jwt(&private, &hs256_header, &valid_claims());
    assert_eq!(
        verifier
            .verify(&hs256_jwt, OIDC_NOW)
            .unwrap_err()
            .to_string(),
        "unsupported OIDC token alg \"HS256\" — only \"RS256\" is verified; mint the token \
         with an RS256-signed key"
    );

    // 8. Not three segments.
    assert_eq!(
        verifier
            .verify("not.enough", OIDC_NOW)
            .unwrap_err()
            .to_string(),
        "malformed OIDC token — expected \"header.payload.signature\" (three dot-separated \
         segments); got 2 segment(s)"
    );

    // 9. End to end: GET /me with the valid JWT bearer on an
    //    App::with_oidc app → 200 "bob".
    let (_sim, mut app) = oidc_app(0x0D1C_0001, &public);
    let ok = app.handle_at(
        Request::new(Method::Get, "/me").header("Authorization", format!("Bearer {valid_jwt}")),
        OIDC_NOW,
    );
    assert_eq!(ok.status, 200, "{}", body_text(&ok));
    assert_eq!(body_text(&ok), "\"bob\"");

    // 10. The OIDC rung is scoped to the Authorization bearer header ONLY
    //     (spec §2 rung 1): a cookie value with two dots, even with a
    //     verifier mounted, always goes down the hash path — never
    //     OIDC-verified — so it lands on the unrecognized-credential 401.
    let cookie_two_dots = app.handle_at(
        Request::new(Method::Get, "/me").header("Cookie", "charpente_session=a.b.c"),
        OIDC_NOW,
    );
    assert_eq!(
        cookie_two_dots.status,
        401,
        "{}",
        body_text(&cookie_two_dots)
    );
    assert_eq!(
        error_text(&cookie_two_dots),
        "unrecognized credential — it may have been revoked; obtain a new one"
    );
}

#[test]
fn oidc_config_errors_are_named_at_mount() {
    let bad_json = OidcConfig {
        issuer: OIDC_ISSUER.into(),
        audience: OIDC_AUDIENCE.into(),
        jwks_json: "not json".into(),
    };
    let err = charpente::auth::OidcVerifier::from_config(bad_json).unwrap_err();
    let OidcConfigError::MalformedJwks(_) = &err else {
        panic!("expected MalformedJwks, got {err:?}");
    };
    assert!(
        err.to_string()
            .starts_with("OidcConfig::jwks_json is not valid JSON: "),
        "{err}"
    );

    let no_keys = OidcConfig {
        issuer: OIDC_ISSUER.into(),
        audience: OIDC_AUDIENCE.into(),
        jwks_json: serde_json::json!({"keys": []}).to_string(),
    };
    let err = charpente::auth::OidcVerifier::from_config(no_keys).unwrap_err();
    assert_eq!(
        err.to_string(),
        "OidcConfig::jwks_json has no keys — the \"keys\" array is empty; add at least one \
         RSA key"
    );

    let non_rsa = OidcConfig {
        issuer: OIDC_ISSUER.into(),
        audience: OIDC_AUDIENCE.into(),
        jwks_json: serde_json::json!({"keys": [{"kty": "EC", "kid": "k1", "n": "x", "e": "y"}]})
            .to_string(),
    };
    let err = charpente::auth::OidcVerifier::from_config(non_rsa).unwrap_err();
    assert_eq!(
        err.to_string(),
        "OIDC JWKS key k1 has kty \"EC\" — only \"RSA\" is supported in v1; remove non-RSA \
         keys from the JWKS"
    );

    let bad_alg = OidcConfig {
        issuer: OIDC_ISSUER.into(),
        audience: OIDC_AUDIENCE.into(),
        jwks_json: serde_json::json!({
            "keys": [{"kty": "RSA", "kid": "k1", "alg": "RS384", "n": "x", "e": "y"}]
        })
        .to_string(),
    };
    let err = charpente::auth::OidcVerifier::from_config(bad_alg).unwrap_err();
    assert_eq!(
        err.to_string(),
        "OIDC JWKS key k1 has alg \"RS384\" — only \"RS256\" is supported; fix or remove the \
         key's \"alg\" field"
    );

    let bad_n = OidcConfig {
        issuer: OIDC_ISSUER.into(),
        audience: OIDC_AUDIENCE.into(),
        jwks_json: serde_json::json!({
            "keys": [{"kty": "RSA", "kid": "k1", "n": "not-base64url!!", "e": "AQAB"}]
        })
        .to_string(),
    };
    let err = charpente::auth::OidcVerifier::from_config(bad_n).unwrap_err();
    assert!(
        err.to_string()
            .starts_with("OIDC JWKS key k1 is invalid: modulus \"n\" is not valid base64url: "),
        "{err}"
    );
}

#[test]
fn one_credential_mint_per_request() {
    let (_sim, mut app) = key_app(0x47F);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        app.handle(Request::new(Method::Post, "/double-mint"))
    }));
    let payload = match result {
        Ok(_) => panic!("minting two credentials in one request must panic"),
        Err(payload) => payload,
    };
    let message = payload
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| payload.downcast_ref::<&str>().map(|s| s.to_string()))
        .expect("panic payload should carry the message string");
    assert!(
        message.contains("one credential mint per request"),
        "{message}"
    );
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
    let handled = app.handle_or_subscribe(
        Request::new(Method::Get, "/feed").header("last-event-id", "5.9"),
        0,
    );
    let Handled::Subscribe(sub) = handled else {
        panic!("expected Subscribe from an SSE route")
    };
    assert_eq!(sub.cursor, Some(Cursor { id: 5, seq: 9 }));

    // Same result from the `cursor` query parameter with no header.
    let handled = app.handle_or_subscribe(Request::new(Method::Get, "/feed?cursor=5.9"), 0);
    let Handled::Subscribe(sub) = handled else {
        panic!("expected Subscribe from an SSE route")
    };
    assert_eq!(sub.cursor, Some(Cursor { id: 5, seq: 9 }));

    // An unparseable cursor is a 400 naming the contract — the handler never runs.
    let handled = app.handle_or_subscribe(
        Request::new(Method::Get, "/feed").header("last-event-id", "garbage"),
        0,
    );
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
        ..AppConfig::default()
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

// Narrower than the shared 4-family `checks::classify` (vs kernel.rs, which
// uses it directly): this workload breaks and restarts on the FIRST Io
// error, so no second op ever reaches a poisoned WAL or redb's latched
// handle — Poisoned/RedbLatch cannot surface, and recognizing them here
// would only ever LOOSEN this oracle. The reopen-time state checks remain
// the oracle either way. Injected and DurableHorizon only (the latter a
// legitimate fault-matrix consequence, not a bug, same as transactions.rs).
fn is_injected(text: &str) -> bool {
    matches!(
        charpente_sim::checks::classify(text),
        charpente_sim::checks::FaultReason::Injected
            | charpente_sim::checks::FaultReason::DurableHorizon
    )
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
