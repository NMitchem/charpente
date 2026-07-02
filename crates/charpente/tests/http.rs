//! The HTTP kernel under the sim (PLAN.md M0 step 5): routing, decoding,
//! sessions, error mapping — unit tests here; the macro end-to-end tests and
//! the request-level crash property are appended by later plan tasks.

use charpente::http::{
    ApiError, App, AppConfig, Ctx, Method, Request, Response, Route, decode_body,
};
use charpente::{Schema, Tx, table};
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

// Hand-written glue mirroring what #[route] generates (Task 4 swaps these).
fn create_note_glue(
    tx: &mut Tx,
    ctx: &Ctx,
    _captures: &[String],
    body: &[u8],
) -> Result<Response, ApiError> {
    let body: CreateNote = decode_body(body)?;
    Ok(Response::json(200, &create_note(tx, body, ctx)?))
}

fn get_note_glue(
    tx: &mut Tx,
    _ctx: &Ctx,
    captures: &[String],
    _body: &[u8],
) -> Result<Response, ApiError> {
    use charpente::http::FromParam;
    use charpente_core::table::View;
    let id: Id<Note> = Id::from_param("id", &captures[0])?;
    let note = tx.get(id)?.ok_or(ApiError::NotFound)?;
    Ok(Response::json(200, &note.text))
}

fn login_glue(
    tx: &mut Tx,
    ctx: &Ctx,
    _captures: &[String],
    body: &[u8],
) -> Result<Response, ApiError> {
    let subject: String = decode_body(body)?;
    let token = ctx.create_session(tx, subject);
    Ok(Response::json(200, &token))
}

fn whoami_glue(
    _tx: &mut Tx,
    ctx: &Ctx,
    _captures: &[String],
    _body: &[u8],
) -> Result<Response, ApiError> {
    Ok(Response::json(
        200,
        &ctx.session().expect("auth route").subject,
    ))
}

fn logout_glue(
    tx: &mut Tx,
    ctx: &Ctx,
    _captures: &[String],
    _body: &[u8],
) -> Result<Response, ApiError> {
    ctx.revoke(tx);
    Ok(Response::json(200, &"bye"))
}

fn note_routes() -> Vec<Route> {
    vec![
        Route {
            method: Method::Post,
            pattern: "/notes",
            auth: false,
            name: "create_note",
            glue: create_note_glue,
        },
        Route {
            method: Method::Get,
            pattern: "/notes/:id",
            auth: false,
            name: "get_note",
            glue: get_note_glue,
        },
        Route {
            method: Method::Post,
            pattern: "/login",
            auth: false,
            name: "login",
            glue: login_glue,
        },
        Route {
            method: Method::Get,
            pattern: "/me",
            auth: true,
            name: "whoami",
            glue: whoami_glue,
        },
        Route {
            method: Method::Post,
            pattern: "/logout",
            auth: true,
            name: "logout",
            glue: logout_glue,
        },
    ]
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
