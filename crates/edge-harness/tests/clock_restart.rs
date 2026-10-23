//! Restart-stable time (0.3.0 spec §5 B): production `now` was process
//! uptime, so a restart rewound every stored time. Three wire oracles over
//! `clock_node`: a schedule keeps ticking after a restart, a session past its
//! TTL is refused after one, and an OIDC token is judged on Unix time.
//!
//! Outside DESIGN §1's determinism fence, as this whole crate is.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use std::path::PathBuf;
use std::thread::sleep;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use edge_harness::child::{ChildNode, build_bins};
use edge_harness::client::Http;
use edge_harness::scenario::Scenario;

fn temp_dir(tag: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "edge-harness-{tag}-{}-{nanos:x}",
        std::process::id()
    ))
}

fn free_addr() -> String {
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .expect("loopback bind")
        .local_addr()
        .expect("bound listener has an address")
        .port();
    format!("127.0.0.1:{port}")
}

fn unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

/// A running `clock_node` (session TTL 2 s) and a client for it.
fn start(name: &str) -> (Scenario, ChildNode, Http) {
    let bins = build_bins().expect("build_bins");
    let root = temp_dir("clock");
    std::fs::create_dir_all(&root).expect("scenario root dir");
    let mut scenario = Scenario::new(&root, name).expect("Scenario::new");
    let addr = free_addr();
    let data = scenario.dir().join("data").display().to_string();
    let env = [
        ("CLOCK_NODE_ADDR", addr.clone()),
        ("CLOCK_NODE_DIR", data),
        ("CLOCK_NODE_SESSION_TTL_SECS", "2".to_string()),
    ];
    let node = ChildNode::spawn_env(&bins.clock_node, &[], &env, &addr, &mut scenario)
        .unwrap_or_else(|e| panic!("spawn clock_node: {e}"));
    (scenario, node, Http::new(addr))
}

fn ticks(http: &mut Http, dir: &str) -> u64 {
    let resp = http
        .get("/ticks")
        .unwrap_or_else(|e| panic!("GET /ticks: {e} (artifacts: {dir})"));
    let body = String::from_utf8_lossy(&resp.body).into_owned();
    assert_eq!(resp.status, 200, "GET /ticks: {body:?} (artifacts: {dir})");
    body.trim_matches('"')
        .parse()
        .unwrap_or_else(|_| panic!("GET /ticks body {body:?} is not a count (artifacts: {dir})"))
}

#[test]
#[ignore = "builds release binaries and drives real processes; run via scripts/edge-harness.sh"]
fn a_schedule_ticks_within_an_interval_after_a_restart() {
    let (mut scenario, mut node, mut http) = start("clock-cron");
    let dir = scenario.dir().display().to_string();
    sleep(Duration::from_secs(6));
    let before = ticks(&mut http, &dir);
    assert!(
        before >= 4,
        "a 1 s schedule ticked {before} times in 6 s (artifacts: {dir})"
    );
    node.kill9(&mut scenario);
    node.restart(&mut scenario).expect("restart");
    let base = ticks(&mut http, &dir);
    sleep(Duration::from_millis(2500));
    let after = ticks(&mut http, &dir);
    assert!(
        after > base,
        "the 1 s schedule did not tick within 2.5 s of a restart: {base} at restart, {after} after (artifacts: {dir})"
    );
}

#[test]
#[ignore = "builds release binaries and drives real processes; run via scripts/edge-harness.sh"]
fn a_session_past_its_ttl_is_refused_after_a_restart() {
    let (mut scenario, mut node, mut http) = start("clock-session");
    let dir = scenario.dir().display().to_string();
    sleep(Duration::from_secs(6));
    let login = http.post("/login", "{}").expect("POST /login");
    assert_eq!(login.status, 200, "POST /login (artifacts: {dir})");
    http.set_session(login.as_json_string().expect("login body is the token"));
    let me = http.get("/me").expect("GET /me");
    assert_eq!(
        me.status, 200,
        "GET /me before the restart (artifacts: {dir})"
    );
    node.kill9(&mut scenario);
    node.restart(&mut scenario).expect("restart");
    sleep(Duration::from_secs(3));
    let me = http.get("/me").expect("GET /me after the restart");
    let body = String::from_utf8_lossy(&me.body).into_owned();
    assert_eq!(
        (me.status, body.contains("session expired — log in again")),
        (401, true),
        "a 2 s session, 3 s after a restart: {} {body:?} (artifacts: {dir})",
        me.status
    );
}

#[test]
#[ignore = "builds release binaries and drives real processes; run via scripts/edge-harness.sh"]
fn an_oidc_token_is_judged_on_unix_time() {
    let (scenario, _node, mut http) = start("clock-oidc");
    let dir = scenario.dir().display().to_string();
    let now = unix_secs();

    http.set_bearer(clock_fixture::mint_jwt(now - 3600, None));
    let expired = http.get("/me").expect("GET /me, expired token");
    let expired_body = String::from_utf8_lossy(&expired.body).into_owned();
    http.set_bearer(clock_fixture::mint_jwt(now + 3600, Some(now - 60)));
    let ok = http.get("/me").expect("GET /me, valid token");
    let ok_body = String::from_utf8_lossy(&ok.body).into_owned();
    // The body is the `{"error": …}` JSON envelope, so the claim's quotes
    // arrive escaped.
    let expired_msg = expired_body
        .contains(r#"OIDC token expired — its \"exp\" claim is in the past; obtain a new one"#);
    assert_eq!(
        ((expired.status, expired_msg), ok.status),
        ((401, true), 200),
        "exp an hour ago: {} {expired_body:?}; exp in an hour, nbf a minute ago: {} {ok_body:?} (artifacts: {dir})",
        expired.status,
        ok.status
    );
}
