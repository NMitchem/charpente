//! Oracle 7 (outbound-effects spec §12): the stock launch-driven binary
//! against a local fake HTTPS remote on a local CA, over real TCP, under
//! `kill -9` and TCP-proxy faults. The fake dedupes by idempotency key, so
//! "applied once" is measured where the money would move.
//!
//! Outside DESIGN §1's determinism fence, as this whole crate is: real
//! `TcpStream`, `Command`, `std::fs` and `thread::sleep` calls are the point,
//! not a violation; the allow below matches `child.rs`/`scenario.rs`/`proxy.rs`'s.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use edge_harness::child::{ChildNode, build_bins};
use edge_harness::client::Http;
use edge_harness::proxy::TcpProxy;
use edge_harness::scenario::Scenario;

const CA: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../outbound-fixture/tls/ca.pem"
);

const PAID_TIMEOUT: Duration = Duration::from_secs(30);
const FIRST_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const POLL_INTERVAL: Duration = Duration::from_millis(20);
/// The bucket `fake_remote` counts a request with no `idempotency-key` under.
const KEYLESS: &str = "<none>";

/// Named `edge-harness-*` under the temp dir: the nightly workflow uploads
/// exactly that glob when a run fails.
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

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("loopback bind")
        .local_addr()
        .expect("bound listener has an address")
        .port()
}

/// `fake_remote`'s `/stats`, by key.
#[derive(Debug, Default)]
struct Stats {
    requests: BTreeMap<String, u64>,
    applied: BTreeMap<String, u64>,
}

/// One scenario: its script and directory, and a running `fake_remote`.
struct Rig {
    root: PathBuf,
    /// The scenario's artifacts directory, named in every assertion message.
    dir: String,
    scenario: Scenario,
    outbound_node: PathBuf,
    _fake: ChildNode,
    /// The fake's TLS address, `pay.fixture`'s real home.
    fake_tls: String,
    admin: Http,
}

impl Rig {
    /// A fresh scenario and a `fake_remote` that holds each key's first
    /// charge `hold_ms` before answering.
    fn open(name: &str, hold_ms: u64) -> Rig {
        let bins = build_bins().expect("build_bins");
        let root = temp_dir("outbound");
        std::fs::create_dir_all(&root).expect("scenario root dir");
        let mut scenario = Scenario::new(&root, name).expect("Scenario::new");
        let dir = scenario.dir().display().to_string();
        let (tls_port, admin_port) = (free_port(), free_port());
        let admin = format!("127.0.0.1:{admin_port}");
        let args = [
            "--tls-port".to_string(),
            tls_port.to_string(),
            "--admin-port".to_string(),
            admin_port.to_string(),
            "--hold-ms".to_string(),
            hold_ms.to_string(),
        ];
        let fake = ChildNode::spawn_env(&bins.fake_remote, &args, &[], &admin, &mut scenario)
            .unwrap_or_else(|e| panic!("spawn fake_remote: {e} (artifacts: {dir})"));
        Rig {
            root,
            dir,
            scenario,
            outbound_node: bins.outbound_node,
            _fake: fake,
            fake_tls: format!("127.0.0.1:{tls_port}"),
            admin: Http::new(admin),
        }
    }

    /// `outbound_node` on a fresh port and a data dir in the scenario's,
    /// reaching `pay.fixture` at `target`, trusting the test CA only when
    /// `trust_ca`.
    fn node(&mut self, target: &str, trust_ca: bool) -> (ChildNode, Http) {
        let addr = format!("127.0.0.1:{}", free_port());
        let mut args = vec![
            "--outbound-base".to_string(),
            format!("pay.fixture=https://{target}"),
        ];
        if trust_ca {
            args.extend(["--outbound-ca".to_string(), CA.to_string()]);
        }
        let data = self.scenario.dir().join("data").display().to_string();
        let env = [
            ("OUTBOUND_NODE_ADDR", addr.clone()),
            ("OUTBOUND_NODE_DIR", data),
            ("FIXTURE_PAY_KEY", "test-key".to_string()),
        ];
        let node =
            ChildNode::spawn_env(&self.outbound_node, &args, &env, &addr, &mut self.scenario)
                .unwrap_or_else(|e| panic!("spawn outbound_node: {e} (artifacts: {})", self.dir));
        (node, Http::new(addr))
    }

    fn stats(&mut self) -> Stats {
        let dir = &self.dir;
        let resp = self
            .admin
            .get("/stats")
            .unwrap_or_else(|e| panic!("GET /stats: {e} (artifacts: {dir})"));
        let body = String::from_utf8_lossy(&resp.body).into_owned();
        assert_eq!(resp.status, 200, "GET /stats: {body:?} (artifacts: {dir})");
        let mut stats = Stats::default();
        for line in body.lines() {
            let words: Vec<&str> = line.split_whitespace().collect();
            let (counts, key, n) = match words[..] {
                ["requests", key, n] => (&mut stats.requests, key, n),
                ["applied", key, n] => (&mut stats.applied, key, n),
                _ => panic!(
                    "a /stats line is `requests|applied <key> <n>`, got {line:?} (artifacts: {dir})"
                ),
            };
            let n = n
                .parse()
                .unwrap_or_else(|_| panic!("a /stats count, got {line:?} (artifacts: {dir})"));
            counts.insert(key.to_string(), n);
        }
        stats
    }

    /// Polls `/stats` until the fake has seen a charge request.
    fn await_first_request(&mut self) {
        let deadline = Instant::now() + FIRST_REQUEST_TIMEOUT;
        loop {
            let stats = self.stats();
            if stats.requests.values().sum::<u64>() > 0 {
                self.scenario
                    .record(format!("the fake saw the first charge: {stats:?}"));
                return;
            }
            assert!(
                Instant::now() < deadline,
                "no charge request reached the fake within {FIRST_REQUEST_TIMEOUT:?} \
                 (artifacts: {})",
                self.dir
            );
            std::thread::sleep(POLL_INTERVAL);
        }
    }

    /// `POST /orders`, which enqueues the order's charge; the order's id.
    fn place_order(&mut self, node: &mut Http) -> String {
        let dir = &self.dir;
        let resp = node
            .post("/orders", "")
            .unwrap_or_else(|e| panic!("POST /orders: {e} (artifacts: {dir})"));
        let body = String::from_utf8_lossy(&resp.body).into_owned();
        assert_eq!(
            resp.status, 200,
            "POST /orders: {body:?} (artifacts: {dir})"
        );
        let id = resp.as_json_string().unwrap_or_else(|| {
            panic!("POST /orders answers the id as a JSON string, got {body:?} (artifacts: {dir})")
        });
        self.scenario.record(format!("POST /orders: order {id}"));
        id
    }

    /// `GET /orders/<id>`'s body.
    fn order(&self, node: &mut Http, id: &str) -> String {
        let dir = &self.dir;
        let resp = node
            .get(&format!("/orders/{id}"))
            .unwrap_or_else(|e| panic!("GET /orders/{id}: {e} (artifacts: {dir})"));
        let body = String::from_utf8_lossy(&resp.body).into_owned();
        assert_eq!(
            resp.status, 200,
            "GET /orders/{id}: {body:?} (artifacts: {dir})"
        );
        body
    }

    fn await_paid(&mut self, node: &mut Http, id: &str) {
        let deadline = Instant::now() + PAID_TIMEOUT;
        loop {
            let body = self.order(node, id);
            if body.contains(r#""paid":true"#) {
                self.scenario.record(format!("order {id} paid: {body}"));
                return;
            }
            assert!(
                Instant::now() < deadline,
                "order {id} was not paid within {PAID_TIMEOUT:?}: last read {body:?}, fake's \
                 stats {:?} (artifacts: {})",
                self.stats(),
                self.dir
            );
            std::thread::sleep(POLL_INTERVAL);
        }
    }

    /// The oracle, read at the remote, for a scenario that placed ONE order
    /// and saw it paid: one charge applied in all, under one key, never
    /// keyless. The total comes first: the fake applies a key at most once,
    /// so a key-per-attempt regression shows only as a second key's
    /// application, and a keyless one only under `<none>`. Returns how many
    /// requests carried that key.
    fn assert_charged_once(&mut self) -> u64 {
        let stats = self.stats();
        let dir = &self.dir;
        assert_eq!(
            stats.applied.values().sum::<u64>(),
            1,
            "one order was placed, so the fake must have applied exactly one charge in all: \
             {stats:?} (artifacts: {dir})"
        );
        assert!(
            !stats.requests.contains_key(KEYLESS) && !stats.applied.contains_key(KEYLESS),
            "a charge reached the fake with no idempotency-key header: {stats:?} \
             (artifacts: {dir})"
        );
        assert_eq!(
            stats.requests.len(),
            1,
            "every attempt at the one order's charge must carry the same key: {stats:?} \
             (artifacts: {dir})"
        );
        let (key, &requests) = stats
            .requests
            .iter()
            .next()
            .expect("exactly one key, asserted above");
        assert_eq!(
            stats.applied.get(key),
            Some(&1),
            "the one key the fake saw must be the one it applied: {stats:?} (artifacts: {dir})"
        );
        self.scenario.record(format!("charged once: {stats:?}"));
        requests
    }

    /// Records the pass, dumps the script, and removes the scenario's files;
    /// a failure panics before this, so its files stay where its message
    /// points.
    fn finish(mut self) {
        self.scenario.record("passed");
        self.scenario.dump().expect("scenario dump");
        drop(self._fake);
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

#[test]
#[ignore]
fn an_order_is_charged_once_over_real_tls() {
    let mut rig = Rig::open("an_order_is_charged_once_over_real_tls", 0);
    let target = rig.fake_tls.clone();
    let (node, mut http) = rig.node(&target, true);
    let id = rig.place_order(&mut http);
    rig.await_paid(&mut http, &id);
    let requests = rig.assert_charged_once();
    assert_eq!(
        requests, 1,
        "nothing failed, so one request was enough (artifacts: {})",
        rig.dir
    );
    drop(node);
    rig.finish();
}

#[test]
#[ignore]
fn kill_9_with_a_call_in_flight_re_calls_with_the_same_key() {
    // The fake applies the charge, then holds its reply 3 s: the kill lands
    // after the money moved and before the node heard.
    let mut rig = Rig::open(
        "kill_9_with_a_call_in_flight_re_calls_with_the_same_key",
        3000,
    );
    let target = rig.fake_tls.clone();
    let (mut node, mut http) = rig.node(&target, true);
    let id = rig.place_order(&mut http);
    rig.await_first_request();
    node.kill9(&mut rig.scenario);
    node.restart(&mut rig.scenario)
        .unwrap_or_else(|e| panic!("restart outbound_node: {e} (artifacts: {})", rig.dir));
    rig.await_paid(&mut http, &id);
    let requests = rig.assert_charged_once();
    assert!(
        requests >= 2,
        "the first call's reply died with the node, so paid means the restarted node \
         re-called — yet the fake saw {requests} request(s) (artifacts: {})",
        rig.dir
    );
    drop(node);
    rig.finish();
}

#[test]
#[ignore]
fn a_reset_between_node_and_remote_is_retried_with_the_same_key() {
    let mut rig = Rig::open(
        "a_reset_between_node_and_remote_is_retried_with_the_same_key",
        1000,
    );
    let proxy = TcpProxy::start(&rig.fake_tls).expect("start proxy");
    let (node, mut http) = rig.node(&proxy.addr(), true);
    let id = rig.place_order(&mut http);
    rig.await_first_request();
    proxy.drop_all(&mut rig.scenario);
    rig.await_paid(&mut http, &id);
    let requests = rig.assert_charged_once();
    assert!(
        requests >= 2,
        "the reset severed the first call inside the fake's 1 s hold, so paid means a \
         re-call — yet the fake saw {requests} request(s) (artifacts: {})",
        rig.dir
    );
    proxy.stop(&mut rig.scenario);
    drop(node);
    rig.finish();
}

#[test]
#[ignore]
fn a_stalled_remote_times_out_and_is_retried() {
    let mut rig = Rig::open("a_stalled_remote_times_out_and_is_retried", 1000);
    let proxy = TcpProxy::start(&rig.fake_tls).expect("start proxy");
    let (node, mut http) = rig.node(&proxy.addr(), true);
    let id = rig.place_order(&mut http);
    rig.await_first_request();
    // A stall, not a partition: `partition` shuts each new connection at
    // once, which is a connect error and so the refusal path, never a
    // deadline. Every chunk now crawls, so the held reply misses ChargeOrder's
    // 2 s deadline, and a re-call's handshake stalls the same way.
    proxy.delay_ms(3000, &mut rig.scenario);
    std::thread::sleep(Duration::from_secs(5));
    proxy.delay_ms(0, &mut rig.scenario);
    rig.await_paid(&mut http, &id);
    let requests = rig.assert_charged_once();
    assert!(
        requests >= 2,
        "the stalled reply could not beat the 2 s deadline, so paid means a deadline fired \
         and the job re-called — yet the fake saw {requests} request(s) (artifacts: {})",
        rig.dir
    );
    proxy.stop(&mut rig.scenario);
    drop(node);
    rig.finish();
}

#[test]
#[ignore]
fn an_unset_credential_refuses_the_start() {
    let bins = build_bins().expect("build_bins");
    let root = temp_dir("outbound");
    std::fs::create_dir_all(&root).expect("scenario root dir");
    let mut scenario =
        Scenario::new(&root, "an_unset_credential_refuses_the_start").expect("Scenario::new");
    let dir = scenario.dir().display().to_string();
    let stderr_path = scenario.dir().join("node-stderr.log");
    // Spawned by hand, not through `ChildNode::spawn_env`: that waits for the
    // port, which this node never opens — the point is that it exits. The
    // credential is removed outright, so an inherited value cannot mask the
    // refusal.
    let mut child = Command::new(&bins.outbound_node)
        .args([
            "--outbound-base",
            "pay.fixture=https://127.0.0.1:9",
            "--outbound-ca",
            CA,
        ])
        .env("OUTBOUND_NODE_ADDR", format!("127.0.0.1:{}", free_port()))
        .env("OUTBOUND_NODE_DIR", scenario.dir().join("data"))
        .env_remove("FIXTURE_PAY_KEY")
        .stdout(Stdio::null())
        .stderr(Stdio::from(
            std::fs::File::create(&stderr_path).expect("stderr file"),
        ))
        .spawn()
        .expect("spawn outbound_node");
    scenario.record(format!(
        "spawn {} without FIXTURE_PAY_KEY (pid {})",
        bins.outbound_node.display(),
        child.id()
    ));
    let deadline = Instant::now() + Duration::from_secs(15);
    let status = loop {
        if let Some(status) = child.try_wait().expect("try_wait") {
            break status;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("still running 15 s after a start with its credential unset (artifacts: {dir})");
        }
        std::thread::sleep(POLL_INTERVAL);
    };
    let stderr = std::fs::read_to_string(&stderr_path).expect("stderr");
    scenario.record(format!("exited {status}: {stderr:?}"));
    assert_eq!(
        status.code(),
        Some(1),
        "{status}, stderr {stderr:?} (artifacts: {dir})"
    );
    assert!(
        stderr.contains(
            "charpente refused to start: outbound: FIXTURE_PAY_KEY is unset — host pay.fixture \
             needs it for job ChargeOrder"
        ),
        "{stderr:?} (artifacts: {dir})"
    );
    scenario.record("passed");
    scenario.dump().expect("scenario dump");
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
#[ignore]
fn without_the_test_ca_no_request_reaches_the_remote() {
    let mut rig = Rig::open("without_the_test_ca_no_request_reaches_the_remote", 0);
    let target = rig.fake_tls.clone();
    let (node, mut http) = rig.node(&target, false);
    let id = rig.place_order(&mut http);
    std::thread::sleep(Duration::from_secs(5));
    rig.scenario.record("waited 5 s");
    // `an_order_is_charged_once_over_real_tls` is the same rig with the CA,
    // and is paid: an empty ledger here is TLS verification refusing the fake.
    let stats = rig.stats();
    assert!(
        stats.requests.is_empty() && stats.applied.is_empty(),
        "the fake's certificate chains to a CA the node was not given, so no request may \
         reach it: {stats:?} (artifacts: {})",
        rig.dir
    );
    let order = rig.order(&mut http, &id);
    assert_eq!(
        order, r#"{"paid":false,"charge":""}"#,
        "no charge reached the remote, so the order stays unpaid (artifacts: {})",
        rig.dir
    );
    rig.scenario.record(format!("order {id} unpaid: {order}"));
    drop(node);
    rig.finish();
}
