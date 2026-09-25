//! The T10 exit criterion: one invoicing `node` behind a `TcpProxy` serves a
//! login, a write, and a read, survives `kill -9`, and serves the same read
//! again after a plain restart against the SAME data directory — proving ack
//! ordering and crash recovery end-to-end over real TCP. Per the crate's
//! honesty contract: `kill -9` is not power loss (the OS page cache survives
//! it), so this proves the restart path, never durability against media
//! failure — that claim stays the simulator's.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use edge_harness::{ChildNode, Http, Scenario, TcpProxy, build_bins};

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

/// Reserves a loopback port by binding to `:0` and dropping the listener —
/// the node needs a knowable, fixed `--addr` up front so the restarted
/// process can rebind to the exact same address the proxy forwards to.
fn free_addr() -> String {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("loopback bind")
        .local_addr()
        .expect("bound listener has an address")
        .to_string()
}

#[test]
#[ignore]
fn smoke_single_node_serves_and_survives_restart() {
    let bins = build_bins().expect("build_bins");

    let scenario_root = temp_dir("scenario-root");
    std::fs::create_dir_all(&scenario_root).expect("scenario root dir");
    let mut scenario = Scenario::new(
        &scenario_root,
        "smoke_single_node_serves_and_survives_restart",
    )
    .expect("Scenario::new");

    let data_dir = temp_dir("data");
    let node_addr = free_addr();
    let args = vec![
        "--addr".to_string(),
        node_addr.clone(),
        "--data-dir".to_string(),
        data_dir.to_str().unwrap().to_string(),
    ];

    let mut node = ChildNode::spawn(&bins.node, &args, &mut scenario).expect("spawn node");
    let proxy = TcpProxy::start(&node_addr).expect("start proxy");

    let mut http = Http::new(proxy.addr());

    let login = http.post("/login", "\"edge-tester\"").expect("login");
    assert_eq!(
        login.status,
        200,
        "login: {}",
        String::from_utf8_lossy(&login.body)
    );
    let token = login
        .as_json_string()
        .expect("login's body is a JSON string token");
    http.set_session(token);
    scenario.record("logged in through the proxy; session established");

    let create = http
        .post(
            "/customers/1",
            r#"{"name":"Ada Lovelace","email":"ada@example.com"}"#,
        )
        .expect("create customer");
    assert_eq!(
        create.status,
        200,
        "create customer: {}",
        String::from_utf8_lossy(&create.body)
    );
    scenario.record("POSTed customer 1 through the proxy");

    let get1 = http.get("/customers/1").expect("get customer");
    assert_eq!(
        get1.status,
        200,
        "get customer: {}",
        String::from_utf8_lossy(&get1.body)
    );
    let body1 = String::from_utf8_lossy(&get1.body).to_string();
    assert!(body1.contains("Ada Lovelace"), "{body1}");
    scenario.record("GET customer 1 through the proxy — present before the kill");

    node.kill9(&mut scenario);
    node.restart(&mut scenario).expect("restart node");

    let get2 = http
        .get("/customers/1")
        .expect("get customer after restart");
    assert_eq!(
        get2.status,
        200,
        "get customer after restart: {}",
        String::from_utf8_lossy(&get2.body)
    );
    let body2 = String::from_utf8_lossy(&get2.body).to_string();
    assert!(body2.contains("Ada Lovelace"), "{body2}");
    scenario.record("GET customer 1 through the proxy — survived kill9 + restart");

    let script = scenario.dump().expect("scenario dump");
    assert!(
        script.exists(),
        "scenario script must exist: {}",
        script.display()
    );
    eprintln!("scenario script: {}", script.display());

    let _ = std::fs::remove_dir_all(&data_dir);
    let _ = std::fs::remove_dir_all(&scenario_root);
}

/// Spec §5 at the process boundary: a node whose kernel cannot start exits
/// nonzero with its named line, instead of parking behind an HTTP edge that
/// answers 500. The startup failure is a corrupt role file (amendment A13):
/// the one a stock binary can be handed from outside.
#[test]
#[ignore]
fn a_node_whose_kernel_cannot_start_exits_nonzero_with_its_named_line() {
    let bins = build_bins().expect("build_bins");
    let data_dir = temp_dir("corrupt-role");
    std::fs::create_dir_all(&data_dir).expect("data dir");
    std::fs::write(data_dir.join("role"), "primary\n00000000").expect("plant the role file");
    let stderr_path = temp_dir("corrupt-role-stderr");
    // failover.rs's replicated-primary argv. Spawned by hand, not through
    // ChildNode::spawn: that waits for the port, which this node never opens
    // for long — the whole point is that it exits.
    let mut child = std::process::Command::new(&bins.node)
        .args([
            "--addr".to_string(),
            free_addr(),
            "--data-dir".to_string(),
            data_dir.to_str().unwrap().to_string(),
            "--repl-listen".to_string(),
            free_addr(),
            "--role".to_string(),
            "primary".to_string(),
            "--secret".to_string(),
            "edge-harness-startup-exit-secret".to_string(),
        ])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::from(
            std::fs::File::create(&stderr_path).expect("stderr file"),
        ))
        .spawn()
        .expect("spawn node");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    let status = loop {
        if let Some(status) = child.try_wait().expect("try_wait") {
            break status;
        }
        if std::time::Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!(
                "still running 30s after its kernel could not start — the zombie spec §5 removes"
            );
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    };
    assert_eq!(status.code(), Some(1), "{status}");
    let stderr = std::fs::read_to_string(&stderr_path).expect("stderr");
    assert!(
        stderr.contains(
            "charpente replication failed to start — fix the role file and restart: role file \
             failed its integrity check (crc mismatch)"
        ),
        "{stderr}"
    );
    let _ = std::fs::remove_dir_all(&data_dir);
    let _ = std::fs::remove_file(&stderr_path);
}
