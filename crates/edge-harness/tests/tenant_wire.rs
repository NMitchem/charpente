//! The two-tenant wire oracle (M2 tenant-scoping sub-project, task 12): the
//! sim proves the kernel, this proves the wire. Two tenants with real
//! sessions against a stock `tenant_node` binary over real TCP — tenant A's
//! session receives none of tenant B's rows over HTTP and none of B's events
//! over SSE, and an older follower meeting a scoped log record refuses it BY
//! NAME rather than misreading it.
//!
//! **Every assertion here is two-sided, and that is not decoration.** A
//! one-sided "A never sees B" passes vacuously when A receives nothing at all
//! — which is exactly how the in-sim event property first failed (task 11:
//! the unkeyed emit reached NOBODY, so `left: []` satisfied every
//! non-leak assertion). So each tenant must receive its OWN rows and events
//! as well as not receiving the other's, and both tenants use the SAME
//! document ids and the SAME `#[index]` tag value: separation here is the
//! scope's job, never the id's and never the tag's. A leak and a
//! fail-closed break are different failures and this file names them
//! differently.
//!
//! **The mixed-version arm (ruling R60).** Tenant scoping changed the log
//! format — a scoped row write is tag `0x05`, carrying a scope AND an id
//! where the unscoped `0x00` carries only a row. A follower on the older
//! shape cannot know that: its `decode_record` reads the fixed fields it
//! knows, so misreading is silent data corruption rather than a crash. The
//! guarantee is that it refuses by name instead, and this arm drives that
//! over real TCP with a real primary shipping real frames. `tenant_node
//! --legacy-unscoped` is the older shape (the same table name and fields,
//! no `scoped_by`); the pair is reachable at all because the replication
//! handshake's schema digest covers a table's fields and version but not its
//! scopedness, which is precisely why the record-level refusal has to exist.
//! The control on the same bytes is a CURRENT follower converging on the
//! same record — without it, "the legacy follower stopped" would be
//! consistent with a broken harness (a wrong secret, a wrong address, a node
//! that never connected at all).
//!
//! **No `TcpProxy` here, deliberately.** This oracle injects no faults: what
//! it is testing is what a real client on a real socket can obtain, and the
//! proxy would only add a hop. `failover.rs` and `smoke.rs` keep the fault
//! surface.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use std::collections::BTreeSet;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use edge_harness::{Bins, ChildNode, Http, Scenario, SseTail, build_bins};

// ---------------------------------------------------------------- fixtures

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
/// `smoke.rs`'s fixture, same rationale: the node needs a knowable, fixed
/// `--addr` up front.
fn free_addr() -> String {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("loopback bind")
        .local_addr()
        .expect("bound listener has an address")
        .to_string()
}

// --------------------------------------------------------------- constants

const ORG_A: u64 = 1;
const ORG_B: u64 = 2;
/// Both tenants write these SAME ids — a scoped table's separation is the
/// scope's, so an oracle that gave each tenant its own id range would pass
/// against a store with no scoping at all.
const DOC_IDS: [u64; 3] = [1, 2, 3];
/// Both tenants write this SAME `#[index]` value, for the same reason.
const SHARED_TAG: &str = "shared";
/// Bounds one `SseTail::next_frame`.
const FRAME_READ_TIMEOUT: Duration = Duration::from_secs(2);
/// Outer bound on a feed drain — a stalled kernel fails by name rather than
/// hanging the test process.
const DRAIN_DEADLINE: Duration = Duration::from_secs(20);
/// How long a follower gets to converge (or to refuse) after the primary has
/// acked the write it must ship.
const REPLICATION_DEADLINE: Duration = Duration::from_secs(30);
const SECRET: &str = "edge-harness-tenant-wire-secret";

/// The refusal a follower on the pre-scoping shape of `Doc` must print
/// (`charpente-core/src/db.rs`'s `resolve_effect_scope`), byte for byte.
/// Hard rule 5: it names what was found (the table, the effect, the scope,
/// the tag), why it is refused (this binary's `Doc` is not tenant-scoped),
/// and the fix.
const SCOPED_RECORD_REFUSAL: &str = "record decode: table \"Doc\" is not tenant-scoped \
     (Row::Scope = Unscoped) but this Put effect carries scope 1 (tag 0x05/0x06) — the record \
     was written for a scoped shape of this table; restore from a replica or backup, or replay \
     under the schema that wrote it";

/// The stop line `serve.rs` wraps it in — asserted too, so this oracle
/// distinguishes "the node refused and stopped" from "the text appeared
/// somewhere in a log while the node carried on applying".
const KERNEL_STOPPED: &str = "charpente kernel stopped on a platform IO failure — restart to \
     recover: ";

fn alpha(id: u64) -> String {
    format!("alpha-{id}")
}

fn beta(id: u64) -> String {
    format!("beta-{id}")
}

// ------------------------------------------------------------------ client

/// Logs `http` in and hands the raw token back, so a second client — a
/// follower's `Http` — can present the SAME session the primary minted
/// (sessions are ordinary rows, so they replicate).
fn login(http: &mut Http, subject: &str) -> Result<String, String> {
    let resp = http
        .post("/login", &format!("\"{subject}\""))
        .map_err(|e| format!("login({subject}) request failed: {e}"))?;
    if resp.status != 200 {
        return Err(format!(
            "login({subject}) -> status {} body {:?}",
            resp.status,
            String::from_utf8_lossy(&resp.body)
        ));
    }
    let token = resp
        .as_json_string()
        .ok_or_else(|| format!("login({subject}) body was not a JSON string token"))?;
    http.set_session(token.clone());
    Ok(token)
}

fn create_doc(http: &mut Http, org: u64, id: u64, text: &str) -> Result<(), String> {
    let body = format!(r#"{{"tag":"{SHARED_TAG}","text":"{text}"}}"#);
    let resp = http
        .post(&format!("/orgs/{org}/docs/{id}"), &body)
        .map_err(|e| format!("create_doc(org {org}, doc {id}) request failed: {e}"))?;
    if resp.status != 200 {
        return Err(format!(
            "create_doc(org {org}, doc {id}) -> status {} body {:?}",
            resp.status,
            String::from_utf8_lossy(&resp.body)
        ));
    }
    Ok(())
}

/// The `text` of every row a listing returned, sorted — the tenant marker
/// this oracle reads, since both tenants share the ids.
fn list_texts(http: &mut Http, path: &str) -> Result<Vec<String>, String> {
    let resp = http
        .get(path)
        .map_err(|e| format!("GET {path} request failed: {e}"))?;
    if resp.status != 200 {
        return Err(format!(
            "GET {path} -> status {} body {:?}",
            resp.status,
            String::from_utf8_lossy(&resp.body)
        ));
    }
    let body = String::from_utf8_lossy(&resp.body).to_string();
    Ok(text_fields(&body))
}

// ------------------------------------------------------------- pure parsing

/// Every `"text":"…"` value in a compact JSON body, sorted. This crate takes
/// no JSON dependency (`realtime_resume.rs`'s `extract_u64_field`,
/// `acked_durable.rs`'s `extract_field`: call-site-local scanners, by
/// convention), and the shape scanned here is fixed by `DocView`/
/// `DocCreated`, both of which spell the field the same way.
fn text_fields(json: &str) -> Vec<String> {
    let key = "\"text\":\"";
    let mut out = Vec::new();
    let mut rest = json;
    while let Some(at) = rest.find(key) {
        rest = &rest[at + key.len()..];
        let mut value = String::new();
        let mut chars = rest.chars();
        let mut consumed = 0;
        while let Some(c) = chars.next() {
            consumed += c.len_utf8();
            match c {
                '"' => break,
                '\\' => {
                    if let Some(escaped) = chars.next() {
                        consumed += escaped.len_utf8();
                        value.push(escaped);
                    }
                }
                other => value.push(other),
            }
        }
        rest = &rest[consumed..];
        out.push(value);
    }
    out.sort();
    out
}

/// Whether a stderr log carries the older-follower refusal AND the stop it
/// must cause. Pure, so this oracle's own verdict is provable without a
/// process (the pins below), the same shape `hunts.rs`/`failover.rs` keep
/// next to each wire assertion.
fn refused_a_scoped_record(stderr: &str) -> bool {
    stderr.contains(SCOPED_RECORD_REFUSAL) && stderr.contains(KERNEL_STOPPED)
}

// ------------------------------------------------------------------ drains

/// Reads frames until every text in `want` has been seen or `deadline`
/// elapses, recording EVERY `doc_created` text that arrives — including a
/// foreign one, which is the whole point: the caller compares the full set,
/// not just the wanted subset.
fn drain_texts(
    tail: &mut SseTail,
    want: &BTreeSet<String>,
    seen: &mut BTreeSet<String>,
    deadline: Instant,
) -> Result<(), String> {
    loop {
        if want.is_subset(seen) {
            return Ok(());
        }
        if Instant::now() >= deadline {
            let missing: Vec<&String> = want.difference(seen).collect();
            return Err(format!(
                "drain_texts: deadline elapsed with {} wanted event(s) never delivered: \
                 {missing:?} (seen: {seen:?}) — a feed that receives NOTHING is a fail-closed \
                 break, not a leak, and this oracle refuses to report it as a pass",
                missing.len()
            ));
        }
        match tail.next_frame(FRAME_READ_TIMEOUT) {
            Ok(Some(frame)) => {
                if frame.event == "doc_created" {
                    for text in text_fields(&frame.data) {
                        seen.insert(text);
                    }
                }
            }
            Ok(None) => {
                return Err(format!(
                    "drain_texts: the stream closed cleanly with {:?} still undelivered",
                    want.difference(seen).collect::<Vec<_>>()
                ));
            }
            Err(e) if e.kind() == io::ErrorKind::TimedOut => {}
            Err(e) => return Err(format!("drain_texts: read failed: {e}")),
        }
    }
}

/// A short best-effort follow-up read: after a tenant has everything it is
/// owed, a leaked frame could still be in flight behind it. Adds whatever
/// arrives to `seen` and returns.
fn drain_trailing(tail: &mut SseTail, seen: &mut BTreeSet<String>, frames: usize) {
    for _ in 0..frames {
        match tail.next_frame(Duration::from_millis(500)) {
            Ok(Some(frame)) => {
                if frame.event == "doc_created" {
                    for text in text_fields(&frame.data) {
                        seen.insert(text);
                    }
                }
            }
            Ok(None) | Err(_) => return,
        }
    }
}

// -------------------------------------------------------------------- rigs

struct Node {
    node: ChildNode,
    addr: String,
    data_dir: PathBuf,
}

fn spawn_tenant_node(
    bins: &Bins,
    scenario: &mut Scenario,
    tag: &str,
    extra: &[&str],
) -> io::Result<Node> {
    let data_dir = temp_dir(tag);
    let addr = free_addr();
    let mut args = vec![
        "--addr".to_string(),
        addr.clone(),
        "--data-dir".to_string(),
        data_dir.to_str().expect("utf-8 temp path").to_string(),
    ];
    args.extend(extra.iter().map(|s| (*s).to_string()));
    let node = ChildNode::spawn(&bins.tenant_node, &args, scenario)?;
    Ok(Node {
        node,
        addr,
        data_dir,
    })
}

impl Node {
    /// Kills the process FIRST, then removes its data directory — the
    /// reverse order would delete a live node's store out from under it.
    fn cleanup(self) {
        let Node { node, data_dir, .. } = self;
        drop(node);
        let _ = std::fs::remove_dir_all(&data_dir);
    }
}

fn scenario_for(name: &str) -> (PathBuf, Scenario) {
    let root = temp_dir("scenario-root");
    std::fs::create_dir_all(&root).expect("scenario root dir");
    let scenario = Scenario::new(&root, name).expect("Scenario::new");
    (root, scenario)
}

// ----------------------------------------------------------------- oracles

/// HTTP: two tenants, one node, one id space. Each session reads exactly its
/// own rows — through the primary key scan AND through the `#[index]`
/// lookup, both of which are prefixed by the scope — and a session that
/// asks for the other tenant's org by name is refused by the app's own
/// `authorize`, with nothing of that tenant's data in the answer.
#[test]
#[ignore]
fn a_tenants_session_never_receives_another_tenants_rows_over_http() {
    let bins = build_bins().expect("build_bins");
    let (root, mut scenario) =
        scenario_for("a_tenants_session_never_receives_another_tenants_rows_over_http");

    let node = spawn_tenant_node(&bins, &mut scenario, "tenant-http", &[]).expect("spawn node");

    let mut a = Http::new(node.addr.clone());
    let mut b = Http::new(node.addr.clone());
    login(&mut a, "org-1").expect("tenant A login");
    login(&mut b, "org-2").expect("tenant B login");
    scenario.record("two tenants logged in over real TCP: org-1 and org-2");

    // Interleaved, so neither tenant's whole batch precedes the other's —
    // an ordering leak would show up in both directions.
    for id in DOC_IDS {
        create_doc(&mut a, ORG_A, id, &alpha(id)).expect("tenant A write");
        create_doc(&mut b, ORG_B, id, &beta(id)).expect("tenant B write");
    }
    scenario.record("both tenants wrote docs 1..3 under the SAME ids and the same tag");

    let want_a: Vec<String> = DOC_IDS.iter().map(|id| alpha(*id)).collect();
    let want_b: Vec<String> = DOC_IDS.iter().map(|id| beta(*id)).collect();

    let got_a = list_texts(&mut a, &format!("/orgs/{ORG_A}/docs")).expect("tenant A scan");
    assert_eq!(
        got_a, want_a,
        "tenant A's scan must return A's OWN rows and only those — anything from tenant B is a \
         cross-tenant leak, and a short/empty answer is a fail-closed break"
    );
    let got_b = list_texts(&mut b, &format!("/orgs/{ORG_B}/docs")).expect("tenant B scan");
    assert_eq!(
        got_b, want_b,
        "tenant B's scan must return B's OWN rows and only those"
    );
    scenario.record("each tenant's scan returned exactly its own three rows");

    let tagged_a = list_texts(&mut a, &format!("/orgs/{ORG_A}/docs/tagged/{SHARED_TAG}"))
        .expect("tenant A index lookup");
    assert_eq!(
        tagged_a, want_a,
        "tenant A's #[index] lookup must be scope-prefixed too: both tenants wrote the tag \
         \"{SHARED_TAG}\", so an unprefixed index entry would hand A tenant B's rows"
    );
    let tagged_b = list_texts(&mut b, &format!("/orgs/{ORG_B}/docs/tagged/{SHARED_TAG}"))
        .expect("tenant B index lookup");
    assert_eq!(
        tagged_b, want_b,
        "tenant B's #[index] lookup must be scope-prefixed too"
    );
    scenario.record("each tenant's index lookup on the shared tag returned only its own rows");

    // Asking for the other tenant BY NAME: the app's own membership check is
    // the answer, and it names the principal and the org (hard rule 5).
    let cross = a
        .get(&format!("/orgs/{ORG_B}/docs"))
        .expect("cross-tenant request");
    let cross_body = String::from_utf8_lossy(&cross.body).to_string();
    assert_eq!(
        cross.status, 400,
        "tenant A naming tenant B's org must be refused by `authorize`, not served: {cross_body}"
    );
    assert_eq!(
        cross_body, "{\"error\":\"principal \\\"org-1\\\" is not a member of org 2\"}",
        "the refusal the client gets names the principal and the org"
    );
    assert!(
        !cross_body.contains("beta"),
        "a refusal must not carry the refused tenant's data: {cross_body}"
    );
    scenario.record("a cross-tenant request was refused by name, carrying no data");

    // Unauthenticated: no session, no tenant, nothing.
    let mut anon = Http::new(node.addr.clone());
    let anon_resp = anon
        .get(&format!("/orgs/{ORG_A}/docs"))
        .expect("anonymous request");
    let anon_body = String::from_utf8_lossy(&anon_resp.body).to_string();
    assert_eq!(
        anon_resp.status, 401,
        "an unauthenticated read of a tenant's rows is a 401: {anon_body}"
    );
    assert!(
        !anon_body.contains("alpha"),
        "a 401 must not carry a tenant's data: {anon_body}"
    );

    let script = scenario.dump().expect("scenario dump");
    eprintln!("scenario script: {}", script.display());
    node.cleanup();
    let _ = std::fs::remove_dir_all(&root);
}

/// SSE: two live subscriptions on one node, one per tenant. Every event is
/// emitted through `ScopedTx::emit`, so it carries its tenant's key; each
/// feed is keyed by the scope its own subscriber proved membership of.
#[test]
#[ignore]
fn a_tenants_feed_never_receives_another_tenants_events_over_sse() {
    let bins = build_bins().expect("build_bins");
    let (root, mut scenario) =
        scenario_for("a_tenants_feed_never_receives_another_tenants_events_over_sse");

    let node = spawn_tenant_node(&bins, &mut scenario, "tenant-sse", &[]).expect("spawn node");

    let mut a = Http::new(node.addr.clone());
    let mut b = Http::new(node.addr.clone());
    let mut a_writer = Http::new(node.addr.clone());
    let mut b_writer = Http::new(node.addr.clone());
    login(&mut a, "org-1").expect("tenant A login");
    login(&mut b, "org-2").expect("tenant B login");
    login(&mut a_writer, "org-1").expect("tenant A writer login");
    login(&mut b_writer, "org-2").expect("tenant B writer login");

    // A session that is not a member cannot even open the other tenant's
    // feed — the subscription is opened through the same `scope()` line the
    // reads are.
    let refused = match a.sse_tail(&format!("/orgs/{ORG_B}/feed"), None) {
        Ok(_) => panic!("tenant A must not be able to open tenant B's feed"),
        Err(e) => e,
    };
    assert!(
        refused.to_string().contains("got 400"),
        "opening another tenant's feed is refused by `authorize` (400): {refused}"
    );
    scenario.record("tenant A was refused tenant B's feed at the subscription itself");

    let mut tail_a = a
        .sse_tail(&format!("/orgs/{ORG_A}/feed"), None)
        .expect("tenant A feed");
    let mut tail_b = b
        .sse_tail(&format!("/orgs/{ORG_B}/feed"), None)
        .expect("tenant B feed");
    scenario.record("both tenants are tailing their own feed");

    for id in DOC_IDS {
        create_doc(&mut a_writer, ORG_A, id, &alpha(id)).expect("tenant A write");
        create_doc(&mut b_writer, ORG_B, id, &beta(id)).expect("tenant B write");
    }
    scenario.record("both tenants wrote docs 1..3 while both feeds were open");

    let want_a: BTreeSet<String> = DOC_IDS.iter().map(|id| alpha(*id)).collect();
    let want_b: BTreeSet<String> = DOC_IDS.iter().map(|id| beta(*id)).collect();

    let deadline = Instant::now() + DRAIN_DEADLINE;
    let mut seen_a = BTreeSet::new();
    let mut seen_b = BTreeSet::new();
    drain_texts(&mut tail_a, &want_a, &mut seen_a, deadline).expect("tenant A feed drain");
    drain_texts(&mut tail_b, &want_b, &mut seen_b, deadline).expect("tenant B feed drain");
    // A leaked frame can be in flight behind the last wanted one.
    drain_trailing(&mut tail_a, &mut seen_a, 3);
    drain_trailing(&mut tail_b, &mut seen_b, 3);

    assert_eq!(
        seen_a, want_a,
        "tenant A's feed must carry A's OWN three events and nothing else — a `beta-*` here is \
         a cross-tenant event leak"
    );
    assert_eq!(
        seen_b, want_b,
        "tenant B's feed must carry B's OWN three events and nothing else"
    );
    scenario.record("each feed delivered exactly its own tenant's three events");

    let script = scenario.dump().expect("scenario dump");
    eprintln!("scenario script: {}", script.display());
    node.cleanup();
    let _ = std::fs::remove_dir_all(&root);
}

/// Mixed-version replication (ruling R60): a primary whose `Doc` is scoped
/// ships a `0x05` record to two followers — one current, one on the
/// pre-scoping shape of the same table. The current one converges; the older
/// one refuses BY NAME and stops, rather than misreading the record's
/// `(scope, id)` as a bare `id` and storing a row nobody asked for.
#[test]
#[ignore]
fn an_older_follower_refuses_a_scoped_record_by_name() {
    let bins = build_bins().expect("build_bins");
    let (root, mut scenario) = scenario_for("an_older_follower_refuses_a_scoped_record_by_name");

    let primary_repl = free_addr();
    let primary = spawn_tenant_node(
        &bins,
        &mut scenario,
        "tenant-primary",
        &[
            "--repl-listen",
            &primary_repl,
            "--role",
            "primary",
            "--secret",
            SECRET,
        ],
    )
    .expect("spawn primary");

    let mut a = Http::new(primary.addr.clone());
    let token_a = login(&mut a, "org-1").expect("tenant A login");
    create_doc(&mut a, ORG_A, DOC_IDS[0], &alpha(DOC_IDS[0])).expect("tenant A write");
    scenario.record("the primary acked one SCOPED write (org 1, doc 1)");

    let control = spawn_tenant_node(
        &bins,
        &mut scenario,
        "tenant-control-follower",
        &[
            "--repl-listen",
            &free_addr(),
            "--role",
            "follower",
            "--primary",
            &primary_repl,
            "--secret",
            SECRET,
        ],
    )
    .expect("spawn control follower");

    let legacy = spawn_tenant_node(
        &bins,
        &mut scenario,
        "tenant-legacy-follower",
        &[
            "--legacy-unscoped",
            "--repl-listen",
            &free_addr(),
            "--role",
            "follower",
            "--primary",
            &primary_repl,
            "--secret",
            SECRET,
        ],
    )
    .expect("spawn legacy follower");

    // The control, on the SAME bytes: a follower built from the same schema
    // applies the scoped record and serves the row. Without this arm, the
    // legacy refusal below would be consistent with a follower that never
    // connected at all.
    let mut control_client = Http::new(control.addr.clone());
    control_client.set_session(token_a.clone());
    let deadline = Instant::now() + REPLICATION_DEADLINE;
    let mut converged = Vec::new();
    while Instant::now() < deadline {
        if let Ok(texts) = list_texts(&mut control_client, &format!("/orgs/{ORG_A}/docs"))
            && !texts.is_empty()
        {
            converged = texts;
            break;
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    assert_eq!(
        converged,
        vec![alpha(DOC_IDS[0])],
        "the CONTROL follower — same binary, same schema — must apply the scoped record and \
         serve the row; if it does not, the legacy refusal below proves nothing about scoping"
    );
    scenario.record("the control follower converged on the scoped row");

    // The older follower: named refusal, and a stopped kernel.
    let stderr_path = legacy.node.stderr_log().to_path_buf();
    let stderr = wait_for_refusal(&stderr_path, Instant::now() + REPLICATION_DEADLINE);
    assert!(
        refused_a_scoped_record(&stderr),
        "the older follower must refuse the scoped record BY NAME and stop — a silent misread \
         is the failure this arm exists to catch. Its stderr was:\n{stderr}"
    );
    scenario.record("the legacy follower refused the scoped record by name and stopped");

    // Negative space: nothing was misread into its store either. Its kernel
    // is gone by construction, so a named non-200 is the expected answer;
    // what must never happen is the row appearing.
    let mut legacy_client = Http::new(legacy.addr.clone());
    match legacy_client.get("/legacy/docs") {
        Ok(resp) => {
            let body = String::from_utf8_lossy(&resp.body).to_string();
            assert!(
                !body.contains(&alpha(DOC_IDS[0])),
                "the older follower refused the record, so the row must not exist in its \
                 unscoped table: status {} body {body}",
                resp.status
            );
            scenario.record(format!(
                "the legacy follower's unscoped table holds no misread row (status {})",
                resp.status
            ));
        }
        Err(e) => scenario.record(format!(
            "the legacy follower no longer answers HTTP after the refusal ({e}) — its kernel \
             stopped, which is the refusal's own consequence"
        )),
    }

    let script = scenario.dump().expect("scenario dump");
    eprintln!("scenario script: {}", script.display());
    legacy.cleanup();
    control.cleanup();
    primary.cleanup();
    let _ = std::fs::remove_dir_all(&root);
}

/// Polls `path` until it carries the refusal (or `deadline` passes), then
/// returns whatever it holds — the caller asserts, so a timeout produces the
/// real (possibly empty) log in the failure message rather than a bare
/// "timed out".
fn wait_for_refusal(path: &Path, deadline: Instant) -> String {
    loop {
        let text = std::fs::read_to_string(path).unwrap_or_default();
        if refused_a_scoped_record(&text) || Instant::now() >= deadline {
            return text;
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

// -------------------------------------------------------------- pure pins
//
// Zero syscalls, so these run in the ordinary `cargo test -p edge-harness`
// sweep (crate doc: the `#[ignore]` line is drawn at syscalls). They protect
// the verdict functions every oracle above depends on — a `text_fields` that
// silently returned nothing would make the leak assertions unfireable.

#[test]
fn text_fields_reads_every_marker_and_nothing_else() {
    let listing = r#"[{"id":"1","tag":"shared","text":"alpha-1"},{"id":"2","tag":"shared","text":"alpha-2"}]"#;
    assert_eq!(text_fields(listing), vec!["alpha-1", "alpha-2"]);
    let frame = r#"{"doc":"2","text":"beta-2"}"#;
    assert_eq!(text_fields(frame), vec!["beta-2"]);
    // Sorted, so the oracles' `assert_eq!` against a sorted expectation is
    // about membership, never delivery order.
    let unordered = r#"{"text":"beta-2"}{"text":"alpha-1"}"#;
    assert_eq!(text_fields(unordered), vec!["alpha-1", "beta-2"]);
    // An empty body yields nothing — which is what makes an empty feed fail
    // the two-sided assertions rather than pass them.
    assert!(text_fields("[]").is_empty());
    // A refusal envelope carries no `text` field, so a leaked error body
    // cannot be mistaken for a row.
    assert!(text_fields(r#"{"error":"principal \"org-1\" is not a member of org 2"}"#).is_empty());
}

#[test]
fn the_scoped_record_refusal_verdict_is_fireable_in_both_directions() {
    let real = format!("{KERNEL_STOPPED}{SCOPED_RECORD_REFUSAL}\n");
    assert!(refused_a_scoped_record(&real));
    // Empty log: the node never refused anything (the timeout shape).
    assert!(!refused_a_scoped_record(""));
    // The refusal without the stop, and the stop without the refusal: this
    // arm's claim is that the follower refused AND stopped, so neither half
    // alone may satisfy it.
    assert!(!refused_a_scoped_record(SCOPED_RECORD_REFUSAL));
    assert!(!refused_a_scoped_record(&format!(
        "{KERNEL_STOPPED}redb: store latch tripped"
    )));
    // The OPPOSITE direction — a current binary meeting a record written for
    // the unscoped shape — is a different refusal, and must not satisfy this
    // one (`resolve_effect_scope`'s other arm).
    let other_way = format!(
        "{KERNEL_STOPPED}record decode: table \"Doc\" is tenant-scoped (Row::Scope != Unscoped) \
         but this Put effect carries no scope (tag 0x00/0x01) — the record was written for an \
         unscoped shape of this table; restore from a replica or backup, or replay under the \
         schema that wrote it"
    );
    assert!(!refused_a_scoped_record(&other_way));
}
