//! The T13 named hunts (spec 2026-07-24 introspection-edge-harness design
//! §4.5): serve.rs hardening probes (#25, open since M0 step 5) and the
//! `charpente switchover` control verb end-to-end against a real pair of
//! nodes, including the `CtrlSwitchover` peer-secret arm — the ledgered
//! "CtrlSwitchover secret arm untested end-to-end" item.
//!
//! **Assert vs. record.** Every probe below states, in its own comment, which
//! of the two it does. Where `serve.rs`/`http/app.rs` already has a contract
//! (a named status, a named error string), the probe ASSERTS it byte-exact.
//! Where the shipped edge has no contract at all — no header-read timeout, no
//! write timeout, no per-connection budget — the probe asserts only the
//! LIVENESS property the brief names (a fresh connection is still answered)
//! and RECORDS the absent contract in the scenario script and the task
//! report. It never invents a contract and asserts it: an assertion here
//! would be this task deciding a product question that hasn't been decided.
//!
//! **Hunt 1 has no edge companion, deliberately.** T13's brief expected one:
//! a wire-visible "persistent 500s on GETs after a latched store read".
//! Writing it would have meant asserting a symptom this rig has no way to
//! produce:
//!   1. There is no persistent-500 symptom **in the store shape this crate
//!      could build** — and that held both before and after the gap was
//!      closed, for opposite reasons. When T13 wrote this leg, a store inside
//!      redb's read cache kept being served: reads answered **200 off the
//!      page cache**, `Db::poisoned()` false, nothing on the wire until the
//!      next write. M2 sub-project 0a (2026-07-31) closed that — a latched
//!      store now poisons the node and the kernel stops, characterized by
//!      `crates/charpente/tests/kernel.rs`'s `kernel_stops_on_a_latched_store`
//!      (this leg's original citation, renamed when it stopped describing a
//!      gap). So there is still nothing here to catch: before, because the
//!      reads succeeded; now, because the node stops serving instead of
//!      emitting a persistent-500 stream. On a store LARGER than that cache
//!      the 500s are real (T13 fix round 2, N-1/I-1) — so this leg is a
//!      statement about the deployment shape reachable from here, not about
//!      the platform.
//!   2. This crate cannot reach the state at all — decisive, and unaffected
//!      by leg 1's scoping. Per the crate's honesty contract, the faults
//!      available here are process- and connection-granular; faulting the
//!      store's own IO is the simulator's job, and `chmod` cannot help — the
//!      node holds the store open, and Unix permissions are checked at
//!      `open`, not per write on a live fd. There is no route from here to a
//!      latched store handle in EITHER shape.
//!
//! The gap itself is characterized in the sim, where it belongs, and the fix
//! proposal is DEFERRED TO M2 by ruling (Noah, 2026-07-26) — that is the
//! proposal's final state, not a pending one, so nothing here is waiting on a
//! decision. The proposal, its recommendation and the policy fork it leaves
//! open are in task-13-report.md and in CLAUDE.md's M2 follow-up entry.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use std::io::{self, Read, Write};
use std::net::{Shutdown, TcpStream};
use std::path::PathBuf;
use std::process::Command;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use edge_harness::{ChildNode, Http, Scenario, build_bins};

// ---------------------------------------------------------------- fixtures
//
// Reimplemented per test file rather than shared, matching this crate's
// established convention for call-site-local helpers (`failover.rs`,
// `acked_durable.rs`, `realtime_resume.rs` each carry their own).

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
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("loopback bind")
        .local_addr()
        .expect("bound listener has an address")
        .to_string()
}

/// Shared peer secret for Hunt 3's primary/follower pair — non-empty, since
/// `Some("")` is a named config error (spec 2026-07-24 amendment).
const SECRET: &str = "edge-harness-t13-secret";

/// `AppConfig::default().body_cap`, which `examples/invoicing` takes
/// unchanged — the number the 413 below quotes back.
const BODY_CAP: usize = 1 << 20;

/// Dumps the scenario script, prints it on failure, and cleans up temp dirs
/// ONLY on success — `failover.rs`'s `finish`, same rationale (the crate's
/// honesty contract: a real finding's evidence must still exist afterward).
fn finish<T>(
    tag: &str,
    scenario: &mut Scenario,
    data_dirs: &[&std::path::Path],
    scenario_root: &std::path::Path,
    result: Result<T, String>,
) -> Result<T, String> {
    match &result {
        Err(e) => scenario.record(format!("FAILED: {e}")),
        Ok(_) => scenario.record("run succeeded"),
    }
    let script = scenario.dump().map_err(|e| e.to_string())?;
    match &result {
        Err(e) => {
            eprintln!(
                "edge-harness {tag}: FAILED — scenario script at {}",
                script.display()
            );
            eprintln!("edge-harness {tag}: {e}");
            eprintln!(
                "edge-harness {tag}: NOT deleting data dir(s) ({}) or scenario root ({}) — \
                 preserved for inspection",
                data_dirs
                    .iter()
                    .map(|p| p.display().to_string())
                    .collect::<Vec<_>>()
                    .join(", "),
                scenario_root.display()
            );
        }
        Ok(_) => {
            for dir in data_dirs {
                let _ = std::fs::remove_dir_all(dir);
            }
            let _ = std::fs::remove_dir_all(scenario_root);
        }
    }
    result
}

/// Spawns one plain (non-replicated) invoicing node and returns it with its
/// HTTP address — the rig every Hunt 2 probe starts from.
fn spawn_plain_node(
    node_bin: &std::path::Path,
    data_dir: &std::path::Path,
    scenario: &mut Scenario,
) -> Result<(ChildNode, String), String> {
    let addr = free_addr();
    let node = ChildNode::spawn(
        node_bin,
        &[
            "--addr".to_string(),
            addr.clone(),
            "--data-dir".to_string(),
            data_dir.to_str().unwrap().to_string(),
        ],
        scenario,
    )
    .map_err(|e| format!("spawning the node failed: {e}"))?;
    Ok((node, addr))
}

// ------------------------------------------------------------ raw wire I/O
//
// Hunt 2's probes send request shapes `client.rs`'s `Http` deliberately
// cannot produce — a body the node stops reading partway, a `Content-Length`
// the client never satisfies, a request head that is never finished. Those
// belong here, at the call site, rather than as new `Http` methods: `Http`'s
// contract is well-formed exchanges, and a "send me a malformed request"
// method on it would be a hazard for every other scenario in this crate.

/// Sends `head` (already CRLF-terminated, including the blank line) followed
/// by `body`, then reads whatever the node sends back until EOF or
/// `read_timeout`. `bytes_written` reports how much of `body` actually went
/// out, so a probe can record the fact instead of guessing.
struct RawExchange {
    bytes_written: usize,
    write_error: Option<String>,
    early_response: bool,
    response: Vec<u8>,
}

/// Between-chunks poll for a response that arrives BEFORE the body finishes.
/// Short: it is paid once per chunk, and every probe here either gets its
/// answer in the first chunk or two, or not at all.
const EARLY_RESPONSE_POLL: Duration = Duration::from_millis(20);

fn raw_exchange(
    addr: &str,
    head: &str,
    body: &[u8],
    read_timeout: Duration,
) -> io::Result<RawExchange> {
    let mut stream = TcpStream::connect(addr)?;
    stream.set_write_timeout(Some(read_timeout))?;
    stream.set_read_timeout(Some(EARLY_RESPONSE_POLL))?;
    stream.write_all(head.as_bytes())?;

    let mut bytes_written = 0;
    let mut write_error = None;
    let mut early_response = false;
    let mut response = Vec::new();
    for chunk in body.chunks(16 * 1024) {
        match stream.write_all(chunk) {
            Ok(()) => bytes_written += chunk.len(),
            Err(e) => {
                write_error = Some(e.to_string());
                break;
            }
        }
        // A server may answer before the body finishes — a 413 does exactly
        // that, then stops reading. A client that keeps blasting regardless
        // fills the node's receive buffer, and the node's close-with-unread-
        // bytes turns the FIN into an RST, which DISCARDS the response
        // already sitting in this side's receive buffer. That is a client
        // bug, not a node one (verified: `curl` gets the 413 byte-exact),
        // and polling between chunks is how a real client avoids it. Without
        // this the oversized-body probe below "observed" no response at all
        // and would have reported a node gap that does not exist.
        let mut buf = [0u8; 4096];
        match stream.read(&mut buf) {
            Ok(0) => {
                early_response = true; // clean EOF: the node is done with us
                break;
            }
            Ok(n) => {
                // Breaking on the FIRST segment is deliberate, and its
                // remainder is picked up by the `read_to_end` drain below
                // (which appends to this same buffer). A response spanning
                // several segments could in principle lose its tail to an RST
                // between the two — considered and left as-is (T13 review,
                // M-2): the failure direction is a FALSE ALARM, never a mask
                // (a short read fails the probe's byte-exact check and prints
                // the raw bytes), and every response these probes expect is
                // well under one segment. Fixing it properly means draining
                // and uploading concurrently, which is not worth a thread here.
                response.extend_from_slice(&buf[..n]);
                early_response = true;
                break;
            }
            // Nothing yet (the common case) — keep uploading. Any hard error
            // is left to the drain below to surface.
            Err(_) => {}
        }
    }

    // Half-close so the node sees the end of the request even when `body` is
    // deliberately shorter than the advertised `Content-Length` — without
    // this a truncated-body probe would just sit until the read timeout and
    // could not tell "the node is waiting for more" from "the node hung up".
    // Skipped once a response has already arrived: there is nothing left to
    // tell the node, and shutting down mid-body would only add a second
    // reason for it to reset.
    if !early_response {
        let _ = stream.shutdown(Shutdown::Write);
    }

    stream.set_read_timeout(Some(read_timeout))?;
    // `read_to_end` surfaces a read timeout as an error even when bytes did
    // arrive first; keep whatever arrived either way — a probe that expects a
    // response cares about the bytes, and one that expects silence cares that
    // there are none.
    let _ = stream.read_to_end(&mut response);
    Ok(RawExchange {
        bytes_written,
        write_error,
        early_response,
        response,
    })
}

/// The status code of a raw HTTP/1.1 response, or `None` when `raw` holds no
/// parseable status line (including the empty "the node said nothing" case).
fn raw_status(raw: &[u8]) -> Option<u16> {
    let text = std::str::from_utf8(raw).ok()?;
    let line = text.lines().next()?;
    line.split_whitespace().nth(1)?.parse().ok()
}

// ------------------------------------------------------- liveness predicate
//
// House rule (failover.rs's `confirmed_absent_count`): one shared predicate,
// several callers, so a rig failure can never be reported as a guarantee
// violation in any probe — and so that discrimination cannot drift apart
// between them.

/// What one liveness probe against a supposedly-live node observed.
#[derive(Debug)]
enum Liveness {
    /// The node answered. ANY status answers the liveness question — a 401 on
    /// an authenticated route proves the accept loop, the kernel channel, and
    /// the response path are all alive just as well as a 200 does. Liveness
    /// is deliberately not about the verdict.
    Answered(u16),
    /// The node accepted (or refused) the connection and then said nothing
    /// inside the bound. This is the ONLY confirmed liveness violation.
    Unresponsive(String),
    /// The rig could not ask the question — the probe never got far enough to
    /// learn anything about the node. Never a violation.
    RigFailure(String),
}

/// The shared classification. `Err(io::ErrorKind::TimedOut)` is the node
/// failing to answer; every other transport error is the rig failing to ask
/// (`client.rs` renames its own socket timeouts to `TimedOut` on both
/// platforms, so this discrimination is not platform-dependent).
fn classify_liveness(outcome: io::Result<u16>) -> Liveness {
    match outcome {
        Ok(status) => Liveness::Answered(status),
        Err(e) if e.kind() == io::ErrorKind::TimedOut => Liveness::Unresponsive(e.to_string()),
        Err(e) => Liveness::RigFailure(e.to_string()),
    }
}

/// The liveness violation's wording — the only message in this file that says
/// the node failed to answer.
fn unresponsive_verdict(probe: &str, waited: Duration, detail: &str) -> String {
    format!(
        "liveness violated during the {probe} probe: the node did not answer a fresh connection \
         within {waited:?} ({detail}) — the accept loop is wedged"
    )
}

/// The rig-failure counterpart. Never contains "liveness violated" — that
/// phrase belongs to `unresponsive_verdict` alone, so a rig hiccup can never
/// be read as a proven wedge.
fn rig_failure_verdict(probe: &str, detail: &str) -> String {
    format!(
        "the {probe} probe could not ask the liveness question — RIG FAILURE, not a proven \
         liveness violation ({detail})"
    )
}

/// Runs one liveness probe: a fresh connection, an unauthenticated request,
/// any answer within `budget` passes. Returns the verdict text on failure.
fn assert_still_live(
    addr: &str,
    probe: &str,
    budget: Duration,
    scenario: &mut Scenario,
) -> Result<(), String> {
    let started = Instant::now();
    let mut http = Http::new(addr);
    let outcome = http.post("/login", "\"hunt-liveness\"").map(|r| r.status);
    let elapsed = started.elapsed();
    match classify_liveness(outcome) {
        Liveness::Answered(status) => {
            if elapsed > budget {
                return Err(unresponsive_verdict(
                    probe,
                    budget,
                    &format!("it answered {status}, but only after {elapsed:?}"),
                ));
            }
            scenario.record(format!(
                "{probe}: a fresh connection was answered {status} in {elapsed:?} (budget \
                 {budget:?}) — the accept loop is alive"
            ));
            Ok(())
        }
        Liveness::Unresponsive(detail) => Err(unresponsive_verdict(probe, budget, &detail)),
        Liveness::RigFailure(detail) => Err(rig_failure_verdict(probe, &detail)),
    }
}

// ============================================================ Hunt 2 (a)
//
// ASSERTS. `serve.rs`'s `to_kernel_request` has a contract here: a body over
// `AppConfig::body_cap` is a 413 naming the cap and the fix, produced at the
// wire before the kernel ever sees the request. Both the status and the
// message are asserted byte-exact (hard rule 5).
//
// Also asserts the follow-on liveness: the node serves the NEXT request
// afterwards. Nothing in `serve.rs` promises the OVERSIZED connection itself
// stays usable — hyper stops reading the body it refused, so that connection
// is expected to be unusable, and this probe records what happened to it
// rather than asserting anything about it.

/// The 413 body, byte-exact (`to_kernel_request`'s `LengthLimitError` arm,
/// wrapped by `Response::error`'s `{"error": …}` envelope).
fn expected_413_body() -> String {
    format!(
        "{{\"error\":\"request body exceeds the {BODY_CAP}-byte cap — raise AppConfig.body_cap \
         or send less\"}}"
    )
}

/// The mismatch verdict for probe (a) — pure, so the zero-IO pins below cover
/// its wording without spawning anything.
fn oversized_verdict(got_status: Option<u16>, got_body: &str) -> String {
    match got_status {
        Some(status) => format!(
            "oversized-body contract violated: expected 413 with body {:?}, got status {status} \
             with body {got_body:?}",
            expected_413_body()
        ),
        None => format!(
            "oversized-body contract violated: expected 413 with body {:?}, but the node sent no \
             parseable HTTP response at all (raw: {got_body:?})",
            expected_413_body()
        ),
    }
}

#[test]
#[ignore]
fn hunt2_oversized_body_is_a_413_and_the_node_keeps_serving() {
    let bins = build_bins().expect("build_bins");
    let tag = "hunt2-oversized";
    let scenario_root = temp_dir(&format!("{tag}-scenario-root"));
    std::fs::create_dir_all(&scenario_root).expect("scenario root");
    let mut scenario = Scenario::new(&scenario_root, tag).expect("Scenario::new");
    let data_dir = temp_dir(&format!("{tag}-data"));

    let result = (|| -> Result<(), String> {
        let (_node, addr) = spawn_plain_node(&bins.node, &data_dir, &mut scenario)?;

        let body = vec![b'x'; BODY_CAP + 1];
        let head = format!(
            "POST /login HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\n\
             Content-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        );
        let exchange = raw_exchange(&addr, &head, &body, Duration::from_secs(10))
            .map_err(|e| rig_failure_verdict("oversized-body", &e.to_string()))?;
        scenario.record(format!(
            "oversized-body: sent Content-Length {} ({} bytes over the {BODY_CAP} cap); wrote {} \
             of them before the node answered (early_response={}), write_error={:?}",
            body.len(),
            body.len() - BODY_CAP,
            exchange.bytes_written,
            exchange.early_response,
            exchange.write_error
        ));

        let raw = String::from_utf8_lossy(&exchange.response).to_string();
        let status = raw_status(&exchange.response);
        if status != Some(413) || !raw.contains(&expected_413_body()) {
            return Err(oversized_verdict(status, &raw));
        }
        scenario.record("oversized-body: 413 with the pinned cap message, byte-exact");

        assert_still_live(
            &addr,
            "oversized-body",
            Duration::from_secs(2),
            &mut scenario,
        )
    })();

    finish(tag, &mut scenario, &[&data_dir], &scenario_root, result).expect(
        "the oversized-body probe must get the pinned 413 and leave the node serving — see \
         stderr above for the scenario script",
    );
}

// ============================================================ Hunt 2 (b)
//
// ASSERTS two things and RECORDS one, and the split is deliberate.
//
// RECORDED, never asserted: WHETHER a response reaches a client that has
// already half-closed mid-body. That is hyper framing, and `serve.rs` promises
// nothing about it — the brief itself expected a bare "connection-level
// failure". Measured 8/8 as a delivered 400 while writing this probe, but a
// probe that asserted delivery would be asserting an undecided question.
//
// ASSERTED: (1) IF a response does arrive, it is `to_kernel_request`'s named
// 400, byte-exact — that message is charpente's own product surface (hard rule
// 5) and must not drift unnoticed just because the framing around it is
// unpromised; (2) one client's malformed request does not cost the node the
// NEXT one. The conditional shape is what keeps (1) honest without turning the
// unpromised delivery question into a flaky assertion.

/// The 400 body, byte-exact (`to_kernel_request`'s catch-all `Err` arm,
/// wrapped by `Response::error`'s `{"error": …}` envelope).
const EXPECTED_400_BODY: &str = "{\"error\":\"request body could not be read\"}";

/// Records what the truncated connection itself produced. Pure, pinned below.
fn truncated_outcome(status: Option<u16>, raw: &str) -> String {
    match status {
        Some(status) => format!(
            "truncated-body: the node answered status {status} on the truncated connection (raw: \
             {raw:?}) — RECORDED, not asserted: serve.rs promises nothing about a response \
             reaching a client that half-closed mid-body"
        ),
        None => format!(
            "truncated-body: the node sent no parseable response on the truncated connection \
             (raw: {raw:?}) — RECORDED, not asserted: a connection-level failure is the \
             expected shape, and serve.rs promises nothing here either way"
        ),
    }
}

/// The verdict when a response DID arrive on the truncated connection but was
/// not the named 400. Pure, pinned below.
fn truncated_wrong_answer_verdict(status: u16, raw: &str) -> String {
    format!(
        "truncated-body contract violated: the node answered the truncated request, so its \
         answer must be the named 400 with body {EXPECTED_400_BODY:?} — got status {status} \
         (raw: {raw:?})"
    )
}

#[test]
#[ignore]
fn hunt2_truncated_body_does_not_cost_the_next_request() {
    let bins = build_bins().expect("build_bins");
    let tag = "hunt2-truncated";
    let scenario_root = temp_dir(&format!("{tag}-scenario-root"));
    std::fs::create_dir_all(&scenario_root).expect("scenario root");
    let mut scenario = Scenario::new(&scenario_root, tag).expect("Scenario::new");
    let data_dir = temp_dir(&format!("{tag}-data"));

    let result = (|| -> Result<(), String> {
        let (_node, addr) = spawn_plain_node(&bins.node, &data_dir, &mut scenario)?;

        // Content-Length 100, 40 bytes sent, then a half-close.
        let head = "POST /login HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\n\
                    Content-Length: 100\r\nConnection: close\r\n\r\n";
        let exchange = raw_exchange(&addr, head, &[b'y'; 40], Duration::from_secs(5))
            .map_err(|e| rig_failure_verdict("truncated-body", &e.to_string()))?;
        let raw = String::from_utf8_lossy(&exchange.response).to_string();
        scenario.record(format!(
            "truncated-body: wrote {} of the 100 advertised bytes, then half-closed",
            exchange.bytes_written
        ));
        let status = raw_status(&exchange.response);
        scenario.record(truncated_outcome(status, &raw));
        // Conditional, per the block comment: the delivery is unpromised, the
        // MESSAGE is not. Nothing is asserted when the node stayed silent.
        if let Some(status) = status
            && (status != 400 || !raw.contains(EXPECTED_400_BODY))
        {
            return Err(truncated_wrong_answer_verdict(status, &raw));
        }

        // The contract that matters most here, and the one this probe always
        // asserts.
        assert_still_live(
            &addr,
            "truncated-body",
            Duration::from_secs(2),
            &mut scenario,
        )
    })();

    finish(tag, &mut scenario, &[&data_dir], &scenario_root, result).expect(
        "a truncated request body must not cost the node the next request — see stderr above \
         for the scenario script",
    );
}

// ============================================================ Hunt 2 (c)
//
// ASSERTS liveness (the brief's own bar: a fresh connection answered within
// 2s while 20 connections drip). RECORDS the absent contract: `serve.rs`'s
// `http_edge` builds `hyper::server::conn::http1::Builder::new()` with no
// `header_read_timeout` and no connection budget, so these 20 connections
// are held for as long as the client cares to drip — indefinitely. That is
// an unbounded-resource exposure (one tokio task and one fd each), and it is
// NOT asserted here: choosing a timeout, or a cap, or neither, is a product
// decision (#25), not this task's to make.

/// One dripping loris connection. `Drop` closes the socket and the drip
/// thread notices on its next write, so a probe never leaks a connection
/// past the end of the test.
struct Loris {
    stream: TcpStream,
}

impl Loris {
    /// Opens a connection, writes a request line and one complete header —
    /// deliberately never the terminating blank line — and returns it ready
    /// to be dripped.
    fn open(addr: &str) -> io::Result<Loris> {
        let mut stream = TcpStream::connect(addr)?;
        stream.set_write_timeout(Some(Duration::from_secs(2)))?;
        stream.write_all(b"GET /customers HTTP/1.1\r\nHost: localhost\r\n")?;
        Ok(Loris { stream })
    }

    /// One byte of a header that is never finished. `false` once the node (or
    /// anything else) has closed the connection.
    fn drip(&mut self) -> bool {
        self.stream.write_all(b"X").is_ok()
    }
}

const LORIS_CONNECTIONS: usize = 20;
/// The brief's cadence: one header byte per second, per connection.
const LORIS_DRIP_INTERVAL: Duration = Duration::from_secs(1);
/// How many drips each connection gets before the probe finishes. Three is
/// enough to prove the connections are being held across multiple drip
/// rounds (a node that killed them on a header timeout would drop them) while
/// keeping the probe near 3s rather than minutes.
const LORIS_DRIPS: usize = 3;

#[test]
#[ignore]
fn hunt2_slow_loris_does_not_block_a_fresh_connection() {
    let bins = build_bins().expect("build_bins");
    let tag = "hunt2-loris";
    let scenario_root = temp_dir(&format!("{tag}-scenario-root"));
    std::fs::create_dir_all(&scenario_root).expect("scenario root");
    let mut scenario = Scenario::new(&scenario_root, tag).expect("Scenario::new");
    let data_dir = temp_dir(&format!("{tag}-data"));

    let result = (|| -> Result<(), String> {
        let (_node, addr) = spawn_plain_node(&bins.node, &data_dir, &mut scenario)?;

        let mut loris: Vec<Loris> = Vec::with_capacity(LORIS_CONNECTIONS);
        for i in 0..LORIS_CONNECTIONS {
            loris.push(Loris::open(&addr).map_err(|e| {
                rig_failure_verdict("slow-loris", &format!("opening connection {i} failed: {e}"))
            })?);
        }
        scenario.record(format!(
            "slow-loris: {LORIS_CONNECTIONS} connections opened with an unfinished request head"
        ));

        // Drip, then probe liveness WHILE the drip is still in progress —
        // probing after the connections were released would prove nothing.
        let mut alive_after_drips = 0;
        for round in 1..=LORIS_DRIPS {
            std::thread::sleep(LORIS_DRIP_INTERVAL);
            alive_after_drips = 0;
            for l in &mut loris {
                if l.drip() {
                    alive_after_drips += 1;
                }
            }
            scenario.record(format!(
                "slow-loris: drip round {round}/{LORIS_DRIPS} — a one-byte write succeeded \
                 locally on {alive_after_drips} of {LORIS_CONNECTIONS} connections"
            ));
        }

        assert_still_live(&addr, "slow-loris", Duration::from_secs(2), &mut scenario)?;

        // RECORDED, never asserted. Worded for what was actually OBSERVED
        // (T13 review, M-4): a successful one-byte `write_all` is a local
        // socket-buffer success, not proof the peer still holds the
        // connection — a write into an already-closed socket succeeds at
        // least once before the RST comes back. So the count below is a
        // PROXY. What makes "the node held them" the right reading is the
        // source, not the count: `serve.rs`'s `http_edge` (serve.rs:211-213)
        // builds `http1::Builder::new()` with no `header_read_timeout` and no
        // connection budget, so nothing in the node is capable of closing
        // them. A future decision to add a timeout would change both the
        // count and that reading, and this probe must not fail when it does.
        scenario.record(format!(
            "slow-loris: RECORDED (not asserted) — one-byte writes still succeeded locally on \
             {alive_after_drips} of {LORIS_CONNECTIONS} stalled connections after {LORIS_DRIPS} \
             drip round(s) (a proxy for still-held, not proof: a write into a closed socket \
             succeeds locally at least once). serve.rs's http_edge sets no header_read_timeout \
             and no connection budget, so nothing in the node closes them and a client can hold \
             them indefinitely: one tokio task and one fd each. Bounding that is a product \
             decision (#25), not this probe's to assert."
        ));
        Ok(())
    })();

    finish(tag, &mut scenario, &[&data_dir], &scenario_root, result).expect(
        "20 dripping connections must not stop the node answering a fresh one — see stderr \
         above for the scenario script",
    );
}

// ============================================================ Hunt 2 (d)
//
// ASSERTS liveness (the accept loop is not wedged by peers that never read
// their responses). RECORDS the absent contract: no write timeout anywhere in
// `serve.rs`, so a connection task can linger for as long as the peer
// declines to read.
//
// **Deviation from the brief, deliberate.** The brief says "SIGSTOP'd
// client". A SIGSTOP does not touch the stopped process's TCP stack: the
// kernel keeps ACKing into the receive buffer and simply stops draining it.
// The only wire-observable consequence is therefore "the peer never reads",
// which is exactly what these sockets do — and doing it in-process makes the
// probe deterministic instead of dependent on an external client binary and
// `kill -STOP` landing before the response is written. `ChildNode::pause`
// stays the right tool for a stopped SERVER (already covered by `client.rs`'s
// `request_times_out_against_a_silent_server`); it is not the right tool
// here, since this crate has no client process to stop.
//
// **Scope of the recorded finding** (T13 review, M-3): these peers request
// `/customers`, whose response fits inside the socket buffers, so the node's
// write completes and its connection task lingers on an idle keep-alive rather
// than on a BLOCKED write. So the "no write timeout" note recorded below is
// read off `serve.rs` (there is none), not stress-tested — actually blocking a
// node's response write needs a body larger than the buffers or a client with
// a shrunken `SO_RCVBUF`, neither of which this probe does.

const HALF_OPEN_PEERS: usize = 20;

/// A peer that sends a complete, well-formed request over a keep-alive
/// connection and then never reads the response. `Drop` closes it.
struct NonReadingPeer {
    _stream: TcpStream,
}

impl NonReadingPeer {
    fn open(addr: &str) -> io::Result<NonReadingPeer> {
        let mut stream = TcpStream::connect(addr)?;
        stream.set_write_timeout(Some(Duration::from_secs(2)))?;
        // No `Connection: close` — a keep-alive connection the node answers
        // and then holds, with the response sitting unread in the socket.
        stream.write_all(b"GET /customers HTTP/1.1\r\nHost: localhost\r\n\r\n")?;
        Ok(NonReadingPeer { _stream: stream })
    }
}

#[test]
#[ignore]
fn hunt2_peers_that_never_read_do_not_wedge_the_accept_loop() {
    let bins = build_bins().expect("build_bins");
    let tag = "hunt2-half-open";
    let scenario_root = temp_dir(&format!("{tag}-scenario-root"));
    std::fs::create_dir_all(&scenario_root).expect("scenario root");
    let mut scenario = Scenario::new(&scenario_root, tag).expect("Scenario::new");
    let data_dir = temp_dir(&format!("{tag}-data"));

    let result = (|| -> Result<(), String> {
        let (_node, addr) = spawn_plain_node(&bins.node, &data_dir, &mut scenario)?;

        let mut peers: Vec<NonReadingPeer> = Vec::with_capacity(HALF_OPEN_PEERS);
        for i in 0..HALF_OPEN_PEERS {
            peers.push(NonReadingPeer::open(&addr).map_err(|e| {
                rig_failure_verdict("half-open", &format!("opening peer {i} failed: {e}"))
            })?);
        }
        scenario.record(format!(
            "half-open: {HALF_OPEN_PEERS} keep-alive peers sent a complete request and will \
             never read the response"
        ));
        // Long enough for the node to have written all 20 responses into
        // sockets nobody is draining.
        std::thread::sleep(Duration::from_millis(500));

        assert_still_live(&addr, "half-open", Duration::from_secs(2), &mut scenario)?;

        scenario.record(format!(
            "half-open: RECORDED (not asserted) — {HALF_OPEN_PEERS} keep-alive connections are \
             open with their responses unread. serve.rs sets no write timeout and no idle \
             timeout (read off the source, not stress-tested here: /customers' response fits \
             the socket buffers, so the node's write completed and its task lingers on an idle \
             keep-alive rather than a blocked write). A peer that never reads therefore holds \
             its task and fd until it disconnects. Bounding that is a product decision (#25), \
             not this probe's to assert."
        ));
        // Explicit, not implicit: the peers are dropped only after the
        // liveness check, so the check ran against a node that really was
        // holding all 20.
        drop(peers);
        Ok(())
    })();

    finish(tag, &mut scenario, &[&data_dir], &scenario_root, result).expect(
        "peers that never read their responses must not wedge the accept loop — see stderr \
         above for the scenario script",
    );
}

// ================================================================= Hunt 3
//
// `charpente switchover` end-to-end against a real primary/follower pair,
// closing the ledgered "CtrlSwitchover secret arm untested end-to-end" item.
// `crates/charpente-cli/tests/ctl.rs` already covers the CLI's framing and
// exit-code mapping against a SCRIPTED peer; what has never run is the real
// node's own `Msg::CtrlSwitchover` secret check (`repl/session.rs`) and the
// hand-off that follows it.
//
// Note the contrast with `failover.rs`'s promote path: switchover DOES demote
// correctly, so the old primary answers writes with 421 naming the new
// primary. That is asserted here.

/// The node's peer-secret refusal, hardcoded because
/// `repl::session::PEER_SECRET_REFUSAL` is `pub(crate)` and this crate does
/// not link against `charpente` at all (crate doc: it drives built binaries).
///
/// **This copy IS the pin.** If `session.rs` rewords the refusal, this
/// constant must be updated in the same commit — a drifted copy here means
/// the operator-facing text changed with nothing at the wire noticing, which
/// is exactly the regression hard rule 5 exists to prevent.
const PEER_SECRET_REFUSAL: &str = "peer secret missing or mismatched — set \
     ReplConfig.peer_secret to match this node (or run the CLI verb with --secret)";

/// The verdict when a `charpente` control verb's exit code or output is not
/// what the contract says. Pure; pinned byte-exact below.
fn ctl_verdict(verb: &str, expected: i32, got: Option<i32>, stdout: &str, stderr: &str) -> String {
    format!(
        "`charpente {verb}` contract violated: expected exit {expected}, got {got:?} — stdout \
         {stdout:?}, stderr {stderr:?}"
    )
}

/// The verdict when the refusal text is missing from a refused verb's stderr.
/// Separate from `ctl_verdict` because the exit code can be right while the
/// message has drifted — and the message is the product surface.
fn refusal_text_verdict(verb: &str, stderr: &str) -> String {
    format!(
        "`charpente {verb}` exited 1 as expected but its stderr did not carry the node's \
         peer-secret refusal verbatim — expected to contain {PEER_SECRET_REFUSAL:?}, got \
         {stderr:?}"
    )
}

/// The node's own switchover-completion message (`repl/mod.rs`'s
/// `finish_switchover`), which the CLI forwards verbatim on exit 0. Built from
/// the runtime target address, so the shape — not the address — is what the
/// pin below fixes.
fn expected_switchover_completion(to: &str) -> String {
    format!("switchover to {to} complete — this node is now a follower of {to}")
}

/// `refusal_text_verdict`'s success-path mirror: exit 0 with the completion
/// message missing or reworded. Pure; pinned below.
fn completion_text_verdict(to: &str, stdout: &str) -> String {
    format!(
        "`charpente switchover` exited 0 but its stdout did not carry the node's completion \
         message verbatim — expected to contain {:?}, got {stdout:?}",
        expected_switchover_completion(to)
    )
}

/// The verdict when the demoted old primary does not answer a write with the
/// 421 naming its successor. One composer for both halves — a wrong status and
/// a 421 that fails to name the new primary are the same contract failing, and
/// an operator needs the same two facts either way.
fn demoted_421_verdict(status: u16, body: &str, new_primary: &str) -> String {
    if status != 421 {
        format!(
            "post-switchover write gate violated: the demoted old primary must refuse writes \
             with 421 naming the new primary {new_primary}, got status {status} (body {body:?})"
        )
    } else {
        format!(
            "post-switchover write gate violated: the demoted old primary answered 421 but did \
             not name the new primary {new_primary} — an operator cannot retry against an \
             address the refusal never gives (body {body:?})"
        )
    }
}

/// Polls the follower's own view of a row until it appears — the same
/// ship-ack barrier `failover.rs` uses, so the switchover below runs against
/// a follower that is genuinely caught up rather than one that merely
/// connected.
fn wait_converged(follower_http: &mut Http, id: u64, budget: Duration) -> bool {
    let deadline = Instant::now() + budget;
    loop {
        if let Ok(resp) = follower_http.get(&format!("/customers/{id}"))
            && resp.status == 200
        {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn login(http: &mut Http, scenario: &mut Scenario) -> Result<String, String> {
    let login = http
        .post("/login", "\"hunt3-tester\"")
        .map_err(|e| format!("login request failed: {e}"))?;
    if login.status != 200 {
        return Err(format!(
            "login failed: status {} body {:?}",
            login.status,
            String::from_utf8_lossy(&login.body)
        ));
    }
    let token = login
        .as_json_string()
        .ok_or_else(|| "login response body was not a JSON string token".to_string())?;
    http.set_session(token.clone());
    scenario.record("logged in against the primary");
    Ok(token)
}

#[test]
#[ignore]
fn hunt3_switchover_refuses_a_wrong_secret_then_hands_off_with_the_right_one() {
    let bins = build_bins().expect("build_bins");
    let tag = "hunt3-switchover";
    let scenario_root = temp_dir(&format!("{tag}-scenario-root"));
    std::fs::create_dir_all(&scenario_root).expect("scenario root");
    let mut scenario = Scenario::new(&scenario_root, tag).expect("Scenario::new");
    let primary_data = temp_dir(&format!("{tag}-primary-data"));
    let follower_data = temp_dir(&format!("{tag}-follower-data"));

    let result = (|| -> Result<(), String> {
        let primary_addr = free_addr();
        let primary_repl_addr = free_addr();
        let follower_addr = free_addr();
        let follower_repl_addr = free_addr();

        let _primary = ChildNode::spawn(
            &bins.node,
            &[
                "--addr".to_string(),
                primary_addr.clone(),
                "--data-dir".to_string(),
                primary_data.to_str().unwrap().to_string(),
                "--repl-listen".to_string(),
                primary_repl_addr.clone(),
                "--role".to_string(),
                "primary".to_string(),
                "--secret".to_string(),
                SECRET.to_string(),
            ],
            &mut scenario,
        )
        .map_err(|e| format!("spawning the primary failed: {e}"))?;

        let _follower = ChildNode::spawn(
            &bins.node,
            &[
                "--addr".to_string(),
                follower_addr.clone(),
                "--data-dir".to_string(),
                follower_data.to_str().unwrap().to_string(),
                "--repl-listen".to_string(),
                follower_repl_addr.clone(),
                "--role".to_string(),
                "follower".to_string(),
                "--primary".to_string(),
                primary_repl_addr.clone(),
                "--secret".to_string(),
                SECRET.to_string(),
            ],
            &mut scenario,
        )
        .map_err(|e| format!("spawning the follower failed: {e}"))?;

        let mut primary_http = Http::new(&primary_addr);
        let token = login(&mut primary_http, &mut scenario)?;
        let mut follower_http = Http::new(&follower_addr);
        follower_http.set_session(token);

        // A converged row first: `begin_switchover` refuses a target that has
        // no live replication session, so without this the wrong-secret arm
        // below could "pass" for the wrong reason.
        let create = primary_http
            .post(
                "/customers/1",
                r#"{"name":"Grace Hopper","email":"grace@example.com"}"#,
            )
            .map_err(|e| format!("seed write failed: {e}"))?;
        if create.status != 200 {
            return Err(format!(
                "seed write: expected 200, got {} body {:?}",
                create.status,
                String::from_utf8_lossy(&create.body)
            ));
        }
        if !wait_converged(&mut follower_http, 1, Duration::from_secs(5)) {
            return Err(
                "the seed write never converged to the follower — the pair is not replicating, \
                 so a switchover result here would prove nothing"
                    .to_string(),
            );
        }
        scenario.record("seed write converged to the follower — the pair is replicating");

        // ---- the secret arm: a WRONG secret must be refused by the NODE ----
        let wrong = Command::new(&bins.charpente)
            .args([
                "switchover",
                "--primary",
                &primary_repl_addr,
                "--to",
                &follower_repl_addr,
                "--secret",
                "definitely-not-the-secret",
            ])
            .output()
            .map_err(|e| format!("running `charpente switchover` failed to launch: {e}"))?;
        let wrong_stdout = String::from_utf8_lossy(&wrong.stdout).to_string();
        let wrong_stderr = String::from_utf8_lossy(&wrong.stderr).to_string();
        scenario.record(format!(
            "switchover --secret <wrong>: exit {:?}, stdout {wrong_stdout:?}, stderr \
             {wrong_stderr:?}",
            wrong.status.code()
        ));
        if wrong.status.code() != Some(1) {
            return Err(ctl_verdict(
                "switchover",
                1,
                wrong.status.code(),
                &wrong_stdout,
                &wrong_stderr,
            ));
        }
        if !wrong_stderr.contains(PEER_SECRET_REFUSAL) {
            return Err(refusal_text_verdict("switchover", &wrong_stderr));
        }
        scenario.record(
            "the node's CtrlSwitchover secret check refused the wrong secret and the CLI \
             forwarded its words byte-exact, exit 1",
        );

        // The refusal must not have moved the primary: it still takes writes.
        let still_primary = primary_http
            .post(
                "/customers/2",
                r#"{"name":"Still Primary","email":"sp@example.com"}"#,
            )
            .map_err(|e| format!("post-refusal write to the primary failed: {e}"))?;
        if still_primary.status != 200 {
            return Err(format!(
                "a refused switchover must leave the primary writable, but a write got {} body \
                 {:?}",
                still_primary.status,
                String::from_utf8_lossy(&still_primary.body)
            ));
        }
        scenario.record("refuse-before-any-state-change confirmed: the primary still writes");

        // ---- the right secret: the hand-off completes -----------------------
        let ok = Command::new(&bins.charpente)
            .args([
                "switchover",
                "--primary",
                &primary_repl_addr,
                "--to",
                &follower_repl_addr,
                "--secret",
                SECRET,
            ])
            .output()
            .map_err(|e| format!("running `charpente switchover` failed to launch: {e}"))?;
        let ok_stdout = String::from_utf8_lossy(&ok.stdout).to_string();
        let ok_stderr = String::from_utf8_lossy(&ok.stderr).to_string();
        scenario.record(format!(
            "switchover --secret <right>: exit {:?}, stdout {ok_stdout:?}, stderr {ok_stderr:?}",
            ok.status.code()
        ));
        if ok.status.code() != Some(0) {
            return Err(ctl_verdict(
                "switchover",
                0,
                ok.status.code(),
                &ok_stdout,
                &ok_stderr,
            ));
        }
        if !ok_stdout.contains(&expected_switchover_completion(&follower_repl_addr)) {
            return Err(completion_text_verdict(&follower_repl_addr, &ok_stdout));
        }
        scenario.record("switchover completed: exit 0, the node's completion message verbatim");

        // ---- the write gate flipped, both ways -----------------------------
        // The OLD primary is now a follower of the new one and must answer a
        // write with 421 naming it. This is the contrast with the promote
        // path (failover.rs's known gap): switchover demotes correctly.
        let refused = primary_http
            .post(
                "/customers/3",
                r#"{"name":"After Handoff","email":"ah@example.com"}"#,
            )
            .map_err(|e| format!("post-switchover write to the old primary failed: {e}"))?;
        let refused_body = String::from_utf8_lossy(&refused.body).to_string();
        if refused.status != 421 || !refused_body.contains(&follower_repl_addr) {
            return Err(demoted_421_verdict(
                refused.status,
                &refused_body,
                &follower_repl_addr,
            ));
        }
        scenario.record(format!(
            "the demoted old primary answers writes 421 naming the new primary \
             {follower_repl_addr}"
        ));

        // And the new primary takes writes.
        let accepted = follower_http
            .post(
                "/customers/4",
                r#"{"name":"New Primary","email":"np@example.com"}"#,
            )
            .map_err(|e| format!("write to the new primary failed: {e}"))?;
        if accepted.status != 200 {
            return Err(format!(
                "after switchover the NEW primary must accept writes, got {} body {:?}",
                accepted.status,
                String::from_utf8_lossy(&accepted.body)
            ));
        }
        scenario.record("the new primary accepts writes — the gate flipped both ways");
        Ok(())
    })();

    finish(
        tag,
        &mut scenario,
        &[&primary_data, &follower_data],
        &scenario_root,
        result,
    )
    .expect(
        "switchover must refuse a wrong secret by name and complete the hand-off with the right \
         one — see stderr above for the scenario script",
    );
}

// ------------------------------------------------- zero-IO named-error pins
//
// Every verdict-composing function above is pure and pinned here byte-exact
// (house rule, `failover.rs`): a verdict is what a failure hands the operator,
// so it must not be assembled inline in an IO path where nothing checks its
// wording. These tests touch no process, socket, or file and are therefore
// NOT `#[ignore]`d (Noah's ruling, 2026-07-25).

#[test]
fn expected_413_body_names_the_cap_and_the_fix() {
    assert_eq!(
        expected_413_body(),
        "{\"error\":\"request body exceeds the 1048576-byte cap — raise AppConfig.body_cap or \
         send less\"}"
    );
}

#[test]
fn oversized_verdict_names_both_the_expected_and_the_observed() {
    assert_eq!(
        oversized_verdict(Some(200), "{\"ok\":true}"),
        "oversized-body contract violated: expected 413 with body \
         \"{\\\"error\\\":\\\"request body exceeds the 1048576-byte cap — raise \
         AppConfig.body_cap or send less\\\"}\", got status 200 with body \"{\\\"ok\\\":true}\""
    );
}

#[test]
fn oversized_verdict_says_so_when_there_was_no_response_at_all() {
    let msg = oversized_verdict(None, "");
    assert!(
        msg.contains("the node sent no parseable HTTP response at all (raw: \"\")"),
        "{msg}"
    );
}

#[test]
fn unresponsive_verdict_is_the_only_wording_that_claims_a_violation() {
    let msg = unresponsive_verdict("slow-loris", Duration::from_secs(2), "timed out");
    assert_eq!(
        msg,
        "liveness violated during the slow-loris probe: the node did not answer a fresh \
         connection within 2s (timed out) — the accept loop is wedged"
    );
}

#[test]
fn rig_failure_verdict_never_claims_a_liveness_violation() {
    let msg = rig_failure_verdict("half-open", "connection refused");
    assert_eq!(
        msg,
        "the half-open probe could not ask the liveness question — RIG FAILURE, not a proven \
         liveness violation (connection refused)"
    );
    assert!(
        !msg.contains("liveness violated"),
        "a rig failure must never carry the violation phrase: {msg}"
    );
}

#[test]
fn classify_liveness_only_calls_a_timeout_unresponsive() {
    assert!(matches!(
        classify_liveness(Ok(401)),
        Liveness::Answered(401)
    ));
    assert!(matches!(
        classify_liveness(Err(io::Error::new(io::ErrorKind::TimedOut, "silent"))),
        Liveness::Unresponsive(_)
    ));
    // A refused connection is NOT a proven wedge: the node may simply not be
    // up yet, which is the rig's problem to report as its own.
    assert!(matches!(
        classify_liveness(Err(io::Error::new(
            io::ErrorKind::ConnectionRefused,
            "refused"
        ))),
        Liveness::RigFailure(_)
    ));
}

#[test]
fn truncated_outcome_records_both_shapes_without_asserting_either() {
    let answered = truncated_outcome(Some(400), "HTTP/1.1 400 Bad Request\r\n\r\n");
    assert!(answered.contains("answered status 400"), "{answered}");
    assert!(answered.contains("RECORDED, not asserted"), "{answered}");
    let silent = truncated_outcome(None, "");
    assert!(silent.contains("no parseable response"), "{silent}");
    assert!(silent.contains("RECORDED, not asserted"), "{silent}");
}

#[test]
fn expected_400_body_is_byte_exact() {
    assert_eq!(
        EXPECTED_400_BODY,
        "{\"error\":\"request body could not be read\"}"
    );
}

#[test]
fn truncated_wrong_answer_verdict_names_the_expected_400() {
    assert_eq!(
        truncated_wrong_answer_verdict(200, "HTTP/1.1 200 OK\r\n\r\n\"tok\""),
        "truncated-body contract violated: the node answered the truncated request, so its \
         answer must be the named 400 with body \"{\\\"error\\\":\\\"request body could not be \
         read\\\"}\" — got status 200 (raw: \"HTTP/1.1 200 OK\\r\\n\\r\\n\\\"tok\\\"\")"
    );
}

#[test]
fn raw_status_reads_the_code_or_nothing() {
    assert_eq!(
        raw_status(b"HTTP/1.1 413 Payload Too Large\r\n\r\n"),
        Some(413)
    );
    assert_eq!(raw_status(b""), None);
    assert_eq!(raw_status(b"garbage\r\n"), None);
}

#[test]
fn ctl_verdict_names_the_verb_the_codes_and_both_streams() {
    assert_eq!(
        ctl_verdict("switchover", 1, Some(0), "done", ""),
        "`charpente switchover` contract violated: expected exit 1, got Some(0) — stdout \
         \"done\", stderr \"\""
    );
}

/// The hardcoded copy of `repl::session::PEER_SECRET_REFUSAL` is itself the
/// pin (see its doc comment) — this test states the exact bytes a second
/// time, so a careless edit to the constant fails here rather than silently
/// weakening the Hunt 3 assertion that reads it.
#[test]
fn peer_secret_refusal_copy_is_byte_exact() {
    assert_eq!(
        PEER_SECRET_REFUSAL,
        "peer secret missing or mismatched — set ReplConfig.peer_secret to match this node (or \
         run the CLI verb with --secret)"
    );
}

#[test]
fn refusal_text_verdict_quotes_the_expected_refusal() {
    let msg = refusal_text_verdict("switchover", "something else");
    assert!(msg.contains("peer secret missing or mismatched"), "{msg}");
    assert!(msg.contains("got \"something else\""), "{msg}");
}

#[test]
fn expected_switchover_completion_is_the_nodes_wording() {
    assert_eq!(
        expected_switchover_completion("127.0.0.1:9001"),
        "switchover to 127.0.0.1:9001 complete — this node is now a follower of 127.0.0.1:9001"
    );
}

#[test]
fn completion_text_verdict_quotes_the_expected_completion() {
    assert_eq!(
        completion_text_verdict("127.0.0.1:9001", "nothing useful"),
        "`charpente switchover` exited 0 but its stdout did not carry the node's completion \
         message verbatim — expected to contain \"switchover to 127.0.0.1:9001 complete — this \
         node is now a follower of 127.0.0.1:9001\", got \"nothing useful\""
    );
}

#[test]
fn demoted_421_verdict_distinguishes_a_wrong_status_from_an_unnamed_successor() {
    assert_eq!(
        demoted_421_verdict(200, "{\"id\":3}", "127.0.0.1:9001"),
        "post-switchover write gate violated: the demoted old primary must refuse writes with \
         421 naming the new primary 127.0.0.1:9001, got status 200 (body \"{\\\"id\\\":3}\")"
    );
    assert_eq!(
        demoted_421_verdict(421, "{\"error\":\"nope\"}", "127.0.0.1:9001"),
        "post-switchover write gate violated: the demoted old primary answered 421 but did not \
         name the new primary 127.0.0.1:9001 — an operator cannot retry against an address the \
         refusal never gives (body \"{\\\"error\\\":\\\"nope\\\"}\")"
    );
}
