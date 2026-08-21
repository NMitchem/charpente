//! Oracle 3 (spec 2026-07-24 introspection-edge-harness design, T12): the
//! wire-level successor to the realtime sub-project's in-sim reconnecting-
//! client property, automating the realtime demo-of-record (M1 sub-project 3
//! close-out). Drives one invoicing `node` through a `TcpProxy`, tails
//! `/events/feed` over SSE while creating and finalizing invoices (each
//! finalize emits `invoice_finalized`, keyed to the caller's org), kills the
//! node mid-stream after deliberately leaving some emitted events unread,
//! restarts it, and reconnects with `Last-Event-ID` set to the last frame
//! seen before the kill. The resumed connection must deliver EXACTLY the
//! invoice ids that were acked but not yet read before the kill — no fewer
//! (a resume loss) and no more/duplicates (a resume re-delivery bug).
//!
//! `invoice_finalized` frames are the ledger this oracle checks (their id is
//! minted synchronously inside `finalize_invoice`'s own transaction, so a
//! 200 response is a hard guarantee the event already exists). The same feed
//! also carries `receipt_sent` frames (`SendReceipt`, an async job
//! `finalize_invoice` enqueues) — this oracle reads and skips them like any
//! other frame on the wire, but does not track them: their emission timing
//! depends on job scheduling, not a synchronous HTTP ack, so tracking them
//! against an expected ledger would tie this oracle's correctness to
//! platform job-scheduling latency instead of the resume guarantee it exists
//! to test.
//!
//! Two named refusals close the oracle out: a garbage `Last-Event-ID`/cursor
//! is the pinned 400 (byte-exact); the below-floor/foreign-cursor 409 is
//! NOT attempted here (ambiguity resolution, task-12 dispatch) — cheaply
//! arranging it needs driving `DbConfig::event_retention` (65_536 by
//! default) past its floor, which is not cheap at wire speed. That refusal
//! stays covered by the in-sim multi-node property (realtime T8) and its
//! pinned error-suite tests.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use std::collections::BTreeSet;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use edge_harness::{ChildNode, Http, Scenario, SseTail, TcpProxy, build_bins};

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
/// `acked_durable.rs`'s fixture, same rationale: the node needs a knowable,
/// fixed `--addr` up front so a restarted process rebinds to the exact
/// address the proxy forwards to.
fn free_addr() -> String {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("loopback bind")
        .local_addr()
        .expect("bound listener has an address")
        .to_string()
}

// ----------------------------------------------------------------- constants

/// Invoices created and fully drained BEFORE the miss phase — establishes
/// that live tailing works at all (a positive control: if this drain never
/// completes, the oracle fails here with a live-tailing message, not a
/// misleading resume-specific one) and gives `last_id` a real starting point.
const LIVE_ROUNDS: u64 = 4;
/// Invoices created and finalized WITHOUT being read before the kill — the
/// pool the pre-kill partial drain deliberately leaves (mostly) undelivered.
const MISS_ROUNDS: u64 = 10;
/// Raw SSE frames read from the still-open tail after the miss phase's
/// invoices are all acked, before killing the node — deliberately far fewer
/// than the up-to-`2 * MISS_ROUNDS` frames the miss phase can produce
/// (`invoice_finalized` + `receipt_sent` per invoice), so the miss phase
/// leaves a real gap to resume across regardless of job-scheduling timing.
const PRE_KILL_DRAIN_FRAMES: usize = 3;
/// Generous headroom over the miss phase's worst case (`2 * MISS_ROUNDS`
/// frames) for the resume drain's frame-read budget.
const RESUME_MAX_FRAMES: usize = 2 * MISS_ROUNDS as usize + 10;
/// Bounds one `SseTail::next_frame` call — long enough that a legitimately
/// slow (but not stuck) delivery still succeeds, short enough that the
/// pre-kill/resume drains stay responsive.
const FRAME_READ_TIMEOUT: Duration = Duration::from_secs(2);
/// How many consecutive timeouts the resume drain tolerates (nothing new
/// arriving) before concluding the stream has gone quiet rather than
/// hanging forever waiting for ids that are never coming.
const MAX_CONSECUTIVE_TIMEOUTS: u32 = 3;
/// Outer wall-clock bound on the live-phase "prove tailing works" drain —
/// bounds total time, not any one read, so a stalled kernel fails by name
/// instead of hanging the test process.
const LIVE_DRAIN_DEADLINE: Duration = Duration::from_secs(20);

// ------------------------------------------------------------------- client

fn login(http: &mut Http, scenario: &mut Scenario) -> Result<(), String> {
    let login = http
        .post("/login", "\"edge-tester\"")
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
    http.set_session(token);
    scenario.record("logged in through the proxy");
    Ok(())
}

fn create_customer(http: &mut Http, id: u64) -> Result<(), String> {
    let body = format!(r#"{{"name":"Customer {id}","email":"c{id}@example.com"}}"#);
    let resp = http
        .post(&format!("/customers/{id}"), &body)
        .map_err(|e| format!("create_customer({id}) request failed: {e}"))?;
    if resp.status != 200 {
        return Err(format!(
            "create_customer({id}) -> status {} body {:?}",
            resp.status,
            String::from_utf8_lossy(&resp.body)
        ));
    }
    Ok(())
}

/// Creates invoice `id` for `customer` (no line items — total is irrelevant
/// to this oracle) and finalizes it. A 200 from finalize is a hard guarantee
/// the `invoice_finalized` event already exists (it's minted synchronously,
/// same transaction) — this is what makes the invoice id ledger this oracle
/// checks trustworthy without reading anything back first.
fn create_and_finalize_invoice(http: &mut Http, id: u64, customer: u64) -> Result<(), String> {
    // `customer` is `Id<Customer>` and `due_day` is `WireU64` (task 5): both
    // are 64-bit values, so both cross the wire as quoted decimal strings,
    // never bare JSON numbers.
    let create_body = format!(r#"{{"customer":"{customer}","due_day":"0","line_items":[]}}"#);
    let create = http
        .post(&format!("/invoices/{id}"), &create_body)
        .map_err(|e| format!("create_invoice({id}) request failed: {e}"))?;
    if create.status != 200 {
        return Err(format!(
            "create_invoice({id}) -> status {} body {:?}",
            create.status,
            String::from_utf8_lossy(&create.body)
        ));
    }
    let finalize = http
        .post(&format!("/invoices/{id}/finalize"), "")
        .map_err(|e| format!("finalize_invoice({id}) request failed: {e}"))?;
    if finalize.status != 200 {
        return Err(format!(
            "finalize_invoice({id}) -> status {} body {:?}",
            finalize.status,
            String::from_utf8_lossy(&finalize.body)
        ));
    }
    Ok(())
}

// ------------------------------------------------------------- pure parsing

/// Extracts a `u64` field from a compact JSON object body. `ReceiptSent`'s
/// wire shape is `{"invoice":N,"amount_cents":M}` — a bare `#[event]`
/// payload field, unaffected by task 5's `Wire` migration. `InvoiceFinalized`'s
/// `invoice` is `Id<Invoice>` (task 5): `Id<T>` always crosses as a quoted
/// decimal string, so its shape is `{"invoice":"N"}` — one optional leading
/// `"` is stripped before the digit scan to read either shape; field order
/// and presence are never in doubt here, so this stays a scan, not a parser.
/// This crate has no JSON dependency, matching `acked_durable.rs`'s
/// `extract_field`.
fn extract_u64_field(json: &str, key: &str) -> Option<u64> {
    let pattern = format!("\"{key}\":");
    let start = json.find(&pattern)? + pattern.len();
    let rest = json[start..].strip_prefix('"').unwrap_or(&json[start..]);
    let digits: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
    if digits.is_empty() {
        None
    } else {
        digits.parse().ok()
    }
}

/// Extracts and unescapes the `"error"` field from a `{"error":"<message>"}`
/// body — the shape `Response::error` (`crates/charpente/src/http/mod.rs`)
/// emits for every named HTTP-level rejection, including the cursor-400 this
/// oracle pins. Pure, zero-IO; reimplemented call-site-local per this
/// crate's convention (`acked_durable.rs`'s `extract_field`,
/// `client.rs::HttpResponse::as_json_string`'s escaping logic) rather than
/// growing `client.rs`'s public surface for a one-off need.
fn extract_error_message(body: &[u8]) -> Option<String> {
    let text = std::str::from_utf8(body).ok()?;
    let key = "\"error\":\"";
    let start = text.find(key)? + key.len();
    let mut out = String::new();
    let mut chars = text[start..].chars();
    while let Some(c) = chars.next() {
        match c {
            '"' => return Some(out),
            '\\' => out.push(chars.next()?),
            other => out.push(other),
        }
    }
    None
}

// ------------------------------------------------------------------- drains

/// Reads frames from `tail` until every id in `want` has been observed as an
/// `invoice_finalized` frame, updating `last_id` on every frame read
/// (regardless of type — `Last-Event-ID` resume is keyed on the whole
/// stream's id sequence, not any one event type). `deadline` bounds total
/// wall-clock so a stalled kernel fails by name instead of hanging the
/// oracle forever; individual reads use the shorter `FRAME_READ_TIMEOUT` and
/// keep polling until the outer deadline.
fn drain_until_seen(
    tail: &mut SseTail,
    want: &BTreeSet<u64>,
    seen: &mut BTreeSet<u64>,
    last_id: &mut Option<String>,
    deadline: Instant,
) -> Result<(), String> {
    loop {
        if want.is_subset(seen) {
            return Ok(());
        }
        if Instant::now() >= deadline {
            let still_missing: Vec<u64> = want.difference(seen).copied().collect();
            return Err(format!(
                "drain_until_seen: deadline elapsed with {} of {} wanted id(s) still unseen: \
                 {still_missing:?}",
                still_missing.len(),
                want.len()
            ));
        }
        match tail.next_frame(FRAME_READ_TIMEOUT) {
            Ok(Some(frame)) => {
                *last_id = Some(frame.id.clone());
                if frame.event == "invoice_finalized"
                    && let Some(id) = extract_u64_field(&frame.data, "invoice")
                {
                    seen.insert(id);
                }
            }
            Ok(None) => {
                return Err(
                    "drain_until_seen: connection closed cleanly before every wanted id arrived"
                        .to_string(),
                );
            }
            Err(e) if e.kind() == io::ErrorKind::TimedOut => {} // keep polling to the deadline
            Err(e) => return Err(format!("drain_until_seen: read failed: {e}")),
        }
    }
}

/// Reads up to `max_frames` raw frames, tolerating an individual read timing
/// out as "nothing more is ready — stop here" rather than a failure. This is
/// the deliberate pre-kill PARTIAL drain: read whatever's ready, then move on
/// to the kill, leaving the rest of the miss phase's events unread on
/// purpose. Returns how many frames were actually read (for the scenario
/// record and the pre-kill vacuity check).
fn drain_up_to(
    tail: &mut SseTail,
    max_frames: usize,
    seen: &mut BTreeSet<u64>,
    last_id: &mut Option<String>,
) -> usize {
    let mut read = 0;
    for _ in 0..max_frames {
        match tail.next_frame(FRAME_READ_TIMEOUT) {
            Ok(Some(frame)) => {
                read += 1;
                *last_id = Some(frame.id.clone());
                if frame.event == "invoice_finalized"
                    && let Some(id) = extract_u64_field(&frame.data, "invoice")
                {
                    seen.insert(id);
                }
            }
            Ok(None) | Err(_) => break, // best-effort — stop early, don't fail the oracle here
        }
    }
    read
}

/// Why `drain_resume` stopped. Three branches (review Important 2 added
/// `BudgetExhausted`, split out of `Drained`): a stream that goes quiet for
/// `MAX_CONSECUTIVE_TIMEOUTS` reads or closes cleanly, WITH a live
/// connection and a valid cursor, is genuine evidence the server never sent
/// a still-missing frame — that stays `Drained`, the only variant
/// `classify_resume` may word as a violation. Exhausting THIS RIG'S OWN
/// `max_frames` read budget says nothing about the server at all — it is
/// the rig deciding to stop asking, not the server refusing to answer — so
/// it gets its own variant and its own non-violation wording. Folding it
/// into `Drained` (as an earlier version of this file did) let a healthy
/// but slightly slower resume print the headline "realtime-resume
/// violated" phrase for a stop condition of this rig's own choosing.
enum StopReason {
    Drained(String),
    BudgetExhausted(String),
    /// The read failed outright (a connection error, or `SseTail`'s own
    /// desync-poison latch) — the rig could not finish draining the stream.
    RigFailure(String),
}

struct ResumeDrain {
    found: BTreeSet<u64>,
    frames_read: usize,
    stop: StopReason,
}

/// Extracts the numeric event id — the part before the `.` — from a wire
/// SSE frame id (`"<id>.<seq>"`, DESIGN §6's cursor format, `serve.rs`'s
/// `encode_frame`). `None` if it doesn't parse.
fn frame_event_id(wire_id: &str) -> Option<u64> {
    wire_id.split('.').next()?.parse().ok()
}

/// Reads the resumed connection until every id in `want` is found, the
/// frame budget is exhausted, delivery goes quiet, or the read fails
/// outright — see `StopReason`. Also enforces two whole-stream invariants
/// as it reads (review Important 1 — "EXACTLY the missed ids" was
/// previously only set-equality, so a stream that reordered or duplicated
/// frames within itself was invisible): the numeric event id must strictly
/// increase across EVERY frame, any type (a real `Last-Event-ID` client
/// would misorder or silently drop events on a stream that didn't hold
/// this), and no `invoice_finalized` id may be found twice WITHIN this one
/// resumed connection (distinct from `redelivery_violation_message`'s
/// cross-phase check against ids already seen before the kill). Returns
/// `Err` rather than asserting/panicking (review Minor 10) so a violation
/// here still flows through the caller's `finish` — the scenario's
/// recorded evidence must survive it, not be skipped by an unwind.
fn drain_resume(
    tail: &mut SseTail,
    want: &BTreeSet<u64>,
    max_frames: usize,
) -> Result<ResumeDrain, String> {
    let mut found = BTreeSet::new();
    let mut frames_read = 0usize;
    let mut consecutive_timeouts = 0u32;
    let mut last_event_id: Option<u64> = None;
    let stop = loop {
        if want.is_subset(&found) {
            break StopReason::Drained("every wanted id was found".to_string());
        }
        if frames_read >= max_frames {
            break StopReason::BudgetExhausted(format!("hit the {max_frames}-frame read budget"));
        }
        match tail.next_frame(FRAME_READ_TIMEOUT) {
            Ok(Some(frame)) => {
                frames_read += 1;
                consecutive_timeouts = 0;
                let Some(event_id) = frame_event_id(&frame.id) else {
                    return Err(format!(
                        "resumed stream: a frame carried an unparseable id {:?} (expected \
                         \"<id>.<seq>\"): frame {frame:?}",
                        frame.id
                    ));
                };
                if let Some(last) = last_event_id
                    && event_id <= last
                {
                    return Err(format!(
                        "realtime-resume violated: frame id {event_id} did not strictly \
                         increase after {last} on the resumed stream — a real Last-Event-ID \
                         client would misorder or silently miss events this way; frame: {frame:?}"
                    ));
                }
                last_event_id = Some(event_id);
                if frame.event == "invoice_finalized"
                    && let Some(id) = extract_u64_field(&frame.data, "invoice")
                {
                    if found.contains(&id) {
                        return Err(format!(
                            "realtime-resume violated: invoice_finalized({id}) was delivered \
                             more than once WITHIN this one resumed connection — a duplicate/\
                             re-delivery bug on a single stream, distinct from a cross-phase \
                             redelivery"
                        ));
                    }
                    found.insert(id);
                }
            }
            Ok(None) => {
                break StopReason::Drained(
                    "the connection closed cleanly before every wanted id arrived".to_string(),
                );
            }
            Err(e) if e.kind() == io::ErrorKind::TimedOut => {
                consecutive_timeouts += 1;
                if consecutive_timeouts >= MAX_CONSECUTIVE_TIMEOUTS {
                    break StopReason::Drained(format!(
                        "{consecutive_timeouts} consecutive timeouts with nothing new arriving"
                    ));
                }
            }
            Err(e) => break StopReason::RigFailure(e.to_string()),
        }
    };
    Ok(ResumeDrain {
        found,
        frames_read,
        stop,
    })
}

// -------------------------------------------------------------- vacuity + classify

/// Pre-kill vacuity (anti-vacuity guard #1, brief + T11's lesson applied
/// up front): the pre-kill partial drain must NOT have already delivered
/// every miss-phase invoice — else there is nothing left to prove resume
/// against and the oracle would trivially "pass" having tested nothing.
fn nothing_left_to_resume_message(miss_rounds: usize, read_before_kill: usize) -> String {
    format!(
        "the resume oracle is vacuous: the pre-kill drain ({read_before_kill} frame(s)) already \
         delivered all {miss_rounds} miss-phase invoice(s) before the kill landed — nothing was \
         left undelivered to prove resume against; refusing to run a resume check with an empty \
         missing set"
    )
}

/// Resume vacuity (anti-vacuity guard #2): the resumed connection must have
/// delivered at least one frame while ids were still outstanding — a resume
/// that reads nothing at all proves nothing about whether the missed events
/// were ever redelivered (distinct from `classify_resume`'s "drained but
/// still missing some" finding, which DID read frames).
fn resume_drained_nothing_message(missing_len: usize) -> String {
    format!(
        "the resume oracle is vacuous: the resumed connection delivered ZERO frames while \
         {missing_len} miss-phase invoice(s) were still outstanding — a resume that reads \
         nothing proves nothing; refusing to treat this as a pass or a failure"
    )
}

/// Composes the resume-oracle failure message: `Drained` (no protocol/IO
/// error, and not this rig's own stopping choice) earns the unqualified
/// "realtime-resume violated" wording; `BudgetExhausted` and `RigFailure`
/// are both worded to never contain that phrase — one names the rig's own
/// frame-count budget as the reason it can't conclude (review Important 2),
/// the other a hard read failure. Pure and separate from `drain_resume`'s
/// real read loop on purpose, same rationale as `classify`: the
/// `classify_resume_*` tests below hand-build `missing`/`stop` and pin all
/// branches byte-exact with zero syscalls.
fn classify_resume(missing: &BTreeSet<u64>, stop: &StopReason, acked_len: usize) -> String {
    // Hard rule 4 (negative space), same guard as `acked_durable.rs`'s
    // `classify`: nothing stops a future caller from passing an empty
    // `missing` — a durability verdict requires at least one.
    assert!(
        !missing.is_empty(),
        "classify_resume: called with no missing ids — a resume verdict requires at least one"
    );
    let missing_list: Vec<u64> = missing.iter().copied().collect();
    match stop {
        StopReason::Drained(reason) => format!(
            "realtime-resume violated: {} of {acked_len} miss-phase invoice(s) never arrived \
             on the resumed stream even though it drained cleanly ({reason}): missing invoice \
             ids {missing_list:?}",
            missing.len()
        ),
        StopReason::BudgetExhausted(reason) => format!(
            "realtime-resume oracle could not conclude: {} of {acked_len} miss-phase \
             invoice(s) were still outstanding when this rig's OWN read budget ran out \
             ({reason}) — NOT evidence of a resume violation (the rig stopped asking; the \
             server was never shown to have failed to send anything). Raise \
             RESUME_MAX_FRAMES or investigate why more frames arrived than expected: missing \
             invoice ids {missing_list:?}",
            missing.len()
        ),
        StopReason::RigFailure(reason) => format!(
            "realtime-resume oracle could not verify {} of {acked_len} miss-phase invoice(s) — \
             RIG FAILURE, not a proven resume violation (the resumed connection failed before \
             it could finish draining: {reason}): missing invoice ids {missing_list:?}",
            missing.len()
        ),
    }
}

/// The redelivery violation: an id the resumed connection served that the
/// client had already consumed before the kill — an exact `Last-Event-ID`
/// resume must never repeat a frame. Pure, so it joins every other verdict
/// string in this file pinned byte-exact (review Minor 7 — this used to be
/// built inline in the IO path and was the only unpinned verdict in either
/// file).
fn redelivery_violation_message(redelivered: &BTreeSet<u64>) -> String {
    assert!(
        !redelivered.is_empty(),
        "redelivery_violation_message: called with no redelivered ids — a redelivery verdict \
         requires at least one"
    );
    format!(
        "realtime-resume violated: the resumed connection RE-delivered {} id(s) already seen \
         before the kill: {redelivered:?} — Last-Event-ID resume must never repeat a frame the \
         client already consumed",
        redelivered.len()
    )
}

/// The symmetric violation: an id the resumed connection delivered that was
/// neither missing nor a redelivery — this oracle never writes during the
/// resume phase, so there is no legitimate source for one. Extracted and
/// pinned alongside `redelivery_violation_message` for the same reason.
fn unexpected_delivery_message(unexpected: &BTreeSet<u64>) -> String {
    assert!(
        !unexpected.is_empty(),
        "unexpected_delivery_message: called with no unexpected ids — a verdict requires at \
         least one"
    );
    format!(
        "realtime-resume violated: the resumed stream delivered id(s) that were neither \
         missing nor a redelivery — unexpected: {unexpected:?} (this oracle never writes \
         during the resume phase)"
    )
}

// ------------------------------------------------------------------ teardown

/// Dumps the scenario script, prints it on failure, and cleans up temp dirs
/// ONLY on success — `acked_durable.rs`'s `finish`, same rationale (crate
/// honesty contract: a real finding's evidence — the scenario log and the
/// data directory the message points at — must still exist afterward).
fn finish(
    tag: &str,
    scenario: &mut Scenario,
    data_dir: &Path,
    scenario_root: &Path,
    result: Result<(), String>,
) -> Result<(), String> {
    match &result {
        Err(e) => scenario.record(format!("FAILED: {e}")),
        Ok(()) => scenario.record("run succeeded"),
    }
    let script = scenario.dump().map_err(|e| e.to_string())?;
    match &result {
        Err(e) => {
            eprintln!(
                "edge-harness realtime_resume[{tag}]: FAILED — scenario script at {}",
                script.display()
            );
            eprintln!("edge-harness realtime_resume[{tag}]: {e}");
            eprintln!(
                "edge-harness realtime_resume[{tag}]: NOT deleting data dir ({}) or scenario \
                 root ({}) — preserved for inspection",
                data_dir.display(),
                scenario_root.display()
            );
        }
        Ok(()) => {
            let _ = std::fs::remove_dir_all(data_dir);
            let _ = std::fs::remove_dir_all(scenario_root);
        }
    }
    result
}

// --------------------------------------------------------------- the oracle

fn run_oracle(node_bin: &Path) -> Result<(), String> {
    let tag = "realtime_resume";
    let scenario_root = temp_dir(&format!("{tag}-scenario-root"));
    std::fs::create_dir_all(&scenario_root).map_err(|e| e.to_string())?;
    let mut scenario = Scenario::new(&scenario_root, tag).map_err(|e| e.to_string())?;
    let data_dir = temp_dir(&format!("{tag}-data"));
    let mut proxy_holder: Option<TcpProxy> = None;

    let result = (|| -> Result<(), String> {
        let node_addr = free_addr();
        let args = vec![
            "--addr".to_string(),
            node_addr.clone(),
            "--data-dir".to_string(),
            data_dir.to_str().unwrap().to_string(),
        ];
        let mut node =
            ChildNode::spawn(node_bin, &args, &mut scenario).map_err(|e| e.to_string())?;
        let proxy = TcpProxy::start(&node_addr).map_err(|e| e.to_string())?;
        let proxy_addr = proxy.addr();
        proxy_holder = Some(proxy);
        let mut http = Http::new(proxy_addr);
        login(&mut http, &mut scenario)?;
        create_customer(&mut http, 1)?;
        scenario.record("customer 1 created — beginning the live phase");

        // ---- live phase: prove the tail delivers what we just wrote -----
        let mut tail = http
            .sse_tail("/events/feed", None)
            .map_err(|e| format!("opening the SSE tail failed: {e}"))?;
        let mut last_id: Option<String> = None;
        let mut delivered_before_kill: BTreeSet<u64> = BTreeSet::new();

        let live_ids: BTreeSet<u64> = (1..=LIVE_ROUNDS).collect();
        for &id in &live_ids {
            create_and_finalize_invoice(&mut http, id, 1)?;
        }
        scenario.record(format!(
            "live phase: {LIVE_ROUNDS} invoice(s) created and finalized — draining until all \
             are observed"
        ));
        drain_until_seen(
            &mut tail,
            &live_ids,
            &mut delivered_before_kill,
            &mut last_id,
            Instant::now() + LIVE_DRAIN_DEADLINE,
        )
        .map_err(|e| format!("live-phase drain (positive control) failed: {e}"))?;
        scenario.record("live phase: all live-round invoices observed on the tail");

        // ---- miss phase: ack writes without reading them ----------------
        let miss_ids: BTreeSet<u64> = (LIVE_ROUNDS + 1..=LIVE_ROUNDS + MISS_ROUNDS).collect();
        for &id in &miss_ids {
            create_and_finalize_invoice(&mut http, id, 1)?;
        }
        scenario.record(format!(
            "miss phase: {MISS_ROUNDS} more invoice(s) created and finalized, acked but not \
             yet read"
        ));

        let read_before_kill = drain_up_to(
            &mut tail,
            PRE_KILL_DRAIN_FRAMES,
            &mut delivered_before_kill,
            &mut last_id,
        );
        scenario.record(format!(
            "pre-kill partial drain: read {read_before_kill} raw frame(s) (budget \
             {PRE_KILL_DRAIN_FRAMES}), last_id={last_id:?}"
        ));

        let missing_before_kill: BTreeSet<u64> = miss_ids
            .difference(&delivered_before_kill)
            .copied()
            .collect();
        // Anti-vacuity guard #1 (brief + T11's lesson applied up front): the
        // resume test is meaningless if the pre-kill drain happened to
        // deliver every miss-phase invoice anyway.
        if missing_before_kill.is_empty() {
            return Err(nothing_left_to_resume_message(
                MISS_ROUNDS as usize,
                read_before_kill,
            ));
        }
        scenario.record(format!(
            "{} of {MISS_ROUNDS} miss-phase invoice(s) left undelivered before the kill: {:?}",
            missing_before_kill.len(),
            missing_before_kill
        ));

        let resume_cursor = last_id
            .clone()
            .ok_or_else(|| "no frame was ever read before the kill — no cursor to resume from (the live phase should have set one)".to_string())?;

        // ---- kill mid-stream, restart, resume ----------------------------
        // The tail stays open THROUGH the kill (review Minor 4 — this used
        // to `drop(tail)` first, so the server saw a clean client
        // disconnect and only then died with no live SSE connection at
        // all, which is not "kill9 mid-stream"). Dropped only after, once
        // the process is already gone, to release our side of a socket
        // nothing is listening on anymore.
        scenario.record("killing the node mid-stream, with events still undelivered");
        node.kill9(&mut scenario);
        drop(tail);
        node.restart(&mut scenario).map_err(|e| e.to_string())?;
        scenario.record("node restarted — reconnecting with Last-Event-ID from before the kill");

        let mut resumed_tail = http
            .sse_tail("/events/feed", Some(&resume_cursor))
            .map_err(|e| format!("reconnecting the SSE tail after restart failed: {e}"))?;
        let drain = drain_resume(&mut resumed_tail, &missing_before_kill, RESUME_MAX_FRAMES)?;
        scenario.record(format!(
            "resume drain: read {} frame(s), found {:?}",
            drain.frames_read, drain.found
        ));

        // Anti-vacuity guard #2: the resumed connection must have delivered
        // SOMETHING while ids were outstanding.
        if drain.frames_read == 0 {
            return Err(resume_drained_nothing_message(missing_before_kill.len()));
        }

        // No re-delivery of ids the pre-kill drain already consumed — an
        // exact resume must never repeat what it already sent.
        let redelivered: BTreeSet<u64> = drain
            .found
            .intersection(&delivered_before_kill)
            .copied()
            .collect();
        if !redelivered.is_empty() {
            return Err(redelivery_violation_message(&redelivered));
        }

        let still_missing: BTreeSet<u64> = missing_before_kill
            .difference(&drain.found)
            .copied()
            .collect();
        if !still_missing.is_empty() {
            return Err(classify_resume(
                &still_missing,
                &drain.stop,
                missing_before_kill.len(),
            ));
        }
        // Symmetric check: the resumed set must be EXACTLY the missing set,
        // not a superset either (an id neither missing nor a redelivery
        // would be a genuinely unexpected event — this oracle never creates
        // invoices during the resume phase). `Err`, not `assert!` (review
        // Minor 10) — a violation here should still flow through `finish`
        // so its evidence survives, rather than an unwind bypassing it.
        let unexpected: BTreeSet<u64> = drain
            .found
            .difference(&missing_before_kill)
            .copied()
            .collect();
        if !unexpected.is_empty() {
            return Err(unexpected_delivery_message(&unexpected));
        }

        scenario.record(format!(
            "resume verified: exactly the {} missing invoice id(s) arrived, no redelivery, no \
             extras",
            missing_before_kill.len()
        ));

        // ---- negative arm: garbage cursor -> the pinned 400 --------------
        let garbage = http
            .get("/events/feed?cursor=not-a-cursor")
            .map_err(|e| format!("garbage-cursor request failed: {e}"))?;
        if garbage.status != 400 {
            return Err(format!(
                "garbage cursor: expected status 400, got {} body {:?}",
                garbage.status,
                String::from_utf8_lossy(&garbage.body)
            ));
        }
        let expected_400 =
            "cursor must be \"<id>.<seq>\" — got \"not-a-cursor\"; omit it to tail from now";
        let message = extract_error_message(&garbage.body).ok_or_else(|| {
            format!(
                "garbage cursor: 400 body did not contain a parseable \"error\" field: {:?}",
                String::from_utf8_lossy(&garbage.body)
            )
        })?;
        if message != expected_400 {
            return Err(format!(
                "garbage cursor: expected the pinned 400 text {expected_400:?}, got {message:?}"
            ));
        }
        scenario.record("garbage cursor: pinned 400 confirmed byte-exact");

        Ok(())
    })();

    if let Some(proxy) = &proxy_holder {
        proxy.stop(&mut scenario);
    }
    finish(tag, &mut scenario, &data_dir, &scenario_root, result)
}

#[test]
#[ignore]
fn realtime_resume_wire_oracle() {
    let bins = build_bins().expect("build_bins");
    run_oracle(&bins.node).expect(
        "realtime SSE resume must deliver exactly the missed events after a kill9+restart — see \
         stderr above for the scenario script",
    );
}

// ---------------------------------------------------------- named-error pins
//
// Same discipline as `acked_durable.rs`'s pins (re-review round 2
// "Important"): `classify_resume` and the two vacuity-message functions are
// pure functions of their arguments (no `Http`, no `Scenario`, no
// filesystem), so they are pinned directly with hand-built inputs — zero
// syscalls, joining the crate's un-ignored zero-IO set.

#[test]
fn classify_resume_drained_is_an_unqualified_violation() {
    let missing: BTreeSet<u64> = [7, 12].into_iter().collect();
    // Review Important 2: budget exhaustion no longer routes through
    // `Drained` (see `classify_resume_budget_exhausted_is_not_a_violation`
    // below) — quiescence is one of the two reasons that legitimately do.
    let stop = StopReason::Drained("3 consecutive timeouts with nothing new arriving".to_string());
    let message = classify_resume(&missing, &stop, 10);
    assert_eq!(
        message,
        "realtime-resume violated: 2 of 10 miss-phase invoice(s) never arrived on the resumed \
         stream even though it drained cleanly (3 consecutive timeouts with nothing new \
         arriving): missing invoice ids [7, 12]"
    );
}

#[test]
fn classify_resume_budget_exhausted_is_not_a_violation() {
    let missing: BTreeSet<u64> = [7, 12].into_iter().collect();
    let stop = StopReason::BudgetExhausted("hit the 30-frame read budget".to_string());
    let message = classify_resume(&missing, &stop, 10);
    assert_eq!(
        message,
        "realtime-resume oracle could not conclude: 2 of 10 miss-phase invoice(s) were still \
         outstanding when this rig's OWN read budget ran out (hit the 30-frame read budget) — \
         NOT evidence of a resume violation (the rig stopped asking; the server was never \
         shown to have failed to send anything). Raise RESUME_MAX_FRAMES or investigate why \
         more frames arrived than expected: missing invoice ids [7, 12]"
    );
    assert!(
        !message.contains("realtime-resume violated"),
        "the rig's own frame budget running out must never speak in the violation's voice: \
         {message:?}"
    );
}

#[test]
fn classify_resume_rig_failure_is_never_worded_as_a_violation() {
    let missing: BTreeSet<u64> = [3].into_iter().collect();
    let stop = StopReason::RigFailure("connection reset by peer".to_string());
    let message = classify_resume(&missing, &stop, 5);
    assert_eq!(
        message,
        "realtime-resume oracle could not verify 1 of 5 miss-phase invoice(s) — RIG FAILURE, \
         not a proven resume violation (the resumed connection failed before it could finish \
         draining: connection reset by peer): missing invoice ids [3]"
    );
    assert!(
        !message.contains("realtime-resume violated"),
        "a rig failure must never contain the violation phrase: {message:?}"
    );
}

#[test]
#[should_panic(expected = "a resume verdict requires at least one")]
fn classify_resume_panics_on_an_empty_missing_set() {
    let missing: BTreeSet<u64> = BTreeSet::new();
    let stop = StopReason::Drained("every wanted id was found".to_string());
    let _ = classify_resume(&missing, &stop, 10);
}

#[test]
fn nothing_left_to_resume_message_names_vacuity() {
    let msg = nothing_left_to_resume_message(10, 3);
    assert_eq!(
        msg,
        "the resume oracle is vacuous: the pre-kill drain (3 frame(s)) already delivered all \
         10 miss-phase invoice(s) before the kill landed — nothing was left undelivered to \
         prove resume against; refusing to run a resume check with an empty missing set"
    );
}

#[test]
fn resume_drained_nothing_message_names_vacuity() {
    let msg = resume_drained_nothing_message(6);
    assert_eq!(
        msg,
        "the resume oracle is vacuous: the resumed connection delivered ZERO frames while 6 \
         miss-phase invoice(s) were still outstanding — a resume that reads nothing proves \
         nothing; refusing to treat this as a pass or a failure"
    );
}

#[test]
fn extract_u64_field_reads_the_invoice_id() {
    assert_eq!(extract_u64_field(r#"{"invoice":42}"#, "invoice"), Some(42));
    assert_eq!(
        extract_u64_field(r#"{"invoice":7,"amount_cents":500}"#, "invoice"),
        Some(7)
    );
    assert_eq!(extract_u64_field(r#"{"other":1}"#, "invoice"), None);
}

/// `InvoiceFinalized.invoice` is `Id<Invoice>` (task 5): it crosses as a
/// quoted decimal string, e.g. `{"invoice":"42"}` — this is the shape the
/// real `invoice_finalized` frames this file scans actually carry.
#[test]
fn extract_u64_field_reads_a_quoted_id_string() {
    assert_eq!(
        extract_u64_field(r#"{"invoice":"42"}"#, "invoice"),
        Some(42)
    );
}

/// Hand-built to match exactly what `Response::error(400, ...)` (compact
/// JSON, no ascii-escaping of non-ASCII bytes) produces for the cursor-400
/// text this oracle pins — proves the parser recovers it byte-exact,
/// including the embedded escaped quotes, with zero syscalls.
#[test]
fn extract_error_message_unescapes_the_cursor_400_body() {
    let body = "{\"error\":\"cursor must be \\\"<id>.<seq>\\\" — got \\\"not-a-cursor\\\"; omit \
                 it to tail from now\"}"
        .as_bytes()
        .to_vec();
    assert_eq!(
        extract_error_message(&body).as_deref(),
        Some("cursor must be \"<id>.<seq>\" — got \"not-a-cursor\"; omit it to tail from now")
    );
}

#[test]
fn extract_error_message_none_without_an_error_field() {
    assert_eq!(extract_error_message(br#"{"id":1}"#), None);
}

#[test]
fn frame_event_id_reads_the_numeric_prefix() {
    assert_eq!(frame_event_id("12.34"), Some(12));
    assert_eq!(frame_event_id("0.0"), Some(0));
    assert_eq!(frame_event_id("not-a-cursor"), None);
    assert_eq!(frame_event_id(""), None);
}

#[test]
fn redelivery_violation_message_names_the_repeated_ids() {
    let redelivered: BTreeSet<u64> = [5, 6].into_iter().collect();
    let msg = redelivery_violation_message(&redelivered);
    assert_eq!(
        msg,
        "realtime-resume violated: the resumed connection RE-delivered 2 id(s) already seen \
         before the kill: {5, 6} — Last-Event-ID resume must never repeat a frame the client \
         already consumed"
    );
}

#[test]
#[should_panic(expected = "a redelivery verdict requires at least one")]
fn redelivery_violation_message_panics_on_an_empty_set() {
    let _ = redelivery_violation_message(&BTreeSet::new());
}

#[test]
fn unexpected_delivery_message_names_the_extra_ids() {
    let unexpected: BTreeSet<u64> = [9].into_iter().collect();
    let msg = unexpected_delivery_message(&unexpected);
    assert_eq!(
        msg,
        "realtime-resume violated: the resumed stream delivered id(s) that were neither \
         missing nor a redelivery — unexpected: {9} (this oracle never writes during the \
         resume phase)"
    );
}

#[test]
#[should_panic(expected = "a verdict requires at least one")]
fn unexpected_delivery_message_panics_on_an_empty_set() {
    let _ = unexpected_delivery_message(&BTreeSet::new());
}
