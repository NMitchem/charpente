//! Oracle 2 (spec 2026-07-24 introspection-edge-harness design, T12): the
//! wire-level successor to the replication sub-project's in-sim property 8
//! (a converged follower never loses an acked write across promote),
//! automating the replication demo-of-record (M1 sub-project 2 close-out).
//! Drives a real primary + follower pair (distinct temp dirs, shared
//! `--secret`, the peer link through a `TcpProxy`), 50 acked customer
//! creates against the primary with kill9+restart interleaved on the
//! primary only, `kill9` the primary for good, runs the REAL `charpente`
//! CLI's `promote` against the follower, and checks the promoted follower
//! for loss. The negative arm restarts the OLD primary pointed at the new
//! one and checks for the durable-role demote/re-seed refusal.
//!
//! **Bounded-loss honesty (DESIGN/DECISIONS §6):** replication ships
//! asynchronously, so an acked-on-primary write may not have shipped by the
//! time the primary dies — that is the documented, bounded loss window, not
//! a bug. This oracle therefore drives writes through a ship-ack barrier:
//! after each POST, it polls the follower's own `/customers/:id` until the
//! row appears (read-your-replication) before counting that id "converged".
//! The headline zero-loss claim binds ONLY converged ids (property 8's own
//! premise — a converged id must never be lost); the acked-but-never-
//! observed-converged tail is reported in the scenario, never silently
//! dropped, and checked too, but its loss (if any) is not a failure.
//!
//! **The resurrection guard (M2 sub-project 0b, closed 2026-07-31):**
//! `run_oracle`'s negative arm and the separate
//! `failover_resurrected_primary_does_not_accept_writes` test below cover the
//! two restart shapes an operator can reach after a `promote`. Restarting an
//! old primary with a config that CONTRADICTS the durable role file (an
//! attempted demote) prints the `role.rs` notice to stderr; that notice is
//! the operator's cue to intervene, never a self-enforced refusal, and the
//! node keeps serving. Restarting it with its ORIGINAL, unchanged config
//! produces no notice at all — which is why, before the witness boot gate,
//! that path produced a silent second writable primary. It no longer does:
//! such a node now withholds writes until it has confirmed that no follower
//! it had shipped to has since been promoted. See
//! `failover_resurrected_primary_does_not_accept_writes` for the wire oracle,
//! and DESIGN §5's boot-gate paragraph for the mechanism.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use std::collections::HashSet;
use std::path::PathBuf;
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use edge_harness::{ChildNode, Http, Scenario, TcpProxy, build_bins};

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

fn free_addr() -> String {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("loopback bind")
        .local_addr()
        .expect("bound listener has an address")
        .to_string()
}

/// Shared peer secret for the primary/follower pair — replication T7's peer
/// auth, required to be non-empty (a `Some("")` secret is a named config
/// error, spec 2026-07-24 amendment) and identical on both sides.
const SECRET: &str = "edge-harness-t12-secret";

// ------------------------------------------------------------ ad-hoc "rng"
//
// Same rationale and shape as `acked_durable.rs`'s `Rng` (this crate makes
// no seed promise — which rounds interleave a primary kill only needs to
// vary run to run, not replay byte-identically); reimplemented rather than
// shared, matching this crate's established convention for call-site-local
// scanners/helpers.

static RNG_SALT: AtomicU64 = AtomicU64::new(0);

struct Rng(u64);

impl Rng {
    fn new() -> Rng {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        let salt = RNG_SALT.fetch_add(1, Ordering::Relaxed);
        Rng((nanos ^ ((std::process::id() as u64) << 32) ^ salt) | 1)
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next_u64() % n
    }
}

const KILL_DELAYS_MS: [u64; 3] = [0, 5, 20];

fn pick_kill_rounds(rng: &mut Rng, rounds: u64, count: u64) -> HashSet<u64> {
    let count = count.min(rounds);
    let mut picked = HashSet::new();
    while (picked.len() as u64) < count {
        picked.insert(1 + rng.below(rounds));
    }
    picked
}

// ----------------------------------------------------------------- constants

/// Fixed by the brief ("50 acked writes"), not overridable — Oracle 2 is a
/// scripted demo automation, not a tunable fuzz-style oracle like Oracle 1.
const ROUNDS: u64 = 50;
/// How many of the 50 rounds interleave a kill9+restart of the PRIMARY only
/// (never the follower) — plural per the brief.
const KILL_ROUNDS_COUNT: u64 = 3;
/// The brief's anti-vacuity floor: at least this many of the 50 acked writes
/// must be OBSERVED converged (read back from the follower) before promote,
/// or the zero-loss assertion below would be checking an empty/tiny set.
const MIN_CONVERGED: usize = 40;
/// How long `run_resurrection_oracle`'s Phase B waits for the conclusive
/// witness refusal to reach the old primary's stderr once the witness is
/// back: the probe's reconnect backoff (`RECONNECT_NANOS`, 500 ms) plus the
/// witness's heartbeat interval (`HEARTBEAT_NANOS`, 1 s — what a witness with
/// nothing to ship answers with), plus a wide margin for process start. Not a
/// tuning knob: needing to raise it means the probe's pacing regressed.
const RESURRECTION_REFUSAL_TIMEOUT: Duration = Duration::from_secs(20);
/// Per-round bound on how long the ship-ack barrier waits for the follower
/// to catch up before giving up on THAT round (not a failure — it just
/// leaves the id in the acked-but-not-converged tail).
const CONVERGE_TIMEOUT: Duration = Duration::from_secs(3);
const CONVERGE_POLL_INTERVAL: Duration = Duration::from_millis(20);
/// How long a mid-load `login` waits for a just-restarted primary to open its
/// write gate. A durable primary with a recorded witness boots `Verifying`
/// and probes that witness before it accepts a write (DESIGN §5, the boot
/// gate); `ChildNode::restart` returns when the HTTP port accepts, which is
/// before the kernel thread has resolved the probe — so the first write can
/// meet the documented 503 (nightlies 2026-09-09 and 2026-09-12, both through
/// `run_oracle`). One loopback round-trip resolves it; 10 s is a wide margin
/// that stays well under `WITNESS_DEADLINE_NANOS` (30 s), so a gate that
/// never opens still fails here by name instead of dissolving into that
/// deadline's read-only fallback. Not a tuning knob: needing to raise it
/// means the probe regressed.
const WRITE_GATE_TIMEOUT: Duration = Duration::from_secs(10);

// ------------------------------------------------------------------- client

/// The witness-gate 503 body, single-witness rendering, pinned byte-exact
/// (hard rule 5): `verifying_message` (http/app.rs) pluralises, so a
/// regression that lost the singular form fails by text at every site that
/// compares against this.
fn witness_gate_503(follower_repl_addr: &str) -> String {
    format!(
        "this node is not accepting writes yet: it was a primary before restarting and has \
         not confirmed that its follower {follower_repl_addr} was never promoted — reads \
         are being served normally, and writes resume automatically once that node answers; \
         if it is gone for good, see GUIDE §10's witness-set remedy"
    )
}

/// `login` against a primary restarted mid-load: retries ONLY while the body
/// is the witness-gate 503 — the one transient a restarted primary is
/// documented to answer with — and fails on anything else exactly as `login`
/// does. Records how long the gate took, so a slow probe shows in the script.
fn login_once_writable(
    http: &mut Http,
    scenario: &mut Scenario,
    follower_repl_addr: &str,
) -> Result<String, String> {
    let gate = witness_gate_503(follower_repl_addr);
    let started = Instant::now();
    let deadline = started + WRITE_GATE_TIMEOUT;
    let mut gated = 0u32;
    loop {
        let resp = http
            .post("/login", "\"edge-tester\"")
            .map_err(|e| format!("login request failed: {e}"))?;
        let body = String::from_utf8_lossy(&resp.body).to_string();
        if resp.status == 503 && body.contains(&gate) {
            gated += 1;
            if Instant::now() >= deadline {
                return Err(format!(
                    "the restarted primary's write gate never opened within \
                     {WRITE_GATE_TIMEOUT:?} ({gated} login attempt(s) answered the witness-gate \
                     503) — its witness {follower_repl_addr} is alive and a follower, so the \
                     probe should have resolved in one round-trip; body {body:?}"
                ));
            }
            std::thread::sleep(CONVERGE_POLL_INTERVAL);
            continue;
        }
        if resp.status != 200 {
            return Err(format!(
                "login failed: status {} body {body:?}",
                resp.status
            ));
        }
        let token = resp
            .as_json_string()
            .ok_or_else(|| "login response body was not a JSON string token".to_string())?;
        http.set_session(token.clone());
        scenario.record(format!(
            "logged in after the restart; write gate open after {gated} gated attempt(s), {:?}",
            started.elapsed()
        ));
        return Ok(token);
    }
}

fn login(http: &mut Http, scenario: &mut Scenario) -> Result<String, String> {
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
    http.set_session(token.clone());
    scenario.record("logged in through the proxy");
    Ok(token)
}

/// Polls the follower's `/customers/:id` until it serves the row
/// (read-your-replication) or `CONVERGE_TIMEOUT` elapses — the ship-ack
/// barrier. Returns whether it converged within the bound; NOT converging is
/// not itself a failure (the acked-but-not-yet-shipped tail DECISIONS §6
/// documents), just a fact this id's caller records.
fn wait_converged(follower_http: &mut Http, id: u64) -> bool {
    let deadline = Instant::now() + CONVERGE_TIMEOUT;
    loop {
        if let Ok(resp) = follower_http.get(&format!("/customers/{id}"))
            && resp.status == 200
        {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(CONVERGE_POLL_INTERVAL);
    }
}

/// `(id, reason)` pairs — same shape and rationale as `acked_durable.rs`'s
/// `IdReasons` (a named alias per clippy's `type_complexity`, applied at
/// every site of this shape).
type IdReasons = Vec<(u64, String)>;

/// The genuine-vs-rig-failure predicate every classification in this file
/// shares: a GET that came back `status 404` is a CONFIRMED absence (the
/// row really isn't there); anything else (401, 500, a transport error) is
/// the rig failing to answer the question, not evidence the row is gone.
/// Extracted so `classify_converged_loss` (the violation path) and
/// `describe_tail_loss`/the tail-exercising test (the expected, non-
/// violation DECISIONS §6 path) apply the IDENTICAL discrimination rather
/// than each growing its own — the same class of hole T11 was built to
/// close (a rig failure silently counted as a genuine loss, in either
/// direction).
fn confirmed_absent_count(reasons: &IdReasons) -> usize {
    reasons
        .iter()
        .filter(|(_, reason)| reason == "status 404")
        .count()
}

/// Composes the post-promote loss message for converged ids: the SAME
/// three-way classification shape as `acked_durable.rs`'s `classify`
/// (genuine-absence / pure-rig-failure / mixed), applied to "a converged id
/// missing after promote" instead of "an acked id missing after restart" —
/// property 8's own premise is that a CONVERGED id is the one that must
/// never be lost, so this is the failure this oracle exists to catch. Pure
/// and separated from the real GET loop for the same reason `classify` is:
/// zero-syscall pin tests below cover every branch byte-exact.
fn classify_converged_loss(missing: &IdReasons, converged_len: usize) -> String {
    assert!(
        !missing.is_empty(),
        "classify_converged_loss: called with no missing ids — a failover verdict requires at \
         least one"
    );
    let confirmed = confirmed_absent_count(missing);
    if confirmed == missing.len() {
        format!(
            "failover violated: {} of {converged_len} converged id(s) missing on the promoted \
             follower: {missing:?} — a converged id (already observed present on the follower \
             BEFORE promote) must never be lost",
            missing.len()
        )
    } else if confirmed == 0 {
        format!(
            "failover oracle could not verify {} of {converged_len} converged id(s) after \
             promote — RIG FAILURE, not a proven failover violation (at least one GET failed \
             for a reason other than a genuine \"status 404\"): {missing:?}",
            missing.len()
        )
    } else {
        format!(
            "failover violated: {confirmed} of {converged_len} converged id(s) confirmed \
             genuinely absent (status 404) on the promoted follower, alongside {} GET(s) that \
             failed for a rig reason: {missing:?}",
            missing.len() - confirmed
        )
    }
}

/// The DECISIONS §6 counterpart to `classify_converged_loss`, for the
/// acked-but-not-observed-converged TAIL — expected, bounded loss, never a
/// violation, but the SAME `confirmed_absent_count` predicate applies: a
/// tail id missing because the GET hit a rig failure (a 401, a 500, a
/// transport hiccup) is not evidence of the bounded-loss window this
/// oracle is supposed to be naming — it's evidence the check itself
/// couldn't finish, which deserves distinct wording just as much on the
/// expected side as on the violation side. Never returns text containing
/// "violated" — that word is `classify_converged_loss`'s alone.
fn describe_tail_loss(tail_missing: &IdReasons, non_converged_tail: usize) -> String {
    if tail_missing.is_empty() {
        return format!(
            "0 of {non_converged_tail} non-converged-tail id(s) missing — every acked-but-not-\
             observed-converged id made it to the follower anyway"
        );
    }
    let confirmed = confirmed_absent_count(tail_missing);
    if confirmed == tail_missing.len() {
        format!(
            "{confirmed} of {non_converged_tail} non-converged-tail id(s) confirmed genuinely \
             absent (status 404) on the promoted follower — DECISIONS §6 bounded loss, expected \
             and not a failure by itself: {tail_missing:?}"
        )
    } else {
        format!(
            "{confirmed} of {non_converged_tail} non-converged-tail id(s) confirmed genuinely \
             absent (status 404, DECISIONS §6 bounded loss, expected); {} could NOT be verified \
             for a rig reason (not evidence of the bounded-loss window, just a check that \
             couldn't finish): {tail_missing:?}",
            tail_missing.len() - confirmed
        )
    }
}

/// Anti-vacuity (brief, plan-review Important #3): the zero-loss assertion
/// is meaningless if too few of the 50 acked writes were ever OBSERVED
/// converged before promote.
fn vacuous_convergence_message(converged_len: usize, acked_len: usize) -> String {
    format!(
        "the failover oracle is vacuous: only {converged_len} of {acked_len} acked write(s) \
         were observed converged on the follower before promote (need >= {MIN_CONVERGED}) — \
         refusing to run a zero-loss check over too small a converged set"
    )
}

/// Anti-vacuity, the acked-write half: every round should ack (creating a
/// customer has no reason to fail here) — a partial ack means the write
/// phase itself is broken, not that failover is being tested.
fn vacuous_ack_message(acked_len: usize, not_acked: &IdReasons) -> String {
    format!(
        "the failover oracle is vacuous: only {acked_len}/{ROUNDS} rounds acked against the \
         primary (every round should ack); not acked: {not_acked:?}; refusing to run a \
         failover check over a partial write phase"
    )
}

// --------------------------------------------------------------- the oracle

/// What a successful `run_oracle` run observed — used by
/// `failover_non_converged_tail_is_exercised_and_bounded` to prove the
/// acked-but-not-converged tail's reporting path actually executes and
/// stays within bounds, not just that it CAN in principle (review item 4,
/// 2026-07-26: a branch nobody has ever run may simply not work).
/// `tail_missing` carries the REASON per id (not just the id) so a caller
/// can apply `confirmed_absent_count` itself — the same discrimination
/// `classify_converged_loss` applies to the converged set, reused here
/// rather than a parallel "any non-200 counts" check.
struct RunStats {
    converged_len: usize,
    non_converged_tail: usize,
    tail_missing: IdReasons,
}

/// `cut_peer_link_before_round`: when `Some(r)`, the peer-link proxy is
/// permanently stopped (`TcpProxy::stop`) immediately before round `r` is
/// processed — every round from `r` on acks normally (the client talks to
/// the primary directly, never through the peer-link proxy) but can never
/// SHIP to the follower again, deterministically producing acked-but-
/// never-converged ids instead of leaving that outcome to loopback timing
/// luck. `None` (used by `failover_wire_oracle`) never cuts the link —
/// unchanged behavior from before this parameter existed.
fn run_oracle(
    node_bin: &std::path::Path,
    charpente_bin: &std::path::Path,
    cut_peer_link_before_round: Option<u64>,
) -> Result<RunStats, String> {
    let tag = "failover";
    let scenario_root = temp_dir(&format!("{tag}-scenario-root"));
    std::fs::create_dir_all(&scenario_root).map_err(|e| e.to_string())?;
    let mut scenario = Scenario::new(&scenario_root, tag).map_err(|e| e.to_string())?;
    let primary_data = temp_dir(&format!("{tag}-primary-data"));
    let follower_data = temp_dir(&format!("{tag}-follower-data"));
    let mut repl_proxy: Option<TcpProxy> = None;
    let mut old_primary_restart: Option<ChildNode> = None;

    let result = (|| -> Result<RunStats, String> {
        let primary_addr = free_addr();
        let primary_repl_addr = free_addr();
        let follower_addr = free_addr();
        let follower_repl_addr = free_addr();

        let mut primary = ChildNode::spawn(
            node_bin,
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

        let proxy = TcpProxy::start(&primary_repl_addr).map_err(|e| e.to_string())?;
        let proxy_addr = proxy.addr();
        repl_proxy = Some(proxy);
        scenario.record(format!(
            "peer link: follower will connect to the primary THROUGH the proxy at {proxy_addr}"
        ));

        // Kept alive (unread) purely so its `Drop` doesn't kill the follower
        // process partway through the scenario — `acked_durable.rs`'s
        // `_verify_node` idiom.
        let _follower = ChildNode::spawn(
            node_bin,
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
                proxy_addr,
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

        let mut rng = Rng::new();
        let kill_rounds = pick_kill_rounds(&mut rng, ROUNDS, KILL_ROUNDS_COUNT);
        let mut sorted_kill_rounds: Vec<_> = kill_rounds.iter().copied().collect();
        sorted_kill_rounds.sort_unstable();
        scenario.record(format!(
            "plan: {ROUNDS} rounds against the primary, kill rounds = {sorted_kill_rounds:?} \
             (primary only, never the follower)"
        ));

        let mut acked: Vec<u64> = Vec::new();
        let mut not_acked: IdReasons = Vec::new();
        let mut converged: Vec<u64> = Vec::new();

        for round in 1..=ROUNDS {
            if cut_peer_link_before_round == Some(round) {
                // Deliberately, permanently sever the peer link so every
                // round from here on can still ACK (the client talks to the
                // primary directly) but can never SHIP to the follower
                // again — the non-converged-tail branch below, exercised on
                // purpose instead of left to loopback timing luck.
                repl_proxy
                    .as_ref()
                    .expect("the peer-link proxy is always Some by this point")
                    .stop(&mut scenario);
                scenario.record(format!(
                    "round {round}: peer link deliberately cut for good — every round from \
                     here on can ack but can never converge"
                ));
            }
            let id = round;
            let body = format!(r#"{{"name":"Round {id}","email":"round{id}@example.com"}}"#);
            match primary_http.post(&format!("/customers/{id}"), &body) {
                Ok(resp) if resp.status == 200 => {
                    acked.push(id);
                    scenario.record(format!(
                        "round {round}: POST /customers/{id} -> 200 (acked)"
                    ));
                }
                Ok(resp) => {
                    scenario.record(format!(
                        "round {round}: POST /customers/{id} -> {} (not acked)",
                        resp.status
                    ));
                    not_acked.push((id, format!("status {}", resp.status)));
                    continue;
                }
                Err(e) => {
                    scenario.record(format!(
                        "round {round}: POST /customers/{id} failed: {e} (not acked)"
                    ));
                    not_acked.push((id, format!("request failed: {e}")));
                    continue;
                }
            }

            if wait_converged(&mut follower_http, id) {
                converged.push(id);
            } else {
                scenario.record(format!(
                    "round {round}: id {id} did NOT converge to the follower within \
                     {CONVERGE_TIMEOUT:?} — left in the acked-but-not-converged tail"
                ));
            }

            if kill_rounds.contains(&round) {
                let delay_ms = KILL_DELAYS_MS[rng.below(KILL_DELAYS_MS.len() as u64) as usize];
                scenario.record(format!(
                    "round {round}: waiting {delay_ms}ms, then kill9 + restart of the PRIMARY"
                ));
                std::thread::sleep(Duration::from_millis(delay_ms));
                primary.kill9(&mut scenario);
                primary.restart(&mut scenario).map_err(|e| e.to_string())?;
                login_once_writable(&mut primary_http, &mut scenario, &follower_repl_addr)?;
            }
        }

        if acked.len() != ROUNDS as usize {
            return Err(vacuous_ack_message(acked.len(), &not_acked));
        }
        scenario.record(format!(
            "all {ROUNDS} rounds acked; {} of {ROUNDS} observed converged before the final kill",
            converged.len()
        ));

        // Anti-vacuity (brief, plan-review Important #3): the zero-loss
        // check below is meaningless over too small a converged set.
        if converged.len() < MIN_CONVERGED {
            return Err(vacuous_convergence_message(converged.len(), acked.len()));
        }

        // ---- fail over: kill the primary for good, promote the follower --
        scenario.record("final kill9 of the primary — no further restarts of it");
        primary.kill9(&mut scenario);

        let promote = Command::new(charpente_bin)
            .args(["promote", "--node", &follower_repl_addr, "--secret", SECRET])
            .output()
            .map_err(|e| format!("running `charpente promote` failed to launch: {e}"))?;
        let promote_stdout = String::from_utf8_lossy(&promote.stdout).to_string();
        let promote_stderr = String::from_utf8_lossy(&promote.stderr).to_string();
        scenario.record(format!(
            "charpente promote --node {follower_repl_addr} --secret <redacted>: exit {:?}, \
             stdout {promote_stdout:?}, stderr {promote_stderr:?}",
            promote.status.code()
        ));
        if !promote.status.success() {
            return Err(format!(
                "charpente promote exited non-zero ({:?}) — stdout {promote_stdout:?} stderr \
                 {promote_stderr:?}",
                promote.status.code()
            ));
        }
        const EXPECTED_WINDOW_TEXT: &str = "— records the previous primary acked but had not \
             yet shipped are lost (the DECISIONS §6 failover window); this timeline is now \
             authoritative";
        if !promote_stdout.starts_with("promoted to primary at last_applied Some(") {
            return Err(format!(
                "charpente promote's stdout did not start with the expected \"promoted to \
                 primary at last_applied Some(...)\" prefix: {promote_stdout:?}"
            ));
        }
        if !promote_stdout.contains(EXPECTED_WINDOW_TEXT) {
            return Err(format!(
                "charpente promote's stdout did not name the DECISIONS §6 bounded-loss window \
                 (expected to contain {EXPECTED_WINDOW_TEXT:?}): {promote_stdout:?}"
            ));
        }
        scenario.record("promote: exit 0, stdout names the DECISIONS §6 bounded-loss window");

        // ---- verify: every CONVERGED id survives on the promoted follower;
        // ---- the non-converged tail is checked too, but reported (via
        // ---- describe_tail_loss) rather than folded into the violation
        // ---- check — DECISIONS §6 bounded loss there is expected, not a
        // ---- failure by itself.
        let mut missing: IdReasons = Vec::new();
        let mut tail_missing: IdReasons = Vec::new();
        let converged_set: HashSet<u64> = converged.iter().copied().collect();
        for &id in &acked {
            match follower_http.get(&format!("/customers/{id}")) {
                Ok(resp) if resp.status == 200 => {}
                Ok(resp) => {
                    let reason = format!("status {}", resp.status);
                    if converged_set.contains(&id) {
                        missing.push((id, reason));
                    } else {
                        tail_missing.push((id, reason));
                    }
                }
                Err(e) => {
                    let reason = format!("request failed: {e}");
                    if converged_set.contains(&id) {
                        missing.push((id, reason));
                    } else {
                        tail_missing.push((id, reason));
                    }
                }
            }
        }
        let non_converged_tail = acked.len() - converged.len();
        // Bookkeeping invariant, NOT the DECISIONS §6 bound (review Minor
        // 6 — the previous comment here over-read this check): `tail_missing`
        // is a subset of the non-converged tail by construction (the
        // `if converged_set.contains` branch above is exhaustive), so this
        // can only ever fail on a bug in THIS function, never on a real
        // DECISIONS §6 outcome. The genuine §6 bound in this oracle is
        // `MIN_CONVERGED`, which caps the tail itself at ≤10 of 50 (checked
        // earlier, before promote — see the `converged.len() < MIN_CONVERGED`
        // gate above). `Err`, not `assert!` (review Minor 10): a violation
        // here should still flow through `finish` so its evidence survives,
        // rather than an unwind bypassing the recorded `FAILED:` line.
        if tail_missing.len() > non_converged_tail {
            return Err(format!(
                "tail_missing ({}) exceeds the non-converged tail itself ({}) — a bookkeeping \
                 bug in this function, not a bounded-loss window",
                tail_missing.len(),
                non_converged_tail
            ));
        }
        scenario.record(describe_tail_loss(&tail_missing, non_converged_tail));
        if !missing.is_empty() {
            return Err(classify_converged_loss(&missing, converged.len()));
        }
        scenario.record(format!(
            "zero loss confirmed: all {} converged id(s) present on the promoted follower",
            converged.len()
        ));

        // ---- the promoted follower accepts a new write ---------------------
        let new_id = ROUNDS + 1;
        let body =
            format!(r#"{{"name":"Post-promote {new_id}","email":"post{new_id}@example.com"}}"#);
        let resp = follower_http
            .post(&format!("/customers/{new_id}"), &body)
            .map_err(|e| format!("post-promote write to the new primary failed: {e}"))?;
        if resp.status != 200 {
            return Err(format!(
                "post-promote write to the new primary: expected 200, got {} body {:?}",
                resp.status,
                String::from_utf8_lossy(&resp.body)
            ));
        }
        scenario.record("post-promote write to the follower-now-primary: 200 (accepted)");

        // ---- negative arm: restart the OLD primary as a follower of the --
        // ---- new one; its durable role file still says primary, so the --
        // ---- demote/re-seed refusal must print on stderr (role.rs).     --
        let old_addr = free_addr();
        let old_repl_addr = free_addr();
        let old_primary = ChildNode::spawn(
            node_bin,
            &[
                "--addr".to_string(),
                old_addr,
                "--data-dir".to_string(),
                primary_data.to_str().unwrap().to_string(),
                "--repl-listen".to_string(),
                old_repl_addr,
                "--role".to_string(),
                "follower".to_string(),
                "--primary".to_string(),
                // The REPL address, not the HTTP one (review Minor 5 —
                // this used to be `follower_addr`, the new primary's HTTP
                // port; a real operator points a follower's `--primary` at
                // the peer's replication listen address, and the pinned
                // notice below must embed the port an actual follower
                // client would dial).
                follower_repl_addr.clone(),
                "--secret".to_string(),
                SECRET.to_string(),
            ],
            &mut scenario,
        )
        .map_err(|e| {
            format!("restarting the old primary (pointed at the new primary) failed: {e}")
        })?;
        let stderr_path = old_primary.stderr_log().to_path_buf();
        old_primary_restart = Some(old_primary);

        let expected_notice = format!(
            "role file says primary, but config says follower of {follower_repl_addr} — the \
             durable role file always wins over config, never the reverse; demoting a primary \
             requires a fresh data dir (re-seed) — a demoted primary's log may have diverged, \
             and re-seeding is the only safe rejoin"
        );
        // `Repl::open`'s notice is printed from the kernel thread, which
        // races the HTTP listener binding `ChildNode::spawn` waits on
        // (`serve.rs`'s `serve_replicated` binds the HTTP port synchronously
        // BEFORE the kernel thread necessarily finishes `Repl::open`) — poll
        // the tee'd stderr log rather than reading it once immediately.
        let deadline = Instant::now() + Duration::from_secs(5);
        let stderr_contents = loop {
            let contents = std::fs::read_to_string(&stderr_path).unwrap_or_default();
            if contents.contains(&expected_notice) || Instant::now() >= deadline {
                break contents;
            }
            std::thread::sleep(Duration::from_millis(20));
        };
        if !stderr_contents.contains(&expected_notice) {
            return Err(format!(
                "the restarted old primary's stderr never printed the pinned demote/re-seed \
                 notice within {:?} — expected to contain {expected_notice:?}, got {:?} (file: \
                 {})",
                Duration::from_secs(5),
                stderr_contents,
                stderr_path.display()
            ));
        }
        // Prefixed by `serve.rs`'s `eprintln!("charpente replication: {notice}")`.
        if !stderr_contents.contains(&format!("charpente replication: {expected_notice}")) {
            return Err(format!(
                "the demote/re-seed notice was present but not on the \"charpente replication: \
                 \" line `serve_replicated` is documented to print it on: {stderr_contents:?}"
            ));
        }
        scenario.record(
            "negative arm confirmed: the restarted old primary's stderr prints the pinned \
             demote/re-seed refusal byte-exact",
        );
        // RULED (Noah, 2026-07-26 — see task-12-report.md): the brief's
        // literal text additionally claimed the restarted old primary
        // "exits nonzero". It doesn't, and this is a brief error, not a
        // product bug — GUIDE.md:2379-2382 is the accurate spec ("a
        // mismatch between the two is surfaced on stderr ... config never
        // silently re-roles a node"; no exit is documented). Confirmed
        // against the stock build: `Repl::open` does NOT error, and
        // `serve_replicated` does not exit on this notice — the durable
        // role (Primary) wins over config, so the process keeps running as
        // a live primary, printing the notice as the operator's cue to
        // intervene, never a self-enforced refusal. This assertion is
        // therefore scoped to what the stock build actually does (the
        // notice, byte-exact) and asserts no exit code.
        //
        // This restart used a config that CONTRADICTS the role file
        // (`--role follower`), which is what produces the notice at all —
        // see `failover_resurrected_primary_does_not_accept_writes` below
        // for the silent variant (an operator restart with the ORIGINAL
        // `--role primary` config produces no notice whatsoever, and is
        // stopped by the witness boot gate rather than by a notice).

        Ok(RunStats {
            converged_len: converged.len(),
            non_converged_tail,
            tail_missing,
        })
    })();

    if let Some(proxy) = &repl_proxy {
        proxy.stop(&mut scenario);
    }
    drop(old_primary_restart); // kills the rogue restarted old primary (ChildNode's Drop)

    finish(
        tag,
        &mut scenario,
        &[&primary_data, &follower_data],
        &scenario_root,
        result,
    )
}

/// Dumps the scenario script, prints it on failure, and cleans up temp dirs
/// ONLY on success — `acked_durable.rs`'s `finish`, same rationale (crate
/// honesty contract: a real finding's evidence must still exist afterward),
/// adapted for TWO data directories (primary + follower). Generic over `T`
/// (`RunStats` for `run_oracle`, `()` for `run_resurrection_oracle`) so a
/// caller that needs the run's observed stats — not just pass/fail — can
/// get them back on success without re-deriving them from the scenario log.
/// `tag` names which caller is reporting (review Minor 8 — this used to
/// hardcode "failover", so a `run_resurrection_oracle` failure printed under
/// the wire oracle's name; `realtime_resume.rs`'s `finish` already took a
/// `tag` for exactly this reason).
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

#[test]
#[ignore]
fn failover_wire_oracle() {
    let bins = build_bins().expect("build_bins");
    run_oracle(&bins.node, &bins.charpente, None).expect(
        "failover must promote the follower with zero loss over converged ids, and the \
         restarted old primary must print the pinned demote/re-seed refusal — see stderr above \
         for the scenario script",
    );
}

/// Round the peer link is cut at for `failover_non_converged_tail_is_exercised_and_bounded`:
/// leaves the last `ROUNDS - CUT_ROUND + 1` rounds unable to converge — 2 of
/// 50, well clear of `MIN_CONVERGED` (48 still converge, comfortably above
/// the 40 floor). Kept small on purpose: `TcpProxy::stop` is permanent, so
/// every round after the cut deterministically burns the full
/// `CONVERGE_TIMEOUT` (3s) waiting for a convergence that can never
/// arrive — 2 rounds is enough for a non-empty tail with a one-round margin
/// against a boundary timing edge case, without needlessly slowing the test.
const CUT_ROUND: u64 = 49;

/// Review item 4 (2026-07-26, sharpened in a second review pass the same
/// day): `run_oracle`'s acked-but-not-converged tail reporting path had
/// never executed in this environment — loopback replication converges
/// all 50 rounds every time, so the discrimination Oracle 2's whole
/// honesty claim rests on (zero loss over CONVERGED ids; the tail is
/// bounded, reported, and NOT a violation) had only ever been tested on
/// the trivial "everything converged, nothing was lost" path. If the
/// converged-vs-tail bookkeeping were wrong in either direction, that
/// version of this test would still have passed. This test forces a real
/// tail: cutting the peer link partway through
/// (`cut_peer_link_before_round`) deterministically produces acked-but-
/// never-converged ids, so both halves of the claim run against real
/// evidence:
///
/// 1. **The guarantee still holds**: `run_oracle` returning `Ok` at all is
///    itself proof the CONVERGED set had zero loss — a converged-set loss
///    would have made it return `Err(classify_converged_loss(..))` and
///    this `.expect()` would panic. This is also proof the oracle did NOT
///    classify the tail loss as a violation: `classify_converged_loss`
///    only ever fires on the converged set, never the tail.
/// 2. **The tail loss is real, not vacuous, and reuses the SAME
///    genuine-vs-rig-failure discrimination as the guarantee side**
///    (`confirmed_absent_count` — the predicate `classify_converged_loss`
///    itself uses — applied to `stats.tail_missing` rather than a
///    parallel "any non-200 counts" check):
///    - `non_converged_tail >= 1` — the cut actually produced a non-empty
///      tail (anti-vacuity: `0` here would mean the cut didn't work).
///    - `confirmed_absent_count(&stats.tail_missing) >= 1` — at least one
///      tail id is CONFIRMED genuinely absent (status 404), not merely
///      "some GET on it didn't return 200" — a rig failure (401/500/
///      transport hiccup) on a tail id would NOT count as evidence of the
///      bounded-loss window this test exists to exercise.
///    - `converged_len >= MIN_CONVERGED` — a second, independent
///      confirmation that the cut didn't eat into the guarantee-bearing
///      set (the first being that `run_oracle` didn't already refuse as
///      vacuous before reaching here).
#[test]
#[ignore]
fn failover_non_converged_tail_is_exercised_and_bounded() {
    let bins = build_bins().expect("build_bins");
    let stats = run_oracle(&bins.node, &bins.charpente, Some(CUT_ROUND)).expect(
        "failover must still hold its CONVERGED-id zero-loss guarantee (and must NOT classify \
         the deliberately-severed tail as a violation) even with a peer link cut late in the \
         run — see stderr above for the scenario script",
    );
    assert!(
        stats.non_converged_tail >= 1,
        "the deliberate peer-link cut at round {CUT_ROUND} produced NO non-converged tail \
         (non_converged_tail == 0) — the cut did not work as intended, so this run proves \
         nothing about the tail-reporting path; got: converged_len={}, non_converged_tail={}",
        stats.converged_len,
        stats.non_converged_tail
    );
    let confirmed = confirmed_absent_count(&stats.tail_missing);
    assert!(
        confirmed >= 1,
        "the non-converged tail was non-empty ({} id(s): {:?}) but ZERO were CONFIRMED \
         genuinely absent (status 404) — either the cut didn't actually prevent shipping, or \
         every GET on a tail id failed for a rig reason instead of a genuine 404; either way \
         this run does not prove the DECISIONS §6 bounded-loss window was actually exercised, \
         only that something else went wrong",
        stats.non_converged_tail,
        stats.tail_missing
    );
    assert!(
        stats.converged_len >= MIN_CONVERGED,
        "the cut at round {CUT_ROUND} ate into the CONVERGED set itself ({} < {MIN_CONVERGED}) \
         — CUT_ROUND is too early; run_oracle would already have refused as vacuous before \
         reaching here, so this assert is a second, independent confirmation",
        stats.converged_len
    );
}

// ------------------------------------------------- the resurrection guard
//
// M2 sub-project 0b (spec docs/superpowers/specs/2026-07-31-dual-primary-
// design.md). This was `failover_known_gap_silent_dual_primary_resurrection`,
// which characterized the gap by proving the two primaries could not see each
// other. The gap is closed, so — exactly as that version's own instructions
// demanded — this test is UPDATED to assert the new, safer behavior, never
// deleted.
//
// What it proves at the wire: an old primary restarted with its ORIGINAL,
// unchanged `--role primary` config after its follower was promoted does not
// accept writes, and says why. It runs in two phases, because the two halves
// fail in different directions and only one of them is attributable to the
// gate:
//
//   **Phase A (witness unreachable) is the anti-vacuity core.** The promoted
//   node is killed BEFORE the old primary restarts, so its probe cannot reach
//   it, the gate stays shut, and the write refusal is a 503 whose body names
//   the unreached witness. Only `TxError::Verifying` — the witness gate
//   itself — produces that status and that body, so a green Phase A cannot be
//   explained away by a node that merely died on startup for some unrelated
//   reason. Reads are asserted in the same phase, against the same live node:
//   serving reads WHILE withholding writes is the ruled behaviour (spec §2,
//   option (c)), not an incidental side effect.
//
//   **Phase B (witness reachable) proves the conclusive path.** The promoted
//   node comes back, the probe reaches it, it answers as a primary, and the
//   old primary refuses BY NAME with both remedies. That refusal bubbles out
//   of `Kernel::run`, so `Server::wait` returns its exit code and the process
//   then exits 1 (payload-shapes spec §5) — a stopped kernel ends the
//   process now, rather than leaving a live edge that only ever answers 500.
//   An exit on its own would prove nothing — any crash produces one — which
//   is exactly why the write refusal is asserted in Phase A rather than here.
//
// Scope, stated honestly (crate honesty contract, DESIGN §7's §4.4): this is
// a wire test against stock binaries with no seed promise. It proves the
// refusal is reachable and correctly worded through real HTTP and TCP. What
// proves it under the fault matrix is the in-sim property
// `a_resurrected_primary_refuses_by_name_when_its_witness_answers_as_a_primary`
// (`crates/charpente/tests/replication.rs`), which pins the same refusal
// byte-exact; the assertion below deliberately reuses that string's shape so
// the two cannot drift apart silently.
//
// DO NOT weaken or delete these assertions to make a red run green. If the
// product changes this behavior, UPDATE them to assert the new behavior — the
// same instruction the known-gap version carried, for the same reason.
fn run_resurrection_oracle(
    node_bin: &std::path::Path,
    charpente_bin: &std::path::Path,
) -> Result<(), String> {
    let tag = "failover_resurrection";
    let scenario_root = temp_dir(&format!("{tag}-scenario-root"));
    std::fs::create_dir_all(&scenario_root).map_err(|e| e.to_string())?;
    let mut scenario = Scenario::new(&scenario_root, tag).map_err(|e| e.to_string())?;
    let primary_data = temp_dir(&format!("{tag}-primary-data"));
    let follower_data = temp_dir(&format!("{tag}-follower-data"));
    let mut repl_proxy: Option<TcpProxy> = None;
    let mut old_primary_restart: Option<ChildNode> = None;

    let result = (|| -> Result<(), String> {
        let primary_addr = free_addr();
        let primary_repl_addr = free_addr();
        let follower_addr = free_addr();
        let follower_repl_addr = free_addr();

        let mut primary = ChildNode::spawn(
            node_bin,
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

        let proxy = TcpProxy::start(&primary_repl_addr).map_err(|e| e.to_string())?;
        let proxy_addr = proxy.addr();
        repl_proxy = Some(proxy);

        // The witness: the node this primary ships to, and therefore the
        // address that lands in the primary's durable witness set. Held
        // mutably (not `let _`) because both phases drive it — Phase A kills
        // it to make the probe's target unreachable, Phase B brings it back.
        let mut witness = ChildNode::spawn(
            node_bin,
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
                proxy_addr,
                "--secret".to_string(),
                SECRET.to_string(),
            ],
            &mut scenario,
        )
        .map_err(|e| format!("spawning the follower failed: {e}"))?;

        let mut primary_http = Http::new(&primary_addr);
        let token = login(&mut primary_http, &mut scenario)?;
        let mut follower_http = Http::new(&follower_addr);
        follower_http.set_session(token.clone());

        // One converged write, so there is durable state on the primary
        // worth resurrecting/protecting.
        let resp = primary_http
            .post("/customers/1", r#"{"name":"A","email":"a@example.com"}"#)
            .map_err(|e| format!("seed write failed: {e}"))?;
        if resp.status != 200 {
            return Err(format!("seed write: expected 200, got {}", resp.status));
        }
        if !wait_converged(&mut follower_http, 1) {
            return Err(
                "seed write never converged to the follower within the timeout — cannot \
                 characterize the gap without a converged row to check"
                    .to_string(),
            );
        }
        scenario.record("seed write (id 1) converged to the follower");

        primary.kill9(&mut scenario);
        scenario.record("primary killed for good");

        let promote = Command::new(charpente_bin)
            .args(["promote", "--node", &follower_repl_addr, "--secret", SECRET])
            .output()
            .map_err(|e| format!("running `charpente promote` failed to launch: {e}"))?;
        if !promote.status.success() {
            return Err(format!(
                "charpente promote exited non-zero ({:?}) — stdout {:?} stderr {:?}",
                promote.status.code(),
                String::from_utf8_lossy(&promote.stdout),
                String::from_utf8_lossy(&promote.stderr)
            ));
        }
        scenario.record("follower promoted to primary");

        // ================== PHASE A: the witness is DOWN ==================
        // Killing the promoted node BEFORE the old primary restarts is what
        // makes Phase A's write refusal attributable to the gate rather than
        // to a dead node: the probe can never resolve, so the old primary
        // stays up, serves reads, and refuses writes with the one 503 only
        // `TxError::Verifying` produces.
        witness.kill9(&mut scenario);
        scenario.record("witness (the promoted node) killed — the probe's target is unreachable");

        // ---- resurrect the old primary with its ORIGINAL, unchanged ----
        // ---- config — no attempted demote, unlike run_oracle's negative --
        // ---- arm. Fresh ports (same rationale as run_oracle: avoid a ----
        // ---- TIME_WAIT collision right after kill9), SAME data dir. ----
        let old_addr = free_addr();
        let old_repl_addr = free_addr();
        // `mut`: Phase B waits on this node's own exit (`wait_exit`, spec
        // §5), which takes `&mut self`. Held here (not moved into
        // `old_primary_restart` until Phase B is done with it) so it can
        // still be borrowed directly; `old_primary_restart` keeps it alive
        // past this closure's early returns either way, for `drop` below.
        let mut old_primary = ChildNode::spawn(
            node_bin,
            &[
                "--addr".to_string(),
                old_addr.clone(),
                "--data-dir".to_string(),
                primary_data.to_str().unwrap().to_string(),
                "--repl-listen".to_string(),
                old_repl_addr,
                "--role".to_string(),
                "primary".to_string(),
                "--secret".to_string(),
                SECRET.to_string(),
            ],
            &mut scenario,
        )
        .map_err(|e| {
            format!("restarting the old primary (original config, no demote attempt) failed: {e}")
        })?;
        let stderr_path = old_primary.stderr_log().to_path_buf();

        let mut old_http = Http::new(&old_addr);
        // Sessions are durable and replicate like any other table row (auth
        // T2) — the token minted through the original primary before it
        // died is still valid against the resurrected old primary, which
        // opened the SAME data dir.
        old_http.set_session(token.clone());

        // Reads are served the whole time the gate is shut. That is the ruled
        // behaviour (spec §2, option (c)) and it is asserted against the same
        // live node as the write refusal below, so "refuses writes" can never
        // be satisfied by a node that is simply down.
        let read = old_http
            .get("/customers/1")
            .map_err(|e| format!("GET on the write-gated old primary failed: {e}"))?;
        if read.status != 200 {
            return Err(format!(
                "a witness-gated node must keep serving reads (spec §2's ruled option (c)): \
                 expected 200 from GET /customers/1 on the resurrected old primary, got {} body \
                 {:?}",
                read.status,
                String::from_utf8_lossy(&read.body)
            ));
        }
        scenario.record("phase A: the resurrected old primary serves reads (200) while gated");

        // Pinned byte-exact (hard rule 5) — see `witness_gate_503`.
        let expected_503 = witness_gate_503(&follower_repl_addr);
        let gated = old_http
            .post("/customers/99", r#"{"name":"Z","email":"z@example.com"}"#)
            .map_err(|e| format!("POST on the write-gated old primary failed: {e}"))?;
        if gated.status != 503 {
            return Err(format!(
                "THE WITNESS GATE DID NOT HOLD: the resurrected old primary answered a write \
                 with status {} instead of the witness-gate 503. A 200 here IS the dual-primary \
                 gap — a second live writable primary reached through an ordinary \"restart the \
                 old box the way it was configured\" action, whose mandated remedy (fresh data \
                 dir + re-seed) then destroys every write it acked. Body: {:?}",
                gated.status,
                String::from_utf8_lossy(&gated.body)
            ));
        }
        let gated_body = String::from_utf8_lossy(&gated.body).to_string();
        if !gated_body.contains(&expected_503) {
            return Err(format!(
                "the write was refused 503, but NOT by the witness gate — some other 503 would \
                 satisfy the status check alone, so the body is what makes this attributable. \
                 Expected it to contain {expected_503:?}, got {gated_body:?}"
            ));
        }
        scenario.record(
            "phase A: the write is refused 503 BY THE WITNESS GATE, naming the unreached \
             witness (only TxError::Verifying renders this body)",
        );

        // An unreachable witness is "unknown", never "is a primary" (spec
        // §2's trichotomy). If the conclusive refusal has already fired here,
        // the probe is manufacturing a verdict it cannot have — the false
        // positive `repl/probe.rs`'s module doc says it must never produce.
        let refusal_marker = format!("witness {follower_repl_addr} answers as a primary itself");
        let stderr_so_far = std::fs::read_to_string(&stderr_path).unwrap_or_default();
        if stderr_so_far.contains(&refusal_marker) {
            return Err(format!(
                "the conclusive witness refusal fired while the witness was DOWN — an \
                 unreachable witness must classify as unknown (back off and retry), never as a \
                 primary; stderr: {stderr_so_far:?}"
            ));
        }

        // ================ PHASE B: the witness comes back =================
        // Restarted with its ORIGINAL follower config; its durable role file
        // says Primary (it was promoted) and the durable role always wins, so
        // it answers past the role match — conclusive proof this node is not
        // the primary any more.
        witness
            .restart(&mut scenario)
            .map_err(|e| format!("restarting the witness (the promoted node) failed: {e}"))?;
        scenario.record("phase B: the witness is back, as the primary it was promoted to be");

        let detail_for = |kind: &str| {
            format!(
                "witness answered {kind} — only a node past the role match (primary, or handing \
                 off) ever sends one"
            )
        };
        let refusal_for = |detail: &str| {
            format!(
                "witness {follower_repl_addr} answers as a primary itself ({detail}): this node \
                 restarted as a durable primary, but a follower it had shipped to has been \
                 promoted or handed off to while it was down, so this node is not the primary \
                 any more. It has NOT demoted itself — it cannot know which timeline you want \
                 to keep, and the writes it acked are not the platform's to discard. If \
                 {follower_repl_addr} holds the timeline to keep, re-seed this node from a \
                 fresh data dir (stop it, delete its data dir, restart to snapshot-seed from \
                 {follower_repl_addr}); if instead this node's records are the ones to keep, \
                 stop {follower_repl_addr}, then clear this node's witness set (GUIDE §10) and \
                 restart it."
            )
        };
        // The parenthetical is the witness's message KIND, and which one it is
        // depends on whether the promoted node had anything to ship when the
        // probe's Hello arrived: `Heartbeat` when the two logs are level (the
        // ordinary case here — nothing is written through the promoted node
        // before this point), `Frames` if it had pulled ahead. Both are
        // equally conclusive (`classify`, repl/probe.rs: only a node past the
        // role match sends either), so this accepts either FULL string and
        // nothing else. Deliberately not a loose substring match: the whole
        // point of the assertion is that the operator prose — both remedies,
        // and the refusal to demote — cannot drift silently.
        let expected_refusals = [
            refusal_for(&detail_for("Heartbeat")),
            refusal_for(&detail_for("Frames")),
        ];
        let deadline = Instant::now() + RESURRECTION_REFUSAL_TIMEOUT;
        let stderr_contents = loop {
            let contents = std::fs::read_to_string(&stderr_path).unwrap_or_default();
            if contents.contains(&refusal_marker) || Instant::now() >= deadline {
                break contents;
            }
            std::thread::sleep(Duration::from_millis(20));
        };
        let Some(matched) = expected_refusals
            .iter()
            .find(|r| stderr_contents.contains(r.as_str()))
        else {
            return Err(format!(
                "the resurrected old primary never printed the witness refusal in full within \
                 {RESURRECTION_REFUSAL_TIMEOUT:?}. Expected one of {expected_refusals:?}; got \
                 {stderr_contents:?} (file: {})",
                stderr_path.display()
            ));
        };
        // Prefixed by `serve.rs`'s kernel-stopped line: the refusal bubbles
        // out of `Kernel::run`, it is not an in-process notice. A witness
        // conflict is a `Refusal` (payload-shapes spec §5, R23-a) — a restart
        // cannot fix it, unlike a platform IO failure — so it gets the
        // "stopped on a refusal" line, not the platform-IO-failure one.
        if !stderr_contents.contains(&format!("charpente kernel stopped on a refusal: {matched}")) {
            return Err(format!(
                "the witness refusal was present but not on the \"charpente kernel stopped on a \
                 refusal\" line it must bubble out of `Kernel::run` on: {stderr_contents:?}"
            ));
        }
        scenario.record(
            "phase B: the resurrected old primary refuses BY NAME — names the witness, refuses \
             to demote itself, and gives both remedies",
        );

        // The refusal stopped the kernel, and a stopped kernel now ends the
        // process (payload-shapes spec §5): an operator's supervisor sees the
        // exit, where it used to see a live process answering every request
        // 500. Phase A still carries the attribution; this pins what an
        // operator sees afterwards.
        let status = old_primary
            .wait_exit(Duration::from_secs(10))
            .map_err(|e| format!("waiting for the refused node's exit failed: {e}"))?
            .ok_or_else(|| {
                "the refused node was still running 10s after its kernel stopped — the \
                 zombie payload-shapes spec §5 removes"
                    .to_string()
            })?;
        if status.code() != Some(1) {
            return Err(format!("the refused node must exit 1, got {status}"));
        }
        scenario.record("phase B: the refused node exits 1 — a stopped kernel ends the process");
        old_primary_restart = Some(old_primary);

        // ---- the legitimate primary is untouched: its own witness set ----
        // ---- was cleared on the flip toward Primary, so it resumes ----
        // ---- immediately and silently (spec §2's empty-set fast path). ----
        let new_write = follower_http
            .post(
                "/customers/200",
                r#"{"name":"Via new primary","email":"n@example.com"}"#,
            )
            .map_err(|e| format!("write through the promoted (new) primary failed: {e}"))?;
        if new_write.status != 200 {
            return Err(format!(
                "the promoted primary must still accept writes — the gate closes on a \
                 RESURRECTED primary, never on the legitimate one (its witness set was cleared \
                 at promote): expected 200, got {} body {:?}",
                new_write.status,
                String::from_utf8_lossy(&new_write.body)
            ));
        }
        // id 99 was refused on the old primary twice and written nowhere
        // else, so it must exist nowhere. This is the acked-write-loss claim
        // in its observable form: nothing was accepted that a later
        // fresh-data-dir re-seed would then destroy.
        let leaked = follower_http
            .get("/customers/99")
            .map_err(|e| format!("cross-check GET on the promoted primary failed: {e}"))?;
        if leaked.status != 404 {
            return Err(format!(
                "id 99 was REFUSED on the resurrected old primary and written nowhere else, but \
                 the promoted primary answered its GET with status {} (expected 404)",
                leaked.status
            ));
        }
        scenario.record(
            "confirmed: the promoted primary still writes, and the refused id exists nowhere",
        );

        Ok(())
    })();

    if let Some(proxy) = &repl_proxy {
        proxy.stop(&mut scenario);
    }
    drop(old_primary_restart);

    finish(
        tag,
        &mut scenario,
        &[&primary_data, &follower_data],
        &scenario_root,
        result,
    )
}

#[test]
#[ignore]
fn failover_resurrected_primary_does_not_accept_writes() {
    let bins = build_bins().expect("build_bins");
    run_resurrection_oracle(&bins.node, &bins.charpente).expect(
        "a primary restarted after its follower was promoted must refuse writes and name why — \
         see stderr above for the scenario script; if the product changed this behavior, update \
         these assertions rather than deleting them (the comment block above \
         `run_resurrection_oracle` carries the full context)",
    );
}

// ---------------------------------------------------------- named-error pins

#[test]
fn classify_converged_loss_all_404_is_an_unqualified_violation() {
    let missing: IdReasons = vec![(3, "status 404".to_string()), (9, "status 404".to_string())];
    let message = classify_converged_loss(&missing, 45);
    assert_eq!(
        message,
        "failover violated: 2 of 45 converged id(s) missing on the promoted follower: [(3, \
         \"status 404\"), (9, \"status 404\")] — a converged id (already observed present on \
         the follower BEFORE promote) must never be lost"
    );
}

#[test]
fn classify_converged_loss_zero_404_is_a_pure_rig_failure() {
    let missing: IdReasons = vec![
        (1, "status 401".to_string()),
        (2, "request failed: connection reset".to_string()),
    ];
    let message = classify_converged_loss(&missing, 42);
    assert_eq!(
        message,
        "failover oracle could not verify 2 of 42 converged id(s) after promote — RIG FAILURE, \
         not a proven failover violation (at least one GET failed for a reason other than a \
         genuine \"status 404\"): [(1, \"status 401\"), (2, \"request failed: connection \
         reset\")]"
    );
    assert!(
        !message.contains("failover violated"),
        "a pure rig failure must never contain the violation phrase: {message:?}"
    );
}

#[test]
fn classify_converged_loss_mixed_confirms_a_violation_without_hedging() {
    let missing: IdReasons = vec![
        (1, "status 404".to_string()),
        (2, "status 404".to_string()),
        (3, "status 500".to_string()),
    ];
    let message = classify_converged_loss(&missing, 47);
    assert_eq!(
        message,
        "failover violated: 2 of 47 converged id(s) confirmed genuinely absent (status 404) on \
         the promoted follower, alongside 1 GET(s) that failed for a rig reason: [(1, \"status \
         404\"), (2, \"status 404\"), (3, \"status 500\")]"
    );
}

#[test]
#[should_panic(expected = "a failover verdict requires at least one")]
fn classify_converged_loss_panics_on_an_empty_missing_set() {
    let missing: IdReasons = Vec::new();
    let _ = classify_converged_loss(&missing, 45);
}

#[test]
fn vacuous_convergence_message_names_the_floor() {
    let msg = vacuous_convergence_message(12, 50);
    assert_eq!(
        msg,
        "the failover oracle is vacuous: only 12 of 50 acked write(s) were observed converged \
         on the follower before promote (need >= 40) — refusing to run a zero-loss check over \
         too small a converged set"
    );
}

#[test]
fn vacuous_ack_message_names_the_shortfall() {
    let not_acked: IdReasons = vec![(7, "status 401".to_string())];
    let msg = vacuous_ack_message(49, &not_acked);
    assert_eq!(
        msg,
        "the failover oracle is vacuous: only 49/50 rounds acked against the primary (every \
         round should ack); not acked: [(7, \"status 401\")]; refusing to run a failover check \
         over a partial write phase"
    );
}

#[test]
fn confirmed_absent_count_only_counts_genuine_404s() {
    let reasons: IdReasons = vec![
        (1, "status 404".to_string()),
        (2, "status 401".to_string()),
        (3, "status 404".to_string()),
        (4, "request failed: connection reset".to_string()),
    ];
    assert_eq!(confirmed_absent_count(&reasons), 2);
    assert_eq!(confirmed_absent_count(&Vec::new()), 0);
}

#[test]
fn describe_tail_loss_empty_tail_names_zero_missing() {
    let msg = describe_tail_loss(&Vec::new(), 3);
    assert_eq!(
        msg,
        "0 of 3 non-converged-tail id(s) missing — every acked-but-not-observed-converged id \
         made it to the follower anyway"
    );
}

#[test]
fn describe_tail_loss_all_confirmed_is_expected_bounded_loss_not_a_violation() {
    let tail_missing: IdReasons = vec![
        (49, "status 404".to_string()),
        (50, "status 404".to_string()),
    ];
    let msg = describe_tail_loss(&tail_missing, 2);
    assert_eq!(
        msg,
        "2 of 2 non-converged-tail id(s) confirmed genuinely absent (status 404) on the \
         promoted follower — DECISIONS §6 bounded loss, expected and not a failure by itself: \
         [(49, \"status 404\"), (50, \"status 404\")]"
    );
    assert!(
        !msg.contains("violated"),
        "the tail's expected loss must never share `classify_converged_loss`'s violation \
         wording: {msg:?}"
    );
}

#[test]
fn describe_tail_loss_mixed_names_the_unverifiable_remainder() {
    let tail_missing: IdReasons = vec![
        (48, "status 404".to_string()),
        (49, "status 404".to_string()),
        (50, "status 500".to_string()),
    ];
    let msg = describe_tail_loss(&tail_missing, 3);
    assert_eq!(
        msg,
        "2 of 3 non-converged-tail id(s) confirmed genuinely absent (status 404, DECISIONS §6 \
         bounded loss, expected); 1 could NOT be verified for a rig reason (not evidence of the \
         bounded-loss window, just a check that couldn't finish): [(48, \"status 404\"), (49, \
         \"status 404\"), (50, \"status 500\")]"
    );
    assert!(
        !msg.contains("violated"),
        "the tail's expected loss must never share `classify_converged_loss`'s violation \
         wording, even in the mixed case: {msg:?}"
    );
}

/// Review Minor 12: `describe_tail_loss` folds `confirmed == 0` (every tail
/// id unverifiable for a rig reason, none confirmed) into the same branch
/// as the mixed case — honest text, but previously the one sub-case of the
/// function with no pin.
#[test]
fn describe_tail_loss_zero_confirmed_is_worded_honestly_as_unverifiable() {
    let tail_missing: IdReasons = vec![
        (49, "status 401".to_string()),
        (50, "status 500".to_string()),
    ];
    let msg = describe_tail_loss(&tail_missing, 2);
    assert_eq!(
        msg,
        "0 of 2 non-converged-tail id(s) confirmed genuinely absent (status 404, DECISIONS §6 \
         bounded loss, expected); 2 could NOT be verified for a rig reason (not evidence of the \
         bounded-loss window, just a check that couldn't finish): [(49, \"status 401\"), (50, \
         \"status 500\")]"
    );
    assert!(
        !msg.contains("violated"),
        "the tail's expected loss must never share `classify_converged_loss`'s violation \
         wording, even when zero of the tail's ids are confirmed genuinely absent: {msg:?}"
    );
}
