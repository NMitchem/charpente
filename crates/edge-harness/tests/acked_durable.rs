//! Oracle 1 (spec 2026-07-24 introspection-edge-harness design, T11): the
//! wire-level successor to the M0 in-sim acked-durability property (demo-app
//! spec :135's deferral, discharged here). Drives one invoicing `node`
//! through a `TcpProxy`, POSTing customers with client-chosen ids across N
//! rounds (`acked_implies_durable_wire_oracle`, `#[ignore]`); a random
//! handful of rounds `kill9` the node a short randomized delay after
//! observing the 200, then restart + re-login. After a final guaranteed
//! restart, every id this rig ever saw acked — 200, and ONLY 200 — must read
//! back. A missing one is an acked-implies-durable violation, not a flake,
//! and the failure names the scenario script an operator replays by hand
//! (crate honesty contract: no seed promise here).
//!
//! The second test, `acked_implies_durable_meta_test_has_teeth`, proves the
//! oracle above has teeth (spec §4.6): the committed
//! `patches/wrong-ack-before-commit.patch` makes the kernel's request arm ack
//! a request BEFORE the write is even attempted. Run against a `node` built
//! from that patch, with kill-on-every-ack forced, the SAME oracle scenario
//! MUST report missing acked ids — a green result there would mean this
//! oracle can't catch the bug it exists to catch. Two wrinkles the brief
//! doesn't spell out, both consequences of the SAME fact — the wrong build's
//! injection corrupts EVERY `Ready::Request` response the same way, a
//! synthetic 200 sent before the real handling even runs, whose real
//! response is discarded, regardless of HTTP method:
//! - The harness can never learn a session token FROM the wrong build,
//!   including via `/login` itself (a real token only ever reaches disk
//!   through the discarded real response). `run_oracle_against_wrong_build`
//!   bootstraps a real, known token via the STOCK build against the same
//!   data directory first (`login`, run once, before switching binaries),
//!   then reuses it unchanged for the whole wrong-build run.
//! - GETs are corrupted identically — a GET against the wrong build returns
//!   a synthetic 200 with an empty body whether or not the row actually
//!   exists, so reading back through the wrong build itself can't tell "this
//!   row survived" from "this build always fakes 200." Verification swaps
//!   BACK to the STOCK build first (see `run_oracle_against_wrong_build`'s
//!   doc comment and `drive`'s).
//!
//! This module is entirely outside DESIGN §1's core determinism fence —
//! `lib.rs`'s honesty contract already establishes that this crate drives
//! built binaries as external processes over real TCP with no seed promise.
//! Real `SystemTime`, `TcpStream`, `Command`, and `thread::sleep` calls
//! throughout are the point, not a violation; the allow below matches
//! `child.rs`/`scenario.rs`/`proxy.rs`'s identical one.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

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

/// Reserves a loopback port by binding to `:0` and dropping the listener —
/// the node needs a knowable, fixed `--addr` up front so a restarted process
/// rebinds to the exact same address the proxy forwards to (`smoke.rs`'s
/// pattern).
fn free_addr() -> String {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("loopback bind")
        .local_addr()
        .expect("bound listener has an address")
        .to_string()
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("edge-harness lives at crates/edge-harness inside the charpente workspace")
        .to_path_buf()
}

// ------------------------------------------------------------ ad-hoc "rng"

/// This crate makes no seed promise (crate doc comment): which rounds kill
/// the node and which of the four delays each kill uses don't need to replay
/// byte-identically the way a sim universe does — they only need to vary run
/// to run so the oracle doesn't always exercise the same instant. A tiny
/// xorshift64 seeded from wall-clock nanos + pid + a per-process salt is
/// enough; a `rand` dependency for this would contradict the crate's
/// "depends on nothing else" stance for no real benefit.
static RNG_SALT: AtomicU64 = AtomicU64::new(0);

struct Rng(u64);

impl Rng {
    fn new() -> Rng {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        let salt = RNG_SALT.fetch_add(1, Ordering::Relaxed);
        let seed = nanos ^ ((std::process::id() as u64) << 32) ^ salt;
        Rng(seed | 1) // xorshift64 requires a nonzero state
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

const KILL_DELAYS_MS: [u64; 4] = [0, 1, 5, 20];

fn pick_kill_rounds(rng: &mut Rng, rounds: u64, count: u64) -> HashSet<u64> {
    let count = count.min(rounds);
    let mut picked = HashSet::new();
    while (picked.len() as u64) < count {
        picked.insert(1 + rng.below(rounds));
    }
    picked
}

// --------------------------------------------------------------- the oracle

const DEFAULT_ROUNDS: u64 = 40;
/// How many of the honest run's rounds get a randomized kill — plural per
/// the brief ("at randomized rounds"), small enough that most rounds still
/// exercise the no-fault path.
const HONEST_KILL_ROUNDS: u64 = 4;

/// Review M2: an unset `EDGE_ACKED_DURABLE_ROUNDS` silently runs the
/// default (fine — that's the point of a default), but a SET-and-invalid
/// one used to fall back to the default identically, so a typo'd override
/// ran silently as if nothing were wrong. Named error instead: unparseable
/// or non-positive is a loud panic naming the bad value, not a quiet
/// substitution (hard rule 5). The effective round count is also recorded
/// into the scenario by `record_plan` at every call site.
fn rounds() -> u64 {
    match std::env::var("EDGE_ACKED_DURABLE_ROUNDS") {
        Err(_) => DEFAULT_ROUNDS,
        Ok(raw) => raw
            .parse::<u64>()
            .ok()
            .filter(|&n| n > 0)
            .unwrap_or_else(|| {
                panic!(
                    "EDGE_ACKED_DURABLE_ROUNDS={raw:?} is not a positive integer round count — \
                 unset it to run the default of {DEFAULT_ROUNDS} rounds, or set it to a \
                 positive integer"
                )
            }),
    }
}

/// Logs in, stores the session on `http`, and returns the raw token — the
/// only place that parses a login response body.
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

fn record_plan(scenario: &mut Scenario, rounds: u64, forced: bool, kill_rounds: &HashSet<u64>) {
    let mut sorted: Vec<_> = kill_rounds.iter().copied().collect();
    sorted.sort_unstable();
    scenario.record(format!(
        "plan: {rounds} rounds, force_kill_every_round={forced}, kill rounds = {sorted:?}"
    ));
}

/// `(id, reason)` pairs — used both for ids that never acked (`drive`) and
/// ids that acked but didn't read back (`VerifyFailure::missing`). A named
/// alias per clippy's `type_complexity` (the bare nested tuple-of-Vec type
/// tripped it at the `-D warnings` gate).
type IdReasons = Vec<(u64, String)>;

/// Drives `rounds` POST rounds against `node`/`http`, killing+restarting at
/// each round in `kill_rounds` (a randomized 0/1/5/20ms delay after the
/// observed ack). Returns every id whose creation was acked (200, and ONLY
/// 200), plus every id that did NOT ack alongside why (re-review follow-up:
/// once I4 gates strictly on `acked.len() == rounds`, WHY a round failed is
/// the first thing an operator wants — previously that reason lived only in
/// the scenario log, not in the vacuity failure message itself).
/// Verification is a SEPARATE step (`verify`) rather than folded in here: a
/// build with the T11 meta-test's injected fault fakes EVERY response the
/// SAME way, GETs included — reading back through the very node under test
/// would make "the id is present" and "the node always fakes 200" the same
/// observation. `run_oracle` verifies through the same (trustworthy) stock
/// node it drove with; `run_oracle_against_wrong_build` swaps to a stock
/// node for verification instead of trusting the wrong build's own reads
/// (see that function's doc comment).
///
/// `relogin_after_restart`: the honest run logs back in after every restart
/// (the brief's literal "restarts, and re-logins"). The wrong-build run
/// passes `false` and supplies an already-established session instead — see
/// the module doc comment for why re-logging in against the wrong build
/// itself cannot work.
fn drive(
    node: &mut ChildNode,
    http: &mut Http,
    scenario: &mut Scenario,
    rounds: u64,
    kill_rounds: &HashSet<u64>,
    relogin_after_restart: bool,
) -> Result<(Vec<u64>, IdReasons), String> {
    let mut rng = Rng::new();
    let mut acked: Vec<u64> = Vec::new();
    let mut not_acked: Vec<(u64, String)> = Vec::new();
    for round in 1..=rounds {
        let id = round;
        let body = format!(r#"{{"name":"Round {id}","email":"round{id}@example.com"}}"#);
        match http.post(&format!("/customers/{id}"), &body) {
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
            }
            Err(e) => {
                scenario.record(format!(
                    "round {round}: POST /customers/{id} failed: {e} (not acked)"
                ));
                not_acked.push((id, format!("request failed: {e}")));
            }
        }

        if kill_rounds.contains(&round) {
            let delay_ms = KILL_DELAYS_MS[rng.below(KILL_DELAYS_MS.len() as u64) as usize];
            scenario.record(format!(
                "round {round}: waiting {delay_ms}ms after the ack, then kill9 + restart"
            ));
            std::thread::sleep(Duration::from_millis(delay_ms));
            node.kill9(scenario);
            node.restart(scenario).map_err(|e| e.to_string())?;
            if relogin_after_restart {
                login(http, scenario)?;
            } else {
                scenario.record(
                    "skipping relogin — the session persists durably; login itself is \
                     corrupted on this build too",
                );
            }
        }
    }
    Ok((acked, not_acked))
}

/// Refuses to proceed unless every round acked. Shared by both `run_oracle`
/// and `run_oracle_against_wrong_build`'s closures — re-review "New Breakage
/// #3": without this check INSIDE the closure, a partial ack returns `Ok`
/// from the closure, `finish` deletes the evidence on that `Ok`, and only
/// THEN does a caller's own `acked_len == rounds()` assertion fire — against
/// evidence that's already gone. `not_acked` (see `drive`) is folded into
/// the message so an operator sees WHY, not just how many.
fn require_full_ack(acked: &[u64], not_acked: &[(u64, String)], rounds: u64) -> Result<(), String> {
    if acked.len() != rounds as usize {
        return Err(format!(
            "the oracle is vacuous: only {}/{rounds} rounds acked (every round should ack — \
             POST /customers/:id has no reason to fail here); not acked: {not_acked:?}; \
             refusing to run a durability check over a partial or empty set",
            acked.len()
        ));
    }
    Ok(())
}

/// A durability-oracle failure: the missing acked ids, each with why it
/// looked missing, plus the human-readable `message` (`finish`'s/the
/// brief's contract: it carries the scenario-script path). Kept structured
/// rather than just a message string so a caller can assert the SPECIFIC
/// shape of a loss instead of trusting a substring match on `message` alone
/// (review I1): every acked id missing, each for a genuine "gone" reason —
/// not a 401 from a broken verify session, a 500, or a transport hiccup a
/// flaky proxy dial can produce (`src/proxy.rs`'s `relay` returns silently
/// on a failed upstream dial, which a client sees as an immediate EOF).
/// `message` itself carries a three-way version of the same distinction (see
/// `verify`): ALL missing ids genuinely-absent ("status 404") earns the
/// unqualified "acked-implies-durable violated" wording; ZERO of them do is
/// worded as a pure rig failure; a MIX says so explicitly and still counts
/// as a violation, since at least one row is confirmed gone — a false
/// "rig failure" verdict on a mixed loss would be the worse direction for a
/// headline claim (re-review "New Breakage").
struct VerifyFailure {
    missing: Vec<(u64, String)>,
    message: String,
}

/// GETs every `acked` id and returns `Err` (naming the scenario script) the
/// moment any is missing. The caller is responsible for `http` pointing at a
/// TRUSTWORTHY reader (see `drive`'s doc comment).
fn verify(http: &mut Http, scenario: &mut Scenario, acked: &[u64]) -> Result<(), VerifyFailure> {
    let mut missing = Vec::new();
    for id in acked {
        match http.get(&format!("/customers/{id}")) {
            Ok(resp) if resp.status == 200 => {}
            Ok(resp) => missing.push((*id, format!("status {}", resp.status))),
            Err(e) => missing.push((*id, format!("request failed: {e}"))),
        }
    }

    if !missing.is_empty() {
        let script_path = scenario.dir().join("scenario.log");
        // Review I1 (second half): a 401/500/transport hiccup on the verify
        // side proves nothing about durability — it means the rig could not
        // even ask the question, not that an acked write vanished. Naming
        // that "acked-implies-durable violated" would misname the headline
        // product claim (hard rule 5). Only "status 404" for a missing id —
        // a live server genuinely reporting the row absent — is real
        // evidence; anything else is a rig failure.
        //
        // Re-review "New Breakage": an all-or-nothing check on the FIRST
        // fix round misnamed a real violation as a rig failure whenever the
        // loss was mixed (39 genuine 404s plus one transport hiccup printed
        // "not a proven durability violation") — a false negative on a
        // headline claim is the worse direction (someone reads that and
        // discards a real finding). Three-way classification instead: ALL
        // 404 is the clean violation wording; ZERO 404 is the pure rig
        // failure wording; anything in between says so explicitly and keeps
        // the "violated" phrase, since at least one row IS confirmed gone.
        let confirmed = missing
            .iter()
            .filter(|(_, reason)| reason == "status 404")
            .count();
        let message = if confirmed == missing.len() {
            format!(
                "acked-implies-durable violated: {} of {} acked id(s) missing: {missing:?} — \
                 scenario script: {}",
                missing.len(),
                acked.len(),
                script_path.display()
            )
        } else if confirmed == 0 {
            format!(
                "acked-implies-durable oracle could not verify {} of {} acked id(s) — RIG \
                 FAILURE, not a proven durability violation (at least one GET failed for a \
                 reason other than a genuine \"status 404\" — an auth failure, a server error, \
                 or a transport hiccup): {missing:?} — scenario script: {}",
                missing.len(),
                acked.len(),
                script_path.display()
            )
        } else {
            format!(
                "acked-implies-durable violated: {confirmed} of {} missing id(s) are \
                 genuinely absent (status 404) — a durability violation may be present \
                 alongside a rig failure: {missing:?} — scenario script: {}",
                missing.len(),
                script_path.display()
            )
        };
        return Err(VerifyFailure { missing, message });
    }
    scenario.record(format!("verified: all {} acked id(s) present", acked.len()));
    Ok(())
}

/// Dumps the scenario script, prints it on failure, cleans up temp dirs ONLY
/// on success, and returns `result` unchanged.
///
/// Review I3: this function used to delete `data_dir`/`scenario_root`
/// unconditionally, microseconds after printing a message that pointed an
/// operator at the very same paths — defeating the brief's "failure message
/// carries the scenario-script path", the crate's honesty contract
/// (`src/lib.rs`: "a failure names a file an operator can open and replay by
/// hand"), and the brief's Step-1 instruction to capture the scenario script
/// on a real finding. A real finding's primary evidence — the scenario log
/// AND the crash-torn data directory the message points at — must still
/// exist when someone goes to read it.
///
/// Re-review "New Breakage #1": the retained log used to end wherever the
/// last successful step happened to leave it — the verdict (that the run
/// failed, and why) lived only in stderr, never in the file the message
/// points an operator at. The failure is now recorded into the scenario
/// BEFORE `dump()`, so the retained log is a complete repro artifact on its
/// own. Wording note (re-review controller doubt #3): this function doesn't
/// know whether an `Err` here is a genuine finding or a caller's DESIGNED
/// outcome (`run_oracle_against_wrong_build`'s wrong-build phase is expected
/// to fail every time) — the retained-evidence message says so honestly
/// rather than asserting every `Err` is a finding; a caller that already
/// knows the failure was designed and has finished characterizing it is
/// free to clean up afterward (see the meta-test).
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
                "edge-harness acked_durable[{tag}]: FAILED — scenario script at {}",
                script.display()
            );
            eprintln!("edge-harness acked_durable[{tag}]: {e}");
            eprintln!(
                "edge-harness acked_durable[{tag}]: NOT deleting data dir ({}) or scenario \
                 root ({}) — preserved for inspection; the caller decides whether this \
                 failure is a genuine finding or an expected, already-characterized outcome",
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

// ------------------------------------------------------------------ Step 1

/// The honest run: one node, one build, throughout.
fn run_oracle(node_bin: &Path, tag: &str, force_kill_every_round: bool) -> Result<(), String> {
    let scenario_root = temp_dir(&format!("{tag}-scenario-root"));
    std::fs::create_dir_all(&scenario_root).map_err(|e| e.to_string())?;
    let mut scenario = Scenario::new(&scenario_root, tag).map_err(|e| e.to_string())?;
    let data_dir = temp_dir(&format!("{tag}-data"));

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
        let mut http = Http::new(proxy.addr());
        login(&mut http, &mut scenario)?;

        let rounds = rounds();
        let mut rng = Rng::new();
        let kill_rounds = if force_kill_every_round {
            (1..=rounds).collect::<HashSet<u64>>()
        } else {
            pick_kill_rounds(&mut rng, rounds, HONEST_KILL_ROUNDS)
        };
        record_plan(&mut scenario, rounds, force_kill_every_round, &kill_rounds);

        let (acked, not_acked) = drive(
            &mut node,
            &mut http,
            &mut scenario,
            rounds,
            &kill_rounds,
            true,
        )?;

        // Review I4: the oracle is vacuous if `acked` is short or empty —
        // `verify` over an empty slice trivially returns `Ok(())`, which
        // would report a durability guarantee that was never exercised
        // (e.g. a future auth/routing regression making every
        // `POST /customers/:id` 401 while `/login` still works). Distinct
        // failure kind from a durability loss on purpose — this is "the rig
        // never got anything to check", not "an acked write vanished" —
        // hence its own wording, never the "acked-implies-durable violated"
        // phrase `verify` uses. `require_full_ack` folds in WHY (re-review
        // follow-up).
        require_full_ack(&acked, &not_acked, rounds)?;
        scenario.record(format!(
            "all {rounds} rounds acked — proceeding to verification"
        ));

        scenario.record("final restart before verification");
        node.kill9(&mut scenario);
        node.restart(&mut scenario).map_err(|e| e.to_string())?;
        login(&mut http, &mut scenario)?;

        verify(&mut http, &mut scenario, &acked).map_err(|failure| failure.message)
    })();

    finish(tag, &mut scenario, &data_dir, &scenario_root, result)
}

#[test]
#[ignore]
fn acked_implies_durable_wire_oracle() {
    let bins = build_bins().expect("build_bins");
    run_oracle(&bins.node, "acked_durable_stock", false).expect(
        "acked-implies-durable must hold against the stock build — see stderr above for the \
         scenario script",
    );
}

// ------------------------------------------------------------------ Step 2

/// Builds the invoicing `node` bin from `patches/wrong-ack-before-commit.patch`
/// applied to a throwaway `git worktree` at HEAD — never touches this
/// checkout. Returns the worktree directory (for cleanup) and the built
/// binary's path.
fn build_wrong_node() -> Result<(PathBuf, PathBuf), String> {
    let repo_root = workspace_root();

    // Review M4: a prior run that panicked between `git worktree add` and
    // `remove_worktree` — or was killed by a test timeout — can leave a
    // stale `.git/worktrees` entry behind, and this checkout is shared by
    // multiple agents. Best-effort hygiene at the start of every run makes
    // that self-healing (once the directory itself is gone, e.g. a manual
    // cleanup) rather than accumulating forever.
    let _ = Command::new("git")
        .args(["worktree", "prune"])
        .current_dir(&repo_root)
        .status();

    let worktree_dir = temp_dir("wrong-ack-worktree");

    let add = Command::new("git")
        .args(["worktree", "add", worktree_dir.to_str().unwrap(), "HEAD"])
        .current_dir(&repo_root)
        .output()
        .map_err(|e| format!("git worktree add: {e}"))?;
    if !add.status.success() {
        return Err(format!(
            "git worktree add {} HEAD failed:\n{}",
            worktree_dir.display(),
            String::from_utf8_lossy(&add.stderr)
        ));
    }

    let patch_path =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("patches/wrong-ack-before-commit.patch");
    let apply = Command::new("git")
        .args(["apply", patch_path.to_str().unwrap()])
        .current_dir(&worktree_dir)
        .output()
        .map_err(|e| format!("git apply: {e}"))?;
    if !apply.status.success() {
        let _ = remove_worktree(&repo_root, &worktree_dir);
        return Err(format!(
            "git apply {} failed in {} — regenerate the patch (kernel drift): re-run the `git \
             worktree add` + hand-apply-the-injection + `git diff` recipe described in \
             task-11-brief.md against current HEAD:\n{}",
            patch_path.display(),
            worktree_dir.display(),
            String::from_utf8_lossy(&apply.stderr)
        ));
    }

    let build = Command::new("cargo")
        .args([
            "build",
            "--release",
            "-p",
            "invoicing",
            "--bins",
            "--message-format=json",
        ])
        .current_dir(&worktree_dir)
        .output()
        .map_err(|e| format!("cargo build (wrong build): {e}"))?;
    if !build.status.success() {
        let _ = remove_worktree(&repo_root, &worktree_dir);
        return Err(format!(
            "cargo build --release -p invoicing --bins failed in the patched worktree {}:\n{}",
            worktree_dir.display(),
            String::from_utf8_lossy(&build.stderr)
        ));
    }
    let stdout = String::from_utf8_lossy(&build.stdout);
    let Some(node_bin) = find_node_bin(&stdout) else {
        let _ = remove_worktree(&repo_root, &worktree_dir);
        return Err(
            "wrong build: no 'node' executable in cargo build --message-format=json output"
                .to_string(),
        );
    };

    Ok((worktree_dir, node_bin))
}

fn remove_worktree(repo_root: &Path, dir: &Path) -> Result<(), String> {
    let status = Command::new("git")
        .args(["worktree", "remove", "--force", dir.to_str().unwrap()])
        .current_dir(repo_root)
        .status()
        .map_err(|e| e.to_string())?;
    if !status.success() {
        return Err(format!(
            "git worktree remove {} failed: {status}",
            dir.display()
        ));
    }
    Ok(())
}

/// Reimplemented rather than shared with `child.rs`'s `find_bins`/
/// `extract_field` (that module's own doc comment gives the same rationale
/// for reimplementing charpente-cli's scanner: this crate keeps scanners
/// call-site-local rather than exporting internal helpers for a one-off
/// need).
fn extract_field(line: &str, key: &str) -> Option<String> {
    let start = line.find(key)? + key.len();
    let mut result = String::new();
    let mut chars = line[start..].chars();
    while let Some(c) = chars.next() {
        match c {
            '"' => return Some(result),
            '\\' => result.push(chars.next()?),
            other => result.push(other),
        }
    }
    None
}

fn find_node_bin(stdout: &str) -> Option<PathBuf> {
    for line in stdout.lines() {
        if line.contains(r#""reason":"compiler-artifact""#)
            && line.contains(r#""name":"node""#)
            && let Some(exe) = extract_field(line, r#""executable":""#)
        {
            return Some(PathBuf::from(exe));
        }
    }
    None
}

/// The wrong-build meta-test's run outcome. `result` is what `finish`
/// already produces — a `String` naming the scenario script on failure, per
/// the brief's contract. `missing`/`acked_len` are populated once `drive`
/// AND `verify` both run (0/empty if an earlier phase — build, bootstrap,
/// drive itself — failed first instead), so a caller can assert the
/// SPECIFIC shape of a durability loss rather than trusting a substring
/// match on `result`'s message alone (review I1). `data_dir`/`scenario_root`
/// are exposed so a caller who has FULLY characterized an expected failure
/// (the wrong-build phase's `Err` is the designed outcome, not a finding —
/// re-review controller doubt #3) can clean them up once its own assertions
/// pass, without weakening `finish`'s keep-on-`Err` rule for every other
/// caller (including the positive-control call below, where an `Err` IS
/// unexpected and must stay preserved).
struct WrongBuildRun {
    result: Result<(), String>,
    missing: Vec<(u64, String)>,
    acked_len: usize,
    data_dir: PathBuf,
    scenario_root: PathBuf,
}

/// The meta-test's run: bootstraps a real session via `stock_bin` against a
/// fresh data directory, then hands that SAME directory to `wrong_bin` for
/// the actual kill-on-every-round scenario, reusing the bootstrapped token
/// throughout (see the module doc comment) — and, after driving, swaps BACK
/// to `stock_bin` for the verification reads. That swap is load-bearing, not
/// cosmetic: the wrong build's injected fault fires on every
/// `Ready::Request`, GETs included — an unauthenticated peek confirmed it
/// returns a synthetic `200` with an empty body for a GET too, so reading
/// through the wrong build itself can never distinguish "this row survived"
/// from "this build always fakes 200." Only a build without the fault can
/// tell the two apart.
///
/// Also the positive control (review I2): calling this with `stock_bin ==
/// wrong_bin` drives the IDENTICAL wiring — bootstrap, kill-every-round,
/// swap to a SECOND process on the same data dir for verification — with a
/// correct binary standing in for both roles, and must return `Ok`. Without
/// this, nothing in the suite demonstrates the verify swap can ever report a
/// row PRESENT: a systematic failure of that path (wrong `--data-dir`, a
/// rebuilt store, an auth mismatch on the second node) would produce
/// "everything missing" indistinguishable from a genuine durability loss.
fn run_oracle_against_wrong_build(stock_bin: &Path, wrong_bin: &Path, tag: &str) -> WrongBuildRun {
    let scenario_root = temp_dir(&format!("{tag}-scenario-root"));
    let data_dir = temp_dir(&format!("{tag}-data"));
    if let Err(e) = std::fs::create_dir_all(&scenario_root) {
        return WrongBuildRun {
            result: Err(e.to_string()),
            missing: Vec::new(),
            acked_len: 0,
            data_dir,
            scenario_root,
        };
    }
    let mut scenario = match Scenario::new(&scenario_root, tag) {
        Ok(s) => s,
        Err(e) => {
            return WrongBuildRun {
                result: Err(e.to_string()),
                missing: Vec::new(),
                acked_len: 0,
                data_dir,
                scenario_root,
            };
        }
    };

    let mut missing: Vec<(u64, String)> = Vec::new();
    let mut acked_len: usize = 0;

    let result = (|| -> Result<(), String> {
        // Phase 0: bootstrap a real, known session token via the STOCK
        // build against the SAME data directory the wrong build will
        // subsequently open.
        let bootstrap_addr = free_addr();
        let bootstrap_args = vec![
            "--addr".to_string(),
            bootstrap_addr.clone(),
            "--data-dir".to_string(),
            data_dir.to_str().unwrap().to_string(),
        ];
        let token = {
            let mut bootstrap = ChildNode::spawn(stock_bin, &bootstrap_args, &mut scenario)
                .map_err(|e| e.to_string())?;
            let bootstrap_proxy = TcpProxy::start(&bootstrap_addr).map_err(|e| e.to_string())?;
            let mut bootstrap_http = Http::new(bootstrap_proxy.addr());
            let token = login(&mut bootstrap_http, &mut scenario)
                .map_err(|e| format!("bootstrap login (via the stock build): {e}"))?;
            scenario.record("bootstrap: obtained a real session token via the STOCK build");
            bootstrap.kill9(&mut scenario);
            token
        };

        // Phase 1: the SAME round-and-kill scenario against the WRONG
        // build, on the SAME data directory (so the bootstrapped session is
        // present at recovery), reusing the bootstrapped token throughout.
        let node_addr = free_addr();
        let args = vec![
            "--addr".to_string(),
            node_addr.clone(),
            "--data-dir".to_string(),
            data_dir.to_str().unwrap().to_string(),
        ];
        let mut node =
            ChildNode::spawn(wrong_bin, &args, &mut scenario).map_err(|e| e.to_string())?;
        let proxy = TcpProxy::start(&node_addr).map_err(|e| e.to_string())?;
        let mut http = Http::new(proxy.addr());
        http.set_session(token);
        scenario.record(
            "wrong build: reusing the bootstrapped session cookie (no relogin — login is \
             corrupted on this build too)",
        );

        let rounds = rounds();
        let kill_rounds: HashSet<u64> = (1..=rounds).collect();
        record_plan(&mut scenario, rounds, true, &kill_rounds);

        let (acked, not_acked) = drive(
            &mut node,
            &mut http,
            &mut scenario,
            rounds,
            &kill_rounds,
            false,
        )?;
        acked_len = acked.len();
        // Re-review "New Breakage #3": without this check INSIDE the
        // closure, a partial ack here would return `Ok`, `finish` would
        // delete the evidence on that `Ok`, and only the caller's OWN
        // `acked_len == rounds()` assertion (the positive control's, or a
        // future one on the wrong-build half) would catch it afterward —
        // against evidence already gone. This also gives both halves of
        // this function the full `acked_len == rounds()` strength I1's
        // original wording asked for.
        require_full_ack(&acked, &not_acked, rounds)?;
        scenario.record(format!(
            "all {rounds} rounds acked — proceeding to phase 2 (kill, swap to stock for \
             verification)"
        ));

        // Phase 2: kill the wrong build for good and swap to the STOCK
        // build, on the SAME data directory, for the verification reads —
        // see this function's doc comment for why the wrong build's own
        // reads cannot be trusted to answer this question.
        scenario.record("final kill9 of the wrong build (no further restarts of it)");
        node.kill9(&mut scenario);
        drop(node);
        drop(proxy);

        let verify_addr = free_addr();
        let verify_args = vec![
            "--addr".to_string(),
            verify_addr.clone(),
            "--data-dir".to_string(),
            data_dir.to_str().unwrap().to_string(),
        ];
        // Kept alive (unread) purely so its `Drop` doesn't kill the process
        // before verification runs — `child.rs`'s `Drop` impl is what tears
        // it down once this closure returns.
        let _verify_node =
            ChildNode::spawn(stock_bin, &verify_args, &mut scenario).map_err(|e| e.to_string())?;
        let verify_proxy = TcpProxy::start(&verify_addr).map_err(|e| e.to_string())?;
        let mut verify_http = Http::new(verify_proxy.addr());
        login(&mut verify_http, &mut scenario)?;
        scenario.record("verification: swapped to the STOCK build for a trustworthy read");

        verify(&mut verify_http, &mut scenario, &acked).map_err(|failure| {
            missing = failure.missing;
            failure.message
        })
    })();

    let result = finish(tag, &mut scenario, &data_dir, &scenario_root, result);
    WrongBuildRun {
        result,
        missing,
        acked_len,
        data_dir,
        scenario_root,
    }
}

#[test]
#[ignore]
fn acked_implies_durable_meta_test_has_teeth() {
    let bins = build_bins().expect("build_bins (stock — needed for the bootstrap login)");

    // Positive control (review I2): before spending time on the wrong
    // build at all, prove the swap-to-a-second-process verification wiring
    // CAN report a row present, using the EXACT same machinery (bootstrap
    // -> drive with kill-every-round -> swap-to-verify) with the stock
    // binary standing in for both roles. See `run_oracle_against_wrong_build`'s
    // doc comment for why this matters: without it, a systematic failure of
    // the verify swap would produce "everything missing" indistinguishable
    // from a genuine durability loss, and the red half below would prove
    // nothing.
    let control =
        run_oracle_against_wrong_build(&bins.node, &bins.node, "acked_durable_positive_control");
    if let Err(e) = &control.result {
        panic!(
            "positive control failed: a stock node driving itself through the wrong-build \
             wiring (bootstrap -> drive with kill forced every round -> kill -> \
             swap-to-verify) reported a failure. This alone cannot distinguish two readings \
             (re-review \"New Breakage\"): EITHER the verify swap itself is broken (the more \
             likely explanation, and the one the red half below depends on), OR this is a \
             genuine acked-implies-durable violation on the STOCK build under \
             kill-on-every-round — strictly harsher than the honest run's \
             {HONEST_KILL_ROUNDS} randomized kills, so a real wire-level loss could \
             plausibly surface here first. Check the retained scenario log named in the \
             error below before assuming it's the rig: {e}"
        );
    }
    assert!(
        control.missing.is_empty(),
        "positive control: unexpected missing ids even though the run reported success: {:?}",
        control.missing
    );
    assert_eq!(
        control.acked_len,
        rounds() as usize,
        "positive control: expected every round to ack against the stock build"
    );

    let (worktree_dir, wrong_node_bin) = build_wrong_node().expect("build_wrong_node");
    let repo_root = workspace_root();
    // Review M5: if `CARGO_TARGET_DIR` were set and shared with the
    // throwaway worktree, the patched build could silently overwrite the
    // very `target/release/node` `build_bins` returned as "stock" — the
    // rig would still fail safe (verify would see synthetic 200s -> `Ok` ->
    // the `expect_err` below panics), but with a baffling message. Name it
    // directly instead.
    assert_ne!(
        wrong_node_bin, bins.node,
        "the wrong build's node binary resolved to the SAME path as the stock build's — \
         likely CARGO_TARGET_DIR is set and shared with the throwaway worktree, silently \
         making the \"stock\" and \"wrong\" binaries identical"
    );

    let run =
        run_oracle_against_wrong_build(&bins.node, &wrong_node_bin, "acked_durable_wrong_build");

    // Clean up the throwaway worktree regardless of outcome — never leave a
    // patched checkout behind, red or green.
    if let Err(e) = remove_worktree(&repo_root, &worktree_dir) {
        eprintln!(
            "edge-harness: failed to remove throwaway worktree {}: {e}",
            worktree_dir.display()
        );
    }

    let err = run.result.expect_err(
        "the WRONG build (acks BEFORE the write is even attempted) must lose acked writes \
         under kill-on-every-ack — a green result here means this oracle has no teeth \
         (spec §4.6)",
    );
    assert!(
        err.contains("acked-implies-durable violated"),
        "the wrong build must fail on the SPECIFIC missing-acked-id oracle, not some unrelated \
         build/spawn/login error — otherwise a build break could masquerade as the oracle \
         having teeth: {err}"
    );

    // Review I1: the failure must be a REAL durability loss, not a 401 from
    // a broken verify session, a 500, or a transport hiccup — any of which
    // `verify` would ALSO have folded into "missing" and produced the exact
    // same "acked-implies-durable violated" substring above. Require EVERY
    // acked id to be missing (not merely some — kill-on-every-round should
    // lose everything, and a partial loss is a weaker, more suspicious
    // signal), and every missing reason to be the specific "status 404" a
    // genuinely absent row produces.
    assert_eq!(
        run.missing.len(),
        run.acked_len,
        "expected ALL acked ids to be missing under kill-on-every-ack against the wrong \
         build, got only {}/{}: {:?}",
        run.missing.len(),
        run.acked_len,
        run.missing
    );
    for (id, reason) in &run.missing {
        assert_eq!(
            reason, "status 404",
            "id {id} was reported missing for a non-durability reason ({reason}) — a 401, \
             500, or transport failure proves nothing about durability and must not count as \
             teeth"
        );
    }

    // Re-review controller doubt #3: the wrong-build run's `Err` is the
    // DESIGNED outcome, not a finding — `finish`'s generic keep-on-`Err`
    // rule left its data dir + scenario root on disk (~628K measured),
    // accumulating under `$TMPDIR` on every meta-test run forever. Now that
    // every assertion above has passed, this failure is fully characterized
    // as the intended teeth-proof (every acked id genuinely 404, none left
    // ambiguous) and the evidence is spent — clean up here rather than
    // leaving it to rot. `run_oracle` and the positive control above keep
    // `finish`'s keep-on-`Err` behavior untouched: an unexpected failure in
    // either still preserves its evidence indefinitely.
    let _ = std::fs::remove_dir_all(&run.data_dir);
    let _ = std::fs::remove_dir_all(&run.scenario_root);
}

// ---------------------------------------------------------- named-error pins

/// Re-review controller doubt #2: pins `verify`'s pure-rig-failure branch
/// byte-exact for everything this crate itself composes. Deterministic and
/// process-free — a closed loopback port fails every GET at the TCP layer
/// near-instantly, no built binary and no spawned process required — so
/// this is NOT `#[ignore]`d, per T10's convention that only process-/socket-
/// spawning tests carry that attribute.
///
/// NOT a literal byte-exact match of the WHOLE message: the per-id reason
/// (`{e}` inside `verify`'s `format!("request failed: {e}")`) embeds the
/// OS's own connect-refused text, which differs by platform — e.g.
/// "Connection refused (os error 61)" on macOS vs. a different os-error
/// number on Linux CI (`.github/workflows/ci.yml` runs `ubuntu-latest`).
/// Hardcoding that text would make this pin fail on CI while passing here.
/// So this pins every part of the message under this crate's control (the
/// "RIG FAILURE" wording, the K-of-N counts, the scenario-script path) via
/// prefix/suffix around the OS-dependent middle, and separately asserts the
/// STABLE "request failed: " prefix on every reason — genuinely byte-exact
/// on the product surface this crate owns, portable on the part it doesn't.
#[test]
fn verify_names_a_rig_failure_distinctly_from_a_durability_violation() {
    let closed_addr = free_addr(); // bound, then dropped — nothing listens on it
    let mut http = Http::new(closed_addr);
    let scenario_root = temp_dir("rig-failure-pin-scenario-root");
    std::fs::create_dir_all(&scenario_root).expect("create scenario root");
    let mut scenario = Scenario::new(&scenario_root, "rig-failure-pin").expect("Scenario::new");

    let acked = vec![1, 2, 3];
    let failure =
        verify(&mut http, &mut scenario, &acked).expect_err("a closed port must fail every GET");

    assert_eq!(failure.missing.len(), 3, "{:?}", failure.missing);
    for (id, reason) in &failure.missing {
        assert!(
            reason.starts_with("request failed: "),
            "id {id}: expected a transport-failure reason, got {reason:?}"
        );
    }

    let expected_prefix = "acked-implies-durable oracle could not verify 3 of 3 acked id(s) \
                            — RIG FAILURE, not a proven durability violation (at least one GET \
                            failed for a reason other than a genuine \"status 404\" — an auth \
                            failure, a server error, or a transport hiccup): [(1, \"request \
                            failed: ";
    assert!(
        failure.message.starts_with(expected_prefix),
        "message prefix mismatch:\n  got:      {:?}\n  expected prefix: {:?}",
        failure.message,
        expected_prefix
    );

    let script_path = scenario.dir().join("scenario.log");
    let expected_suffix = format!(" — scenario script: {}", script_path.display());
    assert!(
        failure.message.ends_with(&expected_suffix),
        "message suffix mismatch:\n  got:      {:?}\n  expected suffix: {:?}",
        failure.message,
        expected_suffix
    );

    assert!(
        !failure.message.contains("acked-implies-durable violated"),
        "a pure rig failure must never contain the durability-violation phrase: {:?}",
        failure.message
    );

    // `verify` never calls `scenario.dump()` itself (only `finish` does) —
    // drop explicitly so `Scenario`'s own `Drop` impl writes `scenario.log`
    // BEFORE this cleanup removes the directory it would write into,
    // instead of racing the implicit end-of-scope drop against
    // `remove_dir_all` and printing a spurious "No such file" error.
    drop(scenario);
    let _ = std::fs::remove_dir_all(&scenario_root);
}
