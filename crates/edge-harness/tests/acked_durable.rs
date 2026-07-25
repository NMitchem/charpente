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

fn rounds() -> u64 {
    std::env::var("EDGE_ACKED_DURABLE_ROUNDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&n| n > 0)
        .unwrap_or(DEFAULT_ROUNDS)
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

/// Drives `rounds` POST rounds against `node`/`http`, killing+restarting at
/// each round in `kill_rounds` (a randomized 0/1/5/20ms delay after the
/// observed ack). Returns every id whose creation was acked (200, and ONLY
/// 200). Verification is a SEPARATE step (`verify`) rather than folded in
/// here: a build with the T11 meta-test's injected fault fakes EVERY
/// response the SAME way, GETs included — reading back through the very
/// node under test would make "the id is present" and "the node always
/// fakes 200" the same observation. `run_oracle` verifies through the same
/// (trustworthy) stock node it drove with; `run_oracle_against_wrong_build`
/// swaps to a stock node for verification instead of trusting the wrong
/// build's own reads (see that function's doc comment).
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
) -> Result<Vec<u64>, String> {
    let mut rng = Rng::new();
    let mut acked: Vec<u64> = Vec::new();
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
            }
            Err(e) => {
                scenario.record(format!(
                    "round {round}: POST /customers/{id} failed: {e} (not acked)"
                ));
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
    Ok(acked)
}

/// GETs every `acked` id and returns `Err` (naming the scenario script) the
/// moment any is missing. The caller is responsible for `http` pointing at a
/// TRUSTWORTHY reader (see `drive`'s doc comment).
fn verify(http: &mut Http, scenario: &mut Scenario, acked: &[u64]) -> Result<(), String> {
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
        return Err(format!(
            "acked-implies-durable violated: {} of {} acked id(s) missing: {missing:?} — \
             scenario script: {}",
            missing.len(),
            acked.len(),
            script_path.display()
        ));
    }
    scenario.record(format!("verified: all {} acked id(s) present", acked.len()));
    Ok(())
}

/// Dumps the scenario script, prints it on failure, cleans up temp dirs, and
/// returns `result` unchanged.
fn finish(
    tag: &str,
    scenario: &mut Scenario,
    data_dir: &Path,
    scenario_root: &Path,
    result: Result<(), String>,
) -> Result<(), String> {
    let script = scenario.dump().map_err(|e| e.to_string())?;
    if let Err(e) = &result {
        eprintln!(
            "edge-harness acked_durable[{tag}]: FAILED — scenario script at {}",
            script.display()
        );
        eprintln!("edge-harness acked_durable[{tag}]: {e}");
    }
    let _ = std::fs::remove_dir_all(data_dir);
    let _ = std::fs::remove_dir_all(scenario_root);
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

        let acked = drive(
            &mut node,
            &mut http,
            &mut scenario,
            rounds,
            &kill_rounds,
            true,
        )?;

        scenario.record("final restart before verification");
        node.kill9(&mut scenario);
        node.restart(&mut scenario).map_err(|e| e.to_string())?;
        login(&mut http, &mut scenario)?;

        verify(&mut http, &mut scenario, &acked)
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
fn run_oracle_against_wrong_build(
    stock_bin: &Path,
    wrong_bin: &Path,
    tag: &str,
) -> Result<(), String> {
    let scenario_root = temp_dir(&format!("{tag}-scenario-root"));
    std::fs::create_dir_all(&scenario_root).map_err(|e| e.to_string())?;
    let mut scenario = Scenario::new(&scenario_root, tag).map_err(|e| e.to_string())?;
    let data_dir = temp_dir(&format!("{tag}-data"));

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

        let acked = drive(
            &mut node,
            &mut http,
            &mut scenario,
            rounds,
            &kill_rounds,
            false,
        )?;

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

        verify(&mut verify_http, &mut scenario, &acked)
    })();

    finish(tag, &mut scenario, &data_dir, &scenario_root, result)
}

#[test]
#[ignore]
fn acked_implies_durable_meta_test_has_teeth() {
    let bins = build_bins().expect("build_bins (stock — needed for the bootstrap login)");
    let (worktree_dir, wrong_node_bin) = build_wrong_node().expect("build_wrong_node");
    let repo_root = workspace_root();

    let result =
        run_oracle_against_wrong_build(&bins.node, &wrong_node_bin, "acked_durable_wrong_build");

    // Clean up the throwaway worktree regardless of outcome — never leave a
    // patched checkout behind, red or green.
    if let Err(e) = remove_worktree(&repo_root, &worktree_dir) {
        eprintln!(
            "edge-harness: failed to remove throwaway worktree {}: {e}",
            worktree_dir.display()
        );
    }

    let err = result.expect_err(
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
}
