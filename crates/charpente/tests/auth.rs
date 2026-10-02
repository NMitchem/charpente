//! Auth negative-space properties (PLAN M1 sub-project 4, task 8): the
//! flagship "raw credentials never reach the durable store" swarm property
//! (DESIGN §8 — credentials are hashed at rest), the naive-victim meta-test
//! that proves the oracle catches a raw token written to disk (hard rule 3),
//! and the determinism check for the added crypto path (spec §7).
//!
//! The ladder / OIDC / peer-secret matrices already live in their own suites
//! (tests/http.rs, tests/replication.rs) from tasks 2/4/5/7 — this file adds
//! only the three properties task 8 owns.

use std::io;
use std::time::Duration;

use charpente::auth::hash_token;
use charpente::http::{ApiError, App, AppConfig, Ctx, Method, Request, Session};
use charpente::kernel::{Kernel, RecordedResponses, SimSource, Step};
use charpente::repl::role::Role;
use charpente::repl::{Repl, ReplConfig};
use charpente::{DbConfig, Schema, Tx, route, routes};
use charpente_core::io::{Clock, Disk};
use charpente_core::log::WalConfig;
use charpente_core::table::Id;
use charpente_sim::checks::{
    FaultReason, classify, durable_corruption_fired, is_fault_fallout, replays_identically,
};
use charpente_sim::{FaultConfig, Sim, SimHost, run_universes_swarm_with};

const SEC: u64 = 1_000_000_000;

/// The two hosts both configure this as their replication peer secret. It is
/// the third raw byte-string the oracle scans for: it authenticates the link
/// (over a plaintext wire, spec §5), so it lives only in config/memory and
/// must never appear in a durable file.
const SWARM_SECRET: &str = "swarm-secret";

const AUTH_ROUNDS: u32 = 8;
const AUTH_CONVERGE_ROUNDS: u32 = 400;
const AUTH_RECOVER_ATTEMPTS: u32 = 200;
/// Power-cycle every Nth recovery attempt rather than every one — cycling
/// before EVERY attempt is the wrong shape: `realtime.rs`'s `settle_boot` doc
/// comment (`:706-712`) measured it directly on seed `0x536007ad99fd90bf`
/// (`write_error` 0.144) at 0 of 20_000 boots ever succeeding, because
/// `Store::apply` commits at `Durability::None` and every `Sim::restart`
/// discards the uncheckpointed store, forcing a full WAL rebuild on every
/// single try. Retrying in place instead lets a boot settle; the same value
/// (`8`) as its siblings (`replication.rs:428`'s `RECOVER_CYCLE_EVERY`,
/// `realtime.rs:657`'s `SETTLE_CYCLE_EVERY`) keeps all the retry loops
/// modeling a failed process start identically.
const AUTH_RECOVER_CYCLE_EVERY: u32 = 8;
const AUTH_STEP_BUDGET: u32 = 20_000;
const AUTH_HORIZON: u64 = 300 * SEC;

/// This binary's own `INCONCLUSIVE` token — the sibling suites' idiom (test
/// binaries share no code): a universe too damaged by faults to adjudicate is
/// tagged with it and folded into a covered skip, never a failure.
const INCONCLUSIVE: &str = "universe-inconclusive";

// ------------------------------------------------------------------ routes

/// Login mints a session; the raw token is the response body (the only place
/// it ever exists — the row stores only its hash).
#[route(POST "/login", public)]
fn login(tx: &mut Tx, subject: String, ctx: &Ctx) -> Result<String, ApiError> {
    Ok(ctx.create_session(tx, subject))
}

/// Mints an API key for the session's subject; the raw `ck_…` key is the
/// response body (again the only place it exists).
#[route(POST "/keys", auth)]
fn create_key(tx: &mut Tx, ctx: &Ctx) -> Result<String, ApiError> {
    let subject = ctx.session().expect("auth route").subject.clone();
    Ok(ctx.create_api_key(tx, subject))
}

/// A read authenticated by the ladder — exercises the credential on the read
/// path (the raw resolves to its hashed row).
#[route(GET "/me", auth)]
fn whoami(tx: &mut Tx, ctx: &Ctx) -> Result<String, ApiError> {
    let _ = tx;
    Ok(ctx.principal().expect("auth route").subject().to_string())
}

// ------------------------------------------------------------------ rig

fn auth_primary_cfg(addr: &str) -> ReplConfig {
    ReplConfig {
        listen: Some(addr.to_string()),
        initial_role: Role::Primary,
        peer_secret: Some(SWARM_SECRET.to_string()),
        strict_witnesses: false,
    }
}

fn auth_follower_cfg(listen: &str, primary: &str) -> ReplConfig {
    ReplConfig {
        listen: Some(listen.to_string()),
        initial_role: Role::Follower {
            primary: primary.to_string(),
        },
        peer_secret: Some(SWARM_SECRET.to_string()),
        strict_witnesses: false,
    }
}

fn multi_node_db_cfg() -> DbConfig {
    DbConfig {
        wal: WalConfig {
            dir: "wal".to_string(),
            segment_bytes: 4096,
        },
        store_path: "tables.redb".to_string(),
        // A small checkpoint cadence so hashes migrate WAL → store within the
        // workload: the oracle then scans populated segments AND a populated
        // store, not just one surface.
        checkpoint_every: 8,
        ..DbConfig::default()
    }
}

/// One replication node driven through its `Kernel` — this file's own copy of
/// replication.rs's `KNode`/realtime.rs's `RNode`, scoped to what this
/// property needs (no SSE sinks, no role flips: it only ever mounts the auth
/// routes and never promotes).
struct AuthNode {
    host: SimHost,
    kernel: Option<Kernel>,
    source: SimSource,
    responses: RecordedResponses,
    name: String,
    repl_cfg: ReplConfig,
    db_cfg: DbConfig,
}

/// Build the App + Repl + Kernel for one host, with the torn-role first-boot
/// re-init idiom `open_kernel` uses in replication.rs. A node here never
/// flips role, so a torn role file is always safely re-initializable from
/// config (no restore dance needed).
fn open_auth_kernel(
    sim: &Sim,
    h: &SimHost,
    cfg: &ReplConfig,
    db_cfg: &DbConfig,
) -> Result<Kernel, String> {
    let mut app = App::open(
        Box::new(h.disk()),
        Schema::new(),
        Box::new(h.entropy()),
        AppConfig {
            body_cap: 1 << 20,
            db: db_cfg.clone(),
            ..AppConfig::default()
        },
    )
    .map_err(|e| format!("App::open: {e}"))?
    .mount(routes![login, create_key, whoami]);

    let repl = match Repl::open(
        Box::new(h.net()),
        Box::new(h.disk()),
        cfg.clone(),
        app.db_for_tests(),
    ) {
        Ok((repl, _notice)) => repl,
        // A torn witness file (M2 sub-project 0b task 2): keyed on its own
        // full, non-overlapping needle ("witness file failed its integrity
        // check"), an independent arm from the role-file arm below (keyed on
        // ITS own full needle, "role file failed its integrity check") — the
        // two are mutually exclusive by construction, so arm ORDER is
        // irrelevant; both need the FULL string rather than the shared
        // "integrity check" substring precisely because that substring alone
        // cannot tell them apart. Unlike the role file, there is no
        // config-derived witness set to re-init from, and witnesses.rs's own
        // fix text is explicit that deleting it is safe ONLY when the
        // operator is certain no follower of this node was ever promoted —
        // a fact this harness cannot prove in a swarm-fuzzed universe (a
        // witness can be recorded by an ordinary running primary with no
        // role flip at all, so `flipped` is not a safe proxy here either,
        // where it exists). Always inconclusive: the operator-restore path
        // is the fix. Counted separately from the generic inconclusive
        // bucket ("repl:witness-file-inconclusive") so this residual,
        // newly-introduced permanent-unavailability path stays measurable,
        // not invisible (fix round 1, Noah's ruling — a genuinely torn
        // NON-empty witness file is a real design fork, escalated
        // separately, not fixed here). (This comment is kept byte-identical
        // at every copy of this arm — replication.rs's two sites, auth.rs,
        // realtime.rs — so a future reviewer can diff them; see
        // task-2-report.md.)
        Err(e)
            if e.to_string()
                .contains("witness file failed its integrity check") =>
        {
            sim.cover("repl:witness-file-inconclusive");
            sim.cover("auth:inconclusive:witness-torn");
            return Err(format!(
                "{INCONCLUSIVE}: {} torn witness file — the operator restore path is the fix: {e}",
                h.name()
            ));
        }
        Err(e)
            if e.to_string()
                .contains("role file failed its integrity check") =>
        {
            let _ = h.disk().remove("role");
            let _ = h.disk().sync_dir("role");
            Repl::open(
                Box::new(h.net()),
                Box::new(h.disk()),
                cfg.clone(),
                app.db_for_tests(),
            )
            .map_err(|e| format!("Repl::open (after torn-role re-init): {e}"))?
            .0
        }
        Err(e) => return Err(format!("Repl::open: {e}")),
    };
    Kernel::with_replication(app, Box::new(sim.clock()), repl).map_err(|e| format!("Kernel: {e}"))
}

fn spawn_auth_node(
    sim: &Sim,
    host: &str,
    cfg: ReplConfig,
    db_cfg: DbConfig,
    horizon: u64,
) -> Result<AuthNode, String> {
    let h = sim.host(host);
    for _ in 0..AUTH_RECOVER_ATTEMPTS {
        match open_auth_kernel(sim, &h, &cfg, &db_cfg) {
            Ok(kernel) => {
                let source = SimSource::new(Box::new(sim.clock()), horizon);
                let responses = source.responses();
                return Ok(AuthNode {
                    host: h,
                    kernel: Some(kernel),
                    source,
                    responses,
                    name: host.to_string(),
                    repl_cfg: cfg,
                    db_cfg,
                });
            }
            Err(e) if e.contains(INCONCLUSIVE) => return Err(e),
            Err(e) if is_fault_fallout(&e) => {
                h.restart();
                continue;
            }
            Err(e) => return Err(format!("{host}: {e}")),
        }
    }
    sim.cover("auth:inconclusive:spawn-exhausted");
    Err(format!(
        "{INCONCLUSIVE}: {host} kernel spawn exhausted {AUTH_RECOVER_ATTEMPTS} attempts"
    ))
}

/// Errors meaning a node's local durable copy is damaged beyond log recovery
/// (media corruption of ACKNOWLEDGED bytes, or a forked timeline the follower
/// refuses) — legitimate consequences of the fault matrix, working as
/// designed, that this property has no restore/re-seed rig for. All fold into
/// an inconclusive skip. Workload-model seeds found while landing, all replay
/// green: 0xce6a9fccf8425fc (rig asserted /me against the wrong round's
/// subject), 0xe5d5ca79d05e9df (primary bitrots its WAL and forks the
/// timeline; follower refuses — working as designed), 0xf728659274ee045c
/// (primary refuses to ship a frame that failed read-back verification).
///
/// DESIGN §4's compacted-log refusal ("cannot be rebuilt from the log alone")
/// also names "restore this node" but is NOT one of these: callers check fault
/// fallout for it and otherwise fail — under redb 4.3.0 the crash model cannot
/// brick the store. (An injected fault inside `Store::open` no longer becomes
/// the refusal at all: `Db::open` returns it as itself, DESIGN §4.)
fn needs_restore(text: &str) -> bool {
    (text.contains("restore this node from a replica or backup")
        || text.contains("refusing to guess")
        || text.contains("forked timeline")
        || text.contains("re-seed the follower"))
        && !text.contains("cannot be rebuilt from the log alone")
}

/// `recover_auth_node`'s arms run `needs_restore`, then fault fallout, then
/// fail. The compacted-log refusal must reach the second (injected cause) or
/// the third (none), never the first — and the first must still take what it
/// exists for. No auth workload reaches that refusal at its pinned base (none
/// in 5 batches, 2026-09-30), so this is the arm's oracle.
#[test]
fn the_compacted_log_refusal_is_fallout_or_a_failure_never_an_unrestorable_skip() {
    let refusal = |cause: &str| {
        format!(
            "table store cannot open ({cause}) and the log is compacted to seq 3 — the records \
             below the store's last checkpoint no longer exist locally, so the store cannot be \
             rebuilt from the log alone; restore this node from a replica or backup, or widen \
             DbConfig::retain_segments (DESIGN §4)"
        )
    };
    let injected = refusal("table store (redb): I/O error: injected read error");
    assert!(!needs_restore(&injected), "{injected}");
    assert!(
        is_fault_fallout(&injected),
        "an injected cause is fallout: {injected}"
    );
    let bare = refusal(
        "table store (redb): DB corrupted: File truncated below stored layout: file_len=4096, \
         layout_len=8192",
    );
    assert!(!needs_restore(&bare), "{bare}");
    assert!(
        !is_fault_fallout(&bare),
        "no injected cause, so it fails: {bare}"
    );
    assert!(needs_restore(
        "log frame 3 is invalid but at-or-below the store's durable horizon 7 — this is \
         corruption of acknowledged records, not a torn tail, and truncating it would silently \
         lose data; restore this node from a replica or backup (DESIGN §2)"
    ));
    assert!(needs_restore("the follower refuses a forked timeline"));
}

/// The witness boot gate's 503 (M2 sub-project 0b task 5): this workload
/// restarts its primary under fault injection, and a primary that comes back
/// with a recorded witness serves reads but withholds writes until that
/// witness answers. A request refused there simply did not happen — the same
/// "no write, no credential, no bug" outcome the fault-fallout arms already
/// tolerate, and the scan below cares only about credentials that WERE acked.
/// Keyed on the 503 body's own leading phrase (`http/app.rs`, pinned
/// byte-exact in `http.rs`).
fn is_witness_gated(body: &[u8]) -> bool {
    String::from_utf8_lossy(body).contains("this node is not accepting writes yet")
}

/// Restart-and-recover a node in place — folds local-corruption-shaped hard
/// errors into an inconclusive skip (no restore rig in this property, exactly
/// as realtime.rs's `recover_rt_node` does).
/// `auth.rs`'s copy of `replication.rs`'s `recover_exhausted_key` — the family
/// of the error the LAST recovery attempt saw.
///
/// Duplicated rather than shared because coverage keys must be `&'static str`
/// (`Sim::cover`'s signature) and each suite's keys carry its own prefix; a
/// shared helper would have to format, which that signature forbids. The match
/// is exhaustive, so a new `FaultReason` variant is a compile error in both
/// files rather than a silently unattributed give-up.
///
/// Replication measured this at 100% `:injected` on both convergence
/// properties, which is what established that "500 attempts were not enough"
/// and "the budget is too small" are different claims. Auth's split is
/// unmeasured; this key is what makes it measurable before anyone reaches for
/// `AUTH_RECOVER_ATTEMPTS`.
fn auth_recover_exhausted_key(last: &str) -> &'static str {
    match classify(last) {
        FaultReason::Injected => "auth:inconclusive:recover-exhausted:injected",
        FaultReason::Poisoned => "auth:inconclusive:recover-exhausted:poisoned",
        FaultReason::RedbLatch => "auth:inconclusive:recover-exhausted:redb-latch",
        FaultReason::DurableHorizon => "auth:inconclusive:recover-exhausted:durable-horizon",
        FaultReason::Unrelated => "auth:inconclusive:recover-exhausted:unrelated",
    }
}

fn recover_auth_node(sim: &Sim, node: &mut AuthNode) -> Result<(), String> {
    sim.cover("restart");
    node.kernel = None;
    let mut last = String::new();
    for attempt in 0..AUTH_RECOVER_ATTEMPTS {
        if attempt.is_multiple_of(AUTH_RECOVER_CYCLE_EVERY) {
            node.host.restart();
        }
        match open_auth_kernel(sim, &node.host, &node.repl_cfg, &node.db_cfg) {
            Ok(kernel) => {
                node.kernel = Some(kernel);
                return Ok(());
            }
            Err(e) if e.contains(INCONCLUSIVE) => return Err(e),
            Err(e) if needs_restore(&e) => {
                sim.cover("auth:inconclusive:local-unrestorable");
                return Err(format!(
                    "{INCONCLUSIVE}: {} local copy unrestorable (no restore rig in this \
                     property): {e}",
                    node.name
                ));
            }
            Err(e) if is_fault_fallout(&e) => last = e,
            Err(e) => return Err(format!("recover {}: {e}", node.name)),
        }
    }
    sim.cover("auth:inconclusive:recover-exhausted");
    // Same budget-or-wedge question replication.rs measured, same answer shape
    // required before anyone raises `AUTH_RECOVER_ATTEMPTS`: see
    // `replication.rs`'s `recover_exhausted_key`.
    sim.cover(auth_recover_exhausted_key(&last));
    Err(format!(
        "{INCONCLUSIVE}: recover {} exhausted {AUTH_RECOVER_ATTEMPTS} attempts (last: {last})",
        node.name
    ))
}

/// One `Kernel::step`, classifying faults exactly like realtime.rs's
/// `kstep_rt_node`.
fn kstep_auth_node(sim: &Sim, node: &mut AuthNode) -> Result<Step, String> {
    let stepped = {
        let kernel = node.kernel.as_mut().expect("kstep on a live node");
        kernel.step(&mut node.source)
    };
    match stepped {
        Ok(step) => Ok(step),
        Err(e) if is_fault_fallout(&e.to_string()) => {
            recover_auth_node(sim, node)?;
            Ok(Step::Worked)
        }
        // Media corruption of acknowledged bytes surfaces here too: the primary
        // refusing to ship a frame that failed read-back verification, or the
        // follower refusing a forked timeline (spec §5's Diverged) — both the
        // platform working as designed, both needing an out-of-scope restore.
        Err(e) if needs_restore(&e.to_string()) => Err({
            sim.cover("auth:inconclusive:needs-restore");
            format!(
                "{INCONCLUSIVE}: {} local durable copy needs restore (media corruption of \
             acknowledged data; no restore rig in this property): {e}",
                node.name
            )
        }),
        Err(e) => Err(format!("{} kernel step: {e}", node.name)),
    }
}

/// One scheduler round over the nodes — mirrors realtime.rs's
/// `rt_kernel_round`.
fn auth_kernel_round(sim: &Sim, nodes: &mut [&mut AuthNode]) -> Result<bool, String> {
    let mut worked = false;
    let mut deadlines: Vec<u64> = Vec::new();
    for node in nodes.iter_mut() {
        match kstep_auth_node(sim, node)? {
            Step::Worked => worked = true,
            Step::Idle { deadline } => {
                if let Some(d) = deadline {
                    deadlines.push(d.as_nanos());
                }
            }
        }
    }
    if worked {
        return Ok(true);
    }
    let now = sim.now().as_nanos();
    let mut next = sim.next_wakeup();
    for d in deadlines {
        if d > now {
            next = Some(next.map_or(d, |x| x.min(d)));
        }
    }
    match next {
        Some(t) if t > now => {
            sim.clock().sleep(Duration::from_nanos(t - now));
            Ok(true)
        }
        _ => Ok(false),
    }
}

/// Push a request onto `target`'s source and drive the whole node set until it
/// is answered — mirrors realtime.rs's `rt_drive_request`.
fn auth_drive_request(
    sim: &Sim,
    nodes: &mut [&mut AuthNode],
    target: usize,
    req: Request,
    budget: u32,
) -> Result<(u16, Vec<u8>), String> {
    let sink = nodes[target].responses.clone();
    let before = sink.lock().expect("responses lock poisoned").len();
    let now = sim.now().as_nanos();
    nodes[target].source.push_at(now, req);
    for _ in 0..budget {
        {
            let guard = sink.lock().expect("responses lock poisoned");
            if guard.len() > before {
                return Ok(guard[before].clone());
            }
        }
        if !auth_kernel_round(sim, nodes)? {
            break;
        }
    }
    let guard = sink.lock().expect("responses lock poisoned");
    if guard.len() > before {
        Ok(guard[before].clone())
    } else {
        sim.cover("auth:inconclusive:request-no-response");
        Err(format!(
            "{INCONCLUSIVE}: no response to a request on node {target} within budget"
        ))
    }
}

fn applied(node: &AuthNode) -> Option<u64> {
    node.kernel
        .as_ref()
        .and_then(|k| k.app().db().last_applied())
}

// ------------------------------------------------------------ durable scan

/// List a disk's files, retrying through transient injected read faults, and
/// tagging exhaustion as inconclusive (a scan that can never read is not a
/// verdict).
fn list_view(sim: &Sim, disk: &impl Disk) -> Result<Vec<String>, String> {
    for _ in 0..64 {
        match disk.list("") {
            Ok(v) => return Ok(v),
            Err(e) if is_fault_fallout(&e.to_string()) => continue,
            Err(e) => return Err(e.to_string()),
        }
    }
    sim.cover("auth:inconclusive:scan-list-faulted");
    Err(format!("{INCONCLUSIVE}: listing durable files faulted 64x"))
}

/// Read a file's whole current view (durable bytes plus any un-synced page
/// cache — a superset of the durable state, so strictly stronger for proving
/// ABSENCE). Same transient-fault handling as `list_view`.
fn read_view(sim: &Sim, disk: &impl Disk, name: &str) -> Result<Vec<u8>, String> {
    for _ in 0..64 {
        let attempt = (|| -> io::Result<Vec<u8>> {
            let file = disk.open(name)?;
            let len = file.len()?;
            let mut buf = vec![0u8; len as usize];
            if len > 0 {
                file.read_at(0, &mut buf)?;
            }
            Ok(buf)
        })();
        match attempt {
            Ok(bytes) => return Ok(bytes),
            Err(e) if is_fault_fallout(&e.to_string()) => continue,
            Err(e) => return Err(e.to_string()),
        }
    }
    sim.cover("auth:inconclusive:scan-read-faulted");
    Err(format!(
        "{INCONCLUSIVE}: reading {name:?} faulted 64x during the durable scan"
    ))
}

/// THE ORACLE. Asserts no raw credential byte-string appears in any durable
/// file reachable through `disk`. Value bytes in a `Codec` record are
/// contiguous (only their length prefix is separate), so a naive substring
/// scan suffices; and since the durable-corruption axes are scoped to `wal/`
/// and cannot fabricate a specific 64-hex/`ck_`/secret string, this NEGATIVE
/// assertion is unconditional — true under every fault. Shared by the flagship
/// (per host) and the meta-test (the global disk), so both prove the exact
/// same predicate.
fn assert_no_raw_credentials_on_disk(
    sim: &Sim,
    disk: &impl Disk,
    label: &str,
    raws: &[String],
) -> Result<(), String> {
    for name in list_view(sim, disk)? {
        let bytes = read_view(sim, disk, &name)?;
        for raw in raws {
            if !raw.is_empty() && bytes.windows(raw.len()).any(|w| w == raw.as_bytes()) {
                return Err(format!(
                    "raw credential {raw:?} found in durable file {name:?} on {label} — \
                     credentials must be hashed at rest (DESIGN §8), never stored raw"
                ));
            }
        }
    }
    Ok(())
}

/// True if `needle` appears in any of the disk's files — the anti-vacuity
/// companion to the oracle (used to prove the scan read real, populated data).
fn disk_contains(sim: &Sim, disk: &impl Disk, needle: &str) -> Result<bool, String> {
    for name in list_view(sim, disk)? {
        let bytes = read_view(sim, disk, &name)?;
        if !needle.is_empty() && bytes.windows(needle.len()).any(|w| w == needle.as_bytes()) {
            return Ok(true);
        }
    }
    Ok(false)
}

fn decode_body(body: &[u8]) -> Result<String, String> {
    serde_json::from_slice::<String>(body).map_err(|e| {
        format!(
            "decoding a credential response body: {e} (body={:?})",
            String::from_utf8_lossy(body)
        )
    })
}

// ------------------------------------------------------------- flagship

/// The workload behind the flagship property AND the determinism check. Mints
/// sessions and API keys on the primary under the fault matrix, ships them to
/// a follower over a peer-secret-authenticated link, then scans every durable
/// file on both hosts for the raw token / key / secret. Returns per-host
/// durable fingerprints plus the sorted acked-credential hashes so the
/// determinism check can compare them: `replays_identically` fingerprints only
/// the GLOBAL disk, which a host-scoped workload leaves empty, so the per-host
/// state has to ride out in the return value.
fn raw_credentials_universe(sim: &mut Sim) -> Result<(u64, u64, Vec<String>), String> {
    let db_cfg = multi_node_db_cfg();
    let mut p = spawn_auth_node(
        sim,
        "p",
        auth_primary_cfg("p:1"),
        db_cfg.clone(),
        AUTH_HORIZON,
    )?;
    let mut f = spawn_auth_node(
        sim,
        "f",
        auth_follower_cfg("f:1", "p:1"),
        db_cfg,
        AUTH_HORIZON,
    )?;

    // Every raw that must never touch disk. The peer secret is durable-config
    // on both hosts from open; sessions/keys accrue as they are minted.
    let mut raws: Vec<String> = vec![SWARM_SECRET.to_string()];
    // The hash of each acked credential — the anti-vacuity guard asserts at
    // least one of these IS on disk (so an empty/rebuilt store can't pass the
    // negative scan vacuously).
    let mut acked_hashes: Vec<String> = Vec::new();
    // The latest successfully-minted session as (raw token, its subject) — the
    // subject rides along so `/me` is checked against the token's OWN subject,
    // not the current round's (a failed login reuses the prior token).
    let mut session: Option<(String, String)> = None;

    for round in 0..AUTH_ROUNDS {
        // Mint a session on the primary.
        let subject = format!("user-{round}");
        let (status, body) = auth_drive_request(
            sim,
            &mut [&mut p, &mut f],
            0,
            Request::new(Method::Post, "/login").json(&subject),
            AUTH_STEP_BUDGET,
        )?;
        if status == 200 {
            let raw = decode_body(&body)?;
            sim.cover("auth:session-mint");
            acked_hashes.push(hash_token(&raw));
            raws.push(raw.clone());
            session = Some((raw, subject));
        } else if !is_fault_fallout(&String::from_utf8_lossy(&body)) && !is_witness_gated(&body) {
            return Err(format!(
                "unexpected /login status {status}: {}",
                String::from_utf8_lossy(&body)
            ));
        }

        // Exercise the credential on the read path, then mint an API key with
        // it. Non-200s are tolerated: an acked-but-uncheckpointed session can be
        // lost to `wal/` media corruption (replication's property 8, not this
        // one's) — but a 200 MUST resolve to the credential's own subject, which
        // is a real auth invariant this scan rides along on.
        if let Some((tok, subj)) = session.clone() {
            let (status, body) = auth_drive_request(
                sim,
                &mut [&mut p, &mut f],
                0,
                Request::new(Method::Get, "/me").header("Authorization", format!("Bearer {tok}")),
                AUTH_STEP_BUDGET,
            )?;
            if status == 200 {
                let got = decode_body(&body)?;
                if got != subj {
                    return Err(format!(
                        "/me resolved subject {got:?}, expected {subj:?} for the presented \
                         session token"
                    ));
                }
            }

            let (status, body) = auth_drive_request(
                sim,
                &mut [&mut p, &mut f],
                0,
                Request::new(Method::Post, "/keys")
                    .header("Authorization", format!("Bearer {tok}")),
                AUTH_STEP_BUDGET,
            )?;
            if status == 200 {
                let raw = decode_body(&body)?;
                sim.cover("auth:key-mint");
                acked_hashes.push(hash_token(&raw));
                raws.push(raw);
            } else if status != 401
                && !is_fault_fallout(&String::from_utf8_lossy(&body))
                && !is_witness_gated(&body)
            {
                return Err(format!(
                    "unexpected /keys status {status}: {}",
                    String::from_utf8_lossy(&body)
                ));
            }
        }

        // Let replication ship this round's records to the follower.
        for _ in 0..16u32 {
            if !auth_kernel_round(sim, &mut [&mut p, &mut f])? {
                break;
            }
        }
    }

    // Drive to convergence: the follower applying a shipped frame proves the
    // hash-at-rest guarantee survives the wire, and that the peer secret was
    // exercised (the link only ships once the Hello carrying it was accepted).
    for _ in 0..AUTH_CONVERGE_ROUNDS {
        let pa = applied(&p);
        if pa.is_some() && pa == applied(&f) {
            break;
        }
        if !auth_kernel_round(sim, &mut [&mut p, &mut f])? {
            break;
        }
    }
    if applied(&f).is_some() {
        sim.cover("auth:peer-secret");
    }

    // ORACLE: no raw on either host's durable files.
    let p_disk = p.host.disk();
    let f_disk = f.host.disk();
    assert_no_raw_credentials_on_disk(sim, &p_disk, "p", &raws)?;
    assert_no_raw_credentials_on_disk(sim, &f_disk, "f", &raws)?;

    // Anti-vacuity: at least one acked hash IS on disk. Gated on no media
    // corruption, which can silently cut an acked-but-uncheckpointed row (the
    // above-horizon window); the negative scan above stays unconditional.
    if !durable_corruption_fired(sim) && !acked_hashes.is_empty() {
        let mut seen = false;
        for h in &acked_hashes {
            if disk_contains(sim, &p_disk, h)? || disk_contains(sim, &f_disk, h)? {
                seen = true;
                break;
            }
        }
        if !seen {
            return Err(format!(
                "anti-vacuity: none of {} acked credential hashes found on either host — the \
                 durable scan may have read empty/rebuilt state",
                acked_hashes.len()
            ));
        }
    }

    acked_hashes.sort();
    Ok((
        p.host.durable_fingerprint(),
        f.host.durable_fingerprint(),
        acked_hashes,
    ))
}

/// `give_up` beside `cover`, same reasoning as replication.rs's
/// `finish_universe`: the coverage key is the floorable cause-side signal, and
/// `give_up` is the scalar the runner reports, which mapping the marker to
/// `Ok(())` here would otherwise hide.
fn finish_auth_universe(
    sim: &Sim,
    r: Result<(u64, u64, Vec<String>), String>,
) -> Result<(), String> {
    match r {
        Err(e) if e.contains(INCONCLUSIVE) => {
            sim.cover("auth:inconclusive");
            sim.give_up(e);
            Ok(())
        }
        Ok(_) => Ok(()),
        Err(e) => Err(e),
    }
}

/// `classify` keys on error PROSE (DESIGN §7 makes that prose product surface),
/// so a reworded exemplar silently becomes `Unrelated` and the wedge it used to
/// name is filed as `:unrelated` — a give-up that looks attributed and is not.
/// The replication twin of this test is
/// `every_recover_exhausted_sub_key_is_reachable_and_distinct`; both must exist,
/// because each file owns its own key prefix.
#[test]
fn every_auth_recover_exhausted_sub_key_is_reachable_and_distinct() {
    let cases = [
        (
            "simulated disk write error (injected fault)",
            "auth:inconclusive:recover-exhausted:injected",
        ),
        (
            "WAL poisoned by an earlier IO failure",
            "auth:inconclusive:recover-exhausted:poisoned",
        ),
        (
            "Storage error: close and re-open the database",
            "auth:inconclusive:recover-exhausted:redb-latch",
        ),
        (
            "log frame 3 is invalid but at-or-below the store's durable horizon 7",
            "auth:inconclusive:recover-exhausted:durable-horizon",
        ),
        ("", "auth:inconclusive:recover-exhausted:unrelated"),
    ];
    for (text, expected) in cases {
        assert_eq!(
            auth_recover_exhausted_key(text),
            expected,
            "classify() no longer routes {text:?} to {expected} — the error was reworded or \
             classify's arms moved, and the give-up it names is now misattributed rather \
             than loudly wrong"
        );
    }
    let mut keys: Vec<&str> = cases
        .iter()
        .map(|(t, _)| auth_recover_exhausted_key(t))
        .collect();
    keys.sort_unstable();
    let before = keys.len();
    keys.dedup();
    assert_eq!(
        before,
        keys.len(),
        "two fault families share a sub-key, so a wedge and a budget count as one thing"
    );
}

// ------------------------------------------------------------------ tests

/// The flagship (DESIGN §8 as a mechanical property): across the full fault
/// matrix, raw credentials — session tokens, API keys, the peer secret —
/// appear NOWHERE in any host's durable store or WAL. Media corruption is left
/// ON: the negative assertion is corruption-robust (a `wal/`-scoped bit flip
/// cannot fabricate a raw), so it holds unconditionally.
#[test]
fn raw_credentials_never_touch_the_durable_store() {
    run_universes_swarm_with!(
        0x2EB3_0001,
        128,
        |_cfg: &mut FaultConfig| {},
        |sim: &mut Sim| {
            let r = raw_credentials_universe(sim);
            finish_auth_universe(sim, r)
        },
        require: ["auth:session-mint", "auth:key-mint", "auth:peer-secret"]
    );
}

/// The naive victim (hard rule 3): a mint path that stores the RAW token where
/// only its hash belongs. Routes can't express this bug — `create_session`
/// always hashes, and apps see only `&mut Tx` — so the victim drives the table
/// primitive directly, exactly as kernel.rs's split-done-mark meta-test does.
/// `transact` syncs the effects record, so the raw lands in `wal/` at once;
/// the SHARED oracle must catch it.
#[test]
fn the_harness_catches_a_raw_token_written_to_disk() {
    let sim = Sim::with_faults(0x2EB3_DEAD, FaultConfig::none());
    let mut app = App::open(
        Box::new(sim.disk()),
        Schema::new(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )
    .unwrap();

    // A realistic 64-hex raw token — what a session token actually looks like.
    let raw = "0123456789abcdef".repeat(4);
    assert_eq!(raw.len(), 64);

    // THE BUG: the raw token in the `token_hash` column instead of its hash.
    app.db_for_tests()
        .transact(|tx| {
            tx.put(&Session {
                id: Id::from_raw(0x0123_4567_89ab_cdef),
                token_hash: raw.clone(),
                subject: "victim".to_string(),
                expires_at: u64::MAX,
            });
            Ok::<(), io::Error>(())
        })
        .unwrap();

    let disk = sim.disk();
    let err = assert_no_raw_credentials_on_disk(&sim, &disk, "global", std::slice::from_ref(&raw))
        .expect_err("the oracle must catch a raw token written to disk");
    assert!(
        err.contains(&raw),
        "the oracle must name the raw it found: {err}"
    );

    // The WAL nuance (spec §7): the raw reached the durable log because the
    // victim built the effects record from the raw, not its hash. Pin it
    // directly so the meta-test discriminates on the durability surface the
    // guarantee is about.
    let mut wal_has_raw = false;
    for name in disk.list("wal/").unwrap() {
        let bytes = read_view(&sim, &disk, &name).unwrap();
        if bytes.windows(raw.len()).any(|w| w == raw.as_bytes()) {
            wal_has_raw = true;
            break;
        }
    }
    assert!(
        wal_has_raw,
        "the raw token must be present in a WAL segment (the effects record)"
    );

    // Belt-and-suspenders discrimination: the CORRECT app (storing the hash)
    // passes the SAME oracle — so the oracle keys on the raw, not merely on any
    // 64-hex string (a hash is also 64 hex chars and must pass).
    let sim_ok = Sim::with_faults(0x2EB3_600D, FaultConfig::none());
    let mut app_ok = App::open(
        Box::new(sim_ok.disk()),
        Schema::new(),
        Box::new(sim_ok.entropy()),
        AppConfig::default(),
    )
    .unwrap();
    app_ok
        .db_for_tests()
        .transact(|tx| {
            tx.put(&Session {
                id: Id::from_raw(0x0123_4567_89ab_cdef),
                token_hash: hash_token(&raw),
                subject: "ok".to_string(),
                expires_at: u64::MAX,
            });
            Ok::<(), io::Error>(())
        })
        .unwrap();
    assert_no_raw_credentials_on_disk(&sim_ok, &sim_ok.disk(), "global", &[raw])
        .expect("a hashed-at-rest app must pass the oracle");
}

/// Determinism check (spec §7): the same seed replays to the same durable
/// state, proving the added crypto path (SHA-256 hashing) introduced no
/// nondeterminism. Compares per-host fingerprints via the workload's return
/// value, since `replays_identically` fingerprints only the (here-empty)
/// global disk.
#[test]
fn auth_workload_replays_identically_for_the_same_seed() {
    replays_identically(0x2EB3_5EED, raw_credentials_universe).unwrap();
}
