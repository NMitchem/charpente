//! Realtime tail tests (PLAN M1 sub-project 3, task 6): the `subscriber_stall`
//! fault axis's consumer, the flagship reconnecting-client property (the PLAN
//! exit criterion — "a reconnecting realtime client provably misses no
//! events"), its off-by-one meta-test, and the determinism check.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::io;
use std::time::Duration;

use charpente::event;
use charpente::http::{ApiError, App, AppConfig, Ctx, Cursor, Method, Request, Subscription};
use charpente::kernel::{Kernel, RecordedResponses, SimSinkHandle, SimSource, SinkHandles, Step};
use charpente::repl::protocol::{Msg, decode, encode};
use charpente::repl::role::Role;
use charpente::repl::{MAX_MESSAGE_BYTES, Repl, ReplConfig};
use charpente::{DbConfig, Schema, Tx, route, routes};
use charpente_core::db::Db;
use charpente_core::event::EventRow;
use charpente_core::io::{Clock, Conn, Disk, Entropy, Net};
use charpente_core::log::WalConfig;
use charpente_core::table::{Codec, View};
use charpente_sim::checks::is_fault_fallout;
use charpente_sim::{FaultConfig, Sim, SimEntropy, SimHost, run_universes_swarm_with};
use serde::{Deserialize, Serialize};

const SEC: u64 = 1_000_000_000;

// ------------------------------------------------------------------ events

#[event]
struct Ping {
    n: u64,
}

#[event]
struct Pong {
    n: u64,
}

#[derive(Serialize, Deserialize)]
struct EmitBody {
    n: u64,
    key: Option<String>,
}

#[route(POST "/ping")]
fn emit_ping(tx: &mut Tx, body: EmitBody) -> Result<(), ApiError> {
    match body.key {
        Some(key) => tx.emit_keyed(key, Ping { n: body.n }),
        None => tx.emit(Ping { n: body.n }),
    }
    Ok(())
}

#[route(POST "/pong")]
fn emit_pong(tx: &mut Tx, body: EmitBody) -> Result<(), ApiError> {
    match body.key {
        Some(key) => tx.emit_keyed(key, Pong { n: body.n }),
        None => tx.emit(Pong { n: body.n }),
    }
    Ok(())
}

/// Three subscribe flavors so the flagship workload exercises the name and
/// key filter branches, not just "everything, unfiltered" (T5 left both
/// unexercised) — one mode, deterministically chosen per universe from the
/// seed, drives every (re)subscribe within that universe.
#[route(SSE "/feed-both")]
fn feed_both(tx: &mut Tx, ctx: &Ctx, cursor: Option<Cursor>) -> Result<Subscription, ApiError> {
    let _ = (tx, ctx);
    Ok(Subscription::of::<Ping>().and::<Pong>().cursor(cursor))
}

#[route(SSE "/feed-ping")]
fn feed_ping(tx: &mut Tx, ctx: &Ctx, cursor: Option<Cursor>) -> Result<Subscription, ApiError> {
    let _ = (tx, ctx);
    Ok(Subscription::of::<Ping>().cursor(cursor))
}

#[route(SSE "/feed-keyed")]
fn feed_keyed(tx: &mut Tx, ctx: &Ctx, cursor: Option<Cursor>) -> Result<Subscription, ApiError> {
    let _ = (tx, ctx);
    Ok(Subscription::of::<Ping>()
        .and::<Pong>()
        .key("org-a")
        .cursor(cursor))
}

fn realtime_app(sim: &Sim) -> io::Result<App> {
    Ok(App::open(
        Box::new(sim.disk()),
        Schema::new(),
        Box::new(sim.entropy()),
        AppConfig {
            body_cap: 1 << 20,
            db: charpente::DbConfig {
                wal: WalConfig {
                    dir: "wal".to_string(),
                    segment_bytes: 4096,
                },
                store_path: "tables.redb".to_string(),
                // Small on purpose: this workload's job is to make a client
                // fall behind retention (the `realtime:gap` path) within a
                // modest number of emits per universe, not just exercise the
                // happy backlog-resume path.
                event_retention: 4,
                checkpoint_every: 2,
                ..charpente::DbConfig::default()
            },
        },
    )?
    .mount(routes![
        emit_ping, emit_pong, feed_both, feed_ping, feed_keyed
    ]))
}

const PING_ONLY: &[&str] = &["ping"];
const BOTH: &[&str] = &["ping", "pong"];

/// This universe's subscribe path, subscribed event names, and optional key
/// filter — one fixed mode for the whole universe, chosen from the seed so
/// it never perturbs any entropy stream.
fn mode_for(seed: u64) -> (&'static str, &'static [&'static str], Option<&'static str>) {
    match seed % 3 {
        0 => ("/feed-both", BOTH, None),
        1 => ("/feed-ping", PING_ONLY, None),
        _ => ("/feed-keyed", BOTH, Some("org-a")),
    }
}

/// One emit request's route and body — content is arbitrary (varied only to
/// exercise the name/key filter branches); the ledger never trusts what was
/// *sent*, only what the server actually materialized (see
/// `Client::sync_ledger`), so nothing here needs to be remembered by the
/// caller.
fn pick_emit(sim: &Sim) -> (&'static str, EmitBody) {
    let e = sim.entropy();
    let is_ping = e.next_u64().is_multiple_of(2);
    let key = match e.next_u64() % 3 {
        0 => None,
        1 => Some("org-a".to_string()),
        _ => Some("org-b".to_string()),
    };
    let n = sim.entropy().next_u64() % 1000;
    if is_ping {
        ("/ping", EmitBody { n, key })
    } else {
        ("/pong", EmitBody { n, key })
    }
}

/// A `SimSink` stall closure keyed on this universe's `subscriber_stall`
/// rate: `FaultConfig.subscriber_stall` has no internal consumption site
/// (`SimSink` lives outside `charpente-sim` and holds no `Sim` reference), so
/// the workload that owns the closure is exactly where the roll belongs.
/// Zero draws when `rate <= 0.0` — the same zero-draw-when-off shape every
/// other axis uses, applied at the workload's own consumption site instead
/// of inside the sim.
fn stall_closure(entropy: SimEntropy, rate: f64) -> Box<dyn FnMut() -> bool + Send> {
    Box::new(move || {
        if rate <= 0.0 {
            return false;
        }
        let unit = (entropy.next_u64() >> 11) as f64 / (1u64 << 53) as f64;
        unit < rate
    })
}

/// The resume contract's core discriminator (DESIGN §6): every frame a
/// client accepts, across every reconnect, must carry a strictly increasing
/// `id` — a resume that ever redelivers (or rewinds past) an id the client
/// already saw is never a legitimate retry. Shared by the flagship property
/// and its off-by-one meta-test, so both exercise the identical oracle.
fn assert_strictly_increasing(id: u64, last_id: &mut Option<u64>) -> Result<(), String> {
    if let Some(prev) = *last_id
        && id <= prev
    {
        return Err(format!(
            "duplicate or out-of-order event id {id} received — a resumed tail must never \
             redeliver an id at or before the last one seen (previous id {prev})"
        ));
    }
    *last_id = Some(id);
    Ok(())
}

// ------------------------------------------------------------------ client

/// What a pending (already-delivered) frame's direct id lookup found in the
/// ledger.
enum MatchOutcome {
    /// The ledger's entry at this id matches the delivered content exactly.
    Matched,
    /// A real content divergence at this id — always a hard failure.
    Mismatch(String),
    /// `sync_ledger` has not scanned this far yet (it may have been skipped
    /// by an injected fault earlier in the same round the event was durably
    /// committed) — try again after a later `sync_ledger` call, never a
    /// verdict on its own.
    Pending,
}

/// The reconnecting client's accumulated state across every epoch and
/// reconnect in one universe: the ledger it must eventually match (keyed by
/// event id — see below), every delivered-but-not-yet-content-verified
/// frame (`pending`), its resume cursor, whether it currently holds a live
/// tail, and the full observed frame trace (the determinism check's
/// oracle).
///
/// The ledger is built from the server's own `charpente.events` table
/// (`sync_ledger`), never from client-side response codes: a `500` does not
/// mean "not committed" — `TxError::Io`'s own contract is "the request may
/// or may not have committed" (e.g. a post-sync apply failure — an injected
/// `write_error`/`sync_error` while checkpointing the table store — still
/// leaves the WAL record durable; the tail can deliver it before this
/// client's own next `sync_ledger` call gets a chance to see it, since that
/// later call reads through the very same connection the apply failure just
/// poisoned). Content verification is therefore deferred (`pending`/
/// `try_match`) and keyed by id rather than by sequential position: a
/// sequential "next expected slot" pointer cannot distinguish "this ledger
/// entry is genuinely forfeited (below a later bare reconnect's boundary)"
/// from "this ledger entry is exactly what a still-pending frame is waiting
/// on" when `sync_ledger` is lagging — both look identical positionally.
/// Direct id lookup sidesteps the distinction entirely: a delivered frame's
/// own id is compared against the ledger entry AT THAT id, full stop.
struct Client {
    mode_names: &'static [&'static str],
    mode_key: Option<&'static str>,
    /// Every event `sync_ledger` has read back, keyed by id.
    ledger: BTreeMap<u64, (&'static str, Option<String>, String)>,
    /// How far `sync_ledger` has scanned `charpente.events` — server-
    /// relative, persists across reopens.
    next_scan_id: u64,
    /// Ids strictly below this are forfeited: a bare reconnect's
    /// `next_event_id` at push time, so `settled()` never waits on them.
    /// Monotonic (a later forfeit is always a superset of an earlier one).
    floor: u64,
    /// Delivered frames not yet checked against the ledger: `(id, name,
    /// json)`, oldest first.
    pending: VecDeque<(u64, &'static str, String)>,
    /// Every id ever content-verified (matched against the ledger) via
    /// `drain` — the membership bound `settled()` checks against, not a
    /// high-water mark. A high-water mark (max verified id + 1) is UNSOUND:
    /// if a later id gets verified before an earlier one was ever delivered
    /// at all (a reconnect that verifies 0-5, then jumps straight to 7
    /// without ever receiving 6), the mark advances past the gap and
    /// permanently excuses it — the interior-skip meta-test proves
    /// membership catches what the mark could not (T6 review). `floor`
    /// stays a separate, permanent forfeit trim; this set only ever grows.
    verified: BTreeSet<u64>,
    cursor: Option<Cursor>,
    need_subscribe: bool,
    current_handle: Option<SimSinkHandle>,
    frames_consumed: usize,
    last_id: Option<u64>,
    trace: Vec<(u64, &'static str, String)>,
}

impl Client {
    fn new(mode_names: &'static [&'static str], mode_key: Option<&'static str>) -> Client {
        Client {
            mode_names,
            mode_key,
            ledger: BTreeMap::new(),
            next_scan_id: 0,
            floor: 0,
            pending: VecDeque::new(),
            verified: BTreeSet::new(),
            cursor: None,
            need_subscribe: true,
            current_handle: None,
            frames_consumed: 0,
            last_id: None,
            trace: Vec::new(),
        }
    }

    fn matches(&self, name: &str, key: &Option<String>) -> bool {
        self.mode_names.contains(&name)
            && (self.mode_key.is_none() || self.mode_key == key.as_deref())
    }

    /// True once every delivered frame has been content-verified AND
    /// nothing scanned in `[floor, next_scan_id)` that matches this
    /// subscription is unaccounted for — the property's success condition
    /// at quiescence. `floor` excludes forfeited ids outright; anything at
    /// or past `next_scan_id` simply hasn't been looked at yet and is
    /// irrelevant here (a still-open tail would still be delivering it).
    fn settled(&self) -> bool {
        self.pending.is_empty() && self.first_unaccounted().is_none()
    }

    /// The first matching (subscribed-filter-passing) ledger id in `[floor,
    /// next_scan_id)` that has not been content-verified — `None` at
    /// quiescence. `settled()`'s worker, factored out so a diagnostic can
    /// name the exact missing id (`unsettled_report`, the skip meta-test's
    /// discriminator) instead of only reporting counts. The `.min` clamp on
    /// `lo` is the same floor-trim safety the old bound used — `floor` can
    /// run ahead of `next_scan_id` transiently (a bare reconnect's boundary
    /// observed before this client's own ledger scan has caught up to it),
    /// and `BTreeMap::range` panics on an inverted bound.
    fn first_unaccounted(&self) -> Option<u64> {
        let lo = self.floor.min(self.next_scan_id);
        for (id, (name, key, _)) in self.ledger.range(lo..self.next_scan_id) {
            if self.matches(name, key) && !self.verified.contains(id) {
                return Some(*id);
            }
        }
        None
    }

    /// The diagnostic `realtime_workload` returns when the settle phase
    /// exhausts its attempts still unsettled — factored onto `Client` so the
    /// skip meta-test can byte-pin the missing id the same way the
    /// off-by-one meta-test byte-pins `duplicate`.
    fn unsettled_report(&self) -> String {
        format!(
            "client never caught up: ledger has {} entries scanned through id {}, floor {}, \
             {} frames still unverified, first unaccounted matching id {:?}, cursor={:?} \
             need_subscribe={}",
            self.ledger.len(),
            self.next_scan_id,
            self.floor,
            self.pending.len(),
            self.first_unaccounted(),
            self.cursor,
            self.need_subscribe
        )
    }

    /// Reads every event committed since the last scan and records it,
    /// decoding each row's payload back through the same `Codec` the
    /// platform encoded it with and re-deriving the exact JSON the delivery
    /// path would produce (`ev_json`, `charpente::http`) — so a ledger entry
    /// is never a guess about what was *sent*, only a readback of what was
    /// *stored*.
    fn sync_ledger(&mut self, db: &Db) -> io::Result<()> {
        let snap = db.snapshot()?;
        let rows: Vec<EventRow> = snap.scan_from(self.next_scan_id, 4096)?;
        for row in rows {
            self.next_scan_id = row.id.raw() + 1;
            let (name, json): (&'static str, String) = match row.name.as_str() {
                "ping" => (
                    "ping",
                    serde_json::to_string(&Ping::decode(&mut &row.payload[..])?)
                        .expect("Ping serializes"),
                ),
                "pong" => (
                    "pong",
                    serde_json::to_string(&Pong::decode(&mut &row.payload[..])?)
                        .expect("Pong serializes"),
                ),
                other => {
                    return Err(io::Error::other(format!(
                        "ledger scan found event name {other:?} — this workload only ever emits \
                         ping/pong"
                    )));
                }
            };
            self.ledger.insert(row.id.raw(), (name, row.key, json));
        }
        Ok(())
    }

    /// Direct id lookup — no positional skipping, no forfeit interaction:
    /// this delivered frame's own id is either in the ledger with matching
    /// content, in the ledger with different content (a real bug), or not
    /// scanned yet.
    fn try_match(&self, id: u64, name: &str, json: &str) -> MatchOutcome {
        match self.ledger.get(&id) {
            Some((exp_name, key, exp_json)) if *exp_name == name && exp_json == json => {
                // Content matches the ledger row at this id exactly — but a
                // delivered frame is only legitimate if that row ALSO
                // satisfies this subscription's own filter. Over-delivery
                // (a leaked wrong-name or wrong-key row) carries genuine
                // content at its own id, so content equality alone would
                // silently Match it; this closes that direction.
                if !self.matches(exp_name, key) {
                    let violation = if !self.mode_names.contains(exp_name) {
                        format!(
                            "name {exp_name:?} is not in the subscribed set {:?}",
                            self.mode_names
                        )
                    } else {
                        format!(
                            "key {key:?} does not match the subscribed key {:?}",
                            self.mode_key
                        )
                    };
                    return MatchOutcome::Mismatch(format!(
                        "over-delivery at event id {id}: delivered frame name {name:?} \
                         violates this subscription's filter — {violation}"
                    ));
                }
                MatchOutcome::Matched
            }
            Some((exp_name, _, exp_json)) => MatchOutcome::Mismatch(format!(
                "content mismatch at event id {id}: expected ({exp_name}, {exp_json}) but \
                 received ({name}, {json})"
            )),
            None => MatchOutcome::Pending,
        }
    }

    /// Drains as much of `pending` as the current ledger allows (oldest
    /// first, stopping — not failing — the moment an id hasn't been scanned
    /// yet).
    fn drain(&mut self) -> Result<(), String> {
        while let Some(&(id, name, ref json)) = self.pending.front() {
            let json = json.clone();
            match self.try_match(id, name, &json) {
                MatchOutcome::Matched => {
                    self.pending.pop_front();
                    self.verified.insert(id);
                }
                MatchOutcome::Pending => break,
                MatchOutcome::Mismatch(e) => return Err(e),
            }
        }
        Ok(())
    }

    /// `sync_ledger` (fault-fallout tolerant) then `drain` — the pairing
    /// used at every point new ground truth or new deliveries might exist.
    fn sync_and_drain(&mut self, db: &Db) -> Result<(), String> {
        if let Err(e) = self.sync_ledger(db)
            && !is_fault_fallout(&e.to_string())
        {
            return Err(format!("ledger scan failed non-injected: {e}"));
        }
        // else: skip; the next successful scan (later this epoch, or at the
        // next reopen) picks up anything still durable.
        self.drain()
    }

    /// Absorbs one round's worth of responses and frames. `new_responses` is
    /// exactly what was recorded since the last `absorb` call, in push
    /// order: the subscribe response first (if `subscribe` is `Some`), then
    /// one per emit request pushed this round — a prefix may be missing if a
    /// crash cut the round short mid-way. `subscribe` is `(was_bare,
    /// boundary)`: `boundary` is `next_event_id` read right before the
    /// subscribe was pushed — the precise point a *successful bare*
    /// subscribe resumes from, folded straight into `floor`.
    fn absorb(
        &mut self,
        sim: &Sim,
        db: &Db,
        new_responses: &[(u16, Vec<u8>)],
        subscribe: Option<(bool, u64)>,
        sinks: &SinkHandles,
    ) -> Result<(), String> {
        self.sync_and_drain(db)?;

        let mut ri = 0usize;
        if subscribe.is_some()
            && let Some((status, body)) = new_responses.first()
        {
            ri += 1;
            let (was_bare, boundary) = subscribe.expect("checked by outer is_some");
            let text = String::from_utf8_lossy(body).to_string();
            match *status {
                200 => {
                    self.need_subscribe = false;
                    if was_bare {
                        self.floor = self.floor.max(boundary);
                    }
                    let handle = sinks
                        .lock()
                        .expect("sink handles lock poisoned by an earlier panic")
                        .last()
                        .cloned()
                        .expect("a 200 subscribe response implies a new recorded sink handle");
                    self.current_handle = Some(handle);
                    self.frames_consumed = 0;
                }
                409 if text.contains("retention floor") => {
                    // Spec §5 / brief: re-sync current state, reconnect
                    // bare, and trim expectation to events after the floor.
                    sim.cover("realtime:gap");
                    self.cursor = None;
                    self.need_subscribe = true;
                }
                _ if is_fault_fallout(&text) => {
                    // Not acked; need_subscribe stays true, retried next
                    // round with the same cursor.
                }
                _ => return Err(format!("unexpected subscribe response {status}: {text}")),
            }
        }
        for (status, body) in &new_responses[ri..] {
            if *status != 200 {
                let text = String::from_utf8_lossy(body);
                if !is_fault_fallout(&text) {
                    return Err(format!("unexpected emit response {status}: {text}"));
                }
            }
        }

        if let Some(handle) = self.current_handle.clone() {
            let frames = handle.frames();
            let mut saw_gap = false;
            for frame in &frames[self.frames_consumed..] {
                self.trace.push((frame.id, frame.name, frame.json.clone()));
                if frame.name == "charpente.gap" {
                    saw_gap = true;
                    sim.cover("realtime:gap");
                    self.cursor = None;
                    self.need_subscribe = true;
                    break;
                }
                assert_strictly_increasing(frame.id, &mut self.last_id)?;
                self.pending
                    .push_back((frame.id, frame.name, frame.json.clone()));
                self.cursor = Some(Cursor {
                    id: frame.id,
                    seq: frame.seq,
                });
            }
            self.frames_consumed = frames.len();
            if saw_gap {
                self.current_handle = None;
            } else if handle.closed() {
                // The only other way a tail dies (T5 policy): the sink
                // reported Full/Closed. Our closure is the sole source of
                // Full, so this is exactly the stall firing.
                sim.cover("realtime:stall-drop");
                sim.cover("fault:subscriber_stall");
                self.need_subscribe = true;
                self.current_handle = None;
            }
        }

        // This round's fresh deliveries get a fair shot at draining too,
        // against whatever `sync_ledger` already picked up above.
        self.sync_and_drain(db)
    }
}

// ---------------------------------------------------------------- workload

const EPOCHS: u32 = 5;
const ROUNDS_PER_EPOCH: u32 = 4;
const SETTLE_ATTEMPTS: u32 = 12;

/// Builds this round's subscribe request (if the client needs one) and
/// queues its stall closure, returning `(was_bare, boundary)` for
/// `Client::absorb` — `boundary` is `next_event_id` read right before the
/// push, so it is the exact resume point a bare subscribe gets. Marks
/// `realtime:resume-backlog` when a cursored resume already has backlog
/// waiting.
fn push_subscribe(
    sim: &Sim,
    kernel: &Kernel,
    source: &mut SimSource,
    path: &'static str,
    cursor: Option<Cursor>,
    t: u64,
) -> Result<(bool, u64), String> {
    let entropy = sim.entropy();
    source.push_stall(stall_closure(entropy, sim.subscriber_stall()));
    let boundary = kernel
        .app()
        .db()
        .next_event_id()
        .map_err(|e| format!("next_event_id read failed: {e}"))?;
    let (req, was_bare) = match cursor {
        Some(c) => {
            if boundary > c.id + 1 {
                sim.cover("realtime:resume-backlog");
            }
            (
                Request::new(Method::Get, format!("{path}?cursor={}", c.wire())),
                false,
            )
        }
        None => (Request::new(Method::Get, path), true),
    };
    source.push_at(t, req);
    Ok((was_bare, boundary))
}

/// One universe: a kernel and one reconnecting client, driven through
/// `EPOCHS` crash/restart-bounded sessions of `ROUNDS_PER_EPOCH` rounds each
/// (emits plus, whenever the client lacks a live tail, a (re)subscribe using
/// its last received id.seq), then a settle phase that drains any remaining
/// backlog through fresh injected faults until the client has caught up to
/// everything the ledger says it should have received. Returns the full
/// observed frame trace (id, name, json) — the determinism check's oracle —
/// on success.
fn realtime_workload(sim: &mut Sim) -> Result<Vec<(u64, &'static str, String)>, String> {
    let (path, mode_names, mode_key) = mode_for(sim.seed());
    let mut client = Client::new(mode_names, mode_key);

    for _epoch in 0..EPOCHS {
        let entropy = sim.entropy();
        sim.crash_disk_after(64 + entropy.next_u64() % 512);
        let app = match realtime_app(sim) {
            Ok(app) => app,
            Err(e) => {
                if !is_fault_fallout(&e.to_string()) {
                    return Err(format!("open failed non-injected: {e}"));
                }
                sim.restart();
                continue;
            }
        };
        let mut kernel = match Kernel::new(app, Box::new(sim.clock())) {
            Ok(k) => k,
            Err(e) => {
                if !is_fault_fallout(&e.to_string()) {
                    return Err(format!("Kernel::new failed non-injected: {e}"));
                }
                sim.restart();
                continue;
            }
        };
        // A fresh App/Kernel never carries our tail over.
        client.need_subscribe = true;
        client.current_handle = None;

        let mut source = SimSource::new(Box::new(sim.clock()), sim.now().as_nanos() + 2000 * SEC);
        let sinks = source.sinks();
        let responses = source.responses();
        let mut resp_idx = 0usize;
        let mut t = sim.now().as_nanos();

        for _round in 0..ROUNDS_PER_EPOCH {
            let mut subscribe = None;
            if client.need_subscribe {
                subscribe = Some(push_subscribe(
                    sim,
                    &kernel,
                    &mut source,
                    path,
                    client.cursor,
                    t,
                )?);
                t += SEC;
            }

            let n_emits = 1 + sim.entropy().next_u64() % 3;
            for _ in 0..n_emits {
                let (route, body) = pick_emit(sim);
                source.push_at(t, Request::new(Method::Post, route).json(&body));
                t += SEC;
            }

            let run_result = kernel.run(&mut source);

            let new_responses = {
                let guard = responses
                    .lock()
                    .expect("responses lock poisoned by an earlier panic");
                guard[resp_idx..].to_vec()
            };
            resp_idx += new_responses.len();
            client.absorb(sim, kernel.app().db(), &new_responses, subscribe, &sinks)?;

            if let Err(e) = run_result {
                if !is_fault_fallout(&e.to_string()) {
                    return Err(format!("kernel.run failed non-injected: {e}"));
                }
                sim.restart();
                client.need_subscribe = true;
                client.current_handle = None;
                break;
            }
        }
    }

    // Settle: drain any remaining backlog with no new emits, retrying
    // through whatever injected faults remain, until the client has caught
    // up to everything the ledger says it should have.
    for _attempt in 0..SETTLE_ATTEMPTS {
        if client.settled() {
            break;
        }
        let app = match realtime_app(sim) {
            Ok(app) => app,
            Err(e) => {
                if !is_fault_fallout(&e.to_string()) {
                    return Err(format!("settle open failed non-injected: {e}"));
                }
                sim.restart();
                continue;
            }
        };
        let mut kernel = match Kernel::new(app, Box::new(sim.clock())) {
            Ok(k) => k,
            Err(e) => {
                if !is_fault_fallout(&e.to_string()) {
                    return Err(format!("settle Kernel::new failed non-injected: {e}"));
                }
                sim.restart();
                continue;
            }
        };
        client.need_subscribe = true;
        client.current_handle = None;

        let mut source = SimSource::new(Box::new(sim.clock()), sim.now().as_nanos() + 2000 * SEC);
        let sinks = source.sinks();
        let responses = source.responses();
        let t = sim.now().as_nanos();

        let subscribe = push_subscribe(sim, &kernel, &mut source, path, client.cursor, t)?;

        let run_result = kernel.run(&mut source);
        let new_responses = responses
            .lock()
            .expect("responses lock poisoned by an earlier panic")
            .clone();
        client.absorb(
            sim,
            kernel.app().db(),
            &new_responses,
            Some(subscribe),
            &sinks,
        )?;
        if let Err(e) = run_result {
            if !is_fault_fallout(&e.to_string()) {
                return Err(format!("settle kernel.run failed non-injected: {e}"));
            }
            sim.restart();
            client.need_subscribe = true;
            client.current_handle = None;
        }
    }

    if !client.settled() {
        return Err(client.unsettled_report());
    }

    Ok(client.trace)
}

// ------------------------------------------------------------------ tests

#[test]
fn reconnecting_client_misses_nothing_under_the_fault_matrix() {
    run_universes_swarm_with!(
        0x2EB2_0001,
        128,
        |_cfg: &mut FaultConfig| {},
        |sim: &mut Sim| realtime_workload(sim).map(|_trace| ()),
        require: [
            "realtime:resume-backlog",
            "realtime:gap",
            "realtime:stall-drop",
            "fault:subscriber_stall"
        ]
    );
}

#[test]
fn realtime_frame_traces_replay_identically_for_the_same_seed() {
    charpente_sim::checks::replays_identically(0x2EB2_5EED, realtime_workload).unwrap();
}

/// The naive victim: a resume that reads from `cursor.id` instead of
/// `validate_subscribe`'s correct `cursor.id + 1` (kernel/mod.rs — untouched,
/// and unreachable to override from outside the crate) — redelivering the
/// cursor's own event. Built directly against the real `charpente.events`
/// table (`EventRow`/`View::scan_from`, the same primitives `deliver_unit`
/// uses), the discriminating-twin pattern from sub-projects 1-2
/// (`harness_catches_a_split_done_mark` in kernel.rs): drive real platform
/// primitives in the wrong order/position, and show the oracle catches it.
#[test]
fn the_harness_catches_a_resume_that_replays_the_cursor_event() {
    let sim = Sim::with_faults(0x2EB2_DEAD, FaultConfig::none());
    let mut app = App::open(
        Box::new(sim.disk()),
        Schema::new(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )
    .unwrap();
    for n in 0..4u64 {
        app.db_for_tests()
            .transact(|tx| {
                tx.emit(Ping { n });
                Ok::<(), std::io::Error>(())
            })
            .unwrap();
    }
    let snap = app.db_for_tests().snapshot().unwrap();
    let events: Vec<EventRow> = snap.scan_from(0, 64).unwrap();
    assert_eq!(events.len(), 4);

    // What a correct client would already have received before reconnecting:
    // the first two events.
    let mut last_id = None;
    for e in &events[..2] {
        assert_strictly_increasing(e.id.raw(), &mut last_id).unwrap();
    }
    let cursor = Cursor {
        id: events[1].id.raw(),
        seq: events[1].seq,
    };

    // THE BUG.
    let naive: Vec<EventRow> = snap.scan_from(cursor.id, 64).unwrap();

    let err = naive
        .iter()
        .find_map(|e| assert_strictly_increasing(e.id.raw(), &mut last_id).err())
        .expect("the harness must catch a resume that redelivers the cursor event");
    assert!(err.contains("duplicate"), "{err}");
    assert!(err.contains(&cursor.id.to_string()), "{err}");
}

/// The skip-direction naive victim: a resume that reads from `cursor.id + 2`
/// instead of `cursor.id + 1` — skipping the successor event (id
/// `cursor.id + 1`) outright rather than redelivering the cursor's own event
/// (the off-by-one test's victim, above). `assert_strictly_increasing` does
/// NOT catch this — ids still strictly ascend across the skip, which is
/// legal on its own. Only a membership-based `settled()` can: it finds the
/// skipped id sitting matched-in-the-ledger-but-never-verified at
/// quiescence. RED evidence (T6 review): against the OLD high-water
/// `verified_through` bound, this exact scenario passed `settled()` — the
/// later ids' verification retroactively excused the skipped one. The fixed
/// `first_unaccounted`/`verified` membership check is what turns that into a
/// named failure below.
#[test]
fn the_harness_catches_a_resume_that_skips_the_successor_event() {
    let sim = Sim::with_faults(0x2EB2_51C1, FaultConfig::none());
    let mut app = App::open(
        Box::new(sim.disk()),
        Schema::new(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )
    .unwrap();
    for n in 0..6u64 {
        app.db_for_tests()
            .transact(|tx| {
                tx.emit(Ping { n });
                Ok::<(), std::io::Error>(())
            })
            .unwrap();
    }
    let snap = app.db_for_tests().snapshot().unwrap();
    let events: Vec<EventRow> = snap.scan_from(0, 64).unwrap();
    assert_eq!(events.len(), 6);

    let mut client = Client::new(PING_ONLY, None);
    client.sync_ledger(app.db_for_tests()).unwrap();

    for e in &events[..2] {
        let json = serde_json::to_string(&Ping::decode(&mut &e.payload[..]).unwrap())
            .expect("Ping serializes");
        client.pending.push_back((e.id.raw(), "ping", json));
    }
    client.drain().unwrap();
    let cursor_id = events[1].id.raw();

    // THE BUG: resume at cursor.id + 2 instead of cursor.id + 1 — the
    // successor event (id = cursor.id + 1) is never requested and never
    // delivered.
    for e in events.iter().filter(|e| e.id.raw() >= cursor_id + 2) {
        let json = serde_json::to_string(&Ping::decode(&mut &e.payload[..]).unwrap())
            .expect("Ping serializes");
        client.pending.push_back((e.id.raw(), "ping", json));
    }
    client.drain().unwrap();

    let missing = cursor_id + 1;
    assert!(
        !client.settled(),
        "the harness must catch a resume that skips event id {missing}"
    );
    assert_eq!(
        client.first_unaccounted(),
        Some(missing),
        "settled() must name the skipped id precisely, not just detect *a* gap"
    );
    let report = client.unsettled_report();
    assert!(
        report.contains(&missing.to_string()),
        "the failure report must name the missing id: {report}"
    );
}

// ==================================== multi-node: follower tails + failover
//
// Task 8: two REPLICATED kernels (the rig shape from replication.rs's T8
// section — `KNode`/`spawn_kernel_node`/`kernel_round`/`drive_request`/
// `CtrlConn`, reproduced here since integration-test binaries share no code)
// wired to T6's OWN client model above, reused verbatim (never forked): a
// client tails the FOLLOWER's kernel while emits land on the primary
// (`realtime:follower-tail`); the primary is killed and the follower
// promoted, and the SAME client reconnects with its cursor, proving the
// failover kept exactly what the follower had durably shipped
// (`realtime:failover-resume` — DECISIONS §6's bounded-loss window is
// implicit here: the client's ledger is synced from whichever node it is
// CONNECTED to, so an event never shipped to the follower simply never
// enters the ledger `settled()` waits on, mirroring how
// `failover_preserves_everything_the_follower_acked` checks a PREFIX of the
// primary's timeline rather than "everything ever acked"); and a second,
// deliberately foreign cursor — built by partitioning the follower, driving
// the primary past the follower's shipped position, and recording one
// unshipped event's id.seq before killing the primary — proves the promoted
// follower refuses it by name by name (kernel/mod.rs's `validate_subscribe`
// 409 family) rather than ever serving the wrong content
// (`realtime:failover-diverged`).

/// Marker prefix meaning "this universe's faults were too persistent for the
/// multi-node dance to complete" — a covered skip. This file's own copy of
/// replication.rs's `INCONCLUSIVE` idiom (two separate test binaries share no
/// platform code, only the pattern).
const INCONCLUSIVE: &str = "universe-inconclusive";

const RT_RECOVER_ATTEMPTS: u32 = 200;
const RT_STEP_BUDGET: u32 = 20_000;
const RT_SETTLE_ATTEMPTS: u32 = 40;
const RT_HORIZON: u64 = 300 * SEC;

/// Disables the durable-media-corruption axes (mirrors replication.rs's own
/// `no_media_corruption`): a primary that bitrots its own WAL and silently
/// loses an acked-but-unconverged record is the above-horizon promise's
/// property (M1 replication task 8's `wal_corruption_on_the_primary_...`),
/// not this one — this property's failover loss window is the DELIBERATE,
/// honest one (partition + unshipped tail), never an incidental one from
/// bitrot noise.
fn no_media_corruption(cfg: &mut FaultConfig) {
    cfg.bit_flip = 0.0;
    cfg.misdirected_write = 0.0;
}

fn rt_primary_cfg(addr: &str) -> ReplConfig {
    ReplConfig {
        listen: Some(addr.to_string()),
        initial_role: Role::Primary,
    }
}

fn rt_follower_cfg(listen: &str, primary: &str) -> ReplConfig {
    ReplConfig {
        listen: Some(listen.to_string()),
        initial_role: Role::Follower {
            primary: primary.to_string(),
        },
    }
}

fn multi_node_db_cfg() -> DbConfig {
    DbConfig {
        wal: WalConfig {
            dir: "wal".to_string(),
            segment_bytes: 4096,
        },
        store_path: "tables.redb".to_string(),
        event_retention: 64,
        checkpoint_every: 8,
        ..DbConfig::default()
    }
}

/// One replication node driven through its `Kernel` — this file's own copy
/// of replication.rs's `KNode`, scoped to what this property needs (no
/// jobs/cron: this workload only ever mounts the emit/subscribe routes).
struct RNode {
    host: SimHost,
    kernel: Option<Kernel>,
    source: SimSource,
    responses: RecordedResponses,
    sinks: SinkHandles,
    name: String,
    repl_cfg: ReplConfig,
    db_cfg: DbConfig,
    recoveries: u32,
    flipped: bool,
}

/// Build the App + Repl + Kernel for one host, with the same torn-role
/// first-boot re-init idiom `open_kernel` uses in replication.rs.
fn open_rt_kernel(
    sim: &Sim,
    h: &SimHost,
    cfg: &ReplConfig,
    db_cfg: &DbConfig,
    flipped: bool,
) -> Result<Kernel, String> {
    let mut app = App::open(
        Box::new(h.disk()),
        Schema::new(),
        Box::new(h.entropy()),
        AppConfig {
            body_cap: 1 << 20,
            db: db_cfg.clone(),
        },
    )
    .map_err(|e| format!("App::open: {e}"))?
    .mount(routes![
        emit_ping, emit_pong, feed_both, feed_ping, feed_keyed
    ]);

    let repl = match Repl::open(
        Box::new(h.net()),
        Box::new(h.disk()),
        cfg.clone(),
        app.db_for_tests(),
    ) {
        Ok((repl, _notice)) => repl,
        Err(e) if e.to_string().contains("integrity check") => {
            if flipped {
                return Err(format!(
                    "{INCONCLUSIVE}: {} torn role file after a role flip — restore, not \
                     re-init, is the fix: {e}",
                    h.name()
                ));
            }
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

fn spawn_rt_node(
    sim: &Sim,
    host: &str,
    cfg: ReplConfig,
    db_cfg: DbConfig,
    horizon: u64,
) -> Result<RNode, String> {
    let h = sim.host(host);
    for _ in 0..RT_RECOVER_ATTEMPTS {
        match open_rt_kernel(sim, &h, &cfg, &db_cfg, false) {
            Ok(kernel) => {
                let source = SimSource::new(Box::new(sim.clock()), horizon);
                let responses = source.responses();
                let sinks = source.sinks();
                return Ok(RNode {
                    host: h,
                    kernel: Some(kernel),
                    source,
                    responses,
                    sinks,
                    name: host.to_string(),
                    repl_cfg: cfg,
                    db_cfg,
                    recoveries: 0,
                    flipped: false,
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
    Err(format!(
        "{INCONCLUSIVE}: {host} kernel spawn exhausted {RT_RECOVER_ATTEMPTS} attempts"
    ))
}

/// Restart-and-recover a node in place. Local-corruption-shaped hard errors
/// (no restore rig in this property — it isn't what this task proves) are
/// folded into the same inconclusive skip the Db-level `recover_node` uses in
/// replication.rs, rather than attempting the kernel-level restore dance.
fn recover_rt_node(sim: &Sim, node: &mut RNode) -> Result<(), String> {
    sim.cover("restart");
    node.recoveries += 1;
    node.kernel = None;
    let mut last = String::new();
    for _ in 0..RT_RECOVER_ATTEMPTS {
        node.host.restart();
        match open_rt_kernel(sim, &node.host, &node.repl_cfg, &node.db_cfg, node.flipped) {
            Ok(kernel) => {
                node.kernel = Some(kernel);
                return Ok(());
            }
            Err(e) if e.contains(INCONCLUSIVE) => return Err(e),
            Err(e)
                if e.contains("restore this node from a replica or backup")
                    || e.contains("cannot be rebuilt from the log alone")
                    || e.contains("refusing to guess") =>
            {
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
    Err(format!(
        "{INCONCLUSIVE}: recover {} exhausted {RT_RECOVER_ATTEMPTS} attempts (last: {last})",
        node.name
    ))
}

/// One `Kernel::step`, classifying faults exactly like replication.rs's
/// `kstep_node`.
fn kstep_rt_node(sim: &Sim, node: &mut RNode) -> Result<Step, String> {
    let stepped = {
        let kernel = node.kernel.as_mut().expect("kstep on a live node");
        kernel.step(&mut node.source)
    };
    match stepped {
        Ok(step) => Ok(step),
        Err(e) if is_fault_fallout(&e.to_string()) => {
            recover_rt_node(sim, node)?;
            Ok(Step::Worked)
        }
        Err(e) => Err(format!("{} kernel step: {e}", node.name)),
    }
}

/// One scheduler round over kernel nodes — mirrors replication.rs's
/// `kernel_round`.
fn rt_kernel_round(sim: &Sim, nodes: &mut [&mut RNode]) -> Result<bool, String> {
    let mut worked = false;
    let mut deadlines: Vec<u64> = Vec::new();
    for node in nodes.iter_mut() {
        match kstep_rt_node(sim, node)? {
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

/// Push a request onto `target`'s source and drive the whole node set until
/// it is answered — mirrors replication.rs's `drive_request`.
fn rt_drive_request(
    sim: &Sim,
    nodes: &mut [&mut RNode],
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
        if !rt_kernel_round(sim, nodes)? {
            break;
        }
    }
    let guard = sink.lock().expect("responses lock poisoned");
    if guard.len() > before {
        Ok(guard[before].clone())
    } else {
        Err(format!(
            "{INCONCLUSIVE}: no response to a request on node {target} within budget"
        ))
    }
}

/// A control connection to a node's replication listener — mirrors
/// replication.rs's `CtrlConn`.
struct CtrlConn {
    conn: Box<dyn Conn>,
    buf: Vec<u8>,
}

impl CtrlConn {
    fn open(sim: &Sim, addr: &str, verb: &Msg) -> Result<CtrlConn, String> {
        let conn = sim
            .host("cli")
            .net()
            .connect(addr)
            .map_err(|e| format!("{INCONCLUSIVE}: ctrl connect {addr}: {e}"))?;
        conn.send(&encode(verb))
            .map_err(|e| format!("{INCONCLUSIVE}: ctrl send: {e}"))?;
        Ok(CtrlConn {
            conn,
            buf: vec![0u8; MAX_MESSAGE_BYTES],
        })
    }

    fn poll(&mut self) -> Result<Option<Msg>, String> {
        match self.conn.recv(&mut self.buf) {
            Ok(Some(0)) => Err("control conn closed before a reply".to_string()),
            Ok(Some(n)) => decode(&self.buf[..n]).map(Some).map_err(|e| e.to_string()),
            Ok(None) => Ok(None),
            Err(e) => Err(format!("ctrl recv: {e}")),
        }
    }
}

fn drive_ctrl_kernel(
    sim: &Sim,
    nodes: &mut [&mut RNode],
    cli: &mut CtrlConn,
    budget: u32,
) -> Result<Msg, String> {
    for _ in 0..budget {
        let advanced = rt_kernel_round(sim, nodes)?;
        match cli.poll() {
            Ok(Some(m)) => return Ok(m),
            Ok(None) => {}
            Err(e) => return Err(format!("{INCONCLUSIVE}: {e}")),
        }
        if !advanced {
            return Err(format!("{INCONCLUSIVE}: quiescent before a control reply"));
        }
    }
    Err(format!("{INCONCLUSIVE}: no control reply within budget"))
}

/// Drives `client` against `nodes[target]` until it holds a live, settled
/// tail: (re)subscribes whenever it lacks one via `push_subscribe` — T6's own
/// helper, already fully generic over any `Kernel`/`SimSource` pair, reused
/// unmodified — and otherwise drives a bounded burst and drains through
/// `Client::absorb`/`sync_and_drain`, T6's SAME oracle, unmodified. A
/// recovery on the target node (fresh `Kernel`, so any existing sink handle
/// is orphaned — the tail never explicitly closes it, so `absorb` alone would
/// wait forever) forces a resubscribe, mirroring how the single-node flagship
/// resets `need_subscribe`/`current_handle` at every epoch/restart boundary.
fn drive_client_to_settled(
    sim: &Sim,
    nodes: &mut [&mut RNode],
    target: usize,
    client: &mut Client,
    path: &'static str,
) -> Result<(), String> {
    for _ in 0..RT_SETTLE_ATTEMPTS {
        let recoveries_before = nodes[target].recoveries;
        if client.need_subscribe {
            let before = nodes[target]
                .responses
                .lock()
                .expect("responses lock poisoned")
                .len();
            let now = sim.now().as_nanos();
            let subscribe_info = {
                let node = &mut *nodes[target];
                let kernel = node
                    .kernel
                    .as_ref()
                    .ok_or_else(|| format!("{INCONCLUSIVE}: target has no kernel"))?;
                push_subscribe(sim, kernel, &mut node.source, path, client.cursor, now)?
            };
            let mut got = None;
            for _ in 0..RT_STEP_BUDGET {
                {
                    let guard = nodes[target]
                        .responses
                        .lock()
                        .expect("responses lock poisoned");
                    if guard.len() > before {
                        got = Some(guard[before].clone());
                    }
                }
                if got.is_some() {
                    break;
                }
                if !rt_kernel_round(sim, nodes)? {
                    break;
                }
            }
            let Some(resp) = got else {
                return Err(format!("{INCONCLUSIVE}: no subscribe reply within budget"));
            };
            let node = &mut *nodes[target];
            let kernel = node
                .kernel
                .as_ref()
                .ok_or_else(|| format!("{INCONCLUSIVE}: target has no kernel"))?;
            client.absorb(
                sim,
                kernel.app().db(),
                &[resp],
                Some(subscribe_info),
                &node.sinks,
            )?;
        } else {
            for _ in 0..64 {
                if !rt_kernel_round(sim, nodes)? {
                    break;
                }
            }
            let node = &mut *nodes[target];
            let kernel = node
                .kernel
                .as_ref()
                .ok_or_else(|| format!("{INCONCLUSIVE}: target has no kernel"))?;
            client.absorb(sim, kernel.app().db(), &[], None, &node.sinks)?;
        }
        if nodes[target].recoveries > recoveries_before {
            client.need_subscribe = true;
            client.current_handle = None;
        }
        if client.settled() && !client.need_subscribe {
            return Ok(());
        }
    }
    Ok(())
}

/// The three-phase property. Setup: a primary `p` (emits+writes) and a
/// follower `f` (the client's tail). Phase 1: subscribe on `f`, emit on `p`,
/// prove live delivery through the shipped log — `realtime:follower-tail`.
/// Setup for phases 2/3: partition `f` from `p`, drive `p` past what `f` ever
/// shipped, record one such event's id.seq (an honestly-constructed foreign
/// cursor — never one `f` could have minted), then kill `p` for good and
/// promote `f`. Phase 2: the SAME client reconnects with its (valid, `f`-
/// shipped) cursor and must settle exactly — `realtime:failover-resume`.
/// Phase 3: a fresh subscribe using the deliberately foreign cursor must be
/// refused by name (never served) — `realtime:failover-diverged`.
fn multi_node_realtime_universe(sim: &mut Sim) -> Result<(), String> {
    let db_cfg = multi_node_db_cfg();
    let mut p = spawn_rt_node(sim, "p", rt_primary_cfg("p:1"), db_cfg.clone(), RT_HORIZON)?;
    let mut f = spawn_rt_node(sim, "f", rt_follower_cfg("f:1", "p:1"), db_cfg, RT_HORIZON)?;

    let (path, mode_names, mode_key) = mode_for(sim.seed());
    let mut client = Client::new(mode_names, mode_key);

    // ---------------------------------------------------- Phase 1: follower-tail
    drive_client_to_settled(sim, &mut [&mut p, &mut f], 1, &mut client, path)?;
    for _ in 0..6u32 {
        let (route, body) = pick_emit(sim);
        let (status, resp_body) = rt_drive_request(
            sim,
            &mut [&mut p, &mut f],
            0,
            Request::new(Method::Post, route).json(&body),
            RT_STEP_BUDGET,
        )?;
        if status != 200 && !is_fault_fallout(&String::from_utf8_lossy(&resp_body)) {
            return Err(format!(
                "unexpected emit status {status} on p: {resp_body:?}"
            ));
        }
        drive_client_to_settled(sim, &mut [&mut p, &mut f], 1, &mut client, path)?;
    }
    if !client.settled() {
        return Err(client.unsettled_report());
    }
    if !client.trace.is_empty() {
        sim.cover("realtime:follower-tail");
    }

    // ------------------------------------------- Phase 2/3 setup: divergence
    sim.partition("p", "f");
    let mut divergent: Option<Cursor> = None;
    for _ in 0..8u32 {
        if divergent.is_some() || p.kernel.is_none() {
            break;
        }
        let boundary = p
            .kernel
            .as_ref()
            .expect("checked above")
            .app()
            .db()
            .next_event_id()
            .map_err(|e| e.to_string())?;
        let (route, body) = pick_emit(sim);
        let (status, resp_body) = match rt_drive_request(
            sim,
            &mut [&mut p],
            0,
            Request::new(Method::Post, route).json(&body),
            RT_STEP_BUDGET,
        ) {
            Ok(v) => v,
            Err(e) if e.contains(INCONCLUSIVE) => continue,
            Err(e) => return Err(e),
        };
        if status == 200 {
            if let Some(k) = p.kernel.as_ref() {
                let snap = k.app().db().snapshot().map_err(|e| e.to_string())?;
                let rows: Vec<EventRow> = snap.scan_from(boundary, 1).map_err(|e| e.to_string())?;
                if let Some(row) = rows.first() {
                    divergent = Some(Cursor {
                        id: row.id.raw(),
                        seq: row.seq,
                    });
                }
            }
        } else if !is_fault_fallout(&String::from_utf8_lossy(&resp_body)) {
            return Err(format!(
                "unexpected emit status {status} on p during the partition window: {resp_body:?}"
            ));
        }
    }
    let Some(divergent) = divergent else {
        return Err(format!(
            "{INCONCLUSIVE}: could not construct a p-only divergent cursor within the retry \
             budget"
        ));
    };

    // Kill p for good — the partitioned tail above never ships.
    p.host.crash_now();

    // Promote f.
    f.flipped = true;
    let reply = CtrlConn::open(sim, "f:1", &Msg::CtrlPromote)
        .and_then(|mut cli| drive_ctrl_kernel(sim, &mut [&mut f], &mut cli, RT_STEP_BUDGET))?;
    match reply {
        Msg::CtrlOk { message } => {
            if !message.contains("last_applied") {
                return Err(format!("promote CtrlOk must name last_applied: {message}"));
            }
        }
        Msg::CtrlErr { message } => {
            return Err(format!("promote refused unexpectedly: {message}"));
        }
        other => return Err(format!("unexpected promote reply: {other:?}")),
    }
    if !f.kernel.as_ref().is_some_and(|k| k.is_primary()) {
        return Err(
            "f must report is_primary() immediately after a successful CtrlPromote reply"
                .to_string(),
        );
    }

    // ---------------------------------------------------- Phase 2: failover-resume
    client.need_subscribe = true;
    client.current_handle = None;
    drive_client_to_settled(sim, &mut [&mut f], 0, &mut client, path)?;
    if !client.settled() {
        return Err(client.unsettled_report());
    }
    sim.cover("realtime:failover-resume");

    // ------------------------------------------------- Phase 3: failover-diverged
    let mut got_family = false;
    for _ in 0..8u32 {
        if f.kernel.is_none() {
            break;
        }
        let before = f.responses.lock().expect("responses lock poisoned").len();
        let now = sim.now().as_nanos();
        f.source.push_at(
            now,
            Request::new(Method::Get, format!("{path}?cursor={}", divergent.wire())),
        );
        let mut resp = None;
        for _ in 0..RT_STEP_BUDGET {
            {
                let guard = f.responses.lock().expect("responses lock poisoned");
                if guard.len() > before {
                    resp = Some(guard[before].clone());
                }
            }
            if resp.is_some() {
                break;
            }
            if !rt_kernel_round(sim, &mut [&mut f])? {
                break;
            }
        }
        let Some((status, body)) = resp else {
            return Err(format!(
                "{INCONCLUSIVE}: no reply to the deliberately foreign-cursor subscribe"
            ));
        };
        let text = String::from_utf8_lossy(&body).to_string();
        if status == 409
            && (text.contains("diverged from this node's timeline")
                || text.contains("retention floor"))
        {
            got_family = true;
            break;
        }
        if is_fault_fallout(&text) {
            continue;
        }
        return Err(format!(
            "a cursor referencing an event minted only on p's lost (partitioned, unshipped) \
             tail must be refused by name — diverged or below-floor, never served: got {status} \
             {text}"
        ));
    }
    if !got_family {
        return Err(format!(
            "{INCONCLUSIVE}: never got a clean diverged/below-floor reply for the foreign \
             cursor within the retry budget"
        ));
    }
    sim.cover("realtime:failover-diverged");

    Ok(())
}

fn finish_rt_universe(sim: &Sim, r: Result<(), String>) -> Result<(), String> {
    match r {
        Err(e) if e.contains(INCONCLUSIVE) => {
            sim.cover("realtime:multi-node-inconclusive");
            Ok(())
        }
        other => other,
    }
}

#[test]
fn follower_tails_and_the_failover_resume_divergence_property() {
    run_universes_swarm_with!(
        0x2EB2_0002,
        128,
        no_media_corruption,
        |sim: &mut Sim| {
            let r = multi_node_realtime_universe(sim);
            finish_rt_universe(sim, r)
        },
        require: [
            "realtime:follower-tail",
            "realtime:failover-resume",
            "realtime:failover-diverged"
        ]
    );
}
