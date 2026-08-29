//! Realtime tail tests (PLAN M1 sub-project 3, task 6): the `subscriber_stall`
//! fault axis's consumer, the flagship reconnecting-client property (the PLAN
//! exit criterion — "a reconnecting realtime client provably misses no
//! events"), its off-by-one meta-test, and the determinism check.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::io;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use charpente::event;
use charpente::http::{
    ApiError, App, AppConfig, Ctx, Cursor, EmittedEvent, Method, Request, Route, RouteDef,
    RouteGlue, SubEntry, Subscription,
};
use charpente::kernel::{Kernel, RecordedResponses, SimSinkHandle, SimSource, SinkHandles, Step};
use charpente::repl::protocol::{Msg, decode, encode};
use charpente::repl::role::Role;
use charpente::repl::{MAX_MESSAGE_BYTES, Repl, ReplConfig};
use charpente::wire::{TypeSchema, Wire, WireU64};
use charpente::{DbConfig, Schema, Tx, Wire as WireDerive, route, routes};
use charpente_core::db::Db;
use charpente_core::event::EventRow;
use charpente_core::io::{Clock, Conn, Disk, Entropy, Net};
use charpente_core::log::WalConfig;
use charpente_core::table::{Codec, Row, View};
use charpente_sim::checks::{durable_corruption_fired, is_fault_fallout};
use charpente_sim::{FaultConfig, Sim, SimEntropy, SimHost, run_universes_swarm_with};
use serde::{Deserialize, Serialize};

const SEC: u64 = 1_000_000_000;

// ------------------------------------------------------------------ events

// `n: WireU64`, not a bare `u64` (task 6): `#[event]` derives `Wire`, and a
// 64-bit integer only crosses the wire as a string. The stored `Codec` bytes
// are unchanged — `WireU64`'s codec IS `u64`'s — so only the frame JSON
// differs ({"n":"3"} rather than {"n":3}).
#[event]
struct Ping {
    n: WireU64,
}

#[event]
struct Pong {
    n: WireU64,
}

#[derive(WireDerive, Serialize, Deserialize)]
struct EmitBody {
    n: WireU64,
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
#[route(SSE "/feed-both", emits(Ping, Pong))]
fn feed_both(tx: &mut Tx, ctx: &Ctx, cursor: Option<Cursor>) -> Result<Subscription, ApiError> {
    let _ = (tx, ctx);
    Ok(Subscription::of::<Ping>().and::<Pong>().cursor(cursor))
}

#[route(SSE "/feed-ping", emits(Ping))]
fn feed_ping(tx: &mut Tx, ctx: &Ctx, cursor: Option<Cursor>) -> Result<Subscription, ApiError> {
    let _ = (tx, ctx);
    Ok(Subscription::of::<Ping>().cursor(cursor))
}

/// `emits(...)` describes WHICH event types reach the stream, not which rows
/// pass `.key(...)` — the key is a per-emit call-site value the contract
/// cannot know (same reason `EventInfo.keyed` is always false), so a keyed
/// feed declares the same two types the unkeyed one does.
#[route(SSE "/feed-keyed", emits(Ping, Pong))]
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
            ..AppConfig::default()
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
    let n = WireU64(sim.entropy().next_u64() % 1000);
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
    /// Ids `sync_ledger` proved aged out of retention: an id a scan stepped
    /// over because its row was gone (first-returned-id > `next_scan_id`;
    /// event ids are dense, so the skipped span was emitted then GC'd). No
    /// future scan will ever return them — recorded here at scan time rather
    /// than silently advancing `next_scan_id` past them, so `drain` can
    /// CONFIRM a delivered frame whose row aged out before it was recorded.
    /// This set is "which ids are gone", NOT "which are forgiven": the
    /// received-only gate stays `pending` membership, in `drain`.
    gone: BTreeSet<u64>,
    /// Ids strictly below this are forfeited: a bare reconnect's
    /// `next_event_id` at push time, so `settled()` never waits on them.
    /// Monotonic (a later forfeit is always a superset of an earlier one).
    floor: u64,
    /// Delivered frames not yet checked against the ledger: `(id, name,
    /// json)`, oldest first. Every entry was RECEIVED live (pushed from a
    /// tail delivery in `absorb`, the only live push site) — the structural
    /// fact `drain` leans on to confirm an aged-out (`gone`) id whose ledger
    /// row disappeared before it could be content-verified, without ever
    /// forgiving an id the client never received.
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
            gone: BTreeSet::new(),
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
        // On-scan gap detection (never advance past a gap silently): event
        // ids are dense below `next_event_id`, so any id in
        // `[next_scan_id, first-returned-id)` has no row — it was emitted and
        // then aged out of retention (GC deletes an oldest-first prefix). Its
        // ledger row is gone for good; record it so `drain` can confirm a
        // delivered frame at that id BY ID. Derived entirely from the scan
        // already in hand — zero extra disk ops, so the injected-fault
        // schedule is untouched.
        if let Some(first) = rows.first() {
            for gone in self.next_scan_id..first.id.raw() {
                self.gone.insert(gone);
            }
        }
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
    /// first). A delivered frame whose id `sync_ledger` proved aged out of
    /// retention (`gone` — its row is gone and no future scan will ever
    /// return it) can never be content-verified, so it is CONFIRMED BY ID
    /// instead of blocking the drain forever. That confirmation resolves ONLY
    /// ids the client actually RECEIVED: every `pending` entry was pushed
    /// from a live tail delivery (`absorb`), so a `gone` pop here is always a
    /// received-then-aged event, never a missing one — the ledger-scan side
    /// has no business forgiving an id the client never got (that is the
    /// subscribe-side 409 / gap-frame contract's job). A still-retained id
    /// the ledger has not caught up to yet stops the drain (not fails),
    /// retried after the next scan.
    fn drain(&mut self) -> Result<(), String> {
        while let Some(&(id, name, ref json)) = self.pending.front() {
            let json = json.clone();
            match self.try_match(id, name, &json) {
                MatchOutcome::Matched => {
                    self.pending.pop_front();
                    self.verified.insert(id);
                }
                MatchOutcome::Pending if self.gone.contains(&id) => {
                    // Received live (it is in `pending`) but its ledger row
                    // aged out before `sync_ledger` recorded it: confirm by
                    // id. A gone row is unverifiable, never misdelivered —
                    // delivery-time `assert_strictly_increasing` already held
                    // the strictly-increasing resume contract for it.
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
                409 if text.contains("diverged from this node's timeline")
                    && durable_corruption_fired(sim) =>
                {
                    // T6 interlude investigation (t6-realtime-interlude-
                    // investigation.md): a synced-and-delivered event can
                    // still be lost to the sanctioned single-node
                    // media-corruption window (M1 sub-project 1's ruling —
                    // `bit_flip`/`misdirected_write`, "wal/"-scoped,
                    // above-horizon, window `checkpoint_every`). That's a
                    // real loss, not a bug, but it rewinds the store
                    // BACKWARD — unlike the retention-floor arm above, which
                    // only ever advances `floor` forward. Mirroring that
                    // arm's plain `cursor=None; need_subscribe=true` would
                    // leave forfeited-but-remembered ids sitting in
                    // `ledger`/`pending` at or past the store's new (lower)
                    // head, which `first_unaccounted` would then block on
                    // forever with no future emit ever able to satisfy them.
                    // So prune down to the store's current head instead of
                    // just resetting the cursor.
                    //
                    // FIX ROUND 2, M-7: this is a live-`Db` read, so a
                    // latched store makes it fail (M2 sub-project 0a) — the
                    // second such read in this file, and the one the fix
                    // round's census missed. Disposition matches §1's: retry,
                    // never excuse. On fault fallout NOTHING here changes,
                    // so `need_subscribe` stays true exactly as in the
                    // fallout arm below, the client re-subscribes with the
                    // same cursor, and the prune happens on the retry once
                    // the node has recovered. No id is forgiven and no
                    // verdict is skipped: a client that never manages the
                    // prune never settles, and `unsettled_report` names it.
                    match db.next_event_id() {
                        Ok(head) => {
                            self.ledger.retain(|&id, _| id < head);
                            self.pending.retain(|&(id, ..)| id < head);
                            self.next_scan_id = self.next_scan_id.min(head);
                            sim.cover("realtime:gap");
                            self.cursor = None;
                            self.need_subscribe = true;
                        }
                        Err(e) if is_fault_fallout(&e.to_string()) => {}
                        Err(e) => {
                            return Err(format!("next_event_id read failed non-injected: {e}"));
                        }
                    }
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
/// Settle attempts: how many times the client is handed a live node to
/// reconnect to and catch up from.
const SETTLE_ATTEMPTS: u32 = 12;
/// Tries `settle_boot` spends getting one of those nodes up, and how many of
/// them share a power cycle — see `settle_boot`.
const SETTLE_BOOT_TRIES: u32 = 64;
const SETTLE_CYCLE_EVERY: u32 = 8;

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

/// Brings a node up for one settle attempt, retrying through injected
/// faults. `Ok(None)` means the budget ran out with nothing serving.
///
/// A `Db::open`/`Kernel::new` that died on an injected write error is a
/// process that FAILED TO START — it is not a power cycle, and modelling it
/// as one is what makes a node unbootable rather than merely hard to boot.
/// `Store::apply` commits at `Durability::None`, so every `Sim::restart`
/// vanishes the store's uncheckpointed state and forces the next open to
/// discard, rebuild from the WAL, and re-`persist_store_at_open` — more
/// writes at the same per-write failure odds, on every single try. Measured
/// on seed `0x536007ad99fd90bf` (`write_error` 0.144): power-cycling before
/// each try, **0 of 20_000 boots** ever served a subscribe; retrying in
/// place, the same universe is serving after ~16 power cycles. Retrying in
/// place is also the faithful model — the machine did not lose power, so the
/// page cache is still there for the next process to open.
///
/// The periodic power cycle is not optional: a pending `crash_disk_after`
/// countdown can fire mid-boot, and a crashed sim disk answers every
/// operation with `crashed_error` until `Sim::restart` — so an in-place
/// retry loop with no cycle at all would spin until its budget expired.
fn settle_boot(sim: &Sim) -> Result<Option<Kernel>, String> {
    for try_n in 0..SETTLE_BOOT_TRIES {
        if try_n > 0 && try_n.is_multiple_of(SETTLE_CYCLE_EVERY) {
            sim.restart();
        }
        let app = match realtime_app(sim) {
            Ok(app) => app,
            Err(e) => {
                if !is_fault_fallout(&e.to_string()) {
                    return Err(format!("settle open failed non-injected: {e}"));
                }
                continue;
            }
        };
        match Kernel::new(app, Box::new(sim.clock())) {
            Ok(kernel) => return Ok(Some(kernel)),
            Err(e) => {
                if !is_fault_fallout(&e.to_string()) {
                    return Err(format!("settle Kernel::new failed non-injected: {e}"));
                }
            }
        }
    }
    Ok(None)
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
                // `push_subscribe` reads `next_event_id` off the live `Db`,
                // so it fails whenever the store has latched — classified
                // exactly like the `kernel.run` call below, and recovered the
                // same way (a restart mints a fresh redb handle and a fresh
                // latch flag). Without this the universe fails outright on an
                // injected fault; see the fix-round report's latent finding,
                // seed 0xdc154f8c66271f15.
                match push_subscribe(sim, &kernel, &mut source, path, client.cursor, t) {
                    Ok(info) => {
                        subscribe = Some(info);
                        t += SEC;
                    }
                    Err(e) if is_fault_fallout(&e) => {
                        sim.restart();
                        client.need_subscribe = true;
                        client.current_handle = None;
                        break;
                    }
                    Err(e) => return Err(format!("push_subscribe failed non-injected: {e}")),
                }
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
    //
    // Each attempt boots its node through `settle_boot` rather than a single
    // `Db::open`, because "the client failed to catch up" and "there was no
    // node to catch up FROM" are different facts and only the first is this
    // property's subject. That distinction became load-bearing with M2
    // sub-project 0a: a latched store now poisons the node, so an epoch whose
    // store latches ends with the client disconnected instead of quietly
    // fed off redb's page cache — the client must then reconnect, and a
    // `write_error` rate high enough to latch a store is also high enough to
    // make `Db::open` itself a coin-flip.
    //
    // This CANNOT mask a missed event, and that is the only thing about it
    // worth checking twice. Retries add chances to RECEIVE, nothing else:
    // `verified` grows solely through `drain` matching a frame the server
    // actually delivered, and a resume reads from `cursor.id + 1`, so an id
    // the platform ever skips is one the cursor has already moved past —
    // no later reconnect requests it, no later reconnect delivers it, and
    // `first_unaccounted` keeps naming it however many attempts follow. The
    // budgets bound how long the client may keep trying; they do not touch
    // what counts as caught up.
    for _attempt in 0..SETTLE_ATTEMPTS {
        if client.settled() {
            break;
        }
        let Some(mut kernel) = settle_boot(sim)? else {
            sim.restart();
            continue;
        };
        client.need_subscribe = true;
        client.current_handle = None;

        let mut source = SimSource::new(Box::new(sim.clock()), sim.now().as_nanos() + 2000 * SEC);
        let sinks = source.sinks();
        let responses = source.responses();
        let t = sim.now().as_nanos();

        // Same classification as the epoch loop above and as this attempt's
        // own `kernel.run` below: a latched store makes this read fail, and
        // the recovery is a restart, not a universe failure.
        let subscribe = match push_subscribe(sim, &kernel, &mut source, path, client.cursor, t) {
            Ok(info) => info,
            Err(e) if is_fault_fallout(&e) => {
                sim.restart();
                continue;
            }
            Err(e) => return Err(format!("settle push_subscribe failed non-injected: {e}")),
        };

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
                tx.emit(Ping { n: WireU64(n) });
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
                tx.emit(Ping { n: WireU64(n) });
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

/// The discriminating twin for the retention-gap confirmation `drain` grew
/// this interlude (the fix for seed 0xbe2f0aa2f20f34c7: a live-delivered
/// frame whose ledger row aged out before `sync_ledger` recorded it used to
/// stall the client's drain forever). The confirmation resolves a
/// delivered-then-aged id BY ID — but it must resolve ONLY ids the client
/// actually RECEIVED, never mask a real delivery-path interior skip. Both
/// halves below face the identical aged-out ledger (real checkpoint-driven
/// GC, `event_retention: 4`); the ONLY difference is whether the still-
/// RETAINED id `floor` was delivered.
///   - honest: every id up to the tail was received live -> the below-floor
///     ids confirm, the retained ids content-verify, the client settles.
///   - skip: the retained id `floor` is never delivered -> the confirmation
///     cannot touch it (it was never in `pending`), and the independent
///     membership check `first_unaccounted` names it, so the client stays
///     RED. This is the same over-excusing failure the T6 review caught the
///     old high-water bound committing, now guarded at the new mechanism.
///     Proven load-bearing by a guard-revert (interlude report): dropping
///     the below-floor arm makes the HONEST half stop settling, while the
///     skip half stays RED either way — the confirmation resolves
///     received-and-aged ids and nothing else.
#[test]
fn the_below_floor_confirm_resolves_only_received_frames() {
    let sim = Sim::with_faults(0x2EB2_6A9F, FaultConfig::none());
    let mut app = realtime_app(&sim).unwrap();
    // Enough emits that checkpoint-driven GC (event_retention: 4,
    // checkpoint_every: 2) ages the oldest rows out while keeping the newest
    // — a real floor > 0 to confirm delivered-then-aged ids against.
    for n in 0..8u64 {
        app.db_for_tests()
            .transact(|tx| {
                tx.emit(Ping { n: WireU64(n) });
                Ok::<(), std::io::Error>(())
            })
            .unwrap();
    }
    let floor = app.db_for_tests().event_floor().unwrap();
    let next = app.db_for_tests().next_event_id().unwrap();
    assert!(
        floor > 0 && floor < next,
        "test needs GC to have aged some rows out but kept others: floor={floor}, next={next}"
    );

    // The delivered frame for an id — content identical to what the store
    // materialized (each emit's `Ping.n` is its own event id, ids dense).
    let frame = |id: u64| -> (u64, &'static str, String) {
        (
            id,
            "ping",
            serde_json::to_string(&Ping { n: WireU64(id) }).expect("Ping serializes"),
        )
    };

    // ---- honest: every id in [0, next) was received live ----
    let mut honest = Client::new(PING_ONLY, None);
    for id in 0..next {
        honest.pending.push_back(frame(id));
    }
    honest.sync_and_drain(app.db_for_tests()).unwrap();
    assert!(
        honest.settled(),
        "delivered-then-aged ids must confirm and the client settle: {}",
        honest.unsettled_report()
    );
    assert!(
        honest.verified.iter().any(|&id| id < floor),
        "the below-floor confirmation must actually have fired here (an aged-out, \
         never-in-ledger id was confirmed) — otherwise this test proves nothing"
    );

    // ---- skip: the still-RETAINED id `floor` is never delivered ----
    let skip = floor;
    let mut skipped = Client::new(PING_ONLY, None);
    for id in 0..next {
        if id == skip {
            continue;
        }
        skipped.pending.push_back(frame(id));
    }
    skipped.sync_and_drain(app.db_for_tests()).unwrap();
    assert!(
        !skipped.settled(),
        "a never-received still-retained id must keep the client unsettled — the retention \
         confirmation must not mask a real delivery-path interior skip"
    );
    assert_eq!(
        skipped.first_unaccounted(),
        Some(skip),
        "the skipped id must be named precisely, not merely detected"
    );
    assert!(
        skipped.unsettled_report().contains(&skip.to_string()),
        "the failure report must name the missing id: {}",
        skipped.unsettled_report()
    );
}

/// The discriminating twin for the durable-corruption divergence arm (T6
/// interlude investigation): the exact same 409 shape, but in a
/// corruption-free universe the gate must stay OFF — `Client::absorb` still
/// hard-fails, proving the excuse can never mask a real (non-corruption)
/// loss. `FaultConfig::none()` never touches `bit_flip`/`misdirected_write`,
/// so `durable_corruption_fired` reads `false` and the new arm's guard
/// doesn't match — the response falls through to the same catch-all `Err`
/// every unrecognized subscribe status hits.
#[test]
fn the_harness_refuses_a_diverged_409_without_durable_corruption() {
    let sim = Sim::with_faults(0x2EB2_C0DE, FaultConfig::none());
    let app = App::open(
        Box::new(sim.disk()),
        Schema::new(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )
    .unwrap();
    assert!(
        !durable_corruption_fired(&sim),
        "a fresh, fault-free sim must not report durable corruption"
    );

    let mut client = Client::new(PING_ONLY, None);
    let body = br#"{"error":"cursor 0.2 diverged from this node's timeline (next event id is 0) -- re-sync current state through reads, then reconnect without a cursor"}"#.to_vec();
    let sinks: SinkHandles = Arc::new(Mutex::new(Vec::new()));
    let err = client
        .absorb(&sim, app.db(), &[(409, body)], Some((true, 0)), &sinks)
        .expect_err("the corruption-gated excuse must not fire without durable corruption");
    assert!(err.contains("diverged"), "{err}");
}

/// Group-commit T6 regression: a subscribe arriving in the same instant as an
/// emit must still receive that emit.
///
/// The kernel's request arm drains many requests into one group, but a
/// subscribe's cursor is resolved AFTER the commit — `validate_subscribe`
/// needs `&Db`, which the open group holds. So a sibling staged behind a
/// subscribe would have its events committed before that resolution, the tail
/// would start past them, and the subscriber would silently miss events it
/// must receive. A gap: precisely what this file's membership oracle forbids.
/// The arm therefore ends a batch at a subscribe.
///
/// Found the honest way — by an existing test going red — but by
/// `tail_encode_error_is_whole_node_fatal`, whose name promises nothing about
/// batch ordering, so the property gets its own pin here rather than living
/// on as a side effect of that one.
#[test]
fn a_subscribe_still_sees_an_emit_that_arrives_with_it() {
    let sim = Sim::with_faults(0x2EB2_B471, FaultConfig::none());
    let app = realtime_app(&sim).unwrap();
    let mut kernel = Kernel::new(app, Box::new(sim.clock())).unwrap();

    let mut source = SimSource::new(Box::new(sim.clock()), sim.now().as_nanos() + 10 * SEC);
    let t = sim.now().as_nanos();
    // Same instant: both are ready in the same step, so the arm would drain
    // them into one batch if the subscribe did not end it.
    source.push_at(t, Request::new(Method::Get, "/feed-ping"));
    source.push_at(
        t,
        Request::new(Method::Post, "/ping").json(&EmitBody {
            n: WireU64(7),
            key: None,
        }),
    );
    let sinks = source.sinks();

    // Run until the script is exhausted and the tail has been serviced.
    for _ in 0..8 {
        match kernel.step(&mut source) {
            Ok(_) => {}
            Err(e) => panic!("no step may fail here: {e}"),
        }
    }

    let handles = sinks.lock().expect("sinks lock");
    assert_eq!(handles.len(), 1, "exactly one subscribe opened a sink");
    let frames = handles[0].frames();
    // Exact, not merely non-empty (T6 review M-1): `!frames.is_empty()` would
    // also pass on a `charpente.gap` terminal frame or any wrong event, and
    // "could this be green while the property is false" is the standard here.
    assert_eq!(
        frames.len(),
        1,
        "exactly the one emit that arrived with the subscribe — an EMPTY tail \
         means its cursor resolved PAST the sibling's event, which is a silent \
         gap: {:?}",
        frames.iter().map(|f| (f.name, &f.json)).collect::<Vec<_>>()
    );
    assert_eq!(frames[0].name, "ping", "the Ping event, not a gap frame");
    // Exact, not `contains('7')` (review round 1, finding 4): a substring test
    // for one digit also passes on `{"n":17}`, on `{"n":7}`, and on any other
    // encoding of the field — including the one this event does NOT use.
    assert_eq!(
        frames[0].json, r#"{"n":"7"}"#,
        "carrying the emitted payload n=7"
    );
}

/// The literal bytes a subscriber receives, pinned as bytes (review round 1,
/// finding 4). Task 6 moved `Ping.n` from a bare `u64` to `WireU64`, which
/// changed this document: the frame body is now `{"n":"7"}`, a decimal STRING,
/// not `{"n":7}`. That is the most product-visible thing task 6 changed, and
/// every other assertion around it holds only by COMPOSITION —
/// `serde_json::to_string(&Ping { .. })` on both sides agrees with itself no
/// matter what the encoding is, so all of them would stay green if the wire
/// form silently reverted. This one asserts the delivered frame directly, at
/// the boundary a browser's `JSON.parse` reads.
#[test]
fn a_delivered_frame_carries_its_64_bit_field_as_a_decimal_string() {
    let sim = Sim::with_faults(0x2EB2_D511, FaultConfig::none());
    let app = realtime_app(&sim).unwrap();
    let mut kernel = Kernel::new(app, Box::new(sim.clock())).unwrap();

    let mut source = SimSource::new(Box::new(sim.clock()), sim.now().as_nanos() + 10 * SEC);
    let t = sim.now().as_nanos();
    source.push_at(t, Request::new(Method::Get, "/feed-ping"));
    source.push_at(
        t,
        Request::new(Method::Post, "/ping").json(&EmitBody {
            // Above 2^53: the exact value a JSON number would have corrupted,
            // which is why the carrier exists at all.
            n: WireU64(9_007_199_254_740_993),
            key: None,
        }),
    );
    let sinks = source.sinks();
    for _ in 0..8 {
        kernel.step(&mut source).expect("no step may fail here");
    }

    let handles = sinks.lock().expect("sinks lock");
    let frames = handles[0].frames();
    assert_eq!(frames.len(), 1, "exactly one delivered frame");
    assert_eq!(frames[0].name, "ping");
    assert_eq!(
        frames[0].json, r#"{"n":"9007199254740993"}"#,
        "the frame body is the decimal STRING form — a JSON number here would \
         reach a JavaScript client as 9007199254740992"
    );
}

/// The encode-error contract (spec §5): `deliver_unit` (kernel/tail.rs)
/// bubbles a `SubEntry::encode_json` failure with `?`, which bubbles again at
/// the `Ready::Tail` call site (kernel/mod.rs) out of `Kernel::step`/`run` —
/// whole-node-fatal, not a per-tail drop. No `#[route(SSE ...)]` handler can
/// produce a failing encoder (the macro only emits `Subscription::of::<Ev>()`'s
/// real, working one), so this subscribes through a HAND-WRITTEN `RouteDef` —
/// no macro — whose glue builds a `Subscription` with a `SubEntry` closure
/// that always errors.
struct FailingFeed;

fn failing_feed_glue(
    _tx: &mut Tx,
    _ctx: &Ctx,
    _captures: &[String],
    cursor: Option<Cursor>,
) -> Result<Subscription, ApiError> {
    Ok(Subscription {
        entries: vec![SubEntry {
            name: "note_added",
            encode_json: |_: &[u8]| -> io::Result<String> {
                Err(io::Error::other("encode exploded"))
            },
        }],
        key: None,
        cursor,
    })
}

impl RouteDef for FailingFeed {
    fn route() -> Route {
        // Untyped glue, not a described route: a hand-written `RouteDef`
        // with no typed handler (Amendment D, task-5-brief.md).
        Route {
            method: Method::Get,
            pattern: "/feed-failing",
            auth: false,
            name: "FailingFeed",
            glue: RouteGlue::Sse(failing_feed_glue),
            params: &[],
            request: None,
            response: || TypeSchema::Null,
            error: || <ApiError as Wire>::reference(),
            register_types: |_| {},
            emits: &[],
        }
    }
}

#[event]
struct NoteAdded {
    n: WireU64,
}

#[derive(WireDerive, Serialize, Deserialize)]
struct NoteBody {
    n: WireU64,
}

#[route(POST "/note")]
fn emit_note(tx: &mut Tx, body: NoteBody) -> Result<(), ApiError> {
    tx.emit(NoteAdded { n: body.n });
    Ok(())
}

#[test]
fn tail_encode_error_is_whole_node_fatal() {
    let sim = Sim::with_faults(0x2EB2_FA11, FaultConfig::none());
    let app = App::open(
        Box::new(sim.disk()),
        Schema::new(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )
    .unwrap()
    .mount(vec![FailingFeed::route()])
    .mount(routes![emit_note]);
    let mut kernel = Kernel::new(app, Box::new(sim.clock())).unwrap();

    let mut source = SimSource::new(Box::new(sim.clock()), sim.now().as_nanos() + 10 * SEC);
    let t = sim.now().as_nanos();
    source.push_at(t, Request::new(Method::Get, "/feed-failing"));
    source.push_at(
        t,
        Request::new(Method::Post, "/note").json(&NoteBody { n: WireU64(1) }),
    );

    // Step 1: the subscribe registers the tail (no events exist yet, so it is
    // not ready this step).
    kernel
        .step(&mut source)
        .expect("the subscribe request must not itself fail");
    // Step 2: the emit request commits the event the tail is waiting for.
    kernel
        .step(&mut source)
        .expect("the emit request must not itself fail");
    // Step 3: nothing is left in the script, so the only ready source is the
    // now-behind tail -- deliver_unit's encode_json call fails, and that
    // failure must bubble all the way out of `step` (`Step` has no `Debug`,
    // so this can't go through `expect_err`).
    let err = match kernel.step(&mut source) {
        Ok(_) => panic!("an encode error must bubble out of Kernel::step, not be absorbed"),
        Err(e) => e,
    };
    assert!(err.to_string().contains("encode exploded"), "{err}");
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
        peer_secret: None,
        strict_witnesses: false,
    }
}

fn rt_follower_cfg(listen: &str, primary: &str) -> ReplConfig {
    ReplConfig {
        listen: Some(listen.to_string()),
        initial_role: Role::Follower {
            primary: primary.to_string(),
        },
        peer_secret: None,
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
            ..AppConfig::default()
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
            return Err(format!(
                "{INCONCLUSIVE}: {} torn witness file — the operator restore path is the fix: {e}",
                h.name()
            ));
        }
        Err(e)
            if e.to_string()
                .contains("role file failed its integrity check") =>
        {
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

/// The witness boot gate's 503 (M2 sub-project 0b task 5): this workload
/// restarts its primary under fault injection, and a primary that comes back
/// with a recorded witness serves reads but withholds writes until that
/// witness answers. An emit refused there did not happen — no event id was
/// minted and nothing was acked — so it is the same "no write, no bug"
/// outcome the fault-fallout arms already tolerate. Keyed on the 503 body's
/// own leading phrase (`http/app.rs`, pinned byte-exact in `http.rs`).
fn rt_witness_gated(body: &[u8]) -> bool {
    String::from_utf8_lossy(body).contains("this node is not accepting writes yet")
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
                let Some(kernel) = node.kernel.as_ref() else {
                    return Err(format!("{INCONCLUSIVE}: target has no kernel"));
                };
                // A latched store makes this read fail (M2 sub-project 0a).
                // FIX ROUND 2, I-1 (Noah decision B): RECOVER the node and
                // keep the verdict. Returning an inconclusive here would
                // propagate through this function's callers into
                // `finish_rt_universe`, which converts it to `Ok(())` — so
                // the phase-1 and phase-2 `client.settled()` checks would be
                // skipped and a client that had already missed an event would
                // be excused rather than reported. `kstep_rt_node` handles
                // this same fault class the same way, and it genuinely
                // recovers: a restart mints a fresh redb handle and a fresh
                // latch flag. The attempt is spent, exactly as if the node
                // had died mid-round. Seed 0xdc154f8c66271f15 is the
                // reproduction of the fault itself.
                match push_subscribe(sim, kernel, &mut node.source, path, client.cursor, now) {
                    Ok(info) => info,
                    Err(e) if is_fault_fallout(&e) => {
                        recover_rt_node(sim, node)?;
                        client.need_subscribe = true;
                        client.current_handle = None;
                        continue;
                    }
                    Err(e) => return Err(e),
                }
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
        if status != 200
            && !is_fault_fallout(&String::from_utf8_lossy(&resp_body))
            && !rt_witness_gated(&resp_body)
        {
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
        } else if !is_fault_fallout(&String::from_utf8_lossy(&resp_body))
            && !rt_witness_gated(&resp_body)
        {
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
    let reply = CtrlConn::open(sim, "f:1", &Msg::CtrlPromote { secret: None })
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

// ==================== T8a Step 4: follower id identity under a BATCHED primary

/// T8a Step 4 (spec §9.5, task-8-preflight.md C-6): follower id identity
/// under a BATCHED primary. After T6/T7 a "single-node run of the same
/// workload" is batched too — one commit path, hard rule 2 forbids a
/// second — so spec §6's actual claim is primary-vs-follower, not
/// batched-vs-unbatched. This is the only rig in the repo with real
/// `Kernel`s on both hosts AND events (`RNode`/`spawn_rt_node`/
/// `rt_kernel_round`, this file's own multi-node section above);
/// `replication.rs`'s `Node` is `Db`-level with no kernel (`:157`), so its
/// primary cannot batch at all.
///
/// `next_job_id` continuity under replication was already proven elsewhere
/// (the seeder work); `next_event_id` is compared NOWHERE ELSE in the
/// repo (C-6) — that is the genuinely new half this test exists for,
/// alongside a full `scan_raw` comparison (the `converged_read` shape,
/// `replication.rs:601`) over every table.
///
/// Deterministic, not a swarm: `FaultConfig::none()`, one fixed seed. The
/// batch-membership guard loops `step()` up to a small budget rather than
/// asserting on the very first call, because a `Ready::Repl` round can
/// legitimately win arbitration before `Ready::Request` gets its turn —
/// `Kernel::step`'s top-of-step `source.poll()` still only pulls the FIRST
/// pending request either way, so whichever step actually services
/// `Ready::Request` first is guaranteed to drain all four (they are all
/// already ready at the same instant), which is what the loop confirms
/// rather than assumes.
#[test]
fn follower_ids_match_a_batched_primarys() {
    let sim = Sim::with_faults(0x60C1_0004, FaultConfig::none());
    let db_cfg = multi_node_db_cfg();
    let mut p = spawn_rt_node(&sim, "p", rt_primary_cfg("p:1"), db_cfg.clone(), RT_HORIZON)
        .expect("primary spawns under FaultConfig::none()");
    let mut f = spawn_rt_node(&sim, "f", rt_follower_cfg("f:1", "p:1"), db_cfg, RT_HORIZON)
        .expect("follower spawns under FaultConfig::none()");

    // A few quiet rounds so the follower dials and Hellos before the batch.
    for _ in 0..20 {
        if !rt_kernel_round(&sim, &mut [&mut p, &mut f]).expect("quiet rounds") {
            break;
        }
    }

    // Four requests at the SAME instant — genuinely contended, so the
    // primary's request arm must drain all four in ONE batch whenever it
    // finally runs.
    let now = sim.now().as_nanos();
    for n in 0..4u64 {
        p.source.push_at(
            now,
            Request::new(Method::Post, "/ping").json(&EmitBody {
                n: WireU64(n),
                key: None,
            }),
        );
    }
    let requests_before = p.kernel.as_ref().expect("primary alive").stats().requests;
    let mut batch_ran = false;
    for _ in 0..50 {
        kstep_rt_node(&sim, &mut p).expect("primary step under FaultConfig::none()");
        if p.kernel.as_ref().expect("primary alive").stats().requests > requests_before {
            batch_ran = true;
            break;
        }
    }
    assert!(
        batch_ran,
        "the primary's request arm never ran within budget"
    );
    assert_eq!(
        p.kernel.as_ref().expect("primary alive").stats().requests - requests_before,
        4,
        "all four same-instant emits must land in ONE batch — a primary that still serves one \
         request per step proves nothing about follower id identity under batching"
    );

    // Drive to convergence: ship, ack, apply on the follower.
    let mut converged = false;
    for _ in 0..RT_STEP_BUDGET {
        let p_next = p
            .kernel
            .as_ref()
            .expect("primary alive")
            .app()
            .db()
            .next_event_id()
            .expect("primary next_event_id");
        let f_next = f
            .kernel
            .as_ref()
            .expect("follower alive")
            .app()
            .db()
            .next_event_id()
            .expect("follower next_event_id");
        if p_next == f_next && p_next > 0 {
            converged = true;
            break;
        }
        if !rt_kernel_round(&sim, &mut [&mut p, &mut f]).expect("converge rounds") {
            break;
        }
    }
    assert!(
        converged,
        "primary and follower must converge on next_event_id within budget"
    );

    let pdb = p.kernel.as_ref().expect("primary alive").app().db();
    let fdb = f.kernel.as_ref().expect("follower alive").app().db();
    let psnap = pdb.snapshot().unwrap();
    let fsnap = fdb.snapshot().unwrap();

    // The batch must have produced FOUR events, and both sides must say so
    // in absolute terms. Comparing the two sides ALONE is not enough, and
    // that is measured, not argued: T8a review I-2 truncated `Group::commit`
    // to a single record — three of four acked writes silently dropped —
    // and this test stayed GREEN, because a one-event run satisfies both
    // `p_next > 0` and byte-equal tables. The count is what carries this
    // test past spec §7's batch-of-1 identity, which is the case it exists
    // to go beyond. Two quantities, not one: the counter can in principle
    // advance without materializing rows, so the events table is checked
    // directly too.
    assert_eq!(
        pdb.next_event_id().unwrap(),
        4,
        "all four requests' emits must be in the batch — one event would mean a degenerate \
         batch and an oracle that proves nothing about batching"
    );
    assert_eq!(
        psnap.scan_raw(EventRow::TABLE).unwrap().len(),
        4,
        "four emits must materialize four event rows on the primary"
    );
    assert_eq!(
        fdb.next_event_id().unwrap(),
        4,
        "the follower must land on the SAME id frontier from its own replay — next_event_id is \
         compared NOWHERE else in the repo before this test (T8a pre-flight C-6)"
    );
    // `next_job_id` is deliberately NOT asserted here: `open_rt_kernel`
    // mounts no jobs and this test enqueues nothing, so both sides are 0
    // and the comparison could not fail (T8a review I-3). Wiring jobs into
    // that shared helper to make it non-vacuous would perturb every
    // multi-node test in this file; job-id continuity under replication is
    // proven by the seeder work instead.
    for table in pdb.table_names() {
        let pr = psnap.scan_raw(table).unwrap();
        let fr = fsnap.scan_raw(table).unwrap();
        assert_eq!(
            pr, fr,
            "table {table} diverged between the batched primary and the follower's replay"
        );
    }
}

/// Task 7's drift oracle on the app that actually delivers events: three
/// stream routes, two distinct declared sets, and one of them keyed — so an
/// `emits(...)` that quietly followed `.key(...)`, or that collapsed every
/// feed to the same set, shows up here rather than in the describe fixture's
/// single feed.
///
/// The declaration is the THIRD place these names are written in this file:
/// `Subscription::of/and` in each handler, `PING_ONLY`/`BOTH` in `mode_for`
/// (which the flagship workload uses as data — it subscribes by path and
/// checks the frames it receives against those names), and now `emits(...)`.
/// Task 7 deletes neither of the first two, so the cross-check at the end is
/// what keeps a future edit from moving one without the others.
#[test]
fn every_stream_routes_declared_emits_matches_its_subscription() {
    let sim = Sim::with_faults(0x2EB7_0007, FaultConfig::none());
    let mut app = realtime_app(&sim).expect("the realtime fixture app must open");
    // Ruling 46: `emits` elements now carry (event name, payload name). The
    // oracle compares the event half — the only half a `Subscription` holds
    // — and the payload half rides along in `declared_pairs` below, so
    // widening the type did not quietly drop it from this file's coverage.
    let declared: Vec<(&'static str, Vec<&'static str>)> = app
        .describe()
        .routes
        .iter()
        .filter(|r| r.stream)
        .map(|r| (r.name, r.emits.iter().map(|e| e.event).collect()))
        .collect();
    // The floor for the loop below (an empty one would pass vacuously), and
    // the pin on registration order, which is the contract's route order.
    assert_eq!(
        declared,
        vec![
            ("feed_both", vec!["ping", "pong"]),
            ("feed_ping", vec!["ping"]),
            ("feed_keyed", vec!["ping", "pong"]),
        ]
    );
    for (name, emits) in declared {
        let actual = app
            .subscription_event_names(name)
            .expect("a stream route must expose its subscription's event names");
        assert_eq!(
            emits, actual,
            "route `{name}` declares emits{emits:?} but its Subscription names {actual:?}"
        );
    }
    for seed in 0..3u64 {
        let (path, names, _) = mode_for(seed);
        let emits: Vec<&str> = app
            .describe()
            .routes
            .iter()
            .find(|r| r.pattern == path)
            .map(|r| r.emits.iter().map(|e| e.event).collect())
            .expect("mode_for names a mounted route");
        assert_eq!(
            emits,
            names.to_vec(),
            "mode_for(\"{path}\") and that route's emits(...) must name the same events"
        );
    }

    // The payload half of the pair (ruling 46), which the loops above do not
    // touch: `Ping`/`Pong` are the `Wire::WIRE_NAME`s a generated TypeScript
    // union writes, and they must line up with the event names one-for-one
    // in declaration order. Both names come off the same type in the macro,
    // so a mismatch here means the pairing itself broke — the one failure
    // the projected-to-`event` comparisons above are blind to.
    let declared_pairs: Vec<(&'static str, Vec<EmittedEvent>)> = app
        .describe()
        .routes
        .iter()
        .filter(|r| r.stream)
        .map(|r| (r.name, r.emits.clone()))
        .collect();
    assert_eq!(
        declared_pairs,
        vec![
            (
                "feed_both",
                vec![
                    EmittedEvent {
                        event: "ping",
                        payload: "Ping"
                    },
                    EmittedEvent {
                        event: "pong",
                        payload: "Pong"
                    },
                ]
            ),
            (
                "feed_ping",
                vec![EmittedEvent {
                    event: "ping",
                    payload: "Ping"
                }]
            ),
            (
                "feed_keyed",
                vec![
                    EmittedEvent {
                        event: "ping",
                        payload: "Ping"
                    },
                    EmittedEvent {
                        event: "pong",
                        payload: "Pong"
                    },
                ]
            ),
        ]
    );
}
