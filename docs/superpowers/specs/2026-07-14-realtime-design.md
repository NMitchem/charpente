# M1 sub-project 3 — realtime SSE with exact resume: design

Date: 2026-07-14. Noah-approved scope and architecture (this session). Third of M1's
six sub-projects (decomposition in 2026-07-07-wal-segment-management-design.md).

## Context

`tx.emit(event)` has committed typed events into the log since M0 step 4 — call-ordered
inside the effects record, applied to no one. DESIGN §6's realtime story ("SSE/WebSocket
subscribers tail the log with a cursor; resume-from-cursor is exact, so reconnects have
no silent gap") and PLAN M1's exit criterion ("a reconnecting realtime client provably
misses no events") are the contract this sub-project discharges. Replication
(sub-project 2) matters here twice: followers apply the same log, so anything
materialized at apply is byte-identical on them, and the below-horizon/re-seed error
shape is the honesty precedent for cursors that age out.

## Scope

In: the SSE event tail (subscribe route flavor, kernel delivery, resume), the
materialized `charpente.events` table with count-based retention, `tx.emit_keyed`, the
`#[event]` macro + `events![]` registry, follower tails, the sim story (sink model +
`subscriber_stall` axis + properties), GUIDE/DESIGN updates.

Out (explicit non-goals): WebSocket transport; TS client codegen (its own later
project, over routes + events together); payload predicates beyond the key filter;
any server-side per-subscriber durable state — the cursor lives client-side, which is
what keeps subscriptions stateless and the resume promise clean; a transactional
wall-time source (the demo FINDINGS gap — retention is count-based partly because of
it, and the gap stays open).

## 1. The events table and the cursor

- The emit effect gains an optional key: `0x03 [event][payload][key: Option<String>]`
  (DESIGN §2 updated; breaking log-format change, acceptable pre-release — dev dirs
  re-init, no migration path shipped). `tx.emit(ev)` = key `None`;
  `tx.emit_keyed(key, ev)` mirrors `enqueue_keyed`. The key is the tenant/entity
  scoping hook (e.g. an org id); the platform treats it as an opaque exact-match
  filter, never parses it.
- Apply materializes each emit into the built-in `charpente.events` table:
  `EventRow { id: u64, seq: u64, name: String, key: Option<String>, payload: Vec<u8> }`.
  `id` comes from a `next_event_id` meta-counter bumped at apply in call order — the
  `next_job_id` pattern exactly, so ids are strictly monotonic, order-isomorphic to
  log position, deterministic under replay, and byte-identical on followers.
- **The cursor is the event id, wire-encoded with its seq: `<id>.<seq>`.** Resume
  from cursor `(c, s)` = deliver every matching event with `id > c`. Exactness is by
  construction: ids are assigned at apply in log order. The seq half exists for one
  reason — failover honesty. Two diverged timelines (an old primary's unshipped tail
  vs a promoted follower's new writes) can mint the same event ids for different
  events; a bare id cannot detect that it came from the other timeline. Resume
  validation (§3) reads the retained row at `id == c` and requires its `seq == s`;
  a mismatch is the named divergence error in §5, never a silent misattribution —
  the same shape as replication's `Diverged` (both seqs, both CRCs).
- Retention: `DbConfig::event_retention: u64` (default 65_536, count-based). GC runs
  at the existing checkpoint cadence: delete rows with
  `id <= next_event_id - 1 - event_retention`. The retention floor (smallest
  retained id, or "empty") is derivable from the table; no new meta key.
- Payloads stay `Codec` bytes in the table (the log's format). JSON encoding happens
  at delivery, through the `events![]` registry — never stored twice, never encoded
  when no subscriber is listening.

## 2. The subscription surface

- New route flavor, explicit in the macro: `#[route(SSE "/path")]`. The handler is a
  normal transaction — `Ctx` session auth like any route — with signature
  `fn(tx: &mut Tx, [args,] ctx: &Ctx, cursor: Option<u64>) -> Result<Subscription, ApiError>`
  (the macro enforces the convention; its errors name the contract, the site, and the
  fix, per hard rule 5). Subscribe handlers are read-only in practice; nothing new
  gates them on followers — a follower serves tails through the existing read path.
- The platform extracts the client cursor before the handler runs: the
  `Last-Event-ID` header wins (browser-native SSE resume), else a `cursor` query
  parameter, else `None`. `None` means "from now" — only future events.
- `Subscription` is a typed builder: `Subscription::of::<Ev>().and::<Ev2>()` (event
  types, compile-checked — names never appear as strings in app code),
  `.key(String)` (optional exact-match filter), `.cursor(cursor)` (the handler passes
  through — or clamps — what the platform extracted). The handler owns access
  control: it decides which types and which key a caller may tail, using its own
  session/row logic.

## 3. Kernel delivery — one path, no modes

- A subscribe route's `Ok(Subscription)` becomes a new kernel response kind; the
  kernel registers `Tail { subscription, sink, next_id }` (`next_id = cursor + 1`, or
  `next_event_id` for "from now"). Registration validates the cursor: below the
  retention floor → the floor error in §5; `id >= next_event_id` (ahead of this
  node's timeline) or the retained row at `id` has a different `seq` (cross-timeline
  after a failover) → the divergence error in §5. Either way no tail is registered;
  a validated tail's first delivered event is exactly `id`'s successor.
- **Delivery is always a bounded table read.** A tail has work whenever
  `next_id < next_event_id`. One unit of work: read up to `TAIL_BATCH_EVENTS` rows
  from `charpente.events` starting at `next_id`, filter by the subscription's
  names/key, JSON-encode matches via the registry, push frames to the sink, advance
  `next_id` past everything read (matching or not). There is no separate
  push-on-commit path and no live/catch-up mode — a commit simply advances
  `next_event_id`, which makes tails ready; "live" is just "zero behind." One code
  path serves both, which is the same-shape rule applied to delivery.
- The ready set gains a Tail source alongside requests/jobs/cron/repl, under the
  existing scheduling rule (one entropy draw only when contended). Tails are serviced
  in registry order within the arm; a single tail's unit of work is bounded, so a
  deep resume never starves the loop.
- The sink is driver-supplied:
  `trait EventSink: Send { fn send(&self, frame: EventFrame) -> SinkStatus }`,
  `SinkStatus::{Ok, Full, Closed}`, `EventFrame { id: u64, name: &'static str,
  json: String }`. Prod: a bounded channel (`EVENT_SINK_BUFFER` frames) drained by
  the hyper SSE writer. Sim: an in-memory buffer the workload reads, with fullness
  scripted by the `subscriber_stall` fault axis.
- **Kernel policy on `Full` or `Closed`: drop the tail** (deterministic, immediate).
  A slow or dead consumer costs its own connection, never kernel progress; the
  client's cursor resume is the recovery. Reaping is otherwise lazy — an idle tail
  whose events never match holds registry memory until its next send attempt; tails
  pin nothing (retention GC is count-based and independent of subscribers).
- A tail that falls behind retention mid-stream (its `next_id` drops below the
  floor before it catches up) gets the terminal gap frame (§5), then the sink is
  closed and the tail dropped. The check is part of the tail's unit of work, so it
  is deterministic in sim.

## 4. The `#[event]` macro and registry

- `#[event]` on a struct derives `Event` (`NAME` = the snake_case type name),
  `Codec` (the log/table format), and `serde::Serialize` (the wire format), matching
  `#[table]`/`#[job]`'s emit style (`::charpente::` paths, usable in charpente's own
  src via `extern crate self`).
- There is no runtime event registry in this sub-project. `Subscription::of::<Ev>()`
  captures `(Ev::NAME, fn(&[u8]) -> Result<String, _>)` — the decode-`Codec`,
  encode-JSON function — statically at the subscribe site, so the delivery path
  already holds the encoder for every name it can match: "unregistered event at
  delivery" is inexpressible, not an error case. An `events![]` registry becomes
  worthwhile when `charpente describe`/TS codegen need enumeration (sub-project 6);
  building it now is speculation.
- Replaces the hand `impl Event` blocks in GUIDE and `examples/invoicing`.

## 5. Error surface (product surface, all pinned by tests against text)

- **Cursor below the retention floor at subscribe:** the request fails (409) naming
  the floor, the client's cursor, and the fix — re-sync current state through normal
  reads, then reconnect without a cursor. Mirrors the demote/re-seed shape: the
  platform never silently skips the gap.
- **Cursor diverged at subscribe (409):** the cursor is ahead of this node's
  timeline, or the retained row at its id carries a different seq — the client's
  history came from another timeline (a failed-over primary). Names both sides (the
  cursor's id.seq, the node's row seq or next id) and the same re-sync fix. Never a
  silent misattribution.
- **Mid-stream overrun:** terminal SSE event `event: charpente.gap` carrying the same
  three facts (floor, tail position, the re-sync fix), then close. A client that
  sees `charpente.gap` knows exactly what it missed and why.
- **Sink overflow / client gone:** connection closes; nothing is sent (there is no
  one to tell). The client that reconnects with `Last-Event-ID` resumes exactly —
  overflow is invisible to a client that retries, which is the point.

## 6. The HTTP edge (serve.rs)

- A subscribe response maps to `Content-Type: text/event-stream`; the edge drains
  the sink channel and writes `id:`/`event:`/`data:` frames (`id` = event id — this
  is what makes `Last-Event-ID` resume browser-native). A comment heartbeat (`:hb`)
  goes out on an edge timer when idle — pure keepalive, no semantics, allowed to be
  nondeterministic exactly like response-write timing today.
- Client disconnect surfaces as `Closed` on the kernel's next send attempt; the edge
  never signals into the kernel directly. hyper/tokio stay confined to serve.rs
  under the existing deny-list discipline.

## 7. The sim story

- `SimSink`: the in-memory sink; the workload reads frames and drives reconnects.
  `subscriber_stall` joins the fault matrix as a field-keyed sub-stream axis (the
  frozen swarm draw order gains no draws; new field defaults to 0.0 with zero draws
  when 0.0 — the sub-project-1/2 pattern, pinned the same way).
- **Flagship property (the PLAN exit criterion):** under the full fault matrix plus
  conn drops, stalls, crashes, and restarts, a client that always reconnects with
  its last received id ends with received ≡ emitted — no gap, no duplicate,
  byte-exact — against an emitted-ledger oracle tracked in the workload. The oracle
  is a consistency oracle: never excused by `durable_corruption_fired`.
- **Multi-node property:** subscribe on a follower; events emitted on the primary
  arrive through the shipped log; kill the primary, promote, reconnect the client
  (with its cursor) to the promoted node — exactness holds across the failover for
  everything the failover kept (the DECISIONS §6 bounded-loss window applies to
  events exactly as it does to state, and the oracle accounts for it). A client
  whose cursor crosses timelines (its events died with the old primary) must
  receive the divergence error, never wrong events — the workload drives that
  reconnect deliberately and the `require:` keys prove both paths fired.
- **Meta-test:** a naive-victim resume that delivers `id == cursor` again
  (off-by-one) must be caught by the harness by name — the discriminating-twin
  pattern from sub-projects 1–2.
- Coverage: `require:` keys for the paths that must fire (a resume with non-empty
  backlog, a `charpente.gap`, a stall-induced drop), so no property passes vacuously
  — the property-8 lesson from the replication close-out.
- Determinism check: frame traces identical for the same seed. Error-text tests for
  every §5 surface. Loopback smoke: real HTTP/1.1 SSE bytes end-to-end (subscribe,
  receive, disconnect, resume with `Last-Event-ID`).

## 8. What changes where

- `charpente-core`: emit-with-key in the effects codec (DESIGN §2); `EventRow` +
  `charpente.events` + `next_event_id` at apply; retention GC at checkpoint;
  `Tx::emit_keyed`; cursor-floor read surface for the kernel.
- `charpente-macros`: `#[event]`.
- `charpente` (kernel): the Tail set, the Tail ready-source arm, `EventSink`,
  registration/validation (floor + divergence), delivery units, drop policy.
- `charpente` (http/serve): the SSE route flavor, cursor extraction, `Subscription`,
  the event-stream edge writer + heartbeat.
- `charpente-sim`: `SimSink`, `subscriber_stall` axis, coverage keys.
- Docs: DESIGN §2 (emit format) + §6 (realtime, from promise to mechanism); GUIDE
  gains the realtime section (compile-gated) and drops the hand `impl Event`;
  invoicing migrates to `#[event]` (a separate demo-app commit, as before).

Constants (deterministic, no entropy): `TAIL_BATCH_EVENTS: usize = 64`;
`EVENT_SINK_BUFFER: usize = 256`; `DbConfig::event_retention: u64 = 65_536`.
Edge-only, non-semantic: SSE heartbeat interval 15s.

## 9. Evidence bar

Every task lands with its suite green over fresh seeds; sim-found bugs land with
their seed pinned. Close-out sweep: fresh swarm batches ×2 for the realtime suite and
the four core suites; the pinned crash-recovery and kernel seeds replay; invoicing
1024-universe batch; full workspace + clippy + fmt; a recorded loopback demo —
subscribe with curl -N, emit through real writes, kill the connection, resume with
`Last-Event-ID`, and show byte-exact continuity plus the `charpente.gap` text when
resuming from below the floor.
