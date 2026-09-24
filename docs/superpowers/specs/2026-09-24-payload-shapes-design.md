# Payload shapes — design

**Date:** 2026-09-24. **Status:** approved in chat (Noah), spec committed for the plan.
**Predecessor:** M3 sub-project 3's verdict (attempt 1, signed PASS by Noah 2026-09-24,
option "PASS, fix before publish"; record `d2c3806`,
`docs/experiments/2026-09-statuspage/attempt-1/`). Of the two platform fixes the 0.1.0
publish now waits on, this is the first: review Critical **C-1**. The second (a job cannot
open a tenant scope as GUIDE §16 documents; `authorize` sees neither operation nor caller
kind) gets its own spec.

## Context

The statuspage builder inserted `severity: Severity` into the `#[event] IncidentOpened` in
the same commit that bumped `Incident` to version 2. The table migration worked. The
event did not, because nothing guards an event: `charpente.events` rows are durable
`Codec` bytes (65,536 retained by default, `charpente-core/src/db.rs:76`), and delivery
decodes each one with whatever struct the running binary has (`ev_json`,
`charpente/src/http/mod.rs:257`). An anonymous `GET` with a pre-upgrade `Last-Event-ID`
made the v1 row decode `severity` from the `maintenance` byte, run out of input, and stop
the kernel. The kernel stopped again after every restart, until retention dropped the row
(`charpente/src/kernel/tail.rs:99-104`). Probe: `attempt-1/probes/upgrade.rs`.

The crash was the lucky outcome. `ev_json` never checks for trailing bytes, so a v2 that
drops a trailing field, or that swaps two fields of the same type, decodes v1 rows
*successfully* and delivers wrong JSON with no error at all.

`#[job]` payloads belong to the same class in a milder form. A Pending job whose payload
no longer decodes is retried and then dead-lettered with a named error; it does not stop
the node. A reshape that still parses, though, misdecodes silently, and the only guard is
a sentence in GUIDE ("don't reorder or remove a job payload's fields once jobs of that
type may exist Pending").

None of this is an accident of implementation. The migrations spec
(`2026-07-16-migrations-design.md`, "Out") deferred `#[job]`/`#[event]` payload evolution
by name as "a follow-up, not free". It also rejected versioned per-row decode twice (it
would "carry multi-version decode indefinitely") in favour of eager rewrite, under the
organizing rule **never decode under a foreign version**. This spec carries that rule to
the two payload kinds the table mechanism never covered.

A third defect sits on the harm path. When the kernel stops, whether on a platform IO
failure or on any named startup refusal (table drift, the witness gate), `serve.rs:45-62`
prints one line and ends the kernel thread. `launch` parks forever, and the HTTP edge
answers every request 500 "the kernel thread is gone". The process looks alive to its
supervisor, so "restart to recover" never happens by itself. The statuspage run recorded
this as an unclaimed platform finding (`docs/LEDGER.md`, 2026-09-24). No DECISIONS entry
or spec chose it; `crates/edge-harness/tests/failover.rs` Phase B pins it as "what an
operator actually sees".

Measured against the manifesto: the framework is the reviewer (§6). A guarantee must say a
named *no* rather than a crash or silence (§3). No refusal gets an escape hatch (§8). Every
new path is executed under fault with a seed (§9).

## §1 Decisions already made

- **2026-09-24 (Noah, verdict ruling):** before the 0.1.0 publish, fix C-1: "#[event]
  evolution unguarded + delivery decode whole-node-fatal + an error naming neither event
  nor fix". No attempt 2.
- **2026-09-24 (Noah, this brainstorm):**
  - **Scope is the class, not the instance:** events *and* jobs.
  - **Approach A, tag and refuse:** every payload records its shape, and nothing decodes a
    payload whose recorded shape is not the running type's. Chosen over (B) table-parity
    versions plus migrations plus a startup rewrite, and over (C) versioned decode at read
    time (already rejected twice).
  - **Nothing a subscriber triggers stops the node.** A corrupt event row and an
    unencodable event (a `NaN`) both fail their own tail, not the kernel.
  - **When the kernel stops, the process exits nonzero**, and refusals stop being labelled
    IO failures. Folded in here.
  - **Deferred to After launch, each with a trigger:** B's migrations, a deep fingerprint
    for nested types, and a commit-time "shape lock" check.

## §2 The rule and the tag

**The rule.** A payload is decoded only when its recorded shape equals the running type's
fingerprint. A payload with no recorded shape counts as foreign. This is the upgrade
barrier's stance (GUIDE §12): refuse rather than decode bytes this binary cannot prove are
its own shape. There is no override, no flag, and no exception for legacy rows.

**The fingerprint.** `#[event]` and `#[job]` compute it with the helper `#[table]` and
`#[row_shape]` already share (`charpente-macros/src/fingerprint.rs`): FNV-1a over the
`(field name, field type tokens)` sequence. `Event` and `Job` (`charpente-core/src/db.rs`)
each gain `const FINGERPRINT: u64` **with no default**. A hand-written impl must state one.
This is deliberately unlike `Row::FINGERPRINT`, whose `None` default silently opts a
hand-written table out of drift protection (GUIDE §12). The ten or so hand-written
`impl Event`/`impl Job` blocks in the test suites supply a constant.

**The log.** Two new record tags take the next free slots (`db.rs`'s tag table,
`0x08` onward):

- `0x08 [job][payload][key: Option<String>][shape: u64 le]` — shaped enqueue;
- `0x09 [event][payload][key: Option<String>][shape: u64 le]` — shaped emit.

`0x02`/`0x03` still decode and materialize with the shape unknown. A binary with this
change writes only `0x08`/`0x09`, and the encoder asserts it. This follows the precedent of
`0x05`/`0x06`/`0x07`, which scoped tables added beside the unscoped tags.

**The rows.** `EventRow` and `JobRow` gain a trailing `shape: Option<u64>`. `Some` encodes
as 8 bytes after the row's last field; `None` encodes as nothing. Rows written before this
change therefore end exactly where they always did, because the preceding field is
length-prefixed or tagged. On decode, 0 remaining bytes means `None`, 8 means `Some`, and
anything else is a named decode error. Apply fills the field from the record, so
followers, replay and the replication fingerprint oracle (which already walks both
tables) see identical rows.

**Known limitation, shared with tables.** The fingerprint hashes a field's type *tokens*.
A change inside a nested type, such as a new variant on the `Severity` enum a field holds,
leaves it unchanged. Nothing documents this today, for tables either. GUIDE §12 and the
new sections below say so, and PLAN's After launch gains a deep fingerprint (every `Codec`
type carrying its own shape constant) with its trigger.

## §3 Events: what a subscriber sees

**Delivery.** `Subscription::of::<Ev>()`'s entry gains `fingerprint: Ev::FINGERPRINT`
beside `encode_json`. Before decoding a row that matched on name and key, `deliver_unit`
compares `row.shape` with the entry's fingerprint. On a mismatch it does not decode. It
sends one terminal frame and the tail closes, the same lifecycle as `charpente.gap`:

```text
id: 5.12
event: charpente.reshaped
data: {"event":"incident_opened","id":5,"fix":"re-sync current state through reads, then reconnect without a cursor"}
```

The node stays up and other tails are untouched.

**Why the frame carries the foreign row's own id.** A client that ignores the frame, such
as a plain browser `EventSource`, reconnects with `Last-Event-ID: 5.12`.
`validate_subscribe` (`charpente/src/kernel/mod.rs`) already loads the retained row at the
cursor's id for its seq check. It now receives the subscription too. If that row's name is
one the subscription carries and its shape is not the subscription's, it refuses:

```text
cursor 5.12 sits on an "incident_opened" event written under a different shape of that
event (recorded 0x…, this binary 0x…) — this binary will not decode it; re-sync current
state through reads, then reconnect without a cursor
```

With no recorded shape, the parenthesis reads `(recorded: none — written before event
shapes were tagged; this binary 0x…)`. A client therefore cannot get past a row it cannot
decode without re-syncing, which is the gap frame's end state: it "fails loud, never
wrong", in the words of the realtime ledger (`docs/LEDGER.md:286-287`). One conservative corner is
accepted. A client whose genuine last-received event is itself a foreign row is refused
even when every later row is fine; that costs one re-sync and never a skipped event. A
cursor on a foreign row of a name the subscription does *not* carry is not refused.

**Corruption: the shape matches but decode fails.** This also covers trailing bytes,
which `ev_json` now checks. The tail gets a terminal frame with **no `id:` line**, so the
client's `Last-Event-ID` does not advance and nothing is skipped:

```text
event: charpente.error
data: {"event":"incident_opened","id":5,"error":"…"}
```

The `error` text names the event, the row id, and says the stored bytes are corrupt, not a
shape change. The plan pins its exact wording, which must also name the operator's
remedy. Each reconnect fails at the same row, the way a corrupt table row answers 500 on
every read (`http/app.rs:857-860`). The node stays up.

**An unencodable payload** (a `NaN`, a non-string map key) takes the same
`charpente.error` frame, carrying the existing `"event <name> failed JSON encoding: …"`
text. This closes the per-tail follow-up recorded at `docs/LEDGER.md:286`.
`realtime.rs::tail_encode_error_is_whole_node_fatal` is that decision's pin; it is renamed
and flipped to assert the per-tail frame, and GUIDE §11's kernel-fatal paragraph is
rewritten.

**The TS client.** The generated runtime ends a stream on `charpente.gap` today
(`charpente-tsgen/src/runtime.rs:332`). It gains `reshaped` and `error` as the other two
terminal cases, carried in its `StreamEnd` type.

**Keeping exact resume across a deploy** is taught, not built. Do not change an existing
event's fields. Add a new event type beside it, and keep the old type in code (and in any
subscription that still needs it) for as long as its rows are retained. Old rows keep
decoding under their own type. GUIDE §11 gains a "Changing an event" subsection saying
this, what a reshape costs instead, and the three pinned texts above.

## §4 Jobs: refused at startup, never at dispatch

**The check.** `Dispatcher::rebuild` (`charpente/src/kernel/dispatch.rs`) already scans
every `JobRow` whenever a node starts or becomes writable. It now compares each Pending
row of a registered job type with that type's `FINGERPRINT`, which `JobDef` gains. If any
row mismatches, the kernel does not start. The refusal gives one line per job type:

```text
3 Pending "SendReceipt" jobs (ids 12, 15, 19) were enqueued under a different shape of
SendReceipt (recorded 0x…, this binary 0x…) — this binary will not decode them; run the
previous binary until they drain, then upgrade
```

The plan fixes how many ids are listed before the line elides the rest. Draining always
ends, because every Pending job reaches Done or Dead within its retry policy. Done and
Dead rows are never decoded again and are ignored. A Pending row of an *unregistered*
name keeps today's behaviour ("job … is not registered").

**No drain needed:** add a new job type for the new shape and keep the old one registered
until its Pending rows finish. The old type keeps dispatching them, because their shape is
still its own.

**At promote.** The check runs before the promote is acknowledged, beside the schema gate
in `do_promote` (`charpente/src/repl/mod.rs:913`). A refused promote answers `CtrlErr` by
name and leaves the node a follower, rather than promoting it and then stopping. Under the
documented upgrade procedure (every node on the same binary, GUIDE §12) this cannot fire.
It exists for mixed-version replication, which is unsupported and which `Hello`'s schema
digest does not catch for job shapes.

**At dispatch.** After the check, a foreign-shape Pending job cannot reach dispatch: the
check runs wherever dispatch can start, and duplicate job names are refused at mount
(`http/app.rs:279`). Dispatch therefore *asserts* shape equality before calling the glue
(hard rule 4) instead of handling it.

**Corruption** (the shape matches but decode fails) keeps today's path: an App error,
retries, then Dead. Its message stops saying "schema drift or corruption" and names
corruption, because drift can no longer reach it.

GUIDE's jobs section replaces the prose rule with the mechanism, the refusal text
(pinned), and the new-type path.

## §5 A stopped kernel ends the process

`launch`'s serve arm waits on the kernel thread instead of parking. When the thread ends
with an error, the process exits nonzero after printing its line. `serve` and
`serve_replicated`, which spawn the kernel detached, expose what `launch` needs to wait on.
(`serve::backup` runs synchronously and already returns its error to the caller.) That
touches the public `Server` surface, so its docs change in the same commit.

The stderr line stops asserting "a platform IO failure — restart to recover" for a stop
that is a refusal, because restarting the same binary cannot help one. The plan
enumerates the kernel's exit paths (`Kernel::new` and `Kernel::run` errors: poison, IO,
the table gate's refusals, the witness refusal, §4's jobs refusal) and pins each one's
line. Every line names its own fix.

Phase B of `crates/edge-harness/tests/failover.rs` changes from asserting the 500 to
asserting the exit and the stderr line. That file's own instruction applies: update to the
new behaviour, never weaken. GUIDE §13's `launch` section and DESIGN §5 state the
behaviour.

## §6 Oracles

Red before green (hard rule 3): each behavioural test below is written first and seen
failing against the platform at `c28250e` (current main), and the failure is recorded.

1. **The review's probe, as a sim test** (`charpente/tests/realtime.rs`). One test binary
   declares the event twice under the same name, `mod v1` and `mod v2` (`NAME` is the
   snake_case of the type name, so both are `incident_opened`). v1 emits, the node
   restarts, and v2 resumes from a pre-change cursor. Today: `kernel.run = Err(…)`. After:
   the `charpente.reshaped` frame, the named 409 on reconnect, and a second, unaffected
   tail still flowing.
2. **The silent misdecodes, pinned as behaviour.** One case swaps two same-typed fields;
   the other drops a trailing field. Today both deliver wrong JSON with no error (red).
   After, both are refused.
3. **A seeded property under the fault matrix:** no frame is ever decoded from a
   foreign-shape row, and every resume across a shape change ends in the named frame or
   the named 409, never in a skipped event. It runs over N fresh seeds; the plan sets N
   after timing one batch, and the commit and LEDGER state N and the master seed. Any
   failing seed is pinned.
4. **Jobs:**
   - a v1 job left Pending makes a v2 start refuse, byte-exact;
   - control: when every job is Done, v2 starts;
   - a v2 follower promoting over v1 Pending jobs gets `CtrlErr` by name and stays a
     follower;
   - each under a crash between enqueue and restart.
5. **Corruption and encoding:** a matching-shape row with a truncated payload gets the
   `charpente.error` frame with no `id:` line, and the node keeps serving. The flipped
   encode test gets the same.
6. **Compatibility:** a record written with `0x02`/`0x03` materializes with no shape and
   is refused on both paths. Old-format `EventRow`/`JobRow` bytes decode as `None`; any
   other trailing length is a named error. Replication's fingerprint oracle stays green
   with the new field.
7. **Process exit:** the updated `failover.rs` Phase B, plus a stock-binary test that a
   startup refusal exits nonzero with its named line.
8. **TS client:** `charpente-tsgen/tests/routes.rs` and `ts-fixture-app`'s drift test
   cover the two new terminal frames.
9. **Mutation testing:** `scripts/mutants.sh` over every new check site (the frame, the
   409, the rebuild check, the dispatch assert, the row decode), with `--test-package`
   naming the crate whose suites exercise it. Zero MISSED; a timeout is not a catch.
10. **The full gate:** `cargo fmt --all --check`, `cargo clippy --workspace --all-targets
    -- -D warnings`, and `cargo test --workspace --no-fail-fast`, confirmed by counting
    `test result: ok` lines.

## §7 Docs, in the same commits (hard rule 6)

- **GUIDE §11:** "Changing an event"; `charpente.reshaped`, `charpente.error` and the 409
  as pinned text blocks; the rewritten encode-failure paragraph.
- **GUIDE jobs section** (L1464-1465, L2721-2726): the mechanism, the pinned refusal, the
  new-type path.
- **GUIDE §12:** the nested-type blind spot, stated for tables and payloads alike.
- **GUIDE §13:** `launch` exits when its kernel stops.
- **GUIDE §14:** the TS client's new stream ends.
- **DESIGN §3** (the typed surface: payload shapes), **§5** (process exit), **§6**
  (delivery's new terminal frames; no whole-node-fatal delivery path remains).
- **PLAN:** mark this pre-publish fix done. Add to After launch: (B) versioned migrations
  for events and jobs; a deep fingerprint; a commit-time shape lock. Each carries its
  trigger (§8).
- **`2026-07-16-migrations-design.md`:** annotate the deferred `#[job]`/`#[event]`
  follow-up as discharged by this spec for detection and refusal, with rewrite deferred.
- **LEDGER:** the entry, with seeds and counts.

## §8 Refused, so the plan does not grow them

- **Versioned migrations for events and jobs (B).** After launch. Trigger: the first app
  that needs exact resume, or a Pending job, to survive a shape change it cannot express
  as a new type.
- **A deep fingerprint for nested types.** After launch. Trigger: the first
  nested-type change that misdecodes in a real app or a docs-only build.
- **A commit-time shape lock** (a committed fingerprint file plus a check verb). After
  launch. Trigger: the first shape change that reaches a deploy unnoticed. Apps using the
  generated TS client already get a CI signal for event payload changes through
  `check-client`'s staleness diff.
- **Job shapes in `Hello`'s schema digest.** Mixed-version replication stays unsupported;
  §4's promote check names the one case that matters.
- **Any override.** No flag skips a shape check, and legacy rows get no exemption (MANIFESTO
  §8).
- **C-2**, the other pre-publish fix, gets its own spec.

## §9 Exit criteria

- The review's C-1 probe, rerun as §6.1, gets the named frame and the named 409, and the
  node keeps serving.
- No path decodes an event or job payload under a shape other than the one it was
  written with, and a test exercises every path that could.
- No delivery path stops the kernel, and a stopped kernel ends the process with a nonzero
  exit and a named line.
- §6's oracles are green, the gate is counted, and the seed count and master seed are
  recorded. §7's docs land in the same commits.

## Amendments (implementation plan)

- **A1 — the jobs check runs wherever the write gate opens, and a promote checks before its role flips.** "Beside the schema gate in `do_promote`" would run it after `store_role(Primary)`, leaving a durable primary that refuses on every restart. It runs before `clear_stale_witnesses` and answers `CtrlErr`, so the operator hears it on the command and nothing durable changes. The kernel checks in `after_repl_step` whenever the write gate opens: a witness gate opening, or a switchover's target becoming primary.
  - **A switchover gets no target-side pre-check.** DESIGN §5 makes HandedOff the point of no return: once the primary sends `HandOff`, it never aborts. A target refusing at that point would leave the primary write-stopped and the target a follower, and neither message would name a fix that works. So the hand-off completes, and the new primary's kernel refuses at its writable flip. That is the startup refusal, and its fix ("run the previous binary until they are Done or Dead") works from that state: the new primary on the old binary dispatches them.
  - `repl.step` and `after_repl_step` run back to back inside one `Ready::Repl` arm, so no dispatch can come between the flip and the check.
- **A2 — the encoder is structural, not asserted.** `TxEffects` carries a bare `u64` shape per enqueue and emit, so a shapeless one cannot be represented. `encode_record` writes only `0x08`/`0x09`, and there is nothing left to assert.
- **A3 — `PROTO_VERSION` 2 → 3.** A version-2 follower would refuse `0x08` at apply by name; the bump refuses it at `Hello`, before any frame ships.
- **A4 — the job-shape registry lives in `Schema`.** `Db::register_job_shapes` writes it and `Db::pending_job_shape_refusal` reads it. It sits in `Schema` rather than in the `Db` or the dispatcher for two reasons:
  - a snapshot re-seed rebuilds the `Db` from its schema (`into_seeder` → `Seeder::finish` → `Db::open`), and a registry held anywhere else would come back empty;
  - `do_promote` holds the `Db`, not the dispatcher.

  It is excluded from `schema_digest` (spec §8: job shapes are not in `Hello`).
- **A5 — one refusal parenthesis.** Every refusal text reads `(recorded X; this binary Y)`, the no-shape case included. The job refusal's wording and its five-id elision are fixed in Task 6.
- **A6 — where the TypeScript runtime test lives.** It goes in `ts-fixture/src/client-runtime.test.ts` (`node --test`), because the new frames are runtime behaviour. `ts-fixture-app`'s drift test covers type shapes, not frames.
- **A7 — `charpente.error` has no `id:` line on the wire.** The encoder omits that one line; `EventFrame.id` still holds the row id for sim tests.
- **A8 — a `Refusal` marker.** `charpente_core::db::Refusal`, carried inside `io::Error::other`, is how the kernel's stderr line tells a refusal from a platform IO failure. Every existing `io::Result` path is untouched.
- **A9 — the pinned `charpente.reshaped` block holds its `event:` and `data:` lines only.** `doc_truth` refuses a pinned line with no 16-character fragment outside placeholders, and an `id: <id>.<seq>` line is nothing else. The prose around the block states the `id:` line.
- **A10 — (verified in Task 3) what serde_json cannot encode.** The spec's example of an unencodable payload, a `NaN`, is believed wrong: serde_json writes non-finite floats as `null`, and the case that fails is a map with non-string keys. Task 3 runs the experiment and writes GUIDE/DESIGN from what it shows.
- **A11 — a corrupt `EventRow` fails its tail too.** This reads spec §1's "a corrupt event row" literally: a row whose own bytes fail to decode (`InvalidData` out of the scan) gets `charpente.error`. redb's own failures (`ErrorKind::Other`) still stop the node, because they are the platform's, not a subscriber's.
- **A12 — four new pinned blocks.** The 409, the reshaped frame, the error frame and the job refusal take `PINNED_FLOOR` from 16 to 20.
- **A13 — the stock-binary startup-exit test uses a corrupt role file.** A jobs refusal needs two builds of one app, which the edge harness does not have; a corrupt role file ends the same kernel thread at startup through `serve_replicated`.
