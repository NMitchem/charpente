# Retry-safe writes

Status: design, 2026-11-22, revision 3. Noah chose the contract ("every write carries an
order number, and charpente throws away repeats", option 1 of the 2026-11-22 twin
discussion) and handed the rest over ("you have autonomy"). The choices marked *(call)* are
the designer's. This is step 1 of two. Step 2, the fault-free twin
(`2026-11-22-fault-free-twin-design.md`), reads this step's records.

Revision 2 follows a builder check of revision 1. These findings were confirmed against the
source:
- `SimSource` lives in `charpente` with no seed (`kernel/mod.rs:1567`), and merge-fixture
  builds one source per bid. So sim-side stamping in `SimSource` would make ids collide.
- `ApiKey` is an ordinary table any handler can read, so storing replies there would break
  DECISIONS §12.
- `App::handle` carries no time, so `tx.now()` panics there (`db.rs:1350`).
- `Group::stage` asserts `MAX_RECORD_BYTES` (`db.rs:5154`), so a large stored reply would
  panic the kernel.
- DECISIONS §6's loss window loses acked records, not unacked ones.
- `demo.rs` and the scaffold pin today's double-apply.

Revision 3 records the calls made while planning (`docs/superpowers/plans/2026-11-22-retry-safe-writes.md`):
- **The wire refusal sits in `stage_or_subscribe`.** The edge's conversion
  (`to_kernel_request`) runs on the network thread, which has no router. It marks the
  request as from the wire, and the kernel refuses after matching and before the
  `fresh_token` draw, so a refused request draws nothing.
- **One 400 text** covers missing, malformed and non-ASCII ids. The edge drops a non-ASCII
  header before charpente sees it.
- **The row id** is the first 8 bytes of a SHA-256 over the length-prefixed principal, route
  and id. A slot collision is refused by name, since it is rare but reachable at scale.
- **`PROTO_VERSION` 9 and `MANIFEST_FORMAT` 6.** Platform tables sit outside the schema
  digest, so the digests alone would let a 0.6.0 follower fail mid-replay.
- **The sweep** is the kernel's eighth ready source: due when the oldest record ages out, no
  sooner than an hour after the last sweep that left rows; at most 4096 rows a step.
- **The scaffold keeps its guard.** Its first branch was retry safety, which the platform now
  provides, and its second is write-once, which is app semantics.
- **README keeps 0.6.0's red runs** as labelled history and adds 0.7.0's green run, pinned.
  Reframing the lead stays Noah's call.

## 1. Why

A client that sends a write and loses the reply cannot tell whether it landed, so it sends
again. charpente today runs the copy as a new request. `examples/double-charge` is that bug
shipped on purpose. An outside agent testing 0.6.0 wrote the same bug into its own order
route unprompted (LEDGER 2026-11-21). MANIFESTO §4 deletes the dual-write bug class by
construction, and this step does the same for the duplicate write. A write names its intent,
the kernel records the intent with the commit, and a repeat gets the first reply back
without running again.

## 2. Goals, non-goals, anti-goals

Goals:
- Within one timeline and the retention window, a re-sent write that carries the same
  intent id never applies twice, across restarts.
- At the wire, a write with no intent id is refused by name.
- `Drive`'s re-sends carry the id they were first sent with, so the sim exercises duplicate
  removal with no workload change.

Non-goals:
- **Reads.** GET and SSE routes are untouched. A GET handler that writes stays outside the
  contract, and GUIDE §5 says so. A PLAN row covers refusing it later.
- **Different intent ids.** Two ids are two intents, even with the same body.
- **In-process callers without an id.** A request handed to `App::handle` or a `SimSource`
  with no id gets today's behaviour. Real clients always pass through the wire.
- **DECISIONS §6's loss window.** A commit the old primary acked but had not shipped is
  lost at failover. If its client re-sends, it runs again on the new timeline, and an
  outbound effect it enqueued mints a new key (DECISIONS §14). This step does not close
  that window.
- **Automatic retries in the generated TS client.**

Anti-goals:
- A raw credential on disk (DESIGN §8; `assert_no_raw_credentials_on_disk` in
  `crates/charpente/tests/auth.rs`).
- A second commit point. The record commits in the request's own staged transaction.
- A table a handler can read or forge.
- A stored reply large enough to fail a record.

## 3. Constraints

- **The request path.** In `crates/charpente/src/http/app.rs`,
  `StagedDispatch::stage_or_subscribe` matches the route, draws `fresh_token`, then stages
  one closure that runs `build_ctx` (auth) and the glue. `Group::stage` reads through the
  group's overlay (`db.rs:4813`). So a second copy of an intent staged in the same group sees
  the first's record. `CommitFailure::invalidates` already fails a repeat answered from a
  sibling's failed record.
- **DECISIONS §12 and §13.** The record is sealed like `PLATFORM_TABLES` (`db.rs:1206`).
  Retry safety is always on, which §13's reopen clause covers. The one choice, a webhook's id
  source, is a route word with a compile-fail pin.
- **DECISIONS §15.** No compatibility. A 0.6.0 follower and a 0.6.0 archive reader are
  refused by name: `PROTO_VERSION` is 9 and `MANIFEST_FORMAT` is 6. A 0.6.0 binary refuses
  a 0.7.0 data directory only while its log holds an intent record. After a checkpoint it
  opens the directory and ignores the table, which §15 accepts, since no downgrade is owed.
  Every workload that writes re-pins its seeds, and each pin's recorded reason is checked.
- **Hard rule 6.** These docs change in the same commits: GUIDE §5, §8 and §13, DESIGN (the
  request path and platform tables), the TS client's docs, and CHANGELOG 0.7.0. README is
  covered in §5.

## 4. Design

### 4.1 The wire

- A write route is one whose method is POST, PUT, PATCH or DELETE and that is not SSE. A
  request to one carries `Idempotency-Key`, the IETF httpapi draft's name. The value is
  opaque, 1 to 255 visible ASCII bytes *(call)*.
- **Missing or malformed.** The refusal sits where the wire becomes a `Request`: serve.rs's
  conversion, which launch-driven nodes share (the builder confirms there is no second
  path). It answers 400, naming the route, the header and the fix: "`POST /orders/:id` needs
  an Idempotency-Key header: charpente answers a re-sent write once (GUIDE §5). The
  generated client sends one, and a webhook route names its sender's id with
  `intent = header(...)`."
  - Naming the route needs the router, so the check runs after matching.
  - A request that matches no route gets the existing 404.
- **Webhooks.** `#[route(POST "/hooks/pay", public, intent = header("X-Event-Id"))]` reads
  the id from the named header *(call: header-only in v1)*. A malformed value is a compile
  error, pinned in `crates/charpente-macros/tests/compile_fail/`.
- **Replayed replies are marked** with `Idempotent-Replayed: true` *(call: Stripe's
  header)*.

### 4.2 The record

- **The table.** `charpente.intents`, hand-written and sealed beside `charpente.jobs` (a
  `PLATFORM_TABLES` entry, `Scope = PlatformOwned`). It is read and written only through a
  path the request staging owns, which handlers cannot reach. The builder designs that core
  API: for example, a capability minted from the `Group`.
- **The row.** Each row holds:
  - the raw intent id, the principal (subject, or none on a public route) and the route
    pattern;
  - a key that hashes all three, so two principals or two routes never share an intent;
  - the request's fingerprint (method, path, body);
  - the reply status, and the body when §4.3 allows it;
  - `committed_at`, the `now` that `stage_or_subscribe` receives. That is the kernel's step
    time, or the epoch under `App::handle`, which has no time.
- **When it is written.** Inside the staged closure, after `build_ctx`, a request carrying
  an id is looked up:
  - **No row.** The glue runs. On `Ok`, if the transaction has any other effect, the record
    is staged with it. A write that changed nothing has nothing to repeat, so it records
    nothing, costs no log record, and still works on a follower. On `Err`, nothing commits,
    and a re-send runs again.
  - **A row with the same fingerprint.** The stored reply is returned, or a 409 under §4.3.
    The glue does not run.
  - **A row with a different fingerprint.** 422, naming the id and saying it was already
    used for a different request.
- **Draws.** `fresh_token`'s draw stays before staging. A repeat skips staging, so it skips
  the outbound key draw a first attempt would make. Universes stay deterministic, and seeds
  re-pin anyway.

### 4.3 What a stored reply keeps

- **The body is stored** when it is at most 64 KiB *(call)* and the transaction minted no
  credential (`Ctx` records a minted session or API key).
- **Otherwise the row keeps the status and a reason.** A repeat then gets a 409 that says
  the request already committed, gives the reason ("its reply carried a credential, which
  charpente shows once" or "its reply was too large to keep"), and tells the client to send
  a new request with a new id. DESIGN §8 is unchanged, and its oracle stays green.

### 4.4 Retention

- A row lives 24 hours *(call; `INTENT_RETENTION`, named like `CLOCK_TOLERANCE`)* past the
  later of its `committed_at` and the node's latest open. Both a process open and a promotion
  count as an open, since clients have just regained a primary either way.
- The sweep is kernel-run, not an app job, because it needs the kernel's open reading
  (`kernel/clock.rs`). It computes its bound when it runs: it deletes nothing until the node
  has been open for `INTENT_RETENTION`, and after that, rows whose `committed_at` is older
  than the retention.
- The lookup ignores age, so whether a re-send is recognised never depends on when the sweep
  last ran.
- This covers the sim's clock-downtime axis, which jumps the wall clock up to 3 days on a
  power cycle. `Drive`'s re-sends arrive just after such a jump and are still recognised.

### 4.5 The sim and tests

- **`Drive` stamps its requests.** A write request without an id gets one from a keyed
  stream of its own (`keyed_stream(seed, "intent:drive")` plus the request index) before the
  faulted phase sends it. The re-send is a clone, so it carries the same id. No fault, input
  or entropy draw moves.
- **Hand drivers and direct `App::handle` callers** keep today's behaviour unless they set an
  id with the new `Request::intent(id)` helper. GUIDE §8 says a hand-built re-send needs the
  first copy's id to be recognised, and that the wire refuses a write with none.
- **Red first.** A sim property sends a write through `Drive`, loses its reply, re-sends it,
  and asserts exactly one effect. It fails on today's main, and passes after.

### 4.6 The generated TS client

Every write function takes an optional `intent` argument and sends it as `Idempotency-Key`.
The default is 16 bytes from `crypto.getRandomValues`, hex-encoded. That works on plain
HTTP, where `crypto.randomUUID` does not. GUIDE documents the retry pattern: call again with
the same `intent`. `ts-fixture-app`'s staleness and drift oracles re-pin.

### 4.7 `describe`

Each write route's entry gains `intent`: `"Idempotency-Key"`, or the declared header. GUIDE
§13 and `describe_contract`'s golden change with it.

## 5. What changes for existing artifacts

- **The scaffold.** `charpente new`'s `add_note` keeps its guard (revision 3): re-sends are
  the platform's now, and a second body under one note id is still the app's to refuse.
  `tests/new.rs`'s `deleting_the_retry_guard_turns_the_echo_check_red_on_a_resend` becomes a
  pin that deleting the guard leaves the echo check green under `Drive`'s re-sends and loses
  only the conflict refusal.
- **double-charge** pins `=0.6.0` on crates.io, so its published story is unchanged. On the
  workspace, the patched copy that `demo.rs` builds stops double charging. `demo.rs`'s
  red-run pins (`master_batch`'s exit 1, `GOLDEN`, `STARTUP`, the frame fit, the repro
  replay) and README's two quoted runs (`demo.rs:316`, `:349`) can't hold.
  - *(call)* The example becomes the 0.7.0 story: the same six-line route, the same re-send,
    and the platform answering it, with the guard lines gone. `demo.rs` pins that run. README
    keeps its two 0.6.0 runs as labelled, unpinned history and adds the 0.7.0 run, pinned
    (revision 3).
  - README's launch video stays, captioned as 0.6.0.
- **Flag for Noah.** README's lead now tells a story about a bug 0.7.0 makes impossible.
  The factual update above keeps README true. Whether the lead should change is his call.

## 6. Order of work

1. The sealed table and its staging-only access path in core.
2. The lookup, the record and the reply rules in the request path. Red first, as in §4.5.
3. `Drive`'s stamping, and `Request::intent`.
4. The wire refusal, `intent = header(...)`, its compile-fail pin, and `describe`.
5. Retention and the kernel sweep, with a property that crosses a clock-downtime jump.
6. The TS client.
7. The format and version bumps: a 0.6.0 follower and archive reader are refused by name
   (`PROTO_VERSION` 9, `MANIFEST_FORMAT` 6). A 0.6.0 binary refuses a 0.7.0 data directory
   only while its log holds an intent record, and after a checkpoint opens it and ignores
   the table, which DECISIONS §15 accepts (no downgrade owed).
8. The scaffold, double-charge, `demo.rs`, README, GUIDE, DESIGN and CHANGELOG.

The evidence bar is the workspace suite green and counted, plus the app batches (sim-fixture,
invoicing, outbound-fixture, a fresh scaffold, double-charge's patched copy) green over fresh
master seeds, with the count stated.

## 7. Left to the builder

- Whether the fingerprint hashes the body or stores it, using a deterministic hash already
  in the tree.
- The core API that lets request staging, and nothing else, read and write the sealed table.
- Where exactly the sweep runs in the kernel loop.
