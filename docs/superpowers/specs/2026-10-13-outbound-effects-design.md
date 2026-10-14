# Outbound effects — design

**Date:** 2026-10-13. **Status:** Design approved section by section in session 2026-10-13;
written spec awaiting Noah's review. No plan yet.
**Predecessor:** none in PLAN.md, which is the finding. The surface was first promised by
the M0 step-6 spec (`2026-07-02-queue-cron-design.md:20-21`: "External IO in handlers is M1
(with a typed outbound surface)") and never scheduled. Every citation below was verified
against `main` at `f21b491`.

## Context

MANIFESTO §5 states a guarantee the platform does not deliver: "External effects (emails,
third-party API calls) execute *after* commit, driven by the durable queue, with idempotency
keys making them exactly-once-observable" (MANIFESTO.md:77-79). README.md:68-71 repeats it
about its own `SendReceipt` example ("external effects (the receipt email) run post-commit via
the durable queue"). DESIGN.md:35 draws it ("external effects (email, APIs) — post-commit
only") and DESIGN.md:416-418 restates it.

There is no way to do it. Every job handler is `fn run(self, tx: &mut Tx)`, itself a
transaction (DESIGN.md:1664-1665), and a transaction cannot await the network (hard rule 2).
`ProdNet` is replication's peer transport, and the archive's production `ObjectStore` is
`FsObjectStore`, a filesystem — there is no outbound HTTP client anywhere in the workspace.
The docs still point at the missing piece as if it existed: DESIGN.md:1676 ("the framing M1's
external-effect handlers need"), DESIGN.md:2054 ("waits for the external-IO surface"),
GUIDE.md:1698-1699 ("M1's external-IO surface consumes it"), `charpente-core/src/db.rs:1456`
("consumed by M1's external-IO surface"), and GUIDE.md:6704-6712 and :6755-6762, which warn
that a restore "can re-fire emails and third-party calls". PLAN.md's M1 deferral list and its
v1.x table never mention it, and docs/LEDGER.md never does.

Every docs-only build was briefed around the gap rather than into it: `examples/BRIEF.md:11`
("there is no external IO"), `docs/experiments/2026-09-helpdesk/BRIEF.md:23` and
`docs/experiments/2026-09-statuspage/BRIEF.md:47` ("a table; no external IO"). The first
outside agents to build freely on 0.1.0 (four reports, 2026-10-13) hit it at once: one wrote
its own ~60-line fault layer for LLM latency, tool failures and duplicate delivery, and
concluded "for running live agents inside charpente, it's the wrong shape."

This spec builds the surface the docs already describe: post-commit, queue-driven, keyed,
and — the part that makes it charpente's — inside the simulator, with the outside world's
faults injected by the platform.

## §1 Decisions already made

- **2026-10-13 (Noah): approach A** — a two-phase outbound job, `call` then `settle`, with the
  kernel owning the call. Approaches B and C are refused (§2 R1, R2).
- **2026-10-13 (Noah): one surface for both targets.** Payment-shaped calls (idempotency, a
  reply lost after the remote applied the call) and LLM-shaped calls (slow, large replies,
  many in flight) are the same HTTP request/response surface; they differ only in limits.
- **2026-10-13 (Noah): done means oracles plus a docs-only build** (§16). Sim properties, an
  edge-harness wire scenario, and a fresh docs-only agent build reviewed as helpdesk and
  statuspage were.
- **2026-10-13 (Noah): §§3-14 below** were presented and approved section by section in
  session (kernel and IO trait; job surface; failure semantics; simulation; production
  edge; testing, exit and docs). This document is their written form plus the precision the
  session left implicit (R6-R10).

## §2 Rulings

1. **Not C, edge workers.** A worker SDK — effect handlers as ordinary async Rust on the
   tokio edge, any client crate — puts the handler outside the deterministic boundary. The
   sim cannot run it and the deny-list cannot police it, so every injected fault stops at the
   worker's door. *Basis:* MANIFESTO §6, "effects are explicit, ordered, and simulable";
   DESIGN §1's single boundary.
2. **Not B, `tx.call(..)` inside the transaction.** The request would be frozen at commit
   (an invoice voided before dispatch still gets its call), its body would ride the WAL
   (prompts and anything sensitive replicate and archive), and storing a reply still needs a
   completion handler — B rebuilds A's `settle` and adds a second mechanism.
3. **`call` cannot write, by type.** It receives `&Tx` / `&ScopedTx<'_, '_, S>`. Reads take
   `&self` and writes take `&mut self` (`db.rs:2179` vs `:2208`), so a write in `call` is a
   compile error with no new type. Its staged transaction has no effects and is discarded
   (DESIGN.md:674-676).
4. **The platform mints the idempotency key** (§6). A key derived from the job id, or from an
   app key built on counter ids, collides across a failover's diverged timelines and turns a
   lost-window effect into a cross-entity misattribution. *Basis:* DECISIONS §6's loss window
   is real and named; the event cursor's `<id>.<seq>` exists for the same reason
   (DESIGN §6).
5. **The key is handed to `call`, never injected.** APIs disagree on where a key goes (Stripe:
   a header; others: a body field; some: nowhere). A platform-chosen header would be a silent
   default (DECISIONS §13).
6. **`start` waits for the group to commit.** `call` reads through the group overlay and can
   see a sibling's uncommitted writes (DESIGN.md:704-709). A started call is an answer given
   to the outside world, and "what a participant SAW, not what it wrote, is the failure rule"
   (DESIGN.md:704). Starts are buffered in the job arm and issued only after
   `group.commit()` returns `Ok`; on a commit failure the arm stops the kernel (as today) and
   the buffer is dropped.
7. **The `Outbound` token is a per-process counter, never the job id.** A timed-out attempt's
   reply can arrive after the retry started; with the job id as token it would settle the
   wrong attempt.
8. **Outbound-ness is part of a job's shape.** A Pending job enqueued as a pure job has no
   minted key, and one enqueued as outbound has no `run`. Declaring or removing `outbound`
   changes the fingerprint, so the existing refusal ("a Pending job of a mounted type carries
   another shape", DESIGN §6) covers the switch by name, with the existing fix (drain with
   the old binary). The *host* is not folded in: a minted key is host-agnostic, and changing
   provider should not force a drain.
9. **Hosts are declared twice, and the sets must match.** A job names its host
   (`outbound = "…"`); the `App` carries one `Host` entry per host (§9). Mount refuses a job
   host with no entry, and an entry no job uses, by name. The sim likewise refuses a host
   with no model (§10). Symmetric strictness is the DECISIONS §13 reading: the platform asks
   at every site rather than defaulting.
10. **The edge makes no decisions** (`serve.rs:1-4`). No timers, no retries, no redirect
    following in the production client. Timeouts are the kernel's, on its `Clock`, so
    production and the sim run the same decision code (DESIGN §1's same-shape rule).

## §3 The `Outbound` IO trait

The sixth top-level trait in `charpente-core/src/io.rs` (after `Clock`, `Entropy`, `Disk`,
`Net`, `ObjectStore`), synchronous and poll-shaped (DECISIONS §10):

```rust
pub struct OutboundCall {
    pub host: &'static str,          // from the job's declaration, never runtime data
    pub method: Method,              // GET | POST | PUT | PATCH | DELETE
    pub path: String,                // begins with '/': path and query only
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
    /// Attribution only: the edge never transmits it. The app places the key where its API
    /// wants it (R5); the sim reads this field to attribute a delivered call to its job.
    pub key: IdempotencyKey,
}

pub struct OutboundReply {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

/// Outbound HTTP to declared hosts. Many calls in flight; at most one per token.
pub trait Outbound: Send {
    /// Begins `call` under `token`. Asserts `token` is not in flight.
    fn start(&self, token: u64, call: OutboundCall) -> io::Result<()>;
    /// One completion, or `None`. A transport failure IS a completion: the error rides
    /// inside the `Some`, as with `ObjectStore::poll`.
    fn poll(&self) -> Option<(u64, io::Result<OutboundReply>)>;
    /// The kernel abandoned `token`. A completion for it may still surface; the kernel
    /// discards it.
    fn cancel(&self, token: u64);
}
```

The trait carries no timeout and no retry: both are kernel decisions (R10).

## §4 The kernel

**Dispatch.** The `Dispatcher` keeps outbound jobs in their own due-set beside the existing
`(due_at, id)` heap, so a capacity-blocked outbound job never blocks a pure job due after it.
An outbound job is due only while the in-flight table is below `max_in_flight`
(`AppConfig`, default 64).

**The job arm** (the existing `Ready::Job` group, `kernel/mod.rs:624`). For an outbound job it
stages `call(&tx, &key)`:
- `Ok(None)` stages the Done-mark — nothing to send is a success.
- `Ok(Some(call))` stages nothing; the arm validates the call (§5 "What `call` may build"),
  removes the job from the due-set, records `token → (job id, deadline = now + timeout)` in
  the kernel's in-flight table, and **buffers** the start (R6).
- `Err(App)` or a panic stages the failure record, exactly as `run` does today.
- `Err(Io)` bubbles, not an attempt, as today.
After `group.commit()` returns `Ok`, the buffered starts are issued in order.

**The seventh ready source, `Ready::Outbound`.** `poll` consumes (the archive arm's
constraint, `kernel/mod.rs:352-356`), so once per step, *and only while the in-flight table is
non-empty*, the kernel drains `poll` into its own completion buffer before building the ready
set. The arm is ready when that buffer is non-empty or the earliest in-flight deadline has
passed. One win drains into one group and commits once (the job arm's shape and its
commit-then-bubble rule):
- a reply for an in-flight token stages `settle(tx, reply)`; `Ok` commits its effects with the
  Done-mark as one record; `Err(App)` or a panic discards them and stages the failure record;
- a transport error for an in-flight token stages the failure record (§7);
- a passed deadline calls `cancel(token)` and stages the failure record;
- a completion for a token not in the table is discarded and counted (`outbound_late`).
A failure record re-enters the job into the due-set at its backoff, as today.

**Parking.** The park deadline gains the earliest in-flight deadline, and while anything is in
flight the idle wait is capped at replication's 5 ms park tick — the precedent it set for
in-flight IO (DESIGN.md:326-328).

**Wiring.** `Kernel::with_outbound(app, clock, outbound)` beside `with_archive`; a node that
needs both gets a builder rather than a five-argument constructor. `serve` supplies the
production client (§9). A kernel built without an `Outbound` for an app that mounts an
outbound job refuses to build:
`kernel: job ChargeInvoice calls api.stripe.com, but this kernel has no Outbound — serve() supplies the production client; a sim test passes Kernel::with_outbound(app, clock, Box::new(sim.outbound(&app.outbound_hosts())?))`.

**Stats.** `KernelStats` gains `outbound_started`, `outbound_settled`, `outbound_failed`,
`outbound_timed_out` and `outbound_late`.

**Invariants, asserted on in release (hard rule 4):**
1. A token is never started while in flight; a token is never reused within a process.
2. A job in the in-flight table is never in a due-set.
3. The in-flight table is empty whenever the node is not writable. The step that observes
   writable → not writable drops it (late completions are then discarded by rule).
4. `settle` never runs for a token that was not in flight.
5. No `start` precedes the commit of the group that staged its `call`.
6. With no outbound job mounted and nothing in flight, the kernel calls no `Outbound` method
   and draws no entropy for outbound: every existing app replays byte-for-byte, and every
   existing pinned seed names the same universe (the `Tail` arm's precedent, DESIGN §1).

A call never fires for an enqueue that is not durable: apply runs strictly after sync
(`engine.rs:169-171`) and the due-heap is fed from apply (`kernel/mod.rs:852-859`). R6
extends the same guarantee from the enqueue to everything `call` read.

## §5 The job surface

```rust
#[job(scoped_by = Org, outbound = "api.stripe.com", retry(max = 8, backoff = "2s"), timeout = "30s")]
struct ChargeInvoice { invoice: Id<Invoice> }

impl ChargeInvoice {
    fn call(&self, tx: &ScopedTx<'_, '_, Org>, key: &IdempotencyKey) -> Result<Option<Call>, JobError> {
        let inv = tx.get(self.invoice)?.ok_or(JobError::app("invoice gone"))?;
        if inv.state != InvoiceState::Final {
            return Ok(None); // voided since enqueue: Done, no call
        }
        Ok(Some(Call::post("/v1/payment_intents")
            .header("idempotency-key", key.as_str())
            .form(&[("amount", inv.amount_cents.to_string())])))
    }

    fn settle(self, tx: &mut ScopedTx<'_, '_, Org>, reply: Reply) -> Result<(), JobError> {
        match reply.status() {
            200 => { /* record the charge id from reply.json::<Intent>()? */ Ok(()) }
            402 => { /* record the decline */ Ok(()) }
            s => Err(JobError::app(format!("stripe answered {s}"))), // an attempt: backoff, same key
        }
    }
}
```

A `global` job's methods take `&Tx` and `&mut Tx`. `#[job]`'s existing words keep their
meaning; `outbound = "<host>"` selects the two-method form and `timeout = "<dur>"` (default
`"30s"`, same duration syntax as `retry`'s `backoff`) applies only with it. A default is
admissible here: DECISIONS §13 binds words that choose between a guarded and an open
behaviour, and a timeout is neither — the `retry` defaults are the precedent.

**`Call`** has `get`/`post`/`put`/`patch`/`delete(path)` constructors and `.header`, `.json(&T)`,
`.form(&[(k, v)])` and `.body(Vec<u8>)` builders. **`Reply`** has `status()`, `header(name)`,
`body()` and `json::<T>()`.

**What `call` may build** — checked when the arm validates it, each refusal an attempt whose
`last_error` names the rule:
- `path` must begin with `/` and carry no scheme or authority;
- `host`, `content-length`, `transfer-encoding` and `connection` are the edge's to set;
- a header a `Host` entry injects (its credential, §9) cannot be set by app code;
- the body is capped at the host's request cap (§9).

**Compile-time refusals**, each pinned in `charpente-macros/tests/compile_fail/` and listed
in GUIDE §9:
- `outbound` with no host, or a host carrying a scheme, port, path or userinfo;
- `timeout` without `outbound`;
- an outbound job whose impl has `run` instead of `call` and `settle` (the macro's message
  names both signatures for the job's tenancy);
- `outbound` with `cross_tenant` (refused in v1; §15, and a v1.x row).

**Enqueueing is unchanged.** `enqueue`, `enqueue_keyed`, `enqueue_in` and cron accept outbound
jobs like any other. `enqueue_keyed`'s app key keeps its v0 meaning: the app's own identity
for the job, carried and never deduped on. GUIDE.md:1698-1699's claim that the outbound
surface consumes it is corrected (§13).

## §6 The idempotency key

**Minted at staging, never at apply.** When an outbound job is enqueued, the staging path
draws 16 bytes from the node's `Entropy` (the source request staging already holds,
`http/app.rs:952`) and records them in the enqueue effect. Apply must stay a pure function of
the record, so the draw can never move there. Rendered for apps as 32 lowercase hex
characters (`IdempotencyKey::as_str`).

**Two new record tags**, following the `0x08`/`0x0a` split by scopedness (DESIGN.md:607-612;
`0x0c` is the next free tag, `db.rs:5794`):
- `0x0c` outbound enqueue: `[job][payload][key: Option<String>][shape: u64][okey: [u8; 16]]`
- `0x0d` scoped outbound enqueue: `[job][payload][key: Option<String>][shape: u64][scope: u64][okey: [u8; 16]]`

**`JobRow` gains a trailing `outbound_key: Option<[u8; 16]>`**, encoded only when `Some`, the
precedent `shape` and `scope` set (`job.rs:84-95`; payload-shapes spec §2, job-tenancy spec
§6), so every existing row decodes unchanged.

**What the key is stable across:** every retry, every crash re-dispatch, every switchover
or failover re-dispatch of a shipped enqueue, and a restore from the archive (the row rides
the snapshot). **What it differs across:** two jobs, always; and two diverged timelines,
because each timeline's enqueue drew its own bytes.

## §7 Failure semantics

| What happens | An attempt? | Result | Key |
|---|---|---|---|
| `call` returns `Ok(None)` | — | Done-mark staged; no call | — |
| `call` returns `Err(App)` or panics | yes | failure record, backoff; no call | — |
| `call` builds a refused call (§5) | yes | failure record naming the rule | — |
| `call` returns `Err(Io)` | no | bubbles, restart-and-recover | — |
| transport failure: refused, TLS, DNS, reset, a reply over the cap | yes | failure record, backoff | same |
| deadline passes | yes | `cancel`; failure record, backoff | same |
| completion for a token not in flight | — | discarded, `outbound_late` counted | — |
| `settle` returns `Ok` | — | its effects and the Done-mark, one record | — |
| `settle` returns `Err(App)` or panics | yes | its effects discarded; failure record, backoff | same |
| `settle` returns `Err(Io)` | no | bubbles; still Pending, called again after restart | same |
| crash with calls in flight | no | in-flight table is volatile; Pending jobs re-dispatch | same |
| node stops being writable | no | in-flight table dropped; the new primary re-dispatches | same |
| restore from the archive | no | Pending rows carry their key; a re-fire is the same request | same |

A failure record follows the existing policy: `attempts` increments, `due_at` doubles from
the base backoff, Dead at `retry.max` (GUIDE §6).

**Error text (hard rule 5).** An attempt that may have reached the remote says so:
- `outbound api.stripe.com: timed out after 30s — the remote may have applied this call; the retry carries the same idempotency key`
- `outbound api.stripe.com: connection reset after the request was sent — the remote may have applied this call; the retry carries the same idempotency key`
- `outbound api.stripe.com: connection refused — the call did not reach the remote`
- `outbound api.stripe.com: reply exceeded the 4 MiB reply cap (raise it with Host::max_reply_bytes)`

**Dead means "no reply was settled"**, not "it never happened at the remote". GUIDE §6 says so
where it describes Dead outbound jobs.

## §8 Honest limits

**The §6 loss window now covers external effects.** A dead primary can call out from a
record it acked but never shipped. The world saw that effect; the surviving timeline has no
record of it. Nothing short of synchronous replication closes this, and DECISIONS §6 chose
asynchronous replication. It is named, not fixed: DESIGN §5's loss-window paragraph and
GUIDE §10's promotion section gain this case. The minted key bounds the rest: an enqueue that
shipped without its Done-mark is re-called with the same key, and an enqueue that never
shipped cannot collide with anything the new timeline mints.

**At-least-once calls cost money where the remote ignores keys.** LLM APIs generally do not
dedupe. A lost reply means a second completion is requested and paid for; `settle` commits at
most once, so state stays exactly-once. GUIDE §6 says this plainly.

**Deploying a changed `call` under an in-flight key.** A restart mid-call followed by a binary
whose `call` builds different parameters sends a reused key with new parameters; Stripe-style
APIs refuse that, and `settle` sees the refusal as an ordinary reply. GUIDE §6 explains it
beside "Changing a job's fields".

**A crash-looping process re-calls.** A crash is not an attempt (as `JobError::Io` is not
today), so a job whose dispatch keeps killing the process is re-called on every restart.

## §9 Host config and the production edge

**Declared on the `App`, one entry per declared host (R9):**

```rust
app.outbound(Host::new("api.stripe.com").bearer_env("STRIPE_SECRET_KEY"));
app.outbound(Host::new("api.postmark.com").header_env("x-postmark-server-token", "POSTMARK_TOKEN"));
```

`Host` builders: `bearer_env(var)`, `header_env(name, var)`, `basic_env(user_var, pass_var)`,
`max_request_bytes(n)` (default 1 MiB, the inbound cap), `max_reply_bytes(n)` (default 4 MiB).
Both caps are bounded at 8 MiB, half of `MAX_RECORD_BYTES` (16 MiB, `log.rs:81`), so a reply
`settle` stores whole leaves room for the rest of its record; a larger setting is a
mount-time contract panic naming the bound.

**Secrets are names, not values.** The app holds environment-variable names. `serve` reads
each value once at start and the edge attaches it per request. Values never enter the log,
the archive, `call`, the sim or `describe`. An unset variable refuses the start:
`outbound: STRIPE_SECRET_KEY is unset — host api.stripe.com needs it for job ChargeInvoice`.

**Addressing.** A host resolves to `https://<host>:443`, certificates verified against
bundled Mozilla roots (`webpki-roots`), so behaviour does not vary with the OS store. Two
operator flags on `charpente::launch`'s serve verb:
- `--outbound-base <host>=<url>` points a host elsewhere — a local fake, a sandbox. Plain
  `http` is accepted only for a loopback address; anything else must be `https`.
- `--outbound-ca <path>` adds a PEM root, for an internal API on a private CA — and how the
  edge harness exercises real TLS (§12).

**The client.** It sits beside the inbound edge in `serve.rs`, under its deny-list allowance,
on the same tokio runtime pattern. `start` spawns a task, completions return on a std channel
the kernel polls, `cancel` aborts the task. HTTP/1.1 only, connection pooling per host, no
redirects (a 3xx is a reply `settle` sees — following one would let a remote move the call to
a host no type declared), no proxy.

**Dependencies** (justified in the commit that adds them): `hyper` gains `client`;
`hyper-util` gains `client-legacy` and `http1` (both already dependencies); new are
`hyper-rustls`, `rustls` with the `ring` provider (no cmake, unlike `aws-lc-rs`) and
`webpki-roots`. All edge-only; nothing enters the deterministic boundary and the deny-list is
unchanged. Rejected: `reqwest` (a larger tree, and its own redirect, retry and timeout
policies are decisions the kernel owns) and `native-tls` (behaviour varies by OS). The
clean-build cost is measured with the existing compile instrument and recorded in
docs/BENCH.md; it is not hidden behind a cargo feature.

## §10 Simulation

**Remote models are the outside world**, one per declared host:

```rust
pub trait Remote: Send {
    /// Runs when the request REACHES the remote. `rng` is the model's own seeded stream —
    /// an LLM model's variance comes from here.
    fn handle(&mut self, call: &OutboundCall, rng: &mut Rng) -> OutboundReply;
}

sim.remote("api.stripe.com", Box::new(Idempotent::header("idempotency-key", StripeModel::default())));
let outbound = sim.outbound(&app.outbound_hosts())?; // refuses a declared host with no model
let kernel = Kernel::with_outbound(app, Box::new(sim.clock()), Box::new(outbound))?;
```

- **A host with no model refuses** at `sim.outbound(hosts)`, which takes the app's declared
  host list as plain data (`charpente-sim` depends on `charpente-core` only, so it cannot take
  an `App`):
  `sim: job ChargeInvoice calls api.stripe.com, which has no model — add sim.remote("api.stripe.com", …) before sim.outbound(…)`.
- **Models survive `sim.restart()`.** A crash kills the box, not the remote.
- **A request in flight at a crash still arrives** at its drawn delivery time — it was on the
  wire — and only its reply is lost. Replies for any pre-restart call are dropped by process
  generation, so a re-dispatched job's new token can never receive an old reply.
- **Workloads read model state** with `sim.remote_ref::<M>(host)` to assert properties.
- **Any `FnMut(&OutboundCall) -> OutboundReply`** implements `Remote`.

**Stock models**, each a named choice and none a default:
- `Recorder` — answers 200 with an empty body and logs every call (fire-and-forget email).
- `Idempotent<M>` — Stripe's semantics over an inner model, keyed by what the *app* sent:
  `Idempotent::header(name, model)` reads a header, `Idempotent::by(extract, model)` reads any
  part of the request. A repeated key replays the first reply; a reused key with different
  parameters answers 409; a request carrying no key is applied every time, as Stripe does;
  `applied()` counts applications per key.

**Fault axes** on `FaultConfig`, each on its own roll stream (the `object.rs:34-42` lesson: a
shared stream would let a 0.0 axis shift every later draw). Delivery and reply times are drawn
at `start`, so `poll` is a pure virtual-time check:

| Axis | The remote sees | The kernel sees |
|---|---|---|
| `outbound_refused` | nothing | an immediate transport error |
| `outbound_lost_request` | nothing | silence, then its deadline |
| `outbound_lost_reply` | the call, applied | silence, then its deadline — the ambiguous case |
| `outbound_throttle` | nothing (overloaded) | a 429 or 503 |
| `outbound_latency` `(min, max)` and `outbound_slow` | the call, late | a reply late, or past the deadline (the LLM tail) |

Duplicates are not an axis: in HTTP they are a retry after a lost reply, and the sim produces
those. Whether a duplicate applies twice is the model's semantics — that is what `Idempotent`
is for. Semantic faults (an ambiguous LLM answer, a wrong tool result) are the app model's,
drawn from its `rng`. Each axis credits a coverage key (`fault:outbound_lost_reply`, …) for
`require [...]`. `FaultConfig::swarm` samples the outbound axes from their own stream, so the
existing axes' sampled values are unchanged.

**Checkers** in `charpente_sim::checks`. Each reads `SimOutbound`'s delivery log, which
attributes every delivered call by `OutboundCall::key` (§3) — never by where the app placed
the key in the request:
- `calls_have_durable_jobs` — every delivered call's key belongs to a `JobRow` in the
  recovered store (single-node: there is no loss window inside one sim box).
- `keys_are_per_job` — no two `JobRow`s carry the same key, so a minting regression (a key
  derived from the id, or drawn at apply) is caught by name.
- `outbound_settles_by_horizon` — under a fault-free tail (GUIDE §8), every outbound job ends
  Done or Dead.

## §11 `describe`

Each job gains `outbound`: `null`, or `{ "host": "api.stripe.com", "timeout_ms": 30000 }`. The
app gains `outbound_hosts`: one entry per `Host` with its credential variable *names* and
caps. "Which outside services does this app talk to, and what does an operator need to set?"
is then an inventory. The `describe_golden` and `describe_contract` fixtures update in the
same commit.

## §12 Oracles

1. **`SimOutbound` unit tests** (`charpente-sim`): each axis; each axis's own-stream rule
   pinned as `object.rs` pins its own (declaring an axis at 0.0 shifts no draw); in-flight
   requests arriving after a crash; reply drop by generation.
2. **`crates/charpente/tests/outbound.rs`**, a new suite under swarm faults: §4's invariants
   1-5; every row of §7's table; `settle` + Done-mark atomicity; a crash mid-flight
   re-dispatching with the same key; a switchover dropping the in-flight table; R6 (a `call`
   that read a sibling's write never starts if the group's commit fails); R7 (a late reply
   after a timeout never settles the retry); the three checkers.
3. **Invariant 6, the determinism oracle**: every existing pinned seed and recorded master
   seed replays unchanged with this landed.
4. **Two planted bugs**, DEMO.md-style, each red with its seed recorded in the regression
   suite and each mutation-proven: (a) an app that omits the key from its request, against
   `Idempotent`, double-charges under `outbound_lost_reply` plus retry; (b) a `settle` that
   treats a 503 as success records a charge that never happened.
5. **Compile-fail pins** for §5's four refusals, and tests against the exact text of the
   kernel, sim, mount and serve refusals (§4, §9, §10).
6. **Mutation run** over the new kernel arm, the dispatcher's outbound due-set and
   `SimOutbound`, with `--test-package charpente` (without it every mutant reads MISSED having
   measured nothing — the 2026-09-16 trap). A survivor is a property to write, not an entry
   for `.cargo/mutants.toml`.
7. **Edge harness, `crates/edge-harness/tests/outbound.rs`**: the stock binary against a local
   fake HTTPS server on a local CA (`--outbound-ca`, `--outbound-base`) that dedupes by key;
   `kill -9` with a call in flight, restart, the fake sees the key twice and applies it once;
   TCP-proxy reset and stall between app and fake driving the transport-failure and timeout
   rows. It joins the nightly job.

## §13 Docs, in the same commits (hard rule 6)

| Doc | Change |
|---|---|
| MANIFESTO §5 | None — it becomes true as written |
| README | :68-71 becomes true; the status section gains outbound; the "not built" list (:156-157) gains HTTP/2 and request signing |
| DESIGN | §1: the sixth trait, the seventh ready source, invariant 6. §2: no change (`What a transaction cannot do` already says it). §5: the loss window covers external effects. §6: replace :1675-1677's "M1's external-effect handlers" sentence with this surface. §7: `describe` fields. :2054's JWKS line reworded (a refresh is now expressible as a global outbound cron job, still unbuilt) |
| GUIDE | §6: an outbound-jobs subsection (`call`/`settle`, keys, Dead's meaning, the money cost of key-ignoring remotes, the deploy hazard) and :1698-1699 corrected. §8: remote models, stock models, axes, checkers. §9: every new refusal. §10: the loss-window case. §15: restore re-fires with the same key. §6 also documents `Host`; §5's "Starting the server" gains the two launch flags |
| DECISIONS | New §14, proposed text in §17 below |
| PLAN | Record this sub-project; v1.x rows per §15 |
| NEIGHBORS | Durable-execution lineage: Temporal (activities; `call`/`settle` is an activity whose workflow is the transaction), Restate, DBOS |
| CLAUDE.md | Hard rule 1's trait list gains `Outbound`; the test-file lists gain `charpente`'s `outbound` and `edge-harness`'s `outbound` (the `doc_inventory` gate enforces it) |
| `db.rs:1456` | The doc comment's "consumed by M1's external-IO surface" corrected |
| BENCH.md | The clean-build compile-time delta of §9's dependencies |
| `examples/BRIEF.md:11` | Unchanged: it describes what that build was given, and stays true of it |

## §14 Compatibility

- **Apps without outbound jobs** are unaffected byte-for-byte (§4 invariant 6, §12 oracle 3).
- **Existing logs and rows** decode unchanged: `0x0c`/`0x0d` are new tags, and `JobRow`'s new
  field is trailing and absent when `None`.
- **Downgrade** is unsupported once an outbound job has been enqueued: an older binary meets a
  `0x0c`/`0x0d` record and refuses it as an unknown tag, by name, as it would any future tag.
- **A job gaining or losing `outbound`** is a shape change, refused while Pending jobs of the
  old shape exist (R8).

## §15 Refused, so the plan does not grow them

- **B** and **C** (R1, R2), a **key derived from job ids or app keys** (R4), a **platform-injected
  key header** (R5), **write access in `call`** (R3).
- **Timers, retries or redirect-following in the edge** (R10).
- **A permissive default sim model** or **an implicit `Host` entry** (R9).
- **Outbound plus `cross_tenant`**, **HTTP/2**, **proxies**, **request signing** (AWS SigV4,
  which SES needs) and **streaming replies** — each becomes a PLAN v1.x row whose trigger is
  the first app that needs it.
- **Inbound webhook dedupe** (duplicate *inbound* delivery is the app's routes and its
  `SimSource` workload), **SMTP or raw TCP**, **failure shrinking**, and **the agent loop**
  (MCP / watch mode) — separate projects.

## §16 Exit criteria

1. **Sim.** Oracles 1, 2, 4 and 5 green; the new properties green over **10,000 universes**
   from fresh master seeds stated in the commit; both planted bugs red with recorded seeds.
2. **Determinism.** Oracle 3: every existing pinned seed and master-seed batch replays
   unchanged.
3. **Mutation.** Oracle 6 catches every viable mutant, counts in the LEDGER, no exclusion added.
4. **Wire.** Oracle 7 green locally and in the nightly job.
5. **Docs-only build.** A fresh agent builds, from the docs alone, an app that sends email and
   calls an LLM, from a brief in `docs/experiments/2026-10-outbound/` (designed in the plan).
   It is reviewed as helpdesk and statuspage were, and passes with zero Criticals the
   platform should have caught. The ambiguous-call check is the reviewer's, on a copy: remove
   the idempotency key from the agent's request, and the agent's *own* sim properties go red
   with a seed under `outbound_lost_reply` plus retry.
6. **Gate.** `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`
   and `cargo test --workspace --no-fail-fast`, green by counting `test result: ok`; CI green.
7. **Docs.** §13 lands in the same commits as the code it describes, and BENCH.md carries
   the compile-time reading.

## §17 Decisions for Noah

1. **DECISIONS §14, new.** Proposed text:

   > ## 14. External effects are kernel-owned two-phase jobs (2026-10-13)
   >
   > An external effect is a job declared `outbound = "<host>"` whose `call` (read-only, by
   > type) decides a request and whose `settle` commits the reply's consequences with the
   > Done-mark as one record; the kernel makes the call between them, outside any
   > transaction, through the `Outbound` IO trait. Rejected: **edge workers** (a worker SDK
   > running async handlers on the tokio edge), because the handler leaves the deterministic
   > boundary and every injected fault stops at its door; and **a call effect recorded inside
   > the transaction** (`tx.call`), because the request freezes at commit, its body rides the
   > WAL, and storing a reply rebuilds `settle` anyway. **Keys are platform-minted** from
   > `Entropy` at staging and recorded in the enqueue effect, because keys derived from ids
   > collide across the DECISIONS §6 loss window's diverged timelines. Reopen if: an app needs
   > an effect whose request cannot be decided from committed state plus its payload, or a
   > protocol that is not request/response.

2. **Sequencing, for the plan.** The trait, `SimOutbound` and the dispatcher's due-set come
   first, with oracle 3 standing from the start, so the determinism guarantee is measured
   before any app-facing surface exists. Then the record tags, key minting and the kernel
   arm; then the `#[job]` surface and `describe`; then the production edge and oracle 7; the
   docs-only build last, against the finished docs. Each phase lands its §13 rows with it.

## Amendments (implementation plan)

- **A1 — `sim.with_remote(host, |m: &M| …)` replaces `remote_ref`.** A reference cannot escape the world's lock. A closure model is wrapped by `charpente_sim::outbound::from_fn(f)`, not a blanket impl over `FnMut`.
- **A2 — `Outbound::next_wake() -> OutboundWake { Unknown, Never, At(Instant) }`, default `Unknown`.** The sim knows when its next completion is due, or that none ever will be (every reply in flight is lost), and says so. Production says `Unknown`, and the kernel parks on a 5 ms outbound tick. Same kernel code either way, as with `Repl::next_deadline`. Without it, a 30 s simulated call costs ~6,000 empty iterations. `Never` is distinct from `Unknown` because a sim whose every reply is lost must park until the in-flight deadline, not tick.
- **A3 — "process generation" is the `SimOutbound` handle.** Each kernel gets its own `sim.outbound(…)` handle, and a completion goes only to the handle that started the call. A restarted process builds a new kernel with a new handle, so a pre-restart reply has nowhere to land. Requests already on the wire still reach the model at their delivery time. No restart hook is needed.
- **A4 — `max_in_flight` is `App::outbound_max_in_flight(n)` (default 64), not an `AppConfig` field.** `AppConfig` has ~30 struct-literal sites. The setting also belongs beside the `Host` entries.
- **A5 — the host-set check runs when the kernel is built, not at mount.** `mount_jobs` and `outbound(Host)` may come in any order. The refusal is a `Refusal`, so `serve` prints "refused to start".
- **A6 — a job that wrote `run` instead of `call`/`settle` gets rustc's E0599 for the missing `call`.** The attribute sits on the struct and cannot see the `impl` block. A compile-fail fixture pins it, and GUIDE §9 lists it.
- **A7 — `Job::OUTBOUND` is a `bool` const, default `false`.** Core needs only "mint and pick the tag"; the host lives on `JobDef`.
- **A8 — `Db::drain_enqueued` returns `Vec<Enqueued { id, outbound }>`.** The flag comes from the record tag at apply. Looking a name up in the store would draw sim read-fault rolls and re-roll every universe (invariant 6).
- **A9 — only App-opened groups can mint.** `App::begin_group` and the kernel's job and cron arms mint. A bare `Db::transact`/`Db::begin_group` that enqueues an outbound job is a contract panic naming the fix. Tests seed outbound jobs through `App::transact_for_tests` (`#[doc(hidden)]`).
- **A10 — the reply-over-cap text says the remote applied the call**, since it answered: `outbound <host>: the reply exceeded the <cap>-byte reply cap — the remote answered, so it applied this call; raise the cap with Host::max_reply_bytes (the retry carries the same idempotency key)`. The text names no length: the production edge stops reading at cap + 1 bytes (Review Focus 4), so it never learns the true length, and the sim and production must print the same line.
- **A11 — TLS identity is always the declared host.** The client keeps `https://<host>[:port]` as the request URI, so SNI and certificate verification use the declared name. `--outbound-base <host>=<url>` changes only where the name resolves (a custom resolver) and the port, as `curl --resolve` does. This adds `tower-service` (a dependency of `hyper-util`'s `client` feature) as a direct dependency.
- **A12 — a path must be visible ASCII, a header name an HTTP token, and a header value free of control characters**; anything else is refused as an attempt naming the rule. This closes header injection (a CR or LF forging a header), which the spec's rules missed.
- **A13 — a new workspace member, `crates/outbound-fixture`.** It holds the app with outbound jobs, its `launch`-driven node binary, and the TLS fake remote that the edge harness drives. Putting outbound jobs in `sim-fixture` or `invoicing` would re-roll their recorded batches (oracle 3) and their goldens.
- **A14 — oracle 3 compares a batch's verdicts, failures (test, seed, message), outcomes and coverage, never `config`.** `config` gains the outbound axes by design. Raw JSON bytes therefore change while every universe stays the same.
- **A15 — `outbound_slow` adds think time drawn uniformly from `[10·hi, 100·hi]`**, where `hi` is `outbound_latency.1`, or from `[1 s, 100 s]` when `hi` is zero. It's drawn before the reply's own latency.
- **A16 — `call` and `settle` panics are absorbed as attempts** with `call panicked: <payload>` / `settle panicked: <payload>`, the prefixes `run`'s `handler panicked:` set.
- **A17 — the no-`Outbound` refusal builds the handle first.** The spec's text, `Kernel::with_outbound(app, clock, Box::new(sim.outbound(&app.outbound_hosts())?))`, does not compile: it moves `app` and then borrows it. The refusal says `let outbound = sim.outbound(&app.outbound_hosts())?; Kernel::with_outbound(app, clock, Box::new(outbound))`.
- **A18 — a refused connection carries its cause.** `ConnectionRefused` is the only "certainly not delivered" kind, and the edge also uses it for DNS and TLS failures before any request byte is sent. The attempt text is therefore `outbound <host>: <the error's own text> — the call did not reach the remote`. The sim's error text is exactly `connection refused`, so the sim reproduces spec §7's example line, and an operator still sees "invalid peer certificate: UnknownIssuer" when TLS is why.
- **A19 — oracle 2's switchover row is pinned at the write gate, not through a two-node rig.** A switchover reaches the kernel only as `Db::forbid_writes` (`Role::HandingOff`, `repl/mod.rs:300`), and invariant 3's drop is level-triggered on writability (Task 11). So Task 11's `losing_writability_drops_in_flight_and_keeps_jobs_pending` is that row. Its mutation proof (Task 11 Step 7) deletes the drop and goes red. A real switchover under outbound load waits for the first replicated app with outbound jobs, as a v1.x row (Task 17).
- **A20 — the docs-only build's ambiguous-call check is a controller gate, run before the review, and its result goes to the reviewer.** Spec §16(5) gave it to the reviewer. But it's a mechanical run whose output is a seed, and this repo's rule is that nothing is believed from a report (Task 20 Step 4).
- **A21 — `sim.outbound(hosts)`'s no-model refusal names the host, not the job (spec §10 names the job).** `sim.outbound` takes the app's host list as plain data, so no job is in view there. The text is the one Task 4 pins.
- **A22 — four corrections found while landing the oracles.**
  1. **The loss window inside one sim box.** §10's "single-node: there is no loss window inside one sim box" is false under durable corruption: a bit flip or misdirected write can destroy a synced enqueue whose call already reached the remote (witness seed `0x81c262542dfce560`, pinned in `crates/charpente/tests/outbound.rs`). So `calls_have_durable_jobs` is excused where `charpente_sim::checks::durable_corruption_fired` holds, and that counts as a give-up, as the acked-write checks in `crates/charpente/tests/transactions.rs` already do. The give-up skips the exactly-once checks for that universe. `durable_corruption_fired`'s doc forbids excusing exactly-once violations; this use stays inside that rule because a loss check, not an exactly-once one, triggers it.
  2. **What `keys_are_per_job` catches.** It catches two jobs that share a key: a constant key, or one derived from the job's data. It cannot see the other two regressions §10 named. An id-derived key never repeats within one store, since job ids come from a monotonic counter; `a_minting_group_draws_16_bytes_per_outbound_enqueue_and_none_otherwise` (`crates/charpente-core/tests/materialize.rs`) catches it, by asserting the draw count and the exact bytes of each recorded key. A key drawn at apply is caught by `a_follower_applies_shipped_outbound_enqueues_with_the_primary_s_keys` (`crates/charpente/tests/replication.rs`), where a follower's `JobRow.outbound_key` equals the primary's byte for byte, and by the same minting test.
  3. **The swarm property's sizes.** It runs 768 universes per batch, not 256: at 256, `fault:outbound_lost_request` missed in about 1 of 200 ambient runs. Its reach is floored on adjudications (`outbound:re-called`, `outbound:re-called-across-restart`), not on universes that ran.
  4. **Plan deviations already landed.** Oracle 4(a)'s first keyless failure is a re-call after a disk fault (apply, then crash); the lost_reply mechanism is pinned by the deterministic `a_keyless_call_whose_replies_are_lost_is_applied_on_every_retry`. `outbound_run_instead_of_call` pins rustc's real text, not A6's E0599.
