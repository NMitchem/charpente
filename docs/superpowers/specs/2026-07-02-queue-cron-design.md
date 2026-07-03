# M0 step 6 — queue + cron + the deterministic edge executor: design

Status: approved (design reviewed with Noah in-session 2026-07-02; the four **[FLAGGED]**
choices below were presented and stand unless he reopens them). Supersedes the raw
`(seq, idx)` jobs substrate from step 4.

## Goal

PLAN.md step 6: transactional enqueue is already real (step 4); this step makes jobs
*run*: post-commit dispatch, typed retry policy, cron ticks as transactions — and the
deterministic edge executor (DECISIONS §10) that DESIGN §1's same-shape rule now
governs: the kernel loop gains its second and third event sources, and the "which ready
source next" decision is one piece of code, identical in production and simulation.

## Ruled decisions

1. **Handlers are pure state transitions** (Noah, in-session). A `#[job]` handler is
   `fn run(self, tx: &mut Tx) -> Result<(), JobError>` — it *is* a transaction, run
   inline on the kernel thread. Its effects and the job's Done-mark commit atomically in
   one log record: **exactly-once state effects, structurally.** External IO in handlers
   is M1 (with a typed outbound surface); the M0 demo's SendReceipt records state.
2. **Idempotency keys are external identity only** (Noah, in-session): "the stable
   identity a job presents to the world outside the log. Inside the deterministic
   boundary it should mean nothing, because the structural guarantee already covers
   everything there." So: `tx.enqueue_keyed(key, job)` stores the key on the job row;
   the platform never reads it in v0 (M1's external-IO surface consumes it). No
   enqueue-dedupe, no key index, no uniqueness semantics. Client-retry dedupe inside
   the boundary is an app-level read in the same serializable transaction — document
   that idiom, don't duplicate the guarantee.

## The kernel event loop (`charpente::kernel`, deterministic)

New module inside the deny-list fence. `Kernel` owns the `App` (Db + router +
sessions), a `Dispatcher`, a `CronScheduler`, `Box<dyn Clock>`, and uses the App's
entropy. One trait crosses to the edge:

```rust
pub type Reply = Box<dyn FnOnce(Response) + Send>;

pub enum Wake { Ready, Deadline, Shutdown }

pub trait RequestSource {
    /// Non-blocking; a returned request must eventually get its Reply called.
    fn poll(&mut self) -> Option<(Request, Reply)>;
    /// Parks until a request arrives (Ready), the deadline passes (Deadline),
    /// or the source is exhausted/disconnected (Shutdown). `None` deadline
    /// means park indefinitely. Must not drop requests: a request observed
    /// during the park is surfaced by the next `poll`.
    fn wait_until(&mut self, deadline: Option<Instant>) -> Wake;
}
```

Loop body — one function, compiled once, used by prod and sim:

1. `now = clock.now()`. Build the ready set in fixed order: pending request
   (`source.poll()` result, stashed), due job (`dispatcher.due(now)`), due cron tick
   (`cron.due(now)`).
2. Empty → `source.wait_until(earliest of dispatcher/cron deadlines)`; on `Shutdown`,
   return (jobs still Pending re-dispatch on next open — durability by design; a
   shutdown is crash-equivalent and safe). Else continue.
3. One ready → service it. Several ready → **one entropy draw picks uniformly** — the
   same-shape decision site. The draw happens only when contended, so idle iterations
   consume no entropy.
4. Servicing anything is at most one committed transaction (a failed job attempt is an
   aborted transaction plus a committed failure-record — see lifecycle). Serializability
   is untouched: the kernel is the single writer.

Prod: `serve.rs`'s kernel thread becomes `Kernel::run(ProdSource { rx })` — `poll` =
`try_recv`, `wait_until` = `recv_timeout`/`recv` (stashing an arriving request),
`Shutdown` on channel disconnect. The hyper side is unchanged. Sim: a `SimSource` the
workload scripts (queue of requests + Reply closures capturing responses);
`wait_until` advances virtual time to the deadline; `Shutdown` when the next wake
(arrival or deadline) lies beyond the source's explicit **horizon** (a mandatory
`SimSource` parameter — a registered cron always has a `next_due`, so deadlines never
drain on their own; the horizon is the universe's end-of-time, and every cron-mounting
sim test must choose one).

Time as data: transactions still never read a clock. The kernel computes `now` and
backoff/next-due values *outside* the transaction and writes them in as row data —
replay-stable because they're effects in the log, honoring "no ambient reads inside
transactions" without a Tx clock surface.

## Job lifecycle — one effect vocabulary, everything through the log

**The `Job` platform table moves into `charpente-core`** (it is write-model substrate:
the enqueue effect materializes it at apply, so core must know its codec — same
reasoning that put `charpente.jobs` in core at step 4; `Session` stays in charpente
because it is HTTP surface). `Db::open` registers it in the Schema itself.

```rust
#[derive(...)] // core-defined Row, not via the macro (core cannot use #[table])
pub struct Job {
    pub id: Id<Job>,            // from a monotonic next_job_id meta counter,
                                // bumped at apply — deterministic across replay
    pub name: String,           // #[job] type name, resolved via the job registry
    pub payload: Vec<u8>,       // Codec bytes of the #[job] struct
    pub key: Option<String>,    // external identity; unread by the platform in v0
    pub status: JobStatus,      // Pending | Done | Dead
    pub attempts: u32,
    pub due_at: u64,            // nanos, kernel-logical time; 0 = immediately due
}
```

- **Enqueue effect (tag 0x02) extends to `[job][payload][key: Option<String>]`.**
  Pre-release: the v0 record format changes in place, no compatibility shim. The raw
  `charpente.jobs` redb table and `Writer::enqueue_raw`/`Snapshot::jobs()` are replaced
  by `Job` rows (tests migrate to `scan::<Job>()`).
- Apply materializes a `Job` row per enqueue effect: `id` from the meta counter
  (atomic with `applied_seq` in the same store transaction), `status: Pending`,
  `attempts: 0`, `due_at: 0`.
- `Db` records the job ids materialized by the last submit;
  `Db::drain_enqueued(&mut self) -> Vec<Id<Job>>` lets the kernel update its due-heap
  incrementally after each serviced transaction. At open, the `Dispatcher` rebuilds the
  heap by scanning Pending rows — the store remains the only truth.
- **Executing a due job is one transaction:** re-read the row (assert Pending and due —
  a violated assert here is a dispatcher bug, not app error), decode the payload via
  the registry, run the handler; on `Ok` the same transaction puts `status: Done` —
  the atomic Done-mark. On `Err`, that transaction aborts wholesale (handler effects
  discarded) and a second transaction commits the failure record: `attempts += 1`,
  then `due_at = now + backoff(attempts)` or `status: Dead` at the policy cap.
- Crash anywhere: an unacknowledged attempt is invisible; the job stays Pending and
  re-dispatches after recovery. At-least-once execution, exactly-once committed
  effects.
- **[FLAGGED] Handler panics:** `catch_unwind` at the job boundary converts a panic
  into `Err` so the retry/Dead path absorbs poison-pill jobs instead of crash-looping
  the server. Deterministic; the aborted `Tx` is discarded safely. A deliberate,
  contained softening of let-it-crash — the panic message is preserved in the failure
  record.
- **[FLAGGED] Done rows are retained** for observability; GC is a later question, filed
  alongside WAL segment GC.

## Retry policy, `#[job]`, registry

```rust
#[job(retry(max = 5, backoff = "30s"))]   // defaults if omitted: max = 5, backoff = 30s
struct SendReceipt { invoice: Id<Invoice> }

impl SendReceipt {
    fn run(self, tx: &mut Tx) -> Result<(), JobError> { ... }
}
```

- `#[job]` derives the field-wise `Codec` (same generation as `#[table]`'s row codec,
  minus the id requirement), `Job::NAME`, a `RetryPolicy` const
  (`{ max: u32, backoff: Duration }`; backoff string accepts `ms/s/m/h` suffixes,
  parsed at macro expansion), and a `JobDef` marker for the registry.
- Backoff schedule: `backoff * 2^(attempts - 1)`, attempts counted from 1. Dead at
  `attempts == max`.
- `JobError` preserves the App/Io distinction `TxError` exists for (review finding,
  2026-07-02): `enum JobError { App(String), Io(io::Error) }`, `From<io::Error>` → `Io`,
  so handler `?` on tx reads works AND platform faults stay platform faults. The
  dispatcher routes `TxError::Io` and `TxError::App(JobError::Io)` identically: **not an
  attempt** — the heap entry is restored, the row stays Pending untouched, no ledger
  write, the error bubbles (restart-and-recover). Only `JobError::App` feeds
  retry/Dead. Rationale: platform faults must not consume retry budget — at-least-once
  execution would silently break, and once latent read errors land in the fault matrix
  (DESIGN §1 tracked extension) injected faults would Dead-letter healthy jobs with the
  failure misattributed. Two deliberate App-side assignments: handler panics (a panic is
  the handler's own deterministic bug — the poison-pill-absorption policy) and payload
  decode failures (schema drift is deterministic; retrying it forever would be worse
  than dead-lettering it with a named message).
- Registration mirrors routes (distributed registration stays rejected):
  `jobs![SendReceipt, Nightly]` → `Vec<JobDef>` where
  `JobDef { name, retry, glue: fn(&mut Tx, payload: &[u8]) -> Result<(), JobError> }`;
  mounted via `App::mount_jobs`. A log-replayed or dispatched job whose name is not
  registered is a contract-violation error naming the job and the fix (mirror the
  Schema unknown-table message).

## Cron

`charpente.cron` platform table (lives in `charpente::kernel`, registered by
`App::open` like `Session`): `id`, `name` (unique — duplicate registration is a
mount-time panic, like route conflicts), `interval_nanos`, `next_due`.

- **[FLAGGED] v0 registration is a builder, not a macro:**
  `.schedule("nightly", every_24h, || Nightly {})` at App construction; rows upsert
  idempotently at open (new name → row with `next_due = now + interval`; existing →
  interval updated, `next_due` kept).
- A due tick is one transaction: advance `next_due` and enqueue the job through the
  normal enqueue path — cron is a recurring enqueuer; ticks inherit retry, status, and
  observability with zero extra machinery.
- **[FLAGGED] Catch-up: fire once, no backfill.** Whenever `next_due <= now` (including
  immediately after recovery from downtime), tick once and set
  `next_due = now + interval` (not `next_due + interval`). Crontab expressions and
  backfill policies are deferred.

## serve.rs reshape

The kernel thread's `while recv` loop is deleted; the thread runs
`Kernel::run(ProdSource { rx })`. Everything else at the edge (hyper, body cap, wire
translation) is unchanged. `App::handle` remains the request-servicing path (the kernel
calls it), so existing kernel-level tests keep working; new tests drive `Kernel` with a
`SimSource`.

## Testing (sim-first, swarm harness, TDD)

Flagship property — the M0 demo centerpiece: a counter table written only by handlers;
under the full fault matrix + kill-at-any-point + restarts, **counter rows ≡ Done jobs,
exactly** — never one more (no double-apply), never one less (no lost ack). Plus:

- **Retry:** a handler that fails N times then succeeds (failure schedule seeded via
  sim entropy): attempts monotone, due_at follows the doubling schedule in virtual
  time, Done at the end; a always-failing handler goes Dead at exactly `max`, with the
  dead-letter message carrying name/id/attempts/last error.
- **Panic absorption:** a panicking handler follows the same retry path; the panic
  message lands in the failure record; the kernel loop survives.
- **Cron:** DESIGN §6's promise, literally — an every-30-days job over ~10 simulated
  years ticks ~120 times in a subsecond test; downtime (crash + restart after several
  intervals) produces exactly one catch-up tick.
- **No starvation:** under a continuous request flood with a due job waiting, the job
  is picked (entropy pick gives probabilistic progress); proven with a sometimes-
  coverage key rather than a flaky timing bound.
- **Key round-trip:** `enqueue_keyed` → row carries the key; platform ignores it.
- **Determinism (folds followup #24):** same seed → identical (status, body) response
  sequence, identical durable fingerprint, identical Job-table state; the transactions
  property workload's `checkpoint().is_err()` routes through `is_injected`.
- **Negative-space meta-test** (c3026b2 tradition): a deliberately wrong executor that
  commits handler effects and the Done-mark in *separate* transactions must fail the
  flagship property under crash injection — proving the harness would catch the
  dual-write bug this design deletes.
- Coverage `require:` keys on the batch: `job:done`, `job:retried`, `job:dead`,
  `cron:tick`, `cron:catchup`, plus the standard `fault:*`/`crash:*` set.

## Error surface (hard rule 5)

Dead-letter, unregistered job name, duplicate cron name, empty idempotency key, and
dispatcher-invariant violations all name the contract, the site, and the fix; each has
a test asserting the message content.

## Non-goals

Edge workers / external IO in handlers (M1 — where keys get read); crontab
expressions; backfill; job priorities; Done-row GC; a requeue-dead API (query the Job
table manually in v0); serve.rs hardening (followup #25, separate task); WAL segment
GC (its own fence, DESIGN §2).
