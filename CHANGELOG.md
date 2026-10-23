# Changelog

## 0.3.0 (unreleased)

## 0.2.0 (2026-10-17)

### Added: outbound effects
A job declared `#[job(outbound = "<host>")]` can call an external service: email, a payment API, an LLM.
- Its `call` is read-only and decides the request. The kernel then makes the request outside any transaction, through the `Outbound` IO trait.
- Its `settle` commits the reply's consequences and the job's Done-mark as one record.
- Every outbound job carries an idempotency key the platform mints from `Entropy` when the enqueue is staged.
- The simulator injects outbound faults: lost requests, lost replies, slow replies, throttling (a 429 or 503) and refused connections. `charpente_sim::checks` gains `calls_have_durable_jobs`, `keys_are_per_job` and `outbound_settles_by_horizon`.
- Production calls go through a hyper + rustls (`ring`) client at the edge, configured by `OutboundOptions` through `serve::serve_with` and by the `--outbound-base`/`--outbound-ca` flags.
- See GUIDE §6 "Outbound jobs: calling the world after commit", §8 "Calling the outside world in a sim", §13 "Outbound jobs and hosts in the contract", and DECISIONS §14.

### Breaking changes
- **`JobOutcome` gains `Calling`.** An exhaustive `match` over it no longer compiles until it handles the new variant. That is deliberate: charpente keeps its public enums exhaustive so that a new outcome is a compile error, not a silent wildcard.
- **Struct literals:**
  - `JobDef` gains `outbound`.
  - `JobRow` gains `outbound_key`.
  - `KernelStats` gains five fields: `outbound_started`, `outbound_settled`, `outbound_failed`, `outbound_timed_out` and `outbound_late`.
  - `FaultConfig` gains six outbound fields: `outbound_refused`, `outbound_lost_request`, `outbound_lost_reply`, `outbound_throttle`, `outbound_slow` and `outbound_latency`. An exhaustive literal breaks; `..FaultConfig::none()` still works.
  - `describe::Contract` gains `outbound_hosts`, and `describe::JobInfo` gains `outbound`.
- **Signatures:**
  - `Db::drain_enqueued` returns `Vec<Enqueued>` instead of `Vec<Id<JobRow>>`, and `Dispatcher::note_enqueued` takes it.
  - `table::Writer::materialize_job` gains an `outbound_key: Option<[u8; 16]>` parameter.
  - `Db::register_job_shapes` takes a 4-tuple; the new fourth element is the job's outbound host.
- **Replication:** `PROTO_VERSION` goes from 5 to 6, so a 0.1 node and a 0.2 node refuse each other at Hello. Upgrade the way GUIDE §12's "The upgrade procedure" describes, with both nodes stopped.
- **Downgrade:** unsupported once an outbound job has been enqueued. A 0.1 binary refuses the outbound enqueue record as an unknown effect tag, and reads a job row that carries an idempotency key as corrupt.
- **Refusals that are new or reworded** (error text is product surface):
  - An enqueue whose `Job::OUTBOUND` (a new associated const, `false` by default) contradicts the mounted `JobDef` is refused at enqueue, by name, before any key is drawn (new). Only a hand-written `impl Job` or `JobDef` can reach it.
  - The `Db::register_job_shapes` panic for a job registered with two declarations now also says, for each, whether it is a pure job or `outbound = "<host>"`.
  - The `enqueue_keyed` empty-key panic now calls the key an `enqueue_keyed` app key, not an idempotency key.
  - The `#[job]` argument errors that quote the accepted forms now list `outbound = "<host>"` and `timeout` among them.
  - An app whose jobs call no host refuses to start when given `--outbound-base`, or an `--outbound-ca` file that cannot be read or holds no PEM certificate. 0.1 had neither flag and rejected both as an unknown verb.
  - The new outbound compile errors and contract panics are listed in GUIDE §9.
- **Output formats:**
  - `describe --format human` gains ` outbound=…` on each job line and an `outbound_hosts:` section.
  - The JSON gains `outbound` on each job and a top-level `outbound_hosts`.
  - A sim failure's `config:` line gains the six new `FaultConfig` fields.
  - `LAUNCH_USAGE` gains a third line for the serve form's outbound flags.
- **Tooling skew:** a 0.2 CLI's `gen-client` cannot read a 0.1 app's `describe` output, because it requires `outbound_hosts`. Upgrade the CLI and the app together.

## 0.1.0 (2026-10-10)
First release.
