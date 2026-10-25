# Changelog

## 0.3.0 (unreleased)

### Breaking changes
- **`Clock` gains `wall()`; an implementor adds it.** `WallTime` is new.
- **Every stored or compared time is a `WallTime`; durations stay on `Instant`.**
  - Fields: `Session.expires_at`, `ApiKey.created_at`, `CronRow.next_due`, `JobRow.due_at` (`WallTime::UNIX_EPOCH` still means due now), `SessionSweep.before`, and `JobOutcome::Retried`'s `due_at`.
  - `App::handle_at`, `App::handle_or_subscribe` and `StagedDispatch::stage_or_subscribe` take `now: WallTime`. `App::handle` passes `WallTime::UNIX_EPOCH`.
  - `OidcVerifier::verify` takes `now: WallTime`.
  - `CronEnqueue` is `Box<dyn Fn(&mut Tx, WallTime) + Send>`. `CronScheduler::upsert`, `tick`, `tick_into` and `has_due` take a `WallTime`, and `next_deadline` returns one.
  - `Dispatcher::run_due` and `run_due_into` take the new `kernel::Now`, one reading of both clocks: `mono` times an outbound call's deadline and `wall` judges due times. `Dispatcher::has_due` takes a `WallTime`, and `next_deadline` returns one.
  - The fingerprints of the platform tables `Session`, `ApiKey` and `CronRow`, and of the platform job `SessionSweep`, change with their field types.
- **A 0.2 data directory, replication peer or archive bucket is refused by name, with the fix in the text.** Nothing is migrated (DECISIONS §15).
  - A data directory whose platform table (`Session`, `ApiKey`, `CronRow`) has another shape: start from an empty directory or restore a 0.3 archive.
  - `PROTO_VERSION` goes from 6 to 7: a 0.2 peer is refused at Hello; replace it with a fresh follower.
  - `Manifest` gains `format`, and `MANIFEST_FORMAT` is 3: `restore` and archive adoption refuse a manifest with another format, or none; archive to a fresh bucket prefix.
- **`CronRow.interval_nanos` is the interval the row's `next_due` was set with.** `CronScheduler::upsert` no longer rewrites it on an existing row; the next tick writes it with `next_due`.
- **The simulator's clock axes.** `FaultConfig` gains `clock_skew`, `clock_step_back` and `clock_downtime`: an exhaustive literal breaks, and `..FaultConfig::none()` still works. `checks::FaultReason` gains `ClockRefused`, so an exhaustive `match` over it must handle it.
- **`run_universes_swarm_with!`'s `adjust` takes the universe's seed: `|seed, cfg| …`, not `|cfg| …`.** A value an adjust draws comes from a stream keyed by the seed, so a change to `FaultConfig` no longer moves it. An adjust that ignores the seed draws nothing new, and no platform suite's pinned seed moved.
- **`Sim::give_up` takes a fixed reason and a per-run detail: `give_up(reason: &'static str, detail: impl Display)`.** The CLI groups give-ups by reason and `--seed` shows the detail. Split a text that embeds an error, count or seq into its fixed part and the detail; a fixed reason passes `""`, and an empty detail is none. `Sim::gave_up()` returns `Option<&'static str>`, `Sim::gave_up_detail()` returns `Option<String>`, and `Outcome::GaveUp` is `{ reason: &'static str, detail: Option<String> }`.

### Changed
- **The `charpente new` scaffold's sim draws its inputs and proves the event.** It posts 1 to 4 notes whose bodies and order come from `sim.input()`, so a failure shrinks to the fewest, simplest notes. A third check, `acked_post_implies_its_event_after_recovery`, requires each acked note's `NoteAdded` in the durable event log exactly once after recovery, so deleting `tx.emit` now goes red. The scaffold's recorded batch moves (named step S4).
- **Cron keeps its grid.** A tick advances `next_due` to the schedule's next grid slot, `next_due + interval * ((now - next_due) / interval + 1)`, not to `now + interval`, so a late tick no longer drifts the phase. Catch-up still fires once.

### Added
- **Wall time never runs backwards within a node.** The kernel holds its wall reading at the latest value it used when the clock steps back by up to `CLOCK_TOLERANCE` (60 s), and stops by name, with both fixes in the text, beyond that. It also stops by name (`clock stalled: …`) when its clock has read at or behind the held value for more than the tolerance since that value last rose, so a clock that stops advancing stops the node instead of holding sessions open and schedules still. Each hold episode, a step back or a freeze, costs at most the tolerance before a named stop. A clock that runs slow, and steps within the tolerance adding up, pass every check and drift against other clocks (an identity provider's, another node's): keep NTP on. A node opening, whatever its role, or being promoted, is refused by name while its clock is more than the tolerance behind the latest time its data directory recorded (an API key's `created_at`, a schedule's last tick or its creation); a switchover's target completes the hand-off and its kernel stops with the same refusal. See GUIDE §7 "Clock errors".
- **The simulator models wall time and its faults.** A clock's `wall()` is `Sim::true_wall()` (virtual time plus 2026-01-01T00:00:00Z) plus its offset, and `SimHost::clock()` gives each host its own. Three axes act on the offset: a skew each clock draws once, within ±`clock_skew` (up to 120 s), a step back of up to 120 s at a wall read, and a forward jump of up to three days at a power cycle (downtime). Each clock's skew, steps and jumps draw on streams of its own, so two hosts' clocks disagree, and the steps and jumps compound over a run; `SimClock::offset` reads a clock's offset without drawing. `charpente test --sim` turns each on in a quarter of runs, and they mark `clock:skewed`, `clock:stepped-back` (with `:within-tolerance` or `:beyond-tolerance` by size) and `clock:downtime` when a reading shows them. The platform's own suites leave them off, so none of their pinned seeds moved. `checks::clock_refused` names the kernel's clock stops and its open/promote refusal; `checks::clock_fallout` excuses one only where a clock axis acted in the run, and `is_fault_fallout` never does. `Sim::quiesce_clock_faults` freezes every clock's offset for a fault-free tail, which waits out a refusal until the instant it names (`checks::clock_reopens_after`; `None` for the stall stop, which names none). See GUIDE §8 "Wall time and its faults".
- **`sim.input()` draws a workload's inputs from their own stream and records them.** `between`, `pick`, `shuffle` and `text` consume raw `u64` choices, so a smaller choice is a simpler value (`lo`, the first item, identity order), and a run's `choices` and `drawn` (each input by name) come back on its end or failure for the harness to replay and shrink. Drawing moves no fault draw. See GUIDE §8 "Drawing inputs".
- **A failure shrinks to the smallest inputs and fewest fault axes that still fail its check.** `charpente test --sim` replays each check's first failure in a batch, in seed order, with smaller input choices and with each fault axis that was on turned off, and keeps the smallest run that still fails the check, with any message. Its budget is 500 replays, counted, never timed, so the result is the same for any `--jobs`. The failure block gains `inputs:` (each draw by name), `shrunk:` (draws before and after, the axes turned off, the replays) and, when the shrunk run failed with another message, `first:` with the original; its headline, `config:` and `fired:` are the shrunk run's, and its repro replays exactly that run. Two new flags, both requiring `--seed`, carry it: `--inputs HEX` (each choice as LEB128, in lowercase hex) and `--faults-off AXIS,…` (`FaultConfig` field names). A `--seed` replay shrinks nothing. `--format json` still records each failure's own unshrunk run. See GUIDE §8 "Reading a failure, replaying its seed" and "Shrinking a failure".

### Fixed
- Production time was process uptime, so after a restart or failover cron stalled until uptime caught up, sessions outlived their TTL, an expired OIDC token was accepted and any token with nbf was refused. Every stored or compared time is now wall-clock time (WallTime).

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
