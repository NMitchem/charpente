# Launch demo, step 2: the judge — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a stack-neutral, black-box judge that drives any deployment of task 0's HTTP API under kill -9, connection and email-provider faults, and scores it on the spec's classes. It is proven against a known-good reference app, one known-bad victim per class, and a do-nothing app.

**Architecture:**
- **A pure scoring core.** It reads a recorded `RunLog` (client events, provider effects, final state) and turns it into violations, a vacuity verdict and availability. It needs no Docker and is unit-tested to the rule.
- **An impure shell around it.** The shell runs a compose deployment, injects faults from a seeded schedule, drives the workload through a fault proxy, plays the email provider over TLS, and writes the `RunLog`.
- **One fixture app.** A single Rust binary selected by an env var serves as the reference, every victim, and the do-nothing app. That makes each victim the reference with exactly one bug.

**Tech Stack:**
- Rust 1.99, edition 2024.
- The judge: `clap` (CLI), `serde` and `serde_json`, `reqwest` (blocking, rustls), `hyper` 1.x with `tokio` and `tokio-rustls` (the provider), `rcgen` (the CA), `rand` with `rand_chacha` and `rand_distr` (schedules), `sha2` (stream keys).
- The fixture: `axum`, `tokio`, `rusqlite` (bundled), `reqwest`, `sha2`, `hex`, `rand`.
- Docker with Compose v2.
- Versions are the latest at implementation time, pinned by each crate's `Cargo.lock`.

**Spec:** `docs/superpowers/specs/2026-11-06-launch-demo-design.md` (in the charpente repo; §4, §5 and §10 govern this plan).

**Scope note:** Speed measurement (spec §5, "Speed") is deferred to the rehearsal plan. It is secondary in the spec, and no rehearsal purpose needs it. Task 1 (spec §4) is out of scope too.

## Global Constraints

- **Repo.** A new repository at `~/Documents/GitHub/judge`. **`judge` is a working name until Noah picks one** (spec §10), so keep the name in `Cargo.toml` `name`, the binary name and README only, so a rename stays a find-and-replace. Creating the GitHub remote and pushing are Noah's steps.
- **No dependency on the charpente workspace.** Code may be copied from it; when copying, note the source path in a comment. The judge does not run or link charpente.
- **License** `MIT OR Apache-2.0` (two license files), as charpente's. Any contact address is `noah@noahmitchem.com`. No employer, past or present, is named or alluded to anywhere.
- **Fault classes, fixed before the rehearsal:**
  1. kill -9 on any service of the deployment, then a restart;
  2. client-to-app connection faults: severed connections, refused connects, and replies lost after the request reached the app. Unanswered requests are re-sent;
  3. email provider faults (dropped replies after the effect landed, delays, 503 and 429, outages up to the brief's bound).
  - **No clock-step class in v1.** Task 12 records the reason in the README.
- **Fresh faults every run.** Every run's faults come from a printed seed. The README states that a black-box run is not byte-for-byte replayable: the seed reproduces the schedule, and the log is the evidence.
- **Preregistered values.** Every value in Task 2's constants table is preregistered. After the rehearsal starts, a change needs a written reason in the README's changelog, and both arms get re-judged under it (spec §3).
- **Exit codes:**
  - 0: every run passed the gate with zero violations;
  - 1: any violation, or any run that failed the gate;
  - 2: a usage error, or the judge's own infrastructure failing (Docker unreachable, an invalid compose file, the clock offset or host-sleep guards).
  - A deployment that never becomes ready is a gate failure (exit 1), not an infrastructure error.
- **Docker Desktop (or a Docker engine) must be running** for every test marked `#[ignore = "docker"]`. Those tests run with `cargo test -- --ignored`; plain `cargo test` never needs Docker.
- **The project gate** is `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test --workspace --no-fail-fast`, and the same three with `--manifest-path fixtures/task0-app/Cargo.toml`. Green means counting: the number of `test result: ok` lines equals the number of `test result:` lines. The shell is zsh, so use `${pipestatus[1]}`.

## Review Focus

1. **Docker Desktop's VM clock drifts from the host's** (after laptop sleep, especially). Due times are judged on the host clock while the app reads the VM's. Expected: before each run the judge measures the offset (`docker run --rm alpine:3 date +%s%3N` against the host) and refuses the run (exit 2) when |offset| > 500 ms, naming the offset and the fix (restart Docker Desktop). Pinned in Task 7.
2. **The host sleeps mid-run.** Phase timings become garbage, but would still be scored. Expected: the run loop records a tick every 250 ms. A gap over 5 s voids the run (exit 2, `host paused for <n> s at <phase>; this run measured nothing — rerun with --seed 0x…`). A void run is reported, never scored, and the rule is fixed before the rehearsal. Pinned in Task 10.
3. **A 2xx whose body does not match the brief** (for example, a create answered 201 with no `booking.id`). Expected: the judge never panics. Lost writes and double applies are keyed by the judge's own `client_ref`, not the app's id, so they are still detected. Pinned in Task 3.
4. **An app slower than the client timeout** (an answer after 5 s). Expected: the request is recorded unanswered and re-sent in the tail. It is not a violation. Pinned in Task 9.
5. **A stale compose project left by a crashed judge run.** Expected: every run starts with `docker compose -p <project> down -v --remove-orphans` on its own project name, so no state leaks between runs. Pinned in Task 7.

---

## File Structure

```
judge/
  Cargo.toml                 workspace: members ["crates/judge"], exclude ["fixtures/task0-app"]
  README.md                  what it is, quick start, submission contract, honesty contract, clock decision, preregistration log
  LICENSE-MIT, LICENSE-APACHE, .gitignore
  tasks/task0/BRIEF.md       the task brief agents receive (API, obligations, timing, email, deployment contract)
  scripts/proof.sh           runs the §5 proof (Task 12)
  crates/judge/
    src/main.rs              CLI entry: parse, dispatch, exit code
    src/cli.rs               argument types and parsing (clap derive)
    src/lib.rs               module tree
    src/runlog.rs            RunLog and its parts (serde)
    src/score.rs             obligations, the seven classes, the gate, availability (pure)
    src/schedule.rs          seeded streams and the fault schedule (pure)
    src/ca.rs                CA and leaf certificate generation
    src/provider.rs          the fake email provider (HTTPS, idempotency, faults, effect log)
    src/proxy.rs             TCP fault proxy (copied from charpente's crates/edge-harness/src/proxy.rs)
    src/deploy.rs            docker compose control, override file, host-gateway IP, VM clock offset
    src/task0/mod.rs         task 0 constants, Template, obligation offsets
    src/task0/workload.rs    the task 0 client: requests, probes, re-sends, final-state collection
    src/run.rs               one run's phases, N runs, `check`
    src/report.rs            human and JSON reports, verdict line, rerun command
    tests/cli.rs             usage and exit codes (no Docker)
    tests/brief_truth.rs     BRIEF.md states every Task 2 constant
    tests/provider.rs        provider over real TLS, in-process
    tests/proxy.rs           proxy behaviour over loopback
    tests/workload.rs        workload against the reference app binary (no Docker)
    tests/deploy.rs          #[ignore = "docker"] compose control, host-gateway reachability, clock offset
    tests/proof.rs           #[ignore = "docker"] the §5 proof
  fixtures/
    task0-app/               standalone crate (own Cargo.lock): reference, victims and do-nothing behind JUDGE_FIXTURE_MODE
      Cargo.toml, Dockerfile
      src/main.rs src/lib.rs src/config.rs src/db.rs src/routes.rs src/worker.rs src/mail.rs src/mode.rs
      tests/http.rs tests/outbox.rs tests/modes.rs
    targets/<mode>/compose.yaml   one per mode: reference, lost_write, missing_effect, duplicate_effect,
                                  effect_not_owed, double_apply, cross_tenant, unauthenticated, do_nothing
```

---

### Task 1: Repository and CLI surface

**Files:**
- Create: `Cargo.toml`, `.gitignore` (`/target`, `/fixtures/task0-app/target`, `/judge-out`), `LICENSE-MIT`, `LICENSE-APACHE`, and a stub `README.md` (title and one paragraph).
- Create: `crates/judge/Cargo.toml` (lib plus bin `judge`), `src/main.rs`, `src/cli.rs`, `src/lib.rs`.
- Create: `crates/judge/tests/cli.rs`.

**Interfaces:**
- Produces, in `cli.rs`:
  - `enum Command { Run(RunArgs), Check(CheckArgs), Score(ScoreArgs) }`
  - `struct RunArgs { task: Task, target: PathBuf, runs: u32, seeds: SeedSpec, out: PathBuf, format: Format }`
  - `struct CheckArgs { task: Task, target: PathBuf }`
  - `struct ScoreArgs { log: PathBuf, format: Format }`
  - `enum Task { Task0 }`, spelled `task0` on the command line
  - `enum SeedSpec { Fresh, Master(u64), Single(u64) }`
  - `enum Format { Human, Json }`
  - `fn parse(args: &[String]) -> Result<Command, UsageError>`
  - `struct UsageError(pub String)`
- Produces, in `main.rs`: exit 2 on a `UsageError`, printing its text and then the usage.

**Flags:**
- `judge run --task task0 --target DIR [--runs N] [--master-seed 0x…] [--seed 0x…] [--out DIR] [--format human|json]`
  - `--runs` defaults to the `RUNS` constant (Task 2). `--seed` forces `runs = 1`. `--seed` and `--master-seed` together are a usage error.
  - `--out` defaults to `./judge-out/<UTC timestamp>`.
- `judge check --task task0 --target DIR`
- `judge score --log PATH [--format human|json]`

- [ ] **Step 1: Write the failing tests** in `tests/cli.rs`. Each runs `env!("CARGO_BIN_EXE_judge")` and asserts on the exit code and stderr:
  1. `no_arguments_prints_usage_and_exits_2`: stderr contains `usage: judge run`.
  2. `an_unknown_task_names_the_known_ones`: `run --task task9 --target .` gives exit 2, and stderr contains `unknown task "task9" — known tasks: task0`.
  3. `a_target_without_compose_yaml_names_the_contract`: `run --task task0 --target <empty temp dir>` gives exit 2, and stderr contains `has no compose.yaml` and `README, "Submission contract"`.
  4. `zero_runs_is_refused`: `--runs 0` gives exit 2, and stderr contains `--runs must be at least 1`.
  5. `seed_and_master_seed_together_are_refused`: exit 2, and stderr contains `--seed replays one run; --master-seed derives a batch — pass one`.
  6. `a_seed_that_is_not_hex_names_the_flag`: `--seed 12z` gives exit 2, and stderr contains `--seed expects 0x followed by hex digits`.
- [ ] **Step 2: Run them; they fail** (the binary doesn't exist yet).
- [ ] **Step 3: Implement** `cli.rs` and `main.rs`.
  - The `compose.yaml` existence check belongs to parsing, because it is part of the usage contract.
  - `run`, `check` and `score` dispatch to `todo!()` stubs, replaced in Task 10.
- [ ] **Step 4: Run the tests; they pass. Run the project gate.**
- [ ] **Step 5: `git init`, then commit** with the message `judge: repository, CLI surface and exit codes`.

---

### Task 2: Task 0's brief and constants

**Files:**
- Create: `tasks/task0/BRIEF.md`, `crates/judge/src/task0/mod.rs`.
- Create: `crates/judge/tests/brief_truth.rs`.

**Interfaces:**
- Produces, in `task0/mod.rs`:
  - every constant below, as `pub const NAME: u64` (milliseconds) or `pub const NAME: u32`;
  - `MAIL_HOST: &str`;
  - `enum Template { ReminderEarly, ReminderLate, NoShow }` with `fn wire(&self) -> &'static str` (`reminder_early`, `reminder_late`, `no_show`) and `fn due(&self, starts_at: u64) -> u64`.

**Constants (preregistered values):**

| Name | Value | Meaning |
|---|---|---|
| `EARLY_OFFSET` | 60_000 | `reminder_early` is due this long before `starts_at` |
| `LATE_OFFSET` | 20_000 | `reminder_late` is due this long before `starts_at` |
| `NO_SHOW_AFTER` | 30_000 | `no_show` is due this long after `starts_at`, if the booking is not checked in |
| `GRACE` | 10_000 | an email is on time up to this long after its due time |
| `EARLY_SLACK` | 2_000 | an email up to this long before its due time still counts |
| `RECOVERY_GRACE` | 30_000 | an obligation due during faults has until faults end plus this long |
| `CANCEL_MARGIN` | 2_000 | a cancel or check-in acknowledged this long before a due time voids the obligation |
| `STARTS_AT_MIN` / `STARTS_AT_MAX` | 70_000 / 150_000 | the judge's bookings start this far after creation |
| `LIVENESS_STARTS_AT` | 3_600_000 | the tail's liveness bookings start an hour out (their obligations fall after the run ends) |
| `WARMUP` / `FAULTS` / `TAIL` | 30_000 / 240_000 / 200_000 | phase lengths |
| `RUNS` | 10 | runs per claim (N); M is the phase total, 7 min 50 s |
| `MIN_WARMUP_WRITES` | 10 | gate: acknowledged creates in the warm-up |
| `MIN_OBLIGATIONS` | 10 | gate: obligations owed and judged in the run |
| `TAIL_RECOVERY` | 60_000 | gate: the first acknowledged tail write within this long after faults end |
| `READY_TIMEOUT` | 120_000 | gate: `GET /health` must answer 200 within this long after `up` returns |
| `OUTAGE_MAX` | 30_000 | the longest provider outage an app must survive |
| `CLIENT_TIMEOUT` | 5_000 | an answer later than this is unanswered |
| `MAX_CLOCK_OFFSET` | 500 | host-to-container clock offset above which a run is refused |
| `MAX_HOST_PAUSE` | 5_000 | a gap between run-loop ticks above which a run is void |
| `MAIL_HOST` | `mail.judge.test` | the email provider's host |

**BRIEF.md content** (exact facts; the prose is the implementer's):
- **Product.** A multi-tenant appointment service. Times are Unix milliseconds. Apps use the system clock.
- **Public routes.**
  - `POST /signup` with body `{"org_name": string}` → `201 {"org_id": string, "api_key": string}`.
  - `GET /health` → `200`.
- **Authenticated routes.** All take `Authorization: Bearer <api_key>`, and the key's org must be `{org}`.
  - `POST /orgs/{org}/bookings`: requires the header `Idempotency-Key`. Body `{"client_ref": string, "customer_email": string, "starts_at": integer}` → `201 {"booking": Booking}`.
    - A repeat with the same key and the same body answers 201 with the same booking.
    - The same key with another body → `409`.
  - `GET /orgs/{org}/bookings/{id}` → `200 Booking` or `404`.
  - `GET /orgs/{org}/bookings` → `200 {"bookings": [Booking]}` (all of them).
  - `POST /orgs/{org}/bookings/{id}/cancel` → `200 Booking`.
    - A repeat on a cancelled booking → `200`.
    - On a checked-in or no-show booking → `409`.
  - `POST /orgs/{org}/bookings/{id}/check-in` → `200 Booking`.
    - A repeat → `200`.
    - On a cancelled or no-show booking → `409`.
- **Errors.** A missing or invalid key → `401`. Another org's key or path → `404`. A bad body → `400`.
- **Booking** = `{"id", "client_ref", "customer_email", "starts_at", "status"}`, where `status` is one of `booked`, `cancelled`, `checked_in`, `no_show`.
- **Obligations.** Each booking owes three emails:
  - `reminder_early`, 60 s before `starts_at`;
  - `reminder_late`, 20 s before;
  - `no_show`, 30 s after, owed only if the booking is not checked in by then. At that moment its status becomes `no_show`.
  - A cancel voids all three. A check-in voids `no_show`.
  - An obligation whose due time has passed when the booking is created is skipped.
  - On time means within 10 s after the due time (or up to 2 s before). An email that came due during crashes or outages must go out within 30 s after the last of them ends.
- **Email.**
  - `POST https://mail.judge.test/v1/send` with the header `Idempotency-Key`. Body `{"to": string, "template": "reminder_early"|"reminder_late"|"no_show", "booking_id": string, "org_id": string}` → `200 {"id": string}`.
  - The provider applies a key once: a repeat answers 200 and sends nothing new.
  - Replies may be lost after the email was sent. The provider may answer 503 or 429, or be unreachable for up to 30 s.
- **The judged properties, in prose.**
  - Acknowledged writes survive crashes.
  - Each owed email is sent exactly once, on time.
  - No email goes out for a void obligation, or for a booking that doesn't exist.
  - A repeated request applies once.
  - No tenant reads or changes another's data.
  - Every non-public route requires a key.
- **Deployment contract.** Link to the README's "Submission contract" (Task 12 writes it):
  - service `app` listens on port 8080;
  - the judge injects `JUDGE_MAIL_BASE` (`https://<ip>:<port>`, an IP address, never a name) and `JUDGE_MAIL_CA` (`/judge/ca.pem`);
  - the app must reach host `mail.judge.test` at `JUDGE_MAIL_BASE` and verify TLS for `mail.judge.test` against `JUDGE_MAIL_CA`;
  - data lives in named volumes;
  - any service may be killed with SIGKILL at any moment and is then started again.

- [ ] **Step 1: Write the failing test** `brief_states_every_constant` in `tests/brief_truth.rs`.
  - It reads `tasks/task0/BRIEF.md` and asserts that each phrase in a table of `(constant, phrase)` appears verbatim.
  - Build the phrases from the constants with `format!`, for example `format!("`reminder_early` is due {} s before `starts_at`", EARLY_OFFSET / 1000)`, so changing a constant without the brief fails.
  - Cover `EARLY_OFFSET`, `LATE_OFFSET`, `NO_SHOW_AFTER`, `GRACE`, `EARLY_SLACK`, `RECOVERY_GRACE`, `OUTAGE_MAX` and `MAIL_HOST`.
- [ ] **Step 2: Run it.** It fails, because neither file exists.
- [ ] **Step 3: Write `task0/mod.rs` and `BRIEF.md`,** with the phrases exactly as the test builds them.
- [ ] **Step 4: Unit-test `Template::due`.** Assert `due(ReminderEarly, 100_000) == 40_000`, `due(ReminderLate, 100_000) == 80_000` and `due(NoShow, 100_000) == 130_000`.
- [ ] **Step 5: Run the gate, then commit** with the message `task0: the brief and its preregistered constants, held together by brief_truth`.

---

### Task 3: The run log and the scoring core

**Files:**
- Create: `crates/judge/src/runlog.rs`, `crates/judge/src/score.rs`. The unit tests go in `score.rs`.

**Interfaces:**
- Produces, in `runlog.rs` (all `Serialize`, `Deserialize`, `Clone`, `Debug`):
  - `type Ms = u64`, a Unix millisecond reading.
  - `struct Phases { warmup_start: Ms, faults_start: Ms, faults_end: Ms, run_end: Ms }`
  - `enum Auth { Own, OtherOrg, Missing, Invalid }`
  - `enum ReqKind { Signup, Create, Cancel, CheckIn, Get, List, Health }`
  - `struct ClientEvent { id: u64, kind: ReqKind, path_org: Option<String>, key_org: Option<String>, auth: Auth, client_ref: Option<String>, booking_id: Option<String>, idem_key: Option<String>, sent_at: Ms, answered_at: Option<Ms>, status: Option<u16>, body: Option<serde_json::Value>, resend_of: Option<u64> }`
  - `enum Status { Booked, Cancelled, CheckedIn, NoShow }`
  - `struct Booking { id: String, client_ref: String, customer_email: String, starts_at: Ms, status: Status }`
  - `struct Effect { at: Ms, idem_key: String, org_id: String, booking_id: String, template: Template }`
  - `struct FaultEvent { at: Ms, what: String }`
  - `struct RunLog { seed: u64, ready: bool, phases: Phases, client: Vec<ClientEvent>, effects: Vec<Effect>, final_state: Option<BTreeMap<String, Vec<Booking>>>, faults: Vec<FaultEvent>, refs: BTreeMap<String, String> }`
    - `final_state` is keyed by org id.
    - `refs` maps each `client_ref` to the org that created it.
- Produces, in `score.rs`:
  - `enum Class { LostWrite, MissingEffect, DuplicateEffect, EffectNotOwed, DoubleApply, CrossTenant, Unauthenticated }`, with `fn name(&self) -> &'static str` returning `lost_write`, `missing_effect`, `duplicate_effect`, `effect_not_owed`, `double_apply`, `cross_tenant` and `unauthenticated`.
  - `struct Violation { class: Class, detail: String, events: Vec<u64>, effect_keys: Vec<String> }`
  - `enum GateFailure { NeverReady, TooFewWarmupWrites { got: u32, need: u32 }, NoTailWrite, LateRecovery { after_ms: u64 }, TooFewObligations { got: u32, need: u32 }, NoFinalState }`
  - `enum Owed { Yes, No, Ambiguous }`
  - `struct Obligation { org_id: String, booking_id: String, template: Template, due: Ms, deadline: Ms, owed: Owed }`
  - `struct Exercised { acked_writes: u32, owed_obligations: u32, kills: u32 }`
  - `struct Availability { faults_answered: f64, tail_answered: f64 }`
  - `struct RunScore { seed: u64, gate: Vec<GateFailure>, violations: Vec<Violation>, exercised: Exercised, availability: Availability }`
    - An empty `gate` means the run passed the gate.
  - `Class` and `GateFailure` serialize as their snake_case names (`lost_write`, `too_few_warmup_writes`, …), with `#[serde(rename_all = "snake_case")]` and, on `GateFailure`, `#[serde(tag = "kind")]`. Task 10's JSON and Task 12's proof read these names.
  - `fn obligations(log: &RunLog) -> Vec<Obligation>`
  - `fn score(log: &RunLog) -> RunScore`

**The rules (preregistered; each one is a unit test):**

- **Definitions:**
  - "2xx" is 200–299.
  - "Owner request": `auth == Own` and `key_org == path_org`.
  - "Write": a Create, Cancel or CheckIn.
  - A booking's ref is `client_ref`. Bookings are matched to the judge's creates by `client_ref` within the org from `refs`, never by the app's id.
  - The **acknowledged create** for a ref is the earliest Create for it answered 2xx (original or re-send).
  - The **owner cancel** for booking `b` is the earliest owner Cancel naming `b` answered 2xx, with ack time `ca` and send time `cs`. The **owner check-in** is the same for CheckIn (`ka`, `ks`).
  - An **uncertain cancel** for `b` is any Cancel naming `b` that is unanswered, answered 5xx, or a non-owner request answered 2xx. An uncertain check-in is defined the same way.
- **Obligations.**
  - For each booking in `final_state` whose ref is in `refs`, emit the three templates.
    - `due = template.due(starts_at)`.
    - `deadline = max(due + GRACE, faults_end + RECOVERY_GRACE)`.
  - Skip an obligation when `due` is before the acknowledged create's `sent_at`, or when `deadline > run_end`.
  - `owed`, applying these in order:
    1. `No` when the owner cancel exists with `ca <= due - CANCEL_MARGIN`. For `NoShow`, also when the owner check-in exists with `ka <= due - CANCEL_MARGIN`.
    2. Otherwise, when the final status is `Cancelled` (or `CheckedIn` for `NoShow`):
       - `Yes` when every cancel (or check-in) naming `b`, owner or uncertain, has `sent_at >= due + CANCEL_MARGIN`;
       - otherwise `Ambiguous`.
    3. Otherwise `Yes`.
- **`LostWrite`:**
  - An acknowledged create whose ref has no booking in its org's final list (`detail`: `create acked at <t>; booking <ref> absent after recovery`).
  - An owner cancel whose booking's final status is not `Cancelled`.
  - An owner check-in whose booking's final status is not `CheckedIn`.
- **`DoubleApply`:** a ref that appears more than once in its org's final list. One violation per extra copy.
- **`MissingEffect`:** an `owed == Yes` obligation with no effect for `(org_id, booking_id, template)` whose `at` lies in `[due - EARLY_SLACK, deadline]`.
- **`EffectNotOwed`:**
  - Any effect for an `owed == No` obligation.
  - Any effect whose `(org_id, booking_id)` names no booking in `final_state`.
  - This widens the spec's "effect after cancel" with the ghost email (an effect for a booking that never committed), the dual-write symptom README's guarantee 2 names. It is flagged for Noah.
- **`DuplicateEffect`:** a `(org_id, booking_id, template)` whose effects carry more than one distinct `idem_key`. One violation, with `effect_keys` listing them.
- **`CrossTenant`:**
  - Any event with `auth == OtherOrg` answered 2xx.
  - Any booking in org A's final list, or in a 2xx Get/List body under org A, whose ref `refs` maps to another org.
- **`Unauthenticated`:** any event with `auth` `Missing` or `Invalid`, of a kind other than `Signup` or `Health`, answered 2xx.
- **When `final_state` is `None`,** only `CrossTenant` (its event half) and `Unauthenticated` are judged. Every rule that reads the final state is skipped, so a list route that fails at the end cannot turn every email into a false `EffectNotOwed`. The gate then fails with `NoFinalState`.
- **The gate:**
  - `NeverReady` when `!ready`. Every other check is skipped.
  - `TooFewWarmupWrites` when the acknowledged creates with `sent_at` in `[warmup_start, faults_start)` number fewer than `MIN_WARMUP_WRITES`.
  - `NoTailWrite` when no write with `sent_at >= faults_end` was answered 2xx.
  - `LateRecovery` when the first such write's `answered_at - faults_end > TAIL_RECOVERY`.
  - `TooFewObligations` when `owed == Yes` obligations number fewer than `MIN_OBLIGATIONS`.
  - `NoFinalState` when `final_state` is `None`.
- **Availability:**
  - `faults_answered` is the share of events with `sent_at` in `[faults_start, faults_end)` that have `answered_at`.
  - `tail_answered` is the same share for `sent_at >= faults_end`. Use 1.0 when there are none.
- **Exercised:**
  - `acked_writes` counts 2xx writes.
  - `owed_obligations` counts `owed == Yes`.
  - `kills` counts `faults` entries whose `what` starts with `kill `.

- [ ] **Step 1: Write the failing unit tests.** Add one per rule, plus the boundaries. Build each `RunLog` with a small helper, `fn log() -> RunLogBuilder`, defined in the test module. Required names:
  - `acked_create_absent_after_recovery_is_a_lost_write`
  - `lost_write_is_found_by_client_ref_when_the_201_body_has_no_id` (Review Focus 3: the 201's body is `{"oops":1}`)
  - `owner_cancel_reverted_is_a_lost_write`
  - `owner_check_in_reverted_is_a_lost_write`
  - `two_bookings_for_one_ref_are_a_double_apply`
  - `owed_reminder_with_no_effect_is_missing`
  - `effect_at_the_deadline_is_on_time_and_one_ms_later_is_missing`
  - `effect_more_than_early_slack_before_due_does_not_count`
  - `an_obligation_due_during_faults_gets_until_faults_end_plus_recovery_grace`
  - `cancel_acked_by_due_minus_margin_voids_and_an_effect_is_not_owed`
  - `cancel_acked_inside_the_margin_is_ambiguous_and_judges_nothing`
  - `cancel_sent_after_due_plus_margin_leaves_the_obligation_owed`
  - `check_in_voids_only_no_show`
  - `an_effect_for_a_booking_that_does_not_exist_is_not_owed`
  - `two_keys_for_one_obligation_are_a_duplicate_and_one_key_twice_is_not`
  - `other_orgs_key_answered_2xx_is_cross_tenant`
  - `another_orgs_booking_in_my_list_is_cross_tenant`
  - `missing_or_invalid_key_answered_2xx_is_unauthenticated_except_signup_and_health`
  - `never_ready_fails_the_gate_and_skips_the_rest`
  - `too_few_warmup_creates_fails_the_gate`
  - `a_do_nothing_log_scores_zero_violations_and_fails_the_gate` (every write answered 500, `/health` 200)
  - `no_final_state_judges_no_effect_and_fails_the_gate` (effects present, `final_state: None`: no `EffectNotOwed`, gate has `NoFinalState`)
  - `no_tail_write_and_late_recovery_fail_the_gate`
  - `obligations_past_run_end_are_not_judged`
  - `availability_counts_answered_events_per_phase`
- [ ] **Step 2: Run `cargo test -p judge score`.** They fail, because the functions don't exist yet.
- [ ] **Step 3: Implement `runlog.rs` and `score.rs`.**
- [ ] **Step 4: Run them; all pass. Run the gate.**
- [ ] **Step 5: Commit** with the message `judge: run log and the preregistered scoring core — seven classes, the vacuity gate, availability`.

---

### Task 4: Seeded streams and the fault schedule

**Files:**
- Create: `crates/judge/src/schedule.rs`. The unit tests go in the same file.

**Interfaces:**
- Produces:
  - `fn stream(seed: u64, name: &str) -> rand_chacha::ChaCha8Rng`. The key is `SHA-256(seed.to_le_bytes() ++ name.as_bytes())`, fed to `ChaCha8Rng::from_seed`. Stream names: `kills`, `conn`, `partitions`, `replies`, `outage`, `provider`, `workload` and `run-seeds`.
  - `fn run_seeds(master: u64, n: u32) -> Vec<u64>`, which draws `n` values from `stream(master, "run-seeds")`.
  - `struct Window { start: Ms, len: Ms }`. Offsets are measured from `faults_start`.
  - `struct Kill { at: Ms, service: usize, restart_after: Ms }`
  - `struct Schedule { seed: u64, kills: Vec<Kill>, conn_drops: Vec<Ms>, partitions: Vec<Window>, reply_loss: Vec<Window>, outage: Option<Window> }`. Reply-loss windows come from their own stream, `replies`.
  - `fn draw(seed: u64, services: usize) -> Schedule`
  - `enum ProviderDecision { Accept, AcceptDropReply, Delay(Ms), Status(u16) }`
  - `fn provider_decision(rng: &mut ChaCha8Rng) -> ProviderDecision`

**Distributions (preregistered):**

| What | Draw |
|---|---|
| kills | exponential inter-arrival, mean 40 s, over `[5 s, FAULTS - 10 s]`; `service` uniform over `0..services`; `restart_after` uniform `[0, 3000]` ms |
| connection drops | exponential inter-arrival, mean 30 s, over `[0, FAULTS)` |
| partitions | exponential inter-arrival, mean 60 s; length uniform `[1000, 5000]` ms; a window must end before `FAULTS` |
| reply loss | exponential inter-arrival, mean 45 s; length uniform `[500, 2000]` ms; a window must end before `FAULTS`. While a window is open, requests reach the app and their replies are dropped, which is what re-sends test |
| outage | present with p = 0.8; length uniform `[5000, OUTAGE_MAX]`; start uniform `[10 s, FAULTS - 30 s - len]` |
| provider, per request | `AcceptDropReply` 0.10, `Delay(uniform [0, 2000])` 0.20, `Status(503)` 0.08, `Status(429)` 0.04, otherwise `Accept` |

- [ ] **Step 1: Write the failing tests:**
  - `same_seed_same_schedule`
  - `streams_are_independent`: `draw(s, 1).conn_drops == draw(s, 3).conn_drops`. Changing the service count moves only kills.
  - `kills_stay_inside_their_window_and_name_a_service`
  - `partitions_reply_loss_and_the_outage_end_before_faults_end`
  - `outage_never_exceeds_outage_max`
  - `provider_decision_frequencies_are_within_two_points_of_the_table_over_100k_draws`
  - `run_seeds_are_distinct_and_stable_for_a_master`
- [ ] **Step 2: Run them; they fail.** **Step 3: Implement.** **Step 4: Run them; they pass. Run the gate.**
- [ ] **Step 5: Commit** with the message `judge: seeded streams and the preregistered fault schedule`.

---

### Task 5: CA and the fake email provider

**Files:**
- Create: `crates/judge/src/ca.rs`, `crates/judge/src/provider.rs`, `crates/judge/tests/provider.rs`.

**Interfaces:**
- Produces, in `ca.rs`:
  - `struct Ca { cert_pem: String, /* key held privately */ }`
  - `fn Ca::generate() -> Ca`
  - `fn Ca::leaf(&self, host: &str) -> (String /* cert pem */, String /* key pem */)`
  - `fn Ca::write_pem(&self, path: &Path) -> io::Result<()>`
- Produces, in `provider.rs`:
  - `enum FaultPlan { None, Seeded(u64), Scripted(Vec<ProviderDecision>) }`.
    - `Seeded(s)` draws each new key's decision with `provider_decision` from `stream(s, "provider")`, in arrival order.
    - `Scripted` pops front, and is `Accept` once empty.
  - `struct ProviderConfig { bind: SocketAddr, ca: Ca, faults: FaultPlan }`
  - `struct Provider`, with:
    - `fn start(cfg: ProviderConfig) -> io::Result<Provider>`
    - `fn addr(&self) -> SocketAddr`
    - `fn effects(&self) -> Vec<Effect>`
    - `fn set_outage(&self, on: bool)`
    - `fn stop(self)`
  - It runs its own tokio runtime on a background thread, so the rest of the judge stays blocking.

**Behaviour:**
- **TLS.** The provider serves TLS with `ca.leaf(MAIL_HOST)`. Only `POST /v1/send` exists; anything else is a 404.
- **Bad requests.** A missing `Idempotency-Key`, or a body that doesn't parse as `{to, template, booking_id, org_id}` with a known template → `400`, with no effect.
- **A key already seen** → `200 {"id": <the first id>}`, with no new effect. This check comes before any fault decision.
- **A new key.** Draw the next decision:
  - `Accept`: record the effect (`at` = receipt time), then `200`.
  - `AcceptDropReply`: record the effect, then close the connection with no response. Return `Err` from the hyper service.
  - `Delay(d)`: record the effect, sleep `d`, then `200`.
  - `Status(503)`: no effect; `503`.
  - `Status(429)`: no effect; `429` with `Retry-After: 1`.
- **During an outage,** accept each connection and close it immediately: no TLS handshake, no effect.

- [ ] **Step 1: Write the failing tests** in `tests/provider.rs`. They use reqwest with the CA added as a root and `resolve(MAIL_HOST, provider.addr())`, against `https://mail.judge.test:<port>/v1/send`:
  - `accepts_and_records_one_effect`
  - `a_repeated_key_is_one_effect_and_the_same_id`
  - `missing_key_is_400_and_no_effect`
  - `scripted_drop_reply_records_the_effect_and_the_client_sees_an_error`
  - `scripted_503_and_429_record_nothing_and_429_says_retry_after`
  - `outage_refuses_without_an_effect_and_lifting_it_restores_service`
  - `tls_for_another_host_name_fails_verification`: resolving `wrong.test` to the same address fails with a certificate error.
- [ ] **Step 2: Run them; they fail.** **Step 3: Implement.** **Step 4: Run them; they pass. Run the gate.**
- [ ] **Step 5: Commit** with the message `judge: CA and the fake email provider — idempotency keys, dropped replies, 503/429, outages`.

---

### Task 6: The TCP fault proxy

**Files:**
- Create: `crates/judge/src/proxy.rs`, copied from charpente's `crates/edge-harness/src/proxy.rs` with a source comment, the `Scenario` dependency removed, and charpente's clippy allows dropped.
- Create: `crates/judge/tests/proxy.rs`.

**Interfaces:**
- Produces:
  - `struct Proxy`, with:
    - `fn start(listen: SocketAddr, upstream: SocketAddr) -> io::Result<Proxy>`
    - `fn addr(&self) -> SocketAddr`
    - `fn drop_all(&self)`
    - `fn partition(&self, on: bool)`
    - `fn sever_replies(&self, on: bool)`. New, not in the copied file. While it is on, client-to-upstream bytes are forwarded, and the first upstream-to-client byte closes both sides without being forwarded: the request lands and its answer is lost.
    - `fn stop(self)`
  - `delay_ms` is not needed; remove it.

- [ ] **Step 1: Write the failing tests:**
  - `forwards_bytes_both_ways` (a loopback echo server upstream)
  - `drop_all_severs_live_connections`: a read on the client side returns EOF or an error.
  - `partition_refuses_new_connections_and_lifting_it_restores_them`
  - `sever_replies_delivers_the_request_and_drops_the_answer`: the upstream records that it received the bytes; the client reads EOF or an error and no reply bytes.
- [ ] **Step 2: Run them; they fail.** **Step 3: Implement** (adapt the copy). **Step 4: Run them; they pass. Run the gate.**
- [ ] **Step 5: Commit** with the message `judge: TCP fault proxy, adapted from charpente's edge harness`.

---

### Task 7: Deployment control

**Files:**
- Create: `crates/judge/src/deploy.rs`, `crates/judge/tests/deploy.rs` (`#[ignore = "docker"]`). The unit tests go in `deploy.rs`.

**Interfaces:**
- Produces:
  - `struct InfraError(pub String)`
  - `struct DeployEnv { mail_base: String, ca_host_path: PathBuf }`
  - `struct Deployment { project: String, dir: PathBuf, override_path: PathBuf, services: Vec<String> }`
  - Methods on `Deployment`:
    - `fn open(target: &Path, project: &str, out_dir: &Path) -> Result<Deployment, InfraError>`. Runs `docker compose -f compose.yaml config --services`, which must list `app`.
    - `fn override_yaml(services: &[String], env: &DeployEnv) -> String`. Pure.
    - `fn reset(&self) -> Result<(), InfraError>`: `down -v --remove-orphans`.
    - `fn up(&self, env: &DeployEnv) -> Result<(), InfraError>`: `up -d --build`, with `JUDGE_MAIL_BASE` and `JUDGE_MAIL_CA` set in the process environment so the compose file can interpolate them.
    - `fn app_addr(&self) -> Result<SocketAddr, InfraError>`: `docker compose port app 8080`.
    - `fn kill(&self, service: &str) -> Result<(), InfraError>`: `kill -s KILL <service>`.
    - `fn start(&self, service: &str) -> Result<(), InfraError>`
    - `fn save_logs(&self, path: &Path) -> Result<(), InfraError>`
    - `fn down(&self) -> Result<(), InfraError>`: `down -v --remove-orphans`.
    - Every command is `docker compose -p <project> -f <dir>/compose.yaml -f <override_path> …`.
  - `fn host_gateway_ip() -> Result<IpAddr, InfraError>`: `docker run --rm --add-host judge-host:host-gateway alpine:3 getent hosts judge-host`, taking the first field.
  - `fn clock_offset_ms() -> Result<i64, InfraError>`: container `date +%s%3N` minus the host's reading, taken at the midpoint of the call.
  - `fn check_clock_offset(offset: i64) -> Result<(), InfraError>`. Pure. Refuses `|offset| > MAX_CLOCK_OFFSET` with `the Docker VM's clock is <offset> ms off the host's (limit 500 ms); due times are judged on the host clock, so this run would measure the drift — restart Docker Desktop and rerun`.

**The override, for every service:**
- `environment` with `JUDGE_MAIL_BASE` and `JUDGE_MAIL_CA: /judge/ca.pem`;
- `volumes` with `<ca_host_path>:/judge/ca.pem:ro`;
- for `app` only, `ports: ["127.0.0.1::8080"]`.

**The project name** is `judge-<seed as 16 hex digits>` for a run, and `judge-check-<pid>` for `check`.

- [ ] **Step 1: Write the failing unit tests:**
  - `override_sets_env_and_ca_mount_on_every_service_and_publishes_only_app`. Parse the YAML back with `serde_yaml` (a dev-dependency) and assert the structure.
  - `clock_offset_over_limit_is_refused_with_the_fix` (Review Focus 1): 501 and −501 are refused, and 500 is accepted.
- [ ] **Step 2: Write the Docker tests** (`#[ignore = "docker"]`):
  - `a_container_reaches_the_provider_at_the_host_gateway_ip_over_tls`. Start a `Provider` bound to `0.0.0.0:0`, then run `curlimages/curl` with `--cacert` (the CA mounted) and `--resolve mail.judge.test:<port>:<gateway ip>` against `https://mail.judge.test:<port>/v1/send`, with a valid body and key. Assert the provider recorded one effect.
    - **If this fails on this machine, stop and report it; do not improvise another network path.** It is the environment fact the spec's `--outbound-base` IP rule depends on.
  - `reset_removes_a_stale_project_so_a_second_run_starts_empty` (Review Focus 5). Bring up `fixtures/targets/reference` under a project name, create a booking through the API, leave it running, and call `open` + `reset` + `up` on the same project name. Assert `GET /orgs/{org}/bookings` with a fresh signup lists nothing, and that the old org's key is refused. This test needs Task 8's fixture; run it after Task 8 and mark it in the task report.
  - `kill_then_start_brings_app_back`.
- [ ] **Step 3: Run the unit tests; they fail.** **Step 4: Implement.** **Step 5: Run the unit tests; they pass. Run `cargo test -p judge --test deploy -- --ignored --test-threads 1`** with Docker running. The provider-reachability and kill tests pass now; the reset test is deferred to Task 8. Run the gate.
- [ ] **Step 6: Commit** with the message `judge: deployment control — compose override, host-gateway reachability, clock-offset guard, stale-project reset`.

---

### Task 8: The reference app (fixture)

**Files:**
- Create: `fixtures/task0-app/` as listed in File Structure, plus `fixtures/targets/reference/compose.yaml`.

**Interfaces:**
- Produces, in the fixture crate:
  - `fn build_router(state: AppState) -> axum::Router`
  - `fn spawn_worker(state: AppState) -> tokio::task::JoinHandle<()>`
  - `struct Config { mail_base: String, mail_ca: PathBuf, mode: Mode, data_dir: PathBuf, port: u16 }`, from the env vars `JUDGE_MAIL_BASE`, `JUDGE_MAIL_CA`, `JUDGE_FIXTURE_MODE` (default `reference`), `DATA_DIR` (default `/data`) and `PORT` (default `8080`)
  - `enum Mode { Reference, LostWrite, MissingEffect, DuplicateEffect, EffectNotOwed, DoubleApply, CrossTenant, Unauthenticated, DoNothing }`, parsed from snake_case. Only `Reference` behaviour exists in this task; Task 11 adds the rest.
- Consumes: the `judge` crate as a **dev-dependency** by path (`../../crates/judge`), for `Provider`, `Ca` and `FaultPlan` in tests.

**Design (decided; the implementer writes the code):**
- **Storage.** SQLite at `<data_dir>/app.db`, `journal_mode=WAL`, `synchronous=FULL`, one connection behind a `Mutex`. Every request is one transaction, and the route answers after `COMMIT`.
- **Tables:**
  - `orgs(id, name)`
  - `api_keys(hash PRIMARY KEY, org_id)`, holding the SHA-256 hex of the key
  - `bookings(id, org_id, client_ref, customer_email, starts_at, status, idem_key, body_hash, UNIQUE(org_id, idem_key))`
  - `outbox(id, org_id, booking_id, template, due_at, state, idem_key)`, where `state` is `pending`, `sent` or `void` and `idem_key = "<booking_id>:<template>"`
- **Create** inserts the booking and its outbox rows in the same transaction, skipping rows whose `due_at` is already past.
- **Cancel** sets `cancelled` and voids pending rows, in one transaction. **Check-in** sets `checked_in` and voids the pending `no_show` row.
- **Auth.** A key is 32 random bytes as hex. A missing or invalid key → `401`. A key whose org differs from the path org → `404`.
- **The worker** loops every 200 ms over pending rows with `due_at <= now`.
  1. In a transaction, re-read the booking and void the row if it is no longer owed. For `no_show` on a still-booked booking, set the status to `no_show` in the same transaction.
  2. Send with `Idempotency-Key` set to the row's `idem_key`.
  3. On a 200, mark the row `sent`.
  4. On any error, retry on a later pass with backoff doubling from 200 ms up to 2 s, so the worker keeps retrying through a 30 s outage.
- **Mail client.** reqwest with rustls, with `JUDGE_MAIL_CA` added as a root and `resolve(MAIL_HOST, <ip:port from JUDGE_MAIL_BASE>)`, sending to `https://mail.judge.test:<port>/v1/send`.
- **Dockerfile.** Multi-stage, `rust:1.99` then `debian:bookworm-slim`, running as a non-root user. `/data` is a volume.
- **`targets/reference/compose.yaml`:** one service `app` built from `../../task0-app`, with `image: judge-task0-app:dev` (so later targets reuse the build), `environment: {JUDGE_FIXTURE_MODE: reference}`, and the named volume `data:/data`.

- [ ] **Step 1: Write the failing HTTP tests** in `tests/http.rs`. Each spawns `build_router` on `127.0.0.1:0` with a temp `data_dir`, with no worker:
  - `signup_answers_201_with_org_id_and_api_key`
  - `create_without_idempotency_key_is_400`
  - `repeat_create_same_key_same_body_returns_the_same_booking`
  - `same_key_other_body_is_409`
  - `no_key_is_401_and_another_orgs_key_is_404`
  - `cancel_twice_is_200_and_check_in_after_cancel_is_409`
  - `list_returns_only_my_orgs_bookings`
- [ ] **Step 2: Write the failing outbox tests** in `tests/outbox.rs`. Each runs the router and the worker against an in-process `Provider`. Use due times a little in the future so the tests are fast:
  - `reminder_early_reaches_the_provider_once_with_its_stable_key`: create with `starts_at = now + 60_500`, then wait at most 5 s. Assert exactly one effect, with key `<id>:reminder_early`.
  - `cancel_before_due_sends_nothing`
  - `check_in_voids_no_show`: create with `starts_at = now - 29_500` (early and late already past, so skipped; `no_show` due in 0.5 s) and check in at once. Wait 3 s. Assert no effect and status `checked_in`.
  - `no_show_marks_the_booking_and_sends_once`
  - `a_503_then_accept_is_one_effect`: `FaultPlan::Scripted(vec![Status(503)])`.
  - `a_dropped_reply_is_retried_with_the_same_key_and_stays_one_effect`
  - `the_worker_retries_through_a_10s_outage`
- [ ] **Step 3: Run them; they fail.** **Step 4: Implement.** **Step 5: Run the fixture's own gate** (`--manifest-path fixtures/task0-app/Cargo.toml`); all pass.
- [ ] **Step 6: Run Task 7's deferred `reset_removes_a_stale_project_…` test** with Docker. It passes.
- [ ] **Step 7: Commit** with the message `fixture: task 0 reference app — SQLite WAL with synchronous=FULL, a transactional outbox, stable idempotency keys`.

---

### Task 9: The task 0 workload

**Files:**
- Create: `crates/judge/src/task0/workload.rs`, `crates/judge/tests/workload.rs`.

**Interfaces:**
- Consumes: the `runlog` types, `schedule::stream` and the `task0` constants.
- Produces:
  - `struct OrgCreds { org_id: String, api_key: String }`
  - `struct Client` (reqwest blocking, timeout `CLIENT_TIMEOUT`, `base` = the proxy's address), with:
    - `fn new(base: SocketAddr) -> Client`
    - `fn signup(&mut self, name: &str) -> Option<OrgCreds>`
    - `fn create(&mut self, org: &OrgCreds, client_ref: &str, email: &str, starts_at: Ms) -> u64`, where the `u64` is the event id
    - `fn cancel(&mut self, org: &OrgCreds, booking_id: &str) -> u64`
    - `fn check_in(&mut self, org: &OrgCreds, booking_id: &str) -> u64`
    - `fn get(&mut self, org: &OrgCreds, booking_id: &str) -> u64`
    - `fn list(&mut self, org: &OrgCreds) -> Option<Vec<Booking>>`
    - `fn probe_cross_tenant(&mut self, rng: &mut ChaCha8Rng, as_org: &OrgCreds, victim: &OrgCreds, booking_id: &str) -> u64`
    - `fn probe_unauthenticated(&mut self, rng: &mut ChaCha8Rng, org: &OrgCreds, booking_id: &str) -> u64`
    - `fn resend_unanswered(&mut self) -> u32`
    - `fn events(&self) -> &[ClientEvent]`
    - `fn into_events(self) -> Vec<ClientEvent>`
  - `struct Workload { rng: ChaCha8Rng, orgs: Vec<OrgCreds>, refs: BTreeMap<String, String>, known: Vec<(usize, String)>, next_ref: u64 }`, where `known` is `(org index, booking id)` from acknowledged creates. Methods:
    - `fn new(seed: u64) -> Workload`
    - `fn warmup(&mut self, c: &mut Client)`
    - `fn fault_step(&mut self, c: &mut Client)`
    - `fn tail(&mut self, c: &mut Client)`
    - `fn collect_final_state(&self, c: &mut Client) -> Option<BTreeMap<String, Vec<Booking>>>`
    - `fn refs(&self) -> &BTreeMap<String, String>`

**Decisions:**
- **Every request** records a `ClientEvent` with host-clock `sent_at` and `answered_at`. A timeout or connection error leaves `answered_at`, `status` and `body` as `None`.
- **Refs and keys.** `client_ref` is `r<seed hex>-<n>`, and it is also the create's `Idempotency-Key`. A re-send reuses both and sets `resend_of`. `customer_email` is `c<n>@example.test`.
- **Warm-up:** sign up 3 orgs, then 12 creates spread over `WARMUP`, each with `starts_at = now + uniform[STARTS_AT_MIN, STARTS_AT_MAX]`.
- **Fault phase:**
  - `fault_step` sends one request after an exponential wait with mean 250 ms.
  - The kind is drawn: create 0.55, cancel 0.15, check-in 0.15, get 0.05, cross-tenant probe 0.05, unauthenticated probe 0.05.
  - Cancel, check-in and get pick from `known`, uniformly.
- **Probes:**
  - **Cross-tenant:** pick get, cancel, check-in or list uniformly, and pick between org A's key on B's path and A's key on A's path with B's booking id. `auth = OtherOrg` and `key_org = A`.
  - **Unauthenticated:** pick one of the five non-public routes, with no `Authorization` header or with `Bearer invalid-key`.
- **Tail:**
  1. `resend_unanswered`, once per unanswered write.
  2. Three liveness creates with `starts_at = now + LIVENESS_STARTS_AT`.
  3. One probe of each kind.
  4. Idle until the run loop ends the phase.
- **Final state:** `list` for each org with its own key, retried every 2 s for up to 30 s per org. `None` if any org never lists.

- [ ] **Step 1: Write the failing tests** in `tests/workload.rs`. Each builds the fixture binary once (`cargo build --manifest-path ../../fixtures/task0-app/Cargo.toml`, path from `CARGO_MANIFEST_DIR`), runs it on a free port with a temp `DATA_DIR` and an in-process `Provider` (`FaultPlan::None`), and talks to it directly with no proxy:
  - `warmup_signs_up_three_orgs_and_acks_twelve_creates`
  - `a_resend_reuses_the_key_and_ref`: stop the app, send a create (it goes unanswered), start the app, call `resend_unanswered`. Assert the re-send's event has `resend_of` equal to the first event's id and the same `Idempotency-Key` and `client_ref`, and the org lists exactly one booking with that ref.
  - `probes_are_refused_by_the_reference_app`: run 20 probes of each kind; no 2xx.
  - `final_state_lists_every_acked_booking`
  - `a_server_slower_than_the_timeout_is_unanswered_and_resent_in_the_tail` (Review Focus 4). Run a 20-line local TCP stub that accepts, sleeps 6 s and then answers 201. Assert the first event has `answered_at == None`, and that the tail produced a re-send with `resend_of` set.
- [ ] **Step 2: Run them; they fail.** **Step 3: Implement.** **Step 4: Run them; they pass. Run the gate.**
- [ ] **Step 5: Commit** with the message `judge: task 0 workload — keyed creates, owner and probe requests, re-sends, final-state collection`.

---

### Task 10: Runs, `check`, `score` and the report

**Files:**
- Create: `crates/judge/src/run.rs`, `crates/judge/src/report.rs`. Modify `src/main.rs` to replace the Task 1 stubs.
- Test: unit tests in `report.rs` and `run.rs`; Docker tests appended to `tests/deploy.rs`.

**Interfaces:**
- Consumes: everything above.
- Produces:
  - `fn run_one(target: &Path, seed: u64, out: &Path) -> Result<RunLog, RunError>`. `out` is the run's own directory, `<--out>/run-<i>`.
  - `enum RunError { Infra(InfraError), Void { at_phase: &'static str, paused_ms: u64 } }`
  - `fn run_all(args: &RunArgs) -> Result<Vec<(u64, Result<RunScore, RunError>)>, InfraError>`
  - `fn check(args: &CheckArgs) -> Result<CheckReport, InfraError>`
  - `struct CheckReport { steps: Vec<(String, bool)> }`
  - `fn detect_pause(ticks: &[Ms]) -> Option<(usize, u64)>`. Pure; returns the index and length of the first gap over `MAX_HOST_PAUSE`.
  - `fn render_human(task: &str, target: &Path, scores: &[(u64, Result<RunScore, RunError>)], master: Option<u64>) -> String`
  - `fn render_json(…) -> serde_json::Value`, with the same arguments.

**`run_one`, in order:**
1. `clock_offset_ms` → `check_clock_offset` (refusal is `Infra`).
2. `Ca::generate`, then write `out/ca.pem`.
3. `Provider::start` on `0.0.0.0:0` with `FaultPlan::Seeded(seed)`. `mail_base = https://<host_gateway_ip>:<port>`.
4. `Deployment::open` + `reset` + `up`.
5. Poll `GET /health` directly on `app_addr` every 500 ms. Not 200 within `READY_TIMEOUT` (counted from `up` returning) gives a `RunLog` with `ready = false`; skip to step 10.
6. `Proxy::start(127.0.0.1:0 → app_addr)`. The `Client` uses the proxy.
7. **Warm-up.** Then **faults:** a fault thread executes `schedule::draw(seed, services.len())`:
   - a kill runs `kill`, sleeps `restart_after`, then runs `start`;
   - a connection drop runs `drop_all`;
   - a partition toggles `partition` on and off;
   - a reply-loss window toggles `sever_replies` on and off;
   - an outage toggles `set_outage`;
   - each action is logged as a `FaultEvent` (`kill app`, `start app`, `conn drop`, `partition on/off`, `replies lost on/off`, `outage on/off`).
   - Meanwhile the main thread loops `fault_step` until `faults_end`.
8. **Tail.**
9. `collect_final_state`.
10. Take `effects` from the provider. `save_logs` to `out/compose.log`. `down`, stop the proxy and the provider. Write `out/runlog.json`.
- The run loop records a tick before every request and every 250 ms while idling. `detect_pause` over the ticks gives `RunError::Void` (Review Focus 2).

**Seeds:**
- `Fresh` draws the master from OS randomness and prints it.
- `Master(m)` uses `run_seeds(m, runs)`.
- `Single(s)` runs once with `s`.

**`check`:**
1. `up`, then wait for readiness.
2. Sign up an org.
3. Create a booking with `starts_at = now + 61_000`.
4. Wait at most 20 s for a `reminder_early` effect for that booking, then `down`.
- Steps, printed as `ok`/`FAIL` lines:
  - `boots: GET /health answered 200`
  - `signup: 201 with org_id and api_key`
  - `create: 201 with booking.id`
  - `email: reminder_early reached mail.judge.test over TLS`
- Exit 0 when all pass, 1 otherwise, 2 on `InfraError`.

**`score`:** read `runlog.json`, run `score`, render, and exit as `run` would.

**Human report:**
- One block per run:
  - `run <i>/<N>  seed 0x<16 hex>  gate ok|FAILED (<failures>)  violations <total>  exercised <acked writes>/<owed>/<kills>  availability faults <p>% tail <p>%`
  - then each violation as `  <class>: <detail>`.
- A void run prints `run <i>/<N>  seed 0x…  VOID — host paused <n> s during <phase>; this run measured nothing — rerun it with: judge run --task task0 --target <dir> --seed 0x…`.
- Then the totals per class in the order of `Class`, then `runs with a violation: <k>/<N>`, then the verdict.
- **The verdict line (exact):** `judge result: PASS — 0 violations in <N> runs of 7 min 50 s (task0; faults: kill -9, connections, email provider)`, or `judge result: FAIL — <v> violations, <g> runs failed the gate, in <N> runs of 7 min 50 s (task0; faults: kill -9, connections, email provider)`.
- **Last line:** `rerun exactly: judge run --task task0 --target <dir> --runs <N> --master-seed 0x<hex>`.
- **JSON:** `{"task", "target", "master_seed", "runs": [{"seed", "void": null|{…}, "gate": [...], "violations": [...], "exercised", "availability"}], "totals": {<class name>: n}, "runs_with_violation", "verdict", "exit"}`.

**Exit:** 2 if any run is `Infra`. Otherwise 1 if any run is void, failed the gate, or has a violation. Otherwise 0. A void run makes the batch exit 1 with the void named, never PASS.

- [ ] **Step 1: Write the failing unit tests:**
  - `detect_pause_finds_the_first_gap_over_5s`
  - `detect_pause_ignores_4999ms`
  - `human_report_pass_line_is_exact`
  - `human_report_fail_line_counts_gate_failures`
  - `a_void_run_names_its_rerun_command_and_fails_the_batch`
  - `json_report_totals_match_the_runs`
  - `rerun_line_carries_the_master_seed`
- [ ] **Step 2: Write the Docker tests:**
  - `check_passes_on_the_reference_target`
  - `one_run_on_the_reference_target_writes_a_runlog_and_scores_clean`. Run `--seed 0x1 --runs 1`. Assert exit 0, `runlog.json` exists, and `judge score --log` reproduces the same verdict line.
- [ ] **Step 3: Run the unit tests; they fail.** **Step 4: Implement.** **Step 5: Run the unit tests; they pass. Run the Docker tests** (about 9 minutes). Run the gate.
- [ ] **Step 6: Commit** with the message `judge: run phases, check, score, and the report — exact verdict and rerun lines, void runs on host pauses`.

---

### Task 11: The victims and the do-nothing app

**Files:**
- Modify: the fixture's `src/mode.rs`, `src/routes.rs`, `src/worker.rs`, `src/db.rs`. Each mode's switch sits at one marked site: `// victim: <mode>`.
- Create: `fixtures/task0-app/tests/modes.rs`, and `fixtures/targets/<mode>/compose.yaml` for the eight non-reference modes. Each is the reference compose with `JUDGE_FIXTURE_MODE` changed.

**Each mode is the reference with exactly one bug:**

| Mode | The bug | The class it must trip |
|---|---|---|
| `lost_write` | create answers 201, then writes the booking and its outbox rows from an in-memory queue flushed every 500 ms | `lost_write` |
| `missing_effect` | the booking commits; its outbox rows live only in an in-memory queue the worker drains | `missing_effect` |
| `duplicate_effect` | a fresh random `Idempotency-Key` on every send attempt | `duplicate_effect` |
| `effect_not_owed` | cancel and check-in don't void outbox rows, and the worker doesn't re-check | `effect_not_owed` |
| `double_apply` | create ignores `Idempotency-Key` and always inserts | `double_apply` |
| `cross_tenant` | `GET /orgs/{org}/bookings/{id}` checks that the key is valid, but not that its org matches | `cross_tenant` |
| `unauthenticated` | `GET /orgs/{org}/bookings/{id}` skips auth | `unauthenticated` |
| `do_nothing` | `GET /health` answers 200; every other route answers 500 | the gate (`too_few_warmup_writes`), with zero violations |

- [ ] **Step 1: Write the failing mode tests** in `tests/modes.rs`, in process, with no Docker or kills:
  - `double_apply_mode_inserts_twice_for_one_key`
  - `cross_tenant_mode_serves_another_orgs_booking`
  - `unauthenticated_mode_serves_a_booking_without_a_key`
  - `effect_not_owed_mode_sends_after_cancel`
  - `duplicate_effect_mode_sends_two_keys_after_a_dropped_reply` (scripted `AcceptDropReply`)
  - `do_nothing_mode_is_healthy_and_refuses_everything_else`
  - `reference_mode_has_none_of_these_behaviours`, which runs every check above against `reference` and expects the correct behaviour.
  - `lost_write` and `missing_effect` need a kill, so they are proven only in Task 12.
- [ ] **Step 2: Run them; they fail.** **Step 3: Implement.** **Step 4: Run them; they pass.** Run the fixture's gate.
- [ ] **Step 5: Commit** with the message `fixture: one victim per scored class, and a do-nothing app, each the reference with one bug`.

---

### Task 12: The proof, and the README

**Files:**
- Create: `crates/judge/tests/proof.rs` (`#[ignore = "docker"]`), `scripts/proof.sh`.
- Modify: `README.md` (the full text).

**The proof** (spec §5, "Proving the judge"; it gates the rehearsal):
- Every test runs the `judge` binary with `--runs 3 --master-seed <M> --format json` on one target. **Use `M = 0x6a75646765` for every test**, written down before the first proof run and never changed to make a test pass.
- `reference_scores_zero_violations_and_passes_every_gate`: exit 0, every class total 0.
- `<mode>_trips_<class>`, one per victim: `totals[<class>] >= 1`.
- `do_nothing_fails_the_gate_every_run_with_zero_violations`: exit 1; every run's `gate` contains `too_few_warmup_writes`; every total is 0.
- **If a victim's class does not fire**, that is a finding about the judge's fault density or the oracle, not a seed to change. Stop and report it with the run logs.
- **`scripts/proof.sh`:**
  - builds the image once;
  - runs `cargo test -p judge --test proof -- --ignored --test-threads 1 2>&1 | tee proof.log`;
  - prints the count of `test result: ok` against the count of `test result:`;
  - exits with `${pipestatus[1]}`.
  - Expected wall time is about 3.5 hours (9 targets × 3 runs × ~8 min). It is meant to run unattended.

**README sections** (facts fixed here; the prose is the implementer's):
1. **What it is.**
2. **Quick start:** `judge check`, then `judge run`.
3. **Submission contract:** the deployment contract from BRIEF.md, plus:
   - the judge adds an override file;
   - `restart` policies are the submitter's;
   - the judge may kill the database too;
   - volumes persist across kills and are removed at run end.
4. **Scoring:** the classes and the gate, linking to `src/score.rs` as the preregistered source.
5. **Honesty contract:**
   - a run is not byte-for-byte replayable; the seed reproduces the fault schedule, and the logs are the evidence;
   - kill -9 never loses a write that reached the OS page cache, so v1 does not judge power-loss durability (a write acknowledged before `fsync` survives kill -9);
   - the judge's author also wrote charpente, and the scoring was committed before any arm ran;
   - building the wrong thing is not scored.
6. **No clock-step class in v1, and why** (decided before the rehearsal):
   - Linux time namespaces offset only `CLOCK_MONOTONIC` and `CLOCK_BOOTTIME`, never the wall clock;
   - `LD_PRELOAD` time fakers miss statically linked binaries;
   - stepping Docker Desktop's VM clock works only where containers run in a VM. On a Linux host it would step the judge's own clock, so the class would mean different things on different machines.
7. **Preregistration log:** an append-only list of changes to any Task 2 or Task 4 value after the rehearsal starts, each with its reason. It starts empty.

- [ ] **Step 1: Write `proof.rs` and `scripts/proof.sh`.**
- [ ] **Step 2: Run the proof** with Docker, in the background (`run_in_background`), and wait for the notification. Count the results.
- [ ] **Step 3: Write the README.** Run the gate.
- [ ] **Step 4: Commit** with the message `judge: the §5 proof (reference clean, every victim caught by its class, do-nothing fails the gate) and the README — submission and honesty contracts, the clock-step decision`. The proof's counts and master seed go in the commit body.
