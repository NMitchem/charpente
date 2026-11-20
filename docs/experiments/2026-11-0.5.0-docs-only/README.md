# The 0.5.0 docs-only build

This is the docs-only bar, re-run for 0.5.0 (spec §7.7). A fresh agent works offline and gets only what ships in the 0.5.0 crates: README, and the `charpente` crate's `GUIDE.md`, `examples/`, and the `tests/` files GUIDE names as worked examples. It builds an app in a new domain, subscription renewals (`renewly`). The app has the three properties the spec asks for:
- timed obligations that fall due within a run (a period's end);
- an outbound effect whose provider can be out for longer than its retry budget (`billing.example.net`, deduplicating on `Idempotency-Key`);
- something a user must be told when the effect is given up (a notice on a past-due renewal).

- **Spec:** [`docs/superpowers/specs/2026-11-18-0.5.0-design.md`](../../superpowers/specs/2026-11-18-0.5.0-design.md) §7.7. **Plan:** [`docs/superpowers/plans/2026-11-19-0.5.0.md`](../../superpowers/plans/2026-11-19-0.5.0.md) Task 15.
- **Brief:** [`BRIEF.md`](BRIEF.md), committed before the run (`f81be9f3`), sha256 `72eeaf4d…`. The README beside it in each run directory has sha256 `1672c171…`.
- **The machine:** the launch run's box ([`../2026-10-launch-run/tools/`](../2026-10-launch-run/tools/)), with no network.
  - The image is `charpente-t18-box:clippy-mutants-27.1.0` (id `sha256:753de9affae9d56b2df9f5adf65270b1bb2fc079ef4fee4ee95ffb1fe0e907e1`), the same image as 0.4.0's build.
  - Its cargo is pointed at a directory registry holding the six 0.5.0 crates and their vendored dependency closure.
  - The registry was packaged at `9f779c4c` for attempt 1 and rebuilt at `903082e2` for attempt 2. The delta between them is records only (`CHANGELOG.md`, `docs/LEDGER.md`), and neither file ships in a crate. Each crate's checksum moves with its `.cargo_vcs_info.json` sha; no shipped document changes.
- **The builder:** one Opus agent per attempt, run headless from its run directory: `claude -p --model opus --restricted --tools Bash,Read --permission-mode dontAsk --allowedTools Read "Bash(./box:*)"`. It has Bash for `./box` calls and Read confined to the run directory.
  - The launcher and both prompts are local and gitignored (`.superpowers/sdd/2026-11-19-0.5.0/launch-docsonly.sh`, `prompt-docsonly-050-{1,2}.txt`).
  - The two prompts differ only in the run directory and in attempt 2's rig note (quoted under "The rig miss" below).

## The bar, written before the run

The bar is spec §7.7:
- (a) zero framework patches;
- (b) no hand-built clock row and no cron sweep for due work;
- (c) every job's stance is a hook or a retain with a stated reason, and every outbound job's Dead path is `require:`d in its sim and reached;
- (d) no untested check;
- (e) mutants run in its session are all caught or argued;
- (f) review finds no Critical that a platform oracle should have caught.

Every finding gets a disposition (spec §5 G, 0.4.0's rule): fixed, a PLAN row with its trigger, or refused with the reason. A miss is a finding, fixed and followed by a new attempt, and both are recorded. The dispositions below are **proposed**. None is applied in this commit.

## Attempts

| Attempt | Registry sha | Image | Network | (a) | (b) | (c) | (d) | (e) | (f) | Verdict |
|---|---|---|---|---|---|---|---|---|---|---|
| 1 | `9f779c4c` | `charpente-t18-box:clippy-mutants-27.1.0` | none | — | — | — | — | — | — | **INCOMPLETE, not judged**: a rig miss. The session refused 9 calls, then the builder stopped to ask for permission in a headless session. Fixed by R-27 |
| 2 | `903082e2` | same | none | met | met | partly met | met | met: `108 total — 75 caught, 0 missed, 33 unviable, 0 timed out` | met (0 Critical) | **Meets (a), (b), (d), (e) and (f). (c) is partly met**: one pure job's retain carries no stated reason. The controller rules on whether that is a miss (see (c)) |

## Attempt 1

Attempt 1 launched at 2026-11-19T22:32:19Z as session `a6e788ea-48cb-4c93-abb9-b11667fd297a` and ended at 22:46:19Z.
- **Run.** It took 836 s, 66 turns and $6.15. The result's subtype is `success`.
- **Calls.**
  - 52 `./box` calls ran ([`attempt-1/box.jsonl`](attempt-1/box.jsonl), seq 1–52) and 3 Read calls.
  - The session's permission check refused 9 tool calls (below).
- **What it delivered.** `renewly` in four commits ([`attempt-1/renewly/`](attempt-1/renewly/); log in `renewly.git-log.txt`).
  - `src/state.rs` is one transition table.
  - `src/billing.rs` has a timed `PeriodEnd` job (`tx.enqueue_at`, line 147) and an outbound `Charge` keyed per period. A give-up marks the subscription past due and writes one notice in the hook's commit.
  - `src/lib.rs` has the routes, including a public `POST /signup/:ws` that mints the first owner key (line 256).
  - `tests/sim.rs` is unfinished (236 lines). `charpente test --sim` and `--mutants` never ran.
- **Its own records.** Its final message (the last line of [`attempt-1/agent.jsonl`](attempt-1/agent.jsonl)) and [`attempt-1/renewly/FINDINGS.md`](attempt-1/renewly/FINDINGS.md). The FINDINGS records 1 restart (sim fixtures → app-minted credentials).
- **Outcome.** The bar is not judged. The app builds (`cargo build --lib`, box seq 44), and nothing was proved.

### The rig miss, and R-27

- **What was refused.** Two of the 9 refused calls were host-side commands outside `./box`: a copy of GUIDE to the host's `/tmp`, and a host-side `push.sh`. The allow-list admits only Read and `Bash(./box:*)`. The other 7 were `./box` writes of `src/` and `tests/sim.rs`:
  - 5 carried a line starting with `#`, a Rust attribute;
  - 2 carried none, and what triggered their refusal was not identified.
- **The stop.** The builder retried in smaller pieces and moved credential minting into the app. Then it stopped, because "rewording the code until the check lets it through would be working around it". It asked the operator to allow the writes or to stop it, in a session where nobody answers.
- **0.4.0's disposition stands.** 0.4.0 met the same refusals, and its README dispositions them as "refused: Claude Code's permission check, not charpente". The miss here is the stop, and the stop is the rig's.
- **The fix.** Ruling R-27 relaunched with this note added to the prompt, and the registry rebuilt at the records head:

  > This session runs unattended: nobody will read a question or grant a permission until it ends, so carry the work through to the brief's end on your own. The session's permission check refuses some ./box calls by their shape, not their purpose; one containing a line that starts with `#`, as a Rust attribute does, is the usual case. A refused call is that check, not a rule about what you may do: send the same content another way, for example with the line indented, or written with a placeholder that a second ./box call restores with sed.

- **The fix held.** Attempt 2 met 6 refusals:
  - 2 were host-side commands outside `./box`;
  - 3 were `./box` calls with a line starting with `#`;
  - 1 was a `./box` call without one: the first 26 KB write of `src/lib.rs`.

  It built a placeholder scheme at box seq 28–29: `@@#` for a leading `#` and `@@Q` for `'`, restored by `/root/fix.sh` with `sed`. It used the scheme from seq 30 on and did not stop.
- **One rig note for the launcher.** Attempt 2's stderr holds `Warning: no stdin data received in 3s`. `claude -p` was started without `< /dev/null`. It is harmless.

## Attempt 2

Attempt 2 launched at 2026-11-19T22:48:57Z as session `d13b5741-ec45-43b4-9adc-c49a1ef5ba8f` and ended at 23:54:31Z.
- **Run.** It took 3930 s, 123 turns and $12.80.
- **Calls.**
  - 108 `./box` calls ran ([`attempt-2/box.jsonl`](attempt-2/box.jsonl), seq 1–108) and 8 Read calls.
  - The session refused 6 tool calls, and the builder worked around each one.
- **What it delivered.** `renewly` in nine commits ([`attempt-2/renewly/`](attempt-2/renewly/); log in `renewly.git-log.txt`).
  - `src/lib.rs` (833 lines) holds the whole app.
  - `tests/sim.rs` (1404 lines) has seven checks, one per behaviour in the brief.
  - `FINDINGS.md` is 456 lines and records 0 restarts.
- **Its own records.** Its final message is the last line of [`attempt-2/agent.jsonl`](attempt-2/agent.jsonl), and its friction ledger is [`attempt-2/renewly/FINDINGS.md`](attempt-2/renewly/FINDINGS.md). The ledger carries the final `--sim` output and the full output of the final `--mutants` run, appended with `cat` from the run's own logs (box seq 106).
- **Batches.**
  - The final batch is `charpente test --sim` under master seed `0x2fcbb226e9bfc8bc`: 7 passed, 0 failed, with 141, 240, 242, 242, 244, 244 and 247 of 256 runs tested (box seq 105).
  - Other green batches at the final code: three fresh master seeds at seq 96, and `cargo test`'s batch at seq 107. A 2000-run batch at `0x2026` was green at commit `b3c8f03` (box seq 69).
  - Red batches on the way: two answer checks under media corruption at seq 62–64, fixed by `b3c8f03`, and the builder's own planted mutations at seq 84–87.
  - `cargo test` and `cargo clippy --all-targets -- -D warnings` were clean at the end (box seq 107). Clippy's last line is `Finished` with no diagnostic above it. The `CLIPPY=$?` echo beside it reads `tail`'s status, not clippy's.
- **Mutation.** There were three `charpente test --mutants --file src/lib.rs --test-package renewly` runs. Each took about 6 to 8 minutes in the box. These are observations, not BENCH figures.
  1. Master `0x355e5c3e78e056cb`: `109 total — 70 caught, 6 missed, 33 unviable, 0 timed out` (box seq 73–74). Five of the six were `&&`→`||` mutants in "the same id with other terms" conflict guards (`put_plan`, `put_customer`, `subscribe`). The sixth was `Charge::pending`'s conjunction, whose two halves each decided every reachable state.
  2. Master `0xb76287c61043e35f`: `108 total — 75 caught, 0 missed, 33 unviable, 0 timed out` (box seq 83). This came after conflict probes were added and `Charge::pending` was split into a state test and a named refusal (`007e3b8`, `0a02904`).
  3. Master `0xadf4d0e6acff9305`: `108 total — 75 caught, 0 missed, 33 unviable, 0 timed out` (box seq 104). This was after the last code commit (`560d7c4`, launched at seq 97). Pass 1 missed one mutant (`retry`'s `==`→`!=`, `src/lib.rs:563:30`), and pass 2 caught it at the full count.
- **The judge's replay.** The judge rebuilt the recorded `attempt-2/renewly` against the `903082e2` registry: `cargo test --locked --offline --test sim -- --master-seed 0x2fcbb226e9bfc8bc`, rustc 1.99.0, on macOS.
  - The batch output matches FINDINGS.md:234–264 line for line.
  - The judge also ran the built sim binary on the one run that batch's corruption excuse gave up (see R1). That was the only other command.

## The bar, attempt 2

- **(a) met.** No `[patch]`, no `path`, and no `git` source.
  - `Cargo.toml` names `charpente = "0.5"` and `charpente-sim = "0.5"` only.
  - All 100 packages in `Cargo.lock` carry the replaced crates-io source.
  - The four charpente checksums in the lock equal the `903082e2` registry's `.cargo-checksum.json` values (`337f813d…`, `0d43d8b5…`, `ee5ec591…`, `421bca13…`).
  - No box call edits a file under `/opt/registry` or `~/.cargo`. The calls that name them read them (seq 1–18, 21–26). The one write there is `cargo install charpente-cli` (seq 2), the brief's setup step.
  - The judge's `--locked --offline` build resolved the same lock against the same registry.
- **(b) met.** No `App::schedule`, no cron, and no stored clock.
  - Each period's end is a delayed job committed with the state change that creates it: `tx.enqueue_at(WallTime::from_nanos(end), StartRenewal { … })` in `subscribe` (`src/lib.rs:503-509`) and in `Charge::settle` (`src/lib.rs:764-770`).
  - Time is read with `tx.now()` (`src/lib.rs:485-487`, 661, 781).
  - The stored times are the subscription's period bounds, which the brief asks for, and `Renewal.due_ns` / `started_ns` (`src/lib.rs:215-223`). Only the sim's checks read these. Nothing sweeps them.
- **(c) partly met.**
  - **The outbound job meets it.** `Charge` is the one outbound job, and it declares `on_dead`: `#[job(scoped_by = Workspace, on_dead, outbound = "billing.example.net", retry(max = 4, backoff = "30s"), timeout = "10s")]`, `src/lib.rs:680-686`.
    - Its hook marks the subscription past due and writes one notice naming the plan and the time, in the Dead commit (`src/lib.rs:776-801`).
    - `dead:Charge` is required (`tests/sim.rs:61`), and `dead_hooks_succeeded` runs in every run (`tests/sim.rs:703`).
    - The Dead path is reached. The final batch is green with the requirement in force (box seq 105), and a batch fails when a required key is never hit (GUIDE:3708-3711). A full batch at master `0x2027` counted 12,780 `dead:Charge` marks (box seq 94).
  - **The pure job does not state its reason.** `StartRenewal` is `retain_dead` (`src/lib.rs:645`), and the code states no reason for the retain.
    - Its `run` has no `Err(JobError::App)` path. Its `?`s convert to `JobError::Io` (`crates/charpente/src/kernel/dispatch.rs:47-50`), which is a platform fault, not an attempt. It reaches Dead only by a panic or a refused transaction.
    - If it did, the subscription would stay active past its period end, uncharged, and nobody would be told.
    - GUIDE never asks a retain for a reason (GUIDE:1951-1958, and the compile error at 2600), and neither does the brief.
  - **Proposed.** Count the outbound clause met. Record the unstated reason as R3, with a GUIDE fix, and run no third attempt. The ruling is the controller's: the plan says a miss gets a fix and a new attempt.
- **(d) met.** The final batch has 7 passed, 0 failed, 0 untested (box seq 105), and the judge's replay matched it. The lowest tested count is 141 of 256, for `acked_requests_are_never_lost` (finding 12).
- **(e) met.** The final run (box seq 104) left 0 missed and 0 timed out, so every viable mutant was caught.
  - The verb named 8 functions with no viable mutant: `SubState::decode`, `Charge::plan`, `bad`, `build_app`, `caller`, `not_a_member`, `read_subscription` and `view`.
  - The builder measured one by hand. Deleting `read_subscription`'s `may_act_on` failed `workspaces_never_see_each_other` in 168 of 243 runs (box seq 84).
  - It argued five by coverage (FINDINGS.md:171-177): `not_a_member`, `bad`, `caller`, `view` and `decode`.
  - It did not argue `Charge::plan` or `build_app`. Every run exercises `build_app`. `Charge::plan`'s price reaches no check (R4).
- **(f) met: 0 Critical.** The review is below.

## Review (f)

Each Critical class in the dispatch was checked against the code and the record.

| Class | Finding | A platform oracle's part |
|---|---|---|
| Double charge | None. One key per period, `renewly-<ws>-<sub>-<period>` (`src/lib.rs:296-298`, 733). An owner's retry charges the same period under the same key (`src/lib.rs:575-586`). A subscription has one live charge, and a second one is refused by name (`src/lib.rs:698-714`). | None expected. GUIDE §6 gives effect identity to the app (GUIDE:2212-2221), and `keys_are_per_effect` passes a per-job key by design. The builder planted the per-job key itself. Its checks met the bug in 1 of 1953 tested runs (box seq 85) until the workload changed; see finding 10. |
| Lost acknowledged request | None. Every write commits in its route's transaction. The acked check is green (141 tested). | The platform's own (WAL, Drive). |
| Cross-tenant leak | None. Every route opens `tx.scope::<Workspace>(ws, ctx)`, and `authorize` admits a subject only into its own workspace (`src/lib.rs:84-89`). The hand deletion above went red. | Tenant scoping, by type. |
| Notice missing or doubled | None. The notice is written by the `on_dead` hook, in the Dead commit (`src/lib.rs:776-801`). The check pairs Dead charges with notices per period (`tests/sim.rs:1010-1058`). | `dead_hooks_succeeded`, run in every run. |
| Charge before period end | None. A `Charge` is enqueued only by `StartRenewal` (`src/lib.rs:670-673`), which is due at the period end, or by an owner retry from `past_due` (`src/lib.rs:572-586`). The app's own check judges in the node's frame (`tests/sim.rs:903-909`). | The true-time bound is the platform's (spec §7.3(d)). |

The other findings, none Critical:
- **R1 (Important, app).** The corruption excuse covers the isolation and refusal oracles whole.
  - `workspaces_never_see_each_other` and `a_refused_transition_names_what_is_allowed` route every error through `lost()`, which gives the run up wherever `durable_corruption_fired` (`tests/sim.rs:737-745`, 1184-1190, 1284-1287). GUIDE says "Never use it to excuse wrong data or a consistency violation" (GUIDE:4115-4116).
  - A cut cannot explain some of what those checks judge: a cross-workspace 200, a refused probe's leftover row, or a charge to another customer.
  - The final batch excused one run. The judge's replay of that run shows `cancel 115 of subscription 15 was answered 404 {"error":"not found"}` with `fault:misdirected_write: 1` fired, which is a subscription row lost to the cut.
  - The 2000-run batch at `b3c8f03` excused 1 isolation run and 11 refusal runs (box seq 69). Those runs cannot be replayed at the final code.
  - This is the third app to do this. PLAN's row "Mechanical enforcement of GUIDE §8's rule that `durable_corruption_fired` may excuse a give-up, never wrong data" already has its trigger met at two.
- **R2 (Important, app).** The app ships no route and no operator tool that creates a workspace or mints a credential.
  - Only the sim fixture writes `Workspace` and `ApiKey` rows (`tests/sim.rs:163-195`), and `src/main.rs` only launches. The app cannot serve a first request outside the sim.
  - GUIDE says the first credential comes from an IdP or a signup route (GUIDE:1095-1098). The brief names no such route.
  - Attempt 1 built one (finding 15).
- **R3 (Minor, app).** `StartRenewal`'s retain states no reason. See (c).
- **R4 (Minor, app).** No check reads the charged amount.
  - The provider model reads `workspace`, `subscription`, `period` and `customer` (`tests/sim.rs:240-245`, 269), never `amount_cents`. A charge at the wrong price passes every check.
  - `Charge::plan` is one of the two unmeasured functions the builder did not argue.
- **R5 (Minor, app).** The repeat check runs before the caller check in `subscribe` (`src/lib.rs:450-461` before 462-472) and in `cancel` (526-534 before 536).
  - A customer re-sending another customer's request under its id gets 200 with no write.
  - This stays within one workspace.
- **R6 (Minor, record).** Two parts of the builder's record do not match the run.
  - FINDINGS.md's headings were written with times that did not match the box clock and were then set by `sed`. The round-1 and round-2 `--mutants` entries were appended at 23:44:33Z (box seq 98) and set to 23:20Z and 23:28Z (seq 99–100). Seq 72 and 107 made the same kind of change. The times match the runs they describe. The brief asked for a ledger "dated as you go".
  - The final message says "some periods end in the first second". The workload draws 1 to 120 s left in the period (`tests/sim.rs:454`).

## Findings and proposed dispositions

The table lists every documentation finding in both FINDINGS.md files and both final messages. "A1" is attempt 1 and "A2" is attempt 2. Each one was checked against the shipped GUIDE (identical to `docs/GUIDE.md` at `903082e2`) and the crate source.

| # | Finding | Reported in | Checked | Proposed disposition |
|---|---|---|---|---|
| 1 | GUIDE says to seed `ApiKey` rows "in §5's storage form" but names no hashing helper | A1 FINDINGS:36-39 and final message; A2 FINDINGS:43-48, 438 and final message | `charpente::auth::hash_token` is public (`crates/charpente/src/auth/mod.rs:7`, `hash.rs:6`), and GUIDE names it 0 times. GUIDE:983 gives the hash and GUIDE:3749-3756 the seeding rule. `tests/clock.rs:39` imports it, but GUIDE names clock.rs for clock bounds (GUIDE:3975), not for seeding. A1 found it by grepping `tests/` (box seq 22–24). A2 wrote its own 60-line SHA-256 (`tests/sim.rs:100-159`). PLAN:382 already calls it "public and undocumented" | **fix now (docs)**: §5 and §8's seeding sentence name `charpente::auth::hash_token(raw) -> String` |
| 2 | A seeded key's `created_at` raises the node's recorded-time floor, and §8's fixture advice does not say so | A2 FINDINGS:49-53, 445-446 and final message | §7 names "the newest API key's `created_at`" as recorded time (GUIDE:2909-2913). §8 (GUIDE:3749-3756) is silent. A2 seeded `WallTime::UNIX_EPOCH` (`tests/sim.rs:182-184`) | **fix now (docs)**: the same §8 sentence says to seed `created_at` no later than the run's start |
| 3 | `http::Response`'s fields are shown only in `tests/clock.rs` | A1 FINDINGS:40-41 and final message | The fields `status`, `headers` and `body` are public (`crates/charpente/src/http/mod.rs:138-142`). GUIDE names `-> Response` (GUIDE:756) and `Response::json` (805), never the fields | **fix now (docs)**: GUIDE:756 names the three fields |
| 4 | The fence's Clock trait points to DESIGN.md, which the crate does not ship | A1 FINDINGS:42-43 and final message | GUIDE:118 and the scaffold's `clippy.toml` (29 reasons) cite "DESIGN.md §1". GUIDE never names `charpente::core::io::Clock`, which `sim.clock().wall()` (GUIDE:3851, 3932) needs in scope. The fence text is held character for character to the workspace fence (`crates/charpente-cli/tests/new.rs:424`) | **fix now (docs)**: "The fence" and §7 "Wall time and its faults" name `charpente::core::io::Clock` and say DESIGN.md ships with the repository, not the crate. The fence text stays |
| 5 | Whether `WallTime` is storable (`Codec`) or `Wire` | A2 FINDINGS:57-59, 439 and final message | `impl Codec for WallTime` (`crates/charpente-core/src/table.rs:165`). It has no `Wire` or serde impl (`io.rs:61`). §2's `Codec` list (GUIDE:189-190) omits it, while §5 shows `WallTime` fields on platform tables (GUIDE:977-980) | **fix now (docs)**: §2's list adds `WallTime`, and one clause says it crosses the wire as a `WireU64` of `as_nanos()` |
| 6 | Whether `Request` is `Clone` | A2 FINDINGS:119-120, 441 and final message | `#[derive(Clone)] pub struct Request` (`crates/charpente/src/http/mod.rs:75-76`). GUIDE is silent | **fix now (docs)**: one clause where §5 introduces `Request` |
| 7 | The tail re-sends in the requests' order: implied, not stated | A2 FINDINGS:75-78, 442 and final message | `recover` pushes the unanswered requests at the tail's start in index order (`crates/charpente-sim/src/drive.rs:339-344`). Step 4 (GUIDE:3659-3666) says only "sent again at the tail's start" | **fix now (docs)**: step 4 says "in their order in the list" |
| 8 | §16's `ScopedTx` surface does not list the enqueue methods (A2 re-read §16 to learn whether `enqueue_at` and `now()` reach through a scope) | A2 FINDINGS:85-88 | GUIDE:9750-9761 lists `scope` … `emit_keyed`. The type also has `enqueue`, `enqueue_at`, `enqueue_after` and `enqueue_keyed` (`crates/charpente-core/src/db.rs:2543-2572`), and `now()` through the deref. §6 (GUIDE:1664) and §7 (GUIDE:2953) say so. Spec §5 F names §16 among 0.5.0's GUIDE sections | **fix now (docs)**: §16's block lists the four enqueue methods and notes `now()` through the deref |
| 9 | The docs never say that work due after the outage window's latest end never meets it, or that a call landing before the window opens is the rare case | A1 FINDINGS:27-30 and final message; A2 FINDINGS:204-208 | GUIDE states the distribution exactly (GUIDE:4831), and that a call made at once is often answered before the window opens (GUIDE:3716-3722). It draws neither consequence: nothing due after 2 s + 4 h is ever refused, and "a call landed, then the host went away" needs a call in the first 2 s or a lost reply | **fix now (docs)**: one sentence in §8 "Coverage for a Dead path" |
| 10 | The outage cannot be placed. Whole-day obligations never meet it, and "a call landed, then the host went away" almost never happens | A2 FINDINGS:35-42, 188-202, 443-444, 452-456 and final message | (i) The app grew a product field to reach the window: `startedSecsAgo` imports a subscription part-way through its period (`src/lib.rs:320-322`, 465-469, 479-484). (ii) The planted per-job key was met in 1 of 1953 tested runs at `--runs 2000` (box seq 85). After the provider model learned "record, then answer 502", it was met in 3 of 248 and 1 of 253 (box seq 87). (iii) Spec §5 E says "a start inside the faulted phase", and spec §8 leaves the distribution to the builder. R-13 (Task 11) kept the distribution so that S-a would not move | **PLAN row**, trigger met (both 0.5.0 attempts): the window opens at a drawn outbound call to the host (the k-th call, k drawn) instead of at a drawn instant in the first 2 s, so every app with a host composes "a call landed, then the host went away" at its own time scale, in the tail as well. Keep it drawn, not app-set: an app-set window tests only the composition its builder already suspects, and this builder found it only by planting the bug. A start drawn over the faulted phase does not reach work due in the tail. Cost: every recorded batch of an app with a host moves (S-a) |
| 11 | Whether a sim kernel reads `Host::bearer_env` | A2 FINDINGS:60-61, 71-72, 440 and final message | GUIDE:2284-2287: `serve` reads each value once when it starts, and "the value never enters the log, the sim, an error text or `describe`" | **refused**: GUIDE §6 "Declaring the hosts" states it |
| 12 | The acked check skips 38–45% of its runs, and the docs give no way to tell whether that is normal | A2 FINDINGS:110-116, 447-451 | Final batch: 115 of 256 skipped. 107 were the app's own "no subscribe, cancel or retry was acked in the faulted phase", and 8 were boots refused for corruption (box seq 105) | **refused**: the give-ups are the fault rate (settled 2026-09-07; 0.4.0 T9/T11). The report prints the split and the reasons, and the builder read them right |
| 13 | §8's "Judge the answers" warning was read and still missed for three writes | A2 FINDINGS:141-149 | GUIDE:3733-3735 says it, and the first `--mutants` run caught all five guards (box seq 73–74) | **refused**: the doc and the verb both did their job |
| 14 | A timed job's guard: GUIDE §7 promises a timed job never runs early, so `PeriodEnd` has no `now < end` guard | A1 FINDINGS:44-46 | GUIDE §7 does promise it | **refused**: not a gap. It records GUIDE being relied on as intended |
| 15 | The first owner had no way to get a credential (restart 1), so A1 added a public `POST /signup/:ws` that mints the owner key | A1 FINDINGS:11-17 and final message | `attempt-1/renewly/src/lib.rs:256-269`. GUIDE:1095-1098 sanctions "a signup route that mints a key for a subject it has just created" | **PLAN** (existing row "A worked first-credential example…", trigger met): renewly is the sixth build to need one. A2 has none (R2) |
| 16 | The permission check refused calls by shape | A1 FINDINGS:53-58 and final message; A2 final message | 9 and 6 refusals; see "The rig miss" | **refused**: Claude Code's permission check, not charpente (0.4.0's disposition). R-27's prompt note fixed the stop |

The review's findings:

| # | Finding | Proposed disposition |
|---|---|---|
| R1 | The corruption excuse covers the isolation and refusal oracles whole | **PLAN** (existing row "Mechanical enforcement of GUIDE §8's rule…", trigger met): renewly is its third app. The app's half is the app's |
| R2 | No production path to a workspace or a credential | **PLAN** (existing first-credential row, as finding 15) |
| R3 | `StartRenewal`'s retain states no reason | **fix now (docs)**: GUIDE §6 asks a `retain_dead` job to say in one line why its Dead row is enough |
| R4 | No check reads the charged amount | **app** |
| R5 | The repeat check runs before the caller check in `subscribe` and `cancel` | **app** |
| R6 | FINDINGS.md's headings were set after the fact, and one claim in the final message is inexact | **app** (record) |

**Counts.**
- The 16 builder findings: 9 fix now (docs) (1–9), 2 PLAN (10 a new row, 15 an existing row), 5 refused (11–14, 16), 0 app.
- The 6 review findings: 1 fix now (docs), 2 PLAN (existing rows), 3 app.
- Finding 10 bears on part E. The spec says "a start inside the faulted phase", and the build draws it in the first 2 s. Spec §8 left the distribution to the builder, so the finding reopens no part and gets a PLAN row. No other finding touches a 0.5.0 part.
- Two rig notes are for the controller, not a disposition: keep R-27's note in the launcher's prompt for later docs-only builds, and start `claude -p` with `< /dev/null`.

## Redaction

Both builders set the box's `git config user.email` to the operator's account address, found in their harness context. The committed records carry `<operator-email>` in its place:
- attempt 1: `agent.jsonl` 2, `box.jsonl` 1;
- attempt 2: `agent.jsonl` 2, `box.jsonl` 1.

No other occurrence remains. Both `renewly/` copies ship without `.git`. The builders also set `git config user.name` (attempt 1 seq 18, attempt 2 seq 19) to the operator's public handle and public name. Those are left as recorded.

Attempt 2 stands for (a), (b), (d), (e) and (f). (c) waits on the controller's ruling, and every disposition above waits on the controller.

## The controller's rulings

- **R-27 (the rig).** Attempt 1's stop was the session's permission check, not charpente. Attempt 2 ran from a prompt that says the session is unattended and that a refused `./box` call is that check. Later docs-only builds keep that note.
- **R-28 ((c)).** The partial on (c) is a docs gap. `StartRenewal`'s stance is right: it has no `Err(JobError::App)` path, so it reaches Dead only through a panic or a refused transaction. What was missing is that nothing asked for its reason. GUIDE §6 now asks every `retain_dead` job to say in one line why its Dead row is enough. There is no third attempt, because a third run would measure only whether an agent writes that one comment.
- **R-29 (dispositions).**
  - Findings 1–9 and R3 are fixed in GUIDE before the release.
  - Finding 10 becomes a new PLAN v1.x row, "an outage a sim can place", with its trigger recorded as met. The leading shape is an outage that opens at a drawn k-th call to the host. It stays drawn, never app-settable, so an app cannot steer around the fault. It is the next oracle-density item. It is not 0.5.0 scope, because it moves every outage-drawing recorded batch and needs its own spec.
  - The first-credential row (15, R2) and the corruption-excuse row (R1) are annotated as trigger met.
  - Findings 11–14 and 16 are refused for the reasons in the table. R4–R6 are the app's.
- **Verdict.** 0.5.0's docs-only bar is met by attempt 2. (a), (b), (d), (e) and (f) are met outright. (c) is met for the outbound job, and closed by a docs fix for the pure job's unstated reason.
