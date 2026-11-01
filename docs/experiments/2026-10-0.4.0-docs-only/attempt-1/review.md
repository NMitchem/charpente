# T18 review: the 0.4.0 docs-only build, attempt 1 (holdqueue)

Reviewer: Claude (Opus), 2026-11-01. Platform read at `4126037f`. App at `db0472a` (history clone).

**VERDICT: MEETS THE BAR** — (a)-(f) all met. Criticals 0, Importants 8, Minors 10. Four of the Importants are platform defects (I1 `Id<T>` GUIDE text and its unexplained error, I2 the API page's login, I3 a skipped replay without `fired:`, I4 the shrinker's count-prefix stall); four are app-side (I5-I8).

## Plan

1. (a) Cargo.toml, Cargo.lock, src/ for patches, path/git deps, copied platform source.
2. (b) Quote the sim's Drive lines; check for `#[event]`, hand recovery, re-send loops, `EventRow` decodes.
3. (c) FINDINGS' final batch, and the controller's replay `logs/t18-sim-replay.log` once it lands.
4. (d) `holdqueue-mutants-2.log`; the 18 untested functions; hand-break the ones a behaviour rests on in a
   scratch copy, judged with `charpente test --sim --runs 64 --master-seed 0x1`.
5. (e) For each of the three shrunk failures: rebuild the source at the failing call from the history
   clone plus box.jsonl's edits, replay the repro, delete one item at a time, replay each.
6. (f) Review against the brief's Domain and Behaviours; the five hand mutations the dispatch names plus
   any the review turns up.
7. Triage every FINDINGS entry and report.md's "where the docs failed me", with file:line at 4126037f.

Scratch: `/private/tmp/claude-501/-Users-noahmitchem-Documents-GitHub-charpente/3bb3689e-6bb7-482d-9e02-5f9383087b24/scratchpad/t18rev/`.

## Findings (as confirmed)

### (e) Shrunk failures: item deletion (done first)

**Source rebuilt exactly, and proved so.** src/, Cargo.toml and Cargo.lock never change after `bfc96ce`
(`git diff --stat` across every later commit touches tests/sim.rs and FINDINGS.md only).
- Failure 1 (box seq 49) ran on `2683251` as committed: seq 48's `git status` shows only an untracked
  `src/probe.rs`, which lib.rs does not name and seq 49 deletes before the run.
- Failures 2 and 3 (box seq 66) ran on `6da2431` plus seq 64's python edit and seq 65's heredoc append to
  tests/sim.rs. I extracted both from box.jsonl verbatim (`t18rev/edits/`) and applied them.
- Proof of both rebuilds by forward application: `2683251` + seq 50 + seq 56 is byte-identical to `6da2431`,
  and my seq-66 state + seq 67 + seq 69 (python, then append) is byte-identical to `cc0d4da` (`cmp`).
- Every repro replays on this Mac (built offline, `--locked`, own target dirs) byte for byte as the Linux box
  printed it: same message, `fired:` and `inputs:` lines. Logs: `t18rev/e{1,2,3}-repro.log`.

**Items.** Every draw in this scenario is a `between` or `pick` (one choice), so draw k starts at choice k. An
item is draws k..m with m the next draw of the same name and m-k >= 2 (harness.rs `delete_items`, 4126037f
:525-564). Choices are the `--inputs` bytes padded with the 0s a replay reads past the end. The script is
`t18rev/items.py`, and each line of its logs carries the exact command it ran (`t18rev/e{1,2,3}-items.log`).

| failure | shrunk | items | deletion still fails (same msg / any msg) | note |
|---|---|---|---|---|
| copies_are_never_set_aside_twice_or_beyond_stock, seed 0xa6197be6132670e5 (69 -> 64, 500 replays) | 64 draws, 7 holds | 42 (holds 1-6, 7 or 8 choices each) | **0 / 0**: all 42 replays pass (exit 0) | per-item bar met; see the stall below |
| refused_transitions_name_what_is_allowed, seed 0x9c25729b477e714 (54 -> 22, 210 replays) | 22 draws, 2 holds | 6 (hold 1, 6 choices each) | **0 / 0**: all 6 pass | item 10..16 runs past the trimmed choices, which the shrinker does not try; I tried it anyway, and it passes |
| repeats_are_answered_alike_and_conflicts_by_name, seed 0xcf5faeefef2b039b (40 -> 24, 18 replays) | 24 draws, every choice 0 | 7 | n/a | every item is all 0s, so deleting one leaves the same choices and the same run; this is the floor |

Example command (69 -> 64, item 5..12 deleted), run in the rebuilt seq-66 copy:
`charpente test --sim --filter copies_are_never_set_aside_twice_or_beyond_stock --seed 0xa6197be6132670e5 --inputs 0100000005000100000000000000000000030000000000000002000001000000000000010000000000000001000000000100000001 --faults-off write_error,sync_error,misdirected_write,object_delete_error,conn_drop,outbound_lost_reply,net_latency`
It exits 0.

**(e) verdict: MET.** No item can be deleted with the failure still replaying, in any of the three.

**The 69 -> 64 stall is still a finding (platform, Important).** The shrunk run keeps 7 holds where 2 fail:
- My hand-built 28-draw run (`--inputs 0100000000000000000002000001000000000001`, same seed and faults-off)
  fails the same check: "hold 1 became ready at a branch that holds no copies of its title".
- The one-step joint move, deleting hold 1's item *and* lowering `holds` 7 -> 6
  (`--inputs 0100000004000100000000000000000000030000000000000002000001000000000000010000000000000001000000000100000001`),
  also fails: "hold 2 became ready…", the same claim on the renumbered hold.
- Neither half keeps alone, and that is why the shrinker stalls. Deleting an item leaves `holds` at 7, so the
  loop reads the restock draws as hold 7 and the restock that makes the failure is gone. Lowering `holds`
  leaves hold 7's draws, which then read as the restock.
- The shape is a count-prefixed loop whose later draws matter. GUIDE §8's own advice produces it ("give the
  draws that make up an item the same names each time, as a loop does", GUIDE.md:4790-4795). The spec-D probe
  (`latest_wins_over_three_draw_items`, harness.rs:4554) has a count prefix too, but its post-loop draws
  tolerate the shift, so the probe cannot see this.
- GUIDE is honest that a spent budget can stop short of a local minimum (GUIDE.md:4822-4824), so nothing it
  says is false.
- Fix: in pass 1, after an item fails to keep, try the same deletion with the nearest earlier choice lowered
  by one, which is the count when the item sits in a counted loop. Pin it with a probe whose failure needs a
  draw *after* the loop. Alternatively, GUIDE §8 teaches a per-item "more" draw in place of a count prefix,
  as Hypothesis does for lists.
- The 64-draw block still pointed the builder at the right bug ("a copy count may be lowered after a hold is
  served"), because `fired: outbound:completed: 1, restart: 1` said no fault was involved.

### (a) Zero framework patches: MET

- Cargo.toml: `charpente = "0.4"`, `serde`, and dev-deps `charpente-sim = "0.4"`, `serde_json`, `sha2`. No
  `[patch]`, and no `path =` or `git =` anywhere.
- Cargo.lock: 101 packages. 100 carry `source = "registry+…crates.io-index"`; the 101st is holdqueue itself.
  No `[[patch.unused]]`.
- Every path ever tracked in the app's history: .gitignore, Cargo.lock, Cargo.toml, clippy.toml (the
  scaffold's), FINDINGS.md, src/{lib,main,notify,queue}.rs, tests/sim.rs. No `.cargo/`, no vendor dir, no
  `include!`, no copied platform source.
- box.jsonl: every command naming `/opt/registry` is a read of GUIDE.md or `examples/`, by `ls`, `sed -n`,
  `cat` or `grep`. None writes there, and none reads a charpente `src/` or an unlisted `tests/` file.

### (b) Helper and typed event reads: MET

- The app declares no `#[event]`: there is no `event` token in src/. So typed event reads are moot, not a miss.
- Drive, tests/sim.rs:29 and :335-344:
  ```rust
  use charpente_sim::drive::{Drive, Recovered};
  …
  let drive = Drive::new(sim, app);
  let phase = drive.faulted_phase(&requests, PHASE)?;
  let Some(recovered) = drive.recover(&phase, TAIL)? else {
      return Ok(None);
  };
  ```
- No hand recovery. tests/sim.rs has no `restart(`, `quiesce`, `boot_through_fallout`,
  `wait_out_clock_refusal`, `SimSource`, `Kernel::`, `is_fault_fallout`, `clock_fallout`, re-send loop or
  `EventRow`.
- The tail's answers are merged by the check itself, as `Recovered::resent`'s doc says a check that wants
  them merged must (sim.rs:347-354). The durability check judges phase acks only (`out.phase[i] != Some(200)`,
  sim.rs:397), which keeps "acked" and "re-sent" apart as the helper intends.
- Two near-misses that do not breach (b):
  - Check 4 decodes `JobRow` payloads by hand (`NotifyReady::decode(&mut &job.payload[..])`, sim.rs:576). The
    checker library's outbound checks (`calls_have_durable_jobs`, `keys_are_per_job`,
    `outbound_settles_by_horizon`, charpente-sim checks.rs:633-682) went unused. That is a Minor app-side
    note, not an `EventRow` decode.
  - `seed_keys` writes `ApiKey` rows from inside the build closure (sim.rs:91-120). That is a credential
    fixture, not recovery; see the first-credential triage.

### (c) Final batch has no untested check: MET

- FINDINGS' final batch (FINDINGS.md:72-105, master 0x4a3bc8dfdc80c1da): 8 checks, all `ok`, tested
  123/253/245/241/247/245/245/239 of 256, `sim result: ok. 8 passed; 0 failed`.
- The controller's replay (`logs/t18-sim-replay.log`) is byte-identical to FINDINGS.md:72-105 (`diff` empty),
  so the replay also crossed from the Linux box to macOS byte for byte.
- t18-gates.out says "SIM REPLAY DIFFERS", but that is the gate script's grep (t18-gates.sh:26). It picks up
  FINDINGS' two `rerun exactly this mutation run` lines, which are not sim output. **Gate-script defect: anchor
  that grep to the final batch's fenced block.**
- `cargo test` on another master seed (`logs/t18-cargotest.log`) is also 8 ok, with no untested check.

### (d) Mutation run in session, every viable mutant caught: MET (proof gaps under I6)

- The builder ran the verb twice in its session (box seq 58 and seq 71).
  - The first run, over queue.rs and notify.rs, had 11 MISSED in `place`, `record_return` and `set_copies`.
    That is the oracle working: restart 3 added the repeat, conflict and lowering-count cases, then check 7.
  - The final run, over queue.rs, notify.rs and lib.rs with `--test-package holdqueue` and master
    0x2c5fe249bb7ccc00, gave **119 total: 82 caught, 0 missed, 37 unviable, 0 timed out**.
- `holdqueue-mutants-2.log` equals FINDINGS.md's final block line for line.
- The controller's re-run of the exact rerun line (`logs/t18-mutants.log`, 1073 s, exit 0) gives the same 82/0/37,
  with every verdict and deciding pass identical.
- No mutant needed an equivalence argument. The 5 that pass 1 streamed as MISSED (`overdue` `+`->`-`,
  `tick`, `Tick::run`) are caught in pass 2 by the unmet `require ["judged:expired"]`, which is the
  documented split.

**The 18 untested functions, broken by hand.** Each run was in the scratch copy (`t18rev/final`):
`CHARPENTE_SIM_NO_SHRINK=1 charpente test --sim --runs 64 --master-seed 0x1`. The scripts are
`t18rev/mutate.py` and `t18rev/runmut.zsh`, and the logs are `t18rev/mut/*.log`.
- Each mutation's diff is printed before its run, and `git status` is clean after the restore (`cp` + `touch`).
- Unmutated baseline: 8 ok (30/64/60/57/61/59/54/61 tested).

| id | function (verb said untested?) | hand break | result | caught by |
|---|---|---|---|---|
| M1 | queue.rs `waiting` | serve the later-placed hold first (`sort_by_key(Reverse(placed_seq))`) | **RED** | copies_go_to_holds_in_placement_order, 30 of 64 failed |
| M2 | queue.rs `serve` | skip the stock bound (`if false && stock.free() == 0`) | **RED** | copies_are_never_set_aside_twice_or_beyond_stock, 15 failed |
| M3 | queue.rs `serve` | drop the `tx.enqueue(NotifyReady …)` | **RED** | a_hold_is_ready_once_and_its_patron_is_told, 56 failed |
| M4a | queue.rs `pick_up` | accept a pickup of a requested hold (`Ready \| Requested` arm) | **RED** | all 8 checks |
| M4b | queue.rs `pick_up` | accept a pickup of a requested hold, no copy accounting | **RED** | copies check (32), expiry check (43: "picked up without having been ready") |
| M5a | queue.rs `READY_HOURS` | expiry at 6 days, `6 * 24` | **RED** | ready_holds_expire_after_seven_days, 50 failed |
| M5b | lib.rs `build_app` (untested) | expiry at 6 days of real time: `TICK_EVERY` 3600 s -> 3086 s, so the 169th tick lands at ~6.04 days | **GREEN** | nothing |
| M6 | lib.rs `waiting_queue` (untested) | delete `require_operator(ctx)?` | **GREEN** | nothing |
| M7 | lib.rs `put_copies` (untested) | delete `require_operator(ctx)?` | **GREEN** | nothing |
| M8 | lib.rs `record_return` (untested) | delete `require_operator(ctx)?` | **GREEN** | nothing |
| M9 | lib.rs `put_branch` (untested) | delete `require_operator(ctx)?` | **GREEN** | nothing |
| M10 | lib.rs `pick_up` (untested) | delete `require_operator(ctx)?` | **RED** | refused_transitions…, 38 failed ("IntruderPickup … answered 200") |
| M11 | lib.rs `my_holds` (untested) | list patron 1's holds for every caller | **RED** | callers_reach_only_what_is_theirs, 31 failed |
| M12 | queue.rs `Codec::decode` (untested) | swap tags 2 and 3 | **RED** | refused_transitions…, 17 failed |

**Each green was shown to take effect** (`t18rev/runprobe.zsh` and `probe.py`, on seed 0x1, or 0x3/0x4/0x6/0x7 for
returns). The probe sends branch, copies, return and queue requests with patron 1's key and prints the answers:
- Unmutated, all four routes answer 400 `operators only: this credential is for "patron:1"; use an operator key`.
- M6: the patron's `GET …/queue` answers **200** `{"holds":[]}`. The list carries each waiting hold's patron
  id, so a patron reads other patrons' holds.
- M7: the patron's `PUT …/copies` passes the guard and reaches the handler. The probe left the branch
  unregistered, so the handler answers "no branch 1…".
- M8: the patron's `POST /returns/:id` passes the guard and reaches the handler ("branch 2 holds no copies…").
- M9: the patron's `PUT /branches/1` answers **200** `"1"`.
- M5b: the logical clock reads **251** at the end of the same run where it reads **216** unmutated, after 777600 s
  of virtual time (216 h). That is 7/6 the rate, so a hold expires 169 x 3086 s = 6.04 days after it is ready.
  The expiry property passes because it judges logical hours only: `ready_hour`, `ended_hour` and
  `SEVEN_DAYS_OF_HOURS` (sim.rs:753-804) never meet virtual time.

So (d) is met on its letter (every viable mutant caught), but the verb's untested list hid one-sided properties
on two brief behaviours. See **I6**.

### (f) Review against the brief: MET (no Critical)

Ways to break each behaviour, each with an input and a severity:

| behaviour | finding | input sequence | severity |
|---|---|---|---|
| acked hold never lost | none in app code. Every state change is one transaction (queue.rs:5-7). Media corruption is the platform's documented loss window | — | — |
| placement order | none. `waiting` sorts by a per-transaction sequence stamp (queue.rs:145-151), and `serve` walks it (M1 red) | — | — |
| set aside once, never beyond stock | none. `serve` stops at `free() == 0` (M2 red), and `set_copies` refuses a count below set_aside + on_loan (queue.rs:224) | — | — |
| ready once, and told | **I5**: the notification job has `retry(max = 12, backoff = "2s")` (notify.rs:24). Backoff doubles (GUIDE.md:1895-1900), so the 12th failure comes ~4094 s (68 min) after the first and the job goes Dead (GUIDE.md:1900). No redrive exists, and the app writes `notified` but never reads it | notify.example.org answers 503 or refuses for 70 minutes; during that window a copy is returned to a branch with a waiting hold | Important |
| disallowed transition refused, naming what is allowed | **I7**: a second pickup of a picked-up hold, or a second cancel of a cancelled one, answers 200 (queue.rs:319, :345). The brief says "picking up a hold that is not ready … is refused" | operator `POST /holds/1/pickup` twice, a day apart | Important (documented bend, FINDINGS.md:23) |
| patron reads another's holds | **m8**: `place` under another patron's hold id answers "hold N already names another patron, title or branch", an existence oracle with no contents. `GET /holds/:id` and cancel answer 404 (lib.rs:341, queue.rs:329) | patron 2 `POST /holds/1 {title:1,branch:1}` | Minor |
| operator route without an operator | **I8**: `POST /operator` is `public` and mints an operator key for whoever calls it first (lib.rs:227-237): trust on first use. **I6**: four operator routes' guards have no property (M6-M9 green) | an attacker's first `POST /operator` on a fresh deploy | Important (I8; PLAN row 369's gap), Important (I6, proof only) |
| expiry at 7 days | **I6**: the tick interval is unproved (M5b green). Also expiry lags by any downtime: the logical clock loses every missed hour (GUIDE.md:2640-2646). Expiry is "at least 7 days", never early | node down 1 day leaves a ready hold ready 8 days | Important (proof), Minor (lag; PLAN row 356) |
| a stale message | **m10**: a notification enqueued at readiness still says "ready" when it is sent after a cancel (notify.rs:42-65 reads the hold but not its state) | cancel within the notify job's backoff | Minor |

**No Critical.** Each break the dispatch named and each one I tried against the app's own code is refused by
the app (M1-M4b, M10-M12 red). The Importants are a liveness gap under a dependency outage (I5), a documented bend
(I7), a bootstrap the platform's first-credential gap forces (I8), and proof holes over correct code (I6). On
whether a platform oracle should have caught them:
- I5 needs Dead to be reachable in the sim. PLAN row 355 records that no universe reaches Dead and that an app
  cannot see one. It is "Due, named for 0.5.0", so no 0.4.0 oracle claims it. This build is further testimony
  for that row.
- I6 is the guard-deletion class cargo-mutants 27.1 does not generate (it replaces bodies and operators, and
  never deletes a `?` statement). The verb named these functions untested and told the builder to check their
  effect in a property (`holdqueue-mutants-2.log` last lines). The platform did its part.
- `require_operator`'s own two mutants were caught, but only through `pick_up`'s intruder op. That reads as "the
  guard is tested" while 4 of its 6 callers have no property.

## Findings triage (FINDINGS.md and report.md "Where the documentation failed me"), at 4126037f

| # | builder's finding | class | evidence | disposition |
|---|---|---|---|---|
| T1 | `Id<T>` crosses the wire only for a table `T` (restart 1) | **platform defect** (I1) | GUIDE.md:772 says "`String` and `Id<T>` (any `T`) satisfy both". wire.rs:339 has `impl<T: charpente_core::table::Row> Wire for Id<T>`, because the schema names `T::TABLE`. rustc's raw text (box seq 35) is "the trait bound `Patron: Row` is not satisfied … required for `charpente::Id<Patron>` to implement `Wire`". It names the bound but no fix, because `Row` (table.rs:331) carries no `#[diagnostic::on_unimplemented]`, while `Wire` does (wire.rs:171-176). That is short of hard rule 5. GUIDE.md:189 ("`Id<T>` for any `T`" implements `Codec`) is true (table.rs:239) | fix (below) |
| T2 | No way to get a first credential; api.rs's `login` trusts a body subject | PLAN row + **platform defect** (I2) | PLAN.md:369 (first credential, trigger met). This is the fifth app to invent one, by trust on first use (I8). Separately, api.rs:377-381, the page GUIDE.md:3 sends readers to first, has `#[route(POST "/login", public)] fn login(tx, body: Login, ctx) { Ok(ctx.create_session(tx, body.subject)) }`. GUIDE.md:1079-1080 forbids exactly that: "never mint for a subject a request body names: that lets anyone log in as anyone" | PLAN 369 testimony; fix api.rs |
| T3 | May app code `tx.put` an `ApiKey` row? | not an issue | GUIDE.md:9565-9568: "a handler reads and writes them like any other global table". The builder read GUIDE only to line 5290 | — |
| T4 | No delayed enqueue; no wall time in a transaction or job | PLAN row | PLAN.md:356, "a transaction's own time", **due, named for 0.5.0**. This is another app with a hand-built logical clock | PLAN 356 testimony |
| T5 | `TxError` has no `Display` | true; PLAN row (trigger met) | db.rs:193-194 is `#[derive(Debug)] pub enum TxError<E>` with no `Display` impl; `ApiError` has none either. GUIDE.md:648-660 shows a hand-written `describe`. PLAN.md:380 (P9) lists `ApiError: Display` with trigger "a second build that hits one of them". A `TxError<ApiError>` that formats needs both, so this build is the second: the trigger is met. Add `TxError<E: Display>: Display` to the row | PLAN P9, trigger met |
| T6 | Answer bodies are `Vec<u8>`, unstated | doc gap (m2) | api.rs:1159 says "(`phase.answers[i]` holds the body too)" and api.rs:1170 names `recovered.resent[i]`, with no type. GUIDE.md:3346-3347 says "`(status, body)`". Spec §4 F says each entry "shows its use with types" | fix: one typed line per field on the page |
| T7 | `--faults-off` unknown name lists no valid names | **platform defect** (m1, rule 5) | harness.rs:156 `format!("--faults-off: \"{name}\" is not a FaultConfig field")`. `FAULT_AXES` (charpente-sim lib.rs:3098) is in scope, and the unknown-flag error a few lines below lists `VALID_FLAGS`. GUIDE.md:4549-4553 says only "A name that is not a field is a usage error" and lists no axis anywhere | fix (below) |
| T8 | Do index lookups in a transaction see its own writes? | doc gap (m4) | GUIDE.md:512-513 says reads see the transaction's own writes. GUIDE.md:619-620 says `get`/`scan`/`scan_by` "read through the snapshot". Neither says `by_<field>` index reads see the overlay. The sim agrees that they do | one sentence in §4 |
| T9 | How to raise a durability property's verdict rate | not an issue (settled), plus doc gap (m7) | See T11's measurement. Spec §2 (design.md:217-221): "the give-ups are the fault rate" (2026-09-07) stands | — |
| T10 | A skipped run lists no faults that fired | **platform defect** (I3) | A `--seed` replay of a skip prints only `skipped:` and `detail:` (seed 0xb8993ed0c11a4c0c, `t18rev/skip-replay-human.log`). The harness holds the coverage: harness.rs:1176-1186 merges `end.coverage` into `report.coverage`, and `--format json` prints it (`fault:sync_error: 3, fault:write_error: 3, boot:retried: 4, restart:process: 4, …`, `t18rev/skip-replay.json`). The human renderer (harness.rs:1940-1947) drops it. GUIDE.md:4575-4576 lists the JSON `coverage` key but never points a skip's diagnosis there. GUIDE.md:4731-4737 calls `fired:` "the line to reason from" | fix (below) |
| T11 | The durability check is judged in about half its runs | fault rate (settled), part app shape | **Final batch skip reasons:** 118 "no placement, pickup or cancel was acked", 13 boot refused for durable corruption, 2 corruption excuse (133 of 256). **I split the 118** with a re-labelled give-up on the same master seed (`t18rev/skip-classify.log`; the totals reproduce, 123 tested and 133 skipped): 54 the phase answered nothing; 24 the phase answered only non-200s; 35 the phase acked setup requests only and ended before any hold op was answered; 5 a hold op was answered but not 200. So 78 (+15) are faults at the first commits, which is the doctrine. About 40 are the app's shape: roughly 12 setup commits (branches, titles, bootstrap, copies, at 0-23 ms) stand before the first placement, at 1-120 s. Seeding that setup in the build closure, as `seed_keys` already does for keys, would expose the claim's writes earlier. No driver defect: the Drive's faulted phase is one kernel lifetime by design (drive.rs:138-182), and acks only come from it | not an issue; doc note m7 |
| T12 | Pass 1 streams coverage-only catches as MISSED | not an issue | The verb prints, before pass 1: "a MISSED or TIMEOUT line from it is judged again in pass 2". The final table attributes them to pass 2, and the builder read it right (FINDINGS.md:40) | — |
| T13 | Seven days without a clock; cron catch-up | PLAN row + doc gap (m5) | PLAN.md:356 (due 0.5.0). GUIDE.md:2640-2646 (catch-up fires once) is settled design. But GUIDE.md:2746-2749 calls it "exactly the semantics most 'advance the current period' logic wants anyway". For a deadline it is not: the clock loses every downtime and the deadline fires late. The app's expiry is "at least 7 days" | PLAN 356; one GUIDE sentence |
| T14 | Repeats against refusals | builder's reading sound; app bend (I7); doc gap (m6) | GUIDE.md:3366-3377 says "Answer a repeat of the same request with what the first one did", and frames repeat against conflict around routes keyed by the resource's own id (`place_order`, `add_note`). A transition route (`POST /holds/:id/pickup`) carries no request id, so a retry of a pickup that landed and a fresh pickup of a picked-up hold are the same bytes. The rule then forces either the bend or a refusal of a successful retry. The docs already hold the resolution, which the app itself used for returns (`POST /returns/:id`, queue.rs:352-381): key the transition by its own id, so that a repeat answers alike and a new disallowed pickup is refused. GUIDE never says so for transitions. So the conflict is real only at the docs' level of advice | doc gap; app may re-key pickups/cancels |
| T15 | `ApiError` has no conflict variant (no 403, no 409) | not an issue (by design) | GUIDE.md:1265-1268: "There is no 403 … pick one of the variants above" | — |
| T16 | Credentials in a fixed sim script (restart 2) | doc gap (m7) | A `Drive` phase is a fixed request list (`faulted_phase(&requests, …)`, drive.rs:138-142), so a minted key cannot feed later requests. GUIDE §8 shows no sim driving `auth` routes. The builder's fixture (`seed_keys`, sim.rs:91-120, through `App::transact_for_tests` in GUIDE §5's storage form) is sound | one GUIDE paragraph |
| T17 | 11 MISSED in the first mutation run (restart 3) | app-side; platform worked | The oracle found the one-sided properties, and restart 3 closed them | — |
| T18 | The corruption excuse (commit 6da2431) | app-side, sound | `excused` (sim.rs:366-372) gives up **only** when `durable_corruption_fired(sim)` and returns `Err(failure)` otherwise. It never passes a run. Every call site excuses a shortfall a cut acked record explains: an acked op lost or reverted (checks 1, 5, 8), "no branch"/"no title" after an acked setup PUT, a conflict answered 200 after an acked original (check 7), and a message for a hold whose ready record was cut (check 4, the shape GUIDE.md:3755-3760 sanctions for an enqueue cut after its call). No isolation oracle is excused: cross-patron reads fail outright (sim.rs:993-994, 1019). **This app is not one for PLAN.md:358** (whose trigger is already met) | — |
| T19 | Hand deletions (`put_branch`, `refuse`) | app-side, partial | Two of 18 untested functions probed. The other 16 included the guard and tick gaps in I6 | see I6 |
| — | Tool permissions refused lines starting with `#` | harness, not counted | — | — |

## Every finding, by severity

**Critical: 0.**

**Important: 8.**
- **I1** (platform): GUIDE.md:772's "`Id<T>` (any `T`)" is false, and the `Row` bound error names no fix (T1). It cost a restart.
- **I2** (platform): the API page (api.rs:377-381) shows a public login minting a session for a body-named subject,
  the hole GUIDE.md:1079-1080 forbids (T2). The page agents are told to read first teaches account takeover.
- **I3** (platform): a `--seed` replay of a skipped run withholds the `fired:` line it holds (T10). The builder
  needed debug prints and `--faults-off` bisection.
- **I4** (platform): the shrinker stalls on a count-prefixed loop whose later draws matter: 64 draws where 28 fail
  (see (e)). This is GUIDE §8's own recommended loop shape.
- **I5** (app): the notification goes Dead after about 68 minutes of notify outage, and the patron is never told
  (see (f)). Platform half: PLAN row 355, due 0.5.0. This build is testimony for it.
- **I6** (app, proof only): the operator guards on `waiting_queue`, `put_copies`, `record_return`, `put_branch` (and
  `put_title`, the same shape), and `TICK_EVERY` behind the 7-day expiry, have no property. M5b-M9 stay green, and
  the probes show each took effect. The expiry property is circular on the app's own logical clock. Fix: send
  each operator route with a patron key in the scenario, and judge expiry against virtual time (record
  `sim.now()` at the act), or assert the clock's final hour against elapsed virtual hours less downtime.
- **I7** (app): a repeated pickup or cancel answers 200 where the brief says refuse (T14). It is documented. Fix by
  keying transitions.
- **I8** (app): `POST /operator` is trust on first use, the first-credential gap's consequence (PLAN 369).

**Minor: 10.** m1 `--faults-off` names (platform, rule 5); m2 body types on the API page; m3 `TxError`/`ApiError`
`Display` (PLAN P9, trigger met); m4 index reads and own writes; m5 GUIDE §7 overclaims catch-up for deadlines;
m6 the re-send rule is silent on transitions; m7 no guidance on sim credentials or front-loading a durability
claim's writes; m8 a hold id's existence leaks through the place conflict; m9 check 4 decodes `JobRow` by hand,
with the outbound checkers unused; m10 a stale "ready" message after a cancel.

Controller tooling (not counted): t18-gates.sh:26's grep picks up FINDINGS' two `rerun exactly this mutation run`
lines and prints a false "SIM REPLAY DIFFERS". Anchor it to the final batch's fence.

## Platform defects and the fix I would make

1. **I1, `Id<T>`.**
   - GUIDE.md:772: "`String` and `Id<T>` for a `#[table]` type `T` satisfy both (the schema names `T`'s table);
     for any other id, `WireU64`."
   - Put `#[diagnostic::on_unimplemented(message = "`{Self}` is not a table, so `Id<{Self}>` names no table's
     row", note = "`Id<T>` crosses the wire only for a `#[table]` type: declare `{Self}` with `#[table]`, or carry
     the id as a `WireU64`")]` on `Row` (table.rs:331).
   - Pin it with a trybuild case for `Id<NotATable>` as a route param.
2. **I2, api.rs login.** Make the page's `login` `auth` and mint for `ctx.principal()`'s subject, as GUIDE.md:1083-1090
   does. Add a census assertion (beside `driver_census.rs`) that no shipped example passes a body field to
   `create_session` or `create_api_key`.
3. **I3, skipped replay.** In the `--seed` human renderer (harness.rs:1940-1947), print
   `    fired:  {render_fired(coverage less run:skipped)}` under `skipped:`/`detail:`. Pin the line, and add one
   sentence to GUIDE §8 "Giving up" saying the replay shows what fired.
4. **I4, shrinker.** In pass 1, when an item's deletion fails to keep, retry it with the nearest earlier nonzero
   choice lowered by one (the count of a counted loop). Pin it with a probe whose failure needs a draw after the
   loop, beside `a_record_of_three_draws_is_deleted_whole`. Alternatively, GUIDE §8 teaches a per-item "more" draw in
   place of a count prefix.
5. **m1, `--faults-off`.** `format!("--faults-off: \"{name}\" is not a fault axis — valid axes: {}",
   FAULT_AXES.join(", "))`. Update the two pinned texts (cli.rs:242, harness.rs:4788), and list the axes in
   GUIDE.md:4549.
6. **m2.** Add `let _bodies: &Vec<Option<(u16, Vec<u8>)>> = &phase.answers;` and the same for `recovered.resent` on
   the API page.
7. **Doc gaps m4-m7.** One sentence each, in GUIDE §4 (index reads see the overlay), §7 (a logical clock lags by
   downtime, so a deadline fires late), §8 (key a state transition by its own id so a retry and a fresh refusal
   differ; seed fixtures in the build closure; drive `auth` routes with seeded keys).
8. **PLAN.** P9: trigger met, add `TxError: Display`. Row 355 (Dead): add this build's testimony (I5). Row 356
   (time): add this build. Row 369 (first credential): this build is the fifth.

## Verdict

**MEETS THE BAR.** (a) met, (b) met, (c) met, (d) met, (e) met, (f) met. Criticals 0, Importants 8, Minors 10.
