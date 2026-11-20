# FINDINGS — renewly on charpente 0.5.0, built from the docs alone

A friction ledger, written as I went. Times are UTC, from the build machine.

Sources used: BRIEF.md, README.md, the crate's GUIDE.md and `examples/*.rs`. No
`src/` of any charpente crate was opened, and no file under `tests/` (GUIDE names
`tests/clock.rs` and `tests/outbound.rs` as worked examples; I did not need them).

## Counters (kept current)

- Approaches restarted: 0
- GUIDE sections re-read: listed in the dated entries below

## 2026-11-19T22:54Z — reading, and the design it forced

Read GUIDE §1–§8 and §16 in full, skimmed §9–§15, and read `examples/api.rs`,
`examples/modelled_remote.rs` and the scaffold's `tests/sim.rs` line by line.

Decisions the docs drove, each with the section that drove it:

1. **The provider dedupe key is the app's, not the platform's.** §6 "Outbound
   jobs": the minted key covers one job and its `on_dead` re-issues, and "any
   other re-issue … is a new job with a new key. So its `call` must send an id
   of the effect itself". An owner's retry of a past-due renewal is a job the
   route enqueues, so `Charge::call` sends
   `Idempotency-Key: renewly-<ws>-<sub>-<period>`. A re-issue from `on_dead`
   cannot model the owner's retry, because the hook picks its due time and the
   owner does not.
2. **Transitions keyed by their own id.** §8 "A route the tail re-sends must be
   safe to receive twice" says to key a pickup or a cancel by an id of its own.
   So cancellation and retry are `PUT /workspaces/:ws/cancellations/:id` and
   `PUT /workspaces/:ws/retries/:id`. Without that, a re-sent cancel whose
   first send landed and a fresh "cancel a cancelled subscription", which the
   brief says to refuse, are the same bytes.
3. **Whole-day periods and the outage axis never meet.** §8 "The outside
   world": `outbound_outage` starts in the first 2 s of virtual time and lasts
   at most 4 h. A renewal due a day after subscribing never meets it, so no
   renewal would ever be given up in a sim run. The docs offer no way to move
   the window or give a workload an outage of its own. My workaround: a
   subscription can be *imported* part-way through its current period
   (`startedSecsAgo`, owner only). That is a real need (migrating existing
   subscribers), and it lets the sim put a period end inside the outage.
4. **Seeding API keys needs SHA-256, and the platform exports none.** §8 says
   to seed "an `ApiKey` row in §5's storage form", and §5 says `key_hash` is
   lowercase-hex SHA-256. No hashing helper is public, and nothing says which
   crate to use. `sha2` is in the registry (charpente depends on it), but the
   brief says to use only what charpente ships, so the test file has its own
   small SHA-256.
5. **`ApiKey.created_at` acts as a clock floor.** §7 "Clock errors": the node
   recovers its recorded time from "the newest API key's `created_at`" and
   refuses to open while its clock is behind it. So seeded keys use
   `WallTime::UNIX_EPOCH`. This is easy to miss: §8's fixture advice does not
   mention it.

Not found in the docs:
- A hashing helper for seeding credential rows (point 4).
- Whether `WallTime` implements `Codec` or `Wire`. §2 lists the `Codec`
  impls and `WallTime` is not among them, yet `ApiKey.created_at` is a
  `WallTime` field. I store nanoseconds as `u64` so I don't depend on it.
- Whether a sim kernel reads `Host::bearer_env` variables. §6 says `serve`
  reads them, and nothing says whether `Kernel::with_outbound` does.

## 2026-11-19T23:17Z — writing it, and the first batches

`src/lib.rs` compiled on its first full build, apart from one unused import.
`tests/sim.rs` compiled on its first build with two warnings. The GUIDE's
signatures and `examples/api.rs` were accurate everywhere I relied on them:
`ScopedTx` in a scoped outbound job's `call`/`settle`/`on_dead`, `enqueue_at` on
a `ScopedTx`, `App::transact_for_tests` in the build closure,
`Snapshot::scan_scoped`, `JobRow.scope`, and `Charge::decode` on a job payload.
The sim kernel did not ask for `RENEWLY_BILLING_KEY`, which answers the
`bearer_env` question above in practice. The docs still do not say it.

Sections re-read, and why:
- §8 "A fault-free tail" (twice): to confirm that the tail re-sends in index
  order and that `recovered.resent[i]` overrides `phase.answers[i]`. The
  first point is implied, not stated; I relied on it for "a subscription
  lands before the probe that reads it".
- §8 "The outside world" (twice): first for the outage window, then for
  whether `Idempotent` keys on the header or on `OutboundCall::key` (the
  header, as it says plainly). I wrote my own model in the end, because I
  needed the provider's ledger per period and a count of key conflicts.
- §6 "Outbound jobs" and "Re-issuing a dead job" (twice): to decide between
  `reissue_after` and an owner-enqueued charge with an app key (entry above).
- §16 "What a `ScopedTx` gives you": whether `tx.now()` and `enqueue_at` reach
  through a `ScopedTx`. `enqueue_at` is listed for scoped jobs in §6 ("A
  tenant-scoped job takes the same two methods on its `ScopedTx`"); `now()` is
  listed in §7 ("reachable through a tenant-scoped transaction too").

What the sim told me:
- Batch 1 (`0x73174399a593aee9`): six checks green. One required key,
  `fault:outbound_lost_reply`, never occurred in 256 runs. The message was
  clear and said what to do ("raise --runs, adjust the workload, or drop the
  requirement"). The cause was that the faulted phase made too few provider
  calls, about one per subscription. `examples/modelled_remote.rs` had warned
  of exactly this (it places 48 to 96 orders for the same reason). Fix:
  workspace 2 imports a drawn 4 to 32 subscriptions.
- Batch 3 (`0xb5884e9d322cc930`, a fresh `cargo test`): two checks red, e.g.
  `request 6 was answered 400 {"error":"customer 1 does not exist"}, not 400
  {"error":"an imported subscription must still be inside its current
  period"}`. The `fired:` line settled it at once: `fault:misdirected_write: 1`.
  Media corruption had cut the acked customer row, so a later request was
  answered for the store that was left. Every failing seed I replayed had
  `misdirected_write` or `bit_flip`. Fix: the two answer-judging checks give
  up (never pass) when `durable_corruption_fired`, as the scaffold's
  `a_resent_note…` check does. The shrunk failure block (62 draws to 14,
  eleven fault axes off) made this a two-minute diagnosis.
- Then five fresh master seeds and one 2000-run batch (`--master-seed 0x2026
  --runs 2000`) were green on all seven checks.
- The one unclear number: `acked_requests_are_never_lost` skips about 38% of
  its runs ("no subscribe, cancel or retry was acked in the faulted phase").
  The report says so plainly, but the docs give no way to tell whether that
  rate is normal for a swarm. I believe it is the swarm's `sync_error`
  poisoning the first kernel before its first batch commits. The other six
  checks judge every run, including those whose requests landed only in the
  tail.

Not found in the docs:
- Whether `Request` is `Clone`. I kept the script as `(offset, Request, Op)`
  and unzipped it, so I never needed to know.

## 2026-11-19T23:20Z — `--mutants`, round 1: six one-sided guards

`charpente test --mutants --file src/lib.rs --test-package renewly`
(master seed `0x355e5c3e78e056cb`): 109 mutants, 70 caught, **6 missed**, 33
unviable, 0 timed out. Pass 1 (32 runs) missed 8, and pass 2 (the full 256)
caught two of them: the `==` → `!=` repeat guards in `cancel` and `retry`,
which only the tail's occasional re-send of a landed request exercised. The
six it missed at the full count:

```text
MISSED   src/lib.rs:385:13: replace && with || in put_plan (pass 2)
MISSED   src/lib.rs:384:13: replace && with || in put_plan (pass 2)
MISSED   src/lib.rs:418:39: replace && with || in put_customer (pass 2)
MISSED   src/lib.rs:453:13: replace && with || in subscribe (pass 2)
MISSED   src/lib.rs:452:13: replace && with || in subscribe (pass 2)
MISSED   src/lib.rs:697:59: replace && with || in Charge::pending (pass 2)
```

What each one told me:
- The first five are "the same id with other terms is a conflict" guards.
  The script never reused an id with other terms, so the guards were
  untested. §8 warns about exactly this ("Judge the answers, not only the
  end state: a guard whose `==` is turned into `!=` …"). I had read that
  passage and still missed it for plans, customers and subscriptions. Fix: late in the
  phase, one request per write re-sends the same body (must be 200) and one
  reuses the id with *the first field changed and the rest equal*. That one
  probe catches both `&&` positions of a three-term conjunction. Every
  cancellation and retry is also sent twice under its id now.
- `Charge::pending` was `state == Renewing && period + 1 == self.period`. In
  every reachable state either test alone decides, so `||` is an equivalent
  mutant: no property can catch it, because no run can reach a state where
  the two differ. I did not add a test-only backdoor to reach one. Instead
  the two checks are now separate, and a renewing subscription waiting on
  another period is a named `JobError`, not a silent `Ok(None)`. "Two
  charges live for one subscription" is the precondition of a double charge,
  so it should be loud. The flipped period comparison then breaks every real
  charge, and is caught.

The verb's report was excellent: per-mutant verdicts, the pass that decided
each, the rerun line, and the paragraph on what MISSED means.

## 2026-11-19T23:28Z — `--mutants`, round 2: none missed

Master seed `0xb76287c61043e35f`: 108 mutants, 75 caught, 0 missed, 33
unviable, 0 timed out. Pass 1 caught every viable mutant, so pass 2 had none
to judge. The verb named 8 functions it could not measure, because no
mutant of theirs compiles (they return `ApiError`, `Id<_>` or view types
with no `Default`): `SubState::decode`, `Charge::plan`, `bad`, `build_app`,
`caller`, `not_a_member`, `read_subscription`, `view`. As §8 says, I deleted
effects by hand to watch a check go red:
- `read_subscription` without its `may_act_on` ownership check:
  `workspaces_never_see_each_other` FAILED in 168 of 243 runs (`customer 1
  reaching for subscription 2 was answered 200 {…"customer":"2"…}`).
- `not_a_member`, `bad`, `caller` and `view` carry the texts and fields the
  probes compare byte for byte, and `decode` carries every state round trip.
  Each one failing would turn a probe or a state check red.

## 2026-11-19T23:44Z — the property that guards the worst bug was weak

The brief says charging one period twice is the worst bug. My properties
caught it only *indirectly*: the provider model records any request whose
key is not `renewly-<ws>-<sub>-<period>` (`bad_keys`), and a check fails on
that. A key-format check is a proxy, not the behaviour. So I swapped the
app's key for the platform-minted `IdempotencyKey`, which is the natural first
draft and the bug §6 warns about, and disabled the proxy:

- 2000 runs (`--runs 2000 --master-seed 0x19`): only **1 run in ~1950**
  showed the double charge (`a retried period was charged 2 times: period 1
  of subscription 6 in workspace 2`). Its `fired:` line showed what it
  needed: `fault:outbound_lost_reply`, `fault:outbound_outage`, `dead:Charge`
  and `retry:accepted` in one run. The outage opens in the first 2 s, so a
  call almost never reaches the provider before the outage refuses the rest.

Two changes made it a case the sim reaches. The provider model sometimes
records a charge and then fails the answer (a 502 after the write: real
providers do this, and it is the same ambiguity as a lost reply). And
period ends fall 1 to 120 s in, so some first calls land before the outage
opens. The same bug then failed **4 of 256** runs, in two checks. A new
required key, `retry:of-an-applied-charge`, holds that every batch retries a
charge the provider had already recorded, which is the one case the period
key exists for. It fires about 80 times a batch.

The docs could say this louder. §6 explains why the key matters, and §8
shows `PayKeyless` caught at one pinned seed. Neither says that the outage
axis, by its placement, almost never composes with an earlier landed call.
A property whose verdict rests on that composition needs its own model
behaviour to reach it.

Coverage tuning, measured as §8 asks ("Measure a required key's rate
before you rely on it"). `fault:outbound_lost_reply` fired only about 4
times per batch at first (P(none) ≈ 2%). I raised the workspace-2 bulk to
24 to 64 subscriptions and moved period ends earlier, so calls happen
before disk faults end the phase. Over 28 fresh batches it then fired 2 to
24 times, mean 11. `notice:read` fired 13 times across a whole batch,
because only workspace-1 customers read their notices and they rarely have
one. Workspace 2's customer now reads too: about 32 per batch. The rates are
written beside the `sim_tests!` block.

## 2026-11-19T23:53Z — final runs

`--mutants`, round 3, after the provider-model change above (master seed
`0xadf4d0e6acff9305`): 108 mutants, **75 caught, 0 missed, 33 unviable, 0
timed out**. Pass 1 (32 runs) missed one, the `==` → `!=` repeat guard in
`retry`. Pass 2 caught it at the full count: an accepted retry is rare enough
that 32 runs need not contain one, and that is what the two passes are for.
The 8 functions with no viable mutant are the same as in round 2, and were
checked by hand as described there.

The `charpente test --sim` output below is a fresh batch at an ambient
master seed, exit code 0, printed in full:

```text
running 7 sim checks, 256 runs each, under random injected faults (master seed 0x2fcbb226e9bfc8bc)

skipped runs hit a fault before reaching the check, so they prove nothing either way:
  acked_requests_are_never_lost
    107  no subscribe, cancel or retry was acked in the faulted phase (first: --seed 0x9288beb12a6e13f7)
      8  the boot was refused for durable corruption at or below the store's horizon, which no restart clears, so nothing past it was adjudicated (first: --seed 0x2aca3211059cbc33)
  no_period_is_charged_twice_or_early
    16  the boot was refused for durable corruption at or below the store's horizon, which no restart clears, so nothing past it was adjudicated (first: --seed 0x1a5a0776a8bbdd2c)
  a_cancelled_subscription_is_never_charged_again
    13  the boot was refused for durable corruption at or below the store's horizon, which no restart clears, so nothing past it was adjudicated (first: --seed 0xf9f2a5b00278437d)
     1  durable corruption cut an enqueue whose call was made (first: --seed 0xac7cbe24e60810ce)
  a_given_up_renewal_is_past_due_with_one_notice
    14  the boot was refused for durable corruption at or below the store's horizon, which no restart clears, so nothing past it was adjudicated (first: --seed 0xc3545b5a8ad402bc)
  an_owner_retry_charges_the_same_period_once
    12  the boot was refused for durable corruption at or below the store's horizon, which no restart clears, so nothing past it was adjudicated (first: --seed 0x6b01ce2af5bb35d)
  workspaces_never_see_each_other
    12  the boot was refused for durable corruption at or below the store's horizon, which no restart clears, so nothing past it was adjudicated (first: --seed 0x1557405c514dc8cd)
  a_refused_transition_names_what_is_allowed
    8  the boot was refused for durable corruption at or below the store's horizon, which no restart clears, so nothing past it was adjudicated (first: --seed 0xb477347d1b97e62c)
    1  durable corruption cut an acked record (first: --seed 0x747b4bff4e5d51cd)

check acked_requests_are_never_lost ..................... ok  (141 tested over 141 distinct inputs, 115 skipped)
check no_period_is_charged_twice_or_early ............... ok  (240 tested over 240 distinct inputs, 16 skipped)
check a_cancelled_subscription_is_never_charged_again ... ok  (242 tested over 242 distinct inputs, 14 skipped)
check a_given_up_renewal_is_past_due_with_one_notice .... ok  (242 tested over 242 distinct inputs, 14 skipped)
check an_owner_retry_charges_the_same_period_once ....... ok  (244 tested over 244 distinct inputs, 12 skipped)
check workspaces_never_see_each_other ................... ok  (244 tested over 244 distinct inputs, 12 skipped)
check a_refused_transition_names_what_is_allowed ........ ok  (247 tested over 247 distinct inputs, 9 skipped)

sim result: ok. 7 passed; 0 failed. Runs tested per check, in the order above: 141, 240, 242, 242, 244, 244, 247 of 256.
rerun exactly this batch: charpente test --sim --master-seed 0x2fcbb226e9bfc8bc
```

Read correctly, each check line gives the runs that reached a verdict. 115
skips of `acked_requests_are_never_lost` are runs whose faulted phase died
before any subscribe, cancel or retry was acked: the swarm's disk faults end
the first kernel early. Those runs are not passes, and the batch does not
count them as passes. The other checks judge the recovered store, so they
reach a verdict even in those runs. Every other skip is the platform's own
durable-corruption refusal at boot, or the corruption excuse §8 prescribes.

The full output of the final `charpente test --mutants --file src/lib.rs
--test-package renewly`:

```text
mutants: master seed 0xadf4d0e6acff9305, set on every test this run starts as CHARPENTE_SIM_MASTER_SEED
mutants: this run is two passes, for the sim_tests! harness in renewly: pass 1 judges every mutant on the first 32 runs of each check, failed runs only unless that is the check's full count, and pass 2 judges what pass 1 missed or timed out at the full count
    Finished `test` profile [unoptimized + debuginfo] target(s) in 0.02s
  Executable unittests src/lib.rs (target/debug/deps/renewly-fe82bcc2ec8e7cb9)
  Executable unittests src/main.rs (target/debug/deps/renewly-90c930a62c3eb85f)
  Executable tests/sim.rs (target/debug/deps/sim-32be55a54f205614)
mutants: running `cargo test --package renewly` at the full count, the baseline pass 2 judges against; its output shows only if it fails
mutants: `cargo test` over renewly at the full count is green (29.2 s of wall time)
mutants: pass 1 of 2, CHARPENTE_SIM_FIRST_RUNS=32: a MISSED or TIMEOUT line from it is judged again in pass 2
Found 108 mutants to test
ok       Unmutated baseline in 6s build + 3s test
 INFO Auto-set test timeout to 20s
MISSED   src/lib.rs:563:30: replace == with != in retry in 1s build + 3s test
108 mutants tested in 6m: 1 missed, 74 caught, 33 unviable
mutants: pass 2 of 2, at the full count: the 1 mutant pass 1 did not catch
 INFO Iteration excludes 107 previously caught or unviable mutants
Found 1 mutant to test
ok       Unmutated baseline in 6s build + 30s test
 INFO Auto-set test timeout to 151s
1 mutant tested in 68s: 1 caught
mutants: each mutant's verdict, and the pass that decided it:
  caught   src/lib.rs:57:9: replace Caller::parse -> Option<Caller> with None (pass 1)
  unviable src/lib.rs:57:9: replace Caller::parse -> Option<Caller> with Some(Default::default()) (pass 1)
  caught   src/lib.rs:59:13: delete match arm ("owner", ws, None) in Caller::parse (pass 1)
  caught   src/lib.rs:62:13: delete match arm ("customer", ws, Some(customer)) in Caller::parse (pass 1)
  caught   src/lib.rs:71:9: replace Caller::workspace -> u64 with 0 (pass 1)
  caught   src/lib.rs:71:9: replace Caller::workspace -> u64 with 1 (pass 1)
  caught   src/lib.rs:85:9: replace <impl Tenant for Workspace>::authorize -> Result<(), ApiError> with Ok(()) (pass 1)
  caught   src/lib.rs:86:29: replace match guard caller.workspace() == scope.raw() with true in <impl Tenant for Workspace>::authorize (pass 1)
  caught   src/lib.rs:86:29: replace match guard caller.workspace() == scope.raw() with false in <impl Tenant for Workspace>::authorize (pass 1)
  caught   src/lib.rs:86:48: replace == with != in <impl Tenant for Workspace>::authorize (pass 1)
  unviable src/lib.rs:93:5: replace not_a_member -> ApiError with Default::default() (pass 1)
  caught   src/lib.rs:108:9: replace SubState::name -> &'static str with "" (pass 1)
  caught   src/lib.rs:108:9: replace SubState::name -> &'static str with "xyzzy" (pass 1)
  caught   src/lib.rs:118:9: replace SubState::allows -> &'static[&'static str] with Vec::leak(Vec::new()) (pass 1)
  caught   src/lib.rs:118:9: replace SubState::allows -> &'static[&'static str] with Vec::leak(vec![""]) (pass 1)
  caught   src/lib.rs:118:9: replace SubState::allows -> &'static[&'static str] with Vec::leak(vec!["xyzzy"]) (pass 1)
  caught   src/lib.rs:129:9: replace <impl Codec for SubState>::encode with () (pass 1)
  unviable src/lib.rs:139:9: replace <impl Codec for SubState>::decode -> io::Result<Self> with Ok(Default::default()) (pass 1)
  caught   src/lib.rs:154:5: replace refusal -> String with String::new() (pass 1)
  caught   src/lib.rs:154:5: replace refusal -> String with "xyzzy".into() (pass 1)
  caught   src/lib.rs:249:5: replace next_id -> io::Result<u64> with Ok(0) (pass 1)
  caught   src/lib.rs:249:5: replace next_id -> io::Result<u64> with Ok(1) (pass 1)
  unviable src/lib.rs:260:5: replace bad -> ApiError with Default::default() (pass 1)
  unviable src/lib.rs:264:5: replace caller -> Result<Caller, ApiError> with Ok(Default::default()) (pass 1)
  caught   src/lib.rs:270:5: replace require_owner -> Result<(), ApiError> with Ok(()) (pass 1)
  caught   src/lib.rs:278:5: replace may_act_on -> Result<(), ApiError> with Ok(()) (pass 1)
  caught   src/lib.rs:280:46: replace match guard customer == sub.customer.raw() with true in may_act_on (pass 1)
  caught   src/lib.rs:280:46: replace match guard customer == sub.customer.raw() with false in may_act_on (pass 1)
  caught   src/lib.rs:280:55: replace == with != in may_act_on (pass 1)
  caught   src/lib.rs:290:5: replace period_len -> io::Result<u64> with Ok(0) (pass 1)
  caught   src/lib.rs:290:5: replace period_len -> io::Result<u64> with Ok(1) (pass 1)
  caught   src/lib.rs:297:5: replace charge_key -> String with String::new() (pass 1)
  caught   src/lib.rs:297:5: replace charge_key -> String with "xyzzy".into() (pass 1)
  unviable src/lib.rs:357:5: replace view -> Result<SubscriptionView, ApiError> with Ok(Default::default()) (pass 1)
  unviable src/lib.rs:380:5: replace put_plan -> Result<Id<Plan>, ApiError> with Ok(Id::new()) (pass 1)
  unviable src/lib.rs:380:5: replace put_plan -> Result<Id<Plan>, ApiError> with Ok(Id::from_iter([Default::default()])) (pass 1)
  unviable src/lib.rs:380:5: replace put_plan -> Result<Id<Plan>, ApiError> with Ok(Id::new(Default::default())) (pass 1)
  unviable src/lib.rs:380:5: replace put_plan -> Result<Id<Plan>, ApiError> with Ok(Id::from(Default::default())) (pass 1)
  caught   src/lib.rs:385:13: replace && with || in put_plan (pass 1)
  caught   src/lib.rs:384:13: replace && with || in put_plan (pass 1)
  caught   src/lib.rs:383:22: replace == with != in put_plan (pass 1)
  caught   src/lib.rs:384:33: replace == with != in put_plan (pass 1)
  caught   src/lib.rs:385:33: replace == with != in put_plan (pass 1)
  caught   src/lib.rs:394:25: replace == with != in put_plan (pass 1)
  unviable src/lib.rs:415:5: replace put_customer -> Result<Id<Customer>, ApiError> with Ok(Id::new()) (pass 1)
  unviable src/lib.rs:415:5: replace put_customer -> Result<Id<Customer>, ApiError> with Ok(Id::from_iter([Default::default()])) (pass 1)
  unviable src/lib.rs:415:5: replace put_customer -> Result<Id<Customer>, ApiError> with Ok(Id::new(Default::default())) (pass 1)
  unviable src/lib.rs:415:5: replace put_customer -> Result<Id<Customer>, ApiError> with Ok(Id::from(Default::default())) (pass 1)
  caught   src/lib.rs:418:39: replace && with || in put_customer (pass 1)
  caught   src/lib.rs:418:26: replace == with != in put_customer (pass 1)
  caught   src/lib.rs:418:57: replace == with != in put_customer (pass 1)
  caught   src/lib.rs:426:8: delete ! in put_customer (pass 1)
  unviable src/lib.rs:448:5: replace subscribe -> Result<Id<Subscription>, ApiError> with Ok(Id::new()) (pass 1)
  unviable src/lib.rs:448:5: replace subscribe -> Result<Id<Subscription>, ApiError> with Ok(Id::from_iter([Default::default()])) (pass 1)
  unviable src/lib.rs:448:5: replace subscribe -> Result<Id<Subscription>, ApiError> with Ok(Id::new(Default::default())) (pass 1)
  unviable src/lib.rs:448:5: replace subscribe -> Result<Id<Subscription>, ApiError> with Ok(Id::from(Default::default())) (pass 1)
  caught   src/lib.rs:453:13: replace && with || in subscribe (pass 1)
  caught   src/lib.rs:452:13: replace && with || in subscribe (pass 1)
  caught   src/lib.rs:451:25: replace == with != in subscribe (pass 1)
  caught   src/lib.rs:452:25: replace == with != in subscribe (pass 1)
  caught   src/lib.rs:453:37: replace == with != in subscribe (pass 1)
  caught   src/lib.rs:464:46: replace match guard customer == body.customer.raw() with true in subscribe (pass 1)
  caught   src/lib.rs:464:46: replace match guard customer == body.customer.raw() with false in subscribe (pass 1)
  caught   src/lib.rs:464:55: replace == with != in subscribe (pass 1)
  caught   src/lib.rs:465:40: replace != with == in subscribe (pass 1)
  caught   src/lib.rs:483:36: replace < with == in subscribe (pass 1)
  caught   src/lib.rs:483:36: replace < with > in subscribe (pass 1)
  caught   src/lib.rs:483:36: replace < with <= in subscribe (pass 1)
  unviable src/lib.rs:524:5: replace cancel -> Result<Id<Cancellation>, ApiError> with Ok(Id::new()) (pass 1)
  unviable src/lib.rs:524:5: replace cancel -> Result<Id<Cancellation>, ApiError> with Ok(Id::from_iter([Default::default()])) (pass 1)
  unviable src/lib.rs:524:5: replace cancel -> Result<Id<Cancellation>, ApiError> with Ok(Id::new(Default::default())) (pass 1)
  unviable src/lib.rs:524:5: replace cancel -> Result<Id<Cancellation>, ApiError> with Ok(Id::from(Default::default())) (pass 1)
  caught   src/lib.rs:527:30: replace == with != in cancel (pass 1)
  caught   src/lib.rs:537:18: replace != with == in cancel (pass 1)
  unviable src/lib.rs:560:5: replace retry -> Result<Id<RetryRequest>, ApiError> with Ok(Id::new()) (pass 1)
  unviable src/lib.rs:560:5: replace retry -> Result<Id<RetryRequest>, ApiError> with Ok(Id::from_iter([Default::default()])) (pass 1)
  unviable src/lib.rs:560:5: replace retry -> Result<Id<RetryRequest>, ApiError> with Ok(Id::new(Default::default())) (pass 1)
  unviable src/lib.rs:560:5: replace retry -> Result<Id<RetryRequest>, ApiError> with Ok(Id::from(Default::default())) (pass 1)
  caught   src/lib.rs:563:30: replace == with != in retry (pass 2)
  caught   src/lib.rs:572:18: replace != with == in retry (pass 1)
  caught   src/lib.rs:575:29: replace + with - in retry (pass 1)
  caught   src/lib.rs:575:29: replace + with * in retry (pass 1)
  unviable src/lib.rs:598:5: replace read_subscription -> Result<SubscriptionView, ApiError> with Ok(Default::default()) (pass 1)
  caught   src/lib.rs:612:5: replace list_subscriptions -> Result<Vec<SubscriptionView>, ApiError> with Ok(vec![]) (pass 1)
  unviable src/lib.rs:612:5: replace list_subscriptions -> Result<Vec<SubscriptionView>, ApiError> with Ok(vec![Default::default()]) (pass 1)
  caught   src/lib.rs:624:5: replace read_notices -> Result<Vec<NoticeView>, ApiError> with Ok(vec![]) (pass 1)
  unviable src/lib.rs:624:5: replace read_notices -> Result<Vec<NoticeView>, ApiError> with Ok(vec![Default::default()]) (pass 1)
  caught   src/lib.rs:653:9: replace StartRenewal::run -> Result<(), JobError> with Ok(()) (pass 1)
  caught   src/lib.rs:656:42: replace || with && in StartRenewal::run (pass 1)
  caught   src/lib.rs:656:22: replace != with == in StartRenewal::run (pass 1)
  caught   src/lib.rs:656:60: replace != with == in StartRenewal::run (pass 1)
  caught   src/lib.rs:656:56: replace + with - in StartRenewal::run (pass 1)
  caught   src/lib.rs:656:56: replace + with * in StartRenewal::run (pass 1)
  caught   src/lib.rs:699:9: replace Charge::pending -> Result<Option<Subscription>, JobError> with Ok(None) (pass 1)
  unviable src/lib.rs:699:9: replace Charge::pending -> Result<Option<Subscription>, JobError> with Ok(Some(Default::default())) (pass 1)
  caught   src/lib.rs:702:22: replace != with == in Charge::pending (pass 1)
  caught   src/lib.rs:705:27: replace != with == in Charge::pending (pass 1)
  caught   src/lib.rs:705:23: replace + with - in Charge::pending (pass 1)
  caught   src/lib.rs:705:23: replace + with * in Charge::pending (pass 1)
  unviable src/lib.rs:717:9: replace Charge::plan -> Result<Plan, JobError> with Ok(Default::default()) (pass 1)
  caught   src/lib.rs:726:9: replace Charge::call -> Result<Option<Call>, JobError> with Ok(None) (pass 1)
  unviable src/lib.rs:726:9: replace Charge::call -> Result<Option<Call>, JobError> with Ok(Some(Default::default())) (pass 1)
  caught   src/lib.rs:745:9: replace Charge::settle -> Result<(), JobError> with Ok(()) (pass 1)
  caught   src/lib.rs:745:27: replace != with == in Charge::settle (pass 1)
  caught   src/lib.rs:768:37: replace + with - in Charge::settle (pass 1)
  caught   src/lib.rs:768:37: replace + with * in Charge::settle (pass 1)
  caught   src/lib.rs:777:9: replace Charge::on_dead -> Result<(), JobError> with Ok(()) (pass 1)
  unviable src/lib.rs:806:5: replace build_app -> io::Result<App> with Ok(Default::default()) (pass 1)
mutants: 108 total — 75 caught, 0 missed, 33 unviable, 0 timed out
  8 functions had no viable mutant, so this run says nothing about them (untested):
    src/lib.rs
      <impl Codec for SubState>::decode
      Charge::plan
      bad
      build_app
      caller
      not_a_member
      read_subscription
      view
  To measure one, give its return type a value cargo-mutants can build, such as a derived Default, or check its effect in a property (GUIDE §8 "Finding a one-sided property").
  two passes on master seed 0xadf4d0e6acff9305: pass 1 judged every mutant on the first 32 runs of each sim check, failed runs only unless that is the check's full count, and pass 2 judged the 1 it did not catch at the full count
rerun exactly this mutation run: charpente test --mutants --file src/lib.rs --test-package renewly --master-seed 0xadf4d0e6acff9305
```

## Summary

- **Approaches restarted: 0.** The design from the first reading held. What
  changed afterwards was the test workload (a bulk workspace, earlier period
  ends, a provider that records and then fails) and one guard in
  `Charge::pending`. Each change is above, with the run that forced it.
- **GUIDE sections re-read:** §8 "A fault-free tail" (2×, re-send order and
  answer precedence); §8 "The outside world" (2×, outage window, what
  `Idempotent` keys on); §6 "Outbound jobs" and "Re-issuing a dead job" (2×,
  minted key versus app key); §16 "What a `ScopedTx` gives you" (1×, `now`
  and `enqueue_at` through a scope); §8 "Finding a one-sided property" (2×,
  what "untested" means, and two-pass verdicts); §8 "Giving up" (1×, the
  corruption excuse must give up, not pass).
- **Not in the docs:**
  - a public SHA-256 for seeding `ApiKey` rows;
  - whether `WallTime` is `Codec` or `Wire`;
  - whether a sim kernel reads `Host` credential variables (it did not);
  - whether `Request` is `Clone`;
  - that the tail re-sends in index order (implied, not stated);
  - any way to place an outbound outage later than the first 2 s, or to give
    a workload its own outage;
  - that a seeded `ApiKey.created_at` raises the node's clock floor (§7
    implies it, and §8's fixture advice does not mention it).
- **What the sim said:** it was never unclear about *what* failed. The
  `fired:` line twice settled a question at once (the corruption excuse, and
  the composition a double charge needs). The one number that needed
  thought was the high skip rate of the ack check, which the report states
  plainly but cannot explain.
- **Where the docs fell short:** the outage axis is fixed in time, and the
  docs never say that this makes "a call landed, *then* the provider was
  unreachable" rare. That composition is exactly where an app-chosen
  idempotency key earns its keep. I found it only by planting the bug myself
  and counting.
