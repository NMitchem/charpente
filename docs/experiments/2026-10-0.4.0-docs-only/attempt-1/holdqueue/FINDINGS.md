# FINDINGS: holdqueue on charpente 0.4.0, built from the docs alone

A friction ledger, written as I went. Every entry is dated 2026-11-01 (the whole build took one session). Sources used: BRIEF.md, the charpente README, the GUIDE.md shipped in `charpente-0.4.0`, its `examples/` (api.rs, drawn_inputs.rs, modelled_remote.rs, restart_under_faults.rs). I opened none of the `tests/` files GUIDE names as worked examples (`tests/clock.rs`, `tests/outbound.rs`); `modelled_remote.rs` and GUIDE section 8 were enough for the outbound side. No `src/` of any charpente crate was read, and the framework was not patched.

## What was built

- `src/lib.rs`: tables (Branch, Title, Stock, Hold, Return, Patron, OperatorSeat), wire shapes, and 12 routes. Operator routes: bootstrap the first operator key, mint a patron key, register branches and titles, record copy counts, record returns, record pickups, list the waiting queue of a title at a branch. Patron routes: place, cancel, list own holds, read one own hold.
- `src/queue.rs`: the state machine (requested, ready for pickup, picked up, expired, cancelled), the allocation of free copies to the earliest-placed requested hold of the same title at the same branch (`serve`), refusals by name, and the hourly `Tick` cron job that advances a logical hour clock and expires holds ready for more than 168 hours.
- `src/notify.rs`: `NotifyReady`, an outbound job to `notify.example.org`, enqueued in the same transaction that makes a hold ready; its message names the title and the branch and carries the platform-minted idempotency key.
- `tests/sim.rs`: one drawn scenario (2 branches, 2 titles, drawn copy counts, 2 to 10 holds by 3 patrons, drawn pickups, cancels, returns, restocks, repeats, one-field conflicts, intruders, queue and hold reads, the operator bootstrap and a patron key mint) driven through `charpente_sim::drive` over a 9-day faulted phase and a 9-day fault-free tail, judged by 8 properties:
  1. `an_acked_hold_is_never_lost`: an acked placement, pickup or cancel stands after the crash.
  2. `copies_go_to_holds_in_placement_order`: from per-hold sequence stamps; plus no idle free copy while a hold waits; plus operator queue reads come in placement order.
  3. `copies_are_never_set_aside_twice_or_beyond_stock`: set aside equals ready holds, out equals pickups less returns, set aside plus out never exceeds copies.
  4. `a_hold_is_ready_once_and_its_patron_is_told`: one notification job per ready hold, Done, one message received per ready hold naming title and branch, none for any other hold.
  5. `refused_transitions_name_what_is_allowed`: exact refusal text per state, checked against a table written in the test; allowed transitions answer 200 and stand; intruders are refused.
  6. `ready_holds_expire_after_seven_days`: expired exactly at the first tick past 168 hours; nothing still ready past it; nothing picked up past it.
  7. `repeats_are_answered_alike_and_conflicts_by_name`: repeats answered alike, conflicts refused by name, copy counts below what is committed refused naming the least allowed count.
  8. `callers_reach_only_what_is_theirs`: one bootstrap ever, a patron key minted for that patron, a patron lists and reads only their own holds.

## Decisions the docs forced (and where they bend the brief)

- **Seven days without a clock.** GUIDE section 4 and section 7 ("A logical clock") say no clock reaches a transaction and there is no wall-clock primitive in v0. There is also no delayed enqueue ("run this job at time T"). So expiry runs on the documented pattern: an hourly cron job advances a counter row, and a hold made ready at hour h expires at the first tick past h + 168. Consequence, straight from GUIDE section 7 "Catch-up fires once, never backfills": if the node is down for a day, the logical clock loses a day, and holds wait that much longer than seven calendar days. Under the sim `clock_downtime` axis (jumps of up to three days) this is exactly what happens. The behaviour is "seven days of node uptime, at least".
- **Repeats versus refusals.** The brief says picking up a hold that is not ready is refused. GUIDE section 8 ("A route the tail re-sends must be safe to receive twice") says a repeat must be answered with what the first one did. These collide on a re-sent pickup whose first send landed. I chose: a pickup of a hold already picked up, and a cancel of a hold already cancelled, are repeats and answer 200 with the hold unchanged; every other disallowed transition is refused with a 400 that names the verb, the state and the transitions allowed from it. `ApiError` has no conflict variant (section 5: "There is no 403", and no 409 either), so conflicts are 400s too.
- **Returns are per title per branch,** recorded under an id the desk gives, so a re-sent return frees one copy, not two. A return needs a copy of that title out from that branch; the docs gave no model of copy identity, and the brief does not need one.
- **Pickup is recorded by an operator** (the desk); cancel by the patron who owns the hold.

## Restarts of approach: 3

1. `Patron` started as a marker struct so holds could carry `Id<Patron>`. GUIDE section 5 says "`String` and `Id<T>` (any `T`) satisfy both" `FromParam` and `Wire`. The compiler said `the trait bound Patron: Row is not satisfied ... required for charpente::Id<Patron> to implement Wire`. Restarted with `Patron` as a one-field global table, written when an operator mints the patron a key.
2. Credentials in the sim. First plan: drive the bootstrap route and read the token back. A `Drive` faulted phase is one fixed script, so a token minted by request k cannot be put into request k+1, and dropping below the helper for this alone looked like rebuilding `Drive`. Restarted: the test writes its own `ApiKey` rows when its app boots (through `App::transact_for_tests`), in the storage form GUIDE section 5 documents (the lowercase-hex SHA-256 of the raw key, and the subject). The bootstrap and mint routes are still exercised in the scenario; only the credentials the script presents are seeded.
3. The first sim suite (6 checks) went green, and then `--mutants` reported 11 MISSED in `place`, `record_return` and `set_copies` (output below). The scenario never sent a repeat, a conflicting reuse of an id, or a copy count at or below what was committed, so the repeat and conflict branches and the comparison in the refusal were never judged. Restarted the scenario: repeats, one-field conflicts, lowering restocks, and a seventh check that judges those answers by name. Later added an eighth check, for the bootstrap, the key mint and own-hold reads, so `src/lib.rs` could join the mutation scope.

## GUIDE sections I re-read, and why

- **Section 5, "Auth: sessions, API keys, and the resolution ladder"** (three times): looking for how a first credential is obtained. The text says charpente has no password check and the first credential "comes from something the app trusts, such as an IdP or a signup route that mints a key". There is no CLI verb and no documented bootstrap. I wrote a one-time `POST /operator` that mints the first operator key and refuses after. I also re-read it for the exact `ApiKey` storage form, for the sim.
- **Section 5, "The route argument convention":** after the `Id<Patron>` compile error, to check whether I had misread "any `T`". I had not.
- **Section 7, "A logical clock" and "Catch-up fires once":** to confirm there is no sanctioned wall time inside a job or a route, and what downtime does to a cron-built clock.
- **Section 8, "A fault-free tail" and "A route the tail re-sends must be safe to receive twice":** to settle the repeat-versus-refusal collision above.
- **Section 8, "Giving up" and `durable_corruption_fired`:** twice, after two red runs that were media corruption cutting an acked record (below). The rule "excuse only where it fired, and give up rather than pass" was clear once re-read.
- **Section 8, "Finding a one-sided property":** to understand why pass 1 streamed `tick` and `Tick::run` as MISSED. Pass 1 judges failed runs only, and those mutants are caught only by an unmet `require` (nothing ever expires, so `judged:expired` never fires), which only the full-count pass 2 judges. Pass 2 caught them, as the section says.

## What I could not find in the docs

- `Id<T>` crossing the wire only for `T: Row`. Section 5 says any `T` (restart 1).
- Any way to obtain a first credential: no CLI verb, no documented bootstrap, and `api.rs` itself shows a `login` that trusts a subject from the body, which section 5 says never to do.
- Whether app code may `tx.put` an `ApiKey` row. Section 5 documents `tx.delete::<ApiKey>(id)` as an ordinary write; put is not mentioned. It worked.
- A delayed enqueue, or any wall time in a transaction or job. Expiry had to be built on the logical clock pattern, with the downtime caveat above.
- `TxError<E>` has no `Display`; `e.to_string()` does not compile. Not stated; `{e:?}` works.
- The body type in `Phase::answers` and `Recovered::resent` is `Vec<u8>`, not `String`. api.rs says the answer "holds the body too" and no more; the compiler told me.
- The list of `FaultConfig` field names for `--faults-off`. A bad name answers `--faults-off: "bogus" is not a FaultConfig field` and does not list the valid ones. I assembled names from the `shrunk:` and `fired:` lines of earlier failures.
- Whether an index lookup (`by_slot`, `by_state`) inside a transaction sees that transaction own earlier writes. Section 4 says reads do; I relied on it for index scans too, and the sim agrees.
- How to raise the verdict rate of a property that needs an ack before the first crash (next section).

## What the sim output told me, and where it was unclear

- **Clear:** the `fired:` line. My first red run (`Pickup { hold: 1 } was answered 200 but hold 1 ends Some(Expired)`) carried `fault:misdirected_write: 1` and shrank to 22 draws; that line alone said the acked pickup record was cut by media corruption, which GUIDE section 8 excuses only as a give-up. The fix was in my check, not the app. The second (`Copies ... was refused with "no title 1"`) had the same shape (`fault:misdirected_write: 1`). The third (`hold 3 became ready at a branch that holds no copies of its title`) shrank to `fired: outbound:completed: 1, restart: 1`, no fault at all, which told me at once it was a plain bug in the check: a copy count may be lowered after a hold is served.
- **Clear:** the per-check lines and the skipped-reasons block. `untested` and unmet-`require` failures read exactly as documented, and the hand-deletion runs below went red with every unmet key named.
- **Unclear:** `an_acked_hold_is_never_lost` reaches a verdict in only about half its runs (123 of 256 in the final batch; 118 skipped as "no placement, pickup or cancel was acked"). The report says how many and why, in my own words, but nothing says what stopped those runs: a skipped run has no `fired:` line, and a `--seed` replay prints only the reason and the detail. I added temporary debug prints and bisected with `--faults-off`: with `write_error,sync_error,read_error` off the seed answered everything; with them on, four boots in a row met injected fsync failures (correctly classified as fallout and retried) and the kernel that finally booted died before answering its first request. Moving placements from minutes to seconds after boot changed nothing (131 against 129 tested on the same master seed). The swarm disk mixes are simply that harsh. The other seven checks reach a verdict in 239 to 253 of 256 runs, because they judge the recovered state, which the tail re-sends into existence.
- **Unclear, minor:** pass 1 of `--mutants` streams coverage-only catches as MISSED. The final table attributes them correctly to pass 2, but a reader of the streaming log sees MISSED first.

## Hand deletions for functions the mutation tool could not measure

`--mutants` names 18 functions with no viable mutant (return types with no `Default`). Following GUIDE section 8, I deleted two effects by hand, watched checks go red, and restored them from git:
- `put_branch` with its `tx.put` removed: all 8 checks FAILED at 32 runs (`Copies { branch: 1, title: 1, copies: 0 } was answered 400 no branch 1: ...`, and every `require` unmet).
- `refuse` with a trailing period added to its text: `refused_transitions_name_what_is_allowed` FAILED in 20 of 30 tested runs (`... which is not the refusal of any state`).

## `charpente test --sim`, final (2026-11-01)

Run from `holdqueue/` at the final commit. Exit 0. `cargo test` runs the same sim binary (green, another master seed) and `cargo clippy --all-targets -- -D warnings` is clean.

```text
running 8 sim checks, 256 runs each, under random injected faults (master seed 0x4a3bc8dfdc80c1da)

skipped runs hit a fault before reaching the check, so they prove nothing either way:
  an_acked_hold_is_never_lost
    118  no placement, pickup or cancel was acked, so durability has nothing to prove (first: --seed 0xb8993ed0c11a4c0c)
     13  the boot was refused for durable corruption at or below the store's horizon, which no restart clears, so nothing past it was adjudicated (first: --seed 0xe94862597a99ca81)
      2  durable corruption fired, so an acked record is excused (first: --seed 0x52503a3818a321b8)
  copies_go_to_holds_in_placement_order
    3  the boot was refused for durable corruption at or below the store's horizon, which no restart clears, so nothing past it was adjudicated (first: --seed 0xb8b80770ccdf5bd2)
  copies_are_never_set_aside_twice_or_beyond_stock
    11  the boot was refused for durable corruption at or below the store's horizon, which no restart clears, so nothing past it was adjudicated (first: --seed 0x4e622c63761764dd)
  a_hold_is_ready_once_and_its_patron_is_told
    15  the boot was refused for durable corruption at or below the store's horizon, which no restart clears, so nothing past it was adjudicated (first: --seed 0x8d52e608039bf0d7)
  refused_transitions_name_what_is_allowed
    9  the boot was refused for durable corruption at or below the store's horizon, which no restart clears, so nothing past it was adjudicated (first: --seed 0x5094917992581e6a)
  callers_reach_only_what_is_theirs
    11  the boot was refused for durable corruption at or below the store's horizon, which no restart clears, so nothing past it was adjudicated (first: --seed 0x77e9630901600e3e)
  repeats_are_answered_alike_and_conflicts_by_name
    10  the boot was refused for durable corruption at or below the store's horizon, which no restart clears, so nothing past it was adjudicated (first: --seed 0x48111cc664b27d96)
     1  durable corruption fired, so an acked record is excused (first: --seed 0x643be0ecedbc923c)
  ready_holds_expire_after_seven_days
    17  the boot was refused for durable corruption at or below the store's horizon, which no restart clears, so nothing past it was adjudicated (first: --seed 0xff32ce0c2d1d1136)

check an_acked_hold_is_never_lost ........................ ok  (123 tested over 123 distinct inputs, 133 skipped)
check copies_go_to_holds_in_placement_order .............. ok  (253 tested over 253 distinct inputs, 3 skipped)
check copies_are_never_set_aside_twice_or_beyond_stock ... ok  (245 tested over 245 distinct inputs, 11 skipped)
check a_hold_is_ready_once_and_its_patron_is_told ........ ok  (241 tested over 241 distinct inputs, 15 skipped)
check refused_transitions_name_what_is_allowed ........... ok  (247 tested over 247 distinct inputs, 9 skipped)
check callers_reach_only_what_is_theirs .................. ok  (245 tested over 245 distinct inputs, 11 skipped)
check repeats_are_answered_alike_and_conflicts_by_name ... ok  (245 tested over 245 distinct inputs, 11 skipped)
check ready_holds_expire_after_seven_days ................ ok  (239 tested over 239 distinct inputs, 17 skipped)

sim result: ok. 8 passed; 0 failed. Runs tested per check, in the order above: 123, 253, 245, 241, 247, 245, 245, 239 of 256.
rerun exactly this batch: charpente test --sim --master-seed 0x4a3bc8dfdc80c1da
```

## `charpente test --mutants`, final (2026-11-01)

`charpente test --mutants --file src/queue.rs --file src/notify.rs --file src/lib.rs --test-package holdqueue`, the full output. Result: 119 mutants, 82 caught, 0 missed, 37 unviable, 0 timed out.

```text
mutants: master seed 0x2c5fe249bb7ccc00, set on every test this run starts as CHARPENTE_SIM_MASTER_SEED
mutants: this run is two passes, for the sim_tests! harness in holdqueue: pass 1 judges every mutant on the first 32 runs of each check, failed runs only unless that is the check's full count, and pass 2 judges what pass 1 missed or timed out at the full count
    Finished `test` profile [unoptimized + debuginfo] target(s) in 0.02s
  Executable unittests src/lib.rs (target/debug/deps/holdqueue-f1ca2a758205d28a)
  Executable unittests src/main.rs (target/debug/deps/holdqueue-0658c5dff0298da6)
  Executable tests/sim.rs (target/debug/deps/sim-6370986e825bf851)
mutants: running `cargo test --package holdqueue` at the full count, the baseline pass 2 judges against; its output shows only if it fails
mutants: `cargo test` over holdqueue at the full count is green (45.1 s of wall time)
mutants: pass 1 of 2, CHARPENTE_SIM_FIRST_RUNS=32: a MISSED or TIMEOUT line from it is judged again in pass 2
Found 119 mutants to test
ok       Unmutated baseline in 7s build + 5s test
 INFO Auto-set test timeout to 29s
MISSED   src/queue.rs:130:27: replace + with - in overdue in 1s build + 11s test
MISSED   src/queue.rs:386:5: replace tick -> io::Result<()> with Ok(()) in 1s build + 4s test
MISSED   src/queue.rs:388:16: replace += with -= in tick in 1s build + 12s test
MISSED   src/queue.rs:388:16: replace += with *= in tick in 1s build + 5s test
MISSED   src/queue.rs:417:9: replace Tick::run -> Result<(), JobError> with Ok(()) in 1s build + 5s test
119 mutants tested in 9m: 5 missed, 77 caught, 37 unviable
mutants: pass 2 of 2, at the full count: the 5 mutants pass 1 did not catch
 INFO Iteration excludes 114 previously caught or unviable mutants
Found 5 mutants to test
ok       Unmutated baseline in 6s build + 44s test
 INFO Auto-set test timeout to 223s
5 mutants tested in 6m: 5 caught
mutants: each mutant's verdict, and the pass that decided it:
  caught   src/lib.rs:68:9: replace Stock::free -> u32 with 0 (pass 1)
  caught   src/lib.rs:68:9: replace Stock::free -> u32 with 1 (pass 1)
  caught   src/lib.rs:68:51: replace + with - in Stock::free (pass 1)
  caught   src/lib.rs:68:51: replace + with * in Stock::free (pass 1)
  unviable src/lib.rs:147:9: replace <impl From<Stock> for StockView>::from -> Self with Default::default() (pass 1)
  unviable src/lib.rs:170:9: replace <impl From<Hold> for HoldView>::from -> Self with Default::default() (pass 1)
  caught   src/lib.rs:188:5: replace subject -> Result<String, ApiError> with Ok(String::new()) (pass 1)
  caught   src/lib.rs:188:5: replace subject -> Result<String, ApiError> with Ok("xyzzy".into()) (pass 1)
  caught   src/lib.rs:195:5: replace require_operator -> Result<(), ApiError> with Ok(()) (pass 1)
  caught   src/lib.rs:196:12: replace == with != in require_operator (pass 1)
  unviable src/lib.rs:207:5: replace require_patron -> Result<Id<Patron>, ApiError> with Ok(Id::new()) (pass 1)
  unviable src/lib.rs:207:5: replace require_patron -> Result<Id<Patron>, ApiError> with Ok(Id::from_iter([Default::default()])) (pass 1)
  unviable src/lib.rs:207:5: replace require_patron -> Result<Id<Patron>, ApiError> with Ok(Id::new(Default::default())) (pass 1)
  unviable src/lib.rs:207:5: replace require_patron -> Result<Id<Patron>, ApiError> with Ok(Id::from(Default::default())) (pass 1)
  caught   src/lib.rs:220:5: replace patron_subject -> String with String::new() (pass 1)
  caught   src/lib.rs:220:5: replace patron_subject -> String with "xyzzy".into() (pass 1)
  caught   src/lib.rs:229:5: replace bootstrap_operator -> Result<String, ApiError> with Ok(String::new()) (pass 1)
  caught   src/lib.rs:229:5: replace bootstrap_operator -> Result<String, ApiError> with Ok("xyzzy".into()) (pass 1)
  caught   src/lib.rs:242:5: replace mint_patron_key -> Result<String, ApiError> with Ok(String::new()) (pass 1)
  caught   src/lib.rs:242:5: replace mint_patron_key -> Result<String, ApiError> with Ok("xyzzy".into()) (pass 1)
  unviable src/lib.rs:250:5: replace put_branch -> Result<Id<Branch>, ApiError> with Ok(Id::new()) (pass 1)
  unviable src/lib.rs:250:5: replace put_branch -> Result<Id<Branch>, ApiError> with Ok(Id::from_iter([Default::default()])) (pass 1)
  unviable src/lib.rs:250:5: replace put_branch -> Result<Id<Branch>, ApiError> with Ok(Id::new(Default::default())) (pass 1)
  unviable src/lib.rs:250:5: replace put_branch -> Result<Id<Branch>, ApiError> with Ok(Id::from(Default::default())) (pass 1)
  unviable src/lib.rs:261:5: replace put_title -> Result<Id<Title>, ApiError> with Ok(Id::new()) (pass 1)
  unviable src/lib.rs:261:5: replace put_title -> Result<Id<Title>, ApiError> with Ok(Id::from_iter([Default::default()])) (pass 1)
  unviable src/lib.rs:261:5: replace put_title -> Result<Id<Title>, ApiError> with Ok(Id::new(Default::default())) (pass 1)
  unviable src/lib.rs:261:5: replace put_title -> Result<Id<Title>, ApiError> with Ok(Id::from(Default::default())) (pass 1)
  unviable src/lib.rs:279:5: replace put_copies -> Result<StockView, ApiError> with Ok(Default::default()) (pass 1)
  unviable src/lib.rs:286:5: replace record_return -> Result<StockView, ApiError> with Ok(Default::default()) (pass 1)
  unviable src/lib.rs:293:5: replace waiting_queue -> Result<HoldList, ApiError> with Ok(Default::default()) (pass 1)
  unviable src/lib.rs:304:5: replace pick_up -> Result<HoldView, ApiError> with Ok(Default::default()) (pass 1)
  unviable src/lib.rs:315:5: replace place_hold -> Result<HoldView, ApiError> with Ok(Default::default()) (pass 1)
  unviable src/lib.rs:322:5: replace cancel_hold -> Result<HoldView, ApiError> with Ok(Default::default()) (pass 1)
  unviable src/lib.rs:329:5: replace my_holds -> Result<HoldList, ApiError> with Ok(Default::default()) (pass 1)
  unviable src/lib.rs:339:5: replace my_hold -> Result<HoldView, ApiError> with Ok(Default::default()) (pass 1)
  caught   src/lib.rs:341:20: replace match guard h.patron == patron with true in my_hold (pass 1)
  caught   src/lib.rs:341:20: replace match guard h.patron == patron with false in my_hold (pass 1)
  caught   src/lib.rs:341:29: replace == with != in my_hold (pass 1)
  unviable src/lib.rs:351:5: replace build_app -> io::Result<App> with Ok(Default::default()) (pass 1)
  caught   src/notify.rs:43:9: replace NotifyReady::call -> Result<Option<Call>, JobError> with Ok(None) (pass 1)
  unviable src/notify.rs:43:9: replace NotifyReady::call -> Result<Option<Call>, JobError> with Ok(Some(Default::default())) (pass 1)
  caught   src/notify.rs:68:9: replace NotifyReady::settle -> Result<(), JobError> with Ok(()) (pass 1)
  caught   src/notify.rs:69:12: delete ! in NotifyReady::settle (pass 1)
  caught   src/queue.rs:21:32: replace * with + (pass 1)
  caught   src/queue.rs:21:32: replace * with / (pass 1)
  caught   src/queue.rs:41:9: replace HoldState::name -> &str with "" (pass 1)
  caught   src/queue.rs:41:9: replace HoldState::name -> &str with "xyzzy" (pass 1)
  caught   src/queue.rs:52:9: replace HoldState::allowed -> &str with "" (pass 1)
  caught   src/queue.rs:52:9: replace HoldState::allowed -> &str with "xyzzy" (pass 1)
  caught   src/queue.rs:68:9: replace <impl Codec for HoldState>::encode with () (pass 1)
  unviable src/queue.rs:79:9: replace <impl Codec for HoldState>::decode -> io::Result<Self> with Ok(Default::default()) (pass 1)
  caught   src/queue.rs:106:5: replace next -> io::Result<u64> with Ok(0) (pass 1)
  caught   src/queue.rs:106:5: replace next -> io::Result<u64> with Ok(1) (pass 1)
  caught   src/queue.rs:109:18: replace += with -= in next (pass 1)
  caught   src/queue.rs:109:18: replace += with *= in next (pass 1)
  caught   src/queue.rs:124:5: replace hour -> io::Result<u64> with Ok(0) (pass 1)
  caught   src/queue.rs:124:5: replace hour -> io::Result<u64> with Ok(1) (pass 1)
  caught   src/queue.rs:130:5: replace overdue -> bool with true (pass 1)
  caught   src/queue.rs:130:5: replace overdue -> bool with false (pass 1)
  caught   src/queue.rs:130:14: replace > with == in overdue (pass 1)
  caught   src/queue.rs:130:14: replace > with < in overdue (pass 1)
  caught   src/queue.rs:130:14: replace > with >= in overdue (pass 1)
  caught   src/queue.rs:130:27: replace + with - in overdue (pass 2)
  caught   src/queue.rs:130:27: replace + with * in overdue (pass 1)
  caught   src/queue.rs:135:5: replace slot_key -> String with String::new() (pass 1)
  caught   src/queue.rs:135:5: replace slot_key -> String with "xyzzy".into() (pass 1)
  caught   src/queue.rs:140:5: replace find_stock -> io::Result<Option<Stock>> with Ok(None) (pass 1)
  unviable src/queue.rs:140:5: replace find_stock -> io::Result<Option<Stock>> with Ok(Some(Default::default())) (pass 1)
  caught   src/queue.rs:146:5: replace waiting -> io::Result<Vec<Hold>> with Ok(vec![]) (pass 1)
  unviable src/queue.rs:146:5: replace waiting -> io::Result<Vec<Hold>> with Ok(vec![Default::default()]) (pass 1)
  caught   src/queue.rs:148:29: replace == with != in waiting (pass 1)
  caught   src/queue.rs:158:5: replace serve -> io::Result<Option<Stock>> with Ok(None) (pass 1)
  unviable src/queue.rs:158:5: replace serve -> io::Result<Option<Stock>> with Ok(Some(Default::default())) (pass 1)
  caught   src/queue.rs:163:25: replace == with != in serve (pass 1)
  caught   src/queue.rs:169:25: replace += with -= in serve (pass 1)
  caught   src/queue.rs:169:25: replace += with *= in serve (pass 1)
  unviable src/queue.rs:178:5: replace no_branch -> ApiError with Default::default() (pass 1)
  unviable src/queue.rs:185:5: replace no_title -> ApiError with Default::default() (pass 1)
  unviable src/queue.rs:194:5: replace refuse -> ApiError with Default::default() (pass 1)
  unviable src/queue.rs:206:5: replace set_copies -> Result<Stock, ApiError> with Ok(Default::default()) (pass 1)
  caught   src/queue.rs:224:15: replace < with == in set_copies (pass 1)
  caught   src/queue.rs:224:15: replace < with > in set_copies (pass 1)
  caught   src/queue.rs:224:15: replace < with <= in set_copies (pass 1)
  caught   src/queue.rs:224:33: replace + with - in set_copies (pass 1)
  caught   src/queue.rs:224:33: replace + with * in set_copies (pass 1)
  unviable src/queue.rs:249:5: replace place -> Result<Hold, ApiError> with Ok(Default::default()) (pass 1)
  caught   src/queue.rs:250:57: replace && with || in place (pass 1)
  caught   src/queue.rs:250:34: replace && with || in place (pass 1)
  caught   src/queue.rs:250:24: replace == with != in place (pass 1)
  caught   src/queue.rs:250:48: replace == with != in place (pass 1)
  caught   src/queue.rs:250:72: replace == with != in place (pass 1)
  caught   src/queue.rs:286:5: replace end -> io::Result<()> with Ok(()) (pass 1)
  unviable src/queue.rs:296:5: replace stock_of -> Result<Stock, ApiError> with Ok(Default::default()) (pass 1)
  unviable src/queue.rs:309:5: replace pick_up -> Result<Hold, ApiError> with Ok(Default::default()) (pass 1)
  caught   src/queue.rs:313:29: replace -= with += in pick_up (pass 1)
  caught   src/queue.rs:313:29: replace -= with /= in pick_up (pass 1)
  caught   src/queue.rs:314:27: replace += with -= in pick_up (pass 1)
  caught   src/queue.rs:314:27: replace += with *= in pick_up (pass 1)
  unviable src/queue.rs:328:5: replace cancel -> Result<Hold, ApiError> with Ok(Default::default()) (pass 1)
  caught   src/queue.rs:329:23: replace match guard hold.patron == patron with true in cancel (pass 1)
  caught   src/queue.rs:329:23: replace match guard hold.patron == patron with false in cancel (pass 1)
  caught   src/queue.rs:329:35: replace == with != in cancel (pass 1)
  caught   src/queue.rs:339:29: replace -= with += in cancel (pass 1)
  caught   src/queue.rs:339:29: replace -= with /= in cancel (pass 1)
  unviable src/queue.rs:353:5: replace record_return -> Result<Stock, ApiError> with Ok(Default::default()) (pass 1)
  caught   src/queue.rs:354:34: replace && with || in record_return (pass 1)
  caught   src/queue.rs:354:24: replace == with != in record_return (pass 1)
  caught   src/queue.rs:354:48: replace == with != in record_return (pass 1)
  caught   src/queue.rs:370:22: replace == with != in record_return (pass 1)
  caught   src/queue.rs:377:19: replace -= with += in record_return (pass 1)
  caught   src/queue.rs:377:19: replace -= with /= in record_return (pass 1)
  caught   src/queue.rs:386:5: replace tick -> io::Result<()> with Ok(()) (pass 2)
  caught   src/queue.rs:388:16: replace += with -= in tick (pass 2)
  caught   src/queue.rs:388:16: replace += with *= in tick (pass 2)
  caught   src/queue.rs:395:12: delete ! in tick (pass 1)
  caught   src/queue.rs:399:29: replace -= with += in tick (pass 1)
  caught   src/queue.rs:399:29: replace -= with /= in tick (pass 1)
  caught   src/queue.rs:417:9: replace Tick::run -> Result<(), JobError> with Ok(()) (pass 2)
mutants: 119 total — 82 caught, 0 missed, 37 unviable, 0 timed out
  18 functions had no viable mutant, so this run says nothing about them (untested):
    src/lib.rs
      <impl From<Hold> for HoldView>::from
      <impl From<Stock> for StockView>::from
      build_app
      cancel_hold
      my_holds
      pick_up
      place_hold
      put_branch
      put_copies
      put_title
      record_return
      require_patron
      waiting_queue
    src/queue.rs
      <impl Codec for HoldState>::decode
      no_branch
      no_title
      refuse
      stock_of
  To measure one, give its return type a value cargo-mutants can build, such as a derived Default, or check its effect in a property (GUIDE §8 "Finding a one-sided property").
  two passes on master seed 0x2c5fe249bb7ccc00: pass 1 judged every mutant on the first 32 runs of each sim check, failed runs only unless that is the check's full count, and pass 2 judged the 5 it did not catch at the full count
rerun exactly this mutation run: charpente test --mutants --file src/queue.rs --file src/notify.rs --file src/lib.rs --test-package holdqueue --master-seed 0x2c5fe249bb7ccc00
```

## `charpente test --mutants`, first run, before restart 3 (2026-11-01)

`charpente test --mutants --file src/queue.rs --file src/notify.rs --test-package holdqueue`, the full output, kept as the record of the 11 one-sided gaps restart 3 fixed.

```text
mutants: master seed 0x259ff25e6654df1f, set on every test this run starts as CHARPENTE_SIM_MASTER_SEED
mutants: this run is two passes, for the sim_tests! harness in holdqueue: pass 1 judges every mutant on the first 32 runs of each check, failed runs only unless that is the check's full count, and pass 2 judges what pass 1 missed or timed out at the full count
    Finished `test` profile [unoptimized + debuginfo] target(s) in 0.04s
  Executable unittests src/lib.rs (target/debug/deps/holdqueue-f1ca2a758205d28a)
  Executable unittests src/main.rs (target/debug/deps/holdqueue-0658c5dff0298da6)
  Executable tests/sim.rs (target/debug/deps/sim-6370986e825bf851)
mutants: running `cargo test --package holdqueue` at the full count, the baseline pass 2 judges against; its output shows only if it fails
mutants: `cargo test` over holdqueue at the full count is green (49.1 s of wall time)
mutants: pass 1 of 2, CHARPENTE_SIM_FIRST_RUNS=32: a MISSED or TIMEOUT line from it is judged again in pass 2
Found 79 mutants to test
ok       Unmutated baseline in 10s build + 4s test
 INFO Auto-set test timeout to 24s
MISSED   src/queue.rs:68:9: replace <impl Codec for HoldState>::encode with () in 1s build + 5s test
MISSED   src/queue.rs:130:27: replace + with - in overdue in 1s build + 9s test
MISSED   src/queue.rs:146:5: replace waiting -> io::Result<Vec<Hold>> with Ok(vec![]) in 1s build + 4s test
MISSED   src/queue.rs:224:15: replace < with == in set_copies in 1s build + 4s test
MISSED   src/queue.rs:224:15: replace < with > in set_copies in 1s build + 4s test
MISSED   src/queue.rs:224:15: replace < with <= in set_copies in 1s build + 4s test
MISSED   src/queue.rs:224:33: replace + with * in set_copies in 1s build + 4s test
MISSED   src/queue.rs:250:57: replace && with || in place in 1s build + 4s test
MISSED   src/queue.rs:250:34: replace && with || in place in 1s build + 4s test
MISSED   src/queue.rs:250:24: replace == with != in place in 1s build + 4s test
MISSED   src/queue.rs:250:48: replace == with != in place in 1s build + 4s test
MISSED   src/queue.rs:250:72: replace == with != in place in 1s build + 5s test
MISSED   src/queue.rs:354:34: replace && with || in record_return in 1s build + 4s test
MISSED   src/queue.rs:354:24: replace == with != in record_return in 1s build + 4s test
MISSED   src/queue.rs:354:48: replace == with != in record_return in 1s build + 4s test
MISSED   src/queue.rs:386:5: replace tick -> io::Result<()> with Ok(()) in 1s build + 3s test
MISSED   src/queue.rs:388:16: replace += with -= in tick in 1s build + 9s test
MISSED   src/queue.rs:388:16: replace += with *= in tick in 1s build + 4s test
MISSED   src/queue.rs:417:9: replace Tick::run -> Result<(), JobError> with Ok(()) in 1s build + 6s test
79 mutants tested in 8m: 19 missed, 46 caught, 14 unviable
mutants: pass 2 of 2, at the full count: the 19 mutants pass 1 did not catch
 INFO Iteration excludes 60 previously caught or unviable mutants
Found 19 mutants to test
ok       Unmutated baseline in 18s build + 34s test
 INFO Auto-set test timeout to 173s
MISSED   src/queue.rs:224:15: replace < with == in set_copies in 1s build + 34s test
MISSED   src/queue.rs:224:15: replace < with <= in set_copies in 1s build + 34s test
MISSED   src/queue.rs:224:33: replace + with * in set_copies in 1s build + 34s test
MISSED   src/queue.rs:250:57: replace && with || in place in 1s build + 34s test
MISSED   src/queue.rs:250:34: replace && with || in place in 1s build + 33s test
MISSED   src/queue.rs:250:24: replace == with != in place in 1s build + 34s test
MISSED   src/queue.rs:250:48: replace == with != in place in 1s build + 38s test
MISSED   src/queue.rs:250:72: replace == with != in place in 1s build + 33s test
MISSED   src/queue.rs:354:34: replace && with || in record_return in 1s build + 37s test
MISSED   src/queue.rs:354:24: replace == with != in record_return in 1s build + 35s test
MISSED   src/queue.rs:354:48: replace == with != in record_return in 1s build + 34s test
19 mutants tested in 14m: 11 missed, 8 caught
mutants: each mutant's verdict, and the pass that decided it:
  caught   src/notify.rs:43:9: replace NotifyReady::call -> Result<Option<Call>, JobError> with Ok(None) (pass 1)
  unviable src/notify.rs:43:9: replace NotifyReady::call -> Result<Option<Call>, JobError> with Ok(Some(Default::default())) (pass 1)
  caught   src/notify.rs:68:9: replace NotifyReady::settle -> Result<(), JobError> with Ok(()) (pass 1)
  caught   src/notify.rs:69:12: delete ! in NotifyReady::settle (pass 1)
  caught   src/queue.rs:21:32: replace * with + (pass 1)
  caught   src/queue.rs:21:32: replace * with / (pass 1)
  caught   src/queue.rs:41:9: replace HoldState::name -> &str with "" (pass 1)
  caught   src/queue.rs:41:9: replace HoldState::name -> &str with "xyzzy" (pass 1)
  caught   src/queue.rs:52:9: replace HoldState::allowed -> &str with "" (pass 1)
  caught   src/queue.rs:52:9: replace HoldState::allowed -> &str with "xyzzy" (pass 1)
  caught   src/queue.rs:68:9: replace <impl Codec for HoldState>::encode with () (pass 2)
  unviable src/queue.rs:79:9: replace <impl Codec for HoldState>::decode -> io::Result<Self> with Ok(Default::default()) (pass 1)
  caught   src/queue.rs:106:5: replace next -> io::Result<u64> with Ok(0) (pass 1)
  caught   src/queue.rs:106:5: replace next -> io::Result<u64> with Ok(1) (pass 1)
  caught   src/queue.rs:109:18: replace += with -= in next (pass 1)
  caught   src/queue.rs:109:18: replace += with *= in next (pass 1)
  caught   src/queue.rs:124:5: replace hour -> io::Result<u64> with Ok(0) (pass 1)
  caught   src/queue.rs:124:5: replace hour -> io::Result<u64> with Ok(1) (pass 1)
  caught   src/queue.rs:130:5: replace overdue -> bool with true (pass 1)
  caught   src/queue.rs:130:5: replace overdue -> bool with false (pass 1)
  caught   src/queue.rs:130:14: replace > with == in overdue (pass 1)
  caught   src/queue.rs:130:14: replace > with < in overdue (pass 1)
  caught   src/queue.rs:130:14: replace > with >= in overdue (pass 1)
  caught   src/queue.rs:130:27: replace + with - in overdue (pass 2)
  caught   src/queue.rs:130:27: replace + with * in overdue (pass 1)
  caught   src/queue.rs:135:5: replace slot_key -> String with String::new() (pass 1)
  caught   src/queue.rs:135:5: replace slot_key -> String with "xyzzy".into() (pass 1)
  caught   src/queue.rs:140:5: replace find_stock -> io::Result<Option<Stock>> with Ok(None) (pass 1)
  unviable src/queue.rs:140:5: replace find_stock -> io::Result<Option<Stock>> with Ok(Some(Default::default())) (pass 1)
  caught   src/queue.rs:146:5: replace waiting -> io::Result<Vec<Hold>> with Ok(vec![]) (pass 2)
  unviable src/queue.rs:146:5: replace waiting -> io::Result<Vec<Hold>> with Ok(vec![Default::default()]) (pass 1)
  caught   src/queue.rs:148:29: replace == with != in waiting (pass 1)
  caught   src/queue.rs:158:5: replace serve -> io::Result<Option<Stock>> with Ok(None) (pass 1)
  unviable src/queue.rs:158:5: replace serve -> io::Result<Option<Stock>> with Ok(Some(Default::default())) (pass 1)
  caught   src/queue.rs:163:25: replace == with != in serve (pass 1)
  caught   src/queue.rs:169:25: replace += with -= in serve (pass 1)
  caught   src/queue.rs:169:25: replace += with *= in serve (pass 1)
  unviable src/queue.rs:178:5: replace no_branch -> ApiError with Default::default() (pass 1)
  unviable src/queue.rs:185:5: replace no_title -> ApiError with Default::default() (pass 1)
  unviable src/queue.rs:194:5: replace refuse -> ApiError with Default::default() (pass 1)
  unviable src/queue.rs:206:5: replace set_copies -> Result<Stock, ApiError> with Ok(Default::default()) (pass 1)
  MISSED   src/queue.rs:224:15: replace < with == in set_copies (pass 2)
  caught   src/queue.rs:224:15: replace < with > in set_copies (pass 2)
  MISSED   src/queue.rs:224:15: replace < with <= in set_copies (pass 2)
  caught   src/queue.rs:224:33: replace + with - in set_copies (pass 1)
  MISSED   src/queue.rs:224:33: replace + with * in set_copies (pass 2)
  unviable src/queue.rs:249:5: replace place -> Result<Hold, ApiError> with Ok(Default::default()) (pass 1)
  MISSED   src/queue.rs:250:57: replace && with || in place (pass 2)
  MISSED   src/queue.rs:250:34: replace && with || in place (pass 2)
  MISSED   src/queue.rs:250:24: replace == with != in place (pass 2)
  MISSED   src/queue.rs:250:48: replace == with != in place (pass 2)
  MISSED   src/queue.rs:250:72: replace == with != in place (pass 2)
  caught   src/queue.rs:286:5: replace end -> io::Result<()> with Ok(()) (pass 1)
  unviable src/queue.rs:296:5: replace stock_of -> Result<Stock, ApiError> with Ok(Default::default()) (pass 1)
  unviable src/queue.rs:309:5: replace pick_up -> Result<Hold, ApiError> with Ok(Default::default()) (pass 1)
  caught   src/queue.rs:313:29: replace -= with += in pick_up (pass 1)
  caught   src/queue.rs:313:29: replace -= with /= in pick_up (pass 1)
  caught   src/queue.rs:314:27: replace += with -= in pick_up (pass 1)
  caught   src/queue.rs:314:27: replace += with *= in pick_up (pass 1)
  unviable src/queue.rs:328:5: replace cancel -> Result<Hold, ApiError> with Ok(Default::default()) (pass 1)
  caught   src/queue.rs:329:23: replace match guard hold.patron == patron with true in cancel (pass 1)
  caught   src/queue.rs:329:23: replace match guard hold.patron == patron with false in cancel (pass 1)
  caught   src/queue.rs:329:35: replace == with != in cancel (pass 1)
  caught   src/queue.rs:339:29: replace -= with += in cancel (pass 1)
  caught   src/queue.rs:339:29: replace -= with /= in cancel (pass 1)
  unviable src/queue.rs:353:5: replace record_return -> Result<Stock, ApiError> with Ok(Default::default()) (pass 1)
  MISSED   src/queue.rs:354:34: replace && with || in record_return (pass 2)
  MISSED   src/queue.rs:354:24: replace == with != in record_return (pass 2)
  MISSED   src/queue.rs:354:48: replace == with != in record_return (pass 2)
  caught   src/queue.rs:370:22: replace == with != in record_return (pass 1)
  caught   src/queue.rs:377:19: replace -= with += in record_return (pass 1)
  caught   src/queue.rs:377:19: replace -= with /= in record_return (pass 1)
  caught   src/queue.rs:386:5: replace tick -> io::Result<()> with Ok(()) (pass 2)
  caught   src/queue.rs:388:16: replace += with -= in tick (pass 2)
  caught   src/queue.rs:388:16: replace += with *= in tick (pass 2)
  caught   src/queue.rs:395:12: delete ! in tick (pass 1)
  caught   src/queue.rs:399:29: replace -= with += in tick (pass 1)
  caught   src/queue.rs:399:29: replace -= with /= in tick (pass 1)
  caught   src/queue.rs:417:9: replace Tick::run -> Result<(), JobError> with Ok(()) (pass 2)
mutants: 79 total — 54 caught, 11 missed, 14 unviable, 0 timed out
  11 MISSED: no test IN THE PACKAGES UNDER TEST caught this mutant. That is either a one-sided property, or an oracle that lives in a package outside --test-package. Without --test-package the verb judges every mutant with charpente-core and charpente-sim's tests — the default target set's oracles — so a --file in any other crate must name its own package here, or its mutants are judged by suites that never exercise them and all report MISSED; the scope is part of the claim. Re-run with --property NAME to ask which single property is blind to it. See the run directory for the surviving diffs.
  5 functions had no viable mutant, so this run says nothing about them (untested):
    src/queue.rs
      <impl Codec for HoldState>::decode
      no_branch
      no_title
      refuse
      stock_of
  To measure one, give its return type a value cargo-mutants can build, such as a derived Default, or check its effect in a property (GUIDE §8 "Finding a one-sided property").
  two passes on master seed 0x259ff25e6654df1f: pass 1 judged every mutant on the first 32 runs of each sim check, failed runs only unless that is the check's full count, and pass 2 judged the 19 it did not catch at the full count
rerun exactly this mutation run: charpente test --mutants --file src/queue.rs --file src/notify.rs --test-package holdqueue --master-seed 0x259ff25e6654df1f
```
