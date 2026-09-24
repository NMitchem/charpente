# FINDINGS — statuspage, a docs-only build on charpente

A friction ledger, kept as I go. Sources: BRIEF.md, docs/README.md, docs/GUIDE.md —
nothing else. Dates are the day the entry was written.

## 2026-09-21 — setup

- `charpente new statuspage` wrote six files; `git init` by hand, `[patch.crates-io]`
  appended exactly as the brief gives it. The scaffold's `tests/sim.rs` is the best
  worked example of the give-up idiom in the whole doc set — more useful than GUIDE §8's
  `whole_app`, because it shows `sim.restart()` + `drop(app)` + reopen through
  `build_app`, which is the shape every property here needs.
- Read order: README → GUIDE §1–§9, §11, §12, §13 (launch + describe verb), §14, §16;
  skimmed §10 and §15 (replication and the archive are not in the brief).

## 2026-09-21 — the app (Incident v1), first compile

- **A job cannot open a tenant scope the documented way.** §16 gives exactly one line,
  `tx.scope::<Org>(org, ctx)?`, and `scope` takes `&Ctx` — the compiler said so
  (`expected &Ctx`). A `#[job]`'s `run(self, tx: &mut Tx)` has no `Ctx`, and `Ctx` has
  no public constructor (`Ctx::new`/`default`/`anonymous` all "not found"). §16's "code
  outside a handler that wants the guarantee should go through a transaction and
  `tx.scope::<Org>(org, ctx)?` exactly as a handler does" therefore cannot be followed by
  a job, and the notify job and the maintenance apply job *must* write scoped rows —
  making `Incident`/`Notification` global to get around it would defeat the tenancy
  design the brief asks for. The compiler's own hint ("there is a method `open_scope`")
  surfaced `Tx::open_scope::<Org>(scope, Option<&Principal>) -> Result<ScopedTx, ApiError>`,
  public, which still runs `Org::authorize`. Used it with `None` in the two jobs and in
  the anonymous public routes; `authorize` admits `None` and checks membership for
  `Some`. Recorded as the one undocumented call in `src/`. The docs should show a job
  opening a scope, or say that it cannot.
- Consequence of the above for the policy: an `authorize` that admits `None` means the
  audit rule is "every `auth` route uses `tx.scope(org, ctx)` (principal checked), never
  `open_scope(.., None)`". One line per handler, still — but the reviewer has to know
  which line.
- `Scoped` is a trait that must be imported for `.scope()` to exist (`use
  charpente::Scoped`). §16's first example does import it, silently; the prose never
  says so. The compiler names it, so cost: one rebuild.
- `ScopedTx` lives at `charpente::ScopedTx`, not `charpente::core::table::ScopedTx`; §16
  shows the impl block with no path. Guessed wrong once.
- An SSE handler's return type is checked *textually*: `-> Result<Subscription, ApiError>`
  must be spelled with that identifier, so `use charpente::http::Subscription as Tail`
  fails with "an SSE route returns Result<Subscription, ApiError> — got a different return
  type". My own table is also called `Subscription`, so the public module imports the
  table under an alias instead. The error names the fix, which is the point — but a
  type-name check that is really a token check is worth a sentence in §11.
- `ApiError` has no `Display`, so a job that hits one (opening its scope, sharing a route
  helper) cannot `e.to_string()` it; `JobError::App(format!("{e:?}"))` is what worked.
- `Codec` has no impl for `Vec<Id<T>>` (§2 lists `Vec<u8>` only); an incident's component
  list is a newtype with a length-prefixed hand-rolled `Codec`, the same shape as §2's
  enum example. Fine, but a general `Vec<T: Codec>` impl is the obvious gap.
- `charpente describe --format human` was the fastest check that every route mounts
  without a shape conflict — cheaper than a sim run. Worth calling out in §5.

## 2026-09-21 — auth, as the docs support it

- **There is no way for an operator to prove who they are at a first login.** §5's
  `login` example is `POST /login` with a bare `subject` body on a `public` route —
  `ctx.create_session(tx, subject)` trusts the caller completely — and the docs carry
  no password store, no hashing primitive (SHA-256 exists inside the platform for
  `key_hash`, but nothing app code can call), and no OIDC without a static JWKS. The
  brief says "session login and API keys as §5 describes"; what §5 describes is a
  credential exchange, so that is the design: founding an org (`POST /orgs`, public)
  mints its first operator's API key, `POST /orgs/:org/operators` mints a new
  operator's key for the caller to hand over, and `POST /login` is `auth` — any live
  credential buys a session. Every operator-only route is then `auth` plus the scope
  line. Recorded as a docs gap: §5 should say out loud that the platform's auth is
  "credentials, not passwords", and what a first credential is supposed to come from.
- `ctx.create_api_key` returns the raw key and nothing else; a `DELETE /keys/:id` route
  needs the row's id, and without a hash primitive the only way to find the row it just
  wrote is to `scan::<ApiKey>()` before and after the mint inside the transaction (the
  transaction reads its own writes, §4). Works; ugly. Returning the id — or the row —
  would be the fix.
- `App::open` registers `Session` and `ApiKey` itself, so a `Schema` that lists them
  panics (§9 says so, and I did not hit it — noting that the docs pre-empted it).
- A `public` route that opens a scope with the request's `ctx` would refuse an operator
  of another org reading a public page, because `authorize` gets that operator's
  principal and the page is not theirs. The public page, subscribe and feed therefore
  open the scope as `None` (the anonymous reader) whatever credential the request
  carries — which `open_scope` allows and `scope(org, ctx)` does not. §16 could say that
  a public route wants the anonymous form on purpose.

## 2026-09-21 — sim harness gaps

- Reading a scoped table from the oracle: §16 says `Db::snapshot()` "exposes scoped and
  raw reads that take a scope on the caller's word" but never names them. A compile
  probe found `Snapshot::scan_scoped::<R>(scope: u64)` and `get_scoped::<R>(scope, id)`.
  Used only in `tests/sim.rs`, which is the operator with the `Db` in §16's sense.
- The virtual clock: `sim.now().as_nanos()` (an `Instant` with `as_nanos`), found by
  probe; §8 never shows how a workload reads the time it is at, which every multi-phase
  script needs to place its next `push_at`.
- `serde_json` is not re-exported by `charpente`, and a sim workload has to parse the
  bodies it gets back (ids, keys). Added as a dev-dependency.

## 2026-09-21 — the sim suite

- **`Kernel::into_app()` hands back an `App` that cannot dispatch jobs.** `Kernel::new`
  takes the job registry (and every live SSE tail) into the kernel; a second
  `Kernel::new` on the `App` that `into_app()` returns runs requests fine but every job
  dies after five attempts with `job "NotifySubscribers" is not registered — add it to
  the jobs![…] list passed to App::mount_jobs`. That message sent me to the mount list,
  which was right; a fault-free probe with two kernels in a row reproduced it in one
  minute. §8 does say "the same kernel can be run again against a second SimSource",
  and §8's prose says `into_app` is for reading the end-state — but nothing says the
  App is *spent*, and the scaffold's own `run_kernel` helper (which hands the app
  back to a caller who then builds another kernel) invites exactly this shape. This
  was my one restart of an approach: the harness went from a kernel per phase to one
  kernel per process lifetime, rebuilt only after a crash or a deliberate restart —
  which is also the honest model of a process. Suggest: `Kernel::new` on a spent App
  should refuse by name, or `into_app` should give the registry back.
- Corollary: SSE tails live in the kernel, so a subscribe made in one phase keeps
  delivering across later `run` calls on the same kernel and dies with it — exactly
  like a connection dies with its process. A connection dropped before its first frame
  has nothing to resume from (a cursor names a received event), so "reconnect without a
  cursor" (§11's own fallback) is the only option and the events in between are gone
  from that subscriber's point of view. Not a bug; worth one sentence in §11.
- Replies stay positional through a crash: a request in flight when the loop poisons
  gets its 500 recorded in place, and the rest of the source is served after the
  reboot. I checked this on crash-heavy seeds before trusting index-based parsing of
  a phase's replies; §8 does not say it either way. **(Wrong — see the correction
  below. Kept as written because the mistake is the finding.)**
- Give-up hygiene: a universe whose fault rates admit no open at all can burn 50 s of
  CPU in retry loops proving nothing. A per-universe open budget (512) and a per-boot
  cap (64), with each give-up cause marked as a coverage key (`gave-up:boot`,
  `gave-up:boot-budget`, …) so the batch report carries the distribution, brought the
  suite to ~1.5 s per universe. §8's "Giving up" says to do the coverage-key half;
  the budget half is my own. The give-up rate over the first 64-universe batch was
  ~40%, dominated by boots that never succeed — the fault rate, as the docs say.
- `is_fault_fallout` reads the *body* of a 500 too (it is the same text the poison
  path produces), which is how a cross-org request that hit a platform fault is
  classified as "not adjudicated" rather than "not refused". Useful; not written down.
- `SimSinkHandle` frames are `EventFrame { id, seq, name, json }` — §11's example only
  touches `id`/`seq`; `name`/`json` came from the compiler.
- `SimSource::responses()` yields `(u16, Vec<u8>)`, not the `(status, body)` string
  pair §8 implies; one `from_utf8_lossy`.
- `#[job]`'s derived `Codec` decodes a `JobRow.payload` back into the job struct
  (`NotifySubscribers::decode(&mut &job.payload[..])`) — §8 decodes a single `u64` by
  hand; the struct form is what an oracle over a multi-field payload wants.

## 2026-09-21 — a wrong note, corrected: replies are NOT positional

- The entry above that says "replies stay positional through a crash" was wrong, and
  a 256-universe batch (master seed 0x6c1f27c15ef6c6f0, seed 0x3c8ab3a50c5c44a0) is
  what caught it: 24 requests, 23 replies. A request in flight when the loop poisons
  usually gets its 500 recorded in place — that is what my five hand-checked seeds
  showed — but a `crash:dropped` request gets **no reply at all**, every later reply
  shifts by one, and an index-based assertion reads the wrong request's reply. In
  production that is a client seeing a connection reset instead of a status, so the
  harness is honest; `SimSource::responses()` just does not say that its vector can be
  shorter than what was pushed. The fix was structural: every request is its own
  kernel run, so nothing is ever matched by position (second restart of an approach,
  and the one I would have avoided with a sentence in §8).
- Cost of that shape: ~7.5 s of CPU per universe, up from ~1.5 s, and a somewhat
  higher give-up rate, because every run is a chance to crash on boot. The verdict
  counts are still hundreds per property per 256-universe batch.

## 2026-09-21 — Incident version 2, end to end

- §12's worked example maps onto a scoped table with no surprises:
  `#[row_shape(version = 1)]` needs no tenancy word, `#[table(version = 2, scoped_by =
  Org)]` compiles, `Schema::migration::<Incident>("Incident")` registers the step, and
  `describe` reports `Incident v2 migrations=[1->2]`.
- **The upgrade barrier can only be passed by a step the docs do not describe.** I
  built the v1 binary (commit 12292c9) and the v2 binary, wrote rows through v1 over
  HTTP, stopped it, and started v2 on the same data directory: `table "Incident" has
  unrecovered writes from the previous binary … run the previous binary once to a clean
  shutdown, then upgrade`. §12 says "a clean exit always checkpoints"; §5 says "v0 has
  no graceful shutdown: the process owns the server for as long as it runs". Both are
  true, and together they mean a `launch`-based binary has no clean exit to perform:
  SIGINT is swallowed (the process keeps serving — some handler is installed, but it
  does not stop anything), SIGTERM kills it, and nothing in the docs triggers a
  checkpoint by hand (`checkpoint_every` is the only knob, default 4096 records). What
  actually works: start the old binary again *with no traffic* and kill it —
  recovery itself checkpoints, so the tail is clean — then start the new one. That is
  what the message's remedy amounts to in practice, and it is one restart of a live
  service that must serve no writes in between, which the text should say. After it,
  the v1 row read back as `"severity":"Minor"` and a new v2 row as `Critical`.
- `App::subscription_event_names("feed")` — §11's drift probe — calls the handler with
  `"0"` for every `:param`, so a feed that 404s an unknown org is "not probeable" (the
  error says so, by name). The feed now opens the scope and subscribes regardless: a
  feed for an org that does not exist just never delivers. Fine, but a handler that
  validates its params is the common case, and §11 should mention the `0`.
- `MigrationInfo` (the `describe` contract's migration entry) has no `Debug`, so an
  `assert_eq!` on the list needs the fields spelled out. Trivial; noted.

## 2026-09-21 — `charpente test --mutants` over the job, the sweep and the state machine

Run at commit `dbe20b3` (the tree the 10,000-universe batch was then started from, minus
a later commit that touched only `src/operators.rs`, `tests/sim.rs` and `client.ts` —
none of the mutated files; the sim change only added assertions), from the app
directory:

```text
$ charpente test --mutants --file src/notify.rs --file src/maintenance.rs \
    --file src/incidents.rs --test-package statuspage --timeout 1200
Found 54 mutants to test
ok       Unmutated baseline in 6s build + 141s test
54 mutants tested in 84m: 35 caught, 19 unviable
mutants: 54 total — 35 caught, 0 missed, 19 unviable, 0 timed out
```

Exit 0. Every mutant, by verdict (cargo-mutants' own `caught.txt` / `unviable.txt`;
`missed.txt` and `timeout.txt` were empty):

```text
--- caught (35)
src/incidents.rs:160:9: replace IncidentState::next -> Option<IncidentState> with None
src/incidents.rs:172:5: replace check_transition -> Result<(), ApiError> with Ok(())
src/incidents.rs:173:23: replace match guard next == to with true in check_transition
src/incidents.rs:173:23: replace match guard next == to with false in check_transition
src/incidents.rs:173:28: replace == with != in check_transition
src/incidents.rs:188:5: replace check_components -> Result<(), ApiError> with Ok(())
src/incidents.rs:256:11: replace == with != in transition
src/incidents.rs:327:5: replace list_components -> Result<Vec<ComponentView>, ApiError> with Ok(vec![])
src/incidents.rs:383:23: replace == with != in post_update
src/incidents.rs:409:5: replace list_incidents -> Result<Vec<IncidentView>, ApiError> with Ok(vec![])
src/incidents.rs:442:5: replace list_subscriptions -> Result<Vec<SubscriptionView>, ApiError> with Ok(vec![])
src/incidents.rs:465:5: replace list_notifications -> Result<Vec<NotificationView>, ApiError> with Ok(vec![])
src/maintenance.rs:68:19: replace <= with > in schedule_window
src/maintenance.rs:100:5: replace list_windows -> Result<Vec<MaintenanceWindowView>, ApiError> with Ok(vec![])
src/maintenance.rs:114:9: replace MaintenanceSweep::run -> Result<(), JobError> with Ok(())
src/maintenance.rs:115:38: replace + with - in MaintenanceSweep::run
src/maintenance.rs:115:38: replace + with * in MaintenanceSweep::run
src/maintenance.rs:132:5: replace window_is_due -> bool with true
src/maintenance.rs:132:5: replace window_is_due -> bool with false
src/maintenance.rs:132:47: replace && with || in window_is_due
src/maintenance.rs:132:63: replace <= with > in window_is_due
src/maintenance.rs:134:68: replace && with || in window_is_due
src/maintenance.rs:134:36: replace && with || in window_is_due
src/maintenance.rs:134:82: replace <= with > in window_is_due
src/maintenance.rs:135:14: replace || with && in window_is_due
src/maintenance.rs:147:9: replace ApplyWindow::run -> Result<(), JobError> with Ok(())
src/maintenance.rs:154:39: replace && with || in ApplyWindow::run
src/maintenance.rs:154:55: replace <= with > in ApplyWindow::run
src/maintenance.rs:168:71: replace && with || in ApplyWindow::run
src/maintenance.rs:168:39: replace && with || in ApplyWindow::run
src/maintenance.rs:168:85: replace <= with > in ApplyWindow::run
src/maintenance.rs:180:31: replace != with == in ApplyWindow::run
src/notify.rs:21:9: replace NotifySubscribers::run -> Result<(), JobError> with Ok(())
src/notify.rs:34:36: replace != with == in NotifySubscribers::run
src/notify.rs:41:38: replace >= with < in NotifySubscribers::run
--- unviable (19)
src/incidents.rs:47:9: replace ComponentView::of -> Self with Default::default()
src/incidents.rs:73:9: replace IncidentView::of -> Self with Default::default()
src/incidents.rs:96:9: replace <impl From<IncidentUpdate> for IncidentUpdateView>::from -> Self with Default::default()
src/incidents.rs:160:9: replace IncidentState::next -> Option<IncidentState> with Some(Default::default())
src/incidents.rs:216:5: replace open -> Result<Incident, ApiError> with Ok(Default::default())
src/incidents.rs:246:5: replace transition -> Result<Incident, ApiError> with Ok(Default::default())
src/incidents.rs:291:5: replace create_component -> Result<ComponentView, ApiError> with Ok(Default::default())
src/incidents.rs:314:5: replace set_component_status -> Result<ComponentView, ApiError> with Ok(Default::default())
src/incidents.rs:327:5: replace list_components -> Result<Vec<ComponentView>, ApiError> with Ok(vec![Default::default()])
src/incidents.rs:342:5: replace open_incident -> Result<IncidentView, ApiError> with Ok(Default::default())
src/incidents.rs:367:5: replace transition_incident -> Result<IncidentView, ApiError> with Ok(Default::default())
src/incidents.rs:381:5: replace post_update -> Result<IncidentUpdateView, ApiError> with Ok(Default::default())
src/incidents.rs:409:5: replace list_incidents -> Result<Vec<IncidentView>, ApiError> with Ok(vec![Default::default()])
src/incidents.rs:424:5: replace get_incident -> Result<IncidentDetail, ApiError> with Ok(Default::default())
src/incidents.rs:442:5: replace list_subscriptions -> Result<Vec<SubscriptionView>, ApiError> with Ok(vec![Default::default()])
src/incidents.rs:465:5: replace list_notifications -> Result<Vec<NotificationView>, ApiError> with Ok(vec![Default::default()])
src/maintenance.rs:46:9: replace MaintenanceWindowView::of -> Self with Default::default()
src/maintenance.rs:67:5: replace schedule_window -> Result<MaintenanceWindowView, ApiError> with Ok(Default::default())
src/maintenance.rs:100:5: replace list_windows -> Result<Vec<MaintenanceWindowView>, ApiError> with Ok(vec![Default::default()])
```

Reading it: the 19 unviable are all `Default::default()` replacements on view and row
types that derive no `Default` — the type system refusing the mutant, as §8 says. Of
the 35 caught, the ones that earn their keep are the sweep's `window_is_due` operators
(nine of them, every `&&`/`||`/`<=` inverted separately), `ApplyWindow`'s guards, the
notify job's stamp comparison (`>=` → `<` would notify only the late subscribers), and
`transition`'s `== Resolved` (an inverted resolve emits `IncidentUpdated` and enqueues
nothing — the notification oracle sees a resolved incident with no job). 84 minutes of
wall clock: each viable mutant is a rebuild plus the 256-universe batch `cargo test`
runs, ~2.5 minutes.

What I would have predicted before running it, for the record: exactly this split. The
oracles were written as end-state consistency checks (rows against rows, rows against
jobs, the clock against the hours), which is the shape that notices a job doing
nothing; the one I added *because* I could not see how a mutant would be caught
otherwise was the "enqueued N times over R restarts" bound, which is what catches the
`window_is_due -> true` family.

## 2026-09-22 — the first 10,000-universe batch: 28 failures, one cause

- `charpente test --sim --universes 10000` (master seed `0x48393f6e4e0955f4`, 1h24m of
  wall clock, 27,110 verdicts, 22,862 give-ups) failed 28 of 50,000 universes. Every
  `fired:` line carried `fault:bit_flip: 1` or `fault:misdirected_write: 1` — durable
  corruption — and every message was a history-dependent expectation: an org's key
  reading as `401 unrecognized credential`, a state-machine step finding the incident
  one state back or `404`, and twice "org beta holds component gamma-api". §8
  documents that recovery legitimately cuts an acked record above the checkpoint
  horizon in such a universe, and documents `durable_corruption_fired` for exactly
  this — but only in the context of an acked-durability *shortfall*. What I had to
  work out from the failures: (1) the cut voids *every* expectation that reads "X was
  acked earlier, therefore this reply is …" — a credential, a transition, a note —
  and it has to be checked at the moment of judging, because the fault fires when it
  fires (my chain evaluated it once, at creation, and missed 8 of the 28); (2) a cut
  rewinds an app's own id counter (§2's pattern), so ids get re-minted: gamma's org
  came back with beta's old id, and a tag-vs-scope check read that as a leak. The
  end-state consistency oracles — rows against rows, jobs, the clock — were never
  affected, which is the split §8 draws ("a cut reverts state to a prefix, which stays
  consistent"), and they stay strong; the history checks now yield to the excuse.
- The id-reuse point deserves its own sentence in §2 or §8: an app whose clients hold
  ids minted from a counter row will, after a corruption cut on a single node, hand a
  stale id a *different* row. Nothing in the counter pattern's text says so, and a
  follower (§10) is the only thing that prevents it.
- Every one of the 28 repro lines replayed byte-for-byte to the same message before
  the fix and to a green (or give-up) after it. The seed contract holds.
- Give-up rate over the batch: 46%, dominated by boots that never succeed and
  end-state reads the faulted disk refuses. That is the fault matrix; the verdict
  counts (5–10k per property) are what the claim rests on.

## 2026-09-22 — the second 10,000-universe batch: green

- Re-run on commit `1d0a0c4` (the check-time corruption excuse), from the app directory,
  1h23m of wall clock:

```text
$ charpente test --sim --universes 10000
sim: 10000 universe(s) per test from master seed 0x58129e7e4b18d000
sim: 6075 of 10000 universes of "acked_writes_survive_restarts" gave up — reached no verdict
sim: 4991 of 10000 universes of "notifications_are_resolved_incidents_times_subscriptions_at_resolve" gave up — reached no verdict
sim: 5439 of 10000 universes of "no_org_observes_another_org" gave up — reached no verdict
sim: 6375 of 10000 universes of "maintenance_windows_open_once_and_resolve_once_never_early" gave up — reached no verdict
sim: 10000 universe(s) per test, 27120 verdict(s), 22880 gave up, no failures (master seed: 0x58129e7e4b18d000)
EXIT=0
```

- **Master seed: `0x58129e7e4b18d000`.** `charpente test --sim --universes 10000
  --master-seed 0x58129e7e4b18d000` replays the batch.
- The give-up counts match the first batch to within 0.5% per property (6070/6075,
  4981/4991, 5404/5439, 6407/6375) from a different master seed, which is the number I
  would expect if give-ups are a property of the fault matrix and not of the workload:
  the same boot-failure and refused-read rates fall out of any seed. 27,120 verdicts
  stand behind the five claims; `a_universe_replays_identically` never gives up.
- Nothing to add to the docs from this run beyond the previous section; a green batch
  produces no friction, only the seed.

## 2026-09-22 — `--mutants` again, on the final tree

The first run judged with the `tests/sim.rs` of `dbe20b3`; the sim change since (the
check-time corruption excuse) loosened history expectations, and a mutant only ever
caught by such an expectation would have gone MISSED without anyone noticing. So the
same command on `daa9bdb` (source and tests identical to HEAD), from the app
directory, 1h47m of wall clock (the machine was busier than during the first run):

```text
$ charpente test --mutants --file src/notify.rs --file src/maintenance.rs \
    --file src/incidents.rs --test-package statuspage --timeout 1200
Found 54 mutants to test
ok       Unmutated baseline in 7s build + 124s test
54 mutants tested in 2h: 35 caught, 19 unviable
mutants: 54 total — 35 caught, 0 missed, 19 unviable, 0 timed out
```

`caught.txt` and `unviable.txt` are byte-identical to the first run's, so every
catch stands on an end-state oracle, not on a history check the excuse can void.

## Restarts of an approach

1. The sim harness: kernel-per-phase → kernel-per-process-lifetime.
2. The sim harness again: batched requests with positional replies → one request per
   run. Everything else was iteration on the same shape.

## GUIDE sections re-read, and why

- §16 (twice) — first for the shape of `authorize`, then hunting for how a job opens a
  scope; it is not there.
- §11 — for the SSE handler signature after the textual return-type error; again for
  cursor semantics when deciding how a dropped connection resumes.
- §8 (three times) — the give-up idiom, the `whole_app` example for `Kernel`/`SimSource`,
  and the checkers' signatures (`acked_implies_durable` takes `sim`, not a seed).
- §6 — retry backoff arithmetic, to size the drain phase (30 s doubling × 5 ≈ 8 min).
- §7 — the logical-clock pattern, to decide what unit a maintenance window is
  scheduled in (sweep ticks).
- §12 (twice) — the worked example, then the upgrade barrier after it fired; and §5's
  "Starting the server" and §3's checkpoint paragraph hunting for a clean exit.

## Not findable in the docs

- How an operator authenticates for the first time. (Nothing; credentials are minted
  by a route that is already trusted, and `POST /orgs` is the bootstrap.)
- How a job (no `Ctx`) opens a tenant scope. (`Tx::open_scope`, via the compiler.)
- The names of `Snapshot`'s scoped reads. (`scan_scoped`/`get_scoped`, via the compiler.)
- How a workload reads virtual "now". (`sim.now().as_nanos()`, via the compiler.)
- That `Kernel::into_app()` returns a spent App. (A fault-free probe.)
- Whether a crash leaves a hole in a phase's recorded replies. (It can: a dropped
  request records nothing. Found by a 256-universe batch, after five seeds said no.)
- How a `launch`-based binary exits cleanly, which the upgrade barrier's remedy
  requires. (It cannot; recovery checkpoints, so a traffic-free restart stands in.)
- What `subscription_event_names` feeds a handler's `:param`s. (`0`; the error text.)
- The fields of an SSE frame as a sim sink records it. (The compiler.)
