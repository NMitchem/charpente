# Attempt 1: code review of `statuspage` (third docs-only build)

Reviewer: blind code review, dated 2026-09-23. App at HEAD `6037704`, clean tree, 18 commits.
Inputs read: BRIEF.md; GUIDE §2, §5, §8, §11, §12, §16 (plus §6/§7 for the job and cron
contracts); every file under `statuspage/src` and `statuspage/tests`; `gates.md`; the
platform source where a question could only be settled there. I did not open FINDINGS.md,
logs/, transcript.jsonl, git-log.txt, or any other file under `.superpowers/`.

Citations: README means the frozen copy `statuspage-run/docs/README.md`, GUIDE means
`statuspage-run/docs/GUIDE.md`. Line numbers are from those files.

**Counts: 2 Critical, 5 Important, 6 Minor.** Both Criticals were demonstrated by running
code, not by reasoning alone. Neither is a defect that a platform claim says it would refuse,
and each citation below says so explicitly.

Scratch artifacts, all under the session scratchpad
`/private/tmp/claude-501/-Users-noahmitchem-Documents-GitHub-charpente/bbc5b92d-8ec8-49a0-ad5b-126420ad85a5/scratchpad/`:
`upgrade/` (the v1-to-v2 probe workspace), `pristine/`, `review-copy/` (write routes
flipped to `public`), `review-copy-2/` (guards removed or weakened), `c1/` `c2/` `c3/`
(public.rs guards removed), and the `*-sim.log` files. Nothing in the app repository was
edited, built into, or committed to.

---

## The shape chosen for public reads (the question the controller asked)

**What it is.** `Org::authorize` (src/lib.rs:42-58) returns `Ok(())` for `who == None`
unconditionally, and for `Some(p)` it requires `p` to resolve to an `Operator` row whose
`org` is the scope. The app then opens a scope in two different ways:

- Every `auth` route calls `tx.scope::<Org>(org, ctx)?`. The platform's `auth` gate
  guarantees `ctx.principal()` is `Some` (GUIDE L911-915), so these routes always run the
  operator-membership branch.
- The three anonymous operations (status_page, subscribe, and feed, at src/public.rs:41, :90,
  :127) and both jobs (NotifySubscribers at src/notify.rs:24, ApplyWindow at
  src/maintenance.rs:149) call the lower-level `tx.open_scope::<Org>(org, None)`. They pass
  `None` on purpose, even when the request carried a credential (public.rs:1-3), so an
  operator of org A can still read org B's public page.
- `MaintenanceSweep` reads every org's windows through the declared `cross_tenant`
  capability (src/maintenance.rs:109-127), which is read-only, and hands each write off
  to a per-org `ApplyWindow`.

**Is it sound?** It is sound as shipped. I traced every route and both jobs:

- Every operator write route is `auth` and opens its scope with the caller's principal.
- Every `public` route does only what the brief grants anonymous callers: read the page,
  read the feed, subscribe. The one exception is org creation (Important I-2).
- Every event is emitted through a `ScopedTx`, so it is keyed (§11 L3355-3363).
- The notify job and the apply job only ever open the org their payload names, and that
  payload was written inside that org's scope.

But it is **fail-open by construction**. Once `authorize` admits `None`, a `ScopedTx`
handed to an anonymous caller carries `put`/`delete` (GUIDE L5415-5428). The whole
write-authorization story then rests on two conventions:

1. Every write route says `auth`.
2. `open_scope(org, None)` appears only on public routes and in jobs.

Neither convention is checked by the compiler, and neither is checked by any property in
the suite. I showed both directly. Flipping all seven operator write routes from `auth`
to `public` gives an anonymous caller 200s on create-component, open-incident, and every
transition through Resolved. The whole sim suite still comes back byte-identical to
pristine on the same master seed (Important I-1). Rewriting four write routes to use
`open_scope(org, None)`, and deleting `create_operator`'s check outright, likewise gives a
byte-identical report (Critical C-2).

In this app, the line GUIDE calls "the one auditable line" (L5335-5336; "the property a
reviewer checks by reading one line per handler", L5667-5668) no longer audits anything on
its own. `tx.scope::<Org>(org, ctx)?` admits an anonymous caller whenever the route is
`public`, so a reviewer must also read the route attribute.

**Does GUIDE §16 as written lead here?** Yes, and it leads here twice over.

1. **Public reads.** §16's only `authorize` example (L5364-5371) refuses `None`. §11's
   worked tenant feed (L3369-3375) is a `public` SSE route that opens the scope with
   `tx.scope::<Org>(org, ctx)?` and comments "authorizes the subscriber, too". Put
   together, those two examples yield a "public" feed that only that org's operators can
   read. The brief asks for an anonymous feed, anonymous page reads, and anonymous
   subscribes, all against scoped rows. With these docs the only way to get that is to
   admit `None`. The one sentence about `None` (L5362-5363, "an `authorize` that admits
   `None` has declared anonymous access to that scope") never says the access is total.
   `authorize` receives `(tx, who, scope)` and nothing about the operation, so an app
   cannot admit anonymous reads while refusing anonymous writes. GUIDE never states this.
2. **Jobs.** GUIDE says a job reaches scoped rows only "through a handle that the app's own
   membership check had to hand back" (L5322-5324). It also says that "a route handler and
   a job receive a `Tx`; through a `Tx`, a scoped table is unreachable without
   `tx.scope::<Org>(org, ctx)?`" (L5665-5667). A job has no `ctx`, and GUIDE never shows a
   job opening a scope. `open_scope` appears nowhere in GUIDE or README; I grepped both and
   found zero hits. The builder found the rustdoc-only primitive `Tx::open_scope(scope,
   who)` (charpente-core/src/db.rs:1157). A job has no principal to pass, so it passes
   `None`. `NotifySubscribers` has to write the scoped `Notification` rows, so `authorize`
   must admit `None` for the brief's own notify job to work at all. The `cross_tenant` job
   capability does not help here, because it is read-only (L5598-5599).

A shape that fails closed was reachable from the source but not from the docs:

- `authorize` refuses `None`.
- Jobs and the three public operations pass a synthetic `Principal`. Its fields are `pub`
  (charpente/src/auth/ladder.rs:26-29), and it would carry a subject no credential in this
  app can be minted for.

With that shape, a write route mistakenly marked `public` would refuse anonymous callers
instead of serving them. The builder could not have learned any of this from GUIDE. The
platform gap is that `authorize` can see neither the operation nor whether the caller is a
route or a job.

---

## Critical

### C-1. The v2 commit reshaped a durable event. After the upgrade, an anonymous resume from before it stops the node.

**Where:** `IncidentOpened` (src/incidents.rs:14-21). Commit `dd1993a` inserted
`pub severity: Severity` between `title` and `maintenance`, in the same commit that bumps
`Incident` to version 2.

**Why it breaks.** Event rows in `charpente.events` are durable `Codec` bytes. By default
65,536 of them are retained (charpente-core/src/db.rs:76), so v1-written `IncidentOpened`
rows outlive the upgrade. Delivery decodes each matching row with the current struct
(`ev_json`, charpente/src/http/mod.rs:257-266). A v1 row decodes `severity` from the
`maintenance` byte and then runs out of input. A decode error in `deliver_unit` is
deliberately whole-node-fatal (charpente/src/kernel/tail.rs:99-104). In production the
kernel thread then exits and the HTTP edge keeps accepting requests that nothing will
serve (charpente/src/serve.rs:59-62).

The feed is `public`. Any caller who presents a cursor older than a retained v1
`IncidentOpened`, via `Last-Event-ID` or `?cursor=`, stops the node, and does so again
after every restart until retention drops those rows. That caller can be a browser
EventSource that dropped before an incident opened and reconnects after the deploy, or
anyone who recorded a cursor from their own org's feed before the upgrade. Org creation
is self-service (I-2), so a hostile caller can always arrange such a cursor.

This violates the brief twice:

- "stream that org's incident events … with exact `Last-Event-ID` resume (GUIDE §11)".
  Resume across the builder's own required upgrade is not merely inexact; it is fatal.
- It is an availability failure that an unauthenticated caller can trigger.

**Shown, not reasoned.** Probe workspace `scratchpad/upgrade/`: `sp_v1` is the tree at
`71edb77`, the last v1 commit; `sp_v2` is the tree at HEAD; the test is
`probe/tests/upgrade.rs`. The run was fault-free (`Sim::with_faults(0x5eed,
FaultConfig::none())`):

1. v1 creates an org and a component, opens a feed from now, and opens two incidents. Every
   kernel run ends at its horizon, which is a clean, checkpointed shutdown
   (charpente/src/kernel/mod.rs:285-293).
2. v2 boots on the same disk. The table migration works: the operator listing shows both
   v1 incidents with `"severity":"Minor"`.
3. An anonymous `GET /orgs/1/events` with `last-event-id: 0.4`, the first frame the v1
   feed was handed, gives
   `kernel.run = Err(InvalidData, "row decode: needed 1 bytes for bool, 0 left — schema drift or corruption")`.
4. After a reboot of v2, the same request gives the same `Err`.
5. Control: resuming from `1.5`, the last v1 frame, is `Ok(())`. The failure comes from
   decoding the v1 row, not from the resume itself.

Log: `scratchpad/upgrade-probe.log`.

**Which platform claim should have refused it: no claim covers it, and this is a platform
gap.**

- README guarantee 1 (L17-18) lists "events" among the "Rust types checked at compile
  time", but what it promises is compile-time consistency of code. It says nothing about
  the durable event log.
- GUIDE §12's safety promise ("without risking a silent misdecode of rows already on disk",
  L3775-3779) and its startup fingerprint backstop (L3787-3792) cover `#[table]` rows
  only. Nothing versions or fingerprints an `#[event]` payload.
- GUIDE §11 documents delivery-time kernel-fatality only for JSON encoding (L3391-3399).
  It does not mention the equally fatal `Codec` decode path.
- `charpente test --sim` would catch this only through an app-written upgrade workload. No
  platform property class models an upgrade, and the brief's four properties do not ask
  for one. `--mutants` has nothing to mutate here.

### C-2. The isolation property never drives most operator writes across orgs. With `create_operator`'s membership check deleted, the whole suite comes back byte-identical, and any operator can take over any org.

**Where:** `no_org_observes_another_org` (tests/sim.rs:1056-1285). Its doc comment claims
"org alpha's operator also tries every write and read against org beta" (L1051-1052). The
intrusions it actually sends (L1090-1104, L1184-1199) are:

- `POST components`,
- the GETs of components, incidents, subscriptions, notifications and maintenance,
- one transition.

Never driven across orgs, or never called at all:

| Route | Coverage |
|---|---|
| `POST /orgs/:org/operators` (`create_operator`, src/operators.rs:169-190) | never called |
| `PUT /orgs/:org/components/:id/status` | never called |
| `GET /orgs/:org/incidents/:id` | never called |
| `POST /orgs/:org/incidents` | only called for the caller's own org |
| `POST /orgs/:org/incidents/:id/updates` | only called for the caller's own org |
| `POST /orgs/:org/maintenance` | only called for the caller's own org |

`create_operator` is where this is fatal. It writes `Operator`, which is a `global` table,
and it has to be one: `authorize` receives only `&Tx` (GUIDE L5364) and must read
membership without a scope. So the compiler does not force `create_operator` to open a
scope at all. Its `tx.scope::<Org>(org, ctx)?` (operators.rs:176) is a voluntary check,
and it is the only thing standing between an operator of org A and a working operator
credential for org B.

**Shown, not reasoned.** In `scratchpad/review-copy-2/` I made two edits.

- **Edit (a):** delete `create_operator`'s scope line. This takes three mechanical fixups:
  `&mut tx` becomes `tx`, and `(*tx).put` becomes `tx.put`. Everything still compiles.
- **Edit (b):** in `set_component_status`, `open_incident`, `post_update` and
  `schedule_window`, replace `tx.scope::<Org>(org, ctx)?` with the pattern the app's own
  public routes use, `tx.open_scope::<Org>(org, None)?`.

Results:

- `charpente test --sim --universes 256 --master-seed 0x00000000000b0001` on the copy:
  EXIT 0, 692 verdicts, 588 gave up. That is identical, test by test, to the pristine
  control on the same seed (148/126/140/174 gave up per test in both). Logs:
  `copy2-sim.log`, `pristine-sim.log`.
- Probe `review-copy-2/tests/takeover.rs`:
  - Alpha's key on `POST /orgs/3/operators` returns
    `200 {"api_key":"ck_6d98…","operator":"8","org":"3"}`.
  - That minted key on `GET /orgs/3/subscriptions` returns
    `200 [{"email":"cfo@beta.test",…}]`.
  - Alpha's own key on `PUT /orgs/3/components/5/status` returns `200 {"status":"Down"}`.

In short, a deleted guard hands any operator full control of every other org, and the
property that claims isolation cannot tell. The identical report is not a coincidence.
None of the edited paths is ever exercised in a way the edit changes, so the property is
unfireable against these guards by construction.

**Which platform claim should have refused it: no claim.**

- The compile-time refusal "`X` is a tenant-scoped table … and a bare `Tx` names no
  tenant" (GUIDE L5470-5482) covers scoped tables only. `Operator` is `global`, as the
  `Tenant::authorize(&Tx, …)` signature requires.
- `Tx::open_scope` is public, so edit (b) compiles. GUIDE L5501-5507 seals only the six
  private primitives.
- `charpente test --mutants` has no mutation operator that deletes a statement inside a
  handler or swaps an argument for `None`. The one mutant that touches this guard,
  `authorize -> Ok(())` (lib.rs:43, CAUGHT in gates.md), mutates the shared function. It is
  caught by the two route families the workload does drive across orgs, and that catch
  hides the uncovered call sites. This is exactly the masking GUIDE §8 warns of: "one
  honest sibling hides every one-sided property beside it" (L2561-2563).
- The brief also scoped `--mutants` to the job, cron and state-machine files, so neither
  `operators.rs` nor `public.rs` was ever mutated.
- The property class that could catch this exists (`charpente test --sim`). The
  workload does not drive these routes.

---

## Important

### I-1. Write authorization rests on the `auth` keyword alone, and no oracle checks it.

This is the fail-open side of the shape described above. In `scratchpad/review-copy/`, all
seven operator write routes were flipped from `auth` to `public`: `POST components`,
`PUT status`, `POST incidents`, `POST transition`, `POST updates`, `POST maintenance`,
`POST operators`.

- `tests/anon_write.rs` with **no credential** returns 200 on create component, open
  incident, and Investigating→Identified→Monitoring→Resolved. The last of these carries
  `"resolved_stamp":"8"`, so it also enqueued a notify job to every subscriber.
- The full sim batch on the same master seed is EXIT 0 with 692 verdicts, byte-identical to
  pristine (`flip-sim.log`). The suite only ever sends an anonymous request to one operator
  route, `GET components` (sim.rs:1242), and never to a write route.

Why not Critical: the shipped code is correct, and the brief's four required properties do
not include "writes are operator-only". So this is a coverage gap over a correct
implementation, not a one-sided property.

Citation: **no claim**. The compiler forces a route to declare `auth` or `public` (GUIDE
L1229, README L62-64, L143-144) but cannot know which one is correct.

### I-2. Org creation is an unauthenticated write the brief does not list.

`POST /orgs` is `public` (src/operators.rs:76-105). It creates an `Org`, an `Operator`, and
an API key for anyone. The brief grants anonymous callers exactly three operations and
says "Every other write is operator-only". The brief never says how the first operator of
an org comes to exist, and the docs offer no bootstrap path. `create_api_key` exists only
on a request's `Ctx`, and GUIDE §5's own login example (L940-943) is a `public` route that
mints a session for any subject named in the body. The builder avoided that trap: its
`/login` is `auth` (operators.rs:108).

No existing org's rows are reachable through `POST /orgs`, which is why I rate this
Important. A strict reading of the brief makes it a Critical under "an unauthenticated
write", with the citation **no claim**: the platform made the builder write `public`
explicitly, and whether that is right is a domain decision. It also feeds C-1, because it
lets anyone manufacture the pre-upgrade cursor.

### I-3. One app-wide id sequence makes public responses a cross-org side channel.

`next_id` draws every id in the app from one counter row (src/lib.rs:99-119). This is
deliberate: `a.id < b.id` then means "committed first", which is what the notify stamp
relies on. The price is that every id a public route returns is a running count of rows
created in *every* org.

Shown on pristine HEAD code (`scratchpad/upgrade/probe/tests/side_channel.rs`, run against
`sp_v2`):

- **Counting another org's private writes.** An anonymous caller subscribes two throwaway
  emails to alpha's page. Between the two subscribes, beta's operator creates three
  components. The gap between the two responses' ids is exactly 3.
- **Detecting another org's subscriber.** Anonymously subscribing `victim@corp.test` to
  alpha returns `subscriber 5, subscription 14`. A fresh email always returns `subscriber ==
  subscription - 1`, so the response reveals that this address already follows another
  org's page. In other words, it reveals that another org's `Subscription` row exists.

The brief says "no route may return, modify, or **count** another org's rows" and "no
public read … may ever carry another org's rows". No row content leaks and no route
computes a count, so I rate this Important rather than Critical. Differencing two public
responses does count another org's rows, though.

Citation: **no claim**. GUIDE §2's counter pattern (L283-296) and §16's "Ids stay globally
unique and app-minted" (L5411-5413) say nothing about the leak.

There is also a platform-level floor under this. An SSE frame's cursor is a global event
id plus a global log seq (§11 L3567-3571), and a keyed tail skips other orgs' rows without
renumbering them (charpente/src/kernel/tail.rs:91-97). So even an app with per-org id
spaces leaks other orgs' event and commit volume through its public feed.

### I-4. The notification oracle takes "at resolve time" from the implementation.

`check_notifications` computes the expected set as `subscriptions.filter(|s| s.id < stamp)`
(tests/sim.rs:985-989). That is the same predicate the job runs (src/notify.rs:40-43),
using the stamp the resolve itself drew. The oracle can therefore never disagree with the
job about which subscriptions existed at resolve time.

Consider a plausible bug that breaks id order, for example reusing the `Subscriber` id as
the `Subscription` id. A subscriber who already follows another page and subscribes to
this org after a resolve would then be notified for that earlier incident. Both the job and
the oracle would count it. The script cannot expose the bug either: its only late
subscriptions (`late@{tag}`) are fresh emails (sim.rs:855-860).

The script does know the true order: two subscriptions acked before the first resolve and
three before the second. Nothing checks against that.

This is reasoned, not run. It is an oracle-strength finding, not a defect in the shipped
job, which I believe is correct. Citation: **no claim**.

### I-5. The `durable_corruption_fired` excuse covers wrong data in the isolation workload.

`expect` returns `Ok` for **every** reply once durable corruption has fired (sim.rs:335-338).
That includes the cross-org refusals: alpha's key against beta's components, subscriptions
and notifications (L1107-1110), and alpha stepping beta's incident (L1193-1198). So in a
corruption universe, a 200 that carries beta's rows back to alpha is excused. GUIDE says of
this checker: "Never use it to excuse wrong data or a consistency violation" (L2069-2070).

The builder's rationale (L328-334) is that a cut can re-mint ids. That holds for
history-dependent expectations. It does not make a cross-org 200 legitimate: alpha's key
either still resolves to alpha's operator, whose org is alpha, or it is gone and the reply
is 401.

The end-state checks and the feed-org check still run in those universes, and the gates
show the authorize mutant is caught, so this narrows the oracle rather than voiding it.
Citation: **no claim** (this is a misuse of a documented checker).

---

## Minor

- **M-1. Maintenance incidents are born in Monitoring.** `ApplyWindow` opens them directly
  in `Monitoring` (src/maintenance.rs:155-161), skipping Investigating and Identified.
  Opening is not a transition, so this is not a transition the brief forbids. It is still an
  entry point outside the lifecycle the brief states, and the code nowhere calls it out as a
  deliberate design choice.
- **M-2. "One row per email" is not guarded.** The brief's "a subscriber is one email
  address … one row, however many orgs it subscribes to" has no property behind it. In
  `scratchpad/c3/` I deleted the `Subscriber::by_email` dedupe so every subscribe mints a
  new `Subscriber`. The full batch was EXIT 0 with 686 verdicts and no failures
  (`c3-sim.log`). The per-org `Subscription` dedupe (public.rs:91-104) is likewise never
  exercised, because the script never subscribes one email to one org twice (reasoned).
- **M-3. The replay property's "0 gave up" means less than it seems.** It is structural.
  `replays_identically` runs the workload on its own fresh `Sim::new` worlds
  (charpente-sim/src/checks.rs:138-146), so `sim.give_up` inside `notification_universe`
  lands on those worlds, never on the batch's universe. Some of the 10,000 "verdicts" are
  universes in which both inner runs gave up at bootstrap and compared `None == None`.
  The durable fingerprint is still compared, so none is vacuous. The inner worlds run
  default fault rates, not the swarm mix (GUIDE L2040-2045). This is a platform reporting
  gap as much as an app one.
- **M-4. Some oracle checks compare the path to itself.** `ComponentView.org`,
  `IncidentView.org`, `SubscriptionView.org` and the rest are filled from the path
  parameter (`ComponentView::of(org, row)`), never from the row's scope. The oracle's
  `c.org != org.org` checks (sim.rs:1370, 1379, 1406, 1415, 1433, 1442) can never fail;
  the id-membership half of each check carries the weight. The public.rs guards I removed
  were caught: dropping the feed's `.key(tx.event_key())` went red with 59 failures
  (`c1-sim.log`, e.g. seed `0x1f897d88784c48c8`), and dropping the page's Resolved filter
  went red with 107 (`c2-sim.log`, e.g. seed `0x23706a3305680ff5`).
- **M-5. The maintenance sweep does linear work over all history.** It scans every window
  of every org on every tick, including long-resolved ones (maintenance.rs:117), which is
  O(all windows ever). `mint_api_key` scans the whole global `ApiKey` table twice per mint
  (operators.rs:54-72) to recover the new key's id, which the platform does not return.
  That second one is a platform API gap more than an app choice.
- **M-6. A cross-org refusal is a 400 that names both orgs** (lib.rs:50-56). An operator
  therefore learns that another org id exists. This follows GUIDE's own example
  (L5367-5370), and 403 is not among `ApiError`'s variants.

## What holds up (for calibration)

- **Resolve is one transaction.** Resolving writes the state change, the stamp, the update
  row, the notify enqueue and the `IncidentResolved` emit in one transaction
  (incidents.rs:241-280). The notify job's effects commit with its Done mark. The
  notification oracle is exact apart from the circularity in I-4.
- **Every emit is keyed.** All emits go through `ScopedTx`. Removing the feed's key goes red
  (M-4 above), and so does removing the page's Resolved filter.
- **The state machine is sound and its errors are actionable** (incidents.rs:171-181). Its
  mutants were caught or unviable (gates.md).
- **Maintenance holds.** Opens and resolves are each at most once, never early in ticks,
  and re-checked inside the per-org job. The oracle checks window-to-incident uniqueness
  independently of `opened_at`.
- **Auth avoids GUIDE's trap.** `/login` exchanges an existing credential rather than
  copying GUIDE §5's public login-as-anyone example. Key revocation is limited to the
  caller's own keys and answers 404 otherwise.
- **The v2 table migration is correct.** It is correct end to end (the upgrade probe's
  listing), and a separate commit from v1. The feed's `emits(...)` drift check follows
  GUIDE §11.
