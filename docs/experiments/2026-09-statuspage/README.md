# The docs-only status-page experiment (2026-09-17 → 2026-09-24)

M3 sub-project 3 (PLAN.md; spec `docs/superpowers/specs/2026-09-17-third-docs-only-build-design.md`).
An agent was given `BRIEF.md`, `README.md` and `docs/GUIDE.md` — nothing else — and asked
to build a status page on charpente in its own repository, scaffolded by `charpente new`
and reaching the platform only through the brief's `[patch.crates-io]` lines. The point
was never the app. It was whether the platform, which since sub-project 2 asks a route to
say `auth` or `public` and a table to say `scoped_by` or `global`, leads a stranger to a
sound shape, and whether its oracles catch what they claim to. The brief reaches for the
shapes where the platform asks — anonymous reads and operator-only writes on one tenant's
rows, a job that writes scoped rows, a durable schema change — and says nothing about how
to get there. `attempt-1/FINDINGS.md` is the agent's own friction ledger, written as it
went; `attempt-1/review.md` is a blind review written without it.

**Verdict: PASS on attempt 1, signed by Noah 2026-09-24 — "PASS, fix before publish".**
Spec §2(b) is read by its letter: both reviewed Criticals are agent bugs that no platform
claim says it would refuse. Before the 0.1.0 publish, two platform findings this build
surfaced are fixed: (1) `#[event]` payload evolution is unguarded, a delivery-time decode
failure is whole-node-fatal, and the decode error names neither the event nor a fix
(Critical C-1's platform half); (2) a job cannot open a tenant scope the way GUIDE §16
documents, and `authorize` sees neither the operation nor the caller kind. There is no
attempt 2.

## Attempt 1

| | |
|---|---|
| Brief sha256 | `3dc351f013fbd3d119d705b0bd2821f890a02ca19a271e9efcb75fc31a9b253c` (80 lines, committed in `d0b2b7e` before any attempt) |
| Pinned sha | `d0b2b7e` (main; the brief commit). The CLI on the builder's PATH was installed from `66d79d8`; the four crates the app compiles (`charpente`, `charpente-core`, `charpente-macros`, `charpente-sim`) and the workspace `Cargo.toml` are identical between the two, and unchanged through `48e6420` |
| Dependency form | `[patch.crates-io]`, the brief's lines unmodified (proven by preflight at `66d79d8`) |
| Builder | one Opus agent, sealed; dispatched 2026-09-21, reported done 2026-09-22 |
| Resume count | 1 — the harness ended the builder on an API 529 (Overloaded), not on anything it did; resumed with a fixed message |
| §2(a) zero framework patches | **PASS.** Charpente's tree clean; `git diff 66d79d8 48e6420 --stat` over the four crates and `Cargo.toml` is empty. The transcript grep found 252 tool calls and 2 hits, both ruled non-violations: hit 1 was the brief's own patch lines written with a heredoc; hit 2, a `grep` into `../../charpente/crates` piped to `head -0`, which macOS `head` rejects, so zero bytes reached the builder. Hit 2 is recorded as a near-miss |
| Master seed | `0x58129e7e4b18d000` (`charpente test --sim --universes 10000`; `attempt-1/FINDINGS.md:327`) |
| §2(d) replay | **PASS.** Two controller replays a day apart, the second under heavy concurrent load, are byte-identical (sha256 `bbdb98898b6c329d56eb268b5150616216f466a19ca8ab6ab34f4805079fa8c0`): 10,000 universes per test, 0 failures, 27,120 verdicts, 22,880 gave up. Per test, verdicts / gave up: `acked_writes_survive_restarts` 3,925 / 6,075; `a_universe_replays_identically` 10,000 / 0; `notifications_are_resolved_incidents_times_subscriptions_at_resolve` 5,009 / 4,991; `no_org_observes_another_org` 4,561 / 5,439; `maintenance_windows_open_once_and_resolve_once_never_early` 3,625 / 6,375. The builder kept a text log, not JSON, so the replay was compared with it field by field: every total and every per-test give-up count matches |
| §2(c) controller's `--mutants` | **PASS.** One run per file, `--test-package statuspage --timeout 600`, each EXIT=0: `notify.rs` 3 — 3 caught, 0 missed, 0 unviable, 0 timed out; `maintenance.rs` 23 — 20 caught, 0 missed, 3 unviable, 0 timed out; `incidents.rs` 28 — 12 caught, 0 missed, 16 unviable, 0 timed out; `lib.rs` 24 — 13 caught, 0 missed, 11 unviable, 0 timed out. Total 78 — 48 caught, 30 unviable, 0 missed, 0 timed out. The builder's own run over the first three files (54 — 35 caught, 19 unviable; `FINDINGS.md:199`, `:351`) equals the controller's sum for them exactly; `lib.rs` (the incident state's `u8` encoding and `authorize`) is the controller's addition. `authorize → Ok(())` is caught by `no_org_observes_another_org` |
| App gates | **PASS.** `cargo clippy --all-targets -- -D warnings` EXIT 0; `cargo test` EXIT 0, 4 `test result: ok`, 0 FAILED; `charpente check-client` EXIT 0; `version = 2` is its own commit, `dd1993a`, after v1's `10c42af` |
| §2(b) Criticals | 2 (review: 2 Critical, 5 Important, 6 Minor); both ruled "no claim", agent bugs — below |
| Verdict | **PASS** |
| Noah's sign-off | 2026-09-24, "PASS, fix before publish" |
| App repository | final commit `6037704`, 18 commits, outside charpente (`statuspage-run/statuspage/`); bundle `statuspage-attempt-1.bundle` beside the run directory, full history, head `6037704` |

## The two Criticals, and why the verdict is PASS

Both were demonstrated by running code, and the controller re-ran both demonstrations. The
probe sources are kept in `attempt-1/probes/`, because the scratch directory they ran in
is session-scoped and dies with the session.

**C-1. The v2 commit reshaped a durable event; a pre-upgrade cursor stops the node.**
`dd1993a` inserted `pub severity: Severity` mid-struct in `IncidentOpened`, between
`title` and `maintenance`, in the commit that bumps `Incident` to version 2. Event rows are
durable `Codec` bytes, and by default 65,536 of them are retained
(`crates/charpente-core/src/db.rs:76`), so v1-shaped rows outlive the upgrade. Delivery
decodes each row with the current struct (`crates/charpente/src/http/mod.rs:257-266`), and
a decode error at delivery is deliberately whole-node-fatal
(`crates/charpente/src/kernel/tail.rs:99-104`). The feed is `public`: any caller holding a
cursor older than a retained v1 `IncidentOpened` stops the node, and stops it again on
every reboot while the row is retained. The kernel's error is
`row decode: needed 1 bytes for bool, 0 left — schema drift or corruption`.

- **Ruling: no claim — an agent bug by §2(b)'s letter.** README guarantee 1 (README.md
  at `d0b2b7e`, lines 17-19) is compile-time consistency of types; it says nothing of the
  durable event log. GUIDE §12's safety promise is scoped to "A `#[table]` row's shape"
  (`docs/GUIDE.md:3775-3779`). DESIGN.md:655-657 says the platform's own `EventRow` "is
  invisible to the gate". GUIDE §11 already documents durable-row delivery failure as
  kernel-fatal, for the encode path (`docs/GUIDE.md:3391-3399`).
- **This was the verdict's closest call, and the severity is platform-made.** `#[event]`
  payloads carry no version or fingerprint, and GUIDE never says how to evolve one — the
  builder's only "add a field" guide was §12, for tables. A decode failure at delivery is
  whole-node-fatal, which makes an anonymous cursor a node-kill switch; the gentler
  per-tail drop is an existing ledgered follow-up. And the decode error names neither the
  event nor a fix (`http/mod.rs:259` is a bare `Ev::decode(&mut input)?`, while the encode
  arm beside it names `Ev::NAME`), which breaks hard rule 5. The spirit of README
  guarantee 1 — "a schema change … is a compile error, not a 3am page" — is violated; the
  letter §2(b) binds is not. Read by its spirit, this Critical would have been a platform
  finding and attempt 1 a FAIL. Noah chose the letter, and made the platform half a
  precondition of the 0.1.0 publish instead.

**C-2. The isolation property is blind to most operator writes.**
`no_org_observes_another_org` says org alpha's operator "tries every write and read against
org beta", but it never drives `create_operator` at all, never drives
`set_component_status` or `get_incident`, and drives `open_incident`, `post_update` and
`schedule_window` only against the caller's own org. `create_operator` writes `Operator`,
a `global` table (it must be: `authorize` receives a bare `&Tx`, `docs/GUIDE.md:5364`), so
its `tx.scope::<Org>(org, ctx)?` (`src/operators.rs:176`) is a voluntary check, and the
only thing between an operator of one org and a working credential for another. With that
line deleted and four write routes rewritten to `tx.open_scope::<Org>(org, None)?`, the
sim batch on the same master seed is byte-identical to pristine, and alpha's key mints a
working operator key in beta and reads beta's subscriber emails.

- **Ruling: no claim — an agent bug, a one-sided property.** The compile-time refusal
  (`docs/GUIDE.md:5470-5482`) covers scoped tables, and `Operator` is `global`.
  `Tx::open_scope` is public (`crates/charpente-core/src/db.rs:1157`). `--mutants` answers
  "if this code did nothing" only for code it can mutate (`docs/GUIDE.md:2522-2525`) and
  states sibling masking as part of the claim (`docs/GUIDE.md:2560-2563`); it has no
  mutant that removes a handler's scope line (`operators.rs` has 22 mutants and none
  removes it), and the brief scoped the mutants run to the job, cron and state-machine
  files.
- Two platform findings ride on it (ranked below, item 10): the membership table is
  necessarily outside the type-level guarantee and §16 never says so, and a guard that is
  one statement inside a handler is invisible to `--mutants` at a granularity §8 does not
  state.

### The probes

- `attempt-1/probes/upgrade.rs` decides C-1. It is the test file of a probe crate that
  depends on two library copies of the app: `sp_v1`, the app's `src/` at `71edb77` (the
  last v1 commit), and `sp_v2`, its `src/` at `6037704`, each with `main.rs` dropped and
  the package renamed. On one fault-free `Sim`, v1 creates an org and a component, opens a
  feed from now and opens two incidents; v2 boots on the same disk, and its operator
  listing reads both incidents back with `"severity":"Minor"` (the table migration works).
  An anonymous resume from the first v1 frame, `0.4`, returns
  `Err(InvalidData, "row decode: needed 1 bytes for bool, 0 left — schema drift or
  corruption")` from `kernel.run`, and the same after a reboot; the control, a resume from
  the last v1 frame, `1.5`, is `Ok`. The resume test asserts the kernel keeps running, so
  its failure is the finding. The controller's re-run printed those three results and
  reported 1 passed (the control), 1 failed (the resume).
- `attempt-1/probes/takeover.rs` demonstrates C-2's consequence. It runs against a copy of
  the app whose `src/` differs from `6037704` by exactly the review's two edits: (a)
  `create_operator`'s scope line deleted (`src/operators.rs:176`), and (b)
  `tx.scope::<Org>(org, ctx)?` replaced by `tx.open_scope::<Org>(org, None)?` in
  `set_component_status`, `open_incident`, `post_update` and `schedule_window`. The
  controller's re-run: alpha's key on `POST /orgs/beta/operators` returns 200 with a
  working beta key; that key on `GET` beta's subscriptions returns 200
  `[cfo@beta.test]`; alpha's own key on `PUT` a beta component's status returns 200
  `Down`. On the same copy, `charpente test --sim --universes 256 --master-seed
  0x00000000000b0001` prints the same `sim:` lines as the pristine control, test by test:
  692 verdicts, 588 gave up, no failures.

## Public reads: the shape the app chose, and what GUIDE §16 led it to

**The shape.** `Org::authorize` (`src/lib.rs:42-58`) returns `Ok(())` for `who == None`
unconditionally and, for `Some`, requires the principal to be an operator of the scope.
Every `auth` route opens its scope with `tx.scope::<Org>(org, ctx)?`. The three anonymous
operations (the status page, subscribe and the feed; `src/public.rs:41`, `:90`, `:127`)
and both jobs (`NotifySubscribers`, `src/notify.rs:24`; `ApplyWindow`,
`src/maintenance.rs:149`) call the rustdoc-only `tx.open_scope::<Org>(org, None)`. The
public routes pass `None` on purpose, even when the request carries a credential, so an
operator of one org can still read another org's public page. `MaintenanceSweep` reads
every org's windows through the read-only `cross_tenant` capability and hands each write
to a per-org `ApplyWindow`.

**Sound, and fail-open.** The reviewer traced every route and both jobs: as shipped, every
operator write is `auth` and scoped by the caller's principal, every `public` route does
only what the brief grants anonymous callers (org creation aside, Important I-2), and every
emit goes through a `ScopedTx`. But once `authorize` admits `None`, write authorization
rests on two conventions no compiler or property checks. Flipping all seven operator write
routes from `auth` to `public` gives an anonymous caller 200s through Resolved, and the sim
batch stays byte-identical to pristine (I-1). The line GUIDE calls the one a reviewer
reads per handler no longer audits anything alone; the route attribute has to be read too.

**§16 led there, twice.**

- **Public reads.** §16's only `authorize` example refuses `None`
  (`docs/GUIDE.md:5364-5371`); §11's worked "public" feed opens the scope with the
  caller's `ctx` (`docs/GUIDE.md:3369-3375`), which yields a feed only that org's
  operators can read. The one sentence about `None` ("an `authorize` that admits `None`
  has declared anonymous access to that scope", `docs/GUIDE.md:5362-5363`) never says the
  access includes writes, and `authorize` receives `(tx, who, scope)` — nothing about the
  operation, and nothing about whether the caller is a route or a job. An app cannot admit
  anonymous reads while refusing anonymous writes, and GUIDE never says so.
- **Jobs.** §16 says a job reaches scoped rows through the membership check's handle
  (`docs/GUIDE.md:5322-5324`), that through a `Tx` a scoped table is unreachable without
  `tx.scope::<Org>(org, ctx)?` (`:5665-5667`), and that code outside a handler should use
  that line "exactly as a handler does" (`:5678-5679`). A job's `run(self, tx)` has no
  `Ctx`, `Ctx` has no public constructor (`crates/charpente/src/http/app.rs:77-92`,
  `:801-836`), and `open_scope` has zero hits in GUIDE and README. `cross_tenant` is
  read-only (`docs/GUIDE.md:5598-5599`), so the notify job's writes force `authorize` to
  admit `None`.

A fail-closed shape — `authorize` refusing `None`, with jobs and public operations passing
a synthetic `Principal` (its fields are `pub`, `crates/charpente/src/auth/ladder.rs:26-29`)
— was reachable from the source and not from the docs. This is platform finding 1 below
and the second of the two pre-publish fixes.

## Read FINDINGS.md as testimony, not as findings

It is an honest record of what the build felt like. The builder made 49 distinct claims
about charpente. Each was checked against the docs the builder read (`docs/GUIDE.md` and
README.md as frozen at `d0b2b7e`; GUIDE is byte-identical at `48e6420`) and against the
code at `d0b2b7e` (the cited crates are unchanged through `48e6420`): **41 right, 2 wrong,
5 right for the wrong reason, and 1 (#45) settled by running it — right.** The controller
re-resolved every wrong and wrong-reason citation and the top findings' citations, and ran
#45; the remaining right rows, low-stakes "the docs do not say X" claims with a cited line
or a zero-hit grep, were adopted from the census.

- **#21 (FINDINGS L117-121) — wrong, and retracted by the builder.** "Replies stay
  positional through a crash". The builder's own 256-universe batch disproved it, and
  L142-152 corrects it, keeping the wrong note because "the mistake is the finding". The
  mechanism is #27's.
- **#28 (L147-149) — wrong.** "In production that is a client seeing a connection reset
  instead of a status, so the harness is honest." In production the kernel thread ends on
  `Err` (`crates/charpente/src/serve.rs:59-63`), dropping the parked reply, and the HTTP
  edge answers `500 "the kernel dropped the request"` (`serve.rs:245-248`); every later
  request gets `500 "the kernel thread is gone"` (`:232-236`). Production answers every
  request; the sim records nothing. That is a sim/production divergence, not honesty
  (platform finding 5).
- **#27 (L142-147) — right that replies go missing, wrong about why.** `crash:dropped` is
  not a request fate: it is the sim disk's coverage key for a pending write op that
  vanished at crash collapse (`crates/charpente-sim/src/disk.rs:149-181`). The real
  mechanism: `step` polls a request into `pending` before choosing an arm
  (`crates/charpente/src/kernel/mod.rs:306-307`), the arm is drawn among the ready ones
  (`:463-466`), and if a job, cron or tail arm returns `Err`, `run` exits with the request
  still parked, unanswered (`:699-701`, `:719`, `:738-740`, `:759`). `SimReply` records
  only on `send`/`stream` (`:1114-1126`). The request arm's own poison path does answer
  first (`:567-571`, then `:591-614`), which is why five hand-checked seeds looked
  positional.
- **#11 (L60-61) — right that the docs name no hash, wrong that none exists.**
  `charpente::auth::hash_token` is public (`crates/charpente/src/auth/mod.rs:7`) and
  computes lowercase-hex SHA-256 (`auth/hash.rs:5-13`); it has zero GUIDE hits. Unsalted
  SHA-256 is not a password hash, so #10's password gap stands.
- **#13 (L69-73) — right that `create_api_key` returns only the raw key
  (`crates/charpente/src/http/app.rs:140-151`), wrong that a scan diff is the only way to
  find its row.** `ApiKey` carries `#[index] key_hash` (`auth/api_key.rs:12-13`,
  documented at `docs/GUIDE.md:861-862`), documented as the key's SHA-256
  (`docs/GUIDE.md:865`), so `ApiKey::by_key_hash(tx, &hash_token(&raw))` finds it in the
  same transaction. The row id is also the raw key's first 16 hex characters
  (`http/app.rs:142-143`, undocumented).
- **#32 (L170-171) — the symptom was observed, the stated cause is wrong.** "SIGINT is
  swallowed … some handler is installed". No handler exists: `signal`, `ctrl_c`, `SIGINT`
  and `sigaction` have zero hits in `crates/charpente/src` and `crates/charpente-core/src`,
  and tokio is built with `rt`, `net` and `sync` only (`crates/charpente/Cargo.toml:18`).
  The likely cause, not confirmed by a run: the builder started the binary as `( … & )`
  from a non-interactive shell, where POSIX starts asynchronous commands with SIGINT
  ignored, and the binary inherited that.
- **#34 (L171-172) — wrong that the docs offer no manual checkpoint, right that a built
  binary cannot take one.** `Db::checkpoint` is documented (`docs/GUIDE.md:445-448`). But
  `App::db()` is `&Db` (`http/app.rs:353`), `db_mut` is `pub(crate)` (`:395`), the only
  `&mut Db` accessor is the `#[doc(hidden)]` `db_for_tests` (`:464-471`), and `launch` owns
  the `App` after `build` returns.
- **#45 (L305-306) — right, by a run.** "Every one of the 28 repro lines replayed
  byte-for-byte to the same message before the fix and to a green (or give-up) after it."
  Each of the 28 repro lines from the builder's first 10,000-universe batch (master seed
  `0x48393f6e4e0955f4`, 28 failures) was run in two clones from the bundle: on `223e30a`,
  the tree that batch ran on, all 28 exit 1 and print their failure block — failure line,
  seed, config, fired — byte for byte as logged; on `1d0a0c4`, the fix, all 28 exit 0
  with a verdict or a give-up and no failure. The seed-repro line of hard rule 5 held for
  every real failure a stranger hit.

Found while checking, and not claimed by the builder: both of GUIDE §8's full examples,
`whole_app` (`docs/GUIDE.md:2319`, `:2334`, `:2340-2344`, `:2363`) and `counter_roundtrip`
(`:1862-1864`, with `:1885`, `:1903`, `:1907`), map fault fallout to a silent `Ok(())` with
no `give_up` — the pattern §8 itself names as the historical defect (`:1955-1959`); a
`launch` node whose kernel stops keeps answering 500 forever and never exits
(`crates/charpente/src/serve.rs:59-63`, `:232-236`; `crates/charpente/src/launch.rs:76-78`),
though GUIDE's error index says "restart the process" (`docs/GUIDE.md:2863`); and the
second kernel's loss of cron under #19 is silent, which is worse than the job failure the
builder saw.

The review's five Importants and six Minors are testimony too; spec §2(b) turns on
Criticals only. I-5 — the app's corruption excuse also waives a cross-org 200 — misuses a
checker GUIDE says never to use "to excuse wrong data" (`docs/GUIDE.md:2069-2070`), an
agent bug. I-1 and I-3 carry platform findings 1 and 11.

### The census

Every claim, in FINDINGS order. `L` is a line of `attempt-1/FINDINGS.md`; `G:` is
`docs/GUIDE.md`; `c/` is `crates/charpente/src/`, `core/` is `crates/charpente-core/src/`,
`sim/` is `crates/charpente-sim/src/`, `mac/` is `crates/charpente-macros/src/`, `cli/` is
`crates/charpente-cli/`. RWR is right for the wrong reason.

| # | L | Claim | Ruling | Evidence |
|---|---|---|---|---|
| 1 | 8 | `charpente new` writes six files | right | `cli/src/new.rs:87-104` |
| 2 | 9-12 | the scaffold's `tests/sim.rs` is the best worked give-up example | right | `cli/templates/sim.rs.tmpl:24-33`, `:78-82`; `whole_app` G:2316-2366 never calls `give_up` |
| 3 | 18-31 | a job cannot open a tenant scope the documented way; `open_scope` is public and runs `authorize` | right | G:5322-5324, G:5666-5667, G:5678-5679; G:1313-1314; `c/tenant.rs:80-86`; `c/http/app.rs:77-92`, `:801-836`; `core/db.rs:1157-1183` |
| 4 | 36-38 | `.scope()` needs `use charpente::Scoped`, which the prose never says | right | `c/tenant.rs:80-99`; `c/lib.rs:37`; G:5344, G:5480 |
| 5 | 39-40 | `ScopedTx` lives at `charpente::ScopedTx`; §16 shows no path | right | `core/db.rs:1217`; `c/lib.rs:10-11`; G:5418 |
| 6 | 41-46 | an SSE handler's return type is checked textually | right | `mac/route.rs:844-869`, `:682-686`; G:3403-3411; absent from G:2650-2671 |
| 7 | 47-48 | `ApiError` has no `Display` | right | `c/http/mod.rs:289-309` |
| 8 | 49-51 | `Codec` has no `Vec<Id<T>>` impl | right | `core/table.rs:163-230`; G:161-162 |
| 9 | 52-53 | `describe` is the fastest mount-conflict check | right | `c/launch.rs:275-289`; G:813-855 |
| 10 | 57-60, 66-68 | no first-login proof; auth is credentials, not passwords | right | G:940-943; `c/http/app.rs:123-133`; "password" has zero hits |
| 11 | 60-61 | no hashing primitive app code can call | RWR | `c/auth/mod.rs:7`; `c/auth/hash.rs:5-13` |
| 12 | 61-62 | no OIDC without a static JWKS | right | G:988-990 |
| 13 | 69-73 | `create_api_key` returns only the raw key; its row is found by scan diff | RWR | `c/http/app.rs:140-151`, `:142-143`; `c/auth/api_key.rs:12-13`; G:861-862, G:865, G:874 |
| 14 | 74-75 | `App::open` registers `Session` and `ApiKey` itself | right | `c/http/app.rs:223-240`; G:978, G:2767, G:2775-2778 |
| 15 | 76-81 | a public route that scopes with `ctx` refuses another org's operator | right | G:912-915; `c/http/app.rs:811-817`; `c/tenant.rs:98` |
| 16 | 85-88 | §16 never names `Snapshot`'s scoped reads | right | G:5672-5673; `core/table.rs:1622`, `:1647` |
| 17 | 89-91 | `Sim::now()` is undocumented | right | `sim/lib.rs:680`; G:1630-1638 |
| 18 | 92-93 | `serde_json` is not re-exported | right | `c/lib.rs:19`; G:60-61 |
| 19 | 97-110 | `Kernel::into_app()` returns an App that cannot dispatch jobs | right | `c/kernel/mod.rs:220-221`, `:923-926`; `c/http/app.rs:442-455`; `c/kernel/dispatch.rs:205-210`; G:1278, G:2370-2374, G:2190-2191, G:1485-1489 |
| 20 | 111-116 | a subscriber dropped before its first frame can only resume from now | right | `c/kernel/mod.rs:255`, `:579`; G:3558, G:3600-3602, G:3629-3631 |
| 21 | 117-121 | replies stay positional through a crash | wrong | retracted at L142-152; mechanism at #27 |
| 22 | 122-128 | §8 documents the coverage-key half of giving up; give-ups are the fault rate | right | G:1968-1976, G:1919-1921 |
| 23 | 129-131 | `is_fault_fallout` reads a 500's body | right (the combination is unstated) | `c/http/app.rs:847-855`; `sim/checks.rs:67-84`; G:1118-1119, G:2844, G:2015-2016 |
| 24 | 132-133 | `EventFrame`'s `name`/`json` fields are undocumented | right | `c/kernel/tail.rs:44-49`; G:3730-3731, G:3746 |
| 25 | 134-135 | `responses()` yields `(u16, Vec<u8>)` | right | `c/kernel/mod.rs:996`; G:2179-2180 |
| 26 | 136-138 | a `#[job]`'s derived `Codec` decodes its payload struct | right (a tip) | G:2287, G:1276 |
| 27 | 142-147 | a `crash:dropped` request gets no reply | RWR | `sim/disk.rs:149-181`; `c/kernel/mod.rs:306-307`, `:463-466`, `:699-701`, `:719`, `:738-740`, `:759`, `:1114-1126`, `:567-571`, `:591-614` |
| 28 | 147-149 | production shows a connection reset, so the harness is honest | wrong | `c/serve.rs:59-63`, `:238-248`, `:232-236` |
| 29 | 149-152 | `responses()` never says it can be short | right | G:2175-2180; `c/kernel/mod.rs:994-996`, `:1007-1008`, `:1039-1041` |
| 30 | 159-162 | §12's worked example maps onto a scoped table | right | G:3818, G:3826, G:3848; `c/describe.rs:366` |
| 31 | 163-169 | a `launch` binary has no clean exit for the upgrade barrier's remedy | right | `core/db.rs:1748-1753`; G:3896-3898, G:3902, G:1168-1169; `c/launch.rs:71-78`; `c/serve.rs:464-490`; `c/kernel/mod.rs:285-293` |
| 32 | 170-171 | SIGINT is swallowed by some installed handler | RWR | zero signal hits in `c/`, `core/`; `crates/charpente/Cargo.toml:18` |
| 33 | 171 | SIGTERM kills it | right | default disposition; no handler (#32) |
| 34 | 171-172 | nothing in the docs triggers a checkpoint by hand | RWR | G:445-448, G:423; `core/db.rs:75`; `c/http/app.rs:353`, `:395`, `:464-471` |
| 35 | 173-177 | a traffic-free boot of the old binary is the real remedy | right | `core/db.rs:2246-2254`, `:2424-2437`, `:1748-1753`; G:3901-3903 |
| 36 | 178-182 | the drift probe feeds `"0"` to every `:param` | right | `c/http/app.rs:36`, `:534-538`, `:566-570`; G:3517-3547 |
| 37 | 183-184 | `MigrationInfo` has no `Debug` | right | `c/describe.rs:73-74`, `:106` |
| 38 | 264-265 | the unviable mutants are the types refusing them | right | G:2571-2573 |
| 39 | 270-272 | each viable mutant is a rebuild plus the 256-universe batch | right | `sim/harness.rs:63`; G:2385-2386, G:2576 |
| 40 | 283-288 | every failure fired a durable-corruption axis | right | `statuspage-run/logs/sim10k.log`: 28 of 28 `fired:` lines; G:2061-2063; `sim/checks.rs:103-111` |
| 41 | 288-291 | `durable_corruption_fired` is documented only for acked-durability shortfalls | right | G:2061-2073 |
| 42 | 292-295 | a cut must be judged at check time | right | `sim/checks.rs:103-111`; G:2061-2063 |
| 43 | 295-300 | a cut rewinds an app's id counter; end-state oracles are unaffected | right | G:2070-2071; G:273-305 |
| 44 | 301-304 | the counter pattern never says a cut re-mints ids | right | G:264-312, G:2071-2073; `sim/checks.rs:95-97`; also falsifies G:5411-5413 |
| 45 | 305-306 | all 28 repro lines replay before the fix and pass after it | right (run: 28 of 28) | clones at `223e30a` and `1d0a0c4` |
| 46 | 307-309 | the 46% give-up rate is the fault matrix | right | the replay's coverage marks; G:1919-1921 |
| 47 | 327-328 | `--master-seed` replays the batch | right | G:2392-2394; the replay above |
| 48 | 369-370 | `acked_implies_durable` takes `sim`, not a seed | right | G:2049, G:2054-2060, G:2097-2102; `sim/checks.rs:193-198` |
| 49 | 371 | retry backoff, 30 s doubling × 5 ≈ 8 min | right | G:1278-1279, G:1470-1474 (30+60+120+240 s = 7.5 min) |

## Platform findings

No platform fix was taken between attempts: attempt 1 passed, so the fail loop (spec §7)
never ran. The findings are testimony, ranked by what they cost an adopter. Noah's
sign-off makes the first two a precondition of the 0.1.0 publish; their commits will be
ledgered when they land.

1. **A job cannot open a tenant scope as documented** (`docs/GUIDE.md:5322`, `:5665-5679`,
   against `run(self, tx)`). `Ctx` has no public constructor and `open_scope` is
   undocumented. Together with `authorize` seeing neither the operation nor the caller
   kind, this pushes every multi-tenant app with anonymous reads or job writes into a
   fail-open `authorize`. **Pre-publish fix.**
2. **`#[event]` evolution is unguarded, and delivery decode is whole-node-fatal (C-1).**
   The decode error breaks hard rule 5. **Pre-publish fix.**
3. **The upgrade barrier's remedy cannot be performed on a `launch` binary.** It demands a
   clean shutdown, but there is no signal handler, `launch` parks forever, and the exit
   checkpoint is unreachable in production. The real remedy — boot the old binary once —
   lives only in a comment (`crates/charpente-core/src/db.rs:2246`).
4. **`Kernel::into_app` returns a spent App.** Jobs fail with a misleading "add it to
   `jobs![…]`" error, and cron silently fires nothing.
5. **Sim/production divergence on a dropped request.** The sim records no reply;
   production answers `500 "the kernel dropped the request"`
   (`crates/charpente/src/serve.rs:245-248`). `SimSource::responses()` does not say it can
   be short.
6. **Durable-corruption cuts have undocumented consequences.** They void
   history-dependent expectations, which must be judged at check time, and they rewind app
   id counters, so §16's "Ids stay globally unique" is false on a single node.
7. **§8's own examples map fault fallout to `Ok(())` with no `give_up`** — `whole_app` and
   `counter_roundtrip`, the anti-pattern §8 names.
8. **A `launch` node whose kernel stops keeps answering 500 forever and never exits.**
   Nothing restarts it, though the error index says "restart the process".
9. **Auth is credentials-only and never says so.** There is no password store and no
   first-login proof; `create_api_key` returns no id; `hash_token` is public but
   undocumented.
10. **The membership table is necessarily outside the type-level guarantee, and §16 never
    says so (C-2).** `--mutants` cannot see a one-statement guard, and §8 does not state
    that granularity.
11. **One global event/seq space leaks another org's volume through any public feed**
    (`crates/charpente/src/kernel/tail.rs:91-97`; I-3).
12. **Smaller doc gaps:** the `Scoped` import, `ScopedTx`'s path, `Snapshot`'s
    `get_scoped`/`scan_scoped`, `Sim::now()`; `EventFrame`'s fields, `responses()`'s element
    types, the SSE last-segment type check; the drift probe's `"0"`, `ApiError` without
    `Display` and `MigrationInfo` without `Debug`; no generic `Vec<T>` `Codec`, and
    `serde_json` not re-exported.

## What the brief asked for that the app did not deliver

Worth keeping visible, as in the helpdesk write-up, because it calibrates the friction
ledger. The brief asked for "exact `Last-Event-ID` resume", and resume across the
builder's own required v2 upgrade stops the node (C-1). It said no route may "count
another org's rows", and one app-wide id sequence lets anyone count them by differencing
two public responses (I-3). It said "Every other write is operator-only", and `POST /orgs`
is `public` — though the brief never says how an org's first operator comes to exist, and
the docs offer no bootstrap (I-2). The isolation property claims to try every write
across orgs and drives two of the seven write routes there (C-2). A subscriber is to be
"one row, however many orgs it subscribes to", and no property checks it: with the dedupe
deleted, the batch stays green (M-2). Everything else
was delivered: the 10,000-universe batch green with its seed, `--mutants` clean over the
job, the sweep and the state machine, a separate v2 commit whose table migration works end
to end, a checked TypeScript client, and a FINDINGS ledger with its two restarts of an
approach, its re-read sections and what the docs did not say. Unlike the helpdesk, the
shipped code has no unauthenticated route into another org's rows, and the builder
avoided GUIDE §5's public login-as-anyone example. Where FINDINGS reports that something
was hard, that is real signal; where it reports that something was done, check the code.
