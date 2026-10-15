# FINDINGS — triage, built on charpente from its docs alone

Sources: `../BRIEF.md`, `../docs/README.md`, `../docs/GUIDE.md`. Nothing else was read.
Compiler diagnostics are the only other input; where one answered a question the docs
left open, the entry says so.

## Friction ledger

### 2026-10-14

- Read README.md whole, then GUIDE.md §1–§9, §13 (the `launch` part), §16, and §11 up to
  the SSE route. Skimmed §10 (replication) far enough to see nothing in it applies to a
  single node. Skipped §12, §14, §15: no migration, client or archive in this brief.
- `charpente new triage` + `git init` + the patch lines: built first time
  (`cargo clippy --all-targets -- -D warnings`, 5.5 s on a warm registry).
- Login. §5's `login` example mints a session for whatever `subject` the body names, which
  is an open door if copied: anyone could log in as any operator. The docs offer no
  password primitive and no hashing helper, so I did not invent one. What the docs do
  support: signup (`POST /orgs/:org`, `public(Org)`) mints the founding operator's API
  key; `POST /login` is an `auth` route that trades a live API key or session for a new
  session; `POST /orgs/:org/operators` mints a key for a new operator of the caller's org.
  Every credential's subject is `operator:<id>`, and `Org::authorize` reads the
  `Operator` row to check its one org.
- `revoke_key` from §5 deletes any key by id for any authenticated caller. Copied as-is
  it lets one org's operator revoke another org's keys; mine checks the key's subject.
- There is no 403 in `ApiError` (§5 lists BadRequest, the 401 family, NotFound,
  Internal). A non-member is refused with 400, as every §16 example does.
- Signup writes the org's scoped on-call row, so it must open the scope for a caller
  with no credential: that is exactly `public(Org)` (§5 "A route's stance"). Read it
  once, worked first time.
- Not in the docs, answered by compiler diagnostics instead (a `let () = x.field;` or an
  exhaustive struct pattern prints the type or the missing field list):
  `OutboundCall`/`OutboundReply` live at `charpente::core::io` (§8 never gives the
  path); `OutboundCall { host, method, path, headers: Vec<(String, String)>, body:
  Vec<u8>, key: IdempotencyKey }`; `OutboundReply { status: u16, headers, body }` has
  public fields and no documented constructor; the model's `Rng` is
  `charpente_sim::rng::Rng` with `next_u64`, `chance(f64)` and `below(n)`; `JobRow` has
  `scope: Option<u64>` (the tenant id) and `outbound_key: Option<[u8; 16]>`;
  `Delivered.at` and `.handle` are `u64`; `Via::ApiKey` carries an `Id<ApiKey>`.
- `sim.with_remote` lends `&M`, never `&mut M`, so a model cannot be told anything from
  the workload through it. I share an `Arc<Mutex<_>>` between the model and the
  workload instead (a quiesce switch, and the model's log).
- The mailer "honours an Idempotency-Key". §8's `Idempotent` wrapper replays "the first
  reply again" for a repeated key, of any status, so a model 503 behind it would be
  replayed forever and the page could never go out. A real mailer stores only what it
  processed. I wrote the mailer model's idempotency by hand: 200 and 422 are stored and
  replayed, 503 is not.
- The sim's request script needs a request→response pairing. `SimSource::responses()`
  is documented as "the recorded (status, body) pairs, filled in as the kernel replies";
  nothing says they are in arrival order, or what a request the kernel never reached
  gets. I read them as one per request in arrival order, a prefix when the kernel
  stopped, and made every 200 body self-identifying (it echoes the org, the item text,
  the viewer) so the oracle checks each pairing and fails loudly if the reading is
  wrong. It has not fired in any run.
- `push_at` and `SimSource::new`'s horizon are absolute virtual nanoseconds. §8 never
  says so; the tail example's `sim.now().as_nanos() + 600 * SEC` implies it.
- `sim.cover` takes a `&'static str` (a compile error told me, E0521).
- First sim run (8 universes, then 256): green on the first build, which I did not
  trust. Instrumented the give-ups: 18% of universes had no item at all, because under
  swarm `sync_error` rates a round's single process poisons on its first few commits
  and every later request in that round goes unanswered. Restructured the driver
  (restart #1, of the workload, not the app): a round is now a supervisor loop of up
  to four processes, re-sending what the stopped one never answered. Items-less
  universes fell from 45 to 18 of 256 on the same master seed.
- Re-read §8 "Giving up" (to place the verdict key past every give-up arm), §8
  "Calling the outside world in a sim" (the tail's order: restart, quiesce both, boot,
  run past every deadline), §6 "When a call fails" (which outcomes are attempts), and
  §8 `boot_through_fallout` (what `Ok(None)` means when it did not give up).
- Planted eleven bugs by hand before trusting the green, each against the property that
  should see it (128 universes, master seed `0x5eed0001`; the honest control is green
  on the same seed). All eleven went red: a page sent to the on-call address as it is
  at send time; a page for a non-urgent label; the idempotency header left off (three
  emails for one item); a 422 retried; `authorize` admitting everyone; a submit that
  never enqueues its classification; an urgent label with no page job; the mail id or
  the page address not recorded; a submit that never puts its item; a label that is
  not the classifier's answer. Then disabled the two direct address comparisons and
  re-planted the send-time bug, to see the black-box check alone (an item's page
  address must be the on-call version current between the last read that saw it
  unlabelled and the first that saw it labelled) catch it: 9 of 128 red. That check
  now judges the address the mailer received, not the one the app claims it sent.
- `charpente test --sim --universes 10000` on commit `68efbfb` (before the key lifecycle
  below): green, master seed `0x200eb2e3fad97884`, 45,452 verdicts and 4,548 give-ups
  over 50,000 universe-runs, 18 min 47 s wall (10,079 s CPU). Superseded by the run on
  the final commit, recorded at the end.
- §5's `/me`, `/keys`, `DELETE /keys/:id` and `/logout` were in the app but nothing
  exercised them, so the workload now mints each founding operator a second key, learns
  its id through `/me`, has another org's operator try to revoke it (refused, and the
  key still works), then revokes it itself, and logs the session out in the tail. A
  revoked key or a logged-out session answering 200 after the revocation was acked
  fails `acked_writes_survive_crashes`. Three more planted bugs, all red: revoke
  ignoring whose key it is, logout doing nothing, revoke without the delete.
- Nothing in §5 says what an API key's `id` looks like. `Via::ApiKey(id)` hands back
  ids like `438550243776832166`, which look drawn from entropy rather than counted.
- In the sim, the hosts' credentials never reach the models: the classifier sees only
  `content-type`, the mailer `idempotency-key` and `content-type` (printed once from a
  scratch edit, then reverted). That matches §6 ("the value never enters ... the sim"),
  and it means no sim oracle can notice a `Host` declared without its credential. The
  wiring is checked outside the sim instead: `charpente describe` lists
  `api.llm.example env=LLM_API_KEY injects=authorization` and `api.mail.example
  env=MAIL_TOKEN injects=x-mail-token`, and the built binary refuses to start without
  either variable (`charpente refused to start: outbound: LLM_API_KEY is unset — host
  api.llm.example needs it for job Classify`, and the same for `MAIL_TOKEN` and `Page`).
- `charpente test --mutants --file src/classify.rs --file src/page.rs --test-package
  triage`: 8 mutants, 6 caught, 2 unviable, 0 missed, 0 timed out, 4 min wall. The verb
  prints only the summary; `cargo mutants --list` over the same files names the eight:
  `call` → `Ok(None)` and `Ok(Some(Default::default()))` for each job (the second does
  not compile: `Call` has no `Default`), `settle` → `Ok(())` for each, `!=` → `==` on
  the classifier's status check, `==` → `!=` on the urgent check. cargo-mutants made no
  match-arm mutant for `Page::settle`'s `200` and `422` arms, because the catch-all arm
  binds `s` rather than `_`; the hand-planted "422 retried" bug above is the evidence
  those arms are pinned. Nothing in GUIDE §8 says where the verb writes its
  `outcomes.json`, and it is not in the repository.
- `cargo test` (no flags) runs the sim's own `main` at its default 256 universes per
  property: green, 1,181 verdicts, 99 give-ups, about 35 s.

## Summary

- **Restarts of an approach: 1.** The sim driver went from one process per round to a
  supervisor loop of up to four (above). The app's design held from its first build:
  no route, table or job was redesigned after the sim ran. The workload grew twice
  (the label-time address check moved to what the mailer received; the key lifecycle),
  which I count as extensions, not restarts.
- **GUIDE sections re-read, and why.** §5 "A route's stance" (signup must enter a
  tenant nobody belongs to yet: `public(Org)`); §5 "Auth" (what `Via` carries, that
  `revoke_key` as printed is cross-tenant); §6 "Outbound jobs" and "When a call fails"
  (the `call`/`settle` shapes for a scoped job, which outcomes are attempts, so the
  retry budget could be sized against the faulted window); §8 "Giving up" (where a
  verdict key goes); §8 "The outside world: remote models" (`with_remote`, `Idempotent`,
  what a restart does to calls in flight); §8 "Calling the outside world in a sim" (the
  tail's order); §8 "The checkers" (`boot_through_fallout`'s `Ok(None)`, and what
  `calls_have_durable_jobs` attributes by); §16 "Where the guarantee stops" (how an
  oracle may read the job queue: `db.snapshot()`).
- **Not in the docs** (each answered by a compiler diagnostic, an experiment, or a
  choice recorded above): a way to verify a login secret (none; §5's example trusts the
  body); a 403; where `OutboundCall`/`OutboundReply` live and their fields, and how to
  build an `OutboundReply`; the model `Rng`'s methods; `JobRow`'s fields beyond the four
  §8 uses (`scope`, `attempts`, `outbound_key`'s type); whether `SimSource::responses()`
  is in arrival order and what an unreached request gets; that `push_at` takes absolute
  time; whether `Idempotent` stores a 5xx; how to tell a model something mid-universe;
  how to read one tenant's scoped rows from a `Snapshot` (§16 says the reads exist,
  names none; my oracles read through operators instead); what an `ApiKey` id looks
  like; where `charpente test --mutants` leaves its outcomes.
- **Master seed** of the 10,000-universe run on the final code (commit `fe71ad6`):
  `0x9363215d418bc339`.

```text
sim: 10000 universe(s) per test from master seed 0x9363215d418bc339
sim: 805 of 10000 universes of "acked_writes_survive_crashes" gave up — reached no verdict
sim: 1674 of 10000 universes of "urgent_items_page_exactly_once" gave up — reached no verdict
sim: 1475 of 10000 universes of "labels_are_written_once" gave up — reached no verdict
sim: 734 of 10000 universes of "orgs_see_only_their_own" gave up — reached no verdict
sim: 10000 universe(s) per test, 45312 verdict(s), 4688 gave up, no failures (master seed: 0x9363215d418bc339)
```

20 min 24 s wall, 10,420 s CPU. The give-ups are universes the injected faults left with
nothing to adjudicate (no org signed up, no item submitted, or a boot refused for durable
corruption), plus violations excused because durable corruption fired in that universe;
every give-up arm names its reason.

- **`--mutants` output**, verbatim, run on the final code:

```text
Found 8 mutants to test
ok       Unmutated baseline in 8s build + 35s test
 INFO Auto-set test timeout to 180s
8 mutants tested in 4m: 6 caught, 2 unviable
mutants: 8 total — 6 caught, 0 missed, 2 unviable, 0 timed out
```

Every mutant is caught or unviable, none missed.

The final 10k run and the mutation run both judge commit `fe71ad6`.
