# The docs-only outbound experiment (2026-10-13 → 2026-10-14)

Outbound-effects sub-project (spec `docs/superpowers/specs/2026-10-13-outbound-effects-design.md`,
exit criterion 5 in §16; plan `docs/superpowers/plans/2026-10-13-outbound-effects.md`,
Task 20). An agent was given `BRIEF.md`, `README.md` and `docs/GUIDE.md` — nothing else —
and asked to build `triage`, a feedback service, in its own repository scaffolded by
`charpente new` and reaching the platform only through the brief's `[patch.crates-io]`
lines. An outside language-model service classifies each item, and an urgent item pages
the org's on-call address by email. The point was never the app. It was whether the docs
lead a stranger to minted keys, read-only `call` and atomic `settle`, and whether the
outbound oracles catch what they claim to. The brief names the two services' behaviour — a
classifier that does not deduplicate and bills every call, a mailer that honours an
`Idempotency-Key` — and never the platform's surface. `attempt-1/FINDINGS.md` is the
agent's own friction ledger, written as it went; `attempt-1/review.md` is a blind review
written without it. `attempt-1/gates.md`, `attempt-1/keyless-check.md` and
`attempt-1/testimony.md` are the controller's records of the mechanical gates, the
ambiguous-call check and the testimony census.

**Verdict: PASS on attempt 1, signed by Noah 2026-10-14.** Spec §16(5) passes an attempt
with zero Criticals the platform should have caught, and asks that removing the
idempotency key turn the agent's own properties red. The blind review found no Critical
at all, so there was nothing to rule on, and the keyless copy went red on the agent's own
email-exactly-once property under `outbound_lost_reply`. No platform finding was fixed in
this plan. There is no attempt 2.

## Attempt 1

| | |
|---|---|
| Brief sha256 | `73692af03241326ac9f903b113d9142d90d712eaae4a85011f3493f7fc0340b9` (71 lines, committed in `706fed55` before any attempt; the run directory's copy is byte-identical) |
| Pinned sha | `706fed550fdcf3ac84885816458b309fc9ea8721` (main; the brief commit). The preflight installed the CLI on the builder's PATH from `0506e22c` and proved the dependency form there; `git diff 0506e22c 706fed55 -- crates Cargo.toml Cargo.lock` is empty |
| Dependency form | `[patch.crates-io]`, the brief's lines unmodified |
| Builder | one Opus agent, sealed; dispatched and done 2026-10-14 |
| Resume count | 0 |
| Zero framework patches | **PASS.** `git diff 706fed5 --stat -- crates Cargo.toml Cargo.lock` is 0 lines (`attempt-1/gates.md:7`). The transcript scan found 1 hit, ruled a non-violation: the brief-mandated `cat >> Cargo.toml` of the brief's own `[patch]` lines, a write and not a read (`gates.md:8`). The wider scan is the scan note below |
| Master seed | `0x9363215d418bc339` (`charpente test --sim --universes 10000` on the final code, `fe71ad6`; `attempt-1/FINDINGS.md:143-144`) |
| Replay | **PASS.** Two controller replays at 10,000 universes, both EXIT 0, byte-identical (`REPLAY_IDENTICAL`): 0 failures, 45,312 verdicts, 4,688 gave up (`gates.md:11`). These are the builder's own recorded totals (`FINDINGS.md:152`) |
| Controller's `--mutants` | **PASS.** One run per file holding an outbound job, each EXIT=0: `src/classify.rs` 5 — 4 caught, 0 missed, 1 unviable, 0 timed out; `src/page.rs` 3 — 2 caught, 0 missed, 1 unviable, 0 timed out (`gates.md:12-13`). The builder's own run over both files, 8 — 6 caught, 2 unviable (`FINDINGS.md:167`), equals the controller's sum (`gates.md:15`) |
| Ambiguous-call (keyless) check | **PASS.** On a copy of the app, exactly one line was deleted: `.header("idempotency-key", key.as_str())` at `src/page.rs:45`, the line that puts the platform key on the mailer request (`keyless-check.md:3-18`). The agent's own batch, 10,000 universes from the same master seed, went red: EXIT=1, 2,672 failures (`keyless-check.md:20`). The first failure is the agent's email-exactly-once property, `urgent_items_page_exactly_once`, seed `0xb2fcbfc15b29ef9f`: "urgent item 23 of org 3 reads sent, and the mailer sent 2 emails for it". Its config carries `outbound_lost_reply` 0.026 (`outbound_slow` 0, `outbound_lost_request` 0). The printed repro line replayed it red with the same text (`keyless-check.md:22-38`) |
| App gates | **PASS.** `cargo clippy --all-targets -- -D warnings` EXIT 0; `cargo test --no-fail-fast` EXIT 0, 3 `test result: ok`, 0 FAILED, 0 `error: test failed` (`gates.md:9-10`; the sim target is `harness = false` and prints no `test result:` line). The app tree was clean after every gate (`gates.md:15`) |
| Criticals | 0 (review: 0 Critical, 5 Important, 11 Minor; `review.md:17`) |
| Verdict | **PASS** |
| Noah's sign-off | 2026-10-14 |
| App repository | final code commit `fe71ad6`, 10 commits, head `bbecb23` (FINDINGS commits after the code), outside charpente (`triage-run/triage/`); bundle `triage-attempt-1.bundle` beside the run directory, full history, head `bbecb23` |

`$RUN` in `attempt-1/keyless-check.md` is the run directory, `triage-run/`; the keyless
copy was removed after the check. `attempt-1/gates.md` summarizes the controller's raw
gate log, `attempt-1/gates-raw.txt`, which sits beside it.

**The scan note.** The builder ran under the same harness session as the controller, so
its scratchpad directory was the controller's, and that directory held plan material. The
wider transcript scan found that the builder used it only for its own logs: every file it
touched there was one it had created, and it never listed, read or searched the directory.
No read of plan material happened. An attempt N ≥ 2 should be checked the same way.

## Why the verdict is PASS

By the plan's rule (Task 20 Step 6), a Critical fails the bar only when the platform claim
it cites holds. There were no Criticals to rule on. The reviewer checked each Critical
class the review prompt names against the code: an unauthenticated write, a cross-tenant
read, a page sent twice or never, a label written twice, an outside reply crossing orgs, a
credential in code or the log, a property that stays green with its code deleted, and a
sim model that cannot produce the faults the brief names. Wherever a hand mutation applied,
the reviewer ran it (`review.md:85-99`). The reviewer re-ran the code as well: eleven hand
mutations at 512 universes from the agent's master seed, each judged by the property it
targets, with pristine controls green under the same filter (`review.md:6-13`,
`:241-257`). Ten went red. M4b stayed green only under the isolation property judged
alone (I2, below).

The four points the review was told to watch all hold (`review.md:21-81`):

1. **The key goes on the deduplicating mailer, and only there.** `Page` puts the platform
   key in the `idempotency-key` header (`src/page.rs:45`), on a body that is byte-identical
   across retries. `Classify` ignores the key (`_key`, `src/classify.rs:39`), which is
   correct, since the classifier does not deduplicate. The mailer model dedupes on the
   header value, never on `OutboundCall::key`, as GUIDE §8 says it must. Removing the
   header gives 2,672 failures over 10,000; the reviewer's M4, a constant header value,
   gives 414 of 512.
2. **The label is exactly-once state, and classifier calls are at-least-once and billed.**
   `Classify::settle` writes the label, and `settle` commits at most once, with the Done
   mark. The model bills every call and answers differently 20% of the time. The property
   requires both `classify:asked-again` and `classify:answers-differed`, and accepts any
   label the classifier answered for that job key rather than insisting on "the" answer.
   That is GUIDE §6's "a remote that ignores the key is called again, and may bill again",
   read the right way round.
3. **Label, on-call capture and page enqueue are one `settle`.** `src/classify.rs:64-79`
   sets the label, captures the on-call address read in the same transaction, enqueues
   `Page` on the job's `ScopedTx` and puts the item. Deleting the enqueue (M2) turns 437 of
   512 universes red; sending to the current address instead of the label-time one (M1)
   turns 50 of 512 red.
4. **The models are the outside world the brief describes.** The classifier answers 429
   and 503 at 5% each, bills every call, never dedupes and varies its answer. The mailer
   answers 503 at 8% without storing it, stores 422 and 200, and replays a repeated key's
   stored response without sending. Both are calmed in the fault-free tail, which is what
   makes `outbound_settles_by_horizon` meaningful. The one gap is the classifier's slow
   tail, which never reaches a timeout in this harness (platform finding 1).

Only `submit`, which the brief mandates, and `signup`, which creates a new org and refuses
an existing id, take writes without a credential, and every read of scoped data is an
`auth` route that opens its scope with `tx.scope::<Org>` (`review.md:91-92`). The five Importants and eleven Minors are testimony, since spec
§16(5) turns on Criticals only. Three Importants are app defects. I1: the app's `judge`
excuses every oracle's failure when durable corruption fired, cross-tenant leaks included
(`tests/sim.rs:1406-1409`), which GUIDE §8 forbids in so many words
(`docs/GUIDE.md:3150-3151`). I2: the isolation property never reads `page_detail`, where
the mailer's reply lands. I5: revoking a key leaves the sessions and keys it minted alive.
I3, I4 and I1's enforcement half mark platform gaps (below).

## Read FINDINGS.md as testimony, not as findings

It is an honest record of what the build felt like. The builder made 41 claims about
charpente. Each was checked against the docs the builder read and against the code at
`706fed55`, read with `git show`, with nothing run. The docs are `triage-run/docs/README.md`
and `triage-run/docs/GUIDE.md`, byte-identical to `README.md` and `docs/GUIDE.md` at
`706fed55`, so every `docs/GUIDE.md:n` below resolves there. The result: **36 right, 3
wrong, 1 right about a symptom for the wrong reason, and 1 unverified**
(`attempt-1/testimony.md:12-19`). The full census, with every ruling's file:line, is
`attempt-1/testimony.md`.

- **T8 (FINDINGS L27) — wrong, in "every".** "A non-member is refused with 400, as every
  §16 example does." Two of §16's `authorize` examples refuse with 400
  (`docs/GUIDE.md:7777`, `:7888`), one admits everyone (`:8144-8146`), and the
  `admit_scope` example refuses with a 401, `Err(ApiError::Unauthorized)` (`:8358-8361`).
- **T38 (L137) — wrong for `attempts`.** FINDINGS lists `attempts` among `JobRow`'s fields
  the docs leave out. §6 names it twice, in backticks, as the row's counter
  (`docs/GUIDE.md:1821-1823`, `:2039`). Only its type is unstated.
- **T40 (L139) — wrong.** FINDINGS lists "whether `Idempotent` stores a 5xx" as missing
  from the docs. GUIDE says a repeated key "gets the first reply again" without
  qualification (`docs/GUIDE.md:3594-3596`), and §6 defines a reply as "the reply of any
  status" (`:1959-1960`). The builder's own L43-45 read it that way and acted on it. What
  the builder lacked was a feature: no `Idempotent` variant declines to store a transient
  failure, which the brief's mailer needed.
- **T35 (L112) — right about the symptom, wrong about the reason.** "… and it is not in
  the repository": the builder could not find the `--mutants` per-mutant record. The verb
  always writes `<dir>/target/charpente-mutants/mutants.out/outcomes.json`
  (`crates/charpente-cli/src/mutants.rs:208-209`, `:319`), and the file exists in the app.
  It is absent only from git, because the scaffold's `.gitignore` has `/target`. The real
  cause is that no doc and no printed line names the path.
- **T33 (L108-110) — unverified.** Why cargo-mutants made no match-arm mutant for
  `Page::settle` is a claim about cargo-mutants 27.1.0's internals, not about charpente.
  The observation itself holds.

**The doc gaps that cost the builder most** (`testimony.md:266-289`, ranked by what
FINDINGS records them costing):

1. `OutboundCall` and `OutboundReply`: their module path, their fields and how to build a
   reply, with no `impl Remote` example anywhere. It was a compiler-diagnostic hunt, and
   both of the app's models depend on it.
2. `SimSource::responses()` never says it holds one entry per request, in arrival order,
   and a prefix when the kernel stops. The app made every 200 body self-identifying to
   guard an unconfirmed reading, which the code confirms (T20).
3. `Snapshot`'s scoped reads are never named, and nothing documented reaches a `&mut Db`
   from an `App`, so every scoped-row oracle went through operator reads.
4. `sim.with_remote` lends `&M` only, with no documented way to signal a model
   mid-universe, so the app shares an `Arc<Mutex<_>>` with its models.
5. The model `Rng`'s path and methods.

Found while checking, and stronger than the builder claimed (T26, right): an API key's or a
session's row id is the first 64 bits of the same 256-bit draw whose hex is the raw
credential (`crates/charpente/src/http/app.rs:236`, `:219`). An app that shows a key id to
anyone but its holder discloses a quarter of that credential's bits, and GUIDE at
`706fed55` does not say so.

**The one restart.** FINDINGS counts one restart of an approach (L118). Under swarm
`sync_error`, a round's single `kernel.run` poisoned early and the rest of its script went
unanswered, so the driver became a supervisor loop of up to four processes. The builder did
not claim it as a doc gap. GUIDE documents each piece (`docs/GUIDE.md:3344-3345`,
`:4548-4551`), but both whole-app examples run one kernel per faulted phase, and nothing
says that a workload that wants verdicts must re-boot and re-send within one
(`testimony.md:291-294`).

## Platform findings

None was fixed in this plan, and no commit belongs to any of them. Attempt 1 passed, so the
fail loop (Task 20 Step 7) never ran. They are what this build hit that no platform claim
covers: candidates for PLAN's v1.x table, which this record does not change.

1. **No per-host, timeout-fired coverage key (I3).** The outbound coverage keys are per
   axis (`docs/GUIDE.md:3641-3647`). None marks "a call to host X timed out", so an app
   cannot `require` that its timeout fired. Here `Classify` declares a 30 s timeout, but
   every faulted kernel run is bounded at 20 s at most and the tail is fault-free, so lost
   request, lost reply and slow all collapse into a call in flight at a crash. Changing
   the timeout to 1 h left the whole coverage map, the verdicts and the give-ups identical
   over 512 universes. The positive control, 5 s, moved `classify:asked-again` from 1,752
   to 1,769 (`review.md:151-176`). The required `fault:outbound_slow` reads as if the slow
   tail was exercised against the classifier, and it was not.
2. **Dead is unreachable in the sim, and an app cannot see its own Dead job (I4).**
   `retry(max = 12, backoff = "1s")` backs off for about 34 minutes in all, plus its
   timeouts, and then the job is Dead. A Dead `Page` leaves its item reading `scheduled`
   with no email sent, and a Dead `Classify` leaves its item unlabelled. There is no Dead
   hook and no backoff cap, and job rows are unreadable from a transaction
   (`docs/GUIDE.md:1657-1660`). The sim never reaches the case: 0 Dead jobs in
   499 universes, and at most 5 of 12 Classify attempts and 3 of 12 Page attempts, because
   the backoff outgrows the rounds (`review.md:178-202`). That `outbound_settles_by_horizon`
   counts Dead as settled is its documented contract, "every outbound job is Done or Dead"
   (`docs/GUIDE.md:3218-3219`), not a defect. The gap is that no sim schedule produces Dead
   and nothing tells the app.
3. **GUIDE §8's corruption-excuse rule is enforced by nothing (I1's platform half).** "Never
   use it to excuse wrong data or a consistency violation" (`docs/GUIDE.md:3150-3151`) is
   prose only. Nothing in `charpente test --sim` or the checkers refuses an app arm that
   excuses every error under `durable_corruption_fired`. With `authorize` admitting every
   operator (M5), 18 of 512 universes recorded a deliberate cross-tenant leak as a give-up.
   The batch still failed on 475 others, so the arm weakened the oracle without blinding it
   (`review.md:105-130`).
4. **`--mutants` cannot mutate a key's placement, so the keyless check stays a hand step
   (review Minor 11, I2).** cargo-mutants mutates no statements or literals, so the
   8 mutants over the two job files touched none of the load-bearing lines: the idempotency
   header, the `Page` enqueue, the 422 arm, the `page_to` capture and the timeout
   (`review.md:230-237`). "Every mutant caught" is weak evidence on its own here. The
   reviewer's M4b, a constant header value, stays green under `orgs_see_only_their_own`
   judged alone and would read MISSED under `--property`, but no generated mutant produces
   it (`review.md:132-149`).

## What the brief asked for that the app did not deliver

Worth keeping visible, as in the statuspage write-up, because it calibrates the friction
ledger. The brief asked that every urgent item cause exactly one email "under the full
fault matrix". The app meets that under the platform's intermittent faults, but a sustained
mailer outage, or a bad `MAIL_TOKEN`, kills the `Page` job after its retries, and the item
reads `scheduled` forever with no email sent (I4). It asked that replay be identical from a
seed, and the app's replay property never replays an outbound-faulted universe; that claim
stands on the controller's double replay, not on the property (Minor 9). It asked that
nothing the outside services return cross into another org's rows. The app holds, but its
isolation property cannot see the one realistic vector, a shared idempotency key (I2). It
asked for "session login and API keys as GUIDE §5 describes", and a session or key minted
from a stolen key outlives that key's revocation (I5). That is beyond the brief, since §5
describes revoke-by-delete and nothing more. Org creation is public (Minor 1), which the
brief never addresses, and one global item id counter lets an anonymous submitter count
every org's items together (Minor 2). Everything else was delivered: the 10,000-universe
batch green with its seed, `--mutants` clean over both job files, the key on the
deduplicating mailer only, the label and its page in one `settle`, and a FINDINGS ledger
with its one restart, its re-read sections and what the docs did not say. Where FINDINGS
reports that something was hard, that is real signal; where it reports that something was
done, check the code.
