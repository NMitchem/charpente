# Attempt 1: code review of `triage` (docs-only outbound build)

Reviewed 2026-10-14. App `triage-run/triage` at HEAD `bbecb23`, last code commit `fe71ad6`, tree clean. Platform pinned at `706fed5`, tree clean.
I read BRIEF.md, GUIDE §5, §6, §8, §9 and §16, every file in `src/` and `tests/`, plus `gates.md` and `keyless-check.md`. I did not open FINDINGS.md or the builder's transcript.

I re-ran the code as well as reading it. A scratch copy of the app (patch paths made absolute, its own target dir, sources restored with `cp` and `touch` before each run) was driven with `charpente test --sim --universes 512 --master-seed 0x9363215d418bc339`, filtered to one property per hand mutation. Controls are pristine runs under the same filter, because a filter changes which seeds the batch draws (GUIDE §8). All controls were green:

| control (pristine) | verdicts | gave up | failures |
|---|---|---|---|
| unfiltered, all five properties | 2337 | 223 | 0 |
| `urgent_items_page_exactly_once` | 450 | 62 | 0 |
| `labels_are_written_once` | 460 | 52 | 0 |
| `orgs_see_only_their_own` | 486 | 26 | 0 |

Scratch artifacts (each diff, JSON report and stderr) are in this session's scratchpad under `review/out/`. They will not outlive the session, so every number they support is quoted here.

**Totals: 0 Critical, 5 Important, 11 Minor.**

---

## The four things to pay attention to

### 1. Where the app puts the idempotency key, and why

- **`Page` (mailer):** the platform key goes in the `idempotency-key` request header (`src/page.rs:45`). The brief says the mailer honours `Idempotency-Key`. Header names are case-insensitive, and `.header` sends them lowercase (GUIDE §6). The body is deterministic across retries: `self.to` comes from the payload, the item text never changes, and the subject is `self.item`. So a re-call is byte-identical, and a Stripe-style 409 cannot arise.
- **`Classify` (classifier):** the key is ignored (`_key`, `src/classify.rs:39`). This is correct. The classifier does not deduplicate, so a key there would do nothing. The doc comment at `classify.rs:13-14` says so.
- **The mailer model** dedupes on the *header value* (`tests/sim.rs:299-321`), never on `OutboundCall::key`. That matches GUIDE §8: "The key is what the app put in the request, never `OutboundCall::key`". It attributes sent mail to jobs by `OutboundCall::key` (`SentMail.key`), which is the attribution GUIDE §8 sanctions.
- **Mechanical proof, both directions:**
  - Removing the header: 2,672 failures over 10k (`keyless-check.md`).
  - My M4, a constant header value: 414 of 512 failures, first `urgent item 31 of org 1 reads sent, and the mailer sent 0 emails for it`.

### 2. Exactly-once state against at-least-once calls

- **The label is state.** `Classify::settle` writes it, and settle commits at most once per job together with the Done mark (GUIDE §6, "Retry, dead-letter, and exactly-once effects").
- **There is one Classify per item.** It has a single enqueue site (`lib.rs:502`), and every submit mints a fresh id.
- **Classifier calls are at-least-once, and the sim treats them that way:**
  - The model bills every call (`ClassifierState.calls`).
  - The model answers differently 20% of the time.
  - The property requires `classify:asked-again` and `classify:answers-differed`.
  - The property accepts any label among the job key's 200 answers (`sim.rs:1724-1734`). It does not insist on "the" answer, which is the honest oracle when the remote does not dedupe.
- **The app does not pretend to dedupe classification.** That is the right reading of GUIDE §6's "A remote that ignores the key is called again, and may bill again… `settle` still commits at most once, so the app's state stays exactly-once, but the remote's bill does not."

### 3. Is the urgent page enqueued in the same transaction as the label?

Yes. `classify.rs:64-79` does four things in the one `settle` transaction that marks the Classify job Done:

- sets `item.label`;
- for an urgent label, sets `page = Scheduled` and `page_to = <the on-call address read in the same transaction>`;
- enqueues `Page { item, to }` on the job's `ScopedTx`;
- puts the item.

So the label and its page commit as one record. The address is the one current at the label, in the same transaction, which is the brief's "as it was when the item was labelled", taken literally.

Mechanical proof:

- **M2** (delete the `tx.enqueue(Page …)`): 437 of 512 failures, `urgent item 10 of org 1 has 0 Page jobs, not one`.
- **M1** (Page sends to the *current* on-call address, not the label-time one): 50 of 512 failures, `records Some("m-1") to oncall-3-v0…, but the mailer sent … to: "oncall-3-v1@pager.example"`.

### 4. Are the sim models the outside world, or a convenience that cannot fail?

They are mostly the outside world the brief describes.

- **Classifier** (`sim.rs:198-251`):
  - 429 and 503 at 5% each until calmed;
  - every call billed;
  - no dedupe;
  - 80/20 answer variance;
  - 404 for a wrong path, 400 for a bad body.
- **Mailer** (`sim.rs:289-377`):
  - 503 at 8%, not stored, because the request was never processed;
  - 422 for `bounce.example`, stored;
  - 200 `{"id"}`, stored;
  - a repeated header key replays the stored response without sending.

Both are calmed in the fault-free tail (`sim.rs:393-398`, `1310`), mirroring `quiesce_outbound_faults`, which is what makes `outbound_settles_by_horizon` meaningful.

The platform's five outbound axes supply the rest: refused, lost request, lost reply, throttle, slow. The page property requires all five fault keys.

Neither model can check credentials. The edge injects them (`charpente/src/serve.rs`) and the sim never sees them, by design. So `bearer_env("LLM_API_KEY")` and `header_env("x-mail-token", "MAIL_TOKEN")` (`lib.rs:565-566`) are correct only by reading. They match the brief.

There is one real gap: the classifier's "slow at the tail" never becomes a timeout in this harness (Important 3).

---

## Critical

**None.** Each Critical class named in the dispatch was checked against the code. Where a hand mutation applied, it was also run.

| Critical class | Finding | Evidence |
|---|---|---|
| Unauthenticated write | Only `submit` (mandated by the brief) and `signup` take writes without a credential. `signup` creates a *new* org and refuses an existing id (`lib.rs:350-355`). It reaches no other tenant. | A design choice outside the brief, listed as Minor 1. |
| Cross-tenant read on any path | Every read of scoped data is an `auth` route with `tx.scope::<Org>` (`list_items`, `read_item`, `set_oncall`). `authorize` compares the caller's `Operator.org` row (`lib.rs:44-55`). | M5 (authorize admits everyone): 475 of 512 red. |
| A page sent twice | Ruled out by the per-job header key. | `keyless-check.md`; M4: 414 of 512 red. |
| A page never sent | Possible only when a Page job goes Dead. | Important 4. |
| A label written twice | Single enqueue site, plus settle-once. | M6 (double enqueue): 447 of 512 red. M10 (label not the classifier's): 438 of 512 red. M12 (Page::settle relabels): 432 of 512 red. |
| An outside reply crossing orgs | Replies carry only a label or a message id. Settle writes through the job's own `ScopedTx`, keyed by the payload's item id. No row id is ever derived from a reply. The only vector is an idempotency replay under a shared key, which the platform's per-job keys rule out. | `keys_are_per_job` is checked (`sim.rs:1513`). The isolation property's blind spot for this vector is Important 2. |
| A credential in code or the log | Environment-variable names only. Raw tokens exist only in the response that mints them. | `git log -p` shows no secrets. |
| A property that stays green with its code deleted | Every hand mutation went red under the property it targets. The exception is the isolation property judged alone, Important 2. | Hand-mutation table. |
| A sim model that cannot produce the named faults | Each named fault is produced. The closest call is the slow tail being truncated into a crash. | Important 3. |

---

## Important

### I1. `judge` excuses every oracle's failure when durable corruption fired, cross-tenant leaks included

`tests/sim.rs:1406-1409`:

```rust
Err(e) if durable_corruption_fired(sim) => {
    sim.give_up(format!("durable corruption fired, and: {e}"));
    Ok(())
}
```

All four judged properties go through this arm. GUIDE §8 (`durable_corruption_fired`) says the excuse is for an acked-durability *shortfall*, and: "Never use it to excuse wrong data or a consistency violation (a cut reverts state to a prefix, which stays consistent)."

Some errors here legitimately need the excuse:

- `calls_have_durable_jobs` (GUIDE §8 says to route it this way);
- an acked item gone;
- a label read before a cut and re-asked after it.

The isolation property's errors never do. No cut can make org B's read return org A's item. The same goes for the page property's cross-org checks, such as `paged org {addr_org}'s on-call`.

**Measured.** With `Org::authorize` admitting every operator (M5) and an `eprintln!` in this arm, 18 of 512 universes recorded a deliberate cross-tenant leak as a give-up instead of a failure. The batch still failed on the other 475, so this weakens the oracle rather than blinding it. A leak that showed up only alongside media corruption would be invisible.

**Fix:** excuse only the shortfall classes, each by name, and let every consistency or isolation error fail.

**Platform claim:** none mechanical. GUIDE §8 states the rule in prose, and nothing in `charpente test --sim` or the checkers enforces it.

### I2. `orgs_see_only_their_own` cannot see a reply crossing orgs; only its sibling catches the one realistic vector

The isolation property checks:

- items by text tag;
- `page_to` by address (`sim.rs:1814-1818`);
- classifier text against the job's org;
- mail `to` and body against the job's org.

It never reads `page_detail`, which is where the mailer's reply lands: the message id, or the 422 body, which names the address (`page.rs:60-67`; the model's 422 text is `"<address> is not deliverable"`).

The vector by which "what the outside services return" could cross into another org's row is an idempotency replay: two jobs sending one key, so org B's settle receives org A's stored response.

**Measured.** M4b (constant header key), judged by `--filter orgs_see_only_their_own` alone, was **green, 0 failures in 512 universes, 494 verdicts**. In those universes, every page after the first settles with another org's stored mailer response, whether a message id or A's 422 text. Only `urgent_items_page_exactly_once` goes red (M4: 414 of 512), and it notices the missing send, not the crossing. Under GUIDE §8's `--property orgs_see_only_their_own`, this mutant would read MISSED.

**Fix:** assert that every `page_detail` is a mailer response produced for this org's own Page job key: a `MailOutcome::Sent(id)` or `Refused` whose `key` is a job of `s.org`.

**Platform claim:** GUIDE §8's `--property` scoping ("one honest sibling hides every one-sided property beside it") is the instrument that would show it. But cargo-mutants generates no string-literal mutant for the header value, so `charpente test --mutants` cannot produce this mutant. No claim refuses it.

### I3. The classifier's slow tail never reaches `Classify`'s 30 s timeout: that path is dead in the sim

- `Classify` declares `timeout = "30s"` (`classify.rs:19`).
- Every faulted kernel run is bounded at `base + ROUND_NANOS - first`, at most 20 s (`sim.rs:78`, `1295`).
- The tail is fault-free and calmed.

So no classifier call is ever outstanding past its deadline inside a live process. Each of lost request, lost reply and slow (GUIDE §8's "the LLM tail") collapses into a call in flight at a crash: re-called on restart, never timed out, never counted as an attempt.

**Measured:**

- T1, Classify timeout 30 s changed to 1 h: the whole JSON coverage map, the verdicts and the give-ups are **identical** to the control over 512 universes.
- T1p, the positive control, changed it to 5 s: the run differs (`classify:asked-again` 1752 to 1769), so the edit path is live.
- Over 499 universes, the most attempts any Classify job reached was 5 of 12. The most for any Page job was 3 of 12.

The required key `fault:outbound_slow` on `labels_are_written_once` gives the impression the slow tail was exercised against the classifier. It is marked per axis, not per host, and the slow-but-alive case never arises.

Two things are never exercised:

- the app's own choice of 30 s, which is a billing and liveness parameter under the brief's "slow at the tail";
- the timeout-then-re-call-in-the-same-process path, which bills the classifier a second time.

State correctness does not depend on either (settle-once is the platform's), which is why this is not Critical.

**Fix:** a round long enough to outlive the timeout (more than 30 s plus backoff), or a dedicated phase with only the outbound axes on and no crash.

**Platform claim:** none. The outbound coverage keys are per axis (GUIDE §8's table), and no key marks "a call to host X timed out". So an app cannot `require` that its timeout fired.

### I4. Dead is unreachable in the sim and unhandled in the app: a sustained outage silently loses pages and labels

- `retry(max = 12, backoff = "1s")` on both jobs (`classify.rs:18`, `page.rs:14`) waits at most 1+2+…+1024 s ≈ 34 min (GUIDE §6: doubling, saturating), plus 12 timeouts.
- After that the job is Dead.
- For `Page`, `item.page` stays `Scheduled` forever and no email goes out. For `Classify`, the item stays unlabelled forever.
- Nothing records the death on the item, and nothing re-enqueues.

The same happens on any non-422 refusal that never clears. For example, a bad `MAIL_TOKEN` gives a 401 on every attempt (`page.rs:69-74`) until the job dies with `page: scheduled`.

The sim cannot reach this:

- 0 Dead jobs in 499 universes;
- at most 5 of 12 Classify attempts and 3 of 12 Page attempts, because the backoff outgrows the 20 s rounds and the tail is fault-free;
- the oracle's `ends with page Some("scheduled")` arm would catch it (M3 reached it), but no fault schedule produces it.

The brief's faults are intermittent ("sometimes 503"), and the app meets the brief's property under the platform's per-call-independent fault matrix. That is why this is Important and not Critical. For a pager, "the urgent email was silently dropped after 34 minutes of mailer outage, and the item still reads scheduled" is the failure an operator would least expect.

The platform constrains the fix:

- Job rows are unreadable from a transaction (GUIDE §6, "Job rows are the platform's").
- There is no Dead hook and no backoff cap.

So the app cannot see its own Dead job. Raising `max` only stretches the gaps (attempt 20 waits about 6 days). The workable shape is an app-level key in the header (stable per item, for example derived from `Item.id`, which is globally unique) plus a cron sweep that re-enqueues pages stuck in `Scheduled`, or an `attempts` field on the item written by `settle` on each non-200.

**Platform claim:** none. GUIDE §6 states "Dead means no reply was settled", and `outbound_settles_by_horizon` counts Dead as settled.

### I5. Revocation is incomplete: a stolen key outlives its own revocation

- `POST /login` trades any credential for a 30-day session (`lib.rs:403-413`).
- `POST /keys` mints more keys from any credential (`lib.rs:435-445`).
- Revoking a key (`lib.rs:447-458`) does not touch the sessions or keys it minted.

There is no route that lists an operator's keys or sessions. An owner therefore cannot learn the ids of keys an attacker minted, and cannot revoke them. `logout` revokes only the presenting session, and there is no operator removal. A leaked key is a permanent compromise of that operator.

The brief asks only for "session login and API keys as GUIDE §5 describes". GUIDE §5 describes revoke-by-delete and nothing more. So this is beyond the brief, but it is a security property a reviewer of the auth surface would flag.

**Platform claim:** none.

---

## Minor

1. **Public signup** (`lib.rs:344-377`, `public(Org)`). Any anonymous caller creates an org at an id of its choosing and receives an operator key. It refuses existing ids, so there is no takeover, but ids can be squatted. The brief is silent on org creation, so this should be recorded as a decision rather than left implicit.
2. **The global item id counter leaks cross-tenant volume.** `next_id::<Item>` draws from one global counter (`lib.rs:75-88`, `492`), and `submit` returns the id (`lib.rs:503-507`). An anonymous submitter learns how many items every org together has received. Per-org counters (a scoped counter row) would close it.
3. **No `label.is_none()` guard in `Classify::settle`** (`classify.rs:64-67`). Once-ness rests entirely on the single enqueue site. A future re-classify path would overwrite silently. M6 shows the sim would catch it, so this is defence in depth only.
4. **A 200 with a body that is not `{"id"}`** makes `reply.json()?` fail (`page.rs:60`). The retry gets the mailer's stored copy of the same body, so the job dies with the mail sent and the item reading `scheduled`. The mailer's contract rules this out today.
5. **`logout` with an API key** answers 200 and revokes nothing (`lib.rs:415-419`, `ctx.revoke` is a no-op without a session). A caller may believe it logged out.
6. **The on-call durability check is end-state only** (`sim.rs:1470-1488`): final version at least the last acked one. An acked on-call version lost in a crash and later superseded by a newer acked put is not detected. Intermediate `List` reads are not checked for regressions.
7. **The page and label properties skip an org with no `TailFinal` read** (`sim.rs:1531`, `1698` iterate `finals`). Only `acked_writes_survive_crashes`, which runs on different universes, fails on a missing final read.
8. **Cross-org probes use only the founder's API key** (`sim.rs:699-721`), never a session or the second operator. This is adequate given `authorize` keys off the subject, but it is narrower than the four credential kinds the workload holds.
9. **`a_universe_replays_from_its_seed` never replays an outbound-faulted universe.** `replays_identically` builds `Sim::new` worlds, and GUIDE §8 notes that `FaultConfig::default()` turns the five outbound faults off. The batch-level `--master-seed` double run in `gates.md` (REPLAY_IDENTICAL at 10k) covers the swarm universes, so the brief's replay claim stands on the gate, not on the property.
10. **Model state is shared through `Arc<Mutex>` and mutated from the workload** (`Models::calm`), not through `sim.with_remote`. It is deterministic and read only after `sim.outbound_deliveries()` (`sim.rs:1363`). But a request in flight at the last crash is answered by an already-calm model, which slightly under-applies the model's own overload on those calls.
11. **The `--mutants` evidence is thin by construction.** cargo-mutants found 8 mutants over the two job files (6 caught, 2 unviable; `gates.md` agrees). It mutates no statements or literals, so none of the load-bearing lines were mutated:
    - the idempotency header;
    - the `Page` enqueue;
    - the 422 arm;
    - the `page_to` capture;
    - the timeout.

    Every hand mutation of those lines below went red under its target property, so the app's oracles are sound. "Every mutant caught" is just weak evidence on its own here.

---

## Hand-mutation results

512 universes, master seed `0x9363215d418bc339`, each run filtered to the property named.

| id | mutation | judged by | result |
|---|---|---|---|
| M1 | `Page::call` sends to the current `OnCall` address, not `self.to` | urgent | RED 50/512 (`records Some("m-1") to …-v0…, but the mailer sent … to …-v1…`) |
| M2 | delete `tx.enqueue(Page { … })` in `Classify::settle` | urgent | RED 437/512 (`has 0 Page jobs, not one`) |
| M3 | 422 arm returns `Err` (retried) | urgent | RED 215/512 (`ends with page Some("scheduled") and job … dead, attempts: 12`) |
| M4 | constant `idempotency-key` header value | urgent | RED 414/512 (`reads sent, and the mailer sent 0 emails`) |
| M4b | M4, judged by isolation alone | isolation | **GREEN 0/512** (I2) |
| M5 | `Org::authorize` admits every operator | isolation | RED 475/512; 18 more excused as give-ups by the corruption arm (I1) |
| M6 | `submit` enqueues `Classify` twice | labels | RED 447/512 (`has 2 Classify jobs`) |
| M9 | `Page::settle` treats any other status as Sent | urgent | RED 157/512 (`reads sent, and the mailer sent 0 emails`) |
| M10 | non-urgent labels forced to `bug` | labels | RED 438/512 (`which the classifier never answered`) |
| M12 | `Page::settle` relabels the item `unsure` on 200 | labels | RED 432/512 |
| M14 | `Page` payload addressed to org 1's on-call | isolation | RED 422/512 (`a page crossed orgs`) |
| T1 | `Classify` timeout 30 s changed to 1 h | labels | GREEN, coverage map **identical** to control (inert path, I3) |
| T1p | `Classify` timeout 30 s changed to 5 s (positive control) | labels | GREEN, coverage differs (`classify:asked-again` 1752 to 1769) |
| T2 | `Page` timeout 10 s changed to 1 h | urgent | GREEN, coverage differs (live path; no property is sensitive to the timeout, which is expected for a liveness parameter) |

---

## What this build says about the platform (for the outbound-effects ledger)

- **The docs carried the hard parts.** The builder placed the key correctly on the deduping remote and nowhere else, read GUIDE §6's billing paragraph the right way round, and put the label, the address capture and the page enqueue in one `settle`.
- **Gaps that this app hit and the platform offers no claim for:**
  - No per-host, timeout-fired coverage key (I3).
  - No Dead hook or backoff cap, while job rows are unreadable from transactions. An app that must never silently drop an outbound effect has to build its own key and sweep (I4).
  - GUIDE §8's `durable_corruption_fired` rule is enforced by nothing (I1).
  - cargo-mutants cannot mutate the line that matters most in an outbound job, the key's placement, so the keyless check remains a hand step (Minor 11).
