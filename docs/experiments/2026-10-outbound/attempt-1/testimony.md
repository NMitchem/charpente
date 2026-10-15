# Attempt 1: testimony classification of `triage/FINDINGS.md`

Classified 2026-10-14 for the outbound-effects plan, Task 20 Step 6.

- **Testimony:** `triage-run/triage/FINDINGS.md` at app HEAD `bbecb23`. The code it describes is `fe71ad6`.
- **Docs the builder was given:** `triage-run/docs/README.md` and `triage-run/docs/GUIDE.md`. Every "GUIDE n" below is a line in that copy.
- **Platform:** charpente at `706fed55`. Every code citation was read with `git show 706fed55:<path>`, and paths are relative to `crates/`.
- **Cross-reference:** `review.md` in this directory. Nothing was run. No cargo was invoked in either tree.

Scope: a sentence is in scope when it claims something about charpente: the platform, its docs, its CLI, its simulator, or its compiler errors. Claims about the app's own design, workload or run counts are out of scope, and so are claims about rustc or cargo-mutants, except where they bear on a charpente verb. Repeats in the Summary's "Not in the docs" list point back to the earlier entry. They get their own entry only where they add a claim.

## Counts

| Verdict | Entries |
|---|---|
| RIGHT | 36 |
| WRONG | 3 (T8, T38, T40) |
| RIGHT ABOUT A SYMPTOM, WRONG REASON | 1 (T35) |
| UNVERIFIED | 1 (T33, a cargo-mutants internal) |

---

## Friction ledger (FINDINGS 11-114)

**T1. L11-13.** "Skimmed §10 (replication) far enough to see nothing in it applies to a single node."
**RIGHT.** §10 configures followers and promotion. Its one restart rule that could look single-node applies only "with a non-empty witness set" (GUIDE 4748-4750).

**T2. L14-15.** `charpente new triage` plus the patch lines "built first time (`cargo clippy --all-targets -- -D warnings`)".
**RIGHT.** The scaffold is pinned to exactly that: `charpente-cli/tests/new.rs:227` `a_scaffolded_app_is_fmt_clean_clippy_clean_under_the_fence_and_green_under_the_sim` runs `clippy --all-targets -- -D warnings` (new.rs:256). The 5.5 s is the builder's own measurement.

**T3. L16-17.** "§5's `login` example mints a session for whatever `subject` the body names, which is an open door if copied."
**RIGHT.** GUIDE 1037-1040: `#[route(POST "/login", public)] fn login(tx: &mut Tx, subject: String, ctx: &Ctx) … Ok(ctx.create_session(tx, subject))`. No caveat accompanies it.

**T4. L17-18.** "The docs offer no password primitive and no hashing helper."
**RIGHT about the docs.**
- GUIDE §5 says the stored hashes are "lowercase-hex SHA-256" (954), but it names no helper and no way to verify a secret.
- The code does export two helpers: `charpente::auth::{hash_token, ct_eq_hex}` (`charpente/src/auth/mod.rs:7`, `auth/hash.rs:6-28`).
- Neither is documented, and unsalted SHA-256 is not a password KDF in any case. So "no password primitive" holds in the code too.

**T5. L18-21.** The docs support a `public(Org)` signup that mints the founder's API key, and an `auth` `POST /login` that trades a live credential for a session.
**RIGHT.**
- `create_api_key` and `create_session` are `Ctx` methods on any route, one mint per request (GUIDE 975-989).
- `public(Org)` admits a credential-less caller into the tenant (GUIDE 1146, 1150-1153).
- Review I5 notes the consequence: a session minted from a key outlives that key's revocation. The GUIDE does not discuss credential lineage, and the builder makes no claim about it.

**T6. L24-25.** "`revoke_key` from §5 deletes any key by id for any authenticated caller. Copied as-is it lets one org's operator revoke another org's keys."
**RIGHT.**
- GUIDE 1065-1070: `let _ = ctx; tx.delete::<ApiKey>(id);` on an `auth` route.
- `ApiKey` is a global table (GUIDE 8330-8333: "revoking a key is `tx.delete::<ApiKey>(id)`, an ordinary write, by design").
- Neither place warns that the example has no ownership check.

**T7. L26.** "There is no 403 in `ApiError` (§5 lists BadRequest, the 401 family, NotFound, Internal)."
**RIGHT.** `charpente/src/http/mod.rs:482-505` has these variants: `BadRequest`, `Unauthorized`, `UnrecognizedCredential`, `SessionExpired`, `Oidc`, `AnonymousScope`, `NotFound`, `Internal`. The `response()` mapping (545-559) emits only 400, 401, 404 and 500. GUIDE 1189-1202 lists the same set.

**T8. L27.** "A non-member is refused with 400, as every §16 example does."
**WRONG**, though only in "every".
- Two of §16's `authorize` examples refuse with 400 (GUIDE 7777, 7888).
- One admits everyone (8144-8146: `Ok(())`).
- The `admit_scope` example refuses with a 401, GUIDE 8358-8361: `// Refuses everyone: the seed below never asks.` / `Err(ApiError::Unauthorized)`.
- The 400 choice itself is what §6's and §16's membership examples model.

**T9. L28-30.** Signup must open the org's scope for a caller with no credential, "that is exactly `public(Org)` (§5 'A route's stance')".
**RIGHT.** GUIDE 1146: "`public(Org)` | every caller | admits every caller into the `Org` tenant it names; no `authorize` runs". The rule-3 401 (1156, 1168) is what plain `public` would have produced.

**T10. L33-34.** "`OutboundCall`/`OutboundReply` live at `charpente::core::io` (§8 never gives the path)."
**RIGHT.**
- The GUIDE names both types only bare: 3209, 3573, 3585, 3593, 3601, 3615. No `use` line anywhere imports them, and no example implements `Remote`.
- They are defined at `charpente-core/src/io.rs:284` and `:302`, and `charpente::core` re-exports `charpente_core` (`charpente/src/lib.rs:9`).
- Neither near path re-exports them. `charpente::outbound` re-exports only `HttpMethod` and `IdempotencyKey` (`charpente/src/outbound.rs:13`), and `charpente_sim::outbound` imports them privately (`charpente-sim/src/outbound.rs:20-22`).

**T11. L34-35.** `OutboundCall { host, method, path, headers: Vec<(String, String)>, body: Vec<u8>, key: IdempotencyKey }`.
**RIGHT.** `io.rs:284-295`. `host` is `&'static str` and `method` is `HttpMethod`.

**T12. L35-36.** "`OutboundReply { status: u16, headers, body }` has public fields and no documented constructor."
**RIGHT.**
- `io.rs:302-306`: three `pub` fields, with no `new` and no builder.
- Every model in the platform builds one by struct literal, e.g. `charpente-sim/src/outbound.rs:73`.
- The GUIDE never shows one being built. Its three model routes (`from_fn`, `Recorder`, `Idempotent`, GUIDE 3584-3602) hand back replies without showing how one is made.

**T13. L36-37.** "the model's `Rng` is `charpente_sim::rng::Rng` with `next_u64`, `chance(f64)` and `below(n)`".
**RIGHT**, and the list is partial.
- `charpente-sim/src/lib.rs:19` declares `pub mod rng`.
- `rng.rs:18-70` has `new`, `next_u64`, `fill`, `chance`, `below` and `between`.
- GUIDE 3573-3577 gives `rng: &mut Rng` with no path and no methods.

**T14. L37-38.** "`JobRow` has `scope: Option<u64>` (the tenant id) and `outbound_key: Option<[u8; 16]>`."
**RIGHT.** `charpente-core/src/job.rs:98`, `:103`.

**T15. L39.** "`Delivered.at` and `.handle` are `u64`."
**RIGHT.**
- `charpente-sim/src/outbound.rs:163-171`.
- GUIDE 3613-3617 describes `at` ("virtual arrival time in nanoseconds") and `handle` ("which `SimOutbound`"), but gives no types.

**T16. L39.** "`Via::ApiKey` carries an `Id<ApiKey>`."
**RIGHT.**
- `charpente/src/auth/ladder.rs:64`.
- The GUIDE writes `Via::ApiKey(id)` (964) and gives `ApiKey`'s `id: Id<ApiKey>` (950), so the type can be inferred but is never stated.

**T17. L40-41.** "`sim.with_remote` lends `&M`, never `&mut M`, so a model cannot be told anything from the workload through it."
**RIGHT.**
- `charpente-sim/src/lib.rs:1152`: `f: impl FnOnce(&M) -> T`. No mutable accessor exists.
- GUIDE 3604 says only "A workload *reads* model state", and "borrows" (3605) leaves mutability open.
- Review Minor 10 confirms the `Arc<Mutex>` workaround is deterministic. It also notes that a request in flight at the last crash is answered by an already-calm model.

**T18. L43-45.** §8's `Idempotent` replays "the first reply again" for a repeated key, of any status, "so a model 503 behind it would be replayed forever".
**RIGHT.**
- `charpente-sim/src/outbound.rs:144-157`: the first reply is stored unconditionally (`self.first.insert(key, (…, reply.clone()))`), then replayed on a matching repeat.
- With the platform's per-job key, every retry would get the stored 503 until the job is Dead.
- This applies to a 503 the *model* produces. The platform's throttle axis answers 429/503 before the model runs ("The remote sees: nothing (overloaded)", GUIDE 3646), so the wrapper never stores those. The builder said "a model 503", which is precise.

**T19. L48-51.** `SimSource::responses()` is documented as "the recorded (status, body) pairs, filled in as the kernel replies", and nothing says they are in arrival order, or what an unreached request gets.
**RIGHT.** The quote is exact (GUIDE 3336-3337). The GUIDE says nothing about order, about one entry per request, or about requests never served. Its one consumer counts 200s and never pairs them (3517-3522).

**T20. L51-53.** The builder's reading: one entry per request, in arrival order, and a prefix when the kernel stopped.
**RIGHT**, confirmed in code.
- `SimSource::poll` pops the script in order (`charpente/src/kernel/mod.rs:1462-1475`).
- The request arm drains one batch in poll order, commits once, then replies in that same order (`kernel/mod.rs:693-747`). Every polled request gets exactly one `send` or `stream`, and both push to the shared vector (1511-1520). A request in a failed commit gets its 500 in place.
- A request never polled, because it lay past the horizon or the kernel stopped on poison (749-753), records nothing.
- The builder's self-identifying bodies were a guard for a reading that is correct.

**T21. L55-56.** "`push_at` and `SimSource::new`'s horizon are absolute virtual nanoseconds. §8 never says so; the tail example's `sim.now().as_nanos() + 600 * SEC` implies it."
**RIGHT.**
- Code: `poll` compares `at <= self.clock.now().as_nanos()`, and `wait_until` compares the absolute target against `horizon` (`kernel/mod.rs:1464-1465`, `1485`).
- GUIDE 3333-3335 and 3374 say "virtual-time horizon" and "virtual nanoseconds", never "absolute". The first example starts at t=0, so it cannot tell the two readings apart. The implication cited is at GUIDE 3828.

**T22. L57.** "`sim.cover` takes a `&'static str` (a compile error told me, E0521)."
**RIGHT.**
- `charpente-sim/src/lib.rs:811`: `pub fn cover(&self, what: &'static str)`.
- The GUIDE uses only literals (3515, 3846) and never states the bound.
- E0521 fits a borrowed `&str` parameter escaping into a `'static` slot. The exact code is not re-derived here.

**T23. L59-61.** "under swarm `sync_error` rates a round's single process poisons on its first few commits and every later request in that round goes unanswered."
**RIGHT** on the mechanism.
- A failed sync poisons the WAL (GUIDE 4529-4533).
- The request arm stops on poison (GUIDE 4548-4551; `kernel/mod.rs:749-753`), and `run` returns `Err` (GUIDE 3344-3345).
- Per T20, unpolled requests record nothing.
- The 18% and the 45 → 18 of 256 are the app's own measurements. They are UNVERIFIED and out of scope.

**T24. L65-68.** The sections re-read and what each answers:
- §8 "Giving up" places the verdict key past every give-up arm.
- §8 "Calling the outside world in a sim" gives the tail's order.
- §6 "When a call fails" says which outcomes are attempts.
- §8 `boot_through_fallout` says what `Ok(None)` means.

**RIGHT**, all four.
- GUIDE 2954-2957: "Its property `require`s instead the key its verdict marks, past every give-up arm."
- GUIDE 3807-3831: restart, `quiesce_disk_faults`, `quiesce_outbound_faults`, boot, run to `now + 600 s`.
- GUIDE 2042-2056: the table.
- GUIDE 3173-3180: what `Ok(None)` means.

**T25. L92.** "Nothing in §5 says what an API key's `id` looks like."
**RIGHT.** §5 gives only the type `id: Id<ApiKey>` (GUIDE 950). It says nothing about how the id is chosen.

**T26. L92-93.** The ids "look drawn from entropy rather than counted".
**RIGHT**, and the fact underneath is stronger. `charpente/src/http/app.rs:236`: `let id = u64::from_str_radix(&self.fresh_token[..16], 16)`. The id is the first 64 bits of the same 256-bit draw whose hex is the raw key `ck_<64 hex>` (237). `create_session` does the same (219). An app that shows a key or session id to anyone but its holder discloses a quarter of that credential's bits. The GUIDE does not say this.

**T27. L94-97.** In the sim the hosts' credentials never reach the models, which "matches §6 ('the value never enters ... the sim')".
**RIGHT.**
- GUIDE 1989-1993: "the value never enters the log, the sim, an error text or `describe`".
- Credentials are appended only by the production client: `charpente/src/serve.rs:1047` "Sends `call` as given, then its host's credential headers".
- Review §4 agrees.

**T28. L97-98.** "no sim oracle can notice a `Host` declared without its credential."
**RIGHT.** This follows from T27. The review records the same: "correct only by reading".

**T29. L98-100.** `charpente describe` lists `api.llm.example env=LLM_API_KEY injects=authorization` and `api.mail.example env=MAIL_TOKEN injects=x-mail-token`.
**RIGHT** in format. GUIDE 7099-7104 gives `pay.test env=PAY_KEY injects=authorization max_request_bytes=… max_reply_bytes=…`, and the code is `charpente/src/describe.rs:537`. The builder quoted the line's head.

**T30. L100-102.** The built binary refuses to start with `charpente refused to start: outbound: LLM_API_KEY is unset — host api.llm.example needs it for job Classify`.
**RIGHT.** The text is `charpente/src/serve.rs:799` (pinned at GUIDE 2001), and the prefix is `charpente/src/launch.rs:212` (`format!("charpente refused to start: {e}")`).

**T31. L104-105.** "The verb prints only the summary."
**RIGHT.**
- `charpente-cli/src/mutants.rs:518-560` prints one `mutants: N total — …` line, plus a MISSED paragraph or a TIMED OUT paragraph only when that count is nonzero. It never names a caught or unviable mutant.
- The MISSED text points at "the run directory" without naming it (532, 543-544).
- The verbatim block at FINDINGS 162-168 matches: the lines above `mutants:` are cargo-mutants' own, passed through.

**T32. L107.** `Ok(Some(Default::default()))` "does not compile: `Call` has no `Default`".
**RIGHT.**
- `charpente/src/outbound.rs:32-37`: `pub struct Call` has no derive and no `Default` impl.
- The on-disk run records it `Unviable` (`triage/target/charpente-mutants/mutants.out/outcomes.json`).

**T33. L108-110.** "cargo-mutants made no match-arm mutant for `Page::settle`'s `200` and `422` arms, because the catch-all arm binds `s` rather than `_`."
**UNVERIFIED as to the cause.** This is a claim about cargo-mutants 27.1.0, not charpente.
- The observation holds: the page.rs-only run left on disk (`mutants.out/mutants.json`, 3 mutants) contains only `FnValue` mutants, with no match-arm genre. That is consistent with review Minor 11 ("It mutates no statements or literals").
- The stated cause would be settled by cargo-mutants 27.1.0's match-arm deletion rule in its source, which is not on this machine.
- It matters to charpente because it bounds what `charpente test --mutants` can test in an outbound job (review Minor 11 and I2).

**T34. L111-112.** "Nothing in GUIDE §8 says where the verb writes its `outcomes.json`."
**RIGHT.** GUIDE 4090-4176 names no output path, and neither `charpente-mutants` nor `mutants.out` appears anywhere in the GUIDE or the README.

**T35. L112.** "… and it is not in the repository."
**RIGHT ABOUT A SYMPTOM, WRONG REASON.**
- The symptom is real: the builder could not find the per-mutant record, and fell back to `cargo mutants --list`.
- The file is not absent, though. The verb always writes `<dir>/target/charpente-mutants/mutants.out/outcomes.json` (`charpente-cli/src/mutants.rs:208-209`, `:319`).
- It exists in this app at `triage-run/triage/target/charpente-mutants/mutants.out/outcomes.json`, beside `mutants.out.old`. The scaffold's `.gitignore` has `/target`, so it is absent only from git.
- The correct cause is that no doc and no printed line names that path (T31, T34).

**T36. L113.** "`cargo test` (no flags) runs the sim's own `main` at its default 256 universes per property."
**RIGHT.** `charpente-sim/src/harness.rs:63` sets `let mut universes = 256u64;`, and harness.rs:680 asserts it. GUIDE 3889-3890 states the same default for `--universes`. The verdict, give-up and 35 s counts are app measurements.

## Summary (FINDINGS 116-170)

**T37. L123-132.** "GUIDE sections re-read, and why": eight pointers.
**RIGHT**, all eight land.
1. §5 "A route's stance" (1137-1146).
2. §5 "Auth", for `Via` (964) and `revoke_key` (1065-1070).
3. §6 "Outbound jobs" (scoped `call`/`settle` shapes, 1926-1931) and "When a call fails" (2042-2056).
4. §8 "Giving up" (2954-2957).
5. §8 "The outside world: remote models", for `with_remote` (3604), `Idempotent` (3590-3602) and restart (3621-3628).
6. §8 "Calling the outside world in a sim" (3807-3831).
7. §8 "The checkers": `boot_through_fallout`'s `Ok(None)` (3173-3180), and `calls_have_durable_jobs` "attributes a call by `OutboundCall::key`" (3209-3210).
8. §16 "Where the guarantee stops": "code that holds the `Db` reads them through a snapshot: `db.snapshot()?.scan::<<row>>()?`" (8306-8307, 8313-8314).

"Not in the docs" (L133-142). Items that repeat an entry above:

| Item | Same as |
|---|---|
| a way to verify a login secret | T3, T4 |
| a 403 | T7 |
| where `OutboundCall`/`OutboundReply` live, their fields, and how to build a reply | T10-T12 |
| the model `Rng`'s methods | T13 |
| `responses()` order and unreached requests | T19 |
| `push_at` absolute | T21 |
| how to tell a model something mid-universe | T17 |
| what an `ApiKey` id looks like | T25 |
| where `--mutants` leaves its outcomes | T34, T35 |

The new claims:

**T38. L137.** "`JobRow`'s fields beyond the four §8 uses (… `attempts` …)".
**WRONG** for `attempts`. §6 names it twice, in backticks, as the row's counter:
- GUIDE 1821-1823: "a *second* transaction records the failure: `attempts` increments, `last_error` gets the message, and: if `attempts < retry.max`: stays Pending, `due_at` moves to …"
- GUIDE 2039: "`attempts` increments, `last_error` names the failure".

Its type (`u32`, `job.rs:79`) is not given.

**T39. L137.** "… (`scope`, … `outbound_key`'s type)".
**RIGHT.**
- The GUIDE never names a `scope` field. It says only "the tenant a job runs in is the one its row carries" (1657).
- It uses `outbound_key` once, as `.is_some()` (3845), with no type. The code is `job.rs:98`, `:103`.

**T40. L139.** "whether `Idempotent` stores a 5xx".
**WRONG.** The GUIDE says it without qualification.
- GUIDE 3594-3596: "A repeated key with the same method, path and body gets the first reply again, and the inner model does not run."
- §6 defines a reply as "the reply of any status" (1959-1960). The code agrees (T18).
- The builder's own L43-45 read the GUIDE this way and acted on it.
- What the builder actually lacked was a feature: no `Idempotent` variant declines to store a transient failure, which the brief's mailer needed. A clause like "whatever its status, a 5xx included" would still be cheap to add.

**T41. L140-141.** "how to read one tenant's scoped rows from a `Snapshot` (§16 says the reads exist, names none; my oracles read through operators instead)".
**RIGHT.**
- GUIDE 8289-8291: "`Db::snapshot()` exposes scoped and raw reads that take a scope on the caller's word". No method is named. They are `Snapshot::get_scoped(scope, id)` and `Snapshot::scan_scoped(scope)` (`charpente-core/src/table.rs:2043`, `:2069`).
- §16 does give a test oracle another way in: `begin_group` → `tenant_admission` → `admit_scope` (8339-8342). But that example takes `db: &mut Db` (8372). An oracle holding the `App` after `kernel.into_app()` has only the documented `app.db() -> &Db` (GUIDE 3348-3349; `charpente/src/http/app.rs:661`). `db_mut` is `pub(crate)` (712), and the only `&mut Db` route is the `#[doc(hidden)]` `db_for_tests` (819-822) or the undocumented `App::begin_group` (731).
- So nothing documented reaches a scoped row from a sim oracle, and the builder's detour through operators was forced.

## Out of scope, noted for the ledger

- **L155-158.** "plus violations excused because durable corruption fired in that universe" describes the app's `judge` arm. Review I1 measured it excusing cross-tenant leaks.
  - This is not a claim about charpente.
  - The GUIDE states the rule the arm breaks: "Never use it to excuse wrong data or a consistency violation" (3150-3151).
  - The docs carried the rule; the app did not follow it, and nothing mechanical enforces it (review I1, "Platform claim").
- **L69-91, L81-84, L143-158.** Planted bugs, workload extensions and run counts are app evidence. `gates.md` and the review's re-runs cover them.

---

## RIGHT doc gaps, ranked by what they cost the builder

Cost here means what FINDINGS records:
- compiler-diagnostic hunts (L31-39, L57);
- workarounds built into the app or its oracle (L48-54, L40-42, L141);
- design choices forced by silence (L16-21, L26).

The one restart (L58-64) was not caused by a claimed gap (see the note after the table).

| Rank | Gap | Entries | What it cost | Candidate GUIDE fix |
|---|---|---|---|---|
| 1 | `OutboundCall`/`OutboundReply`: module path, fields, how to build a reply. No `Remote` impl example anywhere. | T10-T12 | Compiler-diagnostic hunt (`let () = x.field;` tricks). Both models depend on it. | One `impl Remote` example in §8 "remote models", with its `use charpente::core::io::{OutboundCall, OutboundReply}` and a struct-literal reply. |
| 2 | `SimSource::responses()`: one entry per request, in arrival order, a prefix when the kernel stops. | T19, T20 | The app made every 200 body self-identifying, and the oracle checks every pairing, to guard an unconfirmed reading. This shaped the app's wire responses. | One sentence at GUIDE 3336-3337. |
| 3 | `Snapshot`'s scoped reads unnamed, and no documented `&mut Db` from an `App` for `admit_scope`. | T41 | Every scoped-row oracle rerouted through operator reads. | Name `get_scoped`/`scan_scoped` at GUIDE 8289-8291 for operator and oracle code, or show the oracle path from `kernel.into_app()`. |
| 4 | `sim.with_remote` is read-only, with no documented way to signal a model mid-universe. | T17 | An `Arc<Mutex>` side channel between workload and models (review Minor 10). | Say `&M` explicitly at GUIDE 3604, and show the shared-state pattern for a quiesce switch. |
| 5 | The model `Rng`'s path and methods. | T13 | Compiler hunt. | Path and method list at GUIDE 3573-3577. |
| 6 | Credential verification absent; §5's `login` trusts the body; `revoke_key` has no ownership check. | T3, T4, T6 | A design decision (key-based login, an ownership check on revoke), not a hunt. Low cost to this builder, high risk to one who copies. | Mark both examples as not-for-production, and add the subject check to `revoke_key`. |
| 7 | `JobRow`'s `scope` and `outbound_key` types. | T14, T39 | Compiler hunt. | The field list beside §8's `JobRow` imports. |
| 8 | `push_at` and the horizon are absolute. | T21 | Inferred from one example. | One word at GUIDE 3334. |
| 9 | `sim.cover` takes `&'static str`. | T22 | One E0521. | The signature at GUIDE 2744. |
| 10 | No 403, and no guidance on `authorize`'s refusal status. | T7 | A choice (400). | One line at GUIDE 1189-1202. |
| 11 | `Delivered` field types; `Via::ApiKey`'s payload type. | T15, T16 | Compiler hunt, small. | Types at GUIDE 3614 and 964. |
| 12 | What an `ApiKey` id is. | T25, T26 | An experiment (printed ids). | State that the id is the first 64 bits of the minted secret, so an app never shows it to anyone but the holder. |
| 13 | Where `--mutants` writes its record; the verb names no caught mutant. | T31, T34, T35 | Fell back to `cargo mutants --list`. | Name `target/charpente-mutants/mutants.out/` in GUIDE §8 "Finding a one-sided property". |

**The one restart's cause, which the builder did not claim as a gap.**
- What happened: under swarm `sync_error`, a single `kernel.run` per round poisoned early, and the rest of the round's script went unanswered.
- The docs: GUIDE 3344-3345 and 4548-4551 document each piece. Both whole-app examples (3475-3537, 3770-3849) run one kernel per faulted phase, and nothing says a workload that wants verdicts must re-boot and re-send within a phase. GUIDE 3853-3855 does admit the vacuity: "A universe in which neither order committed passes all three checkers having checked nothing".
- Candidate fix: a sentence in "Calling the outside world in a sim" naming the supervisor loop.

**Gaps the review found that the testimony does not mention:**
- no per-host, timeout-fired coverage key (review I3);
- no Dead hook or backoff cap, while job rows are unreadable from a transaction (I4);
- the `durable_corruption_fired` rule is stated but enforced by nothing (I1);
- cargo-mutants cannot mutate an outbound job's key placement (Minor 11, I2);
- `replays_identically` never replays an outbound-faulted universe (Minor 9).
