# Retry-safe writes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** a write request that carries an intent id applies once, however often it is re-sent:
the kernel records the intent in the request's own transaction and answers a repeat with the
first reply.

**Architecture:**
- A sealed platform table, `charpente.intents`, lives in charpente-core. Only a capability
  minted from a `Group` can reach it, and handlers never hold a `Group`.
- `StagedDispatch::stage_or_subscribe` does the work inside the closure it already stages:
  - it looks the intent up after `build_ctx`;
  - it answers a repeat from the row, or else runs the glue and records the row if the
    transaction wrote anything.
- `Drive` stamps every write it sends, so the sim exercises the dedupe without any workload
  change.
- The wire refuses a write that carries no id.
- A kernel-run sweep drops rows `INTENT_RETENTION` after the later of their commit and the
  node's open.

**Tech Stack:** Rust (workspace crates charpente-core, charpente, charpente-macros,
charpente-sim, charpente-cli, charpente-tsgen), `sha2` (already a dependency of
`charpente`), TypeScript (the generated client), trybuild.

**Spec:** `docs/superpowers/specs/2026-11-22-retry-safe-writes-design.md` (revision 3).

## Global Constraints

**Workspace and commits**
- Work in the worktree `../charpente-rsw` on branch `retry-safe-writes`, with
  `CARGO_TARGET_DIR=<worktree>/target`. Never share a target dir across worktrees.
  Fast-forward `main` at the end.
- No `Co-Authored-By` trailer on any commit (CLAUDE.md). No employer named or alluded to
  anywhere.

**Every task's gate**
- Run three commands:
  - `cargo fmt --all --check`
  - `cargo clippy --workspace --all-targets -- -D warnings`
  - `cargo test -p <each touched package> --no-fail-fast 2>&1 | tee <log>`
- Confirm green by counting `grep -c "^test result: ok"` against the package's suite count.
  The absence of `FAILED` proves nothing.
- The shell is zsh: read status with `${pipestatus[1]}`, never `PIPESTATUS`. Pipe through
  `tee`, never `tail`.
- Tasks 4, 5 and 9 also run `cargo test --workspace --no-fail-fast`, in the background,
  because it outruns the 600 s tool ceiling.

**Seeds**
- Every task that changes behaviour under the sim states the seed count of its sim evidence
  in the commit.
- A pinned seed that stops reproducing its scenario gets a new seed found for the same
  reason (its assertion text), never a loosened assertion. The commit names the old and the
  new seed.

**Docs (hard rule 6)**
- Each task updates the docs its public change touches, in the same commit.
- CHANGELOG entries go under `## 0.7.0 (unreleased)`, in the existing voice: bold lead, then
  what breaks and how to adapt.

**Exact values (spec §4)**
- Table name: `charpente.intents`.
- Header: `Idempotency-Key`, stored lowercased as `idempotency-key` in `Request.headers`.
- A valid id is 1 to 255 bytes, each one 0x21..=0x7e.
- A replayed reply carries the header `idempotent-replayed: true`.
- A kept reply is at most 65_536 bytes, headers plus body (`KEPT_REPLY_MAX`).
- `INTENT_RETENTION` is 24 h.
- `PROTO_VERSION` becomes 9 and `MANIFEST_FORMAT` becomes 6.

**Plan-time calls (spec revision 3)**
- `INTENT_SWEEP_INTERVAL` is 1 h and `INTENT_SWEEP_BATCH` is 4096.
- The row id is the first 8 bytes (big-endian) of a SHA-256 over the length-prefixed
  principal, route and id.
- Missing, malformed and non-ASCII ids share one 400 text. The edge drops a non-ASCII header
  before charpente sees it.
- The scaffold keeps its guard, because write-once is app semantics.
- README keeps 0.6.0's red runs as labelled history.

**Exact refusal copy**
`{H}` is the route's id header (`Idempotency-Key` or the declared one). `{M} {P}` is the
method and pattern.
- 400: ``"`{M} {P}` needs an {H} header, 1 to 255 visible ASCII characters: charpente answers a re-sent write once (GUIDE §5). The generated client sends one; a webhook route names its sender's id header with `intent = header(...)`."``
- 422: ``"{H} `{id}` was already used for a different request to `{M} {P}`: send this request under a new {H}."``
- 409, credential withheld: ``"this request already committed under {H} `{id}`, and its reply carried a credential, which charpente shows once: send a new request under a new {H} to get a new one."``
- 409, reply too large: ``"this request already committed under {H} `{id}`, and its reply was {n} bytes, over the 65536 charpente keeps: read the result with a GET."``
- 409, slot collision: ``"{H} `{id}` shares a record slot with another request's id: send this request under a new {H}."``

## Review Focus

Each line names an input and the expected behaviour. Each one has a test in the task that
owns the code.

1. **A re-send in the same kernel batch as its first copy, with a failed commit.** Both are
   answered 500. Neither is replayed as a success. (Task 3:
   `a_repeat_in_the_same_group_fails_with_its_first_copy`)
2. **One intent id used by two principals.** These are two intents, and each applies.
   (Task 3: `one_intent_id_under_two_principals_is_two_intents`)
3. **A re-sent login.** It gets the 409 credential text, and the first reply's raw token is
   in no stored byte. (Task 3:
   `a_resent_login_is_refused_and_its_token_never_reaches_disk`)
4. **A re-send after a power cycle that jumped the wall clock 3 days.** It is answered from
   the record and applies once. (Task 7:
   `a_resend_after_a_three_day_power_cycle_is_answered_from_its_record`)
5. **A key over 255 bytes, or with a non-ASCII byte, sent over real TCP.** It gets the 400
   text. (Task 5: the edge-harness smoke test)

---

### Task 1: Open 0.7.0

**Files:**
- Modify: every file precedent `6b8c5166` touched (`git show --stat 6b8c5166`): the
  workspace `Cargo.toml` version and internal pins, each `"charpente_version": "0.6.0"`
  golden, and the scaffold's version source.
- Modify: `examples/double-charge/Cargo.toml` (both pins become `=0.7.0`).
- Modify: `README.md`, in "Example: a double charge". The sentence "The demo pins charpente
  0.6.0, which printed the output above" becomes "The demo pins charpente 0.7.0; charpente
  0.6.0 printed the output above."
- Modify: `CHANGELOG.md`, which opens `## 0.7.0 (unreleased)` above 0.6.0.

**Interfaces:**
- Consumes: none.
- Produces: the workspace version is 0.7.0.

- [ ] **Step 1:** List what `6b8c5166` changed. Run `git grep -n '0\.6\.0'` and sort each
  hit into one of two piles:
  - **A version pin:** bump it.
  - **History:** leave it. CHANGELOG's 0.6.0 section, the README video caption and
    `docs/demo/double-charge.tape` are history.
- [ ] **Step 2:** Apply the bump, the README sentence and the CHANGELOG heading.
- [ ] **Step 3:** Run the gate. The packages are `charpente-cli` (`demo`, `describe`, `new`),
  `invoicing`, `charpente` (`--test describe`) and `guide-examples`.
  Expected: all green, counted. `patched_demo`'s version assertion passes, and so does
  `readme_names_the_demos_rust_version_and_its_release`, which now finds "charpente 0.7.0".
- [ ] **Step 4:** Commit with "release: versions to 0.7.0; CHANGELOG opens 0.7.0 (unreleased)".
  The demo's own `Cargo.lock` stays as it is, because it is refreshed after the publish, as
  0.6.0's was.

---

### Task 2: The sealed intents table in core

**Files:**
- Create: `crates/charpente-core/src/intent.rs`.
- Modify: `crates/charpente-core/src/lib.rs` (`pub mod intent;` and re-exports).
- Modify: `crates/charpente-core/src/db.rs`:
  - `PLATFORM_TABLES` (:1206) becomes `[&str; 4]`.
  - `refuse_platform_table` (:1237) and `refuse_platform_read` (:1275) each gain an arm.
  - A new token `IntentAccess` and `Group::intent_access`, beside `TenantAdmission` (:2664).
  - New `Tx` methods.
  - Registration in `Db::open` (:3529), `seed_schema` (:4732) and `Seeder::new`'s
    `import_schema` (:5340).
- Modify: `crates/charpente-core/src/table.rs`. A note for `charpente.intents` goes in each
  of the `AppWritable` (:517) and `ScopeReadableBy` (:672) diagnostics.
- Modify: `crates/charpente/src/repl/mod.rs:57` (`PROTO_VERSION = 9`, plus a history line).
- Modify: `crates/charpente/src/archive/manifest.rs:93` (`MANIFEST_FORMAT = 6`, plus a
  history line).
- Create: three trybuild cases in `crates/charpente-macros/tests/compile_fail/`, each with
  its `.stderr`:
  - `intent_row_written_on_bare_tx.rs`
  - `intent_row_read_on_bare_tx.rs`
  - `intent_row_table_name_borrowed.rs`
- Test: `crates/charpente-core/tests/engine.rs`. The seeder assertion goes in whichever
  core test file already exercises `Seeder::new`.
- Docs: DESIGN.md's platform-table list, and CHANGELOG (the new platform table and both
  format numbers). State that a 0.6.0 peer or archive reader is refused by name, and that a
  0.6.0 binary cannot open a 0.7.0 data directory once it holds an intent record.

**Interfaces:**
- Produces (in `charpente_core::intent`, re-exported at the crate root like `JobRow`):
  ```rust
  pub const INTENT_RETENTION: Duration = Duration::from_secs(86_400);
  pub struct IntentRow {
      pub id: Id<IntentRow>,
      pub intent: String,            // the raw id the client sent
      pub principal: Option<String>, // the subject, None on a public route
      pub route: String,             // "POST /orders/:id"
      pub fingerprint: [u8; 32],
      pub committed_at: WallTime,
      pub status: u16,
      pub reply: IntentReply,
  }
  pub enum IntentReply {
      Kept { headers: Vec<(String, String)>, body: Vec<u8> },
      Credential,
      TooLarge { bytes: u64 },
  }
  // Row impl: Scope = PlatformOwned, TABLE = "charpente.intents", INDEXES = &[],
  // FINGERPRINT left None. The hand-written codec follows JobRow's (job.rs:118).
  ```
- Produces (in `db.rs`). Each token is a private-field `PhantomData` type, exactly as
  `TenantAdmission` is:
  ```rust
  pub struct IntentAccess<'g>(PhantomData<&'g ()>);
  impl<'a> Group<'a> { pub fn intent_access(&self) -> IntentAccess<'a>; }
  impl Tx<'_> {
      pub fn get_intent(&self, id: Id<IntentRow>, access: &IntentAccess<'_>) -> io::Result<Option<IntentRow>>;
      pub fn put_intent(&mut self, row: &IntentRow, access: &IntentAccess<'_>);
      pub fn delete_intent(&mut self, id: Id<IntentRow>, access: &IntentAccess<'_>);
      pub fn intents_committed_before(&self, before: WallTime, limit: usize, access: &IntentAccess<'_>) -> io::Result<Vec<Id<IntentRow>>>;
      pub fn has_effects(&self) -> bool; // TxEffects::is_empty negated; adoption rows are added later and do not count
  }
  ```
  - `get_intent` reads this Tx's overlay, then the group's, then the snapshot (`get_any`'s
    order).
  - `put_intent` and `delete_intent` record `written_as` as `put_schema_version` does
    (:1434). Without that, the negative-space assert at :4944 fires.

- [ ] **Step 1: Write the failing tests in `crates/charpente-core/tests/engine.rs`.**
  ```rust
  #[test] fn an_intent_row_round_trips_through_the_log_and_a_reopen()
  //   stage put_intent through a Group's intent_access, commit, drop and reopen the Db
  //   over the same disk; Db::begin_group → stage → get_intent == the row, every field
  //   (cover every IntentReply variant).
  #[test] fn a_later_stage_in_one_group_sees_an_earlier_stages_intent()
  //   stage A puts; stage B in the same group, before commit, get_intent → Some.
  #[test] fn has_effects_turns_true_at_the_first_write_and_not_before()
  //   inside one stage: false after a get, true after a put.
  #[test] fn intents_committed_before_returns_only_older_ids_and_honours_the_limit()
  //   rows at t=1,2,3; before=3, limit=10 → the two older ids; limit=1 → exactly one.
  #[test] fn delete_intent_removes_the_row()
  #[test] fn the_seeder_imports_intents_and_seed_schema_strips_them()
  //   seed_schema().ops("charpente.intents") is refused; a Seeder fed an intents row
  //   finishes and the opened Db's get_intent returns it.
  ```
- [ ] **Step 2: Run the tests.** Run `cargo test -p charpente-core --test engine --no-fail-fast`.
  Expected: a compile failure, because `IntentRow` and `intent_access` are undefined.
- [ ] **Step 3: Implement** the Interfaces above. The refusal arm's text names the table
  and the fix, in the existing arms' form: "`charpente.intents` rows are the platform's
  record of answered writes, and app code cannot write/read them".
- [ ] **Step 4: Write the three trybuild cases.**
  - Generate each `.stderr` with `TRYBUILD=overwrite`, then review it line by line against
    the refusal text.
  - Prove the cases with the FULL `cargo test -p charpente-macros --test trybuild`. A
    filtered trybuild run uses `cargo check`, so the post-monomorphization refusal never
    fires there.
- [ ] **Step 5: Bump the format numbers.** Set `PROTO_VERSION = 9` and
  `MANIFEST_FORMAT = 6`, each with a history line saying the platform table
  `charpente.intents` ships in seeding and archives. Any test that pins either number as a
  literal moves with it.
- [ ] **Step 6: Run the gate.** Run it over `charpente-core`, `charpente-macros`
  (`--test trybuild`) and `charpente` (`--test replication`, `--test archive`), counted.
- [ ] **Step 7: Commit** with "core: the sealed `charpente.intents` table, reachable only
  through a Group's IntentAccess; PROTO_VERSION 9, MANIFEST_FORMAT 6".

---

### Task 3: The request path looks up, records and answers intents

**Files:**
- Create: `crates/charpente/src/http/intent.rs`. It holds the pure logic and its unit
  tests.
- Modify: `crates/charpente/src/http/mod.rs` (`mod intent;` and `Request::intent`).
- Modify: `crates/charpente/src/http/router.rs:39-74`, where `Route` gains
  `pub intent: Option<&'static str>`.
- Modify: every hand-built `Route { … }` literal, each adding `intent: None`:
  - `router.rs:289`
  - `serve.rs:1586, 1664, 1679, 1826, 1916, 1931`
  - `kernel/tail.rs:370, 385`
- Modify: `crates/charpente-macros/src/route.rs`. Both emitted `Route` literals (HTTP at
  :526, SSE at :892) add `intent: None`, for now.
- Modify: `crates/charpente/src/http/app.rs`:
  - `stage_or_subscribe` (:1189), in its Http arm only;
  - mint `let access = group.intent_access();` beside `cap` (:1221).
- Test: `crates/charpente/tests/intents.rs` (new).
- Modify: `CLAUDE.md`. Add `intents` to charpente's test list, between `http` and `kernel`.
  `doc_inventory` requires this.
- Docs: GUIDE §5 gains a new subsection, "### Retry-safe writes":
  - the contract and `Request::intent`;
  - the replay header, and the 409 and 422 texts;
  - writes that change nothing record nothing;
  - a kept reply lives `INTENT_RETENTION`, so a route that returns a secret it stores only
    hashed must mint it through `ctx.create_session` or `ctx.create_api_key`.
- Docs: DESIGN, in the request path and the platform tables. PLAN, an after-launch row:
  "refuse a GET handler that writes", with its trigger. CHANGELOG.

**Interfaces:**
- Consumes: Task 2's `IntentRow`, `IntentReply`, `IntentAccess`, `Tx::get_intent`,
  `Tx::put_intent` and `Tx::has_effects`.
- Produces (`http/intent.rs`, crate-private):
  ```rust
  pub(crate) const DEFAULT_HEADER: &str = "Idempotency-Key";
  pub(crate) const KEPT_REPLY_MAX: usize = 65_536;
  pub(crate) enum Presented { Absent, Valid(String), Malformed }
  pub(crate) fn header_name(route: &Route) -> &'static str;      // route.intent, else DEFAULT_HEADER
  pub(crate) fn is_write(route: &Route) -> bool;                  // RouteGlue::Http and method != Get
  pub(crate) fn presented(request: &Request, route: &Route) -> Presented; // reads header_name lowercased
  pub(crate) fn row_id(principal: Option<&str>, route: &str, id: &str) -> Id<IntentRow>;
  pub(crate) fn fingerprint(request: &Request) -> [u8; 32];       // SHA-256 over method, path, sorted query, body, length-prefixed
  pub(crate) enum Decision { Fresh, Answer(Response) }
  pub(crate) fn decide(row: Option<&IntentRow>, header: &str, id: &str, principal: Option<&str>, route: &str, fingerprint: &[u8; 32]) -> Decision;
  pub(crate) fn record(header: &str, id: &str, principal: Option<&str>, route: &str, fingerprint: [u8; 32], committed_at: WallTime, reply: &Response, minted: bool) -> IntentRow;
  pub(crate) fn malformed(route: &Route) -> Response;             // the 400 copy
  ```
- Produces (`http/mod.rs`):
  `pub fn intent(self, id: impl Into<String>) -> Request { self.header("Idempotency-Key", id) }`
- The flow in `stage_or_subscribe`'s Http arm:
  - **Before the `fresh_token` draw:** compute `presented`. On `Malformed` for a write route,
    return `malformed(route)` without drawing anything.
  - **Inside the staged closure, after `build_ctx`, for a write route with `Valid(id)`:**
    - The principal is `ctx.principal().map(Principal::subject)` and the route string is
      `"{method} {pattern}"`.
    - Call `tx.get_intent(row_id(..), &access)` and then `decide`.
    - `Answer(resp)` returns `Ok(resp)` without running the glue.
    - `Fresh` runs the glue. On `Ok(resp)` with `tx.has_effects()`, it calls
      `tx.put_intent(&record(.., now, &resp, ctx.created.get()), &access)`.
  - `committed_at` is `stage_or_subscribe`'s `now` parameter, never `tx.now()`, because
    `App::handle` has no time.

- [ ] **Step 1: Write the failing integration test.** It is red-first.
  `crates/charpente/tests/intents.rs` defines its own fixture app with these routes:
  - `#[table] Hit`;
  - `POST /hits/:id` puts a `Hit` and returns a count of all hits;
  - `POST /noop` reads and returns without writing;
  - `POST /login` calls `ctx.create_session`;
  - `POST /big` writes and returns a 70_000-byte body;
  - `POST /fail` returns `Err(ApiError::BadRequest(..))` after a write;
  - `GET /hits`;
  - an authed `POST /mine/:id`.
  ```rust
  #[test] fn a_resent_write_answers_its_first_reply_and_applies_once() {
      // handle_at(POST /hits/1 .intent("k1"), t) twice:
      assert_eq!((second.status, &second.body), (first.status, &first.body));
      assert_eq!(second.headers.get("idempotent-replayed").map(String::as_str), Some("true"));
      assert!(first.headers.get("idempotent-replayed").is_none());
      // exactly one Hit row
  }
  ```
- [ ] **Step 2: Run the test.** Run `cargo test -p charpente --test intents --no-fail-fast`.
  Expected: it compiles once `Request::intent` exists as a stub, and FAILS with two Hit rows.
  Save that output.
- [ ] **Step 3: Write the remaining tests in `tests/intents.rs`.** Each asserts the exact copy
  from Global Constraints.
  ```rust
  #[test] fn the_same_id_with_another_body_is_refused_with_422()          // 422 text, still one Hit
  #[test] fn a_write_that_changed_nothing_records_nothing()               // /noop twice: both run;
      // app.db().snapshot()? scans IntentRow → 0 rows
  #[test] fn a_failed_write_records_nothing_and_its_resend_runs_again()   // /fail twice: both 400 app error,
      // neither carries idempotent-replayed
  #[test] fn a_resent_login_is_refused_and_its_token_never_reaches_disk() // 409 credential text; the first
      // reply's raw token appears in no row of any table (walk db.table_names(), raw bytes)
  #[test] fn a_reply_over_64_kib_is_answered_409_on_a_resend()            // 409 too-large text with n
  #[test] fn one_intent_id_under_two_principals_is_two_intents()          // two API keys, same id → two rows applied
  #[test] fn a_repeat_in_the_same_group_answers_from_its_siblings_record()// begin_group, stage both, commit ok:
      // second replayed, one Hit
  #[test] fn a_repeat_in_the_same_group_fails_with_its_first_copy()       // same, commit fails (inject as
      // tests/group_commit.rs does): CommitFailure::invalidates is true for BOTH
  #[test] fn handle_without_time_records_committed_at_as_the_epoch()      // App::handle with an id: no panic,
      // row.committed_at == WallTime::UNIX_EPOCH
  #[test] fn a_get_route_ignores_an_idempotency_key()                     // GET /hits .intent("k"): no row
  #[test] fn a_malformed_key_is_refused_with_400_from_any_source()        // 256 bytes; a space; 0x7f → 400 text
  ```
- [ ] **Step 4: Write the unit tests in `http/intent.rs`.**
  - `presented` accepts the boundaries: 1 byte, 255 bytes, 0x21, 0x7e.
  - `presented` rejects 0 bytes, 256 bytes, 0x20 and 0x7f.
  - `row_id` differs when only the principal, only the route, or only the id differs.
  - `row_id` differs for `("ab","c")` against `("a","bc")`, which proves the length prefix.
  - `decide` covers:
    - a matching row → replay, with headers and body intact and the marker added;
    - a fingerprint mismatch → 422;
    - `Credential` → 409;
    - `TooLarge` → 409 with n;
    - an identity mismatch at the same id → the collision 409.
  - `record` withholds on `minted` and on a reply of 65_537 bytes, and keeps one of 65_536.
- [ ] **Step 5: Implement** the Interfaces, the new `Route` field and literals, and the
  `stage_or_subscribe` flow.
- [ ] **Step 6: Run the tests.** Run
  `cargo test -p charpente --test intents --test http --test transactions --test group_commit --no-fail-fast`
  and `cargo test -p charpente --lib http::intent`. Expected: all green, counted.
- [ ] **Step 7: Run the gate.** Packages: `charpente`, `charpente-macros`, and
  `guide-examples` (because of the GUIDE edit and `doc_inventory`).
- [ ] **Step 8: Commit** with "http: a write's intent id is looked up and recorded in its own
  transaction; a repeat is answered from the record".

---

### Task 4: Drive stamps writes, and the scaffold, demo and README follow

This task flips behaviour. Once `Drive` stamps, every Drive-driven re-send dedupes, which
inverts the scaffold's retry-guard pin and turns the double-charge demo green. Everything
that pins the old behaviour moves in this commit.

**Files:**
- Modify: `crates/charpente-sim/src/lib.rs`. Add `pub(crate) fn next_intent(&self) -> String`
  on `Sim`. It is a universe-wide counter in `State`, and the id is the hex of 16 bytes from
  `keyed_stream(self.seed(), &format!("intent:drive:{n}"))`.
- Modify: `crates/charpente-sim/src/drive.rs:169-213` (`faulted_phase`):
  - Stamp once, before :186. Both :187 (`phase.requests`) and :196 (`push_at`) use the
    stamped copy.
  - Stamp only a request whose method is not `Get` and that has no `idempotency-key`.
  - Stamp before `boot()`, so the early return at :190 still hands back stamped requests.
- Test: `crates/charpente-sim/tests/drive.rs`.
- Modify: `crates/charpente-cli/templates/lib.rs:49-75`. The guard stays. Its doc comment
  now says the platform answers a re-send, and the guard holds a note to one body.
- Modify: `crates/charpente-cli/templates/sim.rs.tmpl`:
  - No message may blame a re-sent POST for a duplicate the platform now prevents. That
    covers the duplicate-Echo text (:198), the duplicate-event text (:247) and the header
    doc (:23).
  - The fourth check's "resent" half may now need a same-body POST under a new intent; see
    Step 6.
- Modify: `crates/charpente-cli/tests/new.rs:575-674`.
- Modify: `crates/charpente-cli/tests/demo.rs`, `examples/double-charge/README.md` and
  `README.md` ("Example: a double charge").
- Docs: GUIDE §8. The re-send prose (around :3770-3791) and "Driving the whole app in a sim"
  say three things:
  - `Drive` stamps each write with an `Idempotency-Key`, and its re-send carries the same
    key.
  - A hand-built re-send needs `Request::intent` with the first copy's id to be recognised.
  - A workload driving a webhook route sets that route's declared header, as its sender
    would.
- Docs: CHANGELOG.

**Interfaces:**
- Consumes: Task 3's lookup. The header name is the literal `"idempotency-key"`.
- Produces: `Sim::next_intent`, crate-private.

- [ ] **Step 1: Write the red-first property** in `crates/charpente-sim/tests/drive.rs`.
  - Name it `a_resent_write_applies_once_under_drive`, and use the file's existing in-file
    app pattern.
  - The app: a `POST /tally/:n` route increments one counter row and stores `n` in a `Seen`
    row, using client-chosen ids.
  - The phase sends 12 POSTs under the swarm fault matrix. Then `recover` runs.
  - Assert that the counter equals the number of distinct requests answered 200. Write the
    failure message in the house form: "tally is {got} but {want} distinct writes landed:
    a re-sent POST /tally/{n} applied twice".
- [ ] **Step 2: Run it on the current tree.** Run
  `cargo test -p charpente-sim --test drive a_resent_write_applies_once -- --nocapture`.
  Expected: FAIL, printing a `CHARPENTE_SEED=0x…` repro line. Record that seed.
- [ ] **Step 3: Implement** `Sim::next_intent` and the stamping.
- [ ] **Step 4: Run it again.** Expected: green over the batch. Then add a test that replays
  the recorded seed alone:
  - name it `the_universe_that_double_applied_before_stamping_applies_once`;
  - its doc comment names the seed and the failure Step 2 printed;
  - it asserts the same tally equality.
- [ ] **Step 5: Run the workspace suite** in the background and triage every failure.
  - **Expected failures:** the scaffold pins in `tests/new.rs` and every red pin in
    `tests/demo.rs`.
  - **Any other failure** is a pinned seed whose universe moved. Find a new seed for the
    same reason, per Global Constraints.
- [ ] **Step 6: Re-pin the scaffold.** Keep the guard.
  - Rename `deleting_the_retry_guard_turns_the_echo_check_red_on_a_resend` to
    `deleting_the_guard_leaves_resends_to_the_platform_but_loses_the_conflict_refusal`.
  - The renamed test asserts exit 1 and a stdout line
    `check acked_post_implies_the_echo_after_recovery ... ok`.
  - Its failure block for `a_resent_note_changes_nothing_and_a_conflict_is_refused` must
    name the conflict, not a duplicate.
  - Run `turning_the_guards_eq_into_ne_turns_the_resend_check_red`:
    - **If it is still red:** re-pin its block's text.
    - **If the batch goes green:** the fourth check must also send a same-body POST of an
      existing note under a fresh intent, so the `!=` mutant is caught. Add that to
      `sim.rs.tmpl` and pin the resulting block.
  - Re-pin `emitting_twice_turns_the_event_check_red_naming_the_route`'s text if the
    message moved.
- [ ] **Step 7: Re-pin the demo** in `tests/demo.rs`. The batch is now green.
  - Rename `the_demo_charges_an_order_twice_and_names_the_fix` to
    `the_demo_answers_each_resent_order_once`. `master_batch` asserts exit 0, and stdout
    carries `sim result: ok.`.
  - Rename `the_demo_goes_red_under_other_master_seeds` to
    `the_demo_stays_green_under_other_master_seeds`, over 0x1 to 0x8, each exit 0.
  - Delete `restoring_the_three_lines_turns_the_demo_green`, along with `GUARD` and `FIX`
    if they are now unused.
  - Re-capture `GOLDEN` as the new stdout. Keep `STARTUP` unless it changed, and say so
    either way.
  - Rename the frame test to `the_demos_output_fits_the_tapes_frame` and the whole-output
    test to `the_demos_output_is_pinned_whole`.
  - Replace `the_demos_repro_line_replays_its_failure` with
    `the_seed_readme_quotes_from_0_6_0_now_passes`. It runs README's 0.6.0 repro command
    (`--filter each_order_is_charged_once --seed 0x82eac71fbdeda9de --inputs 00 --faults-off …`)
    and expects exit 0.
  - `readme_quotes_the_demos_batch_whole` now pins the green block.
- [ ] **Step 8: Update README's section.**
  - Keep the 0.6.0 route, table and both red blocks, labelled "charpente 0.6.0 printed". Its
    present-tense claims of red become "under 0.6.0". These blocks are no longer pinned, so
    adjust `claims_census`'s exemption accordingly.
  - Add one short paragraph: under 0.7.0 `Drive` re-sends each write with the
    `Idempotency-Key` it first carried, the kernel answers from the record without running
    `place_order` again, and the same route with no guard passes.
  - Follow it with the pinned green block, under the command typed.
  - Rewrite `examples/double-charge/README.md`'s three lines to tell the same story.
  - Do NOT reframe the README lead or the video. That is Noah's call (spec §5).
- [ ] **Step 9: Run the workspace suite again.** Expected: green, counted.
- [ ] **Step 10: Run `charpente test --sim --master-seed 0x1`** on sim-fixture
  (`--dir crates/sim-fixture`). Expected: exit 0. Record the count.
- [ ] **Step 11: Commit** with "sim: Drive stamps every write with an intent id, so a re-send
  applies once; the scaffold's guard keeps only write-once; the demo goes green and README
  labels 0.6.0's red run". The message states the failing seed from Step 2 and each re-pinned
  seed.

---

### Task 5: The wire refuses a write without an id, and `intent = header(...)`

**Files:**
- Modify: `crates/charpente/src/http/mod.rs:75-95`. `Request` gains `pub(crate) wire: bool`,
  which `Request::new` sets false. The literal at :95 is the only one.
- Modify: `crates/charpente/src/serve.rs:674`. `to_kernel_request` sets `wire = true`.
- Modify: `crates/charpente/src/http/app.rs`. After the body cap and before the
  `fresh_token` draw:
  `if request.wire && is_write(route) && matches!(presented, Presented::Absent)`
  returns the 400 (`malformed(route)`, the same copy).
- Modify: `crates/charpente-macros/src/route.rs`:
  - `RouteArgs` gains `intent: Option<LitStr>`.
  - A new parser branch `else if flag == "intent"` parses `=`, then the ident `header`,
    then a parenthesised `LitStr`. It follows `public(...)`'s paren handling (:91-99) and
    peeks first, as `emits` does (:124).
  - The `FLAGS` text (:177) adds `` `intent = header("…")` ``.
  - `expand` refuses `intent` on a GET or SSE route.
  - Emit `intent: Some(#lit)` or `None`.
- Create: four cases in `crates/charpente-macros/tests/compile_fail/`, each with its
  `.stderr`:
  - `route_intent_not_header.rs`
  - `route_intent_bad_header.rs`
  - `route_intent_on_get.rs`
  - `route_intent_twice.rs`
- Create: `crates/charpente-macros/tests/compile_pass/route_intent_header.rs`.
- Re-pin: `route_unknown_flag.stderr`.
- Modify: `crates/charpente/examples/api.rs`. Add a webhook route using
  `intent = header("X-Event-Id")`, so `api_census` sees both `intent` and `header`.
- Modify: every client that sends real HTTP writes. Each sends a fresh key per logical write,
  and the SAME key on a re-send of that write:
  - `crates/edge-harness/src/` (its client helper) and its tests;
  - `crates/bench-harness`'s wire layer, if it has one;
  - every `curl -X POST|PUT|PATCH|DELETE` in README.md and docs/GUIDE.md.
  
  Where an edge test re-sends after `kill -9`, check what it asserts about duplicates and
  update it to the new contract.
- Test: `crates/edge-harness/tests/smoke.rs`. Unit tests go in `serve.rs`'s `#[cfg(test)]`
  module, which already builds `Route`s.
- Docs:
  - GUIDE §5's "Retry-safe writes" adds the wire refusal and webhooks.
  - "What #[route] rejects" and the §9 error index gain the new texts.
  - The FLAGS copy at GUIDE :5948 updates.
  - CHANGELOG.

**Interfaces:**
- Consumes: Task 3's `presented`, `is_write`, `malformed` and `header_name`.
- Produces: `Route.intent` is `Some(header)` for a route that declares it.

**Exact macro copy:**
- Not `header(...)`: ``"`intent` takes the header a sender's id arrives in: `intent = header(\"X-Event-Id\")`"``
- An invalid name: ``"`intent = header(\"{name}\")` names no valid header: a header name is letters, digits and !#$%&'*+-.^_`|~"``
- On a read: ``"`intent` is for a write route (POST, PUT, PATCH or DELETE); a GET route carries no intent"``
- Given twice: ``"`intent` given twice — write it once"``

- [ ] **Step 1: Write the failing unit tests in `serve.rs`'s test module.**
  ```rust
  #[test] fn a_wire_write_without_a_key_is_refused_before_any_draw()
  //   wire POST to a write route with no key → 400 with the exact copy naming "POST /x"
  //   and Idempotency-Key; the App's Entropy (wrapped in a counting Entropy) was drawn 0 times
  #[test] fn a_wire_get_needs_no_key()
  #[test] fn an_in_process_write_without_a_key_runs_as_before()
  #[test] fn a_webhook_route_reads_its_declared_header()
  //   X-Event-Id present → recorded and a repeat replays; Idempotency-Key alone on the wire → 400 naming X-Event-Id
  ```
- [ ] **Step 2: Run them.** Expected: FAIL. Then write the trybuild cases and the
  compile-pass case.
- [ ] **Step 3: Implement** the wire flag, the refusal and the macro word.
- [ ] **Step 4: Write the black-box test in `crates/edge-harness/tests/smoke.rs`.**
  - Name it `a_write_without_a_key_is_refused_and_a_resent_key_is_answered_once`.
  - It runs over real TCP against the smoke app's write route.
  - No key → 400 with the copy.
  - A 256-byte key → the same 400.
  - A key with a non-ASCII byte → the same 400.
  - The same valid key twice → equal bodies, the second carries `idempotent-replayed: true`,
    and one effect.
- [ ] **Step 5: Run the tests.** Run trybuild (FULL), `charpente --lib serve`,
  `edge-harness` (all of it, since every wire client changed), and `guide-examples`
  (`api_census`, `doc_truth`).
- [ ] **Step 6: Run the workspace suite** in the background. Expected: green, counted.
- [ ] **Step 7: Commit** with "serve: a write that reaches the wire without an intent id is
  refused by name; #[route(intent = header(...))] keys a webhook by its sender's id".

---

### Task 6: `describe` names each write route's intent header

**Files:**
- Modify: `crates/charpente/src/describe.rs`:
  - `RouteInfo` (:82-114) gains `pub intent: Option<&'static str>` after `stream`;
  - `route_info` (:334) sets it to `Some(header_name(route))` for a write route and `None`
    otherwise.
- Modify: `crates/charpente-cli/src/genclient.rs`:
  - the mirror `RouteInfo` (:283) and `into_route` (:479);
  - `EVERY_SHAPE` (:638).
- Modify: charpente-tsgen's route type, which gains `intent: Option<String>`, and every
  `RouteInfo { … }` literal in its tests (`tests/common/mod.rs`, `tests/routes.rs`,
  `tests/revive.rs`).
- Re-pin these goldens:
  - `examples/invoicing/tests/describe_golden.rs` `GOLDEN`
  - `crates/charpente-cli/tests/describe.rs` `GOLDEN`
  - `crates/charpente/tests/describe.rs` `GOLDEN`
  - GUIDE §13's JSON fence (`describe_contract` diffs it against a fresh build)
- Docs: GUIDE §13 prose, and CHANGELOG ("`describe` gains each route's `intent`").

**Interfaces:**
- Consumes: Task 3's `header_name`, `is_write` and `Route.intent` (which Task 5's macro word fills).
- Produces: the JSON field `"intent"` (a string or `null`), and the tsgen route's
  `intent: Option<String>`, which Task 8 reads.

- [ ] **Step 1: Write the failing test in `crates/charpente/tests/describe.rs`.**
  ```rust
  #[test] fn describe_names_the_intent_header_of_each_write_route()
  //   a POST route → "intent": "Idempotency-Key"; a GET → null; an SSE → null;
  //   a route with intent = header("X-Event-Id") → "intent": "X-Event-Id"
  ```
- [ ] **Step 2: Run it.** Expected: FAIL. Then implement.
- [ ] **Step 3: Regenerate each golden** from a real run. Diff old against new: the ONLY
  change allowed is the added `intent` lines.
- [ ] **Step 4: Run the gate.** Packages: `charpente` (`--test describe`), `charpente-cli`
  (`--test describe`, `--test genclient`), `invoicing`, `charpente-tsgen` and
  `guide-examples` (`describe_contract`).
- [ ] **Step 5: Commit** with "describe: each write route names the header its intent id
  arrives in".

---

### Task 7: Retention, and the kernel's intent sweep

**Files:**
- Modify: `crates/charpente/src/kernel/mod.rs`:
  - `Kernel` gains `opened_at: WallTime` and `next_intent_sweep: Option<WallTime>`. Both
    are set where the open reading is taken: at build (:341-343, when writable) and at
    every non-writable-to-writable edge (:1287-1289). There,
    `next_intent_sweep = Some(wall + INTENT_RETENTION)`.
  - A new `Ready::IntentSweep` variant is pushed LAST, after the outbound source (:619-621).
    It is gated on `writable` and `now.wall >= next`.
  - Its deadline joins the idle deadline array (:635-654) the way cron's does.
  - Its arm copies the Cron arm's commit-then-bubble shape (:1008-1030), using
    `begin_job_group(now.wall)` and `intent_access()`. It deletes
    `intents_committed_before(now.wall - INTENT_RETENTION, INTENT_SWEEP_BATCH)`.
  - After a full batch the sweep is due again at once. Otherwise it is due at
    `now.wall + INTENT_SWEEP_INTERVAL`.
- Constants, in `kernel/mod.rs`: `INTENT_SWEEP_INTERVAL: Duration = 1 h` and
  `INTENT_SWEEP_BATCH: usize = 4096`.
- Test: `crates/charpente/tests/intents.rs`. These tests run the kernel under charpente-sim's
  `Sim`, the way `tests/kernel.rs` drives a kernel with a `SimSource`.
- Docs: GUIDE §5, a retention paragraph. It covers the 24 h past the later of commit and
  open, and states that a re-send after its row is swept runs again. DESIGN. CHANGELOG.

**Interfaces:**
- Consumes: Task 2's `INTENT_RETENTION`, `Tx::intents_committed_before` and
  `Tx::delete_intent`.
- Produces: none for later tasks.

- [ ] **Step 1: Write the failing tests.**
  ```rust
  #[test] fn a_resend_after_a_three_day_power_cycle_is_answered_from_its_record()
  //   commit POST /hits/1 .intent("k") at T; power-cycle with the wall clock moved to T + 3 days;
  //   re-send → idempotent-replayed: true, one Hit
  #[test] fn the_sweep_waits_a_full_retention_after_open_then_drops_older_records()
  //   open at T0, commit at T0; at T0 + 24h − 1min the row is present; at T0 + 24h + 1h it is gone,
  //   and a re-send then runs again (200, no replayed marker, two Hits) — the documented limit
  #[test] fn a_full_batch_is_swept_again_without_waiting_an_interval()
  //   4097 old rows: after two sweep steps at the same wall time, none remain
  ```
- [ ] **Step 2: Run them.** Run `cargo test -p charpente --test intents --no-fail-fast`.
  Expected: FAIL on the sweep tests. The power-cycle test may already pass, which is
  expected: the lookup ignores age. Leave it as a pin.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Prove the two operands.** Mutate `opened_at + INTENT_RETENTION` to
  `opened_at`, and then separately `now.wall - INTENT_RETENTION` to `now.wall`. Each
  mutation must turn a named test red. Restore each with `git checkout`, never `mv`.
- [ ] **Step 5: Run the gate.** Run `charpente` (`--test intents`, `--test kernel`,
  `--test replication`, `--test clock`), counted.
- [ ] **Step 6: Commit** with "kernel: an intent record lives 24 h past the later of its commit
  and the node's open; a kernel-run sweep drops older ones".

---

### Task 8: The generated TS client sends an intent on every write

**Files:**
- Modify: `crates/charpente-tsgen/src/runtime.rs`:
  - the `RUNTIME` string gains
    `export function newIntent(): string` (16 bytes from `crypto.getRandomValues`,
    lowercase hex, 32 characters);
  - `request()` (:230) gains a trailing `intent?: [string, string]` and sets that header;
  - `RESERVED` (:29-41) gains `newIntent`, sorted.
- Modify: `crates/charpente-tsgen/src/routes.rs`:
  - `emit_methods` (:184) gives a route with `intent: Some(h)` a trailing
    `intent?: string` parameter, and calls
    `this.request<…>(method, path, body?, [h.to_lowercase(), intent ?? newIntent()])`;
  - `param_args` (:98) refuses a path param named `intent` the way it refuses `body`.
- Test: `crates/charpente-tsgen/tests/routes.rs` and `tests/revive.rs`, plus every
  `crates/charpente-cli/tests/genclient.rs` needle that moves.
- Regenerate: `ts-fixture/src/generated/client.ts` with
  `cargo run -p charpente-cli -- gen-client --dir crates/ts-fixture-app --out ts-fixture/src/generated/client.ts`.
- Modify: `ts-fixture/testing/fetch-stub.ts`, where `RecordedCall` gains `headers`.
- Create: `ts-fixture/src/intent.test.ts`.
- Docs: GUIDE §14 shows the retry pattern
  (`const intent = newIntent(); … api.createCustomer(body, intent)` again with the same
  `intent`) and the quoted signatures. CHANGELOG.

**Interfaces:**
- Consumes: Task 6's tsgen route `intent: Option<String>`.

- [ ] **Step 1: Write the failing Rust tests in `tests/routes.rs`.**
  ```rust
  #[test] fn a_write_route_takes_an_optional_intent_and_sends_it()
  //   contains `async createCustomer(body: CustomerInput, intent?: string)`
  //   and `this.request<CustomerView, ApiError>("POST", "/customers", body, ["idempotency-key", intent ?? newIntent()])`
  #[test] fn a_get_route_takes_no_intent()
  #[test] fn a_webhook_route_sends_its_declared_header()   // ["x-event-id", intent ?? newIntent()]
  #[test] fn a_path_param_named_intent_is_refused()
  ```
- [ ] **Step 2: Run them.** Expected: FAIL. Then implement, and re-pin the moved needles:
  `a_pattern_with_no_params_stays_a_plain_string_literal`, `a_bodyless_route_passes_no_body_argument`
  (`deleteWidget` still has no `body`) and revive's one-line return.
- [ ] **Step 3: Regenerate `client.ts`.** Then run
  `charpente check-client --dir crates/ts-fixture-app --out ts-fixture/src/generated/client.ts`.
  Expected: exit 0.
- [ ] **Step 4: Write `intent.test.ts`.**
  - A write sends `idempotency-key` matching `/^[0-9a-f]{32}$/`.
  - Two calls without `intent` send different keys.
  - Two calls with the same `intent` send that exact key.
  - A GET sends no key.
- [ ] **Step 5: Run the TS checks.** In `ts-fixture/`, run `npm ci`, then the pinned binaries
  through `npm run check` and `npm run test`. A fresh worktree's `npx` resolves a registry
  stub that type-checks nothing. Run `cargo test -p ts-fixture-app --no-fail-fast`.
  Expected: all green.
- [ ] **Step 6: Run the gate.** Packages: `charpente-tsgen`, `charpente-cli`
  (`--test genclient`) and `ts-fixture-app`.
- [ ] **Step 7: Commit** with "tsgen: every generated write sends an Idempotency-Key, a fresh
  one unless the caller passes the intent to retry".

---

### Task 9: Evidence, then the records

**Files:**
- Modify: `docs/LEDGER.md` (the entry), the spec's status line ("built"),
  `.superpowers/sdd/2026-11-22-twin/progress.md`, and the memory `charpente-twin-oracle.md`.

- [ ] **Step 1: Run the full CI trio over the workspace** (backgrounded). Expected: every
  suite reports `test result: ok`. State the count.
- [ ] **Step 2: Run the app batches**, each under one fresh master seed printed in the
  ledger, with `--runs 500`:
  - sim-fixture;
  - `examples/invoicing`;
  - outbound-fixture;
  - a fresh `charpente new` scaffold in the scratchpad;
  - the demo's patched copy (through `demo.rs`'s helper, or a by-hand copy of it).
  
  Expected: exit 0 on each. State each run count.
- [ ] **Step 3: Run the mutation instrument** on the new pure module:
  `scripts/mutants.sh --file crates/charpente/src/http/intent.rs --test-package charpente`.
  - First validate the scope against `cargo mutants --list`.
  - Expected: no MISSED and no TIMEOUT. A MISSED mutant is a property to write, not an
    exclusion to add.
- [ ] **Step 4: Run `edge-harness` and the `ts-fixture` npm checks** once more on the final
  tree.
- [ ] **Step 5: Write the records.** The ledger entry names the counts, the master seeds,
  the mutation result and the two items left for Noah:
  - whether README's lead should change;
  - the demo's `Cargo.lock` refresh at the publish.
- [ ] **Step 6: Commit the records.** Then fast-forward `main` with
  `git merge --ff-only retry-safe-writes`. If the auto-mode classifier refuses the merge,
  hand Noah the `!` command.
