# Payload Shapes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Every `#[event]` and `#[job]` payload records its structural fingerprint in the log. Nothing decodes a payload under a shape other than the one it was written with, no delivery path stops the node, and a stopped kernel ends the process. This is M3 pre-publish fix C-1.

**Architecture:**
- **The shape tag.** Two new log tags, `0x08` and `0x09`, carry the payload type's `FINGERPRINT` beside every enqueue and emit. Apply copies it into a trailing `shape: Option<u64>` on `JobRow` and `EventRow`. `None` means a pre-shape row, and it counts as foreign.
- **Events.** Delivery compares `row.shape` against the subscription entry's fingerprint *before* decoding. A mismatch ends that one tail with a terminal `charpente.reshaped` frame, and a cursor sitting on such a row gets a named 409. Corrupt bytes and unencodable payloads end the tail with `charpente.error`. The node never stops.
- **Jobs.** The kernel refuses to dispatch while a Pending row of a mounted job type has a foreign shape, and says why. It checks at startup and at every moment its write gate opens: a witness gate opening, or a completed switchover. A `promote` is refused before its role flips. Dispatch then *asserts* the invariant.
- **Process exit.** `serve`'s kernel thread becomes joinable through `Server::wait`. A stop ends the process nonzero, with a stderr line that tells a refusal from an IO failure.

**Tech Stack:**
- Rust: `charpente-core` codec and log records, and `syn`/`quote` proc macros.
- The deterministic simulator (`run_universes_swarm_with!`).
- The generated TypeScript client runtime, tested with `node --test`.
- The stock-binary edge harness.
- `cargo-mutants` 27.1.0, run through `scripts/mutants.sh`.

**Spec:** `docs/superpowers/specs/2026-09-24-payload-shapes-design.md` (commit `880bded`, approved by Noah). Read it first; the plan argues from it. Where this plan differs, the difference is listed under "Spec amendments" below, and Task 1 writes those into the spec.

**Working directory for mid-flight state:** `.superpowers/sdd/2026-09-24-payload-shapes/` (gitignored). Put `progress.md` there, one gate log per task (`gate-task-N.log`), mutation outputs, and every reviewer's report. **A reviewer writes its full report to a file there before returning a summary**, because deliveries have been lost in flight before.

## Spec amendments (Task 1 appends these to the spec, verbatim, under a new "## Amendments (implementation plan)" heading)

- **A1 — the jobs check runs wherever the write gate opens, and a promote checks before its role flips.** "Beside the schema gate in `do_promote`" would run it after `store_role(Primary)`, leaving a durable primary that refuses on every restart. It runs before `clear_stale_witnesses` and answers `CtrlErr`, so the operator hears it on the command and nothing durable changes. The kernel checks in `after_repl_step` whenever the write gate opens: a witness gate opening, or a switchover's target becoming primary.
  - **A switchover gets no target-side pre-check.** DESIGN §5 makes HandedOff the point of no return: once the primary sends `HandOff`, it never aborts. A target refusing at that point would leave the primary write-stopped and the target a follower, and neither message would name a fix that works. So the hand-off completes, and the new primary's kernel refuses at its writable flip. That is the startup refusal, and its fix ("run the previous binary until they are Done or Dead") works from that state: the new primary on the old binary dispatches them.
  - `repl.step` and `after_repl_step` run back to back inside one `Ready::Repl` arm, so no dispatch can come between the flip and the check.
- **A2 — the encoder is structural, not asserted.** `TxEffects` carries a bare `u64` shape per enqueue and emit, so a shapeless one cannot be represented. `encode_record` writes only `0x08`/`0x09`, and there is nothing left to assert.
- **A3 — `PROTO_VERSION` 2 → 3.** A version-2 follower would refuse `0x08` at apply by name; the bump refuses it at `Hello`, before any frame ships.
- **A4 — the job-shape registry lives in `Schema`.** `Db::register_job_shapes` writes it and `Db::pending_job_shape_refusal` reads it. It sits in `Schema` rather than in the `Db` or the dispatcher for two reasons:
  - a snapshot re-seed rebuilds the `Db` from its schema (`into_seeder` → `Seeder::finish` → `Db::open`), and a registry held anywhere else would come back empty;
  - `do_promote` holds the `Db`, not the dispatcher.

  It is excluded from `schema_digest` (spec §8: job shapes are not in `Hello`).
- **A5 — one refusal parenthesis.** Every refusal text reads `(recorded X; this binary Y)`, the no-shape case included. The job refusal's wording and its five-id elision are fixed in Task 6.
- **A6 — where the TypeScript runtime test lives.** It goes in `ts-fixture/src/client-runtime.test.ts` (`node --test`), because the new frames are runtime behaviour. `ts-fixture-app`'s drift test covers type shapes, not frames.
- **A7 — `charpente.error` has no `id:` line on the wire.** The encoder omits that one line; `EventFrame.id` still holds the row id for sim tests.
- **A8 — a `Refusal` marker.** `charpente_core::db::Refusal`, carried inside `io::Error::other`, is how the kernel's stderr line tells a refusal from a platform IO failure. Every existing `io::Result` path is untouched.
- **A9 — the pinned `charpente.reshaped` block holds its `event:` and `data:` lines only.** `doc_truth` refuses a pinned line with no 16-character fragment outside placeholders, and an `id: <id>.<seq>` line is nothing else. The prose around the block states the `id:` line.
- **A10 — (verified in Task 3) what serde_json cannot encode.** The spec's example of an unencodable payload, a `NaN`, is believed wrong: serde_json writes non-finite floats as `null`, and the case that fails is a map with non-string keys. Task 3 runs the experiment and writes GUIDE/DESIGN from what it shows.
- **A11 — a corrupt `EventRow` fails its tail too.** This reads spec §1's "a corrupt event row" literally: a row whose own bytes fail to decode (`InvalidData` out of the scan) gets `charpente.error`. redb's own failures (`ErrorKind::Other`) still stop the node, because they are the platform's, not a subscriber's.
- **A12 — four new pinned blocks.** The 409, the reshaped frame, the error frame and the job refusal take `PINNED_FLOOR` from 16 to 20.
- **A13 — the stock-binary startup-exit test uses a corrupt role file.** A jobs refusal needs two builds of one app, which the edge harness does not have; a corrupt role file ends the same kernel thread at startup through `serve_replicated`.

## Global Constraints

- **Commits.**
  - Commit directly on `main` (CLAUDE.md "Workflow").
  - **No `Co-Authored-By` trailers.** CLAUDE.md is the user's instruction and it overrides the harness's attribution reminder.
  - Before each commit, re-check `git status` and `git log -1`: another session may be writing this tree. Stage named paths only, never `git add -A`.
- **Every commit green.** Tasks are ordered so no commit leaves the workspace red. Task 1 changes the record format and fixes every compile site in the same commit.
- **The gate is three commands, counted:**
  1. `cargo fmt --all --check`
  2. `cargo clippy --workspace --all-targets -- -D warnings`
  3. `cargo test --workspace --no-fail-fast 2>&1 | tee <log>`

  Green means `grep -c "^test result:" <log>` equals `grep -c "^test result: ok" <log>`, and `grep -c FAILED <log>` is 0. Never infer green from the absence of `FAILED`. The baseline at `c28250e` is **87 of 87**, and each task states its new count.
- **Two markers are filled at execution, never guessed.** `2026-09-2x` is the date of the commit that lands the text. `N` in a universe count or a commit message is the value measured in that task's timing or gate step.
- **The full suite exceeds the 600 s tool ceiling.**
  - **Implementers** run the per-package, per-file commands each task lists, in the foreground.
  - **The controller** runs the full workspace gate in the background (`run_in_background`) and polls the log file, never a pipe through `tail`, before it marks a task done.
  - A subagent that backgrounds `cargo test` can park forever, so dispatches must say "foreground only".
- **zsh, not bash.** `${PIPESTATUS[0]}` is empty here; use `${pipestatus[1]}`. An unquoted `$var` does not word-split. `timeout` does not exist on this machine; bound a run with perl `alarm` if needed.
- **Error text is product surface (hard rule 5).** Every message below is fixed wording, pinned byte-exact by a test that spells it out independently of the product's `format!`. Do not "improve" a message without changing its pin and its GUIDE quote in the same commit.
- **Docs in the same commit as the behaviour (hard rule 6).**
  - `docs/GUIDE.md` ```rust blocks are compiled by `crates/guide-examples/build.rs`; check them alone with `cargo build -p guide-examples`.
  - A ` ```text` block preceded by `<!-- pinned -->` is checked by `crates/guide-examples/tests/doc_truth.rs`. **Every non-blank pinned line must contain a fragment of 16 or more characters outside lowercase `<placeholder>`s**, and every such fragment must appear in some `.rs` under `crates/` or `examples/`. The check applies Rust's literal rules: a `\` + newline continuation vanishes, `\"` becomes `"`, and `\n` becomes a newline.
  - Comment lines are not part of that corpus. A pinning test's literal is, on purpose.
  - `PINNED_FLOOR` is a floor, raised in the same commit as each new pinned block.
- **Red before green (spec §6, hard rule 3).** Every behavioural test is run and seen failing *before* its implementation lands, and the red output is quoted in the task report. A compile error is not a behavioural red. For a test that needs new API, write it against the pre-change behaviour where the spec names that behaviour (Tasks 2 and 3 do this), or prove it red by mutation after the fact (Tasks 4, 6 and 8).
- **The shape tag renames sim universes.** Every enqueue and emit record grows by 8 bytes, which moves sector boundaries (`disk.rs::tear` draws once per sector) and segment rollovers. Any universe whose workload enqueues or emits can therefore become a different universe under the same seed. Task 1 carries the census. The doctrine, from memory `pinned-seed-names-a-universe`:
  - re-find a seed that reproduces the named scenario, assert the reason, and record old → new;
  - never loosen an oracle to absorb the move.
- **Sim failures are findings.** A property failure is a finding: stop, pin the seed, and adjudicate before weakening anything. A timeout is not a catch, and a green replay of one seed proves nothing about a rate or a floor.
- **Mutation runs.**
  - Commit before any mutation run.
  - Never share `CARGO_TARGET_DIR` across worktrees.
  - `charpente test --mutants` judges mutants with the core+sim suites unless `--test-package <crate>` is given, whatever `--file` says.
  - Validate each `--re` against `cargo mutants --list` for its function first.
  - `exclude_re` is a substring match.

## Review Focus

These are the inputs the spec implies but does not spell out that are most likely to bite someone using this. Each one's test is added to the task that owns the code.

1. **A keyed subscription must not end on another key's foreign row.** Another tenant's reshaped `incident_opened` must not end *my* tail, and a cursor sitting on another key's foreign row must not be refused. This needs the key filter to run before the shape check, on both paths. Owner: Task 2, `another_keys_reshaped_event_neither_ends_nor_refuses_this_tail`.
2. **A subscription carrying several names ends exactly at the first foreign row.** Earlier rows of the other name are delivered and later ones are not. Owner: Task 2, the probe test `a_reshaped_event_ends_its_tail_by_name_and_the_node_keeps_serving`.
3. **A switchover onto a follower holding foreign Pending jobs must never strand the pair with no writable node and no named way out**, and must never dispatch one of those jobs. This is the ordinary rolling deploy: upgrade the follower, switch over, upgrade the old primary. It goes wrong with the obvious placement of the check, a target-side hand-off refusal: HandedOff is DESIGN §5's point of no return, so the primary would stay write-stopped (amendment A1). The switchover must complete, and the new primary's kernel must refuse at its write-gate flip, before it dispatches. Owners, both in Task 6: `a_switchover_onto_foreign_pending_jobs_completes_and_the_new_primary_names_them` and `a_write_gate_opening_over_foreign_pending_jobs_stops_the_kernel_by_name`.
4. **A snapshot re-seed must not empty the job-shape registry**, so a promote after one still refuses. Owner: Task 6, `a_promote_after_a_snapshot_reseed_still_refuses_foreign_pending_jobs`.
5. **A stored `EventRow` whose own bytes are corrupt fails its tail, not the node.** This is envelope corruption, not payload corruption. Owner: Task 3, `a_corrupt_event_row_fails_its_tail_not_the_node`.

## File map

| File | Responsibility | Tasks |
|---|---|---|
| `crates/charpente-core/src/db.rs` | `Job`/`Event::FINGERPRINT`, tags `0x08`/`0x09`, `Effect.shape`, `Schema.job_shapes`, `Db::register_job_shapes`, `Db::pending_job_shape_refusal`, `Refusal` | 1, 6, 7 |
| `crates/charpente-core/src/{event,job,table}.rs` | trailing `shape` on the rows; `materialize_*` take it | 1 |
| `crates/charpente-macros/src/{event,job,fingerprint}.rs` | emit `FINGERPRINT`; `JobDef.fingerprint`; corruption-only decode message | 1, 6 |
| `crates/charpente/src/http/mod.rs` | `SubEntry.fingerprint`; `ev_json` trailing check and messages | 2, 3 |
| `crates/charpente/src/kernel/tail.rs` | `RESHAPED_FRAME`/`ERROR_FRAME`; `deliver_unit` never errs on subscriber data | 2, 3 |
| `crates/charpente/src/kernel/mod.rs` | `validate_subscribe(&Subscription)` 409; the jobs check at build and at the writable flip | 2, 6 |
| `crates/charpente/src/kernel/dispatch.rs` | `JobDef.fingerprint`; the dispatch assert | 6 |
| `crates/charpente/src/repl/mod.rs` | `PROTO_VERSION` 3; the promote check; refusal sites marked | 1, 6, 7 |
| `crates/charpente/src/serve.rs` | `encode_frame` without `id:` for errors; `Server::wait`; stop lines | 2, 3, 7 |
| `crates/charpente/src/launch.rs`, `examples/invoicing/src/bin/{node,tenant_node}.rs`, `crates/bench-harness/src/bin/bench-app.rs` | exit with `wait()`'s code instead of parking | 7 |
| `crates/charpente-tsgen/src/runtime.rs` + `ts-fixture/src/generated/client.ts` | `StreamEnd` `reshaped`/`error` | 5 |
| `crates/edge-harness/src/child.rs`, `tests/{failover,smoke}.rs` | `wait_exit`; Phase B asserts the exit; a startup-exit test | 7 |
| tests: `charpente/tests/{realtime,kernel,replication,transactions,http}.rs`, `charpente-core/tests/{materialize,engine}.rs`, `charpente-tsgen/tests/routes.rs`, `ts-fixture/src/client-runtime.test.ts`, `guide-examples/tests/doc_truth.rs` | oracles | all |

No new test files, so CLAUDE.md's exhaustive test list and `doc_inventory` stay as they are.

---

### Task 1: The shape tag — every emit and enqueue records its payload's fingerprint

**Files:**
- Modify `crates/charpente-core/src/db.rs`:
  - module doc (lines 8–30);
  - `Job`/`Event` traits (~87–98);
  - `enum Effect` (~349);
  - `TxEffects` (~418);
  - `encode_record` (~441) and `decode_record` (~519);
  - `Tx::enqueue`/`enqueue_keyed`/`emit`/`emit_keyed` (~720–756);
  - the apply arm (~1628);
  - unit tests (~3430–3570).
- Modify `crates/charpente-core/src/table.rs`: `materialize_job` (~1235) and `materialize_event` (~1268).
- Modify `crates/charpente-core/src/event.rs` and `crates/charpente-core/src/job.rs`.
- Modify `crates/charpente-core/tests/engine.rs:494,1147` (`JobRow` literals).
- Modify `crates/charpente-core/tests/materialize.rs`.
- Modify `crates/charpente-macros/src/event.rs`, `crates/charpente-macros/src/job.rs`, and the doc comment in `crates/charpente-macros/src/fingerprint.rs`.
- Modify `crates/charpente/src/repl/mod.rs:38-42` (`PROTO_VERSION`).
- Modify the hand-written impls:
  - `crates/charpente/tests/transactions.rs:49,68,326,373`
  - `crates/charpente/tests/http.rs:1144`
  - `crates/charpente/tests/replication.rs:65`
  - `crates/charpente/src/serve.rs:886`
  - `crates/charpente/src/repl/probe.rs:407`
- Test: `crates/charpente/tests/realtime.rs` (fixture modules plus a fingerprint test), `crates/charpente/tests/kernel.rs` (fixture modules plus a fingerprint test), `crates/charpente/tests/replication.rs` (the version-2 Hello).
- Docs:
  - `docs/GUIDE.md` §3 (~495–545);
  - `docs/GUIDE.md` §11's `#[event]` subsection;
  - `docs/GUIDE.md` §12 (~3783–3796, and "Hand-written `Row` opts out");
  - `DESIGN.md` §2 "Record payload" (~426–446) and §3;
  - the spec, which gets its amendments.

**Interfaces:**
- Produces:
  - `pub trait Job: Codec { const NAME: &'static str; const FINGERPRINT: u64; const RETRY: RetryPolicy = … }`
  - `pub trait Event: Codec { const NAME: &'static str; const FINGERPRINT: u64; }`

    `FINGERPRINT` is required, with no default.
  - `EventRow { id, seq, name, key, payload, shape: Option<u64> }`
  - `JobRow { id, name, payload, key, status, attempts, due_at, last_error, shape: Option<u64> }`
  - `materialize_job(&mut self, name: &str, payload: &[u8], key: Option<&str>, shape: Option<u64>) -> io::Result<u64>`
  - `materialize_event(&mut self, seq: u64, name: &str, payload: &[u8], key: Option<&str>, shape: Option<u64>) -> io::Result<u64>`
  - Record tags `0x08 [job][payload][key][shape: u64 le]` and `0x09 [event][payload][key][shape: u64 le]`. `0x02`/`0x03` decode with `shape: None` and are never written. `0x0a` is the next free tag.
  - `charpente::repl::PROTO_VERSION == 3`
  - Test fixtures used by Tasks 2–4 and 6:
    - `realtime.rs` modules `shapes_v1` and `shapes_v2`, each declaring `IncidentOpened`, `Swapped` and `Dropped`;
    - `kernel.rs` modules `receipt_v1` and `receipt_v2`, each declaring `SendReceipt`.

- [ ] **Step 1: Write the failing core tests**

In `crates/charpente-core/src/db.rs`'s `#[cfg(test)] mod tests`, add:

```rust
    /// A record written before payload shapes (tags `0x02`/`0x03`) still
    /// decodes, its shape unknown (payload-shapes spec §2). Literal bytes, not
    /// a fresh encode: no binary writes these tags any more, so encoding on
    /// both sides would prove nothing.
    #[test]
    fn a_pre_shape_enqueue_and_emit_decode_with_no_shape() {
        let bytes = vec![
            2, 0, 0, 0, // two effects
            2, 4, 0, 0, 0, b'p', b'i', b'n', b'g', 1, 0, 0, 0, 7, 0, // enqueue "ping" [7], no key
            3, 4, 0, 0, 0, b'p', b'o', b'n', b'g', 1, 0, 0, 0, 9, 0, // emit "pong" [9], no key
        ];
        assert_eq!(
            decode_record(&bytes).unwrap(),
            vec![
                Effect::Enqueue {
                    job: "ping".into(),
                    payload: vec![7],
                    key: None,
                    shape: None
                },
                Effect::Emit {
                    event: "pong".into(),
                    payload: vec![9],
                    key: None,
                    shape: None
                },
            ]
        );
    }

    /// The shaped tags, pinned as literal bytes: `0x08`/`0x09`, then the
    /// shape as eight little-endian bytes after the key.
    #[test]
    fn shaped_enqueue_and_emit_bytes_are_pinned() {
        let mut effects = TxEffects::default();
        effects
            .enqueues
            .push(("ping", vec![7], None, 0x0102_0304_0506_0708));
        effects
            .emits
            .push(("pong", vec![9], None, 0x1112_1314_1516_1718));
        assert_eq!(
            encode_record(&effects),
            vec![
                2, 0, 0, 0, // two effects
                8, 4, 0, 0, 0, b'p', b'i', b'n', b'g', 1, 0, 0, 0, 7, 0, // shaped enqueue
                8, 7, 6, 5, 4, 3, 2, 1, // its shape
                9, 4, 0, 0, 0, b'p', b'o', b'n', b'g', 1, 0, 0, 0, 9, 0, // shaped emit
                0x18, 0x17, 0x16, 0x15, 0x14, 0x13, 0x12, 0x11, // its shape
            ]
        );
        assert_eq!(
            decode_record(&encode_record(&effects)).unwrap(),
            vec![
                Effect::Enqueue {
                    job: "ping".into(),
                    payload: vec![7],
                    key: None,
                    shape: Some(0x0102_0304_0506_0708)
                },
                Effect::Emit {
                    event: "pong".into(),
                    payload: vec![9],
                    key: None,
                    shape: Some(0x1112_1314_1516_1718)
                },
            ]
        );
    }
```

Update the existing fixtures in the same module so they compile against the 4-tuple:
- `effects_fixture`: push `("send_receipt", vec![10, 0], None, 0x5E4D)` and `("invoice_finalized", vec![7], None, 0xE7E7)`.
- `record_roundtrip_is_canonical`: expects `shape: Some(0x5E4D)` / `Some(0xE7E7)`.
- `keyed_emit_roundtrips` and `keyed_enqueue_roundtrips`: add a shape to each push, and `shape: Some(..)` to each expected effect.

In `crates/charpente-core/src/event.rs` tests, add a `shape: Some(0xFEED)` field to `event_row_roundtrips`'s row plus `assert_eq!(back.shape, row.shape);`, then add:

```rust
    /// A row written before `shape` existed ends exactly after its payload:
    /// a `None` shape adds no bytes, so old rows decode, as `None`, unchanged.
    #[test]
    fn a_pre_shape_event_row_decodes_with_no_shape() {
        let row = EventRow {
            id: Id::from_raw(3),
            seq: 12,
            name: "invoice_finalized".into(),
            key: None,
            payload: vec![1, 2, 3],
            shape: None,
        };
        let mut bytes = Vec::new();
        row.encode_row(&mut bytes);
        let mut legacy = Vec::new();
        row.id.encode(&mut legacy);
        row.seq.encode(&mut legacy);
        row.name.encode(&mut legacy);
        row.key.encode(&mut legacy);
        row.payload.encode(&mut legacy);
        assert_eq!(bytes, legacy, "a None shape adds no bytes");
        assert_eq!(EventRow::decode_row(&legacy).unwrap().shape, None);
    }

    #[test]
    fn a_torn_event_row_shape_is_a_named_decode_error() {
        let row = EventRow {
            id: Id::from_raw(3),
            seq: 12,
            name: "invoice_finalized".into(),
            key: None,
            payload: vec![1, 2, 3],
            shape: Some(0xFEED),
        };
        let mut bytes = Vec::new();
        row.encode_row(&mut bytes);
        bytes.truncate(bytes.len() - 5);
        let err = EventRow::decode_row(&bytes)
            .err()
            .expect("3 bytes where a shape should be");
        assert_eq!(
            err.to_string(),
            "row decode: 3 bytes after an EventRow's payload — a shape is 8 bytes or absent, \
             so the stored row is corrupt"
        );
    }
```

In `crates/charpente-core/src/job.rs` tests:
- `job_row_roundtrips` gains `shape: Some(0xFEED)` and a matching assertion.
- Add `a_pre_shape_job_row_decodes_with_no_shape` and `a_torn_job_row_shape_is_a_named_decode_error`. They are the same shape as the two event tests above: encode `id, name, payload, key, status, attempts, due_at, last_error` by hand for the legacy bytes.
- The error text is `"row decode: 3 bytes after a JobRow's last_error — a shape is 8 bytes or absent, so the stored row is corrupt"`.

In `crates/charpente-core/tests/materialize.rs`, add `const FINGERPRINT: u64 = 0x7E57_0009;` to `impl Job for Ping` and `const FINGERPRINT: u64 = 0x7E57_000A;` to `impl Event for TestEvent`. Then add:

```rust
#[test]
fn enqueues_and_emits_record_their_types_shape() {
    let sim = Sim::with_faults(0x605, FaultConfig::none());
    let mut db = Db::open(Box::new(sim.disk()), DbConfig::default(), Schema::new()).unwrap();
    db.transact(|tx: &mut Tx| {
        tx.enqueue(Ping);
        tx.emit(TestEvent { n: 1 });
        Ok::<(), io::Error>(())
    })
    .unwrap();
    let snap = db.snapshot().unwrap();
    assert_eq!(
        snap.scan::<JobRow>().unwrap()[0].shape,
        Some(<Ping as Job>::FINGERPRINT)
    );
    let events: Vec<EventRow> = snap.scan_from(0, 1).unwrap();
    assert_eq!(events[0].shape, Some(<TestEvent as Event>::FINGERPRINT));
}

/// Spec §6.6: a record a pre-shape binary wrote (`0x02` enqueue, `0x03`
/// emit), applied the way a follower applies shipped frames, materializes
/// with no shape — which every later check treats as foreign.
#[test]
fn a_pre_shape_record_materializes_with_no_shape() {
    let sim = Sim::with_faults(0x606, FaultConfig::none());
    let mut db = Db::open(Box::new(sim.disk()), DbConfig::default(), Schema::new()).unwrap();
    let mut record = Vec::new();
    2u32.encode(&mut record);
    record.push(2);
    "ping".to_string().encode(&mut record);
    Vec::<u8>::new().encode(&mut record);
    None::<String>.encode(&mut record);
    record.push(3);
    "test_event".to_string().encode(&mut record);
    vec![1u8, 0, 0, 0].encode(&mut record);
    None::<String>.encode(&mut record);
    let seq = db.last_applied().map_or(0, |s| s + 1);
    db.apply_shipped(&[(seq, record)]).unwrap();
    let snap = db.snapshot().unwrap();
    assert_eq!(snap.scan::<JobRow>().unwrap()[0].shape, None);
    let events: Vec<EventRow> = snap.scan_from(0, 1).unwrap();
    assert_eq!(events[0].shape, None);
}
```

- [ ] **Step 2: Write the failing macro, fixture and protocol tests**

In `crates/charpente/tests/realtime.rs`:
- add `use charpente_core::db::Event;` to the imports;
- after the `Pong` event, add:

```rust
// Two shapes of one event under one NAME — what a deploy that edits an
// #[event]'s fields leaves behind in charpente.events. `#[event]` names an
// event by the snake_case of its TYPE name, so each pair shares a name and
// differs only in fields (payload-shapes spec §6.1–6.2).
mod shapes_v1 {
    use charpente::event;
    use charpente::wire::WireU64;

    #[event]
    pub struct IncidentOpened {
        pub n: WireU64,
    }

    #[event]
    pub struct Swapped {
        pub a: WireU64,
        pub b: WireU64,
    }

    #[event]
    pub struct Dropped {
        pub a: WireU64,
        pub b: WireU64,
    }
}

mod shapes_v2 {
    use charpente::event;
    use charpente::wire::WireU64;

    /// v1 plus a field — the statuspage review's C-1 change.
    #[event]
    pub struct IncidentOpened {
        pub n: WireU64,
        pub severity: WireU64,
    }

    /// v1 with its two same-typed fields swapped.
    #[event]
    pub struct Swapped {
        pub b: WireU64,
        pub a: WireU64,
    }

    /// v1 with its trailing field dropped.
    #[event]
    pub struct Dropped {
        pub a: WireU64,
    }
}

/// `#[event]` fingerprints its FIELDS, not its name. Each pair shares one
/// `NAME`, and each is one of the three things a reshaped event does when the
/// new type decodes the old bytes: an inserted field runs out of input, a
/// same-typed swap decodes with the values traded, a dropped trailing field
/// leaves bytes behind. The fingerprint is what tells them apart; Task 2 makes
/// delivery act on it.
#[test]
fn an_events_fingerprint_follows_its_fields_not_its_name() {
    let mut v1 = Vec::new();
    shapes_v1::IncidentOpened { n: WireU64(4) }.encode(&mut v1);
    assert!(
        shapes_v2::IncidentOpened::decode(&mut &v1[..]).is_err(),
        "an inserted field runs out of input"
    );

    let mut v1 = Vec::new();
    shapes_v1::Swapped {
        a: WireU64(1),
        b: WireU64(101),
    }
    .encode(&mut v1);
    let swapped = shapes_v2::Swapped::decode(&mut &v1[..]).unwrap();
    assert_eq!(
        (swapped.a.0, swapped.b.0),
        (101, 1),
        "a same-typed swap decodes with the values traded, and no error anywhere"
    );

    let mut v1 = Vec::new();
    shapes_v1::Dropped {
        a: WireU64(1),
        b: WireU64(101),
    }
    .encode(&mut v1);
    let input = &mut &v1[..];
    let dropped = shapes_v2::Dropped::decode(input).unwrap();
    assert_eq!(dropped.a.0, 1);
    assert_eq!(input.len(), 8, "the dropped field's bytes are left behind");

    assert_eq!(
        <shapes_v1::IncidentOpened as Event>::NAME,
        <shapes_v2::IncidentOpened as Event>::NAME
    );
    assert_ne!(
        <shapes_v1::IncidentOpened as Event>::FINGERPRINT,
        <shapes_v2::IncidentOpened as Event>::FINGERPRINT
    );
    assert_ne!(
        <shapes_v1::Swapped as Event>::FINGERPRINT,
        <shapes_v2::Swapped as Event>::FINGERPRINT
    );
    assert_ne!(
        <shapes_v1::Dropped as Event>::FINGERPRINT,
        <shapes_v2::Dropped as Event>::FINGERPRINT
    );
    // The control: identical fields fingerprint identically whatever the
    // name — NAME carries identity, FINGERPRINT carries shape.
    assert_eq!(
        <Ping as Event>::FINGERPRINT,
        <Pong as Event>::FINGERPRINT
    );
}
```

In `crates/charpente/tests/kernel.rs`, after the `Panics` job, add:

```rust
// Two shapes of one job under one NAME (`#[job]` names a job by its type's
// ident): what a deploy that edits a job's fields meets in charpente.jobs.
mod receipt_v1 {
    use charpente::kernel::JobError;
    use charpente::{Tx, job};

    #[job]
    pub struct SendReceipt {
        pub invoice: u64,
    }

    impl SendReceipt {
        fn run(self, _tx: &mut Tx) -> Result<(), JobError> {
            let _ = self.invoice;
            Ok(())
        }
    }
}

mod receipt_v2 {
    use charpente::kernel::JobError;
    use charpente::{Tx, job};

    /// v1 plus a field.
    #[job]
    pub struct SendReceipt {
        pub invoice: u64,
        pub copies: u32,
    }

    impl SendReceipt {
        fn run(self, _tx: &mut Tx) -> Result<(), JobError> {
            let _ = (self.invoice, self.copies);
            Ok(())
        }
    }
}

#[test]
fn a_jobs_fingerprint_follows_its_fields_not_its_name() {
    assert_eq!(receipt_v1::SendReceipt::NAME, receipt_v2::SendReceipt::NAME);
    assert_ne!(
        <receipt_v1::SendReceipt as Job>::FINGERPRINT,
        <receipt_v2::SendReceipt as Job>::FINGERPRINT
    );
    // The control: two empty payloads fingerprint identically.
    assert_eq!(
        <AlwaysFails as Job>::FINGERPRINT,
        <Panics as Job>::FINGERPRINT
    );
}
```

In `crates/charpente/tests/replication.rs`, next to `hello_declaring_protocol_version_1_is_refused_by_name`, add:

```rust
/// PROTO_VERSION 3 (payload shapes): a version-2 binary writes `0x02`/`0x03`
/// and cannot read `0x08`/`0x09`, so it is refused at Hello, by name, before a
/// single frame ships (spec amendment A3).
#[test]
fn hello_declaring_protocol_version_2_is_refused_by_name() {
    let sim = Sim::with_faults(0x2EB1_0006 ^ 2, FaultConfig::none());
    let mut p = spawn_node(&sim, "p", primary_cfg("p:1"), DbConfig::default()).unwrap();
    let reply = exchange_hello(
        &sim,
        &mut p,
        "p:1",
        Msg::Hello {
            proto: 2,
            listen: None,
            position: None,
            secret: None,
            schema_digest: 0,
        },
    )
    .unwrap();
    match reply {
        Msg::Refuse { message } => assert_eq!(
            message,
            "replication protocol version mismatch: this node speaks protocol version 3, but \
             the peer's Hello declared version 2 — upgrade the older binary so both speak the \
             expected version 3"
        ),
        other => panic!("expected Refuse, got {other:?}"),
    }
}
```

- [ ] **Step 3: Run the new tests and record why each is red**

```zsh
cargo test -p charpente-core --lib -- shape 2>&1 | tail -30
cargo test -p charpente-core --test materialize 2>&1 | tail -30
cargo test -p charpente --test realtime -- an_events_fingerprint 2>&1 | tail -30
cargo test -p charpente --test replication -- hello_declaring_protocol_version_2 --exact 2>&1 | tail -30
```

Expected: compile errors ("no field `shape`", "no associated item named `FINGERPRINT`", a 4-tuple into a 3-tuple Vec). The one *behavioural* red available here is the Hello test, once everything else compiles: under `PROTO_VERSION = 2` it gets no `Refuse`. Record that after Step 4 by setting the version back to 2 for one run. It must fail with "expected Refuse, got …".

- [ ] **Step 4: Implement the tag**

`crates/charpente-core/src/db.rs`, traits:

```rust
pub trait Job: Codec {
    const NAME: &'static str;
    /// The payload's structural fingerprint — `#[job]` computes it from the
    /// fields (the helper `#[table]` uses). Recorded beside every enqueue; a
    /// Pending job is dispatched only by a binary that compiled the same
    /// value (payload-shapes spec §4). No default, unlike `Row::FINGERPRINT`:
    /// a hand-written impl must state one, or a hand-written job would opt
    /// out of the check silently.
    const FINGERPRINT: u64;
    const RETRY: crate::job::RetryPolicy = crate::job::RetryPolicy::DEFAULT;
}

pub trait Event: Codec {
    const NAME: &'static str;
    /// The payload's structural fingerprint, as for `Job::FINGERPRINT`:
    /// recorded beside every emit, and delivery decodes a stored event only
    /// under the shape it was written with (spec §3).
    const FINGERPRINT: u64;
}
```

In `enum Effect`, add a `shape` field to both variants:

```rust
    // `shape`: `None` for the pre-shape tags `0x02`/`0x03`, `Some` for
    // `0x08`/`0x09` — the wire presence, like `scope` above. `None` is
    // foreign to every check downstream (payload-shapes spec §2).
    Enqueue {
        job: String,
        payload: Vec<u8>,
        key: Option<String>,
        shape: Option<u64>,
    },
    Emit {
        event: String,
        payload: Vec<u8>,
        key: Option<String>,
        shape: Option<u64>,
    },
```

`TxEffects`: `enqueues` and `emits` become `Vec<(&'static str, Vec<u8>, Option<String>, u64)>`. The shape is a bare `u64` on purpose, so a shapeless write cannot be represented (amendment A2). Add that as a one-line comment on the fields.

`encode_record`:

```rust
    for (job, payload, key, shape) in &effects.enqueues {
        out.push(8);
        encode_bytes(job.as_bytes(), &mut out);
        encode_bytes(payload, &mut out);
        key.encode(&mut out);
        shape.encode(&mut out);
    }
    for (event, payload, key, shape) in &effects.emits {
        out.push(9);
        encode_bytes(event.as_bytes(), &mut out);
        encode_bytes(payload, &mut out);
        key.encode(&mut out);
        shape.encode(&mut out);
    }
```

In `decode_record`, add `shape: None` to arms `2` and `3`, then add after arm `7`:

```rust
            8 => Effect::Enqueue {
                job: String::decode(input)?,
                payload: Vec::<u8>::decode(input)?,
                key: Option::<String>::decode(input)?,
                shape: Some(u64::decode(input)?),
            },
            9 => Effect::Emit {
                event: String::decode(input)?,
                payload: Vec::<u8>::decode(input)?,
                key: Option::<String>::decode(input)?,
                shape: Some(u64::decode(input)?),
            },
```

The fields must stay in wire order: a struct literal evaluates its fields in the order written, and that is what makes this decode correct.

`Tx`: each push gains the constant. That is `(J::NAME, payload, None, J::FINGERPRINT)` in `enqueue`, `Some(key)` in `enqueue_keyed`, and `Ev::FINGERPRINT` in `emit`/`emit_keyed`.

The apply arm:

```rust
                    Effect::Enqueue {
                        job,
                        payload,
                        key,
                        shape,
                    } => {
                        new_ids.push(w.materialize_job(job, payload, key.as_deref(), *shape)?);
                    }
                    Effect::Emit {
                        event,
                        payload,
                        key,
                        shape,
                    } => {
                        w.materialize_event(seq, event, payload, key.as_deref(), *shape)?;
                    }
```

`crates/charpente-core/src/table.rs`: each materialize function takes `shape: Option<u64>` last, and sets `shape` in the row it builds.

`crates/charpente-core/src/event.rs`: the struct gains its last field, the encoder writes it only when `Some`, and the decoder reads the fields in order:

```rust
    /// The emitting type's `Event::FINGERPRINT`, from the record's `0x09`
    /// tag; `None` for a row applied from a pre-shape `0x03` record. Encoded
    /// LAST and only when `Some`, so a row written before this field existed
    /// decodes unchanged: the payload before it is length-prefixed, and
    /// nothing follows it (payload-shapes spec §2).
    pub shape: Option<u64>,
```

```rust
    fn encode_row(&self, out: &mut Vec<u8>) {
        self.id.encode(out);
        self.seq.encode(out);
        self.name.encode(out);
        self.key.encode(out);
        self.payload.encode(out);
        if let Some(shape) = self.shape {
            shape.encode(out);
        }
    }

    fn decode_row(bytes: &[u8]) -> io::Result<EventRow> {
        let input = &mut &bytes[..];
        let id = Id::decode(input)?;
        let seq = u64::decode(input)?;
        let name = String::decode(input)?;
        let key = Option::<String>::decode(input)?;
        let payload = Vec::<u8>::decode(input)?;
        let shape = match input.len() {
            0 => None,
            8 => Some(u64::decode(input)?),
            n => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "row decode: {n} bytes after an EventRow's payload — a shape is 8 bytes \
                         or absent, so the stored row is corrupt"
                    ),
                ));
            }
        };
        Ok(EventRow {
            id,
            seq,
            name,
            key,
            payload,
            shape,
        })
    }
```

The old trailing-bytes check is now unreachable, so delete it. `crates/charpente-core/src/job.rs` gets the same shape, with the message "…after a JobRow's last_error…". The two `JobRow` literals in `crates/charpente-core/tests/engine.rs` gain `shape: None`.

`crates/charpente-macros/src/event.rs`: compute the fingerprint beside the field lists and emit it.

```rust
    let fingerprint = crate::fingerprint::compute(&field_idents, &field_types);
```

```rust
        impl ::charpente::core::db::Event for #name {
            const NAME: &'static str = #event_name;
            const FINGERPRINT: u64 = #fingerprint;
        }
```

`crates/charpente-macros/src/job.rs`: the same computation, and `const FINGERPRINT: u64 = #fingerprint;` in the `Job` impl, between `NAME` and `RETRY`. `fingerprint.rs`'s module doc becomes "Shared by `#[table]`, `#[row_shape]`, `#[event]` and `#[job]`".

In `crates/charpente/src/repl/mod.rs`:

```rust
/// … Bumps whenever a message body's shape changes — tenant-scoping's
/// `ScopedSnapshotRows` (tag 14) made it 2; payload shapes' `0x08`/`0x09`
/// record tags make it 3, so a version-2 binary is refused at Hello rather
/// than at its first shaped frame.
pub const PROTO_VERSION: u8 = 3;
```

Hand-written impls: add one line to each, using a distinct test value so no two collide.

| Site | Type | Line to add |
|---|---|---|
| `transactions.rs:49` | `SendReceipt` | `const FINGERPRINT: u64 = 0x7E57_0001;` |
| `transactions.rs:68` | `InvoiceFinalized` | `0x7E57_0002` |
| `transactions.rs:326` | `Ping` | `0x7E57_0003` |
| `transactions.rs:373` | `Audit` | `0x7E57_0004` |
| `http.rs:1144` | `Ping` | `0x7E57_0005` |
| `replication.rs:65` | `Ping` | `0x7E57_0006` |
| `serve.rs:886` | `Ping` (event) | `0x7E57_0007` |
| `repl/probe.rs:407` | `Ping` | `0x7E57_0008` |

Update the module doc of `crates/charpente-core/src/db.rs` (lines 8–30):
- the tag list reads `0x08 [job][payload][key: Option<String>][shape: u64 le]` shaped enqueue and `0x09 [event][payload][key: Option<String>][shape: u64 le]` shaped emit;
- add the sentence: "`0x02`/`0x03` (the same without the shape) still decode, with the shape unknown, and are never written — `TxEffects` holds a bare shape, so a shapeless enqueue or emit cannot be represented";
- "`0x08` is the next free tag" becomes "`0x0a` is the next free tag".

- [ ] **Step 5: Run the task's tests and see them pass**

```zsh
cargo test -p charpente-core --no-fail-fast 2>&1 | tee .superpowers/sdd/2026-09-24-payload-shapes/t1-core.log | grep -E "^test result|FAILED|panicked"
cargo test -p charpente-macros --no-fail-fast 2>&1 | grep -E "^test result|FAILED"
cargo test -p charpente --test realtime -- an_events_fingerprint --exact
cargo test -p charpente --test kernel -- a_jobs_fingerprint --exact
cargo test -p charpente --test replication -- hello_declaring_protocol_version --nocapture
```

Expected: all pass. Then set `PROTO_VERSION` back to `2`, run the version-2 Hello test, see it fail, and restore `3`. Quote the red output in the task report.

- [ ] **Step 6: Docs**

`docs/GUIDE.md` §3, the hand-written `impl Job for SendReceipt` inside the compiled ```rust block:

```rust
impl Job for SendReceipt {
    const NAME: &'static str = "send_receipt";
    // The payload's shape, recorded beside every enqueue: a Pending job is
    // only ever decoded by a binary whose FINGERPRINT matches (§6).
    // `#[job]` computes it from the fields; by hand, change it whenever
    // the fields change.
    const FINGERPRINT: u64 = 0x5E4D_0001;
    // `RETRY` defaults to `RetryPolicy::DEFAULT` — retry/backoff and cron
    // are covered in a later section.
}
```

Also in §3:
- the prose "`Job` needs `Codec` + a `NAME`…" becomes "`Job` needs `Codec`, a `NAME` and a `FINGERPRINT`…";
- the `#[event]` sentence's list of derived items gains "`FINGERPRINT` (the payload's structural fingerprint — §11, "Changing an event")".

In §11's `### #[event]` subsection, add this sentence where the derived items are listed: "It also derives `Event::FINGERPRINT`, a structural fingerprint of the payload's fields (§12's helper), recorded beside every emit; delivery decodes a stored event only under the shape it was written with (see "Changing an event" below)." Task 2 writes that subsection. Until then the forward reference stands alone, and Task 2 must land the heading it names.

`docs/GUIDE.md` §12: after the paragraph ending "…nothing forces you to remember to bump `version`)", add:

> **What the fingerprint cannot see.** It hashes each field's name and the *tokens* of its type, not the type's definition. A change inside a type a field holds — a new variant on an enum, a field added to a nested struct — leaves the fingerprint unchanged, for tables and for `#[event]`/`#[job]` payloads alike (§6, §11). Such a change is exactly as dangerous as a field change and nothing catches it: treat an edit to any type a row or payload contains as an edit to that row or payload itself. A deep fingerprint is on PLAN.md's after-launch list with its trigger.

`DESIGN.md` §2, "Record payload":
- replace `enqueue \`0x02 [job][payload][key: Option<String>]\`, emit \`0x03 [event][payload][key: Option<String>]\`` with `enqueue \`0x08 [job][payload][key: Option<String>][shape: u64]\`, emit \`0x09 [event][payload][key: Option<String>][shape: u64]\``;
- add: "The shape is the payload type's structural fingerprint (§3). The pre-shape `0x02`/`0x03` (the same without the shape) still decode, with the shape unknown, and are never written.";
- "`0x08` is the next free tag" becomes "`0x0a` is the next free tag".

`DESIGN.md` §3: after the paragraph that introduces the table fingerprint (search "fingerprint" in §3), add:

> **Payload shapes.** `#[event]` and `#[job]` carry the same structural fingerprint `#[table]` does (`Event::FINGERPRINT`, `Job::FINGERPRINT` — required on a hand-written impl, no default), and every emit and enqueue records it in the log beside the payload. A payload is decoded only under the shape it was written with: delivery refuses a foreign-shape event by name and ends that one tail (§6), and a node refuses to start dispatch over a foreign-shape Pending job (§6). There is no per-row versioned decode — the migrations spec rejected it twice — and no override (MANIFESTO §8).

Append the "Spec amendments" section of this plan to the spec, verbatim, under `## Amendments (implementation plan)`.

- [ ] **Step 7: The seed census**

Every enqueue and emit record is now 8 bytes longer, so universes whose workloads enqueue or emit may be renamed (see Global Constraints). Do this after the full workspace gate has run:

1. Any pinned-seed test that goes red because it asserts a scenario is *reason*-asserting working as intended. Re-find a seed that reproduces the named scenario by scanning the same swarm construction (`CHARPENTE_BASE_SEED=<the test's base>` over a batch). Assert the reason, update the constant and its doc comment ("re-pinned 2026-09-2x: payload shapes grew enqueue/emit records by 8 bytes"), and record old → new in the task report. `realtime.rs`'s give-up pin `0xbb93400e0bc80ca4` is the known candidate.
2. **Re-prove the CI kernel pin's teeth.** `.github/workflows/ci.yml` pins `0xe653f43924f17fe8` as "the live RED-first reproduction" of the `is_fault_fallout` classification at `crates/charpente/tests/kernel.rs` (the `exactly_once_holds(...).and_then(...)` match, ~1591). Delete that arm's `if !is_fault_fallout(&msg)` guard so it always returns `Err(msg)`, then run:

   ```zsh
   CHARPENTE_SEED=0xe653f43924f17fe8 cargo test -p charpente --test kernel -- kernel_jobs_are_exactly_once_under_the_fault_matrix --exact
   ```

   It must FAIL. Restore the guard and confirm it passes.
   - If it passes with the guard deleted, the pin was defanged by the rename. Re-find a RED-first seed with a 10,000-universe `CHARPENTE_BASE_SEED` batch from the file's ambient base, with the guard deleted. Replace the seed in ci.yml and in the kernel.rs comment.
   - Check `git diff` afterwards: no deletion survives.
3. `0x348270b38f166529` (ci.yml's older kernel pin) is kept for the drain-id fix; its comment already says it no longer reaches the classification. `0xff159ed76d2806e4` (replication, a release/debug mode-skew pin) and `0x8bee1752811ae873` (`charpente-sim` crash_recovery, whose workload neither enqueues nor emits) need nothing beyond passing.
4. A coverage `floor:` that moves is re-measured and updated with the old and new values in the report. A byte-count pin (bench accounting, a trace digest) that moves must move by exactly 8 bytes per enqueue or emit in its workload. Confirm that arithmetic before updating it.

- [ ] **Step 8: Gate and commit**

Run the three-command gate (Global Constraints) and count it. The new tests add to the existing binaries' counts; the number of `test result:` lines stays 87. Then:

```zsh
git add crates/charpente-core crates/charpente-macros crates/charpente/src crates/charpente/tests docs/GUIDE.md DESIGN.md docs/superpowers/specs/2026-09-24-payload-shapes-design.md
git status --short   # add .github/workflows/ci.yml only if Step 7 re-pinned it
git commit -m "feat(payload-shapes): every emit and enqueue records its payload's shape — tags 0x08/0x09, EventRow/JobRow.shape, PROTO_VERSION 3"
```

---

### Task 2: Events — a reshaped event ends its tail by name, and its cursor is refused by name

**Files:**
- Modify `crates/charpente/src/http/mod.rs:204-237` (`SubEntry`, `Subscription::of`/`and`).
- Modify `crates/charpente/src/kernel/tail.rs:30-120` (`RESHAPED_FRAME`, `deliver_unit`).
- Modify `crates/charpente/src/kernel/mod.rs`: the re-export at line 11, the call site at ~576, and `validate_subscribe` at ~939.
- Modify `crates/charpente/src/serve.rs` tests (wire test for the reshaped frame).
- Test `crates/charpente/tests/realtime.rs`: `FailingFeed`'s `SubEntry`, the new routes, `shapes_app`, the helpers and tests, and the flagship `Client` arm.
- Docs: `docs/GUIDE.md` §11 (new "Changing an event" subsection, cursor semantics, Delivery), `DESIGN.md` §6, and `crates/guide-examples/tests/doc_truth.rs` (`PINNED_FLOOR` 16 → 18).

**Interfaces:**
- Consumes (Task 1): `Event::FINGERPRINT`, `EventRow.shape`, and the fixture modules `shapes_v1`/`shapes_v2`.
- Produces:
  - `pub struct SubEntry { pub name: &'static str, pub encode_json: fn(&[u8]) -> io::Result<String>, pub fingerprint: u64 }`
  - `pub const RESHAPED_FRAME: &str = "charpente.reshaped";`, re-exported from `charpente::kernel`
  - `fn validate_subscribe(db: &Db, sub: &Subscription) -> Result<u64, Response>`
  - Test helpers in `realtime.rs` that Tasks 3 and 4 reuse: `shapes_app`, `run_script`, `mixed_log`, `frames_of`, `statuses`, `reshaped_frame`, `reshaped_409`, `error_text`, `legacy_emit_record`.

- [ ] **Step 1: Add the routes, app and helpers to `realtime.rs`**

After the `feed_keyed` route, add:

```rust
// ------------------------------------------------------- reshaped events
//
// Routes over the `shapes_v1`/`shapes_v2` pairs. The v1 emitters are what the
// OLD binary served, the v2 emitters and feeds what the new one serves; one
// binary hosts both, so a mixed log is one POST away. The feeds declare no
// `emits(...)`: that list takes bare idents, these types live in modules, and
// nothing here generates a client.

#[route(POST "/v1/incident", public)]
fn emit_v1_incident(tx: &mut Tx, body: EmitBody) -> Result<(), ApiError> {
    let ev = shapes_v1::IncidentOpened { n: body.n };
    match body.key {
        Some(key) => tx.emit_keyed(key, ev),
        None => tx.emit(ev),
    }
    Ok(())
}

#[route(POST "/v2/incident", public)]
fn emit_v2_incident(tx: &mut Tx, body: EmitBody) -> Result<(), ApiError> {
    let ev = shapes_v2::IncidentOpened {
        n: body.n,
        severity: WireU64(body.n.0 % 3),
    };
    match body.key {
        Some(key) => tx.emit_keyed(key, ev),
        None => tx.emit(ev),
    }
    Ok(())
}

#[route(POST "/v1/swapped", public)]
fn emit_v1_swapped(tx: &mut Tx, body: EmitBody) -> Result<(), ApiError> {
    tx.emit(shapes_v1::Swapped {
        a: body.n,
        b: WireU64(body.n.0 + 100),
    });
    Ok(())
}

#[route(POST "/v1/dropped", public)]
fn emit_v1_dropped(tx: &mut Tx, body: EmitBody) -> Result<(), ApiError> {
    tx.emit(shapes_v1::Dropped {
        a: body.n,
        b: WireU64(body.n.0 + 100),
    });
    Ok(())
}

#[route(SSE "/v2/incidents-and-pongs", public)]
fn feed_v2_incidents_and_pongs(
    tx: &mut Tx,
    ctx: &Ctx,
    cursor: Option<Cursor>,
) -> Result<Subscription, ApiError> {
    let _ = (tx, ctx);
    Ok(Subscription::of::<shapes_v2::IncidentOpened>()
        .and::<Pong>()
        .cursor(cursor))
}

#[route(SSE "/v2/incidents-org-a", public)]
fn feed_v2_incidents_org_a(
    tx: &mut Tx,
    ctx: &Ctx,
    cursor: Option<Cursor>,
) -> Result<Subscription, ApiError> {
    let _ = (tx, ctx);
    Ok(Subscription::of::<shapes_v2::IncidentOpened>()
        .key("org-a")
        .cursor(cursor))
}

#[route(SSE "/v2/swapped", public)]
fn feed_v2_swapped(tx: &mut Tx, ctx: &Ctx, cursor: Option<Cursor>) -> Result<Subscription, ApiError> {
    let _ = (tx, ctx);
    Ok(Subscription::of::<shapes_v2::Swapped>().cursor(cursor))
}

#[route(SSE "/v2/dropped", public)]
fn feed_v2_dropped(tx: &mut Tx, ctx: &Ctx, cursor: Option<Cursor>) -> Result<Subscription, ApiError> {
    let _ = (tx, ctx);
    Ok(Subscription::of::<shapes_v2::Dropped>().cursor(cursor))
}

#[route(SSE "/pongs", public)]
fn feed_pongs(tx: &mut Tx, ctx: &Ctx, cursor: Option<Cursor>) -> Result<Subscription, ApiError> {
    let _ = (tx, ctx);
    Ok(Subscription::of::<Pong>().cursor(cursor))
}

/// Default retention (65,536): nothing in these tests may fall behind it, so
/// a gap frame or a below-floor 409 here is a bug, never an excuse.
fn shapes_app(sim: &Sim) -> io::Result<App> {
    Ok(App::open(
        Box::new(sim.disk()),
        Schema::new(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )?
    .mount(routes![
        emit_pong,
        emit_v1_incident,
        emit_v2_incident,
        emit_v1_swapped,
        emit_v1_dropped,
        feed_v2_incidents_and_pongs,
        feed_v2_incidents_org_a,
        feed_v2_swapped,
        feed_v2_dropped,
        feed_pongs
    ]))
}

/// Pushes `requests` at one instant and steps the kernel until the script is
/// spent. Every step must succeed: the property these tests pin is that
/// nothing a subscriber does stops the node.
fn run_script(kernel: &mut Kernel, sim: &Sim, requests: Vec<Request>) -> (RecordedResponses, SinkHandles) {
    let mut source = SimSource::new(Box::new(sim.clock()), sim.now().as_nanos() + 10 * SEC);
    let t = sim.now().as_nanos();
    for request in requests {
        source.push_at(t, request);
    }
    let (responses, sinks) = (source.responses(), source.sinks());
    for _ in 0..16 {
        if let Err(e) = kernel.step(&mut source) {
            panic!("no step may fail — nothing a subscriber does stops the node: {e}");
        }
    }
    (responses, sinks)
}

/// Emits `(path, n, key)` in order on a fresh node — the mixed log a
/// reshaping deploy leaves — then restarts it (the deploy) and returns the new
/// kernel with every retained event row, so each test builds its cursors and
/// expected frames from real ids and seqs.
fn mixed_log(seed: u64, emits: &[(&str, u64, Option<&str>)]) -> (Sim, Kernel, Vec<EventRow>) {
    let sim = Sim::with_faults(seed, FaultConfig::none());
    let mut kernel = Kernel::new(shapes_app(&sim).unwrap(), Box::new(sim.clock())).unwrap();
    let requests = emits
        .iter()
        .map(|&(path, n, key)| {
            Request::new(Method::Post, path).json(&EmitBody {
                n: WireU64(n),
                key: key.map(str::to_string),
            })
        })
        .collect();
    let (responses, _) = run_script(&mut kernel, &sim, requests);
    assert!(
        statuses(&responses).iter().all(|s| *s == 200),
        "every emit commits: {:?}",
        statuses(&responses)
    );
    sim.restart();
    drop(kernel);
    let kernel = Kernel::new(shapes_app(&sim).unwrap(), Box::new(sim.clock())).unwrap();
    let rows: Vec<EventRow> = kernel.app().db().snapshot().unwrap().scan_from(0, 64).unwrap();
    assert_eq!(rows.len(), emits.len(), "every emit is one retained row");
    (sim, kernel, rows)
}

fn statuses(responses: &RecordedResponses) -> Vec<u16> {
    responses.lock().unwrap().iter().map(|(s, _)| *s).collect()
}

fn frames_of(handle: &SimSinkHandle) -> Vec<(u64, u64, String, String)> {
    handle
        .frames()
        .iter()
        .map(|f| (f.id, f.seq, f.name.to_string(), f.json.clone()))
        .collect()
}

fn cursor_of(row: &EventRow) -> String {
    format!("{}.{}", row.id.raw(), row.seq)
}

/// The terminal frame for a foreign row, spelled out here rather than taken
/// from the product so the comparison pins the spec's wording (§3).
fn reshaped_frame(row: &EventRow) -> (u64, u64, String, String) {
    (
        row.id.raw(),
        row.seq,
        "charpente.reshaped".to_string(),
        format!(
            r#"{{"event":"{}","id":{},"fix":"re-sync current state through reads, then reconnect without a cursor"}}"#,
            row.name,
            row.id.raw()
        ),
    )
}

/// The reshaped 409's text (spec §3 as amended, A5), spelled out.
fn reshaped_409(row: &EventRow, compiled: u64) -> String {
    let recorded = match row.shape {
        Some(shape) => format!("{shape:#x}"),
        None => "none — written before event shapes were tagged".to_string(),
    };
    format!(
        "cursor {} sits on an \"{}\" event written under a different shape of that event \
         (recorded {recorded}; this binary {compiled:#x}) — this binary will not decode it; \
         re-sync current state through reads, then reconnect without a cursor",
        cursor_of(row),
        row.name
    )
}

/// A `Response::error` body's `error` text.
fn error_text(body: &[u8]) -> String {
    let v: serde_json::Value = serde_json::from_slice(body).expect("an error body is JSON");
    v["error"].as_str().expect("an error body has an error string").to_string()
}

/// A record as a pre-shape binary wrote it: one `0x03` emit, no shape.
fn legacy_emit_record(event: &str, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    1u32.encode(&mut out);
    out.push(3);
    event.to_string().encode(&mut out);
    payload.to_vec().encode(&mut out);
    None::<String>.encode(&mut out);
    out
}
```

Import whatever these need that the file does not already import: `SimSinkHandle`, `RecordedResponses` and `SinkHandles` come from `charpente::kernel`, and `serde_json` is already a dev-dependency of `charpente`; check `Cargo.toml`.

- [ ] **Step 2: Write the behavioural tests**

```rust
/// Spec §6.1 — the statuspage review's C-1 probe, as a sim test. The old
/// binary emitted `incident_opened` in its v1 shape between pongs; the node
/// restarted as the new binary, whose `incident_opened` gained a field; a
/// client resumes from the pong before it. At c28250e this stopped the kernel
/// (`kernel.step` = Err, "needed 8 bytes for u64, 0 left") and every restart
/// stopped it again. Now the pong before the foreign row is delivered, the
/// foreign row ends the stream with one named frame carrying its own id, the
/// pong after it never arrives, a second tail on the same node keeps flowing,
/// and a client that ignored the frame and reconnects from its id is refused
/// by name.
#[test]
fn a_reshaped_event_ends_its_tail_by_name_and_the_node_keeps_serving() {
    let (sim, mut kernel, rows) = mixed_log(
        0x5A9E_0201,
        &[
            ("/pong", 0, None),
            ("/pong", 1, None),
            ("/v1/incident", 2, None),
            ("/pong", 3, None),
        ],
    );
    let (responses, sinks) = run_script(
        &mut kernel,
        &sim,
        vec![
            Request::new(
                Method::Get,
                format!("/v2/incidents-and-pongs?cursor={}", cursor_of(&rows[0])),
            ),
            Request::new(Method::Get, "/pongs"),
            Request::new(Method::Post, "/pong").json(&EmitBody {
                n: WireU64(4),
                key: None,
            }),
        ],
    );
    assert_eq!(statuses(&responses), vec![200, 200, 200]);
    let handles = sinks.lock().unwrap();
    assert_eq!(
        frames_of(&handles[0]),
        vec![
            (rows[1].id.raw(), rows[1].seq, "pong".to_string(), r#"{"n":"1"}"#.to_string()),
            reshaped_frame(&rows[2]),
        ]
    );
    assert_eq!(
        frames_of(&handles[1]).into_iter().map(|f| f.3).collect::<Vec<_>>(),
        vec![r#"{"n":"4"}"#.to_string()],
        "a second tail on the same node keeps flowing"
    );
    drop(handles);

    let (responses, _) = run_script(
        &mut kernel,
        &sim,
        vec![Request::new(
            Method::Get,
            format!("/v2/incidents-and-pongs?cursor={}", cursor_of(&rows[2])),
        )],
    );
    let responses = responses.lock().unwrap().clone();
    assert_eq!(responses.len(), 1);
    assert_eq!(responses[0].0, 409);
    assert_eq!(
        error_text(&responses[0].1),
        reshaped_409(&rows[2], <shapes_v2::IncidentOpened as Event>::FINGERPRINT)
    );
}

/// Spec §6.2 — the silent misdecodes. At c28250e this delivered
/// {"b":"1","a":"101"} for an event whose a was 1 and whose b was 101, with no
/// error anywhere.
#[test]
fn a_same_typed_field_swap_is_refused_not_delivered_with_the_values_traded() {
    let (sim, mut kernel, rows) =
        mixed_log(0x5A9E_0202, &[("/pong", 0, None), ("/v1/swapped", 1, None)]);
    let (_, sinks) = run_script(
        &mut kernel,
        &sim,
        vec![Request::new(
            Method::Get,
            format!("/v2/swapped?cursor={}", cursor_of(&rows[0])),
        )],
    );
    assert_eq!(frames_of(&sinks.lock().unwrap()[0]), vec![reshaped_frame(&rows[1])]);
}

/// Spec §6.2 — at c28250e this delivered {"a":"1"}: the event's b silently
/// gone, and the eight bytes it left behind never checked.
#[test]
fn a_dropped_trailing_field_is_refused_not_delivered_short() {
    let (sim, mut kernel, rows) =
        mixed_log(0x5A9E_0203, &[("/pong", 0, None), ("/v1/dropped", 1, None)]);
    let (_, sinks) = run_script(
        &mut kernel,
        &sim,
        vec![Request::new(
            Method::Get,
            format!("/v2/dropped?cursor={}", cursor_of(&rows[0])),
        )],
    );
    assert_eq!(frames_of(&sinks.lock().unwrap()[0]), vec![reshaped_frame(&rows[1])]);
}

/// Review focus 1: the key filter runs BEFORE the shape check, on both paths.
/// Another tenant's reshaped event must not end this tenant's tail, and a
/// cursor on it must not be refused — this subscription would never deliver
/// that row either way.
#[test]
fn another_keys_reshaped_event_neither_ends_nor_refuses_this_tail() {
    let (sim, mut kernel, rows) = mixed_log(
        0x5A9E_0204,
        &[
            ("/pong", 0, None),
            ("/v1/incident", 1, Some("org-b")),
            ("/v2/incident", 2, Some("org-a")),
        ],
    );
    let expected = vec![(
        rows[2].id.raw(),
        rows[2].seq,
        "incident_opened".to_string(),
        r#"{"n":"2","severity":"2"}"#.to_string(),
    )];
    let (responses, sinks) = run_script(
        &mut kernel,
        &sim,
        vec![
            Request::new(Method::Get, format!("/v2/incidents-org-a?cursor={}", cursor_of(&rows[0]))),
            Request::new(Method::Get, format!("/v2/incidents-org-a?cursor={}", cursor_of(&rows[1]))),
        ],
    );
    assert_eq!(statuses(&responses), vec![200, 200], "neither subscribe is refused");
    let handles = sinks.lock().unwrap();
    assert_eq!(frames_of(&handles[0]), expected, "org-b's foreign row did not end org-a's tail");
    assert_eq!(frames_of(&handles[1]), expected, "a cursor on org-b's foreign row resumes");
}

/// Spec §3: a cursor on a foreign row of an event the subscription does NOT
/// carry is not refused.
#[test]
fn a_cursor_on_a_foreign_row_of_an_unsubscribed_event_is_not_refused() {
    let (sim, mut kernel, rows) =
        mixed_log(0x5A9E_0205, &[("/v1/incident", 0, None), ("/pong", 1, None)]);
    let (responses, sinks) = run_script(
        &mut kernel,
        &sim,
        vec![Request::new(Method::Get, format!("/pongs?cursor={}", cursor_of(&rows[0])))],
    );
    assert_eq!(statuses(&responses), vec![200]);
    assert_eq!(
        frames_of(&sinks.lock().unwrap()[0]),
        vec![(rows[1].id.raw(), rows[1].seq, "pong".to_string(), r#"{"n":"1"}"#.to_string())]
    );
}

/// Spec §6.6: a row from a pre-shape binary is foreign on both paths — even
/// when its bytes are ones the v2 type WOULD decode. The rule is the recorded
/// shape, not whether a decode happens to succeed.
#[test]
fn a_pre_shape_event_is_foreign_on_both_paths_even_when_its_bytes_would_decode() {
    let (sim, mut kernel, rows) = mixed_log(0x5A9E_0206, &[("/pong", 0, None)]);
    let mut v2_bytes = Vec::new();
    shapes_v2::IncidentOpened {
        n: WireU64(9),
        severity: WireU64(1),
    }
    .encode(&mut v2_bytes);
    {
        let db = kernel.app_mut_for_tests().db_for_tests();
        let seq = db.last_applied().map_or(0, |s| s + 1);
        db.apply_shipped(&[(seq, legacy_emit_record("incident_opened", &v2_bytes))])
            .unwrap();
    }
    let legacy: Vec<EventRow> = kernel.app().db().snapshot().unwrap().scan_from(1, 1).unwrap();
    assert_eq!(legacy[0].shape, None, "a 0x03 record materializes with no shape");

    let (_, sinks) = run_script(
        &mut kernel,
        &sim,
        vec![Request::new(
            Method::Get,
            format!("/v2/incidents-and-pongs?cursor={}", cursor_of(&rows[0])),
        )],
    );
    assert_eq!(frames_of(&sinks.lock().unwrap()[0]), vec![reshaped_frame(&legacy[0])]);

    let (responses, _) = run_script(
        &mut kernel,
        &sim,
        vec![Request::new(
            Method::Get,
            format!("/v2/incidents-and-pongs?cursor={}", cursor_of(&legacy[0])),
        )],
    );
    let responses = responses.lock().unwrap().clone();
    assert_eq!(responses[0].0, 409);
    assert_eq!(
        error_text(&responses[0].1),
        reshaped_409(&legacy[0], <shapes_v2::IncidentOpened as Event>::FINGERPRINT)
    );
}
```

In `crates/charpente/src/serve.rs`'s test module, add the wire pin. `doc_truth` needs this literal: it is the corpus evidence for the pinned GUIDE block's `event:` and `data:` lines.

```rust
    /// The reshaped frame on the wire carries the foreign row's OWN id, so a
    /// client that ignores it and reconnects with `Last-Event-ID` lands on the
    /// named 409 instead of skipping the event (payload-shapes spec §3).
    #[test]
    fn a_reshaped_frame_carries_the_foreign_rows_own_id_on_the_wire() {
        let frame = kernel::EventFrame {
            id: 5,
            seq: 12,
            name: kernel::RESHAPED_FRAME,
            json: r#"{"event":"incident_opened","id":5,"fix":"re-sync current state through reads, then reconnect without a cursor"}"#.to_string(),
        };
        assert_eq!(
            std::str::from_utf8(&encode_frame(&frame)).unwrap(),
            "id: 5.12\nevent: charpente.reshaped\ndata: {\"event\":\"incident_opened\",\"id\":5,\"fix\":\"re-sync current state through reads, then reconnect without a cursor\"}\n\n"
        );
    }
```

- [ ] **Step 3: Run the behavioural tests against the pre-change delivery and record each red**

Run this before touching `http/mod.rs`, `tail.rs` or `kernel/mod.rs`. The serve.rs test cannot compile yet: comment it out for this run, then restore it.

```zsh
cargo test -p charpente --test realtime -- reshaped refused not_refused foreign 2>&1 | tee .superpowers/sdd/2026-09-24-payload-shapes/t2-red.log | grep -E "^test |panicked|FAILED"
```

Expected red, and quote it in the report:
- The probe panics with "no step may fail — nothing a subscriber does stops the node: … needed 8 bytes for u64, 0 left …".
- The swap test's frames show `{"b":"1","a":"101"}`.
- The drop test's frames show `{"a":"1"}`.
- The keyed test passes (there is no shape check to misorder yet). Note that: its red comes from the mutation in Step 5.
- The unsubscribed-name test passes for the same reason.
- The pre-shape test's frame path panics (the same decode crash) or delivers.

- [ ] **Step 4: Implement**

`crates/charpente/src/http/mod.rs`:

```rust
pub struct SubEntry {
    pub name: &'static str,
    pub encode_json: fn(&[u8]) -> io::Result<String>,
    /// The subscribed type's `Event::FINGERPRINT`: delivery decodes a stored
    /// row with `encode_json` only when the row's recorded shape equals this
    /// (payload-shapes spec §3).
    pub fingerprint: u64,
}
```

`Subscription::of` and `and` each set `fingerprint: Ev::FINGERPRINT`. `realtime.rs`'s `FailingFeed` entry gains `fingerprint: <NoteAdded as Event>::FINGERPRINT`, because that feed's name is `note_added`.

`crates/charpente/src/kernel/tail.rs`:

```rust
/// The terminal frame a tail gets instead of a decode when a stored event's
/// recorded shape is not the subscribed type's (payload-shapes spec §3). It
/// carries the foreign row's own `id.seq`, so a client that ignores it and
/// reconnects with that cursor meets `validate_subscribe`'s named 409 rather
/// than resuming past an event it never received.
pub const RESHAPED_FRAME: &str = "charpente.reshaped";
```

In `deliver_unit`, after the entry lookup and before the frame is built:

```rust
        if row.shape != Some(entry.fingerprint) {
            // Never decoded: this binary cannot prove those bytes are its own
            // shape. The tail ends here, the node does not, and every other
            // tail is untouched — the `charpente.gap` lifecycle.
            let _ = tail.sink.send(&EventFrame {
                id: row.id.raw(),
                seq: row.seq,
                name: RESHAPED_FRAME,
                json: format!(
                    "{{\"event\":\"{}\",\"id\":{},\"fix\":\"re-sync current state through reads, then reconnect without a cursor\"}}",
                    entry.name,
                    row.id.raw()
                ),
            });
            return Ok(false);
        }
```

`crates/charpente/src/kernel/mod.rs`:
- the re-export becomes `pub use tail::{EventFrame, EventSink, RESHAPED_FRAME, SimSink, SimSinkHandle, SinkStatus, TAIL_BATCH_EVENTS};`;
- the call site becomes `validate_subscribe(self.app.db(), &sub)`;
- `validate_subscribe` takes `sub: &Subscription`, reads `let Some(cursor) = sub.cursor else { … }` (`Cursor` is `Copy`), and inside the matching arm, after the `row.seq != cursor.seq` refusal and before `Ok(cursor.id + 1)`, gets:

```rust
            // The row the client last saw is one this binary refuses to
            // decode. Resuming past it would skip the very event the
            // `charpente.reshaped` frame named — the frame carries this id, so
            // a client that ignored it reconnects exactly here. A name the
            // subscription does not carry, or a row its key filter excludes,
            // is never delivered to it either way, and is not refused
            // (payload-shapes spec §3).
            let key_passes = sub.key.is_none() || sub.key.as_deref() == row.key.as_deref();
            if let Some(entry) = sub.entries.iter().find(|e| e.name == row.name)
                && key_passes
                && row.shape != Some(entry.fingerprint)
            {
                let recorded = match row.shape {
                    Some(shape) => format!("{shape:#x}"),
                    None => "none — written before event shapes were tagged".to_string(),
                };
                return Err(Response::error(
                    409,
                    &format!(
                        "cursor {} sits on an \"{}\" event written under a different shape of \
                         that event (recorded {recorded}; this binary {:#x}) — this binary will \
                         not decode it; re-sync current state through reads, then reconnect \
                         without a cursor",
                        cursor.wire(),
                        entry.name,
                        entry.fingerprint
                    ),
                ));
            }
```

Update `validate_subscribe`'s doc comment: the checks are now floor, divergence, then reshaped.

In the flagship `Client` frame loop in `realtime.rs` (where `frame.name == "charpente.gap"` is handled, ~684), add before `assert_strictly_increasing`:

```rust
                if frame.name == RESHAPED_FRAME {
                    // Every event in this workload has one shape, and the
                    // matrix's corruption axes (`bit_flip`,
                    // `misdirected_write`) are wal/-scoped and checksummed —
                    // a hit surfaces as a cut, never as a stored row that
                    // decodes wrong. So the platform has no reason to send
                    // this here; if it does, that is a finding to
                    // adjudicate, not an excuse to add.
                    return Err(format!(
                        "a tail ended on {} in a workload with one shape per event: {}",
                        frame.name, frame.json
                    ));
                }
```

- [ ] **Step 5: Run, then mutation-prove the ordering tests**

```zsh
cargo test -p charpente --test realtime --no-fail-fast 2>&1 | tee .superpowers/sdd/2026-09-24-payload-shapes/t2-realtime.log | grep -E "^test result|FAILED|panicked"
cargo test -p charpente --lib -- serve::tests::a_reshaped_frame --exact
```

Expected: all pass, the flagship included. Then three hand mutations, one at a time. Commit first; run only the named test for each; restore with `git checkout -- <file>`; confirm `git diff` is empty after each.

| Mutation | Must go red |
|---|---|
| In `deliver_unit`, move the shape check above the key filter | `another_keys_reshaped_event_neither_ends_nor_refuses_this_tail` (its first sink ends on org-b's row) |
| In `validate_subscribe`, drop `&& key_passes` | the same test (its second subscribe is refused) |
| In `validate_subscribe`, replace `sub.entries.iter().find(|e| e.name == row.name)` with `sub.entries.first()` | `a_cursor_on_a_foreign_row_of_an_unsubscribed_event_is_not_refused` |

Quote each red in the report.

- [ ] **Step 6: Docs**

`docs/GUIDE.md` §11: add a new subsection `### Changing an event`, placed after `### The wire` and before `### Testing a tail in sim`:

````markdown
### Changing an event

Every emit records its payload's shape — `Event::FINGERPRINT`, the structural
fingerprint `#[event]` computes from the fields (§12) — and delivery decodes a stored
event only under the shape it was written with. Change an event's fields, and every
retained row of the old shape is one this binary will not decode. There is no per-row
versioned decode and no override.

A live tail that reaches an old-shape row gets one terminal frame instead of a wrong
or crashing decode, and its connection closes — the `charpente.gap` lifecycle. Its
`id:` line is that row's own `<id>.<seq>`:

<!-- pinned -->
```text
event: charpente.reshaped
data: {"event":"<event>","id":<id>,"fix":"re-sync current state through reads, then reconnect without a cursor"}
```

A client that ignores the frame — a browser's `EventSource` reconnects on its own,
with `Last-Event-ID` set to that id — is refused at subscribe, so it cannot resume
past an event it never received:

<!-- pinned -->
```text
cursor <id>.<seq> sits on an "<event>" event written under a different shape of that event (recorded <recorded>; this binary <fingerprint>) — this binary will not decode it; re-sync current state through reads, then reconnect without a cursor
```

`<recorded>` reads `none — written before event shapes were tagged` for a row emitted
before shapes were recorded, and such a row is refused like any other foreign one. A
cursor on a foreign row of an event the subscription does not carry, or one its key
filter excludes, is not refused: the subscription would never deliver that row anyway.
The node keeps serving throughout, and every other tail is untouched.

**To keep exact resume across a deploy, do not change an existing event's fields.**
Add a new `#[event]` type with the new fields, emit it from the new code, and keep the
old type in code — and in any subscription that still needs it — for as long as its
rows are retained (`DbConfig::event_retention`). Old rows keep decoding under their
own type. Changing the fields in place costs every subscriber positioned before the
change one re-sync. What the fingerprint cannot see, a change inside a type a field
holds, is in §12.
````

In "Cursor semantics":
- add a fourth bullet after the two "Diverged" cases: "- **Reshaped** — the retained row at that id is an event this subscription carries, written under a different shape of it (see "Changing an event" below)."
- change "Every one of these three named errors" to "Every one of these four named errors".

In "Delivery", after the gap frame paragraph, add: "A tail that reaches an event written under a different shape of its type ends the same way, with `charpente.reshaped` (see "Changing an event"). Neither frame stops the node."

`DESIGN.md` §6, in "Honesty at the edges", add after the gap sentence: "A stored event whose recorded shape (§3) is not the subscribed type's is never decoded: the tail gets a terminal `event: charpente.reshaped` frame carrying that row's own `id.seq`, and a subscribe whose cursor sits on such a row — for a name the subscription carries and a key it passes — is refused with a 409 naming the event, both shapes and the same re-sync fix."

`crates/guide-examples/tests/doc_truth.rs`: `const PINNED_FLOOR: usize = 18;`. Extend the constant's doc comment: "16 at landing (2026-09-16); 18 with payload shapes' reshaped frame and 409 (2026-09-2x)".

- [ ] **Step 7: Gate and commit**

```zsh
cargo test -p guide-examples --no-fail-fast 2>&1 | grep -E "^test result|FAILED"
```

Then the counted gate, then:

```zsh
git add crates/charpente/src/http/mod.rs crates/charpente/src/kernel/tail.rs crates/charpente/src/kernel/mod.rs crates/charpente/src/serve.rs crates/charpente/tests/realtime.rs docs/GUIDE.md DESIGN.md crates/guide-examples/tests/doc_truth.rs
git commit -m "feat(payload-shapes): a reshaped event ends its tail with charpente.reshaped and its cursor gets a named 409 — never a decode, never a stopped node"
```

---

### Task 3: Events — corruption and unencodable payloads fail their own tail (`charpente.error`)

**Files:**
- Modify `crates/charpente/src/http/mod.rs:257` (`ev_json`).
- Modify `crates/charpente/src/kernel/tail.rs` (`ERROR_FRAME`, the scan arm, the encode arm).
- Modify `crates/charpente/src/kernel/mod.rs:11` (re-export).
- Modify `crates/charpente/src/serve.rs:380-388` (`encode_frame`) and its tests.
- Test `crates/charpente/tests/realtime.rs`: the flipped encode test, corruption tests, and the flagship arm.
- Docs: `docs/GUIDE.md` §11 (the JSON paragraph near `failed JSON encoding`, Delivery, The wire), `DESIGN.md` §6 (the "kernel-fatal" sentence), `doc_truth.rs` (`PINNED_FLOOR` 18 → 19).

**Interfaces:**
- Consumes (Task 2): `SubEntry.fingerprint`, `RESHAPED_FRAME`, and the `realtime.rs` helpers.
- Produces:
  - `pub const ERROR_FRAME: &str = "charpente.error";`, re-exported from `charpente::kernel`.
  - `deliver_unit` returns `Err` only for the platform's own store failures (`ErrorKind` other than `InvalidData`).
  - The error-frame JSON is `{"event":"<name>","id":<id>,"error":"<text>"}` for a payload, and `{"id":<first undelivered id>,"error":"<text>"}` for a row that does not decode at all.

- [ ] **Step 1: Verify spec amendment A10 before writing any prose about it**

Predict first: `serde_json::to_string(&f64::NAN)` returns `Ok("null")`, and a `BTreeMap<Vec<u8>, u8>` with one entry returns `Err("key must be a string")`. Then check it. Put this temporary test in `crates/charpente/src/http/mod.rs`'s test module, run it, record both results in the report, and delete it:

```rust
    #[test]
    fn scratch_what_serde_json_refuses() {
        eprintln!("NaN -> {:?}", serde_json::to_string(&f64::NAN).map_err(|e| e.to_string()));
        let mut m = std::collections::BTreeMap::new();
        m.insert(vec![1u8], 1u8);
        eprintln!("bytes-keyed map -> {:?}", serde_json::to_string(&m).map_err(|e| e.to_string()));
    }
```

```zsh
cargo test -p charpente --lib -- scratch_what_serde_json_refuses --nocapture
```

Write Step 6's prose from what this prints. If NaN does fail, A10 is wrong: strike it from the spec's amendment list in this commit and keep "a `NaN`" in the prose.

- [ ] **Step 2: Write the failing tests**

In `realtime.rs`, replace `tail_encode_error_is_whole_node_fatal`, and update its doc block above `struct FailingFeed` to describe the new contract:

```rust
/// Spec §3/§6.5: an encode failure fails its own tail, never the node — this
/// test was `tail_encode_error_is_whole_node_fatal`, the pin of the decision
/// payload shapes reversed (docs/LEDGER.md:286's per-tail follow-up). The
/// failing encoder is hand-built (`FailingFeed`), because no `#[event]` the
/// macro accepts can make `Subscription::of`'s real encoder fail on its own
/// bytes. The stream ends with `charpente.error`, and the node keeps serving.
#[test]
fn an_unencodable_event_fails_its_tail_not_the_node() {
    let sim = Sim::with_faults(0x2EB2_FA11, FaultConfig::none());
    let app = App::open(
        Box::new(sim.disk()),
        Schema::new(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )
    .unwrap()
    .mount(vec![FailingFeed::route()])
    .mount(routes![emit_note]);
    let mut kernel = Kernel::new(app, Box::new(sim.clock())).unwrap();
    let (_, sinks) = run_script(
        &mut kernel,
        &sim,
        vec![
            Request::new(Method::Get, "/feed-failing"),
            Request::new(Method::Post, "/note").json(&NoteBody { n: WireU64(1) }),
        ],
    );
    let rows: Vec<EventRow> = kernel.app().db().snapshot().unwrap().scan_from(0, 8).unwrap();
    assert_eq!(
        frames_of(&sinks.lock().unwrap()[0]),
        vec![(
            rows[0].id.raw(),
            rows[0].seq,
            "charpente.error".to_string(),
            format!(
                r#"{{"event":"note_added","id":{},"error":"encode exploded"}}"#,
                rows[0].id.raw()
            ),
        )],
        "one terminal error frame naming the event and the row"
    );
    let (responses, _) = run_script(
        &mut kernel,
        &sim,
        vec![Request::new(Method::Post, "/note").json(&NoteBody { n: WireU64(2) })],
    );
    assert_eq!(statuses(&responses), vec![200], "the node keeps serving");
}
```

`FailingFeed`'s glue builds `Subscription { entries, key, cursor }` by hand. Its `SubEntry` literal gained `fingerprint` in Task 2.

Add the corruption tests, plus a shaped-record helper:

```rust
/// A record exactly as a shaped binary writes it: one `0x09` emit.
fn shaped_emit_record(event: &str, payload: &[u8], shape: u64) -> Vec<u8> {
    let mut out = Vec::new();
    1u32.encode(&mut out);
    out.push(9);
    event.to_string().encode(&mut out);
    payload.to_vec().encode(&mut out);
    None::<String>.encode(&mut out);
    shape.encode(&mut out);
    out
}

/// Applies one shipped record to a live kernel's store, the way a follower
/// applies a frame — the only way a test can plant bytes the typed API would
/// never write.
fn apply_record(kernel: &mut Kernel, record: Vec<u8>) {
    let db = kernel.app_mut_for_tests().db_for_tests();
    let seq = db.last_applied().map_or(0, |s| s + 1);
    db.apply_shipped(&[(seq, record)]).unwrap();
}

/// Spec §6.5: the shape matches but the bytes do not decode — corruption, not
/// a reshape. The frame has no `id:` on the wire (Task 3's serve.rs pin), so
/// the client's cursor stays before the row, and each reconnect fails at the
/// same row, the way a corrupt table row answers 500 on every read.
#[test]
fn a_corrupt_payload_under_a_matching_shape_fails_its_tail_by_name() {
    let (sim, mut kernel, rows) = mixed_log(0x5A9E_0301, &[("/pong", 0, None)]);
    let v2 = <shapes_v2::IncidentOpened as Event>::FINGERPRINT;
    // v2's payload is 16 bytes; 8 is a truncation under a matching shape.
    apply_record(&mut kernel, shaped_emit_record("incident_opened", &[0u8; 8], v2));
    let expected_error = format!(
        "event incident_opened failed to decode although its recorded shape matches this \
         binary ({v2:#x}): row decode: needed 8 bytes for u64, 0 left — schema drift or \
         corruption — the stored bytes are corrupt, not a shape change; restore this node \
         from its archive (GUIDE §15) or re-seed it from its primary (GUIDE §10)"
    );
    for _ in 0..2 {
        let (_, sinks) = run_script(
            &mut kernel,
            &sim,
            vec![Request::new(
                Method::Get,
                format!("/v2/incidents-and-pongs?cursor={}", cursor_of(&rows[0])),
            )],
        );
        let frames = frames_of(&sinks.lock().unwrap()[0]);
        assert_eq!(frames.len(), 1, "{frames:?}");
        assert_eq!(frames[0].2, "charpente.error");
        let body: serde_json::Value = serde_json::from_str(&frames[0].3).unwrap();
        assert_eq!(body["event"], "incident_opened");
        assert_eq!(body["id"], 1);
        assert_eq!(body["error"], expected_error.as_str(), "the same named error on every reconnect");
    }
    let (responses, _) = run_script(
        &mut kernel,
        &sim,
        vec![Request::new(Method::Post, "/pong").json(&EmitBody { n: WireU64(5), key: None })],
    );
    assert_eq!(statuses(&responses), vec![200], "the node keeps serving");
}

/// `ev_json` checks trailing bytes now: a matching shape with bytes left over
/// is corruption too, never a silent short decode.
#[test]
fn leftover_bytes_under_a_matching_shape_fail_the_tail_by_name() {
    let (sim, mut kernel, rows) = mixed_log(0x5A9E_0302, &[("/pong", 0, None)]);
    let v2 = <shapes_v2::IncidentOpened as Event>::FINGERPRINT;
    apply_record(&mut kernel, shaped_emit_record("incident_opened", &[0u8; 24], v2));
    let (_, sinks) = run_script(
        &mut kernel,
        &sim,
        vec![Request::new(
            Method::Get,
            format!("/v2/incidents-and-pongs?cursor={}", cursor_of(&rows[0])),
        )],
    );
    let frames = frames_of(&sinks.lock().unwrap()[0]);
    let body: serde_json::Value = serde_json::from_str(&frames[0].3).unwrap();
    assert_eq!(
        body["error"],
        format!(
            "event incident_opened failed to decode although its recorded shape matches this \
             binary ({v2:#x}): 8 bytes left over after the payload — the stored bytes are \
             corrupt, not a shape change; restore this node from its archive (GUIDE §15) or \
             re-seed it from its primary (GUIDE §10)"
        )
        .as_str()
    );
}

/// Review focus 5 (spec amendment A11): a stored `EventRow` whose own bytes do
/// not decode fails its tail, not the node. Planted through a shipped `0x04`
/// migrate-put — the decode-free raw write — over a real row id, so the
/// counter says the row exists and the tail reaches it.
#[test]
fn a_corrupt_event_row_fails_its_tail_not_the_node() {
    let (sim, mut kernel, rows) = mixed_log(0x5A9E_0303, &[("/pong", 0, None), ("/pong", 1, None)]);
    let mut record = Vec::new();
    1u32.encode(&mut record);
    record.push(4);
    "charpente.events".to_string().encode(&mut record);
    rows[1].id.raw().encode(&mut record);
    vec![0xFFu8; 3].encode(&mut record);
    apply_record(&mut kernel, record);
    let (_, sinks) = run_script(
        &mut kernel,
        &sim,
        vec![Request::new(Method::Get, format!("/pongs?cursor={}", cursor_of(&rows[0])))],
    );
    let frames = frames_of(&sinks.lock().unwrap()[0]);
    assert_eq!(frames.len(), 1, "{frames:?}");
    assert_eq!(frames[0].2, "charpente.error");
    let body: serde_json::Value = serde_json::from_str(&frames[0].3).unwrap();
    assert_eq!(body["id"], rows[1].id.raw());
    assert_eq!(
        body["error"],
        format!(
            "event rows from id {} could not be decoded: row decode: needed 8 bytes for u64, 3 \
             left — schema drift or corruption — the stored bytes are corrupt; restore this node \
             from its archive (GUIDE §15) or re-seed it from its primary (GUIDE §10)",
            rows[1].id.raw()
        )
        .as_str()
    );
}
```

The inner codec texts ("row decode: needed 8 bytes for u64, 0 left — schema drift or corruption") come from `charpente-core/src/table.rs:140`. Confirm them from the first run's output. The planting route is sound: the apply arm for `0x04` writes with `put_raw` and never decodes (`db.rs` "still decode-free"), and `charpente.events` is registered in every `Db`'s schema at open (`db.rs:~2091`, `.table::<EventRow>()`). If apply refuses it anyway, stop and report; do not invent a test-only write API.

In `serve.rs`'s test module:

```rust
    /// `charpente.error` has NO `id:` line (payload-shapes spec §3, A7): a
    /// client's `Last-Event-ID` must not advance past an event it never
    /// received, so a reconnect fails at the same row rather than skipping it.
    #[test]
    fn an_error_frame_carries_no_id_on_the_wire() {
        let frame = kernel::EventFrame {
            id: 5,
            seq: 12,
            name: kernel::ERROR_FRAME,
            json: r#"{"event":"incident_opened","id":5,"error":"boom"}"#.to_string(),
        };
        assert_eq!(
            std::str::from_utf8(&encode_frame(&frame)).unwrap(),
            "event: charpente.error\ndata: {\"event\":\"incident_opened\",\"id\":5,\"error\":\"boom\"}\n\n"
        );
    }
```

- [ ] **Step 3: Run the tests against the pre-change code and record each red**

Comment out the serve.rs test for this run.

```zsh
cargo test -p charpente --test realtime -- an_unencodable a_corrupt leftover 2>&1 | tee .superpowers/sdd/2026-09-24-payload-shapes/t3-red.log | grep -E "^test |panicked"
```

Expected red:
- the encode test panics "no step may fail … encode exploded";
- the payload-corruption test panics "no step may fail … needed 8 bytes";
- the leftover test's frame is a delivered `incident_opened` (no trailing check);
- the row-corruption test panics "no step may fail … needed 8 bytes for u64, 3 left".

- [ ] **Step 4: Implement**

`crates/charpente/src/http/mod.rs`:

```rust
/// Only ever called on a row whose recorded shape is `Ev::FINGERPRINT`
/// (`deliver_unit` checks first), so a decode failure here — a short read or
/// bytes left over — is corrupt storage, not a shape change, and says so.
fn ev_json<Ev: Event + serde::Serialize>(bytes: &[u8]) -> io::Result<String> {
    let corrupt = |detail: String| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "event {} failed to decode although its recorded shape matches this binary \
                 ({:#x}): {detail} — the stored bytes are corrupt, not a shape change; restore \
                 this node from its archive (GUIDE §15) or re-seed it from its primary (GUIDE §10)",
                Ev::NAME,
                Ev::FINGERPRINT
            ),
        )
    };
    let mut input = bytes;
    let ev = Ev::decode(&mut input).map_err(|e| corrupt(e.to_string()))?;
    if !input.is_empty() {
        return Err(corrupt(format!("{} bytes left over after the payload", input.len())));
    }
    serde_json::to_string(&ev).map_err(|e| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "event {} failed JSON encoding: {e} — every #[event] payload must be plainly \
                 JSON (GUIDE §11); fix the emitting code",
                Ev::NAME
            ),
        )
    })
}
```

`crates/charpente/src/kernel/tail.rs`:

```rust
/// The terminal frame for an event this node holds but cannot render: its
/// bytes are corrupt, or its payload does not encode as JSON (payload-shapes
/// spec §3). It fails this tail and never the node. The wire form carries no
/// `id:` line (serve.rs `encode_frame`), so the client's cursor does not
/// advance past the event and every reconnect fails at the same row.
pub const ERROR_FRAME: &str = "charpente.error";
```

In `deliver_unit`, replace the scan line:

```rust
    let rows: Vec<EventRow> = match db.snapshot()?.scan_from(tail.next_id, TAIL_BATCH_EVENTS) {
        Ok(rows) => rows,
        // A stored row `EventRow` cannot decode is corrupt bytes, which
        // subscribers can reach but did not cause: it fails this tail, never
        // the node (spec amendment A11). `InvalidData` is exactly the codec's
        // kind; redb's own failures are `Other` and still bubble, because a
        // failing store is the platform's to stop on.
        Err(e) if e.kind() == io::ErrorKind::InvalidData => {
            let error = format!(
                "event rows from id {} could not be decoded: {e} — the stored bytes are corrupt; \
                 restore this node from its archive (GUIDE §15) or re-seed it from its primary \
                 (GUIDE §10)",
                tail.next_id
            );
            let _ = tail.sink.send(&EventFrame {
                id: tail.next_id,
                seq: 0,
                name: ERROR_FRAME,
                json: format!(
                    "{{\"id\":{},\"error\":{}}}",
                    tail.next_id,
                    serde_json::Value::String(error)
                ),
            });
            return Ok(false);
        }
        Err(e) => return Err(e),
    };
```

Replace the encode line and delete its "deliberately whole-node-fatal" comment:

```rust
        let json = match (entry.encode_json)(&row.payload) {
            Ok(json) => json,
            Err(e) => {
                let _ = tail.sink.send(&EventFrame {
                    id: row.id.raw(),
                    seq: row.seq,
                    name: ERROR_FRAME,
                    json: format!(
                        "{{\"event\":\"{}\",\"id\":{},\"error\":{}}}",
                        entry.name,
                        row.id.raw(),
                        serde_json::Value::String(e.to_string())
                    ),
                });
                return Ok(false);
            }
        };
        let frame = EventFrame {
            id: row.id.raw(),
            seq: row.seq,
            name: entry.name,
            json,
        };
```

`deliver_unit`'s doc gains: "Returns `Err` only for the platform's own store failures; nothing a subscriber can reach stops the node." Add `ERROR_FRAME` to the `kernel/mod.rs` re-export.

In `crates/charpente/src/serve.rs`:

```rust
/// Wire form (spec): `id: <id>.<seq>` — matching `Cursor::wire` exactly is
/// what makes the browser's native `Last-Event-ID` resume work — then
/// `event:`, `data:`, and the blank line ending the frame. `charpente.error`
/// alone carries no `id:` (payload-shapes spec §3): the client's cursor must
/// not advance past an event it never received.
fn encode_frame(frame: &kernel::EventFrame) -> Bytes {
    if frame.name == kernel::ERROR_FRAME {
        return Bytes::from(format!("event: {}\ndata: {}\n\n", frame.name, frame.json));
    }
    Bytes::from(format!(
        "id: {}.{}\nevent: {}\ndata: {}\n\n",
        frame.id, frame.seq, frame.name, frame.json
    ))
}
```

The flagship `Client` arm from Task 2 becomes `if frame.name == RESHAPED_FRAME || frame.name == ERROR_FRAME {`, with the same body.

- [ ] **Step 5: Run**

```zsh
cargo test -p charpente --test realtime --no-fail-fast 2>&1 | tee .superpowers/sdd/2026-09-24-payload-shapes/t3-realtime.log | grep -E "^test result|FAILED|panicked"
cargo test -p charpente --lib -- serve::tests --no-fail-fast 2>&1 | grep -E "^test result|FAILED"
```

- [ ] **Step 6: Docs**

`docs/GUIDE.md` §11: replace the paragraph starting "One contract rides along with the `Serialize` derive" through "Keep event payloads to plainly-JSON shapes." with the following. It is written for A10 as verified; adjust the first two sentences if Step 1 showed otherwise.

````markdown
One contract rides along with the `Serialize` derive: every `#[event]` payload must be
plainly JSON. serde_json refuses a map whose keys are not strings, and it does not
refuse a `NaN` or infinite float — it writes `null`, so a client receives `null` where
a number was. Neither is caught at emit time (the log stores `Codec` bytes; JSON
encoding happens at delivery). An event that fails to encode fails its own tail and
never the node: the stream ends with one terminal frame, the same frame a stored event
whose bytes are corrupt gets:

<!-- pinned -->
```text
event: charpente.error
data: {"event":"<event>","id":<id>,"error":"<message>"}
```

The frame has no `id:` line, so the client's `Last-Event-ID` stays before the event
and every reconnect ends at the same row, the way a corrupt table row answers 500 on
every read. The `error` text names the event and the fix: for an encode failure,
`event <name> failed JSON encoding: <serde_json's message> — every #[event] payload
must be plainly JSON (GUIDE §11); fix the emitting code`; for corrupt bytes, restore
the node from its archive (§15) or re-seed it from its primary (§10). A row whose own
envelope is corrupt gets the same frame, with `id` naming the first event this tail had
not yet received and no `event`.
````

"The wire": append "— except `charpente.error`, which carries no `id:` line (above)."

"Delivery": add `charpente.error` beside `charpente.reshaped` in the sentence Task 2 added: "…ends the same way, with `charpente.reshaped` … and an event that cannot be rendered ends it with `charpente.error`. None of these frames stops the node."

`DESIGN.md` §6: replace "Event payloads must be JSON-serializable — … delivering the offending durable event is kernel-fatal (the encode error stops the loop; a restart re-encounters it)." with:

> Event payloads must be plainly JSON — serde_json refuses a map with non-string keys (and writes a non-finite float as `null`). An event that fails to encode, or whose stored bytes are corrupt, ends that subscriber's tail with a terminal `charpente.error` frame that carries no `id:`, so the client's cursor does not advance past it. No delivery path stops the kernel; only the platform's own store failures do.

`doc_truth.rs`: `PINNED_FLOOR` becomes 19, with the comment extended.

- [ ] **Step 7: Gate and commit**

```zsh
git add crates/charpente/src/http/mod.rs crates/charpente/src/kernel/tail.rs crates/charpente/src/kernel/mod.rs crates/charpente/src/serve.rs crates/charpente/tests/realtime.rs docs/GUIDE.md DESIGN.md crates/guide-examples/tests/doc_truth.rs docs/superpowers/specs/2026-09-24-payload-shapes-design.md
git commit -m "feat(payload-shapes): corrupt and unencodable events fail their own tail with charpente.error — no delivery path stops the node"
```

---

### Task 4: The seeded property — a resume across mixed shapes is exact or named, under the fault matrix

**Files:**
- Test `crates/charpente/tests/realtime.rs`: `Era`, `shapes_boot`, `shapes_workload`, `shapes_verdict`, and the test.

**Interfaces:**
- Consumes (Tasks 1–3): `shapes_app`, `shapes_v2`, `RESHAPED_FRAME`, `reshaped_409`, `stall_closure`, `SETTLE_BOOT_TRIES`, `SETTLE_CYCLE_EVERY`.
- Produces: the coverage keys `shapes:reshaped-frame`, `shapes:reshaped-409` and `shapes:v2-delivered`.

- [ ] **Step 1: Write the workload**

```rust
// ------------------------------------ mixed shapes under the fault matrix

/// How many resume attempts a universe gets through injected faults before it
/// gives up. Only an attempt whose `kernel.run` returns without fault fallout
/// is adjudicated.
const SHAPES_SETTLE_ATTEMPTS: u32 = 12;

#[derive(Clone, Copy, Debug)]
enum Era {
    /// The old binary: `incident_opened` in its v1 shape.
    V1,
    /// The new binary: `incident_opened` in its v2 shape.
    V2,
}

/// `settle_boot` over `shapes_app` — the file's own boot-through-faults idiom,
/// copied like `scoped_settle_boot` rather than parameterized, so the
/// flagship's boot path is untouched.
fn shapes_boot(sim: &Sim) -> Result<Option<Kernel>, String> {
    for try_n in 0..SETTLE_BOOT_TRIES {
        if try_n > 0 && try_n.is_multiple_of(SETTLE_CYCLE_EVERY) {
            sim.restart();
        }
        let app = match shapes_app(sim) {
            Ok(app) => app,
            Err(e) if is_fault_fallout(&e.to_string()) => continue,
            Err(e) => return Err(format!("shapes open failed non-injected: {e}")),
        };
        match Kernel::new(app, Box::new(sim.clock())) {
            Ok(kernel) => return Ok(Some(kernel)),
            Err(e) if is_fault_fallout(&e.to_string()) => {}
            Err(e) => return Err(format!("shapes Kernel::new failed non-injected: {e}")),
        }
    }
    Ok(None)
}

/// One universe of spec §6.3. The old binary emits `incident_opened` in its v1
/// shape (and pongs); the node restarts as the new binary, which emits the v2
/// shape; then a client resumes `/v2/incidents-and-pongs` from a cursor drawn
/// from the durable log. Every phase runs under the fault matrix. The verdict
/// is taken against that log.
fn shapes_workload(sim: &mut Sim) -> Result<(), String> {
    let entropy = sim.entropy();
    for era in [Era::V1, Era::V2] {
        if sim.gave_up().is_some() {
            return Ok(());
        }
        sim.crash_disk_after(64 + entropy.next_u64() % 512);
        if let Some(mut kernel) = shapes_boot(sim)? {
            let mut source =
                SimSource::new(Box::new(sim.clock()), sim.now().as_nanos() + 60 * SEC);
            let t = sim.now().as_nanos();
            for i in 0..(2 + entropy.next_u64() % 5) {
                let path = match (era, entropy.next_u64() % 3) {
                    (_, 0) => "/pong",
                    (Era::V1, _) => "/v1/incident",
                    (Era::V2, _) => "/v2/incident",
                };
                source.push_at(
                    t + i * SEC,
                    Request::new(Method::Post, path).json(&EmitBody {
                        n: WireU64(entropy.next_u64() % 1000),
                        key: None,
                    }),
                );
            }
            if let Err(e) = kernel.run(&mut source)
                && !is_fault_fallout(&e.to_string())
            {
                return Err(format!("{era:?} kernel.run failed non-injected: {e}"));
            }
        }
        // The deploy between eras — or the crash, whichever came first.
        sim.restart();
    }
    for _ in 0..SHAPES_SETTLE_ATTEMPTS {
        if sim.gave_up().is_some() {
            return Ok(());
        }
        let Some(mut kernel) = shapes_boot(sim)? else {
            sim.restart();
            continue;
        };
        let rows: Vec<EventRow> =
            match kernel.app().db().snapshot().and_then(|s| s.scan_from(0, 1024)) {
                Ok(rows) => rows,
                Err(e) if is_fault_fallout(&e.to_string()) => {
                    sim.restart();
                    continue;
                }
                Err(e) => return Err(format!("event scan failed non-injected: {e}")),
            };
        if rows.is_empty() {
            sim.give_up("no event survived the two eras — nothing to resume across");
            return Ok(());
        }
        // `EventRow` is not `Clone`: the anchor is an index into `rows`.
        let anchor_at = (entropy.next_u64() % rows.len() as u64) as usize;
        let mut source = SimSource::new(Box::new(sim.clock()), sim.now().as_nanos() + 60 * SEC);
        source.push_stall(stall_closure(sim.entropy(), sim.subscriber_stall()));
        source.push_at(
            sim.now().as_nanos(),
            Request::new(
                Method::Get,
                format!("/v2/incidents-and-pongs?cursor={}", cursor_of(&rows[anchor_at])),
            ),
        );
        let (responses, sinks) = (source.responses(), source.sinks());
        match kernel.run(&mut source) {
            Ok(()) => return shapes_verdict(sim, &rows, &rows[anchor_at], &responses, &sinks),
            Err(e) if is_fault_fallout(&e.to_string()) => sim.restart(),
            Err(e) => return Err(format!("resume kernel.run failed non-injected: {e}")),
        }
    }
    sim.give_up(format!(
        "no resume completed without fault fallout in {SHAPES_SETTLE_ATTEMPTS} attempts"
    ));
    Ok(())
}
```

`kernel.run` returns `Ok` only once the source's horizon passes. A resume whose tail met an injected IO fault returns `Err`, is classified as fallout and retried, so only a clean run reaches `shapes_verdict`.

```rust
/// The oracle, against the retained log `rows` the resume ran over. A cursor
/// ON a foreign row is refused with the named 409. Otherwise the frames are a
/// prefix of exactly what a resume must produce — every later v2 or pong row,
/// decoded, up to the first foreign row, which arrives as one
/// `charpente.reshaped` frame that ends the stream. A strict prefix is legal
/// only when the sink stalled. Nothing is skipped and nothing is decoded from
/// a foreign row.
fn shapes_verdict(
    sim: &Sim,
    rows: &[EventRow],
    anchor: &EventRow,
    responses: &RecordedResponses,
    sinks: &SinkHandles,
) -> Result<(), String> {
    let v2 = <shapes_v2::IncidentOpened as Event>::FINGERPRINT;
    let foreign = |row: &EventRow| row.name == "incident_opened" && row.shape != Some(v2);
    let responses = responses.lock().expect("responses lock").clone();
    let [(status, body)] = responses.as_slice() else {
        return Err(format!("expected exactly the subscribe's response, got {responses:?}"));
    };
    if foreign(anchor) {
        let expected = reshaped_409(anchor, v2);
        if *status != 409 || error_text(body) != expected {
            return Err(format!(
                "a cursor on foreign row {} must be refused 409 {expected:?}, got {status} {}",
                anchor.id.raw(),
                String::from_utf8_lossy(body)
            ));
        }
        sim.cover("shapes:reshaped-409");
        return Ok(());
    }
    if *status != 200 {
        return Err(format!(
            "a resume from a v2 or pong cursor must open the stream, got {status} {}",
            String::from_utf8_lossy(body)
        ));
    }
    let mut expected = Vec::new();
    for row in rows.iter().filter(|r| r.id.raw() > anchor.id.raw()) {
        if foreign(row) {
            expected.push(reshaped_frame(row));
            break;
        }
        let json = match row.name.as_str() {
            "incident_opened" => serde_json::to_string(
                &shapes_v2::IncidentOpened::decode(&mut &row.payload[..]).map_err(|e| e.to_string())?,
            ),
            "pong" => serde_json::to_string(
                &Pong::decode(&mut &row.payload[..]).map_err(|e| e.to_string())?,
            ),
            other => return Err(format!("this workload emits no {other:?}")),
        }
        .expect("both payloads serialize");
        expected.push((row.id.raw(), row.seq, row.name.clone(), json));
    }
    let handles = sinks.lock().expect("sinks lock");
    let [handle] = handles.as_slice() else {
        return Err(format!("expected one stream, got {}", handles.len()));
    };
    let got = frames_of(handle);
    if got.len() > expected.len() || got[..] != expected[..got.len()] {
        return Err(format!(
            "the resume from {} diverged from the log:\n  expected (a prefix of) {expected:?}\n  got {got:?}",
            cursor_of(anchor)
        ));
    }
    if got.len() < expected.len() && !handle.closed() {
        return Err(format!(
            "the resume from {} stopped short with no stall to explain it: got {got:?} of {expected:?}",
            cursor_of(anchor)
        ));
    }
    if got.last().is_some_and(|f| f.2 == RESHAPED_FRAME) {
        sim.cover("shapes:reshaped-frame");
    }
    if got.iter().any(|f| f.2 == "incident_opened") {
        sim.cover("shapes:v2-delivered");
    }
    Ok(())
}

/// Spec §6.3's property. The universe count was set by timing one batch (see
/// the commit message); the seed base is fixed and replays with
/// `CHARPENTE_SEED`/`CHARPENTE_BASE_SEED` like every swarm here.
#[test]
fn a_resume_across_mixed_shapes_is_exact_or_named_under_the_fault_matrix() {
    run_universes_swarm_with!(
        0x2EB2_5A10,
        64,
        |_cfg: &mut FaultConfig| {},
        |sim: &mut Sim| shapes_workload(sim),
        require: [
            "shapes:reshaped-frame",
            "shapes:reshaped-409",
            "shapes:v2-delivered"
        ]
    );
}
```

- [ ] **Step 2: Run it, and time it**

```zsh
/usr/bin/time -p cargo test -p charpente --test realtime -- a_resume_across_mixed_shapes --exact 2>&1 | tail -20
```

Expected: pass, with all three coverage keys reached.
- A failure is a finding: pin its seed, then adjudicate. Is it the oracle or the platform? Stop and report; do not weaken anything.
- A missing coverage key means the workload is too thin. Raise the emit counts or the universe count, not the requirement.
- Set N so the test runs in at most about 60 s of debug time, and write N in the commit message.

- [ ] **Step 3: Mutation-prove the property**

Commit first. Apply each mutation alone, run the property, expect FAILED, restore, and confirm `git diff` is empty:

| Mutation | Expected red |
|---|---|
| `deliver_unit`: delete the `row.shape != Some(entry.fingerprint)` block | an `error`-frame (or v1-decoded) mismatch against the expected reshaped frame |
| `validate_subscribe`: delete the reshaped 409 block | "a cursor on foreign row … must be refused 409" |
| `realtime.rs`: in `shapes_verdict`, change `break` after the reshaped push to `continue` (an oracle mutation: the oracle must reject a stream that continues past a foreign row) | a length or prefix mismatch |

Quote each red and its seed in the report.

- [ ] **Step 4: Gate and commit**

```zsh
git add crates/charpente/tests/realtime.rs
git commit -m "test(payload-shapes): a resume across mixed shapes is exact or named under the fault matrix — N universes, base 0x2EB2_5A10"
```

Replace N with the timed count.

---

### Task 5: The TypeScript client ends a stream on `reshaped` and `error`

**Files:**
- Modify `crates/charpente-tsgen/src/runtime.rs` (~60–105: `StreamEnd` and the `CursorRefused` doc and message; ~332: the frame loop).
- Test `crates/charpente-tsgen/tests/routes.rs`.
- Regenerate `ts-fixture/src/generated/client.ts` and any other committed generated client (`git ls-files '*client.ts'`).
- Test `ts-fixture/src/client-runtime.test.ts`.
- Docs: `docs/GUIDE.md` §14, where `StreamEnd`/`onEnd` is described.

**Interfaces:**
- Consumes (Tasks 2–3): the frame names `charpente.reshaped` and `charpente.error`.
- Produces:

  ```ts
  StreamEnd = { kind: "gap"; detail: string } | { kind: "reshaped"; detail: string } | { kind: "error"; detail: string } | { kind: "closed" }
  ```

- [ ] **Step 1: Write the failing tests**

`crates/charpente-tsgen/tests/routes.rs`:

```rust
/// The two new terminal frames (payload-shapes spec §3) are dispatched on
/// inside ApiBase, not merely documented in `StreamEnd`'s comment — scoped to
/// the class body for the same reason as the gap test above.
#[test]
fn the_reshaped_and_error_frames_end_the_stream_by_name() {
    let ts = emit_client(&fixture_contract());
    let base = ts
        .split_once("export class ApiBase {")
        .expect("the runtime must declare ApiBase")
        .1;
    let base = &base[..base.find("\n}").expect("the class must close")];
    for (frame, kind) in [("charpente.reshaped", "reshaped"), ("charpente.error", "error")] {
        assert!(
            base.contains(&format!(r#"if (name === "{frame}") {{"#)),
            "{frame} must be dispatched on: {base}"
        );
        assert!(
            base.contains(&format!(r#"opts.onEnd({{ kind: "{kind}", detail: data }});"#)),
            "{frame} must end the stream as {kind}: {base}"
        );
    }
    assert!(
        ts.contains(r#"| { kind: "reshaped"; detail: string }"#)
            && ts.contains(r#"| { kind: "error"; detail: string }"#),
        "StreamEnd must carry both new ends: {ts}"
    );
}
```

Extend `both_early_exits_sit_inside_the_try_whose_finally_cancels_the_reader`: locate the two new `opts.onEnd({ kind: "reshaped", …` and `opts.onEnd({ kind: "error", …` strings, and add `try_ < x && x < finally` for each to the ordering assertion and its message.

`ts-fixture/src/client-runtime.test.ts`:
- change `import type { CustomerId } from "./generated/client";` to `import type { CustomerId, StreamEnd } from "./generated/client";`, confirming `StreamEnd` is exported from the generated file;
- add:

```ts
test("a reshaped frame ends the stream by name, and nothing after it is delivered", async () => {
  const detail = {
    event: "invoice_paid",
    id: 1,
    fix: "re-sync current state through reads, then reconnect without a cursor",
  };
  const { restore } = stubSseResponse([
    { event: "invoice_paid", data: invoicePaid, id: "0.1" },
    { event: "charpente.reshaped", data: detail, id: "1.2" },
    { event: "invoice_voided", data: invoiceVoided, id: "2.3" },
  ]);
  const seen: string[] = [];
  let ended: StreamEnd | undefined;
  try {
    await new Api("http://fixture.invalid").feed({
      onEvent: (ev) => {
        seen.push(ev.event);
      },
      onEnd: (end) => {
        ended = end;
      },
    });
  } finally {
    restore();
  }
  assert.deepEqual(seen, ["invoice_paid"]);
  assert.equal(ended?.kind, "reshaped");
  assert.deepEqual(JSON.parse((ended as { detail: string }).detail), detail);
});

test("an error frame ends the stream by name, and nothing after it is delivered", async () => {
  const detail = { event: "invoice_paid", id: 1, error: "event invoice_paid failed JSON encoding" };
  const { restore } = stubSseResponse([
    { event: "invoice_paid", data: invoicePaid, id: "0.1" },
    { event: "charpente.error", data: detail },
    { event: "invoice_voided", data: invoiceVoided, id: "2.3" },
  ]);
  const seen: string[] = [];
  let ended: StreamEnd | undefined;
  try {
    await new Api("http://fixture.invalid").feed({
      onEvent: (ev) => {
        seen.push(ev.event);
      },
      onEnd: (end) => {
        ended = end;
      },
    });
  } finally {
    restore();
  }
  assert.deepEqual(seen, ["invoice_paid"]);
  assert.equal(ended?.kind, "error");
  assert.deepEqual(JSON.parse((ended as { detail: string }).detail), detail);
});
```

- [ ] **Step 2: Run and record red**

```zsh
cargo test -p charpente-tsgen --test routes -- the_reshaped_and_error both_early_exits 2>&1 | grep -E "^test |panicked"
cd ts-fixture && npm ci && npm run check && npm run test; cd ..
```

Expected:
- the routes tests panic "must be dispatched on";
- the TS tests fail: `charpente.reshaped` reaches `UndeclaredEvent`, or is delivered as an event, and `StreamEnd` has no `reshaped` member. That last one is a type error under `npm run check`, which is the red.

Run `npm ci` first in any fresh tree: a bare `npx tsc` there resolves to a registry stub that exits 1 without type-checking.

- [ ] **Step 3: Implement**

In `crates/charpente-tsgen/src/runtime.rs`, first the type:

```ts
export type StreamEnd =
  | { kind: "gap"; detail: string }
  | { kind: "reshaped"; detail: string }
  | { kind: "error"; detail: string }
  | { kind: "closed" };
```

Then the `StreamEnd` doc comment:
- it becomes "the four ways the SERVER ends one";
- add: "`reshaped` is DESIGN §6's terminal `charpente.reshaped` frame: the server met an event written under a different shape of its type and will not decode it (GUIDE §11, "Changing an event"). `error` is `charpente.error`: a stored event is corrupt or does not encode as JSON. Neither may be resumed from: re-sync, then subscribe without a cursor.";
- its "fires only for the two members below" becomes "fires only for the members below".

In the frame loop, directly after the `charpente.gap` branch:

```ts
          if (name === "charpente.reshaped") {
            opts.onEnd({ kind: "reshaped", detail: data });
            return;
          }
          if (name === "charpente.error") {
            opts.onEnd({ kind: "error", detail: data });
            return;
          }
```

Update the comment above the loop that lists the exits ("the `gap` return, …") to name all three.

`CursorRefused`:
- its doc becomes "THREE different causes …": below the retention floor; diverged history; or "the event at the cursor was written under a different shape of its type";
- its message gains, after the divergence sentence: "At a reshaped event, resuming would skip the one event the server refused to decode."

Regenerate the fixture client:

```zsh
cargo run -p charpente-cli -- gen-client --dir crates/ts-fixture-app --out ts-fixture/src/generated/client.ts
cargo run -p charpente-cli -- check-client --dir crates/ts-fixture-app --out ts-fixture/src/generated/client.ts
```

Confirm the verb's flags with `cargo run -p charpente-cli -- gen-client --help` first. Regenerate every other committed `client.ts` the same way (find them with `git ls-files '*client.ts'`).

- [ ] **Step 4: Run**

```zsh
cargo test -p charpente-tsgen --no-fail-fast 2>&1 | grep -E "^test result|FAILED"
cargo test -p ts-fixture-app --no-fail-fast 2>&1 | grep -E "^test result|FAILED"
cd ts-fixture && npm run check && npm run test; cd ..
```

- [ ] **Step 5: Docs**

GUIDE §14: where `onEnd`/`StreamEnd` is described (grep `StreamEnd` in §14), state the four ends:
- `closed`;
- `gap` (§11);
- `reshaped` (the server met an event this binary will not decode — §11 "Changing an event");
- `error` (a stored event is corrupt or unencodable — §11).

Also state that none of `gap`, `reshaped` or `error` may be auto-resumed from: re-sync, then subscribe without a cursor. If §14 enumerates `CursorRefused`'s causes, make them three.

- [ ] **Step 6: Gate and commit**

```zsh
git add crates/charpente-tsgen crates/ts-fixture-app ts-fixture/src docs/GUIDE.md
git status --short   # the regenerated client.ts files must be in the commit
git commit -m "feat(tsgen): the generated client ends a stream on charpente.reshaped and charpente.error, typed in StreamEnd"
```

---

### Task 6: Jobs — refused before dispatch can start, asserted at dispatch

**Files:**
- Modify `crates/charpente-core/src/db.rs`: `Refusal` (new, beside `TxError`), `Schema.job_shapes`, `Db::register_job_shapes`, `Db::pending_job_shape_refusal`.
- Modify `crates/charpente/src/kernel/dispatch.rs:58-71` (`JobDef.fingerprint`) and `run_due_into` (the assert).
- Modify `crates/charpente/src/kernel/mod.rs`: `Kernel::build` (~211) and `after_repl_step` (~871).
- Modify `crates/charpente/src/repl/mod.rs`: `do_promote` (~867). **`accept_handoff` (~1030) is deliberately left unchanged** (amendment A1): the target of a switchover never refuses the hand-off over job shapes.
- Modify `crates/charpente-macros/src/job.rs`: the `JobDef` literal and the two glue decode messages.
- Test `crates/charpente/tests/kernel.rs` and `crates/charpente/tests/replication.rs`.
- Docs: `docs/GUIDE.md` §6 (~1464 and ~2716–2735, plus a new "Changing a job's fields" subsection), `DESIGN.md` §6 Queue bullet, and `doc_truth.rs` (`PINNED_FLOOR` 19 → 20).

**Interfaces:**
- Consumes (Task 1): `Job::FINGERPRINT`, `JobRow.shape`, and the `kernel.rs` modules `receipt_v1`/`receipt_v2`.
- Produces:
  - `pub struct Refusal(pub String)` in `charpente_core::db`, implementing `Display`, `Debug` and `Error`, with `impl Refusal { pub fn of(e: &io::Error) -> Option<&Refusal> }`. Task 7 depends on this.
  - `Db::register_job_shapes(&mut self, shapes: &[(&'static str, u64)])`
  - `Db::pending_job_shape_refusal(&self) -> io::Result<Option<String>>`
  - `JobDef.fingerprint: u64`
  - The refusal text, fixed:

    ```text
    {n} Pending "{name}" job(s) (ids {ids}) were enqueued under a different shape of {name} (recorded {recorded}; this binary {compiled:#x}) — this binary will not decode them; run the previous binary until they are Done or Dead, then upgrade, or restore {name}'s old fields and give the new shape its own #[job] type (GUIDE §6)
    ```

    `{ids}` lists at most 5 ids in ascending order, then `, …`. `{recorded}` lists the distinct recorded shapes, sorted, joined `, `, each written `{:#x}` or `none — enqueued before job shapes were tagged`. Several job types give one line each, joined by `\n`.
  - At promote the reply is `CtrlErr { message: "cannot promote: " + refusal }`, and nothing durable changes.
  - Where the kernel's write gate opens (`after_repl_step`), and at `Kernel::build` on a writable node, the kernel returns `Err(io::Error::other(Refusal(refusal)))`, so `Kernel::step`/`run` stop with it.

- [ ] **Step 1: Write the failing tests in `kernel.rs`**

```rust
// ------------------------------------------------------------ job shapes

#[derive(WireDerive, Serialize, Deserialize)]
struct ReceiptBody {
    invoice: WireU64,
}

/// The OLD binary's enqueue path; the new binary mounts the same route, but
/// nothing here calls it once the shapes have changed.
#[route(POST "/receipt", public)]
fn enqueue_receipt(tx: &mut Tx, body: ReceiptBody) -> Result<(), ApiError> {
    tx.enqueue(receipt_v1::SendReceipt {
        invoice: body.invoice.0,
    });
    Ok(())
}

fn receipts_app(sim: &Sim, v2: bool) -> std::io::Result<App> {
    let defs = if v2 {
        jobs![receipt_v2::SendReceipt]
    } else {
        jobs![receipt_v1::SendReceipt]
    };
    Ok(App::open(
        Box::new(sim.disk()),
        Schema::new(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )?
    .mount(routes![enqueue_receipt])
    .mount_jobs(defs))
}

/// The refusal text, spelled out from the spec's wording rather than taken
/// from the product's `format!`, so the byte-exact comparison pins the words.
fn receipt_refusal(ids: &[u64], recorded: &str) -> String {
    let mut listed: Vec<String> = ids.iter().take(5).map(u64::to_string).collect();
    if ids.len() > 5 {
        listed.push("…".to_string());
    }
    format!(
        "{} Pending \"SendReceipt\" job(s) (ids {}) were enqueued under a different shape of \
         SendReceipt (recorded {recorded}; this binary {:#x}) — this binary will not decode \
         them; run the previous binary until they are Done or Dead, then upgrade, or restore \
         SendReceipt's old fields and give the new shape its own #[job] type (GUIDE §6)",
        ids.len(),
        listed.join(", "),
        <receipt_v2::SendReceipt as Job>::FINGERPRINT
    )
}

/// Enqueues `n` v1 receipts on a fresh store and never runs them.
fn old_binary_leaves_pending_receipts(seed: u64, n: u64) -> Sim {
    let sim = Sim::with_faults(seed, FaultConfig::none());
    let mut app = receipts_app(&sim, false).unwrap();
    app.db_for_tests()
        .transact(|tx| {
            for invoice in 0..n {
                tx.enqueue(receipt_v1::SendReceipt { invoice });
            }
            Ok::<(), std::io::Error>(())
        })
        .unwrap();
    sim.restart();
    drop(app);
    sim
}

/// Spec §6.4: a v1 job left Pending makes the v2 kernel refuse to start, by
/// name, byte-exact, and the error is a `Refusal` (Task 7 prints it as one).
#[test]
fn a_pending_job_of_another_shape_refuses_the_kernel_by_name() {
    let sim = old_binary_leaves_pending_receipts(0x5A9E_0601, 3);
    let err = match Kernel::new(receipts_app(&sim, true).unwrap(), Box::new(sim.clock())) {
        Ok(_) => panic!("the v2 kernel started over three Pending v1 receipts"),
        Err(e) => e,
    };
    assert_eq!(
        err.to_string(),
        receipt_refusal(
            &[0, 1, 2],
            &format!("{:#x}", <receipt_v1::SendReceipt as Job>::FINGERPRINT)
        )
    );
    assert!(
        charpente::core::db::Refusal::of(&err).is_some(),
        "a refusal, not an IO failure"
    );
}

/// More than five: the line elides, the count does not.
#[test]
fn a_long_refusal_lists_five_ids_and_counts_them_all() {
    let sim = old_binary_leaves_pending_receipts(0x5A9E_0602, 7);
    let err = match Kernel::new(receipts_app(&sim, true).unwrap(), Box::new(sim.clock())) {
        Ok(_) => panic!("the v2 kernel started over seven Pending v1 receipts"),
        Err(e) => e,
    };
    assert_eq!(
        err.to_string(),
        receipt_refusal(
            &[0, 1, 2, 3, 4, 5, 6],
            &format!("{:#x}", <receipt_v1::SendReceipt as Job>::FINGERPRINT)
        )
    );
}

/// The control: when every v1 job is Done, v2 starts. Done and Dead rows are
/// never decoded again.
#[test]
fn a_drained_queue_lets_the_new_shape_start() {
    let sim = Sim::with_faults(0x5A9E_0603, FaultConfig::none());
    let mut app = receipts_app(&sim, false).unwrap();
    let mut dispatcher = Dispatcher::new(app.take_job_defs_for_tests());
    let db = app.db_for_tests();
    db.transact(|tx| {
        tx.enqueue(receipt_v1::SendReceipt { invoice: 1 });
        Ok::<(), std::io::Error>(())
    })
    .unwrap();
    dispatcher.note_enqueued(db.drain_enqueued());
    assert!(matches!(dispatcher.run_due(db, 0).unwrap(), JobOutcome::Done { .. }));
    sim.restart();
    drop(app);
    assert!(Kernel::new(receipts_app(&sim, true).unwrap(), Box::new(sim.clock())).is_ok());
}

/// Spec §6.6: a job enqueued by a pre-shape binary (`0x02`) is refused even
/// when its bytes are ones v2 WOULD decode — the rule is the recorded shape.
#[test]
fn a_pre_shape_pending_job_is_refused_even_when_its_bytes_would_decode() {
    let sim = Sim::with_faults(0x5A9E_0604, FaultConfig::none());
    let mut app = receipts_app(&sim, true).unwrap();
    let mut v2_bytes = Vec::new();
    receipt_v2::SendReceipt {
        invoice: 1,
        copies: 2,
    }
    .encode(&mut v2_bytes);
    let mut record = Vec::new();
    1u32.encode(&mut record);
    record.push(2);
    "SendReceipt".to_string().encode(&mut record);
    v2_bytes.encode(&mut record);
    None::<String>.encode(&mut record);
    let db = app.db_for_tests();
    let seq = db.last_applied().map_or(0, |s| s + 1);
    db.apply_shipped(&[(seq, record)]).unwrap();
    let err = match Kernel::new(app, Box::new(sim.clock())) {
        Ok(_) => panic!("the v2 kernel started over a shapeless Pending receipt"),
        Err(e) => e,
    };
    assert_eq!(
        err.to_string(),
        receipt_refusal(&[0], "none — enqueued before job shapes were tagged")
    );
}

/// A Pending row of an UNREGISTERED name keeps today's path (dispatch's "not
/// registered" dead-letter) and does not refuse the kernel.
#[test]
fn an_unregistered_pending_job_does_not_refuse_the_kernel() {
    let sim = old_binary_leaves_pending_receipts(0x5A9E_0605, 1);
    let app = App::open(
        Box::new(sim.disk()),
        Schema::new(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )
    .unwrap();
    assert!(Kernel::new(app, Box::new(sim.clock())).is_ok());
}

/// Hard rule 4: past the kernel's check, a foreign-shape payload never reaches
/// decode, so dispatch asserts it rather than handling it. Driven through
/// `Dispatcher` directly — the one path that skips `Kernel::build`'s check.
#[test]
#[should_panic(expected = "a foreign-shape payload never reaches decode")]
fn dispatch_asserts_it_never_decodes_a_foreign_shape() {
    let sim = Sim::with_faults(0x5A9E_0606, FaultConfig::none());
    let mut app = receipts_app(&sim, true).unwrap();
    let mut dispatcher = Dispatcher::new(app.take_job_defs_for_tests());
    let db = app.db_for_tests();
    db.transact(|tx| {
        tx.enqueue(receipt_v1::SendReceipt { invoice: 1 });
        Ok::<(), std::io::Error>(())
    })
    .unwrap();
    dispatcher.note_enqueued(db.drain_enqueued());
    let _ = dispatcher.run_due(db, 0);
}
```

Also add the seeded crash property (spec §6.4, "each under a crash between enqueue and restart"):

```rust
/// Spec §6.4 under the fault matrix. The old binary enqueues receipts and runs
/// some, the machine crashes at an arbitrary op, and the new binary opens the
/// same store. The kernel refuses exactly when a durable Pending receipt
/// carries the old shape, naming exactly those ids — it never starts over one
/// and never refuses over none.
fn receipt_upgrade_universe(sim: &mut Sim) -> Result<(), String> {
    let entropy = sim.entropy();
    sim.crash_disk_after(32 + entropy.next_u64() % 256);
    if let Ok(app) = receipts_app(sim, false)
        && let Ok(mut kernel) = Kernel::new(app, Box::new(sim.clock()))
    {
        let mut source = SimSource::new(Box::new(sim.clock()), sim.now().as_nanos() + 60 * SEC);
        let base = sim.now().as_nanos();
        for i in 0..(1 + entropy.next_u64() % 6) {
            source.push_at(
                base + i * SEC,
                Request::new(Method::Post, "/receipt").json(&ReceiptBody {
                    invoice: WireU64(i),
                }),
            );
        }
        if let Err(e) = kernel.run(&mut source)
            && !is_fault_fallout(&e.to_string())
        {
            return Err(format!("v1 kernel.run failed non-injected: {e}"));
        }
    }
    sim.restart();
    for _ in 0..64 {
        let mut app = match receipts_app(sim, true) {
            Ok(app) => app,
            Err(e) if is_fault_fallout(&e.to_string()) => {
                sim.restart();
                continue;
            }
            Err(e) => return Err(format!("v2 open failed non-injected: {e}")),
        };
        let pending: Vec<u64> = match app
            .db_for_tests()
            .snapshot()
            .and_then(|s| s.scan::<JobRow>())
        {
            Ok(rows) => rows
                .iter()
                .filter(|r| r.status == JobStatus::Pending && r.name == "SendReceipt")
                .map(|r| r.id.raw())
                .collect(),
            Err(e) if is_fault_fallout(&e.to_string()) => {
                sim.restart();
                continue;
            }
            Err(e) => return Err(format!("job scan failed non-injected: {e}")),
        };
        return match Kernel::new(app, Box::new(sim.clock())) {
            Ok(_) if pending.is_empty() => {
                sim.cover("shapes:job-drained");
                Ok(())
            }
            Ok(_) => Err(format!("the v2 kernel STARTED over Pending v1 receipts {pending:?}")),
            Err(e) if charpente::core::db::Refusal::of(&e).is_some() => {
                let expected = receipt_refusal(
                    &pending,
                    &format!("{:#x}", <receipt_v1::SendReceipt as Job>::FINGERPRINT),
                );
                if pending.is_empty() || e.to_string() != expected {
                    return Err(format!("refused {e:?}; durable Pending receipts {pending:?}"));
                }
                sim.cover("shapes:job-refused");
                Ok(())
            }
            Err(e) if is_fault_fallout(&e.to_string()) => {
                sim.restart();
                continue;
            }
            Err(e) => Err(format!("v2 Kernel::new failed non-injected: {e}")),
        };
    }
    sim.give_up("the v2 binary never booted cleanly — nothing adjudicated");
    Ok(())
}

#[test]
fn a_new_job_shape_starts_iff_no_old_shape_is_pending_under_the_fault_matrix() {
    run_universes_swarm_with!(
        0x5A9E_0600,
        64,
        |_cfg: &mut FaultConfig| {},
        |sim: &mut Sim| receipt_upgrade_universe(sim),
        require: ["shapes:job-refused", "shapes:job-drained"]
    );
}
```

Import `charpente_sim::run_universes_swarm_with` if `kernel.rs` lacks it.

- [ ] **Step 2: Write the failing tests in `replication.rs`**

```rust
/// A different shape for `Ping` ("ping"): what a follower running a binary
/// whose `Ping` gained a field would register.
const PING_V2_FINGERPRINT: u64 = 0x7E57_0106;

fn ping_refusal(ids: &str, compiled: u64) -> String {
    format!(
        "1 Pending \"ping\" job(s) (ids {ids}) were enqueued under a different shape of ping \
         (recorded {:#x}; this binary {compiled:#x}) — this binary will not decode them; run \
         the previous binary until they are Done or Dead, then upgrade, or restore ping's old \
         fields and give the new shape its own #[job] type (GUIDE §6)",
        <Ping as Job>::FINGERPRINT
    )
}

fn enqueue_ping(node: &mut Node) {
    node.db
        .as_mut()
        .unwrap()
        .transact(|tx| {
            tx.enqueue(Ping { n: 7 });
            Ok::<(), io::Error>(())
        })
        .unwrap();
}

fn promote_f(sim: &Sim, f: &mut Node) -> Msg {
    f.flipped = true;
    CtrlConn::open(sim, "f:1", &Msg::CtrlPromote { secret: None })
        .and_then(|mut cli| drive_ctrl(sim, &mut [f], &mut cli, CONVERGE_BUDGET))
        .unwrap()
}

/// Spec §4 at promote, amendment A1: refused BY NAME before the durable role
/// flips, so the node stays a follower. Promoting first and stopping after
/// would leave a durable primary that refuses to start on every restart.
#[test]
fn a_promote_over_foreign_pending_jobs_is_refused_and_the_node_stays_a_follower() {
    let sim = Sim::with_faults(0x5A9E_0701, FaultConfig::none());
    let mut p = spawn_node(&sim, "p", primary_cfg("p:1"), DbConfig::default()).unwrap();
    let mut f = spawn_node(&sim, "f", follower_cfg("f:1", "p:1"), DbConfig::default()).unwrap();
    f.db.as_mut().unwrap().register_job_shapes(&[("ping", PING_V2_FINGERPRINT)]);
    enqueue_ping(&mut p);
    run_until(&sim, &mut [&mut p, &mut f], converged, CONVERGE_BUDGET).unwrap();
    p.host.crash_now();
    match promote_f(&sim, &mut f) {
        Msg::CtrlErr { message } => assert_eq!(
            message,
            format!("cannot promote: {}", ping_refusal("0", PING_V2_FINGERPRINT))
        ),
        other => panic!("expected CtrlErr, got {other:?}"),
    }
    assert!(matches!(f.repl.role(), Role::Follower { .. }), "still a follower in memory");
    assert!(!f.db.as_ref().unwrap().writes_allowed(), "and not writable");
    let (durable, _) =
        charpente::repl::role::load_role(&f.host.disk(), &Role::Primary).unwrap();
    assert!(matches!(durable, Role::Follower { .. }), "and still a follower on disk");
}

/// The control: the same promote with a matching shape succeeds.
#[test]
fn a_promote_over_same_shape_pending_jobs_succeeds() {
    let sim = Sim::with_faults(0x5A9E_0702, FaultConfig::none());
    let mut p = spawn_node(&sim, "p", primary_cfg("p:1"), DbConfig::default()).unwrap();
    let mut f = spawn_node(&sim, "f", follower_cfg("f:1", "p:1"), DbConfig::default()).unwrap();
    f.db.as_mut().unwrap().register_job_shapes(&[("ping", <Ping as Job>::FINGERPRINT)]);
    enqueue_ping(&mut p);
    run_until(&sim, &mut [&mut p, &mut f], converged, CONVERGE_BUDGET).unwrap();
    p.host.crash_now();
    assert!(matches!(promote_f(&sim, &mut f), Msg::CtrlOk { .. }));
}

/// Done rows are never decoded again, so they never refuse anything.
#[test]
fn a_promote_over_foreign_done_jobs_succeeds() {
    let sim = Sim::with_faults(0x5A9E_0703, FaultConfig::none());
    let mut p = spawn_node(&sim, "p", primary_cfg("p:1"), DbConfig::default()).unwrap();
    let mut f = spawn_node(&sim, "f", follower_cfg("f:1", "p:1"), DbConfig::default()).unwrap();
    f.db.as_mut().unwrap().register_job_shapes(&[("ping", PING_V2_FINGERPRINT)]);
    enqueue_ping(&mut p);
    p.db.as_mut()
        .unwrap()
        .transact(|tx| {
            let mut job = tx.get(Id::<JobRow>::from_raw(0))?.expect("the enqueued ping");
            job.status = JobStatus::Done;
            tx.put(&job);
            Ok::<(), io::Error>(())
        })
        .unwrap();
    run_until(&sim, &mut [&mut p, &mut f], converged, CONVERGE_BUDGET).unwrap();
    p.host.crash_now();
    assert!(matches!(promote_f(&sim, &mut f), Msg::CtrlOk { .. }));
}

/// Review focus 4, amendment A4: a snapshot re-seed rebuilds the follower's
/// `Db` from its schema, so the job-shape registry must live IN the schema. A
/// registry kept anywhere else comes back empty and this promote goes through.
#[test]
fn a_promote_after_a_snapshot_reseed_still_refuses_foreign_pending_jobs() {
    let sim = Sim::with_faults(0x5A9E_0704, FaultConfig::none());
    let mut p = spawn_node(&sim, "p", primary_cfg("p:1"), seeding_db_cfg()).unwrap();
    enqueue_ping(&mut p);
    for i in 0..60u64 {
        p.db.as_mut()
            .unwrap()
            .transact(|tx| {
                tx.put(&Widget {
                    id: Id::from_raw(i),
                    kind: i % 3,
                    weight: i as i64,
                });
                Ok::<(), io::Error>(())
            })
            .unwrap();
    }
    let mut f = spawn_node(&sim, "f", follower_cfg("f:1", "p:1"), seeding_db_cfg()).unwrap();
    f.db.as_mut().unwrap().register_job_shapes(&[("ping", PING_V2_FINGERPRINT)]);
    run_until(
        &sim,
        &mut [&mut p, &mut f],
        |ns| ns[1].repl.stats().seeds >= 1 && converged(ns),
        CONVERGE_BUDGET,
    )
    .unwrap();
    assert!(total_seeds(&f) >= 1, "the precondition: f was snapshot-seeded");
    p.host.crash_now();
    match promote_f(&sim, &mut f) {
        Msg::CtrlErr { message } => assert_eq!(
            message,
            format!("cannot promote: {}", ping_refusal("0", PING_V2_FINGERPRINT))
        ),
        other => panic!("the registry did not survive the re-seed: {other:?}"),
    }
}

/// Review focus 3, amendment A1: a switchover onto a follower holding a
/// Pending job of a foreign shape — the ordinary rolling deploy (upgrade the
/// follower, switch over, upgrade the old primary). HandedOff is DESIGN §5's
/// point of no return, so the target does NOT refuse the hand-off: a refusal
/// there leaves the primary write-stopped and the target a follower, with no
/// named way out. The switchover completes, and the new primary names the jobs
/// — its kernel refuses at the write-gate flip (the next test), and that
/// refusal's fix works from exactly this state.
#[test]
fn a_switchover_onto_foreign_pending_jobs_completes_and_the_new_primary_names_them() {
    let sim = Sim::with_faults(0x5A9E_0705, FaultConfig::none());
    let mut p = spawn_node(&sim, "p", primary_cfg("p:1"), DbConfig::default()).unwrap();
    let mut f = spawn_node(&sim, "f", follower_cfg("f:1", "p:1"), DbConfig::default()).unwrap();
    f.db.as_mut().unwrap().register_job_shapes(&[("ping", PING_V2_FINGERPRINT)]);
    enqueue_ping(&mut p);
    run_until(&sim, &mut [&mut p, &mut f], converged, CONVERGE_BUDGET).unwrap();
    p.flipped = true;
    f.flipped = true;
    let mut cli = CtrlConn::open(
        &sim,
        "p:1",
        &Msg::CtrlSwitchover {
            to: "f:1".to_string(),
            secret: None,
        },
    )
    .unwrap();
    let reply = drive_ctrl(&sim, &mut [&mut p, &mut f], &mut cli, CONVERGE_BUDGET).unwrap();
    assert!(
        matches!(reply, Msg::CtrlOk { .. }),
        "the switchover completes — it is never stranded mid-hand-off: {reply:?}"
    );
    assert!(matches!(f.repl.role(), Role::Primary), "f is the primary");
    assert!(matches!(p.repl.role(), Role::Follower { .. }), "p follows it");
    assert_eq!(
        f.db.as_ref().unwrap().pending_job_shape_refusal().unwrap(),
        Some(ping_refusal("0", PING_V2_FINGERPRINT)),
        "and the new primary names the foreign jobs — the refusal its kernel stops on"
    );
}

/// Spec §4 at the write gate's opening, through the kernel (amendment A1): a
/// primary restarted with a witness opens gated, holding a Pending job of a
/// foreign shape. When the witness answers, the gate opens, and the kernel
/// refuses by name in `after_repl_step` before it dispatches, migrates or
/// writes anything. A completed switchover's target reaches this same check,
/// through `accept_handoff`'s `allow_writes`.
#[test]
fn a_write_gate_opening_over_foreign_pending_jobs_stops_the_kernel_by_name() {
    let sim = Sim::with_faults(0x5A9E_0706, FaultConfig::none());
    let horizon = 100 * 1_000_000_000;
    let ph = sim.host("p");
    {
        let mut db = Db::open(Box::new(ph.disk()), DbConfig::default(), full_schema()).unwrap();
        db.transact(|tx| {
            tx.enqueue(Ping { n: 7 });
            Ok::<(), io::Error>(())
        })
        .unwrap();
        db.checkpoint().unwrap();
    }
    store_witnesses(&ph.disk(), &["w:1".to_string()]).unwrap();
    let mut p = spawn_kernel_node(&sim, "p", primary_cfg("p:1"), DbConfig::default(), horizon, true)
        .unwrap();
    // This binary's `ping` has another shape. `kernel_schema`'s kernels mount
    // `Bump` only, so registering `ping` by hand collides with nothing.
    p.kernel
        .as_mut()
        .unwrap()
        .app_mut_for_tests()
        .db_for_tests()
        .register_job_shapes(&[("ping", PING_V2_FINGERPRINT)]);
    assert!(
        !p.kernel.as_ref().unwrap().app().db().writes_allowed(),
        "the precondition: the kernel opens gated, so Kernel::build's own check did not run"
    );
    let mut w = spawn_kernel_node(&sim, "w", follower_cfg("w:1", "p:1"), DbConfig::default(), horizon, true)
        .unwrap();
    let err = krun_until(&sim, &mut [&mut p, &mut w], |_| false, CONVERGE_BUDGET).unwrap_err();
    assert_eq!(
        err,
        format!("p kernel step: {}", ping_refusal("0", PING_V2_FINGERPRINT))
    );
    let jobs = p
        .kernel
        .as_ref()
        .unwrap()
        .app()
        .db()
        .snapshot()
        .unwrap()
        .scan::<JobRow>()
        .unwrap();
    assert_eq!(
        (jobs.len(), jobs[0].status, jobs[0].attempts),
        (1, JobStatus::Pending, 0),
        "the foreign job was never dispatched"
    );
}
```

Use `replication.rs`'s own import lines and names: `JobRow` and `JobStatus` (add `use charpente_core::job::{JobRow, JobStatus};` if missing), `Widget`, `full_schema`, `store_witnesses`, `spawn_kernel_node`, `krun_until`. Two prefixes are the rig's own formats: `"p kernel step: …"` comes from `kstep_node`, and `"f step: …"` from `step_node`.

- [ ] **Step 3: Run and record red**

```zsh
cargo test -p charpente --test kernel -- shape pending refus drained unregistered dispatch_asserts 2>&1 | tee .superpowers/sdd/2026-09-24-payload-shapes/t6-red.log | grep -E "^test |panicked"
cargo test -p charpente --test replication -- promote_over switchover_onto reseed write_gate_opening 2>&1 | grep -E "^test |panicked"
```

The API (`register_job_shapes`, `Refusal`) must exist before these compile. Do Step 4's `Db` and `Refusal` parts first, leaving the checks unwired. Every "refuse" test must then fail *behaviourally*:
- the kernel tests with "the v2 kernel started over …";
- the promote tests with "expected CtrlErr, got CtrlOk";
- the gate test with a budget or stall message in place of the refusal.

The `should_panic` test fails because there is no panic. The switchover test is green from the start: it pins the *absence* of a hand-off refusal, and Step 6 proves it red by mutation. Record all of it.

- [ ] **Step 4: Implement**

`crates/charpente-core/src/db.rs`, beside `TxError`:

```rust
/// A named refusal: the platform declining to proceed on state it will not
/// guess about — a table-gate barrier, a witness answering as a primary, a
/// Pending job of a foreign shape. Carried inside `io::Error::other`, so every
/// `io::Result` path is untouched; `serve` reads it back to tell a refusal
/// from a platform IO failure on the kernel's last stderr line, because
/// restarting the same binary clears an IO failure and never a refusal
/// (payload-shapes spec §5).
#[derive(Debug)]
pub struct Refusal(pub String);

impl fmt::Display for Refusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Refusal {}

impl Refusal {
    /// `Some` when `e` carries a `Refusal`.
    pub fn of(e: &io::Error) -> Option<&Refusal> {
        e.get_ref().and_then(|inner| inner.downcast_ref::<Refusal>())
    }
}
```

`Schema` gains a field. Its `Default` and `Clone` derives carry it.

```rust
    /// `Job::NAME` → `Job::FINGERPRINT` for every job type this binary
    /// mounts (`Db::register_job_shapes`). Here rather than on `Db` because a
    /// snapshot re-seed rebuilds the `Db` from its schema, and a registry
    /// kept anywhere else would come back empty (spec amendment A4). Not part
    /// of `schema_digest_of`: job shapes are refused at promote, never
    /// compared in Hello (spec §8).
    job_shapes: BTreeMap<&'static str, u64>,
```

`impl Db`:

```rust
    /// Registers each mounted job type's payload shape so
    /// `pending_job_shape_refusal` can name a Pending row this binary would
    /// decode under a foreign shape. `Kernel::build` calls it with the
    /// mounted `JobDef`s. Re-registering the same pair is a no-op; one name
    /// under two shapes is impossible past `mount_jobs`' duplicate refusal.
    pub fn register_job_shapes(&mut self, shapes: &[(&'static str, u64)]) {
        let registry = &mut self.engine.machine_mut().schema.job_shapes;
        for &(name, fingerprint) in shapes {
            let prev = registry.insert(name, fingerprint);
            assert!(
                prev.is_none_or(|p| p == fingerprint),
                "job \"{name}\" registered under two shapes ({:#x} and {fingerprint:#x}) — \
                 mount_jobs refuses a duplicate name, so this is a platform bug",
                prev.unwrap_or_default()
            );
        }
    }

    /// The check that runs wherever dispatch could start (payload-shapes spec
    /// §4, amendment A1): at kernel start on a writable node, whenever the
    /// kernel's write gate opens (a witness gate opening, a completed
    /// switchover), and before a promote flips the role. `Some(refusal)` names every Pending
    /// job of a registered type whose recorded shape is not the one this
    /// binary compiled — a row with no recorded shape included — one line per
    /// job type. Done and Dead rows are never decoded again and are ignored;
    /// a Pending row of an UNREGISTERED name keeps its own path (dispatch's
    /// "not registered" dead-letter).
    pub fn pending_job_shape_refusal(&self) -> io::Result<Option<String>> {
        const LISTED_IDS: usize = 5;
        let registry = &self.engine.machine().schema.job_shapes;
        if registry.is_empty() {
            return Ok(None);
        }
        let mut foreign: BTreeMap<&'static str, (Vec<u64>, BTreeSet<Option<u64>>)> =
            BTreeMap::new();
        for job in self.snapshot()?.scan::<JobRow>()? {
            if job.status != crate::job::JobStatus::Pending {
                continue;
            }
            let Some((&name, &compiled)) = registry.get_key_value(job.name.as_str()) else {
                continue;
            };
            if job.shape == Some(compiled) {
                continue;
            }
            let (ids, recorded) = foreign.entry(name).or_default();
            ids.push(job.id.raw());
            recorded.insert(job.shape);
        }
        if foreign.is_empty() {
            return Ok(None);
        }
        let lines: Vec<String> = foreign
            .into_iter()
            .map(|(name, (ids, recorded))| {
                let mut listed: Vec<String> =
                    ids.iter().take(LISTED_IDS).map(u64::to_string).collect();
                if ids.len() > LISTED_IDS {
                    listed.push("…".to_string());
                }
                let recorded: Vec<String> = recorded
                    .into_iter()
                    .map(|shape| match shape {
                        Some(shape) => format!("{shape:#x}"),
                        None => "none — enqueued before job shapes were tagged".to_string(),
                    })
                    .collect();
                format!(
                    "{} Pending \"{name}\" job(s) (ids {}) were enqueued under a different shape \
                     of {name} (recorded {}; this binary {:#x}) — this binary will not decode \
                     them; run the previous binary until they are Done or Dead, then upgrade, or \
                     restore {name}'s old fields and give the new shape its own #[job] type \
                     (GUIDE §6)",
                    ids.len(),
                    listed.join(", "),
                    recorded.join(", "),
                    registry[name]
                )
            })
            .collect();
        Ok(Some(lines.join("\n")))
    }
```

`scan` returns rows in id order, so `ids` ascends. Import `View` in `db.rs` if needed.

`crates/charpente/src/kernel/dispatch.rs`: `JobDef` gains

```rust
    /// `Job::FINGERPRINT` — the payload shape this binary compiled. Dispatch
    /// asserts a row's recorded shape equals it before the glue decodes
    /// (payload-shapes spec §4).
    pub fingerprint: u64,
```

In `run_due_into`, after `let Some(def) = … else { … };` and before `catch_unwind` (outside it, so the assert is not absorbed as a handler panic):

```rust
            // `Kernel::build` (writable) and `after_repl_step` (every write-
            // gate opening) refuse to start dispatch over a Pending row of a
            // foreign shape, and a heap is only ever fed while writable; a
            // fresh enqueue records this binary's own shape. So a
            // foreign-shape payload never reaches decode — asserted, not
            // handled (hard rule 4).
            assert_eq!(
                row.shape,
                Some(def.fingerprint),
                "dispatcher invariant: job {id} (\"{}\") has recorded shape {:?} but this binary \
                 compiled {:#x} — a foreign-shape payload never reaches decode",
                row.name,
                row.shape,
                def.fingerprint
            );
```

`crates/charpente-macros/src/job.rs`: the `JobDef` literal gains `fingerprint: <#name as ::charpente::core::db::Job>::FINGERPRINT,`. The two glue messages become:

```rust
                            "job decode for {}: {e} — the stored payload is corrupt (its recorded shape is this binary's, so this is not a shape change); it retries, then dead-letters — if other rows are damaged too, restore this node from its archive (GUIDE §15)", #name_str
```

```rust
                                "job decode: {} trailing bytes after a {} payload — the stored payload is corrupt (its recorded shape is this binary's, so this is not a shape change); it retries, then dead-letters — if other rows are damaged too, restore this node from its archive (GUIDE §15)",
```

`crates/charpente/src/kernel/mod.rs` gets a free function:

```rust
/// Spec §4: dispatch may not start over a Pending job of a foreign shape.
fn refuse_foreign_pending_jobs(db: &Db) -> io::Result<()> {
    match db.pending_job_shape_refusal()? {
        Some(refusal) => Err(io::Error::other(Refusal(refusal))),
        None => Ok(()),
    }
}
```

In `Kernel::build`:

```rust
        let defs = app.take_job_defs();
        let shapes: Vec<(&'static str, u64)> =
            defs.iter().map(|d| (d.name, d.fingerprint)).collect();
        app.db_mut().register_job_shapes(&shapes);
        let mut dispatcher = Dispatcher::new(defs);
        …
        if was_writable {
            refuse_foreign_pending_jobs(app.db())?;
            cron.upsert(app.db_mut(), now)?;
        }
```

In `after_repl_step`, the first statement inside `if writable && !self.was_writable {` becomes

```rust
            // Every way the write gate opens passes here: a witness gate
            // opening, a promote (already refused in `do_promote`, so this
            // never fires for one), and a switchover's target after
            // `accept_handoff`. The hand-off itself is never refused over job
            // shapes: HandedOff is the primary's point of no return (DESIGN
            // §5), so refusing there would strand the pair write-stopped. The
            // switchover completes, and this node stops here by name instead
            // (payload-shapes amendment A1).
            refuse_foreign_pending_jobs(self.app.db())?;
```

`repl.step` and this call run back to back in the `Ready::Repl` arm, so nothing dispatches between the flip and the check.

`crates/charpente/src/repl/mod.rs`, `do_promote`: after the `my_listen` check and before `self.clear_stale_witnesses()?`:

```rust
        // Before the durable role flips (spec amendment A1): a refused
        // promote leaves a follower, never a durable primary that refuses to
        // start on every restart.
        if let Some(refusal) = db.pending_job_shape_refusal()? {
            self.reply_ctrl(
                ctrl_id,
                &Msg::CtrlErr {
                    message: format!("cannot promote: {refusal}"),
                },
                now,
            );
            return Ok(());
        }
```

`accept_handoff` stays as it is (amendment A1). `abort_switchover`'s return to Primary needs no check either: that node was a writable primary a moment before, dispatching under this binary's shapes, and a write-stopped drain enqueues nothing.

- [ ] **Step 5: Run**

```zsh
cargo test -p charpente --test kernel --no-fail-fast 2>&1 | tee .superpowers/sdd/2026-09-24-payload-shapes/t6-kernel.log | grep -E "^test result|FAILED|panicked"
cargo test -p charpente --test replication --no-fail-fast 2>&1 | tee .superpowers/sdd/2026-09-24-payload-shapes/t6-repl.log | grep -E "^test result|FAILED"
```

The replication suite is the slowest here. If it exceeds the tool ceiling, run the new tests by name, and let the controller's background gate cover the rest.
- If `dispatch_asserts_it_never_decodes_a_foreign_shape` aborts instead of panicking, check whether `Group`'s `Drop` panics while unwinding. Report it rather than weakening the assert.
- Time the seeded property as in Task 4, set N, and state it in the commit.

- [ ] **Step 6: Mutation-prove**

Commit first. One mutation at a time; restore after each and confirm a clean `git diff`.

| Mutation | Must go red |
|---|---|
| in `seed_schema()`, add `schema.job_shapes = BTreeMap::new();` before returning | `a_promote_after_a_snapshot_reseed_still_refuses_foreign_pending_jobs` |
| delete `after_repl_step`'s `refuse_foreign_pending_jobs` line | `a_write_gate_opening_over_foreign_pending_jobs_stops_the_kernel_by_name` |
| add the tempting target-side check to `accept_handoff`, just before `clear_stale_witnesses`: `if let Some(r) = db.pending_job_shape_refusal()? { if let Some(f) = &self.follower { f.refuse(r.clone()); } return Err(io::Error::other(r)); }` | `a_switchover_onto_foreign_pending_jobs_completes_and_the_new_primary_names_them` (the switchover never completes) |
| move `do_promote`'s check to after `store_role` | `…_the_node_stays_a_follower` (the durable-role assertion) |
| delete the dispatch assert | `dispatch_asserts_it_never_decodes_a_foreign_shape` |

`cargo-mutants` generates no mutants inside `assert_eq!`, so this hand mutation is the proof for the assert.

- [ ] **Step 7: Docs**

GUIDE §6, at ~1464: replace `(schema drift: \`"job decode for <Name>: <e> — schema drift or corruption"\`)` with `(corrupt stored bytes: \`"job decode for <Name>: <e> — the stored payload is corrupt …"\`; a shape change never reaches dispatch — see "Changing a job's fields")`.

GUIDE §6, at ~2716–2735: the block's first line becomes

```text
job decode for SendReceipt: <the underlying decode error> — the stored payload is corrupt (its recorded shape is this binary's, so this is not a shape change); it retries, then dead-letters — if other rows are damaged too, restore this node from its archive (GUIDE §15)
```

The prose "The first: a `JobRow`'s stored payload no longer decodes … Fix, same rule: don't reorder or remove a job payload's fields once jobs of that type may exist Pending in the store." becomes: "The first: a Pending job's stored payload failed to decode although its recorded shape is this binary's — corrupt bytes, not a shape change, because a shape change is refused before dispatch can start (see "Changing a job's fields"). It retries and then dead-letters like any rejection."

Add `### Changing a job's fields` after `### Retry, dead-letter, and exactly-once effects`:

````markdown
### Changing a job's fields

Every enqueue records the payload's shape — `Job::FINGERPRINT`, the structural
fingerprint `#[job]` computes from the fields (§12) — and a Pending job is only ever
decoded by a binary that compiled that same shape. A node about to become able to
dispatch refuses while any Pending job of a type it mounts was enqueued under another
shape. It checks at startup and at every moment its write gate opens (a witness gate
opening, a completed switchover), and `promote` checks before it changes anything:

<!-- pinned -->
```text
<n> Pending "<job>" job(s) (ids <ids>) were enqueued under a different shape of <job> (recorded <recorded>; this binary <fingerprint>) — this binary will not decode them; run the previous binary until they are Done or Dead, then upgrade, or restore <job>'s old fields and give the new shape its own #[job] type (GUIDE §6)
```

At most five ids are listed, then `…`. A job enqueued before shapes were recorded reads
`recorded none — enqueued before job shapes were tagged`, and it is refused the same way:
nothing decodes bytes it cannot prove are its own shape.
- At startup, or when the write gate opens, the refusal stops the kernel, and a stopped
  kernel ends the process (§5).
- At `promote`, it answers `CtrlErr` prefixed `cannot promote:`, and the node stays a follower.
- A switchover is never refused partway. Once the old primary hands off it never resumes
  on its own (DESIGN §5 calls the hand-off its point of no return), so a refusal there
  would leave neither node writable. The switchover completes. The new primary then stops at the moment it
  becomes writable, with the refusal above, and its fix works from there: run the previous
  binary on that node, now the primary, until the jobs are Done or Dead. The old primary
  follows it throughout.

Done and Dead rows are never decoded again and never refuse anything. A Pending row of a
job type this binary does not mount keeps the "not registered" path below.

There are two ways through a shape change:

- **Drain first.** Run the old binary until every Pending job of the type is Done or Dead
  (each reaches one within its retry policy), then deploy. In a replicated pair, drain on
  the primary before a switchover onto an upgraded follower, or that follower stops as
  described above.
- **A new type.** Leave the old type's fields alone, put the new fields in a new `#[job]`
  type, mount both, and switch enqueues to the new one. The old type keeps dispatching
  its rows, because their shape is still its own; remove it once none are Pending.
````

In `DESIGN.md` §6, the Queue bullet gains: "Every enqueue records the payload type's structural fingerprint (§3). The kernel refuses to dispatch while a Pending job of a mounted type carries another shape, naming the job, its ids and both shapes. It checks at startup and wherever its write gate opens, and `promote` checks before its durable flip. A switchover is never refused at the hand-off, which is the primary's point of no return (§5): its target stops at the write-gate flip instead, where the refusal's fix still works. Dispatch asserts the invariant."

`doc_truth.rs`: `PINNED_FLOOR` becomes 20, with the comment extended.

- [ ] **Step 8: Gate and commit**

```zsh
git add crates/charpente-core/src/db.rs crates/charpente/src/kernel crates/charpente/src/repl/mod.rs crates/charpente-macros/src/job.rs crates/charpente/tests/kernel.rs crates/charpente/tests/replication.rs docs/GUIDE.md DESIGN.md crates/guide-examples/tests/doc_truth.rs
git commit -m "feat(payload-shapes): a node refuses to dispatch over a Pending job of a foreign shape — at start, at every write-gate opening, before a promote flips — and dispatch asserts it"
```

---

### Task 7: A stopped kernel ends the process

**Files:**
- Modify `crates/charpente-core/src/db.rs`: mark the table-gate refusal sites (~1862, 1869, 1876, 1883, 2756, 2776, 2840, 2849) as `Refusal`.
- Modify `crates/charpente/src/repl/mod.rs`: mark the witness-conflict refusal (~664) and the strict refusal (~552); rewrite the `ReplConfig::strict_witnesses` doc (~118–129).
- Modify `crates/charpente/src/serve.rs`: `Server`, `serve`, `serve_replicated`, `http_edge`, `kernel_stop_line`, and tests.
- Modify `crates/charpente/src/launch.rs` (~35–50, ~72–78).
- Modify `examples/invoicing/src/bin/node.rs` (~147–186) and `examples/invoicing/src/bin/tenant_node.rs` (~345–380).
- Modify `crates/bench-harness/src/bin/bench-app.rs` (~51).
- Modify `crates/edge-harness/src/child.rs` (`wait_exit`), `crates/edge-harness/tests/failover.rs` (Phase B, ~1255–1295) and `crates/edge-harness/tests/smoke.rs` (a new test).
- Docs:
  - `docs/GUIDE.md` §5's "Starting the server" (~1165–1218);
  - the §7 serve example (~1788);
  - the §10 examples (~2985–3010) and the strict-witnesses paragraph (~3145–3155);
  - §13's `launch` section;
  - `DESIGN.md` §5 (strict witnesses ~1076–1085, and process exit).

**Interfaces:**
- Consumes (Task 6): `Refusal`, `Refusal::of`, and the jobs refusal as a real startup refusal.
- Produces:
  - `pub struct Server { addr: SocketAddr, kernel: JoinHandle<i32> }`
  - `pub fn wait(self) -> i32`: `1` when the kernel stopped on an error (its line already printed), `101` when the thread panicked, `0` on a clean shutdown.
  - `fn kernel_stop_line(started: bool, e: &io::Error) -> String`, with four fixed lines:
    - `charpente kernel refused to start: {e}`
    - `charpente kernel failed to start — fix the store/log and restart: {e}`
    - `charpente kernel stopped on a refusal: {e}`
    - `charpente kernel stopped on a platform IO failure — restart to recover: {e}`
  - `ChildNode::wait_exit(&mut self, timeout: Duration) -> io::Result<Option<ExitStatus>>`

- [ ] **Step 1: Write the failing tests**

In `crates/charpente/src/serve.rs`'s test module:

```rust
    /// The kernel thread's last stderr line names what stopped it (spec §5):
    /// a refusal is named as one, because restarting the same binary clears an
    /// IO failure and never a refusal. Each refusal's own text carries its fix.
    #[test]
    fn every_kernel_stop_line_names_what_stopped_it() {
        let refusal = io::Error::other(charpente_core::db::Refusal(
            "the refusal names its own fix".to_string(),
        ));
        let io_failure = io::Error::other("disk on fire");
        assert_eq!(
            kernel_stop_line(false, &refusal),
            "charpente kernel refused to start: the refusal names its own fix"
        );
        assert_eq!(
            kernel_stop_line(false, &io_failure),
            "charpente kernel failed to start — fix the store/log and restart: disk on fire"
        );
        assert_eq!(
            kernel_stop_line(true, &refusal),
            "charpente kernel stopped on a refusal: the refusal names its own fix"
        );
        assert_eq!(
            kernel_stop_line(true, &io_failure),
            "charpente kernel stopped on a platform IO failure — restart to recover: disk on fire"
        );
    }

    /// Spec §5: a kernel that refuses to start ends the process — `wait`
    /// returns 1 — rather than parking behind an edge that answers 500. The
    /// refusal is Task 6's: a Pending job enqueued under one shape, mounted
    /// under another.
    #[test]
    fn a_kernel_that_refuses_to_start_ends_wait_with_status_1() {
        struct Receipt {
            invoice: u64,
        }
        impl Codec for Receipt {
            fn encode(&self, out: &mut Vec<u8>) {
                self.invoice.encode(out);
            }
            fn decode(input: &mut &[u8]) -> io::Result<Receipt> {
                Ok(Receipt {
                    invoice: u64::decode(input)?,
                })
            }
        }
        impl charpente_core::db::Job for Receipt {
            const NAME: &'static str = "receipt";
            const FINGERPRINT: u64 = 0x7E57_0701;
        }
        fn noop_glue(
            _tx: &mut Tx,
            _payload: &[u8],
            _cap: Option<&charpente_core::db::CrossTenant>,
        ) -> Result<(), crate::kernel::JobError> {
            Ok(())
        }
        let mut suffix = [0u8; 8];
        ProdEntropy::new().unwrap().fill(&mut suffix);
        let root = std::env::temp_dir().join(format!(
            "charpente-serve-refuse-{:016x}",
            u64::from_le_bytes(suffix)
        ));
        let mut app = App::open(
            Box::new(ProdDisk::new(&root).unwrap()),
            Schema::new(),
            Box::new(ProdEntropy::new().unwrap()),
            AppConfig::default(),
        )
        .unwrap()
        .mount_jobs(vec![crate::kernel::JobDef {
            name: "receipt",
            retry: charpente_core::job::RetryPolicy::DEFAULT,
            cross_tenant: false,
            fingerprint: 0x7E57_0702,
            glue: noop_glue,
        }]);
        app.db_mut()
            .transact(|tx| {
                tx.enqueue(Receipt { invoice: 1 });
                Ok::<(), io::Error>(())
            })
            .unwrap();
        let server = serve("127.0.0.1:0", app).unwrap();
        assert_eq!(server.wait(), 1);
    }
```

In `crates/edge-harness/src/child.rs`:

```rust
    /// Waits up to `timeout` for the process to exit ON ITS OWN — a node whose
    /// kernel stopped now ends its process (payload-shapes spec §5).
    /// `Ok(None)` means it was still running at the deadline; this never
    /// kills it.
    pub fn wait_exit(&mut self, timeout: Duration) -> io::Result<Option<std::process::ExitStatus>> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(status) = self.child.try_wait()? {
                return Ok(Some(status));
            }
            if Instant::now() >= deadline {
                return Ok(None);
            }
            std::thread::sleep(PORT_POLL_INTERVAL);
        }
    }
```

In `crates/edge-harness/tests/failover.rs` Phase B, follow the file's instruction: *update to the new behaviour, never weaken*.
- The stderr assertion expects `format!("charpente kernel stopped on a refusal: {matched}")`, and its error message names the new line.
- Replace the whole "after" POST block (the 500 "the kernel thread is gone" check) with:

```rust
        // The refusal stopped the kernel, and a stopped kernel now ends the
        // process (payload-shapes spec §5): an operator's supervisor sees the
        // exit, where it used to see a live process answering every request
        // 500. Phase A still carries the attribution; this pins what an
        // operator sees afterwards.
        let status = old_primary
            .wait_exit(Duration::from_secs(10))
            .map_err(|e| format!("waiting for the refused node's exit failed: {e}"))?
            .ok_or_else(|| {
                "the refused node was still running 10s after its kernel stopped — the \
                 zombie payload-shapes spec §5 removes"
                    .to_string()
            })?;
        if status.code() != Some(1) {
            return Err(format!("the refused node must exit 1, got {status}"));
        }
        scenario.record("phase B: the refused node exits 1 — a stopped kernel ends the process");
```

`wait_exit` takes `&mut self`, so the Phase B spawn at `failover.rs:~1105` becomes `let mut old_primary = ChildNode::spawn(`. Update the long comment above `run_resurrection_oracle`: its "every later request is `serve.rs`'s 500 …" becomes "the process then exits 1".

In `crates/edge-harness/tests/smoke.rs`:

```rust
/// Spec §5 at the process boundary: a node whose kernel cannot start exits
/// nonzero with its named line, instead of parking behind an HTTP edge that
/// answers 500. The startup failure is a corrupt role file (amendment A13):
/// the one a stock binary can be handed from outside.
#[test]
#[ignore]
fn a_node_whose_kernel_cannot_start_exits_nonzero_with_its_named_line() {
    let bins = build_bins().expect("build_bins");
    let data_dir = temp_dir("corrupt-role");
    std::fs::create_dir_all(&data_dir).expect("data dir");
    std::fs::write(data_dir.join("role"), "primary\n00000000").expect("plant the role file");
    let stderr_path = temp_dir("corrupt-role-stderr");
    // failover.rs's replicated-primary argv. Spawned by hand, not through
    // ChildNode::spawn: that waits for the port, which this node never opens
    // for long — the whole point is that it exits.
    let mut child = std::process::Command::new(&bins.node)
        .args([
            "--addr".to_string(),
            free_addr(),
            "--data-dir".to_string(),
            data_dir.to_str().unwrap().to_string(),
            "--repl-listen".to_string(),
            free_addr(),
            "--role".to_string(),
            "primary".to_string(),
            "--secret".to_string(),
            "edge-harness-startup-exit-secret".to_string(),
        ])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::from(
            std::fs::File::create(&stderr_path).expect("stderr file"),
        ))
        .spawn()
        .expect("spawn node");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    let status = loop {
        if let Some(status) = child.try_wait().expect("try_wait") {
            break status;
        }
        if std::time::Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("still running 30s after its kernel could not start — the zombie spec §5 removes");
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    };
    assert_eq!(status.code(), Some(1), "{status}");
    let stderr = std::fs::read_to_string(&stderr_path).expect("stderr");
    assert!(
        stderr.contains(
            "charpente replication failed to start — fix the role file and restart: role file \
             failed its integrity check (crc mismatch)"
        ),
        "{stderr}"
    );
}
```

Confirm that `node.rs`'s argument parser still takes these flags (`--secret` included) before running the test. The role file is `<data_dir>/role` (`ROLE_FILE`), because `node` hands `serve_replicated` a `ProdDisk` over the data dir.

- [ ] **Step 2: Run and record red**

```zsh
cargo test -p charpente --lib -- serve::tests::every_kernel_stop_line serve::tests::a_kernel_that_refuses 2>&1 | grep -E "^test |error\[|panicked"
cargo test -p edge-harness --test smoke -- --ignored a_node_whose_kernel_cannot_start 2>&1 | tail -20
cargo test -p edge-harness --test failover -- --ignored 2>&1 | tail -30
```

Expected red:
- the serve.rs tests do not compile (no `kernel_stop_line`, no `wait`);
- **the smoke test panics "still running 30s after …" — the behavioural red for spec §5**;
- the updated Phase B fails on the line or the exit.

- [ ] **Step 3: Implement**

In `crates/charpente/src/serve.rs`:

```rust
/// A running server: the HTTP edge, and the kernel thread it feeds. v0 has no
/// graceful shutdown. `wait` is how a binary learns the kernel stopped.
pub struct Server {
    addr: SocketAddr,
    kernel: std::thread::JoinHandle<i32>,
}

impl Server {
    pub fn local_addr(&self) -> String {
        self.addr.to_string()
    }

    /// Blocks until the kernel thread ends, and returns the exit code the
    /// process should end with:
    /// - `1` when the kernel stopped on an error (its stderr line is already
    ///   printed, naming the refusal or IO failure and the fix);
    /// - `101` when the thread panicked (the panic message is already printed);
    /// - `0` on a clean shutdown.
    ///
    /// Serve with `std::process::exit(serve(addr, app)?.wait())`: a kernel that
    /// stopped must end the process, or a supervisor sees a live process
    /// answering every request 500 and never restarts it (payload-shapes spec
    /// §5).
    pub fn wait(self) -> i32 {
        self.kernel.join().unwrap_or(101)
    }
}

/// The kernel thread's last stderr line. A refusal is named as one, because
/// restarting the same binary clears an IO failure and never a refusal; every
/// refusal's own text names its fix.
fn kernel_stop_line(started: bool, e: &io::Error) -> String {
    match (started, Refusal::of(e).is_some()) {
        (false, true) => format!("charpente kernel refused to start: {e}"),
        (false, false) => {
            format!("charpente kernel failed to start — fix the store/log and restart: {e}")
        }
        (true, true) => format!("charpente kernel stopped on a refusal: {e}"),
        (true, false) => format!(
            "charpente kernel stopped on a platform IO failure — restart to recover: {e}"
        ),
    }
}
```

In `serve`, the kernel thread returns an `i32`, and its handle goes to `http_edge`:

```rust
    let kernel = std::thread::spawn(move || -> i32 {
        let clock = crate::prod::ProdClock::new();
        let mut kernel = match kernel::Kernel::new(app, Box::new(crate::prod::ProdClock::new())) {
            Ok(kernel) => kernel,
            Err(e) => {
                eprintln!("{}", kernel_stop_line(false, &e));
                return 1;
            }
        };
        let mut source = ProdSource {
            rx: kernel_rx,
            stash: None,
            clock,
        };
        match kernel.run(&mut source) {
            Ok(()) => 0,
            Err(e) => {
                eprintln!("{}", kernel_stop_line(true, &e));
                1
            }
        }
    });

    http_edge(addr, body_cap, kernel_tx, kernel)
```

`serve_replicated` follows the same pattern; its `Repl::open` error arm keeps its own line and `return 1;`. `http_edge` gains the `kernel: std::thread::JoinHandle<i32>` parameter and returns `Server { addr: local, kernel }`. Delete the old "the threads end when the process does" wording.

In `crates/charpente/src/launch.rs`, the serve arm:

```rust
        let app = build(Box::new(disk), Box::new(entropy))?;
        // A stopped kernel ends the process with its code (spec §5), rather
        // than parking forever behind an edge that can only answer 500.
        return Ok(crate::serve::serve(addr, app)?.wait());
```

Update `launch`'s comment: the serve arm now reaches the `match` with the kernel's code, and a nonzero code exits the process.

In `examples/invoicing/src/bin/node.rs`, bind the server from each serve arm and exit with its code. The backup arm keeps its synchronous `serve::backup(...)?; return Ok(());`.

```rust
    let server = match parsed.repl {
        None => {
            let app = build(&parsed.data_dir)?;
            let addr = parsed
                .addr
                .expect("plain serve requires --addr (checked in parse)");
            serve::serve(&addr, app)?
        }
        Some(repl) => {
            /* unchanged cfg construction and backup arm */
            serve::serve_replicated(&addr, app, Box::new(ProdDisk::new(&parsed.data_dir)?), cfg)?
        }
    };
    std::process::exit(server.wait());
```

Do the same in `tenant_node.rs` and `crates/bench-harness/src/bin/bench-app.rs`. Update each file's module doc where it says "park".

Mark the refusal sites. Each `io::Error::other(X)` below becomes `io::Error::other(Refusal(X))`; in `repl/mod.rs` the type is `charpente_core::db::Refusal`, imported.
- `db.rs`: `check_replay_version`'s four returns (no adoption row, upgrade barrier, downgrade, drift).
- `db.rs`: `check_schema_gate`'s `GateStatus::Refuse(msg)` and its `upgrade_barrier_msg` return.
- `db.rs`: `migrate_schema`'s `GateStatus::Refuse(msg)` and its "has no registered migration" return.
- `repl/mod.rs`: the witness-conflict return in `resolve_witness_gate`, and the `strict_witness_refusal()` return.

The texts do not change, so `checks::classify` and every pinned string are untouched. Rewrite the `ReplConfig::strict_witnesses` doc comment: under `serve_replicated` the stop now ends the process through `Server::wait`, so a supervisor restarts it and keeps restarting until a witness answers. Remove the description of a zombie edge.

- [ ] **Step 4: Sweep for the old behaviour**

```zsh
grep -rn "kernel thread is gone\|kernel dropped the request\|stopped on a platform IO failure\|thread::park()" crates examples docs/GUIDE.md DESIGN.md
```

Each hit is either still-correct code (the edge's 500 texts remain for a request that races the exit) or a claim to update to the new behaviour, in this commit.

- [ ] **Step 5: Run**

```zsh
cargo test -p charpente --lib -- serve::tests --no-fail-fast 2>&1 | grep -E "^test result|FAILED"
cargo test -p edge-harness --test smoke -- --ignored 2>&1 | grep -E "^test result|FAILED"
cargo test -p edge-harness --test failover -- --ignored 2>&1 | grep -E "^test result|FAILED"
cargo build -p guide-examples
```

- [ ] **Step 6: Docs**

GUIDE §5, "Starting the server": the prose becomes "…then returns as soon as both are up — it does not block. `Server::wait` blocks until the kernel stops and returns the exit code the process should end with. A kernel that stopped — on a platform IO failure or a named refusal, each printed on stderr with its fix — must end the process, or a supervisor sees a live process answering every request 500 and never restarts it." The example's tail becomes:

```rust
    let server = charpente::serve::serve("0.0.0.0:8080", app)?;
    // `serve` returned once the listener was up; `wait` blocks until the
    // kernel stops. Exiting with its code is what lets a supervisor see a
    // stopped kernel and restart it (no graceful shutdown in v0).
    std::process::exit(server.wait());
```

- §7's serve example: the same change.
- §10's `run_primary`/`run_follower`: `std::process::exit(serve::serve_replicated(…)?.wait())`, with the comment "HTTP on 8080, replication on 9090; exits when the kernel stops".
- §10's strict-witnesses paragraph: replace "And **the stop reaches a supervisor under `serve::backup` only** … On an HTTP node, set this flag only if something is watching for those 500s." with "And the stop reaches a supervisor on every node shape: under `serve::backup` the error returns from the call, and under `serve::serve_replicated` the kernel's stop ends the process through `Server::wait` (§5) with status 1 — so a supervisor restarts it, and keeps restarting it until a witness answers."
- §13 `launch`: "When the kernel stops — a platform IO failure, or a named refusal such as a table-gate barrier or a Pending job of a foreign shape (§6) — `launch` exits the process with status 1 after its stderr line names what stopped it and the fix."

DESIGN §5:
- the strict-witnesses text gets the same correction;
- add one paragraph: "**A stopped kernel ends the process.** `serve`/`serve_replicated` run the kernel on a joined thread; `Server::wait` returns its exit code, and `launch` exits with it. The last stderr line names the stop — `refused to start`/`stopped on a refusal` for a `Refusal` (table-gate barrier, witness conflict, strict-witness deadline, foreign-shape jobs), `failed to start`/`stopped on a platform IO failure — restart to recover` otherwise — because a restart clears an IO failure and never a refusal."

- [ ] **Step 7: Gate and commit**

The edge harness is `#[ignore]`d under `cargo test --workspace`. Its two suites run above and are logged in the task report.

```zsh
git add crates/charpente-core/src/db.rs crates/charpente/src/repl/mod.rs crates/charpente/src/serve.rs crates/charpente/src/launch.rs examples/invoicing/src/bin crates/bench-harness/src/bin/bench-app.rs crates/edge-harness docs/GUIDE.md DESIGN.md
git commit -m "feat(serve): a stopped kernel ends the process — Server::wait, a refusal named as one on stderr, failover Phase B asserts the exit"
```

---

### Task 8: Close-out — mutation run, fresh seeds, the counted gate, the record

**Files:**
- Modify `PLAN.md` (M3 sub-project 3's paragraph; the After-launch header and table).
- Modify `docs/superpowers/specs/2026-07-16-migrations-design.md:67-72`.
- Modify `docs/LEDGER.md` (a new entry).
- Modify `.cargo/mutants.toml` only if a survivor is proven equivalent, with a rationale comment, and then `crates/guide-examples/tests/mutants_exclusions.rs`'s pinned count too. A third survivor is a property to write, not an entry to add.

- [ ] **Step 1: The mutation run (spec §6.9)**

Commit before starting. First validate each `--re` against the tool's own list:

```zsh
cargo mutants --list --file crates/charpente/src/kernel/tail.rs | grep deliver_unit | head
```

Then run each target. **Read the verb's output, not cargo-mutants' exit code.**

```zsh
scripts/mutants.sh --file crates/charpente/src/kernel/tail.rs --test-package charpente
scripts/mutants.sh --file crates/charpente/src/kernel/mod.rs --re 'validate_subscribe|refuse_foreign_pending_jobs|after_repl_step' --test-package charpente
scripts/mutants.sh --file crates/charpente/src/http/mod.rs --re 'ev_json' --test-package charpente
scripts/mutants.sh --file crates/charpente-core/src/db.rs --re 'pending_job_shape_refusal|register_job_shapes' --test-package charpente
scripts/mutants.sh --file crates/charpente-core/src/db.rs --re 'decode_record|encode_record'
scripts/mutants.sh --file crates/charpente-core/src/event.rs --file crates/charpente-core/src/job.rs
scripts/mutants.sh --file crates/charpente/src/repl/mod.rs --re 'do_promote' --test-package charpente
scripts/mutants.sh --file crates/charpente/src/serve.rs --re 'kernel_stop_line|wait|encode_frame' --test-package charpente
```

The target is zero MISSED and zero timeouts.
- A MISSED mutant is a one-sided property: write the test that kills it, commit it, and re-run that target.
- Record every run's caught/missed/unviable counts in the report.
- The dispatch assert, the write-gate check, and the absence of a hand-off check were proven by hand mutation in Task 6. List them in the report beside the tool's runs.

- [ ] **Step 2: Fresh seeds**

Run each new property over fresh bases, recording each base and its result:

```zsh
for base in 0x51A1_0001 0x51A1_0002 0x51A1_0003; do
  CHARPENTE_BASE_SEED=$base cargo test -p charpente --test realtime -- a_resume_across_mixed_shapes --exact 2>&1 | grep -E "^test result|FAILED|seed"
  CHARPENTE_BASE_SEED=$base cargo test -p charpente --test kernel -- a_new_job_shape_starts_iff --exact 2>&1 | grep -E "^test result|FAILED|seed"
done
```

`CHARPENTE_BASE_SEED` skips `require:` and `floor:`, so these runs are evidence about failures only, never about coverage. The evidence stated in the commit and the LEDGER is the total universes over fresh seeds (the tests' N × 3 bases, per property). Any failing seed is pinned as a regression test before anything else happens.

- [ ] **Step 3: The full counted gate**

The controller runs it in the background:

```zsh
LOG=.superpowers/sdd/2026-09-24-payload-shapes/gate-final.log
cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings && \
  cargo test --workspace --no-fail-fast 2>&1 | tee $LOG; echo "exit ${pipestatus[1]}"
grep -c "^test result:" $LOG; grep -c "^test result: ok" $LOG; grep -c FAILED $LOG
```

Expected: the two counts are equal (87, unless a binary was added), and 0 FAILED. Also run CI's replay pins and the TS gate, exactly as `.github/workflows/ci.yml` lists them. The `0xe653…` pin may have been re-pinned in Task 1, so use whatever ci.yml now says.

- [ ] **Step 4: The record**

In `PLAN.md`, M3 sub-project 3's closing sentence: after "(1) `#[event]` payload evolution is unguarded, and a delivery-time decode failure stops the whole node with an error naming neither the event nor a fix", insert "— **fixed 2026-09-2x** (spec `docs/superpowers/specs/2026-09-24-payload-shapes-design.md`: every payload records its shape and is never decoded under another; delivery fails a tail, never the node; a stopped kernel ends the process)".

The After-launch header becomes "…one from M3's claims audit, three from M3's payload-shapes fix, each with the trigger that reopens it". Append these rows:

```markdown
| **Versioned migrations for event and job payloads** (payload-shapes spec §8, approach B) | The first app that needs exact resume, or a Pending job, to survive a shape change it cannot express as a new type. |
| **A deep fingerprint** — every `Codec` type carries its own shape constant, so a new variant on a nested enum changes the fingerprint of every table and payload that holds it | The first nested-type change that misdecodes in a real app or a docs-only build. |
| **A commit-time shape lock** — a committed fingerprint file plus a check verb | The first shape change that reaches a deploy unnoticed. Apps on the generated TS client already get a CI signal for event payload changes from `check-client`'s staleness diff. |
```

`docs/superpowers/specs/2026-07-16-migrations-design.md`: after the deferred `#[job]`/`#[event]` follow-up (lines 67–72), add "**Discharged for detection and refusal** by `2026-09-24-payload-shapes-design.md`: every payload records its shape and nothing decodes under a foreign one. Rewriting payloads stays deferred; PLAN.md's after-launch table carries it with its trigger."

`docs/LEDGER.md` gets a new entry at the end, in the house style:
- the heading is `## 2026-09-2x — payload shapes: every emit and enqueue records its shape, nothing decodes under another, and a stopped kernel ends the process`;
- the body says what shipped (one line per task, with its commit sha);
- the seeds: each property's base, N, the fresh bases and total universes, any re-pins from Task 1's census (old → new) and any seed pinned in this work;
- the mutation table;
- the gate counted (`N of N`);
- A10's measured result.

Update memory `charpente-m1-progress.md`: C-1 is fixed; C-2 (the job tenant scope) is next.

- [ ] **Step 5: Commit**

```zsh
git add PLAN.md docs/superpowers/specs/2026-07-16-migrations-design.md docs/LEDGER.md
git status --short   # plus .cargo/mutants.toml and mutants_exclusions.rs only if Step 1 changed them
git commit -m "ledger(payload-shapes): C-1 fixed — mutants clean over every new check, N universes over fresh seeds, the workspace gate counted N of N"
```

Do not push. Pushing, publishing, and starting C-2 are Noah's calls.
