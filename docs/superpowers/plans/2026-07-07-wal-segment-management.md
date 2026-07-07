# WAL Segment Management Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Segment GC, the two durability fences (recovery horizon, directory entries), and the three fault-matrix extensions, per `docs/superpowers/specs/2026-07-07-wal-segment-management-design.md`.

**Architecture:** Bottom-up in six tasks: the `Disk::sync_dir` barrier end-to-end first (trait + prod + sim model + log/store call sites), then `base_seq`/`Wal::gc`, then checkpoint-driven GC + the rebuild rule at the `Db` layer, then the horizon rule, then the three fault axes via field-keyed sub-streams, then the evidence sweep. DESIGN.md edits land inside the task that makes them true (hard rule 6), not in a docs task.

**Tech Stack:** Rust workspace (edition 2024); no new dependencies.

## Global Constraints

- Commit directly to `main`, no Co-Authored-By trailers. One commit per task unless a task says otherwise.
- **Frozen replay contracts** (violating any of these is a Critical defect): `FaultConfig::swarm`'s sequential draw order (`lib.rs:77-124`) gains no draws; `run_universe`'s `cfg_seed = splitmix64(seed)` prelude is unchanged; new FaultConfig fields default to `0.0` and take **zero RNG draws when 0.0** (gate with `if cfg.field > 0.0`), so the frozen legacy runner (`run_universes!`) and `crash_recovery.rs` replay byte-identically.
- New fault axes strengthen properties, never weaken assertions: a swarm universe where a new fault fires may legitimately end in a *named* corruption/IO error (classified by `checks::is_fault_fallout`), but silent data loss or wrong data remains a failure everywhere.
- Every new fault axis ships in the same task as its meta-test (a naive victim the harness must catch, by name) — hard rule 3's spirit.
- Error messages are product surface (hard rule 5): the two new hard errors (rebuild-refused, horizon-corruption) name the state, the contract, and the fix, and are pinned by tests.
- Sim-first evidence: each task ends with the affected suites green over fresh seeds; Task 6 runs the full evidence bar (spec §6).
- `cargo fmt --all --check` and `cargo clippy --workspace --all-targets -- -D warnings` clean before every commit.

---

### Task 1: Directory-entry durability end-to-end (`Disk::sync_dir`)

**Files:**
- Modify: `crates/charpente-core/src/io.rs:88-97` (Disk trait)
- Modify: `crates/charpente/src/prod.rs` (ProdDisk impl, after `list`)
- Modify: `crates/charpente-sim/src/disk.rs` (DiskWorld model), `crates/charpente-sim/src/lib.rs` (SimDisk impl, FaultConfig field, injection)
- Modify: `crates/charpente-core/src/log.rs` (barrier call sites), `crates/charpente-core/src/table.rs` (`Store::open` barrier), `crates/charpente-core/src/db.rs` (rebuild-path barrier)
- Modify: `DESIGN.md` §2 (fence 2 → held)
- Test: `crates/charpente-sim/tests/wal.rs` (new dir-entry tests)

**Interfaces:**
- Produces: `Disk::sync_dir(&self, prefix: &str) -> io::Result<()>` — flat-namespace prefix, same contract as `list`; on `Ok`, every prior create/remove of a matching name survives any crash. `FaultConfig.dir_entry_loss: f64` (default `0.0`) — P(an unsynced directory entry is affected at crash). Coverage key `fault:dir-entry`.

- [ ] **Step 1: Trait + prod impl.** In `io.rs`, after `list`:

```rust
    /// Directory barrier: on `Ok`, every prior create/remove of a name
    /// starting with `prefix` survives any crash. `File::sync` does not
    /// persist a file's directory entry; this does (DESIGN §2).
    fn sync_dir(&self, prefix: &str) -> io::Result<()>;
```

In `prod.rs` (module-wide allow already covers `std::fs`):

```rust
    fn sync_dir(&self, prefix: &str) -> io::Result<()> {
        // Prefixes are flat names ("wal/", or "" for the root); fsync the
        // directory that holds those entries. Same fsync-vs-F_FULLFSYNC
        // caveat as ProdFile::sync — revisit both together.
        let dir = match prefix.rfind('/') {
            Some(i) => self.root.join(&prefix[..i]),
            None => self.root.clone(),
        };
        std::fs::create_dir_all(&dir)?;
        std::fs::File::open(&dir)?.sync_all()
    }
```

- [ ] **Step 2: Sim model (RED first).** Write the failing test in `crates/charpente-sim/tests/wal.rs`:

```rust
#[test]
fn unsynced_segment_name_vanishes_at_crash_without_a_dir_barrier() {
    // Raw-disk test: create a file, sync its BYTES but not its NAME, crash
    // with dir_entry_loss = 1.0 — the file must be gone.
    let cfg = FaultConfig { dir_entry_loss: 1.0, crash_apply: 1.0, ..FaultConfig::none() };
    let sim = Sim::with_faults(0xD1E_0001, cfg);
    let disk = sim.disk();
    let f = disk.open("wal/00000000000000000000.log").unwrap();
    f.write_at(0, b"acked").unwrap();
    f.sync().unwrap();
    sim.crash_disk_now();
    sim.restart();
    assert!(disk.list("wal/").unwrap().is_empty(), "unsynced dirent must be lost");

    // Same again, but with the barrier — the file must survive.
    let sim = Sim::with_faults(0xD1E_0002, FaultConfig { dir_entry_loss: 1.0, crash_apply: 1.0, ..FaultConfig::none() });
    let disk = sim.disk();
    let f = disk.open("wal/00000000000000000000.log").unwrap();
    f.write_at(0, b"acked").unwrap();
    f.sync().unwrap();
    disk.sync_dir("wal/").unwrap();
    sim.crash_disk_now();
    sim.restart();
    assert_eq!(disk.list("wal/").unwrap().len(), 1, "barriered dirent must survive");
}

#[test]
fn unsynced_removal_resurrects_at_crash_without_a_dir_barrier() {
    let cfg = FaultConfig { dir_entry_loss: 1.0, crash_apply: 1.0, ..FaultConfig::none() };
    let sim = Sim::with_faults(0xD1E_0003, cfg);
    let disk = sim.disk();
    let f = disk.open("wal/00000000000000000000.log").unwrap();
    f.write_at(0, b"stale").unwrap();
    f.sync().unwrap();
    disk.sync_dir("wal/").unwrap();
    disk.remove("wal/00000000000000000000.log").unwrap();
    sim.crash_disk_now();
    sim.restart();
    let names = disk.list("wal/").unwrap();
    assert_eq!(names.len(), 1, "unsynced removal must resurrect the file");
    let f = disk.open(&names[0]).unwrap();
    let mut out = [0u8; 5];
    f.read_at(0, &mut out).unwrap();
    assert_eq!(&out, b"stale", "resurrection restores the last durable content");
}
```

(If `FaultConfig::none()` does not exist, add it: all probabilities 0.0, latencies `Duration::ZERO` pairs — check `lib.rs` first; several tests construct zeroed configs inline and may already have a helper.)

Run: `cargo test -p charpente-sim --test wal unsynced` — Expected: FAIL (no `sync_dir`, no model).

- [ ] **Step 3: Implement the sim model.** In `disk.rs`, extend `DiskWorld`:

```rust
    /// Created names whose directory entry is not yet durable.
    pub pending_creates: BTreeSet<String>,
    /// Removed names whose removal is not yet durable, keyed to the durable
    /// content a crash would resurrect.
    pub pending_removes: BTreeMap<String, Vec<u8>>,
```

Semantics (implement in `lib.rs`'s `SimDisk`/`State`, keeping all fault rolls inside `State` like today):
- `open(path)`: if the file is new, insert into `files` AND `pending_creates`. If `path` is in `pending_removes`, keep that entry (the old durable content can still resurrect) and treat the reopen as a fresh pending create.
- `remove(path)`: if the name is in `pending_creates` (never durable), drop both entries outright. Otherwise move the file's `durable` bytes into `pending_removes` and remove from `files`.
- `sync_dir(prefix)`: `retain` away all `pending_creates`/`pending_removes` entries starting with `prefix` (creates become durable names; removals become final). Route through `State::disk_op` so it ticks the crash countdown and pays disk latency like every other disk op; a `sync_error` roll applies to it exactly as to `File::sync` (an injected dir-barrier failure marks `fault:sync_error` and poisons upstream via the caller's guard).
- Crash collapse (in `Sim::restart`, next to the per-file `collapse` loop): only when `cfg.dir_entry_loss > 0.0` (zero-draw fast path — the frozen contract), roll per pending entry: with P(`dir_entry_loss`) the entry is affected — an affected pending create vanishes (file removed from `files` even if its bytes were synced; that is the fence's whole point), an affected pending remove resurrects (`files` entry restored with the stashed durable bytes, empty `pending`). Unaffected entries behave as synced. Mark `"fault:dir-entry"` for every affected entry. For a name with BOTH a pending remove and a pending create (the rebuild path): roll the remove first — resurrect discards the new file's state; if the remove sticks, roll the create.
- All pending entry sets clear on both outcomes after the crash (the crash resolved them).

Add `dir_entry_loss: f64` to `FaultConfig` with default `0.0`, doc comment `/// P(an unsynced directory entry is lost (create) or resurrected (remove) at crash).` Do NOT touch `FaultConfig::swarm` (Task 5 samples it via a sub-stream). Update `validate()` bounds-checking to include it.

Run: `cargo test -p charpente-sim --test wal unsynced` — Expected: PASS.

- [ ] **Step 4: Barrier call sites (RED via property, then green).** The call sites, each `guard`ed where a guard exists:
- `log.rs::Wal::open`, empty-directory branch: after creating segment 0, `disk.sync_dir(&prefix)?;`
- `log.rs::Wal::open`, cut branch: after the `disk.remove(later)` loop, one `disk.sync_dir(&prefix)?;` (recovery leaves a durably-clean directory).
- `log.rs::rotate`: after `self.tail = self.guard(self.disk.open(&name))?;`, add `self.guard(self.disk.sync_dir(&format!("{}/", self.cfg.dir)))?;` — extends the sync-before-rotate rule to the segment's name (the name must be durable before frames land in it).
- `table.rs::Store::open`: after `let file = disk.open(path)?;`, add `disk.sync_dir(path)?;` (prefix = the store path itself matches only the store file; for root-level `tables.redb` the prod impl fsyncs the root).
- `db.rs::Db::open` rebuild arm: after the tolerated `disk.remove(&cfg.store_path)`, add `disk.sync_dir(&cfg.store_path)?;` before re-opening (a resurrected bricked store must not shadow the rebuilt one).

Then extend the swarm property: in `wal.rs::wal_survives_the_fault_matrix`'s config, nothing changes yet (swarm sampling of the field is Task 5) — instead add a targeted always-on property right below it:

```rust
#[test]
fn wal_survives_dir_entry_loss() {
    // The fault matrix with dir-entry loss forced hot: every universe's
    // FaultConfig gets dir_entry_loss = 0.5 stamped on top of the swarm
    // draw. Acked records must still always recover.
    run_universes_swarm_with!(0x01D_D1E5, 256, |cfg| { cfg.dir_entry_loss = 0.5; }, |sim| {
        wal_workload(sim) // the existing workload fn used by wal_survives_the_fault_matrix
    }, require: ["fault:dir-entry", "restart"]);
}
```

If `run_universes_swarm!` has no config-override variant, add `run_universes_swarm_with!` beside it (same body, one extra closure applied to the sampled config before the universe runs — a NEW macro, so no frozen contract is touched; the existing macro delegates to it with a no-op closure). Check the existing macro's shape in `lib.rs` first and match its argument style, including the `require:` handling.

Run: `cargo test -p charpente-sim --test wal` — Expected: `wal_survives_dir_entry_loss` PASSES only because the call sites landed; temporarily comment out the `rotate` barrier and confirm it FAILS (records vanish with the unsynced segment name), restore it, re-run green. Record both outputs — that is the meta-evidence the barrier is load-bearing.

- [ ] **Step 5: DESIGN update + full verification + commit.** DESIGN §2, replace the fence-2 sentence ("(2) `File::sync` does not persist a file's *directory entry*…") with the held form: `Disk::sync_dir` is the directory barrier; the log barriers rotation and recovery cleanup; the store barriers creation and rebuild; the sim models entry loss and resurrection (`dir_entry_loss`).

Run: `cargo test --workspace && cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check` — Expected: all green (legacy pinned seeds unaffected: `dir_entry_loss` defaults 0.0 and takes zero draws).

```bash
git add crates/charpente-core/src crates/charpente/src/prod.rs crates/charpente-sim/src crates/charpente-sim/tests/wal.rs DESIGN.md
git commit -m "core+sim: Disk::sync_dir directory barrier — fence 2 held (segment-mgmt T1)"
```

---

### Task 2: `base_seq` + `Wal::gc`

**Files:**
- Modify: `crates/charpente-core/src/log.rs`
- Test: `crates/charpente-sim/tests/wal.rs`

**Interfaces:**
- Consumes: `Disk::sync_dir` (Task 1).
- Produces: `Wal::base_seq(&self) -> u64`; `Wal::gc(&mut self, keep_from: u64, retain_segments: usize) -> io::Result<()>` — deletes whole segments every frame of which is below `keep_from`, always retaining at least `retain_segments.max(1)` segments, barriering after each removal; recovery starts at the first listed segment's base instead of 0.

- [ ] **Step 1: Failing tests.**

```rust
#[test]
fn gc_deletes_only_whole_segments_below_keep_from() {
    // Tiny segments (64 bytes) force rotation every ~2 records. Fill 40
    // records, then gc(keep_from = 20, retain 1): every segment whose
    // frames are all < 20 is gone, everything >= the first retained base
    // recovers, and base_seq() reports the new floor.
    ...build Wal on a fault-free sim, append 40 x 8-byte payloads, sync...
    wal.gc(20, 1).unwrap();
    let names = disk.list("wal/").unwrap();
    let first_base = parse of names[0];
    assert!(first_base <= 20 && wal.base_seq() == first_base);
    assert!(names.len() >= 1);
    ...reopen via Wal::open, collect records, assert seqs first_base..40 all present...
}

#[test]
fn gc_respects_the_retain_floor() {
    ...same setup; wal.gc(u64::MAX, 3).unwrap();
    assert!(disk.list("wal/").unwrap().len() >= 3, "floor holds even when keep_from allows more");
}

#[test]
fn gc_interrupted_by_crash_never_leaves_a_gap() {
    // Property over the fault matrix with dir_entry_loss hot: append across
    // many segments, gc, crash anywhere, reopen — recovery must never hit
    // the segment-gap refusal, and must recover every record >= base.
    run_universes_swarm_with!(0x6C_0001, 256, |cfg| { cfg.dir_entry_loss = 0.5; }, |sim| {
        ...append N records (sync each), gc(random-ish keep_from derived from sim entropy, retain 2),
        sim.crash_disk_now(); sim.restart();
        reopen; Err(anything containing "a segment is missing") => return Err(...);
        assert prefix-from-base property...
    }, require: ["restart"]);
}
```

Run: `cargo test -p charpente-sim --test wal gc_` — Expected: FAIL (`gc`/`base_seq` don't exist).

- [ ] **Step 2: Implement.** In `log.rs`: add `base_seq: u64` to `Wal`; set it in `open` from the first listed segment (`0` for an empty dir); change `let mut expected_seq = 0u64;` to `let mut expected_seq = parse_segment_name(&cfg.dir, &names[0])?;` and record it as `base_seq` (the existing contiguity check then validates the rest). Accessor + gc:

```rust
    /// Lowest seq the log still retains; recovery starts here.
    pub fn base_seq(&self) -> u64 {
        self.base_seq
    }

    /// Deletes whole segments every frame of which is below `keep_from`,
    /// always keeping at least `retain_segments` segments. The directory
    /// barrier runs after each removal, so a crash can resurrect at most
    /// the most-recently-removed segment — always adjacent to the retained
    /// set, never a gap (a resurrected segment below a durable checkpoint
    /// replays as no-ops through the store's skip_through gate).
    pub fn gc(&mut self, keep_from: u64, retain_segments: usize) -> io::Result<()> {
        self.check_poison()?;
        let prefix = format!("{}/", self.cfg.dir);
        let names = self.guard(self.disk.list(&prefix))?;
        let mut bases = Vec::with_capacity(names.len());
        for name in &names {
            bases.push(parse_segment_name(&self.cfg.dir, name)?);
        }
        let deletable = names.len().saturating_sub(retain_segments.max(1));
        for i in 0..deletable {
            // names[i]'s frames end where names[i + 1] begins.
            if bases[i + 1] > keep_from {
                break;
            }
            self.guard(self.disk.remove(&names[i]))?;
            self.guard(self.disk.sync_dir(&prefix))?;
            self.base_seq = bases[i + 1];
        }
        Ok(())
    }
```

Note the off-by-one honesty: a segment is deletable when `bases[i + 1] <= keep_from`, i.e. every frame in it is `< keep_from` — `keep_from` itself stays.

Run: `cargo test -p charpente-sim --test wal` — Expected: PASS (including the existing rotation/cut/foreign-file tests, whose `expected_seq` start is unchanged for base 0).

- [ ] **Step 3: Verify + commit.**

Run: `cargo test --workspace && cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check` — Expected: green.

```bash
git add crates/charpente-core/src/log.rs crates/charpente-sim/tests/wal.rs
git commit -m "log: base_seq + Wal::gc — checkpoint-anchored segment deletion, gap-free under crash (segment-mgmt T2)"
```

---

### Task 3: Checkpoint-driven GC + the rebuild rule at the `Db` layer

**Files:**
- Modify: `crates/charpente-core/src/db.rs` (DbConfig, Db::open, checkpoint, transact cadence), `crates/charpente-core/src/engine.rs` (expose `wal_mut()` if absent)
- Modify: `DESIGN.md` §4 (rebuild caveat replaced), `docs/GUIDE.md` §3 only if it shows `checkpoint`'s signature (grep first; update in the same commit if so — the guide gate must stay green)
- Test: `crates/charpente/tests/transactions.rs`

**Interfaces:**
- Consumes: `Wal::gc`/`base_seq` (Task 2).
- Produces: `DbConfig.retain_segments: usize` (default 2), `DbConfig.checkpoint_every: u64` (default 4096, 0 = manual only); `Db::checkpoint(&mut self)` (note: now `&mut self`) runs store checkpoint then `wal.gc(min(applied + 1, pin), retain_segments)`; `Db::pin_retention(&mut self, pin: Box<dyn Fn() -> u64 + Send>)`; `Db::open`'s rebuild rule: store unopenable ∧ `base_seq > 0` → hard error.

- [ ] **Step 1: Failing tests** in `crates/charpente/tests/transactions.rs`:

```rust
#[test]
fn checkpoint_gcs_segments_and_recovery_still_replays_the_tail() {
    // Fault-free sim, tiny WAL segments (segment_bytes: 64). Commit ~50
    // transactions, db.checkpoint(), assert disk.list("wal/") shrank and
    // the earliest base > 0; reopen Db, assert every row readable (the
    // retained tail + checkpointed store reconstruct everything).
}

#[test]
fn retention_pin_holds_segments_gc_would_otherwise_delete() {
    // Same setup; db.pin_retention(Box::new(|| 0)); db.checkpoint();
    // assert nothing was deleted (pin at seq 0 wins over the checkpoint).
}

#[test]
fn auto_checkpoint_fires_on_the_record_cadence() {
    // checkpoint_every: 8, segment_bytes: 64. Commit 9 transactions with no
    // manual checkpoint; assert disk.list("wal/") shows GC ran (earliest
    // base > 0) — the cadence checkpointed for us, deterministically.
}

#[test]
fn unopenable_store_with_compacted_wal_is_a_named_hard_error() {
    // Build a Db with checkpoints + GC so base_seq > 0. Close. Mangle the
    // store file the same way mangled_store_len_recovers does (reuse its
    // helper shape at transactions.rs:863-895). Db::open must now Err —
    // NOT rebuild — and the message must name the compaction and the fix:
    let err = ...;
    let msg = err.to_string();
    for needle in ["cannot open", "compacted", "restore", "retain_segments"] {
        assert!(msg.contains(needle), "error must name the state and the fix: {msg}");
    }
}

#[test]
fn unopenable_store_with_full_wal_still_rebuilds() {
    // Same mangling, but no checkpoint/GC ever ran (base_seq == 0):
    // the existing discard-and-rebuild must still work (regression guard
    // for regression_store_bricked_by_creation_crash's path).
}
```

Run: `cargo test -p charpente --test transactions checkpoint_gcs` — Expected: FAIL.

- [ ] **Step 2: Implement.** `DbConfig` gains the two fields with defaults (2, 4096). `Db` gains `cfg: DbConfig`, `retention_pin: Option<Box<dyn Fn() -> u64 + Send>>`, `records_since_checkpoint: u64`. `engine.rs` gains `pub fn wal_mut(&mut self) -> &mut Wal` (one-liner beside the existing accessors). `Db::checkpoint`:

```rust
    /// Durably persists the store, bounding recovery replay at this seq,
    /// then deletes WAL segments below the checkpoint (respecting the
    /// retention pin and DbConfig::retain_segments).
    pub fn checkpoint(&mut self) -> io::Result<()> {
        self.engine.machine().store.checkpoint()?;
        let applied = self.engine.machine().store.applied_seq()?;
        let mut keep_from = applied.map_or(0, |s| s + 1);
        if let Some(pin) = &self.retention_pin {
            keep_from = keep_from.min(pin());
        }
        self.records_since_checkpoint = 0;
        self.engine.wal_mut().gc(keep_from, self.cfg.retain_segments)
    }
```

Cadence: at the end of `Db::transact`'s success path (after the effects record is applied and before returning `Ok`), and in the equivalent job/cron commit path if it does not route through `transact` (check `db.rs` — if all commits funnel through one `submit` site, hook there instead, once):

```rust
        self.records_since_checkpoint += 1;
        if self.cfg.checkpoint_every > 0 && self.records_since_checkpoint >= self.cfg.checkpoint_every {
            self.checkpoint().map_err(TxError::Io)?; // an fsync failure here poisons like any other (fsyncgate)
        }
```

Rebuild rule in `Db::open`'s `Err` arm, BEFORE the `disk.remove`: list the WAL dir, parse the first segment name (reuse `parse_segment_name` — make it `pub(crate)` if needed); if that base > 0:

```rust
            return Err(io::Error::other(format!(
                "table store cannot open ({e}) and the log is compacted to seq {base} — \
                 the records below the store's last checkpoint no longer exist locally, \
                 so the store cannot be rebuilt from the log alone; restore this node \
                 from a replica or backup, or widen DbConfig::retain_segments \
                 (DESIGN §4)"
            )));
```

Also update the stale comment at `db.rs:452-454` ("Safe only because the WAL is never compacted…") to describe the new rule. DESIGN §4: replace the "That rebuild is safe only while the WAL is never compacted (it retains seq 0); WAL segment GC must revisit it." sentence with the held rule (rebuild when `base_seq == 0`; named hard error otherwise; checkpoint cadence + `retain_segments`).

Run: `cargo test -p charpente --test transactions` — Expected: PASS, including every pre-existing test (the four existing `checkpoint()` call sites in tests need `&mut` — update them).

- [ ] **Step 3: Verify + commit.**

Run: `grep -n "checkpoint" docs/GUIDE.md` — if any shown signature drifted, fix it and run `cargo build -p guide-examples`. Then `cargo test --workspace && cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check` — Expected: green.

```bash
git add crates/charpente-core/src DESIGN.md docs/GUIDE.md crates/charpente/tests/transactions.rs
git commit -m "db: checkpoint cadence + segment GC + the compacted-log rebuild rule (segment-mgmt T3)"
```

---

### Task 4: The recovery-horizon rule

**Files:**
- Modify: `crates/charpente-core/src/log.rs` (`open_with_horizon`), `crates/charpente-core/src/engine.rs` (`recover` passes horizon), `crates/charpente-core/src/db.rs` (`Db::open` supplies `applied_seq`)
- Modify: `DESIGN.md` §2 (fence 1 → held)
- Test: `crates/charpente-sim/tests/wal.rs`, `crates/charpente/tests/transactions.rs`

**Interfaces:**
- Consumes: `base_seq` (Task 2 — the horizon error also reports the base).
- Produces: `Wal::open_with_horizon(disk, cfg, horizon: Option<u64>, on_record)`; `Wal::open(disk, cfg, on_record)` ≡ `open_with_horizon(disk, cfg, None, …)` (standalone behavior unchanged); `Engine::recover(disk, cfg, machine, horizon: Option<u64>)`.

- [ ] **Step 1: Failing tests.** In `wal.rs`:

```rust
#[test]
fn corruption_below_the_horizon_is_a_hard_error_not_a_cut() {
    // Fault-free sim: append seqs 0..10, sync. Flip one byte inside frame 3
    // (reuse bit_flip_cuts_the_log_there_and_deletes_later_segments's
    // frame-locating helper at wal.rs:214-251). Reopen with horizon Some(7):
    let err = Wal::open_with_horizon(disk, cfg, Some(7), |_, _| Ok(())).unwrap_err();
    let msg = err.to_string();
    for needle in ["invalid", "durable horizon", "acknowledged", "restore"] {
        assert!(msg.contains(needle), "horizon error must name the contract: {msg}");
    }
    // And the log was NOT truncated: reopening with horizon None still
    // cuts at frame 3 (proving the hard-error path mutated nothing).
}

#[test]
fn corruption_above_the_horizon_still_cuts() {
    // Same setup, horizon Some(2): frame 3 is above the horizon — recovery
    // cuts there exactly as Wal::open does today, recovering seqs 0..=2.
}
```

In `transactions.rs`:

```rust
#[test]
fn db_open_routes_the_store_horizon_into_recovery() {
    // Commit 10 transactions, db.checkpoint() (horizon now 9), close.
    // Corrupt one durable byte inside an early WAL frame. Db::open must
    // return the horizon hard error, not silently open with fewer rows.
}
```

Run: `cargo test -p charpente-sim --test wal corruption_` — Expected: FAIL (no `open_with_horizon`).

- [ ] **Step 2: Implement.** In `log.rs`, rename the body of `open` to `open_with_horizon` with the extra `horizon: Option<u64>` param; `open` delegates with `None`. In the frame loop, the `Frame::Invalid` arm becomes:

```rust
                Frame::Invalid => {
                    if horizon.is_some_and(|h| expected_seq <= h) {
                        return Err(corrupt(format!(
                            "log frame {expected_seq} is invalid but at-or-below the store's \
                             durable horizon {h} — this is corruption of acknowledged records, \
                             not a torn tail, and truncating it would silently lose data; \
                             restore this node from a replica or backup (DESIGN §2)",
                            h = horizon.expect("checked by is_some_and"),
                        )));
                    }
                    break Some(offset);
                }
```

`Engine::recover` (engine.rs:43-61) gains the `horizon: Option<u64>` param and passes it through; `Db::open` calls `Engine::recover(disk, cfg.wal, machine, skip_through)` — the horizon IS `skip_through` (the store's `applied_seq` read at `db.rs:463`). The one other `Engine::recover` caller (grep; wal.rs tests use `Wal::open` directly) passes `None`. DESIGN §2: move fence 1 from tracked to held, with the rule as implemented.

Run: `cargo test -p charpente-sim --test wal && cargo test -p charpente --test transactions` — Expected: PASS (existing cut-behavior tests unchanged — they use `Wal::open` = horizon `None`).

- [ ] **Step 3: Verify + commit.**

Run: `cargo test --workspace && cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check` — Expected: green.

```bash
git add crates/charpente-core/src DESIGN.md crates/charpente-sim/tests/wal.rs crates/charpente/tests/transactions.rs
git commit -m "log+db: recovery-horizon rule — corruption below applied_seq is a named hard error, never a cut (segment-mgmt T4)"
```

---

### Task 5: Fault-matrix extensions — bit flips, misdirected writes, latent read errors

**Files:**
- Modify: `crates/charpente-sim/src/lib.rs` (FaultConfig fields, field-keyed sub-stream sampling in `run_universe`, injection sites), `crates/charpente-sim/src/disk.rs` (bit-flip at collapse), `crates/charpente-sim/src/checks.rs` (`is_fault_fallout` learns the two new legitimate-error wordings)
- Modify: `DESIGN.md` §1 (tracked extensions → implemented)
- Test: `crates/charpente-sim/tests/wal.rs` (meta-tests + require keys), plus require-key additions in `crates/charpente/tests/{transactions,typed_tables}.rs`

**Interfaces:**
- Consumes: horizon rule (Task 4 — bit flips below the horizon must produce the named error, which `is_fault_fallout` classifies).
- Produces: `FaultConfig.{bit_flip, misdirected_write, read_error}: f64` (defaults 0.0); coverage keys `fault:bit_flip`, `fault:misdirected_write`, `fault:read_error`; field-keyed sub-stream sampler `sample_extended_fields(seed, &mut FaultConfig)` applied inside `run_universe` after the frozen legacy sampling.

- [ ] **Step 1: The sub-stream sampler (with its own pin test, RED first).**

```rust
#[test]
fn extended_field_sampling_never_touches_the_legacy_stream() {
    // The frozen contract's discriminating test: for a fixed seed, the
    // legacy fields must be IDENTICAL with and without extended sampling.
    let mut s = 0x5EED_F00D_u64;
    let cfg_seed = splitmix64(&mut s);
    let legacy = FaultConfig::swarm(&mut Rng::new(cfg_seed));
    let mut extended = legacy.clone();
    sample_extended_fields(0x5EED_F00D, &mut extended);
    assert_eq!(legacy.write_error, extended.write_error);
    assert_eq!(legacy.sync_error, extended.sync_error);
    assert_eq!(legacy.crash_apply, extended.crash_apply);
    assert_eq!(legacy.crash_torn, extended.crash_torn);
    // And the extended fields are deterministic per (seed, field name):
    let mut again = legacy.clone();
    sample_extended_fields(0x5EED_F00D, &mut again);
    assert_eq!(extended.bit_flip, again.bit_flip);
    assert_eq!(extended.misdirected_write, again.misdirected_write);
    assert_eq!(extended.read_error, again.read_error);
    assert_eq!(extended.dir_entry_loss, again.dir_entry_loss);
}
```

Implementation in `lib.rs` (placed right below `FaultConfig::swarm`, with the same do-not-reorder doc discipline — each field's stream is independent, so ADDING fields later never changes existing ones):

```rust
/// Extended fault fields draw from per-field sub-streams keyed by
/// (universe seed, field name) — never from the legacy sequential stream
/// (lib.rs:77's frozen contract). Adding a field here changes no existing
/// universe's legacy draws and no other extended field's draws.
pub fn sample_extended_fields(seed: u64, cfg: &mut FaultConfig) {
    fn stream(seed: u64, name: &str) -> Rng {
        let mut h = 0xcbf2_9ce4_8422_2325u64; // FNV-1a, matching crc.rs's style
        for b in name.bytes() {
            h ^= b as u64;
            h = h.wrapping_mul(0x0000_0100_0000_01b3);
        }
        let mut s = seed ^ h;
        Rng::new(splitmix64(&mut s))
    }
    let mut r = stream(seed, "bit_flip");
    cfg.bit_flip = if r.chance(0.25) { r.below(20) as f64 / 1000.0 } else { 0.0 };
    let mut r = stream(seed, "misdirected_write");
    cfg.misdirected_write = if r.chance(0.25) { r.below(10) as f64 / 1000.0 } else { 0.0 };
    let mut r = stream(seed, "read_error");
    cfg.read_error = if r.chance(0.25) { r.below(50) as f64 / 1000.0 } else { 0.0 };
    let mut r = stream(seed, "dir_entry_loss");
    cfg.dir_entry_loss = if r.chance(0.25) { r.below(500) as f64 / 1000.0 } else { 0.0 };
}
```

Wire it into `run_universe` immediately after `let cfg = FaultConfig::swarm(...)` (making Task 1's field swarm-sampled too). Run the pin test — Expected: PASS. Run `cargo test -p charpente-sim --test crash_recovery` — Expected: PASS untouched (legacy runner never calls `sample_extended_fields`).

- [ ] **Step 2: The three injection sites (each with zero-draw fast path when 0.0).**
- `read_error`: in `SimFile::read_at` (`lib.rs`, beside `write_at`'s injection): `if cfg.read_error > 0.0 && rng.chance(cfg.read_error) { mark "fault:read_error"; return Err(io::Error::other("injected read error")); }` — transient (the next read may succeed).
- `misdirected_write`: in `SimFile::write_at`, after the existing `write_error` roll: `if cfg.misdirected_write > 0.0 && rng.chance(...)` → push the `PendingOp::Write` at `offset + 512 * (1 + rng.below(4))` instead of `offset` (the intended target stays stale; the payload lands beyond it), mark `fault:misdirected_write`. The write call still returns `Ok` — that is the fault's whole treachery.
- `bit_flip`: in `Sim::restart` before per-file collapse: `if cfg.bit_flip > 0.0 && rng.chance(cfg.bit_flip)` → pick one file with non-empty `durable` (by rng index over sorted names), flip one random bit of one random byte, mark `fault:bit_flip`. (Restart-time durable corruption — the bitrot model; per-restart probability keeps it rare enough that universes usually survive.)

- [ ] **Step 3: Teach the checker, then the meta-tests (RED each, by construction).** `checks::is_fault_fallout` gains the two new legitimate wordings: `"durable horizon"` (Task 4's error) and `"injected read error"`. Then three meta-tests in `wal.rs`, following `harness_catches_naive_recovery`'s structure (`crash_recovery.rs:184-209`) — each builds a deliberately naive victim and scans seeds until the harness catches it, asserting it takes < N seeds:

```rust
#[test]
fn harness_catches_recovery_that_trusts_lengths_without_crc() {
    // Victim: reopen path that accepts any frame whose length fits, no CRC
    // re-check — with bit_flip forced hot, a flipped durable byte inside a
    // frame body must produce a wrong-payload detection within 64 seeds.
}

#[test]
fn harness_catches_a_store_that_never_reads_back() {
    // Victim: a toy writer that write_at's then trusts the write landed —
    // with misdirected_write forced hot, read-back must diverge within 64 seeds.
}

#[test]
fn harness_catches_recovery_that_reads_through_errors_as_eof() {
    // Victim: recovery treating read_at Err as CleanEnd — with read_error
    // forced hot it silently drops acked records within 64 seeds.
}
```

Each uses `run_universes_swarm_with!` to force the one axis hot (e.g. `cfg.bit_flip = 0.05`). Then add `require: ["fault:bit_flip"]`-style keys: `wal_survives_the_fault_matrix` gains nothing (extended fields are sampled, sometimes 0 — batch-level require would flake); instead each axis gets its own always-hot 256-universe property test in `wal.rs` asserting the REAL `Wal` + horizon rule survives it (acked records recovered OR the named horizon/read error surfaced — never silent loss), with `require:` for its key. Update `transactions.rs`/`typed_tables.rs` swarm workloads only where a new legitimate error reaches them: route unrecognized errors through the extended `is_fault_fallout` (grep for existing `is_injected`/fallout helpers — the step-6 note says all three property files use checkpoint `is_injected` routing; extend those helpers' wording lists rather than duplicating).

- [ ] **Step 4: DESIGN + verify + commit.** DESIGN §1: the tracked-extensions sentence ("Tracked extensions to the matrix…") becomes the implemented list with the field names and the field-keyed sub-stream rule. Run: `cargo test --workspace && cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check` — Expected: green, including every pinned regression seed (their legacy draws are untouched; where an extended fault now fires in their universe, the code under test must survive it — that is the point; if a pinned-seed test fails, that is a REAL finding: diagnose before touching the test, and if the failure is a new-axis bug, fix the code and note the seed).

```bash
git add crates/charpente-sim/src crates/charpente-sim/tests/wal.rs crates/charpente/tests DESIGN.md
git commit -m "sim: bit-flip, misdirected-write, latent-read-error axes via field-keyed sub-streams — Jepsen granularity lesson in the matrix (segment-mgmt T5)"
```

---

### Task 6: Evidence sweep + close-out

CONTROLLER TASK.

- [ ] Fresh swarm batches: `cargo test -p charpente-sim --test wal && cargo test -p charpente --test transactions --test typed_tables --test kernel` twice (two fresh master seeds each — the suites draw fresh seeds per run). Expected: green both times.
- [ ] Pinned-seed spot replay: `CHARPENTE_SEED=0x8bee1752811ae873 cargo test -p charpente-sim --test crash_recovery` — Expected: green (frozen path byte-identical).
- [ ] Demo app on the new WAL: `cargo run -p charpente-cli -- test --sim --dir examples/invoicing --universes 1024 --jobs 8` — Expected: exit 0; record the master seed.
- [ ] `cargo test --workspace && cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check` — Expected: clean.
- [ ] CLAUDE.md current-state: add the sub-project-1 entry (fences held, GC live with cadence, fault axes + sub-stream rule, evidence seeds) and update the M1 "Next:" line to sub-project 2 (replication). Commit: `docs: M1 sub-project 1 done — segment GC, fences held, fault matrix extended (evidence in message)` with the recorded seeds in the body.
- [ ] Ledger: append per-task completion lines and the evidence seeds.
