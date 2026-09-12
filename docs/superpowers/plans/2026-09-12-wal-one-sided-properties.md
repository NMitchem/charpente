# WAL One-Sided Properties Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the fifteen MISSED mutants the 2026-09-10 sweep named in `crates/charpente-core/src/log.rs` — eleven with properties that are watched turning RED under their mutant, two with semantics-preserving rewrites whose new mutants are watched being caught, two with commented exclusions under a pinned ceiling — so that `scripts/mutants.sh --file crates/charpente-core/src/log.rs --jobs 1` exits 0.

**Architecture:** Seven new tests in `crates/charpente-sim/tests/wal.rs` (the WAL's oracle file, inside the verb's default `--test-package` scope), using the file's existing vocabulary plus two test-local `Disk` wrappers and one raw-frame builder. Three small edits in `log.rs` (two comparison rewrites, one three-line helper extraction), each behaviour-preserving. A two-entry `.cargo/mutants.toml` and a `guide-examples` gate that pins its size and demands a rationale comment per entry.

**Tech Stack:** Rust; `charpente_sim::Sim`/`SimDisk`; `charpente_core::log::{Wal, WalConfig, frame_crc, MAX_RECORD_BYTES}`; cargo-mutants 27.1.0 via `charpente test --mutants`.

**Spec:** `docs/superpowers/specs/2026-09-12-wal-one-sided-properties-design.md` — the classification table (§2) is the authority on which mutant is which; the plan argues from it.

## Global Constraints

Every task's requirements implicitly include this section.

- **No WAL behaviour change** (spec §6) beyond Task 5's two `!=` rewrites, its `assert!(cut <= len)`, and the `torn_header` extraction — all three semantics-preserving. If a property seems to need the code to change, the property is wrong or the finding is a defect for its own sub-project: record it in `decisions-pending.md` (below) and stop that task.
- **RED first, observed** (spec §3, "The RED discipline"). For every property: (a) the test is GREEN on the original; (b) the named mutant is applied by hand with the exact `perl` one-liner given, the test is run and is RED, and the failure text is pasted into the report; (c) `git checkout -- crates/charpente-core/src/log.rs`, GREEN again, `git status --short` clean. A property never watched failing under its mutant is decoration. Every one-liner's anchor is a unique line in `log.rs` at the plan commit; each task greps for it first and `grep -c` must print `1`.
- **No new simulator fault axis; no exclusion beyond the two named; no sweep beyond `log.rs`** (spec §6).
- **Verification is foregrounded.** Never background `cargo test`. The only backgrounded command in this plan is the final sweep, which the controller owns.
- **`--no-fail-fast` is load-bearing.** Count `^test result: ok` lines from a tee'd log; never infer green from the absence of `FAILED`.
- **State the expected count BEFORE running.** `cargo test -p charpente-sim --test wal` is **33** tests on the plan commit; Task 1 makes it 36, Task 2 37, Task 3 39, Task 4 40. The whole-workspace count is **84** `test result: ok` on `a93f100`; Task 6 adds one binary (`guide-examples/tests/mutants_exclusions.rs`) → **85**. Tasks 1–5 add no binary.
- **This shell is zsh.** `${pipestatus[1]}`, not `${PIPESTATUS[0]}`; quote `--include="*.rs"`; an unquoted `$var` is one word.
- **Pipe cargo through `tee`, never `tail`.**
- **Worktree and target dir.** All work in `/Users/noahmitchem/Documents/GitHub/charpente-wal-properties` on branch `wal-properties` (base: `git merge-base main wal-properties`), with `export CARGO_TARGET_DIR=/tmp/charpente-wal-properties-target` in every shell. Never share a `CARGO_TARGET_DIR` across worktrees.
- **`timeout`/`gtimeout` are absent on this machine.**
- **Commit named paths only**, never `git add -A`. Every commit ends with `Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>` and `Claude-Session: https://claude.ai/code/session_017gCrhXqM7QqtVbwYF1266M`.
- **Any claim about a commit is checked with `git show <sha>:<path>`**, never the working tree.
- **Dispatches are unnamed**, and every agent writes its report to a FILE under `.superpowers/sdd/2026-09-12-wal-one-sided-properties/` before returning.
- **Blocked work** goes to `.superpowers/sdd/2026-09-12-wal-one-sided-properties/decisions-pending.md` with the reading taken, the reasoning, and what would falsify it.
- **Scratch mutations are removed before the commit**, and the removal is verified with `git diff --stat` showing only the intended files.

## File Structure

| File | Responsibility | Task |
|---|---|---|
| `crates/charpente-sim/tests/wal.rs` | The WAL's oracle file: gains seven properties, two test-local `Disk` wrappers (`FailOneSync`, `LyingRemove`) and `raw_frame` | 1–4 |
| `crates/charpente-core/src/log.rs` | `cut != len` + assert; `tail_offset != 0`; `torn_header` helper | 5 |
| `.cargo/mutants.toml` | NEW — two commented `exclude_re` entries | 6 |
| `crates/guide-examples/tests/mutants_exclusions.rs` | NEW — the ceiling gate: exactly `EXCLUSIONS` entries, each with a `#` rationale line above it | 6 |
| `CLAUDE.md` | Test list gains `mutants_exclusions`; instrument bullet names the list and the ceiling; workspace-map bullet names the gate | 6 |
| `crates/charpente-cli/src/mutants.rs` | Module doc names the exclusion list | 6 |
| `scripts/mutants.sh` | Header comment names the exclusion list | 6 |

The final sweep (spec §7.5) is controller-owned after Task 6 and is not a task.

---

### Task 1: The three boundary properties — rotation, `read_from`, `gc`

**Files:**
- Modify: `crates/charpente-sim/tests/wal.rs` (append at end of file)

**Interfaces:**
- Consumes: `quiet_sim()`, `tiny_segments()` (64-byte segments), `base_of(&str) -> u64`, `Wal::{open, append, sync, gc, read_from, base_seq}` — all already in the file.
- Produces: nothing later tasks consume.

- [ ] **Step 1: Confirm the three mutant anchors are unique**

```bash
cd /Users/noahmitchem/Documents/GitHub/charpente-wal-properties && export CARGO_TARGET_DIR=/tmp/charpente-wal-properties-target
grep -c 'frame_len > self.cfg.segment_bytes' crates/charpente-core/src/log.rs
grep -c 'if seq > up_to {' crates/charpente-core/src/log.rs
grep -c 'if bases\[i + 1\] > keep_from {' crates/charpente-core/src/log.rs
```
Expected: `1`, `1`, `1`.

- [ ] **Step 2: Append the three tests**

```rust

// ---------------------------------------------------------------------------
// Boundaries the 2026-09-10 mutation sweep found unpinned (spec
// docs/superpowers/specs/2026-09-12-wal-one-sided-properties-design.md §3).
// Each of these was watched going RED under its named mutant before landing.

#[test]
fn a_frame_that_exactly_fills_the_segment_stays_in_it() {
    // 64-byte segments, 16-byte payloads -> 32-byte frames: two fill a
    // segment to the byte. The rotation gate is `tail_offset + frame_len >
    // segment_bytes`; at exactly `==` the frame belongs to THIS segment, and
    // the `>=` mutant rotated it one record early with nothing noticing.
    let sim = quiet_sim();
    let mut wal = Wal::open(Box::new(sim.disk()), tiny_segments(), |_, _| Ok(())).unwrap();
    wal.append(&[1u8; 16]).unwrap();
    wal.append(&[2u8; 16]).unwrap();
    wal.sync().unwrap();
    let names = sim.disk().list("wal/").unwrap();
    assert_eq!(names.len(), 1, "an exact fit does not rotate: {names:?}");
    assert_eq!(
        sim.disk().open(&names[0]).unwrap().len().unwrap(),
        64,
        "the segment is full to the byte"
    );
    wal.append(&[3u8; 16]).unwrap();
    wal.sync().unwrap();
    let names = sim.disk().list("wal/").unwrap();
    assert_eq!(names.len(), 2, "one byte over the capacity rotates: {names:?}");
}

#[test]
fn read_from_includes_up_to_when_its_frame_opens_the_next_segment() {
    // The log of read_from_returns_verified_frames_across_segments: 8-byte
    // payloads, two 24-byte frames per 64-byte segment, bases 0, 2, 4, ….
    // `up_to = 26` is the FIRST frame of a segment, so the scan reaches the
    // clean end of [24, 25] with `seq == up_to` still unread — the boundary
    // the sibling's odd `up_to` never touches, and where both `>` mutants
    // (`==` and `>=`) break out of the segment loop and drop the last frame.
    let sim = quiet_sim();
    let mut wal = Wal::open(Box::new(sim.disk()), tiny_segments(), |_, _| Ok(())).unwrap();
    let records: Vec<Vec<u8>> = (0..40u8).map(|i| vec![i; 8]).collect();
    for record in &records {
        wal.append(record).unwrap();
    }
    wal.sync().unwrap();
    let names = sim.disk().list("wal/").unwrap();
    assert!(
        names.iter().any(|n| base_of(n) == 26),
        "26 must open a segment for this to be the boundary: {names:?}"
    );

    let got = wal.read_from(13, 26, usize::MAX).unwrap();
    assert_eq!(
        got.iter().map(|(s, _)| *s).collect::<Vec<_>>(),
        (13..=26).collect::<Vec<_>>(),
        "up_to is inclusive even when its frame opens the next segment"
    );
    assert_eq!(got.last().unwrap().1, records[26]);
}

#[test]
fn gc_deletes_the_segment_whose_successor_starts_at_keep_from() {
    // Bases 0, 2, 4, …: `gc(4, 1)` may delete [0,1] and [2,3] — every frame
    // in both is below 4 — and must stop at [4,5]. The sibling
    // gc_deletes_only_whole_segments_below_keep_from asserts only
    // `first_base <= keep_from`, which a gc that stops one segment early
    // satisfies; this pins the boundary exactly.
    let sim = quiet_sim();
    let mut wal = Wal::open(Box::new(sim.disk()), tiny_segments(), |_, _| Ok(())).unwrap();
    for i in 0..40u8 {
        wal.append(&[i; 8]).unwrap();
    }
    wal.sync().unwrap();

    wal.gc(4, 1).unwrap();

    let names = sim.disk().list("wal/").unwrap();
    assert_eq!(
        base_of(&names[0]),
        4,
        "the segment ending exactly at keep_from is deletable: {names:?}"
    );
    assert_eq!(wal.base_seq(), 4);
}
```

- [ ] **Step 3: GREEN on the original — predict 36**

```bash
cargo fmt --all && cargo test -p charpente-sim --test wal 2>&1 | tee /tmp/wal-t1-green.log | grep -E '^test result|FAILED'; echo "exit ${pipestatus[1]}"
```
Expected: `test result: ok. 36 passed`, exit 0.

- [ ] **Step 4: RED under mutant #5 (rotation `>` → `>=`)**

```bash
perl -pi -e 's/frame_len > self\.cfg\.segment_bytes/frame_len >= self.cfg.segment_bytes/' crates/charpente-core/src/log.rs && git diff --stat
cargo test -p charpente-sim --test wal a_frame_that_exactly_fills 2>&1 | grep -E '^test |left:|right:|panicked'
git checkout -- crates/charpente-core/src/log.rs && git status --short
```
Expected: the stat shows `log.rs | 2 +-`; the test FAILS with `an exact fit does not rotate` and `left: 2 / right: 1`; after checkout the only change in the tree is `wal.rs`. Paste the failure.

- [ ] **Step 5: RED under mutants #6 and #7 (`read_from`'s `>` → `==`, then `>=`)**

```bash
perl -pi -e 's/if seq > up_to \{/if seq == up_to {/' crates/charpente-core/src/log.rs && git diff --stat
cargo test -p charpente-sim --test wal read_from_includes_up_to 2>&1 | grep -E '^test |left:|right:|panicked'
git checkout -- crates/charpente-core/src/log.rs
perl -pi -e 's/if seq > up_to \{/if seq >= up_to {/' crates/charpente-core/src/log.rs && git diff --stat
cargo test -p charpente-sim --test wal read_from_includes_up_to 2>&1 | grep -E '^test |left:|right:|panicked'
git checkout -- crates/charpente-core/src/log.rs && git status --short
```
Expected: BOTH runs FAIL with `up_to is inclusive even when its frame opens the next segment`, `left` ending at 25 and `right` at 26. Paste both. If the `==` run stays green, STOP and report: the spec's reading of the loop (§2 row 6) would be wrong.

- [ ] **Step 6: RED under mutant #8 (`gc`'s `>` → `>=`)**

```bash
perl -pi -e 's/if bases\[i \+ 1\] > keep_from \{/if bases[i + 1] >= keep_from {/' crates/charpente-core/src/log.rs && git diff --stat
cargo test -p charpente-sim --test wal gc_deletes_the_segment_whose_successor 2>&1 | grep -E '^test |left:|right:|panicked'
git checkout -- crates/charpente-core/src/log.rs && git status --short
```
Expected: FAILS with `left: 2 / right: 4`. Paste it. `git status --short` shows only ` M crates/charpente-sim/tests/wal.rs`.

- [ ] **Step 7: Commit**

```bash
cargo fmt --all --check && cargo clippy -p charpente-sim --all-targets -- -D warnings 2>&1 | tail -n 1
git add crates/charpente-sim/tests/wal.rs
git commit -F - <<'EOF'
test(wal): three boundaries the sweep found unpinned — exact-fit rotation, read_from at a segment head, gc at the watermark

Each watched going RED under its mutant: `>=` in append's rotation gate
rotates an exact fit one record early; both `read_from` mutants at
`seq > up_to` drop the last frame when up_to opens the next segment;
`>=` in gc keeps the segment whose successor starts at keep_from.
charpente-sim wal: 36 passed.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_017gCrhXqM7QqtVbwYF1266M
EOF
```

---

### Task 2: The poison property

**Files:**
- Modify: `crates/charpente-sim/tests/wal.rs` (append at end; add `use std::sync::atomic::{AtomicBool, Ordering}; use std::sync::Arc;` and `use charpente_sim::SimDisk;` to the imports — `cargo fmt` orders them)

**Interfaces:**
- Consumes: `charpente_core::io::{Disk, File}` (already imported), `charpente_sim::SimDisk` (the type `sim.disk()` returns).
- Produces: `FailOneSync` — test-local; Task 4's `LyingRemove` is a sibling, not a consumer.

- [ ] **Step 1: Confirm the anchors**

```bash
grep -c '        self.poisoned$' crates/charpente-core/src/log.rs
grep -c 'if self.poisoned {' crates/charpente-core/src/log.rs
```
Expected: `1` (the accessor body line, exactly eight spaces then `self.poisoned`), `1`.

- [ ] **Step 2: Append the wrapper and the test**

```rust

/// A disk whose files fail `sync` once, on demand, then behave. Poison is
/// one-shot by nature — after one failed fsync the durability of everything
/// since the last good one is unknown, and a later fsync that succeeds
/// cannot un-know it — so the property needs a disk that fails exactly once
/// and heals, a shape the fault matrix's `sync_error` RATE does not express.
/// Test-local on purpose (spec §6): not a fault mode, an injection.
struct FailOneSync {
    inner: SimDisk,
    /// Set by the test right before the sync that must fail; the file clears
    /// it as it fails, so every later sync goes through.
    arm: Arc<AtomicBool>,
}

struct FailOneSyncFile {
    inner: Box<dyn File>,
    arm: Arc<AtomicBool>,
}

impl Disk for FailOneSync {
    fn open(&self, path: &str) -> io::Result<Box<dyn File>> {
        Ok(Box::new(FailOneSyncFile {
            inner: self.inner.open(path)?,
            arm: self.arm.clone(),
        }))
    }
    fn remove(&self, path: &str) -> io::Result<()> {
        self.inner.remove(path)
    }
    fn list(&self, prefix: &str) -> io::Result<Vec<String>> {
        self.inner.list(prefix)
    }
    fn sync_dir(&self, prefix: &str) -> io::Result<()> {
        self.inner.sync_dir(prefix)
    }
}

impl File for FailOneSyncFile {
    fn len(&self) -> io::Result<u64> {
        self.inner.len()
    }
    fn read_at(&self, offset: u64, out: &mut [u8]) -> io::Result<()> {
        self.inner.read_at(offset, out)
    }
    fn write_at(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        self.inner.write_at(offset, data)
    }
    fn set_len(&self, len: u64) -> io::Result<()> {
        self.inner.set_len(len)
    }
    fn sync(&self) -> io::Result<()> {
        if self.arm.swap(false, Ordering::SeqCst) {
            return Err(io::Error::other("injected fsync failure"));
        }
        self.inner.sync()
    }
}

#[test]
fn a_failed_sync_poisons_the_log_and_every_later_call_is_refused() {
    // The WAL's own poison, as distinct from the Engine/Db flag the
    // apply_failure_poisons_the_engine_until_recovery test covers: the
    // accessor was read by no test, and check_poison's body could be
    // replaced with Ok(()) — a poisoned log that refuses nothing — with
    // nothing noticing (three mutants, two functions, 2026-09-10 sweep).
    let sim = quiet_sim();
    let arm = Arc::new(AtomicBool::new(false));
    let disk = FailOneSync {
        inner: sim.disk(),
        arm: arm.clone(),
    };
    let mut wal = Wal::open(Box::new(disk), tiny_segments(), |_, _| Ok(())).unwrap();
    wal.append(&[1u8; 8]).unwrap();
    assert!(!wal.poisoned(), "a healthy log is not poisoned");

    arm.store(true, Ordering::SeqCst);
    let err = wal.sync().unwrap_err();
    assert_eq!(err.to_string(), "injected fsync failure");
    assert!(wal.poisoned(), "one failed fsync poisons the log");
    assert!(
        !arm.load(Ordering::SeqCst),
        "the injection fired exactly once; the disk is healthy from here"
    );

    for (what, result) in [
        ("append", wal.append(&[2u8; 8]).map(|_| ())),
        ("sync", wal.sync()),
        ("gc", wal.gc(0, 1)),
    ] {
        let err = result.unwrap_err();
        assert!(
            err.to_string()
                .contains("WAL poisoned by an earlier IO failure"),
            "{what} after poison must be refused by name; got: {err}"
        );
    }
    assert!(wal.poisoned(), "refusals do not clear the poison");
}
```

- [ ] **Step 3: GREEN — predict 37**

```bash
cargo fmt --all && cargo test -p charpente-sim --test wal 2>&1 | tee /tmp/wal-t2-green.log | grep -E '^test result|FAILED'; echo "exit ${pipestatus[1]}"
```
Expected: `37 passed`, exit 0. If `Wal::open` itself fails with `injected fsync failure`, the arm fired early — that means `open` syncs a file; report it (the test arms AFTER open, so this should not happen).

- [ ] **Step 4: RED under #9, #10 (`poisoned()` → `true`, then `false`) and #11 (`check_poison` never refuses)**

```bash
perl -pi -e 's/^        self\.poisoned$/        true/' crates/charpente-core/src/log.rs && git diff --stat
cargo test -p charpente-sim --test wal a_failed_sync_poisons 2>&1 | grep -E '^test |panicked|healthy'
git checkout -- crates/charpente-core/src/log.rs
perl -pi -e 's/^        self\.poisoned$/        false/' crates/charpente-core/src/log.rs && git diff --stat
cargo test -p charpente-sim --test wal a_failed_sync_poisons 2>&1 | grep -E '^test |panicked|poisons the log'
git checkout -- crates/charpente-core/src/log.rs
perl -pi -e 's/if self\.poisoned \{/if false {/' crates/charpente-core/src/log.rs && git diff --stat
cargo test -p charpente-sim --test wal a_failed_sync_poisons 2>&1 | grep -E '^test |panicked|refused by name'
git checkout -- crates/charpente-core/src/log.rs && git status --short
```
Expected: three FAILs — `a healthy log is not poisoned`; `one failed fsync poisons the log`; `append after poison must be refused by name`. The third one-liner is the hand-applied form of cargo-mutants' "replace body with Ok(())": with `if false` the function never returns `Err`, which is the same observable behaviour. Paste all three.

- [ ] **Step 5: Commit**

```bash
cargo fmt --all --check && cargo clippy -p charpente-sim --all-targets -- -D warnings 2>&1 | tail -n 1
git add crates/charpente-sim/tests/wal.rs
git commit -F - <<'EOF'
test(wal): the log's own poison is read and refused by name

A disk that fails one fsync on demand: poisoned() reads false then
true, and append/sync/gc each refuse with check_poison's message while
the disk is healthy again. Watched RED under all three mutants —
poisoned() -> true, -> false, and check_poison that never refuses.
charpente-sim wal: 37 passed.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_017gCrhXqM7QqtVbwYF1266M
EOF
```

---

### Task 3: The `read_frame` properties — the three-way check, and the maximum

**Files:**
- Modify: `crates/charpente-sim/tests/wal.rs` (append; add `frame_crc` and `MAX_RECORD_BYTES` to the `charpente_core::log` import)

**Interfaces:**
- Consumes: `charpente_core::log::{frame_crc, MAX_RECORD_BYTES}` (both `pub`), `small_segments()` (4 KiB).
- Produces: `raw_frame(seq, declared_len, payload) -> Vec<u8>` — test-local.

- [ ] **Step 1: Confirm the anchors**

```bash
grep -c '|| payload_len > MAX_RECORD_BYTES as u64' crates/charpente-core/src/log.rs
grep -c '        || payload_len > MAX_RECORD_BYTES as u64' crates/charpente-core/src/log.rs
```
Expected: `1`, `1`. (The `||` → `&&` mutant needs the whole three-way check; Step 4 rewrites its two `||` lines.)

- [ ] **Step 2: Append the builder and the two tests**

```rust

/// One frame in the WAL's real format — `[seq u64 le][len u32 le][crc u32
/// le][payload]`, crc over seq‖len‖payload as `frame_crc` computes it — so a
/// hand-built log differs from an appended one only where the test says.
/// `declared_len` lets a frame LIE about its length; the crc is over the
/// bytes actually supplied, which is what recovery recomputes when it reads
/// `declared_len` bytes.
fn raw_frame(seq: u64, declared_len: u32, payload: &[u8]) -> Vec<u8> {
    let mut frame = Vec::with_capacity(16 + payload.len());
    frame.extend_from_slice(&seq.to_le_bytes());
    frame.extend_from_slice(&declared_len.to_le_bytes());
    frame.extend_from_slice(&frame_crc(seq, payload).to_le_bytes());
    frame.extend_from_slice(payload);
    frame
}

const FIRST_SEGMENT: &str = "wal/00000000000000000000.log";

#[test]
fn read_frame_cuts_zero_length_oversized_and_past_end_frames() {
    // One valid frame, then a frame the three-way size check must refuse.
    // cargo-mutants' `:504:9` turns the FIRST `||` into `&&`, and `&&` binds
    // tighter than `||`: the mutant is `(len == 0 && len > MAX) || past_end`,
    // so arms 1-2 are dead and the past-end arm lives. Case (a) — a declared
    // length of 0 with a correct crc — is what distinguishes it: under the
    // mutant it is recovered as an EMPTY record, which append forbids. Cases
    // (b) and (c) pin arms 2 and 3 of the ORIGINAL check (an oversized and a
    // past-end declaration each cut) and are still refused by arm 3 under the
    // mutant. The crc in (a) is deliberately right so the size arm is the only
    // thing between the frame and Valid.
    let valid = raw_frame(0, 8, &[7u8; 8]);
    let cases: [(&str, Vec<u8>); 3] = [
        ("zero-length", raw_frame(1, 0, &[])),
        (
            "oversized",
            raw_frame(1, (MAX_RECORD_BYTES + 1) as u32, &[9u8; 8]),
        ),
        ("past-end", raw_frame(1, 64, &[9u8; 8])),
    ];
    for (name, bad) in cases {
        let sim = quiet_sim();
        let file = sim.disk().open(FIRST_SEGMENT).unwrap();
        file.write_at(0, &valid).unwrap();
        file.write_at(valid.len() as u64, &bad).unwrap();
        file.sync().unwrap();
        sim.disk().sync_dir("wal/").unwrap();

        let mut recovered = Vec::new();
        Wal::open(Box::new(sim.disk()), small_segments(), |seq, p| {
            recovered.push((seq, p.to_vec()));
            Ok(())
        })
        .unwrap_or_else(|e| panic!("{name}: recovery must cut, not fail: {e}"));
        assert_eq!(
            recovered,
            vec![(0, vec![7u8; 8])],
            "{name}: exactly the valid frame is recovered"
        );
        assert_eq!(
            sim.disk().open(FIRST_SEGMENT).unwrap().len().unwrap(),
            valid.len() as u64,
            "{name}: the cut truncates at the bad frame"
        );
    }
}

#[test]
fn a_maximum_size_record_round_trips_and_one_byte_over_is_invalid() {
    // append accepts exactly MAX_RECORD_BYTES; recovery's size arm is
    // `payload_len > MAX`, so a record AT the maximum is the boundary the
    // `>=` mutant cuts, and a frame declaring one byte more — bytes present,
    // crc correct — is the record the `==` mutant recovers.
    let sim = quiet_sim();
    let big = vec![0xABu8; MAX_RECORD_BYTES];
    let mut wal = Wal::open(Box::new(sim.disk()), small_segments(), |_, _| Ok(())).unwrap();
    wal.append(&big).unwrap();
    wal.sync().unwrap();
    let mut recovered = Vec::new();
    Wal::open(Box::new(sim.disk()), small_segments(), |seq, payload| {
        recovered.push((seq, payload.len(), payload == big.as_slice()));
        Ok(())
    })
    .unwrap();
    assert_eq!(
        recovered,
        vec![(0, MAX_RECORD_BYTES, true)],
        "a record of exactly MAX_RECORD_BYTES survives recovery"
    );

    let sim = quiet_sim();
    let over = vec![0xCDu8; MAX_RECORD_BYTES + 1];
    let file = sim.disk().open(FIRST_SEGMENT).unwrap();
    file.write_at(0, &raw_frame(0, (MAX_RECORD_BYTES + 1) as u32, &over))
        .unwrap();
    file.sync().unwrap();
    sim.disk().sync_dir("wal/").unwrap();
    let mut count = 0;
    Wal::open(Box::new(sim.disk()), small_segments(), |_, _| {
        count += 1;
        Ok(())
    })
    .unwrap();
    assert_eq!(
        count, 0,
        "a frame declaring MAX_RECORD_BYTES + 1 is invalid even with its bytes present"
    );
    assert_eq!(
        sim.disk().open(FIRST_SEGMENT).unwrap().len().unwrap(),
        0,
        "…and the cut truncates it away"
    );
}
```

- [ ] **Step 3: GREEN — predict 39**

```bash
cargo fmt --all && cargo test -p charpente-sim --test wal 2>&1 | tee /tmp/wal-t3-green.log | grep -E '^test result|FAILED|finished'; echo "exit ${pipestatus[1]}"
```
Expected: `39 passed`, exit 0. Note the `finished in` time — the 16 MiB fixtures should add well under a second; report the number.

- [ ] **Step 4: RED under #13 (`||` → `&&`)**

cargo-mutants' `:504:9` flips the FIRST `||`; `&&` binds tighter, so the mutant is `(len == 0 && len > MAX) || past_end` — arms 1–2 dead, the past-end arm alive. Apply exactly that:
```bash
perl -pi -e 's/^        \|\| payload_len > MAX_RECORD_BYTES as u64$/        \&\& payload_len > MAX_RECORD_BYTES as u64/' crates/charpente-core/src/log.rs && git diff --stat
git diff crates/charpente-core/src/log.rs | grep -c '^+.*&&'
cargo test -p charpente-sim --test wal read_frame_cuts 2>&1 | grep -E '^test |panicked|recovery must cut|exactly the valid'
git checkout -- crates/charpente-core/src/log.rs && git status --short
```
Expected: the grep prints `1`; the test FAILS on the FIRST case — `zero-length: exactly the valid frame is recovered`, with a second, empty record in `left`. Cases (b) and (c) stay GREEN under this mutant — arm 3 still refuses them — so (a) is the observation for the branch; the report says so. To ALSO see (b)/(c) under the mutant, make a scratch edit to the TEST (comment out the `"zero-length"` case), run, paste the `recovery must cut, not fail` panic naming `oversized`, and revert the test edit with `git diff --stat` showing only the intended hunks — optional, and never committed.

- [ ] **Step 5: RED under #14 and #15 (`> MAX` → `==`, then `>=`)**

```bash
perl -pi -e 's/\|\| payload_len > MAX_RECORD_BYTES as u64/|| payload_len == MAX_RECORD_BYTES as u64/' crates/charpente-core/src/log.rs && git diff --stat
cargo test -p charpente-sim --test wal a_maximum_size_record 2>&1 | grep -E '^test |panicked|left:|right:|invalid even'
git checkout -- crates/charpente-core/src/log.rs
perl -pi -e 's/\|\| payload_len > MAX_RECORD_BYTES as u64/|| payload_len >= MAX_RECORD_BYTES as u64/' crates/charpente-core/src/log.rs && git diff --stat
cargo test -p charpente-sim --test wal a_maximum_size_record 2>&1 | grep -E '^test |panicked|left:|right:|survives recovery'
git checkout -- crates/charpente-core/src/log.rs && git status --short
```
Expected: `==` FAILS with `a frame declaring MAX_RECORD_BYTES + 1 is invalid even with its bytes present` (`left: 1`); `>=` FAILS with `a record of exactly MAX_RECORD_BYTES survives recovery` (`left: []`). Paste both. Also the `==` mutant makes the `read_frame_cuts…` "oversized" case behave differently — run it too under `==` and paste (it should still pass: the past-end arm catches a declared MAX+1 with 8 bytes present; say so).

- [ ] **Step 6: Commit**

```bash
cargo fmt --all --check && cargo clippy -p charpente-sim --all-targets -- -D warnings 2>&1 | tail -n 1
git add crates/charpente-sim/tests/wal.rs
git commit -F - <<'EOF'
test(wal): frame-level corruption detection is pinned in its branch and at its boundary

Raw real-format frames: a zero-length, an oversized and a past-end
frame each cut recovery at the valid prefix; a record of exactly
MAX_RECORD_BYTES round-trips and a frame declaring one byte more is
invalid even with the bytes present. Watched RED under `||` -> `&&`
(the whole check dead: an empty record recovered), `>` -> `==` (a
17 MiB record recovered) and `>` -> `>=` (the maximum cut).
charpente-sim wal: 39 passed.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_017gCrhXqM7QqtVbwYF1266M
EOF
```

---

### Task 4: The gaplessness assert, watched firing

**Files:**
- Modify: `crates/charpente-sim/tests/wal.rs` (append)

**Interfaces:**
- Consumes: `build_log(&sim, &refs)` (4 KiB segments via `small_segments()`, 300-byte records → 316-byte frames, 12 per segment), the byte-flip recipe of `bit_flip_cuts_the_log_there_and_deletes_later_segments`, `SimDisk`.
- Produces: `LyingRemove` — test-local.

- [ ] **Step 1: Confirm the anchor**

```bash
grep -c 'if !later.is_empty() {' crates/charpente-core/src/log.rs
```
Expected: `1`.

- [ ] **Step 2: Append the wrapper and the test**

```rust

/// A disk whose `remove` returns Ok and leaves the name in place. Not a
/// fault any filesystem exhibits — a BROKEN INVARIANT, which is exactly what
/// recovery's gaplessness check (`open_with_horizon`, "no segment past the
/// cut point may still exist before the truncation runs") is there to refuse
/// rather than recover around. Test-local (spec §6).
struct LyingRemove(SimDisk);

impl Disk for LyingRemove {
    fn open(&self, path: &str) -> io::Result<Box<dyn File>> {
        self.0.open(path)
    }
    fn remove(&self, _path: &str) -> io::Result<()> {
        Ok(())
    }
    fn list(&self, prefix: &str) -> io::Result<Vec<String>> {
        self.0.list(prefix)
    }
    fn sync_dir(&self, prefix: &str) -> io::Result<()> {
        self.0.sync_dir(prefix)
    }
}

#[test]
#[should_panic(expected = "orphaned segments must be removed before the cut segment is truncated")]
fn a_remove_that_lies_trips_the_gaplessness_assert_before_the_cut() {
    // The log of bit_flip_cuts_the_log_there_and_deletes_later_segments: 40
    // 300-byte records, 12 frames per 4 KiB segment, one payload byte of
    // frame 3 flipped so the cut lands in segment 0 with later segments on
    // disk. Recovery removes them first; through a disk whose remove lies
    // they are still listed when the negative-space check runs, and the
    // check — not the truncation — is what must happen next. The sweep's
    // mutant inverted the check's guard so it ran only when there was
    // nothing to check; under it recovery truncates and returns Ok.
    let sim = quiet_sim();
    let records: Vec<Vec<u8>> = (0..40u8).map(|i| vec![i; 300]).collect();
    let refs: Vec<&[u8]> = records.iter().map(|r| r.as_slice()).collect();
    build_log(&sim, &refs);
    let names = sim.disk().list("wal/").unwrap();
    assert!(
        names.len() > 1,
        "later segments must exist for the check to have work: {names:?}"
    );
    let segment = sim.disk().open(&names[0]).unwrap();
    let offset = 3 * 316 + 16 + 100;
    let mut byte = [0u8; 1];
    segment.read_at(offset, &mut byte).unwrap();
    segment.write_at(offset, &[byte[0] ^ 0xFF]).unwrap();
    segment.sync().unwrap();

    let result = Wal::open(
        Box::new(LyingRemove(sim.disk())),
        small_segments(),
        |_, _| Ok(()),
    );
    panic!(
        "recovery returned {:?} instead of refusing to truncate beside a surviving later \
         segment",
        result.map(|_| ())
    );
}
```

- [ ] **Step 3: GREEN — predict 40**

```bash
cargo fmt --all && cargo test -p charpente-sim --test wal 2>&1 | tee /tmp/wal-t4-green.log | grep -E '^test result|FAILED'; echo "exit ${pipestatus[1]}"
```
Expected: `40 passed`, exit 0. The test passes BECAUSE the assert panics with the expected phrase; the trailing `panic!` is the path the mutant takes.

- [ ] **Step 4: RED under #2 (`!later.is_empty()` → `later.is_empty()`)**

```bash
perl -pi -e 's/if !later\.is_empty\(\) \{/if later.is_empty() {/' crates/charpente-core/src/log.rs && git diff --stat
cargo test -p charpente-sim --test wal a_remove_that_lies 2>&1 | grep -E '^test |panicked|recovery returned|expected'
git checkout -- crates/charpente-core/src/log.rs && git status --short
```
Expected: FAILS — the panic text is `recovery returned Ok(()) instead of …` and the harness reports the `should_panic` expectation unmet (`panic did not contain expected string`). Paste it.

- [ ] **Step 5: Commit**

```bash
cargo fmt --all --check && cargo clippy -p charpente-sim --all-targets -- -D warnings 2>&1 | tail -n 1
git add crates/charpente-sim/tests/wal.rs
git commit -F - <<'EOF'
test(wal): the gaplessness assert is watched firing through a remove that lies

A disk whose remove returns Ok and leaves the name: recovery must panic
with the gaplessness message before truncating the cut segment, and
under the sweep's mutant — the guard inverted so the check runs only
when there is nothing to check — it truncates and returns Ok instead.
charpente-sim wal: 40 passed.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_017gCrhXqM7QqtVbwYF1266M
EOF
```

---

### Task 5: Two rewrites and the `torn_header` helper, each new mutant watched caught

**Files:**
- Modify: `crates/charpente-core/src/log.rs:263` (`cut < len`), `:296` (`tail_offset > 0`), `:489-495` (`read_frame`'s torn-header check)

**Interfaces:**
- Consumes: nothing new.
- Produces: `fn torn_header(offset: u64, len: u64) -> bool` — private; Task 6's exclusion regex names it (`replace > with >= in torn_header`), so the NAME is the interface.

- [ ] **Step 1: The rewrites**

Change (at `:263`):
```rust
                    if cut < len {
                        // Load-bearing truncation; see the module docs.
                        file.set_len(cut)?;
                        file.sync()?;
                    }
```
to:
```rust
                    assert!(
                        cut <= len,
                        "recovery cut {cut} is past segment length {len} — the cut is an \
                         offset the scan reached inside the file"
                    );
                    if cut != len {
                        // Load-bearing truncation; see the module docs. `!=`
                        // rather than `<`: with the assert above they mean the
                        // same, and `!=`'s mutant (`==`) skips every real
                        // truncation where `<`'s (`<=`) was a no-op set_len
                        // no oracle can see (2026-09-10 sweep, log.rs:263).
                        file.set_len(cut)?;
                        file.sync()?;
                    }
```
Change (at `:296`):
```rust
        if self.tail_offset > 0 && self.tail_offset + frame_len > self.cfg.segment_bytes {
```
to:
```rust
        // `!= 0` rather than `> 0` on a u64: same gate, and its mutant (`== 0`)
        // never rotates a full segment where `> 0`'s (`>= 0`) re-opened the
        // same name and no oracle could see it (2026-09-10 sweep, log.rs:296).
        if self.tail_offset != 0 && self.tail_offset + frame_len > self.cfg.segment_bytes {
```
Change (in `read_frame`):
```rust
    if offset + HEADER_BYTES > len {
        return Ok(Frame::Invalid); // partial header: torn tail
    }
```
to:
```rust
    if torn_header(offset, len) {
        return Ok(Frame::Invalid);
    }
```
and add, directly above `fn read_frame`:
```rust
/// A header that does not fit before the end of the file is a torn tail.
/// Its own function so the instrument can name it: at `offset + HEADER_BYTES
/// == len` the header fits and the payload-length check refuses the frame
/// anyway, so the `>=` mutant here is equivalent — excluded by name in
/// `.cargo/mutants.toml`, where the argument is repeated.
fn torn_header(offset: u64, len: u64) -> bool {
    offset + HEADER_BYTES > len
}
```

- [ ] **Step 2: GREEN — every WAL oracle, predict 40 + charpente-core unchanged**

```bash
cargo fmt --all && cargo clippy -p charpente-core -p charpente-sim --all-targets -- -D warnings 2>&1 | tail -n 1
cargo test -p charpente-sim --test wal 2>&1 | tee /tmp/wal-t5-green.log | grep -E '^test result|FAILED'
cargo test -p charpente-core -p charpente-sim --no-fail-fast 2>&1 | tee /tmp/wal-t5-pkgs.log | grep -cE '^test result: ok'; grep -c FAILED /tmp/wal-t5-pkgs.log
```
Expected: `40 passed`; the package count is **17** `test result: ok` (charpente-core: lib + 8 test files + doc-tests = 10; charpente-sim: lib + 5 test files + doc-tests = 7 — measured with `cargo test --no-run` on `a93f100`) and `0` FAILED.

- [ ] **Step 3: Enumerate the new sites' mutants**

```bash
cargo mutants --list --file crates/charpente-core/src/log.rs 2>/dev/null | grep -E 'torn_header|replace != with ==|replace <= with' | tee /tmp/wal-t5-new-mutants.txt
```
Expected: `torn_header` lines (`-> bool with true`, `with false`, `replace > with ==`, `replace > with >=`, `replace > with <`), the two `!=` → `==` lines (in `Wal::open_with_horizon` and `Wal::append`), and the assert's `<=` lines. Paste the list. Every one EXCEPT `replace > with >= in torn_header` must now be observed CAUGHT.

- [ ] **Step 4: Watch each new mutant caught, one at a time**

`cargo fmt` (Step 2) may have re-wrapped the assert or the rotation line; before each one-liner below, `grep -c` its anchor and adjust the whitespace in the pattern if rustfmt moved it — an anchor that matches nothing leaves the code unmutated and the "caught" reading is then vacuous (`git diff --stat` must show `log.rs` changed before every test run).

```bash
# cut != len -> ==
perl -pi -e 's/if cut != len \{/if cut == len {/' crates/charpente-core/src/log.rs && git diff --stat
cargo test -p charpente-sim --test wal wal_fixed_size_payloads_cannot_resurrect_stale_frames 2>&1 | grep -E '^test |panicked' | head -3
git checkout -- crates/charpente-core/src/log.rs
# tail_offset != 0 -> == 0
perl -pi -e 's/if self\.tail_offset != 0 &&/if self.tail_offset == 0 \&\&/' crates/charpente-core/src/log.rs && git diff --stat
cargo test -p charpente-sim --test wal rotation_spreads_records_across_segments_and_recovers 2>&1 | grep -E '^test |panicked|must have rotated' | head -3
git checkout -- crates/charpente-core/src/log.rs
# torn_header -> true / -> false / > -> == / > -> <
for m in 'true' 'false'; do
  perl -0pi -e "s/fn torn_header\(offset: u64, len: u64\) -> bool \{\n    offset \+ HEADER_BYTES > len/fn torn_header(offset: u64, len: u64) -> bool {\n    $m/" crates/charpente-core/src/log.rs && git diff --stat
  cargo test -p charpente-sim --test wal 2>&1 | grep -E '^test result'
  git checkout -- crates/charpente-core/src/log.rs
done
for op in '==' '<'; do
  perl -pi -e "s/    offset \+ HEADER_BYTES > len/    offset + HEADER_BYTES $op len/" crates/charpente-core/src/log.rs && git diff --stat
  cargo test -p charpente-sim --test wal 2>&1 | grep -E '^test result'
  git checkout -- crates/charpente-core/src/log.rs
done
# the assert's <= : -> < , -> == , -> >
for op in '<' '==' '>'; do
  perl -pi -e "s/^                        cut <= len,/                        cut $op len,/" crates/charpente-core/src/log.rs && git diff --stat
  cargo test -p charpente-sim --test wal 2>&1 | grep -E '^test result'
  git checkout -- crates/charpente-core/src/log.rs
done
git status --short
```
Expected: EVERY run shows `FAILED` (at least one test red) — paste each `test result:` line with the mutant it belongs to. `torn_header -> false` is the one to watch closely: a torn tail then reads past EOF into `UnexpectedEof`; if the fault-matrix universes classify that as fault fallout and stay green, the mutant survives — then the property to write is "a torn header at a crash point cuts, it does not error", in this task, before committing (spec §4's last sentence). `git status --short` must show ONLY ` M crates/charpente-core/src/log.rs` (your rewrites) — run `git diff` and confirm every hunk is one of the three from Step 1 and nothing from the loops leaked.

- [ ] **Step 5: The equivalent one, watched surviving — so the exclusion is earned**

```bash
perl -pi -e 's/    offset \+ HEADER_BYTES > len/    offset + HEADER_BYTES >= len/' crates/charpente-core/src/log.rs && git diff --stat
cargo test -p charpente-core -p charpente-sim --no-fail-fast 2>&1 | tee /tmp/wal-t5-equiv.log | grep -cE '^test result: ok'; grep -c FAILED /tmp/wal-t5-equiv.log
git checkout -- crates/charpente-core/src/log.rs
git diff --stat
```
Expected: the same ok count as Step 2 and `0` FAILED — the mutant is equivalent under every oracle in scope, which is the argument Task 6's comment makes. Paste the counts. Then `git diff --stat` shows only `log.rs` with your three hunks.

- [ ] **Step 6: Commit**

```bash
cargo fmt --all --check
git add crates/charpente-core/src/log.rs
git commit -F - <<'EOF'
refactor(core): two WAL gates written so their mutants are visible, and the torn-header check named

`cut < len` -> `assert!(cut <= len); cut != len`: `<`'s `<=` mutant
was a no-op set_len no oracle can see; `!=`'s `==` skips every real
truncation and wal_fixed_size_payloads_cannot_resurrect_stale_frames
goes red. `tail_offset > 0` -> `!= 0` on the u64: `>= 0` re-opened the
same segment name; `== 0` never rotates a full segment and the rotation
family goes red. The torn-header comparison moves into `torn_header`
so its one equivalent mutant (`>=`: a header that exactly fits is
refused by the payload check anyway) can be excluded by name. Every
other mutant at the three sites watched caught; the equivalent one
watched surviving the whole core+sim suite. No behaviour change.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_017gCrhXqM7QqtVbwYF1266M
EOF
```

---

### Task 6: The exclusion list, its ceiling, and the docs

**Files:**
- Create: `.cargo/mutants.toml`
- Create: `crates/guide-examples/tests/mutants_exclusions.rs`
- Modify: `CLAUDE.md` (test list line for `guide-examples`; the **Mutation instrument** bullet; the `crates/guide-examples` workspace-map bullet)
- Modify: `crates/charpente-cli/src/mutants.rs` (module doc)
- Modify: `scripts/mutants.sh` (header comment)

**Interfaces:**
- Consumes: the mutant names `replace <impl fmt::Debug for Wal>::fmt` and `replace > with >= in torn_header` (Task 5).
- Produces: `EXCLUSIONS: usize = 2` — the ceiling a future exclusion must raise deliberately.

- [ ] **Step 1: Write the config**

Create `.cargo/mutants.toml`:
```toml
# Known-equivalent mutants, hidden from `charpente test --mutants` BY NAME,
# each with the argument that no surface the oracle may see distinguishes it
# from the original. A hidden mutant is a hole the instrument stops
# reporting, so: one comment per entry, and the count is pinned by
# crates/guide-examples/tests/mutants_exclusions.rs (EXCLUSIONS) — raising
# it is a deliberate act for a commit message. Spec:
# docs/superpowers/specs/2026-09-12-wal-one-sided-properties-design.md §5.
exclude_re = [
    # Diagnostic impl kept only so `unwrap_err` has a Debug bound; its text is
    # not an error message an operator reads (hard rule 5 is about those).
    # Replacing the body is unobservable.
    "replace <impl fmt::Debug for Wal>::fmt",
    # At offset + HEADER_BYTES == len the original reads a complete header and
    # the payload-length check returns Invalid; `>=` returns Invalid one read
    # earlier. Watched surviving the whole core+sim suite (2026-09-12).
    "replace > with >= in torn_header",
]
```

- [ ] **Step 2: Prove the config is read, and hides exactly those two**

```bash
cargo mutants --list --file crates/charpente-core/src/log.rs 2>/dev/null | grep -cE 'Debug for Wal|replace > with >= in torn_header'
cargo mutants --list --no-config --file crates/charpente-core/src/log.rs 2>/dev/null | grep -cE 'Debug for Wal|replace > with >= in torn_header'
cargo mutants --list --file crates/charpente-core/src/log.rs 2>/dev/null | wc -l
cargo mutants --list --no-config --file crates/charpente-core/src/log.rs 2>/dev/null | wc -l
```
Expected: `0`, `2`, and the two totals differ by exactly 2. Paste all four numbers.

- [ ] **Step 3: The ceiling gate — RED first against the real file**

Create `crates/guide-examples/tests/mutants_exclusions.rs`:
```rust
//! The known-equivalent mutant list, pinned. `.cargo/mutants.toml`'s
//! `exclude_re` hides mutants from `charpente test --mutants` by name; a
//! hidden mutant is a hole the instrument stops reporting, so every entry
//! must carry — in a `#` comment directly above it — the argument that it is
//! equivalent (unobservable through any surface the oracle may see), and the
//! count is pinned so an entry cannot arrive with a commit that says nothing
//! about it. Two at landing (wal-one-sided-properties spec §5).
//!
//! Read as text, not TOML: the rule is about comments, which a TOML parser
//! drops. An entry is a line inside `exclude_re = [ … ]` whose trimmed start
//! is a double quote.

// Reading the repo's config at test time is build tooling outside the
// deterministic boundary, the same sanctioned-edge pattern as
// tests/doc_inventory.rs.
#![allow(clippy::disallowed_methods)]

use std::path::Path;

/// Raise this in the same commit that adds an entry, and say why there.
const EXCLUSIONS: usize = 2;

fn config() -> String {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.cargo/mutants.toml");
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

/// `(line number, entry text)` for every string entry inside the
/// `exclude_re` array.
fn entries(text: &str) -> Vec<(usize, String)> {
    let mut inside = false;
    let mut out = Vec::new();
    for (i, line) in text.lines().enumerate() {
        let t = line.trim();
        if t.starts_with("exclude_re") && t.ends_with('[') {
            inside = true;
            continue;
        }
        if inside && t.starts_with(']') {
            break;
        }
        if inside && t.starts_with('"') {
            out.push((i + 1, t.trim_end_matches(',').to_string()));
        }
    }
    out
}

/// Entries whose nearest preceding non-blank line is not a `#` comment.
fn uncommented(text: &str) -> Vec<(usize, String)> {
    let lines: Vec<&str> = text.lines().collect();
    entries(text)
        .into_iter()
        .filter(|(line_no, _)| {
            let above = lines[..line_no - 1]
                .iter()
                .rev()
                .find(|l| !l.trim().is_empty());
            !above.is_some_and(|l| l.trim_start().starts_with('#'))
        })
        .collect()
}

#[test]
fn every_exclusion_carries_its_equivalence_argument() {
    let bare = uncommented(&config());
    assert!(
        bare.is_empty(),
        ".cargo/mutants.toml hides {} mutant(s) with no argument for why they are \
         equivalent:\n  {}\ncontract: an excluded mutant is a hole the instrument stops \
         reporting; the `#` comment directly above each entry must say what makes it \
         unobservable. fix: write the argument, or delete the entry and write the property.",
        bare.len(),
        bare.iter()
            .map(|(n, e)| format!(".cargo/mutants.toml:{n}: {e}"))
            .collect::<Vec<_>>()
            .join("\n  "),
    );
}

#[test]
fn the_exclusion_count_is_pinned() {
    let found = entries(&config()).len();
    assert_eq!(
        found, EXCLUSIONS,
        ".cargo/mutants.toml has {found} exclusion(s); EXCLUSIONS in \
         crates/guide-examples/tests/mutants_exclusions.rs says {EXCLUSIONS}. Adding or \
         removing an exclusion is a deliberate act: change the constant in the same commit \
         and say in its message which mutant, and why it is equivalent."
    );
}

#[test]
fn an_entry_without_a_comment_above_it_is_refused() {
    let text = "exclude_re = [\n    # said why\n    \"a\",\n    \"b\",\n\n    # said why\n    \"c\",\n]\n";
    assert_eq!(uncommented(text), vec![(4, "\"b\"".to_string())]);
}

#[test]
fn strings_outside_the_array_are_not_entries() {
    let text = "other = \"x\"\nexclude_re = [\n    # why\n    \"a\",\n]\ntrailing = \"y\"\n";
    assert_eq!(entries(text), vec![(4, "\"a\"".to_string())]);
}
```

Now RED against the real file, BEFORE trusting the gate:
```bash
printf '    "replace scratch with nothing in nowhere",\n' > /tmp/scratch-entry.txt
perl -pi -e 'BEGIN{$s=`cat /tmp/scratch-entry.txt`} s/^\]$/$s]/' .cargo/mutants.toml && tail -n 4 .cargo/mutants.toml
cargo test -p guide-examples --test mutants_exclusions 2>&1 | grep -E '^test |panicked|hides|exclusion\(s\)'
git checkout -- .cargo/mutants.toml 2>/dev/null || perl -ni -e 'print unless /replace scratch with nothing/' .cargo/mutants.toml
git status --short
```
Expected: `every_exclusion_carries_its_equivalence_argument` FAILS naming `.cargo/mutants.toml:<line>: "replace scratch…"`, AND `the_exclusion_count_is_pinned` FAILS with `has 3 exclusion(s); … says 2`; the two fixture tests pass. Paste both failures. (`git checkout` fails because the file is untracked — the `perl -ni` fallback removes the scratch line; confirm with `tail -n 4`.)

- [ ] **Step 4: GREEN — predict 4, then the CLAUDE.md gate demands the name**

```bash
cargo fmt --all && cargo test -p guide-examples --test mutants_exclusions 2>&1 | grep -E '^test result'
cargo test -p guide-examples --test doc_inventory 2>&1 | grep -E '^test |mutants_exclusions'
```
Expected: `4 passed`; then `claude_md_test_inventory_is_exhaustive_as_it_claims ... FAILED` naming `guide-examples: mutants_exclusions`.

- [ ] **Step 5: The docs (hard rule 6)**

`CLAUDE.md`:
- The test-list line: `guide-examples: describe_contract/doc_inventory/doc_truth/hook_census;` → `guide-examples: describe_contract/doc_inventory/doc_truth/hook_census/mutants_exclusions;`
- The **Mutation instrument** bullet, after the sentence ending "no longer needs a hand `mkdir`.": add ` Mutants proven equivalent are hidden by name in `.cargo/mutants.toml`, one rationale comment per entry, and `tests/mutants_exclusions.rs` pins the count — a third survivor is a property to write, not an entry to add.` Keep the ~90-col wrap; no wrapped line may begin with a backtick.
- The `crates/guide-examples` workspace-map bullet: after "the deny-list is inert under a file-level allow." add ` `tests/mutants_exclusions.rs` pins the known-equivalent mutant list's size and demands a rationale per entry.`

`crates/charpente-cli/src/mutants.rs` module doc, after the paragraph ending "and \"the usual cause is a --file or --re\").": add
```rust
//!
//! `.cargo/mutants.toml` (read by cargo-mutants from the `--dir` root) hides
//! mutants PROVEN equivalent — unobservable through any surface the oracle may
//! see — by name, one rationale comment per entry; the list's size is pinned
//! by guide-examples' `mutants_exclusions` gate. Hiding is never the answer to
//! a MISSED: that is a property to write.
```

`scripts/mutants.sh`, after the "Requires the pinned tool" paragraph: add
```bash
# Known-equivalent mutants are hidden by name in .cargo/mutants.toml, each
# with its argument; crates/guide-examples/tests/mutants_exclusions.rs pins
# the count. A MISSED is a property to write, never an entry to add.
```

- [ ] **Step 6: GREEN — guide-examples predict 7 binaries; whole workspace predict 85**

```bash
cargo fmt --all && cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings 2>&1 | tail -n 1; echo "clippy exit ${pipestatus[1]}"
cargo test -p guide-examples --no-fail-fast 2>&1 | tee /tmp/wal-t6-ge.log | grep -cE '^test result: ok'
cargo test --workspace --no-fail-fast 2>&1 | tee /tmp/wal-t6-workspace.log | grep -cE '^test result: ok'; grep -c '^test result: FAILED' /tmp/wal-t6-workspace.log; echo "test exit ${pipestatus[1]}"
```
Expected: `7`; `85`, `0`. The whole-workspace run takes 10–25 minutes; foreground, wait.

- [ ] **Step 7: Commit**

```bash
git add .cargo/mutants.toml crates/guide-examples/tests/mutants_exclusions.rs CLAUDE.md crates/charpente-cli/src/mutants.rs scripts/mutants.sh
git status --short
git commit -F - <<'EOF'
fence(mutants): two equivalent mutants hidden by name, under a pinned ceiling

.cargo/mutants.toml excludes `<impl Debug for Wal>::fmt`'s body
replacement (diagnostic text no operator reads) and `torn_header`'s
`>=` (a header that exactly fits is refused by the payload check
anyway; watched surviving the whole core+sim suite), each with its
argument in the comment above it. guide-examples' mutants_exclusions
gate pins the count at 2 and refuses an entry with no comment — RED
observed against a third, bare entry. --list hides exactly the two
(N with config, N+2 with --no-config). CLAUDE.md, the verb's module
doc and scripts/mutants.sh name the list and the rule: a MISSED is a
property to write, never an entry to add. Workspace: 85 ok.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_017gCrhXqM7QqtVbwYF1266M
EOF
```

---

## After Task 6 — controller-owned

1. Final whole-branch review (opus) with a review package over the merge-base..HEAD, pointing at the spec's §2 table: every row must map to a watched RED (properties), a watched CAUGHT (rewrites) or a watched SURVIVAL (exclusions) in the task reports.
2. **The sweep, spec §7.5** — in the worktree, backgrounded, its own target dir:
   ```bash
   cd /Users/noahmitchem/Documents/GitHub/charpente-wal-properties && export CARGO_TARGET_DIR=/tmp/charpente-wal-properties-target
   scripts/mutants.sh --file crates/charpente-core/src/log.rs --jobs 1 > /tmp/wal-sweep.log 2>&1; echo "SWEEP_EXIT=$?" >> /tmp/wal-sweep.log
   ```
   Expected: `SWEEP_EXIT=0`, the verb's summary line reporting 0 MISSED, 0 timed out, and a total 2 lower than `--no-config`'s list. Wall clock recorded. A MISSED here is a finding: it names a property this plan did not write, and it is handled as a fix wave before merge, not ledgered.
3. Merge `--no-ff`, gate on main (predict 85), push, ledger entry (the fifteen with their observations, the sweep's line and wall clock, the rewrites, the exclusions), remove the worktree, the target dir, and keep the SDD workspace.
