# WAL one-sided properties — design

**Date:** 2026-09-12. **Status:** approved in chat (Noah), spec committed for the plan.
**Predecessors:** the 2026-09-10 mutation sweep (`.superpowers/sdd/2026-09-09-autonomous-run/
mutation-findings.md`, ledger entry of the same date) named fifteen MISSED mutants in
`crates/charpente-core/src/log.rs` and fixed nothing by design; `mechanical-fences` (merged
`41624bc`) and `instrument-hygiene` (merged `6694db8`) made the instrument honest enough to
run from a worktree and refuse the one configuration that lies. This sub-project closes the
fifteen.

## Context

The WAL is "the one place where things become true" (MANIFESTO §5; `log.rs` module doc).
Agent output quality tracks oracle density, and a MISSED mutant is a measured hole in the
oracle: the code can be changed at that site and no test notices. Fifteen such holes in the
commit point is the highest-value item in the queue, and it has waited behind two rounds of
instrument hygiene precisely so that the sweep which closes it can be trusted.

The sweep's verdicts were produced at `--jobs 1` with no shared target dir (the trustworthy
configuration) and re-checked by the controller for the severe site (`:504`, three survivors,
not one). They are taken as observed. What the sweep did not do — deliberately — is decide
what each property should *assert*; that is this document's job.

## §1 What this is really about

A MISSED mutant is one of three things, and only the first is a property to write:

1. **A one-sided property.** The behaviour the mutant changes is observable through the public
   surface, and no test looks. The fix is a test that would go RED under the mutant.
2. **An equivalent mutant.** The change is unobservable through any surface the oracle is
   allowed to see — the classic case here is a comparison whose boundary case is a no-op
   syscall. No property exists; writing a test that "catches" it would be a test of
   syscall counts, which is decoration. The honest treatment is to say so, once, with the
   argument, and stop the instrument from re-reporting it.
3. **An equivalent mutant that a semantics-preserving rewrite turns into case 1.** A gate
   written as `x > 0` on a `u64` has an equivalent mutant (`>= 0`); the same gate written as
   `x != 0` does not — its mutant `== 0` inverts the gate and the existing tests notice.
   The rewrite changes no behaviour and raises oracle density for free.

Every one of the fifteen is classified below. The classification is part of the deliverable:
a reader of the next sweep needs to know which survivors are known-equivalent and why, and
the instrument needs to stop counting them, or it reads red forever and nobody runs it
(`scripts/mutants.sh`'s own warning).

## §2 The fifteen, classified

Line numbers are `main` at `a93f100`. "Kills" names the mutant(s) a property must turn RED.

| # | Site | Mutation | Class | Why |
|---|---|---|---|---|
| 1 | `:106:9` `impl Debug for Wal` | body → `Ok(Default::default())` | **equivalent** | Diagnostic impl that exists so `unwrap_err` has a `Debug` bound; its text is not product surface (hard rule 5 is about error messages the operator reads, and this is not one). Excluded, §5. |
| 2 | `:242:24` `open_with_horizon` | `!later.is_empty()` → `later.is_empty()` | **property** | The gaplessness assert runs only when there is nothing to check. Observable with a disk whose `remove` returns `Ok` and leaves the name: the original panics with the gaplessness message before truncating; the mutant truncates. §3.7. |
| 3 | `:263:28` `open_with_horizon` | `cut < len` → `<=` | **rewrite** | The mutant differs only at `cut == len`, which never reaches the branch (a scan that ends at `len` yields `CleanEnd`, not a cut), so it changes nothing reachable. Rewritten as `assert!(cut <= len); if cut != len`, whose mutant `==` skips every real truncation and is caught by three tests (`bit_flip_cuts_the_log_there_and_deletes_later_segments`, `read_frame_cuts_zero_length_oversized_and_past_end_frames`, `a_maximum_size_record_round_trips_and_one_byte_over_is_invalid`; `wal_fixed_size_payloads_cannot_resurrect_stale_frames` alone stays green — measured 2026-09-12). §4. |
| 4 | `:296:29` `append` | `tail_offset > 0` → `>= 0` | **rewrite** | On a `u64` the mutant deletes the gate; the only effect is a rotate on an empty segment that reopens the *same* name (`segment_name(dir, next_seq)` is unchanged). Rewritten as `tail_offset != 0`, whose mutant `== 0` rotates only on empty segments and never on full ones — segments then grow unbounded, and the rotation family notices. §4. |
| 5 | `:296:65` `append` | `tail_offset + frame_len > segment_bytes` → `>=` | **property** | A frame that lands exactly at `segment_bytes` rotates one record early. §3.1. |
| 6 | `:396:20` `read_from` | `seq > up_to` → `==` | **property** | Both mutants break out of the segment loop when the inner loop ended at a clean segment end with `seq == up_to` — i.e. when `up_to`'s frame is the first frame of the next segment — and drop it. One test kills both. §3.2. |
| 7 | `:396:20` `read_from` | `seq > up_to` → `>=` | **property** | Same as 6. |
| 8 | `:421:29` `gc` | `bases[i+1] > keep_from` → `>=` | **property** | A segment whose successor's base equals the watermark holds only frames below it and is deletable; the mutant retains it. §3.3. |
| 9 | `:435:9` `poisoned` | → `true` | **property** | The accessor is read by no test. §3.4. |
| 10 | `:435:9` `poisoned` | → `false` | **property** | Same. |
| 11 | `:466:9` `check_poison` | body → `Ok(())` | **property** | A poisoned log refuses nothing. §3.4. |
| 12 | `:493:30` `read_frame` | `offset + HEADER_BYTES > len` → `>=` | **equivalent** | At `offset + 16 == len` the original reads a complete header and then fails the payload check (`payload_len == 0` or `… > len`): `Invalid` either way; the mutant returns `Invalid` one read earlier. Excluded, §5, after the check moves into a named helper so the exclusion is line-free. |
| 13 | `:504:9` `read_frame` | `\|\|` → `&&` | **property** | The first `\|\|` becomes `&&`, which binds tighter: `(len == 0 && len > MAX) \|\| past_end` — the zero-length and oversized arms are dead, the past-end arm lives. A zero-length frame with a correct crc is recovered as an empty record. §3.5. |
| 14 | `:504:24` `read_frame` | `payload_len > MAX` → `==` | **property** | Exactly `MAX` is now flagged, so a record of `MAX_RECORD_BYTES` — which `append` accepts — is cut on recovery (the observed RED); and a declared length above `MAX` passes the arm and, with the bytes present, would be recovered as a record larger than the contract allows. §3.6. |
| 15 | `:504:24` `read_frame` | `payload_len > MAX` → `>=` | **property** | A record of exactly `MAX_RECORD_BYTES` — which `append` accepts — is cut on recovery. §3.6. |

Eleven properties at seven sites, two rewrites, two exclusions.

## §3 The properties

All in `crates/charpente-sim/tests/wal.rs`, the WAL's oracle file and inside the verb's
default `--test-package` scope. They use the file's existing vocabulary: `quiet_sim()`,
`tiny_segments()` (64-byte segments), `base_of`, direct `Wal::open(Box::new(sim.disk()), …)`.
Raw frames are the REAL frame format — `[seq u64 le][len u32 le][crc u32 le][payload]` with
`charpente_core::log::frame_crc(seq, payload)` — not the file's `toy_append` toy format,
which belongs to the naive-recovery victims.

**The RED discipline, per property.** A property is one-sided until it has been watched
noticing. Sequence: (a) write the test; (b) run it on the original — it must be GREEN
(a property that fails on correct code is a wrong property); (c) apply the mutant by hand
to `log.rs` — the exact `perl -pi` one-liner is in the plan — and run the test again: it
must be RED, and the failure text is pasted into the report; (d) revert with
`git checkout -- crates/charpente-core/src/log.rs` and confirm GREEN and `git status`
clean. Step (c) is the observation; a test that was never watched failing under its
mutant is decoration. The final sweep (§7) is the second, instrument-side observation.

### 3.1 `a_frame_that_exactly_fills_the_segment_stays_in_it` — kills #5

64-byte segments, two 16-byte payloads (32-byte frames). After the second append and a
sync: exactly ONE segment, its length exactly 64. A third append rotates: two segments.
The mutant rotates at the second append (two segments after two appends).

### 3.2 `read_from_includes_up_to_when_its_frame_opens_the_next_segment` — kills #6, #7

The shape of `read_from_returns_verified_frames_across_segments` (8-byte payloads, two
frames per 64-byte segment, 40 records) but with `up_to` an EVEN seq — the first frame of
a segment. `read_from(13, 26, usize::MAX)` returns exactly `13..=26`. Under either mutant
the inner loop reaches the clean end of the `[24,25]` segment with `seq == 26`, the outer
check breaks, and 26 is missing. The existing test's `up_to = 27` (an odd seq, second in its
segment) is why both mutants survived: the boundary was never at a segment head.

### 3.3 `gc_deletes_the_segment_whose_successor_starts_at_keep_from` — kills #8

Same 40-record, two-per-segment log. Segment bases are `0, 2, 4, …`. `gc(4, 1)`: segments
`[0,1]` and `[2,3]` hold only frames below 4 and go; `base_seq()` reads 4 and the first
listed segment is `wal/…4.log`. The mutant stops at `bases[i+1] == 4` and keeps `[2,3]`:
`base_seq()` reads 2. The existing `gc_deletes_only_whole_segments_below_keep_from` uses
`keep_from = 20`, also a base — but asserts only `first_base <= 20`, which the mutant
satisfies; this test asserts equality.

### 3.4 `a_failed_sync_poisons_the_log_and_every_later_call_is_refused` — kills #9, #10, #11

A test-local `Disk` wrapper around `sim.disk()` whose `File::sync` fails exactly once (an
`io::Error::other("injected fsync failure")` on the first sync, then healthy). Open a log,
append one record, `poisoned()` is `false`; `sync()` returns the injected error and
`poisoned()` is now `true`; then, with the disk healthy again, `append`, `sync` and `gc`
EACH return an error whose text contains `WAL poisoned by an earlier IO failure` — the
message from `check_poison` — and `poisoned()` stays `true`. Mutant #11 makes every later
call succeed; #9 makes the first read `true`; #10 makes the second read `false`.

The wrapper is the file's third victim/wrapper (after the naive-recovery toys); it stays
test-local. A sim fault axis for "fsync fails once, then heals" is not what the fault
matrix models (its `sync_error` is a rate), and a one-shot injection is exactly what this
property needs.

### 3.5 `read_frame_cuts_zero_length_oversized_and_past_end_frames` — kills #13

Three logs, each built by writing raw real-format frames to `wal/00000000000000000000.log`
on `sim.disk()`: one valid frame (seq 0, 8-byte payload) followed by (a) a frame declaring
`len = 0` with a CORRECT crc for seq 1 over the empty payload; (b) a frame declaring
`len = MAX_RECORD_BYTES + 1` with only 8 payload bytes present; (c) a frame declaring
`len = 64` with 8 bytes present. Each `Wal::open` recovers exactly ONE record (seq 0) and
leaves the segment truncated to that frame's length — the cut. Under the mutant — the first `||` turned `&&`, which binds tighter, so `(len == 0 && len > MAX) || past_end` — arms 1–2 are dead and arm 3 lives: (a) passes CRC and seq and is recovered as a second, EMPTY record — a payload the `append` contract forbids — and is the case that kills the mutant; (b) and (c) are still refused by the past-end arm and pin arms 2 and 3 of the original check. The crc in (a) is deliberately correct so that the size arm is the only thing
between the frame and `Valid`; the report states which arm each case pins.

### 3.6 `a_maximum_size_record_round_trips_and_one_byte_over_is_invalid` — kills #14, #15

`append` accepts `payload.len() <= MAX_RECORD_BYTES` (16 MiB). Append one record of exactly
`MAX_RECORD_BYTES` (segment_bytes large enough that it does not rotate mid-record is
irrelevant — rotation is between frames), sync, reopen: one record recovered, byte-equal.
Mutants #15 (`>=`) and #14 (`==`) both cut it — the `==` RED was observed on this half, not on
the over-by-one half. Then, in a fresh sim, write a raw frame declaring
`MAX_RECORD_BYTES + 1` WITH the bytes present and a correct CRC; `Wal::open` recovers zero
records (the frame is `Invalid` by the size arm). Mutant #14 (`==`) recovers it — a record
the contract forbids. Cost: two ~16 MiB in-memory files; the CRC over 16 MiB is tens of
milliseconds.

### 3.7 `a_remove_that_lies_trips_the_gaplessness_assert_before_the_cut` — kills #2

A test-local `Disk` wrapper whose `remove` returns `Ok(())` without removing. Build a
multi-segment log, corrupt a frame in a non-final segment (flip a payload byte through
`sim.disk()` directly, as `bit_flip_cuts_the_log_there_and_deletes_later_segments` does),
then `Wal::open` through the wrapper. The original reaches the assert with the later
segment still listed and panics with `orphaned segments must be removed before the cut
segment is truncated` — `#[should_panic(expected = …)]` on that phrase. The mutant skips the
assert (its `later` is non-empty), truncates, and returns `Ok` — no panic, test fails.

## §4 Two rewrites

Both in `log.rs`, both semantics-preserving, both in Task order before the sweep:

- `if cut < len {` → `assert!(cut <= len, "recovery cut {cut} is past segment length {len}"); if cut != len {`. `cut > len` is impossible by construction (the cut is an offset the scan reached inside the file); the assert is TigerBeetle-style negative space (hard rule 4) and makes the `!=` honest.
- `if self.tail_offset > 0 && …` → `if self.tail_offset != 0 && …`.

After each rewrite, the NEW site's mutants are enumerated with `cargo mutants --list` and
each is observed CAUGHT by hand-applying it and running the named existing test
(the full `wal` suite for the first — three tests go red, and the single test §2 row 3 first named stays green alone;
`rotation_spreads_records_across_segments_and_recovers` for the second). If a new mutant
survives, it is a property to write in this sub-project, not a rewrite to keep.

## §5 The exclusion list, and its ceiling

`.cargo/mutants.toml` (cargo-mutants reads it from the source root; the verb's `--dir` is
the workspace root) with exactly two `exclude_re` entries, each preceded by a comment that
carries the equivalence argument from §2:

```toml
exclude_re = [
    # Diagnostic impl kept only so `unwrap_err` has a Debug bound; its text is
    # not an error message the operator reads. Body-replacement is equivalent.
    "replace <impl fmt::Debug for Wal>::fmt",
    # At offset + HEADER_BYTES == len the original reads a complete header and
    # the payload-length check returns Invalid; `>=` returns Invalid one read
    # earlier. No surface distinguishes them.
    "replace > with >= in torn_header",
]
```

`torn_header` is a new three-line `fn torn_header(offset: u64, len: u64) -> bool { offset +
HEADER_BYTES > len }` in `log.rs`, called from `read_frame` — so the mutant's `--list` name
carries a unique function and the regex needs no line number. Its other six mutants (`→ true`,
`→ false`, `> → ==`, `> → <`, `+ → -`, `+ → *`) are all observable and were observed CAUGHT
(`+ → -` as an overflow panic at offset 0 in the debug build) (a torn-tail universe in
`wal_survives_the_fault_matrix` reads past EOF under `== `).

The ceiling: `crates/guide-examples/tests/mutants_exclusions.rs` reads `.cargo/mutants.toml`
as text and asserts (a) exactly `EXCLUSIONS = 2` string entries inside `exclude_re`, and
(b) the line above each entry is a `#` comment. Lowering or raising the count is a
deliberate act for a commit message, the same discipline as `doc_truth`'s `PINNED_FLOOR`.
`doc_inventory` will demand the new binary's name in CLAUDE.md's list.

Docs in the same commit (hard rule 6): CLAUDE.md's instrument bullet gains one sentence
naming `.cargo/mutants.toml` as the known-equivalent list and the ceiling test that pins
it; `mutants.rs`'s module doc gains the same; `scripts/mutants.sh`'s header comment names
it. The 2026-09-10 ledger entry's "fifteen … nothing fixed" is closed by this
sub-project's own entry, not edited.

## §6 What this sub-project refuses to do

- **No WAL behaviour change** beyond the two `!=` rewrites and the `torn_header` extraction,
  all three semantics-preserving. If a property seems to need the code to change, the
  property is wrong or the finding is a defect for its own sub-project.
- **No new simulator fault axis.** The one-shot failing sync and the lying `remove` are
  test-local wrappers: the first is a shape the rate-based matrix cannot express, the
  second is a broken invariant, not a fault mode any filesystem exhibits.
- **No exclusion beyond the two named.** A third survivor is a property or a finding.
- **No sweep beyond `log.rs`.** The other files' mutants are a later sweep.

## §7 Exit criteria

1. Seven new tests in `wal.rs`, each with its RED observed under its named mutant(s) and
   pasted in the report, GREEN on the original.
2. The two rewrites landed; every mutant at the rewritten sites and at `torn_header`
   observed CAUGHT by a named existing test.
3. `.cargo/mutants.toml` with two commented exclusions; `mutants_exclusions` gate green,
   and observed RED against a third uncommented entry.
4. `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`,
   `cargo test --workspace --no-fail-fast` green with the count reconciled: **85**
   `test result: ok` (84 on `a93f100` plus `mutants_exclusions`).
5. `scripts/mutants.sh --file crates/charpente-core/src/log.rs --jobs 1`, run in a worktree
   with its own `CARGO_TARGET_DIR`, exits **0**: every viable mutant caught, **0 MISSED**,
   the two exclusions absent from the count, 0 timed out. The run's `outcomes.json`
   summary line is quoted in the ledger with the wall clock.
6. CLAUDE.md, `mutants.rs`'s module doc and `scripts/mutants.sh` name the exclusion list
   and its ceiling.
