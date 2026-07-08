# M1 sub-project 1 — WAL segment management: design

Status: approved by Noah 2026-07-07 (M1 decomposition + both guarantee calls — automatic
checkpoint-anchored GC, and GC-on-by-default with a hard restore-naming error — his).

## M1 decomposition (context)

M1 runs as six sub-projects, each its own spec → plan → build cycle, in this order:
(1) **this spec**; (2) replication — async log shipping, follower reads, manual
promotion, backup, multi-node sim; (3) realtime SSE/WS with exact resume; (4) auth
properly; (5) migrations as log records; (6) introspection polish + the Vörtex-style
edge harness. Sequenced this way because replication ships log segments: shipping
unmanaged segments is rework, and DESIGN already rules the durability fences land with
segment management.

## Scope

The tracked debt cluster DESIGN §§1–4 routes here:

1. Segment GC (the log stops growing forever; `Db::open`'s rebuild-from-seq-0
   assumption retired).
2. The recovery-horizon rule (DESIGN §2 fence 1).
3. Directory-entry durability (DESIGN §2 fence 2).
4. Fault-matrix extensions (DESIGN §1 tracked list): bit-level corruption of durable
   bytes, misdirected writes, latent read errors.

Non-goals: shipping segments anywhere (sub-project 2 — only the retention hook it
needs is designed now); snapshots/backup to object storage (also #2); any change to
the frame or record format; read-concurrency changes.

## 1. Segment GC

**Trigger and anchor.** GC runs inside `checkpoint()`: after the store checkpoint at
seq N is durable, delete whole segments whose highest seq is < N, always keeping at
least `DbConfig::retain_segments` segments (default 2) as slack. No new app-facing
API; nothing is ever deletable above the last durable checkpoint. Deterministic in
sim by construction (GC is a pure function of checkpoint seq + segment layout +
config).

**Checkpoint cadence (plan-time addition).** Planning surfaced that `checkpoint()` has
no production call site — only tests call it — so checkpoint-anchored GC would ship
dark. `Db` therefore owns a deterministic auto-checkpoint cadence:
`DbConfig::checkpoint_every` (applied records, default 4096, `0` = manual only); after
an apply that crosses the threshold, the commit path runs `checkpoint()` (which runs
GC). Core-only, deterministic in sim (a pure function of applied-record count), no
kernel changes. Explicit `Db::checkpoint()` remains available and also runs GC.

**base_seq.** The WAL learns its lowest retained seq. Recovery scans from the base
segment as it does today from seq 0; `Wal` exposes the base for the horizon rule and
for `Db::open`'s rebuild decision. Segment file names already carry `base_seq:020`,
so the base is derivable from the directory listing (`Disk::list` is sorted) — no new
metadata file.

**Retention hook.** `Db` holds an optional retention pin (a `lowest_needed() -> u64`
shaped callback). GC never deletes a segment containing seqs ≥ the pin. Sub-project 2
wires the log shipper into it; this sub-project ships it exercised only by tests.

**The rebuild rule (Noah's call).** `Db::open` with an unopenable store (torn
creation, redb open-assert state) today discards and rebuilds by replaying from
seq 0. With GC: if the store cannot open AND `base_seq > 0`, `Db::open` fails with a
contract-grade error naming the state (store unrecoverable, log compacted to
`base_seq`), the restore path (replica/backup — arriving in M1 replication), and the
config that widens the window (`retain_segments`). If `base_seq == 0` the rebuild
path runs as today. The error text is product surface, pinned by a test. Rationale
for shipping GC on rather than dark: the window is narrow (requires an *unopenable*
store, not a stale one — stale reverts to checkpoint and replays the retained tail,
which the checkpoint anchor guarantees exists), and dark machinery rots unexercised.

## 2. The recovery-horizon rule

At `Db::open`, the store's `applied_seq` (read before WAL recovery) is the durable
horizon. WAL recovery distinguishes:

- invalid frame at seq ≤ horizon → **hard corruption error** naming the frame, the
  horizon, and restore-from-replica/backup. Never a cut: these are acknowledged,
  applied records; truncating them is silent data loss.
- invalid frame at seq > horizon → torn tail; cut, truncate, sync — exactly today's
  behavior.

The standalone `Wal::recover` (no store in sight) keeps today's cut-at-first-invalid
behavior; the horizon rule lives at the `Db::open` layer where the horizon exists.
Recovery remains idempotent in both modes.

## 3. Directory-entry durability

**The one trait change.** `Disk` gains a directory barrier: `sync_dir(&self, prefix:
&str) -> io::Result<()>` (flat namespace, same path contract as `list`). Prod impl
fsyncs the directory fd; sim impl moves pending directory entries to durable.

**Call sites.** `log.rs`: after creating a rotated-in segment (before it takes
writes — extending the existing "old segment synced before new takes writes" rule to
the segment's *name*), and after GC deletions (a deleted-then-resurrected segment
must not reappear with stale frames — resurrection after crash is the sim-modeled
hazard). `table.rs`: after store-file creation at first open.

**Sim model.** The sim disk tracks directory entries like it tracks written bytes:
created/deleted entries are pending until `sync_dir`; crash-time collapse
independently keeps or drops each pending entry (create loss = file vanishes;
delete loss = file resurrects with its last durable content). Auto-covered
`fault:dir-entry` coverage key.

## 4. Fault-matrix extensions

Three new axes in the sim disk, each swarm-sampled per universe:

- **Bit-level corruption of durable bytes** (the Jepsen/TigerBeetle granularity
  lesson): a durable byte flips — single-byte, inside otherwise-valid frames, not
  whole-sector. Makes the horizon rule's "corruption below horizon" testable.
- **Misdirected writes**: a `write_at` payload lands at a wrong offset; the intended
  target stays stale. Write-time fault, durable-state consequence.
- **Latent read errors**: `read_at` returns Err on correctly-written bytes —
  transient or persistent per swarm draw. Recovery and store paths must surface
  these as IO errors (poison/fail), never as silent cuts.

**The frozen-replay constraint (load-bearing).** `run_universes_swarm!`'s FaultConfig
sampling order is a frozen replay contract; inserting draws would silently change
every pinned seed's universe. New fields therefore draw from **field-keyed
sub-streams**: each new FaultConfig field seeds its own splitmix stream from
(universe seed, field-name hash), outside the legacy sequential draw. Existing pinned
seeds keep their exact meaning; future fields compose order-independently. The legacy
sequential fields are frozen forever, like the runners.

**Coverage + meta-tests.** Each axis auto-registers its `fault:*` coverage key. Each
ships with a meta-test proving the harness catches a naive victim (house style —
e.g., bit-flip axis: a recovery that trusts lengths without CRC re-verification below
the horizon must fail; misdirected writes: a store layer that never re-reads what it
wrote must fail; latent reads: a recovery that treats read errors as clean EOF must
fail).

**Ruling (Noah, 2026-07-07): the durable-corruption axes are scoped to WAL files.**
T5's landing found that `bit_flip`/`misdirected_write` reaching the table store's own
file defeats redb's crash consistency independently of anything this codebase does
(internal redb panics, or a silent fallback to an older still-consistent root that
under-reports `applied_seq`). The two axes were built to prove the WAL-layer fences
(frame CRC + the recovery-horizon rule) — that is the domain v0 can honestly defend —
so they are scoped by `FaultConfig.corruption_scope` (default `"wal/"`, a constant,
never swarm-drawn). Store bitrot is recorded as an architectural truth: redb + M1
restore-from-replica is the v0 answer; M2's own storage engine owns page checksums.
`read_error` and `dir_entry_loss` stay global — they produce errors, never silent
wrongness.

**Fix-wave finding (same date, evidence from the scoped re-run): the horizon fence
only defends checkpointed data.** With the axes scoped to `wal/`, a second, distinct
silent-loss mode remained: corruption of an acked frame ABOVE the store's checkpoint
horizon (`applied_seq`; `checkpoint_every` defaults to 4096, so small apps have
near-zero horizon coverage) is indistinguishable from a torn tail and is legitimately
cut — the standard WAL convention (Postgres treats a mid-WAL CRC mismatch as
end-of-log the same way). No v0 single-node fence can close it: bitrot of the only
local copy of un-checkpointed data is unrecoverable by design; replication (M1) is
the answer. Consequence for oracles: acked-durability checks route their shortfall
through `checks::durable_corruption_fired(sim)` — loss is excused ONLY in universes
where a durable-corruption fault actually fired, and the strong oracle stands
everywhere else (consistency oracles like exactly-once are never excused; a cut
reverts to a prefix, which stays consistent). Evidence: instrumented seed
0x8ccda9b5911555fa (http; misdirected WAL append at intended offset 355 landing at
2403 → cut above horizon → 3 acked records gone), invoicing seed 0xcb60fa34ef1ab44c
(bit_flip 0.019, wal-scoped, "acked finalize but invoice 1 is Draft").

## 5. What changes where

- `charpente-core::log` — base_seq, GC (`Wal::gc(to_seq)` called by the store
  checkpoint path), rotation dir-barrier, horizon-aware recovery entry point.
- `charpente-core::table`/`db` — checkpoint() drives GC through the retention pin;
  `Db::open` rebuild rule + horizon read; store-creation dir-barrier.
- `charpente-core::io` — `Disk::sync_dir` (breaking trait change; both impls in the
  same commit; deny-list untouched).
- `charpente-sim::disk` — directory-entry model + three fault axes; `FaultConfig`
  field-keyed sub-stream extension; coverage keys.
- `charpente::prod` — `ProdDisk::sync_dir` (fsync parent dir; the sanctioned module).
- DESIGN §1/§2/§4 updated in the same commits: the two fences move from "tracked" to
  "held"; the fault-matrix tracked list moves to implemented; the rebuild rule
  replaces the "never compacted" caveat.

## 6. Evidence bar

- Every existing pinned seed green; frozen runners and legacy FaultConfig sampling
  untouched (byte-identical replay of an old seed's universe demonstrated once in a
  test).
- New property tests: GC-under-crash (a GC'd log recovers to the checkpoint state +
  retained tail, over the full fault matrix including the new axes); the rebuild
  hard-error fires exactly when store-unopenable ∧ base_seq > 0 (both error text and
  the negative case pinned); horizon rule (corruption below horizon → named error;
  above → cut) — deterministic constructions plus swarm exposure; dir-entry
  create-loss and delete-resurrection round-trips.
- Meta-tests per §4 (three naive victims caught by name).
- Multiple fresh 256-universe swarm batches green across the core suites, plus a
  fresh 1024-universe `examples/invoicing` batch (the demo app runs on this WAL).
- Sim found-bug protocol unchanged: any bug found lands with its seed pinned.
