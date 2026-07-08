# Replication Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** M1 sub-project 2 per `docs/superpowers/specs/2026-07-08-replication-design.md`: frame-level async log shipping, follower reads, zero-loss switchover / bounded-loss failover, backup as a headless follower, and the multi-node sim that proves all of it.

**Architecture:** Bottom-up in ten tasks: the sim grows per-host worlds and the two net fault axes first (nothing above is testable without them); then the core surfaces (WAL frame reader, batch apply, the `NotPrimary` write gate, snapshot export/import + `Db::seed`); then the wire protocol + durable role file as pure logic; then the session engines (ship/ack/seed/diverge) proven at the Db level with a test-side round-robin scheduler; then the control plane (switchover/promote); then kernel integration (a step-able kernel, the replication arm, serve/backup surfaces, the kernel-level multi-node property); then the CLI verbs; then the evidence sweep. DESIGN/GUIDE edits land inside the task that makes them true (hard rule 6).

**Tech Stack:** Rust workspace (edition 2024); no new dependencies (the wire codec reuses `charpente_core::table::Codec`; transport is the existing `Net` traits).

## Global Constraints

- Commit directly to `main`, no Co-Authored-By trailers. One commit per task unless a task says otherwise.
- **Frozen replay contracts** (violating any is a Critical defect): `FaultConfig::swarm`'s sequential draw order (`crates/charpente-sim/src/lib.rs:116-179`) gains no draws; `run_universe` (`lib.rs:1220-1234`) is untouched; new `FaultConfig` fields default to `0.0` and take **zero RNG draws when 0.0**; new extended fields sample ONLY via `sample_extended_fields`'s field-keyed sub-streams (`lib.rs:222-256`). The global sim world (`Sim::disk()`, `restart()`, `crash_disk_*`, `durable_fingerprint()`, the `State.faults/entropy/latency` streams) is frozen: per-host worlds are NEW state that existing single-node universes never touch, so every pinned seed replays byte-identically.
- New fault axes strengthen properties, never weaken assertions: a universe where a new fault fires may end in a *named* error (classified by `checks::is_fault_fallout`), but silent loss or wrong data fails everywhere. Consistency oracles (exactly-once, prefix-consistency, fingerprint equality at equal seq) are NEVER excused by `durable_corruption_fired`.
- Every new fault axis and every protocol fence ships in the same task as its meta-test (a naive victim the harness must catch, by name) — hard rule 3.
- Error messages are product surface (hard rule 5): `NotPrimary` (421 + primary address), `Diverged` (both seqs, both CRCs, the re-seed fix), switchover abort (remaining lag + deadline), role-file/config mismatch (which won and why; the demote-requires-re-seed rule), protocol version mismatch (both versions) — all pinned by tests against text.
- Sim-first evidence: each task ends with the affected suites green over fresh seeds; Task 10 runs the full evidence bar (spec §7). Any sim-found bug lands with its seed pinned.
- `cargo fmt --all --check` and `cargo clippy --workspace --all-targets -- -D warnings` clean before every commit.
- Replication protocol constants, fixed here so tasks agree: `PROTO_VERSION: u8 = 1`; `MAX_MESSAGE_BYTES: usize = 17 << 20` (one 16 MiB record + envelope); `FRAMES_BATCH_BYTES: usize = 256 << 10`; `SNAPSHOT_BATCH_BYTES: usize = 256 << 10`; `HEARTBEAT_NANOS: u64 = 1_000_000_000` (1s); `RECONNECT_NANOS: u64 = 500_000_000` (500ms); `REPL_TICK_NANOS: u64 = 5_000_000` (5ms kernel park cap); `SWITCHOVER_DEADLINE_NANOS: u64 = 30_000_000_000` (30s). All deterministic constants — no entropy draws.

---

### Task 1: Sim per-host worlds

**Files:**
- Modify: `crates/charpente-sim/src/lib.rs` (State, Sim, SimDisk/SimFile host routing, SimListener/SimConn crash semantics)
- Modify: `crates/charpente-sim/src/disk.rs` (nothing structural — `DiskWorld` already carries `crashed`/`crash_countdown`/`epoch`; reused per host)
- Modify: `crates/charpente-sim/src/net.rs` (listener epochs, host-crash conn teardown)
- Modify: `DESIGN.md` §1 (the per-host sim model sentence, replacing net.rs's "Host crash/restart is not modeled yet" caveat — update that module doc too)
- Test: unit tests in `lib.rs`/`net.rs` + `crates/charpente-sim/tests/net_partition.rs`

**Interfaces:**
- Produces: `Sim::host(&self, name: &str) -> SimHost`. `SimHost` (Clone) with `disk() -> SimDisk`, `entropy() -> SimEntropy` (sub-stream keyed by (universe seed, host name)), `net() -> SimNet` (same as `sim.net(name)`), `crash_now()`, `crash_after(ops: u64)`, `restart()`, `durable_fingerprint() -> u64`, `name() -> &str`. Host crash covers `"crash:host"`. The global `sim.disk()`/`sim.restart()`/`sim.durable_fingerprint()` are unchanged (frozen).

- [ ] **Step 1: State grows host worlds (failing unit tests first).** In `lib.rs` tests:

```rust
#[test]
fn host_worlds_are_isolated_disks() {
    let sim = Sim::with_faults(0x805_0001, FaultConfig::none());
    let a = sim.host("a");
    let b = sim.host("b");
    let fa = a.disk().open("wal/x").unwrap();
    fa.write_at(0, b"aaa").unwrap();
    fa.sync().unwrap();
    assert!(b.disk().list("wal/").unwrap().is_empty(), "b must not see a's files");
    assert!(sim.disk().list("wal/").unwrap().is_empty(), "the global world must not see host files");
    assert_ne!(a.durable_fingerprint(), b.durable_fingerprint());
}

#[test]
fn host_crash_is_scoped_to_that_host() {
    let sim = Sim::with_faults(0x805_0002, FaultConfig::none());
    let a = sim.host("a");
    let b = sim.host("b");
    let fa = a.disk().open("f").unwrap();
    fa.write_at(0, b"unsynced").unwrap();
    let fb = b.disk().open("f").unwrap();
    fb.write_at(0, b"unsynced").unwrap();
    a.crash_now();
    assert!(fa.len().is_err(), "a's handles die at a's crash");
    assert_eq!(fb.len().unwrap(), 8, "b is untouched");
    a.restart();
    let fa = a.disk().open("f").unwrap();
    assert_eq!(fa.len().unwrap(), 0, "none() drops a's unsynced writes");
    assert_eq!(fb.len().unwrap(), 8, "b's page cache survives a's restart");
    assert_eq!(sim.coverage()["crash:host"], 1);
}

#[test]
fn host_entropy_streams_are_independent_of_interleaving() {
    // Draw from a twice then b once vs b once then a twice: a's draws and
    // b's draws must be identical either way (field-keyed sub-streams).
    let draws = |order_ab: bool| {
        let sim = Sim::with_faults(0x805_0003, FaultConfig::none());
        let (a, b) = (sim.host("a"), sim.host("b"));
        let mut out = Vec::new();
        if order_ab {
            out.push(("a", a.entropy().next_u64()));
            out.push(("a", a.entropy().next_u64()));
            out.push(("b", b.entropy().next_u64()));
        } else {
            out.push(("b", b.entropy().next_u64()));
            out.push(("a", a.entropy().next_u64()));
            out.push(("a", a.entropy().next_u64()));
        }
        out.sort();
        out
    };
    assert_eq!(draws(true), draws(false));
}
```

Run: `cargo test -p charpente-sim --lib host_` — Expected: FAIL (no `Sim::host`).

- [ ] **Step 2: Implement host worlds.** In `lib.rs`:

`State` (`lib.rs:258-271`) gains `hosts: BTreeMap<String, HostWorld>`:

```rust
struct HostWorld {
    disk: DiskWorld,
    /// Fault + entropy streams keyed by (universe seed, host name) — the
    /// field-keyed sub-stream construction (`sample_extended_fields`), so a
    /// host's rolls never depend on other hosts' op interleaving, and no
    /// host perturbs the frozen global streams.
    faults: Rng,
    entropy: Rng,
}
```

Add a keyed-stream helper (hoist the FNV-1a body out of `sample_extended_fields` into `fn keyed_stream(seed: u64, name: &str) -> Rng` and reuse it there — same bytes, one copy). `HostWorld::new(seed, host)`: `faults: keyed_stream(seed, &format!("host-faults:{host}"))`, `entropy: keyed_stream(seed, &format!("host-entropy:{host}"))`. Latency stays on the global `State.latency` stream (time is one dimension), and virtual time stays universe-global.

`SimDisk`/`SimFile` gain `host: Option<String>` (`None` = the frozen global world). Refactor `State::disk_op` and `file_state` to take the host key and route to the right `DiskWorld` (`state.world_of(host: &Option<String>) -> &mut DiskWorld` + `state.faults_of(host) -> &mut Rng`); every existing call site passes `None` — behavior byte-identical. All fault rolls (`write_error`, `sync_error`, `read_error`, `misdirected_write`, crash collapse, dir-entry collapse, bit_flip) for a host draw from that host's `faults` rng; global-world rolls keep using `State.faults` verbatim.

`Sim::host(name)` returns `SimHost { world: Arc<World>, name: String }` creating the `HostWorld` lazily. `SimHost::crash_now/crash_after` mirror `Sim::crash_disk_now/crash_disk_after` on the host's `DiskWorld` (cover `"crash:host"` on the transition, once). `SimHost::restart()` mirrors `Sim::restart` (`lib.rs:431-486`) exactly, scoped to the host: bit_flip roll on the host's `faults` over the host's in-scope files, per-file `collapse`, `collapse_dir_entries` when `dir_entry_loss > 0.0`, epoch bump, crash flags cleared — and then net teardown (Step 3). `SimHost::durable_fingerprint()` is the FNV loop from `Sim::durable_fingerprint` (`lib.rs:498-512`) over the host's files (extract a `fn fingerprint_files(files: &BTreeMap<String, FileState>) -> u64` and call it from both — the global method's bytes unchanged).

Run: `cargo test -p charpente-sim --lib host_` — Expected: the first two PASS; entropy test PASSES.

- [ ] **Step 3: Host crash tears down its network presence (failing test first).** In `net.rs` tests (or `lib.rs`):

```rust
#[test]
fn host_crash_closes_conns_and_listeners() {
    let sim = Sim::with_faults(0x805_0004, FaultConfig::none());
    let a = sim.host("a");
    let b = sim.host("b");
    let listener = b.net().listen("b:7000").unwrap();
    let conn_a = a.net().connect("b:7000").unwrap();
    let conn_b = poll_accept(&sim, &listener); // helper: clock-stepping accept loop
    conn_a.send(b"hi").unwrap();
    sim.clock().sleep(std::time::Duration::from_secs(1));
    let mut buf = [0u8; 16];
    assert_eq!(conn_b.recv(&mut buf).unwrap(), Some(2));

    b.crash_now();
    b.restart();
    // The surviving side observes the close; the crashed side's old handles error.
    assert_eq!(conn_a.recv(&mut buf).unwrap(), Some(0), "peer closed");
    assert!(conn_b.send(b"x").is_err(), "a crashed host's old conn handle is dead");
    // The listener slot is free again after restart.
    let _relisten = b.net().listen("b:7000").expect("crash freed the listener name");
}
```

Implement in `net.rs`/`lib.rs`:
- `ConnState` gains nothing; `NetWorld` gains `pub fn teardown_host(&mut self, host: &str)`: for every conn where `host_a == host` set `open_a = false` (and symmetrically `open_b`), clear the in-flight queue TOWARD the crashed host (`to_a` if `host_a == host` — undelivered messages to a dead host are gone), and remove every listener whose `host_of(addr) == host`.
- `SimConn::send`'s `assert!(my_open, …)` (`lib.rs:902`) becomes an error — a crashed host's stale handle is now a legitimate runtime state, not a caller bug:

```rust
        if !my_open {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "send on a connection whose host crashed or closed it — reconnect after restart",
            ));
        }
```

  `recv` on a closed own side likewise returns `Err(NotConnected)` (peer-closed detection at `lib.rs:950` — `None if !peer_open => Ok(Some(0))` — is for the SURVIVING side and stays).
- Listener epochs so a dead handle can't clobber a re-listen: `NetWorld.listeners: BTreeMap<String, ListenerState>` where `ListenerState { backlog: VecDeque<u64>, token: u64 }` (token from a `next_listener: u64` counter). `SimListener` stores its token; `accept` returns `Err(NotConnected)` on token mismatch or missing entry; `Drop` removes the entry only if the token matches.
- `SimHost::restart()` calls `net.teardown_host(&self.name)` (crash tears down; restart just makes re-listen possible — the entry was already removed at teardown, so call teardown from `crash_now`/countdown-trip as well as restart; use a `DiskWorld.crashed`-transition guard so it runs once per crash).

Run: `cargo test -p charpente-sim --lib && cargo test -p charpente-sim --test net_partition` — Expected: PASS (partition tests never crash hosts; verify no reliance on the removed send assert).

- [ ] **Step 4: Frozen-world regression + DESIGN + commit.** Add the discriminating test:

```rust
#[test]
fn creating_hosts_never_perturbs_the_global_world() {
    let run = |with_hosts: bool| {
        let sim = Sim::new(0x805_0005); // default faults — draws happen
        if with_hosts {
            let h = sim.host("bystander");
            let f = h.disk().open("noise").unwrap();
            let _ = f.write_at(0, b"noise");
            let _ = f.sync();
        }
        let disk = sim.disk();
        let f = disk.open("f").unwrap();
        let mut payload = [0u8; 256];
        sim.entropy().fill(&mut payload);
        let _ = f.write_at(0, &payload);
        let _ = f.sync();
        sim.restart();
        sim.durable_fingerprint()
    };
    assert_eq!(run(false), run(true), "host activity must not shift global draws");
}
```

DESIGN §1: replace the tracked per-host caveat with the implemented model (host worlds, keyed sub-streams, `crash:host`, global world frozen). Update `net.rs`'s module doc (its "not modeled yet" sentence).

Run: `cargo test --workspace && cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check` — Expected: green (pinned seeds untouched: no global-stream draws added).

```bash
git add crates/charpente-sim/src DESIGN.md crates/charpente-sim/tests
git commit -m "sim: per-host worlds — host-scoped disks, crash/restart, net teardown; global world frozen (repl T1)"
```

---

### Task 2: Net fault axes — `conn_drop` and `net_spike`

**Files:**
- Modify: `crates/charpente-sim/src/lib.rs` (FaultConfig fields + validate + none(), `sample_extended_fields`, SimConn injection)
- Modify: `DESIGN.md` §1 (fault matrix list), `docs/superpowers/specs/2026-07-08-replication-design.md` §4 (one-line amendment: the spec's `ship_latency` lands as `net_spike`, applied to every conn — the sim cannot and should not know which conns are "replication"; a superset that still covers them)
- Test: unit tests in `lib.rs`

**Interfaces:**
- Produces: `FaultConfig.conn_drop: f64` (P per `send` that the connection dies: both sides close, the in-flight queues drop, cover `fault:conn_drop`) and `FaultConfig.net_spike: f64` (P per `send` that this message's latency is ×64 the drawn value, cover `fault:net_spike`). Defaults 0.0, zero-draw when 0.0, sampled only via `sample_extended_fields` sub-streams.

- [ ] **Step 1: Failing unit tests.**

```rust
#[test]
fn conn_drop_kills_the_connection_and_is_covered() {
    let sim = Sim::with_faults(0xD20_0001, FaultConfig { conn_drop: 1.0, ..FaultConfig::none() });
    let a = sim.host("a");
    let b = sim.host("b");
    let listener = b.net().listen("b:1").unwrap();
    let conn = a.net().connect("b:1").unwrap();
    let server = poll_accept(&sim, &listener);
    let err = conn.send(b"doomed").unwrap_err();
    assert!(err.to_string().contains("injected"), "{err}");
    let mut buf = [0u8; 8];
    assert_eq!(server.recv(&mut buf).unwrap(), Some(0), "the other side sees close");
    assert_eq!(sim.coverage()["fault:conn_drop"], 1);
}

#[test]
fn net_spike_delays_but_delivers() {
    let mut cfg = FaultConfig::none();
    cfg.net_latency = (Duration::from_millis(1), Duration::from_millis(1));
    cfg.net_spike = 1.0;
    let sim = Sim::with_faults(0xD20_0002, cfg);
    // send; assert not delivered after 1ms but delivered after 64ms.
}

#[test]
fn extended_sampling_of_net_axes_is_order_independent() {
    // extend extended_field_sampling_never_touches_the_legacy_stream's
    // pattern: legacy fields identical with/without; conn_drop/net_spike
    // deterministic per (seed, field name).
}
```

Run: `cargo test -p charpente-sim --lib conn_drop` — Expected: FAIL.

- [ ] **Step 2: Implement.** `FaultConfig` gains the two fields (doc comments in the house voice; `none()`/`Default`/`validate()` updated; `swarm` does NOT draw them — same comment block as the existing extended fields at `lib.rs:158-173`). `sample_extended_fields` appends (order-independent by construction):

```rust
    let mut r = stream(seed, "conn_drop");
    cfg.conn_drop = if r.chance(0.25) { r.below(30) as f64 / 1000.0 } else { 0.0 };
    let mut r = stream(seed, "net_spike");
    cfg.net_spike = if r.chance(0.25) { r.below(200) as f64 / 1000.0 } else { 0.0 };
```

Injection in `SimConn::send` (`lib.rs:873-921`), both zero-draw-gated, rolls on the GLOBAL `faults` stream (a connection spans two hosts; the wire belongs to neither):
- `conn_drop` first (before latency): if it fires, mark `fault:conn_drop`, set both `open_a`/`open_b` false, clear both queues, return `Err(io::Error::new(ConnectionReset, "injected connection drop — the wire died mid-conversation; reconnect"))`.
- `net_spike` inside the latency computation: after drawing `latency`, `if cfg.net_spike > 0.0 && guard.faults.chance(cfg.net_spike) { mark "fault:net_spike"; latency = latency.saturating_mul(64).max(1); }`.

Run: `cargo test -p charpente-sim` — Expected: PASS, including the untouched-legacy pin test.

- [ ] **Step 3: DESIGN/spec lines + verify + commit.** DESIGN §1 fault-matrix paragraph gains the two axes (sub-stream sampled, wire-scoped). Spec §4 amendment sentence as noted in Files.

Run: `cargo test --workspace && cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check` — Expected: green.

```bash
git add crates/charpente-sim/src DESIGN.md docs/superpowers/specs/2026-07-08-replication-design.md
git commit -m "sim: conn_drop + net_spike axes via field-keyed sub-streams — the wire joins the fault matrix (repl T2)"
```

---

### Task 3: Core surfaces — WAL frame reader, batch apply, the `NotPrimary` gate

**Files:**
- Modify: `crates/charpente-core/src/log.rs` (`frame_crc` pub, `Wal::read_from`)
- Modify: `crates/charpente-core/src/engine.rs` (`Engine::submit_batch`, `wal(&self)`)
- Modify: `crates/charpente-core/src/db.rs` (`TxError::NotPrimary`, write gate, `Db::apply_shipped`, `Db::read_log`)
- Modify: `crates/charpente/src/http/app.rs:252-262` (the 421 arm), `crates/charpente/src/kernel/dispatch.rs:187-254` + `crates/charpente/src/kernel/cron.rs:120-124` (exhaustive-match updates)
- Modify: `DESIGN.md` §2 (one sentence: the log gains a verified read surface for the shipper), §3 (follower reads: effect-free transactions run anywhere; effectful ones need the primary)
- Test: `crates/charpente-sim/tests/wal.rs` (read_from), `crates/charpente/tests/transactions.rs` (gate + apply_shipped), `crates/charpente/tests/http.rs` (421)

**Interfaces:**
- Consumes: nothing new (pure core).
- Produces: `pub fn frame_crc(seq: u64, payload: &[u8]) -> u32` (log.rs, was private); `Wal::read_from(&self, from_seq: u64, up_to: u64, max_bytes: usize) -> io::Result<Vec<(u64, Vec<u8>)>>`; `Engine::submit_batch(&mut self, frames: &[(u64, Vec<u8>)]) -> io::Result<()>`; `Engine::wal(&self) -> &Wal`; `TxError::{App, Io, NotPrimary { primary: String }}`; `Db::forbid_writes(&mut self, primary: String)` / `Db::allow_writes(&mut self)` / `Db::writes_allowed(&self) -> bool`; `Db::apply_shipped(&mut self, frames: &[(u64, Vec<u8>)]) -> io::Result<()>`; `Db::read_log(&self, from_seq: u64, max_bytes: usize) -> io::Result<Vec<(u64, Vec<u8>)>>` (caps `up_to` at `last_applied` — never ships unsynced tail); HTTP maps `NotPrimary` to 421.

- [ ] **Step 1: `read_from` (failing tests in `wal.rs` first).**

```rust
#[test]
fn read_from_returns_verified_frames_across_segments() {
    // Fault-free sim, segment_bytes: 64 forces rotation. Append 40 records
    // (8-byte payloads, sync each 4th), then read_from(13, 27, usize::MAX):
    // exactly seqs 13..=27, payloads byte-equal to what was appended.
    // read_from(0, 39, 64): stops after the first frame(s) that fit 64
    // payload bytes but returns at least one frame.
}

#[test]
fn read_from_refuses_a_corrupt_frame_below_up_to() {
    // Append 10, sync. Flip one durable byte inside frame 4's payload (reuse
    // the frame-locating helper pattern at wal.rs's bit-flip tests). Then
    // read_from(0, 9, usize::MAX) must Err naming the frame and the restore
    // path — never skip or truncate:
    let err = wal.read_from(0, 9, usize::MAX).unwrap_err();
    for needle in ["frame 4", "shipped", "restore"] {
        assert!(err.to_string().contains(needle), "{err}");
    }
}

#[test]
fn read_from_below_base_is_a_named_error() {
    // After gc, read_from(base - 1, …) errs naming base and the snapshot path.
}
```

Run: `cargo test -p charpente-sim --test wal read_from` — Expected: FAIL.

- [ ] **Step 2: Implement `read_from`.** In `log.rs` (below `gc`): make `frame_crc` `pub` with a doc line (both replication sides recompute it; the Hello integrity anchor). `read_from` takes `&self` — reads must not poison (a transient `read_at` error surfaces to the caller, who retries or fails the session):

```rust
    /// Reads back already-appended frames `[from_seq, up_to]`, re-verifying
    /// every CRC (the shipper's read surface — a frame corrupted on disk
    /// after append must never travel). Stops early once `max_bytes` of
    /// payload have accumulated, but always returns at least one frame when
    /// any is available. `up_to` must not exceed the durable end — callers
    /// pass the engine's `last_applied`.
    pub fn read_from(&self, from_seq: u64, up_to: u64, max_bytes: usize) -> io::Result<Vec<(u64, Vec<u8>)>> {
        if from_seq < self.base_seq {
            return Err(corrupt(format!(
                "read_from({from_seq}) but the log's base is {} — those frames are GC'd; \
                 ship a snapshot instead (DESIGN §5)",
                self.base_seq
            )));
        }
        assert!(up_to < self.next_seq, "read_from up_to {up_to} is past the log end {}", self.next_seq);
        let prefix = format!("{}/", self.cfg.dir);
        let names = self.disk.list(&prefix)?;
        let mut out = Vec::new();
        let mut bytes = 0usize;
        for name in &names {
            let base = parse_segment_name(&self.cfg.dir, name)?;
            // Skip whole segments that end before from_seq; a segment's end
            // is the next segment's base (the tail segment ends at next_seq).
            // (Walk with the index to peek names[i + 1], as gc does.)
            …
            let file = self.disk.open(name)?;
            let len = file.len()?;
            let mut offset = 0u64;
            let mut seq = base;
            while seq <= up_to {
                match read_frame(&*file, len, offset, seq)? {
                    Frame::Valid { payload } => {
                        let flen = payload.len();
                        if seq >= from_seq {
                            bytes += flen;
                            out.push((seq, payload));
                            if bytes >= max_bytes { return Ok(out); }
                        }
                        seq += 1;
                        offset += HEADER_BYTES + flen as u64;
                    }
                    Frame::CleanEnd => break,
                    Frame::Invalid => {
                        return Err(corrupt(format!(
                            "log frame {seq} failed verification while being read back for \
                             shipping — durable bytes below the acknowledged end are corrupt; \
                             restore this node from a replica or backup (DESIGN §2)"
                        )));
                    }
                }
            }
            if seq > up_to { break; }
        }
        Ok(out)
    }
```

(The tail segment's unsynced-but-appended frames are still `Frame::Valid` in the file view — the `up_to`/`last_applied` cap is what keeps them off the wire; `submit` syncs before apply, so everything ≤ `last_applied` is durable.)

Run: `cargo test -p charpente-sim --test wal` — Expected: PASS.

- [ ] **Step 3: `submit_batch` + the write gate + `apply_shipped` (failing tests in `transactions.rs` first).**

```rust
#[test]
fn forbidden_writes_fail_with_not_primary_and_reads_still_work() {
    // Build a Db on a fault-free sim; commit one row; db.forbid_writes("primary-1:7000".into()).
    // A read-only transact (get) succeeds; an effectful one returns
    // TxError::NotPrimary { primary } with primary == "primary-1:7000";
    // db.allow_writes() restores commits.
}

#[test]
fn apply_shipped_replays_records_byte_identically() {
    // Two Dbs on two sim hosts' disks, same Schema. Commit 20 transactions
    // on A (mixed puts/deletes/enqueues). Ship: read A's records via
    // db_a.read_log(0, usize::MAX) and db_b.apply_shipped(&frames) in two
    // batches. Assert: b.last_applied() == a.last_applied(), every row equal
    // via scans, and (after checkpoint on both + a re-open) the two hosts'
    // durable_fingerprint()s of their store files match at equal seq — the
    // convergence oracle in miniature.
}

#[test]
fn apply_shipped_refuses_a_seq_gap_by_name() {
    // apply_shipped with frames starting at seq 5 on a fresh Db:
    let err = db_b.apply_shipped(&frames[5..]).unwrap_err();
    assert!(err.to_string().contains("expects seq"), "{err}");
}

#[test]
fn apply_shipped_still_works_while_writes_are_forbidden() {
    // forbid_writes THEN apply_shipped — the follower's own path is not gated.
}
```

Implement:
- `engine.rs` — `pub fn wal(&self) -> &Wal { &self.wal }` beside `wal_mut` (`engine.rs:139-141`), and `submit_batch` beside `submit` (`engine.rs:109-129`), same poison discipline, one sync for the batch:

```rust
    /// The follower's apply path: append every frame, one sync, then apply
    /// each — the same append→sync→apply discipline as `submit`, minus the
    /// client ack, amortizing the sync over a shipped batch. Shipped seqs
    /// cross a trust boundary, so a gap is a named error, not an assert.
    pub fn submit_batch(&mut self, frames: &[(u64, Vec<u8>)]) -> io::Result<()> {
        if self.poisoned { return Err(io::Error::other(/* same text as submit */)); }
        for (seq, payload) in frames {
            if *seq != self.wal.next_seq() {
                return Err(io::Error::new(io::ErrorKind::InvalidData, format!(
                    "shipped frame carries seq {seq} but this log expects seq {} — \
                     the ship stream is out of order or from a different timeline; \
                     reconnect and re-Hello", self.wal.next_seq()
                )));
            }
            self.wal.append(payload)?;
        }
        self.wal.sync()?;
        for (seq, payload) in frames {
            assert_eq!(*seq, self.last_applied.map_or(0, |l| l + 1), "apply loop skipped a seq — append and apply have diverged");
            if let Err(e) = self.machine.apply(*seq, payload) {
                self.poisoned = true;
                return Err(e);
            }
            self.last_applied = Some(*seq);
        }
        Ok(())
    }
```

- `db.rs` — `TxError` gains `NotPrimary { primary: String }` (doc: the follower rejection; nothing was written). `Db` gains `not_primary: Option<String>` (init `None` in `open`; omit from `Debug` like the pin or include — include, it's `Debug`). `transact` (`db.rs:591-594`): after `f` returns `Ok` and before encoding, `if !tx.effects.is_empty() && let Some(primary) = &self.not_primary { return Err(TxError::NotPrimary { primary: primary.clone() }); }` (keep the empty-effects early return FIRST — reads never touch the gate). `forbid_writes`/`allow_writes`/`writes_allowed` are three-line methods beside `pin_retention`. `apply_shipped`:

```rust
    /// Applies frames shipped from the primary: the follower's only write
    /// path (DESIGN §5). Runs the checkpoint cadence like transact — a
    /// follower checkpoints and GCs its own log on the same schedule.
    pub fn apply_shipped(&mut self, frames: &[(u64, Vec<u8>)]) -> io::Result<()> {
        if frames.is_empty() { return Ok(()); }
        self.engine.submit_batch(frames)?;
        self.records_since_checkpoint += frames.len() as u64;
        if self.cfg.checkpoint_every > 0 && self.records_since_checkpoint >= self.cfg.checkpoint_every {
            self.checkpoint()?;
        }
        Ok(())
    }

    /// Verified read-back of committed records for the shipper; `up_to` is
    /// capped at `last_applied`, so the unsynced tail never travels.
    pub fn read_log(&self, from_seq: u64, max_bytes: usize) -> io::Result<Vec<(u64, Vec<u8>)>> {
        let Some(last) = self.engine.last_applied() else { return Ok(Vec::new()) };
        if from_seq > last { return Ok(Vec::new()); }
        self.engine.wal().read_from(from_seq, last, max_bytes)
    }
```

- Match-site sweep: `grep -rn "TxError" crates examples docs/GUIDE.md`. `dispatch.rs:193` and the `map_err` at `dispatch.rs:237-239`, `cron.rs:120-124` `flatten`: add a `TxError::NotPrimary { .. }` arm that returns `io::Error::other("BUG: a job/cron transaction hit NotPrimary — the kernel must not run dispatch or cron on a follower (DESIGN §5)")` (the T8 kernel gate makes it unreachable; if it ever fires it must fail loudly, not silently retry). `app.rs:252-262` gains:

```rust
            Err(TxError::NotPrimary { primary }) => Response::error(
                421,
                &format!(
                    "this node is a read-only follower — writes go to the primary at \
                     {primary}; retry there (the request was not committed)"
                ),
            ),
```

If GUIDE shows a `TxError` match (grep), update it in the same commit and run `cargo build -p guide-examples`.

- `http.rs` test:

```rust
#[test]
fn writes_on_a_follower_get_421_naming_the_primary() {
    // Build App on a sim disk with one write route; app.db_for_tests()
    // .forbid_writes("primary-1:9000".into()); handle a write request:
    // status 421, body contains "primary-1:9000" and "follower".
    // A read route still answers 200.
}
```

Run: `cargo test -p charpente --test transactions --test http && cargo test -p charpente-core` — Expected: PASS.

- [ ] **Step 4: DESIGN sentences + verify + commit.**

Run: `cargo test --workspace && cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check` — Expected: green.

```bash
git add crates/charpente-core/src crates/charpente/src crates/charpente-sim/tests/wal.rs crates/charpente/tests DESIGN.md docs/GUIDE.md
git commit -m "core: verified WAL read-back, batch apply, and the NotPrimary write gate — the shipper's substrate (repl T3)"
```

---

### Task 4: Logical snapshot export/import + `Db::seed`

**Files:**
- Modify: `crates/charpente-core/src/table.rs` (`Snapshot::{applied_seq, next_job_id, scan_raw}`, `Store::{import, set_import_meta}`)
- Modify: `crates/charpente-core/src/db.rs` (`Schema: Clone` + `table_names`, `Db::into_seeder`, `Seeder`)
- Modify: `crates/charpente-core/src/log.rs` (`segment_name` → `pub(crate)`, `Wal::into_disk`), `crates/charpente-core/src/engine.rs` (`Engine::into_wal_and_machine` or equivalent — see Step 2)
- Modify: `DESIGN.md` §4/§5 (the logical-snapshot seed path)
- Test: `crates/charpente/tests/transactions.rs`

**Interfaces:**
- Consumes: `apply_shipped`/`read_log` (T3).
- Produces: `Schema` is `Clone`; `Schema::table_names(&self) -> Vec<&'static str>` (sorted — BTreeMap order); `Snapshot::applied_seq(&self) -> io::Result<Option<u64>>` and `next_job_id(&self) -> io::Result<u64>` (read META off the snapshot's OWN txn — consistent with its rows, unlike `Store::applied_seq` which opens a new one); `Snapshot::scan_raw(&self, table: &str) -> io::Result<Vec<(u64, Vec<u8>)>>` (raw row bytes, id order; empty when the table doesn't exist); `Db::into_seeder(self) -> io::Result<Seeder>` and `Seeder::new(disk: Box<dyn Disk>, cfg: DbConfig, schema: Schema) -> io::Result<Seeder>` (both discard any existing store + `wal/` segments, dir-barriered); `Seeder::rows(&mut self, table: &str, rows: &[(u64, Vec<u8>)]) -> io::Result<()>` (typed apply through the schema ops — indexes rebuilt); `Seeder::finish(self, applied_seq: u64, next_job_id: u64) -> io::Result<Db>` (meta set, store checkpointed, an empty segment created at `applied_seq + 1` + `sync_dir`, then the normal `Db::open` — recovery replays nothing, `last_applied == applied_seq`).

- [ ] **Step 1: Failing tests** in `transactions.rs`:

```rust
#[test]
fn seed_rebuilds_a_byte_identical_store() {
    // Host A: Db with two app tables + jobs (enqueue some, run none).
    // Commit 30 txns, checkpoint (so applied_seq is set and gc ran).
    // Export: snapshot = db_a.snapshot(); for each of schema.table_names()
    // (build the same schema both sides), scan_raw; note snapshot.applied_seq()
    // and snapshot.next_job_id().
    // Host B: Seeder::new(b_disk, cfg, schema).rows(…each batch…)
    // .finish(applied, next_job_id) → a working Db:
    //  - last_applied() == applied
    //  - every typed scan equal to A's
    //  - a fresh commit on B (allow_writes default) appends at applied + 1
    //  - B's WAL base == applied + 1 (list the dir, parse the first name)
    //  - reopen B (drop + Db::open) — recovery replays nothing, state intact.
}

#[test]
fn into_seeder_discards_the_old_timeline() {
    // Db with 10 committed records → into_seeder() → finish(4, 0) from a
    // 5-record export: the old wal/ files are gone (list shows only the
    // empty base-5 segment); the store matches the 5-record state.
}

#[test]
fn seeded_indexes_answer_queries() {
    // A table with an #[index] field: seed B, then scan_by on B finds the
    // same rows — proof the import path maintains index tables.
}
```

Run: `cargo test -p charpente --test transactions seed_` — Expected: FAIL.

- [ ] **Step 2: Implement.**
- `table.rs`: derive nothing on `Store`; add the three `Snapshot` methods (same META-reading shape as `Store::applied_seq` at `table.rs:298-308`, on `self.txn`; `scan_raw` is `Snapshot::scan`'s loop (`table.rs:510-522`) minus the decode — `out.push((key, value.value().to_vec()))`). `Store::import(&self, f: impl FnOnce(&mut Writer<'_>) -> io::Result<()>) -> io::Result<()>`: a `Durability::None` write txn constructing a `Writer { txn: &txn, jobs_materialized: 0 }` with NO seq/next_job_id bookkeeping (imports set meta explicitly); `Store::set_import_meta(&self, applied_seq: u64, next_job_id: u64) -> io::Result<()>` inserts both META keys in one `Durability::None` txn.
- `db.rs`: `#[derive(Clone)]` on `TableOps` (two fn pointers) and `Clone` for `Schema` (derive over the BTreeMap). `table_names` returns `self.tables.keys().copied().collect()`. The `Seeder`:

```rust
/// Builds a node's state from a primary's logical snapshot stream: rows in,
/// then `finish` pins the metadata, checkpoints, and lays an empty WAL
/// segment at `applied_seq + 1` — exactly the base = horizon + 1 boundary
/// `Db::open` accepts (DESIGN §5). Everything previously in this data dir
/// is discarded at construction: seeding replaces the timeline.
pub struct Seeder {
    disk: Box<dyn Disk>,
    cfg: DbConfig,
    schema: Schema,          // the caller's schema, passed to Db::open at finish
    import_schema: Schema,   // schema + JobRow, used to decode/import rows
    store: Store,
}
```

`Seeder::new`: remove the store file (NotFound tolerated) + `sync_dir(&cfg.store_path)`; `disk.list(&format!("{}/", cfg.wal.dir))?` and remove each + one `sync_dir`; `Store::open` fresh; `import_schema = schema.clone().table::<JobRow>()`. `rows`: `self.store.import(|w| { for (id, bytes) in rows { let _ = id; (self.import_schema.ops(table)?.apply_put)(w, bytes)?; } Ok(()) })` (the id is IN the row bytes — `apply_put` decodes and `put` re-derives it; keep the tuple shape anyway: the wire carries ids for debuggability and future delta use). `finish`: `store.set_import_meta(applied_seq, next_job_id)?; store.checkpoint()?; drop(store);` then create the empty segment: `let name = crate::log::segment_name(&cfg.wal.dir, applied_seq + 1); disk.open(&name)?; disk.sync_dir(&format!("{}/", cfg.wal.dir))?;` then `Db::open(disk, cfg, schema)`. Make `segment_name` `pub(crate)` in log.rs.

Wait — dropping and reopening the store means `Db::open` re-opens it; redb re-open through `Store::open` is the normal path. ✓

- `Db::into_seeder(self)`: deconstruct to recover the disk. `log.rs`: `pub(crate) fn into_disk(self) -> Box<dyn Disk> { self.disk }` on `Wal`; `engine.rs`: `pub fn into_wal(self) -> Wal { self.wal }` — then `db.rs`: `let disk = self.engine.into_wal().into_disk(); Seeder::new(disk, self.cfg, /* schema */ …)`. The `Db` does not retain its `Schema` (it lives inside `TableMachine`) — thread it out: `engine.rs` gains `pub fn into_parts(self) -> (Wal, S)` instead of `into_wal`, and `TableMachine` gives back its `schema` (add `pub(crate) fn into_schema(self) -> Schema` or destructure — `TableMachine` is db.rs-private, plain field access). NOTE: the recovered schema already includes `JobRow` (added at `Db::open`); `Seeder::new` must NOT add it again — give `Seeder` a second constructor `Seeder::from_registered(disk, cfg, schema_with_jobrow)` used by `into_seeder`, where `import_schema = schema.clone()` and `finish` calls a `Db::open`-variant… simpler: keep ONE `Seeder::new(disk, cfg, schema)` contract = "schema WITHOUT JobRow" and have `into_seeder` pass a schema stripped of nothing — instead, add `Schema::without_jobs(mut self) -> Schema` — NO. Cleanest: `Db::open` currently does `let schema = schema.table::<JobRow>();` — change `Seeder` to hold `schema_for_open: Schema` (no JobRow) and `into_seeder` cannot recover that exactly… Resolve it structurally: add `Schema::remove(&mut self, table: &'static str)` — three lines, `pub(crate)`, used only by `into_seeder` to strip `JobRow::TABLE` before handing to `Seeder::new`. Deterministic, honest, no double-registration assert.

Run: `cargo test -p charpente --test transactions` — Expected: PASS.

- [ ] **Step 3: DESIGN + verify + commit.** DESIGN §5 mechanism paragraph gains the seed shape (logical rows + meta + empty segment at horizon + 1).

Run: `cargo test --workspace && cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check` — Expected: green.

```bash
git add crates/charpente-core/src crates/charpente/tests/transactions.rs DESIGN.md
git commit -m "db: logical snapshot export/import — Seeder lays a follower's store + WAL base at horizon+1 (repl T4)"
```

---

### Task 5: The replication protocol codec + the durable role file

**Files:**
- Create: `crates/charpente/src/repl/mod.rs` (module decl only this task: `pub mod protocol; pub mod role;` + the shared constants from Global Constraints)
- Create: `crates/charpente/src/repl/protocol.rs`
- Create: `crates/charpente/src/repl/role.rs`
- Modify: `crates/charpente/src/lib.rs` (`pub mod repl;`)
- Test: unit tests in both new files

**Interfaces:**
- Produces (`protocol.rs`): `pub const PROTO_VERSION: u8 = 1;` the constants block;

```rust
#[derive(Debug, PartialEq)]
pub enum Msg {
    /// tag 0 — session open. `listen` is the follower's own replication
    /// address when it has one (switchover targets sessions by it);
    /// `position` is (last durable seq, that frame's crc) or None when empty.
    Hello { proto: u8, listen: Option<String>, position: Option<(u64, u32)> },
    Frames { frames: Vec<(u64, Vec<u8>)> },                    // tag 1
    Heartbeat { next_seq: u64 },                               // tag 2
    Ack { durable_seq: u64 },                                  // tag 3
    SnapshotStart { applied_seq: u64, next_job_id: u64 },      // tag 4
    SnapshotRows { table: String, rows: Vec<(u64, Vec<u8>)> }, // tag 5
    SnapshotEnd,                                               // tag 6
    /// tag 7 — the session is refused; `message` is product surface
    /// (diverged / version mismatch / not-a-primary), built by the refusing
    /// side so both ends log the same words.
    Refuse { message: String },
    CtrlPromote,                                               // tag 8
    CtrlSwitchover { to: String },                             // tag 9
    CtrlOk { message: String },                                // tag 10
    CtrlErr { message: String },                               // tag 11
    HandOff { at_seq: u64 },                                   // tag 12
    HandOffDone,                                               // tag 13
}
pub fn encode(msg: &Msg) -> Vec<u8>
pub fn decode(bytes: &[u8]) -> io::Result<Msg>   // unknown tag / trailing bytes: named errors, decode_record style (db.rs:243-263)
```

Field codecs via `charpente_core::table::Codec` (`u8`/`u32`/`u64`/`String`/`Option<T>`/`Vec<u8>` all exist; encode pair-vecs as `[n: u32][(u64, Vec<u8>)…]` by hand).
- Produces (`role.rs`):

```rust
#[derive(Debug, Clone, PartialEq)]
pub enum Role {
    Primary,
    HandingOff { to: String },
    Follower { primary: String },
    Backup { primary: String },
}
/// The durable role file (name "role" in the Disk namespace): written with
/// write_at + set_len + sync + sync_dir — a role flip must survive any crash
/// before it takes effect (spec §2's three-state switchover safety).
pub fn store_role(disk: &dyn Disk, role: &Role) -> io::Result<()>
/// Reads the role file; absent → writes `initial` and returns it (first
/// boot). Present-and-different → the durable role wins; returns
/// (role, Some(mismatch_notice)) where the notice names both roles and,
/// for the Primary-file/Follower-config case, the demote rule: demotion
/// requires a fresh data dir (re-seed) — a demoted primary's log may have
/// diverged, and re-seeding is the only safe rejoin (spec §2).
pub fn load_role(disk: &dyn Disk, initial: &Role) -> io::Result<(Role, Option<String>)>
```

File format: one line — `primary` / `handing-off:<addr>` / `follower:<addr>` / `backup:<addr>`; parse errors name the file and the expected forms.

- [ ] **Step 1: Failing roundtrip tests, then implement.** Protocol tests: every `Msg` variant roundtrips (`decode(encode(m)) == m`, including a `Frames` with a 3-frame batch and empty vec edge); unknown tag and trailing bytes err by name; a `Frames` message of one `MAX_RECORD_BYTES` payload encodes under `MAX_MESSAGE_BYTES`. Role tests: each variant roundtrips through a sim `Disk`; absent-file first boot writes-and-returns; mismatch produces the notice (pin `["role file", "config", "re-seed"]` needles for the demote case); a garbage file errs naming the expected forms. Implement; run `cargo test -p charpente repl` — Expected: PASS.

- [ ] **Step 2: Verify + commit.**

Run: `cargo test --workspace && cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check` — Expected: green.

```bash
git add crates/charpente/src
git commit -m "repl: wire protocol codec + durable role file — the message set and the three-state role truth (repl T5)"
```

---

### Task 6: The session engines — ship, ack, seed, diverge

**Files:**
- Create: `crates/charpente/src/repl/session.rs` (primary-side sessions), `crates/charpente/src/repl/follower.rs` (the client)
- Modify: `crates/charpente/src/repl/mod.rs` (the `Repl` umbrella struct)
- Modify: `DESIGN.md` §5 (mechanism → implemented, data-plane half)
- Test: Create `crates/charpente/tests/replication.rs`

**Interfaces:**
- Consumes: T1–T5 everything.
- Produces (`mod.rs`):

```rust
pub struct ReplConfig {
    /// This node's replication address. Required to be (or become) a
    /// primary; a follower without one cannot be promoted remotely.
    pub listen: Option<String>,
    pub initial_role: Role,
}
pub struct Repl { /* role, listener, sessions, follower client, pin, … */ }
impl Repl {
    /// Loads/initializes the durable role (role_disk is a second handle on
    /// the node's Disk namespace — the Db owns the first), opens the
    /// listener when configured, gates the Db per role, wires the
    /// retention pin. Logs (returns) the mismatch notice if any.
    pub fn open(net: Box<dyn Net>, role_disk: Box<dyn Disk>, cfg: ReplConfig, db: &mut Db)
        -> io::Result<(Repl, Option<String>)>;
    pub fn is_primary(&self) -> bool;      // Primary only — HandingOff is write-stopped
    pub fn role(&self) -> &Role;
    /// Non-destructive IO movement: accept pending conns, drain readable
    /// messages into internal queues, note reconnect timers. No Db access —
    /// called unconditionally each kernel iteration, like source.poll().
    pub fn pump_io(&mut self, now_nanos: u64);
    /// True when a queued message, send backlog (cursor ≤ db.last_applied()),
    /// due heartbeat, or due reconnect exists.
    pub fn has_work(&self, db: &Db, now_nanos: u64) -> bool;
    pub fn next_deadline(&self, now_nanos: u64) -> Option<u64>;
    /// One unit of work: handle one queued message, or push one Frames/
    /// Snapshot batch to one session (round-robin cursor), or heartbeat, or
    /// reconnect. May flip roles (T7) and gate/ungate the Db. Seeding a
    /// follower runs to completion inside one step (blocking on the sim/prod
    /// clock — the node serves nothing during a seed, by design).
    pub fn step(&mut self, db: &mut Db, clock: &dyn Clock) -> io::Result<()>;
    #[doc(hidden)] pub fn db_needs_reseed(&self) -> bool; // see follower seeding note below
}
```

Session bookkeeping (session.rs): `Session { conn, listen: Option<String>, cursor: u64, acked: Option<u64>, phase: Hello|Seeding{…}|Shipping }`. The retention pin: `Repl::open` does `db.pin_retention(Box::new({ let floor = Arc::clone(&self.pin_floor); move || *floor.lock().expect("uncontended") }))` where `pin_floor: Arc<Mutex<u64>>` starts at `u64::MAX` and is recomputed after every ack/session change: `min over live sessions of (acked+1, or seed-start applied_seq+1 for Seeding, or Hello-position+1 for AwaitHello… use cursor)` — sessions gone ⇒ `u64::MAX`.
- Follower seeding and the Db swap: `Repl::step` cannot replace a `Db` it only borrows. Contract: when the primary answers `SnapshotStart`, the follower-side step returns normally but latches `db_needs_reseed`; the CALLER (the kernel arm in T8; the test scheduler here) then runs `repl.run_seed(db: Db, clock: &dyn Clock) -> io::Result<Db>` — consumes the Db, `into_seeder()`s it, loops `conn.recv` (sleeping `RECONNECT_NANOS/10` on empty polls via the clock) feeding `Seeder::rows` until `SnapshotEnd`, `finish()`es, re-gates writes, and returns the new Db; the client resumes `Shipping` from `applied_seq + 1`. A conn death mid-seed restarts the seed from scratch on reconnect (seeding is idempotent — `Seeder::new` discards).
- Hello handling on the primary (session.rs), the decision ladder pinned by tests: version mismatch → `Refuse` naming both versions; follower ahead (`pos.seq >= db.last_applied()+1`… careful: ahead means `pos.seq > last_applied` or (`==` with crc mismatch)) → `Refuse` (diverged, both seqs/crcs, re-seed fix); position retained (`pos.seq >= wal base`) → verify crc via `db.read_log(pos.seq, 1)` + `frame_crc` — mismatch → `Refuse` (diverged); match → `Frames` resume from `pos.seq + 1`; position below base (or empty position with base > 0) → snapshot: mark session Seeding, stream `SnapshotStart { applied_seq, next_job_id }` from ONE `db.snapshot()` (hold it in the session phase; `Snapshot::applied_seq()` for consistency), then `SnapshotRows` batches per `table_names()` in order, `SnapshotEnd`, then Frames from `applied_seq + 1`. Empty position with base == 0 → `Frames` from 0.
- Follower acking: after `apply_shipped` returns Ok (batch synced), send `Ack { durable_seq: db.last_applied().expect("just applied") }`.

- [ ] **Step 1: The test-side scheduler + the happy path (failing).** In `tests/replication.rs`, build the harness helpers once, used by every property in T6/T7/T8:

```rust
/// One replication node driven at the Db level (no kernel until T8).
struct Node { host: SimHost, db: Option<Db>, repl: Repl, clock: SimClock }
fn make_schema() -> Schema { /* two #[table] fixtures, one with an #[index] */ }
fn spawn_node(sim: &Sim, host: &str, cfg: ReplConfig) -> Result<Node, String> { /* Db::open + Repl::open on sim.host(host) */ }
fn crash_node(node: &mut Node) / fn restart_node(sim, host, cfg) -> Node { /* host.crash_now(); host.restart(); spawn again */ }
/// The deterministic round-robin scheduler: pump_io + step every node that
/// has_work; when none does, advance the shared clock to the min
/// next_deadline (or fail on stall past a budget).
fn run_until(sim: &Sim, nodes: &mut [&mut Node], done: impl Fn(&[&mut Node]) -> bool) -> Result<(), String>
```

First failing property:

```rust
#[test]
fn follower_converges_to_the_primary_fingerprint() {
    run_universes_swarm_with!(0x2EB1_0001, 256, |_cfg| {}, |sim| {
        // primary on host "p" (listen "p:1"), follower on "f" (follow "p:1").
        // Drive N=40 committed transactions on p (interleaved with run_until
        // rounds so shipping overlaps commits). Then run_until both idle and
        // f.acked == p.last_applied. Oracle: checkpoint both, then
        // p_host.durable_fingerprint-of-store parity is NOT byte-comparable
        // across hosts (paths differ only if… they don't — same names) —
        // assert BOTH: (a) f.db.last_applied == p.db.last_applied, (b) every
        // typed scan equal, (c) sim.host("p").durable_fingerprint() ==
        // sim.host("f").durable_fingerprint() after checkpoint+graceful
        // close on both (byte-identical stores + logs — determinism's gift;
        // if redb layout divergence ever breaks (c), tighten to store-file
        // equality via scan_raw over every table and record the finding).
        // Fault handling: any io::Error routed through checks::is_fault_fallout
        // → retry via restart_node (recovery), else fail the universe.
        Ok(())
    }, require: ["fault:conn_drop", "restart"]);
}
```

(Note on (c): both nodes execute identical op sequences against redb — DESIGN §4 records redb's byte-determinism under identical op sequences. If a universe's injected faults make op sequences diverge (retries), fall back to oracle (b) for that universe and `sim.cover("repl:fingerprint-skipped")`.)

Run: `cargo test -p charpente --test replication follower_converges` — Expected: FAIL (no `Repl`).

- [ ] **Step 2: Implement `Repl` data plane** (mod.rs/session.rs/follower.rs per the Interfaces block — the recv buffer is one lazily-allocated `vec![0u8; MAX_MESSAGE_BYTES]` per conn, reused). Run the convergence property — Expected: PASS over 256 universes.

- [ ] **Step 3: Seeding, pin, and divergence properties (failing, then green).**

```rust
#[test]
fn late_follower_seeds_below_the_gcd_base_and_converges() {
    // checkpoint_every: 8, retain_segments: 1, segment_bytes: 128 → commit 60
    // txns on p so gc moves base well past 0. THEN spawn f empty. Assert the
    // session went Snapshot (sim.cover via a repl-side cover hook — add
    // sim-agnostic counters on Repl: repl.stats().seeds == 1), converges as
    // in the first property, and f's WAL base == its store applied_seq + 1
    // — the horizon+1 boundary. require: none (deterministic construction
    // inside the swarm workload guarantees the seed path each universe).
}

#[test]
fn live_lagging_session_pins_gc() {
    // Partition p↔f mid-stream (sim.partition("p", "f")) with f behind;
    // drive p past several checkpoint_every boundaries; assert p's WAL base
    // never exceeds f.acked + 1 while the session lives (list the dir).
    // Then crash f WITHOUT repair — the conn dies (partition + conn_drop or
    // crash teardown), session reaped → base advances on the next
    // checkpoint. Then restart f → it re-Hellos; if below base it re-seeds.
}

#[test]
fn diverged_follower_is_refused_by_name() {
    // Build f converged to seq N. Stop shipping (partition). Commit one MORE
    // record directly on f's Db via a test hook: f.db.allow_writes() + one
    // transact (simulating a wrongly-promoted node) — f is now AHEAD/forked.
    // Repair, reconnect: the primary must Refuse. Pin needles on the message:
    ["diverged", "seq", "crc", "re-seed", "fresh data dir"]
    // and assert f's Repl surfaces it as an io::Error from step() (the node
    // stops with the contract error; nothing was merged: p's state unchanged).
}
```

Also two deterministic error-surface pins alongside the diverged test: a `Hello { proto: PROTO_VERSION + 1, .. }` is refused naming BOTH versions (needles `["protocol version", "expected"]`), and a `Hello` sent to a node whose role is Follower is refused naming the not-a-primary state.

- [ ] **Step 4: The two data-plane meta-tests (house style — the harness catches each victim by name).**

```rust
#[test]
fn harness_catches_a_follower_that_acks_before_sync() {
    // Victim: a FollowerRepl variant (test-local copy of the apply loop)
    // that sends Ack { durable_seq } BEFORE db.apply_shipped's sync — build
    // it by acking recv-time seqs. Property: converge, then crash_now the
    // follower host and restart_node; oracle: every seq the follower ever
    // Ack'd must be ≤ its post-recovery last_applied. With crash_torn/drop
    // hot (adjust closure), the victim must be caught within 64 seeds —
    // scan seeds in a loop like crash_recovery.rs's harness_catches test.
}

#[test]
fn harness_catches_a_shipper_that_skips_crc_reverification() {
    // Victim: a read path that reads frames back with raw read_at + length
    // arithmetic, no read_frame verification (test-local fn against the
    // same Wal files via a second Disk handle on p's host). Force
    // misdirected_write hot and scoped to "wal/" (it is by default): after
    // p restarts (bit_flip/misdirected damage above horizon may cut — fine),
    // drive the victim reader over p's retained log and ship to a fresh f:
    // the oracle (f's typed scans == p's) must catch corruption the real
    // read_from would have refused by name, within 64 seeds.
}
```

- [ ] **Step 5: DESIGN §5 data-plane paragraph + verify + commit.**

Run: `cargo test -p charpente --test replication && cargo test --workspace && cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check` — Expected: green. Pin any sim-found bug's seed in the test file (hard rule 3).

```bash
git add crates/charpente/src crates/charpente/tests/replication.rs DESIGN.md
git commit -m "repl: ship/ack/seed/diverge — the data plane, proven at the Db level over the fault matrix (repl T6)"
```

---

### Task 7: The control plane — switchover and promote

**Files:**
- Modify: `crates/charpente/src/repl/mod.rs` + `session.rs` + `follower.rs` (control messages, the HandingOff state machine)
- Modify: `DESIGN.md` §5 (control-plane half: three-state switchover, failover, the demote rule)
- Test: `crates/charpente/tests/replication.rs`

**Interfaces:**
- Consumes: T5 role file + T6 sessions.
- Produces: control handling in `Repl::step`. A CLI/test connects to a node's replication listener and sends `CtrlPromote` or `CtrlSwitchover { to }`; the node replies `CtrlOk`/`CtrlErr` on the same conn. State machines:
  - **Switchover (at the primary):** on `CtrlSwitchover{to}`: no session with `listen == Some(to)` → `CtrlErr` naming the known sessions. Else: `store_role(HandingOff{to})` durably, `db.forbid_writes(to)`, start the drain: on every subsequent Ack from that session, if `acked == db.last_applied()` → send `HandOff { at_seq: acked }` and enter HandedOff (the point of no return — never aborts). Deadline `SWITCHOVER_DEADLINE_NANOS` from the Ctrl arrival: fires while still Draining → `store_role(Primary)`, `db.allow_writes()`, `CtrlErr` naming remaining lag + the deadline; fires while HandedOff → `CtrlErr` "handoff sent; this node stays write-stopped; verify <to> promoted" (role file stays HandingOff — a restart resolves against `to`, spec §2). On `HandOffDone`: `store_role(Follower{primary: to})` durably, `db.forbid_writes(to)`, drop all ship sessions, start the follower client toward `to`, `CtrlOk`.
  - **HandOff (at the follower):** verify `db.last_applied() == Some(at_seq)` (else `Refuse` — a protocol bug, named); require `cfg.listen` `Some` (else refuse: un-promotable, named — checked at Hello time too so switchover fails early); `store_role(Primary)` durably; `db.allow_writes()`; drop the client conn; begin accepting Hellos; send `HandOffDone` on the old client conn before dropping it.
  - **Promote (at a follower/backup):** on `CtrlPromote`: role must be Follower/Backup (a primary answers `CtrlErr` "already primary"); require `listen` Some; drop the client conn; `store_role(Primary)`; `db.allow_writes()`; `CtrlOk` naming `last_applied` (the operator's record of the promotion point — acked-but-unshipped records on the dead primary are lost; the message says so).
  - **Startup resolution for HandingOff:** `Repl::open` with a durable `HandingOff{to}` role keeps writes forbidden and, in `step`, connects to `to` and sends `Hello`: `to` answers as a primary (Frames/Refuse-diverged) → this node demotes (`store_role(Follower{to})`; a Refuse-diverged here is the operator's re-seed signal, surfaced as the step error); `to` connection refused / answers Refuse-not-primary → remain HandingOff write-stopped and retry (the operator resolves via promote or re-run; never silently resume Primary).

- [ ] **Step 1: Failing properties.**

```rust
#[test]
fn switchover_mid_write_load_loses_nothing_and_serves_reads_throughout() {
    run_universes_swarm_with!(0x2EB1_0007, 256, |_cfg| {}, |sim| {
        // p + f converging under load. A third conn (the "CLI") sends
        // CtrlSwitchover{to: "f:1"} mid-stream. Keep driving writes at p:
        // each transact's outcome is recorded — Ok(id) => acked set;
        // NotPrimary => routed (expected once write-stop lands);
        // fallout => classified. Read snapshots on f throughout the drain:
        // every read must succeed (snapshot() never fails on a follower).
        // After CtrlOk: f is primary (writes succeed on f), p is a follower
        // (NotPrimary naming f), and EVERY acked id exists on f. Then keep
        // writing on f and assert p converges — the old primary is a
        // follower of the new one, no divergence.
        Ok(())
    }, require: ["fault:conn_drop"]);
}

#[test]
fn failover_preserves_everything_the_follower_acked() {
    // Converge under load; crash p (host crash, no drain); CtrlPromote to f.
    // Oracle: every id acked by p whose seq ≤ f's acked-at-crash survives on
    // f; ids beyond it are the DECISIONS §6 window — assert the CtrlOk
    // message names last_applied; assert prefix-consistency: f's state
    // equals a prefix-replay of p's log (compare against a reference Db
    // replaying p's retained records up to f.last_applied — use read_log on
    // a reopened p-disk Wal via a fresh Db on a scratch host).
}

#[test]
fn crash_between_handoff_and_demote_never_yields_two_writable_primaries() {
    // Deterministic construction (no swarm): drive to the HandedOff instant
    // (test hook: Repl exposes #[doc(hidden)] phase inspection), then crash
    // p's host BEFORE HandOffDone processing. f promotes via HandOff (or
    // already did). restart_node(p) with its durable HandingOff role:
    // p must come up write-stopped, resolve against f (now primary), and
    // demote to Follower{f}. Assert at every observable point: at most one
    // node has writes_allowed().
}

#[test]
fn switchover_aborts_on_a_lagging_follower_by_name() {
    // Partition p↔f so the drain can't complete; CtrlSwitchover; advance the
    // clock past SWITCHOVER_DEADLINE_NANOS; CtrlErr must name the remaining
    // lag and the deadline; p is Primary again and writes flow.
    ["lag", "deadline", "aborted"]
}
```

- [ ] **Step 2: Implement the state machines.** (Per the Interfaces block; the Ctrl conn is just another accepted conn whose first message is a Ctrl — sessions distinguish on first message: Hello → ship session, Ctrl → control conn, anything else → `Refuse`.) Run the properties — Expected: PASS.

- [ ] **Step 3: The drain meta-test (spec §4's third victim, oracle-side).**

```rust
#[test]
fn the_zero_loss_oracle_catches_a_promote_without_drain() {
    // The "victim" is the operator move the drain exists to prevent:
    // mid-load, with f measurably behind (partition), send CtrlPromote to f
    // (not switchover). Run the SAME oracle switchover uses (every p-acked
    // id present on f). It MUST fail within 64 seeds — proving the oracle
    // detects drain-skipping, which is exactly what keeps switchover honest.
    // (Assert the failure, not green: the meta-test inverts.)
}
```

- [ ] **Step 4: DESIGN §5 control-plane half + verify + commit.**

Run: `cargo test -p charpente --test replication && cargo test --workspace && cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check` — Expected: green.

```bash
git add crates/charpente/src crates/charpente/tests/replication.rs DESIGN.md
git commit -m "repl: switchover + promote — three-state handoff, zero-loss drain, bounded-loss failover (repl T7)"
```

---

### Task 8: Kernel integration, serve/backup surfaces, the kernel-level property

**Files:**
- Modify: `crates/charpente/src/kernel/mod.rs` (`Kernel::step` extraction, `Ready::Repl`, follower gating, park tick, `with_replication`)
- Modify: `crates/charpente/src/http/app.rs` (`take_db`/`put_db` via `db: Option<Db>` — the reseed swap)
- Modify: `crates/charpente/src/serve.rs` (`serve_replicated`, `backup`, `EmptySource`)
- Modify: `docs/GUIDE.md` (new §Replication: ReplConfig, roles, follower reads/421, promote/switchover, backup; compile-gated snippets) — run `cargo build -p guide-examples`
- Modify: `DESIGN.md` §1 (the repl arm joins the ready set; the park tick), §5 (surfaces)
- Test: `crates/charpente/tests/replication.rs` (kernel-level multi-node property + above-horizon property), `crates/charpente/src/serve.rs` unit test (prod loopback smoke)

**Interfaces:**
- Consumes: everything prior.
- Produces: `Kernel::with_replication(app: App, clock: Box<dyn Clock>, repl: Repl) -> io::Result<Kernel>` (`Kernel::new` delegates with no repl — signature unchanged, GUIDE examples untouched); `pub enum Step { Worked, Idle { deadline: Option<Instant> } }` and `Kernel::step(&mut self, source: &mut dyn RequestSource) -> io::Result<Step>` — the loop body extracted verbatim: `step` returns `Worked` after servicing an arm and `Idle { deadline }` instead of parking; `run` becomes `loop { match self.step(source)? { Step::Worked => {} Step::Idle { deadline } => { if let Wake::Shutdown = source.wait_until(deadline) { return Ok(()); } } } }` (Shutdown stays `wait_until`'s to signal — step never parks). The same-shape decision code moves intact — the entropy pick, the arm bodies, the `note_enqueued` tail; `serve::serve_replicated(addr: &str, app: App, repl_disk: Box<dyn Disk>, repl_cfg: repl::ReplConfig) -> io::Result<Server>`; `serve::backup(app: App, repl_disk: Box<dyn Disk>, repl_cfg: repl::ReplConfig) -> io::Result<()>` (current thread, `EmptySource`).

- [ ] **Step 1: `Kernel::step` extraction (pure refactor, tests stay green).** Move `run`'s body (`kernel/mod.rs:108-179`) into `step` returning `Worked` after servicing an arm and `Idle { deadline }` instead of calling `wait_until` (the ready-empty branch). `run` re-wraps as above. Run: `cargo test -p charpente --test kernel --test http` — Expected: PASS unchanged (replay determinism seeds `0x7`/`0xB0B`/`0xC0FFEE` etc. must hold — the entropy sequence is untouched).

- [ ] **Step 2: The repl arm + gating (failing kernel test first).**

```rust
#[test]
fn follower_kernel_serves_reads_never_jobs_or_cron() {
    // Sim: primary App+Kernel on host p (with a job + a cron schedule),
    // follower App+Kernel on host f. Drive with the multi-node scheduler
    // (now: node.kernel.step(&mut node.source) instead of repl.step).
    // Enqueue jobs via p's routes; converge. Assert: f's KernelStats
    // {jobs_done, cron_ticks} == 0 while p's advanced; f's charpente.jobs
    // rows mirror p's (Done marks replicated); a GET on f answers 200 with
    // p's data; a POST on f answers 421 naming p's address.
}
```

Implement in `Kernel`:
- Field `repl: Option<Repl>`; `with_replication` stores it (`Kernel::new(app, clock)` → `repl: None`).
- At the top of `step`, after `source.poll()`: `if let Some(repl) = &mut self.repl { repl.pump_io(now); }`.
- Ready set: jobs/cron arms gated — `let primary = self.repl.as_ref().is_none_or(|r| r.is_primary()); if primary && self.dispatcher.has_due(now) { … } if primary && self.cron.has_due(now) { … }`; new `if self.repl.as_ref().is_some_and(|r| r.has_work(self.app.db(), now)) { ready.push(Ready::Repl); }` — `Ready` gains `Repl`.
- The `Ready::Repl` arm: `let repl = self.repl.as_mut().expect("arm only pushed when Some");` then `repl.step(self.app.db_mut(), &*self.clock)?;` and if `repl.db_needs_reseed()` run the reseed swap: `let db = self.app.take_db(); let db = repl.run_seed(db, &*self.clock)?; self.app.put_db(db);` (App: `db: Option<Db>` internally; `db()`/`db_mut()` unwrap with `expect("db taken for reseed — a kernel bug left it out")`; `take_db`/`put_db` `pub(crate)` + `#[doc(hidden)]` test variants, following `app.rs:189-210`'s existing pattern). `run_seed(db: Db, clock) -> io::Result<Db>` consumes and returns (T6's `run_seed` signature adjusted to by-value here — reconcile in T6 if built the other way).
- Deadline: in the ready-empty branch, `let mut deadline = [dispatcher…, cron…, repl.and_then(next_deadline)].min…;` and the park tick: `if self.repl.is_some() { let tick = Instant::from_nanos(now + REPL_TICK_NANOS); deadline = Some(deadline.map_or(tick, |d| d.min(tick))); }` — repl-configured nodes never park past the tick (in-flight sim messages and prod TCP both get polled within one tick; DESIGN §1 note lands here).
- Poison: the repl arm's `Err` bubbles like the job arm's (restart-and-recover); a `Refuse`-diverged error therefore stops the kernel with the named message ✓.

Run: `cargo test -p charpente --test kernel --test replication follower_kernel` — Expected: PASS.

- [ ] **Step 3: The above-horizon promise property (spec §4 property 8).**

```rust
#[test]
fn wal_corruption_on_the_primary_no_longer_loses_acked_records_with_a_follower() {
    run_universes_swarm_with!(0x2EB1_000B, 256, |cfg| { cfg.bit_flip = 0.02; cfg.misdirected_write = 0.005; }, |sim| {
        // Kernel-level p+f under write load, converge continuously. Crash+
        // restart p repeatedly (host restart fires bit_flip on p's wal/).
        // p's recovery may: (a) hard-error below horizon (named — restore
        // path: this universe then REBUILDS p by re-seeding from f after
        // promoting f, and asserts every acked id survived on f), or
        // (b) cut above horizon — previously silent loss; NOW assert every
        // acked id ≤ f's acked position still exists on f. The single-node
        // durable_corruption_fired excuse is NOT used when a converged
        // follower exists — that's the promise kept. Consistency oracles
        // (job exactly-once from the kernel suite's checker) still hold.
        Ok(())
    }, require: ["fault:bit_flip"]);
}
```

Same commit: `checks.rs`'s `durable_corruption_fired` doc comment gains the replication-era sentence (single-node workloads keep the excuse; a workload with a converged follower asserts the stronger oracle instead — spec §4 property 8), keeping the checker docs true in the commit that changes the truth.

Also the backup property (spec §4 property 7), same file:

```rust
#[test]
fn backup_node_dir_opens_and_promotes_fingerprint_identical() {
    // Kernel-level: primary p + a Backup-role node b (EmptySource-style —
    // in-sim, a Kernel with an empty SimSource script; requests never
    // arrive). Converge under load; assert b's KernelStats.requests == 0
    // and jobs/cron never ran. Then: graceful close (drop b's kernel),
    // Db::open b's dir directly — it opens clean; CtrlPromote a restarted
    // b — it accepts writes from its acked position; typed scans equal p's
    // at that seq. The restore path the compacted-log error text promises.
}
```

- [ ] **Step 4: serve/backup surfaces + prod smoke.** `serve.rs`: `serve_replicated` mirrors `serve` (`serve.rs:33-109`) but the kernel thread builds `Repl::open(Box::new(crate::prod::ProdNet), repl_disk, repl_cfg, app.db_mut())` (eprintln the mismatch notice) and `Kernel::with_replication`; `backup(app, repl_disk, cfg)` runs `Kernel::with_replication(...).run(&mut EmptySource { clock })` on the CURRENT thread (`EmptySource`: `poll` → `None`; `wait_until(deadline)` → sleep to deadline via `ProdClock`, return `Wake::Deadline`; deadline is always Some thanks to the repl tick). Unit smoke test beside `serve_answers_http1_over_loopback` (`serve.rs:224-270`): a primary `serve_replicated` on `127.0.0.1:0` + a follower `serve_replicated` in follower role on a second temp root; POST a row to the primary over real HTTP; poll-loop a GET against the FOLLOWER until it answers 200 with the row (bounded 5s like `poll_until`); assert a POST to the follower yields 421.

- [ ] **Step 5: GUIDE §Replication + DESIGN + verify + commit.** GUIDE: config/roles/reads/verbs/backup with compile-gated snippets; `cargo build -p guide-examples` green.

Run: `cargo test --workspace && cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check` — Expected: green.

```bash
git add crates/charpente/src docs/GUIDE.md DESIGN.md crates/charpente/tests
git commit -m "kernel+serve: the replication arm — follower mode, park tick, backup node, above-horizon promise kept (repl T8)"
```

---

### Task 9: CLI verbs — `promote` and `switchover`

**Files:**
- Modify: `crates/charpente-cli/Cargo.toml` (add `charpente = { path = "../charpente" }`)
- Create: `crates/charpente-cli/src/ctl.rs`
- Modify: `crates/charpente-cli/src/main.rs:6-19` (dispatch + USAGE)
- Test: unit tests in `ctl.rs` (arg parsing, exit-code mapping) + one loopback integration test (spawn a sim?-no: a real `serve_replicated` node is heavyweight here — drive `ctl::run` against a scripted `ProdNet` listener thread that answers CtrlOk/CtrlErr)

**Interfaces:**
- Consumes: `charpente::repl::protocol::{Msg, encode, decode}`, `charpente::prod::{ProdNet, ProdClock}`.
- Produces: `charpente promote --node <addr>` and `charpente switchover --primary <addr> --to <addr>`; exit 0 on CtrlOk (message printed), 1 on CtrlErr/Refuse (message printed to stderr — the node's words are the product surface, forwarded verbatim), 2 on usage/connect/timeout (30s, `ProdClock`-slept poll loop — no ambient sleep; the deny-list stays intact because `ProdClock` is the sanctioned clock).

- [ ] **Step 1: Failing unit tests** (arg parse: missing `--to` → usage string naming both verbs; exit mapping table). **Step 2: implement** `ctl::run(args: &[String]) -> i32`: parse, `ProdNet.connect`, `send(encode(&msg))`, poll `recv` with `ProdClock::sleep(Duration::from_millis(10))` until reply or deadline, map. `main.rs` dispatch gains `(Some("promote"), _)` / `(Some("switchover"), _)` arms; USAGE updated. **Step 3: loopback test**: a thread with `ProdNet.listen("127.0.0.1:0")` answering `CtrlOk { message: "promoted at seq 41" }`; assert exit 0 and stdout contains the message; a CtrlErr answerer → exit 1.

Run: `cargo test -p charpente-cli && cargo test --workspace && cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check` — Expected: green.

```bash
git add crates/charpente-cli
git commit -m "cli: promote + switchover verbs — control messages over the replication port (repl T9)"
```

---

### Task 10: Evidence sweep + close-out

CONTROLLER TASK.

- [ ] Fresh swarm batches: `cargo test -p charpente --test replication` twice, `cargo test -p charpente --test transactions --test typed_tables --test kernel --test http` twice, `cargo test -p charpente-sim` twice — Expected: green every time (fresh master seeds per run).
- [ ] Pinned-seed spot replay: `CHARPENTE_SEED=0x8bee1752811ae873 cargo test -p charpente-sim --test crash_recovery` — Expected: green (frozen path byte-identical). Re-run one pre-existing kernel pinned seed the same way.
- [ ] Demo app regression: `cargo run -p charpente-cli -- test --sim --dir examples/invoicing --universes 1024 --jobs 8` — Expected: exit 0; record the master seed.
- [ ] The recorded end-to-end demo (spec §7): a small script-of-record in the task report — two `serve_replicated` nodes on loopback, write load via curl loop, `kill -9` the primary, `charpente promote --node <f>`, verify every acked write via GET against f, restart the old primary pointing at f and show the startup demote/re-seed refusal text. Paste the transcript into the ledger; this is PLAN M1 exit-criterion evidence.
- [ ] `cargo test --workspace && cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check` — Expected: clean.
- [ ] CLAUDE.md current-state: add the sub-project-2 entry (what shipped, the property matrix, evidence seeds, any Noah rulings landed mid-build) and move "Next:" to M1 sub-project 3 (realtime SSE/WS with exact resume). Commit: `docs: M1 sub-project 2 done — replication (evidence in entry)`.
- [ ] Ledger: per-task completion lines + evidence seeds in `.superpowers/sdd/progress.md`.
