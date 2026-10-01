//! Reusable oracles for sim workloads (step-7 spec §Platform-checker
//! library). v0 shipped exactly three; `durable_corruption_fired` earned
//! its way in when the durable-corruption fault axes landed (segment-mgmt
//! T5), and `boot_through_fallout` when `Db::open` stopped absorbing an
//! injected fault inside the table store's open (follow-up 21) — more earn
//! their way in the same way.

use crate::Sim;

/// Why a universe's error text is (or is not) excusable fault fallout — the
/// typed successor to substring-matching at call sites (spec §3.2). The
/// prose stays product surface (DESIGN §7); classification keys on the
/// variant. Centralizing does not eliminate prose-coupling — a reworded
/// exemplar is still one `classify` body away from silently drifting to
/// `Unrelated` — but it makes that coupling a SINGLE point instead of four
/// (this file plus three test-local `is_injected` copies that had
/// hand-rolled the same substring list), so a reword failure surfaces here,
/// by name, instead of leaking into whichever workload's oracle saw the text
/// first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultReason {
    /// The sim's own injected-fault wordings (write/sync/read, and the
    /// crash-disk-is-gone text). `misdirected_write` is deliberately absent:
    /// it returns `Ok` and has no wording at all — see `classify`.
    Injected,
    /// Our WAL/engine poison wording.
    Poisoned,
    /// redb's latch ("close and re-open the database").
    RedbLatch,
    /// The WAL's durable-horizon hard error (corruption of acked records —
    /// loud by design, not a bug).
    DurableHorizon,
    /// Everything else: NOT fallout; fails the universe for adjudication.
    Unrelated,
}

/// Classifies `text` (an error's Display/to_string output) into one of the
/// four recognized fault-fallout families or `Unrelated`:
///  1. the sim's own injected-fault wordings — including the `read_error`
///     axis (segment-mgmt T5), whose error text is "injected" too.
///     `misdirected_write` is NOT in this family and cannot be: it returns
///     `Ok` with the payload landed past the requested offset, so it produces
///     no error text of its own (T7 review, corrected at T14 — the earlier
///     doc listed it here). What surfaces later is the corrupt frame it
///     leaves behind: family 4 below when the frame is provably durable, and
///     an ordinary torn-tail cut otherwise,
///  2. our WAL/engine poison wording,
///  3. redb's latch ("close and re-open the database") — its internal
///     already-saw-an-IO-error signal,
///  4. the WAL's durable-horizon hard error (segment-mgmt T4's recovery-
///     horizon rule): `bit_flip`/`misdirected_write` corrupting an
///     already-acknowledged frame is a legitimate consequence of the fault
///     matrix, surfaced loudly by design — not a bug.
///
/// Anything else is `Unrelated`: NOT fallout, must fail the universe for
/// adjudication. Recognizing these cannot mask a real bug so long as the
/// workload's end-state oracle (e.g. exactly-once on reopen) still runs
/// either way.
///
/// First-match-wins, and the order is load-bearing: transactions.rs's and
/// http.rs's narrower `matches!(classify(t), Injected | DurableHorizon)`
/// wrappers equal their old 3-needle `contains` ORs only because `Injected`
/// is tested before `Poisoned`/`RedbLatch` — text carrying both an injected
/// wording and a poison wording must land `Injected`, not `Poisoned`, or
/// those two call sites would silently start rejecting text they used to
/// accept. Reordering the arms below is exactly the class of drift this
/// classifier exists to prevent; see the precedence pin in
/// `classify_pins_each_family_to_its_exemplar`.
pub fn classify(text: &str) -> FaultReason {
    if text.contains("simulated") || text.contains("injected") {
        FaultReason::Injected
    } else if text.contains("poisoned") {
        FaultReason::Poisoned
    } else if text.contains("close and re-open the database") {
        FaultReason::RedbLatch
    } else if text.contains("durable horizon") {
        FaultReason::DurableHorizon
    } else {
        FaultReason::Unrelated
    }
}

/// `true` for every recognized family — see `classify`'s doc for the four.
pub fn is_fault_fallout(text: &str) -> bool {
    !matches!(classify(text), FaultReason::Unrelated)
}

/// True when a durable-corruption fault (`bit_flip` or `misdirected_write`)
/// actually fired in this universe. The oracle companion to those two axes:
/// an acked record they corrupted ABOVE the store's checkpoint horizon is
/// silently cut at recovery — the standard WAL convention (an invalid frame
/// is a torn tail unless provably below the horizon), and bitrot of the
/// only local copy is unrecoverable by design; v0's answer is M1
/// replication. So an acked-durability check routes its shortfall through
/// this: loss is excused ONLY in universes where corruption was actually
/// injected, and the strong oracle stands in every other universe. This is
/// the SINGLE-NODE excuse: a workload with a converged follower keeps none
/// of it — a record the primary lost to media corruption still lives on the
/// follower, so such a workload asserts the stronger oracle (every acked id
/// survives on the follower) instead of excusing the loss (spec §4 property
/// 8, the promise M1 replication makes good). Never use it to excuse wrong
/// DATA (corruption must surface as loss or a named error, not as different
/// bytes) or consistency violations (e.g. exactly-once) — a cut reverts
/// state to a prefix, which stays consistent.
pub fn durable_corruption_fired(sim: &Sim) -> bool {
    let coverage = sim.coverage();
    coverage.get("fault:bit_flip").copied().unwrap_or(0) > 0
        || coverage
            .get("fault:misdirected_write")
            .copied()
            .unwrap_or(0)
            > 0
}

/// True when the `dir_entry_loss` axis actually fired in this universe — a
/// pending directory create vanished, or a pending removal resurrected
/// (`FaultConfig.dir_entry_loss`, coverage key `fault:dir-entry`). The
/// fire-time-marked companion to that axis, the same discipline as
/// `durable_corruption_fired`: a workload that routes a recovery gap
/// (`Db::open`'s "a segment is missing … refusing to guess") to a restore
/// path must do so ONLY when entry loss could have gapped the directory. With
/// the recovery-cut ordering fixed (log.rs), the platform never itself leaves
/// a durable gap, so a gap without this axis firing is a genuine platform
/// regression that must fail loud — not be masked as a routine restore.
pub fn dir_entry_loss_fired(sim: &Sim) -> bool {
    sim.coverage().get("fault:dir-entry").copied().unwrap_or(0) > 0
}

/// Determinism oracle: runs `workload` twice, each on its own fresh
/// `Sim::new(seed)` world, and asserts both the returned value and the
/// durable end-state (`Sim::durable_fingerprint`) match. Always constructs
/// its worlds via plain `Sim::new` — never swarm `FaultConfig` derivation —
/// so a caller replaying a swarm universe's exact fault schedule must
/// pre-wrap `workload` to reconstruct that `Sim` itself; this checker has no
/// way to recover a swarm config from a bare seed.
pub fn replays_identically<T: PartialEq + std::fmt::Debug>(
    seed: u64,
    workload: impl Fn(&mut Sim) -> Result<T, String>,
) -> Result<(), String> {
    let mut first = Sim::new(seed);
    let r1 = workload(&mut first)
        .map_err(|e| format!("replays_identically: first run failed under seed {seed:#x}: {e}"))?;
    let fp1 = first.durable_fingerprint();

    let mut second = Sim::new(seed);
    let r2 = workload(&mut second)
        .map_err(|e| format!("replays_identically: second run failed under seed {seed:#x}: {e}"))?;
    let fp2 = second.durable_fingerprint();

    if r1 != r2 {
        return Err(format!(
            "replays_identically violated under seed {seed:#x}: results diverged — {r1:?} vs {r2:?}"
        ));
    }
    if fp1 != fp2 {
        return Err(format!(
            "replays_identically violated under seed {seed:#x}: durable fingerprints diverged — {fp1:#x} vs {fp2:#x} (same result, different durable state)"
        ));
    }
    Ok(())
}

/// Ack-durability oracle: runs `rounds` rounds of drive-then-restart-then-
/// verify on THE CALLER'S `Sim`. Each round, `drive` performs one round of
/// app work and returns the ack ids it answered success for — `drive`
/// classifies its own faults via `is_fault_fallout` so an id only appears
/// here if the app actually promised durability for it. `sim.restart()` then
/// collapses the disk as usual, and `verify` checks that every id acked so
/// far (not just this round's) is still present. A `verify` failure fails
/// the whole check, prefixed with the round and seed so a repro can target
/// the exact round.
///
/// **Takes `&mut Sim`, not a seed, and the distinction is load-bearing** —
/// unlike [`replays_identically`] above, which takes a seed because it needs
/// two worlds identical AT TIME ZERO and a used sim cannot provide that. The
/// rule: a checker that needs a FRESH world takes a seed; one that continues
/// THIS world takes `&mut Sim`. This one continues yours, for two reasons that
/// were defects until 2026-09-05:
///
///  - **Coverage.** `Sim::restart` credits the `restart` key on the sim it is
///    called on, and the harness reports the keys of the sim it handed the
///    workload. A checker restarting a private world credits nothing the
///    harness can see, so `require: ["restart"]` over it could never fire —
///    and `durable_corruption_fired`/`dir_entry_loss_fired`, which gate
///    whether a data-loss shortfall may be EXCUSED, would answer from the
///    wrong world entirely.
///  - **Faults.** `Sim::new` is `FaultConfig::default()`, which zeroes eleven
///    axes a swarm universe may have set. Building a world here silently ran
///    the property outside the matrix its universe declared while reading as
///    though it were inside it.
///
/// Neither is recoverable from a bare seed: `Sim::with_faults` derives its RNG
/// streams at construction with no rewind, so a seed cannot reconstruct a
/// caller's fault SCHEDULE even given its config.
pub fn acked_implies_durable(
    sim: &mut Sim,
    rounds: u32,
    mut drive: impl FnMut(&mut Sim) -> Result<Vec<String>, String>,
    mut verify: impl FnMut(&mut Sim, &[String]) -> Result<(), String>,
) -> Result<(), String> {
    // Named for the error text below, which stays seed-prefixed so a repro line
    // is copy-pasteable — and now names the seed the caller is actually running.
    let seed = sim.seed();
    let mut acked: Vec<String> = Vec::new();
    for round in 1..=rounds {
        let mut new_acks = drive(sim).map_err(|e| {
            format!(
                "acked-implies-durable: drive failed in round {round} under seed {seed:#x}: {e}"
            )
        })?;
        acked.append(&mut new_acks);
        sim.restart();
        verify(sim, &acked).map_err(|e| {
            format!("acked-implies-durable violated after round {round} under seed {seed:#x}: {e}")
        })?;
    }
    Ok(())
}

/// Boots the way a supervised process boots after a failed boot: restart and
/// retry, which is `Db::open`'s own contract for an IO failure (DESIGN §4).
/// Calls `boot`; when it fails with an error `is_fallout` accepts (injected
/// fault fallout, by the caller's own classifier), calls it again, up to
/// `tries` boots in all. Each retry marks `boot:retried`, and a retry that
/// boots marks `boot:booted-after-retry`, so a batch can require that retries
/// do more than run. A retry after an injected IO error restarts the process
/// first (`Sim::restart_process`): the failed boot's handles die, the page
/// cache survives, and an armed crash countdown keeps counting. A retry after
/// a crash power-cycles first (`Sim::restart`), since the disk is gone until
/// then. Power-cycling after every failure instead would turn each injected
/// IO error into a crash, collapsing unsynced writes and rolling the
/// restart-time bit flip, which no restarted process does.
///
/// - `Ok(Some(t))`: the first boot that succeeded.
/// - `Ok(None)`, one of two ways, and the sim is left as the last failed boot
///   left it:
///   - every boot met fallout (marked `boot:exhausted`). What happens next is
///     the caller's: a workload whose only node never came up gives up by
///     name (`Sim::give_up`); a round or epoch of a multi-round workload
///     restarts and moves on, because a later phase still has to adjudicate
///     or give up.
///   - a boot was refused for durable corruption at or below the store's
///     horizon (`FaultReason::DurableHorizon`). Those bytes are on the disk,
///     so no restart clears them: the helper stops at once, marks
///     `boot:durable-refusal` and gives the universe up under that name. The
///     give-up is the first, so a caller's own later one cannot replace it.
/// - `Err(e)`: the first error `is_fallout` rejects, unchanged and at once. A
///   non-injected error is a finding, never a reason to boot again.
///
/// One injected fault used to cost the phase it hit: a workload that booted
/// once gave up on fallout, and a round or an epoch skipped itself. That was
/// cheap only while `Db::open` absorbed a fault inside the table store's open
/// by discarding the store and rebuilding it from the log in the same call.
/// It no longer does (it returns the error, and the next boot retries the same
/// file), so a boot site that does not retry loses the universes, and the
/// rounds, that rebuild used to hand it for free. That includes a round loop:
/// its next round boots again, but the round a failed boot skips is work the
/// property never checks. Aiming one is a measured trade. Unaimed, S5 cost
/// group_commit 8.9% of its verified checks and tenant 11% of its
/// multi-tenant writes over 10 fresh bases (2026-10-01, review I1); aimed, both
/// checked more than before S5.
pub fn boot_through_fallout<T, E: std::fmt::Display>(
    sim: &Sim,
    tries: u32,
    is_fallout: impl Fn(&str) -> bool,
    mut boot: impl FnMut() -> Result<T, E>,
) -> Result<Option<T>, E> {
    assert!(tries > 0, "boot_through_fallout needs at least one boot");
    for attempt in 0..tries {
        if attempt > 0 {
            sim.cover("boot:retried");
            if sim.disk_crashed() {
                sim.restart();
            } else {
                sim.restart_process();
            }
        }
        match boot() {
            Ok(t) => {
                if attempt > 0 {
                    sim.cover("boot:booted-after-retry");
                }
                return Ok(Some(t));
            }
            Err(e) => {
                let text = e.to_string();
                if !is_fallout(&text) {
                    return Err(e);
                }
                if matches!(classify(&text), FaultReason::DurableHorizon) {
                    sim.cover("boot:durable-refusal");
                    sim.give_up(format!(
                        "the boot was refused for durable corruption at or below the store's \
                         horizon, which no restart clears, so nothing past it was adjudicated: \
                         {text}"
                    ));
                    return Ok(None);
                }
            }
        }
    }
    sim.cover("boot:exhausted");
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use charpente_core::io::{Disk, Entropy};
    use std::sync::atomic::{AtomicU64, Ordering};

    #[test]
    fn recognizes_all_recognized_families_and_nothing_else() {
        for fallout in [
            "simulated disk write error (injected fault)",
            "simulated fsync failure — durability of writes since the last successful sync is unknown",
            "WAL poisoned by an earlier IO failure",
            "engine poisoned: a durable record failed to apply",
            "Storage error: close and re-open the database",
            "injected read error",
            "log frame 3 is invalid but at-or-below the store's durable horizon 7 — this is \
             corruption of acknowledged records, not a torn tail, and truncating it would \
             silently lose data; restore this node from a replica or backup (DESIGN §2)",
        ] {
            assert!(is_fault_fallout(fallout), "must classify: {fallout}");
        }
        for real_bug in [
            "row decode: needed 8 bytes for u64, 3 left — schema drift or corruption",
            "assertion failed: durable.is_some_and(|d| d >= applied)",
            "exactly-once violated: hits 3, done 2 for key 7",
        ] {
            assert!(!is_fault_fallout(real_bug), "must NOT classify: {real_bug}");
        }
    }

    // ---------------------------------------------------------- classify

    #[test]
    fn classify_pins_each_family_to_its_exemplar() {
        use FaultReason::*;

        for injected in [
            "simulated disk write error (injected fault)",
            "simulated fsync failure — durability of writes since the last successful sync is unknown",
            "injected read error",
        ] {
            assert_eq!(classify(injected), Injected, "{injected}");
        }
        for poisoned in [
            "WAL poisoned by an earlier IO failure",
            "engine poisoned: a durable record failed to apply",
        ] {
            assert_eq!(classify(poisoned), Poisoned, "{poisoned}");
        }
        assert_eq!(
            classify("Storage error: close and re-open the database"),
            RedbLatch
        );

        // Precedence pin (review I1): transactions.rs's/http.rs's narrower
        // `matches!(classify(t), Injected | DurableHorizon)` wrappers equal
        // their old 3-needle OR only because `Injected` is tested before
        // `Poisoned`/`RedbLatch` above. A text carrying both an injected AND
        // a poisoned wording must land `Injected` — is_fault_fallout's own
        // tests are order-invariant (a union can't see arm order) and every
        // other exemplar here is single-family, so nothing else in the
        // workspace would catch an arm reorder that demoted `Injected`.
        assert_eq!(
            classify(
                "simulated disk write error (injected fault) — WAL poisoned by an earlier IO failure"
            ),
            Injected
        );

        assert_eq!(
            classify(
                "log frame 3 is invalid but at-or-below the store's durable horizon 7 — this is \
                 corruption of acknowledged records, not a torn tail, and truncating it would \
                 silently lose data; restore this node from a replica or backup (DESIGN §2)"
            ),
            DurableHorizon
        );
        for real_bug in [
            "row decode: needed 8 bytes for u64, 3 left — schema drift or corruption",
            "assertion failed: durable.is_some_and(|d| d >= applied)",
            "exactly-once violated: hits 3, done 2 for key 7",
        ] {
            assert_eq!(classify(real_bug), Unrelated, "{real_bug}");
        }

        // db.rs's compacted-log rebuild refusal (Db::open) shares NO needle
        // with any family today — it is Unrelated here, and the replication
        // workload's own arm (tests/replication.rs, matching on "cannot be
        // rebuilt from the log alone") is its handler, not this classifier.
        // Pinned so a future needle addition can't silently absorb it here.
        assert_eq!(
            classify(
                "table store cannot open (...) and the log is compacted to seq 3 — the records \
                 below the store's last checkpoint no longer exist locally, so the store cannot \
                 be rebuilt from the log alone; restore this node from a replica or backup, or \
                 widen DbConfig::retain_segments (DESIGN §4)"
            ),
            Unrelated
        );

        // The reword discriminator: a deliberate rewording of the
        // durable-horizon exemplar sharing NO needle with any recognized
        // family (contrast "WAL was poisoned earlier", which still contains
        // "poisoned" and would wrongly classify Poisoned). Proves a
        // product-text reword now fails HERE, loudly and by name — the one
        // prose-coupling point — instead of silently reclassifying inside
        // whichever workload's oracle happened to see the text first.
        assert_eq!(
            classify("the write-ahead log was corrupted by an earlier fault"),
            Unrelated
        );
    }

    // ------------------------------------------- durable_corruption_fired

    #[test]
    fn durable_corruption_fired_tracks_the_two_corruption_axes_only() {
        use crate::FaultConfig;

        // Quiet world: nothing fired.
        let sim = Sim::with_faults(0xD0C_0001, FaultConfig::none());
        assert!(!durable_corruption_fired(&sim));

        // A read error is NOT durable corruption (it produces errors, never
        // silent wrongness).
        let sim = Sim::with_faults(
            0xD0C_0002,
            FaultConfig {
                read_error: 1.0,
                ..FaultConfig::none()
            },
        );
        let file = sim.disk().open("wal/f").unwrap();
        file.write_at(0, b"x").unwrap();
        file.sync().unwrap();
        let mut out = [0u8; 1];
        let _ = file.read_at(0, &mut out);
        assert!(!durable_corruption_fired(&sim));

        // A misdirected write in scope IS.
        let sim = Sim::with_faults(
            0xD0C_0003,
            FaultConfig {
                misdirected_write: 1.0,
                ..FaultConfig::none()
            },
        );
        let file = sim.disk().open("wal/f").unwrap();
        file.write_at(0, b"x").unwrap();
        assert!(durable_corruption_fired(&sim));

        // A bit flip at restart IS.
        let sim = Sim::with_faults(
            0xD0C_0004,
            FaultConfig {
                bit_flip: 1.0,
                ..FaultConfig::none()
            },
        );
        let file = sim.disk().open("wal/f").unwrap();
        file.write_at(0, b"x").unwrap();
        file.sync().unwrap();
        sim.restart();
        assert!(durable_corruption_fired(&sim));
    }

    // ------------------------------------------------ dir_entry_loss_fired

    #[test]
    fn dir_entry_loss_fired_tracks_the_dir_entry_axis() {
        use crate::FaultConfig;

        // Quiet world: nothing fired.
        let sim = Sim::with_faults(0xD1E_0001, FaultConfig::none());
        assert!(!dir_entry_loss_fired(&sim));

        // A crash with the axis off, even with a pending removal, never fires
        // it (zero-draw fast path — the frozen replay contract).
        let sim = Sim::with_faults(0xD1E_0002, FaultConfig::none());
        let file = sim.disk().open("wal/f").unwrap();
        file.write_at(0, b"x").unwrap();
        file.sync().unwrap();
        sim.disk().sync_dir("wal/").unwrap();
        sim.disk().remove("wal/f").unwrap();
        sim.restart();
        assert!(!dir_entry_loss_fired(&sim));

        // The axis forced hot: an un-barriered removal resurrects at the
        // crash and marks the coverage key.
        let sim = Sim::with_faults(
            0xD1E_0003,
            FaultConfig {
                dir_entry_loss: 1.0,
                ..FaultConfig::none()
            },
        );
        let file = sim.disk().open("wal/f").unwrap();
        file.write_at(0, b"x").unwrap();
        file.sync().unwrap();
        sim.disk().sync_dir("wal/").unwrap();
        sim.disk().remove("wal/f").unwrap();
        sim.restart();
        assert!(dir_entry_loss_fired(&sim));
    }

    // ---------------------------------------------------- replays_identically

    #[test]
    fn replays_identically_catches_a_workload_that_reads_ambient_state() {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let seed = 1;
        let err = replays_identically(seed, |_: &mut Sim| {
            Ok::<u64, String>(COUNTER.fetch_add(1, Ordering::SeqCst))
        })
        .unwrap_err();
        assert!(err.contains("results diverged"), "{err}");
        assert!(err.contains(&format!("{seed:#x}")), "{err}");
    }

    #[test]
    fn replays_identically_wraps_a_first_run_error() {
        let err = replays_identically(7, |_: &mut Sim| Err::<u8, String>("boom".to_string()))
            .unwrap_err();
        assert!(err.contains("first run failed under seed"), "{err}");
        assert!(err.contains("boom"), "{err}");
    }

    #[test]
    fn replays_identically_passes_an_honest_workload() {
        replays_identically(42, |sim: &mut Sim| {
            let mut byte = [0u8; 1];
            sim.entropy().fill(&mut byte);
            Ok::<u8, String>(byte[0])
        })
        .unwrap();
    }

    // ------------------------------------------------- acked_implies_durable

    /// A toy app: each round writes one byte at an advancing offset in a sim
    /// file, optionally syncing before acking it. Faults from `open`/
    /// `write_at`/`sync` are classified via `is_fault_fallout` and turned
    /// into "no ack this round" rather than a hard failure — a real app's
    /// `drive` is expected to do the same (per the doc comment above).
    fn toy_drive(sync_before_ack: bool) -> impl FnMut(&mut Sim) -> Result<Vec<String>, String> {
        let mut round: u64 = 0;
        move |sim: &mut Sim| {
            let file = match sim.disk().open("toy") {
                Ok(f) => f,
                Err(e) if is_fault_fallout(&e.to_string()) => return Ok(vec![]),
                Err(e) => return Err(e.to_string()),
            };
            if let Err(e) = file.write_at(round, &[0xAA]) {
                let msg = e.to_string();
                return if is_fault_fallout(&msg) {
                    Ok(vec![])
                } else {
                    Err(msg)
                };
            }
            if sync_before_ack && let Err(e) = file.sync() {
                let msg = e.to_string();
                return if is_fault_fallout(&msg) {
                    Ok(vec![])
                } else {
                    Err(msg)
                };
            }
            let id = format!("byte-{round}");
            round += 1;
            Ok(vec![id])
        }
    }

    fn toy_verify(sim: &mut Sim, acked: &[String]) -> Result<(), String> {
        let file = sim.disk().open("toy").map_err(|e| e.to_string())?;
        let len = file.len().map_err(|e| e.to_string())?;
        for id in acked {
            let offset: u64 = id
                .strip_prefix("byte-")
                .expect("toy ack ids are \"byte-N\"")
                .parse()
                .expect("toy ack ids carry a numeric offset");
            if offset >= len {
                return Err(format!("{id} missing after restart (file len {len})"));
            }
            let mut byte = [0u8];
            file.read_at(offset, &mut byte).map_err(|e| e.to_string())?;
            if byte[0] != 0xAA {
                return Err(format!("{id} corrupted after restart"));
            }
        }
        Ok(())
    }

    #[test]
    fn acked_implies_durable_wraps_a_drive_error() {
        let mut sim = Sim::new(9);
        let err = acked_implies_durable(
            &mut sim,
            3,
            |_sim: &mut Sim| Err::<Vec<String>, String>("boom".to_string()),
            toy_verify,
        )
        .unwrap_err();
        assert!(err.contains("drive failed in round 1"), "{err}");
        assert!(err.contains("boom"), "{err}");
    }

    #[test]
    fn acked_implies_durable_passes_when_synced_before_ack() {
        let mut sim = Sim::new(0xACED);
        acked_implies_durable(&mut sim, 5, toy_drive(true), toy_verify).unwrap();
    }

    /// The checker must restart the CALLER'S world, because that is the world
    /// the harness reads coverage from — `run_universe` hands the workload a
    /// `Sim` and reports that sim's keys. A checker that restarts a private
    /// world of its own credits `restart` where nothing can see it, so a
    /// workload writing `require: ["restart"]` over this checker gets a key
    /// that cannot fire: green, permanently, for a reason no reader can find.
    /// Exact count, not merely non-zero, so shortening the loop is also red.
    #[test]
    fn acked_implies_durable_credits_restart_on_the_callers_sim() {
        use crate::FaultConfig;
        let mut sim = Sim::with_faults(0x0BAD_C0DE, FaultConfig::none());
        acked_implies_durable(&mut sim, 3, toy_drive(true), toy_verify).unwrap();
        assert_eq!(
            sim.coverage().get("restart").copied().unwrap_or(0),
            3,
            "one `restart` credit per round, on the sim the caller passed in"
        );
    }

    /// And it must run under the caller's FAULT CONFIG. `Sim::new` is
    /// `FaultConfig::default()`, which zeroes eleven axes the swarm sets —
    /// `bit_flip`, `misdirected_write`, `dir_entry_loss`, `read_error`,
    /// `conn_drop`, `net_spike`, `subscriber_stall` and the four `object_*` —
    /// so a checker that built its own world would silently run a property
    /// OUTSIDE the matrix its universe declared, while reading as though it
    /// were inside it. Here `read_error: 1.0` must reach `toy_verify`'s read
    /// and come back named: `len()` does not roll the axis, `read_at` does.
    #[test]
    fn acked_implies_durable_runs_under_the_callers_fault_config() {
        use crate::FaultConfig;
        let mut sim = Sim::with_faults(
            0x0BAD_F00D,
            FaultConfig {
                read_error: 1.0,
                ..FaultConfig::none()
            },
        );
        let err = acked_implies_durable(&mut sim, 1, toy_drive(true), toy_verify).unwrap_err();
        assert!(err.contains("injected read error"), "{err}");
        assert!(err.contains("violated after round 1"), "{err}");
    }

    /// The outcomes, each with what it leaves on the universe: a boot that
    /// clears after two injected IO errors returns it, after two process
    /// restarts and no power cycle; one whose disk crashed is power-cycled
    /// before the next boot; one that never clears returns `None`, marked and
    /// with no give-up of its own; a non-fallout error comes back at once,
    /// unchanged.
    #[test]
    fn boot_through_fallout_boots_again_and_power_cycles_only_a_crashed_disk() {
        let sim = Sim::new(0xB007);
        let mut calls = 0;
        let booted = boot_through_fallout(&sim, 8, is_fault_fallout, || {
            calls += 1;
            if calls < 3 {
                Err(format!(
                    "simulated disk write error (injected fault) #{calls}"
                ))
            } else {
                Ok(calls)
            }
        });
        assert_eq!(booted, Ok(Some(3)));
        assert_eq!(sim.coverage().get("boot:retried"), Some(&2));
        assert_eq!(
            sim.coverage().get("boot:booted-after-retry"),
            Some(&1),
            "a retry that boots says so, once"
        );
        assert_eq!(
            sim.coverage().get("restart"),
            None,
            "an IO error is no crash"
        );
        assert_eq!(
            sim.coverage().get("restart:process"),
            Some(&2),
            "each retry after an IO error restarts the process, so the failed boot's handles die"
        );
        assert_eq!(sim.gave_up(), None);

        let sim = Sim::new(0xB007);
        let mut calls = 0;
        let booted = boot_through_fallout(&sim, 8, is_fault_fallout, || {
            calls += 1;
            if calls == 1 {
                sim.crash_disk_now();
                return Err("simulated crash: the disk is gone until Sim::restart()".to_string());
            }
            if sim.disk_crashed() {
                return Err("booted against a disk that was never power-cycled".to_string());
            }
            Ok(calls)
        });
        assert_eq!(booted, Ok(Some(2)));
        assert_eq!(sim.coverage().get("restart"), Some(&1));

        let sim = Sim::new(0xB007);
        let never = boot_through_fallout(&sim, 4, is_fault_fallout, || {
            Err::<(), _>("injected read error")
        });
        assert_eq!(never, Ok(None));
        assert_eq!(sim.coverage().get("boot:retried"), Some(&3));
        assert_eq!(sim.coverage().get("boot:exhausted"), Some(&1));
        assert_eq!(
            sim.coverage().get("boot:booted-after-retry"),
            None,
            "retries that never boot must not read as recovery"
        );
        assert_eq!(sim.gave_up(), None, "what exhaustion means is the caller's");

        let sim = Sim::new(0xB007);
        let mut calls = 0;
        let refused = boot_through_fallout(&sim, 8, is_fault_fallout, || {
            calls += 1;
            Err::<(), _>(
                "log frame 7 is invalid but at-or-below the store's durable horizon 9".to_string(),
            )
        });
        assert_eq!(refused, Ok(None));
        assert_eq!(
            calls, 1,
            "a durable refusal is never booted past: no restart clears it"
        );
        assert_eq!(sim.coverage().get("boot:durable-refusal"), Some(&1));
        assert_eq!(sim.coverage().get("boot:retried"), None);
        assert_eq!(sim.coverage().get("boot:exhausted"), None);
        let reason = sim
            .gave_up()
            .expect("a durable refusal gives the universe up at once");
        assert!(
            reason.contains("refused for durable corruption")
                && reason.contains("durable horizon 9"),
            "under its own name, carrying the refusal: {reason}"
        );

        let sim = Sim::new(0xB007);
        let mut calls = 0;
        let real = boot_through_fallout(&sim, 8, is_fault_fallout, || {
            calls += 1;
            Err::<(), _>("row decode: needed 8 bytes for u64, 3 left")
        });
        assert_eq!(real, Err("row decode: needed 8 bytes for u64, 3 left"));
        assert_eq!(calls, 1, "a non-fallout error is never booted past");
        assert_eq!(sim.coverage().get("boot:retried"), None);
        assert_eq!(sim.gave_up(), None);
    }

    #[test]
    fn acked_implies_durable_catches_a_toy_that_never_syncs() {
        // Pinned: under this seed the round-1 write's crash-collapse draw
        // lands on "dropped" rather than "applied" (a 1-byte write's only
        // outcomes are those two — see disk::tear), so the missing-sync bug
        // is caught on round 1, deterministically — given the current
        // disk::collapse/tear code and FaultConfig::default()'s crash rates.
        // Re-pin if either changes.
        let seed = 0x5eed_ca11;
        let mut sim = Sim::new(seed);
        let err = acked_implies_durable(&mut sim, 5, toy_drive(false), toy_verify).unwrap_err();
        assert!(
            err.starts_with("acked-implies-durable violated after round"),
            "{err}"
        );
        assert!(err.contains(&format!("{seed:#x}")), "{err}");
    }
}
