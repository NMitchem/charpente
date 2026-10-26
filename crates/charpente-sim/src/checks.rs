//! Reusable oracles for sim workloads (step-7 spec §Platform-checker
//! library). v0 shipped exactly three; `durable_corruption_fired` earned
//! its way in when the durable-corruption fault axes landed (segment-mgmt
//! T5), `boot_through_fallout` when `Db::open` stopped absorbing an
//! injected fault inside the table store's open (follow-up 21), and
//! `calls_have_durable_jobs`, `keys_are_per_job` and
//! `outbound_settles_by_horizon` with outbound effects (outbound-effects
//! spec §10), and `clock_refused`, `clock_fallout` and `clock_reopens_after`
//! with the sim's clock axes (0.3.0), and `wait_out_clock_refusal` once the
//! tail loop that uses the last two had five copies (0.3.0) — more earn their
//! way in the same way.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use charpente_core::io::{Clock, IdempotencyKey, WallTime};
use charpente_core::job::{JobRow, JobStatus};
use charpente_core::table::{Snapshot, View};

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
    /// The WAL's durable-horizon hard error: acknowledged records failed
    /// their check at or below the store's horizon. Loud by design, and
    /// fallout only where durable corruption fired on that node's disk,
    /// which its text cannot say, so [`is_fault_fallout`] never excuses it.
    DurableHorizon,
    /// The kernel's clock stops (backwards or stalled) or its open/promote
    /// refusal ([`clock_refused`]).
    ClockRefused,
    /// Everything else: NOT fallout; fails the universe for adjudication.
    Unrelated,
}

/// Classifies `text` (an error's Display/to_string output) into one of five
/// recognized families or `Unrelated`. Families 1-3 are fault fallout by their
/// text alone ([`is_fault_fallout`]); families 4 and 5 are only named here:
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
///     matrix, surfaced loudly by design. But only then: the same text is
///     what a durability bug in the platform looks like (D1's WAL bug showed
///     itself in 8 of 21 failing bases only as this refusal), so this family
///     is a NAME, not an excuse. Only a check that durable corruption fired
///     excuses it: [`durable_refusal`] in a single-node workload, and
///     [`SimHost::durable_corruption_fired`](crate::SimHost::durable_corruption_fired)
///     for the refusing node in a multi-node one (S3b item 0),
///  5. the kernel's clock stops and its open/promote refusal
///     ([`clock_refused`]). The sim's clock axes produce them, and so does a
///     platform bug: a recovered floor above a time the store saw refuses a
///     node whose clock is right. So this family is a name too, excused only
///     where a clock axis fired ([`clock_fallout`]). Whether one fired exactly
///     when the clock was beyond `CLOCK_TOLERANCE` is a clock property's to
///     judge, not this classifier's.
///
/// Anything else is `Unrelated`: NOT fallout, must fail the universe for
/// adjudication. Recognizing families 1-3 cannot mask a real bug so long as
/// the workload's end-state oracle (e.g. exactly-once on reopen) still runs
/// either way.
///
/// First-match-wins, and the order is load-bearing: transactions.rs's,
/// http.rs's and tenant.rs's narrower `matches!(classify(t), Injected)`
/// wrappers accept injected text only because `Injected` is tested before
/// `Poisoned`/`RedbLatch` — text carrying both an injected wording and a
/// poison wording must land `Injected`, not `Poisoned`, or those call sites
/// would silently start rejecting text they used to accept. Reordering the arms below is exactly the class of drift this
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
    } else if clock_refused(text) {
        FaultReason::ClockRefused
    } else {
        FaultReason::Unrelated
    }
}

/// `true` for the families an error's text alone excuses: injected,
/// poisoned and the redb latch (`classify`'s families 1-3). Never for the
/// clock stop or refusal, which [`clock_fallout`] excuses only where a clock
/// axis fired, so a platform suite, whose clock axes are off, fails one. Never
/// for the durable-horizon refusal: whether that is fallout depends on whether durable
/// corruption fired, which text cannot say, so it is `false` here and a
/// caller that retries or gives up on fallout fails it unless it asks
/// [`durable_refusal`] (or, for one node of several,
/// [`SimHost::durable_corruption_fired`](crate::SimHost::durable_corruption_fired))
/// first. Until S3b item 0 this returned `true` for every recognized family,
/// so any such workload excused the refusal where no fault had fired.
pub fn is_fault_fallout(text: &str) -> bool {
    matches!(
        classify(text),
        FaultReason::Injected | FaultReason::Poisoned | FaultReason::RedbLatch
    )
}

/// `true` for the kernel's clock stops ("clock went backwards: …" and
/// "clock stalled: …") and for its refusal to open or promote a node whose
/// wall clock is more than `CLOCK_TOLERANCE` behind the latest time its data
/// directory recorded (charpente's `kernel/clock.rs`): `classify`'s
/// `ClockRefused`. A name, not an excuse; a workload asks [`clock_fallout`]
/// whether this run's faults explain one, and a clock property marks one
/// when it sees it.
pub fn clock_refused(text: &str) -> bool {
    text.contains("clock went backwards:")
        || text.contains("clock stalled:")
        || text.contains("refusing to open: this node's wall clock")
        || text.contains("refusing to promote: this node's wall clock")
}

/// `true` when `text` is the kernel's clock stop or open/promote refusal
/// ([`clock_refused`]) and one of the sim's clock axes acted in this run:
/// a clock read with its own skew (`clock:skewed`), a step back
/// (`clock:stepped-back`) or a downtime jump (`clock:downtime`). That is when
/// the stop or refusal is fault fallout: a step back past the tolerance, or
/// one that keeps the clock at or below the hold for longer than that, stops the
/// node, and leaves it behind its store's recorded time until true time
/// catches up, and two hosts' skews put one behind times the other recorded.
/// Where no axis acted, nothing excuses one, and it is a platform bug, the
/// rule [`durable_refusal`] follows for its family. An app workload's fallout
/// filter is `|t| is_fault_fallout(t) || clock_fallout(sim, t)`.
pub fn clock_fallout(sim: &Sim, text: &str) -> bool {
    if !clock_refused(text) {
        return false;
    }
    let coverage = sim.coverage();
    ["clock:skewed", "clock:stepped-back", "clock:downtime"]
        .iter()
        .any(|key| coverage.get(key).copied().unwrap_or(0) > 0)
}

/// The instant a clock stop or refusal ([`clock_refused`]) names as the one
/// after which the node runs again: its last "after <instant>", written to the
/// millisecond, so wait a little past it. `None` for any other text, and for
/// the stall stop ("clock stalled: …"), which names no instant: its clock
/// stopped advancing, and no wait fixes that. For a fault-free tail: after
/// `Sim::quiesce_clock_faults` the clock's offset is frozen, so a node refused
/// at open reaches this instant as virtual time passes, and a workload sleeps
/// until then and restarts the process. A frozen offset still advances, so a
/// tail's kernel never stalls, and one that does is a bug the `None` reports.
pub fn clock_reopens_after(text: &str) -> Option<WallTime> {
    if !clock_refused(text) {
        return None;
    }
    let tail = &text[text.rfind(" after ")? + " after ".len()..];
    let stamp = tail.get(..24)?;
    let b = stamp.as_bytes();
    let field = |range: std::ops::Range<usize>| -> Option<i64> {
        let digits = stamp.get(range)?;
        digits
            .bytes()
            .all(|c| c.is_ascii_digit())
            .then(|| digits.parse().ok())?
    };
    if [b[4], b[7], b[10], b[13], b[16], b[19], b[23]] != *b"--T::.Z" {
        return None;
    }
    let (year, month, day) = (field(0..4)?, field(5..7)?, field(8..10)?);
    let (hour, minute, second, millis) = (
        field(11..13)?,
        field(14..16)?,
        field(17..19)?,
        field(20..23)?,
    );
    if !(1..=12).contains(&month)
        || !(1..=31).contains(&day)
        || hour > 23
        || minute > 59
        || second > 59
    {
        return None;
    }
    // Howard Hinnant's days_from_civil, the inverse of `WallTime`'s Display.
    let y = year - i64::from(month <= 2);
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * (if month > 2 { month - 3 } else { month + 9 }) + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    let secs = days * 86_400 + hour * 3_600 + minute * 60 + second;
    let nanos = u64::try_from(secs).ok()?.checked_mul(1_000_000_000)?;
    Some(WallTime::from_nanos(
        nanos.checked_add(millis as u64 * 1_000_000)?,
    ))
}

/// A fault-free tail's boot loop, waiting out a clock refusal (GUIDE §8, "A
/// fault-free tail"). Calls `boot` and returns what it returns, `Ok(Some(t))`
/// booted and `Ok(None)` given up by the caller, unless it fails with a clock
/// stop or refusal that this run's clock faults explain ([`clock_fallout`])
/// and that names an instant ([`clock_reopens_after`]). Then it sleeps one
/// second past that instant on `sim.clock()`, restarts the process
/// (`Sim::restart_process`: a refused node exits, and its supervisor starts it
/// again) and calls `boot` again. One wait is enough, since nothing moves the
/// clock or the recorded time in between; it waits twice at most, and a third
/// refusal gives the run up by name with the refusal as its detail and returns
/// `Ok(None)`. Any other error comes back unchanged, the stall stop included:
/// it names no instant, because no wait fixes a clock that stopped advancing.
/// Call it after `Sim::quiesce_clock_faults`, which freezes the clock's offset
/// so it reaches the instant as virtual time passes.
pub fn wait_out_clock_refusal<T>(
    sim: &Sim,
    mut boot: impl FnMut() -> Result<Option<T>, String>,
) -> Result<Option<T>, String> {
    let mut waits = 0;
    loop {
        let refusal = match boot() {
            Ok(booted) => return Ok(booted),
            Err(e) => e,
        };
        let Some(at) = clock_reopens_after(&refusal).filter(|_| clock_fallout(sim, &refusal))
        else {
            return Err(refusal);
        };
        if waits == 2 {
            sim.give_up("the tail was refused after two waits", refusal);
            return Ok(None);
        }
        waits += 1;
        let wait = at.as_nanos().saturating_sub(sim.clock().wall().as_nanos()) + 1_000_000_000;
        sim.clock().sleep(Duration::from_nanos(wait));
        sim.restart_process();
    }
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
/// state to a prefix, which stays consistent. In a multi-node workload, ask
/// [`SimHost::durable_corruption_fired`](crate::SimHost::durable_corruption_fired)
/// instead: it answers for one host's disk, so a refusal on one node is never
/// excused by a fault on another.
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
/// regression that must fail loud — not be masked as a routine restore. In
/// a multi-node workload, ask
/// [`SimHost::dir_entry_loss_fired`](crate::SimHost::dir_entry_loss_fired)
/// instead: it answers for one host's disk, so one node's gap is never
/// excused by entry loss on another.
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
/// the exact round. A run in which no round acked anything checked nothing,
/// and gives the universe up by name (`Sim::give_up`) instead of passing.
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
    // Every `verify` above ran over an empty list, so none could have failed.
    if acked.is_empty() {
        sim.give_up(
            "acked-implies-durable: no round acked anything, so there was no claim to check",
            format!("{rounds} rounds"),
        );
    }
    Ok(())
}

/// Boots the way a supervised process boots after a failed boot: restart and
/// retry, which is `Db::open`'s own contract for an IO failure (DESIGN §4).
/// Calls `boot`; when it fails with an error `is_fallout` accepts (injected
/// fault fallout, by the caller's own classifier), calls it again, up to
/// `tries` boots in all. A durable-horizon refusal is judged by
/// [`durable_refusal`] before `is_fallout` sees it, so a caller's classifier
/// cannot excuse one where no durable corruption fired. Each retry marks `boot:retried`, and a retry that
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
///     horizon, in a universe where durable corruption fired
///     ([`durable_refusal`]). Those bytes are on the disk, so no restart
///     clears them: the helper stops at once, marks `boot:durable-refusal`
///     and gives the universe up under that name. The give-up is the first,
///     so a caller's own later one cannot replace it.
/// - `Err(e)`: the first error `is_fallout` rejects, unchanged and at once. A
///   non-injected error is a finding, never a reason to boot again. So is a
///   durable refusal where no durable corruption fired: nothing excuses it.
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
                match durable_refusal(sim, &text) {
                    Ok(true) => return Ok(None),
                    Ok(false) => {}
                    Err(_) => return Err(e),
                }
                if !is_fallout(&text) {
                    return Err(e);
                }
            }
        }
    }
    sim.cover("boot:exhausted");
    Ok(None)
}

/// What a boot refused for durable corruption at or below the store's horizon
/// (`FaultReason::DurableHorizon`) ends, for any loop that boots again on
/// fallout: [`boot_through_fallout`] and every final verification's reopen
/// loop. Those bytes are on the disk, so no restart clears them, and booting
/// again only spends the loop's budget before it gives up under the wrong
/// name (S5 re-review R-M2).
///
/// - `Ok(false)`: `text` is no durable refusal; the loop carries on.
/// - `Ok(true)`: it is one, and durable corruption fired in this universe
///   ([`durable_corruption_fired`]). The universe is given up under the
///   refusal's own name and `boot:durable-refusal` is marked; the loop ends.
/// - `Err(..)`: it is one, but no durable corruption fired, so nothing
///   excuses it. The text names it, and the universe fails.
pub fn durable_refusal(sim: &Sim, text: &str) -> Result<bool, String> {
    if !matches!(classify(text), FaultReason::DurableHorizon) {
        return Ok(false);
    }
    if !durable_corruption_fired(sim) {
        return Err(format!(
            "a boot was refused for durable corruption at or below the store's horizon, but no \
             durable corruption fired in this universe, so nothing excuses the refusal: {text}"
        ));
    }
    sim.cover("boot:durable-refusal");
    sim.give_up(
        "the boot was refused for durable corruption at or below the store's horizon, which no \
         restart clears, so nothing past it was adjudicated",
        text,
    );
    Ok(true)
}

/// Every call that reached a remote carries the key of a job in `snap`, the
/// recovered store (outbound-effects spec §10): a call never fires for an
/// enqueue that is not durable. Single-node. One sim box's only loss window is
/// media corruption: where [`durable_corruption_fired`], a cut enqueue whose
/// call was already made fails this check, so a workload routes a failure
/// through that and gives the universe up, never passes it. Attributes by
/// `OutboundCall::key`, never by where the app put the key.
pub fn calls_have_durable_jobs(sim: &Sim, snap: &Snapshot) -> Result<(), String> {
    let keys: BTreeSet<[u8; 16]> = snap
        .scan::<JobRow>()
        .map_err(|e| format!("calls_have_durable_jobs: scanning the jobs: {e}"))?
        .into_iter()
        .filter_map(|row| row.outbound_key)
        .collect();
    for d in sim.outbound_deliveries() {
        if !keys.contains(d.key.as_bytes()) {
            return Err(format!(
                "calls_have_durable_jobs: a call to {} with key {} reached the remote at {}ns, but \
                 no job in the recovered store carries that key — a call fired for an enqueue \
                 that never became durable",
                d.host,
                d.key.as_str(),
                d.at
            ));
        }
    }
    Ok(())
}

/// No two jobs carry one idempotency key (spec §10): it catches two jobs
/// sharing a key, which a constant or data-derived key produces.
pub fn keys_are_per_job(snap: &Snapshot) -> Result<(), String> {
    let mut seen: BTreeMap<[u8; 16], u64> = BTreeMap::new();
    for row in snap
        .scan::<JobRow>()
        .map_err(|e| format!("keys_are_per_job: scanning the jobs: {e}"))?
    {
        let Some(key) = row.outbound_key else {
            continue;
        };
        if let Some(first) = seen.insert(key, row.id.raw()) {
            return Err(format!(
                "keys_are_per_job: jobs {first} and {} carry the same idempotency key {} — keys \
                 are minted per enqueue at staging; a key computed from job data or a constant \
                 repeats",
                row.id.raw(),
                IdempotencyKey::from_bytes(key).as_str()
            ));
        }
    }
    Ok(())
}

/// After a fault-free tail long enough for every retry, every outbound job is
/// Done or Dead (spec §10). Call it only after `quiesce_disk_faults`,
/// `quiesce_outbound_faults`, and a run past the last backoff and deadline.
pub fn outbound_settles_by_horizon(snap: &Snapshot) -> Result<(), String> {
    for row in snap
        .scan::<JobRow>()
        .map_err(|e| format!("outbound_settles_by_horizon: scanning the jobs: {e}"))?
    {
        if row.outbound_key.is_some() && row.status == JobStatus::Pending {
            return Err(format!(
                "outbound_settles_by_horizon: job {} (\"{}\") is still Pending after the \
                 fault-free tail, at attempt {} — an outbound job settles or retries to Dead; one \
                 that does neither was lost by the kernel",
                row.id.raw(),
                row.name,
                row.attempts
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use charpente_core::db::{Db, DbConfig, Job, Schema, Tx, outbound_fingerprint};
    use charpente_core::io::{Disk, Entropy};
    use charpente_core::io::{HttpMethod, Outbound, OutboundCall};
    use charpente_core::table::{Codec, Unscoped};
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
        ] {
            assert!(is_fault_fallout(fallout), "must classify: {fallout}");
        }
        for real_bug in [
            "row decode: needed 8 bytes for u64, 3 left — schema drift or corruption",
            "assertion failed: durable.is_some_and(|d| d >= applied)",
            "exactly-once violated: hits 3, done 2 for key 7",
            // A recognized family, but not fallout by its text: only a check
            // that durable corruption fired excuses it (S3b item 0).
            "log frame 3 is invalid but at-or-below the store's durable horizon 7 — this is \
             corruption of acknowledged records, not a torn tail, and truncating it would \
             silently lose data; restore this node from a replica or backup (DESIGN §2)",
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

        // Precedence pin (review I1): transactions.rs's/http.rs's/tenant.rs's
        // narrower `matches!(classify(t), Injected)` wrappers accept injected
        // text only because `Injected` is tested before `Poisoned`/`RedbLatch`
        // above. A text carrying both an injected AND
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

    /// The kernel's clock stop and its open and promote refusals (charpente's
    /// `kernel/clock.rs`) are a name, like the durable-horizon refusal: text
    /// alone never excuses one, since a platform suite runs with the clock axes
    /// off and a refusal there is a bug. `clock_fallout` excuses one only once
    /// a clock axis acted in the run: a skewed read, a step back, or a read
    /// after a downtime jump. One exemplar of each text, verbatim, and no
    /// other family's exemplar reads as one.
    #[test]
    fn a_clock_refusal_is_fallout_only_where_a_clock_axis_fired() {
        use crate::FaultConfig;
        use charpente_core::io::Clock;

        let texts = [
            "clock went backwards: this node's wall clock reads 2026-01-01T00:00:39.000Z, 61s \
             behind 2026-01-01T00:01:40.000Z, the latest time it already used — more than the \
             60s charpente tolerates (CLOCK_TOLERANCE). The node stopped rather than extend \
             sessions or delay schedules by that much. Fix the system clock (check NTP) and \
             restart; if the clock is right, the earlier reading came from a clock that ran \
             ahead, and the node runs again after 2026-01-01T00:00:40.000Z.",
            "clock stalled: this node's wall clock has not advanced past \
             2026-01-01T00:01:40.000Z, the latest time it already used, for 61s of elapsed time \
             — more than the 60s charpente tolerates (CLOCK_TOLERANCE). The node stopped rather \
             than hold sessions open and schedules still. Fix the system clock (check NTP) and \
             restart.",
            "refusing to open: this node's wall clock reads 2026-01-01T00:00:39.000Z, 61s behind \
             2026-01-01T00:01:40.000Z, the latest time this data directory recorded — more than \
             the 60s charpente tolerates (CLOCK_TOLERANCE). Fix the system clock (check NTP); if \
             the clock is right, the recorded time came from a clock that ran ahead, and the \
             node will open after 2026-01-01T00:00:40.000Z.",
            "refusing to promote: this node's wall clock reads 2026-01-01T00:00:39.000Z, 61s \
             behind 2026-01-01T00:01:40.000Z, the latest time this data directory recorded — \
             more than the 60s charpente tolerates (CLOCK_TOLERANCE). Fix the system clock \
             (check NTP); if the clock is right, the recorded time came from a clock that ran \
             ahead, and the node will promote after 2026-01-01T00:00:40.000Z.",
        ];
        let quiet = Sim::with_faults(0xC10C_F001, FaultConfig::none());
        quiet.clock().wall();
        let skewed = Sim::with_faults(
            0xC10C_F002,
            FaultConfig {
                clock_skew: 90_000_000_000,
                ..FaultConfig::none()
            },
        );
        let stepped = Sim::with_faults(
            0xC10C_F003,
            FaultConfig {
                clock_step_back: (1.0, std::time::Duration::from_secs(120)),
                ..FaultConfig::none()
            },
        );
        let jumped = Sim::with_faults(
            0xC10C_F004,
            FaultConfig {
                clock_downtime: (1.0, std::time::Duration::from_secs(3 * 86_400)),
                ..FaultConfig::none()
            },
        );
        jumped.restart();
        for text in texts {
            assert_eq!(classify(text), FaultReason::ClockRefused, "{text}");
            assert!(clock_refused(text), "{text}");
            assert!(!is_fault_fallout(text), "text alone excuses it: {text}");
            assert!(
                !clock_fallout(&quiet, text),
                "excused with no clock axis on: {text}"
            );
            assert!(
                !clock_fallout(&jumped, text),
                "excused by a jump no reading has shown yet: {text}"
            );
        }
        skewed.clock().wall();
        stepped.clock().wall();
        jumped.clock().wall();
        for (sim, axis) in [
            (&skewed, "skew"),
            (&stepped, "a step back"),
            (&jumped, "downtime"),
        ] {
            for text in texts {
                assert!(clock_fallout(sim, text), "not excused after {axis}: {text}");
            }
        }
        for other in [
            "simulated disk write error (injected fault)",
            "WAL poisoned by an earlier IO failure",
            "Storage error: close and re-open the database",
            "log frame 3 is invalid but at-or-below the store's durable horizon 7",
            "exactly-once violated: hits 3, done 2 for key 7",
            // A refusal to open for another reason is not a clock refusal.
            "refusing to open: the data directory was written by charpente 0.2",
        ] {
            assert!(!clock_refused(other), "{other}");
            assert_ne!(classify(other), FaultReason::ClockRefused, "{other}");
            assert!(!clock_fallout(&stepped, other), "{other}");
        }
    }

    /// The instant each clock text names round-trips through `WallTime`'s
    /// Display, and nothing else parses.
    #[test]
    fn clock_reopens_after_reads_the_instant_each_clock_text_names() {
        let stop = "clock went backwards: this node's wall clock reads 2026-01-01T00:00:39.000Z, \
                    61s behind 2026-01-01T00:01:40.000Z, the latest time it already used — more \
                    than the 60s charpente tolerates (CLOCK_TOLERANCE). The node stopped rather \
                    than extend sessions or delay schedules by that much. Fix the system clock \
                    (check NTP) and restart; if the clock is right, the earlier reading came from \
                    a clock that ran ahead, and the node runs again after 2026-01-01T00:00:40.000Z.";
        assert_eq!(
            clock_reopens_after(stop),
            Some(WallTime::from_nanos(1_767_225_640_000_000_000))
        );
        for verb in ["open", "promote"] {
            let refusal = format!(
                "refusing to {verb}: this node's wall clock reads 2024-02-29T23:59:58.250Z, \
                 79.62s behind 2024-03-01T00:01:17.870Z, the latest time this data directory \
                 recorded — more than the 60s charpente tolerates (CLOCK_TOLERANCE). Fix the \
                 system clock (check NTP); if the clock is right, the recorded time came from a \
                 clock that ran ahead, and the node will {verb} after 2024-03-01T00:00:17.870Z."
            );
            let at = clock_reopens_after(&refusal).expect(&refusal);
            assert_eq!(at.to_string(), "2024-03-01T00:00:17.870Z");
        }
        let mut state = 0x0A57_7E11_u64;
        for _ in 0..2_000 {
            // Up to the year 2554, at millisecond precision.
            let nanos = crate::rng::splitmix64(&mut state) / 1_000_000 * 1_000_000;
            let t = WallTime::from_nanos(nanos);
            let text = format!("clock went backwards: … the node runs again after {t}.");
            assert_eq!(clock_reopens_after(&text), Some(t), "{t}");
        }
        let stall = "clock stalled: this node's wall clock has not advanced past \
                     2026-01-01T00:01:40.000Z, the latest time it already used, for 61s of \
                     elapsed time — more than the 60s charpente tolerates (CLOCK_TOLERANCE). The \
                     node stopped rather than hold sessions open and schedules still. Fix the \
                     system clock (check NTP) and restart.";
        assert!(clock_refused(stall));
        assert_eq!(
            clock_reopens_after(stall),
            None,
            "a stall names no instant: a clock that stopped advancing is not waited out"
        );
        for other in [
            "simulated disk write error (injected fault): retry after 2026-01-01T00:00:40.000Z.",
            "clock went backwards: the node runs again after a while.",
            "clock went backwards: the node runs again after 2026-13-01T00:00:40.000Z.",
            "clock went backwards: the node runs again after 2026-01-01T00:00:40.000",
        ] {
            assert_eq!(clock_reopens_after(other), None, "{other}");
        }
    }

    /// Each time field is refused alone one past its range, and the last
    /// millisecond of a day is read. A text naming no real instant gives
    /// `None`, never a wait for one.
    #[test]
    fn clock_reopens_after_refuses_each_time_field_past_its_range() {
        let at = |stamp: &str| {
            clock_reopens_after(&format!(
                "clock went backwards: … the node runs again after {stamp}."
            ))
        };
        for stamp in [
            "2026-01-01T24:00:00.000Z",
            "2026-01-01T23:60:00.000Z",
            "2026-01-01T23:59:60.000Z",
        ] {
            assert_eq!(at(stamp), None, "{stamp}");
        }
        assert_eq!(
            at("2026-01-01T23:59:59.999Z"),
            Some(WallTime::from_nanos(1_767_311_999_999_000_000))
        );
    }

    /// The date arithmetic across the months it treats apart: January and
    /// February count from the year before, 29 February of a leap year, the
    /// first of March after it, and a year boundary. Each instant's Display is
    /// the text, and it is read back to the millisecond. Epoch milliseconds
    /// from Python's `datetime`.
    #[test]
    fn clock_reopens_after_reads_dates_across_months_and_years() {
        for (stamp, millis) in [
            ("2023-01-31T08:15:30.125Z", 1_675_152_930_125_u64),
            ("2023-02-01T00:00:00.000Z", 1_675_209_600_000),
            ("2023-02-28T23:59:59.999Z", 1_677_628_799_999),
            ("2024-02-29T12:34:56.789Z", 1_709_210_096_789),
            ("2024-03-01T00:00:00.001Z", 1_709_251_200_001),
            ("2025-12-31T23:59:59.999Z", 1_767_225_599_999),
            ("2026-01-01T00:00:00.000Z", 1_767_225_600_000),
        ] {
            let t = WallTime::from_nanos(millis * 1_000_000);
            assert_eq!(t.to_string(), stamp);
            let text = format!(
                "refusing to open: this node's wall clock … and the node will open after {t}."
            );
            assert_eq!(clock_reopens_after(&text), Some(t), "{stamp}");
        }
    }

    /// Each outcome, with what it leaves on the run: a clock refusal this run's
    /// faults explain is waited out (one second past its instant, then one
    /// process restart) and booted again; a third refusal gives up by name with
    /// the refusal as the detail; and anything else, the stall stop or a
    /// refusal no clock fault explains included, comes back at once, with no
    /// wait and no restart.
    #[test]
    fn wait_out_clock_refusal_waits_past_the_instant_twice_at_most() {
        use crate::FaultConfig;

        // A skew acted at the first read, so `clock_fallout` excuses a clock
        // refusal on this run.
        let sim = Sim::with_faults(
            0xC10C_F005,
            FaultConfig {
                clock_skew: 90_000_000_000,
                ..FaultConfig::none()
            },
        );
        let refused_until = |sim: &Sim, ahead: u64| {
            let at = WallTime::from_nanos(sim.clock().wall().as_nanos() + ahead);
            format!("refusing to open: this node's wall clock … and the node will open after {at}.")
        };
        let restarts = |sim: &Sim| sim.coverage().get("restart:process").copied().unwrap_or(0);

        let mut boots = 0;
        let booted = wait_out_clock_refusal(&sim, || {
            boots += 1;
            Ok(Some(boots))
        });
        assert_eq!(booted, Ok(Some(1)), "a clean boot is returned as it is");
        assert_eq!(
            (sim.now().as_nanos(), restarts(&sim)),
            (0, 0),
            "with no refusal there is no wait and no restart"
        );

        let first = refused_until(&sim, 30_000_000_000);
        // The text names its instant to the millisecond.
        let wait = clock_reopens_after(&first).unwrap().as_nanos() - sim.clock().wall().as_nanos()
            + 1_000_000_000;
        assert!((30_000_000_000..=31_000_000_000).contains(&wait), "{wait}");
        let mut boots = 0;
        let booted = wait_out_clock_refusal(&sim, || {
            boots += 1;
            if boots == 1 {
                Err(first.clone())
            } else {
                Ok(Some(boots))
            }
        });
        assert_eq!(booted, Ok(Some(2)), "boots again after the wait");
        assert_eq!(
            (sim.now().as_nanos(), restarts(&sim)),
            (wait, 1),
            "one wait sleeps one second past the instant the refusal names, then restarts the \
             process once"
        );
        assert_eq!(sim.gave_up(), None);

        // A fourth call panics, so a loop that never stops waiting fails here
        // instead of sleeping forever.
        let mut boots = 0;
        let booted = wait_out_clock_refusal(&sim, || -> Result<Option<u32>, String> {
            boots += 1;
            assert!(boots <= 3, "wait_out_clock_refusal booted a fourth time");
            Err(refused_until(&sim, 5_000_000_000))
        });
        assert_eq!(
            (booted, boots, restarts(&sim)),
            (Ok(None), 3, 3),
            "three refusals: two waits, each with its restart, and no third"
        );
        assert_eq!(sim.gave_up(), Some("the tail was refused after two waits"));
        assert!(
            sim.gave_up_detail()
                .is_some_and(|d| d.starts_with("refusing to open: ")),
            "the detail is the last refusal: {:?}",
            sim.gave_up_detail()
        );

        let stall = "clock stalled: this node's wall clock has not advanced past \
                     2026-01-01T00:01:40.000Z, the latest time it already used, for 61s of \
                     elapsed time — more than the 60s charpente tolerates (CLOCK_TOLERANCE). The \
                     node stopped rather than hold sessions open and schedules still. Fix the \
                     system clock (check NTP) and restart.";
        let quiet = Sim::with_faults(0xC10C_F006, FaultConfig::none());
        for (sim, text) in [
            (&sim, stall.to_string()),
            (
                &sim,
                "simulated disk write error (injected fault)".to_string(),
            ),
            (&quiet, refused_until(&quiet, 5_000_000_000)),
        ] {
            let (now, before) = (sim.now(), restarts(sim));
            let mut boots = 0;
            let out = wait_out_clock_refusal(sim, || -> Result<Option<()>, String> {
                boots += 1;
                Err(text.clone())
            });
            assert_eq!(out, Err(text.clone()), "comes back unchanged");
            assert_eq!(
                (boots, sim.now(), restarts(sim)),
                (1, now, before),
                "no wait and no restart: {text}"
            );
        }
        assert!(quiet.gave_up().is_none());
        let mut ended = false;
        let out = wait_out_clock_refusal(&quiet, || -> Result<Option<()>, String> {
            assert!(!ended, "boot is not called again after it gave up");
            ended = true;
            Ok(None)
        });
        assert_eq!(
            out,
            Ok(None),
            "the caller's own give-up is returned as it is"
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
            crate::FaultConfig {
                misdirected_write: 1.0,
                ..crate::FaultConfig::none()
            },
        );
        let file = sim.disk().open("wal/f").unwrap();
        file.write_at(0, b"x").unwrap();
        assert!(durable_corruption_fired(&sim));

        // A bit flip at restart IS.
        let sim = Sim::with_faults(
            0xD0C_0004,
            crate::FaultConfig {
                bit_flip: 1.0,
                ..crate::FaultConfig::none()
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

    /// A run in which no round acked anything checked nothing: `verify` ran
    /// over an empty list every round. It gives up by name rather than pass.
    #[test]
    fn acked_implies_durable_gives_up_when_nothing_was_ever_acked() {
        let mut sim = Sim::new(0xACE0);
        acked_implies_durable(&mut sim, 3, |_sim: &mut Sim| Ok(vec![]), toy_verify).unwrap();
        assert_eq!(
            sim.gave_up(),
            Some("acked-implies-durable: no round acked anything, so there was no claim to check")
        );
        assert_eq!(
            sim.gave_up_detail().as_deref(),
            Some("3 rounds"),
            "the round count is the caller's runtime value, so it is the detail, never part of \
             the fixed reason"
        );
    }

    /// One ack in any round is a claim, and the run reaches a verdict on it.
    #[test]
    fn acked_implies_durable_reaches_a_verdict_on_a_single_ack() {
        let mut sim = Sim::new(0xACE1);
        let mut drive = toy_drive(true);
        let mut round = 0;
        let once = move |sim: &mut Sim| {
            round += 1;
            if round == 2 { drive(sim) } else { Ok(vec![]) }
        };
        acked_implies_durable(&mut sim, 3, once, toy_verify).unwrap();
        assert_eq!(sim.gave_up(), None);
    }

    #[test]
    fn acked_implies_durable_passes_when_synced_before_ack() {
        let mut sim = Sim::new(0xACED);
        acked_implies_durable(&mut sim, 5, toy_drive(true), toy_verify).unwrap();
    }

    /// The checker must restart the CALLER'S world, because that is the world
    /// the harness reads coverage from — `run_app_universe` hands the workload a
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

        // A durable refusal where durable corruption fired: a bit flip on a
        // durable WAL byte at the power cycle.
        let sim = Sim::with_faults(
            0xB007,
            crate::FaultConfig {
                bit_flip: 1.0,
                ..crate::FaultConfig::none()
            },
        );
        let disk = sim.disk();
        let file = disk.open("wal/00000000000000000000.log").unwrap();
        file.write_at(0, &[7; 16]).unwrap();
        file.sync().unwrap();
        disk.sync_dir("wal/").unwrap();
        sim.restart();
        assert!(durable_corruption_fired(&sim));
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
            reason.contains("refused for durable corruption"),
            "under its own name: {reason}"
        );
        assert!(
            sim.gave_up_detail()
                .is_some_and(|d| d.contains("durable horizon 9")),
            "carrying the refusal as its detail: {:?}",
            sim.gave_up_detail()
        );

        // The same refusal where no durable corruption fired is a finding:
        // it comes back unchanged and at once, and nothing is given up (S5
        // re-review R-M3).
        let sim = Sim::new(0xB007);
        let mut calls = 0;
        let unexcused = boot_through_fallout(&sim, 8, is_fault_fallout, || {
            calls += 1;
            Err::<(), _>(
                "log frame 7 is invalid but at-or-below the store's durable horizon 9".to_string(),
            )
        });
        assert_eq!(
            unexcused,
            Err("log frame 7 is invalid but at-or-below the store's durable horizon 9".to_string())
        );
        assert_eq!(
            calls, 1,
            "an unexcused durable refusal is never booted past"
        );
        assert_eq!(sim.coverage().get("boot:durable-refusal"), None);
        assert_eq!(sim.coverage().get("boot:retried"), None);
        assert_eq!(sim.gave_up(), None);

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

    /// `durable_refusal`'s three answers, for a loop that boots on its own:
    /// anything but a durable refusal carries on; a durable refusal gives up
    /// under its name only where durable corruption fired, and fails by name
    /// everywhere else (S5 re-review R-M2, R-M3).
    #[test]
    fn durable_refusal_ends_a_loop_by_name_and_is_excused_only_where_corruption_fired() {
        const REFUSAL: &str = "log frame 3 is invalid but at-or-below the store's durable \
                               horizon 7 — restore this node from a replica or backup";
        let sim = Sim::new(0xD0E5);
        assert_eq!(
            durable_refusal(&sim, "simulated disk write error (injected fault)"),
            Ok(false)
        );
        let err = durable_refusal(&sim, REFUSAL).expect_err("no corruption fired");
        assert!(
            err.starts_with(
                "a boot was refused for durable corruption at or below the store's horizon, but \
                 no durable corruption fired in this universe"
            ) && err.ends_with(REFUSAL),
            "{err}"
        );
        assert_eq!(sim.gave_up(), None);
        assert_eq!(sim.coverage().get("boot:durable-refusal"), None);

        let sim = Sim::with_faults(
            0xD0E5,
            crate::FaultConfig {
                misdirected_write: 1.0,
                ..crate::FaultConfig::none()
            },
        );
        let file = sim.disk().open("wal/00000000000000000000.log").unwrap();
        file.write_at(0, &[7; 16]).unwrap();
        assert!(durable_corruption_fired(&sim));
        assert_eq!(durable_refusal(&sim, REFUSAL), Ok(true));
        assert_eq!(sim.coverage().get("boot:durable-refusal"), Some(&1));
        let reason = sim
            .gave_up()
            .expect("an excused refusal gives the universe up");
        assert!(
            reason.starts_with(
                "the boot was refused for durable corruption at or below the store's horizon, \
                 which no restart clears"
            ),
            "{reason}"
        );
        assert_eq!(sim.gave_up_detail().as_deref(), Some(REFUSAL));
    }

    /// GUIDE §8's promise that no classifier of the caller's can excuse a
    /// durable refusal (S3b review M5). Every other caller passes
    /// `is_fault_fallout`, which already rejects the refusal's text, so only a
    /// classifier that accepts everything can tell whether
    /// `boot_through_fallout` judges the refusal itself. With no corruption
    /// fired, the first refusal comes back at once, unchanged, and nothing
    /// gives up. Where corruption did fire, it gives up at once by name.
    #[test]
    fn no_classifier_can_excuse_a_durable_refusal() {
        const REFUSAL: &str =
            "log frame 7 is invalid but at-or-below the store's durable horizon 9";
        let sim = Sim::new(0xB7F5);
        let mut calls = 0;
        let booted = boot_through_fallout(
            &sim,
            8,
            |_| true,
            || {
                calls += 1;
                Err::<(), _>(REFUSAL.to_string())
            },
        );
        assert_eq!(
            booted,
            Err(REFUSAL.to_string()),
            "nothing fired, so the refusal is the answer, whatever the classifier says"
        );
        assert_eq!(calls, 1, "and it is not booted again");
        assert_eq!(sim.gave_up(), None);
        assert_eq!(sim.coverage().get("boot:retried"), None);

        let sim = Sim::with_faults(
            0xB7F5,
            crate::FaultConfig {
                misdirected_write: 1.0,
                ..crate::FaultConfig::none()
            },
        );
        let file = sim.disk().open("wal/00000000000000000000.log").unwrap();
        file.write_at(0, &[7; 16]).unwrap();
        let mut calls = 0;
        let booted = boot_through_fallout(
            &sim,
            8,
            |_| true,
            || {
                calls += 1;
                Err::<(), _>(REFUSAL.to_string())
            },
        );
        assert_eq!(booted, Ok(None), "corruption fired: the refusal gives up");
        assert_eq!(calls, 1, "at once, not after the classifier's retries");
        assert!(
            sim.gave_up_detail().as_deref() == Some(REFUSAL),
            "{:?}",
            sim.gave_up_detail()
        );
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

    // ------------------------------------------------- the outbound checkers

    struct Charge;
    impl Codec for Charge {
        fn encode(&self, _out: &mut Vec<u8>) {}
        fn decode(_input: &mut &[u8]) -> std::io::Result<Charge> {
            Ok(Charge)
        }
    }
    impl Job for Charge {
        const NAME: &'static str = "charge";
        const FINGERPRINT: u64 = outbound_fingerprint(0x7E57_0C0C);
        type Scope = Unscoped;
        const OUTBOUND: bool = true;
    }

    /// Hands out `keys` in order, so a test can give two jobs one key, as a
    /// constant or data-derived key would: what `keys_are_per_job` catches.
    struct Scripted(std::cell::RefCell<Vec<[u8; 16]>>);
    impl Entropy for Scripted {
        fn fill(&self, buf: &mut [u8]) {
            buf.copy_from_slice(&self.0.borrow_mut().remove(0));
        }
    }

    /// One Pending outbound job per key, each enqueued through a minting
    /// group over a fresh `Sim`.
    fn jobs_with_keys(keys: &[[u8; 16]]) -> (Sim, Db) {
        let sim = Sim::with_faults(0x0c4, crate::FaultConfig::none());
        let mut db = Db::open(Box::new(sim.disk()), DbConfig::default(), Schema::new()).unwrap();
        let mint = Scripted(std::cell::RefCell::new(keys.to_vec()));
        let mut group = db.begin_group_minting(&mint).unwrap();
        group
            .stage(|tx: &mut Tx| {
                for _ in keys {
                    tx.enqueue(Charge);
                }
                Ok::<(), std::io::Error>(())
            })
            .unwrap();
        group.commit().map_err(|f| f.error).unwrap();
        (sim, db)
    }

    fn orphan_call(key: [u8; 16]) -> OutboundCall {
        OutboundCall {
            host: "pay.test",
            method: HttpMethod::Post,
            path: "/x".into(),
            headers: Vec::new(),
            body: Vec::new(),
            key: IdempotencyKey::from_bytes(key),
        }
    }

    #[test]
    fn keys_are_per_job_names_both_jobs_and_the_key() {
        let (_sim, db) = jobs_with_keys(&[[1; 16], [2; 16], [1; 16]]);
        let snap = db.snapshot().unwrap();
        assert_eq!(
            keys_are_per_job(&snap),
            Err(
                "keys_are_per_job: jobs 0 and 2 carry the same idempotency key \
                 01010101010101010101010101010101 — keys are minted per enqueue at staging; a \
                 key computed from job data or a constant repeats"
                    .to_string()
            )
        );
    }

    #[test]
    fn calls_have_durable_jobs_names_an_orphan_call() {
        let (sim, db) = jobs_with_keys(&[[1; 16]]);
        let snap = db.snapshot().unwrap();
        sim.remote("pay.test", crate::outbound::Recorder::new());
        let ob = sim.outbound(&["pay.test"]).unwrap();
        ob.start(0, orphan_call([9; 16])).unwrap();
        let err = calls_have_durable_jobs(&sim, &snap).unwrap_err();
        assert!(
            err.starts_with(
                "calls_have_durable_jobs: a call to pay.test with key \
                 09090909090909090909090909090909 reached the remote at 0ns, but no job in the \
                 recovered store carries that key"
            ),
            "{err}"
        );
    }

    #[test]
    fn outbound_settles_by_horizon_names_a_stuck_job() {
        let (_sim, db) = jobs_with_keys(&[[1; 16]]); // Pending
        let snap = db.snapshot().unwrap();
        let err = outbound_settles_by_horizon(&snap).unwrap_err();
        assert!(
            err.starts_with("outbound_settles_by_horizon: job 0 (\"charge\") is still Pending"),
            "{err}"
        );
    }

    /// Each of the five axes at 1.0, quiesced: the call is answered at once
    /// with the model's 200, and no outbound fault marks coverage. Each
    /// axis's own failure differs (an error, no completion, a 429/503, a
    /// reply seconds late), so every one of the five gates is held.
    #[test]
    fn quiesced_outbound_faults_never_fire_again() {
        let on = |set: fn(&mut crate::FaultConfig)| {
            let mut cfg = crate::FaultConfig::none();
            set(&mut cfg);
            cfg
        };
        for (axis, cfg) in [
            ("refused", on(|c| c.outbound_refused = 1.0)),
            ("lost_request", on(|c| c.outbound_lost_request = 1.0)),
            ("lost_reply", on(|c| c.outbound_lost_reply = 1.0)),
            ("throttle", on(|c| c.outbound_throttle = 1.0)),
            ("slow", on(|c| c.outbound_slow = 1.0)),
        ] {
            let sim = Sim::with_faults(5, cfg);
            sim.remote("pay.test", crate::outbound::Recorder::new());
            sim.quiesce_outbound_faults();
            let ob = sim.outbound(&["pay.test"]).unwrap();
            ob.start(0, orphan_call([1; 16])).unwrap();
            let done = ob
                .poll()
                .map(|(t, r)| (t, r.map(|r| r.status).map_err(|e| e.kind())));
            assert_eq!(done, Some((0, Ok(200))), "{axis}");
            let fired: Vec<&str> = sim
                .coverage()
                .into_keys()
                .filter(|k| k.starts_with("fault:outbound_"))
                .collect();
            assert!(fired.is_empty(), "{axis}: {fired:?}");
        }
    }
}
