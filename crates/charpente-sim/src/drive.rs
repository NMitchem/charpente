//! The recovery drive every app that tests recovery under faults was pasting
//! (0.4.0 spec §4 C): a faulted phase, a power cycle, and a tail free of
//! per-call faults that re-sends what went unanswered and runs on until its
//! outbound work settles, one behaviour per step. The app supplies how to
//! build itself and what to send; the decisions that decide runs live here
//! and nowhere else.
//!
//! ```text
//! let drive = Drive::new(sim, build_app);
//! let phase = drive.faulted_phase(&requests, Duration::from_secs(2))?; // steps 1 and 2
//! let Some(recovered) = drive.recover(&phase, Duration::from_secs(30))? else {
//!     return Ok(()); // given up on a durable refusal
//! }; // steps 3 and 4
//! let snap = recovered.kernel.app().db().snapshot().map_err(|e| e.to_string())?;
//! ```
//!
//! Fault fallout means `is_fault_fallout(t) || clock_fallout(sim, t)` in every
//! step ([`crate::checks`]). Nothing here reads a clock or draws entropy of its
//! own: time is `sim`'s, and every draw is the sim's or the kernel's.

use std::io;
use std::time::Duration;

use charpente::core::io::{Clock as _, WallTime};
use charpente::core::job::{JobRow, JobStatus};
use charpente::core::table::View as _;
use charpente::http::{App, Request};
use charpente::kernel::{Kernel, SimSource};

use crate::Sim;
use crate::checks::{
    boot_through_fallout, clock_fallout, is_fault_fallout, mark_dead_coverage,
    wait_out_clock_refusal,
};
use crate::outbound::no_model;

/// How many times [`Drive::recover`]'s tail runs on past its horizon for
/// outbound work that has not settled, before it fails the run by name
/// (0.5.0 spec §5 E). An extension costs a wake-up per retry, not the retry's
/// wait: a `retry(max = 12)` job through a 4 h outage takes 8, and two
/// `on_dead` hooks that re-enqueue on every Dead through the same outage, on
/// the default policy, take 96 (tests/drive.rs).
pub const TAIL_SETTLE_CAP: u32 = 256;

/// The latest wall time an extension runs to: half the u64 nanosecond range,
/// the year 2262. A retry whose doubling backoff saturated is due at the end
/// of the range, where the platform's daily sweep has no next slot and the
/// kernel stops on the overflow, so no run goes near it: the job stays
/// Pending and the tail fails at the cap, by name.
const SETTLE_LIMIT: u64 = u64::MAX / 2;

/// Boots a step tries before it treats the boot as lost to fallout, as the
/// examples it replaces did.
const BOOT_TRIES: u32 = 8;

/// The tail's boot failure. Alone when every boot met fallout with no
/// give-up; followed by `: ` and the error otherwise.
const COULD_NOT_BOOT: &str = "the fault-free tail could not boot";

/// How the app builds itself on a sim, boxed for `Drive` to keep.
type Build<'s> = Box<dyn Fn(&Sim) -> io::Result<App> + 's>;

/// Drives one app through a faulted phase and a fault-free tail on `sim`
/// (GUIDE §8, "A fault-free tail for an end-of-run check"). `build` opens the
/// app on `sim`'s disk and mounts it; the drive calls it once per boot, so it
/// must build the app the same way every time.
pub struct Drive<'s> {
    sim: &'s Sim,
    build: Build<'s>,
}

/// What the faulted phase sent and what came back, request by request.
#[derive(Clone)]
pub struct Phase {
    /// The requests, in the order given, without their offsets.
    pub requests: Vec<Request>,
    /// `answers[i]` is `requests[i]`'s own `(status, body)`, or `None` when
    /// it went unanswered: the phase ended on fault fallout first, or the
    /// first boot never got past fallout and nothing was sent.
    pub answers: Vec<Option<(u16, Vec<u8>)>>,
}

impl Phase {
    /// The status `requests[i]` was answered with, or `None` when it went
    /// unanswered. Panics when `i` is out of range.
    pub fn status(&self, i: usize) -> Option<u16> {
        self.answers[i].as_ref().map(|(status, _)| *status)
    }
}

/// The tail's kernel, after its run, and the answers to what it re-sent.
pub struct Recovered {
    /// The tail's kernel, which keeps its App: read the end state with
    /// `kernel.app().db().snapshot()` and its counters with `kernel.stats()`.
    pub kernel: Kernel,
    /// The node's wall time when the tail ended, after its last extension:
    /// the `at` that `checks::outbound_settles_by_horizon` judges at.
    pub wall: WallTime,
    /// How many times the tail ran on past its horizon for outbound work, at
    /// most [`TAIL_SETTLE_CAP`]; 0 for an app that declares no host.
    pub extensions: u32,
    /// `resent[i]` is `Some` only for a request the tail re-sent, with the
    /// tail's answer to it. It is kept apart from `Phase::answers`, because a
    /// request that went unanswered may or may not have landed: a claim that
    /// holds for an acked request need not hold for a re-sent one. A check
    /// that wants the two merged merges them itself.
    pub resent: Vec<Option<(u16, Vec<u8>)>>,
}

impl Recovered {
    /// The status the tail answered the re-sent `requests[i]` with, or `None`
    /// when it was not re-sent. Panics when `i` is out of range.
    pub fn status(&self, i: usize) -> Option<u16> {
        self.resent[i].as_ref().map(|(status, _)| *status)
    }
}

impl<'s> Drive<'s> {
    /// A drive over `sim` for the app `build` opens. Nothing runs until
    /// [`Drive::faulted_phase`].
    pub fn new(sim: &'s Sim, build: impl Fn(&Sim) -> io::Result<App> + 's) -> Drive<'s> {
        Drive {
            sim,
            build: Box::new(build),
        }
    }

    /// Steps 1 and 2: boot through fallout, then run the requests through a
    /// kernel under the run's faults. Each request is `(offset, request)`, the
    /// offset counted from the phase's start, the instant the app booted, and
    /// the requests must be in arrival order (`SimSource::push_at` asserts
    /// it). The kernel runs until nothing is left inside `horizon` of that
    /// start, or until fault fallout stops it.
    ///
    /// - **Boot.** The app is booted with [`boot_through_fallout`], up to
    ///   eight boots, restarting between them as a supervisor would. A first
    ///   boot that never gets past fallout gives an empty phase, every
    ///   `answers[i]` `None`, and is no give-up: [`Drive::recover`] sends
    ///   everything in the tail. A boot refused for durable corruption, where
    ///   it fired, is given up by `boot_through_fallout` under its own name
    ///   and also gives an empty phase.
    /// - **Faulted phase.** The kernel is built with the outbound handle
    ///   (`Kernel::with_outbound`) exactly when the app declares hosts
    ///   (`App::outbound_hosts`), and plain (`Kernel::new`) otherwise. Fault
    ///   fallout, from building the kernel or from its run, ends the phase:
    ///   it is the crash under test, never a give-up and never a skip, and the
    ///   answers already recorded stand.
    ///
    /// # Errors
    ///
    /// - A boot or kernel error that is not fault fallout: `Err` with the
    ///   error's own text, unchanged.
    /// - An app that declares a host the workload registered no model for
    ///   (`Sim::remote`): `the app calls <host>, but the workload registered
    ///   no model for it — call sim.remote("<host>", …) before
    ///   Drive::faulted_phase`.
    ///
    /// Panics, before anything runs, when a request's offset lies past
    /// `horizon`: the phase would never send it, and it would leave the
    /// faults unnoticed. The text names the last offset, the horizon and the
    /// fix (raise the horizon past the last request, or space the requests
    /// closer). An offset equal to `horizon` is served.
    pub fn faulted_phase(
        &self,
        requests: &[(Duration, Request)],
        horizon: Duration,
    ) -> Result<Phase, String> {
        // The source serves a request at the horizon and none past it, so a
        // later offset would leave the faults silently: its answer stays
        // `None` and only the tail sends it.
        let last = requests.iter().map(|(at, _)| *at).max().unwrap_or_default();
        assert!(
            last <= horizon,
            "contract: Drive::faulted_phase's last request is {last:?} from the phase's start, \
             past its {horizon:?} horizon, so the faulted phase would never send it and only \
             the fault-free tail would — raise the horizon past the last request, or space the \
             requests closer"
        );
        let sim = self.sim;
        let mut phase = Phase {
            requests: requests.iter().map(|(_, r)| r.clone()).collect(),
            answers: vec![None; requests.len()],
        };
        let Some(app) = self.boot().map_err(|e| e.to_string())? else {
            return Ok(phase);
        };
        let start = sim.now().as_nanos();
        let mut source = SimSource::new(Box::new(sim.clock()), after(start, horizon));
        for (at, request) in requests {
            source.push_at(after(start, *at), request.clone());
        }
        let answers = source.answers();
        let hosts = app.outbound_hosts();
        match self.kernel(app, &hosts)? {
            Ok(mut kernel) => match kernel.run(&mut source) {
                Err(e) if !self.fallout(&e.to_string()) => return Err(e.to_string()),
                _ => {}
            },
            Err(e) if !self.fallout(&e.to_string()) => return Err(e.to_string()),
            Err(_) => {}
        }
        phase.answers = answers
            .lock()
            .expect("answers lock poisoned by an earlier panic")
            .clone();
        Ok(phase)
    }

    /// Steps 3 and 4: power-cycle, quiesce, boot, re-send what went
    /// unanswered, and run to `now + tail`, then on until outbound work
    /// settles.
    ///
    /// - **Tail.** Free of per-call faults; a drawn provider outage runs to
    ///   its end. Calls `sim.restart()`, then `sim.quiesce_disk_faults()`
    ///   and `sim.quiesce_clock_faults()` always, and
    ///   `sim.quiesce_outbound_faults()` when the freshly built app declares
    ///   hosts (`App::outbound_hosts`, read before the kernel takes them).
    ///   A drawn `outbound_outage` is not a roll, so it still refuses its
    ///   host's calls until its own end. The boot goes through
    ///   [`wait_out_clock_refusal`], so a node an earlier step back left
    ///   behind its store's recorded time waits past the instant its refusal
    ///   names and boots again. The kernel is built with the outbound handle
    ///   exactly when the app declares hosts. It then runs until nothing is
    ///   left inside `tail` of the instant it booted, so due jobs, retries
    ///   and cron drain. A remote model's own switches (a `calm` flag) stay
    ///   in the app's code.
    /// - **Re-send.** Every request the phase left without an answer is sent
    ///   again, in its original order, all at the tail's start: one whose
    ///   answer is `None`, or a 5xx whose body is fault fallout. A 4xx, or a
    ///   5xx whose body is not fault fallout (a handler's own error), is the
    ///   app's answer and is not re-sent. The 5xx bound matters: fallout is
    ///   read from the body's text ([`is_fault_fallout`]), so an app's own 400
    ///   saying "the dose was injected twice" reads as fallout and is still
    ///   not re-sent. The converse is the classifier's limit: an app's own
    ///   5xx whose message holds one of the phrases it matches ("simulated",
    ///   "injected", "poisoned", "close and re-open the database") is
    ///   re-sent. The re-sent answers come back in
    ///   [`Recovered::resent`], apart from the phase's.
    /// - **Settle.** For an app that declares hosts, the tail then runs on
    ///   until every outbound job settles. After the run to `tail`, and
    ///   after each extension, it reads the store and the node's wall time.
    ///   It stops when every Pending outbound row is not yet due (no attempt
    ///   made, due after that time: a delayed job or a later re-issue).
    ///   Otherwise it runs again, as one extension, to the latest of those
    ///   rows' due times (or now, when one is already due) plus its job's
    ///   declared timeout plus 1 s, so a retry's wait costs a wake-up and the
    ///   app never sizes `tail` to its retry policy. No extension runs past
    ///   the year 2262. An app with no host is never extended.
    ///   [`Recovered::extensions`] counts the extensions, and
    ///   [`Recovered::wall`] is the wall time at the end, what
    ///   `checks::outbound_settles_by_horizon` judges at.
    /// - **Coverage.** Last, [`mark_dead_coverage`] marks `dead:<job>` once
    ///   per Dead row, for every app, so its sim can `require:` that a job's
    ///   Dead path ran.
    ///
    /// `Ok(None)`: the run gave up, on a boot refused for durable corruption
    /// where it fired (`boot_through_fallout`), and on nothing else. The
    /// give-up is named on `sim`; the caller returns `Ok(())`.
    ///
    /// # Errors
    ///
    /// - Every boot met fault fallout, and nothing gave the run up: `the
    ///   fault-free tail could not boot`.
    /// - The kernel's open was refused for the clock a third time, after
    ///   two waits: [`wait_out_clock_refusal`]'s `the boot was refused for the
    ///   clock a third time, … so this is a platform clock defect, not the
    ///   app's: report it with this run's seed. The third refusal: the
    ///   fault-free tail could not boot: <refusal>`. It fails the run rather
    ///   than giving it up, because a skip would hide the defect.
    /// - Any other failure to boot or to build the kernel: `the fault-free
    ///   tail could not boot: <error>`.
    /// - The tail's kernel stopped, in its first run or an extension: `the
    ///   fault-free tail's kernel stopped: <error>`, in the kernel's words. A
    ///   stop whose own text begins `kernel stopped: ` (a poisoned engine, a
    ///   dead store handle) says it once.
    /// - The tail ran [`TAIL_SETTLE_CAP`] extensions and an outbound job is
    ///   still Pending and due: `Drive::recover: the tail ran 256 extensions
    ///   past its horizon and outbound job <id> ("<name>") is still Pending
    ///   at attempt <a> — an outbound job settles, retries to Dead, or waits
    ///   for a later due time; this one did none of them within the cap of
    ///   256`, naming the first such job by id.
    /// - A failed read of the store, after a run, in the store's words.
    /// - An app that declares a host the workload registered no model for:
    ///   the missing-remote text [`Drive::faulted_phase`] gives.
    ///
    /// Panics when `phase` holds a different number of requests and answers.
    pub fn recover(&self, phase: &Phase, tail: Duration) -> Result<Option<Recovered>, String> {
        assert_eq!(
            phase.requests.len(),
            phase.answers.len(),
            "contract: Drive::recover's phase must hold one answer per request — pass the Phase \
             Drive::faulted_phase returned, or one answer (None to send it) per request"
        );
        let sim = self.sim;
        sim.restart();
        sim.quiesce_disk_faults();
        sim.quiesce_clock_faults();
        let booted = wait_out_clock_refusal(sim, || {
            let Some(app) = self.boot().map_err(|e| format!("{COULD_NOT_BOOT}: {e}"))? else {
                return match sim.gave_up() {
                    Some(_) => Ok(None),
                    None => Err(COULD_NOT_BOOT.to_string()),
                };
            };
            let hosts = app.outbound_hosts();
            if !hosts.is_empty() {
                sim.quiesce_outbound_faults();
            }
            match self.kernel(app, &hosts)? {
                Ok(kernel) => Ok(Some((kernel, !hosts.is_empty()))),
                Err(e) => Err(format!("{COULD_NOT_BOOT}: {e}")),
            }
        })?;
        let Some((mut kernel, settles)) = booted else {
            return Ok(None);
        };
        let start = sim.now().as_nanos();
        let mut source = SimSource::new(Box::new(sim.clock()), after(start, tail));
        let resend: Vec<usize> = (0..phase.requests.len())
            .filter(|&i| self.unanswered(&phase.answers[i]))
            .collect();
        for &i in &resend {
            source.push_at(start, phase.requests[i].clone());
        }
        let answers = source.answers();
        kernel.run(&mut source).map_err(stopped)?;
        let mut extensions = 0;
        // Only an app with hosts has outbound rows to wait for, so a hostless
        // tail is never extended.
        if settles {
            loop {
                let wall = sim.clock().wall();
                let Some((reach, row)) = unsettled(kernel.app(), wall)? else {
                    break;
                };
                if extensions == TAIL_SETTLE_CAP {
                    return Err(format!(
                        "Drive::recover: the tail ran {extensions} extensions past its horizon \
                         and outbound job {} (\"{}\") is still Pending at attempt {} — an \
                         outbound job settles, retries to Dead, or waits for a later due time; \
                         this one did none of them within the cap of {TAIL_SETTLE_CAP}",
                        row.id.raw(),
                        row.name,
                        row.attempts
                    ));
                }
                extensions += 1;
                let reach = reach.min(SETTLE_LIMIT.saturating_sub(wall.as_nanos()));
                let horizon = sim.now().as_nanos().saturating_add(reach);
                let mut source = SimSource::new(Box::new(sim.clock()), horizon);
                kernel.run(&mut source).map_err(stopped)?;
            }
        }
        let wall = sim.clock().wall();
        mark_dead_coverage(sim, kernel.app())?;
        let mut resent = vec![None; phase.requests.len()];
        let answers = answers
            .lock()
            .expect("answers lock poisoned by an earlier panic");
        for (&i, answer) in resend.iter().zip(answers.iter()) {
            resent[i] = answer.clone();
        }
        Ok(Some(Recovered {
            kernel,
            resent,
            wall,
            extensions,
        }))
    }

    fn fallout(&self, text: &str) -> bool {
        is_fault_fallout(text) || clock_fallout(self.sim, text)
    }

    /// No answer, or a 5xx whose body is fault fallout.
    fn unanswered(&self, answer: &Option<(u16, Vec<u8>)>) -> bool {
        match answer {
            None => true,
            Some((status, body)) => {
                (500..600).contains(status) && self.fallout(&String::from_utf8_lossy(body))
            }
        }
    }

    fn boot(&self) -> io::Result<Option<App>> {
        boot_through_fallout(
            self.sim,
            BOOT_TRIES,
            |text| self.fallout(text),
            || (self.build)(self.sim),
        )
    }

    /// A kernel over `app`, with the outbound handle exactly when `hosts` is
    /// non-empty. The outer `Err` is the missing-remote contract; the inner
    /// result is the kernel's own, for the caller to judge.
    fn kernel(&self, app: App, hosts: &[&'static str]) -> Result<io::Result<Kernel>, String> {
        let clock = Box::new(self.sim.clock());
        if hosts.is_empty() {
            return Ok(Kernel::new(app, clock));
        }
        let outbound = self.sim.outbound(hosts).map_err(|e| {
            let text = e.to_string();
            match hosts.iter().find(|host| text == no_model(host)) {
                Some(host) => format!(
                    "the app calls {host}, but the workload registered no model for it — call \
                     sim.remote(\"{host}\", …) before Drive::faulted_phase"
                ),
                None => text,
            }
        })?;
        Ok(Kernel::with_outbound(app, clock, Box::new(outbound)))
    }
}

/// The tail kernel's stop, in the kernel's words, said once.
fn stopped(e: io::Error) -> String {
    let text = e.to_string();
    let words = text.strip_prefix("kernel stopped: ").unwrap_or(&text);
    format!("the fault-free tail's kernel stopped: {words}")
}

/// What keeps the tail running at the node's wall time `now`: its Pending
/// outbound rows, less those not yet due (no attempt made, due after `now`).
/// Hands back the first such row by id, and how far past `now` the next run
/// reaches, in nanoseconds: the most, over those rows, of the row's due time
/// (or `now`, once it is due) plus its job's declared timeout plus 1 s, so
/// the latest-due row's call has room to time out. `None` when no row keeps
/// it.
fn unsettled(app: &App, now: WallTime) -> Result<Option<(u64, JobRow)>, String> {
    const SLACK: Duration = Duration::from_secs(1);
    let snap = app.db().snapshot().map_err(|e| e.to_string())?;
    let mut reach: Option<u64> = None;
    let mut first = None;
    for row in snap.scan::<JobRow>().map_err(|e| e.to_string())? {
        let not_yet_due = row.attempts == 0 && row.due_at > now;
        if row.outbound_key.is_none() || row.status != JobStatus::Pending || not_yet_due {
            continue;
        }
        let timeout = app.outbound_timeout_for_sim(&row.name).unwrap_or_default();
        let wait = row.due_at.saturating_duration_since(now) + timeout + SLACK;
        let wait = u64::try_from(wait.as_nanos()).unwrap_or(u64::MAX);
        reach = Some(reach.map_or(wait, |r| r.max(wait)));
        first.get_or_insert(row);
    }
    Ok(reach.zip(first))
}

/// `start` plus `offset` in virtual nanoseconds, saturating: a horizon past
/// the end of virtual time is no horizon.
fn after(start: u64, offset: Duration) -> u64 {
    start.saturating_add(u64::try_from(offset.as_nanos()).unwrap_or(u64::MAX))
}
