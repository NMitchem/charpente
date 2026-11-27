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
//!     return Ok(()); // given up by name
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
use charpente::http::{App, Method, Request};
use charpente::kernel::{Kernel, SimSource};

use crate::Sim;
use crate::checks::{
    boot_through_fallout, calls_have_durable_jobs, clock_fallout, dead_hooks_succeeded,
    durable_corruption_fired, is_fault_fallout, keys_are_per_effect, mark_dead_coverage,
    wait_out_clock_refusal,
};
use crate::outbound::no_model;

/// How many times [`Drive::recover`]'s tail runs on past its horizon for
/// outbound work that has not settled, before it fails the run by name
/// (0.5.0 spec §5 E). An extension costs a wake-up, not the wait: while a
/// drawn provider outage lasts, one reaches the outage's end. Through a 4 h
/// outage a `retry(max = 12)` job takes 2, two `on_dead` chains on the
/// default policy take 1, and a chain on a 63.5 s policy takes 2
/// (tests/drive.rs).
pub const TAIL_SETTLE_CAP: u32 = 256;

/// The latest wall time an extension runs to: half the u64 nanosecond range,
/// the year 2262. A retry whose doubling backoff saturated is due at the end
/// of the range, where the platform's daily sweep has no next slot and the
/// kernel stops on the overflow, so no run goes near it: the job stays
/// Pending, and once one extension has run to the limit (its reach met or
/// passed it, to the nanosecond) the tail fails at once, by its own text
/// naming the row's due time, since another could reach no further.
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
    /// The requests, in the order given, without their offsets, each write
    /// carrying the intent id the phase sent it with.
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
    /// The wall time the tail stopped at, after its last extension: the
    /// greater of the sim clock's reading and the kernel's held wall (the
    /// latest wall time it handed out, seeded at its open from the store's
    /// recorded time), the line the kernel judges due times by. `recover`
    /// returns only once no Pending outbound row is due at it.
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
    /// - **Intent ids.** Before anything boots, every write (any method but
    ///   GET) that carries no `Idempotency-Key` is given one, 16 random bytes
    ///   in hex from a stream of its own, so no fault, input or entropy draw
    ///   moves. [`Phase::requests`] holds the stamped copies, and
    ///   [`Drive::recover`] re-sends those, so a re-sent write carries the id
    ///   its first copy carried and the kernel answers it from the record
    ///   when that copy committed (GUIDE §5, "Retry-safe writes"). A write
    ///   that names its own id keeps it; a route that reads its id from
    ///   another header needs the workload to set that header.
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
        // Stamped once, before the boot: the phase sends these copies, an
        // empty phase hands them back, and `recover` re-sends from
        // `phase.requests`, so every copy of a write carries one intent id.
        let requests: Vec<(Duration, Request)> = requests
            .iter()
            .map(|(at, r)| (*at, self.stamp(r.clone())))
            .collect();
        let mut phase = Phase {
            requests: requests.iter().map(|(_, r)| r.clone()).collect(),
            answers: vec![None; requests.len()],
        };
        let Some(app) = self.boot().map_err(|e| e.to_string())? else {
            return Ok(phase);
        };
        let start = sim.now().as_nanos();
        let mut source = SimSource::new(Box::new(sim.clock()), after(start, horizon));
        for (at, request) in &requests {
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
    /// - **Settle.** The tail then runs on until every outbound job settles.
    ///   After the run to `tail`, and after each extension, it reads the
    ///   store and the wall time the kernel judges due times by: the greater
    ///   of the clock's reading and the kernel's held wall, which its open
    ///   seeds from the store's recorded time. It stops when every Pending
    ///   outbound row is not yet due (no attempt made, due after that time: a
    ///   delayed job or a later re-issue). Otherwise it runs again, as one
    ///   extension, as far as the most, over those rows, of the row's wait
    ///   (to its due time; none once it is due) or the drawn outage's
    ///   remaining length, whichever is longer, plus its job's declared
    ///   timeout plus 1 s. So a retry's wait costs a wake-up, so does an
    ///   outage, and the app never sizes `tail` to its retry policy. No
    ///   extension runs past the year 2262. An app with no host has no
    ///   outbound row and is never extended. [`Recovered::extensions`] counts
    ///   the extensions, and [`Recovered::wall`] is the wall time it stopped
    ///   at. It returns only once no Pending outbound row is due there (its
    ///   `unsettled` loop), so `checks::outbound_settles_by_horizon` cannot
    ///   fail after `recover`.
    /// - **Checks.** Then, on one snapshot of the tail's store, it runs
    ///   [`keys_are_per_effect`], [`calls_have_durable_jobs`] and
    ///   [`dead_hooks_succeeded`], in that order, for every app, and fails
    ///   the run on the first that fails, in its words. A call that reached
    ///   a remote with no durable job, where [`durable_corruption_fired`],
    ///   gives the run up instead: one sim box's only loss window is media
    ///   corruption, which can cut an enqueue whose call was already made.
    ///   A failure outranks a give-up, so it gives that run up only once
    ///   [`dead_hooks_succeeded`] has passed, and a run whose hook failed as
    ///   well fails.
    /// - **Coverage.** Last, [`mark_dead_coverage`] marks `dead:<job>` once
    ///   per Dead row, for every app, so its sim can `require:` that a job's
    ///   Dead path ran.
    ///
    /// `Ok(None)`: the run gave up, on a boot refused for durable corruption
    /// where it fired (`boot_through_fallout`), or on a call that reached a
    /// remote with no durable job where durable corruption fired and no
    /// `on_dead` hook failed, under `injected corruption cut an enqueue after
    /// its call reached the remote` with the check's text as its detail, and
    /// on nothing else. The give-up is named on `sim`; the caller returns
    /// `Ok(())`.
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
    ///   256, so fix the model of the host it calls, or give the job, and the
    ///   chain its on_dead hook makes, a retry policy that ends`, naming the
    ///   first such job by id.
    /// - An extension has run to the year 2262 (its reach met or passed the
    ///   limit) and an outbound job is still Pending and due:
    ///   `Drive::recover: outbound job <id> ("<name>") is still Pending at
    ///   attempt <a>, due <due>, and the tail ran <n> extension<s>, the last to
    ///   the year 2262, past which it does not run — a due time that far out
    ///   means the job's retry backoff grew past the year 2262, so no run
    ///   reaches its next attempt; give the job a retry policy that ends, a
    ///   retry(max = …) small enough that it reaches Dead`, naming the job due
    ///   last.
    /// - A key that carries two requests, or two jobs under one key neither
    ///   of which is Dead: [`keys_are_per_effect`]'s text, `keys_are_per_effect:
    ///   jobs <a> and <b> carry the same idempotency key <key> …`, unchanged.
    /// - A call that reached a remote with a key no job in the store carries,
    ///   where no durable corruption fired: [`calls_have_durable_jobs`]'s
    ///   text, `calls_have_durable_jobs: a call to <host> with key <key>
    ///   reached the remote at <at>ns, but no job in the recovered store
    ///   carries that key …`, unchanged.
    /// - A Dead job whose `on_dead` hook failed: [`dead_hooks_succeeded`]'s
    ///   text, `dead_hooks_succeeded: job <id> ("<name>") is Dead and its
    ///   on_dead hook failed: …`, unchanged. It fails the run even where a
    ///   call with no durable job, under durable corruption, would have given
    ///   it up: a failure outranks a give-up.
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
                Ok(kernel) => Ok(Some(kernel)),
                Err(e) => Err(format!("{COULD_NOT_BOOT}: {e}")),
            }
        })?;
        let Some(mut kernel) = booted else {
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
        let mut ran_to_limit = false;
        let wall = loop {
            // The kernel judges due times by its held wall, which its open
            // seeds from the store's recorded time and so can sit above the
            // clock's reading: "due" is judged at the greater of the two.
            let clock = sim.clock().wall();
            let wall = clock.max(kernel.wall_for_sim());
            let due = unsettled(kernel.app(), wall, clock, outage_left(sim))?;
            let Some(&(reach, ref far)) = due.iter().max_by_key(|(wait, _)| *wait) else {
                break wall;
            };
            // An extension to the limit leaves no reach for the next one, so
            // the last one having run to it fails the tail whatever is due.
            let limit = SETTLE_LIMIT.saturating_sub(clock.as_nanos());
            if ran_to_limit {
                return Err(format!(
                    "Drive::recover: outbound job {} (\"{}\") is still Pending at attempt {}, \
                     due {}, and the tail ran {extensions} extension{}, the last to the year \
                     2262, past which it does not run — a due time that far out means the job's \
                     retry backoff grew past the year 2262, so no run reaches its next attempt; \
                     give the job a retry policy that ends, a retry(max = …) small enough that it \
                     reaches Dead",
                    far.id.raw(),
                    far.name,
                    far.attempts,
                    far.due_at,
                    if extensions == 1 { "" } else { "s" }
                ));
            }
            if extensions == TAIL_SETTLE_CAP {
                let row = &due[0].1;
                return Err(format!(
                    "Drive::recover: the tail ran {extensions} extensions past its horizon and \
                     outbound job {} (\"{}\") is still Pending at attempt {} — an outbound job \
                     settles, retries to Dead, or waits for a later due time; this one did none \
                     of them within the cap of {TAIL_SETTLE_CAP}, so fix the model of the host it \
                     calls, or give the job, and the chain its on_dead hook makes, a retry policy \
                     that ends",
                    row.id.raw(),
                    row.name,
                    row.attempts
                ));
            }
            extensions += 1;
            ran_to_limit = reach >= limit;
            let horizon = sim.now().as_nanos().saturating_add(reach.min(limit));
            let mut source = SimSource::new(Box::new(sim.clock()), horizon);
            kernel.run(&mut source).map_err(stopped)?;
        };
        let snap = kernel.app().db().snapshot().map_err(|e| e.to_string())?;
        keys_are_per_effect(&snap)?;
        let orphan = match calls_have_durable_jobs(sim, &snap) {
            Ok(()) => None,
            Err(e) if durable_corruption_fired(sim) => Some(e),
            Err(e) => return Err(e),
        };
        // A failure outranks a give-up, so the hooks are judged before the
        // corruption excuses the orphan call.
        dead_hooks_succeeded(&snap)?;
        if let Some(e) = orphan {
            sim.give_up(
                "injected corruption cut an enqueue after its call reached the remote",
                e,
            );
            return Ok(None);
        }
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

    /// A write (any method but GET) without an `Idempotency-Key` gets one
    /// from [`Sim::next_intent`]; a GET, or a write that names its own id,
    /// is sent as given.
    fn stamp(&self, request: Request) -> Request {
        if request.method == Method::Get || request.headers.contains_key("idempotency-key") {
            return request;
        }
        let id = self.sim.next_intent();
        request.intent(id)
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

/// What keeps the tail running at the wall time `now`: the app's Pending
/// outbound rows, less those not yet due (no attempt made, due after `now`).
/// Hands back each such row, in id order, with how far a run must reach for
/// it, in nanoseconds: the row's wait (to its due time, measured from
/// `clock`, the clock's reading; none once it is due) or the drawn outage's
/// remaining length (`outage_left`), whichever is longer, plus its job's
/// declared timeout plus 1 s, so the row's next call starts once both have
/// passed and has room to time out. The next run reaches the most of these.
/// Empty when no row keeps it.
fn unsettled(
    app: &App,
    now: WallTime,
    clock: WallTime,
    outage_left: Duration,
) -> Result<Vec<(u64, JobRow)>, String> {
    const SLACK: Duration = Duration::from_secs(1);
    let snap = app.db().snapshot().map_err(|e| e.to_string())?;
    let mut due = Vec::new();
    for row in snap.scan::<JobRow>().map_err(|e| e.to_string())? {
        let not_yet_due = row.attempts == 0 && row.due_at > now;
        if row.outbound_key.is_none() || row.status != JobStatus::Pending || not_yet_due {
            continue;
        }
        let timeout = app.outbound_timeout_for_sim(&row.name).unwrap_or_default();
        let wait = row.due_at.saturating_duration_since(clock).max(outage_left);
        let wait = u64::try_from((wait + timeout + SLACK).as_nanos()).unwrap_or(u64::MAX);
        due.push((wait, row));
    }
    Ok(due)
}

/// Virtual time left in the universe's drawn provider outage
/// (`FaultConfig::outbound_outage`): none when none was drawn (a length of
/// zero) or once it has ended.
fn outage_left(sim: &Sim) -> Duration {
    let (start, length) = sim.world.cfg.outbound_outage;
    if length.is_zero() {
        return Duration::ZERO;
    }
    (start + length).saturating_sub(Duration::from_nanos(sim.now().as_nanos()))
}

/// `start` plus `offset` in virtual nanoseconds, saturating: a horizon past
/// the end of virtual time is no horizon.
fn after(start: u64, offset: Duration) -> u64 {
    start.saturating_add(u64::try_from(offset.as_nanos()).unwrap_or(u64::MAX))
}

#[cfg(test)]
mod tests {
    use super::*;
    use charpente::Schema;
    use charpente::http::AppConfig;
    use charpente::kernel::JobError;
    use charpente::outbound::{Call, Host, IdempotencyKey, Reply};

    #[charpente::job(global, retain_dead, outbound = "due.test")]
    struct Due {}

    impl Due {
        fn call(
            &self,
            _tx: &charpente::Tx,
            _key: &IdempotencyKey,
        ) -> Result<Option<Call>, JobError> {
            Ok(Some(Call::post("/x")))
        }

        fn settle(self, _tx: &mut charpente::Tx, _reply: Reply) -> Result<(), JobError> {
            Ok(())
        }
    }

    /// A row with no attempt made keeps the tail running from the wall its
    /// due time names: "not yet due" is due after the wall, never at it, as
    /// the kernel reads a due time. Left out at its own instant, the row would
    /// end the tail Pending with no attempt (the mutation sweep's 455:59).
    #[test]
    fn a_row_due_at_the_wall_keeps_the_tail_running() {
        // Fault-free, so the seed only names the universe.
        let sim = Sim::with_faults(0xD7_0B08, crate::FaultConfig::none());
        let mut app = App::open(
            Box::new(sim.disk()),
            Schema::new(),
            Box::new(sim.entropy()),
            AppConfig::default(),
        )
        .unwrap()
        .mount_jobs(charpente::jobs![Due])
        .outbound(Host::new("due.test"));
        let due = WallTime::from_nanos(5_000_000_000);
        app.transact_for_tests(|tx| {
            tx.enqueue_at(due, Due {});
            Ok::<(), io::Error>(())
        })
        .unwrap();
        let before = WallTime::from_nanos(due.as_nanos() - 1);
        assert!(
            unsettled(&app, before, before, Duration::ZERO)
                .unwrap()
                .is_empty(),
            "a nanosecond before its due time the row is not yet due"
        );
        let rows = unsettled(&app, due, due, Duration::ZERO).unwrap();
        let [(reach, row)] = &rows[..] else {
            panic!(
                "at its due time the row is due, and keeps the tail running: {} rows",
                rows.len()
            );
        };
        assert_eq!(
            (row.name.as_str(), row.attempts, row.due_at),
            ("Due", 0, due)
        );
        let timeout = app.outbound_timeout_for_sim("Due").unwrap();
        assert_eq!(
            *reach,
            u64::try_from((timeout + Duration::from_secs(1)).as_nanos()).unwrap(),
            "due now, it waits only its timeout and the slack"
        );
    }
}
