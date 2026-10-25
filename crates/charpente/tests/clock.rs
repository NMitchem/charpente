//! The time properties (0.3.0 spec §5 B). One workload drives a single node
//! through several lives under all three sim clock faults (skew, step-backs,
//! downtime) and records two things: what the node did, and what its clock
//! read at every step. A second drives a primary and a follower, each on its
//! own host and clock, and promotes the follower once the primary is gone.
//! Four properties judge those records against each clock's readings, the
//! guarantee the node makes, with the true-time form (`Sim::true_wall`)
//! derived through each clock's offset, and each carries `require` keys
//! proving its axes fired. A naive victim, a clock that reads process uptime,
//! fails the TTL and stall properties by name.
//!
//! Every bound below is written through a clock's *offset*: its reading minus
//! true wall time at that instant. It is never written through the universe's
//! `clock_skew`, because a clock's error is its skew plus its step-backs plus
//! its downtime jumps, and the node can see none of them. A sub-tolerance step
//! is held and then caught up, so step-backs accumulate without limit, and a
//! clock can run any distance behind true time with every check passing.
//!
//! What the node guarantees is relative to its own clock. Take `R` as the
//! clock's reading at a step and `W` as the wall time the kernel hands out
//! there. Then `R <= W <= R + CLOCK_TOLERANCE` at every step that does not
//! stop:
//! - the hold takes the max of the reading and the held value;
//! - a reading more than the tolerance behind the held value stops the node;
//! - a reading at or behind the held value more than the tolerance of
//!   virtual time after the held value last rose stops it too (a stall), so
//!   `W` sits above `R` for at most the tolerance at a stretch;
//! - an open whose reading is more than the tolerance behind the floor the
//!   store recorded is refused, and otherwise starts the hold at that floor.
//!
//! Each property carries that band through to sessions, schedules and retries.
//! The boundary property holds the band itself, on both workloads.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use charpente::auth::sweep::SESSION_SWEEP_NAME;
use charpente::auth::{ApiKey, hash_token};
use charpente::http::{ApiError, App, AppConfig, Ctx, Method, Request, Session};
use charpente::kernel::{
    CronRow, JobError, Kernel, RecordedResponses, RequestSource, SimSource, Step, Wake,
};
use charpente::repl::protocol::{Msg, decode, encode};
use charpente::repl::role::Role;
use charpente::repl::{MAX_MESSAGE_BYTES, Repl, ReplConfig};
use charpente::{Db, Job, Schema, Tx, View, job, jobs, route, routes};
use charpente_core::io::{CLOCK_TOLERANCE, Clock, Instant, Net, WallTime};
use charpente_core::job::{JobRow, JobStatus};
use charpente_sim::rng::{Rng, splitmix64};
use charpente_sim::{
    FaultConfig, SIM_WALL_BASE_NANOS, Sim, SimClock, SimDisk, SimEntropy, SimHost,
    run_universes_swarm_with,
};

const SEC: u64 = 1_000_000_000;
const TOL: u64 = CLOCK_TOLERANCE.as_nanos() as u64;
/// Short enough that sessions expire inside a life and across a restart.
const TTL: u64 = 20 * SEC;
const BEAT: &str = "beat";
const BEAT_EVERY: u64 = SEC;
const LIVES: u32 = 5;
const OPEN_ATTEMPTS: u32 = 8;
const STEP_BUDGET: u32 = 200_000;
const BASE: u64 = 0xC10C_0001;
const UNIVERSES: u64 = 256;

const TTL_PROPERTY: &str = "no_session_resolves_past_its_ttl_plus_tolerance";
const STALL_PROPERTY: &str = "a_schedule_never_stalls_past_one_interval_plus_tolerance";
const RETRY_PROPERTY: &str = "no_retry_runs_before_its_due_at";
const BOUNDARY_PROPERTY: &str =
    "the_stop_and_the_refusal_fire_exactly_when_the_clock_is_beyond_tolerance";

// ------------------------------------------------------------------- app

#[route(POST "/login", public)]
fn login(tx: &mut Tx, ctx: &Ctx) -> Result<String, ApiError> {
    Ok(ctx.create_session(tx, "alice"))
}

#[route(GET "/me", auth)]
fn me(tx: &mut Tx, ctx: &Ctx) -> Result<String, ApiError> {
    let _ = tx;
    Ok(ctx.principal().expect("auth route").subject().to_string())
}

/// An API key's `created_at` is one of the times the open-time floor reads.
#[route(POST "/key", public)]
fn mint_key(tx: &mut Tx, ctx: &Ctx) -> Result<String, ApiError> {
    Ok(ctx.create_api_key(tx, "alice"))
}

#[route(POST "/flaky", public)]
fn enqueue_flaky(tx: &mut Tx) -> Result<(), ApiError> {
    tx.enqueue(Flaky {});
    Ok(())
}

/// Fails every attempt, so each run past the first is a retry at the
/// `due_at` its predecessor's failure record set.
#[job(global, retry(max = 4, backoff = "2s"))]
struct Flaky {}

impl Flaky {
    fn run(self, _tx: &mut Tx) -> Result<(), JobError> {
        Err(JobError::app("flaky by design"))
    }
}

/// What the one-second schedule enqueues.
#[job(global)]
struct Beat {}

impl Beat {
    fn run(self, _tx: &mut Tx) -> Result<(), JobError> {
        Ok(())
    }
}

fn open_app(disk: SimDisk, entropy: SimEntropy) -> std::io::Result<App> {
    Ok(App::open(
        Box::new(disk),
        Schema::new(),
        Box::new(entropy),
        AppConfig {
            session_ttl: TTL,
            ..AppConfig::default()
        },
    )?
    .mount(routes![login, me, mint_key, enqueue_flaky])
    .mount_jobs(jobs![Flaky, Beat])
    .schedule(BEAT, Duration::from_nanos(BEAT_EVERY), || Beat {}))
}

/// The disk axes off and all three clock axes on. The properties judge time;
/// the disk axes have their own suites, and with them on most universes end
/// in IO fallout before a clock fault is reached. Crash collapse at a power
/// cycle stays as sampled. The draws come from a stream keyed by the
/// universe's seed alone, so no change to `FaultConfig` moves them. Each clock
/// draws its own skew within plus or minus `clock_skew`, so only its size is
/// drawn here.
fn clock_axes(seed: u64, cfg: &mut FaultConfig) {
    let mut state = seed ^ 0xC10C_A8E5;
    let mut rng = Rng::new(splitmix64(&mut state));
    cfg.write_error = 0.0;
    cfg.sync_error = 0.0;
    cfg.dir_entry_loss = 0.0;
    cfg.bit_flip = 0.0;
    cfg.misdirected_write = 0.0;
    cfg.read_error = 0.0;
    cfg.clock_skew = rng.between(0, 120 * SEC) as i64;
    cfg.clock_step_back = (
        rng.between(5, 80) as f64 / 10_000.0,
        Duration::from_secs(120),
    );
    cfg.clock_downtime = (0.5, Duration::from_secs(3 * 86_400));
}

// ----------------------------------------------------------------- trace

/// One wall read the kernel made: the virtual time, what the host's clock
/// read, and what the kernel was handed. The kernel's value is the host's,
/// except under the uptime victim.
#[derive(Clone, Copy, Debug)]
struct Read {
    mono: u64,
    host: u64,
    kernel: u64,
}

impl Read {
    /// `Sim::true_wall` at this read: the virtual time plus the base, which
    /// `TracedClock` checks against the sim's own offset at every read.
    fn true_wall(&self) -> u64 {
        SIM_WALL_BASE_NANOS + self.mono
    }

    /// The host clock's offset from true wall time.
    fn offset(&self) -> i128 {
        i128::from(self.host) - i128::from(self.true_wall())
    }
}

/// The host's clock, as the kernel reads it. It records each wall read, the
/// kernel's one per step and one per open, and draws nothing of its own.
/// The victim (`uptime_since`) still reads the host clock, so its universe
/// rolls the same step-backs, and hands the kernel process uptime on the
/// sim's wall scale instead.
struct TracedClock {
    inner: SimClock,
    uptime_since: Option<u64>,
    reads: Arc<Mutex<Vec<Read>>>,
}

impl Clock for TracedClock {
    fn now(&self) -> Instant {
        self.inner.now()
    }

    fn wall(&self) -> WallTime {
        let host = self.inner.wall().as_nanos();
        let mono = self.inner.now().as_nanos();
        assert_eq!(
            i128::from(host) - i128::from(self.inner.offset()),
            i128::from(SIM_WALL_BASE_NANOS + mono),
            "rig: a reading minus its clock's offset is not Sim::true_wall"
        );
        let kernel = match self.uptime_since {
            None => host,
            Some(start) => SIM_WALL_BASE_NANOS + (mono - start),
        };
        self.reads
            .lock()
            .expect("reads lock")
            .push(Read { mono, host, kernel });
        WallTime::from_nanos(kernel)
    }

    fn sleep(&self, duration: Duration) {
        self.inner.sleep(duration);
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Seen {
    Resolved,
    Expired,
    Unrecognized,
}

/// What the node did, in order. `read` indexes `Trace::reads`: the read of
/// the open or the step the event happened at.
#[derive(Debug)]
enum Event {
    /// The floor is the store's recorded time, read before the kernel. The
    /// beat's `next_due` is read once the node is open.
    Open {
        read: usize,
        floor: Option<u64>,
        beat_due: Option<u64>,
        refused: Option<String>,
    },
    /// `idle` is `Some(deadline)` for a step that went idle.
    Step {
        read: usize,
        idle: Option<Option<u64>>,
        stopped: Option<String>,
    },
    Tick {
        read: usize,
        schedule: &'static str,
        from: u64,
        to: u64,
    },
    Mint {
        read: usize,
        token: usize,
        expires_at: u64,
    },
    Key {
        read: usize,
        created_at: u64,
    },
    Present {
        read: usize,
        token: usize,
        seen: Seen,
    },
    /// `to` is the failure record's new `due_at`, `None` once dead.
    Attempt {
        read: usize,
        job: u64,
        attempts: u32,
        from: u64,
        to: Option<u64>,
    },
    /// A promote's own read: `do_promote` reads the clock past the hold and
    /// checks it against the store's floor before the durable role flip.
    /// `accepted` is whether the write gate opened at that step; `refused` is
    /// the `CtrlErr` text, filled in when the reply reaches the operator.
    Promote {
        read: usize,
        floor: Option<u64>,
        accepted: bool,
        refused: Option<String>,
    },
    /// The hold starting over where the write gate opens (`after_repl_step`,
    /// a promote's or a hand-off's): a read through the hold, then the floor
    /// check with `promote` in its text.
    Start {
        read: usize,
        floor: Option<u64>,
        stopped: Option<String>,
    },
}

struct Trace {
    reads: Vec<Read>,
    events: Vec<Event>,
}

// -------------------------------------------------------------- workload

#[derive(Clone, Copy)]
enum Ask {
    Login,
    Me(usize),
    Key,
    Flaky,
}

enum End {
    Stopped,
    Horizon,
}

/// What a universe's lives share: the reads, the events, the tokens minted
/// so far, and each flaky job's last seen (attempts, due_at).
struct Rig {
    victim: bool,
    rng: Rng,
    reads: Arc<Mutex<Vec<Read>>>,
    events: Vec<Event>,
    tokens: Vec<String>,
    jobs: BTreeMap<u64, (u32, u64)>,
}

impl Rig {
    fn reads_len(&self) -> usize {
        self.reads.lock().expect("reads lock").len()
    }

    fn read(&self, i: usize) -> Read {
        self.reads.lock().expect("reads lock")[i]
    }

    /// The trace associates each event with the one read its step made.
    fn one_read(&self, before: usize, site: &str) -> Result<(), String> {
        let after = self.reads_len();
        if after == before + 1 {
            Ok(())
        } else {
            Err(format!(
                "rig: {site} read the wall clock {} times, and this trace assumes exactly once",
                after - before
            ))
        }
    }
}

/// Up to `LIVES` lives of one node. A life ends at its horizon (a power cycle
/// or a process restart, then the node stays down a while) or when the
/// kernel stops by name (a process restart). The universe ends early if the
/// node never opens.
fn time_universe(sim: &mut Sim, victim: bool) -> Result<Trace, String> {
    let mut rig = Rig {
        victim,
        rng: Rng::new(sim.seed() ^ 0xC10C_7135),
        reads: Arc::new(Mutex::new(Vec::new())),
        events: Vec::new(),
        tokens: Vec::new(),
        jobs: BTreeMap::new(),
    };
    for _ in 0..LIVES {
        let Some(mut kernel) = open_node(sim, &mut rig)? else {
            break;
        };
        let end = run_life(sim, &mut kernel, &mut rig)?;
        drop(kernel);
        match end {
            End::Stopped => sim.restart_process(),
            End::Horizon if rig.rng.chance(0.6) => sim.restart(),
            End::Horizon => sim.restart_process(),
        }
        sim.clock()
            .sleep(Duration::from_millis(rig.rng.below(30_000)));
    }
    let reads = rig.reads.lock().expect("reads lock").clone();
    Ok(Trace {
        reads,
        events: rig.events,
    })
}

/// The latest time the store recorded, by the definition GUIDE §7 gives:
/// the newest API key's `created_at`, and each schedule's `next_due -
/// interval_nanos`.
fn recorded_floor(db: &Db) -> Result<Option<u64>, String> {
    let snap = db.snapshot().map_err(|e| format!("snapshot: {e}"))?;
    let keys = snap.scan::<ApiKey>().map_err(|e| format!("scan: {e}"))?;
    let rows = snap.scan::<CronRow>().map_err(|e| format!("scan: {e}"))?;
    Ok(keys
        .iter()
        .map(|k| k.created_at.as_nanos())
        .chain(
            rows.iter()
                .map(|r| r.next_due.as_nanos().saturating_sub(r.interval_nanos)),
        )
        .max())
}

fn schedule_due(db: &Db, name: &str) -> Result<u64, String> {
    let snap = db.snapshot().map_err(|e| format!("snapshot: {e}"))?;
    CronRow::by_name(&snap, &name.to_string())
        .map_err(|e| format!("by_name: {e}"))?
        .pop()
        .map(|row| row.next_due.as_nanos())
        .ok_or_else(|| format!("rig: no cron row for \"{name}\" on an open node"))
}

/// Opens the node, waiting out a refusal for the time it names (what an
/// operator whose clock is right does). `None` once `OPEN_ATTEMPTS` refusals
/// pass: the uptime victim never opens over a floor more than the tolerance
/// past its restart, since its clock starts over at every attempt.
fn open_node(sim: &Sim, rig: &mut Rig) -> Result<Option<Kernel>, String> {
    for _ in 0..OPEN_ATTEMPTS {
        let app = open_app(sim.disk(), sim.entropy()).map_err(|e| format!("App::open: {e}"))?;
        let floor = recorded_floor(app.db())?;
        let read = rig.reads_len();
        let clock = TracedClock {
            inner: sim.clock(),
            uptime_since: rig.victim.then(|| sim.now().as_nanos()),
            reads: rig.reads.clone(),
        };
        let opened = Kernel::new(app, Box::new(clock));
        rig.one_read(read, "Kernel::new")?;
        match opened {
            Ok(mut kernel) => {
                // `Kernel::run`'s preamble, since this rig drives `step`.
                kernel
                    .app_mut_for_tests()
                    .db_for_tests()
                    .migrate_schema()
                    .map_err(|e| format!("migrate_schema: {e}"))?;
                let beat_due = schedule_due(kernel.app().db(), BEAT)?;
                rig.events.push(Event::Open {
                    read,
                    floor,
                    beat_due: Some(beat_due),
                    refused: None,
                });
                return Ok(Some(kernel));
            }
            Err(e) => {
                let text = e.to_string();
                if !text.starts_with("refusing to open: this node's wall clock reads ") {
                    return Err(format!(
                        "Kernel::new failed, and only the clock's faults are on: {text}"
                    ));
                }
                sim.cover("clock:refused");
                sim.cover("clock:refused:open");
                rig.events.push(Event::Open {
                    read,
                    floor,
                    beat_due: None,
                    refused: Some(text),
                });
                let after = floor.unwrap_or(0).saturating_sub(TOL);
                let wait = after.saturating_sub(rig.read(read).kernel) + SEC;
                sim.clock().sleep(Duration::from_nanos(wait));
                sim.restart_process();
            }
        }
    }
    Ok(None)
}

fn pick(rng: &mut Rng, tokens: usize) -> Ask {
    if tokens == 0 {
        return Ask::Login;
    }
    match rng.below(100) {
        0..30 => Ask::Login,
        30..85 if rng.chance(0.5) => {
            let recent = rng.below(tokens.min(16) as u64) as usize;
            Ask::Me(tokens - 1 - recent)
        }
        30..85 => Ask::Me(rng.below(tokens as u64) as usize),
        85..90 => Ask::Key,
        _ => Ask::Flaky,
    }
}

fn request(ask: Ask, tokens: &[String]) -> Request {
    match ask {
        Ask::Login => Request::new(Method::Post, "/login"),
        Ask::Me(i) => Request::new(Method::Get, "/me")
            .header("Authorization", format!("Bearer {}", tokens[i])),
        Ask::Key => Request::new(Method::Post, "/key"),
        Ask::Flaky => Request::new(Method::Post, "/flaky"),
    }
}

/// One life: requests one at a time, at random gaps, until the horizon, with
/// the kernel stepped by hand so every response, tick and retry is tied to
/// the read of the step it happened at.
fn run_life(sim: &Sim, kernel: &mut Kernel, rig: &mut Rig) -> Result<End, String> {
    let start = sim.now().as_nanos();
    let horizon = start + rig.rng.between(20, 100) * SEC;
    let mut source = SimSource::new(Box::new(sim.clock()), horizon);
    let responses = source.responses();
    let mut asked: Option<Ask> = None;
    let mut next_at = start;
    let mut answered = 0usize;
    let mut stats = kernel.stats();
    let mut beat = schedule_due(kernel.app().db(), BEAT)?;
    let mut sweep = schedule_due(kernel.app().db(), SESSION_SWEEP_NAME)?;
    for _ in 0..STEP_BUDGET {
        if asked.is_none() && next_at <= horizon {
            next_at += rig.rng.between(100, 1_000) * 1_000_000;
            if next_at <= horizon {
                let ask = pick(&mut rig.rng, rig.tokens.len());
                source.push_at(next_at, request(ask, &rig.tokens));
                asked = Some(ask);
            }
        }
        let read = rig.reads_len();
        let step = kernel.step(&mut source);
        rig.one_read(read, "Kernel::step")?;
        let step = match step {
            Ok(step) => step,
            Err(e) => {
                let text = e.to_string();
                if !clock_stop(&text) {
                    return Err(format!(
                        "the kernel stopped, and only the clock's faults are on: {text}"
                    ));
                }
                sim.cover("clock:refused");
                sim.cover("clock:refused:stop");
                rig.events.push(Event::Step {
                    read,
                    idle: None,
                    stopped: Some(text),
                });
                return Ok(End::Stopped);
            }
        };
        if let Step::Idle { deadline } = step {
            rig.events.push(Event::Step {
                read,
                idle: Some(deadline.map(Instant::as_nanos)),
                stopped: None,
            });
            if let Wake::Shutdown = source.wait_until(deadline) {
                return Ok(End::Horizon);
            }
            continue;
        }
        rig.events.push(Event::Step {
            read,
            idle: None,
            stopped: None,
        });
        let response = {
            let recorded = responses.lock().expect("responses lock");
            (recorded.len() > answered).then(|| recorded[answered].clone())
        };
        if let Some((status, body)) = response {
            answered += 1;
            let ask = asked.take().ok_or("rig: a response with nothing asked")?;
            answer(
                kernel,
                &mut rig.events,
                &mut rig.tokens,
                read,
                ask,
                status,
                &body,
            )?;
        }
        let now = kernel.stats();
        if now.cron_ticks != stats.cron_ticks {
            for (schedule, due) in [(BEAT, &mut beat), (SESSION_SWEEP_NAME, &mut sweep)] {
                let to = schedule_due(kernel.app().db(), schedule)?;
                if to != *due {
                    if schedule == BEAT && to > *due + BEAT_EVERY {
                        sim.cover("cron:caught-up");
                    }
                    rig.events.push(Event::Tick {
                        read,
                        schedule,
                        from: *due,
                        to,
                    });
                    *due = to;
                }
            }
        }
        if now.jobs_retried != stats.jobs_retried || now.jobs_dead != stats.jobs_dead {
            let snap = kernel
                .app()
                .db()
                .snapshot()
                .map_err(|e| format!("snapshot: {e}"))?;
            let rows = snap.scan::<JobRow>().map_err(|e| format!("scan: {e}"))?;
            for row in rows.iter().filter(|r| r.name == Flaky::NAME) {
                let id = row.id.raw();
                let (attempts, due) = rig.jobs.get(&id).copied().unwrap_or((0, 0));
                if row.attempts == attempts {
                    continue;
                }
                if row.attempts != attempts + 1 {
                    return Err(format!(
                        "rig: flaky job {id} went from {attempts} to {} attempts between two \
                         steps",
                        row.attempts
                    ));
                }
                rig.events.push(Event::Attempt {
                    read,
                    job: id,
                    attempts: row.attempts,
                    from: due,
                    to: (row.status == JobStatus::Pending).then(|| row.due_at.as_nanos()),
                });
                rig.jobs.insert(id, (row.attempts, row.due_at.as_nanos()));
            }
        }
        stats = now;
    }
    Err(format!("rig: a life ran {STEP_BUDGET} steps"))
}

fn answer(
    kernel: &Kernel,
    events: &mut Vec<Event>,
    tokens: &mut Vec<String>,
    read: usize,
    ask: Ask,
    status: u16,
    body: &[u8],
) -> Result<(), String> {
    let text = String::from_utf8_lossy(body);
    let unexpected = || format!("unexpected {status} answering a request: {text}");
    match ask {
        Ask::Login | Ask::Key if status != 200 => Err(unexpected()),
        Ask::Login => {
            let token: String =
                serde_json::from_slice(body).map_err(|e| format!("login body: {e}"))?;
            let snap = kernel
                .app()
                .db()
                .snapshot()
                .map_err(|e| format!("snapshot: {e}"))?;
            let session = Session::by_token_hash(&snap, &hash_token(&token))
                .map_err(|e| format!("by_token_hash: {e}"))?
                .pop()
                .ok_or("an acknowledged login left no session row")?;
            events.push(Event::Mint {
                read,
                token: tokens.len(),
                expires_at: session.expires_at.as_nanos(),
            });
            tokens.push(token);
            Ok(())
        }
        Ask::Key => {
            let raw: String = serde_json::from_slice(body).map_err(|e| format!("key body: {e}"))?;
            let snap = kernel
                .app()
                .db()
                .snapshot()
                .map_err(|e| format!("snapshot: {e}"))?;
            let key = ApiKey::by_key_hash(&snap, &hash_token(&raw))
                .map_err(|e| format!("by_key_hash: {e}"))?
                .pop()
                .ok_or("an acknowledged key mint left no key row")?;
            events.push(Event::Key {
                read,
                created_at: key.created_at.as_nanos(),
            });
            Ok(())
        }
        Ask::Me(token) => {
            let seen = match status {
                200 => Seen::Resolved,
                401 if text.contains("session expired") => Seen::Expired,
                401 if text.contains("unrecognized credential") => Seen::Unrecognized,
                _ => return Err(unexpected()),
            };
            events.push(Event::Present { read, token, seen });
            Ok(())
        }
        Ask::Flaky if status == 200 => Ok(()),
        Ask::Flaky => Err(unexpected()),
    }
}

/// The kernel's two clock stops: a reading too far behind the hold, or a
/// hold in effect for too long.
fn clock_stop(text: &str) -> bool {
    text.starts_with("clock went backwards: this node's wall clock reads ")
        || text.starts_with("clock stalled: this node's wall clock has not advanced past ")
}

fn wall(nanos: u64) -> WallTime {
    WallTime::from_nanos(nanos)
}

fn signed(nanos: i128) -> String {
    let d = Duration::from_nanos(nanos.unsigned_abs() as u64);
    if nanos < 0 {
        format!("-{d:?}")
    } else {
        format!("+{d:?}")
    }
}

// --------------------------------------------------------------- the hold

/// What the hold does with a read through it, given the held value `h` and
/// the virtual time it rose to that value at (`WallHold::read`).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Rule {
    /// More than the tolerance behind `h`: the node stops.
    Backwards,
    /// At or behind `h` more than the tolerance after `h` rose: the node
    /// stops, after this long.
    Stalled(u64),
    /// Otherwise: the max of the reading and `h`.
    Hands(u64),
}

fn rule(h: u64, raised: u64, r: Read) -> Rule {
    if r.kernel < h.saturating_sub(TOL) {
        Rule::Backwards
    } else if r.kernel <= h && r.mono - raised > TOL {
        Rule::Stalled(r.mono - raised)
    } else {
        Rule::Hands(h.max(r.kernel))
    }
}

/// The hold at one read, replayed from the readings the kernel was handed:
/// the held value before the read and the virtual time it rose to it (`None`
/// at an open, where the hold starts over), and the wall time the node handed
/// out there (`None` where the read stopped the node or was refused, and at a
/// promote's own read, which goes past the hold). It follows what the node
/// did, so one wrong decision does not cascade; the boundary property judges
/// each decision against the rule, and the others use `handed` as the node's
/// own time.
#[derive(Clone, Copy)]
struct Held {
    before: Option<(u64, u64)>,
    handed: Option<u64>,
}

fn replay_hold(t: &Trace) -> Result<Vec<Held>, String> {
    let mut out: Vec<Held> = Vec::with_capacity(t.reads.len());
    // The held value and the virtual time it rose to it.
    let mut held: Option<(u64, u64)> = None;
    for event in &t.events {
        let (read, before, after) = match event {
            Event::Open {
                read,
                floor,
                refused,
                ..
            } => {
                let r = t.reads[*read];
                let after = refused
                    .is_none()
                    .then(|| (floor.map_or(r.kernel, |f| r.kernel.max(f)), r.mono));
                (*read, None, after)
            }
            Event::Step { read, stopped, .. } => {
                let (h, raised) = held.ok_or("rig: a step with no open node")?;
                let r = t.reads[*read];
                let after = match (stopped, rule(h, raised, r)) {
                    (Some(_), _) => None,
                    (None, Rule::Hands(w)) if w > h => Some((w, r.mono)),
                    (None, Rule::Hands(_)) => Some((h, raised)),
                    // The node ran on where the rule stops it: follow it.
                    (None, _) => Some((h.max(r.kernel), raised)),
                };
                (*read, Some((h, raised)), after)
            }
            Event::Promote { read, .. } => {
                let before = held.ok_or("rig: a promote with no open node")?;
                if *read != out.len() {
                    return Err(format!("rig: read {read} is not the next of {}", out.len()));
                }
                out.push(Held {
                    before: Some(before),
                    handed: None,
                });
                continue;
            }
            Event::Start {
                read,
                floor,
                stopped,
            } => {
                let (h, raised) = held.ok_or("rig: a hold start with no open node")?;
                let r = t.reads[*read];
                let after = stopped.is_none().then(|| {
                    let (w, at) = match rule(h, raised, r) {
                        Rule::Hands(w) if w > h => (w, r.mono),
                        _ => (h.max(r.kernel), raised),
                    };
                    match floor {
                        Some(f) if *f > w => (*f, r.mono),
                        _ => (w, at),
                    }
                });
                (*read, Some((h, raised)), after)
            }
            _ => continue,
        };
        if read != out.len() {
            return Err(format!("rig: read {read} is not the next of {}", out.len()));
        }
        out.push(Held {
            before,
            handed: after.map(|(w, _)| w),
        });
        held = after;
    }
    if out.len() != t.reads.len() {
        return Err("rig: a wall read belongs to no open, step or promote".to_string());
    }
    Ok(out)
}

/// The wall time the node handed out at `read`.
fn handed(hold: &[Held], read: usize) -> Result<u64, String> {
    hold[read]
        .handed
        .ok_or_else(|| format!("rig: an event at read {read}, which stopped the node"))
}

// ------------------------------------------------------------ properties

/// The TTL property, derived from the band.
///
/// A session minted at a step with clock reading `R_m` and handed-out time
/// `W_m` stores `expires_at = E = W_m + ttl`. It resolves at a later step,
/// with reading `R_r` and handed-out time `W_r`, exactly while `W_r < E`. The
/// band `R <= W <= R + tol` at both steps gives the checks:
/// - **(L)** Resolved implies `R_r <= W_r < E`: a session never outlives its
///   expiry on the clock that judges it, since the node never hands out a time
///   behind its clock. This is the sharp form, and the one an uptime clock
///   breaks: that kernel's time sits behind the host's clock.
/// - **(T)** Resolved implies `R_r < E <= R_m + tol + ttl`. With `off = R -
///   true_wall` per clock, that is `true_r < true_m + ttl + tol + (off_m -
///   off_r)`: a session resolves no later than its TTL plus the tolerance
///   past its true mint time, plus whatever the judging clock lost against
///   the minting one. The offset difference replaces the brief's `max(0,
///   skew)`, whose sign was the wrong way (a resolver behind true time
///   extends a session) and which no single skew can fill, since step-backs
///   accumulate and `expires_at` is on the minting clock's scale. No restart
///   term is needed once the offsets are explicit: the step after an open
///   still has `W <= R + tol`, because the open refuses unless `floor <= R +
///   tol`. Without the offsets, the change across a restart is bounded only
///   by that refusal (the time since the store's latest recorded time, plus
///   the tolerance), which is GUIDE §7's weaker bound.
/// - **(Er)** Refused as expired implies `E <= W_r <= R_r + tol`: never more
///   than the tolerance early on the judging clock.
/// - **(F)** Every `W` in a life is at least the floor that life opened over,
///   so a session that expired at or before the store's recorded time never
///   resolves after the restart.
/// - **(D)** The decision is exactly `W_r < E`, with `W_r` the replayed hold.
///   This also holds the hold within a life: `W` never decreases, so a
///   session refused as expired never resolves again before a restart.
///
/// A `401 unrecognized` for a minted token means the daily sweep deleted it,
/// which it does for `expires_at <= before`, where `before` is the tick's own
/// `W_tick`, so some sweep tick before it must have handed out `W_tick >=
/// expires_at` (the replayed hold's value, exactly). This catches a session
/// lost with no sweep behind it. It cannot catch a sweep that deletes early:
/// here the daily sweep ticks only right after a downtime jump, which has
/// already expired every session it could reach. The sweep's `before` is
/// pinned in tests/kernel.rs.
fn check_ttl(sim: &Sim, t: &Trace) -> Result<(), String> {
    let hold = replay_hold(t)?;
    let mut life = 0u32;
    let mut floor: Option<u64> = None;
    let mut minted: Vec<(Read, u32, u64)> = Vec::new();
    let mut swept_through = 0u64;
    for event in &t.events {
        match event {
            Event::Open {
                floor: f,
                refused: None,
                ..
            } => {
                life += 1;
                floor = *f;
            }
            Event::Mint {
                read,
                token,
                expires_at,
            } => {
                assert_eq!(minted.len(), *token, "tokens are minted in order");
                minted.push((t.reads[*read], life, *expires_at));
            }
            Event::Tick { read, schedule, .. } if *schedule == SESSION_SWEEP_NAME => {
                swept_through = swept_through.max(handed(&hold, *read)?);
            }
            Event::Present { read, token, seen } => {
                let r = t.reads[*read];
                let (m, minted_in, e) = minted[*token];
                let session = || {
                    format!(
                        "session {token}, minted in life {minted_in} at true wall {} by a clock \
                         reading {} (offset {}), expires at {}; presented in life {life} at \
                         true wall {} to a clock reading {} (offset {})",
                        wall(m.true_wall()),
                        wall(m.host),
                        signed(m.offset()),
                        wall(e),
                        wall(r.true_wall()),
                        wall(r.host),
                        signed(r.offset()),
                    )
                };
                match seen {
                    Seen::Resolved => {
                        if r.host >= e {
                            return Err(format!(
                                "{TTL_PROPERTY}: a session resolved after its expiry on the \
                                 clock that judged it (L): {}. The node handed out a time \
                                 behind its own clock.",
                                session()
                            ));
                        }
                        if r.host >= m.host + TTL + TOL {
                            return Err(format!(
                                "{TTL_PROPERTY}: a session resolved past its TTL plus the \
                                 tolerance (T): {}. The minting step handed out a time more \
                                 than the tolerance ahead of its clock.",
                                session()
                            ));
                        }
                        if let Some(f) = floor.filter(|&f| e <= f) {
                            return Err(format!(
                                "{TTL_PROPERTY}: a session that expired at or before the \
                                 store's recorded time {} resolved after the restart (F): {}",
                                wall(f),
                                session()
                            ));
                        }
                        sim.cover("session:resolved");
                        if minted_in != life {
                            sim.cover("session:resolved-after-restart");
                        }
                    }
                    Seen::Expired => {
                        if r.host + TOL < e {
                            return Err(format!(
                                "{TTL_PROPERTY}: a session was refused as expired more than \
                                 the tolerance before its expiry on the clock that judged it \
                                 (Er): {}",
                                session()
                            ));
                        }
                        sim.cover("session:expired");
                    }
                    Seen::Unrecognized => {
                        if swept_through < e {
                            return Err(format!(
                                "{TTL_PROPERTY}: a session vanished before any sweep could \
                                 delete it (the latest a sweep reached was {}): {}",
                                wall(swept_through),
                                session()
                            ));
                        }
                        sim.cover("session:swept");
                    }
                }
                let w = handed(&hold, *read)?;
                if *seen != Seen::Unrecognized && (w < e) != (*seen == Seen::Resolved) {
                    return Err(format!(
                        "{TTL_PROPERTY}: the node's own time {} decided {seen:?} for a session \
                         expiring at {} (D): {}",
                        wall(w),
                        wall(e),
                        session()
                    ));
                }
            }
            _ => {}
        }
    }
    Ok(())
}

/// The stall property, derived from the band, for the one-second schedule
/// `beat`. A tick at a step with handed-out time `W_k` sets `next_due` to the
/// first slot on the schedule's grid after `W_k`, so `next_due - interval <=
/// W_k < next_due`, and `W_k <= R_k + tol`. At an open, `next_due - interval`
/// is at most the floor, which is at most the open's `W`: the same inequality,
/// and what bounds a restart's first tick. The schedule is due exactly when
/// `W >= next_due`, which `R >= next_due` implies. The checks:
/// - **(S1)** The node never goes idle while the schedule is due on its
///   clock. An uptime clock breaks this: its kernel's time is held at the
///   floor while the host's clock moves on.
/// - **(S2)** An idle node parks no later than the instant its clock reaches
///   the slot, `deadline <= mono + (next_due - R)`: the kernel parks until
///   `mono + (next_due - W)`, and `W >= R`.
/// - **(S3)** In true time, two ticks in one life (or the open and the first
///   tick) are at most `interval + tol + (off_k - off_p) + busy` apart. Here
///   `p` is the last idle step before the tick, `off` a clock's reading minus
///   true wall time, and `busy` the true time from the wake after `p` to the
///   tick. By S2 the wake comes by `next_due - off_p`, which is at most
///   `true_k + interval + tol + off_k - off_p`, and after it the node never
///   idled. This is the brief's "never more than one interval plus the
///   tolerance apart", made exact by adding back the clock's own loss to
///   step-backs and the node's busy time. A downtime jump moves the clock
///   forward, so it only shortens a gap.
/// - **(S4)** A tick fires only once the node's own time reaches its slot
///   (`W >= next_due`; on its clock, at most the tolerance early).
/// - **(C)** Catch-up fires once: the new `next_due` is on the grid and is
///   the first slot after the node's time, never an older slot that leaves
///   the schedule due again.
fn check_stall(sim: &Sim, t: &Trace) -> Result<(), String> {
    struct Life {
        due: u64,
        anchor: Read,
        last_idle: Option<usize>,
    }
    let hold = replay_hold(t)?;
    let mut life: Option<Life> = None;
    for event in &t.events {
        match event {
            Event::Open {
                read,
                beat_due: Some(due),
                refused: None,
                ..
            } => {
                let w = handed(&hold, *read)?;
                if due.saturating_sub(BEAT_EVERY) > w {
                    return Err(format!(
                        "{STALL_PROPERTY}: a node opened at {} with its schedule's next slot \
                         {} more than one interval past it (C)",
                        wall(w),
                        wall(*due)
                    ));
                }
                life = Some(Life {
                    due: *due,
                    anchor: t.reads[*read],
                    last_idle: None,
                });
            }
            Event::Open { .. } => life = None,
            Event::Step {
                stopped: Some(_), ..
            } => life = None,
            Event::Step {
                read,
                idle: Some(deadline),
                ..
            } => {
                let l = life.as_mut().ok_or("rig: a step with no open node")?;
                let r = t.reads[*read];
                if r.host >= l.due {
                    return Err(format!(
                        "{STALL_PROPERTY}: the node went idle with its schedule due on its \
                         clock (S1): the slot was {}, and at true wall {} the host's clock read \
                         {} (offset {}) while the node's time was {}",
                        wall(l.due),
                        wall(r.true_wall()),
                        wall(r.host),
                        signed(r.offset()),
                        wall(handed(&hold, *read)?)
                    ));
                }
                let wake_by = r.mono + (l.due - r.host);
                if deadline.is_none_or(|d| d > wake_by) {
                    return Err(format!(
                        "{STALL_PROPERTY}: the node parked past the instant its clock reaches \
                         the schedule's slot {} (S2): deadline {deadline:?}, the clock gets \
                         there at virtual {wake_by}",
                        wall(l.due)
                    ));
                }
                l.last_idle = Some(*read);
            }
            Event::Tick {
                read,
                schedule,
                from,
                to,
            } if *schedule == BEAT => {
                let l = life.as_mut().ok_or("rig: a tick with no open node")?;
                if *from != l.due {
                    return Err(format!(
                        "rig: the tick moved next_due from {}, and the trace had {}",
                        wall(*from),
                        wall(l.due)
                    ));
                }
                let r = t.reads[*read];
                let w = handed(&hold, *read)?;
                if w < *from {
                    return Err(format!(
                        "{STALL_PROPERTY}: a tick fired at the node's {}, before its slot {} \
                         (S4)",
                        wall(w),
                        wall(*from)
                    ));
                }
                if r.host + TOL < *from {
                    return Err(format!(
                        "{STALL_PROPERTY}: a tick fired more than the tolerance before its slot \
                         {} on its clock, which read {} at true wall {} (S4)",
                        wall(*from),
                        wall(r.host),
                        wall(r.true_wall())
                    ));
                }
                if *to <= *from || (*to - *from) % BEAT_EVERY != 0 {
                    return Err(format!(
                        "{STALL_PROPERTY}: a tick moved next_due off the schedule's grid, \
                         from {} to {} (C)",
                        wall(*from),
                        wall(*to)
                    ));
                }
                if *to <= w || *to - BEAT_EVERY > w {
                    return Err(format!(
                        "{STALL_PROPERTY}: a catch-up did not fire once: a tick at the node's \
                         {} set the next slot to {}, not the first slot after it (C)",
                        wall(w),
                        wall(*to)
                    ));
                }
                if let Some(p) = l.last_idle {
                    let wake = t.reads[p + 1];
                    let gap = i128::from(r.true_wall() - l.anchor.true_wall());
                    let busy = i128::from(r.true_wall() - wake.true_wall());
                    let loss = l.anchor.offset() - t.reads[p].offset();
                    let bound = i128::from(BEAT_EVERY + TOL) + loss + busy;
                    if gap > bound {
                        return Err(format!(
                            "{STALL_PROPERTY}: ticks {:?} apart in true wall time, past one \
                             interval plus the tolerance, the clock's loss ({}) and {:?} busy \
                             (S3): from true wall {} to {}",
                            Duration::from_nanos(gap as u64),
                            signed(loss),
                            Duration::from_nanos(busy as u64),
                            wall(l.anchor.true_wall()),
                            wall(r.true_wall())
                        ));
                    }
                }
                sim.cover("cron:ticked");
                l.due = *to;
                l.anchor = r;
                l.last_idle = None;
            }
            _ => {}
        }
    }
    Ok(())
}

/// The retry property, by the node's own held wall reading. A failed attempt
/// at a step with handed-out time `W` writes `due_at = W + backoff(attempts)`,
/// so each record names the `W` its attempt ran at. That `W` must be the
/// replayed hold's, and at least the `due_at` the attempt before it set. The
/// last attempt writes no `due_at`, so for it only the replayed `W` is
/// checked against the previous `due_at`.
fn check_retry(sim: &Sim, t: &Trace) -> Result<(), String> {
    let hold = replay_hold(t)?;
    let mut life = 0u32;
    let mut ran_in: BTreeMap<u64, u32> = BTreeMap::new();
    for event in &t.events {
        match event {
            Event::Open { refused: None, .. } => life += 1,
            Event::Attempt {
                read,
                job,
                attempts,
                from,
                to,
            } => {
                let w = handed(&hold, *read)?;
                let ran_at = match to {
                    Some(due) => {
                        let ran_at = due
                            .checked_sub(Flaky::RETRY.backoff_nanos(*attempts))
                            .ok_or("rig: a due_at below its own backoff")?;
                        if ran_at != w {
                            return Err(format!(
                                "{RETRY_PROPERTY}: job {job}'s attempt {attempts} recorded its \
                                 run at {}, and the node's held time at that step was {}",
                                wall(ran_at),
                                wall(w)
                            ));
                        }
                        ran_at
                    }
                    None => w,
                };
                if ran_at < *from {
                    return Err(format!(
                        "{RETRY_PROPERTY}: job {job}'s attempt {attempts} ran at the node's {}, \
                         before its due_at {}",
                        wall(ran_at),
                        wall(*from)
                    ));
                }
                if *attempts >= 2 {
                    sim.cover("job:retried");
                    if ran_in.get(job) != Some(&life) {
                        sim.cover("job:retried-after-restart");
                    }
                }
                ran_in.insert(*job, life);
            }
            _ => {}
        }
    }
    Ok(())
}

/// The boundary property, two-sided, on the readings the kernel was handed.
/// The rule:
/// - an open is refused exactly when its reading is more than the tolerance
///   behind the store's recorded floor;
/// - a step stops exactly when its reading is more than the tolerance behind
///   the held value, or is at or behind it more than the tolerance of virtual
///   time after the held value rose (`Rule`);
/// - a promote is refused exactly when its own read, which goes past the
///   hold, is more than the tolerance behind the store's floor, and otherwise
///   opens the write gate;
/// - where the write gate opens, the hold reads by the step's rule, and then
///   refuses exactly when the time it would hand out is more than the
///   tolerance behind the floor.
///
/// The checks:
/// - each stop and refusal fires whenever the rule says it must, and never
///   otherwise;
/// - its text names the reading or held time, the gap, and the floor;
/// - between stops, every time the node hands out (a session's `expires_at -
///   ttl`, a key's `created_at`) is the replayed hold's.
///
/// Wherever a workload excuses a clock stop or refusal as a fault's fallout,
/// this property is the oracle that none fires without a clock beyond the
/// tolerance behind it.
fn check_boundary(sim: &Sim, t: &Trace) -> Result<(), String> {
    let hold = replay_hold(t)?;
    let gave = |site: &str, read: usize, w: u64| -> Result<(), String> {
        let replayed = handed(&hold, read)?;
        if replayed == w {
            Ok(())
        } else {
            Err(format!(
                "{BOUNDARY_PROPERTY}: the node handed out {} for {site}, and the hold replayed \
                 from its readings says {}",
                wall(w),
                wall(replayed)
            ))
        }
    };
    // The stop the hold's rule calls for at `read`, if any, as the text it
    // must start with.
    let stop = |read: usize| -> Result<Option<String>, String> {
        let (h, raised) = hold[read].before.ok_or("rig: a step with no open node")?;
        let r = t.reads[read];
        Ok(match rule(h, raised, r) {
            Rule::Backwards => Some(format!(
                "clock went backwards: this node's wall clock reads {}, {:?} behind {}, ",
                wall(r.kernel),
                Duration::from_nanos(h - r.kernel),
                wall(h)
            )),
            Rule::Stalled(d) => Some(format!(
                "clock stalled: this node's wall clock has not advanced past {}, the latest \
                 time it already used, for {:?} of elapsed time ",
                wall(h),
                Duration::from_nanos(d)
            )),
            Rule::Hands(_) => None,
        })
    };
    let refusal = |verb: &str, r: u64, floor: Option<u64>| -> Option<String> {
        floor.filter(|&f| r < f.saturating_sub(TOL)).map(|f| {
            format!(
                "refusing to {verb}: this node's wall clock reads {}, {:?} behind {}, ",
                wall(r),
                Duration::from_nanos(f - r),
                wall(f)
            )
        })
    };
    // Each decision against the rule, both ways. `at` says what the rule
    // was given there.
    let judge = |site: &str,
                 at: String,
                 due: Option<String>,
                 fired: Option<&String>|
     -> Result<(), String> {
        match (due, fired) {
            (Some(named), Some(text)) if text.starts_with(&named) => Ok(()),
            (Some(named), Some(text)) => Err(format!(
                "{BOUNDARY_PROPERTY}: the {site} at {at} does not name its reading, held time \
                 or floor (expected it to start {named:?}): {text}"
            )),
            (Some(named), None) => Err(format!(
                "{BOUNDARY_PROPERTY}: the {site} at {at} went on, and the rule stops or \
                 refuses it there with {named:?}"
            )),
            (None, Some(text)) => Err(format!(
                "{BOUNDARY_PROPERTY}: the {site} at {at} stopped or was refused within the \
                 tolerance: {text}"
            )),
            (None, None) => Ok(()),
        }
    };
    let held_at = |read: usize| -> String {
        let r = t.reads[read];
        match hold[read].before {
            Some((h, raised)) => format!(
                "true wall {}, its clock reading {}, holding {} since {:?} before",
                wall(r.true_wall()),
                wall(r.kernel),
                wall(h),
                Duration::from_nanos(r.mono - raised)
            ),
            None => format!(
                "true wall {}, its clock reading {}",
                wall(r.true_wall()),
                wall(r.kernel)
            ),
        }
    };
    let over = |read: usize, floor: Option<u64>| -> String {
        match floor {
            Some(f) => format!("{} over the floor {}", held_at(read), wall(f)),
            None => format!("{} over no floor", held_at(read)),
        }
    };
    for event in &t.events {
        match event {
            Event::Open {
                read,
                floor,
                refused,
                ..
            } => {
                let r = t.reads[*read].kernel;
                judge(
                    "open",
                    over(*read, *floor),
                    refusal("open", r, *floor),
                    refused.as_ref(),
                )?;
                if refused.is_none() && floor.is_some_and(|f| r < f) {
                    sim.cover("clock:held:open");
                }
            }
            Event::Step { read, stopped, .. } => {
                judge("step", held_at(*read), stop(*read)?, stopped.as_ref())?;
                let (h, raised) = hold[*read].before.ok_or("rig: a step with no open node")?;
                let r = t.reads[*read];
                match (stopped, rule(h, raised, r)) {
                    (Some(_), Rule::Stalled(_)) => sim.cover("clock:stalled"),
                    (None, _) if r.kernel < h => sim.cover("clock:held"),
                    _ => {}
                }
            }
            Event::Promote {
                read,
                floor,
                accepted,
                refused,
            } => {
                let r = t.reads[*read].kernel;
                let due = refusal("promote", r, *floor);
                // The reply can be lost on its way to the operator; the
                // write gate is the node's own answer.
                if due.is_some() == *accepted {
                    return Err(format!(
                        "{BOUNDARY_PROPERTY}: a promote at {} {} the write gate",
                        over(*read, *floor),
                        if *accepted {
                            "opened, more than the tolerance behind,"
                        } else {
                            "kept shut, within the tolerance,"
                        }
                    ));
                }
                if refused.is_some() {
                    judge("promote", over(*read, *floor), due, refused.as_ref())?;
                    sim.cover("clock:refused:promote");
                } else if floor.is_some_and(|f| r < f) {
                    sim.cover("clock:held:promote");
                }
            }
            Event::Start {
                read,
                floor,
                stopped,
            } => {
                let due = match stop(*read)? {
                    Some(named) => Some(named),
                    None => {
                        let (h, _) = hold[*read].before.ok_or("rig: no open node")?;
                        refusal("promote", h.max(t.reads[*read].kernel), *floor)
                    }
                };
                judge(
                    "write gate's hold start",
                    over(*read, *floor),
                    due,
                    stopped.as_ref(),
                )?;
            }
            Event::Mint {
                read, expires_at, ..
            } => gave("a session", *read, expires_at - TTL)?,
            Event::Key { read, created_at } => gave("an API key", *read, *created_at)?,
            _ => {}
        }
    }
    Ok(())
}

// --------------------------------------------------------------- promote

const PROMOTE_ATTEMPTS: u32 = 8;
const ROUND_BUDGET: u32 = 20_000;

/// One node of the promote workload, on its own host: its own disk, its own
/// clock (skew, step-backs, downtime), and its own trace. The source and its
/// responses outlive any one kernel.
struct Node {
    host: SimHost,
    cfg: ReplConfig,
    kernel: Option<Kernel>,
    source: SimSource,
    responses: RecordedResponses,
    asked: Vec<Ask>,
    tokens: Vec<String>,
    reads: Arc<Mutex<Vec<Read>>>,
    events: Vec<Event>,
    /// A promote has been sent and its reply not yet seen.
    promoting: bool,
    /// The `Promote` event awaiting that reply.
    promote: Option<usize>,
    /// Still a follower: no promote has been accepted.
    follower: bool,
}

impl Node {
    fn new(sim: &Sim, name: &str, listen: &str, initial_role: Role) -> Node {
        let follower = matches!(initial_role, Role::Follower { .. });
        let source = SimSource::new(Box::new(sim.clock()), u64::MAX);
        let responses = source.responses();
        Node {
            host: sim.host(name),
            cfg: ReplConfig {
                listen: Some(listen.to_string()),
                initial_role,
                peer_secret: None,
                strict_witnesses: false,
            },
            kernel: None,
            source,
            responses,
            asked: Vec::new(),
            tokens: Vec::new(),
            reads: Arc::new(Mutex::new(Vec::new())),
            events: Vec::new(),
            promoting: false,
            promote: None,
            follower,
        }
    }

    fn reads_len(&self) -> usize {
        self.reads.lock().expect("reads lock").len()
    }

    fn last_applied(&self) -> Option<u64> {
        self.kernel
            .as_ref()
            .and_then(|k| k.app().db().last_applied())
    }

    fn into_trace(self) -> Trace {
        Trace {
            reads: self.reads.lock().expect("reads lock").clone(),
            events: self.events,
        }
    }
}

/// Opens `node`'s kernel over its host's store, waiting out a refusal for
/// the time it names and power-cycling the host before the next attempt.
/// `false` once `OPEN_ATTEMPTS` refusals pass.
fn open_replicated(sim: &Sim, node: &mut Node) -> Result<bool, String> {
    for _ in 0..OPEN_ATTEMPTS {
        let mut app = open_app(node.host.disk(), node.host.entropy())
            .map_err(|e| format!("App::open on {}: {e}", node.host.name()))?;
        let (repl, _) = Repl::open(
            Box::new(node.host.net()),
            Box::new(node.host.disk()),
            node.cfg.clone(),
            app.db_for_tests(),
        )
        .map_err(|e| format!("Repl::open on {}: {e}", node.host.name()))?;
        let floor = recorded_floor(app.db())?;
        let read = node.reads_len();
        let clock = TracedClock {
            inner: node.host.clock(),
            uptime_since: None,
            reads: node.reads.clone(),
        };
        let opened = Kernel::with_replication(app, Box::new(clock), repl);
        if node.reads_len() != read + 1 {
            return Err(format!(
                "rig: Kernel::with_replication read the wall clock {} times",
                node.reads_len() - read
            ));
        }
        match opened {
            Ok(mut kernel) => {
                if kernel.app().db().writes_allowed() {
                    kernel
                        .app_mut_for_tests()
                        .db_for_tests()
                        .migrate_schema()
                        .map_err(|e| format!("migrate_schema: {e}"))?;
                }
                node.events.push(Event::Open {
                    read,
                    floor,
                    beat_due: None,
                    refused: None,
                });
                node.kernel = Some(kernel);
                return Ok(true);
            }
            Err(e) => {
                let text = e.to_string();
                if !text.starts_with("refusing to open: this node's wall clock reads ") {
                    return Err(format!(
                        "Kernel::with_replication failed on {}, and only the clock's and the \
                         network's faults are on: {text}",
                        node.host.name()
                    ));
                }
                sim.cover("clock:refused");
                sim.cover("clock:refused:open");
                if node.follower {
                    sim.cover("clock:refused:open:follower");
                }
                node.events.push(Event::Open {
                    read,
                    floor,
                    beat_due: None,
                    refused: Some(text),
                });
                let after = floor.unwrap_or(0).saturating_sub(TOL);
                let reading = node.reads.lock().expect("reads lock")[read].kernel;
                sim.clock()
                    .sleep(Duration::from_nanos(after.saturating_sub(reading) + SEC));
                node.host.restart();
            }
        }
    }
    Ok(false)
}

/// Steps `node` once and records what the step read. A step reads the wall
/// clock once through the hold; a promote reads it again, past the hold, and
/// a write gate opening reads it once more through the hold. `None` when the
/// node is down, or stopped at this step.
fn step_node(sim: &Sim, node: &mut Node) -> Result<Option<Step>, String> {
    let reads_len = |reads: &Arc<Mutex<Vec<Read>>>| reads.lock().expect("reads lock").len();
    let Some(kernel) = node.kernel.as_mut() else {
        return Ok(None);
    };
    let gated = !kernel.app().db().writes_allowed();
    let floor = if gated {
        recorded_floor(kernel.app().db())?
    } else {
        None
    };
    let read = reads_len(&node.reads);
    let answered = node.responses.lock().expect("responses lock").len();
    let step = kernel.step(&mut node.source);
    let n = reads_len(&node.reads) - read;
    // `floor`, read before the step, is the one both checks read: the step
    // that runs a control verb applies no record before it (`Repl::step`
    // returns from `begin_control`), a primary applies none, and the
    // schedules are upserted only after the hold starts.
    let opened = gated && kernel.app().db().writes_allowed();
    let stopped = match &step {
        Ok(_) => None,
        Err(e) => {
            let text = e.to_string();
            if !clock_stop(&text)
                && !text.starts_with("refusing to promote: this node's wall clock reads ")
            {
                return Err(format!(
                    "the kernel on {} stopped, and only the clock's and the network's faults \
                     are on: {text}",
                    node.host.name()
                ));
            }
            Some(text)
        }
    };
    let idle = match &step {
        Ok(Step::Idle { deadline }) => Some(deadline.map(Instant::as_nanos)),
        _ => None,
    };
    if opened != (n > 1 && (n == 3 || !node.promoting)) {
        return Err(format!(
            "rig: a step on {} read the wall clock {n} times, and the write gate {} \
             (promoting: {})",
            node.host.name(),
            if opened { "opened" } else { "stayed as it was" },
            node.promoting
        ));
    }
    match (n, node.promoting) {
        (1, _) => node.events.push(Event::Step {
            read,
            idle,
            stopped: stopped.clone(),
        }),
        (2, true) if stopped.is_none() => {
            node.events.push(Event::Step {
                read,
                idle,
                stopped: None,
            });
            node.promote = Some(node.events.len());
            node.events.push(Event::Promote {
                read: read + 1,
                floor,
                accepted: opened,
                refused: None,
            });
        }
        (2, false) => {
            node.events.push(Event::Step {
                read,
                idle,
                stopped: None,
            });
            node.events.push(Event::Start {
                read: read + 1,
                floor,
                stopped: stopped.clone(),
            });
        }
        (3, true) => {
            node.events.push(Event::Step {
                read,
                idle,
                stopped: None,
            });
            node.promote = Some(node.events.len());
            node.events.push(Event::Promote {
                read: read + 1,
                floor,
                accepted: true,
                refused: None,
            });
            node.events.push(Event::Start {
                read: read + 2,
                floor,
                stopped: stopped.clone(),
            });
        }
        _ => {
            return Err(format!(
                "rig: a step on {} read the wall clock {n} times (promoting: {}, stopped: \
                 {stopped:?})",
                node.host.name(),
                node.promoting
            ));
        }
    }
    if let Some(text) = &stopped {
        sim.cover("clock:refused");
        sim.cover(if text.starts_with("refusing to promote") {
            "clock:refused:start"
        } else {
            "clock:refused:stop"
        });
        node.kernel = None;
        return Ok(None);
    }
    let step = step.expect("checked above");
    let response = {
        let recorded = node.responses.lock().expect("responses lock");
        (recorded.len() > answered).then(|| recorded[answered].clone())
    };
    if let Some((status, body)) = response {
        if node.asked.len() <= answered {
            return Err("rig: a response with nothing asked".to_string());
        }
        let ask = node.asked[answered];
        let kernel = node.kernel.as_ref().expect("checked above");
        if status == 503 && String::from_utf8_lossy(&body).contains("not accepting writes yet") {
            // A primary that restarted serves no write until its follower
            // confirms it was never promoted; the write gate opening is
            // judged where it happens (`Event::Start`).
            sim.cover("promote:write-gated");
            return Ok(Some(step));
        }
        answer(
            kernel,
            &mut node.events,
            &mut node.tokens,
            read,
            ask,
            status,
            &body,
        )?;
    }
    Ok(Some(step))
}

/// One round over the live nodes: each steps once. When none worked, virtual
/// time moves to the earliest kernel deadline or message delivery, and never
/// past `limit`. A node that stopped is power-cycled and reopened. `false`
/// once a stopped node never reopens.
fn round(sim: &Sim, nodes: &mut [&mut Node], limit: u64) -> Result<bool, String> {
    let mut worked = false;
    let mut deadlines: Vec<u64> = Vec::new();
    for node in nodes.iter_mut() {
        let live = node.kernel.is_some();
        match step_node(sim, node)? {
            Some(Step::Worked) => worked = true,
            Some(Step::Idle { deadline }) => deadlines.extend(deadline.map(Instant::as_nanos)),
            None if live => {
                node.host.restart();
                if !open_replicated(sim, node)? {
                    return Ok(false);
                }
                worked = true;
            }
            None => {}
        }
    }
    if worked {
        return Ok(true);
    }
    let now = sim.now().as_nanos();
    let next = deadlines
        .into_iter()
        .chain(sim.next_wakeup())
        .filter(|&d| d > now)
        .min()
        .map_or(limit, |d| d.min(limit));
    if next > now {
        sim.clock().sleep(Duration::from_nanos(next - now));
    }
    Ok(true)
}

/// Rounds until `done`, or until virtual time reaches `until`. `None` once a
/// stopped node never reopens.
fn drive(
    sim: &Sim,
    nodes: &mut [&mut Node],
    until: u64,
    done: impl Fn(&[&mut Node]) -> bool,
) -> Result<Option<bool>, String> {
    for _ in 0..ROUND_BUDGET {
        if done(nodes) {
            return Ok(Some(true));
        }
        if sim.now().as_nanos() >= until {
            return Ok(Some(false));
        }
        if !round(sim, nodes, until)? {
            return Ok(None);
        }
    }
    Err(format!(
        "rig: {ROUND_BUDGET} rounds without reaching {}",
        wall(until)
    ))
}

/// Asks `nodes[0]` for `ask` and drives every node until it answers, then
/// for a random gap. `false` when it does not answer in 30 s, or a node never
/// reopens: the caller stops asking, since the answers are matched to the
/// asks in order.
fn ask(sim: &Sim, rng: &mut Rng, nodes: &mut [&mut Node], ask: Ask) -> Result<bool, String> {
    let i = nodes[0].asked.len();
    nodes[0].asked.push(ask);
    let now = sim.now().as_nanos();
    nodes[0].source.push_at(now, request(ask, &[]));
    let answered = |n: &[&mut Node]| n[0].responses.lock().expect("responses lock").len() > i;
    if drive(sim, nodes, now + 30 * SEC, answered)? != Some(true) {
        return Ok(false);
    }
    let gap = sim.now().as_nanos() + rng.between(200, 3_000) * 1_000_000;
    Ok(drive(sim, nodes, gap, |_| false)?.is_some())
}

/// Promotes `f` through the control verb, as an operator does after the
/// primary is lost: asks, and waits out a refusal for the time it names, up
/// to `PROMOTE_ATTEMPTS` times.
fn promote(sim: &Sim, f: &mut Node, addr: &str) -> Result<(), String> {
    let mut buf = vec![0u8; MAX_MESSAGE_BYTES];
    for _ in 0..PROMOTE_ATTEMPTS {
        if f.kernel.is_none() {
            return Ok(());
        }
        let sent = sim.host("cli").net().connect(addr).and_then(|conn| {
            conn.send(&encode(&Msg::CtrlPromote { secret: None }))?;
            Ok(conn)
        });
        let Ok(conn) = sent else {
            let until = sim.now().as_nanos() + SEC;
            if drive(sim, &mut [&mut *f], until, |_| false)?.is_none() {
                return Ok(());
            }
            continue;
        };
        f.promoting = true;
        f.promote = None;
        let until = sim.now().as_nanos() + 30 * SEC;
        let mut reply = None;
        for _ in 0..ROUND_BUDGET {
            if sim.now().as_nanos() >= until || !round(sim, &mut [&mut *f], until)? {
                break;
            }
            match conn.recv(&mut buf) {
                Ok(Some(0)) | Err(_) => break,
                Ok(Some(n)) => {
                    reply = Some(decode(&buf[..n]).map_err(|e| format!("ctrl reply: {e}"))?);
                    break;
                }
                Ok(None) => {}
            }
        }
        f.promoting = false;
        match reply {
            // The boundary property judges the promote from the node's own
            // step (`Event::Promote`); the reply only ends the attempts.
            Some(Msg::CtrlOk { .. }) => {
                sim.cover("clock:promoted");
                f.follower = false;
                return Ok(());
            }
            Some(Msg::CtrlErr { message })
                if message == "cannot promote: this node is already the primary" =>
            {
                // An earlier attempt's answer was lost on its way back.
                f.follower = false;
                return Ok(());
            }
            Some(Msg::CtrlErr { message })
                if message.starts_with("refusing to promote: this node's wall clock reads ") =>
            {
                let i = f
                    .promote
                    .take()
                    .ok_or("rig: a promote refusal with no promote read")?;
                let Event::Promote {
                    read,
                    floor,
                    refused,
                    ..
                } = &mut f.events[i]
                else {
                    return Err("rig: the promote event is not a promote".to_string());
                };
                *refused = Some(message);
                sim.cover("clock:refused");
                let after = floor.unwrap_or(0).saturating_sub(TOL);
                let reading = f.reads.lock().expect("reads lock")[*read].kernel;
                sim.clock()
                    .sleep(Duration::from_nanos(after.saturating_sub(reading) + SEC));
            }
            Some(other) => return Err(format!("the promote was answered {other:?}")),
            // Lost with its connection; the next attempt asks again.
            None => {}
        }
    }
    Ok(())
}

/// A primary and a follower, each on its own host and clock. The primary
/// records times (keys, logins, its schedule's ticks) and ships them; the
/// follower is power-cycled and reopens over them (spec §5 B's "every role is
/// checked at open"); the primary is lost; and the follower is promoted and
/// serves. Both nodes' traces go to the boundary property.
fn promote_universe(sim: &Sim) -> Result<[Trace; 2], String> {
    let mut rng = Rng::new(sim.seed() ^ 0xC10C_9A0E);
    let mut p = Node::new(sim, "p", "p:1", Role::Primary);
    let mut f = Node::new(
        sim,
        "f",
        "f:1",
        Role::Follower {
            primary: "p:1".to_string(),
        },
    );
    let converged = |n: &[&mut Node]| {
        n[1].last_applied().is_some() && n[0].last_applied() == n[1].last_applied()
    };
    'run: {
        if !open_replicated(sim, &mut p)? || !open_replicated(sim, &mut f)? {
            break 'run;
        }
        for _ in 0..rng.between(3, 10) {
            let next = if rng.chance(0.5) {
                Ask::Key
            } else {
                Ask::Login
            };
            if !ask(sim, &mut rng, &mut [&mut p, &mut f], next)? {
                break;
            }
        }
        let until = sim.now().as_nanos() + 60 * SEC;
        if drive(sim, &mut [&mut p, &mut f], until, converged)?.is_none() {
            break 'run;
        }
        if rng.chance(0.75) {
            drop(f.kernel.take());
            f.host.restart();
            if !open_replicated(sim, &mut f)? {
                break 'run;
            }
            let until = sim.now().as_nanos() + 60 * SEC;
            if drive(sim, &mut [&mut p, &mut f], until, converged)?.is_none() {
                break 'run;
            }
        }
        p.host.crash_now();
        drop(p.kernel.take());
        sim.clock().sleep(Duration::from_millis(rng.below(30_000)));
        promote(sim, &mut f, "f:1")?;
        if f.follower {
            break 'run;
        }
        for _ in 0..rng.between(2, 6) {
            let next = if rng.chance(0.5) {
                Ask::Key
            } else {
                Ask::Login
            };
            if !ask(sim, &mut rng, &mut [&mut f], next)? {
                break;
            }
        }
    }
    drop(p.kernel.take());
    drop(f.kernel.take());
    Ok([p.into_trace(), f.into_trace()])
}

// ----------------------------------------------------------------- tests

#[test]
fn no_session_resolves_past_its_ttl_plus_tolerance() {
    run_universes_swarm_with!(
        BASE,
        UNIVERSES,
        clock_axes,
        |sim: &mut Sim| {
            let trace = time_universe(sim, false)?;
            check_ttl(sim, &trace)
        },
        require: [
            "clock:skewed",
            "clock:stepped-back",
            "restart",
            "session:resolved-after-restart",
            "session:expired",
            "session:swept",
        ]
    );
}

#[test]
fn a_schedule_never_stalls_past_one_interval_plus_tolerance() {
    run_universes_swarm_with!(
        BASE,
        UNIVERSES,
        clock_axes,
        |sim: &mut Sim| {
            let trace = time_universe(sim, false)?;
            check_stall(sim, &trace)
        },
        require: ["clock:downtime", "restart", "cron:ticked", "cron:caught-up"]
    );
}

#[test]
fn no_retry_runs_before_its_due_at() {
    run_universes_swarm_with!(
        BASE,
        UNIVERSES,
        clock_axes,
        |sim: &mut Sim| {
            let trace = time_universe(sim, false)?;
            check_retry(sim, &trace)
        },
        require: ["clock:stepped-back", "job:retried", "job:retried-after-restart"]
    );
}

#[test]
fn the_stop_and_the_refusal_fire_exactly_when_the_clock_is_beyond_tolerance() {
    run_universes_swarm_with!(
        BASE,
        UNIVERSES,
        clock_axes,
        |sim: &mut Sim| {
            let trace = time_universe(sim, false)?;
            check_boundary(sim, &trace)
        },
        require: [
            "clock:refused",
            "clock:stepped-back",
            "clock:refused:stop",
            "clock:refused:open",
            "clock:held",
            "clock:held:open",
            "clock:stalled",
        ]
    );
}

/// The boundary property's promote half (spec §5 B: "the stop and the
/// open/promote refusal"), on a primary and a follower whose clocks each
/// carry their own skew, step-backs and downtime: the follower's open over
/// the times it replicated, and its promote, are judged by the same rule as
/// the single node's.
#[test]
fn a_promote_is_refused_exactly_when_the_followers_clock_is_beyond_tolerance() {
    run_universes_swarm_with!(
        BASE,
        UNIVERSES,
        clock_axes,
        |sim: &mut Sim| {
            let [p, f] = promote_universe(sim)?;
            check_boundary(sim, &p)?;
            check_boundary(sim, &f)
        },
        require: [
            "clock:skewed",
            "clock:refused:open:follower",
            "clock:refused:promote",
            "clock:held:promote",
            "clock:promoted",
        ]
    );
}

/// The naive victim (hard rule 3): the node's clock reads process uptime on
/// the sim's wall scale, the pre-0.3.0 bug of a process-local time stored as
/// if it meant something after a restart. Every clock fault is off, so the
/// honest clock is true time and the restart is the victim's only fault. A
/// first life within the tolerance lets the victim reopen, held at the floor
/// that life recorded, with its time behind the host's clock. Each universe
/// first runs on the honest clock, where all four properties pass. On the
/// uptime clock the stop/refusal and retry properties still pass, because the
/// kernel handles the clock it is given correctly. So the failures name the
/// clock, not the workload or the kernel.
#[test]
fn an_uptime_clock_fails_the_ttl_and_stall_properties_by_name() {
    let mut ttl: Option<String> = None;
    let mut stall: Option<String> = None;
    let mut state = 0xC10C_DEAD_u64;
    for _ in 0..64 {
        let seed = splitmix64(&mut state);
        let mut honest = Sim::with_faults(seed, FaultConfig::none());
        let trace = time_universe(&mut honest, false).unwrap();
        check_ttl(&honest, &trace).unwrap();
        check_stall(&honest, &trace).unwrap();
        check_retry(&honest, &trace).unwrap();
        check_boundary(&honest, &trace).unwrap();
        let mut sim = Sim::with_faults(seed, FaultConfig::none());
        let trace = time_universe(&mut sim, true).unwrap();
        check_retry(&sim, &trace).unwrap();
        check_boundary(&sim, &trace).unwrap();
        ttl = ttl.or_else(|| check_ttl(&sim, &trace).err());
        stall = stall.or_else(|| check_stall(&sim, &trace).err());
        if ttl.is_some() && stall.is_some() {
            break;
        }
    }
    let ttl = ttl.expect("the uptime clock passed the TTL property in 64 universes");
    assert!(
        ttl.starts_with(&format!(
            "{TTL_PROPERTY}: a session resolved after its expiry on the clock that judged it"
        )),
        "{ttl}"
    );
    let stall = stall.expect("the uptime clock passed the stall property in 64 universes");
    assert!(
        stall.starts_with(&format!(
            "{STALL_PROPERTY}: the node went idle with its schedule due on its clock"
        )),
        "{stall}"
    );
}
