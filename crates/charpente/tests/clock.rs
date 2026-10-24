//! The time properties (0.3.0 spec §5 B). One workload drives a single node
//! through several lives under all three sim clock faults (skew, step-backs,
//! downtime) and records two things: what the node did, and what its clock
//! read at every step. Four properties judge that record against true wall
//! time (`Sim::true_wall`), each with `require` keys proving its axes fired.
//! A naive victim, a clock that reads process uptime, fails the TTL and stall
//! properties by name.
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
//! - an open whose reading is more than the tolerance behind the floor the
//!   store recorded is refused, and otherwise starts the hold at that floor.
//!
//! Each property carries that band through to sessions, schedules and retries.
//! The boundary property holds the band itself.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use charpente::auth::sweep::SESSION_SWEEP_NAME;
use charpente::auth::{ApiKey, hash_token};
use charpente::http::{ApiError, App, AppConfig, Ctx, Method, Request, Session};
use charpente::kernel::{CronRow, JobError, Kernel, RequestSource, SimSource, Step, Wake};
use charpente::{Db, Job, Schema, Tx, View, job, jobs, route, routes};
use charpente_core::io::{CLOCK_TOLERANCE, Clock, Instant, WallTime};
use charpente_core::job::{JobRow, JobStatus};
use charpente_sim::rng::{Rng, splitmix64};
use charpente_sim::{FaultConfig, SIM_WALL_BASE_NANOS, Sim, SimClock, run_universes_swarm_with};

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

fn open_app(sim: &Sim) -> std::io::Result<App> {
    Ok(App::open(
        Box::new(sim.disk()),
        Schema::new(),
        Box::new(sim.entropy()),
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
/// cycle stays as sampled. `adjust` is handed no seed, so the clock draws key
/// off the swarm-sampled config, which is itself a function of the seed.
fn clock_axes(cfg: &mut FaultConfig) {
    let mut state = format!("{cfg:?}")
        .bytes()
        .fold(0xcbf2_9ce4_8422_2325u64, |h, b| {
            (h ^ u64::from(b)).wrapping_mul(0x0000_0100_0000_01b3)
        });
    let mut rng = Rng::new(splitmix64(&mut state));
    cfg.write_error = 0.0;
    cfg.sync_error = 0.0;
    cfg.dir_entry_loss = 0.0;
    cfg.bit_flip = 0.0;
    cfg.misdirected_write = 0.0;
    cfg.read_error = 0.0;
    const SKEW: u64 = 120 * SEC;
    cfg.clock_skew = rng.between(0, 2 * SKEW) as i64 - SKEW as i64;
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
    /// `Sim::true_wall` at this read. The clock holds no `Sim`, so it keeps
    /// the virtual time, and true wall time is that plus the base, by
    /// `Sim::true_wall`'s definition.
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
        let app = open_app(sim).map_err(|e| format!("App::open: {e}"))?;
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
                if !text.starts_with("clock went backwards: this node's wall clock reads ") {
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
            answer(kernel, rig, read, ask, status, &body)?;
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
    rig: &mut Rig,
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
            rig.events.push(Event::Mint {
                read,
                token: rig.tokens.len(),
                expires_at: session.expires_at.as_nanos(),
            });
            rig.tokens.push(token);
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
            rig.events.push(Event::Key {
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
            rig.events.push(Event::Present { read, token, seen });
            Ok(())
        }
        Ask::Flaky if status == 200 => Ok(()),
        Ask::Flaky => Err(unexpected()),
    }
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

/// The hold at one read, replayed from the readings the kernel was handed:
/// the held value before the read (`None` at an open, where the hold starts
/// over) and the wall time the node handed out there (`None` where the read
/// stopped the node or was refused). It follows what the node did, so one
/// wrong decision does not cascade; the boundary property judges each
/// decision against the rule, and the others use `handed` as the node's own
/// time.
#[derive(Clone, Copy)]
struct Held {
    before: Option<u64>,
    handed: Option<u64>,
}

fn replay_hold(t: &Trace) -> Result<Vec<Held>, String> {
    let mut out: Vec<Held> = Vec::with_capacity(t.reads.len());
    let mut held: Option<u64> = None;
    for event in &t.events {
        let (read, before, handed) = match event {
            Event::Open {
                read,
                floor,
                refused,
                ..
            } => {
                let r = t.reads[*read].kernel;
                let handed = refused.is_none().then(|| floor.map_or(r, |f| r.max(f)));
                (*read, None, handed)
            }
            Event::Step { read, stopped, .. } => {
                let h = held.ok_or("rig: a step with no open node")?;
                let r = t.reads[*read].kernel;
                (*read, Some(h), stopped.is_none().then(|| h.max(r)))
            }
            _ => continue,
        };
        if read != out.len() {
            return Err(format!("rig: read {read} is not the next of {}", out.len()));
        }
        out.push(Held { before, handed });
        held = handed;
    }
    if out.len() != t.reads.len() {
        return Err("rig: a wall read belongs to no open or step".to_string());
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
/// which it does for `expires_at <= W_tick <= R_tick + tol`, so some sweep
/// tick before it must allow that. This catches a session lost with no sweep
/// behind it. It cannot catch a sweep that deletes early: here the daily
/// sweep ticks only right after a downtime jump, which has already expired
/// every session it could reach. The sweep's `before` is pinned in
/// tests/kernel.rs.
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
                swept_through = swept_through.max(t.reads[*read].kernel + TOL);
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
                                 delete it (the latest a sweep could reach was {}): {}",
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
/// The rule: an open is refused exactly when its reading is more than the
/// tolerance behind the store's recorded floor, and a step stops exactly when
/// its reading is more than the tolerance behind the held value. The checks:
/// - each stop and refusal fires whenever the rule says it must, and never
///   otherwise;
/// - its text names the reading, the gap, and the held time or floor;
/// - between stops, every time the node hands out (a session's `expires_at -
///   ttl`, a key's `created_at`) is the replayed hold's.
///
/// Wherever a workload excuses a clock stop or refusal as a fault's fallout,
/// this property is the oracle that neither fires without a clock beyond the
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
    for event in &t.events {
        match event {
            Event::Open {
                read,
                floor,
                refused,
                ..
            } => {
                let r = t.reads[*read].kernel;
                let beyond = floor.filter(|&f| r < f.saturating_sub(TOL));
                match (beyond, refused) {
                    (Some(f), Some(text)) => {
                        let named = format!(
                            "refusing to open: this node's wall clock reads {}, {:?} behind {}, ",
                            wall(r),
                            Duration::from_nanos(f - r),
                            wall(f)
                        );
                        if !text.starts_with(&named) {
                            return Err(format!(
                                "{BOUNDARY_PROPERTY}: the open refusal does not name its \
                                 reading and floor (expected it to start {named:?}): {text}"
                            ));
                        }
                    }
                    (None, None) => {}
                    (Some(f), None) => {
                        return Err(format!(
                            "{BOUNDARY_PROPERTY}: a node opened with its clock reading {}, \
                             more than the tolerance behind {}, the latest time its data \
                             directory recorded",
                            wall(r),
                            wall(f)
                        ));
                    }
                    (None, Some(text)) => {
                        return Err(format!(
                            "{BOUNDARY_PROPERTY}: a node refused to open with its clock \
                             reading {}, within the tolerance of its floor {:?}: {text}",
                            wall(r),
                            floor.map(wall)
                        ));
                    }
                }
            }
            Event::Step { read, stopped, .. } => {
                let h = hold[*read].before.ok_or("rig: a step with no open node")?;
                let r = t.reads[*read].kernel;
                let beyond = r < h.saturating_sub(TOL);
                match (beyond, stopped) {
                    (true, Some(text)) => {
                        let named = format!(
                            "clock went backwards: this node's wall clock reads {}, {:?} behind \
                             {}, ",
                            wall(r),
                            Duration::from_nanos(h - r),
                            wall(h)
                        );
                        if !text.starts_with(&named) {
                            return Err(format!(
                                "{BOUNDARY_PROPERTY}: the stop does not name its reading and \
                                 held time (expected it to start {named:?}): {text}"
                            ));
                        }
                    }
                    (false, None) => {
                        if r < h {
                            sim.cover("clock:held");
                        }
                    }
                    (true, None) => {
                        return Err(format!(
                            "{BOUNDARY_PROPERTY}: the node ran on with its clock reading {}, \
                             more than the tolerance behind {}, the latest time it used",
                            wall(r),
                            wall(h)
                        ));
                    }
                    (false, Some(text)) => {
                        return Err(format!(
                            "{BOUNDARY_PROPERTY}: the node stopped with its clock reading {}, \
                             within the tolerance of {}, the latest time it used: {text}",
                            wall(r),
                            wall(h)
                        ));
                    }
                }
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
