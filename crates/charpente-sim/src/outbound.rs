//! The simulated outside world for outbound calls (outbound-effects spec §10).
//! One world per universe, shared by every `SimOutbound` handle: a crash kills
//! the box, never the remote, so models and requests already on the wire
//! survive a restart. Each handle is one process's view (plan amendment A3): a
//! reply goes only to the handle that started its call. Every fault axis rolls
//! from its own keyed stream — a shared stream would let a 0.0 axis shift every
//! later draw (`object.rs`'s lesson) — and rolls on every call, whatever the
//! others decide, so turning one axis on or off moves no other axis's draws.
//! `Sim::quiesce_outbound_faults` stops every roll alike for the rest of the
//! universe. Delivery and reply times are drawn at `start`, so `poll` is a pure
//! virtual-time check. Models run under the world lock: a model must never call
//! back into the `Sim`.

use std::any::Any;
use std::collections::BTreeMap;
use std::io;
use std::sync::Arc;
use std::time::Duration;

use charpente_core::io::{
    IdempotencyKey, Instant, Outbound, OutboundCall, OutboundReply, OutboundWake,
};

use crate::rng::Rng;
use crate::{State, World, keyed_stream};

/// The outside world behind one declared host. Deterministic: its own
/// variance draws from `rng`, the model's own seeded stream.
pub trait Remote: Any + Send {
    /// Runs when the request REACHES the remote, at its drawn delivery time.
    fn handle(&mut self, call: &OutboundCall, rng: &mut Rng) -> OutboundReply;
}

/// A closure as a model (plan amendment A1).
pub struct FnRemote<F>(F);

pub fn from_fn<F>(f: F) -> FnRemote<F>
where
    F: FnMut(&OutboundCall) -> OutboundReply + Send + 'static,
{
    FnRemote(f)
}

impl<F> Remote for FnRemote<F>
where
    F: FnMut(&OutboundCall) -> OutboundReply + Send + 'static,
{
    fn handle(&mut self, call: &OutboundCall, _rng: &mut Rng) -> OutboundReply {
        (self.0)(call)
    }
}

/// Answers 200 with an empty body and keeps every call it was handed, in
/// delivery order — fire-and-forget email.
#[derive(Default)]
pub struct Recorder {
    calls: Vec<OutboundCall>,
}

impl Recorder {
    pub fn new() -> Recorder {
        Recorder::default()
    }

    pub fn calls(&self) -> &[OutboundCall] {
        &self.calls
    }
}

impl Remote for Recorder {
    fn handle(&mut self, call: &OutboundCall, _rng: &mut Rng) -> OutboundReply {
        self.calls.push(call.clone());
        OutboundReply {
            status: 200,
            headers: Vec::new(),
            body: Vec::new(),
        }
    }
}

/// Stripe's idempotency semantics over an inner model, keyed by what the APP
/// sent — never by `OutboundCall::key`, which no real remote can see. A
/// repeated key replays the first reply; a reused key with different
/// parameters answers 409; a request carrying no key is applied every time.
pub struct Idempotent<M> {
    extract: fn(&OutboundCall) -> Option<String>,
    header: Option<String>,
    inner: M,
    first: BTreeMap<String, (Vec<u8>, String, OutboundReply)>,
    applied: BTreeMap<String, u32>,
}

impl<M: Remote> Idempotent<M> {
    pub fn header(name: &str, inner: M) -> Idempotent<M> {
        Idempotent {
            extract: |_| None,
            header: Some(name.to_ascii_lowercase()),
            inner,
            first: BTreeMap::new(),
            applied: BTreeMap::new(),
        }
    }

    pub fn by(extract: fn(&OutboundCall) -> Option<String>, inner: M) -> Idempotent<M> {
        Idempotent {
            extract,
            header: None,
            inner,
            first: BTreeMap::new(),
            applied: BTreeMap::new(),
        }
    }

    /// Applications per key. A repeated key replays, so every count is 1 by
    /// construction: the map says which keys reached the inner model, and a
    /// retry that sent a fresh key is a second entry. A test counts
    /// applications through `inner()`'s own state.
    pub fn applied(&self) -> &BTreeMap<String, u32> {
        &self.applied
    }

    pub fn inner(&self) -> &M {
        &self.inner
    }

    fn key_of(&self, call: &OutboundCall) -> Option<String> {
        match &self.header {
            Some(name) => call
                .headers
                .iter()
                .find(|(n, _)| n.eq_ignore_ascii_case(name))
                .map(|(_, v)| v.clone()),
            None => (self.extract)(call),
        }
    }
}

impl<M: Remote> Remote for Idempotent<M> {
    fn handle(&mut self, call: &OutboundCall, rng: &mut Rng) -> OutboundReply {
        let Some(key) = self.key_of(call) else {
            return self.inner.handle(call, rng);
        };
        let params = format!("{} {}", call.method.as_str(), call.path);
        if let Some((body, first_params, reply)) = self.first.get(&key) {
            if *body == call.body && *first_params == params {
                return reply.clone();
            }
            return OutboundReply {
                status: 409,
                headers: Vec::new(),
                body: b"idempotency key reused with different parameters".to_vec(),
            };
        }
        let reply = self.inner.handle(call, rng);
        *self.applied.entry(key.clone()).or_insert(0) += 1;
        self.first
            .insert(key, (call.body.clone(), params, reply.clone()));
        reply
    }
}

/// One request that reached a remote — the checkers' evidence (§10).
#[derive(Clone, Debug)]
pub struct Delivered {
    pub host: String,
    pub key: IdempotencyKey,
    /// Virtual nanos at delivery.
    pub at: u64,
    /// The `SimOutbound` handle (process) that sent it.
    pub handle: u64,
}

enum Fate {
    /// Reaches the remote at `deliver_at`; its reply returns at `reply_at`
    /// unless `reply_lost`.
    Delivered {
        deliver_at: u64,
        reply_at: u64,
        reply_lost: bool,
    },
    /// Never reaches the remote; completes at `at` with `result` (a refusal or
    /// a throttle), or never (`at == None`, a lost request).
    Short {
        at: Option<u64>,
        result: Option<io::Result<OutboundReply>>,
    },
}

struct Flight {
    handle: u64,
    token: u64,
    call: OutboundCall,
    fate: Fate,
    cancelled: bool,
    /// The model's reply once delivered; taken at completion.
    reply: Option<OutboundReply>,
    delivered: bool,
}

pub(crate) struct OutboundWorld {
    // Read by `Sim::{remote, outbound, with_remote, outbound_deliveries}` in lib.rs.
    /// Each host's model, its own stream, and its type's name for
    /// `with_remote`'s wrong-type panic.
    pub(crate) models: BTreeMap<String, (Box<dyn Remote>, Rng, &'static str)>,
    flights: BTreeMap<u64, Flight>,
    next_flight: u64,
    pub(crate) next_handle: u64,
    pub(crate) deliveries: Vec<Delivered>,
    /// Set by `Sim::quiesce_outbound_faults`: the fault-free tail's outbound
    /// half. No fault axis rolls from here on, so a quiet world draws nothing
    /// from their streams. Latency stays, since it is not a fault, and a
    /// request already in flight keeps the fate it drew.
    pub(crate) quiet: bool,
    refused: Rng,
    lost_request: Rng,
    lost_reply: Rng,
    throttle: Rng,
    slow: Rng,
    latency: Rng,
}

impl OutboundWorld {
    pub(crate) fn new(seed: u64) -> OutboundWorld {
        OutboundWorld {
            models: BTreeMap::new(),
            flights: BTreeMap::new(),
            next_flight: 0,
            next_handle: 0,
            deliveries: Vec::new(),
            quiet: false,
            refused: keyed_stream(seed, "outbound-rt:refused"),
            lost_request: keyed_stream(seed, "outbound-rt:lost_request"),
            lost_reply: keyed_stream(seed, "outbound-rt:lost_reply"),
            throttle: keyed_stream(seed, "outbound-rt:throttle"),
            slow: keyed_stream(seed, "outbound-rt:slow"),
            latency: keyed_stream(seed, "outbound-rt:latency"),
        }
    }

    /// Runs every delivery due by `now`, in (delivery time, flight) order —
    /// on the wire, whoever's handle sent it.
    pub(crate) fn deliver_due(&mut self, now: u64) {
        let due: Vec<u64> = self
            .flights
            .iter()
            .filter(|(_, f)| !f.delivered)
            .filter_map(|(id, f)| match f.fate {
                Fate::Delivered { deliver_at, .. } if deliver_at <= now => Some((deliver_at, *id)),
                _ => None,
            })
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .map(|(_, id)| id)
            .collect();
        for id in due {
            let flight = self.flights.get_mut(&id).expect("listed above");
            let Fate::Delivered { deliver_at, .. } = flight.fate else {
                unreachable!()
            };
            let (model, rng, _) = self
                .models
                .get_mut(flight.call.host)
                .expect("start refused any host without a model");
            flight.reply = Some(model.handle(&flight.call, rng));
            flight.delivered = true;
            self.deliveries.push(Delivered {
                host: flight.call.host.to_string(),
                key: flight.call.key,
                at: deliver_at,
                handle: flight.handle,
            });
        }
    }
}

/// The no-model refusal (plan amendment A21): `Sim::outbound` returns it for a
/// declared host, and `start` panics with it for a host the list left out.
pub(crate) fn no_model(host: &str) -> String {
    format!(
        "sim: a mounted job calls {host}, which has no model — add \
         sim.remote(\"{host}\", …) before sim.outbound(…)"
    )
}

fn draw(rng: &mut Rng, (lo, hi): (Duration, Duration)) -> u64 {
    let (lo, hi) = (lo.as_nanos() as u64, hi.as_nanos() as u64);
    lo + if hi > lo { rng.below(hi - lo + 1) } else { 0 }
}

/// One process's outbound handle.
pub struct SimOutbound {
    pub(crate) world: Arc<World>,
    pub(crate) handle: u64,
}

impl Outbound for SimOutbound {
    fn start(&self, token: u64, call: OutboundCall) -> io::Result<()> {
        let mut guard = self.world.lock();
        let cfg = &self.world.cfg;
        let now = guard.now;
        let State {
            outbound, coverage, ..
        } = &mut *guard;
        assert!(
            !outbound
                .flights
                .values()
                .any(|f| f.handle == self.handle && f.token == token && !f.cancelled),
            "contract: Outbound::start({token}) while that token is in flight"
        );
        assert!(
            outbound.models.contains_key(call.host),
            "{}",
            no_model(call.host)
        );
        // Every axis rolls once per call, whatever the others decide, so
        // turning one on or off moves no other axis's draw. A draw that only
        // its own axis's roll gates (the throttle's status, the slow think
        // time) stays on that axis's stream. A quiet world skips every roll
        // alike, and the latency draws carry on.
        let refused = !outbound.quiet && outbound.refused.chance(cfg.outbound_refused);
        let lost_request =
            !outbound.quiet && outbound.lost_request.chance(cfg.outbound_lost_request);
        let throttle = if !outbound.quiet && outbound.throttle.chance(cfg.outbound_throttle) {
            Some(if outbound.throttle.chance(0.5) {
                429
            } else {
                503
            })
        } else {
            None
        };
        let up = draw(&mut outbound.latency, cfg.outbound_latency);
        let down = draw(&mut outbound.latency, cfg.outbound_latency);
        let think = if !outbound.quiet && outbound.slow.chance(cfg.outbound_slow) {
            let hi = cfg.outbound_latency.1;
            let (lo, hi) = if hi.is_zero() {
                (Duration::from_secs(1), Duration::from_secs(100))
            } else {
                (hi * 10, hi * 100)
            };
            Some(draw(&mut outbound.slow, (lo, hi)))
        } else {
            None
        };
        let reply_lost = !outbound.quiet && outbound.lost_reply.chance(cfg.outbound_lost_reply);
        let mut cover = |key| *coverage.entry(key).or_insert(0) += 1;
        let fate = if refused {
            cover("fault:outbound_refused");
            Fate::Short {
                at: Some(now),
                result: Some(Err(io::Error::new(
                    io::ErrorKind::ConnectionRefused,
                    "connection refused",
                ))),
            }
        } else if lost_request {
            cover("fault:outbound_lost_request");
            Fate::Short {
                at: None,
                result: None,
            }
        } else if let Some(status) = throttle {
            cover("fault:outbound_throttle");
            Fate::Short {
                at: Some(now + up * 2),
                result: Some(Ok(OutboundReply {
                    status,
                    headers: Vec::new(),
                    body: Vec::new(),
                })),
            }
        } else {
            if think.is_some() {
                cover("fault:outbound_slow");
            }
            if reply_lost {
                cover("fault:outbound_lost_reply");
            }
            Fate::Delivered {
                deliver_at: now + up,
                reply_at: now + up + think.unwrap_or(0) + down,
                reply_lost,
            }
        };
        let id = outbound.next_flight;
        outbound.next_flight += 1;
        outbound.flights.insert(
            id,
            Flight {
                handle: self.handle,
                token,
                call,
                fate,
                cancelled: false,
                reply: None,
                delivered: false,
            },
        );
        Ok(())
    }

    fn poll(&self) -> Option<(u64, io::Result<OutboundReply>)> {
        let mut guard = self.world.lock();
        let now = guard.now;
        let outbound = &mut guard.outbound;
        outbound.deliver_due(now);
        let next = outbound
            .flights
            .iter()
            .filter(|(_, f)| f.handle == self.handle && !f.cancelled)
            .filter_map(|(id, f)| completes_at(f).filter(|at| *at <= now).map(|at| (at, *id)))
            .min()?;
        let flight = outbound.flights.get_mut(&next.1).expect("found above");
        let result = match &mut flight.fate {
            Fate::Short { result, .. } => result.take().expect("a short fate completes once"),
            Fate::Delivered { .. } => Ok(flight.reply.take().expect("delivered before its reply")),
        };
        let token = flight.token;
        outbound.flights.remove(&next.1);
        guard.cover("outbound:completed");
        Some((token, result))
    }

    fn cancel(&self, token: u64) {
        let mut guard = self.world.lock();
        for f in guard.outbound.flights.values_mut() {
            if f.handle == self.handle && f.token == token {
                f.cancelled = true;
            }
        }
    }

    fn next_wake(&self) -> OutboundWake {
        let guard = self.world.lock();
        guard
            .outbound
            .flights
            .values()
            .filter(|f| f.handle == self.handle && !f.cancelled)
            .filter_map(completes_at)
            .min()
            .map_or(OutboundWake::Never, |at| {
                OutboundWake::At(Instant::from_nanos(at))
            })
    }
}

fn completes_at(f: &Flight) -> Option<u64> {
    match f.fate {
        Fate::Delivered {
            reply_at,
            reply_lost: false,
            ..
        } => Some(reply_at),
        Fate::Delivered {
            reply_lost: true, ..
        } => None,
        Fate::Short { at, .. } => at,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{FaultConfig, Sim};
    use charpente_core::io::{
        Clock, HttpMethod, IdempotencyKey, Instant, Outbound, OutboundCall, OutboundReply,
        OutboundWake,
    };
    use std::time::Duration;

    fn call(n: u8) -> OutboundCall {
        OutboundCall {
            host: "pay.test",
            method: HttpMethod::Post,
            path: "/charge".into(),
            headers: vec![("idempotency-key".into(), format!("k{n}"))],
            body: vec![n],
            key: IdempotencyKey::from_bytes([n; 16]),
        }
    }

    fn faults(f: impl FnOnce(&mut FaultConfig)) -> FaultConfig {
        let mut cfg = FaultConfig::none();
        f(&mut cfg);
        cfg
    }

    fn drain(ob: &SimOutbound) -> Vec<(u64, io::Result<OutboundReply>)> {
        std::iter::from_fn(|| ob.poll()).collect()
    }

    /// Steps virtual time from wake to wake until nothing `ob` started can
    /// still complete: each completed token's virtual completion time and its
    /// status, or its error kind.
    fn settle(sim: &Sim, ob: &SimOutbound) -> BTreeMap<u64, (u64, Result<u16, io::ErrorKind>)> {
        let mut done = BTreeMap::new();
        loop {
            for (t, r) in drain(ob) {
                let r = r.map(|r| r.status).map_err(|e| e.kind());
                let again = done.insert(t, (sim.now().as_nanos(), r));
                assert!(again.is_none(), "token {t} completed twice");
            }
            match ob.next_wake() {
                OutboundWake::At(at) => {
                    assert!(
                        at > sim.now(),
                        "a drained handle woke at {at:?}, not after now"
                    );
                    sim.clock().sleep(at.duration_since(sim.now()));
                }
                OutboundWake::Never => return done,
                OutboundWake::Unknown => panic!("the sim always knows its next wake"),
            }
        }
    }

    fn panic_text(f: impl FnOnce()) -> String {
        let payload = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f))
            .expect_err("expected a contract panic");
        crate::panic_message(&*payload)
    }

    #[test]
    fn a_host_with_no_model_is_refused_by_name() {
        let sim = Sim::with_faults(1, FaultConfig::none());
        let err = sim
            .outbound(&["pay.test"])
            .err()
            .expect("no model registered");
        assert_eq!(
            err.to_string(),
            "sim: a mounted job calls pay.test, which has no model — add sim.remote(\"pay.test\", …) before sim.outbound(…)"
        );
    }

    #[test]
    fn a_fault_free_call_is_delivered_then_answered() {
        let sim = Sim::with_faults(1, FaultConfig::none());
        sim.remote("pay.test", Recorder::new());
        let ob = sim.outbound(&["pay.test"]).unwrap();
        ob.start(0, call(1)).unwrap();
        let done = drain(&ob);
        assert_eq!(done.len(), 1);
        assert_eq!(done[0].0, 0);
        assert_eq!(done[0].1.as_ref().unwrap().status, 200);
        sim.with_remote("pay.test", |r: &Recorder| assert_eq!(r.calls().len(), 1));
        assert_eq!(sim.outbound_deliveries().len(), 1);
    }

    #[test]
    fn a_refused_call_never_reaches_the_remote() {
        let sim = Sim::with_faults(1, faults(|c| c.outbound_refused = 1.0));
        sim.remote("pay.test", Recorder::new());
        let ob = sim.outbound(&["pay.test"]).unwrap();
        ob.start(0, call(1)).unwrap();
        let done = drain(&ob);
        assert_eq!(
            done[0].1.as_ref().unwrap_err().kind(),
            io::ErrorKind::ConnectionRefused
        );
        sim.with_remote("pay.test", |r: &Recorder| assert!(r.calls().is_empty()));
        assert_eq!(sim.coverage().get("fault:outbound_refused"), Some(&1));
    }

    #[test]
    fn a_lost_reply_applies_at_the_remote_and_never_completes() {
        let sim = Sim::with_faults(1, faults(|c| c.outbound_lost_reply = 1.0));
        sim.remote("pay.test", Recorder::new());
        let ob = sim.outbound(&["pay.test"]).unwrap();
        ob.start(0, call(1)).unwrap();
        assert!(drain(&ob).is_empty());
        assert_eq!(
            ob.next_wake(),
            OutboundWake::Never,
            "a lost reply is never due"
        );
        sim.with_remote("pay.test", |r: &Recorder| assert_eq!(r.calls().len(), 1));
        assert_eq!(sim.coverage().get("fault:outbound_lost_reply"), Some(&1));
    }

    #[test]
    fn a_lost_request_reaches_nothing() {
        let sim = Sim::with_faults(1, faults(|c| c.outbound_lost_request = 1.0));
        sim.remote("pay.test", Recorder::new());
        let ob = sim.outbound(&["pay.test"]).unwrap();
        ob.start(0, call(1)).unwrap();
        assert!(drain(&ob).is_empty());
        assert_eq!(
            ob.next_wake(),
            OutboundWake::Never,
            "a lost request is never due"
        );
        sim.with_remote("pay.test", |r: &Recorder| assert!(r.calls().is_empty()));
        assert_eq!(sim.coverage().get("fault:outbound_lost_request"), Some(&1));
    }

    #[test]
    fn a_throttle_answers_429_or_503_without_handling() {
        let sim = Sim::with_faults(
            1,
            faults(|c| {
                c.outbound_throttle = 1.0;
                c.outbound_latency = (Duration::from_millis(10), Duration::from_millis(10));
            }),
        );
        sim.remote("pay.test", Recorder::new());
        let ob = sim.outbound(&["pay.test"]).unwrap();
        ob.start(0, call(1)).unwrap();
        assert!(drain(&ob).is_empty(), "a throttle takes the round trip");
        assert_eq!(
            ob.next_wake(),
            OutboundWake::At(Instant::from_nanos(20_000_000)),
            "twice the one-way latency"
        );
        sim.clock().sleep(Duration::from_millis(20));
        let status = drain(&ob)[0].1.as_ref().unwrap().status;
        assert!(status == 429 || status == 503, "{status}");
        sim.with_remote("pay.test", |r: &Recorder| assert!(r.calls().is_empty()));
        assert_eq!(sim.coverage().get("fault:outbound_throttle"), Some(&1));
    }

    #[test]
    fn latency_delays_completion_and_next_wake_names_it() {
        let sim = Sim::with_faults(
            1,
            faults(|c| c.outbound_latency = (Duration::from_millis(10), Duration::from_millis(10))),
        );
        sim.remote("pay.test", Recorder::new());
        let ob = sim.outbound(&["pay.test"]).unwrap();
        ob.start(0, call(1)).unwrap();
        assert!(drain(&ob).is_empty(), "nothing before 20ms");
        assert_eq!(
            ob.next_wake(),
            OutboundWake::At(Instant::from_nanos(20_000_000))
        );
        sim.clock().sleep(Duration::from_millis(20));
        assert_eq!(drain(&ob).len(), 1);
    }

    #[test]
    fn a_cancelled_call_still_arrives_but_never_completes() {
        let sim = Sim::with_faults(
            1,
            faults(|c| c.outbound_latency = (Duration::from_millis(10), Duration::from_millis(10))),
        );
        sim.remote("pay.test", Recorder::new());
        let ob = sim.outbound(&["pay.test"]).unwrap();
        ob.start(0, call(1)).unwrap();
        ob.cancel(0);
        assert_eq!(
            ob.next_wake(),
            OutboundWake::Never,
            "a cancelled call is never due"
        );
        sim.clock().sleep(Duration::from_millis(50));
        assert!(drain(&ob).is_empty());
        sim.with_remote("pay.test", |r: &Recorder| assert_eq!(r.calls().len(), 1));
    }

    /// Cancelling one call leaves every other call alone: the same handle's
    /// other token, and another handle's call under the same token number.
    #[test]
    fn cancelling_one_call_leaves_every_other_call_alone() {
        let sim = Sim::with_faults(
            0x7A13_0020,
            faults(|c| c.outbound_latency = (Duration::from_millis(10), Duration::from_millis(10))),
        );
        sim.remote("pay.test", Recorder::new());
        let a = sim.outbound(&["pay.test"]).unwrap();
        let b = sim.outbound(&["pay.test"]).unwrap();
        a.start(0, call(1)).unwrap();
        a.start(1, call(2)).unwrap();
        b.start(0, call(3)).unwrap();
        a.cancel(0);
        sim.clock().sleep(Duration::from_millis(50));
        let tokens = |ob: &SimOutbound| drain(ob).into_iter().map(|(t, _)| t).collect::<Vec<_>>();
        assert_eq!(
            tokens(&a),
            vec![1],
            "only the cancelled token stops completing"
        );
        assert_eq!(
            tokens(&b),
            vec![0],
            "another handle's token 0 is another call, untouched by this cancel"
        );
    }

    /// A request reaches the remote at its delivery time, one latency after
    /// its start, never earlier: until then the remote has not applied it.
    #[test]
    fn a_request_reaches_the_remote_only_at_its_delivery_time() {
        let sim = Sim::with_faults(
            0x7A13_0021,
            faults(|c| c.outbound_latency = (Duration::from_secs(1), Duration::from_secs(1))),
        );
        sim.remote("pay.test", Recorder::new());
        let ob = sim.outbound(&["pay.test"]).unwrap();
        ob.start(0, call(1)).unwrap();
        assert!(drain(&ob).is_empty());
        sim.with_remote("pay.test", |r: &Recorder| {
            assert_eq!(
                r.calls().len(),
                0,
                "the remote applied a request still on the wire"
            )
        });
        assert!(sim.outbound_deliveries().is_empty());
        sim.clock().sleep(Duration::from_secs(1));
        assert!(drain(&ob).is_empty(), "the reply is still on its way back");
        sim.with_remote("pay.test", |r: &Recorder| assert_eq!(r.calls().len(), 1));
        let at: Vec<u64> = sim.outbound_deliveries().iter().map(|d| d.at).collect();
        assert_eq!(at, vec![1_000_000_000]);
    }

    /// Amendment A3: a reply goes only to the handle that started the call.
    #[test]
    fn a_new_handle_never_receives_an_old_handles_reply() {
        let sim = Sim::with_faults(
            1,
            faults(|c| c.outbound_latency = (Duration::from_millis(10), Duration::from_millis(10))),
        );
        sim.remote("pay.test", Recorder::new());
        let old = sim.outbound(&["pay.test"]).unwrap();
        old.start(0, call(1)).unwrap();
        let new = sim.outbound(&["pay.test"]).unwrap();
        new.start(0, call(2)).unwrap(); // the same token, as a restarted kernel's first call
        sim.clock().sleep(Duration::from_millis(50));
        let got = drain(&new);
        assert_eq!(got.len(), 1);
        assert_eq!(
            new.next_wake(),
            OutboundWake::Never,
            "the old handle's call is never due on the new one"
        );
        assert_eq!(
            sim.outbound_deliveries().len(),
            2,
            "both requests were on the wire"
        );
    }

    /// Answers 200 with how many calls it has handled — a named type, so
    /// `with_remote` can name it to downcast.
    struct Counter(u32);

    impl Remote for Counter {
        fn handle(&mut self, _call: &OutboundCall, _rng: &mut Rng) -> OutboundReply {
            self.0 += 1;
            OutboundReply {
                status: 200,
                headers: vec![],
                body: self.0.to_string().into_bytes(),
            }
        }
    }

    #[test]
    fn a_closure_model_answers_through_from_fn() {
        let sim = Sim::with_faults(1, FaultConfig::none());
        sim.remote(
            "pay.test",
            from_fn(|c: &OutboundCall| OutboundReply {
                status: 201,
                headers: vec![],
                body: c.body.clone(),
            }),
        );
        let ob = sim.outbound(&["pay.test"]).unwrap();
        ob.start(0, call(7)).unwrap();
        let done = drain(&ob);
        let reply = done[0].1.as_ref().unwrap();
        assert_eq!((reply.status, reply.body.clone()), (201, vec![7]));
    }

    #[test]
    fn idempotent_replays_the_first_reply_and_refuses_new_parameters() {
        let sim = Sim::with_faults(1, FaultConfig::none());
        sim.remote(
            "pay.test",
            Idempotent::header("idempotency-key", Counter(0)),
        );
        let ob = sim.outbound(&["pay.test"]).unwrap();
        ob.start(0, call(1)).unwrap();
        ob.start(1, call(1)).unwrap(); // same key, same parameters: replayed
        let mut other = call(1);
        other.body = vec![9]; // same key, new parameters
        ob.start(2, other).unwrap();
        let mut keyless = call(3);
        keyless.headers.clear();
        ob.start(3, keyless.clone()).unwrap();
        ob.start(4, keyless).unwrap(); // no key: applied every time
        let done = drain(&ob);
        let body = |t: u64| {
            let r = done.iter().find(|d| d.0 == t).unwrap().1.as_ref().unwrap();
            (r.status, String::from_utf8(r.body.clone()).unwrap())
        };
        assert_eq!(body(0), (200, "1".into()));
        assert_eq!(body(1), (200, "1".into()));
        assert_eq!(body(2).0, 409);
        assert_eq!(body(3), (200, "2".into()));
        assert_eq!(body(4), (200, "3".into()));
        sim.with_remote("pay.test", |m: &Idempotent<Counter>| {
            assert_eq!(m.applied().get("k1"), Some(&1));
            assert_eq!(m.inner().0, 3, "k1 once, then the keyless call twice");
        });
    }

    #[test]
    fn a_universe_replays_its_outbound_world_byte_for_byte() {
        let run = |seed| {
            let mut cfg = FaultConfig::swarm(&mut crate::rng::Rng::new(seed));
            crate::sample_extended_fields(seed, &mut cfg);
            let sim = Sim::with_faults(seed, cfg);
            sim.remote("pay.test", Recorder::new());
            let ob = sim.outbound(&["pay.test"]).unwrap();
            for t in 0..20 {
                ob.start(t, call(t as u8)).unwrap();
            }
            sim.clock().sleep(Duration::from_secs(300));
            let done: Vec<_> = drain(&ob)
                .into_iter()
                .map(|(t, r)| (t, r.map(|r| r.status).map_err(|e| e.kind())))
                .collect();
            (done, sim.outbound_deliveries().len(), sim.coverage())
        };
        for seed in [3u64, 0x912313adf8115ad0] {
            assert_eq!(
                format!("{:?}", run(seed)),
                format!("{:?}", run(seed)),
                "seed {seed:#x}"
            );
        }
    }

    /// Plan amendment A15: a slow call reaches the remote on time, and its
    /// answer waits out think time from [10·hi, 100·hi], or from [1 s, 100 s]
    /// when hi is zero.
    #[test]
    fn a_slow_call_arrives_on_time_and_is_answered_late() {
        let ms = Duration::from_millis;
        let sim = Sim::with_faults(
            1,
            faults(|c| {
                c.outbound_slow = 1.0;
                c.outbound_latency = (ms(10), ms(10));
            }),
        );
        sim.remote("pay.test", Recorder::new());
        let ob = sim.outbound(&["pay.test"]).unwrap();
        ob.start(0, call(1)).unwrap();
        let OutboundWake::At(due) = ob.next_wake() else {
            panic!("a slow call is still answered")
        };
        let due = due.duration_since(Instant::ZERO);
        assert!((ms(120)..=ms(1020)).contains(&due), "{due:?}");
        sim.clock().sleep(ms(10));
        assert!(drain(&ob).is_empty(), "delivered, not yet answered");
        sim.with_remote("pay.test", |r: &Recorder| assert_eq!(r.calls().len(), 1));
        assert_eq!(sim.outbound_deliveries()[0].at, 10_000_000);
        assert_eq!(sim.coverage().get("fault:outbound_slow"), Some(&1));
        sim.clock().sleep(due - ms(10));
        assert_eq!(drain(&ob).len(), 1);

        // Many calls: every answer lands inside the range, and both ends of
        // the range are reached, so a narrowed range is red too.
        let secs = Duration::from_secs;
        for (latency, (lo, hi)) in [
            ((ms(10), ms(10)), (ms(120), ms(1020))),
            ((Duration::ZERO, Duration::ZERO), (secs(1), secs(100))),
        ] {
            let sim = Sim::with_faults(
                1,
                faults(|c| {
                    c.outbound_slow = 1.0;
                    c.outbound_latency = latency;
                }),
            );
            sim.remote("pay.test", Recorder::new());
            let ob = sim.outbound(&["pay.test"]).unwrap();
            for t in 0..64 {
                ob.start(t, call(t as u8)).unwrap();
            }
            let OutboundWake::At(first) = ob.next_wake() else {
                panic!("slow calls are still answered")
            };
            assert!(
                (lo..=hi).contains(&first.duration_since(Instant::ZERO)),
                "{first:?}"
            );
            let times: Vec<Duration> = settle(&sim, &ob)
                .values()
                .map(|(at, _)| Duration::from_nanos(*at))
                .collect();
            assert_eq!(times.len(), 64);
            assert!(times.iter().all(|t| (lo..=hi).contains(t)), "{times:?}");
            let fifth = (hi - lo) / 5;
            assert!(times.iter().min().unwrap() < &(lo + fifth), "{times:?}");
            assert!(times.iter().max().unwrap() > &(hi - fifth), "{times:?}");
        }
    }

    /// A fixed range (`lo == hi`) is not a draw: it answers `lo` and takes
    /// nothing from the stream. A stream that mixes fixed and ranged draws
    /// then maps each seed to the same universe whichever ranges are fixed,
    /// and seeds are product surface (hard rule 3).
    #[test]
    fn a_fixed_range_draws_nothing_from_the_stream() {
        let fixed = Duration::from_millis(250);
        let mut used = Rng::new(0x7A13_0050);
        let mut fresh = Rng::new(0x7A13_0050);
        assert_eq!(draw(&mut used, (fixed, fixed)), 250_000_000);
        assert_eq!(
            used.next_u64(),
            fresh.next_u64(),
            "a fixed-range draw advanced the stream, so every later draw on it moved"
        );
    }

    /// Every call rolls every axis from that axis's own stream, whatever the
    /// others decide (GUIDE §8). Turning refusals on changes which calls are
    /// refused, and nothing about the rest: not their lost replies, not their
    /// times.
    #[test]
    fn turning_one_axis_on_moves_no_other_axis_draw() {
        let run = |refused: f64| {
            let sim = Sim::with_faults(
                7,
                faults(|c| {
                    c.outbound_refused = refused;
                    c.outbound_lost_reply = 0.5;
                    c.outbound_latency = (Duration::from_millis(1), Duration::from_millis(50));
                }),
            );
            sim.remote("pay.test", Recorder::new());
            let ob = sim.outbound(&["pay.test"]).unwrap();
            for t in 0..64 {
                ob.start(t, call(t as u8)).unwrap();
            }
            let done = settle(&sim, &ob);
            let arrived: BTreeMap<IdempotencyKey, u64> = sim
                .outbound_deliveries()
                .iter()
                .map(|d| (d.key, d.at))
                .collect();
            (done, arrived, sim.coverage())
        };
        let (done_a, arrived_a, _) = run(0.0);
        let (done_b, arrived_b, coverage_b) = run(0.5);
        let refused: Vec<u64> = (0..64)
            .filter(|t| {
                matches!(
                    done_b.get(t),
                    Some((_, Err(io::ErrorKind::ConnectionRefused)))
                )
            })
            .collect();
        let lost_a: Vec<u64> = (0..64).filter(|t| !done_a.contains_key(t)).collect();
        assert!(!refused.is_empty() && refused.len() < 64, "{refused:?}");
        assert!(!lost_a.is_empty() && lost_a.len() < 64, "{lost_a:?}");
        for t in (0..64).filter(|t| !refused.contains(t)) {
            let key = call(t as u8).key;
            assert_eq!(
                done_b.get(&t),
                done_a.get(&t),
                "call {t}: its lost-reply fate or its reply time moved"
            );
            assert_eq!(
                arrived_b.get(&key),
                arrived_a.get(&key),
                "call {t}: its delivery time moved"
            );
        }
        // Coverage counts the fate that applies: a refused call that also
        // rolled a lost reply counts only as refused.
        assert!(
            refused.iter().any(|t| lost_a.contains(t)),
            "no refused call also rolled a lost reply"
        );
        let lost_b = (0..64)
            .filter(|t| !refused.contains(t) && !done_b.contains_key(t))
            .count() as u64;
        assert_eq!(
            coverage_b.get("fault:outbound_refused"),
            Some(&(refused.len() as u64))
        );
        assert_eq!(coverage_b.get("fault:outbound_lost_reply"), Some(&lost_b));
    }

    #[test]
    fn a_call_to_a_host_with_no_model_is_refused_at_start() {
        let sim = Sim::with_faults(1, FaultConfig::none());
        let ob = sim.outbound(&[]).unwrap();
        assert_eq!(
            panic_text(|| {
                let _ = ob.start(0, call(1));
            }),
            "sim: a mounted job calls pay.test, which has no model — add sim.remote(\"pay.test\", …) before sim.outbound(…)"
        );
    }

    #[test]
    fn reading_a_host_with_no_model_names_the_fix() {
        let sim = Sim::with_faults(1, FaultConfig::none());
        assert_eq!(
            panic_text(|| sim.with_remote("pay.test", |_: &Recorder| ())),
            "contract: sim.with_remote(\"pay.test\", …) found no model for pay.test — register one with sim.remote(\"pay.test\", …) first"
        );
    }

    #[test]
    fn reading_a_model_as_the_wrong_type_names_both_types() {
        let sim = Sim::with_faults(1, FaultConfig::none());
        sim.remote(
            "pay.test",
            Idempotent::header("idempotency-key", Counter(0)),
        );
        assert_eq!(
            panic_text(|| sim.with_remote("pay.test", |_: &Counter| ())),
            "contract: sim.with_remote(\"pay.test\", …) asked for charpente_sim::outbound::tests::Counter, but pay.test's model is charpente_sim::outbound::Idempotent<charpente_sim::outbound::tests::Counter> — name the registered type; a wrapped model is read as its wrapper, then .inner()"
        );
    }

    /// Models belong to the universe, not the box: a restart keeps the model
    /// and its state, and a request on the wire at the restart still arrives.
    #[test]
    fn a_model_and_a_request_on_the_wire_survive_a_restart() {
        let ms = Duration::from_millis;
        let sim = Sim::with_faults(1, faults(|c| c.outbound_latency = (ms(10), ms(10))));
        sim.remote("pay.test", Counter(0));
        let old = sim.outbound(&["pay.test"]).unwrap();
        old.start(0, call(1)).unwrap();
        sim.clock().sleep(ms(20));
        assert_eq!(drain(&old)[0].1.as_ref().unwrap().body, b"1");
        old.start(1, call(2)).unwrap(); // on the wire when the box goes down
        sim.restart();
        let new = sim.outbound(&["pay.test"]).unwrap();
        new.start(0, call(3)).unwrap();
        sim.clock().sleep(ms(50));
        let got = drain(&new);
        assert_eq!(got.len(), 1);
        assert_eq!(
            got[0].1.as_ref().unwrap().body,
            b"3",
            "the same model answered, after the pre-restart request reached it"
        );
        sim.with_remote("pay.test", |m: &Counter| assert_eq!(m.0, 3));
    }
}
