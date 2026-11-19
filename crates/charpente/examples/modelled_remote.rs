//! A modelled remote: the payment gateway a job charges is a `Remote` the
//! run owns, with its own seeded `Rng`, so its variance (it is sometimes
//! busy, and it dedupes by idempotency key as a real one does) replays with
//! the seed.
//!
//! Run it in your app: copy this file to `tests/sim.rs` and run
//! `charpente test --sim`. In a charpente checkout: `cargo run -p charpente
//! --example modelled_remote`, with any flag after `--` (`-- --master-seed 0x…`).
//!
//! A drawn 48 to 96 orders are placed under the run's faults, the process is
//! power-cycled, and a fault-free tail lets every charge settle. The claim:
//! the gateway applies no order twice, and an order is paid exactly when its
//! job is Done. Delete the idempotency key from `charge`'s call and
//! the gateway sees a fresh key on every retry, so a lost reply charges twice
//! and the run goes red (GUIDE §8, "The outside world: remote models").
//!
//! `charpente_sim::drive` runs the four steps (GUIDE §8, "A fault-free tail
//! for an end-of-run check"):
//! 1. Boot: the app boots through fault fallout, booting again as a
//!    supervisor would; a first boot that never gets past it leaves the
//!    faulted phase empty, and the run goes on.
//! 2. Faulted phase: the orders are placed under the run's faults, with the
//!    sim's outbound handle, since the app declares a host. A fault ends the
//!    phase, and the replies already sent stand.
//! 3. Tail, free of per-call faults (a drawn provider outage runs to its
//!    end): a power cycle, the disk, clock and outbound faults switched off,
//!    a clock refusal waited out, and the app booted again and run to a
//!    horizon past every backoff and deadline, then on until every charge is
//!    Done or Dead. The gateway's own switch is the workload's: it calms the
//!    gateway before the tail.
//! 4. Re-send: the tail places again every order the phase left unanswered,
//!    as a client that never saw its answer would.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use charpente::core::io::{OutboundCall, OutboundReply};
use charpente::core::job::{JobRow, JobStatus};
use charpente::core::table::Codec as _;
use charpente::http::{ApiError, App, AppConfig, Method, Request};
use charpente::kernel::JobError;
use charpente::outbound::{Call, Host, IdempotencyKey, Reply};
use charpente::{Id, Schema, Tx, View, job, jobs, route, routes, table};
use charpente_sim::Sim;
use charpente_sim::checks::{
    calls_have_durable_jobs, durable_corruption_fired, keys_are_per_effect,
    outbound_settles_by_horizon,
};
use charpente_sim::drive::Drive;
use charpente_sim::outbound::Remote;
use charpente_sim::rng::Rng;

#[table(global)]
struct Order {
    id: Id<Order>,
    paid: bool,
}

#[job(
    global,
    retain_dead,
    outbound = "pay.example",
    retry(max = 8, backoff = "500ms"),
    timeout = "2s"
)]
struct Charge {
    order: Id<Order>,
}

impl Charge {
    fn call(&self, _tx: &Tx, key: &IdempotencyKey) -> Result<Option<Call>, JobError> {
        Ok(Some(
            Call::post("/charge")
                .header("idempotency-key", key.as_str())
                .form(&[("order", self.order.raw().to_string())]),
        ))
    }

    fn settle(self, tx: &mut Tx, reply: Reply) -> Result<(), JobError> {
        match reply.status() {
            200 => {
                tx.put(&Order {
                    id: self.order,
                    paid: true,
                });
                Ok(())
            }
            s => Err(JobError::app(format!("pay.example answered {s}"))),
        }
    }
}

/// Puts the order and enqueues its charge in one transaction. An order that
/// is already stored is answered as placed and changes nothing: a client
/// re-sends an order it never saw answered, as the drive's tail does, and an
/// order whose first send landed before its answer was lost must not get a
/// second charge under a fresh key. The request is the order's id alone, so
/// a repeat is always the same request; a route whose request carries data
/// refuses other data under a stored id (GUIDE §8, "A route the tail re-sends
/// must be safe to receive twice"). Delete the three lines of the guard and
/// `-- --master-seed 0xf931428f591be3a4` fails 22 of 242 tested runs
/// (`--seed 0x74e75093966cb76d`, shrunk to `orders=48`).
#[route(POST "/orders/:id", public)]
fn place_order(tx: &mut Tx, id: Id<Order>) -> Result<Id<Order>, ApiError> {
    if tx.get(id)?.is_some() {
        return Ok(id);
    }
    tx.put(&Order { id, paid: false });
    tx.enqueue(Charge { order: id });
    Ok(id)
}

fn build_app(sim: &Sim) -> std::io::Result<App> {
    Ok(App::open(
        Box::new(sim.disk()),
        Schema::new().table::<Order>(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )?
    .mount(routes![place_order])
    .mount_jobs(jobs![Charge])
    .outbound(Host::new("pay.example")))
}

// The remote's side is not exactly-once. `settle` commits at most once, so
// the app's state stays exactly-once, but a remote that ignores the key may
// apply a retry twice; this gateway dedupes, so the claim below holds. The
// minted key belongs to one job and its redrives: `tx.reissue_after` from the
// job's own `on_dead` resends the same request under the same key. An effect
// the app enqueues again any other way (a route, a cron tick, another job) is
// a new job with a new key, so its call must send an id of the effect itself,
// as `order=<id>` does here.

/// The gateway. It answers 503 one request in five until the workload calms
/// it, and a request it accepts is applied once per idempotency key: a repeat
/// of a key gets the first charge back and moves no money.
struct Gateway {
    calm: Arc<AtomicBool>,
    by_key: BTreeMap<String, String>,
    /// The order of every charge applied, in the order they were applied.
    charged: Vec<u64>,
}

impl Remote for Gateway {
    fn handle(&mut self, call: &OutboundCall, rng: &mut Rng) -> OutboundReply {
        let reply = |status, body: &str| OutboundReply {
            status,
            headers: Vec::new(),
            body: body.as_bytes().to_vec(),
        };
        if !self.calm.load(Ordering::Relaxed) && rng.chance(0.2) {
            return reply(503, "busy");
        }
        let key = call
            .headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case("idempotency-key"))
            .map(|(_, value)| value.clone());
        if let Some(charge) = key.as_ref().and_then(|k| self.by_key.get(k)) {
            return reply(200, charge);
        }
        let order = std::str::from_utf8(&call.body)
            .ok()
            .and_then(|body| body.strip_prefix("order="))
            .and_then(|n| n.parse::<u64>().ok())
            .expect("the job posts order=<id>");
        let charge = format!("ch_{}", self.charged.len());
        self.charged.push(order);
        if let Some(key) = key {
            self.by_key.insert(key, charge.clone());
        }
        reply(200, &charge)
    }
}

// `fault:outbound_lost_reply` is what makes "once" a claim: a lost reply is
// the only way a charge is sent twice. The gateway's own 503s retry without
// it, so a batch that never lost a reply would pass a double charge. The
// axis is on in a quarter of runs, at a rate under 3% a call, so the order
// count is what makes it fire: with 48 to 96 orders a reply is lost in about
// one run in seventeen, and a 256-run batch misses it about once in five
// million batches. With 4 to 8 orders it missed 15 batches in 1000.
charpente_sim::sim_tests! {
    each_order_is_charged_once require ["fault:outbound_lost_reply", "outbound:checked"],
}

fn each_order_is_charged_once(sim: &mut Sim) -> Result<(), String> {
    let orders = sim.input().between("orders", 48, 96);
    let calm = Arc::new(AtomicBool::new(false));
    sim.remote(
        "pay.example",
        Gateway {
            calm: Arc::clone(&calm),
            by_key: BTreeMap::new(),
            charged: Vec::new(),
        },
    );

    // The faulted phase: post the orders 20 ms apart and run past the first
    // retries; the tail's power cycle may find a charge in flight.
    let requests: Vec<(Duration, Request)> = (1..=orders)
        .map(|id| {
            (
                Duration::from_millis(id * 20),
                Request::new(Method::Post, format!("/orders/{id}")),
            )
        })
        .collect();
    let drive = Drive::new(sim, build_app);
    let phase = drive.faulted_phase(&requests, Duration::from_secs(3))?;
    // The tail: eight attempts back off 0.5 s doubling to 32 s, each with a
    // 2 s deadline, well inside the 600 s horizon; `recover` would run on
    // past it for a charge still due there.
    calm.store(true, Ordering::Relaxed);
    let Some(recovered) = drive.recover(&phase, Duration::from_secs(600))? else {
        return Ok(()); // given up by name
    };

    let snap = recovered
        .kernel
        .app()
        .db()
        .snapshot()
        .map_err(|e| e.to_string())?;
    keys_are_per_effect(&snap)?;
    outbound_settles_by_horizon(&snap, recovered.wall)?;
    if let Err(e) = calls_have_durable_jobs(sim, &snap) {
        // Media corruption can cut an enqueue whose call already reached the
        // gateway; excused only where it fired, and as a skipped run.
        if durable_corruption_fired(sim) {
            sim.give_up("durable corruption cut an enqueue after its call", e);
            return Ok(());
        }
        return Err(e);
    }

    let jobs = snap.scan::<JobRow>().map_err(|e| e.to_string())?;
    let mut charges = Vec::new();
    for row in jobs.iter().filter(|row| row.outbound_key.is_some()) {
        charges.push((
            row,
            Charge::decode(&mut &row.payload[..]).map_err(|e| e.to_string())?,
        ));
    }
    let charged = sim.with_remote("pay.example", |g: &Gateway| g.charged.clone());
    let mut per_order: BTreeMap<u64, u32> = BTreeMap::new();
    for order in &charged {
        *per_order.entry(*order).or_insert(0) += 1;
    }
    if let Some((order, n)) = per_order.iter().find(|(_, n)| **n > 1) {
        // Two Charge jobs for one order mean the route placed it twice, each
        // job with its own key; one job applied twice is a re-call with no key.
        let placed = charges
            .iter()
            .filter(|(_, job)| job.order.raw() == *order)
            .count();
        if placed > 1 {
            return Err(format!(
                "double charge: pay.example applied order {order} {n} times, and the store holds \
                 {placed} Charge jobs for it — the order was placed twice (a re-sent POST \
                 /orders/{order} whose first send landed), so each call carried its own job's \
                 key: make place_order answer an order that is already stored"
            ));
        }
        return Err(format!(
            "double charge: pay.example applied order {order} {n} times — a re-call carried no \
             idempotency key the gateway could dedupe on"
        ));
    }
    let mut judged = 0;
    for (row, job) in &charges {
        let order = snap
            .get::<Order>(job.order)
            .map_err(|e| e.to_string())?
            .ok_or_else(|| format!("job {} charges an order that is not stored", row.id.raw()))?;
        if (row.status == JobStatus::Done) != order.paid {
            return Err(format!(
                "job {} is {:?} but order {} reads paid = {} — settle's write and the Done mark \
                 commit as one record",
                row.id.raw(),
                row.status,
                job.order.raw(),
                order.paid
            ));
        }
        let n = per_order.get(&job.order.raw()).copied().unwrap_or(0);
        if row.status == JobStatus::Done && n != 1 {
            return Err(format!(
                "job {} is Done, but pay.example applied order {} {n} times",
                row.id.raw(),
                job.order.raw()
            ));
        }
        judged += 1;
    }
    if judged == 0 {
        sim.give_up(
            "no POST /orders became durable, so no charge was judged",
            "",
        );
        return Ok(());
    }
    sim.cover("outbound:checked");
    Ok(())
}
