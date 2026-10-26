//! A modelled remote: the payment gateway a job charges is a `Remote` the
//! run owns, with its own seeded `Rng`, so its variance (it is sometimes
//! busy, and it dedupes by idempotency key as a real one does) replays with
//! the seed.
//!
//! Run it: `cargo run -p charpente --example modelled_remote`
//!
//! A drawn 1 to 3 orders are placed under the run's faults, the process is
//! power-cycled, and a fault-free tail lets every charge settle. The claim:
//! the gateway applies no order twice, and an order is paid exactly when its
//! job is Done. Delete the idempotency key from `charge`'s call and
//! the gateway sees a fresh key on every retry, so a lost reply charges twice
//! and the run goes red (GUIDE §8, "The outside world: remote models").

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use charpente::core::io::{Clock as _, OutboundCall, OutboundReply};
use charpente::core::job::{JobRow, JobStatus};
use charpente::core::table::Codec as _;
use charpente::http::{ApiError, App, AppConfig, Method, Request};
use charpente::kernel::{JobError, Kernel, SimSource};
use charpente::outbound::{Call, Host, IdempotencyKey, Reply};
use charpente::{Id, Schema, Tx, View, job, jobs, route, routes, table};
use charpente_sim::Sim;
use charpente_sim::checks::{
    boot_through_fallout, calls_have_durable_jobs, clock_fallout, clock_reopens_after,
    durable_corruption_fired, is_fault_fallout, keys_are_per_job, outbound_settles_by_horizon,
};
use charpente_sim::outbound::Remote;
use charpente_sim::rng::Rng;

#[table(global)]
struct Order {
    id: Id<Order>,
    paid: bool,
}

#[job(
    global,
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

/// Puts the order and enqueues its charge in one transaction.
#[route(POST "/orders/:id", public)]
fn place_order(tx: &mut Tx, id: Id<Order>) -> Result<Id<Order>, ApiError> {
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
// it, so a batch that never lost a reply would pass a double charge.
charpente_sim::sim_tests! {
    each_order_is_charged_once require ["fault:outbound_lost_reply", "outbound:checked"],
}

const SEC: u64 = 1_000_000_000; // SimSource timestamps are virtual nanoseconds

/// A fault, or a clock too far behind, ends the phase it hits, not the run.
fn fallout(sim: &Sim, text: &str) -> bool {
    is_fault_fallout(text) || clock_fallout(sim, text)
}

fn boot(sim: &Sim) -> Result<Option<App>, String> {
    boot_through_fallout(sim, 8, |t| fallout(sim, t), || build_app(sim)).map_err(|e| e.to_string())
}

/// One kernel over `source`: the app back, or the fallout's text.
fn run(sim: &Sim, app: App, source: &mut SimSource) -> Result<Result<App, String>, String> {
    let outbound = sim
        .outbound(&app.outbound_hosts())
        .map_err(|e| e.to_string())?;
    let mut kernel = match Kernel::with_outbound(app, Box::new(sim.clock()), Box::new(outbound)) {
        Ok(kernel) => kernel,
        Err(e) if fallout(sim, &e.to_string()) => return Ok(Err(e.to_string())),
        Err(e) => return Err(e.to_string()),
    };
    match kernel.run(source) {
        Ok(()) => Ok(Ok(kernel.into_app())),
        Err(e) if fallout(sim, &e.to_string()) => Ok(Err(e.to_string())),
        Err(e) => Err(e.to_string()),
    }
}

fn each_order_is_charged_once(sim: &mut Sim) -> Result<(), String> {
    let orders = sim.input().between("orders", 1, 3);
    let calm = Arc::new(AtomicBool::new(false));
    sim.remote(
        "pay.example",
        Gateway {
            calm: Arc::clone(&calm),
            by_key: BTreeMap::new(),
            charged: Vec::new(),
        },
    );

    // The faulted phase: post the orders, run past the first retries, and
    // power-cycle with a charge possibly in flight.
    if let Some(app) = boot(sim)? {
        let start = sim.now().as_nanos();
        let mut source = SimSource::new(Box::new(sim.clock()), start + 3 * SEC);
        for id in 1..=orders {
            source.push_at(
                start + id * SEC / 10,
                Request::new(Method::Post, format!("/orders/{id}")),
            );
        }
        drop(run(sim, app, &mut source)?);
    }
    sim.restart();

    // The fault-free tail (GUIDE §8): eight attempts back off 0.5 s doubling
    // to 32 s, each with a 2 s deadline, well inside the 600 s horizon.
    calm.store(true, Ordering::Relaxed);
    sim.quiesce_disk_faults();
    sim.quiesce_outbound_faults();
    sim.quiesce_clock_faults();
    let mut waits = 0;
    let mut app = loop {
        let Some(app) = boot(sim)? else {
            if sim.gave_up().is_none() {
                return Err("the fault-free tail could not boot".to_string());
            }
            return Ok(());
        };
        let horizon = sim.now().as_nanos() + 600 * SEC;
        let refusal = match run(
            sim,
            app,
            &mut SimSource::new(Box::new(sim.clock()), horizon),
        )? {
            Ok(app) => break app,
            Err(text) => text,
        };
        // An earlier step back left the clock behind the time the store
        // recorded: wait past the instant the refusal names, then restart.
        let Some(at) = clock_reopens_after(&refusal) else {
            return Err(format!("the fault-free tail's kernel stopped: {refusal}"));
        };
        if waits == 2 {
            sim.give_up("the tail was refused after two waits", refusal);
            return Ok(());
        }
        waits += 1;
        let wait = at.as_nanos().saturating_sub(sim.clock().wall().as_nanos()) + SEC;
        sim.clock().sleep(Duration::from_nanos(wait));
        sim.restart_process();
    };

    let snap = app.db_for_tests().snapshot().map_err(|e| e.to_string())?;
    keys_are_per_job(&snap)?;
    outbound_settles_by_horizon(&snap)?;
    if let Err(e) = calls_have_durable_jobs(sim, &snap) {
        // Media corruption can cut an enqueue whose call already reached the
        // gateway; excused only where it fired, and as a skipped run.
        if durable_corruption_fired(sim) {
            sim.give_up("durable corruption cut an enqueue after its call", e);
            return Ok(());
        }
        return Err(e);
    }

    let charged = sim.with_remote("pay.example", |g: &Gateway| g.charged.clone());
    let mut per_order: BTreeMap<u64, u32> = BTreeMap::new();
    for order in &charged {
        *per_order.entry(*order).or_insert(0) += 1;
    }
    if let Some((order, n)) = per_order.iter().find(|(_, n)| **n > 1) {
        return Err(format!(
            "double charge: pay.example applied order {order} {n} times — a re-call carried no \
             idempotency key the gateway could dedupe on"
        ));
    }
    let jobs = snap.scan::<JobRow>().map_err(|e| e.to_string())?;
    let mut judged = 0;
    for row in jobs.iter().filter(|row| row.outbound_key.is_some()) {
        let job = Charge::decode(&mut &row.payload[..]).map_err(|e| e.to_string())?;
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
