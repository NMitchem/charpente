//! Each order is charged once. A drawn 48 to 96 orders are placed under the
//! run's faults, the process is power-cycled, and a fault-free tail lets every
//! charge settle; `charpente_sim::drive` runs the steps (GUIDE §8).

mod gateway;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use charpente::View;
use charpente::core::job::{JobRow, JobStatus};
use charpente::core::table::Codec as _;
use charpente::http::{Method, Request};
use charpente_sim::Sim;
use charpente_sim::drive::Drive;
use double_charge::{Charge, Order, build_app};
use gateway::Gateway;

// `fault:outbound_lost_reply` loses the gateway's reply to a charge, and the
// job calls again under the same key. The gateway dedupes on that key, so a
// lost gateway reply never charges twice: a second charge needs a second
// `Charge` job, that is, a second placement. Requiring the fault makes the
// key's dedupe part of what a green batch tested.
charpente_sim::sim_tests! {
    each_order_is_charged_once require ["fault:outbound_lost_reply", "outbound:checked"],
}

fn each_order_is_charged_once(sim: &mut Sim) -> Result<(), String> {
    let orders = sim.input().between("orders", 48, 96);
    let calm = Arc::new(AtomicBool::new(false));
    sim.remote("pay.example", Gateway::new(Arc::clone(&calm)));

    let requests: Vec<(Duration, Request)> = (1..=orders)
        .map(|id| {
            (
                Duration::from_millis(id * 20),
                Request::new(Method::Post, format!("/orders/{id}")),
            )
        })
        .collect();
    let drive = Drive::new(sim, |sim| {
        build_app(Box::new(sim.disk()), Box::new(sim.entropy()))
    });
    let phase = drive.faulted_phase(&requests, Duration::from_secs(3))?;
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
    if let Some((order, _)) = per_order.iter().find(|(_, n)| **n > 1) {
        // Two Charge jobs for one order mean the route placed it twice; one job
        // applied twice is a re-call that carried no key. That branch is
        // unreachable here because `Charge::call` always sends the
        // idempotency-key header (src/lib.rs); it stays so the message is
        // truthful if the demo is edited.
        let placed = charges
            .iter()
            .filter(|(_, job)| job.order.raw() == *order)
            .count();
        // This workload posts each order once, and a re-send carries its
        // first copy's Idempotency-Key, which charpente answers from its
        // record. A second placement is a second POST of the order under a
        // new key, or charpente running a re-sent write again.
        if placed > 1 {
            return Err(format!(
                "order {order} charged twice: it was placed twice, and each placing enqueued \
                 its own charge. A re-sent POST carries its first copy's Idempotency-Key and is \
                 answered from its record, so either a second POST of this order came under a \
                 new key (answer an order that is already stored), or charpente ran a re-sent \
                 write again, a platform defect to report with this seed"
            ));
        }
        return Err(format!(
            "order {order} charged twice: a re-call carried no idempotency key the gateway could \
             dedupe on"
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
