//! `charpente test --sim --dir crates/outbound-fixture` dogfoods this file:
//! the fixture's `ChargeOrder` under each universe's faults, against a model
//! of `pay.fixture` that dedupes by the `idempotency-key` header, as
//! `fake_remote` does over real TLS (oracle 7's sim half).

use charpente::View;
use charpente::core::io::{Entropy as _, OutboundReply};
use charpente::core::job::{JobRow, JobStatus};
use charpente::core::table::Codec as _;
use charpente::http::{App, Method, Request};
use charpente::kernel::{Kernel, SimSource};
use charpente_sim::Sim;
use charpente_sim::checks::{
    boot_through_fallout, calls_have_durable_jobs, durable_corruption_fired, is_fault_fallout,
    keys_are_per_job, outbound_settles_by_horizon,
};
use charpente_sim::outbound::{Idempotent, from_fn};
use outbound_fixture::{ChargeOrder, Order};

charpente_sim::sim_tests! {
    orders_charge_once require ["outbound:checked"],
}

const SEC: u64 = 1_000_000_000;

/// How many boots one `App::open` here gets, each failed boot followed by a
/// restart, as a supervised process restarts (`checks::boot_through_fallout`).
const BOOT_TRIES: u32 = 8;

/// Ends a universe that adjudicated nothing, marking `key`, the arm's name:
/// the harness reports how many universes gave up, not why.
fn gave_up(sim: &Sim, key: &'static str, reason: String) -> Result<(), String> {
    sim.cover(key);
    sim.give_up(reason);
    Ok(())
}

fn boot(sim: &Sim) -> Result<Option<App>, String> {
    boot_through_fallout(sim, BOOT_TRIES, is_fault_fallout, || {
        outbound_fixture::build(Box::new(sim.disk()), Box::new(sim.entropy()))
    })
    .map_err(|e| format!("the app failed to open non-injected: {e}"))
}

/// One kernel over `source`. Fault fallout stops it (`None`); anything else
/// fails the universe.
fn run(sim: &Sim, app: App, source: &mut SimSource) -> Result<Option<App>, String> {
    let outbound = sim
        .outbound(&app.outbound_hosts())
        .map_err(|e| e.to_string())?;
    let mut kernel = match Kernel::with_outbound(app, Box::new(sim.clock()), Box::new(outbound)) {
        Ok(kernel) => kernel,
        Err(e) if is_fault_fallout(&e.to_string()) => return Ok(None),
        Err(e) => return Err(format!("the kernel build failed non-injected: {e}")),
    };
    match kernel.run(source) {
        Ok(()) => Ok(Some(kernel.into_app())),
        Err(e) if is_fault_fallout(&e.to_string()) => Ok(None),
        Err(e) => Err(format!("the kernel stopped non-injected: {e}")),
    }
}

/// Three orders under the universe's faults, the box power-cycled at a drawn
/// moment (a call may be in flight), then a fault-free tail past the last
/// backoff. Properties: the three outbound checkers, and every order paid
/// exactly when its charge is Done.
fn orders_charge_once(sim: &mut Sim) -> Result<(), String> {
    sim.remote(
        "pay.fixture",
        Idempotent::header(
            "idempotency-key",
            from_fn(|_| OutboundReply {
                status: 200,
                headers: vec![],
                body: b"ch_1".to_vec(),
            }),
        ),
    );

    let Some(app) = boot(sim)? else {
        return gave_up(
            sim,
            "outbound-fixture:gave-up:boot",
            format!("the app never booted in {BOOT_TRIES} boots"),
        );
    };
    let start = sim.now().as_nanos();
    let cut = start + SEC + sim.entropy().next_u64() % (8 * SEC);
    let mut source = SimSource::new(Box::new(sim.clock()), cut);
    for i in 0..3 {
        source.push_at(start + i * SEC / 10, Request::new(Method::Post, "/orders"));
    }
    drop(run(sim, app, &mut source)?);
    sim.restart();

    // The fault-free tail (GUIDE §8): 8 attempts back off 0.5 s doubling to
    // 32 s (63.5 s in all), each with a 2 s deadline, well inside 600 s.
    sim.quiesce_disk_faults();
    sim.quiesce_outbound_faults();
    let Some(app) = boot(sim)? else {
        return if sim.gave_up().is_some() {
            Ok(())
        } else {
            Err("the fault-free tail could not boot".into())
        };
    };
    let horizon = sim.now().as_nanos() + 600 * SEC;
    let mut app = run(
        sim,
        app,
        &mut SimSource::new(Box::new(sim.clock()), horizon),
    )?
    .ok_or("the fault-free tail's kernel stopped")?;

    let snap = app.db_for_tests().snapshot().map_err(|e| e.to_string())?;
    keys_are_per_job(&snap)?;
    outbound_settles_by_horizon(&snap)?;
    if let Err(e) = calls_have_durable_jobs(sim, &snap) {
        // One sim box's only loss window is media corruption: a bit flip at
        // the restart can cut an enqueue whose call already reached the
        // remote (`checks::durable_corruption_fired`). Excused only where
        // that fired, and as a give-up, never a pass.
        if durable_corruption_fired(sim) {
            return gave_up(
                sim,
                "outbound-fixture:gave-up:media-cut",
                format!(
                    "durable corruption cut an enqueue whose call had already reached the \
                     remote, so exactly-once was not adjudicated: {e}"
                ),
            );
        }
        return Err(e);
    }

    let jobs = snap.scan::<JobRow>().map_err(|e| e.to_string())?;
    let mut charges = 0;
    for row in jobs.iter().filter(|r| r.outbound_key.is_some()) {
        let job = ChargeOrder::decode(&mut &row.payload[..]).map_err(|e| e.to_string())?;
        let Some(order) = snap.get::<Order>(job.order).map_err(|e| e.to_string())? else {
            return Err(format!(
                "job {} charges order {}, which is not in the store — POST /orders puts the \
                 order and enqueues its charge in one transaction",
                row.id.raw(),
                job.order.raw()
            ));
        };
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
        charges += 1;
    }
    if charges == 0 {
        return gave_up(
            sim,
            "outbound-fixture:gave-up:no-order",
            "no POST /orders became durable, so no charge was adjudicated".into(),
        );
    }
    sim.cover("outbound:checked");
    Ok(())
}
