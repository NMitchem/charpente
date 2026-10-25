//! `charpente test --sim --dir crates/outbound-fixture` dogfoods this file:
//! the fixture's `ChargeOrder` under each universe's faults, against a model
//! of `pay.fixture` that dedupes by the `idempotency-key` header, as
//! `fake_remote` does, and records every call it applies, so a second charge
//! for one order is seen where the money would move.

use std::collections::BTreeMap;
use std::time::Duration;

use charpente::View;
use charpente::core::io::{Clock as _, Entropy as _, IdempotencyKey};
use charpente::core::job::{JobRow, JobStatus};
use charpente::core::table::Codec as _;
use charpente::http::{App, Method, Request};
use charpente::kernel::{Kernel, SimSource};
use charpente_sim::Sim;
use charpente_sim::checks::{
    boot_through_fallout, calls_have_durable_jobs, clock_fallout, clock_reopens_after,
    durable_corruption_fired, is_fault_fallout, keys_are_per_job, outbound_settles_by_horizon,
};
use charpente_sim::outbound::{Idempotent, Recorder};
use outbound_fixture::{ChargeOrder, Order};

// `re-called` is what makes "once" a claim: measured in 355 of 1024 universes
// (master seed 0x222517d092c8c3b7), so 64 universes miss it all with
// probability about 1e-12 (1e-9 at a one-sided 1e-6 lower bound on the rate).
charpente_sim::sim_tests! {
    orders_charge_once require ["outbound:checked", "outbound-fixture:re-called"],
}

const SEC: u64 = 1_000_000_000;

/// How many boots one `App::open` here gets, each failed boot followed by a
/// restart, as a supervised process restarts (`checks::boot_through_fallout`).
const BOOT_TRIES: u32 = 8;

/// Faulted rounds before the tail, each ending in a power cycle.
const ROUNDS: u32 = 3;

/// How many refused opens the fault-free tail waits out. With the clock's
/// faults ended, one wait reaches the instant the refusal names; the refusal
/// after the last of these waits, a bug, ends the universe by name.
const CLOCK_WAITS: u32 = 2;

/// Fault fallout here: the families an error's text alone excuses, and the
/// kernel's clock stop or refusal where the harness's clock faults fired.
fn fallout(sim: &Sim, text: &str) -> bool {
    is_fault_fallout(text) || clock_fallout(sim, text)
}

/// Ends a universe that adjudicated nothing, marking `key`, the arm's name:
/// the harness reports how many universes gave up, and the CLI groups them by
/// `reason`, whose `detail` is this run's own text.
fn gave_up(
    sim: &Sim,
    key: &'static str,
    reason: &'static str,
    detail: impl std::fmt::Display,
) -> Result<(), String> {
    sim.cover(key);
    sim.give_up(reason, detail);
    Ok(())
}

fn boot(sim: &Sim) -> Result<Option<App>, String> {
    boot_through_fallout(
        sim,
        BOOT_TRIES,
        |t| fallout(sim, t),
        || outbound_fixture::build(Box::new(sim.disk()), Box::new(sim.entropy())),
    )
    .map_err(|e| format!("the app failed to open non-injected: {e}"))
}

/// One kernel over `source`. Fault fallout stops it (`Ok(Err(text))`, the
/// fallout's text); anything else fails the universe.
fn run(sim: &Sim, app: App, source: &mut SimSource) -> Result<Result<App, String>, String> {
    let outbound = sim
        .outbound(&app.outbound_hosts())
        .map_err(|e| e.to_string())?;
    let mut kernel = match Kernel::with_outbound(app, Box::new(sim.clock()), Box::new(outbound)) {
        Ok(kernel) => kernel,
        Err(e) if fallout(sim, &e.to_string()) => return Ok(Err(e.to_string())),
        Err(e) => return Err(format!("the kernel build failed non-injected: {e}")),
    };
    match kernel.run(source) {
        Ok(()) => Ok(Ok(kernel.into_app())),
        Err(e) if fallout(sim, &e.to_string()) => Ok(Err(e.to_string())),
        Err(e) => Err(format!("the kernel stopped non-injected: {e}")),
    }
}

/// Up to `ROUNDS` faulted rounds, each booting, posting three orders when the
/// store holds none (a client retries orders that never became durable), and
/// power-cycling at a drawn moment, a call possibly in flight. Then a
/// fault-free tail past the last backoff. Properties: the three outbound
/// checkers; the remote applied no order twice, and every Done charge's order
/// exactly once; every order paid exactly when its charge is Done.
fn orders_charge_once(sim: &mut Sim) -> Result<(), String> {
    sim.remote(
        "pay.fixture",
        Idempotent::header("idempotency-key", Recorder::new()),
    );

    for _round in 0..ROUNDS {
        let Some(mut app) = boot(sim)? else {
            // A durable refusal gave the universe up (`boot_through_fallout`
            // recorded the reason, so only the arm is marked here); boots that
            // ran out end this round, and the next one boots again.
            if sim.gave_up().is_some() {
                sim.cover("outbound-fixture:gave-up:boot");
                return Ok(());
            }
            sim.restart();
            continue;
        };
        let start = sim.now().as_nanos();
        let cut = start + SEC + sim.entropy().next_u64() % (8 * SEC);
        let mut source = SimSource::new(Box::new(sim.clock()), cut);
        let orders = app
            .db_for_tests()
            .snapshot()
            .and_then(|s| s.scan::<Order>())
            .map_err(|e| e.to_string());
        match orders {
            Ok(orders) if orders.is_empty() => {
                for i in 0..3 {
                    source.push_at(start + i * SEC / 10, Request::new(Method::Post, "/orders"));
                }
            }
            Ok(_) => {}
            Err(e) if fallout(sim, &e) => {
                drop(app);
                sim.restart();
                continue;
            }
            Err(e) => return Err(format!("reading the orders failed non-injected: {e}")),
        }
        drop(run(sim, app, &mut source)?);
        sim.restart();
    }

    // The fault-free tail (GUIDE §8): 8 attempts back off 0.5 s doubling to
    // 32 s (63.5 s in all), each with a 2 s deadline, well inside 600 s.
    sim.quiesce_disk_faults();
    sim.quiesce_outbound_faults();
    sim.quiesce_clock_faults();
    let mut waits = 0;
    let mut app = loop {
        let Some(app) = boot(sim)? else {
            return if sim.gave_up().is_some() {
                sim.cover("outbound-fixture:gave-up:boot");
                Ok(())
            } else {
                Err("the fault-free tail could not boot".into())
            };
        };
        let horizon = sim.now().as_nanos() + 600 * SEC;
        let e = match run(
            sim,
            app,
            &mut SimSource::new(Box::new(sim.clock()), horizon),
        )? {
            Ok(app) => break app,
            Err(e) => e,
        };
        // A step back in an earlier round left the clock behind the time the
        // store recorded, and the open is refused. With the clock's faults
        // ended its offset is frozen, so it reaches the instant the refusal
        // names as virtual time passes: wait, and restart the process. Only
        // that refusal: a quiesced clock cannot produce a mid-run stop, so
        // one is a platform bug and fails the run.
        let at = match clock_reopens_after(&e) {
            Some(at) if e.contains("refusing to open") => at,
            _ => return Err(format!("the fault-free tail's kernel stopped: {e}")),
        };
        if waits == CLOCK_WAITS {
            return gave_up(
                sim,
                "outbound-fixture:gave-up:clock",
                "the fault-free tail was still refused after waiting out every refusal it allows",
                format!("refused {} times, the last after waiting: {e}", waits + 1),
            );
        }
        waits += 1;
        sim.cover("outbound-fixture:waited-out-clock");
        let wait = at.as_nanos().saturating_sub(sim.clock().wall().as_nanos()) + SEC;
        sim.clock().sleep(Duration::from_nanos(wait));
        sim.restart_process();
    };

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
                "durable corruption cut an enqueue whose call had already reached the \
                 remote, so exactly-once was not adjudicated",
                e,
            );
        }
        return Err(e);
    }

    // What the remote applied, per order. `Idempotent` hands its inner model
    // only a key's first call, or every keyless one.
    let applied = sim.with_remote(
        "pay.fixture",
        |m: &Idempotent<Recorder>| -> Result<BTreeMap<u64, u32>, String> {
            let mut applied = BTreeMap::new();
            for call in m.inner().calls() {
                let order = std::str::from_utf8(&call.body)
                    .ok()
                    .and_then(|body| body.strip_prefix("order="))
                    .and_then(|n| n.parse::<u64>().ok())
                    .ok_or_else(|| {
                        format!(
                            "pay.fixture applied a call whose body is not order=<id>: {:?}",
                            String::from_utf8_lossy(&call.body)
                        )
                    })?;
                *applied.entry(order).or_insert(0) += 1;
            }
            Ok(applied)
        },
    )?;
    if let Some((order, n)) = applied.iter().find(|(_, n)| **n > 1) {
        return Err(format!(
            "double charge: pay.fixture applied order {order} {n} times — a re-call carried no \
             idempotency key the remote could dedupe on"
        ));
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
        let n = applied.get(&job.order.raw()).copied().unwrap_or(0);
        if row.status == JobStatus::Done && n != 1 {
            return Err(format!(
                "job {} is Done, but pay.fixture applied order {} {n} times — settle took a reply \
                 for a charge that was never applied",
                row.id.raw(),
                job.order.raw()
            ));
        }
        charges += 1;
    }
    if charges == 0 {
        return gave_up(
            sim,
            "outbound-fixture:gave-up:no-order",
            "no POST /orders became durable, so no charge was adjudicated",
            "",
        );
    }

    // The "once" above judged something only where one key reached the
    // remote twice: a re-call after a lost reply, a deadline, or a restart.
    let mut delivered: BTreeMap<IdempotencyKey, u32> = BTreeMap::new();
    for d in sim.outbound_deliveries() {
        *delivered.entry(d.key).or_insert(0) += 1;
    }
    if delivered.values().any(|n| *n > 1) {
        sim.cover("outbound-fixture:re-called");
    }
    sim.cover("outbound:checked");
    Ok(())
}
