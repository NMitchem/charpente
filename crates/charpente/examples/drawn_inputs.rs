//! Drawn inputs: a property that holds for ANY bids, checked on bids the
//! simulator draws, and shrunk to the fewest and simplest when it fails.
//!
//! Run it: `cargo run -p charpente --example drawn_inputs`
//!
//! The app keeps the highest bid per lab, and the check posts 2 to 6 drawn
//! bids, in a drawn order, through the production kernel loop. Hand-written
//! amounts that happen to ascend cannot tell "keep the highest" from "keep
//! the latest"; amounts the run draws can, and a failure comes back as the
//! two bids that break it (GUIDE §8, "Drawing inputs"). Delete
//! `place_bid`'s `Some(best) if best.amount >= amount` arm, so the latest bid
//! always wins, and run it again to watch that.
//!
//! Faults end the faulted phase, not the run. After a fault, or a power cycle,
//! the app is rebooted in a fault-free tail and the recovered state is judged
//! against the same bounds: "every acked bid is held" is exactly the
//! durability claim. Giving up at the first fault instead would skip nearly
//! half the runs and test nothing about them (GUIDE §8, "A fault-free tail for
//! an end-of-run check").
//!
//! The same file is the shape of a `tests/sim.rs`: `sim_tests!` supplies
//! `main`, which parses `--runs`, `--seed` and the rest, runs the batch and
//! exits with its code.

use charpente::core::io::Clock as _;
use charpente::http::{ApiError, App, AppConfig, Method, Request};
use charpente::kernel::{Kernel, SimSource};
use charpente::{Id, Schema, Tx, View, Wire, route, routes, table};
use charpente_sim::Sim;
use charpente_sim::checks::{
    boot_through_fallout, clock_fallout, clock_reopens_after, durable_corruption_fired,
    is_fault_fallout,
};
use serde::{Deserialize, Serialize};

#[table(global)]
struct Best {
    id: Id<Best>,
    amount: u64,
    memo: String,
}

#[derive(Wire, Serialize, Deserialize)]
struct Bid {
    lab: Id<Best>,
    /// `u32`, not `u64`: a bare `u64` cannot cross the wire (GUIDE §5).
    amount: u32,
    memo: String,
}

/// Keeps the highest bid per lab, and the memo that came with it.
#[route(POST "/bids", public)]
fn place_bid(tx: &mut Tx, bid: Bid) -> Result<Id<Best>, ApiError> {
    let id = bid.lab;
    let amount = u64::from(bid.amount);
    match tx.get(id)? {
        Some(best) if best.amount >= amount => {}
        _ => tx.put(&Best {
            id,
            amount,
            memo: bid.memo,
        }),
    }
    Ok(id)
}

charpente_sim::sim_tests! {
    the_highest_acked_bid_stands require ["restart", "sometimes:judged_a_lab"],
}

const SEC: u64 = 1_000_000_000; // SimSource timestamps are virtual nanoseconds
const MS: u64 = 1_000_000;

fn open_app(sim: &Sim, fallout: impl Fn(&str) -> bool) -> Result<Option<App>, String> {
    let open = || {
        App::open(
            Box::new(sim.disk()),
            Schema::new().table::<Best>(),
            Box::new(sim.entropy()),
            AppConfig::default(),
        )
        .map(|app| app.mount(routes![place_bid]))
    };
    boot_through_fallout(sim, 8, fallout, open).map_err(|e| e.to_string())
}

/// The fault-free tail: the faults that cost a bid its ack are over, so the
/// recovery is judged whole. It waits out only a clock refusal, the one stop
/// a frozen clock cures: an earlier step back left the clock behind the time
/// the store recorded, so wait past the instant the refusal names and restart.
fn recover(sim: &Sim) -> Result<Option<Kernel>, String> {
    sim.quiesce_disk_faults();
    sim.quiesce_clock_faults();
    let fallout = |text: &str| is_fault_fallout(text) || clock_fallout(sim, text);
    let mut waits = 0;
    loop {
        let Some(app) = open_app(sim, fallout)? else {
            // Only a durable refusal gives the run up here.
            return match sim.gave_up() {
                Some(_) => Ok(None),
                None => Err("the fault-free tail could not boot".to_string()),
            };
        };
        let refusal = match Kernel::new(app, Box::new(sim.clock())) {
            Ok(kernel) => return Ok(Some(kernel)),
            Err(e) => e.to_string(),
        };
        let Some(at) = clock_reopens_after(&refusal).filter(|_| clock_fallout(sim, &refusal))
        else {
            return Err(refusal);
        };
        if waits == 2 {
            sim.give_up("the tail was refused after two waits", refusal);
            return Ok(None);
        }
        waits += 1;
        let wait = at.as_nanos().saturating_sub(sim.clock().wall().as_nanos()) + SEC;
        sim.clock().sleep(std::time::Duration::from_nanos(wait));
        sim.restart_process();
    }
}

/// The highest bid per lab stands, whatever order the bids arrive in, and
/// through a power cycle. Three claims, none of which names a number:
/// - a lab with an acked bid holds at least the highest amount acked for it;
/// - no lab holds more than the highest amount anyone sent it;
/// - the memo a lab holds is one that was sent for that lab.
///
/// A bid with no ack may or may not have landed before the fault, and the
/// tail re-sends it, so the claims bound the row rather than pin it.
fn the_highest_acked_bid_stands(sim: &mut Sim) -> Result<(), String> {
    // Every draw is named and recorded: a failure lists them on its
    // `inputs:` line, shrunk, and `--inputs` replays them.
    let input = sim.input();
    let count = input.between("bids", 2, 6);
    let mut labs: Vec<u64> = (0..count).map(|_| *input.pick("lab", &[1, 2])).collect();
    input.shuffle("order", &mut labs);
    let bids: Vec<Bid> = labs
        .into_iter()
        .map(|lab| Bid {
            lab: Id::from_raw(lab),
            amount: input.between("amount", 1, 1000) as u32,
            memo: input.text("memo", &["rush", "bulk", "gift", "late"], 1, 2),
        })
        .collect();

    // The faulted phase. Fault fallout ends it, not the run: the replies
    // already sent stand, and the tail judges the recovery.
    let fallout = |text: &str| is_fault_fallout(text) || clock_fallout(sim, text);
    let Some(app) = open_app(sim, is_fault_fallout)? else {
        // A durable refusal already gave the run up by name.
        if sim.gave_up().is_none() {
            sim.give_up("the app never booted", "");
        }
        return Ok(());
    };
    let mut source = SimSource::new(Box::new(sim.clock()), 100 * MS);
    for (at, b) in bids.iter().enumerate() {
        source.push_at(at as u64 * MS, Request::new(Method::Post, "/bids").json(b));
    }
    let responses = source.responses();
    match Kernel::new(app, Box::new(sim.clock())) {
        Ok(mut kernel) => match kernel.run(&mut source) {
            Err(e) if !fallout(&e.to_string()) => return Err(e.to_string()),
            _ => {}
        },
        Err(e) if !fallout(&e.to_string()) => return Err(e.to_string()),
        Err(_) => {}
    }
    // The kernel answers in arrival order, one reply per POST.
    let mut statuses: Vec<u16> = responses
        .lock()
        .map_err(|_| "responses lock poisoned".to_string())?
        .iter()
        .map(|(status, _)| *status)
        .collect();
    statuses.resize(bids.len(), 0); // no reply: the fault came first

    sim.restart();
    let Some(mut kernel) = recover(sim)? else {
        return Ok(());
    };
    // A client re-sends the bids it never saw acked. The fault-free tail acks
    // them, so every run judges a merge that saw its bids, in a drawn order,
    // across a power cycle.
    let mut retry = SimSource::new(Box::new(sim.clock()), 100 * MS);
    let unacked: Vec<usize> = (0..bids.len()).filter(|i| statuses[*i] != 200).collect();
    for (at, i) in unacked.iter().enumerate() {
        retry.push_at(
            at as u64 * MS,
            Request::new(Method::Post, "/bids").json(&bids[*i]),
        );
    }
    let retried = retry.responses();
    kernel.run(&mut retry).map_err(|e| e.to_string())?;
    let replies: Vec<u16> = retried
        .lock()
        .map_err(|_| "responses lock poisoned".to_string())?
        .iter()
        .map(|(status, _)| *status)
        .collect();
    for (i, status) in unacked.iter().zip(replies) {
        statuses[*i] = status;
    }
    let snap = kernel.app().db().snapshot().map_err(|e| e.to_string())?;
    let mut judged = false;
    for lab in [1u64, 2] {
        let sent = || {
            bids.iter()
                .zip(&statuses)
                .filter(|(b, _)| b.lab.raw() == lab)
        };
        let acked_max = sent()
            .filter(|(_, s)| **s == 200)
            .map(|(b, _)| b.amount)
            .max();
        let sent_max = sent().map(|(b, _)| b.amount).max();
        let held = snap
            .get::<Best>(Id::from_raw(lab))
            .map_err(|e| e.to_string())?;
        let Some(sent_max) = sent_max else {
            continue; // no bid was sent to this lab
        };
        let Some(held) = held else {
            let Some(acked_max) = acked_max else {
                continue; // nothing acked, nothing landed: nothing it must hold
            };
            // Acked-but-lost is excused ONLY where the harness injected
            // durable corruption into this run (GUIDE §8, "Giving up").
            if durable_corruption_fired(sim) {
                sim.give_up("durable corruption fired, so the acked row is excused", "");
                return Ok(());
            }
            return Err(format!(
                "lab {lab} has a bid of {acked_max} acked but no Best row"
            ));
        };
        if let Some(acked_max) = acked_max
            && held.amount < u64::from(acked_max)
        {
            if durable_corruption_fired(sim) {
                sim.give_up("durable corruption fired, so the acked row is excused", "");
                return Ok(());
            }
            return Err(format!(
                "lab {lab} holds {} but a bid of {acked_max} was acked: the highest bid must stand, whatever order bids arrive in",
                held.amount
            ));
        }
        if held.amount > u64::from(sent_max) {
            return Err(format!(
                "lab {lab} holds {} but the highest bid sent was {sent_max}",
                held.amount
            ));
        }
        if !sent().any(|(b, _)| b.memo == held.memo) {
            return Err(format!(
                "lab {lab} holds the memo {:?}, which no bid for it carried",
                held.memo
            ));
        }
        judged = true;
    }
    if judged {
        sim.cover("sometimes:judged_a_lab");
    } else {
        sim.give_up("no bid landed, so there was nothing to judge", "");
    }
    Ok(())
}
