//! Drawn inputs: a property that holds for ANY bids, checked on bids the
//! simulator draws, and shrunk to the fewest and simplest when it fails.
//!
//! Run it in your app: copy this file to `tests/sim.rs` and run
//! `charpente test --sim`. In a charpente checkout: `cargo run -p charpente
//! --example drawn_inputs`, with any flag after `--` (`-- --master-seed 0x…`).
//!
//! The app keeps the highest bid per lab, and the check posts 2 to 6 drawn
//! bids, in a drawn order, through the production kernel loop. Hand-written
//! amounts that happen to ascend cannot tell "keep the highest" from "keep
//! the latest"; amounts the run draws can, and a failure comes back as the
//! two bids that break it (GUIDE §8, "Drawing inputs"). Delete
//! `place_bid`'s `Some(best) if best.amount >= amount` arm, so the latest bid
//! always wins, and run it again to watch that.
//!
//! Faults end the faulted phase, not the run, and the recovered state is
//! judged against the same bounds: "every acked bid is held" is exactly the
//! durability claim. Giving up at the first fault instead would skip nearly
//! half the runs and test nothing about them. `charpente_sim::drive` runs the
//! four steps (GUIDE §8, "A fault-free tail for an end-of-run check"):
//! 1. Boot: the app boots through fault fallout, booting again as a
//!    supervisor would; a first boot that never gets past it leaves the
//!    faulted phase empty, and the run goes on.
//! 2. Faulted phase: the bids are posted under the run's faults. A fault ends
//!    the phase, and the replies already sent stand.
//! 3. Fault-free tail: a power cycle, the faults switched off, a clock
//!    refusal waited out, and the app booted again.
//! 4. Re-send: the tail sends again every bid the phase left unanswered, as a
//!    client that never saw its ack would, and reports those answers apart.
//!    This check merges them into its judgement itself.
//!
//! The same file is the shape of a `tests/sim.rs`: `sim_tests!` supplies
//! `main`, which parses `--runs`, `--seed` and the rest, runs the batch and
//! exits with its code.

use std::time::Duration;

use charpente::http::{ApiError, App, AppConfig, Method, Request};
use charpente::{Id, Schema, Tx, View, Wire, route, routes, table};
use charpente_sim::Sim;
use charpente_sim::checks::durable_corruption_fired;
use charpente_sim::drive::Drive;
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

/// How the app builds itself on the run's disk. The drive calls it once per
/// boot.
fn build_app(sim: &Sim) -> std::io::Result<App> {
    Ok(App::open(
        Box::new(sim.disk()),
        Schema::new().table::<Best>(),
        Box::new(sim.entropy()),
        AppConfig::default(),
    )?
    .mount(routes![place_bid]))
}

/// The highest bid per lab stands, whatever order the bids arrive in, and
/// through a power cycle. Three claims, none of which names a number:
/// - a lab with an acked bid holds at least the highest amount acked for it;
/// - no lab holds more than the highest amount anyone sent it;
/// - the memo a lab holds is one that was sent for that lab.
///
/// A bid with no ack may or may not have landed before the fault, and the
/// tail re-sends it, so the claims bound the row rather than pin it. Merging
/// the re-sent answers in weakens the durability claim only for a lab whose
/// re-sent bid outranks a lost ack, since that lab holds the higher amount
/// either way; without the merge the check judges only the bids the faulted
/// phase acked.
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

    // One bid a millisecond, counted from the phase's start.
    let requests: Vec<(Duration, Request)> = bids
        .iter()
        .enumerate()
        .map(|(at, b)| {
            (
                Duration::from_millis(at as u64),
                Request::new(Method::Post, "/bids").json(b),
            )
        })
        .collect();

    let drive = Drive::new(sim, build_app);
    let phase = drive.faulted_phase(&requests, Duration::from_millis(100))?;
    let Some(recovered) = drive.recover(&phase, Duration::from_millis(100))? else {
        return Ok(()); // given up by name, on a durable refusal
    };
    // The tail acks what it re-sent, so every run judges a merge that saw its
    // bids, in a drawn order, across a power cycle. A re-sent bid's answer is
    // the tail's; any other bid's is the phase's, `None` for none.
    let statuses: Vec<Option<u16>> = (0..bids.len())
        .map(|i| recovered.status(i).or(phase.status(i)))
        .collect();
    let snap = recovered
        .kernel
        .app()
        .db()
        .snapshot()
        .map_err(|e| e.to_string())?;
    let mut judged = false;
    for lab in [1u64, 2] {
        let sent = || {
            bids.iter()
                .zip(&statuses)
                .filter(|(b, _)| b.lab.raw() == lab)
        };
        let acked_max = sent()
            .filter(|(_, s)| **s == Some(200))
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
