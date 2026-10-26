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
//! The same file is the shape of a `tests/sim.rs`: `sim_tests!` supplies
//! `main`, which parses `--runs`, `--seed` and the rest, runs the batch and
//! exits with its code.

use charpente::http::{ApiError, App, AppConfig, Method, Request};
use charpente::kernel::{Kernel, SimSource};
use charpente::{Id, Schema, Tx, View, Wire, route, routes, table};
use charpente_sim::Sim;
use charpente_sim::checks::{boot_through_fallout, clock_fallout, is_fault_fallout};
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
    the_highest_acked_bid_stands require ["sometimes:judged_a_lab"],
}

const MS: u64 = 1_000_000; // SimSource timestamps are virtual nanoseconds

/// The highest bid per lab stands, whatever order the bids arrive in. Three
/// claims, none of which names a number:
/// - a lab with an acked bid holds at least the highest amount acked for it;
/// - no lab holds more than the highest amount anyone sent it;
/// - the memo a lab holds is one that was sent for that lab.
///
/// Unacked bids may or may not have landed, so the claims bound the row
/// rather than pin it.
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

    let open = || {
        App::open(
            Box::new(sim.disk()),
            Schema::new().table::<Best>(),
            Box::new(sim.entropy()),
            AppConfig::default(),
        )
        .map(|app| app.mount(routes![place_bid]))
    };
    let Some(app) =
        boot_through_fallout(sim, 8, is_fault_fallout, open).map_err(|e| e.to_string())?
    else {
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
    let mut kernel = match Kernel::new(app, Box::new(sim.clock())) {
        Ok(kernel) => kernel,
        Err(e) if is_fault_fallout(&e.to_string()) || clock_fallout(sim, &e.to_string()) => {
            sim.give_up("a fault stopped the kernel before it opened", e);
            return Ok(());
        }
        Err(e) => return Err(e.to_string()),
    };
    if let Err(e) = kernel.run(&mut source) {
        let text = e.to_string();
        if !(is_fault_fallout(&text) || clock_fallout(sim, &text)) {
            return Err(text);
        }
        sim.give_up("a fault stopped the kernel mid-run", text);
        return Ok(());
    }

    // The kernel answers in arrival order, one reply per POST.
    let statuses: Vec<u16> = responses
        .lock()
        .map_err(|_| "responses lock poisoned".to_string())?
        .iter()
        .map(|(status, _)| *status)
        .collect();
    let snap = match kernel.app().db().snapshot() {
        Ok(snap) => snap,
        Err(e) if is_fault_fallout(&e.to_string()) => {
            sim.give_up("a fault stopped the read-back", e);
            return Ok(());
        }
        Err(e) => return Err(e.to_string()),
    };
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
        let (Some(acked_max), Some(sent_max)) = (acked_max, sent_max) else {
            continue; // no acked bid for this lab: nothing it must hold
        };
        let Some(held) = held else {
            return Err(format!("lab {lab} has an acked bid but no Best row"));
        };
        if held.amount < u64::from(acked_max) {
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
        sim.give_up("no lab had an acked bid, so there was nothing to judge", "");
    }
    Ok(())
}
