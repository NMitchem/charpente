//! The order-sensitive merge oracle's app. An outside agent wrote "latest
//! wins" where "max amount" was meant; hand-scripted inputs, whose amounts
//! happened to ascend, passed 119 runs. `tests/sim.rs` shows drawn inputs
//! catching it and shrinking it to two arrivals.

use charpente::core::io::{Disk, Entropy};
use charpente::http::{ApiError, App, AppConfig};
use charpente::{Id, Schema, Tx, View, Wire, route, routes, table};
use serde::{Deserialize, Serialize};
use std::io;

/// The best bid per lab; the row id is the lab.
#[table(global)]
pub struct Best {
    pub id: Id<Best>,
    pub amount: u64,
}

#[derive(Wire, Serialize, Deserialize)]
pub struct Bid {
    pub lab: Id<Best>,
    /// `u32`, not `u64`: a bare `u64` cannot cross the wire (it loses precision in JS).
    pub amount: u32,
}

/// How a bid merges into its lab's `Best`.
pub enum Merge {
    /// The correct merge: the largest amount ever acked for the lab.
    Max,
    /// The naive victim (DESIGN's victim pattern): the last arrival wins.
    /// It exists only so the oracle has a bug to catch; never mount it for real.
    Latest,
}

#[route(POST "/bids", public)]
fn bid_max(tx: &mut Tx, bid: Bid) -> Result<Id<Best>, ApiError> {
    let id = bid.lab;
    let amount = tx.get(id)?.map_or(u64::from(bid.amount), |b: Best| {
        b.amount.max(u64::from(bid.amount))
    });
    tx.put(&Best { id, amount });
    Ok(id)
}

#[route(POST "/bids", public)]
fn bid_latest(tx: &mut Tx, bid: Bid) -> Result<Id<Best>, ApiError> {
    let id = bid.lab;
    tx.put(&Best {
        id,
        amount: u64::from(bid.amount),
    });
    Ok(id)
}

pub fn build_app(disk: Box<dyn Disk>, entropy: Box<dyn Entropy>, merge: Merge) -> io::Result<App> {
    let app = App::open(
        disk,
        Schema::new().table::<Best>(),
        entropy,
        AppConfig::default(),
    )?;
    Ok(match merge {
        Merge::Max => app.mount(routes![bid_max]),
        Merge::Latest => app.mount(routes![bid_latest]),
    })
}
