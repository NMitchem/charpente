//! The merge: how one more report changes the two values kept per product.
//! Both are pure folds under a strict total order on distinct reports, so the
//! value after any set of reports is the same whatever order they arrived in.
//!
//! A report's identity is all of what it says: product, source, observation
//! time and amount (`report_key`). Two distinct reports for one product
//! therefore differ in at least one of source, time or amount, and both
//! orders below rank on all three, so no two distinct reports ever tie.

use std::cmp::min_by_key;

/// One report, as the merge sees it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Obs {
    pub amount_cents: i64,
    pub source: u64,
    pub observed_at_ms: u64,
}

/// The lowest price: the smallest amount; on equal amounts the earlier
/// observation, then the smaller source id.
pub fn lowest(kept: Option<Obs>, incoming: Obs) -> Obs {
    match kept {
        Some(kept) => min_by_key(kept, incoming, lowest_rank),
        None => incoming,
    }
}

/// The latest report: the latest observation time; on equal times the
/// smaller source id, then (the same source observing twice at one instant)
/// the smaller amount.
pub fn latest(kept: Option<Obs>, incoming: Obs) -> Obs {
    match kept {
        Some(kept) => min_by_key(kept, incoming, latest_rank),
        None => incoming,
    }
}

fn lowest_rank(obs: &Obs) -> (i64, u64, u64) {
    (obs.amount_cents, obs.observed_at_ms, obs.source)
}

/// Negated time, so the smallest rank is the latest report. A plain tuple,
/// not `std::cmp::Reverse`: cargo-mutants cannot build a `Reverse`, so a key
/// written with one has no viable mutant and goes untested (FINDINGS.md).
fn latest_rank(obs: &Obs) -> (i128, u64, i64) {
    (-i128::from(obs.observed_at_ms), obs.source, obs.amount_cents)
}

/// The refusal for an amount that is not a price.
pub fn check_amount(amount_cents: i64) -> Result<i64, String> {
    if amount_cents > 0 {
        Ok(amount_cents)
    } else {
        Err(format!(
            "amount_cents must be a whole number of cents, 1 or more — got {amount_cents}"
        ))
    }
}

/// A report's identity. A redelivered report carries the same four values,
/// so it finds the row its first delivery wrote.
pub fn report_key(product: u64, source: u64, observed_at_ms: u64, amount_cents: i64) -> String {
    format!("{product}/{source}/{observed_at_ms}/{amount_cents}")
}
