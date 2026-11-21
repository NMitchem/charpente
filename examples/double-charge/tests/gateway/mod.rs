//! The payment gateway `pay.example`, modelled: sometimes busy, and it
//! dedupes by idempotency key as a real one does.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use charpente::core::io::{OutboundCall, OutboundReply};
use charpente_sim::outbound::Remote;
use charpente_sim::rng::Rng;

/// It answers 503 one request in five until the workload calms it, and a
/// request it accepts is applied once per idempotency key: a repeat of a key
/// gets the first charge back and moves no money.
pub struct Gateway {
    pub calm: Arc<AtomicBool>,
    by_key: BTreeMap<String, String>,
    /// The order of every charge applied, in the order they were applied.
    pub charged: Vec<u64>,
}

impl Gateway {
    pub fn new(calm: Arc<AtomicBool>) -> Gateway {
        Gateway {
            calm,
            by_key: BTreeMap::new(),
            charged: Vec::new(),
        }
    }
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
