//! renewly under the full fault matrix (GUIDE §8).
//!
//! Every property drives one scenario with `charpente_sim::drive`: two
//! workspaces with owners, customers, plans and one running subscription each
//! (seeded in the build closure, GUIDE §8 "Seed fixtures in the build
//! closure"), whose period ends a few seconds after boot, so its renewal is
//! called while a drawn provider outage may still be on. Drawn requests
//! (subscribe, cancel, retry, reads, and requests into the other workspace)
//! and two fixed owner retries per workspace run under the run's faults for
//! an hour of virtual time; then a power cycle, and a fault-free tail of
//! four days, so new subscriptions renew several times. The provider is a
//! model that applies each idempotency key once and records every charge.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use charpente::auth::{ApiKey, hash_token};
use charpente::core::io::{OutboundCall, OutboundReply, WallTime};
use charpente::core::job::{JobRow, JobStatus};
use charpente::core::table::Codec;
use charpente::http::{App, Method, Request, Response};
use charpente::{Id, View};
use charpente_sim::Sim;
use charpente_sim::checks::{
    calls_have_durable_jobs, dead_hooks_succeeded, durable_corruption_fired, keys_are_per_effect,
    outbound_settles_by_horizon,
};
use charpente_sim::drive::{Drive, Phase, Recovered};
use charpente_sim::outbound::Remote;
use charpente_sim::rng::Rng;
use renewly::{
    BILLING_HOST, Cancellation, Charge, Customer, DAY_NANOS, Notice, Plan, PeriodEnd, Retry,
    SubState, SubscribeBody, Subscription, Transition, TransitionBody, Workspace, build_app,
    customer_subject, refusal,
};

charpente_sim::sim_tests! {
    each_period_is_charged_at_most_once_and_never_early
        require ["renewly:renewed", "renewly:renewed-after-give-up", "fault:outbound_lost_reply"],
    a_given_up_renewal_is_past_due_with_exactly_one_notice
        require ["dead:Charge", "renewly:notice-judged", "renewly:retry-given-up-again"],
    acked_requests_survive_and_workspaces_stay_apart
        require ["restart", "renewly:acked-judged", "renewly:intrusion-refused"],
    a_refused_transition_names_what_the_state_allows
        require ["renewly:refused:cancelled", "renewly:refused:past_due", "renewly:refused:active"],
}

/// The two workspaces.
const WS: [u64; 2] = [1, 2];

/// The customers: (workspace, customer id).
const CUSTOMERS: [(u64, u64); 3] = [(1, 11), (1, 12), (2, 21)];

/// The plans: (workspace, plan id, name, price in cents, period in days).
const PLANS: [(u64, u64, &str, u64, u32); 3] = [
    (1, 1, "basic", 900, 1),
    (1, 2, "pro", 2500, 3),
    (2, 1, "solo", 500, 2),
];

/// The subscription each workspace starts with: (workspace, id, customer, plan).
const RUNNING: [(u64, u64, u64, u64); 2] = [(1, 100, 11, 1), (2, 200, 21, 1)];

fn owner_subject(ws: u64) -> String {
    format!("owner:{ws}")
}

fn owner_token(ws: u64) -> String {
    format!("tok_owner_{ws}")
}

fn customer_token(ws: u64, customer: u64) -> String {
    format!("tok_customer_{ws}_{customer}")
}

fn plan_of(ws: u64, plan: u64) -> Option<(u64, u64, &'static str, u64, u32)> {
    PLANS.iter().copied().find(|p| p.0 == ws && p.1 == plan)
}

/// One renewal the billing model applied.
#[derive(Clone, Debug)]
struct Applied {
    ws: u64,
    subscription: u64,
    seq: u32,
    amount_cents: u64,
    period_end: u64,
    sent_at: u64,
}

/// What the billing model saw, shared with the workload.
#[derive(Default)]
struct Ledger {
    applied: Vec<Applied>,
    /// Requests whose key was already applied: answered with the first
    /// reply, applying nothing.
    replayed: u64,
}

/// The billing model behind `billing.example.net`: it applies a request once
/// per idempotency-key header and answers a repeat of a key with the first
/// reply; a repeat with another body is a 409. Every applied renewal is
/// recorded in the shared ledger.
struct Billing {
    ledger: Arc<Mutex<Ledger>>,
    by_key: BTreeMap<String, (Vec<u8>, OutboundReply)>,
}

fn header<'a>(call: &'a OutboundCall, name: &str) -> Option<&'a str> {
    call.headers
        .iter()
        .find(|(n, _)| n.eq_ignore_ascii_case(name))
        .map(|(_, v)| v.as_str())
}

fn form_field(body: &str, name: &str) -> Option<u64> {
    body.split('&')
        .filter_map(|kv| kv.split_once('='))
        .find(|(k, _)| *k == name)
        .and_then(|(_, v)| v.parse().ok())
}

impl Remote for Billing {
    fn handle(&mut self, call: &OutboundCall, _rng: &mut Rng) -> OutboundReply {
        let key = header(call, "idempotency-key").map(str::to_string);
        if let Some((body, reply)) = key.as_ref().and_then(|k| self.by_key.get(k)) {
            if *body != call.body {
                return OutboundReply {
                    status: 409,
                    headers: Vec::new(),
                    body: b"key reused with another request".to_vec(),
                };
            }
            self.ledger.lock().expect("ledger").replayed += 1;
            return reply.clone();
        }
        let text = String::from_utf8_lossy(&call.body).to_string();
        let field = |name| form_field(&text, name).unwrap_or(u64::MAX);
        let mut ledger = self.ledger.lock().expect("ledger");
        ledger.applied.push(Applied {
            ws: field("workspace"),
            subscription: field("subscription"),
            seq: field("seq") as u32,
            amount_cents: field("amount_cents"),
            period_end: field("period_end"),
            sent_at: header(call, "x-renewly-sent-at")
                .and_then(|v| v.parse().ok())
                .unwrap_or(0),
        });
        let reply = OutboundReply {
            status: 200,
            headers: Vec::new(),
            body: format!("applied {}", ledger.applied.len()).into_bytes(),
        };
        if let Some(key) = key {
            self.by_key.insert(key, (call.body.clone(), reply.clone()));
        }
        reply
    }
}

/// The app on this run's disk.
fn app(sim: &Sim) -> std::io::Result<App> {
    build_app(Box::new(sim.disk()), Box::new(sim.entropy()))
}

/// Fault fallout, as every step of the drive reads it (GUIDE §8).
fn fallout(sim: &Sim, text: &str) -> bool {
    is_fault_fallout(text) || clock_fallout(sim, text)
}

/// Boots the app through fallout, as a supervisor would.
fn boot(sim: &Sim) -> Result<Option<App>, String> {
    boot_through_fallout(sim, 8, |t| fallout(sim, t), || app(sim)).map_err(|e| e.to_string())
}

/// Restarts the process after fallout: a power cycle when the disk crashed.
fn restart(sim: &mut Sim) {
    if sim.disk_crashed() {
        sim.restart();
    } else {
        sim.restart_process();
    }
}

/// The text of a response's body.
fn text(r: &Response) -> String {
    String::from_utf8_lossy(&r.body).to_string()
}

/// A bearer header value.
fn bearer(token: &str) -> String {
    format!("Bearer {token}")
}

/// The raw credential a route answered, a JSON string.
fn token_of(r: &Response) -> String {
    text(r).trim_matches('"').to_string()
}

/// The principals' credentials, all minted by the app's own routes.
struct Principals {
    owner: BTreeMap<u64, String>,
    customer: BTreeMap<(u64, u64), String>,
}

/// Sends one setup request, booting and rebooting through fallout. `None`
/// when fallout outlasted four tries.
fn setup_send(
    sim: &mut Sim,
    app: &mut Option<App>,
    make: impl Fn() -> Request,
) -> Result<Option<Response>, String> {
    for _ in 0..4 {
        if app.is_none() {
            *app = boot(sim)?;
            if app.is_none() {
                return Ok(None);
            }
        }
        let now = sim.clock().wall();
        let r = app.as_mut().expect("booted").handle_at(make(), now);
        if r.status >= 500 && fallout(sim, &text(&r)) {
            *app = None;
            restart(sim);
            continue;
        }
        return Ok(Some(r));
    }
    Ok(None)
}

/// Why a setup gives the run up.
const SETUP_FALLOUT: &str = "setup could not finish through fault fallout";
const SETUP_LOST_SIGNUP: &str = "a signup landed but its answer was lost to fallout, so its owner has no credential";
