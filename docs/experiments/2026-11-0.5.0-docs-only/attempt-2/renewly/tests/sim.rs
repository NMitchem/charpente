//! renewly's behaviours, proved under the full fault matrix of
//! `charpente test --sim` (GUIDE §8): torn writes, crashes, clock skew and
//! jumps, refused, lost, throttled and slow calls to billing.example.net, and a
//! drawn provider outage of up to four hours, which is longer than a renewal's
//! retries wait (four attempts, 30 s backoff doubling, so about four minutes).
//!
//! One workload drives two workspaces through five hours of virtual time under
//! the run's faults, then a fault-free tail of four days:
//!
//! - plans, customers and subscriptions are created; each subscription is
//!   imported a drawn 1 to 120 s before its period ends, so its first renewal
//!   lands inside the window a drawn outage covers (the outage starts in the
//!   first 2 s of virtual time, and whole-day periods would never meet it),
//!   and sometimes just before the outage opens, so a charge the provider
//!   recorded is then given up and retried by the owner;
//! - workspace 2 imports a bulk of 24 to 64 subscriptions, so the faulted
//!   phase makes enough provider calls for a reply to be lost;
//! - some subscriptions are cancelled, a second time as well;
//! - the owner asks for a retry of every subscription at 20 min, 45 min, 2 h
//!   and 4 h 30 min; each is refused unless the subscription is past due;
//! - a workspace's owner and customers probe the other workspace, and each
//!   other's subscriptions;
//! - customers read their subscription and their notices, and the owner lists.
//!
//! Each check below judges one behaviour of BRIEF.md against the recovered
//! store, the provider model's ledger and the answers the requests got.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use charpente::auth::ApiKey;
use charpente::core::io::{OutboundCall, OutboundReply, WallTime};
use charpente::core::job::{JobRow, JobStatus};
use charpente::core::table::Codec;
use charpente::http::{App, Method, Request};
use charpente::wire::WireU64;
use charpente::{Id, Snapshot, TxError, View};
use charpente_sim::Sim;
use charpente_sim::checks::{
    calls_have_durable_jobs, dead_hooks_succeeded, durable_corruption_fired, keys_are_per_effect,
    outbound_settles_by_horizon,
};
use charpente_sim::drive::Drive;
use charpente_sim::outbound::Remote;
use charpente_sim::rng::Rng;
use renewly::{
    BILLING_HOST, Cancellation, Charge, CustomerBody, DAY_NS, Notice, Plan, PlanBody, Renewal,
    RetryRequest, SubState, SubscribeBody, Subscription, TransitionBody, Workspace, build_app,
};

// Measured rates of the rarer required keys, per check per 256-run batch
// (2026-11-19, ambient master seeds): `fault:outbound_lost_reply` 2 to 24 over
// 28 batches, mean about 11; `notice:read` about 32; `retry:of-an-applied-charge`
// about 80; `renewal:settled` about 210. The rest fire in most runs.
charpente_sim::sim_tests! {
    acked_requests_are_never_lost require ["restart", "acked:subscribe", "acked:cancel", "acked:retry"],
    no_period_is_charged_twice_or_early require ["renewal:settled", "renewal:caught-up", "fault:outbound_lost_reply"],
    a_cancelled_subscription_is_never_charged_again require ["cancel:landed"],
    a_given_up_renewal_is_past_due_with_one_notice require ["dead:Charge", "fault:outbound_outage", "notice:read"],
    an_owner_retry_charges_the_same_period_once require ["retry:accepted", "retry:recovered", "retry:of-an-applied-charge"],
    workspaces_never_see_each_other require ["isolation:probed"],
    a_refused_transition_names_what_is_allowed require ["refusal:cancel-cancelled", "refusal:retry-active", "refusal:retry-renewing"],
}

/// The credentials the operator provisions with each workspace: an API key
/// per principal, seeded in the build closure (GUIDE §8, "Seed fixtures in
/// the build closure").
const OWNER_1: u64 = 1;
const OWNER_2: u64 = 2;
const ADA: u64 = 3; // customer 1 of workspace 1
const BO: u64 = 4; // customer 2 of workspace 1
const CY: u64 = 5; // customer 1 of workspace 2
const PRINCIPALS: [(u64, &str); 5] = [
    (OWNER_1, "owner:1"),
    (OWNER_2, "owner:2"),
    (ADA, "customer:1:1"),
    (BO, "customer:1:2"),
    (CY, "customer:2:1"),
];

fn raw_key(principal: u64) -> String {
    format!("ck_{principal:064x}")
}

/// The API key of a workspace's customer.
fn customer_key(ws: u64, customer: u64) -> u64 {
    match (ws, customer) {
        (1, 1) => ADA,
        (1, 2) => BO,
        _ => CY,
    }
}

fn owner_key(ws: u64) -> u64 {
    if ws == 1 { OWNER_1 } else { OWNER_2 }
}

/// SHA-256, lowercase hex: the `key_hash` an `ApiKey` row stores (GUIDE §5).
/// The platform exports no hash helper, so the fixture carries its own.
fn sha256_hex(data: &[u8]) -> String {
    const K: [u32; 64] = [
        0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4,
        0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe,
        0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f,
        0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
        0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc,
        0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
        0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070, 0x19a4c116,
        0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
        0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7,
        0xc67178f2,
    ];
    let mut h: [u32; 8] = [
        0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab,
        0x5be0cd19,
    ];
    let mut msg = data.to_vec();
    let bits = (data.len() as u64).wrapping_mul(8);
    msg.push(0x80);
    while msg.len() % 64 != 56 {
        msg.push(0);
    }
    msg.extend_from_slice(&bits.to_be_bytes());
    for chunk in msg.chunks(64) {
        let mut w = [0u32; 64];
        for (i, word) in chunk.chunks(4).enumerate() {
            w[i] = u32::from_be_bytes([word[0], word[1], word[2], word[3]]);
        }
        for i in 16..64 {
            let s0 = w[i - 15].rotate_right(7) ^ w[i - 15].rotate_right(18) ^ (w[i - 15] >> 3);
            let s1 = w[i - 2].rotate_right(17) ^ w[i - 2].rotate_right(19) ^ (w[i - 2] >> 10);
            w[i] = w[i - 16]
                .wrapping_add(s0)
                .wrapping_add(w[i - 7])
                .wrapping_add(s1);
        }
        let mut v = h;
        for (k, wi) in K.iter().zip(w.iter()) {
            let [a, b, c, d, e, f, g, hh] = v;
            let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let ch = (e & f) ^ (!e & g);
            let t1 = hh
                .wrapping_add(s1)
                .wrapping_add(ch)
                .wrapping_add(*k)
                .wrapping_add(*wi);
            let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let maj = (a & b) ^ (a & c) ^ (b & c);
            let t2 = s0.wrapping_add(maj);
            v = [t1.wrapping_add(t2), a, b, c, d.wrapping_add(t1), e, f, g];
        }
        for (hi, vi) in h.iter_mut().zip(v.iter()) {
            *hi = hi.wrapping_add(*vi);
        }
    }
    h.iter().map(|x| format!("{x:08x}")).collect()
}

/// The app on this run's disk, with the two workspaces and their keys seeded
/// when absent: the closure runs on every boot.
fn app(sim: &Sim) -> std::io::Result<App> {
    let mut app = build_app(Box::new(sim.disk()), Box::new(sim.entropy()))?;
    let seeded = app.transact_for_tests(|tx| -> Result<(), std::io::Error> {
        for ws in [1u64, 2] {
            let id = Id::<Workspace>::from_raw(ws);
            if tx.get(id)?.is_none() {
                tx.put(&Workspace {
                    id,
                    name: format!("workspace {ws}"),
                });
            }
        }
        for (principal, subject) in PRINCIPALS {
            let id = Id::<ApiKey>::from_raw(principal);
            if tx.get(id)?.is_none() {
                tx.put(&ApiKey {
                    id,
                    key_hash: sha256_hex(raw_key(principal).as_bytes()),
                    subject: subject.to_string(),
                    // A seeded key must not raise the node's recorded time
                    // (GUIDE §7, "Clock errors").
                    created_at: WallTime::UNIX_EPOCH,
                });
            }
        }
        Ok(())
    });
    match seeded {
        Ok(()) => Ok(app),
        Err(TxError::App(e)) | Err(TxError::Io(e)) => Err(e),
        Err(other) => Err(std::io::Error::other(format!("{other:?}"))),
    }
}

/// One charge as the provider applied it: workspace, subscription, period.
type PeriodKey = (u64, u64, u32);

/// billing.example.net. It dedupes on the `Idempotency-Key` header, as the
/// brief says the real one does: a repeated key with the same body gets the
/// first reply and moves no money, and a repeated key with another body is a
/// 409. It is busy (503) one call in ten until the workload calms it.
#[derive(Default)]
struct Provider {
    calm: Arc<AtomicBool>,
    by_key: BTreeMap<String, Vec<u8>>,
    ledger: Ledger,
}

/// What the provider saw, read back after the run.
#[derive(Clone, Default)]
struct Ledger {
    /// How many times each period was charged, and the customer it was
    /// charged to.
    applied: BTreeMap<PeriodKey, (u32, u64)>,
    /// Requests that reused a key for another charge.
    conflicts: Vec<String>,
    /// Requests whose key is not the period's own, or that carried none.
    bad_keys: Vec<String>,
}

fn form_field(body: &str, name: &str) -> Option<u64> {
    body.split("&")
        .find_map(|pair| pair.strip_prefix(name)?.strip_prefix("="))
        .and_then(|v| v.parse().ok())
}

impl Remote for Provider {
    fn handle(&mut self, call: &OutboundCall, rng: &mut Rng) -> OutboundReply {
        let reply = |status: u16, body: &str| OutboundReply {
            status,
            headers: Vec::new(),
            body: body.as_bytes().to_vec(),
        };
        if !self.calm.load(Ordering::Relaxed) && rng.chance(0.1) {
            return reply(503, "busy");
        }
        let body = String::from_utf8_lossy(&call.body).into_owned();
        let field = |name| form_field(&body, name).unwrap_or(u64::MAX);
        let period: PeriodKey = (
            field("workspace"),
            field("subscription"),
            u32::try_from(field("period")).unwrap_or(u32::MAX),
        );
        let key = call
            .headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case("idempotency-key"))
            .map(|(_, value)| value.clone());
        let expected = format!("renewly-{}-{}-{}", period.0, period.1, period.2);
        if key.as_deref() != Some(expected.as_str()) {
            self.ledger
                .bad_keys
                .push(format!("{key:?} for the charge {body:?}"));
        }
        if let Some(key) = &key {
            if let Some(first) = self.by_key.get(key) {
                if *first == call.body {
                    return reply(200, "replayed");
                }
                self.ledger.conflicts.push(key.clone());
                return reply(409, "idempotency key reused with another request");
            }
            self.by_key.insert(key.clone(), call.body.clone());
        }
        let entry = self.ledger.applied.entry(period).or_insert((0, 0));
        entry.0 += 1;
        entry.1 = field("customer");
        // The charge is recorded, but the answer fails on the way out: the
        // caller cannot tell this from a charge that never happened, and only
        // the key makes its retry safe.
        if !self.calm.load(Ordering::Relaxed) && rng.chance(0.15) {
            return reply(502, "upstream error after the charge was recorded");
        }
        reply(200, "charged")
    }
}

/// What a request in the script was for, so a check knows what to judge.
#[derive(Clone, Debug)]
enum Op {
    Setup,
    Subscribe { ws: u64, sub: u64, customer: u64, secs_ago: u64 },
    Cancel { ws: u64, id: u64, sub: u64 },
    Retry { ws: u64, id: u64, sub: u64 },
    /// Must be answered with exactly this, and change nothing.
    Expect { status: u16, body: String },
    /// A customer of workspace 1 cancelling another customer's subscription.
    ForeignCancel { sub: u64 },
    ReadOwn { sub: u64 },
    ReadNotices { ws: u64, customer: u64 },
    ListWorkspace1,
}

/// One subscription the script creates.
#[derive(Clone, Debug)]
struct Planned {
    ws: u64,
    sub: u64,
    customer: u64,
}

#[derive(Default)]
struct Script {
    requests: Vec<(Duration, Request, Op)>,
    subs: Vec<Planned>,
}

const fn secs(n: u64) -> Duration {
    Duration::from_secs(n)
}

const fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
}

/// The faulted phase: past the last owner retry and the late reads.
const PHASE: Duration = secs(5 * 3600);
/// The fault-free tail: four days, so every plan renews at least once more.
const TAIL: Duration = secs(4 * 86_400);
/// When the owner asks for a retry of every subscription.
const RETRY_AT: [u64; 4] = [20 * 60, 45 * 60, 2 * 3600, 4 * 3600 + 30 * 60];
/// The late probes and reads.
const PROBE_AT: u64 = 4 * 3600 + 45 * 60;

/// The JSON body an `ApiError` answers with.
fn error_body(text: &str) -> String {
    format!("{{\"error\":\"{text}\"}}")
}

/// A request carrying `who`'s API key.
fn request(method: Method, path: String, who: u64) -> Request {
    Request::new(method, path).header("authorization", format!("Bearer {}", raw_key(who)))
}

impl Script {
    fn put<B: serde::Serialize>(&mut self, at: Duration, path: String, who: u64, body: &B, op: Op) {
        let request = request(Method::Put, path, who).json(body);
        self.requests.push((at, request, op));
    }

    fn get(&mut self, at: Duration, path: String, who: u64, op: Op) {
        self.requests.push((at, request(Method::Get, path, who), op));
    }
}

impl Script {
    /// Plans, customers, and the boundaries a refusal must hold at: a zero-day
    /// plan, an import a whole period old, and a customer importing itself.
    fn setup(&mut self, days: [u32; 2]) {
        let plans = [("Gold", 1500u64), ("Basic", 900u64)];
        for (i, (name, cents)) in plans.iter().enumerate() {
            let ws = i as u64 + 1;
            self.put(
                ms(1 + i as u64),
                format!("/workspaces/{ws}/plans/1"),
                owner_key(ws),
                &PlanBody {
                    name: name.to_string(),
                    price_cents: WireU64(*cents),
                    period_days: days[i],
                },
                Op::Setup,
            );
        }
        let customers = [(1u64, 1u64, "Ada"), (1, 2, "Bo"), (2, 1, "Cy")];
        for (k, (ws, customer, name)) in customers.into_iter().enumerate() {
            self.put(
                ms(3 + k as u64),
                format!("/workspaces/{ws}/customers/{customer}"),
                owner_key(ws),
                &CustomerBody {
                    name: name.to_string(),
                    email: format!("{}@example.com", name.to_lowercase()),
                },
                Op::Setup,
            );
        }
        let zero_day = PlanBody {
            name: "Free".to_string(),
            price_cents: WireU64(0),
            period_days: 0,
        };
        let refused = error_body("a plan period is a whole number of days, at least 1");
        self.put(
            ms(6),
            "/workspaces/1/plans/9".to_string(),
            OWNER_1,
            &zero_day,
            Op::Expect { status: 400, body: refused },
        );
        let whole_period = SubscribeBody {
            customer: Id::from_raw(1),
            plan: Id::from_raw(1),
            started_secs_ago: WireU64(u64::from(days[0]) * 86_400),
        };
        let refused =
            error_body("an imported subscription must still be inside its current period");
        self.put(
            ms(7),
            "/workspaces/1/subscriptions/99".to_string(),
            OWNER_1,
            &whole_period,
            Op::Expect { status: 400, body: refused },
        );
        let self_import = SubscribeBody {
            customer: Id::from_raw(1),
            plan: Id::from_raw(1),
            started_secs_ago: WireU64(60),
        };
        let refused = error_body(
            "only the workspace owner imports a subscription part-way through a period",
        );
        self.put(
            ms(8),
            "/workspaces/1/subscriptions/98".to_string(),
            ADA,
            &self_import,
            Op::Expect { status: 400, body: refused },
        );
    }
}

impl Script {
    fn draw(sim: &Sim) -> Script {
        let input = sim.input();
        let mut s = Script::default();
        let days = [
            input.between("ws1_period_days", 1, 3) as u32,
            input.between("ws2_period_days", 1, 3) as u32,
        ];
        s.setup(days);
        let ws1_subs = input.between("ws1_subscriptions", 1, 3);
        let mut planned: Vec<Planned> = (1..=ws1_subs)
            .map(|sub| Planned {
                ws: 1,
                sub,
                customer: 2 - sub % 2,
            })
            .collect();
        // Workspace 2 imports a bulk of subscriptions for its one customer.
        // A lost reply is the only way a charge is sent twice, and the
        // `outbound_lost_reply` axis rolls per call at under 3%, so the
        // faulted phase needs dozens of renewal calls for it to fire.
        let ws2_subs = input.between("ws2_subscriptions", 24, 64);
        planned.extend((1..=ws2_subs).map(|sub| Planned {
            ws: 2,
            sub,
            customer: 1,
        }));
        for (k, p) in planned.iter().enumerate() {
            let period_secs = u64::from(days[p.ws as usize - 1]) * 86_400;
            let secs_ago = period_secs - input.between("secs_left_in_period", 1, 120);
            let body = SubscribeBody {
                customer: Id::from_raw(p.customer),
                plan: Id::from_raw(1),
                started_secs_ago: WireU64(secs_ago),
            };
            let op = Op::Subscribe {
                ws: p.ws,
                sub: p.sub,
                customer: p.customer,
                secs_ago,
            };
            let path = format!("/workspaces/{}/subscriptions/{}", p.ws, p.sub);
            s.put(ms(10 + k as u64), path, owner_key(p.ws), &body, op);
        }
        for p in &planned {
            if !*input.pick("cancels", &[false, true]) {
                continue;
            }
            let at = secs(input.between("cancel_at_secs", 1, 600));
            let body = TransitionBody {
                subscription: Id::from_raw(p.sub),
            };
            for (id, who, offset) in [
                (100 + p.sub, customer_key(p.ws, p.customer), ms(0)),
                (100 + p.sub, customer_key(p.ws, p.customer), ms(250)),
                (200 + p.sub, owner_key(p.ws), ms(500)),
            ] {
                let path = format!("/workspaces/{}/cancellations/{id}", p.ws);
                let op = Op::Cancel {
                    ws: p.ws,
                    id,
                    sub: p.sub,

                };
                s.put(at + offset, path, who, &body, op);
            }
        }
        for (k, at) in RETRY_AT.iter().enumerate() {
            for (j, p) in planned.iter().enumerate() {
                let id = 1000 + 10 * p.sub + k as u64;
                let body = TransitionBody {
                    subscription: Id::from_raw(p.sub),
                };
                let op = Op::Retry {
                    ws: p.ws,
                    id,
                    sub: p.sub,
                };
                let path = format!("/workspaces/{}/retries/{id}", p.ws);
                let at = secs(*at) + ms(2 * j as u64);
                s.put(at, path.clone(), owner_key(p.ws), &body, op.clone());
                s.put(at + ms(1), path, owner_key(p.ws), &body, op);
            }
        }
        let ws1_sub1_secs = s.requests.iter().find_map(|(_, _, op)| match op {
            Op::Subscribe { ws: 1, sub: 1, secs_ago, .. } => Some(*secs_ago),
            _ => None,
        });
        s.probes(ws1_subs);
        s.repeats(days[0], ws1_sub1_secs.expect("workspace 1 always has subscription 1"));
        s.subs = planned;
        // Stable: requests at one offset keep their order.
        s.requests.sort_by_key(|(at, _, _)| *at);
        s
    }

    /// Late in the phase: each workspace reaching into the other, customers
    /// reaching for what only an owner does or for another customer's
    /// subscription, and the reads a customer and an owner make.
    fn probes(&mut self, ws1_subs: u64) {
        let at = secs(PROBE_AT);
        let not_member_1 = error_body("not a member of workspace 1");
        let not_member_2 = error_body("not a member of workspace 2");
        let only_owner = |what: &str| error_body(&format!("only the workspace owner may {what}"));
        let expect = |body: String| Op::Expect { status: 400, body };
        let sub1 = TransitionBody {
            subscription: Id::from_raw(1),
        };
        let plan = PlanBody {
            name: "Stolen".to_string(),
            price_cents: WireU64(1),
            period_days: 1,
        };
        let customer = CustomerBody {
            name: "Eve".to_string(),
            email: "eve@example.com".to_string(),
        };
        self.get(at, "/workspaces/1/subscriptions/1".into(), OWNER_2, expect(not_member_1.clone()));
        self.put(at, "/workspaces/1/cancellations/900".into(), OWNER_2, &sub1, expect(not_member_1.clone()));
        self.put(at, "/workspaces/1/retries/900".into(), CY, &sub1, expect(not_member_1.clone()));
        self.get(at, "/workspaces/1/subscriptions".into(), CY, expect(not_member_1));
        self.put(at, "/workspaces/2/plans/9".into(), OWNER_1, &plan, expect(not_member_2.clone()));
        self.put(at, "/workspaces/2/cancellations/900".into(), ADA, &sub1, expect(not_member_2.clone()));
        self.get(at, "/workspaces/2/notices".into(), ADA, expect(not_member_2));
        self.get(at, "/workspaces/1/subscriptions".into(), ADA, expect(only_owner("list subscriptions")));
        self.put(at, "/workspaces/1/retries/901".into(), ADA, &sub1, expect(only_owner("retry a renewal")));
        self.put(at, "/workspaces/1/plans/8".into(), BO, &plan, expect(only_owner("create plans")));
        self.put(at, "/workspaces/1/customers/9".into(), BO, &customer, expect(only_owner("register customers")));
        self.get(
            at,
            "/workspaces/1/notices".into(),
            OWNER_1,
            expect(error_body("notices are read by the customer they were sent to")),
        );
        let other = SubscribeBody {
            customer: Id::from_raw(2),
            plan: Id::from_raw(1),
            started_secs_ago: WireU64(0),
        };
        self.put(
            at,
            "/workspaces/1/subscriptions/97".into(),
            ADA,
            &other,
            expect(error_body("a customer subscribes only itself")),
        );
        if ws1_subs >= 2 {
            let sub2 = TransitionBody {
                subscription: Id::from_raw(2),
            };
            self.put(at, "/workspaces/1/cancellations/902".into(), ADA, &sub2, Op::ForeignCancel { sub: 2 });
            self.get(at, "/workspaces/1/subscriptions/2".into(), ADA, Op::ForeignCancel { sub: 2 });
        }
        let reads = at + secs(60);
        self.get(reads, "/workspaces/1/subscriptions/1".into(), ADA, Op::ReadOwn { sub: 1 });
        self.get(reads, "/workspaces/1/notices".into(), ADA, Op::ReadNotices { ws: 1, customer: 1 });
        self.get(reads, "/workspaces/1/notices".into(), BO, Op::ReadNotices { ws: 1, customer: 2 });
        self.get(reads, "/workspaces/2/notices".into(), CY, Op::ReadNotices { ws: 2, customer: 1 });
        self.get(reads, "/workspaces/1/subscriptions".into(), OWNER_1, Op::ListWorkspace1);
    }
}

/// A `Charge` job as the queue holds it.
struct ChargeJob {
    job: u64,
    key: PeriodKey,
    done: bool,
    dead: bool,
}

/// The recovered store, read through the operator's snapshot.
#[derive(Default)]
struct Store {
    plans: BTreeMap<(u64, u64), Plan>,
    subs: BTreeMap<(u64, u64), Subscription>,
    cancels: BTreeMap<(u64, u64), Cancellation>,
    retries: BTreeMap<(u64, u64), RetryRequest>,
    renewals: Vec<(u64, Renewal)>,
    notices: Vec<(u64, Notice)>,
    charges: Vec<ChargeJob>,
}

fn read_store(snap: &Snapshot) -> Result<Store, String> {
    let text = |e: std::io::Error| e.to_string();
    let mut s = Store::default();
    for ws in [1u64, 2] {
        for row in snap.scan_scoped::<Plan>(ws).map_err(text)? {
            s.plans.insert((ws, row.id.raw()), row);
        }
        for row in snap.scan_scoped::<Subscription>(ws).map_err(text)? {
            s.subs.insert((ws, row.id.raw()), row);
        }
        for row in snap.scan_scoped::<Cancellation>(ws).map_err(text)? {
            s.cancels.insert((ws, row.id.raw()), row);
        }
        for row in snap.scan_scoped::<RetryRequest>(ws).map_err(text)? {
            s.retries.insert((ws, row.id.raw()), row);
        }
        for row in snap.scan_scoped::<Renewal>(ws).map_err(text)? {
            s.renewals.push((ws, row));
        }
        for row in snap.scan_scoped::<Notice>(ws).map_err(text)? {
            s.notices.push((ws, row));
        }
    }
    for job in snap.scan::<JobRow>().map_err(text)? {
        if job.name != "Charge" {
            continue;
        }
        let charge = Charge::decode(&mut &job.payload[..]).map_err(text)?;
        s.charges.push(ChargeJob {
            job: job.id.raw(),
            key: (
                job.scope.unwrap_or(u64::MAX),
                charge.subscription.raw(),
                charge.period,
            ),
            done: job.status == JobStatus::Done,
            dead: job.status == JobStatus::Dead,
        });
    }
    Ok(s)
}

/// One run, recovered: what each request was, whether the faulted phase
/// acked it, its final answer (the tail's if the tail re-sent it), the store
/// and the provider's ledger.
struct World {
    ops: Vec<Op>,
    acked: Vec<bool>,
    answers: Vec<Option<(u16, String)>>,
    store: Store,
    ledger: Ledger,
    /// The wall time the tail stopped at.
    wall: u64,
}

/// Runs the script: the faulted phase, a power cycle, the fault-free tail
/// that re-sends what went unanswered (GUIDE §8). `None`: the run was given
/// up by name.
fn world(sim: &mut Sim) -> Result<Option<World>, String> {
    let script = Script::draw(sim);
    let (requests, ops): (Vec<(Duration, Request)>, Vec<Op>) = script
        .requests
        .into_iter()
        .map(|(at, request, op)| ((at, request), op))
        .unzip();
    let calm = Arc::new(AtomicBool::new(false));
    sim.remote(
        BILLING_HOST,
        Provider {
            calm: Arc::clone(&calm),
            ..Provider::default()
        },
    );
    let drive = Drive::new(sim, app);
    let phase = drive.faulted_phase(&requests, PHASE)?;
    calm.store(true, Ordering::Relaxed);
    let Some(recovered) = drive.recover(&phase, TAIL)? else {
        return Ok(None);
    };
    let acked = (0..ops.len()).map(|i| phase.status(i) == Some(200)).collect();
    let answers = (0..ops.len())
        .map(|i| {
            recovered.resent[i]
                .as_ref()
                .or(phase.answers[i].as_ref())
                .map(|(status, body)| (*status, String::from_utf8_lossy(body).into_owned()))
        })
        .collect();
    let snap = recovered
        .kernel
        .app()
        .db()
        .snapshot()
        .map_err(|e| e.to_string())?;
    keys_are_per_effect(&snap)?;
    outbound_settles_by_horizon(&snap, recovered.wall)?;
    dead_hooks_succeeded(&snap)?;
    if let Err(e) = calls_have_durable_jobs(sim, &snap) {
        if durable_corruption_fired(sim) {
            sim.give_up("durable corruption cut an enqueue whose call was made", e);
            return Ok(None);
        }
        return Err(e);
    }
    let store = read_store(&snap)?;
    let ledger = sim.with_remote(BILLING_HOST, |p: &Provider| p.ledger.clone());
    Ok(Some(World {
        ops,
        acked,
        answers,
        store,
        ledger,
        wall: recovered.wall.as_nanos(),
    }))
}

/// What the app answers a refused transition, written out here rather than
/// taken from the app, so a change to the app's text is a finding.
fn refusal_text(transition: &str, sub: u64, state: &str) -> String {
    let allowed = match state {
        "active" => "cancel, renew",
        "renewing" => "settle, give up",
        "past_due" => "retry",
        _ => "no transitions",
    };
    error_body(&format!(
        "cannot {transition} subscription {sub}: it is {state}, and {state} allows {allowed}"
    ))
}

/// Gives the run up when durable corruption fired, the one loss the platform
/// excuses (GUIDE §8, "Giving up"); otherwise the loss is the finding.
fn lost(sim: &Sim, what: String) -> Result<(), String> {
    if durable_corruption_fired(sim) {
        sim.give_up("durable corruption cut an acked record", what);
        return Ok(());
    }
    Err(what)
}

/// An acknowledged subscription, cancellation or retry is never lost, across
/// the faulted phase's crashes and the power cycle before the tail.
fn acked_requests_are_never_lost(sim: &mut Sim) -> Result<(), String> {
    let Some(w) = world(sim)? else {
        return Ok(());
    };
    let mut judged = false;
    for (i, op) in w.ops.iter().enumerate() {
        if !w.acked[i] {
            continue;
        }
        let missing = match op {
            Op::Subscribe { ws, sub, customer, secs_ago } => {
                sim.cover("acked:subscribe");
                match w.store.subs.get(&(*ws, *sub)) {
                    Some(s)
                        if s.customer.raw() == *customer
                            && s.plan.raw() == 1
                            && s.started_secs_ago == *secs_ago => None,
                    Some(_) => Some(format!(
                        "subscription {sub} of workspace {ws} (it came back with other terms)"
                    )),
                    None => Some(format!("subscription {sub} of workspace {ws}")),
                }
            }
            Op::Cancel { ws, id, sub, .. } => {
                sim.cover("acked:cancel");
                match w.store.cancels.get(&(*ws, *id)) {
                    Some(c) if c.subscription.raw() == *sub => {
                        let state = w.store.subs.get(&(*ws, *sub)).map(|s| s.state);
                        if state != Some(SubState::Cancelled) {
                            return Err(format!(
                                "cancellation {id} of workspace {ws} was acked, but subscription {sub} is {state:?}"
                            ));
                        }
                        None
                    }
                    Some(_) => {
                        return Err(format!("acked cancellation {id} names another subscription"));
                    }
                    None => Some(format!("cancellation {id} of workspace {ws}")),
                }
            }
            Op::Retry { ws, id, sub } => {
                sim.cover("acked:retry");
                match w.store.retries.get(&(*ws, *id)) {
                    Some(r) if r.subscription.raw() == *sub => None,
                    Some(_) => return Err(format!("acked retry {id} names another subscription")),
                    None => Some(format!("retry {id} of workspace {ws}")),
                }
            }
            _ => continue,
        };
        judged = true;
        if let Some(what) = missing {
            return lost(sim, format!("acked {what} was lost across a crash"));
        }
    }
    if !judged {
        sim.give_up("no subscribe, cancel or retry was acked in the faulted phase", "");
    }
    Ok(())
}

/// A period is charged at most once and never before it ends; every settled
/// renewal is a charge the provider applied, and the subscription moved on
/// by exactly one period for each; nothing is left renewing, and no active
/// subscription sits past its period end.
fn no_period_is_charged_twice_or_early(sim: &mut Sim) -> Result<(), String> {
    let Some(w) = world(sim)? else {
        return Ok(());
    };
    if let Some(key) = w.ledger.conflicts.first() {
        return Err(format!(
            "the provider saw idempotency key {key} reused for another charge — each period has its own key"
        ));
    }
    if let Some(bad) = w.ledger.bad_keys.first() {
        return Err(format!(
            "the provider saw the key {bad}: a charge carries renewly-<workspace>-<subscription>-<period>"
        ));
    }
    if let Some(((ws, sub, p), (n, _))) = w.ledger.applied.iter().find(|(_, (n, _))| *n > 1) {
        return Err(format!(
            "double charge: the provider applied period {p} of subscription {sub} in workspace {ws} {n} times"
        ));
    }
    let mut done: BTreeMap<PeriodKey, u32> = BTreeMap::new();
    for c in w.store.charges.iter().filter(|c| c.done) {
        *done.entry(c.key).or_insert(0) += 1;
    }
    if let Some(((ws, sub, p), n)) = done.iter().find(|(_, n)| **n > 1) {
        return Err(format!(
            "period {p} of subscription {sub} in workspace {ws} settled {n} times"
        ));
    }
    for (&(ws, id), sub) in &w.store.subs {
        let plan = w
            .store
            .plans
            .get(&(ws, sub.plan.raw()))
            .ok_or_else(|| format!("subscription {id} of workspace {ws} names a missing plan"))?;
        let len = u64::from(plan.period_days) * DAY_NS;
        if sub.period_end_ns.wrapping_sub(sub.period_start_ns) != len {
            return Err(format!(
                "subscription {id} of workspace {ws}: period {} runs {} ns, not the plan's {len}",
                sub.period,
                sub.period_end_ns.wrapping_sub(sub.period_start_ns)
            ));
        }
        let settled: Vec<u32> = done
            .keys()
            .filter(|(w_, s, _)| (*w_, *s) == (ws, id))
            .map(|(_, _, p)| *p)
            .collect();
        let expected: Vec<u32> = (1..=sub.period).collect();
        if settled != expected {
            return Err(format!(
                "subscription {id} of workspace {ws} is in period {} but its settled renewals are {settled:?}",
                sub.period
            ));
        }
        for p in &settled {
            if !w.ledger.applied.contains_key(&(ws, id, *p)) {
                return Err(format!(
                    "period {p} of subscription {id} in workspace {ws} settled, but the provider never applied it"
                ));
            }
        }
        for &(_, _, p) in w.ledger.applied.keys().filter(|(a, b, _)| (*a, *b) == (ws, id)) {
            let given_up = p == sub.period + 1 && sub.state == SubState::PastDue;
            if p > sub.period && !given_up {
                return Err(format!(
                    "the provider charged period {p} of subscription {id} in workspace {ws}, which is in period {} and {:?}",
                    sub.period, sub.state
                ));
            }
        }
        let first_start = sub.period_start_ns - u64::from(sub.period) * len;
        let mut renewed: Vec<u32> = Vec::new();
        for (_, r) in w
            .store
            .renewals
            .iter()
            .filter(|(w_, r)| *w_ == ws && r.subscription.raw() == id)
        {
            renewed.push(r.period);
            let due = first_start + u64::from(r.period) * len;
            if r.due_ns != due {
                return Err(format!(
                    "renewal of period {} of subscription {id} in workspace {ws} was due at {}, not at the end of period {} ({due})",
                    r.period,
                    r.due_ns,
                    r.period - 1
                ));
            }
            if r.started_ns < r.due_ns {
                return Err(format!(
                    "period {} of subscription {id} in workspace {ws} was charged {} ns before the period before it ended",
                    r.period,
                    r.due_ns - r.started_ns
                ));
            }
        }
        renewed.sort_unstable();
        let mut expected: Vec<u32> = (1..=sub.period).collect();
        if sub.state == SubState::PastDue {
            expected.push(sub.period + 1);
        }
        if renewed != expected {
            return Err(format!(
                "subscription {id} of workspace {ws} is {:?} in period {} but renewed periods {renewed:?}",
                sub.state, sub.period
            ));
        }
        if sub.state == SubState::Renewing {
            return Err(format!(
                "subscription {id} of workspace {ws} is still renewing after the tail settled every charge"
            ));
        }
        if sub.state == SubState::Active && sub.period_end_ns + 120 * 1_000_000_000 < w.wall {
            return Err(format!(
                "subscription {id} of workspace {ws} is active but its period ended at {} and the tail ran to {}",
                sub.period_end_ns, w.wall
            ));
        }
        if sub.period >= 2 {
            sim.cover("renewal:caught-up");
        }
    }
    if !done.is_empty() {
        sim.cover("renewal:settled");
    }
    Ok(())
}

/// A cancelled subscription keeps its period and is never charged again:
/// no charge, renewal or charge job for a later period, ever.
fn a_cancelled_subscription_is_never_charged_again(sim: &mut Sim) -> Result<(), String> {
    let Some(w) = world(sim)? else {
        return Ok(());
    };
    let mut per_sub: BTreeMap<(u64, u64), u32> = BTreeMap::new();
    for (&(ws, cid), c) in &w.store.cancels {
        let id = c.subscription.raw();
        *per_sub.entry((ws, id)).or_insert(0) += 1;
        let sub = w
            .store
            .subs
            .get(&(ws, id))
            .ok_or_else(|| format!("cancellation {cid} names missing subscription {id}"))?;
        if sub.state != SubState::Cancelled || sub.period != c.period {
            return Err(format!(
                "subscription {id} of workspace {ws} was cancelled in period {} but is {:?} in period {}",
                c.period, sub.state, sub.period
            ));
        }
        let later = |p: u32| p > c.period;
        if let Some((_, _, p)) = w
            .ledger
            .applied
            .keys()
            .find(|(a, b, p)| (*a, *b) == (ws, id) && later(*p))
        {
            return Err(format!(
                "cancelled subscription {id} of workspace {ws} was charged for period {p}"
            ));
        }
        if let Some(job) = w
            .store
            .charges
            .iter()
            .find(|j| (j.key.0, j.key.1) == (ws, id) && later(j.key.2))
        {
            return Err(format!(
                "cancelled subscription {id} of workspace {ws} has charge job {} for period {}",
                job.job, job.key.2
            ));
        }
        sim.cover("cancel:landed");
    }
    if let Some(((ws, id), n)) = per_sub.iter().find(|(_, n)| **n > 1) {
        return Err(format!(
            "subscription {id} of workspace {ws} was cancelled {n} times — cancelling a cancelled subscription is refused"
        ));
    }
    for (&(ws, id), sub) in &w.store.subs {
        if sub.state == SubState::Cancelled && !per_sub.contains_key(&(ws, id)) {
            return Err(format!(
                "subscription {id} of workspace {ws} is cancelled with no cancellation"
            ));
        }
    }
    Ok(())
}

/// Every given-up renewal leaves its subscription past due and its customer
/// exactly one notice, naming the plan and the time; a customer reads their
/// own notices and nobody else's.
fn a_given_up_renewal_is_past_due_with_one_notice(sim: &mut Sim) -> Result<(), String> {
    let Some(w) = world(sim)? else {
        return Ok(());
    };
    let mut dead: BTreeMap<PeriodKey, u32> = BTreeMap::new();
    for c in w.store.charges.iter().filter(|c| c.dead) {
        *dead.entry(c.key).or_insert(0) += 1;
    }
    let mut noticed: BTreeMap<PeriodKey, u32> = BTreeMap::new();
    for (ws, n) in &w.store.notices {
        *noticed
            .entry((*ws, n.subscription.raw(), n.period))
            .or_insert(0) += 1;
        let sub = w
            .store
            .subs
            .get(&(*ws, n.subscription.raw()))
            .ok_or_else(|| format!("notice {} names a missing subscription", n.id.raw()))?;
        let plan = w
            .store
            .plans
            .get(&(*ws, sub.plan.raw()))
            .ok_or_else(|| format!("notice {} names a missing plan", n.id.raw()))?;
        let names_it = n.text.contains(&plan.name) && n.text.contains(&n.failed_at_ns.to_string());
        if n.customer != sub.customer || n.plan != sub.plan || n.plan_name != plan.name || !names_it {
            return Err(format!(
                "notice {} of workspace {ws} does not tell customer {} that the {} plan failed and when: {:?}",
                n.id.raw(),
                sub.customer.raw(),
                plan.name,
                n.text
            ));
        }
        let due = w
            .store
            .renewals
            .iter()
            .find(|(w_, r)| *w_ == *ws && r.subscription == n.subscription && r.period == n.period)
            .map(|(_, r)| r.due_ns);
        if due.is_none_or(|due| n.failed_at_ns < due) {
            return Err(format!(
                "notice {} says period {} failed at {}, before its renewal was due ({due:?})",
                n.id.raw(),
                n.period,
                n.failed_at_ns
            ));
        }
    }
    if dead != noticed {
        return Err(format!(
            "given-up renewals {dead:?} and notices {noticed:?} differ — each give-up tells its customer once"
        ));
    }
    for (&(ws, id), sub) in &w.store.subs {
        let latest = w
            .store
            .charges
            .iter()
            .filter(|c| c.key == (ws, id, sub.period + 1))
            .max_by_key(|c| c.job);
        let given_up = latest.is_some_and(|c| c.dead);
        if given_up != (sub.state == SubState::PastDue) {
            return Err(format!(
                "subscription {id} of workspace {ws} is {:?}, and its latest charge for period {} was given up: {given_up}",
                sub.state,
                sub.period + 1
            ));
        }
    }
    for (i, op) in w.ops.iter().enumerate() {
        let Op::ReadNotices { ws, customer } = op else {
            continue;
        };
        let Some((200, body)) = &w.answers[i] else {
            return Err(format!(
                "customer {customer} reading their notices got {:?}",
                w.answers[i]
            ));
        };
        let own = w
            .store
            .notices
            .iter()
            .filter(|(w_, n)| w_ == ws && n.customer.raw() == *customer)
            .count();
        if body.matches("\"text\":").count() > own {
            return Err(format!(
                "customer {customer} read more notices than they were sent: {body}"
            ));
        }
        for ((_, id), sub) in w.store.subs.range((*ws, 0)..(*ws + 1, 0)) {
            if sub.customer.raw() != *customer
                && body.contains(&format!("\"subscription\":\"{id}\""))
            {
                return Err(format!(
                    "customer {customer} read a notice of subscription {id}, which is not theirs: {body}"
                ));
            }
        }
        if body.contains("\"text\":") {
            sim.cover("notice:read");
        }
    }
    Ok(())
}

/// An owner retry charges the period that was given up, once: each retry
/// enqueues one charge for that same period, under that period's own key.
fn an_owner_retry_charges_the_same_period_once(sim: &mut Sim) -> Result<(), String> {
    let Some(w) = world(sim)? else {
        return Ok(());
    };
    let mut retries: BTreeMap<PeriodKey, u32> = BTreeMap::new();
    for (&(ws, _), r) in &w.store.retries {
        *retries
            .entry((ws, r.subscription.raw(), r.period))
            .or_insert(0) += 1;
    }
    for (&key, &n) in &retries {
        sim.cover("retry:accepted");
        let (ws, id, p) = key;
        let jobs: Vec<&ChargeJob> = w.store.charges.iter().filter(|c| c.key == key).collect();
        if jobs.len() as u32 != n + 1 {
            return Err(format!(
                "period {p} of subscription {id} in workspace {ws} has {} charge jobs for {n} retries — one renewal and one per retry",
                jobs.len()
            ));
        }
        let first = jobs.iter().min_by_key(|c| c.job).expect("at least one");
        if !first.dead {
            return Err(format!(
                "period {p} of subscription {id} in workspace {ws} was retried, but its renewal was never given up"
            ));
        }
        let applied = w.ledger.applied.get(&key).map_or(0, |(n, _)| *n);
        if applied > 0 {
            // The given-up charge may have reached the provider: the case in
            // which only the period's own key keeps the retry from charging
            // again.
            sim.cover("retry:of-an-applied-charge");
        }
        if applied > 1 {
            return Err(format!(
                "a retried period was charged {applied} times: period {p} of subscription {id} in workspace {ws}"
            ));
        }
        if jobs.iter().any(|c| c.done) {
            let sub = &w.store.subs[&(ws, id)];
            if sub.period < p {
                return Err(format!(
                    "retried period {p} of subscription {id} settled, but the subscription is in period {}",
                    sub.period
                ));
            }
            sim.cover("retry:recovered");
        }
    }
    Ok(())
}

/// The final answer of request `i`, or an error naming it.
fn answer(w: &World, i: usize) -> Result<(u16, &str), String> {
    w.answers[i]
        .as_ref()
        .map(|(status, body)| (*status, body.as_str()))
        .ok_or_else(|| format!("request {i} ({:?}) was never answered", w.ops[i]))
}

/// No workspace sees or changes another workspace's plans, customers,
/// subscriptions or notices, and no customer another customer's: every probe
/// is refused by name and leaves no row behind, every read shows only the
/// caller's own, and every charge is the charged subscription's own.
fn workspaces_never_see_each_other(sim: &mut Sim) -> Result<(), String> {
    let Some(w) = world(sim)? else {
        return Ok(());
    };
    // An answer the store no longer bears out is excused only where media
    // corruption cut an acked row (GUIDE §8, "Giving up").
    match isolation(&w) {
        Ok(()) => {
            sim.cover("isolation:probed");
            Ok(())
        }
        Err(e) => lost(sim, e),
    }
}

fn isolation(w: &World) -> Result<(), String> {
    for (i, op) in w.ops.iter().enumerate() {
        let (status, body) = answer(w, i)?;
        match op {
            Op::Expect { status: want, body: text } => {
                if (status, body) != (*want, text.as_str()) {
                    return Err(format!(
                        "request {i} was answered {status} {body}, not {want} {text}"
                    ));
                }
            }
            Op::ForeignCancel { sub } => {
                let want = if w.store.subs.contains_key(&(1, *sub)) {
                    (400, error_body(&format!("subscription {sub} belongs to another customer")))
                } else {
                    (404, error_body("not found"))
                };
                if (status, body) != (want.0, want.1.as_str()) {
                    return Err(format!(
                        "customer 1 reaching for subscription {sub} was answered {status} {body}, not {} {}",
                        want.0, want.1
                    ));
                }
            }
            Op::ReadOwn { sub } => {
                let Some(stored) = w.store.subs.get(&(1, *sub)) else {
                    if status != 404 {
                        return Err(format!("a missing subscription read as {status} {body}"));
                    }
                    continue;
                };
                let shows = |field: String| body.contains(&field);
                let own = status == 200
                    && shows(format!("\"id\":\"{sub}\""))
                    && shows(format!("\"customer\":\"{}\"", stored.customer.raw()))
                    && shows("\"planName\":\"Gold\"".to_string());
                if !own {
                    return Err(format!(
                        "customer {} reading subscription {sub} got {status} {body}",
                        stored.customer.raw()
                    ));
                }
            }
            Op::ListWorkspace1 => {
                if status != 200 || body.contains("Basic") {
                    return Err(format!("workspace 1's list showed {status} {body}"));
                }
                let listed = body.matches("\"planName\":\"Gold\"").count();
                let stored = w.store.subs.range((1, 0)..(2, 0)).count();
                if listed > stored {
                    return Err(format!(
                        "workspace 1 lists {listed} subscriptions but holds {stored}: {body}"
                    ));
                }
            }
            _ => {}
        }
    }
    let leaked = [
        w.store.cancels.contains_key(&(1, 900)),
        w.store.cancels.contains_key(&(1, 902)),
        w.store.cancels.contains_key(&(2, 900)),
        w.store.retries.contains_key(&(1, 900)),
        w.store.retries.contains_key(&(1, 901)),
        w.store.plans.contains_key(&(1, 8)),
        w.store.plans.contains_key(&(1, 9)),
        w.store.plans.contains_key(&(2, 9)),
        w.store.subs.contains_key(&(1, 97)),
    ];
    if let Some(n) = leaked.iter().position(|l| *l) {
        return Err(format!("refused probe {n} left a row behind"));
    }
    for (&(ws, sub, p), &(_, customer)) in &w.ledger.applied {
        let owner = w.store.subs.get(&(ws, sub)).map(|s| s.customer.raw());
        if owner != Some(customer) {
            return Err(format!(
                "the provider charged customer {customer} for period {p} of subscription {sub} in workspace {ws}, which belongs to {owner:?}"
            ));
        }
    }
    Ok(())
}

/// A transition the state does not allow is refused by name, listing what
/// the state does allow: cancelling a cancelled subscription, retrying an
/// active or renewing one. Every other request gets the answer its state
/// calls for, and a repeat is answered as the first was.
fn a_refused_transition_names_what_is_allowed(sim: &mut Sim) -> Result<(), String> {
    let Some(w) = world(sim)? else {
        return Ok(());
    };
    match refusals(sim, &w) {
        Ok(()) => Ok(()),
        Err(e) => lost(sim, e),
    }
}

fn refusals(sim: &Sim, w: &World) -> Result<(), String> {
    for (i, op) in w.ops.iter().enumerate() {
        let (status, body) = answer(w, i)?;
        let (transition, ws, id, sub, refusable): (&str, u64, u64, u64, &[&str]) = match op {
            Op::Setup => {
                if status != 200 {
                    return Err(format!("setup request {i} was answered {status} {body}"));
                }
                continue;
            }
            Op::Subscribe { sub, .. } => {
                if (status, body) != (200, format!("\"{sub}\"").as_str()) {
                    return Err(format!("subscribing {sub} was answered {status} {body}"));
                }
                continue;
            }
            Op::Expect { status: want, body: text } => {
                if (status, body) != (*want, text.as_str()) {
                    return Err(format!(
                        "request {i} was answered {status} {body}, not {want} {text}"
                    ));
                }
                continue;
            }
            Op::Cancel { ws, id, sub, .. } => {
                ("cancel", *ws, *id, *sub, &["renewing", "past_due", "cancelled"])
            }
            Op::Retry { ws, id, sub } => ("retry", *ws, *id, *sub, &["active", "renewing", "cancelled"]),
            _ => continue,
        };
        if status == 200 {
            let landed = match transition {
                "cancel" => w.store.cancels.contains_key(&(ws, id)),
                _ => w.store.retries.contains_key(&(ws, id)),
            };
            if body != format!("\"{id}\"") || !landed {
                return Err(format!("{transition} {id} was answered 200 {body} but is not stored"));
            }
            continue;
        }
        let Some(state) = refusable
            .iter()
            .find(|s| body == refusal_text(transition, sub, s))
        else {
            return Err(format!(
                "{transition} {id} of subscription {sub} was answered {status} {body}, which is no refusal of a state that forbids it"
            ));
        };
        if status != 400 {
            return Err(format!("a refused {transition} answered {status}, not 400"));
        }
        match (transition, *state) {
            ("cancel", "cancelled") => sim.cover("refusal:cancel-cancelled"),
            ("retry", "active") => sim.cover("refusal:retry-active"),
            ("retry", "renewing") => sim.cover("refusal:retry-renewing"),
            _ => {}
        }
    }
    Ok(())
}

impl Script {
    /// Late in the phase, an id reused: the same request again is answered
    /// as the first was, and other terms under it are a conflict. Each
    /// conflict differs from the stored row in its first field only.
    fn repeats(&mut self, ws1_days: u32, ws1_sub1_secs: u64) {
        let at = secs(PROBE_AT) + secs(30);
        let ok = || Op::Expect {
            status: 200,
            body: format!("{:?}", "1"),
        };
        let conflict = |text: &str| Op::Expect {
            status: 400,
            body: error_body(text),
        };
        let plan = |name: &str| PlanBody {
            name: name.to_string(),
            price_cents: WireU64(1500),
            period_days: ws1_days,
        };
        self.put(at, "/workspaces/1/plans/1".into(), OWNER_1, &plan("Gold"), ok());
        self.put(
            at,
            "/workspaces/1/plans/1".into(),
            OWNER_1,
            &plan("Platinum"),
            conflict("plan 1 already exists with other terms — a plan is written once; create the new terms under a new id"),
        );
        let customer = |name: &str| CustomerBody {
            name: name.to_string(),
            email: "ada@example.com".to_string(),
        };
        self.put(at, "/workspaces/1/customers/1".into(), OWNER_1, &customer("Ada"), ok());
        self.put(
            at,
            "/workspaces/1/customers/1".into(),
            OWNER_1,
            &customer("Ada Lovelace"),
            conflict("customer 1 already exists with other details — a customer is written once"),
        );
        let subscription = |customer: u64| SubscribeBody {
            customer: Id::from_raw(customer),
            plan: Id::from_raw(1),
            started_secs_ago: WireU64(ws1_sub1_secs),
        };
        self.put(at, "/workspaces/1/subscriptions/1".into(), OWNER_1, &subscription(1), ok());
        self.put(
            at,
            "/workspaces/1/subscriptions/1".into(),
            OWNER_1,
            &subscription(2),
            conflict("subscription 1 already exists with other terms — a subscription is written once"),
        );
    }
}
