//! pricewatch's proof. One scenario per run, drawn with `sim.input()`:
//! operators register products and sources, sources deliver drawn reports,
//! some twice, in a drawn arrival order, across two faulted kernels with a
//! power cycle between them; then a fault-free tail redelivers every report
//! and reads everything back. Five properties judge that one scenario, each
//! giving up (never passing) where faults stopped it before its claim could
//! be tested (GUIDE §8).
//!
//! The oracles here never call `pricewatch::merge`: they restate the rules
//! from the brief and compare.

use charpente::http::{Method, Request};
use charpente::kernel::{Kernel, SimSource};
use charpente::wire::{WireI64, WireU64};
use charpente::{Id, View};
use charpente_sim::Sim;
use charpente_sim::checks::{
    boot_through_fallout, clock_fallout, durable_corruption_fired, is_fault_fallout,
    wait_out_clock_refusal,
};
use pricewatch::{
    NewProduct, NewReport, OPERATORS_ONLY, OperatorClaim, Prices, Report, SOURCES_ONLY,
    build_app, configure_operator_secret,
};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

charpente_sim::sim_tests! {
    acked_report_survives_restarts require ["restart", "pricewatch:acked"],
    kept_values_ignore_arrival_order require ["restart", "pricewatch:judged", "pricewatch:tie"],
    a_redelivered_report_counts_once require ["pricewatch:judged", "pricewatch:duplicate-acked"],
    a_bad_amount_is_refused_by_name require ["pricewatch:refused-amount"],
    only_the_right_credentials_reach_each_route require ["pricewatch:judged"],
}

/// The 401 a revoked key gets (GUIDE §5).
const REVOKED: &str = "unrecognized credential — it may have been revoked; obtain a new one";
const SEC: u64 = 1_000_000_000; // SimSource timestamps are virtual nanoseconds
const MS: u64 = 1_000_000;
/// Exactly 16 characters, the shortest secret `configure_operator_secret` takes.
const SECRET: &str = "sim-operator-key";
/// Products 1 and 2 receive reports; product 3 is registered and never does.
const PRODUCTS: u64 = 3;
const SOURCES: u64 = 3;

/// One report as a source sends it.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Spec {
    product: u64,
    source: u64,
    amount: i64,
    observed: u64,
}

impl Spec {
    fn valid(&self) -> bool {
        self.amount > 0
    }

    fn from_row(row: &Report) -> Spec {
        Spec {
            product: row.product.raw(),
            source: row.source.raw(),
            amount: row.amount_cents,
            observed: row.observed_at_ms,
        }
    }
}

/// A kept value: amount, source, observation time.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Kept {
    amount: i64,
    source: u64,
    observed: u64,
}

impl Kept {
    fn of(spec: &Spec) -> Kept {
        Kept {
            amount: spec.amount,
            source: spec.source,
            observed: spec.observed,
        }
    }
}

/// The lowest price over a set of reports: the minimum amount; equal
/// amounts go to the earlier observation, then to the smaller source id.
fn oracle_lowest(reports: &[Spec]) -> Option<Kept> {
    let mut best: Option<Kept> = None;
    for spec in reports {
        let next = Kept::of(spec);
        let better = match best {
            None => true,
            Some(b) => {
                next.amount < b.amount
                    || (next.amount == b.amount && next.observed < b.observed)
                    || (next.amount == b.amount
                        && next.observed == b.observed
                        && next.source < b.source)
            }
        };
        if better {
            best = Some(next);
        }
    }
    best
}

/// The latest report over a set: the maximum observation time; equal times
/// go to the smaller source id, then (one source, one instant) the smaller
/// amount.
fn oracle_latest(reports: &[Spec]) -> Option<Kept> {
    let mut best: Option<Kept> = None;
    for spec in reports {
        let next = Kept::of(spec);
        let better = match best {
            None => true,
            Some(b) => {
                next.observed > b.observed
                    || (next.observed == b.observed && next.source < b.source)
                    || (next.observed == b.observed
                        && next.source == b.source
                        && next.amount < b.amount)
            }
        };
        if better {
            best = Some(next);
        }
    }
    best
}

fn kept_of_row(prices: &Prices) -> (Kept, Kept) {
    (
        Kept {
            amount: prices.lowest_amount_cents,
            source: prices.lowest_source.raw(),
            observed: prices.lowest_observed_at_ms,
        },
        Kept {
            amount: prices.latest_amount_cents,
            source: prices.latest_source.raw(),
            observed: prices.latest_observed_at_ms,
        },
    )
}

/// Every product's kept row must be exactly what the oracle makes of the
/// reports that are durable beside it: same transaction, same record.
fn kept_matches(state: &State, when: &str) -> Result<(), String> {
    for product in 1..=PRODUCTS {
        let reports: Vec<Spec> = state
            .reports
            .iter()
            .filter(|spec| spec.product == product)
            .cloned()
            .collect();
        let want = oracle_lowest(&reports).zip(oracle_latest(&reports));
        let got = state.prices.get(&product).copied();
        if got != want {
            return Err(format!(
                "{when}: product {product} keeps (lowest, latest) {got:?}, but its {} durable report(s) {reports:?} make {want:?}",
                reports.len()
            ));
        }
    }
    Ok(())
}

/// The durable state at one moment, read through a snapshot.
struct State {
    /// Every report row, in id order, with its id.
    rows: Vec<(u64, Spec)>,
    reports: Vec<Spec>,
    prices: BTreeMap<u64, (Kept, Kept)>,
}

fn read_state(kernel: &Kernel) -> Result<State, String> {
    let snap = kernel.app().db().snapshot().map_err(|e| e.to_string())?;
    let mut rows: Vec<(u64, Spec)> = snap
        .scan::<Report>()
        .map_err(|e| e.to_string())?
        .iter()
        .map(|row| (row.id.raw(), Spec::from_row(row)))
        .collect();
    rows.sort();
    let mut prices = BTreeMap::new();
    for row in snap.scan::<Prices>().map_err(|e| e.to_string())? {
        prices.insert(row.id.raw(), kept_of_row(&row));
    }
    let reports = rows.iter().map(|(_, spec)| spec.clone()).collect();
    Ok(State {
        rows,
        reports,
        prices,
    })
}

/// What one tail request was, so its reply can be judged.
enum Ask {
    Redeliver(Spec),
    ReadProduct(u64),
    ListSource(u64),
    /// A request that must be refused: what it was, its status, and the
    /// error text, when the refusal is ours.
    Fence(&'static str, u16, Option<String>),
}

/// Everything one run did and saw.
struct Run {
    /// Each faulted delivery and its reply, `None` where none came.
    faulted: Vec<(Spec, Option<(u16, String)>)>,
    before_tail: State,
    tail: Vec<(Ask, u16, String)>,
    after_tail: State,
    /// Every distinct report the scenario drew.
    drawn: BTreeSet<Spec>,
}

fn text_of(body: &impl AsRef<[u8]>) -> String {
    String::from_utf8_lossy(body.as_ref()).into_owned()
}

fn bearer(request: Request, token: &str) -> Request {
    request.header("authorization", format!("Bearer {token}"))
}

fn report_request(token: Option<&str>, spec: &Spec) -> Request {
    let request = Request::new(Method::Post, "/reports").json(&NewReport {
        product: Id::from_raw(spec.product),
        amount_cents: WireI64(spec.amount),
        observed_at_ms: WireU64(spec.observed),
    });
    match token {
        Some(token) => bearer(request, token),
        None => request,
    }
}

/// The refusal text `POST /reports` must give a bad amount.
fn amount_refusal(amount: i64) -> String {
    format!("amount_cents must be a whole number of cents, 1 or more — got {amount}")
}

/// The `error` of a JSON error envelope, or the whole body.
fn error_of(body: &str) -> String {
    match serde_json::from_str::<Value>(body) {
        Ok(Value::Object(map)) => match map.get("error") {
            Some(Value::String(text)) => text.clone(),
            _ => body.to_string(),
        },
        _ => body.to_string(),
    }
}

/// Boots the app and its kernel under this run's faults, the way a
/// supervised process does. `None`: it never came up (and, for a durable
/// refusal only, the run was given up by name).
fn open_kernel(sim: &Sim) -> Result<Option<Kernel>, String> {
    let fallout = |text: &str| is_fault_fallout(text) || clock_fallout(sim, text);
    let boot = || build_app(Box::new(sim.disk()), Box::new(sim.entropy()));
    let Some(app) = boot_through_fallout(sim, 8, fallout, boot).map_err(|e| e.to_string())? else {
        return Ok(None);
    };
    match Kernel::new(app, Box::new(sim.clock())) {
        Ok(kernel) => Ok(Some(kernel)),
        Err(e) if fallout(&e.to_string()) => Ok(None),
        Err(e) => Err(e.to_string()),
    }
}

/// Serves `requests` one millisecond apart from now. Returns each reply in
/// arrival order (a prefix of the script) and whether the kernel is still
/// up; fault fallout ends the script, not the run.
fn run_script(
    sim: &Sim,
    kernel: &mut Kernel,
    requests: Vec<Request>,
) -> Result<(Vec<(u16, String)>, bool), String> {
    let base = sim.now().as_nanos();
    let mut source = SimSource::new(Box::new(sim.clock()), base + 2 * SEC);
    for (at, request) in requests.into_iter().enumerate() {
        source.push_at(base + at as u64 * MS, request);
    }
    let responses = source.responses();
    let alive = match kernel.run(&mut source) {
        Ok(()) => true,
        Err(e) if is_fault_fallout(&e.to_string()) || clock_fallout(sim, &e.to_string()) => false,
        Err(e) => return Err(e.to_string()),
    };
    let Ok(served) = responses.lock() else {
        return Err("responses lock poisoned".to_string());
    };
    let mut replies = Vec::new();
    for (status, body) in served.iter() {
        replies.push((*status, text_of(body)));
    }
    Ok((replies, alive))
}

/// A 200's JSON body as a string token (an API key).
fn token_of(reply: Option<&(u16, String)>) -> Option<String> {
    match reply {
        Some((200, body)) => serde_json::from_str::<String>(body).ok(),
        _ => None,
    }
}

/// The fault-free tail (GUIDE §8): disk and clock faults end, the app boots
/// on what the faulted phases left, and it waits out only a clock refusal.
fn recover(sim: &Sim) -> Result<Option<Kernel>, String> {
    sim.quiesce_disk_faults();
    sim.quiesce_clock_faults();
    let fallout = |text: &str| is_fault_fallout(text) || clock_fallout(sim, text);
    let boot = || build_app(Box::new(sim.disk()), Box::new(sim.entropy()));
    let boot_kernel = || {
        let Some(app) = boot_through_fallout(sim, 8, fallout, boot).map_err(|e| e.to_string())?
        else {
            return match sim.gave_up() {
                Some(_) => Ok(None),
                None => Err("the fault-free tail could not boot".to_string()),
            };
        };
        Kernel::new(app, Box::new(sim.clock()))
            .map(Some)
            .map_err(|e| e.to_string())
    };
    wait_out_clock_refusal(sim, boot_kernel)
}

/// Draws the distinct reports, each delivered once or twice, in a drawn
/// arrival order; and a second drawn order for the tail's redelivery.
fn draw(sim: &mut Sim) -> (Vec<Spec>, Vec<Spec>) {
    let input = sim.input();
    let count = input.between("reports", 1, 8);
    let mut drawn = BTreeSet::new();
    let mut deliveries = Vec::new();
    for _ in 0..count {
        let spec = Spec {
            product: input.between("product", 1, 2),
            source: input.between("source", 1, SOURCES),
            // -1 and 0 are not prices: about one draw in six is refused.
            amount: input.between("amount", 0, 10) as i64 - 1,
            observed: input.between("observed_at", 1, 4),
        };
        for _ in 0..input.between("copies", 1, 2) {
            deliveries.push(spec.clone());
        }
        drawn.insert(spec);
    }
    input.shuffle("arrival", &mut deliveries);
    let mut redelivery: Vec<Spec> = drawn.into_iter().collect();
    input.shuffle("redelivery", &mut redelivery);
    (redelivery, deliveries)
}

fn claim(secret: &str) -> Request {
    Request::new(Method::Post, "/operator/key").json(&OperatorClaim {
        secret: secret.to_string(),
    })
}

fn register_product(product: u64, operator: &str) -> Request {
    let request = Request::new(Method::Post, format!("/products/{product}")).json(&NewProduct {
        name: format!("product-{product}"),
    });
    bearer(request, operator)
}

fn reports_for(specs: &[Spec], sources: &BTreeMap<u64, String>) -> Vec<Request> {
    let mut requests = Vec::new();
    for spec in specs {
        requests.push(report_request(Some(&sources[&spec.source]), spec));
    }
    requests
}

/// Pairs each delivery with its reply; deliveries past the replies got none.
fn record(
    faulted: &mut Vec<(Spec, Option<(u16, String)>)>,
    specs: &[Spec],
    replies: Vec<(u16, String)>,
) {
    let mut replies = replies.into_iter();
    for spec in specs {
        faulted.push((spec.clone(), replies.next()));
    }
}

/// The credentials setup produced.
struct Keys {
    operator: String,
    sources: BTreeMap<u64, String>,
}

/// One setup request, so its reply can be read.
enum Step {
    Operator,
    Product(u64),
    Source(u64),
}

/// How many times setup asks again for a step faults kept from its ack.
const SETUP_ROUNDS: u32 = 8;

/// A kernel stopped by a fault: its process dies and a supervisor starts
/// another, after a power cycle where the disk crashed.
fn restart_after_stop(sim: &mut Sim) {
    if sim.disk_crashed() {
        sim.restart();
    } else {
        sim.restart_process();
    }
}

/// Mints the operator key, then registers every product and source, under
/// this run's faults. Every step is safe to repeat (a claim rotates the
/// operator key), so a step faults kept from its ack is asked again, in a
/// restarted process when the kernel stopped. `None`: the run was given up.
fn setup(sim: &mut Sim) -> Result<Option<(Kernel, Keys)>, String> {
    let mut operator: Option<String> = None;
    let mut products = BTreeSet::new();
    let mut sources = BTreeMap::new();
    let mut kernel: Option<Kernel> = None;
    let mut rounds = Vec::new();
    for _ in 0..SETUP_ROUNDS {
        let mut up = match kernel.take() {
            Some(up) => up,
            None => match open_kernel(sim)? {
                Some(up) => up,
                None if sim.gave_up().is_some() => return Ok(None),
                None => continue,
            },
        };
        if let Some(operator) = &operator
            && products.len() as u64 == PRODUCTS
            && sources.len() as u64 == SOURCES
        {
            let keys = Keys {
                operator: operator.clone(),
                sources,
            };
            return Ok(Some((up, keys)));
        }
        let mut steps = Vec::new();
        let mut requests = Vec::new();
        match &operator {
            None => {
                steps.push(Step::Operator);
                requests.push(claim(SECRET));
            }
            Some(token) => {
                for product in 1..=PRODUCTS {
                    if !products.contains(&product) {
                        steps.push(Step::Product(product));
                        requests.push(register_product(product, token));
                    }
                }
                for source in 1..=SOURCES {
                    if !sources.contains_key(&source) {
                        steps.push(Step::Source(source));
                        let request = Request::new(Method::Post, format!("/sources/{source}"));
                        requests.push(bearer(request, token));
                    }
                }
            }
        }
        let (replies, alive) = run_script(sim, &mut up, requests)?;
        let mut statuses = Vec::new();
        for reply in &replies {
            statuses.push(reply.0);
        }
        rounds.push(format!("{statuses:?} of {} (up: {alive})", steps.len()));
        for (step, reply) in steps.iter().zip(&replies) {
            match step {
                Step::Operator => operator = token_of(Some(reply)),
                Step::Product(product) if reply.0 == 200 => {
                    products.insert(*product);
                }
                Step::Product(_) => {}
                Step::Source(source) => {
                    if let Some(token) = token_of(Some(reply)) {
                        sources.insert(*source, token);
                    }
                }
            }
        }
        if alive {
            kernel = Some(up);
        } else {
            restart_after_stop(sim);
            drop(up);
        }
    }
    let detail = format!(
        "operator key: {}, products {products:?}, sources {:?}, rounds {rounds:?}",
        operator.is_some(),
        sources.keys().collect::<Vec<_>>()
    );
    sim.give_up("setup: faults kept a step from its ack in every round", detail);
    Ok(None)
}

/// Requests every route must refuse, each with the refusal it must give.
fn fences(keys: &Keys) -> Vec<(Ask, Request)> {
    let one = &keys.sources[&1];
    let ops = Some(OPERATORS_ONLY.to_string());
    let valid = Spec {
        product: 1,
        source: 1,
        amount: 5,
        observed: 2,
    };
    let unregistered = Spec {
        product: 99,
        ..valid.clone()
    };
    vec![
        (Ask::Fence("a wrong operator secret", 401, None), claim("not-the-secret!!")),
        (
            Ask::Fence("a source registering a product", 400, ops.clone()),
            register_product(9, one),
        ),
        (
            Ask::Fence("a source registering a source", 400, ops.clone()),
            bearer(Request::new(Method::Post, "/sources/9"), one),
        ),
        (
            Ask::Fence("a source listing a source's reports", 400, ops),
            bearer(Request::new(Method::Get, "/sources/1/reports"), one),
        ),
        (
            Ask::Fence("the operator submitting a report", 400, Some(SOURCES_ONLY.to_string())),
            report_request(Some(&keys.operator), &valid),
        ),
        (Ask::Fence("an anonymous report", 401, None), report_request(None, &valid)),
        (
            Ask::Fence("an anonymous product registration", 401, None),
            Request::new(Method::Post, "/products/9").json(&NewProduct {
                name: "nobody".to_string(),
            }),
        ),
        (
            Ask::Fence("a report for an unregistered product", 404, None),
            report_request(Some(one), &unregistered),
        ),
        (
            Ask::Fence("reading an unregistered product", 404, None),
            Request::new(Method::Get, "/products/99"),
        ),
        // Last: the operator claims again, which rotates the key, and the
        // key held until now is refused from then on.
        (Ask::Fence("a second operator claim", 200, None), claim(SECRET)),
        (
            Ask::Fence("the operator's key after a rotation", 401, Some(REVOKED.to_string())),
            bearer(Request::new(Method::Get, "/sources/1/reports"), &keys.operator),
        ),
    ]
}

/// One whole run: setup and two delivery phases under faults with a power
/// cycle between them, a power cycle, then the fault-free tail. `None`: the
/// run was given up by name.
fn scenario(sim: &mut Sim) -> Result<Option<Run>, String> {
    configure_operator_secret(SECRET).map_err(|e| e.to_string())?;
    let (redelivery, deliveries) = draw(sim);
    // Two bad amounts in every tail, beside any the draws made.
    let mut redelivery = redelivery;
    for (source, amount) in [(2, 0), (3, -250)] {
        redelivery.push(Spec {
            product: 1,
            source,
            amount,
            observed: 3,
        });
    }

    let Some((mut kernel, keys)) = setup(sim)? else {
        return Ok(None);
    };

    // Phase A: the first half of the deliveries, then a power cycle.
    let half = deliveries.len() / 2;
    let mut faulted = Vec::new();
    let (replies, _) = run_script(sim, &mut kernel, reports_for(&deliveries[..half], &keys.sources))?;
    record(&mut faulted, &deliveries[..half], replies);
    sim.restart();
    drop(kernel);

    // Phase B: a fresh process on what survived, the rest, a power cycle.
    let rest = &deliveries[half..];
    match open_kernel(sim)? {
        Some(mut kernel) => {
            let (replies, _) = run_script(sim, &mut kernel, reports_for(rest, &keys.sources))?;
            record(&mut faulted, rest, replies);
            sim.restart();
            drop(kernel);
        }
        None if sim.gave_up().is_some() => return Ok(None),
        None => {
            record(&mut faulted, rest, Vec::new());
            sim.restart();
        }
    }

    // The tail: redeliver every report, read everything, knock on every fence.
    let Some(mut kernel) = recover(sim)? else {
        return Ok(None);
    };
    let before_tail = read_state(&kernel)?;
    let mut asks = Vec::new();
    let mut requests = Vec::new();
    for spec in &redelivery {
        requests.push(report_request(Some(&keys.sources[&spec.source]), spec));
        asks.push(Ask::Redeliver(spec.clone()));
    }
    for product in 1..=PRODUCTS {
        requests.push(Request::new(Method::Get, format!("/products/{product}")));
        asks.push(Ask::ReadProduct(product));
    }
    for source in 1..=SOURCES {
        let request = Request::new(Method::Get, format!("/sources/{source}/reports"));
        requests.push(bearer(request, &keys.operator));
        asks.push(Ask::ListSource(source));
    }
    for (ask, request) in fences(&keys) {
        requests.push(request);
        asks.push(ask);
    }
    let (replies, alive) = run_script(sim, &mut kernel, requests)?;
    if !alive || replies.len() != asks.len() {
        return Err(format!(
            "the fault-free tail answered {} of {} requests (kernel up: {alive})",
            replies.len(),
            asks.len()
        ));
    }
    let after_tail = read_state(&kernel)?;
    let mut tail = Vec::new();
    for (ask, (status, body)) in asks.into_iter().zip(replies) {
        tail.push((ask, status, body));
    }
    // Setup's acked registrations and keys are durable, so the tail never
    // meets a missing product or a dead key, except where media corruption
    // cut what setup was acked (GUIDE §8): that run proves nothing more.
    for (ask, status, body) in &tail {
        let lost = match ask {
            Ask::Redeliver(spec) => spec.valid() && (*status == 401 || *status == 404),
            Ask::ReadProduct(_) => *status == 404,
            Ask::ListSource(_) => *status == 401,
            Ask::Fence(..) => false,
        };
        if !lost {
            continue;
        }
        if durable_corruption_fired(sim) {
            sim.give_up("durable corruption cut an acked setup step, so the tail is not judged", body);
            return Ok(None);
        }
        return Err(format!("an acked setup step is gone in the fault-free tail: {status} {body}"));
    }
    sim.cover("pricewatch:judged");
    Ok(Some(Run {
        faulted,
        before_tail,
        tail,
        after_tail,
        drawn: redelivery.into_iter().collect(),
    }))
}

/// An acknowledged report is never lost: every report a faulted phase acked
/// with 200 is durable after two power cycles, before the tail redelivers
/// anything that could mask a loss.
fn acked_report_survives_restarts(sim: &mut Sim) -> Result<(), String> {
    let Some(run) = scenario(sim)? else {
        return Ok(());
    };
    let durable: BTreeSet<&Spec> = run.before_tail.reports.iter().collect();
    let mut acked = 0;
    for (spec, reply) in &run.faulted {
        let Some((200, _)) = reply else {
            continue;
        };
        if !spec.valid() {
            return Err(format!("{spec:?} has a bad amount but was acked with 200"));
        }
        acked += 1;
        if durable.contains(spec) {
            continue;
        }
        // Acked-but-lost is excused ONLY where the harness injected durable
        // corruption into this run (GUIDE §8, "Giving up").
        if durable_corruption_fired(sim) {
            sim.give_up("durable corruption fired, so the acked report is excused", "");
            return Ok(());
        }
        return Err(format!(
            "report {spec:?} was acked with 200 but is not durable after the power cycles"
        ));
    }
    if acked == 0 {
        sim.give_up("no report was acked under faults, so durability has nothing to prove", "");
        return Ok(());
    }
    sim.cover("pricewatch:acked");
    Ok(())
}

/// What `GET /products/:id` must say for one kept value.
fn kept_json(kept: Option<Kept>) -> Value {
    match kept {
        None => Value::Null,
        Some(kept) => serde_json::json!({
            "amount_cents": kept.amount.to_string(),
            "source": kept.source.to_string(),
            "observed_at_ms": kept.observed.to_string(),
        }),
    }
}

fn valid_of(drawn: &BTreeSet<Spec>, product: u64) -> Vec<Spec> {
    let mut reports = Vec::new();
    for spec in drawn {
        if spec.valid() && spec.product == product {
            reports.push(spec.clone());
        }
    }
    reports
}

fn parse_json(body: &str) -> Result<Value, String> {
    match serde_json::from_str(body) {
        Ok(value) => Ok(value),
        Err(e) => Err(format!("{e}: {body}")),
    }
}

/// Neither kept value depends on arrival order: at every moment each
/// product's kept row is the oracle over the reports durable beside it, and
/// once the tail has redelivered everything, the oracle over every valid
/// report drawn. The public read says the same.
fn kept_values_ignore_arrival_order(sim: &mut Sim) -> Result<(), String> {
    let Some(run) = scenario(sim)? else {
        return Ok(());
    };
    kept_matches(&run.before_tail, "before the tail")?;
    kept_matches(&run.after_tail, "after the tail")?;
    for product in 1..=PRODUCTS {
        let reports = valid_of(&run.drawn, product);
        let want = oracle_lowest(&reports).zip(oracle_latest(&reports));
        let got = run.after_tail.prices.get(&product).copied();
        if got != want {
            return Err(format!(
                "after every report was delivered, product {product} keeps {got:?}, but {reports:?} make {want:?}"
            ));
        }
        for later in &reports {
            for earlier in &reports {
                if later != earlier && later.observed == earlier.observed {
                    sim.cover("pricewatch:tie");
                }
            }
        }
    }
    for (ask, status, body) in &run.tail {
        let Ask::ReadProduct(product) = ask else {
            continue;
        };
        let reports = valid_of(&run.drawn, *product);
        let want = serde_json::json!({
            "id": product.to_string(),
            "name": format!("product-{product}"),
            "lowest": kept_json(oracle_lowest(&reports)),
            "latest": kept_json(oracle_latest(&reports)),
        });
        let got = parse_json(body)?;
        if *status != 200 || got != want {
            return Err(format!(
                "GET /products/{product} answered {status} {got}, but its reports {reports:?} make {want}"
            ));
        }
    }
    Ok(())
}

/// A report delivered twice counts once: no state ever records one report
/// twice; after the tail exactly the valid drawn reports are recorded; a
/// redelivery's ack says `duplicate` exactly when the report was already
/// durable, and names the row its first delivery wrote.
fn a_redelivered_report_counts_once(sim: &mut Sim) -> Result<(), String> {
    let Some(run) = scenario(sim)? else {
        return Ok(());
    };
    for (when, state) in [("before the tail", &run.before_tail), ("after the tail", &run.after_tail)] {
        let mut seen = BTreeSet::new();
        for (_, spec) in &state.rows {
            if !seen.insert(spec) {
                return Err(format!("{when}: report {spec:?} is recorded twice"));
            }
        }
    }
    let mut valid = BTreeSet::new();
    for spec in &run.drawn {
        if spec.valid() {
            valid.insert(spec.clone());
        }
    }
    let recorded: BTreeSet<Spec> = run.after_tail.reports.iter().cloned().collect();
    if recorded != valid {
        return Err(format!(
            "after every report was delivered, {recorded:?} are recorded, not the valid drawn {valid:?}"
        ));
    }
    let before: BTreeSet<&Spec> = run.before_tail.reports.iter().collect();
    let mut row_of = BTreeMap::new();
    for (row, spec) in &run.after_tail.rows {
        row_of.insert(spec, *row);
    }
    for (ask, status, body) in &run.tail {
        let Ask::Redeliver(spec) = ask else {
            continue;
        };
        if !spec.valid() {
            continue;
        }
        let ack = parse_json(body)?;
        let want = serde_json::json!({
            "report": row_of[spec].to_string(),
            "duplicate": before.contains(spec),
        });
        if *status != 200 || ack != want {
            return Err(format!(
                "redelivering {spec:?} answered {status} {ack}, not {want}"
            ));
        }
    }
    let mut delivered = BTreeSet::new();
    for (spec, reply) in &run.faulted {
        if let Some((200, body)) = reply
            && parse_json(body)?["duplicate"] == Value::Bool(true)
        {
            if !delivered.contains(spec) {
                return Err(format!("the first delivery of {spec:?} was acked as a duplicate"));
            }
            sim.cover("pricewatch:duplicate-acked");
        }
        delivered.insert(spec);
    }
    Ok(())
}

/// An amount of zero or less is refused by name, saying what a valid amount
/// is, and never recorded, under faults or not.
fn a_bad_amount_is_refused_by_name(sim: &mut Sim) -> Result<(), String> {
    let Some(run) = scenario(sim)? else {
        return Ok(());
    };
    for (spec, reply) in &run.faulted {
        if let Some((200, _)) = reply
            && !spec.valid()
        {
            return Err(format!("{spec:?} has a bad amount but was acked with 200"));
        }
    }
    for spec in &run.after_tail.reports {
        if !spec.valid() {
            return Err(format!("{spec:?} has a bad amount but was recorded"));
        }
    }
    let mut judged = false;
    for (ask, status, body) in &run.tail {
        let Ask::Redeliver(spec) = ask else {
            continue;
        };
        if spec.valid() {
            continue;
        }
        let want = amount_refusal(spec.amount);
        if *status != 400 || error_of(body) != want {
            return Err(format!(
                "a report of {} cents answered {status} {body}, not 400 {want:?}",
                spec.amount
            ));
        }
        judged = true;
    }
    if !judged {
        sim.give_up("no bad amount was drawn, so there was no refusal to judge", "");
        return Ok(());
    }
    sim.cover("pricewatch:refused-amount");
    Ok(())
}

/// What `GET /sources/:id/reports` must list: that source's rows, in id
/// order.
fn listing_of(state: &State, source: u64) -> Value {
    let mut listed = Vec::new();
    for (row, spec) in &state.rows {
        if spec.source == source {
            listed.push(serde_json::json!({
                "report": row.to_string(),
                "product": spec.product.to_string(),
                "amount_cents": spec.amount.to_string(),
                "observed_at_ms": spec.observed.to_string(),
            }));
        }
    }
    Value::Array(listed)
}

/// Each route reaches exactly who it should: a claim rotates the operator key, the
/// operator's routes refuse sources and the anonymous, reports come only from
/// sources, and the operator's listing of a source's reports is exactly that
/// source's durable rows.
fn only_the_right_credentials_reach_each_route(sim: &mut Sim) -> Result<(), String> {
    let Some(run) = scenario(sim)? else {
        return Ok(());
    };
    match configure_operator_secret("too-short") {
        Err(e) if e.to_string().contains("at least 16 characters") => {}
        other => return Err(format!("a 9-character operator secret was not refused for its length: {other:?}")),
    }
    for (ask, status, body) in &run.tail {
        match ask {
            Ask::Fence(what, want, text) => {
                if status != want {
                    return Err(format!("{what} answered {status} {body}, not {want}"));
                }
                if let Some(text) = text
                    && error_of(body) != *text
                {
                    return Err(format!("{what} was refused with {body}, not {text:?}"));
                }
            }
            Ask::ListSource(source) => {
                let want = listing_of(&run.after_tail, *source);
                let got = parse_json(body)?;
                if *status != 200 || got != want {
                    return Err(format!(
                        "GET /sources/{source}/reports answered {status} {got}, not {want}"
                    ));
                }
            }
            Ask::Redeliver(_) | Ask::ReadProduct(_) => {}
        }
    }
    Ok(())
}
