//! pricewatch: sources report prices for products; each product keeps its
//! lowest price ever reported and its latest report. Reports arrive late,
//! out of order and more than once; `merge` makes the kept values a function
//! of the set of reports, never of their order.

pub mod merge;

use charpente::auth::ApiKey;
use charpente::core::io::{Disk, Entropy};
use charpente::http::{ApiError, App, AppConfig, Ctx};
use charpente::wire::{WireI64, WireU64};
use charpente::{Id, Schema, Tx, View, Wire, route, routes, table};
use merge::Obs;
use serde::{Deserialize, Serialize};
use std::io;
use std::sync::OnceLock;

/// The subject of the one operator key (`POST /operator/key`).
pub const OPERATOR: &str = "operator";

/// The refusal an operator-only route gives everyone else.
pub const OPERATORS_ONLY: &str = "operators only — present the operator's API key";

/// The refusal `POST /reports` gives a credential that is not a source's.
pub const SOURCES_ONLY: &str = "only a source's API key may submit a report";

/// The deploy-time secret that buys the operator's API key, set once per
/// process by `configure_operator_secret` before the app serves.
static OPERATOR_SECRET: OnceLock<String> = OnceLock::new();

/// Sets the secret `POST /operator/key` asks for. `main` reads it from
/// `PRICEWATCH_OPERATOR_SECRET`. A second, different secret is refused.
pub fn configure_operator_secret(secret: &str) -> io::Result<()> {
    if secret.len() < 16 {
        return Err(io::Error::other(
            "the operator secret must be at least 16 characters",
        ));
    }
    if OPERATOR_SECRET.get_or_init(|| secret.to_string()) == secret {
        Ok(())
    } else {
        Err(io::Error::other("the operator secret is already set to another value"))
    }
}

/// A product, registered by an operator.
#[table(global)]
pub struct Product {
    pub id: Id<Product>,
    pub name: String,
}

/// A price feed. Its API key's subject is `source:<id>`.
#[table(global)]
pub struct Source {
    pub id: Id<Source>,
}

/// Every distinct report ever acknowledged. `key` is its identity
/// (`merge::report_key`), so a redelivery finds the first delivery's row.
#[table(global)]
pub struct Report {
    pub id: Id<Report>,
    #[index]
    pub key: String,
    #[index]
    pub source: Id<Source>,
    pub product: Id<Product>,
    pub amount_cents: i64,
    pub observed_at_ms: u64,
}

/// The two values kept per product, keyed by the product's raw id. Written
/// in the same transaction as the report that changed them.
#[table(global)]
pub struct Prices {
    pub id: Id<Prices>,
    pub lowest_amount_cents: i64,
    pub lowest_source: Id<Source>,
    pub lowest_observed_at_ms: u64,
    pub latest_amount_cents: i64,
    pub latest_source: Id<Source>,
    pub latest_observed_at_ms: u64,
}

impl Prices {
    pub fn lowest(&self) -> Obs {
        Obs {
            amount_cents: self.lowest_amount_cents,
            source: self.lowest_source.raw(),
            observed_at_ms: self.lowest_observed_at_ms,
        }
    }

    pub fn latest(&self) -> Obs {
        Obs {
            amount_cents: self.latest_amount_cents,
            source: self.latest_source.raw(),
            observed_at_ms: self.latest_observed_at_ms,
        }
    }

    fn from_parts(id: Id<Prices>, lowest: Obs, latest: Obs) -> Prices {
        Prices {
            id,
            lowest_amount_cents: lowest.amount_cents,
            lowest_source: Id::from_raw(lowest.source),
            lowest_observed_at_ms: lowest.observed_at_ms,
            latest_amount_cents: latest.amount_cents,
            latest_source: Id::from_raw(latest.source),
            latest_observed_at_ms: latest.observed_at_ms,
        }
    }
}

/// The id counter for reports (GUIDE §2, "Generating ids").
#[table(global)]
pub struct IdCounter {
    pub id: Id<IdCounter>,
    pub next: u64,
}

fn next_report_id(tx: &mut Tx) -> io::Result<Id<Report>> {
    let counter_id = Id::<IdCounter>::from_raw(0);
    let mut counter = tx.get(counter_id)?.unwrap_or(IdCounter {
        id: counter_id,
        next: 0,
    });
    let value = counter.next;
    counter.next += 1;
    tx.put(&counter);
    Ok(Id::from_raw(value))
}

#[derive(Wire, Serialize, Deserialize)]
pub struct OperatorClaim {
    pub secret: String,
}

#[derive(Wire, Serialize, Deserialize)]
pub struct NewProduct {
    pub name: String,
}

#[derive(Wire, Serialize, Deserialize)]
pub struct NewReport {
    pub product: Id<Product>,
    pub amount_cents: WireI64,
    pub observed_at_ms: WireU64,
}

#[derive(Wire, Serialize, Deserialize)]
pub struct ReportAck {
    pub report: Id<Report>,
    /// True when this delivery found the report already recorded.
    pub duplicate: bool,
}

#[derive(Wire, Serialize, Deserialize)]
pub struct PriceView {
    pub amount_cents: WireI64,
    pub source: Id<Source>,
    pub observed_at_ms: WireU64,
}

impl From<Obs> for PriceView {
    fn from(obs: Obs) -> Self {
        PriceView {
            amount_cents: WireI64(obs.amount_cents),
            source: Id::from_raw(obs.source),
            observed_at_ms: WireU64(obs.observed_at_ms),
        }
    }
}

#[derive(Wire, Serialize, Deserialize)]
pub struct ProductView {
    pub id: Id<Product>,
    pub name: String,
    pub lowest: Option<PriceView>,
    pub latest: Option<PriceView>,
}

#[derive(Wire, Serialize, Deserialize)]
pub struct ReportView {
    pub report: Id<Report>,
    pub product: Id<Product>,
    pub amount_cents: WireI64,
    pub observed_at_ms: WireU64,
}

/// Refuses every caller but the operator.
fn require_operator(ctx: &Ctx) -> Result<(), ApiError> {
    match ctx.principal() {
        Some(who) if who.subject() == OPERATOR => Ok(()),
        _ => Err(ApiError::BadRequest(OPERATORS_ONLY.to_string())),
    }
}

/// The registered source a credential speaks for: its subject is
/// `source:<id>` and that source exists.
fn source_of(tx: &Tx, ctx: &Ctx) -> Result<Id<Source>, ApiError> {
    let raw = match ctx.principal() {
        Some(principal) => principal.subject().strip_prefix("source:"),
        None => None,
    };
    if let Some(Ok(raw)) = raw.map(str::parse::<u64>) {
        let source = Id::<Source>::from_raw(raw);
        if tx.get(source)?.is_some() {
            return Ok(source);
        }
    }
    Err(ApiError::BadRequest(SOURCES_ONLY.to_string()))
}

/// Trades the deploy-time operator secret for a fresh operator API key and
/// revokes every earlier one: a claim rotates the key, so a claim whose reply
/// was lost can simply be made again. charpente issues no first credential
/// outside a request (FINDINGS.md), so this is the one door to the operator.
#[route(POST "/operator/key", public)]
fn mint_operator_key(tx: &mut Tx, body: OperatorClaim, ctx: &Ctx) -> Result<String, ApiError> {
    match OPERATOR_SECRET.get() {
        Some(secret) if *secret == body.secret => {}
        _ => return Err(ApiError::UnrecognizedCredential),
    }
    for minted in tx.scan::<ApiKey>()? {
        if minted.subject == OPERATOR {
            tx.delete::<ApiKey>(minted.id);
        }
    }
    Ok(ctx.create_api_key(tx, OPERATOR.to_string()))
}

/// Registers (or renames) a product. Its kept prices are untouched.
#[route(POST "/products/:id", auth)]
fn register_product(
    tx: &mut Tx,
    id: Id<Product>,
    body: NewProduct,
    ctx: &Ctx,
) -> Result<Id<Product>, ApiError> {
    require_operator(ctx)?;
    tx.put(&Product {
        id,
        name: body.name,
    });
    Ok(id)
}

/// Registers a source and issues it an API key (GUIDE §5). Calling it again
/// issues another key for the same source.
#[route(POST "/sources/:id", auth)]
fn register_source(tx: &mut Tx, id: Id<Source>, ctx: &Ctx) -> Result<String, ApiError> {
    require_operator(ctx)?;
    tx.put(&Source { id });
    Ok(ctx.create_api_key(tx, format!("source:{}", id.raw())))
}

/// A source reports a price. The report, and any change it makes to the
/// product's kept prices, commit as one record; a redelivery changes nothing.
#[route(POST "/reports", auth)]
fn submit_report(tx: &mut Tx, body: NewReport, ctx: &Ctx) -> Result<ReportAck, ApiError> {
    let source = source_of(tx, ctx)?;
    let amount_cents = merge::check_amount(body.amount_cents.0).map_err(ApiError::BadRequest)?;
    let product = body.product;
    if tx.get(product)?.is_none() {
        return Err(ApiError::NotFound);
    }
    let observed_at_ms = body.observed_at_ms.0;
    let key = merge::report_key(product.raw(), source.raw(), observed_at_ms, amount_cents);
    if let Some(first) = Report::by_key(tx, &key)?.first() {
        return Ok(ReportAck {
            report: first.id,
            duplicate: true,
        });
    }
    let report = next_report_id(tx)?;
    tx.put(&Report {
        id: report,
        key,
        source,
        product,
        amount_cents,
        observed_at_ms,
    });
    let incoming = Obs {
        amount_cents,
        source: source.raw(),
        observed_at_ms,
    };
    let prices = Id::<Prices>::from_raw(product.raw());
    let kept = tx.get(prices)?;
    tx.put(&Prices::from_parts(
        prices,
        merge::lowest(kept.as_ref().map(Prices::lowest), incoming),
        merge::latest(kept.as_ref().map(Prices::latest), incoming),
    ));
    Ok(ReportAck {
        report,
        duplicate: false,
    })
}

/// Anyone reads a product's lowest price and latest report.
#[route(GET "/products/:id", public)]
fn read_product(tx: &mut Tx, id: Id<Product>) -> Result<ProductView, ApiError> {
    let product = tx.get(id)?.ok_or(ApiError::NotFound)?;
    let kept = tx.get(Id::<Prices>::from_raw(id.raw()))?;
    Ok(ProductView {
        id,
        name: product.name,
        lowest: kept.as_ref().map(Prices::lowest).map(PriceView::from),
        latest: kept.as_ref().map(Prices::latest).map(PriceView::from),
    })
}

impl From<Report> for ReportView {
    fn from(row: Report) -> Self {
        ReportView {
            report: row.id,
            product: row.product,
            amount_cents: WireI64(row.amount_cents),
            observed_at_ms: WireU64(row.observed_at_ms),
        }
    }
}

/// An operator lists the reports one source has made, in the order they
/// were first acknowledged.
#[route(GET "/sources/:id/reports", auth)]
fn list_source_reports(
    tx: &mut Tx,
    id: Id<Source>,
    ctx: &Ctx,
) -> Result<Vec<ReportView>, ApiError> {
    require_operator(ctx)?;
    Ok(Report::by_source(tx, &id)?
        .into_iter()
        .map(ReportView::from)
        .collect())
}

/// The app, in the shape `charpente::launch` and the sim both take.
pub fn build_app(disk: Box<dyn Disk>, entropy: Box<dyn Entropy>) -> io::Result<App> {
    Ok(App::open(
        disk,
        Schema::new()
            .table::<Product>()
            .table::<Source>()
            .table::<Report>()
            .table::<Prices>()
            .table::<IdCounter>(),
        entropy,
        AppConfig::default(),
    )?
    .mount(routes![
        mint_operator_key,
        register_product,
        register_source,
        submit_report,
        read_product,
        list_source_reports
    ]))
}
