//! The dogfood app for the TypeScript client (TS-codegen spec §5's
//! "dogfood target"). Deliberately small in the spirit of
//! `crates/sim-fixture` — it exists to be *described*, not to be an
//! application.
//!
//! **What makes it worth a whole crate.** `crates/charpente-tsgen/tests/
//! common/mod.rs` reaches every `TypeSchema` and `EnumRepr` shape, but it is
//! a HAND-BUILT `Contract`: it proves things about the emitter and nothing
//! about the path from Rust source to generated TypeScript. Its own doc
//! comment names this fixture as the oracle that closes that gap. So every
//! shape it hand-builds is reached HERE through the real macros —
//! `#[table]`, `#[route]`, `#[event]`, `#[derive(Wire)]` — and the client
//! generated from the result is type-checked by `tsc` against real call
//! sites (`ts-fixture/src/calls.ts`).
//!
//! The handlers are stubs on purpose. Nothing here is executed by a request
//! in anger; `charpente describe` walks the registry, and the drift probe
//! (`tests/drift.rs`) runs only the subscribe glue. Bodies that did real
//! work would be surface with no oracle attached.
//!
//! Every derive reaches serde through the `::charpente::serde` re-export, so
//! this crate has no direct `serde` dependency and pins macro-path expansion
//! the way `crates/sim-fixture` does.

pub mod samples;

use std::collections::BTreeMap;

use charpente::core::io::{Disk, Entropy};
use charpente::http::{ApiError, App, AppConfig, Ctx, Cursor, Subscription};
use charpente::serde::{Deserialize, Serialize};
use charpente::wire::{WireI64, WireU64};
use charpente::{Id, Schema, Tx, View, Wire as WireDerive, event, route, routes};
use charpente::{events as events_macro, table};

// ------------------------------------------------------------------ tables
//
// Three, for three different reasons: `Customer` and `Invoice` give the
// generated client two DIFFERENT branded id types, which is what makes
// `ts-fixture/src/negatives.ts` a real assertion rather than a comment; and
// `Widget`'s id is reachable ONLY through a route param, never through a
// registered wire type, which is the case `emit_types` had to grow a second
// collection loop for (types.rs, review round 1 Important 6).

#[table]
pub struct Customer {
    pub id: Id<Customer>,
    pub name: String,
}

#[table]
pub struct Invoice {
    pub id: Id<Invoice>,
    pub customer: Id<Customer>,
}

#[table]
pub struct Widget {
    pub id: Id<Widget>,
    pub hits: u64,
}

// ------------------------------------------------------------- wire types

/// A request body: `rename_all = "camelCase"` so the wire names differ from
/// the Rust names, which is what makes the serde-fidelity literal in
/// `ts-fixture/src/generated/samples.ts` a check rather than a tautology.
#[derive(WireDerive, Serialize, Deserialize)]
#[serde(crate = "::charpente::serde")]
#[serde(rename_all = "camelCase")]
pub struct CreateCustomerBody {
    pub display_name: String,
    pub email_address: Option<String>,
}

/// A request body carrying an `Int64` (chunk B1a, A2 of `chunk-b1-brief.md`):
/// before this route existed, the fixture's only request body
/// (`CreateCustomerBody` above) had no `bigint` position, so nothing exercised
/// the generated client's outbound serialization of one. `JSON.stringify` on
/// a bigint throws — see `charpente-tsgen/src/runtime.rs`'s replacer comment.
#[derive(WireDerive, Serialize, Deserialize)]
#[serde(crate = "::charpente::serde")]
#[serde(rename_all = "camelCase")]
pub struct AdjustInvoiceBody {
    pub delta_cents: WireI64,
}

/// A route's ERROR type carrying an `Int64` (chunk B1b, exit criterion 5):
/// every OTHER route's error is `ApiError`, whose hand-written `Wire` impl
/// (`charpente/src/http/mod.rs`) is the fixed `{error: string}` envelope
/// with no `Int64` position anywhere — nothing before this route existed
/// exercised the generated client's ERROR-position revival at all.
///
/// **A latent framework gap this route's existence surfaces, not fixes**:
/// `#[route]`'s glue calls the handler with `?` (`route.rs:334`), which
/// needs `From<LimitExceeded> for ApiError` below to compile — meaning a
/// REAL running server would put `ApiError`'s fixed envelope on the wire for
/// this route's error case, never `LimitExceeded`'s own shape, no matter
/// what `RouteInfo.error`'s schema advertises to a generated client. That
/// does not affect anything this fixture checks: `withdraw_invoice` is never
/// invoked by a real request (`lib.rs`'s own doc comment — `describe()` only
/// walks the registry), and `ts-fixture/*.test.ts` never hits a live server
/// either (every test stubs `fetch()` with a real `serde_json` document from
/// `samples.rs`). Recorded here rather than fixed, the same posture chunk-
/// b1-brief.md's A8 takes for a bare `Int64` in a path param.
#[derive(WireDerive, Serialize, Deserialize, Debug)]
#[serde(crate = "::charpente::serde")]
#[serde(rename_all = "camelCase")]
pub struct LimitExceeded {
    pub max_cents: WireI64,
}

impl From<LimitExceeded> for ApiError {
    fn from(e: LimitExceeded) -> ApiError {
        ApiError::BadRequest(format!("limit exceeded: {} cents", e.max_cents.0))
    }
}

#[derive(WireDerive, Serialize, Deserialize)]
#[serde(crate = "::charpente::serde")]
#[serde(rename_all = "camelCase")]
pub struct CustomerView {
    pub id: Id<Customer>,
    pub display_name: String,
    pub email_address: Option<String>,
    pub status: Status,
}

/// Every variant is a unit variant: `EnumRepr::Unit`, a TypeScript string
/// union.
#[derive(WireDerive, Serialize, Deserialize)]
#[serde(crate = "::charpente::serde")]
#[serde(rename_all = "kebab-case")]
pub enum Status {
    Active,
    OnHold,
}

/// `skip_serializing_if` is the one thing that makes a field OPTIONAL in the
/// schema (`Field.optional`), which is a different TypeScript rendering
/// (`note?:`) from a plain `Option` (`| null`) — both appear here.
#[derive(WireDerive, Serialize, Deserialize)]
#[serde(crate = "::charpente::serde")]
#[serde(rename_all = "camelCase")]
pub struct LineItem {
    pub unit_amount_cents: WireI64,
    pub quantity: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

/// The wide one. Every remaining `TypeSchema` leaf is here so the generated
/// client has to render each of them, and so `samples.ts` puts serde's real
/// document for each of them in front of `tsc`.
#[derive(WireDerive, Serialize)]
#[serde(crate = "::charpente::serde")]
#[serde(rename_all = "camelCase")]
pub struct InvoiceView {
    pub id: Id<Invoice>,
    pub customer: Id<Customer>,
    /// `Int64 { signed: true }` — a decimal string on the wire.
    pub total_cents: WireI64,
    /// `Int64 { signed: false }`.
    pub sequence: WireU64,
    /// `Seq(Ref)`.
    pub line_items: Vec<LineItem>,
    /// `Seq(Option(_))` — `[]` binds tighter than `|` in TypeScript, so this
    /// is the shape that must render as `(string | null)[]`.
    pub tags: Vec<Option<String>>,
    /// `Map` — `BTreeMap` only; `HashMap` has no `Wire` impl on purpose.
    pub labels: BTreeMap<String, String>,
    pub paid: bool,
    pub ratio: f64,
    pub ratio32: f32,
    pub tiny: i8,
    pub small: i16,
    pub medium: i32,
    pub ubyte: u8,
    pub ushort: u16,
    pub payment: Payment,
    pub notice: Notification,
    pub last_item: FeedItem,
    /// `Option(Int64)` (chunk B1b): the wire ALWAYS carries this key, `null`
    /// when there is no adjustment — the A1 landmine's first shape. Distinct
    /// from `late_fee_cents` below, whose ABSENCE omits the key entirely
    /// rather than writing an explicit `null`.
    pub adjustment_cents: Option<WireI64>,
    /// `Field.optional` over a BARE `Int64`, no `Option` wrapper
    /// (chunk B1b): `skip_serializing_if` omits the key entirely below the
    /// threshold, which is a DIFFERENT runtime shape from `Option`'s
    /// explicit `null` — property access on an omitted key hands the revive
    /// walker `undefined`, not `null` (A1's second landmine shape).
    #[serde(skip_serializing_if = "is_zero_i64")]
    pub late_fee_cents: WireI64,
}

/// The `skip_serializing_if` predicate for `InvoiceView::late_fee_cents` —
/// any predicate name is accepted by `#[derive(Wire)]` (it only checks that
/// the attribute is PRESENT, not which function it names), so this is not
/// `Option::is_none`: the point of this field is a `skip_serializing_if` on
/// a type that is NOT `Option<_>`, the shape `Field.optional` takes without
/// also being `Option(_)` at the schema level.
fn is_zero_i64(v: &WireI64) -> bool {
    v.0 == 0
}

/// serde's externally-tagged default, reaching all four representable
/// variant shapes: a unit variant (the bare JSON string `"Ping"`), a newtype
/// variant over a primitive, a braced variant, and a BRACED-EMPTY variant —
/// `{"in-progress":{}}`, a genuinely different document from the unit case
/// (`EnumRepr::External`'s doc comment) — under a kebab-case name, which is
/// not a legal TypeScript object key unquoted.
///
/// `Ack` is `WireU64`, not a bare `u32` (chunk B1b): `Ping` alongside it is a
/// bare JSON STRING on the wire, not an object, so this is also the fixture
/// for the one hazard `EnumRepr::External`'s revive uniquely has — a
/// presence check (`"Ack" in v`) that runs on a legitimate `Ping` value
/// would THROW (`in` on a primitive string), which a mixed unit/Int64-
/// carrying externally-tagged enum is the minimal shape to prove doesn't
/// happen.
#[derive(WireDerive, Serialize)]
#[serde(crate = "::charpente::serde")]
pub enum FeedItem {
    Ping,
    Ack(WireU64),
    Message {
        body: String,
    },
    #[serde(rename = "in-progress")]
    InProgress {},
}

/// `#[serde(tag = "...")]`: the variant name merges into the payload object.
#[derive(WireDerive, Serialize)]
#[serde(crate = "::charpente::serde")]
#[serde(tag = "type")]
pub enum Payment {
    Cash,
    Card {
        #[serde(rename = "card-number")]
        card_number: String,
    },
}

/// `#[serde(tag = "...", content = "...")]`, with a kebab-case tag key — an
/// arbitrary string literal is legal there, so this needs no `rename_all` to
/// reach the quoting path.
#[derive(WireDerive, Serialize)]
#[serde(crate = "::charpente::serde")]
#[serde(tag = "kind-of", content = "payload")]
pub enum Notification {
    Started,
    Progress { percent: u32 },
}

// ------------------------------------------------------------------ events

#[event]
pub struct InvoicePaid {
    pub invoice: Id<Invoice>,
    /// `WireU64`, not a bare `u64`: an event payload IS the JSON a
    /// subscriber receives, so a 64-bit integer crosses as a string here for
    /// the same reason it does in a route type.
    pub amount_cents: WireU64,
}

#[event]
pub struct InvoiceVoided {
    pub invoice: Id<Invoice>,
}

// ------------------------------------------------------------------ routes

#[route(POST "/customers/:id", auth)]
pub fn create_customer(
    tx: &mut Tx,
    id: Id<Customer>,
    body: CreateCustomerBody,
) -> Result<CustomerView, ApiError> {
    let row = Customer {
        id,
        name: body.display_name,
    };
    tx.put(&row);
    Ok(CustomerView {
        id: row.id,
        display_name: row.name,
        email_address: body.email_address,
        status: Status::Active,
    })
}

#[route(GET "/invoices/:id")]
pub fn get_invoice(tx: &mut Tx, id: Id<Invoice>) -> Result<InvoiceView, ApiError> {
    let inv = tx.get(id)?.ok_or(ApiError::NotFound)?;
    Ok(samples::invoice_view(inv.id, inv.customer))
}

#[route(GET "/invoices")]
pub fn list_invoices(tx: &mut Tx) -> Result<Vec<InvoiceView>, ApiError> {
    Ok(tx
        .scan::<Invoice>()?
        .into_iter()
        .map(|inv| samples::invoice_view(inv.id, inv.customer))
        .collect())
}

/// The route that reaches an `Int64` in a REQUEST body (chunk B1a's coverage
/// gap, A2). The handler is a stub like its neighbours — nothing here is
/// executed in anger — so `body` is unused past being deserialized.
#[route(POST "/invoices/:id/adjust")]
pub fn adjust_invoice(
    tx: &mut Tx,
    id: Id<Invoice>,
    body: AdjustInvoiceBody,
) -> Result<InvoiceView, ApiError> {
    let inv = tx.get(id)?.ok_or(ApiError::NotFound)?;
    let _ = body;
    Ok(samples::invoice_view(inv.id, inv.customer))
}

/// The route whose ERROR type carries an `Int64` (chunk B1b, exit
/// criterion 5) — see `LimitExceeded`'s doc comment for what this route
/// does and does not prove. The handler is a stub like its neighbours.
#[route(POST "/invoices/:id/withdraw")]
pub fn withdraw_invoice(tx: &mut Tx, id: Id<Invoice>) -> Result<InvoiceView, LimitExceeded> {
    let _ = tx;
    Ok(samples::invoice_view(id, Id::<Customer>::from_raw(0)))
}

/// `-> Result<(), ApiError>`: the response schema is `Null`, the JSON
/// literal `null`, which is a different document from `{}`. Also the one
/// route whose `Id<Widget>` never appears in any registered wire type.
#[route(DELETE "/widgets/:id")]
pub fn delete_widget(tx: &mut Tx, id: Id<Widget>) -> Result<(), ApiError> {
    let _ = (tx, id);
    Ok(())
}

/// The declared stream: `emits(...)` names both payloads, so the generated
/// client gets a `FeedEvent` union discriminated by the frame's `event:`
/// name. `tests/drift.rs` checks the declaration against what the
/// `Subscription` below actually subscribes to.
#[route(SSE "/feed", auth, emits(InvoicePaid, InvoiceVoided))]
pub fn feed(tx: &mut Tx, ctx: &Ctx, cursor: Option<Cursor>) -> Result<Subscription, ApiError> {
    let _ = (tx, ctx);
    Ok(Subscription::of::<InvoicePaid>()
        .and::<InvoiceVoided>()
        .cursor(cursor))
}

/// A stream route that has NOT adopted `emits(...)`. Legal — the declaration
/// is optional — and the contract genuinely does not know what it carries,
/// so the generated client types it as the honest `UnknownEvent` rather than
/// inventing a union. Kept precisely so that branch has a real app behind it.
#[route(SSE "/raw-feed")]
pub fn raw_feed(tx: &mut Tx, ctx: &Ctx, cursor: Option<Cursor>) -> Result<Subscription, ApiError> {
    let _ = (tx, ctx);
    Ok(Subscription::of::<InvoicePaid>().cursor(cursor))
}

pub fn schema() -> Schema {
    Schema::new()
        .table::<Customer>()
        .table::<Invoice>()
        .table::<Widget>()
}

pub fn build_app(disk: Box<dyn Disk>, entropy: Box<dyn Entropy>) -> std::io::Result<App> {
    Ok(App::open(disk, schema(), entropy, AppConfig::default())?
        .mount(routes![
            create_customer,
            get_invoice,
            list_invoices,
            adjust_invoice,
            withdraw_invoice,
            delete_widget,
            feed,
            raw_feed,
        ])
        .mount_events(events_macro![InvoicePaid, InvoiceVoided]))
}
