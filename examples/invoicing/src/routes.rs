//! HTTP routes. Response bodies use dedicated `*View` DTOs rather than
//! returning `#[table]` rows directly over JSON.
//!
//! **Updated (task 5):** the doc comment used to say views exist to
//! "sidestep the open question of whether `Id<T>` itself implements
//! `serde::Serialize`" — task 2 answered that question (it does, string-
//! encoded, so a JS client never loses precision on a row id). The `*View`
//! split still earns its keep for an unrelated reason: a route's response
//! must be a type distinct from its table row, because a table row can carry
//! fields (or, after a schema migration, an old shape) that were never meant
//! to cross the wire, and because `#[derive(Wire)]` types cannot be generic
//! while `#[table]` rows commonly are used generically inside the store
//! layer.

use crate::jobs::{ReceiptSent, SendReceipt};
use crate::model::{
    APP_CLOCK_ID, AppClock, Customer, Invoice, InvoiceState, LINE_ITEM_SEQ, LineItem, next_id,
};
use charpente::auth::ApiKey;
use charpente::core::table::Id;
use charpente::http::{ApiError, Ctx, Cursor, Subscription};
use charpente::wire::{WireI64, WireU64};
use charpente::{Tx, View, Wire as WireDerive, event, route};
use serde::{Deserialize, Serialize};

#[event(global)]
pub struct InvoiceFinalized {
    pub invoice: Id<Invoice>,
}

// ---- Customers ----------------------------------------------------------

#[derive(WireDerive, Serialize, Deserialize)]
pub struct CreateCustomerBody {
    pub name: String,
    pub email: String,
}

#[derive(WireDerive, Serialize)]
pub struct CustomerView {
    pub id: Id<Customer>,
    pub name: String,
    pub email: String,
}

impl From<Customer> for CustomerView {
    fn from(c: Customer) -> Self {
        CustomerView {
            id: c.id,
            name: c.name,
            email: c.email,
        }
    }
}

// Demo only: this login mints a session for whatever subject the body names.
// A real app trades a verified credential for a session (GUIDE §5's `login`).
#[route(POST "/login", public)]
pub fn login(tx: &mut Tx, subject: String, ctx: &Ctx) -> Result<String, ApiError> {
    Ok(ctx.create_session(tx, subject))
}

#[route(POST "/customers/:id", auth)]
pub fn create_customer(
    tx: &mut Tx,
    id: Id<Customer>,
    body: CreateCustomerBody,
) -> Result<CustomerView, ApiError> {
    if tx.get(id)?.is_some() {
        return Err(ApiError::BadRequest(format!(
            "customer {} already exists",
            id.raw()
        )));
    }
    let row = Customer {
        id,
        name: body.name,
        email: body.email,
    };
    tx.put(&row);
    Ok(row.into())
}

#[route(GET "/customers/:id", auth)]
pub fn get_customer(tx: &mut Tx, id: Id<Customer>) -> Result<CustomerView, ApiError> {
    Ok(tx.get(id)?.ok_or(ApiError::NotFound)?.into())
}

#[route(GET "/customers", auth)]
pub fn list_customers(tx: &mut Tx) -> Result<Vec<CustomerView>, ApiError> {
    Ok(tx.scan::<Customer>()?.into_iter().map(Into::into).collect())
}

// ---- Invoices -------------------------------------------------------------

#[derive(WireDerive, Serialize, Deserialize)]
pub struct LineItemInput {
    pub description: String,
    pub unit_amount_cents: WireI64,
    pub quantity: WireI64,
}

#[derive(WireDerive, Serialize, Deserialize)]
pub struct CreateInvoiceBody {
    pub customer: Id<Customer>,
    pub due_day: WireU64,
    pub line_items: Vec<LineItemInput>,
}

#[derive(WireDerive, Serialize)]
pub struct LineItemView {
    pub description: String,
    pub unit_amount_cents: WireI64,
    pub quantity: WireI64,
}

#[derive(WireDerive, Serialize)]
pub struct InvoiceView {
    pub id: Id<Invoice>,
    pub customer: Id<Customer>,
    pub state: String,
    pub due_day: WireU64,
    pub line_items: Vec<LineItemView>,
    pub total_cents: WireI64,
}

fn state_name(state: InvoiceState) -> &'static str {
    match state {
        InvoiceState::Draft => "draft",
        InvoiceState::Final => "final",
        InvoiceState::Paid => "paid",
    }
}

fn invoice_view(tx: &mut Tx, inv: &Invoice) -> std::io::Result<InvoiceView> {
    let items = LineItem::by_invoice(tx, &inv.id)?;
    let total_cents = items
        .iter()
        .map(|li| li.unit_amount_cents * li.quantity)
        .sum();
    Ok(InvoiceView {
        id: inv.id,
        customer: inv.customer,
        state: state_name(inv.state).to_string(),
        due_day: WireU64(inv.due_day),
        line_items: items
            .into_iter()
            .map(|li| LineItemView {
                description: li.description,
                unit_amount_cents: WireI64(li.unit_amount_cents),
                quantity: WireI64(li.quantity),
            })
            .collect(),
        total_cents: WireI64(total_cents),
    })
}

#[route(POST "/invoices/:id", auth)]
pub fn create_invoice(
    tx: &mut Tx,
    id: Id<Invoice>,
    body: CreateInvoiceBody,
) -> Result<InvoiceView, ApiError> {
    if tx.get(body.customer)?.is_none() {
        return Err(ApiError::BadRequest(format!(
            "customer {} does not exist",
            body.customer.raw()
        )));
    }
    if tx.get(id)?.is_some() {
        return Err(ApiError::BadRequest(format!(
            "invoice {} already exists",
            id.raw()
        )));
    }

    let inv = Invoice {
        id,
        customer: body.customer,
        state: InvoiceState::Draft,
        due_day: body.due_day.0,
        notes: String::new(),
    };
    tx.put(&inv);

    let mut total_cents: i64 = 0;
    let mut items = Vec::with_capacity(body.line_items.len());
    for item in body.line_items {
        let li_id = next_id::<LineItem>(tx, LINE_ITEM_SEQ)?;
        total_cents += item.unit_amount_cents.0 * item.quantity.0;
        tx.put(&LineItem {
            id: li_id,
            invoice: id,
            description: item.description.clone(),
            unit_amount_cents: item.unit_amount_cents.0,
            quantity: item.quantity.0,
        });
        items.push(LineItemView {
            description: item.description,
            unit_amount_cents: item.unit_amount_cents,
            quantity: item.quantity,
        });
    }

    Ok(InvoiceView {
        id,
        customer: body.customer,
        state: state_name(inv.state).to_string(),
        due_day: WireU64(inv.due_day),
        line_items: items,
        total_cents: WireI64(total_cents),
    })
}

#[route(GET "/invoices/:id", auth)]
pub fn get_invoice(tx: &mut Tx, id: Id<Invoice>) -> Result<InvoiceView, ApiError> {
    let inv = tx.get(id)?.ok_or(ApiError::NotFound)?;
    Ok(invoice_view(tx, &inv)?)
}

#[route(GET "/invoices", auth)]
pub fn list_invoices(tx: &mut Tx) -> Result<Vec<InvoiceView>, ApiError> {
    let mut out = Vec::new();
    for inv in tx.scan::<Invoice>()? {
        out.push(invoice_view(tx, &inv)?);
    }
    Ok(out)
}

#[route(POST "/invoices/:id/finalize", auth)]
pub fn finalize_invoice(tx: &mut Tx, id: Id<Invoice>, ctx: &Ctx) -> Result<InvoiceView, ApiError> {
    let mut inv = tx.get(id)?.ok_or(ApiError::NotFound)?;
    if inv.state != InvoiceState::Draft {
        return Err(ApiError::BadRequest(format!(
            "invoice {} is {:?}, not Draft — only a Draft invoice can be finalized",
            id.raw(),
            inv.state
        )));
    }
    inv.state = InvoiceState::Final;
    tx.put(&inv);
    // The org key scoping `/events/feed` (below): this app has no separate
    // org table, so the logged-in session's own subject stands in for the
    // tenant identity — every event an org's data produces is emitted keyed
    // to it, and `SendReceipt` carries it through to the job that emits
    // `ReceiptSent` later (jobs have no `Ctx` of their own).
    let org = ctx
        .session()
        .expect("auth routes always have a session")
        .subject
        .clone();
    tx.enqueue(SendReceipt {
        invoice: id,
        org: org.clone(),
    });
    tx.emit_keyed(org, InvoiceFinalized { invoice: id });
    Ok(invoice_view(tx, &inv)?)
}

#[route(POST "/invoices/:id/record-payment", auth)]
pub fn record_payment(tx: &mut Tx, id: Id<Invoice>) -> Result<InvoiceView, ApiError> {
    let mut inv = tx.get(id)?.ok_or(ApiError::NotFound)?;
    if inv.state != InvoiceState::Final {
        return Err(ApiError::BadRequest(format!(
            "invoice {} is {:?}, not Final — only a Final invoice can be paid",
            id.raw(),
            inv.state
        )));
    }
    inv.state = InvoiceState::Paid;
    tx.put(&inv);
    Ok(invoice_view(tx, &inv)?)
}

/// Read-only helper the sim tests use to inspect the app clock without
/// duplicating `AppClock`'s field access.
pub fn current_day(tx: &mut Tx) -> std::io::Result<u64> {
    Ok(tx
        .get(Id::<AppClock>::from_raw(APP_CLOCK_ID))?
        .map_or(0, |c| c.day))
}

// ---- Realtime -------------------------------------------------------------

/// One org-scoped feed of both realtime events this app emits — GUIDE §11.
/// `auth` is what makes an org key available at all: the caller's own
/// session subject *is* the org key `finalize_invoice` emitted with, so a
/// caller only ever tails its own org's events, never anyone else's.
#[route(SSE "/events/feed", auth)]
pub fn events_feed(
    tx: &mut Tx,
    ctx: &Ctx,
    cursor: Option<Cursor>,
) -> Result<Subscription, ApiError> {
    let _ = tx;
    let org = ctx
        .session()
        .expect("auth routes always have a session")
        .subject
        .clone();
    Ok(Subscription::of::<InvoiceFinalized>()
        .and::<ReceiptSent>()
        .key(org)
        .cursor(cursor))
}

// ---- API keys (auth T9 demo) -----------------------------------------------

/// Mints an API key for the caller's own subject. Returns the raw
/// `"ck_"`-prefixed key once; only its hash is ever stored (auth T3) — the
/// same shape as `crates/charpente/tests/http.rs`'s `create_key`.
#[route(POST "/keys", auth)]
pub fn create_key(tx: &mut Tx, ctx: &Ctx) -> Result<String, ApiError> {
    let subject = ctx
        .session()
        .expect("auth routes always have a session")
        .subject
        .clone();
    Ok(ctx.create_api_key(tx, subject))
}

/// Revokes an API key by id — revoke-as-delete (auth T3): no separate
/// revoked flag, the row is simply gone, so the next request bearing that
/// key falls through the resolution ladder to the unrecognized-credential
/// 401.
#[route(DELETE "/keys/:id", auth)]
pub fn revoke_key(tx: &mut Tx, id: Id<ApiKey>, ctx: &Ctx) -> Result<&'static str, ApiError> {
    let _ = ctx;
    tx.delete::<ApiKey>(id);
    Ok("revoked")
}
