//! HTTP routes. Response bodies use dedicated `*View` DTOs rather than
//! returning `#[table]` rows directly over JSON — GUIDE.md never shows a
//! table row as a route's `Ok` payload (every example returns a bare `u64`
//! or `String`, see FINDINGS.md), so this sidesteps the open question of
//! whether `Id<T>` itself implements `serde::Serialize`.

use crate::jobs::{ReceiptSent, SendReceipt};
use crate::model::{
    APP_CLOCK_ID, AppClock, Customer, Invoice, InvoiceState, LINE_ITEM_SEQ, LineItem, next_id,
};
use charpente::auth::ApiKey;
use charpente::core::table::Id;
use charpente::http::{ApiError, Ctx, Cursor, Subscription};
use charpente::{Tx, View, event, route};
use serde::{Deserialize, Serialize};

#[event]
pub struct InvoiceFinalized {
    pub invoice: u64,
}

// ---- Customers ----------------------------------------------------------

#[derive(Serialize, Deserialize)]
pub struct CreateCustomerBody {
    pub name: String,
    pub email: String,
}

#[derive(Serialize)]
pub struct CustomerView {
    pub id: u64,
    pub name: String,
    pub email: String,
}

impl From<Customer> for CustomerView {
    fn from(c: Customer) -> Self {
        CustomerView {
            id: c.id.raw(),
            name: c.name,
            email: c.email,
        }
    }
}

#[route(POST "/login")]
pub fn login(tx: &mut Tx, subject: String, ctx: &Ctx) -> Result<String, ApiError> {
    Ok(ctx.create_session(tx, subject))
}

#[route(POST "/customers/:id", auth)]
pub fn create_customer(
    tx: &mut Tx,
    id: u64,
    body: CreateCustomerBody,
) -> Result<CustomerView, ApiError> {
    let customer_id = Id::<Customer>::from_raw(id);
    if tx.get(customer_id)?.is_some() {
        return Err(ApiError::BadRequest(format!(
            "customer {id} already exists"
        )));
    }
    let row = Customer {
        id: customer_id,
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

#[derive(Serialize, Deserialize)]
pub struct LineItemInput {
    pub description: String,
    pub unit_amount_cents: i64,
    pub quantity: i64,
}

#[derive(Serialize, Deserialize)]
pub struct CreateInvoiceBody {
    pub customer: u64,
    pub due_day: u64,
    pub line_items: Vec<LineItemInput>,
}

#[derive(Serialize)]
pub struct LineItemView {
    pub description: String,
    pub unit_amount_cents: i64,
    pub quantity: i64,
}

#[derive(Serialize)]
pub struct InvoiceView {
    pub id: u64,
    pub customer: u64,
    pub state: String,
    pub due_day: u64,
    pub line_items: Vec<LineItemView>,
    pub total_cents: i64,
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
        id: inv.id.raw(),
        customer: inv.customer.raw(),
        state: state_name(inv.state).to_string(),
        due_day: inv.due_day,
        line_items: items
            .into_iter()
            .map(|li| LineItemView {
                description: li.description,
                unit_amount_cents: li.unit_amount_cents,
                quantity: li.quantity,
            })
            .collect(),
        total_cents,
    })
}

#[route(POST "/invoices/:id", auth)]
pub fn create_invoice(
    tx: &mut Tx,
    id: u64,
    body: CreateInvoiceBody,
) -> Result<InvoiceView, ApiError> {
    let customer_id = Id::<Customer>::from_raw(body.customer);
    if tx.get(customer_id)?.is_none() {
        return Err(ApiError::BadRequest(format!(
            "customer {} does not exist",
            body.customer
        )));
    }
    let invoice_id = Id::<Invoice>::from_raw(id);
    if tx.get(invoice_id)?.is_some() {
        return Err(ApiError::BadRequest(format!("invoice {id} already exists")));
    }

    let inv = Invoice {
        id: invoice_id,
        customer: customer_id,
        state: InvoiceState::Draft,
        due_day: body.due_day,
    };
    tx.put(&inv);

    let mut total_cents: i64 = 0;
    let mut items = Vec::with_capacity(body.line_items.len());
    for item in body.line_items {
        let li_id = next_id::<LineItem>(tx, LINE_ITEM_SEQ)?;
        total_cents += item.unit_amount_cents * item.quantity;
        tx.put(&LineItem {
            id: li_id,
            invoice: invoice_id,
            description: item.description.clone(),
            unit_amount_cents: item.unit_amount_cents,
            quantity: item.quantity,
        });
        items.push(LineItemView {
            description: item.description,
            unit_amount_cents: item.unit_amount_cents,
            quantity: item.quantity,
        });
    }

    Ok(InvoiceView {
        id: invoice_id.raw(),
        customer: body.customer,
        state: state_name(inv.state).to_string(),
        due_day: inv.due_day,
        line_items: items,
        total_cents,
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
        invoice: id.raw(),
        org: org.clone(),
    });
    tx.emit_keyed(org, InvoiceFinalized { invoice: id.raw() });
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
