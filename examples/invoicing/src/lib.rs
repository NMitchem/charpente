//! A minimal invoicing SaaS built on charpente, against README.md and
//! docs/GUIDE.md alone. See examples/invoicing/FINDINGS.md for the running
//! log of documentation gaps and surprises hit while building this.

pub mod jobs;
pub mod model;
pub mod routes;

use charpente::core::io::{Disk, Entropy};
use charpente::http::{App, AppConfig};
use charpente::{Schema, events as events_macro, jobs as jobs_macro, routes as routes_macro};
use jobs::{AdvanceDay, ReceiptSent, SendOverdueReminder, SendReceipt};
use routes::{
    InvoiceFinalized, create_customer, create_invoice, create_key, events_feed, finalize_invoice,
    get_customer, get_invoice, list_customers, list_invoices, login, record_payment, revoke_key,
};
use std::time::Duration;

pub fn schema() -> Schema {
    Schema::new()
        .table::<model::Customer>()
        .table::<model::Invoice>()
        .migration::<model::Invoice>("Invoice")
        .table::<model::LineItem>()
        .table::<model::Receipt>()
        .table::<model::Reminder>()
        .table::<model::AppClock>()
        .table::<model::IdCounter>()
}

/// One daily cron tick — `jobs::AdvanceDay` — drives every time-dependent
/// behavior in this app (see model.rs's module doc).
pub const DAILY_TICK: Duration = Duration::from_secs(24 * 3600);

pub fn build_app(disk: Box<dyn Disk>, entropy: Box<dyn Entropy>) -> std::io::Result<App> {
    Ok(App::open(disk, schema(), entropy, AppConfig::default())?
        .mount(routes_macro![
            login,
            create_customer,
            get_customer,
            list_customers,
            create_invoice,
            get_invoice,
            list_invoices,
            finalize_invoice,
            record_payment,
            events_feed,
            create_key,
            revoke_key,
        ])
        .mount_jobs(jobs_macro![SendReceipt, SendOverdueReminder, AdvanceDay])
        // Both events this app emits, so `charpente describe` (and the TS
        // client generated from it) can name the two payload shapes
        // `/events/feed` delivers. Describe-only: delivery works either way.
        .mount_events(events_macro![InvoiceFinalized, ReceiptSent])
        .schedule("daily-overdue-check", DAILY_TICK, || AdvanceDay {}))
}
