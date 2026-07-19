//! Tables and the hand-rolled `Codec` types that ride inside them.
//!
//! Design note (see FINDINGS.md): GUIDE.md never shows a transaction reading
//! current time — `Tx` only exposes table reads/writes (§4), and ambient
//! `SystemTime::now()` would make the app nondeterministic under
//! `charpente test --sim`. So "overdue" here is not "past an absolute
//! timestamp" — it is tracked with a single `AppClock` row that only ever
//! advances once per daily cron tick (`AdvanceDay`, in jobs.rs). Invoices
//! carry a `due_day` in that same app-relative day-count space. This rides
//! on cron's own no-backfill catch-up behavior (GUIDE §7) for free: a
//! catch-up tick still only advances the day counter by one, exactly the
//! "missed days are not backfilled" behavior the brief asks for.

use charpente::core::migration::{MigrationError, TableMigration};
use charpente::core::table::Codec;
use charpente::{Id, Tx, View, row_shape, table};
use std::io;

#[table]
pub struct Customer {
    pub id: Id<Customer>,
    pub name: String,
    pub email: String,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum InvoiceState {
    Draft,
    Final,
    Paid,
}

impl Codec for InvoiceState {
    fn encode(&self, out: &mut Vec<u8>) {
        let tag: u8 = match self {
            InvoiceState::Draft => 0,
            InvoiceState::Final => 1,
            InvoiceState::Paid => 2,
        };
        tag.encode(out);
    }

    fn decode(input: &mut &[u8]) -> io::Result<Self> {
        match u8::decode(input)? {
            0 => Ok(InvoiceState::Draft),
            1 => Ok(InvoiceState::Final),
            2 => Ok(InvoiceState::Paid),
            other => Err(io::Error::other(format!(
                "invalid InvoiceState tag {other} — schema drift or corruption"
            ))),
        }
    }
}

/// The superseded shape — kept only so `TableMigration::migrate` can decode
/// bytes an older binary wrote. No `#[index]`, no redb registration.
#[row_shape(version = 1)]
pub struct InvoiceV1 {
    pub id: Id<InvoiceV1>,
    pub customer: Id<Customer>,
    pub state: InvoiceState,
    pub due_day: u64,
}

/// Bumping to `version = 2` without the `TableMigration` impl below is a
/// compile error — the macro emits an assert-impl const.
#[table(version = 2)]
pub struct Invoice {
    pub id: Id<Invoice>,
    #[index]
    pub customer: Id<Customer>,
    pub state: InvoiceState,
    /// App-relative day this invoice is due, in `AppClock` units — not a
    /// wall-clock timestamp (see module doc).
    pub due_day: u64,
    /// Free-text notes on the invoice; added via the migration mechanism
    /// (GUIDE §12) — existing rows come back with `""`.
    pub notes: String,
}

impl TableMigration for Invoice {
    type Prev = InvoiceV1;
    fn migrate(prev: InvoiceV1) -> Result<Invoice, MigrationError> {
        Ok(Invoice {
            id: Id::from_raw(prev.id.raw()),
            customer: prev.customer,
            state: prev.state,
            due_day: prev.due_day,
            notes: String::new(), // the field's required default
        })
    }
}

#[table]
pub struct LineItem {
    pub id: Id<LineItem>,
    #[index]
    pub invoice: Id<Invoice>,
    pub description: String,
    pub unit_amount_cents: i64,
    pub quantity: i64,
}

#[table]
pub struct Receipt {
    pub id: Id<Receipt>,
    #[index]
    pub invoice: Id<Invoice>,
    pub amount_cents: i64,
    pub issued_day: u64,
}

#[table]
pub struct Reminder {
    pub id: Id<Reminder>,
    #[index]
    pub invoice: Id<Invoice>,
    pub day: u64,
}

/// Singleton row (fixed id 0): the app's own day counter, advanced only by
/// `AdvanceDay` (jobs.rs), once per daily cron tick.
#[table]
pub struct AppClock {
    pub id: Id<AppClock>,
    pub day: u64,
}

pub const APP_CLOCK_ID: u64 = 0;

/// A monotonic counter row, keyed by an arbitrary `seq` id — one per kind of
/// server-generated id this app needs (line items, reminders). There is no
/// documented id-generation primitive for `#[table]` rows in v0 (see
/// FINDINGS.md); this mirrors the platform's own internal `next_job_id`
/// counter pattern (CLAUDE.md, M0 step 6) rather than bit-packing ids out of
/// a caller-supplied invoice id, which is unbounded and would risk collisions.
#[table]
pub struct IdCounter {
    pub id: Id<IdCounter>,
    pub next: u64,
}

pub const LINE_ITEM_SEQ: u64 = 0;
pub const REMINDER_SEQ: u64 = 1;

pub fn next_id<T>(tx: &mut Tx, seq: u64) -> io::Result<Id<T>> {
    let counter_id = Id::<IdCounter>::from_raw(seq);
    let mut counter = tx.get(counter_id)?.unwrap_or(IdCounter {
        id: counter_id,
        next: 0,
    });
    let value = counter.next;
    counter.next += 1;
    tx.put(&counter);
    Ok(Id::from_raw(value))
}
