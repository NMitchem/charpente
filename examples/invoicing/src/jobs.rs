//! Jobs and events. Every job's `run` is its own transaction (GUIDE §6):
//! its effects and its Done-mark commit atomically, so "exactly-once" here
//! is structural, not something this file has to implement by hand.

use crate::model::{
    APP_CLOCK_ID, AppClock, Invoice, InvoiceState, LineItem, REMINDER_SEQ, Receipt, Reminder,
    next_id,
};
use charpente::kernel::JobError;
use charpente::{Id, Tx, View, event, job};

/// Enqueued once, in the same transaction as `finalize_invoice` (routes.rs).
/// "Sending" a receipt means writing a `Receipt` row and emitting an event —
/// there is no external IO in this app (brief). `org` rides along because a
/// job has no `Ctx`/session of its own (GUIDE §6) — `finalize_invoice`
/// carries the caller's org key through so `ReceiptSent` can be emitted
/// keyed to the same org `InvoiceFinalized` was.
#[job]
pub struct SendReceipt {
    pub invoice: Id<Invoice>,
    pub org: String,
}

impl SendReceipt {
    pub fn run(self, tx: &mut Tx) -> Result<(), JobError> {
        let invoice_id = self.invoice;
        let items = LineItem::by_invoice(tx, &invoice_id)?;
        let amount_cents: i64 = items
            .iter()
            .map(|li| li.unit_amount_cents * li.quantity)
            .sum();
        let day = tx
            .get(Id::<AppClock>::from_raw(APP_CLOCK_ID))?
            .map_or(0, |c| c.day);
        // Receipt.id reuses the invoice's own raw id: exactly one receipt is
        // ever produced per invoice (finalize enqueues SendReceipt exactly
        // once), so this is a collision-free key in Receipt's own id space
        // without needing a counter.
        tx.put(&Receipt {
            id: Id::from_raw(self.invoice.raw()),
            invoice: invoice_id,
            amount_cents,
            issued_day: day,
        });
        tx.emit_keyed(
            self.org,
            ReceiptSent {
                invoice: self.invoice.raw(),
                amount_cents,
            },
        );
        Ok(())
    }
}

#[event]
pub struct ReceiptSent {
    pub invoice: u64,
    pub amount_cents: i64,
}

/// One per overdue Final invoice, enqueued from `AdvanceDay`'s own
/// transaction (below) — this is where "a reminder is a reminders-table
/// row" (brief) actually happens.
#[job]
pub struct SendOverdueReminder {
    pub invoice: u64,
    pub day: u64,
}

impl SendOverdueReminder {
    pub fn run(self, tx: &mut Tx) -> Result<(), JobError> {
        let id = next_id::<Reminder>(tx, REMINDER_SEQ)?;
        tx.put(&Reminder {
            id,
            invoice: Id::from_raw(self.invoice),
            day: self.day,
        });
        Ok(())
    }
}

/// The app's only source of time: ticks once per day (App::schedule in
/// app.rs), advances the singleton `AppClock` row by exactly one day, and —
/// in the SAME transaction — enqueues one `SendOverdueReminder` per Final
/// invoice whose `due_day` has arrived or passed. Cron's own catch-up rule
/// (GUIDE §7: "catch-up fires once, never backfills") means a tick after
/// downtime still only advances the day counter by one, which is exactly
/// the brief's "missed days are not backfilled" requirement — for free.
#[job]
pub struct AdvanceDay {}

impl AdvanceDay {
    pub fn run(self, tx: &mut Tx) -> Result<(), JobError> {
        let clock_id = Id::<AppClock>::from_raw(APP_CLOCK_ID);
        let mut clock = tx.get(clock_id)?.unwrap_or(AppClock {
            id: clock_id,
            day: 0,
        });
        clock.day += 1;
        let today = clock.day;
        tx.put(&clock);

        for inv in tx.scan::<Invoice>()? {
            if inv.state == InvoiceState::Final && today >= inv.due_day {
                tx.enqueue_keyed(
                    format!("reminder:{}:{}", inv.id.raw(), today),
                    SendOverdueReminder {
                        invoice: inv.id.raw(),
                        day: today,
                    },
                );
            }
        }
        Ok(())
    }
}
