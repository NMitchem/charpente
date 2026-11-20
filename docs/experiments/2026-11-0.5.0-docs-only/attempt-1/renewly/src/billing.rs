//! Renewals and give-ups.
//!
//! A subscription's period ends at `period_end`, when `PeriodEnd` (a timed
//! job enqueued for that instant, GUIDE §6 "Due at a time") moves it from
//! active to renewing and enqueues the `Charge`. `Charge` is an outbound job
//! (GUIDE §6 "Outbound jobs"): `call` builds the request to the provider,
//! `settle` starts the next period, and `on_dead` gives the renewal up: the
//! subscription goes past due and its customer gets one notice.
//!
//! Charged at most once per period. The provider dedupes on the
//! `Idempotency-Key` header, and the key renewly sends names the effect, not
//! the job: one key per (workspace, subscription, period). The original
//! charge, its retries, and an owner's retry after a give-up (a new job, so a
//! new platform key) all send that one key with the same body, so the
//! provider applies the period's charge once (GUIDE §6: "its `call` must
//! send an id of the effect itself").

use crate::state::{SubState, Transition, step};
use crate::{Counter, NOTICE_COUNTER, Notice, Plan, Subscription, Workspace};
use charpente::core::io::WallTime;
use charpente::kernel::JobError;
use charpente::outbound::{Call, IdempotencyKey, Reply};
use charpente::{Dead, Id, ScopedTx, job};
use std::io;

/// The payment provider's host.
pub const BILLING_HOST: &str = "billing.example.net";

/// One day of wall time, in nanoseconds.
pub const DAY_NANOS: u64 = 86_400 * 1_000_000_000;

/// The end of a period that starts at `start` and lasts `days`.
pub fn period_end(start: u64, days: u32) -> u64 {
    start.saturating_add(u64::from(days).saturating_mul(DAY_NANOS))
}

/// The provider idempotency key of one period's renewal charge.
pub fn effect_key(ws: Id<Workspace>, subscription: u64, seq: u32) -> String {
    format!("renewly-{}-{}-{}", ws.raw(), subscription, seq)
}

/// Ends period `seq` of a subscription: an active one goes renewing and its
/// charge is enqueued. A cancelled one, or one that already moved past
/// `seq`, is left alone, so a cancelled subscription is never charged.
#[job(scoped_by = Workspace, retain_dead)]
pub struct PeriodEnd {
    pub subscription: u64,
    pub seq: u32,
}

impl PeriodEnd {
    pub fn run(self, tx: &mut ScopedTx<'_, '_, Workspace>) -> Result<(), JobError> {
        let Some(mut sub) = tx.get(Id::<Subscription>::from_raw(self.subscription))? else {
            return Ok(());
        };
        if sub.period_seq != self.seq {
            return Ok(());
        }
        let Ok(next) = step(sub.state, Transition::Renew) else {
            return Ok(());
        };
        sub.state = next;
        tx.put(&sub);
        tx.enqueue(Charge {
            subscription: self.subscription,
            seq: self.seq,
        });
        Ok(())
    }
}

/// The renewal charge for the period after period `seq`.
#[job(
    scoped_by = Workspace,
    on_dead,
    outbound = "billing.example.net",
    retry(max = 5, backoff = "30s"),
    timeout = "30s"
)]
pub struct Charge {
    pub subscription: u64,
    pub seq: u32,
}

/// The subscription and its plan, when it is still renewing period `seq`.
fn renewing(
    tx: &ScopedTx<'_, '_, Workspace>,
    subscription: u64,
    seq: u32,
) -> io::Result<Option<(Subscription, Plan)>> {
    let Some(sub) = tx.get(Id::<Subscription>::from_raw(subscription))? else {
        return Ok(None);
    };
    if sub.state != SubState::Renewing || sub.period_seq != seq {
        return Ok(None);
    }
    let plan = tx
        .get(sub.plan)?
        .ok_or_else(|| io::Error::other(format!("subscription {subscription} names a missing plan")))?;
    Ok(Some((sub, plan)))
}

impl Charge {
    /// The charge request, or none when the subscription no longer waits on
    /// this period's charge. The body is the same on every send for one
    /// period; `x-renewly-sent-at` is when this send was decided, which the
    /// provider does not compare.
    pub fn call(
        &self,
        tx: &ScopedTx<'_, '_, Workspace>,
        _key: &IdempotencyKey,
    ) -> Result<Option<Call>, JobError> {
        let Some((sub, plan)) = renewing(tx, self.subscription, self.seq)? else {
            return Ok(None);
        };
        let ws = tx.scope();
        Ok(Some(
            Call::post("/v1/charges")
                .header("idempotency-key", effect_key(ws, self.subscription, self.seq))
                .header("x-renewly-sent-at", tx.now().as_nanos().to_string())
                .form(&[
                    ("workspace", ws.raw().to_string()),
                    ("subscription", self.subscription.to_string()),
                    ("seq", self.seq.to_string()),
                    ("customer", sub.customer.raw().to_string()),
                    ("amount_cents", plan.price_cents.to_string()),
                    ("period_end", sub.period_end.to_string()),
                ]),
        ))
    }

    /// A 2xx settles the renewal: the next period starts where the old one
    /// ended, and its own end is scheduled. Anything else is an attempt.
    pub fn settle(self, tx: &mut ScopedTx<'_, '_, Workspace>, reply: Reply) -> Result<(), JobError> {
        let status = reply.status();
        if !(200..300).contains(&status) {
            return Err(JobError::app(format!("{BILLING_HOST} answered {status}")));
        }
        let Some((mut sub, plan)) = renewing(tx, self.subscription, self.seq)? else {
            return Ok(());
        };
        sub.state = step(sub.state, Transition::Settle).map_err(JobError::app)?;
        sub.period_seq = self.seq + 1;
        sub.period_start = sub.period_end;
        sub.period_end = period_end(sub.period_start, plan.period_days);
        tx.put(&sub);
        tx.enqueue_at(
            WallTime::from_nanos(sub.period_end),
            PeriodEnd {
                subscription: self.subscription,
                seq: sub.period_seq,
            },
        );
        Ok(())
    }

    /// The renewal is given up: past due, and one notice for the customer,
    /// committed with the Dead mark as one record.
    pub fn on_dead(self, tx: &mut ScopedTx<'_, '_, Workspace>, dead: Dead<Self>) -> Result<(), JobError> {
        let Some((mut sub, plan)) = renewing(tx, self.subscription, self.seq)? else {
            return Ok(());
        };
        sub.state = step(sub.state, Transition::GiveUp).map_err(JobError::app)?;
        tx.put(&sub);
        let counter_id = Id::<Counter>::from_raw(NOTICE_COUNTER);
        let mut counter = tx.get(counter_id)?.unwrap_or(Counter {
            id: counter_id,
            next: 1,
        });
        let id = Id::<Notice>::from_raw(counter.next);
        counter.next += 1;
        tx.put(&counter);
        let failed_at = tx.now().as_nanos();
        tx.put(&Notice {
            id,
            customer: sub.customer,
            subscription: sub.id,
            seq: self.seq,
            plan_name: plan.name.clone(),
            failed_at,
            message: format!(
                "the renewal of your {} plan failed at {} (unix nanoseconds) after {} attempts; your subscription is past due",
                plan.name,
                failed_at,
                dead.attempts()
            ),
        });
        Ok(())
    }
}
