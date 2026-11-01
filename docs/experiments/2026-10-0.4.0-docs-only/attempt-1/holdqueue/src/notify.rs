//! Telling a patron their hold is ready, through the outside messaging
//! service at `notify.example.org`.
//!
//! The job is enqueued in the transaction that makes the hold ready (see
//! `queue::serve`), so a hold is ready exactly when its notification is
//! queued. The kernel sends the message after commit with an idempotency key
//! it minted for this job, the same key on every retry, so a service that
//! honours it delivers one message per ready hold.

use crate::{Hold, Patron};
use charpente::kernel::JobError;
use charpente::outbound::{Call, IdempotencyKey, Reply};
use charpente::{Id, Tx, View, job};
use serde::Serialize;

/// The messaging service host. The `#[job]` attribute below repeats it as a
/// literal, which the macro requires.
pub const NOTIFY_HOST: &str = "notify.example.org";

/// Tells the patron of `hold` that a copy is set aside for them.
#[job(
    global,
    outbound = "notify.example.org",
    retry(max = 12, backoff = "2s"),
    timeout = "10s"
)]
pub struct NotifyReady {
    pub hold: Id<Hold>,
}

/// The message the service delivers, naming the title and the branch.
#[derive(Serialize)]
pub struct ReadyMessage {
    pub patron: Id<Patron>,
    pub hold: Id<Hold>,
    pub title: String,
    pub branch: String,
    pub text: String,
}

impl NotifyReady {
    pub fn call(&self, tx: &Tx, key: &IdempotencyKey) -> Result<Option<Call>, JobError> {
        let Some(hold) = tx.get(self.hold)? else {
            return Ok(None);
        };
        let title = tx
            .get(hold.title)?
            .map_or_else(|| format!("title {}", hold.title.raw()), |t| t.name);
        let branch = tx
            .get(hold.branch)?
            .map_or_else(|| format!("branch {}", hold.branch.raw()), |b| b.name);
        let text = format!("Your hold on {title} is ready for pickup at {branch}.");
        let message = ReadyMessage {
            patron: hold.patron,
            hold: hold.id,
            title,
            branch,
            text,
        };
        Ok(Some(
            Call::post("/messages")
                .header("idempotency-key", key.as_str())
                .json(&message),
        ))
    }

    pub fn settle(self, tx: &mut Tx, reply: Reply) -> Result<(), JobError> {
        let status = reply.status();
        if !(200..300).contains(&status) {
            return Err(JobError::app(format!("{NOTIFY_HOST} answered {status}")));
        }
        if let Some(mut hold) = tx.get(self.hold)? {
            hold.notified = true;
            tx.put(&hold);
        }
        Ok(())
    }
}
