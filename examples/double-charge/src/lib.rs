//! An order API: one table, one route, one outbound job that charges the
//! order at `pay.example`. `tests/sim.rs` is the property that says each
//! order is charged once.

use charpente::core::io::{Disk, Entropy};
use charpente::http::{ApiError, App, AppConfig};
use charpente::kernel::JobError;
use charpente::outbound::{Call, Host, IdempotencyKey, Reply};
use charpente::{Id, Schema, Tx, job, jobs, route, routes, table};
use std::io;

#[table(global)]
pub struct Order {
    pub id: Id<Order>,
    pub paid: bool,
}

// retain_dead: this app only shows the remote; an app that must tell someone a charge
// was given up declares `on_dead` (GUIDE §6).
#[job(
    global,
    retain_dead,
    outbound = "pay.example",
    retry(max = 8, backoff = "500ms"),
    timeout = "2s"
)]
pub struct Charge {
    pub order: Id<Order>,
}

impl Charge {
    fn call(&self, _tx: &Tx, key: &IdempotencyKey) -> Result<Option<Call>, JobError> {
        Ok(Some(
            Call::post("/charge")
                .header("idempotency-key", key.as_str())
                .form(&[("order", self.order.raw().to_string())]),
        ))
    }

    fn settle(self, tx: &mut Tx, reply: Reply) -> Result<(), JobError> {
        match reply.status() {
            200 => {
                tx.put(&Order {
                    id: self.order,
                    paid: true,
                });
                Ok(())
            }
            s => Err(JobError::app(format!("pay.example answered {s}"))),
        }
    }
}

/// Puts the order and enqueues its charge in one transaction.
#[route(POST "/orders/:id", public)]
pub fn place_order(tx: &mut Tx, id: Id<Order>) -> Result<Id<Order>, ApiError> {
    tx.put(&Order { id, paid: false });
    tx.enqueue(Charge { order: id });
    Ok(id)
}

pub fn build_app(disk: Box<dyn Disk>, entropy: Box<dyn Entropy>) -> io::Result<App> {
    Ok(App::open(
        disk,
        Schema::new().table::<Order>(),
        entropy,
        AppConfig::default(),
    )?
    .mount(routes![place_order])
    .mount_jobs(jobs![Charge])
    .outbound(Host::new("pay.example")))
}
