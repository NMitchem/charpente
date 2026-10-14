//! The app the edge harness drives over real TLS (outbound-effects spec §12
//! oracle 7, plan amendment A13): an order is created by a route, charged by
//! an outbound job, and marked paid by `settle`. Kept out of sim-fixture and
//! invoicing so their recorded batches and goldens never move (oracle 3).

use std::io;

use charpente::core::io::{Disk, Entropy};
use charpente::http::{ApiError, App, AppConfig};
use charpente::kernel::JobError;
use charpente::outbound::{Call, Host, IdempotencyKey, Reply};
use charpente::{Id, Schema, Tx, View, Wire, job, jobs, route, routes, table};
use serde::{Deserialize, Serialize};

#[table(global)]
pub struct Order {
    pub id: Id<Order>,
    pub paid: bool,
    pub charge: String,
}

/// The order id space's counter row (GUIDE §2, "Generating ids").
#[table(global)]
pub struct IdCounter {
    pub id: Id<IdCounter>,
    pub next: u64,
}

#[job(
    global,
    outbound = "pay.fixture",
    retry(max = 8, backoff = "500ms"),
    timeout = "2s"
)]
pub struct ChargeOrder {
    pub order: Id<Order>,
}

impl ChargeOrder {
    fn call(&self, tx: &Tx, key: &IdempotencyKey) -> Result<Option<Call>, JobError> {
        let Some(order) = tx.get(self.order)? else {
            return Err(JobError::app("order gone"));
        };
        if order.paid {
            return Ok(None);
        }
        Ok(Some(
            Call::post("/charge")
                .header("idempotency-key", key.as_str())
                .form(&[("order", self.order.raw().to_string())]),
        ))
    }

    fn settle(self, tx: &mut Tx, reply: Reply) -> Result<(), JobError> {
        if reply.status() != 200 {
            return Err(JobError::app(format!(
                "pay.fixture answered {}",
                reply.status()
            )));
        }
        let charge = String::from_utf8_lossy(reply.body()).into_owned();
        tx.put(&Order {
            id: self.order,
            paid: true,
            charge,
        });
        Ok(())
    }
}

/// `GET /orders/:id`'s answer.
#[derive(Wire, Serialize, Deserialize)]
pub struct OrderView {
    pub paid: bool,
    pub charge: String,
}

/// Puts an unpaid order and enqueues its charge in one transaction, so the
/// order and the job that charges it are durable together or not at all.
#[route(POST "/orders", public)]
pub fn create_order(tx: &mut Tx) -> Result<Id<Order>, ApiError> {
    let counter_id = Id::<IdCounter>::from_raw(0);
    let mut counter = tx.get(counter_id)?.unwrap_or(IdCounter {
        id: counter_id,
        next: 0,
    });
    let id = Id::from_raw(counter.next);
    counter.next += 1;
    tx.put(&counter);
    tx.put(&Order {
        id,
        paid: false,
        charge: String::new(),
    });
    tx.enqueue(ChargeOrder { order: id });
    Ok(id)
}

#[route(GET "/orders/:id", public)]
pub fn get_order(tx: &mut Tx, id: Id<Order>) -> Result<OrderView, ApiError> {
    let order = tx.get(id)?.ok_or(ApiError::NotFound)?;
    Ok(OrderView {
        paid: order.paid,
        charge: order.charge,
    })
}

pub fn build(disk: Box<dyn Disk>, entropy: Box<dyn Entropy>) -> io::Result<App> {
    Ok(App::open(
        disk,
        Schema::new().table::<Order>().table::<IdCounter>(),
        entropy,
        AppConfig::default(),
    )?
    .mount(routes![create_order, get_order])
    .mount_jobs(jobs![ChargeOrder])
    .outbound(Host::new("pay.fixture").bearer_env("FIXTURE_PAY_KEY")))
}
