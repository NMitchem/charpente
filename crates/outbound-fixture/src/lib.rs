//! The app the edge harness drives over real TLS (outbound-effects spec §12
//! oracle 7, plan amendment A13): an order is created by a route, charged by
//! an outbound job, and marked paid by `settle`. Kept out of sim-fixture and
//! invoicing so their recorded batches and goldens never move (oracle 3).
//! `POST /receipted-orders` also sends the order's receipt, 2 s later, on the
//! default retry policy, which the sim drives to Dead (0.5.0 spec §7.3 a);
//! the edge harness posts `/orders` only, so it never sees a receipt call.

use std::io;
use std::time::Duration;

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
    retain_dead,
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

/// The order's receipt, through the same provider on the default retry
/// policy: five attempts backing off 30 s doubling (450 s in all), each with
/// the default 30 s timeout. A provider outage that outlasts them takes it to
/// Dead, which is what the sim requires to happen.
#[job(global, retain_dead, outbound = "pay.fixture")]
pub struct SendReceipt {
    pub order: Id<Order>,
}

impl SendReceipt {
    fn call(&self, _tx: &Tx, key: &IdempotencyKey) -> Result<Option<Call>, JobError> {
        Ok(Some(
            Call::post("/receipt")
                .header("idempotency-key", key.as_str())
                .form(&[("order", self.order.raw().to_string())]),
        ))
    }

    fn settle(self, _tx: &mut Tx, reply: Reply) -> Result<(), JobError> {
        match reply.status() {
            200 => Ok(()),
            s => Err(JobError::app(format!("pay.fixture answered {s}"))),
        }
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
    place_order(tx)
}

/// `POST /orders`, with the order's receipt enqueued in the same transaction,
/// due 2 s later. The sim's outage axis opens its window in the first two
/// seconds of virtual time, and the sim places its orders at once, so a
/// receipt sent at once made its first call before the window opened in 211
/// of the 247 universes that drew one (1000 runs, master seed
/// 0xae8b2f56ca4150bc), and was answered there unless another fault failed
/// it. Due 2 s later, its first call lands inside the window in 240 of 246.
#[route(POST "/receipted-orders", public)]
pub fn create_receipted_order(tx: &mut Tx) -> Result<Id<Order>, ApiError> {
    let id = place_order(tx)?;
    tx.enqueue_after(Duration::from_secs(2), SendReceipt { order: id });
    Ok(id)
}

fn place_order(tx: &mut Tx) -> Result<Id<Order>, ApiError> {
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
    .mount(routes![create_order, create_receipted_order, get_order])
    .mount_jobs(jobs![ChargeOrder, SendReceipt])
    .outbound(Host::new("pay.fixture").bearer_env("FIXTURE_PAY_KEY")))
}
