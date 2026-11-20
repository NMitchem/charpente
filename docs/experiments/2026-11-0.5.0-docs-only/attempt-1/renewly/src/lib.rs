//! renewly: workspaces sell plans to their customers, and subscriptions renew
//! by charging an outside payment provider at the end of every period.
//!
//! - `state.rs` holds the subscription states and the one transition table.
//! - `billing.rs` holds the renewal: the job that ends a period and the
//!   outbound charge, with its give-up hook.
//! - This file holds the tables, the tenant (`Workspace`) and the routes.

pub mod billing;
pub mod state;

use charpente::auth::Principal;
use charpente::core::io::{Disk, Entropy, WallTime};
use charpente::http::{ApiError, App, AppConfig, Ctx};
use charpente::outbound::Host;
use charpente::wire::WireU64;
use charpente::{Id, Schema, Scoped, ScopedTx, Tenant, Tx, View, Wire, jobs, route, routes, table};
use serde::{Deserialize, Serialize};
use std::io;

pub use billing::{BILLING_HOST, Charge, DAY_NANOS, PeriodEnd};
pub use state::{SubState, Transition, refusal, step};

/// A tenant. Its owner is the subject of the principal that created it.
#[table(global)]
pub struct Workspace {
    pub id: Id<Workspace>,
    pub owner: String,
    pub name: String,
}

/// A plan a workspace sells. Written once: a plan's price and period never
/// change under a subscription that renews on it.
#[table(scoped_by = Workspace)]
pub struct Plan {
    pub id: Id<Plan>,
    pub name: String,
    pub price_cents: u64,
    pub period_days: u32,
}

#[table(scoped_by = Workspace)]
pub struct Customer {
    pub id: Id<Customer>,
    pub name: String,
    pub email: String,
}

/// A customer's subscription to a plan. `period_seq` counts the periods:
/// the first is 0, and each settled renewal adds one. Times are wall-clock
/// nanoseconds since the Unix epoch, in the node's frame (`tx.now()`).
#[table(scoped_by = Workspace)]
pub struct Subscription {
    pub id: Id<Subscription>,
    #[index]
    pub customer: Id<Customer>,
    pub plan: Id<Plan>,
    pub state: SubState,
    pub period_seq: u32,
    pub period_start: u64,
    pub period_end: u64,
}

/// A customer's cancellation, under an id of its own, so a re-sent request
/// finds it and is answered alike.
#[table(scoped_by = Workspace)]
pub struct Cancellation {
    pub id: Id<Cancellation>,
    pub subscription: Id<Subscription>,
}

/// An owner's retry of a past-due subscription, under an id of its own.
/// `seq` is the period it charges for.
#[table(scoped_by = Workspace)]
pub struct Retry {
    pub id: Id<Retry>,
    pub subscription: Id<Subscription>,
    pub seq: u32,
}

/// What a customer is told when a renewal is given up.
#[table(scoped_by = Workspace)]
pub struct Notice {
    pub id: Id<Notice>,
    #[index]
    pub customer: Id<Customer>,
    pub subscription: Id<Subscription>,
    pub seq: u32,
    pub plan_name: String,
    pub failed_at: u64,
    pub message: String,
}

/// A counter row per id space inside a workspace (GUIDE §2).
#[table(scoped_by = Workspace)]
pub struct Counter {
    pub id: Id<Counter>,
    pub next: u64,
}

/// The counter that numbers notices.
pub const NOTICE_COUNTER: u64 = 1;

/// The subject a customer's credential is minted for.
pub fn customer_subject(ws: Id<Workspace>, customer: Id<Customer>) -> String {
    format!("customer:{}:{}", ws.raw(), customer.raw())
}

/// The customer a subject names inside `ws`, if it names one.
pub fn customer_of(subject: &str, ws: Id<Workspace>) -> Option<Id<Customer>> {
    let rest = subject.strip_prefix(&format!("customer:{}:", ws.raw()))?;
    rest.parse::<u64>().ok().map(Id::from_raw)
}

impl Tenant for Workspace {
    const NAME: &'static str = "Workspace";
    type Who = Principal;
    type Error = ApiError;

    /// The owner, and the customers the owner registered. Anyone else is
    /// refused with one text, whether or not the workspace exists.
    fn authorize(tx: &Tx, who: &Principal, scope: Id<Workspace>) -> Result<(), ApiError> {
        let member = match tx.get(scope)? {
            Some(ws) => ws.owner == who.subject() || customer_of(who.subject(), scope).is_some(),
            None => false,
        };
        if member {
            Ok(())
        } else {
            Err(ApiError::BadRequest(format!(
                "not a member of workspace {}",
                scope.raw()
            )))
        }
    }
}

/// Who the caller is inside a workspace it was admitted to.
enum Role {
    Owner,
    Customer(Id<Customer>),
}

fn role(tx: &ScopedTx<'_, '_, Workspace>, ctx: &Ctx) -> Result<Role, ApiError> {
    let ws = tx.scope();
    let subject = ctx.principal().map(|p| p.subject()).unwrap_or_default();
    let owner = (**tx).get(ws)?.map(|w| w.owner);
    if owner.as_deref() == Some(subject) {
        return Ok(Role::Owner);
    }
    customer_of(subject, ws)
        .map(Role::Customer)
        .ok_or_else(|| ApiError::BadRequest(format!("not a member of workspace {}", ws.raw())))
}

fn owner_only(tx: &ScopedTx<'_, '_, Workspace>, ctx: &Ctx) -> Result<(), ApiError> {
    match role(tx, ctx)? {
        Role::Owner => Ok(()),
        Role::Customer(_) => Err(ApiError::BadRequest(
            "only the workspace's owner may do this".to_string(),
        )),
    }
}

fn customer_only(tx: &ScopedTx<'_, '_, Workspace>, ctx: &Ctx) -> Result<Id<Customer>, ApiError> {
    match role(tx, ctx)? {
        Role::Customer(c) => Ok(c),
        Role::Owner => Err(ApiError::BadRequest(
            "only a customer of the workspace may do this".to_string(),
        )),
    }
}

#[derive(Wire, Serialize, Deserialize)]
pub struct WorkspaceBody {
    pub name: String,
}

#[derive(Wire, Serialize, Deserialize)]
pub struct PlanBody {
    pub name: String,
    pub price_cents: WireU64,
    pub period_days: u32,
}

#[derive(Wire, Serialize, Deserialize)]
pub struct CustomerBody {
    pub name: String,
    pub email: String,
}

#[derive(Wire, Serialize, Deserialize)]
pub struct SubscribeBody {
    pub plan: Id<Plan>,
}

/// The body of a cancellation or a retry: the subscription it moves.
#[derive(Wire, Serialize, Deserialize)]
pub struct TransitionBody {
    pub subscription: Id<Subscription>,
}

#[derive(Wire, Serialize, Deserialize)]
pub struct SubscriptionView {
    pub id: Id<Subscription>,
    pub workspace: Id<Workspace>,
    pub customer: Id<Customer>,
    pub plan: Id<Plan>,
    pub state: String,
    pub period_seq: u32,
    pub period_start: WireU64,
    pub period_end: WireU64,
}

impl SubscriptionView {
    fn of(ws: Id<Workspace>, s: &Subscription) -> Self {
        SubscriptionView {
            id: s.id,
            workspace: ws,
            customer: s.customer,
            plan: s.plan,
            state: s.state.name().to_string(),
            period_seq: s.period_seq,
            period_start: WireU64(s.period_start),
            period_end: WireU64(s.period_end),
        }
    }
}

/// The answer to an accepted cancellation or retry, the same on a repeat.
#[derive(Wire, Serialize, Deserialize)]
pub struct TransitionView {
    pub id: WireU64,
    pub subscription: Id<Subscription>,
    pub transition: String,
}

#[derive(Wire, Serialize, Deserialize)]
pub struct NoticeView {
    pub id: Id<Notice>,
    pub subscription: Id<Subscription>,
    pub plan: String,
    pub failed_at: WireU64,
    pub message: String,
}

/// The subject of a workspace's owner.
pub fn owner_subject(ws: Id<Workspace>) -> String {
    format!("owner:{}", ws.raw())
}

/// Signs a new workspace up and answers its owner's first credential: the
/// one way in, minted by the app (GUIDE §5: "a signup route that mints a key
/// for a subject it has just created"). Each id is signed up once; a
/// second signup of a taken id is refused and mints nothing.
#[route(POST "/signup/:ws", public)]
fn signup(tx: &mut Tx, ws: Id<Workspace>, body: WorkspaceBody, ctx: &Ctx) -> Result<String, ApiError> {
    if tx.get(ws)?.is_some() {
        return Err(ApiError::BadRequest(format!(
            "workspace {} already exists — sign up under another id",
            ws.raw()
        )));
    }
    tx.put(&Workspace {
        id: ws,
        owner: owner_subject(ws),
        name: body.name,
    });
    Ok(ctx.create_api_key(tx, owner_subject(ws)))
}

/// The owner adds a plan. Written once: the same plan again is answered
/// alike, another plan under the id is refused.
#[route(PUT "/workspaces/:ws/plans/:id", auth)]
fn put_plan(tx: &mut Tx, ws: Id<Workspace>, id: Id<Plan>, body: PlanBody, ctx: &Ctx) -> Result<Id<Plan>, ApiError> {
    let mut tx = tx.scope::<Workspace>(ws, ctx)?;
    owner_only(&tx, ctx)?;
    if body.period_days == 0 {
        return Err(ApiError::BadRequest(
            "a plan's period is a whole number of days, at least 1".to_string(),
        ));
    }
    if let Some(p) = tx.get(id)? {
        if p.name == body.name && p.price_cents == body.price_cents.0 && p.period_days == body.period_days {
            return Ok(id);
        }
        return Err(ApiError::BadRequest(format!(
            "plan {} already exists with other terms — a plan is written once; add the new terms under a new id",
            id.raw()
        )));
    }
    tx.put(&Plan {
        id,
        name: body.name,
        price_cents: body.price_cents.0,
        period_days: body.period_days,
    });
    Ok(id)
}

/// The owner registers a customer. Written once, like a plan.
#[route(PUT "/workspaces/:ws/customers/:id", auth)]
fn put_customer(tx: &mut Tx, ws: Id<Workspace>, id: Id<Customer>, body: CustomerBody, ctx: &Ctx) -> Result<Id<Customer>, ApiError> {
    let mut tx = tx.scope::<Workspace>(ws, ctx)?;
    owner_only(&tx, ctx)?;
    if let Some(c) = tx.get(id)? {
        if c.name == body.name && c.email == body.email {
            return Ok(id);
        }
        return Err(ApiError::BadRequest(format!(
            "customer {} already exists with other details",
            id.raw()
        )));
    }
    tx.put(&Customer {
        id,
        name: body.name,
        email: body.email,
    });
    Ok(id)
}

/// The owner issues a credential for one of its customers, to hand over.
/// Each call is a new one (one credential mint per request, GUIDE §5).
#[route(POST "/workspaces/:ws/customers/:id/keys", auth)]
fn issue_customer_key(tx: &mut Tx, ws: Id<Workspace>, id: Id<Customer>, ctx: &Ctx) -> Result<String, ApiError> {
    {
        let stx = tx.scope::<Workspace>(ws, ctx)?;
        owner_only(&stx, ctx)?;
        stx.get(id)?.ok_or(ApiError::NotFound)?;
    }
    Ok(ctx.create_api_key(tx, customer_subject(ws, id)))
}

/// A customer subscribes to a plan. The first period starts now and ends
/// the plan's period later, when its renewal is due.
#[route(PUT "/workspaces/:ws/subscriptions/:id", auth)]
fn subscribe(tx: &mut Tx, ws: Id<Workspace>, id: Id<Subscription>, body: SubscribeBody, ctx: &Ctx) -> Result<SubscriptionView, ApiError> {
    let mut tx = tx.scope::<Workspace>(ws, ctx)?;
    let customer = customer_only(&tx, ctx)?;
    if let Some(s) = tx.get(id)? {
        if s.customer == customer && s.plan == body.plan {
            return Ok(SubscriptionView::of(ws, &s));
        }
        return Err(ApiError::BadRequest(format!(
            "subscription {} already exists — subscribe under a new id",
            id.raw()
        )));
    }
    tx.get(customer)?.ok_or_else(|| {
        ApiError::BadRequest(format!("customer {} is not registered", customer.raw()))
    })?;
    let plan = tx.get(body.plan)?.ok_or_else(|| {
        ApiError::BadRequest(format!("no plan {} in this workspace", body.plan.raw()))
    })?;
    let start = tx.now().as_nanos();
    let sub = Subscription {
        id,
        customer,
        plan: body.plan,
        state: SubState::Active,
        period_seq: 0,
        period_start: start,
        period_end: billing::period_end(start, plan.period_days),
    };
    tx.put(&sub);
    tx.enqueue_at(
        WallTime::from_nanos(sub.period_end),
        PeriodEnd {
            subscription: id.raw(),
            seq: 0,
        },
    );
    Ok(SubscriptionView::of(ws, &sub))
}

/// A customer reads their own subscription; the owner reads any.
#[route(GET "/workspaces/:ws/subscriptions/:id", auth)]
fn get_subscription(tx: &mut Tx, ws: Id<Workspace>, id: Id<Subscription>, ctx: &Ctx) -> Result<SubscriptionView, ApiError> {
    let tx = tx.scope::<Workspace>(ws, ctx)?;
    let s = tx.get(id)?.ok_or(ApiError::NotFound)?;
    match role(&tx, ctx)? {
        Role::Customer(c) if c != s.customer => Err(ApiError::NotFound),
        _ => Ok(SubscriptionView::of(ws, &s)),
    }
}

/// The owner lists the workspace's subscriptions, with states and period ends.
#[route(GET "/workspaces/:ws/subscriptions", auth)]
fn list_subscriptions(tx: &mut Tx, ws: Id<Workspace>, ctx: &Ctx) -> Result<Vec<SubscriptionView>, ApiError> {
    let tx = tx.scope::<Workspace>(ws, ctx)?;
    owner_only(&tx, ctx)?;
    Ok(tx
        .scan::<Subscription>()?
        .iter()
        .map(|s| SubscriptionView::of(ws, s))
        .collect())
}

/// A customer cancels their own subscription before its period ends.
#[route(PUT "/workspaces/:ws/cancellations/:id", auth)]
fn cancel(tx: &mut Tx, ws: Id<Workspace>, id: Id<Cancellation>, body: TransitionBody, ctx: &Ctx) -> Result<TransitionView, ApiError> {
    let mut tx = tx.scope::<Workspace>(ws, ctx)?;
    let customer = customer_only(&tx, ctx)?;
    let mut sub = tx.get(body.subscription)?.ok_or(ApiError::NotFound)?;
    if sub.customer != customer {
        return Err(ApiError::NotFound);
    }
    let view = TransitionView {
        id: WireU64(id.raw()),
        subscription: body.subscription,
        transition: Transition::Cancel.name().to_string(),
    };
    if let Some(c) = tx.get(id)? {
        if c.subscription == body.subscription {
            return Ok(view);
        }
        return Err(ApiError::BadRequest(format!(
            "cancellation {} already names another subscription — send a new cancellation under a new id",
            id.raw()
        )));
    }
    sub.state = step(sub.state, Transition::Cancel).map_err(ApiError::BadRequest)?;
    tx.put(&sub);
    tx.put(&Cancellation {
        id,
        subscription: body.subscription,
    });
    Ok(view)
}

/// The owner asks for a past-due subscription to be charged again, for the
/// same period, under the same provider idempotency key (billing.rs).
#[route(PUT "/workspaces/:ws/retries/:id", auth)]
fn retry(tx: &mut Tx, ws: Id<Workspace>, id: Id<Retry>, body: TransitionBody, ctx: &Ctx) -> Result<TransitionView, ApiError> {
    let mut tx = tx.scope::<Workspace>(ws, ctx)?;
    owner_only(&tx, ctx)?;
    let mut sub = tx.get(body.subscription)?.ok_or(ApiError::NotFound)?;
    let view = TransitionView {
        id: WireU64(id.raw()),
        subscription: body.subscription,
        transition: Transition::Retry.name().to_string(),
    };
    if let Some(r) = tx.get(id)? {
        if r.subscription == body.subscription {
            return Ok(view);
        }
        return Err(ApiError::BadRequest(format!(
            "retry {} already names another subscription — send a new retry under a new id",
            id.raw()
        )));
    }
    sub.state = step(sub.state, Transition::Retry).map_err(ApiError::BadRequest)?;
    tx.put(&sub);
    tx.put(&Retry {
        id,
        subscription: body.subscription,
        seq: sub.period_seq,
    });
    tx.enqueue(Charge {
        subscription: body.subscription.raw(),
        seq: sub.period_seq,
    });
    Ok(view)
}

/// A customer reads their own notices.
#[route(GET "/workspaces/:ws/notices", auth)]
fn list_notices(tx: &mut Tx, ws: Id<Workspace>, ctx: &Ctx) -> Result<Vec<NoticeView>, ApiError> {
    let tx = tx.scope::<Workspace>(ws, ctx)?;
    let customer = customer_only(&tx, ctx)?;
    Ok(Notice::by_customer(&tx, &customer)?
        .into_iter()
        .map(|n| NoticeView {
            id: n.id,
            subscription: n.subscription,
            plan: n.plan_name,
            failed_at: WireU64(n.failed_at),
            message: n.message,
        })
        .collect())
}

/// The app, in the shape `charpente::launch` and the sim both take.
pub fn build_app(disk: Box<dyn Disk>, entropy: Box<dyn Entropy>) -> io::Result<App> {
    Ok(App::open(
        disk,
        Schema::new()
            .table::<Workspace>()
            .table::<Plan>()
            .table::<Customer>()
            .table::<Subscription>()
            .table::<Cancellation>()
            .table::<Retry>()
            .table::<Notice>()
            .table::<Counter>(),
        entropy,
        AppConfig::default(),
    )?
    .mount(routes![
        signup,
        put_plan,
        put_customer,
        issue_customer_key,
        subscribe,
        get_subscription,
        list_subscriptions,
        cancel,
        retry,
        list_notices
    ])
    .mount_jobs(jobs![PeriodEnd, Charge])
    .outbound(Host::new(BILLING_HOST)))
}
