// The declared fan-out (job-tenancy spec §2 R5): a cron-scheduled
// `global, cross_tenant` sweep reads every tenant and enqueues a scoped job
// into each tenant it names, and so does a `cross_tenant` route.
use charpente::auth::Principal;
use charpente::core::io::{Disk, Entropy};
use charpente::http::{ApiError, App, AppConfig};
use charpente::kernel::JobError;
use charpente::{CrossTenant, Id, Schema, ScopedTx, Tenant, Tx, job, jobs, route, routes, table};
use std::time::Duration;

#[table(global)]
struct Org {
    id: Id<Org>,
    name: String,
}

impl Tenant for Org {
    type Who = Principal;
    type Error = ApiError;
    fn authorize(_tx: &Tx, _who: &Principal, _scope: Id<Org>) -> Result<(), ApiError> {
        Ok(())
    }
}

#[table(scoped_by = Org)]
struct Window {
    id: Id<Window>,
    applied: bool,
}

#[job(scoped_by = Org)]
struct ApplyWindow {
    window: u64,
}

impl ApplyWindow {
    fn run(self, tx: &mut ScopedTx<'_, '_, Org>) -> Result<(), JobError> {
        if let Some(mut w) = tx.get(Id::<Window>::from_raw(self.window))? {
            w.applied = true;
            tx.put(&w);
        }
        Ok(())
    }
}

#[job(cross_tenant, global)]
struct MaintenanceSweep {}

impl MaintenanceSweep {
    fn run(self, tx: &mut Tx, cap: &CrossTenant) -> Result<(), JobError> {
        let due = tx.across_tenants::<Org>(cap).scan::<Window>()?;
        for (org, window) in due {
            if !window.applied {
                tx.enqueue_in(org, ApplyWindow { window: window.id.raw() }, cap);
            }
        }
        Ok(())
    }
}

#[route(POST "/admin/sweep", auth, cross_tenant)]
fn sweep_now(tx: &mut Tx, cap: &CrossTenant) -> Result<(), ApiError> {
    let windows = tx.across_tenants::<Org>(cap).scan::<Window>()?;
    for (org, window) in windows {
        tx.enqueue_in(org, ApplyWindow { window: window.id.raw() }, cap);
    }
    Ok(())
}

fn build(disk: Box<dyn Disk>, entropy: Box<dyn Entropy>) -> std::io::Result<App> {
    Ok(App::open(disk, Schema::new().table::<Window>(), entropy, AppConfig::default())?
        .mount(routes![sweep_now])
        .mount_jobs(jobs![ApplyWindow, MaintenanceSweep])
        .schedule("maintenance-sweep", Duration::from_secs(3600), || {
            MaintenanceSweep {}
        }))
}

fn main() {
    let _ = build;
}
