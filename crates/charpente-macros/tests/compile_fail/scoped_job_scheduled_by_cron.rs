// A cron tick has no tenant (job-tenancy spec §5): `App::schedule` enqueues
// on a bare `Tx`, so it takes `global` jobs only, and a scoped one meets the
// same `NoJobTenant` note, at the schedule.
use charpente::auth::Principal;
use charpente::core::io::{Disk, Entropy};
use charpente::http::{ApiError, App, AppConfig};
use charpente::kernel::JobError;
use charpente::{Id, Schema, ScopedTx, Tenant, Tx, job, table};
use std::time::Duration;

#[table(global)]
struct Org {
    id: Id<Org>,
    name: String,
}

impl Tenant for Org {
    const NAME: &'static str = "Org";
    type Who = Principal;
    type Error = ApiError;
    fn authorize(_tx: &Tx, _who: &Principal, _scope: Id<Org>) -> Result<(), ApiError> {
        Ok(())
    }
}

#[job(scoped_by = Org)]
struct MaintenanceSweep {}

impl MaintenanceSweep {
    fn run(self, _tx: &mut ScopedTx<'_, '_, Org>) -> Result<(), JobError> {
        Ok(())
    }
}

fn build(disk: Box<dyn Disk>, entropy: Box<dyn Entropy>) -> std::io::Result<App> {
    Ok(App::open(disk, Schema::new(), entropy, AppConfig::default())?
        .schedule("maintenance-sweep", Duration::from_secs(3600), || {
            MaintenanceSweep {}
        }))
}

fn main() {
    let _ = build;
}
