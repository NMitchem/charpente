// A capability token lives no longer than the `Db` borrow of the `Group`
// that minted it (tenancy-completion spec §5), so operator code holding a
// `Db` cannot park one past its group: not in a `static`, not in a
// `thread_local!`, both of which need `'static`. The same holds for the
// `IntentAccess` the request path mints to reach the intents record
// (retry-safe-writes spec §4.2). The types are spelled with
// no lifetime, as an app would write them; a static's elided lifetime is
// `'static`. One function per case: borrowck reports one error per region
// that must outlive `'static`, so two cases in one function would be one.
use std::cell::RefCell;
use std::sync::OnceLock;

use charpente::core::db::IntentAccess;
use charpente::{CrossTenant, Db, TenantAdmission};

static PARKED_CAP: OnceLock<CrossTenant> = OnceLock::new();
static PARKED_ADMISSION: OnceLock<TenantAdmission> = OnceLock::new();
static PARKED_INTENTS: OnceLock<IntentAccess> = OnceLock::new();

thread_local! {
    static HELD_CAP: RefCell<Option<CrossTenant>> = const { RefCell::new(None) };
    static HELD_ADMISSION: RefCell<Option<TenantAdmission>> = const { RefCell::new(None) };
    static HELD_INTENTS: RefCell<Option<IntentAccess>> = const { RefCell::new(None) };
}

fn park_the_capability(db: &mut Db) -> std::io::Result<()> {
    let group = db.begin_group()?;
    let _ = PARKED_CAP.set(group.cross_tenant_cap());
    group.commit().map_err(|failure| failure.error)
}

fn park_the_admission(db: &mut Db) -> std::io::Result<()> {
    let group = db.begin_group()?;
    let _ = PARKED_ADMISSION.set(group.tenant_admission());
    group.commit().map_err(|failure| failure.error)
}

fn hold_the_capability(db: &mut Db) -> std::io::Result<()> {
    let group = db.begin_group()?;
    HELD_CAP.with(|held| *held.borrow_mut() = Some(group.cross_tenant_cap()));
    group.commit().map_err(|failure| failure.error)
}

fn hold_the_admission(db: &mut Db) -> std::io::Result<()> {
    let group = db.begin_group()?;
    HELD_ADMISSION.with(|held| *held.borrow_mut() = Some(group.tenant_admission()));
    group.commit().map_err(|failure| failure.error)
}

fn park_the_intent_access(db: &mut Db) -> std::io::Result<()> {
    let group = db.begin_group()?;
    let _ = PARKED_INTENTS.set(group.intent_access());
    group.commit().map_err(|failure| failure.error)
}

fn hold_the_intent_access(db: &mut Db) -> std::io::Result<()> {
    let group = db.begin_group()?;
    HELD_INTENTS.with(|held| *held.borrow_mut() = Some(group.intent_access()));
    group.commit().map_err(|failure| failure.error)
}

fn main() {
    let _ = (
        park_the_capability,
        park_the_admission,
        hold_the_capability,
        hold_the_admission,
        park_the_intent_access,
        hold_the_intent_access,
    );
}
