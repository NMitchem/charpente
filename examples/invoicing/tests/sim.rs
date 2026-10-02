use charpente::View;
use charpente::auth::{ApiKey, hash_token};
use charpente::core::job::{JobRow, JobStatus};
use charpente::core::table::{Codec, Id};
use charpente::http::{App, Cursor, Method, Request};
use charpente::kernel::{Kernel, SimSource};
use charpente::wire::{WireI64, WireU64};
use charpente_sim::checks::{boot_through_fallout, is_fault_fallout, replays_identically};
use charpente_sim::{FaultConfig, Sim};
use invoicing::model::{Customer, Invoice, InvoiceState, Receipt, Reminder};
use invoicing::routes::{CreateCustomerBody, CreateInvoiceBody, LineItemInput};
use std::collections::{BTreeMap, BTreeSet};

const SEC: u64 = 1_000_000_000;
const DAY: u64 = 24 * 3600 * SEC;

charpente_sim::sim_tests! {
    acked_writes_survive_restart require ["restart"],
    app_replays_deterministically,
    receipts_exactly_once require ["kernel:served"],
    overdue_reminders_over_months require ["reminder:sent"],
    illegal_transitions_are_rejected,
    events_feed_resumes_across_restart,
    api_key_authorizes_then_revoked_is_rejected,
}

fn build(sim: &Sim) -> std::io::Result<App> {
    invoicing::build_app(Box::new(sim.disk()), Box::new(sim.entropy()))
}

/// How many boots the app gets, each failed boot followed by a restart, as a
/// supervised process restarts (`checks::boot_through_fallout`).
const BOOT_TRIES: u32 = 8;

/// Runs a login-only phase against `kernel` and returns the bearer token
/// from the recorded response — GUIDE §8's "script a login phase, read the
/// token out of its recorded responses" pattern. `Ok(None)` means the login
/// request itself didn't ack (fault fallout), not a bug.
fn login(kernel: &mut Kernel, sim: &mut Sim) -> Result<Option<String>, String> {
    let mut source = SimSource::new(Box::new(sim.clock()), SEC);
    source.push_at(
        0,
        Request::new(Method::Post, "/login").json(&"alice".to_string()),
    );
    let responses = source.responses();
    if let Err(e) = kernel.run(&mut source)
        && !is_fault_fallout(&e.to_string())
    {
        return Err(e.to_string());
    }
    let responses = responses
        .lock()
        .map_err(|_| "responses lock poisoned".to_string())?;
    let Some((status, body)) = responses.first() else {
        return Ok(None);
    };
    if *status != 200 {
        return Ok(None);
    }
    let token: String = serde_json::from_slice(body).map_err(|e| e.to_string())?;
    Ok(Some(token))
}

fn statuses_of(source: &SimSource) -> Result<Vec<u16>, String> {
    Ok(source
        .responses()
        .lock()
        .map_err(|_| "responses lock poisoned".to_string())?
        .iter()
        .map(|(s, _)| *s)
        .collect())
}

// ---- 1. no acknowledged write is lost under crash injection ---------------

fn acked_writes_survive_restart(sim: &mut Sim) -> Result<(), String> {
    let Some(app) = boot_through_fallout(sim, BOOT_TRIES, is_fault_fallout, || build(sim))
        .map_err(|e| e.to_string())?
    else {
        sim.give_up(format!("the app never booted in {BOOT_TRIES} boots"));
        return Ok(());
    };
    let mut kernel = match Kernel::new(app, Box::new(sim.clock())) {
        Ok(k) => k,
        Err(e) if is_fault_fallout(&e.to_string()) => return Ok(()),
        Err(e) => return Err(e.to_string()),
    };
    let token = login(&mut kernel, sim)?.unwrap_or_default();
    let auth = format!("Bearer {token}");

    let mut source = SimSource::new(Box::new(sim.clock()), 60 * SEC);
    source.push_at(
        0,
        Request::new(Method::Post, "/customers/1")
            .header("authorization", &auth)
            .json(&CreateCustomerBody {
                name: "Acme".into(),
                email: "acme@example.com".into(),
            }),
    );
    source.push_at(
        SEC,
        Request::new(Method::Post, "/invoices/1")
            .header("authorization", &auth)
            .json(&CreateInvoiceBody {
                customer: Id::from_raw(1),
                due_day: WireU64(30),
                line_items: vec![LineItemInput {
                    description: "Widget".into(),
                    unit_amount_cents: WireI64(500),
                    quantity: WireI64(2),
                }],
            }),
    );
    source.push_at(
        2 * SEC,
        Request::new(Method::Post, "/invoices/1/finalize").header("authorization", &auth),
    );
    if let Err(e) = kernel.run(&mut source)
        && !is_fault_fallout(&e.to_string())
    {
        return Err(e.to_string());
    }
    let statuses = statuses_of(&source)?;
    let customer_acked = statuses.first().copied() == Some(200);
    let invoice_acked = statuses.get(1).copied() == Some(200);
    let finalize_acked = statuses.get(2).copied() == Some(200);

    sim.restart(); // power-cycles the simulated disk: unsynced writes tear or vanish
    drop(kernel); // the old handle is dead after restart — reopen, per its own contract

    let Some(app) = boot_through_fallout(sim, BOOT_TRIES, is_fault_fallout, || build(sim))
        .map_err(|e| e.to_string())?
    else {
        sim.give_up(format!("the app never booted in {BOOT_TRIES} boots"));
        return Ok(());
    };
    let snap = match app.db().snapshot() {
        Ok(s) => s,
        Err(e) if is_fault_fallout(&e.to_string()) => return Ok(()),
        Err(e) => return Err(e.to_string()),
    };

    // Acked-but-lost is excused ONLY when the harness actually injected
    // durable corruption in this universe (`bit_flip`/`misdirected_write`
    // on a WAL file — see `checks::durable_corruption_fired`): corruption
    // of an acked record above the checkpoint horizon is silently cut at
    // recovery, by the WAL convention. Excused means the universe gives up:
    // durability was not adjudicated, and a pass needs a verdict. In every
    // universe where no corruption fired, these stay the hard checks they
    // always were.
    let corrupted = charpente_sim::checks::durable_corruption_fired(sim);
    if customer_acked {
        match snap.get::<Customer>(Id::from_raw(1)) {
            Ok(Some(_)) => {}
            Ok(None) if corrupted => {
                sim.cover("invoicing:acked-customer-cut");
                sim.give_up(
                    "durable corruption cut acked customer 1, so durability was not adjudicated",
                );
                return Ok(());
            }
            Ok(None) => return Err("acked customer 1 missing after restart".to_string()),
            Err(e) if is_fault_fallout(&e.to_string()) => return Ok(()),
            Err(e) => return Err(e.to_string()),
        }
    }
    if invoice_acked {
        match snap.get::<Invoice>(Id::from_raw(1)) {
            Ok(Some(_)) => {}
            Ok(None) if corrupted => {
                sim.cover("invoicing:acked-invoice-cut");
                sim.give_up(
                    "durable corruption cut acked invoice 1, so durability was not adjudicated",
                );
                return Ok(());
            }
            Ok(None) => return Err("acked invoice 1 missing after restart".to_string()),
            Err(e) if is_fault_fallout(&e.to_string()) => return Ok(()),
            Err(e) => return Err(e.to_string()),
        }
    }
    if finalize_acked {
        match snap.get::<Invoice>(Id::from_raw(1)) {
            Ok(Some(inv))
                if inv.state == InvoiceState::Final || inv.state == InvoiceState::Paid => {}
            Ok(Some(_)) | Ok(None) if corrupted => {
                sim.cover("invoicing:acked-finalize-cut");
                sim.give_up(
                    "durable corruption cut invoice 1's acked finalize, so durability was not \
                     adjudicated",
                );
                return Ok(());
            }
            Ok(Some(inv)) => {
                return Err(format!(
                    "acked finalize but invoice 1 is {:?} after restart",
                    inv.state
                ));
            }
            Ok(None) => {
                return Err("acked finalize but invoice 1 missing after restart".to_string());
            }
            Err(e) if is_fault_fallout(&e.to_string()) => return Ok(()),
            Err(e) => return Err(e.to_string()),
        }
    }
    Ok(())
}

// ---- 2. the app replays deterministically ----------------------------------

fn app_replays_deterministically(sim: &mut Sim) -> Result<(), String> {
    replays_identically(sim.seed(), scripted_flow)
}

fn scripted_flow(sim: &mut Sim) -> Result<Vec<u16>, String> {
    let Some(app) = boot_through_fallout(sim, BOOT_TRIES, is_fault_fallout, || build(sim))
        .map_err(|e| e.to_string())?
    else {
        return Ok(vec![]);
    };
    let mut kernel = match Kernel::new(app, Box::new(sim.clock())) {
        Ok(k) => k,
        Err(e) if is_fault_fallout(&e.to_string()) => return Ok(vec![]),
        Err(e) => return Err(e.to_string()),
    };
    let token = login(&mut kernel, sim)?.unwrap_or_default();
    let auth = format!("Bearer {token}");

    let mut source = SimSource::new(Box::new(sim.clock()), 60 * SEC);
    source.push_at(
        0,
        Request::new(Method::Post, "/customers/1")
            .header("authorization", &auth)
            .json(&CreateCustomerBody {
                name: "Acme".into(),
                email: "acme@example.com".into(),
            }),
    );
    source.push_at(
        SEC,
        Request::new(Method::Post, "/invoices/1")
            .header("authorization", &auth)
            .json(&CreateInvoiceBody {
                customer: Id::from_raw(1),
                due_day: WireU64(5),
                line_items: vec![LineItemInput {
                    description: "Widget".into(),
                    unit_amount_cents: WireI64(500),
                    quantity: WireI64(2),
                }],
            }),
    );
    source.push_at(
        2 * SEC,
        Request::new(Method::Post, "/invoices/1/finalize").header("authorization", &auth),
    );
    if let Err(e) = kernel.run(&mut source)
        && !is_fault_fallout(&e.to_string())
    {
        return Err(e.to_string());
    }
    statuses_of(&source)
}

// ---- 3. receipts match finalized invoices exactly (exactly-once) ----------

fn receipts_exactly_once(sim: &mut Sim) -> Result<(), String> {
    let Some(app) = boot_through_fallout(sim, BOOT_TRIES, is_fault_fallout, || build(sim))
        .map_err(|e| e.to_string())?
    else {
        sim.give_up(format!("the app never booted in {BOOT_TRIES} boots"));
        return Ok(());
    };
    let mut kernel = match Kernel::new(app, Box::new(sim.clock())) {
        Ok(k) => k,
        Err(e) if is_fault_fallout(&e.to_string()) => return Ok(()),
        Err(e) => return Err(e.to_string()),
    };
    let token = login(&mut kernel, sim)?.unwrap_or_default();
    let auth = format!("Bearer {token}");

    const N: u64 = 5;
    let mut source = SimSource::new(Box::new(sim.clock()), 2 * 3600 * SEC + SEC);
    source.push_at(
        0,
        Request::new(Method::Post, "/customers/1")
            .header("authorization", &auth)
            .json(&CreateCustomerBody {
                name: "Acme".into(),
                email: "acme@example.com".into(),
            }),
    );
    for i in 0..N {
        source.push_at(
            (i + 1) * SEC,
            Request::new(Method::Post, format!("/invoices/{i}"))
                .header("authorization", &auth)
                .json(&CreateInvoiceBody {
                    customer: Id::from_raw(1),
                    due_day: WireU64(1000),
                    line_items: vec![LineItemInput {
                        description: "Item".into(),
                        unit_amount_cents: WireI64(100 * (i as i64 + 1)),
                        quantity: WireI64(1),
                    }],
                }),
        );
    }
    for i in 0..N {
        source.push_at(
            (N + i + 1) * SEC,
            Request::new(Method::Post, format!("/invoices/{i}/finalize"))
                .header("authorization", &auth),
        );
    }
    if let Err(e) = kernel.run(&mut source)
        && !is_fault_fallout(&e.to_string())
    {
        return Err(e.to_string());
    }
    // Positional, not "last N": under fault injection the kernel can halt
    // before every request gets a response, so the responses vec may be
    // shorter than 2N+1 — an out-of-bounds index must count as "not acked",
    // not be silently misattributed to the wrong request.
    let statuses = statuses_of(&source)?;
    let finalize_acked = (0..N)
        .filter(|&i| statuses.get((N + 1 + i) as usize).copied() == Some(200))
        .count() as u64;

    if kernel.stats().requests > 0 {
        sim.cover("kernel:served");
    }

    let app = kernel.into_app();
    match check_receipts(&app, finalize_acked) {
        Ok(()) => Ok(()),
        Err(msg) if is_fault_fallout(&msg) => Ok(()),
        Err(msg) => Err(msg),
    }
}

fn check_receipts(app: &App, finalize_acked: u64) -> Result<(), String> {
    let snap = app.db().snapshot().map_err(|e| e.to_string())?;

    let mut done_by_invoice: BTreeMap<u64, u64> = BTreeMap::new();
    let mut durable_send_receipt_jobs = 0u64;
    for job in snap.scan::<JobRow>().map_err(|e| e.to_string())? {
        if job.name != "SendReceipt" {
            continue;
        }
        durable_send_receipt_jobs += 1;
        if job.status == JobStatus::Done {
            let invoice = u64::decode(&mut &job.payload[..]).map_err(|e| e.to_string())?;
            *done_by_invoice.entry(invoice).or_insert(0) += 1;
        }
    }
    if durable_send_receipt_jobs < finalize_acked {
        return Err(format!(
            "ack-durability violated: {finalize_acked} finalize 200s acked but only {durable_send_receipt_jobs} SendReceipt jobs durable"
        ));
    }

    let mut receipts_by_invoice: BTreeMap<u64, u64> = BTreeMap::new();
    for r in snap.scan::<Receipt>().map_err(|e| e.to_string())? {
        *receipts_by_invoice.entry(r.invoice.raw()).or_insert(0) += 1;
    }

    for (invoice, done) in &done_by_invoice {
        if *done > 1 {
            return Err(format!(
                "invoice {invoice}: {done} Done SendReceipt jobs — exactly one expected"
            ));
        }
        let receipts = receipts_by_invoice.get(invoice).copied().unwrap_or(0);
        if receipts != *done {
            return Err(format!(
                "exactly-once violated for invoice {invoice}: {done} Done SendReceipt job(s) vs {receipts} Receipt row(s)"
            ));
        }
    }
    for (invoice, receipts) in &receipts_by_invoice {
        if *receipts > 1 {
            return Err(format!(
                "invoice {invoice}: {receipts} Receipt rows — exactly one expected"
            ));
        }
        if !done_by_invoice.contains_key(invoice) {
            return Err(format!(
                "invoice {invoice}: a Receipt row exists with no Done SendReceipt job"
            ));
        }
    }
    Ok(())
}

// ---- 4. overdue-reminder behavior over simulated months --------------------

fn overdue_reminders_over_months(sim: &mut Sim) -> Result<(), String> {
    let Some(app) = boot_through_fallout(sim, BOOT_TRIES, is_fault_fallout, || build(sim))
        .map_err(|e| e.to_string())?
    else {
        sim.give_up(format!("the app never booted in {BOOT_TRIES} boots"));
        return Ok(());
    };
    let mut kernel = match Kernel::new(app, Box::new(sim.clock())) {
        Ok(k) => k,
        Err(e) if is_fault_fallout(&e.to_string()) => return Ok(()),
        Err(e) => return Err(e.to_string()),
    };
    let token = login(&mut kernel, sim)?.unwrap_or_default();
    let auth = format!("Bearer {token}");

    let due_days = [3u64, 10, 45];
    let mut source = SimSource::new(Box::new(sim.clock()), 90 * DAY);
    source.push_at(
        0,
        Request::new(Method::Post, "/customers/1")
            .header("authorization", &auth)
            .json(&CreateCustomerBody {
                name: "Acme".into(),
                email: "acme@example.com".into(),
            }),
    );
    for (i, due) in due_days.iter().enumerate() {
        source.push_at(
            (i as u64 + 1) * SEC,
            Request::new(Method::Post, format!("/invoices/{i}"))
                .header("authorization", &auth)
                .json(&CreateInvoiceBody {
                    customer: Id::from_raw(1),
                    due_day: WireU64(*due),
                    line_items: vec![LineItemInput {
                        description: "Item".into(),
                        unit_amount_cents: WireI64(1000),
                        quantity: WireI64(1),
                    }],
                }),
        );
    }
    for i in 0..due_days.len() as u64 {
        source.push_at(
            (due_days.len() as u64 + i + 1) * SEC,
            Request::new(Method::Post, format!("/invoices/{i}/finalize"))
                .header("authorization", &auth),
        );
    }
    if let Err(e) = kernel.run(&mut source)
        && !is_fault_fallout(&e.to_string())
    {
        return Err(e.to_string());
    }

    let app = kernel.into_app();
    match check_reminders(&app) {
        Ok(any_fired) => {
            if any_fired {
                sim.cover("reminder:sent");
            }
            Ok(())
        }
        Err(msg) if is_fault_fallout(&msg) => Ok(()),
        Err(msg) => Err(msg),
    }
}

fn check_reminders(app: &App) -> Result<bool, String> {
    let snap = app.db().snapshot().map_err(|e| e.to_string())?;
    let invoices: BTreeMap<u64, Invoice> = snap
        .scan::<Invoice>()
        .map_err(|e| e.to_string())?
        .into_iter()
        .map(|inv| (inv.id.raw(), inv))
        .collect();

    let mut seen: BTreeSet<(u64, u64)> = BTreeSet::new();
    let mut any = false;
    for r in snap.scan::<Reminder>().map_err(|e| e.to_string())? {
        any = true;
        let key = (r.invoice.raw(), r.day);
        if !seen.insert(key) {
            return Err(format!(
                "duplicate reminder for invoice {} on day {}",
                r.invoice.raw(),
                r.day
            ));
        }
        if let Some(inv) = invoices.get(&r.invoice.raw())
            && r.day < inv.due_day
        {
            return Err(format!(
                "reminder for invoice {} fired on day {} before its due day {}",
                r.invoice.raw(),
                r.day,
                inv.due_day
            ));
        }
    }
    Ok(any)
}

// ---- 5. illegal transitions are rejected as app errors, not panics --------

fn illegal_transitions_are_rejected(sim: &mut Sim) -> Result<(), String> {
    let seed = sim.seed();
    let mut fresh = Sim::with_faults(seed, FaultConfig::none());

    let app = invoicing::build_app(Box::new(fresh.disk()), Box::new(fresh.entropy()))
        .map_err(|e| e.to_string())?;
    let mut kernel = Kernel::new(app, Box::new(fresh.clock())).map_err(|e| e.to_string())?;
    let token = login(&mut kernel, &mut fresh)?
        .ok_or_else(|| "login did not ack under FaultConfig::none()".to_string())?;
    let auth = format!("Bearer {token}");

    let mut source = SimSource::new(Box::new(fresh.clock()), 60 * SEC);
    source.push_at(
        0,
        Request::new(Method::Post, "/customers/1")
            .header("authorization", &auth)
            .json(&CreateCustomerBody {
                name: "Acme".into(),
                email: "acme@example.com".into(),
            }),
    );
    source.push_at(
        SEC,
        Request::new(Method::Post, "/invoices/1")
            .header("authorization", &auth)
            .json(&CreateInvoiceBody {
                customer: Id::from_raw(1),
                due_day: WireU64(0),
                line_items: vec![LineItemInput {
                    description: "Item".into(),
                    unit_amount_cents: WireI64(100),
                    quantity: WireI64(1),
                }],
            }),
    );
    // illegal: record-payment before finalize (still Draft)
    source.push_at(
        2 * SEC,
        Request::new(Method::Post, "/invoices/1/record-payment").header("authorization", &auth),
    );
    // legal: Draft -> Final
    source.push_at(
        3 * SEC,
        Request::new(Method::Post, "/invoices/1/finalize").header("authorization", &auth),
    );
    // illegal: already Final
    source.push_at(
        4 * SEC,
        Request::new(Method::Post, "/invoices/1/finalize").header("authorization", &auth),
    );
    // legal: Final -> Paid
    source.push_at(
        5 * SEC,
        Request::new(Method::Post, "/invoices/1/record-payment").header("authorization", &auth),
    );
    // illegal: already Paid
    source.push_at(
        6 * SEC,
        Request::new(Method::Post, "/invoices/1/record-payment").header("authorization", &auth),
    );
    // a plain read, for good measure
    source.push_at(
        7 * SEC,
        Request::new(Method::Get, "/invoices/1").header("authorization", &auth),
    );

    kernel.run(&mut source).map_err(|e| e.to_string())?;

    let statuses = statuses_of(&source)?;
    let expected = [200u16, 200, 400, 200, 400, 200, 400, 200];
    if statuses != expected {
        return Err(format!("expected statuses {expected:?}, got {statuses:?}"));
    }

    let responses = source.responses();
    let responses = responses
        .lock()
        .map_err(|_| "responses lock poisoned".to_string())?;
    let (_, body) = &responses[7];
    let paid_view: serde_json::Value = serde_json::from_slice(body).map_err(|e| e.to_string())?;
    if paid_view["state"] != "paid" {
        return Err(format!(
            "expected final GET to show state=paid, got {paid_view}"
        ));
    }
    // total_cents is WireI64 (task 5): a 64-bit integer is always
    // string-encoded on the wire, so the JSON value is the string "100",
    // not the bare number 100.
    if paid_view["total_cents"] != "100" {
        return Err(format!(
            "expected total_cents=100, got {}",
            paid_view["total_cents"]
        ));
    }
    Ok(())
}

// ---- 6. an org's SSE feed resumes exactly across a crash/restart ----------

/// GUIDE §11's exact-resume property, at the app level: a subscriber to
/// `/events/feed` that reconnects with its last-received cursor after a
/// crash sees every event its org emitted, no gap and no duplicate — even
/// though the whole process (and the live SSE connection with it) died in
/// between. Deterministic like `illegal_transitions_are_rejected`: this is
/// about the resume guarantee itself, not fault-tolerance under injection,
/// so it drives its own fault-free `Sim` from this universe's seed.
fn events_feed_resumes_across_restart(sim: &mut Sim) -> Result<(), String> {
    let seed = sim.seed();
    let mut fresh = Sim::with_faults(seed, FaultConfig::none());

    let app = invoicing::build_app(Box::new(fresh.disk()), Box::new(fresh.entropy()))
        .map_err(|e| e.to_string())?;
    let mut kernel = Kernel::new(app, Box::new(fresh.clock())).map_err(|e| e.to_string())?;
    let token = login(&mut kernel, &mut fresh)?
        .ok_or_else(|| "login did not ack under FaultConfig::none()".to_string())?;
    let auth = format!("Bearer {token}");

    let mut source = SimSource::new(Box::new(fresh.clock()), 10 * SEC);
    source.push_at(
        0,
        Request::new(Method::Get, "/events/feed").header("authorization", &auth),
    );
    source.push_at(
        SEC,
        Request::new(Method::Post, "/customers/1")
            .header("authorization", &auth)
            .json(&CreateCustomerBody {
                name: "Acme".into(),
                email: "acme@example.com".into(),
            }),
    );
    source.push_at(
        2 * SEC,
        Request::new(Method::Post, "/invoices/1")
            .header("authorization", &auth)
            .json(&CreateInvoiceBody {
                customer: Id::from_raw(1),
                due_day: WireU64(30),
                line_items: vec![LineItemInput {
                    description: "Widget".into(),
                    unit_amount_cents: WireI64(500),
                    quantity: WireI64(2),
                }],
            }),
    );
    source.push_at(
        3 * SEC,
        Request::new(Method::Post, "/invoices/1/finalize").header("authorization", &auth),
    );
    let sinks = source.sinks();
    kernel.run(&mut source).map_err(|e| e.to_string())?;

    // `finalize` emits `InvoiceFinalized` directly; its enqueued `SendReceipt`
    // job runs inline on the same kernel loop and emits `ReceiptSent` — both
    // land inside this one `kernel.run` call (no extra virtual time needed).
    let received_before = {
        let handles = sinks
            .lock()
            .map_err(|_| "sink handles lock poisoned".to_string())?;
        if handles.is_empty() {
            return Err("the subscribe never upgraded to SSE".to_string());
        }
        handles[0].frames()
    };
    let names_before: Vec<&str> = received_before.iter().map(|f| f.name).collect();
    if names_before != ["invoice_finalized", "receipt_sent"] {
        return Err(format!(
            "expected [invoice_finalized, receipt_sent] before the restart, got {names_before:?}"
        ));
    }
    let resume_cursor = {
        let last = received_before.last().expect("checked above: len 2");
        Cursor {
            id: last.id,
            seq: last.seq,
        }
        .wire()
    };
    let before_ids: BTreeSet<u64> = received_before.iter().map(|f| f.id).collect();

    fresh.restart(); // power-cycles the simulated disk: the live connection dies with the process
    drop(kernel);

    let app = invoicing::build_app(Box::new(fresh.disk()), Box::new(fresh.entropy()))
        .map_err(|e| e.to_string())?;
    let mut kernel = Kernel::new(app, Box::new(fresh.clock())).map_err(|e| e.to_string())?;

    let mut source = SimSource::new(Box::new(fresh.clock()), 10 * SEC);
    // The bearer token is durable (login acked before anything else ran), so
    // the reconnect needs no fresh login — exactly what a real client gets:
    // its session outlives the crash, and so does its resume cursor.
    source.push_at(
        0,
        Request::new(Method::Get, format!("/events/feed?cursor={resume_cursor}"))
            .header("authorization", &auth),
    );
    source.push_at(
        SEC,
        Request::new(Method::Post, "/invoices/2")
            .header("authorization", &auth)
            .json(&CreateInvoiceBody {
                customer: Id::from_raw(1),
                due_day: WireU64(30),
                line_items: vec![LineItemInput {
                    description: "Gadget".into(),
                    unit_amount_cents: WireI64(1000),
                    quantity: WireI64(1),
                }],
            }),
    );
    source.push_at(
        2 * SEC,
        Request::new(Method::Post, "/invoices/2/finalize").header("authorization", &auth),
    );
    let sinks = source.sinks();
    kernel.run(&mut source).map_err(|e| e.to_string())?;

    let received_after = {
        let handles = sinks
            .lock()
            .map_err(|_| "sink handles lock poisoned".to_string())?;
        if handles.is_empty() {
            return Err("the resumed subscribe never upgraded to SSE".to_string());
        }
        handles[0].frames()
    };
    let names_after: Vec<&str> = received_after.iter().map(|f| f.name).collect();
    if names_after != ["invoice_finalized", "receipt_sent"] {
        return Err(format!(
            "expected [invoice_finalized, receipt_sent] after the resume, got {names_after:?} \
             — a gap or a duplicate in the resumed tail"
        ));
    }
    if let Some(dup) = received_after.iter().find(|f| before_ids.contains(&f.id)) {
        return Err(format!(
            "resumed tail re-delivered event {} — already seen before the restart",
            dup.id
        ));
    }
    Ok(())
}

// ---- 7. a minted API key authorizes; once revoked, it is rejected ---------

/// `#[job]`-independent: this workload never touches `SendReceipt` or the
/// day-advance cron, just `/keys` mint/revoke against an authed route
/// (`/customers`). Runs under `sim`'s own fault config (not a fresh
/// fault-free `Sim`, unlike `illegal_transitions_are_rejected` /
/// `events_feed_resumes_across_restart`), so every step follows the other
/// fault-injected workloads' discipline: an unclean ack is fault fallout,
/// not a failure, EXCEPT the final read — once the revoke itself acked 200
/// (durably committed, per the platform's ack-after-durable rule), a 200 on
/// the next read with the same key is a real bug, not fallout.
fn api_key_authorizes_then_revoked_is_rejected(sim: &mut Sim) -> Result<(), String> {
    let Some(app) = boot_through_fallout(sim, BOOT_TRIES, is_fault_fallout, || build(sim))
        .map_err(|e| e.to_string())?
    else {
        sim.give_up(format!("the app never booted in {BOOT_TRIES} boots"));
        return Ok(());
    };
    let mut kernel = match Kernel::new(app, Box::new(sim.clock())) {
        Ok(k) => k,
        Err(e) if is_fault_fallout(&e.to_string()) => return Ok(()),
        Err(e) => return Err(e.to_string()),
    };
    let token = login(&mut kernel, sim)?.unwrap_or_default();
    let auth = format!("Bearer {token}");

    // Mint an API key.
    let mut source = SimSource::new(Box::new(sim.clock()), SEC);
    source.push_at(
        0,
        Request::new(Method::Post, "/keys").header("authorization", &auth),
    );
    if let Err(e) = kernel.run(&mut source)
        && !is_fault_fallout(&e.to_string())
    {
        return Err(e.to_string());
    }
    let Some(200) = statuses_of(&source)?.first().copied() else {
        return Ok(()); // mint didn't cleanly ack — fault fallout
    };
    let raw_key: String = {
        let responses = source.responses();
        let responses = responses
            .lock()
            .map_err(|_| "responses lock poisoned".to_string())?;
        serde_json::from_slice(&responses[0].1).map_err(|e| e.to_string())?
    };
    let key_auth = format!("Bearer {raw_key}");

    // The freshly minted key authorizes an authed route.
    let mut source = SimSource::new(Box::new(sim.clock()), SEC);
    source.push_at(
        0,
        Request::new(Method::Get, "/customers").header("authorization", &key_auth),
    );
    if let Err(e) = kernel.run(&mut source)
        && !is_fault_fallout(&e.to_string())
    {
        return Err(e.to_string());
    }
    let Some(200) = statuses_of(&source)?.first().copied() else {
        return Ok(()); // the authorizing read didn't cleanly ack — fault fallout
    };
    sim.cover("auth:key-authorizes");

    // Look up the minted key's row to get its id for the revoke route —
    // mirrors crates/charpente/tests/http.rs's resolution_ladder_matrix.
    let key_hash = hash_token(&raw_key);
    let snap = match kernel.app().db().snapshot() {
        Ok(s) => s,
        Err(e) if is_fault_fallout(&e.to_string()) => return Ok(()),
        Err(e) => return Err(e.to_string()),
    };
    let key_id = match ApiKey::by_key_hash(&snap, &key_hash) {
        Ok(rows) => match rows.into_iter().next() {
            Some(row) => row.id,
            None => return Ok(()), // row not durable yet — fault fallout
        },
        Err(e) if is_fault_fallout(&e.to_string()) => return Ok(()),
        Err(e) => return Err(e.to_string()),
    };
    drop(snap);

    // Revoke it.
    let mut source = SimSource::new(Box::new(sim.clock()), SEC);
    source.push_at(
        0,
        Request::new(Method::Delete, format!("/keys/{}", key_id.raw()))
            .header("authorization", &auth),
    );
    if let Err(e) = kernel.run(&mut source)
        && !is_fault_fallout(&e.to_string())
    {
        return Err(e.to_string());
    }
    let Some(200) = statuses_of(&source)?.first().copied() else {
        return Ok(()); // revoke didn't cleanly ack — fault fallout
    };

    // The revoked key is now rejected — the named 401, byte-exact (copied
    // from crates/charpente/tests/http.rs:456-457's UNRECOGNIZED constant).
    let mut source = SimSource::new(Box::new(sim.clock()), SEC);
    source.push_at(
        0,
        Request::new(Method::Get, "/customers").header("authorization", &key_auth),
    );
    if let Err(e) = kernel.run(&mut source)
        && !is_fault_fallout(&e.to_string())
    {
        return Err(e.to_string());
    }
    let responses = source.responses();
    let (status, body) = {
        let responses = responses
            .lock()
            .map_err(|_| "responses lock poisoned".to_string())?;
        match responses.first() {
            Some((status, body)) => (*status, body.clone()),
            None => return Ok(()), // no response recorded — fault fallout
        }
    };
    const UNRECOGNIZED: &str =
        "unrecognized credential — it may have been revoked; obtain a new one";
    match status {
        200 => Err(
            "revoked API key still authorized a request — revoke did not take effect \
                     even though the revoke request itself acked 200"
                .to_string(),
        ),
        401 => {
            #[derive(serde::Deserialize)]
            struct ErrBody {
                error: String,
            }
            let err: ErrBody = serde_json::from_slice(&body).map_err(|e| e.to_string())?;
            if err.error != UNRECOGNIZED {
                return Err(format!(
                    "expected {UNRECOGNIZED:?} for a revoked key, got {:?}",
                    err.error
                ));
            }
            Ok(())
        }
        // Any other status (e.g. 500 from a fault mid-ladder-resolution) is
        // fault fallout, not a bug: the revoke's own 200 ack already proved
        // the delete committed.
        _ => Ok(()),
    }
}
