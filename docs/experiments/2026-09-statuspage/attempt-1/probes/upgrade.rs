//! Review probe: an SSE resume that spans the builder's own v1 -> v2 upgrade.
use charpente::http::{Cursor, Method, Request};
use charpente::kernel::{Kernel, SimSinkHandle, SimSource};
use charpente_sim::{FaultConfig, Sim};
use serde_json::{Value, json};

const SEC: u64 = 1_000_000_000;

fn send(sim: &Sim, kernel: &mut Kernel, req: Request) -> (u16, Value) {
    let at = sim.now().as_nanos() + SEC;
    let mut src = SimSource::new(Box::new(sim.clock()), at + SEC);
    src.push_at(at, req);
    let responses = src.responses();
    kernel.run(&mut src).expect("kernel run");
    let r = responses.lock().unwrap();
    let (status, body) = r.first().expect("a reply").clone();
    (status, serde_json::from_slice(&body).unwrap_or(Value::Null))
}

/// Opens an SSE tail; returns its handle and the result of the kernel run that served it.
fn tail(sim: &Sim, kernel: &mut Kernel, req: Request) -> (Option<SimSinkHandle>, std::io::Result<()>) {
    let at = sim.now().as_nanos() + SEC;
    let mut src = SimSource::new(Box::new(sim.clock()), at + SEC);
    src.push_at(at, req);
    let sinks = src.sinks();
    let ran = kernel.run(&mut src);
    let h = sinks.lock().unwrap().first().cloned();
    (h, ran)
}

fn key(req: Request, k: &str) -> Request {
    req.header("authorization", format!("Bearer {k}"))
}

/// v1 phase: one org, one component, a feed from now, two incidents opened (two v1-shaped
/// IncidentOpened rows). Returns (sim, org id, api key, the frames the v1 feed received).
fn v1_phase(seed: u64) -> (Sim, String, String, Vec<(u64, u64, String)>) {
    let sim = Sim::with_faults(seed, FaultConfig::none());
    let app = sp_v1::build_app(Box::new(sim.disk()), Box::new(sim.entropy())).unwrap();
    let mut k = Kernel::new(app, Box::new(sim.clock())).unwrap();
    let (s, created) = send(&sim, &mut k, Request::new(Method::Post, "/orgs").json(&json!({"name":"acme","operator":"ann"})));
    assert_eq!(s, 200, "{created}");
    let org = created["org"].as_str().unwrap().to_string();
    let api = created["api_key"].as_str().unwrap().to_string();
    let (s, comp) = send(&sim, &mut k, key(Request::new(Method::Post, format!("/orgs/{org}/components")).json(&json!({"name":"api","status":"Operational"})), &api));
    assert_eq!(s, 200, "{comp}");
    let comp = comp["id"].as_str().unwrap().to_string();
    let (feed, ran) = tail(&sim, &mut k, Request::new(Method::Get, format!("/orgs/{org}/events")));
    ran.unwrap();
    let feed = feed.expect("v1 feed served");
    for title in ["first", "second"] {
        let (s, b) = send(&sim, &mut k, key(Request::new(Method::Post, format!("/orgs/{org}/incidents")).json(&json!({"title":title,"components":[comp]})), &api));
        assert_eq!(s, 200, "{b}");
    }
    let frames: Vec<(u64, u64, String)> = feed.frames().iter().map(|f| (f.id, f.seq, f.name.to_string())).collect();
    assert_eq!(frames.len(), 2, "v1 feed saw both IncidentOpened frames: {frames:?}");
    // the kernel's last run ended at its horizon: a clean, checkpointed shutdown
    drop(k.into_app());
    (sim, org, api, frames)
}

fn v2_kernel(sim: &Sim) -> Kernel {
    let app = sp_v2::build_app(Box::new(sim.disk()), Box::new(sim.entropy())).unwrap();
    Kernel::new(app, Box::new(sim.clock())).unwrap()
}

#[test]
fn control_resume_from_the_last_v1_frame_after_the_upgrade() {
    let (sim, org, api, frames) = v1_phase(0x5eed);
    let mut k = v2_kernel(&sim);
    // the table migration itself worked: the operator listing reads v1 rows back as Minor
    let (s, listed) = send(&sim, &mut k, key(Request::new(Method::Get, format!("/orgs/{org}/incidents")), &api));
    assert_eq!(s, 200);
    println!("v2 incident listing after migration: {listed}");
    let (id, seq, _) = frames[1];
    let (feed, ran) = tail(&sim, &mut k, Request::new(Method::Get, format!("/orgs/{org}/events")).header("last-event-id", Cursor { id, seq }.wire()));
    println!("control resume from {id}.{seq}: run = {ran:?}, served = {}", feed.is_some());
    ran.expect("resuming past every v1 row is fine");
}

#[test]
fn resume_from_the_first_v1_frame_after_the_upgrade() {
    let (sim, org, _api, frames) = v1_phase(0x5eed);
    let mut k = v2_kernel(&sim);
    let (id, seq, _) = frames[0];
    // anonymous: no credential, only a cursor any earlier subscriber was handed
    let (_feed, ran) = tail(&sim, &mut k, Request::new(Method::Get, format!("/orgs/{org}/events")).header("last-event-id", Cursor { id, seq }.wire()));
    println!("resume from {id}.{seq} after the v2 upgrade: kernel.run = {ran:?}");
    // after a restart, the same anonymous request stops the new kernel the same way
    drop(k);
    let mut k = v2_kernel(&sim);
    let (_feed, ran2) = tail(&sim, &mut k, Request::new(Method::Get, format!("/orgs/{org}/events")).header("last-event-id", Cursor { id, seq }.wire()));
    println!("same request after a reboot: kernel.run = {ran2:?}");
    assert!(ran.is_ok(), "the kernel stopped: {ran:?}");
}
