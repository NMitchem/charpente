//! Partition smoke test for the simulated network: delivery respects virtual
//! latency, partitions refuse connects and eat packets, repair restores flow,
//! close is observable as a zero-length recv.

use std::time::Duration;

use charpente_core::io::{Clock, Net};
use charpente_sim::{FaultConfig, Sim};

fn fixed_latency() -> FaultConfig {
    FaultConfig {
        net_latency: (Duration::from_millis(5), Duration::from_millis(5)),
        ..FaultConfig::none()
    }
}

#[test]
fn partition_and_repair() {
    let sim = Sim::with_faults(7, fixed_latency());
    let clock = sim.clock();
    let server_net = sim.net("server");
    let client_net = sim.net("client");

    let listener = server_net.listen("server:7000").unwrap();
    let client = client_net.connect("server:7000").unwrap();
    let server = listener
        .accept()
        .unwrap()
        .expect("connection is in the backlog");
    assert!(
        listener.accept().unwrap().is_none(),
        "backlog must now be empty"
    );

    // Delivery takes virtual latency: nothing arrives until time passes.
    client.send(b"ping").unwrap();
    let mut buf = [0u8; 64];
    assert_eq!(server.recv(&mut buf).unwrap(), None);
    clock.sleep(Duration::from_millis(5));
    assert_eq!(server.recv(&mut buf).unwrap(), Some(4));
    assert_eq!(&buf[..4], b"ping");

    // Partition: connects are refused, packets are silently lost.
    sim.partition("client", "server");
    let Err(refused) = client_net.connect("server:7000") else {
        panic!("connect across a partition must fail");
    };
    assert!(
        refused.to_string().contains("simulated partition"),
        "{refused}"
    );
    client.send(b"lost").unwrap();
    clock.sleep(Duration::from_secs(1));
    assert_eq!(
        server.recv(&mut buf).unwrap(),
        None,
        "partitioned packets must be lost"
    );

    // Repair: new packets flow; the lost one stays lost.
    sim.repair("client", "server");
    client.send(b"back").unwrap();
    clock.sleep(Duration::from_millis(5));
    assert_eq!(server.recv(&mut buf).unwrap(), Some(4));
    assert_eq!(&buf[..4], b"back");

    // In-flight data survives the sender closing; then recv reports closed.
    server.send(b"bye").unwrap();
    drop(server);
    clock.sleep(Duration::from_millis(5));
    assert_eq!(client.recv(&mut buf).unwrap(), Some(3));
    assert_eq!(client.recv(&mut buf).unwrap(), Some(0), "peer closed");
}
