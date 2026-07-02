//! The simulated network's state: named hosts, listeners, message-oriented
//! connections with delivery latency, and partitions. Messages sent across a
//! partition are silently lost, like the real thing; a partitioned `connect`
//! is refused. Host crash/restart is not modeled yet — it arrives with the
//! deterministic executor when components run as sim-scheduled tasks.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

#[derive(Default)]
pub(crate) struct NetWorld {
    /// Listener address -> backlog of connections awaiting accept.
    pub listeners: BTreeMap<String, VecDeque<u64>>,
    pub conns: BTreeMap<u64, ConnState>,
    pub next_conn: u64,
    /// Normalized host pairs currently partitioned.
    pub partitions: BTreeSet<(String, String)>,
}

pub(crate) struct ConnState {
    pub host_a: String,
    pub host_b: String,
    /// The address side A connected to; sides report it as the peer/local addr.
    pub listen_addr: String,
    /// In-flight messages: (virtual deliver-at nanos, payload).
    pub to_a: VecDeque<(u64, Vec<u8>)>,
    pub to_b: VecDeque<(u64, Vec<u8>)>,
    pub open_a: bool,
    pub open_b: bool,
}

impl NetWorld {
    pub fn partitioned(&self, a: &str, b: &str) -> bool {
        self.partitions.contains(&normalize(a, b))
    }
}

pub(crate) fn normalize(a: &str, b: &str) -> (String, String) {
    if a <= b {
        (a.to_string(), b.to_string())
    } else {
        (b.to_string(), a.to_string())
    }
}

/// The host part of a `"host:port"` address.
pub(crate) fn host_of(addr: &str) -> &str {
    addr.rsplit_once(':').map_or(addr, |(host, _)| host)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn host_of_strips_port() {
        assert_eq!(host_of("replica-1:7000"), "replica-1");
        assert_eq!(host_of("bare-name"), "bare-name");
    }

    #[test]
    fn partition_is_symmetric() {
        let mut net = NetWorld::default();
        net.partitions.insert(normalize("b", "a"));
        assert!(net.partitioned("a", "b"));
        assert!(net.partitioned("b", "a"));
        assert!(!net.partitioned("a", "c"));
    }
}
