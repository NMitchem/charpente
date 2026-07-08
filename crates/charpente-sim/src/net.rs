//! The simulated network's state: named hosts, listeners, message-oriented
//! connections with delivery latency, and partitions. Messages sent across a
//! partition are silently lost, like the real thing; a partitioned `connect`
//! is refused. Host crash/restart tears down that host's network presence
//! (`teardown_host`): every conn with that host on one side has that side
//! marked closed and its inbound-to-that-host queue dropped (undelivered
//! messages to a dead host are gone — it will never read them), and every
//! listener the host owns is removed, freeing the address for a fresh
//! `listen` once the host restarts. Listener tokens (`ListenerState.token`,
//! from `NetWorld.next_listener`) stop a stale handle held across a crash
//! from clobbering a re-listen on the same address: `accept` and `Drop` both
//! check the token, not just the address.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

#[derive(Default)]
pub(crate) struct NetWorld {
    /// Listener address -> its backlog + the token identifying this listen
    /// generation.
    pub listeners: BTreeMap<String, ListenerState>,
    pub conns: BTreeMap<u64, ConnState>,
    pub next_conn: u64,
    pub next_listener: u64,
    /// Normalized host pairs currently partitioned.
    pub partitions: BTreeSet<(String, String)>,
}

#[derive(Default)]
pub(crate) struct ListenerState {
    pub backlog: VecDeque<u64>,
    pub token: u64,
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

    /// Host crash/restart teardown. Idempotent — safe to call again from
    /// `restart()` even when `crash_now`/the crash countdown already ran it
    /// for this crash, and safe to call from `restart()` with no prior crash
    /// at all (a clean power cycle drops connections too).
    pub fn teardown_host(&mut self, host: &str) {
        for conn in self.conns.values_mut() {
            if conn.host_a == host {
                conn.open_a = false;
                conn.to_a.clear(); // undelivered messages TO the dead host
            }
            if conn.host_b == host {
                conn.open_b = false;
                conn.to_b.clear();
            }
        }
        self.listeners.retain(|addr, _| host_of(addr) != host);
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

    #[test]
    fn teardown_host_closes_conns_and_drops_only_inbound_to_the_dead_host() {
        let mut net = NetWorld::default();
        net.conns.insert(
            0,
            ConnState {
                host_a: "a".to_string(),
                host_b: "b".to_string(),
                listen_addr: "b:7000".to_string(),
                to_a: VecDeque::from([(0, vec![1])]),
                to_b: VecDeque::from([(0, vec![2])]),
                open_a: true,
                open_b: true,
            },
        );
        net.listeners.insert(
            "b:7000".to_string(),
            ListenerState {
                backlog: VecDeque::new(),
                token: 0,
            },
        );
        net.teardown_host("b");
        let conn = &net.conns[&0];
        assert!(conn.open_a, "the surviving side stays open");
        assert!(!conn.open_b, "the crashed side is closed");
        assert_eq!(
            conn.to_a.len(),
            1,
            "inbound to the surviving host is untouched"
        );
        assert!(
            conn.to_b.is_empty(),
            "inbound to the crashed host is dropped"
        );
        assert!(
            !net.listeners.contains_key("b:7000"),
            "the crashed host's listener is gone"
        );
    }
}
