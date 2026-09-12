//! The TCP fault proxy: a listener `L` forwards every connection to
//! `upstream`, with per-connection control the plan's scenarios need —
//! severing every live connection (`drop_all`), refusing new ones
//! (`partition`), and delaying forwarded bytes (`delay_ms`). Connection
//! granularity only (crate doc's honesty contract): this proxy can sever or
//! delay a TCP connection, never corrupt or reorder the bytes inside one —
//! that belongs to the simulator.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use std::io::{self, Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use crate::scenario::Scenario;

const CHUNK: usize = 4096;

struct ProxyState {
    partitioned: bool,
    delay_ms: u64,
    conns: Vec<TcpStream>,
    /// Set by `stop` (T12 routed prerequisite — T10's M2): once true, the
    /// accept loop shuts down the connection that triggered it and exits
    /// instead of spawning a relay, freeing the listening port so a scenario
    /// that needs more than one proxy in sequence (Oracle 2's peer link)
    /// isn't stuck on a permanently bound ephemeral address.
    stopped: bool,
}

/// A TCP fault proxy in front of one upstream address. Every accepted
/// connection spawns two relay threads (client→upstream, upstream→client);
/// `drop_all`/`partition`/`delay_ms` mutate shared state the relay threads
/// (and the accept loop) read continuously.
pub struct TcpProxy {
    addr: String,
    state: Arc<Mutex<ProxyState>>,
}

impl TcpProxy {
    /// Binds an ephemeral local port and starts forwarding to `upstream`.
    pub fn start(upstream: &str) -> io::Result<TcpProxy> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?.to_string();
        let state = Arc::new(Mutex::new(ProxyState {
            partitioned: false,
            delay_ms: 0,
            conns: Vec::new(),
            stopped: false,
        }));
        let upstream = upstream.to_string();
        let accept_state = state.clone();
        thread::spawn(move || {
            for incoming in listener.incoming() {
                let Ok(client) = incoming else { continue };
                let st = accept_state.lock().unwrap();
                if st.stopped {
                    drop(st);
                    let _ = client.shutdown(Shutdown::Both);
                    break; // drop `listener` — frees the ephemeral port
                }
                if st.partitioned {
                    drop(st);
                    let _ = client.shutdown(Shutdown::Both);
                    continue;
                }
                drop(st);
                let upstream = upstream.clone();
                let state = accept_state.clone();
                thread::spawn(move || relay(client, &upstream, state));
            }
        });
        Ok(TcpProxy { addr, state })
    }

    pub fn addr(&self) -> String {
        self.addr.clone()
    }

    /// Severs every currently live connection through this proxy, both
    /// directions, immediately.
    pub fn drop_all(&self, scenario: &mut Scenario) {
        let mut state = self.state.lock().unwrap();
        let n = state.conns.len();
        for conn in state.conns.drain(..) {
            let _ = conn.shutdown(Shutdown::Both);
        }
        scenario.record(format!("proxy.drop_all: severed {n} live connection(s)"));
    }

    /// `on = true` refuses every NEW connection from now on (existing
    /// connections are untouched — pair with `drop_all` to also sever them).
    pub fn partition(&self, on: bool, scenario: &mut Scenario) {
        self.state.lock().unwrap().partitioned = on;
        scenario.record(format!("proxy.partition({on})"));
    }

    /// Adds a per-chunk delay to every byte forwarded in both directions
    /// from now on (0 clears it).
    pub fn delay_ms(&self, ms: u64, scenario: &mut Scenario) {
        self.state.lock().unwrap().delay_ms = ms;
        scenario.record(format!("proxy.delay_ms({ms})"));
    }

    /// Stops this proxy for good: severs every live connection (like
    /// `drop_all`), refuses any connection already in flight to the accept
    /// loop, and frees the listening port — a scenario that needs more than
    /// one `TcpProxy` in sequence (T10 routed prerequisite M2, e.g. Oracle
    /// 2's peer link, torn down once the primary/follower pairing under it
    /// is retired) calls this rather than leaking a forwarding thread and an
    /// ephemeral port per proxy for the rest of the process's life.
    /// Idempotent — a second `stop` is a harmless no-op, not a double-record.
    pub fn stop(&self, scenario: &mut Scenario) {
        let mut state = self.state.lock().unwrap();
        if state.stopped {
            return;
        }
        state.stopped = true;
        let n = state.conns.len();
        for conn in state.conns.drain(..) {
            let _ = conn.shutdown(Shutdown::Both);
        }
        drop(state);
        scenario.record(format!(
            "proxy.stop: severed {n} live connection(s), refusing all further ones"
        ));
        // The accept loop blocks in `listener.incoming()`; a self-connect
        // wakes it so it observes `stopped` and exits (see the loop body
        // above) instead of leaking a thread parked on `accept()` forever.
        let _ = TcpStream::connect(&self.addr);
    }
}

fn relay(client: TcpStream, upstream: &str, state: Arc<Mutex<ProxyState>>) {
    // Registered BEFORE dialing upstream (review Minor 9): a client
    // accepted here is already a live connection through this proxy even
    // though the upstream dial hasn't finished — registering only after
    // `connect` returns left a window where `stop`/`drop_all`'s drain could
    // neither see nor sever a connection that had already passed the
    // accept-loop's `stopped` check.
    //
    // And `stopped` is re-read under the SAME lock as the registration: the
    // accept loop checked it, released the lock, then spawned this thread,
    // so a `stop` landing in that gap drains a list this connection is not
    // on yet and never severs it — it then relays as if nothing happened
    // (nightly 2026-09-11, `stop_severs_live_connections_and_frees_the_port`
    // read an echo back). One critical section leaves no interleaving in
    // which neither the drain nor this check sees the connection.
    {
        let mut st = state.lock().unwrap();
        if st.stopped {
            drop(st);
            let _ = client.shutdown(Shutdown::Both);
            return;
        }
        if let Ok(clone) = client.try_clone() {
            st.conns.push(clone);
        }
    }
    let Ok(upstream_conn) = TcpStream::connect(upstream) else {
        return;
    };
    let (Ok(client_to_up), Ok(up_to_client)) = (client.try_clone(), upstream_conn.try_clone())
    else {
        return;
    };

    let s1 = state.clone();
    let t1 = thread::spawn(move || pump(client_to_up, upstream_conn, s1));
    let s2 = state.clone();
    let t2 = thread::spawn(move || pump(up_to_client, client, s2));
    let _ = t1.join();
    let _ = t2.join();
}

fn pump(mut from: TcpStream, mut to: TcpStream, state: Arc<Mutex<ProxyState>>) {
    let mut buf = [0u8; CHUNK];
    loop {
        let n = match from.read(&mut buf) {
            Ok(0) | Err(_) => break,
            Ok(n) => n,
        };
        let delay = state.lock().unwrap().delay_ms;
        if delay > 0 {
            thread::sleep(Duration::from_millis(delay));
        }
        if to.write_all(&buf[..n]).is_err() {
            break;
        }
    }
    let _ = to.shutdown(Shutdown::Both);
    let _ = from.shutdown(Shutdown::Both);
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A single-shot echo "upstream": accepts one connection, echoes
    /// whatever it reads back, then returns.
    fn spawn_echo_upstream() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                let mut buf = [0u8; 64];
                if let Ok(n) = stream.read(&mut buf) {
                    let _ = stream.write_all(&buf[..n]);
                }
            }
        });
        addr
    }

    #[test]
    #[ignore]
    fn forwards_bytes_to_upstream_and_back() {
        let upstream_addr = spawn_echo_upstream();
        let proxy = TcpProxy::start(&upstream_addr).unwrap();
        let mut client = TcpStream::connect(proxy.addr()).unwrap();
        client.write_all(b"hello").unwrap();
        let mut buf = [0u8; 5];
        client.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, b"hello");
    }

    #[test]
    #[ignore]
    fn partition_refuses_new_connections() {
        let upstream_addr = spawn_echo_upstream();
        let proxy = TcpProxy::start(&upstream_addr).unwrap();
        let mut scenario = Scenario::new(&std::env::temp_dir(), "proxy_partition_test").unwrap();
        proxy.partition(true, &mut scenario);

        let mut client = TcpStream::connect(proxy.addr()).unwrap();
        client.write_all(b"hello").unwrap();
        let mut buf = [0u8; 5];
        // The proxy shuts the connection down immediately on accept while
        // partitioned — the write above may or may not error (best-effort
        // local write), but the read must observe the closed connection
        // rather than an echo.
        let read = client.read(&mut buf);
        assert!(
            matches!(read, Ok(0)) || read.is_err(),
            "expected a closed connection while partitioned, got {read:?}"
        );
        std::fs::remove_dir_all(scenario.dir()).unwrap();
    }

    /// T10 routed prerequisite M2: `stop` must sever a live connection AND
    /// free the port so a second, independent proxy can bind to it — the
    /// exact sequential-reuse need Oracle 2 has for the peer link. Binding a
    /// second listener to the exact address `stop` just vacated is the
    /// proof; a leaked accept thread would leave the address in use (or the
    /// live connection un-severed) and this would fail with `AddrInUse` or a
    /// successful echo instead.
    #[test]
    #[ignore]
    fn stop_severs_live_connections_and_frees_the_port() {
        let upstream_addr = spawn_echo_upstream();
        let proxy = TcpProxy::start(&upstream_addr).unwrap();
        let proxy_addr = proxy.addr();
        let mut scenario = Scenario::new(&std::env::temp_dir(), "proxy_stop_test").unwrap();

        let mut client = TcpStream::connect(&proxy_addr).unwrap();
        proxy.stop(&mut scenario);

        // The live connection must be severed, not left echoing.
        client.write_all(b"hello").unwrap();
        let mut buf = [0u8; 5];
        let read = client.read(&mut buf);
        assert!(
            matches!(read, Ok(0)) || read.is_err(),
            "expected the live connection to be severed by stop, got {read:?}"
        );

        // Idempotent: a second stop is a no-op, not a panic or a double record.
        proxy.stop(&mut scenario);

        // The port must be free: a second, unrelated listener can bind the
        // exact address this proxy vacated. Retried briefly since the OS can
        // take a moment to release the address after the accept thread's
        // listener drops.
        let mut bound = None;
        for _ in 0..50 {
            if let Ok(l) = TcpListener::bind(&proxy_addr) {
                bound = Some(l);
                break;
            }
            thread::sleep(Duration::from_millis(20));
        }
        assert!(
            bound.is_some(),
            "stop must free the listening port {proxy_addr} — a second listener could not bind it"
        );

        std::fs::remove_dir_all(scenario.dir()).unwrap();
    }
}
