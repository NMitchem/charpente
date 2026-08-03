//! The witness probe client (M2 sub-project 0b task 4, spec
//! docs/superpowers/specs/2026-07-31-dual-primary-design.md §2): connects to
//! ONE recorded witness, sends one `Hello` carrying this node's real
//! position, classifies the first response into a verdict about the
//! witness's ROLE, and disconnects. Task 5 runs one probe per recorded
//! witness at primary startup to detect silent primary resurrection (the
//! dual-primary gap): a primary that died, was replaced by a promoted
//! follower, and later restarts exactly as configured must not resume
//! serving writes until it has confirmed none of its witnesses is now a
//! live primary. Nothing calls this module until Task 5.
//!
//! **Classification, not adoption.** `decide_hello`'s ladder (`session.rs`)
//! checks the peer secret, protocol version, schema digest, and listen
//! address BEFORE it ever consults the role — every refusal in that prefix
//! carries ZERO information about the witness's role, so this probe must not
//! read any of them as proof of anything. Only two shapes are conclusive:
//! the not-a-primary refusal (a role match that failed — the witness is
//! provably not a primary) and anything past the role match (a diverged
//! refusal, or an actual ship message — the witness is provably a primary).
//! Everything else classifies `Inconclusive`, never `IsPrimary`: a false
//! "is primary" would make a resurrected node refuse to start over a
//! phantom split-brain caused by, say, a mistyped secret — a false positive
//! in the direction that costs availability, which this probe must never
//! manufacture.
//!
//! **`Role::HandingOff` classifies `IsPrimary`, and that is correct — not a
//! bug to "fix".** It passes the role match exactly like `Role::Primary`
//! (`session.rs`), so a `HandingOff` witness answers a ship message, never
//! "not a primary". A node in THIS node's witness set is, by construction,
//! one this node shipped TO — its follower — and `HandingOff` is only ever
//! set by `begin_switchover`, which only a `Primary` can initiate. So a
//! `HandingOff` witness proves a promotion/handoff occurred, exactly what
//! `IsPrimary` means for the caller.
//!
//! **Never adopts.** Unlike `Follower::step`, this client classifies the
//! first response and disconnects — it never falls through to
//! `apply_and_ack`/seeding the way `Follower::new_resolving`'s resolver
//! does. Silently adopting a witness's timeline is exactly what the
//! 2026-07-16 diverged-BEHIND ruling rejected: this node cannot know it is
//! the loser, and its acked writes are not the probe's to discard.
//! Structurally enforced, not just by convention: `step`'s `db: &Db` is
//! immutable, and the apply path needs `&mut Db` — it is simply not
//! reachable from here.
//!
//! **The Hello carries the real position, never `None`.** A `None`-position
//! Hello can make a healthy primary witness run `begin_seeding` — a full
//! snapshot render — as a side effect of someone merely restarting a
//! resurrected node. The real position is equally conclusive: a
//! same-timeline follower witness refuses at the role match before position
//! logic runs, and a forked witness answers `Frames` or a `diverged_*`
//! refusal without seeding.

use std::io;

use charpente_core::io::{Conn, Net};

use crate::Db;
use crate::repl::follower::hello_position;
use crate::repl::protocol::{Msg, decode, encode};
use crate::repl::session::{DIVERGED_LABEL, NOT_A_PRIMARY_MARKER};
use crate::repl::{MAX_MESSAGE_BYTES, PROTO_VERSION, RECONNECT_NANOS};

/// This witness's role, as far as one probe can tell from its first
/// response. See the module doc for why the four-state shape exists —
/// `IsPrimary` and `NotPrimary` are the only two conclusive outcomes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WitnessVerdict {
    /// No response yet: still connecting (or reconnecting), or connected and
    /// awaiting the first message.
    Pending,
    /// The witness refused at the role match: provably not a primary.
    NotPrimary,
    /// The witness answered past the role match — a diverged refusal, or an
    /// actual ship message: provably a primary (or a node handing off,
    /// which classifies the same way — see the module doc).
    IsPrimary(String),
    /// The witness refused for a reason the role match never reached (peer
    /// secret, protocol version, schema digest, or an invalid listen
    /// address): this response carries no information about its role.
    Inconclusive(String),
}

/// Connects to one witness, sends one `Hello`, classifies the first
/// response, and disconnects. See the module doc.
pub struct WitnessProbe {
    addr: String,
    my_listen: Option<String>,
    secret: Option<String>,
    conn: Option<Box<dyn Conn>>,
    recv_buf: Option<Vec<u8>>,
    /// Reconnect backoff after a failed connect/send, mirroring `Follower`'s
    /// pacing (`RECONNECT_NANOS`) rather than busy-looping the caller.
    reconnect_at: Option<u64>,
    verdict: WitnessVerdict,
}

impl WitnessProbe {
    pub fn new(addr: String, my_listen: Option<String>, secret: Option<String>) -> WitnessProbe {
        WitnessProbe {
            addr,
            my_listen,
            secret,
            conn: None,
            recv_buf: None,
            reconnect_at: None,
            verdict: WitnessVerdict::Pending,
        }
    }

    pub fn verdict(&self) -> &WitnessVerdict {
        &self.verdict
    }

    /// One unit of work: connect + `Hello` when disconnected and due, else
    /// read one message and classify it. A verdict other than `Pending` is
    /// terminal (the conn is already dropped) — later calls are a no-op, so
    /// the caller can keep stepping every probe uniformly without tracking
    /// which ones are already decided.
    pub fn step(&mut self, net: &dyn Net, db: &Db, now: u64) -> io::Result<()> {
        if !matches!(self.verdict, WitnessVerdict::Pending) {
            return Ok(());
        }
        if self.conn.is_none() {
            if self.reconnect_at.is_some_and(|t| now < t) {
                return Ok(());
            }
            return self.connect_and_hello(net, db, now);
        }
        let WitnessProbe { conn, recv_buf, .. } = self;
        let buf = recv_buf.get_or_insert_with(|| vec![0u8; MAX_MESSAGE_BYTES]);
        let c = conn.as_ref().expect("checked non-none above");
        match c.recv(buf) {
            Ok(Some(0)) | Err(_) => {
                // No conclusion from this attempt: back off and let the
                // caller retry, mirroring Follower's reconnect pacing.
                self.conn = None;
                self.reconnect_at = Some(now + RECONNECT_NANOS);
                Ok(())
            }
            Ok(None) => Ok(()), // nothing yet — stays Pending
            Ok(Some(n)) => {
                let msg = decode(&buf[..n])?;
                self.verdict = classify(&msg);
                // Classify and disconnect: never fall through to apply
                // anything (the module doc's "never adopts").
                self.conn = None;
                Ok(())
            }
        }
    }

    fn connect_and_hello(&mut self, net: &dyn Net, db: &Db, now: u64) -> io::Result<()> {
        let conn = match net.connect(&self.addr) {
            Ok(conn) => conn,
            // Partition / witness not listening: back off and retry — not a
            // node fault, nothing to recover, just wait.
            Err(_) => {
                self.reconnect_at = Some(now + RECONNECT_NANOS);
                return Ok(());
            }
        };
        // The real position, never None — the module doc's cost-trap
        // rationale.
        let position = hello_position(db)?;
        let hello = Msg::Hello {
            proto: PROTO_VERSION,
            listen: self.my_listen.clone(),
            position,
            secret: self.secret.clone(),
            schema_digest: db.schema_digest(),
        };
        if conn.send(&encode(&hello)).is_err() {
            self.reconnect_at = Some(now + RECONNECT_NANOS);
            return Ok(());
        }
        self.conn = Some(conn);
        self.reconnect_at = None;
        Ok(())
    }
}

/// Classifies one response from a witness (see the module doc for the
/// ladder rationale — this ladder mirrors `decide_hello`'s check ORDER, not
/// its full logic). The default arm — any `Refuse` not carrying one of the
/// two markers — is deliberate: it absorbs every prefix refusal
/// (secret/version/schema/listen) and any refusal added to the ladder in
/// the future, so a new prefix check never needs a matching new arm here.
fn classify(msg: &Msg) -> WitnessVerdict {
    match msg {
        Msg::Refuse { message } if message.contains(NOT_A_PRIMARY_MARKER) => {
            WitnessVerdict::NotPrimary
        }
        Msg::Refuse { message } if message.contains(DIVERGED_LABEL) => {
            WitnessVerdict::IsPrimary(message.clone())
        }
        Msg::Refuse { message } => WitnessVerdict::Inconclusive(message.clone()),
        // Only a witness that passed the role match ever sends a non-Refuse
        // message — HandingOff included (module doc).
        other => WitnessVerdict::IsPrimary(format!(
            "witness answered {} — only a node past the role match (primary, or handing off) \
             ever sends one",
            other.name()
        )),
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::collections::VecDeque;
    use std::rc::Rc;

    use charpente_core::io::Listener;
    use charpente_core::table::Codec;

    use super::*;
    use crate::repl::role::Role;
    use crate::repl::session::{PEER_SECRET_REFUSAL, Session};
    use crate::{DbConfig, Job, Schema};
    use charpente_sim::{FaultConfig, Sim};

    // ---------------------------------------------------- test fixtures

    fn fresh_db(seed: u64) -> Db {
        let sim = Sim::with_faults(seed, FaultConfig::none());
        Db::open(Box::new(sim.disk()), DbConfig::default(), Schema::new())
            .expect("Db::open on a fresh sim disk")
    }

    /// A job, so `db.transact` produces one non-empty effects record — no
    /// `#[table]` needed just to get `last_applied` off `None`.
    struct Ping {
        n: u64,
    }
    impl Codec for Ping {
        fn encode(&self, out: &mut Vec<u8>) {
            self.n.encode(out);
        }
        fn decode(input: &mut &[u8]) -> io::Result<Ping> {
            Ok(Ping {
                n: u64::decode(input)?,
            })
        }
    }
    impl Job for Ping {
        const NAME: &'static str = "probe-test-ping";
    }

    /// A db with one committed record, so `last_applied` is `Some` — needed
    /// to prove the anti-adoption guard (test 3) and to drive a real
    /// diverged-ahead refusal (the coupling pin).
    fn seeded_db(seed: u64) -> Db {
        let mut db = fresh_db(seed);
        db.transact(|tx| {
            tx.enqueue(Ping { n: 1 });
            Ok::<(), String>(())
        })
        .expect("committing one job");
        db
    }

    /// A `Conn` double whose `recv` yields pre-loaded messages one at a
    /// time (the "scripted peer") and whose `send` is a no-op — the probe's
    /// own outbound `Hello` is not under test here.
    struct ScriptedConn {
        inbound: RefCell<VecDeque<Vec<u8>>>,
    }
    impl ScriptedConn {
        fn new(msg: &Msg) -> ScriptedConn {
            let mut q = VecDeque::new();
            q.push_back(encode(msg));
            ScriptedConn {
                inbound: RefCell::new(q),
            }
        }
    }
    impl Conn for ScriptedConn {
        fn send(&self, _data: &[u8]) -> io::Result<()> {
            Ok(())
        }
        fn recv(&self, out: &mut [u8]) -> io::Result<Option<usize>> {
            match self.inbound.borrow_mut().pop_front() {
                Some(bytes) => {
                    out[..bytes.len()].copy_from_slice(&bytes);
                    Ok(Some(bytes.len()))
                }
                None => Ok(None),
            }
        }
        fn peer_addr(&self) -> String {
            "witness".to_string()
        }
    }

    /// A `Net` double that hands back one pre-built `Conn` on the first
    /// `connect` and panics on a second — `WitnessProbe` never reconnects
    /// once it has decided, and these tests never exercise backoff.
    struct OneShotNet(RefCell<Option<Box<dyn Conn>>>);
    impl OneShotNet {
        fn new(conn: Box<dyn Conn>) -> OneShotNet {
            OneShotNet(RefCell::new(Some(conn)))
        }
    }
    impl Net for OneShotNet {
        fn listen(&self, _addr: &str) -> io::Result<Box<dyn Listener>> {
            unreachable!("WitnessProbe never listens")
        }
        fn connect(&self, _addr: &str) -> io::Result<Box<dyn Conn>> {
            Ok(self
                .0
                .borrow_mut()
                .take()
                .expect("OneShotNet: connect called more than once in a test"))
        }
    }

    /// Drives a `WitnessProbe` against one scripted response: connects (step
    /// 1), then reads and classifies it (step 2).
    fn probe_scripted(seed: u64, response: &Msg) -> (WitnessProbe, Db) {
        let db = fresh_db(seed);
        let net = OneShotNet::new(Box::new(ScriptedConn::new(response)));
        let mut probe = WitnessProbe::new("w:1".to_string(), None, None);
        probe.step(&net, &db, 0).expect("connect + send Hello");
        probe.step(&net, &db, 0).expect("recv + classify");
        (probe, db)
    }

    // ---------------------------------------------------- the five tests

    #[test]
    fn a_not_a_primary_refusal_classifies_not_primary() {
        let refusal = Msg::Refuse {
            message: format!("this node is {NOT_A_PRIMARY_MARKER} (it is a follower of p:1)"),
        };
        let (probe, _db) = probe_scripted(0x0DEB_0401, &refusal);
        assert_eq!(*probe.verdict(), WitnessVerdict::NotPrimary);
    }

    #[test]
    fn a_diverged_family_refusal_classifies_is_primary() {
        let refusal = Msg::Refuse {
            message: format!(
                "{DIVERGED_LABEL}: the follower holds a proven record at seq 40 but this \
                 primary has GC'd every record below its retained base 50"
            ),
        };
        let (probe, _db) = probe_scripted(0x0DEB_0402, &refusal);
        match probe.verdict() {
            WitnessVerdict::IsPrimary(message) => assert!(message.contains(DIVERGED_LABEL)),
            other => panic!("expected IsPrimary, got {other:?}"),
        }
    }

    #[test]
    fn frames_classify_is_primary_without_touching_last_applied() {
        // A REAL WAL frame — read back from a separate source db's own log,
        // not a garbage payload. A garbage payload fails to decode
        // regardless of whether `step` wrongly falls through to apply it,
        // so it would prove nothing (verified by mutation: see the task 4
        // report). This frame is genuinely applicable, so a reintroduced
        // anti-adoption bug is actually caught here, not just asserted past.
        let mut source = fresh_db(0x0DEB_040A);
        source
            .transact(|tx| {
                tx.enqueue(Ping { n: 7 });
                Ok::<(), String>(())
            })
            .expect("committing one job");
        let seq = source.last_applied().expect("source committed one record");
        let raw = source
            .read_log(seq, 1 << 20)
            .expect("read back the real frame");

        let db = fresh_db(0x0DEB_040B);
        assert_eq!(db.last_applied(), None, "test needs a fresh local db");

        let frames = Msg::Frames { frames: raw };
        let net = OneShotNet::new(Box::new(ScriptedConn::new(&frames)));
        let mut probe = WitnessProbe::new("w:1".to_string(), None, None);
        probe.step(&net, &db, 0).expect("connect + send Hello");
        probe.step(&net, &db, 0).expect("recv + classify");
        match probe.verdict() {
            WitnessVerdict::IsPrimary(_) => {}
            other => panic!("expected IsPrimary, got {other:?}"),
        }
        assert_eq!(
            db.last_applied(),
            None,
            "anti-adoption: the probe must never apply a witness's shipped frames"
        );
    }

    #[test]
    fn a_peer_secret_refusal_classifies_inconclusive() {
        let refusal = Msg::Refuse {
            message: PEER_SECRET_REFUSAL.to_string(),
        };
        let (probe, _db) = probe_scripted(0x0DEB_0404, &refusal);
        match probe.verdict() {
            WitnessVerdict::Inconclusive(message) => assert_eq!(message, PEER_SECRET_REFUSAL),
            other => panic!("expected Inconclusive, got {other:?}"),
        }
    }

    #[test]
    fn a_version_mismatch_refusal_classifies_inconclusive_via_the_default_arm() {
        // An inline format!, no shared const — this proves the DEFAULT arm
        // absorbs an unrecognized refusal, not that the classifier happens
        // to match a second const.
        let refusal = Msg::Refuse {
            message: format!(
                "replication protocol version mismatch: this node speaks protocol version \
                 {PROTO_VERSION}, but the peer's Hello declared version {}",
                PROTO_VERSION + 1
            ),
        };
        assert!(!refusal_message(&refusal).contains(NOT_A_PRIMARY_MARKER));
        assert!(!refusal_message(&refusal).contains(DIVERGED_LABEL));
        let (probe, _db) = probe_scripted(0x0DEB_0405, &refusal);
        match probe.verdict() {
            WitnessVerdict::Inconclusive(_) => {}
            other => panic!("expected Inconclusive, got {other:?}"),
        }
    }

    fn refusal_message(msg: &Msg) -> &str {
        match msg {
            Msg::Refuse { message } => message,
            _ => panic!("expected Refuse"),
        }
    }

    // ---------------------------------------------- the coupling pin

    /// The shared state behind `RecordingHandle` — an `Rc` so the test can
    /// inspect what a real `Session` sent after handing ownership of one
    /// handle to it.
    struct RecordingConn {
        inbound: RefCell<VecDeque<Vec<u8>>>,
        sent: RefCell<Vec<Vec<u8>>>,
    }
    impl RecordingConn {
        fn new() -> Rc<RecordingConn> {
            Rc::new(RecordingConn {
                inbound: RefCell::new(VecDeque::new()),
                sent: RefCell::new(Vec::new()),
            })
        }
    }

    /// A local newtype over `Rc<RecordingConn>`: `impl Conn for Rc<_>`
    /// directly is an orphan-rule violation (`Rc` is not `#[fundamental]`
    /// the way `Box` is), so this thin wrapper carries the impl instead.
    #[derive(Clone)]
    struct RecordingHandle(Rc<RecordingConn>);
    impl Conn for RecordingHandle {
        fn send(&self, data: &[u8]) -> io::Result<()> {
            self.0.sent.borrow_mut().push(data.to_vec());
            Ok(())
        }
        fn recv(&self, out: &mut [u8]) -> io::Result<Option<usize>> {
            match self.0.inbound.borrow_mut().pop_front() {
                Some(bytes) => {
                    out[..bytes.len()].copy_from_slice(&bytes);
                    Ok(Some(bytes.len()))
                }
                None => Ok(None),
            }
        }
        fn peer_addr(&self) -> String {
            "test".to_string()
        }
    }

    /// Drives ONE real `Session::service` call against `db`/`role` with
    /// `hello` as the inbound message, and returns whatever the session
    /// actually sent back — the real `decide_hello` output, not a copy of
    /// its prose.
    fn real_reply(db: &Db, role: &Role, hello: &Msg) -> Msg {
        let conn = RecordingConn::new();
        conn.inbound.borrow_mut().push_back(encode(hello));
        let handle = RecordingHandle(conn.clone());
        let mut session = Session::new(Box::new(handle), 0, 0);
        session.drain();
        session
            .service(db, 0, role, &None)
            .expect("service must not IO-error against an in-memory conn");
        let sent = conn
            .sent
            .borrow_mut()
            .pop()
            .expect("the session must have replied to the Hello");
        decode(&sent).expect("the session's reply must decode")
    }

    // Structural keying (task 4 brief): the classifier keys on
    // NOT_A_PRIMARY_MARKER/DIVERGED_LABEL, the SAME consts `decide_hello`
    // builds its refusals from. This test proves the coupling end-to-end —
    // driving the real producer, not a hand-copied string — for the
    // not-a-primary refusal, a real diverged (ahead) refusal, and a real
    // prefix refusal (version mismatch) that must fall through to the
    // default arm rather than accidentally matching either marker.
    #[test]
    fn classifier_recognizes_the_real_shipped_refusal_strings() {
        // Not-a-primary: a real Follower-role Db + Session.
        let follower_db = fresh_db(0x0DEB_0406);
        let hello = Msg::Hello {
            proto: PROTO_VERSION,
            listen: None,
            position: None,
            secret: None,
            schema_digest: follower_db.schema_digest(),
        };
        let role = Role::Follower {
            primary: "p:1".to_string(),
        };
        let reply = real_reply(&follower_db, &role, &hello);
        assert_eq!(classify(&reply), WitnessVerdict::NotPrimary);

        // Diverged (ahead): a real Primary-role Db with one committed
        // record, Hello'd with a position ahead of it.
        let primary_db = seeded_db(0x0DEB_0407);
        let last = primary_db
            .last_applied()
            .expect("seeded_db commits one record");
        let hello = Msg::Hello {
            proto: PROTO_VERSION,
            listen: None,
            position: Some((last + 50, 0xdead_beef)),
            secret: None,
            schema_digest: primary_db.schema_digest(),
        };
        let reply = real_reply(&primary_db, &Role::Primary, &hello);
        match classify(&reply) {
            WitnessVerdict::IsPrimary(message) => assert!(message.contains(DIVERGED_LABEL)),
            other => panic!("expected IsPrimary, got {other:?}"),
        }

        // A real prefix refusal (version mismatch — an inline format!, no
        // shared const) must still classify Inconclusive, not accidentally
        // match either marker.
        let other_db = fresh_db(0x0DEB_0408);
        let hello = Msg::Hello {
            proto: PROTO_VERSION + 1,
            listen: None,
            position: None,
            secret: None,
            schema_digest: other_db.schema_digest(),
        };
        let reply = real_reply(&other_db, &Role::Primary, &hello);
        match classify(&reply) {
            WitnessVerdict::Inconclusive(_) => {}
            other => panic!("expected Inconclusive, got {other:?}"),
        }
    }
}
