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
//! A response this node cannot decode at all takes the same `Inconclusive`
//! route, one step earlier — it never reaches `classify`. It is the absence
//! of an answer, not an answer, so it files with unreachable exactly as spec
//! §2's trichotomy says: back off and ask again. Making it an error instead
//! would hand anything squatting on a witness's address a way to take a
//! resurrected primary down.
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
pub(crate) enum WitnessVerdict {
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
pub(crate) struct WitnessProbe {
    addr: String,
    my_listen: Option<String>,
    secret: Option<String>,
    conn: Option<Box<dyn Conn>>,
    recv_buf: Option<Vec<u8>>,
    /// The witness's first response, moved off the wire by `drain` and
    /// waiting for `step` to classify it.
    pending: Option<Vec<u8>>,
    /// Reconnect backoff after a failed connect/send, mirroring `Follower`'s
    /// pacing (`RECONNECT_NANOS`) rather than busy-looping the caller.
    reconnect_at: Option<u64>,
    verdict: WitnessVerdict,
}

impl WitnessProbe {
    pub(crate) fn new(
        addr: String,
        my_listen: Option<String>,
        secret: Option<String>,
    ) -> WitnessProbe {
        WitnessProbe {
            addr,
            my_listen,
            secret,
            conn: None,
            recv_buf: None,
            pending: None,
            reconnect_at: None,
            verdict: WitnessVerdict::Pending,
        }
    }

    pub(crate) fn verdict(&self) -> &WitnessVerdict {
        &self.verdict
    }

    /// The witness this probe is questioning — the caller names it in both
    /// the refusal and the deadline notice.
    pub(crate) fn addr(&self) -> &str {
        &self.addr
    }

    /// Non-destructive IO movement (`Repl::pump_io`), the same split
    /// `Follower::drain`/`Session::drain` use: move the witness's first
    /// response off the wire into `pending`; an EOF or recv error drops the
    /// conn and schedules a reconnect. No `Db` access, so `pump_io` — which
    /// has none — can call it.
    ///
    /// The split is load-bearing, not tidiness. A probe holds its conn open
    /// while the witness thinks (`recv` → `Ok(None)`), and the only way for a
    /// `has_work`-driven caller to notice a reply that arrives later would be
    /// to report work for an idle open conn — a kernel that never parks, and
    /// under the simulator's scheduler a universe whose virtual time never
    /// advances, so the reply it is spinning for can never be delivered.
    /// Polling here instead keeps `has_work` honest: work means a message to
    /// classify or a backoff that has come due, never "still waiting".
    pub(crate) fn drain(&mut self, now: u64) {
        if self.conn.is_none() || self.pending.is_some() {
            return;
        }
        // `decided`, not `!Pending` — and this one is the load-bearing copy.
        // `drain` is the ONLY writer of `self.pending`, which is the only
        // thing `step` classifies from, so gating it on `Pending` strands an
        // `Inconclusive` probe one step further along than gating the others
        // would: it reconnects, then never reads the answer, and holds an
        // open conn nobody will ever drain or close.
        if self.decided() {
            return;
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
            }
            Ok(None) => {} // nothing yet — stays Pending, conn stays open
            Ok(Some(n)) => self.pending = Some(buf[..n].to_vec()),
        }
    }

    /// True when this probe has learned something the gate can act on, and so
    /// will never speak again. **`Inconclusive` is deliberately NOT decided**:
    /// it is the verdict meaning "I learned nothing", absorbing
    /// `decide_hello`'s whole prefix ladder (peer secret, protocol version,
    /// schema digest, invalid listen), and spec §2's trichotomy files
    /// "unknown" with unreachable — stay write-stopped, back off, retry.
    /// Treating it as decided strands the gate shut forever on an ordinary
    /// rolling-upgrade skew, and makes the notice's and the 503's promise
    /// that "writes resume the moment every witness answers" a lie.
    ///
    /// One consequence of keeping the verdict across retries rather than
    /// resetting it to `Pending`: the reason an operator sees is the last one
    /// **received**, not necessarily the current truth. A witness that answers
    /// `Inconclusive` and then goes down keeps reporting that answer, because
    /// a failed reconnect has nothing to overwrite it with. A retry that
    /// reaches the witness always overwrites, including with a different
    /// inconclusive reason.
    fn decided(&self) -> bool {
        matches!(
            self.verdict,
            WitnessVerdict::NotPrimary | WitnessVerdict::IsPrimary(_)
        )
    }

    /// True when `step` has something to do: a drained response to classify,
    /// or a due (re)connect. A decided probe and a probe waiting on an open
    /// conn both report `false` — see `drain`.
    pub(crate) fn has_work(&self, now: u64) -> bool {
        if self.decided() {
            return false;
        }
        if self.pending.is_some() {
            return true;
        }
        self.conn.is_none() && self.reconnect_at.is_none_or(|t| now >= t)
    }

    /// The reconnect backoff this probe wants to wake for, so a caller's
    /// scheduler advances to it instead of stalling (`Follower::next_deadline`
    /// one for one).
    pub(crate) fn next_deadline(&self) -> Option<u64> {
        if !self.decided() && self.conn.is_none() {
            self.reconnect_at
        } else {
            None
        }
    }

    /// One unit of work: classify the drained response, else connect +
    /// `Hello` when disconnected and due. A *decided* verdict is terminal
    /// (the conn is already dropped) — later calls are a no-op, so the caller
    /// can keep stepping every probe uniformly without tracking which ones
    /// are already decided. `Inconclusive` is not decided; see `decided`.
    pub(crate) fn step(&mut self, net: &dyn Net, db: &Db, now: u64) -> io::Result<()> {
        if self.decided() {
            return Ok(());
        }
        let Some(raw) = self.pending.take() else {
            if self.conn.is_none() && self.reconnect_at.is_none_or(|t| now >= t) {
                return self.connect_and_hello(net, db, now);
            }
            return Ok(()); // backoff not yet due, or connected and waiting
        };
        // Undecodable is not a verdict — it is the absence of one, and spec
        // §2's trichotomy files "unknown" with unreachable: back off and ask
        // again. Propagating it instead (the bare `?` this replaced) let
        // anything holding a witness's address take a resurrected primary
        // down with a few bytes of garbage, and reported it as a decode
        // fault rather than as what the operator needs to act on — which
        // witness, and that the gate is still shut because of it. Bounded
        // and escaped like every other peer-derived payload here: the text
        // quotes the wire's own tag back.
        let msg = match decode(&raw) {
            Ok(msg) => msg,
            Err(e) => {
                self.verdict = WitnessVerdict::Inconclusive(bounded_witness_message(&format!(
                    "undecodable response: {e}"
                )));
                self.conn = None;
                self.reconnect_at = Some(now + RECONNECT_NANOS);
                return Ok(());
            }
        };
        self.verdict = classify(&msg);
        // Classify and disconnect: never fall through to apply anything (the
        // module doc's "never adopts").
        self.conn = None;
        // An undecided verdict re-arms the backoff, so the witness is asked
        // again on the same pacing a failed connect uses. The verdict is kept
        // rather than reset to `Pending` so the operator notice can say WHY
        // this witness is still unresolved; `decided` is what makes it
        // retryable, not the variant itself.
        if !self.decided() {
            self.reconnect_at = Some(now + RECONNECT_NANOS);
        }
        Ok(())
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

/// The bound on a witness's `Refuse` message this probe retains (mirrors
/// `session.rs`'s `ECHOED_LISTEN_ADDRESS_BYTES` — the same class of trust
/// boundary, one hop further out): `Msg::Refuse.message` decodes off the
/// wire bounded only by `MAX_MESSAGE_BYTES` (17 MiB), and Task 5 is
/// specified to surface a witness's message in a loud operator notice, so a
/// hostile or buggy witness could otherwise hand back up to ~17 MiB of
/// arbitrary UTF-8 for this probe to retain and echo. Sized generously above
/// every message the shipped refusal builders actually produce today (the
/// longest, `diverged_behind`, is ~730 bytes; `invalid_listen_refusal`'s
/// worst case with a maximally-long echoed address is ~470) — a bound that
/// would truncate legitimate operator-facing prose defeats hard rule 5's
/// "names the contract, the site, and the fix", so this is a hostile-input
/// bound, not a tight one.
const ECHOED_WITNESS_MESSAGE_BYTES: usize = 1024;

/// Bounds and Debug-escapes a witness's `Refuse` message before this probe
/// retains it (hard rule 4: never trust peer-supplied bytes into an
/// operator-facing string unescaped and unbounded). Truncates on a UTF-8
/// char boundary — never mid-character — appending a literal "..." marker
/// before escaping, then Debug-formats the whole thing: this both escapes
/// control characters/ANSI/CR-LF a hostile witness could plant and bounds
/// the size, mirroring `invalid_listen_refusal`'s truncation shape
/// (`session.rs`) exactly.
fn bounded_witness_message(message: &str) -> String {
    let mut cut = message.len().min(ECHOED_WITNESS_MESSAGE_BYTES);
    while cut > 0 && !message.is_char_boundary(cut) {
        cut -= 1;
    }
    let shown = if cut < message.len() {
        format!("{}...", &message[..cut])
    } else {
        message.to_string()
    };
    format!("{shown:?}")
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
            WitnessVerdict::IsPrimary(bounded_witness_message(message))
        }
        Msg::Refuse { message } => WitnessVerdict::Inconclusive(bounded_witness_message(message)),
        // Only a witness that passed the role match ever sends a non-Refuse
        // message — HandingOff included (module doc). `other.name()` is a
        // fixed message-KIND label off `Msg`'s own enum (protocol.rs), never
        // peer-controlled bytes, so this string needs no bounding/escaping —
        // unlike the two arms above, nothing here reads off the wire.
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
    use crate::repl::session::{PEER_SECRET_REFUSAL, SEED_WHILE_VERIFYING_REFUSAL, Session};
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

    /// A `Net` handing out a scripted sequence of conns, one per `connect` —
    /// needed to drive a probe across a RETRY, which `OneShotNet` cannot do.
    struct SequencedNet(RefCell<VecDeque<Box<dyn Conn>>>);
    impl SequencedNet {
        fn new(conns: Vec<Box<dyn Conn>>) -> SequencedNet {
            SequencedNet(RefCell::new(conns.into()))
        }
    }
    impl Net for SequencedNet {
        fn listen(&self, _addr: &str) -> io::Result<Box<dyn Listener>> {
            unreachable!("WitnessProbe never listens")
        }
        fn connect(&self, _addr: &str) -> io::Result<Box<dyn Conn>> {
            self.0
                .borrow_mut()
                .pop_front()
                .ok_or_else(|| io::Error::other("SequencedNet: connect beyond the script"))
        }
    }

    /// Drives a `WitnessProbe` against one scripted response: connects (step
    /// 1), pumps the reply off the wire (`drain`, `Repl::pump_io`'s half),
    /// then classifies it (step 2).
    fn probe_scripted(seed: u64, response: &Msg) -> (WitnessProbe, Db) {
        let db = fresh_db(seed);
        let net = OneShotNet::new(Box::new(ScriptedConn::new(response)));
        let mut probe = WitnessProbe::new("w:1".to_string(), None, None);
        probe.step(&net, &db, 0).expect("connect + send Hello");
        probe.drain(0);
        probe.step(&net, &db, 0).expect("classify");
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
        probe.drain(0);
        probe.step(&net, &db, 0).expect("classify");
        match probe.verdict() {
            // Fix round 1, I-2: pinned byte-exact — this is the one
            // operator-facing string `classify`'s default (non-Refuse) arm
            // authors itself, with no other occurrence in the tree to pin
            // it. "Frames" is `Msg::name()`'s fixed message-KIND label, not
            // peer-controlled bytes, so it needs no bounding/escaping.
            WitnessVerdict::IsPrimary(message) => assert_eq!(
                message,
                "witness answered Frames — only a node past the role match (primary, or \
                 handing off) ever sends one"
            ),
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
            // Bounded/Debug-escaped (fix round 1, I-1): PEER_SECRET_REFUSAL is
            // well under ECHOED_WITNESS_MESSAGE_BYTES and carries nothing
            // Debug needs to escape, so the stored form is exactly the
            // quoted original — `format!("{:?}", ...)`, not raw equality.
            WitnessVerdict::Inconclusive(message) => {
                assert_eq!(message, &format!("{PEER_SECRET_REFUSAL:?}"))
            }
            other => panic!("expected Inconclusive, got {other:?}"),
        }
    }

    /// An `Inconclusive` verdict must NOT strand the gate. It is the verdict
    /// that means "I learned nothing" — it absorbs `decide_hello`'s whole
    /// prefix ladder (peer secret, protocol version, **schema digest**,
    /// invalid listen), every entry of which is a transient misconfiguration
    /// an operator fixes. Spec §2's trichotomy files "unknown" with
    /// unreachable: *stay write-stopped, back off, retry*.
    ///
    /// Making it terminal stranded a restarted primary permanently and
    /// silently — the gate could never reopen even after the operator fixed
    /// the skew — while three shipped strings promised writes resume the
    /// moment the witness answers. Schema-digest skew is the ordinary
    /// rolling-upgrade window, and this sub-project's own restart-order rule
    /// (followers first) is what walks an operator into it.
    #[test]
    fn an_inconclusive_witness_is_retried_not_stranded() {
        let db = fresh_db(0x0DEB_0405);
        // Round 1 answers with a misconfiguration (peer-secret refusal);
        // round 2 is the same witness after an operator fixed it.
        let net = SequencedNet::new(vec![
            Box::new(ScriptedConn::new(&Msg::Refuse {
                message: PEER_SECRET_REFUSAL.to_string(),
            })),
            Box::new(ScriptedConn::new(&Msg::Refuse {
                message: format!("this node is {NOT_A_PRIMARY_MARKER} (it is a follower of p:1)"),
            })),
        ]);
        let mut probe = WitnessProbe::new("w:1".to_string(), None, None);

        probe.step(&net, &db, 0).expect("connect + send Hello");
        probe.drain(0);
        probe.step(&net, &db, 0).expect("classify");
        assert!(
            matches!(probe.verdict(), WitnessVerdict::Inconclusive(_)),
            "precondition: this refusal classifies Inconclusive"
        );
        assert!(
            probe.next_deadline().is_some(),
            "an inconclusive probe must schedule a retry, or the caller's scheduler parks \
             forever and the gate never reopens"
        );
        assert!(
            !probe.has_work(0),
            "the backoff is not yet due at the instant it was armed"
        );
        assert!(
            probe.has_work(RECONNECT_NANOS),
            "once the backoff comes due the probe must report work"
        );

        // THE PART THAT MATTERS, and the part an earlier version of this test
        // stopped one call short of: the retry must COMPLETE. A probe that
        // reconnects but never drains the answer is stranded just as
        // thoroughly as one that never reconnects — worse, actually, since it
        // also holds an undrained conn open forever. Gating `drain` on
        // `Pending` rather than `decided` produced exactly that, and every
        // assertion above stayed green while it did.
        let t = RECONNECT_NANOS;
        probe.step(&net, &db, t).expect("reconnect + send Hello");
        probe.drain(t);
        assert!(
            probe.has_work(t),
            "STRANDED: the second answer was never moved off the wire, so the probe reports no \
             work and no deadline and the gate can never reopen"
        );
        probe
            .step(&net, &db, t)
            .expect("classify the second answer");
        assert_eq!(
            *probe.verdict(),
            WitnessVerdict::NotPrimary,
            "the retry must reach a real verdict — this is what makes the notice's and the \
             503's promise that writes resume once every witness answers actually true"
        );
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

    /// The busy-spin guard (task 5, C-1). A probe that has connected and is
    /// waiting for its witness to answer must report NO work: reporting work
    /// there would make a `has_work`-driven caller step it forever without
    /// parking, and under the simulator's scheduler — which advances virtual
    /// time only when nothing is runnable — the very reply it spins for could
    /// never be delivered. Work appears when `drain` has something, and
    /// disappears for good once the verdict is in.
    #[test]
    fn work_appears_only_for_a_drained_reply_or_a_due_reconnect() {
        let db = fresh_db(0x0DEB_0409);
        let conn = RecordingConn::new();
        let net = OneShotNet::new(Box::new(RecordingHandle(conn.clone())));
        let mut probe = WitnessProbe::new("w:1".to_string(), None, None);

        assert!(probe.has_work(0), "a fresh probe must connect");
        probe.step(&net, &db, 0).expect("connect + send Hello");
        probe.drain(0);
        assert!(
            !probe.has_work(0),
            "a connected probe still awaiting its witness's reply must report no work"
        );
        assert_eq!(
            probe.next_deadline(),
            None,
            "and has no timer to wake for — the reply is the network's to deliver"
        );

        // The witness answers.
        conn.inbound.borrow_mut().push_back(encode(&Msg::Refuse {
            message: format!("this node is {NOT_A_PRIMARY_MARKER} (it is a follower of p:1)"),
        }));
        probe.drain(0);
        assert!(probe.has_work(0), "a drained reply is work");
        probe.step(&net, &db, 0).expect("classify");
        assert_eq!(*probe.verdict(), WitnessVerdict::NotPrimary);
        assert!(
            !probe.has_work(0) && probe.next_deadline().is_none(),
            "a decided probe is inert — the caller may keep stepping every probe uniformly"
        );
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
    /// its prose. `witnesses` is this node's recorded witness set — `&[]` for
    /// every case here that does not care about it.
    fn real_reply(db: &Db, role: &Role, hello: &Msg, witnesses: &[String]) -> Msg {
        let conn = RecordingConn::new();
        conn.inbound.borrow_mut().push_back(encode(hello));
        let handle = RecordingHandle(conn.clone());
        let mut session = Session::new(Box::new(handle), 0, 0);
        session.drain();
        session
            .service(db, 0, role, &None, witnesses)
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
        let reply = real_reply(&follower_db, &role, &hello, &[]);
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
        let reply = real_reply(&primary_db, &Role::Primary, &hello, &[]);
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
        let reply = real_reply(&other_db, &Role::Primary, &hello, &[]);
        match classify(&reply) {
            WitnessVerdict::Inconclusive(_) => {}
            other => panic!("expected Inconclusive, got {other:?}"),
        }

        // The seed-while-verifying refusal (M2 interlude) must land on
        // classify's default arm. Load-bearing in BOTH directions: reading it
        // as NotPrimary would let a probe treat "I am also unsure" as "I am
        // satisfied" and open its gate on no evidence; IsPrimary would refuse
        // a healthy node over a phantom split-brain. Driven through a real
        // `decide_hello` call, like its three siblings above, now that Task 3
        // wires a production emitter: a verifying `Db`, Hello'd with an
        // address the (empty) witness set does not contain.
        let mut verifying_db = fresh_db(0x0DEB_040C);
        verifying_db.verify_writes(vec!["f:1".to_string()]);
        let hello = Msg::Hello {
            proto: PROTO_VERSION,
            listen: Some("g:1".to_string()),
            position: None,
            secret: None,
            schema_digest: verifying_db.schema_digest(),
        };
        let reply = real_reply(&verifying_db, &Role::Primary, &hello, &[]);
        assert_eq!(
            reply,
            Msg::Refuse {
                message: SEED_WHILE_VERIFYING_REFUSAL.to_string(),
            },
            "must be the real shipped refusal, byte-exact"
        );
        match classify(&reply) {
            WitnessVerdict::Inconclusive(_) => {}
            other => panic!("expected Inconclusive, got {other:?}"),
        }
    }
}
