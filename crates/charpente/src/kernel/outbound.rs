//! The kernel's outbound bookkeeping (outbound-effects spec §4): the
//! in-flight table, the per-process token counter (R7), the starts a job
//! batch buffers until its group commits (R6), and the completions drained
//! from `Outbound::poll`. Volatile by design: a crash, a restart or losing
//! writability drops it, and the store's Pending rows re-dispatch with the
//! same key (§7). Owned by the `Dispatcher`, beside the due-sets it must stay
//! disjoint from (§4 invariant 2).

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::io;
use std::time::Duration;

use charpente_core::io::{OutboundCall, OutboundReply, WallTime};

pub(crate) struct Flight {
    pub id: u64,
    /// The `due_at` it was dispatched at — where it goes back if its start
    /// is abandoned.
    pub due_at: WallTime,
    pub deadline: u64,
    pub host: &'static str,
    pub timeout: Duration,
}

pub(crate) struct InFlight {
    flights: BTreeMap<u64, Flight>,
    jobs: BTreeSet<u64>,
    deadlines: BTreeSet<(u64, u64)>,
    next_token: u64,
    max: usize,
    starts: Vec<(u64, OutboundCall)>,
    completions: VecDeque<(u64, io::Result<OutboundReply>)>,
}

impl InFlight {
    pub(crate) fn new(max: usize) -> InFlight {
        InFlight {
            flights: BTreeMap::new(),
            jobs: BTreeSet::new(),
            deadlines: BTreeSet::new(),
            next_token: 0,
            max,
            starts: Vec::new(),
            completions: VecDeque::new(),
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.flights.len()
    }

    pub(crate) fn has_room(&self) -> bool {
        self.flights.len() < self.max
    }

    pub(crate) fn holds_job(&self, id: u64) -> bool {
        self.jobs.contains(&id)
    }

    /// Records a decided call and buffers its start until the batch commits
    /// (R6). The token is fresh: never reused within a process (invariant 1).
    pub(crate) fn begin(
        &mut self,
        id: u64,
        due_at: WallTime,
        now: u64,
        host: &'static str,
        timeout: Duration,
        call: OutboundCall,
    ) -> u64 {
        assert!(
            self.has_room(),
            "dispatcher invariant: an outbound job was dispatched with the in-flight table full ({})",
            self.max
        );
        assert!(
            self.jobs.insert(id),
            "dispatcher invariant: job {id} began a call while already in flight (spec §4 invariant 2)"
        );
        let token = self.next_token;
        self.next_token += 1;
        let deadline = now.saturating_add(u64::try_from(timeout.as_nanos()).unwrap_or(u64::MAX));
        self.flights.insert(
            token,
            Flight {
                id,
                due_at,
                deadline,
                host,
                timeout,
            },
        );
        self.deadlines.insert((deadline, token));
        self.starts.push((token, call));
        token
    }

    pub(crate) fn take_starts(&mut self) -> Vec<(u64, OutboundCall)> {
        std::mem::take(&mut self.starts)
    }

    /// The batch's commit failed: its calls never start (R6, invariant 5).
    /// Returns each abandoned `(due_at, id)` for the caller to requeue.
    pub(crate) fn abandon_starts(&mut self) -> Vec<(WallTime, u64)> {
        let tokens: Vec<u64> = self.starts.drain(..).map(|(t, _)| t).collect();
        tokens
            .into_iter()
            .map(|t| {
                let f = self.finish(t).unwrap_or_else(|| {
                    panic!(
                        "dispatcher invariant: buffered start {t} has no flight — begin records \
                         both, and finish never runs while a start is buffered"
                    )
                });
                (f.due_at, f.id)
            })
            .collect()
    }

    /// Removes `token`'s flight. `None` for a token not in flight — a late
    /// completion, which the caller discards (invariant 4).
    pub(crate) fn finish(&mut self, token: u64) -> Option<Flight> {
        assert!(
            !self.starts.iter().any(|&(t, _)| t == token),
            "dispatcher invariant: token {token} finished while its start is still buffered — a \
             buffered start is issued after its batch commits or abandoned with it (R6), never \
             finished in between, or the remote would still get the call"
        );
        let f = self.flights.remove(&token)?;
        self.jobs.remove(&f.id);
        self.deadlines.remove(&(f.deadline, token));
        Some(f)
    }

    pub(crate) fn next_deadline(&self) -> Option<u64> {
        self.deadlines.first().map(|&(d, _)| d)
    }

    pub(crate) fn expired(&self, now: u64) -> Option<u64> {
        self.deadlines
            .first()
            .filter(|&&(d, _)| d <= now)
            .map(|&(_, t)| t)
    }

    pub(crate) fn push_completion(&mut self, c: (u64, io::Result<OutboundReply>)) {
        self.completions.push_back(c);
    }

    pub(crate) fn pop_completion(&mut self) -> Option<(u64, io::Result<OutboundReply>)> {
        self.completions.pop_front()
    }

    pub(crate) fn has_completions(&self) -> bool {
        !self.completions.is_empty()
    }

    /// The node is not writable (invariant 3): every flight and buffered
    /// completion is dropped. Returns the tokens to cancel and how many
    /// buffered completions were discarded. Their jobs stay Pending in the
    /// store for the next primary, with the same key (§7).
    pub(crate) fn drop_all(&mut self) -> (Vec<u64>, u64) {
        assert!(
            self.starts.is_empty(),
            "dispatcher invariant: starts buffered across a step boundary"
        );
        self.jobs.clear();
        self.deadlines.clear();
        let discarded = self.completions.drain(..).count() as u64;
        (
            std::mem::take(&mut self.flights).into_keys().collect(),
            discarded,
        )
    }
}

/// Spec §7: an attempt that may have reached the remote says so. Only
/// `ConnectionRefused` is "certainly not delivered" (io.rs's contract).
pub(crate) fn transport_failure(host: &str, e: &io::Error) -> String {
    if e.kind() == io::ErrorKind::ConnectionRefused {
        format!("outbound {host}: {e} — the call did not reach the remote")
    } else {
        format!(
            "outbound {host}: {e} — the remote may have applied this call; the retry carries the \
             same idempotency key"
        )
    }
}

pub(crate) fn timed_out(host: &str, timeout: Duration) -> String {
    format!(
        "outbound {host}: timed out after {timeout:?} — the remote may have applied this call; \
         the retry carries the same idempotency key"
    )
}

/// Plan amendment A10.
pub(crate) fn reply_over_cap(host: &str, cap: usize) -> String {
    format!(
        "outbound {host}: the reply exceeded the {cap}-byte reply cap — the remote \
         answered, so it applied this call; raise the cap with Host::max_reply_bytes (the retry \
         carries the same idempotency key)"
    )
}
