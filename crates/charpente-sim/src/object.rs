//! The simulated object store: an in-memory bucket behind `ObjectStore`.
//! Completions are immediate — latency and faults arrive in Task 2.
//!
//! State lives behind one `Mutex`, not `RefCell` — same reasoning as the
//! rest of this crate (`lib.rs`'s `World`): the simulation is single-
//! threaded and the lock is never contended, but `ObjectStore: Send`
//! requires a handle that is honestly `Send`, and `Mutex<T>` gets that for
//! free with no `unsafe`.

use std::collections::BTreeMap;
use std::io;
use std::sync::Mutex;

use charpente_core::io::{ObjectDone, ObjectOp, ObjectStore};

struct State {
    objects: BTreeMap<String, Vec<u8>>,
    in_flight: Option<ObjectOp>,
    fail_next_put: bool,
}

pub struct SimObjectStore {
    state: Mutex<State>,
}

impl SimObjectStore {
    #[allow(clippy::new_without_default)]
    pub fn new() -> SimObjectStore {
        SimObjectStore {
            state: Mutex::new(State {
                objects: BTreeMap::new(),
                in_flight: None,
                fail_next_put: false,
            }),
        }
    }

    /// Test hook: the next `Put` fails after the op is accepted.
    pub fn fail_next_put(&self) {
        self.lock().fail_next_put = true;
    }

    /// Test-only inspection of the bucket's keys, sorted.
    pub fn keys(&self) -> Vec<String> {
        self.lock().objects.keys().cloned().collect()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        // Single-threaded: never contended; poison only follows a panic.
        self.state
            .lock()
            .expect("SimObjectStore lock poisoned by an earlier panic")
    }
}

impl ObjectStore for SimObjectStore {
    fn start(&self, op: ObjectOp) -> io::Result<()> {
        // Copy the flag and drop the guard before asserting: an assert that
        // fires while the guard is held poisons the mutex on unwind, turning
        // a caller-bug contract error into an unrelated "lock poisoned" one.
        let already_in_flight = self.lock().in_flight.is_some();
        assert!(
            !already_in_flight,
            "ObjectStore::start called with an operation already in flight — \
             poll to completion before starting another (archive spec §1)"
        );
        self.lock().in_flight = Some(op);
        Ok(())
    }

    fn poll(&self) -> Option<io::Result<ObjectDone>> {
        let mut state = self.lock();
        let op = state.in_flight.take()?;
        Some(match op {
            ObjectOp::Put { key, data } => {
                if std::mem::take(&mut state.fail_next_put) {
                    // The prior value stays: a PUT is atomic, never torn.
                    Err(io::Error::other(format!(
                        "simulated PUT failure for \"{key}\""
                    )))
                } else {
                    state.objects.insert(key, data);
                    Ok(ObjectDone::Put)
                }
            }
            ObjectOp::Get { key } => match state.objects.get(&key) {
                Some(v) => Ok(ObjectDone::Get(v.clone())),
                None => Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("no object at key \"{key}\""),
                )),
            },
            ObjectOp::List { prefix } => Ok(ObjectDone::List(
                state
                    .objects
                    .range(prefix.clone()..)
                    .take_while(|(k, _)| k.starts_with(&prefix))
                    .map(|(k, _)| k.clone())
                    .collect(),
            )),
            ObjectOp::Delete { key } => {
                state.objects.remove(&key);
                Ok(ObjectDone::Delete)
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use charpente_core::io::{ObjectDone, ObjectOp, ObjectStore};

    // Bounded, not an unbounded spin: precedent is `poll_accept` (lib.rs),
    // which advances virtual time between polls. This helper cannot do that
    // yet — Task 1's `SimObjectStore` completes every op immediately and has
    // no `Clock` handle to advance; that lands with Task 2's latency. An
    // unbounded loop here would silently hang the instant Task 2 adds it, so
    // the bound goes in now, generous enough that a legitimately slow future
    // op still finishes well inside it.
    fn drain(s: &SimObjectStore) -> std::io::Result<ObjectDone> {
        for _ in 0..10_000 {
            if let Some(r) = s.poll() {
                return r;
            }
        }
        panic!("drain: ObjectStore::poll returned None 10,000 times in a row — stuck in flight");
    }

    #[test]
    fn put_then_get_roundtrips() {
        let s = SimObjectStore::new();
        s.start(ObjectOp::Put {
            key: "a".into(),
            data: vec![1, 2, 3],
        })
        .unwrap();
        assert!(matches!(drain(&s).unwrap(), ObjectDone::Put));
        s.start(ObjectOp::Get { key: "a".into() }).unwrap();
        assert!(matches!(drain(&s).unwrap(), ObjectDone::Get(v) if v == vec![1, 2, 3]));
    }

    #[test]
    fn list_is_sorted_and_prefix_scoped() {
        let s = SimObjectStore::new();
        for k in ["p/b", "p/a", "q/c"] {
            s.start(ObjectOp::Put {
                key: k.into(),
                data: vec![],
            })
            .unwrap();
            drain(&s).unwrap();
        }
        s.start(ObjectOp::List {
            prefix: "p/".into(),
        })
        .unwrap();
        match drain(&s).unwrap() {
            ObjectDone::List(v) => assert_eq!(v, vec!["p/a".to_string(), "p/b".to_string()]),
            other => panic!("expected List, got {other:?}"),
        }
    }

    // The contract that distinguishes ObjectStore from Disk (spec §1): a PUT
    // is atomic. A failed PUT leaves the key absent or holding its PREVIOUS
    // value — never half. Disk's File tears; this must not.
    #[test]
    fn failed_put_leaves_previous_value_never_a_partial() {
        let s = SimObjectStore::new();
        s.start(ObjectOp::Put {
            key: "a".into(),
            data: vec![1; 8],
        })
        .unwrap();
        drain(&s).unwrap();
        s.fail_next_put();
        s.start(ObjectOp::Put {
            key: "a".into(),
            data: vec![2; 8],
        })
        .unwrap();
        assert!(drain(&s).is_err());
        s.start(ObjectOp::Get { key: "a".into() }).unwrap();
        match drain(&s).unwrap() {
            ObjectDone::Get(v) => assert_eq!(v, vec![1; 8], "torn or half-written object"),
            other => panic!("expected Get, got {other:?}"),
        }
    }

    // The other half of the atomic-PUT contract: a failed PUT to a key that
    // never existed must leave it absent, not present-and-empty.
    #[test]
    fn failed_put_to_a_fresh_key_leaves_it_absent() {
        let s = SimObjectStore::new();
        s.fail_next_put();
        s.start(ObjectOp::Put {
            key: "new".into(),
            data: vec![1, 2, 3],
        })
        .unwrap();
        assert!(drain(&s).is_err());
        s.start(ObjectOp::Get { key: "new".into() }).unwrap();
        let e = drain(&s).unwrap_err();
        assert_eq!(
            e.kind(),
            std::io::ErrorKind::NotFound,
            "a failed PUT must not create the key"
        );
    }

    #[test]
    #[should_panic(expected = "operation already in flight")]
    fn second_start_before_poll_asserts() {
        let s = SimObjectStore::new();
        s.start(ObjectOp::Get { key: "a".into() }).unwrap();
        let _ = s.start(ObjectOp::Get { key: "b".into() });
    }

    #[test]
    fn get_absent_key_is_not_found() {
        let s = SimObjectStore::new();
        s.start(ObjectOp::Get { key: "nope".into() }).unwrap();
        let e = drain(&s).unwrap_err();
        assert_eq!(e.kind(), std::io::ErrorKind::NotFound);
        assert!(
            e.to_string().contains("nope"),
            "error must name the key: {e}"
        );
    }

    #[test]
    fn delete_removes_the_object() {
        let s = SimObjectStore::new();
        s.start(ObjectOp::Put {
            key: "a".into(),
            data: vec![9],
        })
        .unwrap();
        drain(&s).unwrap();
        s.start(ObjectOp::Delete { key: "a".into() }).unwrap();
        assert!(matches!(drain(&s).unwrap(), ObjectDone::Delete));
        s.start(ObjectOp::Get { key: "a".into() }).unwrap();
        let e = drain(&s).unwrap_err();
        assert_eq!(
            e.kind(),
            std::io::ErrorKind::NotFound,
            "delete did not remove the object"
        );
    }
}
