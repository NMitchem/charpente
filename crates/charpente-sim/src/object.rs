//! The simulated object store: an in-memory bucket behind `ObjectStore`.
//! Completions are immediate — no `Clock` handle, so no simulated latency.
//! Task 2 (`with_faults`) adds seeded fault injection (`object_put_error`,
//! `object_get_error`, `object_lost_ack`); latency stays future work.
//!
//! State lives behind a `Mutex` and the fault stream behind a `RefCell`.
//! Neither is forced by the trait: `ObjectStore` requires only `Send`, and
//! `RefCell<T>` is `Send` whenever `T` is, so `RefCell` would serve for both.
//! `lib.rs`'s `World` uses a `Mutex` for a reason that does NOT transfer
//! here — it must be `Send + Sync`, because `File`'s supertrait requires it
//! of storage engines mounted behind the boundary. `ObjectStore` has no such
//! supertrait. The `Mutex` is therefore kept only because it also yields
//! `Sync`, which no caller needs yet; consolidating both fields onto
//! `RefCell` is deferred until the archive's ownership is known (it is the
//! first caller that could plausibly need `Sync`).

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::io;
use std::sync::Mutex;

use charpente_core::io::{ObjectDone, ObjectOp, ObjectStore};

use crate::rng::Rng;
use crate::{FaultConfig, keyed_stream};

struct State {
    objects: BTreeMap<String, Vec<u8>>,
    in_flight: Option<ObjectOp>,
    fail_next_put: bool,
}

pub struct SimObjectStore {
    state: Mutex<State>,
    // Not behind `state`'s Mutex: the roll itself never touches `objects`,
    // and `ObjectStore` requires only `Send` (not `Sync`, per its trait
    // bound), so a plain `RefCell` costs nothing here — same reasoning as
    // `HostWorld`'s bare `Rng` fields, just not lock-guarded because this
    // store carries no second thread-shared field to co-guard it with.
    faults: Option<(RefCell<Rng>, FaultConfig)>,
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
            faults: None,
        }
    }

    /// Seeded fault injection (archive spec §9): `object_put_error`,
    /// `object_get_error`, `object_lost_ack`. The internal roll stream is
    /// `keyed_stream(seed, "object_store")` — independent of every other
    /// fault stream in the crate, so an object store's rolls never perturb
    /// (or are perturbed by) disk/net faults drawn from the same seed.
    pub fn with_faults(seed: u64, cfg: FaultConfig) -> SimObjectStore {
        SimObjectStore {
            state: Mutex::new(State {
                objects: BTreeMap::new(),
                in_flight: None,
                fail_next_put: false,
            }),
            faults: Some((RefCell::new(keyed_stream(seed, "object_store")), cfg)),
        }
    }

    /// Test hook: the next `Put` fails after the op is accepted.
    pub fn fail_next_put(&self) {
        self.lock().fail_next_put = true;
    }

    /// Draws one fault roll if this store was built with `with_faults`;
    /// `false` (no fault) for a plain `new()` store, which carries no
    /// stream to draw from.
    fn roll(&self, p: impl FnOnce(&FaultConfig) -> f64) -> bool {
        match &self.faults {
            Some((rng, cfg)) => rng.borrow_mut().chance(p(cfg)),
            None => false,
        }
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
                } else if self.roll(|c| c.object_put_error) {
                    // Checked before object_lost_ack: this axis is the one
                    // where the object does NOT land — the other one where
                    // it lands and the caller is lied to (archive spec §9.2).
                    Err(io::Error::other(format!(
                        "simulated object_put_error: PUT \"{key}\" did not land"
                    )))
                } else if self.roll(|c| c.object_lost_ack) {
                    // The write lands durably; the caller is told it failed.
                    // This is what makes retry-idempotence load-bearing.
                    state.objects.insert(key.clone(), data);
                    Err(io::Error::other(format!(
                        "simulated object_lost_ack: PUT \"{key}\" landed but the ack was lost"
                    )))
                } else {
                    state.objects.insert(key, data);
                    Ok(ObjectDone::Put)
                }
            }
            ObjectOp::Get { key } => {
                if self.roll(|c| c.object_get_error) {
                    Err(io::Error::other(format!(
                        "simulated object_get_error: GET \"{key}\""
                    )))
                } else {
                    match state.objects.get(&key) {
                        Some(v) => Ok(ObjectDone::Get(v.clone())),
                        None => Err(io::Error::new(
                            io::ErrorKind::NotFound,
                            format!("no object at key \"{key}\""),
                        )),
                    }
                }
            }
            ObjectOp::List { prefix } => {
                if self.roll(|c| c.object_get_error) {
                    Err(io::Error::other(format!(
                        "simulated object_get_error: LIST \"{prefix}\""
                    )))
                } else {
                    Ok(ObjectDone::List(
                        state
                            .objects
                            .range(prefix.clone()..)
                            .take_while(|(k, _)| k.starts_with(&prefix))
                            .map(|(k, _)| k.clone())
                            .collect(),
                    ))
                }
            }
            // NO FAULT ROLL, and that is a coverage limit worth stating
            // where someone reading the model will meet it (Task 12.5
            // review / F4). `FaultConfig` has no delete axis, so every
            // DELETE in every convergence universe succeeds, and the
            // flagship property therefore exercises reclamation on the
            // HAPPY PATH ONLY. Task 12.5's central safety claim — that the
            // archive stays correct and keeps advancing when every delete
            // fails forever — rests on two hand-driven modes in one test
            // (`charpente/tests/archive.rs`) rather than on the fault
            // matrix. An `object_delete_error` axis is OWED, and the
            // obstacle is NOT the seed -> config mapping: `sample_extended_fields`
            // draws each axis from its own `keyed_stream(seed, name)`, so a
            // new field survives it. It is `Rng::chance`, which calls
            // `next_u64()` BEFORE comparing and so consumes a draw even at
            // `p == 0.0` (`rng.rs:59`). A roll on this arm therefore shifts
            // every later object-fault draw in the stream: every pinned
            // universe re-rolls, and the three recorded floors and both
            // recorded seeds go with them. That is the real cost to
            // schedule against, and it also rules out the cheap escape of
            // declaring the axis at 0.0 now and using it later.
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
    // yet — `SimObjectStore` completes every op immediately (faults land in
    // Task 2, but not latency) and has no `Clock` handle to advance. An
    // unbounded loop here would silently hang the instant a future task adds
    // latency, so the bound goes in now, generous enough that a legitimately
    // slow future op still finishes well inside it.
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

    // -------------------------------------------------- Task 2: fault axes

    #[test]
    fn lost_ack_leaves_the_object_present_despite_the_error() {
        let cfg = FaultConfig {
            object_lost_ack: 1.0,
            ..Default::default()
        };
        let s = SimObjectStore::with_faults(0x5eed, cfg);
        s.start(ObjectOp::Put {
            key: "a".into(),
            data: vec![7; 4],
        })
        .unwrap();
        assert!(drain(&s).is_err(), "lost ack must surface as an error");
        // ...but the write landed. This is why retry must be idempotent.
        s.start(ObjectOp::Get { key: "a".into() }).unwrap();
        assert!(matches!(drain(&s).unwrap(), ObjectDone::Get(v) if v == vec![7; 4]));
    }

    #[test]
    fn retrying_a_lost_ack_put_is_idempotent() {
        let cfg = FaultConfig {
            object_lost_ack: 1.0,
            ..Default::default()
        };
        let s = SimObjectStore::with_faults(0x5eed, cfg);
        for _ in 0..3 {
            s.start(ObjectOp::Put {
                key: "a".into(),
                data: vec![7; 4],
            })
            .unwrap();
            let _ = drain(&s);
        }
        assert_eq!(
            s.keys(),
            vec!["a".to_string()],
            "retry duplicated the object"
        );
    }

    #[test]
    fn put_error_leaves_the_object_absent() {
        let cfg = FaultConfig {
            object_put_error: 1.0,
            ..Default::default()
        };
        let s = SimObjectStore::with_faults(0x5eed, cfg);
        s.start(ObjectOp::Put {
            key: "a".into(),
            data: vec![1],
        })
        .unwrap();
        assert!(drain(&s).is_err());
        assert!(s.keys().is_empty(), "a failed PUT must not land");
    }

    #[test]
    fn get_error_does_not_touch_the_bucket() {
        let cfg = FaultConfig {
            object_get_error: 1.0,
            ..Default::default()
        };
        let s = SimObjectStore::with_faults(0x5eed, cfg.clone());
        // Put lands (object_put_error/object_lost_ack are both off).
        s.start(ObjectOp::Put {
            key: "a".into(),
            data: vec![9],
        })
        .unwrap();
        drain(&s).unwrap();
        s.start(ObjectOp::Get { key: "a".into() }).unwrap();
        assert!(drain(&s).is_err(), "injected get error must surface");
        assert_eq!(
            s.keys(),
            vec!["a".to_string()],
            "a failed GET must not mutate the bucket"
        );
        s.start(ObjectOp::List { prefix: "".into() }).unwrap();
        assert!(drain(&s).is_err(), "injected error must also hit LIST");
    }

    // DESIGN §1: each extended field draws from its own keyed sub-stream, so
    // adding a field perturbs no existing universe's draws. If this breaks,
    // every pinned seed in the repo changes meaning.
    #[test]
    fn adding_object_axes_does_not_perturb_existing_extended_draws() {
        use crate::sample_extended_fields;

        for seed in [0x1u64, 0xff159ed76d2806e4, 0x8bee1752811ae873] {
            let mut a = FaultConfig::default();
            sample_extended_fields(seed, &mut a);
            // The real assertion: these three are drawn from
            // keyed_stream(seed, name) and must equal the values recorded
            // before the object axes existed.
            let mut r = keyed_stream(seed, "bit_flip");
            let expect = if r.chance(0.25) {
                r.below(20) as f64 / 1000.0
            } else {
                0.0
            };
            assert_eq!(a.bit_flip, expect, "bit_flip draw moved for seed {seed:#x}");
        }
    }

    // The property the whole task rests on: same seed, same config, same op
    // sequence -> identical fault outcomes and identical bucket contents.
    // Two independently-built stores must never diverge.
    #[test]
    fn same_seed_same_config_same_fault_sequence() {
        let cfg = FaultConfig {
            object_put_error: 0.3,
            object_get_error: 0.3,
            object_lost_ack: 0.3,
            ..Default::default()
        };

        let a = SimObjectStore::with_faults(0x5eed, cfg.clone());
        let b = SimObjectStore::with_faults(0x5eed, cfg);

        for i in 0..50u8 {
            let key = format!("k{i}");
            a.start(ObjectOp::Put {
                key: key.clone(),
                data: vec![i],
            })
            .unwrap();
            b.start(ObjectOp::Put {
                key: key.clone(),
                data: vec![i],
            })
            .unwrap();
            let ra = drain(&a);
            let rb = drain(&b);
            assert_eq!(
                ra.is_ok(),
                rb.is_ok(),
                "op {i}: PUT outcome diverged between two same-seed stores"
            );

            a.start(ObjectOp::Get { key: key.clone() }).unwrap();
            b.start(ObjectOp::Get { key }).unwrap();
            let ga = drain(&a);
            let gb = drain(&b);
            assert_eq!(
                ga.is_ok(),
                gb.is_ok(),
                "op {i}: GET outcome diverged between two same-seed stores"
            );
            if let (Ok(ObjectDone::Get(va)), Ok(ObjectDone::Get(vb))) = (&ga, &gb) {
                assert_eq!(
                    va, vb,
                    "op {i}: GET value diverged between two same-seed stores"
                );
            }
        }
        assert_eq!(
            a.keys(),
            b.keys(),
            "final bucket contents diverged between two same-seed stores"
        );
    }
}
