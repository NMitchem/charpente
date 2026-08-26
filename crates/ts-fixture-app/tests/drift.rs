//! The `emits(...)` drift oracle (TS-codegen spec §5), run against a REAL
//! app rather than a test-only one.
//!
//! `crates/charpente/tests/describe.rs` already carries this check over its
//! own in-file fixture; this one exists because the whole point of
//! `ts-fixture-app` is that a client is generated from it and compiled. A
//! declaration that drifted from the handler here would put a `FeedEvent`
//! union in front of `tsc` naming events the route never sends — and `tsc`
//! cannot tell, because both halves of that lie are generated from the same
//! declaration.

use charpente::http::App;
use charpente_sim::{FaultConfig, Sim};

fn fixture_app() -> App {
    let sim = Sim::with_faults(0x7357_0011, FaultConfig::none());
    ts_fixture_app::build_app(Box::new(sim.disk()), Box::new(sim.entropy()))
        .expect("the fixture app must build under FaultConfig::none()")
}

/// Order-sensitive, like the charpente-side oracle: declaration order is the
/// order the generated union's arms appear in, so `emits(B, A)` against
/// `of::<A>().and::<B>()` is a real mismatch.
///
/// The floor assertion is what keeps this from passing vacuously — an
/// undeclared stream route is SKIPPED (`emits(...)` is optional, and `[]`
/// means "not declared", not "declares nothing"), so without the floor an
/// app that declared none would sail through an empty loop.
#[test]
fn the_declared_emits_set_matches_the_handlers_subscription() {
    let mut app = fixture_app();
    let declared: Vec<(&'static str, Vec<&'static str>)> = app
        .describe()
        .routes
        .iter()
        .filter(|r| r.stream && !r.emits.is_empty())
        .map(|r| (r.name, r.emits.iter().map(|e| e.event).collect()))
        .collect();
    assert_eq!(
        declared.len(),
        1,
        "the floor: this fixture has exactly one stream route with a declared \
         emits(...) (`feed`; `raw_feed` declares none on purpose), so an empty \
         loop below would be a vacuous pass — got {declared:?}"
    );
    for (name, emits) in declared {
        let actual = app
            .subscription_event_names(name)
            .expect("a stream route must expose its subscription's event names");
        assert_eq!(
            emits, actual,
            "route `{name}` declares emits{emits:?} but its Subscription names {actual:?}"
        );
    }
}
