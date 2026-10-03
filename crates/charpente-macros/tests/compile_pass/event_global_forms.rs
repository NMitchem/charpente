// `#[event(global)]` (tenancy-completion spec §6, amendment A1): the one
// tenancy word an event takes today, at most once, with an optional trailing
// comma. It expands to nothing: a `global` event is what a bare `#[event]`
// was, so its NAME, its fingerprint and its stored bytes are unchanged.
// trybuild runs `main`, so its assertions are part of the pin.
use charpente::wire::WireU64;
use charpente::{Event, event};

#[event]
struct Bare {
    n: WireU64,
}

#[event(global)]
struct Declared {
    n: WireU64,
}

#[event(global,)]
struct TrailingComma {
    n: WireU64,
}

fn main() {
    assert_eq!(Declared::NAME, "declared");
    assert_eq!(TrailingComma::NAME, "trailing_comma");
    assert_eq!(
        <Declared as Event>::FINGERPRINT,
        <Bare as Event>::FINGERPRINT,
        "`global` changes nothing the fingerprint hashes"
    );
    assert_eq!(
        <TrailingComma as Event>::FINGERPRINT,
        <Bare as Event>::FINGERPRINT
    );
}
