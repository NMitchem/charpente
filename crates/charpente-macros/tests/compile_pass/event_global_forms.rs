// `#[event(global)]` (tenancy-completion spec §6, amendment A1): an event's
// global tenancy, written once, with an optional trailing comma. `global`
// changes nothing the fingerprint hashes: a global event's is the bare field
// fold, FNV-1a over `"n\0WireU64\0"`. That is the value a bare `#[event]`
// computed before this spec refused the bare form (`event_bare.rs`).
// trybuild runs `main`, so its assertions are part of the pin.
use charpente::wire::WireU64;
use charpente::{Event, event};

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
        0x9caf_3f93_9b10_65e1,
        "`global` changes nothing the fingerprint hashes"
    );
    assert_eq!(<TrailingComma as Event>::FINGERPRINT, 0x9caf_3f93_9b10_65e1);
}
