// The Wire trait's #[diagnostic::on_unimplemented] tells a reader who hits
// the bare-u64/i64 rejection to reach for `Id<T>` for a row id, or
// `WireU64`/`WireI64` to carry a bare 64-bit value as a string. This fixture
// exercises all three prescribed remedies against a real `Wire` bound so
// that if any one of them stops implementing `Wire`, this goes red — the
// diagnostic can never silently start lying again.
use charpente::wire::{Wire, WireI64, WireU64};
use charpente::{Id, table};

#[table]
struct SomeRow {
    id: Id<SomeRow>,
}

fn needs_wire<T: Wire>() {}

fn main() {
    needs_wire::<Id<SomeRow>>();
    needs_wire::<WireU64>();
    needs_wire::<WireI64>();
}
