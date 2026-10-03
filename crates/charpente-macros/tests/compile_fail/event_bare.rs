// Neither `global` nor `scoped_by` (tenancy-completion spec §6, DECISIONS
// §13): an event's tenancy is a required choice, and absence is a compile
// error naming both answers. Spanned at the attribute, as `#[job]`'s and
// `#[table]`'s refusals are. `WireU64` is named by path because a refused
// struct is not emitted, so an import of it would sit unused in the pin.
use charpente::event;

#[event]
struct NoteAdded {
    note: charpente::wire::WireU64,
}

fn main() {}
