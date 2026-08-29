// Positive control for wire_bare_u64.rs / wire_bare_i64.rs: same shape, but
// with u32/i32 (which ARE wire primitives). Without this, the two
// compile-fail fixtures would stay green even if their `needs_wire` bound
// were broken in some unrelated way (bad import, wrong trait, a typo) — a
// compile-fail fixture only proves the code fails to compile, not that it
// fails for the stated reason. This proves the u64/i64 rejection is caused
// specifically by their size, not by `needs_wire` itself.
fn main() {
    fn needs_wire<T: charpente::wire::Wire>() {}
    needs_wire::<u32>();
    needs_wire::<i32>();
}
