fn main() {
    fn needs_wire<T: charpente::wire::Wire>() {}
    needs_wire::<u64>();
}
