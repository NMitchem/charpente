// `on_dead` and `retain_dead` answer one question, so naming both is refused
// at whichever came second, in either order — never one silently winning.
use charpente::job;

#[job(global, on_dead, retain_dead)]
struct HookFirst {}

#[job(global, retain_dead, on_dead)]
struct RetainFirst {}

fn main() {}
