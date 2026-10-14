// `outbound` names the host it calls (outbound-effects spec §5): the bare word,
// with no `= "<host>"`, is refused with the accepted forms.
use charpente::job;

#[job(global, outbound)]
struct Charge {}

fn main() {}
