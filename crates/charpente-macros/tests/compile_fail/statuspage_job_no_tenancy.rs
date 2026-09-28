// The statuspage oracle's job (job-tenancy spec, Context and §9 item 1),
// quoted from `6037704:src/notify.rs:10-17`: attribute, doc comment and
// fields verbatim. A job that acts inside one org, declared with a bare
// `#[job]`. It had no `Ctx`, so it opened its org with
// `open_scope(org, None)`, which is why that app's `authorize` admitted
// `None` for every caller. The bare form is now a refusal that names both
// answers. `run` is left out: a refused job emits no type for an `impl` to
// name.
use charpente::job;

#[job]
pub struct NotifySubscribers {
    pub org: u64,
    pub incident: u64,
    /// The id-counter stamp the resolve transaction drew: subscriptions with an id
    /// below it existed at resolve time; later ones did not.
    pub stamp: u64,
}

fn main() {}
