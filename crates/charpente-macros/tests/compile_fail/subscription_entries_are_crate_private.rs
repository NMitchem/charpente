// A feed's entries are the platform's (tenancy-completion spec §6). Only
// `Subscription::of`/`and` and a tenant feed's `subscription`/`and` add one,
// each bounded by the event's declared tenancy. Were the list public, a
// handler admitted to one tenant could push an entry naming another tenant
// type's event, its NAME and FINGERPRINT copied from the type, and read
// that tenant's rows wherever the two types' raw ids coincide. Both doors
// are pinned shut: the entry type, and the field. The sibling of
// `principal_is_read_only_to_an_app.rs`. `SubEntry` is named by path, so a
// refused import cannot also sit in the pin as an unused one.
use charpente::http::Subscription;

fn widen(mut feed: Subscription, entry: charpente::http::SubEntry) -> Subscription {
    feed.entries.push(entry);
    feed
}

fn main() {
    let _ = widen;
}
