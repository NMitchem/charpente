// A feed's entries are the platform's (tenancy-completion spec §6). Only
// `Subscription::of`/`and` and a tenant feed's `subscription`/`and` add one,
// each bounded by the event's declared tenancy and checking in a `const`
// block that its FINGERPRINT is that tenancy's, which is what refuses a
// hand-written `impl Event` copying another tenant type's NAME and
// FINGERPRINT (forged_event_subscribed_in_another_tenant.rs). Were the list
// public, a handler admitted to one tenant could push an entry holding those
// two values by hand, which no type, bound or check stands behind, and read
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
