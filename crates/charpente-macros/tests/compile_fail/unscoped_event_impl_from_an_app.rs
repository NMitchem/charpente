// The event twin of `unscoped_job_impl_from_an_app.rs` (tenancy-completion
// spec §6). `UnscopedEvent` is the bound `Tx::emit`, `Tx::emit_keyed`,
// `Subscription::of` and `Subscription::and` actually name, so sealing
// `NoEventTenant` alone would leave the same one-line opt-out one level up.
// The event below is hand-written, as an app may write one. `Event::Scope`
// has no default, so it states its tenant, and `impl UnscopedEvent for
// Noted {}` is orphan-legal (`Noted` is local). Unsealed, that line would let
// a scoped event be emitted on a bare `Tx`, recorded with no tenant, and
// delivered to every global feed. Sealed, the impl is refused with the note
// that says where to emit it.
use charpente::auth::Principal;
use charpente::core::table::{Codec, UnscopedEvent};
use charpente::http::ApiError;
use charpente::{Event, Id, Tenant, Tx, table};

#[table(global)]
struct Org {
    id: Id<Org>,
    name: String,
}

impl Tenant for Org {
    const NAME: &'static str = "Org";
    type Who = Principal;
    type Error = ApiError;
    fn authorize(_tx: &Tx, _who: &Principal, _scope: Id<Org>) -> Result<(), ApiError> {
        Err(ApiError::Unauthorized)
    }
}

struct Noted;

impl Codec for Noted {
    fn encode(&self, _out: &mut Vec<u8>) {}
    fn decode(_input: &mut &[u8]) -> std::io::Result<Noted> {
        Ok(Noted)
    }
}

impl Event for Noted {
    const NAME: &'static str = "noted";
    const FINGERPRINT: u64 = 0x7E57_0B0B;
    type Scope = Org;
}

impl UnscopedEvent for Noted {}

fn emit(tx: &mut Tx) {
    tx.emit(Noted);
}

fn main() {
    let _ = emit;
}
