// FINDINGS §4's uncertainty ("#[index] on enum fields unshown"), proven
// unfounded: #[index] imposes no bound beyond Codec — no Eq/Hash/Ord/serde
// needed. Mirrors GUIDE's `InvoiceState` worked example.
use charpente::core::table::Codec;
use charpente::{Id, table};
use std::io;

enum InvoiceState {
    Draft,
    Final,
    Paid,
}

impl Codec for InvoiceState {
    fn encode(&self, out: &mut Vec<u8>) {
        let tag: u8 = match self {
            InvoiceState::Draft => 0,
            InvoiceState::Final => 1,
            InvoiceState::Paid => 2,
        };
        tag.encode(out);
    }

    fn decode(input: &mut &[u8]) -> io::Result<Self> {
        match u8::decode(input)? {
            0 => Ok(InvoiceState::Draft),
            1 => Ok(InvoiceState::Final),
            2 => Ok(InvoiceState::Paid),
            other => Err(io::Error::other(format!(
                "invalid InvoiceState tag {other} — schema drift or corruption"
            ))),
        }
    }
}

#[table]
struct Invoice {
    id: Id<Invoice>,
    #[index]
    state: InvoiceState,
}

fn main() {}
