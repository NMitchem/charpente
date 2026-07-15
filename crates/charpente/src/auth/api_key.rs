//! API keys (M1 auth T3): `ck_`-prefixed bearer credentials, hashed at rest
//! like sessions (DESIGN §8) — the raw key exists only in the mint response,
//! never reaching disk. Revocation is `tx.delete::<ApiKey>(id)`; there is no
//! separate revoked flag (revoke-as-delete).

use charpente_core::table::Id;
use charpente_macros::table;

#[table]
pub struct ApiKey {
    pub id: Id<ApiKey>,
    #[index]
    pub key_hash: String,
    pub subject: String,
    pub created_at: u64,
}
