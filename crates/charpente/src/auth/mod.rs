pub mod api_key;
mod hash;
pub mod ladder;
pub mod oidc;
pub mod sweep;
pub use api_key::ApiKey;
pub use hash::{ct_eq_hex, hash_token};
pub use ladder::{AuthReject, Principal, Via};
// `Presented` and `resolve` are the ladder's own internals (job-tenancy spec
// §2 R2): every `Principal` construction lives inside `charpente`, and a
// `pub` door here would let a handler mint one from a subject it names
// (`resolve(tx, &Presented::Bearer(raw), ..)` off a key it just staged).
pub(crate) use ladder::{Presented, resolve};
pub use oidc::{OidcConfig, OidcConfigError, OidcReject, OidcVerifier};
pub use sweep::SessionSweep;
