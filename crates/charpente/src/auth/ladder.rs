//! The resolution ladder (auth T4, spec §2): one entry point that turns a
//! presented bearer/cookie token into a `Principal`, replacing the old
//! direct-session-only lookup `build_ctx` used to do inline. Order: a
//! two-dot bearer is judged as a JWT (OIDC verify if configured, never a
//! fall-through to hash lookup); any other token hashes once and checks
//! `ApiKey` before `Session` (expiry-checked); nothing presented is
//! `Ok(None)` — the caller's `auth` gate, not this function, answers the
//! no-credential 401. Pure over `(tx, token, now)`: zero entropy draws, zero
//! clock reads (`now` is the caller's single reading) — the frozen replay
//! contract holds by construction.

use std::io;

use charpente_core::db::Tx;
use charpente_core::table::Id;

use crate::auth::{ApiKey, hash_token};
use crate::http::Session;

/// The authenticated identity for a request. `subject` is whatever
/// `create_session`/`create_api_key` was called with, or an OIDC token's
/// `sub` claim; `via` names which ladder rung produced it.
#[derive(Debug, Clone)]
pub struct Principal {
    pub subject: String,
    pub via: Via,
}

/// Which ladder rung authenticated this request. `Session`/`ApiKey` carry
/// the matched row's id, so a caller (`Ctx::session()`'s compat accessor)
/// can re-look it up; `Oidc` is ephemeral — no row, nothing to revoke but a
/// short `exp` (spec §3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Via {
    Session(Id<Session>),
    ApiKey(Id<ApiKey>),
    Oidc,
}

/// Why the ladder rejected a *presented* credential (spec §6). Nothing
/// presented at all is never an `Err` here — `resolve` returns `Ok(None)`
/// for that, and the caller's `auth` gate answers the no-credential 401.
/// `Io` is the platform-failure passthrough (a lookup faulted mid-fault-
/// injection), not a named 401 — the caller maps it to the internal-error
/// path instead.
#[derive(Debug)]
pub enum AuthReject {
    /// Matched nothing live: absent from both indexes, revoked (row
    /// deleted), garbage, or a two-dot bearer with no OIDC configured.
    Unrecognized,
    /// Matched a `Session` row past its `expires_at`.
    Expired,
    Io(io::Error),
}

impl From<io::Error> for AuthReject {
    fn from(e: io::Error) -> AuthReject {
        AuthReject::Io(e)
    }
}

/// Placeholder for the static-JWKS RS256 verifier auth T5 builds (spec §3).
/// Nothing in this task constructs one — there is no `OidcConfig`/
/// `App::with_oidc` yet — so every caller here passes `None`, and a
/// two-dot bearer is always the unrecognized-credential reject (`resolve`'s
/// `None` arm below). T5 replaces this type (likely relocating it into its
/// own `auth::oidc` module, with real fields: parsed JWKS, issuer,
/// audience) and `verify`'s body (RS256 signature check, then `iss`/`aud`/
/// `exp`/`nbf` claims). The method's signature — pure over `(token, now)`,
/// returning an ephemeral `Principal { via: Via::Oidc }` on success or an
/// `AuthReject` naming which check failed — is the hook this task fixes;
/// T5 fills it in without reshaping `resolve` (T5 may still enrich
/// `AuthReject` with per-check variants — that's additive, not a reshape).
pub struct OidcVerifier;

impl OidcVerifier {
    pub fn verify(&self, token: &str, now: u64) -> Result<Principal, AuthReject> {
        let _ = (token, now);
        unimplemented!("auth T5: static-JWKS RS256 verification (spec §3)")
    }
}

/// The resolution ladder. `token` is whatever the caller already resolved
/// from `Authorization: Bearer <t>` or the `charpente_session` cookie
/// (mirroring `bearer_or_cookie`'s merge today) — both sources run the same
/// ladder here, JWT-shape rung included.
pub fn resolve(
    tx: &mut Tx,
    token: Option<&str>,
    oidc: Option<&OidcVerifier>,
    now: u64,
) -> Result<Option<Principal>, AuthReject> {
    let Some(token) = token else {
        return Ok(None);
    };
    if token.matches('.').count() == 2 {
        return match oidc {
            Some(verifier) => verifier.verify(token, now).map(Some),
            None => Err(AuthReject::Unrecognized),
        };
    }
    let hash = hash_token(token);
    if let Some(key) = ApiKey::by_key_hash(tx, &hash)?.into_iter().next() {
        return Ok(Some(Principal {
            subject: key.subject,
            via: Via::ApiKey(key.id),
        }));
    }
    match Session::by_token_hash(tx, &hash)?.into_iter().next() {
        Some(session) if session.expires_at <= now => Err(AuthReject::Expired),
        Some(session) => Ok(Some(Principal {
            subject: session.subject,
            via: Via::Session(session.id),
        })),
        None => Err(AuthReject::Unrecognized),
    }
}
