//! The resolution ladder (auth T4, spec §2): one entry point that turns a
//! presented bearer/cookie token into a `Principal`, replacing the old
//! direct-session-only lookup `build_ctx` used to do inline. Order: a
//! two-dot BEARER (never a cookie — spec §2 rung 1 is scoped to the
//! Authorization header) is judged as a JWT (OIDC verify if configured,
//! never a fall-through to hash lookup); any other presented token hashes
//! once and checks `ApiKey` before `Session` (expiry-checked); nothing
//! presented is `Ok(None)` — the caller's `auth` gate, not this function,
//! answers the no-credential 401. Pure over `(tx, presented, now)`: zero
//! entropy draws, zero clock reads (`now` is the caller's single reading) —
//! the frozen replay contract holds by construction.

use std::io;

use charpente_core::db::Tx;
use charpente_core::io::WallTime;
use charpente_core::table::Id;

use crate::auth::oidc::{OidcReject, OidcVerifier};
use crate::auth::{ApiKey, hash_token};
use crate::http::Session;

/// The authenticated identity for a request. `subject` is whatever
/// `create_session`/`create_api_key` was called with, or an OIDC token's
/// `sub` claim; `via` names which ladder rung produced it.
///
/// The fields are `pub(crate)`, so every `Principal` is one the resolution
/// ladder produced (job-tenancy spec §2 R2). This is what makes
/// `Tenant::authorize`'s `who` unforgeable: a handler reads a principal and
/// never builds one.
///
/// Not `Clone` (tenancy-completion spec §2 ruling 5, amendment A19): the one
/// principal an app holds is the `&Principal` its request's `Ctx` lends,
/// which cannot outlive that request. `Tx::open_scope` takes any
/// `&Principal` as the `who` it asks `authorize` about, so a copy parked in a
/// `static` would let a later request, on any route and with no credential
/// of its own, open every tenant the parked principal belongs to. With no
/// copy to park, a parked identity opens no tenant, and the `who` an app can
/// hand `open_scope` is its own request's caller.
#[derive(Debug)]
pub struct Principal {
    pub(crate) subject: String,
    pub(crate) via: Via,
}

impl Principal {
    /// The subject the credential was minted for, or the OIDC `sub` claim.
    pub fn subject(&self) -> &str {
        &self.subject
    }

    /// The ladder rung that authenticated this request.
    pub fn via(&self) -> Via {
        self.via
    }
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
    /// A two-dot bearer, judged as a JWT, failed OIDC verification — the
    /// per-check 401 taxonomy lives in `OidcReject` (spec §6); this is the
    /// one mapping seam (T5's ruling), not a reshape of the other variants.
    Oidc(OidcReject),
    Io(io::Error),
}

impl From<io::Error> for AuthReject {
    fn from(e: io::Error) -> AuthReject {
        AuthReject::Io(e)
    }
}

/// Where a resolved credential came from — the source that scopes the OIDC
/// rung (spec §2 rung 1: the Authorization bearer header ONLY). A
/// `charpente_session` cookie always goes down the hash path (`ApiKey` then
/// `Session`) even if its value happens to contain two dots; only a
/// `Bearer` token is ever judged as a JWT.
pub(crate) enum Presented {
    Bearer(String),
    Cookie(String),
    None,
}

/// The resolution ladder. `presented` is whatever the caller already
/// resolved from `Authorization: Bearer <t>` or the `charpente_session`
/// cookie (`bearer_or_cookie`), tagged with its source so the two-dot
/// JWT-shape check applies to a bearer only.
pub(crate) fn resolve(
    tx: &mut Tx,
    presented: &Presented,
    oidc: Option<&OidcVerifier>,
    now: WallTime,
) -> Result<Option<Principal>, AuthReject> {
    let token = match presented {
        Presented::None => return Ok(None),
        Presented::Bearer(token) => {
            if token.matches('.').count() == 2 {
                return match oidc {
                    Some(verifier) => verifier
                        .verify(token, now)
                        .map(|subject| {
                            Some(Principal {
                                subject,
                                via: Via::Oidc,
                            })
                        })
                        .map_err(AuthReject::Oidc),
                    None => Err(AuthReject::Unrecognized),
                };
            }
            token.as_str()
        }
        Presented::Cookie(token) => token.as_str(),
    };
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
