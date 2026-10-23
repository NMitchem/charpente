//! OIDC static-JWKS RS256 verification (auth T5, spec §3). `OidcVerifier` is
//! only constructible via `from_config` — a bad key, unsupported `alg`, or
//! malformed JWKS document is a named mount error, never a request-time one
//! (CLAUDE.md hard rule 5). `verify` is pure over `(jwt, now)`: zero entropy
//! draws, zero clock reads — RS256 signature check, then the `iss`/`aud`/
//! `exp`/`nbf` claims, returning the `sub` claim on success.

use std::fmt;

use base64ct::{Base64UrlUnpadded, Encoding};
use charpente_core::io::WallTime;
use rsa::{BigUint, Pkcs1v15Sign, RsaPublicKey};
use serde::Deserialize;
use sha2::{Digest, Sha256};

/// Clock skew tolerance for `exp`/`nbf`, in nanoseconds (spec §3/§8: 60s).
pub const JWT_CLOCK_SKEW_NANOS: u64 = charpente_core::io::CLOCK_TOLERANCE.as_nanos() as u64;

/// App-mount config (spec §3): the IdP's issuer/audience and its JWKS
/// document verbatim (`{"keys": [...]}`), parsed once at `from_config`.
pub struct OidcConfig {
    pub issuer: String,
    pub audience: String,
    pub jwks_json: String,
}

/// Why `OidcVerifier::from_config` refused a JWKS document — named at mount
/// time, never at request time (spec §3). `key_ref` names the offending key
/// (its `kid`, or its position when `kid` is absent) so a multi-key JWKS
/// points at the exact entry to fix.
#[derive(Debug)]
pub enum OidcConfigError {
    MalformedJwks(String),
    NoKeys,
    UnsupportedKty { key_ref: String, kty: String },
    UnsupportedAlg { key_ref: String, alg: String },
    BadKey { key_ref: String, reason: String },
}

impl fmt::Display for OidcConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OidcConfigError::MalformedJwks(reason) => write!(
                f,
                "OidcConfig::jwks_json is not valid JSON: {reason} — pass the IdP's JWKS \
                 document verbatim (a {{\"keys\": [...]}} object)"
            ),
            OidcConfigError::NoKeys => write!(
                f,
                "OidcConfig::jwks_json has no keys — the \"keys\" array is empty; add at \
                 least one RSA key"
            ),
            OidcConfigError::UnsupportedKty { key_ref, kty } => write!(
                f,
                "OIDC JWKS key {key_ref} has kty \"{kty}\" — only \"RSA\" is supported in v1; \
                 remove non-RSA keys from the JWKS"
            ),
            OidcConfigError::UnsupportedAlg { key_ref, alg } => write!(
                f,
                "OIDC JWKS key {key_ref} has alg \"{alg}\" — only \"RS256\" is supported; fix \
                 or remove the key's \"alg\" field"
            ),
            OidcConfigError::BadKey { key_ref, reason } => {
                write!(f, "OIDC JWKS key {key_ref} is invalid: {reason}")
            }
        }
    }
}

/// Why `OidcVerifier::verify` rejected a presented JWT (spec §6): one named
/// variant per check, each naming the check and the fix.
///
/// No longer derives `Wire`/`Serialize` (task 5 fix round 1, F1): `ApiError`
/// itself no longer derives `Wire` either — it has a hand-written impl
/// describing the fixed `{"error": "<message>"}` envelope `Response::error`
/// actually emits, which never contains a `Ref("OidcReject")`. Its message
/// is flattened into that envelope string by
/// `ApiError::Oidc(reject) => Response::error(401, &reject.to_string())`
/// (`http/mod.rs`), so it never needed its own wire schema.
#[derive(Debug)]
pub enum OidcReject {
    Malformed(String),
    UnsupportedAlg(String),
    BadSignature,
    Expired,
    NotYetValid,
    WrongIssuer { got: String, configured: String },
    WrongAudience { got: String, configured: String },
}

impl fmt::Display for OidcReject {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OidcReject::Malformed(reason) => write!(f, "malformed OIDC token — {reason}"),
            OidcReject::UnsupportedAlg(alg) => write!(
                f,
                "unsupported OIDC token alg \"{alg}\" — only \"RS256\" is verified; mint the \
                 token with an RS256-signed key"
            ),
            OidcReject::BadSignature => write!(
                f,
                "OIDC token signature does not verify against any configured JWKS key — the \
                 token was not issued by a trusted key; obtain a new one"
            ),
            OidcReject::Expired => write!(
                f,
                "OIDC token expired — its \"exp\" claim is in the past; obtain a new one"
            ),
            OidcReject::NotYetValid => write!(
                f,
                "OIDC token is not yet valid — its \"nbf\" claim is in the future; wait or \
                 mint a fresh token"
            ),
            OidcReject::WrongIssuer { got, configured } => write!(
                f,
                "OIDC token has issuer \"{got}\" but this server is configured for issuer \
                 \"{configured}\" — mint the token against the configured issuer or fix \
                 OidcConfig::issuer"
            ),
            OidcReject::WrongAudience { got, configured } => write!(
                f,
                "OIDC token has audience \"{got}\" but this server is configured for audience \
                 \"{configured}\" — mint the token for the configured audience or fix \
                 OidcConfig::audience"
            ),
        }
    }
}

#[derive(Deserialize)]
struct JwkSet {
    keys: Vec<Jwk>,
}

#[derive(Deserialize)]
struct Jwk {
    kty: String,
    #[serde(default)]
    kid: Option<String>,
    #[serde(default)]
    alg: Option<String>,
    n: String,
    e: String,
}

#[derive(Deserialize)]
struct Header {
    alg: String,
    #[serde(default)]
    kid: Option<String>,
}

/// `aud` is a string or an array of strings (spec §3).
#[derive(Deserialize)]
#[serde(untagged)]
enum Audience {
    One(String),
    Many(Vec<String>),
}

impl Audience {
    fn contains(&self, want: &str) -> bool {
        match self {
            Audience::One(s) => s == want,
            Audience::Many(v) => v.iter().any(|s| s == want),
        }
    }

    fn display(&self) -> String {
        match self {
            Audience::One(s) => s.clone(),
            Audience::Many(v) => v.join(","),
        }
    }
}

#[derive(Deserialize)]
struct Claims {
    iss: String,
    aud: Audience,
    exp: u64,
    #[serde(default)]
    nbf: Option<u64>,
    #[serde(default)]
    sub: Option<String>,
}

/// A parsed static JWKS, ready to verify RS256-signed JWTs against it.
/// Construct only via `from_config` (spec §3) — there is no public
/// constructor that skips JWKS validation.
#[derive(Debug)]
pub struct OidcVerifier {
    issuer: String,
    audience: String,
    /// `(kid, key)` pairs in JWKS document order; `kid` is `None` for a
    /// keyless entry (matched only when a presented token also omits `kid`).
    keys: Vec<(Option<String>, RsaPublicKey)>,
}

impl OidcVerifier {
    /// Parses `config.jwks_json` into RSA public keys (spec §3). Every key
    /// in the set is validated eagerly — a bad key, unsupported `kty`/`alg`,
    /// or malformed document is a named `OidcConfigError`, never deferred to
    /// request time.
    pub fn from_config(config: OidcConfig) -> Result<OidcVerifier, OidcConfigError> {
        let jwks: JwkSet = serde_json::from_str(&config.jwks_json)
            .map_err(|e| OidcConfigError::MalformedJwks(e.to_string()))?;
        if jwks.keys.is_empty() {
            return Err(OidcConfigError::NoKeys);
        }
        let mut keys = Vec::with_capacity(jwks.keys.len());
        for (i, jwk) in jwks.keys.iter().enumerate() {
            let key_ref = jwk
                .kid
                .clone()
                .unwrap_or_else(|| format!("keys[{i}] (no kid)"));
            if jwk.kty != "RSA" {
                return Err(OidcConfigError::UnsupportedKty {
                    key_ref,
                    kty: jwk.kty.clone(),
                });
            }
            if let Some(alg) = &jwk.alg
                && alg != "RS256"
            {
                return Err(OidcConfigError::UnsupportedAlg {
                    key_ref,
                    alg: alg.clone(),
                });
            }
            let n_bytes =
                Base64UrlUnpadded::decode_vec(&jwk.n).map_err(|e| OidcConfigError::BadKey {
                    key_ref: key_ref.clone(),
                    reason: format!("modulus \"n\" is not valid base64url: {e}"),
                })?;
            let e_bytes =
                Base64UrlUnpadded::decode_vec(&jwk.e).map_err(|e| OidcConfigError::BadKey {
                    key_ref: key_ref.clone(),
                    reason: format!("exponent \"e\" is not valid base64url: {e}"),
                })?;
            let n = BigUint::from_bytes_be(&n_bytes);
            let e = BigUint::from_bytes_be(&e_bytes);
            let public_key = RsaPublicKey::new(n, e).map_err(|e| OidcConfigError::BadKey {
                key_ref: key_ref.clone(),
                reason: format!("invalid RSA key: {e}"),
            })?;
            keys.push((jwk.kid.clone(), public_key));
        }
        Ok(OidcVerifier {
            issuer: config.issuer,
            audience: config.audience,
            keys,
        })
    }

    /// Verifies a compact JWT (spec §3), returning the `sub` claim on
    /// success. Pure over `(jwt, now)` — `now` is the caller's single wall-clock
    /// reading, the scale of `exp`/`nbf`; no clock read, no entropy draw happens
    /// in here.
    pub fn verify(&self, jwt: &str, now: WallTime) -> Result<String, OidcReject> {
        let mut segments = jwt.split('.');
        let (Some(header_b64), Some(payload_b64), Some(sig_b64), None) = (
            segments.next(),
            segments.next(),
            segments.next(),
            segments.next(),
        ) else {
            return Err(OidcReject::Malformed(format!(
                "expected \"header.payload.signature\" (three dot-separated segments); got \
                 {} segment(s)",
                jwt.matches('.').count() + 1
            )));
        };

        let header_bytes = decode_segment(header_b64, "header")?;
        let payload_bytes = decode_segment(payload_b64, "payload")?;
        let sig = decode_segment(sig_b64, "signature")?;

        let header: Header = serde_json::from_slice(&header_bytes).map_err(|e| {
            OidcReject::Malformed(format!("the header segment does not decode to JSON: {e}"))
        })?;
        if header.alg != "RS256" {
            return Err(OidcReject::UnsupportedAlg(header.alg));
        }

        let signing_input = format!("{header_b64}.{payload_b64}");
        let digest = Sha256::digest(signing_input.as_bytes());
        let scheme = Pkcs1v15Sign::new::<Sha256>();

        let candidates: Vec<&RsaPublicKey> = match &header.kid {
            Some(kid) => self
                .keys
                .iter()
                .filter(|(k, _)| k.as_deref() == Some(kid.as_str()))
                .map(|(_, key)| key)
                .collect(),
            None => self.keys.iter().map(|(_, key)| key).collect(),
        };
        let verified = candidates
            .iter()
            .any(|key| key.verify(scheme.clone(), &digest, &sig).is_ok());
        if !verified {
            return Err(OidcReject::BadSignature);
        }

        let claims: Claims = serde_json::from_slice(&payload_bytes).map_err(|e| {
            OidcReject::Malformed(format!("the payload segment does not decode to JSON: {e}"))
        })?;

        if claims.iss != self.issuer {
            return Err(OidcReject::WrongIssuer {
                got: claims.iss,
                configured: self.issuer.clone(),
            });
        }
        if !claims.aud.contains(&self.audience) {
            return Err(OidcReject::WrongAudience {
                got: claims.aud.display(),
                configured: self.audience.clone(),
            });
        }
        let now = now.as_nanos();
        let exp_nanos = claims.exp.saturating_mul(1_000_000_000);
        if exp_nanos <= now.saturating_sub(JWT_CLOCK_SKEW_NANOS) {
            return Err(OidcReject::Expired);
        }
        if let Some(nbf) = claims.nbf {
            let nbf_nanos = nbf.saturating_mul(1_000_000_000);
            if nbf_nanos > now.saturating_add(JWT_CLOCK_SKEW_NANOS) {
                return Err(OidcReject::NotYetValid);
            }
        }
        claims
            .sub
            .ok_or_else(|| OidcReject::Malformed("missing the required \"sub\" claim".into()))
    }
}

fn decode_segment(segment: &str, name: &str) -> Result<Vec<u8>, OidcReject> {
    Base64UrlUnpadded::decode_vec(segment).map_err(|e| {
        OidcReject::Malformed(format!("the {name} segment is not valid base64url: {e}"))
    })
}
