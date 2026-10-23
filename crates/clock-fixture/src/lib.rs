//! The app the edge harness drives for restart-stable time (0.3.0 spec §5 B):
//! a one-second schedule that bumps a `Ticks` row, a session route with a
//! configurable TTL, and an OIDC verifier over a fixed test key. Its own crate
//! so no other fixture's recorded batches or goldens move.

use std::io;
use std::time::Duration;

use base64ct::{Base64UrlUnpadded, Encoding};
use charpente::auth::OidcConfig;
use charpente::core::io::{Disk, Entropy};
use charpente::http::{ApiError, App, AppConfig, Ctx};
use charpente::kernel::JobError;
use charpente::{Id, Schema, Tx, View, job, jobs, route, routes, table};
use rsa::{BigUint, Pkcs1v15Sign, RsaPrivateKey};
use sha2::{Digest, Sha256};

pub const ISSUER: &str = "https://idp.clock.fixture";
pub const AUDIENCE: &str = "clock-fixture";
const KID: &str = "test-key-1";

/// The public half of the fixed key below, as a JWKS document.
pub const TEST_JWKS: &str = r#"{"keys":[{"kty":"RSA","kid":"test-key-1","n":"sz4VgBAgnrnbCC2yusrGXkgS8F_klybsOk0kAGAeUds5WHRtDb7cEX2NZDjxB-obCGp0AUW1e5q4RogtQlfq-w907riII1SHa3BGSa2ejED6ej22crQlpR1EL9I51k6E-Q-7KdMJxd8OZ1gfgeoFj00DZGwg_43PlAMS4og6gAL9pmr3pRWpnszP4BBbi7pGmedHrTz4mz-J7LJ-fK6zV8OkqUlSUDUyqsjdbV0vpsU2ajmpZUorSMZMcKEMEGMwu8aZj8l0ZrPWNc_WzPI7NClA34mGZcL-RFp2NwAi5xE3JMAbkMH9UcQfFmPhf-DzWZMkzqdt8t7sbCAyHy-eTw","e":"AQAB"}]}"#;

// The fixed RSA-2048 key's two primes (the same pair as charpente's
// tests/http.rs); `from_p_q` rebuilds the key and the no-RNG PKCS1v15 sign
// path makes every token byte-reproducible.
const TEST_RSA_P: &[u8] = &[
    0xee, 0x28, 0xc5, 0xb6, 0x51, 0xe4, 0x61, 0xc6, 0x53, 0xd5, 0x9a, 0xe4, 0x22, 0x85, 0xa9, 0x52,
    0x2a, 0x43, 0xf7, 0xac, 0x85, 0x04, 0xfd, 0xb5, 0x1c, 0xa7, 0x30, 0x98, 0xc8, 0xf2, 0x5d, 0x71,
    0x58, 0xa5, 0x3f, 0x3a, 0xf8, 0x1f, 0xb1, 0xec, 0x68, 0xc7, 0xd4, 0x72, 0x6e, 0xc6, 0xad, 0xe1,
    0xf1, 0xc1, 0xe3, 0x70, 0xd0, 0x94, 0xca, 0x01, 0x7a, 0x2d, 0x0e, 0xc0, 0x0f, 0xa8, 0x61, 0xe8,
    0xc4, 0x06, 0x87, 0x9a, 0xaa, 0xf9, 0x78, 0xc5, 0x88, 0xd2, 0xba, 0x4d, 0x96, 0x3f, 0x05, 0x56,
    0x18, 0x64, 0xf8, 0x95, 0x42, 0x8d, 0x9c, 0xed, 0x6c, 0xbb, 0x0a, 0xb3, 0x95, 0xc3, 0x6e, 0x33,
    0xe9, 0xd5, 0x4c, 0x3a, 0x6b, 0x05, 0x6d, 0x8d, 0x15, 0x8d, 0xe1, 0x0e, 0x11, 0x21, 0x2f, 0x0e,
    0x16, 0x5a, 0x5f, 0xdc, 0xed, 0x2c, 0xa8, 0x2c, 0x37, 0x88, 0x5f, 0xea, 0xdd, 0x71, 0x73, 0x35,
];
const TEST_RSA_Q: &[u8] = &[
    0xc0, 0xab, 0x74, 0x12, 0x0a, 0x2d, 0xb5, 0xb0, 0x20, 0x84, 0x33, 0x8f, 0xf2, 0x85, 0xef, 0xac,
    0x2e, 0xeb, 0x07, 0x7b, 0x01, 0xcb, 0x66, 0x97, 0x63, 0x87, 0x1c, 0x3e, 0x0d, 0x41, 0xd5, 0xde,
    0x66, 0x63, 0x95, 0xe6, 0x9a, 0xd5, 0x4f, 0xc0, 0xd3, 0x0c, 0x8e, 0x27, 0xfd, 0x19, 0x5f, 0xc4,
    0x5a, 0xb6, 0x41, 0xd5, 0xdc, 0xee, 0x9c, 0xcb, 0x7c, 0x73, 0x80, 0x2f, 0x0f, 0xf6, 0xfe, 0x50,
    0x67, 0xe9, 0x7d, 0x33, 0x2b, 0x26, 0x42, 0x63, 0x64, 0xb3, 0xdd, 0xa8, 0x71, 0xcc, 0xdf, 0xce,
    0xac, 0xac, 0x75, 0x56, 0xf4, 0xc2, 0xc5, 0x6e, 0x22, 0x09, 0x14, 0x9f, 0x24, 0x28, 0x9f, 0xb4,
    0x04, 0x85, 0x98, 0xab, 0xdb, 0x3d, 0x6f, 0xd4, 0x8e, 0x64, 0x32, 0x7c, 0x5f, 0x93, 0x5f, 0x8b,
    0xa0, 0x93, 0xb9, 0xd8, 0xe5, 0x6f, 0x77, 0xdb, 0x86, 0x07, 0xc0, 0xbf, 0x79, 0xbf, 0x97, 0xf3,
];

/// An RS256 token for subject `bob` with the given `exp` (and optional
/// `nbf`), both Unix seconds.
pub fn mint_jwt(exp_unix_secs: u64, nbf_unix_secs: Option<u64>) -> String {
    let key = RsaPrivateKey::from_p_q(
        BigUint::from_bytes_be(TEST_RSA_P),
        BigUint::from_bytes_be(TEST_RSA_Q),
        BigUint::from(65537u32),
    )
    .expect("fixed test primes are valid");
    let nbf = nbf_unix_secs.map_or(String::new(), |n| format!(r#","nbf":{n}"#));
    let header = format!(r#"{{"alg":"RS256","typ":"JWT","kid":"{KID}"}}"#);
    let payload = format!(
        r#"{{"iss":"{ISSUER}","aud":"{AUDIENCE}","sub":"bob","exp":{exp_unix_secs}{nbf}}}"#
    );
    let input = format!(
        "{}.{}",
        Base64UrlUnpadded::encode_string(header.as_bytes()),
        Base64UrlUnpadded::encode_string(payload.as_bytes())
    );
    let sig = key
        .sign(
            Pkcs1v15Sign::new::<Sha256>(),
            &Sha256::digest(input.as_bytes()),
        )
        .expect("no-rng PKCS1v15 sign");
    format!("{input}.{}", Base64UrlUnpadded::encode_string(&sig))
}

#[table(global)]
pub struct Ticks {
    pub id: Id<Ticks>,
    pub count: u64,
}

#[job(global)]
pub struct Tick {}

impl Tick {
    pub fn run(self, tx: &mut Tx) -> Result<(), JobError> {
        let id = Id::<Ticks>::from_raw(0);
        let count = tx.get(id)?.map_or(0, |t| t.count);
        tx.put(&Ticks {
            id,
            count: count + 1,
        });
        Ok(())
    }
}

#[route(POST "/login", public)]
pub fn login(tx: &mut Tx, ctx: &Ctx) -> Result<String, ApiError> {
    Ok(ctx.create_session(tx, "alice".to_string()))
}

#[route(GET "/me", auth)]
pub fn me(tx: &mut Tx, ctx: &Ctx) -> Result<String, ApiError> {
    let _ = tx;
    Ok(ctx.principal().expect("auth route").subject().to_string())
}

/// The `Ticks` count, as a decimal string.
#[route(GET "/ticks", public)]
pub fn ticks(tx: &mut Tx) -> Result<String, ApiError> {
    let count = tx.get(Id::<Ticks>::from_raw(0))?.map_or(0, |t| t.count);
    Ok(count.to_string())
}

/// `session_ttl_secs` is the session lifetime; the node binary reads it from
/// `CLOCK_NODE_SESSION_TTL_SECS`.
pub fn build(
    disk: Box<dyn Disk>,
    entropy: Box<dyn Entropy>,
    session_ttl_secs: u64,
) -> io::Result<App> {
    let config = AppConfig {
        session_ttl: session_ttl_secs * 1_000_000_000,
        ..AppConfig::default()
    };
    Ok(
        App::open(disk, Schema::new().table::<Ticks>(), entropy, config)?
            .mount(routes![login, me, ticks])
            .mount_jobs(jobs![Tick])
            .schedule("tick", Duration::from_secs(1), || Tick {})
            .with_oidc(OidcConfig {
                issuer: ISSUER.into(),
                audience: AUDIENCE.into(),
                jwks_json: TEST_JWKS.into(),
            }),
    )
}
