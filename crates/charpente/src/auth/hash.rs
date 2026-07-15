//! Credential hashing. Raw tokens/keys/secrets never reach the durable store
//! (DESIGN §8) — only these SHA-256 hex digests do.
use sha2::{Digest, Sha256};

/// Lowercase-hex SHA-256 of a raw credential.
pub fn hash_token(raw: &str) -> String {
    let digest = Sha256::digest(raw.as_bytes());
    let mut out = String::with_capacity(64);
    for byte in digest {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

/// Constant-time equality of two hex strings. Unequal lengths return false
/// without leaking via early exit past the length check (the length of a
/// hash is not secret). Used for the repl peer-secret compare.
pub fn ct_eq_hex(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for i in 0..a.len() {
        diff |= a[i] ^ b[i];
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_token_is_lowercase_hex_sha256() {
        // Known vector: SHA-256("abc") = ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad
        assert_eq!(
            hash_token("abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_eq!(hash_token("abc").len(), 64);
    }

    #[test]
    fn ct_eq_hex_matches_only_equal() {
        let h = hash_token("secret");
        assert!(ct_eq_hex(&h, &hash_token("secret")));
        assert!(!ct_eq_hex(&h, &hash_token("other")));
        assert!(!ct_eq_hex(&h, "deadbeef")); // length mismatch → false, no panic
    }
}
