//! Seeded PRNG: xoshiro256++ expanded from a u64 seed via splitmix64. Own
//! implementation rather than the `rand` crate so nothing inside the
//! deterministic boundary can drift across platforms or crate versions —
//! a seed must replay identically forever.

pub fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

pub struct Rng {
    s: [u64; 4],
}

impl Rng {
    pub fn new(seed: u64) -> Rng {
        let mut sm = seed;
        let s = [
            splitmix64(&mut sm),
            splitmix64(&mut sm),
            splitmix64(&mut sm),
            splitmix64(&mut sm),
        ];
        assert!(s != [0; 4], "xoshiro256++ state must not be all-zero");
        Rng { s }
    }

    pub fn next_u64(&mut self) -> u64 {
        let s = &mut self.s;
        let result = s[0].wrapping_add(s[3]).rotate_left(23).wrapping_add(s[0]);
        let t = s[1] << 17;
        s[2] ^= s[0];
        s[3] ^= s[1];
        s[1] ^= s[2];
        s[0] ^= s[3];
        s[2] ^= t;
        s[3] = s[3].rotate_left(45);
        result
    }

    pub fn fill(&mut self, out: &mut [u8]) {
        for chunk in out.chunks_mut(8) {
            let bytes = self.next_u64().to_le_bytes();
            chunk.copy_from_slice(&bytes[..chunk.len()]);
        }
    }

    /// True with probability `p`. Uniform enough for fault injection; not for
    /// statistics.
    pub fn chance(&mut self, p: f64) -> bool {
        assert!(
            (0.0..=1.0).contains(&p),
            "chance({p}) — probability must be in [0, 1]"
        );
        // Map the top 53 bits to [0, 1); modulo bias is irrelevant here.
        let unit = (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64;
        unit < p
    }

    /// Uniform in `0..n`.
    pub fn below(&mut self, n: u64) -> u64 {
        assert!(n > 0, "below(0) — empty range");
        self.next_u64() % n
    }

    /// Uniform in `lo..=hi`.
    pub fn between(&mut self, lo: u64, hi: u64) -> u64 {
        assert!(lo <= hi, "between({lo}, {hi}) — empty range");
        lo + self.below(hi - lo + 1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_seed_same_stream() {
        let mut a = Rng::new(42);
        let mut b = Rng::new(42);
        for _ in 0..1000 {
            assert_eq!(a.next_u64(), b.next_u64());
        }
    }

    #[test]
    fn fill_matches_next_u64_stream() {
        let mut a = Rng::new(7);
        let mut b = Rng::new(7);
        let mut buf = [0u8; 20];
        a.fill(&mut buf);
        // 20 bytes = two full u64s + one 4-byte prefix, little-endian.
        assert_eq!(&buf[0..8], &b.next_u64().to_le_bytes());
        assert_eq!(&buf[8..16], &b.next_u64().to_le_bytes());
        assert_eq!(&buf[16..20], &b.next_u64().to_le_bytes()[..4]);
    }

    #[test]
    fn chance_extremes() {
        let mut rng = Rng::new(1);
        for _ in 0..100 {
            assert!(!rng.chance(0.0));
            assert!(rng.chance(1.0));
        }
    }

    #[test]
    fn between_bounds() {
        let mut rng = Rng::new(3);
        for _ in 0..1000 {
            let v = rng.between(5, 9);
            assert!((5..=9).contains(&v));
        }
    }
}
