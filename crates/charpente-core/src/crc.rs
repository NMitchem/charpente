//! CRC-32 (IEEE, reflected, poly 0xEDB88320) — the frame checksum for the
//! write-ahead log. Own implementation: nothing inside the deterministic
//! boundary may drift across platforms or crate versions.

const TABLE: [u32; 256] = {
    let mut table = [0u32; 256];
    let mut i = 0;
    while i < 256 {
        let mut crc = i as u32;
        let mut bit = 0;
        while bit < 8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ 0xEDB8_8320
            } else {
                crc >> 1
            };
            bit += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
};

pub struct Crc32 {
    state: u32,
}

impl Crc32 {
    pub fn new() -> Crc32 {
        Crc32 { state: !0 }
    }

    pub fn update(&mut self, data: &[u8]) -> &mut Crc32 {
        for &byte in data {
            self.state =
                (self.state >> 8) ^ TABLE[((self.state ^ u32::from(byte)) & 0xFF) as usize];
        }
        self
    }

    pub fn finish(&self) -> u32 {
        !self.state
    }
}

impl Default for Crc32 {
    fn default() -> Crc32 {
        Crc32::new()
    }
}

pub fn crc32(data: &[u8]) -> u32 {
    Crc32::new().update(data).finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_vectors() {
        // The canonical CRC-32/IEEE check value.
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
        assert_eq!(crc32(b""), 0);
        assert_eq!(crc32(b"charpente"), crc32(b"charpente"));
        assert_ne!(crc32(b"charpente"), crc32(b"charpentf"));
    }

    #[test]
    fn incremental_matches_oneshot() {
        let mut incremental = Crc32::new();
        incremental.update(b"123").update(b"456").update(b"789");
        assert_eq!(incremental.finish(), crc32(b"123456789"));
    }

    #[test]
    fn zeros_do_not_checksum_to_zero() {
        // Recovery relies on this: a zero-filled region must never validate
        // as a frame whose stored crc is zero.
        assert_ne!(crc32(&[0u8; 16]), 0);
    }
}
