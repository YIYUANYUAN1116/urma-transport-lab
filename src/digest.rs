use crate::{Error, Result};
use std::io::Read;

pub const DRAGONFLY_CRC32_PREFIX: &str = "crc32:";

#[derive(Clone, Debug)]
pub struct Crc32Hasher(crc32fast::Hasher);

impl Default for Crc32Hasher {
    fn default() -> Self {
        Self::new()
    }
}

impl Crc32Hasher {
    pub fn new() -> Self {
        Self(crc32fast::Hasher::new())
    }

    pub fn update(&mut self, bytes: &[u8]) {
        self.0.update(bytes);
    }

    /// Appends a separately hashed byte range to this digest. This permits
    /// independent windows to be hashed in parallel and combined in wire
    /// order without rescanning their payloads.
    pub fn combine(&mut self, other: &Self) {
        self.0.combine(&other.0);
    }

    pub fn finalize(self) -> u32 {
        self.0.finalize()
    }
}

pub fn crc32_bytes(bytes: &[u8]) -> u32 {
    let mut hasher = Crc32Hasher::new();
    hasher.update(bytes);
    hasher.finalize()
}

pub fn crc32_reader(reader: &mut impl Read) -> Result<(u32, u64)> {
    let mut hasher = Crc32Hasher::new();
    let mut length = 0u64;
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let read = match reader.read(&mut buffer) {
            Ok(0) => break,
            Ok(read) => read,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(error) => {
                return Err(Error::Io {
                    operation: "read input for CRC32",
                    message: error.to_string(),
                });
            }
        };
        hasher.update(&buffer[..read]);
        length = length
            .checked_add(read as u64)
            .ok_or_else(|| Error::Protocol("CRC32 input length overflow".into()))?;
    }
    Ok((hasher.finalize(), length))
}

pub fn format_crc32_digest(value: u32) -> String {
    format!("{DRAGONFLY_CRC32_PREFIX}{value}")
}

pub fn parse_crc32_digest(input: &str) -> Result<u32> {
    let encoded = input
        .strip_prefix(DRAGONFLY_CRC32_PREFIX)
        .ok_or_else(|| Error::Protocol("CRC32 digest must start with crc32:".into()))?;
    if encoded.is_empty() || !encoded.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(Error::Protocol(
            "CRC32 digest value must be decimal u32".into(),
        ));
    }
    let value = encoded
        .parse::<u32>()
        .map_err(|_| Error::Protocol("CRC32 digest value exceeds u32".into()))?;
    if input != format_crc32_digest(value) {
        return Err(Error::Protocol(
            "CRC32 digest value is not canonical decimal".into(),
        ));
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn crc32_empty_payload_matches_dragonfly_representation() {
        assert_eq!(crc32_bytes(b""), 0);
        assert_eq!(format_crc32_digest(0), "crc32:0");
    }

    #[test]
    fn crc32_known_payload_matches_ieee_vector() {
        assert_eq!(crc32_bytes(b"123456789"), 3_421_780_262);
        assert_eq!(
            format_crc32_digest(crc32_bytes(b"123456789")),
            "crc32:3421780262"
        );
    }

    #[test]
    fn streaming_and_one_shot_crc32_match() {
        let bytes = b"Dragonfly Piece over URMA";
        let mut hasher = Crc32Hasher::new();
        for chunk in bytes.chunks(3) {
            hasher.update(chunk);
        }
        assert_eq!(hasher.finalize(), crc32_bytes(bytes));

        let (reader_digest, length) = crc32_reader(&mut Cursor::new(bytes)).unwrap();
        assert_eq!(reader_digest, crc32_bytes(bytes));
        assert_eq!(length, bytes.len() as u64);
    }

    #[test]
    fn independently_hashed_ranges_combine_in_order() {
        let mut first = Crc32Hasher::new();
        first.update(b"parallel ");
        let mut second = Crc32Hasher::new();
        second.update(b"windows");
        first.combine(&second);
        assert_eq!(first.finalize(), crc32_bytes(b"parallel windows"));
    }

    #[test]
    fn crc32_format_parse_round_trip_is_canonical() {
        for value in [0, 1, 1_475_635_037, u32::MAX] {
            let formatted = format_crc32_digest(value);
            assert_eq!(parse_crc32_digest(&formatted), Ok(value));
        }
        for invalid in ["", "crc32:", "crc32:00", "crc32:-1", "CRC32:1", "1"] {
            assert!(parse_crc32_digest(invalid).is_err(), "accepted {invalid}");
        }
    }
}
