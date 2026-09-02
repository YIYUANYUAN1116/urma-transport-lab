use crate::{Error, Result};

pub const SEND_IMM_PROBE_PAYLOAD_LEN: usize = 64;
pub const SEND_IMM_PROBE_MAX_MESSAGES: usize = 4096;
const MAGIC: &[u8; 8] = b"DFURMAIM";

/// Returns an identity set that exercises both halves of UMDK's 64-bit
/// immediate field, followed by four interleaved transfer namespaces.
pub fn probe_identity(ordinal: usize) -> Result<u64> {
    if ordinal >= SEND_IMM_PROBE_MAX_MESSAGES {
        return Err(Error::InvalidConfiguration(format!(
            "SEND_IMM probe ordinal {ordinal} exceeds {}",
            SEND_IMM_PROBE_MAX_MESSAGES - 1
        )));
    }
    Ok(match ordinal {
        0 => 0x0000_0001_0000_0002,
        1 => 0x1234_5678_9abc_def0,
        2 => 0xfedc_ba98_7654_3210,
        _ => {
            let logical = ordinal - 3;
            let transfer_id = 0x8000_0001u32 + (logical % 4) as u32;
            let chunk = (logical / 4) as u32;
            (u64::from(transfer_id) << 32) | u64::from(chunk)
        }
    })
}

pub fn probe_payload(ordinal: usize, identity: u64) -> Vec<u8> {
    let mut payload = vec![0u8; SEND_IMM_PROBE_PAYLOAD_LEN];
    payload[..8].copy_from_slice(MAGIC);
    payload[8..16].copy_from_slice(&identity.to_be_bytes());
    payload[16..20].copy_from_slice(&(ordinal as u32).to_be_bytes());
    for (index, byte) in payload[20..].iter_mut().enumerate() {
        let shift = ((index % 8) * 8) as u32;
        *byte = ((identity.rotate_left((index % 63 + 1) as u32) >> shift) as u8)
            ^ (ordinal as u8).wrapping_mul(31)
            ^ index as u8;
    }
    payload
}

pub fn validate_probe_payload(ordinal: usize, identity: u64, payload: &[u8]) -> Result<()> {
    if payload != probe_payload(ordinal, identity) {
        return Err(Error::Protocol(format!(
            "SEND_IMM payload mismatch for ordinal={ordinal} identity=0x{identity:016x}"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn identities_are_unique_and_exercise_high_and_low_halves() {
        let identities = (0..256)
            .map(|ordinal| probe_identity(ordinal).expect("identity"))
            .collect::<Vec<_>>();
        assert_eq!(
            identities.iter().copied().collect::<HashSet<_>>().len(),
            256
        );
        assert!(identities.iter().all(|identity| identity >> 32 != 0));
        assert_eq!(identities[0], 0x0000_0001_0000_0002);
        assert_eq!(identities[2], 0xfedc_ba98_7654_3210);
    }

    #[test]
    fn payload_binds_ordinal_to_full_identity() {
        let identity = probe_identity(17).expect("identity");
        let mut payload = probe_payload(17, identity);
        validate_probe_payload(17, identity, &payload).expect("valid payload");
        payload[63] ^= 1;
        assert!(validate_probe_payload(17, identity, &payload).is_err());
        assert!(validate_probe_payload(18, identity, &probe_payload(17, identity)).is_err());
    }
}
