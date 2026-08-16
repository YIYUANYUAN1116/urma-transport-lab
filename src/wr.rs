use crate::{Error, Result, SlotId};

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[repr(u8)]
pub enum OperationType {
    Send = 1,
    Recv = 2,
}

impl TryFrom<u8> for OperationType {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            1 => Ok(Self::Send),
            2 => Ok(Self::Recv),
            _ => Err(Error::Protocol(format!(
                "invalid WR operation type {value}"
            ))),
        }
    }
}

/// Stable, pointer-free user_ctx encoding.
///
/// `[connection:16][generation:8][operation:8][slot:32]`
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct WrToken {
    pub connection_id: u16,
    pub generation: u8,
    pub operation: OperationType,
    pub slot: SlotId,
}

impl WrToken {
    pub fn encode(self) -> Result<u64> {
        if self.connection_id == 0 || self.generation == 0 {
            return Err(Error::InvalidConfiguration(
                "connection_id and generation must be non-zero".into(),
            ));
        }
        let slot = u32::try_from(self.slot.index())
            .map_err(|_| Error::InvalidConfiguration("slot id exceeds 32 bits".into()))?;
        Ok((u64::from(self.connection_id) << 48)
            | (u64::from(self.generation) << 40)
            | (u64::from(self.operation as u8) << 32)
            | u64::from(slot))
    }

    pub fn decode(value: u64) -> Result<Self> {
        let connection_id = (value >> 48) as u16;
        let generation = ((value >> 40) & 0xff) as u8;
        let operation = OperationType::try_from(((value >> 32) & 0xff) as u8)?;
        if connection_id == 0 || generation == 0 {
            return Err(Error::Protocol("CQE user_ctx has a zero identity".into()));
        }
        Ok(Self {
            connection_id,
            generation,
            operation,
            slot: SlotId::from_index((value & 0xffff_ffff) as usize),
        })
    }
}

#[derive(Default)]
#[cfg_attr(not(feature = "urma"), allow(dead_code))]
pub(crate) struct ReceiveCredit {
    posted: usize,
    ever_posted: bool,
}

#[cfg_attr(not(feature = "urma"), allow(dead_code))]
impl ReceiveCredit {
    pub(crate) fn posted(&mut self) {
        self.posted += 1;
        self.ever_posted = true;
    }

    pub(crate) fn completed(&mut self) {
        self.posted = self.posted.saturating_sub(1);
    }

    pub(crate) fn require_before_send(&self) -> Result<()> {
        if !self.ever_posted {
            Err(Error::Protocol(
                "SEND is forbidden before at least one RECV is posted".into(),
            ))
        } else {
            Ok(())
        }
    }

    pub(crate) fn current(&self) -> usize {
        self.posted
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn user_ctx_round_trip_has_no_pointer() {
        let token = WrToken {
            connection_id: 9,
            generation: 2,
            operation: OperationType::Recv,
            slot: SlotId::from_index(1234),
        };
        assert_eq!(WrToken::decode(token.encode().unwrap()), Ok(token));
    }

    #[test]
    fn send_before_post_recv_is_rejected() {
        let credit = ReceiveCredit::default();
        assert!(credit.require_before_send().is_err());
    }
}
