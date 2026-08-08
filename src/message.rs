use crate::{Error, Result};

pub const DATA_MAGIC: u32 = 0x5552_4d44;
pub const DATA_VERSION: u16 = 1;
pub const MAX_DATA_PAYLOAD_LEN: usize = 64 * 1024 - 12;
const HEADER_LEN: usize = 12;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u16)]
pub enum MessageType {
    Ping = 1,
    Pong = 2,
}

impl TryFrom<u16> for MessageType {
    type Error = Error;

    fn try_from(value: u16) -> Result<Self> {
        match value {
            1 => Ok(Self::Ping),
            2 => Ok(Self::Pong),
            _ => Err(Error::Protocol(format!(
                "unknown data message type {value}"
            ))),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Message {
    pub message_type: MessageType,
    pub payload: Vec<u8>,
}

impl Message {
    pub fn ping() -> Self {
        Self {
            message_type: MessageType::Ping,
            payload: b"urma-phase0-ping".to_vec(),
        }
    }

    pub fn pong() -> Self {
        Self {
            message_type: MessageType::Pong,
            payload: b"urma-phase0-pong".to_vec(),
        }
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        if self.payload.is_empty() || self.payload.len() > MAX_DATA_PAYLOAD_LEN {
            return Err(Error::Protocol(format!(
                "data payload length {} is outside 1..={MAX_DATA_PAYLOAD_LEN}",
                self.payload.len()
            )));
        }
        let length = u32::try_from(self.payload.len())
            .map_err(|_| Error::Protocol("data payload exceeds u32".into()))?;
        let mut out = Vec::with_capacity(HEADER_LEN + self.payload.len());
        out.extend_from_slice(&DATA_MAGIC.to_be_bytes());
        out.extend_from_slice(&DATA_VERSION.to_be_bytes());
        out.extend_from_slice(&(self.message_type as u16).to_be_bytes());
        out.extend_from_slice(&length.to_be_bytes());
        out.extend_from_slice(&self.payload);
        Ok(out)
    }

    pub fn decode(input: &[u8]) -> Result<Self> {
        if input.len() < HEADER_LEN {
            return Err(Error::Protocol("truncated data message header".into()));
        }
        let magic = u32::from_be_bytes(input[0..4].try_into().expect("fixed slice"));
        if magic != DATA_MAGIC {
            return Err(Error::Protocol(format!("invalid data magic 0x{magic:08x}")));
        }
        let version = u16::from_be_bytes(input[4..6].try_into().expect("fixed slice"));
        if version != DATA_VERSION {
            return Err(Error::Protocol(format!(
                "unsupported data version {version}"
            )));
        }
        let message_type = MessageType::try_from(u16::from_be_bytes(
            input[6..8].try_into().expect("fixed slice"),
        ))?;
        let length = u32::from_be_bytes(input[8..12].try_into().expect("fixed slice")) as usize;
        if length == 0 || length > MAX_DATA_PAYLOAD_LEN || input.len() != HEADER_LEN + length {
            return Err(Error::Protocol(format!(
                "data payload length {length} disagrees with frame length {}",
                input.len()
            )));
        }
        Ok(Self {
            message_type,
            payload: input[HEADER_LEN..].to_vec(),
        })
    }

    pub fn validate_ping(&self) -> Result<()> {
        if self == &Self::ping() {
            Ok(())
        } else {
            Err(Error::Protocol("unexpected Ping payload".into()))
        }
    }

    pub fn validate_pong(&self) -> Result<()> {
        if self == &Self::pong() {
            Ok(())
        } else {
            Err(Error::Protocol("unexpected Pong payload".into()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ping_and_pong_round_trip() {
        for message in [Message::ping(), Message::pong()] {
            assert_eq!(Message::decode(&message.encode().unwrap()), Ok(message));
        }
    }

    #[test]
    fn rejects_payload_length_mismatch() {
        let mut bytes = Message::ping().encode().unwrap();
        bytes[11] += 1;
        assert!(Message::decode(&bytes).is_err());
    }

    #[test]
    fn protocol_loopback_runs_one_and_one_hundred_rounds() {
        for rounds in [1, 100] {
            for _ in 0..rounds {
                let ping = Message::decode(&Message::ping().encode().unwrap()).unwrap();
                ping.validate_ping().unwrap();
                let pong = Message::decode(&Message::pong().encode().unwrap()).unwrap();
                pong.validate_pong().unwrap();
            }
        }
    }
}
