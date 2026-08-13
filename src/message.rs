use crate::{Error, Result};

pub const DATA_MAGIC: u32 = 0x5552_4d44;
pub const DATA_VERSION: u16 = 2;
pub const DATA_HEADER_LEN: usize = 24;
pub const MAX_DATA_PAYLOAD_LEN: usize = 64 * 1024 - DATA_HEADER_LEN;
pub const SHA256_DIGEST_LEN: usize = 32;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u16)]
pub enum MessageType {
    Ping = 1,
    Pong = 2,
    Request = 3,
    Metadata = 4,
    Data = 5,
    End = 6,
    Error = 7,
}

impl TryFrom<u16> for MessageType {
    type Error = Error;

    fn try_from(value: u16) -> Result<Self> {
        match value {
            1 => Ok(Self::Ping),
            2 => Ok(Self::Pong),
            3 => Ok(Self::Request),
            4 => Ok(Self::Metadata),
            5 => Ok(Self::Data),
            6 => Ok(Self::End),
            7 => Ok(Self::Error),
            _ => Err(Error::Protocol(format!(
                "unknown data message type {value}"
            ))),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MessageBody {
    Ping(Vec<u8>),
    Pong(Vec<u8>),
    Request {
        task_id: String,
        piece_number: u32,
    },
    Metadata {
        offset: u64,
        total_length: u64,
        digest: [u8; SHA256_DIGEST_LEN],
    },
    Data(Vec<u8>),
    End {
        total_length: u64,
        chunk_count: u32,
    },
    Error {
        code: u32,
        message: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Message {
    pub request_id: u64,
    pub sequence: u32,
    pub body: MessageBody,
}

impl Message {
    pub fn ping() -> Self {
        Self {
            request_id: 0,
            sequence: 0,
            body: MessageBody::Ping(b"urma-phase0-ping".to_vec()),
        }
    }

    pub fn pong() -> Self {
        Self {
            request_id: 0,
            sequence: 0,
            body: MessageBody::Pong(b"urma-phase0-pong".to_vec()),
        }
    }

    pub fn request(request_id: u64, task_id: impl Into<String>, piece_number: u32) -> Self {
        Self {
            request_id,
            sequence: 0,
            body: MessageBody::Request {
                task_id: task_id.into(),
                piece_number,
            },
        }
    }

    pub fn metadata(
        request_id: u64,
        offset: u64,
        total_length: u64,
        digest: [u8; SHA256_DIGEST_LEN],
    ) -> Self {
        Self {
            request_id,
            sequence: 0,
            body: MessageBody::Metadata {
                offset,
                total_length,
                digest,
            },
        }
    }

    pub fn data(request_id: u64, sequence: u32, payload: Vec<u8>) -> Self {
        Self {
            request_id,
            sequence,
            body: MessageBody::Data(payload),
        }
    }

    pub fn end(request_id: u64, sequence: u32, total_length: u64) -> Self {
        Self {
            request_id,
            sequence,
            body: MessageBody::End {
                total_length,
                chunk_count: sequence,
            },
        }
    }

    pub fn error(request_id: u64, code: u32, message: impl Into<String>) -> Self {
        Self {
            request_id,
            sequence: 0,
            body: MessageBody::Error {
                code,
                message: message.into(),
            },
        }
    }

    pub fn message_type(&self) -> MessageType {
        match self.body {
            MessageBody::Ping(_) => MessageType::Ping,
            MessageBody::Pong(_) => MessageType::Pong,
            MessageBody::Request { .. } => MessageType::Request,
            MessageBody::Metadata { .. } => MessageType::Metadata,
            MessageBody::Data(_) => MessageType::Data,
            MessageBody::End { .. } => MessageType::End,
            MessageBody::Error { .. } => MessageType::Error,
        }
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        let payload = self.encode_payload()?;
        if payload.len() > MAX_DATA_PAYLOAD_LEN {
            return Err(Error::Protocol(format!(
                "data payload length {} exceeds {MAX_DATA_PAYLOAD_LEN}",
                payload.len()
            )));
        }
        if matches!(self.body, MessageBody::Data(_)) && payload.is_empty() {
            return Err(Error::Protocol("Data payload must not be empty".into()));
        }
        let length = u32::try_from(payload.len())
            .map_err(|_| Error::Protocol("data payload exceeds u32".into()))?;
        let mut out = Vec::with_capacity(DATA_HEADER_LEN + payload.len());
        out.extend_from_slice(&DATA_MAGIC.to_be_bytes());
        out.extend_from_slice(&DATA_VERSION.to_be_bytes());
        out.extend_from_slice(&(self.message_type() as u16).to_be_bytes());
        out.extend_from_slice(&self.request_id.to_be_bytes());
        out.extend_from_slice(&self.sequence.to_be_bytes());
        out.extend_from_slice(&length.to_be_bytes());
        out.extend_from_slice(&payload);
        Ok(out)
    }

    pub fn decode(input: &[u8]) -> Result<Self> {
        if input.len() < DATA_HEADER_LEN {
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
        let request_id = u64::from_be_bytes(input[8..16].try_into().expect("fixed slice"));
        let sequence = u32::from_be_bytes(input[16..20].try_into().expect("fixed slice"));
        let length = u32::from_be_bytes(input[20..24].try_into().expect("fixed slice")) as usize;
        if length > MAX_DATA_PAYLOAD_LEN || input.len() != DATA_HEADER_LEN + length {
            return Err(Error::Protocol(format!(
                "data payload length {length} disagrees with frame length {}",
                input.len()
            )));
        }
        let payload = &input[DATA_HEADER_LEN..];
        let body = Self::decode_payload(message_type, payload)?;
        if matches!(body, MessageBody::Data(_)) && payload.is_empty() {
            return Err(Error::Protocol("Data payload must not be empty".into()));
        }
        Ok(Self {
            request_id,
            sequence,
            body,
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

    fn encode_payload(&self) -> Result<Vec<u8>> {
        match &self.body {
            MessageBody::Ping(payload)
            | MessageBody::Pong(payload)
            | MessageBody::Data(payload) => Ok(payload.clone()),
            MessageBody::Request {
                task_id,
                piece_number,
            } => {
                let task = task_id.as_bytes();
                if task.is_empty() {
                    return Err(Error::Protocol("Request task_id must not be empty".into()));
                }
                let task_len = u32::try_from(task.len())
                    .map_err(|_| Error::Protocol("Request task_id exceeds u32".into()))?;
                let mut out = Vec::with_capacity(8 + task.len());
                out.extend_from_slice(&piece_number.to_be_bytes());
                out.extend_from_slice(&task_len.to_be_bytes());
                out.extend_from_slice(task);
                Ok(out)
            }
            MessageBody::Metadata {
                offset,
                total_length,
                digest,
            } => {
                let mut out = Vec::with_capacity(48);
                out.extend_from_slice(&offset.to_be_bytes());
                out.extend_from_slice(&total_length.to_be_bytes());
                out.extend_from_slice(digest);
                Ok(out)
            }
            MessageBody::End {
                total_length,
                chunk_count,
            } => {
                let mut out = Vec::with_capacity(12);
                out.extend_from_slice(&total_length.to_be_bytes());
                out.extend_from_slice(&chunk_count.to_be_bytes());
                Ok(out)
            }
            MessageBody::Error { code, message } => {
                let bytes = message.as_bytes();
                if bytes.is_empty() {
                    return Err(Error::Protocol("Error message must not be empty".into()));
                }
                let mut out = Vec::with_capacity(4 + bytes.len());
                out.extend_from_slice(&code.to_be_bytes());
                out.extend_from_slice(bytes);
                Ok(out)
            }
        }
    }

    fn decode_payload(message_type: MessageType, payload: &[u8]) -> Result<MessageBody> {
        match message_type {
            MessageType::Ping => Ok(MessageBody::Ping(payload.to_vec())),
            MessageType::Pong => Ok(MessageBody::Pong(payload.to_vec())),
            MessageType::Request => {
                if payload.len() < 8 {
                    return Err(Error::Protocol("truncated Request payload".into()));
                }
                let piece_number =
                    u32::from_be_bytes(payload[0..4].try_into().expect("fixed slice"));
                let task_len =
                    u32::from_be_bytes(payload[4..8].try_into().expect("fixed slice")) as usize;
                if task_len == 0 || payload.len() != 8 + task_len {
                    return Err(Error::Protocol("invalid Request task_id length".into()));
                }
                let task_id = std::str::from_utf8(&payload[8..])
                    .map_err(|_| Error::Protocol("Request task_id is not UTF-8".into()))?
                    .to_owned();
                Ok(MessageBody::Request {
                    task_id,
                    piece_number,
                })
            }
            MessageType::Metadata => {
                if payload.len() != 48 {
                    return Err(Error::Protocol("Metadata payload length must be 48".into()));
                }
                Ok(MessageBody::Metadata {
                    offset: u64::from_be_bytes(payload[0..8].try_into().expect("fixed slice")),
                    total_length: u64::from_be_bytes(
                        payload[8..16].try_into().expect("fixed slice"),
                    ),
                    digest: payload[16..48].try_into().expect("fixed slice"),
                })
            }
            MessageType::Data => Ok(MessageBody::Data(payload.to_vec())),
            MessageType::End => {
                if payload.len() != 12 {
                    return Err(Error::Protocol("End payload length must be 12".into()));
                }
                Ok(MessageBody::End {
                    total_length: u64::from_be_bytes(
                        payload[0..8].try_into().expect("fixed slice"),
                    ),
                    chunk_count: u32::from_be_bytes(
                        payload[8..12].try_into().expect("fixed slice"),
                    ),
                })
            }
            MessageType::Error => {
                if payload.len() < 5 {
                    return Err(Error::Protocol("truncated Error payload".into()));
                }
                Ok(MessageBody::Error {
                    code: u32::from_be_bytes(payload[0..4].try_into().expect("fixed slice")),
                    message: std::str::from_utf8(&payload[4..])
                        .map_err(|_| Error::Protocol("Error message is not UTF-8".into()))?
                        .to_owned(),
                })
            }
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
    fn m4_messages_round_trip() {
        let digest = [7; SHA256_DIGEST_LEN];
        let messages = [
            Message::request(9, "task-a", 4),
            Message::metadata(9, 123, 456, digest),
            Message::data(9, 0, vec![1, 2, 3]),
            Message::end(9, 1, 3),
            Message::error(9, 42, "not found"),
        ];
        for message in messages {
            assert_eq!(Message::decode(&message.encode().unwrap()), Ok(message));
        }
    }

    #[test]
    fn rejects_payload_length_mismatch() {
        let mut bytes = Message::ping().encode().unwrap();
        bytes[23] += 1;
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
