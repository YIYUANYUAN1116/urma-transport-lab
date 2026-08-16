use crate::{Error, Result};

pub const DATA_MAGIC: u32 = 0x5552_4d44;
pub const DATA_VERSION: u16 = 2;
pub const DATA_HEADER_LEN: usize = 24;
pub const MAX_DATA_PAYLOAD_LEN: usize = 64 * 1024 - DATA_HEADER_LEN;
pub const SHA256_DIGEST_LEN: usize = 32;
pub const INTEGRATION_VERSION: u16 = 3;
pub const MAX_DIGEST_VALUE_LEN: usize = 64;
const INTEGRATION_METADATA_PREFIX_LEN: usize = 20;

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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DigestAlgorithm {
    Crc32,
    Sha256,
}

impl DigestAlgorithm {
    pub const CRC32_WIRE_VALUE: u16 = 1;
    pub const SHA256_WIRE_VALUE: u16 = 2;

    fn wire_value(self) -> u16 {
        match self {
            Self::Crc32 => Self::CRC32_WIRE_VALUE,
            Self::Sha256 => Self::SHA256_WIRE_VALUE,
        }
    }

    fn from_wire(value: u16) -> Result<Self> {
        match value {
            Self::CRC32_WIRE_VALUE => Ok(Self::Crc32),
            Self::SHA256_WIRE_VALUE => Ok(Self::Sha256),
            _ => Err(Error::Protocol(format!("unknown digest algorithm {value}"))),
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Crc32 => "crc32",
            Self::Sha256 => "sha256",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DigestDescriptor {
    pub algorithm: DigestAlgorithm,
    pub value: String,
}

impl DigestDescriptor {
    pub fn new(algorithm: DigestAlgorithm, value: impl Into<String>) -> Result<Self> {
        let descriptor = Self {
            algorithm,
            value: value.into(),
        };
        descriptor.validate()?;
        Ok(descriptor)
    }

    pub fn crc32(value: u32) -> Self {
        Self {
            algorithm: DigestAlgorithm::Crc32,
            value: value.to_string(),
        }
    }

    pub fn external_string(&self) -> Result<String> {
        self.validate()?;
        Ok(format!("{}:{}", self.algorithm.label(), self.value))
    }

    fn validate(&self) -> Result<()> {
        let bytes = self.value.as_bytes();
        if bytes.is_empty() || bytes.len() > MAX_DIGEST_VALUE_LEN {
            return Err(Error::Protocol(format!(
                "digest value length {} is outside 1..={MAX_DIGEST_VALUE_LEN}",
                bytes.len()
            )));
        }
        match self.algorithm {
            DigestAlgorithm::Crc32 => {
                if !bytes.iter().all(u8::is_ascii_digit) {
                    return Err(Error::Protocol(
                        "CRC32 digest value must be decimal u32".into(),
                    ));
                }
                let value = self
                    .value
                    .parse::<u32>()
                    .map_err(|_| Error::Protocol("CRC32 digest value exceeds u32".into()))?;
                if self.value != value.to_string() {
                    return Err(Error::Protocol(
                        "CRC32 digest value is not canonical decimal".into(),
                    ));
                }
            }
            DigestAlgorithm::Sha256 => {
                if bytes.len() != 64
                    || !bytes
                        .iter()
                        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte))
                {
                    return Err(Error::Protocol(
                        "SHA-256 digest must be 64 lowercase hexadecimal bytes".into(),
                    ));
                }
            }
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IntegrationMessageTypeV3 {
    Request,
    Metadata,
    Data,
    End,
    Error,
}

impl IntegrationMessageTypeV3 {
    fn wire_value(self) -> u16 {
        match self {
            Self::Request => 3,
            Self::Metadata => 4,
            Self::Data => 5,
            Self::End => 6,
            Self::Error => 7,
        }
    }

    fn from_wire(value: u16) -> Result<Self> {
        match value {
            3 => Ok(Self::Request),
            4 => Ok(Self::Metadata),
            5 => Ok(Self::Data),
            6 => Ok(Self::End),
            7 => Ok(Self::Error),
            _ => Err(Error::Protocol(format!(
                "unknown integration message type {value}"
            ))),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IntegrationMessageBodyV3 {
    Request {
        task_id: String,
        piece_number: u32,
    },
    Metadata {
        offset: u64,
        total_length: u64,
        digest: DigestDescriptor,
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
pub struct IntegrationMessageV3 {
    pub request_id: u64,
    pub sequence: u32,
    pub body: IntegrationMessageBodyV3,
}

impl IntegrationMessageV3 {
    pub fn request(request_id: u64, task_id: impl Into<String>, piece_number: u32) -> Self {
        Self {
            request_id,
            sequence: 0,
            body: IntegrationMessageBodyV3::Request {
                task_id: task_id.into(),
                piece_number,
            },
        }
    }

    pub fn metadata(
        request_id: u64,
        offset: u64,
        total_length: u64,
        digest: DigestDescriptor,
    ) -> Self {
        Self {
            request_id,
            sequence: 0,
            body: IntegrationMessageBodyV3::Metadata {
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
            body: IntegrationMessageBodyV3::Data(payload),
        }
    }

    pub fn end(request_id: u64, sequence: u32, total_length: u64) -> Self {
        Self {
            request_id,
            sequence,
            body: IntegrationMessageBodyV3::End {
                total_length,
                chunk_count: sequence,
            },
        }
    }

    pub fn error(request_id: u64, code: u32, message: impl Into<String>) -> Self {
        Self {
            request_id,
            sequence: 0,
            body: IntegrationMessageBodyV3::Error {
                code,
                message: message.into(),
            },
        }
    }

    pub fn message_type(&self) -> IntegrationMessageTypeV3 {
        match self.body {
            IntegrationMessageBodyV3::Request { .. } => IntegrationMessageTypeV3::Request,
            IntegrationMessageBodyV3::Metadata { .. } => IntegrationMessageTypeV3::Metadata,
            IntegrationMessageBodyV3::Data(_) => IntegrationMessageTypeV3::Data,
            IntegrationMessageBodyV3::End { .. } => IntegrationMessageTypeV3::End,
            IntegrationMessageBodyV3::Error { .. } => IntegrationMessageTypeV3::Error,
        }
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        self.validate_common()?;
        let payload = self.encode_payload()?;
        let length = u32::try_from(payload.len())
            .map_err(|_| Error::Protocol("integration payload exceeds u32".into()))?;
        let frame_len = DATA_HEADER_LEN
            .checked_add(payload.len())
            .ok_or_else(|| Error::Protocol("integration frame length overflow".into()))?;
        let mut out = Vec::with_capacity(frame_len);
        out.extend_from_slice(&DATA_MAGIC.to_be_bytes());
        out.extend_from_slice(&INTEGRATION_VERSION.to_be_bytes());
        out.extend_from_slice(&self.message_type().wire_value().to_be_bytes());
        out.extend_from_slice(&self.request_id.to_be_bytes());
        out.extend_from_slice(&self.sequence.to_be_bytes());
        out.extend_from_slice(&length.to_be_bytes());
        out.extend_from_slice(&payload);
        Ok(out)
    }

    pub fn decode(input: &[u8]) -> Result<Self> {
        if input.len() < DATA_HEADER_LEN {
            return Err(Error::Protocol(
                "truncated integration message header".into(),
            ));
        }
        let magic = u32::from_be_bytes(input[0..4].try_into().expect("fixed slice"));
        if magic != DATA_MAGIC {
            return Err(Error::Protocol(format!("invalid data magic 0x{magic:08x}")));
        }
        let version = u16::from_be_bytes(input[4..6].try_into().expect("fixed slice"));
        if version != INTEGRATION_VERSION {
            return Err(Error::Protocol(format!(
                "unsupported integration version {version}"
            )));
        }
        let message_type = IntegrationMessageTypeV3::from_wire(u16::from_be_bytes(
            input[6..8].try_into().expect("fixed slice"),
        ))?;
        let request_id = u64::from_be_bytes(input[8..16].try_into().expect("fixed slice"));
        let sequence = u32::from_be_bytes(input[16..20].try_into().expect("fixed slice"));
        let length = u32::from_be_bytes(input[20..24].try_into().expect("fixed slice")) as usize;
        let frame_len = DATA_HEADER_LEN
            .checked_add(length)
            .ok_or_else(|| Error::Protocol("integration frame length overflow".into()))?;
        if input.len() != frame_len {
            return Err(Error::Protocol(format!(
                "integration payload length {length} disagrees with frame length {}",
                input.len()
            )));
        }
        let message = Self {
            request_id,
            sequence,
            body: Self::decode_payload(message_type, &input[DATA_HEADER_LEN..])?,
        };
        message.validate_common()?;
        Ok(message)
    }

    fn validate_common(&self) -> Result<()> {
        if self.request_id == 0 {
            return Err(Error::Protocol(
                "integration request_id must be non-zero".into(),
            ));
        }
        match &self.body {
            IntegrationMessageBodyV3::Request { task_id, .. } => {
                if self.sequence != 0 || task_id.is_empty() {
                    return Err(Error::Protocol(
                        "Request requires sequence zero and non-empty task_id".into(),
                    ));
                }
            }
            IntegrationMessageBodyV3::Metadata { digest, .. } => {
                if self.sequence != 0 {
                    return Err(Error::Protocol("Metadata sequence must be zero".into()));
                }
                digest.validate()?;
            }
            IntegrationMessageBodyV3::Data(payload) => {
                if payload.is_empty() {
                    return Err(Error::Protocol("Data payload must not be empty".into()));
                }
            }
            IntegrationMessageBodyV3::End { chunk_count, .. } => {
                if *chunk_count != self.sequence {
                    return Err(Error::Protocol(
                        "End chunk count must equal sequence".into(),
                    ));
                }
            }
            IntegrationMessageBodyV3::Error { message, .. } => {
                if self.sequence != 0 || message.is_empty() {
                    return Err(Error::Protocol(
                        "Error requires sequence zero and non-empty message".into(),
                    ));
                }
            }
        }
        Ok(())
    }

    fn encode_payload(&self) -> Result<Vec<u8>> {
        match &self.body {
            IntegrationMessageBodyV3::Request {
                task_id,
                piece_number,
            } => {
                let task = task_id.as_bytes();
                let task_len = u32::try_from(task.len())
                    .map_err(|_| Error::Protocol("Request task_id exceeds u32".into()))?;
                let mut out = Vec::with_capacity(8 + task.len());
                out.extend_from_slice(&piece_number.to_be_bytes());
                out.extend_from_slice(&task_len.to_be_bytes());
                out.extend_from_slice(task);
                Ok(out)
            }
            IntegrationMessageBodyV3::Metadata {
                offset,
                total_length,
                digest,
            } => {
                let bytes = digest.value.as_bytes();
                let digest_len = u16::try_from(bytes.len())
                    .map_err(|_| Error::Protocol("digest value exceeds u16".into()))?;
                let mut out = Vec::with_capacity(INTEGRATION_METADATA_PREFIX_LEN + bytes.len());
                out.extend_from_slice(&offset.to_be_bytes());
                out.extend_from_slice(&total_length.to_be_bytes());
                out.extend_from_slice(&digest.algorithm.wire_value().to_be_bytes());
                out.extend_from_slice(&digest_len.to_be_bytes());
                out.extend_from_slice(bytes);
                Ok(out)
            }
            IntegrationMessageBodyV3::Data(payload) => Ok(payload.clone()),
            IntegrationMessageBodyV3::End {
                total_length,
                chunk_count,
            } => {
                let mut out = Vec::with_capacity(12);
                out.extend_from_slice(&total_length.to_be_bytes());
                out.extend_from_slice(&chunk_count.to_be_bytes());
                Ok(out)
            }
            IntegrationMessageBodyV3::Error { code, message } => {
                let bytes = message.as_bytes();
                let mut out = Vec::with_capacity(4 + bytes.len());
                out.extend_from_slice(&code.to_be_bytes());
                out.extend_from_slice(bytes);
                Ok(out)
            }
        }
    }

    fn decode_payload(
        message_type: IntegrationMessageTypeV3,
        payload: &[u8],
    ) -> Result<IntegrationMessageBodyV3> {
        match message_type {
            IntegrationMessageTypeV3::Request => {
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
                Ok(IntegrationMessageBodyV3::Request {
                    task_id,
                    piece_number,
                })
            }
            IntegrationMessageTypeV3::Metadata => {
                if payload.len() < INTEGRATION_METADATA_PREFIX_LEN {
                    return Err(Error::Protocol("truncated Metadata v3 payload".into()));
                }
                let digest_len =
                    u16::from_be_bytes(payload[18..20].try_into().expect("fixed slice")) as usize;
                if digest_len == 0 || digest_len > MAX_DIGEST_VALUE_LEN {
                    return Err(Error::Protocol(format!(
                        "Metadata v3 digest length {digest_len} is outside 1..={MAX_DIGEST_VALUE_LEN}"
                    )));
                }
                if payload.len() != INTEGRATION_METADATA_PREFIX_LEN + digest_len {
                    return Err(Error::Protocol(format!(
                        "Metadata v3 digest length {digest_len} disagrees with payload length {}",
                        payload.len()
                    )));
                }
                let algorithm = DigestAlgorithm::from_wire(u16::from_be_bytes(
                    payload[16..18].try_into().expect("fixed slice"),
                ))?;
                let value = std::str::from_utf8(&payload[20..])
                    .map_err(|_| Error::Protocol("Metadata v3 digest is not UTF-8".into()))?
                    .to_owned();
                Ok(IntegrationMessageBodyV3::Metadata {
                    offset: u64::from_be_bytes(payload[0..8].try_into().expect("fixed slice")),
                    total_length: u64::from_be_bytes(
                        payload[8..16].try_into().expect("fixed slice"),
                    ),
                    digest: DigestDescriptor::new(algorithm, value)?,
                })
            }
            IntegrationMessageTypeV3::Data => Ok(IntegrationMessageBodyV3::Data(payload.to_vec())),
            IntegrationMessageTypeV3::End => {
                if payload.len() != 12 {
                    return Err(Error::Protocol("End payload length must be 12".into()));
                }
                Ok(IntegrationMessageBodyV3::End {
                    total_length: u64::from_be_bytes(
                        payload[0..8].try_into().expect("fixed slice"),
                    ),
                    chunk_count: u32::from_be_bytes(
                        payload[8..12].try_into().expect("fixed slice"),
                    ),
                })
            }
            IntegrationMessageTypeV3::Error => {
                if payload.len() < 5 {
                    return Err(Error::Protocol("truncated Error payload".into()));
                }
                Ok(IntegrationMessageBodyV3::Error {
                    code: u32::from_be_bytes(payload[0..4].try_into().expect("fixed slice")),
                    message: std::str::from_utf8(&payload[4..])
                        .map_err(|_| Error::Protocol("Error message is not UTF-8".into()))?
                        .to_owned(),
                })
            }
        }
    }
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

    fn set_payload_len(frame: &mut [u8], length: usize) {
        frame[20..24].copy_from_slice(&(length as u32).to_be_bytes());
    }

    fn crc32_metadata(offset: u64, total_length: u64) -> IntegrationMessageV3 {
        IntegrationMessageV3::metadata(
            9,
            offset,
            total_length,
            DigestDescriptor::crc32(1_475_635_037),
        )
    }

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
    fn integration_v3_crc32_metadata_round_trip_with_nonzero_offset() {
        let message = crc32_metadata(4096, 512 * 1024);
        let encoded = message.encode().unwrap();
        assert_eq!(u16::from_be_bytes(encoded[4..6].try_into().unwrap()), 3);
        assert_eq!(
            u16::from_be_bytes(encoded[40..42].try_into().unwrap()),
            DigestAlgorithm::CRC32_WIRE_VALUE
        );
        assert_eq!(IntegrationMessageV3::decode(&encoded), Ok(message));
    }

    #[test]
    fn integration_v3_metadata_accepts_zero_length_piece() {
        let message = crc32_metadata(0, 0);
        assert_eq!(
            IntegrationMessageV3::decode(&message.encode().unwrap()),
            Ok(message)
        );
    }

    #[test]
    fn integration_v3_metadata_accepts_max_supported_digest_length() {
        let digest = DigestDescriptor::new(DigestAlgorithm::Sha256, "a".repeat(64)).unwrap();
        assert_eq!(digest.value.len(), MAX_DIGEST_VALUE_LEN);
        let message = IntegrationMessageV3::metadata(1, u64::MAX, 0, digest);
        assert_eq!(
            IntegrationMessageV3::decode(&message.encode().unwrap()),
            Ok(message)
        );
    }

    #[test]
    fn integration_v3_rejects_truncated_metadata() {
        let mut bytes = crc32_metadata(0, 1).encode().unwrap();
        bytes.pop();
        let payload_len = bytes.len() - DATA_HEADER_LEN;
        set_payload_len(&mut bytes, payload_len);
        assert!(IntegrationMessageV3::decode(&bytes).is_err());

        let mut truncated_prefix = crc32_metadata(0, 1).encode().unwrap();
        truncated_prefix.truncate(DATA_HEADER_LEN + INTEGRATION_METADATA_PREFIX_LEN - 1);
        let payload_len = truncated_prefix.len() - DATA_HEADER_LEN;
        set_payload_len(&mut truncated_prefix, payload_len);
        assert!(IntegrationMessageV3::decode(&truncated_prefix).is_err());
    }

    #[test]
    fn integration_v3_rejects_unknown_digest_algorithm() {
        let mut bytes = crc32_metadata(0, 1).encode().unwrap();
        bytes[40..42].copy_from_slice(&99u16.to_be_bytes());
        assert!(IntegrationMessageV3::decode(&bytes).is_err());
    }

    #[test]
    fn integration_v3_rejects_digest_length_mismatch() {
        let mut bytes = crc32_metadata(0, 1).encode().unwrap();
        let length = u16::from_be_bytes(bytes[42..44].try_into().unwrap());
        bytes[42..44].copy_from_slice(&(length + 1).to_be_bytes());
        assert!(IntegrationMessageV3::decode(&bytes).is_err());
    }

    #[test]
    fn integration_v3_rejects_metadata_trailing_bytes() {
        let mut bytes = crc32_metadata(0, 1).encode().unwrap();
        bytes.push(0);
        let payload_len = bytes.len() - DATA_HEADER_LEN;
        set_payload_len(&mut bytes, payload_len);
        assert!(IntegrationMessageV3::decode(&bytes).is_err());
    }

    #[test]
    fn integration_v3_rejects_wrong_protocol_version() {
        let mut bytes = crc32_metadata(0, 1).encode().unwrap();
        bytes[4..6].copy_from_slice(&DATA_VERSION.to_be_bytes());
        assert!(IntegrationMessageV3::decode(&bytes).is_err());
    }

    #[test]
    fn integration_v3_non_metadata_messages_round_trip() {
        let messages = [
            IntegrationMessageV3::request(9, "task-a", 4),
            IntegrationMessageV3::data(9, 0, vec![1, 2, 3]),
            IntegrationMessageV3::end(9, 1, 3),
            IntegrationMessageV3::error(9, 42, "not found"),
        ];
        for message in messages {
            assert_eq!(
                IntegrationMessageV3::decode(&message.encode().unwrap()),
                Ok(message)
            );
        }
    }

    #[test]
    fn integration_v3_supports_one_mib_data_payload() {
        let message = IntegrationMessageV3::data(7, 0, vec![0x5a; 1024 * 1024]);
        let encoded = message.encode().unwrap();
        assert_eq!(encoded.len(), DATA_HEADER_LEN + 1024 * 1024);
        assert_eq!(IntegrationMessageV3::decode(&encoded), Ok(message));
    }

    #[test]
    fn v2_and_v3_decoders_do_not_reinterpret_each_other() {
        let v2 = Message::metadata(9, 0, 0, [0; SHA256_DIGEST_LEN])
            .encode()
            .unwrap();
        let v3 = crc32_metadata(0, 0).encode().unwrap();
        assert!(IntegrationMessageV3::decode(&v2).is_err());
        assert!(Message::decode(&v3).is_err());
    }

    #[test]
    fn digest_descriptor_matches_dragonfly_crc32_string() {
        let digest = DigestDescriptor::crc32(1_475_635_037);
        assert_eq!(digest.external_string().unwrap(), "crc32:1475635037");
        assert!(DigestDescriptor::new(DigestAlgorithm::Crc32, "").is_err());
        assert!(DigestDescriptor::new(DigestAlgorithm::Crc32, "01").is_err());
        assert!(DigestDescriptor::new(DigestAlgorithm::Crc32, "4294967296").is_err());
        assert!(DigestDescriptor::new(DigestAlgorithm::Sha256, "A".repeat(64)).is_err());
        assert!(DigestDescriptor::new(DigestAlgorithm::Sha256, "a".repeat(65)).is_err());
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
