#[cfg(any(feature = "urma", test))]
use crate::{Error, Result};
#[cfg(any(feature = "urma", test))]
use std::io::{Read, Write};

pub const OOB_MAGIC: u32 = 0x5552_4d41;
pub const OOB_VERSION: u16 = 2;
pub const MAX_OOB_PAYLOAD_LEN: usize = 128 * 1024;
#[cfg(any(feature = "urma", test))]
const HEADER_LEN: usize = 12;

#[cfg(any(feature = "urma", test))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u16)]
enum MessageType {
    Hello = 1,
    HelloAck = 2,
    Ready = 3,
    ReadyAck = 4,
}

#[cfg(any(feature = "urma", test))]
impl TryFrom<u16> for MessageType {
    type Error = Error;

    fn try_from(value: u16) -> Result<Self> {
        match value {
            1 => Ok(Self::Hello),
            2 => Ok(Self::HelloAck),
            3 => Ok(Self::Ready),
            4 => Ok(Self::ReadyAck),
            _ => Err(Error::Protocol(format!("unknown OOB message type {value}"))),
        }
    }
}

#[cfg(any(feature = "urma", test))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
enum Role {
    Parent = 1,
    Child = 2,
}

#[cfg(any(feature = "urma", test))]
impl TryFrom<u8> for Role {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            1 => Ok(Self::Parent),
            2 => Ok(Self::Child),
            _ => Err(Error::Protocol(format!("invalid OOB role {value}"))),
        }
    }
}

#[cfg(any(feature = "urma", test))]
#[derive(Clone, Debug, Eq, PartialEq)]
struct CapabilityWire {
    transport_type: i32,
    eid_index: u32,
    max_jfc_depth: u32,
    max_jfs_depth: u32,
    max_jfr_depth: u32,
    max_jfs_sge: u32,
    max_jfr_sge: u32,
    max_msg_size: u64,
    transport_modes: u16,
}

#[cfg(any(feature = "urma", test))]
impl CapabilityWire {
    const LEN: usize = 38;

    #[cfg(feature = "urma")]
    fn from_capability(capability: &crate::UrmaDeviceCapability) -> Self {
        Self {
            transport_type: capability.transport_type,
            eid_index: capability.selected_eid_index,
            max_jfc_depth: capability.max_jfc_depth,
            max_jfs_depth: capability.max_jfs_depth,
            max_jfr_depth: capability.max_jfr_depth,
            max_jfs_sge: capability.max_jfs_sge,
            max_jfr_sge: capability.max_jfr_sge,
            max_msg_size: capability.max_msg_size,
            transport_modes: capability.transport_modes,
        }
    }

    fn encode(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.transport_type.to_be_bytes());
        out.extend_from_slice(&self.eid_index.to_be_bytes());
        out.extend_from_slice(&self.max_jfc_depth.to_be_bytes());
        out.extend_from_slice(&self.max_jfs_depth.to_be_bytes());
        out.extend_from_slice(&self.max_jfr_depth.to_be_bytes());
        out.extend_from_slice(&self.max_jfs_sge.to_be_bytes());
        out.extend_from_slice(&self.max_jfr_sge.to_be_bytes());
        out.extend_from_slice(&self.max_msg_size.to_be_bytes());
        out.extend_from_slice(&self.transport_modes.to_be_bytes());
    }

    fn decode(input: &[u8]) -> Result<Self> {
        if input.len() != Self::LEN {
            return Err(Error::Protocol(format!(
                "capability payload length {} is not {}",
                input.len(),
                Self::LEN
            )));
        }
        Ok(Self {
            transport_type: i32::from_be_bytes(input[0..4].try_into().expect("fixed slice")),
            eid_index: u32::from_be_bytes(input[4..8].try_into().expect("fixed slice")),
            max_jfc_depth: u32::from_be_bytes(input[8..12].try_into().expect("fixed slice")),
            max_jfs_depth: u32::from_be_bytes(input[12..16].try_into().expect("fixed slice")),
            max_jfr_depth: u32::from_be_bytes(input[16..20].try_into().expect("fixed slice")),
            max_jfs_sge: u32::from_be_bytes(input[20..24].try_into().expect("fixed slice")),
            max_jfr_sge: u32::from_be_bytes(input[24..28].try_into().expect("fixed slice")),
            max_msg_size: u64::from_be_bytes(input[28..36].try_into().expect("fixed slice")),
            transport_modes: u16::from_be_bytes(input[36..38].try_into().expect("fixed slice")),
        })
    }
}

#[cfg(any(feature = "urma", test))]
struct Frame {
    message_type: MessageType,
    payload: Vec<u8>,
}

#[cfg(any(feature = "urma", test))]
impl Frame {
    fn write_to(&self, writer: &mut impl Write) -> Result<()> {
        if self.payload.len() > MAX_OOB_PAYLOAD_LEN {
            return Err(Error::Protocol("outbound OOB payload is too large".into()));
        }
        let length = u32::try_from(self.payload.len())
            .map_err(|_| Error::Protocol("outbound OOB payload exceeds u32".into()))?;
        writer
            .write_all(&OOB_MAGIC.to_be_bytes())
            .and_then(|_| writer.write_all(&OOB_VERSION.to_be_bytes()))
            .and_then(|_| writer.write_all(&(self.message_type as u16).to_be_bytes()))
            .and_then(|_| writer.write_all(&length.to_be_bytes()))
            .and_then(|_| writer.write_all(&self.payload))
            .map_err(|error| io_error("write OOB frame", error))
    }

    fn read_from(reader: &mut impl Read) -> Result<Self> {
        let mut header = [0u8; HEADER_LEN];
        reader
            .read_exact(&mut header)
            .map_err(|error| io_error("read OOB frame header", error))?;
        let magic = u32::from_be_bytes(header[0..4].try_into().expect("fixed slice"));
        if magic != OOB_MAGIC {
            return Err(Error::Protocol(format!("invalid OOB magic 0x{magic:08x}")));
        }
        let version = u16::from_be_bytes(header[4..6].try_into().expect("fixed slice"));
        if version != OOB_VERSION {
            return Err(Error::Protocol(format!(
                "unsupported OOB version {version}"
            )));
        }
        let message_type = MessageType::try_from(u16::from_be_bytes(
            header[6..8].try_into().expect("fixed slice"),
        ))?;
        let length = u32::from_be_bytes(header[8..12].try_into().expect("fixed slice")) as usize;
        if length > MAX_OOB_PAYLOAD_LEN {
            return Err(Error::Protocol(format!(
                "OOB payload length {length} exceeds {MAX_OOB_PAYLOAD_LEN}"
            )));
        }
        let mut payload = vec![0u8; length];
        reader
            .read_exact(&mut payload)
            .map_err(|error| io_error("read OOB frame payload", error))?;
        Ok(Self {
            message_type,
            payload,
        })
    }
}

#[cfg(any(feature = "urma", test))]
fn io_error(operation: &'static str, error: std::io::Error) -> Error {
    Error::Io {
        operation,
        message: error.to_string(),
    }
}

#[cfg(feature = "urma")]
mod native {
    use super::*;
    use crate::{JettyDescriptor, UrmaConnection};
    use std::net::{Shutdown, TcpStream};

    pub struct OobSession {
        stream: TcpStream,
    }

    impl OobSession {
        pub(crate) fn stream_mut(&mut self) -> &mut TcpStream {
            &mut self.stream
        }

        /// M2 carries no post-handshake messages. Parent waits until child closes.
        pub fn wait_for_peer_close(mut self) -> Result<()> {
            let mut byte = [0u8; 1];
            match self.stream.read(&mut byte) {
                Ok(0) => Ok(()),
                Ok(_) => Err(Error::Protocol(
                    "unexpected data received after READY_ACK".into(),
                )),
                Err(error) => Err(io_error("wait for peer close", error)),
            }
        }

        pub fn close(self) -> Result<()> {
            self.stream
                .shutdown(Shutdown::Both)
                .map_err(|error| io_error("close OOB socket", error))
        }
    }

    pub fn parent_handshake(
        mut stream: TcpStream,
        connection: &mut UrmaConnection<'_>,
    ) -> Result<OobSession> {
        let result = parent_handshake_inner(&mut stream, connection);
        finish_handshake(result, stream, connection)
    }

    pub fn child_handshake(
        mut stream: TcpStream,
        connection: &mut UrmaConnection<'_>,
    ) -> Result<OobSession> {
        let result = child_handshake_inner(&mut stream, connection);
        finish_handshake(result, stream, connection)
    }

    fn finish_handshake(
        result: Result<()>,
        stream: TcpStream,
        connection: &mut UrmaConnection<'_>,
    ) -> Result<OobSession> {
        match result {
            Ok(()) => {
                stream
                    .set_nodelay(true)
                    .map_err(|error| io_error("configure OOB TCP_NODELAY", error))?;
                Ok(OobSession { stream })
            }
            Err(error) => {
                connection.fail();
                let _ = stream.shutdown(Shutdown::Both);
                Err(error)
            }
        }
    }

    fn parent_handshake_inner(
        stream: &mut TcpStream,
        connection: &mut UrmaConnection<'_>,
    ) -> Result<()> {
        let hello = expect_frame(stream, MessageType::Hello)?;
        if hello.payload.len() <= 1 + CapabilityWire::LEN {
            return Err(Error::Protocol(
                "HELLO lacks a Child Jetty descriptor".into(),
            ));
        }
        let peer_capability =
            decode_role_capability(&hello.payload[..1 + CapabilityWire::LEN], Role::Child)?;
        validate_peer_capability(connection, &peer_capability)?;
        eprintln!("parent: HELLO received and capability validated");
        let child_descriptor =
            JettyDescriptor::deserialize(&hello.payload[1 + CapabilityWire::LEN..])?;
        if child_descriptor.eid_index != peer_capability.eid_index {
            return Err(Error::Protocol(
                "Child descriptor EID index disagrees with capability".into(),
            ));
        }

        let descriptor = connection.export_descriptor()?;
        connection.import_and_bind(&child_descriptor)?;
        connection.recv_ready()?;
        eprintln!("parent: Child descriptor imported, Bound, RX posted");
        let mut payload = encode_role_capability(Role::Parent, connection);
        payload.extend_from_slice(&descriptor.serialize()?);
        Frame {
            message_type: MessageType::HelloAck,
            payload,
        }
        .write_to(stream)?;
        eprintln!("parent: descriptor sent");

        let ready = expect_frame(stream, MessageType::Ready)?;
        expect_role_only(&ready.payload, Role::Child)?;
        eprintln!("parent: child reported Bound");
        Frame {
            message_type: MessageType::ReadyAck,
            payload: vec![Role::Parent as u8],
        }
        .write_to(stream)?;
        eprintln!("parent: READY_ACK sent");
        connection.mark_ready()
    }

    fn child_handshake_inner(
        stream: &mut TcpStream,
        connection: &mut UrmaConnection<'_>,
    ) -> Result<()> {
        let descriptor = connection.export_descriptor()?;
        let mut hello_payload = encode_role_capability(Role::Child, connection);
        hello_payload.extend_from_slice(&descriptor.serialize()?);
        Frame {
            message_type: MessageType::Hello,
            payload: hello_payload,
        }
        .write_to(stream)?;
        eprintln!("child: HELLO sent");

        let ack = expect_frame(stream, MessageType::HelloAck)?;
        if ack.payload.len() <= 1 + CapabilityWire::LEN {
            return Err(Error::Protocol("HELLO_ACK lacks a Jetty descriptor".into()));
        }
        let capability =
            decode_role_capability(&ack.payload[..1 + CapabilityWire::LEN], Role::Parent)?;
        validate_peer_capability(connection, &capability)?;
        let descriptor = JettyDescriptor::deserialize(&ack.payload[1 + CapabilityWire::LEN..])?;
        if descriptor.eid_index != capability.eid_index {
            return Err(Error::Protocol(
                "descriptor EID index disagrees with parent capability".into(),
            ));
        }
        eprintln!("child: descriptor received and validated");
        connection.import_and_bind(&descriptor)?;
        connection.recv_ready()?;
        eprintln!("child: descriptor imported; Jetty Bound; RX posted");
        Frame {
            message_type: MessageType::Ready,
            payload: vec![Role::Child as u8],
        }
        .write_to(stream)?;
        let ready_ack = expect_frame(stream, MessageType::ReadyAck)?;
        expect_role_only(&ready_ack.payload, Role::Parent)?;
        eprintln!("child: READY_ACK received");
        connection.mark_ready()
    }

    fn expect_frame(stream: &mut TcpStream, expected: MessageType) -> Result<Frame> {
        let frame = Frame::read_from(stream)?;
        if frame.message_type != expected {
            return Err(Error::Protocol(format!(
                "received {:?}, expected {expected:?}",
                frame.message_type
            )));
        }
        Ok(frame)
    }

    fn encode_role_capability(role: Role, connection: &UrmaConnection<'_>) -> Vec<u8> {
        let mut payload = Vec::with_capacity(1 + CapabilityWire::LEN);
        payload.push(role as u8);
        CapabilityWire::from_capability(connection.capability()).encode(&mut payload);
        payload
    }

    fn decode_role_capability(input: &[u8], expected: Role) -> Result<CapabilityWire> {
        if input.len() != 1 + CapabilityWire::LEN {
            return Err(Error::Protocol("invalid HELLO capability length".into()));
        }
        let role = Role::try_from(input[0])?;
        if role != expected {
            return Err(Error::Protocol(format!(
                "peer role {role:?}, expected {expected:?}"
            )));
        }
        CapabilityWire::decode(&input[1..])
    }

    fn expect_role_only(input: &[u8], expected: Role) -> Result<()> {
        if input.len() != 1 || Role::try_from(input[0])? != expected {
            return Err(Error::Protocol(format!(
                "invalid role payload, expected {expected:?}"
            )));
        }
        Ok(())
    }

    fn validate_peer_capability(
        connection: &UrmaConnection<'_>,
        peer: &CapabilityWire,
    ) -> Result<()> {
        if peer.transport_type != connection.capability().transport_type {
            return Err(Error::Protocol(format!(
                "peer transport type {} does not match local {}",
                peer.transport_type,
                connection.capability().transport_type
            )));
        }
        Ok(())
    }
}

#[cfg(feature = "urma")]
pub use native::{child_handshake, parent_handshake, OobSession};

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn encoded_header(magic: u32, version: u16, length: u32) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&magic.to_be_bytes());
        bytes.extend_from_slice(&version.to_be_bytes());
        bytes.extend_from_slice(&(MessageType::Hello as u16).to_be_bytes());
        bytes.extend_from_slice(&length.to_be_bytes());
        bytes
    }

    #[test]
    fn rejects_invalid_magic() {
        assert!(Frame::read_from(&mut Cursor::new(encoded_header(0, OOB_VERSION, 0))).is_err());
    }

    #[test]
    fn rejects_invalid_version() {
        assert!(Frame::read_from(&mut Cursor::new(encoded_header(
            OOB_MAGIC,
            OOB_VERSION + 1,
            0,
        )))
        .is_err());
    }

    #[test]
    fn rejects_oversized_payload_before_allocation() {
        assert!(Frame::read_from(&mut Cursor::new(encoded_header(
            OOB_MAGIC,
            OOB_VERSION,
            (MAX_OOB_PAYLOAD_LEN + 1) as u32,
        )))
        .is_err());
    }

    #[test]
    fn frame_and_capability_use_explicit_wire_encoding() {
        let capability = CapabilityWire {
            transport_type: 3,
            eid_index: 7,
            max_jfc_depth: 64,
            max_jfs_depth: 63,
            max_jfr_depth: 62,
            max_jfs_sge: 2,
            max_jfr_sge: 1,
            max_msg_size: 4096,
            transport_modes: 4,
        };
        let mut payload = vec![Role::Child as u8];
        capability.encode(&mut payload);
        assert_eq!(Role::try_from(payload[0]), Ok(Role::Child));
        assert_eq!(CapabilityWire::decode(&payload[1..]), Ok(capability));

        let frame = Frame {
            message_type: MessageType::Hello,
            payload,
        };
        let mut bytes = Vec::new();
        frame.write_to(&mut bytes).expect("encode frame");
        let decoded = Frame::read_from(&mut Cursor::new(bytes)).expect("decode frame");
        assert_eq!(decoded.message_type, MessageType::Hello);
        assert_eq!(decoded.payload[0], Role::Child as u8);
    }
}
