use crate::{Error, Message, MessageBody, Result};
use sha2::{Digest, Sha256};
use std::io::{Read, Write};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TransferSummary {
    pub bytes: u64,
    pub data_messages: u32,
    pub digest: [u8; 32],
}

enum Phase {
    AwaitMetadata,
    Receiving {
        total_length: u64,
        expected_digest: [u8; 32],
    },
    Complete,
}

pub struct ReceiveState {
    request_id: u64,
    phase: Phase,
    next_sequence: u32,
    received: u64,
    hasher: Sha256,
}

impl ReceiveState {
    pub fn new(request_id: u64) -> Result<Self> {
        if request_id == 0 {
            return Err(Error::Protocol("request_id must be non-zero".into()));
        }
        Ok(Self {
            request_id,
            phase: Phase::AwaitMetadata,
            next_sequence: 0,
            received: 0,
            hasher: Sha256::new(),
        })
    }

    pub fn accept(
        &mut self,
        message: &Message,
        output: &mut impl Write,
    ) -> Result<Option<TransferSummary>> {
        if message.request_id != self.request_id {
            return Err(Error::Protocol(format!(
                "message request_id {} does not match {}",
                message.request_id, self.request_id
            )));
        }
        match (&self.phase, &message.body) {
            (
                Phase::AwaitMetadata,
                MessageBody::Metadata {
                    offset,
                    total_length,
                    digest,
                },
            ) => {
                if message.sequence != 0 {
                    return Err(Error::Protocol("Metadata sequence must be zero".into()));
                }
                if *offset != 0 {
                    return Err(Error::Protocol(format!(
                        "demo only supports offset zero, got {offset}"
                    )));
                }
                self.phase = Phase::Receiving {
                    total_length: *total_length,
                    expected_digest: *digest,
                };
                Ok(None)
            }
            (Phase::AwaitMetadata, MessageBody::Error { code, message }) => {
                Err(Error::Protocol(format!("remote error {code}: {message}")))
            }
            (Phase::AwaitMetadata, _) => Err(Error::Protocol("expected Metadata or Error".into())),
            (Phase::Receiving { total_length, .. }, MessageBody::Data(payload)) => {
                if message.sequence != self.next_sequence {
                    return Err(Error::Protocol(format!(
                        "Data sequence {}, expected {}",
                        message.sequence, self.next_sequence
                    )));
                }
                let length = u64::try_from(payload.len())
                    .map_err(|_| Error::Protocol("Data length exceeds u64".into()))?;
                let next_received = self
                    .received
                    .checked_add(length)
                    .ok_or_else(|| Error::Protocol("received length overflow".into()))?;
                if next_received > *total_length {
                    return Err(Error::Protocol(format!(
                        "Data exceeds advertised length {total_length}"
                    )));
                }
                output.write_all(payload).map_err(|error| Error::Io {
                    operation: "write output file",
                    message: error.to_string(),
                })?;
                self.hasher.update(payload);
                self.received = next_received;
                self.next_sequence = self
                    .next_sequence
                    .checked_add(1)
                    .ok_or_else(|| Error::Protocol("Data sequence overflow".into()))?;
                Ok(None)
            }
            (
                Phase::Receiving {
                    total_length,
                    expected_digest,
                },
                MessageBody::End {
                    total_length: end_length,
                    chunk_count,
                },
            ) => {
                if message.sequence != self.next_sequence || *chunk_count != self.next_sequence {
                    return Err(Error::Protocol(format!(
                        "End chunk count/sequence does not match {}",
                        self.next_sequence
                    )));
                }
                if end_length != total_length || self.received != *total_length {
                    return Err(Error::Protocol(format!(
                        "End length {end_length}, received {}, advertised {total_length}",
                        self.received
                    )));
                }
                let digest: [u8; 32] = self.hasher.clone().finalize().into();
                if &digest != expected_digest {
                    return Err(Error::Protocol(format!(
                        "digest mismatch: expected {}, got {}",
                        hex_digest(expected_digest),
                        hex_digest(&digest)
                    )));
                }
                self.phase = Phase::Complete;
                Ok(Some(TransferSummary {
                    bytes: self.received,
                    data_messages: self.next_sequence,
                    digest,
                }))
            }
            (Phase::Receiving { .. }, MessageBody::Error { code, message }) => {
                Err(Error::Protocol(format!("remote error {code}: {message}")))
            }
            (Phase::Receiving { .. }, _) => {
                Err(Error::Protocol("expected Data, End, or Error".into()))
            }
            (Phase::Complete, _) => Err(Error::Protocol(
                "message received after transfer completion".into(),
            )),
        }
    }
}

pub fn digest_reader(reader: &mut impl Read) -> Result<([u8; 32], u64)> {
    let mut hasher = Sha256::new();
    let mut length = 0u64;
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let read = reader.read(&mut buffer).map_err(|error| Error::Io {
            operation: "read input file for digest",
            message: error.to_string(),
        })?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        length = length
            .checked_add(read as u64)
            .ok_or_else(|| Error::Protocol("input length overflow".into()))?;
    }
    Ok((hasher.finalize().into(), length))
}

pub fn hex_digest(digest: &[u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(64);
    for byte in digest {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0xf) as usize] as char);
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::MAX_DATA_PAYLOAD_LEN;
    use std::io::Cursor;

    fn digest(bytes: &[u8]) -> [u8; 32] {
        Sha256::digest(bytes).into()
    }

    fn transfer(chunks: &[&[u8]]) -> Result<(TransferSummary, Vec<u8>)> {
        let all = chunks.concat();
        let mut state = ReceiveState::new(7)?;
        let mut output = Vec::new();
        state.accept(
            &Message::metadata(7, 0, all.len() as u64, digest(&all)),
            &mut output,
        )?;
        for (sequence, chunk) in chunks.iter().enumerate() {
            state.accept(
                &Message::data(7, sequence as u32, chunk.to_vec()),
                &mut output,
            )?;
        }
        let summary = state
            .accept(
                &Message::end(7, chunks.len() as u32, all.len() as u64),
                &mut output,
            )?
            .expect("End completes transfer");
        Ok((summary, output))
    }

    #[test]
    fn accepts_empty_small_and_multi_message_files() {
        let cases: Vec<Vec<Vec<u8>>> = vec![
            vec![],
            vec![b"small".to_vec()],
            vec![
                vec![1; MAX_DATA_PAYLOAD_LEN],
                vec![2; MAX_DATA_PAYLOAD_LEN],
                vec![3; 17],
            ],
        ];
        for chunks in cases {
            let refs: Vec<&[u8]> = chunks.iter().map(Vec::as_slice).collect();
            let expected = refs.concat();
            let (summary, output) = transfer(&refs).unwrap();
            assert_eq!(output, expected);
            assert_eq!(summary.bytes, expected.len() as u64);
            assert_eq!(summary.data_messages, refs.len() as u32);
        }
    }

    #[test]
    fn rejects_sequence_gap_duplicate_and_out_of_order() {
        for sequence in [1, 2] {
            let mut state = ReceiveState::new(7).unwrap();
            let mut output = Vec::new();
            state
                .accept(&Message::metadata(7, 0, 2, digest(b"ab")), &mut output)
                .unwrap();
            assert!(state
                .accept(&Message::data(7, sequence, b"a".to_vec()), &mut output)
                .is_err());
        }

        let mut state = ReceiveState::new(7).unwrap();
        let mut output = Vec::new();
        state
            .accept(&Message::metadata(7, 0, 2, digest(b"aa")), &mut output)
            .unwrap();
        state
            .accept(&Message::data(7, 0, b"a".to_vec()), &mut output)
            .unwrap();
        assert!(state
            .accept(&Message::data(7, 0, b"a".to_vec()), &mut output)
            .is_err());
    }

    #[test]
    fn rejects_end_total_length_mismatch() {
        let mut state = ReceiveState::new(7).unwrap();
        let mut output = Vec::new();
        state
            .accept(&Message::metadata(7, 0, 1, digest(b"a")), &mut output)
            .unwrap();
        state
            .accept(&Message::data(7, 0, b"a".to_vec()), &mut output)
            .unwrap();
        assert!(state.accept(&Message::end(7, 1, 2), &mut output).is_err());
    }

    #[test]
    fn reports_error_message() {
        let mut state = ReceiveState::new(7).unwrap();
        let error = state
            .accept(&Message::error(7, 404, "missing"), &mut Vec::new())
            .unwrap_err();
        assert!(error.to_string().contains("404"));
    }

    #[test]
    fn digest_reader_is_streaming_and_correct() {
        let bytes = vec![9; MAX_DATA_PAYLOAD_LEN + 1];
        let (actual, length) = digest_reader(&mut Cursor::new(&bytes)).unwrap();
        assert_eq!(actual, digest(&bytes));
        assert_eq!(length, bytes.len() as u64);
    }
}
