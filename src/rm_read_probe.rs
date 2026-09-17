use crate::{Error, Result};

pub const RM_READ_PROBE_DEFAULT_BYTES: usize = 64 * 1024 * 1024;
pub const RM_READ_PROBE_DEFAULT_CHUNK: usize = 1024 * 1024;
pub const RM_READ_PROBE_DEFAULT_DEPTH: u32 = 128;
#[cfg(feature = "urma")]
const WIRE_VERSION: u32 = 1;
#[cfg(feature = "urma")]
const MAX_FRAME: usize = 128 * 1024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RmReadProbeConfig {
    pub device_name: String,
    pub eid_index: u32,
    pub transfer_bytes: usize,
    pub chunk_bytes: usize,
    pub queue_depth: u32,
    pub jetty_token: u32,
    pub segment_token: u32,
}

impl RmReadProbeConfig {
    pub fn new(device_name: impl Into<String>, eid_index: u32) -> Self {
        Self {
            device_name: device_name.into(),
            eid_index,
            transfer_bytes: RM_READ_PROBE_DEFAULT_BYTES,
            chunk_bytes: RM_READ_PROBE_DEFAULT_CHUNK,
            queue_depth: RM_READ_PROBE_DEFAULT_DEPTH,
            jetty_token: 0x4a46_524d,
            segment_token: 0x5345_4752,
        }
    }

    fn validate(&self) -> Result<()> {
        if self.device_name.is_empty()
            || self.transfer_bytes == 0
            || self.chunk_bytes == 0
            || self.chunk_bytes > u32::MAX as usize
            || self.queue_depth == 0
        {
            return Err(Error::InvalidConfiguration(
                "RM READ probe requires nonzero bytes, chunk, and queue depth".into(),
            ));
        }
        Ok(())
    }
}

pub fn probe_bytes(length: usize) -> Vec<u8> {
    (0..length)
        .map(|index| {
            let lane = (index as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15);
            (lane.rotate_left((index % 61) as u32) as u8) ^ (index >> 12) as u8
        })
        .collect()
}

#[cfg(feature = "urma")]
pub use native::{run_rm_read_child, run_rm_read_parent, RmReadProbeReport};

#[cfg(feature = "urma")]
mod native {
    use super::*;
    use crate::{
        ffi::{
            self, CompletionRecord, JettyConfig as NativeJettyConfig, JettyHandle, JfcHandle,
            JfceHandle, NativeRuntime, ReadDescriptorData, ReadSegmentHandle, ReadSourceHandle,
            SegmentHandle, WrHandle,
        },
        JettyDescriptor,
    };
    use sha2::{Digest, Sha256};
    use std::{
        collections::{BTreeSet, HashMap},
        ffi::CString,
        io::{Read, Write},
        net::TcpStream,
        thread,
        time::{Duration, Instant},
    };

    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct RmReadProbeReport {
        pub role: &'static str,
        pub transfer_bytes: usize,
        pub chunk_bytes: usize,
        pub completions: usize,
        pub max_read_size: u32,
        pub busy_unimport_status: Option<i32>,
        pub opcodes: Vec<u32>,
        pub completion_lengths: Vec<u32>,
        pub local_ids: Vec<u32>,
        pub is_jetty_values: Vec<bool>,
        pub remote_id_valid_values: Vec<bool>,
        pub imm_data_valid_values: Vec<bool>,
        pub content_verified: bool,
        pub owners_retired: bool,
        pub clean_shutdown: bool,
    }

    impl RmReadProbeReport {
        pub fn to_json(&self) -> String {
            format!(
                "{{\"state\":\"passed\",\"role\":\"{}\",\"transportMode\":\"RM\",\"tpType\":\"RTP\",\"transferBytes\":{},\"chunkBytes\":{},\"completions\":{},\"maxReadSize\":{},\"busyUnimportStatus\":{},\"opcodes\":{:?},\"completionLengths\":{:?},\"localIds\":{:?},\"isRecv\":false,\"userCtxValid\":true,\"isJettyValues\":{:?},\"remoteIdValidValues\":{:?},\"immDataValidValues\":{:?},\"contentVerified\":{},\"ownersRetired\":{},\"cleanShutdown\":{}}}",
                self.role,
                self.transfer_bytes,
                self.chunk_bytes,
                self.completions,
                self.max_read_size,
                self.busy_unimport_status.map_or("null".into(), |v| v.to_string()),
                self.opcodes,
                self.completion_lengths,
                self.local_ids,
                self.is_jetty_values,
                self.remote_id_valid_values,
                self.imm_data_valid_values,
                self.content_verified,
                self.owners_retired,
                self.clean_shutdown,
            )
        }
    }

    pub fn run_rm_read_parent(
        mut stream: TcpStream,
        config: &RmReadProbeConfig,
    ) -> Result<RmReadProbeReport> {
        config.validate()?;
        stream
            .set_nodelay(true)
            .map_err(|e| io_error("set TCP_NODELAY", e))?;
        let mut tree = ProbeTree::open(config, false)?;
        let source_bytes = probe_bytes(config.transfer_bytes);
        let digest = Sha256::digest(&source_bytes);
        let mut source = ReadSourceHandle::register(
            tree.runtime.as_mut().expect("runtime"),
            &source_bytes,
            config.segment_token,
        )
        .map_err(|e| ffi_error("register READ source", e))?;
        let read_descriptor = source
            .descriptor()
            .map_err(|e| ffi_error("export READ Segment", e))?;
        let jetty_descriptor = JettyDescriptor::from_ffi(
            tree.jetty
                .as_ref()
                .expect("jetty")
                .export_descriptor()
                .map_err(|e| ffi_error("export RM Jetty", e))?,
        )?;

        write_frame(&mut stream, &jetty_descriptor.serialize()?)?;
        write_frame(&mut stream, &encode_read_descriptor(&read_descriptor))?;
        let mut manifest = Vec::with_capacity(4 + 8 + 4 + digest.len());
        manifest.extend_from_slice(&WIRE_VERSION.to_be_bytes());
        manifest.extend_from_slice(&(config.transfer_bytes as u64).to_be_bytes());
        manifest.extend_from_slice(&(config.chunk_bytes as u32).to_be_bytes());
        manifest.extend_from_slice(&digest);
        write_frame(&mut stream, &manifest)?;
        expect_control(&mut stream, b"IMPORTED")?;
        expect_control(&mut stream, b"DRAINED_UNIMPORTED")?;

        source
            .close()
            .map_err(|e| ffi_error("unregister/release READ source", e))?;
        tree.close()?;
        write_frame(&mut stream, b"SOURCE_RELEASED")?;
        Ok(RmReadProbeReport {
            role: "parent",
            transfer_bytes: config.transfer_bytes,
            chunk_bytes: config.chunk_bytes,
            completions: 0,
            max_read_size: tree.max_read_size,
            busy_unimport_status: None,
            opcodes: Vec::new(),
            completion_lengths: Vec::new(),
            local_ids: Vec::new(),
            is_jetty_values: Vec::new(),
            remote_id_valid_values: Vec::new(),
            imm_data_valid_values: Vec::new(),
            content_verified: true,
            owners_retired: true,
            clean_shutdown: true,
        })
    }

    pub fn run_rm_read_child(
        mut stream: TcpStream,
        config: &RmReadProbeConfig,
    ) -> Result<RmReadProbeReport> {
        config.validate()?;
        stream
            .set_nodelay(true)
            .map_err(|e| io_error("set TCP_NODELAY", e))?;
        let mut tree = ProbeTree::open(config, true)?;
        let jetty_descriptor = JettyDescriptor::deserialize(&read_frame(&mut stream)?)?;
        let read_descriptor = decode_read_descriptor(&read_frame(&mut stream)?)?;
        let manifest = read_frame(&mut stream)?;
        let (transfer_bytes, chunk_bytes, expected_digest) = decode_manifest(&manifest)?;
        if transfer_bytes != config.transfer_bytes
            || chunk_bytes != config.chunk_bytes
            || read_descriptor.length != transfer_bytes as u64
        {
            return Err(Error::Protocol("RM READ manifest/config mismatch".into()));
        }
        if config.chunk_bytes > tree.max_read_size as usize {
            return Err(Error::InvalidConfiguration(format!(
                "chunk_bytes={} exceeds provider max_read_size={}",
                config.chunk_bytes, tree.max_read_size
            )));
        }

        let jetty = tree.jetty.as_mut().expect("jetty");
        jetty
            .import_rm(&jetty_descriptor.to_ffi()?, config.jetty_token)
            .map_err(|e| ffi_error("import RM Jetty", e))?;
        let mut remote = ReadSegmentHandle::import(
            jetty,
            &read_descriptor,
            config.segment_token,
            tree.max_read_size,
        )
        .map_err(|e| ffi_error("import READ Segment", e))?;
        let mut local = SegmentHandle::create(
            tree.runtime.as_mut().expect("runtime"),
            transfer_bytes as u64,
            4096,
        )
        .map_err(|e| ffi_error("create READ destination", e))?;
        write_frame(&mut stream, b"IMPORTED")?;

        let expected_cqes = transfer_bytes.div_ceil(chunk_bytes);
        let mut next = 0usize;
        let mut owners: HashMap<u64, WrHandle> = HashMap::new();
        let mut completed = 0usize;
        let mut busy_status = None;
        let mut raw = [CompletionRecord::default(); 16];
        let mut opcodes = BTreeSet::new();
        let mut completion_lengths = BTreeSet::new();
        let mut local_ids = BTreeSet::new();
        let mut is_jetty_values = BTreeSet::new();
        let mut remote_id_valid_values = BTreeSet::new();
        let mut imm_data_valid_values = BTreeSet::new();
        let deadline = Instant::now() + Duration::from_secs(60);

        while completed < expected_cqes {
            while next < expected_cqes && owners.len() < config.queue_depth as usize {
                let offset = next * chunk_bytes;
                let length = (transfer_bytes - offset).min(chunk_bytes) as u32;
                let user_ctx = next as u64 + 1;
                let posted = jetty
                    .post_read(
                        &local,
                        &remote,
                        offset as u64,
                        offset as u64,
                        length,
                        user_ctx,
                    )
                    .map_err(|e| ffi_error("post RM READ", e))?;
                if posted.status != 0 {
                    return Err(Error::Native {
                        operation: "post RM READ (ambiguous owner retained)",
                        status: posted.status,
                    });
                }
                let owner = posted.handle.ok_or(Error::NullHandle {
                    operation: "post RM READ",
                })?;
                if owners.insert(user_ctx, owner).is_some() {
                    return Err(Error::Protocol("duplicate READ user_ctx".into()));
                }
                next += 1;
                if busy_status.is_none() {
                    busy_status = match remote.close() {
                        Err(ffi::FfiError::Status(status)) if status == -libc::EBUSY => {
                            Some(status)
                        }
                        Err(error) => return Err(ffi_error("busy unimport probe", error)),
                        Ok(()) => {
                            return Err(Error::Protocol(
                                "READ Segment unimport succeeded with an outstanding owner".into(),
                            ))
                        }
                    };
                }
            }
            if Instant::now() >= deadline {
                return Err(Error::Timeout {
                    operation: "drain RM READ CQEs",
                });
            }
            let count = tree
                .send_jfc
                .as_ref()
                .expect("send JFC")
                .poll_into(&mut raw)
                .map_err(|e| ffi_error("poll RM READ JFC", e))?;
            if count == 0 {
                thread::yield_now();
                continue;
            }
            for record in &raw[..count] {
                opcodes.insert(record.opcode);
                completion_lengths.insert(record.completion_len);
                local_ids.insert(record.local_id);
                is_jetty_values.insert(record.is_jetty);
                remote_id_valid_values.insert(record.remote_id_valid);
                imm_data_valid_values.insert(record.imm_data_valid);
                if record.status != 0
                    || record.is_recv
                    || !record.user_ctx_valid
                    || record.event_kind != 0
                {
                    return Err(Error::Completion {
                        status: record.status,
                        opcode: record.opcode,
                        user_ctx: record.user_ctx,
                        sequence: None,
                        post_call: None,
                        post_index: None,
                        post_count: None,
                    });
                }
                let owner = owners.remove(&record.user_ctx).ok_or_else(|| {
                    Error::Protocol(format!(
                        "unknown or duplicate READ CQE user_ctx={}",
                        record.user_ctx
                    ))
                })?;
                owner.complete();
                completed += 1;
            }
        }
        if !owners.is_empty() || completed != expected_cqes {
            return Err(Error::Protocol(
                "READ owner drain was not exact-once".into(),
            ));
        }
        let received = local
            .read(0, transfer_bytes as u32)
            .map_err(|e| ffi_error("read READ destination", e))?;
        if Sha256::digest(&received).as_slice() != expected_digest {
            return Err(Error::Protocol("RM READ content digest mismatch".into()));
        }
        remote
            .close()
            .map_err(|e| ffi_error("unimport drained READ Segment", e))?;
        jetty
            .unimport()
            .map_err(|e| ffi_error("unimport RM Jetty", e))?;
        write_frame(&mut stream, b"DRAINED_UNIMPORTED")?;
        expect_control(&mut stream, b"SOURCE_RELEASED")?;
        local
            .close()
            .map_err(|e| ffi_error("delete READ destination", e))?;
        tree.close()?;

        Ok(RmReadProbeReport {
            role: "child",
            transfer_bytes,
            chunk_bytes,
            completions: completed,
            max_read_size: tree.max_read_size,
            busy_unimport_status: busy_status,
            opcodes: opcodes.into_iter().collect(),
            completion_lengths: completion_lengths.into_iter().collect(),
            local_ids: local_ids.into_iter().collect(),
            is_jetty_values: is_jetty_values.into_iter().collect(),
            remote_id_valid_values: remote_id_valid_values.into_iter().collect(),
            imm_data_valid_values: imm_data_valid_values.into_iter().collect(),
            content_verified: true,
            owners_retired: true,
            clean_shutdown: true,
        })
    }

    struct ProbeTree {
        runtime: Option<NativeRuntime>,
        jfce: Option<JfceHandle>,
        send_jfc: Option<JfcHandle>,
        recv_jfc: Option<JfcHandle>,
        jetty: Option<JettyHandle>,
        max_read_size: u32,
    }

    impl ProbeTree {
        fn open(config: &RmReadProbeConfig, needs_destination: bool) -> Result<Self> {
            let device =
                CString::new(config.device_name.as_str()).map_err(|_| Error::InvalidDeviceName)?;
            let mut runtime = NativeRuntime::open(&device, config.eid_index)
                .map_err(|e| ffi_error("open RM READ runtime", e))?;
            let capability = runtime
                .query_device()
                .map_err(|e| ffi_error("query RM READ capability", e))?;
            if capability.transport_modes & 1 == 0
                || capability.max_read_size == 0
                || capability.max_jfs_sge == 0
                || capability.max_jfs_rsge == 0
            {
                return Err(Error::InvalidConfiguration(
                    "provider lacks RM READ capability or required SGEs".into(),
                ));
            }
            if config.queue_depth > capability.max_jfs_depth {
                return Err(Error::InvalidConfiguration(format!(
                    "queue_depth={} exceeds max_jfs_depth={}",
                    config.queue_depth, capability.max_jfs_depth
                )));
            }
            if needs_destination && config.transfer_bytes > u32::MAX as usize {
                return Err(Error::InvalidConfiguration(
                    "probe destination is limited to u32 bytes".into(),
                ));
            }
            let jfce = JfceHandle::create(&mut runtime)
                .map_err(|e| ffi_error("create RM READ JFCE", e))?;
            let send_jfc = JfcHandle::create(&mut runtime, &jfce, config.queue_depth.max(16))
                .map_err(|e| ffi_error("create RM READ send JFC", e))?;
            let recv_jfc = JfcHandle::create(&mut runtime, &jfce, 16)
                .map_err(|e| ffi_error("create RM READ receive JFC", e))?;
            let native_config = NativeJettyConfig {
                send_depth: config.queue_depth,
                recv_depth: 16,
                max_send_sge: 1,
                max_recv_sge: 1,
                token: config.jetty_token,
            };
            let jetty = JettyHandle::create_rm(&mut runtime, &send_jfc, &recv_jfc, &native_config)
                .map_err(|e| ffi_error("create RM/RTP Jetty", e))?;
            Ok(Self {
                runtime: Some(runtime),
                jfce: Some(jfce),
                send_jfc: Some(send_jfc),
                recv_jfc: Some(recv_jfc),
                jetty: Some(jetty),
                max_read_size: capability.max_read_size,
            })
        }

        fn close(&mut self) -> Result<()> {
            if let Some(mut jetty) = self.jetty.take() {
                jetty.close().map_err(|e| ffi_error("delete RM Jetty", e))?;
            }
            if let Some(mut jfc) = self.recv_jfc.take() {
                jfc.close()
                    .map_err(|e| ffi_error("delete receive JFC", e))?;
            }
            if let Some(mut jfc) = self.send_jfc.take() {
                jfc.close().map_err(|e| ffi_error("delete send JFC", e))?;
            }
            if let Some(mut jfce) = self.jfce.take() {
                jfce.close().map_err(|e| ffi_error("delete JFCE", e))?;
            }
            if let Some(mut runtime) = self.runtime.take() {
                runtime
                    .close()
                    .map_err(|e| ffi_error("close RM READ runtime", e))?;
            }
            Ok(())
        }
    }

    fn encode_read_descriptor(value: &ReadDescriptorData) -> Vec<u8> {
        let mut out = Vec::with_capacity(52);
        out.extend_from_slice(&value.version.to_be_bytes());
        out.extend_from_slice(&value.eid);
        out.extend_from_slice(&value.uasid.to_be_bytes());
        out.extend_from_slice(&value.va.to_be_bytes());
        out.extend_from_slice(&value.length.to_be_bytes());
        out.extend_from_slice(&value.token_id.to_be_bytes());
        out.extend_from_slice(&value.access.to_be_bytes());
        out.extend_from_slice(&value.token_policy.to_be_bytes());
        out
    }

    fn decode_read_descriptor(input: &[u8]) -> Result<ReadDescriptorData> {
        if input.len() != 52 {
            return Err(Error::Protocol("invalid READ descriptor length".into()));
        }
        Ok(ReadDescriptorData {
            version: u32::from_be_bytes(input[0..4].try_into().expect("slice")),
            eid: input[4..20].try_into().expect("slice"),
            uasid: u32::from_be_bytes(input[20..24].try_into().expect("slice")),
            va: u64::from_be_bytes(input[24..32].try_into().expect("slice")),
            length: u64::from_be_bytes(input[32..40].try_into().expect("slice")),
            token_id: u32::from_be_bytes(input[40..44].try_into().expect("slice")),
            access: u32::from_be_bytes(input[44..48].try_into().expect("slice")),
            token_policy: u32::from_be_bytes(input[48..52].try_into().expect("slice")),
        })
    }

    fn decode_manifest(input: &[u8]) -> Result<(usize, usize, &[u8])> {
        if input.len() != 48
            || u32::from_be_bytes(input[0..4].try_into().expect("slice")) != WIRE_VERSION
        {
            return Err(Error::Protocol("invalid RM READ manifest".into()));
        }
        let bytes = usize::try_from(u64::from_be_bytes(input[4..12].try_into().expect("slice")))
            .map_err(|_| Error::Protocol("manifest length exceeds usize".into()))?;
        let chunk = u32::from_be_bytes(input[12..16].try_into().expect("slice")) as usize;
        Ok((bytes, chunk, &input[16..48]))
    }

    fn write_frame(stream: &mut TcpStream, payload: &[u8]) -> Result<()> {
        if payload.len() > MAX_FRAME {
            return Err(Error::Protocol("RM READ OOB frame exceeds limit".into()));
        }
        stream
            .write_all(&(payload.len() as u32).to_be_bytes())
            .and_then(|_| stream.write_all(payload))
            .map_err(|e| io_error("write RM READ OOB frame", e))
    }

    fn read_frame(stream: &mut TcpStream) -> Result<Vec<u8>> {
        let mut length = [0u8; 4];
        stream
            .read_exact(&mut length)
            .map_err(|e| io_error("read RM READ OOB length", e))?;
        let length = u32::from_be_bytes(length) as usize;
        if length > MAX_FRAME {
            return Err(Error::Protocol("RM READ OOB frame exceeds limit".into()));
        }
        let mut payload = vec![0u8; length];
        stream
            .read_exact(&mut payload)
            .map_err(|e| io_error("read RM READ OOB payload", e))?;
        Ok(payload)
    }

    fn expect_control(stream: &mut TcpStream, expected: &[u8]) -> Result<()> {
        let actual = read_frame(stream)?;
        if actual != expected {
            return Err(Error::Protocol(format!(
                "unexpected RM READ control frame: expected {:?}, got {:?}",
                String::from_utf8_lossy(expected),
                String::from_utf8_lossy(&actual)
            )));
        }
        Ok(())
    }

    fn ffi_error(operation: &'static str, error: ffi::FfiError) -> Error {
        match error {
            ffi::FfiError::Contract(detail) => Error::FfiContract { operation, detail },
            ffi::FfiError::NullHandle => Error::NullHandle { operation },
            ffi::FfiError::Status(status) => Error::Native { operation, status },
        }
    }

    fn io_error(operation: &'static str, error: std::io::Error) -> Error {
        Error::Io {
            operation,
            message: error.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn payload_is_deterministic_and_position_sensitive() {
        let left = probe_bytes(8192);
        let right = probe_bytes(8192);
        assert_eq!(left, right);
        assert_ne!(&left[..4096], &left[4096..]);
    }

    #[test]
    fn config_rejects_zero_sizing() {
        let mut config = RmReadProbeConfig::new("urma0", 0);
        config.chunk_bytes = 0;
        assert!(config.validate().is_err());
    }
}
