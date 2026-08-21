use crate::{Error, Result};

pub const JETTY_DESCRIPTOR_VERSION: u16 = 1;
pub const MAX_JETTY_DESCRIPTOR_LEN: usize = 64 * 1024;

/// Stable wire DTO around provider-owned opaque remote-Jetty bytes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JettyDescriptor {
    pub version: u16,
    pub transport_type: u32,
    pub eid_index: u32,
    pub jetty_id: u32,
    pub opaque_len: u32,
    pub opaque_data: Vec<u8>,
}

impl JettyDescriptor {
    const FIXED_LEN: usize = 2 + 4 + 4 + 4 + 4;

    pub fn validate(&self) -> Result<()> {
        if self.version != JETTY_DESCRIPTOR_VERSION {
            return Err(Error::Protocol(format!(
                "unsupported Jetty descriptor version {}",
                self.version
            )));
        }
        let declared = usize::try_from(self.opaque_len)
            .map_err(|_| Error::Protocol("descriptor length does not fit usize".into()))?;
        if declared == 0 || declared != self.opaque_data.len() {
            return Err(Error::Protocol(
                "descriptor opaque_len does not match opaque_data".into(),
            ));
        }
        if declared > MAX_JETTY_DESCRIPTOR_LEN {
            return Err(Error::Protocol(format!(
                "descriptor length {declared} exceeds {MAX_JETTY_DESCRIPTOR_LEN}"
            )));
        }
        Ok(())
    }

    pub fn serialize(&self) -> Result<Vec<u8>> {
        self.validate()?;
        let mut out = Vec::with_capacity(Self::FIXED_LEN + self.opaque_data.len());
        out.extend_from_slice(&self.version.to_be_bytes());
        out.extend_from_slice(&self.transport_type.to_be_bytes());
        out.extend_from_slice(&self.eid_index.to_be_bytes());
        out.extend_from_slice(&self.jetty_id.to_be_bytes());
        out.extend_from_slice(&self.opaque_len.to_be_bytes());
        out.extend_from_slice(&self.opaque_data);
        Ok(out)
    }

    pub fn deserialize(input: &[u8]) -> Result<Self> {
        if input.len() < Self::FIXED_LEN {
            return Err(Error::Protocol("truncated Jetty descriptor".into()));
        }
        let version = u16::from_be_bytes([input[0], input[1]]);
        let transport_type = u32::from_be_bytes(input[2..6].try_into().expect("fixed slice"));
        let eid_index = u32::from_be_bytes(input[6..10].try_into().expect("fixed slice"));
        let jetty_id = u32::from_be_bytes(input[10..14].try_into().expect("fixed slice"));
        let opaque_len = u32::from_be_bytes(input[14..18].try_into().expect("fixed slice"));
        let descriptor = Self {
            version,
            transport_type,
            eid_index,
            jetty_id,
            opaque_len,
            opaque_data: input[Self::FIXED_LEN..].to_vec(),
        };
        descriptor.validate()?;
        Ok(descriptor)
    }

    #[cfg(feature = "urma")]
    pub(crate) fn from_ffi(raw: crate::ffi::JettyDescriptorData) -> Result<Self> {
        let opaque_len = u32::try_from(raw.opaque_data.len())
            .map_err(|_| Error::Protocol("native descriptor exceeds u32".into()))?;
        let descriptor = Self {
            version: JETTY_DESCRIPTOR_VERSION,
            transport_type: raw.transport_type,
            eid_index: raw.eid_index,
            jetty_id: raw.jetty_id,
            opaque_len,
            opaque_data: raw.opaque_data,
        };
        descriptor.validate()?;
        Ok(descriptor)
    }

    #[cfg(feature = "urma")]
    fn to_ffi(&self) -> Result<crate::ffi::JettyDescriptorData> {
        self.validate()?;
        Ok(crate::ffi::JettyDescriptorData {
            transport_type: self.transport_type,
            eid_index: self.eid_index,
            jetty_id: self.jetty_id,
            opaque_data: self.opaque_data.clone(),
        })
    }
}

/// Prototype RC Jetty sizing and statically provisioned import token.
pub struct JettyConfig {
    pub send_depth: u32,
    pub recv_depth: u32,
    pub max_send_sge: u32,
    pub max_recv_sge: u32,
    token: u32,
}

impl Default for JettyConfig {
    fn default() -> Self {
        Self {
            send_depth: 128,
            recv_depth: 512,
            max_send_sge: 1,
            max_recv_sge: 1,
            token: 0,
        }
    }
}

impl JettyConfig {
    pub fn with_token(mut self, token: u32) -> Self {
        self.token = token;
        self
    }

    #[cfg(feature = "urma")]
    pub(crate) fn token(&self) -> u32 {
        self.token
    }
}

#[cfg(feature = "urma")]
mod native {
    use super::*;
    use crate::ffi;

    /// Safe owner of a local Jetty and an optional imported/bound remote Jetty.
    pub(crate) struct UrmaJetty {
        handle: ffi::JettyHandle,
        token: u32,
        imported: bool,
        bound: bool,
    }

    impl UrmaJetty {
        pub(crate) fn create(
            runtime: &mut ffi::NativeRuntime,
            send_jfc: &ffi::JfcHandle,
            recv_jfc: &ffi::JfcHandle,
            config: &JettyConfig,
        ) -> Result<Self> {
            let ffi_config = ffi::JettyConfig {
                send_depth: config.send_depth,
                recv_depth: config.recv_depth,
                max_send_sge: config.max_send_sge,
                max_recv_sge: config.max_recv_sge,
                token: config.token(),
            };
            let handle = ffi::JettyHandle::create(runtime, send_jfc, recv_jfc, &ffi_config)
                .map_err(|error| map_ffi_error("create_jetty", error))?;
            Ok(Self {
                handle,
                token: config.token(),
                imported: false,
                bound: false,
            })
        }

        pub(crate) fn export_descriptor(&self) -> Result<JettyDescriptor> {
            let raw = self
                .handle
                .export_descriptor()
                .map_err(|error| map_ffi_error("get_rjetty", error))?;
            JettyDescriptor::from_ffi(raw)
        }

        pub(crate) fn import(&mut self, descriptor: &JettyDescriptor) -> Result<()> {
            if self.imported {
                return Err(Error::Protocol("a remote Jetty is already imported".into()));
            }
            let raw = descriptor.to_ffi()?;
            self.handle
                .import(&raw, self.token)
                .map_err(|error| map_ffi_error("import_jetty", error))?;
            self.imported = true;
            Ok(())
        }

        pub(crate) fn bind(&mut self) -> Result<()> {
            if !self.imported {
                return Err(Error::Protocol("bind requires an imported Jetty".into()));
            }
            self.handle
                .bind()
                .map_err(|error| map_ffi_error("bind_jetty", error))?;
            self.bound = true;
            Ok(())
        }

        #[allow(dead_code)]
        pub(crate) fn mark_error(&mut self) -> Result<()> {
            self.handle
                .mark_error()
                .map_err(|error| map_ffi_error("modify_jetty_error", error))
        }

        pub(crate) fn post_send(
            &mut self,
            segment: &ffi::SegmentHandle,
            offset: u64,
            length: u32,
            user_ctx: u64,
            complete_enable: bool,
        ) -> Result<ffi::WrHandle> {
            self.handle
                .post_send(segment, offset, length, user_ctx, complete_enable)
                .map_err(|error| map_ffi_error("post_jetty_send_wr", error))
        }

        pub(crate) fn post_recv(
            &mut self,
            segment: &ffi::SegmentHandle,
            offset: u64,
            length: u32,
            user_ctx: u64,
        ) -> Result<ffi::WrHandle> {
            self.handle
                .post_recv(segment, offset, length, user_ctx)
                .map_err(|error| map_ffi_error("post_jetty_recv_wr", error))
        }

        pub(crate) fn post_send_batch(
            &mut self,
            segment: &ffi::SegmentHandle,
            descriptors: &[ffi::WrDescriptor],
        ) -> Result<ffi::PostBatch> {
            self.handle
                .post_send_batch(segment, descriptors)
                .map_err(|error| map_ffi_error("post_jetty_send_wr_batch", error))
        }

        pub(crate) fn post_recv_batch(
            &mut self,
            segment: &ffi::SegmentHandle,
            descriptors: &[ffi::WrDescriptor],
        ) -> Result<ffi::PostBatch> {
            self.handle
                .post_recv_batch(segment, descriptors)
                .map_err(|error| map_ffi_error("post_jetty_recv_wr_batch", error))
        }

        pub(crate) fn close(&mut self) -> Result<()> {
            let mut failures = Vec::new();
            if self.bound {
                match self.handle.unbind() {
                    Ok(()) => self.bound = false,
                    Err(error) => failures.push(map_ffi_error("unbind_jetty", error).to_string()),
                }
            }
            if self.imported && !self.bound {
                match self.handle.unimport() {
                    Ok(()) => self.imported = false,
                    Err(error) => failures.push(map_ffi_error("unimport_jetty", error).to_string()),
                }
            }
            if !self.bound && !self.imported {
                if let Err(error) = self.handle.close() {
                    failures.push(map_ffi_error("delete_jetty", error).to_string());
                }
            }
            if failures.is_empty() {
                Ok(())
            } else {
                Err(Error::Shutdown { failures })
            }
        }
    }

    impl Drop for UrmaJetty {
        fn drop(&mut self) {
            let _ = self.close();
        }
    }

    fn map_ffi_error(operation: &'static str, error: ffi::FfiError) -> Error {
        match error {
            ffi::FfiError::Contract(detail) => Error::FfiContract { operation, detail },
            ffi::FfiError::NullHandle => Error::NullHandle { operation },
            ffi::FfiError::Status(status) => Error::Native { operation, status },
        }
    }
}

#[cfg(feature = "urma")]
pub(crate) use native::UrmaJetty;

#[cfg(test)]
mod tests {
    use super::*;

    fn descriptor() -> JettyDescriptor {
        JettyDescriptor {
            version: JETTY_DESCRIPTOR_VERSION,
            transport_type: 0,
            eid_index: 3,
            jetty_id: 42,
            opaque_len: 4,
            opaque_data: vec![1, 2, 3, 4],
        }
    }

    #[test]
    fn descriptor_round_trip() {
        let descriptor = descriptor();
        let bytes = descriptor.serialize().expect("serialize");
        assert_eq!(JettyDescriptor::deserialize(&bytes), Ok(descriptor));
    }

    #[test]
    fn descriptor_rejects_invalid_version() {
        let mut descriptor = descriptor();
        descriptor.version += 1;
        assert!(descriptor.serialize().is_err());
    }

    #[test]
    fn descriptor_rejects_oversized_payload() {
        let descriptor = JettyDescriptor {
            opaque_len: (MAX_JETTY_DESCRIPTOR_LEN + 1) as u32,
            opaque_data: vec![0; MAX_JETTY_DESCRIPTOR_LEN + 1],
            ..descriptor()
        };
        assert!(descriptor.validate().is_err());
    }
}
