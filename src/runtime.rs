use crate::{BufferPoolConfig, Error, Result};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeConfig {
    pub device_name: String,
    pub eid_index: u32,
    pub send_jfc_depth: u32,
    pub recv_jfc_depth: u32,
    pub buffer_pool: BufferPoolConfig,
}

impl RuntimeConfig {
    pub fn new(device_name: impl Into<String>, eid_index: u32) -> Self {
        Self {
            device_name: device_name.into(),
            eid_index,
            send_jfc_depth: 4096,
            recv_jfc_depth: 4096,
            buffer_pool: BufferPoolConfig::default(),
        }
    }
}

/// Header/layout fingerprint reported by the C compiler that built the shim.
///
/// This is diagnostic data, not a promise that private UMDK layouts are stable.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AbiBaseline {
    pub shim_abi_version: u32,
    pub pointer_size: u32,
    pub status_size: u32,
    pub init_attr_size: u32,
    pub eid_size: u32,
    pub device_size: u32,
    pub context_size: u32,
    pub success_value: i32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeviceEid {
    pub index: u32,
    pub bytes: Vec<u8>,
}

/// Rust-owned snapshot copied from `urma_device_attr_t` and the EID list.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UrmaDeviceCapability {
    pub device_name: String,
    pub transport_type: i32,
    pub selected_eid_index: u32,
    pub eids: Vec<DeviceEid>,
    pub max_jfc: u32,
    pub max_jfs: u32,
    pub max_jfr: u32,
    pub max_jetty: u32,
    pub max_jfc_depth: u32,
    pub max_jfs_depth: u32,
    pub max_jfr_depth: u32,
    pub max_jfs_inline_len: u32,
    pub max_jfs_sge: u32,
    pub max_jfs_rsge: u32,
    pub max_jfr_sge: u32,
    pub max_msg_size: u64,
    pub transport_modes: u16,
    pub page_size_cap: u64,
}

/// Returns the ABI fingerprint compiled into the C shim.
pub fn abi_baseline() -> Result<AbiBaseline> {
    native::abi_baseline()
}

#[cfg(feature = "urma")]
mod native {
    use super::*;
    use crate::{
        buffer::UrmaBufferPool,
        connection::UrmaConnection,
        ffi::{self, NativeRuntime},
        jetty::UrmaJetty,
        jfc::{JfcKind, UrmaJfc},
        JettyConfig, SlotId, SlotKind, SlotState,
    };
    use std::{
        ffi::CString,
        marker::PhantomData,
        rc::Rc,
        sync::atomic::{AtomicBool, Ordering},
    };

    static ACTIVE: AtomicBool = AtomicBool::new(false);

    pub(super) fn abi_baseline() -> Result<AbiBaseline> {
        let raw = ffi::abi_baseline().map_err(|error| map_ffi_error("get_abi_baseline", error))?;
        Ok(AbiBaseline {
            shim_abi_version: raw.shim_abi_version,
            pointer_size: raw.pointer_size,
            status_size: raw.status_size,
            init_attr_size: raw.init_attr_size,
            eid_size: raw.eid_size,
            device_size: raw.device_size,
            context_size: raw.context_size,
            success_value: raw.success_value,
        })
    }

    /// Process-level owner of the complete M1 native resource tree.
    pub struct UrmaRuntime {
        config: RuntimeConfig,
        capability: UrmaDeviceCapability,
        buffer_pool: Option<UrmaBufferPool>,
        recv_jfc: Option<UrmaJfc>,
        send_jfc: Option<UrmaJfc>,
        jfce: Option<ffi::JfceHandle>,
        native: Option<ffi::NativeRuntime>,
        accepting: bool,
        poisoned: bool,
        next_connection_id: u16,
        _not_send_sync: PhantomData<Rc<()>>,
    }

    impl UrmaRuntime {
        pub fn start(config: RuntimeConfig) -> Result<Self> {
            if ACTIVE
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                return Err(Error::AlreadyInitialized);
            }

            match Self::start_inner(config) {
                Ok(runtime) => Ok(runtime),
                Err(error) => {
                    if !matches!(&error, Error::StartupRollback { .. }) {
                        ACTIVE.store(false, Ordering::Release);
                    }
                    Err(error)
                }
            }
        }

        /// Compatibility alias retained from M0.
        pub fn open(config: RuntimeConfig) -> Result<Self> {
            Self::start(config)
        }

        fn start_inner(config: RuntimeConfig) -> Result<Self> {
            let device =
                CString::new(config.device_name.as_str()).map_err(|_| Error::InvalidDeviceName)?;
            let mut native: NativeRuntime = ffi::NativeRuntime::open(&device, config.eid_index)
                .map_err(|error| map_ffi_error("runtime_open", error))?;

            let capability = match native.query_device() {
                Ok(capability) => from_ffi_capability(capability),
                Err(error) => {
                    let primary = map_ffi_error("query_device", error);
                    return Err(rollback_startup(
                        primary,
                        None,
                        None,
                        None,
                        None,
                        Some(native),
                    ));
                }
            };
            if let Err(primary) = validate_config(&config, &capability) {
                return Err(rollback_startup(
                    primary,
                    None,
                    None,
                    None,
                    None,
                    Some(native),
                ));
            }

            let jfce = match ffi::JfceHandle::create(&mut native) {
                Ok(jfce) => jfce,
                Err(error) => {
                    let primary = map_ffi_error("create_jfce", error);
                    return Err(rollback_startup(
                        primary,
                        None,
                        None,
                        None,
                        None,
                        Some(native),
                    ));
                }
            };
            let send_jfc =
                match UrmaJfc::create(&mut native, &jfce, JfcKind::Send, config.send_jfc_depth) {
                    Ok(jfc) => jfc,
                    Err(primary) => {
                        return Err(rollback_startup(
                            primary,
                            None,
                            None,
                            None,
                            Some(jfce),
                            Some(native),
                        ));
                    }
                };
            let recv_jfc = match UrmaJfc::create(
                &mut native,
                &jfce,
                JfcKind::Receive,
                config.recv_jfc_depth,
            ) {
                Ok(jfc) => jfc,
                Err(primary) => {
                    return Err(rollback_startup(
                        primary,
                        None,
                        None,
                        Some(send_jfc),
                        Some(jfce),
                        Some(native),
                    ));
                }
            };
            let buffer_pool = match UrmaBufferPool::create(&mut native, config.buffer_pool.clone())
            {
                Ok(pool) => pool,
                Err(primary) => {
                    return Err(rollback_startup(
                        primary,
                        None,
                        Some(recv_jfc),
                        Some(send_jfc),
                        Some(jfce),
                        Some(native),
                    ));
                }
            };

            debug_assert_eq!(send_jfc.kind(), JfcKind::Send);
            debug_assert_eq!(recv_jfc.kind(), JfcKind::Receive);
            Ok(Self {
                config,
                capability,
                buffer_pool: Some(buffer_pool),
                recv_jfc: Some(recv_jfc),
                send_jfc: Some(send_jfc),
                jfce: Some(jfce),
                native: Some(native),
                accepting: true,
                poisoned: false,
                next_connection_id: 1,
                _not_send_sync: PhantomData,
            })
        }

        pub fn config(&self) -> &RuntimeConfig {
            &self.config
        }

        pub fn capability(&self) -> &UrmaDeviceCapability {
            &self.capability
        }

        pub fn jfc_depths(&self) -> (u32, u32) {
            (
                self.send_jfc.as_ref().map_or(0, UrmaJfc::depth),
                self.recv_jfc.as_ref().map_or(0, UrmaJfc::depth),
            )
        }

        pub fn registered_memory_layout(&self) -> Option<(usize, usize)> {
            self.buffer_pool
                .as_ref()
                .map(|pool| (pool.registered_len(), pool.alignment()))
        }

        pub fn allocate_slot(&mut self, kind: SlotKind) -> Option<SlotId> {
            if !self.accepting {
                return None;
            }
            self.buffer_pool.as_mut()?.allocate(kind)
        }

        pub fn release_slot(&mut self, id: SlotId) -> Result<()> {
            self.buffer_pool
                .as_mut()
                .ok_or_else(|| Error::InvalidConfiguration("buffer pool is closed".into()))?
                .release(id)
        }

        pub fn slot_state(&self, id: SlotId) -> Option<SlotState> {
            self.buffer_pool.as_ref()?.slot_state(id)
        }

        pub fn create_connection(&mut self, config: JettyConfig) -> Result<UrmaConnection<'_>> {
            if !self.accepting {
                return Err(Error::InvalidConfiguration(
                    "runtime is no longer accepting operations".into(),
                ));
            }
            validate_jetty_config(&config, &self.capability)?;
            let capability = self.capability.clone();
            let connection_id = self.next_connection_id;
            self.next_connection_id = self
                .next_connection_id
                .checked_add(1)
                .filter(|id| *id != 0)
                .ok_or_else(|| {
                    Error::InvalidConfiguration("connection id space exhausted".into())
                })?;
            let native = self
                .native
                .as_mut()
                .ok_or_else(|| Error::InvalidConfiguration("runtime is closed".into()))?;
            let send_jfc = self
                .send_jfc
                .as_ref()
                .ok_or_else(|| Error::InvalidConfiguration("send JFC is closed".into()))?;
            let recv_jfc = self
                .recv_jfc
                .as_ref()
                .ok_or_else(|| Error::InvalidConfiguration("receive JFC is closed".into()))?;
            let jfce = self
                .jfce
                .as_ref()
                .ok_or_else(|| Error::InvalidConfiguration("JFCE is closed".into()))?;
            let buffer_pool = self
                .buffer_pool
                .as_mut()
                .ok_or_else(|| Error::InvalidConfiguration("buffer pool is closed".into()))?;
            let jetty = UrmaJetty::create(native, send_jfc.handle(), recv_jfc.handle(), &config)?;
            UrmaConnection::new(
                capability,
                jetty,
                buffer_pool,
                send_jfc.handle(),
                recv_jfc.handle(),
                jfce,
                connection_id,
                1,
            )
        }

        pub fn shutdown(mut self) -> Result<()> {
            self.shutdown_inner()
        }

        /// Compatibility alias retained from M0.
        pub fn close(self) -> Result<()> {
            self.shutdown()
        }

        fn shutdown_inner(&mut self) -> Result<()> {
            if self.poisoned {
                return Err(Error::Shutdown {
                    failures: vec!["a previous shutdown attempt left native state uncertain".into()],
                });
            }
            self.accepting = false;
            let mut failures = Vec::new();

            if let Some(mut recv_jfc) = self.recv_jfc.take() {
                if let Err(error) = recv_jfc.close() {
                    failures.push(error.to_string());
                }
            }
            if let Some(mut send_jfc) = self.send_jfc.take() {
                if let Err(error) = send_jfc.close() {
                    failures.push(error.to_string());
                }
            }
            if let Some(mut jfce) = self.jfce.take() {
                if let Err(error) = jfce.close() {
                    failures.push(map_ffi_error("delete_jfce", error).to_string());
                }
            }
            if let Some(mut pool) = self.buffer_pool.take() {
                pool.stop();
                if let Err(error) = pool.close() {
                    failures.push(error.to_string());
                }
            }
            if let Some(mut native) = self.native.take() {
                if let Err(error) = native.close() {
                    failures.push(map_ffi_error("runtime_close", error).to_string());
                }
            }

            if failures.is_empty() {
                ACTIVE.store(false, Ordering::Release);
                Ok(())
            } else {
                // Native state is uncertain; keep the process guard active.
                self.poisoned = true;
                Err(Error::Shutdown { failures })
            }
        }
    }

    impl Drop for UrmaRuntime {
        fn drop(&mut self) {
            if !self.poisoned {
                let _ = self.shutdown_inner();
            }
        }
    }

    fn validate_config(config: &RuntimeConfig, capability: &UrmaDeviceCapability) -> Result<()> {
        config.buffer_pool.total_len()?;
        if capability.max_jfc < 2 {
            return Err(Error::InvalidConfiguration(
                "device reports fewer than two available JFC resources".into(),
            ));
        }
        for (name, depth) in [
            ("send_jfc_depth", config.send_jfc_depth),
            ("recv_jfc_depth", config.recv_jfc_depth),
        ] {
            if depth == 0 || depth > capability.max_jfc_depth {
                return Err(Error::InvalidConfiguration(format!(
                    "{name}={depth} is outside 1..={}",
                    capability.max_jfc_depth
                )));
            }
        }
        let slot_size = u64::try_from(config.buffer_pool.slot_size)
            .map_err(|_| Error::InvalidConfiguration("slot_size does not fit u64".into()))?;
        if slot_size > capability.max_msg_size {
            return Err(Error::InvalidConfiguration(format!(
                "slot_size={slot_size} exceeds max_msg_size={}",
                capability.max_msg_size
            )));
        }
        // TODO(M1-verify): decode page_size_cap for the target provider before
        // enforcing that the configured alignment is advertised.
        Ok(())
    }

    fn validate_jetty_config(
        config: &JettyConfig,
        capability: &UrmaDeviceCapability,
    ) -> Result<()> {
        if capability.max_jetty == 0 || capability.max_jfs == 0 || capability.max_jfr == 0 {
            return Err(Error::InvalidConfiguration(
                "device does not advertise the resources required by a duplex Jetty".into(),
            ));
        }
        for (name, value, maximum) in [
            ("send_depth", config.send_depth, capability.max_jfs_depth),
            ("recv_depth", config.recv_depth, capability.max_jfr_depth),
            ("max_send_sge", config.max_send_sge, capability.max_jfs_sge),
            ("max_recv_sge", config.max_recv_sge, capability.max_jfr_sge),
        ] {
            if value == 0 || value > maximum {
                return Err(Error::InvalidConfiguration(format!(
                    "Jetty {name}={value} is outside 1..={maximum}"
                )));
            }
        }
        if capability.max_jfs_rsge == 0 {
            return Err(Error::InvalidConfiguration(
                "device does not advertise an RC remote-SGE capability".into(),
            ));
        }
        // TODO(M2-verify): confirm the transport_modes RC bit interpretation
        // against the target provider before enforcing it here.
        Ok(())
    }

    fn rollback_startup(
        primary: Error,
        mut buffer_pool: Option<UrmaBufferPool>,
        mut recv_jfc: Option<UrmaJfc>,
        mut send_jfc: Option<UrmaJfc>,
        mut jfce: Option<ffi::JfceHandle>,
        mut native: Option<ffi::NativeRuntime>,
    ) -> Error {
        let mut cleanup_failures = Vec::new();
        if let Some(pool) = buffer_pool.as_mut() {
            if let Err(error) = pool.close() {
                cleanup_failures.push(error.to_string());
            }
        }
        if let Some(jfc) = recv_jfc.as_mut() {
            if let Err(error) = jfc.close() {
                cleanup_failures.push(error.to_string());
            }
        }
        if let Some(jfc) = send_jfc.as_mut() {
            if let Err(error) = jfc.close() {
                cleanup_failures.push(error.to_string());
            }
        }
        if let Some(jfce) = jfce.as_mut() {
            if let Err(error) = jfce.close() {
                cleanup_failures.push(map_ffi_error("delete_jfce", error).to_string());
            }
        }
        if let Some(runtime) = native.as_mut() {
            if let Err(error) = runtime.close() {
                cleanup_failures.push(map_ffi_error("runtime_close", error).to_string());
            }
        }
        if cleanup_failures.is_empty() {
            primary
        } else {
            Error::StartupRollback {
                primary: Box::new(primary),
                cleanup_failures,
            }
        }
    }

    fn from_ffi_capability(raw: ffi::DeviceCapability) -> UrmaDeviceCapability {
        UrmaDeviceCapability {
            device_name: raw.device_name,
            transport_type: raw.transport_type,
            selected_eid_index: raw.selected_eid_index,
            eids: raw
                .eids
                .into_iter()
                .map(|eid| DeviceEid {
                    index: eid.index,
                    bytes: eid.bytes,
                })
                .collect(),
            max_jfc: raw.max_jfc,
            max_jfs: raw.max_jfs,
            max_jfr: raw.max_jfr,
            max_jetty: raw.max_jetty,
            max_jfc_depth: raw.max_jfc_depth,
            max_jfs_depth: raw.max_jfs_depth,
            max_jfr_depth: raw.max_jfr_depth,
            max_jfs_inline_len: raw.max_jfs_inline_len,
            max_jfs_sge: raw.max_jfs_sge,
            max_jfs_rsge: raw.max_jfs_rsge,
            max_jfr_sge: raw.max_jfr_sge,
            max_msg_size: raw.max_msg_size,
            transport_modes: raw.transport_modes,
            page_size_cap: raw.page_size_cap,
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

#[cfg(not(feature = "urma"))]
mod native {
    use super::*;

    /// Hardware-independent placeholder used by ordinary development builds.
    pub struct UrmaRuntime;

    pub(super) fn abi_baseline() -> Result<AbiBaseline> {
        Err(Error::FeatureDisabled)
    }

    impl UrmaRuntime {
        pub fn start(_config: RuntimeConfig) -> Result<Self> {
            Err(Error::FeatureDisabled)
        }

        pub fn open(config: RuntimeConfig) -> Result<Self> {
            Self::start(config)
        }

        pub fn shutdown(self) -> Result<()> {
            Ok(())
        }

        pub fn close(self) -> Result<()> {
            self.shutdown()
        }
    }
}

pub use native::UrmaRuntime;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_config_keeps_device_selection_and_m1_defaults() {
        let config = RuntimeConfig::new("urma0", 2);
        assert_eq!(config.device_name, "urma0");
        assert_eq!(config.eid_index, 2);
        assert_eq!(config.send_jfc_depth, 4096);
        assert_eq!(config.recv_jfc_depth, 4096);
        assert_eq!(config.buffer_pool, BufferPoolConfig::default());
    }

    #[cfg(not(feature = "urma"))]
    #[test]
    fn feature_off_reports_clear_error_without_umdk() {
        let error = UrmaRuntime::start(RuntimeConfig::new("urma0", 0)).err();
        assert_eq!(error, Some(Error::FeatureDisabled));
        assert_eq!(abi_baseline(), Err(Error::FeatureDisabled));
    }

    #[cfg(feature = "urma")]
    #[test]
    fn abi_baseline_matches_verified_m0_contract() {
        let baseline = abi_baseline().expect("C shim must return its ABI baseline");
        assert_eq!(baseline.shim_abi_version, 8);
        assert_eq!(baseline.pointer_size as usize, std::mem::size_of::<usize>());
        assert_eq!(baseline.status_size as usize, std::mem::size_of::<i32>());
        assert_eq!(baseline.success_value, 0);
        assert!(baseline.init_attr_size > 0);
        assert!(baseline.eid_size > 0);
        assert!(baseline.device_size > 0);
        assert!(baseline.context_size > 0);
    }
}
