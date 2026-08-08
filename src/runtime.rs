use crate::{Error, Result};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeConfig {
    pub device_name: String,
    pub eid_index: u32,
}

impl RuntimeConfig {
    pub fn new(device_name: impl Into<String>, eid_index: u32) -> Self {
        Self {
            device_name: device_name.into(),
            eid_index,
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

/// Returns the ABI fingerprint compiled into the C shim.
pub fn abi_baseline() -> Result<AbiBaseline> {
    native::abi_baseline()
}

#[cfg(feature = "urma")]
mod native {
    use super::*;
    use crate::ffi;
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

    /// Unique process-level owner of liburma and its device context.
    ///
    /// `Rc` in the marker deliberately prevents moving this native owner to a
    /// different thread. Later Phase 0 work will construct it inside the poller.
    pub struct UrmaRuntime {
        config: RuntimeConfig,
        native: Option<ffi::NativeRuntime>,
        _not_send_sync: PhantomData<Rc<()>>,
    }

    impl UrmaRuntime {
        pub fn open(config: RuntimeConfig) -> Result<Self> {
            if ACTIVE
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                return Err(Error::AlreadyInitialized);
            }

            let device = CString::new(config.device_name.as_str()).map_err(|_| {
                ACTIVE.store(false, Ordering::Release);
                Error::InvalidDeviceName
            })?;
            let native = ffi::NativeRuntime::open(&device, config.eid_index).map_err(|error| {
                ACTIVE.store(false, Ordering::Release);
                map_ffi_error("runtime_open", error)
            })?;

            Ok(Self {
                config,
                native: Some(native),
                _not_send_sync: PhantomData,
            })
        }

        pub fn config(&self) -> &RuntimeConfig {
            &self.config
        }

        pub fn close(mut self) -> Result<()> {
            self.close_inner()
        }

        fn close_inner(&mut self) -> Result<()> {
            let Some(mut native) = self.native.take() else {
                return Ok(());
            };
            match native.close() {
                Ok(()) => {
                    ACTIVE.store(false, Ordering::Release);
                    Ok(())
                }
                Err(error) => {
                    // A failed close leaves liburma's process state uncertain. Keep
                    // the guard active so this process cannot initialize it again.
                    Err(map_ffi_error("runtime_close", error))
                }
            }
        }
    }

    impl Drop for UrmaRuntime {
        fn drop(&mut self) {
            let _ = self.close_inner();
        }
    }

    fn map_ffi_error(operation: &'static str, error: ffi::FfiError) -> Error {
        match error {
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
        pub fn open(_config: RuntimeConfig) -> Result<Self> {
            Err(Error::FeatureDisabled)
        }

        pub fn close(self) -> Result<()> {
            Ok(())
        }
    }
}

pub use native::UrmaRuntime;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_config_keeps_device_selection() {
        let config = RuntimeConfig::new("urma0", 2);
        assert_eq!(config.device_name, "urma0");
        assert_eq!(config.eid_index, 2);
    }

    #[cfg(not(feature = "urma"))]
    #[test]
    fn feature_off_reports_clear_error() {
        let error = UrmaRuntime::open(RuntimeConfig::new("urma0", 0)).err();
        assert_eq!(error, Some(Error::FeatureDisabled));
        assert_eq!(abi_baseline(), Err(Error::FeatureDisabled));
    }

    #[cfg(feature = "urma")]
    #[test]
    fn abi_baseline_matches_verified_m0_contract() {
        let baseline = abi_baseline().expect("C shim must return its ABI baseline");
        assert_eq!(baseline.shim_abi_version, 1);
        assert_eq!(baseline.pointer_size as usize, std::mem::size_of::<usize>());
        assert_eq!(baseline.status_size as usize, std::mem::size_of::<i32>());
        assert_eq!(baseline.success_value, 0);
        assert!(baseline.init_attr_size > 0);
        assert!(baseline.eid_size > 0);
        assert!(baseline.device_size > 0);
        assert!(baseline.context_size > 0);
    }
}
