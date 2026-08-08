//! Crate-private native boundary. Raw generated types must not escape this module.

use std::{
    ffi::{c_int, CStr},
    marker::PhantomData,
    ptr::NonNull,
    rc::Rc,
};

mod sys {
    #![allow(non_camel_case_types, non_snake_case, non_upper_case_globals)]
    #![allow(clippy::all)]

    include!(concat!(env!("OUT_DIR"), "/urma_bindings.rs"));
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct AbiBaseline {
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
pub(crate) struct DeviceEid {
    pub index: u32,
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DeviceCapability {
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FfiError {
    Contract(&'static str),
    NullHandle,
    Status(c_int),
}

pub(crate) fn abi_baseline() -> Result<AbiBaseline, FfiError> {
    let mut raw = std::mem::MaybeUninit::<sys::urma_lab_abi_baseline_t>::uninit();
    // SAFETY: `raw` points to writable storage of the exact generated shim DTO.
    let status = unsafe { sys::urma_lab_get_abi_baseline(raw.as_mut_ptr()) };
    if status != 0 {
        return Err(FfiError::Status(status));
    }
    // SAFETY: The shim contract initializes every field when it returns zero.
    let raw = unsafe { raw.assume_init() };
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

/// Unique Rust owner of the opaque C shim runtime.
///
/// All raw pointers and unsafe calls terminate in this type. The `Rc` marker
/// prevents the handle from crossing threads during Phase 0.
pub(crate) struct NativeRuntime {
    raw: Option<NonNull<sys::urma_lab_runtime_t>>,
    _not_send_sync: PhantomData<Rc<()>>,
}

impl NativeRuntime {
    pub(crate) fn open(device_name: &CStr, eid_index: u32) -> Result<Self, FfiError> {
        let mut raw = std::ptr::null_mut();
        // SAFETY: `device_name` is NUL terminated and valid for the call; `raw`
        // is writable and the returned pointer is checked before ownership.
        let status = unsafe {
            sys::urma_lab_runtime_open(device_name.as_ptr(), eid_index, &mut raw)
        };
        if status != 0 {
            return Err(FfiError::Status(status));
        }
        let raw = NonNull::new(raw).ok_or(FfiError::NullHandle)?;
        Ok(Self {
            raw: Some(raw),
            _not_send_sync: PhantomData,
        })
    }

    pub(crate) fn close(&mut self) -> Result<(), FfiError> {
        let Some(raw) = self.raw.take() else {
            return Ok(());
        };
        // SAFETY: `raw` is the unique live allocation returned by the shim and
        // is consumed exactly once, regardless of the reported close status.
        let status = unsafe { sys::urma_lab_runtime_close(raw.as_ptr()) };
        if status == 0 {
            Ok(())
        } else {
            Err(FfiError::Status(status))
        }
    }

    pub(crate) fn query_device(&self) -> Result<DeviceCapability, FfiError> {
        let raw_runtime = self.raw.ok_or(FfiError::Contract("runtime is closed"))?;
        let mut raw = std::mem::MaybeUninit::<sys::urma_lab_device_capability_t>::uninit();
        // SAFETY: Both pointers are valid for the duration of the call and the
        // shim initializes the complete pointer-free DTO on success.
        let status = unsafe {
            sys::urma_lab_runtime_query_device(raw_runtime.as_ptr(), raw.as_mut_ptr())
        };
        if status != 0 {
            return Err(FfiError::Status(status));
        }
        // SAFETY: A zero shim status guarantees full DTO initialization.
        let raw = unsafe { raw.assume_init() };
        let eid_count = usize::try_from(raw.eid_count)
            .map_err(|_| FfiError::Contract("EID count does not fit usize"))?;
        if eid_count > raw.eids.len() {
            return Err(FfiError::Contract("EID count exceeds shim DTO capacity"));
        }

        let mut eids = Vec::with_capacity(eid_count);
        for eid in &raw.eids[..eid_count] {
            let length = usize::try_from(eid.length)
                .map_err(|_| FfiError::Contract("EID length does not fit usize"))?;
            if length > eid.bytes.len() {
                return Err(FfiError::Contract("EID length exceeds shim storage"));
            }
            eids.push(DeviceEid {
                index: eid.index,
                bytes: eid.bytes[..length].to_vec(),
            });
        }

        // SAFETY: shim.c always writes a trailing NUL into device_name.
        let device_name = unsafe { CStr::from_ptr(raw.device_name.as_ptr()) }
            .to_string_lossy()
            .into_owned();
        Ok(DeviceCapability {
            device_name,
            transport_type: raw.transport_type,
            selected_eid_index: raw.selected_eid_index,
            eids,
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
        })
    }
}

impl Drop for NativeRuntime {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

pub(crate) struct JfcHandle {
    raw: Option<NonNull<sys::urma_lab_jfc_t>>,
    _not_send_sync: PhantomData<Rc<()>>,
}

impl JfcHandle {
    pub(crate) fn create(runtime: &mut NativeRuntime, depth: u32) -> Result<Self, FfiError> {
        let raw_runtime = runtime
            .raw
            .ok_or(FfiError::Contract("runtime is closed"))?;
        let mut raw = std::ptr::null_mut();
        // SAFETY: The runtime pointer is live and `raw` is a valid out pointer.
        let status = unsafe { sys::urma_lab_jfc_create(raw_runtime.as_ptr(), depth, &mut raw) };
        if status != 0 {
            return Err(FfiError::Status(status));
        }
        let raw = NonNull::new(raw).ok_or(FfiError::NullHandle)?;
        Ok(Self {
            raw: Some(raw),
            _not_send_sync: PhantomData,
        })
    }

    pub(crate) fn close(&mut self) -> Result<(), FfiError> {
        let Some(raw) = self.raw.take() else {
            return Ok(());
        };
        // SAFETY: This is the unique live shim JFC wrapper.
        let status = unsafe { sys::urma_lab_jfc_delete(raw.as_ptr()) };
        if status == 0 {
            Ok(())
        } else {
            Err(FfiError::Status(status))
        }
    }
}

impl Drop for JfcHandle {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

pub(crate) struct SegmentHandle {
    raw: Option<NonNull<sys::urma_lab_segment_t>>,
    _not_send_sync: PhantomData<Rc<()>>,
}

impl SegmentHandle {
    pub(crate) fn create(
        runtime: &mut NativeRuntime,
        length: u64,
        alignment: u64,
    ) -> Result<Self, FfiError> {
        let raw_runtime = runtime
            .raw
            .ok_or(FfiError::Contract("runtime is closed"))?;
        let mut raw = std::ptr::null_mut();
        // SAFETY: The runtime pointer is live and `raw` is a valid out pointer.
        let status = unsafe {
            sys::urma_lab_segment_create(raw_runtime.as_ptr(), length, alignment, &mut raw)
        };
        if status != 0 {
            return Err(FfiError::Status(status));
        }
        let raw = NonNull::new(raw).ok_or(FfiError::NullHandle)?;
        Ok(Self {
            raw: Some(raw),
            _not_send_sync: PhantomData,
        })
    }

    pub(crate) fn close(&mut self) -> Result<(), FfiError> {
        let Some(raw) = self.raw.take() else {
            return Ok(());
        };
        // SAFETY: This is the unique live shim Segment wrapper.
        let status = unsafe { sys::urma_lab_segment_delete(raw.as_ptr()) };
        if status == 0 {
            Ok(())
        } else {
            Err(FfiError::Status(status))
        }
    }
}

impl Drop for SegmentHandle {
    fn drop(&mut self) {
        let _ = self.close();
    }
}
