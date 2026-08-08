//! Crate-private native boundary. Raw generated types must not escape this module.

use std::{
    ffi::{c_int, CStr},
    marker::PhantomData,
    ptr::NonNull,
    rc::Rc,
};

pub(crate) mod sys {
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FfiError {
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
}

impl Drop for NativeRuntime {
    fn drop(&mut self) {
        let _ = self.close();
    }
}
