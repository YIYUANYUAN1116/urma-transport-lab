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

pub(crate) struct JettyConfig {
    pub send_depth: u32,
    pub recv_depth: u32,
    pub max_send_sge: u32,
    pub max_recv_sge: u32,
    pub token: u32,
}

pub(crate) struct JettyDescriptorData {
    pub transport_type: u32,
    pub eid_index: u32,
    pub jetty_id: u32,
    pub opaque_data: Vec<u8>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CompletionRecord {
    pub status: i32,
    pub opcode: u32,
    pub user_ctx: u64,
    pub completion_len: u32,
    pub is_recv: bool,
    pub is_jetty: bool,
    pub user_ctx_valid: bool,
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
        let status =
            unsafe { sys::urma_lab_runtime_open(device_name.as_ptr(), eid_index, &mut raw) };
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
        let Some(raw) = self.raw else {
            return Ok(());
        };
        // SAFETY: `raw` is the unique live allocation returned by the shim and
        // is consumed exactly once, regardless of the reported close status.
        let status = unsafe { sys::urma_lab_runtime_close(raw.as_ptr()) };
        if status == 0 {
            self.raw = None;
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
        let status =
            unsafe { sys::urma_lab_runtime_query_device(raw_runtime.as_ptr(), raw.as_mut_ptr()) };
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

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct JfcEventReady {
    pub send: bool,
    pub recv: bool,
}

pub(crate) struct JfceHandle {
    raw: Option<NonNull<sys::urma_lab_jfce_t>>,
    _not_send_sync: PhantomData<Rc<()>>,
}

impl JfceHandle {
    pub(crate) fn create(runtime: &mut NativeRuntime) -> Result<Self, FfiError> {
        let raw_runtime = runtime.raw.ok_or(FfiError::Contract("runtime is closed"))?;
        let mut raw = std::ptr::null_mut();
        // SAFETY: The runtime pointer is live and `raw` is a valid out pointer.
        let status = unsafe { sys::urma_lab_jfce_create(raw_runtime.as_ptr(), &mut raw) };
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
        let Some(raw) = self.raw else {
            return Ok(());
        };
        // SAFETY: This is the unique live shim JFCE wrapper and all associated
        // JFC owners are closed before this method is called.
        let status = unsafe { sys::urma_lab_jfce_delete(raw.as_ptr()) };
        if status == 0 {
            self.raw = None;
            Ok(())
        } else {
            Err(FfiError::Status(status))
        }
    }

    pub(crate) fn wait(
        &self,
        send_jfc: &JfcHandle,
        recv_jfc: &JfcHandle,
        timeout_ms: i32,
    ) -> Result<Option<JfcEventReady>, FfiError> {
        if timeout_ms < 0 {
            return Err(FfiError::Contract("JFCE timeout must be non-negative"));
        }
        let jfce = self.raw.ok_or(FfiError::Contract("JFCE is closed"))?;
        let send = send_jfc
            .raw
            .ok_or(FfiError::Contract("send JFC is closed"))?;
        let recv = recv_jfc
            .raw
            .ok_or(FfiError::Contract("receive JFC is closed"))?;
        let mut ready_mask = 0u32;
        // SAFETY: All handles are live, associated by the shim at creation,
        // and `ready_mask` is a valid writable out pointer.
        let status = unsafe {
            sys::urma_lab_jfce_wait(
                jfce.as_ptr(),
                send.as_ptr(),
                recv.as_ptr(),
                timeout_ms,
                &mut ready_mask,
            )
        };
        if status < 0 {
            return Err(FfiError::Status(status));
        }
        if status == 0 {
            return Ok(None);
        }
        let known_mask = sys::URMA_LAB_JFCE_SEND_READY | sys::URMA_LAB_JFCE_RECV_READY;
        if ready_mask == 0 || ready_mask & !known_mask != 0 {
            return Err(FfiError::Contract("JFCE returned an invalid ready mask"));
        }
        Ok(Some(JfcEventReady {
            send: ready_mask & sys::URMA_LAB_JFCE_SEND_READY != 0,
            recv: ready_mask & sys::URMA_LAB_JFCE_RECV_READY != 0,
        }))
    }

    pub(crate) fn ack(&self) -> Result<(), FfiError> {
        let jfce = self.raw.ok_or(FfiError::Contract("JFCE is closed"))?;
        // SAFETY: A successful preceding wait stored one or more live event
        // records in the shim; this call acknowledges and clears them.
        let status = unsafe { sys::urma_lab_jfce_ack(jfce.as_ptr()) };
        if status == 0 {
            Ok(())
        } else {
            Err(FfiError::Status(status))
        }
    }
}

impl Drop for JfceHandle {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

pub(crate) struct JfcHandle {
    raw: Option<NonNull<sys::urma_lab_jfc_t>>,
    _not_send_sync: PhantomData<Rc<()>>,
}

impl JfcHandle {
    pub(crate) fn create(
        runtime: &mut NativeRuntime,
        jfce: &JfceHandle,
        depth: u32,
    ) -> Result<Self, FfiError> {
        let raw_runtime = runtime.raw.ok_or(FfiError::Contract("runtime is closed"))?;
        let raw_jfce = jfce.raw.ok_or(FfiError::Contract("JFCE is closed"))?;
        let mut raw = std::ptr::null_mut();
        // SAFETY: The runtime and JFCE pointers are live and `raw` is a valid
        // out pointer. The shim verifies that both owners match.
        let status = unsafe {
            sys::urma_lab_jfc_create(raw_runtime.as_ptr(), raw_jfce.as_ptr(), depth, &mut raw)
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
        let Some(raw) = self.raw else {
            return Ok(());
        };
        // SAFETY: This is the unique live shim JFC wrapper.
        let status = unsafe { sys::urma_lab_jfc_delete(raw.as_ptr()) };
        if status == 0 {
            self.raw = None;
            Ok(())
        } else {
            Err(FfiError::Status(status))
        }
    }

    pub(crate) fn poll(&self, capacity: usize) -> Result<Vec<CompletionRecord>, FfiError> {
        if capacity == 0 || capacity > 16 {
            return Err(FfiError::Contract("poll capacity must be in 1..=16"));
        }
        let raw = self.raw.ok_or(FfiError::Contract("JFC is closed"))?;
        let mut records: Vec<std::mem::MaybeUninit<sys::urma_lab_completion_t>> =
            Vec::with_capacity(capacity);
        records.resize_with(capacity, std::mem::MaybeUninit::uninit);
        // SAFETY: `records` has `capacity` writable entries and the live JFC is
        // only polled synchronously on its owner thread.
        let count = unsafe {
            sys::urma_lab_jfc_poll(raw.as_ptr(), capacity as u32, records.as_mut_ptr().cast())
        };
        if count < 0 {
            return Err(FfiError::Status(count));
        }
        let count = usize::try_from(count)
            .map_err(|_| FfiError::Contract("poll count does not fit usize"))?;
        if count > capacity {
            return Err(FfiError::Contract("provider returned too many completions"));
        }
        let mut out = Vec::with_capacity(count);
        for record in records.into_iter().take(count) {
            // SAFETY: the shim initializes exactly the first `count` entries.
            let record = unsafe { record.assume_init() };
            out.push(CompletionRecord {
                status: record.status,
                opcode: record.opcode,
                user_ctx: record.user_ctx,
                completion_len: record.completion_len,
                is_recv: record.is_recv != 0,
                is_jetty: record.is_jetty != 0,
                user_ctx_valid: record.user_ctx_valid != 0,
            });
        }
        Ok(out)
    }

    pub(crate) fn rearm(&self) -> Result<(), FfiError> {
        let raw = self.raw.ok_or(FfiError::Contract("JFC is closed"))?;
        // SAFETY: `raw` is a live JFC associated with a live JFCE.
        let status = unsafe { sys::urma_lab_jfc_rearm(raw.as_ptr()) };
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
        let raw_runtime = runtime.raw.ok_or(FfiError::Contract("runtime is closed"))?;
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
        let Some(raw) = self.raw else {
            return Ok(());
        };
        // SAFETY: This is the unique live shim Segment wrapper.
        let status = unsafe { sys::urma_lab_segment_delete(raw.as_ptr()) };
        if status == 0 {
            self.raw = None;
            Ok(())
        } else {
            Err(FfiError::Status(status))
        }
    }

    pub(crate) fn write(&self, offset: u64, data: &[u8]) -> Result<(), FfiError> {
        let raw = self.raw.ok_or(FfiError::Contract("Segment is closed"))?;
        let length = u32::try_from(data.len())
            .map_err(|_| FfiError::Contract("write length exceeds u32"))?;
        if length == 0 {
            return Err(FfiError::Contract("zero-length Segment write"));
        }
        // SAFETY: data remains valid for the synchronous copy into the Segment.
        status_result(unsafe {
            sys::urma_lab_segment_write(raw.as_ptr(), offset, data.as_ptr(), length)
        })
    }

    pub(crate) fn read(&self, offset: u64, length: u32) -> Result<Vec<u8>, FfiError> {
        let raw = self.raw.ok_or(FfiError::Contract("Segment is closed"))?;
        if length == 0 {
            return Err(FfiError::Contract("zero-length Segment read"));
        }
        let mut out = vec![0u8; length as usize];
        // SAFETY: out has exactly `length` writable bytes.
        status_result(unsafe {
            sys::urma_lab_segment_read(raw.as_ptr(), offset, out.as_mut_ptr(), length)
        })?;
        Ok(out)
    }
}

impl Drop for SegmentHandle {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

pub(crate) struct JettyHandle {
    raw: Option<NonNull<sys::urma_lab_jetty_t>>,
    _not_send_sync: PhantomData<Rc<()>>,
}

impl JettyHandle {
    pub(crate) fn create(
        runtime: &mut NativeRuntime,
        send_jfc: &JfcHandle,
        recv_jfc: &JfcHandle,
        config: &JettyConfig,
    ) -> Result<Self, FfiError> {
        let runtime = runtime.raw.ok_or(FfiError::Contract("runtime is closed"))?;
        let send_jfc = send_jfc
            .raw
            .ok_or(FfiError::Contract("send JFC is closed"))?;
        let recv_jfc = recv_jfc
            .raw
            .ok_or(FfiError::Contract("recv JFC is closed"))?;
        let raw_config = sys::urma_lab_jetty_config_t {
            send_depth: config.send_depth,
            recv_depth: config.recv_depth,
            max_send_sge: config.max_send_sge,
            max_recv_sge: config.max_recv_sge,
            token: config.token,
        };
        let mut raw = std::ptr::null_mut();
        // SAFETY: All three owners are live and `raw` is a valid out pointer.
        let status = unsafe {
            sys::urma_lab_jetty_create(
                runtime.as_ptr(),
                send_jfc.as_ptr(),
                recv_jfc.as_ptr(),
                &raw_config,
                &mut raw,
            )
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

    pub(crate) fn export_descriptor(&self) -> Result<JettyDescriptorData, FfiError> {
        let jetty = self.raw.ok_or(FfiError::Contract("Jetty is closed"))?;
        let mut descriptor = std::ptr::null_mut();
        // SAFETY: Jetty is live and descriptor is a valid out pointer.
        let status =
            unsafe { sys::urma_lab_jetty_export_descriptor(jetty.as_ptr(), &mut descriptor) };
        if status != 0 {
            return Err(FfiError::Status(status));
        }
        let descriptor = NonNull::new(descriptor).ok_or(FfiError::NullHandle)?;
        let result = copy_descriptor(descriptor);
        // SAFETY: descriptor is the unique allocation returned above.
        unsafe { sys::urma_lab_descriptor_free(descriptor.as_ptr()) };
        result
    }

    pub(crate) fn import(
        &mut self,
        descriptor: &JettyDescriptorData,
        token: u32,
    ) -> Result<(), FfiError> {
        let jetty = self.raw.ok_or(FfiError::Contract("Jetty is closed"))?;
        let opaque_len = u32::try_from(descriptor.opaque_data.len())
            .map_err(|_| FfiError::Contract("descriptor length exceeds u32"))?;
        let meta = sys::urma_lab_jetty_descriptor_meta_t {
            transport_type: descriptor.transport_type,
            eid_index: descriptor.eid_index,
            jetty_id: descriptor.jetty_id,
            opaque_len,
        };
        // SAFETY: Descriptor bytes are validated by the safe wire layer and
        // remain live for the synchronous shim import call.
        let status = unsafe {
            sys::urma_lab_jetty_import(
                jetty.as_ptr(),
                &meta,
                descriptor.opaque_data.as_ptr(),
                opaque_len,
                token,
            )
        };
        status_result(status)
    }

    pub(crate) fn bind(&mut self) -> Result<(), FfiError> {
        let jetty = self.raw.ok_or(FfiError::Contract("Jetty is closed"))?;
        // SAFETY: Jetty and its imported target are owned by this handle.
        status_result(unsafe { sys::urma_lab_jetty_bind(jetty.as_ptr()) })
    }

    pub(crate) fn unbind(&mut self) -> Result<(), FfiError> {
        let jetty = self.raw.ok_or(FfiError::Contract("Jetty is closed"))?;
        // SAFETY: Jetty is uniquely owned by this handle.
        status_result(unsafe { sys::urma_lab_jetty_unbind(jetty.as_ptr()) })
    }

    pub(crate) fn unimport(&mut self) -> Result<(), FfiError> {
        let jetty = self.raw.ok_or(FfiError::Contract("Jetty is closed"))?;
        // SAFETY: The imported target, if any, is uniquely owned by the shim.
        status_result(unsafe { sys::urma_lab_jetty_unimport(jetty.as_ptr()) })
    }

    pub(crate) fn mark_error(&mut self) -> Result<(), FfiError> {
        let jetty = self.raw.ok_or(FfiError::Contract("Jetty is closed"))?;
        // SAFETY: Jetty is uniquely owned by this handle.
        status_result(unsafe { sys::urma_lab_jetty_mark_error(jetty.as_ptr()) })
    }

    pub(crate) fn post_send(
        &mut self,
        segment: &SegmentHandle,
        offset: u64,
        length: u32,
        user_ctx: u64,
    ) -> Result<WrHandle, FfiError> {
        self.post(segment, offset, length, user_ctx, true)
    }

    pub(crate) fn post_recv(
        &mut self,
        segment: &SegmentHandle,
        offset: u64,
        length: u32,
        user_ctx: u64,
    ) -> Result<WrHandle, FfiError> {
        self.post(segment, offset, length, user_ctx, false)
    }

    fn post(
        &mut self,
        segment: &SegmentHandle,
        offset: u64,
        length: u32,
        user_ctx: u64,
        send: bool,
    ) -> Result<WrHandle, FfiError> {
        let jetty = self.raw.ok_or(FfiError::Contract("Jetty is closed"))?;
        let segment = segment.raw.ok_or(FfiError::Contract("Segment is closed"))?;
        let mut raw = std::ptr::null_mut();
        // SAFETY: Jetty and Segment are live, range validation is repeated by
        // the shim, and raw is a valid out pointer.
        let status = unsafe {
            if send {
                sys::urma_lab_post_send(
                    jetty.as_ptr(),
                    segment.as_ptr(),
                    offset,
                    length,
                    user_ctx,
                    &mut raw,
                )
            } else {
                sys::urma_lab_post_recv(
                    jetty.as_ptr(),
                    segment.as_ptr(),
                    offset,
                    length,
                    user_ctx,
                    &mut raw,
                )
            }
        };
        if status != 0 {
            return Err(FfiError::Status(status));
        }
        Ok(WrHandle {
            raw: Some(NonNull::new(raw).ok_or(FfiError::NullHandle)?),
            _not_send_sync: PhantomData,
        })
    }

    pub(crate) fn close(&mut self) -> Result<(), FfiError> {
        let Some(jetty) = self.raw else {
            return Ok(());
        };
        // SAFETY: This consumes the unique local Jetty wrapper.
        let result = status_result(unsafe { sys::urma_lab_jetty_delete(jetty.as_ptr()) });
        if result.is_ok() {
            self.raw = None;
        }
        result
    }
}

/// Owns C WR/SGE metadata until the matching CQE is consumed.
pub(crate) struct WrHandle {
    raw: Option<NonNull<sys::urma_lab_wr_t>>,
    _not_send_sync: PhantomData<Rc<()>>,
}

impl WrHandle {
    pub(crate) fn complete(mut self) {
        if let Some(raw) = self.raw.take() {
            // SAFETY: Completion routing guarantees one call for the unique WR.
            unsafe { sys::urma_lab_wr_complete(raw.as_ptr()) };
        }
    }
}

impl Drop for WrHandle {
    fn drop(&mut self) {
        // A posted WR cannot be freed safely without its CQE. Deliberately leak
        // it; shim outstanding counters prevent teardown of dependent objects.
    }
}

impl Drop for JettyHandle {
    fn drop(&mut self) {
        let _ = self.unbind();
        let _ = self.unimport();
        let _ = self.close();
    }
}

fn copy_descriptor(
    descriptor: NonNull<sys::urma_lab_descriptor_t>,
) -> Result<JettyDescriptorData, FfiError> {
    let mut meta = std::mem::MaybeUninit::<sys::urma_lab_jetty_descriptor_meta_t>::uninit();
    // SAFETY: Both pointers are live for this call.
    let status =
        unsafe { sys::urma_lab_descriptor_get_meta(descriptor.as_ptr(), meta.as_mut_ptr()) };
    if status != 0 {
        return Err(FfiError::Status(status));
    }
    // SAFETY: A zero status initializes the complete integer-only DTO.
    let meta = unsafe { meta.assume_init() };
    let length = usize::try_from(meta.opaque_len)
        .map_err(|_| FfiError::Contract("descriptor length does not fit usize"))?;
    const MAX_NATIVE_DESCRIPTOR_LEN: usize = 64 * 1024;
    if length == 0 || length > MAX_NATIVE_DESCRIPTOR_LEN {
        return Err(FfiError::Contract(
            "native descriptor length is zero or exceeds the M2 limit",
        ));
    }
    let mut opaque_data = vec![0u8; length];
    // SAFETY: The vector has exactly the capacity reported by the descriptor.
    let status = unsafe {
        sys::urma_lab_descriptor_copy(
            descriptor.as_ptr(),
            opaque_data.as_mut_ptr(),
            meta.opaque_len,
        )
    };
    if status != 0 {
        return Err(FfiError::Status(status));
    }
    Ok(JettyDescriptorData {
        transport_type: meta.transport_type,
        eid_index: meta.eid_index,
        jetty_id: meta.jetty_id,
        opaque_data,
    })
}

fn status_result(status: c_int) -> Result<(), FfiError> {
    if status == 0 {
        Ok(())
    } else {
        Err(FfiError::Status(status))
    }
}
