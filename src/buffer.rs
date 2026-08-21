use crate::{Error, Result};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BufferPoolConfig {
    pub slot_size: usize,
    pub tx_slot_count: usize,
    pub rx_slot_count: usize,
    pub alignment: usize,
    /// Diagnostic/perftest-style layout: logical TX slots share one immutable
    /// physical range. Normal writes are forbidden once this is enabled.
    pub alias_tx_slots: bool,
}

impl Default for BufferPoolConfig {
    fn default() -> Self {
        Self {
            slot_size: 64 * 1024,
            tx_slot_count: 128,
            rx_slot_count: 512,
            alignment: 4096,
            alias_tx_slots: false,
        }
    }
}

impl BufferPoolConfig {
    pub fn total_len(&self) -> Result<usize> {
        if self.slot_size == 0 {
            return Err(Error::InvalidConfiguration(
                "slot_size must be non-zero".into(),
            ));
        }
        if self.tx_slot_count == 0 || self.rx_slot_count == 0 {
            return Err(Error::InvalidConfiguration(
                "tx_slot_count and rx_slot_count must be non-zero".into(),
            ));
        }
        if self.alignment < std::mem::size_of::<usize>() || !self.alignment.is_power_of_two() {
            return Err(Error::InvalidConfiguration(
                "alignment must be a power of two and at least pointer-sized".into(),
            ));
        }
        let physical_tx_slots = if self.alias_tx_slots {
            1
        } else {
            self.tx_slot_count
        };
        let slots = physical_tx_slots
            .checked_add(self.rx_slot_count)
            .ok_or_else(|| Error::InvalidConfiguration("slot count overflow".into()))?;
        self.slot_size
            .checked_mul(slots)
            .ok_or_else(|| Error::InvalidConfiguration("buffer pool size overflow".into()))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SlotKind {
    Tx,
    Rx,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SlotState {
    Free,
    Allocated,
    Posted,
    Completed,
    Leased,
    PostedRecv,
    RecvCompleted,
    SendPosted,
    SendCompleted,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SlotStateSnapshot {
    pub free: usize,
    pub allocated: usize,
    pub posted_recv: usize,
    pub recv_completed: usize,
    pub send_posted: usize,
    pub send_completed: usize,
    pub other: usize,
}

impl SlotStateSnapshot {
    fn observe(&mut self, state: SlotState) {
        match state {
            SlotState::Free => self.free += 1,
            SlotState::Allocated => self.allocated += 1,
            SlotState::PostedRecv => self.posted_recv += 1,
            SlotState::RecvCompleted => self.recv_completed += 1,
            SlotState::SendPosted => self.send_posted += 1,
            SlotState::SendCompleted => self.send_completed += 1,
            SlotState::Posted | SlotState::Completed | SlotState::Leased => self.other += 1,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct SlotId(usize);

impl SlotId {
    pub fn index(self) -> usize {
        self.0
    }

    pub(crate) fn from_index(index: usize) -> Self {
        Self(index)
    }
}

#[cfg(feature = "urma")]
#[derive(Clone, Debug)]
struct BufferSlot {
    kind: SlotKind,
    offset: usize,
    len: usize,
    state: SlotState,
}

#[cfg(feature = "urma")]
mod native {
    use super::*;
    use crate::ffi;
    use std::{
        collections::VecDeque,
        ptr::NonNull,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
    };

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    pub(crate) struct RegisteredRxChunk {
        pub(crate) sequence: Option<u64>,
        pub(crate) length: usize,
    }

    /// A filled TX slot run that is not yet referenced by any provider WR.
    /// The owning connection must either post or explicitly discard it.
    #[must_use = "a prepared TX batch must be posted or discarded"]
    pub(crate) struct PreparedTxBatch {
        pub(crate) layouts: Vec<(SlotId, u64, u32)>,
    }

    impl PreparedTxBatch {
        pub(crate) fn len(&self) -> usize {
            self.layouts.len()
        }
    }

    #[derive(Clone, Copy)]
    struct RegisteredRxSpan {
        data: NonNull<u8>,
        length: usize,
    }

    /// Read-only ownership of a logical completed RX window. Its registered
    /// slots need not be physically contiguous: provider receive matching and
    /// slot reuse order are transport details, while `spans` retains wire
    /// order. Slots remain `Leased` until the transport consumes this value in
    /// `recycle_recv_lease`.
    pub(crate) struct RegisteredRxWindowLease {
        spans: Vec<RegisteredRxSpan>,
        length: usize,
        chunks: Vec<RegisteredRxChunk>,
        slots: Vec<SlotId>,
        tracker: Arc<AtomicUsize>,
        #[cfg(test)]
        _owned_test_bytes: Vec<Box<[u8]>>,
    }

    // SAFETY: construction requires a successful CQE for every covered slot,
    // changes those slots to Leased, and the only public memory access is
    // shared. The slots cannot be reposted until this value is consumed by the
    // owning UrmaBufferPool. Pool close refuses to free an active lease.
    unsafe impl Send for RegisteredRxWindowLease {}
    // SAFETY: parts() returns shared immutable access only. Concurrent CRC and
    // pwrite readers cannot mutate the registered ranges.
    unsafe impl Sync for RegisteredRxWindowLease {}

    impl RegisteredRxWindowLease {
        pub(crate) fn parts(&self) -> impl Iterator<Item = &[u8]> {
            self.spans.iter().map(|span| {
                // SAFETY: every span was validated against the live Segment
                // during construction and remains leased from repost.
                unsafe { std::slice::from_raw_parts(span.data.as_ptr(), span.length) }
            })
        }

        pub(crate) fn single_span_bytes(&self) -> Result<&[u8]> {
            if self.spans.len() != 1 {
                return Err(Error::Protocol(
                    "control RX lease unexpectedly spans multiple slots".into(),
                ));
            }
            Ok(self.parts().next().expect("one span was checked"))
        }

        pub(crate) fn chunks(&self) -> &[RegisteredRxChunk] {
            &self.chunks
        }

        pub(crate) fn len(&self) -> usize {
            self.length
        }

        #[cfg(test)]
        pub(crate) fn from_test_bytes(bytes: Vec<u8>, chunks: Vec<(Option<u64>, usize)>) -> Self {
            Self::from_test_parts(vec![bytes], chunks)
        }

        #[cfg(test)]
        pub(crate) fn from_test_parts(
            parts: Vec<Vec<u8>>,
            chunks: Vec<(Option<u64>, usize)>,
        ) -> Self {
            let owned = parts
                .into_iter()
                .map(Vec::into_boxed_slice)
                .collect::<Vec<_>>();
            let spans = owned
                .iter()
                .map(|bytes| RegisteredRxSpan {
                    data: NonNull::new(bytes.as_ptr().cast_mut()).expect("test span is non-empty"),
                    length: bytes.len(),
                })
                .collect::<Vec<_>>();
            Self {
                length: owned.iter().map(|bytes| bytes.len()).sum(),
                spans,
                chunks: chunks
                    .into_iter()
                    .map(|(sequence, length)| RegisteredRxChunk { sequence, length })
                    .collect(),
                slots: Vec::new(),
                tracker: Arc::new(AtomicUsize::new(1)),
                _owned_test_bytes: owned,
            }
        }
    }

    impl Drop for RegisteredRxWindowLease {
        fn drop(&mut self) {
            let previous = self.tracker.fetch_sub(1, Ordering::AcqRel);
            debug_assert_ne!(previous, 0, "RX lease tracker underflow");
        }
    }

    /// RAII owner of one local-only registered Segment and its backing memory.
    pub(crate) struct UrmaRegisteredSegment {
        handle: ffi::SegmentHandle,
        len: usize,
        alignment: usize,
    }

    impl UrmaRegisteredSegment {
        fn create(runtime: &mut ffi::NativeRuntime, len: usize, alignment: usize) -> Result<Self> {
            let length = u64::try_from(len).map_err(|_| {
                Error::InvalidConfiguration("registered length does not fit u64".into())
            })?;
            let alignment = u64::try_from(alignment)
                .map_err(|_| Error::InvalidConfiguration("alignment does not fit u64".into()))?;
            let handle = ffi::SegmentHandle::create(runtime, length, alignment)
                .map_err(|error| map_ffi_error("register_segment", error))?;
            Ok(Self {
                handle,
                len,
                alignment: alignment as usize,
            })
        }

        fn close(&mut self) -> Result<()> {
            self.handle
                .close()
                .map_err(|error| map_ffi_error("unregister_segment", error))
        }
    }

    /// Fixed-size slot metadata over one local registered Segment.
    pub struct UrmaBufferPool {
        config: BufferPoolConfig,
        segment: Option<UrmaRegisteredSegment>,
        slots: Vec<BufferSlot>,
        free_tx: Vec<usize>,
        free_rx: VecDeque<usize>,
        accepting: bool,
        active_rx_leases: Arc<AtomicUsize>,
    }

    impl UrmaBufferPool {
        pub(crate) fn create(
            runtime: &mut ffi::NativeRuntime,
            config: BufferPoolConfig,
        ) -> Result<Self> {
            let total_len = config.total_len()?;
            let segment = UrmaRegisteredSegment::create(runtime, total_len, config.alignment)?;
            let slot_count = config
                .tx_slot_count
                .checked_add(config.rx_slot_count)
                .ok_or_else(|| Error::InvalidConfiguration("slot count overflow".into()))?;
            let mut slots = Vec::with_capacity(slot_count);
            let physical_tx_slots = if config.alias_tx_slots {
                1
            } else {
                config.tx_slot_count
            };
            for index in 0..config.tx_slot_count {
                slots.push(BufferSlot {
                    kind: SlotKind::Tx,
                    offset: slot_offset(&config, if config.alias_tx_slots { 0 } else { index })?,
                    len: config.slot_size,
                    state: SlotState::Free,
                });
            }
            for index in 0..config.rx_slot_count {
                slots.push(BufferSlot {
                    kind: SlotKind::Rx,
                    offset: slot_offset(
                        &config,
                        physical_tx_slots.checked_add(index).ok_or_else(|| {
                            Error::InvalidConfiguration("slot index overflow".into())
                        })?,
                    )?,
                    len: config.slot_size,
                    state: SlotState::Free,
                });
            }
            let free_tx = (0..config.tx_slot_count).rev().collect();
            let free_rx = (config.tx_slot_count..slot_count).collect();
            Ok(Self {
                config,
                segment: Some(segment),
                slots,
                free_tx,
                free_rx,
                accepting: true,
                active_rx_leases: Arc::new(AtomicUsize::new(0)),
            })
        }

        pub fn config(&self) -> &BufferPoolConfig {
            &self.config
        }

        pub fn allocate(&mut self, kind: SlotKind) -> Option<SlotId> {
            if !self.accepting {
                return None;
            }
            let index = match kind {
                SlotKind::Tx => self.free_tx.pop()?,
                SlotKind::Rx => self.free_rx.pop_front()?,
            };
            let slot = self.slots.get_mut(index)?;
            debug_assert_eq!(slot.kind, kind);
            debug_assert_eq!(slot.state, SlotState::Free);
            slot.state = SlotState::Allocated;
            Some(SlotId(index))
        }

        pub fn release(&mut self, id: SlotId) -> Result<()> {
            let slot = self.slots.get_mut(id.0).ok_or_else(|| {
                Error::InvalidConfiguration("slot id is outside this buffer pool".into())
            })?;
            if !matches!(
                slot.state,
                SlotState::Allocated | SlotState::RecvCompleted | SlotState::SendCompleted
            ) {
                return Err(Error::InvalidConfiguration(
                    "slot cannot be released while a WR may still reference it".into(),
                ));
            }
            slot.state = SlotState::Free;
            match slot.kind {
                SlotKind::Tx => self.free_tx.push(id.0),
                SlotKind::Rx => self.free_rx.push_back(id.0),
            }
            Ok(())
        }

        pub fn slot_state(&self, id: SlotId) -> Option<SlotState> {
            self.slots.get(id.0).map(|slot| slot.state)
        }

        pub(crate) fn slot_state_snapshot(&self, kind: SlotKind) -> SlotStateSnapshot {
            let mut snapshot = SlotStateSnapshot::default();
            for slot in self.slots.iter().filter(|slot| slot.kind == kind) {
                snapshot.observe(slot.state);
            }
            snapshot
        }

        pub fn slot_layout(&self, id: SlotId) -> Option<(usize, usize)> {
            self.slots.get(id.0).map(|slot| (slot.offset, slot.len))
        }

        pub(crate) fn segment_handle(&self) -> Result<&ffi::SegmentHandle> {
            self.segment
                .as_ref()
                .map(|segment| &segment.handle)
                .ok_or_else(|| Error::InvalidConfiguration("registered Segment is closed".into()))
        }

        pub(crate) fn write_tx(&mut self, id: SlotId, data: &[u8]) -> Result<(u64, u32)> {
            if self.config.alias_tx_slots
                && self
                    .slots
                    .iter()
                    .take(self.config.tx_slot_count)
                    .enumerate()
                    .any(|(index, slot)| index != id.0 && slot.state != SlotState::Free)
            {
                return Err(Error::Protocol(
                    "aliased TX memory cannot be rewritten while another TX WR is outstanding"
                        .into(),
                ));
            }
            let (offset, capacity, kind, state) = self.slot_fields(id)?;
            if kind != SlotKind::Tx || state != SlotState::Allocated {
                return Err(Error::InvalidConfiguration(
                    "TX write requires an allocated TX slot".into(),
                ));
            }
            if data.is_empty() || data.len() > capacity {
                return Err(Error::InvalidConfiguration(format!(
                    "TX message length {} is outside 1..={capacity}",
                    data.len()
                )));
            }
            let offset = u64::try_from(offset)
                .map_err(|_| Error::InvalidConfiguration("slot offset exceeds u64".into()))?;
            self.segment_handle()?
                .write(offset, data)
                .map_err(|error| map_ffi_error("write_tx_slot", error))?;
            let length = u32::try_from(data.len())
                .map_err(|_| Error::InvalidConfiguration("TX length exceeds u32".into()))?;
            Ok((offset, length))
        }

        /// Allocates one physically contiguous run of TX slots and fills the
        /// whole registered range in one producer call. Every slot except the
        /// final one must be full so that packed file bytes remain aligned with
        /// the independently posted SEND SGEs.
        pub(crate) fn prepare_tx_batch(
            &mut self,
            lengths: &[usize],
            fill: impl FnOnce(&mut [u8]) -> Result<()>,
        ) -> Result<PreparedTxBatch> {
            if self.config.alias_tx_slots || lengths.is_empty() {
                return Err(Error::InvalidConfiguration(
                    "direct TX batch requires non-empty, non-aliased TX slots".into(),
                ));
            }
            if lengths.len() > self.config.tx_slot_count {
                return Err(Error::InvalidConfiguration(
                    "direct TX batch exceeds TX slot count".into(),
                ));
            }
            for (index, &length) in lengths.iter().enumerate() {
                if length == 0 || length > self.config.slot_size {
                    return Err(Error::InvalidConfiguration(format!(
                        "direct TX batch length {length} is outside 1..={} at index {index}",
                        self.config.slot_size
                    )));
                }
                if index + 1 != lengths.len() && length != self.config.slot_size {
                    return Err(Error::InvalidConfiguration(
                        "only the final direct TX batch slot may be short".into(),
                    ));
                }
            }
            if self.segment.is_none() {
                return Err(Error::InvalidConfiguration(
                    "registered Segment is closed".into(),
                ));
            }
            let fill_length = self
                .config
                .slot_size
                .checked_mul(lengths.len() - 1)
                .and_then(|prefix| prefix.checked_add(*lengths.last().expect("non-empty")))
                .ok_or_else(|| Error::InvalidConfiguration("TX batch length overflow".into()))?;
            let fill_length_u32 = u32::try_from(fill_length)
                .map_err(|_| Error::InvalidConfiguration("TX batch length exceeds u32".into()))?;

            let start = self
                .slots
                .iter()
                .take(self.config.tx_slot_count)
                .map(|slot| slot.state)
                .collect::<Vec<_>>()
                .windows(lengths.len())
                .position(|states| states.iter().all(|state| *state == SlotState::Free))
                .ok_or_else(|| Error::InvalidConfiguration("no contiguous free TX batch".into()))?;
            let end = start + lengths.len();
            let first_offset_u64 = u64::try_from(self.slots[start].offset)
                .map_err(|_| Error::InvalidConfiguration("slot offset exceeds u64".into()))?;
            let layouts = lengths
                .iter()
                .enumerate()
                .map(|(relative, &length)| {
                    let id = SlotId(start + relative);
                    let offset = u64::try_from(self.slots[id.0].offset).map_err(|_| {
                        Error::InvalidConfiguration("slot offset exceeds u64".into())
                    })?;
                    let length = u32::try_from(length)
                        .map_err(|_| Error::InvalidConfiguration("TX length exceeds u32".into()))?;
                    Ok((id, offset, length))
                })
                .collect::<Result<Vec<_>>>()?;
            let allocated = (start..end).collect::<Vec<_>>();
            self.free_tx.retain(|index| !allocated.contains(index));
            for &index in &allocated {
                self.slots[index].state = SlotState::Allocated;
            }

            let fill_result = self
                .segment
                .as_mut()
                .expect("Segment presence checked before allocating TX slots")
                .handle
                .with_write(first_offset_u64, fill_length_u32, fill)
                .map_err(|error| map_ffi_error("fill_tx_batch", error))
                .and_then(|result| result);
            if let Err(error) = fill_result {
                for index in allocated {
                    self.release(SlotId(index))?;
                }
                return Err(error);
            }
            Ok(PreparedTxBatch { layouts })
        }

        pub(crate) fn discard_tx_batch(&mut self, batch: PreparedTxBatch) -> Result<()> {
            for (slot, _, _) in batch.layouts {
                self.release(slot)?;
            }
            Ok(())
        }

        pub(crate) fn prepare_aliased_tx(&mut self, data: &[u8]) -> Result<()> {
            if !self.config.alias_tx_slots || data.is_empty() || data.len() > self.config.slot_size
            {
                return Err(Error::InvalidConfiguration(
                    "aliased TX preparation requires a non-empty payload within slot_size".into(),
                ));
            }
            if self
                .slots
                .iter()
                .take(self.config.tx_slot_count)
                .any(|slot| slot.state != SlotState::Free)
            {
                return Err(Error::Protocol(
                    "aliased TX payload can only be prepared with no outstanding TX WR".into(),
                ));
            }
            self.segment_handle()?
                .write(0, data)
                .map_err(|error| map_ffi_error("prepare_aliased_tx", error))
        }

        pub(crate) fn aliased_tx_layout(&self, id: SlotId, length: usize) -> Result<(u64, u32)> {
            let (offset, capacity, kind, state) = self.slot_fields(id)?;
            if !self.config.alias_tx_slots || kind != SlotKind::Tx || state != SlotState::Allocated
            {
                return Err(Error::InvalidConfiguration(
                    "prepared TX requires an allocated aliased TX slot".into(),
                ));
            }
            if length == 0 || length > capacity {
                return Err(Error::InvalidConfiguration(
                    "prepared TX length is outside the slot capacity".into(),
                ));
            }
            Ok((
                offset as u64,
                u32::try_from(length)
                    .map_err(|_| Error::InvalidConfiguration("TX length exceeds u32".into()))?,
            ))
        }

        pub(crate) fn recv_post_layout(&self, id: SlotId) -> Result<(u64, u32)> {
            let (offset, capacity, kind, state) = self.slot_fields(id)?;
            if kind != SlotKind::Rx || state != SlotState::Allocated {
                return Err(Error::InvalidConfiguration(
                    "RECV post requires an allocated RX slot".into(),
                ));
            }
            Ok((
                u64::try_from(offset)
                    .map_err(|_| Error::InvalidConfiguration("slot offset exceeds u64".into()))?,
                u32::try_from(capacity)
                    .map_err(|_| Error::InvalidConfiguration("slot size exceeds u32".into()))?,
            ))
        }

        pub(crate) fn mark_posted(&mut self, id: SlotId, kind: SlotKind) -> Result<()> {
            let expected_kind = self
                .slots
                .get(id.0)
                .ok_or_else(|| Error::Protocol("completion slot is outside buffer pool".into()))?
                .kind;
            if expected_kind != kind {
                return Err(Error::Protocol(
                    "WR operation does not match slot kind".into(),
                ));
            }
            self.transition(
                id,
                SlotState::Allocated,
                match kind {
                    SlotKind::Tx => SlotState::SendPosted,
                    SlotKind::Rx => SlotState::PostedRecv,
                },
            )
        }

        pub(crate) fn rollback_post(&mut self, id: SlotId, kind: SlotKind) -> Result<()> {
            self.transition(
                id,
                match kind {
                    SlotKind::Tx => SlotState::SendPosted,
                    SlotKind::Rx => SlotState::PostedRecv,
                },
                SlotState::Allocated,
            )
        }

        pub(crate) fn complete_send(&mut self, id: SlotId) -> Result<()> {
            self.transition(id, SlotState::SendPosted, SlotState::SendCompleted)
        }

        pub(crate) fn complete_error(
            &mut self,
            id: SlotId,
            operation: crate::wr::OperationType,
        ) -> Result<()> {
            let (from, to) = match operation {
                crate::wr::OperationType::Send => (SlotState::SendPosted, SlotState::SendCompleted),
                crate::wr::OperationType::Recv => (SlotState::PostedRecv, SlotState::RecvCompleted),
            };
            self.transition(id, from, to)
        }

        pub(crate) fn complete_recv(&mut self, id: SlotId, length: u32) -> Result<Vec<u8>> {
            let (offset, capacity, kind, state) = self.slot_fields(id)?;
            if kind != SlotKind::Rx || state != SlotState::PostedRecv {
                return Err(Error::Protocol(
                    "RECV CQE does not match a posted RX slot".into(),
                ));
            }
            let length = usize::try_from(length)
                .map_err(|_| Error::Protocol("completion length exceeds usize".into()))?;
            if length == 0 || length > capacity {
                return Err(Error::Protocol(format!(
                    "RECV completion length {length} is outside 1..={capacity}"
                )));
            }
            let bytes = self
                .segment_handle()?
                .read(offset as u64, length as u32)
                .map_err(|error| map_ffi_error("read_rx_slot", error))?;
            self.transition(id, SlotState::PostedRecv, SlotState::RecvCompleted)?;
            Ok(bytes)
        }

        pub(crate) fn complete_recv_with<R>(
            &mut self,
            id: SlotId,
            length: u32,
            consume: impl FnOnce(&[u8]) -> Result<R>,
        ) -> Result<R> {
            let (offset, capacity, kind, state) = self.slot_fields(id)?;
            if kind != SlotKind::Rx || state != SlotState::PostedRecv {
                return Err(Error::Protocol(
                    "RECV CQE does not match a posted RX slot".into(),
                ));
            }
            let length = usize::try_from(length)
                .map_err(|_| Error::Protocol("completion length exceeds usize".into()))?;
            if length == 0 || length > capacity {
                return Err(Error::Protocol(format!(
                    "RECV completion length {length} is outside 1..={capacity}"
                )));
            }
            // The CQE proves the provider no longer mutates this receive
            // range. Keep the slot non-reusable while the consumer borrows it.
            let consumed = self
                .segment_handle()?
                .with_read(offset as u64, length as u32, consume)
                .map_err(|error| map_ffi_error("borrow_rx_slot", error))?;
            self.transition(id, SlotState::PostedRecv, SlotState::RecvCompleted)?;
            consumed
        }

        pub(crate) fn complete_recv_leased(&mut self, id: SlotId, length: u32) -> Result<()> {
            let (_, capacity, kind, state) = self.slot_fields(id)?;
            if kind != SlotKind::Rx || state != SlotState::PostedRecv {
                return Err(Error::Protocol(
                    "RECV CQE does not match a posted RX slot".into(),
                ));
            }
            let length = usize::try_from(length)
                .map_err(|_| Error::Protocol("completion length exceeds usize".into()))?;
            if length == 0 || length > capacity {
                return Err(Error::Protocol(format!(
                    "RECV completion length {length} is outside 1..={capacity}"
                )));
            }
            self.transition(id, SlotState::PostedRecv, SlotState::RecvCompleted)
        }

        pub(crate) fn lease_completed_recv_window(
            &mut self,
            completions: &[(SlotId, Option<u64>, u32)],
        ) -> Result<RegisteredRxWindowLease> {
            if completions.is_empty() {
                return Err(Error::InvalidConfiguration(
                    "RX lease requires at least one completion".into(),
                ));
            }
            let mut slots = Vec::with_capacity(completions.len());
            let mut chunks = Vec::with_capacity(completions.len());
            let mut spans: Vec<RegisteredRxSpan> = Vec::with_capacity(completions.len());
            let mut total = 0usize;
            let mut previous_end = None;
            let base = self
                .segment_handle()?
                .base_ptr()
                .map_err(|error| map_ffi_error("borrow_rx_window", error))?;
            let registered_len = self.registered_len();
            for (index, &(slot, sequence, length)) in completions.iter().enumerate() {
                let (offset, capacity, kind, state) = self.slot_fields(slot)?;
                if kind != SlotKind::Rx || state != SlotState::RecvCompleted {
                    return Err(Error::Protocol(
                        "RX lease requires completed receive slots".into(),
                    ));
                }
                let length = usize::try_from(length)
                    .map_err(|_| Error::Protocol("completion length exceeds usize".into()))?;
                if length == 0 || length > capacity {
                    return Err(Error::Protocol("invalid RX lease chunk length".into()));
                }
                if index + 1 != completions.len() && length != capacity {
                    return Err(Error::Protocol(
                        "only the final RX lease chunk may be short".into(),
                    ));
                }
                let end = offset
                    .checked_add(length)
                    .ok_or_else(|| Error::Protocol("RX lease offset overflow".into()))?;
                if end > registered_len {
                    return Err(Error::Protocol(
                        "RX lease span exceeds registered Segment".into(),
                    ));
                }
                // SAFETY: offset..end was checked against the live registered
                // Segment, and the slot cannot be reposted while leased.
                let data = unsafe { NonNull::new_unchecked(base.as_ptr().add(offset)) };
                if previous_end == Some(offset) {
                    let previous = spans
                        .last_mut()
                        .expect("a previous end implies a previous span");
                    previous.length = previous
                        .length
                        .checked_add(length)
                        .ok_or_else(|| Error::Protocol("RX lease span length overflow".into()))?;
                } else {
                    spans.push(RegisteredRxSpan { data, length });
                }
                previous_end = Some(end);
                total = total
                    .checked_add(length)
                    .ok_or_else(|| Error::Protocol("RX lease length overflow".into()))?;
                slots.push(slot);
                chunks.push(RegisteredRxChunk { sequence, length });
            }
            for &slot in &slots {
                self.transition(slot, SlotState::RecvCompleted, SlotState::Leased)?;
            }
            self.active_rx_leases.fetch_add(1, Ordering::AcqRel);
            Ok(RegisteredRxWindowLease {
                spans,
                length: total,
                chunks,
                slots,
                tracker: self.active_rx_leases.clone(),
                #[cfg(test)]
                _owned_test_bytes: Vec::new(),
            })
        }

        pub(crate) fn recycle_recv_lease(
            &mut self,
            lease: RegisteredRxWindowLease,
        ) -> Result<usize> {
            if !Arc::ptr_eq(&lease.tracker, &self.active_rx_leases) {
                return Err(Error::Protocol("RX lease belongs to another pool".into()));
            }
            for &slot in &lease.slots {
                let (_, _, kind, state) = self.slot_fields(slot)?;
                if kind != SlotKind::Rx || state != SlotState::Leased {
                    return Err(Error::Protocol("RX lease slot state mismatch".into()));
                }
            }
            // FIFO allocation keeps fresh backing ahead of recycled windows
            // and preserves each released window's ascending receive order.
            for &slot in &lease.slots {
                self.transition(slot, SlotState::Leased, SlotState::RecvCompleted)?;
                self.release(slot)?;
            }
            let count = lease.slots.len();
            drop(lease);
            Ok(count)
        }

        fn slot_fields(&self, id: SlotId) -> Result<(usize, usize, SlotKind, SlotState)> {
            self.slots
                .get(id.0)
                .map(|slot| (slot.offset, slot.len, slot.kind, slot.state))
                .ok_or_else(|| Error::InvalidConfiguration("slot id is outside buffer pool".into()))
        }

        fn transition(&mut self, id: SlotId, from: SlotState, to: SlotState) -> Result<()> {
            let slot = self
                .slots
                .get_mut(id.0)
                .ok_or_else(|| Error::Protocol("completion slot is outside buffer pool".into()))?;
            if slot.state != from {
                return Err(Error::Protocol(format!(
                    "slot {} state {:?}, expected {:?}",
                    id.0, slot.state, from
                )));
            }
            slot.state = to;
            Ok(())
        }

        pub(crate) fn stop(&mut self) {
            self.accepting = false;
        }

        pub(crate) fn close(&mut self) -> Result<()> {
            self.stop();
            let active = self.active_rx_leases.load(Ordering::Acquire);
            if active != 0 {
                return Err(Error::InvalidConfiguration(format!(
                    "cannot close registered Segment with {active} active RX leases"
                )));
            }
            let Some(mut segment) = self.segment.take() else {
                return Ok(());
            };
            segment.close()
        }

        pub(crate) fn registered_len(&self) -> usize {
            self.segment.as_ref().map_or(0, |segment| segment.len)
        }

        pub(crate) fn alignment(&self) -> usize {
            self.segment
                .as_ref()
                .map_or(self.config.alignment, |segment| segment.alignment)
        }
    }

    impl Drop for UrmaBufferPool {
        fn drop(&mut self) {
            if self.active_rx_leases.load(Ordering::Acquire) != 0 {
                // An application violated the required shutdown order. Leak
                // the registration rather than free memory still readable by
                // another thread.
                if let Some(segment) = self.segment.take() {
                    std::mem::forget(segment);
                }
            }
        }
    }

    fn slot_offset(config: &BufferPoolConfig, index: usize) -> Result<usize> {
        config
            .slot_size
            .checked_mul(index)
            .ok_or_else(|| Error::InvalidConfiguration("slot offset overflow".into()))
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
pub(crate) use native::PreparedTxBatch;
#[cfg(feature = "urma")]
pub(crate) use native::RegisteredRxWindowLease;
#[cfg(feature = "urma")]
pub use native::UrmaBufferPool;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_fixed_slot_layout_without_urma() {
        let config = BufferPoolConfig {
            slot_size: 1024,
            tx_slot_count: 2,
            rx_slot_count: 3,
            alignment: 4096,
            alias_tx_slots: false,
        };
        assert_eq!(config.total_len(), Ok(5 * 1024));

        let aliased = BufferPoolConfig {
            alias_tx_slots: true,
            ..config
        };
        assert_eq!(aliased.total_len(), Ok(4 * 1024));
    }

    #[test]
    fn rejects_invalid_layout_without_touching_urma() {
        let config = BufferPoolConfig {
            slot_size: 0,
            ..BufferPoolConfig::default()
        };
        assert!(matches!(
            config.total_len(),
            Err(Error::InvalidConfiguration(_))
        ));
    }

    #[test]
    fn rejects_registered_pool_size_overflow() {
        let config = BufferPoolConfig {
            slot_size: usize::MAX,
            tx_slot_count: 1,
            rx_slot_count: 1,
            alignment: 4096,
            alias_tx_slots: false,
        };
        assert!(matches!(
            config.total_len(),
            Err(Error::InvalidConfiguration(_))
        ));
    }

    #[test]
    fn slot_state_snapshot_counts_data_plane_states() {
        let mut snapshot = SlotStateSnapshot::default();
        for state in [
            SlotState::Free,
            SlotState::Allocated,
            SlotState::PostedRecv,
            SlotState::RecvCompleted,
            SlotState::SendPosted,
            SlotState::SendCompleted,
            SlotState::Posted,
        ] {
            snapshot.observe(state);
        }
        assert_eq!(snapshot.free, 1);
        assert_eq!(snapshot.allocated, 1);
        assert_eq!(snapshot.posted_recv, 1);
        assert_eq!(snapshot.recv_completed, 1);
        assert_eq!(snapshot.send_posted, 1);
        assert_eq!(snapshot.send_completed, 1);
        assert_eq!(snapshot.other, 1);
    }
}
