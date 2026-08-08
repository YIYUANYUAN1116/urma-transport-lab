use crate::{Error, Result};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BufferPoolConfig {
    pub slot_size: usize,
    pub tx_slot_count: usize,
    pub rx_slot_count: usize,
    pub alignment: usize,
}

impl Default for BufferPoolConfig {
    fn default() -> Self {
        Self {
            slot_size: 64 * 1024,
            tx_slot_count: 8,
            rx_slot_count: 8,
            alignment: 4096,
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
        let slots = self
            .tx_slot_count
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
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct SlotId(usize);

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
        accepting: bool,
    }

    impl UrmaBufferPool {
        pub(crate) fn create(
            runtime: &mut ffi::NativeRuntime,
            config: BufferPoolConfig,
        ) -> Result<Self> {
            let total_len = config.total_len()?;
            let segment = UrmaRegisteredSegment::create(runtime, total_len, config.alignment)?;
            let mut slots = Vec::with_capacity(config.tx_slot_count + config.rx_slot_count);
            for index in 0..config.tx_slot_count {
                slots.push(BufferSlot {
                    kind: SlotKind::Tx,
                    offset: index * config.slot_size,
                    len: config.slot_size,
                    state: SlotState::Free,
                });
            }
            for index in 0..config.rx_slot_count {
                slots.push(BufferSlot {
                    kind: SlotKind::Rx,
                    offset: (config.tx_slot_count + index) * config.slot_size,
                    len: config.slot_size,
                    state: SlotState::Free,
                });
            }
            Ok(Self {
                config,
                segment: Some(segment),
                slots,
                accepting: true,
            })
        }

        pub fn config(&self) -> &BufferPoolConfig {
            &self.config
        }

        pub fn allocate(&mut self, kind: SlotKind) -> Option<SlotId> {
            if !self.accepting {
                return None;
            }
            let (index, slot) = self
                .slots
                .iter_mut()
                .enumerate()
                .find(|(_, slot)| slot.kind == kind && slot.state == SlotState::Free)?;
            slot.state = SlotState::Allocated;
            Some(SlotId(index))
        }

        pub fn release(&mut self, id: SlotId) -> Result<()> {
            let slot = self.slots.get_mut(id.0).ok_or_else(|| {
                Error::InvalidConfiguration("slot id is outside this buffer pool".into())
            })?;
            if slot.state != SlotState::Allocated {
                return Err(Error::InvalidConfiguration(
                    "only an allocated slot can be released in M1".into(),
                ));
            }
            slot.state = SlotState::Free;
            Ok(())
        }

        pub fn slot_state(&self, id: SlotId) -> Option<SlotState> {
            self.slots.get(id.0).map(|slot| slot.state)
        }

        pub fn slot_layout(&self, id: SlotId) -> Option<(usize, usize)> {
            self.slots.get(id.0).map(|slot| (slot.offset, slot.len))
        }

        pub(crate) fn stop(&mut self) {
            self.accepting = false;
        }

        pub(crate) fn close(&mut self) -> Result<()> {
            self.stop();
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

    fn map_ffi_error(operation: &'static str, error: ffi::FfiError) -> Error {
        match error {
            ffi::FfiError::Contract(detail) => Error::FfiContract { operation, detail },
            ffi::FfiError::NullHandle => Error::NullHandle { operation },
            ffi::FfiError::Status(status) => Error::Native { operation, status },
        }
    }
}

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
        };
        assert_eq!(config.total_len(), Ok(5 * 1024));
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
}
