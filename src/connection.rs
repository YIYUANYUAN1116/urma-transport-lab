#[cfg(feature = "urma")]
use crate::{Error, Result};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectionState {
    Init,
    ContextReady,
    JettyCreated,
    DescriptorExchanged,
    Bound,
    Ready,
    Draining,
    Failed,
    Closed,
}

#[cfg(feature = "urma")]
mod native {
    use super::*;
    use crate::{
        buffer::RegisteredRxWindowLease,
        buffer::UrmaBufferPool,
        completion::{
            check_deadline, deadline_after, CompletedRecv, CompletionDiagnostic, CompletionEvent,
            CompletionPoller, CompletionStats,
        },
        ffi,
        jetty::UrmaJetty,
        message::Message,
        wr::{OperationType, ReceiveCredit, WrToken},
        JettyDescriptor, SlotKind, SlotStateSnapshot, UrmaDeviceCapability,
    };
    use std::{collections::VecDeque, time::Duration};

    /// M2 control-plane owner. It intentionally exposes no data-plane API.
    pub struct UrmaConnection<'runtime> {
        state: ConnectionState,
        capability: UrmaDeviceCapability,
        jetty: UrmaJetty,
        buffer_pool: &'runtime mut UrmaBufferPool,
        send_jfc: &'runtime ffi::JfcHandle,
        recv_jfc: &'runtime ffi::JfcHandle,
        jfce: &'runtime ffi::JfceHandle,
        poller: CompletionPoller,
        receive_credit: ReceiveCredit,
        pending_frames: VecDeque<Vec<u8>>,
        send_completion_interval: usize,
        sends_since_completion: usize,
    }

    impl<'runtime> UrmaConnection<'runtime> {
        pub(crate) fn new(
            capability: UrmaDeviceCapability,
            jetty: UrmaJetty,
            buffer_pool: &'runtime mut UrmaBufferPool,
            send_jfc: &'runtime ffi::JfcHandle,
            recv_jfc: &'runtime ffi::JfcHandle,
            jfce: &'runtime ffi::JfceHandle,
            connection_id: u16,
            generation: u8,
        ) -> Result<Self> {
            let mut connection = Self {
                state: ConnectionState::ContextReady,
                capability,
                jetty,
                buffer_pool,
                send_jfc,
                recv_jfc,
                jfce,
                poller: CompletionPoller::new(connection_id, generation, 16)?,
                receive_credit: ReceiveCredit::default(),
                pending_frames: VecDeque::new(),
                send_completion_interval: 1,
                sends_since_completion: 0,
            };
            connection.transition(ConnectionState::JettyCreated);
            Ok(connection)
        }

        pub fn state(&self) -> ConnectionState {
            self.state
        }

        pub fn capability(&self) -> &UrmaDeviceCapability {
            &self.capability
        }

        pub(crate) fn export_descriptor(&mut self) -> Result<JettyDescriptor> {
            if !matches!(
                self.state,
                ConnectionState::JettyCreated | ConnectionState::DescriptorExchanged
            ) {
                return Err(self.state_error("export descriptor"));
            }
            let descriptor = self.jetty.export_descriptor()?;
            if self.state == ConnectionState::JettyCreated {
                self.transition(ConnectionState::DescriptorExchanged);
            }
            Ok(descriptor)
        }

        pub(crate) fn import_and_bind(&mut self, descriptor: &JettyDescriptor) -> Result<()> {
            if !matches!(
                self.state,
                ConnectionState::JettyCreated | ConnectionState::DescriptorExchanged
            ) {
                return Err(self.state_error("import descriptor"));
            }
            let local_transport = u32::try_from(self.capability.transport_type)
                .map_err(|_| Error::Protocol("local transport type is negative".into()))?;
            if descriptor.transport_type != local_transport {
                return Err(Error::Protocol(format!(
                    "remote transport type {} does not match local {}",
                    descriptor.transport_type, local_transport
                )));
            }
            if self.state == ConnectionState::JettyCreated {
                self.transition(ConnectionState::DescriptorExchanged);
            }
            self.jetty.import(descriptor)?;
            self.jetty.bind()?;
            self.transition(ConnectionState::Bound);
            Ok(())
        }

        pub(crate) fn mark_ready(&mut self) -> Result<()> {
            self.require(ConnectionState::Bound)?;
            self.transition(ConnectionState::Ready);
            Ok(())
        }

        pub fn recv_ready(&mut self) -> Result<()> {
            self.recv_ready_with_sequence(None)
        }

        pub fn recv_ready_tracked(&mut self, sequence: u64) -> Result<()> {
            self.recv_ready_with_sequence(Some(sequence))
        }

        pub(crate) fn recv_ready_tracked_batch(
            &mut self,
            first_sequence: u64,
            count: usize,
        ) -> Result<usize> {
            if count == 0 {
                return Ok(0);
            }
            if !matches!(self.state, ConnectionState::Bound | ConnectionState::Ready) {
                return Err(self.state_error("post receive batch"));
            }
            let last = u64::try_from(count - 1)
                .map_err(|_| Error::Protocol("RECV batch length exceeds u64".into()))?;
            if first_sequence.checked_add(last).is_none() {
                return Err(Error::Protocol("RECV sequence overflow".into()));
            }

            let mut entries = Vec::with_capacity(count);
            for _ in 0..count {
                let Some(slot) = self.buffer_pool.allocate(SlotKind::Rx) else {
                    for (slot, _, _) in entries {
                        self.buffer_pool.release(slot)?;
                    }
                    return Err(Error::InvalidConfiguration("no free RX slot".into()));
                };
                let (offset, length) = match self.buffer_pool.recv_post_layout(slot) {
                    Ok(layout) => layout,
                    Err(error) => {
                        self.buffer_pool.release(slot)?;
                        for (slot, _, _) in entries {
                            self.buffer_pool.release(slot)?;
                        }
                        return Err(error);
                    }
                };
                entries.push((slot, offset, length));
            }

            let mut descriptors = Vec::with_capacity(count);
            for (index, &(slot, offset, length)) in entries.iter().enumerate() {
                let token = WrToken {
                    connection_id: self.poller.connection_id(),
                    generation: self.poller.generation(),
                    operation: OperationType::Recv,
                    slot,
                };
                let user_ctx = match token.encode() {
                    Ok(value) => value,
                    Err(error) => {
                        for &(rollback, _, _) in entries.iter().take(index) {
                            self.buffer_pool.rollback_post(rollback, SlotKind::Rx)?;
                        }
                        for &(slot, _, _) in &entries {
                            self.buffer_pool.release(slot)?;
                        }
                        return Err(error);
                    }
                };
                if let Err(error) = self.buffer_pool.mark_posted(slot, SlotKind::Rx) {
                    for &(rollback, _, _) in entries.iter().take(index) {
                        self.buffer_pool.rollback_post(rollback, SlotKind::Rx)?;
                    }
                    for &(release, _, _) in &entries {
                        self.buffer_pool.release(release)?;
                    }
                    return Err(error);
                }
                descriptors.push(crate::ffi::WrDescriptor {
                    offset,
                    length,
                    user_ctx,
                    complete_enable: true,
                });
            }

            let posted_result = match self.buffer_pool.segment_handle() {
                Ok(segment) => self.jetty.post_recv_batch(segment, &descriptors),
                Err(error) => {
                    for &(slot, _, _) in &entries {
                        self.buffer_pool.rollback_post(slot, SlotKind::Rx)?;
                        self.buffer_pool.release(slot)?;
                    }
                    return Err(error);
                }
            };
            let posted = match posted_result {
                Ok(posted) => posted,
                Err(error) => {
                    for &(slot, _, _) in &entries {
                        self.buffer_pool.rollback_post(slot, SlotKind::Rx)?;
                        self.buffer_pool.release(slot)?;
                    }
                    return Err(error);
                }
            };
            let posted_count = posted.handles.len();
            self.poller
                .record_post_call(OperationType::Recv, descriptors.len());
            for (index, handle) in posted.handles.into_iter().enumerate() {
                self.poller.track(
                    descriptors[index].user_ctx,
                    OperationType::Recv,
                    handle,
                    Some(first_sequence + index as u64),
                    true,
                )?;
                self.receive_credit.posted();
            }
            for &(slot, _, _) in entries.iter().skip(posted_count) {
                self.buffer_pool.rollback_post(slot, SlotKind::Rx)?;
                self.buffer_pool.release(slot)?;
            }
            if posted.status != 0 {
                return Err(Error::Native {
                    operation: "post_jetty_recv_wr_batch",
                    status: posted.status,
                });
            }
            if posted_count != count {
                return Err(Error::Protocol(
                    "successful RECV batch did not post every WR".into(),
                ));
            }
            Ok(posted_count)
        }

        fn recv_ready_with_sequence(&mut self, sequence: Option<u64>) -> Result<()> {
            if !matches!(self.state, ConnectionState::Bound | ConnectionState::Ready) {
                return Err(self.state_error("post receive"));
            }
            let slot = self
                .buffer_pool
                .allocate(SlotKind::Rx)
                .ok_or_else(|| Error::InvalidConfiguration("no free RX slot".into()))?;
            let (offset, length) = self.buffer_pool.recv_post_layout(slot)?;
            let token = WrToken {
                connection_id: self.poller.connection_id(),
                generation: self.poller.generation(),
                operation: OperationType::Recv,
                slot,
            };
            let user_ctx = token.encode()?;
            self.buffer_pool.mark_posted(slot, SlotKind::Rx)?;
            let result =
                self.jetty
                    .post_recv(self.buffer_pool.segment_handle()?, offset, length, user_ctx);
            let wr = match result {
                Ok(wr) => wr,
                Err(error) => {
                    self.buffer_pool.rollback_post(slot, SlotKind::Rx)?;
                    self.buffer_pool.release(slot)?;
                    return Err(error);
                }
            };
            self.poller.record_post_call(OperationType::Recv, 1);
            self.poller
                .track(user_ctx, OperationType::Recv, wr, sequence, true)?;
            self.receive_credit.posted();
            Ok(())
        }

        pub fn send(&mut self, message: &Message) -> Result<()> {
            self.send_frame(&message.encode()?)
        }

        /// Post one encoded message without imposing a completion drain.
        pub fn send_frame(&mut self, bytes: &[u8]) -> Result<()> {
            self.send_frame_with_sequence(bytes, None, true, None)
        }

        pub fn send_frame_tracked(&mut self, bytes: &[u8], sequence: u64) -> Result<()> {
            self.send_frame_with_sequence(bytes, Some(sequence), false, None)
        }

        pub fn send_frame_tracked_tail(&mut self, bytes: &[u8], sequence: u64) -> Result<()> {
            self.send_frame_with_sequence(bytes, Some(sequence), true, None)
        }

        pub fn prepare_aliased_tx(&mut self, bytes: &[u8]) -> Result<()> {
            self.require(ConnectionState::Ready)?;
            self.buffer_pool.prepare_aliased_tx(bytes)
        }

        pub fn send_prepared_tracked(
            &mut self,
            length: usize,
            sequence: u64,
            is_tail: bool,
        ) -> Result<()> {
            self.send_frame_with_sequence(&[], Some(sequence), is_tail, Some(length))
        }

        pub(crate) fn prepare_filled_batch(
            &mut self,
            lengths: &[usize],
            fill: impl FnOnce(&mut [u8]) -> Result<()>,
        ) -> Result<crate::buffer::PreparedTxBatch> {
            self.require(ConnectionState::Ready)?;
            self.receive_credit.require_before_send()?;
            self.buffer_pool.prepare_tx_batch(lengths, fill)
        }

        pub(crate) fn discard_prepared_batch(
            &mut self,
            batch: crate::buffer::PreparedTxBatch,
        ) -> Result<()> {
            self.buffer_pool.discard_tx_batch(batch)
        }

        pub(crate) fn post_prepared_batch_tracked(
            &mut self,
            batch: crate::buffer::PreparedTxBatch,
            first_sequence: u64,
        ) -> Result<usize> {
            if let Err(error) = self.require(ConnectionState::Ready) {
                self.buffer_pool.discard_tx_batch(batch)?;
                return Err(error);
            }
            let batch_len = batch.len();
            let last_offset = match u64::try_from(batch_len.saturating_sub(1)) {
                Ok(offset) => offset,
                Err(_) => {
                    self.buffer_pool.discard_tx_batch(batch)?;
                    return Err(Error::Protocol("SEND batch length exceeds u64".into()));
                }
            };
            if first_sequence.checked_add(last_offset).is_none() {
                self.buffer_pool.discard_tx_batch(batch)?;
                return Err(Error::Protocol("SEND sequence overflow".into()));
            }
            let layouts = batch.layouts;
            let mut descriptors = Vec::with_capacity(layouts.len());
            let mut sends_since_completion = self.sends_since_completion;
            for (index, &(slot, offset, length)) in layouts.iter().enumerate() {
                // A batch is drained before its registered range may be
                // filled again, so its final WR must always establish a
                // completion frontier even when moderation does not divide
                // the batch length.
                let complete_enable = index + 1 == layouts.len()
                    || sends_since_completion + 1 >= self.send_completion_interval;
                if complete_enable {
                    sends_since_completion = 0;
                } else {
                    sends_since_completion += 1;
                }
                let token = WrToken {
                    connection_id: self.poller.connection_id(),
                    generation: self.poller.generation(),
                    operation: OperationType::Send,
                    slot,
                };
                let user_ctx = match token.encode() {
                    Ok(value) => value,
                    Err(error) => {
                        for &(rollback, _, _) in layouts.iter().take(index) {
                            self.buffer_pool.rollback_post(rollback, SlotKind::Tx)?;
                        }
                        for &(release, _, _) in &layouts {
                            self.buffer_pool.release(release)?;
                        }
                        return Err(error);
                    }
                };
                if let Err(error) = self.buffer_pool.mark_posted(slot, SlotKind::Tx) {
                    for &(rollback, _, _) in layouts.iter().take(index) {
                        self.buffer_pool.rollback_post(rollback, SlotKind::Tx)?;
                    }
                    for &(release, _, _) in &layouts {
                        self.buffer_pool.release(release)?;
                    }
                    return Err(error);
                }
                descriptors.push(crate::ffi::WrDescriptor {
                    offset,
                    length,
                    user_ctx,
                    complete_enable,
                });
            }
            let posted_result = match self.buffer_pool.segment_handle() {
                Ok(segment) => self.jetty.post_send_batch(segment, &descriptors),
                Err(error) => {
                    for &(slot, _, _) in &layouts {
                        self.buffer_pool.rollback_post(slot, SlotKind::Tx)?;
                        self.buffer_pool.release(slot)?;
                    }
                    return Err(error);
                }
            };
            let posted = match posted_result {
                Ok(posted) => posted,
                Err(error) => {
                    for &(slot, _, _) in &layouts {
                        self.buffer_pool.rollback_post(slot, SlotKind::Tx)?;
                        self.buffer_pool.release(slot)?;
                    }
                    return Err(error);
                }
            };
            let posted_count = posted.handles.len();
            self.poller
                .record_post_call(OperationType::Send, descriptors.len());
            for (index, handle) in posted.handles.into_iter().enumerate() {
                self.poller.track(
                    descriptors[index].user_ctx,
                    OperationType::Send,
                    handle,
                    Some(first_sequence + index as u64),
                    descriptors[index].complete_enable,
                )?;
                if descriptors[index].complete_enable {
                    self.sends_since_completion = 0;
                } else {
                    self.sends_since_completion += 1;
                }
            }
            for &(slot, _, _) in layouts.iter().skip(posted_count) {
                self.buffer_pool.rollback_post(slot, SlotKind::Tx)?;
                self.buffer_pool.release(slot)?;
            }
            if posted.status != 0 {
                return Err(Error::Native {
                    operation: "post_jetty_send_wr_batch",
                    status: posted.status,
                });
            }
            if posted_count != layouts.len() {
                return Err(Error::Protocol(
                    "successful SEND batch did not post every WR".into(),
                ));
            }
            Ok(posted_count)
        }

        pub fn configure_send_completion_interval(&mut self, interval: usize) -> Result<()> {
            if interval == 0 || self.poller.outstanding_send() != 0 {
                return Err(Error::InvalidConfiguration(
                    "completion interval must be non-zero and changed only with an empty SQ".into(),
                ));
            }
            self.send_completion_interval = interval;
            self.sends_since_completion = 0;
            self.poller
                .reserve_send(self.buffer_pool.config().tx_slot_count);
            Ok(())
        }

        fn send_frame_with_sequence(
            &mut self,
            bytes: &[u8],
            sequence: Option<u64>,
            force_completion: bool,
            prepared_length: Option<usize>,
        ) -> Result<()> {
            self.require(ConnectionState::Ready)?;
            self.receive_credit.require_before_send()?;
            let complete_enable = force_completion
                || self.sends_since_completion + 1 >= self.send_completion_interval;
            let slot = self
                .buffer_pool
                .allocate(SlotKind::Tx)
                .ok_or_else(|| Error::InvalidConfiguration("no free TX slot".into()))?;
            let layout = if let Some(length) = prepared_length {
                self.buffer_pool.aliased_tx_layout(slot, length)
            } else {
                self.buffer_pool.write_tx(slot, bytes)
            };
            let (offset, length) = match layout {
                Ok(layout) => layout,
                Err(error) => {
                    self.buffer_pool.release(slot)?;
                    return Err(error);
                }
            };
            self.post_allocated_send(slot, offset, length, sequence, complete_enable)
        }

        fn post_allocated_send(
            &mut self,
            slot: crate::SlotId,
            offset: u64,
            length: u32,
            sequence: Option<u64>,
            complete_enable: bool,
        ) -> Result<()> {
            let token = WrToken {
                connection_id: self.poller.connection_id(),
                generation: self.poller.generation(),
                operation: OperationType::Send,
                slot,
            };
            let user_ctx = match token.encode() {
                Ok(user_ctx) => user_ctx,
                Err(error) => {
                    self.buffer_pool.release(slot)?;
                    return Err(error);
                }
            };
            if let Err(error) = self.buffer_pool.mark_posted(slot, SlotKind::Tx) {
                self.buffer_pool.release(slot)?;
                return Err(error);
            }
            let result = match self.buffer_pool.segment_handle() {
                Ok(segment) => {
                    self.jetty
                        .post_send(segment, offset, length, user_ctx, complete_enable)
                }
                Err(error) => {
                    self.buffer_pool.rollback_post(slot, SlotKind::Tx)?;
                    self.buffer_pool.release(slot)?;
                    return Err(error);
                }
            };
            let wr = match result {
                Ok(wr) => wr,
                Err(error) => {
                    self.buffer_pool.rollback_post(slot, SlotKind::Tx)?;
                    self.buffer_pool.release(slot)?;
                    return Err(error);
                }
            };
            self.poller.record_post_call(OperationType::Send, 1);
            self.poller
                .track(user_ctx, OperationType::Send, wr, sequence, complete_enable)?;
            if complete_enable {
                self.sends_since_completion = 0;
            } else {
                self.sends_since_completion += 1;
            }
            Ok(())
        }

        pub fn poll_once(&mut self) -> Result<Vec<CompletionEvent>> {
            self.require(ConnectionState::Ready)?;
            let events = match self.poller.poll_once(
                self.send_jfc,
                self.recv_jfc,
                self.jfce,
                self.buffer_pool,
            ) {
                Ok(events) => events,
                Err(error) => {
                    self.fail();
                    return Err(error);
                }
            };
            for event in &events {
                if matches!(event, CompletionEvent::RecvCompleted { .. }) {
                    self.receive_credit.completed();
                }
            }
            Ok(events)
        }

        pub fn poll_recv_direct(
            &mut self,
            consume: impl FnMut(Option<u64>, &[u8]) -> Result<()>,
        ) -> Result<usize> {
            self.require(ConnectionState::Ready)?;
            let count = match self
                .poller
                .poll_recv_direct(self.recv_jfc, self.buffer_pool, consume)
            {
                Ok(count) => count,
                Err(error) => {
                    self.fail();
                    return Err(error);
                }
            };
            for _ in 0..count {
                self.receive_credit.completed();
            }
            Ok(count)
        }

        pub(crate) fn poll_recv_leased(&mut self) -> Result<Vec<CompletedRecv>> {
            self.require(ConnectionState::Ready)?;
            let completed = match self
                .poller
                .poll_recv_leased(self.recv_jfc, self.buffer_pool)
            {
                Ok(completed) => completed,
                Err(error) => {
                    self.fail();
                    return Err(error);
                }
            };
            for _ in 0..completed.len() {
                self.receive_credit.completed();
            }
            Ok(completed)
        }

        pub(crate) fn lease_completed_recvs(
            &mut self,
            completed: &[CompletedRecv],
        ) -> Result<RegisteredRxWindowLease> {
            let records = completed
                .iter()
                .map(|completion| (completion.slot, completion.sequence, completion.length))
                .collect::<Vec<_>>();
            self.buffer_pool.lease_completed_recv_window(&records)
        }

        pub(crate) fn recycle_recv_lease(
            &mut self,
            lease: RegisteredRxWindowLease,
        ) -> Result<usize> {
            self.buffer_pool.recycle_recv_lease(lease)
        }

        pub fn wait_for_message(&mut self, timeout: Duration) -> Result<Message> {
            Message::decode(&self.wait_for_frame(timeout)?)
        }

        pub fn wait_for_frame(&mut self, timeout: Duration) -> Result<Vec<u8>> {
            let deadline = deadline_after(timeout);
            loop {
                if self.poller.outstanding_send() == 0 {
                    if let Some(frame) = self.pending_frames.pop_front() {
                        return Ok(frame);
                    }
                }
                check_deadline(deadline, "wait_for_frame")?;
                for event in self.poll_once()? {
                    if let CompletionEvent::RecvCompleted { bytes, .. } = event {
                        self.pending_frames.push_back(bytes);
                    }
                }
            }
        }

        pub fn drain_completions(&mut self, timeout: Duration) -> Result<()> {
            let deadline = deadline_after(timeout);
            while self.poller.outstanding_send() != 0 {
                check_deadline(deadline, "drain completions")?;
                for event in self.poll_once()? {
                    if let CompletionEvent::RecvCompleted { bytes, .. } = event {
                        self.pending_frames.push_back(bytes);
                    }
                }
            }
            Ok(())
        }

        pub fn stats(&self) -> CompletionStats {
            self.poller.stats()
        }

        pub fn outstanding_send(&self) -> usize {
            self.poller.outstanding_send()
        }

        pub fn outstanding_recv(&self) -> usize {
            self.poller.outstanding_recv()
        }

        pub fn receive_credit(&self) -> usize {
            self.receive_credit.current()
        }

        pub fn tx_slot_state_snapshot(&self) -> SlotStateSnapshot {
            self.buffer_pool.slot_state_snapshot(SlotKind::Tx)
        }

        pub fn rx_slot_state_snapshot(&self) -> SlotStateSnapshot {
            self.buffer_pool.slot_state_snapshot(SlotKind::Rx)
        }

        pub fn pending_send_diagnostic(&self) -> CompletionDiagnostic {
            self.poller
                .diagnostic(OperationType::Send, self.buffer_pool)
        }

        pub fn pending_recv_diagnostic(&self) -> CompletionDiagnostic {
            self.poller
                .diagnostic(OperationType::Recv, self.buffer_pool)
        }

        pub(crate) fn fail(&mut self) {
            if self.state != ConnectionState::Closed {
                self.transition(ConnectionState::Failed);
            }
        }

        pub fn close(mut self) -> Result<()> {
            self.close_inner()
        }

        fn close_inner(&mut self) -> Result<()> {
            if self.state == ConnectionState::Closed {
                return Ok(());
            }
            if self.poller.outstanding() != 0 {
                self.transition(ConnectionState::Draining);
                let mut failures = Vec::new();
                if let Err(error) = self.jetty.mark_error() {
                    failures.push(error.to_string());
                }
                let deadline = deadline_after(Duration::from_secs(1));
                while self.poller.outstanding() != 0 && check_deadline(deadline, "drain WR").is_ok()
                {
                    match self.poller.poll_once(
                        self.send_jfc,
                        self.recv_jfc,
                        self.jfce,
                        self.buffer_pool,
                    ) {
                        Ok(_) => {}
                        Err(Error::Completion { .. }) => {}
                        Err(error) => failures.push(error.to_string()),
                    }
                }
                if self.poller.outstanding() != 0 {
                    failures.push("timed out draining outstanding WRs".into());
                }
                if !failures.is_empty() {
                    self.transition(ConnectionState::Failed);
                    return Err(Error::Shutdown { failures });
                }
            }
            let result = self.jetty.close();
            if result.is_ok() {
                self.transition(ConnectionState::Closed);
            } else {
                self.transition(ConnectionState::Failed);
            }
            result
        }

        fn require(&self, expected: ConnectionState) -> Result<()> {
            if self.state == expected {
                Ok(())
            } else {
                Err(Error::Protocol(format!(
                    "connection state {:?}, expected {:?}",
                    self.state, expected
                )))
            }
        }

        fn state_error(&self, operation: &str) -> Error {
            Error::Protocol(format!(
                "cannot {operation} while connection is {:?}",
                self.state
            ))
        }

        fn transition(&mut self, state: ConnectionState) {
            eprintln!("M2 connection: {:?} -> {state:?}", self.state);
            self.state = state;
        }
    }

    impl Drop for UrmaConnection<'_> {
        fn drop(&mut self) {
            let _ = self.close_inner();
        }
    }
}

#[cfg(feature = "urma")]
pub use native::UrmaConnection;
