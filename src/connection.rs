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
        buffer::UrmaBufferPool,
        completion::{
            check_deadline, deadline_after, CompletionDiagnostic, CompletionEvent,
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
            self.poller
                .track(user_ctx, OperationType::Recv, wr, sequence)?;
            self.receive_credit.posted();
            Ok(())
        }

        pub fn send(&mut self, message: &Message) -> Result<()> {
            self.send_frame(&message.encode()?)
        }

        /// Post one encoded message without imposing a completion drain.
        pub fn send_frame(&mut self, bytes: &[u8]) -> Result<()> {
            self.send_frame_with_sequence(bytes, None)
        }

        pub fn send_frame_tracked(&mut self, bytes: &[u8], sequence: u64) -> Result<()> {
            self.send_frame_with_sequence(bytes, Some(sequence))
        }

        fn send_frame_with_sequence(&mut self, bytes: &[u8], sequence: Option<u64>) -> Result<()> {
            self.require(ConnectionState::Ready)?;
            self.receive_credit.require_before_send()?;
            let slot = self
                .buffer_pool
                .allocate(SlotKind::Tx)
                .ok_or_else(|| Error::InvalidConfiguration("no free TX slot".into()))?;
            let (offset, length) = match self.buffer_pool.write_tx(slot, bytes) {
                Ok(layout) => layout,
                Err(error) => {
                    self.buffer_pool.release(slot)?;
                    return Err(error);
                }
            };
            let token = WrToken {
                connection_id: self.poller.connection_id(),
                generation: self.poller.generation(),
                operation: OperationType::Send,
                slot,
            };
            let user_ctx = token.encode()?;
            self.buffer_pool.mark_posted(slot, SlotKind::Tx)?;
            let result =
                self.jetty
                    .post_send(self.buffer_pool.segment_handle()?, offset, length, user_ctx);
            let wr = match result {
                Ok(wr) => wr,
                Err(error) => {
                    self.buffer_pool.rollback_post(slot, SlotKind::Tx)?;
                    self.buffer_pool.release(slot)?;
                    return Err(error);
                }
            };
            self.poller
                .track(user_ctx, OperationType::Send, wr, sequence)
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
