use crate::{Error, Result, SlotId, SlotState};
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CompletionStats {
    pub send_post: u64,
    pub recv_post: u64,
    /// Hardware SEND completion records consumed from the JFC.
    pub send_cqe: u64,
    /// Logical SEND WRs retired by those ordered completion frontiers.
    pub send_retired: u64,
    pub recv_cqe: u64,
    pub cqe_error: u64,
    pub poll_calls: u64,
    pub empty_polls: u64,
    pub send_jfc_poll_calls: u64,
    pub recv_jfc_poll_calls: u64,
    pub yield_count: u64,
    pub sleep_count: u64,
    pub backoff_sleep_ns: u64,
    pub jfc_rearm_count: u64,
    pub event_wait_count: u64,
    pub event_wakeup_count: u64,
    pub event_timeout_count: u64,
    pub spurious_wakeup_count: u64,
    pub event_wait_ns: u64,
    pub max_event_wait_ns: u64,
    pub max_empty_streak: u64,
    pub nonempty_polls: u64,
    pub completion_batch_total: u64,
    pub max_completion_poll_gap_ns: u64,
    pub max_outstanding_send: u64,
}

#[cfg(any(feature = "urma", test))]
const HOT_POLL_LIMIT: u64 = u64::MAX;
#[cfg(any(feature = "urma", test))]
const EVENT_WAIT_TIMEOUT_MS: i32 = 10;

#[cfg(any(feature = "urma", test))]
fn should_wait_for_event(streak: u64) -> bool {
    streak > HOT_POLL_LIMIT
}

#[cfg(any(feature = "urma", test))]
fn duration_ns_saturating(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CompletionEvent {
    SendCompleted { slot: SlotId },
    RecvCompleted { slot: SlotId, bytes: Vec<u8> },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PendingWrSnapshot {
    pub sequence: Option<u64>,
    pub slot: SlotId,
    pub state: SlotState,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CompletionDiagnostic {
    pub pending: Vec<PendingWrSnapshot>,
    pub last_completed_sequence: Option<u64>,
}

pub fn check_deadline(deadline: Instant, operation: &'static str) -> Result<()> {
    if Instant::now() >= deadline {
        Err(Error::Timeout { operation })
    } else {
        Ok(())
    }
}

pub fn deadline_after(timeout: Duration) -> Instant {
    Instant::now() + timeout
}

pub fn validate_completion_status(status: i32, opcode: u32, user_ctx: u64) -> Result<()> {
    if status == 0 {
        Ok(())
    } else {
        Err(Error::Completion {
            status,
            opcode,
            user_ctx,
        })
    }
}

#[cfg(feature = "urma")]
mod native {
    use super::*;
    use crate::{
        buffer::UrmaBufferPool,
        ffi,
        wr::{OperationType, WrToken},
    };
    use std::collections::VecDeque;

    struct OutstandingWr {
        user_ctx: u64,
        handle: ffi::WrHandle,
        sequence: Option<u64>,
        signaled: bool,
    }

    pub(crate) struct CompletionPoller {
        connection_id: u16,
        generation: u8,
        batch: usize,
        outstanding: Vec<Option<OutstandingWr>>,
        outstanding_total: usize,
        send_order: VecDeque<u64>,
        outstanding_send: usize,
        outstanding_recv: usize,
        last_completed_send_sequence: Option<u64>,
        last_completed_recv_sequence: Option<u64>,
        empty_streak: u64,
        last_poll_started: Option<Instant>,
        send_armed: bool,
        recv_armed: bool,
        stats: CompletionStats,
    }

    impl CompletionPoller {
        pub(crate) fn new(connection_id: u16, generation: u8, batch: usize) -> Result<Self> {
            if batch == 0 || batch > 16 {
                return Err(Error::InvalidConfiguration(
                    "completion poll batch must be in 1..=16".into(),
                ));
            }
            Ok(Self {
                connection_id,
                generation,
                batch,
                outstanding: Vec::new(),
                outstanding_total: 0,
                send_order: VecDeque::new(),
                outstanding_send: 0,
                outstanding_recv: 0,
                last_completed_send_sequence: None,
                last_completed_recv_sequence: None,
                empty_streak: 0,
                last_poll_started: None,
                send_armed: false,
                recv_armed: false,
                stats: CompletionStats::default(),
            })
        }

        pub(crate) fn track(
            &mut self,
            user_ctx: u64,
            operation: OperationType,
            wr: ffi::WrHandle,
            sequence: Option<u64>,
            signaled: bool,
        ) -> Result<()> {
            let token = WrToken::decode(user_ctx)?;
            let slot = token.slot.index();
            if self.outstanding.len() <= slot {
                self.outstanding.resize_with(slot + 1, || None);
            }
            if self.outstanding[slot].is_some() {
                return Err(Error::Protocol("duplicate outstanding user_ctx".into()));
            }
            self.outstanding[slot] = Some(OutstandingWr {
                user_ctx,
                handle: wr,
                sequence,
                signaled,
            });
            self.outstanding_total += 1;
            match operation {
                OperationType::Send => {
                    self.send_order.push_back(user_ctx);
                    self.stats.send_post += 1;
                    self.outstanding_send += 1;
                    self.stats.max_outstanding_send = self
                        .stats
                        .max_outstanding_send
                        .max(self.outstanding_send as u64);
                }
                OperationType::Recv => {
                    self.stats.recv_post += 1;
                    self.outstanding_recv += 1;
                }
            }
            Ok(())
        }

        pub(crate) fn poll_once(
            &mut self,
            send_jfc: &ffi::JfcHandle,
            recv_jfc: &ffi::JfcHandle,
            jfce: &ffi::JfceHandle,
            pool: &mut UrmaBufferPool,
        ) -> Result<Vec<CompletionEvent>> {
            let poll_started = Instant::now();
            let previous_poll_started = self.last_poll_started.replace(poll_started);
            self.stats.poll_calls += 1;
            let mut events = self.poll_active(send_jfc, recv_jfc, pool)?;
            if !events.is_empty() {
                self.record_nonempty(events.len(), previous_poll_started);
                return Ok(events);
            }

            self.empty_streak = self.empty_streak.saturating_add(1);
            self.stats.max_empty_streak = self.stats.max_empty_streak.max(self.empty_streak);
            if !should_wait_for_event(self.empty_streak)
                || self.outstanding_send + self.outstanding_recv == 0
            {
                self.stats.empty_polls += 1;
                std::hint::spin_loop();
                return Ok(events);
            }

            let newly_armed = self.rearm_active(send_jfc, recv_jfc)?;
            if newly_armed {
                // Close the arm race: a CQE may have arrived after the first
                // empty poll and before its JFC was armed.
                events = self.poll_active(send_jfc, recv_jfc, pool)?;
                if !events.is_empty() {
                    self.record_nonempty(events.len(), previous_poll_started);
                    return Ok(events);
                }
            }

            self.stats.event_wait_count += 1;
            let wait_started = Instant::now();
            let ready = jfce
                .wait(send_jfc, recv_jfc, EVENT_WAIT_TIMEOUT_MS)
                .map_err(|error| map_ffi_error("wait_jfce", error))?;
            let wait_elapsed = duration_ns_saturating(wait_started.elapsed());
            self.stats.event_wait_ns = self.stats.event_wait_ns.saturating_add(wait_elapsed);
            self.stats.max_event_wait_ns = self.stats.max_event_wait_ns.max(wait_elapsed);

            let Some(ready) = ready else {
                self.stats.event_timeout_count += 1;
                self.stats.empty_polls += 1;
                return Ok(events);
            };
            self.stats.event_wakeup_count += 1;
            if ready.send {
                self.send_armed = false;
            }
            if ready.recv {
                self.recv_armed = false;
            }

            let poll_result = self.poll_active(send_jfc, recv_jfc, pool);
            // Follow the UMDK event contract: consume CQEs before acknowledging
            // the event, then rearm. Ack is still attempted if CQ routing fails
            // so JFC deletion cannot be left waiting on an event reference.
            jfce.ack()
                .map_err(|error| map_ffi_error("ack_jfce", error))?;
            events = poll_result?;
            let rearmed = self.rearm_active(send_jfc, recv_jfc)?;
            if rearmed {
                // Also close the race after acknowledging an event and
                // rearming its JFC.
                events.extend(self.poll_active(send_jfc, recv_jfc, pool)?);
            }
            if events.is_empty() {
                self.stats.spurious_wakeup_count += 1;
                self.stats.empty_polls += 1;
            } else {
                self.record_nonempty(events.len(), previous_poll_started);
            }
            Ok(events)
        }

        pub(crate) fn poll_recv_direct(
            &mut self,
            recv_jfc: &ffi::JfcHandle,
            pool: &mut UrmaBufferPool,
            mut consume: impl FnMut(Option<u64>, &[u8]) -> Result<()>,
        ) -> Result<usize> {
            let poll_started = Instant::now();
            let previous_poll_started = self.last_poll_started.replace(poll_started);
            self.stats.poll_calls += 1;
            self.stats.recv_jfc_poll_calls += 1;
            let mut records = [ffi::CompletionRecord::default(); 16];
            let count = recv_jfc
                .poll_into(&mut records[..self.batch])
                .map_err(|error| map_ffi_error("poll_recv_jfc", error))?;
            if count == 0 {
                self.stats.empty_polls += 1;
                self.empty_streak = self.empty_streak.saturating_add(1);
                self.stats.max_empty_streak = self.stats.max_empty_streak.max(self.empty_streak);
                std::hint::spin_loop();
                return Ok(0);
            }
            for record in records.into_iter().take(count) {
                self.route_recv_direct(record, pool, &mut consume)?;
            }
            self.record_nonempty(count, previous_poll_started);
            Ok(count)
        }

        fn route_recv_direct(
            &mut self,
            record: ffi::CompletionRecord,
            pool: &mut UrmaBufferPool,
            consume: &mut impl FnMut(Option<u64>, &[u8]) -> Result<()>,
        ) -> Result<()> {
            if !record.user_ctx_valid {
                self.stats.cqe_error += 1;
                return Err(Error::Completion {
                    status: record.status,
                    opcode: record.opcode,
                    user_ctx: 0,
                });
            }
            let token = WrToken::decode(record.user_ctx)?;
            if token.connection_id != self.connection_id
                || token.generation != self.generation
                || token.operation != OperationType::Recv
                || !record.is_recv
                || !record.is_jetty
            {
                self.stats.cqe_error += 1;
                return Err(Error::Protocol(
                    "direct receive CQE identity/queue flags disagree".into(),
                ));
            }
            let outstanding = self.take_outstanding(record.user_ctx)?;
            outstanding.handle.complete();
            self.outstanding_recv -= 1;
            if let Err(error) =
                validate_completion_status(record.status, record.opcode, record.user_ctx)
            {
                self.stats.cqe_error += 1;
                pool.complete_error(token.slot, OperationType::Recv)?;
                pool.release(token.slot)?;
                return Err(error);
            }
            if record.opcode != 0 {
                self.stats.cqe_error += 1;
                pool.complete_error(token.slot, OperationType::Recv)?;
                pool.release(token.slot)?;
                return Err(Error::Protocol(format!(
                    "unexpected receive CQE opcode {}",
                    record.opcode
                )));
            }
            let consumed = pool.complete_recv_with(token.slot, record.completion_len, |bytes| {
                consume(outstanding.sequence, bytes)
            });
            // Release even when application validation or sink I/O fails: the
            // CQE has ended device access and the callback borrow is over.
            pool.release(token.slot)?;
            consumed?;
            self.stats.recv_cqe += 1;
            if let Some(sequence) = outstanding.sequence {
                self.last_completed_recv_sequence = Some(sequence);
            }
            Ok(())
        }

        fn poll_active(
            &mut self,
            send_jfc: &ffi::JfcHandle,
            recv_jfc: &ffi::JfcHandle,
            pool: &mut UrmaBufferPool,
        ) -> Result<Vec<CompletionEvent>> {
            let mut events = Vec::new();
            if self.outstanding_send != 0 {
                self.stats.send_jfc_poll_calls += 1;
                let mut records = [ffi::CompletionRecord::default(); 16];
                let count = send_jfc
                    .poll_into(&mut records[..self.batch])
                    .map_err(|error| map_ffi_error("poll_send_jfc", error))?;
                for record in records.into_iter().take(count) {
                    events.extend(self.route(record, false, pool)?);
                }
            }
            if self.outstanding_recv != 0 {
                self.stats.recv_jfc_poll_calls += 1;
                let mut records = [ffi::CompletionRecord::default(); 16];
                let count = recv_jfc
                    .poll_into(&mut records[..self.batch])
                    .map_err(|error| map_ffi_error("poll_recv_jfc", error))?;
                for record in records.into_iter().take(count) {
                    events.extend(self.route(record, true, pool)?);
                }
            }
            Ok(events)
        }

        fn rearm_active(
            &mut self,
            send_jfc: &ffi::JfcHandle,
            recv_jfc: &ffi::JfcHandle,
        ) -> Result<bool> {
            let mut rearmed = false;
            if self.outstanding_send != 0 && !self.send_armed {
                send_jfc
                    .rearm()
                    .map_err(|error| map_ffi_error("rearm_send_jfc", error))?;
                self.send_armed = true;
                self.stats.jfc_rearm_count += 1;
                rearmed = true;
            }
            if self.outstanding_recv != 0 && !self.recv_armed {
                recv_jfc
                    .rearm()
                    .map_err(|error| map_ffi_error("rearm_recv_jfc", error))?;
                self.recv_armed = true;
                self.stats.jfc_rearm_count += 1;
                rearmed = true;
            }
            Ok(rearmed)
        }

        fn record_nonempty(&mut self, event_count: usize, previous_poll_started: Option<Instant>) {
            self.empty_streak = 0;
            self.stats.nonempty_polls += 1;
            self.stats.completion_batch_total = self
                .stats
                .completion_batch_total
                .saturating_add(event_count as u64);
            if let Some(last) = previous_poll_started {
                let gap = Instant::now().saturating_duration_since(last);
                self.stats.max_completion_poll_gap_ns = self
                    .stats
                    .max_completion_poll_gap_ns
                    .max(duration_ns_saturating(gap));
            }
        }

        fn route(
            &mut self,
            record: ffi::CompletionRecord,
            recv_queue: bool,
            pool: &mut UrmaBufferPool,
        ) -> Result<Vec<CompletionEvent>> {
            if !record.user_ctx_valid {
                self.stats.cqe_error += 1;
                return Err(Error::Completion {
                    status: record.status,
                    opcode: record.opcode,
                    user_ctx: 0,
                });
            }
            let token = WrToken::decode(record.user_ctx)?;
            if token.connection_id != self.connection_id || token.generation != self.generation {
                return Err(Error::Protocol("stale or foreign CQE user_ctx".into()));
            }
            let expected_recv = token.operation == OperationType::Recv;
            if expected_recv != recv_queue || record.is_recv != recv_queue || !record.is_jetty {
                self.stats.cqe_error += 1;
                return Err(Error::Protocol("CQE queue/operation flags disagree".into()));
            }
            match token.operation {
                OperationType::Send => self.route_send_frontier(record, pool),
                OperationType::Recv => {
                    let outstanding = self.take_outstanding(record.user_ctx)?;
                    outstanding.handle.complete();
                    self.outstanding_recv -= 1;
                    if let Err(error) =
                        validate_completion_status(record.status, record.opcode, record.user_ctx)
                    {
                        self.stats.cqe_error += 1;
                        pool.complete_error(token.slot, token.operation)?;
                        pool.release(token.slot)?;
                        return Err(error);
                    }
                    // URMA documents opcode only for receive CRs; M3 accepts
                    // only the SEND opcode value (zero).
                    if record.opcode != 0 {
                        self.stats.cqe_error += 1;
                        pool.complete_error(token.slot, token.operation)?;
                        pool.release(token.slot)?;
                        return Err(Error::Protocol(format!(
                            "unexpected receive CQE opcode {}",
                            record.opcode
                        )));
                    }
                    let bytes = pool.complete_recv(token.slot, record.completion_len)?;
                    pool.release(token.slot)?;
                    self.stats.recv_cqe += 1;
                    if let Some(sequence) = outstanding.sequence {
                        self.last_completed_recv_sequence = Some(sequence);
                    }
                    Ok(vec![CompletionEvent::RecvCompleted {
                        slot: token.slot,
                        bytes,
                    }])
                }
            }
        }

        /// A successful signaled SEND completion is the local completion
        /// frontier used by urma_perftest: all earlier WRs on this ordered JFS
        /// are retired together. Their registered slots remain pinned until
        /// this point, including WRs that did not request their own CQE.
        fn route_send_frontier(
            &mut self,
            record: ffi::CompletionRecord,
            pool: &mut UrmaBufferPool,
        ) -> Result<Vec<CompletionEvent>> {
            let frontier = self
                .send_order
                .iter()
                .position(|user_ctx| *user_ctx == record.user_ctx)
                .ok_or_else(|| Error::Protocol("SEND CQE has no ordered frontier".into()))?;
            let completion_error =
                validate_completion_status(record.status, record.opcode, record.user_ctx).err();
            // Providers may report a flushed WR even when that WR did not ask
            // for a normal success CQE. Accept that only on the error path: it
            // proves device access has ended and lets shutdown retire the
            // ordered prefix without releasing any buffer early.
            if completion_error.is_none() && !self.outstanding_for(record.user_ctx)?.signaled {
                return Err(Error::Protocol(
                    "SEND CQE corresponds to an unsignaled WR".into(),
                ));
            }
            self.stats.send_cqe += 1;
            let mut events = Vec::with_capacity(frontier + 1);
            for _ in 0..=frontier {
                let user_ctx = self
                    .send_order
                    .pop_front()
                    .expect("frontier position proves queue entry");
                let outstanding = self.take_outstanding(user_ctx)?;
                let token = WrToken::decode(user_ctx)?;
                outstanding.handle.complete();
                self.outstanding_send -= 1;
                if completion_error.is_some() {
                    pool.complete_error(token.slot, OperationType::Send)?;
                } else {
                    pool.complete_send(token.slot)?;
                }
                pool.release(token.slot)?;
                self.stats.send_retired += 1;
                if let Some(sequence) = outstanding.sequence {
                    self.last_completed_send_sequence = Some(sequence);
                }
                events.push(CompletionEvent::SendCompleted { slot: token.slot });
            }
            if let Some(error) = completion_error {
                self.stats.cqe_error += 1;
                return Err(error);
            }
            Ok(events)
        }

        pub(crate) fn stats(&self) -> CompletionStats {
            self.stats
        }

        pub(crate) fn connection_id(&self) -> u16 {
            self.connection_id
        }

        pub(crate) fn generation(&self) -> u8 {
            self.generation
        }

        pub(crate) fn reserve_send(&mut self, additional: usize) {
            self.send_order.reserve(additional);
        }

        pub(crate) fn outstanding(&self) -> usize {
            self.outstanding_total
        }

        pub(crate) fn outstanding_send(&self) -> usize {
            self.outstanding_send
        }

        pub(crate) fn outstanding_recv(&self) -> usize {
            self.outstanding_recv
        }

        pub(crate) fn diagnostic(
            &self,
            operation: OperationType,
            pool: &UrmaBufferPool,
        ) -> CompletionDiagnostic {
            let mut pending = self
                .outstanding
                .iter()
                .filter_map(|outstanding| {
                    let outstanding = outstanding.as_ref()?;
                    let token = WrToken::decode(outstanding.user_ctx).ok()?;
                    (token.operation == operation).then(|| PendingWrSnapshot {
                        sequence: outstanding.sequence,
                        slot: token.slot,
                        state: pool.slot_state(token.slot).unwrap_or(SlotState::Free),
                    })
                })
                .collect::<Vec<_>>();
            pending.sort_by_key(|item| (item.sequence, item.slot.index()));
            CompletionDiagnostic {
                pending,
                last_completed_sequence: match operation {
                    OperationType::Send => self.last_completed_send_sequence,
                    OperationType::Recv => self.last_completed_recv_sequence,
                },
            }
        }

        fn outstanding_for(&self, user_ctx: u64) -> Result<&OutstandingWr> {
            let token = WrToken::decode(user_ctx)?;
            self.outstanding
                .get(token.slot.index())
                .and_then(Option::as_ref)
                .filter(|outstanding| outstanding.user_ctx == user_ctx)
                .ok_or_else(|| Error::Protocol("CQE has no outstanding WR".into()))
        }

        fn take_outstanding(&mut self, user_ctx: u64) -> Result<OutstandingWr> {
            let token = WrToken::decode(user_ctx)?;
            let entry = self
                .outstanding
                .get_mut(token.slot.index())
                .ok_or_else(|| Error::Protocol("CQE slot is outside outstanding table".into()))?;
            if !entry
                .as_ref()
                .is_some_and(|outstanding| outstanding.user_ctx == user_ctx)
            {
                return Err(Error::Protocol("CQE has no outstanding WR".into()));
            }
            self.outstanding_total -= 1;
            Ok(entry.take().expect("entry checked above"))
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
pub(crate) use native::CompletionPoller;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expired_deadline_reports_timeout_without_polling() {
        assert_eq!(
            check_deadline(Instant::now(), "test_poll"),
            Err(Error::Timeout {
                operation: "test_poll"
            })
        );
    }

    #[test]
    fn cqe_error_is_structured() {
        let error = validate_completion_status(9, 0, 42).unwrap_err();
        assert!(error.to_string().contains("status=9"));
    }

    #[test]
    fn hybrid_policy_keeps_a_hot_phase_before_event_wait() {
        assert!(!should_wait_for_event(1));
        assert!(!should_wait_for_event(HOT_POLL_LIMIT));
        assert!(!should_wait_for_event(HOT_POLL_LIMIT));
        assert_eq!(EVENT_WAIT_TIMEOUT_MS, 10);
    }

    #[test]
    fn duration_conversion_saturates_to_transport_counter_width() {
        assert_eq!(duration_ns_saturating(Duration::from_nanos(42)), 42);
    }
}
