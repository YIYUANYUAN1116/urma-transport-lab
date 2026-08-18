use crate::{Error, Result, SlotId, SlotState};
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CompletionStats {
    pub send_post: u64,
    pub recv_post: u64,
    pub send_cqe: u64,
    pub recv_cqe: u64,
    pub cqe_error: u64,
    pub poll_calls: u64,
    pub empty_polls: u64,
    pub send_jfc_poll_calls: u64,
    pub recv_jfc_poll_calls: u64,
    pub yield_count: u64,
    pub sleep_count: u64,
    pub backoff_sleep_ns: u64,
    pub max_empty_streak: u64,
    pub nonempty_polls: u64,
    pub completion_batch_total: u64,
    pub max_completion_poll_gap_ns: u64,
    pub max_outstanding_send: u64,
}

#[cfg(any(feature = "urma", test))]
const HOT_POLL_LIMIT: u64 = 64;
#[cfg(any(feature = "urma", test))]
const YIELD_POLL_LIMIT: u64 = 128;
#[cfg(any(feature = "urma", test))]
const EMPTY_POLL_SLEEP: Duration = Duration::from_micros(10);

#[cfg(any(feature = "urma", test))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum EmptyPollAction {
    Spin,
    Yield,
    Sleep(Duration),
}

#[cfg(any(feature = "urma", test))]
fn empty_poll_action(streak: u64) -> EmptyPollAction {
    if streak <= HOT_POLL_LIMIT {
        EmptyPollAction::Spin
    } else if streak <= YIELD_POLL_LIMIT {
        EmptyPollAction::Yield
    } else {
        EmptyPollAction::Sleep(EMPTY_POLL_SLEEP)
    }
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
    use std::collections::HashMap;

    struct OutstandingWr {
        handle: ffi::WrHandle,
        sequence: Option<u64>,
    }

    pub(crate) struct CompletionPoller {
        connection_id: u16,
        generation: u8,
        batch: usize,
        outstanding: HashMap<u64, OutstandingWr>,
        outstanding_send: usize,
        outstanding_recv: usize,
        last_completed_send_sequence: Option<u64>,
        last_completed_recv_sequence: Option<u64>,
        empty_streak: u64,
        last_poll_started: Option<Instant>,
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
                outstanding: HashMap::new(),
                outstanding_send: 0,
                outstanding_recv: 0,
                last_completed_send_sequence: None,
                last_completed_recv_sequence: None,
                empty_streak: 0,
                last_poll_started: None,
                stats: CompletionStats::default(),
            })
        }

        pub(crate) fn track(
            &mut self,
            user_ctx: u64,
            operation: OperationType,
            wr: ffi::WrHandle,
            sequence: Option<u64>,
        ) -> Result<()> {
            if self.outstanding.contains_key(&user_ctx) {
                return Err(Error::Protocol("duplicate outstanding user_ctx".into()));
            }
            self.outstanding.insert(
                user_ctx,
                OutstandingWr {
                    handle: wr,
                    sequence,
                },
            );
            match operation {
                OperationType::Send => {
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
            pool: &mut UrmaBufferPool,
        ) -> Result<Vec<CompletionEvent>> {
            let poll_started = Instant::now();
            let poll_gap = self
                .last_poll_started
                .map(|last| poll_started.saturating_duration_since(last));
            self.last_poll_started = Some(poll_started);
            self.stats.poll_calls += 1;
            let mut events = Vec::new();
            if self.outstanding_send != 0 {
                self.stats.send_jfc_poll_calls += 1;
                for record in send_jfc
                    .poll(self.batch)
                    .map_err(|error| map_ffi_error("poll_send_jfc", error))?
                {
                    events.push(self.route(record, false, pool)?);
                }
            }
            if self.outstanding_recv != 0 {
                self.stats.recv_jfc_poll_calls += 1;
                for record in recv_jfc
                    .poll(self.batch)
                    .map_err(|error| map_ffi_error("poll_recv_jfc", error))?
                {
                    events.push(self.route(record, true, pool)?);
                }
            }
            if events.is_empty() {
                self.stats.empty_polls += 1;
                self.empty_streak = self.empty_streak.saturating_add(1);
                self.stats.max_empty_streak = self.stats.max_empty_streak.max(self.empty_streak);
                match empty_poll_action(self.empty_streak) {
                    EmptyPollAction::Spin => std::hint::spin_loop(),
                    EmptyPollAction::Yield => {
                        self.stats.yield_count += 1;
                        std::thread::yield_now();
                    }
                    EmptyPollAction::Sleep(duration) => {
                        self.stats.sleep_count += 1;
                        self.stats.backoff_sleep_ns = self
                            .stats
                            .backoff_sleep_ns
                            .saturating_add(duration_ns_saturating(duration));
                        std::thread::sleep(duration);
                    }
                }
            } else {
                self.empty_streak = 0;
                self.stats.nonempty_polls += 1;
                self.stats.completion_batch_total = self
                    .stats
                    .completion_batch_total
                    .saturating_add(events.len() as u64);
                if let Some(gap) = poll_gap {
                    self.stats.max_completion_poll_gap_ns = self
                        .stats
                        .max_completion_poll_gap_ns
                        .max(duration_ns_saturating(gap));
                }
            }
            Ok(events)
        }

        fn route(
            &mut self,
            record: ffi::CompletionRecord,
            recv_queue: bool,
            pool: &mut UrmaBufferPool,
        ) -> Result<CompletionEvent> {
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
            let outstanding = self
                .outstanding
                .remove(&record.user_ctx)
                .ok_or_else(|| Error::Protocol("CQE has no outstanding WR".into()))?;
            outstanding.handle.complete();
            match token.operation {
                OperationType::Send => self.outstanding_send -= 1,
                OperationType::Recv => self.outstanding_recv -= 1,
            }
            if expected_recv != recv_queue || record.is_recv != recv_queue || !record.is_jetty {
                self.stats.cqe_error += 1;
                pool.complete_error(token.slot, token.operation)?;
                pool.release(token.slot)?;
                return Err(Error::Protocol("CQE queue/operation flags disagree".into()));
            }

            if let Err(error) =
                validate_completion_status(record.status, record.opcode, record.user_ctx)
            {
                self.stats.cqe_error += 1;
                pool.complete_error(token.slot, token.operation)?;
                pool.release(token.slot)?;
                return Err(error);
            }
            match token.operation {
                OperationType::Send => {
                    pool.complete_send(token.slot)?;
                    pool.release(token.slot)?;
                    self.stats.send_cqe += 1;
                    if let Some(sequence) = outstanding.sequence {
                        self.last_completed_send_sequence = Some(sequence);
                    }
                    Ok(CompletionEvent::SendCompleted { slot: token.slot })
                }
                OperationType::Recv => {
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
                    Ok(CompletionEvent::RecvCompleted {
                        slot: token.slot,
                        bytes,
                    })
                }
            }
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

        pub(crate) fn outstanding(&self) -> usize {
            self.outstanding.len()
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
                .filter_map(|(user_ctx, outstanding)| {
                    let token = WrToken::decode(*user_ctx).ok()?;
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
    fn empty_poll_backoff_keeps_a_hot_phase_before_yield_and_short_sleep() {
        assert_eq!(empty_poll_action(1), EmptyPollAction::Spin);
        assert_eq!(empty_poll_action(HOT_POLL_LIMIT), EmptyPollAction::Spin);
        assert_eq!(
            empty_poll_action(HOT_POLL_LIMIT + 1),
            EmptyPollAction::Yield
        );
        assert_eq!(empty_poll_action(YIELD_POLL_LIMIT), EmptyPollAction::Yield);
        assert_eq!(
            empty_poll_action(YIELD_POLL_LIMIT + 1),
            EmptyPollAction::Sleep(Duration::from_micros(10))
        );
    }

    #[test]
    fn duration_conversion_saturates_to_transport_counter_width() {
        assert_eq!(duration_ns_saturating(Duration::from_nanos(42)), 42);
    }
}
