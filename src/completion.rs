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
    pub max_outstanding_send: u64,
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
            self.stats.poll_calls += 1;
            let mut events = Vec::new();
            for record in send_jfc
                .poll(self.batch)
                .map_err(|error| map_ffi_error("poll_send_jfc", error))?
            {
                events.push(self.route(record, false, pool)?);
            }
            for record in recv_jfc
                .poll(self.batch)
                .map_err(|error| map_ffi_error("poll_recv_jfc", error))?
            {
                events.push(self.route(record, true, pool)?);
            }
            if events.is_empty() {
                self.stats.empty_polls += 1;
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
}
