use crate::{Error, Result, SlotId};
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CompletionStats {
    pub send_post: u64,
    pub recv_post: u64,
    pub send_cqe: u64,
    pub recv_cqe: u64,
    pub cqe_error: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CompletionEvent {
    SendCompleted { slot: SlotId },
    RecvCompleted { slot: SlotId, bytes: Vec<u8> },
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

    pub(crate) struct CompletionPoller {
        connection_id: u16,
        generation: u8,
        batch: usize,
        outstanding: HashMap<u64, ffi::WrHandle>,
        outstanding_send: usize,
        outstanding_recv: usize,
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
                stats: CompletionStats::default(),
            })
        }

        pub(crate) fn track(
            &mut self,
            user_ctx: u64,
            operation: OperationType,
            wr: ffi::WrHandle,
        ) -> Result<()> {
            if self.outstanding.contains_key(&user_ctx) {
                return Err(Error::Protocol("duplicate outstanding user_ctx".into()));
            }
            self.outstanding.insert(user_ctx, wr);
            match operation {
                OperationType::Send => {
                    self.stats.send_post += 1;
                    self.outstanding_send += 1;
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
            let wr = self
                .outstanding
                .remove(&record.user_ctx)
                .ok_or_else(|| Error::Protocol("CQE has no outstanding WR".into()))?;
            wr.complete();
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
