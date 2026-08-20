//! B2 bounded-pipeline policy and the URMA benchmark data path.

use crate::{
    BenchmarkCase, BenchmarkSink, BenchmarkTransport, DigestAlgorithm, Error,
    IntegrationMessageBodyV3, IntegrationMessageV3, Result,
};
use std::time::{Duration, Instant};

pub const URMA_PROTOCOL_HEADER_LEN: usize = crate::message::DATA_HEADER_LEN;

pub(crate) fn idle_timeout_elapsed(
    last_progress: Instant,
    now: Instant,
    timeout: Duration,
) -> bool {
    now.saturating_duration_since(last_progress) >= timeout
}

pub(crate) fn scaled_ratio(numerator: u64, denominator: u64, scale: u64) -> u64 {
    if denominator == 0 {
        return 0;
    }
    let scaled = u128::from(numerator) * u128::from(scale) / u128::from(denominator);
    u64::try_from(scaled).unwrap_or(u64::MAX)
}

/// Derives the registered slot used by the URMA benchmark. `chunk_size`
/// remains the business payload size; only the backing slot is rounded up.
pub fn derive_urma_slot_size(case: &BenchmarkCase, alignment: usize) -> Result<usize> {
    if alignment < std::mem::size_of::<usize>() || !alignment.is_power_of_two() {
        return Err(invalid(
            "URMA slot alignment must be a power of two and at least pointer-sized",
        ));
    }
    let chunk_size = case.chunk_size_usize()?;
    // Bulk Data is sent as a raw URMA message. Request/Metadata/End remain
    // framed control messages, but they are much smaller than a data slot.
    // This mirrors Dragonfly's RDMA path and lets a provider with a 64 KiB
    // max_msg_size carry a full 64 KiB piece chunk.
    chunk_size
        .checked_add(alignment - 1)
        .map(|value| value & !(alignment - 1))
        .ok_or_else(|| invalid("aligned URMA slot_size overflow"))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct UrmaPipelineLimits {
    pub slot_size: usize,
    pub tx_slot_count: usize,
    pub rx_slot_count: usize,
    pub send_jfc_depth: u32,
    pub recv_jfc_depth: u32,
    pub jetty_send_depth: u32,
    pub jetty_recv_depth: u32,
    pub provider_max_message_size: u64,
}

impl UrmaPipelineLimits {
    pub fn from_configs(runtime: &crate::RuntimeConfig, jetty: &crate::JettyConfig) -> Self {
        Self {
            slot_size: runtime.buffer_pool.slot_size,
            tx_slot_count: runtime.buffer_pool.tx_slot_count,
            rx_slot_count: runtime.buffer_pool.rx_slot_count,
            send_jfc_depth: runtime.send_jfc_depth,
            recv_jfc_depth: runtime.recv_jfc_depth,
            jetty_send_depth: jetty.send_depth,
            jetty_recv_depth: jetty.recv_depth,
            provider_max_message_size: u64::MAX,
        }
    }

    pub fn with_provider_max_message_size(mut self, value: u64) -> Self {
        self.provider_max_message_size = value;
        self
    }
}

pub fn validate_urma_case(case: &BenchmarkCase, limits: UrmaPipelineLimits) -> Result<()> {
    case.validate()?;
    if case.transport != BenchmarkTransport::Urma {
        return Err(invalid("URMA runner requires transport=urma"));
    }
    let window = usize::try_from(case.window).map_err(|_| invalid("window exceeds usize"))?;
    for (name, maximum) in [
        ("TX slot count", limits.tx_slot_count as u64),
        ("RX slot count", limits.rx_slot_count as u64),
        ("send JFC depth", u64::from(limits.send_jfc_depth)),
        ("receive JFC depth", u64::from(limits.recv_jfc_depth)),
        ("Jetty send depth", u64::from(limits.jetty_send_depth)),
        ("Jetty receive depth", u64::from(limits.jetty_recv_depth)),
    ] {
        if window as u64 > maximum {
            return Err(invalid(format!(
                "window={} exceeds {name}={maximum}",
                case.window
            )));
        }
    }
    let slot_payload = u64::try_from(limits.slot_size)
        .map_err(|_| invalid("registered slot_size does not fit u64"))?;
    let maximum_payload = slot_payload.min(limits.provider_max_message_size);
    if case.chunk_size > maximum_payload {
        return Err(invalid(format!(
            "chunk_size={} exceeds effective URMA payload limit {maximum_payload} (slot_size={}, provider_max_message_size={})",
            case.chunk_size,
            limits.slot_size,
            limits.provider_max_message_size
        )));
    }
    if case.chunk_count()? > u64::from(u32::MAX) {
        return Err(invalid("URMA Data sequence count exceeds u32"));
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PipelineTracker {
    configured_window: usize,
    current: usize,
    maximum: usize,
}

impl PipelineTracker {
    pub fn new(window: usize) -> Result<Self> {
        if window == 0 {
            return Err(invalid("pipeline window must be non-zero"));
        }
        Ok(Self {
            configured_window: window,
            current: 0,
            maximum: 0,
        })
    }

    pub fn can_post(&self) -> bool {
        self.current < self.configured_window
    }

    pub fn posted(&mut self) -> Result<()> {
        if !self.can_post() {
            return Err(invalid("outstanding SEND would exceed configured window"));
        }
        self.current += 1;
        self.maximum = self.maximum.max(self.current);
        Ok(())
    }

    pub fn completed(&mut self) -> Result<()> {
        self.current = self
            .current
            .checked_sub(1)
            .ok_or_else(|| Error::Protocol("send completion without outstanding SEND".into()))?;
        Ok(())
    }

    pub const fn current(&self) -> usize {
        self.current
    }

    pub const fn configured_window(&self) -> usize {
        self.configured_window
    }

    pub const fn maximum(&self) -> usize {
        self.maximum
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReceiveCreditController {
    configured: usize,
    remaining_messages: usize,
    current_credit: usize,
    next_post_sequence: u64,
}

/// Sender-side accounting for receive work requests that the peer has
/// explicitly confirmed as posted. Local SEND completion never replenishes
/// this credit: it only proves that the local TX buffer may be reclaimed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RemoteReceiveCredit {
    initial: usize,
    available: usize,
    returned: usize,
    consumed: usize,
    updates: usize,
    waits: usize,
}

impl RemoteReceiveCredit {
    pub fn new(initial: usize) -> Result<Self> {
        if initial == 0 {
            return Err(invalid("initial remote receive credit must be non-zero"));
        }
        Ok(Self {
            initial,
            available: initial,
            returned: 0,
            consumed: 0,
            updates: 0,
            waits: 0,
        })
    }

    pub const fn can_send(&self) -> bool {
        self.available != 0
    }

    pub fn consume(&mut self) -> Result<()> {
        self.available = self
            .available
            .checked_sub(1)
            .ok_or_else(|| Error::Protocol("SEND without remote receive credit".into()))?;
        self.consumed = self
            .consumed
            .checked_add(1)
            .ok_or_else(|| Error::Protocol("remote receive credit consumption overflow".into()))?;
        Ok(())
    }

    pub fn grant(&mut self, count: usize) -> Result<()> {
        if count == 0 {
            return Err(Error::Protocol("zero remote receive credit update".into()));
        }
        let available = self
            .available
            .checked_add(count)
            .ok_or_else(|| Error::Protocol("remote receive credit overflow".into()))?;
        if available > self.initial {
            return Err(Error::Protocol(format!(
                "remote receive credit {available} exceeds posted RQ capacity {}",
                self.initial
            )));
        }
        self.available = available;
        self.returned = self
            .returned
            .checked_add(count)
            .ok_or_else(|| Error::Protocol("returned remote receive credit overflow".into()))?;
        self.updates = self
            .updates
            .checked_add(1)
            .ok_or_else(|| Error::Protocol("remote receive credit update overflow".into()))?;
        Ok(())
    }

    pub fn waited(&mut self) -> Result<()> {
        self.waits = self
            .waits
            .checked_add(1)
            .ok_or_else(|| Error::Protocol("remote receive credit wait overflow".into()))?;
        Ok(())
    }

    pub const fn initial(&self) -> usize {
        self.initial
    }

    pub const fn available(&self) -> usize {
        self.available
    }

    pub const fn returned(&self) -> usize {
        self.returned
    }

    pub const fn consumed(&self) -> usize {
        self.consumed
    }

    pub const fn updates(&self) -> usize {
        self.updates
    }

    pub const fn waits(&self) -> usize {
        self.waits
    }
}

/// Receiver-side batching for credits that became safe only after the RX WRs
/// were successfully reposted.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RemoteCreditReturn {
    threshold: usize,
    pending: usize,
    returned: usize,
    updates: usize,
}

impl RemoteCreditReturn {
    pub fn new(posted_credit: usize) -> Result<Self> {
        if posted_credit == 0 {
            return Err(invalid("posted receive credit must be non-zero"));
        }
        Ok(Self {
            threshold: (posted_credit / 4).max(1),
            pending: 0,
            returned: 0,
            updates: 0,
        })
    }

    pub fn reposted(&mut self, count: usize) -> Result<Option<usize>> {
        self.pending = self
            .pending
            .checked_add(count)
            .ok_or_else(|| Error::Protocol("pending remote credit overflow".into()))?;
        if self.pending < self.threshold {
            return Ok(None);
        }
        let count = self.pending / self.threshold * self.threshold;
        self.pending -= count;
        self.returned = self
            .returned
            .checked_add(count)
            .ok_or_else(|| Error::Protocol("returned receive credit overflow".into()))?;
        self.updates = self
            .updates
            .checked_add(1)
            .ok_or_else(|| Error::Protocol("receive credit update overflow".into()))?;
        Ok(Some(count))
    }

    /// Returns a final partial batch when the sender has posted every Data
    /// message but still needs one receive credit for End.
    pub fn flush(&mut self) -> Result<Option<usize>> {
        if self.pending == 0 {
            return Ok(None);
        }
        let count = self.pending;
        self.pending = 0;
        self.returned = self
            .returned
            .checked_add(count)
            .ok_or_else(|| Error::Protocol("returned receive credit overflow".into()))?;
        self.updates = self
            .updates
            .checked_add(1)
            .ok_or_else(|| Error::Protocol("receive credit update overflow".into()))?;
        Ok(Some(count))
    }

    pub const fn threshold(&self) -> usize {
        self.threshold
    }

    pub const fn pending(&self) -> usize {
        self.pending
    }

    pub const fn returned(&self) -> usize {
        self.returned
    }

    pub const fn updates(&self) -> usize {
        self.updates
    }
}

pub(crate) fn receive_credit_target(
    _window: usize,
    rx_slot_count: usize,
    remaining_messages: usize,
) -> Result<usize> {
    // Match urma_perftest's receive model: the physical RQ is kept deep and
    // independent of the sender's application window. A CQ poll may consume a
    // complete batch before Rust can repost; retaining hundreds of posted
    // receives prevents that batch boundary from taking the RQ to zero.
    let target = rx_slot_count.min(remaining_messages);
    if target == 0 {
        return Err(invalid("receive credit target must be non-zero"));
    }
    Ok(target)
}

impl ReceiveCreditController {
    pub fn new(configured: usize, remaining_messages: usize) -> Result<Self> {
        if configured == 0 {
            return Err(invalid("receive credit must be non-zero"));
        }
        Ok(Self {
            configured,
            remaining_messages,
            current_credit: 0,
            next_post_sequence: 0,
        })
    }

    pub fn posts_needed(&self) -> usize {
        self.configured
            .min(self.remaining_messages)
            .saturating_sub(self.current_credit)
    }

    pub fn posted(&mut self) -> Result<()> {
        if self.posts_needed() == 0 {
            return Err(invalid("receive post would exceed required credit"));
        }
        let next_post_sequence = self
            .next_post_sequence
            .checked_add(1)
            .ok_or_else(|| Error::Protocol("receive post sequence overflow".into()))?;
        self.current_credit += 1;
        self.next_post_sequence = next_post_sequence;
        Ok(())
    }

    pub fn completed(&mut self) -> Result<()> {
        self.current_credit = self
            .current_credit
            .checked_sub(1)
            .ok_or_else(|| Error::Protocol("receive completion without posted credit".into()))?;
        self.remaining_messages = self
            .remaining_messages
            .checked_sub(1)
            .ok_or_else(|| Error::Protocol("receive completion after logical End".into()))?;
        Ok(())
    }

    pub const fn current_credit(&self) -> usize {
        self.current_credit
    }

    pub const fn configured_credit(&self) -> usize {
        self.configured
    }

    pub const fn remaining_messages(&self) -> usize {
        self.remaining_messages
    }

    pub const fn next_post_sequence(&self) -> u64 {
        self.next_post_sequence
    }
}

#[derive(Debug)]
pub struct UrmaReceiveState {
    request_id: u64,
    expected_bytes: u64,
    expected_crc32: u32,
    next_sequence: u32,
    actual_bytes: u64,
    metadata_seen: bool,
    complete: bool,
}

impl UrmaReceiveState {
    pub fn new(request_id: u64, expected_bytes: u64, expected_crc32: u32) -> Result<Self> {
        if request_id == 0 {
            return Err(Error::Protocol("request_id must be non-zero".into()));
        }
        Ok(Self {
            request_id,
            expected_bytes,
            expected_crc32,
            next_sequence: 0,
            actual_bytes: 0,
            metadata_seen: false,
            complete: false,
        })
    }

    pub fn accept_metadata(&mut self, message: &IntegrationMessageV3) -> Result<()> {
        self.check_identity(message)?;
        match &message.body {
            IntegrationMessageBodyV3::Metadata {
                offset,
                total_length,
                digest,
            } if message.sequence == 0 => {
                if self.metadata_seen || *offset != 0 || *total_length != self.expected_bytes {
                    return Err(Error::Protocol(
                        "URMA Metadata does not match benchmark case".into(),
                    ));
                }
                if digest.algorithm != DigestAlgorithm::Crc32
                    || digest.value.parse::<u32>().ok() != Some(self.expected_crc32)
                {
                    return Err(Error::Protocol("URMA Metadata CRC32 mismatch".into()));
                }
                self.metadata_seen = true;
                Ok(())
            }
            _ => Err(Error::Protocol("expected URMA Metadata".into())),
        }
    }

    pub fn accept_payload(
        &mut self,
        message: &IntegrationMessageV3,
        sink: &mut impl BenchmarkSink,
    ) -> Result<bool> {
        self.check_identity(message)?;
        if !self.metadata_seen || self.complete {
            return Err(Error::Protocol(
                "URMA payload outside receiving phase".into(),
            ));
        }
        match &message.body {
            IntegrationMessageBodyV3::Data(payload) => {
                self.accept_data(message.sequence, payload, sink)?;
                Ok(false)
            }
            IntegrationMessageBodyV3::End { .. } => self.accept_end(message).map(|()| true),
            IntegrationMessageBodyV3::Error { code, message } => Err(Error::Protocol(format!(
                "remote URMA error {code}: {message}"
            ))),
            _ => Err(Error::Protocol("expected URMA Data, End, or Error".into())),
        }
    }

    /// Accepts one raw bulk message. Sequence is carried by receive-post order,
    /// not by a per-message transport header. The sink is the sole digest
    /// owner, avoiding the previous duplicate CRC scan.
    pub fn accept_data(
        &mut self,
        sequence: u32,
        payload: &[u8],
        sink: &mut impl BenchmarkSink,
    ) -> Result<()> {
        let next = self.validate_data(sequence, payload.len())?;
        sink.write_chunk(payload)?;
        self.commit_data(next)?;
        Ok(())
    }

    #[cfg(feature = "urma")]
    pub(crate) fn accept_data_length(&mut self, sequence: u32, length: usize) -> Result<()> {
        let next = self.validate_data(sequence, length)?;
        self.commit_data(next)
    }

    pub(crate) fn accept_end(&mut self, message: &IntegrationMessageV3) -> Result<()> {
        self.check_identity(message)?;
        if !self.metadata_seen || self.complete {
            return Err(Error::Protocol(
                "URMA payload outside receiving phase".into(),
            ));
        }
        let IntegrationMessageBodyV3::End {
            total_length,
            chunk_count,
        } = &message.body
        else {
            return Err(Error::Protocol("expected URMA End".into()));
        };
        if message.sequence != self.next_sequence || *chunk_count != self.next_sequence {
            return Err(Error::Protocol(
                "URMA End sequence/chunk count mismatch".into(),
            ));
        }
        if *total_length != self.expected_bytes || self.actual_bytes != self.expected_bytes {
            return Err(Error::Protocol("URMA End/received length mismatch".into()));
        }
        self.complete = true;
        Ok(())
    }

    fn validate_data(&self, sequence: u32, length: usize) -> Result<u64> {
        if !self.metadata_seen || self.complete {
            return Err(Error::Protocol(
                "URMA payload outside receiving phase".into(),
            ));
        }
        if sequence != self.next_sequence {
            return Err(Error::Protocol(format!(
                "URMA Data sequence {sequence}, expected {}",
                self.next_sequence
            )));
        }
        if length == 0 {
            return Err(Error::Protocol(
                "URMA Data payload must not be empty".into(),
            ));
        }
        let next = self
            .actual_bytes
            .checked_add(length as u64)
            .ok_or_else(|| Error::Protocol("URMA received length overflow".into()))?;
        if next > self.expected_bytes {
            return Err(Error::Protocol(
                "URMA Data exceeds advertised length".into(),
            ));
        }
        Ok(next)
    }

    fn commit_data(&mut self, next: u64) -> Result<()> {
        self.actual_bytes = next;
        self.next_sequence = self
            .next_sequence
            .checked_add(1)
            .ok_or_else(|| Error::Protocol("URMA Data sequence overflow".into()))?;
        Ok(())
    }

    fn check_identity(&self, message: &IntegrationMessageV3) -> Result<()> {
        if message.request_id != self.request_id {
            Err(Error::Protocol("URMA request_id mismatch".into()))
        } else {
            Ok(())
        }
    }
}

fn invalid(message: impl Into<String>) -> Error {
    Error::InvalidConfiguration(message.into())
}

#[cfg(feature = "urma")]
mod native;

#[cfg(feature = "urma")]
pub use native::{
    run_urma_child, run_urma_child_profile, run_urma_child_profile_with_crc_workers,
    run_urma_parent, run_urma_parent_profile, UrmaBenchmarkDestination, UrmaBenchmarkProfile,
    UrmaBenchmarkSource, UrmaTransportStats,
};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        crc32_bytes, BenchmarkScenario, BenchmarkSink, FileCompletionPolicy, MemorySink, TimingMode,
    };

    fn case(bytes: u64, chunk: u64, window: u32) -> BenchmarkCase {
        BenchmarkCase::new(
            "b2-test",
            1,
            BenchmarkScenario::Memory,
            BenchmarkTransport::Urma,
            bytes,
            chunk,
            window,
            TimingMode::SteadyState,
            FileCompletionPolicy::Buffered,
            7,
        )
        .unwrap()
    }

    fn limits() -> UrmaPipelineLimits {
        UrmaPipelineLimits {
            slot_size: 64 * 1024,
            tx_slot_count: 8,
            rx_slot_count: 8,
            send_jfc_depth: 64,
            recv_jfc_depth: 64,
            jetty_send_depth: 64,
            jetty_recv_depth: 64,
            provider_max_message_size: 64 * 1024,
        }
    }

    #[test]
    fn validates_window_and_chunk_against_every_limit() {
        validate_urma_case(&case(1024, 1024, 8), limits()).unwrap();
        assert!(validate_urma_case(&case(1024, 1024, 9), limits()).is_err());
        assert!(validate_urma_case(&case(65_537, 65_537, 1), limits()).is_err());
    }

    #[test]
    fn derives_aligned_slots_for_b3_payload_candidates() {
        for payload in [64 * 1024, 256 * 1024, 512 * 1024, 1024 * 1024] {
            let benchmark = case(payload as u64, payload as u64, 4);
            let slot = derive_urma_slot_size(&benchmark, 4096).unwrap();
            assert!(slot >= payload);
            assert_eq!(slot % 4096, 0);
            let mut candidate = limits();
            candidate.slot_size = slot;
            candidate.provider_max_message_size = slot as u64;
            validate_urma_case(&benchmark, candidate).unwrap();
        }
    }

    #[test]
    fn rejects_small_slot_and_provider_message_limit() {
        let benchmark = case(256 * 1024, 256 * 1024, 4);
        let slot = derive_urma_slot_size(&benchmark, 4096).unwrap();
        let mut candidate = limits();
        candidate.slot_size = 256 * 1024 - 1;
        candidate.provider_max_message_size = slot as u64;
        assert!(validate_urma_case(&benchmark, candidate).is_err());

        candidate.slot_size = slot;
        candidate.provider_max_message_size = (slot - 1) as u64;
        assert!(validate_urma_case(&benchmark, candidate).is_err());
    }

    #[test]
    fn bounded_pipeline_reaches_multiple_outstanding_and_drains() {
        let mut pipeline = PipelineTracker::new(4).unwrap();
        for _ in 0..4 {
            pipeline.posted().unwrap();
        }
        assert!(!pipeline.can_post());
        assert!(pipeline.posted().is_err());
        assert!(pipeline.maximum() > 1);
        for _ in 0..4 {
            pipeline.completed().unwrap();
        }
        assert_eq!(pipeline.current(), 0);
    }

    #[test]
    fn receive_credit_replenishes_without_overposting() {
        let mut credit = ReceiveCreditController::new(4, 7).unwrap();
        while credit.posts_needed() != 0 {
            assert_eq!(credit.next_post_sequence(), credit.current_credit() as u64);
            credit.posted().unwrap();
        }
        assert_eq!(credit.current_credit(), 4);
        credit.completed().unwrap();
        assert_eq!(credit.posts_needed(), 1);
        credit.posted().unwrap();
        for _ in 0..6 {
            credit.completed().unwrap();
            while credit.posts_needed() != 0 {
                credit.posted().unwrap();
            }
        }
        assert_eq!(credit.remaining_messages(), 0);
        assert_eq!(credit.current_credit(), 0);
        assert_eq!(credit.next_post_sequence(), 7);
    }

    #[test]
    fn remote_receive_credit_is_independent_of_local_completion() {
        let mut credit = RemoteReceiveCredit::new(4).unwrap();
        for _ in 0..4 {
            assert!(credit.can_send());
            credit.consume().unwrap();
        }
        assert!(!credit.can_send());
        assert!(credit.consume().is_err());
        credit.waited().unwrap();
        credit.grant(2).unwrap();
        assert_eq!(credit.initial(), 4);
        assert_eq!(credit.available(), 2);
        assert_eq!(credit.returned(), 2);
        assert_eq!(credit.consumed(), 4);
        assert_eq!(credit.updates(), 1);
        assert_eq!(credit.waits(), 1);
        assert!(credit.grant(0).is_err());
    }

    #[test]
    fn remote_credit_is_returned_only_in_reposted_batches() {
        let mut returned = RemoteCreditReturn::new(512).unwrap();
        assert_eq!(returned.threshold(), 128);
        assert_eq!(returned.reposted(127).unwrap(), None);
        assert_eq!(returned.reposted(1).unwrap(), Some(128));
        assert_eq!(returned.reposted(300).unwrap(), Some(256));
        assert_eq!(returned.pending(), 44);
        assert_eq!(returned.returned(), 384);
        assert_eq!(returned.updates(), 2);
        assert_eq!(returned.flush().unwrap(), Some(44));
        assert_eq!(returned.pending(), 0);
        assert_eq!(returned.returned(), 428);
        assert_eq!(returned.updates(), 3);
        assert_eq!(returned.flush().unwrap(), None);

        let returned = RemoteCreditReturn::new(3).unwrap();
        assert_eq!(returned.threshold(), 1);
    }

    #[test]
    fn batched_remote_credit_preserves_one_final_end_receive() {
        let data_messages = 32_768usize;
        let total_messages = data_messages + 1;
        let mut local = ReceiveCreditController::new(512, total_messages).unwrap();
        while local.posts_needed() != 0 {
            local.posted().unwrap();
        }
        let mut remote = RemoteReceiveCredit::new(local.current_credit()).unwrap();
        let mut returned = RemoteCreditReturn::new(local.current_credit()).unwrap();

        for _ in 0..data_messages {
            assert!(remote.can_send());
            remote.consume().unwrap();
            local.completed().unwrap();
            let mut reposted = 0;
            while local.posts_needed() != 0 {
                local.posted().unwrap();
                reposted += 1;
            }
            if let Some(count) = returned.reposted(reposted).unwrap() {
                remote.grant(count).unwrap();
            }
        }

        if let Some(count) = returned.flush().unwrap() {
            remote.grant(count).unwrap();
        }
        assert!(remote.can_send(), "End must retain one remote RQE");
        remote.consume().unwrap();
        local.completed().unwrap();
        assert_eq!(local.remaining_messages(), 0);
        assert_eq!(local.current_credit(), 0);
    }

    #[test]
    fn idle_timeout_is_measured_from_latest_progress() {
        let start = Instant::now();
        let timeout = Duration::from_secs(30);
        let progress = start + Duration::from_secs(25);
        let now = start + Duration::from_secs(40);

        assert!(idle_timeout_elapsed(start, now, timeout));
        assert!(!idle_timeout_elapsed(progress, now, timeout));
    }

    #[test]
    fn scaled_poll_ratios_are_stable_and_overflow_safe() {
        assert_eq!(scaled_ratio(3, 4, 1_000_000), 750_000);
        assert_eq!(scaled_ratio(4, 3, 1_000), 1_333);
        assert_eq!(scaled_ratio(1, 0, 1_000), 0);
        assert_eq!(scaled_ratio(u64::MAX, 1, u64::MAX), u64::MAX);
    }

    #[test]
    fn receive_credit_target_adds_bounded_rq_headroom() {
        assert_eq!(receive_credit_target(4, 8, 100), Ok(8));
        assert_eq!(receive_credit_target(8, 8, 100), Ok(8));
        assert_eq!(receive_credit_target(4, 16, 3), Ok(3));
        assert_eq!(receive_credit_target(usize::MAX, 8, 100), Ok(8));
    }

    #[test]
    fn receive_state_checks_sequence_length_crc_and_zero_length() {
        let payload = b"abcdef";
        let crc = crc32_bytes(payload);
        let mut state = UrmaReceiveState::new(1, payload.len() as u64, crc).unwrap();
        state
            .accept_metadata(&IntegrationMessageV3::metadata(
                1,
                0,
                payload.len() as u64,
                crate::DigestDescriptor::crc32(crc),
            ))
            .unwrap();
        let mut sink = MemorySink::new(payload.len() as u64, crc);
        assert!(state
            .accept_payload(
                &IntegrationMessageV3::data(1, 1, payload.to_vec()),
                &mut sink
            )
            .is_err());

        let mut state = UrmaReceiveState::new(1, payload.len() as u64, crc).unwrap();
        state
            .accept_metadata(&IntegrationMessageV3::metadata(
                1,
                0,
                payload.len() as u64,
                crate::DigestDescriptor::crc32(crc),
            ))
            .unwrap();
        state
            .accept_payload(
                &IntegrationMessageV3::data(1, 0, payload.to_vec()),
                &mut sink,
            )
            .unwrap();
        assert!(state
            .accept_payload(&IntegrationMessageV3::end(1, 1, 7), &mut sink)
            .is_err());

        let mut empty = UrmaReceiveState::new(2, 0, crc32_bytes(b"")).unwrap();
        empty
            .accept_metadata(&IntegrationMessageV3::metadata(
                2,
                0,
                0,
                crate::DigestDescriptor::crc32(crc32_bytes(b"")),
            ))
            .unwrap();
        let mut empty_sink = MemorySink::new(0, crc32_bytes(b""));
        assert!(empty
            .accept_payload(&IntegrationMessageV3::end(2, 0, 0), &mut empty_sink)
            .unwrap());
        assert!(empty_sink.finish().unwrap().is_ok());
    }

    #[test]
    fn receive_state_rejects_crc_mismatch() {
        let mut state = UrmaReceiveState::new(3, 1, crc32_bytes(b"a")).unwrap();
        state
            .accept_metadata(&IntegrationMessageV3::metadata(
                3,
                0,
                1,
                crate::DigestDescriptor::crc32(crc32_bytes(b"a")),
            ))
            .unwrap();
        let mut sink = MemorySink::new(1, crc32_bytes(b"a"));
        state
            .accept_payload(&IntegrationMessageV3::data(3, 0, b"b".to_vec()), &mut sink)
            .unwrap();
        assert!(state
            .accept_payload(&IntegrationMessageV3::end(3, 1, 1), &mut sink)
            .unwrap());
        assert!(!sink.finish().unwrap().is_ok());
    }
}
