use super::*;
use crate::{
    derive_urma_slot_size,
    oob::{child_handshake, parent_handshake, OobSession},
    BenchmarkResult, BenchmarkScenario, BenchmarkTimer, CompletionEvent, CompletionStats, CpuUsage,
    Crc32Hasher, DigestDescriptor, FileCompletionPolicy, FileSink, FileSource, IntegrityResult,
    JettyConfig, MemorySink, MemorySource, RuntimeConfig, TimingMode, TimingSample, UrmaConnection,
    UrmaRuntime,
};
use std::{
    collections::BTreeMap,
    fs::File,
    io::{self, Read, Write},
    net::{TcpListener, TcpStream, ToSocketAddrs},
    path::PathBuf,
    time::{Duration, Instant},
};

const REQUEST_ID: u64 = 1;
const TIMEOUT: Duration = Duration::from_secs(30);
const CONTROL_MAGIC: u32 = 0x4252_4d41;
const CONTROL_VERSION: u16 = 1;
const CONTROL_HEADER_LEN: usize = 12;
const MAX_CONTROL_PAYLOAD: usize = 4096;
const READY: u16 = 1;
const START: u16 = 2;
const DONE: u16 = 3;
const SEND_COMPLETION_INTERVAL: usize = 100;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum UrmaBenchmarkProfile {
    #[default]
    Normal,
    /// Reuse one immutable registered TX address for every logical SEND while
    /// preserving distinct WR ownership and end-to-end CRC verification.
    FixedTx,
    Rx128,
    FixedTxRx128,
}

impl UrmaBenchmarkProfile {
    fn fixed_tx(self) -> bool {
        matches!(self, Self::FixedTx | Self::FixedTxRx128)
    }

    fn rx_slots(self) -> usize {
        if matches!(self, Self::Rx128 | Self::FixedTxRx128) {
            128
        } else {
            512
        }
    }
}

#[derive(Clone, Debug)]
pub enum UrmaBenchmarkSource {
    Memory(MemorySource),
    File(FileSource),
}

impl UrmaBenchmarkSource {
    fn validate(&self, case: &BenchmarkCase) -> Result<()> {
        let (scenario, length) = match self {
            Self::Memory(source) => (BenchmarkScenario::Memory, source.length()),
            Self::File(source) => (BenchmarkScenario::File, source.length()),
        };
        if scenario != case.scenario || length != case.transfer_bytes {
            return Err(invalid("URMA source does not match benchmark case"));
        }
        Ok(())
    }

    fn expected_crc32(&self) -> u32 {
        match self {
            Self::Memory(source) => source.expected_crc32(),
            Self::File(source) => source.expected_crc32(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum UrmaBenchmarkDestination {
    Memory,
    File(PathBuf),
}

impl UrmaBenchmarkDestination {
    fn validate(&self, case: &BenchmarkCase) -> Result<()> {
        let scenario = match self {
            Self::Memory => BenchmarkScenario::Memory,
            Self::File(_) => BenchmarkScenario::File,
        };
        if scenario != case.scenario {
            return Err(invalid("URMA destination does not match benchmark case"));
        }
        Ok(())
    }

    fn create_sink(
        &self,
        expected_bytes: u64,
        expected_crc32: u32,
        policy: FileCompletionPolicy,
    ) -> Result<ActiveSink> {
        match self {
            Self::Memory => Ok(ActiveSink::Memory(MemorySink::new(
                expected_bytes,
                expected_crc32,
            ))),
            Self::File(path) => Ok(ActiveSink::File(FileSink::create(
                path,
                expected_bytes,
                expected_crc32,
                policy,
            )?)),
        }
    }
}

enum ActiveSink {
    Memory(MemorySink),
    File(FileSink),
}

impl BenchmarkSink for ActiveSink {
    fn write_chunk(&mut self, bytes: &[u8]) -> Result<()> {
        match self {
            Self::Memory(sink) => sink.write_chunk(bytes),
            Self::File(sink) => sink.write_chunk(bytes),
        }
    }

    fn finish(self) -> Result<IntegrityResult> {
        match self {
            Self::Memory(sink) => sink.finish(),
            Self::File(sink) => sink.finish(),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct UrmaTransportStats {
    pub send_post: u64,
    pub recv_post: u64,
    pub send_cqe: u64,
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
    pub avg_poll_batch_milli: u64,
    pub empty_poll_ratio_ppm: u64,
    pub max_completion_poll_gap_ns: u64,
    pub max_outstanding_send: u64,
    pub current_outstanding_send: u64,
    pub current_outstanding_recv: u64,
    pub configured_window: u64,
    pub configured_receive_credit: u64,
    pub slot_size: u64,
    pub effective_payload_size: u64,
    pub tx_slot_count: u64,
    pub rx_slot_count: u64,
    pub total_registered_bytes: u64,
    pub bytes_sent: u64,
    pub bytes_received: u64,
}

impl UrmaTransportStats {
    fn insert_all(self, output: &mut BTreeMap<String, u64>) {
        for (name, value) in [
            ("send_post", self.send_post),
            ("recv_post", self.recv_post),
            ("send_cqe", self.send_cqe),
            ("send_retired", self.send_retired),
            ("recv_cqe", self.recv_cqe),
            ("cqe_error", self.cqe_error),
            ("poll_calls", self.poll_calls),
            ("empty_polls", self.empty_polls),
            ("send_jfc_poll_calls", self.send_jfc_poll_calls),
            ("recv_jfc_poll_calls", self.recv_jfc_poll_calls),
            ("yield_count", self.yield_count),
            ("sleep_count", self.sleep_count),
            ("backoff_sleep_ns", self.backoff_sleep_ns),
            ("jfc_rearm_count", self.jfc_rearm_count),
            ("event_wait_count", self.event_wait_count),
            ("event_wakeup_count", self.event_wakeup_count),
            ("event_timeout_count", self.event_timeout_count),
            ("spurious_wakeup_count", self.spurious_wakeup_count),
            ("event_wait_ns", self.event_wait_ns),
            ("max_event_wait_ns", self.max_event_wait_ns),
            ("max_empty_streak", self.max_empty_streak),
            ("nonempty_polls", self.nonempty_polls),
            ("completion_batch_total", self.completion_batch_total),
            ("avg_poll_batch_milli", self.avg_poll_batch_milli),
            ("empty_poll_ratio_ppm", self.empty_poll_ratio_ppm),
            (
                "max_completion_poll_gap_ns",
                self.max_completion_poll_gap_ns,
            ),
            ("max_outstanding_send", self.max_outstanding_send),
            ("current_outstanding_send", self.current_outstanding_send),
            ("current_outstanding_recv", self.current_outstanding_recv),
            ("configured_window", self.configured_window),
            ("configured_receive_credit", self.configured_receive_credit),
            ("slot_size", self.slot_size),
            ("effective_payload_size", self.effective_payload_size),
            ("tx_slot_count", self.tx_slot_count),
            ("rx_slot_count", self.rx_slot_count),
            ("total_registered_bytes", self.total_registered_bytes),
            ("bytes_sent", self.bytes_sent),
            ("bytes_received", self.bytes_received),
        ] {
            output.insert(name.into(), value);
        }
    }
}

pub fn run_urma_parent(
    case: &BenchmarkCase,
    device: impl Into<String>,
    eid_index: u32,
    listen: impl ToSocketAddrs,
    source: UrmaBenchmarkSource,
) -> Result<BenchmarkResult> {
    run_urma_parent_profile(
        case,
        device,
        eid_index,
        listen,
        source,
        UrmaBenchmarkProfile::Normal,
    )
}

pub fn run_urma_parent_profile(
    case: &BenchmarkCase,
    device: impl Into<String>,
    eid_index: u32,
    listen: impl ToSocketAddrs,
    source: UrmaBenchmarkSource,
    profile: UrmaBenchmarkProfile,
) -> Result<BenchmarkResult> {
    source.validate(case)?;
    if profile.fixed_tx()
        && (case.scenario != BenchmarkScenario::Memory
            || case.transfer_bytes == 0
            || case.transfer_bytes % case.chunk_size != 0)
    {
        return Err(invalid(
            "fixed-tx profile requires a non-empty, chunk-aligned memory case",
        ));
    }
    let runtime_config = benchmark_runtime_config(case, device, eid_index, profile)?;
    let jetty_config = JettyConfig::default();
    validate_urma_case(
        case,
        UrmaPipelineLimits::from_configs(&runtime_config, &jetty_config),
    )?;
    let setup_measurement = setup_measurement(case.timing_mode)?;
    let listener = TcpListener::bind(listen).map_err(|error| io_error("bind URMA OOB", error))?;
    let mut runtime = UrmaRuntime::start(runtime_config.clone())?;
    validate_urma_case(
        case,
        UrmaPipelineLimits::from_configs(&runtime_config, &jetty_config)
            .with_provider_max_message_size(runtime.capability().max_msg_size),
    )?;
    let mut connection = runtime.create_connection(jetty_config)?;
    let (stream, _) = listener
        .accept()
        .map_err(|error| io_error("accept URMA OOB", error))?;
    let mut session = parent_handshake(stream, &mut connection)?;

    let request = IntegrationMessageV3::decode(&connection.wait_for_frame(TIMEOUT)?)?;
    match &request.body {
        IntegrationMessageBodyV3::Request {
            task_id,
            piece_number,
        } if request.request_id == REQUEST_ID
            && request.sequence == 0
            && task_id == &case.case_id
            && *piece_number == case.repeat => {}
        _ => return Err(Error::Protocol("invalid URMA benchmark Request".into())),
    }
    let fixed_payload = profile
        .fixed_tx()
        .then(|| vec![0x5a; case.chunk_size_usize().expect("case validated")]);
    let expected_crc32 = if let Some(payload) = fixed_payload.as_deref() {
        repeated_payload_crc32(payload, case.chunk_count()?)
    } else {
        source.expected_crc32()
    };
    let metadata = IntegrationMessageV3::metadata(
        REQUEST_ID,
        0,
        case.transfer_bytes,
        DigestDescriptor::crc32(expected_crc32),
    );
    connection.send_frame(&metadata.encode()?)?;
    connection.drain_completions(TIMEOUT)?;

    expect_case_control(&mut session, READY, &case.case_id)?;
    let measurement = match setup_measurement {
        Some(measurement) => measurement,
        None => Measurement::start(case.timing_mode)?,
    };
    write_control(session.stream_mut(), START, case.case_id.as_bytes())?;
    let mut pipeline = PipelineTracker::new(case.window as usize)?;
    connection
        .configure_send_completion_interval((case.window as usize).min(SEND_COMPLETION_INTERVAL))?;
    if let Some(payload) = fixed_payload.as_deref() {
        connection.prepare_aliased_tx(payload)?;
    }
    let mut bytes_sent = 0u64;
    let data_messages = if let Some(payload) = fixed_payload.as_deref() {
        send_fixed_payload(
            payload.len(),
            case.chunk_count()?,
            &mut connection,
            &mut pipeline,
            &mut bytes_sent,
        )?
    } else {
        send_source(
            &source,
            case.chunk_size_usize()?,
            &mut connection,
            &mut pipeline,
            &mut bytes_sent,
        )?
    };
    drain_pipeline(&mut connection, &mut pipeline)?;
    let end = IntegrationMessageV3::end(REQUEST_ID, data_messages, case.transfer_bytes);
    connection.send_frame(&end.encode()?)?;
    connection.drain_completions(TIMEOUT)?;
    if pipeline.current() != 0 || connection.outstanding_send() != 0 {
        return Err(Error::Protocol("URMA pipeline did not fully drain".into()));
    }
    if case.window > 1 && data_messages > 1 && pipeline.maximum() <= 1 {
        return Err(Error::Protocol(
            "configured W>1 but max_outstanding_send did not exceed one".into(),
        ));
    }
    let (parent_sample, parent_cpu) = measurement.finish()?;

    let done = decode_done(&read_control(session.stream_mut(), DONE)?)?;
    if done.case_id != case.case_id || !done.integrity.is_ok() {
        return Err(Error::Protocol("invalid URMA benchmark Done".into()));
    }
    let local = connection.stats();
    let stats = combined_stats(
        case,
        &runtime_config,
        local,
        done.completion,
        bytes_sent,
        done.bytes_received,
    )?;
    let child_sample =
        TimingSample::from_duration(case.timing_mode, Duration::from_nanos(done.elapsed_ns));
    let mut result = BenchmarkResult::from_sample(case, child_sample, done.integrity)?;
    result.parent_cpu = Some(parent_cpu);
    result.child_cpu = Some(done.child_cpu);
    stats.insert_all(&mut result.transport_stats);
    result
        .transport_stats
        .insert("fixed_tx_profile".into(), u64::from(profile.fixed_tx()));
    result
        .transport_stats
        .insert("parent_elapsed_ns".into(), parent_sample.elapsed_ns()?);

    session.close()?;
    connection.close()?;
    runtime.shutdown()?;
    Ok(result)
}

pub fn run_urma_child(
    case: &BenchmarkCase,
    device: impl Into<String>,
    eid_index: u32,
    parent: impl ToSocketAddrs,
    destination: UrmaBenchmarkDestination,
) -> Result<BenchmarkResult> {
    run_urma_child_profile(
        case,
        device,
        eid_index,
        parent,
        destination,
        UrmaBenchmarkProfile::Normal,
    )
}

pub fn run_urma_child_profile(
    case: &BenchmarkCase,
    device: impl Into<String>,
    eid_index: u32,
    parent: impl ToSocketAddrs,
    destination: UrmaBenchmarkDestination,
    profile: UrmaBenchmarkProfile,
) -> Result<BenchmarkResult> {
    destination.validate(case)?;
    let runtime_config = benchmark_runtime_config(case, device, eid_index, profile)?;
    let jetty_config = JettyConfig::default();
    validate_urma_case(
        case,
        UrmaPipelineLimits::from_configs(&runtime_config, &jetty_config),
    )?;
    let setup_measurement = setup_measurement(case.timing_mode)?;
    let mut runtime = UrmaRuntime::start(runtime_config.clone())?;
    validate_urma_case(
        case,
        UrmaPipelineLimits::from_configs(&runtime_config, &jetty_config)
            .with_provider_max_message_size(runtime.capability().max_msg_size),
    )?;
    let mut connection = runtime.create_connection(jetty_config)?;
    let stream = TcpStream::connect(parent).map_err(|error| io_error("connect URMA OOB", error))?;
    let mut session = child_handshake(stream, &mut connection)?;

    let request = IntegrationMessageV3::request(REQUEST_ID, case.case_id.clone(), case.repeat);
    connection.send_frame(&request.encode()?)?;
    let metadata = IntegrationMessageV3::decode(&connection.wait_for_frame(TIMEOUT)?)?;
    let expected_crc32 = match &metadata.body {
        IntegrationMessageBodyV3::Metadata { digest, .. }
            if digest.algorithm == DigestAlgorithm::Crc32 =>
        {
            digest
                .value
                .parse::<u32>()
                .map_err(|_| Error::Protocol("invalid Metadata CRC32".into()))?
        }
        _ => return Err(Error::Protocol("expected URMA benchmark Metadata".into())),
    };
    let mut receiver = UrmaReceiveState::new(REQUEST_ID, case.transfer_bytes, expected_crc32)?;
    receiver.accept_metadata(&metadata)?;
    let mut sink =
        destination.create_sink(case.transfer_bytes, expected_crc32, case.completion_policy)?;
    let remaining_messages = usize::try_from(case.chunk_count()? + 1)
        .map_err(|_| invalid("receive message count exceeds usize"))?;
    let credit_target = receive_credit_target(
        case.window as usize,
        runtime_config.buffer_pool.rx_slot_count,
        remaining_messages,
    )?;
    let mut credit = ReceiveCreditController::new(credit_target, remaining_messages)?;
    replenish_credit(&mut connection, &mut credit)?;
    if connection.receive_credit() != credit.current_credit() {
        return Err(Error::Protocol("RX credit accounting mismatch".into()));
    }
    write_control(session.stream_mut(), READY, case.case_id.as_bytes())?;
    expect_case_control(&mut session, START, &case.case_id)?;
    let measurement = match setup_measurement {
        Some(measurement) => measurement,
        None => Measurement::start(case.timing_mode)?,
    };

    let mut last_progress = Instant::now();
    let mut bytes_received = 0u64;
    let expected_data_messages = u32::try_from(case.chunk_count()?)
        .map_err(|_| Error::Protocol("URMA Data sequence count exceeds u32".into()))?;
    let mut received_data_messages = 0u32;
    'receive: loop {
        let mut transfer_complete = false;
        let completed = connection.poll_recv_direct(|_posted_sequence, bytes| {
            credit.completed()?;
            if received_data_messages < expected_data_messages {
                receiver.accept_data(received_data_messages, bytes, &mut sink)?;
                bytes_received = bytes_received
                    .checked_add(bytes.len() as u64)
                    .ok_or_else(|| Error::Protocol("received byte count overflow".into()))?;
                received_data_messages += 1;
            } else {
                let message = IntegrationMessageV3::decode(bytes)?;
                transfer_complete = receiver.accept_payload(&message, &mut sink)?;
            }
            Ok(())
        })?;
        if completed == 0 {
            if idle_timeout_elapsed(last_progress, Instant::now(), TIMEOUT) {
                log_child_receive_timeout(&connection, &credit);
                return Err(Error::Timeout {
                    operation: "URMA benchmark receive",
                });
            }
            continue;
        }
        last_progress = Instant::now();
        // The callback consumed bytes directly from completed registered RX
        // slots. Refill the deep RQ before the next CQ batch.
        replenish_credit(&mut connection, &mut credit)?;
        if transfer_complete {
            break 'receive;
        }
    }
    if credit.remaining_messages() != 0
        || credit.current_credit() != 0
        || connection.outstanding_recv() != 0
    {
        return Err(Error::Protocol(
            "RX credits or slots remain outstanding after End".into(),
        ));
    }
    let integrity = sink.finish()?;
    if !integrity.is_ok() {
        return Err(Error::Protocol(
            "URMA sink integrity verification failed".into(),
        ));
    }
    let (sample, child_cpu) = measurement.finish()?;
    let completion = connection.stats();
    let mut result = BenchmarkResult::from_sample(case, sample, integrity)?;
    result.child_cpu = Some(child_cpu);
    combined_stats(
        case,
        &runtime_config,
        CompletionStats::default(),
        completion,
        0,
        bytes_received,
    )?
    .insert_all(&mut result.transport_stats);
    let done = Done {
        case_id: case.case_id.clone(),
        integrity,
        elapsed_ns: result.elapsed_ns,
        child_cpu,
        completion,
        bytes_received,
    };
    write_control(session.stream_mut(), DONE, &encode_done(&done)?)?;

    session.close()?;
    connection.close()?;
    runtime.shutdown()?;
    Ok(result)
}

fn send_source(
    source: &UrmaBenchmarkSource,
    chunk_size: usize,
    connection: &mut UrmaConnection<'_>,
    pipeline: &mut PipelineTracker,
    bytes_sent: &mut u64,
) -> Result<u32> {
    let mut last_progress = Instant::now();
    let mut sequence = 0u32;
    let chunk_count = match source {
        UrmaBenchmarkSource::Memory(source) => source.length().div_ceil(chunk_size as u64),
        UrmaBenchmarkSource::File(source) => source.length().div_ceil(chunk_size as u64),
    };
    match source {
        UrmaBenchmarkSource::Memory(source) => {
            for chunk in source.chunks(chunk_size)? {
                post_data(
                    connection,
                    pipeline,
                    sequence,
                    chunk,
                    u64::from(sequence) + 1 == chunk_count,
                    &mut last_progress,
                )?;
                *bytes_sent += chunk.len() as u64;
                sequence = sequence
                    .checked_add(1)
                    .ok_or_else(|| Error::Protocol("URMA sequence overflow".into()))?;
            }
        }
        UrmaBenchmarkSource::File(source) => {
            let mut file = source.open()?;
            let mut buffer = vec![0u8; chunk_size];
            loop {
                let read = read_chunk(&mut file, &mut buffer)?;
                if read == 0 {
                    break;
                }
                post_data(
                    connection,
                    pipeline,
                    sequence,
                    &buffer[..read],
                    u64::from(sequence) + 1 == chunk_count,
                    &mut last_progress,
                )?;
                *bytes_sent += read as u64;
                sequence = sequence
                    .checked_add(1)
                    .ok_or_else(|| Error::Protocol("URMA sequence overflow".into()))?;
            }
        }
    }
    Ok(sequence)
}

fn repeated_payload_crc32(payload: &[u8], repetitions: u64) -> u32 {
    let mut hasher = Crc32Hasher::new();
    for _ in 0..repetitions {
        hasher.update(payload);
    }
    hasher.finalize()
}

fn send_fixed_payload(
    payload_len: usize,
    chunk_count: u64,
    connection: &mut UrmaConnection<'_>,
    pipeline: &mut PipelineTracker,
    bytes_sent: &mut u64,
) -> Result<u32> {
    let mut last_progress = Instant::now();
    let count = u32::try_from(chunk_count)
        .map_err(|_| Error::Protocol("URMA sequence count exceeds u32".into()))?;
    for sequence in 0..count {
        while !pipeline.can_post() {
            let completed = poll_send_completions(connection, pipeline)?;
            if completed != 0 {
                last_progress = Instant::now();
            } else if idle_timeout_elapsed(last_progress, Instant::now(), TIMEOUT) {
                return Err(Error::Timeout {
                    operation: "URMA fixed TX pipeline capacity",
                });
            }
        }
        connection.send_prepared_tracked(
            payload_len,
            u64::from(sequence),
            sequence + 1 == count,
        )?;
        pipeline.posted()?;
        *bytes_sent = bytes_sent
            .checked_add(payload_len as u64)
            .ok_or_else(|| Error::Protocol("sent byte count overflow".into()))?;
    }
    Ok(count)
}

fn post_data(
    connection: &mut UrmaConnection<'_>,
    pipeline: &mut PipelineTracker,
    sequence: u32,
    payload: &[u8],
    is_last: bool,
    last_progress: &mut Instant,
) -> Result<()> {
    while !pipeline.can_post() {
        let completed = poll_send_completions(connection, pipeline)?;
        if completed != 0 {
            *last_progress = Instant::now();
        } else if idle_timeout_elapsed(*last_progress, Instant::now(), TIMEOUT) {
            log_parent_pipeline_capacity_timeout(connection, pipeline);
            return Err(Error::Timeout {
                operation: "URMA pipeline capacity",
            });
        }
    }
    if is_last {
        connection.send_frame_tracked_tail(payload, u64::from(sequence))?;
    } else {
        connection.send_frame_tracked(payload, u64::from(sequence))?;
    }
    pipeline.posted()?;
    debug_assert_eq!(pipeline.current(), connection.outstanding_send());
    Ok(())
}

fn drain_pipeline(
    connection: &mut UrmaConnection<'_>,
    pipeline: &mut PipelineTracker,
) -> Result<()> {
    let mut last_progress = Instant::now();
    while pipeline.current() != 0 {
        let completed = poll_send_completions(connection, pipeline)?;
        if completed != 0 {
            last_progress = Instant::now();
        } else if idle_timeout_elapsed(last_progress, Instant::now(), TIMEOUT) {
            log_parent_pipeline_timeout(connection, pipeline, "pipeline_drain");
            return Err(Error::Timeout {
                operation: "URMA pipeline drain",
            });
        }
    }
    Ok(())
}

fn poll_send_completions(
    connection: &mut UrmaConnection<'_>,
    pipeline: &mut PipelineTracker,
) -> Result<usize> {
    let mut completed = 0;
    for event in connection.poll_once()? {
        match event {
            CompletionEvent::SendCompleted { .. } => {
                pipeline.completed()?;
                completed += 1;
            }
            CompletionEvent::RecvCompleted { .. } => {
                return Err(Error::Protocol(
                    "unexpected receive CQE while sending URMA payload".into(),
                ))
            }
        }
    }
    debug_assert_eq!(pipeline.current(), connection.outstanding_send());
    Ok(completed)
}

fn replenish_credit(
    connection: &mut UrmaConnection<'_>,
    credit: &mut ReceiveCreditController,
) -> Result<()> {
    while credit.posts_needed() != 0 {
        connection.recv_ready_tracked(credit.next_post_sequence())?;
        credit.posted()?;
    }
    Ok(())
}

fn log_parent_pipeline_capacity_timeout(
    connection: &UrmaConnection<'_>,
    pipeline: &PipelineTracker,
) {
    log_parent_pipeline_timeout(connection, pipeline, "pipeline_capacity");
}

fn log_parent_pipeline_timeout(
    connection: &UrmaConnection<'_>,
    pipeline: &PipelineTracker,
    operation: &str,
) {
    let stats = connection.stats();
    let slots = connection.tx_slot_state_snapshot();
    let diagnostic = connection.pending_send_diagnostic();
    eprintln!(
        "{{\"event\":\"urma_benchmark_timeout\",\"role\":\"parent\",\"operation\":\"{}\",\"configured_window\":{},\"current_outstanding_send\":{},\"pipeline_tracker_current\":{},\"max_outstanding_send\":{},\"send_post\":{},\"send_cqe\":{},\"recv_post\":{},\"recv_cqe\":{},\"cqe_error\":{},\"poll_calls\":{},\"empty_polls\":{},\"connection_outstanding_send\":{},\"last_completed_sequence\":{},\"pending_send\":{},\"tx_slots\":{{\"free\":{},\"allocated\":{},\"send_posted\":{},\"send_completed\":{},\"other\":{}}}}}",
        operation,
        pipeline.configured_window(),
        connection.outstanding_send(),
        pipeline.current(),
        stats.max_outstanding_send,
        stats.send_post,
        stats.send_cqe,
        stats.recv_post,
        stats.recv_cqe,
        stats.cqe_error,
        stats.poll_calls,
        stats.empty_polls,
        connection.outstanding_send(),
        optional_sequence_json(diagnostic.last_completed_sequence),
        pending_wr_json(&diagnostic.pending),
        slots.free,
        slots.allocated,
        slots.send_posted,
        slots.send_completed,
        slots.other,
    );
}

fn log_child_receive_timeout(connection: &UrmaConnection<'_>, credit: &ReceiveCreditController) {
    let stats = connection.stats();
    let slots = connection.rx_slot_state_snapshot();
    let diagnostic = connection.pending_recv_diagnostic();
    eprintln!(
        "{{\"event\":\"urma_benchmark_timeout\",\"role\":\"child\",\"operation\":\"benchmark_receive\",\"configured_receive_credit\":{},\"current_receive_credit\":{},\"benchmark_credit_current\":{},\"benchmark_credit_remaining_messages\":{},\"recv_post\":{},\"recv_cqe\":{},\"send_post\":{},\"send_cqe\":{},\"cqe_error\":{},\"poll_calls\":{},\"empty_polls\":{},\"connection_outstanding_recv\":{},\"last_completed_sequence\":{},\"pending_recv\":{},\"rx_slots\":{{\"free\":{},\"allocated\":{},\"posted_recv\":{},\"recv_completed\":{},\"other\":{}}}}}",
        credit.configured_credit(),
        connection.receive_credit(),
        credit.current_credit(),
        credit.remaining_messages(),
        stats.recv_post,
        stats.recv_cqe,
        stats.send_post,
        stats.send_cqe,
        stats.cqe_error,
        stats.poll_calls,
        stats.empty_polls,
        connection.outstanding_recv(),
        optional_sequence_json(diagnostic.last_completed_sequence),
        pending_wr_json(&diagnostic.pending),
        slots.free,
        slots.allocated,
        slots.posted_recv,
        slots.recv_completed,
        slots.other,
    );
}

fn optional_sequence_json(sequence: Option<u64>) -> String {
    sequence.map_or_else(|| "null".into(), |value| value.to_string())
}

fn pending_wr_json(pending: &[crate::PendingWrSnapshot]) -> String {
    let entries = pending
        .iter()
        .map(|item| {
            format!(
                "{{\"sequence\":{},\"slot_id\":{},\"slot_state\":\"{:?}\"}}",
                optional_sequence_json(item.sequence),
                item.slot.index(),
                item.state,
            )
        })
        .collect::<Vec<_>>();
    format!("[{}]", entries.join(","))
}

fn read_chunk(file: &mut File, buffer: &mut [u8]) -> Result<usize> {
    let mut filled = 0;
    while filled < buffer.len() {
        match file.read(&mut buffer[filled..]) {
            Ok(0) => break,
            Ok(read) => filled += read,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(io_error("read URMA benchmark source", error)),
        }
    }
    Ok(filled)
}

fn setup_measurement(mode: TimingMode) -> Result<Option<Measurement>> {
    if mode == TimingMode::SetupIncluded {
        Ok(Some(Measurement::start(mode)?))
    } else {
        Ok(None)
    }
}

#[derive(Clone, Copy, Debug)]
struct CpuSnapshot(CpuUsage);

impl CpuSnapshot {
    fn capture() -> Result<Self> {
        let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
        if unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) } != 0 {
            return Err(io_error(
                "get process CPU usage",
                io::Error::last_os_error(),
            ));
        }
        Ok(Self(CpuUsage {
            user_us: timeval_us(usage.ru_utime)?,
            system_us: timeval_us(usage.ru_stime)?,
        }))
    }

    fn elapsed_since(self, start: Self) -> Result<CpuUsage> {
        Ok(CpuUsage {
            user_us: self
                .0
                .user_us
                .checked_sub(start.0.user_us)
                .ok_or_else(|| invalid("process user CPU time moved backwards"))?,
            system_us: self
                .0
                .system_us
                .checked_sub(start.0.system_us)
                .ok_or_else(|| invalid("process system CPU time moved backwards"))?,
        })
    }
}

fn timeval_us(value: libc::timeval) -> Result<u64> {
    let seconds = u64::try_from(value.tv_sec).map_err(|_| invalid("negative CPU seconds"))?;
    let micros = u64::try_from(value.tv_usec).map_err(|_| invalid("negative CPU micros"))?;
    seconds
        .checked_mul(1_000_000)
        .and_then(|value| value.checked_add(micros))
        .ok_or_else(|| invalid("process CPU time overflow"))
}

struct Measurement {
    timer: BenchmarkTimer,
    cpu: CpuSnapshot,
}

impl Measurement {
    fn start(mode: TimingMode) -> Result<Self> {
        Ok(Self {
            timer: BenchmarkTimer::start(mode),
            cpu: CpuSnapshot::capture()?,
        })
    }

    fn finish(self) -> Result<(TimingSample, CpuUsage)> {
        Ok((
            self.timer.finish(),
            CpuSnapshot::capture()?.elapsed_since(self.cpu)?,
        ))
    }
}

fn combined_stats(
    case: &BenchmarkCase,
    runtime: &RuntimeConfig,
    parent: CompletionStats,
    child: CompletionStats,
    bytes_sent: u64,
    bytes_received: u64,
) -> Result<UrmaTransportStats> {
    let total_registered_bytes = runtime.buffer_pool.total_len()?;
    let remaining_messages = usize::try_from(case.chunk_count()? + 1)
        .map_err(|_| invalid("receive message count exceeds usize"))?;
    let configured_receive_credit = receive_credit_target(
        case.window as usize,
        runtime.buffer_pool.rx_slot_count,
        remaining_messages,
    )?;
    let poll_calls = parent.poll_calls.saturating_add(child.poll_calls);
    let empty_polls = parent.empty_polls.saturating_add(child.empty_polls);
    let nonempty_polls = parent.nonempty_polls.saturating_add(child.nonempty_polls);
    let completion_batch_total = parent
        .completion_batch_total
        .saturating_add(child.completion_batch_total);
    Ok(UrmaTransportStats {
        send_post: parent.send_post,
        recv_post: child.recv_post,
        send_cqe: parent.send_cqe,
        send_retired: parent.send_retired,
        recv_cqe: child.recv_cqe,
        cqe_error: parent.cqe_error + child.cqe_error,
        poll_calls,
        empty_polls,
        send_jfc_poll_calls: parent
            .send_jfc_poll_calls
            .saturating_add(child.send_jfc_poll_calls),
        recv_jfc_poll_calls: parent
            .recv_jfc_poll_calls
            .saturating_add(child.recv_jfc_poll_calls),
        yield_count: parent.yield_count.saturating_add(child.yield_count),
        sleep_count: parent.sleep_count.saturating_add(child.sleep_count),
        backoff_sleep_ns: parent
            .backoff_sleep_ns
            .saturating_add(child.backoff_sleep_ns),
        jfc_rearm_count: parent.jfc_rearm_count.saturating_add(child.jfc_rearm_count),
        event_wait_count: parent
            .event_wait_count
            .saturating_add(child.event_wait_count),
        event_wakeup_count: parent
            .event_wakeup_count
            .saturating_add(child.event_wakeup_count),
        event_timeout_count: parent
            .event_timeout_count
            .saturating_add(child.event_timeout_count),
        spurious_wakeup_count: parent
            .spurious_wakeup_count
            .saturating_add(child.spurious_wakeup_count),
        event_wait_ns: parent.event_wait_ns.saturating_add(child.event_wait_ns),
        max_event_wait_ns: parent.max_event_wait_ns.max(child.max_event_wait_ns),
        max_empty_streak: parent.max_empty_streak.max(child.max_empty_streak),
        nonempty_polls,
        completion_batch_total,
        avg_poll_batch_milli: scaled_ratio(completion_batch_total, nonempty_polls, 1_000),
        empty_poll_ratio_ppm: scaled_ratio(empty_polls, poll_calls, 1_000_000),
        max_completion_poll_gap_ns: parent
            .max_completion_poll_gap_ns
            .max(child.max_completion_poll_gap_ns),
        max_outstanding_send: parent.max_outstanding_send,
        current_outstanding_send: 0,
        current_outstanding_recv: 0,
        configured_window: u64::from(case.window),
        configured_receive_credit: u64::try_from(configured_receive_credit)
            .map_err(|_| invalid("configured receive credit does not fit u64"))?,
        slot_size: u64::try_from(runtime.buffer_pool.slot_size)
            .map_err(|_| invalid("slot_size does not fit result u64"))?,
        effective_payload_size: case.chunk_size,
        tx_slot_count: u64::try_from(runtime.buffer_pool.tx_slot_count)
            .map_err(|_| invalid("TX slot count does not fit result u64"))?,
        rx_slot_count: u64::try_from(runtime.buffer_pool.rx_slot_count)
            .map_err(|_| invalid("RX slot count does not fit result u64"))?,
        total_registered_bytes: u64::try_from(total_registered_bytes)
            .map_err(|_| invalid("registered pool size does not fit result u64"))?,
        bytes_sent,
        bytes_received,
    })
}

fn benchmark_runtime_config(
    case: &BenchmarkCase,
    device: impl Into<String>,
    eid_index: u32,
    profile: UrmaBenchmarkProfile,
) -> Result<RuntimeConfig> {
    let mut config = RuntimeConfig::new(device, eid_index);
    config.buffer_pool.slot_size = derive_urma_slot_size(case, config.buffer_pool.alignment)?;
    config.buffer_pool.alias_tx_slots = profile.fixed_tx();
    config.buffer_pool.rx_slot_count = profile.rx_slots();
    config.buffer_pool.total_len()?;
    Ok(config)
}

fn expect_case_control(session: &mut OobSession, kind: u16, case_id: &str) -> Result<()> {
    let payload = read_control(session.stream_mut(), kind)?;
    if payload != case_id.as_bytes() {
        return Err(Error::Protocol(
            "URMA benchmark control case_id mismatch".into(),
        ));
    }
    Ok(())
}

fn write_control(stream: &mut TcpStream, kind: u16, payload: &[u8]) -> Result<()> {
    if payload.len() > MAX_CONTROL_PAYLOAD {
        return Err(invalid("URMA benchmark control payload too large"));
    }
    let mut header = Vec::with_capacity(CONTROL_HEADER_LEN);
    header.extend_from_slice(&CONTROL_MAGIC.to_be_bytes());
    header.extend_from_slice(&CONTROL_VERSION.to_be_bytes());
    header.extend_from_slice(&kind.to_be_bytes());
    header.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    stream
        .write_all(&header)
        .and_then(|_| stream.write_all(payload))
        .map_err(|error| io_error("write URMA benchmark control", error))
}

fn read_control(stream: &mut TcpStream, expected_kind: u16) -> Result<Vec<u8>> {
    let mut header = [0u8; CONTROL_HEADER_LEN];
    stream
        .read_exact(&mut header)
        .map_err(|error| io_error("read URMA benchmark control header", error))?;
    let magic = u32::from_be_bytes(header[0..4].try_into().expect("fixed slice"));
    let version = u16::from_be_bytes(header[4..6].try_into().expect("fixed slice"));
    let kind = u16::from_be_bytes(header[6..8].try_into().expect("fixed slice"));
    let length = u32::from_be_bytes(header[8..12].try_into().expect("fixed slice")) as usize;
    if magic != CONTROL_MAGIC
        || version != CONTROL_VERSION
        || kind != expected_kind
        || length > MAX_CONTROL_PAYLOAD
    {
        return Err(Error::Protocol(
            "invalid URMA benchmark control frame".into(),
        ));
    }
    let mut payload = vec![0u8; length];
    stream
        .read_exact(&mut payload)
        .map_err(|error| io_error("read URMA benchmark control payload", error))?;
    Ok(payload)
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Done {
    case_id: String,
    integrity: IntegrityResult,
    elapsed_ns: u64,
    child_cpu: CpuUsage,
    completion: CompletionStats,
    bytes_received: u64,
}

fn encode_done(done: &Done) -> Result<Vec<u8>> {
    let case = done.case_id.as_bytes();
    let case_len = u16::try_from(case.len()).map_err(|_| invalid("case_id too long"))?;
    let mut output = Vec::with_capacity(128 + case.len());
    output.extend_from_slice(&case_len.to_be_bytes());
    output.extend_from_slice(case);
    for value in [
        done.integrity.expected_bytes,
        done.integrity.actual_bytes,
        done.elapsed_ns,
        done.child_cpu.user_us,
        done.child_cpu.system_us,
        done.completion.send_post,
        done.completion.recv_post,
        done.completion.send_cqe,
        done.completion.send_retired,
        done.completion.recv_cqe,
        done.completion.cqe_error,
        done.completion.poll_calls,
        done.completion.empty_polls,
        done.completion.send_jfc_poll_calls,
        done.completion.recv_jfc_poll_calls,
        done.completion.yield_count,
        done.completion.sleep_count,
        done.completion.backoff_sleep_ns,
        done.completion.jfc_rearm_count,
        done.completion.event_wait_count,
        done.completion.event_wakeup_count,
        done.completion.event_timeout_count,
        done.completion.spurious_wakeup_count,
        done.completion.event_wait_ns,
        done.completion.max_event_wait_ns,
        done.completion.max_empty_streak,
        done.completion.nonempty_polls,
        done.completion.completion_batch_total,
        done.completion.max_completion_poll_gap_ns,
        done.completion.max_outstanding_send,
        done.bytes_received,
    ] {
        output.extend_from_slice(&value.to_be_bytes());
    }
    output.extend_from_slice(&done.integrity.expected_crc32.to_be_bytes());
    output.extend_from_slice(&done.integrity.actual_crc32.to_be_bytes());
    Ok(output)
}

fn decode_done(input: &[u8]) -> Result<Done> {
    if input.len() < 2 {
        return Err(Error::Protocol("truncated URMA Done".into()));
    }
    let case_len = u16::from_be_bytes([input[0], input[1]]) as usize;
    let expected_len = 2 + case_len + 31 * 8 + 2 * 4;
    if input.len() != expected_len {
        return Err(Error::Protocol("invalid URMA Done length".into()));
    }
    let case_id = std::str::from_utf8(&input[2..2 + case_len])
        .map_err(|_| Error::Protocol("URMA Done case_id is not UTF-8".into()))?
        .to_owned();
    let mut offset = 2 + case_len;
    let mut next_u64 = || {
        let value = u64::from_be_bytes(input[offset..offset + 8].try_into().expect("fixed slice"));
        offset += 8;
        value
    };
    let expected_bytes = next_u64();
    let actual_bytes = next_u64();
    let elapsed_ns = next_u64();
    let user_us = next_u64();
    let system_us = next_u64();
    let send_post = next_u64();
    let recv_post = next_u64();
    let send_cqe = next_u64();
    let send_retired = next_u64();
    let recv_cqe = next_u64();
    let cqe_error = next_u64();
    let poll_calls = next_u64();
    let empty_polls = next_u64();
    let send_jfc_poll_calls = next_u64();
    let recv_jfc_poll_calls = next_u64();
    let yield_count = next_u64();
    let sleep_count = next_u64();
    let backoff_sleep_ns = next_u64();
    let jfc_rearm_count = next_u64();
    let event_wait_count = next_u64();
    let event_wakeup_count = next_u64();
    let event_timeout_count = next_u64();
    let spurious_wakeup_count = next_u64();
    let event_wait_ns = next_u64();
    let max_event_wait_ns = next_u64();
    let max_empty_streak = next_u64();
    let nonempty_polls = next_u64();
    let completion_batch_total = next_u64();
    let max_completion_poll_gap_ns = next_u64();
    let max_outstanding_send = next_u64();
    let bytes_received = next_u64();
    let expected_crc32 = u32::from_be_bytes(input[offset..offset + 4].try_into().expect("fixed"));
    offset += 4;
    let actual_crc32 = u32::from_be_bytes(input[offset..offset + 4].try_into().expect("fixed"));
    Ok(Done {
        case_id,
        integrity: IntegrityResult::new(expected_bytes, actual_bytes, expected_crc32, actual_crc32),
        elapsed_ns,
        child_cpu: CpuUsage { user_us, system_us },
        completion: CompletionStats {
            send_post,
            recv_post,
            send_cqe,
            send_retired,
            recv_cqe,
            cqe_error,
            poll_calls,
            empty_polls,
            send_jfc_poll_calls,
            recv_jfc_poll_calls,
            yield_count,
            sleep_count,
            backoff_sleep_ns,
            jfc_rearm_count,
            event_wait_count,
            event_wakeup_count,
            event_timeout_count,
            spurious_wakeup_count,
            event_wait_ns,
            max_event_wait_ns,
            max_empty_streak,
            nonempty_polls,
            completion_batch_total,
            max_completion_poll_gap_ns,
            max_outstanding_send,
        },
        bytes_received,
    })
}

fn io_error(operation: &'static str, error: io::Error) -> Error {
    Error::Io {
        operation,
        message: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn done_control_round_trip_preserves_hybrid_polling_stats() {
        let done = Done {
            case_id: "hybrid-poll".into(),
            integrity: IntegrityResult::new(64, 64, 7, 7),
            elapsed_ns: 11,
            child_cpu: CpuUsage {
                user_us: 13,
                system_us: 17,
            },
            completion: CompletionStats {
                send_post: 1,
                recv_post: 2,
                send_cqe: 3,
                send_retired: 4,
                recv_cqe: 5,
                cqe_error: 6,
                poll_calls: 7,
                empty_polls: 8,
                send_jfc_poll_calls: 9,
                recv_jfc_poll_calls: 10,
                yield_count: 11,
                sleep_count: 12,
                backoff_sleep_ns: 13,
                jfc_rearm_count: 14,
                event_wait_count: 15,
                event_wakeup_count: 16,
                event_timeout_count: 17,
                spurious_wakeup_count: 18,
                event_wait_ns: 19,
                max_event_wait_ns: 20,
                max_empty_streak: 21,
                nonempty_polls: 22,
                completion_batch_total: 23,
                max_completion_poll_gap_ns: 24,
                max_outstanding_send: 25,
            },
            bytes_received: 64,
        };

        assert_eq!(decode_done(&encode_done(&done).unwrap()).unwrap(), done);
    }
}
